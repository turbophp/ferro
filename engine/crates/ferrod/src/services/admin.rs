//! **`ADMIN`/`BACKUP` — the §7.6 online snapshot (M2-C3-7b).**
//!
//! Reached only after the session's D15 gate admitted the peer for an OPERATE verb
//! (`crate::admin::authorize`); nothing here re-checks authorization, and nothing here runs for a
//! refused peer.
//!
//! **Mechanism: `VACUUM INTO`, as C3-7a measured** — SQLite's own consistent-snapshot statement, run
//! through the pool like any other, so the per-request `timeout_ms` and `CANCEL` reach it through the
//! same guarded runner the SQL service uses (`run_autocommit_exec`: interrupt handle fired, statement
//! DRAINED, never dropped). It needs no pool coordination (a snapshot under an open writer excludes
//! the writer's uncommitted rows), and the target is a BOUND parameter, so no path is ever spliced
//! into SQL text. The D14 path guard still authorizes the file SQLite opens.
//!
//! **Destination policy.** The client names a plain FILE NAME — `[A-Za-z0-9._-]`, at most 255 bytes,
//! not starting with `.` — and the engine places it in the pool's D14 allowed directory. No path
//! crosses the wire in either direction (§12/D8 keep the database's location in the engine), and a
//! name cannot traverse, because it cannot contain a separator.
//!
//! **Atomic finalisation, so the engine never destroys a file it did not create.** The snapshot is
//! written to a unique temporary name in the same directory (`.<file>.ferro-backup-<pid>-<n>`, which
//! no policy-valid name can collide with, since those cannot start with `.`), then:
//!
//! * `replace = false` → `hard_link(tmp, target)`, which fails atomically if the name exists. An
//!   existing file is never touched.
//! * `replace = true` → `rename(tmp, target)`, which atomically swaps the new snapshot in. A FAILED
//!   backup leaves the previous snapshot exactly as it was — delete-then-write would have destroyed
//!   it before knowing the new one would succeed.
//!
//! Either way the temporary file is removed on every path, and on failure it is the only file the
//! engine removes.
//!
//! **Fate.** `VACUUM INTO` never modifies the SOURCE database, and the engine itself resolves the fate
//! of the one file it writes (finalised or removed), so a backup is classified as `readonly` for
//! `fate.rs`: a cancelled or timed-out backup is `Cancelled`/`QueryTimeout`, never `Indeterminate`.
//! The connection is nonetheless checked out NON-readonly, because `PRAGMA query_only` refuses
//! `VACUUM INTO` (C3-7a, property 4).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use ferro_proto::messages::ErrorPayload;
use ferro_proto::messages::admin::{BackupRequest, BackupResponse};
use ferro_proto::value::Value;
use tokio_util::sync::CancellationToken;

use crate::admin::forbidden;
use crate::pools::{AnyPool, PoolRegistry};
use crate::services::fate::{self, OpContext};
use crate::services::sql::{protocol, run_autocommit_exec, unsupported};
use crate::session::codec::InFrame;
use crate::session::responder::Responder;

/// Longest accepted file name, in bytes — the common `NAME_MAX`.
const MAX_FILE_NAME: usize = 255;

/// The fate context for a failure BEFORE the snapshot statement was dispatched: nothing was sent, so
/// nothing can be Indeterminate (and `VACUUM INTO` writes nothing to the source database anyway).
const NOT_SENT: OpContext = OpContext {
    readonly: true,
    sent: false,
    in_tx: false,
};

/// Disambiguates concurrent backups' temporary names within one process.
static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// `service=ADMIN, method=BACKUP`.
pub async fn handle_backup(
    frame: InFrame,
    responder: Responder,
    registry: &PoolRegistry,
    cancel: CancellationToken,
) {
    let req = match BackupRequest::decode(&frame.payload) {
        Ok(r) => r,
        Err(e) => {
            responder.end_error(protocol(format!("malformed BACKUP request: {e}")));
            return;
        }
    };
    let Some(pool) = registry.get(&req.pool) else {
        responder.end_error(unsupported(format!("unknown pool {:?}", req.pool)));
        return;
    };
    let AnyPool::Sqlite(pool) = pool else {
        // M2 scope, stated rather than approximated: a server database is snapshotted by its own
        // tooling (pg_dump, mysqldump, a physical backup), which an engine-side statement cannot
        // reproduce consistently for every deployment.
        responder.end_error(unsupported(format!(
            "BACKUP is implemented for SQLite pools only; pool {:?} is a server database — use its \
             own backup tooling",
            req.pool
        )));
        return;
    };
    if let Err(why) = check_file_name(&req.file) {
        responder.end_error(forbidden(why));
        return;
    }

    // The ONE resolution of the pool's allowed directory, shared with the D14 guard, so the
    // directory the snapshot is placed in and the directory the guard authorizes cannot differ.
    let root = match pool.backend().allowed_root() {
        Ok(r) => r,
        Err(e) => {
            responder.end_error(fate::classify_fate(e, NOT_SENT));
            return;
        }
    };
    let target = root.join(&req.file);
    if let Err(ep) = check_existing_target(&target, req.replace) {
        responder.end_error(ep);
        return;
    }
    let tmp = root.join(format!(
        ".{}.ferro-backup-{}-{}",
        req.file,
        std::process::id(),
        TMP_SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let Some(tmp_text) = tmp.to_str().map(str::to_owned) else {
        responder.end_error(forbidden(
            "the pool's allowed directory is not valid UTF-8, so a snapshot path cannot be bound"
                .to_string(),
        ));
        return;
    };

    let mut co = match pool.checkout_declared(false).await {
        Ok(co) => co,
        Err(e) => {
            responder.end_error(fate::classify_fate(e, NOT_SENT));
            return;
        }
    };
    let queue_us = co.stats().queue_us;
    let (result, exec_us) = run_autocommit_exec(
        &mut co,
        "VACUUM INTO ?1",
        &[Value::Text(tmp_text)],
        req.timeout_ms,
        &cancel,
    )
    .await;
    drop(co);

    if let Err(e) = result {
        remove_quietly(&tmp);
        responder.end_error(fate::classify_fate(
            e,
            OpContext {
                readonly: true,
                sent: true,
                in_tx: false,
            },
        ));
        return;
    }

    match finalise(&tmp, &target, req.replace) {
        Ok(bytes) => responder.end_ok(Bytes::from(
            BackupResponse {
                bytes,
                queue_us,
                exec_us,
            }
            .encode(),
        )),
        Err(ep) => {
            remove_quietly(&tmp);
            responder.end_error(ep);
        }
    }
}

/// The destination policy: a plain file name. The reason is returned for the `Forbidden` message.
fn check_file_name(name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > MAX_FILE_NAME {
        return Err(format!(
            "a backup file name must be 1..={MAX_FILE_NAME} bytes"
        ));
    }
    if name.starts_with('.') {
        return Err("a backup file name must not start with '.'".to_string());
    }
    if let Some(c) = name
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')))
    {
        return Err(format!(
            "a backup file name is a plain name placed in the pool's allowed directory, so it may \
             contain only ASCII letters, digits, '.', '_' and '-' (found {c:?}); directories are \
             chosen by the operator's FERRO_POOL_<NAME>_ALLOW_DIR, never by the request"
        ));
    }
    Ok(())
}

/// Refuse a target the engine must not replace: anything that is not a regular file, and — without
/// `replace` — anything at all. `lstat`, so a symlink is seen as one rather than followed.
fn check_existing_target(target: &Path, replace: bool) -> Result<(), ErrorPayload> {
    match target.symlink_metadata() {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(forbidden(format!(
            "cannot inspect the backup target: {}",
            e.kind()
        ))),
        Ok(m) if !m.file_type().is_file() => Err(forbidden(
            "the backup target exists and is not a regular file (a symlink or a directory); the \
             engine will not replace it"
                .to_string(),
        )),
        Ok(_) if !replace => Err(forbidden(
            "the backup target already exists; set replace to swap the new snapshot in atomically"
                .to_string(),
        )),
        Ok(_) => Ok(()),
    }
}

/// Move the finished snapshot into place and report its size. See the module doc for why each mode
/// uses the call it does.
fn finalise(tmp: &Path, target: &Path, replace: bool) -> Result<u64, ErrorPayload> {
    let moved = if replace {
        std::fs::rename(tmp, target)
    } else {
        std::fs::hard_link(tmp, target).and_then(|()| std::fs::remove_file(tmp))
    };
    if let Err(e) = moved {
        return Err(if e.kind() == std::io::ErrorKind::AlreadyExists {
            forbidden(
                "the backup target appeared while the snapshot was being taken; set replace to \
                 swap the new snapshot in"
                    .to_string(),
            )
        } else {
            unsupported(format!(
                "the snapshot was taken but could not be moved into place: {}",
                e.kind()
            ))
        });
    }
    Ok(std::fs::metadata(target).map(|m| m.len()).unwrap_or(0))
}

fn remove_quietly(path: &PathBuf) {
    if let Err(e) = std::fs::remove_file(path)
        && e.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(error = %e.kind(), "BACKUP: could not remove its temporary file");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_plain_file_name_is_accepted() {
        for ok in [
            "nightly.db",
            "snap-2026_10_02.sqlite",
            "a",
            &"x".repeat(MAX_FILE_NAME),
        ] {
            assert!(check_file_name(ok).is_ok(), "{ok:?} should be accepted");
        }
        for bad in [
            "",
            ".hidden",
            "..",
            ".",
            "../escape.db",
            "sub/dir.db",
            "/abs.db",
            "back\\slash.db",
            "spa ce.db",
            "nul\0.db",
            "ünï.db",
            &"x".repeat(MAX_FILE_NAME + 1),
        ] {
            assert!(check_file_name(bad).is_err(), "{bad:?} should be refused");
        }
    }

    #[test]
    fn an_existing_target_needs_replace_and_must_be_a_regular_file() {
        let dir = tempfile::tempdir().unwrap();
        let absent = dir.path().join("absent.db");
        assert!(check_existing_target(&absent, false).is_ok());

        let present = dir.path().join("present.db");
        std::fs::write(&present, b"old").unwrap();
        assert!(check_existing_target(&present, false).is_err());
        assert!(check_existing_target(&present, true).is_ok());

        let link = dir.path().join("link.db");
        std::os::unix::fs::symlink(dir.path().join("nowhere"), &link).unwrap();
        assert!(
            check_existing_target(&link, true).is_err(),
            "a dangling symlink is refused"
        );
        let sub = dir.path().join("sub.db");
        std::fs::create_dir(&sub).unwrap();
        assert!(
            check_existing_target(&sub, true).is_err(),
            "a directory is refused"
        );
    }

    #[test]
    fn finalise_never_overwrites_without_replace_and_swaps_atomically_with_it() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("t.db");
        std::fs::write(&target, b"previous").unwrap();

        let tmp = dir.path().join(".t.db.tmp");
        std::fs::write(&tmp, b"new snapshot").unwrap();
        let err = finalise(&tmp, &target, false).unwrap_err();
        assert_eq!(err.code, ferro_proto::consts::errc::FORBIDDEN);
        assert_eq!(
            std::fs::read(&target).unwrap(),
            b"previous",
            "never overwritten"
        );

        assert_eq!(finalise(&tmp, &target, true).unwrap(), 12);
        assert_eq!(std::fs::read(&target).unwrap(), b"new snapshot");
        assert!(!tmp.exists(), "the temporary name is gone after a rename");

        let fresh = dir.path().join("fresh.db");
        let tmp2 = dir.path().join(".fresh.db.tmp");
        std::fs::write(&tmp2, b"abc").unwrap();
        assert_eq!(finalise(&tmp2, &fresh, false).unwrap(), 3);
        assert!(!tmp2.exists(), "the temporary name is gone after a link");
    }
}
