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
//! into SQL text. SQLite re-issues `VACUUM INTO ?1` internally as a literal `ATTACH`, so the D14 path
//! guard still authorizes the file it opens.
//!
//! **Destination policy.** The client names a plain FILE NAME — `[A-Za-z0-9._-]`, at most
//! [`MAX_FILE_NAME`] bytes, not starting with `.` — and the engine places it in the pool's D14
//! allowed directory. No path crosses the wire in either direction (§12/D8 keep the database's
//! location in the engine), and a name cannot traverse, because it cannot contain a separator. A name
//! that is a LIVE DATABASE of any SQLite pool in that directory, or one of its sidecars (`-wal`,
//! `-shm`, `-journal`), is refused outright — the review measured `replace` over `main.db` losing an
//! acknowledged write, and over `main.db-wal` corrupting the database.
//!
//! **The temporary is the engine's own file, provably.** It is created EXCLUSIVELY (`O_CREAT|O_EXCL`,
//! mode `0600`) under an unpredictable 128-bit name (`.<file>.ferro-backup-<32 hex>`, which no
//! policy-valid name can equal, since those cannot start with `.`), and its `(dev, ino)` is recorded.
//! `VACUUM INTO` writes into that existing empty file. Before publishing, the engine re-checks that
//! the name still refers to the same inode with exactly ONE link — so a file planted at the name, a
//! hard link taken to exfiltrate the snapshot, or a swap of the name for something else, is refused
//! rather than published. The review reproduced all three against the first version, whose
//! predictable `<pid>-<n>` names it pre-linked to an outside file and read the snapshot through.
//! The engine only ever removes a file whose inode it created.
//!
//! **Atomic finalisation, so a failed backup never destroys the previous snapshot:**
//!
//! * `replace = false` → `hard_link(tmp, target)`, which fails atomically if the name exists.
//! * `replace = true` → `rename(tmp, target)`, which atomically swaps the new snapshot in. A FAILED
//!   backup leaves the previous snapshot exactly as it was — delete-then-write would have destroyed
//!   it before knowing the new one would succeed.
//!
//! **Fate.** `VACUUM INTO` never modifies the SOURCE database, and the engine itself resolves the fate
//! of the one file it writes (published or removed), so a backup is classified as `readonly` for
//! `fate.rs`: a cancelled or timed-out backup is `Cancelled`/`QueryTimeout`, never `Indeterminate`.
//! The connection is nonetheless checked out NON-readonly, because `PRAGMA query_only` refuses
//! `VACUUM INTO` (C3-7a, property 4).
//!
//! **Threat model, stated.** A party that can WRITE the allowed directory — by default the
//! database's own directory — can already delete or replace the database itself; the checks above
//! make the engine refuse rather than be steered by such a party, but they do not make a shared
//! writable directory safe. The snapshot is created `0600`, owned by `ferrod`'s user.

use std::fs::OpenOptions;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

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

/// Longest accepted file name, in bytes. Not `NAME_MAX` (255): the temporary adds a `.` prefix and
/// a `.ferro-backup-<32 hex>` suffix (47 bytes), and the review measured names of 230–255 bytes
/// failing at the temporary while the documented limit promised them. 200 + 47 = 247 ≤ 255.
const MAX_FILE_NAME: usize = 200;

/// The infix a temporary carries after the client's file name; its length is load-bearing for
/// [`MAX_FILE_NAME`] (`tmp_name_fits_name_max_and_is_never_a_valid_file_name` pins the arithmetic).
const TMP_INFIX: &str = ".ferro-backup-";

/// The fate context for a failure BEFORE the snapshot statement was dispatched: nothing was sent, so
/// nothing can be Indeterminate (and `VACUUM INTO` writes nothing to the source database anyway).
const NOT_SENT: OpContext = OpContext {
    readonly: true,
    sent: false,
    in_tx: false,
};

/// The temporary the engine created, held OPEN until it is published or removed.
///
/// The open handle is what makes `(dev, ino)` an identity rather than a coincidence: a filesystem
/// may reuse a freed inode number at once (tmpfs does — measured, the first version of
/// `the_temporary_is_exclusive_private_and_only_ours_is_removed` saw a stranger's replacement file
/// arrive under the deleted temporary's inode number and be deleted as "ours"). While the engine
/// holds the file open its inode cannot be freed, so no other file can carry its number.
struct Temp {
    path: PathBuf,
    _handle: std::fs::File,
    id: (u64, u64),
}

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
        Err(_) => {
            responder.end_error(unsupported(
                "the pool's allowed directory does not resolve, so no snapshot can be placed in it"
                    .to_string(),
            ));
            return;
        }
    };
    if let Err(ep) = check_not_live(registry, &root, &req.file) {
        responder.end_error(ep);
        return;
    }
    let target = root.join(&req.file);
    if let Err(ep) = check_existing_target(&target, req.replace) {
        responder.end_error(ep);
        return;
    }
    let tmp = match create_temp(&root, &req.file) {
        Ok(t) => t,
        Err(ep) => {
            responder.end_error(ep);
            return;
        }
    };
    let Some(tmp_text) = tmp.path.to_str().map(str::to_owned) else {
        remove_ours(&tmp);
        responder.end_error(forbidden(
            "the pool's allowed directory is not valid UTF-8, so a snapshot path cannot be bound"
                .to_string(),
        ));
        return;
    };

    let mut co = match pool.checkout_declared(false).await {
        Ok(co) => co,
        Err(e) => {
            remove_ours(&tmp);
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
        remove_ours(&tmp);
        responder.end_error(redact_root(
            fate::classify_fate(
                e,
                OpContext {
                    readonly: true,
                    sent: true,
                    in_tx: false,
                },
            ),
            &root,
        ));
        return;
    }

    match publish(&tmp, &target, req.replace) {
        Ok(bytes) => responder.end_ok(Bytes::from(
            BackupResponse {
                bytes,
                queue_us,
                exec_us,
            }
            .encode(),
        )),
        Err(ep) => responder.end_error(ep),
    }
}

/// SQLite names the file it could not open in its message (`unable to open database: /full/path`,
/// measured), and the snapshot's path lies in the allowed directory — which §12/D8 keep out of the
/// client's reach, and which this verb otherwise never sends. Replace the directory with a
/// placeholder in everything the peer will read.
fn redact_root(mut ep: ErrorPayload, root: &Path) -> ErrorPayload {
    let dir = root.display().to_string();
    if dir.is_empty() {
        return ep;
    }
    ep.message = ep.message.replace(&dir, "<allowed directory>");
    if let Some(d) = ep.detail.as_mut() {
        *d = d.replace(&dir, "<allowed directory>");
    }
    ep
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

/// Refuse a name that is the live database of ANY SQLite pool whose database sits in `root`, or one
/// of that database's sidecars. Regardless of `replace`: even without it, creating a `-wal` or
/// `-journal` beside a live database is a file SQLite may read as its own.
fn check_not_live(registry: &PoolRegistry, root: &Path, file: &str) -> Result<(), ErrorPayload> {
    for name in registry.names() {
        let Some(AnyPool::Sqlite(pool)) = registry.get(name) else {
            continue;
        };
        let Ok(db) = pool.backend().database_path() else {
            continue;
        };
        if db.parent() != Some(root) {
            continue;
        }
        let Some(db_name) = db.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if live_names(db_name).iter().any(|n| n == file) {
            return Err(forbidden(format!(
                "{file:?} is a live database file (or one of its -wal/-shm/-journal sidecars) of a \
                 pool in this directory; a snapshot may never be written over it"
            )));
        }
    }
    Ok(())
}

/// A database file name and the sidecar names SQLite may create beside it.
fn live_names(db_name: &str) -> [String; 4] {
    [
        db_name.to_string(),
        format!("{db_name}-wal"),
        format!("{db_name}-shm"),
        format!("{db_name}-journal"),
    ]
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

/// The temporary's name for `file`: hidden, and unpredictable.
fn tmp_name(file: &str, nonce: &[u8; 16]) -> String {
    let mut hex = String::with_capacity(32);
    for b in nonce {
        hex.push_str(&format!("{b:02x}"));
    }
    format!(".{file}{TMP_INFIX}{hex}")
}

/// Create the temporary for `file` under a fresh random name (see [`create_at`]).
fn create_temp(root: &Path, file: &str) -> Result<Temp, ErrorPayload> {
    let mut nonce = [0u8; 16];
    getrandom::getrandom(&mut nonce)
        .map_err(|_| unsupported("no randomness for the snapshot's temporary name".to_string()))?;
    create_at(root.join(tmp_name(file, &nonce)))
}

/// Create `path` EXCLUSIVELY (`O_CREAT|O_EXCL`), mode 0600, and keep it open (see [`Temp`]). An
/// existing file at the name is refused and left untouched — it is not the engine's.
fn create_at(path: PathBuf) -> Result<Temp, ErrorPayload> {
    let created = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)
        .and_then(|f| f.metadata().map(|m| (f, m)));
    match created {
        Ok((handle, m)) => Ok(Temp {
            path,
            _handle: handle,
            id: (m.dev(), m.ino()),
        }),
        // Something already holds a 128-bit random name: someone is planting files here.
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Err(forbidden(
            "the snapshot's temporary name was already taken; refusing to write into a file the \
             engine did not create"
                .to_string(),
        )),
        Err(e) => Err(unsupported(format!(
            "could not create the snapshot's temporary file: {}",
            e.kind()
        ))),
    }
}

/// Whether the temporary's name still refers exactly to the file the engine created: same inode,
/// one link.
fn still_ours(tmp: &Temp) -> bool {
    tmp.path
        .symlink_metadata()
        .is_ok_and(|m| m.file_type().is_file() && (m.dev(), m.ino()) == tmp.id && m.nlink() == 1)
}

/// Verify the temporary is still the engine's own, then move it into place and report its size. On
/// any refusal the temporary is removed — only if it is still the engine's inode.
fn publish(tmp: &Temp, target: &Path, replace: bool) -> Result<u64, ErrorPayload> {
    if !still_ours(tmp) {
        remove_ours(tmp);
        return Err(forbidden(
            "the snapshot's temporary file was replaced or gained another link while it was \
             written; refusing to publish it"
                .to_string(),
        ));
    }
    let bytes = tmp.path.symlink_metadata().map(|m| m.len()).unwrap_or(0);
    let moved = if replace {
        std::fs::rename(&tmp.path, target)
    } else {
        std::fs::hard_link(&tmp.path, target).map(|()| remove_ours(tmp))
    };
    match moved {
        Ok(()) => Ok(bytes),
        Err(e) => {
            remove_ours(tmp);
            Err(if e.kind() == std::io::ErrorKind::AlreadyExists {
                forbidden(
                    "the backup target appeared while the snapshot was being taken; set replace \
                     to swap the new snapshot in"
                        .to_string(),
                )
            } else {
                unsupported(format!(
                    "the snapshot was taken but could not be moved into place: {}",
                    e.kind()
                ))
            })
        }
    }
}

/// Remove the temporary's name only if it still refers to the inode the engine created (any link
/// count). Anything else at that name is left alone: it is not the engine's to delete.
fn remove_ours(tmp: &Temp) {
    let ours = tmp
        .path
        .symlink_metadata()
        .is_ok_and(|m| m.file_type().is_file() && (m.dev(), m.ino()) == tmp.id);
    if !ours {
        return;
    }
    if let Err(e) = std::fs::remove_file(&tmp.path)
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

    /// The temporary for the LONGEST accepted name still fits a 255-byte `NAME_MAX`, and no
    /// temporary name is itself a policy-valid file name (it starts with `.`), so a client can never
    /// name — and so never collide with or `replace` — an in-flight temporary.
    #[test]
    fn tmp_name_fits_name_max_and_is_never_a_valid_file_name() {
        let longest = "x".repeat(MAX_FILE_NAME);
        let tmp = tmp_name(&longest, &[0xab; 16]);
        assert!(tmp.len() <= 255, "{} bytes", tmp.len());
        assert!(check_file_name(&tmp_name("a.db", &[0; 16])).is_err());
        assert_ne!(
            tmp_name("a.db", &[1; 16]),
            tmp_name("a.db", &[2; 16]),
            "the nonce is in the name"
        );
    }

    #[test]
    fn a_failed_snapshot_never_names_the_allowed_directory() {
        let root = Path::new("/srv/app/data");
        let ep = redact_root(
            unsupported("unable to open database: /srv/app/data/.x.db.ferro-backup-ab".to_string()),
            root,
        );
        assert!(!ep.message.contains("/srv/app/data"), "{}", ep.message);
        assert!(
            ep.message.contains("<allowed directory>/.x.db"),
            "{}",
            ep.message
        );
    }

    #[test]
    fn live_database_names_and_their_sidecars_are_listed() {
        assert_eq!(
            live_names("main.db"),
            ["main.db", "main.db-wal", "main.db-shm", "main.db-journal"].map(String::from)
        );
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

    /// The temporary is created EXCLUSIVELY and private, and `remove_ours` never deletes a file
    /// that is not the inode the engine created.
    #[test]
    fn the_temporary_is_exclusive_private_and_only_ours_is_removed() {
        let dir = tempfile::tempdir().unwrap();
        let tmp = create_temp(dir.path(), "snap.db").expect("create");
        let m = tmp.path.symlink_metadata().unwrap();
        assert_eq!(m.mode() & 0o777, 0o600, "the temporary is private");
        assert_eq!(m.len(), 0);
        // Something else at the name — the engine's name unlinked, a stranger's file in its place.
        // The engine still holds its temporary open, so the stranger's file cannot reuse its inode
        // number (without the held handle, tmpfs reused it and this assertion failed).
        std::fs::remove_file(&tmp.path).unwrap();
        std::fs::write(&tmp.path, b"someone else's").unwrap();
        assert!(
            !still_ours(&tmp),
            "a replaced name is not the engine's file"
        );
        remove_ours(&tmp);
        assert_eq!(std::fs::read(&tmp.path).unwrap(), b"someone else's");
    }

    /// The create is EXCLUSIVE: a file already at the name — the review's planted file — is refused
    /// and left exactly as it was, never opened for writing (a non-exclusive create would have
    /// handed `VACUUM INTO` the planted file, and its inode to whoever planted it).
    #[test]
    fn a_planted_file_at_the_temporary_name_is_refused_and_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let planted = dir.path().join(tmp_name("snap.db", &[7; 16]));
        std::fs::write(&planted, b"planted").unwrap();
        let err = create_at(planted.clone())
            .err()
            .expect("an existing name is refused");
        assert_eq!(err.code, ferro_proto::consts::errc::FORBIDDEN);
        assert_eq!(std::fs::read(&planted).unwrap(), b"planted");
    }

    /// A hard link taken to the temporary (the exfiltration the review reproduced) is detected
    /// before publishing: the snapshot is not published, and the engine's name for it is removed.
    #[test]
    fn a_linked_temporary_is_not_published() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let tmp = create_temp(dir.path(), "snap.db").unwrap();
        std::fs::write(&tmp.path, b"snapshot bytes").unwrap();
        std::fs::hard_link(&tmp.path, outside.path().join("leak")).unwrap();
        let target = dir.path().join("snap.db");
        let err = publish(&tmp, &target, false).unwrap_err();
        assert_eq!(err.code, ferro_proto::consts::errc::FORBIDDEN);
        assert!(!target.exists(), "a linked snapshot was published");
        assert!(
            !tmp.path.exists(),
            "the engine's own temporary name was left behind"
        );

        // CONTROL: an unlinked temporary publishes.
        let tmp = create_temp(dir.path(), "snap.db").unwrap();
        std::fs::write(&tmp.path, b"snapshot bytes").unwrap();
        assert_eq!(publish(&tmp, &target, false).unwrap(), 14);
        assert_eq!(std::fs::read(&target).unwrap(), b"snapshot bytes");
        assert!(!tmp.path.exists());
    }

    #[test]
    fn publish_never_overwrites_without_replace_and_swaps_atomically_with_it() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("t.db");
        std::fs::write(&target, b"previous").unwrap();

        let tmp = create_temp(dir.path(), "t.db").unwrap();
        std::fs::write(&tmp.path, b"new snapshot").unwrap();
        let err = publish(&tmp, &target, false).unwrap_err();
        assert_eq!(err.code, ferro_proto::consts::errc::FORBIDDEN);
        assert_eq!(
            std::fs::read(&target).unwrap(),
            b"previous",
            "never overwritten"
        );
        assert!(
            !tmp.path.exists(),
            "a refused publish removes the engine's temporary"
        );

        let tmp = create_temp(dir.path(), "t.db").unwrap();
        std::fs::write(&tmp.path, b"new snapshot").unwrap();
        assert_eq!(publish(&tmp, &target, true).unwrap(), 12);
        assert_eq!(std::fs::read(&target).unwrap(), b"new snapshot");
        assert!(
            !tmp.path.exists(),
            "the temporary name is gone after a rename"
        );
    }
}
