//! ADMIN-service wire messages (service `ADMIN`, M2-C3-7b; SPEC §7.6 + D15). Like the TX messages
//! they are `Value`-free, so they ride the same `msg!`/rmp-serde compact positional layout: a fixarray
//! of their fields in declaration order, `Option<T>` present as a bare `nil` when absent (never
//! omitted). `BackupResponse` is the terminal `Outcome::Ok` body for `BACKUP`, composed via
//! `Outcome::Ok(BackupResponse.encode())` exactly as `BeginResponse` is. Layouts are pinned in
//! `/proto/PROTOCOL.md` §11 and locked by the `admin_*` golden vectors.
//!
//! **No path crosses this wire.** `file` is a plain FILE NAME, and the engine places it in the pool's
//! D14 allowed directory — the client never learns, and never chooses, the directory (§12/D8 keep the
//! database's location in the engine). The engine refuses anything that is not a plain name.

use super::{from_slice, to_vec};
use crate::CodecError;
use serde::{Deserialize, Serialize};

msg!(
    /// `ADMIN`/`BACKUP` request (an OPERATE verb under D15).
    ///
    /// * `pool` — the pool whose database is snapshotted. SQLite pools only in M2.
    /// * `file` — the snapshot's FILE NAME, placed in the pool's D14 allowed directory.
    /// * `replace` — remove an existing regular file of that name first. Without it, an existing
    ///   target is refused (by SQLite itself, which will not overwrite).
    /// * `timeout_ms` — bounds the snapshot; `nil` leaves it unbounded, and `CANCEL` still stops it.
    BackupRequest {
        pool: String,
        file: String,
        replace: bool,
        timeout_ms: Option<u32>
    }
);
msg!(
    /// `ADMIN`/`BACKUP` success body: the snapshot's size in bytes, and the §13 `queue_us`/`exec_us`
    /// split (pool wait vs. the snapshot statement itself).
    BackupResponse {
        bytes: u64,
        queue_us: u64,
        exec_us: u64
    }
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::Outcome;

    #[test]
    fn backup_request_roundtrips_with_and_without_a_timeout() {
        for timeout_ms in [Some(30_000), None] {
            let req = BackupRequest {
                pool: "main".into(),
                file: "nightly.db".into(),
                replace: true,
                timeout_ms,
            };
            let body = req.encode();
            assert_eq!(body[0], 0x94, "BackupRequest is a fixarray(4)");
            assert_eq!(BackupRequest::decode(&body).unwrap(), req);
        }
    }

    #[test]
    fn backup_response_composes_with_outcome_ok() {
        let resp = BackupResponse {
            bytes: 86_458_368,
            queue_us: 12,
            exec_us: 734_001,
        };
        let body = resp.encode();
        assert_eq!(body[0], 0x93, "BackupResponse is a fixarray(3)");
        match Outcome::decode(&Outcome::Ok(body.clone()).encode()).unwrap() {
            Outcome::Ok(recovered) => assert_eq!(BackupResponse::decode(&recovered).unwrap(), resp),
            other => panic!("expected Outcome::Ok, got {other:?}"),
        }
    }

    #[test]
    fn admin_messages_reject_trailing_bytes_and_the_wrong_arity() {
        let mut b = BackupResponse {
            bytes: 1,
            queue_us: 2,
            exec_us: 3,
        }
        .encode();
        b.push(0xc0);
        assert!(matches!(
            BackupResponse::decode(&b),
            Err(CodecError::TrailingBytes(1))
        ));
        // A 3-element body (an older or newer shape) is not a BackupRequest.
        let short = rmp_serde::to_vec(&("main", "nightly.db", true)).unwrap();
        assert!(matches!(
            BackupRequest::decode(&short),
            Err(CodecError::Malformed(_))
        ));
    }
}
