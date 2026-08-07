//! M1-S6 task 1 — the BEHAVIORAL fork spike.
//!
//! This is the EVIDENCE that the `CLIENT_SESSION_TRACK` fork (`vendor/mysql-async`) works live, not
//! merely that the accessors exist. It asserts observable wire BEHAVIOR over the FORKED build:
//!
//!   (a) `SET SESSION sort_buffer_size = …` yields a NON-EMPTY `session_state_info()` that decodes a
//!       `SessionStateChange::SystemVariables` tracker naming `sort_buffer_size`. On stock
//!       `mysql_async` (no `CLIENT_SESSION_TRACK` negotiated) this vec is ALWAYS empty — so a
//!       non-empty decode is proof the forked capability bit was advertised and the server honored it.
//!   (b) `START TRANSACTION` + a read toggles `status_flags() & SERVER_STATUS_IN_TRANS` ON, and
//!       `COMMIT` toggles it OFF — the transaction-state authority the pin engine will read.
//!
//! SKIPS cleanly offline (no `FERRO_TEST_MYSQL_URL`). To run live, stand up a MySQL 8 with the
//! server trackers enabled, e.g.:
//!
//! ```text
//! docker run --rm -d --name ferro-mysql-spike \
//!   -e MYSQL_ROOT_PASSWORD=ferro -e MYSQL_DATABASE=ferro \
//!   -e MYSQL_USER=ferro -e MYSQL_PASSWORD=ferro -p 33060:3306 mysql:8 \
//!   --session_track_state_change=ON --session_track_system_variables='*' \
//!   --session_track_transaction_info=STATE
//! FERRO_TEST_MYSQL_URL=mysql://ferro:ferro@127.0.0.1:33060/ferro \
//!   cargo test -p ferro-backend-mysql --test tracker_spike_it -- --nocapture
//! ```

use ferro_backend_mysql::{SessionStateChange, in_transaction, session_state_changes};
use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Opts};

#[tokio::test]
async fn session_track_fork_surfaces_trackers_live() {
    let Some(url) = std::env::var("FERRO_TEST_MYSQL_URL").ok() else {
        eprintln!("skip: FERRO_TEST_MYSQL_URL unset (session_track_fork_surfaces_trackers_live)");
        return;
    };

    let opts = Opts::from_url(&url).expect("FERRO_TEST_MYSQL_URL is a valid mysql:// url");
    let mut conn = Conn::new(opts).await.expect("connect to live MySQL");
    println!("[spike] connected: conn id = {}", conn.id());

    // ---- (a) session-tracker FORK PROOF: SET SESSION emits a non-empty tracker -----------------
    conn.query_drop("SET SESSION sort_buffer_size = 262144")
        .await
        .expect("SET SESSION sort_buffer_size");

    let changes = {
        let ok = conn
            .last_ok_packet()
            .expect("SET SESSION produced an OK packet");
        session_state_changes(ok)
    };

    println!(
        "[spike] decoded {} session-state tracker(s):",
        changes.len()
    );
    for c in &changes {
        println!("[spike]   {c:?}");
    }
    assert!(
        !changes.is_empty(),
        "FORK PROOF FAILED: session_state_info() is EMPTY after SET SESSION — CLIENT_SESSION_TRACK \
         was not negotiated (stock mysql_async behavior). The fork is not in effect."
    );

    let found_sysvar = changes.iter().any(|c| match c {
        SessionStateChange::SystemVariables(vars) => vars
            .iter()
            .any(|v| v.name_str() == "sort_buffer_size" && v.value_str() == "262144"),
        _ => false,
    });
    assert!(
        found_sysvar,
        "expected a SystemVariables tracker naming sort_buffer_size=262144; got {changes:?}"
    );
    println!("[spike] (a) OK — non-empty SystemVariables tracker for sort_buffer_size=262144");

    // ---- (b) transaction-state authority: SERVER_STATUS_IN_TRANS toggles -----------------------
    conn.query_drop("START TRANSACTION")
        .await
        .expect("START TRANSACTION");
    conn.query_drop("SELECT 1").await.expect("read inside tx");
    let in_tx = {
        let ok = conn
            .last_ok_packet()
            .expect("read produced a trailing OK packet");
        println!("[spike] in-tx status_flags = {:?}", ok.status_flags());
        in_transaction(ok)
    };
    assert!(
        in_tx,
        "expected SERVER_STATUS_IN_TRANS SET after START TRANSACTION + read"
    );
    println!("[spike] (b1) OK — SERVER_STATUS_IN_TRANS SET inside transaction");

    conn.query_drop("COMMIT").await.expect("COMMIT");
    let still_in_tx = {
        let ok = conn.last_ok_packet().expect("COMMIT produced an OK packet");
        println!("[spike] post-commit status_flags = {:?}", ok.status_flags());
        in_transaction(ok)
    };
    assert!(
        !still_in_tx,
        "expected SERVER_STATUS_IN_TRANS CLEARED after COMMIT"
    );
    println!("[spike] (b2) OK — SERVER_STATUS_IN_TRANS CLEARED after COMMIT");

    conn.disconnect().await.expect("clean disconnect");
    println!("[spike] PASS — CLIENT_SESSION_TRACK fork surfaces live trackers");
}
