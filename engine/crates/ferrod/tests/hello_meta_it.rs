//! M1-S8a Tasks 11/12 — the `HELLO_ACK` pool-metadata gate, against a REAL multi-pool `ferrod`.
//!
//! This is the metadata a DBAL driver reads at connect time: per pool, its NAME, its backend
//! FAMILY (inferred from the DSN scheme, so it is known even for a backend that has never been
//! dialled) and — Task 12 — its SERVER VERSION, learned lazily on the first handshake that asks.
//!
//! Three properties are load-bearing here and each has its own test, because each one looks like a
//! simplification and is not:
//!
//! 1. **The version is CACHED**, not merely stable. `assert_eq!(ack2.pools, ack.pools)` proves
//!    stability — a registry that re-probed on every handshake passes it identically. The
//!    observable is `PoolRegistry::probes_issued()`, a monotonic counter, so a lost cache goes RED.
//! 2. **An unreachable pool never breaks or delays the handshake.** The probe sits INSIDE the
//!    client's own I/O deadline (`Ferro::connect` defaults `ioTimeout` to 5 s and applies it to the
//!    HELLO_ACK read), so the whole call is bounded and every pool is probed concurrently: N
//!    unreachable pools cost what one does.
//! 3. **A failure is remembered only briefly, and a success is not immortal.** A sealed failure
//!    means a backend that comes back needs a daemon restart; a sealed success means a rolling
//!    backend upgrade leaves `ferrod` advertising a pre-restart version that a driver converts
//!    into a PLATFORM choice.
//!
//! Property 3's SUCCESS half — the TTL expiry — is guarded in `pools.rs`'s own `mod tests`, not
//! here, along with three more properties of the same machinery: the probe task's DETACHMENT, the
//! `in_flight` marker surviving a panicking or dropped probe, and the bounded dial. They live there
//! because each needs the probe's timing constants shrunk to millisecond scale (`build_tuned` /
//! `ProbeTuning`, `#[cfg(test)]` and therefore unreachable from this integration crate). See
//! `pools::tests::{the_probe_is_detached_…, an_expired_version_…, a_panicking_probe_…,
//! a_probe_future_dropped_…, a_black_holed_dial_…}`.

mod common;

use common::{mariadb_url, mysql_url, pg_url, pools_server};

/// The metadata a DBAL driver reads at connect time, from a real multi-pool `ferrod`: the kind of
/// each pool, its server version — and, on the second handshake, proof the version came out of the
/// cache rather than off a fresh round trip.
#[tokio::test]
async fn hello_ack_reports_each_pools_kind_and_server_version() {
    let (Some(pg), Some(my)) = (pg_url(), mysql_url()) else {
        return; // each helper prints its own `skip:` line
    };
    let (server, registry) = pools_server(&[("pgpool", pg.as_str()), ("mypool", my.as_str())]);

    let ack = server.connect().await.hello(0).await.ack;

    let pgi = ack
        .pools
        .iter()
        .find(|p| p.name == "pgpool")
        .expect("pgpool advertised");
    assert_eq!(pgi.kind, "postgres");
    let v = pgi
        .server_version
        .as_deref()
        .expect("PG version must be learned");
    assert!(
        v.starts_with("PostgreSQL "),
        "PG reports its version() verbatim, got {v:?}"
    );

    let myi = ack
        .pools
        .iter()
        .find(|p| p.name == "mypool")
        .expect("mypool advertised");
    assert_eq!(myi.kind, "mysql");
    let v = myi
        .server_version
        .as_deref()
        .expect("MySQL version must be learned");
    assert!(
        v.chars().next().is_some_and(|c| c.is_ascii_digit()),
        "MySQL's version() starts with a digit, got {v:?}"
    );

    // ---- Learned ONCE. THIS is the caching assertion, and it is deliberately NOT
    // `assert_eq!(ack2.pools, ack.pools)`: that proves STABILITY, not caching — a registry that
    // re-probed on every handshake would pass it. The probe COUNTER is the observable, so a lost
    // cache goes RED.
    let after_first = registry.probes_issued();
    assert_eq!(after_first, 2, "one probe per pool on the first handshake");

    let ack2 = server.connect().await.hello(0).await.ack;
    assert_eq!(
        ack2.pools, ack.pools,
        "the metadata must be stable across handshakes"
    );
    assert_eq!(
        registry.probes_issued(),
        after_first,
        "a second handshake inside the TTL must issue NO new probe — the cache is real, not just \
         a stable answer"
    );
}

/// MariaDB's `version()` must CONTAIN "mariadb" (case-insensitively) — that substring is how a
/// Doctrine driver selects the MariaDB platform branch, so the engine passing the string through
/// VERBATIM (rather than normalising it) is load-bearing.
#[tokio::test]
async fn hello_ack_mariadb_version_is_distinguishable_from_mysql() {
    let Some(url) = mariadb_url() else {
        return; // prints `skip: FERRO_TEST_MARIADB_URL unset`
    };
    let (server, _registry) = pools_server(&[("maria", url.as_str())]);
    let ack = server.connect().await.hello(0).await.ack;
    let v = ack.pools[0]
        .server_version
        .clone()
        .expect("MariaDB version must be learned");
    assert!(v.to_ascii_lowercase().contains("mariadb"), "got {v:?}");
}

/// THE safety property: the handshake NEVER depends on backend availability, and it never blows the
/// client's I/O deadline. `ferrod` handshakes with unreachable pools; their version is `nil`.
///
/// The THREE dead pools are the point: `Ferro::connect`'s default `ioTimeout` is 5 s and covers the
/// HELLO_ACK read, so a per-pool bound with serial probing would take 3x the per-pool timeout and
/// fail the connect. The whole call is bounded and the pools are probed concurrently, so this
/// handshake completes in roughly ONE budget regardless of how many are down — which is what the
/// elapsed-time assertion pins.
#[tokio::test]
async fn unreachable_pools_still_handshake_with_a_null_version_and_do_not_blow_the_io_deadline() {
    let Some(pg) = pg_url() else {
        return;
    };
    // Port 1 is reserved and refuses instantly on loopback; 10.255.255.x black-holes (measured on
    // this host: a TCP connect there hangs past 12 s), so the dead pools cover BOTH failure shapes
    // — fast refusal and hang-until-budget.
    let refused = "postgres://ferro:ferro@127.0.0.1:1/ferro";
    let blackhole_a = "postgres://ferro:ferro@10.255.255.1:5432/ferro";
    let blackhole_b = "postgres://ferro:ferro@10.255.255.2:5432/ferro";
    let (server, _registry) = pools_server(&[
        ("live", pg.as_str()),
        ("dead1", refused),
        ("dead2", blackhole_a),
        ("dead3", blackhole_b),
    ]);

    let started = std::time::Instant::now();
    let ack = server.connect().await.hello(0).await.ack;
    let elapsed = started.elapsed();

    let live = ack.pools.iter().find(|p| p.name == "live").unwrap();
    assert!(
        live.server_version.is_some(),
        "a reachable pool still reports its version"
    );
    for name in ["dead1", "dead2", "dead3"] {
        let d = ack.pools.iter().find(|p| p.name == name).unwrap();
        assert_eq!(
            d.kind, "postgres",
            "the KIND is known from the DSN scheme regardless"
        );
        assert_eq!(
            d.server_version, None,
            "an unreachable pool reports nil, it does not fail"
        );
    }
    assert!(
        elapsed < std::time::Duration::from_secs(4),
        "THREE unreachable pools must cost roughly ONE probe budget, not three: the client's \
         default ioTimeout is 5s and covers this read. Took {elapsed:?}"
    );
}

/// A failed probe is remembered for a SHORT window, not forever — so a backend that comes back is
/// picked up without a daemon restart, and a backend that stays down does not cost every handshake
/// a full probe budget.
#[tokio::test]
async fn a_failed_probe_is_retried_after_the_backoff_and_never_sealed() {
    let Some(pg) = pg_url() else {
        return;
    };
    let (server, registry) = pools_server(&[("dead", "postgres://ferro:ferro@127.0.0.1:1/ferro")]);
    let _ = pg; // the reachable DSN is only needed to gate the live lane

    let a = server.connect().await.hello(0).await.ack;
    assert_eq!(a.pools[0].server_version, None);
    let after_first = registry.probes_issued();
    assert_eq!(after_first, 1);

    // Inside the backoff: NO new probe (this is what stops a down backend costing every handshake).
    let _ = server.connect().await.hello(0).await;
    assert_eq!(
        registry.probes_issued(),
        after_first,
        "a failure inside the backoff window must not re-probe"
    );

    // After it: a new probe IS issued. `VERSION_RETRY_BACKOFF` is 5s; sleep just past it.
    tokio::time::sleep(std::time::Duration::from_millis(5_200)).await;
    let _ = server.connect().await.hello(0).await;
    assert!(
        registry.probes_issued() > after_first,
        "a failure must NEVER be sealed — after the backoff the probe must run again"
    );
}

/// A backend that HANGS (rather than refusing) must be probed ONCE, not once per handshake.
///
/// The refusal shape is covered by the backoff test above: the probe resolves fast, records a
/// failure, and the backoff suppresses the next one. A BLACK HOLE never resolves inside the
/// handshake budget, so there is no failure to back off from — without an explicit in-flight
/// marker, every handshake would start ANOTHER probe and every handshake would pay the full budget.
/// That is the FPM-reconnect-storm shape after a `boot_epoch` change (SPEC §19.1).
///
/// Both halves are asserted: the counter does not grow, AND the second handshake is fast (it is not
/// merely reusing an in-flight probe's WAIT — it does not wait at all).
#[tokio::test]
async fn a_hanging_backend_is_probed_once_not_once_per_handshake() {
    let Some(pg) = pg_url() else {
        return;
    };
    let _ = pg; // the reachable DSN only gates the live lane; this test needs no live backend
    let (server, registry) = pools_server(&[(
        "blackhole",
        "postgres://ferro:ferro@10.255.255.1:5432/ferro",
    )]);

    let first = server.connect().await.hello(0).await.ack;
    assert_eq!(first.pools[0].server_version, None);
    assert_eq!(
        registry.probes_issued(),
        1,
        "one probe on the first handshake"
    );

    let started = std::time::Instant::now();
    let second = server.connect().await.hello(0).await.ack;
    let elapsed = started.elapsed();
    assert_eq!(second.pools[0].server_version, None);
    assert_eq!(
        registry.probes_issued(),
        1,
        "a probe already in flight must not be duplicated by the next handshake"
    );
    assert!(
        elapsed < std::time::Duration::from_millis(500),
        "the second handshake must not wait on the in-flight probe at all, took {elapsed:?}"
    );
}
