//! **M3-D2d: the engine runs checked SQL by `query_id`, and refuses a client built against another
//! manifest.** SPEC §11, `ferrod::manifest`.
//!
//! Everything goes through a real session over a real socket against two SQLite pools (`default`
//! and `reports`, two different database FILES, so "ran on the wrong pool" is observable as "the
//! table is not there"). SQLite needs no server, so this runs everywhere, CI included.

mod common;

use std::sync::Arc;

use common::{TestClient, TestServer, assert_session_alive, exec, exec_err, exec_ok, req};
use ferro_manifest::{Manifest, Query};
use ferro_proto::consts::{
    TYPE_REGISTRY_HASH, errc, feature_engine, flags, method_core, method_sql, method_tx, service,
};
use ferro_proto::header::Header;
use ferro_proto::messages::sql::ExecRequest;
use ferro_proto::messages::tx::{BeginRequest, BeginResponse};
use ferro_proto::messages::{Hello, HelloAck, Outcome};
use ferro_proto::value::Value;
use ferrod::config::{Config, PoolSpec};
use ferrod::epoch::BootEpoch;
use ferrod::manifest::LoadedManifest;
use ferrod::pools::PoolRegistry;
use ferrod::services::sql;
use ferrod::session::codec::OutFrame;
use ferrod::tx::TxRegistry;

fn query(sql: &str, pool: &str, readonly: bool) -> Query {
    Query {
        sql: sql.into(),
        pool: pool.into(),
        readonly,
        idempotent: false,
        dto: None,
        source: None,
    }
}

fn manifest() -> Manifest {
    let mut m = Manifest::new();
    for (id, q) in [
        ("t.count", query("SELECT count(*) FROM t", "default", true)),
        (
            "t.add",
            query("INSERT INTO t(v) VALUES (?1)", "default", false),
        ),
        (
            "r.add",
            query("INSERT INTO r(v) VALUES (?1)", "reports", false),
        ),
        (
            "r.all",
            query("SELECT v FROM r ORDER BY v", "reports", true),
        ),
    ] {
        m.insert(id.into(), q).unwrap();
    }
    m
}

/// A server with pools `default` and `reports` (two SQLite files), and `manifest` loaded if given.
fn server(dir: &tempfile::TempDir, manifest: Option<Manifest>) -> TestServer {
    let spec = |name: &str| PoolSpec {
        name: name.into(),
        dsn: format!(
            "sqlite://{}",
            dir.path().join(format!("{name}.db")).display()
        ),
        kind: ferrod::config::PoolKind::Sqlite,
        pin_functions: Vec::new(),
        pin_on_unknown: true,
        allow_dir: None,
    };
    let mut config = Config {
        pools: vec![spec("default"), spec("reports")],
        ..Config::default()
    };
    config.manifest = manifest.map(|m| LoadedManifest::from_manifest(m, &config.pools).unwrap());
    let registry = PoolRegistry::build(&config);
    let tx_registry = Arc::new(TxRegistry::new(config.drain_deadline));
    let factory = sql::make_handler(
        registry.clone(),
        tx_registry.clone(),
        config.idle_in_tx,
        config.max_tx,
        config.tx_teardown_timeout,
    );
    TestServer::spawn_with_factory_and_config(BootEpoch(1), config, registry, tx_registry, factory)
}

fn hello_frame(manifest_hash: Option<String>) -> OutFrame {
    let payload = Hello {
        client_version: 1,
        type_registry_hash: TYPE_REGISTRY_HASH.to_string(),
        manifest_hash,
        pid: std::process::id(),
        features: 0,
    }
    .encode();
    OutFrame {
        header: Header {
            flags: 0,
            service: service::CORE,
            method: method_core::HELLO,
            request_id: 1,
            payload_len: payload.len() as u32,
        },
        payload: payload.into(),
    }
}

/// HELLO with `manifest_hash`; `Ok(ack)` or the session-fatal error's message.
async fn hello(client: &mut TestClient, manifest_hash: Option<String>) -> Result<HelloAck, String> {
    client.send(hello_frame(manifest_hash)).await;
    let f = client.recv().await;
    if f.header.method == method_core::HELLO_ACK {
        return Ok(HelloAck::decode(&f.payload).unwrap());
    }
    assert_eq!(
        f.header.request_id, 0,
        "a refused handshake is session-fatal (rid 0)"
    );
    assert_eq!(f.header.flags & flags::END, flags::END);
    match Outcome::decode(&f.payload).unwrap() {
        Outcome::Error(ep) => {
            assert_eq!(ep.code, errc::UNSUPPORTED, "{ep:?}");
            client.recv_eof().await;
            Err(ep.message)
        }
        other => panic!("{other:?}"),
    }
}

fn by_id(id: &str, pool: &str, readonly: bool, params: Vec<Value>) -> ExecRequest {
    ExecRequest {
        pool: pool.into(),
        sql: None,
        query_id: Some(id.into()),
        params,
        readonly,
        fetch: 0,
        ..req("")
    }
}

fn inline(sql: &str, pool: &str) -> ExecRequest {
    ExecRequest {
        pool: pool.into(),
        readonly: false,
        ..req(sql)
    }
}

async fn setup(client: &mut TestClient) {
    exec_ok(client, 2, &inline("CREATE TABLE t(v TEXT)", "default")).await;
    exec_ok(client, 3, &inline("CREATE TABLE r(v TEXT)", "reports")).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_handshake_admits_the_same_manifest_or_none_and_refuses_another() {
    let dir = tempfile::tempdir().unwrap();
    let srv = server(&dir, Some(manifest()));
    let hash = manifest().hash();

    // The same manifest: admitted, and the ack advertises MANIFEST.
    let ack = hello(&mut srv.connect().await, Some(hash.clone()))
        .await
        .unwrap();
    assert_eq!(
        ack.features & u32::from(feature_engine::MANIFEST),
        u32::from(feature_engine::MANIFEST)
    );
    // No claim: admitted (an inline-SQL client knows nothing to disagree about).
    hello(&mut srv.connect().await, None).await.unwrap();

    // Another manifest: refused at connect, before any query id can run with stale declarations.
    let mut other = manifest();
    other.queries.get_mut("t.add").unwrap().idempotent = true;
    let err = hello(&mut srv.connect().await, Some(other.hash()))
        .await
        .unwrap_err();
    assert!(err.contains("manifest_hash mismatch"), "{err}");

    // An engine with NO manifest refuses a client that claims one, and does not advertise it.
    let bare = server(&dir, None);
    let ack = hello(&mut bare.connect().await, None).await.unwrap();
    assert_eq!(ack.features & u32::from(feature_engine::MANIFEST), 0);
    let err = hello(&mut bare.connect().await, Some(hash))
        .await
        .unwrap_err();
    assert!(err.contains("no manifest loaded"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_query_id_runs_the_declared_sql_on_the_declared_pool() {
    let dir = tempfile::tempdir().unwrap();
    let srv = server(&dir, Some(manifest()));
    let mut c = srv.connect().await;
    hello(&mut c, Some(manifest().hash())).await.unwrap();
    setup(&mut c).await;

    let ok = exec_ok(
        &mut c,
        4,
        &by_id("t.add", "default", false, vec![Value::Text("a".into())]),
    )
    .await;
    assert_eq!(ok.affected, 1);
    let ok = exec_ok(
        &mut c,
        5,
        &by_id("r.add", "reports", false, vec![Value::Text("b".into())]),
    )
    .await;
    assert_eq!(ok.affected, 1);

    let ok = exec_ok(&mut c, 6, &by_id("t.count", "default", true, vec![])).await;
    assert_eq!(ok.rows, vec![vec![Value::I64(1)]]);
    let ok = exec_ok(&mut c, 7, &by_id("r.all", "reports", true, vec![])).await;
    assert_eq!(
        ok.rows,
        vec![vec![Value::Text("b".into())]],
        "ran on `reports`, not `default`"
    );

    assert_session_alive(&mut c, 70).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_request_that_disagrees_with_the_manifest_is_refused_and_nothing_runs() {
    let dir = tempfile::tempdir().unwrap();
    let srv = server(&dir, Some(manifest()));
    let mut c = srv.connect().await;
    hello(&mut c, None).await.unwrap();
    setup(&mut c).await;

    let refused = |ep: ferro_proto::messages::ErrorPayload, want: &str| {
        assert_eq!(ep.code, errc::UNSUPPORTED, "{ep:?}");
        assert!(ep.message.contains(want), "{}", ep.message);
    };
    // The wrong pool: `r.add` is declared for `reports`.
    let a = Value::Text("x".into());
    refused(
        exec_err(
            &mut c,
            4,
            &by_id("r.add", "default", false, vec![a.clone()]),
        )
        .await,
        "declared for pool `reports`",
    );
    // A write claimed readonly — the claim that would turn an unknown fate into "retryable".
    refused(
        exec_err(&mut c, 5, &by_id("t.add", "default", true, vec![a.clone()])).await,
        "declared readonly=false",
    );
    refused(
        exec_err(&mut c, 6, &by_id("nope.nope", "default", false, vec![])).await,
        "unknown query_id",
    );
    // Both sql and query_id: a malformed request, not an unsupported one.
    let both = ExecRequest {
        sql: Some("SELECT 1".into()),
        ..by_id("t.count", "default", true, vec![])
    };
    assert_eq!(exec_err(&mut c, 7, &both).await.code, errc::PROTOCOL);

    // None of the refused writes ran.
    for (rid, pool, table) in [(8, "default", "t"), (9, "reports", "r")] {
        let ok = exec_ok(
            &mut c,
            rid,
            &inline(&format!("SELECT count(*) FROM {table}"), pool),
        )
        .await;
        assert_eq!(ok.rows, vec![vec![Value::I64(0)]], "{pool}");
    }
    assert_session_alive(&mut c, 71).await;
}

async fn begin(c: &mut TestClient, rid: u32, pool: &str) -> u64 {
    let b = BeginRequest {
        pool: pool.into(),
        isolation: None,
        readonly: false,
    };
    c.send_request(rid, service::TX, method_tx::BEGIN, b.encode())
        .await;
    let t = c.recv().await;
    match Outcome::decode(&t.payload).unwrap() {
        Outcome::Ok(body) => BeginResponse::decode(&body).unwrap().tx_id,
        other => panic!("{other:?}"),
    }
}

/// **The tx-scoped path checks the pool the transaction is PINNED to**, because that path ignores
/// the request's `pool` field: a request naming `default` inside a `reports` transaction would
/// otherwise pass a check against its own claim and run `default`'s query on `reports`.
#[tokio::test(flavor = "multi_thread")]
async fn inside_a_transaction_the_pinned_pool_is_what_must_match() {
    let dir = tempfile::tempdir().unwrap();
    let srv = server(&dir, Some(manifest()));
    let mut c = srv.connect().await;
    hello(&mut c, None).await.unwrap();
    setup(&mut c).await;

    let tx = begin(&mut c, 4, "reports").await;
    let in_tx = |id: &str, claimed_pool: &str, readonly: bool, params: Vec<Value>| ExecRequest {
        tx_id: Some(tx),
        ..by_id(id, claimed_pool, readonly, params)
    };
    // `t.add` is declared for `default`; the request even CLAIMS `default`. Refused: it would run
    // on `reports`.
    let ep = exec_err(
        &mut c,
        5,
        &in_tx("t.add", "default", false, vec![Value::Text("x".into())]),
    )
    .await;
    assert_eq!(ep.code, errc::UNSUPPORTED);
    assert!(
        ep.message
            .contains("declared for pool `default`, but this request runs on pool `reports`"),
        "{}",
        ep.message
    );
    // A `reports` query runs in it, whatever the request's `pool` field says.
    let ok = exec_ok(
        &mut c,
        6,
        &in_tx("r.add", "default", false, vec![Value::Text("y".into())]),
    )
    .await;
    assert_eq!(ok.affected, 1);
    assert_session_alive(&mut c, 72).await;
}

/// A streamed fetch takes the same resolution (the stream producer is a separate code path).
#[tokio::test(flavor = "multi_thread")]
async fn a_streamed_query_id_runs_the_manifest_sql() {
    let dir = tempfile::tempdir().unwrap();
    let srv = server(&dir, Some(manifest()));
    let mut c = srv.connect().await;
    hello(&mut c, None).await.unwrap();
    setup(&mut c).await;
    exec_ok(
        &mut c,
        4,
        &inline("INSERT INTO r(v) VALUES ('s1'), ('s2')", "reports"),
    )
    .await;

    let streamed = ExecRequest {
        fetch: 2,
        ..by_id("r.all", "reports", true, vec![])
    };
    c.send_request(5, service::SQL, method_sql::EXEC, streamed.encode())
        .await;
    let mut data_frames = 0;
    loop {
        let f = c.recv().await;
        assert_eq!(f.header.request_id, 5);
        if f.header.flags & flags::END != 0 {
            assert!(
                matches!(Outcome::decode(&f.payload).unwrap(), Outcome::Ok(_)),
                "the stream ends Ok"
            );
            break;
        }
        if f.header.flags & flags::STREAM != 0 {
            data_frames += 1;
        }
    }
    assert!(data_frames >= 1, "rows were streamed");

    // And a refused one is refused BEFORE any stream frame.
    let wrong = ExecRequest {
        fetch: 2,
        ..by_id("r.all", "default", true, vec![])
    };
    match exec(&mut c, 6, &wrong).await {
        Outcome::Error(ep) => assert!(ep.message.contains("declared for pool `reports`")),
        other => panic!("{other:?}"),
    }
    assert_session_alive(&mut c, 73).await;
}
