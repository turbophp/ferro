//! **The READ ceiling on a PostgreSQL `numeric`, measured rather than claimed.**
//!
//! ```text
//! docker compose -f testkit/docker-compose.yml up -d
//! FERRO_TEST_PG_URL=postgres://ferro:ferro@127.0.0.1:55432/ferro \
//!   cargo test -p ferro-backend-pg --test pg_numeric_ceiling_it
//! ```
//! Skips (prints, never fails) when `FERRO_TEST_PG_URL` is unset, like every live file here.
//!
//! # Why this file exists
//!
//! Three artifacts state, as a settled fact, that "a 131 072-digit numeric survives". The
//! whole-branch review measured that it does NOT — and, worse, that **no test anywhere asserted
//! the number**, so a claim that had never been executed read exactly like a proven one
//! (`review/wb-guards.md`, MINOR).
//!
//! The mechanism: the engine asks PostgreSQL for `numeric` in the BINARY result format, and that
//! format carries `ndigits` as an **int16**. 131 072 decimal digits is 32 768 base-10000 groups,
//! which overflows to `-32768`, and `pgtext::numeric_to_text` refuses it. So PostgreSQL's own
//! documented maximum-size numeric is unreadable through Ferro today.
//!
//! **Safety is intact and that is the point of pinning it**: the refusal is a LOUD `Backend`
//! (`NonRetryable`) error naming the reason — never a silent truncation, never `Indeterminate`.
//! This file locks the exact boundary so the prose can be corrected against a measurement, and so
//! a future change (asking for TEXT format for oversized numerics, say) is a deliberate act with a
//! red test rather than a silent one.

use std::time::Duration;

use ferro_backend_pg::PgBackend;
use ferro_pool::config::PoolConfig;
use ferro_pool::pool::Pool;
use ferro_proto::consts::tag;
use ferro_proto::value::Value;

fn test_url() -> Option<String> {
    match std::env::var("FERRO_TEST_PG_URL") {
        Ok(u) => Some(u),
        Err(_) => {
            eprintln!("skip: FERRO_TEST_PG_URL unset");
            None
        }
    }
}

fn config() -> PoolConfig {
    PoolConfig {
        max_size: 1,
        checkout_timeout: Duration::from_secs(5),
        max_lifetime: Duration::from_secs(30 * 60),
        reap_interval: None,
        ..PoolConfig::default()
    }
}

/// The boundary, to the digit. `131_068` is the largest all-nines integer that reads back; one more
/// digit is refused. (131 068 = 32 767 base-10000 groups exactly, i.e. `i16::MAX` groups — which is
/// where the int16 `ndigits` runs out.)
#[tokio::test(flavor = "multi_thread")]
async fn the_numeric_read_ceiling_is_the_int16_ndigits_bound_not_pgs_own_maximum() {
    let Some(url) = test_url() else {
        return;
    };
    let pool = Pool::new(PgBackend::new(url), config());
    let mut co = pool.checkout().await.expect("checkout");

    // Below the bound: reads back, byte-exact, as canonical DECIMAL text.
    for digits in [131_060_usize, 131_064, 131_068] {
        let r = co
            .query(&format!("SELECT (repeat('9',{digits}))::numeric"), &[])
            .await
            .unwrap_or_else(|e| panic!("{digits} digits must read: {e:?}"));
        assert_eq!(r.cols[0].tag, tag::DECIMAL);
        match &r.rows[0][0] {
            Value::Decimal(s) => {
                assert_eq!(s.len(), digits, "{digits} digits must survive verbatim");
                assert!(
                    s.bytes().all(|b| b == b'9'),
                    "{digits} digits must be all nines"
                );
            }
            other => panic!("{digits} digits came back as {other:?}"),
        }
    }

    // One digit past it: a LOUD refusal naming the reason. The assertion is on the MESSAGE, not
    // merely on `is_err()`, because "the query failed" would also be satisfied by a connection
    // loss — and the difference between those two is the difference between a NonRetryable and a
    // §19.3 Indeterminate.
    for digits in [131_069_usize, 131_072] {
        let err = co
            .query(&format!("SELECT (repeat('9',{digits}))::numeric"), &[])
            .await
            .expect_err(&format!(
                "{digits} digits is past the int16 ndigits bound and must be refused — if this now \
                 SUCCEEDS the ceiling moved, and the three prose claims about 131072 need \
                 correcting in the same change",
            ));
        let msg = format!("{err:?}");
        assert!(
            msg.contains("negative ndigits"),
            "{digits} digits must be refused for the ndigits overflow, got: {msg}",
        );
    }

    // …and the session is fine afterwards: this is a per-VALUE decode refusal, not a broken link.
    let r = co
        .query("SELECT 1::numeric", &[])
        .await
        .expect("still usable");
    assert_eq!(r.rows[0][0], Value::Decimal("1".to_string()));
}
