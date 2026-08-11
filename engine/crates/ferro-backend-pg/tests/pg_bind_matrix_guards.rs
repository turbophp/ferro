//! **M1-S8b Task 4, the guards the whole-branch review found missing** (`review/wb-guards.md`).
//!
//! Offline by construction — every predicate under test is pure (`bind::check_param`,
//! `bind::accepts`, `rowmap::oid_extract_type`/`oid_to_tag`), so these run in
//! `cargo test -p ferro-backend-pg` with no Docker and no `FERRO_TEST_PG_URL`. The live half of
//! Task 4 lives next door in `pg_bind_widening_it.rs`, where PostgreSQL itself is the oracle.
//!
//! Three findings are closed here, each one a case where the code was right and nothing could have
//! told us if it stopped being:
//!
//! 1. **The SENTINEL gate was never DOMAIN-resolved by any test.** Keying the refusal on the
//!    DECLARED type (`*ty`) instead of the resolved base (`*base`) left the whole crate green,
//!    because the only `Value::Text` in the cross-product fixture is `"x"` — not a sentinel, so no
//!    input could reach the gate through a domain at all. `CREATE DOMAIN d AS date` is the exact
//!    `Kind::Domain` shape S8a exists for, and `information_schema` is full of them.
//! 2. **`.trim()` in both special-literal predicates was load-bearing and untested.** Measured
//!    against the live PG 17 in testkit:
//!    `SELECT ' infinity '::date, '  NaN '::numeric, ' today '::date, E'\ninfinity\t'::timestamp`
//!    → `infinity|NaN|2026-08-11|infinity`. PG's own parsers skip surrounding whitespace, so the
//!    trim is not defensive padding — it IS the guard, and dropping it lets `' infinity '` acquire
//!    the sentinel meaning the gate exists to refuse.
//! 3. **Nothing pinned the widened TEXT bind matrix against the READ side.** See
//!    [`the_text_bind_matrix_is_pinned_against_the_read_table`] for the rule and for what changed
//!    under M1-S8c's TEXT FALLBACK, which made the review's suggested one-line fix an equivalent
//!    mutant.
//!
//! Plus one MINOR from the same journal: the WIDENED `PgText` arm's payload bytes were never
//! inspected — every byte-level proof in the crate covers a different arm.

use bytes::BytesMut;
use ferro_backend_pg::bind::{accepts, check_param, to_boxed_params};
use ferro_backend_pg::rowmap::{oid_extract_type, oid_to_tag};
use ferro_proto::consts::tag;
use ferro_proto::value::Value;
use tokio_postgres::types::{Kind, Type};

/// The five slots the S7/S8b sentinel gate covers — PostgreSQL's four temporal types plus
/// `numeric`, i.e. exactly the types whose input parser turns a bare word into a real value.
fn gated_slots() -> Vec<Type> {
    vec![
        Type::DATE,
        Type::TIME,
        Type::TIMESTAMP,
        Type::TIMESTAMPTZ,
        Type::NUMERIC,
    ]
}

fn domain_over(base: Type, oid: u32, name: &str) -> Type {
    Type::new(
        name.to_string(),
        oid,
        Kind::Domain(base),
        "public".to_string(),
    )
}

/// The refusal text the gate produces, matched on the one phrase that is unique to it. A test that
/// only asserted `is_err()` could not tell the SENTINEL refusal from the wrong-type refusal that
/// sits three lines above it in `check_param`, and those two are exactly what this file must keep
/// apart.
const SENTINEL_PHRASE: &str = "SPECIAL input literals";

fn refusal(v: &Value, ty: &Type) -> Option<String> {
    check_param(v, ty).err()
}

/// **Finding 1: the sentinel gate, through a DOMAIN.**
///
/// `s8a_every_arm_treats_a_domain_exactly_as_its_base` already compares `accepts(v, base)` with
/// `accepts(v, dom)` over the full cross product — but `accepts` is not where this gate lives
/// (it is a VALUE-aware refusal inside `check_param`), and the fixture's only TEXT entry is the
/// non-sentinel `"x"`. So the base-vs-domain comparison is made here, on inputs that can actually
/// reach the gate, over the whole sentinel vocabulary of both predicates.
///
/// The mutation this exists to kill is `let refused = match *base` → `match *ty`: a domain over
/// `date` then matches no arm, `refused` is false, and the bare string `'infinity'` is bound as
/// text into a `date` slot where PG's parser turns it into the INFINITY SENTINEL — a silent wrong
/// value written, which is the failure class this engine exists to refuse.
#[test]
fn the_sentinel_gate_treats_a_domain_exactly_as_its_base() {
    // Every word both predicates know, in the spellings they accept, plus values that must NOT be
    // refused — without those the test would also pass for a gate that refuses everything.
    let sentinels = [
        "infinity",
        "-infinity",
        "+infinity",
        "Infinity",
        "INFINITY",
        "now",
        "today",
        "tomorrow",
        "yesterday",
        "epoch",
        "allballs",
        "NaN",
        "nan",
        "inf",
        "-inf",
        "+inf",
    ];
    let innocuous = ["2026-08-05", "12:00:00", "1.25", "x", "infinity pool"];

    for (i, base) in gated_slots().into_iter().enumerate() {
        let dom = domain_over(
            base.clone(),
            900_200 + i as u32,
            &format!("dom_of_{}", base.name()),
        );
        // A domain over a domain: `resolve_domain` walks the chain, and a gate that unwrapped only
        // one level would pass the single-level case and fail here.
        let dom2 = domain_over(
            dom.clone(),
            900_300 + i as u32,
            &format!("dom2_of_{}", base.name()),
        );

        for s in sentinels.iter().chain(innocuous.iter()) {
            let v = Value::Text((*s).to_string());
            let on_base = refusal(&v, &base);
            let on_dom = refusal(&v, &dom);
            let on_dom2 = refusal(&v, &dom2);

            assert_eq!(
                on_base.is_some(),
                on_dom.is_some(),
                "the pre-flight must treat a DOMAIN exactly as its base: {s:?} against {} vs {}",
                base.name(),
                dom.name(),
            );
            assert_eq!(
                on_base.is_some(),
                on_dom2.is_some(),
                "…including a domain over a domain: {s:?} against {} vs {}",
                base.name(),
                dom2.name(),
            );

            // …and refused for the RIGHT REASON. Without this, a gate that refused every Text for
            // every temporal slot (the wrong-type refusal) would satisfy the comparison above.
            if let Some(msg) = &on_base {
                assert!(
                    msg.contains(SENTINEL_PHRASE),
                    "{s:?} against {} must be refused AS A SENTINEL, got: {msg}",
                    base.name(),
                );
                let d = on_dom.as_ref().expect("checked above");
                assert!(
                    d.contains(SENTINEL_PHRASE),
                    "{s:?} against the domain must be refused AS A SENTINEL, got: {d}",
                );
            }
        }

        // The mirror, stated positively: at least one sentinel really is refused for this slot, so
        // the equality above is comparing two refusals rather than two acceptances.
        let one = if base == Type::NUMERIC {
            "NaN"
        } else {
            "infinity"
        };
        assert!(
            refusal(&Value::Text(one.to_string()), &dom).is_some(),
            "{one:?} must be refused for a domain over {}",
            base.name(),
        );
        // …and an innocuous value is NOT refused, so the gate is not simply banning TEXT here.
        let ok = if base == Type::NUMERIC {
            "1.25"
        } else if base == Type::TIME {
            "12:00:00"
        } else {
            "2026-08-05"
        };
        assert_eq!(
            refusal(&Value::Text(ok.to_string()), &dom),
            None,
            "{ok:?} must still bind to a domain over {}",
            base.name(),
        );
    }
}

/// **Finding 2: the `.trim()` in both predicates.**
///
/// PostgreSQL's own input functions skip leading and trailing whitespace — measured against the
/// live PG 17 in testkit, tabs and newlines included — so `' infinity '` in a `date` slot means
/// infinity just as surely as `'infinity'` does. Replace either `s.trim()` with `s` and the padded
/// forms below stop being refused, which is the whole gate defeated by one space.
///
/// Both predicates are covered: `is_pg_special_datetime_literal` (the four temporal slots) and
/// `is_pg_special_numeric_literal` (`numeric`), each with space, tab and newline padding, and each
/// through a DOMAIN as well so the two findings cannot regress independently.
#[test]
fn a_padded_sentinel_is_refused_exactly_like_a_bare_one() {
    let padded_datetime = [
        "  infinity  ",
        "\tinfinity\n",
        " today ",
        "\n-infinity\t",
        " EPOCH ",
    ];
    let padded_numeric = ["  NaN  ", "\tNaN\n", " infinity ", "\n-inf\t", " +Inf "];

    for (i, base) in gated_slots().into_iter().enumerate() {
        let dom = domain_over(
            base.clone(),
            900_400 + i as u32,
            &format!("dom_pad_{}", base.name()),
        );
        let cases: &[&str] = if base == Type::NUMERIC {
            &padded_numeric
        } else {
            &padded_datetime
        };
        for s in cases {
            for ty in [&base, &dom] {
                let msg = refusal(&Value::Text((*s).to_string()), ty).unwrap_or_else(|| {
                    panic!(
                        "the padded sentinel {s:?} must be refused for {} — PostgreSQL's parser \
                         skips surrounding whitespace, so without the trim it acquires the \
                         sentinel meaning",
                        ty.name(),
                    )
                });
                assert!(
                    msg.contains(SENTINEL_PHRASE),
                    "{s:?} against {} must be refused AS A SENTINEL, got: {msg}",
                    ty.name(),
                );
            }
        }
    }

    // The mirror: padding does NOT make an ordinary value refused. Without this the test above is
    // satisfiable by refusing every string that contains whitespace.
    assert_eq!(
        refusal(&Value::Text("  2026-08-05  ".into()), &Type::DATE),
        None
    );
    assert_eq!(
        refusal(&Value::Text("\t1.25\n".into()), &Type::NUMERIC),
        None
    );
}

/// **Finding 3: the widened TEXT bind matrix, pinned against the READ table.**
///
/// The rule, in one sentence: **what a column reads as decides what a bare canonical `TEXT`
/// parameter may bind to.** Every OID falls into exactly one of three classes, keyed on
/// `rowmap::oid_to_tag` — the SAME table the read path uses, not a second hand-written list:
///
/// | read tag | bare TEXT binds? | why |
/// |---|---|---|
/// | `BOOL`, `I64`, `F64`, `BYTES` | **no** | these have narrow, typed binary bind paths (`PgBool`, the S8a `PgInt`/`PgFloat` narrowing, `PgBytes`); admitting text would disable those pre-flights for no caller that exists |
/// | `DECIMAL`, `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMPTZ`, `UUID`, `JSON` | **yes** | D-S8b-4: stock DBAL's type layer stringifies all of these and binds them `ParameterType::STRING`, and PG's text input syntax for them is exactly the canonical text |
/// | `TEXT` | **yes** | D-S8b-6's round trip: a value Ferro READ as text must be writable BACK as text, or the fallback breaks the moment anyone updates a row they selected |
///
/// **What changed since the review, and why its one-line fix is now an equivalent mutant.** The
/// finding proposed adding `Type::TIMETZ` to the NEGATIVE list of the in-`bind.rs` test, on the
/// grounds that `timetz` was `Unsupported` on the read side and admitting it on the write side
/// would create a column Ferro can write but not read. M1-S8c's TEXT FALLBACK (D-S8b-6, landed in
/// this same branch) removed that read-side refusal: `timetz` now reads as `TAG_TEXT` and
/// `is_unmapped_text_fallback_target` therefore ALREADY admits it for binding. Adding it to
/// `is_text_input_target` today changes no observable behaviour at all (measured — see the fix
/// journal), so the write-only-column hazard it named is closed by construction and a test of that
/// list membership would be untestable by definition. The rule above is what is left to pin, it
/// covers the original hazard (a type readable only as text must stay bindable as text, and a type
/// with a narrow binary path must stay unreachable from bare text), and unlike the list check it
/// has live mutations that redden it.
#[test]
fn the_text_bind_matrix_is_pinned_against_the_read_table() {
    // Every canonical mapping in `rowmap`, plus five OIDs on the TEXT FALLBACK side of the table
    // (four builtin, one array) so both halves of the rule have inputs.
    let all = [
        Type::BOOL,
        Type::INT2,
        Type::INT4,
        Type::INT8,
        Type::FLOAT4,
        Type::FLOAT8,
        Type::TEXT,
        Type::VARCHAR,
        Type::BPCHAR,
        Type::NAME,
        Type::CHAR,
        Type::BYTEA,
        Type::NUMERIC,
        Type::DATE,
        Type::TIME,
        Type::TIMESTAMP,
        Type::TIMESTAMPTZ,
        Type::UUID,
        Type::JSON,
        Type::JSONB,
        Type::OID,
        Type::REGTYPE,
        Type::REGCLASS,
        Type::TIMETZ,
        Type::INTERVAL,
        Type::INET,
        Type::XML,
        Type::INT4_ARRAY,
    ];

    let probe = Value::Text("1".to_string());
    let mut seen_text_fallback = 0;
    let mut seen_widened = 0;
    let mut seen_narrow = 0;

    for ty in all {
        let binds = accepts(&probe, &ty);
        let t = oid_to_tag(ty.oid());
        match t {
            tag::BOOL | tag::I64 | tag::F64 | tag::BYTES => {
                seen_narrow += 1;
                assert!(
                    !binds,
                    "{} reads as tag {t} (a narrow binary bind path), so a bare canonical TEXT \
                     must NOT bind to it — admitting it disables the typed pre-flight that makes \
                     a §19.3 bind error a KNOWN fate",
                    ty.name(),
                );
            }
            tag::DECIMAL
            | tag::DATE
            | tag::TIME
            | tag::TIMESTAMP
            | tag::TIMESTAMPTZ
            | tag::UUID
            | tag::JSON => {
                seen_widened += 1;
                assert!(
                    binds,
                    "{} is a D-S8b-4 widened slot (stock DBAL stringifies this type and binds it \
                     ParameterType::STRING), so a bare canonical TEXT must bind to it",
                    ty.name(),
                );
            }
            tag::TEXT => {
                // THE ROUND TRIP (D-S8b-6). One documented exception, measured at HEAD:
                // `"char"` (OID 18) reads through `ExtractType::CharByte` as TAG_TEXT but binds
                // nothing at all — no canonical value reaches it, so it is READ-ONLY through
                // Ferro. Recorded in the fix journal as a follow-up rather than silently widened
                // here: widening a bind is an engine change, and this file may not make one.
                if ty == Type::CHAR {
                    assert!(
                        !binds,
                        "if `\"char\"` became bindable, delete this exception — the rule is that \
                         everything read as TEXT binds as TEXT",
                    );
                    continue;
                }
                if oid_extract_type(ty.oid()).is_none() {
                    seen_text_fallback += 1;
                }
                assert!(
                    binds,
                    "{} reads as TAG_TEXT, so a bare canonical TEXT MUST bind back to it — \
                     otherwise a value Ferro handed the application cannot be written back \
                     (D-S8b-6's round trip)",
                    ty.name(),
                );
            }
            other => panic!(
                "{} reads as tag {other}, which this rule does not classify — a new canonical tag \
                 must decide, explicitly, whether a bare TEXT parameter may bind to it",
                ty.name(),
            ),
        }
    }

    // The classes are not empty: a rule whose branches never fire is not a rule.
    assert!(
        seen_narrow >= 8,
        "narrow-bind class under-covered: {seen_narrow}"
    );
    assert!(
        seen_widened >= 8,
        "widened class under-covered: {seen_widened}"
    );
    assert!(
        seen_text_fallback >= 5,
        "TEXT-FALLBACK class under-covered: {seen_text_fallback}",
    );
}

/// **The MINOR from the same journal: the WIDENED `PgText` arm's payload bytes, witnessed.**
///
/// `bind.rs`'s contract for the canonical-text arms is "written verbatim … nothing is re-rendered",
/// and every existing byte-level proof covers a DIFFERENT arm (`PgDecimalText`, `PgInt`/`PgFloat`,
/// or a base-vs-domain comparison that an identical corruption on both sides survives). So the
/// plausible "be helpful" edit — `out.extend_from_slice(self.0.trim().as_bytes())` — is invisible.
///
/// The witness is a value whose bytes a trim would CHANGE and PostgreSQL still accepts: a padded
/// date. (Padded SENTINELS are refused pre-send by the gate above; padded ordinary values are not,
/// which is what makes this observable at all.)
#[test]
fn the_widened_text_arm_writes_its_payload_verbatim() {
    let cases: Vec<(Value, Type)> = vec![
        (Value::Text("  2026-08-05  ".to_string()), Type::DATE),
        (Value::Text("\t1.2500\n".to_string()), Type::NUMERIC),
        (Value::Text(" 12:00:00 ".to_string()), Type::TIME),
        (Value::Text("  {\"a\": 1}  ".to_string()), Type::JSONB),
        // The TEXT FALLBACK arm (M1-S8c) writes verbatim too.
        (Value::Text(" 1 year ".to_string()), Type::INTERVAL),
    ];

    for (v, ty) in cases {
        let Value::Text(expected) = v.clone() else {
            unreachable!()
        };
        let boxed = to_boxed_params(std::slice::from_ref(&v));
        let mut out = BytesMut::new();
        boxed[0]
            .to_sql_checked(&ty, &mut out)
            .unwrap_or_else(|e| panic!("{} must accept the canonical text: {e}", ty.name()));
        assert_eq!(
            out.as_ref(),
            expected.as_bytes(),
            "the canonical text must reach the wire VERBATIM for {} — got {:?}, expected {:?}",
            ty.name(),
            String::from_utf8_lossy(out.as_ref()),
            expected,
        );
    }
}
