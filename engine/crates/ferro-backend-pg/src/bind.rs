//! Canonical [`Value`] params → tokio-postgres `ToSql`, for every M0 scalar incl. `Null` and
//! `Bytes`, plus the eight M1-S7 canonical tags.
//!
//! Each param becomes a `Box<dyn ToSql + Sync + Send>` (which `BorrowToSql` accepts), so the boxed Vec
//! can be handed straight to `Client::query_raw` as an `ExactSizeIterator`. `Value::Null` is the
//! subtle one: a NULL has no canonical Rust type, so it is bound via [`PgNull`], a `ToSql` that
//! `accepts` EVERY type and always writes `IsNull::Yes`. That sidesteps the usual "which
//! `Option::<T>::None`?" problem — with a prepared statement PG has already fixed each param's
//! type, and `PgNull` writes a typed NULL slot for whatever that type is.
//!
//! ## M1-S7: the canonical tags bind as TEXT-format params
//!
//! The eight tags added in M1-S7 carry **canonical text** (`proto/PROTOCOL.md` §3.2), and each one
//! binds through its **own** newtype ([`PgDecimalText`] … [`PgJsonText`]) that writes that text
//! verbatim in PG's **text** wire format. Two properties make this the right shape:
//!
//! - **Text format is per-param selectable** (hazard 17). The vendored fork builds a per-param
//!   format array from `ToSql::encode_format` (`vendor/tokio-postgres/src/query.rs:305-308`) even
//!   though the RESULT format is hardcoded binary (`:324`). Sending text lets PG's own input parser
//!   do the work — no hand-written base-10000 NUMERIC encoder, no 2000-epoch date arithmetic — and
//!   it is exactly why a display scale (`1.10` ≠ `1.1`), a 131 072-digit numeric, `NaN` and the
//!   ±`infinity` sentinels all survive a bind untouched: nothing re-renders them.
//! - **One newtype PER TAG, each with a NARROW `accepts`** (hazard 19 / F17). See
//!   [`pg_canonical_text_param`] for why a single shared newtype would silently disable the §19.3
//!   pre-flight for all eight tags at once.
//!
//! ## M1-S8a: a parameter's DOMAIN is resolved to its base, on BOTH sides of EVERY arm
//!
//! PG resolves a domain to its base in the `RowDescription` but **not** in `stmt.params()`, which
//! reports the domain's own oid — so a value read out of a `CREATE DOMAIN` column could not be
//! bound back into it (SPEC §22.2 (g)). [`resolve_domain`] is the bounded unwrap that closes it,
//! and the rule is **resolve in both `accepts` and `to_sql`, in every arm, or in neither**:
//! `postgres-types` has ZERO `Kind::Domain` handling, so resolving only in the pre-flight while
//! delegating `Bool`/`Text`/`Bytes` to its own impls would make the pre-flight LOOSER than the impl
//! it fronts — §19.3's forbidden direction, and a false `Indeterminate` on a write that never left
//! the process. Hence [`PgBool`]/[`PgText`]/[`PgBytes`].
//!
//! There is **no `unreachable!()` in this module**: every `Value` variant has a real box, so a
//! caller that skipped the `accepts` pre-flight would get a typed `WrongType` error from
//! `to_sql_checked`, never a daemon panic.

use ferro_proto::value::Value;
use tokio_postgres::types::{Format, IsNull, ToSql, Type, to_sql_checked};

/// Maximum DOMAIN nesting the parameter-type resolver will unwrap. PG itself allows a domain over a
/// domain; the depth is bounded here so a pathological (or hostile, or simply cyclic-by-bug) `Type`
/// can never spin the daemon inside a pre-flight. A `Type` nested deeper than this falls through to
/// the ordinary "cannot bind" refusal — loud and known-fate, never a hang.
const MAX_DOMAIN_DEPTH: usize = 8;

/// Resolve a PARAMETER's declared `Type` to the type the bind must actually satisfy.
///
/// PG resolves a domain to its base when it builds a `RowDescription` (`printtup.c` →
/// `getBaseTypeAndTypmod`), which is why READS need no unwrap at all. It does NOT do that for
/// `stmt.params()`: a parameter slot reports the DOMAIN's own oid, so a `Type`-identity match
/// refuses binding the very value just read back out of that column (SPEC §22.2 (g)). A
/// user-defined domain column (`CREATE DOMAIN positive_int AS int4 CHECK (VALUE > 0)`) is the shape
/// this exists for.
///
/// **It must be applied on BOTH sides of every arm** — in [`check_param`] AND inside the concrete
/// `ToSql` each `Value` boxes as. `postgres-types` has ZERO `Kind::Domain` handling of its own
/// (measured on PG 17: `<String as ToSql>::accepts(domain_over_text)` is `false`, likewise `bool`
/// over a domain-of-`bool` and `Vec<u8>` over a domain-of-`bytea`), so resolving in the pre-flight
/// while delegating an arm to a raw `postgres-types` impl makes the pre-flight LOOSER than the impl
/// it fronts — the one direction §19.3 forbids. See [`pg_domain_aware_param`].
///
/// It NEVER widens: the base type's own strictness is what the caller then checks against, so a
/// domain over an unsupported base (`timetz`) stays exactly as refused as the bare base is.
fn resolve_domain(ty: &Type) -> &Type {
    let mut cur = ty;
    for _ in 0..MAX_DOMAIN_DEPTH {
        match cur.kind() {
            tokio_postgres::types::Kind::Domain(inner) => cur = inner,
            _ => return cur,
        }
    }
    cur
}

/// A type-agnostic SQL `NULL`. `accepts` returns `true` for any `Type`, and `to_sql` writes no
/// bytes and reports `IsNull::Yes`, so it binds a NULL for whatever type the prepared statement
/// assigned the parameter — no need to know the concrete type at bind time.
///
/// A domain needs no unwrap here: a NULL slot writes no value bytes at all, so there is nothing the
/// base type could change. This is the ONE legitimate universally-true `accepts` in this module.
#[derive(Debug)]
struct PgNull;

impl ToSql for PgNull {
    fn to_sql(
        &self,
        _ty: &Type,
        _out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        Ok(IsNull::Yes)
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    to_sql_checked!();
}

/// Declares a newtype that delegates to `postgres-types`' own `ToSql` for `$inner`, but resolves a
/// DOMAIN to its base type FIRST — in `accepts` **and** in `to_sql`, so the pair stays a mirror.
///
/// **Why a wrapper at all (M1-S8a).** `postgres-types` has ZERO `Kind::Domain` handling: measured
/// live on PG 17, `<String as ToSql>::accepts(domain_over_text)` is `false`, and likewise for
/// `bool`/`Vec<u8>`. `stmt.params()` reports a parameter's DOMAIN oid verbatim (unlike
/// `RowDescription`, which resolves to the base — which is why READS already worked), so without
/// this wrapper a `Value::Text` bound to a `CREATE DOMAIN … AS text` column is refused by the impl.
/// Resolving in the PRE-FLIGHT alone would be worse than not resolving at all: the pre-flight would
/// then be LOOSER than the impl it fronts, `to_sql_checked` would fail with an error carrying no
/// `DbError`, `is_session_fatal` would read that as `ConnectionLost`, and §19.3 would report a
/// **false `Indeterminate`** for a write that was never sent — the precise hazard the pre-flight
/// exists to prevent, created by the fix for it.
macro_rules! pg_domain_aware_param {
    ($(#[$meta:meta])* $name:ident wraps $inner:ty) => {
        $(#[$meta])*
        #[derive(Debug)]
        struct $name($inner);

        impl ToSql for $name {
            fn to_sql(
                &self,
                ty: &Type,
                out: &mut tokio_postgres::types::private::BytesMut,
            ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
                <$inner as ToSql>::to_sql(&self.0, resolve_domain(ty), out)
            }

            fn accepts(ty: &Type) -> bool {
                <$inner as ToSql>::accepts(resolve_domain(ty))
            }

            /// Delegated, resolved, for the same reason `to_sql` is: the FORMAT a param is sent in
            /// must be decided by the type whose encoder actually runs. A no-op today (every
            /// `$inner` here takes the trait's `Format::Binary` default), pinned so a future inner
            /// impl that overrides it cannot silently desync the wrapper from what it wraps.
            fn encode_format(&self, ty: &Type) -> Format {
                <$inner as ToSql>::encode_format(&self.0, resolve_domain(ty))
            }

            to_sql_checked!();
        }
    };
}

pg_domain_aware_param! {
    /// `BOOL` → `bool`, plus a domain over one. Binary format, exactly as the bare `bool` was.
    PgBool wraps bool
}

pg_domain_aware_param! {
    /// `BYTES` → `bytea`, plus a domain over it.
    PgBytes wraps Vec<u8>
}

/// The PG types a canonical `TAG_TEXT` payload may bind to IN ADDITION to the string types
/// `String`'s own `accepts` already covers (`varchar`, `text`, `bpchar`, `name`, `unknown`, plus the
/// name-keyed `citext`/`ltree`/`lquery`/`ltxtquery`).
///
/// **The membership rule is one sentence: PG's TEXT INPUT SYNTAX for this type is exactly what a
/// canonical text payload carries** — which is the same rule the seven [`pg_canonical_text_param`]
/// newtypes assert per tag, and the same thing `pdo_pgsql` relies on for every parameter it sends.
/// `int2`/`int4`/`int8`, `bool`, `float4`/`float8` and `bytea` are deliberately NOT here: the
/// canonical wire forms for those are `I64`/`Bool`/`F64`/`Bytes`, which have their own narrow
/// binary bind paths (the S8a [`PgInt`] narrowing is what made a `serial` primary key work), and
/// admitting text there would disable those pre-flights for no caller that exists.
///
/// A function rather than a `const [Type; 8]`, matching the array-literal-plus-`contains` idiom
/// [`pg_canonical_text_param`] already uses (`[$(Type::$ty),+].contains(resolve_domain(ty))`). It
/// takes the ALREADY-RESOLVED base: all three call sites resolve first, and each for its own
/// reason, so resolving again in here would hide which of them forgot to.
fn is_text_input_target(base: &Type) -> bool {
    [
        Type::NUMERIC,
        Type::DATE,
        Type::TIME,
        Type::TIMESTAMP,
        Type::TIMESTAMPTZ,
        Type::UUID,
        Type::JSON,
        Type::JSONB,
    ]
    .contains(base)
}

/// `TEXT` → the string types (unchanged, delegated, BINARY) **plus** [`is_text_input_target`]'s
/// eight, written verbatim in PG's **text** wire format.
///
/// **Why this is not `pg_domain_aware_param! { PgText wraps String }` any more (M1-S8b).** Two
/// reasons, and the second is a wire bug waiting to happen:
///
///  1. `<String as ToSql>::accepts` admits only the string types, so a stock Doctrine DBAL insert —
///     whose type layer stringifies every `datetime`/`date`/`time`/`decimal`/`json`/`guid` value and
///     binds it as `ParameterType::STRING` — was refused pre-send on EVERY such column. MySQL has no
///     equivalent pre-flight, so the same driver "worked" there and hard-failed here.
///  2. That macro delegates `encode_format`, and `<String as ToSql>` takes the trait's
///     `Format::Binary` default. Widening `accepts` alone would therefore hand PG the UTF-8 bytes of
///     `2026-08-05` and tell it they are a 4-byte BINARY `date`.
///
/// **Both `to_sql` and `encode_format` BRANCH on the resolved base, and the branch is load-bearing
/// in two independent ways.**
///
/// *Correctness:* it is NOT true that "the text-format bytes are the binary-format bytes for every
/// string type this already accepted". `<&str as ToSql>::accepts` (postgres-types-0.2.14
/// `src/lib.rs:1148-1153`) also admits `citext`, `ltree`, `lquery` and `ltxtquery` BY NAME, and for
/// the last three the BINARY form is `0x01 || text` while the text form is bare text
/// (`<&str as ToSql>::to_sql` matches on `ty.name()`). Text == binary holds for
/// `varchar`/`text`/`bpchar`/`name`/`unknown`/`citext` and for nothing else. Keeping the delegated
/// path for those types means this task's regression surface on everything that already worked is
/// EMPTY, rather than "believed harmless".
///
/// *Falsifiability:* that same name-sensitive encoder is what makes clause (3) of
/// `tests::s8a_every_arm_treats_a_domain_exactly_as_its_base` able to fail at all. The `ltree` entry
/// in `tests::every_target_type` was added by S8a's review round precisely because every other entry
/// is bound by an impl that ignores its `Type`, which left the payload-BYTES clause unfalsifiable. A
/// type-blind `to_sql` here would make `ltree` and `dom_of_ltree` write identical bytes BY
/// CONSTRUCTION and quietly revert that fix — measured: the mutation that is RED at HEAD goes GREEN.
///
/// **§19.3 is intact.** [`check_param`]'s `Value::Text` arm delegates to THIS `accepts`, so the
/// pre-flight is bit-identical to the predicate `to_sql_checked` applies — the two cannot drift. And
/// what the widening admits is not an unclassifiable failure: a malformed date text now fails
/// SERVER-side with a real `22007` `DbError`, which `is_session_fatal` reads as non-fatal and
/// `error_map` classifies `NonRetryable`. The direction the rule forbids — a pre-flight LOOSER than
/// its impl — is not what changed; both moved together, in this edit.
#[derive(Debug)]
struct PgText(String);

impl ToSql for PgText {
    /// Verbatim canonical text for the widened targets (nothing is re-rendered, re-parsed or
    /// validated — a round trip through a date/numeric type would lose a display scale or a
    /// sentinel); the unchanged delegated path, **against the RESOLVED base**, for everything
    /// `String` already accepted.
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        let base = resolve_domain(ty);
        if is_text_input_target(base) {
            out.extend_from_slice(self.0.as_bytes());
            return Ok(IsNull::No);
        }
        <String as ToSql>::to_sql(&self.0, base, out)
    }

    fn accepts(ty: &Type) -> bool {
        let base = resolve_domain(ty);
        <String as ToSql>::accepts(base) || is_text_input_target(base)
    }

    /// Text format for THIS param only (the RESULT format stays binary, hazard 17) — and only for
    /// the widened targets. The string types keep the `Format::Binary` they have always had, which
    /// is what `ltree`'s `0x01 || text` payload requires.
    fn encode_format(&self, ty: &Type) -> Format {
        let base = resolve_domain(ty);
        if is_text_input_target(base) {
            Format::Text
        } else {
            <String as ToSql>::encode_format(&self.0, base)
        }
    }

    to_sql_checked!();
}

/// Declares one **canonical-text** `ToSql` newtype (`proto/PROTOCOL.md` §3.2): a `String` wrapper
/// that writes its canonical text VERBATIM in PG's **text** wire format and `accepts` ONLY the PG
/// types named at the call site.
///
/// **Why one newtype per tag (hazard 19 / F17).** [`check_param`] is the §19.3 known-fate pre-flight
/// — `query.rs` runs it BEFORE the statement is sent. The rule is DIRECTIONAL: it may be STRICTER
/// than the concrete `ToSql` it fronts (a clean, diagnosable pre-send rejection), but it must NEVER
/// be looser, because a looser pre-flight lets `to_sql_checked` fail instead — and THAT failure is
/// MISCLASSIFIED (`Error::to_sql(..)` carries no `DbError`, so `is_session_fatal` reads it as a lost
/// connection → §19.3 mints a false `Indeterminate`), which is precisely the path the pre-flight
/// exists to prevent. One SHARED newtype would have to
/// accept the union of every target type the eight tags touch (`numeric ∪ date ∪ time ∪ timestamp ∪
/// timestamptz ∪ uuid ∪ json ∪ jsonb`), disabling the pre-flight for all eight at once: a
/// `Value::Decimal` would sail into a `date` column and fail on the wire. And never copy
/// [`PgNull`]'s `accepts(_ty) -> true` — that is legitimate ONLY for a typed NULL slot, which
/// writes no bytes at all.
macro_rules! pg_canonical_text_param {
    ($(#[$meta:meta])* $name:ident accepts [$($ty:ident),+ $(,)?]) => {
        $(#[$meta])*
        #[derive(Debug)]
        struct $name(String);

        impl ToSql for $name {
            /// The canonical text, byte-for-byte. It is NOT re-rendered, re-parsed or validated
            /// here: the reader already produced the exact form PG's input parser accepts, and any
            /// round trip through a numeric/date type would lose the display scale or a sentinel.
            fn to_sql(
                &self,
                _ty: &Type,
                out: &mut tokio_postgres::types::private::BytesMut,
            ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
                out.extend_from_slice(self.0.as_bytes());
                Ok(IsNull::No)
            }

            /// NARROW by construction — only the types listed at the declaration site, or a DOMAIN
            /// over one of them (M1-S8a). `to_sql` writes the canonical text verbatim and ignores
            /// the `Type` entirely, so it needs no matching change: the two stay a mirror because
            /// the text impl accepts every type its `accepts` admits.
            fn accepts(ty: &Type) -> bool {
                [$(Type::$ty),+].contains(resolve_domain(ty))
            }

            /// Text format for THIS param only; the result format stays binary (hazard 17).
            fn encode_format(&self, _ty: &Type) -> Format {
                Format::Text
            }

            to_sql_checked!();
        }
    };
}

pg_canonical_text_param! {
    /// `DECIMAL` → `numeric` only. PG parses the canonical text itself, so full precision, the
    /// display scale (`1.10` and `1.1` stay distinct) and the `NaN` / `Infinity` / `-Infinity`
    /// payloads all survive — none of which a binary encoder through a fixed-width decimal type
    /// could preserve (hazard 10).
    PgDecimalText accepts [NUMERIC]
}

pg_canonical_text_param! {
    /// `DATE` → `date` only. Never `timestamp`: promoting a date to a timestamp is a guess, and the
    /// `infinity` / `-infinity` sentinels bind as the literals PG itself accepts.
    PgDateText accepts [DATE]
}

pg_canonical_text_param! {
    /// `TIME` → `time` only. Deliberately NOT `timetz` (hazard 15): `timetz` has a 12-byte payload
    /// and no `FromSql`, so it is `Unsupported` on the read side — admitting it here would create a
    /// column Ferro can write but not read back.
    PgTimeText accepts [TIME]
}

pg_canonical_text_param! {
    /// `TIMESTAMP` (NAIVE) → `timestamp` only. Never `timestamptz`: the canonical payload carries no
    /// zone, so binding it to `timestamptz` would make PG apply the session `TimeZone` — a silent
    /// shift. A naive value that genuinely means an instant must arrive as `TIMESTAMPTZ`.
    PgTimestampText accepts [TIMESTAMP]
}

pg_canonical_text_param! {
    /// `TIMESTAMPTZ` (a UTC INSTANT) → `timestamptz` only. The canonical text ends in a literal `Z`,
    /// which PG's parser reads as UTC regardless of the session `TimeZone`. Never `timestamp`:
    /// that would silently drop the zone and store the UTC wall clock as a local one.
    PgTimestampTzText accepts [TIMESTAMPTZ]
}

pg_canonical_text_param! {
    /// `UUID` → `uuid` only. Never `text`: the canonical 36-char lowercase form would then bind to
    /// any string column, defeating the pre-flight for the whole tag.
    PgUuidText accepts [UUID]
}

pg_canonical_text_param! {
    /// `JSON` → `json` AND `jsonb` (the one tag with two legitimate targets — the canonical payload
    /// is the raw document text, which is the text input form of both). Never `text`.
    PgJsonText accepts [JSON, JSONB]
}

/// A canonical `I64` bound against whichever PG integer width the prepared statement inferred
/// (M1-S8a). PG's own `ToSql for i64` accepts `int8` ONLY, so before this every DBAL insert into a
/// `serial`/`int4` PK — and every `$qb->setParameter('id', 5)` against one — was a hard, pre-send
/// `NonRetryable` refusal.
///
/// **Format is BINARY**, not text: PG's param format IS per-param selectable (`encode_format`), but
/// there is nothing to gain here — `<i16/i32/i64 as ToSql>` already writes the exact native binary
/// form, so this delegates rather than re-rendering a decimal string PG would have to re-parse.
///
/// **The range check is NOT here.** It lives in [`check_param`], which sees the VALUE (unlike
/// `ToSql::accepts`, which sees only the `Type`), one step earlier. The reason is **misclassification**,
/// not transmission: `encode_bind_raw` serialises every param into a LOCAL buffer BEFORE `start`
/// writes anything to the socket, so a `to_sql` failure means the statement provably never left the
/// process — but it surfaces as `Error::to_sql(..)`, whose `as_db_error()` is `None`, which
/// `conn.rs`'s `is_session_fatal` reads as a transport failure → `PoolError::ConnectionLost` →
/// which §19.3 turns into `WriteUnconfirmed{Indeterminate}` on a sent, non-readonly, non-in-tx op.
/// A statement that never left the process would then be reported as a write of UNKNOWN fate. The
/// `try_from`s below are therefore a totality backstop for a caller that skipped the pre-flight —
/// they yield a typed `WrongType`-class error, never a panic.
#[derive(Debug)]
struct PgInt(i64);

impl ToSql for PgInt {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        // `to_sql_checked!()` hands us the DECLARED type, which for a domain parameter is the
        // domain itself (`stmt.params()` does not resolve it). `accepts` below resolves
        // identically, so the pair stays a mirror.
        //
        // Equality, not constant patterns — see `check_range` (hazard 57: `Type` is not `Copy`) and
        // the S7 macro's own `[Type::X].contains(ty)` idiom.
        let base = resolve_domain(ty);
        if *base == Type::INT2 {
            i16::try_from(self.0)?.to_sql(base, out)
        } else if *base == Type::INT4 {
            i32::try_from(self.0)?.to_sql(base, out)
        } else if *base == Type::INT8 {
            self.0.to_sql(base, out)
        } else {
            Err(format!("PgInt cannot bind PG type {}", ty.name()).into())
        }
    }

    fn accepts(ty: &Type) -> bool {
        [Type::INT2, Type::INT4, Type::INT8].contains(resolve_domain(ty))
    }

    to_sql_checked!();
}

/// A canonical `F64` bound against `float4` or `float8` (M1-S8a). Same shape and same rationale as
/// [`PgInt`]; the range guard for `float4` lives in [`check_param`].
///
/// **Precision loss inside the REPRESENTABLE `f32` range is ACCEPTED and is not a miscast**: it is
/// the column's own precision, and near the upper boundary PG's own input parser rounds a text
/// literal identically (`3.4028235004135232e38` → the same `f32` in both). What is NOT accepted is a
/// *finite* `f64` that leaves that range in EITHER direction: one that overflows `f32` and becomes
/// `inf`, and one that is non-zero but underflows `f32` and becomes `0`. Both are silent corrupt
/// writes, both are refused pre-send by [`check_param`], and PG itself refuses the corresponding
/// literals with `22003` — measured on PG 17: `'1e-46'::float4` is
/// `"1e-46" is out of range for type real`. The boundary is REPRESENTABILITY, not normality:
/// `'1e-45'` is a legal f32 subnormal that PG accepts and Ferro binds.
#[derive(Debug)]
struct PgFloat(f64);

impl ToSql for PgFloat {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        // The declared type may be a DOMAIN over a float width; resolve on both sides (see
        // [`PgInt::to_sql`] and [`resolve_domain`]).
        let base = resolve_domain(ty);
        if *base == Type::FLOAT4 {
            (self.0 as f32).to_sql(base, out)
        } else if *base == Type::FLOAT8 {
            self.0.to_sql(base, out)
        } else {
            Err(format!("PgFloat cannot bind PG type {}", ty.name()).into())
        }
    }

    fn accepts(ty: &Type) -> bool {
        [Type::FLOAT4, Type::FLOAT8].contains(resolve_domain(ty))
    }

    to_sql_checked!();
}

/// `U64` has **no** PG target type in S7 — PostgreSQL has no unsigned integer type, so there is
/// nothing a `U64` param could bind to without a widening guess (`int8` cannot hold the top half of
/// the range; `numeric` would silently change the column's type semantics). Its `accepts` is
/// therefore `false` for EVERY type: a legitimate, diagnosable known-fate rejection, not an
/// oversight.
///
/// It still exists as a real newtype rather than an `unreachable!()` arm so that `value_to_boxed`
/// stays TOTAL: a caller that somehow skipped the `accepts` pre-flight gets a typed `WrongType`
/// error out of `to_sql_checked`, never a panic reachable from a user-supplied param.
#[derive(Debug)]
struct PgU64Text(String);

impl ToSql for PgU64Text {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        out.extend_from_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }

    /// Accepts NOTHING — see the type docs. This is the strict end of the directional rule
    /// (hazard 19): stricter than any impl can be, so it can never be looser.
    fn accepts(_ty: &Type) -> bool {
        false
    }

    fn encode_format(&self, _ty: &Type) -> Format {
        Format::Text
    }

    to_sql_checked!();
}

/// Converts canonical params into boxed `ToSql` values ready for `query_raw`. Owned (cloned
/// `String`/`Vec<u8>`) so the boxes outlive the query without borrowing the caller's slice.
pub fn to_boxed_params(params: &[Value]) -> Vec<Box<dyn ToSql + Sync + Send>> {
    params.iter().map(value_to_boxed).collect()
}

fn value_to_boxed(v: &Value) -> Box<dyn ToSql + Sync + Send> {
    match v {
        Value::Null => Box::new(PgNull),
        // ---- M1-S8a: DOMAIN RESOLUTION. These three used to box as a bare `bool`/`String`/
        // `Vec<u8>`, whose `postgres-types` impls have no `Kind::Domain` handling at all. With the
        // pre-flight resolving domains, delegating here would make the pre-flight LOOSER than the
        // impl — §19.3's forbidden direction, and via the `to_sql` → `as_db_error()==None` →
        // `ConnectionLost` chain a FALSE `Indeterminate` on a write. See [`pg_domain_aware_param`].
        Value::Bool(b) => Box::new(PgBool(*b)),
        // ---- M1-S8a: NARROWING (and domain resolution). `i64`/`f64` box as the widest PG type only
        // (`int8`/`float8`), so these two go through newtypes that also write `int2`/`int4`/
        // `float4`. The VALUE-aware range gate stays in `check_param`/`check_range`, never here —
        // see [`PgInt`].
        Value::I64(n) => Box::new(PgInt(*n)),
        Value::F64(f) => Box::new(PgFloat(*f)),
        Value::Text(s) => Box::new(PgText(s.clone())),
        Value::Bytes(b) => Box::new(PgBytes(b.clone())),
        // ---- M1-S7 (Task 8b): one text-format newtype PER TAG, each with a NARROW `accepts`
        // mirrored arm-for-arm in `accepts` below — the two MUST move together (a widened
        // `accepts` over a missing box, or vice versa, is the defect this pairing prevents). No
        // `unreachable!()`: every variant boxes, so no user param can reach a panic here.
        Value::U64(n) => Box::new(PgU64Text(n.to_string())),
        Value::Decimal(s) => Box::new(PgDecimalText(s.clone())),
        Value::Date(s) => Box::new(PgDateText(s.clone())),
        Value::Time(s) => Box::new(PgTimeText(s.clone())),
        Value::Timestamp(s) => Box::new(PgTimestampText(s.clone())),
        Value::TimestampTz(s) => Box::new(PgTimestampTzText(s.clone())),
        Value::Uuid(s) => Box::new(PgUuidText(s.clone())),
        Value::Json(s) => Box::new(PgJsonText(s.clone())),
    }
}

/// The §19.3 bind PRE-FLIGHT for one parameter slot: is this the exact bind `query_raw` will
/// perform, and will it succeed? Returns the operator-facing REASON on refusal.
///
/// It MUST mirror `value_to_boxed` arm-for-arm, because `query_raw`'s own `to_sql_checked` calls
/// `accepts` on precisely these concrete types. `query.rs` runs it BEFORE sending the statement so a
/// bind error surfaces as a KNOWN-FATE error (the statement provably never executed).
///
/// The rule is DIRECTIONAL (see the module docs): this may be STRICTER than the concrete `ToSql`
/// impl `value_to_boxed` boxes — which is exactly what [`check_range`] below does — but it must
/// NEVER be looser. A looser pre-flight lets `to_sql_checked` fail instead, and that failure is
/// MISCLASSIFIED: `Error::to_sql(..)` carries no `DbError`, `conn.rs`'s `is_session_fatal` reads
/// that as a transport failure → `PoolError::ConnectionLost` → §19.3 mints a false
/// `WriteUnconfirmed{Indeterminate}` for a write that never happened.
///
/// `Value::Null` accepts every type: it is bound via [`PgNull`], whose `accepts` is `true` for any
/// `Type`, so a NULL never mis-binds.
pub fn check_param(v: &Value, ty: &Type) -> Result<(), String> {
    // M1-S8a: the DOMAIN unwrap. `stmt.params()` reports a parameter's DOMAIN oid verbatim, so
    // every arm here — and every boxed impl behind it — must end up checking the BASE (see
    // [`resolve_domain`]). Both halves are required: `postgres-types` has no `Kind::Domain`
    // handling, so an arm that resolved here and delegated to a raw `<String as ToSql>` would be
    // LOOSER than the impl it fronts, which is §19.3's forbidden direction.
    //
    // **Each arm is handed `ty`, the DECLARED type — never a pre-resolved one.** That is not a
    // detail: `to_sql_checked!()` evaluates `Self::accepts(ty)` on exactly the type `query_raw`
    // read out of `stmt.params()`, so delegating the same `ty` makes this the bit-identical
    // predicate BY CONSTRUCTION. Pre-resolving here first would make the pre-flight unwrap up to
    // `2 × MAX_DOMAIN_DEPTH` levels (once here, once inside the newtype) against the impl's one
    // `MAX_DOMAIN_DEPTH`, so a domain nested `MAX+1 ..= 2×MAX` deep would pass the pre-flight and
    // then be refused by `to_sql_checked` — a LOOSER pre-flight, i.e. a false `Indeterminate`.
    // `s8a_domain_nesting_is_bounded_and_the_bound_refuses` pins it.
    let accepted = match v {
        // The one legitimate universally-true `accepts` (a typed NULL slot writes no value bytes),
        // so there is nothing for the base type to constrain.
        Value::Null => true,
        Value::Bool(_) => <PgBool as ToSql>::accepts(ty),
        // ---- M1-S8a: the NARROWING arms. Each delegates to the newtype `value_to_boxed` boxes it
        // as, so widening one without the other fails the lockstep proof.
        Value::I64(_) => <PgInt as ToSql>::accepts(ty),
        Value::F64(_) => <PgFloat as ToSql>::accepts(ty),
        Value::Text(_) => <PgText as ToSql>::accepts(ty),
        Value::Bytes(_) => <PgBytes as ToSql>::accepts(ty),
        // ---- M1-S7 (Task 8b): each tag delegates to the SAME newtype `value_to_boxed` boxes it
        // as, so the pre-flight is by construction the exact predicate `query_raw`'s own
        // `to_sql_checked` will apply. Every one of these is NARROW (never `PgNull`'s universally
        // true `accepts`), and `U64` is narrow to the point of empty — PG has no unsigned type.
        Value::U64(_) => <PgU64Text as ToSql>::accepts(ty),
        Value::Decimal(_) => <PgDecimalText as ToSql>::accepts(ty),
        Value::Date(_) => <PgDateText as ToSql>::accepts(ty),
        Value::Time(_) => <PgTimeText as ToSql>::accepts(ty),
        Value::Timestamp(_) => <PgTimestampText as ToSql>::accepts(ty),
        Value::TimestampTz(_) => <PgTimestampTzText as ToSql>::accepts(ty),
        Value::Uuid(_) => <PgUuidText as ToSql>::accepts(ty),
        Value::Json(_) => <PgJsonText as ToSql>::accepts(ty),
    };
    // The resolved base is what the VALUE-aware gate and the operator-facing message need. It is
    // the same one unwrap `PgInt::to_sql`/`PgFloat::to_sql` perform, from the same `ty`, so the
    // range check below constrains exactly the width those impls will write.
    let base = resolve_domain(ty);
    // `ty.name()` for the operator ("positive_int"), `base.name()` for the actual constraint
    // ("int4") — a message naming only one of them is unactionable.
    //
    // The middle arm is the PAST-BOUND case (Task 5 fix round 1, F3). When the chain is nested
    // deeper than `MAX_DOMAIN_DEPTH`, `resolve_domain` gives up with `base` still a DOMAIN, and the
    // two-arm form printed `dom_nest_8 (domain over dom_nest_0)` — naming another domain as if it
    // were the base type, and never saying the nesting bound was what refused the bind. Loud and
    // known-fate either way; this only makes it actionable for the operator who hits it.
    let named = if std::ptr::eq(base, ty) {
        ty.name().to_string()
    } else if matches!(base.kind(), tokio_postgres::types::Kind::Domain(_)) {
        format!(
            "{} (domain nesting exceeds the {MAX_DOMAIN_DEPTH}-level resolver bound; resolution \
             stopped at {}, which is itself a domain)",
            ty.name(),
            base.name()
        )
    } else {
        format!("{} (domain over {})", ty.name(), base.name())
    };
    if !accepted {
        return Err(format!(
            "canonical {} cannot bind to PG type {named}",
            value_kind(v),
        ));
    }
    // ---- M1-S8b: the VALUE-aware half of the widened TEXT bind. --------------------------------
    // `PgText::accepts` now admits the types whose input syntax is text, which is what makes the
    // Doctrine tier possible. But PG's parser also turns a handful of BARE WORDS into real values —
    // `infinity` into a timestamp sentinel, `now`/`today` into a clock reading, `NaN` into a numeric
    // — so a string that happens to hold one must not acquire that meaning merely by landing in a
    // temporal or numeric slot. A caller that MEANS a sentinel says so with the tag
    // (`Ferro\Date('infinity')`, `Ferro\Decimal('NaN')`), which still binds.
    //
    // This is a REFUSAL keyed on the SLOT's type. It is NOT content sniffing: nothing here infers a
    // TAG from a payload, and the identical string is accepted without comment for a text column.
    //
    // **Placed HERE — after the `!accepted` return, alongside the range gate below — deliberately.**
    // The two orderings are not equivalent, and the earlier one silently costs a guard. Ahead of the
    // `!accepted` return this refusal fires even for a slot the TEXT tag cannot bind AT ALL, so
    // `check_param(Text("infinity"), DATE)` says SPECIAL whether or not `PgText::accepts` was
    // widened — measured: with `accepts` reverted to `<String as ToSql>::accepts`,
    // `s8b_a_bare_text_sentinel_is_still_refused_for_a_temporal_or_numeric_slot` stayed GREEN,
    // i.e. it could no longer tell "refused because it is a SPECIAL literal" from "refused because
    // the whole tag is banned" — the ONE distinction it exists to make. Behind the return it also
    // reports the PRIMARY reason first: a slot that cannot take a string at all is a wrong-type
    // refusal, not a sentinel one.
    if let Value::Text(s) = v {
        let refused = match *base {
            Type::DATE | Type::TIME | Type::TIMESTAMP | Type::TIMESTAMPTZ => {
                is_pg_special_datetime_literal(s)
            }
            Type::NUMERIC => is_pg_special_numeric_literal(s),
            _ => false,
        };
        if refused {
            return Err(format!(
                "canonical Text {s:?} is one of PostgreSQL's SPECIAL input literals for {}, so \
                 binding it as a bare string would silently give it that meaning; send it with its \
                 own canonical tag instead (Ferro\\Date / Ferro\\Time / Ferro\\NaiveTimestamp / \
                 Ferro\\Decimal), which binds it deliberately",
                base.name()
            ));
        }
    }
    // Task 4's value-aware gate, against the RESOLVED type: a domain over int4 narrows exactly as
    // int4 does.
    check_range(v, base)
}

/// The VALUE-aware half of the pre-flight. `ToSql::accepts` sees only the target type, so a
/// narrowing overflow is invisible to it; caught here the refusal is KNOWN-FATE and pre-send.
///
/// Split out as its own function so [`check_param`] stays one screen and so the domain unwrap has
/// exactly ONE place to pass the resolved base type. `ty` here is therefore ALREADY resolved —
/// [`check_param`] calls [`resolve_domain`] before it reaches this — which is what makes a domain
/// over `int4` narrow exactly as `int4` does rather than falling through every arm as an unknown.
///
/// Spelled with `==` rather than constant patterns: `match (v, *ty)` is E0507 (`Type` is not
/// `Copy`), and `Type` is a non-structural type, so equality is both the compiling and the durable
/// form (hazard 57).
fn check_range(v: &Value, ty: &Type) -> Result<(), String> {
    match v {
        Value::I64(n) => {
            if *ty == Type::INT4 && i32::try_from(*n).is_err() {
                return Err(format!(
                    "canonical I64 value {n} is out of range for PG type int4 \
                     (pre-send rejection: the statement was never executed)"
                ));
            }
            if *ty == Type::INT2 && i16::try_from(*n).is_err() {
                return Err(format!(
                    "canonical I64 value {n} is out of range for PG type int2 \
                     (pre-send rejection: the statement was never executed)"
                ));
            }
            Ok(())
        }
        Value::F64(f) => {
            if *ty == Type::FLOAT4 && f.is_finite() && !(*f as f32).is_finite() {
                return Err(format!(
                    "canonical F64 value {f} is out of range for PG type float4 (it would \
                     silently become infinity; pre-send rejection: the statement was never executed)"
                ));
            }
            // The UNDERFLOW mirror of the overflow arm above (Task 4 review). A finite NON-ZERO
            // magnitude smaller than the smallest f32 subnormal truncates to `0` — a silent corrupt
            // write of the same class, and one PG's own parser refuses: measured on PG 17,
            // `'1e-46'::float4` is `22003 "1e-46" is out of range for type real`. `-0.0 == 0.0` in
            // IEEE, so this catches the negative side too; a genuine `0.0`/`-0.0` input is
            // unaffected (`*f != 0.0` is false for both), and `NaN`/`±inf` never reach here.
            //
            // The boundary is REPRESENTABILITY, not normality: `1e-45` binds, because it lands on
            // the smallest f32 subnormal (`1.401298464324817e-45`) — byte-for-byte what PG stores
            // for the literal `'1e-45'`, which PG accepts.
            //
            // Rendered `{f:e}`, not `{f}` (Task 5 fix round 1, F4): `Display` for `f64` spells a
            // subnormal out in full, so `1e-46` printed as 46 zeros and `f64::MIN_POSITIVE`'s
            // subnormal (`5e-324`) would put ~330 characters of zeros into an operator-facing
            // string. Scientific notation is the only readable form for the magnitudes this arm
            // exists to reject.
            if *ty == Type::FLOAT4 && *f != 0.0 && (*f as f32) == 0.0 {
                return Err(format!(
                    "canonical F64 value {f:e} is out of range for PG type float4 (it would \
                     silently become zero; pre-send rejection: the statement was never executed)"
                ));
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Whether [`check_param`] accepts this pair. Retained as the boolean façade so the directional
/// lockstep proof and every existing call site read the SAME predicate the pre-flight enforces.
pub fn accepts(v: &Value, ty: &Type) -> bool {
    check_param(v, ty).is_ok()
}

/// PostgreSQL's special date/time input literals (`datetime.c`'s `deltatktbl`/`datetktbl` special
/// entries), case-insensitively. Deliberately a CLOSED list rather than a parse attempt: the
/// question is not "is this a valid date" — PG answers that itself, loudly, server-side — but "would
/// binding this bare string silently MEAN something other than a literal date".
fn is_pg_special_datetime_literal(s: &str) -> bool {
    const SPECIALS: [&str; 8] = [
        "infinity",
        "-infinity",
        "+infinity",
        "now",
        "today",
        "tomorrow",
        "yesterday",
        "epoch",
    ];
    let t = s.trim();
    SPECIALS.iter().any(|k| t.eq_ignore_ascii_case(k)) || t.eq_ignore_ascii_case("allballs")
}

/// PostgreSQL's special `numeric` input literals. `NaN` compares unequal to everything including
/// itself, and `Infinity` is unbounded — either one silently acquired by a bare string is a value
/// no application asked for.
fn is_pg_special_numeric_literal(s: &str) -> bool {
    const SPECIALS: [&str; 5] = ["nan", "infinity", "-infinity", "+infinity", "inf"];
    let t = s.trim();
    SPECIALS.iter().any(|k| t.eq_ignore_ascii_case(k))
        || t.eq_ignore_ascii_case("-inf")
        || t.eq_ignore_ascii_case("+inf")
}

/// The canonical-type label for a `Value`, used only to build a clear diagnostic bind-error
/// message ("parameter N: canonical I64 cannot bind to PG type int4 …").
pub fn value_kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "NULL",
        Value::Bool(_) => "BOOL",
        Value::I64(_) => "I64",
        Value::F64(_) => "F64",
        Value::Text(_) => "TEXT",
        Value::Bytes(_) => "BYTES",
        Value::U64(_) => "U64",
        Value::Decimal(_) => "DECIMAL",
        Value::Date(_) => "DATE",
        Value::Time(_) => "TIME",
        Value::Timestamp(_) => "TIMESTAMP",
        Value::TimestampTz(_) => "TIMESTAMPTZ",
        Value::Uuid(_) => "UUID",
        Value::Json(_) => "JSON",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binds_all_m0_scalars() {
        let params = [
            Value::Null,
            Value::Bool(true),
            Value::I64(-200),
            Value::F64(1.5),
            Value::Text("x".to_string()),
            Value::Bytes(vec![0xde, 0xad]),
        ];
        let boxed = to_boxed_params(&params);
        assert_eq!(boxed.len(), 6, "one boxed ToSql per param");
    }

    #[test]
    fn pgnull_accepts_any_type_and_is_null() {
        assert!(<PgNull as ToSql>::accepts(&Type::TEXT));
        assert!(<PgNull as ToSql>::accepts(&Type::INT4));
        let mut buf = tokio_postgres::types::private::BytesMut::new();
        let is_null = PgNull.to_sql(&Type::TEXT, &mut buf).unwrap();
        assert!(matches!(is_null, IsNull::Yes));
        assert!(buf.is_empty(), "a NULL writes no value bytes");
    }

    /// `accepts` mirrors `value_to_boxed`: it is the pre-flight of the exact bind `query_raw`
    /// performs. Since M1-S8a the integer/float arms NARROW (`I64` → `int2`/`int4`/`int8`, `F64` →
    /// `float4`/`float8`) via [`PgInt`]/[`PgFloat`], so the M0 `I64`-vs-`int4` refusal is gone for
    /// an IN-RANGE value; the out-of-range refusal it replaced is pinned by
    /// `s8a_out_of_range_narrowing_is_refused_before_send`. Offline (no Docker) proof of the
    /// COMMIT-1 fix's core predicate.
    #[test]
    fn accepts_mirrors_boxed_binding() {
        // M1-S8a: an in-range I64 binds every PG integer width (the M0 int4/int2 refusal is gone).
        assert!(accepts(&Value::I64(1), &Type::INT8));
        assert!(accepts(&Value::I64(1), &Type::INT4));
        assert!(accepts(&Value::I64(1), &Type::INT2));
        // ...and an F64 binds both float widths.
        assert!(accepts(&Value::F64(1.0), &Type::FLOAT8));
        assert!(accepts(&Value::F64(1.0), &Type::FLOAT4));
        // The straightforward same-type binds accept.
        assert!(accepts(&Value::Bool(true), &Type::BOOL));
        assert!(accepts(&Value::Text("x".to_string()), &Type::TEXT));
        assert!(accepts(&Value::Text("x".to_string()), &Type::VARCHAR));
        assert!(accepts(&Value::Bytes(vec![0xde]), &Type::BYTEA));
        // NULL binds against anything (PgNull::accepts is universally true).
        assert!(accepts(&Value::Null, &Type::INT4));
        assert!(accepts(&Value::Null, &Type::TEXT));
        // A canonical mismatch is caught (Text cannot bind int4).
        assert!(!accepts(&Value::Text("x".to_string()), &Type::INT4));
    }

    /// M1-S8a: a canonical `I64` binds to EVERY PG integer width, and an `F64` to both float widths.
    /// This is the single highest-frequency DBAL blocker — `Types\IntegerType` returns a PHP `int`,
    /// and `IntegerType`/`SmallIntType` map to PG `INT`/`SMALLINT`, so every insert into a
    /// `serial`/`int4` PK and every identifier lookup binds exactly this pair.
    #[test]
    fn s8a_i64_binds_to_every_integer_width_and_f64_to_both_floats() {
        for ty in [Type::INT2, Type::INT4, Type::INT8] {
            assert!(accepts(&Value::I64(42), &ty), "I64 must bind {ty:?}");
        }
        for ty in [Type::FLOAT4, Type::FLOAT8] {
            assert!(accepts(&Value::F64(1.5), &ty), "F64 must bind {ty:?}");
        }
        // Still NARROW: widening the integer arms must not make an int bindable anywhere else.
        for ty in [
            Type::TEXT,
            Type::NUMERIC,
            Type::DATE,
            Type::TIMESTAMP,
            Type::UUID,
            Type::BOOL,
        ] {
            assert!(!accepts(&Value::I64(42), &ty), "I64 must not bind {ty:?}");
            assert!(!accepts(&Value::F64(1.5), &ty), "F64 must not bind {ty:?}");
        }
    }

    /// The range check is a PRE-SEND, known-fate rejection — NOT a `to_sql` failure. A value outside
    /// the target width is refused here, where the statement provably has not been sent, so it can
    /// never mint a false §19.3 `WriteUnconfirmed{Indeterminate}`.
    #[test]
    fn s8a_out_of_range_narrowing_is_refused_before_send() {
        assert!(!accepts(&Value::I64(i64::from(i32::MAX) + 1), &Type::INT4));
        assert!(!accepts(&Value::I64(i64::from(i32::MIN) - 1), &Type::INT4));
        assert!(!accepts(&Value::I64(i64::from(i16::MAX) + 1), &Type::INT2));
        assert!(!accepts(&Value::I64(i64::from(i16::MIN) - 1), &Type::INT2));
        // ...and the in-range boundaries DO bind.
        assert!(accepts(&Value::I64(i64::from(i32::MAX)), &Type::INT4));
        assert!(accepts(&Value::I64(i64::from(i16::MIN)), &Type::INT2));
        // int8 is the full range.
        assert!(accepts(&Value::I64(i64::MAX), &Type::INT8));

        // f64 -> float4: a finite value that OVERFLOWS f32 becomes `inf` — a silent corrupt write.
        assert!(!accepts(&Value::F64(1e39), &Type::FLOAT4));
        assert!(!accepts(&Value::F64(-1e39), &Type::FLOAT4));
        assert!(accepts(&Value::F64(1e38), &Type::FLOAT4));
        // ...and the MIRROR case (Task 4 review): a finite NON-ZERO value that UNDERFLOWS f32
        // becomes `0` — equally silent, equally corrupt, and PG's own parser refuses the identical
        // text literal. Measured on PG 17: `INSERT INTO t(r float4) VALUES ('1e-46')` is
        // `22003 "1e-46" is out of range for type real`, while Ferro stored `0.0` (and `-0.0` for
        // the negative). This is NOT caught by the directional lockstep proof: `check_param` and
        // `to_sql` AGREE to truncate, so the pre-flight is not looser — the hole is semantic.
        assert!(!accepts(&Value::F64(1e-46), &Type::FLOAT4));
        assert!(!accepts(&Value::F64(-1e-46), &Type::FLOAT4));
        // f64::MIN_POSITIVE_SUBNORMAL — the extreme of the same class.
        assert!(!accepts(&Value::F64(5e-324), &Type::FLOAT4));
        // ...while `1e-45` DOES bind: it is representable as an f32 SUBNORMAL, and PG's own parser
        // accepts the literal `'1e-45'` and stores exactly the value Ferro's bind produces
        // (`1.401298464324817e-45`, measured). The refusal must be "underflows to zero", never
        // "is subnormal".
        assert!(accepts(&Value::F64(1e-45), &Type::FLOAT4));
        assert!(accepts(&Value::F64(-1e-45), &Type::FLOAT4));
        // A genuine zero is a genuine zero, in both signs.
        assert!(accepts(&Value::F64(0.0), &Type::FLOAT4));
        assert!(accepts(&Value::F64(-0.0), &Type::FLOAT4));
        // float8 never narrows, so a tiny value is not out of range there either.
        assert!(accepts(&Value::F64(5e-324), &Type::FLOAT8));
        // Non-finite values are representable in BOTH widths and stay bindable.
        assert!(accepts(&Value::F64(f64::INFINITY), &Type::FLOAT4));
        assert!(accepts(&Value::F64(f64::NAN), &Type::FLOAT4));
        // float8 never narrows, so nothing is out of range there.
        assert!(accepts(&Value::F64(1e300), &Type::FLOAT8));
    }

    /// The refusal REASON distinguishes "wrong type" from "out of range" — an operator staring at a
    /// failed insert needs to know which. Both are `Sql{Unsupported}` known-fate rejections.
    #[test]
    fn s8a_check_param_reasons_are_distinct_and_actionable() {
        let too_big = check_param(&Value::I64(i64::from(i32::MAX) + 1), &Type::INT4)
            .expect_err("out of range");
        assert!(too_big.contains("out of range"), "{too_big}");
        assert!(too_big.contains("int4"), "{too_big}");
        assert!(
            too_big.contains("2147483648"),
            "the offending VALUE must be named: {too_big}"
        );

        let wrong_type = check_param(&Value::Text("x".into()), &Type::INT4).expect_err("mismatch");
        assert!(wrong_type.contains("cannot bind"), "{wrong_type}");
        assert!(wrong_type.contains("TEXT"), "{wrong_type}");
        assert!(!wrong_type.contains("out of range"), "{wrong_type}");

        // The UNDERFLOW arm renders the value SCIENTIFICALLY (Task 5 fix round 1, F4). `Display`
        // for `f64` spells a subnormal out in full, so `{f}` printed `1e-46` as 46 zeros and
        // `f64`'s smallest subnormal as ~330 characters of them — technically correct and
        // operationally useless. `{f:e}` is the only readable form for the magnitudes this arm
        // exists to reject.
        let underflow =
            check_param(&Value::F64(5e-324), &Type::FLOAT4).expect_err("underflows float4");
        assert!(
            underflow.contains("5e-324"),
            "the offending value must be named in scientific notation: {underflow}"
        );
        assert!(
            !underflow.contains("0.0000"),
            "...never spelled out digit by digit ({} chars): {underflow}",
            underflow.len()
        );
    }

    /// PG resolves a domain to its BASE type in the `RowDescription` (so READS already work), but
    /// NOT in `stmt.params()` — a parameter slot reports the DOMAIN's own oid. Matching on `Type`
    /// identity therefore refused binding the very value just read back out of that column
    /// (SPEC §22.2 (g)). The pre-flight must check against the base.
    #[test]
    fn s8a_a_domain_parameter_is_checked_against_its_base_type() {
        use tokio_postgres::types::Kind;
        let dom_int4 = Type::new(
            "positive_int".to_string(),
            900_020,
            Kind::Domain(Type::INT4),
            "public".to_string(),
        );
        assert!(
            accepts(&Value::I64(7), &dom_int4),
            "a domain over int4 must accept an I64"
        );
        assert!(
            !accepts(&Value::Text("x".into()), &dom_int4),
            "the base type's strictness must survive the unwrap"
        );
        assert!(
            !accepts(&Value::I64(i64::from(i32::MAX) + 1), &dom_int4),
            "and so must the range gate"
        );

        // Nested: PG allows a domain over a domain.
        let dom_dom = Type::new(
            "small_positive_int".to_string(),
            900_021,
            Kind::Domain(dom_int4.clone()),
            "public".to_string(),
        );
        assert!(accepts(&Value::I64(7), &dom_dom));

        // A domain over an UNSUPPORTED base is still refused — the unwrap widens nothing.
        let dom_timetz = Type::new(
            "tz".to_string(),
            900_022,
            Kind::Domain(Type::TIMETZ),
            "public".to_string(),
        );
        assert!(!accepts(&Value::Time("12:00:00".into()), &dom_timetz));
    }

    /// **The arms the v1 design would have broken (probe 1, blocker B2).** `postgres-types`'
    /// own `ToSql` impls have NO `Kind::Domain` handling, so `<String as ToSql>::accepts`,
    /// `<bool as ToSql>::accepts` and `<Vec<u8> as ToSql>::accepts` are all `false` for a domain
    /// over their base type. Delegating to them behind a domain-resolving pre-flight makes the
    /// pre-flight LOOSER than the impl — the §19.3-forbidden direction, which lands as a false
    /// `Indeterminate` via the `to_sql` → `as_db_error()==None` → `ConnectionLost` chain.
    ///
    /// This test asserts BOTH halves of the mirror explicitly, because a half-applied fix is the
    /// realistic failure and it is invisible to a `bool`-returning `accepts` test alone.
    #[test]
    fn s8a_bool_text_and_bytes_resolve_the_domain_on_both_sides() {
        use tokio_postgres::types::Kind;
        let cases: &[(Value, Type)] = &[
            (
                Value::Text("x".into()),
                Type::new(
                    "dom_text".into(),
                    900_010,
                    Kind::Domain(Type::TEXT),
                    "public".into(),
                ),
            ),
            (
                Value::Bool(true),
                Type::new(
                    "dom_bool".into(),
                    900_011,
                    Kind::Domain(Type::BOOL),
                    "public".into(),
                ),
            ),
            (
                Value::Bytes(vec![0xde, 0xad]),
                Type::new(
                    "dom_bytea".into(),
                    900_012,
                    Kind::Domain(Type::BYTEA),
                    "public".into(),
                ),
            ),
        ];
        for (v, dom) in cases {
            // (1) the PRE-FLIGHT accepts it...
            assert!(
                accepts(v, dom),
                "pre-flight must accept {v:?} against {dom:?}"
            );
            // (2) ...and so does the impl `value_to_boxed` actually boxes. If (1) held without (2)
            // the bind would fail at `to_sql_checked` and be reported as a possibly-applied write.
            let boxed = value_to_boxed(v);
            let mut buf = tokio_postgres::types::private::BytesMut::new();
            assert!(
                boxed.to_sql_checked(dom, &mut buf).is_ok(),
                "the BOXED impl must also resolve the domain for {v:?} / {dom:?} — a pre-flight \
                 that is looser than the impl is the false-Indeterminate path"
            );
        }
        // The unwrap widens nothing: the base type's strictness survives it.
        let dom_text = Type::new(
            "dom_text".into(),
            900_010,
            Kind::Domain(Type::TEXT),
            "public".into(),
        );
        assert!(!accepts(&Value::Bool(true), &dom_text));
        assert!(!accepts(&Value::Bytes(vec![1]), &dom_text));
    }

    /// `depth` nested DOMAINs over `base`, innermost first. PG lets a domain wrap a domain with no
    /// declared limit, so this is the shape [`MAX_DOMAIN_DEPTH`] bounds.
    fn nested_domain(depth: usize, first_oid: u32, base: Type) -> Type {
        use tokio_postgres::types::Kind;
        let mut cur = base;
        for i in 0..depth {
            cur = Type::new(
                format!("dom_nest_{i}"),
                first_oid + i as u32,
                Kind::Domain(cur),
                "public".to_string(),
            );
        }
        cur
    }

    /// **The bound is a REFUSAL, and the pre-flight unwraps EXACTLY as deep as the impl does.**
    ///
    /// Found by mutation-proving [`MAX_DOMAIN_DEPTH`] (Task 5 Step 7 mutation 6), which turned up a
    /// hazard the plan did not predict: if [`check_param`] resolves the domain AND THEN hands the
    /// already-resolved type to a newtype's `accepts` (which resolves again), the pre-flight
    /// effectively unwraps `2 × MAX_DOMAIN_DEPTH` levels while `to_sql_checked` — which is handed
    /// the DECLARED type — unwraps only `MAX_DOMAIN_DEPTH`. For a domain nested `MAX+1 ..= 2×MAX`
    /// deep the pre-flight is then LOOSER than the impl it fronts: §19.3's forbidden direction, and
    /// a false `Indeterminate` on a write. The fix is that `check_param` delegates `accepts` with
    /// the DECLARED type, so its predicate is bit-identical to `to_sql_checked`'s by construction.
    ///
    /// Beyond the bound the answer is a clean known-fate refusal — never a hang, and never a yes.
    #[test]
    fn s8a_domain_nesting_is_bounded_and_the_bound_refuses() {
        // Derived from the const, never hard-coded: moving MAX_DOMAIN_DEPTH moves this test.
        let at_bound = nested_domain(MAX_DOMAIN_DEPTH, 900_100, Type::INT4);
        let past_bound = nested_domain(MAX_DOMAIN_DEPTH + 1, 900_200, Type::INT4);
        assert!(
            accepts(&Value::I64(1), &at_bound),
            "a domain nested exactly MAX_DOMAIN_DEPTH deep must still resolve"
        );
        assert!(
            !accepts(&Value::I64(1), &past_bound),
            "past the bound the resolver gives up — and giving up means REFUSE, not accept"
        );

        // ...and the refusal SAYS SO (Task 5 fix round 1, F3). Past the bound `base` is itself a
        // domain, so the original two-arm message read `dom_nest_8 (domain over dom_nest_0)` — it
        // named another DOMAIN as if it were the base type and never mentioned nesting at all,
        // leaving the one operator who hits this with nothing to act on. Derived from the const, so
        // moving `MAX_DOMAIN_DEPTH` moves the assertion with it.
        let why = check_param(&Value::I64(1), &past_bound)
            .expect_err("past the bound the bind is refused");
        assert!(
            why.contains("nesting"),
            "a past-bound refusal must say the NESTING bound is what refused it: {why}"
        );
        assert!(
            why.contains(&format!("{MAX_DOMAIN_DEPTH}-level")),
            "...and must name the bound it exceeded: {why}"
        );
        assert!(
            !why.contains("domain over"),
            "...and must NOT claim an inner domain is the base type: {why}"
        );

        // ...and past the bound the boxed impl refuses too, so the two still agree. This is the
        // assertion that catches the double-unwrap: with `check_param` pre-resolving, `accepts`
        // says yes here (16 effective unwraps) while `to_sql_checked` says no (8).
        for depth in [
            MAX_DOMAIN_DEPTH + 1,
            MAX_DOMAIN_DEPTH + 4,
            MAX_DOMAIN_DEPTH * 2,
        ] {
            let deep = nested_domain(depth, 900_300 + depth as u32 * 100, Type::INT4);
            let boxed = value_to_boxed(&Value::I64(1));
            let mut buf = tokio_postgres::types::private::BytesMut::new();
            assert_eq!(
                accepts(&Value::I64(1), &deep),
                boxed.to_sql_checked(&deep, &mut buf).is_ok(),
                "at nesting depth {depth} the pre-flight and the boxed impl must agree — a \
                 pre-flight that unwraps DEEPER than the impl is LOOSER than it, which is the \
                 false-Indeterminate direction (§19.3)"
            );
        }
    }

    /// The refusal message names BOTH the domain and its base, or an operator staring at
    /// `positive_int` has no idea what the bind actually needed.
    ///
    /// The synthetic oid is in the `900_0xx` band on purpose: `2205` is `regclass`'s REAL oid and
    /// `2206` is `regtype`'s (hazard 11), so reusing either would make the fixture lie about what
    /// PG would have sent.
    #[test]
    fn s8a_a_domain_refusal_names_the_domain_and_its_base() {
        use tokio_postgres::types::Kind;
        let dom = Type::new(
            "positive_int".to_string(),
            900_003,
            Kind::Domain(Type::INT4),
            "public".to_string(),
        );
        let why = check_param(&Value::Text("x".into()), &dom)
            .expect_err("a TEXT cannot bind an int4 domain");
        assert!(why.contains("positive_int"), "names the DOMAIN: {why}");
        assert!(why.contains("int4"), "names the BASE: {why}");
    }

    #[test]
    fn value_kind_labels_each_variant() {
        assert_eq!(value_kind(&Value::Null), "NULL");
        assert_eq!(value_kind(&Value::I64(1)), "I64");
        assert_eq!(value_kind(&Value::F64(1.0)), "F64");
        assert_eq!(value_kind(&Value::Text(String::new())), "TEXT");
        assert_eq!(value_kind(&Value::Bytes(vec![])), "BYTES");
        assert_eq!(value_kind(&Value::Bool(true)), "BOOL");
        // M1-S7 canonical tags: a label per tag so a bind rejection names the real canonical type.
        assert_eq!(value_kind(&Value::U64(1)), "U64");
        assert_eq!(value_kind(&Value::Decimal("1".into())), "DECIMAL");
        assert_eq!(value_kind(&Value::Date("2026-01-01".into())), "DATE");
        assert_eq!(value_kind(&Value::Time("00:00:00".into())), "TIME");
        assert_eq!(
            value_kind(&Value::Timestamp("2026-01-01 00:00:00".into())),
            "TIMESTAMP"
        );
        assert_eq!(
            value_kind(&Value::TimestampTz("2026-01-01T00:00:00Z".into())),
            "TIMESTAMPTZ"
        );
        assert_eq!(
            value_kind(&Value::Uuid("00000000-0000-0000-0000-000000000000".into())),
            "UUID"
        );
        assert_eq!(value_kind(&Value::Json("null".into())), "JSON");
    }

    /// One instance of every canonical `Value` variant — the totality fixture.
    ///
    /// Completeness is NOT checkable from here (it is a hand-written `Vec`); [`_exhaustive`] below
    /// is the compile-forced guard that a variant cannot go missing.
    fn every_variant() -> Vec<Value> {
        vec![
            Value::Null,
            Value::Bool(true),
            Value::I64(-200),
            // M1-S8a: the magnitudes the narrowing range gate exists for. Without these three the
            // cross-product proof below only ever sees an in-range integer and the gate is UNPROVEN
            // (the hard-coded-fixture failure mode).
            Value::I64(i64::MAX),
            Value::I64(i64::from(i32::MAX) + 1),
            Value::I64(i64::from(i16::MAX) + 1),
            Value::F64(1.5),
            Value::F64(1e39),
            // The UNDERFLOW magnitude (Task 4 review). NB it cannot catch the semantic hole it was
            // added for — `check_param` and `to_sql` AGREE to truncate, so the DIRECTIONAL proof
            // below is blind to it; the guard for that is
            // `s8a_out_of_range_narrowing_is_refused_before_send`. It is here so the fixture
            // carries every magnitude the range gate exists for, and so the refusal is proven to
            // stay STRICTER than the impl rather than drifting looser.
            Value::F64(1e-46),
            Value::F64(f64::NAN),
            Value::Text("x".to_string()),
            Value::Bytes(vec![0xde, 0xad]),
            Value::U64(u64::MAX),
            Value::Decimal("-12345.6700".to_string()),
            Value::Date("2026-08-05".to_string()),
            Value::Time("24:00:00".to_string()),
            Value::Timestamp("2026-08-05 11:45:07.250000".to_string()),
            Value::TimestampTz("2026-08-05T11:45:07.250000Z".to_string()),
            Value::Uuid("3f2b8c1a-0000-4fff-8000-abcdefabcdef".to_string()),
            Value::Json(r#"{"a":[1,2]}"#.to_string()),
        ]
    }

    /// **Compile-forced completeness for [`every_variant`].**
    ///
    /// `every_variant` is a hand-written `Vec`, so `assert_eq!(x.len(), every_variant().len())`
    /// proves only that boxing drops nothing — it is a TAUTOLOGY with respect to a variant that was
    /// never added. This match has **no `_` arm**, so adding a 15th variant to
    /// `ferro_proto::value::Value` breaks THIS FILE's build.
    ///
    /// **When that build break happens, the fix is to add the variant to `every_variant()` above**
    /// (and to give it a real box in `value_to_boxed` and a real arm in `check_param`) — NOT to add
    /// an arm here and move on. The arms below exist only to make the omission impossible to miss.
    #[allow(dead_code)]
    fn _exhaustive(v: &Value) {
        match v {
            Value::Null => (),
            Value::Bool(_) => (),
            Value::I64(_) => (),
            Value::F64(_) => (),
            Value::Text(_) => (),
            Value::Bytes(_) => (),
            Value::U64(_) => (),
            Value::Decimal(_) => (),
            Value::Date(_) => (),
            Value::Time(_) => (),
            Value::Timestamp(_) => (),
            Value::TimestampTz(_) => (),
            Value::Uuid(_) => (),
            Value::Json(_) => (),
        }
    }

    /// Every PG `Type` any of the arms above could plausibly be aimed at, plus a few that must
    /// never be accepted. Used for the cross-product directional proof below.
    ///
    /// **This fixture gets NO compile-forced guard, deliberately** (unlike [`every_variant`], whose
    /// guard is [`_exhaustive`]). `tokio_postgres::types::Type` is an EXTERNAL, OPEN type — any OID
    /// constructs one — so no `match` over it can be exhaustive and no compile-forced completeness
    /// check exists. Behavioural cross-product coverage is the right and only guard here; the
    /// standing obligation is to GROW this list whenever an arm admits a new target type, and to
    /// mutation-prove that the growth was load-bearing. Do not "fix" the asymmetry with `_`.
    ///
    /// M1-S8a grew it with six DOMAIN entries and mutation-proved the growth: with the boxed
    /// `Bool`/`Text`/`Bytes` arms reverted to their bare `postgres-types` impls (the broken v1
    /// shape), a fixture carrying only `dom_int4`/`dom_numeric` leaves
    /// [`s7_accepts_is_never_looser_than_the_boxed_impl`] GREEN over the bug; the `dom_text`/
    /// `dom_bool`/`dom_bytea` entries are what turn it RED.
    fn every_target_type() -> Vec<Type> {
        use tokio_postgres::types::Kind;
        vec![
            Type::BOOL,
            Type::INT2,
            Type::INT4,
            Type::INT8,
            Type::FLOAT4,
            Type::FLOAT8,
            Type::TEXT,
            Type::VARCHAR,
            Type::BPCHAR,
            Type::BYTEA,
            Type::NUMERIC,
            Type::DATE,
            Type::TIME,
            Type::TIMETZ,
            Type::TIMESTAMP,
            Type::TIMESTAMPTZ,
            Type::UUID,
            Type::JSON,
            Type::JSONB,
            Type::INTERVAL,
            Type::INET,
            Type::INT4_ARRAY,
            // **The one NAME-SENSITIVE encoder in the fixture** (Task 5 fix round 1, F1). Every
            // other entry above is bound by an impl whose `to_sql` IGNORES the `Type` it is handed,
            // which made the payload-BYTES clause of
            // [`s8a_every_arm_treats_a_domain_exactly_as_its_base`] a guard that could not fail:
            // with `pg_domain_aware_param`'s `to_sql` mutated to use the UNRESOLVED `ty`, all 19
            // offline tests stayed green.
            //
            // `<&str as ToSql>::to_sql` (which `String`'s forwards to, which `PgText` wraps)
            // switches on `ty.name()`: `ltree`/`lquery`/`ltxtquery` get a leading VERSION byte,
            // everything else is written verbatim. So a `PgText` handed the declared `dom_of_ltree`
            // instead of the resolved `ltree` writes DIFFERENT BYTES — `[120]` where the base wrote
            // `[1, 120]` — and the bytes clause goes red. `accepts` still resolves under that
            // mutation, so clauses (1) and (2) stay green: this entry is the ONLY thing standing
            // between the payload half of §22.2 (g) and unfalsifiability.
            //
            // The oid is in the `16_38x` band, not the `900_0xx` synthetic band: `ltree` is an
            // EXTENSION type, so PG really does assign it a user-space oid here (hazard 11 —
            // a fixture must not lie about what PG would have sent).
            Type::new(
                "ltree".to_string(),
                16_385,
                Kind::Simple,
                "public".to_string(),
            ),
            // M1-S8a: domains, which `stmt.params()` reports VERBATIM for a parameter slot (unlike
            // `RowDescription`, which resolves to the base). Without these the cross-product proof
            // never exercises `resolve_domain` at all.
            //
            // The text/bool/bytea entries are load-bearing, not symmetry: `postgres-types` has no
            // `Kind::Domain` handling, so those three are precisely the arms where a half-applied
            // unwrap (pre-flight resolves, boxed impl does not) is LOOSER than the impl — and a
            // fixture carrying only dom_int4/dom_numeric stays GREEN over that bug, because
            // `PgInt`/`PgDecimalText` are Ferro-owned and resolve on both sides already.
            //
            // The oids sit in the `900_0xx` band on purpose: `2205` is `regclass`'s REAL oid and
            // `2206` is `regtype`'s (hazard 11), so reusing either would make the fixture lie about
            // what PG would have sent.
            Type::new(
                "dom_int4".to_string(),
                900_001,
                Kind::Domain(Type::INT4),
                "public".to_string(),
            ),
            Type::new(
                "dom_numeric".to_string(),
                900_002,
                Kind::Domain(Type::NUMERIC),
                "public".to_string(),
            ),
            Type::new(
                "dom_text".to_string(),
                900_010,
                Kind::Domain(Type::TEXT),
                "public".to_string(),
            ),
            Type::new(
                "dom_bool".to_string(),
                900_011,
                Kind::Domain(Type::BOOL),
                "public".to_string(),
            ),
            Type::new(
                "dom_bytea".to_string(),
                900_012,
                Kind::Domain(Type::BYTEA),
                "public".to_string(),
            ),
            // A domain over a domain is legal in PG; this exercises the bounded loop, not just the
            // single-step unwrap.
            Type::new(
                "dom_dom_int4".to_string(),
                900_013,
                Kind::Domain(Type::new(
                    "dom_int4".to_string(),
                    900_001,
                    Kind::Domain(Type::INT4),
                    "public".to_string(),
                )),
                "public".to_string(),
            ),
            // A chain nested PAST `MAX_DOMAIN_DEPTH` but inside `2 × MAX_DOMAIN_DEPTH`. This is the
            // window in which a pre-flight that resolves ONCE ITSELF and then delegates to a
            // newtype's `accepts` (which resolves again) is LOOSER than `to_sql_checked`, which is
            // handed the DECLARED type and unwraps only once. `accepts` must say NO here; the
            // cross-product proof turns RED the moment it says yes.
            nested_domain(MAX_DOMAIN_DEPTH + 4, 900_100, Type::INT4),
        ]
    }

    /// Hazard 19 (DIRECTIONAL): `accepts` may be STRICTER than the boxed impl, never LOOSER. Each
    /// new tag gets its OWN narrow newtype — a shared one would accept every target type the eight
    /// tags collectively touch and silently disable the §19.3 pre-flight for all of them.
    #[test]
    fn s7_accepts_is_narrow_per_tag() {
        let cases: &[(Value, Type, bool)] = &[
            (Value::Decimal("1.10".into()), Type::NUMERIC, true),
            (Value::Decimal("1.10".into()), Type::DATE, false),
            (Value::Decimal("1.10".into()), Type::INT4, false),
            (Value::Date("2026-08-05".into()), Type::DATE, true),
            (Value::Date("2026-08-05".into()), Type::TIMESTAMP, false),
            (Value::Time("24:00:00".into()), Type::TIME, true),
            // Hazard 15 stays closed: `timetz` has a 12-byte payload and no `FromSql`; it is
            // Unsupported on the read side, so it must not be bindable either.
            (Value::Time("24:00:00".into()), Type::TIMETZ, false),
            (
                Value::Timestamp("2026-08-05 00:00:00".into()),
                Type::TIMESTAMP,
                true,
            ),
            // A naive value never guesses a zone.
            (
                Value::Timestamp("2026-08-05 00:00:00".into()),
                Type::TIMESTAMPTZ,
                false,
            ),
            (
                Value::TimestampTz("2026-08-05T00:00:00Z".into()),
                Type::TIMESTAMPTZ,
                true,
            ),
            (
                Value::TimestampTz("2026-08-05T00:00:00Z".into()),
                Type::TIMESTAMP,
                false,
            ),
            (
                Value::Uuid("3f2b8c1a-0000-4fff-8000-abcdefabcdef".into()),
                Type::UUID,
                true,
            ),
            (
                Value::Uuid("3f2b8c1a-0000-4fff-8000-abcdefabcdef".into()),
                Type::TEXT,
                false,
            ),
            (Value::Json("{}".into()), Type::JSON, true),
            (Value::Json("{}".into()), Type::JSONB, true),
            (Value::Json("{}".into()), Type::TEXT, false),
            // U64 has no PG target type in S7 — PG has no unsigned integer type, so it stays a
            // known-fate rejection everywhere (never a silent widening to int8/numeric).
            (Value::U64(1), Type::INT8, false),
            (Value::U64(1), Type::NUMERIC, false),
        ];
        for (v, ty, want) in cases {
            assert_eq!(accepts(v, ty), *want, "accepts({v:?}, {ty:?})");
        }
    }

    /// `U64` is refused against EVERY type, not just the two spot-checked above — PG has no
    /// unsigned integer type in scope for S7, so there is no target it could bind to.
    #[test]
    fn s7_u64_is_a_known_fate_rejection_against_every_type() {
        for ty in every_target_type() {
            assert!(
                !accepts(&Value::U64(1), &ty),
                "U64 must stay a known-fate rejection against {ty:?}"
            );
        }
    }

    /// The newtypes send **TEXT** format. Param format IS per-param selectable (hazard 17 — the
    /// vendored fork builds a per-param format array at `query.rs:305-308`), even though the RESULT
    /// format is hardcoded binary at `:324`. That asymmetry is what lets PG's own input parser
    /// consume the canonical text, so no base-10000 NUMERIC encoder and no 2000-epoch date
    /// arithmetic has to be hand-written on the write side.
    #[test]
    fn s7_newtypes_send_text_format() {
        assert!(matches!(
            PgDecimalText("1.10".into()).encode_format(&Type::NUMERIC),
            Format::Text
        ));
        assert!(matches!(
            PgDateText("2026-08-05".into()).encode_format(&Type::DATE),
            Format::Text
        ));
        assert!(matches!(
            PgTimeText("24:00:00".into()).encode_format(&Type::TIME),
            Format::Text
        ));
        assert!(matches!(
            PgTimestampText("2026-08-05 00:00:00".into()).encode_format(&Type::TIMESTAMP),
            Format::Text
        ));
        assert!(matches!(
            PgTimestampTzText("2026-08-05T00:00:00Z".into()).encode_format(&Type::TIMESTAMPTZ),
            Format::Text
        ));
        assert!(matches!(
            PgUuidText("3f2b8c1a-0000-4fff-8000-abcdefabcdef".into()).encode_format(&Type::UUID),
            Format::Text
        ));
        assert!(matches!(
            PgJsonText("{}".into()).encode_format(&Type::JSONB),
            Format::Text
        ));
    }

    /// The canonical text is written VERBATIM — no re-rendering, so the display scale (`1.10` ≠
    /// `1.1`) and the `NaN`/`infinity` sentinels reach PG's parser exactly as the reader produced
    /// them.
    #[test]
    fn s7_newtypes_write_the_canonical_text_verbatim() {
        for (text, ty) in [
            ("1.10", Type::NUMERIC),
            ("NaN", Type::NUMERIC),
            ("-Infinity", Type::NUMERIC),
        ] {
            let mut buf = tokio_postgres::types::private::BytesMut::new();
            let is_null = PgDecimalText(text.into()).to_sql(&ty, &mut buf).unwrap();
            assert!(matches!(is_null, IsNull::No));
            assert_eq!(
                &buf[..],
                text.as_bytes(),
                "canonical text must go out as-is"
            );
        }
    }

    /// **M1-S8a review F3a: the NARROWING binds write the EXACT bytes of the EXACT target width.**
    ///
    /// Every other proof around [`PgInt`]/[`PgFloat`] is blind to a wrong-BYTES encoder, which is
    /// why this one exists as a hand-computed byte fixture rather than another property:
    /// * [`s7_accepts_is_never_looser_than_the_boxed_impl`] only asserts `to_sql_checked(..).is_ok()`
    ///   and never inspects the buffer;
    /// * [`s7_newtypes_write_the_canonical_text_verbatim`] covers only the DECIMAL text newtype;
    /// * [`s8a_every_arm_treats_a_domain_exactly_as_its_base`] compares base-vs-domain bytes TO EACH
    ///   OTHER, so an encoder that is wrong identically on both sides passes;
    /// * and the live acceptance bound only `F64(1.5)`/`F64(2.25)` — both EXACTLY f32-representable,
    ///   so no value in the whole suite could tell an `f64` encoder from an `f32` one. Measured: the
    ///   mutation `self.0.to_sql(base, out)` in `PgFloat::to_sql` was GREEN across
    ///   the entire ferro-backend-pg + ferrod (live) suites.
    ///
    /// The expected vectors below are written out by hand from the IEEE-754 / two's-complement
    /// definitions — NOT produced by calling the impl under test, and NOT by calling
    /// `f64::to_be_bytes` on the same expression the impl uses — so they are an independent oracle:
    /// * `-200` is `0xFF38` / `0xFFFFFF38` / `0xFFFFFFFFFFFFFF38` at widths 2/4/8 (PG sends every
    ///   integer big-endian);
    /// * `0.1 + 0.2` is the canonical non-`f32`-representable double `0.30000000000000004`,
    ///   `0x3FD3333333333334` (the nearest double to 0.3 is `...3333`, one ULP below); rounded to
    ///   `f32` it is `0x3E99999A`, whose widening back to `f64` is `0x3FD3333340000000` — a
    ///   DIFFERENT 8-byte payload, which is precisely what an `f32`-truncating `float8` encoder
    ///   would emit;
    /// * `f64::MAX` is `0x7FEFFFFFFFFFFFFF`; through `f32` it saturates to `inf`
    ///   (`0x7FF0000000000000`).
    #[test]
    fn s8a_int_and_float_binds_write_the_exact_target_width_bytes() {
        fn bytes_of(v: &Value, ty: &Type) -> Vec<u8> {
            let mut buf = tokio_postgres::types::private::BytesMut::new();
            let is_null = value_to_boxed(v)
                .to_sql_checked(ty, &mut buf)
                .unwrap_or_else(|e| panic!("{v:?} against {} must bind: {e}", ty.name()));
            assert!(matches!(is_null, IsNull::No), "{v:?} is not NULL");
            buf.to_vec()
        }

        // ---- PgInt: the same i64 must be written at the DECLARED width, big-endian, sign-extended.
        assert_eq!(bytes_of(&Value::I64(-200), &Type::INT2), vec![0xFF, 0x38]);
        assert_eq!(
            bytes_of(&Value::I64(-200), &Type::INT4),
            vec![0xFF, 0xFF, 0xFF, 0x38]
        );
        assert_eq!(
            bytes_of(&Value::I64(-200), &Type::INT8),
            vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x38]
        );
        // The full-width magnitude: an `int8` bind that truncated through `i32` (or through the
        // `int4` arm) could not represent this at all.
        assert_eq!(
            bytes_of(&Value::I64(i64::MAX), &Type::INT8),
            vec![0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]
        );
        assert_eq!(
            bytes_of(&Value::I64(i64::from(i32::MIN)), &Type::INT4),
            vec![0x80, 0x00, 0x00, 0x00]
        );

        // ---- PgFloat: `float8` gets the FULL double; only `float4` may narrow.
        let non_f32 = 0.1_f64 + 0.2_f64; // 0.30000000000000004
        assert_eq!(
            bytes_of(&Value::F64(non_f32), &Type::FLOAT8),
            vec![0x3F, 0xD3, 0x33, 0x33, 0x33, 0x33, 0x33, 0x34],
            "a float8 bind must send the double VERBATIM — 0x3FD3333340000000 here would mean the \
             value had been round-tripped through f32"
        );
        assert_eq!(
            bytes_of(&Value::F64(non_f32), &Type::FLOAT4),
            vec![0x3E, 0x99, 0x99, 0x9A],
            "a float4 bind is the ONLY arm allowed to narrow, and must produce the correctly \
             rounded f32"
        );
        assert_eq!(
            bytes_of(&Value::F64(f64::MAX), &Type::FLOAT8),
            vec![0x7F, 0xEF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
            "f64::MAX must not saturate to +inf (0x7FF0000000000000) — which is what a float8 \
             encoder that went through f32 would write"
        );
        assert_eq!(
            bytes_of(&Value::F64(1.5), &Type::FLOAT8),
            vec![0x3F, 0xF8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
        );
        assert_eq!(
            bytes_of(&Value::F64(1.5), &Type::FLOAT4),
            vec![0x3F, 0xC0, 0x00, 0x00]
        );
    }

    /// **The lockstep proof (carry C2/C3/C12).** Over the FULL cross product of every canonical
    /// variant × every plausible target type: whenever `accepts` says yes, the concrete boxed impl
    /// must actually bind. That is the directional rule mechanically — `accepts` can be stricter
    /// (a clean pre-send rejection), never looser (which would let `to_sql_checked` fail instead,
    /// and a `to_sql` failure carries no `DbError` → it is MISCLASSIFIED as a lost connection →
    /// §19.3 mints a false `Indeterminate`). It also proves `accepts` and `value_to_boxed` were
    /// flipped together: widening one without the other fails here. M1-S8a made it load-bearing for
    /// the range gate too — the out-of-range magnitudes in `every_variant` are the inputs that
    /// separate "the pre-flight is stricter" from "the pre-flight is looser".
    #[test]
    fn s7_accepts_is_never_looser_than_the_boxed_impl() {
        for v in every_variant() {
            let boxed = value_to_boxed(&v);
            for ty in every_target_type() {
                if !accepts(&v, &ty) {
                    continue;
                }
                let mut buf = tokio_postgres::types::private::BytesMut::new();
                assert!(
                    boxed.to_sql_checked(&ty, &mut buf).is_ok(),
                    "accepts({v:?}, {ty:?}) said yes but the boxed impl refuses it — a LOOSER \
                     pre-flight lets to_sql_checked fail instead, and a to_sql failure carries no \
                     DbError, so it is misclassified as ConnectionLost (false Indeterminate, §19.3)"
                );
            }
        }
    }

    /// **M1-S8a: a DOMAIN behaves EXACTLY as its base — in every arm, on both sides, byte for byte.**
    ///
    /// Derived rather than enumerated: it wraps each non-domain entry of [`every_target_type`] in a
    /// synthetic `Kind::Domain` and asserts three things for the full cross product with
    /// [`every_variant`], so it grows automatically with either fixture.
    ///
    /// 1. the PRE-FLIGHT answers identically (`accepts(v, base) == accepts(v, domain_over_base)`) —
    ///    an EQUALITY, which is stronger than §19.3's directional rule and is what "resolve the
    ///    domain" actually means: never looser (the false-`Indeterminate` direction) and never
    ///    stricter (which would leave §22.2 (g)'s readable-but-not-bindable asymmetry open);
    /// 2. the BOXED impl answers identically, so the mirror holds on the side that reaches the
    ///    socket. This is the half `postgres-types` cannot do for itself — it has ZERO
    ///    `Kind::Domain` handling — and the half the rejected v1 design omitted;
    /// 3. when both succeed, the PAYLOAD BYTES are identical. Resolving a domain must change what
    ///    the bind is *checked against*, never what it *writes*.
    ///
    /// **Correction (Task 5 fix round 1, F1).** Clause (3) shipped UNFALSIFIABLE and was described
    /// as mutation-proven when only clauses (1)/(2) were: every entry `every_target_type` then held
    /// is bound by an impl whose `to_sql` ignores the `Type`, so mutating `pg_domain_aware_param`'s
    /// `to_sql` to use the UNRESOLVED `ty` left all 19 offline tests green. The `ltree` entry (see
    /// [`every_target_type`]) is what makes clause (3) bite: its encoder switches on `ty.name()`.
    /// Measured RED under exactly that mutation, GREEN restored.
    ///
    /// This is the guard that covers the arms the hand-written domain tests do not name
    /// individually (the seven canonical-text tags, whose resolution lives in one macro body).
    #[test]
    fn s8a_every_arm_treats_a_domain_exactly_as_its_base() {
        use tokio_postgres::types::Kind;
        let mut oid = 901_000u32;
        let mut checked = 0usize;
        for base in every_target_type() {
            // Skip the fixture's own domains: wrapping them again just re-tests nesting, which
            // `s8a_domain_nesting_is_bounded_and_the_bound_refuses` owns.
            if matches!(base.kind(), Kind::Domain(_)) {
                continue;
            }
            oid += 1;
            let dom = Type::new(
                format!("dom_of_{}", base.name()),
                oid,
                Kind::Domain(base.clone()),
                "public".to_string(),
            );
            for v in every_variant() {
                // (1) the pre-flight.
                assert_eq!(
                    accepts(&v, &base),
                    accepts(&v, &dom),
                    "the pre-flight must treat a DOMAIN exactly as its base: {v:?} against \
                     {} vs {}",
                    base.name(),
                    dom.name()
                );
                // (2) the boxed impl — the half `postgres-types` has no domain handling for.
                let boxed = value_to_boxed(&v);
                let mut on_base = tokio_postgres::types::private::BytesMut::new();
                let mut on_dom = tokio_postgres::types::private::BytesMut::new();
                let base_ok = boxed.to_sql_checked(&base, &mut on_base).is_ok();
                let dom_ok = boxed.to_sql_checked(&dom, &mut on_dom).is_ok();
                assert_eq!(
                    base_ok,
                    dom_ok,
                    "the BOXED impl must treat a DOMAIN exactly as its base for {v:?} against \
                     {}: a pre-flight looser than the impl is the false-Indeterminate path (§19.3)",
                    base.name()
                );
                // (3) and the wire payload is unchanged by the unwrap.
                if base_ok {
                    assert_eq!(
                        &on_base[..],
                        &on_dom[..],
                        "resolving a domain must change what the bind is CHECKED against, never \
                         what it WRITES: {v:?} against {}",
                        base.name()
                    );
                }
                checked += 1;
            }
        }
        // Not a completeness claim — `every_target_type` is an open, hand-grown list (see its
        // docs). This only pins that the loop above actually ran a cross product rather than
        // silently skipping everything, which is how a `continue`-guarded test dies quietly.
        assert_eq!(
            checked,
            every_variant().len()
                * every_target_type()
                    .iter()
                    .filter(|t| !matches!(t.kind(), Kind::Domain(_)))
                    .count(),
            "every (value, non-domain target) pair must have been exercised"
        );
        assert!(checked > 0);
    }

    /// **No panic is reachable from a user param (carry C2/C3/C12).** `value_to_boxed` used to be
    /// an `unreachable!()` for the eight canonical tags, sound only while `accepts` gated every
    /// path. It now has a real box per variant, so even a caller that skipped the pre-flight
    /// entirely gets a typed `WrongType` error rather than a daemon panic. Exercised against the
    /// full cross product, including the pairs `accepts` rejects.
    #[test]
    fn s7_value_to_boxed_is_total_and_never_panics() {
        for v in every_variant() {
            let boxed = value_to_boxed(&v);
            for ty in every_target_type() {
                let mut buf = tokio_postgres::types::private::BytesMut::new();
                // The only contract here is "does not panic"; a rejected pair returns Err.
                let _ = boxed.to_sql_checked(&ty, &mut buf);
            }
        }
        // One boxed ToSql per fixture value. NOT a completeness check — `_exhaustive` above is the
        // guard that a variant cannot go missing; this only pins that boxing is total and drops
        // nothing. Written derived rather than as a literal so the fixture can grow freely.
        assert_eq!(
            to_boxed_params(&every_variant()).len(),
            every_variant().len(),
            "one boxed ToSql per fixture value"
        );
    }

    /// **M1-S8b: a canonical TEXT payload binds where PG's own TEXT INPUT SYNTAX is what it
    /// carries.** Stock Doctrine DBAL stringifies every temporal, decimal, JSON and UUID value in
    /// its type layer and binds it with `ParameterType::STRING`, so on PostgreSQL every such INSERT
    /// used to be refused pre-send. The widening is not a loosening of the §19.3 direction: the
    /// pre-flight still delegates to the very predicate `to_sql_checked` will apply, and the
    /// failure it now permits lands as a real server-side `22007`/`22P02` `DbError`, i.e. a KNOWN
    /// fate, never the unclassifiable band.
    ///
    /// This REPLACES `s7_a_bare_text_never_binds_to_a_temporal_or_numeric_column`. That test's
    /// PROPERTY — a bare string never silently becomes a PG sentinel — survives verbatim in
    /// [`s8b_a_bare_text_sentinel_is_still_refused_for_a_temporal_or_numeric_slot`] below, as a
    /// VALUE-aware gate instead of a whole-tag ban.
    #[test]
    fn s8b_bare_text_binds_to_every_type_whose_input_syntax_is_text() {
        for ty in [
            Type::TEXT,
            Type::VARCHAR,
            Type::BPCHAR,
            Type::NAME,
            Type::UNKNOWN,
            Type::NUMERIC,
            Type::DATE,
            Type::TIME,
            Type::TIMESTAMP,
            Type::TIMESTAMPTZ,
            Type::UUID,
            Type::JSON,
            Type::JSONB,
        ] {
            assert!(
                accepts(&Value::Text("2026-08-05".to_string()), &ty),
                "a canonical TEXT param must bind to {ty:?} — DBAL's type layer sends every \
                 temporal/decimal/json/uuid value as a string"
            );
        }
        // Still NARROW where text is NOT the input form: an integer, a boolean and a byte array
        // have binary-only bind paths here, and the S8a narrowing that made `serial` PKs work
        // must not be undone by this widening.
        for ty in [
            Type::INT2,
            Type::INT4,
            Type::INT8,
            Type::BOOL,
            Type::BYTEA,
            Type::FLOAT8,
        ] {
            assert!(
                !accepts(&Value::Text("42".to_string()), &ty),
                "a bare TEXT param must NOT bind to {ty:?}"
            );
        }
        // The domain unwrap still applies on the widened path (S8a). Built INLINE, in the `900_0xx`
        // synthetic band and NOT reusing an existing fixture oid, exactly as every other domain in
        // this file is built.
        let dom_date = Type::new(
            "dom_date".to_string(),
            900_020,
            tokio_postgres::types::Kind::Domain(Type::DATE),
            "public".to_string(),
        );
        assert!(accepts(&Value::Text("2026-08-05".to_string()), &dom_date));

        // **The FORMAT must resolve the domain too, and it is a separate branch from `to_sql`.**
        // `PgText::encode_format` decides whether PG reads these bytes as text or as a 4-byte binary
        // `date`; a version that tested the UNRESOLVED type would send `Format::Binary` for
        // `dom_date` while sending `Format::Text` for `date`, i.e. a wire bug reachable only through
        // a domain — and `s8a_every_arm_treats_a_domain_exactly_as_its_base` compares `to_sql`
        // BYTES, not formats, so nothing else in the tree would notice.
        assert!(matches!(
            PgText("2026-08-05".to_string()).encode_format(&dom_date),
            Format::Text
        ));
        assert!(matches!(
            PgText("2026-08-05".to_string()).encode_format(&Type::DATE),
            Format::Text
        ));
        // …and the types that were ALREADY accepted keep the binary format they have always had, so
        // this task's regression surface on the shipping path is empty.
        let dom_text = Type::new(
            "dom_text_fmt".to_string(),
            900_021,
            tokio_postgres::types::Kind::Domain(Type::TEXT),
            "public".to_string(),
        );
        for ty in [Type::TEXT, Type::VARCHAR, dom_text] {
            assert!(
                matches!(PgText("x".to_string()).encode_format(&ty), Format::Binary),
                "an already-accepted string type must keep its binary format: {ty:?}"
            );
        }
    }

    /// **The sentinel discipline, preserved — as a VALUE-aware gate, not a whole-tag ban.** PG's
    /// input parser turns the bare words `infinity`, `now`, `today`, … into real values, so a
    /// string that happens to hold one must not become a timestamp sentinel just because it landed
    /// in a temporal slot. Same for `NaN` / `Infinity` against `numeric`. The refusal names the
    /// canonical tag route (`Ferro\Date`, `Ferro\NaiveTimestamp`, `Ferro\Decimal`), which IS how a
    /// caller expresses a sentinel on purpose.
    ///
    /// This is a REFUSAL keyed on the SLOT's type, never an inference of a TAG from content:
    /// nothing here decides that `'2026-08-05'` "is a date".
    #[test]
    fn s8b_a_bare_text_sentinel_is_still_refused_for_a_temporal_or_numeric_slot() {
        for lit in [
            "infinity",
            "-infinity",
            "Infinity",
            "NOW",
            "today",
            "Tomorrow",
            "yesterday",
            "epoch",
            "allballs",
        ] {
            for ty in [Type::DATE, Type::TIME, Type::TIMESTAMP, Type::TIMESTAMPTZ] {
                let err = check_param(&Value::Text(lit.to_string()), &ty).expect_err(
                    "a PG special datetime literal must be refused for a temporal slot",
                );
                // **Case matters and the token is spelled ONCE, here and in the refusal message.**
                // `str::contains` is case-sensitive; plan v1 asserted lowercase `"special"` against
                // a message containing only `SPECIAL`, which fails on the first of these 36
                // iterations against a CORRECT implementation (measured), and whose cheapest
                // "repair" is to delete the assertion — at which point the guard stops
                // distinguishing "refused because it is a SPECIAL literal" from "refused because
                // the whole TEXT tag is banned", the ONE distinction this rewrite exists to make.
                assert!(
                    err.contains("SPECIAL"),
                    "the refusal must say WHY, got {err:?}"
                );
                // …and it must be ACTIONABLE: name the SLOT type and the tagged escape route.
                // `contains("SPECIAL")` alone passes for any message containing the word.
                assert!(
                    err.contains(ty.name()),
                    "the refusal must name the slot type, got {err:?}"
                );
                assert!(
                    err.contains("Ferro\\Date"),
                    "the refusal must name the tagged route that DOES bind a sentinel, got {err:?}"
                );
            }
        }
        for lit in ["NaN", "nan", "Infinity", "-Infinity"] {
            check_param(&Value::Text(lit.to_string()), &Type::NUMERIC)
                .expect_err("a numeric special literal must be refused for a numeric slot");
        }
        // …and the SAME strings are perfectly ordinary values in a text column, which is the whole
        // reason the gate is keyed on the slot rather than on the content.
        for lit in ["infinity", "NaN", "today"] {
            check_param(&Value::Text(lit.to_string()), &Type::TEXT)
                .expect("a special literal is just a string in a text column");
        }
        // …and a sentinel that arrived TAG-INTACT still binds, exactly as before.
        assert!(accepts(&Value::Date("infinity".into()), &Type::DATE));
    }
}
