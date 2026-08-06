use crate::CodecError;
use crate::consts::tag;
use rmp::Marker;
use rmp::decode::{self as dec, RmpRead};
use rmp::encode as enc;

/// A scalar carried on the wire as the 2-element MessagePack array `[tag, payload]`
/// (decision W-1). Integer encoding follows `rmp::encode::write_sint` canonical narrowing:
/// non-negative values narrow to unsigned markers, negative values narrow to signed markers.
/// `U64` uses the sibling `write_uint` ladder (same narrowing, unsigned-only, so values above
/// `i64::MAX` stay representable). This exact byte shape is mirrored by the PHP `Value` codec and
/// locked by golden vectors — do not special-case it away.
///
/// **Tag-byte invariant:** the tag is written with `enc::write_pfix` and read with `dec::read_pfix`,
/// i.e. a BARE positive fixint. That is exact for the whole registry (tags 0..=17) and is the
/// contract, not an accident — do not "generalize" it to a generic int read, which would admit a
/// non-canonical multi-byte encoding of the same tag.
///
/// The M1-S7 variants below `Bytes` all carry **canonical text** as defined in
/// `proto/PROTOCOL.md` §3. The codec moves that text verbatim and validates nothing beyond UTF-8;
/// producing it correctly is the backend's job (that is where the source format is known).
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Text(String),
    Bytes(Vec<u8>),
    // ---- M1-S7: canonical type coverage. Each text variant holds the CANONICAL
    // payload string defined in proto/PROTOCOL.md §3; the backend produces it, the
    // codec only moves it. U64 is the one non-str addition (msgpack uint family).
    U64(u64),
    Decimal(String),
    Date(String),
    Time(String),
    Timestamp(String),
    TimestampTz(String),
    Uuid(String),
    Json(String),
}

impl Value {
    pub fn tag(&self) -> u8 {
        match self {
            Value::Null => tag::NULL,
            Value::Bool(_) => tag::BOOL,
            Value::I64(_) => tag::I64,
            Value::F64(_) => tag::F64,
            Value::Text(_) => tag::TEXT,
            Value::Bytes(_) => tag::BYTES,
            Value::U64(_) => tag::U64,
            Value::Decimal(_) => tag::DECIMAL,
            Value::Date(_) => tag::DATE,
            Value::Time(_) => tag::TIME,
            Value::Timestamp(_) => tag::TIMESTAMP,
            Value::TimestampTz(_) => tag::TIMESTAMPTZ,
            Value::Uuid(_) => tag::UUID,
            Value::Json(_) => tag::JSON,
        }
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        // [tag, payload] — fixarray(2)
        enc::write_array_len(out, 2).unwrap();
        enc::write_pfix(out, self.tag()).unwrap(); // tags 0..=17 fit positive fixint
        match self {
            Value::Null => enc::write_nil(out).unwrap(),
            Value::Bool(b) => enc::write_bool(out, *b).unwrap(),
            Value::I64(n) => {
                enc::write_sint(out, *n).unwrap();
            }
            Value::F64(f) => enc::write_f64(out, *f).unwrap(),
            Value::Text(s) => enc::write_str(out, s).unwrap(),
            Value::Bytes(b) => enc::write_bin(out, b).unwrap(),
            // `write_uint` — NOT `write_u64` (which always emits the fixed 0xcf marker) and NOT
            // `write_sint` (which cannot represent > i64::MAX, the entire reason U64 exists).
            // `write_uint` narrows canonically and is byte-identical to PHP `PurePacker::packUint`
            // across the whole range; the golden vectors lock that equality.
            Value::U64(n) => {
                enc::write_uint(out, *n).unwrap();
            }
            // Every other S7 tag rides the `str` family carrying its CANONICAL TEXT (PROTOCOL.md
            // §3). The codec neither validates nor reformats it — the backend owns the rendering.
            Value::Decimal(s)
            | Value::Date(s)
            | Value::Time(s)
            | Value::Timestamp(s)
            | Value::TimestampTz(s)
            | Value::Uuid(s)
            | Value::Json(s) => enc::write_str(out, s).unwrap(),
        }
    }

    pub fn decode(rd: &mut &[u8]) -> Result<Value, CodecError> {
        let len =
            dec::read_array_len(rd).map_err(|e| CodecError::Malformed(format!("array: {e:?}")))?;
        if len != 2 {
            return Err(CodecError::Malformed(format!(
                "TypedValue array len {len} != 2"
            )));
        }
        let value_tag: u8 =
            dec::read_pfix(rd).map_err(|e| CodecError::Malformed(format!("tag: {e:?}")))?;
        match value_tag {
            t if t == tag::NULL => {
                read_nil(rd)?;
                Ok(Value::Null)
            }
            t if t == tag::BOOL => Ok(Value::Bool(read_bool(rd)?)),
            t if t == tag::I64 => {
                Ok(Value::I64(dec::read_int(rd).map_err(|e| {
                    CodecError::Malformed(format!("i64: {e:?}"))
                })?))
            }
            t if t == tag::F64 => {
                Ok(Value::F64(dec::read_f64(rd).map_err(|e| {
                    CodecError::Malformed(format!("f64: {e:?}"))
                })?))
            }
            t if t == tag::TEXT => Ok(Value::Text(read_str(rd)?)),
            t if t == tag::BYTES => Ok(Value::Bytes(read_bin(rd)?)),
            // `read_int` — NOT `read_u64`, which is marker-strict (`rmp` `decode/uint.rs`: it
            // accepts ONLY `Marker::U64`) and would therefore reject the canonically-narrowed
            // `Value::U64(0)` this codec emits. `read_int` is generic over the target, infers `u64`
            // from the variant, and handles `Marker::U64` losslessly.
            t if t == tag::U64 => {
                Ok(Value::U64(dec::read_int(rd).map_err(|e| {
                    CodecError::Malformed(format!("u64: {e:?}"))
                })?))
            }
            // All str-payload tags go through `read_str`, which `bound_len`-checks the length prefix
            // BEFORE allocating — never hand-roll a reader here (hazard 2).
            t if t == tag::DECIMAL => Ok(Value::Decimal(read_str(rd)?)),
            t if t == tag::DATE => Ok(Value::Date(read_str(rd)?)),
            t if t == tag::TIME => Ok(Value::Time(read_str(rd)?)),
            t if t == tag::TIMESTAMP => Ok(Value::Timestamp(read_str(rd)?)),
            t if t == tag::TIMESTAMPTZ => Ok(Value::TimestampTz(read_str(rd)?)),
            t if t == tag::UUID => Ok(Value::Uuid(read_str(rd)?)),
            t if t == tag::JSON => Ok(Value::Json(read_str(rd)?)),
            other => Err(CodecError::Malformed(format!(
                "unsupported TypedValue tag {other}"
            ))),
        }
    }
}

fn read_nil(rd: &mut &[u8]) -> Result<(), CodecError> {
    match dec::read_marker(rd).map_err(|e| CodecError::Malformed(format!("nil: {e:?}")))? {
        Marker::Null => Ok(()),
        m => Err(CodecError::Malformed(format!("expected nil, got {m:?}"))),
    }
}
pub(crate) fn read_bool(rd: &mut &[u8]) -> Result<bool, CodecError> {
    match dec::read_marker(rd).map_err(|e| CodecError::Malformed(format!("bool: {e:?}")))? {
        Marker::True => Ok(true),
        Marker::False => Ok(false),
        m => Err(CodecError::Malformed(format!("expected bool, got {m:?}"))),
    }
}
pub(crate) fn read_str(rd: &mut &[u8]) -> Result<String, CodecError> {
    let len = dec::read_str_len(rd).map_err(|e| CodecError::Malformed(format!("str len: {e:?}")))?
        as usize;
    bound_len(len, rd.len())?;
    let mut buf = vec![0u8; len];
    rd.read_exact_buf(&mut buf)
        .map_err(|e| CodecError::Malformed(format!("str body: {e:?}")))?;
    String::from_utf8(buf).map_err(|_| CodecError::Malformed("invalid utf8".into()))
}
fn read_bin(rd: &mut &[u8]) -> Result<Vec<u8>, CodecError> {
    let len = dec::read_bin_len(rd).map_err(|e| CodecError::Malformed(format!("bin len: {e:?}")))?
        as usize;
    bound_len(len, rd.len())?;
    let mut buf = vec![0u8; len];
    rd.read_exact_buf(&mut buf)
        .map_err(|e| CodecError::Malformed(format!("bin body: {e:?}")))?;
    Ok(buf)
}

/// Reject a length prefix that exceeds the bytes actually remaining BEFORE allocating, so a lying
/// str/bin/array length (up to u32::MAX) cannot force a huge pre-allocation. The frame payload is
/// already capped at MAX_FRAME_PAYLOAD, so `remaining` is bounded; this bounds the allocation to it.
/// Shared with the bespoke SQL codec (`messages::sql`), which applies the identical discipline to
/// every decoded array length (a MessagePack element is ≥1 byte, so `len > remaining` is a lie).
pub(crate) fn bound_len(len: usize, remaining: usize) -> Result<(), CodecError> {
    if len > remaining {
        return Err(CodecError::Truncated {
            need: len,
            have: remaining,
        });
    }
    Ok(())
}
