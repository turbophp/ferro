use crate::CodecError;
use crate::consts::tag;
use rmp::Marker;
use rmp::decode::{self as dec, RmpRead};
use rmp::encode as enc;

/// A scalar carried on the wire as the 2-element MessagePack array `[tag, payload]`
/// (decision W-1). Integer encoding follows `rmp::encode::write_sint` canonical narrowing:
/// non-negative values narrow to unsigned markers, negative values narrow to signed markers.
/// This exact byte shape is mirrored by the PHP `Value` codec (Task 8) and locked by golden
/// vectors — do not special-case it away.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Text(String),
    Bytes(Vec<u8>),
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
        let tag: u8 =
            dec::read_pfix(rd).map_err(|e| CodecError::Malformed(format!("tag: {e:?}")))?;
        match tag {
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
            other => Err(CodecError::Malformed(format!(
                "unsupported TypedValue tag {other} in M0"
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
fn read_bool(rd: &mut &[u8]) -> Result<bool, CodecError> {
    match dec::read_marker(rd).map_err(|e| CodecError::Malformed(format!("bool: {e:?}")))? {
        Marker::True => Ok(true),
        Marker::False => Ok(false),
        m => Err(CodecError::Malformed(format!("expected bool, got {m:?}"))),
    }
}
fn read_str(rd: &mut &[u8]) -> Result<String, CodecError> {
    let len = dec::read_str_len(rd).map_err(|e| CodecError::Malformed(format!("str len: {e:?}")))?
        as usize;
    let mut buf = vec![0u8; len];
    rd.read_exact_buf(&mut buf)
        .map_err(|e| CodecError::Malformed(format!("str body: {e:?}")))?;
    String::from_utf8(buf).map_err(|_| CodecError::Malformed("invalid utf8".into()))
}
fn read_bin(rd: &mut &[u8]) -> Result<Vec<u8>, CodecError> {
    let len = dec::read_bin_len(rd).map_err(|e| CodecError::Malformed(format!("bin len: {e:?}")))?
        as usize;
    let mut buf = vec![0u8; len];
    rd.read_exact_buf(&mut buf)
        .map_err(|e| CodecError::Malformed(format!("bin body: {e:?}")))?;
    Ok(buf)
}
