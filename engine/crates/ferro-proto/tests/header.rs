use ferro_proto::CodecError;
use ferro_proto::consts::{self, flags};
use ferro_proto::header::Header;

#[test]
fn encode_is_16_bytes_little_endian() {
    let h = Header {
        flags: flags::END,
        service: consts::service::CORE,
        method: consts::method_core::PING,
        request_id: 0x0A0B0C0D,
        payload_len: 1,
    };
    let b = h.encode();
    assert_eq!(b.len(), 16);
    assert_eq!(b[0], consts::MAGIC); // 0xF7
    assert_eq!(b[1], consts::PROTOCOL_VERSION); // 1
    assert_eq!(u16::from_le_bytes([b[2], b[3]]), flags::END);
    assert_eq!(u16::from_le_bytes([b[4], b[5]]), consts::service::CORE);
    assert_eq!(u16::from_le_bytes([b[6], b[7]]), consts::method_core::PING);
    assert_eq!(u32::from_le_bytes([b[8], b[9], b[10], b[11]]), 0x0A0B0C0D);
    assert_eq!(u32::from_le_bytes([b[12], b[13], b[14], b[15]]), 1);
}

#[test]
fn roundtrip() {
    let h = Header {
        flags: flags::STREAM | flags::END,
        service: 2,
        method: 1,
        request_id: 42,
        payload_len: 7,
    };
    assert_eq!(Header::decode(&h.encode()).unwrap(), h);
}

#[test]
fn rejects_bad_magic() {
    let mut b = Header {
        flags: 0,
        service: 1,
        method: 3,
        request_id: 1,
        payload_len: 0,
    }
    .encode();
    b[0] = 0x00;
    assert_eq!(
        Header::decode(&b),
        Err(CodecError::BadMagic {
            expected: consts::MAGIC,
            got: 0x00
        })
    );
}

#[test]
fn rejects_bad_version() {
    let mut b = Header {
        flags: 0,
        service: 1,
        method: 3,
        request_id: 1,
        payload_len: 0,
    }
    .encode();
    b[1] = 99;
    assert_eq!(
        Header::decode(&b),
        Err(CodecError::BadVersion {
            expected: consts::PROTOCOL_VERSION,
            got: 99
        })
    );
}

#[test]
fn rejects_oversize_payload_len_without_reading_payload() {
    let mut b = Header {
        flags: 0,
        service: 2,
        method: 1,
        request_id: 1,
        payload_len: 0,
    }
    .encode();
    let too_big = consts::MAX_FRAME_PAYLOAD + 1;
    b[12..16].copy_from_slice(&too_big.to_le_bytes());
    assert_eq!(
        Header::decode(&b),
        Err(CodecError::FrameTooLarge {
            len: too_big,
            max: consts::MAX_FRAME_PAYLOAD
        })
    );
}

#[test]
fn rejects_short_buffer() {
    assert_eq!(
        Header::decode(&[0u8; 15]),
        Err(CodecError::Truncated { need: 16, have: 15 })
    );
}

#[test]
fn rejects_reserved_and_unknown_flags() {
    use ferro_proto::flags as F;
    assert_eq!(F::validate(flags::OOB_FD), Err(CodecError::UnsupportedFlag));
    assert_eq!(
        F::validate(flags::COMPRESSED),
        Err(CodecError::UnsupportedFlag)
    );
    assert_eq!(
        F::validate(0x8000),
        Err(CodecError::UnknownFlags { bits: 0x8000 })
    );
    assert!(F::validate(flags::STREAM | flags::END | flags::CANCEL).is_ok());
}
