use bytes::BytesMut;
use ferro_proto::consts::{method_core, service};
use ferro_proto::header::Header;
use ferro_proto::messages::Ping;
use ferrod::session::codec::{FrameCodec, FrameError, InFrame, OutFrame};
use tokio_util::codec::{Decoder, Encoder};

fn ping_frame() -> OutFrame {
    let payload = Ping { token: 7 }.encode();
    OutFrame {
        header: Header {
            flags: 0,
            service: service::CORE,
            method: method_core::PING,
            request_id: 1,
            payload_len: payload.len() as u32,
        },
        payload: payload.into(),
    }
}

#[test]
fn encode_then_decode_roundtrips_a_frame() {
    let mut codec = FrameCodec;
    let mut buf = BytesMut::new();
    codec.encode(ping_frame(), &mut buf).unwrap();
    let decoded: InFrame = codec.decode(&mut buf).unwrap().expect("a full frame");
    assert_eq!(decoded.header.service, service::CORE);
    assert_eq!(decoded.header.method, method_core::PING);
    assert_eq!(Ping::decode(&decoded.payload).unwrap(), Ping { token: 7 });
    assert!(codec.decode(&mut buf).unwrap().is_none());
}

#[test]
fn decode_waits_for_full_payload() {
    let mut codec = FrameCodec;
    let mut buf = BytesMut::new();
    codec.encode(ping_frame(), &mut buf).unwrap();
    let full = buf.split();
    let mut partial = BytesMut::from(&full[..17]);
    assert!(codec.decode(&mut partial).unwrap().is_none());
}

#[test]
fn decode_rejects_bad_magic() {
    let mut codec = FrameCodec;
    let mut buf = BytesMut::new();
    codec.encode(ping_frame(), &mut buf).unwrap();
    buf[0] = 0x00;
    assert!(matches!(codec.decode(&mut buf), Err(FrameError::Codec(_))));
}
