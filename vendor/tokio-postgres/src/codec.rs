use bytes::{Buf, Bytes, BytesMut};
use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend;
use postgres_protocol::message::frontend::CopyData;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use tokio_util::codec::{Decoder, Encoder};

pub enum FrontendMessage {
    Raw(Bytes),
    CopyData(CopyData<Box<dyn Buf + Send>>),
}

pub enum BackendMessage {
    Normal {
        messages: BackendMessages,
        request_complete: bool,
    },
    Async(backend::Message),
}

pub struct BackendMessages(BytesMut);

impl BackendMessages {
    pub fn empty() -> BackendMessages {
        BackendMessages(BytesMut::new())
    }
}

impl FallibleIterator for BackendMessages {
    type Item = backend::Message;
    type Error = io::Error;

    fn next(&mut self) -> io::Result<Option<backend::Message>> {
        backend::Message::parse(&mut self.0)
    }
}

/// FERRO M1-S1 fork (see `/UPSTREAM_PR.md`, drop when upstream merges): `tx_status` mirrors the
/// most recently seen `ReadyForQuery` status byte (`I`/`T`/`E`) into a shared atomic so
/// `Client::transaction_status()` can read it synchronously — stock tokio-postgres parses this
/// byte off the wire in `decode` below and discards it.
pub struct PostgresCodec {
    pub tx_status: Arc<AtomicU8>,
}

impl Encoder<FrontendMessage> for PostgresCodec {
    type Error = io::Error;

    fn encode(&mut self, item: FrontendMessage, dst: &mut BytesMut) -> io::Result<()> {
        match item {
            FrontendMessage::Raw(buf) => dst.extend_from_slice(&buf),
            FrontendMessage::CopyData(data) => data.write(dst),
        }

        Ok(())
    }
}

impl Decoder for PostgresCodec {
    type Item = BackendMessage;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<BackendMessage>, io::Error> {
        let mut idx = 0;
        let mut request_complete = false;

        while let Some(header) = backend::Header::parse(&src[idx..])? {
            let len = header.len() as usize + 1;
            if src[idx..].len() < len {
                break;
            }

            match header.tag() {
                backend::NOTICE_RESPONSE_TAG
                | backend::NOTIFICATION_RESPONSE_TAG
                | backend::PARAMETER_STATUS_TAG => {
                    if idx == 0 {
                        let message = backend::Message::parse(src)?.unwrap();
                        return Ok(Some(BackendMessage::Async(message)));
                    } else {
                        break;
                    }
                }
                _ => {}
            }

            idx += len;

            if header.tag() == backend::READY_FOR_QUERY_TAG {
                // FERRO M1-S1: the RFQ body is exactly 1 byte (the status), so it's the last byte
                // of this frame — `src[idx - len..idx]`. Bounds are already guaranteed by the
                // `src[idx..].len() < len` check above (this frame is fully buffered).
                self.tx_status.store(src[idx - 1], Ordering::Relaxed);
                request_complete = true;
                break;
            }
        }

        if idx == 0 {
            Ok(None)
        } else {
            Ok(Some(BackendMessage::Normal {
                messages: BackendMessages(src.split_to(idx)),
                request_complete,
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A raw `ReadyForQuery` wire frame: tag `Z` (1 byte), big-endian i32 length = 5 (the length
    /// field counts itself + the 1-byte status body, per the Postgres frontend/backend protocol),
    /// then the 1-byte status.
    fn rfq_frame(status: u8) -> BytesMut {
        BytesMut::from(&[b'Z', 0, 0, 0, 5, status][..])
    }

    #[test]
    fn decode_ready_for_query_stores_status_byte() {
        let tx_status = Arc::new(AtomicU8::new(0));
        let mut codec = PostgresCodec {
            tx_status: Arc::clone(&tx_status),
        };

        let mut buf = rfq_frame(b'T');
        let msg = codec
            .decode(&mut buf)
            .expect("decode must not error")
            .expect("a fully-buffered RFQ frame decodes to Some");
        match msg {
            BackendMessage::Normal {
                request_complete, ..
            } => assert!(
                request_complete,
                "ReadyForQuery must set request_complete = true"
            ),
            BackendMessage::Async(_) => panic!("ReadyForQuery is never an Async message"),
        }
        assert_eq!(
            tx_status.load(Ordering::Relaxed),
            b'T',
            "decode must store the RFQ status byte into the shared atomic"
        );
    }

    #[test]
    fn decode_ready_for_query_tracks_idle_and_error_bytes() {
        for status in [b'I', b'E'] {
            let tx_status = Arc::new(AtomicU8::new(0));
            let mut codec = PostgresCodec {
                tx_status: Arc::clone(&tx_status),
            };
            let mut buf = rfq_frame(status);
            codec.decode(&mut buf).expect("decode must not error");
            assert_eq!(
                tx_status.load(Ordering::Relaxed),
                status,
                "the atomic must reflect whichever RFQ status byte was on the wire"
            );
        }
    }
}
