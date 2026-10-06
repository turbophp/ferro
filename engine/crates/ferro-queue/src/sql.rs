//! The `sql` store kind's two encodings (SPEC §24.3, decided at M7-G1a): how a row's `bigint` `id`
//! becomes the opaque `job_id` bytes on the wire, and how `(created_at, attempts)` becomes the opaque
//! token. Both are INTERNAL to the kind (D22 (b), D24): a client never interprets either, and a future
//! kind mints its own. Both decoders are STRICT — a byte string decodes iff it is exactly what the
//! encoder would have produced — and both refusals become the `InvalidHandle` terminal, before any
//! statement (§24.3 prerequisite (c)).
//!
//! # `job_id`: canonical decimal text
//!
//! The encoding of an `i64` id is **exactly `i64::to_string()`'s bytes** — ASCII digits, a leading
//! `-` only for a negative value, no leading zeros (`0` is the one zero), no `+`, no whitespace. So:
//!
//! - every `i64` has exactly ONE encoding, and the decoder accepts a byte string iff it is the
//!   encoding of some `i64` — prerequisite (a): two byte strings never name one row, so a dedup replay
//!   (G4) that re-encodes the stored id returns bytes identical to the first send's;
//! - it is 1 to 20 bytes (`-9223372036854775808`), far inside the wire's 1 024;
//! - the Laravel tier can hand Laravel the same digits stock `DatabaseQueue::push()` returns (§24.11,
//!   G5's to decide), and a `job_id` is safe in JSON and logs — the reasons §24.3 gives for weighing it.
//!
//! **Cost, stated:** up to 20 bytes where a fixed 8-byte binary form would always be 8 — at most 12
//! extra bytes per job on the wire — and the encoder allocates. Negative ids are representable
//! because the decoder must be total over what a `bigint` column can hold; Laravel's `bigIncrements`
//! never produces one.
//!
//! # token: 8 bytes, `created_at` above `attempts`
//!
//! `u64::to_be_bytes((created_at_bits << 16) | attempts_bits)`: the stored `created_at`'s 32 bits
//! (PG `integer` reinterpreted as `u32`; MySQL `INT UNSIGNED` as is) above `attempts`' 16 bits (PG
//! `smallint` reinterpreted as `u16`; MySQL `TINYINT UNSIGNED` widened). The top two bytes are always
//! zero. Both widths are EXACT for the stock layout — shape verification ([`crate::shape`]) refuses a
//! table whose columns would not fit them, so the token never truncates a stored value — and the
//! fence compares the decoded values against the row's columns (§24.3).
//!
//! The decoder refuses any length other than 8 (§24.3) **and any token whose top two bytes are not
//! zero** — G1a's refinement: no engine ever minted such a token, so it can only be a client defect,
//! and refusing it makes the token canonical too.

/// Why a `job_id` or token could not be decoded by the `sql` kind. Its `Display` is log-safe: it
/// never quotes the client's bytes (an opaque handle is not ours to print).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Undecodable {
    /// A `job_id` that is not the canonical decimal text of an `i64`.
    JobId,
    /// A token that is not 8 bytes with its top two bytes zero.
    Token,
}

impl std::fmt::Display for Undecodable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Undecodable::JobId => f.write_str(
                "the job_id is not one this store minted (the sql kind's job_id is the canonical \
                 decimal text of a 64-bit id)",
            ),
            Undecodable::Token => f.write_str(
                "the token is not one this store minted (the sql kind's token is 8 bytes)",
            ),
        }
    }
}

/// A row's `id`, as the `sql` kind names it on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobId(pub i64);

impl JobId {
    /// The longest encoding: `i64::MIN`'s 20 bytes. The frame clamp's per-job overhead counts it.
    pub const MAX_ENCODED_LEN: usize = 20;

    pub fn encode(self) -> Vec<u8> {
        self.0.to_string().into_bytes()
    }

    /// Accepts `b` iff it is `encode()` of some `i64`.
    pub fn decode(b: &[u8]) -> Result<JobId, Undecodable> {
        if b.is_empty() || b.len() > Self::MAX_ENCODED_LEN {
            return Err(Undecodable::JobId);
        }
        // `from_utf8` before `parse`: a non-ASCII byte is refused here rather than by the parser.
        let s = std::str::from_utf8(b).map_err(|_| Undecodable::JobId)?;
        let v: i64 = s.parse().map_err(|_| Undecodable::JobId)?;
        // The canonical check: `parse` accepts `+5`, `007` and `-0`; the encoder writes none of
        // them, so the round trip refuses all three.
        if v.to_string().as_bytes() != b {
            return Err(Undecodable::JobId);
        }
        Ok(JobId(v))
    }
}

/// The fence's two mutable-or-fixed columns, as the `sql` kind's token carries them: the raw BITS of
/// `created_at` (32) and `attempts` (16). [`Token::pg_created_at`]/[`Token::pg_attempts`] read them
/// back as the PostgreSQL column types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Token {
    pub created_at: u32,
    pub attempts: u16,
}

impl Token {
    pub const ENCODED_LEN: usize = 8;

    /// From a PostgreSQL row's `created_at integer` and `attempts smallint` (the stock layout).
    pub fn from_pg(created_at: i32, attempts: i16) -> Token {
        Token {
            created_at: created_at as u32,
            attempts: attempts as u16,
        }
    }

    pub fn pg_created_at(self) -> i32 {
        self.created_at as i32
    }

    pub fn pg_attempts(self) -> i16 {
        self.attempts as i16
    }

    pub fn encode(self) -> [u8; Self::ENCODED_LEN] {
        ((u64::from(self.created_at) << 16) | u64::from(self.attempts)).to_be_bytes()
    }

    pub fn decode(b: &[u8]) -> Result<Token, Undecodable> {
        let bytes: [u8; Self::ENCODED_LEN] = b.try_into().map_err(|_| Undecodable::Token)?;
        if bytes[0] != 0 || bytes[1] != 0 {
            return Err(Undecodable::Token);
        }
        let v = u64::from_be_bytes(bytes);
        Ok(Token {
            created_at: (v >> 16) as u32,
            attempts: (v & 0xFFFF) as u16,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_job_id_is_exactly_i64_to_string_and_round_trips_at_every_edge() {
        for v in [0, 1, 9, 10, 42, -1, -10, i64::MAX, i64::MIN, 1_000_000_007] {
            let b = JobId(v).encode();
            assert_eq!(b, v.to_string().into_bytes());
            assert_eq!(JobId::decode(&b), Ok(JobId(v)), "{v}");
        }
        assert_eq!(JobId(i64::MIN).encode().len(), JobId::MAX_ENCODED_LEN);
    }

    #[test]
    fn every_non_canonical_job_id_is_refused() {
        for bad in [
            &b""[..],
            b"007",
            b"00",
            b"-0",
            b"+5",
            b" 5",
            b"5 ",
            b"5\n",
            b"\t5",
            b"1_000",
            b"1e3",
            b"0x10",
            b"9223372036854775808",  // i64::MAX + 1
            b"-9223372036854775809", // i64::MIN - 1
            b"\xff",
            b"\xd9\xa5", // ARABIC-INDIC DIGIT FIVE: a digit, not an ASCII one
            b"-",
            b"--5",
            b"00000000000000000001",  // 20 bytes, a leading-zero form of 1
            b"123456789012345678901", // 21 bytes
        ] {
            assert_eq!(
                JobId::decode(bad),
                Err(Undecodable::JobId),
                "{:?}",
                String::from_utf8_lossy(bad)
            );
        }
    }

    /// SPEC §24.3 prerequisite (a): a dedup replay re-encodes the STORED id, so the bytes it returns
    /// must equal the first send's. With a canonical encoding, decode∘encode and encode∘decode are
    /// both identities on their domains.
    #[test]
    fn a_replayed_id_is_byte_identical() {
        for v in [1i64, 42, 9_223_372_036_854_775_807] {
            let first = JobId(v).encode();
            let replay = JobId(JobId::decode(&first).unwrap().0).encode();
            assert_eq!(first, replay);
        }
    }

    #[test]
    fn a_token_packs_created_at_above_attempts_big_endian_and_round_trips() {
        let t = Token::from_pg(1_790_000_000, 3);
        let b = t.encode();
        assert_eq!(&b[..2], &[0, 0], "top two bytes are zero");
        assert_eq!(&b[2..6], &1_790_000_000u32.to_be_bytes(), "created_at");
        assert_eq!(&b[6..], &3u16.to_be_bytes(), "attempts");
        assert_eq!(Token::decode(&b), Ok(t));
        // Signed PG values survive the bit reinterpretation exactly.
        for (c, a) in [(-1, -1), (i32::MIN, i16::MIN), (i32::MAX, i16::MAX), (0, 0)] {
            let t = Token::from_pg(c, a);
            let back = Token::decode(&t.encode()).unwrap();
            assert_eq!((back.pg_created_at(), back.pg_attempts()), (c, a));
        }
    }

    #[test]
    fn a_token_of_any_other_length_or_with_high_bytes_set_is_refused() {
        for len in [0usize, 1, 7, 9, 16, 1024] {
            assert_eq!(
                Token::decode(&vec![0; len]),
                Err(Undecodable::Token),
                "{len}"
            );
        }
        let mut b = Token::from_pg(5, 1).encode();
        b[0] = 1;
        assert_eq!(Token::decode(&b), Err(Undecodable::Token));
        let mut b = Token::from_pg(5, 1).encode();
        b[1] = 0x80;
        assert_eq!(Token::decode(&b), Err(Undecodable::Token));
    }

    /// The golden vectors carry an `sql`-kind token minted by hand in `gen-vectors`
    /// (`sql_token`). This ties them to THIS encoder: if the layout here changes, the vector the
    /// client side is locked against no longer describes what the engine mints.
    #[test]
    fn the_golden_vectors_sql_tokens_are_this_encoders_output() {
        use ferro_proto::messages::{FencedRequest, Outcome, ReserveResponse};
        let dir =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../proto/vectors");
        let load = |name: &str| -> Vec<u8> {
            let v: serde_json::Value = serde_json::from_str(
                &std::fs::read_to_string(dir.join(format!("{name}.json"))).unwrap(),
            )
            .unwrap();
            let hex = v["frame_hex"].as_str().unwrap();
            (16 * 2..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
                .collect()
        };
        let Outcome::Ok(body) = Outcome::decode(&load("queue_reserve_response")).unwrap() else {
            panic!("queue_reserve_response is an Outcome::Ok");
        };
        let job = &ReserveResponse::decode(&body).unwrap().jobs[0];
        let created_at = i32::try_from(job.created_at).unwrap();
        let attempts = i16::try_from(job.attempts).unwrap();
        assert_eq!(
            job.token,
            Token::from_pg(created_at, attempts).encode().to_vec()
        );
        assert_eq!(
            JobId::decode(&job.job_id).map(JobId::encode),
            Ok(job.job_id.clone())
        );
        let ack = FencedRequest::decode(&load("queue_ack_request")).unwrap();
        let t = Token::decode(&ack.token).unwrap();
        assert_eq!((t.pg_created_at(), t.pg_attempts()), (1_790_000_000, 1));
        assert_eq!(JobId::decode(&ack.job_id), Ok(JobId(42)));
    }
}
