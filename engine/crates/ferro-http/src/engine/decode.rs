//! Content decoding (SPEC §23.9.2; slice M6-F4b).
//!
//! When a request carries `decode = true` and the response's `Content-Encoding` is exactly ONE of
//! `gzip`, `x-gzip` or `deflate` (zlib, with a raw-deflate fallback), the engine decodes the body
//! incrementally and removes `content-encoding` and `content-length` from the head, reporting both
//! in `HttpHead.decoded`. Every other encoding — `br`, `zstd`, `identity`, a list (`gzip, br`), or
//! two `Content-Encoding` fields — passes through with `decoded = nil`.
//!
//! **The bounds §23.9.2 names, and how each is held:**
//!
//! - **Memory is bounded by the window.** The decoder emits at most [`super::MAX_BODY_CHUNK`] bytes
//!   per step and holds no more than one step's output and the unconsumed tail of one input chunk.
//!   The engine sends each step as one `BODY` frame through the credit gauntlet BEFORE asking for
//!   the next, so a frame parked on credit stops decompression. That is why this uses the low-level
//!   [`flate2::Decompress`] with a bounded output buffer rather than `flate2::write::GzDecoder`,
//!   which writes ALL the output for one input chunk at once (a 256 KiB gzip chunk of zeros is about
//!   256 MiB out).
//! - **CPU is bounded by the deadline.** The engine checks its cancel token and total deadline
//!   between steps, so a bomb that is never short of credit still ends at the request's deadline.
//!   That needs every step to be short in CPU as well as in output, so a step is bounded by its
//!   INPUT and its TIME too: it ends after [`STEP_MAX_INPUT`] bytes of input, [`STEP_MAX_MEMBERS`]
//!   gzip member headers or [`STEP_MAX_TIME`], whichever is first, returning [`Next::Yield`] if it
//!   produced nothing; inflate is handed its input [`INFLATE_SLICE`] bytes at a time so the time is
//!   checked often. A byte bound alone is not a CPU bound: empty gzip members (20 bytes, nothing
//!   out) were consumed a whole network read per step (review round 1), and EMPTY fixed-Huffman
//!   deflate blocks (about 10 bits each, ~2.5 µs of inflate per input byte) made even 64 KiB a
//!   ~160 ms step (review round 2) — no output, so no credit parking, no check and no yield, a core
//!   burned until the deadline (§22.2 (db)). The inflater is reset per member, never reallocated,
//!   and a gzip header is parsed incrementally, each byte examined once however it is split across
//!   reads.
//! - **A corrupt stream is `ResponseIncomplete` (`decode`)**: a bad gzip header, a CRC32 or length
//!   mismatch, an inflate error, a stream truncated at the end of the body, or bytes after the end
//!   of the compressed stream that do not begin another gzip member.
//!
//! An entirely EMPTY body is not an error under any encoding: a `HEAD` response, a 204 or a 304
//! routinely carries `Content-Encoding: gzip` with no body at all.

use bytes::Bytes;
use flate2::{Crc, Decompress, FlushDecompress, Status};

/// The longest gzip header the decoder buffers (the optional `FEXTRA`, `FNAME` and `FCOMMENT`
/// fields make its length unbounded on the wire). A header that has not ended by here is refused.
pub const MAX_GZIP_HEADER: usize = 64 * 1024;

/// The most input one step consumes: a step that has consumed this much ends, with a (possibly
/// short) chunk or [`Next::Yield`]. A step over input that inflates to MORE than `max_chunk` is
/// bounded by its output instead, as before.
pub const STEP_MAX_INPUT: usize = 64 * 1024;

/// The most time one step spends before it ends with what it has (checked between inflate slices
/// and member boundaries, once the step has made progress). Bounds a step over input that costs
/// much more CPU per byte than it produces, which no byte bound can.
pub const STEP_MAX_TIME: std::time::Duration = std::time::Duration::from_millis(1);

/// The most input one inflate call is handed, so [`STEP_MAX_TIME`] is checked at least every this
/// many bytes.
pub const INFLATE_SLICE: usize = 256;

/// The most gzip member headers one step starts. Each costs a header parse and an inflater reset
/// whatever its size, so this bounds a step of tiny members that the input bound would not.
pub const STEP_MAX_MEMBERS: usize = 64;

/// What one decode step produced.
#[derive(Debug, PartialEq, Eq)]
pub enum Next {
    /// Decoded bytes: non-empty, at most `max_chunk`.
    Chunk(Vec<u8>),
    /// Work was done but nothing was decoded, and the step's input or member bound was reached:
    /// call again (after checking the deadline and the stop, and yielding).
    Yield,
    /// The decoder needs more input.
    NeedInput,
}

/// How one step ended, before the output is copied out.
enum StepEnd {
    NeedInput,
    Bound,
}

/// Which decoding a response gets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Coding {
    Gzip,
    Deflate,
}

/// The response's coding, if the engine decodes it: exactly one `Content-Encoding` field whose
/// whole value is (ASCII case-insensitively) `gzip`, `x-gzip` or `deflate`.
pub fn coding_of(headers: &http::HeaderMap) -> Option<(Coding, String)> {
    let mut all = headers.get_all(http::header::CONTENT_ENCODING).iter();
    let value = all.next()?;
    if all.next().is_some() {
        return None; // stacked across fields: pass through
    }
    let s = value.to_str().ok()?.trim();
    let coding = if s.eq_ignore_ascii_case("gzip") || s.eq_ignore_ascii_case("x-gzip") {
        Coding::Gzip
    } else if s.eq_ignore_ascii_case("deflate") {
        Coding::Deflate
    } else {
        return None;
    };
    Some((coding, s.to_string()))
}

/// Why a body could not be decoded. Carries no data from the body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodeError {
    Header,
    Inflate,
    Checksum,
    Truncated,
    TrailingData,
}

#[derive(Debug)]
enum State {
    /// Gzip: waiting for (another) member header. `first` is false after a complete member, where
    /// running out of input is a clean end.
    GzHeader { first: bool },
    /// Deflate: the first two bytes decide zlib or raw.
    Sniff,
    /// Inflating a member (gzip: raw deflate) or the whole stream (deflate).
    Inflate,
    /// Gzip: waiting for the 8-byte trailer (CRC32, ISIZE).
    GzTrailer,
    /// Deflate: the stream ended; nothing may follow.
    Done,
}

/// An incremental, bounded decoder. Feed it each body chunk with [`Decoder::feed`], then drain it
/// with [`Decoder::next_chunk`] until that returns [`Next::NeedInput`]; at the end of the body,
/// [`Decoder::finish`].
pub struct Decoder {
    coding: Coding,
    state: State,
    inflate: Decompress,
    crc: Crc,
    /// The gzip member header being parsed.
    hdr: GzHeader,
    /// [`STEP_MAX_TIME`]; tests lift it to reach the byte and member bounds deterministically.
    max_step_time: std::time::Duration,
    /// Unconsumed input. Only a short tail (a partial trailer or sniff, at most 8 bytes) is ever
    /// carried into the next `feed`: a header is consumed as it is parsed and inflate consumes what
    /// it is given.
    pending: Bytes,
    pos: usize,
    seen_input: bool,
    /// The one output buffer, `max_chunk` long.
    buf: Vec<u8>,
}

impl Decoder {
    pub fn new(coding: Coding, max_chunk: usize) -> Self {
        Decoder {
            coding,
            state: match coding {
                Coding::Gzip => State::GzHeader { first: true },
                Coding::Deflate => State::Sniff,
            },
            inflate: Decompress::new(false),
            crc: Crc::new(),
            hdr: GzHeader::new(),
            max_step_time: STEP_MAX_TIME,
            pending: Bytes::new(),
            pos: 0,
            seen_input: false,
            buf: vec![0u8; max_chunk.max(1)],
        }
    }

    /// Hand the decoder the next body chunk.
    pub fn feed(&mut self, input: Bytes) {
        if input.is_empty() {
            return;
        }
        self.seen_input = true;
        if self.pos >= self.pending.len() {
            self.pending = input;
        } else {
            // A short tail the previous step could not use yet: join it to the new input.
            let mut joined = Vec::with_capacity(self.pending.len() - self.pos + input.len());
            joined.extend_from_slice(&self.pending[self.pos..]);
            joined.extend_from_slice(&input);
            self.pending = Bytes::from(joined);
        }
        self.pos = 0;
    }

    /// Without the time bound: only the byte and member bounds end a step (tests).
    #[cfg(test)]
    fn without_time_bound(mut self) -> Self {
        self.max_step_time = std::time::Duration::MAX;
        self
    }

    fn rest(&self) -> &[u8] {
        &self.pending[self.pos..]
    }

    /// One bounded step: the next decoded chunk (non-empty, at most `max_chunk` bytes), a
    /// [`Next::Yield`] when the step's input or member bound was reached with nothing decoded, or
    /// [`Next::NeedInput`].
    pub fn next_chunk(&mut self) -> Result<Next, DecodeError> {
        // A fixed-size buffer, allocated once: one step can never emit more than `max_chunk`,
        // whatever the ratio, and only the filled part is copied out.
        let mut out = std::mem::take(&mut self.buf);
        let mut filled = 0usize;
        let r = self.step(&mut out, &mut filled);
        let chunk = (filled > 0).then(|| out[..filled].to_vec());
        self.buf = out;
        let end = r?;
        Ok(match (chunk, end) {
            (Some(c), _) => Next::Chunk(c),
            (None, StepEnd::Bound) => Next::Yield,
            (None, StepEnd::NeedInput) => Next::NeedInput,
        })
    }

    fn step(&mut self, out: &mut [u8], filled: &mut usize) -> Result<StepEnd, DecodeError> {
        // Input consumed by THIS step: `pos` only moves forward within a step (`feed` is not
        // called during one), so the difference is exact.
        let start = self.pos;
        let mut members = 0usize;
        let began = std::time::Instant::now();
        loop {
            if self.pos - start >= STEP_MAX_INPUT {
                return Ok(StepEnd::Bound);
            }
            // The time bound, once the step has done something (so every step makes progress).
            if (self.pos > start || *filled > 0) && began.elapsed() >= self.max_step_time {
                return Ok(StepEnd::Bound);
            }
            match self.state {
                State::GzHeader { .. } => {
                    if self.rest().is_empty() {
                        return Ok(StepEnd::NeedInput);
                    }
                    if members == STEP_MAX_MEMBERS && !self.hdr.started() {
                        return Ok(StepEnd::Bound);
                    }
                    let (used, done) = self.hdr.advance(&self.pending[self.pos..])?;
                    self.pos += used;
                    if !done {
                        return Ok(StepEnd::NeedInput);
                    }
                    members += 1;
                    self.hdr = GzHeader::new();
                    // Reset, never reallocate: the inflater's state is ~43 KB, and a member can be
                    // 20 bytes.
                    self.inflate.reset(false);
                    self.crc = Crc::new();
                    self.state = State::Inflate;
                }
                State::Sniff => {
                    if self.rest().len() < 2 {
                        return Ok(StepEnd::NeedInput);
                    }
                    // RFC 1950: CM = 8, CINFO <= 7, FCHECK makes the pair a multiple of 31, and no
                    // preset dictionary. Anything else is taken as raw deflate (the fallback servers
                    // that send RFC 1951 under `deflate` need).
                    let (cmf, flg) = (self.rest()[0], self.rest()[1]);
                    let zlib = cmf & 0x0f == 8
                        && cmf >> 4 <= 7
                        && ((u16::from(cmf) << 8) | u16::from(flg)) % 31 == 0
                        && flg & 0x20 == 0;
                    self.inflate.reset(zlib);
                    self.state = State::Inflate;
                }
                State::Inflate => {
                    if *filled == out.len() {
                        return Ok(StepEnd::Bound);
                    }
                    // Hand inflate a slice of at most `INFLATE_SLICE` within the step's budget, so ONE
                    // call can neither consume a whole network read nor run long between two time
                    // checks (a run of empty deflate blocks inflates to nothing, like a run of
                    // empty members, and costs far more CPU per byte).
                    let budget = (STEP_MAX_INPUT - (self.pos - start)).min(INFLATE_SLICE);
                    let avail = self.pending.len() - self.pos;
                    let end = self.pos + avail.min(budget);
                    let in_before = self.inflate.total_in();
                    let out_before = self.inflate.total_out();
                    let status = self
                        .inflate
                        .decompress(
                            &self.pending[self.pos..end],
                            &mut out[*filled..],
                            FlushDecompress::None,
                        )
                        .map_err(|_| DecodeError::Inflate)?;
                    let consumed = usize::try_from(self.inflate.total_in() - in_before)
                        .map_err(|_| DecodeError::Inflate)?;
                    let produced = usize::try_from(self.inflate.total_out() - out_before)
                        .map_err(|_| DecodeError::Inflate)?;
                    self.pos += consumed;
                    if self.coding == Coding::Gzip {
                        self.crc.update(&out[*filled..*filled + produced]);
                    }
                    *filled += produced;
                    match status {
                        Status::StreamEnd => {
                            self.state = match self.coding {
                                Coding::Gzip => State::GzTrailer,
                                Coding::Deflate => State::Done,
                            };
                        }
                        Status::Ok | Status::BufError => {
                            if produced == 0 && consumed == 0 {
                                // No progress: inflate wants input (room is handled above). If the
                                // slice was cut short, more input IS here: end the step, and the next
                                // one continues.
                                return Ok(if avail > budget {
                                    StepEnd::Bound
                                } else {
                                    StepEnd::NeedInput
                                });
                            }
                        }
                    }
                }
                State::GzTrailer => {
                    if self.rest().len() < 8 {
                        return Ok(StepEnd::NeedInput);
                    }
                    let t = &self.rest()[..8];
                    let crc = u32::from_le_bytes([t[0], t[1], t[2], t[3]]);
                    let isize = u32::from_le_bytes([t[4], t[5], t[6], t[7]]);
                    if crc != self.crc.sum() || isize != self.crc.amount() {
                        return Err(DecodeError::Checksum);
                    }
                    self.pos += 8;
                    // RFC 1952 §2.2: a gzip file is a series of members.
                    self.state = State::GzHeader { first: false };
                }
                State::Done => {
                    if !self.rest().is_empty() {
                        return Err(DecodeError::TrailingData);
                    }
                    return Ok(StepEnd::NeedInput);
                }
            }
        }
    }

    /// The body ended. `Ok` only at a clean end: no input at all, or a complete stream (gzip: at a
    /// member boundary) with nothing left over.
    pub fn finish(&self) -> Result<(), DecodeError> {
        if !self.seen_input {
            return Ok(());
        }
        match self.state {
            State::GzHeader { first: false } if self.rest().is_empty() && !self.hdr.started() => {
                Ok(())
            }
            State::Done if self.rest().is_empty() => Ok(()),
            State::GzHeader { first: false } | State::Done => Err(DecodeError::TrailingData),
            _ => Err(DecodeError::Truncated),
        }
    }
}

const FHCRC: u8 = 0x02;
const FEXTRA: u8 = 0x04;
const FNAME: u8 = 0x08;
const FCOMMENT: u8 = 0x10;
const RESERVED: u8 = 0xe0;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HdrStage {
    /// ID1 ID2 CM FLG MTIME(4) XFL OS.
    Fixed,
    XLen,
    Extra,
    Name,
    Comment,
    Hcrc,
    Done,
}

/// An RFC 1952 §2.3 member header, parsed INCREMENTALLY: every byte is examined once and consumed,
/// however the header is split across reads. (Re-joining and re-scanning a partial header on every
/// `feed` was quadratic in its length — 1-byte chunks of a header with a ~64 KiB `FNAME` cost
/// seconds; review round 2, §22.2 (db).)
#[derive(Debug)]
struct GzHeader {
    stage: HdrStage,
    flg: u8,
    /// Header bytes consumed so far; bounded by [`MAX_GZIP_HEADER`].
    len: usize,
    /// Bytes of a fixed-size stage seen so far.
    got: usize,
    two: [u8; 2],
    extra_left: usize,
    /// CRC32 of the header bytes before `FHCRC` (its low 16 bits are the `FHCRC` value).
    crc: Crc,
}

impl GzHeader {
    fn new() -> Self {
        GzHeader {
            stage: HdrStage::Fixed,
            flg: 0,
            len: 0,
            got: 0,
            two: [0; 2],
            extra_left: 0,
            crc: Crc::new(),
        }
    }

    fn started(&self) -> bool {
        self.len > 0
    }

    /// The stage after `from`, by the header's flags.
    fn after(&self, from: HdrStage) -> HdrStage {
        let order = [
            (HdrStage::XLen, FEXTRA),
            (HdrStage::Name, FNAME),
            (HdrStage::Comment, FCOMMENT),
            (HdrStage::Hcrc, FHCRC),
        ];
        let from_ix = match from {
            HdrStage::Fixed => 0,
            HdrStage::Extra => 1,
            HdrStage::Name => 2,
            HdrStage::Comment => 3,
            _ => 4,
        };
        order[from_ix..]
            .iter()
            .find(|(_, flag)| self.flg & flag != 0)
            .map_or(HdrStage::Done, |(stage, _)| *stage)
    }

    fn eat(&mut self, b: &[u8]) {
        self.crc.update(b);
        self.len += b.len();
    }

    /// Consume header bytes from `b`: `(used, true)` when the header completed after `used` bytes,
    /// `(b.len(), false)` when all of `b` was consumed and more is needed.
    fn advance(&mut self, b: &[u8]) -> Result<(usize, bool), DecodeError> {
        let mut i = 0;
        while i < b.len() && self.stage != HdrStage::Done {
            match self.stage {
                HdrStage::Fixed => {
                    let c = b[i];
                    // The magic is checked as soon as it is visible, so a non-gzip body fails at
                    // its first bytes.
                    let bad = match self.got {
                        0 => c != 0x1f,
                        1 => c != 0x8b,
                        2 => c != 8,
                        3 => c & RESERVED != 0,
                        _ => false,
                    };
                    if bad {
                        return Err(DecodeError::Header);
                    }
                    if self.got == 3 {
                        self.flg = c;
                    }
                    self.eat(&b[i..=i]);
                    i += 1;
                    self.got += 1;
                    if self.got == 10 {
                        self.got = 0;
                        self.stage = self.after(HdrStage::Fixed);
                    }
                }
                HdrStage::XLen => {
                    self.two[self.got] = b[i];
                    self.eat(&b[i..=i]);
                    i += 1;
                    self.got += 1;
                    if self.got == 2 {
                        self.got = 0;
                        self.extra_left = usize::from(u16::from_le_bytes(self.two));
                        self.stage = if self.extra_left == 0 {
                            self.after(HdrStage::Extra)
                        } else {
                            HdrStage::Extra
                        };
                    }
                }
                HdrStage::Extra => {
                    let n = self.extra_left.min(b.len() - i);
                    self.eat(&b[i..i + n]);
                    i += n;
                    self.extra_left -= n;
                    if self.extra_left == 0 {
                        self.stage = self.after(HdrStage::Extra);
                    }
                }
                HdrStage::Name | HdrStage::Comment => {
                    let field = self.stage;
                    match b[i..].iter().position(|&c| c == 0) {
                        Some(z) => {
                            self.eat(&b[i..=i + z]);
                            i += z + 1;
                            self.stage = self.after(field);
                        }
                        None => {
                            self.eat(&b[i..]);
                            i = b.len();
                        }
                    }
                }
                HdrStage::Hcrc => {
                    self.two[self.got] = b[i];
                    i += 1;
                    self.len += 1;
                    self.got += 1;
                    if self.got == 2 {
                        if (self.crc.sum() & 0xffff) as u16 != u16::from_le_bytes(self.two) {
                            return Err(DecodeError::Header);
                        }
                        self.stage = HdrStage::Done;
                    }
                }
                HdrStage::Done => unreachable!("the loop stops at Done"),
            }
            if self.stage != HdrStage::Done && self.len >= MAX_GZIP_HEADER {
                return Err(DecodeError::Header);
            }
        }
        Ok((i, self.stage == HdrStage::Done))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::Compression;
    use std::io::Write;

    fn gzip(data: &[u8]) -> Vec<u8> {
        let mut e = flate2::write::GzEncoder::new(Vec::new(), Compression::default());
        e.write_all(data).unwrap();
        e.finish().unwrap()
    }
    fn zlib(data: &[u8]) -> Vec<u8> {
        let mut e = flate2::write::ZlibEncoder::new(Vec::new(), Compression::default());
        e.write_all(data).unwrap();
        e.finish().unwrap()
    }
    fn raw(data: &[u8]) -> Vec<u8> {
        let mut e = flate2::write::DeflateEncoder::new(Vec::new(), Compression::default());
        e.write_all(data).unwrap();
        e.finish().unwrap()
    }

    /// Decode `wire` delivered in `piece`-byte chunks; returns the output and every chunk's size.
    fn run(
        coding: Coding,
        wire: &[u8],
        piece: usize,
        max: usize,
    ) -> Result<(Vec<u8>, usize), DecodeError> {
        let mut d = Decoder::new(coding, max);
        let mut out = Vec::new();
        let mut biggest = 0;
        for c in wire.chunks(piece.max(1)) {
            d.feed(Bytes::copy_from_slice(c));
            loop {
                match d.next_chunk()? {
                    Next::Chunk(chunk) => {
                        assert!(!chunk.is_empty() && chunk.len() <= max);
                        biggest = biggest.max(chunk.len());
                        out.extend_from_slice(&chunk);
                    }
                    Next::Yield => {}
                    Next::NeedInput => break,
                }
            }
        }
        d.finish()?;
        Ok((out, biggest))
    }

    fn sample() -> Vec<u8> {
        (0..200_000u32)
            .flat_map(|i| (i % 251).to_le_bytes())
            .collect()
    }

    #[test]
    fn gzip_zlib_and_raw_deflate_round_trip_at_every_chunking() {
        let data = sample();
        for piece in [1, 2, 7, 4096, 1 << 20] {
            for (coding, wire) in [
                (Coding::Gzip, gzip(&data)),
                (Coding::Deflate, zlib(&data)),
                (Coding::Deflate, raw(&data)),
            ] {
                if piece == 1 && wire.len() > 300_000 {
                    continue;
                }
                let (out, _) = run(coding, &wire, piece, 64 * 1024).unwrap();
                assert!(out == data, "{coding:?} piece={piece}");
            }
        }
    }

    /// The memory bound: one step never emits more than `max_chunk`, however compressible the input
    /// (a 64 MiB run of zeros is ~64 KiB of gzip — fed in ONE chunk here).
    #[test]
    fn a_bomb_is_emitted_in_bounded_steps() {
        let zeros = vec![0u8; 64 * 1024 * 1024];
        let wire = gzip(&zeros);
        assert!(wire.len() < 128 * 1024);
        let mut d = Decoder::new(Coding::Gzip, 256 * 1024);
        d.feed(Bytes::from(wire));
        let mut total = 0usize;
        let mut steps = 0usize;
        loop {
            match d.next_chunk().unwrap() {
                Next::Chunk(c) => {
                    assert!(c.len() <= 256 * 1024);
                    total += c.len();
                    steps += 1;
                }
                Next::Yield => {}
                Next::NeedInput => break,
            }
        }
        d.finish().unwrap();
        assert_eq!(total, zeros.len());
        assert!(steps >= 256, "{steps} steps");
    }

    /// The CPU bound (review round): a step is bounded by its INPUT, not only its output. A network
    /// read made of EMPTY gzip members (each a valid member that inflates to nothing) used to be
    /// consumed whole in one step — thousands of members, no output, so no credit parking and no
    /// check of the deadline or the stop in between. Now one step starts at most
    /// [`STEP_MAX_MEMBERS`] members and consumes at most [`STEP_MAX_INPUT`] bytes, returning
    /// [`Next::Yield`]; the stream still decodes to its (empty) whole and finishes cleanly. Counted,
    /// not timed. The same for a deflate stream of empty stored blocks, which one inflate call
    /// would otherwise consume whole. Run with the time bound lifted, so these two bounds — not
    /// [`STEP_MAX_TIME`], which in a debug build ends such a step first — are what is tested.
    #[test]
    fn a_step_is_bounded_by_its_input_when_nothing_is_decoded() {
        let member = gzip(b"");
        let wire = member.repeat((256 * 1024) / member.len());
        let mut d = Decoder::new(Coding::Gzip, 256 * 1024).without_time_bound();
        d.feed(Bytes::from(wire.clone()));
        let before = d.rest().len();
        assert_eq!(d.next_chunk().unwrap(), Next::Yield);
        let used = before - d.rest().len();
        assert!(
            used <= STEP_MAX_MEMBERS * member.len() && used <= STEP_MAX_INPUT,
            "one step consumed {used} of {before} bytes"
        );
        let mut steps = 1;
        loop {
            match d.next_chunk().unwrap() {
                Next::Chunk(c) => panic!("decoded {} bytes from empty members", c.len()),
                Next::Yield => steps += 1,
                Next::NeedInput => break,
            }
        }
        d.finish().unwrap();
        assert!(
            steps >= wire.len() / member.len() / STEP_MAX_MEMBERS,
            "{steps}"
        );

        // Raw deflate: 64 Ki non-final EMPTY stored blocks (5 bytes each: BFINAL=0 BTYPE=00, LEN=0,
        // NLEN=0xffff), then one final empty block.
        let mut raw = [0u8, 0, 0, 0xff, 0xff].repeat(64 * 1024);
        raw.extend_from_slice(&[1, 0, 0, 0xff, 0xff]);
        let mut d = Decoder::new(Coding::Deflate, 256 * 1024).without_time_bound();
        d.feed(Bytes::from(raw.clone()));
        let before = d.rest().len();
        assert_eq!(d.next_chunk().unwrap(), Next::Yield);
        let used = before - d.rest().len();
        assert!(
            used <= STEP_MAX_INPUT,
            "one step consumed {used} of {before} bytes"
        );
        loop {
            match d.next_chunk().unwrap() {
                Next::Chunk(c) => panic!("decoded {} bytes from empty blocks", c.len()),
                Next::Yield => {}
                Next::NeedInput => break,
            }
        }
        d.finish().unwrap();
    }

    /// A deflate stream of EMPTY fixed-Huffman blocks (what `FlushCompress::Partial` on no input
    /// emits: about 10 bits each), zlib-wrapped or raw.
    fn empty_static_blocks(zlib: bool, at_least: usize) -> Vec<u8> {
        let mut c = flate2::Compress::new(Compression::default(), zlib);
        let mut out = Vec::new();
        let mut buf = [0u8; 64];
        while out.len() < at_least {
            let before = c.total_out();
            c.compress(&[], &mut buf, flate2::FlushCompress::Partial)
                .unwrap();
            let n = usize::try_from(c.total_out() - before).unwrap();
            out.extend_from_slice(&buf[..n]);
        }
        out
    }

    /// The TIME bound (review round 2): empty fixed-Huffman blocks cost ~2.5 µs of inflate per
    /// input byte (release) and decode to nothing, so the 64 KiB input bound alone allowed a
    /// ~160 ms step. Now a step ends after [`STEP_MAX_TIME`], with inflate handed at most
    /// [`INFLATE_SLICE`] bytes per call. Expressed as a COUNT, not a clock: the first step consumes
    /// at most a quarter of the input bound — at the measured cost a step that ran to the byte
    /// bound would take ~40× the time bound — and returns `Yield`. Both under the zlib sniff and
    /// the raw-deflate fallback.
    #[test]
    fn a_step_is_bounded_by_time_over_empty_fixed_huffman_blocks() {
        for zlib in [true, false] {
            let wire = empty_static_blocks(zlib, STEP_MAX_INPUT + 16 * 1024);
            let mut d = Decoder::new(Coding::Deflate, 256 * 1024);
            d.feed(Bytes::from(wire));
            let before = d.rest().len();
            assert_eq!(d.next_chunk().unwrap(), Next::Yield, "zlib={zlib}");
            let used = before - d.rest().len();
            assert!(
                used > 0 && used <= STEP_MAX_INPUT / 4,
                "zlib={zlib}: one step consumed {used} bytes of empty fixed-Huffman blocks"
            );
        }
    }

    /// A partial gzip header is consumed as it is parsed, never re-joined and re-scanned (review
    /// round 2: four members with a ~60 KiB `FNAME` each, fed one byte at a time, cost 3.24 s —
    /// quadratic). Counted, not timed: the input the decoder still holds after each feed, summed
    /// over every feed, stays linear in the body's length (it was ~n²/2), and the body decodes.
    #[test]
    fn a_partial_gzip_header_is_not_rescanned_per_feed() {
        let mut wire = Vec::new();
        for i in 0..4 {
            let mut e = flate2::GzBuilder::new()
                .filename(vec![b'a' + i; 60_000])
                .comment("c")
                .write(Vec::new(), Compression::fast());
            e.write_all(b"x").unwrap();
            wire.extend(e.finish().unwrap());
        }
        let mut d = Decoder::new(Coding::Gzip, 64 * 1024);
        let mut held = 0usize;
        let mut out = Vec::new();
        for b in wire.chunks(1) {
            d.feed(Bytes::copy_from_slice(b));
            loop {
                match d.next_chunk().unwrap() {
                    Next::Chunk(c) => out.extend(c),
                    Next::Yield => {}
                    Next::NeedInput => break,
                }
            }
            held += d.rest().len();
        }
        d.finish().unwrap();
        assert_eq!(out, b"xxxx");
        assert!(
            held <= 16 * wire.len(),
            "{held} bytes held across {} one-byte feeds",
            wire.len()
        );
    }

    #[test]
    fn multi_member_gzip_and_header_fields_are_accepted() {
        let mut wire = gzip(b"hello ");
        wire.extend(gzip(b"world"));
        assert_eq!(run(Coding::Gzip, &wire, 3, 16).unwrap().0, b"hello world");
        // FEXTRA + FNAME + FCOMMENT + FHCRC.
        let mut e = flate2::GzBuilder::new()
            .extra(vec![1, 2, 3])
            .filename("f.txt")
            .comment("c")
            .operating_system(3)
            .write(Vec::new(), Compression::fast());
        e.write_all(b"fields").unwrap();
        let wire = e.finish().unwrap();
        assert_eq!(run(Coding::Gzip, &wire, 1, 16).unwrap().0, b"fields");
    }

    #[test]
    fn corrupt_truncated_and_trailing_streams_are_refused() {
        let data = sample();
        let good = gzip(&data);
        // Truncated anywhere after the header.
        for cut in [5, 12, good.len() / 2, good.len() - 4, good.len() - 1] {
            let e = run(Coding::Gzip, &good[..cut], 4096, 65536).unwrap_err();
            assert!(matches!(e, DecodeError::Truncated), "cut {cut}: {e:?}");
        }
        // A flipped CRC byte.
        let mut bad = good.clone();
        let n = bad.len();
        bad[n - 6] ^= 0xff;
        assert_eq!(
            run(Coding::Gzip, &bad, 4096, 65536).unwrap_err(),
            DecodeError::Checksum
        );
        // A wrong ISIZE.
        let mut bad = good.clone();
        bad[n - 1] ^= 0x01;
        assert_eq!(
            run(Coding::Gzip, &bad, 4096, 65536).unwrap_err(),
            DecodeError::Checksum
        );
        // Not gzip at all; reserved flag bits.
        assert_eq!(
            run(Coding::Gzip, b"plain text", 4, 16).unwrap_err(),
            DecodeError::Header
        );
        let mut bad = good.clone();
        bad[3] |= 0x80;
        assert_eq!(
            run(Coding::Gzip, &bad, 4096, 65536).unwrap_err(),
            DecodeError::Header
        );
        // Garbage after a complete member / after a complete deflate stream.
        let mut bad = good.clone();
        bad.extend_from_slice(b"junk");
        assert_eq!(
            run(Coding::Gzip, &bad, 4096, 65536).unwrap_err(),
            DecodeError::Header
        );
        let mut bad = zlib(&data);
        bad.extend_from_slice(b"junk");
        assert_eq!(
            run(Coding::Deflate, &bad, 4096, 65536).unwrap_err(),
            DecodeError::TrailingData
        );
        // Inflate garbage.
        let mut bad = zlib(&data);
        for b in &mut bad[2..40] {
            *b = 0xff;
        }
        assert!(run(Coding::Deflate, &bad, 4096, 65536).is_err());
        // An endless header (FNAME never terminated) stops at the cap.
        let mut hdr = vec![0x1f, 0x8b, 8, 0x08, 0, 0, 0, 0, 0, 3];
        hdr.extend(std::iter::repeat_n(b'a', MAX_GZIP_HEADER));
        assert_eq!(
            run(Coding::Gzip, &hdr, 4096, 65536).unwrap_err(),
            DecodeError::Header
        );
    }

    #[test]
    fn an_empty_body_is_clean_under_every_coding() {
        for c in [Coding::Gzip, Coding::Deflate] {
            assert_eq!(run(c, b"", 1, 16).unwrap().0, b"");
        }
    }

    #[test]
    fn only_a_single_known_coding_is_decoded() {
        let h = |vals: &[&str]| {
            let mut m = http::HeaderMap::new();
            for v in vals {
                m.append(http::header::CONTENT_ENCODING, v.parse().unwrap());
            }
            coding_of(&m)
        };
        assert_eq!(h(&["gzip"]), Some((Coding::Gzip, "gzip".into())));
        assert_eq!(h(&["X-Gzip"]), Some((Coding::Gzip, "X-Gzip".into())));
        assert_eq!(h(&["GZip"]), Some((Coding::Gzip, "GZip".into())));
        assert_eq!(h(&["Deflate"]), Some((Coding::Deflate, "Deflate".into())));
        for none in [
            &[][..],
            &["br"],
            &["zstd"],
            &["identity"],
            &["gzip, br"],
            &["gzip", "gzip"],
        ] {
            assert_eq!(h(none), None, "{none:?}");
        }
    }
}
