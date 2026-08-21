//! Writer-facing granule API: the pgrcolumnar A4 integration surface.
//!
//! pgrcolumnar float chunks are addressed in 8192-row granules
//! ([`GRANULE_VALUES`] = 8 ALP vectors) through a per-granule directory,
//! and the DeltaFor precedent holds: non-raw granule payloads are ALWAYS
//! framed, every frame independently decodable. Three entry points:
//!
//! - [`analyze`]: deterministic election over one chunk's values with
//!   EXACT frame-image byte totals for the three arms (ALP classic /
//!   ALP-RD / raw bit images) plus exception stats — the accounting a
//!   chunk writer compares against its RawF(+codec) arm under the 10%-win
//!   discipline ([`ElectionReport::wins_by_ten_percent`], the
//!   encode_int_chunk model). Derived from real encoding, so it can never
//!   drift from what [`encode`] emits.
//! - [`encode`]: the same election, materializing one self-describing
//!   frame per granule for the chunk writer to codec-frame and store. The
//!   scheme is chosen per granule on exact sizes, raw winning ties, so no
//!   frame ever exceeds its granule's raw image plus the 4-byte header
//!   (the per-granule incompressible-guard bound).
//! - [`decode_frame`]: decode one frame back to bit-exact f64s with no
//!   chunk-level state. The RD dictionary rides inside each RD frame
//!   (<= 19 bytes against an 8 KiB granule) — that is what makes frames
//!   self-describing.
//!
//! Election parameters (the candidate (e,f) set and the RD dictionary)
//! are sampled ONCE per call over the whole input slice with the
//! reference's deterministic strides — the chunk-level analog of the
//! writer's sample-granule-0 codec pick: cheap, deterministic,
//! chunk-scoped. Short tails are handled at both levels: the last granule
//! may hold fewer than 8 vectors and its last vector fewer than 1024
//! values.
//!
//! Frame layout (integers little-endian, no alignment padding):
//!
//! ```text
//! frame     := scheme u8 | nvectors u8 | nvalues u16 | payload
//! Alp   (0) := nvectors x alp_vec
//!   alp_vec := len u16 | exponent u8 | factor u8 | bit_width u8
//!            | for_base i64 | exc_count u16
//!            | packed (bit_width*16 u64) | exc_positions (exc_count u16)
//!            | exc_values (exc_count u64)
//! AlpRd (1) := right_bit_width u8 | left_bit_width u8 | dict_len u8
//!            | dict (dict_len u16) | nvectors x rd_vec
//!   rd_vec  := len u16 | exc_count u16
//!            | packed_right (right_bit_width*16 u64)
//!            | packed_left (left_bit_width*16 u64)
//!            | exc_positions (exc_count u16) | exc_left (exc_count u16)
//! Raw   (2) := nvalues x u64 f64 bit images
//! ```
//!
//! This framing is the serialized form of record; the rowgroup-level
//! [`crate::encode`] size accounting composes from the same per-vector
//! formulas but does not emit bytes.

use crate::classic::{self, AlpVector};
use crate::constants::{MAX_EXPONENT, MAX_RD_DICTIONARY_SIZE, VECTOR_SIZE};
use crate::rd::{self, RdDictionary, RdVector};
use crate::{bitpack, Scheme};

/// Vectors per granule (pgrcolumnar: BLOCKS_PER_GRANULE).
pub const GRANULE_VECTORS: usize = 8;
/// Values per granule (pgrcolumnar: GRANULE_ROWS).
pub const GRANULE_VALUES: usize = GRANULE_VECTORS * VECTOR_SIZE;

/// scheme(1) + nvectors(1) + nvalues(2).
const FRAME_HEADER_BYTES: usize = 4;

const TAG_ALP: u8 = 0;
const TAG_ALP_RD: u8 = 1;
const TAG_RAW: u8 = 2;

/// Election summary for one chunk's worth of values. Every byte count is
/// EXACT (frame images as [`encode`] would emit them), never an estimate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ElectionReport {
    pub total_values: usize,
    pub ngranules: usize,
    /// Elected scheme per granule, input order.
    pub granule_schemes: Vec<Scheme>,
    /// 8 * total_values: the unframed bit-image payload (what an
    /// uncompressed RawF chunk stores).
    pub raw_bytes: usize,
    /// Exact whole-chunk frame totals had every granule been stored under
    /// a single arm.
    pub alp_frame_bytes: usize,
    pub alp_rd_frame_bytes: usize,
    pub raw_frame_bytes: usize,
    /// Exact total of the frames [`encode`] emits (per-granule best arm);
    /// the number the chunk writer runs its election on.
    pub frame_bytes: usize,
    /// Exceptions in the emitted frames (ALP round-trip-verify misses +
    /// RD dictionary misses).
    pub exceptions: usize,
}

impl ElectionReport {
    /// Granules elected under `scheme`.
    pub fn granules_using(&self, scheme: Scheme) -> usize {
        self.granule_schemes.iter().filter(|&&s| s == scheme).count()
    }

    /// The chunk writer's deterministic election discipline
    /// (encode_int_chunk model): engage only on a >=10% exact-byte win
    /// over the baseline arm.
    pub fn wins_by_ten_percent(&self, baseline_bytes: usize) -> bool {
        self.frame_bytes * 10 <= baseline_bytes * 9
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct GranuleEncoded {
    pub report: ElectionReport,
    /// One self-describing frame per granule, input order.
    pub frames: Vec<Vec<u8>>,
}

impl GranuleEncoded {
    pub fn decode(&self) -> Result<Vec<f64>, FrameError> {
        let mut out = Vec::with_capacity(self.report.total_values);
        for frame in &self.frames {
            decode_frame(frame, &mut out)?;
        }
        Ok(out)
    }
}

/// Run the election without materializing frames. Deterministic: same
/// input, same report — and always exactly [`encode`]'s report.
pub fn analyze(values: &[f64]) -> ElectionReport {
    run(values, false).report
}

/// Encode a chunk's values into per-granule self-describing frames.
pub fn encode(values: &[f64]) -> GranuleEncoded {
    run(values, true)
}

fn run(values: &[f64], emit: bool) -> GranuleEncoded {
    let mut enc = GranuleEncoded {
        report: ElectionReport {
            total_values: values.len(),
            ngranules: 0,
            granule_schemes: Vec::new(),
            raw_bytes: values.len() * 8,
            alp_frame_bytes: 0,
            alp_rd_frame_bytes: 0,
            raw_frame_bytes: 0,
            frame_bytes: 0,
            exceptions: 0,
        },
        frames: Vec::new(),
    };
    if values.is_empty() {
        return enc;
    }
    // Chunk-scoped first-stage sampling: candidate (e,f) set and the RD
    // dictionary are pure functions of the input slice.
    let sampled = crate::first_stage_sample(values);
    let selection = classic::find_top_k_combinations(&sampled);
    let flat: Vec<f64> = sampled.into_iter().flatten().collect();
    let dict = rd::find_best_dictionary(&flat);
    for g in values.chunks(GRANULE_VALUES) {
        let alp_vecs: Vec<AlpVector> = g
            .chunks(VECTOR_SIZE)
            .map(|c| {
                let (e, f) = classic::choose_ef(&selection.combinations, c);
                classic::encode_vector(c, e, f)
            })
            .collect();
        let alp_size =
            FRAME_HEADER_BYTES + alp_vecs.iter().map(|v| v.size_bytes()).sum::<usize>();
        let rd_vecs: Vec<RdVector> =
            g.chunks(VECTOR_SIZE).map(|c| rd::encode_rd_vector(c, &dict)).collect();
        let rd_size = FRAME_HEADER_BYTES
            + dict.size_bytes()
            + rd_vecs.iter().map(|v| v.size_bytes()).sum::<usize>();
        let raw_size = FRAME_HEADER_BYTES + g.len() * 8;
        enc.report.alp_frame_bytes += alp_size;
        enc.report.alp_rd_frame_bytes += rd_size;
        enc.report.raw_frame_bytes += raw_size;
        // Per-granule election on exact sizes. Raw wins ties (an encoding
        // that cannot beat raw bit images is never stored), then ALP over
        // ALP-RD (cheaper decode, no dictionary in the frame).
        let (scheme, size) = if alp_size.min(rd_size) >= raw_size {
            (Scheme::Raw, raw_size)
        } else if alp_size <= rd_size {
            (Scheme::Alp, alp_size)
        } else {
            (Scheme::AlpRd, rd_size)
        };
        enc.report.frame_bytes += size;
        enc.report.exceptions += match scheme {
            Scheme::Alp => alp_vecs.iter().map(|v| v.exc_positions.len()).sum(),
            Scheme::AlpRd => rd_vecs.iter().map(|v| v.exc_positions.len()).sum(),
            Scheme::Raw => 0,
        };
        enc.report.ngranules += 1;
        enc.report.granule_schemes.push(scheme);
        if emit {
            let mut frame = Vec::with_capacity(size);
            let tag = match scheme {
                Scheme::Alp => TAG_ALP,
                Scheme::AlpRd => TAG_ALP_RD,
                Scheme::Raw => TAG_RAW,
            };
            frame.push(tag);
            frame.push(alp_vecs.len() as u8);
            put_u16(&mut frame, g.len() as u16);
            match scheme {
                Scheme::Alp => {
                    for v in &alp_vecs {
                        put_alp_vector(&mut frame, v);
                    }
                }
                Scheme::AlpRd => {
                    put_dict(&mut frame, &dict);
                    for v in &rd_vecs {
                        put_rd_vector(&mut frame, v);
                    }
                }
                Scheme::Raw => {
                    for &x in g {
                        put_u64(&mut frame, x.to_bits());
                    }
                }
            }
            debug_assert_eq!(frame.len(), size, "frame image drifted from size accounting");
            enc.frames.push(frame);
        }
    }
    enc
}

// ---- serialization ----------------------------------------------------------

fn put_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_i64(out: &mut Vec<u8>, v: i64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_alp_vector(out: &mut Vec<u8>, v: &AlpVector) {
    put_u16(out, v.len);
    out.push(v.exponent);
    out.push(v.factor);
    out.push(v.bit_width);
    put_i64(out, v.for_base);
    put_u16(out, v.exc_positions.len() as u16);
    for &w in &v.packed {
        put_u64(out, w);
    }
    for &p in &v.exc_positions {
        put_u16(out, p);
    }
    for &b in &v.exc_values {
        put_u64(out, b);
    }
}

fn put_dict(out: &mut Vec<u8>, d: &RdDictionary) {
    out.push(d.right_bit_width);
    out.push(d.left_bit_width);
    out.push(d.dict.len() as u8);
    for &e in &d.dict {
        put_u16(out, e);
    }
}

fn put_rd_vector(out: &mut Vec<u8>, v: &RdVector) {
    put_u16(out, v.len);
    put_u16(out, v.exc_positions.len() as u16);
    for &w in &v.packed_right {
        put_u64(out, w);
    }
    for &w in &v.packed_left {
        put_u64(out, w);
    }
    for &p in &v.exc_positions {
        put_u16(out, p);
    }
    for &l in &v.exc_left {
        put_u16(out, l);
    }
}

// ---- decode -----------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameError {
    /// The frame ended before its declared payload.
    Truncated,
    /// Unknown scheme tag byte.
    BadScheme(u8),
    /// The named field is outside its legal range (or inconsistent with
    /// the rest of the frame).
    BadField(&'static str),
    /// Bytes remain past the declared payload.
    Trailing,
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameError::Truncated => write!(f, "alp granule frame truncated"),
            FrameError::BadScheme(t) => write!(f, "alp granule frame: unknown scheme tag {t}"),
            FrameError::BadField(name) => write!(f, "alp granule frame: bad {name}"),
            FrameError::Trailing => write!(f, "alp granule frame: trailing bytes"),
        }
    }
}

impl std::error::Error for FrameError {}

/// Bounds-checked little-endian cursor over one frame.
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], FrameError> {
        if self.0.len() < n {
            return Err(FrameError::Truncated);
        }
        let (head, tail) = self.0.split_at(n);
        self.0 = tail;
        Ok(head)
    }

    fn u8(&mut self) -> Result<u8, FrameError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, FrameError> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }

    fn i64(&mut self) -> Result<i64, FrameError> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn words(&mut self, n: usize, buf: &mut [u64]) -> Result<(), FrameError> {
        let bytes = self.take(n * 8)?;
        for (slot, ch) in buf[..n].iter_mut().zip(bytes.chunks_exact(8)) {
            *slot = u64::from_le_bytes(ch.try_into().unwrap());
        }
        Ok(())
    }
}

/// Decode sink: the frame decoders append whole decoded vectors through
/// this, so the f64 surface ([`decode_frame`]) and the bit-word surface
/// ([`decode_frame_words`]) share one decode implementation.
trait Sink {
    fn extend_f64s(&mut self, vals: &[f64]);
    /// Raw-scheme value: verbatim f64 bit image.
    fn push_bits(&mut self, bits: u64);
}

impl Sink for Vec<f64> {
    fn extend_f64s(&mut self, vals: &[f64]) {
        self.extend_from_slice(vals);
    }

    fn push_bits(&mut self, bits: u64) {
        self.push(f64::from_bits(bits));
    }
}

impl Sink for Vec<u64> {
    fn extend_f64s(&mut self, vals: &[f64]) {
        self.extend(vals.iter().map(|v| v.to_bits()));
    }

    fn push_bits(&mut self, bits: u64) {
        self.push(bits);
    }
}

/// Decode exactly one self-describing granule frame, appending its values
/// to `out` bit-exactly. Returns the number of values appended. The slice
/// must be exactly one frame; on error, `out` is restored to its previous
/// length. Never panics on corrupt input — every field is validated.
pub fn decode_frame(frame: &[u8], out: &mut Vec<f64>) -> Result<usize, FrameError> {
    let start = out.len();
    decode_frame_inner(frame, out).inspect_err(|_| out.truncate(start))
}

/// [`decode_frame`] emitting `f64::to_bits` images instead of f64s, for
/// engines whose float value words ARE u64 bit images (the pgrcolumnar A4
/// datum-word decode). Identical validation and bit-exactness contract.
pub fn decode_frame_words(frame: &[u8], out: &mut Vec<u64>) -> Result<usize, FrameError> {
    let start = out.len();
    decode_frame_inner(frame, out).inspect_err(|_| out.truncate(start))
}

fn decode_frame_inner<S: Sink>(frame: &[u8], out: &mut S) -> Result<usize, FrameError> {
    let mut rd = Reader(frame);
    let tag = rd.u8()?;
    let nvectors = rd.u8()? as usize;
    let nvalues = rd.u16()? as usize;
    if nvectors == 0 || nvectors > GRANULE_VECTORS {
        return Err(FrameError::BadField("nvectors"));
    }
    if nvalues == 0 || nvalues > GRANULE_VALUES {
        return Err(FrameError::BadField("nvalues"));
    }
    match tag {
        TAG_ALP => decode_alp_payload(&mut rd, nvectors, nvalues, out)?,
        TAG_ALP_RD => decode_rd_payload(&mut rd, nvectors, nvalues, out)?,
        TAG_RAW => {
            let bytes = rd.take(nvalues * 8)?;
            for ch in bytes.chunks_exact(8) {
                out.push_bits(u64::from_le_bytes(ch.try_into().unwrap()));
            }
        }
        t => return Err(FrameError::BadScheme(t)),
    }
    if !rd.0.is_empty() {
        return Err(FrameError::Trailing);
    }
    Ok(nvalues)
}

fn decode_alp_payload<S: Sink>(
    rd: &mut Reader,
    nvectors: usize,
    nvalues: usize,
    out: &mut S,
) -> Result<(), FrameError> {
    let mut words = [0u64; 64 * bitpack::LANES];
    let mut buf = [0.0f64; VECTOR_SIZE];
    let mut total = 0usize;
    for _ in 0..nvectors {
        let len = rd.u16()? as usize;
        if len == 0 || len > VECTOR_SIZE {
            return Err(FrameError::BadField("vector len"));
        }
        let exponent = rd.u8()?;
        let factor = rd.u8()?;
        if exponent > MAX_EXPONENT || factor > MAX_EXPONENT {
            return Err(FrameError::BadField("exponent/factor"));
        }
        let bit_width = rd.u8()? as u32;
        if bit_width > 64 {
            return Err(FrameError::BadField("bit width"));
        }
        let for_base = rd.i64()?;
        let exc = rd.u16()? as usize;
        if exc > len {
            return Err(FrameError::BadField("exception count"));
        }
        let nwords = bitpack::packed_words(bit_width);
        rd.words(nwords, &mut words)?;
        classic::decode_packed_into(
            &words[..nwords],
            bit_width,
            exponent,
            factor,
            for_base,
            &mut buf,
        );
        let pos_bytes = rd.take(exc * 2)?;
        let val_bytes = rd.take(exc * 8)?;
        for (p, v) in pos_bytes.chunks_exact(2).zip(val_bytes.chunks_exact(8)) {
            let pos = u16::from_le_bytes(p.try_into().unwrap()) as usize;
            if pos >= len {
                return Err(FrameError::BadField("exception position"));
            }
            buf[pos] = f64::from_bits(u64::from_le_bytes(v.try_into().unwrap()));
        }
        out.extend_f64s(&buf[..len]);
        total += len;
    }
    if total != nvalues {
        return Err(FrameError::BadField("nvalues"));
    }
    Ok(())
}

fn decode_rd_payload<S: Sink>(
    rd: &mut Reader,
    nvectors: usize,
    nvalues: usize,
    out: &mut S,
) -> Result<(), FrameError> {
    let right_bw = rd.u8()?;
    // The reference cuts at left widths 1..=16, so right is 48..=63; the
    // decode shifts below additionally rely on right_bw < 64.
    if !(48..=63).contains(&right_bw) {
        return Err(FrameError::BadField("right bit width"));
    }
    let left_bw = rd.u8()?;
    // ceil_log2(dict_len <= 8).max(1) is 1..=3; the masked table gather in
    // decode_rd_packed_into relies on codes fitting MAX_RD_DICTIONARY_SIZE.
    if left_bw == 0 || left_bw > 3 {
        return Err(FrameError::BadField("left bit width"));
    }
    let dict_len = rd.u8()? as usize;
    if dict_len == 0 || dict_len > MAX_RD_DICTIONARY_SIZE {
        return Err(FrameError::BadField("dictionary len"));
    }
    let dict_bytes = rd.take(dict_len * 2)?;
    let dict = RdDictionary {
        right_bit_width: right_bw,
        left_bit_width: left_bw,
        dict: dict_bytes
            .chunks_exact(2)
            .map(|c| u16::from_le_bytes(c.try_into().unwrap()))
            .collect(),
    };
    let right_mask = (1u64 << right_bw) - 1;
    let nr = bitpack::packed_words(right_bw as u32);
    let nl = bitpack::packed_words(left_bw as u32);
    let mut rwords = [0u64; 63 * bitpack::LANES];
    let mut lwords = [0u64; 3 * bitpack::LANES];
    let mut buf = [0.0f64; VECTOR_SIZE];
    let mut total = 0usize;
    for _ in 0..nvectors {
        let len = rd.u16()? as usize;
        if len == 0 || len > VECTOR_SIZE {
            return Err(FrameError::BadField("vector len"));
        }
        let exc = rd.u16()? as usize;
        if exc > len {
            return Err(FrameError::BadField("exception count"));
        }
        rd.words(nr, &mut rwords)?;
        rd.words(nl, &mut lwords)?;
        rd::decode_rd_packed_into(&rwords[..nr], &lwords[..nl], &dict, &mut buf);
        let pos_bytes = rd.take(exc * 2)?;
        let left_bytes = rd.take(exc * 2)?;
        for (p, l) in pos_bytes.chunks_exact(2).zip(left_bytes.chunks_exact(2)) {
            let pos = u16::from_le_bytes(p.try_into().unwrap()) as usize;
            if pos >= len {
                return Err(FrameError::BadField("exception position"));
            }
            // The right (low) bits decoded correctly even on a dictionary
            // miss; only the left part needs the patch.
            let left = u16::from_le_bytes(l.try_into().unwrap()) as u64;
            let right = buf[pos].to_bits() & right_mask;
            buf[pos] = f64::from_bits((left << right_bw) | right);
        }
        out.extend_f64s(&buf[..len]);
        total += len;
    }
    if total != nvalues {
        return Err(FrameError::BadField("nvalues"));
    }
    Ok(())
}
