//! Writer-facing f32 granule API (SB-5 / CMP-D): the `granule.rs` surface
//! at the 4-byte width — 8192-value granules as independent self-describing
//! frames, exact-size election per granule, bit-exact decode.
//!
//! Two arms only: ALP classic (`classic32.rs`) and raw f32 bit images. The
//! scheme-tag vocabulary stays family-aligned with the f64 frames (`Alp` =
//! 0, `Raw` = 2); tag 1 (ALP-RD) is never emitted at this width and refuses
//! on decode — a "real floats" granule simply prices above raw and stores
//! raw, keeping every bit pattern (NaN payloads, ±0.0, infinities,
//! denormals) exact by construction.
//!
//! Frame layout (integers little-endian, no alignment padding):
//!
//! ```text
//! frame       := scheme u8 | nvectors u8 | nvalues u16 | payload
//! Alp   (0)   := nvectors x alp_vec32
//!   alp_vec32 := len u16 | exponent u8 | factor u8 | bit_width u8
//!              | for_base i32 | exc_count u16
//!              | packed (bit_width*16 u64) | exc_positions (exc_count u16)
//!              | exc_values (exc_count u32)
//! Raw   (2)   := nvalues x u32 f32 bit images
//! ```
//!
//! This framing is the serialized form of record for the f32 family; the
//! election parameters (the candidate (e,f) set) are sampled ONCE per call
//! over the whole input slice with the reference's deterministic strides,
//! exactly like the f64 `granule::run`.

use crate::classic32::{self, AlpF32Vector, MAX_EXPONENT_F32};
use crate::constants::{ROWGROUP_SAMPLES_JUMP, SAMPLES_PER_VECTOR, VECTOR_SIZE};
use crate::granule::FrameError;
use crate::{bitpack, Scheme};

pub use crate::granule::{GRANULE_VALUES, GRANULE_VECTORS};

/// scheme(1) + nvectors(1) + nvalues(2).
const FRAME_HEADER_BYTES: usize = 4;

const TAG_ALP: u8 = 0;
const TAG_RAW: u8 = 2;

/// Election summary for one chunk's worth of f32 values. Every byte count
/// is EXACT (frame images as [`encode`] would emit them).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ElectionReportF32 {
    pub total_values: usize,
    pub ngranules: usize,
    /// Elected scheme per granule, input order (never [`Scheme::AlpRd`]).
    pub granule_schemes: Vec<Scheme>,
    /// 4 * total_values: the unframed bit-image payload.
    pub raw_bytes: usize,
    /// Exact whole-chunk frame totals had every granule been stored under a
    /// single arm.
    pub alp_frame_bytes: usize,
    pub raw_frame_bytes: usize,
    /// Exact total of the frames [`encode`] emits (per-granule best arm).
    pub frame_bytes: usize,
    /// Exceptions in the emitted frames (round-trip-verify misses).
    pub exceptions: usize,
}

impl ElectionReportF32 {
    pub fn granules_using(&self, scheme: Scheme) -> usize {
        self.granule_schemes.iter().filter(|&&s| s == scheme).count()
    }

    /// The chunk writer's deterministic election discipline: engage only on
    /// a >=10% exact-byte win over the baseline arm.
    pub fn wins_by_ten_percent(&self, baseline_bytes: usize) -> bool {
        self.frame_bytes * 10 <= baseline_bytes * 9
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct GranuleEncodedF32 {
    pub report: ElectionReportF32,
    /// One self-describing frame per granule, input order.
    pub frames: Vec<Vec<u8>>,
}

impl GranuleEncodedF32 {
    pub fn decode(&self) -> Result<Vec<f32>, FrameError> {
        let mut out = Vec::with_capacity(self.report.total_values);
        for frame in &self.frames {
            decode_frame32(frame, &mut out)?;
        }
        Ok(out)
    }
}

/// Run the election without materializing frames. Deterministic: same
/// input, same report — and always exactly [`encode`]'s report.
pub fn analyze(values: &[f32]) -> ElectionReportF32 {
    run32(values, false).report
}

/// Encode a chunk's f32s into per-granule self-describing frames.
pub fn encode(values: &[f32]) -> GranuleEncodedF32 {
    run32(values, true)
}

/// First-stage sample at f32 width (mirror of `crate::first_stage_sample`).
fn first_stage_sample32(values: &[f32]) -> Vec<Vec<f32>> {
    let mut out: Vec<Vec<f32>> = Vec::new();
    for (idx, chunk) in values.chunks(VECTOR_SIZE).enumerate() {
        if idx % ROWGROUP_SAMPLES_JUMP != 0 {
            continue;
        }
        if chunk.len() < VECTOR_SIZE && !out.is_empty() {
            continue;
        }
        let inc = chunk.len().div_ceil(SAMPLES_PER_VECTOR).max(1);
        out.push(chunk.iter().step_by(inc).copied().collect());
    }
    out
}

fn run32(values: &[f32], emit: bool) -> GranuleEncodedF32 {
    let mut enc = GranuleEncodedF32 {
        report: ElectionReportF32 {
            total_values: values.len(),
            ngranules: 0,
            granule_schemes: Vec::new(),
            raw_bytes: values.len() * 4,
            alp_frame_bytes: 0,
            raw_frame_bytes: 0,
            frame_bytes: 0,
            exceptions: 0,
        },
        frames: Vec::new(),
    };
    if values.is_empty() {
        return enc;
    }
    // Chunk-scoped sampling: the candidate (e,f) set is a pure function of
    // the input slice.
    let sampled = first_stage_sample32(values);
    let combinations = classic32::find_top_k_combinations32(&sampled);
    for g in values.chunks(GRANULE_VALUES) {
        let alp_vecs: Vec<AlpF32Vector> = g
            .chunks(VECTOR_SIZE)
            .map(|c| {
                let (e, f) = classic32::choose_ef32(&combinations, c);
                classic32::encode_vector32(c, e, f)
            })
            .collect();
        let alp_size =
            FRAME_HEADER_BYTES + alp_vecs.iter().map(|v| v.size_bytes()).sum::<usize>();
        let raw_size = FRAME_HEADER_BYTES + g.len() * 4;
        enc.report.alp_frame_bytes += alp_size;
        enc.report.raw_frame_bytes += raw_size;
        // Per-granule election on exact sizes; raw wins ties (an encoding
        // that cannot beat raw bit images is never stored).
        let (scheme, size) = if alp_size >= raw_size {
            (Scheme::Raw, raw_size)
        } else {
            (Scheme::Alp, alp_size)
        };
        enc.report.frame_bytes += size;
        enc.report.exceptions += match scheme {
            Scheme::Alp => alp_vecs.iter().map(|v| v.exc_positions.len()).sum(),
            _ => 0,
        };
        enc.report.ngranules += 1;
        enc.report.granule_schemes.push(scheme);
        if emit {
            let mut frame = Vec::with_capacity(size);
            frame.push(match scheme {
                Scheme::Alp => TAG_ALP,
                _ => TAG_RAW,
            });
            frame.push(alp_vecs.len() as u8);
            frame.extend_from_slice(&(g.len() as u16).to_le_bytes());
            match scheme {
                Scheme::Alp => {
                    for v in &alp_vecs {
                        put_alp_vector32(&mut frame, v);
                    }
                }
                _ => {
                    for &x in g {
                        frame.extend_from_slice(&x.to_bits().to_le_bytes());
                    }
                }
            }
            debug_assert_eq!(frame.len(), size, "f32 frame image drifted from size accounting");
            enc.frames.push(frame);
        }
    }
    enc
}

// ---- serialization ----------------------------------------------------------

fn put_alp_vector32(out: &mut Vec<u8>, v: &AlpF32Vector) {
    out.extend_from_slice(&v.len.to_le_bytes());
    out.push(v.exponent);
    out.push(v.factor);
    out.push(v.bit_width);
    out.extend_from_slice(&v.for_base.to_le_bytes());
    out.extend_from_slice(&(v.exc_positions.len() as u16).to_le_bytes());
    for &w in &v.packed {
        out.extend_from_slice(&w.to_le_bytes());
    }
    for &p in &v.exc_positions {
        out.extend_from_slice(&p.to_le_bytes());
    }
    for &b in &v.exc_values {
        out.extend_from_slice(&b.to_le_bytes());
    }
}

// ---- decode -----------------------------------------------------------------

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

    fn i32(&mut self) -> Result<i32, FrameError> {
        Ok(i32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn words(&mut self, n: usize, buf: &mut [u64]) -> Result<(), FrameError> {
        let bytes = self.take(n * 8)?;
        for (slot, ch) in buf[..n].iter_mut().zip(bytes.chunks_exact(8)) {
            *slot = u64::from_le_bytes(ch.try_into().unwrap());
        }
        Ok(())
    }
}

/// Decode sink: f32 surface and u32-bit-image surface share one decoder.
trait Sink32 {
    fn extend_f32s(&mut self, vals: &[f32]);
    fn push_bits(&mut self, bits: u32);
}

impl Sink32 for Vec<f32> {
    fn extend_f32s(&mut self, vals: &[f32]) {
        self.extend_from_slice(vals);
    }
    fn push_bits(&mut self, bits: u32) {
        self.push(f32::from_bits(bits));
    }
}

impl Sink32 for Vec<u32> {
    fn extend_f32s(&mut self, vals: &[f32]) {
        self.extend(vals.iter().map(|v| v.to_bits()));
    }
    fn push_bits(&mut self, bits: u32) {
        self.push(bits);
    }
}

/// Decode exactly one self-describing f32 granule frame, appending its
/// values to `out` bit-exactly. On error, `out` is restored. Never panics
/// on corrupt input — every field is validated.
pub fn decode_frame32(frame: &[u8], out: &mut Vec<f32>) -> Result<usize, FrameError> {
    let start = out.len();
    decode_frame_inner32(frame, out).inspect_err(|_| out.truncate(start))
}

/// [`decode_frame32`] emitting `f32::to_bits` images, for engines whose
/// float value words ARE bit images (the pgrc2 datum-word decode oracle).
pub fn decode_frame_bits32(frame: &[u8], out: &mut Vec<u32>) -> Result<usize, FrameError> {
    let start = out.len();
    decode_frame_inner32(frame, out).inspect_err(|_| out.truncate(start))
}

fn decode_frame_inner32<S: Sink32>(frame: &[u8], out: &mut S) -> Result<usize, FrameError> {
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
        TAG_ALP => decode_alp_payload32(&mut rd, nvectors, nvalues, out)?,
        TAG_RAW => {
            let bytes = rd.take(nvalues * 4)?;
            for ch in bytes.chunks_exact(4) {
                out.push_bits(u32::from_le_bytes(ch.try_into().unwrap()));
            }
        }
        t => return Err(FrameError::BadScheme(t)),
    }
    if !rd.0.is_empty() {
        return Err(FrameError::Trailing);
    }
    Ok(nvalues)
}

fn decode_alp_payload32<S: Sink32>(
    rd: &mut Reader,
    nvectors: usize,
    nvalues: usize,
    out: &mut S,
) -> Result<(), FrameError> {
    let mut words = [0u64; 32 * bitpack::LANES];
    let mut buf = [0.0f32; VECTOR_SIZE];
    let mut total = 0usize;
    for _ in 0..nvectors {
        let len = rd.u16()? as usize;
        if len == 0 || len > VECTOR_SIZE {
            return Err(FrameError::BadField("vector len"));
        }
        let exponent = rd.u8()?;
        let factor = rd.u8()?;
        if exponent > MAX_EXPONENT_F32 || factor > MAX_EXPONENT_F32 {
            return Err(FrameError::BadField("exponent/factor"));
        }
        let bit_width = rd.u8()? as u32;
        if bit_width > 32 {
            return Err(FrameError::BadField("bit width"));
        }
        let for_base = rd.i32()?;
        let exc = rd.u16()? as usize;
        if exc > len {
            return Err(FrameError::BadField("exception count"));
        }
        let nwords = bitpack::packed_words(bit_width);
        rd.words(nwords, &mut words)?;
        classic32::decode_packed_into32(
            &words[..nwords],
            bit_width,
            exponent,
            factor,
            for_base,
            &mut buf,
        );
        let pos_bytes = rd.take(exc * 2)?;
        let val_bytes = rd.take(exc * 4)?;
        for (p, v) in pos_bytes.chunks_exact(2).zip(val_bytes.chunks_exact(4)) {
            let pos = u16::from_le_bytes(p.try_into().unwrap()) as usize;
            if pos >= len {
                return Err(FrameError::BadField("exception position"));
            }
            buf[pos] = f32::from_bits(u32::from_le_bytes(v.try_into().unwrap()));
        }
        out.extend_f32s(&buf[..len]);
        total += len;
    }
    if total != nvalues {
        return Err(FrameError::BadField("nvalues"));
    }
    Ok(())
}
