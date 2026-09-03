//! pgvector 0.8.5 halfvec.c — the halfvec type: layout, checks, text parsing,
//! scalar kernels (halfutils.c distance functions, non-SIMD arms).
//! DIVERGENCES (recorded): F16C/target-clone dispatch not ported (scalar
//! loops); results are identical because both arms round the same way.
//! DIVERGENCES (recorded): halfvec.c's own `errdetail` for the "must start
//! with [" parse error reads "Vector contents must start with \"[\"."
//! (copy-pasted from vector.c, verified at $S/src/halfvec.c:192 in pgvector
//! 0.8.5) rather than a halfvec-specific message; this port reproduces that
//! text verbatim to match upstream byte-for-byte.
use mcx::{Mcx, PgVec};
use types_error::{
    PgError, PgResult, ERRCODE_DATA_EXCEPTION, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};

use crate::halfutils::{float4_to_half, float4_to_half_unchecked, half_is_inf, half_is_nan, half_to_float4};
use crate::vec::{strtof_prefix, StrtofVal};

pub const HALFVEC_MAX_DIM: usize = 16000;

// Payload layout after the 4-byte varlena header: i16 dim, i16 unused, u16 x[dim] (half floats).
pub const HALFVEC_PAYLOAD_HDR: usize = 4;

#[derive(Clone, Copy)]
pub struct HalfVecView<'a> {
    data: &'a [u8],
}

impl<'a> HalfVecView<'a> {
    pub fn from_payload(data: &'a [u8]) -> PgResult<HalfVecView<'a>> {
        if data.len() < HALFVEC_PAYLOAD_HDR {
            return Err(PgError::error("corrupt halfvec datum").into());
        }
        let v = HalfVecView { data };
        let want = HALFVEC_PAYLOAD_HDR + v.dim() * 2;
        if data.len() < want {
            return Err(PgError::error("corrupt halfvec datum").into());
        }
        Ok(v)
    }

    #[inline]
    pub fn dim(&self) -> usize {
        i16::from_ne_bytes([self.data[0], self.data[1]]) as usize
    }

    #[inline]
    pub fn raw(&self, i: usize) -> u16 {
        let off = HALFVEC_PAYLOAD_HDR + i * 2;
        u16::from_ne_bytes([self.data[off], self.data[off + 1]])
    }

    #[inline]
    pub fn x(&self, i: usize) -> f32 {
        half_to_float4(self.raw(i))
    }

    pub fn iter(&self) -> impl Iterator<Item = f32> + '_ {
        (0..self.dim()).map(|i| self.x(i))
    }
}

pub struct HalfVecBuilder<'mcx> {
    img: PgVec<'mcx, u8>,
}

// Full varlena image (4-byte header), zero-initialized elements like C InitHalfVector.
impl<'mcx> HalfVecBuilder<'mcx> {
    pub fn new(mcx: Mcx<'mcx>, dim: usize) -> PgResult<HalfVecBuilder<'mcx>> {
        let size = 4 + HALFVEC_PAYLOAD_HDR + dim * 2;
        let mut img: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, size)?;
        img.resize(size, 0);
        let hdr = ((size as u32) << 2).to_ne_bytes();
        img[..4].copy_from_slice(&hdr);
        img[4..6].copy_from_slice(&(dim as i16).to_ne_bytes());
        Ok(HalfVecBuilder { img })
    }

    #[inline]
    pub fn set_raw(&mut self, i: usize, h: u16) {
        let off = 4 + HALFVEC_PAYLOAD_HDR + i * 2;
        self.img[off..off + 2].copy_from_slice(&h.to_ne_bytes());
    }

    /// C: Float4ToHalf (range-checked).
    pub fn set(&mut self, i: usize, v: f32) -> PgResult<()> {
        self.set_raw(i, float4_to_half(v)?);
        Ok(())
    }

    #[inline]
    pub fn get_raw(&self, i: usize) -> u16 {
        let off = 4 + HALFVEC_PAYLOAD_HDR + i * 2;
        u16::from_ne_bytes([self.img[off], self.img[off + 1]])
    }

    pub fn image(self) -> PgVec<'mcx, u8> {
        self.img
    }
}

#[track_caller]
#[cold]
fn dim_error() -> Box<PgError> {
    PgError::error("halfvec must have at least 1 dimension")
        .with_sqlstate(ERRCODE_DATA_EXCEPTION)
        .into()
}

#[track_caller]
#[cold]
fn max_dim_error() -> Box<PgError> {
    PgError::error(format!(
        "halfvec cannot have more than {HALFVEC_MAX_DIM} dimensions"
    ))
    .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
    .into()
}

// C: CheckDim (halfvec.c)
pub fn check_dim(dim: usize) -> PgResult<()> {
    if dim < 1 {
        return Err(dim_error());
    }
    if dim > HALFVEC_MAX_DIM {
        return Err(max_dim_error());
    }
    Ok(())
}

// C: CheckExpectedDim (halfvec.c)
pub fn check_expected_dim(typmod: i32, dim: usize) -> PgResult<()> {
    if typmod != -1 && typmod as usize != dim {
        return Err(PgError::error(format!(
            "expected {typmod} dimensions, not {dim}"
        ))
        .with_sqlstate(ERRCODE_DATA_EXCEPTION)
        .into());
    }
    Ok(())
}

// C: CheckElement (halfvec.c)
pub fn check_element(h: u16) -> PgResult<()> {
    if half_is_nan(h) {
        return Err(PgError::error("NaN not allowed in halfvec")
            .with_sqlstate(ERRCODE_DATA_EXCEPTION)
            .into());
    }
    if half_is_inf(h) {
        return Err(PgError::error("infinite value not allowed in halfvec")
            .with_sqlstate(ERRCODE_DATA_EXCEPTION)
            .into());
    }
    Ok(())
}

// C: CheckDims (halfvec.c)
pub fn check_dims(a: &HalfVecView<'_>, b: &HalfVecView<'_>) -> PgResult<()> {
    if a.dim() != b.dim() {
        return Err(PgError::error(format!(
            "different halfvec dimensions {} and {}",
            a.dim(),
            b.dim()
        ))
        .with_sqlstate(ERRCODE_DATA_EXCEPTION)
        .into());
    }
    Ok(())
}

// C: scanner_isspace (PG17+ arm): space \t \n \r \v \f
fn halfvec_isspace(ch: u8) -> bool {
    matches!(ch, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

#[track_caller]
#[cold]
fn invalid_text(lit: &[u8], detail: Option<&str>) -> Box<PgError> {
    let mut e = PgError::error(format!(
        "invalid input syntax for type halfvec: \"{}\"",
        String::from_utf8_lossy(lit)
    ))
    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION);
    if let Some(d) = detail {
        e = e.with_detail(d);
    }
    e.into()
}

// halfvec_in body (halfvec.c): strtof-per-element between '[' ']', converted
// via Float4ToHalf(Unchecked) and range/element-checked as each is parsed.
pub fn parse_halfvec(
    lit: &[u8],
    typmod: i32,
    x: &mut [u16; HALFVEC_MAX_DIM],
) -> PgResult<usize> {
    let mut pt = 0usize;
    let n = lit.len();
    let mut dim = 0usize;

    while pt < n && halfvec_isspace(lit[pt]) {
        pt += 1;
    }
    if pt >= n || lit[pt] != b'[' {
        // Verbatim from halfvec.c ($S/src/halfvec.c:192): copy-pasted from
        // vector.c, so the detail text says "Vector", not "Halfvec".
        return Err(invalid_text(lit, Some("Vector contents must start with \"[\".")));
    }
    pt += 1;
    while pt < n && halfvec_isspace(lit[pt]) {
        pt += 1;
    }
    if pt < n && lit[pt] == b']' {
        return Err(dim_error());
    }

    loop {
        if dim == HALFVEC_MAX_DIM {
            return Err(max_dim_error());
        }
        while pt < n && halfvec_isspace(lit[pt]) {
            pt += 1;
        }
        if pt >= n {
            return Err(invalid_text(lit, None));
        }
        let (val, consumed) = match strtof_prefix(&lit[pt..]) {
            Some(r) => r,
            None => return Err(invalid_text(lit, None)),
        };
        if let StrtofVal::Erange(tok_len) = val {
            return Err(PgError::error(format!(
                "\"{}\" is out of range for type halfvec",
                String::from_utf8_lossy(&lit[pt..pt + tok_len])
            ))
            .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
            .into());
        }
        let StrtofVal::Ok(val) = val else { unreachable!() };
        // C: x[dim] = Float4ToHalfUnchecked(val); range check is
        // `(errno == ERANGE && isinf(val)) || (HalfIsInf(x[dim]) && !isinf(val))`
        // (the first disjunct is handled above via StrtofVal::Erange); on
        // overflow the message uses the ORIGINAL token text
        // (`pnstrdup(pt, stringEnd - pt)`), not halfutils::float4_to_half's
        // shortest-decimal rendering (that helper is for the cast paths in
        // later tasks, matching C's Float4ToHalf call sites there).
        let h = float4_to_half_unchecked(val);
        if half_is_inf(h) && !val.is_infinite() {
            return Err(PgError::error(format!(
                "\"{}\" is out of range for type halfvec",
                String::from_utf8_lossy(&lit[pt..pt + consumed])
            ))
            .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
            .into());
        }
        check_element(h)?;
        x[dim] = h;
        dim += 1;
        pt += consumed;

        while pt < n && halfvec_isspace(lit[pt]) {
            pt += 1;
        }
        if pt < n && lit[pt] == b',' {
            pt += 1;
        } else if pt < n && lit[pt] == b']' {
            pt += 1;
            break;
        } else {
            return Err(invalid_text(lit, None));
        }
    }

    while pt < n && halfvec_isspace(lit[pt]) {
        pt += 1;
    }
    if pt != n {
        return Err(invalid_text(lit, Some("Junk after closing right brace.")));
    }

    check_dim(dim)?;
    check_expected_dim(typmod, dim)?;
    Ok(dim)
}

// ---- distance kernels (halfutils.c non-SIMD "Default" arms) ----

pub fn l2_squared_distance(a: &HalfVecView<'_>, b: &HalfVecView<'_>) -> f32 {
    let mut distance = 0.0f32;
    for i in 0..a.dim() {
        let diff = a.x(i) - b.x(i);
        distance += diff * diff;
    }
    distance
}

pub fn inner_product(a: &HalfVecView<'_>, b: &HalfVecView<'_>) -> f32 {
    let mut distance = 0.0f32;
    for i in 0..a.dim() {
        distance += a.x(i) * b.x(i);
    }
    distance
}

/// C: HalfvecCosineSimilarity — returns similarity as double; caller clamps
/// to [-1, 1] and computes `1 - similarity` (halfvec_cosine_distance, Task 5).
pub fn cosine_similarity(a: &HalfVecView<'_>, b: &HalfVecView<'_>) -> f64 {
    let mut similarity = 0.0f32;
    let mut norma = 0.0f32;
    let mut normb = 0.0f32;
    for i in 0..a.dim() {
        let (xa, xb) = (a.x(i), b.x(i));
        similarity += xa * xb;
        norma += xa * xa;
        normb += xb * xb;
    }
    similarity as f64 / ((norma as f64) * (normb as f64)).sqrt()
}

pub fn l1_distance(a: &HalfVecView<'_>, b: &HalfVecView<'_>) -> f32 {
    let mut distance = 0.0f32;
    for i in 0..a.dim() {
        distance += (a.x(i) - b.x(i)).abs();
    }
    distance
}

/// C: halfvec_l2_norm — double accumulation.
pub fn norm(a: &HalfVecView<'_>) -> f64 {
    let mut n = 0.0f64;
    for x in a.iter() {
        n += x as f64 * x as f64;
    }
    n.sqrt()
}

/// C: halfvec_cmp_internal — element-wise (as float, converted from half),
/// then shorter-first on differing dims.
pub fn cmp_internal(a: &HalfVecView<'_>, b: &HalfVecView<'_>) -> i32 {
    let dim = a.dim().min(b.dim());
    for i in 0..dim {
        if a.x(i) < b.x(i) {
            return -1;
        }
        if a.x(i) > b.x(i) {
            return 1;
        }
    }
    if a.dim() < b.dim() {
        return -1;
    }
    if a.dim() > b.dim() {
        return 1;
    }
    0
}

/// C: halfvec_l2_normalize — double norm; divide each element (as double),
/// narrow via Float4ToHalfUnchecked, then check the result for overflow
/// (`HalfIsInf(rx[i])` -> `float_overflow_error()`, "value out of range:
/// overflow"). Zero norm returns the zero vector: C's `InitHalfVector` result
/// is palloc0'd and never written to in that branch, so e.g. `[-0]` (raw
/// 0x8000) normalizes to `[0]` (raw 0x0000), not a copy of the input's raw
/// bits — `HalfVecBuilder::new` is already zero-filled, matching this.
pub fn halfvec_l2_normalize_image<'m>(mcx: Mcx<'m>, img: &[u8]) -> PgResult<PgVec<'m, u8>> {
    let v = HalfVecView::from_payload(&img[4..])?;
    let mut b = HalfVecBuilder::new(mcx, v.dim())?;
    let mut norm = 0.0f64;
    for x in v.iter() {
        norm += x as f64 * x as f64;
    }
    norm = norm.sqrt();
    if norm > 0.0 {
        for i in 0..v.dim() {
            let h = float4_to_half_unchecked((v.x(i) as f64 / norm) as f32);
            if half_is_inf(h) {
                return Err(Box::new(adt_float::float_overflow_error()));
            }
            b.set_raw(i, h);
        }
    }
    Ok(b.image())
}

/// C: hnswutils.c HnswGetTypeInfo halfvec arm: maxDimensions = HNSW_MAX_DIM *
/// 2, normalize = halfvec_l2_normalize, checkValue = NULL.
pub static HALFVEC_TYPE_INFO: types_hnsw::HnswTypeInfo = types_hnsw::HnswTypeInfo {
    max_dimensions: (types_hnsw::HNSW_MAX_DIM * 2) as i32,
    normalize: Some(halfvec_l2_normalize_image),
    check_value: None,
};

#[cfg(test)]
mod tests {
    use super::*;

    fn hv<'m>(m: mcx::Mcx<'m>, xs: &[f32]) -> mcx::PgVec<'m, u8> {
        let mut b = HalfVecBuilder::new(m, xs.len()).unwrap();
        for (i, x) in xs.iter().enumerate() {
            b.set(i, *x).unwrap();
        }
        b.image()
    }

    #[test]
    fn layout_is_hdr_dim_unused_halves() {
        let o = mcx::MemoryContext::new_bump("t");
        let m = o.mcx();
        let img = hv(m, &[1.0, -2.0]);
        assert_eq!(img.len(), 4 + 4 + 2 * 2);
        assert_eq!(&img[4..6], &2i16.to_ne_bytes());
        assert_eq!(&img[6..8], &[0, 0]);
        let v = HalfVecView::from_payload(&img[4..]).unwrap();
        assert_eq!((v.raw(0), v.raw(1)), (0x3C00, 0xC000));
        assert_eq!(v.x(1), -2.0);
    }

    #[test]
    fn parse_vtab_is_whitespace() {
        // C: scanner_isspace treats \v (0x0b) as space on PG17+.
        let mut x = [0u16; HALFVEC_MAX_DIM];
        assert_eq!(parse_halfvec(b"[\x0b1]", -1, &mut x).unwrap(), 1);
        assert_eq!(x[0], 0x3C00);
    }

    #[test]
    fn parse_matches_c_errors() {
        let mut x = [0u16; HALFVEC_MAX_DIM];
        assert_eq!(parse_halfvec(b" [1, 2,3 ] ", -1, &mut x).unwrap(), 3);
        assert_eq!(x[1], 0x4000);
        let e = parse_halfvec(b"[1,2", -1, &mut x).unwrap_err();
        assert_eq!(e.message(), "invalid input syntax for type halfvec: \"[1,2\"");
        let e = parse_halfvec(b"[]", -1, &mut x).unwrap_err();
        assert_eq!(e.message(), "halfvec must have at least 1 dimension");
        let e = parse_halfvec(b"[1,2]", 3, &mut x).unwrap_err();
        assert_eq!(e.message(), "expected 3 dimensions, not 2");
        let e = parse_halfvec(b"[nan]", -1, &mut x).unwrap_err();
        assert_eq!(e.message(), "NaN not allowed in halfvec");
        let e = parse_halfvec(b"[inf]", -1, &mut x).unwrap_err();
        assert_eq!(e.message(), "infinite value not allowed in halfvec");
        let e = parse_halfvec(b"[65520]", -1, &mut x).unwrap_err();
        assert_eq!(e.message(), "\"65520\" is out of range for type halfvec");
    }

    #[test]
    fn parse_overflow_reports_original_token_text() {
        let mut x = [0u16; HALFVEC_MAX_DIM];
        // 1e5 == 100000.0, overflows half range (max 65504) but is not itself
        // an ERANGE strtof result; C reports the original token, not a
        // shortest-decimal rendering of the parsed value.
        let e = parse_halfvec(b"[1e5]", -1, &mut x).unwrap_err();
        assert_eq!(e.message(), "\"1e5\" is out of range for type halfvec");
        // Still passes: ERANGE-from-strtof path keeps working.
        let e = parse_halfvec(b"[65520]", -1, &mut x).unwrap_err();
        assert_eq!(e.message(), "\"65520\" is out of range for type halfvec");
    }

    #[test]
    fn kernels_accumulate_in_f32_after_conversion() {
        let o = mcx::MemoryContext::new_bump("t");
        let m = o.mcx();
        let a = hv(m, &[1.0, 2.0, 3.0]);
        let b = hv(m, &[4.0, 5.0, 6.0]);
        let (a, b) = (
            HalfVecView::from_payload(&a[4..]).unwrap(),
            HalfVecView::from_payload(&b[4..]).unwrap(),
        );
        assert_eq!(l2_squared_distance(&a, &b), 27.0);
        assert_eq!(inner_product(&a, &b), 32.0);
        assert_eq!(l1_distance(&a, &b), 9.0);
        assert!((cosine_similarity(&a, &b) - 0.9746318461970762).abs() < 1e-6);
        assert_eq!(cmp_internal(&a, &b), -1);
        assert_eq!(cmp_internal(&a, &a), 0);
    }

    #[test]
    fn normalize_image_and_type_info() {
        let o = mcx::MemoryContext::new_bump("t");
        let m = o.mcx();
        let img = hv(m, &[3.0, 4.0]);
        let out = halfvec_l2_normalize_image(m, &img).unwrap();
        let v = HalfVecView::from_payload(&out[4..]).unwrap();
        assert!((v.x(0) - 0.6).abs() < 1e-3 && (v.x(1) - 0.8).abs() < 1e-3);
        assert_eq!(HALFVEC_TYPE_INFO.max_dimensions, 4000);
        assert!(HALFVEC_TYPE_INFO.check_value.is_none());
    }

    #[test]
    fn normalize_zero_norm_returns_zero_not_input_bits() {
        // C: InitHalfVector's result is palloc0'd and never written to when
        // norm == 0, so [-0] (raw 0x8000) normalizes to [0] (raw 0x0000), not
        // a copy of the input's raw bits.
        let o = mcx::MemoryContext::new_bump("t");
        let m = o.mcx();
        let img = hv(m, &[-0.0]);
        assert_eq!(HalfVecView::from_payload(&img[4..]).unwrap().raw(0), 0x8000);
        let out = halfvec_l2_normalize_image(m, &img).unwrap();
        let v = HalfVecView::from_payload(&out[4..]).unwrap();
        assert_eq!(v.raw(0), 0x0000);
    }
}
