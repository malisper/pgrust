//! pgvector 0.8.5 sparsevec.c — the sparsevec type. DIVERGENCES (recorded): none.
//!
//! Note on `sparsevec_in` and `CheckNnz`: in upstream sparsevec.c, `CheckNnz`
//! (the "cannot have more elements than dimensions" check) is called only
//! from `sparsevec_recv` (binary), not from `sparsevec_in` (text). The text
//! parser relies solely on `CheckIndex` (0-based indices must be in
//! `[0, dim)`, ascending, without duplicates) to reject overflowing input:
//! since indices are unique values in `[0, dim)`, `nnz > dim` always trips
//! `CheckIndex` first. `check_nnz` is kept here (unused by `parse_sparsevec`)
//! for the binary path landing in a later task.
use crate::vec::{strtof_prefix, StrtofVal};
use mcx::{Mcx, PgVec};
use types_error::{
    PgError, PgResult, ERRCODE_DATA_EXCEPTION, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};

pub const SPARSEVEC_MAX_DIM: i32 = 1_000_000_000;
pub const SPARSEVEC_MAX_NNZ: i32 = 16000;
pub const SPARSEVEC_PAYLOAD_HDR: usize = 12; // dim, nnz, unused

#[derive(Clone, Copy)]
pub struct SparseVecView<'a> {
    data: &'a [u8],
}
impl<'a> SparseVecView<'a> {
    pub fn from_payload(data: &'a [u8]) -> PgResult<Self> {
        if data.len() < SPARSEVEC_PAYLOAD_HDR {
            return Err(PgError::error("corrupt sparsevec datum").into());
        }
        let v = SparseVecView { data };
        if v.nnz() > SPARSEVEC_MAX_NNZ as usize || data.len() < SPARSEVEC_PAYLOAD_HDR + v.nnz() * 8
        {
            return Err(PgError::error("corrupt sparsevec datum").into());
        }
        Ok(v)
    }
    #[inline]
    fn i32_at(&self, o: usize) -> i32 {
        i32::from_ne_bytes(self.data[o..o + 4].try_into().unwrap())
    }
    #[inline]
    pub fn dim(&self) -> i32 {
        self.i32_at(0)
    }
    #[inline]
    pub fn nnz(&self) -> usize {
        self.i32_at(4) as usize
    }
    #[inline]
    pub fn index(&self, i: usize) -> i32 {
        self.i32_at(SPARSEVEC_PAYLOAD_HDR + i * 4)
    }
    #[inline]
    pub fn value(&self, i: usize) -> f32 {
        let o = SPARSEVEC_PAYLOAD_HDR + self.nnz() * 4 + i * 4;
        f32::from_ne_bytes(self.data[o..o + 4].try_into().unwrap())
    }
}

pub struct SparseVecBuilder<'m> {
    img: PgVec<'m, u8>,
    nnz: usize,
}
impl<'m> SparseVecBuilder<'m> {
    /// C: InitSparseVector (dim/nnz set, indices/values zeroed).
    pub fn new(mcx: Mcx<'m>, dim: i32, nnz: usize) -> PgResult<Self> {
        let size = 4 + SPARSEVEC_PAYLOAD_HDR + nnz * 8;
        let mut img: PgVec<'m, u8> = mcx::vec_with_capacity_in(mcx, size)?;
        img.resize(size, 0);
        img[..4].copy_from_slice(&((size as u32) << 2).to_ne_bytes());
        img[4..8].copy_from_slice(&dim.to_ne_bytes());
        img[8..12].copy_from_slice(&(nnz as i32).to_ne_bytes());
        Ok(SparseVecBuilder { img, nnz })
    }
    pub fn set(&mut self, i: usize, index: i32, value: f32) {
        let io = 4 + SPARSEVEC_PAYLOAD_HDR + i * 4;
        self.img[io..io + 4].copy_from_slice(&index.to_ne_bytes());
        let vo = 4 + SPARSEVEC_PAYLOAD_HDR + self.nnz * 4 + i * 4;
        self.img[vo..vo + 4].copy_from_slice(&value.to_ne_bytes());
    }
    pub fn image(self) -> PgVec<'m, u8> {
        self.img
    }
}

fn de(msg: impl Into<String>) -> Box<PgError> {
    PgError::error(msg)
        .with_sqlstate(ERRCODE_DATA_EXCEPTION)
        .into()
}

pub fn check_dim(dim: i32) -> PgResult<()> {
    if dim < 1 {
        return Err(de("sparsevec must have at least 1 dimension"));
    }
    if dim > SPARSEVEC_MAX_DIM {
        return Err(PgError::error(format!(
            "sparsevec cannot have more than {SPARSEVEC_MAX_DIM} dimensions"
        ))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        .into());
    }
    Ok(())
}
pub fn check_expected_dim(typmod: i32, dim: i32) -> PgResult<()> {
    if typmod != -1 && typmod != dim {
        return Err(de(format!("expected {typmod} dimensions, not {dim}")));
    }
    Ok(())
}
/// C: CheckNnz. Called from sparsevec_recv (binary); the text parser
/// (sparsevec_in / parse_sparsevec) does not call this — see module note.
pub fn check_nnz(nnz: i32, dim: i32) -> PgResult<()> {
    if nnz < 0 {
        return Err(de("sparsevec cannot have negative number of elements"));
    }
    if nnz > SPARSEVEC_MAX_NNZ {
        return Err(PgError::error(format!(
            "sparsevec cannot have more than {SPARSEVEC_MAX_NNZ} non-zero elements"
        ))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        .into());
    }
    if nnz > dim {
        // C: CheckNnz's nnz > dim branch uses ERRCODE_PROGRAM_LIMIT_EXCEEDED
        // (sparsevec.c:92-95), not ERRCODE_DATA_EXCEPTION.
        return Err(PgError::error("sparsevec cannot have more elements than dimensions")
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .into());
    }
    Ok(())
}
/// C: CheckIndex over a sorted element list (0-based indices).
pub fn check_index(indices: impl Iterator<Item = i32> + Clone, dim: i32) -> PgResult<()> {
    let mut prev: Option<i32> = None;
    for idx in indices {
        if idx < 0 || idx >= dim {
            return Err(de("sparsevec index out of bounds"));
        }
        if let Some(p) = prev {
            if idx < p {
                return Err(de("sparsevec indices must be in ascending order"));
            }
            if idx == p {
                return Err(de("sparsevec indices must not contain duplicates"));
            }
        }
        prev = Some(idx);
    }
    Ok(())
}
pub fn check_element(v: f32) -> PgResult<()> {
    if v.is_nan() {
        return Err(de("NaN not allowed in sparsevec"));
    }
    if v.is_infinite() {
        return Err(de("infinite value not allowed in sparsevec"));
    }
    Ok(())
}
pub fn check_dims(a: &SparseVecView<'_>, b: &SparseVecView<'_>) -> PgResult<()> {
    if a.dim() != b.dim() {
        return Err(de(format!(
            "different sparsevec dimensions {} and {}",
            a.dim(),
            b.dim()
        )));
    }
    Ok(())
}

pub struct SparseInputElement {
    pub index: i32,
    pub value: f32,
}

fn sparsevec_isspace(ch: u8) -> bool {
    matches!(ch, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

#[track_caller]
#[cold]
fn invalid_text(lit: &[u8], detail: Option<&str>) -> Box<PgError> {
    let mut e = PgError::error(format!(
        "invalid input syntax for type sparsevec: \"{}\"",
        String::from_utf8_lossy(lit)
    ))
    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION);
    if let Some(d) = detail {
        e = e.with_detail(d);
    }
    e.into()
}

#[track_caller]
#[cold]
fn ran_out_of_buffer(lit: &[u8]) -> Box<PgError> {
    PgError::error(format!(
        "ran out of buffer: \"{}\"",
        String::from_utf8_lossy(lit)
    ))
    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
    .into()
}

// C: uses strtol like int2vectorin. Leading whitespace, optional sign,
// decimal digits; returns None if no digits were consumed (stringEnd == pt).
fn strtol_prefix(s: &[u8]) -> Option<(i64, usize)> {
    let mut i = 0usize;
    while i < s.len() && (s[i] as char).is_ascii_whitespace() {
        i += 1;
    }
    let mut neg = false;
    let mut j = i;
    if j < s.len() && (s[j] == b'+' || s[j] == b'-') {
        neg = s[j] == b'-';
        j += 1;
    }
    let digit_start = j;
    let mut val: i128 = 0;
    while j < s.len() && s[j].is_ascii_digit() {
        val = val * 10 + (s[j] - b'0') as i128;
        if val > i64::MAX as i128 {
            val = i64::MAX as i128;
        }
        j += 1;
    }
    if j == digit_start {
        return None;
    }
    if neg {
        val = -val;
    }
    let clamped = val.clamp(i64::MIN as i128, i64::MAX as i128) as i64;
    Some((clamped, j))
}

/// C: sparsevec_in (sparsevec.c ~199-399). Parses `lit` into `out` (sorted by
/// index, zero values dropped, 0-based indices) and returns dim.
pub fn parse_sparsevec(
    lit: &[u8],
    typmod: i32,
    out: &mut Vec<SparseInputElement>,
) -> PgResult<i32> {
    let n = lit.len();

    // First pass: count commas to size the "buffer" (maxNnz), matching C's
    // pre-scan that bounds the ran-out-of-buffer check below.
    let mut max_nnz: i64 = 1;
    for &b in lit {
        if b == b',' {
            max_nnz += 1;
        }
    }
    if max_nnz > SPARSEVEC_MAX_NNZ as i64 {
        return Err(PgError::error(format!(
            "sparsevec cannot have more than {SPARSEVEC_MAX_NNZ} non-zero elements"
        ))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        .into());
    }

    let mut pt = 0usize;
    while pt < n && sparsevec_isspace(lit[pt]) {
        pt += 1;
    }
    if pt >= n || lit[pt] != b'{' {
        return Err(invalid_text(lit, Some("Vector contents must start with \"{\".")));
    }
    pt += 1;

    while pt < n && sparsevec_isspace(lit[pt]) {
        pt += 1;
    }

    if pt < n && lit[pt] == b'}' {
        pt += 1;
    } else {
        let mut nnz_stored: usize = 0;
        loop {
            if nnz_stored == max_nnz as usize {
                return Err(ran_out_of_buffer(lit));
            }

            while pt < n && sparsevec_isspace(lit[pt]) {
                pt += 1;
            }
            if pt >= n {
                return Err(invalid_text(lit, None));
            }

            // Use similar logic as int2vectorin.
            let (index_raw, consumed) = match strtol_prefix(&lit[pt..]) {
                Some(r) => r,
                None => return Err(invalid_text(lit, None)),
            };
            pt += consumed;

            // Keep in int range for correct error message later.
            let index: i32 = if index_raw > i32::MAX as i64 {
                i32::MAX
            } else if index_raw < i32::MIN as i64 + 1 {
                i32::MIN + 1
            } else {
                index_raw as i32
            };

            while pt < n && sparsevec_isspace(lit[pt]) {
                pt += 1;
            }
            if pt >= n || lit[pt] != b':' {
                return Err(invalid_text(lit, None));
            }
            pt += 1;

            while pt < n && sparsevec_isspace(lit[pt]) {
                pt += 1;
            }

            // Use strtof like float4in to avoid a double-rounding problem.
            let (val, consumed) = match strtof_prefix(&lit[pt..]) {
                Some(r) => r,
                None => return Err(invalid_text(lit, None)),
            };
            if let StrtofVal::Erange(tok_len) = val {
                return Err(PgError::error(format!(
                    "\"{}\" is out of range for type sparsevec",
                    String::from_utf8_lossy(&lit[pt..pt + tok_len])
                ))
                .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
                .into());
            }
            let StrtofVal::Ok(value) = val else {
                unreachable!()
            };

            check_element(value)?;

            // Do not store zero values.
            if value != 0.0 {
                // Convert 1-based numbering (SQL) to 0-based (C).
                out.push(SparseInputElement {
                    index: index - 1,
                    value,
                });
                nnz_stored += 1;
            }

            pt += consumed;

            while pt < n && sparsevec_isspace(lit[pt]) {
                pt += 1;
            }
            if pt < n && lit[pt] == b',' {
                pt += 1;
            } else if pt < n && lit[pt] == b'}' {
                pt += 1;
                break;
            } else {
                return Err(invalid_text(lit, None));
            }
        }
    }

    while pt < n && sparsevec_isspace(lit[pt]) {
        pt += 1;
    }
    if pt >= n || lit[pt] != b'/' {
        return Err(invalid_text(lit, Some("Unexpected end of input.")));
    }
    pt += 1;

    while pt < n && sparsevec_isspace(lit[pt]) {
        pt += 1;
    }

    // Use similar logic as int2vectorin.
    let (dim_raw, consumed) = match strtol_prefix(&lit[pt..]) {
        Some(r) => r,
        None => return Err(invalid_text(lit, None)),
    };
    pt += consumed;

    // Keep in int range for correct error message later.
    let dim: i32 = if dim_raw > i32::MAX as i64 {
        i32::MAX
    } else if dim_raw < i32::MIN as i64 {
        i32::MIN
    } else {
        dim_raw as i32
    };

    // Only whitespace is allowed after the closing brace.
    while pt < n && sparsevec_isspace(lit[pt]) {
        pt += 1;
    }
    if pt != n {
        return Err(invalid_text(lit, Some("Junk after closing.")));
    }

    check_dim(dim)?;
    check_expected_dim(typmod, dim)?;

    out.sort_by_key(|e| e.index);
    check_index(out.iter().map(|e| e.index), dim)?;

    Ok(dim)
}

/// C: SparsevecL2SquaredDistance (sparsevec.c ~811-844). Two-pointer merge
/// over both index lists; f32 accumulation.
///
/// Note (here and in `inner_product`/`l1_distance` below): `bpos`/`j` are
/// `usize` *positions* into the index/value arrays, while `ai`/`bi` are
/// `i32` index *values* (matching C's `int` indices) — intentionally
/// different types, not a naming accident.
pub fn l2_squared_distance(a: &SparseVecView<'_>, b: &SparseVecView<'_>) -> f32 {
    let mut distance: f32 = 0.0;
    let mut bpos: usize = 0;
    let bn = b.nnz();
    for i in 0..a.nnz() {
        let ai = a.index(i);
        let mut bi: i32 = -1;
        let mut j = bpos;
        while j < bn {
            bi = b.index(j);
            if ai == bi {
                let diff = a.value(i) - b.value(j);
                distance += diff * diff;
            } else if ai > bi {
                distance += b.value(j) * b.value(j);
            }
            if ai >= bi {
                bpos = j + 1;
            }
            if bi >= ai {
                break;
            }
            j += 1;
        }
        if ai != bi {
            distance += a.value(i) * a.value(i);
        }
    }
    for j in bpos..bn {
        distance += b.value(j) * b.value(j);
    }
    distance
}

/// C: SparsevecInnerProduct (sparsevec.c ~858-880).
pub fn inner_product(a: &SparseVecView<'_>, b: &SparseVecView<'_>) -> f32 {
    let mut distance: f32 = 0.0;
    let mut bpos: usize = 0;
    let bn = b.nnz();
    for i in 0..a.nnz() {
        let ai = a.index(i);
        let mut j = bpos;
        while j < bn {
            let bi = b.index(j);
            if ai == bi {
                distance += a.value(i) * b.value(j);
            }
            if ai >= bi {
                bpos = j + 1;
            }
            if bi >= ai {
                break;
            }
            j += 1;
        }
    }
    distance
}

/// C: sparsevec_cosine_distance's similarity computation (sparsevec.c
/// ~908-935), stopping before the final `1.0 - similarity` (that belongs to
/// the SQL-facing distance function in a later task). f32 partial norms cast
/// to f64, `similarity /= sqrt(norma * normb)`, then clamped to [-1, 1]. The
/// `#ifdef _MSC_VER` NaN early-return is MSVC-`/fp:fast`-only and not
/// applicable here.
pub fn cosine_similarity(a: &SparseVecView<'_>, b: &SparseVecView<'_>) -> f64 {
    let mut norma: f32 = 0.0;
    let mut normb: f32 = 0.0;
    let mut similarity = inner_product(a, b) as f64;

    for i in 0..a.nnz() {
        norma += a.value(i) * a.value(i);
    }
    for i in 0..b.nnz() {
        normb += b.value(i) * b.value(i);
    }

    similarity /= (norma as f64 * normb as f64).sqrt();

    if similarity > 1.0 {
        similarity = 1.0;
    } else if similarity < -1.0 {
        similarity = -1.0;
    }
    similarity
}

/// C: sparsevec_l1_distance (sparsevec.c ~1005-1043).
pub fn l1_distance(a: &SparseVecView<'_>, b: &SparseVecView<'_>) -> f32 {
    let mut distance: f32 = 0.0;
    let mut bpos: usize = 0;
    let bn = b.nnz();
    for i in 0..a.nnz() {
        let ai = a.index(i);
        let mut bi: i32 = -1;
        let mut j = bpos;
        while j < bn {
            bi = b.index(j);
            if ai == bi {
                distance += (a.value(i) - b.value(j)).abs();
            } else if ai > bi {
                distance += b.value(j).abs();
            }
            if ai >= bi {
                bpos = j + 1;
            }
            if bi >= ai {
                break;
            }
            j += 1;
        }
        if ai != bi {
            distance += a.value(i).abs();
        }
    }
    for j in bpos..bn {
        distance += b.value(j).abs();
    }
    distance
}

/// C: sparsevec_l2_norm (sparsevec.c ~1049-1061). Double accumulation.
pub fn norm(a: &SparseVecView<'_>) -> f64 {
    let mut n: f64 = 0.0;
    for i in 0..a.nnz() {
        let x = a.value(i) as f64;
        n += x * x;
    }
    n.sqrt()
}

/// C: sparsevec_cmp_internal (sparsevec.c ~1136-1173). Compares values
/// before dimensions, treating a missing index (past one side's nnz, but
/// still below that side's dim) as an implicit 0.
pub fn cmp_internal(a: &SparseVecView<'_>, b: &SparseVecView<'_>) -> i32 {
    let nnz = a.nnz().min(b.nnz());
    for i in 0..nnz {
        let (ai, bi) = (a.index(i), b.index(i));
        if ai < bi {
            return if a.value(i) < 0.0 { -1 } else { 1 };
        }
        if ai > bi {
            return if b.value(i) < 0.0 { 1 } else { -1 };
        }
        if a.value(i) < b.value(i) {
            return -1;
        }
        if a.value(i) > b.value(i) {
            return 1;
        }
    }

    if a.nnz() < b.nnz() && b.index(nnz) < a.dim() {
        return if b.value(nnz) < 0.0 { 1 } else { -1 };
    }
    if a.nnz() > b.nnz() && a.index(nnz) < b.dim() {
        return if a.value(nnz) < 0.0 { -1 } else { 1 };
    }

    if a.dim() < b.dim() {
        return -1;
    }
    if a.dim() > b.dim() {
        return 1;
    }
    0
}

/// C: sparsevec_l2_normalize (sparsevec.c ~1069-1120), adapted to the
/// image-in/image-out shape `HnswTypeInfo::normalize` expects (mirrors
/// `vec::l2_normalize_image`). Norm is accumulated in double. When norm > 0,
/// each value is divided by the norm; an infinite result is a hard error
/// (`float_overflow_error`), matching C's `isinf(rx[i])` check. C then
/// rebuilds the vector a second time, dropping any entries that underflowed
/// to exactly 0 (0.8.x behavior — read from sparsevec.c, not guessed) so the
/// returned nnz can be smaller than the input's. When norm == 0, C's
/// `InitSparseVector` result is returned untouched: since a stored
/// sparsevec never keeps zero-valued elements, norm == 0 implies nnz == 0,
/// so `SparseVecBuilder::new`'s zero-fill already matches (there is nothing
/// to copy).
pub fn sparsevec_l2_normalize_image<'m>(mcx: Mcx<'m>, img: &[u8]) -> PgResult<PgVec<'m, u8>> {
    let v = SparseVecView::from_payload(&img[4..])?;
    let nnz = v.nnz();

    let mut norm: f64 = 0.0;
    for i in 0..nnz {
        let x = v.value(i) as f64;
        norm += x * x;
    }
    norm = norm.sqrt();

    if norm > 0.0 {
        let mut rx: Vec<f32> = Vec::with_capacity(nnz);
        for i in 0..nnz {
            let r = (v.value(i) as f64 / norm) as f32;
            // This guard mirrors C's `isinf(rx[i])` check, but it is
            // unreachable for any input, matching C: `norm` is
            // `sqrt(sum(x_i^2))`, so floating-point summation of the
            // non-negative `x_i^2` terms guarantees `norm >= |x_i|` for
            // every `i` (each partial sum is monotonically non-decreasing),
            // hence `|x_i / norm| <= 1` always — far below f32's range, so
            // downcasting to f32 can never overflow to infinity for finite
            // input. Even a hand-built image with an infinite element
            // (bypassing `check_element`, which normal input always passes
            // through) does not reach this branch either: `norm` itself
            // becomes `f64::INFINITY`, and IEEE-754 `inf / inf = NaN` (not
            // `inf`), so the infinite element normalizes to NaN, and every
            // other (finite) element normalizes to `finite / inf = 0.0` —
            // neither is infinite. So there is no reachable input, valid or
            // hand-built, that trips this check; it is kept only because C
            // has the equivalent dead check.
            if r.is_infinite() {
                return Err(Box::new(adt_float::float_overflow_error()));
            }
            rx.push(r);
        }

        let zeros = rx.iter().filter(|&&r| r == 0.0).count();
        let mut b = SparseVecBuilder::new(mcx, v.dim(), nnz - zeros)?;
        let mut j = 0;
        for i in 0..nnz {
            if rx[i] == 0.0 {
                continue;
            }
            b.set(j, v.index(i), rx[i]);
            j += 1;
        }
        return Ok(b.image());
    }

    // norm == 0: C returns the zero-initialized InitSparseVector result
    // (indices/values never copied on this path).
    let b = SparseVecBuilder::new(mcx, v.dim(), nnz)?;
    Ok(b.image())
}

/// C: hnswutils.c SparsevecCheckValue.
pub const HNSW_MAX_NNZ: i32 = 1000;
pub fn sparsevec_check_value(img: &[u8]) -> PgResult<()> {
    let v = SparseVecView::from_payload(&img[4..])?;
    if v.nnz() as i32 > HNSW_MAX_NNZ {
        return Err(PgError::error(format!(
            "sparsevec cannot have more than {HNSW_MAX_NNZ} non-zero elements for hnsw index"
        ))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        .into());
    }
    Ok(())
}
pub static SPARSEVEC_TYPE_INFO: types_hnsw::HnswTypeInfo = types_hnsw::HnswTypeInfo {
    max_dimensions: SPARSEVEC_MAX_DIM,
    normalize: Some(sparsevec_l2_normalize_image),
    check_value: Some(sparsevec_check_value),
};

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(lit: &str) -> Result<(i32, Vec<(i32, f32)>), String> {
        let mut out = Vec::new();
        match parse_sparsevec(lit.as_bytes(), -1, &mut out) {
            Ok(dim) => Ok((dim, out.iter().map(|e| (e.index, e.value)).collect())),
            Err(e) => Err(e.message().to_string()),
        }
    }

    #[test]
    fn parses_sorts_and_drops_zeros() {
        assert_eq!(parse("{1:1.5,3:2}/5").unwrap(), (5, vec![(0, 1.5), (2, 2.0)]));
        assert_eq!(parse(" { 3 : 2 , 1 : 1 } / 5 ").unwrap(), (5, vec![(0, 1.0), (2, 2.0)]));
        assert_eq!(parse("{1:0,2:0}/3").unwrap(), (3, vec![]));
        assert_eq!(parse("{}/3").unwrap(), (3, vec![]));
    }

    #[test]
    fn errors_match_c() {
        assert_eq!(parse("{1:1}").unwrap_err(), "invalid input syntax for type sparsevec: \"{1:1}\"");
        assert_eq!(parse("{1:1}/0").unwrap_err(), "sparsevec must have at least 1 dimension");
        assert_eq!(parse("{1:1}/1000000001").unwrap_err(), "sparsevec cannot have more than 1000000000 dimensions");
        assert_eq!(parse("{0:1}/3").unwrap_err(), "sparsevec index out of bounds");
        assert_eq!(parse("{4:1}/3").unwrap_err(), "sparsevec index out of bounds");
        assert_eq!(parse("{1:1,1:2}/3").unwrap_err(), "sparsevec indices must not contain duplicates");
        assert_eq!(parse("{1:nan}/3").unwrap_err(), "NaN not allowed in sparsevec");
        assert_eq!(parse("{1:inf}/3").unwrap_err(), "infinite value not allowed in sparsevec");
        assert_eq!(parse("{1:4e38}/3").unwrap_err(), "\"4e38\" is out of range for type sparsevec");
        // C sparsevec_in never calls CheckNnz (recv-only); the 4th index (3, 0-based) fails CheckIndex.
        assert_eq!(parse("{1:1,2:2,3:3,4:4}/3").unwrap_err(), "sparsevec index out of bounds");
        let mut out = Vec::new();
        let e = parse_sparsevec(b"{1:1}/3", 4, &mut out).unwrap_err();
        assert_eq!(e.message(), "expected 4 dimensions, not 3");
    }

    #[test]
    fn layout_round_trip() {
        let o = mcx::MemoryContext::new_bump("t"); let m = o.mcx();
        let mut b = SparseVecBuilder::new(m, 10, 2).unwrap();
        b.set(0, 1, 0.5); b.set(1, 7, -2.0);
        let img = b.image();
        assert_eq!(img.len(), 4 + 12 + 2 * 4 + 2 * 4);
        let v = SparseVecView::from_payload(&img[4..]).unwrap();
        assert_eq!((v.dim(), v.nnz()), (10, 2));
        assert_eq!((v.index(1), v.value(1)), (7, -2.0));
        assert_eq!(&img[12..16], &0i32.to_ne_bytes(), "unused is zero");
    }

    fn sv<'m>(m: mcx::Mcx<'m>, dim: i32, e: &[(i32, f32)]) -> mcx::PgVec<'m, u8> {
        let mut b = SparseVecBuilder::new(m, dim, e.len()).unwrap();
        for (i, (idx, v)) in e.iter().enumerate() {
            b.set(i, *idx, *v);
        }
        b.image()
    }

    #[test]
    fn two_pointer_kernels() {
        let o = mcx::MemoryContext::new_bump("t");
        let m = o.mcx();
        let a = sv(m, 5, &[(0, 1.0), (2, 2.0), (4, 3.0)]);
        let b = sv(m, 5, &[(1, 4.0), (2, 5.0), (4, 6.0)]);
        let (a, b) = (
            SparseVecView::from_payload(&a[4..]).unwrap(),
            SparseVecView::from_payload(&b[4..]).unwrap(),
        );
        assert_eq!(l2_squared_distance(&a, &b), 1.0 + 16.0 + 9.0 + 9.0);
        assert_eq!(inner_product(&a, &b), 10.0 + 18.0);
        assert_eq!(l1_distance(&a, &b), 1.0 + 4.0 + 3.0 + 3.0);
        assert!((norm(&a) - 14f64.sqrt()).abs() < 1e-12);
        // C sparsevec_cmp_internal: at index 0, a->indices[0]=0 < b->indices[0]=1,
        // so it returns (ax[0] < 0 ? -1 : 1); ax[0] = 1.0, so this is 1 (not -1).
        assert_eq!(cmp_internal(&a, &b), 1);
    }

    #[test]
    fn cmp_treats_missing_as_zero_then_dim() {
        let o = mcx::MemoryContext::new_bump("t");
        let m = o.mcx();
        let a = sv(m, 3, &[(1, 1.0)]);
        let b = sv(m, 3, &[(0, 1.0)]);
        let (a, b) = (
            SparseVecView::from_payload(&a[4..]).unwrap(),
            SparseVecView::from_payload(&b[4..]).unwrap(),
        );
        assert_eq!(cmp_internal(&a, &b), -1); // index0: a=0 < b=1
        let c = sv(m, 4, &[(1, 1.0)]);
        let c = SparseVecView::from_payload(&c[4..]).unwrap();
        assert_eq!(cmp_internal(&a, &c), -1); // equal prefix, shorter dim first
    }

    #[test]
    fn normalize_and_check_value() {
        let o = mcx::MemoryContext::new_bump("t");
        let m = o.mcx();
        let a = sv(m, 5, &[(0, 3.0), (4, 4.0)]);
        let out = sparsevec_l2_normalize_image(m, &a).unwrap();
        let v = SparseVecView::from_payload(&out[4..]).unwrap();
        assert!((v.value(0) - 0.6).abs() < 1e-7 && (v.value(1) - 0.8).abs() < 1e-7);
        let big = {
            let e: Vec<(i32, f32)> = (0..1001).map(|i| (i, 1.0)).collect();
            sv(m, 2000, &e)
        };
        let err = sparsevec_check_value(&big).unwrap_err();
        assert_eq!(
            err.message(),
            "sparsevec cannot have more than 1000 non-zero elements for hnsw index"
        );
        assert_eq!(SPARSEVEC_TYPE_INFO.max_dimensions, SPARSEVEC_MAX_DIM);
    }

    // Fix round 1: exercise the final b-tail drain (`for j in bpos..bn`) in
    // `l2_squared_distance`/`l1_distance` — b has indices trailing past a's
    // last index — and its mirror, where a has the trailing indices instead
    // (which takes the *other* code path: the per-`ai` `if ai != bi`
    // in-loop tail-add, reached via the `bi = -1` sentinel once b is
    // exhausted). Both are mathematically the same distance, but they run
    // through different branches.
    #[test]
    fn tail_drain_both_directions() {
        let o = mcx::MemoryContext::new_bump("t");
        let m = o.mcx();

        // b has the trailing indices: exercises `for j in bpos..bn`.
        let a = sv(m, 6, &[(0, 1.0)]);
        let b = sv(m, 6, &[(0, 1.0), (3, 2.0), (5, 3.0)]);
        let (a, b) = (
            SparseVecView::from_payload(&a[4..]).unwrap(),
            SparseVecView::from_payload(&b[4..]).unwrap(),
        );
        assert_eq!(l2_squared_distance(&a, &b), 4.0 + 9.0);
        assert_eq!(l1_distance(&a, &b), 2.0 + 3.0);

        // Mirror: a has the trailing indices, exercises the in-loop
        // `if ai != bi` tail-add (b exhausted, bi stuck at -1).
        let a2 = sv(m, 6, &[(0, 1.0), (3, 2.0), (5, 3.0)]);
        let b2 = sv(m, 6, &[(0, 1.0)]);
        let (a2, b2) = (
            SparseVecView::from_payload(&a2[4..]).unwrap(),
            SparseVecView::from_payload(&b2[4..]).unwrap(),
        );
        assert_eq!(l2_squared_distance(&a2, &b2), 4.0 + 9.0);
        assert_eq!(l1_distance(&a2, &b2), 2.0 + 3.0);
    }

    // Fix round 1: exercise `cmp_internal`'s post-common-prefix branches —
    // equal prefix, then one side has one more element whose index is still
    // inside the *other* side's `dim` (so it counts as an implicit 0 on the
    // shorter side, per C, rather than falling through to the `dim`
    // comparison) — for both which side is longer and both signs of the
    // extra value.
    #[test]
    fn cmp_internal_post_prefix_branches() {
        let o = mcx::MemoryContext::new_bump("t");
        let m = o.mcx();

        let a = sv(m, 3, &[(0, 1.0)]);
        let a = SparseVecView::from_payload(&a[4..]).unwrap();

        // b longer, extra value positive: missing (0) < 5 -> a < b.
        let b_pos = sv(m, 3, &[(0, 1.0), (2, 5.0)]);
        let b_pos = SparseVecView::from_payload(&b_pos[4..]).unwrap();
        assert_eq!(cmp_internal(&a, &b_pos), -1);

        // b longer, extra value negative: missing (0) > -5 -> a > b.
        let b_neg = sv(m, 3, &[(0, 1.0), (2, -5.0)]);
        let b_neg = SparseVecView::from_payload(&b_neg[4..]).unwrap();
        assert_eq!(cmp_internal(&a, &b_neg), 1);

        // Mirror: a longer, extra value positive: 5 > missing (0) -> a > b.
        let a_pos = sv(m, 3, &[(0, 1.0), (2, 5.0)]);
        let a_pos = SparseVecView::from_payload(&a_pos[4..]).unwrap();
        assert_eq!(cmp_internal(&a_pos, &a), 1);

        // Mirror: a longer, extra value negative: -5 < missing (0) -> a < b.
        let a_neg = sv(m, 3, &[(0, 1.0), (2, -5.0)]);
        let a_neg = SparseVecView::from_payload(&a_neg[4..]).unwrap();
        assert_eq!(cmp_internal(&a_neg, &a), -1);
    }

    // Fix round 1: `sparsevec_l2_normalize_image`'s zero-drop rebuild — one
    // value underflows to exactly 0.0 in f32 after dividing by the norm
    // while another stays a normal value, so the output must be rebuilt
    // with a smaller nnz and the surviving index/value shifted down.
    #[test]
    fn normalize_drops_underflowed_zeros() {
        let o = mcx::MemoryContext::new_bump("t");
        let m = o.mcx();
        // norm = sqrt((1e-38)^2 + (3e38)^2) ~= 3e38 (the second term
        // completely dominates in f64), so index 0 divides down to
        // ~3.3e-77, which underflows to exactly 0.0 in f32, while index 1
        // divides to ~1.0.
        let a = sv(m, 3, &[(0, 1e-38), (1, 3e38)]);
        let out = sparsevec_l2_normalize_image(m, &a).unwrap();
        let v = SparseVecView::from_payload(&out[4..]).unwrap();
        assert_eq!(v.nnz(), 1);
        assert_eq!(v.index(0), 1);
        assert_eq!(v.value(0), 1.0);
    }
}
