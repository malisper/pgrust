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
        assert_eq!(parse("{1:1,2:2,3:3,4:4}/3").unwrap_err(), "sparsevec cannot have more elements than dimensions");
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
}
