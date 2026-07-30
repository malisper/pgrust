use mcx::{Mcx, PgVec};
use types_error::{
    PgError, PgResult, ERRCODE_DATA_EXCEPTION, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
};

pub const VECTOR_MAX_DIM: usize = 16000;

// Payload layout after the 4-byte varlena header: i16 dim, i16 unused, f32 x[dim].
pub const VECTOR_PAYLOAD_HDR: usize = 4;

#[derive(Clone, Copy)]
pub struct VecView<'a> {
    data: &'a [u8],
}

impl<'a> VecView<'a> {
    pub fn from_payload(data: &'a [u8]) -> PgResult<VecView<'a>> {
        if data.len() < VECTOR_PAYLOAD_HDR {
            return Err(PgError::error("corrupt vector datum").into());
        }
        let v = VecView { data };
        let want = VECTOR_PAYLOAD_HDR + v.dim() * 4;
        if data.len() < want {
            return Err(PgError::error("corrupt vector datum").into());
        }
        Ok(v)
    }

    #[inline]
    pub fn dim(&self) -> usize {
        i16::from_ne_bytes([self.data[0], self.data[1]]) as usize
    }

    #[inline]
    pub fn x(&self, i: usize) -> f32 {
        let off = VECTOR_PAYLOAD_HDR + i * 4;
        f32::from_ne_bytes(self.data[off..off + 4].try_into().unwrap())
    }

    #[inline]
    pub fn xs_ptr(&self) -> *const u8 {
        // SAFETY of use sites: dim()*4 readable bytes past VECTOR_PAYLOAD_HDR
        // (checked in from_payload); reads are unaligned f32 loads.
        self.data[VECTOR_PAYLOAD_HDR..].as_ptr()
    }

    pub fn iter(&self) -> impl Iterator<Item = f32> + '_ {
        (0..self.dim()).map(|i| self.x(i))
    }
}

pub struct VecBuilder<'mcx> {
    img: PgVec<'mcx, u8>,
}

// Full varlena image (4-byte header), zero-initialized elements like C InitVector.
impl<'mcx> VecBuilder<'mcx> {
    pub fn new(mcx: Mcx<'mcx>, dim: usize) -> PgResult<VecBuilder<'mcx>> {
        let size = 4 + VECTOR_PAYLOAD_HDR + dim * 4;
        let mut img: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, size)?;
        img.resize(size, 0);
        let hdr = ((size as u32) << 2).to_ne_bytes();
        img[..4].copy_from_slice(&hdr);
        img[4..6].copy_from_slice(&(dim as i16).to_ne_bytes());
        Ok(VecBuilder { img })
    }

    #[inline]
    pub fn set(&mut self, i: usize, v: f32) {
        let off = 4 + VECTOR_PAYLOAD_HDR + i * 4;
        self.img[off..off + 4].copy_from_slice(&v.to_ne_bytes());
    }

    #[inline]
    pub fn get(&self, i: usize) -> f32 {
        let off = 4 + VECTOR_PAYLOAD_HDR + i * 4;
        f32::from_ne_bytes(self.img[off..off + 4].try_into().unwrap())
    }

    pub fn image(self) -> PgVec<'mcx, u8> {
        self.img
    }
}

#[track_caller]
#[cold]
fn dim_error() -> Box<PgError> {
    PgError::error("vector must have at least 1 dimension")
        .with_sqlstate(ERRCODE_DATA_EXCEPTION)
        .into()
}

#[track_caller]
#[cold]
fn max_dim_error() -> Box<PgError> {
    PgError::error(format!(
        "vector cannot have more than {VECTOR_MAX_DIM} dimensions"
    ))
    .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
    .into()
}

pub fn check_dim(dim: usize) -> PgResult<()> {
    if dim < 1 {
        return Err(dim_error());
    }
    if dim > VECTOR_MAX_DIM {
        return Err(max_dim_error());
    }
    Ok(())
}

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

pub fn check_element(value: f32) -> PgResult<()> {
    if value.is_nan() {
        return Err(PgError::error("NaN not allowed in vector")
            .with_sqlstate(ERRCODE_DATA_EXCEPTION)
            .into());
    }
    if value.is_infinite() {
        return Err(PgError::error("infinite value not allowed in vector")
            .with_sqlstate(ERRCODE_DATA_EXCEPTION)
            .into());
    }
    Ok(())
}

pub fn check_dims(a: &VecView<'_>, b: &VecView<'_>) -> PgResult<()> {
    if a.dim() != b.dim() {
        return Err(PgError::error(format!(
            "different vector dimensions {} and {}",
            a.dim(),
            b.dim()
        ))
        .with_sqlstate(ERRCODE_DATA_EXCEPTION)
        .into());
    }
    Ok(())
}

fn vector_isspace(ch: u8) -> bool {
    matches!(ch, b' ' | b'\t' | b'\n' | b'\r' | 0x0c)
}

#[track_caller]
#[cold]
fn invalid_text(lit: &[u8], detail: Option<&str>) -> Box<PgError> {
    let mut e = PgError::error(format!(
        "invalid input syntax for type vector: \"{}\"",
        String::from_utf8_lossy(lit)
    ))
    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION);
    if let Some(d) = detail {
        e = e.with_detail(d);
    }
    e.into()
}

// vector_in body (vector.c): strtof-per-element between '[' ']'.
pub fn parse_vector(lit: &[u8], typmod: i32, x: &mut [f32; VECTOR_MAX_DIM]) -> PgResult<usize> {
    let mut pt = 0usize;
    let n = lit.len();
    let mut dim = 0usize;

    while pt < n && vector_isspace(lit[pt]) {
        pt += 1;
    }
    if pt >= n || lit[pt] != b'[' {
        return Err(invalid_text(lit, Some("Vector contents must start with \"[\".")));
    }
    pt += 1;
    while pt < n && vector_isspace(lit[pt]) {
        pt += 1;
    }
    if pt < n && lit[pt] == b']' {
        return Err(dim_error());
    }

    loop {
        if dim == VECTOR_MAX_DIM {
            return Err(max_dim_error());
        }
        while pt < n && vector_isspace(lit[pt]) {
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
                "\"{}\" is out of range for type vector",
                String::from_utf8_lossy(&lit[pt..pt + tok_len])
            ))
            .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
            .into());
        }
        let StrtofVal::Ok(val) = val else { unreachable!() };
        check_element(val)?;
        x[dim] = val;
        dim += 1;
        pt += consumed;

        while pt < n && vector_isspace(lit[pt]) {
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

    while pt < n && vector_isspace(lit[pt]) {
        pt += 1;
    }
    if pt != n {
        return Err(invalid_text(lit, Some("Junk after closing right brace.")));
    }

    check_dim(dim)?;
    check_expected_dim(typmod, dim)?;
    Ok(dim)
}

pub enum StrtofVal {
    Ok(f32),
    // ERANGE with infinite result (overflow); payload = token length for the message.
    Erange(usize),
}

// libc strtof over a byte prefix: leading isspace skip, decimal/hex floats,
// inf/nan literals; underflow keeps the (denormal or zero) value like strtof.
pub fn strtof_prefix(s: &[u8]) -> Option<(StrtofVal, usize)> {
    let mut i = 0usize;
    while i < s.len() && (s[i] as char).is_ascii_whitespace() {
        i += 1;
    }
    let rest = &s[i..];
    let tok_len = float_token_len(rest)?;
    let token = core::str::from_utf8(&rest[..tok_len]).ok()?;
    let val: f32 = parse_float_token(token);
    if val.is_infinite() && !token_is_inf_literal(token) {
        return Some((StrtofVal::Erange(i + tok_len), i + tok_len));
    }
    Some((StrtofVal::Ok(val), i + tok_len))
}

fn token_is_inf_literal(tok: &str) -> bool {
    let t = tok.trim_start_matches(['+', '-']);
    t.len() >= 3 && t[..3].eq_ignore_ascii_case("inf")
}

fn parse_float_token(tok: &str) -> f32 {
    let neg = tok.starts_with('-');
    let t = tok.trim_start_matches(['+', '-']);
    if t.len() >= 3 && t[..3].eq_ignore_ascii_case("inf") {
        return if neg { f32::NEG_INFINITY } else { f32::INFINITY };
    }
    if t.len() >= 3 && t[..3].eq_ignore_ascii_case("nan") {
        return f32::NAN;
    }
    if t.len() >= 2 && t.as_bytes()[0] == b'0' && (t.as_bytes()[1] | 0x20) == b'x' {
        return parse_hex_f32(tok.as_bytes());
    }
    tok.parse::<f32>().unwrap_or(0.0)
}

// C99 hex-float grammar (strtof accepts it); round via f64 then narrow.
fn parse_hex_f32(tok: &[u8]) -> f32 {
    let mut i = 0usize;
    let mut neg = false;
    if i < tok.len() && (tok[i] == b'+' || tok[i] == b'-') {
        neg = tok[i] == b'-';
        i += 1;
    }
    i += 2; // 0x
    let mut mant: f64 = 0.0;
    let mut scale: f64 = 1.0;
    let mut seen_dot = false;
    while i < tok.len() {
        let c = tok[i];
        if c == b'.' {
            seen_dot = true;
            i += 1;
            continue;
        }
        let d = match (c as char).to_digit(16) {
            Some(d) => d,
            None => break,
        };
        mant = mant * 16.0 + d as f64;
        if seen_dot {
            scale *= 16.0;
        }
        i += 1;
    }
    mant /= scale;
    let mut exp: i32 = 0;
    if i < tok.len() && (tok[i] == b'p' || tok[i] == b'P') {
        i += 1;
        let mut eneg = false;
        if i < tok.len() && (tok[i] == b'+' || tok[i] == b'-') {
            eneg = tok[i] == b'-';
            i += 1;
        }
        while i < tok.len() && tok[i].is_ascii_digit() {
            exp = exp.saturating_mul(10).saturating_add((tok[i] - b'0') as i32);
            i += 1;
        }
        if eneg {
            exp = -exp;
        }
    }
    let v = mant * 2f64.powi(exp);
    (if neg { -v } else { v }) as f32
}

// Length of the leading strtof-recognizable token, or None.
fn float_token_len(s: &[u8]) -> Option<usize> {
    let mut i = 0usize;
    if i < s.len() && (s[i] == b'+' || s[i] == b'-') {
        i += 1;
    }
    let body = &s[i..];
    if body.len() >= 3 && body[..3].eq_ignore_ascii_case(b"inf") {
        if body.len() >= 8 && body[..8].eq_ignore_ascii_case(b"infinity") {
            return Some(i + 8);
        }
        return Some(i + 3);
    }
    if body.len() >= 3 && body[..3].eq_ignore_ascii_case(b"nan") {
        let mut j = 3;
        if body.len() > 3 && body[3] == b'(' {
            let mut k = 4;
            while k < body.len() && (body[k].is_ascii_alphanumeric() || body[k] == b'_') {
                k += 1;
            }
            if k < body.len() && body[k] == b')' {
                j = k + 1;
            }
        }
        return Some(i + j);
    }
    if body.len() >= 2
        && body[0] == b'0'
        && (body[1] == b'x' || body[1] == b'X')
        && body.len() > 2
        && (body[2].is_ascii_hexdigit() || body[2] == b'.')
    {
        let mut j = 2;
        let mut digits = 0;
        while j < body.len() && body[j].is_ascii_hexdigit() {
            j += 1;
            digits += 1;
        }
        if j < body.len() && body[j] == b'.' {
            j += 1;
            while j < body.len() && body[j].is_ascii_hexdigit() {
                j += 1;
                digits += 1;
            }
        }
        if digits == 0 {
            return None;
        }
        if j < body.len() && (body[j] == b'p' || body[j] == b'P') {
            let mut k = j + 1;
            if k < body.len() && (body[k] == b'+' || body[k] == b'-') {
                k += 1;
            }
            let e0 = k;
            while k < body.len() && body[k].is_ascii_digit() {
                k += 1;
            }
            if k > e0 {
                j = k;
            }
        }
        return Some(i + j);
    }
    let mut j = 0usize;
    let mut digits = 0;
    while j < body.len() && body[j].is_ascii_digit() {
        j += 1;
        digits += 1;
    }
    if j < body.len() && body[j] == b'.' {
        j += 1;
        while j < body.len() && body[j].is_ascii_digit() {
            j += 1;
            digits += 1;
        }
    }
    if digits == 0 {
        return None;
    }
    if j < body.len() && (body[j] == b'e' || body[j] == b'E') {
        let mut k = j + 1;
        if k < body.len() && (body[k] == b'+' || body[k] == b'-') {
            k += 1;
        }
        let e0 = k;
        while k < body.len() && body[k].is_ascii_digit() {
            k += 1;
        }
        if k > e0 {
            j = k;
        }
    }
    Some(i + j)
}

// ---- distance kernels ----
// Upstream compiles with -fassociative-math -ftree-vectorize, so the C
// extension's summation order is compiler-chosen. These loops keep C source
// order (no reassociation); see the port's float-summation audit note.

#[inline]
unsafe fn loadf(p: *const u8, i: usize) -> f32 {
    unsafe { core::ptr::read_unaligned(p.add(i * 4) as *const f32) }
}

pub fn l2_squared_distance(a: &VecView<'_>, b: &VecView<'_>) -> f32 {
    let (pa, pb) = (a.xs_ptr(), b.xs_ptr());
    let mut distance = 0.0f32;
    for i in 0..a.dim() {
        // SAFETY: from_payload bounds both buffers to dim*4 bytes.
        let diff = unsafe { loadf(pa, i) - loadf(pb, i) };
        distance += diff * diff;
    }
    distance
}

pub fn inner_product(a: &VecView<'_>, b: &VecView<'_>) -> f32 {
    let (pa, pb) = (a.xs_ptr(), b.xs_ptr());
    let mut distance = 0.0f32;
    for i in 0..a.dim() {
        // SAFETY: as above.
        distance += unsafe { loadf(pa, i) * loadf(pb, i) };
    }
    distance
}

pub fn cosine_similarity(a: &VecView<'_>, b: &VecView<'_>) -> f64 {
    let (pa, pb) = (a.xs_ptr(), b.xs_ptr());
    let mut similarity = 0.0f32;
    let mut norma = 0.0f32;
    let mut normb = 0.0f32;
    for i in 0..a.dim() {
        // SAFETY: as above.
        let (xa, xb) = unsafe { (loadf(pa, i), loadf(pb, i)) };
        similarity += xa * xb;
        norma += xa * xa;
        normb += xb * xb;
    }
    similarity as f64 / ((norma as f64) * (normb as f64)).sqrt()
}

pub fn l1_distance(a: &VecView<'_>, b: &VecView<'_>) -> f32 {
    let (pa, pb) = (a.xs_ptr(), b.xs_ptr());
    let mut distance = 0.0f32;
    for i in 0..a.dim() {
        // SAFETY: as above.
        distance += unsafe { (loadf(pa, i) - loadf(pb, i)).abs() };
    }
    distance
}

pub fn vector_norm(a: &VecView<'_>) -> f64 {
    let pa = a.xs_ptr();
    let mut norm = 0.0f64;
    for i in 0..a.dim() {
        // SAFETY: as above.
        let x = unsafe { loadf(pa, i) } as f64;
        norm += x * x;
    }
    norm.sqrt()
}

pub fn vector_cmp_internal(a: &VecView<'_>, b: &VecView<'_>) -> i32 {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Result<Vec<f32>, String> {
        let mut x = Box::new([0.0f32; VECTOR_MAX_DIM]);
        match parse_vector(s.as_bytes(), -1, &mut x) {
            Ok(n) => Ok(x[..n].to_vec()),
            Err(e) => Err(e.message().to_string()),
        }
    }

    #[test]
    fn parse_basics() {
        assert_eq!(parse("[1,2,3]").unwrap(), vec![1.0, 2.0, 3.0]);
        assert_eq!(parse("[1.,2.,3.]").unwrap(), vec![1.0, 2.0, 3.0]);
        assert_eq!(parse(" [ 1,  2 ,    3  ] ").unwrap(), vec![1.0, 2.0, 3.0]);
        assert_eq!(parse("[1e-46,1]").unwrap(), vec![0.0, 1.0]);
        assert_eq!(parse("[1.5e+38,-1.5e+38]").unwrap(), vec![1.5e38, -1.5e38]);
    }

    #[test]
    fn parse_errors() {
        assert!(parse("[hello,1]").unwrap_err().contains("invalid input syntax"));
        assert!(parse("[NaN,1]").unwrap_err().contains("NaN not allowed"));
        assert!(parse("[Infinity,1]").unwrap_err().contains("infinite value not allowed"));
        assert!(parse("[4e38,1]").unwrap_err().contains("\"4e38\" is out of range"));
        assert!(parse("[]").unwrap_err().contains("at least 1 dimension"));
        assert!(parse("[1,2,3").unwrap_err().contains("invalid input syntax"));
        assert!(parse("[1,2,3]x").unwrap_err().contains("invalid input syntax"));
        assert!(parse("1,2,3").unwrap_err().contains("invalid input syntax"));
    }

    #[test]
    fn cmp_matches_c() {
        let cx = mcx::MemoryContext::new_bump("t");
        let m = cx.mcx();
        let mk = |vals: &[f32]| {
            let mut b = VecBuilder::new(m, vals.len()).unwrap();
            for (i, v) in vals.iter().enumerate() {
                b.set(i, *v);
            }
            b.image()
        };
        let a = mk(&[1.0, 2.0]);
        let b = mk(&[1.0, 2.0, 3.0]);
        let va = VecView::from_payload(&a[4..]).unwrap();
        let vb = VecView::from_payload(&b[4..]).unwrap();
        assert_eq!(vector_cmp_internal(&va, &vb), -1);
        assert_eq!(vector_cmp_internal(&vb, &va), 1);
        assert_eq!(vector_cmp_internal(&va, &va), 0);
    }

    #[test]
    fn distance_kernels() {
        let cx = mcx::MemoryContext::new_bump("t");
        let m = cx.mcx();
        let mk = |vals: &[f32]| {
            let mut b = VecBuilder::new(m, vals.len()).unwrap();
            for (i, v) in vals.iter().enumerate() {
                b.set(i, *v);
            }
            b.image()
        };
        let a = mk(&[0.0, 0.0]);
        let b = mk(&[3.0, 4.0]);
        let va = VecView::from_payload(&a[4..]).unwrap();
        let vb = VecView::from_payload(&b[4..]).unwrap();
        assert_eq!(l2_squared_distance(&va, &vb), 25.0);
        assert_eq!(inner_product(&va, &vb), 0.0);
        assert_eq!(l1_distance(&va, &vb), 7.0);
        assert_eq!(vector_norm(&vb), 5.0);
    }
}
