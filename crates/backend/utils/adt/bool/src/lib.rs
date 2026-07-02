pub mod builtins;
#[cfg(test)]
mod tests;

extern crate alloc;

use alloc::boxed::Box;
use alloc::format;

use ::datum::{Bytea, Varlena};
use ::mcx::Mcx;
use ::stringinfo::StringInfo;
use ::types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_INTERNAL_ERROR,
    ERRCODE_INVALID_TEXT_REPRESENTATION,
};

fn is_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

// pg_strncasecmp(value, lit, n) == 0 for the ASCII literals below; reads past
// either slice yield NUL (C reads the cstring terminator there; for the
// n = 2 > len 'o' case C may read a trimmed trailing space instead, which
// mismatches 'n'/'f' exactly as NUL does).
fn ncasecmp_eq(value: &[u8], lit: &[u8], n: usize) -> bool {
    for i in 0..n {
        let ch1 = value.get(i).copied().unwrap_or(0);
        let ch2 = lit.get(i).copied().unwrap_or(0);
        if ch1.to_ascii_lowercase() != ch2 {
            return false;
        }
        if ch1 == 0 {
            break;
        }
    }
    true
}

pub fn parse_bool(value: &str) -> Option<bool> {
    parse_bool_with_len(value.as_bytes())
}

pub fn parse_bool_with_len(value: &[u8]) -> Option<bool> {
    let len = value.len();
    match value.first().copied().unwrap_or(0) {
        b't' | b'T' if ncasecmp_eq(value, b"true", len) => Some(true),
        b'f' | b'F' if ncasecmp_eq(value, b"false", len) => Some(false),
        b'y' | b'Y' if ncasecmp_eq(value, b"yes", len) => Some(true),
        b'n' | b'N' if ncasecmp_eq(value, b"no", len) => Some(false),
        // 'o' is not unique enough
        b'o' | b'O' if ncasecmp_eq(value, b"on", len.max(2)) => Some(true),
        b'o' | b'O' if ncasecmp_eq(value, b"off", len.max(2)) => Some(false),
        b'1' if len == 1 => Some(true),
        b'0' if len == 1 => Some(false),
        _ => None,
    }
}

#[cold]
#[inline(never)]
fn invalid_syntax_err(input: &str) -> PgError {
    PgError::error(format!("invalid input syntax for type boolean: \"{input}\""))
        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
}

pub fn boolin(in_str: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<bool> {
    let bytes = in_str.as_bytes();
    let start = bytes
        .iter()
        .position(|&b| !is_space(b))
        .unwrap_or(bytes.len());
    let mut end = bytes.len();
    while end > start && is_space(bytes[end - 1]) {
        end -= 1;
    }
    if let Some(result) = parse_bool_with_len(&bytes[start..end]) {
        return Ok(result);
    }
    ereturn(escontext, false, invalid_syntax_err(in_str))
}

pub const fn boolout(b: bool) -> u8 {
    if b {
        b't'
    } else {
        b'f'
    }
}

pub fn boolrecv(buf: &mut StringInfo<'_>) -> PgResult<bool> {
    Ok(pqformat::pq_getmsgbyte(buf)? != 0)
}

pub fn boolsend<'mcx>(mcx: Mcx<'mcx>, arg1: bool) -> PgResult<Bytea<'mcx>> {
    let mut buf = pqformat::pq_begintypsend(mcx)?;
    pqformat::pq_sendbyte(&mut buf, arg1 as u8)?;
    Ok(pqformat::pq_endtypsend(buf))
}

// SQL-spec spelling, unlike boolout.
pub fn booltext<'mcx>(mcx: Mcx<'mcx>, arg1: bool) -> PgResult<Varlena<'mcx>> {
    varlena::cstring_to_text(mcx, if arg1 { b"true" } else { b"false" })
}

pub const fn booleq(arg1: bool, arg2: bool) -> bool {
    arg1 == arg2
}

pub const fn boolne(arg1: bool, arg2: bool) -> bool {
    arg1 != arg2
}

pub const fn boollt(arg1: bool, arg2: bool) -> bool {
    (arg1 as u8) < arg2 as u8
}

pub const fn boolgt(arg1: bool, arg2: bool) -> bool {
    arg1 as u8 > arg2 as u8
}

pub const fn boolle(arg1: bool, arg2: bool) -> bool {
    arg1 as u8 <= arg2 as u8
}

pub const fn boolge(arg1: bool, arg2: bool) -> bool {
    arg1 as u8 >= arg2 as u8
}

pub fn hashbool(arg: bool) -> u32 {
    hashfn::hash_bytes_uint32(arg as i32 as u32)
}

pub fn hashboolextended(arg: bool, seed: u64) -> u64 {
    hashfn::hash_bytes_uint32_extended(arg as i32 as u32, seed)
}

pub const fn booland_statefunc(arg1: bool, arg2: bool) -> bool {
    arg1 && arg2
}

pub const fn boolor_statefunc(arg1: bool, arg2: bool) -> bool {
    arg1 || arg2
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BoolAggState {
    pub aggcount: i64,
    pub aggtrue: i64,
}

// C allocates the state in the agg context on first call (makeBoolAggState);
// the registering agg frame owns the storage and the non-agg-context check.
pub fn bool_accum(state: Option<BoolAggState>, val: Option<bool>) -> BoolAggState {
    let mut state = state.unwrap_or_default();
    if let Some(v) = val {
        state.aggcount += 1;
        if v {
            state.aggtrue += 1;
        }
    }
    state
}

pub fn bool_accum_inv(state: Option<BoolAggState>, val: Option<bool>) -> PgResult<BoolAggState> {
    let Some(mut state) = state else {
        return Err(Box::new(
            PgError::error("bool_accum_inv called with NULL state")
                .with_sqlstate(ERRCODE_INTERNAL_ERROR),
        ));
    };
    if let Some(v) = val {
        state.aggcount -= 1;
        if v {
            state.aggtrue -= 1;
        }
    }
    Ok(state)
}

pub fn bool_alltrue(state: Option<&BoolAggState>) -> Option<bool> {
    match state {
        None => None,
        Some(s) if s.aggcount == 0 => None,
        Some(s) => Some(s.aggtrue == s.aggcount),
    }
}

pub fn bool_anytrue(state: Option<&BoolAggState>) -> Option<bool> {
    match state {
        None => None,
        Some(s) if s.aggcount == 0 => None,
        Some(s) => Some(s.aggtrue > 0),
    }
}

pub fn init_seams() {
    scalar_seams::parse_bool::set(parse_bool);
}
