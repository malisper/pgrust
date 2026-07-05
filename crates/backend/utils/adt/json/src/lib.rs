//! json.c, the TEXT-backed `json` type: json_in validates via the recursive
//! descent parser (jsonapi) and stores the text verbatim; json_out is
//! verbatim. Rendering (to_json/row_to_json/array_to_json, builders,
//! aggregates) and the jsonfuncs.c json-half workers are lex-based over the
//! stored text. Loud via the unported-OID fmgr gap: json_populate_record
//! family, jsonpath.

pub mod aggs;
pub mod builtins;
pub mod funcs;
pub mod getpath;
pub mod jsonapi;
pub mod srfs;
pub mod tojson;
#[cfg(test)]
mod tests;

use datum::{Bytea, Varlena};
use jsonapi::{JsonError, JsonLex};
use mcx::Mcx;
use stringinfo::StringInfo;
use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_UNTRANSLATABLE_CHARACTER,
};

// C: json_errsave_error (shared by json and jsonb; the errmsg says "json" for
// both). JSON_SEM_ACTION_FAILED never reaches here — callers handle it.
#[cold]
#[inline(never)]
pub fn errsave_parse_error(
    error: JsonError,
    lex: &JsonLex<'_>,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<()> {
    let err = if matches!(
        error,
        JsonError::UnicodeUntranslatable | JsonError::UnicodeCodePointZero
    ) {
        PgError::error("unsupported Unicode escape sequence")
            .with_sqlstate(ERRCODE_UNTRANSLATABLE_CHARACTER)
    } else {
        PgError::error("invalid input syntax for type json")
            .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
    };
    let err = err
        .with_detail(lex.errdetail(error))
        .with_context(lex.errcontext());
    ereturn(escontext, (), err)
}

/// C: json_in. Validate the input text; on success the stored representation is
/// the input bytes verbatim (json is validated text). Returns `None` when a
/// soft-error context absorbed a parse failure.
pub fn json_in<'mcx>(
    mcx: Mcx<'mcx>,
    input: &[u8],
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<Varlena<'mcx>>> {
    let mut lex = JsonLex::new(input, mbutils::GetDatabaseEncoding());
    let result = jsonapi::parse(&mut lex)?;
    if result != JsonError::Success {
        errsave_parse_error(result, &lex, escontext)?;
        return Ok(None);
    }
    Ok(Some(varlena::cstring_to_text(mcx, input)?))
}

/// C: json_out — TextDatumGetCString, verbatim (json is stored as text).
pub fn json_out<'mcx>(mcx: Mcx<'mcx>, t: &[u8]) -> PgResult<mcx::PgVec<'mcx, u8>> {
    varlena::text_to_cstring(mcx, t)
}

/// C: json_recv — read the message text, validate, store verbatim.
pub fn json_recv<'mcx>(mcx: Mcx<'mcx>, buf: &mut StringInfo<'_>) -> PgResult<Varlena<'mcx>> {
    let rawbytes = buf.len().saturating_sub(buf.cursor);
    let str = pqformat::pq_getmsgtext(mcx, buf, rawbytes)?;
    let mut lex = JsonLex::new(&str, mbutils::GetDatabaseEncoding());
    let result = jsonapi::parse(&mut lex)?;
    if result != JsonError::Success {
        errsave_parse_error(result, &lex, None)?;
        unreachable!("hard errsave without escontext returns Err");
    }
    varlena::cstring_to_text(mcx, &str)
}

/// C: json_send — verbatim text bytes over the wire.
pub fn json_send<'mcx>(mcx: Mcx<'mcx>, t: &[u8]) -> PgResult<Bytea<'mcx>> {
    let mut sbuf = pqformat::pq_begintypsend(mcx)?;
    pqformat::pq_sendtext(&mut sbuf, t)?;
    Ok(pqformat::pq_endtypsend(sbuf))
}

/// C: escape_json/escape_json_with_len — produce a JSON string literal.
/// Clean runs are appended in bulk (C's Vector8 scan + flush shape).
pub fn escape_json(buf: &mut StringInfo<'_>, s: &[u8]) -> PgResult<()> {
    buf.enlarge(s.len() + 2)?;
    buf.append_byte(b'"')?;
    let mut copypos = 0;
    for (i, &c) in s.iter().enumerate() {
        if c < b' ' || c == b'"' || c == b'\\' {
            buf.append_bytes(&s[copypos..i])?;
            escape_json_char(buf, c)?;
            copypos = i + 1;
        }
    }
    buf.append_bytes(&s[copypos..])?;
    buf.append_byte(b'"')
}

fn escape_json_char(buf: &mut StringInfo<'_>, c: u8) -> PgResult<()> {
    match c {
        0x08 => buf.append_bytes(b"\\b"),
        0x0c => buf.append_bytes(b"\\f"),
        b'\n' => buf.append_bytes(b"\\n"),
        b'\r' => buf.append_bytes(b"\\r"),
        b'\t' => buf.append_bytes(b"\\t"),
        b'"' => buf.append_bytes(b"\\\""),
        b'\\' => buf.append_bytes(b"\\\\"),
        _ if c < b' ' => {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            buf.append_bytes(&[b'\\', b'u', b'0', b'0', HEX[(c >> 4) as usize], HEX[(c & 0xf) as usize]])
        }
        _ => buf.append_byte(c),
    }
}

pub fn init_seams() {}
