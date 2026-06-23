//! FAMILY: text/unknown wire I/O, name<->text, and the length/concat SQL
//! entry points.
//!
//! `textin`/`textout`/`textrecv`/`textsend`,
//! `unknownin`/`unknownout`/`unknownrecv`/`unknownsend`,
//! `text_name`/`name_text`, `textlen`/`textoctetlen`/`textcat` (the SQL
//! wrappers over the keystone `text_length`/`text_catenate`).
//!
//! `recv`/`send` consult the `pq` wire-format buffer; `textin`/`textout`
//! consult the client/server encoding converters (mbutils seam). Depends on
//! the keystone carrier conventions and `text_length`/`text_catenate`.

use mcx::{Mcx, PgVec};
use datum::Bytea;
use types_error::PgResult;
use stringinfo::StringInfo;

use pqformat as pq;
use mbutils_seams as mb;

use crate::keystone;

/// C: `textin(PG_FUNCTION_ARGS)` — `cstring` -> `text` (client/server
/// encoding conversion happens at the fmgr boundary; here the payload is the
/// verified server-encoding bytes).
///
/// C: `PG_RETURN_TEXT_P(cstring_to_text(inputText))`.
pub fn textin<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    keystone::cstring_to_text(mcx, s)
}

/// C: `textout(PG_FUNCTION_ARGS)` — `text` -> `cstring`.
///
/// C: `PG_RETURN_CSTRING(TextDatumGetCString(txt))` where
/// `TextDatumGetCString` is `text_to_cstring(DatumGetTextPP(d))`. The carrier
/// `t` is the already-detoasted payload; the result is its NUL-terminated
/// copy.
pub fn textout<'mcx>(mcx: Mcx<'mcx>, t: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    keystone::text_to_cstring(mcx, t)
}

/// C: `textrecv(PG_FUNCTION_ARGS)` — external binary format -> `text`.
///
/// C reads the remaining bytes of the message as a (converted) counted text
/// string via `pq_getmsgtext`, then builds the `text` with
/// `cstring_to_text_with_len`. The result is the `text` payload charged to
/// `mcx`.
pub fn textrecv<'mcx>(mcx: Mcx<'mcx>, buf: &mut StringInfo<'_>) -> PgResult<PgVec<'mcx, u8>> {
    // C: str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);
    let rawbytes = buf.data.len().saturating_sub(buf.cursor);
    let str = pq::pq_getmsgtext(mcx, buf, rawbytes)?;
    // C: result = cstring_to_text_with_len(str, nbytes); pfree(str).
    // `pq_getmsgtext` returns the contents without the terminator, so its
    // length is C's `nbytes`.
    let nbytes = str.len() as i32;
    keystone::cstring_to_text_with_len(mcx, &str, nbytes)
}

/// C: `textsend(PG_FUNCTION_ARGS)` — `text` -> external binary format.
///
/// C: `pq_begintypsend(&buf); pq_sendtext(&buf, VARDATA_ANY(t),
/// VARSIZE_ANY_EXHDR(t)); PG_RETURN_BYTEA_P(pq_endtypsend(&buf))`. The carrier
/// `t` is the payload bytes (`VARDATA_ANY` of size `VARSIZE_ANY_EXHDR`).
pub fn textsend<'mcx>(mcx: Mcx<'mcx>, t: &[u8]) -> PgResult<Bytea<'mcx>> {
    let mut buf = pq::pq_begintypsend(mcx)?;
    pq::pq_sendtext(&mut buf, t)?;
    Ok(pq::pq_endtypsend(buf))
}

/// C: `unknownin(PG_FUNCTION_ARGS)` — `cstring` -> internal `unknown`.
///
/// C: `PG_RETURN_CSTRING(pstrdup(str))` — the representation is the same as a
/// cstring, so this is just a copy charged to `mcx` (NUL-terminated, per the
/// cstring contract).
pub fn unknownin<'mcx>(mcx: Mcx<'mcx>, str: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    pstrdup(mcx, str)
}

/// C: `unknownout(PG_FUNCTION_ARGS)` — internal `unknown` -> `cstring`.
///
/// C: `PG_RETURN_CSTRING(pstrdup(str))` — representation is the same as a
/// cstring; a copy charged to `mcx`.
pub fn unknownout<'mcx>(mcx: Mcx<'mcx>, str: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    pstrdup(mcx, str)
}

/// C: `unknownrecv(PG_FUNCTION_ARGS)` — external binary format -> `unknown`.
///
/// C: `str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);
/// PG_RETURN_CSTRING(str)` — representation is the same as a cstring, returned
/// directly (already a fresh palloc'd result from `pq_getmsgtext`, with a
/// trailing NUL).
pub fn unknownrecv<'mcx>(mcx: Mcx<'mcx>, buf: &mut StringInfo<'_>) -> PgResult<PgVec<'mcx, u8>> {
    let rawbytes = buf.data.len().saturating_sub(buf.cursor);
    pq::pq_getmsgtext(mcx, buf, rawbytes)
}

/// C: `unknownsend(PG_FUNCTION_ARGS)` — `unknown` -> external binary format.
///
/// C: `pq_begintypsend(&buf); pq_sendtext(&buf, str, strlen(str));
/// PG_RETURN_BYTEA_P(pq_endtypsend(&buf))`. The carrier `str` is the cstring
/// bytes; `strlen(str)` is the length up to (but excluding) the trailing NUL.
pub fn unknownsend<'mcx>(mcx: Mcx<'mcx>, str: &[u8]) -> PgResult<Bytea<'mcx>> {
    let mut buf = pq::pq_begintypsend(mcx)?;
    // C uses strlen(str): send the bytes up to the first NUL (the logical
    // cstring value), matching the C representation exactly.
    let len = str.iter().position(|&b| b == 0).unwrap_or(str.len());
    pq::pq_sendtext(&mut buf, &str[..len])?;
    Ok(pq::pq_endtypsend(buf))
}

/// C: `pstrdup(const char *in)` — a NUL-terminated copy of a cstring charged
/// to `mcx`. The input `str` may already include a trailing NUL (the cstring
/// contract); the copy duplicates the bytes up to the first NUL and re-appends
/// exactly one NUL terminator.
fn pstrdup<'mcx>(mcx: Mcx<'mcx>, str: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let len = str.iter().position(|&b| b == 0).unwrap_or(str.len());
    let mut out = mcx::vec_with_capacity_in(mcx, len + 1)?;
    out.extend_from_slice(&str[..len]);
    out.push(0);
    Ok(out)
}

/// C: `textlen(PG_FUNCTION_ARGS)` — SQL `length(text)` over the keystone
/// `text_length`.
///
/// C: `PG_RETURN_INT32(text_length(str))`.
pub fn textlen(t: &[u8]) -> PgResult<i32> {
    keystone::text_length(t)
}

/// C: `textoctetlen(PG_FUNCTION_ARGS)` — physical (byte) length.
///
/// C: `PG_RETURN_INT32(toast_raw_datum_size(str) - VARHDRSZ)`. The carrier is
/// the already-detoasted payload, so its raw datum size minus `VARHDRSZ` is
/// exactly the payload length.
pub fn textoctetlen(t: &[u8]) -> PgResult<i32> {
    Ok(t.len() as i32)
}

/// C: `textcat(PG_FUNCTION_ARGS)` — SQL `||` over the keystone
/// `text_catenate`.
///
/// C: `PG_RETURN_TEXT_P(text_catenate(t1, t2))`.
pub fn textcat<'mcx>(mcx: Mcx<'mcx>, t1: &[u8], t2: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    keystone::text_catenate(mcx, t1, t2)
}

/// C: `text_name(PG_FUNCTION_ARGS)` — `text` -> `name` (truncate to
/// NAMEDATALEN-1, zero-pad).
pub fn text_name(t: &[u8]) -> PgResult<[u8; keystone::NAMEDATALEN]> {
    // C: len = VARSIZE_ANY_EXHDR(s); the carrier is the payload.
    let mut len = t.len();
    // C: if (len >= NAMEDATALEN) len = pg_mbcliplen(VARDATA_ANY(s), len,
    //    NAMEDATALEN - 1);
    if len >= keystone::NAMEDATALEN {
        len = mb::pg_mbcliplen::call(t, len as i32, (keystone::NAMEDATALEN - 1) as i32).max(0)
            as usize;
    }
    // C: palloc0(NAMEDATALEN) — zero-padded; memcpy(NameStr(*result),
    //    VARDATA_ANY(s), len).
    let mut result = [0u8; keystone::NAMEDATALEN];
    result[..len].copy_from_slice(&t[..len]);
    Ok(result)
}

/// C: `name_text(PG_FUNCTION_ARGS)` — `name` -> `text` (bytes up to NUL).
///
/// C: `PG_RETURN_TEXT_P(cstring_to_text(NameStr(*s)))`. `NameStr` is the
/// NUL-terminated cstring view of the fixed-width name buffer, so the logical
/// value is the bytes up to the first NUL.
pub fn name_text<'mcx>(mcx: Mcx<'mcx>, name: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let len = name.iter().position(|&b| b == 0).unwrap_or(name.len());
    keystone::cstring_to_text(mcx, &name[..len])
}
