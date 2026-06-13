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

#![allow(unused_variables)]

use mcx::{Mcx, PgVec};
use types_error::PgResult;

/// C: `textin(PG_FUNCTION_ARGS)` — `cstring` -> `text` (client/server
/// encoding conversion happens at the fmgr boundary; here the payload is the
/// verified server-encoding bytes).
pub fn textin<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    todo!("wire_io family: port textin")
}

/// C: `textout(PG_FUNCTION_ARGS)` — `text` -> `cstring`.
pub fn textout<'mcx>(mcx: Mcx<'mcx>, t: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    todo!("wire_io family: port textout")
}

/// C: `textlen(PG_FUNCTION_ARGS)` — SQL `length(text)` over the keystone
/// `text_length`.
pub fn textlen(t: &[u8]) -> PgResult<i32> {
    todo!("wire_io family: port textlen (delegates to keystone::text_length)")
}

/// C: `textoctetlen(PG_FUNCTION_ARGS)` — physical (byte) length.
pub fn textoctetlen(t: &[u8]) -> PgResult<i32> {
    todo!("wire_io family: port textoctetlen")
}

/// C: `textcat(PG_FUNCTION_ARGS)` — SQL `||` over the keystone
/// `text_catenate`.
pub fn textcat<'mcx>(mcx: Mcx<'mcx>, t1: &[u8], t2: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    todo!("wire_io family: port textcat (delegates to keystone::text_catenate)")
}

/// C: `text_name(PG_FUNCTION_ARGS)` — `text` -> `name` (truncate to
/// NAMEDATALEN-1, zero-pad).
pub fn text_name(t: &[u8]) -> PgResult<[u8; crate::keystone::NAMEDATALEN]> {
    todo!("wire_io family: port text_name")
}

/// C: `name_text(PG_FUNCTION_ARGS)` — `name` -> `text` (bytes up to NUL).
pub fn name_text<'mcx>(mcx: Mcx<'mcx>, name: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    todo!("wire_io family: port name_text")
}
