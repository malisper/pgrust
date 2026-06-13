//! FAMILY: bytea I/O + scalar ops + comparison + int casts.
//!
//! `byteain`/`byteaout`/`bytearecv`/`byteasend`, `byteacat`/`bytea_catenate`,
//! `byteaoctetlen`, `byteaoverlay*`/`bytea_overlay`, `byteapos`,
//! `byteaGetByte`/`byteaGetBit`/`byteaSetByte`/`byteaSetBit`, `bytea_reverse`,
//! `bytea_bit_count`, the bytea relational ops
//! (`byteaeq`/`byteane`/`bytealt`/`byteale`/`byteagt`/`byteage`/`byteacmp`,
//! `bytea_larger`/`bytea_smaller`), and the bytea<->int casts
//! (`bytea_int2`/`int4`/`int8`, `int2_bytea`/`int4_bytea`/`int8_bytea`).
//!
//! `bytea` comparison is always raw `memcmp` + length tiebreak (no
//! collation). Depends on the keystone carrier conventions only.

#![allow(unused_variables)]

use mcx::{Mcx, PgVec};
use types_error::PgResult;

/// C: `byteain(PG_FUNCTION_ARGS)` — parse `\x...` hex or traditional escaped
/// input into a `bytea` payload.
pub fn byteain<'mcx>(mcx: Mcx<'mcx>, input: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    todo!("bytea family: port byteain")
}

/// C: `byteaout(PG_FUNCTION_ARGS)` — render a `bytea` payload as the `\x` hex
/// or escaped form per the `bytea_output` GUC.
pub fn byteaout<'mcx>(mcx: Mcx<'mcx>, v: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    todo!("bytea family: port byteaout")
}

/// C: `bytea_catenate(bytea *t1, bytea *t2)` (guts of `byteacat`).
pub fn bytea_catenate<'mcx>(mcx: Mcx<'mcx>, t1: &[u8], t2: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    todo!("bytea family: port bytea_catenate")
}

/// C: `byteacmp(PG_FUNCTION_ARGS)` — raw `memcmp` + length tiebreak.
pub fn byteacmp(a: &[u8], b: &[u8]) -> PgResult<i32> {
    todo!("bytea family: port byteacmp")
}

/// C: `byteaGetByte(PG_FUNCTION_ARGS)` — 0-based byte fetch (array-subscript
/// error out of range).
pub fn bytea_get_byte(v: &[u8], n: i32) -> PgResult<i32> {
    todo!("bytea family: port byteaGetByte")
}

/// C: `byteaGetBit` / `byteaSetByte` / `byteaSetBit` / `bytea_reverse` /
/// `bytea_bit_count` / `byteapos` and the bytea<->int casts are filled here
/// alongside the above.
pub fn bytea_set_byte<'mcx>(mcx: Mcx<'mcx>, v: &[u8], n: i32, newbyte: i32) -> PgResult<PgVec<'mcx, u8>> {
    todo!("bytea family: port byteaSetByte")
}

/// C: `bytea_int4(PG_FUNCTION_ARGS)` — big-endian decode (errors if too long).
pub fn bytea_int4(v: &[u8]) -> PgResult<i32> {
    todo!("bytea family: port bytea_int4")
}

/// C: `int4_bytea(PG_FUNCTION_ARGS)` — big-endian 4-byte encode.
pub fn int4_bytea<'mcx>(mcx: Mcx<'mcx>, val: i32) -> PgResult<PgVec<'mcx, u8>> {
    todo!("bytea family: port int4_bytea")
}
