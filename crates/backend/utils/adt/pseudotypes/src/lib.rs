//! pseudotypes.c: ereport-only I/O stubs, real cstring/void I/O, and the
//! pg_node_tree delegates onto the ported varlena text lanes. Outputs whose
//! delegate unit is unported (array/enum/range/multirange *_out and *_send)
//! are named loud panics.

pub mod builtins;
#[cfg(test)]
mod tests;

use datum::{Bytea, Datum};
use mcx::{Mcx, PgVec};
use stringinfo::StringInfo;
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};

#[cold]
#[inline(never)]
fn cannot_accept(typname: &str) -> PgError {
    PgError::error(format!("cannot accept a value of type {typname}"))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
}

#[cold]
#[inline(never)]
fn cannot_display(typname: &str) -> PgError {
    PgError::error(format!("cannot display a value of type {typname}"))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
}

macro_rules! dummy_accept {
    ($($fname:ident: $typname:literal;)*) => {$(
        pub fn $fname() -> PgResult<Datum> {
            Err(cannot_accept($typname).into())
        }
    )*};
}

macro_rules! dummy_display {
    ($($fname:ident: $typname:literal;)*) => {$(
        pub fn $fname() -> PgResult<Datum> {
            Err(cannot_display($typname).into())
        }
    )*};
}

dummy_accept! {
    anyarray_in: "anyarray";
    anyarray_recv: "anyarray";
    anycompatiblearray_in: "anycompatiblearray";
    anycompatiblearray_recv: "anycompatiblearray";
    anyenum_in: "anyenum";
    anyrange_in: "anyrange";
    anycompatiblerange_in: "anycompatiblerange";
    anymultirange_in: "anymultirange";
    anycompatiblemultirange_in: "anycompatiblemultirange";
    pg_node_tree_in: "pg_node_tree";
    pg_node_tree_recv: "pg_node_tree";
    pg_ddl_command_in: "pg_ddl_command";
    pg_ddl_command_recv: "pg_ddl_command";
    any_in: "any";
    trigger_in: "trigger";
    event_trigger_in: "event_trigger";
    language_handler_in: "language_handler";
    fdw_handler_in: "fdw_handler";
    table_am_handler_in: "table_am_handler";
    index_am_handler_in: "index_am_handler";
    tsm_handler_in: "tsm_handler";
    internal_in: "internal";
    anyelement_in: "anyelement";
    anynonarray_in: "anynonarray";
    anycompatible_in: "anycompatible";
    anycompatiblenonarray_in: "anycompatiblenonarray";
}

dummy_display! {
    pg_ddl_command_out: "pg_ddl_command";
    pg_ddl_command_send: "pg_ddl_command";
    any_out: "any";
    trigger_out: "trigger";
    event_trigger_out: "event_trigger";
    language_handler_out: "language_handler";
    fdw_handler_out: "fdw_handler";
    table_am_handler_out: "table_am_handler";
    index_am_handler_out: "index_am_handler";
    tsm_handler_out: "tsm_handler";
    internal_out: "internal";
    anyelement_out: "anyelement";
    anynonarray_out: "anynonarray";
    anycompatible_out: "anycompatible";
    anycompatiblenonarray_out: "anycompatiblenonarray";
}

pub fn shell_in() -> PgResult<Datum> {
    Err(PgError::error("cannot accept a value of a shell type")
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
        .into())
}

pub fn shell_out() -> PgResult<Datum> {
    Err(PgError::error("cannot display a value of a shell type")
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
        .into())
}

macro_rules! unported_delegate {
    ($($fname:ident -> $target:literal in $unit:literal;)*) => {$(
        pub fn $fname() -> ! {
            panic!(concat!(
                stringify!($fname), ": delegates to ", $target, " (", $unit, " not ported)"
            ))
        }
    )*};
}

unported_delegate! {
    anyarray_out -> "array_out" in "arrayfuncs";
    anyarray_send -> "array_send" in "arrayfuncs";
    anycompatiblearray_out -> "array_out" in "arrayfuncs";
    anycompatiblearray_send -> "array_send" in "arrayfuncs";
    anyenum_out -> "enum_out" in "adt_enum";
    anyrange_out -> "range_out" in "rangetypes";
    anycompatiblerange_out -> "range_out" in "rangetypes";
    anymultirange_out -> "multirange_out" in "multirangetypes";
    anycompatiblemultirange_out -> "multirange_out" in "multirangetypes";
}

// C: pstrdup — bytes to the first NUL, re-terminated.
fn pstrdup<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let len = s.iter().position(|&b| b == 0).unwrap_or(s.len());
    let mut out = mcx::vec_with_capacity_in(mcx, len + 1)?;
    mcx::vec_append_bytes(&mut out, &s[..len])?;
    out.push(0);
    Ok(out)
}

pub fn cstring_in<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    pstrdup(mcx, s)
}

pub fn cstring_out<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    pstrdup(mcx, s)
}

pub fn cstring_recv<'mcx>(mcx: Mcx<'mcx>, buf: &mut StringInfo<'_>) -> PgResult<PgVec<'mcx, u8>> {
    let rawbytes = buf.len().saturating_sub(buf.cursor);
    let mut str = pqformat::pq_getmsgtext(mcx, buf, rawbytes)?;
    str.push(0);
    Ok(str)
}

pub fn cstring_send<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<Bytea<'mcx>> {
    let len = s.iter().position(|&b| b == 0).unwrap_or(s.len());
    let mut buf = pqformat::pq_begintypsend(mcx)?;
    pqformat::pq_sendtext(&mut buf, &s[..len])?;
    Ok(pqformat::pq_endtypsend(buf))
}

pub const fn void_in() -> Datum {
    Datum::null()
}

pub fn void_out<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, u8>> {
    pstrdup(mcx, b"")
}

// C: consumes no bytes; a non-empty payload fails at the message frame.
pub const fn void_recv() -> Datum {
    Datum::null()
}

pub fn void_send(mcx: Mcx<'_>) -> PgResult<Bytea<'_>> {
    let buf = pqformat::pq_begintypsend(mcx)?;
    Ok(pqformat::pq_endtypsend(buf))
}

pub fn pg_node_tree_out<'mcx>(mcx: Mcx<'mcx>, t: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    varlena::textout(mcx, t)
}

pub fn pg_node_tree_send<'mcx>(mcx: Mcx<'mcx>, t: &[u8]) -> PgResult<Bytea<'mcx>> {
    varlena::textsend(mcx, t)
}
