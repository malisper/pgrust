#![no_std]
#![allow(clippy::result_large_err)]

//! Port of PostgreSQL 18.3 `src/backend/utils/adt/pseudotypes.c`: the I/O
//! functions for the system *pseudo-types*.
//!
//! A pseudo-type isn't really a type and never has any operations, but we do
//! need to supply input and output functions to satisfy the links in the
//! pseudo-type's entry in `pg_type`. In most cases the functions just throw an
//! error if invoked — those `ereport(ERROR)`s ARE the implementation, ported as
//! real `PgError` returns (`ERRCODE_FEATURE_NOT_SUPPORTED`). `cstring` and
//! `void` carry full working I/O; a handful of *delegating* output/send
//! functions forward to a real type's I/O function.
//!
//! Like the sibling adt ports (`backend-utils-adt-char`,
//! `backend-utils-adt-version`) these are plain typed Rust functions; the
//! fmgr/`Datum` argument-decode boundary (the bare-word `PGFunction` registry)
//! is not part of this unit. A by-value argument arrives as a [`Datum`]; a
//! `cstring` arrives as `&str`; a by-reference value (array / text /
//! multirange) arrives as its detoasted payload bytes / value; binary I/O uses
//! the [`StringInfo`] message buffer.
//!
//! The delegating outputs/sends are the C `return target(fcinfo)` one-liners:
//! `anyarray_out -> array_out`, `pg_node_tree_out -> textout`, …. Targets that
//! would form a dependency cycle (`range_out`, and the not-yet-ported
//! `enum_out`) are reached through their owner's seam crate; the rest are
//! direct calls.

extern crate alloc;

use backend_libpq_pqformat::{pq_begintypsend, pq_endtypsend, pq_getmsgtext};
use mcx::{Mcx, PgString, PgVec};
use types_datum::{Bytea, Datum};
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use types_rangetypes::RangeTypeP;
use types_stringinfo::StringInfo;

// ===========================================================================
// Dummy error helpers (the PSEUDOTYPE_DUMMY_* macro expansions).
//
// In C these are macro-generated `ereport(ERROR, (errcode(...), errmsg(...)))`
// calls that never return (pseudotypes.c:34-92). Two message variants (input
// vs. output) plus the shell-type specials. All carry
// ERRCODE_FEATURE_NOT_SUPPORTED.
// ===========================================================================

/// `PSEUDOTYPE_DUMMY_INPUT_FUNC` / `PSEUDOTYPE_DUMMY_RECEIVE_FUNC` body
/// (pseudotypes.c:34-43 / 68-77): `errmsg("cannot accept a value of type %s",
/// typname)`.
fn cannot_accept(typname: &str) -> PgError {
    PgError::error(alloc::format!("cannot accept a value of type {typname}"))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
}

/// Output half of `PSEUDOTYPE_DUMMY_IO_FUNCS` /
/// `PSEUDOTYPE_DUMMY_BINARY_IO_FUNCS` (pseudotypes.c:50-58 / 84-92):
/// `errmsg("cannot display a value of type %s", typname)`.
fn cannot_display(typname: &str) -> PgError {
    PgError::error(alloc::format!("cannot display a value of type {typname}"))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
}

// ===========================================================================
// cstring (pseudotypes.c:100-141)
//
// cstring is marked as a pseudo-type because we don't want people using it in
// tables. But it's really a perfectly functional type, so provide a full set
// of working I/O functions for it.
// ===========================================================================

/// `cstring_in` (pseudotypes.c:101): `PG_RETURN_CSTRING(pstrdup(str))`. Echoes
/// the input cstring as a fresh private copy in `mcx`.
pub fn cstring_in<'mcx>(mcx: Mcx<'mcx>, str: &str) -> PgResult<PgString<'mcx>> {
    PgString::from_str_in(str, mcx)
}

/// `cstring_out` (pseudotypes.c:110): `PG_RETURN_CSTRING(pstrdup(str))`. Echoes
/// the value cstring as a fresh private copy in `mcx`.
pub fn cstring_out<'mcx>(mcx: Mcx<'mcx>, str: &str) -> PgResult<PgString<'mcx>> {
    PgString::from_str_in(str, mcx)
}

/// `cstring_recv` (pseudotypes.c:119): read the remaining message text
/// (`buf->len - buf->cursor`) via `pq_getmsgtext` and return it as a `cstring`.
pub fn cstring_recv<'mcx>(mcx: Mcx<'mcx>, buf: &mut StringInfo<'_>) -> PgResult<PgVec<'mcx, u8>> {
    // str = pq_getmsgtext(buf, buf->len - buf->cursor, &nbytes);
    let remaining = buf.data.len().saturating_sub(buf.cursor);
    pq_getmsgtext(mcx, buf, remaining)
}

/// `cstring_send` (pseudotypes.c:130): `pq_begintypsend`, `pq_sendtext(str,
/// strlen(str))`, `PG_RETURN_BYTEA_P(pq_endtypsend(&buf))`.
pub fn cstring_send<'mcx>(mcx: Mcx<'mcx>, str: &str) -> PgResult<Bytea<'mcx>> {
    let mut buf = pq_begintypsend(mcx)?;
    backend_libpq_pqformat::pq_sendtext(&mut buf, str.as_bytes())?;
    Ok(pq_endtypsend(buf))
}

// ===========================================================================
// anyarray (pseudotypes.c:144-166)
//
// We need to allow output of anyarray so that, e.g., pg_statistic columns can
// be printed. Input has to be disallowed, however.
// ===========================================================================

/// `anyarray_in` (pseudotypes.c:154, PSEUDOTYPE_DUMMY_INPUT_FUNC): throws.
pub fn anyarray_in(_str: &str) -> PgResult<Datum> {
    Err(cannot_accept("anyarray"))
}

/// `anyarray_recv` (pseudotypes.c:155, PSEUDOTYPE_DUMMY_RECEIVE_FUNC): throws.
pub fn anyarray_recv(_buf: &mut StringInfo<'_>) -> PgResult<Datum> {
    Err(cannot_accept("anyarray"))
}

/// `anyarray_out` (pseudotypes.c:158): `return array_out(fcinfo)`.
pub fn anyarray_out<'mcx>(mcx: Mcx<'mcx>, array: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    backend_utils_adt_arrayfuncs::io::array_out(mcx, array)
}

/// `anyarray_send` (pseudotypes.c:164): `return array_send(fcinfo)`.
pub fn anyarray_send<'mcx>(mcx: Mcx<'mcx>, array: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    backend_utils_adt_arrayfuncs::io::array_send(mcx, array)
}

// ===========================================================================
// anycompatiblearray (pseudotypes.c:168-186)
//
// We may as well allow output, since we do for anyarray.
// ===========================================================================

/// `anycompatiblearray_in` (pseudotypes.c:174): throws.
pub fn anycompatiblearray_in(_str: &str) -> PgResult<Datum> {
    Err(cannot_accept("anycompatiblearray"))
}

/// `anycompatiblearray_recv` (pseudotypes.c:175): throws.
pub fn anycompatiblearray_recv(_buf: &mut StringInfo<'_>) -> PgResult<Datum> {
    Err(cannot_accept("anycompatiblearray"))
}

/// `anycompatiblearray_out` (pseudotypes.c:178): `return array_out(fcinfo)`.
pub fn anycompatiblearray_out<'mcx>(mcx: Mcx<'mcx>, array: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    backend_utils_adt_arrayfuncs::io::array_out(mcx, array)
}

/// `anycompatiblearray_send` (pseudotypes.c:184): `return array_send(fcinfo)`.
pub fn anycompatiblearray_send<'mcx>(mcx: Mcx<'mcx>, array: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    backend_utils_adt_arrayfuncs::io::array_send(mcx, array)
}

// ===========================================================================
// anyenum (pseudotypes.c:189-198)
//
// We may as well allow output, since enum_out will in fact work.
// ===========================================================================

/// `anyenum_in` (pseudotypes.c:194): throws.
pub fn anyenum_in(_str: &str) -> PgResult<Datum> {
    Err(cannot_accept("anyenum"))
}

/// `anyenum_out` (pseudotypes.c:197): `return enum_out(fcinfo)`.
///
/// An `anyenum` value is a by-value enum OID; it crosses to the (unported)
/// `enum.c` owner as that `Oid` via the scalar seam crate.
pub fn anyenum_out<'mcx>(mcx: Mcx<'mcx>, enumval: Datum) -> PgResult<PgString<'mcx>> {
    backend_utils_adt_scalar_seams::enum_out::call(mcx, enumval.as_oid())
}

// ===========================================================================
// anyrange (pseudotypes.c:201-210)
//
// We may as well allow output, since range_out will in fact work.
// ===========================================================================

/// `anyrange_in` (pseudotypes.c:207): throws.
pub fn anyrange_in(_str: &str) -> PgResult<Datum> {
    Err(cannot_accept("anyrange"))
}

/// `anyrange_out` (pseudotypes.c:210): `return range_out(fcinfo)`.
pub fn anyrange_out(range: RangeTypeP<'_>) -> PgResult<alloc::string::String> {
    backend_utils_adt_rangetypes_seams::range_out::call(range)
}

// ===========================================================================
// anycompatiblerange (pseudotypes.c:214-223)
//
// We may as well allow output, since range_out will in fact work.
// ===========================================================================

/// `anycompatiblerange_in` (pseudotypes.c:220): throws.
pub fn anycompatiblerange_in(_str: &str) -> PgResult<Datum> {
    Err(cannot_accept("anycompatiblerange"))
}

/// `anycompatiblerange_out` (pseudotypes.c:223): `return range_out(fcinfo)`.
pub fn anycompatiblerange_out(range: RangeTypeP<'_>) -> PgResult<alloc::string::String> {
    backend_utils_adt_rangetypes_seams::range_out::call(range)
}

// ===========================================================================
// anymultirange (pseudotypes.c:227-236)
//
// We may as well allow output, since multirange_out will in fact work.
// ===========================================================================

/// `anymultirange_in` (pseudotypes.c:233): throws.
pub fn anymultirange_in(_str: &str) -> PgResult<Datum> {
    Err(cannot_accept("anymultirange"))
}

/// `anymultirange_out` (pseudotypes.c:236): `return multirange_out(fcinfo)`.
pub fn anymultirange_out(mcx: Mcx<'_>, multirange: Datum) -> PgResult<alloc::string::String> {
    backend_utils_adt_multirangetypes::typcache_io::multirange_out(mcx, multirange)
}

// ===========================================================================
// anycompatiblemultirange (pseudotypes.c:240-249)
//
// We may as well allow output, since multirange_out will in fact work.
// ===========================================================================

/// `anycompatiblemultirange_in` (pseudotypes.c:246): throws.
pub fn anycompatiblemultirange_in(_str: &str) -> PgResult<Datum> {
    Err(cannot_accept("anycompatiblemultirange"))
}

/// `anycompatiblemultirange_out` (pseudotypes.c:249): `return
/// multirange_out(fcinfo)`.
pub fn anycompatiblemultirange_out(mcx: Mcx<'_>, multirange: Datum) -> PgResult<alloc::string::String> {
    backend_utils_adt_multirangetypes::typcache_io::multirange_out(mcx, multirange)
}

// ===========================================================================
// void (pseudotypes.c:252-289)
//
// We support void_in so that PL functions can return VOID without any special
// hack in the PL handler. void_out and void_send are needed so that "SELECT
// function_returning_void(...)" works.
// ===========================================================================

/// `void_in` (pseudotypes.c:263): `PG_RETURN_VOID()` — accepts anything,
/// returns nothing (a 0-width pass-by-value `void`).
pub fn void_in(_str: &str) -> PgResult<Datum> {
    Ok(Datum::null())
}

/// `void_out` (pseudotypes.c:269): `PG_RETURN_CSTRING(pstrdup(""))` — empty
/// cstring.
pub fn void_out<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgString<'mcx>> {
    PgString::from_str_in("", mcx)
}

/// `void_recv` (pseudotypes.c:275): `PG_RETURN_VOID()`. Consumes no bytes, so
/// anything but an empty message yields "invalid message format" downstream.
pub fn void_recv(_buf: &mut StringInfo<'_>) -> PgResult<Datum> {
    Ok(Datum::null())
}

/// `void_send` (pseudotypes.c:285): begin an empty typsend buffer and finish
/// it, i.e. send an empty string. `PG_RETURN_BYTEA_P(pq_endtypsend(&buf))`.
pub fn void_send<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Bytea<'mcx>> {
    // send an empty string
    let buf = pq_begintypsend(mcx)?;
    Ok(pq_endtypsend(buf))
}

// ===========================================================================
// shell (pseudotypes.c:292-317)
//
// shell_in and shell_out are entered in pg_type for "shell" types (those not
// yet filled in). They should be unreachable, but we set them up just in case.
// ===========================================================================

/// `shell_in` (pseudotypes.c:303): `errmsg("cannot accept a value of a shell
/// type")`.
pub fn shell_in(_str: &str) -> PgResult<Datum> {
    Err(PgError::error("cannot accept a value of a shell type")
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED))
}

/// `shell_out` (pseudotypes.c:313): `errmsg("cannot display a value of a shell
/// type")`.
pub fn shell_out(_value: Datum) -> PgResult<PgString<'static>> {
    Err(PgError::error("cannot display a value of a shell type")
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED))
}

// ===========================================================================
// pg_node_tree (pseudotypes.c:320-345)
//
// Not really a pseudotype --- it's real enough to be a table column --- but it
// presently has no operations of its own and disallows input too. We disallow
// input (the SQL functions on the type are not secure against malformed input)
// but allow output.
// ===========================================================================

/// `pg_node_tree_in` (pseudotypes.c:334): throws.
pub fn pg_node_tree_in(_str: &str) -> PgResult<Datum> {
    Err(cannot_accept("pg_node_tree"))
}

/// `pg_node_tree_recv` (pseudotypes.c:335): throws.
pub fn pg_node_tree_recv(_buf: &mut StringInfo<'_>) -> PgResult<Datum> {
    Err(cannot_accept("pg_node_tree"))
}

/// `pg_node_tree_out` (pseudotypes.c:338): `return textout(fcinfo)`.
pub fn pg_node_tree_out<'mcx>(mcx: Mcx<'mcx>, t: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    backend_utils_adt_varlena::wire_io::textout(mcx, t)
}

/// `pg_node_tree_send` (pseudotypes.c:344): `return textsend(fcinfo)`.
pub fn pg_node_tree_send<'mcx>(mcx: Mcx<'mcx>, t: &[u8]) -> PgResult<Bytea<'mcx>> {
    backend_utils_adt_varlena::wire_io::textsend(mcx, t)
}

// ===========================================================================
// pg_ddl_command (pseudotypes.c:348-360)
//
// Like pg_node_tree, pg_ddl_command isn't really a pseudotype. We have no good
// way to output it directly, so punt for output as well as input (and for
// binary I/O).
// ===========================================================================

/// `pg_ddl_command_in` (pseudotypes.c:359, PSEUDOTYPE_DUMMY_IO_FUNCS): throws.
pub fn pg_ddl_command_in(_str: &str) -> PgResult<Datum> {
    Err(cannot_accept("pg_ddl_command"))
}

/// `pg_ddl_command_out` (pseudotypes.c:359, PSEUDOTYPE_DUMMY_IO_FUNCS): throws.
pub fn pg_ddl_command_out(_value: Datum) -> PgResult<PgString<'static>> {
    Err(cannot_display("pg_ddl_command"))
}

/// `pg_ddl_command_recv` (pseudotypes.c:359, PSEUDOTYPE_DUMMY_BINARY_IO_FUNCS):
/// throws.
pub fn pg_ddl_command_recv(_buf: &mut StringInfo<'_>) -> PgResult<Datum> {
    Err(cannot_accept("pg_ddl_command"))
}

/// `pg_ddl_command_send` (pseudotypes.c:359, PSEUDOTYPE_DUMMY_BINARY_IO_FUNCS):
/// throws.
pub fn pg_ddl_command_send(_value: Datum) -> PgResult<Bytea<'static>> {
    Err(cannot_display("pg_ddl_command"))
}

// ===========================================================================
// Dummy I/O functions for various other pseudotypes
// (PSEUDOTYPE_DUMMY_IO_FUNCS, pseudotypes.c:363-377).
// ===========================================================================

macro_rules! dummy_io {
    ($typname:literal, $in_fn:ident, $out_fn:ident) => {
        #[doc = concat!("`", $typname, "_in` (pseudotypes.c:363-377): throws.")]
        pub fn $in_fn(_str: &str) -> PgResult<Datum> {
            Err(cannot_accept($typname))
        }

        #[doc = concat!("`", $typname, "_out` (pseudotypes.c:363-377): throws.")]
        pub fn $out_fn(_value: Datum) -> PgResult<PgString<'static>> {
            Err(cannot_display($typname))
        }
    };
}

dummy_io!("any", any_in, any_out);
dummy_io!("trigger", trigger_in, trigger_out);
dummy_io!("event_trigger", event_trigger_in, event_trigger_out);
dummy_io!("language_handler", language_handler_in, language_handler_out);
dummy_io!("fdw_handler", fdw_handler_in, fdw_handler_out);
dummy_io!("table_am_handler", table_am_handler_in, table_am_handler_out);
dummy_io!("index_am_handler", index_am_handler_in, index_am_handler_out);
dummy_io!("tsm_handler", tsm_handler_in, tsm_handler_out);
dummy_io!("internal", internal_in, internal_out);
dummy_io!("anyelement", anyelement_in, anyelement_out);
dummy_io!("anynonarray", anynonarray_in, anynonarray_out);
dummy_io!("anycompatible", anycompatible_in, anycompatible_out);
dummy_io!("anycompatiblenonarray", anycompatiblenonarray_in, anycompatiblenonarray_out);

/// This unit has no inbound cyclic callers, so it owns no seam crate and
/// installs no seams. The empty `init_seams()` keeps the uniform
/// `seams-init::init_all()` wiring.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
