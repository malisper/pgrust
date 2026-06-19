//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for every SQL-callable
//! function in `xid.c` (the scalar `xid` / `xid8` / `cid` I/O, comparison,
//! hashing and min/max helpers).
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word /
//! by-reference payload. [`register_xid_builtins`] / [`register_cid_builtins`]
//! register every row into the fmgr-core builtin table (C: `fmgr_builtins[]`),
//! so by-OID dispatch (and the `fmgr_isbuiltin` fast path) resolves them. OIDs /
//! nargs / strict / retset are transcribed exactly from `pg_proc.dat`.
//!
//! `xid` and `cid` are 32-bit pass-by-value types (the `Datum` word holds the
//! `u32`). `xid8` (`FullTransactionId`) is a 64-bit pass-by-value type; its
//! 8-byte `value` rides in the full-width `Datum` word, exactly as C's
//! `FullTransactionIdGetDatum` / `DatumGetFullTransactionId` reinterpret the
//! `Datum` on a 64-bit build.
//!
//! `xideqint4` (OID 1319) and `xidneqint4` (OID 3309) share the `xideq` /
//! `xidneq` C bodies (`prosrc => 'xideq'` / `'xidneq'`): the `int4` argument is
//! read as a 32-bit word and compared as a `TransactionId`, so they register the
//! same adapters under their own OIDs.

use types_core::{CommandId, FullTransactionId, TransactionId};
use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};
use types_stringinfo::StringInfo;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_TRANSACTIONID(i)` / `PG_GETARG_INT32(i)` (for the `int4` operand
/// of `xideqint4`): the low 32 bits of arg `i`'s word.
#[inline]
fn arg_xid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> TransactionId {
    fcinfo.arg(i).expect("xid fn: missing arg").value.as_u32() as TransactionId
}

/// `PG_GETARG_COMMANDID(i)` → `DatumGetCommandId`: the low 32 bits.
#[inline]
fn arg_cid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> CommandId {
    fcinfo.arg(i).expect("xid fn: missing arg").value.as_u32() as CommandId
}

/// `PG_GETARG_FULLTRANSACTIONID(i)` → `DatumGetFullTransactionId`: the full
/// 64-bit `Datum` word reinterpreted as the `xid8` value.
#[inline]
fn arg_fxid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> FullTransactionId {
    FullTransactionId {
        value: fcinfo.arg(i).expect("xid fn: missing arg").value.as_u64(),
    }
}

/// `PG_GETARG_INT64(i)`: the seed of the `*extended` hash functions.
#[inline]
fn arg_i64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("xid fn: missing arg").value.as_i64()
}

/// `PG_GETARG_CSTRING(i)`: the input text on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("xid fn: cstring arg missing from by-ref lane")
}

/// `PG_GETARG_POINTER(i)` for a `StringInfo` (a `*recv` wire buffer): the raw
/// message bytes on the by-ref lane.
#[inline]
fn arg_varlena<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("xid fn: by-ref wire buffer missing from by-ref lane")
}

#[inline]
fn ret_xid(v: TransactionId) -> Datum {
    Datum::from_u32(v)
}
#[inline]
fn ret_cid(v: CommandId) -> Datum {
    Datum::from_u32(v)
}
#[inline]
fn ret_fxid(v: FullTransactionId) -> Datum {
    Datum::from_u64(v.value)
}
#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}
#[inline]
fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
}
#[inline]
fn ret_u32(v: u32) -> Datum {
    Datum::from_u32(v)
}
#[inline]
fn ret_u64(v: u64) -> Datum {
    Datum::from_u64(v)
}

/// Set a `cstring` (`*out`) result on the by-ref lane and return the dummy word.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}
/// Set a `bytea` (`*send`) result on the by-ref lane.
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}

/// A scratch context for cores that allocate their result through `Mcx`.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("xid fmgr scratch")
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    std::panic::panic_any(err);
}

/// Build a [`StringInfo`] over the inbound wire bytes for a `*recv` body.
fn recv_buf<'mcx>(mcx: mcx::Mcx<'mcx>, src: &[u8]) -> StringInfo<'mcx> {
    let mut data = mcx::PgVec::new_in(mcx);
    if data.try_reserve(src.len()).is_err() {
        raise(types_error::PgError::error("out of memory"));
    }
    data.extend_from_slice(src);
    StringInfo::from_vec(data)
}

// ---------------------------------------------------------------------------
// xid fc_ adapters.
// ---------------------------------------------------------------------------

fn fc_xidin(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // C: uint32in_subr(s, NULL, "xid", fcinfo->context). A soft context is not
    // modeled on the fmgr frame; a hard parse is used (matching every other adt
    // _in). InputFunctionCallSafe catches the hard error and records it into the
    // real escontext, so pg_input_is_valid still observes a soft failure.
    match crate::xidin(arg_cstring(fcinfo, 0), None) {
        Ok(v) => ret_xid(v),
        Err(e) => raise(e),
    }
}

fn fc_xidout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = crate::xidout(arg_xid(fcinfo, 0));
    ret_cstring(fcinfo, s)
}

fn fc_xidrecv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let mut buf = recv_buf(m.mcx(), arg_varlena(fcinfo, 0));
    match crate::xidrecv(&mut buf) {
        Ok(v) => ret_xid(v),
        Err(e) => raise(e),
    }
}

fn fc_xidsend(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = arg_xid(fcinfo, 0);
    let m = scratch_mcx();
    let bytes = match crate::xidsend(m.mcx(), arg1) {
        Ok(bytea) => bytea.as_bytes().to_vec(),
        Err(e) => raise(e),
    };
    ret_varlena(fcinfo, bytes)
}

fn fc_xideq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::xideq(arg_xid(fcinfo, 0), arg_xid(fcinfo, 1)))
}
fn fc_xidneq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::xidneq(arg_xid(fcinfo, 0), arg_xid(fcinfo, 1)))
}

fn fc_hashxid(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_u32(crate::hashxid(arg_xid(fcinfo, 0)))
}
fn fc_hashxidextended(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_u64(crate::hashxidextended(arg_xid(fcinfo, 0), arg_i64(fcinfo, 1) as u64))
}

fn fc_xid_age(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::xid_age(arg_xid(fcinfo, 0)) {
        Ok(v) => ret_i32(v),
        Err(e) => raise(e),
    }
}
fn fc_mxid_age(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::mxid_age(arg_xid(fcinfo, 0)) {
        Ok(v) => ret_i32(v),
        Err(e) => raise(e),
    }
}

// ---------------------------------------------------------------------------
// xid8 fc_ adapters.
// ---------------------------------------------------------------------------

fn fc_xid8in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::xid8in(arg_cstring(fcinfo, 0), None) {
        Ok(v) => ret_fxid(v),
        Err(e) => raise(e),
    }
}

fn fc_xid8out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = crate::xid8out(arg_fxid(fcinfo, 0));
    ret_cstring(fcinfo, s)
}

fn fc_xid8recv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let mut buf = recv_buf(m.mcx(), arg_varlena(fcinfo, 0));
    match crate::xid8recv(&mut buf) {
        Ok(v) => ret_fxid(v),
        Err(e) => raise(e),
    }
}

fn fc_xid8send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = arg_fxid(fcinfo, 0);
    let m = scratch_mcx();
    let bytes = match crate::xid8send(m.mcx(), arg1) {
        Ok(bytea) => bytea.as_bytes().to_vec(),
        Err(e) => raise(e),
    };
    ret_varlena(fcinfo, bytes)
}

fn fc_xid8toxid(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_xid(crate::xid8toxid(arg_fxid(fcinfo, 0)))
}

fn fc_xid8eq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::xid8eq(arg_fxid(fcinfo, 0), arg_fxid(fcinfo, 1)))
}
fn fc_xid8ne(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::xid8ne(arg_fxid(fcinfo, 0), arg_fxid(fcinfo, 1)))
}
fn fc_xid8lt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::xid8lt(arg_fxid(fcinfo, 0), arg_fxid(fcinfo, 1)))
}
fn fc_xid8gt(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::xid8gt(arg_fxid(fcinfo, 0), arg_fxid(fcinfo, 1)))
}
fn fc_xid8le(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::xid8le(arg_fxid(fcinfo, 0), arg_fxid(fcinfo, 1)))
}
fn fc_xid8ge(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::xid8ge(arg_fxid(fcinfo, 0), arg_fxid(fcinfo, 1)))
}
fn fc_xid8cmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_i32(crate::xid8cmp(arg_fxid(fcinfo, 0), arg_fxid(fcinfo, 1)))
}
fn fc_xid8_larger(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_fxid(crate::xid8_larger(arg_fxid(fcinfo, 0), arg_fxid(fcinfo, 1)))
}
fn fc_xid8_smaller(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_fxid(crate::xid8_smaller(arg_fxid(fcinfo, 0), arg_fxid(fcinfo, 1)))
}
fn fc_hashxid8(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_u32(crate::hashxid8(arg_fxid(fcinfo, 0)))
}
fn fc_hashxid8extended(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_u64(crate::hashxid8extended(arg_fxid(fcinfo, 0), arg_i64(fcinfo, 1) as u64))
}

// ---------------------------------------------------------------------------
// cid fc_ adapters.
// ---------------------------------------------------------------------------

fn fc_cidin(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::cidin(arg_cstring(fcinfo, 0), None) {
        Ok(v) => ret_cid(v),
        Err(e) => raise(e),
    }
}

fn fc_cidout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = crate::cidout(arg_cid(fcinfo, 0));
    ret_cstring(fcinfo, s)
}

fn fc_cidrecv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let mut buf = recv_buf(m.mcx(), arg_varlena(fcinfo, 0));
    match crate::cidrecv(&mut buf) {
        Ok(v) => ret_cid(v),
        Err(e) => raise(e),
    }
}

fn fc_cidsend(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = arg_cid(fcinfo, 0);
    let m = scratch_mcx();
    let bytes = match crate::cidsend(m.mcx(), arg1) {
        Ok(bytea) => bytea.as_bytes().to_vec(),
        Err(e) => raise(e),
    };
    ret_varlena(fcinfo, bytes)
}

fn fc_cideq(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_bool(crate::cideq(arg_cid(fcinfo, 0), arg_cid(fcinfo, 1)))
}
fn fc_hashcid(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_u32(crate::hashcid(arg_cid(fcinfo, 0)))
}
fn fc_hashcidextended(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_u64(crate::hashcidextended(arg_cid(fcinfo, 0), arg_i64(fcinfo, 1) as u64))
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    strict: bool,
    retset: bool,
    func: fn(&mut FunctionCallInfoBaseData) -> Datum,
) -> BuiltinFunction {
    BuiltinFunction {
        foid,
        name: name.to_string(),
        nargs,
        strict,
        retset,
        func: Some(func),
    }
}

/// Register the `xid` / `xid8` fmgr builtins (C: their `fmgr_builtins[]` rows).
/// OIDs / nargs / strict / retset transcribed from `pg_proc.dat`.
pub fn register_xid_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // ---- xid I/O ----
        builtin(50, "xidin", 1, true, false, fc_xidin),
        builtin(51, "xidout", 1, true, false, fc_xidout),
        builtin(2440, "xidrecv", 1, true, false, fc_xidrecv),
        builtin(2441, "xidsend", 1, true, false, fc_xidsend),
        // ---- xid comparison ----
        builtin(68, "xideq", 2, true, false, fc_xideq),
        builtin(3308, "xidneq", 2, true, false, fc_xidneq),
        // xideqint4 / xidneqint4 share xideq / xidneq (prosrc => 'xideq'/'xidneq').
        // canonical registry keys these on the prosrc symbol (xideq / xidneq).
        builtin(1319, "xideq", 2, true, false, fc_xideq),
        builtin(3309, "xidneq", 2, true, false, fc_xidneq),
        // ---- xid hashing ----
        builtin(6419, "hashxid", 1, true, false, fc_hashxid),
        builtin(6420, "hashxidextended", 2, true, false, fc_hashxidextended),
        // ---- xid age (age(xid) / mxid_age) ----
        builtin(1181, "xid_age", 1, true, false, fc_xid_age),
        builtin(3939, "mxid_age", 1, true, false, fc_mxid_age),
        // ---- xid8 I/O ----
        builtin(5070, "xid8in", 1, true, false, fc_xid8in),
        builtin(5081, "xid8out", 1, true, false, fc_xid8out),
        builtin(5082, "xid8recv", 1, true, false, fc_xid8recv),
        builtin(5083, "xid8send", 1, true, false, fc_xid8send),
        builtin(5071, "xid8toxid", 1, true, false, fc_xid8toxid),
        // ---- xid8 comparison ----
        builtin(5084, "xid8eq", 2, true, false, fc_xid8eq),
        builtin(5085, "xid8ne", 2, true, false, fc_xid8ne),
        builtin(5034, "xid8lt", 2, true, false, fc_xid8lt),
        builtin(5035, "xid8gt", 2, true, false, fc_xid8gt),
        builtin(5036, "xid8le", 2, true, false, fc_xid8le),
        builtin(5037, "xid8ge", 2, true, false, fc_xid8ge),
        builtin(5096, "xid8cmp", 2, true, false, fc_xid8cmp),
        builtin(5097, "xid8_larger", 2, true, false, fc_xid8_larger),
        builtin(5098, "xid8_smaller", 2, true, false, fc_xid8_smaller),
        // ---- xid8 hashing ----
        builtin(6421, "hashxid8", 1, true, false, fc_hashxid8),
        builtin(6422, "hashxid8extended", 2, true, false, fc_hashxid8extended),
    ]);
}

/// Register the `cid` fmgr builtins (C: their `fmgr_builtins[]` rows). Same
/// contract as [`register_xid_builtins`].
pub fn register_cid_builtins() {
    backend_utils_fmgr_core::register_builtins([
        builtin(52, "cidin", 1, true, false, fc_cidin),
        builtin(53, "cidout", 1, true, false, fc_cidout),
        builtin(2442, "cidrecv", 1, true, false, fc_cidrecv),
        builtin(2443, "cidsend", 1, true, false, fc_cidsend),
        builtin(69, "cideq", 2, true, false, fc_cideq),
        builtin(6423, "hashcid", 1, true, false, fc_hashcid),
        builtin(6424, "hashcidextended", 2, true, false, fc_hashcidextended),
    ]);
}
