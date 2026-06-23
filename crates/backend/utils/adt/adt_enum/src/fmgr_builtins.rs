//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! functions in `enum.c`. Each entry is a `fc_<name>` adapter that reads its
//! arguments off the fmgr call frame, calls the matching value core in [`crate`],
//! and writes back the result word / by-reference payload.
//! [`register_enum_builtins`] registers every row into the fmgr-core builtin
//! table (C: `fmgr_builtins[]`), so by-OID dispatch resolves them. OIDs / nargs /
//! strict / retset are transcribed exactly from `fmgrtab.c` / `pg_proc.dat`.
//!
//! # Threaded state at the registry boundary
//!
//!  * An enum value is a 4-byte pass-by-value type, so its `Datum` word is the
//!    value's OID (`PG_GETARG_OID` / `PG_RETURN_OID`).
//!  * `enum_in`/`enum_recv` take the enum type OID in their second argument (C's
//!    `PG_GETARG_OID(1)`); `enum_first`/`enum_last`/`enum_range` read it off the
//!    calling expression tree via `get_fn_expr_argtype(flinfo, 0)`, the
//!    polymorphic-resolution path (the argument value itself is never examined).
//!  * `transaction_xmin` is C's `TransactionXmin` global, read live off the
//!    active snapshot (`snapmgr::TransactionXmin()`) and threaded into the cores
//!    per the no-ambient-global rule.
//!  * `enum_in`'s soft-error context (`escontext = fcinfo->context`) is not
//!    carried on a bare registry call frame, so this layer takes the hard-error
//!    path (`None`), mirroring `numeric_in`'s registry adapter.
//!
//! `cstring` inputs/outputs cross on the by-ref `Cstring` lane; `enum_send`'s
//! `bytea` body and `enum_range`'s array image cross on the by-ref `Varlena`
//! lane.

use ::mcx::MemoryContext;
use ::datum::Datum;
use ::fmgr::boundary::RefPayload;
use ::fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};
use ::types_core::primitive::Oid;
use ::types_core::TransactionId;

use snapmgr as snapmgr;
use fmgr_core as fmgr_core;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_OID(i)`: read a by-value OID argument (an enum value's `Datum`).
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo.arg(i).expect("enum fn: missing arg").value.as_oid()
}

/// `PG_GETARG_CSTRING(i)`: the input cstring on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("enum fn: cstring arg missing from by-ref lane")
}

/// `PG_GETARG_POINTER(0)` as a `StringInfo` wire buffer: the `recv` byte image
/// on the by-ref lane.
#[inline]
fn arg_recv_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("enum fn: recv buffer arg missing from by-ref lane")
}

/// `get_fn_expr_argtype(fcinfo->flinfo, 0)`: the actual enum type OID resolved
/// from the calling expression tree (the polymorphic-resolution path).
#[inline]
fn fn_expr_argtype0(fcinfo: &FunctionCallInfoBaseData) -> Oid {
    fmgr_core::get_fn_expr_argtype(fcinfo.flinfo.as_deref(), 0)
}

/// `TransactionXmin` (snapmgr.c:158) read off the active snapshot.
#[inline]
fn transaction_xmin() -> TransactionId {
    snapmgr::TransactionXmin()
}

#[inline]
fn ret_oid(v: Oid) -> Datum {
    Datum::from_oid(v)
}
#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}
#[inline]
fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
}

/// Set a `cstring` (`_out`) result on the by-ref lane and return the dummy word.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}

/// Set an array (by-reference) result on the by-ref lane. The `enum_range`
/// array image is already a header-ful varlena (`arrayfuncs` stamps the 4-byte
/// length word when it constructs the array), so it crosses verbatim under the
/// header-ful-everywhere convention.
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}

/// Set a `bytea` (by-reference) `_send` result on the by-ref lane. The
/// `enum_send` core builds the bare wire payload; under the header-ful-
/// everywhere convention a `bytea` value is the full varlena image, so this
/// stamps the 4-byte uncompressed length word in front (`SET_VARSIZE`). The
/// wire layer strips that header downstream.
#[inline]
fn ret_send(fcinfo: &mut FunctionCallInfoBaseData, payload: Vec<u8>) -> Datum {
    const VARHDRSZ: usize = 4;
    let mut image = Vec::with_capacity(payload.len() + VARHDRSZ);
    image.extend_from_slice(&::datum::varlena::set_varsize_4b(payload.len() + VARHDRSZ));
    image.extend_from_slice(&payload);
    fcinfo.set_ref_result(RefPayload::Varlena(image));
    Datum::from_usize(0)
}

/// A scratch / result context for the enum cores that allocate through `Mcx`
/// (the `enum_send` body, the `enum_range` array image, the ordered scan). The
/// result bytes are copied off the by-ref lane before it is dropped (C: the
/// palloc'd result lives in the caller's context; here it crosses by value).
fn scratch_mcx() -> MemoryContext {
    MemoryContext::new("enum fmgr scratch")
}

// ---------------------------------------------------------------------------
// fc_ adapters — I/O.
// ---------------------------------------------------------------------------

/// `enum_in(cstring, oid) -> anyenum` (oid 3506). C: `escontext =
/// fcinfo->context`. Forward the soft ErrorSaveContext installed on the frame by
/// InputFunctionCallSafe so an unrecognized label `ereturn`s into the sink
/// (returning `Ok(None)`) instead of throwing past `invoke?`.
fn fc_enum_in(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let name = arg_cstring(fcinfo, 0).to_string();
    let enumtypoid = arg_oid(fcinfo, 1);
    let xmin = transaction_xmin();
    match crate::enum_in(&name, enumtypoid, xmin, fcinfo.escontext_mut())? {
        Some(oid) => Ok(ret_oid(oid)),
        // Soft-error path returned `(Datum) 0` after `ereturn` recorded the
        // failure into the sink; surface a NULL placeholder the caller discards
        // after `soft_error_occurred()`.
        None => {
            fcinfo.set_result_null(true);
            Ok(Datum::from_usize(0))
        }
    }
}

/// `enum_out(anyenum) -> cstring` (oid 3507).
fn fc_enum_out(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let val = arg_oid(fcinfo, 0);
    let s = crate::enum_out(val)?;
    Ok(ret_cstring(fcinfo, s))
}

/// `enum_recv(internal, oid) -> anyenum` (oid 3532).
fn fc_enum_recv(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let src = arg_recv_bytes(fcinfo, 0).to_vec();
    let enumtypoid = arg_oid(fcinfo, 1);
    let xmin = transaction_xmin();
    let m = scratch_mcx();
    let mut data = ::mcx::PgVec::new_in(m.mcx());
    data.extend_from_slice(&src);
    let mut buf = stringinfo::StringInfo::from_vec(data);
    let oid = crate::enum_recv(&mut buf, enumtypoid, xmin)?;
    Ok(ret_oid(oid))
}

/// `enum_send(anyenum) -> bytea` (oid 3533).
fn fc_enum_send(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let val = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    let bytes = crate::enum_send(m.mcx(), val)?;
    Ok(ret_send(fcinfo, bytes.as_slice().to_vec()))
}

// ---------------------------------------------------------------------------
// fc_ adapters — comparison operators.
// ---------------------------------------------------------------------------

macro_rules! fc_cmp_bool {
    ($fc:ident, $core:path) => {
        fn $fc(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
            let a = arg_oid(fcinfo, 0);
            let b = arg_oid(fcinfo, 1);
            Ok(ret_bool($core(a, b)?))
        }
    };
}
fc_cmp_bool!(fc_enum_lt, crate::enum_lt);
fc_cmp_bool!(fc_enum_le, crate::enum_le);
fc_cmp_bool!(fc_enum_gt, crate::enum_gt);
fc_cmp_bool!(fc_enum_ge, crate::enum_ge);

/// `enum_eq(anyenum, anyenum) -> bool` (oid 3508) — OID equality, no catalog.
fn fc_enum_eq(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::enum_eq(arg_oid(fcinfo, 0), arg_oid(fcinfo, 1))))
}

/// `enum_ne(anyenum, anyenum) -> bool` (oid 3509) — OID inequality, no catalog.
fn fc_enum_ne(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::enum_ne(arg_oid(fcinfo, 0), arg_oid(fcinfo, 1))))
}

/// `enum_cmp(anyenum, anyenum) -> int4` (oid 3514).
fn fc_enum_cmp(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_oid(fcinfo, 0);
    let b = arg_oid(fcinfo, 1);
    Ok(ret_i32(crate::enum_cmp(a, b)?))
}

/// `enum_smaller(anyenum, anyenum) -> anyenum` (oid 3524).
fn fc_enum_smaller(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_oid(fcinfo, 0);
    let b = arg_oid(fcinfo, 1);
    Ok(ret_oid(crate::enum_smaller(a, b)?))
}

/// `enum_larger(anyenum, anyenum) -> anyenum` (oid 3525).
fn fc_enum_larger(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let a = arg_oid(fcinfo, 0);
    let b = arg_oid(fcinfo, 1);
    Ok(ret_oid(crate::enum_larger(a, b)?))
}

// ---------------------------------------------------------------------------
// fc_ adapters — programming support (enum_first / enum_last / enum_range).
// ---------------------------------------------------------------------------

/// `enum_first(anyenum) -> anyenum` (oid 3528, NOT strict — the arg may be NULL;
/// the type is taken from the call expression, not the value).
fn fc_enum_first(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let enumtypoid = fn_expr_argtype0(fcinfo);
    let xmin = transaction_xmin();
    let m = scratch_mcx();
    Ok(ret_oid(crate::enum_first(m.mcx(), enumtypoid, xmin)?))
}

/// `enum_last(anyenum) -> anyenum` (oid 3529, NOT strict).
fn fc_enum_last(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let enumtypoid = fn_expr_argtype0(fcinfo);
    let xmin = transaction_xmin();
    let m = scratch_mcx();
    Ok(ret_oid(crate::enum_last(m.mcx(), enumtypoid, xmin)?))
}

/// `enum_range(anyenum, anyenum) -> anyarray` (oid 3530, NOT strict): every
/// member from `lower` to `upper` inclusive; a NULL bound is open-ended.
fn fc_enum_range_bounds(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let lower = fcinfo
        .arg(0)
        .filter(|d| !d.isnull)
        .map(|d| d.value.as_oid());
    let upper = fcinfo
        .arg(1)
        .filter(|d| !d.isnull)
        .map(|d| d.value.as_oid());
    let enumtypoid = fn_expr_argtype0(fcinfo);
    let xmin = transaction_xmin();
    let m = scratch_mcx();
    let image = crate::enum_range_bounds(m.mcx(), lower, upper, enumtypoid, xmin)?;
    Ok(ret_varlena(fcinfo, image.as_slice().to_vec()))
}

/// `enum_range(anyenum) -> anyarray` (oid 3531, NOT strict): every member.
fn fc_enum_range_all(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let enumtypoid = fn_expr_argtype0(fcinfo);
    let xmin = transaction_xmin();
    let m = scratch_mcx();
    let image = crate::enum_range_all(m.mcx(), enumtypoid, xmin)?;
    Ok(ret_varlena(fcinfo, image.as_slice().to_vec()))
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
    native: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs,
            strict,
            retset,
            func: None,
        },
        native,
    )
}

/// Register every `enum.c` builtin into the fmgr-core builtin table (C:
/// `fmgr_builtins[]`), so by-OID dispatch resolves them. Called from this
/// crate's `init_seams()`. OIDs/nargs/strict/retset transcribed from
/// `fmgrtab.c`.
pub fn register_enum_builtins() {
    fmgr_core::register_builtins_native([
        // ---- basic + binary I/O ----
        builtin(3506, "enum_in", 2, true, false, fc_enum_in),
        builtin(3507, "enum_out", 1, true, false, fc_enum_out),
        builtin(3532, "enum_recv", 2, true, false, fc_enum_recv),
        builtin(3533, "enum_send", 1, true, false, fc_enum_send),
        // ---- comparison operators ----
        builtin(3508, "enum_eq", 2, true, false, fc_enum_eq),
        builtin(3509, "enum_ne", 2, true, false, fc_enum_ne),
        builtin(3510, "enum_lt", 2, true, false, fc_enum_lt),
        builtin(3511, "enum_gt", 2, true, false, fc_enum_gt),
        builtin(3512, "enum_le", 2, true, false, fc_enum_le),
        builtin(3513, "enum_ge", 2, true, false, fc_enum_ge),
        builtin(3514, "enum_cmp", 2, true, false, fc_enum_cmp),
        builtin(3524, "enum_smaller", 2, true, false, fc_enum_smaller),
        builtin(3525, "enum_larger", 2, true, false, fc_enum_larger),
        // ---- programming support ----
        builtin(3528, "enum_first", 1, false, false, fc_enum_first),
        builtin(3529, "enum_last", 1, false, false, fc_enum_last),
        builtin(3530, "enum_range_bounds", 2, false, false, fc_enum_range_bounds),
        builtin(3531, "enum_range_all", 1, false, false, fc_enum_range_all),
    ]);
}
