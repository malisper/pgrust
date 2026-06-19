//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! functions of this unit (`regproc.c`, `genfile.c`, `lockfuncs.c`,
//! `partitionfuncs.c`, `pg_upgrade_support.c`) whose argument / result types
//! are expressible at the current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word /
//! by-reference payload. [`register_misc2_builtins`] registers every row into
//! the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch (and
//! the `fmgr_isbuiltin` fast path) resolves them. OIDs / nargs / strict /
//! retset are transcribed exactly from `pg_proc.dat`.
//!
//! NOT registered here (genuinely not expressible at the current boundary, so
//! skipped per the discipline rather than hollow-stubbed):
//! * the `windowfuncs.c` window functions (`row_number`/`rank`/`dense_rank`/
//!   `percent_rank`/`cume_dist`/`ntile`) — their argument source is the SRF-only
//!   `WindowObject` (`PG_WINDOW_OBJECT()`/`windowapi.h`), which is not carried on
//!   the `FunctionCallInfoBaseData` frame; the value cores call the unported
//!   `windowapi` context stubs.

use std::string::{String, ToString};

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};
use types_stringinfo::StringInfo;

use types_core::Oid;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_OID(i)` → `DatumGetObjectId`: the low 32 bits of arg `i`'s word.
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo.arg(i).expect("misc2 fn: missing arg").value.as_oid()
}

/// `PG_GETARG_INT32(i)`.
#[inline]
fn arg_i32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("misc2 fn: missing arg").value.as_i32()
}

/// `PG_GETARG_INT64(i)`.
#[inline]
fn arg_i64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("misc2 fn: missing arg").value.as_i64()
}

/// `PG_GETARG_BOOL(i)`.
#[inline]
fn arg_bool(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).expect("misc2 fn: missing arg").value.as_bool()
}

/// `PG_GETARG_CSTRING(i)`: the input text on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("misc2 fn: cstring arg missing from by-ref lane")
}

/// A `text` / `name` arg's detoasted `VARDATA_ANY` payload bytes, decoded as
/// UTF-8 (the boundary carries `text` args header-less on the by-ref lane,
/// matching the established adt convention). `name` is fixed-length NUL-padded;
/// trim any trailing NULs.
#[inline]
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("misc2 fn: text arg missing from by-ref lane");
    // VARDATA_ANY: skip the 4-byte varlena header on the header-ful image.
    let bytes = &image[types_datum::varlena::VARHDRSZ..];
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    core::str::from_utf8(&bytes[..end]).expect("misc2 fn: text arg not valid UTF-8")
}

#[inline]
fn ret_oid(v: Oid) -> Datum {
    Datum::from_oid(v)
}
#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}
/// `PG_RETURN_VOID()`: C returns `(Datum) 0`; nothing is NULL.
#[inline]
fn ret_void() -> Datum {
    Datum::from_usize(0)
}
/// `PG_RETURN_NULL()`: mark the result NULL and return a dummy word.
#[inline]
fn ret_null(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    fcinfo.set_result_null(true);
    Datum::from_usize(0)
}
/// `PG_RETURN_OID(oid)` or `PG_RETURN_NULL()` for `None` (the soft / not-found
/// path of the `reg*in`/`to_reg*` cores).
#[inline]
fn ret_oid_opt(fcinfo: &mut FunctionCallInfoBaseData, v: Option<Oid>) -> Datum {
    match v {
        Some(o) => ret_oid(o),
        None => ret_null(fcinfo),
    }
}

/// Set a `cstring` (`reg*out`) result on the by-ref lane.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}

/// Map a `Datum<'mcx>` value-core result for a `text`/`bytea` (`pg_read_*`) or
/// `regclass`/`oid` (`pg_partition_root`) onto the fmgr frame.
///
/// The `ByRef` arm is a full varlena image (header included); the boundary's
/// `RefPayload::Varlena` for a result carries the header-less payload, so strip
/// `VARHDRSZ`. The `ByVal(0)` arm is the cores' `Datum::null()` (the
/// missing-file / not-a-partition NULL); other `ByVal` words are real scalars
/// (an `oid`). A non-zero scalar therefore returns the word as-is.
fn ret_value_datum(fcinfo: &mut FunctionCallInfoBaseData, d: types_tuple::Datum<'_>) -> Datum {
    match d {
        types_tuple::Datum::ByRef(bytes) => {
            // Header-ful everywhere: the by-ref image (full varlena, header
            // included) crosses verbatim onto the by-ref lane.
            fcinfo.set_ref_result(RefPayload::Varlena(bytes.as_slice().to_vec()));
            Datum::from_usize(0)
        }
        types_tuple::Datum::ByVal(0) => ret_null(fcinfo),
        types_tuple::Datum::ByVal(w) => Datum::from_usize(w),
        types_tuple::Datum::Cstring(s) => {
            fcinfo.set_ref_result(RefPayload::Cstring(s));
            Datum::from_usize(0)
        }
        // No other arm is produced by the cores registered here.
        _ => panic!("misc2 fmgr: unexpected Datum arm from value core"),
    }
}

/// A scratch context for cores that allocate their result through `Mcx`.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("misc2 fmgr scratch")
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    std::panic::panic_any(err);
}

// ===========================================================================
// regproc.c — reg* alias-type I/O + to_reg* lookups.
// ===========================================================================

/// Generic `reg*in(cstring)` adapter: the value core takes `(mcx, &str,
/// Option<&mut SoftErrorContext>)` and returns `PgResult<Option<Oid>>`. The
/// soft-error sink comes from the call frame (C: `escontext = (Node *)
/// fcinfo->context`): a recoverable parse/lookup failure `ereturn`s into it and
/// the core returns `Ok(None)`, leaving the result NULL; a hard `ereport(ERROR)`
/// (escontext absent, or a non-recoverable callee) propagates as a panic. With
/// no caller-supplied sink the frame's escontext is `None`, so every error is
/// hard — matching C's `regprocin` with a NULL escontext.
fn fc_regin(
    fcinfo: &mut FunctionCallInfoBaseData,
    core: fn(mcx::Mcx<'_>, &str, Option<&mut types_error::SoftErrorContext>) -> types_error::PgResult<Option<Oid>>,
) -> Datum {
    let s = alloc::string::String::from(arg_cstring(fcinfo, 0));
    let m = scratch_mcx();
    // Move the frame's sink out so it can be threaded into the core, then put
    // the (possibly error-recorded) sink back for the boundary to inspect.
    let mut escontext = fcinfo.escontext.take();
    let outcome = core(m.mcx(), &s, escontext.as_mut());
    fcinfo.escontext = escontext;
    match outcome {
        Ok(opt) => ret_oid_opt(fcinfo, opt),
        Err(e) => raise(e),
    }
}

/// Generic `to_reg*(text)` adapter: `(mcx, &str) -> PgResult<Option<Oid>>`,
/// returning NULL when not found.
fn fc_to_reg(
    fcinfo: &mut FunctionCallInfoBaseData,
    core: fn(mcx::Mcx<'_>, &str) -> types_error::PgResult<Option<Oid>>,
) -> Datum {
    let s = arg_text(fcinfo, 0);
    let m = scratch_mcx();
    match core(m.mcx(), s) {
        Ok(opt) => ret_oid_opt(fcinfo, opt),
        Err(e) => raise(e),
    }
}

/// Generic `reg*out(oid)` adapter: `(mcx, Oid) -> PgResult<PgString>`.
fn fc_regout(
    fcinfo: &mut FunctionCallInfoBaseData,
    core: fn(mcx::Mcx<'_>, Oid) -> types_error::PgResult<mcx::PgString<'_>>,
) -> Datum {
    let oid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    let res = core(m.mcx(), oid);
    match res {
        Ok(s) => ret_cstring(fcinfo, s.as_str().to_string()),
        Err(e) => raise(e),
    }
}

/// `reg*recv(internal)` — every reg* binary-input is byte-for-byte `oidrecv`
/// (regproc.c: `return oidrecv(fcinfo)`). The wire `StringInfo` message buffer
/// arrives on the by-ref lane as its raw bytes; rebuild a `StringInfo` (in a
/// scratch context) and hand it to the real `oid.c` value core.
fn fc_regrecv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let src = fcinfo
        .ref_arg(0)
        .and_then(|p| p.as_varlena())
        .expect("reg*recv: by-ref StringInfo arg missing from by-ref lane");
    let m = scratch_mcx();
    let mut data = mcx::PgVec::new_in(m.mcx());
    if data.try_reserve(src.len()).is_err() {
        raise(types_error::PgError::error("out of memory"));
    }
    data.extend_from_slice(src);
    let mut buf = StringInfo::from_vec(data);
    match backend_utils_adt_oid::oidrecv(&mut buf) {
        Ok(o) => ret_oid(o),
        Err(e) => raise(e),
    }
}

/// `reg*send(reg*)` — every reg* binary-output is byte-for-byte `oidsend`
/// (regproc.c: `return oidsend(fcinfo)`). Delegates to the real `oid.c` value
/// core and writes the `bytea` wire image onto the by-ref lane.
fn fc_regsend(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg1 = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    let image = match backend_utils_adt_oid::oidsend(m.mcx(), arg1) {
        Ok(bytea) => bytea.as_bytes().to_vec(),
        Err(e) => raise(e),
    };
    ret_bytea_image(fcinfo, &image)
}

fn fc_regprocin(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_regin(f, crate::regproc::regprocin)
}
fn fc_regprocout(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_regout(f, crate::regproc::regprocout)
}
fn fc_regprocedurein(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_regin(f, crate::regproc::regprocedurein)
}
fn fc_regprocedureout(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_regout(f, crate::regproc::regprocedureout)
}
fn fc_regoperin(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_regin(f, crate::regproc::regoperin)
}
fn fc_regoperout(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_regout(f, crate::regproc::regoperout)
}
fn fc_regoperatorin(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_regin(f, crate::regproc::regoperatorin)
}
fn fc_regoperatorout(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_regout(f, crate::regproc::regoperatorout)
}
fn fc_regclassin(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_regin(f, crate::regproc::regclassin)
}
fn fc_regclassout(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_regout(f, crate::regproc::regclassout)
}
fn fc_regtypein(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_regin(f, crate::regproc::regtypein)
}
fn fc_regtypeout(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_regout(f, crate::regproc::regtypeout)
}
fn fc_regconfigin(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_regin(f, crate::regproc::regconfigin)
}
fn fc_regconfigout(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_regout(f, crate::regproc::regconfigout)
}
fn fc_regdictionaryin(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_regin(f, crate::regproc::regdictionaryin)
}
fn fc_regdictionaryout(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_regout(f, crate::regproc::regdictionaryout)
}
fn fc_regrolein(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_regin(f, crate::regproc::regrolein)
}
fn fc_regroleout(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_regout(f, crate::regproc::regroleout)
}
fn fc_regnamespacein(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_regin(f, crate::regproc::regnamespacein)
}
fn fc_regnamespaceout(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_regout(f, crate::regproc::regnamespaceout)
}
fn fc_regcollationin(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_regin(f, crate::regproc::regcollationin)
}
fn fc_regcollationout(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_regout(f, crate::regproc::regcollationout)
}

fn fc_to_regproc(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_to_reg(f, crate::regproc::to_regproc)
}
fn fc_to_regprocedure(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_to_reg(f, crate::regproc::to_regprocedure)
}
fn fc_to_regoper(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_to_reg(f, crate::regproc::to_regoper)
}
fn fc_to_regoperator(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_to_reg(f, crate::regproc::to_regoperator)
}
fn fc_to_regtype(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_to_reg(f, crate::regproc::to_regtype)
}
fn fc_to_regclass(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_to_reg(f, crate::regproc::to_regclass)
}
fn fc_to_regrole(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_to_reg(f, crate::regproc::to_regrole)
}
fn fc_to_regnamespace(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_to_reg(f, crate::regproc::to_regnamespace)
}
fn fc_to_regcollation(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_to_reg(f, crate::regproc::to_regcollation)
}

/// `to_regtypemod(text)` — `(mcx, &str) -> PgResult<Option<i32>>`, NULL when
/// not found.
fn fc_to_regtypemod(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_text(fcinfo, 0);
    let m = scratch_mcx();
    match crate::regproc::to_regtypemod(m.mcx(), s) {
        Ok(Some(v)) => Datum::from_i32(v),
        Ok(None) => ret_null(fcinfo),
        Err(e) => raise(e),
    }
}

/// `regclass(text)` (the implicit text→regclass cast) — `text_regclass(mcx,
/// &str) -> PgResult<Oid>` (always a hard error / never NULL).
fn fc_text_regclass(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_text(fcinfo, 0);
    let m = scratch_mcx();
    match crate::regproc::text_regclass(m.mcx(), s) {
        Ok(oid) => ret_oid(oid),
        Err(e) => raise(e),
    }
}

// ===========================================================================
// genfile.c — pg_read_file / pg_read_binary_file (text / bytea result).
// ===========================================================================

fn fc_pg_read_file_off_len(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let filename = arg_text(fcinfo, 0);
    let off = arg_i64(fcinfo, 1);
    let len = arg_i64(fcinfo, 2);
    let m = scratch_mcx();
    let res = crate::admin::pg_read_file_off_len(m.mcx(), filename, off, len);
    match res {
        Ok(d) => ret_value_datum(fcinfo, d),
        Err(e) => raise(e),
    }
}
fn fc_pg_read_file_off_len_missing(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let filename = arg_text(fcinfo, 0);
    let off = arg_i64(fcinfo, 1);
    let len = arg_i64(fcinfo, 2);
    let missing = arg_bool(fcinfo, 3);
    let m = scratch_mcx();
    let res = crate::admin::pg_read_file_off_len_missing(m.mcx(), filename, off, len, missing);
    match res {
        Ok(d) => ret_value_datum(fcinfo, d),
        Err(e) => raise(e),
    }
}
fn fc_pg_read_file_all(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let filename = arg_text(fcinfo, 0);
    let m = scratch_mcx();
    let res = crate::admin::pg_read_file_all(m.mcx(), filename);
    match res {
        Ok(d) => ret_value_datum(fcinfo, d),
        Err(e) => raise(e),
    }
}
fn fc_pg_read_file_all_missing(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let filename = arg_text(fcinfo, 0);
    let missing = arg_bool(fcinfo, 1);
    let m = scratch_mcx();
    let res = crate::admin::pg_read_file_all_missing(m.mcx(), filename, missing);
    match res {
        Ok(d) => ret_value_datum(fcinfo, d),
        Err(e) => raise(e),
    }
}
fn fc_pg_read_binary_file_off_len(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let filename = arg_text(fcinfo, 0);
    let off = arg_i64(fcinfo, 1);
    let len = arg_i64(fcinfo, 2);
    let m = scratch_mcx();
    let res = crate::admin::pg_read_binary_file_off_len(m.mcx(), filename, off, len);
    match res {
        Ok(d) => ret_value_datum(fcinfo, d),
        Err(e) => raise(e),
    }
}
fn fc_pg_read_binary_file_off_len_missing(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let filename = arg_text(fcinfo, 0);
    let off = arg_i64(fcinfo, 1);
    let len = arg_i64(fcinfo, 2);
    let missing = arg_bool(fcinfo, 3);
    let m = scratch_mcx();
    let res = crate::admin::pg_read_binary_file_off_len_missing(m.mcx(), filename, off, len, missing);
    match res {
        Ok(d) => ret_value_datum(fcinfo, d),
        Err(e) => raise(e),
    }
}
fn fc_pg_read_binary_file_all(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let filename = arg_text(fcinfo, 0);
    let m = scratch_mcx();
    let res = crate::admin::pg_read_binary_file_all(m.mcx(), filename);
    match res {
        Ok(d) => ret_value_datum(fcinfo, d),
        Err(e) => raise(e),
    }
}
fn fc_pg_read_binary_file_all_missing(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let filename = arg_text(fcinfo, 0);
    let missing = arg_bool(fcinfo, 1);
    let m = scratch_mcx();
    let res = crate::admin::pg_read_binary_file_all_missing(m.mcx(), filename, missing);
    match res {
        Ok(d) => ret_value_datum(fcinfo, d),
        Err(e) => raise(e),
    }
}

// ===========================================================================
// partitionfuncs.c — pg_partition_root (regclass result).
// ===========================================================================

fn fc_pg_partition_root(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let relid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    let res = crate::admin::pg_partition_root(m.mcx(), relid);
    match res {
        Ok(d) => ret_value_datum(fcinfo, d),
        Err(e) => raise(e),
    }
}

// ===========================================================================
// lockfuncs.c — advisory locks (int8 / int4 keys; void or bool result).
// ===========================================================================

/// `(int8) -> void` advisory adapter.
fn fc_adv_void_int8(
    fcinfo: &mut FunctionCallInfoBaseData,
    core: fn(i64) -> types_error::PgResult<()>,
) -> Datum {
    match core(arg_i64(fcinfo, 0)) {
        Ok(()) => ret_void(),
        Err(e) => raise(e),
    }
}
/// `(int8) -> bool` advisory adapter.
fn fc_adv_bool_int8(
    fcinfo: &mut FunctionCallInfoBaseData,
    core: fn(i64) -> types_error::PgResult<bool>,
) -> Datum {
    match core(arg_i64(fcinfo, 0)) {
        Ok(b) => ret_bool(b),
        Err(e) => raise(e),
    }
}
/// `(int4, int4) -> void` advisory adapter.
fn fc_adv_void_int4(
    fcinfo: &mut FunctionCallInfoBaseData,
    core: fn(i32, i32) -> types_error::PgResult<()>,
) -> Datum {
    match core(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1)) {
        Ok(()) => ret_void(),
        Err(e) => raise(e),
    }
}
/// `(int4, int4) -> bool` advisory adapter.
fn fc_adv_bool_int4(
    fcinfo: &mut FunctionCallInfoBaseData,
    core: fn(i32, i32) -> types_error::PgResult<bool>,
) -> Datum {
    match core(arg_i32(fcinfo, 0), arg_i32(fcinfo, 1)) {
        Ok(b) => ret_bool(b),
        Err(e) => raise(e),
    }
}

fn fc_pg_advisory_lock_int8(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_adv_void_int8(f, crate::admin::pg_advisory_lock_int8)
}
fn fc_pg_advisory_lock_shared_int8(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_adv_void_int8(f, crate::admin::pg_advisory_lock_shared_int8)
}
fn fc_pg_try_advisory_lock_int8(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_adv_bool_int8(f, crate::admin::pg_try_advisory_lock_int8)
}
fn fc_pg_try_advisory_lock_shared_int8(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_adv_bool_int8(f, crate::admin::pg_try_advisory_lock_shared_int8)
}
fn fc_pg_advisory_unlock_int8(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_adv_bool_int8(f, crate::admin::pg_advisory_unlock_int8)
}
fn fc_pg_advisory_unlock_shared_int8(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_adv_bool_int8(f, crate::admin::pg_advisory_unlock_shared_int8)
}
fn fc_pg_advisory_xact_lock_int8(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_adv_void_int8(f, crate::admin::pg_advisory_xact_lock_int8)
}
fn fc_pg_advisory_xact_lock_shared_int8(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_adv_void_int8(f, crate::admin::pg_advisory_xact_lock_shared_int8)
}
fn fc_pg_try_advisory_xact_lock_int8(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_adv_bool_int8(f, crate::admin::pg_try_advisory_xact_lock_int8)
}
fn fc_pg_try_advisory_xact_lock_shared_int8(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_adv_bool_int8(f, crate::admin::pg_try_advisory_xact_lock_shared_int8)
}

fn fc_pg_advisory_lock_int4(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_adv_void_int4(f, crate::admin::pg_advisory_lock_int4)
}
fn fc_pg_advisory_lock_shared_int4(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_adv_void_int4(f, crate::admin::pg_advisory_lock_shared_int4)
}
fn fc_pg_try_advisory_lock_int4(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_adv_bool_int4(f, crate::admin::pg_try_advisory_lock_int4)
}
fn fc_pg_try_advisory_lock_shared_int4(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_adv_bool_int4(f, crate::admin::pg_try_advisory_lock_shared_int4)
}
fn fc_pg_advisory_unlock_int4(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_adv_bool_int4(f, crate::admin::pg_advisory_unlock_int4)
}
fn fc_pg_advisory_unlock_shared_int4(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_adv_bool_int4(f, crate::admin::pg_advisory_unlock_shared_int4)
}
fn fc_pg_advisory_xact_lock_int4(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_adv_void_int4(f, crate::admin::pg_advisory_xact_lock_int4)
}
fn fc_pg_advisory_xact_lock_shared_int4(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_adv_void_int4(f, crate::admin::pg_advisory_xact_lock_shared_int4)
}
fn fc_pg_try_advisory_xact_lock_int4(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_adv_bool_int4(f, crate::admin::pg_try_advisory_xact_lock_int4)
}
fn fc_pg_try_advisory_xact_lock_shared_int4(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_adv_bool_int4(f, crate::admin::pg_try_advisory_xact_lock_shared_int4)
}
fn fc_pg_advisory_unlock_all(_f: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::admin::pg_advisory_unlock_all() {
        Ok(()) => ret_void(),
        Err(e) => raise(e),
    }
}

// ===========================================================================
// pg_upgrade_support.c — binary_upgrade_* setters (void / bool result).
// ===========================================================================

/// `(oid) -> void` binary-upgrade setter adapter.
fn fc_binup_oid_void(
    fcinfo: &mut FunctionCallInfoBaseData,
    core: fn(u32) -> types_error::PgResult<()>,
) -> Datum {
    match core(arg_oid(fcinfo, 0)) {
        Ok(()) => ret_void(),
        Err(e) => raise(e),
    }
}

fn fc_binary_upgrade_set_next_pg_type_oid(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_binup_oid_void(f, crate::admin::binary_upgrade_set_next_pg_type_oid)
}
fn fc_binary_upgrade_set_next_array_pg_type_oid(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_binup_oid_void(f, crate::admin::binary_upgrade_set_next_array_pg_type_oid)
}
fn fc_binary_upgrade_set_next_multirange_pg_type_oid(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_binup_oid_void(f, crate::admin::binary_upgrade_set_next_multirange_pg_type_oid)
}
fn fc_binary_upgrade_set_next_multirange_array_pg_type_oid(
    f: &mut FunctionCallInfoBaseData,
) -> Datum {
    fc_binup_oid_void(
        f,
        crate::admin::binary_upgrade_set_next_multirange_array_pg_type_oid,
    )
}
fn fc_binary_upgrade_set_next_heap_pg_class_oid(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_binup_oid_void(f, crate::admin::binary_upgrade_set_next_heap_pg_class_oid)
}
fn fc_binary_upgrade_set_next_heap_relfilenode(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_binup_oid_void(f, crate::admin::binary_upgrade_set_next_heap_relfilenode)
}
fn fc_binary_upgrade_set_next_index_pg_class_oid(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_binup_oid_void(f, crate::admin::binary_upgrade_set_next_index_pg_class_oid)
}
fn fc_binary_upgrade_set_next_index_relfilenode(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_binup_oid_void(f, crate::admin::binary_upgrade_set_next_index_relfilenode)
}
fn fc_binary_upgrade_set_next_toast_pg_class_oid(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_binup_oid_void(f, crate::admin::binary_upgrade_set_next_toast_pg_class_oid)
}
fn fc_binary_upgrade_set_next_toast_relfilenode(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_binup_oid_void(f, crate::admin::binary_upgrade_set_next_toast_relfilenode)
}
fn fc_binary_upgrade_set_next_pg_enum_oid(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_binup_oid_void(f, crate::admin::binary_upgrade_set_next_pg_enum_oid)
}
fn fc_binary_upgrade_set_next_pg_authid_oid(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_binup_oid_void(f, crate::admin::binary_upgrade_set_next_pg_authid_oid)
}
fn fc_binary_upgrade_set_next_pg_tablespace_oid(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_binup_oid_void(f, crate::admin::binary_upgrade_set_next_pg_tablespace_oid)
}

fn fc_binary_upgrade_set_record_init_privs(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::admin::binary_upgrade_set_record_init_privs(arg_bool(fcinfo, 0)) {
        Ok(()) => ret_void(),
        Err(e) => raise(e),
    }
}

fn fc_binary_upgrade_set_missing_value(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let table_id = arg_oid(fcinfo, 0);
    let attname = arg_text(fcinfo, 1);
    let value = arg_text(fcinfo, 2);
    match crate::admin::binary_upgrade_set_missing_value(table_id, attname, value) {
        Ok(()) => ret_void(),
        Err(e) => raise(e),
    }
}

fn fc_binary_upgrade_logical_slot_has_caught_up(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let slot_name = arg_text(fcinfo, 0);
    match crate::admin::binary_upgrade_logical_slot_has_caught_up(slot_name) {
        Ok(b) => ret_bool(b),
        Err(e) => raise(e),
    }
}

// ===========================================================================
// rowtypes.c — record / row-as-value I/O, comparison, image-compare, hash.
//
// A composite value crosses the fmgr boundary on the by-reference side channel
// as a `RefPayload::Composite` — the flat `HeapTupleHeader` Datum image C points
// a record `Datum` at. `arg_record` reconstructs the [`FormedTuple`] the value
// cores take; `ret_record` serializes a `FormedTuple` result back onto the lane.
// ===========================================================================

use types_tuple::backend_access_common_heaptuple::FormedTuple;

/// `PG_GETARG_HEAPTUPLEHEADER(i)` → the composite arg's [`FormedTuple`],
/// reconstructed from its flat `HeapTupleHeader` Datum image on the by-ref lane.
fn arg_record<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    fcinfo: &FunctionCallInfoBaseData,
    i: usize,
) -> FormedTuple<'mcx> {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_composite())
        .expect("rowtypes fn: composite arg missing from by-ref lane");
    match FormedTuple::from_datum_image(mcx, image) {
        Ok(t) => t,
        Err(e) => raise(e),
    }
}

/// Set a composite (`record`) result on the by-ref lane as its flat
/// `HeapTupleHeader` Datum image (`PG_RETURN_HEAPTUPLEHEADER`).
#[inline]
fn ret_record(fcinfo: &mut FunctionCallInfoBaseData, t: &FormedTuple<'_>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Composite(t.to_datum_image()));
    Datum::from_usize(0)
}

/// Set a `bytea` result (`record_send`) on the by-ref lane. Header-ful
/// everywhere: the core returns the full varlena image, carried verbatim (the
/// protocol-level `VARHDRSZ` strip stays in fmgr-core's send path).
#[inline]
fn ret_bytea_image(fcinfo: &mut FunctionCallInfoBaseData, image: &[u8]) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(image.to_vec()));
    Datum::from_usize(0)
}

#[inline]
fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
}
#[inline]
fn ret_i64(v: i64) -> Datum {
    Datum::from_i64(v)
}

/// A `(mcx, &FormedTuple, &FormedTuple) -> PgResult<bool>` comparison adapter
/// (`record_eq`/`ne`/`lt`/`gt`/`le`/`ge` and the `record_image_*` family).
fn fc_record_cmp_bool(
    fcinfo: &mut FunctionCallInfoBaseData,
    core: for<'m> fn(
        mcx::Mcx<'m>,
        &FormedTuple<'_>,
        &FormedTuple<'_>,
    ) -> types_error::PgResult<bool>,
) -> Datum {
    let m = scratch_mcx();
    let left = arg_record(m.mcx(), fcinfo, 0);
    let right = arg_record(m.mcx(), fcinfo, 1);
    match core(m.mcx(), &left, &right) {
        Ok(b) => ret_bool(b),
        Err(e) => raise(e),
    }
}

/// A `(mcx, &FormedTuple, &FormedTuple) -> PgResult<i32>` comparison adapter
/// (`btrecordcmp` / `record_cmp`).
fn fc_record_cmp_i32(
    fcinfo: &mut FunctionCallInfoBaseData,
    core: for<'m> fn(
        mcx::Mcx<'m>,
        &FormedTuple<'_>,
        &FormedTuple<'_>,
    ) -> types_error::PgResult<i32>,
) -> Datum {
    let m = scratch_mcx();
    let left = arg_record(m.mcx(), fcinfo, 0);
    let right = arg_record(m.mcx(), fcinfo, 1);
    match core(m.mcx(), &left, &right) {
        Ok(v) => ret_i32(v),
        Err(e) => raise(e),
    }
}

fn fc_record_eq(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_record_cmp_bool(f, crate::rowtypes::record_eq)
}
fn fc_record_ne(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_record_cmp_bool(f, crate::rowtypes::record_ne)
}
fn fc_record_lt(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_record_cmp_bool(f, crate::rowtypes::record_lt)
}
fn fc_record_gt(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_record_cmp_bool(f, crate::rowtypes::record_gt)
}
fn fc_record_le(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_record_cmp_bool(f, crate::rowtypes::record_le)
}
fn fc_record_ge(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_record_cmp_bool(f, crate::rowtypes::record_ge)
}
fn fc_btrecordcmp(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_record_cmp_i32(f, crate::rowtypes::btrecordcmp)
}

fn fc_record_image_eq(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_record_cmp_bool(f, crate::rowtypes::record_image_eq)
}
fn fc_record_image_ne(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_record_cmp_bool(f, crate::rowtypes::record_image_ne)
}
fn fc_record_image_lt(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_record_cmp_bool(f, crate::rowtypes::record_image_lt)
}
fn fc_record_image_gt(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_record_cmp_bool(f, crate::rowtypes::record_image_gt)
}
fn fc_record_image_le(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_record_cmp_bool(f, crate::rowtypes::record_image_le)
}
fn fc_record_image_ge(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_record_cmp_bool(f, crate::rowtypes::record_image_ge)
}

/// `record_larger`/`record_smaller`: `(mcx, FormedTuple, FormedTuple) ->
/// PgResult<FormedTuple>` (returns one of the inputs as a composite result).
fn fc_record_larger_smaller(
    fcinfo: &mut FunctionCallInfoBaseData,
    core: for<'m> fn(
        mcx::Mcx<'m>,
        FormedTuple<'m>,
        FormedTuple<'m>,
    ) -> types_error::PgResult<FormedTuple<'m>>,
) -> Datum {
    let m = scratch_mcx();
    let left = arg_record(m.mcx(), fcinfo, 0);
    let right = arg_record(m.mcx(), fcinfo, 1);
    let r = core(m.mcx(), left, right);
    match r {
        Ok(t) => ret_record(fcinfo, &t),
        Err(e) => raise(e),
    }
}
fn fc_record_larger(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_record_larger_smaller(f, crate::rowtypes::record_larger)
}
fn fc_record_smaller(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_record_larger_smaller(f, crate::rowtypes::record_smaller)
}

/// `hash_record(record) -> int4`.
fn fc_hash_record(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let rec = arg_record(m.mcx(), fcinfo, 0);
    match crate::rowtypes::hash_record(m.mcx(), &rec) {
        Ok(h) => ret_i32(h as i32),
        Err(e) => raise(e),
    }
}

/// `hash_record_extended(record, int8) -> int8`.
fn fc_hash_record_extended(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let rec = arg_record(m.mcx(), fcinfo, 0);
    let seed = arg_i64(fcinfo, 1) as u64;
    match crate::rowtypes::hash_record_extended(m.mcx(), &rec, seed) {
        Ok(h) => ret_i64(h as i64),
        Err(e) => raise(e),
    }
}

/// `record_in(cstring, oid, int4) -> record`. The soft-error sink comes from
/// the call frame (C: `escontext = (Node *) fcinfo->context`): a recoverable
/// parse failure `ereturn`s into it and the core returns `Ok(None)` (NULL
/// result); a hard error propagates. With no caller sink the frame's escontext
/// is `None`, so every error is hard.
fn fc_record_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = alloc::string::String::from(arg_cstring(fcinfo, 0));
    let tupioparam = arg_oid(fcinfo, 1);
    let tup_typmod = arg_i32(fcinfo, 2);
    let m = scratch_mcx();
    let mut escontext = fcinfo.escontext.take();
    let r = crate::rowtypes::record_in(
        m.mcx(),
        Some(&s),
        tupioparam,
        tup_typmod,
        escontext.as_mut(),
    );
    fcinfo.escontext = escontext;
    match r {
        Ok(Some(t)) => ret_record(fcinfo, &t),
        Ok(None) => ret_null(fcinfo),
        Err(e) => raise(e),
    }
}

/// `record_out(record) -> cstring`.
fn fc_record_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let rec = arg_record(m.mcx(), fcinfo, 0);
    let r = crate::rowtypes::record_out(m.mcx(), &rec);
    match r {
        Ok(bytes) => {
            let s = String::from_utf8(bytes.as_slice().to_vec())
                .expect("record_out: result not valid UTF-8");
            ret_cstring(fcinfo, s)
        }
        Err(e) => raise(e),
    }
}

/// `record_send(record) -> bytea`.
fn fc_record_send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let rec = arg_record(m.mcx(), fcinfo, 0);
    let r = crate::rowtypes::record_send(m.mcx(), &rec);
    match r {
        Ok(bytes) => ret_bytea_image(fcinfo, bytes.as_slice()),
        Err(e) => raise(e),
    }
}

// ===========================================================================
// tid.c — the `tid` ItemPointer type's SQL-callable I/O, comparison, hashing
// and min/max helpers.
// ===========================================================================

/// A `tid` arg as a `Datum<'mcx>` for the value cores: the ItemPointer's 6-byte
/// fixed-length image (`BlockIdData{bi_hi,bi_lo}` + `uint16` offset, no varlena
/// header) crosses on the by-ref lane verbatim. Materialize it as a `ByRef`
/// `Datum` in `mcx`, exactly the carrier `getarg_itempointer` deref's.
fn arg_tid<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    fcinfo: &FunctionCallInfoBaseData,
    i: usize,
) -> types_tuple::Datum<'mcx> {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("tid fn: by-ref ItemPointer arg missing from by-ref lane");
    types_tuple::Datum::ByRef(
        mcx::slice_in(mcx, image).expect("tid fn: out of memory copying ItemPointer image"),
    )
}

fn fc_tidin(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_cstring(fcinfo, 0);
    let m = scratch_mcx();
    let r = crate::scalars::tidin(m.mcx(), Some(s));
    match r {
        Ok(d) => ret_value_datum(fcinfo, d),
        Err(e) => raise(e),
    }
}

fn fc_tidout(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let arg = arg_tid(m.mcx(), fcinfo, 0);
    let r = crate::scalars::tidout(m.mcx(), arg);
    match r {
        Ok(d) => ret_value_datum(fcinfo, d),
        Err(e) => raise(e),
    }
}

fn fc_tidrecv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let src = fcinfo
        .ref_arg(0)
        .and_then(|p| p.as_varlena())
        .expect("tidrecv: by-ref StringInfo arg missing from by-ref lane");
    let m = scratch_mcx();
    let r = crate::scalars::tidrecv(m.mcx(), src);
    match r {
        Ok(d) => ret_value_datum(fcinfo, d),
        Err(e) => raise(e),
    }
}

fn fc_tidsend(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let arg = arg_tid(m.mcx(), fcinfo, 0);
    let r = crate::scalars::tidsend(m.mcx(), arg);
    match r {
        Ok(d) => ret_value_datum(fcinfo, d),
        Err(e) => raise(e),
    }
}

/// Shared `(tid, tid) -> bool` operator adapter.
fn fc_tid_cmp_bool(
    fcinfo: &mut FunctionCallInfoBaseData,
    f: fn(types_tuple::Datum<'_>, types_tuple::Datum<'_>) -> types_error::PgResult<bool>,
) -> Datum {
    let m = scratch_mcx();
    let a1 = arg_tid(m.mcx(), fcinfo, 0);
    let a2 = arg_tid(m.mcx(), fcinfo, 1);
    match f(a1, a2) {
        Ok(b) => ret_bool(b),
        Err(e) => raise(e),
    }
}

fn fc_tideq(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_tid_cmp_bool(f, crate::scalars::tideq)
}
fn fc_tidne(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_tid_cmp_bool(f, crate::scalars::tidne)
}
fn fc_tidlt(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_tid_cmp_bool(f, crate::scalars::tidlt)
}
fn fc_tidle(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_tid_cmp_bool(f, crate::scalars::tidle)
}
fn fc_tidgt(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_tid_cmp_bool(f, crate::scalars::tidgt)
}
fn fc_tidge(f: &mut FunctionCallInfoBaseData) -> Datum {
    fc_tid_cmp_bool(f, crate::scalars::tidge)
}

fn fc_bttidcmp(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let a1 = arg_tid(m.mcx(), fcinfo, 0);
    let a2 = arg_tid(m.mcx(), fcinfo, 1);
    match crate::scalars::bttidcmp(a1, a2) {
        Ok(v) => Datum::from_i32(v),
        Err(e) => raise(e),
    }
}

fn fc_tidlarger(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let a1 = arg_tid(m.mcx(), fcinfo, 0);
    let a2 = arg_tid(m.mcx(), fcinfo, 1);
    let r = crate::scalars::tidlarger(a1, a2);
    match r {
        Ok(d) => ret_value_datum(fcinfo, d),
        Err(e) => raise(e),
    }
}

fn fc_tidsmaller(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let a1 = arg_tid(m.mcx(), fcinfo, 0);
    let a2 = arg_tid(m.mcx(), fcinfo, 1);
    let r = crate::scalars::tidsmaller(a1, a2);
    match r {
        Ok(d) => ret_value_datum(fcinfo, d),
        Err(e) => raise(e),
    }
}

fn fc_hashtid(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let key = arg_tid(m.mcx(), fcinfo, 0);
    match crate::scalars::hashtid(key) {
        Ok(v) => Datum::from_u32(v),
        Err(e) => raise(e),
    }
}

fn fc_hashtidextended(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let key = arg_tid(m.mcx(), fcinfo, 0);
    let seed = arg_i64(fcinfo, 1) as u64;
    match crate::scalars::hashtidextended(key, seed) {
        Ok(v) => Datum::from_u64(v),
        Err(e) => raise(e),
    }
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

/// Register every SQL-callable builtin of this unit whose types are expressible
/// at the current fmgr boundary (C: their `fmgr_builtins[]` rows). Called from
/// this crate's `init_seams()`. OIDs/nargs/strict/retset transcribed exactly
/// from `pg_proc.dat` (none is strict or retset).
pub fn register_misc2_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // ---- regproc.c: reg* I/O ----
        builtin(44, "regprocin", 1, true, false, fc_regprocin),
        builtin(45, "regprocout", 1, true, false, fc_regprocout),
        builtin(2212, "regprocedurein", 1, true, false, fc_regprocedurein),
        builtin(2213, "regprocedureout", 1, true, false, fc_regprocedureout),
        builtin(2214, "regoperin", 1, true, false, fc_regoperin),
        builtin(2215, "regoperout", 1, true, false, fc_regoperout),
        builtin(2216, "regoperatorin", 1, true, false, fc_regoperatorin),
        builtin(2217, "regoperatorout", 1, true, false, fc_regoperatorout),
        builtin(2218, "regclassin", 1, true, false, fc_regclassin),
        builtin(2219, "regclassout", 1, true, false, fc_regclassout),
        builtin(2220, "regtypein", 1, true, false, fc_regtypein),
        builtin(2221, "regtypeout", 1, true, false, fc_regtypeout),
        builtin(3736, "regconfigin", 1, true, false, fc_regconfigin),
        builtin(3737, "regconfigout", 1, true, false, fc_regconfigout),
        builtin(3771, "regdictionaryin", 1, true, false, fc_regdictionaryin),
        builtin(3772, "regdictionaryout", 1, true, false, fc_regdictionaryout),
        builtin(4084, "regnamespacein", 1, true, false, fc_regnamespacein),
        builtin(4085, "regnamespaceout", 1, true, false, fc_regnamespaceout),
        builtin(4092, "regroleout", 1, true, false, fc_regroleout),
        builtin(4098, "regrolein", 1, true, false, fc_regrolein),
        builtin(4193, "regcollationin", 1, true, false, fc_regcollationin),
        builtin(4194, "regcollationout", 1, true, false, fc_regcollationout),
        // ---- regproc.c: reg* binary wire codecs (byte-for-byte oidrecv/oidsend) ----
        builtin(2444, "regprocrecv", 1, true, false, fc_regrecv),
        builtin(2445, "regprocsend", 1, true, false, fc_regsend),
        builtin(2446, "regprocedurerecv", 1, true, false, fc_regrecv),
        builtin(2447, "regproceduresend", 1, true, false, fc_regsend),
        builtin(2448, "regoperrecv", 1, true, false, fc_regrecv),
        builtin(2449, "regopersend", 1, true, false, fc_regsend),
        builtin(2450, "regoperatorrecv", 1, true, false, fc_regrecv),
        builtin(2451, "regoperatorsend", 1, true, false, fc_regsend),
        builtin(2452, "regclassrecv", 1, true, false, fc_regrecv),
        builtin(2453, "regclasssend", 1, true, false, fc_regsend),
        builtin(2454, "regtyperecv", 1, true, false, fc_regrecv),
        builtin(2455, "regtypesend", 1, true, false, fc_regsend),
        builtin(4196, "regcollationrecv", 1, true, false, fc_regrecv),
        builtin(4197, "regcollationsend", 1, true, false, fc_regsend),
        builtin(3738, "regconfigrecv", 1, true, false, fc_regrecv),
        builtin(3739, "regconfigsend", 1, true, false, fc_regsend),
        builtin(3773, "regdictionaryrecv", 1, true, false, fc_regrecv),
        builtin(3774, "regdictionarysend", 1, true, false, fc_regsend),
        builtin(4094, "regrolerecv", 1, true, false, fc_regrecv),
        builtin(4095, "regrolesend", 1, true, false, fc_regsend),
        builtin(4087, "regnamespacerecv", 1, true, false, fc_regrecv),
        builtin(4088, "regnamespacesend", 1, true, false, fc_regsend),
        // ---- regproc.c: text->reg implicit cast + to_reg* lookups ----
        builtin(1079, "text_regclass", 1, true, false, fc_text_regclass),
        builtin(3476, "to_regoperator", 1, true, false, fc_to_regoperator),
        builtin(3479, "to_regprocedure", 1, true, false, fc_to_regprocedure),
        builtin(3492, "to_regoper", 1, true, false, fc_to_regoper),
        builtin(3493, "to_regtype", 1, true, false, fc_to_regtype),
        builtin(3494, "to_regproc", 1, true, false, fc_to_regproc),
        builtin(3495, "to_regclass", 1, true, false, fc_to_regclass),
        builtin(4086, "to_regnamespace", 1, true, false, fc_to_regnamespace),
        builtin(4093, "to_regrole", 1, true, false, fc_to_regrole),
        builtin(4195, "to_regcollation", 1, true, false, fc_to_regcollation),
        builtin(6317, "to_regtypemod", 1, true, false, fc_to_regtypemod),
        // ---- genfile.c: pg_read_file / pg_read_binary_file ----
        builtin(2624, "pg_read_file_off_len", 3, true, false, fc_pg_read_file_off_len),
        builtin(3293, "pg_read_file_off_len_missing", 4, true, false, fc_pg_read_file_off_len_missing),
        builtin(3826, "pg_read_file_all", 1, true, false, fc_pg_read_file_all),
        builtin(6208, "pg_read_file_all_missing", 2, true, false, fc_pg_read_file_all_missing),
        builtin(3827, "pg_read_binary_file_off_len", 3, true, false, fc_pg_read_binary_file_off_len),
        builtin(3295, "pg_read_binary_file_off_len_missing", 4, true, false, fc_pg_read_binary_file_off_len_missing),
        builtin(3828, "pg_read_binary_file_all", 1, true, false, fc_pg_read_binary_file_all),
        builtin(6209, "pg_read_binary_file_all_missing", 2, true, false, fc_pg_read_binary_file_all_missing),
        // ---- partitionfuncs.c ----
        builtin(3424, "pg_partition_root", 1, true, false, fc_pg_partition_root),
        // ---- lockfuncs.c: advisory locks (int8) ----
        builtin(2880, "pg_advisory_lock_int8", 1, true, false, fc_pg_advisory_lock_int8),
        builtin(2881, "pg_advisory_lock_shared_int8", 1, true, false, fc_pg_advisory_lock_shared_int8),
        builtin(2882, "pg_try_advisory_lock_int8", 1, true, false, fc_pg_try_advisory_lock_int8),
        builtin(2883, "pg_try_advisory_lock_shared_int8", 1, true, false, fc_pg_try_advisory_lock_shared_int8),
        builtin(2884, "pg_advisory_unlock_int8", 1, true, false, fc_pg_advisory_unlock_int8),
        builtin(2885, "pg_advisory_unlock_shared_int8", 1, true, false, fc_pg_advisory_unlock_shared_int8),
        builtin(3089, "pg_advisory_xact_lock_int8", 1, true, false, fc_pg_advisory_xact_lock_int8),
        builtin(3090, "pg_advisory_xact_lock_shared_int8", 1, true, false, fc_pg_advisory_xact_lock_shared_int8),
        builtin(3091, "pg_try_advisory_xact_lock_int8", 1, true, false, fc_pg_try_advisory_xact_lock_int8),
        builtin(3092, "pg_try_advisory_xact_lock_shared_int8", 1, true, false, fc_pg_try_advisory_xact_lock_shared_int8),
        // ---- lockfuncs.c: advisory locks (int4, int4) ----
        builtin(2886, "pg_advisory_lock_int4", 2, true, false, fc_pg_advisory_lock_int4),
        builtin(2887, "pg_advisory_lock_shared_int4", 2, true, false, fc_pg_advisory_lock_shared_int4),
        builtin(2888, "pg_try_advisory_lock_int4", 2, true, false, fc_pg_try_advisory_lock_int4),
        builtin(2889, "pg_try_advisory_lock_shared_int4", 2, true, false, fc_pg_try_advisory_lock_shared_int4),
        builtin(2890, "pg_advisory_unlock_int4", 2, true, false, fc_pg_advisory_unlock_int4),
        builtin(2891, "pg_advisory_unlock_shared_int4", 2, true, false, fc_pg_advisory_unlock_shared_int4),
        builtin(3093, "pg_advisory_xact_lock_int4", 2, true, false, fc_pg_advisory_xact_lock_int4),
        builtin(3094, "pg_advisory_xact_lock_shared_int4", 2, true, false, fc_pg_advisory_xact_lock_shared_int4),
        builtin(3095, "pg_try_advisory_xact_lock_int4", 2, true, false, fc_pg_try_advisory_xact_lock_int4),
        builtin(3096, "pg_try_advisory_xact_lock_shared_int4", 2, true, false, fc_pg_try_advisory_xact_lock_shared_int4),
        builtin(2892, "pg_advisory_unlock_all", 0, true, false, fc_pg_advisory_unlock_all),
        // ---- pg_upgrade_support.c ----
        builtin(3582, "binary_upgrade_set_next_pg_type_oid", 1, true, false, fc_binary_upgrade_set_next_pg_type_oid),
        builtin(3584, "binary_upgrade_set_next_array_pg_type_oid", 1, true, false, fc_binary_upgrade_set_next_array_pg_type_oid),
        builtin(4390, "binary_upgrade_set_next_multirange_pg_type_oid", 1, true, false, fc_binary_upgrade_set_next_multirange_pg_type_oid),
        builtin(4391, "binary_upgrade_set_next_multirange_array_pg_type_oid", 1, true, false, fc_binary_upgrade_set_next_multirange_array_pg_type_oid),
        builtin(3586, "binary_upgrade_set_next_heap_pg_class_oid", 1, true, false, fc_binary_upgrade_set_next_heap_pg_class_oid),
        builtin(4545, "binary_upgrade_set_next_heap_relfilenode", 1, true, false, fc_binary_upgrade_set_next_heap_relfilenode),
        builtin(3587, "binary_upgrade_set_next_index_pg_class_oid", 1, true, false, fc_binary_upgrade_set_next_index_pg_class_oid),
        builtin(4546, "binary_upgrade_set_next_index_relfilenode", 1, true, false, fc_binary_upgrade_set_next_index_relfilenode),
        builtin(3588, "binary_upgrade_set_next_toast_pg_class_oid", 1, true, false, fc_binary_upgrade_set_next_toast_pg_class_oid),
        builtin(4547, "binary_upgrade_set_next_toast_relfilenode", 1, true, false, fc_binary_upgrade_set_next_toast_relfilenode),
        builtin(3589, "binary_upgrade_set_next_pg_enum_oid", 1, true, false, fc_binary_upgrade_set_next_pg_enum_oid),
        builtin(3590, "binary_upgrade_set_next_pg_authid_oid", 1, true, false, fc_binary_upgrade_set_next_pg_authid_oid),
        builtin(4548, "binary_upgrade_set_next_pg_tablespace_oid", 1, true, false, fc_binary_upgrade_set_next_pg_tablespace_oid),
        builtin(4083, "binary_upgrade_set_record_init_privs", 1, true, false, fc_binary_upgrade_set_record_init_privs),
        builtin(4101, "binary_upgrade_set_missing_value", 3, true, false, fc_binary_upgrade_set_missing_value),
        builtin(6312, "binary_upgrade_logical_slot_has_caught_up", 1, true, false, fc_binary_upgrade_logical_slot_has_caught_up),
        // ---- rowtypes.c: record I/O (record_recv deferred — `internal`
        //      StringInfo arg0 is not on the fmgr frame) ----
        builtin(2290, "record_in", 3, true, false, fc_record_in),
        builtin(2291, "record_out", 1, true, false, fc_record_out),
        builtin(2403, "record_send", 1, true, false, fc_record_send),
        // ---- rowtypes.c: record comparison / btree support ----
        builtin(2981, "record_eq", 2, true, false, fc_record_eq),
        builtin(2982, "record_ne", 2, true, false, fc_record_ne),
        builtin(2983, "record_lt", 2, true, false, fc_record_lt),
        builtin(2984, "record_gt", 2, true, false, fc_record_gt),
        builtin(2985, "record_le", 2, true, false, fc_record_le),
        builtin(2986, "record_ge", 2, true, false, fc_record_ge),
        builtin(2987, "btrecordcmp", 2, true, false, fc_btrecordcmp),
        // ---- rowtypes.c: record image comparison ----
        builtin(3181, "record_image_eq", 2, true, false, fc_record_image_eq),
        builtin(3182, "record_image_ne", 2, true, false, fc_record_image_ne),
        builtin(3183, "record_image_lt", 2, true, false, fc_record_image_lt),
        builtin(3184, "record_image_gt", 2, true, false, fc_record_image_gt),
        builtin(3185, "record_image_le", 2, true, false, fc_record_image_le),
        builtin(3186, "record_image_ge", 2, true, false, fc_record_image_ge),
        // ---- rowtypes.c: record hashing ----
        builtin(6192, "hash_record", 1, true, false, fc_hash_record),
        builtin(6193, "hash_record_extended", 2, true, false, fc_hash_record_extended),
        // ---- rowtypes.c: record min/max aggregate transition helpers ----
        builtin(6375, "record_larger", 2, true, false, fc_record_larger),
        builtin(6376, "record_smaller", 2, true, false, fc_record_smaller),
        // ---- tid.c: ItemPointer type I/O, comparison, hashing, min/max ----
        builtin(48, "tidin", 1, true, false, fc_tidin),
        builtin(49, "tidout", 1, true, false, fc_tidout),
        builtin(2438, "tidrecv", 1, true, false, fc_tidrecv),
        builtin(2439, "tidsend", 1, true, false, fc_tidsend),
        builtin(1292, "tideq", 2, true, false, fc_tideq),
        builtin(1265, "tidne", 2, true, false, fc_tidne),
        builtin(2791, "tidlt", 2, true, false, fc_tidlt),
        builtin(2793, "tidle", 2, true, false, fc_tidle),
        builtin(2790, "tidgt", 2, true, false, fc_tidgt),
        builtin(2792, "tidge", 2, true, false, fc_tidge),
        builtin(2794, "bttidcmp", 2, true, false, fc_bttidcmp),
        builtin(2795, "tidlarger", 2, true, false, fc_tidlarger),
        builtin(2796, "tidsmaller", 2, true, false, fc_tidsmaller),
        builtin(2233, "hashtid", 1, true, false, fc_hashtid),
        builtin(2234, "hashtidextended", 2, true, false, fc_hashtidextended),
    ]);
}

// ===========================================================================
// Tests — drive the record by-ref builtins through the fmgr registry by OID,
// proving a composite `Datum` genuinely crosses the boundary via
// `RefPayload::Composite` (serialized with `FormedTuple::to_datum_image`,
// reconstructed with `from_datum_image` in the bridge / `arg_record`).
// ===========================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use std::vec;
    use std::vec::Vec;
    use backend_access_common_heaptuple::heap_form_tuple;
    use types_tuple::backend_access_common_heaptuple::Datum as CanonDatum;
    use types_tuple::heaptuple::{
        CompactAttribute, FormData_pg_attribute, TupleDescData,
    };

    /// A synthetic one-column `int4` composite type for the tests.
    const TEST_ROWTYPE_OID: u32 = 100_000;

    /// Build a fresh one-column `int4` `TupleDescData` in `mcx`. This is the
    /// descriptor our installed `lookup_rowtype_tupdesc` seam hands back, so the
    /// `deform_record` path in the comparison cores finds it.
    fn int4_tupdesc(mcx: mcx::Mcx<'_>) -> mcx::PgBox<'_, TupleDescData<'_>> {
        let mut att = FormData_pg_attribute::default();
        att.atttypid = 23; // INT4OID
        att.attlen = 4;
        att.attnum = 1;
        att.atttypmod = -1;
        att.attbyval = true;
        att.attalign = b'i' as i8;
        att.attstorage = b'p' as i8;
        att.attcollation = 0;

        let catt = CompactAttribute {
            attcacheoff: -1,
            attlen: 4,
            attbyval: true,
            attispackable: false,
            atthasmissing: false,
            attisdropped: false,
            attgenerated: false,
            attnullability: 0,
            attalignby: b'i',
        };

        let mut attrs = mcx::PgVec::new_in(mcx);
        attrs.push(att);
        let mut compact = mcx::PgVec::new_in(mcx);
        compact.push(catt);

        let td = TupleDescData {
            natts: 1,
            tdtypeid: TEST_ROWTYPE_OID,
            tdtypmod: -1,
            tdrefcount: -1,
            constr: None,
            compact_attrs: compact,
            attrs,
        };
        mcx::alloc_in(mcx, td).expect("alloc tupdesc")
    }

    /// Install the seams the comparison cores reach (idempotent across tests):
    /// `lookup_rowtype_tupdesc` (return our test descriptor) and the by-value
    /// `datum_image_eq_v` (word equality — the faithful by-value path of
    /// scalar-datum-core's real impl, which `record_image_eq` calls per column).
    /// Then register the misc2 builtins.
    fn setup() {
        // `std::sync::Once` so the one-shot seams install exactly once even when
        // the test threads race (the seam `::set` panics on a double install).
        static INSTALL: std::sync::Once = std::sync::Once::new();
        INSTALL.call_once(|| {
            backend_utils_cache_typcache_seams::lookup_rowtype_tupdesc::set(
                |mcx, type_id, _typmod| {
                    assert_eq!(type_id, TEST_ROWTYPE_OID, "test seam: unexpected rowtype");
                    Ok(int4_tupdesc(mcx))
                },
            );
            backend_utils_adt_datum_seams::datum_image_eq_v::set(|v1, v2, typ_byval, _typ_len| {
                // The int4 columns under test are by-value: word equality.
                assert!(typ_byval, "test datum_image_eq_v: only by-value path used");
                match (v1, v2) {
                    (CanonDatum::ByVal(a), CanonDatum::ByVal(b)) => Ok(a == b),
                    _ => panic!("test datum_image_eq_v: by-value attribute deformed as by-reference"),
                }
            });
        });
        register_misc2_builtins();
    }

    /// Build the flat `HeapTupleHeader` Datum image of a one-column `int4`
    /// composite holding `v`, via `heap_form_tuple` + `to_datum_image`.
    fn int4_record_image(v: i32) -> Vec<u8> {
        let m = scratch_mcx();
        let td = int4_tupdesc(m.mcx());
        let values = [CanonDatum::from_i32(v)];
        let nulls = [false];
        let tuple = heap_form_tuple(m.mcx(), &td, &values, &nulls)
            .expect("heap_form_tuple");
        tuple.to_datum_image()
    }

    /// Dispatch a 2-arg record comparison builtin by OID, passing two composite
    /// images on the by-ref `Composite` lane.
    fn call_record_cmp2(oid: u32, a: &[u8], b: &[u8]) -> Datum {
        setup();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 2, 0, None, None);
        fcinfo.args = vec![
            types_datum::NullableDatum::value(Datum::null()),
            types_datum::NullableDatum::value(Datum::null()),
        ];
        fcinfo.ref_args = vec![
            Some(RefPayload::Composite(a.to_vec())),
            Some(RefPayload::Composite(b.to_vec())),
        ];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(oid).expect("builtin registered");
        (entry.func.unwrap())(&mut fcinfo)
    }

    #[test]
    fn record_image_eq_through_registry() {
        let five = int4_record_image(5);
        let seven = int4_record_image(7);
        // record_image_eq (OID 3181) -> bool.
        assert!(call_record_cmp2(3181, &five, &five).as_bool());
        assert!(!call_record_cmp2(3181, &five, &seven).as_bool());
        // record_image_ne (OID 3182).
        assert!(call_record_cmp2(3182, &five, &seven).as_bool());
        assert!(!call_record_cmp2(3182, &five, &five).as_bool());
    }

    #[test]
    fn record_image_order_through_registry() {
        let five = int4_record_image(5);
        let seven = int4_record_image(7);
        // record_image_lt (3183) / gt (3184) / le (3185) / ge (3186).
        assert!(call_record_cmp2(3183, &five, &seven).as_bool());
        assert!(!call_record_cmp2(3183, &seven, &five).as_bool());
        assert!(call_record_cmp2(3184, &seven, &five).as_bool());
        assert!(call_record_cmp2(3185, &five, &five).as_bool());
        assert!(call_record_cmp2(3186, &seven, &seven).as_bool());
    }

    /// Prove the *result* Composite lane: the bridge's `ref_out_to_datum`
    /// reconstructs a `RefPayload::Composite` result back into a canonical
    /// `Datum::Composite`, and the round-trip preserves the row's typeid/typmod
    /// and user data (the same path `record_in`/`record_larger` results take).
    #[test]
    fn composite_result_lane_roundtrips() {
        let m = scratch_mcx();
        let td = int4_tupdesc(m.mcx());
        let values = [CanonDatum::from_i32(42)];
        let tuple = heap_form_tuple(m.mcx(), &td, &values, &[false]).expect("form");
        let image = tuple.to_datum_image();

        // Reconstruct exactly as the fmgr-core bridge does for a composite
        // result/argument.
        let rebuilt =
            types_tuple::backend_access_common_heaptuple::FormedTuple::from_datum_image(
                m.mcx(),
                &image,
            )
            .expect("from_datum_image");

        let hdr = rebuilt.tuple.t_data.as_ref().expect("header");
        assert_eq!(
            types_tuple::heaptuple::HeapTupleHeaderGetTypeId(hdr),
            TEST_ROWTYPE_OID
        );
        assert_eq!(
            types_tuple::heaptuple::HeapTupleHeaderGetTypMod(hdr),
            -1
        );
        // The user-data area (the int4 word) is preserved byte-for-byte.
        assert_eq!(rebuilt.data.as_slice(), tuple.data.as_slice());
        // And the re-serialized image is identical (full round-trip).
        assert_eq!(rebuilt.to_datum_image(), image);
    }

    /// Dispatch `tidin('(b,o)')` (OID 48) through the registry, returning the
    /// 6-byte ItemPointer image off the result by-ref lane.
    fn call_tidin(s: &str) -> Vec<u8> {
        setup();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![types_datum::NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Cstring(s.to_string()))];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(48).expect("tidin registered");
        (entry.func.unwrap())(&mut fcinfo);
        match fcinfo.take_ref_result().expect("tidin set a by-ref result") {
            RefPayload::Varlena(b) => b,
            other => panic!("tidin returned unexpected lane: {other:?}"),
        }
    }

    /// Dispatch `tidout(tid)` (OID 49) given the ItemPointer image, returning the
    /// rendered cstring.
    fn call_tidout(image: &[u8]) -> String {
        setup();
        let mut fcinfo = FunctionCallInfoBaseData::new(None, 1, 0, None, None);
        fcinfo.args = vec![types_datum::NullableDatum::value(Datum::null())];
        fcinfo.ref_args = vec![Some(RefPayload::Varlena(image.to_vec()))];
        let entry = backend_utils_fmgr_core::fmgr_isbuiltin(49).expect("tidout registered");
        (entry.func.unwrap())(&mut fcinfo);
        match fcinfo.take_ref_result().expect("tidout set a by-ref result") {
            RefPayload::Cstring(s) => s,
            other => panic!("tidout returned unexpected lane: {other:?}"),
        }
    }

    /// `'(0,1)'::tid` and `'(42,7)'::tid` dispatch through the fmgr registry and
    /// round-trip: tidin -> 6-byte image -> tidout reproduces the literal.
    #[test]
    fn tid_io_round_trips_through_registry() {
        for lit in ["(0,1)", "(42,7)", "(4294967295,65535)"] {
            let image = call_tidin(lit);
            assert_eq!(image.len(), 6, "ItemPointer image is 6 bytes");
            assert_eq!(call_tidout(&image), lit);
        }
    }

    /// `tideq`/`tidne` (OID 1292/1265) dispatch through the registry over two
    /// ItemPointer images.
    #[test]
    fn tid_eq_through_registry() {
        let a = call_tidin("(0,1)");
        let b = call_tidin("(0,2)");
        let call2 = |oid: u32, x: &[u8], y: &[u8]| -> bool {
            setup();
            let mut fcinfo = FunctionCallInfoBaseData::new(None, 2, 0, None, None);
            fcinfo.args = vec![
                types_datum::NullableDatum::value(Datum::null()),
                types_datum::NullableDatum::value(Datum::null()),
            ];
            fcinfo.ref_args =
                vec![Some(RefPayload::Varlena(x.to_vec())), Some(RefPayload::Varlena(y.to_vec()))];
            let entry = backend_utils_fmgr_core::fmgr_isbuiltin(oid).expect("registered");
            (entry.func.unwrap())(&mut fcinfo).as_bool()
        };
        assert!(call2(1292, &a, &a)); // tideq: (0,1) == (0,1)
        assert!(!call2(1292, &a, &b)); // tideq: (0,1) != (0,2)
        assert!(call2(1265, &a, &b)); // tidne
    }
}
