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
    let bytes = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("misc2 fn: text arg missing from by-ref lane");
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
            let image = bytes.as_slice();
            let payload = image
                .get(types_datum::varlena::VARHDRSZ..)
                .unwrap_or(&[])
                .to_vec();
            fcinfo.set_ref_result(RefPayload::Varlena(payload));
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
    let chars = types_error::unpack_sqlstate(err.sqlstate());
    let code = core::str::from_utf8(&chars).unwrap_or("XX000");
    std::panic::panic_any(std::format!("PGRUST-SQLSTATE:{code}:{}", err.message()));
}

// ===========================================================================
// regproc.c — reg* alias-type I/O + to_reg* lookups.
// ===========================================================================

/// Generic `reg*in(cstring)` adapter: the value core takes `(mcx, &str,
/// Option<&mut SoftErrorContext>)` and returns `PgResult<Option<Oid>>`. At this
/// boundary the soft-error context is `None` (a hard parse — `fcinfo->context`
/// soft folding is not modeled on the frame, matching every other adt `_in`).
fn fc_regin(
    fcinfo: &mut FunctionCallInfoBaseData,
    core: fn(mcx::Mcx<'_>, &str, Option<&mut types_error::SoftErrorContext>) -> types_error::PgResult<Option<Oid>>,
) -> Datum {
    let s = arg_cstring(fcinfo, 0);
    let m = scratch_mcx();
    match core(m.mcx(), s, None) {
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
        builtin(44, "regprocin", 1, false, false, fc_regprocin),
        builtin(45, "regprocout", 1, false, false, fc_regprocout),
        builtin(2212, "regprocedurein", 1, false, false, fc_regprocedurein),
        builtin(2213, "regprocedureout", 1, false, false, fc_regprocedureout),
        builtin(2214, "regoperin", 1, false, false, fc_regoperin),
        builtin(2215, "regoperout", 1, false, false, fc_regoperout),
        builtin(2216, "regoperatorin", 1, false, false, fc_regoperatorin),
        builtin(2217, "regoperatorout", 1, false, false, fc_regoperatorout),
        builtin(2218, "regclassin", 1, false, false, fc_regclassin),
        builtin(2219, "regclassout", 1, false, false, fc_regclassout),
        builtin(2220, "regtypein", 1, false, false, fc_regtypein),
        builtin(2221, "regtypeout", 1, false, false, fc_regtypeout),
        builtin(3736, "regconfigin", 1, false, false, fc_regconfigin),
        builtin(3737, "regconfigout", 1, false, false, fc_regconfigout),
        builtin(3771, "regdictionaryin", 1, false, false, fc_regdictionaryin),
        builtin(3772, "regdictionaryout", 1, false, false, fc_regdictionaryout),
        builtin(4084, "regnamespacein", 1, false, false, fc_regnamespacein),
        builtin(4085, "regnamespaceout", 1, false, false, fc_regnamespaceout),
        builtin(4092, "regroleout", 1, false, false, fc_regroleout),
        builtin(4098, "regrolein", 1, false, false, fc_regrolein),
        builtin(4193, "regcollationin", 1, false, false, fc_regcollationin),
        builtin(4194, "regcollationout", 1, false, false, fc_regcollationout),
        // ---- regproc.c: text->reg implicit cast + to_reg* lookups ----
        builtin(1079, "regclass", 1, false, false, fc_text_regclass),
        builtin(3476, "to_regoperator", 1, false, false, fc_to_regoperator),
        builtin(3479, "to_regprocedure", 1, false, false, fc_to_regprocedure),
        builtin(3492, "to_regoper", 1, false, false, fc_to_regoper),
        builtin(3493, "to_regtype", 1, false, false, fc_to_regtype),
        builtin(3494, "to_regproc", 1, false, false, fc_to_regproc),
        builtin(3495, "to_regclass", 1, false, false, fc_to_regclass),
        builtin(4086, "to_regnamespace", 1, false, false, fc_to_regnamespace),
        builtin(4093, "to_regrole", 1, false, false, fc_to_regrole),
        builtin(4195, "to_regcollation", 1, false, false, fc_to_regcollation),
        builtin(6317, "to_regtypemod", 1, false, false, fc_to_regtypemod),
        // ---- genfile.c: pg_read_file / pg_read_binary_file ----
        builtin(2624, "pg_read_file", 3, false, false, fc_pg_read_file_off_len),
        builtin(3293, "pg_read_file", 4, false, false, fc_pg_read_file_off_len_missing),
        builtin(3826, "pg_read_file", 1, false, false, fc_pg_read_file_all),
        builtin(6208, "pg_read_file", 2, false, false, fc_pg_read_file_all_missing),
        builtin(3827, "pg_read_binary_file", 3, false, false, fc_pg_read_binary_file_off_len),
        builtin(3295, "pg_read_binary_file", 4, false, false, fc_pg_read_binary_file_off_len_missing),
        builtin(3828, "pg_read_binary_file", 1, false, false, fc_pg_read_binary_file_all),
        builtin(6209, "pg_read_binary_file", 2, false, false, fc_pg_read_binary_file_all_missing),
        // ---- partitionfuncs.c ----
        builtin(3424, "pg_partition_root", 1, false, false, fc_pg_partition_root),
        // ---- lockfuncs.c: advisory locks (int8) ----
        builtin(2880, "pg_advisory_lock", 1, false, false, fc_pg_advisory_lock_int8),
        builtin(2881, "pg_advisory_lock_shared", 1, false, false, fc_pg_advisory_lock_shared_int8),
        builtin(2882, "pg_try_advisory_lock", 1, false, false, fc_pg_try_advisory_lock_int8),
        builtin(2883, "pg_try_advisory_lock_shared", 1, false, false, fc_pg_try_advisory_lock_shared_int8),
        builtin(2884, "pg_advisory_unlock", 1, false, false, fc_pg_advisory_unlock_int8),
        builtin(2885, "pg_advisory_unlock_shared", 1, false, false, fc_pg_advisory_unlock_shared_int8),
        builtin(3089, "pg_advisory_xact_lock", 1, false, false, fc_pg_advisory_xact_lock_int8),
        builtin(3090, "pg_advisory_xact_lock_shared", 1, false, false, fc_pg_advisory_xact_lock_shared_int8),
        builtin(3091, "pg_try_advisory_xact_lock", 1, false, false, fc_pg_try_advisory_xact_lock_int8),
        builtin(3092, "pg_try_advisory_xact_lock_shared", 1, false, false, fc_pg_try_advisory_xact_lock_shared_int8),
        // ---- lockfuncs.c: advisory locks (int4, int4) ----
        builtin(2886, "pg_advisory_lock", 2, false, false, fc_pg_advisory_lock_int4),
        builtin(2887, "pg_advisory_lock_shared", 2, false, false, fc_pg_advisory_lock_shared_int4),
        builtin(2888, "pg_try_advisory_lock", 2, false, false, fc_pg_try_advisory_lock_int4),
        builtin(2889, "pg_try_advisory_lock_shared", 2, false, false, fc_pg_try_advisory_lock_shared_int4),
        builtin(2890, "pg_advisory_unlock", 2, false, false, fc_pg_advisory_unlock_int4),
        builtin(2891, "pg_advisory_unlock_shared", 2, false, false, fc_pg_advisory_unlock_shared_int4),
        builtin(3093, "pg_advisory_xact_lock", 2, false, false, fc_pg_advisory_xact_lock_int4),
        builtin(3094, "pg_advisory_xact_lock_shared", 2, false, false, fc_pg_advisory_xact_lock_shared_int4),
        builtin(3095, "pg_try_advisory_xact_lock", 2, false, false, fc_pg_try_advisory_xact_lock_int4),
        builtin(3096, "pg_try_advisory_xact_lock_shared", 2, false, false, fc_pg_try_advisory_xact_lock_shared_int4),
        builtin(2892, "pg_advisory_unlock_all", 0, false, false, fc_pg_advisory_unlock_all),
        // ---- pg_upgrade_support.c ----
        builtin(3582, "binary_upgrade_set_next_pg_type_oid", 1, false, false, fc_binary_upgrade_set_next_pg_type_oid),
        builtin(3584, "binary_upgrade_set_next_array_pg_type_oid", 1, false, false, fc_binary_upgrade_set_next_array_pg_type_oid),
        builtin(4390, "binary_upgrade_set_next_multirange_pg_type_oid", 1, false, false, fc_binary_upgrade_set_next_multirange_pg_type_oid),
        builtin(4391, "binary_upgrade_set_next_multirange_array_pg_type_oid", 1, false, false, fc_binary_upgrade_set_next_multirange_array_pg_type_oid),
        builtin(3586, "binary_upgrade_set_next_heap_pg_class_oid", 1, false, false, fc_binary_upgrade_set_next_heap_pg_class_oid),
        builtin(4545, "binary_upgrade_set_next_heap_relfilenode", 1, false, false, fc_binary_upgrade_set_next_heap_relfilenode),
        builtin(3587, "binary_upgrade_set_next_index_pg_class_oid", 1, false, false, fc_binary_upgrade_set_next_index_pg_class_oid),
        builtin(4546, "binary_upgrade_set_next_index_relfilenode", 1, false, false, fc_binary_upgrade_set_next_index_relfilenode),
        builtin(3588, "binary_upgrade_set_next_toast_pg_class_oid", 1, false, false, fc_binary_upgrade_set_next_toast_pg_class_oid),
        builtin(4547, "binary_upgrade_set_next_toast_relfilenode", 1, false, false, fc_binary_upgrade_set_next_toast_relfilenode),
        builtin(3589, "binary_upgrade_set_next_pg_enum_oid", 1, false, false, fc_binary_upgrade_set_next_pg_enum_oid),
        builtin(3590, "binary_upgrade_set_next_pg_authid_oid", 1, false, false, fc_binary_upgrade_set_next_pg_authid_oid),
        builtin(4548, "binary_upgrade_set_next_pg_tablespace_oid", 1, false, false, fc_binary_upgrade_set_next_pg_tablespace_oid),
        builtin(4083, "binary_upgrade_set_record_init_privs", 1, false, false, fc_binary_upgrade_set_record_init_privs),
        builtin(4101, "binary_upgrade_set_missing_value", 3, false, false, fc_binary_upgrade_set_missing_value),
        builtin(6312, "binary_upgrade_logical_slot_has_caught_up", 1, false, false, fc_binary_upgrade_logical_slot_has_caught_up),
    ]);
}
