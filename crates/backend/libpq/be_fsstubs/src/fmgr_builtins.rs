//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! large-object (`lo_*`) functions in `be-fsstubs.c`.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching `be_lo_*` value core, and writes back the
//! result word / by-reference payload. [`register_be_fsstubs_builtins`]
//! registers every row into the fmgr-core builtin table (C: `fmgr_builtins[]`),
//! so by-OID dispatch resolves them. OIDs / nargs / strict / retset are
//! transcribed exactly from `pg_proc.dat`.
//!
//! The cores return `PgResult`; an `Err` is re-raised through the one dispatch
//! point every builtin crosses (`invoke_pgfunction`'s `catch_unwind`) via the
//! `PGRUST-SQLSTATE:` panic protocol, exactly like every other adt builtin.
//!
//! By-reference arguments/results: `text` filename and `bytea` content cross on
//! the by-ref lane as [`RefPayload::Varlena`]; the `bytea` results
//! (`loread`/`lo_get`/`lo_get_fragment`) are written back the same way.

use ::utils_error::PgResult;
use ::types_core::{int64, Oid};
use ::datum::Datum;
use ::fmgr::boundary::RefPayload;
use ::fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `VARHDRSZ` — the 4-byte uncompressed varlena length word.
const VARHDRSZ: usize = 4;

/// `PG_GETARG_OID(i)` → `DatumGetObjectId`: the low 32 bits of arg `i`'s word.
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo.arg(i).expect("lo fn: missing arg").value.as_oid()
}

/// `PG_GETARG_INT32(i)` → `DatumGetInt32`.
#[inline]
fn arg_i32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("lo fn: missing arg").value.as_i32()
}

/// `PG_GETARG_INT64(i)` → `DatumGetInt64`.
#[inline]
fn arg_i64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> int64 {
    fcinfo.arg(i).expect("lo fn: missing arg").value.as_i64()
}

/// `PG_GETARG_TEXTP`/`PG_GETARG_BYTEA_PP`(i): the varlena byte image on the
/// by-ref lane (the `text` filename / `bytea` content's `VARDATA_ANY`).
#[inline]
fn arg_varlena<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("lo fn: varlena arg missing from by-ref lane");
    // `VARDATA_ANY`: skip the 4-byte header on the header-ful image.
    if image.len() >= VARHDRSZ {
        &image[VARHDRSZ..]
    } else {
        &[]
    }
}

#[inline]
fn ret_oid(v: Oid) -> Datum {
    Datum::from_oid(v)
}
#[inline]
fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
}
#[inline]
fn ret_i64(v: int64) -> Datum {
    Datum::from_i64(v)
}

/// Set a `bytea` result on the by-ref lane and return the dummy word.
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    // `palloc(VARHDRSZ + len)` + `SET_VARSIZE`: build the header-ful image.
    let total = bytes.len() + VARHDRSZ;
    let mut img = Vec::with_capacity(total);
    img.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
    img.extend_from_slice(&bytes);
    fcinfo.set_ref_result(RefPayload::Varlena(img));
    Datum::from_usize(0)
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

fn fc_lo_create(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_oid(crate::be_lo_create(arg_oid(fcinfo, 0))?))
}

fn fc_lo_creat(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    // C: lo_creat(int4) ignores its argument (legacy mode flag).
    let _ = fcinfo;
    Ok(ret_oid(crate::be_lo_creat()?))
}

fn fc_lo_import(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let filename = arg_varlena(fcinfo, 0);
    Ok(ret_oid(crate::be_lo_import(filename)?))
}

fn fc_lo_import_with_oid(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let filename = arg_varlena(fcinfo, 0);
    let oid = arg_oid(fcinfo, 1);
    Ok(ret_oid(crate::be_lo_import_with_oid(filename, oid)?))
}

fn fc_lo_export(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let lobj_id = arg_oid(fcinfo, 0);
    let filename = arg_varlena(fcinfo, 1);
    Ok(ret_i32(crate::be_lo_export(lobj_id, filename)?))
}

fn fc_lo_open(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let lobj_id = arg_oid(fcinfo, 0);
    let mode = arg_i32(fcinfo, 1);
    Ok(ret_i32(crate::be_lo_open(lobj_id, mode)?))
}

fn fc_lo_close(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_i32(crate::be_lo_close(arg_i32(fcinfo, 0))?))
}

fn fc_loread(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let fd = arg_i32(fcinfo, 0);
    let len = arg_i32(fcinfo, 1);
    let bytes = crate::be_loread(fd, len)?;
    Ok(ret_varlena(fcinfo, bytes))
}

fn fc_lowrite(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let fd = arg_i32(fcinfo, 0);
    let wbuf = arg_varlena(fcinfo, 1);
    Ok(ret_i32(crate::be_lowrite(fd, wbuf)?))
}

fn fc_lo_lseek(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let fd = arg_i32(fcinfo, 0);
    let offset = arg_i32(fcinfo, 1);
    let whence = arg_i32(fcinfo, 2);
    Ok(ret_i32(crate::be_lo_lseek(fd, offset, whence)?))
}

fn fc_lo_lseek64(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let fd = arg_i32(fcinfo, 0);
    let offset = arg_i64(fcinfo, 1);
    let whence = arg_i32(fcinfo, 2);
    Ok(ret_i64(crate::be_lo_lseek64(fd, offset, whence)?))
}

fn fc_lo_tell(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_i32(crate::be_lo_tell(arg_i32(fcinfo, 0))?))
}

fn fc_lo_tell64(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_i64(crate::be_lo_tell64(arg_i32(fcinfo, 0))?))
}

fn fc_lo_unlink(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    Ok(ret_i32(crate::be_lo_unlink(arg_oid(fcinfo, 0))?))
}

fn fc_lo_truncate(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let fd = arg_i32(fcinfo, 0);
    let len = arg_i32(fcinfo, 1);
    Ok(ret_i32(crate::be_lo_truncate(fd, len)?))
}

fn fc_lo_truncate64(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let fd = arg_i32(fcinfo, 0);
    let len = arg_i64(fcinfo, 1);
    Ok(ret_i32(crate::be_lo_truncate64(fd, len)?))
}

fn fc_lo_from_bytea(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let lo_oid = arg_oid(fcinfo, 0);
    let bytes = arg_varlena(fcinfo, 1);
    Ok(ret_oid(crate::be_lo_from_bytea(lo_oid, bytes)?))
}

fn fc_lo_get(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let lo_oid = arg_oid(fcinfo, 0);
    let bytes = crate::be_lo_get(lo_oid)?;
    Ok(ret_varlena(fcinfo, bytes))
}

fn fc_lo_get_fragment(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let lo_oid = arg_oid(fcinfo, 0);
    let offset = arg_i64(fcinfo, 1);
    let nbytes = arg_i32(fcinfo, 2);
    let bytes = crate::be_lo_get_fragment(lo_oid, offset, nbytes)?;
    Ok(ret_varlena(fcinfo, bytes))
}

fn fc_lo_put(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let lo_oid = arg_oid(fcinfo, 0);
    let offset = arg_i64(fcinfo, 1);
    let str = arg_varlena(fcinfo, 2);
    crate::be_lo_put(lo_oid, offset, str)?;
    // C: PG_RETURN_VOID() == (Datum) 0.
    Ok(Datum::from_usize(0))
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

/// Register every SQL-callable `be-fsstubs.c` `lo_*` builtin (C: their
/// `fmgr_builtins[]` rows). Called from this crate's [`crate::init_seams`].
/// OIDs/nargs/strict/retset transcribed exactly from `pg_proc.dat` (all are
/// `proisstrict => 't'` by default, none `proretset`).
pub fn register_be_fsstubs_builtins() {
    fmgr_core::register_builtins_native([
        builtin(715, "be_lo_create", 1, true, false, fc_lo_create),
        builtin(764, "be_lo_import", 1, true, false, fc_lo_import),
        builtin(765, "be_lo_export", 2, true, false, fc_lo_export),
        builtin(767, "be_lo_import_with_oid", 2, true, false, fc_lo_import_with_oid),
        builtin(952, "be_lo_open", 2, true, false, fc_lo_open),
        builtin(953, "be_lo_close", 1, true, false, fc_lo_close),
        builtin(954, "be_loread", 2, true, false, fc_loread),
        builtin(955, "be_lowrite", 2, true, false, fc_lowrite),
        builtin(956, "be_lo_lseek", 3, true, false, fc_lo_lseek),
        builtin(957, "be_lo_creat", 1, true, false, fc_lo_creat),
        builtin(958, "be_lo_tell", 1, true, false, fc_lo_tell),
        builtin(964, "be_lo_unlink", 1, true, false, fc_lo_unlink),
        builtin(1004, "be_lo_truncate", 2, true, false, fc_lo_truncate),
        builtin(3170, "be_lo_lseek64", 3, true, false, fc_lo_lseek64),
        builtin(3171, "be_lo_tell64", 1, true, false, fc_lo_tell64),
        builtin(3172, "be_lo_truncate64", 2, true, false, fc_lo_truncate64),
        builtin(3457, "be_lo_from_bytea", 2, true, false, fc_lo_from_bytea),
        builtin(3458, "be_lo_get", 1, true, false, fc_lo_get),
        builtin(3459, "be_lo_get_fragment", 3, true, false, fc_lo_get_fragment),
        builtin(3460, "be_lo_put", 3, true, false, fc_lo_put),
    ]);
}
