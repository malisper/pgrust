//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! object-introspection functions in `objectaddress.c` whose argument/result
//! types are expressible at the current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word /
//! by-reference payload. [`register_objectaddress_builtins`] registers every row
//! into the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch
//! resolves them. OIDs / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat`.

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
    fcinfo
        .arg(i)
        .expect("objectaddress fn: missing arg")
        .value
        .as_oid()
}

/// `PG_GETARG_INT32(i)` → `DatumGetInt32`.
#[inline]
fn arg_i32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo
        .arg(i)
        .expect("objectaddress fn: missing arg")
        .value
        .as_i32()
}

/// Set a `text` result on the by-ref lane (the owner of the `VARHDRSZ` framing
/// is the boundary, so the payload is the bare text bytes). C: `PG_RETURN_TEXT_P`.
#[inline]
fn ret_text(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    // `cstring_to_text`: build a header-ful `text` image (4-byte length word).
    let total = bytes.len() + 4;
    let mut img = Vec::with_capacity(total);
    img.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
    img.extend_from_slice(&bytes);
    fcinfo.set_ref_result(RefPayload::Varlena(img));
    Datum::from_usize(0)
}

/// `PG_RETURN_NULL()`.
#[inline]
fn ret_null(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    fcinfo.set_result_null(true);
    Datum::from_usize(0)
}

/// A scratch context for cores that allocate their result through `Mcx`.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("objectaddress fmgr scratch")
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    std::panic::panic_any(err);
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `pg_describe_object(classid oid, objid oid, objsubid int4) -> text`
/// (objectaddress.c 4220). The C wraps `getObjectDescription`'s cstring into a
/// `text` varlena, or returns NULL for "pinned" pg_depend items.
fn fc_pg_describe_object(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let classid = arg_oid(fcinfo, 0);
    let objid = arg_oid(fcinfo, 1);
    let objsubid = arg_i32(fcinfo, 2);

    let m = scratch_mcx();
    // Lower the borrowed `PgString` to an owned byte image while `m` is still
    // alive, then write to the frame (the `text` framing is the boundary's).
    let bytes: Option<Vec<u8>> =
        match crate::fmgr_sql::pg_describe_object(m.mcx(), classid, objid, objsubid) {
            Ok(Some(s)) => Some(s.as_bytes().to_vec()),
            Ok(None) => None,
            Err(e) => raise(e),
        };
    match bytes {
        Some(b) => ret_text(fcinfo, b),
        None => ret_null(fcinfo),
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

/// Register every SQL-callable `objectaddress.c` builtin whose boundary types
/// are expressible (C: their `fmgr_builtins[]` rows). Called from this crate's
/// `init_seams()`. OIDs/nargs/strict/retset transcribed exactly from
/// `pg_proc.dat`.
pub fn register_objectaddress_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // pg_describe_object: oid '3537', proargtypes 'oid oid int4',
        // prorettype 'text'; no proisstrict (=> strict false), no proretset.
        builtin(
            3537,
            "pg_describe_object",
            3,
            true,
            false,
            fc_pg_describe_object,
        ),
    ]);
}
