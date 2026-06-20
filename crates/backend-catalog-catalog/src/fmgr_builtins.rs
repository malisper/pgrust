//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! catalog functions in `catalog.c` whose argument/result types are
//! expressible at the current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word.
//! [`register_catalog_builtins`] registers every row into the fmgr-core builtin
//! table (C: `fmgr_builtins[]`), so by-OID dispatch resolves them. It is invoked
//! from this crate's `init_seams()`. OIDs / nargs / strict / retset are
//! transcribed exactly from `pg_proc.dat`.
//!
//! ```text
//! { oid => '275',  proname => 'pg_nextoid',
//!   prorettype => 'oid',  proargtypes => 'regclass name regclass',
//!   prosrc => 'pg_nextoid' },
//! { oid => '6241', proname => 'pg_stop_making_pinned_objects',
//!   prorettype => 'void', proargtypes => '',
//!   prosrc => 'pg_stop_making_pinned_objects' },
//! ```
//! Both default `proisstrict => 't'` (no key present) and neither is a set
//! returning function.

use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

use types_core::Oid;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_OID(i)` → `DatumGetObjectId`: the low 32 bits of arg `i`'s word.
/// `regclass` is `oid` at the storage level, so a `regclass` argument is read
/// exactly like an `oid`.
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo
        .arg(i)
        .expect("catalog fn: missing arg")
        .value
        .as_oid()
}

/// `PG_GETARG_NAME(i)` then `NameStr(*name)`: a `Name` arrives on the by-ref
/// lane as its full `NAMEDATALEN`-byte image (`namedata`); the SQL value is the
/// NUL-terminated C string at its head.
#[inline]
fn arg_name<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    let bytes = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("catalog fn: name arg missing from by-ref lane");
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    core::str::from_utf8(&bytes[..end]).expect("catalog fn: name arg is not valid UTF-8")
}

#[inline]
fn ret_oid(v: Oid) -> Datum {
    Datum::from_oid(v)
}

/// `PG_RETURN_VOID()`: the dummy result word for a `void`-returning function.
#[inline]
fn ret_void() -> Datum {
    Datum::from_usize(0)
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `Datum pg_nextoid(PG_FUNCTION_ARGS)`:
/// `reloid = PG_GETARG_OID(0); attname = PG_GETARG_NAME(1); idxoid = PG_GETARG_OID(2);`
fn fc_pg_nextoid(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let reloid = arg_oid(fcinfo, 0);
    let attname = arg_name(fcinfo, 1);
    let idxoid = arg_oid(fcinfo, 2);
    Ok(ret_oid(crate::pg_nextoid(reloid, attname, idxoid)?))
}

/// `Datum pg_stop_making_pinned_objects(PG_FUNCTION_ARGS)`: no fmgr arguments,
/// `PG_RETURN_VOID()`.
fn fc_pg_stop_making_pinned_objects(
    _fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    crate::pg_stop_making_pinned_objects()?;
    Ok(ret_void())
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

/// Register this crate's SQL-callable `catalog.c` builtins (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
pub fn register_catalog_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        builtin(275, "pg_nextoid", 3, true, false, fc_pg_nextoid),
        builtin(
            6241,
            "pg_stop_making_pinned_objects",
            0,
            true,
            false,
            fc_pg_stop_making_pinned_objects,
        ),
    ]);
}
