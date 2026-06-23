//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! functions in `pg_publication.c`.
//!
//! Currently just `pg_relation_is_publishable(oid) -> bool`, which psql's
//! `\d`/`\d+` describe path calls to decide whether to list a relation's
//! publication memberships. Each entry is a `fc_<name>` adapter that reads its
//! arguments off the fmgr call frame, calls the matching value core, and writes
//! back the result word. [`register_pg_publication_builtins`] registers every
//! row into the fmgr-core builtin table (C: `fmgr_builtins[]`) so by-OID
//! dispatch resolves them. OIDs / nargs / strict / retset are transcribed
//! exactly from `pg_proc.dat`.

use ::datum::Datum;
use fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

use ::types_core::Oid;

/// `PG_GETARG_OID(i)` → `DatumGetObjectId`: the low 32 bits of arg `i`'s word.
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo
        .arg(i)
        .expect("pg_publication fn: missing arg")
        .value
        .as_oid()
}

/// A scratch context for the core's syscache read.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("pg_publication fmgr scratch")
}

/// `pg_relation_is_publishable(PG_FUNCTION_ARGS)` (pg_publication.c). The core
/// returns `Option<bool>`: `None` is the C `PG_RETURN_NULL()` (the relation
/// vanished from the catalog mid-call), which maps onto `fcinfo->isnull`.
fn fc_pg_relation_is_publishable(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let relid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    match crate::pg_relation_is_publishable(m.mcx(), relid)? {
        Some(b) => Ok(Datum::from_bool(b)),
        None => {
            fcinfo.set_result_null(true);
            Ok(Datum::from_usize(0))
        }
    }
}

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

/// Register every `pg_publication.c` SQL-callable builtin (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
/// OID/nargs/strict from `pg_proc.dat` (`proisstrict` default `t`, not
/// `proretset`).
pub fn register_pg_publication_builtins() {
    fmgr_core::register_builtins_native([builtin(
        6121,
        "pg_relation_is_publishable",
        1,
        true,
        false,
        fc_pg_relation_is_publishable,
    )]);
}
