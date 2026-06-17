//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! schema-visibility functions in `namespace.c` (`pg_*_is_visible`,
//! `pg_my_temp_schema`, `pg_is_other_temp_schema`).
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word.
//! [`register_namespace_builtins`] registers every row into the fmgr-core
//! builtin table (C: `fmgr_builtins[]`) so by-OID dispatch resolves them.
//! OIDs / nargs / strict / retset are transcribed exactly from `pg_proc.dat`
//! (all `proisstrict => 't'` by default, none `proretset`).
//!
//! The `*_is_visible` cores return `Option<bool>` (C: the SQL-NULL the C
//! versions produce via `PG_RETURN_NULL()` when the object has vanished from
//! the catalog mid-call); that maps onto `fcinfo->isnull` here.

use types_datum::Datum;
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
        .expect("namespace fn: missing arg")
        .value
        .as_oid()
}

#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}
#[inline]
fn ret_oid(v: Oid) -> Datum {
    Datum::from_oid(v)
}

/// A scratch context for cores that allocate / read through `Mcx`.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("namespace fmgr scratch")
}

/// Raise a core's `ereport(ERROR)` through the one dispatch point every builtin
/// crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    let chars = types_error::unpack_sqlstate(err.sqlstate());
    let code = core::str::from_utf8(&chars).unwrap_or("XX000");
    std::panic::panic_any(format!("PGRUST-SQLSTATE:{code}:{}", err.message()));
}

/// Run one of the `Option<bool>`-returning `pg_*_is_visible` cores: write the
/// bool result word, or set `fcinfo->isnull` for the SQL-NULL (object gone).
#[inline]
fn vis(
    fcinfo: &mut FunctionCallInfoBaseData,
    core: impl FnOnce(mcx::Mcx<'_>, Oid) -> types_error::PgResult<Option<bool>>,
) -> Datum {
    let oid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    match core(m.mcx(), oid) {
        Ok(Some(b)) => ret_bool(b),
        Ok(None) => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
        Err(e) => raise(e),
    }
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

fn fc_pg_table_is_visible(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    vis(fcinfo, crate::pg_table_is_visible)
}
fn fc_pg_type_is_visible(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    vis(fcinfo, crate::pg_type_is_visible)
}
fn fc_pg_function_is_visible(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    vis(fcinfo, crate::pg_function_is_visible)
}
fn fc_pg_operator_is_visible(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    vis(fcinfo, crate::pg_operator_is_visible)
}
fn fc_pg_opclass_is_visible(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    vis(fcinfo, crate::pg_opclass_is_visible)
}
fn fc_pg_opfamily_is_visible(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    vis(fcinfo, crate::pg_opfamily_is_visible)
}
fn fc_pg_collation_is_visible(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    vis(fcinfo, crate::pg_collation_is_visible)
}
fn fc_pg_conversion_is_visible(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    vis(fcinfo, crate::pg_conversion_is_visible)
}
fn fc_pg_statistics_obj_is_visible(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    vis(fcinfo, crate::pg_statistics_obj_is_visible)
}
fn fc_pg_ts_parser_is_visible(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    vis(fcinfo, crate::pg_ts_parser_is_visible)
}
fn fc_pg_ts_dict_is_visible(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    vis(fcinfo, crate::pg_ts_dict_is_visible)
}
fn fc_pg_ts_template_is_visible(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    vis(fcinfo, crate::pg_ts_template_is_visible)
}
fn fc_pg_ts_config_is_visible(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    vis(fcinfo, crate::pg_ts_config_is_visible)
}

fn fc_pg_my_temp_schema(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    ret_oid(crate::pg_my_temp_schema())
}

fn fc_pg_is_other_temp_schema(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let oid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    match crate::pg_is_other_temp_schema(m.mcx(), oid) {
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

/// Register every `namespace.c` schema-visibility builtin (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
/// OIDs/nargs/strict from `pg_proc.dat` (all `proisstrict` default `t`, none
/// `proretset`).
pub fn register_namespace_builtins() {
    backend_utils_fmgr_core::register_builtins([
        builtin(2079, "pg_table_is_visible", 1, true, false, fc_pg_table_is_visible),
        builtin(2080, "pg_type_is_visible", 1, true, false, fc_pg_type_is_visible),
        builtin(2081, "pg_function_is_visible", 1, true, false, fc_pg_function_is_visible),
        builtin(2082, "pg_operator_is_visible", 1, true, false, fc_pg_operator_is_visible),
        builtin(2083, "pg_opclass_is_visible", 1, true, false, fc_pg_opclass_is_visible),
        builtin(3829, "pg_opfamily_is_visible", 1, true, false, fc_pg_opfamily_is_visible),
        builtin(2093, "pg_conversion_is_visible", 1, true, false, fc_pg_conversion_is_visible),
        builtin(
            3403,
            "pg_statistics_obj_is_visible",
            1,
            true,
            false,
            fc_pg_statistics_obj_is_visible,
        ),
        builtin(3756, "pg_ts_parser_is_visible", 1, true, false, fc_pg_ts_parser_is_visible),
        builtin(3757, "pg_ts_dict_is_visible", 1, true, false, fc_pg_ts_dict_is_visible),
        builtin(3768, "pg_ts_template_is_visible", 1, true, false, fc_pg_ts_template_is_visible),
        builtin(3758, "pg_ts_config_is_visible", 1, true, false, fc_pg_ts_config_is_visible),
        builtin(3815, "pg_collation_is_visible", 1, true, false, fc_pg_collation_is_visible),
        builtin(2854, "pg_my_temp_schema", 0, true, false, fc_pg_my_temp_schema),
        builtin(2855, "pg_is_other_temp_schema", 1, true, false, fc_pg_is_other_temp_schema),
    ]);
}
