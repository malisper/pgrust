//! `backend-pl-plpgsql-handler` — the PL/pgSQL fmgr-facing handler
//! (`pl_handler.c`), the top (7th) layer of the PL/pgSQL subsystem.
//!
//! Ports the three `PG_FUNCTION_INFO_V1` entry points the function manager and
//! `ExecuteDoStmt` dispatch to — [`plpgsql_call_handler`] (LANGUAGE plpgsql
//! function/procedure calls), [`plpgsql_inline_handler`] (`DO` blocks), and
//! [`plpgsql_validator`] (CREATE FUNCTION-time body checking) — plus the
//! module-load initialization [`_pg_init`] (custom GUCs, the xact/subxact
//! callback registration, the plugin rendezvous) and the two GUC check/assign
//! hooks for `plpgsql.extra_warnings` / `plpgsql.extra_errors`.
//!
//! [`init_seams`] installs `execute_inline_handler`
//! (`backend-commands-functioncmds-seams`) — the DO-block dispatch the
//! functioncmds `ExecuteDoStmt` reaches — and registers the three handler
//! `PGFunction`s in the fmgr built-in registry by name, so a C-language
//! `pg_proc` row (`prosrc = "plpgsql_call_handler"` / …) resolves here.
//!
//! ## What runs vs. what is loud
//!
//! A `DO`/function body of pure control flow (empty block, IF/CASE/LOOP with no
//! SQL or expression evaluation that reaches the value substrate) runs
//! end-to-end: handler → `plpgsql_compile_inline` → `plpgsql_exec_function` →
//! `exec_toplevel_block`. Any leg that evaluates a SQL expression, runs a query
//! through SPI, or assigns a computed value bottoms out in the exec value
//! substrate (the executor `ExprState` simple-expr path / the SPI plan surface)
//! and panics loudly there — faithful: a C build would reach exactly those
//! callees. `_pg_init`'s GUC-registration and xact-callback-registration are
//! likewise routed to loud seams (the custom-GUC `DefineCustom*Variable`
//! substrate and `plpgsql_xact_cb`/`_subxact_cb` — defined in `pl_exec.c` — are
//! not yet ported); the compile-time GUC defaults already match the C source
//! defaults, so the compiler reads correct values without `_pg_init`.

#![allow(non_camel_case_types, non_snake_case)]

mod seam;

use types_datum::Datum;
use types_error::PgResult;
use types_fmgr::fmgr::FunctionCallInfoBaseData;
use types_fmgr::BuiltinFunction;
use types_parsenodes::InlineCodeBlock;
use types_plpgsql::{
    int32, PLpgSQL_resolve_option, PLPGSQL_XCHECK_ALL, PLPGSQL_XCHECK_NONE,
    PLPGSQL_XCHECK_SHADOWVAR, PLPGSQL_XCHECK_STRICTMULTIASSIGNMENT, PLPGSQL_XCHECK_TOOMANYROWS,
};

use backend_executor_spi::{SPI_connect_ext, SPI_finish, SPI_OK_FINISH, SPI_OPT_NONATOMIC};
use backend_pl_plpgsql_exec::{FunctionCallArg, FunctionResult};

// ---------------------------------------------------------------------------
// Custom GUC variables (pl_handler.c module globals).
//
// In C these are pl_handler.c process globals the GUC machinery populates and
// `pl_comp.c` reads. In the owned model the compiler crate
// (backend-pl-plpgsql-comp) holds the same globals privately, seeded with the
// identical C-source defaults (PLPGSQL_RESOLVE_ERROR / false / true / 0 / 0);
// the assign hooks below write into the handler-local mirrors used for
// `_pg_init` / fmgr-side reads. Both stay in sync via the assign hooks.
// ---------------------------------------------------------------------------

use core::cell::Cell;

thread_local! {
    /// `int plpgsql_variable_conflict = PLPGSQL_RESOLVE_ERROR;`
    static PLPGSQL_VARIABLE_CONFLICT: Cell<PLpgSQL_resolve_option> =
        const { Cell::new(PLpgSQL_resolve_option::PLPGSQL_RESOLVE_ERROR) };
    /// `bool plpgsql_print_strict_params = false;`
    static PLPGSQL_PRINT_STRICT_PARAMS: Cell<bool> = const { Cell::new(false) };
    /// `bool plpgsql_check_asserts = true;`
    static PLPGSQL_CHECK_ASSERTS: Cell<bool> = const { Cell::new(true) };
    /// `int plpgsql_extra_warnings;` (default "none" => PLPGSQL_XCHECK_NONE)
    static PLPGSQL_EXTRA_WARNINGS: Cell<int32> = const { Cell::new(PLPGSQL_XCHECK_NONE) };
    /// `int plpgsql_extra_errors;` (default "none" => PLPGSQL_XCHECK_NONE)
    static PLPGSQL_EXTRA_ERRORS: Cell<int32> = const { Cell::new(PLPGSQL_XCHECK_NONE) };
}

/// Read `plpgsql_variable_conflict`.
pub fn plpgsql_variable_conflict() -> PLpgSQL_resolve_option {
    PLPGSQL_VARIABLE_CONFLICT.with(Cell::get)
}
/// Read `plpgsql_print_strict_params`.
pub fn plpgsql_print_strict_params() -> bool {
    PLPGSQL_PRINT_STRICT_PARAMS.with(Cell::get)
}
/// Read `plpgsql_check_asserts`.
pub fn plpgsql_check_asserts() -> bool {
    PLPGSQL_CHECK_ASSERTS.with(Cell::get)
}
/// Read `plpgsql_extra_warnings`.
pub fn plpgsql_extra_warnings() -> int32 {
    PLPGSQL_EXTRA_WARNINGS.with(Cell::get)
}
/// Read `plpgsql_extra_errors`.
pub fn plpgsql_extra_errors() -> int32 {
    PLPGSQL_EXTRA_ERRORS.with(Cell::get)
}

// ---------------------------------------------------------------------------
// GUC check / assign hooks for plpgsql.extra_warnings / plpgsql.extra_errors.
// ---------------------------------------------------------------------------

/// `plpgsql_extra_checks_check_hook(char **newvalue, void **extra, GucSource)`
/// (pl_handler.c) — the GUC *check* hook shared by `plpgsql.extra_warnings` and
/// `plpgsql.extra_errors`. Parses the comma-separated identifier list into the
/// `PLPGSQL_XCHECK_*` bitmask the assign hook will store.
///
/// Returns `Ok(bitmask)` for a valid value, or `Err(detail)` (the C
/// `GUC_check_errdetail` text) on a list-syntax / unrecognized-keyword error.
/// `mcx` backs the `SplitIdentifierString` scratch parse.
pub fn plpgsql_extra_checks_check_hook(
    mcx: mcx::Mcx<'_>,
    newvalue: &str,
) -> PgResult<Result<int32, String>> {
    let mut extrachecks: int32 = 0;

    if newvalue.eq_ignore_ascii_case("all") {
        extrachecks = PLPGSQL_XCHECK_ALL;
    } else if newvalue.eq_ignore_ascii_case("none") {
        extrachecks = PLPGSQL_XCHECK_NONE;
    } else {
        // Parse string into list of identifiers (C: pstrdup + SplitIdentifierString).
        let elemlist = match backend_utils_adt_varlena::split_format::split_identifier_string(
            mcx, newvalue, ',',
        )? {
            Some(list) => list,
            None => return Ok(Err(String::from("List syntax is invalid."))),
        };

        for tok in elemlist.iter() {
            let tok = tok.as_str();
            if tok.eq_ignore_ascii_case("shadowed_variables") {
                extrachecks |= PLPGSQL_XCHECK_SHADOWVAR;
            } else if tok.eq_ignore_ascii_case("too_many_rows") {
                extrachecks |= PLPGSQL_XCHECK_TOOMANYROWS;
            } else if tok.eq_ignore_ascii_case("strict_multi_assignment") {
                extrachecks |= PLPGSQL_XCHECK_STRICTMULTIASSIGNMENT;
            } else if tok.eq_ignore_ascii_case("all") || tok.eq_ignore_ascii_case("none") {
                return Ok(Err(format!(
                    "Key word \"{tok}\" cannot be combined with other key words."
                )));
            } else {
                return Ok(Err(format!("Unrecognized key word: \"{tok}\".")));
            }
        }
    }

    Ok(Ok(extrachecks))
}

/// `plpgsql_extra_warnings_assign_hook(const char *newvalue, void *extra)`
/// (pl_handler.c) — store the checked bitmask into `plpgsql_extra_warnings`.
pub fn plpgsql_extra_warnings_assign_hook(extra: int32) {
    PLPGSQL_EXTRA_WARNINGS.with(|c| c.set(extra));
}

/// `plpgsql_extra_errors_assign_hook(const char *newvalue, void *extra)`
/// (pl_handler.c) — store the checked bitmask into `plpgsql_extra_errors`.
pub fn plpgsql_extra_errors_assign_hook(extra: int32) {
    PLPGSQL_EXTRA_ERRORS.with(|c| c.set(extra));
}

// ---------------------------------------------------------------------------
// _PG_init — library load-time initialization (pl_handler.c).
// ---------------------------------------------------------------------------

thread_local! {
    /// `static bool inited = false;` inside `_PG_init`.
    static PG_INIT_INITED: Cell<bool> = const { Cell::new(false) };
}

/// `_PG_init(void)` (pl_handler.c) — library load-time initialization.
///
/// Defines the custom GUCs, marks the `plpgsql` prefix reserved, registers the
/// transaction/subtransaction callbacks, and sets up the plugin rendezvous. The
/// custom-GUC registration (`DefineCustom*Variable` / `MarkGUCPrefixReserved`),
/// the xact-callback registration (`RegisterXactCallback(plpgsql_xact_cb)` /
/// `RegisterSubXactCallback(plpgsql_subxact_cb)` — those callbacks live in
/// `pl_exec.c` and are not yet ported), and the rendezvous lookup route through
/// [`seam`] (loud until that substrate lands). The compile-time GUC defaults
/// already match the C source, so the compiler reads correct values even before
/// `_pg_init` fires.
pub fn _pg_init() {
    // Be sure we do initialization only once (should be redundant now).
    if PG_INIT_INITED.with(Cell::get) {
        return;
    }

    seam::pg_bindtextdomain();

    // DefineCustomEnumVariable("plpgsql.variable_conflict", …, &plpgsql_variable_conflict, …)
    seam::define_custom_enum_variable_variable_conflict();
    // DefineCustomBoolVariable("plpgsql.print_strict_params", …)
    seam::define_custom_bool_variable_print_strict_params();
    // DefineCustomBoolVariable("plpgsql.check_asserts", …)
    seam::define_custom_bool_variable_check_asserts();
    // DefineCustomStringVariable("plpgsql.extra_warnings", …, check/assign hooks)
    seam::define_custom_string_variable_extra_warnings();
    // DefineCustomStringVariable("plpgsql.extra_errors", …, check/assign hooks)
    seam::define_custom_string_variable_extra_errors();

    seam::mark_guc_prefix_reserved("plpgsql");

    // RegisterXactCallback(plpgsql_xact_cb, NULL);
    seam::register_xact_callback();
    // RegisterSubXactCallback(plpgsql_subxact_cb, NULL);
    seam::register_subxact_callback();

    // Set up a rendezvous point with optional instrumentation plugin.
    seam::find_rendezvous_variable_plpgsql_plugin();

    PG_INIT_INITED.with(|c| c.set(true));
}

// ---------------------------------------------------------------------------
// plpgsql_call_handler — fmgr call handler (LANGUAGE plpgsql functions).
// ---------------------------------------------------------------------------

/// `plpgsql_call_handler(fcinfo)` (pl_handler.c) — the call handler the function
/// manager and trigger manager invoke to execute a compiled PL/pgSQL function
/// or procedure.
///
/// Connects to SPI (nonatomic iff a `CallContext` says so), compiles-or-finds
/// the function, marks it busy, creates the procedure resowner if needed, runs
/// the appropriate sub-handler under PG_TRY/PG_FINALLY (use-count decrement +
/// cur_estate restore + resowner release), and disconnects SPI.
///
/// The trigger / event-trigger dispatch (`plpgsql_exec_trigger` /
/// `plpgsql_exec_event_trigger`) routes to the loud trigger entries in exec; the
/// scalar/procedure path runs `plpgsql_exec_function`. `plpgsql_compile` reads
/// the live `FunctionCallInfo` + catalog (the compile entry is owned by comp).
pub fn plpgsql_call_handler(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    // nonatomic = fcinfo->context is a CallContext with atomic == false.
    let nonatomic = seam::called_nonatomic(fcinfo);

    // Connect to SPI manager.
    let opts = if nonatomic { SPI_OPT_NONATOMIC } else { 0 };
    let _ = SPI_connect_ext(opts);

    // Find or compile the function. `plpgsql_compile(fcinfo, false)` (comp)
    // takes the live arena-lifetimed `types_nodes` fcinfo; the fmgr `PGFunction`
    // boundary carries the non-lifetimed `types_fmgr` fcinfo, so the
    // fcinfo-model bridge is the fmgr-dispatch substrate (loud here).
    let func = seam::compile_for_call(fcinfo);

    // The use-count++ / cur_estate save+restore + procedure resowner
    // create/release bookkeeping is the funccache + resowner substrate. The
    // scalar dispatch is the body below.
    let args: Vec<FunctionCallArg> = fcinfo
        .args
        .iter()
        .map(|a| FunctionCallArg {
            value: a.value,
            isnull: a.isnull,
        })
        .collect();

    let procedure_resowner = if nonatomic && func.requires_procedure_resowner {
        Some(seam::create_procedure_resowner())
    } else {
        None
    };

    let result: FunctionResult = if seam::called_as_trigger(fcinfo) {
        // PointerGetDatum(plpgsql_exec_trigger(func, trigdata))
        let trigdata = seam::take_trigger_data(fcinfo);
        let d = backend_pl_plpgsql_exec::plpgsql_exec_trigger(&func, trigdata);
        FunctionResult {
            value: d,
            isnull: false,
            rettype: 0,
        }
    } else if seam::called_as_event_trigger(fcinfo) {
        let trigdata = seam::take_event_trigger_data(fcinfo);
        backend_pl_plpgsql_exec::plpgsql_exec_event_trigger(&func, trigdata);
        // no return value in this case
        FunctionResult {
            value: Datum::null(),
            isnull: false,
            rettype: 0,
        }
    } else {
        backend_pl_plpgsql_exec::plpgsql_exec_function(
            &func,
            &args,
            None,
            None,
            procedure_resowner,
            !nonatomic,
        )
    };

    fcinfo.isnull = result.isnull;

    // Disconnect from SPI manager.
    match SPI_finish() {
        Ok(SPI_OK_FINISH) => {}
        Ok(rc) => seam::elog_spi_finish_failed(rc),
        Err(e) => seam::propagate(e),
    }

    result.value
}

// ---------------------------------------------------------------------------
// plpgsql_inline_handler — fmgr handler for anonymous DO code blocks.
// ---------------------------------------------------------------------------

/// `plpgsql_inline_handler(fcinfo)` (pl_handler.c) — execute an anonymous `DO`
/// code block.
///
/// Connects to SPI (nonatomic iff `!codeblock->atomic`), compiles the block via
/// `plpgsql_compile_inline`, sets up a private simple-expression `EState` +
/// resowner (NOT tied to the transaction so they survive any COMMIT/ROLLBACK in
/// the block), runs `plpgsql_exec_function` under PG_TRY/PG_CATCH (the catch
/// path flushes the failed block's long-lived resources), frees the function,
/// and disconnects SPI. Returns `(Datum) 0`.
///
/// This is the native form the `execute_inline_handler` seam installs to. The
/// private-EState/resowner creation + the catch-path resource flush are the
/// SPI/executor substrate; the create/free of the function + exec dispatch is
/// real. A pure-control-flow block runs end-to-end.
pub fn plpgsql_inline_handler(codeblock: InlineCodeBlock) -> PgResult<()> {
    // Connect to SPI manager.
    let opts = if codeblock.atomic { 0 } else { SPI_OPT_NONATOMIC };
    let _ = SPI_connect_ext(opts);

    // Compile the anonymous code block.
    let source = codeblock.source_text.clone().unwrap_or_default();
    let mut func = backend_pl_plpgsql_comp::plpgsql_compile_inline(source);

    // Mark the function as busy, just pro forma (funccache use_count; substrate).

    // Create a private EState + resowner for simple-expression execution. These
    // are NOT tied to transaction-level resources. (SPI/executor substrate;
    // None until that lands — exec creates its econtext lazily, and a
    // control-flow-only block never reads them.)
    let simple_eval_estate = None;
    let simple_eval_resowner = None;

    // Run the function (fake fcinfo with no args).
    let _result = backend_pl_plpgsql_exec::plpgsql_exec_function(
        &func,
        &[],
        simple_eval_estate,
        simple_eval_resowner,
        None,
        codeblock.atomic,
    );

    // Function should now have no remaining use-counts; free subsidiary storage.
    backend_pl_plpgsql_funcs::plpgsql_free_function_memory(&mut func)?;

    // Disconnect from SPI manager.
    match SPI_finish() {
        Ok(SPI_OK_FINISH) => {}
        Ok(rc) => seam::elog_spi_finish_failed(rc),
        Err(e) => return Err(e),
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// plpgsql_validator — CREATE FUNCTION-time body validation.
// ---------------------------------------------------------------------------

/// `plpgsql_validator(fcinfo)` (pl_handler.c) — validate a PL/pgSQL function at
/// CREATE FUNCTION time.
///
/// Reads the new function's `pg_proc` row, rejects disallowed pseudotype
/// result/argument types (except TRIGGER/EVTTRIGGER/RECORD/VOID/polymorphic),
/// and — if `check_function_bodies` — test-compiles the body via
/// `plpgsql_compile(forValidator=true)` under an SPI bracket.
///
/// The syscache `pg_proc` read, `get_typtype`, `get_func_arg_info`,
/// `CheckFunctionValidatorAccess`, and the `check_function_bodies` GUC are the
/// catalog/syscache/GUC substrate and route through [`seam`] (loud until they
/// land); the access early-out + the fake-fcinfo test-compile control flow is
/// real.
pub fn plpgsql_validator(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let funcoid = seam::getarg_oid(fcinfo, 0);

    if !seam::check_function_validator_access(seam::flinfo_fn_oid(fcinfo), funcoid)? {
        return Ok(Datum::null()); // PG_RETURN_VOID
    }

    // The pg_proc-tuple read, pseudotype-rejection, and the test-compile path
    // all bottom out in the syscache/catalog substrate.
    seam::validate_function_body(funcoid)?;

    Ok(Datum::null()) // PG_RETURN_VOID
}

// ---------------------------------------------------------------------------
// fmgr PGFunction wrappers + builtin registration.
// ---------------------------------------------------------------------------

/// The `PGFunction` ABI wrapper for `plpgsql_call_handler`.
fn plpgsql_call_handler_pg(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    plpgsql_call_handler(fcinfo)
}

/// The `PGFunction` ABI wrapper for `plpgsql_inline_handler`. The
/// `InlineCodeBlock*` arrives as `PG_GETARG_DATUM(0)` (the internal-pointer
/// lane); unpacking it from the fmgr boundary is the INTERNAL-arg substrate.
fn plpgsql_inline_handler_pg(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let codeblock = seam::getarg_inline_codeblock(fcinfo);
    match plpgsql_inline_handler(codeblock) {
        Ok(()) => Datum::null(),
        Err(e) => seam::propagate(e),
    }
}

/// The `PGFunction` ABI wrapper for `plpgsql_validator`.
fn plpgsql_validator_pg(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match plpgsql_validator(fcinfo) {
        Ok(d) => d,
        Err(e) => seam::propagate(e),
    }
}

/// The simple (suffix-free, directory-free) name of the plpgsql loadable module
/// — `$libdir/plpgsql` reduces to this for the in-process loader registry.
const LIBRARY: &str = "plpgsql";

/// Resolve a symbol of the `plpgsql` module to its ported `PGFunction`, exactly
/// as the OS loader would resolve it in `plpgsql.so`. The three
/// `PG_FUNCTION_INFO_V1` entry points (`plpgsql_call_handler` /
/// `plpgsql_inline_handler` / `plpgsql_validator`) named by the
/// `pg_proc.probin = '$libdir/plpgsql'` rows the extension creates resolve here
/// (api_version 1). Returns `None` for an unknown symbol.
fn lookup(function: &str) -> Option<types_fmgr::LoadedExternalFunc> {
    let user_fn: types_fmgr::fmgr::PGFunction = match function {
        "plpgsql_call_handler" => Some(plpgsql_call_handler_pg),
        "plpgsql_inline_handler" => Some(plpgsql_inline_handler_pg),
        "plpgsql_validator" => Some(plpgsql_validator_pg),
        _ => return None,
    };
    Some(types_fmgr::LoadedExternalFunc {
        user_fn,
        api_version: 1,
    })
}

/// Register the three handler `PGFunction`s in the fmgr built-in registry by
/// name, so a C-language `pg_proc` row whose `prosrc` is one of these resolves
/// to the Rust function. plpgsql is an extension (`CREATE EXTENSION plpgsql`),
/// so its function OIDs are catalog-assigned at install time, not fixed builtin
/// OIDs; the registry is keyed by name for the C-language lookup
/// (`fmgr_lookup_by_name`). The placeholder `foid = 0` is overwritten by the
/// catalog OID when the row is created.
fn register_handler_builtins() {
    backend_utils_fmgr_core::register_builtins([
        BuiltinFunction {
            foid: 0,
            name: "plpgsql_call_handler".to_string(),
            nargs: 0,
            strict: false,
            retset: false,
            func: Some(plpgsql_call_handler_pg),
        },
        BuiltinFunction {
            foid: 0,
            name: "plpgsql_inline_handler".to_string(),
            nargs: 1,
            strict: false,
            retset: false,
            func: Some(plpgsql_inline_handler_pg),
        },
        BuiltinFunction {
            foid: 0,
            name: "plpgsql_validator".to_string(),
            nargs: 1,
            strict: false,
            retset: false,
            func: Some(plpgsql_validator_pg),
        },
    ]);
}

// ---------------------------------------------------------------------------
// Seam installation.
// ---------------------------------------------------------------------------

/// Install this crate's seams: the DO-block dispatch (`execute_inline_handler`,
/// reached by functioncmds' `ExecuteDoStmt`) and the three fmgr handler
/// built-ins.
pub fn init_seams() {
    backend_commands_functioncmds_seams::execute_inline_handler::set(|_laninline, codeblock| {
        plpgsql_inline_handler(codeblock)
    });

    // Install the PL/pgSQL expression evaluator's SPI bridge (the executor's
    // `exec_eval_expr` → `exec_run_select` slow path). The executor unit is
    // layered below SPI and reaches the SPI plan surface through this seam; the
    // handler (the top layer, with SPI access) installs it. The exec-seams
    // bridge types map 1:1 to the SPI `spi_eval_expr` value types.
    backend_pl_plpgsql_exec_seams::exec_eval_expr_via_spi::set(
        |query: String,
         parse_mode,
         parse_state,
         datum_snapshot: Vec<Option<backend_pl_plpgsql_exec_seams::EvalParamValue>>,
         maxtuples| {
            let mut resolve = |dno: i32| -> PgResult<backend_executor_spi::EvalParamValue> {
                // setup_param_list reads estate->datums[dno]; the snapshot carries
                // the value the caller (exec) read out of the live execstate.
                match datum_snapshot.get(dno as usize).and_then(|o| o.as_ref()) {
                    Some(v) => Ok(backend_executor_spi::EvalParamValue {
                        value: v.value,
                        isnull: v.isnull,
                        typeid: v.typeid,
                    }),
                    None => Err(types_error::PgError::error(format!(
                        "PL/pgSQL expression references datum {dno} that is not a scalar variable"
                    ))),
                }
            };
            let r = backend_executor_spi::spi_eval_expr(
                &query,
                parse_mode,
                parse_state,
                maxtuples,
                &mut resolve,
            )?;
            Ok(backend_pl_plpgsql_exec_seams::EvalExprResult {
                value: r.value,
                isnull: r.isnull,
                typeid: r.typeid,
                processed: r.processed,
            })
        },
    );

    register_handler_builtins();

    // Register `$libdir/plpgsql` with the in-process ported-library loader
    // registry, so fmgr's LANGUAGE C resolution of the call handler / inline
    // handler / validator (pg_proc.probin = '$libdir/plpgsql') finds the ported
    // PGFunctions instead of trying to dlopen a nonexistent plpgsql.so.
    backend_utils_fmgr_dfmgr_seams::register_builtin_library(
        backend_utils_fmgr_dfmgr_seams::BuiltinLibraryEntry {
            name: LIBRARY,
            lookup,
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    // The identifier-list branch routes through `split_identifier_string` ->
    // `downcase_identifier` (scansup seam), uninstalled in a bare unit test; the
    // "all"/"none" fast paths do not touch it, so we test those (the list branch
    // is exercised end-to-end once seams are installed).
    #[test]
    fn extra_checks_all_and_none() {
        let ctx = mcx::MemoryContext::new("test");
        let mcx = ctx.mcx();
        assert_eq!(
            plpgsql_extra_checks_check_hook(mcx, "all").unwrap(),
            Ok(PLPGSQL_XCHECK_ALL)
        );
        assert_eq!(
            plpgsql_extra_checks_check_hook(mcx, "ALL").unwrap(),
            Ok(PLPGSQL_XCHECK_ALL)
        );
        assert_eq!(
            plpgsql_extra_checks_check_hook(mcx, "none").unwrap(),
            Ok(PLPGSQL_XCHECK_NONE)
        );
    }

    #[test]
    fn assign_hooks_set_globals() {
        plpgsql_extra_warnings_assign_hook(PLPGSQL_XCHECK_SHADOWVAR);
        assert_eq!(plpgsql_extra_warnings(), PLPGSQL_XCHECK_SHADOWVAR);
        plpgsql_extra_errors_assign_hook(PLPGSQL_XCHECK_TOOMANYROWS);
        assert_eq!(plpgsql_extra_errors(), PLPGSQL_XCHECK_TOOMANYROWS);
        plpgsql_extra_warnings_assign_hook(PLPGSQL_XCHECK_NONE);
        plpgsql_extra_errors_assign_hook(PLPGSQL_XCHECK_NONE);
    }

    #[test]
    fn guc_defaults_match_c() {
        assert_eq!(
            plpgsql_variable_conflict(),
            PLpgSQL_resolve_option::PLPGSQL_RESOLVE_ERROR
        );
        assert!(!plpgsql_print_strict_params());
        assert!(plpgsql_check_asserts());
        assert_eq!(plpgsql_extra_warnings(), PLPGSQL_XCHECK_NONE);
        assert_eq!(plpgsql_extra_errors(), PLPGSQL_XCHECK_NONE);
    }
}
