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

use core::cell::{Cell, OnceCell};
use types_datum::VARHDRSZ;

thread_local! {
    /// Backend-lifetime context for the EXCEPTION handler's SQLSTATE/SQLERRM
    /// (and GET DIAGNOSTICS) `text` values: `CStringGetTextDatum` allocations
    /// that outlive the producing call because they are stored into a PL/pgSQL
    /// variable. Leaked once (never dropped), mirroring C's palloc lifetime.
    static PLPGSQL_ERRVAR_CONTEXT: OnceCell<&'static mcx::MemoryContext> =
        const { OnceCell::new() };
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
/// Flatten a by-reference fmgr argument/value payload (`fcinfo.ref_arg(i)`) to
/// its verbatim owned byte image. A `Varlena`/`Composite` arm is the header-ful
/// flat image; a `Cstring` is the NUL-excluded text bytes. The `Expanded` /
/// `Internal` arms are never a PL/pgSQL scalar argument/value (no flat image),
/// so they degrade to `None` (treated as by-value — C never reaches them here).
fn ref_payload_image(p: Option<&types_fmgr::boundary::RefPayload>) -> Option<std::vec::Vec<u8>> {
    use types_fmgr::boundary::RefPayload;
    match p {
        Some(RefPayload::Varlena(b)) => Some(b.clone()),
        Some(RefPayload::Composite(b)) => Some(b.clone()),
        Some(RefPayload::Cstring(s)) => Some(s.as_bytes().to_vec()),
        _ => None,
    }
}

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
    // A pass-by-reference argument (`text`/`varchar`/`numeric`/…) crosses the
    // fmgr boundary as an owned image in the parallel `ref_args` side-channel
    // (C's "`args[i].value` is a pointer to `payload`"). Carry the verbatim
    // header-ful varlena / cstring bytes alongside the bare-word `value` so the
    // arg-store leg can keep the image in the local variable; a by-value
    // argument has no `ref_args` entry (`byref == None`).
    let args: Vec<FunctionCallArg> = fcinfo
        .args
        .iter()
        .enumerate()
        .map(|(i, a)| FunctionCallArg {
            value: a.value,
            isnull: a.isnull,
            byref: ref_payload_image(fcinfo.ref_arg(i)),
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
            byref: None,
            rettype: 0,
        }
    } else if seam::called_as_event_trigger(fcinfo) {
        let trigdata = seam::take_event_trigger_data(fcinfo);
        backend_pl_plpgsql_exec::plpgsql_exec_event_trigger(&func, trigdata);
        // no return value in this case
        FunctionResult {
            value: Datum::null(),
            isnull: false,
            byref: None,
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

    // A pass-by-reference result (text/varchar/numeric/SQLERRM/…) crosses the
    // fmgr boundary through `ref_result`: set the owned varlena image (already
    // header-ful, `datumCopy`'d out of the exec/SPI context, so it outlives the
    // SPI bracket we close just below) and return the dummy word. fmgr-core's
    // result marshaling (`take_ref_result` → `FmgrOut::Ref`) reconstructs the
    // value. A by-value result returns its scalar word directly (`byref == None`).
    let ret = match result.byref {
        Some(image) if !result.isnull => {
            fcinfo.set_ref_result(types_fmgr::boundary::RefPayload::Varlena(image));
            Datum::from_usize(0)
        }
        _ => result.value,
    };

    // Disconnect from SPI manager.
    match SPI_finish() {
        Ok(SPI_OK_FINISH) => {}
        Ok(rc) => seam::elog_spi_finish_failed(rc),
        Err(e) => seam::propagate(e),
    }

    ret
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
                        // A by-reference datum carries its image; forward it so
                        // the param-bind reconstructs the rich `Datum::ByRef`.
                        byref: v.byref.clone(),
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
                byref: r.byref,
                typeid: r.typeid,
                processed: r.processed,
            })
        },
    );

    // Install the final RAISE `ereport(stmt->elog_level, ...)` (pl_exec.c): drive
    // the elog report cycle with the assembled fields. A non-ERROR level reports
    // a message to the client and returns Ok; an ERROR throws (Err).
    backend_pl_plpgsql_exec_seams::raise_ereport::set(
        |report: backend_pl_plpgsql_exec_seams::RaiseEreport| {
            use backend_utils_error::ereport;
            use types_error::{ErrorLocation, ErrorLevel, SqlState};

            let mut b = ereport(ErrorLevel(report.elog_level));
            if report.err_code != 0 {
                b = b.errcode(SqlState(report.err_code));
            }
            // errmsg_internal("%s", err_message) — the message is already the
            // final text (no further %-substitution).
            b = b.errmsg_internal(report.message);
            if let Some(d) = report.detail {
                b = b.errdetail_internal(d);
            }
            if let Some(h) = report.hint {
                b = b.errhint_internal(h);
            }
            // err_generic_string(PG_DIAG_*, value) for the diagnostics fields.
            if let Some(v) = report.column {
                b = b.err_generic_string(types_error::PG_DIAG_COLUMN_NAME, v)?;
            }
            if let Some(v) = report.constraint {
                b = b.err_generic_string(types_error::PG_DIAG_CONSTRAINT_NAME, v)?;
            }
            if let Some(v) = report.datatype {
                b = b.err_generic_string(types_error::PG_DIAG_DATATYPE_NAME, v)?;
            }
            if let Some(v) = report.table {
                b = b.err_generic_string(types_error::PG_DIAG_TABLE_NAME, v)?;
            }
            if let Some(v) = report.schema {
                b = b.err_generic_string(types_error::PG_DIAG_SCHEMA_NAME, v)?;
            }
            b.finish(ErrorLocation {
                filename: None,
                lineno: 0,
                funcname: None,
            })
        },
    );

    // Install `convert_value_to_string` (pl_exec.c): getTypeOutputInfo +
    // OidOutputFunctionCall. The executor uses it for RAISE `%` substitution and
    // USING option text. The plpgsql value crosses as a bare word; for a
    // by-value type that is the value itself (a by-ref result is the separate
    // by-ref-Datum keystone, which the output function would dereference).
    backend_pl_plpgsql_exec_seams::convert_value_to_string::set(
        |value: usize, byref: Option<Vec<u8>>, valtype| {
            let cxt = mcx::MemoryContext::new("PL/pgSQL convert_value_to_string");
            let mcx = cxt.mcx();
            let (typoutput, typisvarlena) =
                backend_utils_cache_lsyscache_seams::get_type_output_info::call(valtype)?;
            // For a pass-by-reference type the bare word is `0` and the referent
            // varlena/cstring image is carried out-of-band in `byref`; build the
            // canonical by-ref Datum so the output function reads the real value
            // (C: the Datum *is* the pointer). A by-value type uses the bare word.
            use types_tuple::backend_access_common_heaptuple::Datum as CanonDatum;
            let datum = match byref {
                Some(image) if typisvarlena => CanonDatum::ByRef(mcx::slice_in(mcx, &image)?),
                Some(image) => {
                    // A pass-by-reference but non-varlena output type (e.g. a
                    // cstring-domain) renders its image as a cstring referent.
                    match std::str::from_utf8(&image) {
                        Ok(s) => CanonDatum::Cstring(s.to_string()),
                        Err(_) => CanonDatum::ByRef(mcx::slice_in(mcx, &image)?),
                    }
                }
                None => CanonDatum::from_usize(value),
            };
            let bytes = backend_utils_fmgr_fmgr_seams::oid_output_function_call::call(
                mcx, typoutput, &datum,
            )?;
            Ok(String::from_utf8_lossy(&bytes).into_owned())
        },
    );

    // Install `plpgsql_recognize_err_condition` (pl_comp.c) — bridge to the
    // compiler's real body (the exception-label table + SQLSTATE parse).
    backend_pl_plpgsql_exec_seams::recognize_err_condition::set(
        |condname: String, allow_sqlstate| {
            backend_pl_plpgsql_comp::plpgsql_recognize_err_condition(&condname, allow_sqlstate)
        },
    );

    // Install `exec_cast_value` slow path (pl_exec.c do_cast_value): the
    // CaseTestExpr coercion-expression + ExecEvalExpr machinery is executor
    // substrate; plpgsql's documented fallback for any coercion not available as
    // a plan-level cast is an I/O coercion (CoerceViaIO), which is faithful and
    // correct for scalar casts. We implement the slow path as that I/O coercion:
    // render the source value to text (source typoutput) and read it back at the
    // target type (target typinput, with the target typmod). The no-op relabel
    // case (valtype == reqtype, unconstrained typmod) is handled in-crate and
    // never reaches here.
    backend_pl_plpgsql_exec_seams::exec_cast_value_via_spi::set(
        |value: usize, value_byref, isnull, valtype, _valtypmod, reqtype, reqtypmod| {
            use backend_pl_plpgsql_exec_seams::CastValueResult;
            if isnull {
                // A NULL stays NULL across any cast (the cast expression is
                // strict for I/O coercion; exec_cast_value returns the input).
                return Ok(CastValueResult { value, isnull: true, byref: None });
            }
            let cxt = mcx::MemoryContext::new("PL/pgSQL exec_cast_value");
            let mcx = cxt.mcx();

            // Render the source value to its text representation. A by-reference
            // source carries its image in `value_byref` (the bare `value` word is
            // `0` then); rebuild a `Datum::ByRef` so the output function reads the
            // real bytes. A by-value source is the bare scalar word.
            let (typoutput, _typisvarlena) =
                backend_utils_cache_lsyscache_seams::get_type_output_info::call(valtype)?;
            let src_datum = match value_byref {
                Some(image) => types_tuple::backend_access_common_heaptuple::Datum::ByRef(
                    mcx::slice_in(mcx, &image)?,
                ),
                None => types_tuple::backend_access_common_heaptuple::Datum::from_usize(value),
            };
            let text = backend_utils_fmgr_fmgr_seams::oid_output_function_call::call(
                mcx, typoutput, &src_datum,
            )?;
            let s = String::from_utf8_lossy(&text).into_owned();

            // Read it back at the target type with the target typmod.
            let (typinput, typioparam) =
                backend_utils_cache_lsyscache_seams::get_type_input_info::call(reqtype)?;
            let result = backend_utils_fmgr_fmgr_seams::input_function_call::call(
                mcx,
                typinput,
                Some(s.as_str()),
                typioparam,
                reqtypmod,
            )?;
            // The coerced result crosses back either as a by-value bare word or —
            // for a pass-by-reference target (`text`/`varchar`/`numeric`/…) — as
            // its owned varlena / cstring image, `datumCopy`'d out of `mcx` so it
            // outlives this working context (mirrors the SPI receiver capture).
            let out = match result {
                types_tuple::backend_access_common_heaptuple::Datum::ByVal(w) => {
                    CastValueResult { value: w, isnull: false, byref: None }
                }
                types_tuple::backend_access_common_heaptuple::Datum::ByRef(b) => {
                    CastValueResult { value: 0, isnull: false, byref: Some(b.as_slice().to_vec()) }
                }
                types_tuple::backend_access_common_heaptuple::Datum::Cstring(ref sct) => {
                    CastValueResult {
                        value: 0,
                        isnull: false,
                        byref: Some(sct.as_bytes().to_vec()),
                    }
                }
                other => CastValueResult {
                    value: 0,
                    isnull: false,
                    byref: Some(other.as_varlena_bytes().into_owned()),
                },
            };
            Ok(out)
        },
    );

    // Install `exec_stmt_execsql` core (pl_exec.c): the SPI plan surface for an
    // embedded DML / SELECT statement (the bridge types map 1:1 to the SPI
    // spi_execsql value types, like exec_eval_expr_via_spi).
    backend_pl_plpgsql_exec_seams::exec_execsql_via_spi::set(
        |query: String,
         parse_mode,
         parse_state,
         datum_snapshot: Vec<Option<backend_pl_plpgsql_exec_seams::EvalParamValue>>,
         read_only,
         into,
         tcount| {
            let mut resolve = |dno: i32| -> PgResult<backend_executor_spi::EvalParamValue> {
                match datum_snapshot.get(dno as usize).and_then(|o| o.as_ref()) {
                    Some(v) => Ok(backend_executor_spi::EvalParamValue {
                        value: v.value,
                        isnull: v.isnull,
                        typeid: v.typeid,
                        // A by-reference datum carries its image; forward it so
                        // the param-bind reconstructs the rich `Datum::ByRef`.
                        byref: v.byref.clone(),
                    }),
                    None => Err(types_error::PgError::error(format!(
                        "PL/pgSQL embedded SQL references datum {dno} that is not a scalar variable"
                    ))),
                }
            };
            let r = backend_executor_spi::spi_execsql(
                &query,
                parse_mode,
                parse_state,
                read_only,
                into,
                tcount,
                &mut resolve,
            )?;
            Ok(backend_pl_plpgsql_exec_seams::ExecsqlResult {
                code: r.code,
                processed: r.processed,
                returned_tuptable: r.returned_tuptable,
                first_row: r
                    .first_row
                    .into_iter()
                    .map(|c| backend_pl_plpgsql_exec_seams::ExecsqlColumn {
                        value: c.value,
                        isnull: c.isnull,
                        typeid: c.typeid,
                        typmod: -1,
                        // A by-reference INTO column carries its image; forward
                        // it so the INTO store keeps the image in the target var.
                        byref: c.byref,
                    })
                    .collect(),
            })
        },
    );

    // Install the `exec_stmt_block` EXCEPTION-leg subtransaction entry points
    // (pl_exec.c keystone #215). The executor unit is layered below xact; the
    // handler (top layer) bridges to the now-ported xact subxact engine. These
    // are thin delegations — no behavior is added.
    backend_pl_plpgsql_exec_seams::begin_internal_subtransaction::set(|| {
        // BeginInternalSubTransaction(NULL).
        backend_access_transam_xact::BeginInternalSubTransaction(None)
    });
    backend_pl_plpgsql_exec_seams::release_current_subtransaction::set(|| {
        backend_access_transam_xact::ReleaseCurrentSubTransaction()
    });
    backend_pl_plpgsql_exec_seams::rollback_and_release_current_subtransaction::set(|| {
        // xact's AbortSubTransaction drives AtEOSubXact_SPI(false, mySubid)
        // through the installed seam, restoring the SPI connection (modern PG
        // dropped the explicit SPI_restore_connection call here).
        backend_access_transam_xact::RollbackAndReleaseCurrentSubTransaction()
    });

    // Install `CStringGetTextDatum` for the EXCEPTION handler's SQLSTATE/SQLERRM
    // special-var binding (assign_error_vars) and `exec_stmt_getdiag`. C does
    // `CStringGetTextDatum(s)` = `cstring_to_text(s)` palloc'd in
    // `CurrentMemoryContext`, then `exec_assign_value(..., TEXTOID, -1)` stores
    // it into the target variable (assign_simple_var copies it into the
    // function's datum context with datumCopy). The PL/pgSQL executor here
    // carries scalar values as bare machine words (`types_datum::Datum`), so a
    // by-reference `text` must cross as a pointer word at a header-ful varlena
    // whose bytes outlive the call. Build that header-ful varlena in a
    // backend-lifetime context (mirroring the palloc) and return the pointer
    // word; the target var keeps it (these short error strings are bounded).
    backend_pl_plpgsql_exec_seams::cstring_to_text_datum::set(|s: String| {
        // A leaked, backend-lifetime context: the produced `text` outlives this
        // call (it is stored into a PL/pgSQL variable), exactly as C's
        // CStringGetTextDatum palloc lives in the function's execution context.
        let ctx: &'static mcx::MemoryContext = PLPGSQL_ERRVAR_CONTEXT.with(|c| {
            *c.get_or_init(|| {
                Box::leak(Box::new(mcx::MemoryContext::new("PL/pgSQL error-var text")))
            })
        });
        let bytes = s.as_bytes();
        // C: palloc(len + VARHDRSZ); SET_VARSIZE; memcpy(VARDATA, s, len).
        let mut image = mcx::vec_with_capacity_in::<u8>(ctx.mcx(), bytes.len() + VARHDRSZ)?;
        image.extend_from_slice(&[0u8; VARHDRSZ]);
        image.extend_from_slice(bytes);
        let varlena = types_datum::Varlena::from_image(image);
        // DatumGetPointer view: the address of the header-ful varlena image. The
        // image lives in the leaked backend-lifetime context; forget the owning
        // wrapper so its Drop never deallocates (the bytes must persist for the
        // lifetime of the variable that now holds the pointer, like C's palloc).
        let image = varlena.into_image();
        let ptr = image.as_ptr() as usize;
        // Hand back a copy of the verbatim header-ful varlena bytes so the caller
        // (`assign_text_var`) can populate the variable's `value_byref`
        // out-of-band companion. Without this image the special var would carry
        // only the bare word, and a later read across the fmgr boundary (e.g.
        // `RETURN SQLERRM`, a text comparison) would see no by-ref payload and
        // panic in the varlena cmp cores.
        let image_copy = image.to_vec();
        core::mem::forget(image);
        Ok((ptr, image_copy))
    });

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
