//! Outward substrate the PL/pgSQL handler consumes (`pl_handler.c`'s externals).
//!
//! The handler brackets every entry with SPI (real, called directly in
//! `lib.rs`), drives compile + exec (real owner crates), and frees the function
//! (real). The remaining externals — the custom-GUC `DefineCustom*Variable`
//! registration, `MarkGUCPrefixReserved`, the xact/subxact callback
//! registration (whose callbacks `plpgsql_xact_cb`/`_subxact_cb` live in
//! `pl_exec.c` and are not yet ported), the plugin rendezvous, the fmgr
//! `CallContext`/`TriggerData` context demux, the procedure resowner creation,
//! and the validator's syscache `pg_proc` reads — bottom out in subsystems not
//! yet reachable here. Per REAL-OR-LOUD each names its precise C callee + the
//! external it needs and panics; a faithful C build would `ereport`/elog at
//! exactly these points until those owners land.

use types_error::{PgError, PgResult};
use types_fmgr::fmgr::FunctionCallInfoBaseData;
use types_core::Oid;
use types_parsenodes::InlineCodeBlock;
use types_plpgsql::{int32, EventTriggerData, ResourceOwner, TriggerData};

/// `T_CallContext` (nodetags.h).
const T_CALL_CONTEXT: u32 = 332;
/// `T_TriggerData` (nodetags.h).
const T_TRIGGER_DATA: u32 = 442;
/// `T_EventTriggerData` (nodetags.h).
const T_EVENT_TRIGGER_DATA: u32 = 443;

// --- _PG_init substrate ------------------------------------------------------

/// `pg_bindtextdomain(TEXTDOMAIN)` — message-catalog binding. No-op in this
/// single-locale build (the NLS substrate is not wired); the C call has no
/// control-flow effect.
pub fn pg_bindtextdomain() {}

/// `DefineCustomEnumVariable("plpgsql.variable_conflict", …)` — register the
/// custom GUC. The custom-GUC registration substrate (`guc.c`
/// `DefineCustom*Variable` + the `config_enum_entry` table) is not yet ported;
/// the compile-time default (`PLPGSQL_RESOLVE_ERROR`) the compiler reads already
/// matches, so the variable is correct without registration.
pub fn define_custom_enum_variable_variable_conflict() {
    panic!(
        "seam not wired: DefineCustomEnumVariable(\"plpgsql.variable_conflict\") (pl_handler.c) — \
         custom-GUC registration substrate (guc.c) not yet ported"
    );
}

/// `DefineCustomBoolVariable("plpgsql.print_strict_params", …)`.
pub fn define_custom_bool_variable_print_strict_params() {
    panic!(
        "seam not wired: DefineCustomBoolVariable(\"plpgsql.print_strict_params\") (pl_handler.c) — \
         custom-GUC registration substrate (guc.c) not yet ported"
    );
}

/// `DefineCustomBoolVariable("plpgsql.check_asserts", …)`.
pub fn define_custom_bool_variable_check_asserts() {
    panic!(
        "seam not wired: DefineCustomBoolVariable(\"plpgsql.check_asserts\") (pl_handler.c) — \
         custom-GUC registration substrate (guc.c) not yet ported"
    );
}

/// `DefineCustomStringVariable("plpgsql.extra_warnings", …)`.
pub fn define_custom_string_variable_extra_warnings() {
    panic!(
        "seam not wired: DefineCustomStringVariable(\"plpgsql.extra_warnings\") (pl_handler.c) — \
         custom-GUC registration substrate (guc.c) not yet ported"
    );
}

/// `DefineCustomStringVariable("plpgsql.extra_errors", …)`.
pub fn define_custom_string_variable_extra_errors() {
    panic!(
        "seam not wired: DefineCustomStringVariable(\"plpgsql.extra_errors\") (pl_handler.c) — \
         custom-GUC registration substrate (guc.c) not yet ported"
    );
}

/// `MarkGUCPrefixReserved("plpgsql")` (guc.c).
pub fn mark_guc_prefix_reserved(_prefix: &str) {
    panic!(
        "seam not wired: MarkGUCPrefixReserved(\"plpgsql\") (pl_handler.c) — \
         GUC prefix-reservation substrate (guc.c) not yet ported"
    );
}

/// `RegisterXactCallback(plpgsql_xact_cb, NULL)` (xact.c). The callback
/// `plpgsql_xact_cb` lives in `pl_exec.c` and is not yet ported; registering a
/// fabricated no-op would silently break the (sub)transaction-boundary cast/
/// econtext resets, so this is loud.
pub fn register_xact_callback() {
    panic!(
        "seam not wired: RegisterXactCallback(plpgsql_xact_cb) (pl_handler.c) — \
         plpgsql_xact_cb (pl_exec.c) not yet ported"
    );
}

/// `RegisterSubXactCallback(plpgsql_subxact_cb, NULL)` (xact.c).
pub fn register_subxact_callback() {
    panic!(
        "seam not wired: RegisterSubXactCallback(plpgsql_subxact_cb) (pl_handler.c) — \
         plpgsql_subxact_cb (pl_exec.c) not yet ported"
    );
}

/// `plpgsql_plugin_ptr = find_rendezvous_variable("PLpgSQL_plugin")`
/// (utils/init/miscinit.c). The rendezvous-variable substrate is not yet
/// ported; the plugin pointer stays null (no plugin), which the exec hooks
/// already treat as "no plugin".
pub fn find_rendezvous_variable_plpgsql_plugin() {
    panic!(
        "seam not wired: find_rendezvous_variable(\"PLpgSQL_plugin\") (pl_handler.c) — \
         rendezvous-variable substrate (miscinit.c) not yet ported"
    );
}

// --- call-handler fmgr context demux ----------------------------------------

/// `nonatomic = fcinfo->context && IsA(fcinfo->context, CallContext) &&
/// !castNode(CallContext, fcinfo->context)->atomic`.
///
/// The `CallContext.atomic` flag is not carried through the tag-only
/// `ContextNode`; absent a CallContext the call is atomic (the function-call /
/// CREATE-FUNCTION common case), matching C when `context` is not a CallContext.
pub fn called_nonatomic(fcinfo: &FunctionCallInfoBaseData) -> bool {
    match &fcinfo.context {
        Some(c) if c.tag == T_CALL_CONTEXT => {
            // The atomic bit is not modeled on ContextNode; a procedure CALL in
            // a nonatomic context needs it. Loud rather than silently atomic.
            panic!(
                "seam not wired: CallContext.atomic (pl_handler.c) — the nonatomic flag is not \
                 carried through the tag-only fmgr ContextNode (CALL/procedure substrate)"
            );
        }
        _ => false,
    }
}

/// `CALLED_AS_TRIGGER(fcinfo)` — context is a `TriggerData` node.
pub fn called_as_trigger(fcinfo: &FunctionCallInfoBaseData) -> bool {
    matches!(&fcinfo.context, Some(c) if c.tag == T_TRIGGER_DATA)
}

/// `CALLED_AS_EVENT_TRIGGER(fcinfo)` — context is an `EventTriggerData` node.
pub fn called_as_event_trigger(fcinfo: &FunctionCallInfoBaseData) -> bool {
    matches!(&fcinfo.context, Some(c) if c.tag == T_EVENT_TRIGGER_DATA)
}

/// `(TriggerData *) fcinfo->context` — the live trigger context. The rich
/// `TriggerData` (relation / NEW-OLD tuples / tupdesc) is not carried through
/// the tag-only `ContextNode`; the trigger dispatch substrate is not reachable.
pub fn take_trigger_data(_fcinfo: &mut FunctionCallInfoBaseData) -> TriggerData {
    panic!(
        "seam not wired: TriggerData from fcinfo->context (pl_handler.c) — the trigger context \
         (relation/tuples/tupdesc) is not carried through the tag-only fmgr ContextNode \
         (trigger substrate)"
    );
}

/// `(EventTriggerData *) fcinfo->context` — the live event-trigger context.
pub fn take_event_trigger_data(_fcinfo: &mut FunctionCallInfoBaseData) -> EventTriggerData {
    panic!(
        "seam not wired: EventTriggerData from fcinfo->context (pl_handler.c) — the event-trigger \
         context is not carried through the tag-only fmgr ContextNode (event-trigger substrate)"
    );
}

/// `ResourceOwnerCreate(NULL, "PL/pgSQL procedure resources")` (resowner.c) —
/// the parentless procedure-lifespan resowner for CALL/DO statements.
pub fn create_procedure_resowner() -> ResourceOwner {
    panic!(
        "seam not wired: ResourceOwnerCreate(NULL, \"PL/pgSQL procedure resources\") \
         (pl_handler.c) — parentless resowner substrate (resowner.c) not yet ported"
    );
}

/// `plpgsql_compile(fcinfo, false)` (pl_comp.c) for the call handler. The
/// compile entry consumes the arena-lifetimed `types_nodes` `FunctionCallInfo`
/// (catalog reads + polymorphic-argtype resolution off the live call
/// expression); the fmgr `PGFunction` boundary supplies the non-lifetimed
/// `types_fmgr` fcinfo, so bridging the two fcinfo models is the fmgr-dispatch
/// substrate. Loud until that bridge lands; `plpgsql_compile_inline` (the DO
/// path) takes a plain `String` and is reached directly without it.
pub fn compile_for_call(
    _fcinfo: &mut FunctionCallInfoBaseData,
) -> types_plpgsql::PLpgSQL_function {
    panic!(
        "seam not wired: plpgsql_compile(fcinfo) for the call handler (pl_handler.c) — the \
         fmgr PGFunction fcinfo (types_fmgr) must be bridged to the arena-lifetimed compile \
         fcinfo (types_nodes) the comp entry consumes (fmgr-dispatch substrate)"
    );
}

// --- validator substrate -----------------------------------------------------

/// `PG_GETARG_OID(0)` — read the validated function's OID from the fcinfo.
pub fn getarg_oid(fcinfo: &FunctionCallInfoBaseData, n: usize) -> Oid {
    fcinfo.args[n].value.as_oid()
}

/// `fcinfo->flinfo->fn_oid` — the validator's own OID.
pub fn flinfo_fn_oid(fcinfo: &FunctionCallInfoBaseData) -> Oid {
    fcinfo
        .flinfo
        .as_ref()
        .map(|f| f.fn_oid)
        .unwrap_or(types_core::InvalidOid)
}

/// `CheckFunctionValidatorAccess(fcinfo->flinfo->fn_oid, funcoid)` (fmgr.c) —
/// permission gate before body validation. Delegates to the ported body
/// (fmgr-core, behind the pg_proc-seams seam): reads the function's pg_proc +
/// pg_language entries, verifies this validator is the language's `lanvalidator`,
/// and ACL-checks USAGE on the language + EXECUTE on the function.
pub fn check_function_validator_access(validator_oid: Oid, funcoid: Oid) -> PgResult<bool> {
    backend_catalog_pg_proc_seams::check_function_validator_access::call(validator_oid, funcoid)
}

/// `TYPTYPE_PSEUDO` (`pg_type.h`).
const TYPTYPE_PSEUDO: u8 = types_catalog::pg_type::TYPTYPE_PSEUDO as u8;
/// `TRIGGEROID` / `EVENT_TRIGGEROID` / `RECORDOID` / `VOIDOID` (pg_type.h).
const TRIGGEROID: Oid = 2279;
const EVENT_TRIGGEROID: Oid = 3838;
const RECORDOID: Oid = 2249;
const VOIDOID: Oid = 2278;

/// `IsPolymorphicType(typid)` (pg_type.h) — a pure OID comparison.
fn is_polymorphic_type(typid: Oid) -> bool {
    use types_tuple::heaptuple::{
        ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEMULTIRANGEOID, ANYCOMPATIBLENONARRAYOID,
        ANYCOMPATIBLEOID, ANYCOMPATIBLERANGEOID, ANYELEMENTOID, ANYENUMOID, ANYMULTIRANGEOID,
        ANYNONARRAYOID, ANYRANGEOID,
    };
    typid == ANYELEMENTOID
        || typid == ANYARRAYOID
        || typid == ANYNONARRAYOID
        || typid == ANYENUMOID
        || typid == ANYRANGEOID
        || typid == ANYMULTIRANGEOID
        || typid == ANYCOMPATIBLEOID
        || typid == ANYCOMPATIBLEARRAYOID
        || typid == ANYCOMPATIBLENONARRAYOID
        || typid == ANYCOMPATIBLERANGEOID
        || typid == ANYCOMPATIBLEMULTIRANGEOID
}

/// `format_type_be(type_oid)` (format_type.c) for the pseudotype-rejection error.
fn format_type_be(type_oid: Oid) -> String {
    backend_utils_adt_format_type_seams::format_type_be_owned::call(type_oid)
        .unwrap_or_else(|_| type_oid.to_string())
}

/// `check_function_bodies` GUC (guc_tables.c) — whether to test-compile the body.
fn check_function_bodies() -> bool {
    (backend_utils_misc_guc_tables::vars::check_function_bodies.get().get)()
}

/// The `plpgsql_validator` body (pl_handler.c) past the access gate: read the
/// `pg_proc` row, reject disallowed pseudotype result/argument types (all but
/// TRIGGER / EVTTRIGGER / RECORD / VOID / polymorphic), and — if
/// `check_function_bodies` — test-compile the body via
/// `plpgsql_compile(fake_fcinfo, forValidator=true)`.
pub fn validate_function_body(funcoid: Oid) -> PgResult<()> {
    let scratch = mcx::MemoryContext::new("plpgsql_validator");
    let mcx = scratch.mcx();

    // tuple = SearchSysCache1(PROCOID, funcoid); proc = GETSTRUCT(tuple).
    let proc = backend_utils_cache_syscache_seams::proc_row_by_oid::call(mcx, funcoid)?
        .ok_or_else(|| {
            types_error::PgError::error(format!("cache lookup failed for function {funcoid}"))
        })?;

    let mut is_dml_trigger = false;
    let mut is_event_trigger = false;

    // functyptype = get_typtype(proc->prorettype);
    // Disallow pseudotype result except TRIGGER/EVTTRIGGER/RECORD/VOID/polymorphic.
    let functyptype = backend_utils_cache_lsyscache_seams::get_typtype::call(proc.prorettype)?;
    if functyptype == TYPTYPE_PSEUDO {
        if proc.prorettype == TRIGGEROID {
            is_dml_trigger = true;
        } else if proc.prorettype == EVENT_TRIGGEROID {
            is_event_trigger = true;
        } else if proc.prorettype != RECORDOID
            && proc.prorettype != VOIDOID
            && !is_polymorphic_type(proc.prorettype)
        {
            return Err(types_error::PgError::error(format!(
                "PL/pgSQL functions cannot return type {}",
                format_type_be(proc.prorettype)
            ))
            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED));
        }
    }

    // Disallow pseudotypes in arguments (IN or OUT) except RECORD and polymorphic.
    // get_func_arg_info returns all argument types; `proallargtypes` (when
    // present) carries OUT args too, else `proargtypes` is the IN-arg list.
    let argtypes: Vec<Oid> = match &proc.proallargtypes {
        Some(all) => all.values.iter().copied().collect(),
        None => proc.proargtypes.iter().copied().collect(),
    };
    for &argtype in &argtypes {
        if backend_utils_cache_lsyscache_seams::get_typtype::call(argtype)? == TYPTYPE_PSEUDO
            && argtype != RECORDOID
            && !is_polymorphic_type(argtype)
        {
            return Err(types_error::PgError::error(format!(
                "PL/pgSQL functions cannot accept type {}",
                format_type_be(argtype)
            ))
            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED));
        }
    }

    // Postpone body checks if !check_function_bodies.
    if check_function_bodies() {
        // SPI_connect() — test-compile bracket.
        let _ = backend_executor_spi::SPI_connect_ext(0);

        // plpgsql_compile(fake_fcinfo, true): the validator test-compile. The
        // fake-fcinfo construction (fn_oid + dml/event-trigger context flag) plus
        // the funccache/fcinfo-model bridge is the compile keystone; route through
        // the comp owner's owned-inputs compile via the call-handler bridge seam.
        validate_test_compile(funcoid, is_dml_trigger, is_event_trigger)?;

        match backend_executor_spi::SPI_finish() {
            Ok(rc) if rc == backend_executor_spi::SPI_OK_FINISH => {}
            Ok(rc) => elog_spi_finish_failed(rc),
            Err(e) => return Err(e),
        }
    }

    Ok(())
}

/// `plpgsql_compile(fake_fcinfo, true)` — the validator's test-compile. The
/// owned-inputs compile entry (`plpgsql_compile_from_source`) needs the full
/// `ProcCompileFacts` projected from the `pg_proc` row; assembling those facts +
/// the polymorphic-return resolution from the (absent, validator-time) call
/// expression is the compile substrate. Loud until the compile bridge lands; the
/// pseudotype checks above are real and CREATE FUNCTION succeeds with
/// `check_function_bodies = off`.
fn validate_test_compile(
    funcoid: Oid,
    _is_dml_trigger: bool,
    _is_event_trigger: bool,
) -> PgResult<()> {
    panic!(
        "seam not wired: plpgsql_compile(fake_fcinfo, forValidator=true) (pl_handler.c) for \
         function {funcoid} — the validator test-compile needs ProcCompileFacts projected from \
         pg_proc + the funccache/fcinfo-model compile bridge (compile keystone); set \
         check_function_bodies = off to skip body validation"
    );
}

/// `castNode(InlineCodeBlock, DatumGetPointer(PG_GETARG_DATUM(0)))` — unpack the
/// DO codeblock from the internal-pointer arg lane.
pub fn getarg_inline_codeblock(_fcinfo: &mut FunctionCallInfoBaseData) -> InlineCodeBlock {
    panic!(
        "seam not wired: InlineCodeBlock from PG_GETARG_DATUM(0) (pl_handler.c) — the \
         internal-pointer arg lane unpack is the fmgr INTERNAL-arg substrate; the DO dispatch \
         seam (execute_inline_handler) passes the InlineCodeBlock by value instead"
    );
}

// --- error plumbing ----------------------------------------------------------

/// `elog(ERROR, "SPI_finish failed: %s", SPI_result_code_string(rc))`.
pub fn elog_spi_finish_failed(rc: int32) -> ! {
    panic!(
        "SPI_finish failed: {}",
        backend_executor_spi::SPI_result_code_string(rc)
    );
}

/// Re-raise a structured `PgError` across the bare-`Datum` PGFunction boundary
/// (the `panic_any(PgError)` channel `invoke_pgfunction` catches), mirroring C's
/// `PG_RE_THROW()` / ereport longjmp.
pub fn propagate(err: PgError) -> ! {
    std::panic::panic_any(err);
}
