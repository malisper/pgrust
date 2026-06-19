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

/// `CheckFunctionValidatorAccess(fcinfo->flinfo->fn_oid, funcoid)`
/// (catalog/pg_proc.c) — permission gate before body validation.
pub fn check_function_validator_access(_validator_oid: Oid, _funcoid: Oid) -> PgResult<bool> {
    panic!(
        "seam not wired: CheckFunctionValidatorAccess (pl_handler.c) — pg_proc access-check \
         substrate (catalog/pg_proc.c) not yet ported"
    );
}

/// The validator body: read the `pg_proc` tuple, reject disallowed pseudotype
/// result/argument types, and (if `check_function_bodies`) test-compile via
/// `plpgsql_compile(forValidator=true)`. The syscache `pg_proc` read +
/// `get_typtype` + `get_func_arg_info` + the `check_function_bodies` GUC are the
/// catalog/syscache/GUC substrate.
pub fn validate_function_body(_funcoid: Oid) -> PgResult<()> {
    panic!(
        "seam not wired: plpgsql_validator body (pl_handler.c) — SearchSysCache1(PROCOID) + \
         get_typtype + get_func_arg_info + check_function_bodies GUC + plpgsql_compile(forValidator) \
         (syscache/catalog/GUC substrate)"
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
