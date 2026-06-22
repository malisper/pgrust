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
/// The CALL dispatcher (`ExecuteCallStmt`) deposits `CallContext.atomic` onto
/// the `ContextNode` it stamps on the call frame, so the nonatomic flag is
/// `IsA(fcinfo->context, CallContext) && !context->atomic`. Absent a CallContext
/// the call is atomic (the function-call / CREATE-FUNCTION common case), matching
/// C when `context` is not a CallContext.
pub fn called_nonatomic(fcinfo: &FunctionCallInfoBaseData) -> bool {
    match &fcinfo.context {
        Some(c) if c.tag == T_CALL_CONTEXT => !c.atomic,
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

/// `(TriggerData *) fcinfo->context` — the live trigger context.
///
/// The rich `TriggerData` (relation / NEW-OLD tuples / tupdesc) cannot ride the
/// tag-only fmgr `ContextNode`; instead it lives on the firing path's per-call
/// thread-local side-channel (`commands/trigger.c`'s `LocTriggerData`), which
/// the trigger executor reads through the `tg_*` accessor seams. Crossing the
/// fmgr boundary, all `fcinfo->context` carries is the `T_TriggerData` demux
/// tag — already verified by [`called_as_trigger`] before we get here — so the
/// `TriggerData` handle returned to `plpgsql_exec_trigger` is the opaque marker
/// that resolves to that current-trigger side-channel (the `TriggerData(0)`
/// "the trigger in flight" handle the accessors key off).
pub fn take_trigger_data(_fcinfo: &mut FunctionCallInfoBaseData) -> TriggerData {
    debug_assert!(called_as_trigger(_fcinfo));
    TriggerData(0)
}

/// `(EventTriggerData *) fcinfo->context` — the live event-trigger context.
///
/// Like [`take_trigger_data`], the rich `EventTriggerData` (event / tag /
/// parsetree) cannot ride the tag-only fmgr `ContextNode`; it lives on the firing
/// path's per-call thread-local side-channel (`commands/event_trigger.c`'s
/// `CURRENT_EVENT_TRIGGER`), which the executor reads through the
/// `event_trigger_get_event` / `event_trigger_get_tag_name` accessor seams. All
/// `fcinfo->context` carries is the `T_EventTriggerData` demux tag — already
/// verified by [`called_as_event_trigger`] — so the handle returned here is the
/// opaque marker (`EventTriggerData(0)`) that resolves to that side-channel.
pub fn take_event_trigger_data(_fcinfo: &mut FunctionCallInfoBaseData) -> EventTriggerData {
    debug_assert!(called_as_event_trigger(_fcinfo));
    EventTriggerData(0)
}

/// `ResourceOwnerCreate(NULL, "PL/pgSQL procedure resources")` (resowner.c) —
/// the parentless procedure-lifespan resowner for CALL/DO statements.
pub fn create_procedure_resowner() -> ResourceOwner {
    panic!(
        "seam not wired: ResourceOwnerCreate(NULL, \"PL/pgSQL procedure resources\") \
         (pl_handler.c) — parentless resowner substrate (resowner.c) not yet ported"
    );
}

/// `plpgsql_compile(fcinfo, false)` (pl_comp.c) for the call handler.
///
/// In C this dispatches `cached_function_compile(fcinfo, …, forValidator=false)`,
/// which on a cache miss reads the called function's `pg_proc` row and runs
/// `plpgsql_compile_callback`. The fmgr `PGFunction` boundary carries the
/// non-lifetimed `types_fmgr` fcinfo whose `flinfo->fn_oid` names the function
/// being called and whose `fncollation` is the call's input collation; the
/// funccache cache-reuse + `fn_extra` save (the funccache↔plpgsql opaque-`cfunc`
/// header keystone) is not yet modeled, so each call recompiles from the
/// on-disk `pg_proc` row here. That recompile is value-equivalent to the cache
/// miss in C (the result is the same `PLpgSQL_function`); only the reuse
/// optimization is absent.
///
/// This is the bridge between the fmgr call frame and the comp owner's
/// owned-inputs compile body: project the `pg_proc` row to [`ProcCompileFacts`]
/// (mirroring `plpgsql_compile`'s `SearchSysCache1(PROCOID)` + `get_func_arg_info`)
/// with `for_validator = false`, the live `fncollation` as the input collation,
/// and the trigtype demuxed from `fcinfo->context`, then drive
/// `plpgsql_compile_from_source`. A polymorphic return type is resolved from the
/// call expression via `get_fn_expr_rettype`; for the non-polymorphic common
/// case it is left `InvalidOid` (the compile body uses the declared `prorettype`).
pub fn compile_for_call(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_plpgsql::PLpgSQL_function {
    // fcinfo->flinfo->fn_oid — the function being called.
    let funcoid = flinfo_fn_oid(fcinfo);

    // The trigger / event-trigger demux (CALLED_AS_TRIGGER / CALLED_AS_EVENT_TRIGGER):
    // the caller routes the trigger entries directly, but plpgsql_compile keys the
    // compile arm off the same context flags, so carry them into the facts.
    let is_dml_trigger = called_as_trigger(fcinfo);
    let is_event_trigger = called_as_event_trigger(fcinfo);

    // fcinfo->fncollation — the input collation the compile records on the
    // function (used by exec for collation-aware simple-expr eval).
    let fn_input_collation = fcinfo.fncollation;

    // Resolve a polymorphic return type and polymorphic argument types from the
    // call expression carried on `fcinfo->flinfo->fn_expr`
    // (`get_fn_expr_rettype` / `resolve_polymorphic_argtypes` over the
    // `FuncExpr`/`OpExpr`). When the field-bearing call node is present these
    // yield the concrete types the call passes; otherwise they degrade to
    // `InvalidOid` and the compile body errors (C: "could not determine actual
    // type for polymorphic function") exactly as in PG when no call expr exists.
    let resolved_rettype = resolved_rettype_from_call(fcinfo);
    let call_expr = call_expr_from_fcinfo(fcinfo);

    match compile_proc_from_row(
        funcoid,
        is_dml_trigger,
        is_event_trigger,
        fn_input_collation,
        /* for_validator = */ false,
        resolved_rettype,
        call_expr,
    ) {
        Ok(func) => func,
        Err(e) => propagate(e),
    }
}

/// `get_fn_expr_rettype(fcinfo->flinfo)` — read the actual result type of the
/// call expression carried on `fcinfo->flinfo->fn_expr`. `InvalidOid` when no
/// field-bearing call node is present (the compile body then errors for a
/// polymorphic return, as C does when `fn_expr` is unavailable).
fn resolved_rettype_from_call(fcinfo: &FunctionCallInfoBaseData) -> Oid {
    match fcinfo.flinfo.as_deref() {
        Some(flinfo) => backend_utils_fmgr_core::get_fn_expr_rettype(Some(flinfo)),
        None => types_core::InvalidOid,
    }
}

/// `fcinfo->flinfo->fn_expr` projected to the funcapi `CallExpr` carrier the
/// full `resolve_polymorphic_argtypes` resolver reads. This is the
/// `call_expr` argument `cfunc_resolve_polymorphic_argtypes` (funccache.c)
/// passes to `resolve_polymorphic_argtypes`: the call's `FuncExpr`/`OpExpr`
/// node, from which the resolver reads each *input* argument's actual type
/// (`get_call_expr_argtype`) to deduce every polymorphic position — both IN and
/// OUT. Returns `None` when no field-bearing call node is present (the validator
/// / no-call-expr case), in which case the compile body keeps the declared types
/// (and errors if any input is polymorphic, exactly as C does when `call_expr`
/// is unavailable).
fn call_expr_from_fcinfo(
    fcinfo: &FunctionCallInfoBaseData,
) -> Option<backend_utils_fmgr_funcapi::polymorphic::CallExpr> {
    let flinfo = fcinfo.flinfo.as_deref()?;
    match flinfo.fn_expr.as_deref() {
        Some(types_fmgr::fmgr::FnExpr::External(ext)) => {
            // C carries the field-bearing call node in `fn_expr`; the erased
            // carrier holds the real expression node the resolver downcasts to
            // read argument types. A tag-only carrier (`node == None`) cannot
            // supply argument types, so there is no call expression to resolve
            // from (matches C's `InvalidOid` fall-through).
            let erased = ext.node.clone()?;
            Some(backend_utils_fmgr_funcapi::polymorphic::CallExpr::from_erased(erased))
        }
        _ => None,
    }
}

/// Shared body of the call-handler / validator compile: project the `pg_proc`
/// row to [`ProcCompileFacts`] (the `SearchSysCache1(PROCOID)` + `GETSTRUCT` +
/// `get_func_arg_info` of `plpgsql_compile`) and drive the comp owner's
/// owned-inputs compile body `plpgsql_compile_from_source`.
fn compile_proc_from_row(
    funcoid: Oid,
    is_dml_trigger: bool,
    is_event_trigger: bool,
    fn_input_collation: Oid,
    for_validator: bool,
    resolved_rettype: Oid,
    call_expr: Option<backend_utils_fmgr_funcapi::polymorphic::CallExpr>,
) -> PgResult<types_plpgsql::PLpgSQL_function> {
    use backend_pl_plpgsql_comp::ProcCompileFacts;
    use types_plpgsql::PLpgSQL_trigtype;

    let scratch = mcx::MemoryContext::new("plpgsql_compile");
    let mcx = scratch.mcx();

    // proc_compile_row == plpgsql_compile's SearchSysCache1(PROCOID, funcid) +
    // GETSTRUCT + get_func_arg_info; a cache miss is the C `cache lookup failed`.
    let row = backend_utils_cache_syscache_seams::proc_compile_row::call(mcx, funcoid)?
        .ok_or_else(|| {
            PgError::error(format!("cache lookup failed for function {funcoid}"))
        })?;

    // cfunc_resolve_polymorphic_argtypes (funccache.c): in the non-validator
    // case resolve every polymorphic argument position — IN *and* OUT — from the
    // call's FuncExpr via the full `resolve_polymorphic_argtypes` two-pass
    // deduction (deriving e.g. `anyarray`/`anycompatiblearray` OUT positions from
    // their sibling IN actuals). `get_func_arg_info` returns all argument types
    // (OUT included) keyed positionally with `argmodes`, so the resolved array is
    // length-matched to the declared `argtypes` the compile body consumes. The
    // validator (`for_validator`) path leaves `resolved_argtypes` empty; the
    // compile body substitutes the integer family itself.
    let resolved_argtypes: Vec<Oid> = if for_validator {
        Vec::new()
    } else {
        let mut argtypes: Vec<Oid> = row.argtypes.iter().copied().collect();
        let argmodes: Vec<u8> = row.argmodes.iter().copied().collect();
        let argmodes_opt = if argmodes.is_empty() {
            None
        } else {
            Some(argmodes.as_slice())
        };
        match backend_utils_fmgr_funcapi::polymorphic::resolve_polymorphic_argtypes(
            &mut argtypes,
            argmodes_opt,
            call_expr.as_ref(),
        ) {
            // Fully resolved: hand the concrete per-position types to the compile
            // body (which copies them into the declared polymorphic slots).
            Ok(true) => argtypes,
            // Could not determine (no usable call expression / unresolvable):
            // leave `resolved_argtypes` empty so the compile body's
            // `plpgsql_resolve_polymorphic_argtypes` raises the C
            // "could not determine actual argument type for polymorphic
            // function" error when a declared input is polymorphic.
            Ok(false) => Vec::new(),
            Err(e) => return Err(e),
        }
    };

    // fn_is_trigger arm (the C `switch (function->fn_is_trigger)`).
    let fn_is_trigger = if is_dml_trigger {
        PLpgSQL_trigtype::PLPGSQL_DML_TRIGGER
    } else if is_event_trigger {
        PLpgSQL_trigtype::PLPGSQL_EVENT_TRIGGER
    } else {
        PLpgSQL_trigtype::PLPGSQL_NOT_TRIGGER
    };

    let facts = ProcCompileFacts {
        proname: row.proname.as_str().to_owned(),
        fn_oid: funcoid,
        fn_input_collation,
        prosrc: row.prosrc.as_str().to_owned(),
        prorettype: row.prorettype,
        proretset: row.proretset,
        prokind: row.prokind,
        provolatile: row.provolatile,
        pronargs: row.pronargs,
        argtypes: row.argtypes.iter().copied().collect(),
        argnames: row.argnames.iter().map(|s| s.as_str().to_owned()).collect(),
        argmodes: row.argmodes.iter().copied().collect(),
        fn_is_trigger,
        for_validator,
        resolved_rettype,
        resolved_argtypes,
    };

    // The compile body `ereport`s on a faulty function; that propagates as a
    // PgError panic caught at the PGFunction boundary (== C's longjmp).
    backend_pl_plpgsql_comp::plpgsql_compile_from_source(&facts)
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

/// `plpgsql_compile(fake_fcinfo, true)` — the validator's test-compile.
///
/// The fake fcinfo `plpgsql_validator` builds carries only `flinfo->fn_oid =
/// funcoid`, a zeroed `fncollation` (== `InvalidOid`), and the dml/event-trigger
/// `context` flag. `plpgsql_compile` resolves the on-disk `pg_proc` row and calls
/// `plpgsql_compile_callback(forValidator=true)`. Here we project that `pg_proc`
/// row (`proc_compile_row`: the `Form_pg_proc` scalars + `prosrc` +
/// `get_func_arg_info` decomposition), assemble the owned [`ProcCompileFacts`],
/// and drive the comp owner's owned-inputs compile body
/// (`plpgsql_compile_from_source`). Trigger/event-trigger functions take their
/// `fn_is_trigger` arm; for a polymorphic return the compile body substitutes the
/// integer family (the C `forValidator` branch), so no call-expression rettype is
/// needed. A syntax/semantic error surfaces as the compile body's `ereport`,
/// caught at the `PGFunction` boundary, exactly as in C.
fn validate_test_compile(
    funcoid: Oid,
    is_dml_trigger: bool,
    is_event_trigger: bool,
) -> PgResult<()> {
    // The validator's fake fcinfo zeroes fncollation (== InvalidOid) and has no
    // call expression; the for_validator compile branch substitutes the integer
    // family for a polymorphic return type itself, so resolved_rettype stays
    // InvalidOid.
    let _function = compile_proc_from_row(
        funcoid,
        is_dml_trigger,
        is_event_trigger,
        /* fn_input_collation = */ types_core::InvalidOid,
        /* for_validator = */ true,
        /* resolved_rettype = */ types_core::InvalidOid,
        // Validator has no call expression; the compile body's forValidator
        // branch substitutes the int4 family for any polymorphic argument.
        /* call_expr = */ None,
    )?;
    Ok(())
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
