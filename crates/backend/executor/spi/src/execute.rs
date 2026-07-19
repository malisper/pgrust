use datum::Datum;
use elog::ereport;
use snapmgr::Snapshot;
use tcop_dest::{CreateDestReceiver, DestReceiver};
use types_dest::CommandDest;
use types_error::{PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_SYNTAX_ERROR, ERROR};
use types_nodes::nodes_enums::CmdType;
use types_nodes::plannodes::PlannedStmt;
use types_nodes::NodeTag;
use types_portal::params::{ParamExternData, PARAM_FLAG_CONST};
use types_portal::{
    ParamListHandle, QueryCompletion, QueryDescHandle, QueryEnvHandle, CMDTAG_SELECT,
    CURSOR_OPT_PARALLEL_OK,
};
use types_scan::sdir::ForwardScanDirection;
use types_slot::EXEC_FLAG_SKIP_TRIGGERS;

use crate::plan::{self, SpiPlanPtr, SpiPlanState};
use crate::{
    current_exec_mcx, set_spi_processed, set_spi_tuptable, with_current,
    TuptabHandle, _SPI_begin_call, _SPI_end_call, SPI_ERROR_ARGUMENT, SPI_ERROR_COPY,
    SPI_ERROR_OPUNKNOWN, SPI_ERROR_PARAM, SPI_ERROR_TRANSACTION, SPI_OK_DELETE,
    SPI_OK_DELETE_RETURNING, SPI_OK_INSERT, SPI_OK_INSERT_RETURNING, SPI_OK_MERGE,
    SPI_OK_MERGE_RETURNING, SPI_OK_REWRITTEN, SPI_OK_SELECT, SPI_OK_SELINTO, SPI_OK_UPDATE,
    SPI_OK_UPDATE_RETURNING, SPI_OK_UTILITY,
};

pub struct SpiExecuteOptions {
    pub params: ParamListHandle,
    pub read_only: bool,
    pub allow_nonatomic: bool,
    pub must_return_tuples: bool,
    pub tcount: u64,
}

impl Default for SpiExecuteOptions {
    fn default() -> Self {
        SpiExecuteOptions {
            params: ParamListHandle::NULL,
            read_only: false,
            allow_nonatomic: false,
            must_return_tuples: false,
            tcount: 0,
        }
    }
}

pub fn SPI_execute(src: &str, read_only: bool, tcount: i64) -> PgResult<i32> {
    if tcount < 0 {
        return Ok(SPI_ERROR_ARGUMENT);
    }
    let res = _SPI_begin_call(true);
    if res < 0 {
        return Ok(res);
    }

    let mut plan = plan::prepare_oneshot(src, CURSOR_OPT_PARALLEL_OK)?;
    let options = SpiExecuteOptions {
        read_only,
        tcount: tcount as u64,
        ..Default::default()
    };
    let res = _SPI_execute_plan(&plan, &options, None, None, true);
    plan::drop_state_sources(&mut plan);
    let res = res?;

    _SPI_end_call(true);
    Ok(res)
}

pub fn SPI_exec(src: &str, tcount: i64) -> PgResult<i32> {
    SPI_execute(src, false, tcount)
}

// SPI_execute_extended's params leg (spi.c): one-shot plan, $n types drawn
// from the caller's param list (C paramlist_parser_setup equivalent).
pub fn SPI_execute_extended(
    src: &str,
    argtypes: &[types_core::Oid],
    values: &[Datum],
    nulls: &[bool],
    read_only: bool,
) -> PgResult<i32> {
    if argtypes.len() != values.len() || values.len() != nulls.len() {
        return Ok(SPI_ERROR_PARAM);
    }
    let res = _SPI_begin_call(true);
    if res < 0 {
        return Ok(res);
    }

    let mut plan = plan::prepare_oneshot_args(src, CURSOR_OPT_PARALLEL_OK, argtypes)?;
    // C SPI_execute_extended: caller-materialized params, no paramFetch hook
    // (plpgsql EXECUTE ... USING builds a plain list there too).
    let params = convert_params(argtypes, values, nulls, false)?;
    let options = SpiExecuteOptions {
        params,
        read_only,
        ..Default::default()
    };
    let res = _SPI_execute_plan(&plan, &options, None, None, true);
    if !params.is_null() {
        types_portal::params::free(params);
    }
    plan::drop_state_sources(&mut plan);
    let res = res?;

    _SPI_end_call(true);
    Ok(res)
}

pub fn SPI_execute_plan(
    ptr: SpiPlanPtr,
    values: &[Datum],
    nulls: &[bool],
    read_only: bool,
    tcount: i64,
) -> PgResult<i32> {
    execute_plan_common(ptr, values, nulls, false, read_only, false, tcount, None, None, true)
}

// SPI_execute_plan_with_paramlist (spi.c): C's entry for a PL-built
// ParamListInfo carrying a paramFetch hook (plpgsql setup_param_list). This
// port materializes PL variables into value arrays, so the shape matches
// SPI_execute_plan; the surviving C-observable difference is the hooked
// provenance bit on the registered params (params.c BuildParamLogString
// suppression — auto_explain's Query Parameters line).
pub fn SPI_execute_plan_with_paramlist(
    ptr: SpiPlanPtr,
    values: &[Datum],
    nulls: &[bool],
    read_only: bool,
    tcount: i64,
) -> PgResult<i32> {
    execute_plan_common(ptr, values, nulls, true, read_only, false, tcount, None, None, true)
}

// SPI_execute_plan_extended's allow_nonatomic leg (spi.c); params ride the
// values/nulls arrays like SPI_execute_plan. The only in-tree caller is
// plpgsql (C: options.params = a hooked PL paramLI), hence params_hooked.
pub fn SPI_execute_plan_extended(
    ptr: SpiPlanPtr,
    values: &[Datum],
    nulls: &[bool],
    params_hooked: bool,
    read_only: bool,
    allow_nonatomic: bool,
    tcount: i64,
) -> PgResult<i32> {
    execute_plan_common(
        ptr,
        values,
        nulls,
        params_hooked,
        read_only,
        allow_nonatomic,
        tcount,
        None,
        None,
        true,
    )
}

pub fn SPI_execp(ptr: SpiPlanPtr, values: &[Datum], nulls: &[bool], tcount: i64) -> PgResult<i32> {
    SPI_execute_plan(ptr, values, nulls, false, tcount)
}

pub fn SPI_execute_snapshot(
    ptr: SpiPlanPtr,
    values: &[Datum],
    nulls: &[bool],
    snapshot: Option<Snapshot>,
    crosscheck_snapshot: Option<Snapshot>,
    read_only: bool,
    fire_triggers: bool,
    tcount: i64,
) -> PgResult<i32> {
    execute_plan_common(
        ptr,
        values,
        nulls,
        false,
        read_only,
        false,
        tcount,
        snapshot,
        crosscheck_snapshot,
        fire_triggers,
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_plan_common(
    ptr: SpiPlanPtr,
    values: &[Datum],
    nulls: &[bool],
    params_hooked: bool,
    read_only: bool,
    allow_nonatomic: bool,
    tcount: i64,
    snapshot: Option<Snapshot>,
    crosscheck_snapshot: Option<Snapshot>,
    fire_triggers: bool,
) -> PgResult<i32> {
    if ptr.is_null() || tcount < 0 {
        return Ok(SPI_ERROR_ARGUMENT);
    }
    let Some(state) = plan::state_snapshot(ptr) else {
        return Ok(SPI_ERROR_ARGUMENT);
    };
    if state.argtypes.len() != values.len() || values.len() != nulls.len() {
        return Ok(SPI_ERROR_PARAM);
    }
    let res = _SPI_begin_call(true);
    if res < 0 {
        return Ok(res);
    }

    let params = convert_params(&state.argtypes, values, nulls, params_hooked)?;
    let options = SpiExecuteOptions {
        params,
        read_only,
        allow_nonatomic,
        tcount: tcount as u64,
        ..Default::default()
    };
    let res = _SPI_execute_plan(&state, &options, snapshot, crosscheck_snapshot, fire_triggers);
    if !params.is_null() {
        types_portal::params::free(params);
    }
    let res = res?;

    _SPI_end_call(true);
    Ok(res)
}

// C _SPI_convert_params: the ParamExternData array lives in the SPI exec
// context (address-stable until _SPI_end_call resets it, after params::free).
pub(crate) fn convert_params(
    argtypes: &[types_core::Oid],
    values: &[Datum],
    nulls: &[bool],
    hooked: bool,
) -> PgResult<ParamListHandle> {
    if argtypes.is_empty() {
        return Ok(ParamListHandle::NULL);
    }
    let mcx = current_exec_mcx();
    let mut v: mcx::PgVec<'static, ParamExternData> = mcx::vec_with_capacity_in(mcx, argtypes.len())?;
    for i in 0..argtypes.len() {
        v.push(ParamExternData {
            value: values[i],
            isnull: nulls[i],
            pflags: PARAM_FLAG_CONST,
            ptype: argtypes[i],
        });
    }
    let slice = mcx::vec_borrow_in(mcx, v)?;
    // SAFETY: slice outlives free() — freed in execute_plan_common before the
    // exec-context reset.
    Ok(unsafe {
        if hooked {
            types_portal::params::register_hooked(slice)
        } else {
            types_portal::params::register(slice)
        }
    })
}

pub(crate) fn command_tag_of(stmt: &PlannedStmt<'_>) -> types_core::CommandTag {
    match stmt.utilityStmt {
        Some(u) => utility_seams::create_command_tag::call(u),
        None => match stmt.commandType {
            CmdType::CMD_SELECT => CMDTAG_SELECT,
            CmdType::CMD_INSERT => types_portal::CMDTAG_INSERT,
            CmdType::CMD_UPDATE => types_portal::CMDTAG_UPDATE,
            CmdType::CMD_DELETE => types_portal::CMDTAG_DELETE,
            CmdType::CMD_MERGE => types_portal::CMDTAG_MERGE,
            _ => types_portal::CMDTAG_UNKNOWN,
        },
    }
}

pub(crate) fn _SPI_execute_plan(
    plan: &SpiPlanState,
    options: &SpiExecuteOptions,
    snapshot: Option<Snapshot>,
    crosscheck_snapshot: Option<Snapshot>,
    fire_triggers: bool,
) -> PgResult<i32> {
    let atomic = with_current(|c| c.atomic).expect("SPI: not connected");
    let allow_nonatomic = options.allow_nonatomic && !atomic && !xact::IsSubTransaction();

    let mut pushed_active_snap = false;
    let mut held_cplan: Option<types_portal::CachedPlanHandle> = None;
    let mut my_processed: u64 = 0;
    let mut my_tuptable: Option<u64> = None;

    let result = (|| -> PgResult<i32> {
        let mut my_res: i32 = 0;

        if let Some(snap) = &snapshot {
            debug_assert!(!options.allow_nonatomic);
            if options.read_only {
                snapmgr::PushActiveSnapshot(snap)?;
            } else {
                snapmgr::PushCopiedSnapshot(snap)?;
            }
            pushed_active_snap = true;
        }

        if options.must_return_tuples && plan.sources.is_empty() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg("empty query does not return tuples")
                .into_error()
                .into());
        }

        for &(psrc, stmt_index) in &plan.sources {
            if plan.oneshot {
                plan::complete_source(psrc, stmt_index, &plan.argtypes, plan.cursor_options)?;
            }

            if options.must_return_tuples && plancache::CachedPlanResultDesc(psrc).is_none() {
                let tag = plancache::CachedPlanCommandTag(psrc);
                let cmdname = if tag == CMDTAG_SELECT {
                    "SELECT INTO"
                } else {
                    cmdtag::GetCommandTagName(tag)
                };
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!("{cmdname} query does not return tuples"))
                    .into_error()
                    .into());
            }

            let cplan =
                plancache::GetCachedPlan(psrc, options.params, None, crate::current_query_env())?;
            held_cplan = Some(cplan);
            let stmt_list = plancache::CachedPlanStmtList(cplan);
            let query_string = plancache::CachedPlanQueryString(psrc);

            if snapshot.is_none()
                && (stmt_list.len() > 1
                    || (stmt_list.len() == 1
                        && pquery::PlannedStmtRequiresSnapshot(&stmt_list[0])))
            {
                pquery::EnsurePortalSnapshotExists()?;
                if !options.read_only && !allow_nonatomic {
                    if pushed_active_snap {
                        snapmgr::PopActiveSnapshot()?;
                    }
                    let snap = snapmgr::GetTransactionSnapshot()?;
                    snapmgr::PushActiveSnapshot(&snap)?;
                    pushed_active_snap = true;
                }
            }

            for stmt in stmt_list {
                let can_set_tag = stmt.canSetTag;

                with_current(|c| {
                    c.processed = 0;
                    c.tuptable = None;
                });

                if let Some(ustmt) = stmt.utilityStmt {
                    match ustmt.node_tag() {
                        NodeTag::T_CopyStmt => {
                            let cstmt = ustmt.as_copy_stmt().expect("tag-checked");
                            if cstmt.filename.is_none() {
                                my_res = SPI_ERROR_COPY;
                                return Ok(my_res);
                            }
                        }
                        NodeTag::T_TransactionStmt => {
                            my_res = SPI_ERROR_TRANSACTION;
                            return Ok(my_res);
                        }
                        _ => {}
                    }
                }

                if options.read_only && !utility::CommandIsReadOnly(stmt) {
                    let name = cmdtag::GetCommandTagName(command_tag_of(stmt));
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg(format!("{name} is not allowed in a non-volatile function"))
                        .into_error()
                        .into());
                }

                if !options.read_only && pushed_active_snap {
                    xact::CommandCounterIncrement()?;
                    snapmgr::UpdateActiveSnapshotCommandId()?;
                }

                let mut dest = CreateDestReceiver(if can_set_tag {
                    CommandDest::Spi
                } else {
                    CommandDest::None
                });

                let res = if stmt.utilityStmt.is_none() {
                    let snap = snapmgr::ActiveSnapshotSet().then(snapmgr::GetActiveSnapshot);
                    let qd = execmain_seams::create_query_desc::call(
                        stmt,
                        query_string,
                        snap,
                        crosscheck_snapshot.clone(),
                        dest.mydest(),
                        options.params,
                        crate::current_query_env(),
                        0,
                    )?;
                    let tcount = if can_set_tag { options.tcount } else { 0 };
                    let mut qd_owner = pquery::QueryDescOwner(qd);
                    let r = _SPI_pquery(qd, stmt, &mut dest, fire_triggers, tcount)?;
                    qd_owner.disarm();
                    execmain_seams::free_query_desc::call(qd);
                    r
                } else {
                    let context = if allow_nonatomic {
                        utility_seams::ProcessUtilityContext::PROCESS_UTILITY_QUERY_NONATOMIC
                    } else {
                        utility_seams::ProcessUtilityContext::PROCESS_UTILITY_QUERY
                    };
                    let mut qc = QueryCompletion::default();
                    cmdtag::InitializeQueryCompletion(&mut qc);
                    let mcx = current_exec_mcx();
                    // C passes readOnlyTree=true and copyObjects the tree in
                    // ProcessUtility; a oneshot tree dies with this call, so
                    // the copy is skipped (saved plans keep the loud arm).
                    utility_seams::process_utility::call(
                        mcx,
                        stmt,
                        query_string,
                        !plan.oneshot,
                        context,
                        options.params,
                        crate::current_query_env(),
                        &mut dest,
                        Some(&mut qc),
                    )?;

                    with_current(|c| {
                        if let Some(id) = c.tuptable {
                            c.processed = crate::tuptable::numvals_of(&c.tuptables, id);
                        }
                    });

                    let mut ures = SPI_OK_UTILITY;
                    let ustmt = stmt.utilityStmt.expect("utility arm");
                    match ustmt.node_tag() {
                        NodeTag::T_CreateTableAsStmt => {
                            let ctastmt = ustmt
                                .as_variant::<types_nodes::rawnodes::CreateTableAsStmt>()
                                .expect("tag-checked");
                            if qc.commandTag == CMDTAG_SELECT {
                                with_current(|c| c.processed = qc.nprocessed);
                            } else {
                                // Must be an IF NOT EXISTS that did nothing, or a
                                // CREATE ... WITH NO DATA.
                                debug_assert!(
                                    ctastmt.if_not_exists
                                        || ctastmt
                                            .into
                                            .and_then(|n| {
                                                n.as_variant::<types_nodes::rawnodes::IntoClause>()
                                            })
                                            .is_some_and(|ic| ic.skipData)
                                );
                                with_current(|c| c.processed = 0);
                            }
                            // For historical reasons, CREATE TABLE AS spelled as
                            // SELECT INTO returns a special return code.
                            if ctastmt.is_select_into {
                                ures = SPI_OK_SELINTO;
                            }
                        }
                        NodeTag::T_CopyStmt => {
                            with_current(|c| c.processed = qc.nprocessed);
                        }
                        _ => {}
                    }
                    ures
                };

                if can_set_tag {
                    let (processed, tuptable) =
                        with_current(|c| (c.processed, c.tuptable)).expect("connected");
                    my_processed = processed;
                    if let Some(old) = my_tuptable.take() {
                        free_tuptable_id(old);
                    }
                    my_tuptable = tuptable;
                    my_res = res;
                } else {
                    let tuptable = with_current(|c| c.tuptable.take()).flatten();
                    if let Some(id) = tuptable {
                        free_tuptable_id(id);
                    }
                }

                if res < 0 {
                    my_res = res;
                    return Ok(my_res);
                }
            }

            plancache::ReleaseCachedPlan(held_cplan.take().expect("held above"));

            // Post-list CCI so DDL is visible to the next CachedPlanSource.
            if !options.read_only {
                xact::CommandCounterIncrement()?;
            }
        }

        Ok(my_res)
    })();

    if pushed_active_snap {
        let popped = snapmgr::PopActiveSnapshot();
        if result.is_ok() {
            popped?;
        }
    }
    if let Some(cplan) = held_cplan.take() {
        plancache::ReleaseCachedPlan(cplan);
    }

    let mut my_res = result?;

    set_spi_processed(my_processed);
    set_spi_tuptable(my_tuptable.map(TuptabHandle));
    with_current(|c| c.tuptable = None);

    if my_res == 0 {
        my_res = SPI_OK_REWRITTEN;
    }
    Ok(my_res)
}

fn free_tuptable_id(id: u64) {
    let _ = crate::SPI_freetuptable(TuptabHandle(id));
}

fn _SPI_checktuples() -> bool {
    with_current(|c| match c.tuptable {
        None => true,
        Some(id) => crate::tuptable::numvals_of(&c.tuptables, id) != c.processed,
    })
    .unwrap_or(true)
}

fn _SPI_pquery(
    qd: QueryDescHandle,
    stmt: &PlannedStmt<'_>,
    dest: &mut DestReceiver<'_>,
    fire_triggers: bool,
    tcount: u64,
) -> PgResult<i32> {
    let res = match stmt.commandType {
        CmdType::CMD_SELECT => {
            if dest.mydest() == CommandDest::None {
                SPI_OK_UTILITY
            } else {
                SPI_OK_SELECT
            }
        }
        CmdType::CMD_INSERT => {
            if stmt.hasReturning {
                SPI_OK_INSERT_RETURNING
            } else {
                SPI_OK_INSERT
            }
        }
        CmdType::CMD_DELETE => {
            if stmt.hasReturning {
                SPI_OK_DELETE_RETURNING
            } else {
                SPI_OK_DELETE
            }
        }
        CmdType::CMD_UPDATE => {
            if stmt.hasReturning {
                SPI_OK_UPDATE_RETURNING
            } else {
                SPI_OK_UPDATE
            }
        }
        CmdType::CMD_MERGE => {
            if stmt.hasReturning {
                SPI_OK_MERGE_RETURNING
            } else {
                SPI_OK_MERGE
            }
        }
        _ => return Ok(SPI_ERROR_OPUNKNOWN),
    };

    let eflags = if fire_triggers { 0 } else { EXEC_FLAG_SKIP_TRIGGERS };
    execmain_seams::executor_start::call(qd, eflags)?;
    execmain_seams::executor_run::call(qd, ForwardScanDirection, tcount, dest)?;
    with_current(|c| c.processed = execmain_seams::query_desc_es_processed::call(qd));

    if (res == SPI_OK_SELECT || stmt.hasReturning) && dest.mydest() == CommandDest::Spi {
        if _SPI_checktuples() {
            return Err(ereport(ERROR)
                .errmsg_internal("consistency check on SPI tuple count failed")
                .into_error()
                .into());
        }
    }

    execmain_seams::executor_finish::call(qd)?;
    execmain_seams::executor_end::call(qd)?;
    Ok(res)
}
