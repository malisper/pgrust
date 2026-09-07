// postgres_fdw.c modify half: postgresAddForeignUpdateTargets,
// postgresPlanForeignModify, postgresBeginForeignModify (create_foreign_modify),
// postgresExecForeignInsert/Update/Delete (execute_foreign_modify over a
// remote prepared statement), postgresEndForeignModify (finish_foreign_modify),
// postgresIsForeignRelUpdatable, batch insert (text params buffered here,
// flushed as C's N-row VALUES prepare/execute), and direct modify
// (postgresPlanDirectModify + Begin/Iterate/EndDirectModify, base-rel arm).
use std::ffi::CStr;

use datum::Datum;
use mcx::{Mcx, PgString, PgVec};
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult};
use types_fmgr::FmgrInfo;
use types_nodes::list::{IntList, NodeList};
use types_nodes::nodes_enums::CmdType;
use types_nodes::plannodes::ModifyTable;
use types_nodes::{Node, OnConflictAction};
use types_tuple::htup::SelfItemPointerAttributeNumber;
use types_tuple::ItemPointerData;

use executils::{EStateData, ExecSlotId};
use pgclient::ExecStatus;
use planner::run::PlannerRun;

use crate::connection;
use crate::deparse;
use crate::exec::{convert_result_row, AttInMeta};

const FIRST_LOW_INVALID_HEAP_ATTNUM: i32 = types_tuple::htup::FirstLowInvalidHeapAttributeNumber;

// ---------- planner half ----------

// postgresAddForeignUpdateTargets: register a junk "ctid" row-identity Var.
pub(crate) fn add_foreign_update_targets<'mcx>(
    mcx: Mcx<'mcx>,
    rtindex: u32,
    register: &mut dyn FnMut(Node<'mcx>, &'static str) -> PgResult<()>,
) -> PgResult<()> {
    let var = Node::mk_var(
        mcx,
        rtindex as i32,
        SelfItemPointerAttributeNumber as i16,
        types_core::catalog::TIDOID,
        -1,
        0,
        0,
    )?;
    register(var, "ctid")
}

fn mcx_str<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let bytes = mcx::slice_borrow_in(mcx, s.as_bytes())?;
    // SAFETY: bytes copied verbatim from a &str.
    Ok(unsafe { core::str::from_utf8_unchecked(bytes) })
}

// postgresPlanForeignModify: deparse the remote DML and return the
// fdw_private list in FdwModifyPrivateIndex order:
// [UpdateSql, TargetAttnums, Len(values_end, -1 unless INSERT),
//  HasReturning, RetrievedAttrs], plus a sixth pgrust-only entry: the local
// query has a RETURNING list (C's ri_projectReturning, see the batch size).
pub(crate) fn plan_foreign_modify<'mcx>(
    run: &mut PlannerRun<'mcx>,
    plan: &ModifyTable<'mcx>,
    result_relation: u32,
    subplan_index: usize,
) -> PgResult<NodeList<'mcx>> {
    let mcx = run.mcx;
    let operation = plan.operation;
    let rte = run.rte(result_relation as usize);
    let rel = table::table_open(mcx, rte.relid, types_rel::lock::NoLock)?;

    let trigdesc = if rel.rd_hastriggers {
        relcache_seams::relation_get_trigger_desc::call(rel.rd_id)?
    } else {
        None
    };
    let trig_update_before = trigdesc.as_deref().is_some_and(|t| t.trig_update_before_row);
    let trig_after_row = match operation {
        CmdType::CMD_INSERT => trigdesc.as_deref().is_some_and(|t| t.trig_insert_after_row),
        CmdType::CMD_UPDATE => trigdesc.as_deref().is_some_and(|t| t.trig_update_after_row),
        CmdType::CMD_DELETE => trigdesc.as_deref().is_some_and(|t| t.trig_delete_after_row),
        _ => false,
    };

    // In an INSERT, transmit all columns; in an UPDATE with BEFORE ROW UPDATE
    // triggers likewise; otherwise only the explicitly updated columns
    // (get_rel_all_updated_cols: perminfo updatedCols + dependent generated).
    let mut target_attrs: PgVec<'mcx, i32> = PgVec::new_in(mcx);
    if operation == CmdType::CMD_INSERT
        || (operation == CmdType::CMD_UPDATE && trig_update_before)
    {
        for attnum in 1..=rel.rd_att.natts as usize {
            if !rel.rd_att.attr(attnum - 1).attisdropped {
                target_attrs.push(attnum as i32);
            }
        }
    } else if operation == CmdType::CMD_UPDATE {
        // get_rel_all_updated_cols: the result relation's updatedCols mapped
        // to this (possibly child) target's attnums, plus dependent
        // generated columns.
        let all_updated = planner::get_rel_all_updated_cols(run, result_relation)?;
        let mut col = -1i32;
        loop {
            col = all_updated.next_member(col);
            if col < 0 {
                break;
            }
            let attno = col + FIRST_LOW_INVALID_HEAP_ATTNUM;
            if attno <= 0 {
                return Err(Box::new(PgError::error(
                    "system-column update is not supported",
                )));
            }
            target_attrs.push(attno);
        }
    }

    let wco_list: Vec<Node<'mcx>> = if plan.withCheckOptionLists.is_nil() {
        Vec::new()
    } else {
        plan.withCheckOptionLists
            .nth(subplan_index)
            .as_list()
            .expect("withCheckOptionLists cell is a List")
            .iter()
            .collect()
    };
    let returning_list: Vec<Node<'mcx>> = if plan.returningLists.is_nil() {
        Vec::new()
    } else {
        plan.returningLists
            .nth(subplan_index)
            .as_list()
            .expect("returningLists cell is a List")
            .iter()
            .collect()
    };

    // Only ON CONFLICT DO NOTHING without an inference specification is
    // supported (the optimizer already rejects arbiters on foreign tables).
    let do_nothing = if plan.onConflictAction == OnConflictAction::ONCONFLICT_NOTHING as u32 {
        true
    } else if plan.onConflictAction == OnConflictAction::ONCONFLICT_NONE as u32
        || plan.onConflictAction == 0
    {
        false
    } else {
        // postgres_fdw.c:1867, elog(ERROR).
        return Err(Box::new(PgError::error(format!(
            "unexpected ON CONFLICT specification: {}",
            plan.onConflictAction
        ))));
    };

    let mut sql: PgString<'mcx> = PgString::new_in(mcx);
    let mut retrieved_attrs: PgVec<'mcx, i32> = PgVec::new_in(mcx);
    let mut values_end_len: i32 = -1;
    let rti = result_relation as i32;
    match operation {
        CmdType::CMD_INSERT => {
            values_end_len = deparse::deparse_insert_sql(
                &mut sql,
                mcx,
                run,
                rte,
                rti,
                &rel,
                &target_attrs,
                do_nothing,
                trig_after_row,
                &wco_list,
                &returning_list,
                &mut retrieved_attrs,
            )?;
        }
        CmdType::CMD_UPDATE => {
            deparse::deparse_update_sql(
                &mut sql,
                mcx,
                run,
                rte,
                rti,
                &rel,
                &target_attrs,
                trig_after_row,
                &wco_list,
                &returning_list,
                &mut retrieved_attrs,
            )?;
        }
        CmdType::CMD_DELETE => {
            deparse::deparse_delete_sql(
                &mut sql,
                mcx,
                run,
                rte,
                rti,
                &rel,
                trig_after_row,
                &returning_list,
                &mut retrieved_attrs,
            )?;
        }
        other => {
            // postgres_fdw.c:1893, elog(ERROR).
            return Err(Box::new(PgError::error(format!(
                "unexpected operation: {}",
                other as i32
            ))));
        }
    }
    table::table_close(rel, types_rel::lock::NoLock)?;

    let mut fdw_private: NodeList<'mcx> = NodeList::nil();
    fdw_private.lappend(mcx, Node::mk_string(mcx, mcx_str(mcx, sql.as_str())?)?)?;
    let mut ta: IntList<'mcx> = IntList::nil();
    for &a in target_attrs.iter() {
        ta.lappend(mcx, a)?;
    }
    fdw_private.lappend(mcx, Node::mk_int_list(mcx, ta)?)?;
    fdw_private.lappend(mcx, Node::mk_integer(mcx, values_end_len)?)?;
    fdw_private.lappend(mcx, Node::mk_boolean(mcx, !retrieved_attrs.is_empty())?)?;
    let mut ra: IntList<'mcx> = IntList::nil();
    for &a in retrieved_attrs.iter() {
        ra.lappend(mcx, a)?;
    }
    fdw_private.lappend(mcx, Node::mk_int_list(mcx, ra)?)?;
    // Sixth entry (pgrust only): the local query has a RETURNING list. C reads
    // resultRelInfo->ri_projectReturning in postgresGetForeignModifyBatchSize
    // (postgres_fdw.c:2070); the plan carries it because neither the modify
    // hooks nor the EXPLAIN hook see the ResultRelInfo here.
    fdw_private.lappend(mcx, Node::mk_boolean(mcx, !returning_list.is_empty())?)?;
    Ok(fdw_private)
}

// postgresIsForeignRelUpdatable: server/table "updatable" option, default on.
pub(crate) fn is_foreign_rel_updatable<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<i32> {
    let mut updatable = true;
    let table = foreigncmds::foreign::GetForeignTable(mcx, relid)?;
    let server = foreigncmds::foreign::GetForeignServer(mcx, table.serverid)?;
    for opt in server.options.iter() {
        if opt.name == "updatable" {
            updatable = parse_bool(opt.require_value()?);
        }
    }
    for opt in table.options.iter() {
        if opt.name == "updatable" {
            updatable = parse_bool(opt.require_value()?);
        }
    }
    Ok(if updatable {
        (1 << CmdType::CMD_INSERT as i32)
            | (1 << CmdType::CMD_UPDATE as i32)
            | (1 << CmdType::CMD_DELETE as i32)
    } else {
        0
    })
}

// get_batch_size_option: server option overridden by table option, default 1.
fn get_batch_size_option(mcx: Mcx<'_>, relid: Oid) -> PgResult<i32> {
    let mut batch_size = 1i32;
    let table = foreigncmds::foreign::GetForeignTable(mcx, relid)?;
    let server = foreigncmds::foreign::GetForeignServer(mcx, table.serverid)?;
    for opt in server.options.iter() {
        if opt.name == "batch_size" {
            if let Ok(v) = opt.require_value()?.parse::<i32>() {
                batch_size = v;
            }
        }
    }
    for opt in table.options.iter() {
        if opt.name == "batch_size" {
            if let Ok(v) = opt.require_value()?.parse::<i32>() {
                batch_size = v;
            }
        }
    }
    Ok(batch_size)
}

pub(crate) fn parse_bool(value: &str) -> bool {
    // defGetBoolean's accepted spellings; validator-checked upstream.
    matches!(
        value.to_ascii_lowercase().as_str(),
        "true" | "t" | "on" | "1" | "yes" | "y"
    )
}

// ---------- executor half ----------

// C PgFdwModifyState (no aux_fmstate — routed foreign inserts and COPY into
// foreign tables are refused upstream). Batch insert buffers text params here
// (C buffers slots in the executor; the wire shape — one N-row VALUES prepare
// + one execute per batch — is identical).
struct PgFdwModifyState {
    conn_key: Oid,
    rti: u32,
    p_name: Option<String>,
    query: String,
    orig_query: String,
    values_end: i32,
    /// Params per row (non-generated targets); 0 for DELETE.
    p_nums: usize,
    /// Effective batch size (1 = no batching).
    batch_size: i32,
    /// Rows the currently prepared statement's VALUES clause covers.
    num_slots: i32,
    pending_params: Vec<Option<String>>,
    pending_rows: i32,
    target_attrs: Vec<i32>,
    // per-target_attrs entry: attgenerated columns transmit as DEFAULT.
    attgenerated: Vec<bool>,
    has_returning: bool,
    retrieved_attrs: Vec<i32>,
    attin: Option<AttInMeta>,
    /// ModifyTable.canSetTag: a flushed batch adds the remote's row count to
    /// es_processed only for the tag-setting statement (nodeModifyTable.c:1411).
    can_set_tag: bool,
    ctid_attno: i16,
    // [tid output for UPDATE/DELETE] + per-non-generated-target output fns.
    p_flinfo: Vec<FmgrInfo>,
    // Param-conversion scratch (C temp_cxt), reset per call.
    temp_mcx: mcx::MemoryContext,
    // RETURNING datum payloads: live until the projected row is consumed;
    // reset at the START of the next modify call (scan batch_mcx precedent).
    returning_mcx: mcx::MemoryContext,
}

fn exec_find_junk_attribute_in_tlist(tlist: &types_nodes::NodeList<'_>, attr_name: &str) -> i16 {
    for tle_node in tlist {
        let tle = tle_node.as_target_entry().expect("TargetEntry");
        if tle.resjunk && tle.resname == Some(attr_name) {
            return tle.resno;
        }
    }
    0
}

// postgresBeginForeignModify + create_foreign_modify.
pub(crate) fn begin_foreign_modify<'mcx>(
    node: &'mcx ModifyTable<'mcx>,
    rti: u32,
    list_index: usize,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<Option<Box<dyn core::any::Any>>> {
    if eflags & types_slot::EXEC_FLAG_EXPLAIN_ONLY != 0 {
        return Ok(None);
    }
    let mcx = estate.es_query_cxt;

    // Deconstruct fdw_private (FdwModifyPrivateIndex order).
    let fp = node
        .fdwPrivLists
        .nth(list_index)
        .as_list()
        .expect("fdwPrivLists cell is a List");
    let mut it = fp.iter();
    let query = it
        .next()
        .and_then(|n| n.as_string())
        .expect("fdw_private[0] is the remote DML")
        .sval
        .to_string();
    let target_attrs: Vec<i32> = it
        .next()
        .and_then(|n| n.as_int_list())
        .expect("fdw_private[1] is targetAttnums")
        .iter()
        .collect();
    let values_end = it
        .next()
        .and_then(|n| n.as_integer())
        .expect("fdw_private[2] is values_end_len")
        .ival;
    let has_returning = it
        .next()
        .and_then(|n| n.as_boolean())
        .expect("fdw_private[3] is hasReturning")
        .boolval;
    let retrieved_attrs: Vec<i32> = it
        .next()
        .and_then(|n| n.as_int_list())
        .expect("fdw_private[4] is retrieved_attrs")
        .iter()
        .collect();
    let local_returning =
        it.next().and_then(|n| n.as_boolean()).map(|b| b.boolval).unwrap_or(false);

    // ExecGetResultRelCheckAsUser (execUtils.c:1489): the checkAsUser of the
    // result relation's RTEPermissionInfo, else the current user. A child
    // result relation (a partition / inheritance child UPDATE target, or a
    // routed INSERT leaf) has no perminfo of its own: C reads the root target
    // relation's through ri_RootResultRelInfo (nodeModifyTable.c:4862), which
    // exists exactly when the plan carries rootRelation.
    let perm_rti = if node.rootRelation > 0 { node.rootRelation } else { rti };
    let rte = estate.exec_rt_fetch(perm_rti);
    let mut userid = miscinit::GetUserId();
    if rte.perminfoindex > 0 {
        if let Some(pis) = estate.es_rteperminfos {
            let pi = pis
                .nth(rte.perminfoindex as usize - 1)
                .as_rte_permission_info()
                .expect("permInfos cell");
            if pi.checkAsUser != InvalidOid {
                userid = pi.checkAsUser;
            }
        }
    }

    // Snapshot what we need from the relation before touching connections.
    let (rd_id, attin, attgenerated, out_fn_oids) = {
        let rel = estate.es_relations[(rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        let attin = AttInMeta::build(rel.name(), &rel.rd_att)?;
        let mut gens = Vec::with_capacity(target_attrs.len());
        let mut outs = Vec::new();
        for &attnum in &target_attrs {
            let att = rel.rd_att.attr(attnum as usize - 1);
            debug_assert!(!att.attisdropped);
            gens.push(att.attgenerated != 0);
            if att.attgenerated == 0 {
                outs.push(att.atttypid);
            }
        }
        (rel.rd_id, attin, gens, outs)
    };

    let table = foreigncmds::foreign::GetForeignTable(mcx, rd_id)?;
    let user = foreigncmds::foreign::GetUserMapping(mcx, userid, table.serverid)?;
    let conn_key = connection::get_connection(mcx, &user, true)?;

    let operation = node.operation;

    // GetForeignModifyBatchSize (folded into begin: the option value, gated
    // by RETURNING / WCO / row triggers, clamped by the 65535-param limit).
    let p_nums = target_attrs
        .iter()
        .zip(attgenerated.iter())
        .filter(|&(_, &g)| !g)
        .count();
    let batch_size = if operation != CmdType::CMD_INSERT {
        1
    } else {
        let has_wco = !node.withCheckOptionLists.is_nil()
            && !node
                .withCheckOptionLists
                .nth(list_index)
                .as_list()
                .map(|l| l.is_nil())
                .unwrap_or(true);
        let has_insert_row_triggers = {
            let rel = estate.es_relations[(rti - 1) as usize]
                .as_ref()
                .expect("result relation opened");
            rel.rd_hastriggers
                && relcache_seams::relation_get_trigger_desc::call(rel.rd_id)?
                    .is_some_and(|t| t.trig_insert_before_row || t.trig_insert_after_row)
        };
        // C gates on ri_projectReturning — any local RETURNING list, whether
        // or not the remote statement returns columns (postgres_fdw.c:2070).
        if local_returning || has_wco || has_insert_row_triggers || target_attrs.is_empty() {
            1
        } else {
            let opt = get_batch_size_option(mcx, rd_id)?;
            if p_nums > 0 {
                opt.min((65535 / p_nums) as i32).max(1)
            } else {
                opt.max(1)
            }
        }
    };
    let mut p_flinfo: Vec<FmgrInfo> = Vec::with_capacity(out_fn_oids.len() + 1);
    let mut ctid_attno: i16 = 0;
    if operation == CmdType::CMD_UPDATE || operation == CmdType::CMD_DELETE {
        let subplan = node
            .plan
            .lefttree
            .expect("ModifyTable has a subplan")
            .as_plan()
            .expect("plan node");
        ctid_attno = exec_find_junk_attribute_in_tlist(&subplan.targetlist, "ctid");
        if ctid_attno <= 0 {
            return Err(Box::new(PgError::error("could not find junk ctid column")));
        }
        let (tid_out, _isvarlena) =
            lsyscache::getTypeOutputInfo(types_core::catalog::TIDOID)?;
        p_flinfo.push(fmgr_seams::fmgr_info::call(tid_out)?);
    }
    if operation == CmdType::CMD_INSERT || operation == CmdType::CMD_UPDATE {
        for typid in out_fn_oids {
            let (out_fn, _isvarlena) = lsyscache::getTypeOutputInfo(typid)?;
            p_flinfo.push(fmgr_seams::fmgr_info::call(out_fn)?);
        }
    }

    Ok(Some(Box::new(PgFdwModifyState {
        conn_key,
        rti,
        p_name: None,
        orig_query: query.clone(),
        query,
        values_end,
        p_nums,
        batch_size,
        num_slots: 1,
        pending_params: Vec::new(),
        pending_rows: 0,
        target_attrs,
        attgenerated,
        has_returning,
        retrieved_attrs,
        attin: has_returning.then_some(attin),
        can_set_tag: node.canSetTag,
        ctid_attno,
        p_flinfo,
        temp_mcx: mcx::MemoryContext::new_bump("postgres_fdw temporary data"),
        returning_mcx: mcx::MemoryContext::new_bump("postgres_fdw returning data"),
    })))
}

// OutputFunctionCall through the per-call scratch, copied out as a String.
pub(crate) fn output_to_string(
    flinfo: &mut FmgrInfo,
    scratch: &mcx::MemoryContext,
    value: Datum,
) -> PgResult<String> {
    let d = types_fmgr::function_call1_coll_in(flinfo, InvalidOid, scratch.mcx(), value)?;
    // SAFETY: output functions return a NUL-terminated cstring datum; copied
    // out before the scratch context resets.
    let s = unsafe { CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) };
    Ok(s.to_string_lossy().into_owned())
}

pub(crate) fn exec_foreign_insert<'mcx>(
    state: &mut dyn core::any::Any,
    estate: &mut EStateData<'mcx>,
    slot: ExecSlotId,
    plan_slot: ExecSlotId,
) -> PgResult<bool> {
    execute_foreign_modify(state, estate, CmdType::CMD_INSERT, slot, plan_slot)
}

pub(crate) fn exec_foreign_update<'mcx>(
    state: &mut dyn core::any::Any,
    estate: &mut EStateData<'mcx>,
    slot: ExecSlotId,
    plan_slot: ExecSlotId,
) -> PgResult<bool> {
    execute_foreign_modify(state, estate, CmdType::CMD_UPDATE, slot, plan_slot)
}

pub(crate) fn exec_foreign_delete<'mcx>(
    state: &mut dyn core::any::Any,
    estate: &mut EStateData<'mcx>,
    slot: ExecSlotId,
    plan_slot: ExecSlotId,
) -> PgResult<bool> {
    execute_foreign_modify(state, estate, CmdType::CMD_DELETE, slot, plan_slot)
}

// PQcmdTuples: the trailing count of the command tag ("INSERT 0 1" -> 1).
fn cmd_tuples(tag: &str) -> i64 {
    tag.rsplit(' ')
        .next()
        .and_then(|t| t.parse::<i64>().ok())
        .unwrap_or(0)
}

// execute_foreign_modify (single-row leg).
fn execute_foreign_modify<'mcx>(
    state: &mut dyn core::any::Any,
    estate: &mut EStateData<'mcx>,
    operation: CmdType,
    slot_id: ExecSlotId,
    plan_slot: ExecSlotId,
) -> PgResult<bool> {
    let st = state
        .downcast_mut::<PgFdwModifyState>()
        .expect("ri_FdwState is PgFdwModifyState");
    st.returning_mcx.reset();

    // Batch insert: buffer this row's text params; flush at batch_size (and
    // at end-of-source via the flush hook). Batching is disabled whenever
    // RETURNING/WCO/row triggers apply, so consuming the row here is exact.
    // A buffered row is "not inserted yet" (C ExecInsert returns NULL for it,
    // nodeModifyTable.c:1027): the caller must not count it — the flush adds
    // the remote's own count (postgres_fdw.c:4212 atoi(PQcmdTuples)).
    if operation == CmdType::CMD_INSERT && st.batch_size > 1 {
        let nestlevel = crate::transmission::set_transmission_modes();
        let r = (|| -> PgResult<()> {
            let mut j = 0usize;
            for (k, &attnum) in st.target_attrs.iter().enumerate() {
                if st.attgenerated[k] {
                    continue;
                }
                let mut isnull = false;
                let value = {
                    let s = &mut estate.es_tupleTable[slot_id.0 as usize];
                    exectuples::slot_getattr(s, attnum, &mut isnull)
                };
                if isnull {
                    st.pending_params.push(None);
                } else {
                    st.pending_params.push(Some(output_to_string(
                        &mut st.p_flinfo[j],
                        &st.temp_mcx,
                        value,
                    )?));
                }
                j += 1;
            }
            Ok(())
        })();
        crate::transmission::reset_transmission_modes(nestlevel);
        r?;
        st.pending_rows += 1;
        st.temp_mcx.reset();
        if st.pending_rows >= st.batch_size {
            flush_pending_inserts(st, estate)?;
        }
        return Ok(false);
    }

    // Set up the prepared statement on the remote server, if we didn't yet.
    // Parameter types are intentionally unspecified (remote derives them).
    if st.p_name.is_none() {
        let name = format!("pgsql_fdw_prep_{}", connection::get_prep_stmt_number());
        let res = connection::with_entry(st.conn_key, |e| -> PgResult<_> {
            // C pgfdw_exec_query's process_pending_request guard: settle/park
            // any in-flight async FETCH before using the connection.
            connection::park_pending_entry(e)?;
            Ok(e.conn.as_mut().expect("live connection").prepare(&name, &st.query, &[]))
        })???;
        if res.status != ExecStatus::CommandOk {
            return Err(connection::remote_error(&res, Some(&st.query)));
        }
        st.p_name = Some(name);
    }

    // Convert parameters to text (C convert_prep_stmt_params).
    let mut p_values: Vec<Option<String>> = Vec::with_capacity(st.p_flinfo.len());
    let mut pindex = 0usize;
    if operation == CmdType::CMD_UPDATE || operation == CmdType::CMD_DELETE {
        let mut isnull = false;
        let d = {
            let plan = &mut estate.es_tupleTable[plan_slot.0 as usize];
            exectuples::slot_getattr(plan, st.ctid_attno as i32, &mut isnull)
        };
        if isnull {
            return Err(Box::new(PgError::error("ctid is NULL")));
        }
        // SAFETY: the junk tid datum points at a live ItemPointerData (the
        // scan slot's tts_tid); copied by value before any slot mutation.
        let tid: ItemPointerData = unsafe { *(d.as_usize() as *const ItemPointerData) };
        // No set_transmission_modes for TID output, as C.
        let s = output_to_string(
            &mut st.p_flinfo[0],
            &st.temp_mcx,
            Datum::from_usize(&tid as *const ItemPointerData as usize),
        )?;
        p_values.push(Some(s));
        pindex = 1;
    }
    if (operation == CmdType::CMD_INSERT || operation == CmdType::CMD_UPDATE)
        && !st.target_attrs.is_empty()
    {
        let nestlevel = crate::transmission::set_transmission_modes();
        let r = (|| -> PgResult<()> {
            let mut j = pindex;
            for (k, &attnum) in st.target_attrs.iter().enumerate() {
                if st.attgenerated[k] {
                    continue;
                }
                let mut isnull = false;
                let value = {
                    let s = &mut estate.es_tupleTable[slot_id.0 as usize];
                    exectuples::slot_getattr(s, attnum, &mut isnull)
                };
                if isnull {
                    p_values.push(None);
                } else {
                    p_values.push(Some(output_to_string(
                        &mut st.p_flinfo[j],
                        &st.temp_mcx,
                        value,
                    )?));
                }
                j += 1;
            }
            Ok(())
        })();
        crate::transmission::reset_transmission_modes(nestlevel);
        r?;
    }

    // Execute the prepared statement.
    let params: Vec<Option<&str>> = p_values.iter().map(|v| v.as_deref()).collect();
    let p_name = st.p_name.as_deref().expect("prepared above").to_string();
    let res = connection::with_entry(st.conn_key, |e| -> PgResult<_> {
        // Same async-fetch settle/park guard as the prepare above.
        connection::park_pending_entry(e)?;
        Ok(e.conn.as_mut().expect("live connection").exec_prepared(&p_name, &params))
    })???;
    let expected = if st.has_returning {
        ExecStatus::TuplesOk
    } else {
        ExecStatus::CommandOk
    };
    if res.status != expected {
        return Err(connection::remote_error(&res, Some(&st.query)));
    }

    // Row count + RETURNING data.
    let n_rows = if st.has_returning {
        let n = res.rows.len() as i64;
        if n > 0 {
            store_returning_result(st, estate, slot_id, &res.rows[0])?;
        }
        n
    } else {
        cmd_tuples(&res.cmd_tag)
    };
    st.temp_mcx.reset();
    Ok(n_rows > 0)
}

// The flush half of ExecBatchInsert/execute_foreign_modify: rebuild the
// INSERT for the pending row count when it changed (rebuildInsertSql +
// re-prepare, C's exact wire), then execute with the buffered params.
fn flush_pending_inserts<'mcx>(
    st: &mut PgFdwModifyState,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if st.pending_rows == 0 {
        return Ok(());
    }
    let n = st.pending_rows;
    if st.num_slots != n {
        if let Some(name) = st.p_name.take() {
            let sql = format!("DEALLOCATE {name}");
            let res = connection::exec_query(st.conn_key, &sql)?;
            if res.status != ExecStatus::CommandOk {
                return Err(connection::remote_error(&res, Some(&sql)));
            }
        }
        let mcx = estate.es_query_cxt;
        let rel = estate.es_relations[(st.rti - 1) as usize]
            .as_ref()
            .expect("result relation opened");
        let mut sql: PgString<'mcx> = PgString::new_in(mcx);
        deparse::rebuild_insert_sql(
            &mut sql,
            rel,
            &st.orig_query,
            &st.target_attrs,
            st.values_end,
            st.p_nums as i32,
            n - 1,
        );
        st.query = sql.as_str().to_string();
        st.num_slots = n;
    }
    if st.p_name.is_none() {
        let name = format!("pgsql_fdw_prep_{}", connection::get_prep_stmt_number());
        let res = connection::with_entry(st.conn_key, |e| -> PgResult<_> {
            connection::park_pending_entry(e)?;
            Ok(e.conn.as_mut().expect("live connection").prepare(&name, &st.query, &[]))
        })???;
        if res.status != ExecStatus::CommandOk {
            return Err(connection::remote_error(&res, Some(&st.query)));
        }
        st.p_name = Some(name);
    }
    let params: Vec<Option<&str>> = st.pending_params.iter().map(|v| v.as_deref()).collect();
    let p_name = st.p_name.as_deref().expect("prepared above").to_string();
    let res = connection::with_entry(st.conn_key, |e| -> PgResult<_> {
        connection::park_pending_entry(e)?;
        Ok(e.conn.as_mut().expect("live connection").exec_prepared(&p_name, &params))
    })???;
    if res.status != ExecStatus::CommandOk {
        return Err(connection::remote_error(&res, Some(&st.query)));
    }
    // ExecBatchInsert (nodeModifyTable.c:1411-1412): the rows the remote
    // reports as inserted (ON CONFLICT DO NOTHING skips are not counted).
    let n_rows = cmd_tuples(&res.cmd_tag);
    if st.can_set_tag && n_rows > 0 {
        estate.es_processed += n_rows as u64;
    }
    st.pending_params.clear();
    st.pending_rows = 0;
    Ok(())
}

// The FdwModifyRoutine flush hook (C ExecPendingInserts' per-rel half).
pub(crate) fn flush_foreign_modify<'mcx>(
    state: &mut dyn core::any::Any,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let st = state
        .downcast_mut::<PgFdwModifyState>()
        .expect("ri_FdwState is PgFdwModifyState");
    flush_pending_inserts(st, estate)
}

// store_returning_result: convert the remote RETURNING row into the slot.
fn store_returning_result<'mcx>(
    st: &mut PgFdwModifyState,
    estate: &mut EStateData<'mcx>,
    slot_id: ExecSlotId,
    row: &[Option<Vec<u8>>],
) -> PgResult<()> {
    let attin = st.attin.as_mut().expect("has_returning implies attinmeta");
    let (values, nulls, ctid) =
        convert_result_row(attin, &st.retrieved_attrs, row, st.returning_mcx.mcx())?;
    let mcx = estate.es_query_cxt;
    let slot = &mut estate.es_tupleTable[slot_id.0 as usize];
    exectuples::exec_clear_tuple(slot, mcx);
    {
        let base = slot.base_mut();
        base.tts_values.clear();
        base.tts_values.extend_from_slice(&values);
        base.tts_isnull.clear();
        base.tts_isnull.extend_from_slice(&nulls);
        base.tts_tid = ctid;
    }
    exectuples::exec_store_virtual_tuple(slot);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::cmd_tuples;

    // PQcmdTuples shapes: INSERT carries an OID slot, UPDATE/DELETE don't.
    #[test]
    fn cmd_tuples_parses_command_tags() {
        assert_eq!(cmd_tuples("INSERT 0 1"), 1);
        assert_eq!(cmd_tuples("INSERT 0 0"), 0);
        assert_eq!(cmd_tuples("UPDATE 1"), 1);
        assert_eq!(cmd_tuples("DELETE 0"), 0);
        assert_eq!(cmd_tuples("UPDATE 42"), 42);
        assert_eq!(cmd_tuples("SELECT"), 0);
    }
}

// postgresEndForeignModify + finish_foreign_modify + deallocate_query.
pub(crate) fn end_foreign_modify(state: Box<dyn core::any::Any>) -> PgResult<()> {
    let st = state
        .downcast::<PgFdwModifyState>()
        .expect("ri_FdwState is PgFdwModifyState");
    if let Some(name) = &st.p_name {
        let sql = format!("DEALLOCATE {name}");
        let res = connection::exec_query(st.conn_key, &sql)?;
        if res.status != ExecStatus::CommandOk {
            return Err(connection::remote_error(&res, Some(&sql)));
        }
    }
    connection::release_connection(st.conn_key);
    Ok(())
}

// postgresExplainForeignModify (postgres_fdw.c:2955-2975): "Remote SQL" under
// VERBOSE, plus "Batch Size" for INSERT. C prints ri_BatchSize, which
// postgresGetForeignModifyBatchSize (postgres_fdw.c:2034-2094) computed from
// the plan (RETURNING / WCO / row triggers force 1) plus, only when the plan
// was executed (fmstate exists: EXPLAIN ANALYZE), the zero-column gate and the
// 65535 / p_nums parameter clamp. Recomputed here from the same inputs.
pub(crate) fn explain_foreign_modify<'mcx>(
    fdw_private: &NodeList<'mcx>,
    relid: Oid,
    has_wco: bool,
    flags: types_nodes::FdwExplainFlags<'_>,
    emit: &mut dyn FnMut(&str, types_nodes::FdwExplainProp<'_>) -> PgResult<()>,
) -> PgResult<()> {
    if !flags.verbose {
        return Ok(());
    }
    let mut it = fdw_private.iter();
    if let Some(sql) = it.next().and_then(|n| n.as_string()) {
        emit("Remote SQL", types_nodes::FdwExplainProp::Text(sql.sval))?;
    }
    let target_attrs: Vec<i32> = it
        .next()
        .and_then(|n| n.as_int_list())
        .map(|l| l.iter().collect())
        .unwrap_or_default();
    let values_end = it.next().and_then(|n| n.as_integer()).map(|i| i.ival).unwrap_or(-1);
    let _remote_returning = it.next();
    let _retrieved_attrs = it.next();
    let local_returning =
        it.next().and_then(|n| n.as_boolean()).map(|b| b.boolval).unwrap_or(false);
    if values_end >= 0 {
        // INSERT: report the effective batch size.
        let has_insert_row_triggers = relcache_seams::relation_get_trigger_desc::call(relid)?
            .is_some_and(|t| t.trig_insert_before_row || t.trig_insert_after_row);
        let batch = if local_returning || has_wco || has_insert_row_triggers {
            1
        } else if flags.analyze && target_attrs.is_empty() {
            // fmstate && list_length(fmstate->target_attrs) == 0
            1
        } else {
            let scratch = mcx::MemoryContext::new("postgres_fdw explain batch size");
            let mut batch = get_batch_size_option(scratch.mcx(), relid)?.max(1);
            if flags.analyze {
                // fmstate->p_nums: the non-generated target columns
                // (create_foreign_modify, postgres_fdw.c:4051-4066).
                let mut p_nums = 0i32;
                for &attnum in &target_attrs {
                    if lsyscache::attribute::get_attgenerated(relid, attnum as i16)? == 0 {
                        p_nums += 1;
                    }
                }
                if p_nums > 0 {
                    batch = batch.min(65535 / p_nums);
                }
            }
            batch
        };
        emit(
            "Batch Size",
            types_nodes::FdwExplainProp::Integer { value: batch as i64, unit: "" },
        )?;
    }
    Ok(())
}

// ---------- direct modify (postgresPlanDirectModify + executor half) ----------

// find_modifytable_subplan: the target ForeignScan is the ModifyTable's
// immediate child, or the subplan_index'th child of an Append (possibly
// under a Result computing the UPDATE tlist).
fn find_modifytable_subplan<'mcx>(
    plan: &ModifyTable<'mcx>,
    rtindex: u32,
    subplan_index: usize,
) -> Option<Node<'mcx>> {
    use types_nodes::NodeTag;
    let mut subplan = plan.plan.lefttree?;
    if subplan.node_tag() == NodeTag::T_Append {
        let app = subplan.as_append()?;
        if subplan_index < app.appendplans.len() {
            subplan = app.appendplans.nth(subplan_index);
        }
    } else if subplan.node_tag() == NodeTag::T_Result {
        if let Some(l) = subplan.as_plan()?.lefttree {
            if l.node_tag() == NodeTag::T_Append {
                let app = l.as_append()?;
                if subplan_index < app.appendplans.len() {
                    subplan = app.appendplans.nth(subplan_index);
                }
            }
        }
    }
    let fs = subplan.as_foreign_scan()?;
    if fs.fs_base_relids.is_member(rtindex as i32) {
        Some(subplan)
    } else {
        None
    }
}

// postgresPlanDirectModify, base-relation arm. Divergences (both fall back
// to the safe per-row DML path): inherited-child targets
// (get_translated_update_targetlist) and foreign-join targets (no EPQ-capable
// pushed join paths are generated under UPDATE/DELETE).
pub(crate) fn plan_direct_modify<'mcx>(
    run: &mut PlannerRun<'mcx>,
    plan: &mut ModifyTable<'mcx>,
    result_relation: u32,
    subplan_index: usize,
) -> PgResult<bool> {
    let mcx = run.mcx;
    let operation = plan.operation;
    if operation != CmdType::CMD_UPDATE && operation != CmdType::CMD_DELETE {
        return Ok(false);
    }
    let Some(subplan) = find_modifytable_subplan(plan, result_relation, subplan_index) else {
        return Ok(false);
    };
    {
        let fs = subplan.as_foreign_scan().expect("checked");
        if !fs.scan.plan.qual.is_nil() {
            return Ok(false);
        }
        if fs.scan.scanrelid == 0 {
            return Ok(false);
        }
    }
    if result_relation as i32 != run.parse().resultRelation {
        return Ok(false);
    }
    let foreignrel = run.root.simple_rel_array[result_relation as usize]
        .expect("result relation has a RelOptInfo");

    let mut tlist_nodes: Vec<Node<'mcx>> = Vec::new();
    let mut target_attrs: Vec<i32> = Vec::new();
    if operation == CmdType::CMD_UPDATE {
        let colnos: Vec<i32> =
            run.root.update_colnos.iter().map(|&a| a as i32).collect();
        for (tle_node, &attno) in run.processed_tlist().iter().zip(colnos.iter()) {
            let tle = tle_node.as_target_entry().expect("TargetEntry");
            debug_assert!(!tle.resjunk);
            if attno <= 0 {
                return Err(Box::new(PgError::error(
                    "system-column update is not supported",
                )));
            }
            if !deparse::is_foreign_expr(run, foreignrel, tle.expr)? {
                return Ok(false);
            }
            tlist_nodes.push(tle_node);
            target_attrs.push(attno);
        }
    }

    let remote_exprs: Vec<types_pathnodes::NodeId> =
        crate::relinfo::fpinfo(run.root.rel(foreignrel))
            .borrow()
            .final_remote_exprs
            .iter()
            .copied()
            .collect();

    let returning_list: Vec<Node<'mcx>> = if plan.returningLists.is_nil() {
        Vec::new()
    } else {
        plan.returningLists
            .nth(subplan_index)
            .as_list()
            .expect("returningLists cell is a List")
            .iter()
            .collect()
    };

    let rte = run.rte(result_relation as usize);
    let rel = table::table_open(mcx, rte.relid, types_rel::lock::NoLock)?;
    let mut retrieved_attrs: PgVec<'mcx, i32> = PgVec::new_in(mcx);
    let mut ctx = deparse::DeparseCtx {
        run,
        foreignrel,
        scanrel: foreignrel,
        buf: PgString::new_in(mcx),
        params_list: Some(PgVec::new_in(mcx)),
        mcx,
    };
    match operation {
        CmdType::CMD_UPDATE => deparse::deparse_direct_update_sql(
            &mut ctx,
            result_relation as i32,
            &rel,
            rte,
            &tlist_nodes,
            &target_attrs,
            &remote_exprs,
            &returning_list,
            &mut retrieved_attrs,
        )?,
        CmdType::CMD_DELETE => deparse::deparse_direct_delete_sql(
            &mut ctx,
            result_relation as i32,
            &rel,
            rte,
            &remote_exprs,
            &returning_list,
            &mut retrieved_attrs,
        )?,
        _ => unreachable!(),
    }
    let sql = ctx.buf;
    let params = ctx.params_list.take().expect("params_list set above");
    table::table_close(rel, types_rel::lock::NoLock)?;

    let mut fdw_exprs: NodeList<'mcx> = NodeList::nil();
    for p in params.iter() {
        fdw_exprs.lappend(mcx, *p)?;
    }
    // fdw_private (FdwDirectModifyPrivateIndex order):
    // [UpdateSql, HasReturning, RetrievedAttrs, SetProcessed], plus a fifth
    // pgrust-only entry: whether the LOCAL query has a RETURNING list. C
    // reads that at execution as resultRelInfo->ri_projectReturning
    // (postgres_fdw.c:2789); the ForeignScanState here cannot reach the
    // ModifyTable's result relation, so the plan carries it.
    let mut fdw_private: NodeList<'mcx> = NodeList::nil();
    fdw_private.lappend(mcx, Node::mk_string(mcx, mcx_str(mcx, sql.as_str())?)?)?;
    fdw_private.lappend(mcx, Node::mk_boolean(mcx, !retrieved_attrs.is_empty())?)?;
    let mut ra: IntList<'mcx> = IntList::nil();
    for &a in retrieved_attrs.iter() {
        ra.lappend(mcx, a)?;
    }
    fdw_private.lappend(mcx, Node::mk_int_list(mcx, ra)?)?;
    fdw_private.lappend(mcx, Node::mk_boolean(mcx, plan.canSetTag)?)?;
    fdw_private.lappend(mcx, Node::mk_boolean(mcx, !returning_list.is_empty())?)?;

    // SAFETY: exclusive plan-tree ownership at create_plan time (the same
    // contract createplan/setrefs rely on for in-place plan rewrites).
    unsafe {
        subplan.with_mut::<types_nodes::plannodes::ForeignScan, _>(|f| {
            f.operation = operation;
            f.resultRelation = result_relation;
            f.fdw_exprs = fdw_exprs;
            f.fdw_private = fdw_private;
            f.scan.plan.async_capable = false;
        })
    }
    .expect("ForeignScan node");
    Ok(true)
}

// C PgFdwDirectModifyState (executor half).
struct PgFdwDirectModifyState {
    conn_key: Oid,
    query: &'static str,
    /// The remote statement has a RETURNING list (retrieved_attrs non-empty).
    has_returning: bool,
    retrieved_attrs: Vec<i32>,
    set_processed: bool,
    /// The local query has a RETURNING list (C ri_projectReturning): when
    /// `has_returning` is false, each affected row yields an all-NULL dummy
    /// tuple (postgres_fdw.c:4654-4658, "UPDATE ... RETURNING 1").
    local_returning: bool,
    param_flinfo: Vec<FmgrInfo>,
    param_exprs: Vec<mcx::PgBox<'static, execexpr::ExprState<'static>>>,
    /// -1 = statement not executed yet.
    num_tuples: i64,
    next_tuple: usize,
    rows: Vec<(Vec<Datum>, Vec<bool>, ItemPointerData)>,
    batch_mcx: mcx::MemoryContext,
    attin: Option<AttInMeta>,
}

fn dmstate<'a>(
    node: &'a mut nodeforeignscan::ForeignScanState<'_>,
) -> Option<&'a mut PgFdwDirectModifyState> {
    node.fdw_state.as_mut().and_then(|s| s.downcast_mut::<PgFdwDirectModifyState>())
}

// postgresBeginDirectModify.
pub(crate) fn begin_direct_modify<'mcx>(
    node: &mut nodeforeignscan::ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<()> {
    if eflags & types_slot::EXEC_FLAG_EXPLAIN_ONLY != 0 {
        return Ok(());
    }
    let mcx = estate.es_query_cxt;
    let fsplan = node.plan;
    debug_assert!(fsplan.scan.scanrelid > 0, "join direct modify is not planned");

    let userid = if fsplan.checkAsUser != InvalidOid {
        fsplan.checkAsUser
    } else {
        miscinit::GetUserId()
    };
    let rel = node.ss.ss_currentRelation.as_ref().expect("direct modify scans the target rel");
    let table = foreigncmds::foreign::GetForeignTable(mcx, rel.rd_id)?;
    let user = foreigncmds::foreign::GetUserMapping(mcx, userid, table.serverid)?;
    let conn_key = connection::get_connection(mcx, &user, false)?;

    let mut it = fsplan.fdw_private.iter();
    let query = it
        .next()
        .and_then(|n| n.as_string())
        .expect("fdw_private[0] is the remote DML")
        .sval;
    let has_returning = it
        .next()
        .and_then(|n| n.as_boolean())
        .expect("fdw_private[1] is has_returning")
        .boolval;
    let retrieved_attrs: Vec<i32> = it
        .next()
        .and_then(|n| n.as_int_list())
        .expect("fdw_private[2] is retrieved_attrs")
        .iter()
        .collect();
    let set_processed = it
        .next()
        .and_then(|n| n.as_boolean())
        .expect("fdw_private[3] is set_processed")
        .boolval;
    let local_returning = it
        .next()
        .and_then(|n| n.as_boolean())
        .expect("fdw_private[4] is the local RETURNING flag")
        .boolval;

    let attin =
        if has_returning { Some(AttInMeta::build(rel.name(), &rel.rd_att)?) } else { None };

    let (param_flinfo, param_exprs) =
        crate::exec::prepare_query_params(estate, &fsplan.fdw_exprs)?;
    // SAFETY: plan-lived string, restamped (PgFdwScanState precedent).
    let query = unsafe { core::mem::transmute::<&'mcx str, &'static str>(query) };

    node.fdw_state = Some(Box::new(PgFdwDirectModifyState {
        conn_key,
        query,
        has_returning,
        retrieved_attrs,
        set_processed,
        local_returning,
        param_flinfo,
        param_exprs,
        num_tuples: -1,
        next_tuple: 0,
        rows: Vec::new(),
        batch_mcx: mcx::MemoryContext::new_bump("postgres_fdw direct modify data"),
        attin,
    }));
    Ok(())
}

// execute_dml_stmt: run the direct UPDATE/DELETE remotely (text params).
fn execute_dml_stmt<'mcx>(
    node: &mut nodeforeignscan::ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let ecxt = node.ss.ps_ExprContext;
    let state = dmstate(node).expect("fdw_state set by BeginDirectModify");
    let values = if state.param_exprs.is_empty() {
        Vec::new()
    } else {
        crate::exec::process_query_params(
            &mut state.param_exprs,
            &mut state.param_flinfo,
            estate,
            ecxt,
        )?
    };
    let params: Vec<Option<&str>> = values.iter().map(|v| v.as_deref()).collect();
    let res =
        connection::exec_query_params(state.conn_key, state.query, &params, state.query)?;
    let expected =
        if state.has_returning { ExecStatus::TuplesOk } else { ExecStatus::CommandOk };
    if res.status != expected {
        return Err(connection::remote_error(&res, Some(state.query)));
    }
    if state.has_returning {
        state.num_tuples = res.rows.len() as i64;
        state.rows.clear();
        state.batch_mcx.reset();
        state.rows.reserve(res.rows.len());
        for row in &res.rows {
            let PgFdwDirectModifyState { attin, retrieved_attrs, batch_mcx, rows, .. } = state;
            rows.push(convert_result_row(
                attin.as_mut().expect("has_returning built attin"),
                retrieved_attrs,
                row,
                batch_mcx.mcx(),
            )?);
        }
    } else {
        state.num_tuples = cmd_tuples(&res.cmd_tag);
    }
    Ok(())
}

// postgresIterateDirectModify (+ get_returning_data, base-rel arm).
pub(crate) fn iterate_direct_modify<'mcx>(
    node: &mut nodeforeignscan::ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if dmstate(node).expect("fdw_state set by BeginDirectModify").num_tuples == -1 {
        execute_dml_stmt(node, estate)?;
    }
    let scan_slot = node.ss.ss_ScanTupleSlot;
    let instr_idx = node.ss.instr_idx;
    let qmcx = estate.es_query_cxt;
    let state = dmstate(node).expect("fdw_state set by BeginDirectModify");
    // The local query has no RETURNING: just clear the slot
    // (postgres_fdw.c:2789-2803).
    if !state.local_returning {
        debug_assert!(!state.has_returning);
        if state.set_processed {
            estate.es_processed += state.num_tuples as u64;
            state.set_processed = false;
        }
        // EXPLAIN ANALYZE's tuple count: ExecScan sees no tuple, so the
        // affected rows are credited here (postgres_fdw.c:2801).
        if let Some(idx) = instr_idx {
            estate.es_instrumentation[idx as usize].tuplecount += state.num_tuples as f64;
        }
        exectuples::exec_clear_tuple(estate.slot_mut(scan_slot), qmcx);
        return Ok(false);
    }
    // get_returning_data (postgres_fdw.c:4630).
    if state.next_tuple as i64 >= state.num_tuples {
        exectuples::exec_clear_tuple(estate.slot_mut(scan_slot), qmcx);
        return Ok(false);
    }
    if state.set_processed {
        estate.es_processed += 1;
    }
    if !state.has_returning {
        // "UPDATE/DELETE .. RETURNING 1": no remote column, one all-NULL
        // dummy tuple per affected row (postgres_fdw.c:4654-4658).
        state.next_tuple += 1;
        let slot = estate.slot_mut(scan_slot);
        exectuples::exec_clear_tuple(slot, qmcx);
        {
            let base = slot.base_mut();
            let natts = base
                .tts_tupleDescriptor
                .as_ref()
                .expect("scan slot has a descriptor")
                .natts as usize;
            base.tts_values.clear();
            base.tts_values.resize(natts, Datum::null());
            base.tts_isnull.clear();
            base.tts_isnull.resize(natts, true);
        }
        exectuples::exec_store_virtual_tuple(slot);
        estate.es_direct_returning_slot = Some(scan_slot);
        return Ok(true);
    }
    let (values, nulls, ctid) = &state.rows[state.next_tuple];
    let ctid = *ctid;
    let values = values.clone();
    let nulls = nulls.clone();
    state.next_tuple += 1;
    let slot = estate.slot_mut(scan_slot);
    exectuples::exec_clear_tuple(slot, qmcx);
    {
        let base = slot.base_mut();
        base.tts_values.clear();
        base.tts_values.extend_from_slice(&values);
        base.tts_isnull.clear();
        base.tts_isnull.extend_from_slice(&nulls);
        base.tts_tid = ctid;
    }
    exectuples::exec_store_virtual_tuple(slot);
    // Hand the rel-format RETURNING tuple to ModifyTable's direct arm (C
    // stores it into ri_projectReturning's econtext scantuple).
    estate.es_direct_returning_slot = Some(scan_slot);
    Ok(true)
}

// postgresEndDirectModify.
pub(crate) fn end_direct_modify<'mcx>(
    node: &mut nodeforeignscan::ForeignScanState<'mcx>,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let Some(state) = dmstate(node) else {
        return Ok(()); // EXPLAIN
    };
    connection::release_connection(state.conn_key);
    node.fdw_state = None;
    Ok(())
}
