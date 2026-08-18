// postgres_fdw.c modify half: postgresAddForeignUpdateTargets,
// postgresPlanForeignModify, postgresBeginForeignModify (create_foreign_modify),
// postgresExecForeignInsert/Update/Delete (execute_foreign_modify over a
// remote prepared statement), postgresEndForeignModify (finish_foreign_modify),
// postgresIsForeignRelUpdatable. Batch insert (ExecForeignBatchInsert) and
// direct modify are unmodeled: every row runs C's single-row leg (C's default
// batch_size is 1; a larger batch_size option changes only round-trips).
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
//  HasReturning, RetrievedAttrs].
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
        let mut all_updated = types_nodes::Bitmapset::empty();
        if rte.perminfoindex > 0 {
            let pi = run
                .parse()
                .rteperminfos
                .nth(rte.perminfoindex as usize - 1)
                .as_rte_permission_info()
                .expect("rteperminfos cell");
            all_updated.add_members(mcx, &pi.updatedCols)?;
        }
        let extra = planner::plancat::get_dependent_generated_columns(
            run,
            result_relation as usize,
            &all_updated,
        )?;
        all_updated.add_members(mcx, &extra)?;
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
        panic!(
            "unexpected ON CONFLICT specification: {}",
            plan.onConflictAction
        );
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
        other => panic!("unexpected operation: {other:?}"),
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

fn parse_bool(value: &str) -> bool {
    // defGetBoolean's accepted spellings; validator-checked upstream.
    matches!(
        value.to_ascii_lowercase().as_str(),
        "true" | "t" | "on" | "1" | "yes" | "y"
    )
}

// ---------- executor half ----------

// C PgFdwModifyState (single-row leg; no batching, no aux_fmstate — routed
// foreign inserts and COPY into foreign tables are refused upstream).
struct PgFdwModifyState {
    conn_key: Oid,
    p_name: Option<String>,
    query: String,
    target_attrs: Vec<i32>,
    // per-target_attrs entry: attgenerated columns transmit as DEFAULT.
    attgenerated: Vec<bool>,
    has_returning: bool,
    retrieved_attrs: Vec<i32>,
    attin: Option<AttInMeta>,
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
    let _values_end = it
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

    // ExecGetResultRelCheckAsUser: the perminfo's checkAsUser, else current.
    let rte = estate.exec_rt_fetch(rti);
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
        p_name: None,
        query,
        target_attrs,
        attgenerated,
        has_returning,
        retrieved_attrs,
        attin: has_returning.then_some(attin),
        ctid_attno,
        p_flinfo,
        temp_mcx: mcx::MemoryContext::new_bump("postgres_fdw temporary data"),
        returning_mcx: mcx::MemoryContext::new_bump("postgres_fdw returning data"),
    })))
}

// OutputFunctionCall through the per-call scratch, copied out as a String.
fn output_to_string(
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
