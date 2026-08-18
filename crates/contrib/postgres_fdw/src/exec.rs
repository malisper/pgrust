// postgres_fdw.c executor half (scan path): BeginForeignScan /
// IterateForeignScan / ReScanForeignScan / EndForeignScan over the pgclient
// connection cache. Remote rows arrive as text batches from a cursor
// ("DECLARE c%u CURSOR FOR\n%s" + "FETCH %d FROM c%u", C's exact strings) and
// convert through the local input functions into a per-batch bump context
// (C's batch_cxt shape; the per-row temp_cxt garbage separation is folded
// into the batch reset — bounded by one batch, recorded divergence).
use std::ffi::CString;

use datum::Datum;
use mcx::PgBox;
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use types_fmgr::FmgrInfo;
use types_tuple::TupleDescData;

use execexpr::{exec_eval_expr, exec_init_expr, EvalSlots, ExprState};
use executils::{EStateData, EcxtId};
use nodeforeignscan::ForeignScanState;
use exectuples;
use pgclient::ExecStatus;

use crate::connection;

// C PgFdwScanState. SAFETY (lifetime restamp): every 'static here is
// es_query_cxt-lived (query points into the plan's fdw_private, the
// ExprStates are compiled in es_query_cxt); the state is dropped at end-scan,
// before that context resets (file_fdw / executils 'static-restamp
// precedent).
struct PgFdwScanState {
    conn_key: Oid,
    cursor_number: u32,
    cursor_exists: bool,
    async_capable: bool,
    query: &'static str,
    fetch_size: i32,
    retrieved_attrs: Vec<i32>,
    attin: AttInMeta,
    // parallel arrays over fdw_exprs (C param_flinfo / param_exprs).
    param_flinfo: Vec<FmgrInfo>,
    param_exprs: Vec<PgBox<'static, ExprState<'static>>>,
    // current batch (C batch_cxt): datum payloads live in batch_mcx.
    batch_mcx: mcx::MemoryContext,
    tuples: Vec<(Vec<Datum>, Vec<bool>, types_tuple::ItemPointerData)>,
    next_tuple: usize,
    fetch_ct_2: i32,
    eof_reached: bool,
}

// C AttInMetadata over the foreign table's descriptor (dblink's port shape),
// plus typlen/typbyval so retained datums can be copied out of the fmgr
// per-call scratch (see retain_datum), plus tidin for the retrieved-ctid arm.
pub(crate) struct AttInMeta {
    natts: usize,
    relname: String,
    attnames: Vec<String>,
    in_funcs: Vec<FmgrInfo>,
    typioparams: Vec<Oid>,
    typmods: Vec<i32>,
    typlens: Vec<i16>,
    typbyvals: Vec<bool>,
    tid_in: FmgrInfo,
    tid_ioparam: Oid,
}

impl AttInMeta {
    pub(crate) fn build(relname: &str, tupdesc: &TupleDescData<'_>) -> PgResult<AttInMeta> {
        let natts = tupdesc.natts as usize;
        let mut attnames = Vec::with_capacity(natts);
        let mut in_funcs = Vec::with_capacity(natts);
        let mut typioparams = Vec::with_capacity(natts);
        let mut typmods = Vec::with_capacity(natts);
        let mut typlens = Vec::with_capacity(natts);
        let mut typbyvals = Vec::with_capacity(natts);
        for i in 0..natts {
            let att = tupdesc.attr(i);
            attnames.push(String::from_utf8_lossy(att.attname.name_str()).into_owned());
            if att.attisdropped {
                // Dropped columns never appear in retrieved_attrs (the
                // deparser skips them); keep unresolved placeholders.
                in_funcs.push(FmgrInfo::unresolved());
                typioparams.push(InvalidOid);
                typmods.push(-1);
                typlens.push(0);
                typbyvals.push(true);
                continue;
            }
            let (infunc, typioparam) = lsyscache::getTypeInputInfo(att.atttypid)?;
            in_funcs.push(fmgr_seams::fmgr_info::call(infunc)?);
            typioparams.push(typioparam);
            typmods.push(att.atttypmod);
            let (typlen, typbyval) = lsyscache::get_typlenbyval(att.atttypid)?;
            typlens.push(typlen);
            typbyvals.push(typbyval);
        }
        let (tid_infunc, tid_ioparam) =
            lsyscache::getTypeInputInfo(types_core::catalog::TIDOID)?;
        Ok(AttInMeta {
            natts,
            relname: relname.to_string(),
            attnames,
            in_funcs,
            typioparams,
            typmods,
            typlens,
            typbyvals,
            tid_in: fmgr_seams::fmgr_info::call(tid_infunc)?,
            tid_ioparam,
        })
    }
}

fn fsstate<'a>(node: &'a mut ForeignScanState<'_>) -> Option<&'a mut PgFdwScanState> {
    node.fdw_state.as_mut().and_then(|s| s.downcast_mut::<PgFdwScanState>())
}

#[track_caller]
#[cold]
fn system_columns_unported() -> Box<PgError> {
    Box::new(
        PgError::error(
            "postgres_fdw: retrieving system columns from a remote table is not yet \
             supported (phase 3: heap-tuple scan slots)",
        )
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

// postgresBeginForeignScan.
pub(crate) fn begin_foreign_scan<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<()> {
    // EXPLAIN (no ANALYZE): no connection; fdw_state stays None.
    if eflags & types_slot::EXEC_FLAG_EXPLAIN_ONLY != 0 {
        return Ok(());
    }
    let mcx = estate.es_query_cxt;
    let fsplan = node.plan;

    // Identify which user to do the remote access as (checkAsUser for
    // view-owner access, else the current user).
    let userid = if fsplan.checkAsUser != InvalidOid {
        fsplan.checkAsUser
    } else {
        miscinit::GetUserId()
    };

    let rel = node.ss.ss_currentRelation.as_ref().expect("base foreign scan has a relation");
    let table = foreigncmds::foreign::GetForeignTable(mcx, rel.rd_id)?;
    let user = foreigncmds::foreign::GetUserMapping(mcx, userid, table.serverid)?;

    // Get the (cached) connection, with the remote transaction open.
    let conn_key = connection::get_connection(mcx, &user, false)?;

    // Assign a unique ID for the cursor (created on first Iterate).
    let cursor_number = connection::get_cursor_number();

    // fdw_private: [SELECT sql, retrieved_attrs, fetch_size].
    let mut it = fsplan.fdw_private.iter();
    let query = it
        .next()
        .and_then(|n| n.as_string())
        .expect("fdw_private[0] is the remote SELECT")
        .sval;
    let retrieved_attrs: Vec<i32> = it
        .next()
        .and_then(|n| n.as_int_list())
        .expect("fdw_private[1] is retrieved_attrs")
        .iter()
        .collect();
    let fetch_size = it
        .next()
        .and_then(|n| n.as_integer())
        .expect("fdw_private[2] is fetch_size")
        .ival;

    // ctid (SelfItemPointerAttributeNumber) is retrieved into the slot's
    // tts_tid (the UPDATE/DELETE row identity); other system columns need
    // heap-tuple scan slots and stay unported.
    if retrieved_attrs
        .iter()
        .any(|&a| a <= 0 && a != types_tuple::htup::SelfItemPointerAttributeNumber)
    {
        return Err(system_columns_unported());
    }

    let attin = AttInMeta::build(rel.name(), &rel.rd_att)?;

    // prepare_query_params: output functions + compiled expressions for
    // fdw_exprs (Params after replace_nestloop_params; no SubPlans — the
    // shippability walker rejects them).
    let pb = estate.param_bind();
    let mut param_flinfo = Vec::with_capacity(fsplan.fdw_exprs.len());
    let mut param_exprs = Vec::with_capacity(fsplan.fdw_exprs.len());
    for expr in fsplan.fdw_exprs.iter() {
        let (typoutput, _isvarlena) =
            lsyscache::getTypeOutputInfo(nodes_core::node_funcs::expr_type(expr))?;
        param_flinfo.push(fmgr_seams::fmgr_info::call(typoutput)?);
        let state =
            exec_init_expr(mcx, Some(expr), pb)?.expect("fdw_exprs entries are expressions");
        // SAFETY: es_query_cxt restamp; dropped at end-scan (struct comment).
        param_exprs.push(unsafe {
            core::mem::transmute::<PgBox<'mcx, ExprState<'mcx>>, PgBox<'static, ExprState<'static>>>(
                state,
            )
        });
    }

    // SAFETY: plan-lived string, restamped (struct comment).
    let query = unsafe { core::mem::transmute::<&'mcx str, &'static str>(query) };

    node.fdw_state = Some(Box::new(PgFdwScanState {
        conn_key,
        cursor_number,
        cursor_exists: false,
        // C copies ps.async_capable, which ExecInitNode clears under EPQ.
        async_capable: fsplan.scan.plan.async_capable && !estate.es_epq_active,
        query,
        fetch_size,
        retrieved_attrs,
        attin,
        param_flinfo,
        param_exprs,
        batch_mcx: mcx::MemoryContext::new_bump("postgres_fdw tuple data"),
        tuples: Vec::new(),
        next_tuple: 0,
        fetch_ct_2: 0,
        eof_reached: false,
    }));
    Ok(())
}

// process_query_params: evaluate fdw_exprs and convert to text via the
// output functions, under the transmission modes (datestyle=ISO etc.), as C.
fn process_query_params<'mcx>(
    state: &mut PgFdwScanState,
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
) -> PgResult<Vec<Option<String>>> {
    let nestlevel = crate::transmission::set_transmission_modes();
    let r = (|| -> PgResult<Vec<Option<String>>> {
        let mut values: Vec<Option<String>> = Vec::with_capacity(state.param_exprs.len());
        let scratch = mcx::MemoryContext::new_bump("postgres_fdw param output");
        for (expr, flinfo) in state.param_exprs.iter_mut().zip(state.param_flinfo.iter_mut()) {
            let per_tuple = estate.ecxt(ecxt).per_tuple_mcx();
            // SAFETY: reset-only per-tuple context, outlives the evaluation.
            unsafe { expr.arm_result_mcx_raw(per_tuple) };
            let mut slots = EvalSlots { scan: None, inner: None, outer: None };
            let nd = exec_eval_expr(expr, &mut slots)?;
            if nd.isnull {
                values.push(None);
            } else {
                let d = types_fmgr::function_call1_coll_in(
                    flinfo,
                    InvalidOid,
                    scratch.mcx(),
                    nd.value,
                )?;
                // SAFETY: output functions return a NUL-terminated cstring
                // datum; copied out before the scratch context resets.
                let s = unsafe {
                    core::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char)
                };
                values.push(Some(s.to_string_lossy().into_owned()));
            }
        }
        Ok(values)
    })();
    crate::transmission::reset_transmission_modes(nestlevel);
    estate.ecxt_mut(ecxt).reset();
    r
}

// create_cursor: "DECLARE c%u CURSOR FOR\n%s" via the extended protocol
// (PQsendQueryParams), text params.
fn create_cursor<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let ecxt = node.ss.ps_ExprContext;
    let state = fsstate(node).expect("fdw_state set by BeginForeignScan");
    let values = if state.param_exprs.is_empty() {
        Vec::new()
    } else {
        process_query_params(state, estate, ecxt)?
    };
    let params: Vec<Option<&str>> = values.iter().map(|v| v.as_deref()).collect();
    let sql = format!("DECLARE c{} CURSOR FOR\n{}", state.cursor_number, state.query);
    let res = connection::exec_query_params(state.conn_key, &sql, &params)?;
    if res.status != ExecStatus::CommandOk {
        return Err(connection::remote_error(&res, Some(state.query)));
    }
    state.cursor_exists = true;
    state.tuples.clear();
    state.batch_mcx.reset();
    state.next_tuple = 0;
    state.fetch_ct_2 = 0;
    state.eof_reached = false;
    Ok(())
}

// fetch_more_data's conversion half: install one FETCH result as the batch.
fn absorb_batch(state: &mut PgFdwScanState, res: &pgclient::QueryResult) -> PgResult<()> {
    state.tuples.clear();
    state.batch_mcx.reset();
    if res.status != ExecStatus::TuplesOk {
        // On error, report the original query, not the FETCH.
        return Err(connection::remote_error(res, Some(state.query)));
    }
    let numrows = res.rows.len();
    state.tuples.reserve(numrows);
    for row in &res.rows {
        let tup = make_tuple_from_result_row(state, row)?;
        state.tuples.push(tup);
    }
    state.next_tuple = 0;
    if state.fetch_ct_2 < 2 {
        state.fetch_ct_2 += 1;
    }
    state.eof_reached = numrows < state.fetch_size as usize;
    Ok(())
}

// fetch_more_data: async arm completes the in-flight FETCH begun by
// fetch_more_data_begin; sync arm sends "FETCH %d FROM c%u" and waits.
fn fetch_more_data(state: &mut PgFdwScanState) -> PgResult<()> {
    if state.async_capable {
        debug_assert!(connection::pending_fetch(state.conn_key)?
            .is_some_and(|p| p.cursor_number == state.cursor_number));
        let res = connection::finish_pending_fetch(state.conn_key, state.query)?;
        return absorb_batch(state, &res);
    }
    let sql = format!("FETCH {} FROM c{}", state.fetch_size, state.cursor_number);
    let res = connection::exec_query(state.conn_key, &sql)?;
    absorb_batch(state, &res)
}

// Adopt a batch another scan drained and parked on our behalf (the requestor
// half of C's process_pending_request runs at our next ConfigureWait/Notify).
fn adopt_parked(state: &mut PgFdwScanState) -> PgResult<bool> {
    match connection::take_parked(state.conn_key, state.cursor_number)? {
        Some(res) => {
            absorb_batch(state, &res)?;
            Ok(true)
        }
        None => Ok(false),
    }
}

// make_tuple_from_result_row: text -> datum through the input functions
// (called for NULLs too — domains), with C's conversion errcontext. Shared by
// the scan batches and the modify RETURNING path (store_returning_result).
// The third result is the retrieved ctid (invalid when not fetched) — C
// stores it into the built tuple's t_self.
pub(crate) fn convert_result_row(
    attin: &mut AttInMeta,
    retrieved_attrs: &[i32],
    row: &[Option<Vec<u8>>],
    batch: mcx::Mcx<'_>,
) -> PgResult<(Vec<Datum>, Vec<bool>, types_tuple::ItemPointerData)> {
    let natts = attin.natts;
    let nfields = row.len();
    let mut values = vec![Datum::null(); natts];
    let mut nulls = vec![true; natts];
    let mut ctid = types_tuple::ItemPointerData::default();
    types_tuple::itemptr::ItemPointerSetInvalid(&mut ctid);

    let mut j = 0usize;
    for &i in retrieved_attrs {
        let cell = row.get(j).cloned().flatten();
        let cstr = match &cell {
            None => None,
            Some(bytes) => Some(CString::new(bytes.as_slice()).map_err(|_| {
                Box::new(PgError::error("remote value contains embedded NUL byte"))
            })?),
        };
        if i == types_tuple::htup::SelfItemPointerAttributeNumber {
            // ctid arm: tidin over the non-NULL text (C skips NULLs).
            if cell.is_some() {
                let d = types_fmgr::input_function_call(
                    &mut attin.tid_in,
                    cstr.as_deref(),
                    attin.tid_ioparam,
                    -1,
                    batch,
                )
                .map_err(|e| conversion_error(e, attin, i))?;
                // SAFETY: tidin returns a pointer datum to an ItemPointerData;
                // copied by value before the fmgr scratch is reused.
                ctid = unsafe { *(d.as_usize() as *const types_tuple::ItemPointerData) };
            }
            j += 1;
            continue;
        }
        debug_assert!(i >= 1 && i as usize <= natts, "begin gated system columns");
        let idx = (i - 1) as usize;
        let r = types_fmgr::input_function_call(
            &mut attin.in_funcs[idx],
            cstr.as_deref(),
            attin.typioparams[idx],
            attin.typmods[idx],
            batch,
        );
        match r {
            Ok(d) => {
                nulls[idx] = cell.is_none();
                if !nulls[idx] {
                    // fmgr results are PER-CALL: input functions may return a
                    // pointer into the FmgrInfo's reused out-scratch (textin
                    // does). Retaining across calls requires a copy — this is
                    // C's heap_form_tuple materialization, done per datum
                    // (CI cluster r2 find: every retained text datum aliased the
                    // batch's last row; a use-after-free on Linux).
                    values[idx] = retain_datum(
                        batch,
                        d,
                        attin.typlens[idx],
                        attin.typbyvals[idx],
                    )?;
                }
            }
            Err(e) => return Err(conversion_error(e, attin, i)),
        }
        j += 1;
    }
    // Check that the remote result matches the expected shape.
    if j > 0 && j != nfields {
        return Err(Box::new(PgError::error(
            "remote query result does not match the foreign table",
        )));
    }
    Ok((values, nulls, ctid))
}

fn make_tuple_from_result_row(
    state: &mut PgFdwScanState,
    row: &[Option<Vec<u8>>],
) -> PgResult<(Vec<Datum>, Vec<bool>, types_tuple::ItemPointerData)> {
    let PgFdwScanState { attin, retrieved_attrs, batch_mcx, .. } = state;
    convert_result_row(attin, retrieved_attrs, row, batch_mcx.mcx())
}

// Deep-copy a byref datum image into the batch context (datumCopy shape;
// evaluate_expr's byref-image precedent).
pub(crate) fn retain_datum(batch: mcx::Mcx<'_>, d: Datum, typlen: i16, typbyval: bool) -> PgResult<Datum> {
    if typbyval {
        return Ok(d);
    }
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null byref datum fresh from the input function: typlen
    // bytes readable, or a live varlena/cstring image for -1/-2; copied
    // before the fmgr scratch is touched again.
    let bytes = unsafe {
        match typlen {
            -1 => core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)),
            -2 => {
                let mut n = 0usize;
                while *p.add(n) != 0 {
                    n += 1;
                }
                core::slice::from_raw_parts(p, n + 1)
            }
            l => core::slice::from_raw_parts(p, l as usize),
        }
    };
    Ok(Datum::from_usize(mcx::slice_borrow_in(batch, bytes)?.as_ptr() as usize))
}

// conversion_error_callback: C's errcontext line for a failed conversion.
#[track_caller]
#[cold]
pub(crate) fn conversion_error(e: Box<PgError>, attin: &AttInMeta, attno: i32) -> Box<PgError> {
    let line = if attno >= 1 && attno as usize <= attin.natts {
        format!(
            "column \"{}\" of foreign table \"{}\"",
            attin.attnames[(attno - 1) as usize],
            attin.relname
        )
    } else {
        format!("processing expression at position {attno} in select list")
    };
    let mut e = e;
    e.context = Some(match e.context.take() {
        Some(prev) => format!("{prev}\n{line}"),
        None => line,
    });
    e
}

// postgresIterateForeignScan (the slot-filling half lives in the caller's
// ScanNode::scan_next; we fill ss_ScanTupleSlot and return found).
pub(crate) fn iterate_foreign_scan<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if !fsstate(node).expect("fdw_state set by BeginForeignScan").cursor_exists {
        create_cursor(node, estate)?;
    }
    let scan_slot = node.ss.ss_ScanTupleSlot;
    let qmcx = estate.es_query_cxt;
    let state = fsstate(node).expect("fdw_state set by BeginForeignScan");

    if state.next_tuple >= state.tuples.len() {
        // In async mode, just clear the tuple slot: the next batch arrives
        // through the ForeignAsyncNotify path, never a blocking fetch here.
        if state.async_capable {
            exectuples::exec_clear_tuple(estate.slot_mut(scan_slot), qmcx);
            return Ok(false);
        }
        if !state.eof_reached {
            fetch_more_data(state)?;
        }
        if state.next_tuple >= state.tuples.len() {
            exectuples::exec_clear_tuple(estate.slot_mut(scan_slot), qmcx);
            return Ok(false);
        }
    }

    let (values, nulls, ctid) = &state.tuples[state.next_tuple];
    let ctid = *ctid;
    state.next_tuple += 1;
    let slot = estate.slot_mut(scan_slot);
    exectuples::exec_clear_tuple(slot, qmcx);
    {
        let base = slot.base_mut();
        base.tts_values.clear();
        base.tts_values.extend_from_slice(values);
        base.tts_isnull.clear();
        base.tts_isnull.extend_from_slice(nulls);
        // C's t_self: the retrieved ctid feeds the junk Var(-1) fetch
        // (slot_getsysattr reads tts_tid).
        base.tts_tid = ctid;
    }
    exectuples::exec_store_virtual_tuple(slot);
    Ok(true)
}

// postgresReScanForeignScan. Divergence from C: the executor does not track
// chgParam, so any parameterized scan closes + recreates the cursor (same
// results; C skips the recreate when params provably did not change). C also
// MOVEs BACKWARD on pre-15 remotes; we always close + recreate (the v15+
// arm), which every supported remote handles.
pub(crate) fn rescan_foreign_scan<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let Some(state) = fsstate(node) else {
        return Ok(()); // EXPLAIN
    };
    if !state.cursor_exists {
        return Ok(());
    }
    // Complete an in-flight (or parked) async fetch before restarting the
    // scan (postgresReScanForeignScan's async arm; parked = the batch another
    // scan drained for us, see adopt_parked).
    if state.async_capable {
        if connection::pending_fetch(state.conn_key)?
            .is_some_and(|p| p.cursor_number == state.cursor_number)
        {
            fetch_more_data(state)?;
        } else {
            adopt_parked(state)?;
        }
    }
    if state.param_exprs.is_empty() && state.fetch_ct_2 <= 1 {
        // Just rewind the local batch (the cursor has not moved past it).
        state.next_tuple = 0;
        return Ok(());
    }
    let sql = format!("CLOSE c{}", state.cursor_number);
    state.cursor_exists = false;
    let res = connection::exec_query(state.conn_key, &sql)?;
    if res.status != ExecStatus::CommandOk {
        return Err(connection::remote_error(&res, Some(&sql)));
    }
    state.tuples.clear();
    state.batch_mcx.reset();
    state.next_tuple = 0;
    state.fetch_ct_2 = 0;
    state.eof_reached = false;
    Ok(())
}

// postgresEndForeignScan: close the cursor, release the connection (no-op).
pub(crate) fn end_foreign_scan<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    _estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let Some(state) = fsstate(node) else {
        return Ok(()); // EXPLAIN
    };
    // Drain an in-flight async fetch and discard any parked batch: the
    // cursor is going away.
    if state.async_capable {
        if connection::pending_fetch(state.conn_key)?
            .is_some_and(|p| p.cursor_number == state.cursor_number)
        {
            let _ = connection::finish_pending_fetch(state.conn_key, state.query)?;
        }
        connection::drop_parked(state.conn_key, state.cursor_number)?;
    }
    if state.cursor_exists {
        let sql = format!("CLOSE c{}", state.cursor_number);
        let res = connection::exec_query(state.conn_key, &sql)?;
        if res.status != ExecStatus::CommandOk {
            return Err(connection::remote_error(&res, Some(&sql)));
        }
        state.cursor_exists = false;
    }
    connection::release_connection(state.conn_key);
    node.fdw_state = None;
    Ok(())
}

// ---- asynchronous execution (postgres_fdw.c async half) ----

// postgresForeignAsyncRequest.
pub(crate) fn foreign_async_request<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    areq: &mut executils::AsyncRequest,
) -> PgResult<()> {
    produce_tuple_asynchronously(node, estate, areq, true)
}

// postgresForeignAsyncConfigureWait.
pub(crate) fn foreign_async_configure_wait<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    areq: &mut executils::AsyncRequest,
    wait: &executils::AsyncWaitCtx,
) -> PgResult<()> {
    debug_assert!(areq.callback_pending);
    let tuples_ready = {
        let state = fsstate(node).expect("fdw_state set by BeginForeignScan");
        adopt_parked(state)? || state.next_tuple < state.tuples.len()
    };
    if tuples_ready {
        // Another scan already drained our fetch: complete the request.
        complete_pending_request(node, estate, areq)?;
        if areq.request_complete {
            return Ok(());
        }
        debug_assert!(areq.callback_pending);
    }
    let (conn_key, cursor_number) = {
        let state = fsstate(node).expect("fdw_state set by BeginForeignScan");
        debug_assert!(state.next_tuple >= state.tuples.len());
        (state.conn_key, state.cursor_number)
    };
    debug_assert!(waiteventset::GetNumRegisteredWaitEvents(wait.set) >= 1);
    match connection::pending_fetch(conn_key)? {
        None => fetch_more_data_begin(node, estate, areq)?,
        Some(p) if p.requestor_plan_id != areq.requestor_plan_id => {
            // In-flight request from another Append, which may not need more
            // tuples: prefer skipping this request over processing it. But
            // never leave the set with only the postmaster-death event.
            if !wait.needrequest_empty {
                return Ok(());
            }
            if waiteventset::GetNumRegisteredWaitEvents(wait.set) > 1 {
                return Ok(());
            }
            connection::park_pending_fetch(conn_key)?;
            fetch_more_data_begin(node, estate, areq)?;
        }
        Some(p) if p.cursor_number != cursor_number => {
            // Same Append, different child: only that child's event gets
            // configured.
            return Ok(());
        }
        Some(_) => {} // our own fetch is in flight
    }
    waiteventset::AddWaitEventToSet(
        wait.set,
        types_storage::waiteventset::WL_SOCKET_READABLE,
        connection::socket(conn_key)?,
        None,
        Some(areq.request_index),
    )?;
    Ok(())
}

// postgresForeignAsyncNotify.
pub(crate) fn foreign_async_notify<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    areq: &mut executils::AsyncRequest,
) -> PgResult<()> {
    // The core code would have reset callback_pending.
    debug_assert!(!areq.callback_pending);
    let tuples_ready = {
        let state = fsstate(node).expect("fdw_state set by BeginForeignScan");
        adopt_parked(state)? || state.next_tuple < state.tuples.len()
    };
    if !tuples_ready {
        let state = fsstate(node).expect("fdw_state set by BeginForeignScan");
        debug_assert!(connection::pending_fetch(state.conn_key)?
            .is_some_and(|p| p.cursor_number == state.cursor_number));
        fetch_more_data(state)?;
    }
    produce_tuple_asynchronously(node, estate, areq, true)
}

// produce_tuple_asynchronously.
fn produce_tuple_asynchronously<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    areq: &mut executils::AsyncRequest,
    fetch: bool,
) -> PgResult<()> {
    let (conn_key, cursor_number, have_tuple, eof) = {
        let state = fsstate(node).expect("fdw_state set by BeginForeignScan");
        (
            state.conn_key,
            state.cursor_number,
            state.next_tuple < state.tuples.len(),
            state.eof_reached,
        )
    };
    // Not to be called while our own request is in flight.
    debug_assert!(!connection::pending_fetch(conn_key)?
        .is_some_and(|p| p.cursor_number == cursor_number));
    if !have_tuple {
        return produce_out_of_tuples(node, estate, areq, fetch, eof, conn_key);
    }
    // Get a tuple from the ForeignScan node (C ExecProcNodeReal).
    match nodeforeignscan::exec_foreign_scan(node, estate)? {
        Some(slot) => {
            areq.request_complete = true;
            areq.result = Some(slot);
            Ok(())
        }
        None => {
            let eof = fsstate(node).expect("fdw_state").eof_reached;
            produce_out_of_tuples(node, estate, areq, fetch, eof, conn_key)
        }
    }
}

fn produce_out_of_tuples<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    areq: &mut executils::AsyncRequest,
    fetch: bool,
    eof: bool,
    conn_key: Oid,
) -> PgResult<()> {
    if !eof {
        // ExecAsyncRequestPending.
        areq.callback_pending = true;
        areq.request_complete = false;
        areq.result = None;
        // Begin another fetch if requested and no request is pending.
        if fetch && connection::pending_fetch(conn_key)?.is_none() {
            fetch_more_data_begin(node, estate, areq)?;
        }
    } else {
        // ExecAsyncRequestDone(NULL): nothing more to do.
        areq.request_complete = true;
        areq.result = None;
    }
    Ok(())
}

// complete_pending_request: unset callback_pending ourselves and produce
// without starting a new fetch. (C also calls ExecAsyncResponse here; the
// requestor half runs in nodeappend right after ConfigureWait returns.)
fn complete_pending_request<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    areq: &mut executils::AsyncRequest,
) -> PgResult<()> {
    debug_assert!(areq.callback_pending);
    areq.callback_pending = false;
    produce_tuple_asynchronously(node, estate, areq, false)
}

// fetch_more_data_begin: create the cursor synchronously if needed, then
// send the FETCH without waiting for the response.
fn fetch_more_data_begin<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    areq: &mut executils::AsyncRequest,
) -> PgResult<()> {
    let conn_key = fsstate(node).expect("fdw_state").conn_key;
    debug_assert!(connection::pending_fetch(conn_key)?.is_none());
    if !fsstate(node).expect("fdw_state").cursor_exists {
        create_cursor(node, estate)?;
    }
    let state = fsstate(node).expect("fdw_state");
    let sql = format!("FETCH {} FROM c{}", state.fetch_size, state.cursor_number);
    connection::begin_async_fetch(
        state.conn_key,
        &sql,
        state.cursor_number,
        areq.requestor_plan_id,
        state.query,
    )
}
