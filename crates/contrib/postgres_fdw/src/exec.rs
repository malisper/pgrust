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
    query: &'static str,
    fetch_size: i32,
    retrieved_attrs: Vec<i32>,
    attin: AttInMeta,
    // parallel arrays over fdw_exprs (C param_flinfo / param_exprs).
    param_flinfo: Vec<FmgrInfo>,
    param_exprs: Vec<PgBox<'static, ExprState<'static>>>,
    // current batch (C batch_cxt): datum payloads live in batch_mcx.
    batch_mcx: mcx::MemoryContext,
    tuples: Vec<(Vec<Datum>, Vec<bool>)>,
    next_tuple: usize,
    fetch_ct_2: i32,
    eof_reached: bool,
}

// C AttInMetadata over the foreign table's descriptor (dblink's port shape),
// plus typlen/typbyval so retained datums can be copied out of the fmgr
// per-call scratch (see retain_datum).
struct AttInMeta {
    natts: usize,
    relname: String,
    attnames: Vec<String>,
    in_funcs: Vec<FmgrInfo>,
    typioparams: Vec<Oid>,
    typmods: Vec<i32>,
    typlens: Vec<i16>,
    typbyvals: Vec<bool>,
}

impl AttInMeta {
    fn build(relname: &str, tupdesc: &TupleDescData<'_>) -> PgResult<AttInMeta> {
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
        Ok(AttInMeta {
            natts,
            relname: relname.to_string(),
            attnames,
            in_funcs,
            typioparams,
            typmods,
            typlens,
            typbyvals,
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

    if retrieved_attrs.iter().any(|&a| a <= 0) {
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

// fetch_more_data: "FETCH %d FROM c%u", convert the batch.
fn fetch_more_data(state: &mut PgFdwScanState) -> PgResult<()> {
    state.tuples.clear();
    state.batch_mcx.reset();

    let sql = format!("FETCH {} FROM c{}", state.fetch_size, state.cursor_number);
    let res = connection::exec_query(state.conn_key, &sql)?;
    if res.status != ExecStatus::TuplesOk {
        return Err(connection::remote_error(&res, Some(state.query)));
    }
    let numrows = res.rows.len();
    state.tuples.reserve(numrows);
    for row in &res.rows {
        let tup = make_tuple_from_result_row(state, row.len(), row)?;
        state.tuples.push(tup);
    }
    state.next_tuple = 0;
    if state.fetch_ct_2 < 2 {
        state.fetch_ct_2 += 1;
    }
    state.eof_reached = numrows < state.fetch_size as usize;
    Ok(())
}

// make_tuple_from_result_row: text -> datum through the input functions
// (called for NULLs too — domains), with C's conversion errcontext.
fn make_tuple_from_result_row(
    state: &mut PgFdwScanState,
    nfields: usize,
    row: &[Option<Vec<u8>>],
) -> PgResult<(Vec<Datum>, Vec<bool>)> {
    let natts = state.attin.natts;
    let mut values = vec![Datum::null(); natts];
    let mut nulls = vec![true; natts];
    let batch = state.batch_mcx.mcx();

    let mut j = 0usize;
    for &i in &state.retrieved_attrs {
        debug_assert!(i >= 1 && i as usize <= natts, "begin gated system columns");
        let idx = (i - 1) as usize;
        let cell = row.get(j).cloned().flatten();
        let cstr = match &cell {
            None => None,
            Some(bytes) => Some(CString::new(bytes.as_slice()).map_err(|_| {
                Box::new(PgError::error("remote value contains embedded NUL byte"))
            })?),
        };
        let attin = &mut state.attin;
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
                    // (fleet r2 find: every retained text datum aliased the
                    // batch's last row; a use-after-free on Linux).
                    values[idx] = retain_datum(
                        batch,
                        d,
                        attin.typlens[idx],
                        attin.typbyvals[idx],
                    )?;
                }
            }
            Err(e) => return Err(conversion_error(e, &state.attin, i)),
        }
        j += 1;
    }
    // Check that the remote result matches the expected shape.
    if j > 0 && j != nfields {
        return Err(Box::new(PgError::error(
            "remote query result does not match the foreign table",
        )));
    }
    Ok((values, nulls))
}

// Deep-copy a byref datum image into the batch context (datumCopy shape;
// evaluate_expr's byref-image precedent).
fn retain_datum(batch: mcx::Mcx<'_>, d: Datum, typlen: i16, typbyval: bool) -> PgResult<Datum> {
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
fn conversion_error(e: Box<PgError>, attin: &AttInMeta, attno: i32) -> Box<PgError> {
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
        if !state.eof_reached {
            fetch_more_data(state)?;
        }
        if state.next_tuple >= state.tuples.len() {
            exectuples::exec_clear_tuple(estate.slot_mut(scan_slot), qmcx);
            return Ok(false);
        }
    }

    let (values, nulls) = &state.tuples[state.next_tuple];
    state.next_tuple += 1;
    let slot = estate.slot_mut(scan_slot);
    exectuples::exec_clear_tuple(slot, qmcx);
    {
        let base = slot.base_mut();
        base.tts_values.clear();
        base.tts_values.extend_from_slice(values);
        base.tts_isnull.clear();
        base.tts_isnull.extend_from_slice(nulls);
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
