// postgres_fdw.c executor half (scan path): BeginForeignScan /
// IterateForeignScan / ReScanForeignScan / EndForeignScan over the pgclient
// connection cache. Remote rows arrive as text batches from a cursor
// ("DECLARE c%u CURSOR FOR\n%s" + "FETCH %d FROM c%u", C's exact strings) and
// convert through the local input functions into a per-batch bump context
// (C's batch_cxt shape; the per-row temp_cxt garbage separation is folded
// into the batch reset — bounded by one batch, recorded divergence).
use std::ffi::CString;

use datum::{Datum, NullableDatum};
use mcx::PgBox;
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use types_fmgr::FmgrInfo;
use types_nodes::list::NodeList;
use types_nodes::Node;
use types_tuple::TupleDescData;

use execexpr::{exec_init_expr_subplans, ExprState};
use executils::{EStateData, EcxtId};
use nodeforeignscan::{ForeignScanState, OuterPlanDrive};
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
    // conversion_error_callback's names (postgres_fdw.c:7762-7836), resolved
    // once: one entry per output column, plus the relation name the retrieved
    // ctid (SelfItemPointerAttributeNumber) is reported under.
    ctx: Vec<ConvCtx>,
    ctid_rel: Option<String>,
    in_funcs: Vec<FmgrInfo>,
    typioparams: Vec<Oid>,
    typmods: Vec<i32>,
    typlens: Vec<i16>,
    typbyvals: Vec<bool>,
    tid_in: FmgrInfo,
    tid_ioparam: Oid,
}

// The errcontext line conversion_error_callback prints for one output
// column (postgres_fdw.c:7825-7835).
#[derive(Clone)]
pub(crate) enum ConvCtx {
    /// column "<att>" of foreign table "<rel>"
    Column(String, String),
    /// whole-row reference to foreign table "<rel>"
    WholeRow(String),
    /// processing expression at position %d in select list
    Expr,
}

impl AttInMeta {
    /// The non-ForeignScan shape (postgres_fdw.c:7813-7824): names from the
    /// relation itself (RelationGetRelationName + the tupdesc's attnames).
    pub(crate) fn build(relname: &str, tupdesc: &TupleDescData<'_>) -> PgResult<AttInMeta> {
        let natts = tupdesc.natts as usize;
        let mut ctx = Vec::with_capacity(natts);
        let mut in_funcs = Vec::with_capacity(natts);
        let mut typioparams = Vec::with_capacity(natts);
        let mut typmods = Vec::with_capacity(natts);
        let mut typlens = Vec::with_capacity(natts);
        let mut typbyvals = Vec::with_capacity(natts);
        for i in 0..natts {
            let att = tupdesc.attr(i);
            ctx.push(ConvCtx::Column(
                relname.to_string(),
                String::from_utf8_lossy(att.attname.name_str()).into_owned(),
            ));
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
            ctx,
            ctid_rel: Some(relname.to_string()),
            in_funcs,
            typioparams,
            typmods,
            typlens,
            typbyvals,
            tid_in: fmgr_seams::fmgr_info::call(tid_infunc)?,
            tid_ioparam,
        })
    }

    /// The ForeignScan shape (postgres_fdw.c:7770-7812): names come from the
    /// rangetable aliases, never from the relation, for consistency between
    /// the simple-relation and remote-join cases.
    fn set_scan_context(&mut self, ctx: Vec<ConvCtx>, ctid_rel: Option<String>) {
        debug_assert_eq!(ctx.len(), self.natts);
        self.ctx = ctx;
        self.ctid_rel = ctid_rel;
    }

    fn context_line(&self, attno: i32) -> String {
        let ctx = if attno == types_tuple::htup::SelfItemPointerAttributeNumber {
            self.ctid_rel.as_ref().map(|r| ConvCtx::Column(r.clone(), "ctid".to_string()))
        } else if attno >= 1 && attno as usize <= self.ctx.len() {
            Some(self.ctx[(attno - 1) as usize].clone())
        } else {
            None
        };
        match ctx {
            Some(ConvCtx::WholeRow(rel)) => {
                format!("whole-row reference to foreign table \"{rel}\"")
            }
            Some(ConvCtx::Column(rel, att)) => {
                format!("column \"{att}\" of foreign table \"{rel}\"")
            }
            _ => format!("processing expression at position {attno} in select list"),
        }
    }
}

// The (relname, colnames) pair conversion_error_callback reads off an RTE's
// eref (postgres_fdw.c:7799-7809): the alias name and the column aliases.
fn rte_eref_names(rte: &types_nodes::parsenodes::RangeTblEntry<'_>) -> (String, Vec<String>) {
    let Some(eref) = rte.eref else {
        return (String::new(), Vec::new());
    };
    let colnames = eref
        .colnames
        .iter()
        .map(|n| n.as_string().map(|s| s.sval.to_string()).unwrap_or_default())
        .collect();
    (eref.aliasname.unwrap_or("").to_string(), colnames)
}

// One ConvCtx for a Var of the scan (postgres_fdw.c:7795-7811): colno 0 is
// the whole row, 1..=len(colnames) a named column, ctid by its attno, and
// anything else falls through to the positional message.
fn var_conv_ctx(relname: &str, colnames: &[String], colno: i32) -> ConvCtx {
    if colno == 0 {
        ConvCtx::WholeRow(relname.to_string())
    } else if colno > 0 && colno as usize <= colnames.len() {
        ConvCtx::Column(relname.to_string(), colnames[(colno - 1) as usize].clone())
    } else if colno == types_tuple::htup::SelfItemPointerAttributeNumber {
        ConvCtx::Column(relname.to_string(), "ctid".to_string())
    } else {
        ConvCtx::Expr
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

    // C: rtindex = scanrelid, or the lowest fs_base_relids member for a
    // pushed-down join/upper scan.
    let rte_relid = if fsplan.scan.scanrelid > 0 {
        node.ss.ss_currentRelation.as_ref().expect("base foreign scan has a relation").rd_id
    } else {
        let rtindex = fsplan.fs_base_relids.next_member(-1);
        debug_assert!(rtindex > 0);
        estate.es_range_table[(rtindex - 1) as usize].relid
    };
    let table = foreigncmds::foreign::GetForeignTable(mcx, rte_relid)?;
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

    // Error-context names always come from the rangetable aliases in a scan
    // node (postgres_fdw.c:7770-7812: rte->eref, for consistency between the
    // simple-relation and remote-join cases), never from the relation.
    let attin = if fsplan.scan.scanrelid > 0 {
        let rel = node.ss.ss_currentRelation.as_ref().expect("base foreign scan has a relation");
        let mut attin = AttInMeta::build(rel.name(), &rel.rd_att)?;
        let rte = estate.es_range_table[(fsplan.scan.scanrelid - 1) as usize];
        let (relname, colnames) = rte_eref_names(rte);
        let natts = rel.rd_att.natts as usize;
        let ctx = (1..=natts as i32).map(|colno| var_conv_ctx(&relname, &colnames, colno)).collect();
        attin.set_scan_context(ctx, Some(relname));
        attin
    } else {
        // Join/upper scan tuples follow the fdw_scan_tlist-shaped slot; a Var
        // entry names its relation's alias, an expression only its position.
        let desc = estate
            .slot(node.ss.ss_ScanTupleSlot)
            .base()
            .tts_tupleDescriptor
            .clone()
            .expect("scan slot descriptor");
        let mut attin = AttInMeta::build("foreign join", &desc)?;
        let mut ctx = Vec::with_capacity(desc.natts as usize);
        for tle_node in fsplan.fdw_scan_tlist.iter() {
            let tle = tle_node.as_target_entry().expect("fdw_scan_tlist holds TargetEntries");
            let mut c = ConvCtx::Expr;
            if let Some(var) = tle.expr.as_var() {
                if var.varno > 0 {
                    let rte = estate.es_range_table[(var.varno - 1) as usize];
                    let (relname, colnames) = rte_eref_names(rte);
                    c = var_conv_ctx(&relname, &colnames, var.varattno as i32);
                }
            }
            ctx.push(c);
        }
        ctx.resize(desc.natts as usize, ConvCtx::Expr);
        attin.set_scan_context(ctx, None);
        attin
    };

    let (param_flinfo, param_exprs) = prepare_query_params(estate, &fsplan.fdw_exprs)?;

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

// prepare_query_params: output functions + compiled expressions for
// fdw_exprs (Params after replace_nestloop_params, including initplan output
// params; the shippability walker rejects SubPlan nodes themselves).
pub(crate) fn prepare_query_params<'mcx>(
    estate: &mut EStateData<'mcx>,
    fdw_exprs: &NodeList<'mcx>,
) -> PgResult<(Vec<FmgrInfo>, Vec<PgBox<'static, ExprState<'static>>>)> {
    let mut param_flinfo = Vec::with_capacity(fdw_exprs.len());
    let mut param_exprs = Vec::with_capacity(fdw_exprs.len());
    for expr in fdw_exprs.iter() {
        let (typoutput, _isvarlena) =
            lsyscache::getTypeOutputInfo(nodes_core::node_funcs::expr_type(expr))?;
        param_flinfo.push(fmgr_seams::fmgr_info::call(typoutput)?);
        param_exprs.push(compile_query_param(estate, expr)?);
    }
    Ok((param_flinfo, param_exprs))
}

fn compile_query_param<'mcx>(
    estate: &mut EStateData<'mcx>,
    expr: Node<'mcx>,
) -> PgResult<PgBox<'static, ExprState<'static>>> {
    let mcx = estate.es_query_cxt;
    let pb = estate.param_bind();
    let state = executils::with_subplan_compile_env(estate, |env| {
        exec_init_expr_subplans(mcx, Some(expr), pb, env)
    })?
    .expect("fdw_exprs entries are expressions");
    // SAFETY: es_query_cxt restamp; dropped at end-scan (struct comment).
    Ok(unsafe {
        core::mem::transmute::<PgBox<'mcx, ExprState<'mcx>>, PgBox<'static, ExprState<'static>>>(
            state,
        )
    })
}

// A pending initplan output param ($1 for `c3 = (SELECT MAX(c3) ...)`)
// suspends the interpreter; the subplan driver runs ExecSetParamPlan for it.
pub(crate) fn eval_query_params<'mcx>(
    param_exprs: &mut [PgBox<'static, ExprState<'static>>],
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
) -> PgResult<Vec<NullableDatum>> {
    let mut values = Vec::with_capacity(param_exprs.len());
    for expr in param_exprs.iter_mut() {
        // SAFETY: the reverse of the compile-time restamp (es_query_cxt-lived).
        let expr = unsafe {
            core::mem::transmute::<&mut ExprState<'static>, &mut ExprState<'mcx>>(&mut **expr)
        };
        let per_tuple = estate.ecxt(ecxt).per_tuple_mcx();
        // SAFETY: reset-only per-tuple context, outlives the evaluation.
        unsafe { expr.arm_result_mcx_raw(per_tuple) };
        values.push(executils::exec_eval_expr_with_subplans(expr, estate, ecxt)?);
    }
    Ok(values)
}

// process_query_params: evaluate fdw_exprs and convert to text via the
// output functions, under the transmission modes (datestyle=ISO etc.), as C.
pub(crate) fn process_query_params<'mcx>(
    param_exprs: &mut [PgBox<'static, ExprState<'static>>],
    param_flinfo: &mut [FmgrInfo],
    estate: &mut EStateData<'mcx>,
    ecxt: EcxtId,
) -> PgResult<Vec<Option<String>>> {
    let nestlevel = crate::transmission::set_transmission_modes();
    let r = (|| -> PgResult<Vec<Option<String>>> {
        let scratch = mcx::MemoryContext::new_bump("postgres_fdw param output");
        let datums = eval_query_params(param_exprs, estate, ecxt)?;
        let mut values: Vec<Option<String>> = Vec::with_capacity(datums.len());
        for (nd, flinfo) in datums.iter().zip(param_flinfo.iter_mut()) {
            if nd.isnull {
                values.push(None);
            } else {
                values.push(Some(crate::modify::output_to_string(flinfo, &scratch, nd.value)?));
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
        process_query_params(&mut state.param_exprs, &mut state.param_flinfo, estate, ecxt)?
    };
    let params: Vec<Option<&str>> = values.iter().map(|v| v.as_deref()).collect();
    let sql = format!("DECLARE c{} CURSOR FOR\n{}", state.cursor_number, state.query);
    let res = connection::exec_query_params(state.conn_key, &sql, &params, state.query)?;
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
            Some(bytes) => {
                // The remote session runs with client_encoding set to our
                // database encoding (C's configure_remote_session passes
                // GetDatabaseEncodingName as the connection's client_encoding),
                // so these bytes are only *claimed* to be in the database
                // encoding; a malicious/compromised remote can ship bytes that
                // are invalid in it. C tolerates the resulting mojibake because
                // it stays byte-oriented, but this port has from_utf8_unchecked
                // sinks that rely on the engine invariant that stored text is
                // valid in the database encoding — feeding them invalid bytes
                // is UB (unsound str). Verify here, before the value reaches an
                // input function or becomes a Rust str, raising a catchable
                // error under the column's conversion errcontext. SQL_ASCII
                // accepts every byte, so valid behavior is preserved. This
                // guards both the scan-batch and modify RETURNING paths.
                mbutils::pg_verifymbstr(bytes, false)
                    .map_err(|e| conversion_error(e, attin, i))?;
                Some(CString::new(bytes.as_slice()).map_err(|_| {
                    Box::new(PgError::error("remote value contains embedded NUL byte"))
                })?)
            }
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
    let line = attin.context_line(attno);
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
    // Get a tuple from the ForeignScan node (C ExecProcNodeReal). Async
    // subplans are never registered under EvalPlanQual (nodeAppend.c:205),
    // so the EPQ outer subplan is unreachable from here.
    debug_assert!(!estate.es_epq_active);
    match nodeforeignscan::exec_foreign_scan::<nodeforeignscan::NoOuter>(node, None, estate)? {
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
    produce_tuple_asynchronously(node, estate, areq, false)?;
    // Also, we do instrumentation ourselves, if required (postgres_fdw.c:7573
    // InstrUpdateTupleCount): this tuple bypasses ExecAsyncRequest/Notify's
    // instrumented dispatch (execAsync.c), and ConfigureWait's wrapper stops
    // the node with zero tuples.
    if let Some(idx) = node.ss.instr_idx {
        estate.es_instrumentation[idx as usize].tuplecount +=
            if areq.result.is_some() { 1.0 } else { 0.0 };
    }
    Ok(())
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

#[cfg(test)]
mod query_param_tests {
    use super::*;
    use core::ptr::NonNull;
    use executils::{create_executor_state, free_executor_state, SubplanStateCell};
    use types_nodes::primnodes::{Param, ParamKind};
    use types_portal::params::ParamExecData;

    // ExecSetParamPlan stand-in: fills param 0 and clears its pending bit.
    unsafe fn set_param_plan(_: NonNull<()>, estate: &mut EStateData<'_>) -> PgResult<()> {
        estate.es_param_exec_vals[0] =
            ParamExecData { value: Datum::from_i32(1206), isnull: false, exec_plan: false };
        Ok(())
    }

    #[test]
    fn pending_initplan_param_is_driven() {
        let parent = mcx::MemoryContext::new("postgres_fdw query params");
        let mut estate = create_executor_state(&parent).unwrap();
        estate.with_mut(|es| {
            es.es_param_exec_vals.push(ParamExecData {
                value: Datum::null(),
                isnull: true,
                exec_plan: true,
            });
            es.es_param_subplans.push(Some(SubplanStateCell(NonNull::dangling())));
            es.es_subplan_hook = Some(set_param_plan);
            let ecxt = es.create_expr_context();
            let node = Node::mk(
                es.es_query_cxt,
                Param {
                    paramkind: ParamKind::PARAM_EXEC,
                    paramid: 0,
                    paramtype: types_core::catalog::INT4OID,
                    paramtypmod: -1,
                    paramcollid: 0,
                    location: -1,
                },
            );
            let mut exprs = vec![compile_query_param(es, node.unwrap()).unwrap()];
            let vals = eval_query_params(&mut exprs, es, ecxt).unwrap();
            assert_eq!(vals.len(), 1);
            assert!(!vals[0].isnull);
            assert_eq!(vals[0].value.as_i32(), 1206);
            assert!(!es.es_param_exec_vals[0].exec_plan);
        });
        free_executor_state(estate);
    }
}

// postgresRecheckForeignScan (postgres_fdw.c:2356): execute the local join
// execution plan for a foreign join and store its row in `slot`.
pub(crate) fn recheck_foreign_scan<'mcx>(
    node: &mut ForeignScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    slot: executils::ExecSlotId,
    outer: Option<&mut OuterPlanDrive<'_, 'mcx>>,
) -> PgResult<bool> {
    // For base foreign relations, it suffices to set fdw_recheck_quals.
    if node.plan.scan.scanrelid > 0 {
        return Ok(true);
    }
    let outer = outer.expect("pushed-down foreign join has an outer (EPQ) plan");
    // Execute a local join execution plan.
    let Some(result) = outer(estate)? else {
        return Ok(false);
    };
    // Store result in the given slot.
    let mcx = estate.es_query_cxt;
    let [dst, src] = estate
        .es_tupleTable
        .get_disjoint_mut([slot.0 as usize, result.0 as usize])
        .expect("EPQ test slot and outer result slot are distinct");
    exectuples::exec_copy_slot(dst, src, mcx, mcx)?;
    Ok(true)
}
