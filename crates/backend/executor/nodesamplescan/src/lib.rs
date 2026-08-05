// nodeSamplescan.c over the TSM dispatch enum (tablesample crate; in-core
// bernoulli/system plus the contrib system_rows/system_time extension arms).
#![allow(non_snake_case)]

use ::datum::Datum;
use ::execexpr::{exec_eval_expr, EvalSlots, ExprState};
use ::execscan::{exec_scan_epq, exec_scan_extended, ScanNode, ScanState};
use ::executils::{EStateData, ExecSlotId};
use ::mcx::{Mcx, PgBox, PgVec};
use ::tableam::{
    table_beginscan_sampling, table_endscan, table_rescan_set_params,
    table_scan_sample_next_block, table_scan_sample_next_tuple, table_slot_callbacks,
};
use ::tablesample::{Tsm, TsmState};
use ::types_error::{
    PgError, PgResult, ERRCODE_INVALID_TABLESAMPLE_ARGUMENT, ERRCODE_INVALID_TABLESAMPLE_REPEAT,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
};
use ::types_nodes::plannodes::SampleScan;
use ::types_slot::{EXEC_FLAG_EXPLAIN_ONLY, EXEC_FLAG_WITH_NO_DATA};

pub fn init_seams() {}

pub struct SampleScanState<'mcx> {
    pub ss: ScanState<'mcx>,
    args: PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>>,
    repeatable: Option<PgBox<'mcx, ExprState<'mcx>>>,
    tsm: Tsm,
    tsm_state: TsmState,
    seed: u32,
    use_bulkread: bool,
    use_pagemode: bool,
    epq: bool,
    begun: bool,
    done: bool,
    haveblock: bool,
    pub donetuples: i64,
}

impl<'mcx> ScanNode<'mcx> for SampleScanState<'mcx> {
    #[inline(always)]
    fn ss_mut(&mut self) -> &mut ScanState<'mcx> {
        &mut self.ss
    }

    /// `SampleRecheck`: like SeqScan, no AM conditions to re-verify.
    #[inline(always)]
    fn epq_recheck(
        &mut self,
        _estate: &mut EStateData<'mcx>,
        _slot: ExecSlotId,
    ) -> PgResult<bool> {
        Ok(true)
    }

    /// `SampleNext`.
    fn scan_next(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        if !self.begun {
            self.tablesample_init(estate)?;
        }
        self.tablesample_getnext(estate)
    }
}

/// `ExecSampleScan`.
pub fn exec_sample_scan<'mcx>(
    node: &mut SampleScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    if node.epq {
        return exec_scan_epq(node, estate);
    }
    match (node.ss.qual.is_some(), node.ss.ps_ProjInfo.is_some()) {
        (false, false) => exec_scan_extended::<_, false, false>(node, estate),
        (true, false) => exec_scan_extended::<_, true, false>(node, estate),
        (false, true) => exec_scan_extended::<_, false, true>(node, estate),
        (true, true) => exec_scan_extended::<_, true, true>(node, estate),
    }
}

/// `ExecInitSampleScan`.
pub fn exec_init_sample_scan<'mcx>(
    mcx: Mcx<'mcx>,
    node: &'mcx SampleScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<SampleScanState<'mcx>> {
    debug_assert!(node.scan.plan.lefttree.is_none() && node.scan.plan.righttree.is_none());
    let tsc = node
        .tablesample
        .expect("SampleScan without a tablesample clause")
        .as_table_sample_clause()
        .expect("SampleScan tablesample is a TableSampleClause");

    let rel = estate.exec_get_range_table_relation(node.scan.scanrelid, false)?;
    if eflags & (EXEC_FLAG_EXPLAIN_ONLY | EXEC_FLAG_WITH_NO_DATA) == 0
        && !rel.rd_rel.relispopulated
    {
        return Err(unpopulated_matview(&rel));
    }
    let rel = rel.alias();

    let ps_ExprContext = estate.exec_assign_expr_context();
    let kind = table_slot_callbacks(&rel);
    let ss_ScanTupleSlot = estate.exec_init_extra_tuple_slot(Some(rel.rd_att.clone()), kind);

    let mut ss = ScanState {
        qual: None,
        ps_ProjInfo: None,
        ps_ExprContext,
        scanrelid: node.scan.scanrelid,
        ss_currentRelation: Some(rel),
        ss_currentScanDesc: None,
        ss_ScanTupleSlot,
        instr_idx: None,
    };
    execscan::exec_assign_scan_projection_info(mcx, estate, &mut ss, &node.scan.plan.targetlist)?;
    let params = estate.param_bind();
    ss.qual = ::executils::with_subplan_compile_env(estate, |env| {
        ::execexpr::exec_init_qual_subplans(mcx, &node.scan.plan.qual, params, env)
    })?;

    let mut args = PgVec::new_in(mcx);
    for arg in tsc.args.iter() {
        let mut state = ::execexpr::exec_init_expr(mcx, Some(arg), params)?.expect("arg is Some");
        // C evaluates TABLESAMPLE args in ps_ExprContext's per-tuple memory;
        // by-ref intermediates ride the armed result mcx.
        // SAFETY: the ExprContext outlives the programs (same estate).
        unsafe { state.arm_result_mcx_raw(estate.ecxt(ps_ExprContext).per_tuple_mcx()) };
        args.push(state);
    }
    let mut repeatable = ::execexpr::exec_init_expr(mcx, tsc.repeatable, params)?;
    if let Some(st) = repeatable.as_mut() {
        // Same convention as the args above.
        // SAFETY: the ExprContext outlives the program (same estate).
        unsafe { st.arm_result_mcx_raw(estate.ecxt(ps_ExprContext).per_tuple_mcx()) };
    }

    // Seed once at init so it stays fixed over rescans (C picks it here iff
    // there is no REPEATABLE clause).
    let seed = if repeatable.is_none() {
        pg_prng::global_prng(pg_prng::PgPrng::next_u32)
    } else {
        0
    };

    let tsm = Tsm::get(mcx, tsc.tsmhandler)?;
    Ok(SampleScanState {
        ss,
        args,
        repeatable,
        tsm,
        tsm_state: tsm.init_state(),
        seed,
        use_bulkread: true,
        use_pagemode: true,
        epq: estate.es_epq_active,
        begun: false,
        done: false,
        haveblock: false,
        donetuples: 0,
    })
}

/// `ExecEndSampleScan`.
pub fn exec_end_sample_scan(node: &mut SampleScanState<'_>) -> PgResult<()> {
    if let Some(scandesc) = node.ss.ss_currentScanDesc.take() {
        table_endscan(scandesc)?;
    }
    Ok(())
}

/// `ExecReScanSampleScan`.
pub fn exec_rescan_sample_scan<'mcx>(
    node: &mut SampleScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    node.begun = false;
    node.done = false;
    node.haveblock = false;
    node.donetuples = 0;
    execscan::exec_scan_rescan(&mut node.ss, estate);
    Ok(())
}

impl<'mcx> SampleScanState<'mcx> {
    /// `tablesample_init`.
    #[inline(never)]
    fn tablesample_init(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        let mcx = estate.es_query_cxt;
        self.donetuples = 0;

        let ecxt = self.ss.ps_ExprContext;
        let mut params: PgVec<'mcx, Datum> = PgVec::new_in(mcx);
        for arg in self.args.iter_mut() {
            estate.reset_expr_context(ecxt);
            let mut slots = EvalSlots { scan: None, inner: None, outer: None };
            let v = exec_eval_expr(arg, &mut slots)?;
            if v.isnull {
                return Err(null_param());
            }
            params.push(v.value);
        }

        let seed = match self.repeatable.as_deref_mut() {
            Some(expr) => {
                estate.reset_expr_context(ecxt);
                let mut slots = EvalSlots { scan: None, inner: None, outer: None };
                let v = exec_eval_expr(expr, &mut slots)?;
                if v.isnull {
                    return Err(null_repeatable());
                }
                // hashfloat8 keeps REPEATABLE(0) machine-independent (as C).
                adt_float::builtins::hashfloat8(v.value.as_f64())
            }
            None => self.seed,
        };

        self.use_bulkread = true;
        self.use_pagemode = true;
        let (bulkread, pagemode) = self.tsm_state.begin_sample_scan(&params, seed)?;
        self.use_bulkread = bulkread;
        self.use_pagemode = pagemode;

        let allow_sync = !self.tsm.has_next_sample_block();
        match self.ss.ss_currentScanDesc.as_mut() {
            None => {
                let snapshot = estate.es_snapshot.clone();
                self.ss.ss_currentScanDesc = Some(table_beginscan_sampling(
                    mcx,
                    self.ss.ss_currentRelation.as_ref().expect("samplescan has a relation"),
                    snapshot,
                    0,
                    PgVec::new_in(mcx),
                    self.use_bulkread,
                    allow_sync,
                    self.use_pagemode,
                )?);
            }
            Some(scandesc) => {
                table_rescan_set_params(
                    mcx,
                    scandesc,
                    None,
                    self.use_bulkread,
                    allow_sync,
                    self.use_pagemode,
                )?;
            }
        }

        self.begun = true;
        Ok(())
    }

    fn tablesample_getnext(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        let mcx = estate.es_query_cxt;
        exectuples::exec_clear_tuple(estate.slot_mut(self.ss.ss_ScanTupleSlot), mcx);
        if self.done {
            return Ok(false);
        }
        let SampleScanState { ss, tsm_state, haveblock, done, donetuples, .. } = self;
        loop {
            let scan = ss.ss_currentScanDesc.as_mut().expect("sample scan begun");
            if !*haveblock {
                if !table_scan_sample_next_block(mcx, scan, tsm_state, *donetuples)? {
                    *haveblock = false;
                    *done = true;
                    return Ok(false);
                }
                *haveblock = true;
            }
            let slot = estate.slot_mut(ss.ss_ScanTupleSlot);
            if !table_scan_sample_next_tuple(mcx, scan, tsm_state, *donetuples, slot)? {
                *haveblock = false;
                continue;
            }
            break;
        }
        *donetuples += 1;
        Ok(true)
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn null_param() -> Box<PgError> {
    Box::new(
        PgError::error("TABLESAMPLE parameter cannot be null")
            .with_sqlstate(ERRCODE_INVALID_TABLESAMPLE_ARGUMENT),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn null_repeatable() -> Box<PgError> {
    Box::new(
        PgError::error("TABLESAMPLE REPEATABLE parameter cannot be null")
            .with_sqlstate(ERRCODE_INVALID_TABLESAMPLE_REPEAT),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn unpopulated_matview(rel: &::types_rel::Relation<'_>) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "materialized view \"{}\" has not been populated",
            rel.name()
        ))
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .with_hint("Use the REFRESH MATERIALIZED VIEW command."),
    )
}

// args/repeatable exempt: ExprStates release with procnode teardown.
mcx::forget_safe_struct!(
    SampleScanState<'_> {
        ss, tsm, tsm_state, seed, use_bulkread, use_pagemode, epq, begun, done,
        haveblock, donetuples; args, repeatable,
    },
);
