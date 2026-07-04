// nodeSeqscan.c. ExecProcNode dispatch is the variant enum resolved once at
// init (C installs one of five function pointers). Parallel-scan entries
// loud-panic pending DSM/shm_toc.
#![allow(non_snake_case)]

extern crate alloc;

use ::execexpr::exec_init_qual;
use ::execscan::{exec_scan_epq, exec_scan_extended, ScanNode, ScanState};
use ::executils::{EStateData, ExecSlotId};
use ::mcx::{Mcx, PgVec};
use ::tableam::{
    table_beginscan, table_endscan, table_rescan, table_scan_getnextslot, table_slot_callbacks,
};
use ::types_error::{PgError, PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE};
use ::types_nodes::plannodes::SeqScan;
use ::types_rel::Relation;
use ::types_slot::{
    EXEC_FLAG_BACKWARD, EXEC_FLAG_EXPLAIN_ONLY, EXEC_FLAG_MARK, EXEC_FLAG_WITH_NO_DATA,
};

pub fn init_seams() {}

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SeqScanVariant {
    Plain,
    WithQual,
    WithProject,
    WithQualProject,
    Epq,
}

pub struct SeqScanState<'mcx> {
    pub ss: ScanState<'mcx>,
    variant: SeqScanVariant,
    // Boxed: PlanStateNode carries a 1024-byte size assert.
    batch_soa: Option<::mcx::PgBox<'mcx, BatchSoa<'mcx>>>,
    scan_batch: ScanBatchMode,
    batch_allowed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScanBatchMode {
    Unknown,
    Off,
    On,
}

struct BatchSoa<'mcx> {
    plan: ::exectuples::SoaDeformPlan<'mcx>,
    soa: ::exectuples::SoaBatch<'mcx>,
    // Bitmap-able kernel qual (QualScanVarCmpConst on a prefix column).
    qual_armed: bool,
    // Scan-node drive: deform the qual column only; survivors deform lazily.
    qual_only: bool,
    // Fused-sort direct key feed: deform this column only, never publish the
    // prefix onto the slot (other prefix cells stay stale).
    key_col: Option<u16>,
    // Precomputed !qual_only && key_col.is_none(): one test on the store path.
    publish: bool,
    qual_col: u16,
    qual_cmp: ::execexpr::CmpOp,
    qual_konst: ::datum::Datum,
    sel: [u64; ::exectuples::SOA_BM_WORDS],
    nwords: u32,
    cur_word: u32,
    cur_bits: u64,
}

impl BatchSoa<'_> {
    #[inline(always)]
    fn next_selected(&mut self) -> Option<u32> {
        loop {
            if self.cur_bits != 0 {
                let bit = self.cur_bits.trailing_zeros();
                self.cur_bits &= self.cur_bits - 1;
                return Some(self.cur_word * 64 + bit);
            }
            if self.cur_word + 1 >= self.nwords {
                return None;
            }
            self.cur_word += 1;
            self.cur_bits = self.sel[self.cur_word as usize];
        }
    }

    fn reset_staged(&mut self) {
        self.nwords = 0;
        self.cur_word = 0;
        self.cur_bits = 0;
    }
}

impl<'mcx> SeqScanState<'mcx> {
    pub fn variant(&self) -> SeqScanVariant {
        self.variant
    }
}

impl<'mcx> ScanNode<'mcx> for SeqScanState<'mcx> {
    #[inline(always)]
    fn ss_mut(&mut self) -> &mut ScanState<'mcx> {
        &mut self.ss
    }

    /// `SeqRecheck`: seqscans have no access-method conditions to re-verify.
    #[inline(always)]
    fn epq_recheck(
        &mut self,
        _estate: &mut EStateData<'mcx>,
        _slot: ExecSlotId,
    ) -> PgResult<bool> {
        Ok(true)
    }

    /// `SeqNext`.
    #[inline(always)]
    fn scan_next(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        let mcx = estate.es_query_cxt;
        let direction = estate.es_direction;

        self.ensure_scandesc(estate)?;

        // SAFETY: written by ensure_scandesc when None; single test+branch
        // like C's scandesc == NULL check.
        let scandesc = unsafe { self.ss.ss_currentScanDesc.as_mut().unwrap_unchecked() };
        let slot = estate.slot_mut(self.ss.ss_ScanTupleSlot);
        table_scan_getnextslot(mcx, scandesc, direction, slot)
    }
}

impl<'mcx> SeqScanState<'mcx> {
    // Hot per-row check stays a single inlined test+branch (C's scandesc ==
    // NULL check); the once-per-scan open is outlined.
    #[inline(always)]
    fn ensure_scandesc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        if self.ss.ss_currentScanDesc.is_none() {
            self.open_scandesc(estate)?;
        }
        Ok(())
    }

    #[inline(never)]
    fn open_scandesc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        let mcx = estate.es_query_cxt;
        let snapshot = estate.es_snapshot.clone();
        self.ss.ss_currentScanDesc = Some(table_beginscan(
            mcx,
            self.ss.ss_currentRelation.as_ref().expect("seqscan has a relation"),
            snapshot,
            0,
            PgVec::new_in(mcx),
        )?);
        Ok(())
    }
}

/// Fused page-batch drive support (upstream batch scan, CF 6176). The caller
/// owns qual/projection evaluation and gates its own variant set.
pub fn seq_scan_batch_supported<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if matches!(node.variant, SeqScanVariant::Epq) {
        return Ok(false);
    }
    node.ensure_scandesc(estate)?;
    let scandesc = node.ss.ss_currentScanDesc.as_ref().unwrap();
    Ok(::tableam::table_scan_supports_pagebatch(scandesc))
}

/// Arm SoA batch deform of the `prefix`-column prefix for the fused drive;
/// stays disarmed (per-row lazy deform) unless the prefix is all fixed-width.
pub fn seq_scan_batch_soa_prepare<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    prefix: i32,
    qual_only: bool,
) {
    if prefix <= 0 {
        node.batch_soa = None;
        return;
    }
    if let Some(b) = &node.batch_soa {
        if b.plan.ncols() as i32 == prefix && b.qual_only == qual_only && b.key_col.is_none() {
            return;
        }
    }
    let mcx = estate.es_query_cxt;
    let rel = node.ss.ss_currentRelation.as_ref().expect("seqscan has a relation");
    let atts: &[_] = &rel.rd_att.compact_attrs;
    let qual = match node.ss.qual.as_deref().map(|q| q.kernel()) {
        Some(::execexpr::Kernel::QualScanVarCmpConst { attnum, konst, cmp })
            if (attnum as i32) < prefix =>
        {
            Some((attnum, cmp, konst))
        }
        _ => None,
    };
    // Break-even: at <=2 fixed columns the deform+gather double-copy loses to
    // the per-row walk (distinct +2.3% instr) unless a bitmap qual skips the
    // gather for non-survivors; group_agg's 3-column prefix wins -4.9%.
    if qual.is_none() && prefix < 3 {
        node.batch_soa = None;
        return;
    }
    node.batch_soa = ::exectuples::SoaDeformPlan::try_new(mcx, atts, prefix as usize).map(|plan| {
        ::mcx::PgBox::new_in(
            BatchSoa {
                soa: ::exectuples::SoaBatch::new_in(mcx, plan.ncols()),
                plan,
                qual_armed: qual.is_some(),
                qual_only: qual_only && qual.is_some(),
                key_col: None,
                publish: !(qual_only && qual.is_some()),
                qual_col: qual.map_or(0, |(a, _, _)| a),
                qual_cmp: qual.map_or(::execexpr::CmpOp::Int4Eq, |(_, c, _)| c),
                qual_konst: qual.map_or(::datum::Datum::null(), |(_, _, k)| k),
                sel: [0; ::exectuples::SOA_BM_WORDS],
                nwords: 0,
                cur_word: 0,
                cur_bits: 0,
            },
            mcx,
        )
    });
}

/// Arm the fused-sort direct key feed: outer column 0 of the scan's output
/// must be exactly one scan Var (bare single-column scan, or a single
/// `JustAssignVar` projection) whose column the fixed-width SoA plan covers,
/// with no qual. False leaves the per-row emit path armed and untouched.
pub fn seq_scan_sortkey_direct<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> bool {
    if node.ss.qual.is_some() {
        return false;
    }
    let rel = node.ss.ss_currentRelation.as_ref().expect("seqscan has a relation");
    let attnum = match node.ss.ps_ProjInfo.as_ref() {
        None if rel.rd_att.natts == 1 => 0u16,
        None => return false,
        Some(p) => match p.pi_state.kernel() {
            ::execexpr::Kernel::JustAssignVar {
                src: ::execexpr::SlotSrc::Scan,
                attnum,
                resultnum: 0,
            } => attnum,
            _ => return false,
        },
    };
    if let Some(b) = &node.batch_soa {
        if b.key_col == Some(attnum) {
            return true;
        }
    }
    let mcx = estate.es_query_cxt;
    let atts: &[_] = &rel.rd_att.compact_attrs;
    let Some(plan) = ::exectuples::SoaDeformPlan::try_new(mcx, atts, attnum as usize + 1)
    else {
        return false;
    };
    node.batch_soa = Some(::mcx::PgBox::new_in(
        BatchSoa {
            soa: ::exectuples::SoaBatch::new_in(mcx, plan.ncols()),
            plan,
            qual_armed: false,
            qual_only: false,
            key_col: Some(attnum),
            publish: false,
            qual_col: 0,
            qual_cmp: ::execexpr::CmpOp::Int4Eq,
            qual_konst: ::datum::Datum::null(),
            sel: [0; ::exectuples::SOA_BM_WORDS],
            nwords: 0,
            cur_word: 0,
            cur_bits: 0,
        },
        mcx,
    ));
    true
}

/// Direct key read for staged row `i`; None = fallback row (narrow tuple),
/// the caller must take the full emit path.
#[inline(always)]
pub fn seq_scan_batch_key<'mcx>(
    node: &SeqScanState<'mcx>,
    i: u32,
) -> Option<(::datum::Datum, bool)> {
    let b = node.batch_soa.as_deref().expect("direct key feed armed");
    let c = b.key_col.expect("direct key feed armed") as usize;
    if b.soa.is_fallback(i) {
        return None;
    }
    Some((b.soa.col_values(c)[i as usize], b.soa.col_isnull(c)[i as usize]))
}

pub fn seq_scan_next_pagebatch<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<u32> {
    node.ensure_scandesc(estate)?;
    let SeqScanState { ss, batch_soa, .. } = node;
    // SAFETY: written by ensure_scandesc when None.
    let scandesc = unsafe { ss.ss_currentScanDesc.as_mut().unwrap_unchecked() };
    let n = ::tableam::table_scan_getnextpagebatch(scandesc)?;
    if n > 0 {
        if let Some(b) = batch_soa.as_mut() {
            let b = &mut **b;
            let qual_col_only =
                (b.qual_only && b.qual_armed).then_some(b.qual_col).or(b.key_col);
            ::tableam::table_scan_batch_deform(scandesc, &b.plan, &mut b.soa, qual_col_only);
            if b.qual_armed {
                ::execexpr::qual_bitmap_cmp_const(
                    b.qual_cmp,
                    b.qual_konst,
                    b.soa.col_values(b.qual_col as usize),
                    b.soa.col_isnull(b.qual_col as usize),
                    &mut b.sel,
                );
                let nwords = (n as usize).div_ceil(64);
                // Skipped rows carry a forced bit; the fetch re-checks them.
                for (w, fb) in b.sel[..nwords].iter_mut().zip(b.soa.fallback_words()) {
                    *w |= fb;
                }
                b.nwords = nwords as u32;
                b.cur_word = 0;
                b.cur_bits = b.sel[0];
            }
        }
    }
    Ok(n)
}

/// Store row `i` of the staged batch and apply the scan qual; false =
/// filtered out (bitmap-armed batches test the selection bit instead of
/// evaluating the kernel per row).
#[inline(always)]
pub fn seq_scan_batch_fetch<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    i: u32,
) -> PgResult<bool> {
    if let Some(b) = node.batch_soa.as_deref() {
        if b.qual_armed {
            if b.sel[(i / 64) as usize] & (1u64 << (i % 64)) == 0 {
                return Ok(false);
            }
            if !b.soa.is_fallback(i) {
                seq_scan_batch_store(node, estate, i);
                return Ok(true);
            }
        }
    }
    seq_scan_batch_store(node, estate, i);
    match node.ss.qual.as_deref_mut() {
        None => Ok(true),
        Some(q) => {
            let slot_id = node.ss.ss_ScanTupleSlot;
            let mut slots = ::execexpr::EvalSlots {
                scan: Some(estate.slot_mut(slot_id)),
                inner: None,
                outer: None,
            };
            ::execexpr::exec_qual(Some(q), &mut slots)
        }
    }
}

#[inline(always)]
pub fn seq_scan_batch_store<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    i: u32,
) {
    let mcx = estate.es_query_cxt;
    let scandesc =
        node.ss.ss_currentScanDesc.as_mut().expect("batch store before batch fetch");
    let slot = estate.slot_mut(node.ss.ss_ScanTupleSlot);
    ::tableam::table_scan_batch_store_slot(mcx, scandesc, i, slot);
    if let Some(b) = node.batch_soa.as_ref() {
        if b.publish {
            ::exectuples::soa_store_prefix(slot, &b.soa, i);
        }
    }
}

/// Fused-feed emit: reset the per-tuple context, fetch row `i`, apply the
/// qual, project — `ExecScanExtended`'s body over a staged batch row. None =
/// filtered; Some = the scan's output slot.
#[inline(always)]
pub fn seq_scan_batch_emit<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    i: u32,
) -> PgResult<Option<ExecSlotId>> {
    estate.ecxt_mut(node.ss.ps_ExprContext).reset();
    if !seq_scan_batch_fetch(node, estate, i)? {
        return Ok(None);
    }
    let scan_id = node.ss.ss_ScanTupleSlot;
    estate.ecxt_mut(node.ss.ps_ExprContext).ecxt_scantuple = Some(scan_id);
    let Some(proj) = node.ss.ps_ProjInfo.as_mut() else {
        return Ok(Some(scan_id));
    };
    let mcx = estate.es_query_cxt;
    let result_id = proj.pi_result_slot;
    let (scan_slot, result_slot) = ::execscan::slot_pair(estate, scan_id, result_id);
    let mut slots = ::execexpr::EvalSlots { scan: Some(scan_slot), inner: None, outer: None };
    ::execexpr::exec_project(&mut proj.pi_state, &mut slots, result_slot, mcx)?;
    Ok(Some(result_id))
}

/// `ExecSeqScan` + its four specialized variants, dispatched on the enum
/// selected at init instead of C's per-variant function pointers.
pub fn exec_seq_scan<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    match node.variant {
        SeqScanVariant::Plain => exec_scan_extended::<_, false, false>(node, estate),
        SeqScanVariant::WithQual => {
            if scan_batch_ready(node, estate)? {
                return exec_seq_scan_batch::<false>(node, estate);
            }
            exec_scan_extended::<_, true, false>(node, estate)
        }
        SeqScanVariant::WithProject => exec_scan_extended::<_, false, true>(node, estate),
        SeqScanVariant::WithQualProject => {
            if scan_batch_ready(node, estate)? {
                return exec_seq_scan_batch::<true>(node, estate);
            }
            exec_scan_extended::<_, true, true>(node, estate)
        }
        SeqScanVariant::Epq => exec_scan_epq(node, estate),
    }
}

#[inline(always)]
fn scan_batch_ready<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    match node.scan_batch {
        ScanBatchMode::On => Ok(true),
        ScanBatchMode::Off => Ok(false),
        ScanBatchMode::Unknown => scan_batch_probe(node, estate),
    }
}

// Once per scan: the page-batch bitmap-qual drive covers uninstrumented
// forward-only kernel-qual scans (subplan-free projection); everything else
// keeps the per-tuple drive.
#[inline(never)]
fn scan_batch_probe<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    node.scan_batch = ScanBatchMode::Off;
    if !node.batch_allowed || node.ss.instr_idx.is_some() || estate.es_epq_active {
        return Ok(false);
    }
    if let Some(p) = node.ss.ps_ProjInfo.as_ref() {
        if p.pi_state.has_subplan() || !p.pi_state.param_exec_deps().is_empty() {
            return Ok(false);
        }
    }
    let Some(q) = node.ss.qual.as_deref() else { return Ok(false) };
    let ::execexpr::Kernel::QualScanVarCmpConst { attnum, .. } = q.kernel() else {
        return Ok(false);
    };
    node.ensure_scandesc(estate)?;
    if !::tableam::table_scan_supports_pagebatch(node.ss.ss_currentScanDesc.as_ref().unwrap()) {
        return Ok(false);
    }
    seq_scan_batch_soa_prepare(node, estate, attnum as i32 + 1, true);
    if node.batch_soa.as_deref().is_some_and(|b| b.qual_armed) {
        node.scan_batch = ScanBatchMode::On;
        return Ok(true);
    }
    node.batch_soa = None;
    Ok(false)
}

#[inline(never)]
fn exec_seq_scan_batch<'mcx, const PROJ: bool>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    debug_assert!(::types_scan::sdir::ScanDirectionIsForward(estate.es_direction));
    estate.ecxt_mut(node.ss.ps_ExprContext).reset();
    loop {
        let next = node.batch_soa.as_deref_mut().expect("batch drive armed").next_selected();
        let Some(i) = next else {
            let n = seq_scan_next_pagebatch(node, estate)?;
            if n == 0 {
                let mcx = estate.es_query_cxt;
                if PROJ {
                    let proj = node.ss.ps_ProjInfo.as_ref().unwrap();
                    ::exectuples::exec_clear_tuple(estate.slot_mut(proj.pi_result_slot), mcx);
                }
                return Ok(None);
            }
            continue;
        };
        if !seq_scan_batch_fetch(node, estate, i)? {
            continue;
        }
        let scan_id = node.ss.ss_ScanTupleSlot;
        estate.ecxt_mut(node.ss.ps_ExprContext).ecxt_scantuple = Some(scan_id);
        if !PROJ {
            return Ok(Some(scan_id));
        }
        let mcx = estate.es_query_cxt;
        let proj = node.ss.ps_ProjInfo.as_mut().unwrap();
        let result_id = proj.pi_result_slot;
        let (scan_slot, result_slot) = ::execscan::slot_pair(estate, scan_id, result_id);
        let mut slots =
            ::execexpr::EvalSlots { scan: Some(scan_slot), inner: None, outer: None };
        ::execexpr::exec_project(&mut proj.pi_state, &mut slots, result_slot, mcx)?;
        return Ok(Some(result_id));
    }
}

/// `ExecInitSeqScan`; opens the scan relation through the estate range table.
pub fn exec_init_seq_scan<'mcx>(
    mcx: Mcx<'mcx>,
    node: &SeqScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<SeqScanState<'mcx>> {
    let rel = exec_open_scan_relation(estate, node, eflags)?;
    let mut state = exec_init_seq_scan_rel(mcx, node, estate, rel)?;
    state.batch_allowed = eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0;
    Ok(state)
}

/// `ExecOpenScanRelation`.
fn exec_open_scan_relation<'mcx>(
    estate: &mut EStateData<'mcx>,
    node: &SeqScan<'mcx>,
    eflags: i32,
) -> PgResult<Relation<'mcx>> {
    let rel = estate.exec_get_range_table_relation(node.scan.scanrelid, false)?;
    if eflags & (EXEC_FLAG_EXPLAIN_ONLY | EXEC_FLAG_WITH_NO_DATA) == 0
        && !rel.rd_rel.relispopulated
    {
        return Err(unpopulated_matview(rel));
    }
    Ok(rel.alias())
}

#[cold]
#[inline(never)]
fn unpopulated_matview(rel: &Relation<'_>) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "materialized view \"{}\" has not been populated",
            rel.name()
        ))
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .with_hint("Use the REFRESH MATERIALIZED VIEW command."),
    )
}

/// C divergence: init over a caller-opened relation (test surface;
/// `exec_init_seq_scan` is the real path through the estate range table).
pub fn exec_init_seq_scan_rel<'mcx>(
    mcx: Mcx<'mcx>,
    node: &SeqScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    rel: Relation<'mcx>,
) -> PgResult<SeqScanState<'mcx>> {
    debug_assert!(node.scan.plan.lefttree.is_none() && node.scan.plan.righttree.is_none());

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

    let variant = if estate.es_epq_active {
        SeqScanVariant::Epq
    } else {
        match (ss.qual.is_some(), ss.ps_ProjInfo.is_some()) {
            (false, false) => SeqScanVariant::Plain,
            (true, false) => SeqScanVariant::WithQual,
            (false, true) => SeqScanVariant::WithProject,
            (true, true) => SeqScanVariant::WithQualProject,
        }
    };
    Ok(SeqScanState {
        ss,
        variant,
        batch_soa: None,
        scan_batch: ScanBatchMode::Unknown,
        batch_allowed: false,
    })
}

/// `ExecEndSeqScan`.
pub fn exec_end_seq_scan(node: &mut SeqScanState<'_>) -> PgResult<()> {
    if let Some(scandesc) = node.ss.ss_currentScanDesc.take() {
        table_endscan(scandesc)?;
    }
    Ok(())
}

/// `ExecReScanSeqScan`.
pub fn exec_rescan_seq_scan<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    if let Some(scan) = node.ss.ss_currentScanDesc.as_mut() {
        table_rescan(mcx, scan, None)?;
    }
    if let Some(b) = node.batch_soa.as_deref_mut() {
        b.reset_staged();
    }
    execscan::exec_scan_rescan(&mut node.ss, estate);
    Ok(())
}

pub fn exec_seq_scan_estimate(_node: &mut SeqScanState<'_>) -> ! {
    panic!("nodeseqscan: ExecSeqScanEstimate pending parallel DSM/shm_toc")
}

pub fn exec_seq_scan_initialize_dsm(_node: &mut SeqScanState<'_>) -> ! {
    panic!("nodeseqscan: ExecSeqScanInitializeDSM pending parallel DSM/shm_toc")
}

pub fn exec_seq_scan_reinitialize_dsm(_node: &mut SeqScanState<'_>) -> ! {
    panic!("nodeseqscan: ExecSeqScanReInitializeDSM pending parallel DSM/shm_toc")
}

pub fn exec_seq_scan_initialize_worker(_node: &mut SeqScanState<'_>) -> ! {
    panic!("nodeseqscan: ExecSeqScanInitializeWorker pending parallel DSM/shm_toc")
}

mcx::forget_safe_nodrop!(SeqScanVariant);

mcx::forget_safe_nodrop!(ScanBatchMode);

mcx::forget_safe_struct!(
    SeqScanState<'_> { ss, variant, batch_soa, scan_batch, batch_allowed },
    BatchSoa<'_> {
        plan, soa, qual_armed, qual_only, key_col, publish, qual_col, qual_cmp, qual_konst,
        sel, nwords, cur_word, cur_bits,
    },
);
