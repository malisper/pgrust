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
use ::types_slot::{EXEC_FLAG_EXPLAIN_ONLY, EXEC_FLAG_WITH_NO_DATA};

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

        if self.ss.ss_currentScanDesc.is_none() {
            let snapshot = estate.es_snapshot.clone();
            self.ss.ss_currentScanDesc = Some(table_beginscan(
                mcx,
                self.ss.ss_currentRelation.as_ref().expect("seqscan has a relation"),
                snapshot,
                0,
                PgVec::new_in(mcx),
            )?);
        }

        // SAFETY: written just above when None; single test+branch like C's
        // scandesc == NULL check.
        let scandesc = unsafe { self.ss.ss_currentScanDesc.as_mut().unwrap_unchecked() };
        let slot = estate.slot_mut(self.ss.ss_ScanTupleSlot);
        table_scan_getnextslot(mcx, scandesc, direction, slot)
    }
}

/// `ExecSeqScan` + its four specialized variants, dispatched on the enum
/// selected at init instead of C's per-variant function pointers.
pub fn exec_seq_scan<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    match node.variant {
        SeqScanVariant::Plain => exec_scan_extended::<_, false, false>(node, estate),
        SeqScanVariant::WithQual => exec_scan_extended::<_, true, false>(node, estate),
        SeqScanVariant::WithProject => exec_scan_extended::<_, false, true>(node, estate),
        SeqScanVariant::WithQualProject => exec_scan_extended::<_, true, true>(node, estate),
        SeqScanVariant::Epq => exec_scan_epq(node, estate),
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
    exec_init_seq_scan_rel(mcx, node, estate, rel)
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
    Ok(SeqScanState { ss, variant })
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

mcx::forget_safe_struct!(
    SeqScanState<'_> { ss, variant },
);
