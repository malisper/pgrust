// nodeNamedtuplestorescan.c; C's `Tuplestorestate *relation` is the
// registry-held store the trigger side owns (reldata handle), so the scan
// never ends it — only its private read pointer moves.
#![allow(non_snake_case)]

use ::execexpr::exec_init_qual;
use ::execscan::{exec_scan_epq, exec_scan_extended, ScanNode, ScanState};
use ::executils::{EStateData, ExecSlotId};
use ::mcx::Mcx;
use ::types_error::PgResult;
use ::types_nodes::plannodes::NamedTuplestoreScan;
use ::types_portal::TuplestoreHandle;
use ::types_slot::{TupleSlotKind, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, EXEC_FLAG_REWIND};

pub fn init_seams() {}

pub struct NamedTuplestoreScanState<'mcx> {
    pub ss: ScanState<'mcx>,
    relation: TuplestoreHandle,
    readptr: i32,
}

impl<'mcx> ScanNode<'mcx> for NamedTuplestoreScanState<'mcx> {
    #[inline(always)]
    fn ss_mut(&mut self) -> &mut ScanState<'mcx> {
        &mut self.ss
    }

    /// `NamedTuplestoreScanRecheck`: nothing to check.
    fn epq_recheck(
        &mut self,
        _estate: &mut EStateData<'mcx>,
        _slot: ExecSlotId,
    ) -> PgResult<bool> {
        Ok(true)
    }

    fn scan_next(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        debug_assert!(::types_scan::sdir::ScanDirectionIsForward(estate.es_direction));
        let mcx = estate.es_query_cxt;
        let readptr = self.readptr;
        let slot = estate.slot_mut(self.ss.ss_ScanTupleSlot);
        ::tuplestore::hold::with_store(self.relation, |ts| {
            ts.select_read_pointer(readptr)?;
            ts.gettupleslot(true, false, slot, mcx)
        })
    }
}

pub fn exec_named_tuplestore_scan<'mcx>(
    node: &mut NamedTuplestoreScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    // C ExecScan reads es_epq_active per call (see nodefunctionscan).
    if estate.es_epq_active {
        return exec_scan_epq(node, estate);
    }
    match (node.ss.qual.is_some(), node.ss.ps_ProjInfo.is_some()) {
        (false, false) => exec_scan_extended::<_, false, false>(node, estate),
        (true, false) => exec_scan_extended::<_, true, false>(node, estate),
        (false, true) => exec_scan_extended::<_, false, true>(node, estate),
        (true, true) => exec_scan_extended::<_, true, true>(node, estate),
    }
}

pub fn exec_init_named_tuplestore_scan<'mcx>(
    mcx: Mcx<'mcx>,
    node: &NamedTuplestoreScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<NamedTuplestoreScanState<'mcx>> {
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);
    debug_assert!(node.scan.plan.lefttree.is_none() && node.scan.plan.righttree.is_none());

    let enrname = node.enrname.expect("NamedTuplestoreScan carries an enrname");
    let enr = estate
        .es_queryEnv
        .and_then(|env| ::queryenvironment::get_ENR(env, enrname))
        .unwrap_or_else(|| {
            panic!(
                "ExecInitNamedTuplestoreScan (nodeNamedtuplestorescan.c): executor \
                 could not find named tuplestore \"{enrname}\""
            )
        });
    debug_assert!(!enr.reldata.is_null());
    let relation = enr.reldata;
    let tupdesc = ::queryenvironment::ENRMetadataGetTupDesc(mcx, &enr.md)?;

    // The new read pointer copies pointer 0's position: rewind it explicitly.
    let readptr = ::tuplestore::hold::with_store(relation, |ts| {
        let p = ts.alloc_read_pointer(EXEC_FLAG_REWIND);
        ts.select_read_pointer(p)?;
        ts.rescan()?;
        Ok::<i32, Box<::types_error::PgError>>(p)
    })?;

    let ps_ExprContext = estate.exec_assign_expr_context();
    let ss_ScanTupleSlot =
        estate.exec_init_extra_tuple_slot(Some(tupdesc), TupleSlotKind::MinimalTuple);
    let mut ss = ScanState {
        qual: None,
        ps_ProjInfo: None,
        ps_ExprContext,
        scanrelid: node.scan.scanrelid,
        ss_currentRelation: None,
        ss_currentScanDesc: None,
        ss_ScanTupleSlot,
        instr_idx: None,
    };
    execscan::exec_assign_scan_projection_info(mcx, estate, &mut ss, &node.scan.plan.targetlist)?;
    ss.qual = {
        let pb = estate.param_bind();
        ::executils::with_subplan_compile_env(estate, |env| {
            ::execexpr::exec_init_qual_subplans(mcx, &node.scan.plan.qual, pb, env)
        })?
    };

    Ok(NamedTuplestoreScanState { ss, relation, readptr })
}

pub fn exec_rescan_named_tuplestore_scan<'mcx>(
    node: &mut NamedTuplestoreScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    execscan::exec_scan_rescan(&mut node.ss, estate);
    ::tuplestore::hold::with_store(node.relation, |ts| {
        ts.select_read_pointer(node.readptr)?;
        ts.rescan()
    })
}

mcx::forget_safe_struct!(
    NamedTuplestoreScanState<'_> { ss, relation, readptr },
);
