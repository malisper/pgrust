// nodeValuesscan.c; SubPlan-bearing rows are loud (their eval state must
// link into the outer plan tree, which this port does not model yet).
#![allow(non_snake_case)]

extern crate alloc;

use ::execexpr::{exec_eval_expr, exec_init_expr, exec_init_qual, EvalSlots};
use ::execscan::{exec_scan_extended, ScanNode, ScanState};
use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::mcx::{Mcx, PgVec};
use ::types_error::PgResult;
use ::types_nodes::plannodes::ValuesScan;
use ::types_nodes::Node;
use ::types_slot::TupleSlotKind;

pub fn init_seams() {}

#[cfg(test)]
mod tests;

pub struct ValuesScanState<'mcx> {
    pub ss: ScanState<'mcx>,
    rowcontext: EcxtId,
    exprlists: PgVec<'mcx, Node<'mcx>>,
    curr_idx: i32,
    array_len: i32,
}

impl<'mcx> ScanNode<'mcx> for ValuesScanState<'mcx> {
    #[inline(always)]
    fn ss_mut(&mut self) -> &mut ScanState<'mcx> {
        &mut self.ss
    }

    fn scan_next(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        let forward =
            matches!(estate.es_direction, ::types_scan::ScanDirection::ForwardScanDirection);
        if forward {
            if self.curr_idx < self.array_len {
                self.curr_idx += 1;
            }
        } else if self.curr_idx >= 0 {
            self.curr_idx -= 1;
        }

        let qmcx = estate.es_query_cxt;
        exectuples::exec_clear_tuple(estate.slot_mut(self.ss.ss_ScanTupleSlot), qmcx);

        if self.curr_idx < 0 || self.curr_idx >= self.array_len {
            return Ok(false);
        }

        estate.ecxt_mut(self.rowcontext).reset();

        let row = self.exprlists[self.curr_idx as usize].as_list().expect("values row is a List");
        {
            let natts = estate.slot_mut(self.ss.ss_ScanTupleSlot).base_mut().tts_values.len();
            assert_eq!(row.len(), natts, "values row length vs scan tupdesc");
        }

        for (resind, expr) in row.iter().enumerate() {
            // C builds the row's eval state in the per-row context and drops
            // it at the next reset; the R/W-expanded-datum read-only force is
            // a no-op here (expanded datums are unmodeled).
            let d = {
                let pb = estate.param_bind();
                let mcx = estate.ecxt(self.rowcontext).per_tuple_mcx();
                let mut state =
                    exec_init_expr(mcx, Some(expr), pb)?.expect("non-NULL values expression");
                // C evaluates in the per-row context (CurrentMemoryContext);
                // by-ref results (RowExpr forms) need the frames armed with it.
                state.arm_result_mcx(mcx);
                let mut slots = EvalSlots { scan: None, inner: None, outer: None };
                exec_eval_expr(&mut state, &mut slots)?
            };
            let base = estate.slot_mut(self.ss.ss_ScanTupleSlot).base_mut();
            base.tts_values[resind] = d.value;
            base.tts_isnull[resind] = d.isnull;
        }

        exectuples::exec_store_virtual_tuple(estate.slot_mut(self.ss.ss_ScanTupleSlot));
        Ok(true)
    }
}

pub fn exec_values_scan<'mcx>(
    node: &mut ValuesScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    match (node.ss.qual.is_some(), node.ss.ps_ProjInfo.is_some()) {
        (false, false) => exec_scan_extended::<_, false, false>(node, estate),
        (true, false) => exec_scan_extended::<_, true, false>(node, estate),
        (false, true) => exec_scan_extended::<_, false, true>(node, estate),
        (true, true) => exec_scan_extended::<_, true, true>(node, estate),
    }
}

/// `ExecInitValuesScan`.
pub fn exec_init_values_scan<'mcx>(
    mcx: Mcx<'mcx>,
    node: &ValuesScan<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<ValuesScanState<'mcx>> {
    debug_assert!(node.scan.plan.lefttree.is_none() && node.scan.plan.righttree.is_none());

    let rowcontext = estate.exec_assign_expr_context();
    let ps_ExprContext = estate.exec_assign_expr_context();

    let first_row = node.values_lists.nth(0).as_list().expect("values_lists cell is a List");
    let tupdesc = exec_type_from_expr_list(mcx, &first_row)?;
    let ss_ScanTupleSlot = estate
        .exec_init_extra_tuple_slot(Some(alloc::rc::Rc::new(tupdesc)), TupleSlotKind::Virtual);

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
    ss.qual = exec_init_qual(mcx, &node.scan.plan.qual, estate.param_bind())?;

    let array_len = node.values_lists.len() as i32;
    let mut exprlists: PgVec<'mcx, Node<'mcx>> =
        mcx::vec_with_capacity_in(mcx, array_len as usize)?;
    for row in &node.values_lists {
        if !estate.es_subplanstates.is_empty() && clauses::contain_subplans(row)? {
            panic!(
                "ExecInitValuesScan (nodeValuesscan.c): SubPlan in a VALUES row \
                 (pre-initialized exprstatelists) unported — unit \
                 backend-executor-nodeValuesscan"
            );
        }
        exprlists.push(row);
    }

    Ok(ValuesScanState { ss, rowcontext, exprlists, curr_idx: -1, array_len })
}

// ExecTypeFromExprList (execTuples.c): anonymous RECORD rowtype from the
// exprs' types.
fn exec_type_from_expr_list<'mcx>(
    mcx: Mcx<'mcx>,
    exprs: &types_nodes::NodeList<'mcx>,
) -> PgResult<types_tuple::TupleDescData<'mcx>> {
    let mut d = tupdesc::CreateTemplateTupleDesc(mcx, exprs.len() as i32)?;
    d.tdtypeid = types_core::catalog::RECORDOID;
    d.tdtypmod = -1;
    for (i, e) in exprs.iter().enumerate() {
        let attnum = (i + 1) as i16;
        tupdesc::TupleDescInitEntry(
            &mut d,
            attnum,
            None,
            execexpr::expr_type(e),
            execscan::expr_typmod(e),
            0,
        )?;
        tupdesc::TupleDescInitEntryCollation(&mut d, attnum, execscan::expr_collation(e));
    }
    Ok(d)
}

pub fn exec_end_values_scan(_node: &mut ValuesScanState<'_>) {}

/// `ExecReScanValuesScan`.
pub fn exec_rescan_values_scan<'mcx>(
    node: &mut ValuesScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    execscan::exec_scan_rescan(&mut node.ss, estate);
    node.curr_idx = -1;
    Ok(())
}

mcx::forget_safe_struct!(
    ValuesScanState<'_> { ss, rowcontext, exprlists, curr_idx, array_len },
);
