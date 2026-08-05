// nodeValuesscan.c.
#![allow(non_snake_case)]

extern crate alloc;

use ::execexpr::{exec_eval_expr, exec_init_expr, exec_init_qual, EvalSlots, ExprState};
use ::mcx::PgBox;
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
    /// C exprstatelists: SubPlan-bearing rows are pre-initialized once so
    /// their SubPlan states link into the plan tree (nodeValuesscan.c).
    exprstatelists: PgVec<'mcx, Option<PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>>>>,
    curr_idx: i32,
    array_len: i32,
}

impl<'mcx> ScanNode<'mcx> for ValuesScanState<'mcx> {
    #[inline(always)]
    fn ss_mut(&mut self) -> &mut ScanState<'mcx> {
        &mut self.ss
    }

    /// `ValuesRecheck`: nothing to check.
    fn epq_recheck(
        &mut self,
        _estate: &mut EStateData<'mcx>,
        _slot: ExecSlotId,
    ) -> PgResult<bool> {
        Ok(true)
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

        if self.exprstatelists[self.curr_idx as usize].is_some() {
            let rowcontext = self.rowcontext;
            let scan_slot = self.ss.ss_ScanTupleSlot;
            let states = self.exprstatelists[self.curr_idx as usize].as_mut().unwrap();
            {
                let natts = estate.slot_mut(scan_slot).base_mut().tts_values.len();
                assert_eq!(states.len(), natts, "values row length vs scan tupdesc");
            }
            for resind in 0..states.len() {
                // C runs pending initplans lazily inside ExecEvalExpr
                // (ExecEvalParamExec); the $n params resolve here instead.
                if !states[resind].param_exec_deps().is_empty() {
                    let deps = states[resind].param_exec_deps().to_vec();
                    ::executils::exec_eval_param_exec_params(estate, &deps)?;
                }
                let d = ::executils::exec_eval_expr_with_subplans(
                    &mut states[resind],
                    estate,
                    rowcontext,
                )?;
                let base = estate.slot_mut(scan_slot).base_mut();
                base.tts_values[resind] = d.value;
                base.tts_isnull[resind] = d.isnull;
            }
            exectuples::exec_store_virtual_tuple(estate.slot_mut(scan_slot));
            return Ok(true);
        }

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
    // ExecScanFetch: an EPQ recheck substitutes the wholerow rowmark row
    // (ROW_MARK_COPY) for the VALUES rescan; exec_scan reads es_epq_active.
    if estate.es_epq_active {
        return ::execscan::exec_scan(node, estate);
    }
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
    ss.qual = {
        let pb = estate.param_bind();
        ::executils::with_subplan_compile_env(estate, |env| {
            ::execexpr::exec_init_qual_subplans(mcx, &node.scan.plan.qual, pb, env)
        })?
    };

    let array_len = node.values_lists.len() as i32;
    let mut exprlists: PgVec<'mcx, Node<'mcx>> =
        mcx::vec_with_capacity_in(mcx, array_len as usize)?;
    // Droppy ExprState carriers: released in exec_end_values_scan, so the
    // no-drop ctor is skipped (nodeagg peragg precedent).
    let mut exprstatelists: PgVec<'mcx, Option<PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>>>> =
        PgVec::new_in(mcx);
    exprstatelists
        .try_reserve(array_len as usize)
        .map_err(|_| mcx.oom(array_len as usize))?;
    for row in &node.values_lists {
        // Rows referencing initplan $n params ride the pre-initialized leg
        // too: it can run pending initplans (ExecEvalParamExec's lazy arm).
        if !estate.es_subplanstates.is_empty()
            && (clauses::contain_subplans(row)? || clauses::contain_exec_params(row)?)
        {
            let row_list = row.as_list().expect("values row is a List");
            let pb = estate.param_bind();
            let mut states: PgVec<'mcx, PgBox<'mcx, ExprState<'mcx>>> = PgVec::new_in(mcx);
            states.try_reserve(row_list.len()).map_err(|_| mcx.oom(row_list.len()))?;
            ::executils::with_subplan_compile_env(estate, |env| -> PgResult<()> {
                for e in row_list.iter() {
                    states.push(
                        ::execexpr::exec_init_expr_subplans(mcx, Some(e), pb, env)?
                            .expect("non-NULL values expression"),
                    );
                }
                Ok(())
            })?;
            for st in states.iter_mut() {
                // SAFETY: the rowcontext ExprContext outlives the program
                // (same estate, reset-only).
                unsafe { st.arm_result_mcx_raw(estate.ecxt(rowcontext).per_tuple_mcx()) };
            }
            exprstatelists.push(Some(states));
        } else {
            exprstatelists.push(None);
        }
        exprlists.push(row);
    }

    Ok(ValuesScanState { ss, rowcontext, exprlists, exprstatelists, curr_idx: -1, array_len })
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

pub fn exec_end_values_scan(node: &mut ValuesScanState<'_>) {
    node.exprstatelists.clear();
}

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
    ValuesScanState<'_> { ss, rowcontext, exprlists, curr_idx, array_len; exprstatelists },
);
