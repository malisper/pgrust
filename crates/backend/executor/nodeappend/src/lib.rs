// nodeAppend.c, sync-sequential slice: async/parallel/pruning lanes are loud.
#![allow(non_snake_case)]

use ::executils::{EStateData, ExecSlotId};
use ::types_error::PgResult;
use ::types_nodes::plannodes::Append;
use ::types_scan::ScanDirection;
use ::types_slot::EXEC_FLAG_MARK;

pub fn init_seams() {}

const INVALID_SUBPLAN_INDEX: i32 = -1;

pub struct AppendState<'mcx> {
    pub plan: &'mcx Append<'mcx>,
    as_whichplan: i32,
    as_begun: bool,
    as_nplans: usize,
}

/// `ExecInitAppend` minus child linkage (caller inits subplans in order).
pub fn exec_init_append<'mcx>(
    node: &'mcx Append<'mcx>,
    _estate: &mut EStateData<'mcx>,
    eflags: i32,
    nplans: usize,
) -> PgResult<AppendState<'mcx>> {
    debug_assert!(eflags & EXEC_FLAG_MARK == 0);
    if node.nasyncplans != 0 {
        panic!("ExecInitAppend (nodeAppend.c): async-capable subplans not ported");
    }
    if node.part_prune_index != -1 {
        panic!("ExecInitAppend (nodeAppend.c): run-time partition pruning not ported");
    }
    if node.plan.parallel_aware {
        panic!("ExecInitAppend (nodeAppend.c): parallel-aware Append not ported");
    }
    debug_assert_eq!(nplans, node.appendplans.len());
    Ok(AppendState {
        plan: node,
        as_whichplan: INVALID_SUBPLAN_INDEX,
        as_begun: false,
        as_nplans: nplans,
    })
}

/// `ExecAppend` with `choose_next_subplan_locally` inlined (valid set = all).
pub fn exec_append<'mcx, F>(
    node: &mut AppendState<'mcx>,
    estate: &mut EStateData<'mcx>,
    mut fetch_subplan: F,
) -> PgResult<Option<ExecSlotId>>
where
    F: FnMut(&mut EStateData<'mcx>, usize) -> PgResult<Option<ExecSlotId>>,
{
    if estate.es_direction != ScanDirection::ForwardScanDirection {
        panic!("ExecAppend (nodeAppend.c): backward Append scan not ported");
    }
    if !node.as_begun {
        debug_assert!(node.as_whichplan == INVALID_SUBPLAN_INDEX);
        if node.as_nplans == 0 {
            return Ok(None);
        }
        node.as_whichplan = 0;
        node.as_begun = true;
    }
    loop {
        if init_small::globals::InterruptPending() {
            postgres_seams::check_for_interrupts::call()?;
        }
        let whichplan = node.as_whichplan as usize;
        if whichplan >= node.as_nplans {
            return Ok(None);
        }
        if let Some(slot) = fetch_subplan(estate, whichplan)? {
            return Ok(Some(slot));
        }
        node.as_whichplan += 1;
    }
}

pub fn exec_end_append(_node: &mut AppendState<'_>) {}

pub fn exec_rescan_append(node: &mut AppendState<'_>) {
    node.as_whichplan = INVALID_SUBPLAN_INDEX;
    node.as_begun = false;
}

mcx::forget_safe_struct!(
    AppendState<'_> { plan, as_whichplan, as_begun, as_nplans },
);
