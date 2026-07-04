// nodeAppend.c, sync-sequential slice with runtime partition pruning:
// async/parallel lanes are loud.
#![allow(non_snake_case)]

use ::execpartition::pruning::PartitionPruneState;
use ::executils::{EStateData, ExecSlotId};
use ::types_error::PgResult;
use ::types_nodes::bitmapset::Bitmapset;
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
    as_prune_state: Option<Box<PartitionPruneState<'mcx>>>,
    as_valid_subplans_identified: bool,
    as_valid_subplans: Bitmapset<'mcx>,
}

pub fn exec_init_append<'mcx>(
    node: &'mcx Append<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
    nplans: usize,
    prune_state: Option<Box<PartitionPruneState<'mcx>>>,
) -> PgResult<AppendState<'mcx>> {
    debug_assert!(eflags & EXEC_FLAG_MARK == 0);
    if node.nasyncplans != 0 {
        panic!("ExecInitAppend (nodeAppend.c): async-capable subplans not ported");
    }
    if node.plan.parallel_aware {
        panic!("ExecInitAppend (nodeAppend.c): parallel-aware Append not ported");
    }
    let mcx = estate.es_query_cxt;
    let mut st = AppendState {
        plan: node,
        as_whichplan: INVALID_SUBPLAN_INDEX,
        as_begun: false,
        as_nplans: nplans,
        as_prune_state: prune_state,
        as_valid_subplans_identified: false,
        as_valid_subplans: Bitmapset::empty(),
    };
    let do_exec_prune = st.as_prune_state.as_ref().is_some_and(|p| p.do_exec_prune);
    if !do_exec_prune && nplans > 0 {
        ::partprune::bms_add_range(mcx, &mut st.as_valid_subplans, 0, nplans as i32 - 1)?;
        st.as_valid_subplans_identified = true;
    }
    Ok(st)
}

/// `ExecAppend` with `choose_next_subplan_locally` inlined.
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
        if !node.as_valid_subplans_identified {
            let ps = node
                .as_prune_state
                .as_mut()
                .expect("unidentified valid set implies an exec prune state");
            node.as_valid_subplans =
                ::execpartition::pruning::exec_find_matching_subplans(ps, estate, false, None)?;
            node.as_valid_subplans_identified = true;
        }
        let first = node.as_valid_subplans.next_member(-1);
        if first < 0 {
            return Ok(None);
        }
        node.as_whichplan = first;
        node.as_begun = true;
    }
    loop {
        if init_small::globals::InterruptPending() {
            postgres_seams::check_for_interrupts::call()?;
        }
        let whichplan = node.as_whichplan;
        if whichplan < 0 || whichplan as usize >= node.as_nplans {
            return Ok(None);
        }
        if let Some(slot) = fetch_subplan(estate, whichplan as usize)? {
            return Ok(Some(slot));
        }
        node.as_whichplan = node.as_valid_subplans.next_member(whichplan);
    }
}

pub fn exec_end_append(node: &mut AppendState<'_>) {
    node.as_prune_state = None;
}

pub fn exec_rescan_append(node: &mut AppendState<'_>) {
    node.as_whichplan = INVALID_SUBPLAN_INDEX;
    node.as_begun = false;
}

pub fn exec_rescan_append_chg<'mcx>(node: &mut AppendState<'mcx>, chg: &Bitmapset<'mcx>) {
    if let Some(ps) = node.as_prune_state.as_ref() {
        if chg.overlap(&ps.execparamids) {
            node.as_valid_subplans_identified = false;
            node.as_valid_subplans = Bitmapset::empty();
        }
    }
    exec_rescan_append(node);
}

// Exempt: as_prune_state is a droppy owner, released by exec_end_append.
mcx::forget_safe_struct!(
    AppendState<'_> { plan, as_whichplan, as_begun, as_nplans,
        as_valid_subplans_identified, as_valid_subplans; as_prune_state },
);
