// nodeMergeAppend.c with the binary heap of lib/binaryheap.c specialized to
// subplan indexes (max-heap over the inverted slot comparison, as C).
#![allow(non_snake_case)]

use ::execpartition::pruning::PartitionPruneState;
use ::executils::{EStateData, ExecSlotId};
use ::tuplesort::{
    apply_sort_comparator_in, prepare_sort_support_from_ordering_op, SortSupport, SortSupportInit,
};
use ::types_error::PgResult;
use ::types_nodes::bitmapset::Bitmapset;
use ::types_nodes::plannodes::MergeAppend;
use ::types_slot::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK};

pub fn init_seams() {}

pub struct MergeAppendState<'mcx> {
    pub plan: &'mcx MergeAppend<'mcx>,
    ms_nplans: usize,
    ms_sortkeys: mcx::PgVec<'mcx, SortSupport>,
    ms_slots: mcx::PgVec<'mcx, Option<ExecSlotId>>,
    ms_heap: mcx::PgVec<'mcx, i32>,
    ms_initialized: bool,
    ms_prune_state: Option<Box<PartitionPruneState<'mcx>>>,
    ms_valid_subplans_identified: bool,
    ms_valid_subplans: Bitmapset<'mcx>,
}

pub fn exec_init_merge_append<'mcx>(
    node: &'mcx MergeAppend<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
    nplans: usize,
    prune_state: Option<Box<PartitionPruneState<'mcx>>>,
) -> PgResult<MergeAppendState<'mcx>> {
    debug_assert!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0);
    let mcx = estate.es_query_cxt;
    let mut st = MergeAppendState {
        plan: node,
        ms_nplans: nplans,
        ms_sortkeys: mcx::vec_with_capacity_in(mcx, node.numCols as usize)?,
        ms_slots: mcx::vec_with_capacity_in(mcx, nplans)?,
        ms_heap: mcx::vec_with_capacity_in(mcx, nplans)?,
        ms_initialized: false,
        ms_prune_state: prune_state,
        ms_valid_subplans_identified: false,
        ms_valid_subplans: Bitmapset::empty(),
    };
    for _ in 0..nplans {
        st.ms_slots.push(None);
    }
    let do_exec_prune = st.ms_prune_state.as_ref().is_some_and(|p| p.do_exec_prune);
    if !do_exec_prune && nplans > 0 {
        ::partprune::bms_add_range(mcx, &mut st.ms_valid_subplans, 0, nplans as i32 - 1)?;
        st.ms_valid_subplans_identified = true;
    }
    for i in 0..node.numCols as usize {
        let init = SortSupportInit {
            ssup_collation: node.collations[i],
            ssup_nulls_first: node.nullsFirst[i],
            ssup_attno: node.sortColIdx[i],
        };
        // abbreviate = false: tuples enter the heap one at a time.
        st.ms_sortkeys.push(prepare_sort_support_from_ordering_op(node.sortOperators[i], &init)?);
    }
    Ok(st)
}

pub fn exec_merge_append<'mcx, F>(
    node: &mut MergeAppendState<'mcx>,
    estate: &mut EStateData<'mcx>,
    mut fetch_subplan: F,
) -> PgResult<Option<ExecSlotId>>
where
    F: FnMut(&mut EStateData<'mcx>, usize) -> PgResult<Option<ExecSlotId>>,
{
    if init_small::globals::InterruptPending() {
        postgres_seams::check_for_interrupts::call()?;
    }

    if !node.ms_initialized {
        if node.ms_nplans == 0 {
            return Ok(None);
        }
        if !node.ms_valid_subplans_identified {
            let ps = node
                .ms_prune_state
                .as_mut()
                .expect("unidentified valid set implies an exec prune state");
            node.ms_valid_subplans =
                ::execpartition::pruning::exec_find_matching_subplans(ps, estate, false, None)?;
            node.ms_valid_subplans_identified = true;
        }
        let mut i = node.ms_valid_subplans.next_member(-1);
        while i >= 0 {
            let slot = fetch_subplan(estate, i as usize)?;
            node.ms_slots[i as usize] = slot;
            if slot.is_some() {
                node.ms_heap.push(i);
            }
            i = node.ms_valid_subplans.next_member(i);
        }
        binaryheap_build(node, estate);
        node.ms_initialized = true;
    } else {
        let i = node.ms_heap[0];
        let slot = fetch_subplan(estate, i as usize)?;
        node.ms_slots[i as usize] = slot;
        if slot.is_some() {
            sift_down(node, 0, estate);
        } else {
            binaryheap_remove_first(node, estate);
        }
    }

    match node.ms_heap.first() {
        None => Ok(None),
        Some(&i) => Ok(node.ms_slots[i as usize]),
    }
}

// heap_compare_slots (nodeMergeAppend.c): INVERT_COMPARE_RESULT'ed three-way
// key comparison, making the max-heap yield the smallest tuple first.
fn heap_compare_slots<'mcx>(
    node: &MergeAppendState<'mcx>,
    estate: &mut EStateData<'mcx>,
    a: i32,
    b: i32,
) -> i32 {
    let mcx = estate.es_query_cxt;
    let id1 = node.ms_slots[a as usize].expect("compared subplan slot is empty");
    let id2 = node.ms_slots[b as usize].expect("compared subplan slot is empty");
    let table = &mut estate.es_tupleTable[..];
    let [s1, s2] = table
        .get_disjoint_mut([id1.0 as usize, id2.0 as usize])
        .expect("distinct in-range merge slot ids");
    for key in node.ms_sortkeys.iter() {
        let attno = key.ssup_attno as i32;
        let mut isnull1 = false;
        let mut isnull2 = false;
        let datum1 = ::exectuples::slot_getattr(s1, attno, &mut isnull1);
        let datum2 = ::exectuples::slot_getattr(s2, attno, &mut isnull2);
        let compare = apply_sort_comparator_in(mcx, datum1, isnull1, datum2, isnull2, key);
        if compare != 0 {
            return if compare < 0 { 1 } else { compare.wrapping_neg() };
        }
    }
    0
}

// binaryheap_build (binaryheap.c) over heap_compare_slots.
fn binaryheap_build<'mcx>(node: &mut MergeAppendState<'mcx>, estate: &mut EStateData<'mcx>) {
    let n = node.ms_heap.len() as i32;
    if n <= 1 {
        return;
    }
    for i in (0..=(n - 2) / 2).rev() {
        sift_down(node, i, estate);
    }
}

// binaryheap_remove_first (binaryheap.c).
fn binaryheap_remove_first<'mcx>(node: &mut MergeAppendState<'mcx>, estate: &mut EStateData<'mcx>) {
    let last = node.ms_heap.pop().expect("binaryheap_remove_first on empty heap");
    if !node.ms_heap.is_empty() {
        node.ms_heap[0] = last;
        sift_down(node, 0, estate);
    }
}

// sift_down (binaryheap.c), hole-motion form.
fn sift_down<'mcx>(node: &mut MergeAppendState<'mcx>, mut node_off: i32, estate: &mut EStateData<'mcx>) {
    let size = node.ms_heap.len() as i32;
    let node_val = node.ms_heap[node_off as usize];
    loop {
        let left_off = 2 * node_off + 1;
        let right_off = 2 * node_off + 2;
        let mut swap_off = left_off;
        if right_off < size {
            let l = node.ms_heap[left_off as usize];
            let r = node.ms_heap[right_off as usize];
            if heap_compare_slots(node, estate, l, r) < 0 {
                swap_off = right_off;
            }
        }
        if left_off >= size {
            break;
        }
        let swap_val = node.ms_heap[swap_off as usize];
        if heap_compare_slots(node, estate, node_val, swap_val) >= 0 {
            break;
        }
        node.ms_heap[node_off as usize] = swap_val;
        node_off = swap_off;
    }
    node.ms_heap[node_off as usize] = node_val;
}

pub fn exec_end_merge_append(node: &mut MergeAppendState<'_>) {
    node.ms_prune_state = None;
}

pub fn exec_rescan_merge_append(node: &mut MergeAppendState<'_>) {
    node.ms_heap.clear();
    for s in node.ms_slots.iter_mut() {
        *s = None;
    }
    node.ms_initialized = false;
}

pub fn exec_rescan_merge_append_chg<'mcx>(node: &mut MergeAppendState<'mcx>, chg: &Bitmapset<'mcx>) {
    if let Some(ps) = node.ms_prune_state.as_ref() {
        if chg.overlap(&ps.execparamids) {
            node.ms_valid_subplans_identified = false;
            node.ms_valid_subplans = Bitmapset::empty();
        }
    }
    exec_rescan_merge_append(node);
}

const _: () = assert!(!core::mem::needs_drop::<SortSupport>());

// Exempt: ms_prune_state is a droppy owner, released by exec_end_merge_append;
// ms_sortkeys holds drop-free SortSupport (assert above).
mcx::forget_safe_struct!(
    MergeAppendState<'_> { plan, ms_nplans, ms_slots, ms_heap,
        ms_initialized, ms_valid_subplans_identified, ms_valid_subplans;
        ms_prune_state, ms_sortkeys },
);
