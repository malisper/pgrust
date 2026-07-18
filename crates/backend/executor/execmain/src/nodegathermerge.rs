// nodeGatherMerge.c with nodemergeappend's specialized binaryheap over
// participant indexes (0 = leader, 1..=n = workers).

use ::executils::{EStateData, ExecSlotId};
use ::tuplesort::{
    apply_sort_comparator_in, prepare_sort_support_from_ordering_op, SortSupport, SortSupportInit,
};
use ::types_error::PgResult;
use ::types_nodes::plannodes::GatherMerge;
use ::types_slot::TupleSlotKind;
use ::types_tuple::MinimalTupleData;

use crate::execparallel::{
    self, exec_init_parallel_plan, exec_parallel_cleanup, exec_parallel_create_readers,
    exec_parallel_finish, exec_parallel_reinitialize, ParallelExecutorInfo,
};
use crate::nodegather::{leader_participation, wait_on_my_latch, WAIT_EVENT_EXECUTE_GATHER};
use crate::procnode::{exec_proc_node, with_eval_slots, PlanStateBase, PlanStateNode};

const MAX_TUPLE_STORE: usize = 10;

// Retained 8-aligned tuple copy (heap_copy_minimal_tuple with capacity reuse).
#[derive(Default)]
struct TupleBuf {
    words: Vec<u64>,
    len: usize,
}

impl TupleBuf {
    fn store(&mut self, bytes: &[u8]) {
        let nwords = bytes.len().div_ceil(8);
        if self.words.len() < nwords {
            self.words.resize(nwords, 0);
        }
        // SAFETY: destination holds nwords*8 >= bytes.len() writable bytes.
        unsafe {
            core::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.words.as_mut_ptr().cast::<u8>(),
                bytes.len(),
            );
        }
        self.len = bytes.len();
    }

    fn tuple_ptr(&self) -> core::ptr::NonNull<MinimalTupleData> {
        debug_assert!(self.len > 0);
        core::ptr::NonNull::new(self.words.as_ptr().cast_mut())
            .expect("stored tuple buffer is non-null")
            .cast()
    }
}

// GMReaderTupleBuffer (nodeGatherMerge.c).
#[derive(Default)]
struct GmTupleBuffer {
    tuple: Vec<TupleBuf>,
    // The slot's current tuple lives outside `tuple` so refills never
    // overwrite it while stored.
    cur: TupleBuf,
    ntuples: usize,
    read_counter: usize,
    done: bool,
}

pub struct GatherMergeState<'mcx> {
    pub plan: &'mcx GatherMerge<'mcx>,
    pub ps: PlanStateBase<'mcx>,
    pub initialized: bool,
    pub gm_initialized: bool,
    pub need_to_scan_locally: bool,
    pub tuples_needed: i64,
    pub pei: Option<ParallelExecutorInfo>,
    pub nworkers_launched: i32,
    pub nreaders: usize,
    pub reader: Vec<tqueue::TupleQueueReader>,
    gm_nkeys: usize,
    gm_sortkeys: mcx::PgVec<'mcx, SortSupport>,
    // gm_slots[0] is the leader's latest child slot; 1..=num_workers are
    // per-worker minimal-tuple slots.
    gm_slots: mcx::PgVec<'mcx, Option<ExecSlotId>>,
    worker_slots: mcx::PgVec<'mcx, ExecSlotId>,
    gm_heap: mcx::PgVec<'mcx, i32>,
    tuple_buffers: Vec<GmTupleBuffer>,
    // C outerPlan->chgParam: deferred-rescan set from ExecReScanGatherMerge;
    // consumed at the leader's next local pull, after ExecParallelReinitialize.
    pub outer_chg: ::types_nodes::bitmapset::Bitmapset<'mcx>,
    // WS-O (wave 2): the gang's admission-ledger width lease (see
    // nodegather::lease_gather_width; None on every fail-open path).
    width_lease: Option<runtime::ParallelWidthLease>,
}

/// `ExecInitGatherMerge` + `gather_merge_setup` (nodeGatherMerge.c).
pub fn exec_init_gather_merge<'mcx>(
    node: &'mcx GatherMerge<'mcx>,
    estate: &mut EStateData<'mcx>,
    outer: &PlanStateNode<'mcx>,
) -> PgResult<GatherMergeState<'mcx>> {
    debug_assert!(node.plan.righttree.is_none());
    debug_assert!(node.plan.qual.is_nil());
    let mcx = estate.es_query_cxt;
    let ecxt = estate.exec_assign_expr_context();

    let outer_plan =
        node.plan.lefttree.expect("GatherMerge without an outer plan").as_plan().unwrap();
    let tup_desc = outer.exec_get_result_type(outer_plan)?;

    let proj = ::execscan::exec_conditional_assign_projection_info(
        mcx,
        estate,
        &node.plan.targetlist,
        ::types_nodes::primnodes::OUTER_VAR as u32,
        &tup_desc,
    )?;
    // C ExecInitGather builds the result type from the Gather tlist
    // (ExecInitResultTypeTL) even without a projection: only the top plan's
    // tlist carries the labeled resnames RowDescription needs; the outer
    // scan's descriptor is nameless.
    let result_desc = crate::exec_type_from_tl(&node.plan.targetlist)?;
    let (result_slot, proj_state) = match proj {
        Some(p) => (Some(p.pi_result_slot), Some(p.pi_state)),
        None => (None, None),
    };

    let mut gm_sortkeys = mcx::vec_with_capacity_in(mcx, node.numCols as usize)?;
    for i in 0..node.numCols as usize {
        let init = SortSupportInit {
            ssup_collation: node.collations[i],
            ssup_nulls_first: node.nullsFirst[i],
            ssup_attno: node.sortColIdx[i],
        };
        // abbreviate = false, as MergeAppend.
        gm_sortkeys.push(prepare_sort_support_from_ordering_op(node.sortOperators[i], &init)?);
    }

    let nreaders = node.num_workers.max(0) as usize;
    let mut gm_slots = mcx::vec_with_capacity_in(mcx, nreaders + 1)?;
    gm_slots.push(None);
    let mut worker_slots = mcx::vec_with_capacity_in(mcx, nreaders)?;
    let mut tuple_buffers = Vec::with_capacity(nreaders);
    for _ in 0..nreaders {
        let slot = estate
            .exec_init_extra_tuple_slot(Some(tup_desc.clone()), TupleSlotKind::MinimalTuple);
        worker_slots.push(slot);
        gm_slots.push(None);
        let mut buf = GmTupleBuffer::default();
        buf.tuple.resize_with(MAX_TUPLE_STORE, TupleBuf::default);
        tuple_buffers.push(buf);
    }

    let gm_heap = mcx::vec_with_capacity_in(mcx, nreaders + 1)?;

    Ok(GatherMergeState {
        plan: node,
        ps: PlanStateBase {
            plan: &node.plan,
            ps_ExprContext: Some(ecxt),
            ps_ResultTupleDesc: Some(result_desc),
            ps_ResultTupleSlot: result_slot,
            ps_ProjInfo: proj_state,
            qual: None,
        },
        initialized: false,
        gm_initialized: false,
        need_to_scan_locally: false,
        tuples_needed: -1,
        pei: None,
        nworkers_launched: 0,
        nreaders: 0,
        reader: Vec::new(),
        gm_nkeys: node.numCols as usize,
        gm_sortkeys,
        gm_slots,
        worker_slots,
        gm_heap,
        tuple_buffers,
        outer_chg: ::types_nodes::bitmapset::Bitmapset::empty(),
        width_lease: None,
    })
}

/// `ExecGatherMerge` (nodeGatherMerge.c).
pub fn exec_gather_merge<'mcx>(
    node: &mut GatherMergeState<'mcx>,
    outer: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    crate::cfi()?;

    gather_merge_ensure_launched(node, outer, estate)?;

    let ecxt = node.ps.ps_ExprContext.expect("GatherMergeState without ExprContext");
    estate.reset_expr_context(ecxt);

    let Some(slot) = gather_merge_getnext(node, outer, estate)? else {
        return Ok(None);
    };
    if node.ps.ps_ProjInfo.is_none() {
        return Ok(Some(slot));
    }
    estate.ecxt_mut(ecxt).ecxt_outertuple = Some(slot);
    let result_slot = node.ps.ps_ResultTupleSlot.expect("projection without result slot");
    let proj = node.ps.ps_ProjInfo.as_deref_mut().unwrap();
    with_eval_slots(estate, ecxt, Some(result_slot), |slots, result, mcx| {
        ::execexpr::exec_project(proj, slots, result.unwrap(), mcx)
    })?;
    Ok(Some(result_slot))
}

/// The one-time worker launch half of `ExecGatherMerge` (the `!initialized`
/// arm), extracted so the lane-v2 parallel-DISTINCT drive can launch the
/// workers BEFORE it runs the leader's own partial build (lanev2
/// `try_own_*_agg_over_gather_merge`; the drive then clears
/// `need_to_scan_locally` and reads stray rows through the normal pulls).
pub(crate) fn gather_merge_ensure_launched<'mcx>(
    node: &mut GatherMergeState<'mcx>,
    outer: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if node.initialized {
        return Ok(());
    }
    let gm = node.plan;
    if gm.num_workers > 0 && estate.es_use_parallel_mode {
        match node.pei.as_mut() {
            None => {
                node.pei = Some(exec_init_parallel_plan(
                    gm.plan.lefttree.expect("GatherMerge without an outer plan"),
                    outer,
                    estate,
                    &gm.initParam,
                    gm.num_workers,
                    node.tuples_needed,
                )?)
            }
            Some(pei) => exec_parallel_reinitialize(outer, estate, pei, &gm.initParam)?,
        }
        let pei = node.pei.as_mut().expect("just initialized");
        // WS-O width lease (default-OFF knob; fail-open paths leave the
        // launch untouched) — the Gather seam, shared.
        debug_assert!(node.width_lease.is_none(), "lease survived a shutdown");
        let (lease, face) =
            crate::nodegather::lease_gather_width(pei.pcxt, gm.num_workers);
        node.width_lease = lease;
        parallel::LaunchParallelWorkers(pei.pcxt)?;
        node.nworkers_launched = parallel::nworkers_launched(pei.pcxt);
        crate::nodegather::settle_gather_width(&mut node.width_lease, node.nworkers_launched);
        crate::nodegather::mirror_gather_width(
            face,
            gm.num_workers,
            &node.width_lease,
            node.nworkers_launched,
        );
        execparallel::account_workers(estate, pei.pcxt);

        if node.nworkers_launched > 0 {
            exec_parallel_create_readers(pei);
            node.reader = core::mem::take(&mut pei.reader);
        } else {
            node.reader = Vec::new();
        }
        node.nreaders = node.reader.len();
    }
    if leader_participation() || node.nreaders == 0 {
        node.need_to_scan_locally = true;
    }
    node.initialized = true;
    Ok(())
}

/// `gather_merge_init` (nodeGatherMerge.c).
fn gather_merge_init<'mcx>(
    node: &mut GatherMergeState<'mcx>,
    outer: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let nreaders = node.nreaders;
    debug_assert!(nreaders <= node.plan.num_workers.max(0) as usize);
    let mut nowait = true;

    node.gm_slots[0] = None;
    for i in 0..nreaders {
        node.tuple_buffers[i].ntuples = 0;
        node.tuple_buffers[i].read_counter = 0;
        node.tuple_buffers[i].done = false;
        let mcx = estate.es_query_cxt;
        ::exectuples::exec_clear_tuple(estate.slot_mut(node.worker_slots[i]), mcx);
        node.gm_slots[i + 1] = None;
    }
    node.gm_heap.clear();

    'reread: loop {
        for i in 0..=nreaders {
            crate::cfi()?;
            let known_done = if i == 0 {
                !node.need_to_scan_locally
            } else {
                node.tuple_buffers[i - 1].done && node.gm_slots[i].is_none()
            };
            if known_done {
                continue;
            }
            if node.gm_slots[i].is_none() {
                if gather_merge_readnext(node, outer, estate, i, nowait)? {
                    node.gm_heap.push(i as i32);
                }
            } else if i > 0 {
                load_tuple_array(node, i);
            }
        }
        for i in 1..=nreaders {
            if !node.tuple_buffers[i - 1].done && node.gm_slots[i].is_none() {
                nowait = false;
                continue 'reread;
            }
        }
        break;
    }

    binaryheap_build(node, estate);
    node.gm_initialized = true;
    Ok(())
}

/// `gather_merge_clear_tuples` (nodeGatherMerge.c).
fn gather_merge_clear_tuples<'mcx>(
    node: &mut GatherMergeState<'mcx>,
    estate: &mut EStateData<'mcx>,
) {
    for i in 0..node.nreaders {
        let b = &mut node.tuple_buffers[i];
        b.ntuples = 0;
        b.read_counter = 0;
        let mcx = estate.es_query_cxt;
        ::exectuples::exec_clear_tuple(estate.slot_mut(node.worker_slots[i]), mcx);
        node.gm_slots[i + 1] = None;
    }
}

/// `gather_merge_getnext` (nodeGatherMerge.c).
fn gather_merge_getnext<'mcx>(
    node: &mut GatherMergeState<'mcx>,
    outer: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    if !node.gm_initialized {
        gather_merge_init(node, outer, estate)?;
    } else {
        let i = node.gm_heap[0];
        if gather_merge_readnext(node, outer, estate, i as usize, false)? {
            sift_down(node, 0, estate);
        } else {
            binaryheap_remove_first(node, estate);
        }
    }

    match node.gm_heap.first() {
        None => {
            gather_merge_clear_tuples(node, estate);
            Ok(None)
        }
        Some(&i) => Ok(node.gm_slots[i as usize]),
    }
}

/// `load_tuple_array` (nodeGatherMerge.c). Nowait prefetch errors surface on
/// the next demanded read (C reports immediately; the retry hits the same
/// error).
fn load_tuple_array(node: &mut GatherMergeState<'_>, reader: usize) {
    if reader == 0 {
        return;
    }
    let b = reader - 1;
    let readers = &mut node.reader;
    let buf = &mut node.tuple_buffers[b];
    if buf.ntuples == buf.read_counter {
        buf.ntuples = 0;
        buf.read_counter = 0;
    }
    for i in buf.ntuples..MAX_TUPLE_STORE {
        let mut done = buf.done;
        let got = match readers[b].next(true, &mut done) {
            Ok(Some(bytes)) => {
                buf.tuple[i].store(bytes);
                true
            }
            Ok(None) | Err(_) => false,
        };
        buf.done = done;
        if !got {
            break;
        }
        buf.ntuples += 1;
    }
}

/// `gather_merge_readnext` + `gm_readnext_tuple` (nodeGatherMerge.c).
fn gather_merge_readnext<'mcx>(
    node: &mut GatherMergeState<'mcx>,
    outer: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
    reader: usize,
    nowait: bool,
) -> PgResult<bool> {
    if reader == 0 {
        if node.need_to_scan_locally {
            crate::nodegather::apply_pending_outer_chg(
                &mut node.outer_chg,
                outer,
                node.plan.plan.lefttree.expect("GatherMerge without an outer plan"),
                estate,
            )?;
            if let Some(id) = exec_proc_node(outer, estate)? {
                node.gm_slots[0] = Some(id);
                return Ok(true);
            }
            node.need_to_scan_locally = false;
        }
        node.gm_slots[0] = None;
        return Ok(false);
    }

    let b = reader - 1;
    let have_buffered = node.tuple_buffers[b].ntuples > node.tuple_buffers[b].read_counter;
    if have_buffered {
        let buf = &mut node.tuple_buffers[b];
        let rc = buf.read_counter;
        buf.read_counter += 1;
        let GmTupleBuffer { cur, tuple, .. } = buf;
        core::mem::swap(cur, &mut tuple[rc]);
    } else if node.tuple_buffers[b].done {
        node.gm_slots[reader] = None;
        return Ok(false);
    } else {
        crate::cfi()?;
        let mut done = false;
        let got = {
            let readers = &mut node.reader;
            let buf = &mut node.tuple_buffers[b];
            match readers[b].next(nowait, &mut done)? {
                Some(bytes) => {
                    buf.cur.store(bytes);
                    true
                }
                None => false,
            }
        };
        node.tuple_buffers[b].done = done;
        if !got {
            node.gm_slots[reader] = None;
            return Ok(false);
        }
        load_tuple_array(node, reader);
    }

    let ptr = node.tuple_buffers[reader - 1].cur.tuple_ptr();
    let mcx = estate.es_query_cxt;
    let slot_id = node.worker_slots[reader - 1];
    // SAFETY: 8-aligned owned copy, untouched until this reader's next
    // serve replaces it.
    unsafe { ::exectuples::exec_store_minimal_tuple_ptr(estate.slot_mut(slot_id), mcx, ptr) };
    node.gm_slots[reader] = Some(slot_id);
    Ok(true)
}

// heap_compare_slots (nodeGatherMerge.c).
fn heap_compare_slots<'mcx>(
    node: &GatherMergeState<'mcx>,
    estate: &mut EStateData<'mcx>,
    a: i32,
    b: i32,
) -> i32 {
    let mcx = estate.es_query_cxt;
    let id1 = node.gm_slots[a as usize].expect("compared participant slot is empty");
    let id2 = node.gm_slots[b as usize].expect("compared participant slot is empty");
    let table = &mut estate.es_tupleTable[..];
    let [s1, s2] = table
        .get_disjoint_mut([id1.0 as usize, id2.0 as usize])
        .expect("distinct in-range participant slot ids");
    for key in node.gm_sortkeys.iter().take(node.gm_nkeys) {
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

fn binaryheap_build<'mcx>(node: &mut GatherMergeState<'mcx>, estate: &mut EStateData<'mcx>) {
    let n = node.gm_heap.len() as i32;
    if n <= 1 {
        return;
    }
    for i in (0..=(n - 2) / 2).rev() {
        sift_down(node, i, estate);
    }
}

fn binaryheap_remove_first<'mcx>(
    node: &mut GatherMergeState<'mcx>,
    estate: &mut EStateData<'mcx>,
) {
    let last = node.gm_heap.pop().expect("binaryheap_remove_first on empty heap");
    if !node.gm_heap.is_empty() {
        node.gm_heap[0] = last;
        sift_down(node, 0, estate);
    }
}

fn sift_down<'mcx>(
    node: &mut GatherMergeState<'mcx>,
    mut node_off: i32,
    estate: &mut EStateData<'mcx>,
) {
    let size = node.gm_heap.len() as i32;
    let node_val = node.gm_heap[node_off as usize];
    loop {
        let left_off = 2 * node_off + 1;
        let right_off = 2 * node_off + 2;
        let mut swap_off = left_off;
        if right_off < size {
            let l = node.gm_heap[left_off as usize];
            let r = node.gm_heap[right_off as usize];
            if heap_compare_slots(node, estate, l, r) < 0 {
                swap_off = right_off;
            }
        }
        if left_off >= size {
            break;
        }
        let swap_val = node.gm_heap[swap_off as usize];
        if heap_compare_slots(node, estate, node_val, swap_val) >= 0 {
            break;
        }
        node.gm_heap[node_off as usize] = swap_val;
        node_off = swap_off;
    }
    node.gm_heap[node_off as usize] = node_val;
}

impl GatherMergeState<'_> {
    pub(crate) fn tuple_buffers_release(&mut self) {
        self.tuple_buffers = Vec::new();
    }
}

/// `ExecShutdownGatherMergeWorkers` (nodeGatherMerge.c).
pub fn exec_shutdown_gather_merge_workers(node: &mut GatherMergeState<'_>) -> PgResult<()> {
    node.reader = Vec::new();
    node.nreaders = 0;
    if let Some(pei) = node.pei.as_mut() {
        exec_parallel_finish(pei)?;
    }
    // WS-O: gang done — retire the width lease (drop returns the width).
    node.width_lease = None;
    Ok(())
}

/// `ExecShutdownGatherMerge` (nodeGatherMerge.c).
pub fn exec_shutdown_gather_merge(
    node: &mut GatherMergeState<'_>,
    estate: &mut EStateData<'_>,
) -> PgResult<()> {
    exec_shutdown_gather_merge_workers(node)?;
    if let Some(mut pei) = node.pei.take() {
        exec_parallel_cleanup(estate, &mut pei)?;
    }
    Ok(())
}

/// `ExecReScanGatherMerge` (nodeGatherMerge.c), before the child rescan.
pub(crate) fn exec_rescan_gather_merge_pre<'mcx>(
    node: &mut GatherMergeState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    exec_shutdown_gather_merge_workers(node)?;
    gather_merge_clear_tuples(node, estate);
    node.gm_heap.clear();
    node.initialized = false;
    node.gm_initialized = false;
    node.need_to_scan_locally = false;
    Ok(())
}

/// `ExecReScanGatherMerge` (nodeGatherMerge.c). With a rescan_param the child
/// rescan is deferred (chgParam): parallel-aware children must see
/// ReInitializeDSM before their ReScan.
pub fn exec_rescan_gather_merge<'mcx>(
    node: &mut GatherMergeState<'mcx>,
    outer: &mut PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    exec_rescan_gather_merge_pre(node, estate)?;
    if node.plan.rescan_param >= 0 {
        let mcx = estate.es_query_cxt;
        node.outer_chg.add_member(mcx, node.plan.rescan_param)?;
    }
    if node.outer_chg.is_empty() {
        return crate::execami::exec_re_scan(outer, estate);
    }
    Ok(())
}

const _: () = assert!(!core::mem::needs_drop::<SortSupport>());

// pei/reader/tuple_buffers/width_lease are droppy owners, released by
// ExecShutdownGatherMerge and release_owned.
::mcx::forget_safe_struct!(
    GatherMergeState<'_> { plan, ps, initialized, gm_initialized,
        need_to_scan_locally, tuples_needed, nworkers_launched, nreaders,
        gm_nkeys, gm_slots, worker_slots, gm_heap, outer_chg;
        pei, reader, gm_sortkeys, tuple_buffers, width_lease },
);
