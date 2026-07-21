// nodeSort.c. The outer child stays with the ExecProcNode dispatcher: the
// feed loop takes a monomorphized fetch closure (C's ExecProcNode indirect
// call), keeping this crate out of a cycle with the node-enum owner.
#![allow(non_snake_case)]

use std::rc::Rc;

use ::datum::Datum;
use ::executils::{EStateData, ExecSlotId};
use ::tuplesort::{Tuplesort, TUPLESORT_ALLOWBOUNDED, TUPLESORT_NONE, TUPLESORT_RANDOMACCESS};
use ::types_error::PgResult;
use ::types_nodes::plannodes::Sort;
use ::types_scan::sdir::ScanDirectionIsForward;
use ::types_slot::{TupleSlotKind, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, EXEC_FLAG_REWIND};
use ::types_tuple::TupleDescData;

pub fn init_seams() {}

// C's CHECK_FOR_INTERRUPTS at ExecSort entry.
#[inline(always)]
fn cfi() -> PgResult<()> {
    if init_small::globals::InterruptPending() {
        return postgres_seams::check_for_interrupts::call();
    }
    Ok(())
}

#[cfg(test)]
mod tests;

// M3 top-N sink kernels (docs/design/m3-sort.md §3): the POD bounded
// (key, rowref) heap on the rule-2 total order + the winner merge. Pure
// data structures — the runtime SealedParallelSink impl over them lives at
// the engagement seam (execmain lanev2/runtime_sort.rs, inc-2).
pub mod fullsort;
pub mod mjmerge;
pub mod sink;

pub struct SortState<'mcx> {
    pub plan: &'mcx Sort<'mcx>,
    pub ps_ResultTupleDesc: Option<Rc<TupleDescData<'static>>>,
    pub ps_ResultTupleSlot: ExecSlotId,
    pub randomAccess: bool,
    pub bounded: bool,
    pub bound: i64,
    sort_Done: bool,
    bounded_Done: bool,
    bound_Done: i64,
    datumSort: bool,
    tuplesortstate: Option<Tuplesort>,
    // Lane refsort (late-materialization top-N) state: when `refsort` is
    // set, `tuplesortstate` holds the NARROW synthetic (key, ref) sort and
    // must never be read back as node output — the emit face serves the
    // gathered winners from `refsort_out` instead (sorted order, <= bound
    // rows). Cleared with the tuplesort on every reset/rescan/end path.
    refsort: bool,
    refsort_out: std::collections::VecDeque<::heaptuple::MinimalTuple<'mcx>>,
    // Sticky per-node refsort refusal (a demoted feed never re-arms).
    refsort_refused: bool,
    // Memoized synthetic 2-col (key, ref) desc — one build per node, reused
    // across rescan re-feeds.
    refsort_desc: Option<Rc<TupleDescData<'static>>>,
    // Runtime FULL-SORT adoption (m3-sort-b shape b): the sealed runs +
    // partition outputs published by the runtime sink. When set, the emit
    // face serves rows straight out of the run buffers in partition order
    // (the canonical (keys, rowref) total order) — no tuplesort exists.
    // Cleared with the refsort state on every reset/rescan/end path.
    runtime_full: Option<Box<fullsort::FullAdopted>>,
    // WS-AD wave-8 (sort randomAccess admission): memoized BARE-hook
    // verdict for randomAccess sorts under PGRUST_LANE_V2_SORT_RANDOMACCESS.
    // The chain-shared `lane_fusible` memo (SortNode) keeps refusing
    // randomAccess for every chain host this increment; only
    // `lanev2::try_own_sort` consults/stores this. Init-stable like the
    // chain memo (same child refuse-sets), so never cleared.
    lane_ra_fusible: Option<bool>,
}

/// `ExecInitSort` minus child linkage: the caller (execProcnode's T_Sort arm)
/// inits the outer child with `sort_child_eflags` and passes its result type.
pub fn exec_init_sort<'mcx>(
    node: &'mcx Sort<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
    outer_desc: &Rc<TupleDescData<'static>>,
    result_desc: Rc<TupleDescData<'static>>,
) -> PgResult<SortState<'mcx>> {
    let randomAccess =
        eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) != 0;
    let ps_ResultTupleSlot = estate
        .exec_init_extra_tuple_slot(Some(result_desc.clone()), TupleSlotKind::MinimalTuple);
    Ok(SortState {
        plan: node,
        ps_ResultTupleDesc: Some(result_desc),
        ps_ResultTupleSlot,
        randomAccess,
        bounded: false,
        bound: 0,
        sort_Done: false,
        bounded_Done: false,
        bound_Done: 0,
        datumSort: outer_desc.natts == 1,
        tuplesortstate: None,
        refsort: false,
        refsort_out: std::collections::VecDeque::new(),
        refsort_refused: false,
        refsort_desc: None,
        runtime_full: None,
        lane_ra_fusible: None,
    })
}

/// C shields the child from REWIND/BACKWARD/MARK.
pub fn sort_child_eflags(eflags: i32) -> i32 {
    eflags & !(EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)
}

impl SortState<'_> {
    pub fn sort_done(&self) -> bool {
        self.sort_Done
    }

    /// Never pulled, fed, or adopted — the whole node state is pristine
    /// (the MJSORT probe-once law: an engagement bypassing this node may
    /// proceed only while a later fallback would still drive a virgin
    /// Volcano tree byte-identically).
    pub fn sort_virgin(&self) -> bool {
        !self.sort_Done
            && self.tuplesortstate.is_none()
            && self.runtime_full.is_none()
            && !self.refsort
            && self.refsort_out.is_empty()
    }
}


/// `ExecSort`: sort the subplan on first fetch, then feed from tuplesort.
/// Forward-only (backward-execution wave B6): C nodeSort.c's direction-aware
/// drain (`ScanDirectionIsForward(dir)` into tuplesort_gettupleslot/
/// getdatum) and its feed-time es_direction save/pin/restore dance exist so
/// a backward pull can read the finished sort backwards; the run seam
/// refuses backward entry (deletion-prep B1), so the drain reads forward
/// unconditionally. C RETAINS backward sort reads; ratified strategy
/// divergence (Michael's 2026-07-17 SCROLL/WITH-HOLD decision).
pub fn exec_sort<'mcx, F>(
    node: &mut SortState<'mcx>,
    estate: &mut EStateData<'mcx>,
    outer_desc: Rc<TupleDescData<'static>>,
    mut fetch_outer: F,
) -> PgResult<Option<ExecSlotId>>
where
    F: FnMut(&mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>,
{
    cfi()?;
    debug_assert!(
        ScanDirectionIsForward(estate.es_direction),
        "backward drive below the forward-only run seam (deletion-prep B1)"
    );
    let mcx = estate.es_query_cxt;
    debug_assert!(node.datumSort == (outer_desc.natts == 1));

    if !node.sort_Done {
        let mut tuplesortopts = TUPLESORT_NONE;
        if node.randomAccess {
            tuplesortopts |= TUPLESORT_RANDOMACCESS;
        }
        if node.bounded {
            tuplesortopts |= TUPLESORT_ALLOWBOUNDED;
        }
        let work_mem = init_small::globals::work_mem();
        let mut ts = if node.datumSort {
            Tuplesort::begin_datum(
                outer_desc.attr(0).atttypid,
                node.plan.sortOperators[0],
                node.plan.collations[0],
                node.plan.nullsFirst[0],
                work_mem,
                tuplesortopts,
            )?
        } else {
            Tuplesort::begin_heap(
                outer_desc,
                node.plan.sortColIdx,
                node.plan.sortOperators,
                node.plan.collations,
                node.plan.nullsFirst,
                work_mem,
                tuplesortopts,
            )?
        };
        if node.bounded {
            ts.set_bound(node.bound);
        }

        if node.datumSort {
            if ts.datum_sort_is_byref() {
                // By-ref datums must go through the datumCopy arm — the batch
                // putter parks raw slot pointers the next fetch recycles.
                while let Some(id) = fetch_outer(estate)? {
                    let slot = estate.slot_mut(id);
                    exectuples::slot_getsomeattrs(slot, 1);
                    let base = slot.base();
                    ts.putdatum(base.tts_values[0], base.tts_isnull[0])?;
                }
            } else {
                ts.putdatum_batch(|p| {
                    while let Some(id) = fetch_outer(estate)? {
                        let slot = estate.slot_mut(id);
                        exectuples::slot_getsomeattrs(slot, 1);
                        let base = slot.base();
                        p.put(base.tts_values[0], base.tts_isnull[0])?;
                    }
                    Ok(())
                })?;
            }
        } else {
            while let Some(id) = fetch_outer(estate)? {
                ts.puttupleslot(estate.slot_mut(id), mcx)?;
            }
        }

        ts.performsort()?;

        let id = node.plan.plan.plan_node_id;
        let stats = ts.get_stats();
        match estate.es_sort_instrumentation.iter_mut().find(|(i, _)| *i == id) {
            Some((_, s)) => *s = stats,
            None => estate.es_sort_instrumentation.push((id, stats)),
        }

        node.sort_Done = true;
        node.bounded_Done = node.bounded;
        node.bound_Done = node.bound;
        node.tuplesortstate = Some(ts);
    }

    // Lane refsort fallback drain (the fused batched-feed twin arm was
    // deleted with fused arm #5, se/deletion-prep C1).
    if node.refsort {
        return Ok(refsort_pop(node, estate));
    }
    let ts = node.tuplesortstate.as_mut().expect("sort_Done without tuplesortstate");
    let slot_id = node.ps_ResultTupleSlot;
    let slot = estate.slot_mut(slot_id);
    let got = if node.datumSort {
        exectuples::exec_clear_tuple(slot, mcx);
        match ts.getdatum(true)? {
            Some(nd) => {
                let base = slot.base_mut();
                base.tts_values[0] = if nd.isnull { Datum::null() } else { nd.value };
                base.tts_isnull[0] = nd.isnull;
                exectuples::exec_store_virtual_tuple(slot);
                true
            }
            None => false,
        }
    } else {
        ts.gettupleslot(true, false, slot, mcx)?
    };
    Ok(if got { Some(slot_id) } else { None })
}

// ---------------------------------------------------------------------------
// Lane-executor-v2 sort-breaker seam (docs/design/lane-executor-v2.md §8:
// breakers delegate finalize/read-back to the row-path state). The breaker
// node lives in `execmain::lanev2`; these four legs give it `exec_sort`'s
// exact tuplesort drive — build / put / performsort / drain — over the SAME
// node state (`sort_Done` doubles as the breaker's Feed→Emit phase flag, and
// `exec_rescan_sort` resets it for free), so falling back to `exec_sort` at
// any call boundary is byte-safe and the output order is C's by construction.
// Each leg mirrors the corresponding `exec_sort` leg — keep them in lockstep.
// ---------------------------------------------------------------------------

/// Build leg: create the tuplesort exactly as `exec_sort` does (same options,
/// same work_mem, same begin_* arms, same bound). The caller owns the
/// `!sort_done()` check.
pub fn sort_lane_begin<'mcx>(
    node: &mut SortState<'mcx>,
    outer_desc: Rc<TupleDescData<'static>>,
) -> PgResult<()> {
    debug_assert!(!node.sort_Done && node.tuplesortstate.is_none());
    debug_assert!(node.datumSort == (outer_desc.natts == 1));
    let mut tuplesortopts = TUPLESORT_NONE;
    if node.randomAccess {
        tuplesortopts |= TUPLESORT_RANDOMACCESS;
    }
    if node.bounded {
        tuplesortopts |= TUPLESORT_ALLOWBOUNDED;
    }
    let work_mem = init_small::globals::work_mem();
    let mut ts = if node.datumSort {
        Tuplesort::begin_datum(
            outer_desc.attr(0).atttypid,
            node.plan.sortOperators[0],
            node.plan.collations[0],
            node.plan.nullsFirst[0],
            work_mem,
            tuplesortopts,
        )?
    } else {
        Tuplesort::begin_heap(
            outer_desc,
            node.plan.sortColIdx,
            node.plan.sortOperators,
            node.plan.collations,
            node.plan.nullsFirst,
            work_mem,
            tuplesortopts,
        )?
    };
    if node.bounded {
        ts.set_bound(node.bound);
    }
    node.tuplesortstate = Some(ts);
    Ok(())
}

// --- WS-AD wave-8: sort-breaker randomAccess admission seam --------------

/// Memoized bare-hook randomAccess verdict (`None` until the first
/// `sort_lane_ra_fusible_set`). See the field doc: the chain-shared memo
/// keeps refusing randomAccess; this side memo is the bare sort hook's
/// alone.
#[inline(always)]
pub fn sort_lane_ra_fusible(node: &SortState<'_>) -> Option<bool> {
    node.lane_ra_fusible
}

/// Store the bare-hook randomAccess verdict (once; init-stable inputs).
pub fn sort_lane_ra_fusible_set(node: &mut SortState<'_>, v: bool) {
    debug_assert!(node.lane_ra_fusible.is_none() || node.lane_ra_fusible == Some(v));
    node.lane_ra_fusible = Some(v);
}

/// Delegation probe (WS-AD acceptance ladder 2): true iff the node's
/// read-back face is the row-path `Tuplesort` itself — a finished sort
/// with NO lane-substituted emit face (refsort winner buffer / adopted
/// runtime output). randomAccess read-back (rescan replay via
/// `tuplesort_rescan`, mark/restore; backward pulls retired with the
/// backward-execution wave B6) is sound exactly when this holds, because
/// every one of those paths operates on `tuplesortstate` directly.
pub fn sort_lane_readback_delegated(node: &SortState<'_>) -> bool {
    node.sort_Done && node.tuplesortstate.is_some() && !node.refsort && node.runtime_full.is_none()
}

// --- end WS-AD wave-8 seam ------------------------------------------------

/// `sort_lane_begin` with the comparator NARROWED to the first `nkeys` sort
/// keys (the lane's grouped exact-DISTINCT order-relaxation arm: the dropped
/// suffix keys' only observable effect was intra-group row order, which the
/// caller has proven nothing downstream observes). The tuplesort still
/// stores whole input rows — only the compare narrows. Callers must have
/// refused `bounded` (a top-N bound over a narrowed comparator is a
/// different top-N) and `randomAccess` stays refused by the breaker gate.
pub fn sort_lane_begin_narrowed<'mcx>(
    node: &mut SortState<'mcx>,
    outer_desc: Rc<TupleDescData<'static>>,
    nkeys: usize,
) -> PgResult<()> {
    debug_assert!(!node.sort_Done && node.tuplesortstate.is_none());
    debug_assert!(!node.bounded && !node.randomAccess);
    debug_assert!(nkeys >= 1 && nkeys < node.plan.numCols as usize);
    debug_assert!(!node.datumSort, "narrowing implies >=2 sort keys => heap sort");
    let work_mem = init_small::globals::work_mem();
    let ts = Tuplesort::begin_heap(
        outer_desc,
        &node.plan.sortColIdx[..nkeys],
        &node.plan.sortOperators[..nkeys],
        &node.plan.collations[..nkeys],
        &node.plan.nullsFirst[..nkeys],
        work_mem,
        TUPLESORT_NONE,
    )?;
    node.tuplesortstate = Some(ts);
    Ok(())
}

// ---------------------------------------------------------------------------
// Lane refsort (late-materialization top-N; notes/latemat-lane.md Phase B
// conversion 1). The feed puts NARROW (key, ref) rows into a synthetic 2-col
// tuplesort built with the plan's leading-key comparator (same operator/
// collation/nullsFirst, same bounded discard, same put order => the winner
// SET and ORDER are byte-identical to the legacy wide feed); after
// performsort the caller gathers each winner's full row from the ref and
// buffers the projected outer tuples here, in sorted order. The emit face
// then serves `refsort_out` and never reads the narrow tuplesort as output.
// ---------------------------------------------------------------------------

/// Pack a pgrcolumnar row ref: (row group, rg-global row index) -> i64.
#[inline(always)]
pub fn refsort_encode(rg: u32, row: u32) -> i64 {
    (((rg as u64) << 32) | row as u64) as i64
}

/// Unpack a [`refsort_encode`]d ref.
#[inline(always)]
pub fn refsort_decode(r: i64) -> (u32, u32) {
    ((r as u64 >> 32) as u32, r as u32)
}

/// Sticky per-node refsort refusal (set on demote; a demoted node never
/// re-arms — the legacy feed owns it for the node's life).
#[inline]
pub fn sort_lane_refsort_refused(node: &SortState<'_>) -> bool {
    node.refsort_refused
}

pub fn sort_lane_refsort_refuse(node: &mut SortState<'_>) {
    node.refsort_refused = true;
}

/// Memoized synthetic (key, ref) desc for this node; `None` until the first
/// `sort_lane_begin_refsort` stored one.
pub fn sort_lane_refsort_key_desc(node: &SortState<'_>) -> Option<Rc<TupleDescData<'static>>> {
    node.refsort_desc.clone()
}

/// Build leg of the refsort feed: a bounded heap tuplesort over the caller's
/// synthetic 2-col desc (col 1 = the outer leading sort key's type, col 2 =
/// int8 ref), sorted on column 1 with the plan's key-0 operator/collation/
/// nullsFirst — the SAME comparator `sort_lane_begin` would install for the
/// leading key, over the same bound (ALLOWBOUNDED + set_bound, `exec_sort`'s
/// construction). Marks the node `refsort` so the emit face serves the
/// gathered-winner buffer.
///
/// `rule2` (lazytopn lane — the train-10 landing follow-up "extend the
/// narrow comparator with the ref column"): sort on BOTH columns — the ref
/// column (int8, ascending = physically-earliest-first) becomes the rule-2
/// tie-breaker, making the bounded selection the (key, rowref) TOTAL ORDER
/// of docs/conformance/tie-ordering.md. Selection and retained-tie emit
/// order are then byte-identical to the wide feed's `arm_topk_rowref` arm
/// by construction, with no tie machinery (a total order cannot tie).
pub fn sort_lane_begin_refsort<'mcx>(
    node: &mut SortState<'mcx>,
    key_desc: Rc<TupleDescData<'static>>,
    rule2: bool,
) -> PgResult<()> {
    debug_assert!(!node.sort_Done && node.tuplesortstate.is_none());
    debug_assert!(node.bounded && node.bound > 0 && !node.randomAccess);
    debug_assert!(!node.datumSort, "single-column output is already narrow");
    debug_assert!(key_desc.natts == 2);
    /// pg_operator int8 `<` OID (the execindexing validate_scan precedent).
    const INT8_LESS_OPERATOR: u32 = 412;
    let work_mem = init_small::globals::work_mem();
    let mut ts = if rule2 {
        Tuplesort::begin_heap(
            key_desc.clone(),
            &[1, 2],
            &[node.plan.sortOperators[0], INT8_LESS_OPERATOR],
            &[node.plan.collations[0], 0], // InvalidOid — int8 is non-collatable
            &[node.plan.nullsFirst[0], false],
            work_mem,
            TUPLESORT_ALLOWBOUNDED,
        )?
    } else {
        Tuplesort::begin_heap(
            key_desc.clone(),
            &[1],
            &node.plan.sortOperators[..1],
            &node.plan.collations[..1],
            &node.plan.nullsFirst[..1],
            work_mem,
            TUPLESORT_ALLOWBOUNDED,
        )?
    };
    ts.set_bound(node.bound);
    node.tuplesortstate = Some(ts);
    node.refsort = true;
    node.refsort_out.clear();
    node.refsort_desc = Some(key_desc);
    Ok(())
}

/// Feed leg: put one narrow (key, ref) row. One `puttuple_common` per row,
/// exactly the legacy feed's per-row put accounting.
#[inline]
pub fn sort_lane_put_refsort(
    node: &mut SortState<'_>,
    key: Datum,
    isnull: bool,
    refval: i64,
) -> PgResult<()> {
    debug_assert!(node.refsort);
    let ts =
        node.tuplesortstate.as_mut().expect("sort_lane_put_refsort before begin");
    ts.putvalues(&[key, Datum::from_i64(refval)], &[isnull, false])
}

/// Winner-ref read-back (after `sort_lane_finish`): the next narrow tuple's
/// decoded (rg, row) ref in sorted output order; `None` = drained.
pub fn sort_lane_refsort_next_ref(node: &mut SortState<'_>) -> PgResult<Option<(u32, u32)>> {
    debug_assert!(node.refsort && node.sort_Done);
    let ts =
        node.tuplesortstate.as_mut().expect("refsort ref read before finish");
    let mut values = [Datum::null(); 2];
    let mut isnull = [false; 2];
    if !ts.getvalues(true, &mut values, &mut isnull)? {
        return Ok(None);
    }
    debug_assert!(!isnull[1], "refsort ref column is never null");
    Ok(Some(refsort_decode(values[1].as_i64())))
}

/// Buffer one gathered winner (outer-format values/isnull, in sorted order):
/// forms an owned minimal tuple in `mcx` (the query context — outlives the
/// narrow tuplesort) under the node's result desc.
pub fn sort_lane_refsort_push_winner<'mcx>(
    node: &mut SortState<'mcx>,
    mcx: ::mcx::Mcx<'mcx>,
    values: &[Datum],
    isnull: &[bool],
) -> PgResult<()> {
    debug_assert!(node.refsort);
    let desc = node.ps_ResultTupleDesc.as_ref().expect("Sort already ended");
    let mtup = ::heaptuple::heap_form_minimal_tuple(mcx, desc, values, isnull, 0)?;
    node.refsort_out.push_back(mtup);
    Ok(())
}

/// Buffered winner count (trace/verification aid).
pub fn sort_lane_refsort_winners(node: &SortState<'_>) -> usize {
    node.refsort_out.len()
}

/// Runtime top-N adoption face, begin leg (m3-sort inc-2,
/// docs/design/m3-sort.md §4): the runtime sink merged the winner
/// (key, rowref) list off-node — mark the node refsort-served with NO
/// narrow tuplesort so the caller can buffer the gathered winners
/// (`sort_lane_refsort_push_winner`) and flip the emit face on
/// (`sort_lane_runtime_topn_done`). Same emit face
/// (`sort_lane_next` → `refsort_pop`) and the same reset/rescan/end
/// lifecycle as the serial refsort (every `refsort_clear` path covers
/// this state; `tuplesortstate` simply stays `None`).
pub fn sort_lane_runtime_topn_begin(node: &mut SortState<'_>) {
    debug_assert!(!node.sort_Done && node.tuplesortstate.is_none());
    debug_assert!(node.bounded && node.bound > 0 && !node.randomAccess);
    // Datum-shaped nodes (natts == 1) are served identically by the refsort
    // emit face: `refsort_pop` stores a 1-column minimal tuple into the
    // result slot, and the tuplesort datum feed/drain legs are never
    // reached (no tuplesort exists on this face). The runtime sink's own
    // admission decides which single-column shapes engage (today: only
    // specs with a DictCode key — the int-family single-column shapes keep
    // their census refusal, docs/design/dict-code-flow.md inc-1).
    node.refsort = true;
    node.refsort_out.clear();
}

/// Runtime top-N adoption face, finish leg: flip the breaker's Feed→Emit
/// phase after the winners are buffered — `sort_lane_finish`'s tail
/// without a tuplesort (no performsort exists; the EXPLAIN sort-stats
/// write is unreachable because the runtime arm refuses instrumented
/// runs).
pub fn sort_lane_runtime_topn_done(node: &mut SortState<'_>) {
    debug_assert!(node.refsort);
    node.sort_Done = true;
    node.bounded_Done = node.bounded;
    node.bound_Done = node.bound;
}

/// Runtime FULL-SORT adoption face (m3-sort-b shape b): install the
/// published runs + partition outputs and flip the node to Emit — no
/// tuplesort exists (the runtime result is the sort). Same reset/rescan/
/// end lifecycle as the refsort state (`refsort_clear` drops it; the
/// runtime arm refuses randomAccess/bounded shapes at admission).
pub fn sort_lane_runtime_full_adopt(
    node: &mut SortState<'_>,
    runs: Vec<std::sync::Arc<fullsort::FullRun>>,
    parts: Vec<Vec<(u16, u32)>>,
) {
    debug_assert!(!node.sort_Done && node.tuplesortstate.is_none());
    debug_assert!(!node.bounded && !node.randomAccess);
    debug_assert!(!node.datumSort, "runtime full sort refuses datum sorts");
    node.runtime_full = Some(Box::new(fullsort::FullAdopted::new(runs, parts)));
    node.sort_Done = true;
    node.bounded_Done = false;
    node.bound_Done = 0;
}

/// Adopted-row count (trace/verification aid).
pub fn sort_lane_runtime_full_rows(node: &SortState<'_>) -> usize {
    node.runtime_full.as_ref().map_or(0, |f| f.total_rows())
}

/// Runtime full-sort emit face: the next adopted row as a VIRTUAL tuple in
/// `ps_ResultTupleSlot` (datum copy; byref cells point into the adopted
/// run arenas). `None` = drained (clears the slot like the tuplesort drain
/// leg does).
fn runtime_full_pop<'mcx>(
    node: &mut SortState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> Option<ExecSlotId> {
    let mcx = estate.es_query_cxt;
    let slot_id = node.ps_ResultTupleSlot;
    let f = node.runtime_full.as_mut().expect("adopted full sort");
    match f.next_row() {
        Some((values, nulls)) => {
            let natts = values.len();
            let slot = estate.slot_mut(slot_id);
            exectuples::exec_clear_tuple(slot, mcx);
            {
                let sb = slot.base_mut();
                sb.tts_values[..natts].copy_from_slice(values);
                sb.tts_isnull[..natts].copy_from_slice(nulls);
            }
            exectuples::exec_store_virtual_tuple(slot);
            Some(slot_id)
        }
        None => {
            exectuples::exec_clear_tuple(estate.slot_mut(slot_id), mcx);
            None
        }
    }
}

/// Refsort emit face: pop the next gathered winner into `ps_ResultTupleSlot`
/// (owned store — the slot frees it on the next store/clear). `None` = EOF
/// (buffer drained; clears the slot like the tuplesort drain leg does).
fn refsort_pop<'mcx>(
    node: &mut SortState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> Option<ExecSlotId> {
    let mcx = estate.es_query_cxt;
    let slot_id = node.ps_ResultTupleSlot;
    match node.refsort_out.pop_front() {
        Some(mtup) => {
            exectuples::exec_store_minimal_tuple_owned(estate.slot_mut(slot_id), mcx, mtup);
            Some(slot_id)
        }
        None => {
            exectuples::exec_clear_tuple(estate.slot_mut(slot_id), mcx);
            None
        }
    }
}

/// Drop all refsort state (demote/reset/rescan/end): the narrow tuplesort is
/// the caller's to clear (`sort_lane_reset_for_refeed` / the rescan paths do
/// it alongside this).
fn refsort_clear(node: &mut SortState<'_>) {
    node.refsort = false;
    node.refsort_out.clear();
    node.runtime_full = None;
}

/// Feed leg (breaker `Sink::accept`): put one outer tuple. Datum sorts take
/// `putdatum` for BOTH by-ref and by-val keys: by-ref must copy (exactly as
/// `exec_sort`), and the by-val batch putter is a closure-scoped lever the
/// one-tuple-per-accept push feed cannot hold open — `putdatum`'s by-val arm
/// is the same `puttuple_common` call with identical accounting, so the sort
/// state and output are unchanged (a per-put len round-trip is the only
/// cost; re-batching it is a later perf lever).
pub fn sort_lane_put<'mcx>(
    node: &mut SortState<'mcx>,
    estate: &mut EStateData<'mcx>,
    id: ExecSlotId,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let ts = node.tuplesortstate.as_mut().expect("sort_lane_put before sort_lane_begin");
    if node.datumSort {
        let slot = estate.slot_mut(id);
        exectuples::slot_getsomeattrs(slot, 1);
        let base = slot.base();
        ts.putdatum(base.tts_values[0], base.tts_isnull[0])
    } else {
        ts.puttupleslot(estate.slot_mut(id), mcx)
    }
}

/// `sort_lane_put` over a caller-owned slot (not estate-registered): the
/// hash-grouped distinct arm's degrade dump feeds each group's stored
/// representative row through its own outer-format slot. Heap sorts only —
/// the narrowed sort is always a heap sort (>= 2 plan sort keys).
pub fn sort_lane_put_slot<'mcx>(
    node: &mut SortState<'mcx>,
    mcx: ::mcx::Mcx<'mcx>,
    slot: &mut ::types_slot::SlotData<'mcx>,
) -> PgResult<()> {
    debug_assert!(!node.datumSort);
    let ts = node.tuplesortstate.as_mut().expect("sort_lane_put_slot before sort_lane_begin");
    ts.puttupleslot(slot, mcx)
}

/// True when this sort's outer shape sorts bare datums (single-column
/// outer). Callers use it to gate the direct-key feed probe — the arming
/// mirror of the deleted fused `exec_sort_batched` feed, which probed
/// `key_direct` only inside its
/// `node.datumSort` arm.
#[inline(always)]
pub fn sort_lane_is_datum(node: &SortState<'_>) -> bool {
    node.datumSort
}

/// Per-row feed face for `sort_lane_put_batch` — the batch-positioned
/// analogue of the deleted `SortFeedSource`'s `emit`/`emit_key` pair (one face so both
/// legs share the caller's emit state).
pub trait SortLaneBatchFeed<'mcx> {
    /// Produce staged row `i`'s output slot; `None` = qual-filtered.
    fn emit(
        &mut self,
        i: u32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>>;
    /// Direct sort-key read for staged row `i` (only consulted when the
    /// caller armed `direct`); `None` = fallback, take the `emit` path.
    fn emit_key(&mut self, _i: u32) -> Option<(Datum, bool)> {
        None
    }
    /// Physical rowref of staged row `i` (rowref mode, tie-ordering rule 2:
    /// `(row_group << 32) | rg-global-row`); `None` = the feed carries no
    /// rowrefs (the default) and the put takes the bare path, which a
    /// rowref-armed tuplesort records as a contract break (the caller then
    /// demotes). Consulted on the heap-tuple put leg only.
    fn emit_rowref(&self, _i: u32) -> Option<u64> {
        None
    }
    /// Skip mask for staged positions: a bit-CLEARED position is one whose
    /// `emit` (and `emit_key`) yields nothing by the feed's contract — the
    /// batch put loop may skip it without calling either, which is
    /// put-stream-identical (same rows, same order, same puts). Bits at or
    /// past the staged row count are zero by the producer's contract.
    /// `None` (the default) = every position must be offered to `emit`.
    fn live_words(&self) -> Option<[u64; exectuples::SOA_BM_WORDS]> {
        None
    }
}

/// Iterate the put positions of `pos..n`, skipping bit-cleared positions
/// when the feed exposes a skip mask (`SortLaneBatchFeed::live_words`).
/// Word-granular: an all-clear word advances 64 positions in one compare —
/// the selective-qual sort feed's per-row emit ceremony (ExprContext reset,
/// CFI, bitmap re-test per row) collapses to one word test per 64 rows.
/// One shared implementation: `exectuples::for_each_live` (the wordskip
/// generalization of this qualed-sort-feed helper); the differential test below stays
/// the put-stream-identity gate at this seam.
#[inline(always)]
fn for_each_put(
    live: Option<&[u64; exectuples::SOA_BM_WORDS]>,
    pos: u32,
    n: u32,
    f: impl FnMut(u32) -> PgResult<()>,
) -> PgResult<()> {
    exectuples::for_each_live(live.map(|w| &w[..]), pos, n, f)
}

/// Batch-granular feed leg (breaker `BatchSink::accept_batch`): put every
/// row `emit` yields for staged positions `pos..n`. Row-for-row this is
/// `sort_lane_put` over the same emit stream in the same order — the
/// dispatch-granularity change only — with the per-put invariants hoisted
/// out of the loop, exactly as the deleted fused feed's arms hoisted them:
///   * the tuplesort handle is resolved once per batch, not per put;
///   * by-val datum sorts hold the batch putter open across the batch
///     (`putdatum_batch` — the same `puttuple_common` accounting as
///     `putdatum`, per-put len round-trip elided; `exec_sort` itself feeds
///     through it, so the sort state and output are unchanged);
///   * by-ref datum sorts keep `putdatum` (its datumCopy arm — the batch
///     putter parks raw slot pointers the next emit would recycle).
///
/// Direct key feed (the deleted fused feed's `key_direct`/`emit_key` arms,
/// verbatim): when `direct` is armed (datum sort, key served straight from
/// the leaf's staged column — value/null identical to `emit` +
/// `slot_getsomeattrs(1)`, no qual, same row order), rows `emit_key` covers
/// put straight from the staged column; `None` rows (narrow-tuple fallback)
/// take the existing full emit path in order.
pub fn sort_lane_put_batch<'mcx, F>(
    node: &mut SortState<'mcx>,
    estate: &mut EStateData<'mcx>,
    pos: u32,
    n: u32,
    direct: bool,
    feed: &mut F,
) -> PgResult<()>
where
    F: SortLaneBatchFeed<'mcx>,
{
    let mcx = estate.es_query_cxt;
    let ts = node.tuplesortstate.as_mut().expect("sort_lane_put_batch before sort_lane_begin");
    // Skip mask, fetched once per batch: cleared positions yield nothing
    // from `emit`/`emit_key` by the feed's contract, so skipping them puts
    // the identical stream (see `for_each_put`).
    let live = feed.live_words();
    if node.datumSort {
        if ts.datum_sort_is_byref() {
            for_each_put(live.as_ref(), pos, n, |i| {
                if direct {
                    if let Some((val, isnull)) = feed.emit_key(i) {
                        return ts.putdatum(val, isnull);
                    }
                }
                let Some(id) = feed.emit(i, estate)? else { return Ok(()) };
                let slot = estate.slot_mut(id);
                exectuples::slot_getsomeattrs(slot, 1);
                let base = slot.base();
                ts.putdatum(base.tts_values[0], base.tts_isnull[0])
            })?;
        } else {
            ts.putdatum_batch(|p| {
                for_each_put(live.as_ref(), pos, n, |i| {
                    if direct {
                        if let Some((val, isnull)) = feed.emit_key(i) {
                            return p.put(val, isnull);
                        }
                    }
                    let Some(id) = feed.emit(i, estate)? else { return Ok(()) };
                    let slot = estate.slot_mut(id);
                    exectuples::slot_getsomeattrs(slot, 1);
                    let base = slot.base();
                    p.put(base.tts_values[0], base.tts_isnull[0])
                })
            })?;
        }
    } else {
        for_each_put(live.as_ref(), pos, n, |i| {
            let Some(id) = feed.emit(i, estate)? else { return Ok(()) };
            match feed.emit_rowref(i) {
                Some(rr) => ts.puttupleslot_rowref(estate.slot_mut(id), mcx, rr),
                None => ts.puttupleslot(estate.slot_mut(id), mcx),
            }
        })?;
    }
    Ok(())
}

/// Streaming top-k cutoff boundary for the lane's sort-feed pre-filter: the
/// current k-th (worst surviving) tuple's leading-key datum while the
/// tuplesort's bounded heap is full; `None` before the heap fills or for
/// unbounded sorts. See `Tuplesort::topk_boundary` for the by-value-only
/// soundness contract.
#[inline]
pub fn sort_lane_topk_boundary(node: &SortState<'_>) -> Option<(Datum, bool)> {
    node.tuplesortstate.as_ref()?.topk_boundary()
}

/// Arm top-k boundary-tie tracking on the just-begun tuplesort (the lane's
/// zone-adaptive sort feed: arrival order is about to change, so tie
/// selection at the LIMIT cut must be provably arrival-insensitive or the
/// feed demotes). Call between `sort_lane_begin` and the first put.
pub fn sort_lane_topk_tie_track_arm(node: &mut SortState<'_>) {
    node.tuplesortstate
        .as_mut()
        .expect("tie track armed before sort_lane_begin")
        .arm_topk_tie_track();
}

/// Arm the top-k rowref total order (tie-ordering rule 2) on the just-begun
/// tuplesort: the bounded heap resolves full-key ties by physical rowref, so
/// survivor selection is the physical-order feed's by construction and the
/// zone-adaptive feed needs no tie-selection demotion (only a rowref
/// contract break demotes, via `sort_lane_topk_tie_ambiguity`). Heap sorts
/// only; call between `sort_lane_begin` and the first put.
pub fn sort_lane_topk_rowref_arm(node: &mut SortState<'_>) {
    debug_assert!(!node.datumSort);
    node.tuplesortstate
        .as_mut()
        .expect("rowref mode armed before sort_lane_begin")
        .arm_topk_rowref();
}

/// After `sort_lane_finish`, with tracking armed: could the selection or
/// order of the emitted top-N depend on feed arrival order, and which
/// trigger fired? (see `Tuplesort::topk_tie_ambiguity`). `None` when
/// tracking was never armed or no tie is arrival-sensitive.
pub fn sort_lane_topk_tie_ambiguity(
    node: &SortState<'_>,
) -> Option<::tuplesort::TopkTieAmbiguity> {
    node.tuplesortstate.as_ref().and_then(|ts| ts.topk_tie_ambiguity())
}

/// Demotion reset (the zone-adaptive feed observed an ambiguous boundary
/// tie): drop the finished tuplesort and clear the phase flag so the caller
/// can re-run `sort_lane_begin` + a physical-order re-feed. The node's
/// bounded/bound plan state is untouched — the re-begun sort is built
/// exactly as the first one was.
pub fn sort_lane_reset_for_refeed(node: &mut SortState<'_>) {
    node.tuplesortstate = None;
    node.sort_Done = false;
    node.bounded_Done = false;
    refsort_clear(node);
}

/// Finalize leg (breaker `Sink::finish`): `performsort` + the EXPLAIN sort
/// stats + the built flags — `exec_sort`'s build-leg tail verbatim. Flips
/// `sort_Done`, the breaker's Feed→Emit phase flag.
pub fn sort_lane_finish<'mcx>(
    node: &mut SortState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let ts =
        node.tuplesortstate.as_mut().expect("sort_lane_finish before sort_lane_begin");
    ts.performsort()?;

    let id = node.plan.plan.plan_node_id;
    let stats = ts.get_stats();
    match estate.es_sort_instrumentation.iter_mut().find(|(i, _)| *i == id) {
        Some((_, s)) => *s = stats,
        None => estate.es_sort_instrumentation.push((id, stats)),
    }

    node.sort_Done = true;
    node.bounded_Done = node.bounded;
    node.bound_Done = node.bound;
    Ok(())
}

/// Read-back leg (breaker `Source::produce`): `exec_sort`'s drain leg,
/// forward-only (the lane refuses non-forward calls before engaging).
/// Fetches into `ps_ResultTupleSlot`; `None` = exhausted.
pub fn sort_lane_next<'mcx>(
    node: &mut SortState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    debug_assert!(node.sort_Done);
    // Runtime full sort (m3-sort-b shape b): serve the adopted partition
    // outputs in the canonical order — virtual rows indexing straight into
    // the sealed run buffers (byref datums point into the runs' arenas,
    // which live in the node state until reset/rescan/end — the adopted
    // sink-emit lifetime discipline).
    if node.runtime_full.is_some() {
        return Ok(runtime_full_pop(node, estate));
    }
    // Lane refsort: the narrow (key, ref) tuplesort is NEVER node output —
    // serve the gathered winners, in sorted order, from the buffer.
    if node.refsort {
        return Ok(refsort_pop(node, estate));
    }
    let mcx = estate.es_query_cxt;
    let ts = node.tuplesortstate.as_mut().expect("sort_lane_next before sort_lane_finish");
    let slot_id = node.ps_ResultTupleSlot;
    let slot = estate.slot_mut(slot_id);
    let got = if node.datumSort {
        exectuples::exec_clear_tuple(slot, mcx);
        match ts.getdatum(true)? {
            Some(nd) => {
                let base = slot.base_mut();
                base.tts_values[0] = if nd.isnull { Datum::null() } else { nd.value };
                base.tts_isnull[0] = nd.isnull;
                exectuples::exec_store_virtual_tuple(slot);
                true
            }
            None => false,
        }
    } else {
        ts.gettupleslot(true, false, slot, mcx)?
    };
    Ok(got.then_some(slot_id))
}

/// `ExecEndSort` node-local half; the caller ends the outer child.
pub fn exec_end_sort(node: &mut SortState<'_>) {
    node.tuplesortstate = None;
    node.ps_ResultTupleDesc = None;
    refsort_clear(node);
    node.refsort_desc = None;
}

/// `ExecSortMarkPos`.
pub fn exec_sort_mark_pos(node: &mut SortState<'_>) -> PgResult<()> {
    if !node.sort_Done {
        return Ok(());
    }
    node.tuplesortstate.as_mut().unwrap().markpos()
}

/// `ExecSortRestrPos`.
pub fn exec_sort_restr_pos(node: &mut SortState<'_>) -> PgResult<()> {
    if !node.sort_Done {
        return Ok(());
    }
    node.tuplesortstate.as_mut().unwrap().restorepos()
}

/// `ExecReScanSort` node-local half. Returns true when the caller must rescan
/// the outer child (C's chgParam is always NULL until the Param lanes land).
/// ExecReScanSort (nodeSort.c), chgParam-nonnull arm: the input changed, so
/// any finished sort is stale.
pub fn exec_rescan_sort_chg<'mcx>(
    node: &mut SortState<'mcx>,
    estate: &mut EStateData<'mcx>,
) {
    if node.sort_Done {
        let mcx = estate.es_query_cxt;
        exectuples::exec_clear_tuple(estate.slot_mut(node.ps_ResultTupleSlot), mcx);
    }
    node.sort_Done = false;
    node.tuplesortstate = None;
    refsort_clear(node);
}

pub fn exec_rescan_sort<'mcx>(
    node: &mut SortState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if !node.sort_Done {
        return Ok(false);
    }
    let mcx = estate.es_query_cxt;
    exectuples::exec_clear_tuple(estate.slot_mut(node.ps_ResultTupleSlot), mcx);

    if node.bounded != node.bounded_Done || node.bound != node.bound_Done || !node.randomAccess
    {
        node.sort_Done = false;
        node.tuplesortstate = None;
        // Refsort never arms with randomAccess, so a refsort-fed node always
        // takes this reset arm: refs/winners never cross a rescan.
        refsort_clear(node);
        Ok(true)
    } else {
        node.tuplesortstate.as_mut().unwrap().rescan()?;
        Ok(false)
    }
}

/// The `ExecSetTupleBound` SortState arm (execProcnode.c).
pub fn sort_set_tuple_bound(node: &mut SortState<'_>, tuples_needed: i64) {
    if tuples_needed < 0 {
        node.bounded = false;
    } else {
        node.bounded = true;
        node.bound = tuples_needed;
    }
}

/// `ExecGetResultType` for a Sort node.
pub fn sort_result_type(node: &SortState<'_>) -> Rc<TupleDescData<'static>> {
    node.ps_ResultTupleDesc.clone().expect("sort already ended")
}

// Exempt: released in exec_end_sort.
mcx::forget_safe_struct!(
    SortState<'_> { plan, ps_ResultTupleSlot, randomAccess, bounded, bound,
        sort_Done, bounded_Done, bound_Done, datumSort, refsort, refsort_refused,
        lane_ra_fusible;
        ps_ResultTupleDesc, tuplesortstate, refsort_out, refsort_desc,
        runtime_full },
);
