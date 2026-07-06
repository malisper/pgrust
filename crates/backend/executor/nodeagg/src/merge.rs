// Parallel-finalize table handoff (docs/design/parallel-finalize-merge.md):
// partial-hashagg workers install their finished tables here by pointer
// (thread-native; C must serialize rows through the tuple queues), and the
// finalize Agg merges them bucket-by-bucket (top-8 hash bits) instead of
// re-hashing per-row. Engagement is leader-decided at ExecInitAgg from the
// plan shape; anything outside it runs the classic row path unchanged.

use core::ptr::NonNull;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use ::datum::NullableDatum;
use ::execexpr::{exec_eval_expr, AggPerGroup, EvalSlots};
use ::execgrouping::TupleHashEntryData;
use ::executils::{EStateData, ExecSlotId};
use ::mcx::{Mcx, PgVec};
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{AggStateNode, FmgrInfo, LocalFcinfo};
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::Agg;
use ::types_nodes::primnodes::Aggref;
use ::types_nodes::NodeTag;
use ::types_pathnodes::{AGGSPLIT_FINAL_DESERIAL, AGGSPLIT_INITIAL_SERIAL, AGG_HASHED};
use ::types_slot::{SlotData, TupleSlotKind};
use ::types_tuple::htup::MinimalTupleData;
use ::types_tuple::TupleDescData;

use crate::{
    collect_base_var_cols, finalize_aggregates, lookup_hash_entry, AggStateData, PerHashData,
    TransTyp,
};

// Combine fns whose merge-order reassociation is unobservable: integer and
// boolean add/min/max. Floats stay on the row path (reassociation changes
// low-order bits); internal transtypes are excluded by the byval gate.
// (oid, name): 176 int2pl, 177 int4pl, 463 int8pl, 768 int4larger,
// 769 int4smaller, 770 int2larger, 771 int2smaller, 1236 int8larger,
// 1237 int8smaller, 2515 booland_statefunc, 2516 boolor_statefunc.
const COMBINE_WHITELIST: &[Oid] = &[176, 177, 463, 768, 769, 770, 771, 1236, 1237, 2515, 2516];

// One worker table, self-contained: `buf` owns the [pergroups][tuple] images
// the entries point into (byval transvalues only, per the engagement gate).
pub struct HandedAggTable {
    entries: Vec<TupleHashEntryData>,
    additionalsize: usize,
    _buf: Vec<u64>,
}

// SAFETY: entries point only into the struct's own heap buffer (stable across
// moves); install/take hand ownership through the handoff Mutex and the
// installer never touches the payload again.
unsafe impl Send for HandedAggTable {}

#[derive(Default)]
pub struct AggTableHandoff {
    slots: Mutex<Vec<HandedAggTable>>,
}

impl AggTableHandoff {
    fn install(&self, t: HandedAggTable) {
        self.slots.lock().unwrap_or_else(|e| e.into_inner()).push(t);
    }

    fn take_all(&self) -> Vec<HandedAggTable> {
        core::mem::take(&mut *self.slots.lock().unwrap_or_else(|e| e.into_inner()))
    }
}

// Per-thread handoff registry keyed by the PARTIAL Agg plan node's address —
// unique per live plan, and the same object on leader and worker threads
// (worker pstmts share the leader's plan tree by reference). Kept out of
// EStateData so the serial per-query path pays nothing (select1 gate).
// Leader entries are Weak (the finalize's FinalizeMerge holds the strong Arc
// and deregisters on drop); worker threads adopt for the run and clear after.
std::thread_local! {
    static REGISTRY: core::cell::RefCell<Vec<(usize, std::sync::Weak<AggTableHandoff>)>> =
        const { core::cell::RefCell::new(Vec::new()) };
}

fn registry_insert(key: usize, h: &Arc<AggTableHandoff>) {
    REGISTRY.with(|r| {
        let mut v = r.borrow_mut();
        v.retain(|(_, w)| w.strong_count() > 0);
        v.push((key, Arc::downgrade(h)));
    });
}

fn registry_remove(key: usize) {
    let _ = REGISTRY.try_with(|r| r.borrow_mut().retain(|(k, _)| *k != key));
}

fn registry_get(key: usize) -> Option<Arc<AggTableHandoff>> {
    REGISTRY.with(|r| {
        r.borrow().iter().find_map(|(k, w)| (*k == key).then(|| w.upgrade()).flatten())
    })
}

/// Leader-side snapshot for execParallel: every registered handoff (workers
/// match by plan-node address, so entries of unrelated Gathers are inert).
pub struct AggHandoffExport(Vec<(usize, Arc<AggTableHandoff>)>);

pub fn export_registry() -> AggHandoffExport {
    AggHandoffExport(REGISTRY.with(|r| {
        r.borrow().iter().filter_map(|(k, w)| w.upgrade().map(|a| (*k, a))).collect()
    }))
}

/// Worker-thread adoption before the run (parallel_query_main); the export
/// (held in ParallelExecShared) keeps the strong refs for the run.
pub fn adopt_registry(export: &AggHandoffExport) {
    REGISTRY.with(|r| {
        let mut v = r.borrow_mut();
        for (k, a) in &export.0 {
            v.push((*k, Arc::downgrade(a)));
        }
    });
}

/// Worker-thread cleanup after the run (all paths, incl. unwind).
pub fn clear_thread_registry() {
    let _ = REGISTRY.try_with(|r| r.borrow_mut().clear());
}

struct MergeCombine {
    flinfo: FmgrInfo,
    strict: bool,
    collation: Oid,
}

pub(crate) struct FinalizeMerge<'mcx> {
    handoff: Arc<AggTableHandoff>,
    registry_key: usize,
    combines: Vec<MergeCombine>,
    // transno -> partial-output attno of the state column (replay fallback).
    state_cols: Vec<i16>,
    replay_slot: ExecSlotId,
    // hash_desc-shaped minimal slot: probe/deform side of entry tuples.
    key_slot: SlotData<'mcx>,
    run: Option<MergeRun>,
}

impl FinalizeMerge<'_> {
    pub(crate) fn has_run(&self) -> bool {
        self.run.is_some()
    }
}

impl Drop for FinalizeMerge<'_> {
    fn drop(&mut self) {
        registry_remove(self.registry_key);
    }
}

// Entry indexes of one source, bucketed by the top-8 hash bits.
struct Partition {
    starts: Vec<u32>,
    idx: Vec<u32>,
}

fn partition_entries(entries: &[TupleHashEntryData]) -> Partition {
    let mut counts = [0u32; 256];
    for e in entries {
        counts[(e.hash() >> 24) as usize] += 1;
    }
    let mut starts = Vec::with_capacity(257);
    let mut acc = 0u32;
    starts.push(0);
    for c in counts {
        acc += c;
        starts.push(acc);
    }
    let mut cursor: Vec<u32> = starts[..256].to_vec();
    let mut idx = vec![0u32; entries.len()];
    for (i, e) in entries.iter().enumerate() {
        let b = (e.hash() >> 24) as usize;
        idx[cursor[b] as usize] = i as u32;
        cursor[b] += 1;
    }
    Partition { starts, idx }
}

const PROBE_EMPTY: u32 = u32::MAX;

// PGRUST_AGG_MERGE_STATS engagement probe (PGRUST_TQUEUE_STATS precedent):
// off (one cached env read) on production paths.
fn merge_stats_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var_os("PGRUST_AGG_MERGE_STATS").is_some())
}

struct MergeRun {
    tables: Vec<HandedAggTable>,
    // parts[0] covers the finalize's own row-built table; parts[1..] the
    // handed tables in install order.
    parts: Vec<Partition>,
    additionalsize: usize,
    bucket: usize,
    // Current bucket's merged groups, first-seen order (source-major).
    out: Vec<TupleHashEntryData>,
    out_pos: usize,
    // Open-addressed (hash, out index) probe over the current bucket.
    probe: Vec<(u32, u32)>,
}

// C advance_combine semantics for a byval transition, one incoming partial
// state (the AggPlainTransInitStrictByVal contract, input-check folded).
// `dst` is a WORKER partial state serving as the accumulator, not a fresh
// finalize pergroup: its no_trans_value is stale under non-strict partial
// transfns (int4_sum never clears it), so never-adopted is detected by
// trans_value_is_null — exact for the whitelist because those fns never
// return NULL from non-NULL args (a strict combine chain cannot go null).
fn combine_one(
    c: &mut MergeCombine,
    agg_node: NonNull<AggStateNode>,
    per_tuple: Mcx<'_>,
    dst: &mut AggPerGroup,
    src: &AggPerGroup,
) -> PgResult<()> {
    if c.strict {
        if src.trans_value_is_null {
            return Ok(());
        }
        if dst.trans_value_is_null {
            dst.trans_value = src.trans_value;
            dst.trans_value_is_null = false;
            dst.no_trans_value = false;
            return Ok(());
        }
    }
    let mut fcinfo = LocalFcinfo::<2>::fresh(c.collation);
    fcinfo.nargs = 2;
    fcinfo.context = Some(agg_node.cast());
    // SAFETY: the per-tuple context outlives this stack frame's single call.
    unsafe { fcinfo.set_result_mcx(per_tuple) };
    fcinfo.args[0] =
        NullableDatum { value: dst.trans_value, isnull: dst.trans_value_is_null };
    fcinfo.args[1] = NullableDatum { value: src.trans_value, isnull: src.trans_value_is_null };
    let value = c.flinfo.invoke(&mut fcinfo)?;
    dst.trans_value = value;
    dst.trans_value_is_null = fcinfo.isnull;
    dst.no_trans_value = false;
    Ok(())
}

fn partial_agg_of<'mcx>(
    node: &'mcx Agg<'mcx>,
) -> Option<(&'mcx ::types_nodes::plannodes::Gather<'mcx>, &'mcx Agg<'mcx>)> {
    let gather = node.plan.lefttree?;
    if gather.node_tag() != NodeTag::T_Gather {
        return None;
    }
    let g = gather.as_gather()?;
    let partial = g.plan.lefttree?;
    if partial.node_tag() != NodeTag::T_Agg {
        return None;
    }
    Some((g, partial.as_agg()?))
}

// tlist position `pos` (1-based) is a pure OUTER passthrough Var of the same
// position (post-setrefs shape).
fn tle_is_passthrough(tlist: &::types_nodes::list::NodeList<'_>, pos: i16) -> bool {
    if pos < 1 || pos as usize > tlist.len() {
        return false;
    }
    let Some(te) = tlist.nth((pos - 1) as usize).as_target_entry() else { return false };
    te.expr.as_var().is_some_and(|v| {
        v.varno == ::types_nodes::primnodes::OUTER_VAR && v.varattno == pos
    })
}

// The leader-side engagement decision + carrier build (ExecInitAgg tail of
// the finalize AGG_HASHED arm). None = classic row path.
#[allow(clippy::too_many_arguments)]
pub(crate) fn init_finalize_merge<'mcx>(
    node: &'mcx Agg<'mcx>,
    estate: &mut EStateData<'mcx>,
    trans_fnoid: &[Oid],
    trans_typ: &[TransTyp],
    trans_aggref: &[Option<(Node<'mcx>, &'mcx Aggref<'mcx>)>],
    pertrans_sort_empty: bool,
    evaltrans_has_subplan: bool,
    ph: &PerHashData<'mcx>,
    outer_desc: Option<&Rc<TupleDescData<'static>>>,
) -> PgResult<Option<FinalizeMerge<'mcx>>> {
    let mcx = estate.es_query_cxt;
    let Some(outer_desc) = outer_desc else { return Ok(None) };
    if node.aggsplit != AGGSPLIT_FINAL_DESERIAL
        || estate.es_instrument != 0
        || !pertrans_sort_empty
        || evaltrans_has_subplan
    {
        return Ok(None);
    }
    let Some((gather, partial)) = partial_agg_of(node) else { return Ok(None) };
    let num_cols = node.numCols as usize;
    if partial.aggstrategy != AGG_HASHED
        || partial.aggsplit != AGGSPLIT_INITIAL_SERIAL
        || partial.numCols as usize != num_cols
        || partial.grpOperators != node.grpOperators
        || partial.grpCollations != node.grpCollations
        || !partial.plan.qual.is_nil()
        || ph.hash_grp_col_idx_input.len() != num_cols
    {
        return Ok(None);
    }
    // Worker tables must carry exactly the grouping key columns, in grpColIdx
    // order, so their tuples deform under the finalize's hash_desc.
    let partial_outer = partial
        .plan
        .lefttree
        .and_then(Node::as_plan)
        .expect("partial Agg without an outer plan");
    {
        let mut base: PgVec<'mcx, bool> =
            ::mcx::vec_with_capacity_in(mcx, partial_outer.targetlist.len())?;
        base.resize(partial_outer.targetlist.len(), false);
        for tle in partial.plan.targetlist.iter() {
            collect_base_var_cols(tle, &mut base);
        }
        for &attno in partial.grpColIdx {
            base[(attno - 1) as usize] = false;
        }
        if base.iter().any(|&b| b) {
            return Ok(None);
        }
    }
    let hash_desc = ph
        .retrieve_slot
        .base()
        .tts_tupleDescriptor
        .clone()
        .expect("perhash retrieve slot carries the hash desc");
    // Key correspondence proof: the finalize's key i must reach, through a
    // passthrough Gather tlist, the partial-output Var over the partial's
    // key i (a partial-INPUT column — what the worker table stores): same
    // source column, hence same datum and type.
    for i in 0..num_cols {
        let pos = node.grpColIdx[i];
        if !tle_is_passthrough(&gather.plan.targetlist, pos) {
            return Ok(None);
        }
        if pos < 1 || pos as usize > partial.plan.targetlist.len() {
            return Ok(None);
        }
        let Some(tle) = partial.plan.targetlist.nth((pos - 1) as usize).as_target_entry()
        else {
            return Ok(None);
        };
        let matches = tle.expr.as_var().is_some_and(|v| {
            v.varno == ::types_nodes::primnodes::OUTER_VAR
                && v.varattno == partial.grpColIdx[i]
        });
        if !matches {
            return Ok(None);
        }
    }

    let numtrans = trans_fnoid.len();
    // Worker pergroup arrays are sized by the partial's transno count; the
    // merge reads them by the finalize's — require identical numbering.
    let partial_numtrans = partial
        .plan
        .targetlist
        .iter()
        .filter_map(|n| n.as_target_entry())
        .filter_map(|te| te.expr.as_aggref())
        .map(|a| a.aggtransno as usize + 1)
        .max()
        .unwrap_or(0);
    if partial_numtrans != numtrans {
        return Ok(None);
    }
    let mut combines = Vec::with_capacity(numtrans);
    let mut state_cols = Vec::with_capacity(numtrans);
    for t in 0..numtrans {
        if !trans_typ[t].byval || !COMBINE_WHITELIST.contains(&trans_fnoid[t]) {
            return Ok(None);
        }
        let (_, aggref) = trans_aggref[t].expect("planner aggtransno numbering has gaps");
        let arg = aggref
            .args
            .iter()
            .next()
            .and_then(|n| n.as_target_entry())
            .map(|te| te.expr)
            .and_then(|e| e.as_var());
        let Some(var) = arg else { return Ok(None) };
        if var.varattno < 1 || var.varattno as i32 > outer_desc.natts {
            return Ok(None);
        }
        // Transno correspondence: this finalize transition's state column
        // must carry the PARTIAL transition of the same transno (worker
        // pergroups are indexed by the partial's aggtransno).
        if !tle_is_passthrough(&gather.plan.targetlist, var.varattno) {
            return Ok(None);
        }
        let partial_te = partial
            .plan
            .targetlist
            .nth((var.varattno - 1) as usize)
            .as_target_entry()
            .and_then(|te| te.expr.as_aggref());
        let Some(pref) = partial_te else { return Ok(None) };
        if pref.aggtransno as usize != t || pref.aggtranstype != aggref.aggtranstype {
            return Ok(None);
        }
        state_cols.push(var.varattno);
        let mut flinfo = fmgr_core::fmgr_info(trans_fnoid[t])?;
        let mut fnexpr_types: PgVec<'mcx, Oid> = ::mcx::vec_with_capacity_in(mcx, 2)?;
        fnexpr_types.push(aggref.aggtranstype);
        fnexpr_types.push(aggref.aggtranstype);
        // SAFETY: leaked into the query arena; the flinfo dies with the plan
        // (exec_init_agg's finalfn carrier precedent).
        let fnexpr_types: &'static [Oid] = unsafe { core::mem::transmute(fnexpr_types.leak()) };
        let carrier = ::mcx::alloc_leak_in(
            mcx,
            ::types_core::fmgr::AggFnArgTypes {
                rettype: aggref.aggtranstype,
                argtypes: fnexpr_types,
            },
        )?;
        // SAFETY: carrier is arena-backed for the query, see above.
        flinfo.fn_expr =
            Some(unsafe { ::types_core::fmgr::FnExprErased::from_node_ref(carrier) });
        let strict = flinfo.fn_strict;
        combines.push(MergeCombine { flinfo, strict, collation: aggref.inputcollid });
    }

    let replay_slot = {
        // 'static desc narrows into the query lifetime (procnode's
        // exec_type_from_tl carriers are 'static-typed the same way).
        let d: Rc<TupleDescData<'mcx>> =
            unsafe { core::mem::transmute(outer_desc.clone()) };
        estate.exec_init_extra_tuple_slot(Some(d), TupleSlotKind::Virtual)
    };
    let key_slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(hash_desc));

    let handoff = Arc::new(AggTableHandoff::default());
    let registry_key = partial as *const Agg<'_> as usize;
    registry_insert(registry_key, &handoff);
    Ok(Some(FinalizeMerge {
        handoff,
        registry_key,
        combines,
        state_cols,
        replay_slot,
        key_slot,
        run: None,
    }))
}

// Worker-side install at fill completion: the leader registered a handoff for
// this plan node iff the shape is engaged. A spilled table keeps the classic
// row emission (its groups already went partly to tape).
pub(crate) fn maybe_install_handoff(node: &mut AggStateData<'_>) {
    if node.plan.aggsplit != AGGSPLIT_INITIAL_SERIAL || node.plan.aggstrategy != AGG_HASHED {
        return;
    }
    let id = node.plan.plan.plan_node_id;
    let Some(handoff) = registry_get(node.plan as *const Agg<'_> as usize) else {
        return;
    };
    let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
    if ph.spill.ever_spilled || !ph.spill.batches.is_empty() {
        return;
    }
    let additionalsize = ph.hashtable.additionalsize();
    let src = ph.hashtable.entries();
    let mut bytes = 0usize;
    for e in src {
        // SAFETY: entry images are live table_ctx allocations led by t_len.
        let t_len = unsafe { (*e.tuple().as_ptr()).t_len } as usize;
        bytes += (additionalsize + t_len + 7) & !7;
    }
    let mut buf: Vec<u64> = vec![0; bytes / 8];
    let mut entries: Vec<TupleHashEntryData> = Vec::with_capacity(src.len());
    let base = buf.as_mut_ptr().cast::<u8>();
    let mut off = 0usize;
    for e in src {
        // SAFETY: source image is [additionalsize][tuple of t_len] per the
        // table's exec_copy_slot_minimal_tuple layout; dst has bytes reserved.
        let e2 = unsafe {
            let t_len = (*e.tuple().as_ptr()).t_len as usize;
            let img = e.tuple().as_ptr().cast::<u8>().sub(additionalsize);
            let dst = base.add(off);
            core::ptr::copy_nonoverlapping(img, dst, additionalsize + t_len);
            off += (additionalsize + t_len + 7) & !7;
            let mut e2 = *e;
            e2.set_tuple(NonNull::new_unchecked(
                dst.add(additionalsize).cast::<MinimalTupleData>(),
            ));
            e2
        };
        entries.push(e2);
    }
    if merge_stats_enabled() {
        eprintln!("AGG_MERGE_STATS install: node={id} entries={} bytes={bytes}", entries.len());
    }
    handoff.install(HandedAggTable { entries, additionalsize, _buf: buf });
    ph.hashtable.reset();
}

// Leader-side consumption at the finalize's fill boundary (before
// hashagg_finish_initial_spills): a never-spilled finalize takes the tables
// into a bucket-merge run; a spilled one replays their entries through the
// spill-aware row machinery.
pub(crate) fn consume_handoff<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let Some(m) = node.merge.as_ref() else { return Ok(()) };
    let tables = m.handoff.take_all();
    if tables.is_empty() {
        return Ok(());
    }
    let ph = node.perhash.as_ref().expect("hashed Agg has perhash");
    if merge_stats_enabled() {
        eprintln!(
            "AGG_MERGE_STATS consume: tables={} entries={} row_groups={} mode={}",
            tables.len(),
            tables.iter().map(|t| t.entries.len()).sum::<usize>(),
            ph.hashtable.entries().len(),
            if ph.spill.ever_spilled { "replay" } else { "bucket-merge" },
        );
    }
    if ph.spill.ever_spilled {
        return replay_handed_rows(node, estate, tables);
    }
    let additionalsize = ph.hashtable.additionalsize();
    let mut parts = Vec::with_capacity(tables.len() + 1);
    parts.push(partition_entries(ph.hashtable.entries()));
    for t in &tables {
        debug_assert!(t.additionalsize == additionalsize);
        parts.push(partition_entries(&t.entries));
    }
    node.merge.as_mut().unwrap().run = Some(MergeRun {
        tables,
        parts,
        additionalsize,
        bucket: 0,
        out: Vec::new(),
        out_pos: 0,
        probe: Vec::new(),
    });
    Ok(())
}

// Handed entries re-enter as synthesized partial-output rows through the
// classic fill body (lookup + evaltrans, spill included) — byte-equivalent
// to the rows the worker would have sent.
fn replay_handed_rows<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    tables: Vec<HandedAggTable>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let mut m = node.merge.take().expect("replay under an engaged merge");
    let mut result = Ok(());
    'outer: for t in &tables {
        for e in &t.entries {
            {
                let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
                // SAFETY: entry images live in the handed buffer for the
                // whole replay.
                unsafe {
                    exectuples::exec_store_minimal_tuple_ptr(&mut m.key_slot, mcx, e.tuple())
                };
                exectuples::slot_getallattrs(&mut m.key_slot);
                let replay = estate.slot_mut(m.replay_slot);
                exectuples::exec_store_all_null_tuple(replay, mcx);
                {
                    let src = m.key_slot.base();
                    let dst = replay.base_mut();
                    for (i, &attno) in ph.hash_grp_col_idx_input.iter().enumerate() {
                        dst.tts_values[(attno - 1) as usize] = src.tts_values[i];
                        dst.tts_isnull[(attno - 1) as usize] = src.tts_isnull[i];
                    }
                    if let Some(add) = e.additional(t.additionalsize) {
                        let pg = add.cast::<AggPerGroup>();
                        for (transno, &attno) in m.state_cols.iter().enumerate() {
                            // SAFETY: additionalsize holds numtrans pergroups.
                            let s = unsafe { &*pg.as_ptr().add(transno) };
                            dst.tts_values[(attno - 1) as usize] = s.trans_value;
                            dst.tts_isnull[(attno - 1) as usize] = s.trans_value_is_null;
                        }
                    }
                }
            }
            estate.ecxt_mut(node.tmpcontext).ecxt_outertuple = Some(m.replay_slot);
            match lookup_hash_entry(node, estate, m.replay_slot) {
                Ok(true) => {
                    let replay = estate.slot_mut(m.replay_slot);
                    let mut slots =
                        EvalSlots { scan: None, inner: None, outer: Some(replay) };
                    if let Err(e) = exec_eval_expr(node.evaltrans.as_mut().unwrap(), &mut slots)
                    {
                        result = Err(e);
                        break 'outer;
                    }
                }
                Ok(false) => {}
                Err(e) => {
                    result = Err(e);
                    break 'outer;
                }
            }
            estate.reset_expr_context(node.tmpcontext);
        }
    }
    node.merge = Some(m);
    result
}

// agg_retrieve_hash_table's merged twin: one qual-passing merged group per
// call, buckets merged on demand in top-8-hash-bit order, groups within a
// bucket in first-seen (source-major) order.
pub(crate) fn agg_retrieve_merged<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    let mcx = estate.es_query_cxt;
    loop {
        estate.reset_expr_context(node.ps_ExprContext);

        let next = next_merged_group(node, estate)?;
        let Some(entry) = next else {
            node.agg_done = true;
            return Ok(None);
        };
        let additionalsize =
            node.merge.as_ref().unwrap().run.as_ref().unwrap().additionalsize;
        let pergroup = {
            let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
            // SAFETY: merged entry images live in the run's buffers (or the
            // node's table context) until the run drops.
            unsafe {
                exectuples::exec_store_minimal_tuple_ptr(&mut ph.retrieve_slot, mcx, entry.tuple())
            };
            exectuples::slot_getallattrs(&mut ph.retrieve_slot);
            exectuples::exec_store_all_null_tuple(&mut ph.first_slot, mcx);
            {
                let PerHashData {
                    retrieve_slot: hashslot, first_slot, hash_grp_col_idx_input, ..
                } = &mut *ph;
                let src = hashslot.base();
                let dst = first_slot.base_mut();
                for (i, &attno) in hash_grp_col_idx_input.iter().enumerate() {
                    let v = (attno - 1) as usize;
                    dst.tts_values[v] = src.tts_values[i];
                    dst.tts_isnull[v] = src.tts_isnull[i];
                }
            }
            entry.additional(additionalsize).map_or(NonNull::dangling(), |p| p.cast())
        };
        finalize_aggregates(node, estate, pergroup)?;

        {
            let AggStateData { perhash, qual, .. } = node;
            let ph = perhash.as_mut().unwrap();
            let mut slots =
                EvalSlots { scan: None, inner: None, outer: Some(&mut ph.first_slot) };
            if !::execexpr::exec_qual(qual.as_deref_mut(), &mut slots)? {
                continue;
            }
        }
        let result_slot = estate.slot_mut(node.ps_ResultTupleSlot);
        let ph = node.perhash.as_mut().unwrap();
        let mut slots = EvalSlots { scan: None, inner: None, outer: Some(&mut ph.first_slot) };
        ::execexpr::exec_project(&mut node.proj, &mut slots, result_slot, mcx)?;
        return Ok(Some(node.ps_ResultTupleSlot));
    }
}

fn next_merged_group<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<TupleHashEntryData>> {
    loop {
        {
            let run = node
                .merge
                .as_mut()
                .and_then(|m| m.run.as_mut())
                .expect("merged retrieve under a built run");
            if run.out_pos < run.out.len() {
                let e = run.out[run.out_pos];
                run.out_pos += 1;
                return Ok(Some(e));
            }
            if run.bucket >= 256 {
                return Ok(None);
            }
        }
        merge_next_bucket(node, estate)?;
    }
}

fn merge_next_bucket<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let per_tuple = estate.ecxt(node.tmpcontext).per_tuple_mcx();
    let agg_node = node.agg_node;
    let AggStateData { perhash, merge, .. } = node;
    let ph = perhash.as_mut().expect("hashed Agg has perhash");
    let m = merge.as_mut().expect("merge engaged");
    let FinalizeMerge { run, key_slot, combines, .. } = m;
    let run = run.as_mut().expect("run built");
    let b = run.bucket;
    run.bucket += 1;
    run.out.clear();
    run.out_pos = 0;

    let mut total = 0usize;
    for p in &run.parts {
        total += (p.starts[b + 1] - p.starts[b]) as usize;
    }
    if total == 0 {
        return Ok(());
    }
    let cap = (total * 2).next_power_of_two().max(16);
    run.probe.clear();
    run.probe.resize(cap, (0, PROBE_EMPTY));
    let mask = (cap - 1) as u32;
    let MergeRun { tables, parts, probe, out, additionalsize, .. } = run;
    let additionalsize = *additionalsize;

    for (src, part) in parts.iter().enumerate() {
        let lo = part.starts[b] as usize;
        let hi = part.starts[b + 1] as usize;
        for &eix in &part.idx[lo..hi] {
            let e = if src == 0 {
                ph.hashtable.entries()[eix as usize]
            } else {
                tables[src - 1].entries[eix as usize]
            };
            // SAFETY: entry images live for the run (handed buffers / the
            // node's table context).
            unsafe { exectuples::exec_store_minimal_tuple_ptr(key_slot, mcx, e.tuple()) };
            let input_key = ph.hashtable.kernel_key_of(key_slot);
            let mut pos = e.hash() & mask;
            loop {
                let (h, oix) = probe[pos as usize];
                if oix == PROBE_EMPTY {
                    probe[pos as usize] = (e.hash(), out.len() as u32);
                    out.push(e);
                    break;
                }
                if h == e.hash() {
                    let cand = out[oix as usize];
                    if ph.hashtable.match_tuple(key_slot, input_key, &cand, mcx)? {
                        let dst = cand.additional(additionalsize).map(|p| p.cast::<AggPerGroup>());
                        let sp = e.additional(additionalsize).map(|p| p.cast::<AggPerGroup>());
                        if let (Some(dst), Some(sp)) = (dst, sp) {
                            for (transno, c) in combines.iter_mut().enumerate() {
                                // SAFETY: additionalsize holds numtrans
                                // pergroups on both sides; dst is uniquely
                                // reachable through the merged bucket.
                                unsafe {
                                    combine_one(
                                        c,
                                        agg_node,
                                        per_tuple,
                                        &mut *dst.as_ptr().add(transno),
                                        &*sp.as_ptr().add(transno),
                                    )?;
                                }
                            }
                        }
                        break;
                    }
                }
                pos = (pos + 1) & mask;
            }
        }
    }
    Ok(())
}

// Rescan: merged results reference handed buffers mutated in place by the
// combine pass, so a rescan always rebuilds from a fresh worker run (the
// caller rescans the outer Gather, which relaunches workers).
pub(crate) fn reset_merge_for_rescan(node: &mut AggStateData<'_>) -> bool {
    let Some(m) = node.merge.as_mut() else { return false };
    let had_run = m.run.take().is_some();
    m.handoff.take_all();
    had_run
}
