// Parallel-finalize table handoff (docs/design/parallel-finalize-merge.md):
// partial-hashagg workers install their finished tables here by pointer
// (thread-native; C must serialize rows through the tuple queues), and the
// finalize Agg merges them bucket-by-bucket (top-8 hash bits) instead of
// re-hashing per-row. Engagement is leader-decided at ExecInitAgg from the
// plan shape; anything outside it runs the classic row path unchanged.

use core::cell::UnsafeCell;
use core::ptr::NonNull;
use core::sync::atomic::{AtomicUsize, Ordering};
use std::rc::Rc;
use std::sync::{Arc, Barrier, Mutex};

use ::adt_numeric::{Int128AggState, NumericAggState};
use ::datum::{Datum, NullableDatum};
use ::execexpr::{exec_eval_expr, AggPerGroup, EvalSlots};
use ::execgrouping::TupleHashEntryData;
use ::executils::{EStateData, ExecSlotId};
use ::mcx::{Mcx, PgVec};
use ::types_core::Oid;
use ::types_error::{PgError, PgResult};
use ::types_fmgr::{AggStateNode, FmgrInfo, LocalFcinfo, PGFunction};
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::Agg;
use ::types_nodes::primnodes::Aggref;
use ::types_nodes::NodeTag;
use ::types_pathnodes::{AGGSPLIT_FINAL_DESERIAL, AGGSPLIT_INITIAL_SERIAL, AGG_HASHED};
use ::types_slot::{SlotData, TupleSlotKind};
use ::types_tuple::htup::MinimalTupleData;
use ::types_tuple::tupmacs::{att_isnull, att_nominal_alignby, att_pointer_alignby, fetch_att};
use ::types_tuple::varatt::{
    varatt_is_1b, varatt_is_1b_e, varatt_is_4b_u, varsize_1b, varsize_4b, varsize_any,
    VARHDRSZ, VARHDRSZ_SHORT,
};
use ::types_tuple::{
    TupleDescData, HEAP_HASNULL, MINIMAL_TUPLE_OFFSET, SizeofMinimalTupleHeader,
};

use crate::{
    collect_base_var_cols, finalize_aggregates, lookup_hash_entry, AggStateData, PerHashData,
    TransTyp,
};

// Byval combine fns whose merge-order reassociation is unobservable: integer,
// boolean, and date/time add/min/max. Floats stay on the row path (SUM
// reassociation changes low-order bits; MIN/MAX can flip which equal-comparing
// bit pattern survives, e.g. -0.0 vs +0.0).
// (oid, name): 176 int2pl, 177 int4pl, 463 int8pl, 768 int4larger,
// 769 int4smaller, 770 int2larger, 771 int2smaller, 1236 int8larger,
// 1237 int8smaller, 2515 booland_statefunc, 2516 boolor_statefunc,
// 1138 date_larger, 1139 date_smaller, 1377 time_larger, 1378 time_smaller,
// 2036 timestamp_larger, 2035 timestamp_smaller, 1196/1195 the timestamptz
// pg_proc rows over the same int64 comparison.
const COMBINE_WHITELIST: &[Oid] = &[
    176, 177, 463, 768, 769, 770, 771, 1236, 1237, 2515, 2516, 1138, 1139, 1377, 1378, 2036,
    2035, 1196, 1195,
];

// Internal-transtype combines the merge admits: the states hand across
// threads by pointer once the install relocates them into the handed buffer.
// numeric_poly_combine 3338 / int8_avg_combine 2785 (Int128AggState, pure
// arithmetic — bucket-parallel capable); numeric_combine 3341 /
// numeric_avg_combine 3337 (NumericAggState, combine allocates in the agg
// context — serial bucket merge only).
const COMBINE_POLY_SUMX2: Oid = 3338;
const COMBINE_POLY: Oid = 2785;
const COMBINE_NUMERIC_SUMX2: Oid = 3341;
const COMBINE_NUMERIC: Oid = 3337;
const INTERNALOID: Oid = 2281;

// How the merge owns and combines one transno's state (per the whitelists).
#[derive(Clone, Copy, PartialEq)]
enum CombineKind {
    Byval,
    PolyInt128 { sum_x2: bool },
    NumericAgg { sum_x2: bool },
}

// One worker table, self-contained: `buf` owns the [pergroups][tuple][states]
// images the entries point into — byval transvalues ride in the pergroups;
// internal-transtype states are relocated behind them and the copied
// pergroups repointed (u128 backing keeps Int128AggState's alignment).
pub struct HandedAggTable {
    entries: Vec<TupleHashEntryData>,
    additionalsize: usize,
    _buf: Vec<u128>,
}

// SAFETY: entries point only into the struct's own heap buffer (stable across
// moves); install/take hand ownership through the handoff Mutex and the
// installer never touches the payload again.
unsafe impl Send for HandedAggTable {}

pub struct AggTableHandoff {
    slots: Mutex<Vec<HandedAggTable>>,
    // Leader-decided per-transno state plan; the worker install relocates
    // internal states by it. Immutable after construction.
    kinds: Vec<CombineKind>,
}

impl AggTableHandoff {
    fn new(kinds: Vec<CombineKind>) -> AggTableHandoff {
        AggTableHandoff { slots: Mutex::new(Vec::new()), kinds }
    }

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

// Grouping-equality fns whose semantics the parallel claim path can evaluate
// thread-natively: bit-equality of the deformed datum for fixed byval keys,
// payload memcmp for texteq. (oid, name): 60 booleq, 61 chareq, 63 int2eq,
// 65 int4eq, 184 oideq, 467 int8eq, 1086 date_eq, 1145 time_eq,
// 2052 timestamp_eq, 1152 timestamptz_eq (integer datetimes — equality is
// bit equality); 67 texteq (deterministic collations only — default/C/POSIX;
// nondeterministic ones need varstr_cmp).
const EQ_FIXED_WHITELIST: &[Oid] = &[60, 61, 63, 65, 184, 467, 1086, 1145, 2052, 1152];
const EQ_TEXT: Oid = 67;
const DETERMINISTIC_COLLATIONS: &[Oid] = &[100, 950, 951];

// One key column of the thread-native deform/compare plan (the hash_desc
// prefix: increment 1 proves keys are the first numCols attrs on both the
// finalize's and the workers' stored tuples, with identical source types).
#[derive(Clone, Copy)]
struct KeyAtt {
    attlen: i16,
    attbyval: bool,
    attalignby: u8,
    memcmp_payload: bool,
}

// The combine reduced to what the thread path actually needs. Byval: the
// whitelist fns never touch flinfo, fcinfo.context, or the result mcx (byval
// results), so threads call the pointer bare. PolyInt128: the arithmetic of
// poly_combine_common runs natively on the relocated states — no call at all.
#[derive(Clone, Copy)]
struct ParCombine {
    func: PGFunction,
    strict: bool,
    collation: Oid,
    kind: CombineKind,
}

// Present iff every key column and combine qualifies for bucket-parallel
// finalize; otherwise the increment-1 serial bucket merge runs unchanged.
struct ParSpec {
    atts: Vec<KeyAtt>,
    combines: Vec<ParCombine>,
    has_varlena: bool,
}

pub(crate) struct FinalizeMerge<'mcx> {
    handoff: Arc<AggTableHandoff>,
    registry_key: usize,
    combines: Vec<MergeCombine>,
    kinds: Vec<CombineKind>,
    // transno -> partial-output attno of the state column (replay fallback).
    state_cols: Vec<i16>,
    replay_slot: ExecSlotId,
    // hash_desc-shaped minimal slot: probe/deform side of entry tuples.
    key_slot: SlotData<'mcx>,
    par: Option<ParSpec>,
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
    // Bucket-parallel results (increment 2): all 256 buckets merged up front
    // by the claimer pool; retrieval drains them in bucket order.
    pre: Option<Vec<Vec<TupleHashEntryData>>>,
}

// PGRUST_AGG_MERGE_NO_PARALLEL triage kill-switch: forces the increment-1
// serial bucket merge on otherwise parallel-qualified shapes.
fn parallel_merge_disabled() -> bool {
    static OFF: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *OFF.get_or_init(|| std::env::var_os("PGRUST_AGG_MERGE_NO_PARALLEL").is_some())
}

// C advance_combine semantics, one incoming partial state (the
// AggPlainTransInitStrictByVal contract for byval, input-check folded; the
// non-strict internal combines run every call and manage their state args
// themselves — handed transvalues point at states relocated into the handed
// buffer at install). `dst` is a WORKER partial state serving as the
// accumulator, not a fresh finalize pergroup: its no_trans_value is stale
// under non-strict partial transfns (int4_sum never clears it), so
// never-adopted is detected by trans_value_is_null — exact for the whitelist
// because those fns never return NULL from non-NULL args (a strict combine
// chain cannot go null, and the internal combines return a state pointer).
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
    let mut kinds = Vec::with_capacity(numtrans);
    let mut state_cols = Vec::with_capacity(numtrans);
    for t in 0..numtrans {
        let (_, aggref) = trans_aggref[t].expect("planner aggtransno numbering has gaps");
        let kind = if aggref.aggtranstype == INTERNALOID {
            match trans_fnoid[t] {
                COMBINE_POLY_SUMX2 => CombineKind::PolyInt128 { sum_x2: true },
                COMBINE_POLY => CombineKind::PolyInt128 { sum_x2: false },
                COMBINE_NUMERIC_SUMX2 => CombineKind::NumericAgg { sum_x2: true },
                COMBINE_NUMERIC => CombineKind::NumericAgg { sum_x2: false },
                _ => return Ok(None),
            }
        } else if trans_typ[t].byval && COMBINE_WHITELIST.contains(&trans_fnoid[t]) {
            CombineKind::Byval
        } else {
            return Ok(None);
        };
        kinds.push(kind);
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

    // Bucket-parallel qualification (increment 2): every key column's
    // grouping equality must be evaluable thread-natively, and every combine
    // must run without the executor (NumericAgg combines allocate digit
    // buffers in the agg context). Failing shapes leave the engagement intact
    // on the serial bucket merge.
    let par = 'par: {
        if kinds.iter().any(|k| matches!(k, CombineKind::NumericAgg { .. })) {
            break 'par None;
        }
        let mut atts = Vec::with_capacity(num_cols);
        let mut has_varlena = false;
        for i in 0..num_cols {
            let eqfn = lsyscache::get_opcode(node.grpOperators[i])?;
            let a = hash_desc.compact_attr(i);
            let memcmp_payload = if EQ_FIXED_WHITELIST.contains(&eqfn)
                && a.attbyval
                && a.attlen > 0
            {
                false
            } else if eqfn == EQ_TEXT
                && a.attlen == -1
                && DETERMINISTIC_COLLATIONS.contains(&node.grpCollations[i])
            {
                has_varlena = true;
                true
            } else {
                break 'par None;
            };
            atts.push(KeyAtt {
                attlen: a.attlen,
                attbyval: a.attbyval,
                attalignby: a.attalignby,
                memcmp_payload,
            });
        }
        let par_combines = combines
            .iter()
            .zip(&kinds)
            .map(|(c, &kind)| ParCombine {
                func: c.flinfo.fn_addr,
                strict: c.strict,
                collation: c.collation,
                kind,
            })
            .collect();
        Some(ParSpec { atts, combines: par_combines, has_varlena })
    };

    let replay_slot = {
        // 'static desc narrows into the query lifetime (procnode's
        // exec_type_from_tl carriers are 'static-typed the same way).
        let d: Rc<TupleDescData<'mcx>> =
            unsafe { core::mem::transmute(outer_desc.clone()) };
        estate.exec_init_extra_tuple_slot(Some(d), TupleSlotKind::Virtual)
    };
    let key_slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(hash_desc));

    let handoff = Arc::new(AggTableHandoff::new(kinds.clone()));
    let registry_key = partial as *const Agg<'_> as usize;
    registry_insert(registry_key, &handoff);
    Ok(Some(FinalizeMerge {
        handoff,
        registry_key,
        combines,
        kinds,
        state_cols,
        replay_slot,
        key_slot,
        par,
        run: None,
    }))
}

const fn align16(n: usize) -> usize {
    (n + 15) & !15
}

// Handed-buffer bytes the entry's internal-transtype states need (the copy
// loop's exact layout: one 16-aligned slot per non-null non-byval state).
//
// # Safety
// `e` is a live table entry whose `additionalsize` payload holds
// `kinds.len()` pergroups with live state pointers behind them.
unsafe fn entry_state_bytes(
    e: &TupleHashEntryData,
    additionalsize: usize,
    kinds: &[CombineKind],
) -> usize {
    let Some(add) = e.additional(additionalsize) else { return 0 };
    let pg = add.cast::<AggPerGroup>();
    let mut bytes = 0usize;
    for (transno, k) in kinds.iter().enumerate() {
        // SAFETY: caller contract — additionalsize holds kinds.len()
        // pergroups.
        let s = unsafe { &*pg.as_ptr().add(transno) };
        if s.trans_value_is_null {
            continue;
        }
        bytes += match k {
            CombineKind::Byval => 0,
            CombineKind::PolyInt128 { .. } => align16(core::mem::size_of::<Int128AggState>()),
            CombineKind::NumericAgg { .. } => {
                // SAFETY: non-null internal transvalue is the live state.
                let st = unsafe { &*(s.trans_value.as_usize() as *const NumericAggState) };
                align16(core::mem::size_of::<NumericAggState>() + st.digits_bytes())
            }
        };
    }
    bytes
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
    let kinds = &handoff.kinds;
    let additionalsize = ph.hashtable.additionalsize();
    let src = ph.hashtable.entries();
    let mut bytes = 0usize;
    for e in src {
        // SAFETY: entry images are live table_ctx allocations led by t_len;
        // pergroup state pointers live in the worker's agg arenas until the
        // reset below.
        unsafe {
            let t_len = (*e.tuple().as_ptr()).t_len as usize;
            bytes += (additionalsize + t_len + 15) & !15;
            bytes += entry_state_bytes(e, additionalsize, kinds);
        }
    }
    let mut buf: Vec<u128> = vec![0; bytes.div_ceil(16)];
    let mut entries: Vec<TupleHashEntryData> = Vec::with_capacity(src.len());
    let base = buf.as_mut_ptr().cast::<u8>();
    let mut off = 0usize;
    for e in src {
        // SAFETY: source image is [additionalsize][tuple of t_len] per the
        // table's exec_copy_slot_minimal_tuple layout; dst has bytes reserved.
        // Internal-transtype states are relocated behind the image (16-aligned
        // slots off the u128 backing) and the copied pergroups repointed —
        // after this the handed table references nothing worker-owned.
        let e2 = unsafe {
            let t_len = (*e.tuple().as_ptr()).t_len as usize;
            let img = e.tuple().as_ptr().cast::<u8>().sub(additionalsize);
            let dst = base.add(off);
            core::ptr::copy_nonoverlapping(img, dst, additionalsize + t_len);
            off += (additionalsize + t_len + 15) & !15;
            for (transno, k) in kinds.iter().enumerate() {
                if matches!(k, CombineKind::Byval) {
                    continue;
                }
                let pg = &mut *dst.cast::<AggPerGroup>().add(transno);
                if pg.trans_value_is_null {
                    continue;
                }
                let state = base.add(off);
                match k {
                    CombineKind::Byval => unreachable!(),
                    CombineKind::PolyInt128 { .. } => {
                        let sp = pg.trans_value.as_usize() as *const Int128AggState;
                        state.cast::<Int128AggState>().write(*sp);
                        off += align16(core::mem::size_of::<Int128AggState>());
                    }
                    CombineKind::NumericAgg { .. } => {
                        let sp = &*(pg.trans_value.as_usize() as *const NumericAggState);
                        let digits =
                            state.add(core::mem::size_of::<NumericAggState>()).cast::<i32>();
                        state.cast::<NumericAggState>().write(sp.relocated_into(digits));
                        off += align16(
                            core::mem::size_of::<NumericAggState>() + sp.digits_bytes(),
                        );
                    }
                }
                pg.trans_value = Datum::from_usize(state as usize);
            }
            let mut e2 = *e;
            // Verbatim image copy: relocate through the table so a by-ref
            // cached key (Text probe kernel) is rebased, not left dangling.
            ph.hashtable.relocate_entry(
                &mut e2,
                NonNull::new_unchecked(dst.add(additionalsize).cast::<MinimalTupleData>()),
            );
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
    let pre = match &m.par {
        Some(spec) if !parallel_merge_disabled() => {
            parallel_merge(spec, ph.hashtable.entries(), &tables, &parts, additionalsize)?
        }
        _ => None,
    };
    node.merge.as_mut().unwrap().run = Some(MergeRun {
        tables,
        parts,
        additionalsize,
        bucket: 0,
        out: Vec::new(),
        out_pos: 0,
        probe: Vec::new(),
        pre,
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
    let mut state_vals: Vec<NullableDatum> = vec![NullableDatum::null(); m.state_cols.len()];
    'outer: for t in &tables {
        for e in &t.entries {
            // The synthesized row must carry what the worker would have SENT:
            // internal transtypes cross as their serialfn bytea (evaltrans
            // deserializes), so relocated states re-serialize here, into the
            // per-tuple arena the row-body reset below reclaims.
            if let Err(err) = synth_state_vals(
                &m.kinds,
                e,
                t.additionalsize,
                estate.ecxt(node.tmpcontext).per_tuple_mcx(),
                &mut state_vals,
            ) {
                result = Err(err);
                break 'outer;
            }
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
                    for (&attno, v) in m.state_cols.iter().zip(&state_vals) {
                        dst.tts_values[(attno - 1) as usize] = v.value;
                        dst.tts_isnull[(attno - 1) as usize] = v.isnull;
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

// One replayed entry's state-column datums: byval transvalues pass through;
// internal states serialize with the same plain functions the worker's
// serialfn wraps, so the synthesized row is byte-identical to a sent one.
fn synth_state_vals(
    kinds: &[CombineKind],
    e: &TupleHashEntryData,
    additionalsize: usize,
    per_tuple: Mcx<'_>,
    out: &mut [NullableDatum],
) -> PgResult<()> {
    let Some(add) = e.additional(additionalsize) else {
        out.fill(NullableDatum::null());
        return Ok(());
    };
    let pg = add.cast::<AggPerGroup>();
    for (transno, k) in kinds.iter().enumerate() {
        // SAFETY: additionalsize holds kinds.len() pergroups; non-null
        // internal transvalues are live relocated states in the handed
        // buffer (numeric serialization's lazy carry is the only mutation,
        // and each entry replays once).
        out[transno] = unsafe {
            let s = &*pg.as_ptr().add(transno);
            if s.trans_value_is_null || matches!(k, CombineKind::Byval) {
                NullableDatum { value: s.trans_value, isnull: s.trans_value_is_null }
            } else {
                let mut buf = ::pqformat::pq_begintypsend(per_tuple)?;
                match *k {
                    CombineKind::Byval => unreachable!(),
                    CombineKind::PolyInt128 { sum_x2 } => {
                        let st = &*(s.trans_value.as_usize() as *const Int128AggState);
                        ::adt_numeric::aggregates::int128_agg_state_serialize(
                            st, sum_x2, &mut buf,
                        )?;
                    }
                    CombineKind::NumericAgg { sum_x2 } => {
                        let st = &mut *(s.trans_value.as_usize() as *mut NumericAggState);
                        ::adt_numeric::aggregates::numeric_agg_state_serialize(
                            st, sum_x2, &mut buf,
                        )?;
                    }
                }
                NullableDatum {
                    value: ::types_fmgr::varlena_result(::pqformat::pq_endtypsend(buf)),
                    isnull: false,
                }
            }
        };
    }
    Ok(())
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
            if let Some(pre) = run.pre.as_mut() {
                let b = run.bucket;
                run.bucket += 1;
                run.out = core::mem::take(&mut pre[b]);
                run.out_pos = 0;
                continue;
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

// --- Bucket-parallel finalize merge (increment 2) ---
//
// Parallelism source: a scoped claimer pool at the leader's merge boundary —
// the Gather's workers have already exited their plans by the time the
// finalize consumes the handoff (tables install at worker fill completion),
// so the pool is sized to the handed-table count (the launched worker count
// on the engaged path) with the leader participating as one more claimer
// (parallel_leader_participation's spirit). No parallel/lib.rs surface is
// touched. Claimers take buckets 0..255 from an atomic counter after a
// barrier (so multi-claimer participation is deterministic, not a race with
// spawn latency) and run the same source-major first-seen merge as the
// serial path: identical bucket order, identical within-bucket order,
// identical combine application order — byte-identical output.

// Deform of the key prefix (first atts.len() attrs) of a stored entry tuple.
// The increment-1 engagement proof guarantees both the finalize's and the
// workers' tuples lead with the grouping keys under identical source types,
// so one KeyAtt plan walks both. No attcacheoff (Cell — not thread-safe).
//
// # Safety
// `tuple` is a live, complete minimal-tuple image whose leading attributes
// match `atts`; values/isnull have at least atts.len() slots.
unsafe fn deform_key_prefix(
    tuple: NonNull<MinimalTupleData>,
    atts: &[KeyAtt],
    values: &mut [Datum],
    isnull: &mut [bool],
) {
    // SAFETY: caller contract — live minimal tuple.
    let mt = unsafe { tuple.as_ref() };
    let base = tuple.as_ptr().cast::<u8>();
    // SAFETY: in-bounds offsets of the tuple image.
    let tp = unsafe { base.add(mt.t_hoff as usize - MINIMAL_TUPLE_OFFSET) };
    // SAFETY: as above.
    let bp = unsafe { base.add(SizeofMinimalTupleHeader) };
    let hasnulls = (mt.t_infomask & HEAP_HASNULL) != 0;
    let tuple_natts = mt.natts() as usize;
    let mut off = 0usize;
    for (i, a) in atts.iter().enumerate() {
        // SAFETY: i < tuple_natts, so the null bitmap covers bit i.
        if i >= tuple_natts || (hasnulls && unsafe { att_isnull(i, bp) }) {
            values[i] = Datum::null();
            isnull[i] = true;
            continue;
        }
        isnull[i] = false;
        // SAFETY: the walk visits attributes present in the tuple in order
        // (slot_deform_heap_tuple's contract, cacheoff branch dropped).
        unsafe {
            if a.attlen == -1 {
                off = att_pointer_alignby(off, a.attalignby, -1, tp.add(off));
            } else {
                off = att_nominal_alignby(off, a.attalignby);
            }
            values[i] = fetch_att(tp.add(off), a.attbyval, a.attlen as i32);
            if a.attlen > 0 {
                off += a.attlen as usize;
            } else {
                debug_assert!(a.attlen == -1);
                off += varsize_any(tp.add(off));
            }
        }
    }
}

// texteq's deterministic-collation core over pre-validated plain varlena
// (short or uncompressed 4B header): payload length + bytes.
//
// # Safety
// Both datums point at live varlena images that are 1b-short or
// 4b-uncompressed (the consume-time validation pass rejects everything else).
unsafe fn var_payload_eq(a: Datum, b: Datum) -> bool {
    // SAFETY: caller contract.
    unsafe {
        let (pa, la) = var_payload(a.as_usize() as *const u8);
        let (pb, lb) = var_payload(b.as_usize() as *const u8);
        la == lb && core::slice::from_raw_parts(pa, la) == core::slice::from_raw_parts(pb, lb)
    }
}

/// # Safety
/// As [`var_payload_eq`], one side.
unsafe fn var_payload(p: *const u8) -> (*const u8, usize) {
    // SAFETY: caller contract — plain short or 4B-uncompressed image.
    unsafe {
        if varatt_is_1b(p) {
            (p.add(VARHDRSZ_SHORT), varsize_1b(p) - VARHDRSZ_SHORT)
        } else {
            (p.add(VARHDRSZ), varsize_4b(p) - VARHDRSZ)
        }
    }
}

fn keys_equal(
    atts: &[KeyAtt],
    group_keys: &[Datum],
    group_nulls: &[bool],
    oix: u32,
    inv: &[Datum],
    invn: &[bool],
) -> bool {
    let base = oix as usize * atts.len();
    for (i, a) in atts.iter().enumerate() {
        let (n1, n2) = (group_nulls[base + i], invn[i]);
        if n1 != n2 {
            return false;
        }
        if n1 {
            continue;
        }
        if a.memcmp_payload {
            // SAFETY: consume-time validation proved plain representations.
            if unsafe { !var_payload_eq(group_keys[base + i], inv[i]) } {
                return false;
            }
        } else if group_keys[base + i].as_u64() != inv[i].as_u64() {
            return false;
        }
    }
    true
}

// combine_one's thread-native twin. Byval: bare fn-pointer call — the
// whitelist fns read only their args (no flinfo, no fcinfo.context, byval
// result — the result mcx stays unarmed) so semantics match the serial invoke
// exactly. PolyInt128: poly_combine_common's arithmetic run natively; where C
// allocates a fresh agg-context state for a NULL accumulator, the merge
// adopts the source's relocated state by pointer (owned by the run, consumed
// exactly once) — the resulting field values are identical.
fn combine_one_par(c: &ParCombine, dst: &mut AggPerGroup, src: &AggPerGroup) -> PgResult<()> {
    if let CombineKind::PolyInt128 { sum_x2 } = c.kind {
        if src.trans_value_is_null {
            return Ok(());
        }
        if dst.trans_value_is_null {
            dst.trans_value = src.trans_value;
            dst.trans_value_is_null = false;
            dst.no_trans_value = false;
            return Ok(());
        }
        // SAFETY: non-null internal transvalues are live relocated states
        // (handed buffers) or finalize-arena states; dst is uniquely
        // reachable through this claimer's bucket and src feeds exactly once.
        unsafe {
            let d = &mut *(dst.trans_value.as_usize() as *mut Int128AggState);
            let s = &*(src.trans_value.as_usize() as *const Int128AggState);
            if s.n > 0 {
                d.n += s.n;
                d.sum_x += s.sum_x;
                if sum_x2 {
                    d.sum_x2 += s.sum_x2;
                }
            }
        }
        return Ok(());
    }
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
    fcinfo.args[0] = NullableDatum { value: dst.trans_value, isnull: dst.trans_value_is_null };
    fcinfo.args[1] = NullableDatum { value: src.trans_value, isnull: src.trans_value_is_null };
    let value = (c.func)(None, &mut fcinfo)?;
    dst.trans_value = value;
    dst.trans_value_is_null = fcinfo.isnull;
    dst.no_trans_value = false;
    Ok(())
}

// Per-claimer reusable buffers: the bucket probe, the merged groups' deformed
// keys (stride = numCols, parallel to the bucket's out vec), and the incoming
// entry's deformed keys.
struct ParScratch {
    probe: Vec<(u32, u32)>,
    group_keys: Vec<Datum>,
    group_nulls: Vec<bool>,
    inv: Vec<Datum>,
    invn: Vec<bool>,
}

impl ParScratch {
    fn new(ncols: usize) -> ParScratch {
        ParScratch {
            probe: Vec::new(),
            group_keys: Vec::new(),
            group_nulls: Vec::new(),
            inv: vec![Datum::null(); ncols],
            invn: vec![false; ncols],
        }
    }
}

struct ParCtx<'a> {
    spec: &'a ParSpec,
    leader: &'a [TupleHashEntryData],
    tables: &'a [HandedAggTable],
    parts: &'a [Partition],
    additionalsize: usize,
    next: AtomicUsize,
    barrier: Barrier,
    // 256 bucket outputs; slot b is written only by the claimer that took b.
    out: Vec<UnsafeCell<Vec<TupleHashEntryData>>>,
}

// SAFETY: shared read-only entry/tuple images (leader table + handed bufs)
// stay live and unmoved for the scope (no arena allocation happens inside);
// the only mutation is (a) each claimer's exclusive `out[b]` slot — bucket b
// is handed to exactly one claimer by the fetch_add — and (b) pergroup
// payloads and the internal states behind their transvalue pointers, which
// partition by bucket (an entry's bucket is a pure function of its hash), so
// claimers never alias them.
unsafe impl Sync for ParCtx<'_> {}

fn claim_loop(ctx: &ParCtx<'_>, scratch: &mut ParScratch) -> PgResult<usize> {
    ctx.barrier.wait();
    let mut merged = 0usize;
    loop {
        let b = ctx.next.fetch_add(1, Ordering::Relaxed);
        if b >= 256 {
            return Ok(merged);
        }
        let out = merge_bucket_par(ctx, b, scratch)?;
        if !out.is_empty() {
            merged += 1;
        }
        // SAFETY: bucket b was claimed exclusively via the counter.
        unsafe { *ctx.out[b].get() = out };
    }
}

// merge_next_bucket's thread-native twin: same source-major first-seen order,
// same probe scheme, structural key equality, bare-pointer combines.
fn merge_bucket_par(
    ctx: &ParCtx<'_>,
    b: usize,
    s: &mut ParScratch,
) -> PgResult<Vec<TupleHashEntryData>> {
    let ncols = ctx.spec.atts.len();
    let mut total = 0usize;
    for p in ctx.parts {
        total += (p.starts[b + 1] - p.starts[b]) as usize;
    }
    let mut out = Vec::new();
    if total == 0 {
        return Ok(out);
    }
    let cap = (total * 2).next_power_of_two().max(16);
    s.probe.clear();
    s.probe.resize(cap, (0, PROBE_EMPTY));
    s.group_keys.clear();
    s.group_nulls.clear();
    let mask = (cap - 1) as u32;

    for (src, part) in ctx.parts.iter().enumerate() {
        let lo = part.starts[b] as usize;
        let hi = part.starts[b + 1] as usize;
        for &eix in &part.idx[lo..hi] {
            let e = if src == 0 {
                ctx.leader[eix as usize]
            } else {
                ctx.tables[src - 1].entries[eix as usize]
            };
            // SAFETY: entry images live for the run (handed buffers / the
            // node's table context), leading attrs match the KeyAtt plan.
            unsafe { deform_key_prefix(e.tuple(), &ctx.spec.atts, &mut s.inv, &mut s.invn) };
            let mut pos = e.hash() & mask;
            loop {
                let (h, oix) = s.probe[pos as usize];
                if oix == PROBE_EMPTY {
                    s.probe[pos as usize] = (e.hash(), out.len() as u32);
                    s.group_keys.extend_from_slice(&s.inv[..ncols]);
                    s.group_nulls.extend_from_slice(&s.invn[..ncols]);
                    out.push(e);
                    break;
                }
                if h == e.hash()
                    && keys_equal(&ctx.spec.atts, &s.group_keys, &s.group_nulls, oix, &s.inv, &s.invn)
                {
                    let cand = out[oix as usize];
                    let dst = cand.additional(ctx.additionalsize).map(|p| p.cast::<AggPerGroup>());
                    let sp = e.additional(ctx.additionalsize).map(|p| p.cast::<AggPerGroup>());
                    if let (Some(dst), Some(sp)) = (dst, sp) {
                        for (transno, c) in ctx.spec.combines.iter().enumerate() {
                            // SAFETY: additionalsize holds numtrans pergroups
                            // on both sides; dst is uniquely reachable through
                            // this claimer's bucket.
                            unsafe {
                                combine_one_par(
                                    c,
                                    &mut *dst.as_ptr().add(transno),
                                    &*sp.as_ptr().add(transno),
                                )?;
                            }
                        }
                    }
                    break;
                }
                pos = (pos + 1) & mask;
            }
        }
    }
    Ok(out)
}

// The parallel run at the consume boundary. Ok(None) = fell back to the
// serial bucket merge (a varlena key datum with a compressed/external
// representation, which the thread comparator must not touch — detoast needs
// the executor). The validation pass mutates nothing, so falling back is
// clean.
fn parallel_merge(
    spec: &ParSpec,
    leader: &[TupleHashEntryData],
    tables: &[HandedAggTable],
    parts: &[Partition],
    additionalsize: usize,
) -> PgResult<Option<Vec<Vec<TupleHashEntryData>>>> {
    let ncols = spec.atts.len();
    if spec.has_varlena {
        let mut values = vec![Datum::null(); ncols];
        let mut isnull = vec![false; ncols];
        let mut check = |entries: &[TupleHashEntryData]| -> bool {
            for e in entries {
                // SAFETY: live entry images under the KeyAtt plan (as the
                // merge itself).
                unsafe { deform_key_prefix(e.tuple(), &spec.atts, &mut values, &mut isnull) };
                for (i, a) in spec.atts.iter().enumerate() {
                    if !a.memcmp_payload || isnull[i] {
                        continue;
                    }
                    let p = values[i].as_usize() as *const u8;
                    // SAFETY: non-null varlena datum in a live image.
                    let plain = unsafe {
                        (varatt_is_1b(p) && !varatt_is_1b_e(p)) || varatt_is_4b_u(p)
                    };
                    if !plain {
                        return false;
                    }
                }
            }
            true
        };
        if !check(leader) || !tables.iter().all(|t| check(&t.entries)) {
            if merge_stats_enabled() {
                eprintln!("AGG_MERGE_STATS parallel-fallback: non-plain varlena key");
            }
            return Ok(None);
        }
    }

    let nthreads = tables.len();
    let claimers = nthreads + 1;
    let ctx = ParCtx {
        spec,
        leader,
        tables,
        parts,
        additionalsize,
        next: AtomicUsize::new(0),
        barrier: Barrier::new(claimers),
        out: (0..256).map(|_| UnsafeCell::new(Vec::new())).collect(),
    };
    let mut claims: Vec<usize> = Vec::with_capacity(claimers);
    let mut first_err: Option<Box<PgError>> = None;
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..nthreads)
            .map(|_| {
                scope.spawn(|| {
                    let mut s = ParScratch::new(ncols);
                    claim_loop(&ctx, &mut s)
                })
            })
            .collect();
        let leader_res = {
            let mut s = ParScratch::new(ncols);
            claim_loop(&ctx, &mut s)
        };
        for res in core::iter::once(leader_res)
            .chain(handles.into_iter().map(|h| h.join().expect("merge claimer panicked")))
        {
            match res {
                Ok(n) => claims.push(n),
                Err(e) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
        }
    });
    if let Some(e) = first_err {
        return Err(e);
    }
    let pre: Vec<Vec<TupleHashEntryData>> =
        ctx.out.into_iter().map(UnsafeCell::into_inner).collect();
    if merge_stats_enabled() {
        eprintln!(
            "AGG_MERGE_STATS parallel: claimers={} claims={:?} groups={}",
            claimers,
            claims,
            pre.iter().map(Vec::len).sum::<usize>(),
        );
    }
    Ok(Some(pre))
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
