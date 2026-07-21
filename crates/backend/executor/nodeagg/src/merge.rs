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
use ::heaptuple::{heap_compute_data_size, heap_fill_tuple};
use ::types_tuple::{
    TupleDescData, BITMAPLEN, HEAP_HASNULL, MAXALIGN, MINIMAL_TUPLE_OFFSET,
    SizeofMinimalTupleHeader,
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
pub(crate) const COMBINE_WHITELIST: &[Oid] = &[
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

// Byref non-internal combine kinds (q29coded lane, merge-face increment):
// int4_avg_combine 3324 over the _int8[2] {count,sum} transarray (shared by
// avg/sum(int2) and avg/sum(int4)) and text_larger 458 / text_smaller 459
// over a text transvalue (min/max(text), memcmp-tier collations only — the
// admission gate). Both states are plain inline varlena images: the install
// relocates them into the handed buffer like the internal kinds, the serial
// bucket merge combines them through the production fmgr entry points, and
// the bucket-parallel merge runs them natively (pure adds / memcmp+len
// pick-pointer). Before this, these combines refused `init_finalize_merge`
// entirely — long-text-shape partials (avg(length(text)) + min(text)) fell to
// the classic tuple-queue + leader-re-hash path, THE measured combine wall.
const COMBINE_INT4_AVG: Oid = 3324;
const F_TEXT_LARGER: Oid = 458;
const F_TEXT_SMALLER: Oid = 459;
const INT8ARRAYOID: Oid = 1016;
const TEXTOID: Oid = 25;

/// `PGRUST_AGG_MERGE_BYREF=0|off` kill switch for the byref non-internal
/// combine kinds (AvgInt8Array / VarlenaMinMax): off restores the historical
/// refusal — affected shapes keep the classic tuple-queue merge.
fn merge_byref_kinds_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_AGG_MERGE_BYREF").as_deref(), Ok("0") | Ok("off"))
    })
}

// How the merge owns and combines one transno's state (per the whitelists).
#[derive(Clone, Copy, PartialEq, Debug)]
pub(crate) enum CombineKind {
    Byval,
    PolyInt128 { sum_x2: bool },
    NumericAgg { sum_x2: bool },
    /// int4_avg_combine over the _int8[2] transarray (fixed 40-byte 4B-U
    /// image after relocation; payload at ARR_OVERHEAD_NONULLS_1).
    AvgInt8Array,
    /// text_smaller/text_larger over a text transvalue (admitted under
    /// memcmp-tier collations only, so the native compare is memcmp + length
    /// tiebreak — `varstrfastcmp_c` — and text ties are byte-equal, making
    /// merge order unobservable). bpchar is excluded (its trailing-blank tie
    /// survivors differ by padding).
    VarlenaMinMax { larger: bool },
}

// One worker table, self-contained: `buf` owns the [pergroups][tuple][states]
// images the entries point into — byval transvalues ride in the pergroups;
// internal-transtype states are relocated behind them and the copied
// pergroups repointed (u128 backing keeps Int128AggState's alignment).
pub struct HandedAggTable {
    entries: Vec<TupleHashEntryData>,
    additionalsize: usize,
    // Stage-4 pool: radix partition (top-8 hash bits) computed on the WORKER
    // thread at install when the lane pool is armed, so the leader's consume
    // boundary starts bucket-claiming immediately instead of running T
    // serial counting sorts over O(T·G) entries first (the Stage-0.4
    // prototype's merge-wall correction). None = leader partitions (classic).
    parts: Option<Partition>,
    _buf: Vec<u128>,
}

// SAFETY: entries point only into the struct's own heap buffer (stable across
// moves); install/take hand ownership through the handoff Mutex and the
// installer never touches the payload again.
unsafe impl Send for HandedAggTable {}

// Raw (columnar) handed table — the Stage-4 §4.4 exchange's wire format for
// single-int-key compact shapes. Rows are radix-partitioned AT INSTALL by the
// C kernel hash's top-8 bits (the SAME bucket function `partition_entries`
// uses on classic entries, so mixed classic/raw sources stay bucket-
// consistent): bucket b's keys/state-blocks are the contiguous runs
// starts[b]..starts[b+1] — the merge streams them sequentially instead of
// pointer-chasing minimal-tuple images (the tuple-format exchange measured
// ~200ns/entry on the merge, DRAM-bound; this format is the Stage-0.4
// prototype's flat-merge shape).
pub struct HandedRawTable {
    // 257 bucket offsets over keys/states (non-NULL rows only).
    starts: Vec<u32>,
    // Canonical sign-extended i64 keys (the compact table's own repr).
    keys: Vec<i64>,
    // One `additionalsize`-byte pergroup block per key, 16-aligned stride.
    states: Vec<u128>,
    stride16: usize,
    // Relocated internal (PolyInt128) states the copied pergroups point into.
    _extra: Vec<u128>,
    // The out-of-band NULL group's pergroup block (compact null_row).
    null_states: Option<Vec<u128>>,
}

// SAFETY: self-contained buffers; install/take hand ownership through the
// handoff Mutex and the installer never touches the payload again.
unsafe impl Send for HandedRawTable {}

#[derive(Default)]
struct HandoffSlots {
    classic: Vec<HandedAggTable>,
    raw: Vec<HandedRawTable>,
}

pub struct AggTableHandoff {
    slots: Mutex<HandoffSlots>,
    // Leader-decided per-transno state plan; the worker install relocates
    // internal states by it. Immutable after construction.
    kinds: Vec<CombineKind>,
    // Stage-4 §4.4 radix exchange: Some(cap) = the leader admitted the
    // high-NDV exchange for this shape (bucket-parallel merge qualified on a
    // single fixed int grouping key + pool armed + plan NDV over the floor),
    // and workers bound their compact partial tables at `cap` entries,
    // installing raw radix-partitioned flushes mid-fill. None = classic
    // one-table-per-worker handoff. Immutable after construction (workers
    // read it through the registry).
    exchange_cap: Option<u32>,
}

impl AggTableHandoff {
    fn new(kinds: Vec<CombineKind>, exchange_cap: Option<u32>) -> AggTableHandoff {
        AggTableHandoff { slots: Mutex::new(HandoffSlots::default()), kinds, exchange_cap }
    }

    fn install(&self, t: HandedAggTable) {
        self.slots.lock().unwrap_or_else(|e| e.into_inner()).classic.push(t);
    }

    fn install_raw(&self, t: HandedRawTable) {
        self.slots.lock().unwrap_or_else(|e| e.into_inner()).raw.push(t);
    }

    fn take_all(&self) -> (Vec<HandedAggTable>, Vec<HandedRawTable>) {
        let mut s = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        (core::mem::take(&mut s.classic), core::mem::take(&mut s.raw))
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
    // Stage-4 §4.4 raw exchange: the handed raw tables (buffer owners) and
    // the flat per-bucket merge output; retrieval drains buckets in order,
    // rows in insertion order, synthesizing key datums directly (no minimal
    // tuples). `raw_pre` present ⇒ `pre` absent and retrieval takes the raw
    // leg; the classic fields still cover mixed classic sources (merged
    // INTO raw_pre at consume).
    raw_tables: Vec<HandedRawTable>,
    raw_pre: Option<Vec<::lanetable::LaneAggTable>>,
    // The single grouping key's attlen for datum synthesis (raw leg only).
    raw_key_len: i16,
    // Parallel-finalize prepared emit: per-bucket fully-projected rows built
    // by the merge claimers. Present ⇒ raw_pre present; retrieval drains
    // these instead of running the per-group finalize/project tail (same
    // bucket order, same within-bucket order — identical output order).
    emit_pre: Option<Vec<EmitBuf>>,
    emit_natts: usize,
}

// PGRUST_AGG_MERGE_NO_PARALLEL triage kill-switch: forces the increment-1
// serial bucket merge on otherwise parallel-qualified shapes.
fn parallel_merge_disabled() -> bool {
    static OFF: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *OFF.get_or_init(|| std::env::var_os("PGRUST_AGG_MERGE_NO_PARALLEL").is_some())
}

// PGRUST_AGG_FINALIZE_NO_PARALLEL triage kill-switch: forces the serial
// per-group finalize/project retrieve on otherwise emit-qualified raw shapes.
fn parallel_finalize_disabled() -> bool {
    static OFF: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *OFF.get_or_init(|| std::env::var_os("PGRUST_AGG_FINALIZE_NO_PARALLEL").is_some())
}

// --- Parallel finalize + prepared emit (the serial-emit tail, ratified
// order-relaxation lane) ---
//
// After the raw exchange merge, groups live in 256 disjoint per-bucket
// LaneAggTables — finalize is embarrassingly parallel by construction; only
// the leader's emit ordering forced serialization (the radixexchange lane's
// measured ~1.0s tail at 10M groups: per-group finalize_aggregates +
// exec_project + slot churn on one thread). When the shape qualifies —
// finalize is the identity (no finalfn, byval transtype: count/sum/min/max
// over ints), no HAVING qual, and the projection is a pure column shuffle of
// {the single grouping key, Aggref results} — each merge claimer finalizes
// its buckets in the SAME parallel pass that merged them, materializing
// fully-projected rows (all byval datums, self-contained) into per-bucket
// EmitBufs. The leader's drain then memcpys row datums into the result slot:
// no finalize, no projection interpreter, no per-row expr-context reset.
// Emit order is bucket 0..255, insertion order within a bucket — byte-
// identical to the serial raw retrieve's order, so this lane changes NO
// observable ordering at all. Disqualified shapes (finalfns, HAVING,
// expressions over aggregates) keep the serial per-group retrieve unchanged.

// One output column of the prepared-emit projection.
#[derive(Clone, Copy)]
enum EmitCol {
    // The single raw grouping key (NULL for the null group's row).
    Key,
    // Aggregate result = the byval transvalue of this transno (no finalfn).
    Agg { transno: u32 },
}

struct EmitPlan {
    cols: Vec<EmitCol>,
}

// One bucket's fully-projected output rows, row-major with stride
// cols.len(). Byval datums only (the plan's qualification) — self-contained
// across threads.
#[derive(Default)]
struct EmitBuf {
    values: Vec<Datum>,
    nulls: Vec<bool>,
    nrows: usize,
}

// The emit-lane qualification (leader, consume boundary). None = the serial
// per-group finalize/project retrieve runs unchanged.
fn build_emit_plan(node: &AggStateData<'_>) -> Option<EmitPlan> {
    if parallel_finalize_disabled() {
        return None;
    }
    // HAVING quals and finalize skipping keep the classic per-group path.
    if node.skip_final || node.qual.is_some() {
        return None;
    }
    // Identity finalize only: no finalfn (result IS the transvalue), no
    // direct args (ordered-set aggs can't be AGG_HASHED anyway), byval
    // transtype so the handed datum is self-contained and no expanded-object
    // read-only wrap applies. The raw exchange's combine whitelist already
    // bounds this to int/bool/date-time count/sum/min/max shapes.
    for pa in node.peragg.iter() {
        if pa.finalfn.is_some()
            || !pa.direct_args.is_empty()
            || !node.trans_typ[pa.transno as usize].byval
        {
            return None;
        }
    }
    let ph = node.perhash.as_ref()?;
    let key_attno = *ph.hash_grp_col_idx_input.first()?;
    let tlist = &node.plan.plan.targetlist;
    let mut cols = Vec::with_capacity(tlist.len());
    for n in tlist.iter() {
        let te = n.as_target_entry()?;
        if let Some(v) = te.expr.as_var() {
            // The projection evaluates over first_slot (outer), where the
            // retrieve places the key at hash_grp_col_idx_input[0]; any
            // other Var would read an unset column — disqualify.
            if v.varno == ::types_nodes::primnodes::OUTER_VAR && v.varattno == key_attno {
                cols.push(EmitCol::Key);
                continue;
            }
            return None;
        }
        if let Some(a) = te.expr.as_aggref() {
            if a.aggno < 0 || a.aggno as usize >= node.peragg.len() {
                return None;
            }
            cols.push(EmitCol::Agg { transno: node.peragg[a.aggno as usize].transno });
            continue;
        }
        // Expressions over aggregates/keys keep the projection interpreter.
        return None;
    }
    Some(EmitPlan { cols })
}

// One bucket's finalize+project, thread-native: rows in the merge table's
// insertion order (the serial retrieve's exact order), columns per the plan.
fn emit_bucket(plan: &EmitPlan, key_len: i16, t: &::lanetable::LaneAggTable) -> EmitBuf {
    let natts = plan.cols.len();
    let n = t.nrows();
    let mut values: Vec<Datum> = Vec::with_capacity(n * natts);
    let mut nulls: Vec<bool> = Vec::with_capacity(n * natts);
    for row in 0..n {
        let key = t.row_key_int(row);
        let states = t.row_states(row).cast_const().cast::<AggPerGroup>();
        for c in &plan.cols {
            match *c {
                EmitCol::Key => match key {
                    Some(k) => {
                        values.push(raw_key_datum(key_len, k));
                        nulls.push(false);
                    }
                    None => {
                        values.push(Datum::null());
                        nulls.push(true);
                    }
                },
                // SAFETY: the row's state block holds numtrans pergroups
                // (table config = additionalsize = the finalize's pergroup
                // array); transno < numtrans by construction. Byval
                // transvalues only (qualification), so the datum copy is
                // self-contained.
                EmitCol::Agg { transno } => unsafe {
                    let pg = &*states.add(transno as usize);
                    values.push(pg.trans_value);
                    nulls.push(pg.trans_value_is_null);
                },
            }
        }
    }
    EmitBuf { values, nulls, nrows: n }
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
        } else if trans_fnoid[t] == COMBINE_INT4_AVG
            && aggref.aggtranstype == INT8ARRAYOID
            && merge_byref_kinds_enabled()
        {
            // avg/sum(int2|int4): the {count,sum} transarray — element adds,
            // reassociation-invariant like the byval int sums.
            CombineKind::AvgInt8Array
        } else if matches!(trans_fnoid[t], F_TEXT_SMALLER | F_TEXT_LARGER)
            && aggref.aggtranstype == TEXTOID
            && ::lanefold::str_collation_safe(aggref.inputcollid)
            && merge_byref_kinds_enabled()
        {
            // min/max(text) under a memcmp-tier collation: the comparison is
            // memcmp + length tiebreak, ties are byte-equal, so any combine
            // order yields byte-identical survivors. Non-memcmp collations
            // refuse (locale order in worker threads + tie identity are not
            // in this increment's proof).
            CombineKind::VarlenaMinMax { larger: trans_fnoid[t] == F_TEXT_LARGER }
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

    // Stage-4 §4.4 exchange admission (leader-decided, workers read it off
    // the handoff): requires the bucket-parallel merge (a serial leader-side
    // merge over cap-flushed O(N) entries would be a regression), the RAW
    // wire shape — exactly one fixed-width int grouping key of a compact-
    // hostable width, so worker flushes stream canonical i64 keys + state
    // blocks and the merge is flat (the tuple-format exchange measured
    // ~200ns/entry, DRAM-bound pointer chasing) — and the NDV floor over the
    // finalize's plan-time group estimate (footer-HLL-honest on never-
    // ANALYZEd pgrcolumnar since the cbparallelstats lane). Low-NDV shapes keep
    // classic behavior byte-for-byte (their tables never reach the cap even
    // when admitted, but the floor keeps the planner and executor aligned).
    let raw_shape = par.as_ref().is_some_and(|spec| {
        spec.atts.len() == 1
            && !spec.atts[0].memcmp_payload
            && matches!(spec.atts[0].attlen, 2 | 4 | 8)
    });
    let exchange_cap = (raw_shape
        && !parallel_merge_disabled()
        && ::guc_tables::lane_pool::agg_exchange_admits(node.numGroups as f64))
    .then(|| ::guc_tables::lane_pool::agg_exchange_cap() as u32);
    if merge_stats_enabled() {
        eprintln!(
            "AGG_MERGE_STATS engage: node={} kinds={:?} par={} exchange={:?}",
            node.plan.plan_node_id,
            kinds,
            par.is_some(),
            exchange_cap,
        );
    }
    let handoff = Arc::new(AggTableHandoff::new(kinds.clone(), exchange_cap));
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
    // SAFETY: caller contract.
    unsafe { states_extra_bytes(add.as_ptr().cast::<AggPerGroup>(), kinds) }
}

// `entry_state_bytes` over a bare pergroup array (the compact-table rows'
// state blocks carry the same `kinds.len()` pergroups).
//
// # Safety
// `pg` points at `kinds.len()` live pergroups with live state pointers.
unsafe fn states_extra_bytes(pg: *const AggPerGroup, kinds: &[CombineKind]) -> usize {
    let mut bytes = 0usize;
    for (transno, k) in kinds.iter().enumerate() {
        // SAFETY: caller contract — pg holds kinds.len() pergroups.
        let s = unsafe { &*pg.add(transno) };
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
            CombineKind::AvgInt8Array | CombineKind::VarlenaMinMax { .. } => {
                // SAFETY: non-null byref transvalue is a live plain inline
                // varlena image (transition/combine outputs are never
                // toasted; the relocation below asserts the form).
                align16(unsafe { varsize_any(s.trans_value.as_usize() as *const u8) })
            }
        };
    }
    bytes
}

// Relocate one handed image's internal-transtype states behind it: for each
// non-null non-byval pergroup at `dst` (the image's already-copied pergroup
// prefix), copy the state into `base + off` (16-aligned slots off the u128
// backing) and repoint the copied pergroup — after this the image references
// nothing owner-arena-backed. Returns the advanced `off`.
//
// # Safety
// `dst` holds `kinds.len()` copied pergroups whose non-null internal
// transvalues point at live source states; `base + off ..` has
// `states_extra_bytes` bytes reserved for exactly these states.
unsafe fn relocate_states_into(
    dst: *mut u8,
    kinds: &[CombineKind],
    base: *mut u8,
    mut off: usize,
) -> usize {
    for (transno, k) in kinds.iter().enumerate() {
        if matches!(k, CombineKind::Byval) {
            continue;
        }
        // SAFETY: caller contract throughout.
        unsafe {
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
                    let digits = state.add(core::mem::size_of::<NumericAggState>()).cast::<i32>();
                    state.cast::<NumericAggState>().write(sp.relocated_into(digits));
                    off += align16(core::mem::size_of::<NumericAggState>() + sp.digits_bytes());
                }
                CombineKind::AvgInt8Array | CombineKind::VarlenaMinMax { .. } => {
                    // Verbatim image copy into the 16-aligned slot. The
                    // sources are plain inline images by construction: the
                    // avg transarray is the accum family's 4B-U MAXALIGNed
                    // aggcontext image; text min/max transvalues are
                    // ExecAggCopyTransValue datumCopies of detoasted inputs
                    // (short or 4B-U — never compressed/external).
                    let sp = pg.trans_value.as_usize() as *const u8;
                    debug_assert!(
                        varatt_is_1b(sp) || varatt_is_4b_u(sp),
                        "handed byref transvalue must be a plain inline varlena"
                    );
                    debug_assert!(
                        !matches!(k, CombineKind::AvgInt8Array) || varatt_is_4b_u(sp),
                        "avg transarray images are 4B-U MAXALIGNed"
                    );
                    let len = varsize_any(sp);
                    core::ptr::copy_nonoverlapping(sp, state, len);
                    off += align16(len);
                }
            }
            pg.trans_value = Datum::from_usize(state as usize);
        }
    }
    off
}

// Worker-side install at fill completion: the leader registered a handoff for
// this plan node iff the shape is engaged. A spilled table keeps the classic
// row emission (its groups already went partly to tape). A compact-armed
// build (Stage 2.2 × Stage 4) exports the compact table directly.
pub(crate) fn maybe_install_handoff<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if node.plan.aggsplit != AGGSPLIT_INITIAL_SERIAL || node.plan.aggstrategy != AGG_HASHED {
        return Ok(());
    }
    let id = node.plan.plan.plan_node_id;
    let Some(handoff) = registry_get(node.plan as *const Agg<'_> as usize) else {
        return Ok(());
    };
    {
        let ph = node.perhash.as_ref().expect("hashed Agg has perhash");
        if ph.spill.ever_spilled || !ph.spill.batches.is_empty() {
            return Ok(());
        }
        if ph.compact.is_some() {
            // Exchange-mode builds hand their remainder in the raw wire
            // format (matching their mid-fill flushes); everything else
            // keeps the tuple export.
            if matches!(ph.exchange, ExchangeState::On { .. }) {
                return install_raw_handoff(node, estate, &handoff, id, false).map(|_| ());
            }
            return install_compact_handoff(node, estate, &handoff, id);
        }
    }
    install_classic_handoff(node, &handoff, id)
}

// The classic C-table install body (fill-completion handoff).
fn install_classic_handoff<'mcx>(
    node: &mut AggStateData<'mcx>,
    handoff: &Arc<AggTableHandoff>,
    id: i32,
) -> PgResult<()> {
    let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
    let handed = export_handed_table(
        &ph.hashtable,
        &handoff.kinds,
        id,
        // Stage-4 pool: pre-partition on the worker thread (parallel across
        // workers) so the leader's merge boundary is bucket-claim-ready.
        // Armed sessions only — unarmed heap parallel agg keeps the classic
        // leader-side partitioning byte-for-byte.
        ::guc_tables::lane_pool::lane_parallel_pool_armed(),
    );
    handoff.install(handed);
    ph.hashtable.reset();
    Ok(())
}

// The classic install's entry-export core (also the byref-merge-invariant
// unit's surface): copy every live entry's [pergroups][tuple][states] image
// into a self-contained buffer, relocate internal-transtype states, and
// REBASE each copied hash onto the leader's IV=0 mapping. The rebase is the
// r3 defect fix: participant tables are built with per-worker variable hash
// IVs (execGrouping.c parity, the q18fin stall fix), but the finalize's
// bucket merge compares STORED hashes across participant tables AND its own
// IV=0 table — same key, different IV => different hash/bucket => equal keys
// never match => duplicate finalize groups (t26 q18fin-t26-r2 re-earn
// verdict: 5 copies of every group under 4 workers + leader). C never
// compares cross-participant hashes (tuple funnel + leader re-hash per ROW);
// this boundary normalizes at per-GROUP cost on the worker thread —
// `TupleHashTable::hash_to_iv0` is a closed-form O(1) rebase, not a key
// re-hash. The LIVE table keeps its own-IV hashes (bucket indexes depend on
// them); only the handed copies are restamped.
pub(crate) fn export_handed_table(
    table: &::execgrouping::TupleHashTable<'_>,
    kinds: &[CombineKind],
    id: i32,
    pre_partition: bool,
) -> HandedAggTable {
    let additionalsize = table.additionalsize();
    let src = table.entries();
    let mut bytes = 0usize;
    for e in src {
        // SAFETY: entry images are live table_ctx allocations led by t_len;
        // pergroup state pointers live in the worker's agg arenas until the
        // caller's reset.
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
        let mut e2 = unsafe {
            let t_len = (*e.tuple().as_ptr()).t_len as usize;
            let img = e.tuple().as_ptr().cast::<u8>().sub(additionalsize);
            let dst = base.add(off);
            core::ptr::copy_nonoverlapping(img, dst, additionalsize + t_len);
            off += (additionalsize + t_len + 15) & !15;
            off = relocate_states_into(dst, kinds, base, off);
            let mut e2 = *e;
            // Verbatim image copy: relocate through the table so a by-ref
            // cached key (Text probe kernel) is rebased, not left dangling.
            table.relocate_entry(
                &mut e2,
                NonNull::new_unchecked(dst.add(additionalsize).cast::<MinimalTupleData>()),
            );
            e2
        };
        e2.set_hash(table.hash_to_iv0(e2.hash()));
        entries.push(e2);
    }
    let parts = pre_partition.then(|| partition_entries(&entries));
    if merge_stats_enabled() {
        eprintln!(
            "AGG_MERGE_STATS install: node={id} entries={} bytes={bytes} pre-partitioned={}",
            entries.len(),
            parts.is_some(),
        );
    }
    HandedAggTable { entries, additionalsize, parts, _buf: buf }
}

#[cfg(test)]
impl HandedAggTable {
    /// Unit view of the handed entries (byref-merge hash-comparability
    /// invariant tests).
    pub(crate) fn entries(&self) -> &[TupleHashEntryData] {
        &self.entries
    }
}

// The compact table's handoff export (Stage 2.2 × Stage 4, the G4
// groupby_high blocker): a compact-armed partial build hands its groups to
// the finalize WITHOUT rebuilding the C tuplehash. Per row — insertion
// order, the same first-arrival order the C entries vec would carry —
// reconstruct the key datums (the migration walk's read-back leg), hash
// them through the SAME kernel `hash_slot` classic worker entries used (the
// compact table's internal CRC/Fmix hash never crosses the boundary —
// tableresidual semantics check), form the hash_desc minimal tuple directly
// into the handed buffer at the classic install's image layout
// (`[pergroups][tuple][relocated internal states]`, heap_form_minimal_tuple's
// body over pre-zeroed bytes), and assemble the entry with
// `TupleHashEntryData::from_parts`. Transvalue bytes are the compact rows'
// live pergroups, copied verbatim — the same states the C path would have
// handed. The table is consumed (disarmed) by the export: retrieve finds
// compact=None over an empty C table and emits nothing, exactly like the
// classic install's `hashtable.reset()`.
fn install_compact_handoff<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    handoff: &Arc<AggTableHandoff>,
    id: i32,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let kinds = &handoff.kinds;
    let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
    debug_assert!(!ph.spill.mode, "compact builds never enter spill mode");
    let additionalsize = ph.hashtable.additionalsize();
    let ch = ph.compact.take().expect("compact install requires an armed table");
    let nrows = ch.table.nrows();
    let desc = ph
        .hashslot
        .base()
        .tts_tupleDescriptor
        .clone()
        .expect("perhash hashslot carries the hash desc");
    let natts = desc.natts as usize;
    let mut mk_scratch: Vec<(Datum, bool)> = Vec::new();
    // Pass 1: exact byte total + per-row tuple lengths (entries hold raw
    // pointers into the handed buffer, so it cannot grow). Interned text
    // components materialize per pass into the node-lifetime table context
    // (mk+dict shapes only; two small allocations per such GROUP, bulk-freed
    // with the node — docs/no-drop.md).
    let mut tlens: Vec<u32> = Vec::with_capacity(nrows);
    let mut bytes = 0usize;
    for row in 0..nrows {
        crate::compact::compact_row_into_hashslot(
            &ch,
            &mut ph.hashslot,
            &mut mk_scratch,
            row,
            ph.table_ctx.mcx(),
            mcx,
        )?;
        let sb = ph.hashslot.base();
        let hasnull = sb.tts_isnull[..natts].contains(&true);
        let mut hlen = SizeofMinimalTupleHeader;
        if hasnull {
            hlen += BITMAPLEN(natts as i32) as usize;
        }
        let t_len =
            MAXALIGN(hlen) + heap_compute_data_size(&desc, &sb.tts_values, &sb.tts_isnull);
        tlens.push(t_len as u32);
        bytes += (additionalsize + t_len + 15) & !15;
        // SAFETY: the row's state block holds kinds.len() live pergroups
        // (compact build contract; numtrans matches the engagement proof).
        bytes +=
            unsafe { states_extra_bytes(ch.table.row_states(row).cast_const().cast(), kinds) };
    }
    let mut buf: Vec<u128> = vec![0; bytes.div_ceil(16)];
    let mut entries: Vec<TupleHashEntryData> = Vec::with_capacity(nrows);
    let base = buf.as_mut_ptr().cast::<u8>();
    let mut off = 0usize;
    for row in 0..nrows {
        crate::compact::compact_row_into_hashslot(
            &ch,
            &mut ph.hashslot,
            &mut mk_scratch,
            row,
            ph.table_ctx.mcx(),
            mcx,
        )?;
        let hash = ph.hashtable.hash_slot(&mut ph.hashslot)?;
        // Rebased onto the leader's IV=0 mapping, exactly like the classic
        // export (export_handed_table's byref-merge invariant note).
        let hash = ph.hashtable.hash_to_iv0(hash);
        let t_len = tlens[row] as usize;
        // SAFETY: pass 1 reserved additionalsize + t_len (+ state bytes) at
        // `off` for exactly this row over the same reconstructed datums; the
        // buffer is pre-zeroed as heap_form_minimal_tuple/heap_fill_tuple
        // require; dst is 16-aligned (off stays 16-aligned) and the tuple at
        // MAXALIGN'd additionalsize is MAXALIGN-aligned.
        let tuple = unsafe {
            let dst = base.add(off);
            core::ptr::copy_nonoverlapping(ch.table.row_states(row), dst, additionalsize);
            let tp = dst.add(additionalsize);
            let sb = ph.hashslot.base();
            let hasnull = sb.tts_isnull[..natts].contains(&true);
            let mut hlen = SizeofMinimalTupleHeader;
            if hasnull {
                hlen += BITMAPLEN(natts as i32) as usize;
            }
            let hoff = MAXALIGN(hlen);
            let mt = &mut *tp.cast::<MinimalTupleData>();
            mt.t_len = t_len as u32;
            mt.set_natts(natts as u16);
            mt.t_hoff = (hoff + MINIMAL_TUPLE_OFFSET) as u8;
            heap_fill_tuple(
                &desc,
                &sb.tts_values,
                &sb.tts_isnull,
                tp.add(hoff),
                t_len - hoff,
                &mut (*tp.cast::<MinimalTupleData>()).t_infomask,
                hasnull.then(|| tp.add(SizeofMinimalTupleHeader)),
            );
            off += (additionalsize + t_len + 15) & !15;
            off = relocate_states_into(dst, kinds, base, off);
            NonNull::new_unchecked(tp.cast::<MinimalTupleData>())
        };
        let (key, key_isnull) = crate::compact::compact_export_entry_key(&ch, row);
        entries.push(TupleHashEntryData::from_parts(tuple, hash, key, key_isnull));
    }
    debug_assert_eq!(off, bytes);
    // Stage-4 pool: pre-partition on the worker thread, exactly like the
    // classic install (compact exports only arise under an armed pool, but
    // the gate keeps the two paths' conditions literally identical).
    let parts =
        ::guc_tables::lane_pool::lane_parallel_pool_armed().then(|| partition_entries(&entries));
    if merge_stats_enabled() {
        eprintln!(
            "AGG_MERGE_STATS install: node={id} entries={} bytes={bytes} pre-partitioned={} src=compact",
            entries.len(),
            parts.is_some(),
        );
    }
    handoff.install(HandedAggTable { entries, additionalsize, parts, _buf: buf });
    Ok(())
}

// The raw (columnar) install — the exchange's wire format, both for the
// mid-fill flushes (`rearm=true`: the emptied table is re-armed and the
// build continues) and the fill-completion remainder (`rearm=false`: the
// table is consumed, exactly like the tuple exports). Per row: canonical
// i64 key + the C kernel hash (bucket routing, consistent with classic
// entries), the pergroup block copied verbatim, internal (PolyInt128)
// states relocated behind it. No minimal tuples anywhere — reconstruction
// was the tuple exchange's measured wall. Returns the handed byte count
// (the exchange budget).
fn install_raw_handoff<'mcx>(
    node: &mut AggStateData<'mcx>,
    _estate: &mut EStateData<'mcx>,
    handoff: &Arc<AggTableHandoff>,
    id: i32,
    rearm: bool,
) -> PgResult<usize> {
    let kinds = &handoff.kinds;
    let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
    debug_assert!(!ph.spill.mode, "raw installs never run in spill mode");
    let additionalsize = ph.hashtable.additionalsize();
    let stride16 = additionalsize.div_ceil(16);
    let mut ch = ph.compact.take().expect("raw install requires an armed table");
    let width = match &ch.key {
        crate::compact::CompactKeySpec::Single { width } => *width,
        // Reduced is unreachable too: raw exchange admission requires the
        // parallel handoff spec to carry exactly ONE key att, and the
        // reduced lane's plan shape always has 2..N grouping keys.
        crate::compact::CompactKeySpec::Multi(_) | crate::compact::CompactKeySpec::Reduced(_) => {
            unreachable!("raw exchange admission requires a single-int-key shape")
        }
    };
    let n = ch.table.nrows();
    // Pass 1: keys, kernel hashes (the classic entries' bucket function —
    // hash_staged == hash_slot, the staged-probe equivalence), per-bucket
    // counts, and the internal-state arena size.
    let mut keys: Vec<i64> = Vec::with_capacity(n);
    let mut key_datums: Vec<Datum> = Vec::with_capacity(n);
    let mut isnull: Vec<bool> = Vec::with_capacity(n);
    let mut extra_bytes = 0usize;
    for row in 0..n {
        match ch.table.row_key_int(row) {
            Some(k) => {
                keys.push(k);
                key_datums.push(match width {
                    2 => Datum::from_i16(k as i16),
                    4 => Datum::from_i32(k as i32),
                    _ => Datum::from_i64(k),
                });
                isnull.push(false);
            }
            None => {
                keys.push(0);
                key_datums.push(Datum::null());
                isnull.push(true);
            }
        }
        // SAFETY: the row's state block holds kinds.len() live pergroups
        // (compact build contract; numtrans matches the engagement proof).
        extra_bytes +=
            unsafe { states_extra_bytes(ch.table.row_states(row).cast_const().cast(), kinds) };
    }
    let mut hashes: Vec<u32> = Vec::new();
    ph.hashtable.hash_staged(&key_datums, &isnull, &mut hashes)?;
    // Rebase the bucket-routing hashes onto the leader's IV=0 mapping,
    // exactly like the classic export (export_handed_table's byref-merge
    // invariant note): the finalize's consume side buckets its OWN entries
    // and the exchange null_hash with its IV=0 kernel.
    if ph.hashtable.has_variable_iv() {
        for h in &mut hashes {
            *h = ph.hashtable.hash_to_iv0(*h);
        }
    }
    drop(key_datums);
    let mut counts = [0u32; 256];
    for i in 0..n {
        if !isnull[i] {
            counts[(hashes[i] >> 24) as usize] += 1;
        }
    }
    let mut starts: Vec<u32> = Vec::with_capacity(257);
    let mut acc = 0u32;
    starts.push(0);
    for c in counts {
        acc += c;
        starts.push(acc);
    }
    let nonnull = acc as usize;
    let mut cursor: Vec<u32> = starts[..256].to_vec();
    let mut out_keys: Vec<i64> = vec![0; nonnull];
    let mut states: Vec<u128> = vec![0; nonnull * stride16];
    let mut extra: Vec<u128> = vec![0; extra_bytes.div_ceil(16)];
    let extra_base = extra.as_mut_ptr().cast::<u8>();
    let mut extra_off = 0usize;
    let mut null_states: Option<Vec<u128>> = None;
    for row in 0..n {
        // SAFETY: dst blocks were sized in pass 1 (stride16 per row, extra
        // arena by states_extra_bytes); source blocks are live compact rows;
        // relocate_states_into repoints the COPIED pergroups only.
        unsafe {
            let dst: *mut u8 = if isnull[row] {
                let block = null_states.get_or_insert_with(|| vec![0u128; stride16]);
                block.as_mut_ptr().cast()
            } else {
                let b = (hashes[row] >> 24) as usize;
                let slot = cursor[b] as usize;
                cursor[b] += 1;
                out_keys[slot] = keys[row];
                states.as_mut_ptr().add(slot * stride16).cast()
            };
            core::ptr::copy_nonoverlapping(ch.table.row_states(row), dst, additionalsize);
            extra_off = relocate_states_into(dst, kinds, extra_base, extra_off);
        }
    }
    debug_assert_eq!(extra_off, extra_bytes);
    let bytes = nonnull * (8 + stride16 * 16) + extra_bytes;
    if merge_stats_enabled() {
        eprintln!(
            "AGG_MERGE_STATS install: node={id} entries={n} bytes={bytes} pre-partitioned=true src=raw flush={rearm}",
        );
    }
    handoff.install_raw(HandedRawTable {
        starts,
        keys: out_keys,
        states,
        stride16,
        _extra: extra,
        null_states,
    });
    if rearm {
        // Mid-fill flush: re-arm the SAME table emptied (reset keeps
        // repr/hash/layout/state width; the intern table — mk-only — cannot
        // occur here). The aggcontext keeps the original internal states
        // (no-drop arena); the exchange budget bounds that growth.
        ch.table.reset();
        ph.compact = Some(ch);
    }
    Ok(bytes)
}

// --- Stage-4 §4.4 radix exchange: worker-side bounded partial tables ---
//
// The worker-side half of the exchange (the leader half is the existing
// bucket-claim parallel merge, unchanged): when the leader admitted the
// exchange on the handoff, the partial build bounds its table at `cap`
// entries and, on reaching the bound, installs the table radix-partitioned
// (the classic/compact install bodies verbatim, `flush=true`) and continues
// into the emptied table. Group ownership at the final aggregation is then
// disjoint per bucket claimer, per-worker builds stay cache-resident instead
// of DRAM-random-probing G-sized tables, and the merge's per-bucket probe
// tables are G/256-sized — the Leis'14 radix-partitioned-output shape the
// Stage-0.4 prototype demanded for the O(T·G) merge wall.

// Worker-side exchange runtime, hosted in PerHashData. Resolution is lazy
// (first probe of the build): ExecInitAgg of the participating leader's
// partial node runs before init_finalize_merge registers the handoff.
pub(crate) enum ExchangeState {
    Unresolved,
    Off,
    On {
        handoff: Arc<AggTableHandoff>,
        cap: u32,
        // Handed-buffer bytes this build has installed: the exchange's
        // work_mem discipline (the flushes outlive the table, so the bounded
        // table alone is not the build's footprint). Over the hash_mem limit
        // the exchange turns Off and the classic growth/spill/refusal
        // machinery takes over untouched.
        installed_bytes: usize,
    },
}

#[cold]
#[inline(never)]
fn exchange_resolve(node: &mut AggStateData<'_>) {
    let state = 'r: {
        if node.plan.aggsplit != AGGSPLIT_INITIAL_SERIAL
            || node.plan.aggstrategy != AGG_HASHED
        {
            break 'r ExchangeState::Off;
        }
        let Some(handoff) = registry_get(node.plan as *const Agg<'_> as usize) else {
            break 'r ExchangeState::Off;
        };
        match handoff.exchange_cap {
            Some(cap) => ExchangeState::On { handoff, cap, installed_bytes: 0 },
            None => ExchangeState::Off,
        }
    };
    node.perhash.as_mut().expect("hashed Agg has perhash").exchange = state;
}

/// The exchange bound for this build (compact-arm sizing hook): a bounded
/// table cannot become spill-eligible, so the arm gates/sizes by the cap
/// instead of the full plan-time group estimate.
pub(crate) fn exchange_cap_for_build(node: &mut AggStateData<'_>) -> Option<u32> {
    // M2 sink worker builds (sink.rs): the sink cap plays the exchange cap's
    // role — bounded table, flush at the cap — without any handoff registry.
    if let Some(cap) = node.perhash.as_ref().and_then(|ph| ph.sink_cap) {
        return Some(cap);
    }
    if matches!(
        node.perhash.as_ref().expect("hashed Agg has perhash").exchange,
        ExchangeState::Unresolved
    ) {
        exchange_resolve(node);
    }
    match node.perhash.as_ref().expect("hashed Agg has perhash").exchange {
        ExchangeState::On { cap, .. } => Some(cap),
        _ => None,
    }
}

/// The exchange's bound check — called BEFORE the row/batch probes
/// (`lookup_hash_entry` top; compact backstop), so no caller-held group
/// pointer is ever invalidated by a flush.
#[inline]
pub(crate) fn exchange_maybe_flush<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let ph = node.perhash.as_ref().expect("hashed Agg has perhash");
    match &ph.exchange {
        ExchangeState::Off => Ok(()),
        ExchangeState::Unresolved => {
            // First probe of the build: the table is empty — resolve only.
            exchange_resolve(node);
            Ok(())
        }
        ExchangeState::On { cap, .. } => {
            // Raw flushes stream off the compact table only; a build whose
            // compact arm refused (or migrated away) keeps the classic
            // one-install path — the merge handles mixed sources.
            let over = ph
                .compact
                .as_ref()
                .is_some_and(|ch| ch.table.len() >= *cap as usize);
            if !over {
                return Ok(());
            }
            exchange_flush(node, estate)
        }
    }
}

#[cold]
#[inline(never)]
fn exchange_flush<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let ph = node.perhash.as_ref().expect("hashed Agg has perhash");
    // A spilled build stops exchanging: its remaining groups follow the
    // classic spill/emission path (and the fill-end install refuses).
    // Already-installed flushes stay correct — each flush removed those
    // groups' states from the table, so tape/table/handed contributions are
    // disjoint deltas the finalize's combines add up. (Under the cap the
    // table itself never trips the limits; this arms only when the
    // aggcontext's transvalue growth does.)
    if ph.spill.mode || ph.spill.ever_spilled {
        node.perhash.as_mut().unwrap().exchange = ExchangeState::Off;
        return Ok(());
    }
    let ExchangeState::On { handoff, .. } = &ph.exchange else {
        unreachable!("exchange_flush under ExchangeState::On")
    };
    let handoff = Arc::clone(handoff);
    let hash_mem_limit = ph.hash_mem_limit;
    let id = node.plan.plan.plan_node_id;
    let bytes = install_raw_handoff(node, estate, &handoff, id, true)?;
    let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
    if let ExchangeState::On { installed_bytes, .. } = &mut ph.exchange {
        *installed_bytes += bytes;
        // Budget discipline: handed buffers are this build's real footprint.
        // Over the limit, stop exchanging — the table then grows classically
        // and the existing spill machinery owns the memory bound.
        if *installed_bytes > hash_mem_limit {
            ph.exchange = ExchangeState::Off;
        }
    }
    Ok(())
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
    let (tables, raw_tables) = m.handoff.take_all();
    if tables.is_empty() && raw_tables.is_empty() {
        return Ok(());
    }
    let ph = node.perhash.as_ref().expect("hashed Agg has perhash");
    if merge_stats_enabled() {
        eprintln!(
            "AGG_MERGE_STATS consume: tables={} entries={} raw_tables={} raw_entries={} row_groups={} mode={}",
            tables.len(),
            tables.iter().map(|t| t.entries.len()).sum::<usize>(),
            raw_tables.len(),
            raw_tables.iter().map(|t| t.keys.len()).sum::<usize>(),
            ph.hashtable.entries().len(),
            if ph.spill.ever_spilled { "replay" } else { "bucket-merge" },
        );
    }
    if ph.spill.ever_spilled {
        replay_handed_rows(node, estate, tables)?;
        return replay_raw_rows(node, estate, raw_tables);
    }
    let t0 = merge_stats_enabled().then(std::time::Instant::now);
    let additionalsize = ph.hashtable.additionalsize();
    let mut tables = tables;
    let mut parts = Vec::with_capacity(tables.len() + 1);
    parts.push(partition_entries(ph.hashtable.entries()));
    for t in &mut tables {
        debug_assert!(t.additionalsize == additionalsize);
        // Worker-partitioned handoff (Stage-4 pool): reuse; else partition
        // here, exactly as before.
        parts.push(match t.parts.take() {
            Some(p) => p,
            None => partition_entries(&t.entries),
        });
    }
    if !raw_tables.is_empty() {
        // Raw exchange consume: flat bucket-claim merge over the raw runs +
        // any classic sources (the leader's own row-built table; fallback
        // workers). Raw installs exist only under the exchange admission,
        // which required the parallel-qualified spec.
        let spec = m.par.as_ref().expect("raw handoff implies a parallel-qualified merge");
        let raw_key_len = spec.atts[0].attlen;
        let null_hash = {
            let mut v: Vec<u32> = Vec::new();
            ph.hashtable.hash_staged(&[Datum::null()], &[true], &mut v)?;
            v[0]
        };
        // Parallel-finalize emit plan (order-relaxation lane): built here so
        // the merge claimers finalize+project their buckets in the same pass.
        let emit_plan = build_emit_plan(node);
        let (raw_pre, emit_pre) = parallel_merge_raw(
            spec,
            ph.hashtable.entries(),
            &tables,
            &raw_tables,
            &parts,
            additionalsize,
            null_hash,
            emit_plan.as_ref(),
        )?;
        if let Some(t0) = t0 {
            eprintln!(
                "AGG_MERGE_STATS merge-wall-us={} mode=raw emit={}",
                t0.elapsed().as_micros(),
                if emit_pre.is_some() { "pre" } else { "serial" },
            );
        }
        let emit_natts = emit_plan.map_or(0, |p| p.cols.len());
        node.merge.as_mut().unwrap().run = Some(MergeRun {
            tables,
            parts,
            additionalsize,
            bucket: 0,
            out: Vec::new(),
            out_pos: 0,
            probe: Vec::new(),
            pre: None,
            raw_tables,
            raw_pre: Some(raw_pre),
            raw_key_len,
            emit_pre,
            emit_natts,
        });
        return Ok(());
    }
    let pre = match &m.par {
        Some(spec) if !parallel_merge_disabled() => {
            parallel_merge(spec, ph.hashtable.entries(), &tables, &parts, additionalsize)?
        }
        _ => None,
    };
    if let Some(t0) = t0 {
        // Merge-wall probe for the G4 merge-fraction gate: partition reuse +
        // the bucket-claim merge, leader-observed.
        eprintln!(
            "AGG_MERGE_STATS merge-wall-us={} mode={}",
            t0.elapsed().as_micros(),
            if pre.is_some() { "parallel" } else { "serial-buckets-deferred" },
        );
    }
    node.merge.as_mut().unwrap().run = Some(MergeRun {
        tables,
        parts,
        additionalsize,
        bucket: 0,
        out: Vec::new(),
        out_pos: 0,
        probe: Vec::new(),
        pre,
        raw_tables: Vec::new(),
        raw_pre: None,
        raw_key_len: 0,
        emit_pre: None,
        emit_natts: 0,
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

// The single raw grouping key's datum, from its canonical i64 (the compact
// table's sign-extended storage; compact_key_datum's exact construction).
#[inline]
fn raw_key_datum(attlen: i16, k: i64) -> Datum {
    match attlen {
        2 => Datum::from_i16(k as i16),
        4 => Datum::from_i32(k as i32),
        _ => Datum::from_i64(k),
    }
}

// A classic entry's key datum canonicalized to the raw i64 (the exact
// widths compact canonicalizes — both sides sign-extend identically).
#[inline]
fn raw_canon_key(attlen: i16, d: Datum) -> i64 {
    match attlen {
        2 => d.as_i16() as i64,
        4 => d.as_i32() as i64,
        _ => d.as_i64(),
    }
}

// `replay_handed_rows` for raw handed tables: each raw group re-enters as a
// synthesized partial-output row through the classic fill body.
fn replay_raw_rows<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    tables: Vec<HandedRawTable>,
) -> PgResult<()> {
    if tables.is_empty() {
        return Ok(());
    }
    let mcx = estate.es_query_cxt;
    let mut m = node.merge.take().expect("replay under an engaged merge");
    let key_len =
        m.par.as_ref().expect("raw handoff implies a parallel-qualified merge").atts[0].attlen;
    let key_attno = node
        .perhash
        .as_ref()
        .expect("hashed Agg has perhash")
        .hash_grp_col_idx_input[0];
    let mut result = Ok(());
    let mut state_vals: Vec<NullableDatum> = vec![NullableDatum::null(); m.state_cols.len()];
    'outer: for t in &tables {
        let n = t.keys.len();
        // Rows 0..n, then the out-of-band NULL block (index n).
        for i in 0..=n {
            let (key_datum, key_isnull, pg): (Datum, bool, NonNull<AggPerGroup>) = if i < n {
                let pg = if t.stride16 == 0 {
                    NonNull::dangling()
                } else {
                    // SAFETY: states holds n stride16-blocks (install layout).
                    unsafe {
                        NonNull::new_unchecked(
                            t.states.as_ptr().add(i * t.stride16).cast_mut().cast(),
                        )
                    }
                };
                (raw_key_datum(key_len, t.keys[i]), false, pg)
            } else {
                let Some(block) = &t.null_states else { continue };
                let pg = if t.stride16 == 0 {
                    NonNull::dangling()
                } else {
                    // SAFETY: the NULL block holds one stride16-block.
                    unsafe { NonNull::new_unchecked(block.as_ptr().cast_mut().cast()) }
                };
                (Datum::null(), true, pg)
            };
            // SAFETY: raw blocks hold kinds.len() pergroups whose internal
            // states were relocated into the table's arena at install.
            if let Err(err) = unsafe {
                synth_state_vals_pg(
                    &m.kinds,
                    pg,
                    estate.ecxt(node.tmpcontext).per_tuple_mcx(),
                    &mut state_vals,
                )
            } {
                result = Err(err);
                break 'outer;
            }
            {
                let replay = estate.slot_mut(m.replay_slot);
                exectuples::exec_store_all_null_tuple(replay, mcx);
                let dst = replay.base_mut();
                dst.tts_values[(key_attno - 1) as usize] = key_datum;
                dst.tts_isnull[(key_attno - 1) as usize] = key_isnull;
                for (&attno, v) in m.state_cols.iter().zip(&state_vals) {
                    dst.tts_values[(attno - 1) as usize] = v.value;
                    dst.tts_isnull[(attno - 1) as usize] = v.isnull;
                }
            }
            estate.ecxt_mut(node.tmpcontext).ecxt_outertuple = Some(m.replay_slot);
            match lookup_hash_entry(node, estate, m.replay_slot) {
                Ok(true) => {
                    let replay = estate.slot_mut(m.replay_slot);
                    let mut slots = EvalSlots { scan: None, inner: None, outer: Some(replay) };
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
    // SAFETY: additionalsize holds kinds.len() pergroups (fn contract below).
    unsafe { synth_state_vals_pg(kinds, add.cast::<AggPerGroup>(), per_tuple, out) }
}

// `synth_state_vals` over a bare pergroup block (the raw exchange's replay).
//
// # Safety
// `pg` points at `kinds.len()` live pergroups whose non-null internal
// transvalues are live relocated states.
unsafe fn synth_state_vals_pg(
    kinds: &[CombineKind],
    pg: NonNull<AggPerGroup>,
    per_tuple: Mcx<'_>,
    out: &mut [NullableDatum],
) -> PgResult<()> {
    for (transno, k) in kinds.iter().enumerate() {
        // SAFETY: additionalsize holds kinds.len() pergroups; non-null
        // internal transvalues are live relocated states in the handed
        // buffer (numeric serialization's lazy carry is the only mutation,
        // and each entry replays once).
        out[transno] = unsafe {
            let s = &*pg.as_ptr().add(transno);
            if s.trans_value_is_null
                || matches!(
                    k,
                    CombineKind::Byval
                        | CombineKind::AvgInt8Array
                        | CombineKind::VarlenaMinMax { .. }
                )
            {
                // Byval rides inline; the byref non-internal kinds cross the
                // classic row path as their PLAIN datum (these transtypes
                // have no serialfn) — the handed image outlives the replay.
                NullableDatum { value: s.trans_value, isnull: s.trans_value_is_null }
            } else {
                let mut buf = ::pqformat::pq_begintypsend(per_tuple)?;
                match *k {
                    CombineKind::Byval
                    | CombineKind::AvgInt8Array
                    | CombineKind::VarlenaMinMax { .. } => unreachable!(),
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
    if let Some(r) = node.merge.as_ref().and_then(|m| m.run.as_ref()) {
        if r.emit_pre.is_some() {
            return agg_retrieve_emitted(node, estate);
        }
        if r.raw_pre.is_some() {
            return agg_retrieve_merged_raw(node, estate);
        }
    }
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

// The prepared-emit drain (parallel finalize): rows were fully finalized and
// projected by the merge claimers into per-bucket EmitBufs — the serial path
// is a datum memcpy into the result slot per row. Bucket 0..255, insertion
// order within a bucket: the exact order agg_retrieve_merged_raw emits, so
// engaging this lane changes no observable ordering. No per-row expr-context
// reset: nothing on this path allocates per tuple (byval datums only).
fn agg_retrieve_emitted<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    let mcx = estate.es_query_cxt;
    let next = {
        let run = node
            .merge
            .as_mut()
            .and_then(|m| m.run.as_mut())
            .expect("emitted retrieve under a built run");
        let bufs = run.emit_pre.as_ref().expect("emitted retrieve under a prepared run");
        loop {
            if run.bucket >= 256 {
                break None;
            }
            let b = &bufs[run.bucket];
            if run.out_pos >= b.nrows {
                run.bucket += 1;
                run.out_pos = 0;
                continue;
            }
            let row = run.out_pos;
            run.out_pos += 1;
            break Some((run.bucket, row));
        }
    };
    let Some((bucket, row)) = next else {
        node.agg_done = true;
        return Ok(None);
    };
    let run = node.merge.as_ref().and_then(|m| m.run.as_ref()).unwrap();
    let natts = run.emit_natts;
    let buf = &run.emit_pre.as_ref().unwrap()[bucket];
    let base = row * natts;
    let slot = estate.slot_mut(node.ps_ResultTupleSlot);
    exectuples::exec_clear_tuple(slot, mcx);
    {
        let sb = slot.base_mut();
        sb.tts_values[..natts].copy_from_slice(&buf.values[base..base + natts]);
        sb.tts_isnull[..natts].copy_from_slice(&buf.nulls[base..base + natts]);
    }
    exectuples::exec_store_virtual_tuple(slot);
    Ok(Some(node.ps_ResultTupleSlot))
}

// The raw exchange's retrieve: buckets 0..255 in order, rows in insertion
// order, key datums synthesized straight from the flat merge tables (no
// minimal tuples — the tuple store/deform pair was a measured wall at 10M
// groups). Same finalize/qual/project tail as the classic merged retrieve.
fn agg_retrieve_merged_raw<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    let mcx = estate.es_query_cxt;
    loop {
        estate.reset_expr_context(node.ps_ExprContext);

        let (key, states, key_len) = {
            let run = node
                .merge
                .as_mut()
                .and_then(|m| m.run.as_mut())
                .expect("merged retrieve under a built run");
            let buckets = run.raw_pre.as_ref().expect("raw retrieve under a raw run");
            let next = loop {
                if run.bucket >= 256 {
                    break None;
                }
                let t = &buckets[run.bucket];
                if run.out_pos >= t.nrows() {
                    run.bucket += 1;
                    run.out_pos = 0;
                    continue;
                }
                let row = run.out_pos;
                run.out_pos += 1;
                break Some((t.row_key_int(row), t.row_states(row)));
            };
            let Some((key, states)) = next else {
                node.agg_done = true;
                return Ok(None);
            };
            (key, states, run.raw_key_len)
        };
        // SAFETY: merge-table rows are live for the run; the state block is
        // the kinds.len()-pergroup array the finalize reads.
        let pergroup: NonNull<AggPerGroup> =
            unsafe { NonNull::new_unchecked(states.cast()) };
        {
            let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
            exectuples::exec_store_all_null_tuple(&mut ph.first_slot, mcx);
            if let Some(k) = key {
                let v = (ph.hash_grp_col_idx_input[0] - 1) as usize;
                let dst = ph.first_slot.base_mut();
                dst.tts_values[v] = raw_key_datum(key_len, k);
                dst.tts_isnull[v] = false;
            }
        }
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
    if matches!(c.kind, CombineKind::AvgInt8Array | CombineKind::VarlenaMinMax { .. }) {
        // Native byref arms (both combines are strict): NULL handling is the
        // strict adopt-pointer path (the PolyInt128 precedent — relocated
        // states are owned by the run and consumed exactly once).
        if src.trans_value_is_null {
            return Ok(());
        }
        if dst.trans_value_is_null {
            dst.trans_value = src.trans_value;
            dst.trans_value_is_null = false;
            dst.no_trans_value = false;
            return Ok(());
        }
        match c.kind {
            CombineKind::AvgInt8Array => {
                // int4_avg_combine's element adds, run natively on the
                // relocated 4B-U transarray images (payload at the no-nulls
                // 1-D overhead — the fmgr port's exact layout).
                // SAFETY: relocation copied/asserted 4B-U images into
                // 16-aligned handed slots; dst is uniquely reachable through
                // this claimer's bucket and src feeds exactly once.
                unsafe {
                    let d = (dst.trans_value.as_usize() as *mut u8)
                        .add(::lanefold::ARR_OVERHEAD_NONULLS_1)
                        .cast::<i64>();
                    let s = (src.trans_value.as_usize() as *const u8)
                        .add(::lanefold::ARR_OVERHEAD_NONULLS_1)
                        .cast::<i64>();
                    *d += *s;
                    *d.add(1) += *s.add(1);
                }
            }
            CombineKind::VarlenaMinMax { larger } => {
                // text_smaller/larger's exact pick under a memcmp-tier
                // collation: memcmp + length tiebreak; C returns arg1 (dst)
                // only on a STRICT win, so ties take the src datum — for
                // text, ties are byte-equal, so either pointer is
                // byte-identical output.
                // SAFETY: relocated plain inline images (relocation assert).
                unsafe {
                    let (dp, dl) = var_payload(dst.trans_value.as_usize() as *const u8);
                    let (sp, sl) = var_payload(src.trans_value.as_usize() as *const u8);
                    let cmp = ::varlena::varstrfastcmp_c(
                        core::slice::from_raw_parts(dp, dl),
                        core::slice::from_raw_parts(sp, sl),
                    );
                    let keep_dst = if larger { cmp > 0 } else { cmp < 0 };
                    if !keep_dst {
                        dst.trans_value = src.trans_value;
                    }
                }
            }
            _ => unreachable!(),
        }
        dst.no_trans_value = false;
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
    if total == 0 {
        return Ok(Vec::new());
    }
    // dedupsub reserve wave (vecaudit rider): the merged bucket is bounded
    // by the donors' partition totals, computed above — allocate once.
    let mut out = Vec::with_capacity(total);
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

    // Claimer pool size: one per handed table (the launched worker count on
    // the classic engaged path) plus the leader. Exchange flushes install
    // MANY small tables per worker, so an armed pool clamps to its DOP —
    // spawning a thread per flush would oversubscribe the node.
    let nthreads = match ::guc_tables::lane_pool::lane_parallel_pool_dop() {
        dop if dop > 0 => tables.len().min(dop as usize),
        _ => tables.len(),
    };
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

// --- Raw exchange merge (Stage-4 §4.4) ---
//
// The flat bucket-claim merge over raw handed runs + classic sources: per
// bucket, one claimer streams the raw (key, state-block) runs sequentially
// into a LaneAggTable keyed by the canonical i64 (classic entries deform
// their single key and canonicalize the same way), insert-or-combine with
// the thread-native ParCombine set. This is the Stage-0.4 prototype's merge
// shape — no tuple deform, no per-entry pointer chase.

struct RawParCtx<'a> {
    combines: &'a [ParCombine],
    key_att: KeyAtt,
    additionalsize: usize,
    null_bucket: usize,
    leader: &'a [TupleHashEntryData],
    tables: &'a [HandedAggTable],
    raw: &'a [HandedRawTable],
    parts: &'a [Partition],
    next: AtomicUsize,
    barrier: Barrier,
    // 256 bucket outputs; slot b is written only by the claimer that took b.
    out: Vec<UnsafeCell<::lanetable::LaneAggTable>>,
    // Parallel finalize (order-relaxation lane): Some ⇒ each claimer also
    // materializes bucket b's fully-projected rows into out_emit[b].
    emit: Option<&'a EmitPlan>,
    out_emit: Vec<UnsafeCell<EmitBuf>>,
}

// SAFETY: same argument as ParCtx — shared read-only sources; the only
// mutation is each claimer's exclusive out[b] slot (and out_emit[b], byval
// datums only) plus state payloads that partition by bucket (an entry's
// bucket is a pure function of its kernel hash on both classic and raw
// sides).
unsafe impl Sync for RawParCtx<'_> {}

// Insert-or-combine one source state block into the bucket table.
#[inline]
fn raw_absorb(
    ctx: &RawParCtx<'_>,
    pr: ::lanetable::Probe,
    src: Option<NonNull<u8>>,
) -> PgResult<()> {
    let Some(src) = src else { return Ok(()) };
    if pr.is_new {
        // SAFETY: the row's state block holds additionalsize bytes (table
        // config); src is a live pergroup block of the same layout. Adopted
        // internal-state pointers stay live for the run (handed buffers /
        // leader arenas) and each source block feeds exactly once.
        unsafe {
            core::ptr::copy_nonoverlapping(src.as_ptr(), pr.states, ctx.additionalsize);
        }
        return Ok(());
    }
    for (transno, c) in ctx.combines.iter().enumerate() {
        // SAFETY: both blocks hold numtrans pergroups; dst is uniquely
        // reachable through this claimer's bucket.
        unsafe {
            combine_one_par(
                c,
                &mut *pr.states.cast::<AggPerGroup>().add(transno),
                &*src.as_ptr().cast::<AggPerGroup>().add(transno),
            )?;
        }
    }
    Ok(())
}

fn merge_bucket_raw(ctx: &RawParCtx<'_>, b: usize) -> PgResult<::lanetable::LaneAggTable> {
    let mut total = 0usize;
    for p in ctx.parts {
        total += (p.starts[b + 1] - p.starts[b]) as usize;
    }
    for rt in ctx.raw {
        total += (rt.starts[b + 1] - rt.starts[b]) as usize;
    }
    let mut t = ::lanetable::LaneAggTable::new(
        ::lanetable::KeyRepr::Int,
        ctx.additionalsize,
        total.max(4),
    );
    let mut values = [Datum::null(); 1];
    let mut nulls = [false; 1];
    // Classic sources first (leader = source 0), in install order — the
    // source-major first-seen policy of the tuple merge.
    for (src, part) in ctx.parts.iter().enumerate() {
        let lo = part.starts[b] as usize;
        let hi = part.starts[b + 1] as usize;
        for &eix in &part.idx[lo..hi] {
            let e = if src == 0 {
                ctx.leader[eix as usize]
            } else {
                ctx.tables[src - 1].entries[eix as usize]
            };
            // SAFETY: live entry images under the single-key KeyAtt plan
            // (the raw admission proved the key shape).
            unsafe { deform_key_prefix(e.tuple(), core::slice::from_ref(&ctx.key_att), &mut values, &mut nulls) };
            let pr = if nulls[0] {
                t.probe_null()
            } else {
                let k = raw_canon_key(ctx.key_att.attlen, values[0]);
                t.probe_int(k, t.hash_key_int(k as u64))
            };
            raw_absorb(ctx, pr, e.additional(ctx.additionalsize))?;
        }
    }
    for rt in ctx.raw {
        let lo = rt.starts[b] as usize;
        let hi = rt.starts[b + 1] as usize;
        for i in lo..hi {
            let k = rt.keys[i];
            let pr = t.probe_int(k, t.hash_key_int(k as u64));
            // SAFETY: states holds one stride16 block per key (install
            // layout, alive for the run).
            let src = unsafe {
                NonNull::new_unchecked(rt.states.as_ptr().add(i * rt.stride16).cast_mut().cast::<u8>())
            };
            raw_absorb(ctx, pr, (ctx.additionalsize > 0).then_some(src))?;
        }
        if b == ctx.null_bucket {
            if let Some(block) = &rt.null_states {
                let pr = t.probe_null();
                // SAFETY: the NULL block holds one stride16 block.
                let src = unsafe {
                    NonNull::new_unchecked(block.as_ptr().cast_mut().cast::<u8>())
                };
                raw_absorb(ctx, pr, (ctx.additionalsize > 0).then_some(src))?;
            }
        }
    }
    Ok(t)
}

fn raw_claim_loop(ctx: &RawParCtx<'_>) -> PgResult<usize> {
    ctx.barrier.wait();
    let mut merged = 0usize;
    loop {
        let b = ctx.next.fetch_add(1, Ordering::Relaxed);
        if b >= 256 {
            return Ok(merged);
        }
        let t = merge_bucket_raw(ctx, b)?;
        if t.nrows() > 0 {
            merged += 1;
        }
        if let Some(plan) = ctx.emit {
            // Bucket b is fully merged — finalize+project it on this claimer.
            let buf = emit_bucket(plan, ctx.key_att.attlen, &t);
            // SAFETY: bucket b was claimed exclusively via the counter.
            unsafe { *ctx.out_emit[b].get() = buf };
        }
        // SAFETY: bucket b was claimed exclusively via the counter.
        unsafe { *ctx.out[b].get() = t };
    }
}

#[allow(clippy::too_many_arguments)]
fn parallel_merge_raw(
    spec: &ParSpec,
    leader: &[TupleHashEntryData],
    tables: &[HandedAggTable],
    raw: &[HandedRawTable],
    parts: &[Partition],
    additionalsize: usize,
    null_hash: u32,
    emit: Option<&EmitPlan>,
) -> PgResult<(Vec<::lanetable::LaneAggTable>, Option<Vec<EmitBuf>>)> {
    debug_assert_eq!(spec.atts.len(), 1);
    // Claimer pool sizing: the armed DOP + the leader (raw tables are many
    // small flushes, not one per worker).
    let dop = ::guc_tables::lane_pool::lane_parallel_pool_dop().max(0) as usize;
    let nthreads = raw.len().saturating_add(tables.len()).min(dop.max(1));
    let ctx = RawParCtx {
        combines: &spec.combines,
        key_att: spec.atts[0],
        additionalsize,
        null_bucket: (null_hash >> 24) as usize,
        leader,
        tables,
        raw,
        parts,
        next: AtomicUsize::new(0),
        barrier: Barrier::new(nthreads + 1),
        out: (0..256)
            .map(|_| {
                UnsafeCell::new(::lanetable::LaneAggTable::new(
                    ::lanetable::KeyRepr::Int,
                    additionalsize,
                    4,
                ))
            })
            .collect(),
        emit,
        out_emit: (0..256).map(|_| UnsafeCell::new(EmitBuf::default())).collect(),
    };
    let mut claims: Vec<usize> = Vec::with_capacity(nthreads + 1);
    let mut first_err: Option<Box<PgError>> = None;
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..nthreads)
            .map(|_| scope.spawn(|| raw_claim_loop(&ctx)))
            .collect();
        let leader_res = raw_claim_loop(&ctx);
        for res in core::iter::once(leader_res)
            .chain(handles.into_iter().map(|h| h.join().expect("raw merge claimer panicked")))
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
    let has_emit = ctx.emit.is_some();
    let out_emit = ctx.out_emit;
    let pre: Vec<::lanetable::LaneAggTable> =
        ctx.out.into_iter().map(UnsafeCell::into_inner).collect();
    let emit_pre =
        has_emit.then(|| out_emit.into_iter().map(UnsafeCell::into_inner).collect());
    if merge_stats_enabled() {
        eprintln!(
            "AGG_MERGE_STATS parallel: mode=raw claimers={} claims={:?} groups={} emit={}",
            nthreads + 1,
            claims,
            pre.iter().map(::lanetable::LaneAggTable::nrows).sum::<usize>(),
            if has_emit { "pre" } else { "serial" },
        );
    }
    Ok((pre, emit_pre))
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
