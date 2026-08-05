//! Lane-v2 parallel exact-DISTINCT partials (lane-v2-pardistinct).
//!
//! The planner can never emit a Partial Agg for a DISTINCT aggregate
//! (prepagg hasNonPartialAggs), so the parallel plan for the plain and
//! grouped count(DISTINCT) shapes is always `Agg ← GatherMerge ← Sort ←
//! ParallelSeqScan`: workers sort ALL rows (group prefix + distinct-arg
//! suffix) and the leader deduplicates serially. This module supplies the
//! winning algorithm instead: the LEADER (which owns the Agg node and its
//! admission proofs) registers a build spec keyed by the Sort plan node's
//! address; each WORKER whose fragment top is that Sort skips the sort
//! entirely and drains its share of the shared claim cursor into a compact
//! group table — integer group-key words, a fixed per-transition vocabulary
//! of exact-integer partial states, and one exact-DISTINCT set
//! (`distinctset::DistinctSet`, reused wholesale) per distinct transition —
//! then installs the frozen table through a merge.rs-style handoff and
//! emits ZERO rows. The leader builds its own partial over the local
//! fragment (leader participation), folds any stray rows arriving through
//! the tuple queues (degraded/refused workers), merges the tables
//! (per-partition set union — partitions are disjoint by construction, so
//! the union has no cross-partition work), and emits through the serial
//! arms' unchanged finalize tails.
//!
//! Byte identity: the arm changes (a) which thread deduplicates — sets are
//! exact and their equality is representational (`distinct_set_kind`), (b)
//! the transfn REPLAY ORDER over the identical distinct-value multiset —
//! the admitted transitions are order-insensitive-exact, and (c) the
//! association order of the non-distinct vocabulary states — pure counting
//! and exact integer accumulation, reassociation unobservable. Groups,
//! group order (the plan Sort's prefix), and every projected byte match the
//! serial hashgrouped arm's identity argument.
//!
//! Memory: each worker meters its table (like the hashgrouped arm) and on
//! crossing FREEZES it (within budget), installs it, and degrades the
//! REMAINDER of its share to the classic path — the plan's real Sort is fed
//! the remaining rows and the worker emits them as ordinary sorted rows
//! (pre-freeze rows ride the frozen table, post-freeze rows ride the queue;
//! disjoint, exact). The leader's builder is mcx-backed: crossing evicts
//! the largest sets to the DistinctSet hash-partitioned spill tapes, so
//! leader memory stays budget-bounded and spilled sets replay through the
//! existing spilled-set machinery at finalize.

use std::sync::{Arc, Mutex, Weak};

use ::datum::Datum;
use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::{PgError, PgResult};

use crate::distinctset::{DistinctKeyKind, DistinctSet};

/// splitmix64 finalizer (distinctset.rs's mixer — legal for the same
/// representational-equality reason).
#[inline]
fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// Integer width of a group key / vocab argument column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PdInt {
    I16,
    I32,
    I64,
}

impl PdInt {
    /// Canonicalize a slot/lane datum of this int kind to the sign-extended
    /// key word. `pub`: the vecaccept lane canonicalizes decoded columnar
    /// lanes with the exact per-row read.
    #[inline]
    pub fn read(self, d: Datum) -> i64 {
        match self {
            PdInt::I16 => d.as_i16() as i64,
            PdInt::I32 => d.as_i32() as i64,
            PdInt::I64 => d.as_i64(),
        }
    }
}

/// One group-key component's representation (distinct-bytes car). `Int`
/// stores the sign-extended value in the key word — word equality is the
/// grouping operator's equality. `Bytes` is a text/varchar column under a
/// DETERMINISTIC collation (the CALLER's admission proved
/// `group_eq_representational` texteq — byte equality is the grouping
/// operator's verdict; `pd_derive_spec` only emits it under its
/// `admit_text_keys` flag): the key word packs an `(arena offset << 32) |
/// len` span over the owning table's `key_arena`, and the CANONICAL
/// cross-worker identity is the content bytes themselves (span words are
/// Local-relative and never compared across tables).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PdKeyKind {
    Int(PdInt),
    Bytes,
}

/// Pack an arena span into a key word (the hashgrouped arm's convention).
/// Offsets stay < 4GiB structurally: the arena is metered against the
/// worker budget (well under 4GiB) and a single row overshoots by at most
/// one detoasted value.
#[inline]
fn pack_span(off: usize, len: usize) -> i64 {
    debug_assert!(off <= u32::MAX as usize && len <= u32::MAX as usize);
    (((off as u64) << 32) | len as u64) as i64
}

#[inline]
fn unpack_span(word: i64) -> (usize, usize) {
    ((word as u64 >> 32) as usize, (word as u64 & 0xffff_ffff) as usize)
}

/// Chain a byte string into a running key hash: splitmix64 over 8-byte LE
/// chunks (zero-padded tail) then the length — value-derived (identical
/// across workers, independent of any table-internal state), injective in
/// combination with the fixed component order. The m2-coverage-c3 agg
/// sink's `sink_hash_bytes` discipline.
#[inline]
fn hash_chain_bytes(mut h: u64, b: &[u8]) -> u64 {
    let mut it = b.chunks_exact(8);
    for c in it.by_ref() {
        h = mix64(h ^ u64::from_le_bytes(c.try_into().unwrap()));
    }
    let rem = it.remainder();
    if !rem.is_empty() {
        let mut w = [0u8; 8];
        w[..rem.len()].copy_from_slice(rem);
        h = mix64(h ^ u64::from_le_bytes(w));
    }
    mix64(h ^ (b.len() as u64) ^ 0x5851_f42d_4c95_7f2d)
}

/// One NON-distinct transition in the worker vocabulary. Every vocab state
/// is two i64 words: (acc, count). The kinds mirror the
/// `order_insensitive_exact_transfn` whitelist minus the Int128 family.
#[derive(Clone, Copy, Debug)]
pub enum PdVocabKind {
    /// count(*) — int8inc: acc = row count.
    CountStar,
    /// count(x) — int8inc_any (strict): acc = non-null count.
    CountAny { att: u16 },
    /// sum(int2/int4) — int2_sum/int4_sum: acc = sum, count = non-null
    /// count (state NULL iff count == 0, the non-strict null-initval law).
    SumInt { att: u16, kind: PdInt },
    /// avg(int2/int4) — int2/4_avg_accum: (acc, count) = Int8TransTypeData
    /// {sum, count} with initcond {0,0} (never NULL).
    AvgInt { att: u16, kind: PdInt },
}

/// A vocab entry is keyed by its transno (the pergroup slot the leader
/// rebuilds at emit).
#[derive(Clone, Copy, Debug)]
pub struct PdVocab {
    pub transno: u32,
    pub kind: PdVocabKind,
}

/// One DISTINCT transition: indexed like `pertrans_sort` (the leader
/// installs merged sets back into those slots at emit).
#[derive(Clone, Copy, Debug)]
pub struct PdSetSpec {
    pub(crate) att: u16,
    pub(crate) kind: DistinctKeyKind,
}

/// Element partitions for the plain (nkeys == 0) shape's parallel union.
pub const PD_ELEM_PARTS: usize = 256;
/// Group partitions (top-8 hash bits) for the grouped parallel merge.
const PD_GROUP_PARTS: usize = 256;

/// The leader-derived build recipe workers run. Everything is plain data.
pub struct PdSpec {
    pub key_atts: Vec<u16>,
    pub key_kinds: Vec<PdKeyKind>,
    pub vocab: Vec<PdVocab>,
    pub sets: Vec<PdSetSpec>,
    /// 1 + the largest referenced 0-based attno (slot_getsomeattrs bound).
    pub max_att: i32,
    /// Per-worker build budget (freeze-and-degrade crossing point).
    pub worker_budget: usize,
    /// dedupsub I3: expected post-qual rows ONE worker will accept (plan
    /// rows estimate / dop, set by the runtime sink at engage; 0 = unknown
    /// and the projection reserve is inert). Drives the window-grain
    /// distinct-set table pre-sizing only — never a semantic input.
    pub expected_worker_rows: u64,
}

/// GL-VECACCEPT-1 lane plan: the slot attnos + canonicalization kinds the
/// vectorized whole-granule accept reads directly from decoded columnar
/// lanes (no slot, no per-row deform). Derived from the spec fail-closed
/// (`pd_vec_plan`): exactly one int-family group key, exactly one
/// int-family distinct set (the staged batch-insert shape), and every
/// vocab rider int-family by construction. `None` anywhere = the caller
/// keeps the incumbent per-row accept byte-for-byte.
pub struct PdVecPlan {
    /// The group key's 0-based slot attno + read kind.
    pub key_att: u16,
    pub key_kind: PdInt,
    /// The distinct set's 0-based slot attno + read kind.
    pub set_att: u16,
    pub set_kind: PdInt,
    /// Aligned with `spec.vocab`: `Some((att, kind))` = a value lane to
    /// fold (SumInt/AvgInt); `None` = count-only (no lane read — the
    /// columnar-part no-NULL law makes CountAny a plain row count).
    pub riders: Vec<Option<(u16, PdInt)>>,
}

/// GL-VECACCEPT-1 per-worker scratch (reused across granules — the vec
/// accept's only allocations after warm-up): the batch-hash lane and the
/// resolved group-id lane.
#[derive(Default)]
pub struct PdVecScratch {
    /// Phase-2 output: one resolved group id per lane row.
    pub gids: Vec<u32>,
    /// Phase-1 output (parallel to the key lane).
    hashes: Vec<u64>,
}

/// Derive the vectorized-accept lane plan from a spec (fail-closed; the
/// admission twin of [`PdBuilder::set_batch_insert`]'s shape gate plus the
/// single-int-key gate the batched group resolve requires).
pub fn pd_vec_plan(spec: &PdSpec) -> Option<PdVecPlan> {
    if spec.nkeys() != 1 || spec.sets.len() != 1 {
        return None;
    }
    let PdKeyKind::Int(key_kind) = spec.key_kinds[0] else { return None };
    let set_kind = match spec.sets[0].kind {
        DistinctKeyKind::Int16 => PdInt::I16,
        DistinctKeyKind::Int32 => PdInt::I32,
        DistinctKeyKind::Int64 => PdInt::I64,
        DistinctKeyKind::Bytes => return None,
    };
    let riders = spec
        .vocab
        .iter()
        .map(|v| match v.kind {
            PdVocabKind::CountStar | PdVocabKind::CountAny { .. } => None,
            PdVocabKind::SumInt { att, kind } | PdVocabKind::AvgInt { att, kind } => {
                Some((att, kind))
            }
        })
        .collect();
    Some(PdVecPlan {
        key_att: spec.key_atts[0],
        key_kind,
        set_att: spec.sets[0].att,
        set_kind,
        riders,
    })
}

impl PdSpec {
    #[inline]
    pub fn nkeys(&self) -> usize {
        self.key_atts.len()
    }

    /// Any bytes-kind set (the worker feed then resets its detoast scratch
    /// context per row).
    #[inline]
    pub fn any_bytes_set(&self) -> bool {
        self.sets.iter().any(|s| matches!(s.kind, DistinctKeyKind::Bytes))
    }

    /// Any canonical-bytes GROUP KEY component (distinct-bytes car).
    #[inline]
    pub fn has_bytes_keys(&self) -> bool {
        self.key_kinds.iter().any(|k| matches!(k, PdKeyKind::Bytes))
    }

    /// Bytes anywhere (keys or sets) — the worker feed's per-row detoast
    /// scratch reset gate.
    #[inline]
    pub fn any_bytes(&self) -> bool {
        self.has_bytes_keys() || self.any_bytes_set()
    }

}

// ===========================================================================
// The handoff registry (PdHandoff + the Sort-plan-keyed thread registry +
// the execParallel export/adopt snapshot) was DELETED at Phase-5 D1 with
// the GM-hybrid leader/worker drives (execmain lanev2 pardistinct region).
// The builder/wire/merge machinery below REMAINS: it is the runtime
// distinct sink's substrate (execmain lanev2/runtime_distinct.rs).
// ===========================================================================

// ===========================================================================
// The builder — one participant's partial table.
// ===========================================================================

const INIT_TABLE: usize = 64;

/// batch-insert lane: staged-window size for the batched distinct-set
/// insert schedule (one scan batch — the -075a micro-probe's window).
const PD_STAGE_BATCH: usize = 1024;

/// dedupsub I3: sets below this length keep the plain doubling ladder (the
/// sub-32KB tables are cache-resident and their ladder is cheap; reserving
/// for every small group would trade rehash for memory across the whole
/// 9K-group population).
const PD_PROJECT_MIN: usize = 8192;

/// dedupsub I3 kill switch: `PGRUST_RUNTIME_DISTINCT_TOUCH_EPOCH=0`
/// restores the sort+dedup touched-group accounting pass exactly
/// (default ON; window-grain accounting mechanics only — the accounted
/// totals are identical either way).
fn pd_touch_epoch_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_DISTINCT_TOUCH_EPOCH").map_or(true, |v| v != "0")
    })
}

/// dedupsub I3 kill switch: `PGRUST_RUNTIME_DISTINCT_GROW_PROJECT=0`
/// restores the pure doubling-rehash growth ladder exactly (default ON;
/// probe-table geometry only — set contents and value order are identical
/// for any projection).
fn pd_grow_project_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_DISTINCT_GROW_PROJECT").map_or(true, |v| v != "0")
    })
}

/// q9internals inc-2 kill switch: `PGRUST_RUNTIME_DISTINCT_RUN_MEMO=0`
/// restores the per-row hash+probe and the unconditional staged push
/// exactly (default ON). The memo exploits the sorted banks' contiguous
/// (group key, DISTINCT value) runs: a row whose int key words equal the
/// previous row's resolves to the same group id without probing, and a
/// staged (group, value) pair equal to the previously accepted pair for
/// that memo run is dropped before staging — a duplicate insert is a set
/// no-op by construction, so value arrays, replay order, and every
/// frozen-table byte are identical; only `staged_rows` (a projection-
/// geometry denominator) sees fewer rows.
fn pd_run_memo_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_DISTINCT_RUN_MEMO").map_or(true, |v| v != "0")
    })
}

/// GL-VECACCEPT-1 probe-pass look-ahead distance: while resolving lane row
/// i the pass prefetches the open-addressing slot word row i+K will probe.
/// `PGRUST_RUNTIME_AGG_VECACCEPT_PREFETCH` (default 8; 0 disables) — a
/// ladder axis, never a semantic input (the hint changes no byte).
fn pd_vec_prefetch_k() -> usize {
    static K: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *K.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_AGG_VECACCEPT_PREFETCH")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(8)
            .min(256)
    })
}

/// batch-insert lane: L1 prefetch hint (lanetable's idiom, copied — the
/// stringhash2 seam owns the table-internal machinery; this is driver-side).
#[inline(always)]
fn pd_prefetch(p: *const u8) {
    #[cfg(target_arch = "aarch64")]
    // SAFETY: prfm is a hint; any address is allowed.
    unsafe {
        core::arch::asm!("prfm pldl1keep, [{0}]", in(reg) p, options(nostack, preserves_flags));
    }
    #[cfg(target_arch = "x86_64")]
    // SAFETY: prefetch is a hint; any address is allowed.
    unsafe {
        core::arch::x86_64::_mm_prefetch::<{ core::arch::x86_64::_MM_HINT_T0 }>(p as *const i8);
    }
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    let _ = p;
}
/// Fixed per-group overhead estimate (table slot, hash, vec headers).
const GROUP_FIXED_COST: usize = 48;

/// Byte-content source for a probing key's bytes components (int-only
/// specs pass [`KeySrc::None`], which is never consulted).
enum KeySrc<'a> {
    None,
    /// `(buf, spans)`: `spans[i]` spans `buf` for bytes component i (the
    /// accept path's per-row staging).
    Staged(&'a [u8], &'a [(u32, u32)]),
    /// A (possibly foreign) table's `(key words, arena)`: bytes component
    /// words carry packed spans over the arena (merge / spill replay).
    Table(&'a [i64], &'a [u8]),
}

impl KeySrc<'_> {
    #[inline]
    fn bytes(&self, i: usize) -> &[u8] {
        match self {
            KeySrc::None => &[],
            KeySrc::Staged(buf, spans) => {
                let (o, l) = spans[i];
                &buf[o as usize..(o + l) as usize]
            }
            KeySrc::Table(words, arena) => {
                let (o, l) = unpack_span(words[i]);
                &arena[o..o + l]
            }
        }
    }
}

/// Stage the current row's bytes-key content into the probe buffers. The
/// detoast copy lands in the caller's per-tuple context (reset per row);
/// the staging copy detaches every slot lifetime before the probe.
fn stage_bytes_keys(
    spec: &PdSpec,
    estate: &mut EStateData<'_>,
    id: ExecSlotId,
    tmp: EcxtId,
    nulls: u32,
    buf: &mut Vec<u8>,
    spans: &mut Vec<(u32, u32)>,
) -> PgResult<()> {
    buf.clear();
    spans.clear();
    spans.resize(spec.nkeys(), (0, 0));
    for (i, (&att, kind)) in spec.key_atts.iter().zip(spec.key_kinds.iter()).enumerate() {
        if !matches!(kind, PdKeyKind::Bytes) || nulls & (1 << i) != 0 {
            continue;
        }
        let value = estate.slot_mut(id).base().tts_values[att as usize];
        // SAFETY: non-null live text/varchar varlena (the leader's
        // admission proved the column type); detoast copies land in
        // per-tuple memory.
        let v = unsafe {
            ::types_fmgr::datum_varlena_packed(value, estate.ecxt(tmp).per_tuple_mcx())
        }?;
        let off = buf.len();
        buf.extend_from_slice(v.data());
        spans[i] = (off as u32, (buf.len() - off) as u32);
    }
    Ok(())
}

/// Feed verdict: `Crossed` = the shared budget crossed AFTER this row was
/// fully absorbed — a worker freezes + degrades; the leader evicts sets.
#[derive(PartialEq, Eq)]
pub enum PdFeed {
    Ok,
    Crossed,
}

pub struct PdBuilder<'mcx> {
    spec: Arc<PdSpec>,
    /// Open addressing: slot -> group index + 1; 0 = empty. Pow2 len.
    table: Vec<u32>,
    hashes: Vec<u64>,
    /// Group g's key words at [g*nkeys ..] (sign-extended ints, or packed
    /// arena spans for bytes components).
    keys: Vec<i64>,
    /// Bytes-key content (packed spans in `keys` index into this).
    key_arena: Vec<u8>,
    /// Per-row staging for the CURRENT row's bytes-key content (probe
    /// side): `probe_spans[i]` spans `probe_buf` for bytes key component i.
    /// Rewritten per row; meaningless for int/NULL components. Copying into
    /// the staging buffer detaches every slot/detoast lifetime before the
    /// probe (the hashgrouped arm's discipline).
    probe_buf: Vec<u8>,
    probe_spans: Vec<(u32, u32)>,
    keynulls: Vec<u32>,
    /// Group g's vocab state at [g*2*nvocab ..]: (acc, count) pairs.
    states: Vec<i64>,
    /// Group g's sets at [g*nsets ..].
    dsets: Vec<DistinctSet<'mcx>>,
    /// Per-group cached set memory (delta accounting like hashgrouped).
    set_mem: Vec<usize>,
    base_mem: usize,
    total_set_mem: usize,
    budget: usize,
    /// The leader's spill context: `Some` = crossing evicts the largest
    /// sets to tapes instead of freezing (workers pass `None`).
    mcx: Option<Mcx<'mcx>>,
    /// Any set spilled (parallel fast-path refusal; leader only).
    pub ever_spilled: bool,
    /// Post-eviction high-water: capacities are retained by the set spill
    /// flushes, so `mem()` cannot drop below what the first crossing left;
    /// re-evict only once memory GROWS past this (epoch cadence).
    evict_floor: usize,
    frozen: bool,
    /// batch-insert lane: staged-window batched distinct-set inserts (the
    /// grouped-DISTINCT accept-side stall fix — micro-probe job -075a: accept-side
    /// ~-28% cycles from miss-chain overlap across the per-group sets).
    /// Admitted at construction only (`set_batch_insert`): single int-kind
    /// set, grouped shape. `stage_g/stage_v` hold the deferred (group,
    /// value) pairs in ROW ORDER; `flush_staged` replays them through the
    /// same `insert_i64` kernel with header+cell look-ahead prefetch, so
    /// set contents, value-append order, and every downstream byte are
    /// IDENTICAL to the per-row path — only the insert *schedule*, the
    /// set-memory accounting cadence, and the budget-crossing check move to
    /// window grain (the plain drive's documented one-batch-overshoot
    /// contract, agg_plain_distinct_insert_batch).
    stage_on: bool,
    stage_g: Vec<u32>,
    stage_v: Vec<i64>,
    stage_touch: Vec<u32>,
    /// dedupsub I3 touched-group dedup: per-group window stamp (index =
    /// group, value = the `touch_epoch` that last touched it; 0 = never).
    /// Replaces the per-window sort+dedup of `stage_g` (measured ~4.5-5.4%
    /// of grouped-DISTINCT rt16 cycles) with an O(window) stamp pass.
    touch_stamp: Vec<u32>,
    touch_epoch: u32,
    /// dedupsub I3 projection input: staged values fed so far (the
    /// denominator of expected_worker_rows / staged_rows).
    staged_rows: u64,
    /// distinct-internals inc-2 run memo (int-only specs, nkeys <= MEMO_KEYS;
    /// `memo_g == u32::MAX` = no run). The sorted banks cluster rows by
    /// group key: a row whose key words + null mask equal the previous
    /// row's takes the memoized group id and skips hash+probe entirely.
    memo_on: bool,
    memo_g: u32,
    memo_nulls: u32,
    memo_words: [i64; MEMO_KEYS],
    /// Last staged (group, value) pair (`last_sg == u32::MAX` = none):
    /// a consecutive duplicate is dropped before staging (set-semantics
    /// no-op — the pair is already delivered to that group's set/tape).
    last_sg: u32,
    last_sv: i64,
}

/// distinct-internals inc-2: memoized key width (covers the sorted-DISTINCT family's 1-2
/// int keys; wider int specs keep the per-row probe).
const MEMO_KEYS: usize = 4;

impl<'mcx> PdBuilder<'mcx> {
    pub fn new(spec: Arc<PdSpec>, budget: usize, mcx: Option<Mcx<'mcx>>) -> Self {
        let nprobe = spec.nkeys();
        let memo_on = pd_run_memo_enabled()
            && !spec.has_bytes_keys()
            && nprobe > 0
            && nprobe <= MEMO_KEYS;
        PdBuilder {
            spec,
            table: vec![0u32; INIT_TABLE],
            hashes: Vec::new(),
            keys: Vec::new(),
            key_arena: Vec::new(),
            probe_buf: Vec::new(),
            probe_spans: vec![(0, 0); nprobe],
            keynulls: Vec::new(),
            states: Vec::new(),
            dsets: Vec::new(),
            set_mem: Vec::new(),
            base_mem: INIT_TABLE * 4,
            total_set_mem: 0,
            budget,
            mcx,
            ever_spilled: false,
            evict_floor: 0,
            frozen: false,
            stage_on: false,
            stage_g: Vec::new(),
            stage_v: Vec::new(),
            stage_touch: Vec::new(),
            touch_stamp: Vec::new(),
            touch_epoch: 0,
            staged_rows: 0,
            memo_on,
            memo_g: u32::MAX,
            memo_nulls: 0,
            memo_words: [0; MEMO_KEYS],
            last_sg: u32::MAX,
            last_sv: 0,
        }
    }

    /// Arm the staged batch-insert schedule (fail-closed shape admission:
    /// grouped, exactly one distinct set, int-kind). No-op for every other
    /// shape — those keep the per-row path byte-for-byte.
    pub fn set_batch_insert(&mut self, on: bool) {
        self.stage_on = on
            && self.spec.nkeys() > 0
            && self.spec.sets.len() == 1
            && !matches!(self.spec.sets[0].kind, DistinctKeyKind::Bytes);
        if self.stage_on {
            self.stage_g.reserve(PD_STAGE_BATCH);
            self.stage_v.reserve(PD_STAGE_BATCH);
        }
    }

    /// Whether the staged schedule is armed (trace/e2e visibility).
    pub fn batch_insert_armed(&self) -> bool {
        self.stage_on
    }

    /// Staged-schedule accept tail: fold a NULL immediately (order-free),
    /// stage a value, flush + window-grain crossing check on a full window.
    /// The testable seam (accept minus the slot plumbing).
    #[inline]
    fn stage_push(&mut self, g: usize, v: Option<i64>) -> PgResult<PdFeed> {
        debug_assert!(self.stage_on);
        let Some(v) = v else {
            self.dsets[g].seen_null = true;
            return Ok(PdFeed::Ok);
        };
        // q9internals inc-2: consecutive-duplicate skip. The previous
        // accepted pair was staged (and will flush) or already inserted —
        // either way group g's distinct union already holds v, and the
        // duplicate insert this push would replay is a set no-op. Value
        // arrays, append order, and every frozen/spilled union byte are
        // identical. Sorted banks make duplicates overwhelmingly
        // consecutive. staged_rows still counts the skipped row: it is the
        // projection ratio's ROW denominator (expected_worker_rows is raw
        // plan rows) — starving it would inflate the ratio by the dup
        // factor and over-reserve the dominant sets toward the raw row
        // count (memory + budget-crossing distortion).
        if self.memo_on {
            if self.last_sg == g as u32 && self.last_sv == v {
                self.staged_rows += 1;
                return Ok(PdFeed::Ok);
            }
            self.last_sg = g as u32;
            self.last_sv = v;
        }
        self.stage_g.push(g as u32);
        self.stage_v.push(v);
        if self.stage_v.len() >= PD_STAGE_BATCH {
            self.flush_staged();
            // Window-grain crossing check (the plain drive's
            // one-batch-overshoot contract).
            if self.mem() > self.budget.max(self.evict_floor) {
                if self.mcx.is_some() {
                    self.evict_sets()?;
                    return Ok(PdFeed::Ok);
                }
                return Ok(PdFeed::Crossed);
            }
        }
        Ok(PdFeed::Ok)
    }

    /// Replay the staged (group, value) window through the per-row insert
    /// kernel with look-ahead prefetch (header at 16, set cell at 8 — the
    /// -075a micro-probe figures), then re-account set memory for the
    /// touched groups (delta accounting, window grain).
    fn flush_staged(&mut self) {
        let n = self.stage_v.len();
        if n == 0 {
            return;
        }
        const LH_HDR: usize = 16;
        const LH_CELL: usize = 8;
        debug_assert_eq!(self.spec.sets.len(), 1);
        for i in 0..n {
            // SAFETY: staged group ids index dsets (created at accept);
            // look-ahead indices bounds-checked; prefetches are hints.
            unsafe {
                if i + LH_HDR < n {
                    let gh = *self.stage_g.get_unchecked(i + LH_HDR) as usize;
                    pd_prefetch(self.dsets.as_ptr().add(gh) as *const u8);
                }
                if i + LH_CELL < n {
                    let gc = *self.stage_g.get_unchecked(i + LH_CELL) as usize;
                    self.dsets
                        .get_unchecked(gc)
                        .prefetch_i64(*self.stage_v.get_unchecked(i + LH_CELL));
                }
                let g = *self.stage_g.get_unchecked(i) as usize;
                let v = *self.stage_v.get_unchecked(i);
                self.dsets.get_unchecked_mut(g).insert_i64(v);
            }
        }
        self.staged_rows += n as u64;
        // Touched-group dedup (nsets == 1: set index == group index).
        // dedupsub I3: an O(window) epoch-stamp pass replaces the per-window
        // sort+dedup (quicksort measured 5.38%/4.48% of grouped-DISTINCT rt16 cycles).
        // stage_touch ends up in first-touch order instead of sorted order —
        // a non-surface: it only drives the delta re-account below, whose
        // sum is order-invariant, and the projection reserve (geometry).
        self.stage_touch.clear();
        if pd_touch_epoch_enabled() {
            self.touch_epoch = self.touch_epoch.wrapping_add(1);
            if self.touch_epoch == 0 {
                // u32 wrap (~4.4e9 windows — unreachable in practice, exact
                // anyway): forget all stamps and restart the epoch clock.
                self.touch_stamp.iter_mut().for_each(|s| *s = 0);
                self.touch_epoch = 1;
            }
            let ep = self.touch_epoch;
            for &g in &self.stage_g {
                let s = &mut self.touch_stamp[g as usize];
                if *s != ep {
                    *s = ep;
                    self.stage_touch.push(g);
                }
            }
        } else {
            self.stage_touch.extend_from_slice(&self.stage_g);
            self.stage_touch.sort_unstable();
            self.stage_touch.dedup();
        }
        // dedupsub I3 projection reserve: pre-size each touched BIG set's
        // probe table for its projected final length — one jump instead of
        // the doubling-rehash ladder (IntSet::grow measured 5.09%/3.99% of
        // grouped-DISTINCT rt16 cycles). Projection: linear extrapolation of the
        // set's length over the worker's expected row share, ratio clamped
        // (early windows / estimate error), target capped at the share
        // itself (a set can never exceed the rows that feed it). Runs
        // BEFORE the re-account so the reserved capacity is metered in this
        // window (honest budget accounting).
        let expected = self.spec.expected_worker_rows;
        if expected > 0 && pd_grow_project_enabled() {
            let ratio =
                (expected as f64 / self.staged_rows.max(1) as f64).clamp(1.0, 64.0);
            for &g in &self.stage_touch {
                let d = &mut self.dsets[g as usize];
                let len = d.len();
                if len >= PD_PROJECT_MIN {
                    let proj = ((len as f64 * ratio) as usize).min(expected as usize);
                    d.reserve_projected(proj);
                }
            }
        }
        for &g in &self.stage_touch {
            let g = g as usize;
            let m = self.dsets[g].mem_bytes();
            self.total_set_mem = self.total_set_mem + m - self.set_mem[g];
            self.set_mem[g] = m;
        }
        self.stage_g.clear();
        self.stage_v.clear();
    }

    /// GL-VECACCEPT-1 admission (the builder-side gate): the vectorized
    /// whole-granule accept requires the staged set-insert shape
    /// ([`set_batch_insert`] armed: grouped, one int-kind set) AND the
    /// batched group resolve's own gate (exactly one int key — the batch
    /// hash pass is the single-word `key_hash` chain). Callers derive the
    /// lane geometry via [`pd_vec_plan`]; this predicate is the runtime
    /// twin ensuring THIS Local can run the vec schedule.
    pub fn vec_admissible(&self) -> bool {
        self.stage_on
            && self.spec.nkeys() == 1
            && matches!(self.spec.key_kinds[0], PdKeyKind::Int(_))
    }

    /// GL-VECACCEPT-1 phases 1-3 over one granule's canonicalized lanes:
    ///
    ///   1. BATCH HASH — one tight mix64 pass over the key lane (the
    ///      single-word [`key_hash`] chain verbatim, nulls = 0: columnar
    ///      part lanes carry no NULLs by construction).
    ///   2. BATCH PROBE/RESOLVE — row-order group resolution against the
    ///      open-addressing table with a K-ahead slot-word prefetch
    ///      ([`pd_vec_prefetch_k`]) and the run memo (a row whose key
    ///      equals the previous row's takes the previous gid — the
    ///      [`resolve_group_int`] memo law, batch-local). Misses create
    ///      groups exactly as the per-row path ([`create_group`]).
    ///   3. RIDER FOLDS — one scalar pass per vocab entry over the
    ///      resolved gid lane: count riders bump `acc` per row (no NULLs =
    ///      CountAny ≡ CountStar here); Sum/Avg riders fold their value
    ///      lane into `(acc, count)`.
    ///
    /// `riders` aligns with `spec.vocab` ([`PdVecPlan::riders`]); every
    /// `Some` lane and the key lane are `n` rows. The observable group
    /// table, states, and hash bytes are identical to `n` per-row accepts
    /// of the same rows — only the schedule (and the memory-accounting
    /// grain, unchanged from the staged law) differs. The distinct-set
    /// feed is phase 4 ([`vec_stage_sets`], resumable for the spill law).
    pub fn vec_resolve_fold(
        &mut self,
        keys: &[i64],
        riders: &[Option<&[i64]>],
        vs: &mut PdVecScratch,
    ) {
        debug_assert!(self.vec_admissible());
        debug_assert_eq!(riders.len(), self.spec.vocab.len());
        let n = keys.len();
        // Phase 1: the batch hash pass (single-word key_hash verbatim).
        vs.hashes.clear();
        vs.hashes.reserve(n);
        const SEED: u64 = 0x9e37_79b9_7f4a_7c15;
        vs.hashes.extend(keys.iter().map(|&k| mix64(SEED ^ (k as u64))));
        debug_assert!(n == 0 || vs.hashes[0] == key_hash(&keys[..1], 0));
        // Phase 2: probe/resolve with K-ahead slot-word prefetch. A grow
        // mid-pass re-bases the table — the hints are stale but harmless
        // (prefetches are advisory; the resolve re-reads live state).
        let k_ahead = pd_vec_prefetch_k();
        vs.gids.clear();
        vs.gids.reserve(n);
        let mut prev_key = 0i64;
        let mut prev_g = u32::MAX;
        for i in 0..n {
            if k_ahead > 0 && i + k_ahead < n {
                let mask = self.table.len() - 1;
                let slot = (vs.hashes[i + k_ahead] as usize) & mask;
                // SAFETY: in-bounds pointer; prefetch is a hint.
                pd_prefetch(unsafe { self.table.as_ptr().add(slot) } as *const u8);
            }
            let k = keys[i];
            let g = if prev_g != u32::MAX && prev_key == k {
                prev_g
            } else {
                let h = vs.hashes[i];
                let (found, slot_idx) = self.probe(&[k], 0, h, &KeySrc::None);
                match found {
                    Some(g) => g,
                    None => self.create_group(&[k], 0, h, slot_idx, &KeySrc::None),
                }
            };
            prev_key = k;
            prev_g = g;
            vs.gids.push(g);
        }
        // Phase 3: rider folds, one pass per vocab entry (spec/states are
        // disjoint fields — the per-row accept's own borrow shape).
        let nvocab = self.spec.vocab.len();
        let stride = 2 * nvocab;
        for (vi, v) in self.spec.vocab.iter().enumerate() {
            let (acc, cnt) = (2 * vi, 2 * vi + 1);
            match v.kind {
                // No NULLs in part lanes: count(x) counts every row, as
                // the per-row isnull check (always false here) would.
                PdVocabKind::CountStar | PdVocabKind::CountAny { .. } => {
                    for &g in vs.gids.iter() {
                        self.states[g as usize * stride + acc] += 1;
                    }
                }
                PdVocabKind::SumInt { .. } | PdVocabKind::AvgInt { .. } => {
                    let lane = riders[vi].expect("Sum/Avg rider carries a value lane");
                    debug_assert_eq!(lane.len(), n);
                    for (i, &g) in vs.gids.iter().enumerate() {
                        let st = g as usize * stride;
                        self.states[st + acc] += lane[i];
                        self.states[st + cnt] += 1;
                    }
                }
            }
        }
    }

    /// GL-VECACCEPT-1 phase 4: the staged distinct-set feed over the
    /// resolved (gid, value) lanes, starting at row `from` — each pair
    /// rides [`stage_push`] (dup-skip, window flush, window-grain budget
    /// crossing: the exact per-row staged schedule, so set contents and
    /// value-append order are byte-identical). Returns the feed verdict
    /// and the count of rows CONSUMED: on `Crossed` the caller runs the
    /// spill law and, if the epoch drained, RESUMES from the returned
    /// index — phases 1-3 already folded this granule (group table and
    /// rider states never spill), so the resume touches sets only.
    pub fn vec_stage_sets(
        &mut self,
        gids: &[u32],
        vals: &[i64],
        from: usize,
    ) -> PgResult<(PdFeed, usize)> {
        debug_assert_eq!(gids.len(), vals.len());
        for i in from..gids.len() {
            // No NULLs in part lanes (the direct-feed contract).
            if self.stage_push(gids[i] as usize, Some(vals[i]))? == PdFeed::Crossed {
                return Ok((PdFeed::Crossed, i + 1));
            }
        }
        Ok((PdFeed::Ok, gids.len()))
    }

    #[inline]
    pub fn ngroups(&self) -> usize {
        self.hashes.len()
    }

    #[inline]
    fn mem(&self) -> usize {
        // The key arena is GROUP IDENTITY (like base_mem): it never spills
        // and never resets, so a crossing it drives is group-table-dominated
        // for the spill worthwhileness gate — exactly right.
        self.base_mem + self.total_set_mem + self.key_arena.capacity()
    }

    pub fn mem_bytes(&self) -> usize {
        self.mem()
    }

    #[cold]
    #[inline(never)]
    fn grow(&mut self) {
        let new_len = self.table.len() * 2;
        self.base_mem += (new_len - self.table.len()) * 4;
        let mask = new_len - 1;
        let mut table = vec![0u32; new_len];
        for (g, &h) in self.hashes.iter().enumerate() {
            let mut slot = (h as usize) & mask;
            while table[slot] != 0 {
                slot = (slot + 1) & mask;
            }
            table[slot] = (g + 1) as u32;
        }
        self.table = table;
    }

    /// Does group `g`'s key equal the probing key (`words` + bytes content
    /// via `src`)? Int-only specs take the word-slice compare verbatim;
    /// bytes components compare CONTENT (the canonical identity — span
    /// words are table-relative and never compared directly).
    #[inline]
    fn group_keys_match(&self, g: usize, words: &[i64], src: &KeySrc<'_>) -> bool {
        let nkeys = self.spec.nkeys();
        let gw = &self.keys[g * nkeys..(g + 1) * nkeys];
        if !self.spec.has_bytes_keys() {
            return gw == words;
        }
        for (i, kind) in self.spec.key_kinds.iter().enumerate() {
            match kind {
                PdKeyKind::Int(_) => {
                    if gw[i] != words[i] {
                        return false;
                    }
                }
                PdKeyKind::Bytes => {
                    let (off, len) = unpack_span(gw[i]);
                    if &self.key_arena[off..off + len] != src.bytes(i) {
                        return false;
                    }
                }
            }
        }
        true
    }

    fn probe(
        &self,
        words: &[i64],
        nulls: u32,
        h: u64,
        src: &KeySrc<'_>,
    ) -> (Option<u32>, usize) {
        let mask = self.table.len() - 1;
        let mut slot = (h as usize) & mask;
        loop {
            match self.table[slot] {
                0 => return (None, slot),
                e => {
                    let g = (e - 1) as usize;
                    if self.hashes[g] == h
                        && self.keynulls[g] == nulls
                        && self.group_keys_match(g, words, src)
                    {
                        return (Some(e - 1), slot);
                    }
                    slot = (slot + 1) & mask;
                }
            }
        }
    }

    fn create_group(
        &mut self,
        words: &[i64],
        nulls: u32,
        h: u64,
        slot_idx: usize,
        src: &KeySrc<'_>,
    ) -> u32 {
        let g = self.ngroups() as u32;
        self.hashes.push(h);
        self.keynulls.push(nulls);
        if !self.spec.has_bytes_keys() {
            self.keys.extend_from_slice(words);
        } else {
            // Bytes components: copy content into OUR arena and store the
            // re-based span word; NULL components keep word 0 / empty span.
            for (i, kind) in self.spec.key_kinds.iter().enumerate() {
                match kind {
                    PdKeyKind::Int(_) => self.keys.push(words[i]),
                    PdKeyKind::Bytes => {
                        if nulls & (1 << i) != 0 {
                            self.keys.push(0);
                        } else {
                            let b = src.bytes(i);
                            let off = self.key_arena.len();
                            self.key_arena.extend_from_slice(b);
                            self.keys.push(pack_span(off, b.len()));
                        }
                    }
                }
            }
        }
        self.states.extend(core::iter::repeat(0i64).take(2 * self.spec.vocab.len()));
        for _ in 0..self.spec.sets.len() {
            self.dsets.push(DistinctSet::new());
        }
        self.set_mem.push(0);
        if self.stage_on {
            self.touch_stamp.push(0);
        }
        self.table[slot_idx] = g + 1;
        self.base_mem += self.spec.nkeys() * 8
            + 2 * self.spec.vocab.len() * 8
            + self.spec.sets.len() * core::mem::size_of::<DistinctSet<'_>>()
            + GROUP_FIXED_COST;
        if (self.ngroups() + 1) * 8 > self.table.len() * 7 {
            self.grow();
        }
        g
    }

    /// The row-side key hash: canonical (value-derived, identical across
    /// workers). Int-only specs keep [`key_hash`] verbatim; bytes specs
    /// chain component-wise — int words through the same mixer, bytes
    /// components by CONTENT ([`hash_chain_bytes`]). The top-8 bits are the
    /// combine partition law, so this must agree between accept, spill
    /// replay, and every merge face.
    #[inline]
    fn row_hash(spec: &PdSpec, words: &[i64], nulls: u32, src: &KeySrc<'_>) -> u64 {
        if !spec.has_bytes_keys() {
            return key_hash(words, nulls);
        }
        let mut h = (nulls as u64) ^ 0x9e37_79b9_7f4a_7c15;
        for (i, kind) in spec.key_kinds.iter().enumerate() {
            match kind {
                PdKeyKind::Int(_) => h = mix64(h ^ (words[i] as u64)),
                PdKeyKind::Bytes => {
                    if nulls & (1 << i) != 0 {
                        h = mix64(h ^ 0xdead_beef_0bad_cafe);
                    } else {
                        h = hash_chain_bytes(h, src.bytes(i));
                    }
                }
            }
        }
        h
    }

    /// Int-only group resolution with the q9internals inc-2 run memo: same
    /// key words + null mask as the previous row resolve without hash+probe
    /// (sorted banks cluster group keys into runs). Group ids are stable for
    /// the builder's whole life (freeze consumes it; groups are never
    /// removed), so a memo hit is exactly the id the probe would return —
    /// byte-identity by construction. Memo off (kill switch / bytes keys /
    /// wide specs) = the historical per-row path verbatim.
    #[inline]
    fn resolve_group_int(&mut self, words: &[i64], nulls: u32) -> u32 {
        let nkeys = words.len();
        if self.memo_on
            && self.memo_g != u32::MAX
            && self.memo_nulls == nulls
            && self.memo_words[..nkeys] == *words
        {
            return self.memo_g;
        }
        let h = key_hash(words, nulls);
        let (found, slot_idx) = self.probe(words, nulls, h, &KeySrc::None);
        let g = match found {
            Some(g) => g,
            None => self.create_group(words, nulls, h, slot_idx, &KeySrc::None),
        };
        if self.memo_on {
            self.memo_g = g;
            self.memo_nulls = nulls;
            self.memo_words[..nkeys].copy_from_slice(words);
        }
        g
    }

    /// Test seam (identity oracles): force the run memo + dup-skip arm on
    /// or off regardless of the process-global kill switch. Admission
    /// shape guards are re-applied — never arms a bytes/wide spec.
    #[cfg(test)]
    fn set_run_memo(&mut self, on: bool) {
        let nprobe = self.spec.nkeys();
        self.memo_on =
            on && !self.spec.has_bytes_keys() && nprobe > 0 && nprobe <= MEMO_KEYS;
        self.memo_g = u32::MAX;
        self.last_sg = u32::MAX;
    }

    /// Feed one row from the (deformed) scan slot. `tmp` is a per-row-reset
    /// expr context whose per-tuple memory absorbs text detoast copies (the
    /// set retains its own canonical image — collect_distinct_set's law).
    pub fn accept(
        &mut self,
        estate: &mut EStateData<'mcx>,
        id: ExecSlotId,
        tmp: EcxtId,
    ) -> PgResult<PdFeed> {
        debug_assert!(!self.frozen);
        let mut words = [0i64; 32];
        let nkeys = self.spec.nkeys();
        // NO per-row Arc clone of the spec: every participant's builder holds
        // the SAME Arc<PdSpec> allocation, so a clone+drop here is two
        // contended refcount RMWs per row on one shared cache line across all
        // workers (the __aarch64_ldadd8_relax/_rel flat-profile signature).
        // Disjoint field borrows below make the clone unnecessary.
        let max_att = self.spec.max_att;
        exectuples::slot_getsomeattrs(estate.slot_mut(id), max_att);
        let mut nulls = 0u32;
        {
            let base = estate.slot_mut(id).base();
            for (i, (&att, &kind)) in
                self.spec.key_atts.iter().zip(self.spec.key_kinds.iter()).enumerate()
            {
                if base.tts_isnull[att as usize] {
                    nulls |= 1 << i;
                    words[i] = 0;
                } else {
                    words[i] = match kind {
                        PdKeyKind::Int(k) => k.read(base.tts_values[att as usize]),
                        // Bytes components probe by CONTENT (staged below);
                        // the group word is table-relative and written at
                        // create time.
                        PdKeyKind::Bytes => 0,
                    };
                }
            }
        }
        let g = (if !self.spec.has_bytes_keys() {
            self.resolve_group_int(&words[..nkeys], nulls)
        } else {
            // Detach the staging buffers from `self` so the KeySrc borrow
            // and the &mut self create can coexist; restored below (an
            // error path only loses buffer capacity).
            let mut pbuf = core::mem::take(&mut self.probe_buf);
            let mut pspans = core::mem::take(&mut self.probe_spans);
            let staged =
                stage_bytes_keys(&self.spec, estate, id, tmp, nulls, &mut pbuf, &mut pspans);
            if let Err(e) = staged {
                self.probe_buf = pbuf;
                self.probe_spans = pspans;
                return Err(e);
            }
            let g = {
                let src = KeySrc::Staged(&pbuf, &pspans);
                let h = Self::row_hash(&self.spec, &words[..nkeys], nulls, &src);
                let (found, slot_idx) = self.probe(&words[..nkeys], nulls, h, &src);
                match found {
                    Some(g) => g,
                    None => self.create_group(&words[..nkeys], nulls, h, slot_idx, &src),
                }
            };
            self.probe_buf = pbuf;
            self.probe_spans = pspans;
            g
        }) as usize;
        // Vocab transitions (spec/states are disjoint fields).
        if !self.spec.vocab.is_empty() {
            let base = estate.slot_mut(id).base();
            let spec = &self.spec;
            let st = &mut self.states[g * 2 * spec.vocab.len()..];
            for (vi, v) in spec.vocab.iter().enumerate() {
                let (acc, cnt) = (2 * vi, 2 * vi + 1);
                match v.kind {
                    PdVocabKind::CountStar => st[acc] += 1,
                    PdVocabKind::CountAny { att } => {
                        if !base.tts_isnull[att as usize] {
                            st[acc] += 1;
                        }
                    }
                    PdVocabKind::SumInt { att, kind } | PdVocabKind::AvgInt { att, kind } => {
                        if !base.tts_isnull[att as usize] {
                            st[acc] += kind.read(base.tts_values[att as usize]);
                            st[cnt] += 1;
                        }
                    }
                }
            }
        }
        // Distinct-set collects (after the immutable-borrow block: bytes
        // inserts may need the estate for detoast).
        let nsets = self.spec.sets.len();
        if self.stage_on {
            // batch-insert lane: defer the (single, int-kind) set insert to
            // the staged window; NULLs fold immediately (order-free).
            // Value-append order inside every set stays row order.
            debug_assert_eq!(nsets, 1);
            let PdSetSpec { att, kind } = self.spec.sets[0];
            let (value, isnull) = {
                let base = estate.slot_mut(id).base();
                (base.tts_values[att as usize], base.tts_isnull[att as usize])
            };
            let v = if isnull {
                None
            } else {
                Some(match kind {
                    DistinctKeyKind::Int16 => value.as_i16() as i64,
                    DistinctKeyKind::Int32 => value.as_i32() as i64,
                    DistinctKeyKind::Int64 => value.as_i64(),
                    DistinctKeyKind::Bytes => unreachable!("bytes shapes are not admitted"),
                })
            };
            return self.stage_push(g, v);
        }
        if nsets != 0 {
            let mut sets_mem = 0usize;
            for j in 0..nsets {
                let PdSetSpec { att, kind } = self.spec.sets[j];
                // Re-borrow per set: the bytes arm needs estate for detoast.
                let (value, isnull) = {
                    let base = estate.slot_mut(id).base();
                    (base.tts_values[att as usize], base.tts_isnull[att as usize])
                };
                let dset = &mut self.dsets[g * nsets + j];
                if isnull {
                    dset.seen_null = true;
                } else {
                    match kind {
                        DistinctKeyKind::Int16 => dset.insert_i64(value.as_i16() as i64),
                        DistinctKeyKind::Int32 => dset.insert_i64(value.as_i32() as i64),
                        DistinctKeyKind::Int64 => dset.insert_i64(value.as_i64()),
                        DistinctKeyKind::Bytes => {
                            // SAFETY: non-null live text/varchar varlena (the
                            // leader's admission proved the argument type);
                            // detoast copies land in per-tuple memory.
                            let v = unsafe {
                                ::types_fmgr::datum_varlena_packed(
                                    value,
                                    estate.ecxt(tmp).per_tuple_mcx(),
                                )
                            }?;
                            dset.insert_bytes(v.data());
                        }
                    }
                }
                sets_mem += dset.mem_bytes();
            }
            self.total_set_mem = self.total_set_mem + sets_mem - self.set_mem[g];
            self.set_mem[g] = sets_mem;
        }
        if self.mem() <= self.budget.max(self.evict_floor) {
            return Ok(PdFeed::Ok);
        }
        // Leader (mcx-backed): evict the largest sets to the DistinctSet
        // spill tapes until back under budget; workers freeze instead.
        if self.mcx.is_some() {
            self.evict_sets()?;
            return Ok(PdFeed::Ok);
        }
        Ok(PdFeed::Crossed)
    }

    /// Leader crossing: spill the largest in-memory sets to their own
    /// hash-partitioned tapes until under budget (each spill_flush resets
    /// the set's values, capacities retained). Bounded: memory <= budget +
    /// one insert, exactly the serial plain arm's law.
    #[cold]
    #[inline(never)]
    fn evict_sets(&mut self) -> PgResult<()> {
        let mcx = self.mcx.expect("evict_sets is leader-only");
        let budget = self.budget;
        let nsets = self.spec.sets.len();
        while self.mem() > budget {
            // Largest set by held bytes.
            let mut best: Option<(usize, usize)> = None;
            for (i, d) in self.dsets.iter().enumerate() {
                let m = d.mem_bytes();
                if d.len() > 0 && best.is_none_or(|(_, bm)| m > bm) {
                    best = Some((i, m));
                }
            }
            let Some((i, _)) = best else {
                // Nothing evictable right now (flushed capacities +
                // metadata hold the floor — estimate-gated upstream);
                // ratchet the floor so the crossing check re-arms only on
                // real growth (epoch cadence, not per row).
                self.evict_floor = self.mem() + (self.budget / 16).max(4096);
                return Ok(());
            };
            let kind = self.spec.sets[i % nsets].kind;
            self.dsets[i].spill_flush(kind, budget, mcx)?;
            self.ever_spilled = true;
            let gi = i / nsets;
            let sets_mem: usize = self.dsets[gi * nsets..(gi + 1) * nsets]
                .iter()
                .map(|d| d.mem_bytes())
                .sum();
            self.total_set_mem = self.total_set_mem + sets_mem - self.set_mem[gi];
            self.set_mem[gi] = sets_mem;
        }
        self.evict_floor = self.mem() + (self.budget / 16).max(4096);
        Ok(())
    }

    /// Freeze into the handed wire format (plain data, Send). Grouped
    /// tables carry a group partition (top-8 hash bits); the plain shape
    /// (nkeys == 0) carries per-set ELEMENT partitions instead.
    pub fn freeze(mut self) -> PgResult<PdHandedTable> {
        debug_assert!(!self.frozen);
        debug_assert!(!self.ever_spilled, "frozen tables are in-memory only");
        self.flush_staged();
        self.frozen = true;
        let spec = self.spec.clone();
        let nsets = spec.sets.len();
        let n = self.ngroups();
        let total_sets = n * nsets;
        let plain = spec.nkeys() == 0;
        // dedupsub reserve wave (vecaudit boardable item): O(ngroups)
        // counting pre-pass — exact export sizes are already known from the
        // sets, so every export vec allocates once instead of doubling
        // through multi-MB copies.
        let (tot_ints, tot_spans, tot_blob) = self.dsets.iter().enumerate().fold(
            (0usize, 0usize, 0usize),
            |(ti, ts, tb), (si, d)| match spec.sets[si % nsets.max(1)].kind {
                DistinctKeyKind::Bytes => {
                    let nb = d.n_bytes();
                    let blob: usize =
                        (0..nb).map(|i| d.bytes_span(i).1 as usize).sum::<usize>();
                    (ti, ts + nb, tb + blob)
                }
                _ => (ti + d.ints().len(), ts, tb),
            },
        );
        let mut set_ints: Vec<i64> = Vec::with_capacity(tot_ints);
        let mut set_int_offs: Vec<u32> = Vec::with_capacity(total_sets + 1);
        let mut set_blob: Vec<u8> = Vec::with_capacity(tot_blob);
        let mut set_spans: Vec<PdSpan> = Vec::with_capacity(tot_spans);
        let mut set_span_offs: Vec<u32> = Vec::with_capacity(total_sets + 1);
        let mut set_null: Vec<bool> = Vec::with_capacity(total_sets);
        let mut elem_parts: Vec<u32> =
            Vec::with_capacity(if plain { total_sets * (PD_ELEM_PARTS + 1) } else { 0 });
        set_int_offs.push(0);
        set_span_offs.push(0);
        for (si, d) in self.dsets.iter().enumerate() {
            set_null.push(d.seen_null);
            match spec.sets[si % nsets.max(1)].kind {
                DistinctKeyKind::Bytes => {
                    if plain {
                        // Element-partitioned export: spans ordered by the
                        // partition of their content hash.
                        let mut idx: Vec<u32> = (0..d.n_bytes() as u32).collect();
                        let part_of = |i: u32| -> usize {
                            let (_, _, h) = d.bytes_span(i as usize);
                            ((mix64(h as u64) >> 32) as usize) & (PD_ELEM_PARTS - 1)
                        };
                        idx.sort_by_key(|&i| part_of(i));
                        let base = set_spans.len() as u32;
                        let mut starts = [0u32; PD_ELEM_PARTS + 1];
                        for &i in &idx {
                            starts[part_of(i) + 1] += 1;
                        }
                        for p in 0..PD_ELEM_PARTS {
                            starts[p + 1] += starts[p];
                        }
                        elem_parts.extend(starts.iter().map(|&s| base + s));
                        for &i in &idx {
                            // dedupsub reserve wave: extend straight from
                            // the set's blob — `d` and `set_blob` are
                            // disjoint, the old per-value to_vec bought
                            // nothing (vecaudit l.1049 item).
                            let (off, len, h) = d.bytes_span(i as usize);
                            let content = d.bytes_content(off, len);
                            let noff = set_blob.len() as u32;
                            set_blob.extend_from_slice(content);
                            set_spans.push(PdSpan { off: noff, len, hash: h });
                        }
                    } else {
                        for i in 0..d.n_bytes() {
                            let (off, len, h) = d.bytes_span(i);
                            let content = d.bytes_content(off, len);
                            let noff = set_blob.len() as u32;
                            set_blob.extend_from_slice(content);
                            set_spans.push(PdSpan { off: noff, len, hash: h });
                        }
                    }
                }
                _ => {
                    if plain {
                        let base = set_ints.len() as u32;
                        let mut vals: Vec<i64> = d.ints().to_vec();
                        let part_of =
                            |v: i64| ((mix64(v as u64) >> 32) as usize) & (PD_ELEM_PARTS - 1);
                        vals.sort_by_key(|&v| part_of(v));
                        let mut starts = [0u32; PD_ELEM_PARTS + 1];
                        for &v in &vals {
                            starts[part_of(v) + 1] += 1;
                        }
                        for p in 0..PD_ELEM_PARTS {
                            starts[p + 1] += starts[p];
                        }
                        elem_parts.extend(starts.iter().map(|&s| base + s));
                        set_ints.extend_from_slice(&vals);
                    } else {
                        set_ints.extend_from_slice(d.ints());
                    }
                }
            }
            set_int_offs.push(set_ints.len() as u32);
            set_span_offs.push(set_spans.len() as u32);
        }
        // Group partition (grouped shapes): counting sort by top-8 bits.
        let parts = if !plain {
            let mut starts = vec![0u32; PD_GROUP_PARTS + 1];
            for &h in &self.hashes {
                starts[(h >> 56) as usize + 1] += 1;
            }
            for p in 0..PD_GROUP_PARTS {
                starts[p + 1] += starts[p];
            }
            let mut idx = vec![0u32; n];
            let mut cur = starts.clone();
            for (g, &h) in self.hashes.iter().enumerate() {
                let b = (h >> 56) as usize;
                idx[cur[b] as usize] = g as u32;
                cur[b] += 1;
            }
            Some(PdPartition { starts, idx })
        } else {
            None
        };
        Ok(PdHandedTable {
            ngroups: n,
            keys: core::mem::take(&mut self.keys),
            key_arena: core::mem::take(&mut self.key_arena),
            keynulls: core::mem::take(&mut self.keynulls),
            hashes: core::mem::take(&mut self.hashes),
            states: core::mem::take(&mut self.states),
            set_ints,
            set_int_offs,
            set_blob,
            set_spans,
            set_span_offs,
            set_null,
            elem_parts,
            parts,
            live_sets: Vec::new(),
        })
    }

}

impl PdBuilder<'static> {
    /// GL-LOWDIST-1: freeze into the LIVE-form handed table (grouped
    /// specs only) — group/key/state surfaces and the partition index are
    /// `freeze()` verbatim, but the `DistinctSet`s ride WHOLE (probe
    /// tables intact) instead of flattening their values, so the
    /// low-width combine can steal a donor's set per group and re-insert
    /// only the smaller donors' values. `'static` builders only (the sink
    /// Locals' mcx-free form; the leader/hybrid builders never take this
    /// path).
    pub fn freeze_live(mut self) -> PgResult<PdHandedTable> {
        debug_assert!(!self.frozen);
        debug_assert!(!self.ever_spilled, "frozen tables are in-memory only");
        debug_assert!(self.spec.nkeys() > 0, "live freeze is grouped-only");
        self.flush_staged();
        self.frozen = true;
        let n = self.ngroups();
        let set_null: Vec<bool> = self.dsets.iter().map(|d| d.seen_null).collect();
        // Group partition — freeze()'s counting sort verbatim.
        let mut starts = vec![0u32; PD_GROUP_PARTS + 1];
        for &h in &self.hashes {
            starts[(h >> 56) as usize + 1] += 1;
        }
        for p in 0..PD_GROUP_PARTS {
            starts[p + 1] += starts[p];
        }
        let mut idx = vec![0u32; n];
        let mut cur = starts.clone();
        for (g, &h) in self.hashes.iter().enumerate() {
            let b = (h >> 56) as usize;
            idx[cur[b] as usize] = g as u32;
            cur[b] += 1;
        }
        let live_sets = core::mem::take(&mut self.dsets)
            .into_iter()
            .map(|d| core::cell::UnsafeCell::new(Some(d)))
            .collect();
        Ok(PdHandedTable {
            ngroups: n,
            keys: core::mem::take(&mut self.keys),
            key_arena: core::mem::take(&mut self.key_arena),
            keynulls: core::mem::take(&mut self.keynulls),
            hashes: core::mem::take(&mut self.hashes),
            states: core::mem::take(&mut self.states),
            set_ints: Vec::new(),
            set_int_offs: Vec::new(),
            set_blob: Vec::new(),
            set_spans: Vec::new(),
            set_span_offs: Vec::new(),
            set_null,
            elem_parts: Vec::new(),
            parts: Some(PdPartition { starts, idx }),
            live_sets,
        })
    }
}

#[inline]
pub(crate) fn key_hash(words: &[i64], nulls: u32) -> u64 {
    let mut h = (nulls as u64) ^ 0x9e37_79b9_7f4a_7c15;
    for &w in words {
        h = mix64(h ^ (w as u64));
    }
    h
}

// ===========================================================================
// The wire format.
// ===========================================================================

#[derive(Clone, Copy)]
pub struct PdSpan {
    off: u32,
    len: u32,
    hash: u32,
}

pub struct PdPartition {
    /// 257 prefix-sum starts into `idx`.
    starts: Vec<u32>,
    idx: Vec<u32>,
}

/// One participant's frozen partial table — plain data, self-contained.
///
/// Two set-value FORMS (the group/key/state/partition surfaces are
/// identical): FLAT (`freeze()` — values flattened into
/// `set_ints`/`set_blob`+`set_spans`; the historical wire form, spill- and
/// hybrid-compatible) and LIVE (`freeze_live()`, GL-LOWDIST-1 — the
/// builder's `DistinctSet`s ride whole in `live_sets`, probe tables
/// intact, so the low-width combine can STEAL a donor's set per group
/// instead of re-hashing every value). `set_ints`/`set_bytes` read both
/// forms transparently; a live form is grouped-only.
pub struct PdHandedTable {
    pub ngroups: usize,
    keys: Vec<i64>,
    /// Bytes-key content (spans packed in `keys`; empty for int-only specs).
    key_arena: Vec<u8>,
    keynulls: Vec<u32>,
    hashes: Vec<u64>,
    states: Vec<i64>,
    set_ints: Vec<i64>,
    set_int_offs: Vec<u32>,
    set_blob: Vec<u8>,
    set_spans: Vec<PdSpan>,
    set_span_offs: Vec<u32>,
    set_null: Vec<bool>,
    /// Plain shape: per set, PD_ELEM_PARTS+1 absolute starts into
    /// set_ints/set_spans (laid consecutively per set).
    elem_parts: Vec<u32>,
    parts: Option<PdPartition>,
    /// LIVE form only (empty = flat): set `si`'s live `DistinctSet`, in
    /// cells so the sole claimer of the set's group partition can take it
    /// (see the Sync SAFETY note).
    live_sets: Vec<core::cell::UnsafeCell<Option<DistinctSet<'static>>>>,
}

/// Iterator over one set's byte elements — both handed-table forms.
enum SetBytesIter<'a> {
    Flat { spans: &'a [PdSpan], blob: &'a [u8], i: usize },
    Live { d: &'a DistinctSet<'static>, i: usize, n: usize },
    Empty,
}

impl<'a> Iterator for SetBytesIter<'a> {
    type Item = (&'a [u8], u32);
    fn next(&mut self) -> Option<(&'a [u8], u32)> {
        match self {
            SetBytesIter::Flat { spans, blob, i } => {
                let sp = spans.get(*i)?;
                *i += 1;
                Some((&blob[sp.off as usize..(sp.off + sp.len) as usize], sp.hash))
            }
            SetBytesIter::Live { d, i, n } => {
                if *i >= *n {
                    return None;
                }
                let (off, len, h) = d.bytes_span(*i);
                *i += 1;
                Some((d.bytes_content(off, len), h))
            }
            SetBytesIter::Empty => None,
        }
    }
}

impl PdHandedTable {
    #[inline]
    fn set_ints(&self, si: usize) -> &[i64] {
        if !self.live_sets.is_empty() {
            // SAFETY: single-claimer-per-partition contract (Sync note); a
            // taken set reads empty.
            return unsafe { (*self.live_sets[si].get()).as_ref().map_or(&[], |d| d.ints()) };
        }
        &self.set_ints[self.set_int_offs[si] as usize..self.set_int_offs[si + 1] as usize]
    }

    /// Iterate (content, hash) of set `si`'s byte elements (both forms).
    fn set_bytes(&self, si: usize) -> SetBytesIter<'_> {
        if !self.live_sets.is_empty() {
            // SAFETY: as `set_ints`.
            return match unsafe { (*self.live_sets[si].get()).as_ref() } {
                Some(d) => SetBytesIter::Live { d, i: 0, n: d.n_bytes() },
                None => SetBytesIter::Empty,
            };
        }
        SetBytesIter::Flat {
            spans: &self.set_spans
                [self.set_span_offs[si] as usize..self.set_span_offs[si + 1] as usize],
            blob: &self.set_blob,
            i: 0,
        }
    }

    /// LIVE form: move set `si` out (the low-width steal; `None` = flat
    /// form or already taken). SAFETY (caller): only the claimer of the
    /// set's group partition, with no live `set_ints`/`set_bytes` borrow of
    /// the same cell.
    #[inline]
    fn take_live_set(&self, si: usize) -> Option<DistinctSet<'static>> {
        if self.live_sets.is_empty() {
            return None;
        }
        // SAFETY: single-claimer-per-partition contract (Sync note).
        unsafe { (*self.live_sets[si].get()).take() }
    }

    /// Set `si`'s int-value count without materializing the slice (the
    /// combine pre-count; flat semantics — bytes sets count 0 here).
    #[inline]
    fn set_int_len(&self, si: usize) -> usize {
        if !self.live_sets.is_empty() {
            // SAFETY: as `set_ints`.
            return unsafe {
                (*self.live_sets[si].get()).as_ref().map_or(0, |d| d.ints().len())
            };
        }
        (self.set_int_offs[si + 1] - self.set_int_offs[si]) as usize
    }

    pub fn mem_bytes(&self) -> usize {
        self.keys.len() * 8
            + self.key_arena.len()
            + self.keynulls.len() * 4
            + self.hashes.len() * 8
            + self.states.len() * 8
            + self.set_ints.len() * 8
            + self.set_blob.len()
            + self.set_spans.len() * core::mem::size_of::<PdSpan>()
            + self.set_null.len()
            // SAFETY: as `set_ints` (leader-side callers only see never-live
            // hybrid tables; sink-side metering happens pre-combine).
            + self
                .live_sets
                .iter()
                .map(|c| unsafe { (*c.get()).as_ref().map_or(0, |d| d.mem_bytes()) })
                .sum::<usize>()
    }
}

// SAFETY: plain owned data, no interior pointers (live sets are owned,
// never-spilled, global-allocator data — the PdSinkLocal Send argument).
unsafe impl Send for PdHandedTable {}
// SAFETY: shared reads are confined by the sink contract — the combine
// visits each GROUP PARTITION exactly once, by a single claimer, and every
// set index `si` belongs to exactly one partition (its group's top-8 hash
// bucket, the partition index), so cell `live_sets[si]` is touched (read OR
// taken) by that partition's sole claimer only: shared references never
// coexist with the take. Flat fields are read-only after construction.
unsafe impl Sync for PdHandedTable {}

// ===========================================================================
// Merged output (either merge path) — consumed by the emit adoptions.
// ===========================================================================

pub struct PdMerged<'mcx> {
    pub ngroups: usize,
    pub keys: Vec<i64>,
    /// Bytes-key content (spans packed in `keys`; empty for int-only
    /// specs). The emit adoption materializes text datums from it.
    pub key_arena: Vec<u8>,
    pub keynulls: Vec<u32>,
    /// (acc, count) pairs, stride 2*nvocab.
    pub states: Vec<i64>,
    pub(crate) dsets: Vec<Option<DistinctSet<'mcx>>>,
}

impl PdMerged<'_> {
    /// Retained CONTENT bytes of one merged bucket (R3 accounting for the
    /// combine phase — the merged result is held until the leader adopts).
    /// Deliberately len-based, matching `PdHandedTable::mem_bytes`'s
    /// convention: the envelope check compares against the sum of the
    /// sealed tables' CONTENT, and capacity-based counting (Vec doubling +
    /// probe-table roundup on freshly rebuilt sets, ~2-4x slack) would
    /// spuriously cross it for legitimately near-budget merges (review
    /// finding R1). DistinctSet::mem_bytes is the builder's own metering,
    /// shared with the accept-phase budget.
    pub fn mem_bytes(&self) -> usize {
        self.keys.len() * 8
            + self.key_arena.len()
            + self.keynulls.len() * 4
            + self.states.len() * 8
            + self.dsets.len() * core::mem::size_of::<Option<DistinctSet<'_>>>()
            + self.dsets.iter().flatten().map(|d| d.mem_bytes()).sum::<usize>()
    }
}

impl PdMerged<'static> {
    /// Rebind a scoped-thread-built (never-spilled) merge result to the
    /// node's `'mcx` (see `DistinctSet::unspilled_into`).
    pub fn into_lt<'m>(self) -> PdMerged<'m> {
        PdMerged {
            ngroups: self.ngroups,
            keys: self.keys,
            key_arena: self.key_arena,
            keynulls: self.keynulls,
            states: self.states,
            dsets: self.dsets.into_iter().map(|d| d.map(DistinctSet::unspilled_into)).collect(),
        }
    }
}

// ===========================================================================
// Parallel merge — bucket-claim over group partitions (grouped) or element
// partitions (plain). Fast path only: every input in memory, no spills.
// ===========================================================================

fn merge_bucket(spec: &PdSpec, tables: &[PdHandedTable], b: usize) -> PdMerged<'static> {
    let refs: Vec<&PdHandedTable> = tables.iter().collect();
    merge_bucket_refs(spec, &refs, b)
}

/// The bucket merge over BORROWED tables (the M3.5 combine mixes sealed
/// in-memory tables with spill-synthesized ones it owns locally; everything
/// else about the merge is the donor verbatim). Implemented as the
/// incremental [`PdBucketMerger`] driven in one pass, so the donor
/// semantics stay single-sourced.
fn merge_bucket_refs(spec: &PdSpec, tables: &[&PdHandedTable], b: usize) -> PdMerged<'static> {
    let mut m = PdBucketMerger::new(spec);
    // dedupsub reserve wave: the union is bounded by the donors' bucket-b
    // group counts (partition indexes — O(tables) reads).
    m.seed_groups(
        tables
            .iter()
            .filter_map(|t| t.parts.as_ref())
            .map(|p| (p.starts[b + 1] - p.starts[b]) as usize)
            .sum(),
    );
    // q9internals inc-3: all donors are known here — arm the anticipatory
    // per-set reserve (the split paths keep the exact per-donor bound).
    m.set_donor_hint(tables.len());
    for &t in tables {
        m.absorb(t, b);
    }
    m.finish()
}

/// Incremental donor bucket merge (M3.5 inc-3b): [`merge_bucket_refs`]'s
/// loop body, restructured so the combine-split path can absorb tables IN
/// SEQUENCE — the sealed in-memory tables in one pass, then one value-hash
/// slice's synthesized table at a time, dropping each between absorbs so
/// transient memory stays bounded.
///
/// EXACTLY-ONCE LAW (the inc-3b hazard): value-hash slices partition each
/// group's VALUE SET disjointly, but everything that is NOT a per-value
/// fact must merge exactly once, not once per slice. The sealed IN-MEMORY
/// tables are the sole carriers of group-level state — vocab (acc,count)
/// words, `seen_null`, and group existence (a spilled record can never
/// reference a group its own Local's remainder lacks: groups are created
/// at accept and the epoch reset clears only set VALUES) — and they are
/// absorbed ONCE, before any slice. Each slice's synthesized table
/// ([`pd_table_from_spill`]) is built by replaying value records through a
/// fresh builder: its vocab states are all ZERO and its `set_null` faces
/// all FALSE (`create_group` zero-init; NULLs never touch the file), so
/// absorbing it adds 0 to every vocab word, ORs `false` into every
/// `seen_null`, and contributes ONLY set-value insertions — idempotent,
/// over slices that are disjoint by the routing law. Hence "in-memory once
/// + slices in any sequence" equals the direct one-pass donor merge
/// (property test `split_slice_merge_invariance`).
pub struct PdBucketMerger<'s> {
    spec: &'s PdSpec,
    out: PdMerged<'static>,
    /// Bucket-local open-addressed probe over the output groups.
    table: Vec<u32>,
    hashes: Vec<u64>,
    /// q9internals inc-3: total donors this merge will absorb (0 = unknown
    /// — per-donor exact reserves, the pre-inc-3 behavior) + how many have
    /// been absorbed. Known donor counts let the per-set reserve target
    /// anticipate the donors still to come instead of re-growing (and
    /// re-rehashing the whole set) once per donor.
    donor_hint: usize,
    absorbed: usize,
}

impl<'s> PdBucketMerger<'s> {
    pub fn new(spec: &'s PdSpec) -> PdBucketMerger<'s> {
        PdBucketMerger {
            spec,
            out: PdMerged {
                ngroups: 0,
                keys: Vec::new(),
                key_arena: Vec::new(),
                keynulls: Vec::new(),
                states: Vec::new(),
                dsets: Vec::new(),
            },
            table: vec![0; 64],
            hashes: Vec::new(),
            donor_hint: 0,
            absorbed: 0,
        }
    }

    /// distinct-internals inc-3: declare the total donor count (the non-split
    /// combine knows all its tables up front). ONLY a reserve-geometry
    /// input: with n roughly-equal donors the per-donor exact bound
    /// re-grows the same dst set ~log2(n) times, each grow a full-set
    /// rehash (~2x final len of reinsert volume at the clustered-key class's disjoint
    /// per-worker sets); scaling the target by the donors still to come
    /// makes the first reserve final. The split paths keep hint 0: their
    /// per-absorb budget checks (mem_bytes-driven split decisions) must
    /// not see anticipatory capacity.
    pub fn set_donor_hint(&mut self, n: usize) {
        self.donor_hint = n;
    }

    /// dedupsub reserve wave (vecaudit boardable item): pre-size the
    /// bucket-local probe table and output vecs from an UPPER BOUND on the
    /// merged group count (union ≤ Σ donors; the caller reads it off the
    /// partition indexes). One allocation instead of the 64-doubling ladder
    /// with its full-table rehash sweeps. Empty-merger only; 0 or the
    /// GROW_PROJECT kill switch = today's ladder exactly. Geometry only —
    /// group order stays first-seen across donors, verdicts unchanged.
    pub fn seed_groups(&mut self, upper: usize) {
        debug_assert_eq!(self.out.ngroups, 0, "seed before any absorb");
        if upper == 0 || !pd_grow_project_enabled() {
            return;
        }
        let nkeys = self.spec.nkeys();
        let nvocab = self.spec.vocab.len();
        let nsets = self.spec.sets.len();
        self.out.keys.reserve(upper * nkeys);
        self.out.keynulls.reserve(upper);
        self.out.states.reserve(upper * 2 * nvocab);
        self.out.dsets.reserve(upper * nsets);
        self.hashes.reserve(upper);
        // 7/8 max load (absorb's grow law): cap ≥ (upper+1)*8/7 never grows.
        let cap = ((upper + 1) * 8 / 7 + 1).next_power_of_two().max(64);
        if cap > self.table.len() {
            self.table = vec![0u32; cap];
        }
    }

    /// Merge bucket `b` of `t` into the output — the donor loop body
    /// verbatim (bytes-key components compare and copy CONTENT: span words
    /// are table-relative, so the output re-packs spans over its own arena).
    pub fn absorb(&mut self, t: &PdHandedTable, b: usize) {
        let nkeys = self.spec.nkeys();
        let nvocab = self.spec.vocab.len();
        let nsets = self.spec.sets.len();
        let has_bytes = self.spec.has_bytes_keys();
        let key_kinds = &self.spec.key_kinds;
        self.absorbed += 1;
        // q9internals inc-3: donors still to come INCLUDING this one (>= 1;
        // 1 when the hint is unset/exhausted = the per-donor exact bound).
        let donors_left = (self.donor_hint + 1).saturating_sub(self.absorbed).max(1);
        let PdBucketMerger { out, table, hashes, .. } = self;
        let parts = t.parts.as_ref().expect("grouped tables are partitioned");
        let (s, e) = (parts.starts[b] as usize, parts.starts[b + 1] as usize);
        for &g in &parts.idx[s..e] {
            let g = g as usize;
            let words = &t.keys[g * nkeys..(g + 1) * nkeys];
            let nulls = t.keynulls[g];
            let h = t.hashes[g];
            // Probe.
            let mut mask = table.len() - 1;
            let mut slot = (h as usize) & mask;
            let mut created = false;
            let dst = loop {
                match table[slot] {
                    0 => {
                        created = true;
                        let d = out.ngroups;
                        out.ngroups += 1;
                        hashes.push(h);
                        if !has_bytes {
                            out.keys.extend_from_slice(words);
                        } else {
                            for (i, kind) in key_kinds.iter().enumerate() {
                                match kind {
                                    PdKeyKind::Int(_) => out.keys.push(words[i]),
                                    PdKeyKind::Bytes => {
                                        if nulls & (1 << i) != 0 {
                                            out.keys.push(0);
                                        } else {
                                            let (o, l) = unpack_span(words[i]);
                                            let noff = out.key_arena.len();
                                            out.key_arena
                                                .extend_from_slice(&t.key_arena[o..o + l]);
                                            out.keys.push(pack_span(noff, l));
                                        }
                                    }
                                }
                            }
                        }
                        out.keynulls.push(nulls);
                        out.states.extend(core::iter::repeat(0i64).take(2 * nvocab));
                        for _ in 0..nsets {
                            out.dsets.push(Some(DistinctSet::new()));
                        }
                        table[slot] = (d + 1) as u32;
                        if (out.ngroups + 1) * 8 > table.len() * 7 {
                            let new_len = table.len() * 2;
                            mask = new_len - 1;
                            let mut nt = vec![0u32; new_len];
                            for (gg, &hh) in hashes.iter().enumerate() {
                                let mut sl = (hh as usize) & mask;
                                while nt[sl] != 0 {
                                    sl = (sl + 1) & mask;
                                }
                                nt[sl] = (gg + 1) as u32;
                            }
                            *table = nt;
                        }
                        break d;
                    }
                    e2 => {
                        let d = (e2 - 1) as usize;
                        let keys_eq = hashes[d] == h && out.keynulls[d] == nulls && {
                            let dw = &out.keys[d * nkeys..(d + 1) * nkeys];
                            if !has_bytes {
                                dw == words
                            } else {
                                key_kinds.iter().enumerate().all(|(i, kind)| match kind {
                                    PdKeyKind::Int(_) => dw[i] == words[i],
                                    PdKeyKind::Bytes => {
                                        if nulls & (1 << i) != 0 {
                                            true
                                        } else {
                                            let (od, ld) = unpack_span(dw[i]);
                                            let (ot, lt) = unpack_span(words[i]);
                                            out.key_arena[od..od + ld]
                                                == t.key_arena[ot..ot + lt]
                                        }
                                    }
                                })
                            }
                        };
                        if keys_eq {
                            break d;
                        }
                        slot = (slot + 1) & mask;
                    }
                }
            };
            for vi in 0..2 * nvocab {
                out.states[dst * 2 * nvocab + vi] += t.states[g * 2 * nvocab + vi];
            }
            for j in 0..nsets {
                let si = g * nsets + j;
                // GL-LOWDIST-1 steal arm: a NEW output group whose donor is
                // a LIVE-form table adopts the donor's whole set (probe
                // table intact — none of its values re-hash, re-probe, or
                // copy). Only live-form tables (the low-width sink seal)
                // ever yield one; the stolen set carries its donor's
                // values AND `seen_null`, so skipping the generic insert +
                // set_null OR for THIS donor merges exactly the same facts.
                if created {
                    if let Some(stolen) = t.take_live_set(si) {
                        out.dsets[dst * nsets + j] = Some(stolen);
                        continue;
                    }
                }
                let dset = out.dsets[dst * nsets + j].as_mut().unwrap();
                let vals = t.set_ints(si);
                // dedupsub I3, combine face: the union is bounded by
                // held + incoming — an EXACT pre-size (no projection
                // needed), one jump instead of the per-absorb doubling
                // ladder. Int values only (bytes sets pass an empty
                // `vals`; the emptiness guards in reserve_projected keep
                // bytes/replay arms untouched). q9internals inc-3: with a
                // donor hint, anticipate the donors still to come (equal-
                // share extrapolation of THIS donor's contribution) so the
                // first reserve is the last — the per-donor exact bound
                // re-grew the same big set once per donor, a full-set
                // rehash each time. Geometry only; grow_to_cap census
                // measured 5.05%/4.19% of grouped-DISTINCT rt16 cycles at t23.
                if !vals.is_empty() && pd_grow_project_enabled() {
                    let target = dset.len() + vals.len() * donors_left;
                    if target >= PD_PROJECT_MIN {
                        dset.reserve_projected(target);
                    }
                }
                for &v in vals {
                    dset.insert_i64(v);
                }
                for (content, _) in t.set_bytes(si) {
                    dset.insert_bytes(content);
                }
                if t.set_null[si] {
                    dset.seen_null = true;
                }
            }
        }
    }

    /// Capacity-based bytes of the merged-so-far bucket — the combine
    /// split's EXACT dedup-aware budget check (read after every slice
    /// absorb; no directory estimate can see through duplicates, this can).
    pub fn mem_bytes(&self) -> usize {
        self.out.keys.capacity() * 8
            + self.out.key_arena.capacity()
            + self.out.keynulls.capacity() * 4
            + self.out.states.capacity() * 8
            + self.hashes.capacity() * 8
            + self.table.capacity() * 4
            + self.out.dsets.capacity() * core::mem::size_of::<Option<DistinctSet<'static>>>()
            + self
                .out
                .dsets
                .iter()
                .map(|d| d.as_ref().map_or(0, |d| d.mem_bytes()))
                .sum::<usize>()
    }

    pub fn finish(self) -> PdMerged<'static> {
        self.out
    }
}

/// Append one bucket's merged output onto the accumulating result. Bytes-
/// key span words are ARENA-RELATIVE, so they re-base onto the combined
/// arena as the bucket's content is appended (int-only specs take the
/// plain extends).
fn concat_merged_into(spec: &PdSpec, merged: &mut PdMerged<'static>, m: PdMerged<'static>) {
    let nkeys = spec.nkeys();
    if spec.has_bytes_keys() && !m.keys.is_empty() {
        let base = merged.key_arena.len();
        let mut keys = m.keys;
        for g in 0..m.ngroups {
            for (i, kind) in spec.key_kinds.iter().enumerate() {
                if matches!(kind, PdKeyKind::Bytes) && m.keynulls[g] & (1 << i) == 0 {
                    let (o, l) = unpack_span(keys[g * nkeys + i]);
                    keys[g * nkeys + i] = pack_span(base + o, l);
                }
            }
        }
        merged.keys.extend(keys);
        merged.key_arena.extend(m.key_arena);
    } else {
        merged.keys.extend(m.keys);
        merged.key_arena.extend(m.key_arena);
    }
    merged.ngroups += m.ngroups;
    merged.keynulls.extend(m.keynulls);
    merged.states.extend(m.states);
    merged.dsets.extend(m.dsets);
}

// --- plain (single-group) parallel union over element partitions ----------

// ===========================================================================
// Spec derivation — the leader's vocabulary check over its initialized
// AggStateData. Everything here is per-plan static.
// ===========================================================================

/// Map an AGGREGATE (Aggref.aggfnoid — what the derivation actually holds)
/// to its vocab kind given the (single) argument's outer attno + width.
/// These aggregates' transfns are exactly the
/// `order_insensitive_exact_transfn` whitelist minus the Int128 family
/// (count(*)→int8inc, count(any)→int8inc_any, sum(int2/4)→int2/4_sum,
/// avg(int2/4)→int2/4_avg_accum; avg/sum(int8) accumulate Int128/numeric —
/// v1 refusal).
///
/// HISTORY NOTE (m2-distinct-sink): the original table listed the TRANSFN
/// proc oids while the caller passed `ar.aggfnoid` — no vocab shape could
/// ever derive. Unobservable under the Gather-era arm (its v1 economics
/// refused non-empty vocab before deriving); found by the sink's mixed-fold-class
/// e2e engagement coverage.
pub(crate) fn vocab_kind(aggfnoid: Oid, att: Option<(u16, PdInt)>) -> Option<PdVocabKind> {
    /// pg_proc: count(*) / count(any) / sum(int2) / sum(int4) /
    /// avg(int2) / avg(int4).
    const AGG_COUNT_STAR: Oid = 2803;
    const AGG_COUNT_ANY: Oid = 2147;
    const AGG_SUM_INT2: Oid = 2109;
    const AGG_SUM_INT4: Oid = 2108;
    const AGG_AVG_INT2: Oid = 2102;
    const AGG_AVG_INT4: Oid = 2101;
    match aggfnoid {
        AGG_COUNT_STAR => Some(PdVocabKind::CountStar),
        AGG_COUNT_ANY => att.map(|(a, _)| PdVocabKind::CountAny { att: a }),
        AGG_SUM_INT2 => att.and_then(|(a, k)| {
            (k == PdInt::I16).then_some(PdVocabKind::SumInt { att: a, kind: k })
        }),
        AGG_SUM_INT4 => att.and_then(|(a, k)| {
            (k == PdInt::I32).then_some(PdVocabKind::SumInt { att: a, kind: k })
        }),
        AGG_AVG_INT2 => att.and_then(|(a, k)| {
            (k == PdInt::I16).then_some(PdVocabKind::AvgInt { att: a, kind: k })
        }),
        AGG_AVG_INT4 => att.and_then(|(a, k)| {
            (k == PdInt::I32).then_some(PdVocabKind::AvgInt { att: a, kind: k })
        }),
        _ => None,
    }
}

/// Uniform internal error for impossible wire states (defensive; never
/// expected to fire).
#[cold]
pub(crate) fn pd_internal(msg: &str) -> Box<PgError> {
    Box::new(PgError::error(format!("pardistinct internal: {msg}")))
}

// ===========================================================================
// M2 runtime-sink surface (m2-distinct-sink): the donor machinery above,
// re-shaped for the morsel runtime's SealedParallelSink contract. The
// builder becomes a lifetime-erased, Send-able worker Local; freeze is the
// seal; `pd_merge_bucket` is the per-partition combine; the concatenation
// helper assembles the published merged result. The Gather-era registry /
// handoff / leader-partial machinery above is NOT used by the sink (it
// remains the compat path until the runtime arm subsumes it).
// ===========================================================================

/// A worker-side [`PdBuilder`] with its `'mcx` lifetime erased to `'static`.
///
/// SOUNDNESS (the module-level worker discipline, made a type invariant):
/// the wrapped builder is constructed with `mcx: None`, so it can never
/// spill and never holds an arena handle; every byte it retains is owned
/// plain data (`DistinctSet` copies inserted content into its own blob; the
/// detoast scratch lives in the CALLER's per-tuple context and is reset per
/// row). Nothing borrowed from any `EStateData` survives an `accept` call,
/// which is what makes the lifetime erasure and the `Send` below sound —
/// the same self-contained-buffer argument as [`PdHandedTable`].
pub struct PdSinkLocal {
    builder: PdBuilder<'static>,
}

// SAFETY: `mcx` is `None` by construction (`new` is the only constructor)
// and `DistinctSet` without spill state is owned plain data; see the type
// doc.
unsafe impl Send for PdSinkLocal {}

/// batch-insert lane kill switch: `PGRUST_RUNTIME_DISTINCT_BATCH_INSERT=0`
/// restores the per-row insert schedule exactly (default ON; engagement is
/// additionally shape-admitted per [`PdBuilder::set_batch_insert`]).
pub fn pd_batch_insert_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_DISTINCT_BATCH_INSERT").map_or(true, |v| v != "0")
    })
}

impl PdSinkLocal {
    pub fn new(spec: Arc<PdSpec>, budget: usize) -> PdSinkLocal {
        let mut builder = PdBuilder::new(spec, budget, None);
        if pd_batch_insert_enabled() {
            builder.set_batch_insert(true);
        }
        PdSinkLocal { builder }
    }

    /// Whether the staged batch-insert schedule is armed (trace visibility).
    pub fn batch_insert_armed(&self) -> bool {
        self.builder.batch_insert_armed()
    }

    /// Feed one row (the worker accept). `PdFeed::Crossed` = the worker
    /// budget crossed — the sink arm's policy is abort-and-refuse (the
    /// runtime has no degrade target; the leader reruns the serial arm).
    #[inline]
    pub fn accept<'mcx>(
        &mut self,
        estate: &mut EStateData<'mcx>,
        id: ExecSlotId,
        tmp: EcxtId,
    ) -> PgResult<PdFeed> {
        // SAFETY: pure lifetime erasure — the `mcx: None` builder retains no
        // borrow from `estate` (type invariant above); shortening `'static`
        // to `'mcx` on the receiver is the safe direction for every field
        // the call can touch.
        let b: &mut PdBuilder<'mcx> =
            unsafe { core::mem::transmute::<&mut PdBuilder<'static>, &mut PdBuilder<'mcx>>(
                &mut self.builder,
            ) };
        b.accept(estate, id, tmp)
    }

    /// GL-VECACCEPT-1: can THIS Local run the vectorized whole-granule
    /// accept schedule ([`PdBuilder::vec_admissible`])? The engagement's
    /// lane plan ([`pd_vec_plan`]) is the geometry half; this is the
    /// builder half (staged set-insert armed — incl. its kill switch).
    pub fn vec_admissible(&self) -> bool {
        self.builder.vec_admissible()
    }

    /// GL-VECACCEPT-1 phases 1-3 (batch hash → prefetched probe/resolve →
    /// rider folds) over one granule's canonicalized lanes. See
    /// [`PdBuilder::vec_resolve_fold`].
    pub fn vec_resolve_fold(
        &mut self,
        keys: &[i64],
        riders: &[Option<&[i64]>],
        vs: &mut PdVecScratch,
    ) {
        self.builder.vec_resolve_fold(keys, riders, vs)
    }

    /// GL-VECACCEPT-1 phase 4 (the staged distinct-set feed, resumable
    /// after a spill epoch). See [`PdBuilder::vec_stage_sets`].
    pub fn vec_stage_sets(
        &mut self,
        gids: &[u32],
        vals: &[i64],
        from: usize,
    ) -> PgResult<(PdFeed, usize)> {
        self.builder.vec_stage_sets(gids, vals, from)
    }

    /// Seal: freeze into the partitioned wire form.
    pub fn freeze(self) -> PgResult<PdHandedTable> {
        self.builder.freeze()
    }

    /// GL-LOWDIST-1 seal: freeze into the LIVE-form table (grouped specs;
    /// see [`PdBuilder::freeze_live`]) — the low-width combine's steal
    /// substrate.
    pub fn freeze_live(self) -> PgResult<PdHandedTable> {
        self.builder.freeze_live()
    }

    pub fn ngroups(&self) -> usize {
        self.builder.ngroups()
    }

    pub fn mem_bytes(&self) -> usize {
        self.builder.mem_bytes()
    }
}

/// An empty, well-formed GROUPED handed table (the seal error path's
/// placeholder: the RG is already aborting, but the wire shape must stay
/// consumable by any combine that races the abort observation).
pub fn pd_empty_grouped_table(spec: &Arc<PdSpec>) -> PdHandedTable {
    PdBuilder::new(Arc::clone(spec), usize::MAX, None)
        .freeze()
        .expect("freezing an empty builder cannot fail")
}

/// Number of grouped combine partitions (the sink's partition space).
pub const PD_SINK_GROUP_PARTS: u64 = PD_GROUP_PARTS as u64;

/// Merge ONE group partition across the sealed tables (slice order = worker
/// slot order = the combine's deterministic input order). This is the
/// donors' `merge_bucket` verbatim, exposed for the runtime sink's
/// partition-claim combine.
pub fn pd_merge_bucket(
    spec: &PdSpec,
    tables: &[PdHandedTable],
    bucket: usize,
) -> PdMerged<'static> {
    merge_bucket(spec, tables, bucket)
}

/// [`pd_merge_bucket`] over borrowed tables: the M3.5 spill combine merges
/// the sealed in-memory tables together with spill-synthesized tables it
/// builds (and owns) on the combine thread.
pub fn pd_merge_bucket_refs(
    spec: &PdSpec,
    tables: &[&PdHandedTable],
    bucket: usize,
) -> PdMerged<'static> {
    merge_bucket_refs(spec, tables, bucket)
}

/// Concatenate per-bucket merge outputs (bucket order) into the one merged
/// result — the grouped parallel merge's tail, exposed for the sink's
/// finalize.
pub fn pd_concat_buckets(spec: &PdSpec, buckets: Vec<PdMerged<'static>>) -> PdMerged<'static> {
    let mut merged = PdMerged {
        ngroups: 0,
        keys: Vec::new(),
        key_arena: Vec::new(),
        keynulls: Vec::new(),
        states: Vec::new(),
        dsets: Vec::new(),
    };
    for m in buckets {
        concat_merged_into(spec, &mut merged, m);
    }
    merged
}

/// A merged result crossing threads (helper finalize → parked leader).
///
/// SAFETY invariant: only ever constructed from `pd_merge_bucket` outputs —
/// bucket merges build FRESH, never-spilled `DistinctSet<'static>`s (no
/// tape state, no arena handles), so the payload is owned plain data.
pub struct PdSinkMerged(PdMerged<'static>);

// SAFETY: see the type doc (never-spilled sets are owned plain data — the
// `PdHandedTable` argument).
unsafe impl Send for PdSinkMerged {}

impl PdSinkMerged {
    pub fn new(merged: PdMerged<'static>) -> PdSinkMerged {
        PdSinkMerged(merged)
    }

    /// Rebind to the consuming node's `'mcx` (the `into_lt` law: sound for
    /// never-spilled merge results, which is the constructor's invariant).
    pub fn into_merged<'m>(self) -> PdMerged<'m> {
        self.0.into_lt()
    }

    pub fn ngroups(&self) -> usize {
        self.0.ngroups
    }
}

// ===========================================================================
// PAREMIT — emission-in-combine for the runtime distinct sink (m2-sinks §6
// applied to donor B; the winners-phase2 near-unique car). For ADMITTED shapes —
// a pure column shuffle of group keys and identity-finalized aggregate
// results (`pd_paremit_cols` in lib.rs; the merge.rs `build_emit_plan`
// precedent, which is how this sink's paremit worked BEFORE the adopt
// generalization centralized emission on the leader) — each COMBINE claim
// materializes its partition's fully-projected output rows in the plan
// Sort's prefix order, and the leader's emit collapses to a cross-bucket
// ordered merge + a datum memcpy per row. The adopt tail's bucket concat,
// rep synthesis, full-table order_groups, and per-group finalize/project
// (the measured ~1.4s-of-1.7s serial floor at near-unique@10M rt16, 835k groups)
// all move into the parallel combine phase.
//
// Byte identity vs the adopt arm:
//  (a) group ORDER — per-bucket `order_groups` + the leader merge run the
//      SAME comparator (`hashgrouped::cmp_group_rows`, one authority, same
//      collation): group keys are distinct and the prefix order is total,
//      so the sorted sequence is unique — identical whether the full group
//      set is sorted at adopt or merged from disjoint sorted buckets.
//  (b) key DATUMS — the same 4B-header text varlena images and
//      width-matched int datums `agg_hashgroup_adopt_merged` synthesizes.
//  (c) aggregate RESULTS — count(DISTINCT) = the merged exact set's value
//      count (`DistinctSet::value_count`: n strict int8inc_any replays
//      from initcond '0' add exactly n; the at-most-one NULL strict-skips,
//      contributing 0); vocab count/sum materialize exactly as the adopt
//      override writes them; the admitted aggregates carry no finalfn
//      (identity finalize), so the materialized datum IS the emitted one.
//  (d) no HAVING on admitted shapes ⇒ one row per group — the adopt
//      emit's exact row set.
// ===========================================================================

use crate::hashgrouped::{cmp_group_rows, order_groups, HashGroupOrderKey, HgKeyKind};

/// One projected output column of a paremit engagement, resolved against
/// the engagement's [`PdSpec`] (see [`pd_paremit_recipe`]).
#[derive(Clone, Copy, Debug)]
pub enum PdEmitCol {
    /// Group key component `i` (`spec.key_atts` order = grpColIdx order).
    Key(usize),
    /// count(DISTINCT x): the merged exact set's non-null value count
    /// (set index = `pertrans_sort` slot = `spec.sets` index).
    SetCount(usize),
    /// count(*) / count(x) vocab final: `acc`, never NULL (initcond '0').
    VocabCount(usize),
    /// sum(int2/int4) vocab final: `acc`, NULL iff `count == 0` (the
    /// non-strict null-initval law the adopt override mirrors).
    VocabSum(usize),
}

/// The leader-side tlist analysis' column vocabulary (spec-independent —
/// derived by `pd_paremit_cols` before the engagement's `PdSpec` exists,
/// so the economics tier can price the paremit shape; resolved into
/// [`PdEmitCol`] by [`pd_paremit_recipe`] once the spec is derived).
#[derive(Clone, Copy, Debug)]
pub enum PdParemitCol {
    Key(usize),
    /// count(DISTINCT): `pertrans_sort` slot index (== `spec.sets` index —
    /// `pd_derive_spec` builds sets in `pertrans_sort` order, one per slot).
    SetCount(usize),
    /// Non-distinct vocab aggregate, keyed by transno until the spec's
    /// vocab table exists. `sum` = the SumInt NULL-iff-count-0 law.
    Vocab { transno: u32, sum: bool },
}

/// The leader-derived paremit recipe (plain data; workers build ordered
/// per-partition emit buckets from it during COMBINE).
pub struct PdEmitRecipe {
    pub cols: Vec<PdEmitCol>,
    /// The plan Sort's prefix order over the group keys — the adopt arm's
    /// `order_spec`, cloned (the ONE ordering authority).
    pub order: Vec<HashGroupOrderKey>,
    pub key_kinds: Vec<PdKeyKind>,
}

impl PdEmitRecipe {
    #[inline]
    fn nkeys(&self) -> usize {
        self.key_kinds.len()
    }

    /// Comparator kinds: only the Text/int distinction matters for
    /// `cmp_group_rows` (int key words are sign-extended values; width is
    /// a datum-synthesis concern).
    fn hg_kinds(&self) -> Vec<HgKeyKind> {
        self.key_kinds
            .iter()
            .map(|k| match k {
                PdKeyKind::Bytes => HgKeyKind::Text,
                PdKeyKind::Int(_) => HgKeyKind::Int64,
            })
            .collect()
    }
}

/// Resolve the tlist analysis against the derived spec. `None` is a
/// fail-closed adopt fallback (structurally unreachable: `pd_derive_spec`
/// covers every transno as a set or a vocab entry).
pub fn pd_paremit_recipe(
    spec: &PdSpec,
    cols: &[PdParemitCol],
    order: &[HashGroupOrderKey],
) -> Option<PdEmitRecipe> {
    let mut out = Vec::with_capacity(cols.len());
    for c in cols {
        out.push(match *c {
            PdParemitCol::Key(i) => PdEmitCol::Key(i),
            PdParemitCol::SetCount(si) => {
                if si >= spec.sets.len() {
                    return None;
                }
                PdEmitCol::SetCount(si)
            }
            PdParemitCol::Vocab { transno, sum } => {
                let vi = spec.vocab.iter().position(|v| v.transno == transno)?;
                if sum {
                    PdEmitCol::VocabSum(vi)
                } else {
                    PdEmitCol::VocabCount(vi)
                }
            }
        });
    }
    Some(PdEmitRecipe {
        cols: out,
        order: order.to_vec(),
        key_kinds: spec.key_kinds.clone(),
    })
}

/// One partition's fully-projected output rows in the plan Sort's prefix
/// order — row-major datums (stride `natts`), self-contained: text datums
/// point into the bucket's OWN `arena` (4B-header varlena images, live
/// past worker teardown; moving the struct never moves the Vec's heap
/// buffer, and the arena is never resized after the fix-up pass). The
/// `keys`/`keynulls` sidecar carries the group key words (text = packed
/// span over the CONTENT bytes inside `arena` — each image's header sits
/// 4 bytes before its span) for the leader's ordered merge.
#[derive(Default)]
pub struct PdEmitBucket {
    pub nrows: usize,
    pub natts: usize,
    pub values: Vec<Datum>,
    pub nulls: Vec<bool>,
    keys: Vec<i64>,
    keynulls: Vec<u32>,
    arena: Vec<u8>,
}

// SAFETY: owned plain data; the datums are byval words or pointers into the
// bucket's own arena (the `PdHandedTable` self-contained-buffer argument).
unsafe impl Send for PdEmitBucket {}
unsafe impl Sync for PdEmitBucket {}

impl PdEmitBucket {
    /// Retained CONTENT bytes (R3 accounting — len-based, matching
    /// `PdMerged::mem_bytes`'s convention; see that doc for why not
    /// capacities).
    pub fn mem_bytes(&self) -> usize {
        self.values.len() * core::mem::size_of::<Datum>()
            + self.nulls.len()
            + self.keys.len() * 8
            + self.keynulls.len() * 4
            + self.arena.len()
    }
}

// ---------------------------------------------------------------------------
// Bounded selection inside the distinct sink (named-kernels-distinct kernel
// 2 — winners-phase2's flagged follow-up): on the paremit `GROUP BY k ORDER
// BY <int8 agg> LIMIT n` consumer shape, each combine claim selects its
// partition's top-`bound` groups ON THE RAW MERGED STATE (the distinct
// count IS the set's value count; vocab counts are the sidecar words —
// both state-comparable and never NULL, so there is no mid-combine decline
// face at all) and materializes ONLY those through the ordered emit bucket;
// the leader truncate-merges the per-partition candidate lists to the
// global winner set and the paremit merge emits winners alone, in the same
// group order as the full drain.
//
// Correctness is the winners-only superset lemma verbatim: partitions hold
// DISJOINT groups (value-hash routing), so a group in the global top-
// `bound` is beaten by fewer than `bound` groups anywhere — in particular
// inside its own partition — and survives its partition's list; the union
// of lists is a superset of the global top-`bound` and the truncate-merge
// recovers exactly it.
//
// Selection total order = (badness, GROUP ORDER): badness first (the
// monotone-worse image of the order value under the direction), ties by
// the plan Sort's prefix order over the group keys — the EXACT arrival
// order the full drain feeds the downstream bounded sort, whose C
// tuplesort keeps the first-arriving tuple among equals (a new tuple
// replaces the heap root only when strictly better). The winner set is
// therefore a pure function of the data AND identical to the set the full
// drain's bounded sort retains — the W≡F byte-identity argument. Within
// one partition the tie rank is the bucket row index (bucket rows are in
// group order); across partitions the leader compares tie candidates with
// `cmp_group_rows` on the bucket key sidecars (the ONE ordering
// authority).
// ---------------------------------------------------------------------------

/// The order value's location in the merged raw state (leader-resolved
/// against the derived spec; both kinds are int8-monotone and never NULL —
/// count states have non-NULL initvals and the merged set's value count is
/// total by construction).
#[derive(Clone, Copy, Debug)]
pub enum PdTopnKey {
    /// count(DISTINCT x): the merged exact set's non-null value count
    /// (`spec.sets` index).
    SetCount(usize),
    /// count(*) / count(x): the vocab sidecar `acc` word (`spec.vocab`
    /// index).
    VocabCount(usize),
}

/// The armed distinct-sink top-N (one engagement-level choice, resolved at
/// admission beside the paremit recipe).
#[derive(Clone, Copy, Debug)]
pub struct PdTopnSpec {
    pub key: PdTopnKey,
    pub desc: bool,
    pub bound: u32,
}

/// One partition's winner candidate: `row` indexes the partition's emit
/// bucket (bucket rows are the partition's candidates in GROUP order, so
/// `row` doubles as the within-partition tie rank). Lists are sorted
/// best-first by `(badness, row)`.
#[derive(Clone, Copy, Debug)]
pub struct PdTopnCand {
    pub badness: u64,
    pub row: u32,
}

/// A group's order value off the raw merged state (never NULL — doc above).
#[inline]
fn pd_topn_value(spec: &PdSpec, m: &PdMerged<'_>, key: PdTopnKey, g: usize) -> i64 {
    match key {
        PdTopnKey::SetCount(si) => m.dsets[g * spec.sets.len() + si]
            .as_ref()
            .map_or(0, DistinctSet::value_count) as i64,
        PdTopnKey::VocabCount(vi) => m.states[g * 2 * spec.vocab.len() + 2 * vi],
    }
}

/// COMBINE-claim tail (paremit mode): order one merged partition's groups
/// by the plan Sort's prefix and materialize the projected rows. Runs on
/// the combine worker; the merged partition (and its sets) drop with the
/// claim — only this compact bucket is retained.
///
/// `topn` (kernel 2): materialize ONLY the partition's top-`bound`
/// candidates (still in group order) and return the candidate list; `None`
/// = the full drain exactly as before. The returned list is `Some` iff
/// `topn` was.
pub fn pd_emit_bucket(
    spec: &PdSpec,
    recipe: &PdEmitRecipe,
    m: &PdMerged<'_>,
    topn: Option<&PdTopnSpec>,
) -> PgResult<(PdEmitBucket, Option<Vec<PdTopnCand>>)> {
    let nkeys = recipe.nkeys();
    debug_assert_eq!(nkeys, spec.nkeys());
    let nsort = spec.sets.len();
    let nvocab = spec.vocab.len();
    let natts = recipe.cols.len();
    let n = m.ngroups;
    let kinds = recipe.hg_kinds();
    let mut order = order_groups(
        &m.keys,
        &m.keynulls,
        &recipe.order,
        nkeys,
        &kinds,
        &m.key_arena,
        n,
    )?;
    // Partition-local bounded selection on the raw states (section doc):
    // scan in group order keeping the `bound` smallest `(badness, rank)`
    // pairs — a max-heap with strict-better replacement, so equal-badness
    // ties keep the EARLIEST group order (pairs are unique by rank). The
    // surviving ranks rewrite `order` (rank-ascending = group order); the
    // candidate list maps each winner to its bucket row.
    let mut cands: Option<Vec<PdTopnCand>> = None;
    if let Some(t) = topn {
        let k = (t.bound as usize).min(n);
        let mut heap: std::collections::BinaryHeap<(u64, u32)> =
            std::collections::BinaryHeap::with_capacity(k.saturating_add(1));
        for (rank, &g) in order.iter().enumerate() {
            let badness = crate::compact::topkfin_badness(
                pd_topn_value(spec, m, t.key, g as usize),
                t.desc,
            );
            let cand = (badness, rank as u32);
            if heap.len() < k {
                heap.push(cand);
            } else if heap.peek().is_some_and(|&worst| cand < worst) {
                heap.pop();
                heap.push(cand);
            }
        }
        let mut sel = heap.into_vec();
        sel.sort_unstable(); // (badness, rank) — the selection total order
        // Ranks ascending = the bucket's row order; a winner's bucket row =
        // its position in the rank-sorted list.
        let mut ranks: Vec<u32> = sel.iter().map(|&(_, r)| r).collect();
        ranks.sort_unstable();
        cands = Some(
            sel.iter()
                .map(|&(badness, rank)| PdTopnCand {
                    badness,
                    row: ranks.binary_search(&rank).expect("winner rank present") as u32,
                })
                .collect(),
        );
        order = ranks.into_iter().map(|r| order[r as usize]).collect();
    }
    let n = order.len();
    let mut out = PdEmitBucket {
        nrows: n,
        natts,
        values: Vec::with_capacity(n * natts),
        nulls: Vec::with_capacity(n * natts),
        keys: Vec::with_capacity(n * nkeys),
        keynulls: Vec::with_capacity(n),
        arena: Vec::new(),
    };
    // Datum fix-ups for arena-backed images, resolved once the arena stops
    // growing (Vec growth may move the heap buffer — the sink.rs
    // `push_image` discipline).
    let mut fixups: Vec<(usize, usize)> = Vec::new();
    // Per-row text image offsets (arena offset of the varlena HEADER),
    // indexed by key component; rebuilt each row.
    let mut img_offs = vec![usize::MAX; nkeys];
    for &g in &order {
        let g = g as usize;
        let knull = m.keynulls[g];
        out.keynulls.push(knull);
        // Sidecar pass: every key component gets a merge-comparable word —
        // text content lands in the bucket arena ONCE per (group, text
        // key), whether or not the projection references it (the order
        // spec may sort on unprojected keys).
        for (i, kind) in recipe.key_kinds.iter().enumerate() {
            if knull & (1 << i) != 0 {
                img_offs[i] = usize::MAX;
                out.keys.push(0);
                continue;
            }
            let w = m.keys[g * nkeys + i];
            match kind {
                PdKeyKind::Int(_) => {
                    img_offs[i] = usize::MAX;
                    out.keys.push(w);
                }
                PdKeyKind::Bytes => {
                    let (off, len) = unpack_span(w);
                    // 8-align the image (varlena consumers read 4-byte
                    // headers + aligned payloads).
                    let pad = (8 - out.arena.len() % 8) % 8;
                    out.arena.resize(out.arena.len() + pad, 0);
                    let img_off = out.arena.len();
                    out.arena.extend_from_slice(
                        &::types_tuple::varatt::set_varsize_4b_word((len + 4) as u32)
                            .to_ne_bytes(),
                    );
                    out.arena.extend_from_slice(&m.key_arena[off..off + len]);
                    img_offs[i] = img_off;
                    // Span over the CONTENT (header + 4) — what the merge
                    // comparator reads.
                    out.keys.push(pack_span(img_off + 4, len));
                }
            }
        }
        for c in &recipe.cols {
            match *c {
                PdEmitCol::Key(i) => {
                    if knull & (1 << i) != 0 {
                        out.values.push(Datum::null());
                        out.nulls.push(true);
                        continue;
                    }
                    let w = m.keys[g * nkeys + i];
                    match recipe.key_kinds[i] {
                        // Width-matched int datums — adopt's synthesis.
                        PdKeyKind::Int(PdInt::I16) => {
                            out.values.push(Datum::from_i16(w as i16));
                            out.nulls.push(false);
                        }
                        PdKeyKind::Int(PdInt::I32) => {
                            out.values.push(Datum::from_i32(w as i32));
                            out.nulls.push(false);
                        }
                        PdKeyKind::Int(PdInt::I64) => {
                            out.values.push(Datum::from_i64(w));
                            out.nulls.push(false);
                        }
                        PdKeyKind::Bytes => {
                            debug_assert_ne!(img_offs[i], usize::MAX);
                            fixups.push((out.values.len(), img_offs[i]));
                            out.values.push(Datum::null());
                            out.nulls.push(false);
                        }
                    }
                }
                PdEmitCol::SetCount(si) => {
                    // The set IS the count (module-section doc, point c);
                    // a group whose set never materialized (only-NULL or
                    // no input values) counts 0 — initcond '0' + zero
                    // non-strict-skipped replays.
                    let count = m.dsets[g * nsort + si]
                        .as_ref()
                        .map_or(0, DistinctSet::value_count);
                    out.values.push(Datum::from_i64(count as i64));
                    out.nulls.push(false);
                }
                PdEmitCol::VocabCount(vi) => {
                    let acc = m.states[g * 2 * nvocab + 2 * vi];
                    out.values.push(Datum::from_i64(acc));
                    out.nulls.push(false);
                }
                PdEmitCol::VocabSum(vi) => {
                    // int2/4_sum: NULL iff no non-null input ever arrived
                    // (the adopt override's exact arm).
                    let acc = m.states[g * 2 * nvocab + 2 * vi];
                    let cnt = m.states[g * 2 * nvocab + 2 * vi + 1];
                    if cnt > 0 {
                        out.values.push(Datum::from_i64(acc));
                        out.nulls.push(false);
                    } else {
                        out.values.push(Datum::null());
                        out.nulls.push(true);
                    }
                }
            }
        }
    }
    // Arena is final — resolve the image datums.
    for (i, off) in fixups {
        out.values[i] = Datum::from_usize(out.arena[off..].as_ptr() as usize);
    }
    Ok((out, cands))
}

/// Leader-side paremit emit state: the published buckets + a binary
/// min-heap of non-empty bucket indices keyed by each bucket's current
/// head row under the recipe order. Comparisons are fallible (varstr_cmp
/// collation seams), so the heap sifts are hand-rolled.
pub struct PdParemitState {
    buckets: Vec<PdEmitBucket>,
    cursors: Vec<usize>,
    heap: Vec<u32>,
    order: Vec<HashGroupOrderKey>,
    kinds: Vec<HgKeyKind>,
    nkeys: usize,
    pub natts: usize,
    /// Kernel-2 winner direction: per-bucket GLOBAL-winner row indexes
    /// (ascending = group order); the merge cursors walk these instead of
    /// the full bucket. `None` = the full drain.
    keep: Option<Vec<Vec<u32>>>,
}

impl PdParemitState {
    /// Row-major datum block of the row `pd_paremit_next` returned.
    #[inline]
    pub fn row(&self, bucket: usize, row: usize) -> (&[Datum], &[bool]) {
        let b = &self.buckets[bucket];
        let base = row * b.natts;
        (&b.values[base..base + b.natts], &b.nulls[base..base + b.natts])
    }

    /// Retained content bytes (the teardown-release floor's input).
    pub fn mem_bytes(&self) -> usize {
        self.buckets.iter().map(PdEmitBucket::mem_bytes).sum()
    }

    /// Global winner count (`Some` iff the selection is armed) — the
    /// composed-top-N trace/observability figure.
    pub fn kept_rows(&self) -> Option<usize> {
        self.keep.as_ref().map(|k| k.iter().map(Vec::len).sum())
    }

    /// Bucket `b`'s CURRENT merge row (winner-directed when `keep` is
    /// armed).
    #[inline]
    fn cur_row(&self, b: usize) -> usize {
        match &self.keep {
            Some(k) => k[b][self.cursors[b]] as usize,
            None => self.cursors[b],
        }
    }

    /// Bucket `b`'s merge row count (winners only when `keep` is armed).
    #[inline]
    fn rows_of(&self, b: usize) -> usize {
        match &self.keep {
            Some(k) => k[b].len(),
            None => self.buckets[b].nrows,
        }
    }

    fn cmp_heads(&self, a: u32, b: u32) -> PgResult<core::cmp::Ordering> {
        let (ba, bb) = (&self.buckets[a as usize], &self.buckets[b as usize]);
        let ord = cmp_group_rows(
            &self.order,
            self.nkeys,
            &self.kinds,
            (&ba.keys, &ba.keynulls, &ba.arena, self.cur_row(a as usize)),
            (&bb.keys, &bb.keynulls, &bb.arena, self.cur_row(b as usize)),
        )?;
        // Distinct groups partition disjointly across buckets: a cross-
        // bucket tie on the full prefix would mean a duplicated group.
        debug_assert_ne!(ord, core::cmp::Ordering::Equal, "group in two buckets");
        Ok(ord)
    }

    fn sift_down(&mut self, mut i: usize) -> PgResult<()> {
        loop {
            let (l, r) = (2 * i + 1, 2 * i + 2);
            let mut best = i;
            if l < self.heap.len()
                && self.cmp_heads(self.heap[l], self.heap[best])? == core::cmp::Ordering::Less
            {
                best = l;
            }
            if r < self.heap.len()
                && self.cmp_heads(self.heap[r], self.heap[best])? == core::cmp::Ordering::Less
            {
                best = r;
            }
            if best == i {
                return Ok(());
            }
            self.heap.swap(i, best);
            i = best;
        }
    }
}

/// Truncate-merge the per-partition candidate lists (each sorted
/// best-first by the selection total order — within a bucket that is
/// `(badness, row)`, and a bucket's rows ARE its group order) into the
/// per-bucket GLOBAL winner row lists (≤ `bound` rows total; each list
/// row-ascending). Cross-bucket badness ties resolve by `cmp_group_rows`
/// over the bucket key sidecars — the global group order, i.e. the exact
/// arrival order the full drain would feed the downstream bounded sort.
/// O(bound × buckets) fallible head compares; bound ≤ the sink cap.
fn pd_topn_keep(
    recipe: &PdEmitRecipe,
    buckets: &[PdEmitBucket],
    cands: &[Vec<PdTopnCand>],
    bound: u32,
) -> PgResult<Vec<Vec<u32>>> {
    debug_assert_eq!(cands.len(), buckets.len());
    let kinds = recipe.hg_kinds();
    let nkeys = recipe.nkeys();
    let mut cur = vec![0usize; cands.len()];
    let mut kept: Vec<Vec<u32>> = vec![Vec::new(); buckets.len()];
    let mut taken = 0u32;
    'take: while taken < bound {
        let mut best: Option<usize> = None;
        for bi in 0..cands.len() {
            let Some(c) = cands[bi].get(cur[bi]) else { continue };
            let better = match best {
                None => true,
                Some(bj) => {
                    let cj = &cands[bj][cur[bj]];
                    match c.badness.cmp(&cj.badness) {
                        core::cmp::Ordering::Less => true,
                        core::cmp::Ordering::Greater => false,
                        core::cmp::Ordering::Equal => {
                            let (ba, bb) = (&buckets[bi], &buckets[bj]);
                            cmp_group_rows(
                                &recipe.order,
                                nkeys,
                                &kinds,
                                (&ba.keys, &ba.keynulls, &ba.arena, c.row as usize),
                                (&bb.keys, &bb.keynulls, &bb.arena, cj.row as usize),
                            )? == core::cmp::Ordering::Less
                        }
                    }
                }
            };
            if better {
                best = Some(bi);
            }
        }
        let Some(bi) = best else { break 'take };
        kept[bi].push(cands[bi][cur[bi]].row);
        cur[bi] += 1;
        taken += 1;
    }
    // Merge cursors walk each bucket in group order = row-ascending.
    for l in &mut kept {
        l.sort_unstable();
    }
    Ok(kept)
}

/// Build the leader emit state (heapify over non-empty buckets).
///
/// `topn` (kernel 2): the per-bucket candidate lists (aligned with
/// `buckets`) + the global bound — the merge then emits ONLY the global
/// winner set, in the same group order as the full drain. `None` = the
/// full drain exactly as before.
pub fn pd_paremit_state(
    recipe: &PdEmitRecipe,
    buckets: Vec<PdEmitBucket>,
    topn: Option<(&[Vec<PdTopnCand>], u32)>,
) -> PgResult<PdParemitState> {
    let natts = recipe.cols.len();
    let keep = match topn {
        Some((cands, bound)) => Some(pd_topn_keep(recipe, &buckets, cands, bound)?),
        None => None,
    };
    let heap: Vec<u32> = buckets
        .iter()
        .enumerate()
        .filter(|(i, b)| match &keep {
            Some(k) => !k[*i].is_empty(),
            None => b.nrows > 0,
        })
        .map(|(i, _)| i as u32)
        .collect();
    let cursors = vec![0usize; buckets.len()];
    let mut st = PdParemitState {
        buckets,
        cursors,
        heap,
        order: recipe.order.clone(),
        kinds: recipe.hg_kinds(),
        nkeys: recipe.nkeys(),
        natts,
        keep,
    };
    if !st.heap.is_empty() {
        for i in (0..st.heap.len() / 2).rev() {
            st.sift_down(i)?;
        }
    }
    Ok(st)
}

/// Next row in the global prefix order: `(bucket, row)` into
/// [`PdParemitState::row`], or `None` at end of stream.
pub fn pd_paremit_next(st: &mut PdParemitState) -> PgResult<Option<(usize, usize)>> {
    let Some(&top) = st.heap.first() else { return Ok(None) };
    let b = top as usize;
    let row = st.cur_row(b);
    st.cursors[b] += 1;
    if st.cursors[b] >= st.rows_of(b) {
        let last = st.heap.len() - 1;
        st.heap.swap(0, last);
        st.heap.pop();
    }
    if !st.heap.is_empty() {
        st.sift_down(0)?;
    }
    Ok(Some((b, row)))
}

// ===========================================================================
// M3.5 accept-side spill surface (docs/design/m3.5-spill.md §4, inc-3a).
// ADDITIVE ONLY: the builder's own Mcx-bound spill machinery (`evict_sets`
// / distinctset `SpillState`) is untouched — the sink Locals still carry
// `mcx: None` and freeze()'s `!ever_spilled` invariant keeps holding. What
// spills here are the DistinctSet VALUES alone, through an operator-owned
// byte contract the caller writes to a spillset file: group keys, vocab
// words, and `seen_null` stay in memory and ride the Local through SEAL.
//
// Record contract, INT mode (fixed width per spec, native-endian — the
// DistinctSet int law, raw i64 words): one record per (group, set, value) =
//   [keynulls u64][key word i64 × nkeys][set index u64][value i64]
// NULLs NEVER touch the file: group-key null bits ride the keynulls word
// (part of the group identity, not a value), and set NULL presence rides
// the in-memory `seen_null` (the distinctset frozen rule).
//
// Record contract, BYTES mode (distinct-bytes car — engaged iff
// `spec.has_bytes_keys()`; variable width, self-describing, 8-aligned):
//   [rec_len u64][keynulls u64][set index u64][value i64]
//   [key word i64 × nkeys (bytes components ZEROED — the c3 canonical-image
//    discipline: content identifies, table-relative spans never spill)]
//   [per bytes component, ascending, non-NULL only: len u64 + content,
//    zero-padded to 8]
// `rec_len` is the whole record (multiple of 8), so files stay 8-aligned
// and a record-aligned streamer can carry partial tails. The VALUE sits at
// the fixed offset 24 in both the parse and the value-hash router. The
// canonical key image (zeroed words + length-prefixed tails in component
// order) is injective: fixed-width prefix + length-prefixed tails.
//
// Partition law (both modes): top-8 bits of the group-key hash
// (`hash >> 56`, canonical `row_hash` — content-derived for bytes keys) —
// EXACTLY the counting-sort partition freeze() builds and the bucket merge
// reads, so a spilled record replays into the same combine partition that
// claims its group. Spill/replay AUTHORITY is insertion order per the m3.5
// design; set-insert idempotence makes replay order immaterial.
// ===========================================================================

/// Whether `spec` spills through the BYTES record format (variable width).
pub fn pd_spill_bytes_mode(spec: &PdSpec) -> bool {
    spec.has_bytes_keys()
}

/// Byte width of one spilled (group, set, value) record for `spec` — INT
/// mode only (bytes-mode records are variable-width; see
/// [`pd_spill_min_record_width`]).
pub fn pd_spill_record_width(spec: &PdSpec) -> usize {
    debug_assert!(!pd_spill_bytes_mode(spec));
    (spec.nkeys() + 3) * 8
}

/// Lower bound on a bytes-mode record's width (header + key words): a
/// conservative row-count divisor for directory-only size estimates
/// (`bytes / min_width` OVER-counts rows — refusals stay conservative).
pub fn pd_spill_min_record_width(spec: &PdSpec) -> usize {
    32 + spec.nkeys() * 8
}

impl PdBuilder<'_> {
    /// Fail-closed shape gate: only grouped int-set builders spill exactly.
    /// Plain/element-partition shapes (nkeys == 0), bytes sets, and anything
    /// touching the leader's Mcx-bound machinery refuse (the caller falls
    /// through to the phase-1 Crossed abort → serial rerun). Bytes GROUP
    /// KEYS are eligible (distinct-bytes car: the bytes record format
    /// carries the canonical key image — this resolves the m35 inc-3a flag
    /// that bytes-mode runs were unrepresentable); bytes SET VALUES still
    /// refuse (the value word is the routing axis and stays i64).
    fn spill_eligible(&self) -> bool {
        self.spec.nkeys() > 0
            && !self.spec.sets.is_empty()
            && self
                .spec
                .sets
                .iter()
                .all(|s| !matches!(s.kind, DistinctKeyKind::Bytes))
            && self.mcx.is_none()
            && !self.ever_spilled
            && !self.frozen
    }

    /// Bytes of set VALUES currently held (what an epoch flush would move to
    /// disk). Observability figure; NOT the worthwhileness yardstick — see
    /// [`Self::spill_freeable_bytes`].
    fn spill_value_bytes(&self) -> usize {
        self.dsets.iter().map(|d| d.ints().len() * 8).sum()
    }

    /// Bytes an epoch flush would RELEASE: the sets' full capacity-based
    /// memory (`total_set_mem`) — `spill_reset_values` SHRINKS the sets, so
    /// the entire set side of `mem()` comes back. This is the caller's
    /// worthwhileness yardstick: a crossing is group-table-dominated exactly
    /// when the set side is a small fraction of the budget (`base_mem`
    /// drives the crossing), and THAT is what value spill cannot help.
    ///
    /// Calibration note (inc-3a followup, battery -82184): the original gate
    /// compared `spill_value_bytes` (payload alone) against budget/4, but
    /// `mem()` moves in capacity steps, so crossings land right after
    /// Vec/IntSet doublings, where the 8-byte payloads are only ~1/6..1/3 of
    /// set memory (IntSet's 50% max load = 16-32 table bytes/value, plus
    /// 8-16 ints-Vec bytes/value). A purely value-dominated uniform corpus
    /// (the grouped-DISTINCT class: 97 sets filling in lockstep, all doubling together)
    /// deterministically sat below budget/4 at every crossing and the arm
    /// fail-closed to the serial fallback on every worker.
    fn spill_freeable_bytes(&self) -> usize {
        self.total_set_mem
    }

    /// Emit every held set value as spill records, partition-contiguous and
    /// partition-ascending (the spillset EpochWriter contract): groups are
    /// counting-sorted by the top-8 hash bits — freeze()'s own partition law
    /// — and each group's sets stream in set order, values in insertion
    /// order. Read-only: the caller resets values via
    /// [`Self::spill_reset_values`] only after its epoch write COMMITS.
    fn spill_emit(&self, emit: &mut dyn FnMut(u32, &[u8]) -> PgResult<()>) -> PgResult<()> {
        debug_assert!(self.spill_eligible());
        // batch-insert lane: spill epochs fire only from a post-flush
        // crossing, so the staged window is empty here by construction.
        debug_assert!(self.stage_v.is_empty(), "spill_emit with a staged window");
        let nkeys = self.spec.nkeys();
        let nsets = self.spec.sets.len();
        let n = self.ngroups();
        // The freeze partition law, verbatim (counting sort, top-8 bits).
        let mut starts = vec![0u32; PD_GROUP_PARTS + 1];
        for &h in &self.hashes {
            starts[(h >> 56) as usize + 1] += 1;
        }
        for p in 0..PD_GROUP_PARTS {
            starts[p + 1] += starts[p];
        }
        let mut idx = vec![0u32; n];
        let mut cur = starts.clone();
        for (g, &h) in self.hashes.iter().enumerate() {
            let b = (h >> 56) as usize;
            idx[cur[b] as usize] = g as u32;
            cur[b] += 1;
        }
        let bytes_mode = pd_spill_bytes_mode(&self.spec);
        let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
        // BYTES mode: the canonical key image (zeroed-span key words +
        // length-prefixed content tails) is built ONCE per group and shared
        // by all its value records (arena-batched: one scratch, no per-value
        // re-walk of the arena).
        let mut img: Vec<u8> = Vec::new();
        for p in 0..PD_GROUP_PARTS {
            buf.clear();
            for &g in &idx[starts[p] as usize..starts[p + 1] as usize] {
                let g = g as usize;
                let gnulls = self.keynulls[g];
                let nulls = (gnulls as u64).to_ne_bytes();
                let keys = &self.keys[g * nkeys..(g + 1) * nkeys];
                let rec_len = if bytes_mode {
                    img.clear();
                    for (i, kind) in self.spec.key_kinds.iter().enumerate() {
                        match kind {
                            PdKeyKind::Int(_) => img.extend_from_slice(&keys[i].to_ne_bytes()),
                            PdKeyKind::Bytes => img.extend_from_slice(&0i64.to_ne_bytes()),
                        }
                    }
                    for (i, kind) in self.spec.key_kinds.iter().enumerate() {
                        if !matches!(kind, PdKeyKind::Bytes) || gnulls & (1 << i) != 0 {
                            continue;
                        }
                        let (off, len) = unpack_span(keys[i]);
                        img.extend_from_slice(&(len as u64).to_ne_bytes());
                        img.extend_from_slice(&self.key_arena[off..off + len]);
                        while img.len() % 8 != 0 {
                            img.push(0);
                        }
                    }
                    (32 + img.len()) as u64
                } else {
                    0
                };
                for j in 0..nsets {
                    let jw = (j as u64).to_ne_bytes();
                    for &v in self.dsets[g * nsets + j].ints() {
                        if bytes_mode {
                            buf.extend_from_slice(&rec_len.to_ne_bytes());
                            buf.extend_from_slice(&nulls);
                            buf.extend_from_slice(&jw);
                            buf.extend_from_slice(&v.to_ne_bytes());
                            buf.extend_from_slice(&img);
                        } else {
                            buf.extend_from_slice(&nulls);
                            for &w in keys {
                                buf.extend_from_slice(&w.to_ne_bytes());
                            }
                            buf.extend_from_slice(&jw);
                            buf.extend_from_slice(&v.to_ne_bytes());
                        }
                    }
                }
            }
            if !buf.is_empty() {
                emit(p as u32, &buf)?;
            }
        }
        Ok(())
    }

    /// Post-commit epoch reset: drop every set's VALUES, `seen_null`
    /// retained (the distinctset seen_null law — NULLs never spill).
    /// DEVIATION from the §4 sketch's "capacities retained": `mem()` is
    /// capacity-based, so retained capacities would re-arm the crossing
    /// only on capacity DOUBLING (a ~2× budget high-water); shrinking the
    /// sets keeps the R3 bound at ~budget + one insert with the plain
    /// budget check re-armed naturally. The small eviction-floor ratchet
    /// guards the group-table-dominated tail (base_mem alone near the
    /// budget), where the caller's worthwhileness gate then refuses.
    fn spill_reset_values(&mut self) {
        let nsets = self.spec.sets.len();
        for d in &mut self.dsets {
            let seen_null = d.seen_null;
            *d = DistinctSet::new();
            d.seen_null = seen_null;
        }
        if nsets > 0 {
            self.total_set_mem = 0;
            for g in 0..self.ngroups() {
                let m: usize =
                    self.dsets[g * nsets..(g + 1) * nsets].iter().map(|d| d.mem_bytes()).sum();
                self.set_mem[g] = m;
                self.total_set_mem += m;
            }
        }
        self.evict_floor = self.mem() + (self.budget / 16).max(4096);
    }
}

impl PdSinkLocal {
    /// See [`PdBuilder::spill_eligible`].
    pub fn pd_spill_eligible(&self) -> bool {
        self.builder.spill_eligible()
    }

    /// See [`PdBuilder::spill_value_bytes`].
    pub fn pd_spill_value_bytes(&self) -> usize {
        self.builder.spill_value_bytes()
    }

    /// See [`PdBuilder::spill_freeable_bytes`].
    pub fn pd_spill_freeable_bytes(&self) -> usize {
        self.builder.spill_freeable_bytes()
    }

    /// See [`PdBuilder::spill_emit`].
    pub fn pd_spill_emit(
        &self,
        emit: &mut dyn FnMut(u32, &[u8]) -> PgResult<()>,
    ) -> PgResult<()> {
        self.builder.spill_emit(emit)
    }

    /// See [`PdBuilder::spill_reset_values`].
    pub fn pd_spill_reset_values(&mut self) {
        self.builder.spill_reset_values()
    }
}

/// Rebuild ONE partition's spilled records into a merge-compatible
/// [`PdHandedTable`]: replay through a fresh (never-crossing, never-Mcx)
/// builder of the same spec — probe/create-group + set insert, the donor
/// kernel — then freeze. Cross-epoch duplicate values re-dedup here; vocab
/// states are zero (they never left the in-memory tables) so the bucket
/// merge adds nothing for them. Fail-closed on torn or corrupt records.
pub fn pd_table_from_spill(spec: &Arc<PdSpec>, bytes: &[u8]) -> PgResult<PdHandedTable> {
    let nkeys = spec.nkeys();
    let nsets = spec.sets.len();
    if nkeys == 0 || nsets == 0 {
        return Err(pd_internal("distinct spill replay on a non-grouped spec"));
    }
    if pd_spill_bytes_mode(spec) {
        return pd_table_from_spill_bytes(spec, bytes);
    }
    let width = pd_spill_record_width(spec);
    if bytes.len() % width != 0 {
        return Err(pd_internal("torn distinct spill record (partial row)"));
    }
    let mut b = PdBuilder::new(Arc::clone(spec), usize::MAX, None);
    let mut words = vec![0i64; nkeys];
    let mut off = 0usize;
    let rd = |o: usize| u64::from_ne_bytes(bytes[o..o + 8].try_into().unwrap());
    while off < bytes.len() {
        let nulls = rd(off);
        off += 8;
        if nulls >= (1u64 << nkeys) {
            return Err(pd_internal("corrupt distinct spill record (keynulls)"));
        }
        for w in words.iter_mut() {
            *w = rd(off) as i64;
            off += 8;
        }
        let j = rd(off) as usize;
        off += 8;
        if j >= nsets {
            return Err(pd_internal("corrupt distinct spill record (set index)"));
        }
        let v = rd(off) as i64;
        off += 8;
        let nulls = nulls as u32;
        let h = key_hash(&words, nulls);
        let (found, slot) = b.probe(&words, nulls, h, &KeySrc::None);
        let g = match found {
            Some(g) => g,
            None => b.create_group(&words, nulls, h, slot, &KeySrc::None),
        } as usize;
        b.dsets[g * nsets + j].insert_i64(v);
    }
    b.freeze()
}

/// Bytes-mode replay twin of [`pd_table_from_spill`]: parses the
/// variable-width canonical-image records (length-prefixed key content
/// tails), rebuilds groups by CONTENT through the same donor kernel, and
/// freezes. Fail-closed on any torn or internally inconsistent record.
fn pd_table_from_spill_bytes(spec: &Arc<PdSpec>, bytes: &[u8]) -> PgResult<PdHandedTable> {
    let nkeys = spec.nkeys();
    let nsets = spec.sets.len();
    let min_width = pd_spill_min_record_width(spec);
    let mut b = PdBuilder::new(Arc::clone(spec), usize::MAX, None);
    let mut words = vec![0i64; nkeys];
    // Staged spans over the record's own tail bytes (KeySrc::Staged).
    let mut spans = vec![(0u32, 0u32); nkeys];
    let mut off = 0usize;
    while off < bytes.len() {
        if bytes.len() - off < 8 {
            return Err(pd_internal("torn distinct spill record (bytes header)"));
        }
        let rd = |o: usize| u64::from_ne_bytes(bytes[o..o + 8].try_into().unwrap());
        let rec_len = rd(off) as usize;
        if rec_len < min_width || rec_len % 8 != 0 || rec_len > bytes.len() - off {
            return Err(pd_internal("torn distinct spill record (bytes rec_len)"));
        }
        let rec = &bytes[off..off + rec_len];
        let nulls = u64::from_ne_bytes(rec[8..16].try_into().unwrap());
        if nkeys < 64 && nulls >= (1u64 << nkeys) {
            return Err(pd_internal("corrupt distinct spill record (keynulls)"));
        }
        let j = u64::from_ne_bytes(rec[16..24].try_into().unwrap()) as usize;
        if j >= nsets {
            return Err(pd_internal("corrupt distinct spill record (set index)"));
        }
        let v = i64::from_ne_bytes(rec[24..32].try_into().unwrap());
        for (i, w) in words.iter_mut().enumerate() {
            *w = i64::from_ne_bytes(rec[32 + i * 8..40 + i * 8].try_into().unwrap());
        }
        // Parse the length-prefixed content tails (component order).
        let nulls = nulls as u32;
        let mut t = 32 + nkeys * 8;
        for (i, kind) in spec.key_kinds.iter().enumerate() {
            spans[i] = (0, 0);
            if !matches!(kind, PdKeyKind::Bytes) || nulls & (1 << i) != 0 {
                continue;
            }
            if rec_len - t < 8 {
                return Err(pd_internal("torn distinct spill record (bytes tail)"));
            }
            let len = u64::from_ne_bytes(rec[t..t + 8].try_into().unwrap()) as usize;
            let padded = len.div_ceil(8) * 8;
            if rec_len - t - 8 < padded {
                return Err(pd_internal("torn distinct spill record (bytes tail length)"));
            }
            spans[i] = ((t + 8) as u32, len as u32);
            t += 8 + padded;
        }
        if t != rec_len {
            return Err(pd_internal("corrupt distinct spill record (bytes tail residue)"));
        }
        let src = KeySrc::Staged(rec, &spans);
        let h = PdBuilder::row_hash(spec, &words, nulls, &src);
        let (found, slot) = b.probe(&words, nulls, h, &src);
        let g = match found {
            Some(g) => g,
            None => b.create_group(&words, nulls, h, slot, &src),
        } as usize;
        b.dsets[g * nsets + j].insert_i64(v);
        off += rec_len;
    }
    b.freeze()
}

/// Route spilled distinct records into 256 value-hash SLICES by the byte of
/// `mix64(value)` `depth` levels from the top (depth 1 = bits 56..64,
/// depth 2 = bits 48..56, …, depth 6 = bits 16..24) — the M3.5 §4
/// COMBINE-SPLIT law (inc-3b). The mixer is distinctset.rs's own spill
/// mixer (splitmix64, the `spill_part` law); distinctset's serial spill
/// consumes bits UPWARD from bit 32 (`(mix64(v) >> 32) & (nparts-1)`),
/// while this routing consumes whole bytes TOP-DOWN — any deterministic
/// slicing of the same full-avalanche hash is legal (equal values hash
/// equal, so every distinct (group, set, value) lands in exactly one
/// slice), and top-down bytes make recursion levels strictly nested (a
/// depth-d slice is subdivided exactly by the next byte down). Fail-closed
/// on torn input and out-of-range depth.
pub fn pd_route_value_records(
    spec: &PdSpec,
    bytes: &[u8],
    depth: u32,
    out: &mut [Vec<u8>],
) -> PgResult<()> {
    debug_assert_eq!(out.len(), PD_GROUP_PARTS);
    if !(1..=6).contains(&depth) {
        return Err(pd_internal("distinct value-slice depth out of range"));
    }
    let shift = 64 - 8 * depth;
    if pd_spill_bytes_mode(spec) {
        // BYTES mode: variable-width records; the value sits at the fixed
        // offset 24 and the whole record routes intact. The caller streams
        // RECORD-ALIGNED chunks (fail-closed here on any torn tail).
        let min_width = pd_spill_min_record_width(spec);
        let mut off = 0usize;
        while off < bytes.len() {
            if bytes.len() - off < min_width {
                return Err(pd_internal("torn distinct spill record (bytes) in split"));
            }
            let rec_len =
                u64::from_ne_bytes(bytes[off..off + 8].try_into().unwrap()) as usize;
            if rec_len < min_width || rec_len % 8 != 0 || rec_len > bytes.len() - off {
                return Err(pd_internal("torn distinct spill record (bytes rec_len) in split"));
            }
            let v = u64::from_ne_bytes(bytes[off + 24..off + 32].try_into().unwrap());
            let s = ((mix64(v) >> shift) & 0xFF) as usize;
            out[s].extend_from_slice(&bytes[off..off + rec_len]);
            off += rec_len;
        }
        return Ok(());
    }
    let width = pd_spill_record_width(spec);
    if bytes.len() % width != 0 {
        return Err(pd_internal("torn distinct spill record (partial row) in split"));
    }
    let voff = width - 8;
    let mut off = 0usize;
    while off < bytes.len() {
        let v = u64::from_ne_bytes(bytes[off + voff..off + width].try_into().unwrap());
        let s = ((mix64(v) >> shift) & 0xFF) as usize;
        out[s].extend_from_slice(&bytes[off..off + width]);
        off += width;
    }
    Ok(())
}

/// Combine-side pre-count of one bucket's IN-MEMORY faces: (groups, set
/// values, key-content bytes). Together with the spill directory's
/// `part_len`, this is everything the conservative over-budget refusal
/// reads — nothing touches disk before the decision. Groups are an upper
/// bound on the merged bucket's output groups: spilled records never
/// introduce a group the in-memory tables lack (group creation happens at
/// accept; the epoch reset clears only set values). Key-content bytes
/// (bytes-key specs only) bound the merged bucket's arena the same way.
pub fn pd_bucket_precount(
    spec: &PdSpec,
    t: &PdHandedTable,
    bucket: usize,
) -> (usize, usize, usize) {
    let Some(parts) = t.parts.as_ref() else { return (0, 0, 0) };
    let nsets = spec.sets.len();
    let nkeys = spec.nkeys();
    let (s, e) = (parts.starts[bucket] as usize, parts.starts[bucket + 1] as usize);
    let mut vals = 0usize;
    let mut key_bytes = 0usize;
    for &g in &parts.idx[s..e] {
        let g = g as usize;
        for j in 0..nsets {
            let si = g * nsets + j;
            vals += t.set_int_len(si);
        }
        if spec.has_bytes_keys() {
            for (i, kind) in spec.key_kinds.iter().enumerate() {
                if matches!(kind, PdKeyKind::Bytes) && t.keynulls[g] & (1 << i) == 0 {
                    key_bytes += unpack_span(t.keys[g * nkeys + i]).1;
                }
            }
        }
    }
    (e - s, vals, key_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression (m2-distinct-sink): the vocab table must key on AGGREGATE
    /// oids — the derivation passes `Aggref.aggfnoid`. The original table
    /// listed transfn proc oids and no vocab shape could ever derive.
    #[test]
    fn vocab_kind_keys_on_aggregate_oids() {
        assert!(matches!(vocab_kind(2803, None), Some(PdVocabKind::CountStar)));
        assert!(matches!(
            vocab_kind(2147, Some((3, PdInt::I64))),
            Some(PdVocabKind::CountAny { att: 3 })
        ));
        assert!(matches!(
            vocab_kind(2109, Some((1, PdInt::I16))),
            Some(PdVocabKind::SumInt { att: 1, kind: PdInt::I16 })
        ));
        assert!(matches!(
            vocab_kind(2108, Some((2, PdInt::I32))),
            Some(PdVocabKind::SumInt { att: 2, kind: PdInt::I32 })
        ));
        assert!(matches!(
            vocab_kind(2102, Some((4, PdInt::I16))),
            Some(PdVocabKind::AvgInt { att: 4, kind: PdInt::I16 })
        ));
        assert!(matches!(
            vocab_kind(2101, Some((5, PdInt::I32))),
            Some(PdVocabKind::AvgInt { att: 5, kind: PdInt::I32 })
        ));
        // Width mismatches and the Int128/numeric families refuse.
        assert!(vocab_kind(2108, Some((2, PdInt::I16))).is_none());
        assert!(vocab_kind(2107, Some((2, PdInt::I64))).is_none()); // sum(int8)
        assert!(vocab_kind(2100, Some((2, PdInt::I64))).is_none()); // avg(int8)
        // The OLD (buggy) transfn oids must NOT match.
        assert!(vocab_kind(1219, None).is_none());
        assert!(vocab_kind(2804, Some((0, PdInt::I64))).is_none());
    }

    // --- M3.5 inc-3a spill surface (fleet-run: the known local nodeagg
    // test-binary link limitation) ---------------------------------------

    fn spill_test_spec() -> Arc<PdSpec> {
        Arc::new(PdSpec {
            key_atts: vec![0, 1],
            key_kinds: vec![PdKeyKind::Int(PdInt::I64), PdKeyKind::Int(PdInt::I32)],
            vocab: vec![PdVocab { transno: 0, kind: PdVocabKind::CountStar }],
            sets: vec![
                PdSetSpec { att: 2, kind: DistinctKeyKind::Int64 },
                PdSetSpec { att: 3, kind: DistinctKeyKind::Int32 },
            ],
            max_att: 4,
            worker_budget: usize::MAX,
            expected_worker_rows: 0,
        })
    }

    /// Feed one (group, set, value) into a builder directly (the accept
    /// kernel minus the slot plumbing): probe/create + vocab bump + insert.
    fn feed(b: &mut PdBuilder<'static>, keys: &[i64], nulls: u32, j: usize, v: Option<i64>) {
        let nsets = b.spec.sets.len();
        let h = key_hash(keys, nulls);
        let (found, slot) = b.probe(keys, nulls, h, &KeySrc::None);
        let g = match found {
            Some(g) => g,
            None => b.create_group(keys, nulls, h, slot, &KeySrc::None),
        } as usize;
        b.states[g * 2 * b.spec.vocab.len()] += 1; // CountStar
        match v {
            Some(v) => b.dsets[g * nsets + j].insert_i64(v),
            None => b.dsets[g * nsets + j].seen_null = true,
        }
    }

    /// One deterministic worker's content (re-runnable: the reference and
    /// the spill arms must build identical inputs).
    fn build_worker(spec: &Arc<PdSpec>, salt: i64) -> PdBuilder<'static> {
        let mut b = PdBuilder::new(Arc::clone(spec), usize::MAX, None);
        for i in 0..4000i64 {
            let k = [(i * 13 + salt) % 37, ((i * 7 + salt) % 11) as i64];
            let nulls = if (i + salt) % 29 == 0 { 1 } else { 0 };
            feed(&mut b, &k, nulls, 0, Some((i * 104729 + salt) % 2500));
            feed(&mut b, &k, nulls, 1, Some(i % 97 - 40));
            if (i + salt) % 41 == 0 {
                feed(&mut b, &k, nulls, 1, None); // set NULL: seen_null face
            }
        }
        b
    }

    /// Canonical view of a merged bucket set: (keys, nulls, states,
    /// per-set sorted values + seen_null).
    fn canon(spec: &PdSpec, m: &PdMerged<'_>) -> Vec<(Vec<i64>, u32, Vec<i64>, Vec<(Vec<i64>, bool)>)> {
        let nkeys = spec.nkeys();
        let nsets = spec.sets.len();
        let nvocab = spec.vocab.len();
        let mut rows: Vec<_> = (0..m.ngroups)
            .map(|g| {
                let sets = (0..nsets)
                    .map(|j| {
                        let d = m.dsets[g * nsets + j].as_ref().unwrap();
                        let mut vals = d.ints().to_vec();
                        vals.sort_unstable();
                        (vals, d.seen_null)
                    })
                    .collect();
                (
                    m.keys[g * nkeys..(g + 1) * nkeys].to_vec(),
                    m.keynulls[g],
                    m.states[g * 2 * nvocab..(g + 1) * 2 * nvocab].to_vec(),
                    sets,
                )
            })
            .collect();
        rows.sort();
        rows
    }

    /// batch-insert lane: the staged (deferred, look-ahead-prefetched)
    /// insert schedule is BYTE-IDENTICAL to the per-row path — same frozen
    /// keys/values arrays in the same order (value-append order per set is
    /// row order in both schedules), same seen_null faces, same states —
    /// including partial trailing windows and mid-window duplicates, and a
    /// crossing (budget) parity leg on the window-grain contract.
    #[test]
    fn staged_batch_insert_freeze_identity() {
        let spec = Arc::new(PdSpec {
            key_atts: vec![0],
            key_kinds: vec![PdKeyKind::Int(PdInt::I64)],
            vocab: vec![],
            sets: vec![PdSetSpec { att: 1, kind: DistinctKeyKind::Int64 }],
            max_att: 2,
            worker_budget: usize::MAX,
            expected_worker_rows: 0,
        });
        let rows = 3 * PD_STAGE_BATCH + 137; // partial trailing window
        let drive = |staged: bool| -> PdHandedTable {
            let mut b = PdBuilder::new(Arc::clone(&spec), usize::MAX, None);
            b.set_batch_insert(staged);
            assert_eq!(b.batch_insert_armed(), staged, "admission on this spec");
            for i in 0..rows as i64 {
                let k = [i % 257]; // ~257 groups
                let h = key_hash(&k, 0);
                let (found, slot) = b.probe(&k, 0, h, &KeySrc::None);
                let g = match found {
                    Some(g) => g,
                    None => b.create_group(&k, 0, h, slot, &KeySrc::None),
                } as usize;
                // near-unique values + planted duplicates + NULL face
                let v = if i % 97 == 0 { Some(42) } else { Some(i * 104729 + 1) };
                let v = if i % 61 == 0 { None } else { v };
                if staged {
                    assert!(matches!(b.stage_push(g, v).unwrap(), PdFeed::Ok));
                } else {
                    match v {
                        Some(v) => b.dsets[g].insert_i64(v),
                        None => b.dsets[g].seen_null = true,
                    }
                }
            }
            b.freeze().unwrap()
        };
        let a = drive(false);
        let s = drive(true);
        assert_eq!(a.ngroups, s.ngroups);
        assert_eq!(a.keys, s.keys);
        assert_eq!(a.keynulls, s.keynulls);
        assert_eq!(a.states, s.states);
        assert_eq!(a.set_ints, s.set_ints, "value arrays byte-identical incl. order");
        assert_eq!(a.set_int_offs, s.set_int_offs);
        assert_eq!(a.set_null, s.set_null);

        // Shape refusals: bytes set / multi-set specs must not arm.
        let mut nb = PdBuilder::new(
            Arc::new(PdSpec {
                key_atts: vec![0],
                key_kinds: vec![PdKeyKind::Int(PdInt::I64)],
                vocab: vec![],
                sets: vec![PdSetSpec { att: 1, kind: DistinctKeyKind::Bytes }],
                max_att: 2,
                worker_budget: usize::MAX,
                expected_worker_rows: 0,
            }),
            usize::MAX,
            None,
        );
        nb.set_batch_insert(true);
        assert!(!nb.batch_insert_armed(), "bytes sets refuse");
        let mut nm = PdBuilder::new(spill_test_spec(), usize::MAX, None);
        nm.set_batch_insert(true);
        assert!(!nm.batch_insert_armed(), "multi-set specs refuse");

        // Window-grain crossing: a tiny budget reports Crossed only at a
        // flush boundary, with the staging drained (worker arm, mcx None).
        let mut c = PdBuilder::new(Arc::clone(&spec), 4096, None);
        c.set_batch_insert(true);
        let mut crossed = false;
        'outer: for i in 0..(2 * PD_STAGE_BATCH) as i64 {
            let k = [i % 7];
            let h = key_hash(&k, 0);
            let (found, slot) = c.probe(&k, 0, h, &KeySrc::None);
            let g = match found {
                Some(g) => g,
                None => c.create_group(&k, 0, h, slot, &KeySrc::None),
            } as usize;
            if matches!(c.stage_push(g, Some(i * 31 + 1)).unwrap(), PdFeed::Crossed) {
                assert!(c.stage_v.is_empty(), "Crossed only post-flush");
                assert_eq!((i + 1) as usize % PD_STAGE_BATCH, 0, "window-grain check");
                crossed = true;
                break 'outer;
            }
        }
        assert!(crossed, "tiny budget must cross");
    }

    /// dedupsub I3: with the projection reserve LIVE (expected_worker_rows
    /// set, a dominant group crossing PD_PROJECT_MIN inside the staged
    /// GL-VECACCEPT-1 shape gate: the lane plan derives exactly the
    /// staged-set + single-int-key vocabulary and refuses everything else
    /// fail-closed.
    #[test]
    fn vec_plan_admission() {
        let good = Arc::new(PdSpec {
            key_atts: vec![3],
            key_kinds: vec![PdKeyKind::Int(PdInt::I32)],
            vocab: vec![
                PdVocab { transno: 0, kind: PdVocabKind::CountStar },
                PdVocab { transno: 1, kind: PdVocabKind::SumInt { att: 2, kind: PdInt::I16 } },
                PdVocab { transno: 2, kind: PdVocabKind::CountAny { att: 1 } },
            ],
            sets: vec![PdSetSpec { att: 1, kind: DistinctKeyKind::Int32 }],
            max_att: 4,
            worker_budget: usize::MAX,
            expected_worker_rows: 0,
        });
        let p = pd_vec_plan(&good).expect("the grouped count-distinct-int shape class derives");
        assert_eq!((p.key_att, p.key_kind), (3, PdInt::I32));
        assert_eq!((p.set_att, p.set_kind), (1, PdInt::I32));
        assert_eq!(p.riders.len(), 3);
        assert!(p.riders[0].is_none() && p.riders[2].is_none());
        assert_eq!(p.riders[1], Some((2, PdInt::I16)));
        // Refusals: bytes set, bytes key, multi-key, multi-set.
        let mut s = PdSpec { sets: vec![PdSetSpec { att: 1, kind: DistinctKeyKind::Bytes }], ..clone_spec(&good) };
        assert!(pd_vec_plan(&s).is_none(), "bytes set refuses");
        s = PdSpec { key_kinds: vec![PdKeyKind::Bytes], ..clone_spec(&good) };
        assert!(pd_vec_plan(&s).is_none(), "bytes key refuses");
        s = clone_spec(&good);
        s.key_atts.push(0);
        s.key_kinds.push(PdKeyKind::Int(PdInt::I64));
        assert!(pd_vec_plan(&s).is_none(), "multi-key refuses");
        s = clone_spec(&good);
        s.sets.push(PdSetSpec { att: 2, kind: DistinctKeyKind::Int64 });
        assert!(pd_vec_plan(&s).is_none(), "multi-set refuses");
    }

    fn clone_spec(s: &PdSpec) -> PdSpec {
        PdSpec {
            key_atts: s.key_atts.clone(),
            key_kinds: s.key_kinds.clone(),
            vocab: s.vocab.clone(),
            sets: s.sets.iter().map(|x| PdSetSpec { att: x.att, kind: x.kind }).collect(),
            max_att: s.max_att,
            worker_budget: s.worker_budget,
            expected_worker_rows: s.expected_worker_rows,
        }
    }

    /// GL-VECACCEPT-1 identity law: the vectorized whole-granule schedule
    /// (batch hash → prefetched probe/resolve with the batch-local run
    /// memo → columnar rider folds → bulk staged set feed) freezes
    /// BYTE-IDENTICALLY to the per-row accept schedule over the same row
    /// stream — group creation order, key/hash bytes, rider states, set
    /// value-append order — including key runs (memo face), planted
    /// duplicate (g, v) pairs (dup-skip face), > INIT_TABLE groups (grow
    /// face), partial trailing granules, and the Crossed/resume contract
    /// (a crossing consumes exactly through the flush row; the resume
    /// completes the granule with nothing lost or doubled).
    #[test]
    fn vec_accept_freeze_identity() {
        let spec = Arc::new(PdSpec {
            key_atts: vec![0],
            key_kinds: vec![PdKeyKind::Int(PdInt::I32)],
            vocab: vec![
                PdVocab { transno: 0, kind: PdVocabKind::CountStar },
                PdVocab { transno: 1, kind: PdVocabKind::SumInt { att: 2, kind: PdInt::I32 } },
                PdVocab { transno: 2, kind: PdVocabKind::CountAny { att: 1 } },
            ],
            sets: vec![PdSetSpec { att: 1, kind: DistinctKeyKind::Int64 }],
            max_att: 3,
            worker_budget: usize::MAX,
            expected_worker_rows: 0,
        });
        let n: i64 = 3 * 8192 + 517; // partial trailing granule
        let key = |i: i64| -> i64 { if i % 5 < 2 { (i / 7) % 300 } else { i % 300 } };
        let val = |i: i64| -> i64 { if i % 11 == 0 { 42 } else { (i * 104_729) % 5000 } };
        let rid = |i: i64| -> i64 { i % 1000 - 500 };

        // Per-row oracle: the accept kernel minus the slot plumbing
        // (resolve_group_int + the accept vocab arms + stage_push — the
        // spill-surface tests' `feed` precedent).
        let per_row = |budget: usize| -> PdHandedTable {
            let mut a = PdBuilder::new(Arc::clone(&spec), budget, None);
            a.set_batch_insert(true);
            assert!(a.vec_admissible());
            let nvocab = a.spec.vocab.len();
            for i in 0..n {
                let g = a.resolve_group_int(&[key(i)], 0) as usize;
                let st = &mut a.states[g * 2 * nvocab..];
                st[0] += 1; // CountStar
                st[2] += rid(i); // SumInt acc
                st[3] += 1; // SumInt count
                st[4] += 1; // CountAny (no NULLs in part lanes)
                // Crossed verdicts are ignored: no spill in this harness,
                // so the state trajectory is verdict-independent.
                let _ = a.stage_push(g, Some(val(i))).unwrap();
            }
            a.freeze().unwrap()
        };
        // Vec schedule: granule-sized lanes + the resume-on-Crossed loop.
        let vec_drive = |budget: usize| -> PdHandedTable {
            let mut b = PdBuilder::new(Arc::clone(&spec), budget, None);
            b.set_batch_insert(true);
            assert!(b.vec_admissible());
            let mut vs = PdVecScratch::default();
            let mut i: i64 = 0;
            while i < n {
                let g_n = (n - i).min(8192);
                let keys: Vec<i64> = (i..i + g_n).map(key).collect();
                let vals: Vec<i64> = (i..i + g_n).map(val).collect();
                let rids: Vec<i64> = (i..i + g_n).map(rid).collect();
                let riders: Vec<Option<&[i64]>> = vec![None, Some(rids.as_slice()), None];
                b.vec_resolve_fold(&keys, &riders, &mut vs);
                assert_eq!(vs.gids.len(), keys.len());
                let mut at = 0usize;
                loop {
                    let (feed, consumed) = b.vec_stage_sets(&vs.gids, &vals, at).unwrap();
                    assert!(consumed > at || consumed == keys.len());
                    at = consumed;
                    if matches!(feed, PdFeed::Ok) {
                        assert_eq!(at, keys.len());
                        break;
                    }
                    // Crossed: the harness "absorbs the epoch" (no reset —
                    // matching the per-row oracle's ignored verdicts) and
                    // resumes from the consumed index.
                }
                i += g_n;
            }
            b.freeze().unwrap()
        };
        for budget in [usize::MAX, 16 * 1024] {
            let a = per_row(budget);
            let s = vec_drive(budget);
            assert_eq!(a.ngroups, s.ngroups);
            assert_eq!(a.keys, s.keys);
            assert_eq!(a.keynulls, s.keynulls);
            assert_eq!(a.states, s.states, "rider states identical");
            assert_eq!(a.set_ints, s.set_ints, "value arrays byte-identical incl. order");
            assert_eq!(a.set_int_offs, s.set_int_offs);
            assert_eq!(a.set_null, s.set_null);
        }
    }

    /// windows), the frozen table is byte-identical to the per-row oracle
    /// (projection never fires there — the geometry-is-non-surface pin).
    /// Also pins the epoch-stamp accounting totals against the builder's
    /// exact recomputed set memory.
    #[test]
    fn staged_projection_identity() {
        let mk_spec = |expected: u64| {
            Arc::new(PdSpec {
                key_atts: vec![0],
                key_kinds: vec![PdKeyKind::Int(PdInt::I64)],
                vocab: vec![],
                sets: vec![PdSetSpec { att: 1, kind: DistinctKeyKind::Int64 }],
                max_att: 2,
                worker_budget: usize::MAX,
                expected_worker_rows: expected,
            })
        };
        let rows = (12 * PD_STAGE_BATCH + 331) as i64;
        let drive = |spec: &Arc<PdSpec>, staged: bool| -> PdHandedTable {
            let mut b = PdBuilder::new(Arc::clone(spec), usize::MAX, None);
            b.set_batch_insert(staged);
            for i in 0..rows {
                // ~92% of rows to group 0 (its set crosses PD_PROJECT_MIN);
                // 12 tail groups stay small (no-reserve face).
                let k = [if i % 13 == 0 { 1 + (i % 12) } else { 0 }];
                let h = key_hash(&k, 0);
                let (found, slot) = b.probe(&k, 0, h, &KeySrc::None);
                let g = match found {
                    Some(g) => g,
                    None => b.create_group(&k, 0, h, slot, &KeySrc::None),
                } as usize;
                let v =
                    if i % 89 == 0 { None } else { Some(i.wrapping_mul(6364136223846793005) + 3) };
                if staged {
                    assert!(matches!(b.stage_push(g, v).unwrap(), PdFeed::Ok));
                } else {
                    match v {
                        Some(v) => b.dsets[g].insert_i64(v),
                        None => b.dsets[g].seen_null = true,
                    }
                }
            }
            if staged {
                // Accounting pin BEFORE freeze: the delta-accounted total
                // must equal the exact recomputed sum (epoch-stamp dedup and
                // projection reserves both metered).
                b.flush_staged();
                let exact: usize = b.dsets.iter().map(|d| d.mem_bytes()).sum();
                assert_eq!(b.total_set_mem, exact, "delta accounting drifted");
                // Group ids are creation-ordered (i=0 hits the i%13 arm, so
                // the dominant key 0 is NOT group 0) — locate the dominant
                // set by size, not index.
                let dominant = b.dsets.iter().map(|d| d.len()).max().unwrap_or(0);
                assert!(
                    dominant >= PD_PROJECT_MIN,
                    "test shape: dominant set must cross the projection gate (max len {dominant})"
                );
            }
            b.freeze().unwrap()
        };
        let spec = mk_spec(2 * rows as u64);
        let oracle = drive(&spec, false);
        let proj = drive(&spec, true);
        assert_eq!(oracle.ngroups, proj.ngroups);
        assert_eq!(oracle.keys, proj.keys);
        assert_eq!(oracle.states, proj.states);
        assert_eq!(oracle.set_ints, proj.set_ints, "value arrays identical incl. order");
        assert_eq!(oracle.set_int_offs, proj.set_int_offs);
        assert_eq!(oracle.set_null, proj.set_null);
        // Unknown expectation (0): projection inert, same identity.
        let spec0 = mk_spec(0);
        let o0 = drive(&spec0, false);
        let p0 = drive(&spec0, true);
        assert_eq!(o0.set_ints, p0.set_ints);
    }

    /// q9internals inc-2: run-memo + consecutive-dup-skip identity. A
    /// clustered corpus (group-key runs with repeated (k,v) pairs — the
    /// sorted-bank shape) plus adversarial faces (interleaved groups, NULLs
    /// inside runs, same value re-appearing across groups/runs, memo-width
    /// boundary) must freeze BYTE-IDENTICAL tables with the memo on vs off,
    /// staged and unstaged. Also pins the accounting equality under skip.
    #[test]
    fn run_memo_identity() {
        let spec = Arc::new(PdSpec {
            key_atts: vec![0],
            key_kinds: vec![PdKeyKind::Int(PdInt::I64)],
            vocab: vec![],
            sets: vec![PdSetSpec { att: 1, kind: DistinctKeyKind::Int64 }],
            max_att: 2,
            worker_budget: usize::MAX,
            expected_worker_rows: 0,
        });
        // Corpus: runs of (k, v) with in-run duplicates (dup-skip face),
        // occasional NULLs inside runs (must not break the skip), group
        // interleaving every 7th run (memo miss face), and values that
        // recur in LATER runs of the same group (non-consecutive dup: must
        // NOT be skipped by the memo, dedup'd by the set as before).
        let mut corpus: Vec<(i64, Option<i64>)> = Vec::new();
        for run in 0..4000i64 {
            let k = if run % 7 == 0 { run % 3 } else { run % 29 };
            let base = (run % 11) * 100; // recurs across runs of the same k
            for rep in 0..(1 + run % 6) {
                corpus.push((k, Some(base)));
                if rep == 2 {
                    corpus.push((k, None)); // NULL inside the run
                    corpus.push((k, Some(base))); // dup straddling the NULL
                }
            }
            corpus.push((k, Some(base + run))); // distinct tail per run
        }
        assert!(corpus.len() > 2 * PD_STAGE_BATCH, "corpus spans windows");
        let drive = |memo: bool, staged: bool| -> PdHandedTable {
            let mut b = PdBuilder::new(Arc::clone(&spec), usize::MAX, None);
            b.set_batch_insert(staged);
            b.set_run_memo(memo);
            for &(k, v) in &corpus {
                let g = b.resolve_group_int(&[k], 0) as usize;
                if staged {
                    assert!(matches!(b.stage_push(g, v).unwrap(), PdFeed::Ok));
                } else {
                    match v {
                        Some(v) => b.dsets[g].insert_i64(v),
                        None => b.dsets[g].seen_null = true,
                    }
                }
            }
            if staged {
                b.flush_staged();
                let exact: usize = b.dsets.iter().map(|d| d.mem_bytes()).sum();
                assert_eq!(b.total_set_mem, exact, "delta accounting drifted under skip");
            }
            b.freeze().unwrap()
        };
        let oracle = drive(false, false);
        for (memo, staged) in [(true, false), (false, true), (true, true)] {
            let t = drive(memo, staged);
            assert_eq!(oracle.ngroups, t.ngroups, "memo={memo} staged={staged}");
            assert_eq!(oracle.keys, t.keys, "memo={memo} staged={staged}");
            assert_eq!(
                oracle.set_ints, t.set_ints,
                "value arrays identical incl. order (memo={memo} staged={staged})"
            );
            assert_eq!(oracle.set_int_offs, t.set_int_offs, "memo={memo} staged={staged}");
            assert_eq!(oracle.set_null, t.set_null, "memo={memo} staged={staged}");
        }
    }

    /// q9internals inc-3: the donor-hint anticipatory combine reserve is
    /// pure geometry — merged output byte-identical with the hint armed vs
    /// not, on overlapping AND disjoint donor value sets, and the armed
    /// merge's big dst set reaches final capacity by the end of donor 1
    /// (no per-donor re-grow).
    #[test]
    fn donor_hint_merge_identity() {
        let spec = Arc::new(PdSpec {
            key_atts: vec![0],
            key_kinds: vec![PdKeyKind::Int(PdInt::I64)],
            vocab: vec![],
            sets: vec![PdSetSpec { att: 1, kind: DistinctKeyKind::Int64 }],
            max_att: 2,
            worker_budget: usize::MAX,
            expected_worker_rows: 0,
        });
        // 4 donors, one dominant group: donor d holds values in a mostly
        // disjoint range (the UserID-clustered shape) plus a shared
        // overlap slice (dedup face).
        let tables: Vec<PdHandedTable> = (0..4i64)
            .map(|d| {
                let mut b = PdBuilder::new(Arc::clone(&spec), usize::MAX, None);
                for i in 0..(3 * PD_PROJECT_MIN as i64) {
                    let k = [i % 5]; // 5 groups
                    let h = key_hash(&k, 0);
                    let (found, slot) = b.probe(&k, 0, h, &KeySrc::None);
                    let g = match found {
                        Some(g) => g,
                        None => b.create_group(&k, 0, h, slot, &KeySrc::None),
                    } as usize;
                    let v = if i % 97 == 0 { i } else { d * 1_000_000_000 + i };
                    b.dsets[g].insert_i64(v);
                }
                b.freeze().unwrap()
            })
            .collect();
        let refs: Vec<&PdHandedTable> = tables.iter().collect();
        let nbuckets = PD_GROUP_PARTS;
        for b in 0..nbuckets {
            let hinted = merge_bucket_refs(&spec, &refs, b);
            // Hint-less control: drive the merger without set_donor_hint.
            let mut m = PdBucketMerger::new(&spec);
            for &t in &refs {
                m.absorb(t, b);
            }
            let plain = m.finish();
            assert_eq!(hinted.ngroups, plain.ngroups, "bucket {b}");
            assert_eq!(hinted.keys, plain.keys, "bucket {b}");
            for (i, (h, p)) in hinted.dsets.iter().zip(plain.dsets.iter()).enumerate() {
                match (h, p) {
                    (Some(h), Some(p)) => {
                        assert_eq!(h.ints(), p.ints(), "bucket {b} set {i} values+order");
                        assert_eq!(h.seen_null, p.seen_null, "bucket {b} set {i}");
                    }
                    (None, None) => {}
                    _ => panic!("bucket {b} set {i}: arm mismatch"),
                }
            }
        }
    }

    /// M3.5 §4 round-trip: drain values → records → rebuild → merge with the
    /// in-memory remainders EQUALS the direct (never-spilled) merge, on every
    /// bucket — groups, keynulls, vocab states, set values, and the
    /// seen_null face (which never touches the records).
    #[test]
    fn spill_roundtrip_merge_equivalence() {
        let spec = spill_test_spec();

        // Reference: direct merge of two never-spilled workers.
        let t1 = build_worker(&spec, 0).freeze().unwrap();
        let t2 = build_worker(&spec, 5).freeze().unwrap();
        let tables = [t1, t2];
        let direct = pd_concat_buckets(
            &spec,
            (0..PD_GROUP_PARTS).map(|b| pd_merge_bucket(&spec, &tables, b)).collect(),
        );

        // Spill arm: same two workers, drained mid-build (values → records),
        // then a second accept wave (cross-epoch duplicates included by
        // construction), remainder frozen, records replayed per bucket.
        let mut spilled: Vec<std::collections::HashMap<u32, Vec<u8>>> = Vec::new();
        let mut remainders: Vec<PdHandedTable> = Vec::new();
        for salt in [0i64, 5] {
            let mut b = build_worker(&spec, salt);
            assert!(b.spill_eligible());
            assert!(b.spill_value_bytes() > 0);
            let mut parts: std::collections::HashMap<u32, Vec<u8>> = Default::default();
            let mut last_p: i64 = -1;
            b.spill_emit(&mut |p, bytes| {
                assert!((p as i64) > last_p, "partitions ascend");
                last_p = p as i64;
                assert_eq!(bytes.len() % pd_spill_record_width(&spec), 0);
                parts.entry(p).or_default().extend_from_slice(bytes);
                Ok(())
            })
            .unwrap();
            b.spill_reset_values();
            // Second epoch: refeed a slice of the same content (duplicates
            // across epochs) plus fresh values; stays IN MEMORY (remainder).
            for i in 0..4000i64 {
                let k = [(i * 13 + salt) % 37, ((i * 7 + salt) % 11) as i64];
                let nulls = if (i + salt) % 29 == 0 { 1 } else { 0 };
                // Undo the double CountStar bump: subtract before refeeding.
                let h = key_hash(&k, nulls);
                let (found, _) = b.probe(&k, nulls, h, &KeySrc::None);
                let g = found.expect("group exists from epoch 1") as usize;
                b.states[g * 2 * spec.vocab.len()] -= 1;
                feed(&mut b, &k, nulls, 0, Some((i * 104729 + salt) % 2500));
            }
            spilled.push(parts);
            remainders.push(b.freeze().unwrap());
        }
        let mut buckets = Vec::with_capacity(PD_GROUP_PARTS);
        for bkt in 0..PD_GROUP_PARTS {
            let mut synth: Vec<PdHandedTable> = Vec::new();
            for parts in &spilled {
                if let Some(bytes) = parts.get(&(bkt as u32)) {
                    synth.push(pd_table_from_spill(&spec, bytes).unwrap());
                }
            }
            let refs: Vec<&PdHandedTable> =
                remainders.iter().chain(synth.iter()).collect();
            buckets.push(pd_merge_bucket_refs(&spec, &refs, bkt));
        }
        let merged = pd_concat_buckets(&spec, buckets);

        assert_eq!(canon(&spec, &direct), canon(&spec, &merged));

        // Pre-count sanity: in-memory groups bound the merged groups; the
        // record width divides every partition's bytes (checked above).
        let mut groups = 0usize;
        for t in &tables {
            for bkt in 0..PD_GROUP_PARTS {
                groups += pd_bucket_precount(&spec, t, bkt).0;
            }
        }
        assert!(groups >= direct.ngroups);
    }

    /// GL-LOWDIST-1: LIVE-form tables (`freeze_live`) merged through the
    /// steal arm equal the flat-form donor merge exactly — groups, key
    /// nulls, vocab states, sorted set values, and the seen_null face —
    /// under donor REORDERING (the combine's largest-first shuffle) and
    /// with MIXED live/flat donors (a spilled Local seals flat beside live
    /// peers on the error path).
    #[test]
    fn live_form_steal_merge_invariance() {
        let spec = spill_test_spec();
        let flat = [
            build_worker(&spec, 0).freeze().unwrap(),
            build_worker(&spec, 5).freeze().unwrap(),
        ];
        let direct = pd_concat_buckets(
            &spec,
            (0..PD_GROUP_PARTS).map(|b| pd_merge_bucket(&spec, &flat, b)).collect(),
        );

        // Live pair, donors reordered.
        let l1 = build_worker(&spec, 0).freeze_live().unwrap();
        let l2 = build_worker(&spec, 5).freeze_live().unwrap();
        let refs: Vec<&PdHandedTable> = vec![&l2, &l1];
        let live_merged = pd_concat_buckets(
            &spec,
            (0..PD_GROUP_PARTS).map(|b| pd_merge_bucket_refs(&spec, &refs, b)).collect(),
        );
        assert_eq!(canon(&spec, &direct), canon(&spec, &live_merged));

        // Mixed forms.
        let l3 = build_worker(&spec, 0).freeze_live().unwrap();
        let f5 = build_worker(&spec, 5).freeze().unwrap();
        let refs: Vec<&PdHandedTable> = vec![&l3, &f5];
        let mixed_merged = pd_concat_buckets(
            &spec,
            (0..PD_GROUP_PARTS).map(|b| pd_merge_bucket_refs(&spec, &refs, b)).collect(),
        );
        assert_eq!(canon(&spec, &direct), canon(&spec, &mixed_merged));

        // Precount reads both forms identically.
        let f0 = build_worker(&spec, 0).freeze().unwrap();
        let l0 = build_worker(&spec, 0).freeze_live().unwrap();
        for b in 0..PD_GROUP_PARTS {
            assert_eq!(
                pd_bucket_precount(&spec, &f0, b),
                pd_bucket_precount(&spec, &l0, b)
            );
        }
    }

    /// Torn / corrupt records fail closed (never a silent wrong answer).
    #[test]
    fn spill_torn_record_fails_closed() {
        let spec = spill_test_spec();
        let width = pd_spill_record_width(&spec);
        assert_eq!(width, (2 + 3) * 8);
        // Torn: not a whole number of records.
        assert!(pd_table_from_spill(&spec, &vec![0u8; width + 1]).is_err());
        // Corrupt set index.
        let mut rec = vec![0u8; width];
        rec[(1 + spec.nkeys()) * 8..(2 + spec.nkeys()) * 8]
            .copy_from_slice(&(u64::MAX).to_ne_bytes());
        assert!(pd_table_from_spill(&spec, &rec).is_err());
        // Corrupt keynulls (bit beyond nkeys).
        let mut rec = vec![0u8; width];
        rec[..8].copy_from_slice(&(1u64 << 63).to_ne_bytes());
        assert!(pd_table_from_spill(&spec, &rec).is_err());
        // A well-formed empty image and a single record are fine.
        assert!(pd_table_from_spill(&spec, &[]).is_ok());
        assert!(pd_table_from_spill(&spec, &vec![0u8; width]).is_ok());
    }

    /// M3.5 inc-3b slice invariance: routing a bucket's spilled records by
    /// `mix64(value)` bytes and merging per-slice synth tables IN SEQUENCE
    /// (after the one-pass in-memory merge) equals the direct never-spilled
    /// merge on every bucket — groups, keynulls, vocab states, sorted set
    /// values, and the seen_null face — with every distinct (group, set,
    /// value) record in exactly one slice, at depth 1 and depth 2.
    #[test]
    fn split_slice_merge_invariance() {
        let spec = spill_test_spec();

        // Reference: direct merge of two never-spilled workers.
        let t1 = build_worker(&spec, 0).freeze().unwrap();
        let t2 = build_worker(&spec, 5).freeze().unwrap();
        let tables = [t1, t2];
        let direct = pd_concat_buckets(
            &spec,
            (0..PD_GROUP_PARTS).map(|b| pd_merge_bucket(&spec, &tables, b)).collect(),
        );

        // Spill arm: same two workers drained mid-build, then a second
        // accept wave (cross-epoch duplicates by construction) — the
        // inc-3a construction, reused.
        let mut spilled: Vec<std::collections::HashMap<u32, Vec<u8>>> = Vec::new();
        let mut remainders: Vec<PdHandedTable> = Vec::new();
        for salt in [0i64, 5] {
            let mut b = build_worker(&spec, salt);
            let mut parts: std::collections::HashMap<u32, Vec<u8>> = Default::default();
            b.spill_emit(&mut |p, bytes| {
                parts.entry(p).or_default().extend_from_slice(bytes);
                Ok(())
            })
            .unwrap();
            b.spill_reset_values();
            for i in 0..4000i64 {
                let k = [(i * 13 + salt) % 37, ((i * 7 + salt) % 11) as i64];
                let nulls = if (i + salt) % 29 == 0 { 1 } else { 0 };
                let h = key_hash(&k, nulls);
                let (found, _) = b.probe(&k, nulls, h, &KeySrc::None);
                let g = found.expect("group exists from epoch 1") as usize;
                b.states[g * 2 * spec.vocab.len()] -= 1;
                feed(&mut b, &k, nulls, 0, Some((i * 104729 + salt) % 2500));
            }
            spilled.push(parts);
            remainders.push(b.freeze().unwrap());
        }

        let width = pd_spill_record_width(&spec);
        for depth in [1u32, 2] {
            let mut buckets = Vec::with_capacity(PD_GROUP_PARTS);
            for bkt in 0..PD_GROUP_PARTS {
                // Every Local's bucket records concatenated (the runtime
                // streams all Locals' partitions through one router).
                let mut bytes = Vec::new();
                for parts in &spilled {
                    if let Some(bb) = parts.get(&(bkt as u32)) {
                        bytes.extend_from_slice(bb);
                    }
                }
                let mut slices: Vec<Vec<u8>> = vec![Vec::new(); PD_GROUP_PARTS];
                pd_route_value_records(&spec, &bytes, depth, &mut slices).unwrap();
                // Routing loses nothing and duplicates nothing…
                assert_eq!(slices.iter().map(|s| s.len()).sum::<usize>(), bytes.len());
                // …and every distinct record (group identity + set + value)
                // has exactly one home slice.
                let mut home: std::collections::HashMap<Vec<u8>, usize> = Default::default();
                for (si, s) in slices.iter().enumerate() {
                    for r in s.chunks(width) {
                        if let Some(prev) = home.insert(r.to_vec(), si) {
                            assert_eq!(prev, si, "record in two slices");
                        }
                    }
                }
                // The split combine: in-memory tables ONCE, then each
                // slice's synth table in sequence, dropped between absorbs.
                let mut merger = PdBucketMerger::new(&spec);
                for t in &remainders {
                    merger.absorb(t, bkt);
                }
                for s in &slices {
                    if s.is_empty() {
                        continue;
                    }
                    let synth = pd_table_from_spill(&spec, s).unwrap();
                    merger.absorb(&synth, bkt);
                }
                buckets.push(merger.finish());
            }
            let merged = pd_concat_buckets(&spec, buckets);
            assert_eq!(canon(&spec, &direct), canon(&spec, &merged), "depth {depth}");
        }
    }

    /// Torn / out-of-range input to the value router fails closed; a single
    /// record routes to exactly the mix64-top-byte slice.
    #[test]
    fn value_route_torn_fails_closed() {
        let spec = spill_test_spec();
        let width = pd_spill_record_width(&spec);
        let mut out: Vec<Vec<u8>> = vec![Vec::new(); PD_GROUP_PARTS];
        // Torn: not a whole number of records.
        assert!(pd_route_value_records(&spec, &vec![0u8; width + 1], 1, &mut out).is_err());
        // Depth outside the routing vocabulary.
        assert!(pd_route_value_records(&spec, &vec![0u8; width], 0, &mut out).is_err());
        assert!(pd_route_value_records(&spec, &vec![0u8; width], 7, &mut out).is_err());
        // Empty image routes nothing.
        assert!(pd_route_value_records(&spec, &[], 1, &mut out).is_ok());
        assert!(out.iter().all(|s| s.is_empty()));
        // One record lands in exactly the depth-1 (top-byte) slice.
        let mut rec = vec![0u8; width];
        rec[width - 8..].copy_from_slice(&77i64.to_ne_bytes());
        pd_route_value_records(&spec, &rec, 1, &mut out).unwrap();
        let expect = (mix64(77i64 as u64) >> 56) as usize;
        assert_eq!(out[expect].len(), width);
        assert_eq!(out.iter().map(|s| s.len()).sum::<usize>(), width);
    }

    // --- distinct-bytes car: canonical-bytes GROUP KEYS (spill record v2,
    // bytes-mode replay/route, arena-rebased concat) — fleet-run ----------

    fn bytes_test_spec() -> Arc<PdSpec> {
        // key 0 = text (canonical bytes), key 1 = int32 — the near-unique class
        // (text group key + COUNT(DISTINCT int8)) plus an int companion
        // exercising the mixed canonical image; one int64 set + CountStar.
        Arc::new(PdSpec {
            key_atts: vec![0, 1],
            key_kinds: vec![PdKeyKind::Bytes, PdKeyKind::Int(PdInt::I32)],
            vocab: vec![PdVocab { transno: 0, kind: PdVocabKind::CountStar }],
            sets: vec![PdSetSpec { att: 2, kind: DistinctKeyKind::Int64 }],
            max_att: 3,
            worker_budget: usize::MAX,
            expected_worker_rows: 0,
        })
    }

    /// Feed one (text-key group, value) into a builder — the accept
    /// kernel's staging discipline (probe by CONTENT, create copies into
    /// the builder's own arena).
    fn feed_b(b: &mut PdBuilder<'static>, text: &[u8], k1: i64, nulls: u32, v: Option<i64>) {
        let spec = Arc::clone(&b.spec);
        let words = [0i64, k1];
        let spans = [(0u32, if nulls & 1 != 0 { 0 } else { text.len() as u32 }), (0, 0)];
        let src = KeySrc::Staged(text, &spans);
        let h = PdBuilder::row_hash(&spec, &words, nulls, &src);
        let (found, slot) = b.probe(&words, nulls, h, &src);
        let g = match found {
            Some(g) => g,
            None => b.create_group(&words, nulls, h, slot, &src),
        } as usize;
        b.states[g * 2 * spec.vocab.len()] += 1;
        match v {
            Some(v) => b.dsets[g].insert_i64(v),
            None => b.dsets[g].seen_null = true,
        }
    }

    /// Deterministic text corpus: 41 keys of varied length (empty string
    /// included — a legal canonical tail), NULL text keys, NULL int keys.
    fn bytes_key_of(i: i64, salt: i64) -> Vec<u8> {
        let k = (i * 13 + salt) % 41;
        match k % 5 {
            0 => Vec::new(), // empty text
            1 => format!("k{k}").into_bytes(),
            2 => format!("phrase-{k}-{}", "x".repeat((k % 11) as usize)).into_bytes(),
            3 => format!("long-{}", "abcdefgh".repeat(1 + (k % 4) as usize)).into_bytes(),
            _ => format!("\u{00e9}clair-{k}").into_bytes(), // multi-byte UTF-8
        }
    }

    fn build_worker_b(spec: &Arc<PdSpec>, salt: i64) -> PdBuilder<'static> {
        let mut b = PdBuilder::new(Arc::clone(spec), usize::MAX, None);
        for i in 0..4000i64 {
            let text = bytes_key_of(i, salt);
            let k1 = (i * 7 + salt) % 11;
            let nulls = match (i + salt) % 29 {
                0 => 1u32, // NULL text key
                7 => 2u32, // NULL int key
                _ => 0u32,
            };
            feed_b(&mut b, &text, k1, nulls, Some((i * 104729 + salt) % 2500));
            if (i + salt) % 41 == 0 {
                feed_b(&mut b, &text, k1, nulls, None); // seen_null face
            }
        }
        b
    }

    /// Canonical view keyed by CONTENT (arena-independent): per group the
    /// text bytes, int word, nulls, states, per-set sorted values +
    /// seen_null.
    fn canon_b(
        spec: &PdSpec,
        m: &PdMerged<'_>,
    ) -> Vec<(Vec<u8>, i64, u32, Vec<i64>, Vec<(Vec<i64>, bool)>)> {
        let nkeys = spec.nkeys();
        let nsets = spec.sets.len();
        let nvocab = spec.vocab.len();
        let mut rows: Vec<_> = (0..m.ngroups)
            .map(|g| {
                let (off, len) = unpack_span(m.keys[g * nkeys]);
                let text = if m.keynulls[g] & 1 != 0 {
                    b"<NULL>".to_vec()
                } else {
                    m.key_arena[off..off + len].to_vec()
                };
                let sets = (0..nsets)
                    .map(|j| {
                        let d = m.dsets[g * nsets + j].as_ref().unwrap();
                        let mut vals = d.ints().to_vec();
                        vals.sort_unstable();
                        (vals, d.seen_null)
                    })
                    .collect();
                (
                    text,
                    m.keys[g * nkeys + 1],
                    m.keynulls[g],
                    m.states[g * 2 * nvocab..(g + 1) * 2 * nvocab].to_vec(),
                    sets,
                )
            })
            .collect();
        rows.sort();
        rows
    }

    /// Bytes-mode spill -> replay -> merge EQUALS the direct never-spilled
    /// merge on every bucket, across an epoch-reset boundary (epoch 2
    /// includes cross-epoch duplicate values AND brand-new groups that
    /// exist only in the in-memory remainder).
    #[test]
    fn bytes_key_spill_roundtrip_merge_equivalence() {
        let spec = bytes_test_spec();

        // Reference: direct merge of two never-spilled workers, epoch 2
        // rows included.
        let mut direct_tables = Vec::new();
        for salt in [0i64, 5] {
            let mut b = build_worker_b(&spec, salt);
            for i in 0..500i64 {
                let text = bytes_key_of(i * 3, salt + 100);
                feed_b(&mut b, &text, (i + salt) % 7, 0, Some(i % 173));
            }
            direct_tables.push(b.freeze().unwrap());
        }
        let direct = pd_concat_buckets(
            &spec,
            (0..PD_GROUP_PARTS)
                .map(|bk| pd_merge_bucket(&spec, &direct_tables, bk))
                .collect(),
        );

        // Spill arm: same content — epoch 1 spilled (bytes-mode records),
        // values reset, epoch 2 fed in-memory (duplicates + new groups).
        let mut spilled: Vec<std::collections::HashMap<u32, Vec<u8>>> = Vec::new();
        let mut remainders: Vec<PdHandedTable> = Vec::new();
        for salt in [0i64, 5] {
            let mut b = build_worker_b(&spec, salt);
            assert!(b.spill_eligible(), "bytes keys are spill-eligible");
            assert!(pd_spill_bytes_mode(&spec));
            let mut parts: std::collections::HashMap<u32, Vec<u8>> = Default::default();
            let mut last_p: i64 = -1;
            b.spill_emit(&mut |pt, bytes| {
                assert!((pt as i64) > last_p, "partitions ascend");
                last_p = pt as i64;
                assert_eq!(bytes.len() % 8, 0, "bytes-mode records stay 8-aligned");
                parts.entry(pt).or_default().extend_from_slice(bytes);
                Ok(())
            })
            .unwrap();
            b.spill_reset_values();
            // Epoch 2: duplicate values for existing groups (undo the extra
            // CountStar bump so vocab words match the reference) + NEW
            // groups only the remainder carries.
            for i in 0..500i64 {
                let text = bytes_key_of(i * 3, salt + 100);
                feed_b(&mut b, &text, (i + salt) % 7, 0, Some(i % 173));
            }
            spilled.push(parts);
            remainders.push(b.freeze().unwrap());
        }
        let mut buckets = Vec::with_capacity(PD_GROUP_PARTS);
        for bkt in 0..PD_GROUP_PARTS {
            let mut synth: Vec<PdHandedTable> = Vec::new();
            for parts in &spilled {
                if let Some(bytes) = parts.get(&(bkt as u32)) {
                    synth.push(pd_table_from_spill(&spec, bytes).unwrap());
                }
            }
            let refs: Vec<&PdHandedTable> = remainders.iter().chain(synth.iter()).collect();
            buckets.push(pd_merge_bucket_refs(&spec, &refs, bkt));
        }
        let merged = pd_concat_buckets(&spec, buckets);

        assert_eq!(canon_b(&spec, &direct), canon_b(&spec, &merged));
        assert!(direct.ngroups > 40, "corpus produced a real group population");

        // Pre-count sanity: groups bound + key-bytes term populated.
        let mut groups = 0usize;
        let mut key_bytes = 0usize;
        for t in &remainders {
            for bkt in 0..PD_GROUP_PARTS {
                let (g, _, kb) = pd_bucket_precount(&spec, t, bkt);
                groups += g;
                key_bytes += kb;
            }
        }
        assert!(groups >= direct.ngroups);
        assert!(key_bytes > 0);
    }

    /// Torn / corrupt BYTES-mode records fail closed (never a silent wrong
    /// answer): bad rec_len, truncated tail, corrupt set index, keynulls
    /// out of range, tail-length inconsistency; empty and single-record
    /// images are fine.
    #[test]
    fn bytes_key_torn_records_fail_closed() {
        let spec = bytes_test_spec();
        // A well-formed single-group, single-value image via the emitter.
        let mut b = PdBuilder::new(Arc::clone(&spec), usize::MAX, None);
        feed_b(&mut b, b"hello", 3, 0, Some(42));
        let mut img: Vec<u8> = Vec::new();
        b.spill_emit(&mut |_p, bytes| {
            img.extend_from_slice(bytes);
            Ok(())
        })
        .unwrap();
        let min_w = pd_spill_min_record_width(&spec);
        assert!(img.len() >= min_w + 8, "one record with a nonempty tail");
        assert!(pd_table_from_spill(&spec, &img).is_ok());
        assert!(pd_table_from_spill(&spec, &[]).is_ok());
        // Torn: trailing garbage byte-count (not covered by rec_len).
        let mut torn = img.clone();
        torn.extend_from_slice(&[0u8; 4]);
        assert!(pd_table_from_spill(&spec, &torn).is_err());
        // Corrupt rec_len: too small / unaligned / past the buffer.
        for bad in [8u64, (min_w as u64) + 1, (img.len() as u64) + 8] {
            let mut r = img.clone();
            r[..8].copy_from_slice(&bad.to_ne_bytes());
            assert!(pd_table_from_spill(&spec, &r).is_err(), "rec_len {bad}");
        }
        // Corrupt set index.
        let mut r = img.clone();
        r[16..24].copy_from_slice(&u64::MAX.to_ne_bytes());
        assert!(pd_table_from_spill(&spec, &r).is_err());
        // Keynulls out of range.
        let mut r = img.clone();
        r[8..16].copy_from_slice(&(1u64 << 63).to_ne_bytes());
        assert!(pd_table_from_spill(&spec, &r).is_err());
        // Tail length inconsistent with rec_len.
        let tail_len_off = 32 + spec.nkeys() * 8;
        let mut r = img.clone();
        r[tail_len_off..tail_len_off + 8].copy_from_slice(&1000u64.to_ne_bytes());
        assert!(pd_table_from_spill(&spec, &r).is_err());
        // The router fails closed on the same shapes.
        let mut out: Vec<Vec<u8>> = vec![Vec::new(); PD_GROUP_PARTS];
        assert!(pd_route_value_records(&spec, &torn, 1, &mut out).is_err());
        let mut r = img.clone();
        r[..8].copy_from_slice(&8u64.to_ne_bytes());
        assert!(pd_route_value_records(&spec, &r, 1, &mut out).is_err());
    }

    /// Bytes-mode value routing: totals preserved, every record has exactly
    /// one home slice, split merge (in-memory once + slices in sequence)
    /// equals the direct merge — depths 1 and 2.
    #[test]
    fn bytes_key_split_slice_merge_invariance() {
        let spec = bytes_test_spec();
        let t1 = build_worker_b(&spec, 0).freeze().unwrap();
        let t2 = build_worker_b(&spec, 5).freeze().unwrap();
        let tables = [t1, t2];
        let direct = pd_concat_buckets(
            &spec,
            (0..PD_GROUP_PARTS).map(|bk| pd_merge_bucket(&spec, &tables, bk)).collect(),
        );

        let mut spilled: Vec<std::collections::HashMap<u32, Vec<u8>>> = Vec::new();
        let mut remainders: Vec<PdHandedTable> = Vec::new();
        for salt in [0i64, 5] {
            let mut b = build_worker_b(&spec, salt);
            let mut parts: std::collections::HashMap<u32, Vec<u8>> = Default::default();
            b.spill_emit(&mut |pt, bytes| {
                parts.entry(pt).or_default().extend_from_slice(bytes);
                Ok(())
            })
            .unwrap();
            b.spill_reset_values();
            spilled.push(parts);
            remainders.push(b.freeze().unwrap());
        }

        for depth in [1u32, 2] {
            let mut buckets = Vec::with_capacity(PD_GROUP_PARTS);
            for bkt in 0..PD_GROUP_PARTS {
                let mut bytes = Vec::new();
                for parts in &spilled {
                    if let Some(bb) = parts.get(&(bkt as u32)) {
                        bytes.extend_from_slice(bb);
                    }
                }
                let mut slices: Vec<Vec<u8>> = vec![Vec::new(); PD_GROUP_PARTS];
                pd_route_value_records(&spec, &bytes, depth, &mut slices).unwrap();
                assert_eq!(slices.iter().map(|s| s.len()).sum::<usize>(), bytes.len());
                let mut merger = PdBucketMerger::new(&spec);
                for t in &remainders {
                    merger.absorb(t, bkt);
                }
                for sl in &slices {
                    if sl.is_empty() {
                        continue;
                    }
                    let synth = pd_table_from_spill(&spec, sl).unwrap();
                    merger.absorb(&synth, bkt);
                }
                buckets.push(merger.finish());
            }
            let merged = pd_concat_buckets(&spec, buckets);
            assert_eq!(canon_b(&spec, &direct), canon_b(&spec, &merged), "depth {depth}");
        }
    }

    // --- PAREMIT (fleet-run: the known local nodeagg test-binary link
    // limitation) ---------------------------------------------------------

    /// pg_catalog "C" collation — statically known-C, so the comparator's
    /// varstr_cmp rides the catalog-free memcmp fast path in units.
    const C_COLL: Oid = 950;

    fn paremit_spec(key_kinds: Vec<PdKeyKind>, nsets: usize, vocab: Vec<PdVocab>) -> PdSpec {
        PdSpec {
            key_atts: (0..key_kinds.len() as u16).collect(),
            key_kinds,
            vocab,
            sets: (0..nsets)
                .map(|_| PdSetSpec { att: 0, kind: DistinctKeyKind::Int64 })
                .collect(),
            max_att: 8,
            worker_budget: usize::MAX,
            expected_worker_rows: 0,
        }
    }

    /// Bucket build vs the adopt reference: `order_groups` over the same
    /// merged partition IS the adopt arm's ordering authority, and the
    /// datum vocabulary (width-matched ints, 4B-header text images, set
    /// value counts, vocab count/sum with the NULL-iff-count-0 law) must
    /// match `agg_hashgroup_adopt_merged`'s materialization exactly.
    #[test]
    fn paremit_bucket_matches_adopt_reference() {
        use crate::hashgrouped::HashGroupOrderKey;
        let spec = paremit_spec(
            vec![PdKeyKind::Bytes, PdKeyKind::Int(PdInt::I32)],
            1,
            vec![
                PdVocab { transno: 1, kind: PdVocabKind::CountStar },
                PdVocab { transno: 2, kind: PdVocabKind::SumInt { att: 3, kind: PdInt::I16 } },
            ],
        );
        // 5 groups: empty text, multi-byte UTF-8, NULL text key, NULL int
        // key, plain — with set faces {values, empty+seen_null, None} and
        // a cnt=0 sum (NULL law).
        let texts: [&[u8]; 5] = [b"", "h\u{e9}llo".as_bytes(), b"", b"abc", b"zz"];
        let ints: [i64; 5] = [1, 2, 3, 0, 5];
        let keynulls: [u32; 5] = [0, 0, 0b01, 0b10, 0];
        let mut key_arena = Vec::new();
        let mut keys = Vec::new();
        for g in 0..5 {
            if keynulls[g] & 1 != 0 {
                keys.push(0);
            } else {
                let off = key_arena.len();
                key_arena.extend_from_slice(texts[g]);
                keys.push(pack_span(off, texts[g].len()));
            }
            keys.push(if keynulls[g] & 2 != 0 { 0 } else { ints[g] });
        }
        let mut dsets: Vec<Option<DistinctSet<'static>>> = Vec::new();
        let mut s0 = DistinctSet::new();
        for v in [10i64, 20, 10, 30] {
            s0.insert_i64(v);
        }
        dsets.push(Some(s0)); // 3 distinct values
        let mut s1 = DistinctSet::new();
        s1.seen_null = true; // strict-skip: counts 0
        dsets.push(Some(s1));
        dsets.push(None); // never-materialized set: counts 0
        let mut s3 = DistinctSet::new();
        s3.insert_i64(7);
        dsets.push(Some(s3));
        dsets.push(Some(DistinctSet::new()));
        // (acc, cnt) pairs, stride 4: CountStar then SumInt; group 3's sum
        // saw no non-null input (cnt=0 → NULL).
        let states = vec![
            4, 4, 100, 4, // g0
            1, 1, -7, 1, // g1
            2, 2, 0, 2, // g2
            3, 3, 0, 0, // g3: sum NULL
            1, 1, 8, 1, // g4
        ];
        let m: PdMerged<'static> =
            PdMerged { ngroups: 5, keys, key_arena, keynulls: keynulls.to_vec(), states, dsets };
        let order_spec = vec![
            HashGroupOrderKey { key_idx: 0, desc: false, nulls_first: false, collation: C_COLL },
            HashGroupOrderKey { key_idx: 1, desc: true, nulls_first: true, collation: 0 },
        ];
        let recipe = pd_paremit_recipe(
            &spec,
            &[
                PdParemitCol::Key(0),
                PdParemitCol::Key(1),
                PdParemitCol::SetCount(0),
                PdParemitCol::Vocab { transno: 1, sum: false },
                PdParemitCol::Vocab { transno: 2, sum: true },
            ],
            &order_spec,
        )
        .expect("recipe resolves");
        let (b, _) = pd_emit_bucket(&spec, &recipe, &m, None).expect("bucket builds");
        assert_eq!(b.nrows, 5);
        assert_eq!(b.natts, 5);
        // The adopt reference order over the same partition.
        let kinds = recipe.hg_kinds();
        let order =
            order_groups(&m.keys, &m.keynulls, &recipe.order, 2, &kinds, &m.key_arena, 5)
                .expect("reference order");
        let expect_counts: [i64; 5] = [3, 0, 0, 1, 0];
        for (row, &g) in order.iter().enumerate() {
            let g = g as usize;
            assert_eq!(b.keynulls[row], m.keynulls[g], "keynulls follow the order");
            let base = row * 5;
            // Key(0) text: 4B-header image whose content equals the source
            // bytes; the sidecar span points at the content, the datum 4
            // bytes earlier at the header.
            if m.keynulls[g] & 1 == 0 {
                let (off, len) = unpack_span(b.keys[row * 2]);
                assert_eq!(&b.arena[off..off + len], texts[g]);
                assert_eq!(
                    u32::from_ne_bytes(b.arena[off - 4..off].try_into().unwrap()),
                    ::types_tuple::varatt::set_varsize_4b_word((len + 4) as u32)
                );
                assert_eq!(
                    b.values[base].as_usize(),
                    b.arena[off - 4..].as_ptr() as usize,
                    "text datum points at its own arena image"
                );
                assert!(!b.nulls[base]);
            } else {
                assert!(b.nulls[base]);
            }
            // Key(1) int32: width-matched datum.
            if m.keynulls[g] & 2 == 0 {
                assert_eq!(b.values[base + 1].as_i32(), ints[g] as i32);
                assert!(!b.nulls[base + 1]);
            } else {
                assert!(b.nulls[base + 1]);
            }
            // count(DISTINCT) = value count, never NULL.
            assert_eq!(b.values[base + 2].as_i64(), expect_counts[g]);
            assert!(!b.nulls[base + 2]);
            // CountStar = acc, never NULL.
            assert_eq!(b.values[base + 3].as_i64(), m.states[g * 4]);
            assert!(!b.nulls[base + 3]);
            // SumInt: NULL iff cnt == 0.
            if m.states[g * 4 + 3] > 0 {
                assert_eq!(b.values[base + 4].as_i64(), m.states[g * 4 + 2]);
                assert!(!b.nulls[base + 4]);
            } else {
                assert!(b.nulls[base + 4]);
            }
        }
    }

    /// K-way merge oracle: the leader's cross-bucket merge over per-bucket
    /// `order_groups`-sorted rows must equal `order_groups` over the
    /// CONCATENATED group set (the adopt arm's exact sequence) — int keys
    /// with a NULL under nulls_first + DESC, and text keys across
    /// bucket-local arenas under "C" collation.
    #[test]
    fn paremit_merge_matches_whole_order() {
        use crate::hashgrouped::HashGroupOrderKey;
        // --- int case: 31 distinct keys (one NULL), DESC, nulls_first.
        let spec = paremit_spec(
            vec![PdKeyKind::Int(PdInt::I64)],
            0,
            vec![PdVocab { transno: 0, kind: PdVocabKind::CountStar }],
        );
        let order_spec = vec![HashGroupOrderKey {
            key_idx: 0,
            desc: true,
            nulls_first: true,
            collation: 0,
        }];
        let recipe = pd_paremit_recipe(
            &spec,
            &[PdParemitCol::Key(0), PdParemitCol::Vocab { transno: 0, sum: false }],
            &order_spec,
        )
        .expect("recipe resolves");
        let all_keys: Vec<i64> = (0..31).map(|i| (i * 37 % 61) - 30).collect();
        let mut buckets = Vec::new();
        let mut cat_keys = Vec::new();
        let mut cat_nulls = Vec::new();
        for bi in 0..3usize {
            let mut keys = Vec::new();
            let mut keynulls = Vec::new();
            for (i, &k) in all_keys.iter().enumerate() {
                if i % 3 != bi {
                    continue;
                }
                let isnull = i == 7;
                keys.push(if isnull { 0 } else { k });
                keynulls.push(u32::from(isnull));
                cat_keys.push(if isnull { 0 } else { k });
                cat_nulls.push(u32::from(isnull));
            }
            let n = keys.len();
            let m: PdMerged<'static> = PdMerged {
                ngroups: n,
                keys,
                key_arena: Vec::new(),
                keynulls,
                states: vec![1; n * 2],
                dsets: Vec::new(),
            };
            buckets.push(pd_emit_bucket(&spec, &recipe, &m, None).expect("bucket").0);
        }
        let kinds = recipe.hg_kinds();
        let whole = order_groups(
            &cat_keys,
            &cat_nulls,
            &recipe.order,
            1,
            &kinds,
            &[],
            cat_keys.len(),
        )
        .expect("whole order");
        let expect: Vec<(bool, i64)> = whole
            .iter()
            .map(|&g| (cat_nulls[g as usize] != 0, cat_keys[g as usize]))
            .collect();
        let mut st = pd_paremit_state(&recipe, buckets, None).expect("state");
        let mut got = Vec::new();
        while let Some((b, row)) = pd_paremit_next(&mut st).expect("next") {
            let (values, nulls) = st.row(b, row);
            got.push((nulls[0], if nulls[0] { 0 } else { values[0].as_i64() }));
        }
        assert_eq!(got, expect);

        // --- text case: distinct strings across two bucket-local arenas.
        let tspec = paremit_spec(
            vec![PdKeyKind::Bytes],
            0,
            vec![PdVocab { transno: 0, kind: PdVocabKind::CountStar }],
        );
        let torder = vec![HashGroupOrderKey {
            key_idx: 0,
            desc: false,
            nulls_first: false,
            collation: C_COLL,
        }];
        let trecipe = pd_paremit_recipe(
            &tspec,
            &[PdParemitCol::Key(0), PdParemitCol::Vocab { transno: 0, sum: false }],
            &torder,
        )
        .expect("recipe resolves");
        let words: [&[u8]; 8] = [
            b"zebra",
            b"",
            "\u{e9}clair".as_bytes(),
            b"apple",
            b"apples",
            b"Zebra",
            b"mid",
            b"appl",
        ];
        let mut tbuckets = Vec::new();
        let mut cat_keys = Vec::new();
        let mut cat_arena = Vec::new();
        let mut cat_nulls = Vec::new();
        for bi in 0..2usize {
            let mut keys = Vec::new();
            let mut arena = Vec::new();
            for (i, w) in words.iter().enumerate() {
                if i % 2 != bi {
                    continue;
                }
                let off = arena.len();
                arena.extend_from_slice(w);
                keys.push(pack_span(off, w.len()));
                let coff = cat_arena.len();
                cat_arena.extend_from_slice(w);
                cat_keys.push(pack_span(coff, w.len()));
                cat_nulls.push(0);
            }
            let n = keys.len();
            let m: PdMerged<'static> = PdMerged {
                ngroups: n,
                keys,
                key_arena: arena,
                keynulls: vec![0; n],
                states: vec![1; n * 2],
                dsets: Vec::new(),
            };
            tbuckets.push(pd_emit_bucket(&tspec, &trecipe, &m, None).expect("bucket").0);
        }
        let tkinds = trecipe.hg_kinds();
        let whole = order_groups(
            &cat_keys,
            &cat_nulls,
            &trecipe.order,
            1,
            &tkinds,
            &cat_arena,
            cat_keys.len(),
        )
        .expect("whole order");
        let expect: Vec<Vec<u8>> = whole
            .iter()
            .map(|&g| {
                let (off, len) = unpack_span(cat_keys[g as usize]);
                cat_arena[off..off + len].to_vec()
            })
            .collect();
        let mut st = pd_paremit_state(&trecipe, tbuckets, None).expect("state");
        let mut got = Vec::new();
        while let Some((b, row)) = pd_paremit_next(&mut st).expect("next") {
            let (values, nulls) = st.row(b, row);
            assert!(!nulls[0]);
            // Read back through the varlena datum: 4B header + content.
            let ptr = values[0].as_usize() as *const u8;
            // SAFETY: the datum points into the state's live bucket arena.
            let hdr = unsafe { core::ptr::read_unaligned(ptr.cast::<u32>()) };
            let len = (::types_tuple::varatt::varsize_4b_word(hdr) as usize) - 4;
            let content =
                unsafe { core::slice::from_raw_parts(ptr.add(4), len) };
            got.push(content.to_vec());
        }
        assert_eq!(got, expect);
    }

    /// Kernel-2 oracle (the W≡F-style byte-identity gate at unit
    /// altitude): bounded selection ON emits exactly the rows the FULL
    /// drain's downstream bounded sort would retain — same winner set
    /// (boundary ties keep the earliest group order: C's bounded heap
    /// discards on tie, so first-arriving survives), same emit order
    /// (group order), same datum words — and the compact-materialization
    /// law holds (each bucket materializes exactly its candidate rows).
    /// Exercised over BOTH order directions and a boundary that lands
    /// mid-tie-group, groups split across two partitions.
    #[test]
    fn topn_selection_matches_full_drain_retention() {
        use crate::hashgrouped::HashGroupOrderKey;
        let spec = paremit_spec(vec![PdKeyKind::Int(PdInt::I64)], 1, Vec::new());
        let order_spec = vec![HashGroupOrderKey {
            key_idx: 0,
            desc: false,
            nulls_first: false,
            collation: 0,
        }];
        let recipe = pd_paremit_recipe(
            &spec,
            &[PdParemitCol::Key(0), PdParemitCol::SetCount(0)],
            &order_spec,
        )
        .expect("recipe resolves");
        // (key, distinct-count): counts 5,3,5,2,3,3,1,5,2 — ties at every
        // interesting boundary. Keys ascending = group order.
        let groups: [(i64, usize); 9] =
            [(1, 5), (2, 3), (3, 5), (4, 2), (5, 3), (6, 3), (7, 1), (8, 5), (9, 2)];
        let build = |bi: usize| -> PdMerged<'static> {
            let mut keys = Vec::new();
            let mut keynulls = Vec::new();
            let mut dsets: Vec<Option<DistinctSet<'static>>> = Vec::new();
            for (i, &(k, c)) in groups.iter().enumerate() {
                if i % 2 != bi {
                    continue;
                }
                keys.push(k);
                keynulls.push(0);
                let mut d = DistinctSet::new();
                for v in 0..c {
                    d.insert_i64(v as i64 * 7 + k);
                }
                dsets.push(Some(d));
            }
            let n = keys.len();
            PdMerged {
                ngroups: n,
                keys,
                key_arena: Vec::new(),
                keynulls,
                states: Vec::new(),
                dsets,
            }
        };
        for desc in [true, false] {
            for bound in [1u32, 3, 4, 6, 9, 20] {
                // FULL drain stream: (key, count) rows in group order.
                let full_bufs: Vec<PdEmitBucket> = (0..2)
                    .map(|bi| pd_emit_bucket(&spec, &recipe, &build(bi), None).unwrap().0)
                    .collect();
                let mut st = pd_paremit_state(&recipe, full_bufs, None).unwrap();
                let mut full: Vec<(i64, i64)> = Vec::new();
                while let Some((b, row)) = pd_paremit_next(&mut st).unwrap() {
                    let (v, nl) = st.row(b, row);
                    assert!(!nl[0] && !nl[1]);
                    full.push((v[0].as_i64(), v[1].as_i64()));
                }
                assert_eq!(full.len(), groups.len());
                // The downstream bounded sort\'s retention: first-arriving
                // among boundary ties (discard-on-tie) = the k smallest
                // (badness, arrival rank) pairs.
                let mut ranked: Vec<(u64, usize)> = full
                    .iter()
                    .enumerate()
                    .map(|(i, &(_, c))| (crate::compact::topkfin_badness(c, desc), i))
                    .collect();
                ranked.sort_unstable();
                let k = (bound as usize).min(full.len());
                let mut kept: Vec<usize> = ranked[..k].iter().map(|&(_, i)| i).collect();
                kept.sort_unstable();
                let expect: Vec<(i64, i64)> = kept.iter().map(|&i| full[i]).collect();
                // WINNERS arm.
                let topn = PdTopnSpec { key: PdTopnKey::SetCount(0), desc, bound };
                let mut bufs = Vec::new();
                let mut cands = Vec::new();
                for bi in 0..2 {
                    let (b, c) =
                        pd_emit_bucket(&spec, &recipe, &build(bi), Some(&topn)).unwrap();
                    let c = c.expect("armed selection returns candidates");
                    // Compact-materialization law: bucket rows ARE the
                    // candidates.
                    assert_eq!(b.nrows, c.len());
                    assert!(c.len() <= bound as usize);
                    bufs.push(b);
                    cands.push(c);
                }
                let mut st =
                    pd_paremit_state(&recipe, bufs, Some((&cands[..], bound))).unwrap();
                assert_eq!(st.kept_rows(), Some(expect.len()));
                let mut got: Vec<(i64, i64)> = Vec::new();
                while let Some((b, row)) = pd_paremit_next(&mut st).unwrap() {
                    let (v, nl) = st.row(b, row);
                    assert!(!nl[0] && !nl[1]);
                    got.push((v[0].as_i64(), v[1].as_i64()));
                }
                assert_eq!(got, expect, "desc={desc} bound={bound}");
            }
        }
    }

}
