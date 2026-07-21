//! M3 SORT SINK — parallel top-N (ORDER BY key LIMIT n) on the morsel
//! runtime (docs/design/m3-sort.md §2–§4; docs/design/
//! parallelism-redesign-2026-07.md §2.2/§5-M3).
//!
//! Shape: the SERIAL-plan bounded sort breaker `Sort(bounded) ←
//! SeqScan(pgrcolumnar)` (the ClickBench Q24/Q25 class), executed as one
//! SealedParallelSink on the runtime: ACCEPT (granule-morsel scan →
//! PREWHERE → narrow (key, rowref) pushes into a per-worker bounded
//! `TopnHeap` on the tie-ordering rule-2 TOTAL order) → SEAL (parallel
//! per-worker `into_sorted`) → COMBINE (partitions() = 1: one k-way
//! truncate-merge of ≤ W×bound POD entries — the only serial point between
//! scan end and gather) → finalize (publish the winner list). The parked
//! leader adopts the winners and performs refsort v2's ONE
//! late-materialization gather (`seq_scan_gather_row` per winner, ≤ bound
//! rows total — vs the Gather-Merge arm's N_workers × bound disease) into
//! the node's `refsort_out` buffer; the UNCHANGED refsort emit face serves
//! them in merged order.
//!
//! Determinism (design §3): the winner list is the `bound` smallest
//! entries of the union under a total order (rowrefs unique across
//! disjoint granule claims) — a pure function of the table contents,
//! independent of claim order and worker count. No tie tracking, no
//! demote ladder. Ordering/parity law vs non-rowref-canonical serial
//! channels: design §4 (tie-normalized gates + boundary-tie count gate).
//!
//! Memory (design §7): a Local is ≤ bound × 16 B — no work_mem
//! interaction, no budget-crossing path. The only mid-flight fallback is a
//! CONTRACT BREAK (a staged batch without a window ref, a gather miss):
//! recorded, RG aborted, leader reruns the serial arm from scratch
//! (nothing was emitted — the R5 whole-attempt-rerun discipline).
//!
//! Engagement layering (all cheap; absent = today's serial path, byte-
//! and perf-identical): PGRUST_RUNTIME=1 (pool spawned) + SET
//! pgrust.runtime_sort_pool = <dop> (this arm's own DOP knob — NOT the
//! scan knob; the m2-distinct coupling gotcha is deliberately avoided) +
//! PGRUST_RUNTIME_SORT != 0 (arm kill switch). Plan surface stays the
//! serial plan; EXPLAIN unchanged; instrumented runs refuse (EXPLAIN
//! ANALYZE stays C-exact).

use std::cell::UnsafeCell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use ::executils::{EStateData, ExecSlotId};
use ::nodesort::sink::{
    topn_merge, TopnEntry, TopnHeap, TopnWideHeap, WideEntry, TOPN_MAX_BOUND,
};
use ::types_error::{PgError, PgResult, ERROR};
use ::types_nodes::plannodes::PlannedStmt;
use ::types_nodes::NodeTag;

use super::router::{self, ArmClass, ArmCounter};
use super::{drain_pipeline, BatchEmit, BatchSink, SeqScanFilterProject, SeqScanSource, Sink, SinkFeed};
use super::{lane_trace, seq_scan_fusible, trace_feed};

// ---------------------------------------------------------------------------
// Admission spec (leader-derived plain data; workers read it from the
// payload). The shape law is the refsort census (lanev2 `refsort_arm`,
// duplicated here rather than refactored — the serial arm keeps its own
// kill switch, sticky-refusal memo and Gather-era parallel refusal, none of
// which apply to the sink) PLUS the int-family key vocabulary the POD heap
// encoding requires (the adaptive-walk vocabulary, `CmpOp::for_fn_oid`).
// ---------------------------------------------------------------------------

/// Sort-key datum width for the order-preserving i64 widening
/// (`TopnEntry::encode`'s input contract). The CmpOp families guarantee the
/// widths: Int2/Int4/Int8 compare sign-extended, Oid compares unsigned
/// 32-bit (zero-extended to i64 it stays order-correct).
#[derive(Clone, Copy)]
enum KeyWidth {
    I2,
    I4,
    I8,
    U32,
}

#[inline]
fn key_i64(d: ::datum::Datum, w: KeyWidth) -> i64 {
    match w {
        KeyWidth::I2 => d.as_i16() as i64,
        KeyWidth::I4 => d.as_i32() as i64,
        KeyWidth::I8 => d.as_i64(),
        KeyWidth::U32 => d.as_u32() as i64,
    }
}

/// One admitted sort key (plain data; workers read it from the payload).
#[derive(Clone, Copy)]
struct KeyCol {
    /// Scan column (0-based; the SoA fast-leg read).
    attno_scan: u16,
    /// Position in the outer (child output) desc — the fallback leg reads
    /// this projected slot cell.
    resno_outer: usize,
    desc: bool,
    nulls_first: bool,
    width: KeyWidth,
    /// DictCode class (docs/design/dict-code-flow.md inc-1): the key is a
    /// dict-text column ordered by its v7 part-global byte-rank code (u32,
    /// order-identical to `varstr_cmp` under the admitted memcmp-safe
    /// collation). The observation is the stitch global code widened to
    /// i64 (`width` is `KeyWidth::U32` for the encode contract); the
    /// per-row emit fallback leg CANNOT serve this class (a text datum has
    /// no order-preserving i64) — any fallback-forced row or unstitched /
    /// non-dict window is a sink contract break (RG abort, serial rerun).
    dictcode: bool,
}

struct TopnSpec {
    /// The admitted sort keys in plan order (1 = the narrow u128 heap;
    /// 2..=TOPN_MAX_KEYS = the wide heap — inc-5).
    keys: Vec<KeyCol>,
    /// Outer resno -> scan attno (the deferred Var-only winner projection).
    tlist_map: Vec<u16>,
    bound: usize,
}

/// Shared shape derivation (both sort-sink arms): datum-sort refusal,
/// pgrcolumnar window-ref availability, the Var-only tlist census, and the
/// int-family key vocabulary. `None` = the serial feed runs unchanged.
fn sort_keys_and_map<'mcx>(
    state: &::nodesort::SortState<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
    outer_desc: &::types_tuple::TupleDescData<'static>,
) -> Option<(Vec<KeyCol>, Vec<u16>)> {
    // Single-column output sorts bare datums (nothing to late-materialize
    // / no row image to carry) — refused UNLESS a DictCode key admits (the
    // deferred check below): for a dict-text key the datum IS a string
    // gather per survivor, exactly what the winner-only late
    // materialization elides (Q26's shape).
    let is_datum = ::nodesort::sort_lane_is_datum(state);
    let plan = state.plan;
    let nkeys = plan.numCols as usize;
    if !(1..=::nodesort::sink::TOPN_MAX_KEYS).contains(&nkeys)
        || plan.sortColIdx.len() < nkeys
        || plan.sortOperators.len() < nkeys
        || plan.nullsFirst.len() < nkeys
    {
        return None;
    }
    // Window refs only exist for pgrcolumnar staged batches.
    if !::nodeseqscan::seq_scan_is_pgrcolumnar(ss) {
        return None;
    }
    let natts = outer_desc.natts as usize;
    let tlist_map: Vec<u16> = match ss.ss.ps_ProjInfo.as_ref() {
        // No projection: outer resno j is scan attno j (physical tlist).
        None => (0..natts as u16).collect(),
        // Projected scans admit only the pure Var-copy census (a computing
        // column deferred past the accept could elide C's error).
        Some(p) => match p.pi_state.scan_proj_cols() {
            Some(cols) => {
                if cols.any_arith() || cols.n as usize != natts {
                    return None;
                }
                cols.cols[..natts]
                    .iter()
                    .map(|c| match *c {
                        ::execexpr::ScanProjCol::Var { attnum } => Some(attnum),
                        _ => None,
                    })
                    .collect::<Option<Vec<u16>>>()?
            }
            // A single-column pure-Var projection compiles to a
            // JustAssignVar KERNEL (no step program => no scan_proj_cols
            // census) — the datum-shaped DictCode admission (Q26's exact
            // shape; the sortkey_direct census, same pattern).
            None => match p.pi_state.kernel() {
                ::execexpr::Kernel::JustAssignVar {
                    src: ::execexpr::SlotSrc::Scan,
                    attnum,
                    resultnum: 0,
                }
                | ::execexpr::Kernel::JustAssignVarVirt {
                    src: ::execexpr::SlotSrc::Scan,
                    attnum,
                    resultnum: 0,
                } if natts == 1 => vec![attnum],
                _ => return None,
            },
        },
    };
    // Every key: int-family operator (the POD-heap encoding vocabulary —
    // the adaptive walk's own list; timestamps/dates ride their I8/I4 cmp
    // shapes) over a mapped scan column, OR the DictCode text class
    // (docs/design/dict-code-flow.md inc-1): `text_lt`/`text_gt` over a
    // text/varchar column under a memcmp-safe deterministic collation —
    // the observation is the v7 part-global byte-rank code, order-
    // identical to `varstr_cmp` by the sorted-dict/stitch construction.
    // Anything else refuses.
    // pg_proc text_lt / text_gt — the btree text opclass's `<` / `>`.
    const F_TEXT_LT: ::types_core::Oid = 740;
    const F_TEXT_GT: ::types_core::Oid = 742;
    let mut keys = Vec::with_capacity(nkeys);
    for i in 0..nkeys {
        let oc = plan.sortColIdx[i];
        if oc < 1 || oc as usize > natts {
            return None;
        }
        let resno_outer = (oc - 1) as usize;
        let opfn = ::lsyscache::get_opcode(plan.sortOperators[i]).ok()?;
        use ::execexpr::CmpOp::*;
        let (width, desc, dictcode) = match ::execexpr::CmpOp::for_fn_oid(opfn) {
            Some(Int2Lt) => (KeyWidth::I2, false, false),
            Some(Int2Gt) => (KeyWidth::I2, true, false),
            Some(Int4Lt) => (KeyWidth::I4, false, false),
            Some(Int4Gt) => (KeyWidth::I4, true, false),
            Some(Int8Lt) => (KeyWidth::I8, false, false),
            Some(Int8Gt) => (KeyWidth::I8, true, false),
            Some(OidLt) => (KeyWidth::U32, false, false),
            Some(OidGt) => (KeyWidth::U32, true, false),
            _ => {
                if !(opfn == F_TEXT_LT || opfn == F_TEXT_GT)
                    || !runtime_sort_dictcode_enabled()
                {
                    return None;
                }
                // Order via byte-rank codes is `varstr_cmp` order only
                // under the memcmp tier (C-locale class); everything else
                // refuses (Law D, dict-code-flow.md §2.2).
                if plan.collations.len() < nkeys {
                    return None;
                }
                let coll = plan.collations[i];
                if coll == 0 || !::lanefold::str_collation_safe(coll) {
                    return None;
                }
                // TEXT(25)/VARCHAR(1043) only (the codedgroup census;
                // bpchar's space-pad compare is not memcmp).
                let atttypid = outer_desc.attrs[resno_outer].atttypid;
                if atttypid != 25 && atttypid != 1043 {
                    return None;
                }
                (KeyWidth::U32, opfn == F_TEXT_GT, true)
            }
        };
        keys.push(KeyCol {
            attno_scan: tlist_map[resno_outer],
            resno_outer,
            desc,
            nulls_first: plan.nullsFirst[i],
            width,
            dictcode,
        });
    }
    // The deferred datum-shape gate (see the census note above).
    if is_datum && !keys.iter().any(|k| k.dictcode) {
        return None;
    }
    Some((keys, tlist_map))
}

/// Shape derivation (fail-closed; `None` = the serial feed runs unchanged).
fn topn_spec<'mcx>(
    state: &::nodesort::SortState<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
    outer_desc: &::types_tuple::TupleDescData<'static>,
) -> Option<TopnSpec> {
    if !state.bounded || state.bound <= 0 || state.bound > TOPN_MAX_BOUND as i64 {
        return None;
    }
    let (keys, tlist_map) = sort_keys_and_map(state, ss, outer_desc)?;
    Some(TopnSpec { keys, tlist_map, bound: state.bound as usize })
}

/// Shape-(b) full-sort spec: UNBOUNDED forward-only sorts; the shared key
/// vocabulary + the self-contained-copy column census (byval / fixed-len
/// byref / varlena; cstring refuses).
struct FullSpec {
    keys: Vec<KeyCol>,
    natts: usize,
    cols: Vec<::nodesort::fullsort::RunCol>,
}

fn full_spec<'mcx>(
    state: &::nodesort::SortState<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
    outer_desc: &::types_tuple::TupleDescData<'static>,
) -> Option<FullSpec> {
    // Bounded sorts are the top-N arm's; rewind/mark/backward shapes
    // refuse (the runtime result is forward-streamed once).
    if state.bounded || state.randomAccess {
        return None;
    }
    let (keys, _tlist_map) = sort_keys_and_map(state, ss, outer_desc)?;
    // DictCode keys are top-N-only (inc-1): the full-sort accept reads
    // every key from the projected slot (per-row emit leg), which cannot
    // serve a code observation.
    if keys.iter().any(|k| k.dictcode) {
        return None;
    }
    let natts = outer_desc.natts as usize;
    let mut cols = Vec::with_capacity(natts);
    for a in outer_desc.attrs.iter().take(natts) {
        if !a.attbyval && a.attlen != -1 && a.attlen <= 0 {
            return None; // cstring / unknown copy law
        }
        cols.push(::nodesort::fullsort::RunCol { byval: a.attbyval, len: a.attlen });
    }
    Some(FullSpec { keys, natts, cols })
}

// ---------------------------------------------------------------------------
// Shared state: the parallel context's private payload AND the sink body
// (one struct, one Arc — the runtime_scan/runtime_distinct discipline).
// ---------------------------------------------------------------------------

struct SendConstPstmt(*const PlannedStmt<'static>);
// SAFETY: read-only erased reference into the leader's executor arena; the
// leader keeps it alive until DestroyParallelContext has joined every helper
// (the execparallel SendConst contract, verbatim).
unsafe impl Send for SendConstPstmt {}
// SAFETY: as above; helpers only read.
unsafe impl Sync for SendConstPstmt {}

pub(super) struct RuntimeSortShared {
    rt: &'static Arc<runtime::Runtime>,
    rg: OnceLock<runtime::WeakRgHandle>,
    pcxt_shared: OnceLock<Arc<parallel::ParallelShared>>,
    pstmt: SendConstPstmt,
    query_text: String,
    eflags: i32,
    /// Worker-readable spec pod (plain data; Locals fork from `bound` +
    /// the key arity).
    keys: Vec<KeyCol>,
    bound: usize,
    /// Helpers whose binder validate() refused (before any claim).
    refused: AtomicUsize,
    /// Helpers that bound and entered the drive.
    started: AtomicUsize,
    /// Helpers that have EXITED `helper_drive` (every exit path — refused
    /// bind, errored, drove to completion, panic-unwind — bumps exactly
    /// once, by drop guard; the m35-spill inc-2c `ExitBump` pattern, ported
    /// here per the inc-2c FLAG). Liveness reap input: a pinned RG is
    /// invisible to pool workers, so once `exited >= launched` with the RG
    /// incomplete, nobody will ever step it — the leader must reap or park
    /// forever.
    exited: AtomicUsize,
    /// First worker-phase error (entry-phase errors ride the ordinary
    /// parallel message channel).
    error: Mutex<Option<Box<PgError>>>,
    failed: AtomicBool,
    /// A sink contract break (staged batch without a window ref): NOT an
    /// error — the RG aborts and the leader reruns the serial arm (R5).
    broke: AtomicBool,
    /// The published winner ROWREF list in emission order (combine writes
    /// — partitions()=1, single claimer; the leader takes after
    /// completion and gathers by rowref — the entry width is a worker
    /// detail).
    winners: Mutex<Option<Vec<u64>>>,
    /// GCUT (inc-2): the shared cross-worker cutoff in `nodesort::sink::
    /// cut64` space — monotone `fetch_min`, `u64::MAX` = unbounded. Seeded
    /// at engage time from the zone-max seed (the k-th smallest zone-max
    /// word: >= bound rows provably sit at-or-below it), tightened by every
    /// worker's full-heap floor. Prune/skip comparisons are STRICT `>`
    /// (see `cut64`'s safety doc). Dormant (never read) unless
    /// `runtime_sort_gcut_enabled()`.
    cutoff: AtomicU64,
    /// GCUT: per-granule BEST cut64 words (leader zone stats, absolute
    /// granule space, len == total_granules). A granule whose best word
    /// exceeds the current cutoff cannot contribute a winner — workers
    /// skip it before staging/decompression. `None` = no zone skip
    /// (dictcode leading key, non-exact encodings only, or GCUT off).
    zone_best: Option<Arc<Vec<u64>>>,
    /// GCUT: granules skipped pre-staging (engagement witness).
    zone_skipped: AtomicU64,
    /// Shape (b) FULL SORT (m3-sort-b car 2; design §5). `None` = the
    /// top-N arm (everything above).
    full: Option<FullShared>,
    /// M2 inc-1 standing channel: the live board entry, held for the
    /// PRIVATE_SHUTDOWN standing join (standing_channel, scan discipline).
    standing: Mutex<Option<Arc<parallel::standing::StandingEngagement>>>,
}

/// Shape-(b) full-sort payload half: per-worker self-contained run
/// buffers, splitter-sliced partition-parallel merge.
pub(super) struct FullShared {
    /// Output arity + per-column copy law (outer-desc census).
    natts: usize,
    cols: Vec<::nodesort::fullsort::RunCol>,
    /// Per-Local byte budget (work_mem per participant, design §7). A
    /// runtime crossing = refusal (recorded), RG abort, serial rerun.
    budget: usize,
    /// Partition-parallel merge width.
    parts: usize,
    /// Splitters, computed once by the first combine claimer from the
    /// sealed runs (they gate BALANCE only, never content).
    splitters: OnceLock<Vec<::nodesort::sink::WideEntry>>,
    /// Per-partition merged (run, bufrow) outputs; slot p is written only
    /// by partition p's combine claim (single writer, sink contract).
    out_parts: Vec<UnsafeCell<Vec<(u16, u32)>>>,
    /// A Local crossed the byte budget: not an error — refusal + serial
    /// rerun (R5), the design-§7 law.
    budget_refused: AtomicBool,
    /// finalize's published output: the sealed runs (Arc — O(W) publish,
    /// no row copies) + partition outputs in partition order.
    published: Mutex<Option<FullPublish>>,
}

pub(super) struct FullPublish {
    runs: Vec<Arc<::nodesort::fullsort::FullRun>>,
    parts: Vec<Vec<(u16, u32)>>,
}

// SAFETY: `out_parts` slots follow the single-writer-per-partition sink
// contract (each combine partition is claimed exactly once; finalize is
// the single reader after all claims settle) — the runtime_agg `AggSink`
// argument, verbatim.
unsafe impl Sync for FullShared {}

/// Per-worker Local: the narrow u128 heap at key arity 1 (the shipped
/// inc-2 path), the wide heap at arity 2..=TOPN_MAX_KEYS (inc-5), or the
/// shape-(b) full-sort run under construction (car 2).
pub(super) enum TopnLocal {
    Narrow(TopnHeap),
    Wide(TopnWideHeap),
    Full(FullLocal),
}

/// A full-sort run under construction: unsorted entries + the
/// self-contained row buffer (budget-metered).
pub(super) struct FullLocal {
    entries: Vec<::nodesort::fullsort::RunEnt>,
    buf: ::nodesort::fullsort::RunBuf,
}

impl FullLocal {
    fn bytes(&self) -> usize {
        self.buf.bytes()
            + self.entries.capacity() * core::mem::size_of::<::nodesort::fullsort::RunEnt>()
    }
}

/// The sealed (sorted) form of a Local. Variants never mix within one
/// engagement (fork decides once from the spec's key arity/mode).
pub(super) enum TopnSealed {
    Narrow(Vec<TopnEntry>),
    Wide(Vec<WideEntry>),
    /// Sealed full-sort run (sorted entries + fixed-up buf) — Arc so the
    /// finalize publish clones pointers, never row data.
    Full(Arc<::nodesort::fullsort::FullRun>),
}

impl RuntimeSortShared {
    fn fail(&self, e: Box<PgError>) {
        {
            let mut g = self.error.lock().unwrap_or_else(|p| p.into_inner());
            if g.is_none() {
                *g = Some(e);
            }
        }
        self.failed.store(true, Ordering::SeqCst);
        self.abort_rg();
    }

    fn break_contract(&self) {
        self.broke.store(true, Ordering::SeqCst);
        self.abort_rg();
    }

    fn abort_rg(&self) {
        if let Some(rg) = self.rg.get().and_then(|w| w.upgrade()) {
            rg.abort();
        }
    }

    fn take_error(&self) -> Option<Box<PgError>> {
        self.error.lock().unwrap_or_else(|p| p.into_inner()).take()
    }

    fn take_winners(&self) -> Option<Vec<u64>> {
        self.winners.lock().unwrap_or_else(|p| p.into_inner()).take()
    }
}

// ---------------------------------------------------------------------------
// The SealedParallelSink implementation. accept_local/seal/combine are
// INFALLIBLE BY CONTRACT: errors and panics are caught, recorded (first
// wins), and turn into an RG abort — the runtime never sees an unwind.
// ---------------------------------------------------------------------------

impl runtime::SealedParallelSink for RuntimeSortShared {
    type Local = TopnLocal;
    type Sealed = TopnSealed;

    fn fork(&self, _worker: usize) -> TopnLocal {
        if let Some(full) = &self.full {
            TopnLocal::Full(FullLocal {
                entries: Vec::new(),
                buf: ::nodesort::fullsort::RunBuf::new(full.natts),
            })
        } else if self.keys.len() == 1 {
            TopnLocal::Narrow(TopnHeap::new(self.bound))
        } else {
            TopnLocal::Wide(TopnWideHeap::new(self.bound))
        }
    }

    fn accept_local(&self, local: &mut TopnLocal, _worker: usize, range: runtime::MorselRange) {
        if self.failed.load(Ordering::SeqCst)
            || self.broke.load(Ordering::SeqCst)
            || self.full.as_ref().is_some_and(|f| f.budget_refused.load(Ordering::SeqCst))
        {
            return; // aborting: drain the claim without work
        }
        let r = catch_unwind(AssertUnwindSafe(|| self.morsel_body(local, range)));
        match r {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                mark_self_errored();
                self.fail(e);
            }
            Err(_panic) => {
                mark_self_errored();
                self.fail(PgError::new(ERROR, "runtime sort worker panicked in a morsel").into());
            }
        }
    }

    fn seal(&self, _worker: usize, local: TopnLocal) -> TopnSealed {
        if self.failed.load(Ordering::SeqCst) || self.broke.load(Ordering::SeqCst) {
            return TopnSealed::Narrow(Vec::new());
        }
        // POD sort — cannot unwind (RunBuf fixup is index arithmetic).
        match local {
            TopnLocal::Narrow(h) => TopnSealed::Narrow(h.into_sorted()),
            TopnLocal::Wide(h) => TopnSealed::Wide(h.into_sorted()),
            TopnLocal::Full(mut l) => {
                let full = self.full.as_ref().expect("full local under a full spec");
                l.entries.sort_unstable();
                l.buf.seal_fixup(&full.cols);
                TopnSealed::Full(Arc::new(::nodesort::fullsort::FullRun {
                    entries: l.entries,
                    buf: l.buf,
                }))
            }
        }
    }

    fn partitions(&self) -> u64 {
        match &self.full {
            Some(f) => f.parts as u64,
            None => 1,
        }
    }

    fn combine(&self, part: u64, sealed: &[TopnSealed]) {
        if self.failed.load(Ordering::SeqCst)
            || self.broke.load(Ordering::SeqCst)
            || self.full.as_ref().is_some_and(|f| f.budget_refused.load(Ordering::SeqCst))
        {
            return;
        }
        if let Some(full) = &self.full {
            // Shape (b): slice every sealed run to this partition's key
            // range and k-way merge (design §5). Splitters computed once
            // by the first claimer (balance only, never content).
            let runs: Vec<&[::nodesort::fullsort::RunEnt]> = sealed
                .iter()
                .map(|s| match s {
                    TopnSealed::Full(r) => r.entries.as_slice(),
                    // Aborted-path placeholder seals are Narrow(empty).
                    TopnSealed::Narrow(v) => {
                        debug_assert!(v.is_empty(), "mixed sealed variants in one sort sink");
                        &[]
                    }
                    TopnSealed::Wide(_) => {
                        unreachable!("mixed sealed variants in one sort sink")
                    }
                })
                .collect();
            let splitters = full
                .splitters
                .get_or_init(|| ::nodesort::fullsort::fullsort_splitters(&runs, full.parts));
            let out =
                ::nodesort::fullsort::fullsort_partition_merge(&runs, splitters, part as usize);
            // SAFETY: partition `part` is claimed exactly once (runtime
            // contract); this is its single writer.
            unsafe { *full.out_parts[part as usize].get() = out };
            return;
        }
        debug_assert_eq!(part, 0);
        // Variants never mix within one engagement (fork decides once);
        // aborted-path placeholder Narrow(empty) seals are harmless in
        // either collection (empty runs contribute nothing).
        let rowrefs: Vec<u64> = if self.keys.len() == 1 {
            let runs: Vec<Vec<TopnEntry>> = sealed
                .iter()
                .map(|s| match s {
                    TopnSealed::Narrow(v) => v.clone(),
                    TopnSealed::Wide(_) | TopnSealed::Full(_) => {
                        unreachable!("mixed sealed variants in one sort sink")
                    }
                })
                .collect();
            topn_merge(&runs, self.bound).iter().map(|e| e.rowref()).collect()
        } else {
            let runs: Vec<Vec<WideEntry>> = sealed
                .iter()
                .map(|s| match s {
                    TopnSealed::Wide(v) => v.clone(),
                    // Aborted seal placeholders are Narrow(empty).
                    TopnSealed::Narrow(v) => {
                        debug_assert!(v.is_empty(), "mixed sealed variants in one sort sink");
                        Vec::new()
                    }
                    TopnSealed::Full(_) => {
                        unreachable!("mixed sealed variants in one sort sink")
                    }
                })
                .collect();
            topn_merge(&runs, self.bound).iter().map(|e| e.rowref()).collect()
        };
        *self.winners.lock().unwrap_or_else(|p| p.into_inner()) = Some(rowrefs);
    }

    fn finalize(&self, sealed: &[TopnSealed]) {
        // Top-N: publish already happened in the (single) combine.
        // Full sort: collect the partition outputs + clone the run Arcs
        // (O(W + partitions) — never row data). Aborted RGs skip
        // finalize; the leader validates the published slot on the
        // Completed path (protocol-violation error, never silence).
        let Some(full) = &self.full else { return };
        if self.failed.load(Ordering::SeqCst)
            || self.broke.load(Ordering::SeqCst)
            || full.budget_refused.load(Ordering::SeqCst)
        {
            return;
        }
        // INDEX ALIGNMENT: partition outputs address runs by SEALED SLOT
        // index — non-Full placeholders map to an empty run (no entry ever
        // addresses one: its combine slice was empty).
        let runs: Vec<Arc<::nodesort::fullsort::FullRun>> = sealed
            .iter()
            .map(|s| match s {
                TopnSealed::Full(r) => Arc::clone(r),
                _ => Arc::new(::nodesort::fullsort::FullRun {
                    entries: Vec::new(),
                    buf: ::nodesort::fullsort::RunBuf::new(full.natts),
                }),
            })
            .collect();
        let parts: Vec<Vec<(u16, u32)>> = full
            .out_parts
            .iter()
            // SAFETY: all combine claims settled (last-worker-out);
            // finalize is the single reader.
            .map(|c| unsafe { std::mem::take(&mut *c.get()) })
            .collect();
        *full.published.lock().unwrap_or_else(|p| p.into_inner()) =
            Some(FullPublish { runs, parts });
    }
}

// ---------------------------------------------------------------------------
// Worker (helper) side: thread-local executor + the accept morsel body.
// ---------------------------------------------------------------------------

struct WorkerExec {
    qd: ::types_portal::QueryDescHandle,
    errored: std::cell::Cell<bool>,
}

thread_local! {
    static WORKER_EXEC: std::cell::RefCell<Option<WorkerExec>> =
        const { std::cell::RefCell::new(None) };
}

fn mark_self_errored() {
    WORKER_EXEC.with(|cell| {
        if let Some(ex) = cell.borrow().as_ref() {
            ex.errored.set(true);
        }
    });
}

/// The per-morsel accept feed: narrow (key, rowref) pushes into the
/// worker's bounded heap — the RefSortSink batch loop with the heap in
/// place of the narrow tuplesort (same two-leg key law: clean staged rows
/// read the SoA column; requal/fallback rows run the exact per-row emit —
/// C detoast semantics, C's errors on C's row — and read the projected
/// cell). `broke` = a staged batch without a window ref (or a per-row
/// arrival): the sink cannot carry rowrefs — contract break, RG abort,
/// serial rerun.
struct TopnAcceptSink<'a> {
    heap: &'a mut TopnLocal,
    keys: &'a [KeyCol],
    broke: bool,
    /// Any key is the DictCode class: the fast leg must also answer a
    /// stitched code lane per window, and NO row may take the per-row emit
    /// leg (a text datum has no order-preserving i64) — a window that
    /// cannot serve the fast leg, or any fallback-forced row, is a
    /// contract break (RG abort, serial rerun; nothing was emitted).
    dictcode: bool,
    /// Per-row multi-key scratch (avoids a per-row alloc).
    obs: [( i64, bool); ::nodesort::sink::TOPN_MAX_KEYS],
    flags: [(bool, bool); ::nodesort::sink::TOPN_MAX_KEYS],
    /// GCUT (inc-2): the payload's shared cutoff. Read once per staged
    /// batch and pruned/published against in the COLSTAGE tight loop only
    /// (`runtime_sort_gcut_enabled()` — dormant otherwise).
    cutoff: &'a AtomicU64,
}

impl<'a> TopnAcceptSink<'a> {
    fn new(
        heap: &'a mut TopnLocal,
        keys: &'a [KeyCol],
        cutoff: &'a AtomicU64,
    ) -> TopnAcceptSink<'a> {
        let mut flags = [(false, false); ::nodesort::sink::TOPN_MAX_KEYS];
        for (i, k) in keys.iter().enumerate() {
            flags[i] = (k.desc, k.nulls_first);
        }
        TopnAcceptSink {
            heap,
            keys,
            broke: false,
            dictcode: keys.iter().any(|k| k.dictcode),
            obs: [(0, false); ::nodesort::sink::TOPN_MAX_KEYS],
            flags,
            cutoff,
        }
    }

    /// Push the row whose per-key observations sit in `self.obs[..nkeys]`.
    #[inline]
    fn push_obs(&mut self, rg: u32, row: u32) {
        let rowref = ((rg as u64) << 32) | row as u64;
        let nk = self.keys.len();
        match self.heap {
            TopnLocal::Narrow(h) => {
                let (k, n) = self.obs[0];
                let key = &self.keys[0];
                h.push(TopnEntry::encode(k, n, key.desc, key.nulls_first, rowref));
            }
            TopnLocal::Wide(h) => {
                h.push(WideEntry::encode(&self.obs[..nk], &self.flags[..nk], rowref));
            }
            TopnLocal::Full(_) => unreachable!("full locals feed FullAcceptSink"),
        }
    }
}

impl<'mcx> Sink<'mcx> for TopnAcceptSink<'_> {
    fn accept(&mut self, _tuple: ExecSlotId, _estate: &mut EStateData<'mcx>) -> PgResult<SinkFeed> {
        // Row-granular arrival = no staged window ref to pair the row with.
        // Never reached from the seqscan drain (its operator overrides
        // consume_batch); defensive break.
        self.broke = true;
        Ok(SinkFeed::NeedMore)
    }

    fn finish(&mut self, _estate: &mut EStateData<'mcx>) -> PgResult<()> {
        Ok(())
    }
}

impl<'mcx> BatchSink<'mcx> for TopnAcceptSink<'_> {
    fn accept_batch<E: BatchEmit<'mcx>>(
        &mut self,
        emit: &mut E,
        pos: u32,
        n: u32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()> {
        if self.broke {
            return Ok(());
        }
        let Some((rg, row0)) = emit.window_ref() else {
            self.broke = true;
            return Ok(());
        };
        // Interrupt cadence floor: one check per staged batch (the fast
        // leg's rows have no per-row seam call; emit-path rows keep their
        // per-row check inside `emit`) — the RefSortSink cadence.
        ::postgres_seams::check_for_interrupts::call()?;
        // Fast leg availability: EVERY key column's staged lane must be
        // clean-readable (the single-key law applied per key). The
        // fallback masks are OR-united across keys (a row forced to the
        // per-row path for ANY key takes it for all — one emit, all keys
        // read from the projected slot). The sel bitmap is per-batch
        // (whole-qual verdict), identical across columns.
        let nk = self.keys.len();
        let fast = {
            let mut fb = [0u64; ::exectuples::SOA_BM_WORDS];
            let mut selw: Option<[u64; ::exectuples::SOA_BM_WORDS]> = None;
            let mut dlanes: [Option<::exectuples::SoaDictLane>;
                ::nodesort::sink::TOPN_MAX_KEYS] = [None; ::nodesort::sink::TOPN_MAX_KEYS];
            let mut ok = true;
            for (ki, key) in self.keys.iter().enumerate() {
                if key.dictcode {
                    // DictCode key: observations come from the stitched
                    // code lane of the window — NEVER from SoA datum cells
                    // (a text column may sit past the fixed-width deform's
                    // coverage; the code lane reads the scan's own staged
                    // window). Raw windows and unstitched parts cannot
                    // serve order via codes (dict-code-flow.md §2.2 Law D).
                    match emit.refsort_dictcode_batch(key.attno_scan) {
                        Some(l) if l.table.has_stitch() => dlanes[ki] = Some(l),
                        _ => {
                            ok = false;
                            break;
                        }
                    }
                    continue;
                }
                // Int-family key: the SoA datum cells must be certified
                // clean-readable (the incumbent law, per key).
                if emit.refsort_key_batch(key.attno_scan, n).is_none() {
                    ok = false;
                    break;
                }
            }
            // Batch masks (whole-qual sel verdict + forced-fallback), once
            // per batch — column-independent (a dictcode-only spec never
            // calls the key-batch accessor).
            if ok {
                match emit.refsort_batch_masks(n) {
                    Some((fallback, sel)) => {
                        for (w, &word) in fallback.iter().enumerate() {
                            fb[w] |= word;
                        }
                        selw = sel.map(|s| {
                            let mut w = [0u64; ::exectuples::SOA_BM_WORDS];
                            w[..s.len()].copy_from_slice(s);
                            w
                        });
                    }
                    None => ok = false,
                }
            }
            ok.then_some((fb, selw, dlanes))
        };
        // DictCode specs have no per-row emit leg: a window that cannot
        // serve the whole fast leg is a sink contract break — the RG
        // aborts and the leader reruns the serial arm (R5; nothing was
        // emitted).
        if self.dictcode && fast.is_none() {
            self.broke = true;
            return Ok(());
        }
        let max_resno = self.keys.iter().map(|k| k.resno_outer).max().unwrap_or(0);
        // Composed skip mask, word-skipped by `for_each_live`: the fast
        // leg's exact qual-cleared rows and the feed's qual-survivor
        // snapshot (`live_sel` — cleared bits answer `emit` with None, and
        // cover the no-fast/requal legs) each produce nothing, so skipping
        // them keeps the surviving observation stream identical.
        let skip = {
            let mut skip: Option<[u64; ::exectuples::SOA_BM_WORDS]> = None;
            let selw = fast.as_ref().and_then(|(_, s, _)| *s);
            for m in [emit.live_sel(), selw].into_iter().flatten() {
                match &mut skip {
                    None => skip = Some(m),
                    Some(acc) => {
                        for (a, b) in acc.iter_mut().zip(m.iter()) {
                            *a &= *b;
                        }
                    }
                }
            }
            skip
        };
        // COLSTAGE follow-ups (night/sort-merge-redesign, same kill
        // switch): when NO row of the staged batch is fallback-forced,
        // run the emit-free TIGHT loop — the per-key batch views hoisted
        // out of the per-row walk (the per-row `refsort_key_batch`
        // re-borrow was the largest remaining accept cost after the
        // staging fix) and the floor prefilter (`admits`) applied before
        // the heap-push ceremony. Observation-stream identity: the loop
        // visits the same live rows in the same order, encodes the same
        // entries under the same total order, and a non-admitted push was
        // already a heap no-op (`BoundedTopnHeap::push` compares against
        // the same floor), so the winner set is bit-identical. Any
        // fallback bit in the batch keeps the incumbent per-row walk
        // below, whole-batch (fail closed, rare on staged columnar
        // windows).
        if runtime_sort_colstage_enabled() {
            if let Some((fb, _, dlanes)) = &fast {
                let nwords = ((n as usize) + 63) / 64;
                let fallback_free = fb[..nwords.min(fb.len())]
                    .iter()
                    .enumerate()
                    .all(|(w, &word)| {
                        // Mask off bits at and past `n` in the last word.
                        let hi = (n as usize).saturating_sub(w * 64).min(64);
                        let mask = if hi >= 64 { !0u64 } else { (1u64 << hi) - 1 };
                        word & mask == 0
                    });
                if fallback_free {
                    // Hoist per-key batch views once (batch-stable — the
                    // fast-leg availability check above proved each key
                    // serves this window).
                    let mut kv: [Option<(&[::datum::Datum], &[bool])>;
                        ::nodesort::sink::TOPN_MAX_KEYS] =
                        [None; ::nodesort::sink::TOPN_MAX_KEYS];
                    for (ki, key) in self.keys.iter().enumerate() {
                        if !key.dictcode {
                            let (vals, nulls, _, _) = emit
                                .refsort_key_batch(key.attno_scan, n)
                                .expect("refsort key batch stable within a staged batch");
                            kv[ki] = Some((vals, nulls));
                        }
                    }
                    let keys = self.keys;
                    let flags = &self.flags;
                    let obs = &mut self.obs;
                    // GCUT (inc-2): shared-cutoff prune + floor publication
                    // (see `cut64`'s safety doc — strict `>` prune only,
                    // publication is a monotone fetch_min of the local
                    // full-heap floor). OFF ⇒ `cut = u64::MAX`: the prune
                    // compare is statically false and no publication runs —
                    // the COLSTAGE loop stays exactly the inc-1 shape.
                    let gcut = runtime_sort_gcut_enabled();
                    let cutoff = self.cutoff;
                    let mut cut =
                        if gcut { cutoff.load(Ordering::Relaxed) } else { u64::MAX };
                    match &mut *self.heap {
                        TopnLocal::Narrow(h) => {
                            let key = keys[0];
                            ::exectuples::for_each_live(
                                skip.as_ref().map(|w| &w[..]),
                                pos,
                                n,
                                |i| -> PgResult<()> {
                                    let rowref = ((rg as u64) << 32) | (row0 + i) as u64;
                                    let e = if key.dictcode {
                                        let lane = dlanes[0].expect(
                                            "dictcode lane present under an engaged fast leg",
                                        );
                                        let g = lane.table.global_code(lane.code(i as usize));
                                        TopnEntry::encode(
                                            g as i64,
                                            false,
                                            key.desc,
                                            key.nulls_first,
                                            rowref,
                                        )
                                    } else {
                                        let (vals, nulls) =
                                            kv[0].expect("int key view hoisted");
                                        TopnEntry::encode(
                                            key_i64(vals[i as usize], key.width),
                                            nulls[i as usize],
                                            key.desc,
                                            key.nulls_first,
                                            rowref,
                                        )
                                    };
                                    if e.cut64() > cut {
                                        return Ok(());
                                    }
                                    if h.admits(e) {
                                        h.push(e);
                                        if gcut {
                                            if let Some(f) = h.floor() {
                                                let c = f.cut64();
                                                if c < cut {
                                                    cut = c;
                                                    cutoff
                                                        .fetch_min(c, Ordering::Relaxed);
                                                }
                                            }
                                        }
                                    }
                                    Ok(())
                                },
                            )?;
                            return Ok(());
                        }
                        TopnLocal::Wide(h) => {
                            ::exectuples::for_each_live(
                                skip.as_ref().map(|w| &w[..]),
                                pos,
                                n,
                                |i| -> PgResult<()> {
                                    for (ki, key) in keys.iter().enumerate() {
                                        if key.dictcode {
                                            let lane = dlanes[ki].expect(
                                                "dictcode lane present under an engaged fast leg",
                                            );
                                            obs[ki] = (
                                                lane.table.global_code(lane.code(i as usize))
                                                    as i64,
                                                false,
                                            );
                                        } else {
                                            let (vals, nulls) =
                                                kv[ki].expect("int key view hoisted");
                                            obs[ki] = (
                                                key_i64(vals[i as usize], key.width),
                                                nulls[i as usize],
                                            );
                                        }
                                    }
                                    let rowref = ((rg as u64) << 32) | (row0 + i) as u64;
                                    let e =
                                        WideEntry::encode(&obs[..nk], &flags[..nk], rowref);
                                    if e.cut64() > cut {
                                        return Ok(());
                                    }
                                    if h.admits(e) {
                                        h.push(e);
                                        if gcut {
                                            if let Some(f) = h.floor() {
                                                let c = f.cut64();
                                                if c < cut {
                                                    cut = c;
                                                    cutoff
                                                        .fetch_min(c, Ordering::Relaxed);
                                                }
                                            }
                                        }
                                    }
                                    Ok(())
                                },
                            )?;
                            return Ok(());
                        }
                        TopnLocal::Full(_) => {
                            unreachable!("full locals feed FullAcceptSink")
                        }
                    }
                }
            }
        }
        ::exectuples::for_each_live(skip.as_ref().map(|w| &w[..]), pos, n, |i| -> PgResult<()> {
            // Contract-break rows are dead (the RG aborts and the serial
            // arm reruns): keep dict-code-flow's whole-batch early return
            // under the closure walk — no emit runs after `broke`.
            if self.broke {
                return Ok(());
            }
            if let Some((fb, _, dlanes)) = &fast {
                let w = (i / 64) as usize;
                let bit = 1u64 << (i % 64);
                if fb[w] & bit == 0 {
                    // Clean staged row: every key straight from its SoA
                    // column (re-borrowed per key — batch-stable); DictCode
                    // keys read the window's code and map it part-global
                    // (dict windows carry no NULLs — pgrcolumnar stores none).
                    for ki in 0..nk {
                        let key = self.keys[ki];
                        if key.dictcode {
                            let lane = dlanes[ki]
                                .expect("dictcode lane present under an engaged fast leg");
                            let g = lane.table.global_code(lane.code(i as usize));
                            self.obs[ki] = (g as i64, false);
                            continue;
                        }
                        let (kvals, knulls, _, _) = emit
                            .refsort_key_batch(key.attno_scan, n)
                            .expect("refsort key batch stable within a staged batch");
                        let (d, isnull) = (kvals[i as usize], knulls[i as usize]);
                        self.obs[ki] = (key_i64(d, key.width), isnull);
                    }
                    self.push_obs(rg, row0 + i);
                    return Ok(());
                }
                // Forced-fallback row: exact per-row emit below — which a
                // DictCode spec cannot serve (contract break, R5 rerun).
                if self.dictcode {
                    self.broke = true;
                    return Ok(());
                }
            }
            let Some(id) = emit.emit(i, estate)? else { return Ok(()) };
            {
                let slot = estate.slot_mut(id);
                ::exectuples::slot_getsomeattrs(slot, max_resno as i32 + 1);
                let base = slot.base();
                for ki in 0..nk {
                    let key = self.keys[ki];
                    let (d, isnull) =
                        (base.tts_values[key.resno_outer], base.tts_isnull[key.resno_outer]);
                    self.obs[ki] = (key_i64(d, key.width), isnull);
                }
            }
            self.push_obs(rg, row0 + i);
            Ok(())
        })
    }
}

/// Shape-(b) full-sort accept feed: EVERY surviving row runs the exact
/// per-row emit (C qual/detoast semantics, C's errors on C's row; the
/// staged-lane fast leg is a chartered perf lever, not phase 1), then the
/// outer-format slot row is copied SELF-CONTAINED into the run buffer and
/// its (keys, global rowref) entry stamped. `broke` = a staged batch
/// without a window ref (no rowref ⇒ no canonical tie order): contract
/// break, RG abort, serial rerun. `budget_broke` = the Local crossed the
/// work_mem-per-participant byte budget: refusal, RG abort, serial rerun
/// (design §7 — the serial arm then spills correctly).
struct FullAcceptSink<'a> {
    local: &'a mut FullLocal,
    keys: &'a [KeyCol],
    cols: &'a [::nodesort::fullsort::RunCol],
    natts: usize,
    budget: usize,
    broke: bool,
    budget_broke: bool,
    obs: [(i64, bool); ::nodesort::sink::TOPN_MAX_KEYS],
    flags: [(bool, bool); ::nodesort::sink::TOPN_MAX_KEYS],
}

impl<'a> FullAcceptSink<'a> {
    fn new(
        local: &'a mut FullLocal,
        keys: &'a [KeyCol],
        full: &'a FullShared,
    ) -> FullAcceptSink<'a> {
        let mut flags = [(false, false); ::nodesort::sink::TOPN_MAX_KEYS];
        for (i, k) in keys.iter().enumerate() {
            flags[i] = (k.desc, k.nulls_first);
        }
        FullAcceptSink {
            local,
            keys,
            cols: &full.cols,
            natts: full.natts,
            budget: full.budget,
            broke: false,
            budget_broke: false,
            obs: [(0, false); ::nodesort::sink::TOPN_MAX_KEYS],
            flags,
        }
    }
}

impl<'mcx> Sink<'mcx> for FullAcceptSink<'_> {
    fn accept(&mut self, _tuple: ExecSlotId, _estate: &mut EStateData<'mcx>) -> PgResult<SinkFeed> {
        // Row-granular arrival = no staged window ref (defensive break, as
        // the top-N sink).
        self.broke = true;
        Ok(SinkFeed::NeedMore)
    }

    fn finish(&mut self, _estate: &mut EStateData<'mcx>) -> PgResult<()> {
        Ok(())
    }
}

impl<'mcx> BatchSink<'mcx> for FullAcceptSink<'_> {
    fn accept_batch<E: BatchEmit<'mcx>>(
        &mut self,
        emit: &mut E,
        pos: u32,
        n: u32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()> {
        if self.broke || self.budget_broke {
            return Ok(());
        }
        let Some((rg, row0)) = emit.window_ref() else {
            self.broke = true;
            return Ok(());
        };
        ::postgres_seams::check_for_interrupts::call()?;
        let nk = self.keys.len();
        // Emit-dead word skip (`live_sel`): a cleared bit answers `emit`
        // with None and no observable effect — same surviving rows, same
        // run-buffer order.
        let live = emit.live_sel();
        ::exectuples::for_each_live(live.as_ref().map(|w| &w[..]), pos, n, |i| -> PgResult<()> {
            // The per-row emit leg: qual verdict + projection + detoast —
            // C semantics, C's errors on C's row. None = qual-filtered.
            let Some(id) = emit.emit(i, estate)? else { return Ok(()) };
            let bufrow = self.local.buf.nrows as u32;
            {
                let slot = estate.slot_mut(id);
                ::exectuples::slot_getsomeattrs(slot, self.natts as i32);
                let base = slot.base();
                for ki in 0..nk {
                    let key = self.keys[ki];
                    let (d, isnull) =
                        (base.tts_values[key.resno_outer], base.tts_isnull[key.resno_outer]);
                    self.obs[ki] = (key_i64(d, key.width), isnull);
                }
                // SAFETY: outer-format slot cells are live, fully-detoasted
                // datums under the admitted column census (byval / fixed-len
                // byref / plain varlena — cstring never admits).
                unsafe {
                    self.local.buf.push_row(
                        &base.tts_values[..self.natts],
                        &base.tts_isnull[..self.natts],
                        self.cols,
                    )
                };
            }
            let rowref = ((rg as u64) << 32) | (row0 + i) as u64;
            self.local.entries.push(::nodesort::fullsort::RunEnt {
                key: ::nodesort::sink::WideEntry::encode(
                    &self.obs[..nk],
                    &self.flags[..nk],
                    rowref,
                ),
                bufrow,
            });
            Ok(())
        })?;
        if self.local.bytes() > self.budget {
            self.budget_broke = true;
        }
        Ok(())
    }
}

impl RuntimeSortShared {
    fn morsel_body(&self, local: &mut TopnLocal, range: runtime::MorselRange) -> PgResult<()> {
        WORKER_EXEC.with(|cell| {
            let b = cell.borrow();
            let Some(ex) = b.as_ref() else {
                return Err(Box::new(PgError::new(
                    ERROR,
                    "runtime sort morsel without a bound executor",
                )));
            };
            crate::querydesc::with_qd(ex.qd, |q| {
                let x = q.exec.as_mut().expect("runtime sort worker executor state");
                x.with_mut(|d| -> PgResult<()> {
                    let estate = &mut d.estate;
                    let ss = sort_worker_scan(d.planstate.as_mut())?;
                    // train-12 composition: AM-dispatched positioner (heap
                    // lane rename); this arm admits only pgrcolumnar scans by
                    // construction.
                    let (broke, budget_broke) = match local {
                        TopnLocal::Full(l) => {
                            ::nodeseqscan::seq_scan_set_morsel_range(
                                ss,
                                estate,
                                range.start,
                                range.end,
                            )?;
                            let full =
                                self.full.as_ref().expect("full local under a full spec");
                            let mut sink = FullAcceptSink::new(l, &self.keys, full);
                            let fed = drain_pipeline(
                                ss,
                                &mut SeqScanSource,
                                &mut SeqScanFilterProject,
                                &mut sink,
                                estate,
                            );
                            let flags = (sink.broke, sink.budget_broke);
                            fed?;
                            flags
                        }
                        local => {
                            // GCUT (inc-2): segment the claim at zone-skipped
                            // granules — a granule whose BEST cut64 word
                            // exceeds the current shared cutoff cannot
                            // contribute a winner (strict `>`, the cut64
                            // safety law), so it is skipped BEFORE staging /
                            // decompression. Consecutive survivors drain as
                            // one range (a skip-free claim = exactly the
                            // incumbent single-range shape); the cutoff is
                            // re-read between segments (it only tightens).
                            // Out-of-range indices answer 0 = never skip
                            // (defensive; engage pinned len == geometry).
                            let zone = self.zone_best.as_deref();
                            let mut broke = false;
                            let mut skipped = 0u64;
                            let mut g = range.start;
                            while g < range.end && !broke {
                                let cut = self.cutoff.load(Ordering::Relaxed);
                                if let Some(best) = zone {
                                    while g < range.end
                                        && best.get(g as usize).copied().unwrap_or(0) > cut
                                    {
                                        g += 1;
                                        skipped += 1;
                                    }
                                    if g >= range.end {
                                        break;
                                    }
                                }
                                let s0 = g;
                                g = match zone {
                                    Some(best) => {
                                        let mut e = g;
                                        while e < range.end
                                            && best.get(e as usize).copied().unwrap_or(0)
                                                <= cut
                                        {
                                            e += 1;
                                        }
                                        e
                                    }
                                    None => range.end,
                                };
                                ::nodeseqscan::seq_scan_set_morsel_range(ss, estate, s0, g)?;
                                let mut sink = TopnAcceptSink::new(
                                    &mut *local,
                                    &self.keys,
                                    &self.cutoff,
                                );
                                let fed = drain_pipeline(
                                    ss,
                                    &mut SeqScanSource,
                                    &mut SeqScanFilterProject,
                                    &mut sink,
                                    estate,
                                );
                                broke = sink.broke;
                                fed?;
                            }
                            if skipped > 0 {
                                self.zone_skipped.fetch_add(skipped, Ordering::Relaxed);
                            }
                            (broke, false)
                        }
                    };
                    if budget_broke {
                        trace_feed("runtime sort worker budget crossing; refusing to serial");
                        if let Some(full) = &self.full {
                            full.budget_refused.store(true, Ordering::SeqCst);
                        }
                        self.abort_rg();
                    } else if broke {
                        trace_feed("runtime sort worker contract break; aborting to serial fallback");
                        self.break_contract();
                    }
                    Ok(())
                })
            })
        })
    }
}

/// The worker plan tree is the SCAN SUBTREE alone (workers never run the
/// Sort — accept_local drives scan → PREWHERE → narrow pushes; the worker
/// pstmt's planTree is the SeqScan node).
fn sort_worker_scan<'a, 'mcx>(
    planstate: Option<&'a mut crate::procnode::PlanStateNode<'mcx>>,
) -> PgResult<&'a mut ::nodeseqscan::SeqScanState<'mcx>> {
    let Some(crate::procnode::PlanStateNode::SeqScan(ss)) = planstate else {
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime sort worker plan is not a SeqScan root",
        )));
    };
    Ok(ss)
}

// ---------------------------------------------------------------------------
// Helper entry + POST_TASK_PARK drive (the shared ceremony; the hook
// registries are multi-registrant and every hook no-ops on foreign
// payloads).
// ---------------------------------------------------------------------------

fn runtime_sort_worker_main(_shared: &parallel::ParallelShared) -> PgResult<()> {
    Ok(())
}

fn runtime_sort_post_task_park(shared: &parallel::ParallelShared) {
    let Some(private) = shared.private() else { return };
    let Ok(payload) = private.downcast::<RuntimeSortShared>() else { return };
    // Every LAUNCHED helper bumps `exited` exactly once, on EVERY exit path
    // (the leader's liveness reap counts these against `launched`;
    // m35-spill inc-2c port). HOOK-frame placement (the scan arm's law):
    // the standing driver reuses helper_drive and must NOT bump — standing
    // exits ride the board's claimed/detached accounting.
    let _exit = super::runtime_agg::ExitBump(&payload.exited);
    let r = catch_unwind(AssertUnwindSafe(|| helper_drive(shared, &payload)));
    if r.is_err() {
        payload.fail(PgError::new(ERROR, "runtime sort helper panicked").into());
    }
    latch::SetLatch(::types_storage::latch::LatchHandle::proc(
        shared.parallel_leader_proc_number,
    ));
}

/// The standing driver (M2 inc-1, parallel::set_standing_driver): the
/// POST_TASK_PARK body minus the ExitBump; exit-committed unwinds (FATAL)
/// rethrow to the gang glue (a terminated worker must die).
fn runtime_sort_standing_driver(shared: &parallel::ParallelShared) {
    let Some(private) = shared.private() else { return };
    let Ok(payload) = private.downcast::<RuntimeSortShared>() else { return };
    let r = catch_unwind(AssertUnwindSafe(|| helper_drive(shared, &payload)));
    if let Err(unwind) = r {
        payload.fail(PgError::new(ERROR, "runtime sort standing executor panicked").into());
        latch::SetLatch(::types_storage::latch::LatchHandle::proc(
            shared.parallel_leader_proc_number,
        ));
        if parallel::standing::is_exit_unwind(&*unwind) {
            std::panic::resume_unwind(unwind);
        }
        return;
    }
    latch::SetLatch(::types_storage::latch::LatchHandle::proc(
        shared.parallel_leader_proc_number,
    ));
}

fn helper_drive(shared: &parallel::ParallelShared, payload: &Arc<RuntimeSortShared>) {
    let _ = shared;
    // Liveness-battery injection (test-only, default-off): the wedge-class
    // exit — panic before binding or driving; the reap must convert it into
    // a prompt error (scripts/runtime-liveness-e2e.sh).
    super::test_helper_panic("sort");
    // F1 fail-closed accounting: a helper that cannot participate must NEVER
    // vanish silently — every early exit below counts itself as a refusal
    // (the leader's started==0 && refused>=launched probe is its fallback
    // signal) and traces why.
    let Some(target) = payload.pcxt_shared.get() else {
        lane_trace("runtime-sort: helper refused (no pcxt shared)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let Some(rg) = payload.rg.get().and_then(|w| w.upgrade()) else {
        lane_trace("runtime-sort: helper refused (rg gone)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let Some(lane) = payload.rt.acquire_external_lane() else {
        lane_trace("runtime-sort: helper refused (no external lane)");
        payload.refused.fetch_add(1, Ordering::SeqCst);
        return;
    };
    let mut local = lane.local();
    let entered = std::cell::Cell::new(false);
    let bound = parallel::with_query_task_binding(target, || {
        entered.set(true);
        payload.started.fetch_add(1, Ordering::SeqCst);
        drive_bound(payload, &mut local, &rg)
    });
    match bound {
        Ok(()) => {}
        Err(e) => {
            if entered.get() {
                payload.fail(e);
                // F1 liveness (the agg-arm wedge mechanism, closed here
                // too): a helper that errored BEFORE joining the drive
                // (build_worker_exec failure) has aborted the RG via
                // fail() — but an aborted PINNED RG still needs a driver to
                // run invalidate/finalize/complete, or the leader parks on
                // its recheck cadence until the reap. Drive the closed
                // generation to completion here (pure protocol cleanup,
                // the drain_rg discipline); post-drive errors find it
                // already complete and skip.
                if rg.try_outcome().is_none() {
                    rg.abort();
                    let _ = payload.rt.drive_pinned(&mut local, &rg);
                }
            } else {
                lane_trace(&format!("runtime-sort: helper bind refused: {}", e.message()));
                payload.refused.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
}

fn drive_bound(
    payload: &Arc<RuntimeSortShared>,
    local: &mut runtime::WorkerLocal,
    rg: &runtime::RgHandle,
) -> PgResult<()> {
    build_worker_exec(payload)?;
    let _outcome = payload.rt.drive_pinned(local, rg);
    let self_errored =
        WORKER_EXEC.with(|cell| cell.borrow().as_ref().is_some_and(|ex| ex.errored.get()));
    teardown_worker_exec(!self_errored)
}

fn build_worker_exec(payload: &Arc<RuntimeSortShared>) -> PgResult<()> {
    WORKER_EXEC.with(|cell| -> PgResult<()> {
        if let Some(stale) = cell.borrow_mut().take() {
            crate::querydesc::release_query_desc_seam(stale.qd);
        }
        // SAFETY: leader-arena pstmt, alive until DestroyParallelContext
        // joins this helper (SendConst contract).
        let pstmt: &PlannedStmt<'_> = unsafe { &*payload.pstmt.0 };
        let qd = crate::querydesc::create_query_desc_seam(
            pstmt,
            &payload.query_text,
            Some(::snapmgr::GetActiveSnapshot()),
            None,
            ::types_dest::CommandDest::None,
            ::types_portal::ParamListHandle::NULL,
            ::types_portal::QueryEnvHandle::NULL,
            0,
        )?;
        let armed = (|| -> PgResult<()> {
            crate::execmain::executor_start_seam(qd, payload.eflags)?;
            crate::querydesc::with_qd(qd, |q| {
                let x = q.exec.as_mut().expect("runtime sort worker ExecutorStart");
                x.with_mut(|d| -> PgResult<()> {
                    let estate = &mut d.estate;
                    let ss = sort_worker_scan(d.planstate.as_mut())?;
                    // Key-only staged accept (inc-4 lever 1): the worker
                    // emits nothing but (keys, rowref) — narrow the scan's
                    // needed set to qual columns ∪ the sort keys so staging
                    // never decompresses the payload width (q24 take-1
                    // profile: 77.5% decompress_frame_into of columns
                    // nothing read; the LEADER's winner gather runs under
                    // its own FULL needed set). Before staging arms, so the
                    // deform plans bake the narrowed set. Unneeded
                    // per-row-emit cells read NULL and never escape (the
                    // sink reads the key cells only).
                    // FULL SORT (car 2): workers copy WHOLE output rows into
                    // their run buffers — the needed set stays full (no
                    // narrowing; the top-N late-mat contract does not apply).
                    if payload.full.is_none() {
                        let key_attnos: Vec<u16> =
                            payload.keys.iter().map(|k| k.attno_scan).collect();
                        ::nodeseqscan::seq_scan_cb_narrow_needed(ss, &key_attnos);
                        // DictCode specs (dict-code-flow inc-1): the accept
                        // FAST LEG is mandatory — no per-row emit can observe
                        // a code — so staged coverage must be forced BEFORE
                        // the generic arm: (a) with a qual, the PREWHERE
                        // prefix covers only the qual's columns (leg-7 v3:
                        // q5/q6 int keys past it broke every window) — widen
                        // it to the int-family key columns (dict keys read
                        // the code side channel, never datum cells, and stay
                        // OUT of the ask: a varlena in the fixed-width ask
                        // forces the virtual-prefix detour for nothing);
                        // (b) with NO qual, nothing arms at all and the
                        // drain delivers row-granular arrivals (leg-7 v3:
                        // q2/q4) — arm the offset-free columnar staging
                        // (batched windows + window refs; the masks
                        // accessor's no-qual arm serves all-survive).
                        // Refusal on either path leaves the batch unarmed
                        // and the sink contract-breaks to the serial arm,
                        // exactly the fail-closed ladder.
                        if payload.keys.iter().any(|k| k.dictcode) {
                            let int_ask = payload
                                .keys
                                .iter()
                                .filter(|k| !k.dictcode)
                                .map(|k| k.attno_scan as i32 + 1)
                                .max()
                                .unwrap_or(0);
                            if ss.ss.qual.is_some() {
                                let _ = ::nodeseqscan::seq_scan_cb_prewhere_arm(
                                    ss, estate, int_ask,
                                )?;
                            } else {
                                let _ = ::nodeseqscan::seq_scan_cb_columnar_arm(
                                    ss,
                                    estate,
                                    int_ask.max(1),
                                    None,
                                );
                            }
                        } else if runtime_sort_colstage_enabled() {
                            // COLSTAGE (night/sort-merge-redesign spike;
                            // kill-switch, DEFAULT OFF): the INT-FAMILY
                            // top-N accept has NO staged fast leg on
                            // qual-less scans — `arm_seq_scan_qual_bitmap`
                            // arms nothing without a qual (the exact hole
                            // the DictCode branch above documents), so
                            // `refsort_key_batch` refuses and EVERY row
                            // takes the per-row emit ceremony (projection
                            // program + per-row columnar datum decode +
                            // slot ceremony + per-row CFI) — profiled at
                            // ~75% of worker accept time on the
                            // zone-hostile rand-key top-N fixture (the
                            // GL-SORTECON-1 4.28x class). Arm the same
                            // offset-free columnar staging over the key
                            // prefix (no qual), or widen the PREWHERE
                            // prefix to the key columns (qual) — the
                            // sink's EXISTING fast leg then serves keys
                            // straight from the staged SoA lanes; rows the
                            // masks force to fallback keep the exact
                            // per-row emit. Staging availability never
                            // changes the observation stream (the fast-leg
                            // soundness contract the qual/dictcode arms
                            // already ride), so parity is unchanged.
                            let int_ask = payload
                                .keys
                                .iter()
                                .map(|k| k.attno_scan as i32 + 1)
                                .max()
                                .unwrap_or(0);
                            let armed = if ss.ss.qual.is_some() {
                                ::nodeseqscan::seq_scan_cb_prewhere_arm(
                                    ss, estate, int_ask,
                                )?
                            } else {
                                ::nodeseqscan::seq_scan_cb_columnar_arm(
                                    ss,
                                    estate,
                                    int_ask.max(1),
                                    None,
                                )
                            };
                            if armed {
                                lane_trace(
                                    "runtime-sort: colstage armed (staged int-key accept)",
                                );
                            }
                        }
                    }
                    super::arm_scan_staging(
                        ss,
                        estate,
                        super::ScanFeedShape::RowFeed {
                            ctx: "runtime sort worker feed",
                            stitch: true,
                        },
                    )
                })
            })
        })();
        match armed {
            Ok(()) => {
                *cell.borrow_mut() =
                    Some(WorkerExec { qd, errored: std::cell::Cell::new(false) });
                Ok(())
            }
            Err(e) => {
                crate::querydesc::release_query_desc_seam(qd);
                Err(e)
            }
        }
    })
}

fn teardown_worker_exec(clean: bool) -> PgResult<()> {
    WORKER_EXEC.with(|cell| -> PgResult<()> {
        let Some(ex) = cell.borrow_mut().take() else { return Ok(()) };
        if clean {
            let r = crate::execmain::executor_finish_seam(ex.qd)
                .and_then(|()| crate::execmain::executor_end_seam(ex.qd));
            match r {
                Ok(()) => {
                    crate::querydesc::free_query_desc_seam(ex.qd);
                    Ok(())
                }
                Err(e) => {
                    crate::querydesc::release_query_desc_seam(ex.qd);
                    Err(e)
                }
            }
        } else {
            crate::querydesc::release_query_desc_seam(ex.qd);
            Ok(())
        }
    })
}

fn runtime_sort_private_shutdown(private: &(dyn std::any::Any + Send + Sync)) {
    let Some(payload) = private.downcast_ref::<RuntimeSortShared>() else { return };
    let rg = payload.rg.get().and_then(|w| w.upgrade());
    if let Some(rg) = &rg {
        rg.abort();
    }
    // Standing channel (M2 inc-1): complete the standing join on leader
    // unwind paths (standing_channel::shutdown_standing_join).
    super::standing_channel::shutdown_standing_join(&payload.standing, rg.as_ref(), &|rg| {
        drain_rg(payload.rt, rg)
    });
}

fn ensure_hooks_registered() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        parallel::register_parallel_worker_entrypoint(
            "pgrust_runtime_sort_main",
            runtime_sort_worker_main,
        );
        parallel::register_parallel_post_task_park(runtime_sort_post_task_park);
        parallel::register_parallel_private_shutdown(runtime_sort_private_shutdown);
    });
}

/// `PGRUST_RUNTIME_SORT` arm kill switch (default ON when the runtime is
/// armed; the runtime itself defaults OFF).
fn runtime_sort_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_RUNTIME_SORT").as_deref(), Ok("0") | Ok("off"))
    })
}

// ---------------------------------------------------------------------------
// Leader-side engagement.
// ---------------------------------------------------------------------------

/// Refusal diagnosis trace (PGRUST_LANE_V2_TRACE only; emitted only once the
/// arm is ARMED — dop set + runtime on — so unarmed sessions stay silent).
/// M5-1: every sort-arm refusal also feeds the router's consolidated
/// taxonomy (static vocabulary — the callers all pass literals).
#[cold]
fn refused(reason: &'static str) {
    router::tick_refused(ArmClass::Sort, reason);
    lane_trace(&format!("runtime-sort: refused ({reason})"));
}

/// `PGRUST_RUNTIME_SORT_FULL` kill switch for the shape-(b) full-sort arm
/// (default ON; layered under the arm-wide `PGRUST_RUNTIME_SORT`).
fn runtime_sort_full_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_RUNTIME_SORT_FULL").as_deref(), Ok("0") | Ok("off"))
    })
}

/// COLSTAGE kill switch (night/sort-merge-redesign): arm the staged
/// columnar accept fast leg for INT-FAMILY top-N specs (the DictCode leg's
/// no-qual staging arm, extended to the int-key vocabulary).
/// DEFAULT ON since the GL-SORTECON-3 flip increment (flipped-kill idiom):
/// `PGRUST_RUNTIME_SORT_COLSTAGE=0|off` restores the incumbent per-row
/// emit accept byte-identically. Parity evidence: OFF/ON md5-identical on
/// every zonetopn/ladder leg, local + fleet jobs 61f2/3efc/5509 @
/// 0296033fd (2026-07-21).
fn runtime_sort_colstage_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(
            std::env::var("PGRUST_RUNTIME_SORT_COLSTAGE").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// GCUT kill switch (night/sort-merge-redesign inc-2): shared cross-worker
/// cutoff (zone-max seed + published worker floors, insert-time prune) plus
/// pre-staging zone granule skip AND the band predicate. LAYERED ON TOP of
/// COLSTAGE — the cutoff publishes/prunes in the COLSTAGE tight loop, so
/// GCUT without COLSTAGE would be dead weight; requiring both keeps the
/// parity story one switch deep (OFF either one = the incumbent
/// observation stream, and the band predicate stands down with it).
/// DEFAULT ON since the GL-SORTECON-3 flip increment (flipped-kill idiom):
/// `PGRUST_RUNTIME_SORT_GCUT=0|off` disables.
fn runtime_sort_gcut_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        runtime_sort_colstage_enabled()
            && !matches!(
                std::env::var("PGRUST_RUNTIME_SORT_GCUT").as_deref(),
                Ok("0") | Ok("off")
            )
    })
}

/// Zone-stats granule cap (the GL-SORTECON-3 pre-flip bound on engage-time
/// cost): above this many granules the leader zone walk is SKIPPED and the
/// band predicate cannot run — the arm refuses to the serial walk
/// (fail-conservative: the serial zone walk is never catastrophic; the
/// arm's win on an ultra-huge zone-hostile table is forgone, a documented
/// residual). Default 131072 granules ≈ 1.07B rows; the walk measured
/// ~82ns/granule (0.2ms @ 2442 granules, fleet job 3efc @ 0296033fd,
/// 2026-07-21) ⇒ ≤ ~11ms worst engage cost at the cap, noise against a
/// query of that scale. `PGRUST_RUNTIME_SORT_ZONESTATS_CAP` overrides.
fn runtime_sort_zonestats_cap() -> u64 {
    static CAP: OnceLock<u64> = OnceLock::new();
    crate::once_val(&CAP, || {
        std::env::var("PGRUST_RUNTIME_SORT_ZONESTATS_CAP")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(131_072)
    })
}

/// The band predicate's threshold (GL-SORTECON-3): the fraction of granules
/// PROVABLY serial-skippable at the zone-max seed bound (best word > seed —
/// a LOWER bound on what the serial zone-adaptive walk skips, since the
/// seed is achievable by construction). At or above this fraction the
/// shape is the ZONE-FRIENDLY band: the serial walk reads ≤ half the
/// granules and stays the winner — the arm refuses. Below it the shape is
/// the hostile/semi-hostile band the arm wins (fleet ladder @ 0296033fd,
/// 2026-07-21: dup_int rt/serial 0.49-0.09, rand_int ≤1.08 worst point at
/// dop4 with damped geomean 1.005 — the dop dependence is folded into the
/// planner floor's min_dop=4, notes/sort-merge-redesign-lane.md).
/// Fixture witness: asc_int frac=610/611 → serial; rand_int/dup_int
/// frac=0 → runtime.
const ZONE_FRIENDLY_MIN_SKIP_FRAC: f64 = 0.5;

/// DictCode sort-key class kill switch (docs/design/dict-code-flow.md
/// inc-1): `0|off` refuses text keys only — the int-family vocabulary and
/// every other admission are untouched.
fn runtime_sort_dictcode_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    crate::once_val(&ON, || {
        !matches!(std::env::var("PGRUST_RUNTIME_SORT_DICTCODE").as_deref(), Ok("0") | Ok("off"))
    })
}

/// The runtime sort sink arms, probed from the sort feed's SeqScan branch
/// BEFORE the serial arms arm anything: bounded sorts take the top-N sink
/// (shape a); unbounded forward-only sorts take the full-sort sink (shape
/// b, m3-sort-b car 2). `Ok(false)` = refused or fell back (nothing
/// consumed, no sort state touched; the serial feed runs byte-identically).
/// `Ok(true)` = the arm owns the node and its emit face is live.
pub(super) fn try_own_sort<'mcx>(
    state: &mut ::nodesort::SortState<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    outer_desc: &::types_tuple::TupleDescData<'static>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // --- Arming + kill-switch layering (all cheap; absent = today's path).
    // M5-1: the router is the DOP source (bench GUC verbatim when set; else
    // engine=runtime arms at pgrust.runtime_dop; else 0 = today's path).
    // The arm's own kills (PGRUST_RUNTIME_SORT / _FULL) keep gating here.
    let dop = router::arm_dop(ArmClass::Sort);
    if dop <= 0 || !runtime::runtime_enabled() || !runtime_sort_enabled() {
        return Ok(false);
    }
    if !state.bounded && !runtime_sort_full_enabled() {
        return Ok(false);
    }
    let Some(rt) = runtime::global() else { return Ok(false) };
    router::tick(ArmClass::Sort, ArmCounter::Offered);
    lane_trace("runtime-sort: probed");

    // --- Shape + session gates (fail-closed; every refusal = serial arm).
    if !seq_scan_fusible(ss, estate)? {
        refused("scan not fusible");
        return Ok(false);
    }
    if estate.es_instrument != 0 || estate.es_epq_active {
        refused("instrumented/epq");
        return Ok(false);
    }
    // WS-AD wave-8: both runtime shapes REPLACE the node's read-back face
    // (top-N winner buffer / adopted run partitions) — random-access reads
    // (backward, rescan replay, mark/restore) must land on a row-path
    // Tuplesort, so randomAccess refuses here wholesale. Previously
    // unreachable (the breaker refused randomAccess before this probe);
    // reachable once PGRUST_LANE_V2_SORT_RANDOMACCESS admits the bare-hook
    // feed. (`full_spec` also refuses on its own — this is the arm-wide
    // fail-closed gate covering the top-N shape too.)
    if state.randomAccess {
        refused("randomAccess (adopted emit face cannot serve random-access reads)");
        return Ok(false);
    }
    if parallel::IsParallelWorker() || xact::IsInParallelMode() {
        refused("already in parallel machinery");
        return Ok(false);
    }
    let spec = if state.bounded {
        let Some(spec) = topn_spec(state, ss, outer_desc) else {
            refused("shape spec (bound/key vocabulary/tlist census)");
            return Ok(false);
        };
        if spec.keys.iter().any(|k| k.dictcode) {
            // Observability for the on/off gates (dict-code-flow.md inc-1).
            lane_trace("runtime-sort: dictcode key class admitted");
        }
        ArmSpec::Topn(spec)
    } else {
        let Some(spec) = full_spec(state, ss, outer_desc) else {
            refused("full-sort shape spec (rewind/key vocabulary/tlist/column census)");
            return Ok(false);
        };
        // Per-participant budget: work_mem per Local (design §7); the
        // admission estimate refuses up front, a runtime crossing refuses
        // at accept (R5 rerun) — phase 1 has NO spill.
        let budget = (::init_small::globals::work_mem().max(64) as usize) * 1024;
        let est_row = state.plan.plan.plan_width.max(1) as f64
            + core::mem::size_of::<::nodesort::fullsort::RunEnt>() as f64
            + (spec.natts * 9) as f64;
        let est_per_local = state.plan.plan.plan_rows.max(0.0) * est_row / dop.max(1) as f64;
        if est_per_local > budget as f64 {
            refused("full-sort admission estimate exceeds work_mem per participant");
            return Ok(false);
        }
        ArmSpec::Full(spec, budget)
    };
    if estate.es_param_list_info.is_some_and(|p| !p.is_empty()) {
        refused("extern params");
        return Ok(false);
    }
    let Some(leader_pstmt) = estate.es_plannedstmt else {
        refused("no planned stmt");
        return Ok(false);
    };
    if leader_pstmt.paramExecTypes.iter().next().is_some() {
        refused("exec params");
        return Ok(false);
    }
    // Plan shape below the Sort: exactly THIS SeqScan (the workers receive
    // the SCAN SUBTREE as their pstmt; the Sort need not be the plan root —
    // Limit above it is the whole point of the shape).
    let Some(scan_node) = state.plan.plan.lefttree else {
        refused("sort child missing");
        return Ok(false);
    };
    if scan_node.node_tag() != NodeTag::T_SeqScan {
        refused("sort child not SeqScan");
        return Ok(false);
    }
    let scan_plan = scan_node.as_seq_scan().expect("SeqScan tag");
    if !super::runtime_scan::exprs_parallel_safe(scan_plan.scan.plan.qual.iter())?
        || !super::runtime_scan::exprs_parallel_safe(scan_plan.scan.plan.targetlist.iter())?
    {
        refused("parallel-unsafe scan exprs");
        return Ok(false);
    }
    if !estate.es_snapshot.as_deref().is_some_and(::types_snapshot::IsMVCCSnapshot) {
        refused("non-MVCC snapshot");
        return Ok(false);
    }
    let policy = parallel::query_task_policy_probe();
    if policy.has_params || policy.temp_state || policy.serializable || policy.pending_invalidations
    {
        refused("binder policy sources");
        return Ok(false);
    }

    // --- Geometry: enough granules to be worth a gang. (This also OPENS the
    // leader's scan desc — the winner gather below depends on it.) `None` =
    // no columnar Part yet (freshly INSERTed data still row-store-resident;
    // rowrefs would not exist) — refuse.
    let Some((total_granules, starts)) = ::nodeseqscan::seq_scan_cb_granule_geometry(ss, estate)?
    else {
        refused("granule geometry unavailable (no columnar part)");
        return Ok(false);
    };
    if total_granules < super::runtime_scan::min_granules().max(2 * dop as u64) {
        refused("granule floor");
        return Ok(false);
    }
    // DOP-elastic admission (tails192 #5): floors above ran against the
    // POOL dop; arm only what the work can feed (kill: PGRUST_RUNTIME_ELASTIC_DOP=0).
    let dop = super::runtime_scan::elastic_dop(dop, total_granules);

    // --- BAND PREDICATE + zone stats (GL-SORTECON-3 flip increment; GCUT
    // machinery, so the GCUT kill restores the pre-flip admission). For the
    // int-leading-key top-N shape (m5-coverage row 61), read the leading
    // key's zone stats ONCE (capped — see `runtime_sort_zonestats_cap`) and
    // classify the band: the fraction of granules PROVABLY skippable at the
    // zone-max seed is a LOWER bound on what the serial zone-adaptive walk
    // skips, so at >= ZONE_FRIENDLY_MIN_SKIP_FRAC the serial walk is the
    // proven winner and the arm refuses; below it the arm engages and the
    // same stats seed the shared cutoff + granule skip (computed once,
    // passed through to the payload).
    let zone: Option<(Arc<Vec<u64>>, Option<u64>)> = match (&spec, runtime_sort_gcut_enabled())
    {
        (ArmSpec::Topn(s), true) if !s.keys[0].dictcode => {
            if total_granules > runtime_sort_zonestats_cap() {
                // Fail-conservative: without stats the friendly band cannot
                // be detected — keep the serial walk (never catastrophic;
                // the forgone ultra-huge hostile win is the documented
                // residual).
                refused("zone-stats granule cap (band unknown; serial walk retained)");
                return Ok(false);
            }
            let k0 = s.keys[0];
            match ::nodeseqscan::seq_scan_cb_zone_topk_words(
                ss,
                k0.attno_scan,
                k0.desc,
                s.bound as u64,
            )? {
                Some((words, seed)) if words.len() == total_granules as usize => {
                    let nf = k0.nulls_first;
                    let t64: Vec<u64> = words
                        .into_iter()
                        .map(|w| ::nodesort::sink::cut64(nf, w))
                        .collect();
                    let seed_t64 = seed.map(|w| ::nodesort::sink::cut64(nf, w));
                    if let Some(sd) = seed_t64 {
                        let skippable = t64.iter().filter(|&&w| w > sd).count();
                        let frac = skippable as f64 / t64.len().max(1) as f64;
                        if frac >= ZONE_FRIENDLY_MIN_SKIP_FRAC {
                            lane_trace(&format!(
                                "runtime-sort: band predicate — zone-friendly \
                                 ({skippable}/{} granules provably serial-skippable)",
                                t64.len()
                            ));
                            refused("zone-friendly band (serial zone walk retained)");
                            return Ok(false);
                        }
                        lane_trace(&format!(
                            "runtime-sort: band predicate — hostile \
                             ({skippable}/{} granules seed-skippable)",
                            t64.len()
                        ));
                    }
                    Some((Arc::new(t64), seed_t64))
                }
                _ => None,
            }
        }
        _ => None,
    };

    // --- Engage.
    // Router counter choke point (M5-1): Engaged = ceremony entered;
    // Completed = the runtime answered; Fallback = R5 serial rerun.
    router::tick(ArmClass::Sort, ArmCounter::Engaged);
    let r = engage(
        state,
        ss,
        outer_desc,
        estate,
        rt,
        dop,
        total_granules,
        starts,
        spec,
        scan_node,
        zone,
    )?;
    router::tick(
        ArmClass::Sort,
        if r { ArmCounter::Completed } else { ArmCounter::Fallback },
    );
    Ok(r)
}

/// Which sort-sink arm is engaging (the payload construction switch).
enum ArmSpec {
    Topn(TopnSpec),
    /// Full sort + the per-Local byte budget (work_mem per participant).
    Full(FullSpec, usize),
}

/// Partition-parallel merge width for shape (b) (partition-count-agnostic
/// like the join lane's; 256 = the sink bucket precedent).
const FULLSORT_PARTS: usize = 256;

#[allow(clippy::too_many_arguments)]
fn engage<'mcx>(
    state: &mut ::nodesort::SortState<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    outer_desc: &::types_tuple::TupleDescData<'static>,
    estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    total_granules: u64,
    starts: Vec<u64>,
    spec: ArmSpec,
    scan_node: ::types_nodes::node_tree::Node<'mcx>,
    // GCUT zone stats (per-granule best cut64 words + cutoff seed),
    // computed ONCE by `try_own_sort`'s band predicate and passed
    // through. `None` = no zone machinery (GCUT off, dictcode lead,
    // full-sort spec, or stats unavailable).
    zone: Option<(Arc<Vec<u64>>, Option<u64>)>,
) -> PgResult<bool> {
    ensure_hooks_registered();
    crate::execparallel::register_parallel_query_main();

    let pstmt = crate::execparallel::build_worker_pstmt(estate, scan_node)?;

    let (keys, bound, full) = match &spec {
        ArmSpec::Topn(s) => (s.keys.clone(), s.bound, None),
        ArmSpec::Full(s, budget) => (
            s.keys.clone(),
            0,
            Some(FullShared {
                natts: s.natts,
                cols: s.cols.clone(),
                budget: *budget,
                parts: FULLSORT_PARTS,
                splitters: OnceLock::new(),
                out_parts: (0..FULLSORT_PARTS).map(|_| UnsafeCell::new(Vec::new())).collect(),
                budget_refused: AtomicBool::new(false),
                published: Mutex::new(None),
            }),
        ),
    };
    // GCUT (inc-2): the leader zone stats — per-granule best cut64 words
    // for the pre-staging granule skip + the zone-max SEED that starts the
    // shared cutoff where a best-first (zone-ordered) claim schedule would
    // have converged it. Computed once by the band predicate in
    // `try_own_sort`; passed through here.
    let (zone_best, cutoff_seed) = match zone {
        Some((best, seed)) => {
            lane_trace(&format!(
                "runtime-sort: gcut armed (zone granules={} seed={})",
                best.len(),
                if seed.is_some() { "yes" } else { "no" }
            ));
            (Some(best), seed)
        }
        None => (None, None),
    };
    let payload = Arc::new(RuntimeSortShared {
        rt,
        rg: OnceLock::new(),
        pcxt_shared: OnceLock::new(),
        // SAFETY (lifetime erasure): leader executor arena, held across the
        // whole engagement; DestroyParallelContext joins helpers before this
        // frame returns on every path.
        pstmt: SendConstPstmt(unsafe {
            core::mem::transmute::<*const PlannedStmt<'mcx>, *const PlannedStmt<'static>>(
                pstmt as *const PlannedStmt<'mcx>,
            )
        }),
        query_text: estate.es_sourceText.unwrap_or("").to_string(),
        eflags: estate.es_top_eflags,
        keys,
        bound,
        refused: AtomicUsize::new(0),
        started: AtomicUsize::new(0),
        exited: AtomicUsize::new(0),
        error: Mutex::new(None),
        failed: AtomicBool::new(false),
        broke: AtomicBool::new(false),
        winners: Mutex::new(None),
        cutoff: AtomicU64::new(cutoff_seed.unwrap_or(u64::MAX)),
        zone_best,
        zone_skipped: AtomicU64::new(0),
        full,
        standing: Mutex::new(None),
    });

    xact::EnterParallelMode();
    let engaged = engage_ceremony(
        state,
        ss,
        outer_desc,
        estate,
        rt,
        dop,
        total_granules,
        starts,
        &payload,
        &spec,
    );
    xact::ExitParallelMode();
    engaged
}

enum EngageOutcome {
    Fallback,
    Completed(Vec<u64>),
    CompletedFull(FullPublish),
}

/// This arm's standing-channel constants (M2 inc-1; see
/// standing_channel::StandingArm — sinks_gate: PGRUST_RUNTIME_POOLBIND_SINKS).
static STANDING_ARM: super::standing_channel::StandingArm =
    super::standing_channel::StandingArm {
        label: "runtime-sort",
        died: "runtime sort standing executors exited before completing the sort",
        sinks_gate: true,
    };

/// Shared post-outcome tail (standing and launched channels): worker-phase
/// errors rethrow PLAIN; budget/contract refusals take the R5 whole-attempt
/// serial rerun; an unexplained abort surfaces the pending interrupt or
/// reports; completed engagements must have published (protocol violation
/// otherwise, never silently wrong output).
fn finish_outcome(
    payload: &Arc<RuntimeSortShared>,
    outcome: runtime::RgOutcome,
) -> PgResult<EngageOutcome> {
    if let Some(e) = payload.take_error() {
        lane_trace(&format!("runtime-sort: worker-phase error: {}", e.message()));
        return Err(e);
    }
    if outcome == runtime::RgOutcome::Aborted {
        if let Some(full) = &payload.full {
            if full.budget_refused.load(Ordering::SeqCst) {
                // Design §7: a runtime budget crossing is a recorded
                // refusal + whole-attempt serial rerun (the serial arm
                // spills correctly), never an error.
                refused("per-participant sort budget crossed at accept");
                return Ok(EngageOutcome::Fallback);
            }
        }
        if payload.broke.load(Ordering::SeqCst) {
            lane_trace("runtime-sort: sink contract break; serial fallback");
            return Ok(EngageOutcome::Fallback);
        }
        ::postgres_seams::check_for_interrupts::call()?;
        return Err(Box::new(PgError::new(ERROR, "runtime sort pipeline aborted")));
    }
    if payload.started.load(Ordering::SeqCst) == 0 {
        return Ok(EngageOutcome::Fallback);
    }
    if let Some(full) = &payload.full {
        let Some(publish) = full.published.lock().unwrap_or_else(|p| p.into_inner()).take()
        else {
            // Completed with participants but nothing published: a
            // protocol violation, never silently wrong output.
            return Err(Box::new(PgError::new(
                ERROR,
                "runtime full sort completed without a published result",
            )));
        };
        return Ok(EngageOutcome::CompletedFull(publish));
    }
    // GCUT engagement witness (the ladder rig greps this line) — the
    // shared tail serves both the standing and launched channels, so the
    // witness fires on either.
    if let Some(zb) = &payload.zone_best {
        lane_trace(&format!(
            "runtime-sort: gcut zone-skip granules_skipped={} of {}",
            payload.zone_skipped.load(Ordering::SeqCst),
            zb.len()
        ));
    }
    let Some(winners) = payload.take_winners() else {
        // Completed with participants but no published winners: a
        // protocol violation, never silently wrong output.
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime sort completed without a winner list",
        )));
    };
    Ok(EngageOutcome::Completed(winners))
}

#[allow(clippy::too_many_arguments)]
fn engage_ceremony<'mcx>(
    state: &mut ::nodesort::SortState<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    outer_desc: &::types_tuple::TupleDescData<'static>,
    estate: &mut EStateData<'mcx>,
    rt: &'static Arc<runtime::Runtime>,
    dop: i32,
    total_granules: u64,
    starts: Vec<u64>,
    payload: &Arc<RuntimeSortShared>,
    spec: &ArmSpec,
) -> PgResult<bool> {
    let pcxt = parallel::CreateParallelContext("postgres", "pgrust_runtime_sort_main", dop)?;
    let mut submitted: Option<runtime::RgHandle> = None;

    let body = (|mut_submitted: &mut Option<runtime::RgHandle>| -> PgResult<EngageOutcome> {
        parallel::InitializeParallelDSM(pcxt)?;
        let nworkers = parallel::nworkers(pcxt);
        if nworkers <= 0 {
            return Ok(EngageOutcome::Fallback);
        }
        parallel::InstallQueryTaskBinding(pcxt, parallel::QueryTaskBindingPolicy::default())?;
        payload
            .pcxt_shared
            .set(parallel::shared_for(pcxt))
            .unwrap_or_else(|_| unreachable!("pcxt shared set once"));
        parallel::set_private(pcxt, Arc::clone(payload) as _);
        // Standing driver dispatch (M2 inc-1): deferred_bind false — this
        // arm binds EAGERLY (with_query_task_binding); the standing serve
        // re-establishes visibility up front and evicts parked sticky.
        parallel::set_standing_driver(pcxt, parallel::standing::StandingDriver {
            drive: runtime_sort_standing_driver,
            deferred_bind: false,
        });

        // Submit the pinned RG (accept → seal → combine) before launch.
        let source = Arc::new(super::runtime_scan::PgrcolumnarGranuleSource {
            starts: Arc::new(starts),
            // This arm feeds claims straight into set_granule_range
            // (single-epoch contract); it does not subdivide multi-epoch
            // claims — never coalesce.
            coalesce: false,
        });
        let runtime::SealedSinkTaskSets { accept, freeze, combine, probe: _probe } =
            runtime::sealed_sink_tasksets(
                Arc::clone(payload),
                source,
                rt.nthreads() + runtime::MAX_EXTERNAL_LANES,
                0,
            );
        static NEXT_QUERY_ID: AtomicUsize = AtomicUsize::new(1);
        let (rg, waiter) = rt.submit_pinned_with_affinity(runtime::QuerySpec {
            query_id: NEXT_QUERY_ID.fetch_add(1, Ordering::SeqCst) as u64,
            tasksets: vec![accept, freeze, combine],
        }, router::session_affinity_token());
        payload.rg.set(rg.downgrade()).unwrap_or_else(|_| unreachable!("rg set once"));
        *mut_submitted = Some(rg.clone());

        // M2 inc-1: STANDING engagement first — no worker launch, one
        // binder bind per participant; fallback leaves the RG untouched
        // for the launched path below.
        let census = match spec {
            ArmSpec::Topn(s) => format!("bound={}", s.bound),
            ArmSpec::Full(..) => "full".to_string(),
        };
        match super::standing_channel::standing_wait(
            &STANDING_ARM,
            super::standing_channel::StandingLeader {
                // M2 inc-2: sink arms ride the pool-db channel in the
                // follow-up wiring (scan arm first — the funnel
                // discipline); None = gang-first, inc-1 exactly.
                pool: None,
                shared: payload.pcxt_shared.get().expect("pcxt shared set above"),
                slot: &payload.standing,
                started: &payload.started,
                refused: &payload.refused,
                take_error: &|| payload.take_error(),
                drain: &|rg| drain_rg(rt, rg),
                census: &census,
            },
            dop,
            total_granules,
            &rg,
            &waiter,
        )? {
            super::standing_channel::StandingWait::Done(outcome) => {
                return finish_outcome(payload, outcome);
            }
            super::standing_channel::StandingWait::Fallback => {}
        }

        let launched = parallel::LaunchParallelWorkers(pcxt)?;
        if launched <= 0 {
            lane_trace("runtime-sort: zero workers launched");
            drain_rg(rt, &rg);
            return Ok(EngageOutcome::Fallback);
        }
        // The launched-DOP census line (the m1-heap-source harness trap:
        // max_worker_processes silently caps DOP probes — every probe
        // config must be able to see the LAUNCHED number, not the asked
        // one).
        match spec {
            ArmSpec::Topn(s) => lane_trace(&format!(
                "runtime-sort: engaged dop={launched}/{dop} granules={total_granules} bound={}",
                s.bound
            )),
            ArmSpec::Full(..) => lane_trace(&format!(
                "runtime-sort: engaged dop={launched}/{dop} granules={total_granules} full"
            )),
        }

        // Submit-and-park (the WaitForParallelWorkersToFinish shape).
        let mut all_exited_seen = false;
        let outcome = loop {
            if let Some(o) = waiter.try_wait() {
                break o;
            }
            if let Err(e) = ::postgres_seams::check_for_interrupts::call()
                .and_then(|()| parallel::ProcessParallelMessages())
            {
                rg.abort();
                drain_rg(rt, &rg);
                return Err(e);
            }
            let refused = payload.refused.load(Ordering::SeqCst);
            let started = payload.started.load(Ordering::SeqCst);
            if started == 0 && refused >= launched as usize {
                lane_trace(&format!("runtime-sort: all {refused} helpers refused the bind"));
                rg.abort();
                drain_rg(rt, &rg);
                return Ok(EngageOutcome::Fallback);
            }
            // LIVENESS backstop (m1 helper-death fix 5cf96f83d, ported from
            // runtime_scan.rs — F1 defect layer 2a; this arm lacked it until
            // the M5-2 consolidation): every launched helper's task has
            // ENDED (normal hook exit keeps BGWH_STARTED until after the
            // drive, so this cannot trip mid-drive) yet the RG is incomplete
            // — helpers died or returned without a channel message and
            // without driving. Nothing claimed => clean serial fallback;
            // claimed => reap if possible and surface a real error (with the
            // arm's budget/contract fallbacks honored first).
            if parallel::parallel_workers_all_stopped(pcxt) {
                if let Some(o) = waiter.try_wait() {
                    break o;
                }
                let claimed = rg.stats().tasks_claimed;
                lane_trace(&format!(
                    "runtime-sort: helpers all stopped, rg incomplete (claimed={claimed})"
                ));
                rg.abort();
                let drained = drain_rg(rt, &rg);
                if let Some(e) = payload.take_error() {
                    return Err(e);
                }
                if payload
                    .full
                    .as_ref()
                    .is_some_and(|f| f.budget_refused.load(Ordering::SeqCst))
                {
                    // (`self::` — the loop-local `refused` count shadows
                    // the module-level trace fn in the value namespace.)
                    self::refused("per-participant sort budget crossed at accept");
                    return Ok(EngageOutcome::Fallback);
                }
                if payload.broke.load(Ordering::SeqCst) {
                    lane_trace("runtime-sort: sink contract break; serial fallback");
                    return Ok(EngageOutcome::Fallback);
                }
                if claimed == 0 && drained {
                    return Ok(EngageOutcome::Fallback);
                }
                return Err(Box::new(PgError::new(
                    ERROR,
                    "runtime sort helpers exited before completing the sort",
                )));
            }
            // LIVENESS REAP (m35-spill inc-2c port — the FLAG named this
            // arm class; the agg leg-4d wedge): a pinned RG is invisible to
            // pool workers, so once every launched helper has exited
            // without the RG completing, NOBODY will ever step it and the
            // leader parks forever (the all-stopped probe above cannot see
            // helpers that exited their drive but parked back to the pool).
            // Reap: abort + drain the closed generation ourselves; the next
            // try_wait surfaces Aborted and the existing error/budget/broke/
            // fallback handling below decides. Two consecutive sightings
            // before reaping let a mid-settlement completion land first —
            // belt only: a helper's exit bump happens-after its drive's
            // completion, and abort + drive_pinned on a completed RG are
            // benign no-ops.
            if payload.exited.load(Ordering::SeqCst) >= launched as usize {
                if all_exited_seen && waiter.try_wait().is_none() {
                    lane_trace(
                        "runtime-sort: all helpers exited without completing the RG — reaping",
                    );
                    rg.abort();
                    drain_rg(rt, &rg);
                    continue;
                }
                all_exited_seen = true;
            }
            // A raised cancel disposition (statement_timeout /
            // pg_cancel_backend) surfaces from the latch quantum as an Err
            // (F1 defect layer 2b): abort + drain the RG, then propagate —
            // exactly the CFI branch above. Discarding it made this park
            // loop uncancellable (the F1 chaos finding the quantum's
            // contract documents; fixed at the M5-2 consolidation).
            if let Err(e) = parallel::wait_parallel_finish_quantum() {
                rg.abort();
                drain_rg(rt, &rg);
                return Err(e);
            }
        };

        finish_outcome(payload, outcome)
    })(&mut submitted);

    // Teardown tail (every path): a submitted RG must be COMPLETE before the
    // parallel context is destroyed and this frame's arena can unwind.
    if let Some(rg) = &submitted {
        if rg.try_outcome().is_none() {
            drain_rg(rt, rg);
        }
    }
    let destroy = parallel::DestroyParallelContext(pcxt);
    let outcome = body?;
    destroy?;

    match (outcome, spec) {
        (EngageOutcome::Fallback, _) => {
            lane_trace("runtime-sort: fallback to serial arm");
            Ok(false)
        }
        (EngageOutcome::Completed(winners), ArmSpec::Topn(spec)) => {
            adopt_winners(state, ss, outer_desc, estate, spec, winners)
        }
        (EngageOutcome::CompletedFull(publish), ArmSpec::Full(..)) => {
            ::nodesort::sort_lane_runtime_full_adopt(state, publish.runs, publish.parts);
            trace_feed("runtime full sort adopt + partition emit engaged");
            lane_trace(&format!(
                "runtime-sort: complete (full), rows={}",
                ::nodesort::sort_lane_runtime_full_rows(state)
            ));
            Ok(true)
        }
        _ => Err(Box::new(PgError::new(ERROR, "runtime sort outcome/arm mismatch"))),
    }
}

/// The leader's late-materialization gather (refsort v2, design §4): decode
/// each winner's rowref, gather the full row through the leader's own scan
/// state, project Var-only into outer format, buffer on the node. ≤ bound
/// rows total. Any gather miss resets the node and falls back to the serial
/// arm BEFORE any output escapes (the winners buffer is node-internal until
/// the emit face pops it — the serial refsort invariant, reused).
fn adopt_winners<'mcx>(
    state: &mut ::nodesort::SortState<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    outer_desc: &::types_tuple::TupleDescData<'static>,
    estate: &mut EStateData<'mcx>,
    spec: &TopnSpec,
    winners: Vec<u64>,
) -> PgResult<bool> {
    ::nodesort::sort_lane_runtime_topn_begin(state);
    let natts = outer_desc.natts as usize;
    let mut values = vec![::datum::Datum::null(); natts];
    let mut isnull = vec![true; natts];
    let mcx = estate.es_query_cxt;
    for &r in &winners {
        let (rg, row) = ((r >> 32) as u32, r as u32);
        if !::nodeseqscan::seq_scan_gather_row(ss, estate, rg, row) {
            lane_trace("runtime-sort: winner gather failed; serial fallback");
            ::nodesort::sort_lane_reset_for_refeed(state);
            return Ok(false);
        }
        {
            let slot = estate.slot_mut(ss.ss.ss_ScanTupleSlot);
            let base = slot.base();
            for (j, &c) in spec.tlist_map.iter().enumerate() {
                values[j] = base.tts_values[c as usize];
                isnull[j] = base.tts_isnull[c as usize];
                // Needed-set guard (the refsort law): gather_row nulls only
                // unneeded cells (pgrcolumnar stores no NULLs), so a null
                // projected cell means the column was outside the scan's
                // needed set — fall back before any output escapes.
                if isnull[j] {
                    lane_trace("runtime-sort: gathered cell outside the needed set; serial fallback");
                    ::nodesort::sort_lane_reset_for_refeed(state);
                    return Ok(false);
                }
            }
        }
        ::nodesort::sort_lane_refsort_push_winner(state, mcx, &values, &isnull)?;
    }
    ::nodesort::sort_lane_runtime_topn_done(state);
    trace_feed("runtime sort sink adopt + refsort emit engaged");
    lane_trace(&format!(
        "runtime-sort: complete, winners={} (bound {})",
        ::nodesort::sort_lane_refsort_winners(state),
        spec.bound
    ));
    Ok(true)
}

/// Reap a pinned RG no helper will drive (abort/fallback paths) — protocol
/// cleanup driving, not leader work execution (§2.5). Abort + BOUNDED drain
/// (M5-2 consolidation: this arm's drain previously spun UNBOUNDED for an
/// external lane and could block forever on a dead participant's pin — the
/// scan/agg/hashjoin arms' bounded shape is the family discipline). True =
/// the RG completed; false = it could not be completed — the RG and its
/// slot are deliberately LEAKED (bounded by the slot array; a process
/// restart resets everything) and the caller must surface an error rather
/// than wait forever.
fn drain_rg(rt: &'static Arc<runtime::Runtime>, rg: &runtime::RgHandle) -> bool {
    rg.abort();
    // Bounded lane wait (~2s): helper drives settle within a morsel.
    let mut lane = None;
    for _ in 0..4000 {
        if let Some(l) = rt.acquire_external_lane() {
            lane = Some(l);
            break;
        }
        std::thread::sleep(std::time::Duration::from_micros(500));
    }
    let Some(lane) = lane else {
        lane_trace("runtime-sort: LEAKED pinned RG (no external lane for the drain)");
        return false;
    };
    let mut local = lane.local();
    let drained = rt.try_drain_pinned(&mut local, rg, 4000).is_some();
    if !drained {
        lane_trace("runtime-sort: LEAKED pinned RG (drain gave up — dead participant?)");
    }
    drained
}
