// nodeSeqscan.c. ExecProcNode dispatch is the variant enum resolved once at
// init (C installs one of five function pointers).
#![allow(non_snake_case)]

extern crate alloc;

use ::execexpr::exec_init_qual;
use ::execscan::{exec_scan_epq, exec_scan_extended, ScanNode, ScanState};
use ::executils::{EStateData, ExecSlotId};
use ::mcx::{Mcx, PgVec};
use ::tableam::{
    table_beginscan, table_beginscan_parallel, table_endscan, table_parallelscan_initialize,
    table_parallelscan_reinitialize, table_rescan, table_scan_arm_adaptive_order,
    table_scan_disarm_adaptive_order, table_scan_getnextslot, table_scan_update_scan_bound,
    table_slot_callbacks, ParallelTableScanDescShared,
};
use ::types_error::{PgError, PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE};
use ::types_nodes::plannodes::SeqScan;
use ::types_rel::Relation;
use ::types_slot::{
    SlotData, EXEC_FLAG_BACKWARD, EXEC_FLAG_EXPLAIN_ONLY, EXEC_FLAG_MARK, EXEC_FLAG_WITH_NO_DATA,
};

pub fn init_seams() {}

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SeqScanVariant {
    Plain,
    WithQual,
    WithProject,
    WithQualProject,
    // Hashjoin-pushed Bloom filter over the scan's key column (pure filter,
    // false positives only); reverts to Plain on disarm.
    PlainBloom,
    Epq,
}

pub struct SeqScanState<'mcx> {
    pub ss: ScanState<'mcx>,
    variant: SeqScanVariant,
    plan_node_id: i32,
    // The planner's output-row estimate for this scan (Plan.plan_rows,
    // retained at init like plan_node_id): plan-time admission floors read
    // it without a plan-tree walk (K1 heap grouped small-N floor).
    plan_rows: f64,
    parallel_aware: bool,
    // Keeps the scan desc's NonNull target alive for the scan's lifetime.
    parallel: Option<std::sync::Arc<ParallelTableScanDescShared>>,
    // Boxed: PlanStateNode carries a 1024-byte size assert.
    batch_soa: Option<::mcx::PgBox<'mcx, BatchSoa<'mcx>>>,
    scan_batch: ScanBatchMode,
    batch_allowed: bool,
    bloom: Option<::mcx::PgBox<'mcx, BloomScan<'mcx>>>,
    // Lane-executor-v2 page-batch cursor (driven by `execmain::lanev2`):
    // position within the currently-staged page batch and its row count.
    // `lane_pos == lane_n` (both 0 initially) means "pull the next batch".
    // Reset on rescan/park. Only touched via the accessors below; the lane
    // drive itself lives entirely in the `lanev2` module.
    lane_pos: u32,
    lane_n: u32,
    // Lane-executor-v2 memoized STATIC fusibility verdict (plan shape + AM
    // page-batch support), computed once at the first dispatch: the refuse
    // verdict must be stable across Volcano calls — a mid-scan REFUSE→OWN
    // flip would skip the staged remainder of the current page — and the
    // fusibility cascade must not run per pulled tuple. Dynamic per-call
    // gates (EPQ, direction) stay in the lane. None = not yet evaluated.
    // Reset on park (rebind may change the backing scan).
    lane_verdict: Option<bool>,
    // Memoized STANDALONE-ownership verdict for pgrcolumnar scans (lane-v2):
    // admitted only with an armed qual kernel; the arm outcome is static per
    // node, and the admission cascade must not re-run per pulled tuple (the
    // per-pull walk measured +20% on kernel-less count(*) shapes). Reset
    // with lane_verdict on park.
    cb_standalone: Option<bool>,
    // Memoized PREWHERE-arm refusal (walker/translate refused the qual —
    // static per node: the qual never changes). Refused shapes must not
    // re-pay the translate cascade (LIKE-kernel builds, the regex probe
    // compile) per feed event or rescan — the refusal-audit "admission-
    // attempt tax" (coordinator rider, 2026-07-14). Never set on success.
    cb_prewhere_refused: bool,
    // The memoized-false standalone verdict's REASON split: true = the
    // tiny-input row floor refused (before any arm cascade ran), so the
    // per-pull refusal accounting ticks tiny-input-floor instead of
    // admission-economics. Reset with cb_standalone on park.
    cb_tiny: bool,
    // Cursor-suspension park record (WS-AI wave-9.5, lane-cursors.md §2):
    // (b0, b1, pos, n) — the settled lane-staged page batch's block b0, the
    // remainder window end b1 (forward walk [b0, b1), no wrap — the
    // park-point probe refuses wrap-capable walks), and the consume cursor
    // at suspension. Written by `seq_scan_cursor_settle` (which released
    // the staged claim's pin — R3 zero-pins-at-settle), consumed by
    // `seq_scan_cursor_resume` (restage + cursor restore). Reset on
    // rescan/skeleton-park (a rebound or rescanned scan restarts; a stale
    // park record must never reposition it).
    lane_park: Option<SeqScanCursorPark>,
    // SE-R41 v2 cursor-fill pin posture (see `lane_hold_pin()`): true once a
    // cursor store batch fill engaged this scan — the staged page batch and
    // its pin survive suspension (C-parity Volcano posture), and
    // `seq_scan_cursor_settle` refuses to park. Reset on rescan (the next
    // engagement re-establishes it).
    lane_hold_pin: bool,
    // pgrcolumnar relations only: plan-derived column need-set + zone-mappable
    // conjuncts, installed on the scan desc at open (pgrcolumnar-impl.md §7.3).
    cb_scan: Option<std::boxed::Box<CbScanInfo>>,
}

/// Cursor-suspension park record (WS-AI wave-9.5; the `lane_park` field's
/// payload): the settled lane-staged batch's block `b0`, remainder window
/// end `b1` (forward walk `[b0, b1)`, no wrap), and the consume cursor
/// `(pos, n)` at suspension.
#[derive(Clone, Copy)]
struct SeqScanCursorPark {
    b0: u64,
    b1: u64,
    pos: u32,
    n: u32,
}

/// Plan-derived pgrcolumnar scan settings (built once at init, applied to every
/// freshly opened scan desc — serial open and both parallel init paths).
struct CbScanInfo {
    /// Columns the scan reads (qual + targetlist Vars; whole row when a
    /// whole-row Var appears). Only these columns' chunks decode.
    needed: Vec<bool>,
    /// The QUAL's own column contribution alone (whole-row-in-qual forces
    /// all) — the floor `seq_scan_cb_narrow_needed` may not shrink below:
    /// the exact per-row qual must keep reading real cells.
    qual_needed: Vec<bool>,
    /// The plan-derived full needed set, stashed by
    /// `seq_scan_cb_narrow_needed` so `seq_scan_cb_restore_needed` can put
    /// it back (the serial refsort accept-narrow / gather-restore pair —
    /// lazytopn lane). `None` = not currently narrowed.
    needed_full: Option<Vec<bool>>,
    /// Zone-map-mappable `Var CMP Const` conjuncts of the scan qual
    /// (advisory pruning only; the executor still evaluates the full qual
    /// on surviving rows).
    zone: Vec<::tableam::ZoneQual>,
    /// v7 zero-count meta qual: Some iff the scan qual is EXACTLY one
    /// conjunct and it lowers to `col <> 0` / `col = 0` in the stored
    /// domain (cb_zone_conjunct's int/date/timestamp compare families).
    /// Unlike `zone`, this is a SEMANTIC recognition — the metaagg arm
    /// answers the whole node from it, so it must equal the full qual.
    zero_qual: Option<::tableam::MetaZeroQual>,
    /// EVERY conjunct of the scan qual lowered to a zone qual (`zone` IS
    /// the full qual, not an advisory subset). Grants the footer-stat agg
    /// meta arm its all-rows-pass proof: an AllPass zone verdict on every
    /// entry proves the whole qual over the unit's rows.
    zone_covers_qual: bool,
}

// Hashjoin Bloom pushdown state: key-column-only SoA deform per staged page,
// selection bits from the filter; survivors store like the per-row path.
struct BloomScan<'mcx> {
    filter: std::rc::Rc<::nodehash::ProbeBloom<'mcx>>,
    plan: ::exectuples::SoaDeformPlan<'mcx>,
    soa: ::exectuples::SoaBatch<'mcx>,
    col: u16,
    sel: [u64; ::exectuples::SOA_BM_WORDS],
    nwords: u32,
    cur_word: u32,
    cur_bits: u64,
    seen: u32,
    kept: u32,
}

impl BloomScan<'_> {
    #[inline(always)]
    fn next_selected(&mut self) -> Option<u32> {
        loop {
            if self.cur_bits != 0 {
                let bit = self.cur_bits.trailing_zeros();
                self.cur_bits &= self.cur_bits - 1;
                return Some(self.cur_word * 64 + bit);
            }
            if self.cur_word + 1 >= self.nwords {
                return None;
            }
            self.cur_word += 1;
            self.cur_bits = self.sel[self.cur_word as usize];
        }
    }

    fn reset_staged(&mut self) {
        self.nwords = 0;
        self.cur_word = 0;
        self.cur_bits = 0;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScanBatchMode {
    Unknown,
    Off,
    On,
}

struct BatchSoa<'mcx> {
    plan: ::exectuples::SoaDeformPlan<'mcx>,
    soa: ::exectuples::SoaBatch<'mcx>,
    // Bitmap-able kernel qual (QualScanVarCmpConst on a prefix column).
    qual_armed: bool,
    // Scan-node drive: deform the qual column only; survivors deform lazily.
    qual_only: bool,
    // Fused-sort direct key feed: deform this column only, never publish the
    // prefix onto the slot (other prefix cells stay stale).
    key_col: Option<u16>,
    // Varlena key: staged into soa column 0 via the varkey pass.
    varkey: Option<::exectuples::SoaVarKeyPlan>,
    // Precomputed emit_key read column (0 for varkey, key_col for fixed):
    // one load on the per-row read path.
    key_read_col: u16,
    // Precomputed !qual_only && key_col.is_none(): one test on the store path.
    publish: bool,
    // The kernel-qual clause list (AND of scan-Var-CMP-Const; 1 = the fused
    // kernel, 2+ = the multi-clause census the lane admits).
    quals: [(u16, ::execexpr::CmpOp, ::datum::Datum); ::execexpr::SCAN_CMP_MAX_CLAUSES],
    nquals: u8,
    // Contains-LIKE kernel qual (the strsearch census) over the varkey-staged
    // qual column; exclusive with `quals` (nquals stays 0).
    contains: Option<::execexpr::ScanContainsClause>,
    // Tier-2 stitched-JIT state; armed only by the lane driver on drain
    // pipelines feeding breakers (`seq_scan_stitch_arm`).
    stitch: Option<QualStitch>,
    // Stitched-projection state (Phase-3 projection stitching); armed only
    // by the lane driver on drain pipelines (`seq_scan_proj_stitch_arm`).
    proj: Option<ProjStitch<'mcx>>,
    // PREWHERE v1 lane qual (pgrcolumnar scans under lane-v2 only; phase4 design
    // §3): the fail-closed translation of the scan qual — staged clauses in
    // ascending cost order (zone folds + per-clause late materialization at
    // window staging), the dict text tier, and the hybrid requal split. When
    // armed it OWNS the selection bitmap; the kernel `quals`/stitch tiers
    // are bypassed for this scan.
    lane: Option<Box<::laneexec::LaneQualProg>>,
    // Hybrid lane qual: the sel bits are a conservative PRE-FILTER (the
    // qual's vectorizable clause prefix); every selected row re-runs the
    // FULL original qual per row at fetch. Exact-bitmap consumers
    // (`seq_scan_batch_qual_sel`, the qual census) refuse these batches.
    lane_requal: bool,
    // BITS-ONLY drive (dop1-tax2 inc-2, the census consumer): nothing past
    // the qual reads the staged SoA — the drive consumes selection bits
    // (fold_batch popcount) and fallback rows re-check off the STORE path.
    // Skip the post-eval materialization the lane does for SoA readers
    // (survivor-window completing deform + dict-lane gather; the serial
    // Volcano census never does either). Bits are computed identically —
    // eval reads the staged clause columns, which still stage. Set only by
    // `seq_scan_batch_bits_only` (the runtime census arm).
    bits_only: bool,
    // Dict-GROUP consumer column (pgrcolumnar dict-code grouping, pgrcolumnar-v2
    // plan Stage 2.1): the agg feed reads this column as codes+dict past the
    // qual, so the post-qual gather-to-Raw must SKIP it (the feed is the
    // dict-code consumer PREWHERE v1 said didn't exist yet). None = every
    // dict-answered qual lane gathers back to Raw as before.
    dict_group: Option<u16>,
    // Condition cache armed (pgrust.condition_cache): the scan desc carries
    // the fingerprint+entry state; this flag gates the pagebatch drive's
    // lookup/store calls and the end-of-scan stats line.
    cond_armed: bool,
    // K1 inc-2 late materialization (wave-9 WS-AH): when armed, the heap
    // staging deform narrows its kind-0 column-major pass to exactly this
    // set ({qual clause cols ∪ the grouped feed's key cols}, sorted); the
    // deferred prefix columns fill for qual survivors only, through
    // `seq_scan_batch_complete_deform` (the storage seam's
    // `complete_deform`). Classification is UNCHANGED (kind-1 hasnulls rows
    // still full-deform at classify; kind-2 rows keep the fallback bit).
    // None = today's full staging bytes. Armed per BUILD by the grouped
    // drains (`seq_scan_k1_latemat_arm`), heap kernel-qual stagings only.
    stage_cols: Option<Vec<u16>>,
    sel: [u64; ::exectuples::SOA_BM_WORDS],
    nwords: u32,
    cur_word: u32,
    cur_bits: u64,
}

/// Tier-2 (stitched-JIT) state for the kernel-qual filter segment — the JIT
/// ladder per design doc §3a: interpreter (oracle/floor, inside
/// `StitchedProgram::run`) → AOT bitmap passes (`qual_bitmap_cmp_const`) →
/// the stitched body. Lives on the `BatchSoa` so the row census and the
/// sticky refusal are per plan-node arming; `exec_end_seq_scan` releases it
/// (the deform-JIT Rc precedent).
struct QualStitch {
    /// The clause program (LoadLane/LoadConst/Cmp/Qual per clause), the
    /// translation of `BatchSoa::quals` — also the replay/oracle source the
    /// stitched body falls back to on drift or refuse-and-replay.
    prog: ::lanestitch::Program,
    /// Lane-view width the body compiles against (max clause col + 1).
    ncols: usize,
    /// Compiled once past the row floor; None below it (AOT tier owns).
    body: Option<::lanestitch::StitchedProgram>,
    /// Rows staged through the armed qual so far (the tier-2 row floor).
    rows_seen: u64,
    /// Sticky per-plan refusal (classification / arch / arena refuse).
    refused: bool,
    // Engagement telemetry (PGRUST_LANE_V2_TRACE summary at scan end).
    n_stitched: u64,
    n_aot: u64,
    n_interp: u64,
}

/// Stitched-projection state for a lane-owned projected scan (Phase-3
/// projection stitching): the vocabulary-covered target list (Var
/// passthrough / same-width int2/4/8 arith — `ScanProjCols`) compiled over
/// the staged SoA lanes, computing per-batch OUTPUT lanes for the qual
/// bitmap's true survivors (forced-fallback rows are masked out — their
/// lanes are undeformed; they keep the per-row path). The emit's fast lane
/// fills the projection result slot from the output lanes; everything the
/// vocabulary does not cover refuses at arm time and leaves the per-row
/// `exec_project` path untouched.
///
/// Refuse-and-replay (charter discipline): an arith trap (overflow / zero
/// divisor) makes the body exit refused having constructed NO error and
/// this batch's `staged` stays false — every row of the batch then projects
/// per-row through the C-ported `exec_project`, which raises C's exact
/// error text on C's row after consuming the preceding survivors. Sticky
/// per plan: after one replay the body never runs again.
struct ProjStitch<'mcx> {
    /// The tlist translation (LoadLane/LoadConst/Arith/StoreOut per column).
    prog: ::lanestitch::Program,
    /// Lane-view width the body compiles against (max read attnum + 1).
    ncols: usize,
    /// Output-lane count == tlist arity == result-slot natts.
    nouts: u16,
    /// Compiled once past the row floor; None below it (per-row tier owns).
    body: Option<::lanestitch::StitchedProjection>,
    rows_seen: u64,
    /// Sticky per-plan refusal (classification / arch / arena / replay).
    refused: bool,
    /// Outputs valid for the CURRENTLY staged batch (set at staging).
    staged: bool,
    /// The selectivity disarm applies: hosting WIDENED the per-batch deform
    /// beyond what the qual alone stages (the single-clause col-only case),
    /// so low-selectivity scans pay full-prefix deform for few saved
    /// projections. `stitched_rows`/`stitched_survivors` (rows staged /
    /// true survivors through the stitched body) feed the one-shot check in
    /// `stitch_project`.
    adapt: bool,
    adapt_checked: bool,
    stitched_rows: u64,
    stitched_survivors: u64,
    /// Output lanes, nouts x SOA_MAX_ROWS (column-major, SoaBatch layout).
    out_values: ::mcx::PgVec<'mcx, ::datum::Datum>,
    out_isnull: ::mcx::PgVec<'mcx, bool>,
    // Engagement telemetry (PGRUST_LANE_V2_TRACE summary at scan end).
    n_stitched: u64,
    n_perrow: u64,
}

/// Selectivity floor for the ADAPTIVE projection disarm (admission
/// economics, measured 2026-07-12 on the 10M-row lane-bench dataset, warm
/// best-of-3x3 interleaved): when hosting widened a single-clause col-only
/// deform to the full projection prefix, ~10%-selectivity shapes ran +1-2%
/// (p1/p4: extra 4-5 col deform on every staged row, few saved projections)
/// while ~50%-selectivity shapes won 13-19% (p2/p3). One-shot check after
/// PROJ_ADAPT_ROWS staged rows: below the floor, drop the projection arm —
/// staging returns to the qual-only col deform and the per-row projection
/// path (exactly the pre-projstitch lane). Ratchet only with a measurement.
const PROJ_MIN_SELECTIVITY_PCT: u64 = 20;
// 16k rows: >=1.6k survivors even at the 10% floor case — ample signal; the
// widened-deform probe window stays ~0.2% of a 10M-row scan.
const PROJ_ADAPT_ROWS: u64 = 16384;

/// Tier-2 row floor (the batchexec POC admission number): the stitched body
/// engages only once ~2048 rows have flowed through the armed qual — OLTP-
/// sized scans never pay a stitch.
const STITCH_ROW_FLOOR: u64 = 2048;

/// Tier-2 fusion floor (admission economics, design §4 — never preempt a
/// measured-faster path): the stitched body engages only when it FUSES
/// something the AOT tier runs as separate passes, i.e. >= 2 clauses. A
/// single-clause body re-runs exactly the AOT kernel's one pass plus the
/// per-batch call/params overhead — measured 2026-07-12 (10M-row filtered
/// drain shapes, warm best-of-6 interleaved): 1-clause agg feeds 0.998x
/// (parity), 1-clause sort feed 1.04x (loss); 3-clause shapes 0.97-0.98x
/// (fusion win). Ratchet DOWN only with a measurement.
const STITCH_MIN_CLAUSES: u8 = 2;

/// Engagement trace (verification aid, no perf path): mirrors lanev2's
/// `PGRUST_LANE_V2_TRACE` switch so one env var traces the whole lane.
fn lane_trace(event: &str) {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    if *ON.get_or_init(|| {
        matches!(std::env::var("PGRUST_LANE_V2_TRACE").as_deref(), Ok("1") | Ok("on"))
    }) {
        eprintln!("[lane-v2] {event}");
    }
}

impl BatchSoa<'_> {
    #[inline(always)]
    fn next_selected(&mut self) -> Option<u32> {
        loop {
            if self.cur_bits != 0 {
                let bit = self.cur_bits.trailing_zeros();
                self.cur_bits &= self.cur_bits - 1;
                return Some(self.cur_word * 64 + bit);
            }
            if self.cur_word + 1 >= self.nwords {
                return None;
            }
            self.cur_word += 1;
            self.cur_bits = self.sel[self.cur_word as usize];
        }
    }

    fn reset_staged(&mut self) {
        self.nwords = 0;
        self.cur_word = 0;
        self.cur_bits = 0;
        if let Some(p) = self.proj.as_mut() {
            // The staged batch is gone; its output lanes go with it. (The
            // emit fast lane is additionally gated on nwords > 0, so this
            // is belt-and-braces.)
            p.staged = false;
        }
    }
}

impl<'mcx> SeqScanState<'mcx> {
    pub fn variant(&self) -> SeqScanVariant {
        self.variant
    }

    pub fn plan_node_id(&self) -> i32 {
        self.plan_node_id
    }

    /// The planner's output-row estimate for this scan (Plan.plan_rows —
    /// an ESTIMATE, only ever an admission-floor input, never semantics).
    pub fn plan_rows(&self) -> f64 {
        self.plan_rows
    }

    pub fn parallel_aware(&self) -> bool {
        self.parallel_aware
    }

    /// Parallel leader or worker. (The lane-v2 SeqScan drive now admits
    /// parallel scans — the batched page feed rides the shared DSM block
    /// cursor; kept for gates that still refuse parallel.)
    pub fn is_parallel(&self) -> bool {
        self.parallel_aware || self.parallel.is_some()
    }

    /// Forward, non-mark eflags at init (`ExecInitSeqScan`). False for a
    /// mergejoin-mark-armed scan (the scroll/backward eflags producer retired with the backward-execution wave, B2) — the lane-v2 page-batch
    /// drive is forward-only, so it refuses these.
    pub fn batch_allowed(&self) -> bool {
        self.batch_allowed
    }

    /// Lane-executor-v2 page-batch cursor `(pos, n)`: the drive lives in the
    /// `lanev2` module, this only stores its position across the Volcano
    /// per-call boundary.
    pub fn lane_cursor(&self) -> (u32, u32) {
        (self.lane_pos, self.lane_n)
    }

    pub fn set_lane_cursor(&mut self, pos: u32, n: u32) {
        self.lane_pos = pos;
        self.lane_n = n;
    }

    /// Memoized static lane fusibility verdict; `None` = not yet evaluated.
    pub fn lane_verdict(&self) -> Option<bool> {
        self.lane_verdict
    }

    pub fn set_lane_verdict(&mut self, v: bool) {
        self.lane_verdict = Some(v);
    }

    /// SE-R41 v2 cursor-fill pin posture (notes/se-r41-v2.md §3): the
    /// C-parity Volcano posture — the staged page batch and its `rs_cbuf`
    /// pin SURVIVE a budgeted-run suspension, exactly as C keeps a cursor's
    /// heap page pinned across FETCHes (and exactly as our own row-chain
    /// per-tuple walk already does mid-page). Set at `cursor_store_batch_fill`
    /// engagement; `seq_scan_cursor_settle` then refuses to park (the
    /// documented not-settleable C-parity class, widened deliberately to
    /// cursor-fill-owned scans), so the park→release→restage
    /// (`page_collect_tuples` re-walk) cycle — the measured ~19k-instr
    /// per-fill ceremony on deficit-1 fills — never runs. R3
    /// zero-pins-at-settle continues to bind for every LANE claim the walker
    /// parks (join-pipeline scans, SPI-flavor claims): this posture is
    /// scoped to the serial cursor store fill that owns its scan for the
    /// portal's lifetime.
    pub fn lane_hold_pin(&self) -> bool {
        self.lane_hold_pin
    }

    pub fn set_lane_hold_pin(&mut self) {
        self.lane_hold_pin = true;
    }

    /// Memoized standalone pgrcolumnar ownership verdict (see the field doc).
    pub fn cb_standalone_verdict(&self) -> Option<bool> {
        self.cb_standalone
    }

    pub fn set_cb_standalone_verdict(&mut self, v: bool) {
        self.cb_standalone = Some(v);
    }

    /// The memoized-false standalone verdict was the tiny-input floor's (the
    /// per-pull accounting attributes the refusal to the right reason).
    pub fn cb_standalone_tiny(&self) -> bool {
        self.cb_tiny
    }

    pub fn set_cb_standalone_tiny(&mut self) {
        self.cb_tiny = true;
    }

    pub fn release_parallel(&mut self) {
        self.parallel = None;
    }
}

impl<'mcx> ScanNode<'mcx> for SeqScanState<'mcx> {
    #[inline(always)]
    fn ss_mut(&mut self) -> &mut ScanState<'mcx> {
        &mut self.ss
    }

    /// `SeqRecheck`: seqscans have no access-method conditions to re-verify.
    #[inline(always)]
    fn epq_recheck(
        &mut self,
        _estate: &mut EStateData<'mcx>,
        _slot: ExecSlotId,
    ) -> PgResult<bool> {
        Ok(true)
    }

    /// `SeqNext`.
    #[inline(always)]
    fn scan_next(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<bool> {
        let mcx = estate.es_query_cxt;
        let direction = estate.es_direction;

        self.ensure_scandesc(estate)?;

        // SAFETY: written by ensure_scandesc when None; single test+branch
        // like C's scandesc == NULL check.
        let scandesc = unsafe { self.ss.ss_currentScanDesc.as_mut().unwrap_unchecked() };
        let slot = estate.slot_mut(self.ss.ss_ScanTupleSlot);
        table_scan_getnextslot(mcx, scandesc, direction, slot)
    }
}

impl<'mcx> SeqScanState<'mcx> {
    // Hot per-row check stays a single inlined test+branch (C's scandesc ==
    // NULL check); the once-per-scan open is outlined.
    #[inline(always)]
    fn ensure_scandesc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        if self.ss.ss_currentScanDesc.is_none() {
            self.open_scandesc(estate)?;
        }
        Ok(())
    }

    #[inline(never)]
    fn open_scandesc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        // GL-FIXCOUNT-2 — THE SCAN-DESCRIPTOR-OPEN CHOKEPOINT. This is the
        // only place a SeqScanState opens a PRIVATE (non-parallel) scan
        // descriptor: `table_beginscan` with no shared
        // `ParallelTableScanDescShared`, so the drive walks the WHOLE
        // relation instead of claiming through the shared cursor
        // (`phs_nallocated` / `claim_next_rg`).
        //
        // Doing that for a plan node the planner marked `parallel_aware`,
        // in an execution whose plan tree HAS been parallel-wired, means
        // every participant of a classic-parallel scan walks the whole
        // relation: each partial aggregate becomes the GLOBAL answer and
        // the finalize sums them — a result silently inflated by the
        // participant count, with no error anywhere. That is what a lane
        // arm which synthesizes a TWIN executor over the real scan plan
        // node produces (the twin's scan state never passes through
        // `exec_seq_scan_initialize_dsm`/`_worker`, so it holds no wiring
        // and lands here), and it is the route the GL-FIXCOUNT-1 morsel
        // gates cannot see: they gate the private MORSEL MAP, and this
        // route never asks for one.
        //
        // Refuse at CONSTRUCTION rather than detect at completion: a route
        // that cannot open the descriptor cannot produce numbers.
        // Release-effective by construction (an `Err`, never a
        // `debug_assert` — the profile the fleet runs is the profile that
        // has to fail closed; cf. the debug-assert masking law).
        //
        // `es_parallel_scan_wired` is what makes this precise rather than
        // merely conservative: an un-wired `parallel_aware` scan in an
        // execution where NO wiring ever happened is the legitimate
        // single-participant case (`gather_startup` skips
        // `exec_init_parallel_plan` when `es_use_parallel_mode` is false),
        // and a private descriptor is correct there. EPQ builds its own
        // scan states and never reaches a real drive.
        if self.parallel_aware
            && self.parallel.is_none()
            && estate.es_parallel_scan_wired
            && !matches!(self.variant, SeqScanVariant::Epq)
        {
            return Err(Box::new(::types_error::PgError::error(
                "private scan descriptor on a parallel-aware scan node".to_string(),
            )));
        }
        let mcx = estate.es_query_cxt;
        let snapshot = estate.es_snapshot.clone();
        self.ss.ss_currentScanDesc = Some(table_beginscan(
            mcx,
            self.ss.ss_currentRelation.as_ref().expect("seqscan has a relation"),
            snapshot,
            0,
            PgVec::new_in(mcx),
        )?);
        self.apply_cb_scan_settings();
        self.arm_slot_jit_deform(estate);
        Ok(())
    }

    // pgrcolumnar need-set + zone quals onto a freshly opened scan desc (serial
    // open_scandesc and both parallel init paths).
    fn apply_cb_scan_settings(&mut self) {
        if let Some(cb) = self.cb_scan.as_deref() {
            let sd = self.ss.ss_currentScanDesc.as_mut().unwrap();
            ::tableam::table_scan_set_needed_attrs(sd, &cb.needed);
            ::tableam::table_scan_push_zone_quals(sd, &cb.zone);
        }
    }

    // Rung 1 (per-row lazy path): arm the scan slot with a kernel sized to
    // what the scan actually fetches (qual + projection max_fetch; whole row
    // when absent or shape-unknown), clamped to the fixed prefix; 1-2-column
    // fetches stay on the interpreter (JIT_DEFORM_ROW_MIN_COLS).
    fn arm_slot_jit_deform(&mut self, estate: &mut EStateData<'mcx>) {
        let scandesc = self.ss.ss_currentScanDesc.as_ref().expect("armed after beginscan");
        let nblocks = ::tableam::table_scan_nblocks(scandesc);
        let rel = self.ss.ss_currentRelation.as_ref().expect("seqscan has a relation");
        let natts = rel.rd_att.natts;
        let mut need = 0i32;
        match self.ss.ps_ProjInfo.as_ref() {
            Some(p) => need = need.max(p.pi_state.max_fetch(::execexpr::SlotSrc::Scan).unwrap_or(natts)),
            None => need = natts,
        }
        if let Some(q) = self.ss.qual.as_deref() {
            need = need.max(q.max_fetch(::execexpr::SlotSrc::Scan).unwrap_or(natts));
        }
        let prefix = ::jit_deform::fixed_prefix(&rel.rd_att.compact_attrs);
        let ncols = prefix.min(need.max(0) as usize);
        if ncols < JIT_DEFORM_ROW_MIN_COLS {
            return;
        }
        let Some(k) = jit_deform_kernel(rel, ncols, nblocks, JIT_DEFORM_ROW_MIN_PAGES) else {
            return;
        };
        match estate.slot_mut(self.ss.ss_ScanTupleSlot) {
            SlotData::Heap(h) => h.jit_deform = Some(k),
            SlotData::BufferHeap(b) => b.base.jit_deform = Some(k),
            _ => {}
        }
    }
}

// Deform-JIT gates (docs/optimizations/jit-deform.md). Break-even vs the
// interpreted walk is <2 pages; gated with 2x margin. Both rungs share it
// since rung 3 removed the AOT column pass (the old 48-page batch gate
// priced JIT against AOT's ~23-page break-even). Relation-local page counts
// stand in for C's query-level jit_above_cost shape: a ~5us stencil install
// cannot use thresholds sized for ~ms LLVM compiles. C's jit +
// jit_tuple_deforming GUCs stay the kill switches.
const JIT_DEFORM_ROW_MIN_PAGES: u32 = 4;
const JIT_DEFORM_BATCH_MIN_PAGES: u32 = 4;
// Kernel + double-call overhead vs the warm inline walk crosses between 2
// and 3 fetched columns (v2 train: sort_limit need-3 -3.2%, distinct
// need-2 +1.3%).
const JIT_DEFORM_ROW_MIN_COLS: usize = 3;
// The floor survives the AOT removal: LLVM unswitches the generic fetch
// loop back to monomorphic shape (JIT-off A/B ran flat vs the AOT loops),
// so the kernel still loses below 4 columns (rung-3 first cut armed c=1
// hash-build and c=3 agg batches: joins +0.8%, group_agg +0.7%).
const JIT_DEFORM_BATCH_MIN_COLS: usize = 4;

fn jit_deform_kernel(
    rel: &Relation<'_>,
    ncols: usize,
    nblocks: u32,
    min_pages: u32,
) -> Option<std::rc::Rc<::jit_deform::DeformKernel>> {
    if ncols == 0 || nblocks < min_pages || !::jit_deform::available() {
        return None;
    }
    let jit_on = ::guc_tables::vars::jit_enabled.installed()
        && ::guc_tables::vars::jit_tuple_deforming.installed()
        && ::guc_tables::vars::jit_enabled.read()
        && ::guc_tables::vars::jit_tuple_deforming.read();
    if !jit_on || !relcache_seams::relation_get_deform_kernel::is_installed() {
        return None;
    }
    let k = relcache_seams::relation_get_deform_kernel::call(rel.rd_id, ncols as u16)?;
    // A held-but-rebuilt relation must never run the current entry's kernel.
    k.matches(&rel.rd_att).then_some(k)
}

/// Fused page-batch drive support (upstream batch scan, CF 6176). The caller
/// owns qual/projection evaluation and gates its own variant set.
pub fn seq_scan_batch_supported<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if matches!(node.variant, SeqScanVariant::Epq) {
        return Ok(false);
    }
    node.ensure_scandesc(estate)?;
    let scandesc = node.ss.ss_currentScanDesc.as_ref().unwrap();
    Ok(::tableam::table_scan_supports_pagebatch(scandesc))
}

/// As `seq_scan_batch_supported`, but also admits parallel scan descriptors
/// (the batched page feed routes block acquisition through the shared DSM
/// block cursor). Lane-v2 SeqScan drive only — the fused agg/sort/hash
/// drives keep the conservative serial-only gate.
pub fn seq_scan_batch_supported_parallel<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if matches!(node.variant, SeqScanVariant::Epq) {
        return Ok(false);
    }
    node.ensure_scandesc(estate)?;
    let scandesc = node.ss.ss_currentScanDesc.as_ref().unwrap();
    Ok(::tableam::table_scan_supports_pagebatch_parallel(scandesc))
}

/// Metadata-aggregate admission (lane-v2 metaagg arm): a BARE pgrcolumnar scan —
/// variant Plain (no qual, no projection), no zone-mappable quals — over an
/// AM that carries footer metadata. v1 requires literally no qual: a qual
/// (even one fully staged as zone quals) keeps the scan drive. Opens the
/// scan desc.
pub fn seq_scan_meta_agg_ok<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if node.variant != SeqScanVariant::Plain
        || node.ss.qual.is_some()
        || !node.cb_scan.as_deref().is_some_and(|cb| cb.zone.is_empty())
    {
        return Ok(false);
    }
    node.ensure_scandesc(estate)?;
    Ok(::tableam::table_scan_supports_meta_count(
        node.ss.ss_currentScanDesc.as_ref().unwrap(),
    ))
}

/// v7 zero-count meta-qual admission (the metaagg arm's qual extension):
/// the scan is a qual-ONLY pgrcolumnar scan (variant WithQual — no projection)
/// whose ENTIRE qual is the recognized `col <> 0` / `col = 0` conjunct
/// (cb_scan_info's semantic single-conjunct recognition). Opens the scan
/// desc; returns the qual for admission + the runtime meta call.
pub fn seq_scan_meta_zero_qual<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<::tableam::MetaZeroQual>> {
    let Some(zq) = node.cb_scan.as_deref().and_then(|cb| cb.zero_qual) else {
        return Ok(None);
    };
    if node.variant != SeqScanVariant::WithQual {
        return Ok(None);
    }
    node.ensure_scandesc(estate)?;
    if !::tableam::table_scan_supports_meta_count(node.ss.ss_currentScanDesc.as_ref().unwrap())
    {
        return Ok(None);
    }
    Ok(Some(zq))
}

/// Metadata MIN/MAX/COUNT/SUM one-shot answer; None = the scan drive owns it
/// (parallel scan, uncovered column type, a v<=6 part under a zero-count
/// qual, or a heap AM). Consumes no scan position.
pub fn seq_scan_meta_agg<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    cols: &[u16],
    sum_cols: &[u16],
    zq: Option<::tableam::MetaZeroQual>,
) -> PgResult<Option<::tableam::MetaAggScan>> {
    node.ensure_scandesc(estate)?;
    ::tableam::table_scan_meta_agg(
        node.ss.ss_currentScanDesc.as_ref().unwrap(),
        cols,
        sum_cols,
        zq,
    )
}

/// Arm SoA batch deform of the `prefix`-column prefix for the fused drive;
/// stays disarmed (per-row lazy deform) unless the prefix is all fixed-width.
/// `multi`: admit multi-clause kernel quals (AND of scan-Var-CMP-Const) to
/// the selection bitmap — lane-v2 callers only; the incumbent fused drives
/// pass false and keep their exact single-kernel admission.
pub fn seq_scan_batch_soa_prepare<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    prefix: i32,
    qual_only: bool,
    force: bool,
    multi: bool,
) {
    if let Some(b) = &node.batch_soa {
        // An armed PREWHERE lane owns this scan's staging: keep it whenever
        // its forced full-prefix deform covers the ask (qual-only and
        // narrower asks are subsumed — the owned bitmap serves them).
        if b.lane.is_some() && b.key_col.is_none() && b.plan.ncols() as i32 >= prefix {
            return;
        }
    }
    if prefix <= 0 {
        node.batch_soa = None;
        return;
    }
    if let Some(b) = &node.batch_soa {
        if b.plan.ncols() as i32 == prefix && b.qual_only == qual_only && b.key_col.is_none() {
            return;
        }
    }
    let mcx = estate.es_query_cxt;
    let rel = node.ss.ss_currentRelation.as_ref().expect("seqscan has a relation");
    let atts: &[_] = &rel.rd_att.compact_attrs;
    let qual = node
        .ss
        .qual
        .as_deref()
        .and_then(|q| q.scan_cmp_const_clauses())
        .filter(|c| {
            (multi || c.n == 1)
                && c.clauses[..c.n as usize].iter().all(|&(col, _, _)| (col as i32) < prefix)
        });
    // Break-even: at <=2 fixed columns the deform+gather double-copy loses to
    // the per-row walk (distinct +2.3% instr) unless a bitmap qual skips the
    // gather for non-survivors; group_agg's 3-column prefix wins -4.9%.
    if qual.is_none() && prefix < 3 && !force {
        node.batch_soa = None;
        return;
    }
    // AGGSEQ-STAGE walk-tail admission (fail-closed at every step): only
    // when the fixed-width proof refused, only for the lane fold/gagg
    // staging asks (`force && multi` — the incumbent fused drives pass
    // multi=false and keep their exact refusal), only on heap scans
    // (pgrcolumnar keeps its virtual/dict-group arms), only under the
    // default-OFF knob.
    let varwalk = force && multi && stage_varwalk_enabled() && seq_scan_is_heap(node);
    node.batch_soa = ::exectuples::SoaDeformPlan::try_new(mcx, atts, prefix as usize)
        .or_else(|| {
            if !varwalk {
                return None;
            }
            let p = ::exectuples::SoaDeformPlan::try_new_walk(mcx, atts, prefix as usize);
            if p.is_some() {
                lane_trace("seqscan varwalk staging armed (prefix crosses varlena; per-row walk tail)");
            }
            p
        })
        .map(|plan| {
        // Rung 2 (dense batch pass): the JIT batch kernel replaces the AOT
        // column loops on dense full-prefix deforms; col-only passes and
        // mixed batches keep the AOT/interpreted paths. Walk-tail plans
        // never arm (the kernel deforms the whole prefix at static offsets).
        let mut plan = plan;
        if plan.walk_from().is_none() && plan.ncols() as usize >= JIT_DEFORM_BATCH_MIN_COLS {
            if let Some(sd) = node.ss.ss_currentScanDesc.as_ref() {
                let rel = node.ss.ss_currentRelation.as_ref().expect("seqscan has a relation");
                if let Some(k) = jit_deform_kernel(
                    rel,
                    plan.ncols() as usize,
                    ::tableam::table_scan_nblocks(sd),
                    JIT_DEFORM_BATCH_MIN_PAGES,
                ) {
                    plan.arm_jit(k);
                }
            }
        }
        ::mcx::PgBox::new_in(
            BatchSoa {
                soa: ::exectuples::SoaBatch::new_in(mcx, plan.ncols()),
                plan,
                qual_armed: qual.is_some(),
                qual_only: qual_only && qual.is_some(),
                key_col: None,
                varkey: None,
                key_read_col: 0,
                publish: !(qual_only && qual.is_some()),
                quals: qual.map_or(
                    [(0, ::execexpr::CmpOp::Int4Eq, ::datum::Datum::null());
                        ::execexpr::SCAN_CMP_MAX_CLAUSES],
                    |c| c.clauses,
                ),
                nquals: qual.map_or(0, |c| c.n),
                contains: None,
                stitch: None,
                proj: None,
                lane: None,
                lane_requal: false,
                bits_only: false,
                dict_group: None,
                cond_armed: false,
                stage_cols: None,
                sel: [0; ::exectuples::SOA_BM_WORDS],
                nwords: 0,
                cur_word: 0,
                cur_bits: 0,
            },
            mcx,
        )
    });
}

/// `PGRUST_LANE_V2_STAGE_VARWALK` (default OFF; AGGSEQ-STAGE — the heap
/// grouped staging seam, R-KNOBS registry spelling): allow the lane
/// fold/gagg staging asks (`force && multi` callers only — the incumbent
/// fused drives pass `multi = false` and keep their exact refusal, so the
/// AGG_SEQ arm's own staging is byte-untouched) to stage a HEAP prefix
/// that CROSSES a varlena column, via the walk-tail plan
/// (`SoaDeformPlan::try_new_walk`): the maximal fixed-width head keeps
/// today's static column-major deform and columns at/past the first
/// varlena deform per row (`soa_deform_walk_tail` — deform_internal's
/// slow-lane discipline; varlena cells stage the same in-page pointer
/// Datums the per-row slot deform stores). OFF = the fixed-width-prefix
/// refusal stands byte-for-byte. AtomicU8 + `_set_for_tests` idiom
/// (rowmode.rs precedent) so units can A/B both worlds in one process.
static STAGE_VARWALK: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

fn stage_varwalk_enabled() -> bool {
    use std::sync::atomic::Ordering::Relaxed;
    match STAGE_VARWALK.load(Relaxed) {
        1 => false,
        2 => true,
        _ => {
            let on = matches!(
                std::env::var("PGRUST_LANE_V2_STAGE_VARWALK").as_deref(),
                Ok("1") | Ok("on")
            );
            STAGE_VARWALK.store(if on { 2 } else { 1 }, Relaxed);
            on
        }
    }
}

/// Same-process A/B lever for the unit corpus (`crate::tests`).
#[cfg(test)]
pub(crate) fn stage_varwalk_set_for_tests(on: bool) {
    STAGE_VARWALK.store(if on { 2 } else { 1 }, std::sync::atomic::Ordering::Relaxed);
}

/// Exact scan-column-set kill switch (A/B tooling): `PGRUST_CB_SCANCOLS=0`/
/// `off` makes `cb_scan_info` ignore `SeqScan::cb_scan_cols` and fall back to
/// the plan-tlist walk (the physical-tlist-inflated needed set). Default ON;
/// the lane GUC still gates the consumer.
fn cb_scan_cols_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_CB_SCANCOLS").as_deref(), Ok("0") | Ok("off"))
    })
}

/// PREWHERE v1 kill switch (A/B tooling): `PGRUST_LANE_V2_PREWHERE=0`/`off`
/// keeps pgrcolumnar lane quals on the kernel-bitmap/per-row paths. Default ON —
/// the master `PGRUST_LANE_V2` switch still gates every caller.
fn prewhere_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_LANE_V2_PREWHERE").as_deref(), Ok("0") | Ok("off"))
    })
}

/// PREWHERE v1 arm for a pgrcolumnar scan under the lane (phase4 design §3):
/// translate the scan qual fail-closed (`lane_scan_qual` walker ->
/// `translate_scan_qual`) into staged clauses (ascending static cost class,
/// pg_statistic-refined), the dict text tier, and the hybrid requal split;
/// arm the forced full-prefix SoA deform covering max(qual columns,
/// `min_prefix` — the feed's own column ask) and install the program on the
/// batch state. On success the staged window drive in
/// `seq_scan_next_pagebatch` owns the qual (zone folds + per-clause late
/// materialization + selection bitmap; requal survivors re-run the full
/// original qual at fetch) and granule decode goes lazy (per-column on
/// demand; `store_slot` completes the needed set for surviving rows only).
/// False = refused; the caller's heap-shaped arms proceed unchanged
/// (byte-safe either way). Idempotent: an armed lane whose prefix covers the
/// ask is kept.
#[cold]
pub fn seq_scan_cb_prewhere_arm<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    min_prefix: i32,
) -> PgResult<bool> {
    if node.cb_scan.is_none() || !prewhere_enabled() {
        return Ok(false);
    }
    if let Some(b) = node.batch_soa.as_deref() {
        if b.lane.is_some() {
            return Ok(b.plan.ncols() as i32 >= min_prefix);
        }
    }
    // Refusal memo: the qual is static per node — a refused translate must
    // not re-run (kernel builds, the regex probe compile) per feed event,
    // rescan, or memoized-standalone pull (the admission-attempt tax).
    if node.cb_prewhere_refused {
        return Ok(false);
    }
    let Some(q) = node.ss.qual.as_deref() else { return Ok(false) };
    let shape = match ::execexpr::lane_scan_qual(q) {
        Ok(s) => s,
        Err(reason) => {
            ::laneexec::log_refused(reason);
            node.cb_prewhere_refused = true;
            return Ok(false);
        }
    };
    // Dict text lanes are a pgrcolumnar capability (heap has no text SoA lane).
    let mut lq = match ::laneexec::translate_scan_qual(&shape, true) {
        Ok(lq) => lq,
        Err(reason) => {
            ::laneexec::log_refused(reason);
            node.cb_prewhere_refused = true;
            return Ok(false);
        }
    };
    node.ensure_scandesc(estate)?;
    // Prewhere clause order: refine the static cost classes with live
    // pg_statistic selectivity; a stats-free relation keeps the static
    // order (equality < range < LIKE). Static order only in v1 — the
    // observed-pass-rate re-refinement is deferred (phase4 design §3).
    let relid = node.ss.ss_currentRelation.as_ref().expect("seqscan has a relation").rd_id;
    lq.order_staged_with_stats(estate.es_query_cxt, relid);
    // Full-prefix deform (qual_only=false, forced): lane quals read several
    // columns and the lane sel skips the gather for non-survivors — the same
    // economics as the kernel bitmap. The prefix must also cover the feed's
    // own SoA reads (`min_prefix`); an uncoverable ask refuses wholesale so
    // the caller's arms rebuild their own staging.
    let prefix = (lq.max_attnum as i32 + 1).max(min_prefix);
    seq_scan_batch_soa_prepare(node, estate, prefix, false, true, true);
    if node.batch_soa.is_none() {
        // Text-qual staging (likeband): the fixed-width prefix refused — a
        // text column sits inside the qual prefix (the LIKE band's selective-text-qual
        // shape). That proof is a heap tuple-walk requirement only; the
        // pgrcolumnar window deform fills ANY column type per column
        // (`batch_deform_col` publishes decoded pointer Datums for text) and
        // the staged evaluators already consume them (dict lanes /
        // `eval_raw_rows` over the Raw pointer lane). Arm the same lane over
        // a VIRTUAL prefix plan carrying only the column count: this
        // function is unreachable for heap scans (`cb_scan` gate above), the
        // pgrcolumnar deform consumes exactly `ncols()`, and the slot publish is
        // the virtual-slot no-op — the missing offset chain is never walked.
        let mcx = estate.es_query_cxt;
        node.batch_soa =
            ::exectuples::SoaDeformPlan::virtual_prefix(mcx, prefix as usize).map(|plan| {
                ::mcx::PgBox::new_in(
                    BatchSoa {
                        soa: ::exectuples::SoaBatch::new_in(mcx, plan.ncols()),
                        plan,
                        qual_armed: true,
                        qual_only: false,
                        key_col: None,
                        varkey: None,
                        key_read_col: 0,
                        publish: true,
                        quals: [(0, ::execexpr::CmpOp::Int4Eq, ::datum::Datum::null());
                            ::execexpr::SCAN_CMP_MAX_CLAUSES],
                        nquals: 0,
                        contains: None,
                        stitch: None,
                        proj: None,
                        lane: None,
                        lane_requal: false,
                        bits_only: false,
                        dict_group: None,
                        cond_armed: false,
                        stage_cols: None,
                        sel: [0; ::exectuples::SOA_BM_WORDS],
                        nwords: 0,
                        cur_word: 0,
                        cur_bits: 0,
                    },
                    mcx,
                )
            });
    }
    match node.batch_soa.as_deref_mut() {
        Some(b) => {
            b.qual_armed = true;
            b.lane_requal = lq.requal;
            // Dict-lane arming: the AM's batch fill answers these columns as
            // codes+dict (zero decode); the dict tier evaluates on the memo
            // and the drive gathers survivors' lanes back to Raw for the
            // SoA-reading consumers (v1 — no dict-code-carrying consumer
            // exists yet).
            for c in lq.dict_cols() {
                b.soa.set_dict_want(c);
            }
            ::laneexec::log_compiled(lq.nclauses, lq.requal);
            if lq.ndict() > 0 {
                ::laneexec::log_dict_clauses(lq.ndict());
            }
            let cond_fp = lq.fingerprint;
            b.lane = Some(lq);
            // Post-qual materialization: granule decode per column on
            // demand — undeformed clauses' columns never decode, store_slot
            // completes the needed set on the first surviving row.
            let sd = node.ss.ss_currentScanDesc.as_mut().unwrap();
            ::tableam::table_scan_set_lazy_decode(sd, true);
            // Condition cache (pgrust.condition_cache, default OFF): arm the
            // scan with the staged prefix's canonical fingerprint so hot
            // re-executions serve window verdicts from memory. Requires a
            // cacheable prefix (fingerprint Some: deterministic and
            // non-erroring per clause — volatile/param/subplan quals never
            // translate, regex dict clauses refuse).
            if ::guc_tables::backing::pgrust_condition_cache() {
                if let Some(fp) = cond_fp {
                    let cap =
                        ::guc_tables::backing::pgrust_condition_cache_size().max(0) as u64 * 1024;
                    b.cond_armed = ::tableam::table_scan_condcache_arm(sd, fp, cap);
                    if b.cond_armed {
                        ::laneexec::log_condcache_armed();
                        lane_trace("cbstore condition cache armed");
                    }
                }
            }
            lane_trace("cbstore prewhere armed");
            Ok(true)
        }
        None => {
            // Residual staging refusal: only `virtual_prefix`'s u16 bound
            // can fail now (the fixed-width-prefix refusal died with the
            // virtual plan — likeband). Kept distinct so the gates can prove
            // the old reason no longer fires.
            ::laneexec::log_refused("qual prefix exceeds the staging bound");
            node.cb_prewhere_refused = true;
            Ok(false)
        }
    }
}

pub fn seq_scan_cb_dictgroup_arm<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    prefix: i32,
    key: u16,
) -> bool {
    seq_scan_cb_columnar_arm(node, estate, prefix, Some(key))
}

/// Whether a PREWHERE lane owns this scan's staged batch AND its forced
/// prefix covers `prefix` columns (the filtered grouped-distinct batch
/// feed's staging question: a covered live lane already fills every prefix
/// column for survivor windows — `lane_fill_wanted` is unmasked, dict-tier
/// qual columns gather back to Raw post-qual — so no further staging arm is
/// needed or legal; an uncovered/absent lane sends the caller to its own
/// columnar arm).
pub fn seq_scan_cb_lane_covers(node: &SeqScanState<'_>, prefix: i32) -> bool {
    node.batch_soa
        .as_deref()
        .is_some_and(|b| b.lane.is_some() && b.plan.ncols() as i32 >= prefix)
}

/// EXTRA dict-lane registration (band-2a CaseDict computed-key class): opt ANOTHER
/// column into dict lanes on the ALREADY-ARMED columnar batch — the CASE
/// source column, read per (epoch, code) beside the primary dict-group key.
/// Call with/after [`seq_scan_cb_columnar_arm`], before the first window
/// (the fill reads `dict_want` per window). `false` = no armed batch (the
/// caller refuses its feed; nothing was changed).
pub fn seq_scan_cb_dict_want_extra(node: &mut SeqScanState<'_>, c: u16) -> bool {
    match node.batch_soa.as_deref_mut() {
        Some(b) if (c as usize) < b.plan.ncols() as usize => {
            b.soa.set_dict_want(c);
            true
        }
        _ => false,
    }
}

/// The dict-group columnar arm generalized over the dict registration
/// (expr-key grouping tranche): `dict_key = None` arms the same offset-free
/// columnar staging with NO column opted into dict lanes — every window
/// fills decoded Datums (the expr-key ARITH class over pgrcolumnar, whose
/// grouping-key inputs may sit past varlena columns the heap fixed-width
/// prefix plan refuses). `Some(key)` is `seq_scan_cb_dictgroup_arm` exactly.
pub fn seq_scan_cb_columnar_arm<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    prefix: i32,
    dict_key: Option<u16>,
) -> bool {
    if node.cb_scan.is_none() || prefix <= 0 {
        return false;
    }
    if let Some(key) = dict_key {
        if (key as i32) >= prefix {
            return false;
        }
    }
    if let Some(b) = node.batch_soa.as_deref_mut() {
        // Idempotent: a matching armed batch is kept (a dict-free ask is
        // served by ANY covering staging — the dict registration, if one
        // exists, belongs to a co-resident consumer).
        if b.plan.ncols() as i32 >= prefix && b.dict_group == dict_key {
            return true;
        }
        if b.key_col.is_some() || b.varkey.is_some() {
            return false;
        }
        // A live PREWHERE lane owns the batch: register the dict-group
        // consumer on it when its forced full prefix covers the ask (the
        // fill answers the key as codes+dict from the next window on; the
        // gather-to-Raw skip keeps the codes up past the qual).
        if b.lane.is_some() && b.plan.ncols() as i32 >= prefix {
            if let Some(key) = dict_key {
                b.soa.set_dict_want(key);
                b.dict_group = Some(key);
            }
            return true;
        }
    }
    let mcx = estate.es_query_cxt;
    let Some(plan) = ::exectuples::SoaDeformPlan::columnar(mcx, prefix as usize) else {
        return false;
    };
    let mut soa = ::exectuples::SoaBatch::new_in(mcx, plan.ncols());
    if let Some(key) = dict_key {
        soa.set_dict_want(key);
    }
    node.batch_soa = Some(::mcx::PgBox::new_in(
        BatchSoa {
            soa,
            plan,
            qual_armed: false,
            qual_only: false,
            key_col: None,
            varkey: None,
            key_read_col: 0,
            publish: false,
            quals: [(0, ::execexpr::CmpOp::Int4Eq, ::datum::Datum::null());
                ::execexpr::SCAN_CMP_MAX_CLAUSES],
            nquals: 0,
            contains: None,
            stitch: None,
            proj: None,
            lane: None,
            lane_requal: false,
            bits_only: false,
            dict_group: dict_key,
            cond_armed: false,
            stage_cols: None,
            sel: [0; ::exectuples::SOA_BM_WORDS],
            nwords: 0,
            cur_word: 0,
            cur_bits: 0,
        },
        mcx,
    ));
    lane_trace(if dict_key.is_some() {
        "cbstore dict-group staging armed"
    } else {
        "cbstore columnar staging armed"
    });
    true
}

/// Materialize a dict-answered column's lane into its Raw Datum cells for the
/// CURRENT staged batch (`SoaBatch::gather_dict_lane` — byte-identical to the
/// filler's own Raw fill), clearing the lane. The expr-key feed calls this
/// AFTER its per-code key derivation so fold/resid consumers can read the
/// same column's decoded values. No-op when the window answered Raw.
#[inline]
pub fn seq_scan_batch_gather_dict(node: &mut SeqScanState<'_>, c: usize) {
    if let Some(sd) = node.ss.ss_currentScanDesc.as_mut() {
        if let Some(b) = node.batch_soa.as_deref_mut() {
            // The expr-key feed's consumers read SELECTED rows only
            // (`xk.rows` off the armed bitmap) — the PREWHERE stale-cell
            // contract; narrow lazy dict ensures to them when armed.
            let sel = (b.qual_armed && !b.lane_requal && b.nwords > 0)
                .then(|| &b.sel[..b.nwords as usize]);
            gather_or_len(sd, &mut b.soa, c, sel);
        }
    }
}

/// Post-qual materialization of a dict-tier qual column: length-armed
/// columns re-answer their SoA lane as lengths straight off the scan-side
/// decode state (per-code length table on dict windows, header read / C mb
/// walk on Raw windows — the clause's fallback eval read the datums BEFORE
/// this), everything else gathers `dict[code]` to Raw exactly as before.
#[inline]
fn gather_or_len(
    scandesc: &mut ::tableam::TableScanDesc<'_>,
    soa: &mut ::exectuples::SoaBatch<'_>,
    c: usize,
    sel: Option<&[u64]>,
) {
    match soa.len_want(c) {
        0 => match sel {
            // PREWHERE stale-cell contract: lazy sub-framed dicts ensure
            // selected rows only (identical cell writes).
            Some(sel) => soa.gather_dict_lane_sel(c, sel),
            None => soa.gather_dict_lane(c),
        },
        k => {
            ::tableam::table_scan_batch_fill_len(
                scandesc,
                c as u16,
                k == ::exectuples::LEN_WANT_CHARS,
                soa,
            );
            soa.clear_dict_lane(c);
        }
    }
}

/// Arm column `c` of the staged batch as a fold LENGTH lane (lane-v2-
/// asciilen): the pgrcolumnar fill answers the column's values cells as
/// `Datum::from_i64(length)` — per-dict-code table on dict chunks, header
/// read / C mb-walk on Raw chunks — instead of varlena datum pointers.
/// `chars` = UTF-8 character length (the caller's classify admitted the
/// encoding), else octet length.
///
/// Refuses (false) whenever any co-consumer reads the column's Datum cells
/// as datums: kernel quals / contains / stitched tiers / projections own
/// lanes (conservatively: any of them armed), the column is a varkey or a
/// key/dict-group column, a lane-less qual owns the scan, or the PREWHERE
/// lane reads it raw (dict-tier clauses are fine: the post-qual gather
/// re-answers the lane as lengths off the scan-side decode state, both
/// kinds). The fold and guard reads consult
/// the SAME batch flag the fill honors, so a refusal (or a later batch
/// rebuild) byte-safely keeps the datum-lane path.
pub fn seq_scan_batch_len_want(node: &mut SeqScanState<'_>, c: u16, chars: bool) -> bool {
    let has_qual = node.ss.qual.is_some();
    let Some(b) = node.batch_soa.as_deref_mut() else {
        return false;
    };
    let want = if chars {
        ::exectuples::LEN_WANT_CHARS
    } else {
        ::exectuples::LEN_WANT_BYTES
    };
    match b.soa.len_want(c as usize) {
        0 => {}
        k => return k == want, // idempotent re-arm of the same ask only
    }
    if b.key_col.is_some()
        || b.varkey.is_some()
        || b.dict_group == Some(c)
        || b.stitch.is_some()
        || b.proj.is_some()
        || b.contains.is_some()
        || b.nquals != 0
    {
        return false;
    }
    match b.lane.as_deref() {
        Some(lq) => {
            if lq.reads_col_raw(c) {
                return false;
            }
        }
        // A qual not owned by the PREWHERE lane evaluates through paths this
        // seam cannot audit — refuse (the datum path stays correct).
        None if has_qual => return false,
        None => {}
    }
    b.soa.set_len_want(c, want);
    true
}

/// Dict-code side channel for a str MIN/MAX fold column of the CURRENT
/// staged batch (lane-v2-dictminmax): the staged window's u32 codes + the
/// per-RG dictionary identity, valid until the next window stages. `Some`
/// certifies the `lanefold::LaneCols::col_codes` contract half this seam can
/// audit: the column's SoA values cells hold REAL datums for the window's
/// selected rows (no still-up dict-lane answer, no length staging — both
/// leave the cells stale or integer-valued), and on a dict window every
/// datum cell was gathered as `dict[code]` (the pgrcolumnar fill's only Raw
/// path for a dict chunk), so values[i] is pointer-identical to
/// `table.datum(code(i))`. Raw (non-dict) windows and heap scans answer
/// `None` — the fold keeps its datum memcmp path, byte-identically.
#[inline]
pub fn seq_scan_batch_dict_codes(
    node: &SeqScanState<'_>,
    c: usize,
) -> Option<::exectuples::SoaDictLane> {
    let sd = node.ss.ss_currentScanDesc.as_ref()?;
    let b = node.batch_soa.as_deref()?;
    if b.soa.dict_lane(c).is_some() || b.soa.len_want(c) != 0 {
        return None;
    }
    ::tableam::table_scan_batch_dict_codes(sd, c as u16)
}

/// Column-independent half of `seq_scan_refsort_key_batch`'s certification:
/// `(fallback_words, sel_words)` for the CURRENT staged batch — the exact
/// whole-qual selection verdict plus the forced-fallback mask, with NO
/// claim about any column's datum cells (the DictCode sort-key class reads
/// its observations from the dict-code side channel, never from SoA datum
/// cells — docs/design/dict-code-flow.md inc-1). `sel` soundness is exactly
/// the key-batch accessor's: `Some` iff the scan HAS a qual and the armed
/// bitmap is the WHOLE qual's verdict (no hybrid requal tail); `None` with
/// no qual = every staged row survives; a qual-bearing batch without an
/// exact bitmap refuses wholesale. Forced-fallback rows carry SET sel bits
/// and must take the per-row emit (or the caller's own fail-closed path).
pub fn seq_scan_refsort_batch_masks<'a, 'mcx>(
    node: &'a SeqScanState<'mcx>,
    n: u32,
) -> Option<(&'a [u64], Option<&'a [u64]>)> {
    let b = node.batch_soa.as_deref()?;
    let sel = if node.ss.qual.is_some() {
        if !(b.qual_armed && !b.lane_requal && b.nwords > 0) {
            return None;
        }
        Some(&b.sel[..])
    } else {
        None
    };
    if b.soa.nrows() < n {
        return None;
    }
    Some((b.soa.fallback_words(), sel))
}

/// `seq_scan_batch_dict_codes` with the v7 part-global stitch published
/// when the scan carries one — the DictCode sort-key side channel
/// (docs/design/dict-code-flow.md inc-1). Same audit guards as the
/// per-epoch accessor (no still-up dict-lane answer, no length staging);
/// consumers gate order use on `table.has_stitch()` and fail closed
/// otherwise.
#[inline]
pub fn seq_scan_batch_dict_codes_global(
    node: &mut SeqScanState<'_>,
    c: usize,
) -> Option<::exectuples::SoaDictLane> {
    {
        let b = node.batch_soa.as_deref()?;
        if b.soa.dict_lane(c).is_some() || b.soa.len_want(c) != 0 {
            return None;
        }
    }
    let sd = node.ss.ss_currentScanDesc.as_mut()?;
    ::tableam::table_scan_batch_dict_codes_global(sd, c as u16)
}

/// Physical rowref base of the CURRENT staged batch (tie-ordering rule 2,
/// the zone-adaptive rowref-selection sort feed): staged row `i`'s rowref is
/// `base + i` — the SoA batch stages exactly the scan's current staged
/// window, so batch indices ARE window row offsets. `None` for heap scans or
/// when nothing is staged; the rowref-armed consumer then demotes.
#[inline]
pub fn seq_scan_batch_rowref_base(node: &SeqScanState<'_>) -> Option<u64> {
    let sd = node.ss.ss_currentScanDesc.as_ref()?;
    node.batch_soa.as_deref()?;
    ::tableam::table_scan_batch_rowref_base(sd)
}

/// The registered dict-group consumer column, when the dict-group arm (or a
/// PREWHERE co-arm) holds. The agg feed re-checks this per build — a rebuilt
/// batch (a later consumer re-armed the staging) drops the registration and
/// the feed falls back to the Raw key path, byte-safely.
#[inline]
pub fn seq_scan_batch_dictgroup_col(node: &SeqScanState<'_>) -> Option<u16> {
    node.batch_soa.as_deref().and_then(|b| b.dict_group)
}

/// Arm K1 inc-2 late-materialization staging (wave-9 WS-AH) on this node's
/// armed heap batch: narrow the staging deform's kind-0 column pass to
/// {qual clause cols ∪ `key_cols`} and return the DEFERRED column set
/// (`[0, prefix) \ staged` — the completion set the drain passes to
/// `seq_scan_batch_complete_deform` per batch, over the qual-survivor
/// bitmap). The deferred set is the FULL remaining prefix, not just the
/// fold's needed columns: the per-row emit publishes every prefix cell of a
/// selected row (`soa_store_prefix`), so anything less would publish stale
/// cells on the arrival/fallback legs (rail B: value movement only).
///
/// Admission (each refusal NAMED for the M5-1 funnel, returned to the
/// caller to tick):
/// - `k1-latemat-no-qual` — no armed whole-qual kernel bitmap (rail J: the
///   no-qual all-columns shapes keep today's single JIT full deform), a
///   hybrid requal tail, or a qual-col-only staging (nothing to defer past
///   the qual's own column selection);
/// - `k1-latemat-shape` — the staging is not the plain heap kernel-qual
///   prefix shape (varkey / contains / PREWHERE lane / key-col redirect /
///   stitched projection / bits-only census / virtual plan / non-heap AM);
/// - `k1-latemat-varwalk` — walk-tail staging (AGGSEQ-STAGE): the
///   completion pass indexes the static offset chain (head-only on walk
///   plans); survivor-only tail completion is the recorded follow-up;
/// - `k1-latemat-all-staged` — the staged set already covers the prefix
///   (narrowing must defer something).
///
/// The narrowing is per-BUILD state: callers disarm + re-decide every build
/// (`seq_scan_k1_latemat_disarm`); rescans rebuild through the same drains.
pub fn seq_scan_k1_latemat_arm(
    node: &mut SeqScanState<'_>,
    key_cols: &[u16],
) -> Result<Vec<u16>, &'static str> {
    if !seq_scan_is_heap(node) {
        return Err("k1-latemat-shape");
    }
    let Some(b) = node.batch_soa.as_deref_mut() else { return Err("k1-latemat-shape") };
    if b.plan.is_virtual()
        || b.key_col.is_some()
        || b.varkey.is_some()
        || b.contains.is_some()
        || b.lane.is_some()
        || b.proj.is_some()
        || b.bits_only
    {
        return Err("k1-latemat-shape");
    }
    if !b.qual_armed || b.lane_requal || b.qual_only || b.nquals == 0 {
        return Err("k1-latemat-no-qual");
    }
    // AGGSEQ-STAGE: walk-tail stagings refuse the latemat split — the
    // completion machinery (`soa_deform_columns_set`) indexes the static
    // offset chain, which covers only a walk plan's fixed head, and the
    // walk tail is one sequential pass per row anyway (nothing narrows).
    // Survivor-only tail completion is the recorded follow-up.
    if b.plan.walk_from().is_some() {
        return Err("k1-latemat-varwalk");
    }
    let ncols = b.plan.ncols();
    if key_cols.iter().any(|&c| c >= ncols) {
        return Err("k1-latemat-shape");
    }
    let mut staged: Vec<u16> =
        b.quals[..b.nquals as usize].iter().map(|&(c, _, _)| c).collect();
    for &k in key_cols {
        if !staged.contains(&k) {
            staged.push(k);
        }
    }
    let complete: Vec<u16> = (0..ncols).filter(|c| !staged.contains(c)).collect();
    if complete.is_empty() {
        return Err("k1-latemat-all-staged");
    }
    staged.sort_unstable();
    b.stage_cols = Some(staged);
    Ok(complete)
}

/// Drop the K1 late-materialization narrowing (per-build re-decision; also
/// the unit levers' reset). The NEXT staged batch returns to the full
/// staging deform; the CURRENT batch's cells are untouched.
pub fn seq_scan_k1_latemat_disarm(node: &mut SeqScanState<'_>) {
    if let Some(b) = node.batch_soa.as_deref_mut() {
        b.stage_cols = None;
    }
}

/// Whether the K1 late-materialization narrowing is armed (unit pins).
pub fn seq_scan_k1_latemat_armed(node: &SeqScanState<'_>) -> bool {
    node.batch_soa.as_deref().is_some_and(|b| b.stage_cols.is_some())
}

/// K1 inc-2 completion (pass B): fill `cols` for `sel`-selected kind-0 rows
/// of the CURRENT staged batch off the still-pinned page (ownership ABI R3
/// — valid until the next batch advance/reposition/settle). Word-skips
/// all-zero 64-row selection words; kind-1 rows were deformed at classify
/// and kind-2 fallback rows never fill (their bits, if OR'd into the qual
/// bitmap, are harmless). Value movement only — idempotent per (col, row).
pub fn seq_scan_batch_complete_deform(node: &mut SeqScanState<'_>, cols: &[u16], sel: &[u64]) {
    let SeqScanState { ss, batch_soa, .. } = node;
    let Some(b) = batch_soa.as_deref_mut() else { return };
    let Some(sd) = ss.ss_currentScanDesc.as_mut() else { return };
    ::tableam::table_scan_batch_complete_deform(sd, &b.plan, &mut b.soa, cols, sel);
}

/// SE-T2AGG CAR A (plain SELECT DISTINCT): arm the direct key feed at an
/// EXPLICIT scan output column — the unprojected physical-tlist shape
/// (`Agg(AGG_HASHED) → SeqScan` keeps the scan unprojected, so the key is
/// an arbitrary output column, not column 0). Same laws as
/// [`seq_scan_sortkey_direct`] otherwise: no qual, no projection (a
/// projection re-orders output columns — the single-column shapes keep the
/// sortkey resolution), `arm_key_soa`'s own fixed-width / varkey /
/// pgrcolumnar-columnar ladder decides stageability. False leaves the
/// per-row path.
pub fn seq_scan_key_direct_att<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    attnum: u16,
) -> bool {
    if node.ss.qual.is_some() || node.ss.ps_ProjInfo.is_some() {
        return false;
    }
    let rel = node.ss.ss_currentRelation.as_ref().expect("seqscan has a relation");
    if (attnum as i32) >= i32::from(rel.rd_att.natts) {
        return false;
    }
    arm_key_soa(node, estate, attnum)
}

/// Arm the fused-sort direct key feed: output column 0 must be exactly one
/// scan Var (bare single-column scan or a lone `JustAssignVar` projection)
/// the fixed-width SoA plan covers, no qual. False leaves the per-row path.
pub fn seq_scan_sortkey_direct<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> bool {
    if node.ss.qual.is_some() {
        return false;
    }
    let rel = node.ss.ss_currentRelation.as_ref().expect("seqscan has a relation");
    let attnum = match node.ss.ps_ProjInfo.as_ref() {
        None if rel.rd_att.natts == 1 => 0u16,
        None => return false,
        Some(p) => match p.pi_state.kernel() {
            ::execexpr::Kernel::JustAssignVar {
                src: ::execexpr::SlotSrc::Scan,
                attnum,
                resultnum: 0,
            } => attnum,
            _ => return false,
        },
    };
    arm_key_soa(node, estate, attnum)
}

/// Shared key-column staging body (fused-sort direct key feed + the lane's
/// top-k cutoff pre-filter): arm a key-only `BatchSoa` (publish off, no qual)
/// staging scan column `attnum` per page batch — the fixed-width prefix
/// deform when the plan covers it, else the varlena key pass. Idempotent when
/// the same key is already armed; refuses (false) rather than disturb a
/// `BatchSoa` armed for anything else (kernel qual bitmap / stitch /
/// different key), since `seq_scan_next_pagebatch`'s column-selection rule
/// stages exactly one consumer's columns.
fn arm_key_soa<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    attnum: u16,
) -> bool {
    if let Some(b) = &node.batch_soa {
        return b.key_col == Some(attnum);
    }
    let rel = node.ss.ss_currentRelation.as_ref().expect("seqscan has a relation");
    let mcx = estate.es_query_cxt;
    let atts: &[_] = &rel.rd_att.compact_attrs;
    let (plan, varkey) =
        match ::exectuples::SoaDeformPlan::try_new(mcx, atts, attnum as usize + 1) {
            Some(plan) => (plan, None),
            None => match ::exectuples::SoaVarKeyPlan::try_new(atts, attnum as usize) {
                Some(vk) => (::exectuples::SoaDeformPlan::unused(mcx), Some(vk)),
                None => {
                    // pgrcolumnar columnar key staging (the int-key-distinct refusal): a
                    // FIXED-WIDTH key sitting past a varlena column — the
                    // heap fixed-width-prefix proof refuses and the varkey
                    // pass wants a varlena key. The pgrcolumnar window deform
                    // fills per column with no offset chain (the virtual
                    // plan, likeband precedent), and a keyed batch stages
                    // ONLY the key column (`.or(b.key_col)` at
                    // `seq_scan_next_pagebatch`'s tail) — one decoded key
                    // lane per window. Heap scans keep the refusal (their
                    // deform walks the offset chain).
                    if node.cb_scan.is_none() {
                        return false;
                    }
                    match ::exectuples::SoaDeformPlan::columnar(mcx, attnum as usize + 1) {
                        Some(plan) => (plan, None),
                        None => return false,
                    }
                }
            },
        };
    let soa_cols = if varkey.is_some() { 1 } else { plan.ncols() };
    let key_read_col = if varkey.is_some() { 0 } else { attnum };
    node.batch_soa = Some(::mcx::PgBox::new_in(
        BatchSoa {
            soa: ::exectuples::SoaBatch::new_in(mcx, soa_cols),
            plan,
            qual_armed: false,
            qual_only: false,
            key_col: Some(attnum),
            varkey,
            key_read_col,
            publish: false,
            quals: [(0, ::execexpr::CmpOp::Int4Eq, ::datum::Datum::null());
                ::execexpr::SCAN_CMP_MAX_CLAUSES],
            nquals: 0,
            contains: None,
            stitch: None,
            proj: None,
            lane: None,
            lane_requal: false,
            bits_only: false,
                dict_group: None,
            cond_armed: false,
            stage_cols: None,
            sel: [0; ::exectuples::SOA_BM_WORDS],
            nwords: 0,
            cur_word: 0,
            cur_bits: 0,
        },
        mcx,
    ));
    true
}

/// Arm the staged key lane for the lane sort breaker's streaming top-k
/// cutoff: stage scan column `attnum` (the sort's leading key, resolved by
/// the lane) per page batch so the pre-filter can compare a whole staged
/// batch against the tuplesort's k-th boundary vectorized. Requires a
/// qual-less scan (the pre-filter skips rows without running their emit
/// body, so no per-row evaluation may be observable) and a free or matching
/// `BatchSoa`. False = not stageable; the sort feed proceeds unfiltered.
pub fn seq_scan_topk_key_arm<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    attnum: u16,
) -> bool {
    if node.ss.qual.is_some() {
        return false;
    }
    arm_key_soa(node, estate, attnum)
}

/// Arm zone-ordered adaptive granule traversal for a bounded-sort (top-N)
/// feed: the pgrcolumnar scan visits granules by the sort key's zone bound
/// (`min` ascending for ASC, `max` descending for DESC) and stops once the
/// consumer-fed boundary strictly dominates the next bound
/// (docs/design/pgrcolumnar-zone-adaptive.md). `attnum` is the 0-based scan
/// column of the sort's LEADING key.
///
/// Skipped granules elide their rows' per-row qual evaluation and emit body,
/// so admission requires both to be observation-free: no scan qual at all,
/// or a staged whole-qual kernel bitmap (`qual_armed && !lane_requal` — the
/// kernel/stitch/PREWHERE vocabularies are non-erroring, volatile-free
/// comparisons by construction; hybrid-requal feeds re-run the full
/// error-capable qual per survivor and are refused). The projection side is
/// the caller's admission (pure-Var shapes only, the topk-cut resolution).
/// The AM arm itself refuses parallel scans, non-pgrcolumnar AMs, text keys and
/// non-exact zone encodings. False = not armed; the physical-order feed
/// proceeds untouched.
pub fn seq_scan_adaptive_topk_arm<'mcx>(
    node: &mut SeqScanState<'mcx>,
    attnum: u16,
    desc: bool,
) -> PgResult<bool> {
    if node.ss.qual.is_some()
        && !node.batch_soa.as_deref().is_some_and(|b| b.qual_armed && !b.lane_requal)
    {
        return Ok(false);
    }
    let Some(scan) = node.ss.ss_currentScanDesc.as_mut() else {
        return Ok(false);
    };
    table_scan_arm_adaptive_order(scan, attnum as usize, desc, /* strict */ false)
}

/// Consumer bound feedback for an armed adaptive traversal: the bounded
/// sort's current k-th boundary LEADING-key datum (by-value; the lane admits
/// int-family keys only). No-op when no adaptive order is armed.
#[inline]
pub fn seq_scan_adaptive_push_bound(node: &mut SeqScanState<'_>, key: ::datum::Datum) {
    if let Some(scan) = node.ss.ss_currentScanDesc.as_mut() {
        table_scan_update_scan_bound(scan, key);
    }
}

/// Demote an armed adaptive traversal back to the physical-order drive and
/// rescan (the zone-adaptive feed observed an arrival-order-sensitive
/// boundary tie): disarm at the AM, then `exec_rescan_seq_scan` so the
/// re-feed restages from row zero in physical claim order.
pub fn seq_scan_adaptive_disarm_rescan<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if let Some(scan) = node.ss.ss_currentScanDesc.as_mut() {
        table_scan_disarm_adaptive_order(scan);
    }
    exec_rescan_seq_scan(node, estate)
}

/// The staged key lane of the CURRENT page batch for the top-k cutoff
/// pre-filter: `(values, isnull, fallback_words)` slices over the first `n`
/// staged rows (fallback bits mark rows the deform skipped — narrow tuples —
/// which the pre-filter must pass through). `None` = lane not armed or the
/// staging did not cover this batch; the caller feeds unfiltered.
#[inline]
pub fn seq_scan_topk_key_lane<'a, 'mcx>(
    node: &'a SeqScanState<'mcx>,
    n: u32,
) -> Option<(&'a [::datum::Datum], &'a [bool], &'a [u64])> {
    let b = node.batch_soa.as_deref()?;
    b.key_col?;
    if b.soa.nrows() < n {
        return None;
    }
    let c = b.key_read_col as usize;
    Some((
        &b.soa.col_values(c)[..n as usize],
        &b.soa.col_isnull(c)[..n as usize],
        b.soa.fallback_words(),
    ))
}

/// Staged pgrcolumnar window base for ref-carrying consumers (the lane refsort
/// feed): (row group, rg-global row index of staged row 0); the ref of
/// staged row `i` is `base + i`, resolvable via `seq_scan_gather_row` for
/// the scan's life. `None` = heap AM or nothing staged.
#[inline]
pub fn seq_scan_batch_window_ref(node: &SeqScanState<'_>) -> Option<(u32, u32)> {
    let sd = node.ss.ss_currentScanDesc.as_ref()?;
    ::tableam::table_scan_window_ref(sd)
}

/// Materialize rg-global `row` of row group `rg` into the scan tuple slot
/// under the scan's CURRENT needed set (unneeded cells null) — the refsort
/// winner gather. Uses the AM's gather scratch; the staged window is
/// untouched. `false` = unsupported AM / no open scan (the caller demotes).
pub fn seq_scan_gather_row<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    rg: u32,
    row: u32,
) -> bool {
    let Some(sd) = node.ss.ss_currentScanDesc.as_mut() else {
        return false;
    };
    let slot = estate.slot_mut(node.ss.ss_ScanTupleSlot);
    ::tableam::table_scan_gather_row(sd, rg, row, slot)
}

/// The refsort feed's fast-leg view of the CURRENT staged batch:
/// `(key_values, key_isnull, fallback_words, sel_words)` over the first `n`
/// staged rows for scan column `col` (0-based), or `None` when any part is
/// unavailable — the caller then routes every row through the per-row emit
/// (byte-identical fallback). Soundness:
///   * `sel_words` is `Some` iff the scan HAS a qual and the armed selection
///     bitmap is the WHOLE qual's verdict (kernel/PREWHERE-owned, no hybrid
///     requal tail); `None` with no qual = every staged row survives. A
///     qual-bearing batch without an exact bitmap refuses wholesale.
///   * The key column's staged datum cells must be valid for selected
///     non-fallback rows: the whole-prefix deform (`!qual_only`, no foreign
///     `key_col` redirect, no varkey pointer lane) or the dedicated
///     key-column staging on exactly this column, and `col_datum_ready`
///     (no unanswered dict lane, fill not skipped by a lane-read mask).
///   * Forced-fallback rows carry a SET sel bit and stale cells — callers
///     MUST route them through the per-row emit (exact re-check + C detoast).
pub fn seq_scan_refsort_key_batch<'a, 'mcx>(
    node: &'a SeqScanState<'mcx>,
    col: u16,
    n: u32,
) -> Option<(&'a [::datum::Datum], &'a [bool], &'a [u64], Option<&'a [u64]>)> {
    let b = node.batch_soa.as_deref()?;
    let sel = if node.ss.qual.is_some() {
        if !(b.qual_armed && !b.lane_requal && b.nwords > 0) {
            return None;
        }
        Some(&b.sel[..])
    } else {
        None
    };
    if b.varkey.is_some() {
        return None;
    }
    match b.key_col {
        Some(k) if k == col => {}
        Some(_) => return None,
        None => {
            if b.qual_only {
                return None;
            }
        }
    }
    let c = col as usize;
    if c >= b.soa.ncols() as usize || !b.soa.col_datum_ready(c) || b.soa.nrows() < n {
        return None;
    }
    Some((
        &b.soa.col_values(c)[..n as usize],
        &b.soa.col_isnull(c)[..n as usize],
        b.soa.fallback_words(),
        sel,
    ))
}

/// Arm the varlena lane feed for the lane-v2 agg fold: stage per-row datum
/// pointers to varlena column `attnum` into SoA column 0 via the varkey pass
/// (the fixed-width prefix deform cannot host an `attlen == -1` column).
/// Publish stays off — the fold feed stores every emitted row per-row, so
/// slot deform semantics are untouched. False = the column's tuple walk is
/// not stageable (an `attlen == -2` attribute precedes it); the caller keeps
/// its per-row path.
pub fn seq_scan_batch_soa_prepare_varlane<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    attnum: u16,
) -> bool {
    if let Some(b) = &node.batch_soa {
        if b.key_col == Some(attnum) && b.varkey.is_some() {
            return true;
        }
    }
    let mcx = estate.es_query_cxt;
    let rel = node.ss.ss_currentRelation.as_ref().expect("seqscan has a relation");
    let atts: &[_] = &rel.rd_att.compact_attrs;
    let Some(vk) = ::exectuples::SoaVarKeyPlan::try_new(atts, attnum as usize) else {
        return false;
    };
    node.batch_soa = Some(::mcx::PgBox::new_in(
        BatchSoa {
            soa: ::exectuples::SoaBatch::new_in(mcx, 1),
            plan: ::exectuples::SoaDeformPlan::unused(mcx),
            qual_armed: false,
            qual_only: false,
            key_col: Some(attnum),
            varkey: Some(vk),
            key_read_col: 0,
            publish: false,
            quals: [(0, ::execexpr::CmpOp::Int4Eq, ::datum::Datum::null());
                ::execexpr::SCAN_CMP_MAX_CLAUSES],
            nquals: 0,
            contains: None,
            stitch: None,
            proj: None,
            lane: None,
            lane_requal: false,
            bits_only: false,
            dict_group: None,
            cond_armed: false,
            stage_cols: None,
            sel: [0; ::exectuples::SOA_BM_WORDS],
            nwords: 0,
            cur_word: 0,
            cur_bits: 0,
        },
        mcx,
    ));
    true
}

/// SE-T2AGG CAR B (string-min/max engagement fix): shed the fold feed's own
/// bare varlena REMAP staging (`seq_scan_batch_soa_prepare_varlane`'s exact
/// shape: `key_col == attnum`, varkey pass, NO qual arming, NO co-resident
/// consumer — lane/stitch/contains/proj/dict all absent) so the caller can
/// arm the full-prefix columnar deform in its place. The remap staging hosts
/// ONE column at SoA index 0 and cannot widen, while the runtime agg sink's
/// drains read DIRECT SoA indexes — on QUAL-FREE single-varlena grouped
/// shapes (string min/max passengers, plain and bounded-top-n compositions
/// alike) the sink's mandatory re-arm ([`seq_scan_cb_columnar_arm`]) used to
/// hit the foreign-consumer guard and refuse EVERY engagement, landing the
/// probe-suppressed plan SERIAL (the suppress-then-unarmed bug class).
///
/// QUAL-FREE scans only (`ss.qual` must be None): the plain columnar arm
/// never stages a qual program, and the sink drains read the selection as
/// the WHOLE qual verdict — on a qualed scan whose lane arm refused (the
/// only way a qualed scan still carries the bare remap here), shedding
/// would silently drop the qual. Those shapes keep today's refusal.
/// `false` = a qual is present, the staged batch is not the bare remap
/// shape (a foreign consumer owns it), or nothing is staged — nothing is
/// changed, the caller keeps its refusal. A later serial fallback re-arms
/// the remap through the staging ladder (`seq_scan_batch_soa_prepare_varlane`
/// rebuilds on any non-memo-hit), so shedding is never observable outside
/// the engagement attempt.
pub fn seq_scan_cb_varlane_shed(node: &mut SeqScanState<'_>, attnum: u16) -> bool {
    if node.ss.qual.is_some() {
        return false;
    }
    match node.batch_soa.as_deref() {
        Some(b)
            if b.key_col == Some(attnum)
                && b.varkey.is_some()
                && !b.qual_armed
                && b.nquals == 0
                && b.contains.is_none()
                && b.lane.is_none()
                && b.stitch.is_none()
                && b.proj.is_none()
                && b.dict_group.is_none() =>
        {
            node.batch_soa = None;
            true
        }
        _ => false,
    }
}

/// Arm the contains-LIKE kernel qual (the lane-v2 strsearch tier,
/// `notes/strsearch-parity-2026-07-12.md`): the scan qual is exactly one
/// `scan_var LIKE '%literal%'` clause (execexpr's `scan_contains_clause`
/// census). The text column stages per-row varlena pointers via the varkey
/// pass into SoA column 0; `seq_scan_next_pagebatch` then runs one
/// `qual_bitmap_contains` pass per staged batch. Rows whose datum is
/// compressed/external are undecidable in the kernel — they take the
/// forced-fallback bit and the per-row program (which detoasts exactly as C
/// does) re-checks them, so semantics stay byte-identical. False = not
/// armable (no census clause / unstageable column / collation lookup fails —
/// the per-row path then raises that error itself / another batch feed
/// already owns the node); the scalar per-row path continues unchanged.
pub fn seq_scan_batch_soa_prepare_contains<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> bool {
    let Some(c) = node.ss.qual.as_deref().and_then(|q| q.scan_contains_clause()) else {
        return false;
    };
    if let Some(b) = &node.batch_soa {
        // Memo hit on our own arm; any other armed feed wins (fail closed).
        return b.contains.is_some() && b.varkey.is_some();
    }
    // The per-row matcher resolves the collation once per call
    // (generic_match_text -> pg_newlocale_from_collation); a failing lookup
    // must surface as ITS error, not a silently-filtering kernel.
    if ::pg_locale::pg_newlocale_from_collation(c.collation).is_err() {
        return false;
    }
    let mcx = estate.es_query_cxt;
    let rel = node.ss.ss_currentRelation.as_ref().expect("seqscan has a relation");
    let atts: &[_] = &rel.rd_att.compact_attrs;
    let Some(vk) = ::exectuples::SoaVarKeyPlan::try_new(atts, c.attnum as usize) else {
        return false;
    };
    node.batch_soa = Some(::mcx::PgBox::new_in(
        BatchSoa {
            soa: ::exectuples::SoaBatch::new_in(mcx, 1),
            plan: ::exectuples::SoaDeformPlan::unused(mcx),
            qual_armed: true,
            qual_only: true,
            key_col: None,
            varkey: Some(vk),
            key_read_col: 0,
            publish: false,
            quals: [(0, ::execexpr::CmpOp::Int4Eq, ::datum::Datum::null());
                ::execexpr::SCAN_CMP_MAX_CLAUSES],
            nquals: 0,
            contains: Some(c),
            stitch: None,
            proj: None,
            lane: None,
            lane_requal: false,
            bits_only: false,
                dict_group: None,
            cond_armed: false,
            stage_cols: None,
            sel: [0; ::exectuples::SOA_BM_WORDS],
            nwords: 0,
            cur_word: 0,
            cur_bits: 0,
        },
        mcx,
    ));
    true
}

/// Arm the dict-code answer on an already-armed varlena direct key feed
/// (`seq_scan_sortkey_direct` with the varkey pass): the consumer opts into
/// reading the key as codes+dict per staged window
/// (`seq_scan_batch_key_dict_lane`), and the columnar fill answers with a
/// zero-gather dict lane where the chunk is dict-encoded (Raw windows — and
/// every heap batch — keep the per-row datum lane; consumers must treat a
/// missing lane as "this window is Raw"). False = no varkey staging armed
/// (fixed-width key or no batch SoA); nothing changes.
pub fn seq_scan_key_dict_arm(node: &mut SeqScanState<'_>) -> bool {
    let Some(b) = node.batch_soa.as_deref_mut() else { return false };
    if b.varkey.is_none() {
        return false;
    }
    b.soa.set_dict_want(0);
    true
}

/// The staged window's dict-code lane for the varkey direct key feed, when
/// the fill answered one (see `seq_scan_key_dict_arm`). While a lane is up,
/// the key column's datum/isnull cells are STALE — the caller must consume
/// codes+dict for the whole window and never call `seq_scan_batch_key` on
/// it.
#[inline]
pub fn seq_scan_batch_key_dict_lane(
    node: &SeqScanState<'_>,
) -> Option<::exectuples::SoaDictLane> {
    let b = node.batch_soa.as_deref()?;
    if b.varkey.is_none() {
        return None;
    }
    b.soa.dict_lane(0)
}

/// Direct key read for staged row `i`; None = fallback row (narrow tuple),
/// the caller must take the full emit path.
#[inline(always)]
pub fn seq_scan_batch_key<'mcx>(
    node: &SeqScanState<'mcx>,
    i: u32,
) -> Option<(::datum::Datum, bool)> {
    let b = node.batch_soa.as_deref().expect("direct key feed armed");
    debug_assert!(b.key_col.is_some());
    let c = b.key_read_col as usize;
    if b.soa.is_fallback(i) {
        return None;
    }
    Some((b.soa.col_values(c)[i as usize], b.soa.col_isnull(c)[i as usize]))
}

/// Kernel-qual selection bitmap armed on the batch SoA — the lane-v2
/// filtered-scan fast path (also true under the fused full-prefix deform,
/// where one deform serves both the qual bitmap and the fold lanes).
#[inline(always)]
pub fn seq_scan_batch_qual_bitmap_armed(node: &SeqScanState<'_>) -> bool {
    node.batch_soa.as_deref().is_some_and(|b| b.qual_armed)
}

/// Bitmap computed for the CURRENTLY staged page batch (armed + a non-empty
/// selection word set). False for a batch staged before arming — the caller
/// must keep the per-row walk for that batch.
#[inline(always)]
pub fn seq_scan_batch_qual_bitmap_ready(node: &SeqScanState<'_>) -> bool {
    node.batch_soa.as_deref().is_some_and(|b| b.qual_armed && b.nwords > 0)
}

/// Pop the next selection-bitmap survivor of the staged batch (ascending
/// staged-row index): bitmap hits plus forced fallback bits — the SoA prefix
/// deform skipped those rows, so `seq_scan_batch_fetch` re-checks them
/// per-row. The iterator cursor is node-resident (`cur_word`/`cur_bits`),
/// surviving the Volcano call boundary; `exec_rescan_seq_scan` resets it.
#[inline(always)]
pub fn seq_scan_batch_next_selected(node: &mut SeqScanState<'_>) -> Option<u32> {
    let b = node.batch_soa.as_deref_mut()?;
    debug_assert!(b.qual_armed);
    b.next_selected()
}

/// Staged SoA batch when the full-prefix deform is armed (columnar readers).
#[inline]
pub fn seq_scan_batch_soa<'a, 'mcx>(
    node: &'a SeqScanState<'mcx>,
) -> Option<&'a ::exectuples::SoaBatch<'mcx>> {
    let b = node.batch_soa.as_deref()?;
    (!b.qual_only).then_some(&b.soa)
}

/// Staged kernel-qual selection bitmap when the SoA deform armed the batch
/// qual (bits over the current staged batch; forced fallback rows carry a
/// set bit and must be re-checked per-row — compose with `fallback_words`).
/// None = no bitmap qual staged; the per-row fetch path owns the qual. A
/// `Some` covers the scan's WHOLE qual (the kernel is the entire program).
#[inline]
pub fn seq_scan_batch_qual_sel<'a, 'mcx>(node: &'a SeqScanState<'mcx>) -> Option<&'a [u64]> {
    let b = node.batch_soa.as_deref()?;
    // Hybrid lane quals: the bits are a conservative pre-filter (survivors
    // still re-run the full qual per row) — never expose them as the whole
    // qual's verdicts.
    (b.qual_armed && !b.lane_requal).then_some(&b.sel[..])
}

/// Skip-side view of the CURRENT staged batch's selection bitmap: a bit
/// CLEARED here is a row `seq_scan_batch_fetch` rejects on its first
/// compare with no other observable effect — a definitive rejection even
/// for hybrid requal quals (`lane_requal` re-runs the full qual on SET
/// bits only; fallback rows carry SET bits by the staging OR). Batch
/// consumers may therefore skip cleared rows without the `emit` call.
/// Unlike `seq_scan_batch_qual_sel`, this answers only "which rows can
/// `emit` possibly yield", NEVER "which rows pass the whole qual" — so it
/// may serve under `lane_requal`. Gated on `nwords > 0` (the emit fast
/// lane's own this-batch-has-live-bits guard). `None` = no live bitmap:
/// every row must go through `emit`.
pub fn seq_scan_batch_skip_sel<'a>(node: &'a SeqScanState<'_>) -> Option<&'a [u64]> {
    let b = node.batch_soa.as_deref()?;
    (b.qual_armed && b.nwords > 0).then_some(&b.sel[..b.nwords as usize])
}

/// Declare the drive BITS-ONLY (dop1-tax2 inc-2): the consumer reads the
/// selection bitmap (and per-row fallback emits off the store path) and
/// NEVER the staged SoA cells — the runtime census drive (`census_drain`:
/// fold_batch reads no lane columns). The lane then skips its post-eval
/// SoA materialization (survivor-window completing deform + dict-lane
/// gather) — dead work the serial Volcano census never does. Selection
/// bits and fallback words are computed identically. True = accepted (a
/// staged batch arm owns the scan); false = no staging armed (no-op).
pub fn seq_scan_batch_bits_only(node: &mut SeqScanState<'_>) -> bool {
    match node.batch_soa.as_deref_mut() {
        // Hybrid requal quals refuse: their survivors re-run the full qual
        // per row at fetch, and `seq_scan_batch_qual_sel` refuses their
        // bits anyway — the drive falls to the per-row path, which must
        // see today's staging exactly.
        Some(b) if !b.lane_requal => {
            b.bits_only = true;
            // No SoA reader: never copy prefix cells onto stored slots
            // (they may be un-materialized under this arm). The pgrcolumnar
            // store path fills the slot's needed set itself (the prefix
            // publish is a virtual-slot no-op there regardless).
            b.publish = false;
            true
        }
        _ => false,
    }
}

/// PREWHERE lane program armed on the batch staging (pgrcolumnar scans). The
/// staged SoA columns fill LAZILY under this arm (per-clause late
/// materialization; the completing deform runs only for survivor windows), so
/// a columnar reader above the scan must confine itself to SELECTED rows —
/// unselected cells may be stale (see `seq_scan_batch_lane_sel`).
#[inline]
pub fn seq_scan_batch_lane_armed(node: &SeqScanState<'_>) -> bool {
    node.batch_soa.as_deref().is_some_and(|b| b.lane.is_some())
}

/// Conservative staged-batch selection words when a PREWHERE lane owns the
/// qual: bitmap hits plus forced fallback bits, INCLUDING requal-pending rows
/// (hybrid lane quals re-run the full qual per survivor at fetch, so these
/// bits are a superset of the true survivors — usable as a proof domain for
/// batch-level guards over rows the consumer will touch, never as verdicts).
/// None = no lane armed / nothing staged for the current batch.
#[inline]
pub fn seq_scan_batch_lane_sel<'a>(node: &'a SeqScanState<'_>) -> Option<&'a [u64]> {
    let b = node.batch_soa.as_deref()?;
    (b.lane.is_some() && b.nwords > 0).then(|| &b.sel[..b.nwords as usize])
}

/// Slot-free batch filter classification (colagg): how a staged batch's
/// survivor set can be decided WITHOUT the per-row `seq_scan_batch_emit`
/// sequence, for consumers that never read the scan slot (the fold feed's
/// deferred-probe arms read keys and transition inputs from the staged SoA
/// lanes; the emit's slot store is pure discard there).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotFreeFilter {
    /// No qual at all: every staged row survives.
    All,
    /// The armed selection bitmap IS the whole qual's verdict for every
    /// non-fallback row (kernel/stitched/PREWHERE-owned bitmap, no hybrid
    /// requal tail). Forced-fallback rows carry a SET bit that demands the
    /// per-row re-check — callers must exclude fallback-bearing batches
    /// (the deferred-probe arms already admit all-lane batches only).
    Bitmap,
}

/// Classify the CURRENT staged batch for slot-free survivor collection.
/// `Some(kind)` = calling `seq_scan_batch_emit` on every staged row would
/// return `Some(slot)` exactly for the rows `kind` selects, with no
/// consumer-visible effect beyond the (discarded) slot store and the
/// per-tuple context reset: no projection, no hosted (subplan/param) qual,
/// no hybrid requal, no stale-bit hazard. `None` = the per-row emit owns the
/// batch (byte-identical fallback).
#[inline]
pub fn seq_scan_batch_slotfree_filter(node: &SeqScanState<'_>) -> Option<SlotFreeFilter> {
    if node.ss.ps_ProjInfo.is_some() {
        return None;
    }
    if node.ss.qual.is_none() {
        return Some(SlotFreeFilter::All);
    }
    let b = node.batch_soa.as_deref()?;
    // `qual_armed` bitmaps cover the scan's WHOLE qual (all-or-nothing
    // census / lane translation); `lane_requal` bits are a pre-filter only.
    // A hosted qual (subplan/param deps) never arms the bitmap.
    (b.qual_armed && !b.lane_requal && b.nwords > 0).then_some(SlotFreeFilter::Bitmap)
}

/// Whole-qual selection verdicts for the CURRENT staged batch when the armed
/// bitmap decides the ENTIRE qual (no hybrid requal tail) — the slot-free
/// filter's Bitmap arm WITHOUT the projection gate, for lane consumers that
/// never materialize the scan slot at all (the caller must have proven every
/// projection column skip-safe, e.g. bare Vars — the codedgroup feed's
/// admission). Forced-fallback rows still carry a set bit demanding the
/// per-row re-check; callers admit fallback-free batches only. None = the
/// per-row emit owns the batch.
#[inline]
pub fn seq_scan_batch_whole_qual_sel<'a>(node: &'a SeqScanState<'_>) -> Option<&'a [u64]> {
    let b = node.batch_soa.as_deref()?;
    (b.qual_armed && !b.lane_requal && b.nwords > 0).then_some(&b.sel[..])
}

/// Footer value min/max covering EVERY row of the CURRENT staged window
/// (pgrcolumnar granule zone entry; int-family columns only; `col` is the
/// 0-based scan column). None = no zone metadata / heap scan. Consumers use
/// it as a whole-window value proof (guard intervals, constant-key windows) —
/// it covers all staged rows, so any row subset is covered too.
#[inline]
pub fn seq_scan_window_value_minmax(
    node: &SeqScanState<'_>,
    col: usize,
) -> Option<(i64, i64)> {
    let sd = node.ss.ss_currentScanDesc.as_ref()?;
    ::tableam::table_scan_window_value_minmax(sd, col)
}

/// Qual coverage for the granule length-stats fold (lane-v2-lenfooter):
/// `Some(None)` = the scan has no qual; `Some(Some(c))` = the WHOLE qual is
/// `col c <> ''` hosted by the armed PREWHERE lane (its bitmap is then a
/// pure length predicate — footer empty-string counts derive it exactly);
/// `None` = not coverable, the meta arm must refuse.
pub fn seq_scan_meta_qual_shape(node: &SeqScanState<'_>) -> Option<Option<u16>> {
    if node.ss.qual.is_none() {
        return Some(None);
    }
    let b = node.batch_soa.as_deref()?;
    if b.lane_requal {
        return None;
    }
    b.lane.as_deref()?.ne_empty_single_col().map(Some)
}

/// v7 granule length-stats metadata peek (pgrcolumnar; see
/// `CbScanDescData::granule_meta_peek`). Callable only after the scan desc
/// exists (the fold drive's staging loop guarantees it via next_pagebatch's
/// ensure_scandesc; a missing desc reads as NotMeta).
pub fn seq_scan_granule_meta_peek<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    key_cols: &[u16],
    len_cols: &[u16],
    key_mm: &mut [(i64, i64)],
    len_stats: &mut [(u64, u32, u32)],
) -> PgResult<::tableam::CbGranuleMetaStep> {
    node.ensure_scandesc(estate)?;
    let Some(sd) = node.ss.ss_currentScanDesc.as_mut() else {
        return Ok(::tableam::CbGranuleMetaStep::NotMeta);
    };
    ::tableam::table_scan_granule_meta_peek(sd, key_cols, len_cols, key_mm, len_stats)
}

/// Consume the granule the peek just answered (never decoded).
pub fn seq_scan_granule_meta_consume(node: &mut SeqScanState<'_>) {
    let sd = node.ss.ss_currentScanDesc.as_mut().expect("peek preceded consume");
    ::tableam::table_scan_granule_meta_consume(sd);
}

/// Footer-stat agg meta arm's QUAL admission (the all-rows-pass proof's
/// executor half): true iff this pgrcolumnar scan's zone quals are the ENTIRE
/// scan qual — either there is no qual at all (vacuous), or every conjunct
/// lowered to a zone qual AND the staged drive owns the whole qual as a
/// bitmap (`qual_armed && !lane_requal`, the fold drive's own bitmap-mode
/// signal: no hybrid-requal tail re-runs any clause per row). Under that
/// grant, an AllPass zone verdict on every pushed entry proves every row of
/// the unit passes — the fold over the unit is the fold over an all-ones
/// selection. Call AFTER `arm_scan_staging` (the batch/lane arming decides
/// `qual_armed`).
pub fn seq_scan_agg_meta_qual_ok(node: &SeqScanState<'_>) -> bool {
    let Some(cb) = node.cb_scan.as_deref() else { return false };
    if node.ss.qual.is_none() {
        return cb.zone.is_empty();
    }
    cb.zone_covers_qual
        && node.batch_soa.as_deref().is_some_and(|b| b.qual_armed && !b.lane_requal)
}

/// Footer-stat aggregate metadata peek (pgrcolumnar; see
/// `CbScanDescData::agg_meta_peek`). A missing desc reads as NotMeta.
#[allow(clippy::too_many_arguments)]
pub fn seq_scan_agg_meta_peek<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    mm_cols: &[u16],
    sum_cols: &[u16],
    len_cols: &[u16],
    mm: &mut [(i64, i64)],
    sums: &mut [i128],
    lens: &mut [i64],
) -> PgResult<::tableam::CbAggMetaStep> {
    node.ensure_scandesc(estate)?;
    let Some(sd) = node.ss.ss_currentScanDesc.as_mut() else {
        return Ok(::tableam::CbAggMetaStep::NotMeta);
    };
    ::tableam::table_scan_agg_meta_peek(sd, mm_cols, sum_cols, len_cols, mm, sums, lens)
}

/// Consume the row group the peek just answered (`MetaRg`; never decoded).
pub fn seq_scan_agg_meta_consume_rg(node: &mut SeqScanState<'_>) {
    let sd = node.ss.ss_currentScanDesc.as_mut().expect("peek preceded consume");
    ::tableam::table_scan_agg_meta_consume_rg(sd);
}

/// Consume the granule the peek just answered (`MetaGranule`; never decoded).
pub fn seq_scan_agg_meta_consume_granule(node: &mut SeqScanState<'_>) {
    let sd = node.ss.ss_currentScanDesc.as_mut().expect("peek preceded consume");
    ::tableam::table_scan_agg_meta_consume_granule(sd);
}

pub fn seq_scan_next_pagebatch<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<u32> {
    node.ensure_scandesc(estate)?;
    let SeqScanState { ss, batch_soa, .. } = node;
    // SAFETY: written by ensure_scandesc when None.
    let scandesc = unsafe { ss.ss_currentScanDesc.as_mut().unwrap_unchecked() };
    let n = ::tableam::table_scan_getnextpagebatch(scandesc)?;
    if n > 0 {
        if let Some(b) = batch_soa.as_mut() {
            let b = &mut **b;
            if let Some(vk) = &b.varkey {
                ::tableam::table_scan_batch_stage_varkey(scandesc, vk, &mut b.soa);
                // Contains-LIKE kernel qual (strsearch tier): one bitmap
                // pass over the staged varlena pointer lane. Undecidable
                // rows (compressed/external datums) become forced-fallback
                // bits: they join the selection so `seq_scan_batch_fetch`
                // re-checks them with the per-row program (which detoasts
                // exactly as C does) — same rows, same order, same errors.
                if b.qual_armed {
                    if let Some(c) = &b.contains {
                        let nwords = (n as usize).div_ceil(64);
                        let mut undecided = [0u64; ::exectuples::SOA_BM_WORDS];
                        // SAFETY: staged varkey lane — every non-null cell
                        // is a live in-page varlena pointer readable through
                        // its header (`soa_stage_varkey`'s contract; null
                        // and narrow rows carry isnull/fallback bits).
                        unsafe {
                            ::execexpr::qual_bitmap_contains(
                                c.needle(),
                                &b.soa.col_values(0)[..n as usize],
                                &b.soa.col_isnull(0)[..n as usize],
                                &mut b.sel,
                                &mut undecided,
                            );
                        }
                        b.soa.mark_fallback_words(&undecided[..nwords]);
                        for (w, fb) in
                            b.sel[..nwords].iter_mut().zip(b.soa.fallback_words())
                        {
                            *w |= fb;
                        }
                        b.nwords = nwords as u32;
                        b.cur_word = 0;
                        b.cur_bits = b.sel[0];
                    }
                }
                return Ok(n);
            }
            // PREWHERE v1 staged drive (pgrcolumnar lane quals; phase4 design
            // §3): staged clauses run cheapest-first, each (a) folding
            // against the staged granule's zone metadata — AllFail clears
            // the window without touching data, AllPass skips the clause's
            // evaluation — then (b) deforming ONLY its own columns (late
            // materialization: undeformed clauses' columns never decode) and
            // ANDing into the selection bitmap, with an early-out once the
            // bitmap empties. Surviving windows complete to the full fill
            // set so every downstream SoA reader sees exactly the unstaged
            // deform; dict-answered lanes gather back to Raw (no dict-code
            // consumer past the qual in v1). Requal tails re-run the FULL
            // original qual per survivor at fetch (error identity / LIMIT
            // truncation / volatile counts — the per-row drive's by
            // construction). Below two staged clauses the whole-prefix lane
            // eval is the same one deform + one pass.
            if let Some(lq) = b.lane.as_deref_mut() {
                let nwords = (n as usize).div_ceil(64);
                b.sel[..nwords].fill(u64::MAX);
                if n % 64 != 0 {
                    b.sel[nwords - 1] = (1u64 << (n % 64)) - 1;
                }
                // Condition cache hit (pgrust.condition_cache): the staged
                // window's prefix verdicts served from memory — the qual's
                // decode + evaluation legs are skipped wholesale. Surviving
                // windows complete the FULL deform exactly as the miss
                // path's survivor branch does (dict-answered lanes gather
                // back to Raw, dict-group codes stay); an all-fail window
                // matches the miss path's zone-AllFail state (begun batch,
                // no fills — nothing downstream reads an empty selection).
                // The requal tail is untouched: it re-runs the full original
                // qual per surviving row at fetch on hit and miss alike.
                let cond_hit = b.cond_armed
                    && ::tableam::table_scan_condcache_lookup(scandesc, &mut b.sel);
                if cond_hit {
                    b.soa.begin(n);
                    if !b.bits_only && b.sel[..nwords].iter().any(|&w| w != 0) {
                        // Survivor-window COMPLETING deform: the armed lane
                        // owns the qual, consumers read SELECTED rows only
                        // (stale-cell contract) — lazy sub-framed dicts
                        // ensure survivors' codes only.
                        ::tableam::table_scan_batch_deform_sel(
                            scandesc,
                            &b.plan,
                            &mut b.soa,
                            None,
                            Some(&b.sel[..nwords]),
                        );
                        for c in lq.dict_cols() {
                            if b.dict_group != Some(c) {
                                gather_or_len(
                                    scandesc,
                                    &mut b.soa,
                                    c as usize,
                                    Some(&b.sel[..nwords]),
                                );
                            }
                        }
                    }
                } else if lq.nstaged() >= 2 {
                    lq.log_staged_once();
                    b.soa.begin(n);
                    for k in 0..lq.nstaged() {
                        if !b.sel[..nwords].iter().any(|&w| w != 0) {
                            break;
                        }
                        // Compressed-domain fold: a `Var CMP Const` clause
                        // whose staged granule is uniformly pass/fail skips
                        // its column decode and per-row eval entirely. The
                        // zone qual derives through the SAME extraction that
                        // built the pruning zone quals, so the folded
                        // verdict is byte-identical to the pruning path's.
                        if let Some(zs) = lq.staged_zone_src(k) {
                            if let Some((attnum, op, val)) =
                                cb_zone_from_parts(zs.col + 1, zs.fn_oid, zs.commuted, zs.konst)
                            {
                                let zq = ::tableam::ZoneQual { attnum, op, val };
                                match ::tableam::table_scan_staged_granule_verdict(scandesc, &zq)
                                {
                                    ::tableam::ZoneVerdict::AllPass => continue,
                                    ::tableam::ZoneVerdict::AllFail => {
                                        b.sel[..nwords].fill(0);
                                        break;
                                    }
                                    ::tableam::ZoneVerdict::Mixed => {}
                                }
                            }
                        }
                        for &c in lq.staged_cols(k) {
                            ::tableam::table_scan_batch_deform_col(scandesc, c, &mut b.soa);
                        }
                        lq.eval_staged(k, &b.soa, n, &mut b.sel)?;
                    }
                    if !b.bits_only && b.sel[..nwords].iter().any(|&w| w != 0) {
                        // Survivors: complete the deform to the full fill
                        // set (idempotent per column; dict-wanted columns
                        // re-answer as lanes and gather below — except a
                        // registered dict-code consumer's column, whose
                        // codes the dict-group feed reads directly).
                        // Lazy sub-framed dicts ensure SELECTED rows only
                        // (the armed lane's stale-cell contract).
                        ::tableam::table_scan_batch_deform_sel(
                            scandesc,
                            &b.plan,
                            &mut b.soa,
                            None,
                            Some(&b.sel[..nwords]),
                        );
                        for c in lq.dict_cols() {
                            if b.dict_group != Some(c) {
                                gather_or_len(
                                    scandesc,
                                    &mut b.soa,
                                    c as usize,
                                    Some(&b.sel[..nwords]),
                                );
                            }
                        }
                    }
                } else {
                    ::tableam::table_scan_batch_deform(scandesc, &b.plan, &mut b.soa, None);
                    ::laneexec::eval_lane_qual(lq, &b.soa, n, &mut b.sel)?;
                    // Bits-only drives skip the dict gather (nothing reads
                    // the Raw cells); every other consumer gathers exactly
                    // as before.
                    if !b.bits_only {
                        // Post-eval gather: sel is decided — lazy sub-framed
                        // dicts ensure selected rows only.
                        for c in lq.dict_cols() {
                            if b.dict_group != Some(c) {
                                gather_or_len(
                                    scandesc,
                                    &mut b.soa,
                                    c as usize,
                                    Some(&b.sel[..nwords]),
                                );
                            }
                        }
                    }
                }
                // Condition cache miss: record the freshly evaluated prefix
                // verdicts (pure qual bits — BEFORE the fallback OR below,
                // though pgrcolumnar stages no fallback rows).
                if b.cond_armed && !cond_hit {
                    ::tableam::table_scan_condcache_store(scandesc, &b.sel);
                }
                // pgrcolumnar stages no fallback rows; keep the OR for the
                // contract with `seq_scan_batch_fetch` anyway.
                for (w, fb) in b.sel[..nwords].iter_mut().zip(b.soa.fallback_words()) {
                    *w |= fb;
                }
                b.nwords = nwords as u32;
                b.cur_word = 0;
                b.cur_bits = b.sel[0];
                if let Some(p) = &mut b.proj {
                    // The stitched projection never co-arms with a lane qual
                    // (`seq_scan_proj_stitch_arm` refuses); belt anyway.
                    p.staged = false;
                }
                return Ok(n);
            }
            // K1 inc-2 late-materialization staging (wave-9 WS-AH): an armed
            // grouped heap feed narrows the kind-0 column pass to
            // {qual clause cols ∪ key cols}; the deferred columns fill for
            // qual survivors only, AFTER the bitmap below, through the
            // drain's `seq_scan_batch_complete_deform` call. Arming
            // (`seq_scan_k1_latemat_arm`) guarantees the shape this branch
            // assumes: a heap kernel-qual staging that OWNS the whole qual
            // (no requal tail), no varkey/proj/lane/key_col co-arm.
            if let Some(sc) = b.stage_cols.as_deref() {
                ::tableam::table_scan_batch_deform_cols(scandesc, &b.plan, &mut b.soa, sc);
            } else {
                // Single-clause qual-only staging deforms just the qual
                // column; a multi-clause qual needs every clause column, so
                // it stages the full (fixed-width) prefix. An armed stitched
                // projection reads its tlist columns from the lanes too, so
                // it also forces the full prefix.
                let qual_col_only =
                    (b.qual_only && b.qual_armed && b.nquals == 1 && b.proj.is_none())
                        .then_some(b.quals[0].0)
                        .or(b.key_col);
                ::tableam::table_scan_batch_deform(scandesc, &b.plan, &mut b.soa, qual_col_only);
            }
            if b.qual_armed {
                let nwords = (n as usize).div_ceil(64);
                // Tier ladder (design §3a): tier 2 = the stitched body over
                // the staged lanes (drain pipelines only, past the row
                // floor); tier 1 = the AOT bitmap kernel, one pass per
                // clause ANDed; tier 0 = the lanestitch interpreter, run
                // inside `StitchedProgram::run` on per-batch drift or after
                // a sticky refuse-and-replay. All tiers produce the same
                // selection bits over the same staged lanes (the lanestitch
                // equivalence contract + the strict-compare AND identity).
                if !stitch_qual_bitmap(b, n)? {
                    for (ci, &(col, cmp, konst)) in
                        b.quals[..b.nquals as usize].iter().enumerate()
                    {
                        if ci == 0 {
                            ::execexpr::qual_bitmap_cmp_const(
                                cmp,
                                konst,
                                b.soa.col_values(col as usize),
                                b.soa.col_isnull(col as usize),
                                &mut b.sel,
                            );
                        } else {
                            let mut tmp = [0u64; ::exectuples::SOA_BM_WORDS];
                            ::execexpr::qual_bitmap_cmp_const(
                                cmp,
                                konst,
                                b.soa.col_values(col as usize),
                                b.soa.col_isnull(col as usize),
                                &mut tmp,
                            );
                            for (w, t) in b.sel[..nwords].iter_mut().zip(&tmp[..nwords]) {
                                *w &= t;
                            }
                        }
                    }
                }
                // Stitched projection over the TRUE qual survivors: runs on
                // the pure qual bits BEFORE the forced-fallback OR below
                // (fallback rows carry no lane values — they keep the
                // per-row store+qual+project path; a garbage lane value must
                // never reach an erroring arith stencil). A true return =
                // the adaptive selectivity floor tripped: drop the arm, so
                // the NEXT staging returns to the qual-only column deform.
                if stitch_project(b, n) {
                    b.proj = None;
                }
                // Skipped rows carry a forced bit; the fetch re-checks them.
                for (w, fb) in b.sel[..nwords].iter_mut().zip(b.soa.fallback_words()) {
                    *w |= fb;
                }
                b.nwords = nwords as u32;
                b.cur_word = 0;
                b.cur_bits = b.sel[0];
            } else if let Some(p) = &mut b.proj {
                // No qual bitmap staged for this batch (bitmap disarmed):
                // the per-row path owns projection too.
                p.staged = false;
            }
        }
    }
    Ok(n)
}

/// Tier-2 attempt for one staged batch: run the stitched body (compiling it
/// first once past the row floor) over the staged SoA lanes into `b.sel`.
/// false = the AOT tier owns this batch (below floor / sticky refused /
/// never armed). The one-deform-two-consumers property holds by
/// construction: the lanes handed to the body are views over the SAME
/// staged `SoaBatch` the fold/emit consumers read; the selection bitmap is
/// the only coupling currency.
fn stitch_qual_bitmap(b: &mut BatchSoa<'_>, n: u32) -> PgResult<bool> {
    // Disjoint field borrows: the body reads `soa` lanes and the runner
    // writes `sel`; `stitch` carries the program + telemetry.
    let BatchSoa { soa, sel, stitch, .. } = b;
    let Some(st) = stitch.as_mut() else { return Ok(false) };
    let mut ran = false;
    if !st.refused {
        if st.body.is_none() && st.rows_seen >= STITCH_ROW_FLOOR {
            match ::lanestitch::StitchedProgram::compile(&st.prog, st.ncols) {
                Some(p) => {
                    lane_trace(&format!(
                        "stitch compiled (cols={} bytes={} nanos={} simd={})",
                        st.ncols,
                        p.code_bytes,
                        p.stitch_nanos,
                        p.is_simd(),
                    ));
                    st.body = Some(p);
                }
                None => {
                    // Sticky per plan: classification / arch / kill switch /
                    // arena refuse — the AOT tier owns every later batch.
                    st.refused = true;
                    lane_trace("stitch refused (compile)");
                }
            }
        }
        if let Some(body) = &st.body {
            // Stack lane views over the staged SoA (zero allocation on the
            // per-batch path — doctrine rule 7).
            let mut lanes =
                [::lanestitch::Lane { values: &[], isnull: &[] }; ::lanestitch::MAX_COLS];
            for (c, lane) in lanes[..st.ncols].iter_mut().enumerate() {
                *lane = ::lanestitch::Lane {
                    values: soa.col_values(c),
                    isnull: soa.col_isnull(c),
                };
            }
            // The body writes the pipeline's own selection words (all-ones
            // over n on entry, tail clear; only failures store).
            let nwords = (n as usize).div_ceil(64);
            sel[..nwords].fill(!0u64);
            if n % 64 != 0 {
                sel[nwords - 1] = (1u64 << (n % 64)) - 1;
            }
            // Per-batch signature check + refuse-and-replay live in the
            // runner: lane drift or an oversize batch interprets this batch
            // (fail-open); an erroring stitched exit replays the batch on
            // the interpreter and refuses the body for good. Our compare
            // programs are non-erroring, so the error arm is unreachable —
            // kept because fail-open must never become wrong-answer.
            match body.run_into(&st.prog, n, &lanes[..st.ncols], &mut sel[..nwords])? {
                ::lanestitch::RunOutcome::Stitched => st.n_stitched += 1,
                ::lanestitch::RunOutcome::InterpretedDrift
                | ::lanestitch::RunOutcome::InterpretedSticky => st.n_interp += 1,
            }
            ran = true;
        }
    }
    if !ran {
        st.n_aot += 1;
    }
    st.rows_seen += n as u64;
    Ok(ran)
}

/// Stitched-projection attempt for one staged batch: compute the output
/// lanes for the TRUE qual survivors (the pure qual bits, fallback rows
/// masked out — their lanes are undeformed garbage). Sets `proj.staged`;
/// on any refuse/drift the batch's rows project per-row (`exec_project`),
/// and a runtime trap additionally refuses the body for good (sticky
/// refuse-and-replay: the body constructed NO error; the per-row replay
/// raises C's exact error on C's row).
/// Returns true when the caller must DISARM projection hosting (the
/// adaptive selectivity floor tripped): dropping the arm returns staging to
/// the qual-only column deform, i.e. the pre-projstitch lane behavior.
fn stitch_project(b: &mut BatchSoa<'_>, n: u32) -> bool {
    let BatchSoa { soa, sel, proj, .. } = b;
    let Some(p) = proj.as_mut() else { return false };
    p.staged = false;
    if !p.refused {
        if p.body.is_none() && p.rows_seen >= STITCH_ROW_FLOOR {
            match ::lanestitch::StitchedProjection::compile(&p.prog, p.ncols, p.nouts as usize) {
                Some(body) => {
                    lane_trace(&format!(
                        "proj stitch compiled (cols={} outs={} bytes={} nanos={})",
                        p.ncols, p.nouts, body.code_bytes, body.stitch_nanos,
                    ));
                    p.body = Some(body);
                }
                None => {
                    p.refused = true;
                    lane_trace("proj stitch refused (compile)");
                }
            }
        }
        if let Some(body) = &p.body {
            let nwords = (n as usize).div_ceil(64);
            // True survivors only: qual bits minus forced-fallback bits
            // (the AOT/stitched qual computed garbage bits for undeformed
            // fallback rows; they must never reach an erroring stencil).
            let mut proj_sel = [0u64; ::exectuples::SOA_BM_WORDS];
            for ((d, s), fb) in proj_sel[..nwords]
                .iter_mut()
                .zip(&sel[..nwords])
                .zip(soa.fallback_words())
            {
                *d = s & !fb;
            }
            let mut lanes =
                [::lanestitch::Lane { values: &[], isnull: &[] }; ::lanestitch::MAX_COLS];
            for (c, lane) in lanes[..p.ncols].iter_mut().enumerate() {
                *lane = ::lanestitch::Lane {
                    values: soa.col_values(c),
                    isnull: soa.col_isnull(c),
                };
            }
            // Output-lane views over the arm-time buffers (zero per-batch
            // allocation): one SOA_MAX_ROWS chunk per tlist column.
            let mut outs: [::lanestitch::OutLane<'_>; ::lanestitch::MAX_OUTS] = {
                let mut vch = p.out_values.chunks_mut(::exectuples::SOA_MAX_ROWS);
                let mut nch = p.out_isnull.chunks_mut(::exectuples::SOA_MAX_ROWS);
                core::array::from_fn(|_| ::lanestitch::OutLane {
                    values: vch.next().map(|c| &mut c[..n as usize]).unwrap_or(&mut []),
                    isnull: nch.next().map(|c| &mut c[..n as usize]).unwrap_or(&mut []),
                })
            };
            match body.run_into(n, &lanes[..p.ncols], &proj_sel[..nwords], &mut outs[..p.nouts as usize]) {
                ::lanestitch::ProjOutcome::Stitched => {
                    p.staged = true;
                    p.n_stitched += 1;
                    p.stitched_rows += n as u64;
                    p.stitched_survivors +=
                        proj_sel[..nwords].iter().map(|w| w.count_ones() as u64).sum::<u64>();
                }
                ::lanestitch::ProjOutcome::Drift => {
                    p.n_perrow += 1;
                }
                ::lanestitch::ProjOutcome::Refused => {
                    // Sticky refuse-and-replay: this plan's data errors —
                    // the per-row C path owns the batch (and all later
                    // ones), raising the exact error on the exact row.
                    p.refused = true;
                    p.n_perrow += 1;
                    lane_trace("proj stitch refused (replay: data error)");
                }
            }
        } else {
            p.n_perrow += 1;
        }
    } else {
        p.n_perrow += 1;
    }
    p.rows_seen += n as u64;
    // Adaptive selectivity disarm (one-shot, PROJ_MIN_SELECTIVITY_PCT):
    // only when hosting widened the deform; the caller drops the arm.
    if p.adapt && !p.adapt_checked && p.stitched_rows >= PROJ_ADAPT_ROWS {
        p.adapt_checked = true;
        if p.stitched_survivors * 100 < p.stitched_rows * PROJ_MIN_SELECTIVITY_PCT {
            lane_trace(&format!(
                "proj stitch disarmed (selectivity {}/{} below {}%)",
                p.stitched_survivors, p.stitched_rows, PROJ_MIN_SELECTIVITY_PCT
            ));
            return true;
        }
    }
    false
}

/// Map an execexpr comparator + its const onto the stitcher vocabulary,
/// canonicalizing the const to the lanestitch canonical-datum contract
/// (sign-extended integer image at the const's own width — `Datum::from_iN`).
fn stitch_cmp(
    cmp: ::execexpr::CmpOp,
    konst: ::datum::Datum,
) -> (::lanestitch::CmpOp, ::datum::Datum) {
    use ::execexpr::CmpOp as E;
    use ::lanestitch::CmpOp as S;
    let op = match cmp {
        E::Int4Eq => S::Int4Eq,
        E::Int4Ne => S::Int4Ne,
        E::Int4Lt => S::Int4Lt,
        E::Int4Le => S::Int4Le,
        E::Int4Gt => S::Int4Gt,
        E::Int4Ge => S::Int4Ge,
        E::Int8Eq => S::Int8Eq,
        E::Int8Ne => S::Int8Ne,
        E::Int8Lt => S::Int8Lt,
        E::Int8Le => S::Int8Le,
        E::Int8Gt => S::Int8Gt,
        E::Int8Ge => S::Int8Ge,
        E::Int2Eq => S::Int2Eq,
        E::Int2Ne => S::Int2Ne,
        E::Int2Lt => S::Int2Lt,
        E::Int2Le => S::Int2Le,
        E::Int2Gt => S::Int2Gt,
        E::Int2Ge => S::Int2Ge,
        E::Int84Eq => S::Int84Eq,
        E::Int84Ne => S::Int84Ne,
        E::Int84Lt => S::Int84Lt,
        E::Int84Le => S::Int84Le,
        E::Int84Gt => S::Int84Gt,
        E::Int84Ge => S::Int84Ge,
        E::Int48Eq => S::Int48Eq,
        E::Int48Ne => S::Int48Ne,
        E::Int48Lt => S::Int48Lt,
        E::Int48Le => S::Int48Le,
        E::Int48Gt => S::Int48Gt,
        E::Int48Ge => S::Int48Ge,
        E::Int24Eq => S::Int24Eq,
        E::Int24Ne => S::Int24Ne,
        E::Int24Lt => S::Int24Lt,
        E::Int24Le => S::Int24Le,
        E::Int24Gt => S::Int24Gt,
        E::Int24Ge => S::Int24Ge,
        E::Int42Eq => S::Int42Eq,
        E::Int42Ne => S::Int42Ne,
        E::Int42Lt => S::Int42Lt,
        E::Int42Le => S::Int42Le,
        E::Int42Gt => S::Int42Gt,
        E::Int42Ge => S::Int42Ge,
        E::OidEq => S::OidEq,
        E::OidNe => S::OidNe,
        E::OidLt => S::OidLt,
        E::OidLe => S::OidLe,
        E::OidGt => S::OidGt,
        E::OidGe => S::OidGe,
        E::Float4Eq => S::Float4Eq,
        E::Float4Ne => S::Float4Ne,
        E::Float4Lt => S::Float4Lt,
        E::Float4Le => S::Float4Le,
        E::Float4Gt => S::Float4Gt,
        E::Float4Ge => S::Float4Ge,
        E::Float8Eq => S::Float8Eq,
        E::Float8Ne => S::Float8Ne,
        E::Float8Lt => S::Float8Lt,
        E::Float8Le => S::Float8Le,
        E::Float8Gt => S::Float8Gt,
        E::Float8Ge => S::Float8Ge,
        E::Float48Eq => S::Float48Eq,
        E::Float48Ne => S::Float48Ne,
        E::Float48Lt => S::Float48Lt,
        E::Float48Le => S::Float48Le,
        E::Float48Gt => S::Float48Gt,
        E::Float48Ge => S::Float48Ge,
        E::Float84Eq => S::Float84Eq,
        E::Float84Ne => S::Float84Ne,
        E::Float84Lt => S::Float84Lt,
        E::Float84Le => S::Float84Le,
        E::Float84Gt => S::Float84Gt,
        E::Float84Ge => S::Float84Ge,
    };
    // The const operand's own width per comparator family (the b side).
    let k = match cmp {
        E::Int2Eq | E::Int2Ne | E::Int2Lt | E::Int2Le | E::Int2Gt | E::Int2Ge
        | E::Int42Eq | E::Int42Ne | E::Int42Lt | E::Int42Le | E::Int42Gt | E::Int42Ge => {
            ::datum::Datum::from_i16(konst.as_i16())
        }
        E::Int4Eq | E::Int4Ne | E::Int4Lt | E::Int4Le | E::Int4Gt | E::Int4Ge
        | E::Int84Eq | E::Int84Ne | E::Int84Lt | E::Int84Le | E::Int84Gt | E::Int84Ge
        | E::Int24Eq | E::Int24Ne | E::Int24Lt | E::Int24Le | E::Int24Gt | E::Int24Ge => {
            ::datum::Datum::from_i32(konst.as_i32())
        }
        E::Int8Eq | E::Int8Ne | E::Int8Lt | E::Int8Le | E::Int8Gt | E::Int8Ge
        | E::Int48Eq | E::Int48Ne | E::Int48Lt | E::Int48Le | E::Int48Gt | E::Int48Ge => {
            ::datum::Datum::from_i64(konst.as_i64())
        }
        // Oid: sign-extend the u32 image (the stitcher's canonical-datum
        // contract — makes the 2x64 unsigned NEON compares exact).
        E::OidEq | E::OidNe | E::OidLt | E::OidLe | E::OidGt | E::OidGe => {
            ::datum::Datum::from_i32(konst.as_u32() as i32)
        }
        // Float consts: raw bit patterns at the const's own width (low-word
        // f32 / full-word f64 — the b side of each family).
        E::Float4Eq | E::Float4Ne | E::Float4Lt | E::Float4Le | E::Float4Gt | E::Float4Ge
        | E::Float84Eq | E::Float84Ne | E::Float84Lt | E::Float84Le | E::Float84Gt
        | E::Float84Ge => ::datum::Datum::from_f32(konst.as_f32()),
        E::Float8Eq | E::Float8Ne | E::Float8Lt | E::Float8Le | E::Float8Gt | E::Float8Ge
        | E::Float48Eq | E::Float48Ne | E::Float48Lt | E::Float48Le | E::Float48Gt
        | E::Float48Ge => ::datum::Datum::from_f64(konst.as_f64()),
    };
    (op, k)
}

/// Arm the tier-2 stitched body for an armed kernel-qual bitmap. Called ONLY
/// by the lane driver on drain pipelines feeding breakers (design rule: the
/// stitched segment never runs on pull-one-tuple pipelines). Idempotent; a
/// no-op when the bitmap is not armed, the stitcher is unavailable, or a
/// clause column exceeds the stitcher's lane window. Compilation itself is
/// deferred past the row floor (`stitch_qual_bitmap`); this only translates
/// the clause list into the stitch program.
pub fn seq_scan_stitch_arm(node: &mut SeqScanState<'_>) {
    let Some(b) = node.batch_soa.as_deref_mut() else { return };
    // A PREWHERE lane qual owns the bitmap (staged clauses + dict tier +
    // requal); the kernel `quals` it may shadow must not run a second tier.
    if b.lane.is_some() {
        return;
    }
    if !b.qual_armed
        || b.nquals < STITCH_MIN_CLAUSES
        || b.stitch.is_some()
        || !::lanestitch::available()
    {
        return;
    }
    let mut prog = ::lanestitch::Program::new();
    let mut ncols = 0usize;
    for &(col, cmp, konst) in &b.quals[..b.nquals as usize] {
        if col as usize >= ::lanestitch::MAX_COLS {
            return;
        }
        let (op, k) = stitch_cmp(cmp, konst);
        let kix = prog.push_const(::datum::NullableDatum { value: k, isnull: false });
        prog.steps.push(::lanestitch::Step::LoadLane { col, out: 0 });
        prog.steps.push(::lanestitch::Step::LoadConst { k: kix, out: 1 });
        prog.steps.push(::lanestitch::Step::Cmp { op, a: 0, b: 1, out: 2 });
        prog.steps.push(::lanestitch::Step::Qual { a: 2 });
        ncols = ncols.max(col as usize + 1);
    }
    let nquals = b.nquals;
    b.stitch = Some(QualStitch {
        prog,
        ncols,
        body: None,
        rows_seen: 0,
        refused: false,
        n_stitched: 0,
        n_aot: 0,
        n_interp: 0,
    });
    lane_trace(&format!("stitch armed (clauses={nquals})"));
}

/// PGRUST_LANE_V2_TRACE engagement summary, emitted when the scan releases
/// its batch state (end / park).
fn stitch_trace_summary(node: &SeqScanState<'_>) {
    if let Some(b) = node.batch_soa.as_deref() {
        if let Some(st) = &b.stitch {
            lane_trace(&format!(
                "stitch summary: stitched={} aot={} interp={} refused={}",
                st.n_stitched, st.n_aot, st.n_interp, st.refused
            ));
        }
        if let Some(p) = &b.proj {
            lane_trace(&format!(
                "proj stitch summary: stitched={} perrow={} refused={}",
                p.n_stitched, p.n_perrow, p.refused
            ));
        }
    }
}

/// Kill switch for measurement: PGRUST_LANESTITCH_PROJ=0|off disables the
/// stitched-projection tier (the per-row `exec_project` path owns projected
/// scans, i.e. exactly the pre-projstitch lane behavior).
fn proj_stitch_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_LANESTITCH_PROJ").as_deref(), Ok("0") | Ok("off"))
    })
}

fn proj_arith(op: ::execexpr::ProjArithOp) -> ::lanestitch::ArithOp {
    use ::execexpr::ProjArithOp as E;
    use ::lanestitch::ArithOp as S;
    match op {
        E::Add2 => S::Add2,
        E::Sub2 => S::Sub2,
        E::Mul2 => S::Mul2,
        E::Div2 => S::Div2,
        E::Add4 => S::Add4,
        E::Sub4 => S::Sub4,
        E::Mul4 => S::Mul4,
        E::Div4 => S::Div4,
        E::Add8 => S::Add8,
        E::Sub8 => S::Sub8,
        E::Mul8 => S::Mul8,
        E::Div8 => S::Div8,
    }
}

/// Canonicalize an arith const to the lanestitch canonical-datum contract
/// (sign-extended image at the op's own width — same-width families only).
fn proj_arith_konst(op: ::execexpr::ProjArithOp, konst: ::datum::Datum) -> ::datum::Datum {
    use ::execexpr::ProjArithOp as E;
    match op {
        E::Add2 | E::Sub2 | E::Mul2 | E::Div2 => ::datum::Datum::from_i16(konst.as_i16()),
        E::Add4 | E::Sub4 | E::Mul4 | E::Div4 => ::datum::Datum::from_i32(konst.as_i32()),
        E::Add8 | E::Sub8 | E::Mul8 | E::Div8 => ::datum::Datum::from_i64(konst.as_i64()),
    }
}

/// The SoA prefix a stitched projection needs (max read attnum + 1), when
/// this scan's projection is census-covered and hostable: lane driver
/// callers widen their `seq_scan_batch_soa_prepare` prefix by this BEFORE
/// arming (`seq_scan_proj_stitch_arm` requires the staged prefix to cover
/// it). None = no hostable projection (no ProjInfo / census refused /
/// out-of-window / kill switch / stitcher unavailable).
///
/// Admission economics (design §4 — fail closed until measured): Var-only
/// tlists are refused (`any_arith`) — the stitched fill would only replace
/// the per-row Assign walk while WIDENING the deform prefix (a real
/// per-batch deform cost on every staged row), an unproven trade. Computed
/// columns are where the fused lanes carry a measured win (see the
/// projstitch A/B in the branch log). Ratchet DOWN (admit Var-only) only
/// with a measurement, STITCH_MIN_CLAUSES-style.
pub fn seq_scan_proj_stitch_prefix(node: &SeqScanState<'_>) -> Option<i32> {
    if !proj_stitch_enabled() || !::lanestitch::available() {
        return None;
    }
    let proj = node.ss.ps_ProjInfo.as_ref()?;
    let cols = proj.pi_state.scan_proj_cols()?;
    if !cols.any_arith() {
        return None;
    }
    if cols.n as usize > ::lanestitch::MAX_OUTS
        || cols.max_attnum() as usize >= ::lanestitch::MAX_COLS
    {
        return None;
    }
    Some(cols.max_attnum() as i32 + 1)
}

/// Arm the stitched-projection tier for an armed kernel-qual bitmap whose
/// staged prefix covers the projection's read columns. Called ONLY by the
/// lane driver on drain pipelines (the stitched segments never run on
/// pull-one-tuple pipelines). Idempotent; a no-op when unhostable — the
/// per-row `exec_project` path stays untouched (fail closed). Compilation
/// defers past the row floor (`stitch_project`); this translates the census
/// into the stitch program and allocates the output lanes once.
pub fn seq_scan_proj_stitch_arm<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) {
    let Some(prefix) = seq_scan_proj_stitch_prefix(node) else { return };
    let Some(proj) = node.ss.ps_ProjInfo.as_ref() else { return };
    let Some(cols) = proj.pi_state.scan_proj_cols() else { return };
    let result_slot = proj.pi_result_slot;
    let Some(b) = node.batch_soa.as_deref_mut() else { return };
    // Never co-arm with a PREWHERE lane qual: its bits may be a requal
    // pre-filter, and the stitched-projection emit fast lane bypasses the
    // per-row qual re-check entirely.
    if b.lane.is_some() {
        return;
    }
    if !b.qual_armed || b.proj.is_some() || (b.plan.ncols() as i32) < prefix {
        return;
    }
    // The projection writes the result slot's value arrays positionally;
    // its descriptor arity must equal the census arity (defense in depth —
    // the projection program was compiled against this slot).
    if estate.slot_mut(result_slot).base_mut().tts_values.len() != cols.n as usize {
        return;
    }
    let mut prog = ::lanestitch::Program::new();
    for (j, col) in cols.cols[..cols.n as usize].iter().enumerate() {
        match *col {
            ::execexpr::ScanProjCol::Var { attnum } => {
                prog.steps.push(::lanestitch::Step::LoadLane { col: attnum, out: 0 });
                prog.steps.push(::lanestitch::Step::StoreOut { a: 0, out: j as u16 });
            }
            ::execexpr::ScanProjCol::ArithVV { op, a, b: bcol } => {
                prog.steps.push(::lanestitch::Step::LoadLane { col: a, out: 0 });
                prog.steps.push(::lanestitch::Step::LoadLane { col: bcol, out: 1 });
                prog.steps.push(::lanestitch::Step::Arith {
                    op: proj_arith(op),
                    a: 0,
                    b: 1,
                    out: 2,
                });
                prog.steps.push(::lanestitch::Step::StoreOut { a: 2, out: j as u16 });
            }
            ::execexpr::ScanProjCol::ArithVK { op, attnum, konst, var_is_arg0 } => {
                let k = proj_arith_konst(op, konst);
                let kix = prog
                    .push_const(::datum::NullableDatum { value: k, isnull: false });
                prog.steps.push(::lanestitch::Step::LoadLane { col: attnum, out: 0 });
                prog.steps.push(::lanestitch::Step::LoadConst { k: kix, out: 1 });
                let (a, bb) = if var_is_arg0 { (0u8, 1u8) } else { (1u8, 0u8) };
                prog.steps.push(::lanestitch::Step::Arith {
                    op: proj_arith(op),
                    a,
                    b: bb,
                    out: 2,
                });
                prog.steps.push(::lanestitch::Step::StoreOut { a: 2, out: j as u16 });
            }
        }
    }
    let mcx = estate.es_query_cxt;
    let cells = cols.n as usize * ::exectuples::SOA_MAX_ROWS;
    // The adaptive selectivity disarm applies iff hosting WIDENS the
    // per-batch deform beyond the qual's own staging: single-clause
    // qual-only staging deforms one column, multi-clause the clause-covering
    // prefix; anything wider is projection-hosting cost that low-selectivity
    // scans cannot amortize (PROJ_MIN_SELECTIVITY_PCT).
    let qual_deform_cols = if b.qual_only && b.nquals == 1 {
        1
    } else {
        b.quals[..b.nquals as usize].iter().map(|&(c, _, _)| c as usize + 1).max().unwrap_or(0)
    };
    b.proj = Some(ProjStitch {
        prog,
        ncols: cols.max_attnum() as usize + 1,
        nouts: cols.n as u16,
        body: None,
        rows_seen: 0,
        refused: false,
        staged: false,
        adapt: b.plan.ncols() as usize > qual_deform_cols,
        adapt_checked: false,
        stitched_rows: 0,
        stitched_survivors: 0,
        out_values: ::mcx::vec_from_elem_in(mcx, ::datum::Datum::null(), cells),
        out_isnull: ::mcx::vec_from_elem_in(mcx, false, cells),
        n_stitched: 0,
        n_perrow: 0,
    });
    lane_trace(&format!("proj stitch armed (cols={})", cols.n));
}

/// Bitmap-armed batch census: rows of the staged batch passing the kernel
/// qual. Bitmap hits count with no per-row work; forced fallback rows (the
/// SoA prefix deform skipped them) run the per-row store+qual path. None =
/// no bitmap qual staged, the per-row drain owns the batch.
pub fn seq_scan_batch_qual_count<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    n: u32,
) -> PgResult<Option<u32>> {
    let nwords = (n as usize).div_ceil(64);
    let mut fallback = [0u64; ::exectuples::SOA_BM_WORDS];
    let mut count = 0u32;
    {
        let Some(b) = node.batch_soa.as_deref() else { return Ok(None) };
        if !b.qual_armed || b.lane_requal {
            // A hybrid lane qual's bits are a pre-filter, not verdicts; the
            // census cannot count off them.
            return Ok(None);
        }
        for (w, fb) in b.soa.fallback_words()[..nwords].iter().enumerate() {
            count += (b.sel[w] & !fb).count_ones();
            fallback[w] = *fb;
        }
    }
    for (w, mut bits) in fallback[..nwords].iter().copied().enumerate() {
        while bits != 0 {
            let i = (w as u32) * 64 + bits.trailing_zeros();
            bits &= bits - 1;
            if seq_scan_batch_fetch(node, estate, i)? {
                count += 1;
            }
        }
    }
    Ok(Some(count))
}

/// Store row `i` of the staged batch and apply the scan qual; false =
/// filtered (bitmap-armed batches test the selection bit, not the kernel).
#[inline(always)]
pub fn seq_scan_batch_fetch<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    i: u32,
) -> PgResult<bool> {
    if let Some(b) = node.batch_soa.as_deref() {
        if b.qual_armed {
            if b.sel[(i / 64) as usize] & (1u64 << (i % 64)) == 0 {
                return Ok(false);
            }
            // Hybrid lane quals (`lane_requal`): the bit is a conservative
            // pre-filter — fall through to the full per-row qual below
            // (error identity/order and volatile-call counts are the
            // original evaluator's by construction).
            if !b.soa.is_fallback(i) && !b.lane_requal {
                seq_scan_batch_store(node, estate, i);
                return Ok(true);
            }
        }
    }
    seq_scan_batch_store(node, estate, i);
    let ecxt = node.ss.ps_ExprContext;
    match node.ss.qual.as_deref_mut() {
        None => Ok(true),
        Some(q) => {
            // Per-tuple result mcx for arg-detoasting quals (C's
            // ecxt_per_tuple_memory; the emit-entry ExprContext reset frees
            // it) — mirrors `exec_scan_impl`'s per-row arming; es_query_cxt
            // would otherwise accumulate over the whole fused feed.
            let per_tuple = estate.ecxt(ecxt).per_tuple_mcx();
            // SAFETY: reset-only context, arena-boxed (address-stable),
            // outlives the plan.
            unsafe { q.arm_result_mcx_raw(per_tuple) };
            let slot_id = node.ss.ss_ScanTupleSlot;
            let mut slots = ::execexpr::EvalSlots {
                scan: Some(estate.slot_mut(slot_id)),
                inner: None,
                outer: None,
            };
            ::execexpr::exec_qual(Some(q), &mut slots)
        }
    }
}

#[inline(always)]
pub fn seq_scan_batch_store<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    i: u32,
) {
    let mcx = estate.es_query_cxt;
    let scandesc =
        node.ss.ss_currentScanDesc.as_mut().expect("batch store before batch fetch");
    let slot = estate.slot_mut(node.ss.ss_ScanTupleSlot);
    ::tableam::table_scan_batch_store_slot(mcx, scandesc, i, slot);
    if let Some(b) = node.batch_soa.as_ref() {
        if b.publish {
            ::exectuples::soa_store_prefix(slot, &b.soa, i);
        }
    }
}

/// Fused-feed emit: reset the per-tuple context, fetch row `i`, apply the
/// qual, project — `ExecScanExtended`'s body over a staged batch row. None =
/// filtered; Some = the scan's output slot.
///
/// Subplan- and param-bearing quals/projections run `exec_scan_impl`'s exact
/// arms (pending-initplan param evaluation, then the suspension-driven
/// subplan qual/projection drivers) — same per-row program, same order, same
/// per-tuple context discipline → byte-identical to the per-tuple path.
#[inline(always)]
pub fn seq_scan_batch_emit<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    i: u32,
) -> PgResult<Option<ExecSlotId>> {
    estate.ecxt_mut(node.ss.ps_ExprContext).reset();
    // Stitched-projection fast lane: this batch's output lanes are staged
    // (qual bitmap computed, projection body ran over the true survivors),
    // so a bitmap hit fills the result slot straight from the output lanes —
    // no scan-slot store, no per-row `exec_project`. Same values, same
    // isnull, same result-slot state as the per-row path (the census admits
    // only Var images and strict int arith, whose outputs are exactly the
    // per-row program's Datums). Fallback rows (no lane values) fall through
    // to the per-row path below, as do batches the body refused/drifted on.
    {
        let SeqScanState { ss, batch_soa, .. } = node;
        if let Some(b) = batch_soa.as_deref() {
            if b.qual_armed && b.nwords > 0 {
                if let Some(p) = &b.proj {
                    if p.staged {
                        if b.sel[(i / 64) as usize] & (1u64 << (i % 64)) == 0 {
                            return Ok(None);
                        }
                        if !b.soa.is_fallback(i) {
                            let proj =
                                ss.ps_ProjInfo.as_ref().expect("proj stitch armed with ProjInfo");
                            let result_id = proj.pi_result_slot;
                            let mcx = estate.es_query_cxt;
                            let slot = estate.slot_mut(result_id);
                            ::exectuples::exec_clear_tuple(slot, mcx);
                            let base = slot.base_mut();
                            let idx = i as usize;
                            for j in 0..p.nouts as usize {
                                base.tts_values[j] =
                                    p.out_values[j * ::exectuples::SOA_MAX_ROWS + idx];
                                base.tts_isnull[j] =
                                    p.out_isnull[j * ::exectuples::SOA_MAX_ROWS + idx];
                            }
                            ::exectuples::exec_store_virtual_tuple(slot);
                            return Ok(Some(result_id));
                        }
                    }
                }
            }
        }
    }
    let qual_hosted = node
        .ss
        .qual
        .as_deref()
        .is_some_and(|q| q.has_subplan() || !q.param_exec_deps().is_empty());
    let passes = if qual_hosted {
        // Subplan/param quals never arm the kernel bitmap (the kernel shapes
        // are subplan- and param-free), so the plain store path is the only
        // one live here.
        debug_assert!(node.batch_soa.as_deref().is_none_or(|b| !b.qual_armed));
        seq_scan_batch_store(node, estate, i);
        let scan_id = node.ss.ss_ScanTupleSlot;
        let ecxt = node.ss.ps_ExprContext;
        estate.ecxt_mut(ecxt).ecxt_scantuple = Some(scan_id);
        // ExecEvalParamExec pending-initplan arm, hoisted out of the
        // interpreter — mirrors `exec_scan_impl`.
        let deps = node.ss.qual.as_deref().unwrap().param_exec_deps();
        if !deps.is_empty() {
            ::executils::exec_eval_param_exec_params(estate, deps)?;
        }
        if node.ss.qual.as_deref().is_some_and(|q| q.has_subplan()) {
            ::executils::exec_qual_with_subplans(node.ss.qual.as_deref_mut(), estate, ecxt)?
        } else {
            // Param-only qual (initplan or correlated exec params, no subplan
            // steps): the params are plain datum reads once evaluated above —
            // `exec_scan_impl`'s ordinary per-row qual arm.
            let per_tuple = estate.ecxt(ecxt).per_tuple_mcx();
            // SAFETY: reset-only context, arena-boxed (address-stable),
            // outlives the plan.
            unsafe { node.ss.qual.as_deref_mut().unwrap().arm_result_mcx_raw(per_tuple) };
            let mut slots = ::execexpr::EvalSlots {
                scan: Some(estate.slot_mut(scan_id)),
                inner: None,
                outer: None,
            };
            ::execexpr::exec_qual(node.ss.qual.as_deref_mut(), &mut slots)?
        }
    } else {
        seq_scan_batch_fetch(node, estate, i)?
    };
    if !passes {
        return Ok(None);
    }
    let scan_id = node.ss.ss_ScanTupleSlot;
    let ecxt = node.ss.ps_ExprContext;
    estate.ecxt_mut(ecxt).ecxt_scantuple = Some(scan_id);
    if node.ss.ps_ProjInfo.is_none() {
        return Ok(Some(scan_id));
    };
    // C reads projection initplan params inside the projection, which never
    // runs on a qual-rejected tuple — mirrors `exec_scan_impl`.
    {
        let deps = node.ss.ps_ProjInfo.as_ref().unwrap().pi_state.param_exec_deps();
        if !deps.is_empty() {
            ::executils::exec_eval_param_exec_params(estate, deps)?;
        }
    }
    let proj = node.ss.ps_ProjInfo.as_mut().unwrap();
    let result_id = proj.pi_result_slot;
    if proj.pi_state.has_subplan() {
        ::executils::exec_project_with_subplans(&mut proj.pi_state, estate, ecxt, result_id)?;
        return Ok(Some(result_id));
    }
    // By-ref projection results (and callee scratch) must live in the
    // per-tuple memory reset at the next emit entry (C projects into
    // ecxt_per_tuple_memory) — mirrors `exec_scan_impl`; es_query_cxt would
    // otherwise accumulate over the whole fused feed.
    // SAFETY: reset-only context, arena-boxed (address-stable), outlives the
    // plan.
    unsafe {
        let per_tuple = estate.ecxt(ecxt).per_tuple_mcx();
        proj.pi_state.arm_result_mcx_raw(per_tuple);
    }
    let mcx = estate.es_query_cxt;
    let result_id = proj.pi_result_slot;
    let (scan_slot, result_slot) = ::execscan::slot_pair(estate, scan_id, result_id);
    let mut slots = ::execexpr::EvalSlots { scan: Some(scan_slot), inner: None, outer: None };
    ::execexpr::exec_project_prearmed(&mut proj.pi_state, &mut slots, result_slot, mcx)?;
    Ok(Some(result_id))
}

/// `ExecSeqScan` + its four specialized variants, dispatched on the enum
/// selected at init instead of C's per-variant function pointers.
pub fn exec_seq_scan<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    match node.variant {
        SeqScanVariant::Plain => exec_scan_extended::<_, false, false>(node, estate),
        SeqScanVariant::WithQual => {
            if scan_batch_ready(node, estate)? {
                return exec_seq_scan_batch::<false>(node, estate);
            }
            exec_scan_extended::<_, true, false>(node, estate)
        }
        SeqScanVariant::PlainBloom => exec_seq_scan_bloom(node, estate),
        SeqScanVariant::WithProject => exec_scan_extended::<_, false, true>(node, estate),
        SeqScanVariant::WithQualProject => {
            if scan_batch_ready(node, estate)? {
                return exec_seq_scan_batch::<true>(node, estate);
            }
            exec_scan_extended::<_, true, true>(node, estate)
        }
        SeqScanVariant::Epq => exec_scan_epq(node, estate),
    }
}

#[inline(always)]
fn scan_batch_ready<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    match node.scan_batch {
        ScanBatchMode::On => Ok(true),
        ScanBatchMode::Off => Ok(false),
        ScanBatchMode::Unknown => scan_batch_probe(node, estate),
    }
}

// Once per scan: the page-batch bitmap-qual drive covers uninstrumented
// forward-only kernel-qual scans (subplan-free projection); everything else
// keeps the per-tuple drive.
#[inline(never)]
fn scan_batch_probe<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    node.scan_batch = ScanBatchMode::Off;
    if !node.batch_allowed || node.ss.instr_idx.is_some() || estate.es_epq_active {
        return Ok(false);
    }
    if let Some(p) = node.ss.ps_ProjInfo.as_ref() {
        if p.pi_state.has_subplan() || !p.pi_state.param_exec_deps().is_empty() {
            return Ok(false);
        }
    }
    let Some(q) = node.ss.qual.as_deref() else { return Ok(false) };
    let ::execexpr::Kernel::QualScanVarCmpConst { attnum, .. } = q.kernel() else {
        return Ok(false);
    };
    node.ensure_scandesc(estate)?;
    if !::tableam::table_scan_supports_pagebatch(node.ss.ss_currentScanDesc.as_ref().unwrap()) {
        return Ok(false);
    }
    seq_scan_batch_soa_prepare(node, estate, attnum as i32 + 1, true, false, false);
    if node.batch_soa.as_deref().is_some_and(|b| b.qual_armed) {
        node.scan_batch = ScanBatchMode::On;
        return Ok(true);
    }
    node.batch_soa = None;
    Ok(false)
}

#[inline(never)]
fn exec_seq_scan_batch<'mcx, const PROJ: bool>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    debug_assert!(::types_scan::sdir::ScanDirectionIsForward(estate.es_direction));
    estate.ecxt_mut(node.ss.ps_ExprContext).reset();
    loop {
        let next = node.batch_soa.as_deref_mut().expect("batch drive armed").next_selected();
        let Some(i) = next else {
            let n = seq_scan_next_pagebatch(node, estate)?;
            if n == 0 {
                let mcx = estate.es_query_cxt;
                if PROJ {
                    let proj = node.ss.ps_ProjInfo.as_ref().unwrap();
                    ::exectuples::exec_clear_tuple(estate.slot_mut(proj.pi_result_slot), mcx);
                }
                return Ok(None);
            }
            continue;
        };
        if !seq_scan_batch_fetch(node, estate, i)? {
            continue;
        }
        let scan_id = node.ss.ss_ScanTupleSlot;
        estate.ecxt_mut(node.ss.ps_ExprContext).ecxt_scantuple = Some(scan_id);
        if !PROJ {
            return Ok(Some(scan_id));
        }
        let mcx = estate.es_query_cxt;
        let proj = node.ss.ps_ProjInfo.as_mut().unwrap();
        let result_id = proj.pi_result_slot;
        let (scan_slot, result_slot) = ::execscan::slot_pair(estate, scan_id, result_id);
        let mut slots =
            ::execexpr::EvalSlots { scan: Some(scan_slot), inner: None, outer: None };
        ::execexpr::exec_project(&mut proj.pi_state, &mut slots, result_slot, mcx)?;
        return Ok(Some(result_id));
    }
}

/// Hashjoin Bloom pushdown seat: arm (Some) or disarm (None) after a hash
/// build. Runtime gate only — plans are untouched; Instrumented outers never
/// reach here, so EXPLAIN ANALYZE keeps the per-tuple drive and its counters.
pub fn seq_scan_set_bloom<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    push: Option<(std::rc::Rc<::nodehash::ProbeBloom<'mcx>>, u16)>,
) -> PgResult<bool> {
    if node.variant == SeqScanVariant::PlainBloom {
        node.variant = SeqScanVariant::Plain;
        node.bloom = None;
    }
    let Some((filter, col)) = push else { return Ok(false) };
    if node.variant != SeqScanVariant::Plain
        || !node.batch_allowed
        || node.ss.instr_idx.is_some()
        || estate.es_epq_active
        || !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction)
    {
        return Ok(false);
    }
    node.ensure_scandesc(estate)?;
    if !::tableam::table_scan_supports_pagebatch(node.ss.ss_currentScanDesc.as_ref().unwrap()) {
        return Ok(false);
    }
    let mcx = estate.es_query_cxt;
    let rel = node.ss.ss_currentRelation.as_ref().expect("seqscan has a relation");
    let atts: &[_] = &rel.rd_att.compact_attrs;
    let Some(plan) = ::exectuples::SoaDeformPlan::try_new(mcx, atts, col as usize + 1) else {
        return Ok(false);
    };
    node.bloom = Some(::mcx::PgBox::new_in(
        BloomScan {
            soa: ::exectuples::SoaBatch::new_in(mcx, plan.ncols()),
            plan,
            filter,
            col,
            sel: [0; ::exectuples::SOA_BM_WORDS],
            nwords: 0,
            cur_word: 0,
            cur_bits: 0,
            seen: 0,
            kept: 0,
        },
        mcx,
    ));
    node.variant = SeqScanVariant::PlainBloom;
    Ok(true)
}

// Plain-scan Bloom drive: stage a page, deform the key column only, keep
// rows the filter admits (misses prove no hash match; NULL keys test hash 0
// like the Hash32Var kernel; fallback rows pass conservatively). Same tuple
// order and slot state as the per-row Plain path for every surviving row.
#[inline(never)]
fn exec_seq_scan_bloom<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    debug_assert!(::types_scan::sdir::ScanDirectionIsForward(estate.es_direction));
    estate.ecxt_mut(node.ss.ps_ExprContext).reset();
    loop {
        let next = node.bloom.as_deref_mut().expect("bloom drive armed").next_selected();
        let Some(i) = next else {
            // Page boundary: rs_cindex parks at page end, so the per-tuple
            // walk resumes on the NEXT page — disarming here is order-exact.
            // Break-even ~9% rejected (filter ~45 instr/row vs ~500 saved).
            {
                let b = node.bloom.as_deref().expect("bloom drive armed");
                if b.seen >= 1024 && 8 * (b.kept as u64) > 7 * (b.seen as u64) {
                    node.bloom = None;
                    node.variant = SeqScanVariant::Plain;
                    return exec_scan_extended::<_, false, false>(node, estate);
                }
            }
            node.ensure_scandesc(estate)?;
            let SeqScanState { ss, bloom, .. } = node;
            // SAFETY: written by ensure_scandesc when None.
            let scandesc = unsafe { ss.ss_currentScanDesc.as_mut().unwrap_unchecked() };
            let n = ::tableam::table_scan_getnextpagebatch(scandesc)?;
            if n == 0 {
                return Ok(None);
            }
            let b = &mut **bloom.as_mut().expect("bloom drive armed");
            ::tableam::table_scan_batch_deform(scandesc, &b.plan, &mut b.soa, Some(b.col));
            b.filter.sel_hash32_low32(
                b.soa.col_values(b.col as usize),
                b.soa.col_isnull(b.col as usize),
                &mut b.sel,
            );
            let nwords = (n as usize).div_ceil(64);
            // Skipped rows carry a forced bit: no columnar key, pass through.
            for (w, fb) in b.sel[..nwords].iter_mut().zip(b.soa.fallback_words()) {
                *w |= fb;
            }
            b.nwords = nwords as u32;
            b.cur_word = 0;
            b.cur_bits = b.sel[0];
            b.seen += n;
            b.kept += b.sel[..nwords].iter().map(|w| w.count_ones()).sum::<u32>();
            continue;
        };
        let mcx = estate.es_query_cxt;
        let scandesc =
            node.ss.ss_currentScanDesc.as_mut().expect("bloom fetch after page stage");
        let slot = estate.slot_mut(node.ss.ss_ScanTupleSlot);
        ::tableam::table_scan_batch_store_slot(mcx, scandesc, i, slot);
        return Ok(Some(node.ss.ss_ScanTupleSlot));
    }
}

/// `ExecInitSeqScan`; opens the scan relation through the estate range table.
pub fn exec_init_seq_scan<'mcx>(
    mcx: Mcx<'mcx>,
    node: &SeqScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    eflags: i32,
) -> PgResult<SeqScanState<'mcx>> {
    let rel = exec_open_scan_relation(estate, node, eflags)?;
    let mut state = exec_init_seq_scan_rel(mcx, node, estate, rel)?;
    state.batch_allowed = eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) == 0;
    Ok(state)
}

/// `ExecOpenScanRelation`.
fn exec_open_scan_relation<'mcx>(
    estate: &mut EStateData<'mcx>,
    node: &SeqScan<'mcx>,
    eflags: i32,
) -> PgResult<Relation<'mcx>> {
    let rel = estate.exec_get_range_table_relation(node.scan.scanrelid, false)?;
    if eflags & (EXEC_FLAG_EXPLAIN_ONLY | EXEC_FLAG_WITH_NO_DATA) == 0
        && !rel.rd_rel.relispopulated
    {
        return Err(unpopulated_matview(rel));
    }
    Ok(rel.alias())
}

#[track_caller]
#[cold]
#[inline(never)]
fn unpopulated_matview(rel: &Relation<'_>) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "materialized view \"{}\" has not been populated",
            rel.name()
        ))
        .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .with_hint("Use the REFRESH MATERIALIZED VIEW command."),
    )
}

/// C divergence: init over a caller-opened relation (test surface;
/// `exec_init_seq_scan` is the real path through the estate range table).
pub fn exec_init_seq_scan_rel<'mcx>(
    mcx: Mcx<'mcx>,
    node: &SeqScan<'mcx>,
    estate: &mut EStateData<'mcx>,
    rel: Relation<'mcx>,
) -> PgResult<SeqScanState<'mcx>> {
    debug_assert!(node.scan.plan.lefttree.is_none() && node.scan.plan.righttree.is_none());

    let ps_ExprContext = estate.exec_assign_expr_context();
    let kind = table_slot_callbacks(&rel);
    let ss_ScanTupleSlot = estate.exec_init_extra_tuple_slot(Some(rel.rd_att.clone()), kind);

    let mut ss = ScanState {
        qual: None,
        ps_ProjInfo: None,
        ps_ExprContext,
        scanrelid: node.scan.scanrelid,
        ss_currentRelation: Some(rel),
        ss_currentScanDesc: None,
        ss_ScanTupleSlot,
        instr_idx: None,
    };
    execscan::exec_assign_scan_projection_info(mcx, estate, &mut ss, &node.scan.plan.targetlist)?;
    let params = estate.param_bind();
    ss.qual = ::executils::with_subplan_compile_env(estate, |env| {
        ::execexpr::exec_init_qual_subplans(mcx, &node.scan.plan.qual, params, env)
    })?;

    let variant = if estate.es_epq_active {
        SeqScanVariant::Epq
    } else {
        match (ss.qual.is_some(), ss.ps_ProjInfo.is_some()) {
            (false, false) => SeqScanVariant::Plain,
            (true, false) => SeqScanVariant::WithQual,
            (false, true) => SeqScanVariant::WithProject,
            (true, true) => SeqScanVariant::WithQualProject,
        }
    };
    let cb_scan = match rel_am_is_pgrcolumnar(ss.ss_currentRelation.as_ref().unwrap()) {
        false => None,
        true => Some(std::boxed::Box::new(cb_scan_info(node, &ss)?)),
    };
    Ok(SeqScanState {
        ss,
        variant,
        plan_node_id: node.scan.plan.plan_node_id,
        plan_rows: node.scan.plan.plan_rows,
        parallel_aware: node.scan.plan.parallel_aware,
        parallel: None,
        batch_soa: None,
        scan_batch: ScanBatchMode::Unknown,
        batch_allowed: false,
        bloom: None,
        lane_pos: 0,
        lane_n: 0,
        lane_verdict: None,
        cb_standalone: None,
        cb_prewhere_refused: false,
        cb_tiny: false,
        lane_park: None,
        lane_hold_pin: false,
        cb_scan,
    })
}

fn rel_am_is_pgrcolumnar(rel: &Relation<'_>) -> bool {
    ::tableam::TableAm::of(rel) == Some(::tableam::TableAm::Pgrcolumnar)
}

/// A pgrcolumnar relation drives this scan (lane arm gates; the lane's cbscan
/// engagement class ticks on this).
pub fn seq_scan_is_pgrcolumnar(node: &SeqScanState<'_>) -> bool {
    node.cb_scan.is_some()
}

/// Total committed rows of a pgrcolumnar scan's Part (footer metadata only; opens
/// the scan descriptor if needed — the same open the drive does anyway).
/// None = heap. The lane's tiny-input admission floor reads this BEFORE the
/// arm cascade runs.
pub fn seq_scan_cb_total_rows<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<u64>> {
    node.ensure_scandesc(estate)?;
    Ok(::tableam::table_scan_cb_total_rows(node.ss.ss_currentScanDesc.as_ref().unwrap()))
}

/// Part-global granule geometry of a pgrcolumnar scan (runtime morsel source,
/// M1 scan pipelines): (total granules, row-group-start prefix sums = the
/// hard morsel boundaries). None = heap or empty part. Opens the scan
/// descriptor if needed, exactly as the drive would.
pub fn seq_scan_cb_granule_geometry<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<(u64, Vec<u64>)>> {
    // GL-Q4142 — THE morsel-geometry chokepoint. This geometry is a PRIVATE,
    // PART-GLOBAL claim space; a scan carrying parallel wiring divides its
    // work through the shared in-AM cursor (`phs_nallocated`, claimed by
    // `claim_next_rg`) instead. Handing the part-global map to a participant
    // of a classic-parallel scan makes EVERY participant walk the whole
    // part, so each partial aggregate is the global answer and the finalize
    // sums them — a result silently inflated by the participant count.
    // `None` is every caller's fail-closed refusal (the arm falls back to
    // the classic parallel drive, always byte-safe), so one gate here covers
    // all of them instead of five open-coded conjuncts.
    if node.is_parallel() {
        return Ok(None);
    }
    node.ensure_scandesc(estate)?;
    Ok(::tableam::table_scan_cb_granule_geometry(
        node.ss.ss_currentScanDesc.as_ref().unwrap(),
    ))
}

/// EA-on-morsels: this scan descriptor's cumulative pgrcolumnar counters (the
/// CBSCAN fields — see tableam::table_scan_cb_ea_counters for the order).
/// None = heap or no descriptor opened yet. Read-only snapshot; the EA
/// prune fold takes it at claim end (docs/design/ea-morsels.md §2).
pub fn seq_scan_cb_ea_counters(node: &SeqScanState<'_>) -> Option<[u64; 7]> {
    ::tableam::table_scan_cb_ea_counters(node.ss.ss_currentScanDesc.as_ref()?)
}

/// This scan carries parallel wiring — the plan node is `parallel_aware`
/// and/or a shared `ParallelTableScanDescShared` is attached (GL-Q4142
/// morsel-source gate). Such a scan divides its work through the SHARED
/// in-AM cursor, so a private whole-relation morsel map must never drive it.
/// Free function mirror of `SeqScanState::is_parallel` for the lane's
/// morsel-source gates, which hold the node behind the crate boundary.
pub fn seq_scan_is_parallel(node: &SeqScanState<'_>) -> bool {
    node.is_parallel()
}

/// A plain heap relation drives this scan (runtime heap morsel source gate,
/// M1 heap source).
pub fn seq_scan_is_heap(node: &SeqScanState<'_>) -> bool {
    node.ss
        .ss_currentRelation
        .as_ref()
        .is_some_and(|rel| ::tableam::TableAm::of(rel) == Some(::tableam::TableAm::Heap))
}

/// Block-range geometry of a heap scan (runtime morsel source, M1 heap
/// source): total blocks at scan start — the granule count (granule = one
/// block, no interior boundaries). None = not heap or an empty relation.
/// Opens the scan descriptor if needed, exactly as the drive would.
pub fn seq_scan_heap_block_geometry<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<u64>> {
    // GL-Q4142: same gate as the columnar geometry above — a private
    // `0..nblocks` claim space must never drive a scan that divides its work
    // through the shared parallel block cursor. (`heap_set_block_range` is
    // the AM backstop; this is the fail-closed refusal.)
    if node.is_parallel() {
        return Ok(None);
    }
    node.ensure_scandesc(estate)?;
    Ok(::tableam::table_scan_heap_block_geometry(
        node.ss.ss_currentScanDesc.as_ref().unwrap(),
    )
    .filter(|&n| n > 0))
}

/// Position a PGRCOLUMNAR scan on the granule claim [g0, g1). Ok(false) = not
/// pgrcolumnar, scan untouched — the m2 sink arms' fail-closed worker check
/// (pgrcolumnar-only admission). The AM-dispatched positioning for the runtime
/// scan arm is `seq_scan_set_morsel_range` below.

pub fn seq_scan_cb_set_granule_range<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    g0: u64,
    g1: u64,
) -> PgResult<bool> {
    node.ensure_scandesc(estate)?;
    ::tableam::table_scan_cb_set_granule_range(
        node.ss.ss_currentScanDesc.as_mut().unwrap(),
        g0,
        g1,
    )
}

/// End-of-claim release seam (single-executor wave 2, WS-O inc-2,
/// append-only): drop the heap scan's current page pin and reset it to the
/// drained state (the R3 zero-pins-at-settle law; pgrcolumnar no-op below
/// the AM dispatch). Never OPENS the scan — a scan that never opened holds
/// nothing. Called by the knob-ON batch sources' `end_claim`, INCLUDING on
/// error paths, so a failed claim never carries its pin into the abort
/// drain (pin-lifetime under stealing: a re-split claim remainder changes
/// hands with no pin left behind).
pub fn seq_scan_end_claim_release(node: &mut SeqScanState<'_>) {
    if let Some(scan) = node.ss.ss_currentScanDesc.as_mut() {
        ::tableam::table_scan_end_claim_release(scan);
    }
}

/// Cursor-suspension settle (WS-AI wave-9.5, lane-cursors.md §2; the
/// claim-release chain's cursor arm): if this scan holds a LANE-STAGED page
/// batch (`lane_n > 0` — the standalone lane pipeline's node-resident
/// consume cursor; drain-site claims never span a suspension), record its
/// reposition point and retire the claim through
/// `table_scan_end_claim_release` → `heap_end_claim_release`. Returns true
/// iff a park record was written (the caller then clears the scan/result
/// slots and arms the estate resume flag).
///
/// What deliberately does NOT settle here:
/// * the VOLCANO scan's own cross-FETCH `rs_cbuf` pin (`lane_n == 0`) — C
///   parity, untouched (design §2's stated divergence prices LANE claims
///   only);
/// * CURSOR-FILL-OWNED scans (`lane_hold_pin`, SE-R41 v2) — the cursor
///   store batch fill adopts the same C-parity posture as the Volcano row
///   chain it replaces: the staged page batch and its one pin survive the
///   suspension and the next fill continues in place (killing the
///   per-fill park→restage ceremony the SE12 B4 letter priced at ~19k
///   instr on deficit-1 fills); notes/se-r41-v2.md §3;
/// * pgrcolumnar staged windows — the park-point probe answers None below
///   the AM dispatch (R4 decode scratch is Arc/mmap-backed, holds no
///   bufmgr pins, and is node-resident by design §1);
/// * wrap-capable heap walks (syncscan-started) — the probe refuses them
///   and the pin-held C-parity posture stands (production standalone lane
///   admission is pgrcolumnar-only today; the heap arm is exercised by the
///   unit fixture).
///
/// R3 ZERO-PINS-AT-SETTLE is debug-asserted: a settled claim holds no pin.
pub fn seq_scan_cursor_settle(node: &mut SeqScanState<'_>) -> bool {
    if node.lane_n == 0 {
        return false;
    }
    // SE-R41 v2 (notes/se-r41-v2.md §3): a cursor-fill-owned scan keeps the
    // C-parity Volcano posture — the staged page batch and its pin survive
    // the suspension (exactly the pin C's cursor, and our own row-chain
    // per-tuple walk, hold across FETCHes), and the next fill continues
    // emitting from the node-resident consume cursor with ZERO restage.
    // This joins the settle doc's existing not-settleable C-parity class
    // (wrap-capable walks); R3 zero-pins-at-settle continues to bind for
    // every LANE claim this walker parks.
    if node.lane_hold_pin {
        return false;
    }
    let Some(scan) = node.ss.ss_currentScanDesc.as_mut() else {
        return false;
    };
    let Some((b0, b1)) = ::tableam::table_scan_cursor_park_point(scan) else {
        return false;
    };
    node.lane_park =
        Some(SeqScanCursorPark { b0, b1, pos: node.lane_pos, n: node.lane_n });
    node.lane_pos = 0;
    node.lane_n = 0;
    ::tableam::table_scan_end_claim_release(scan);
    debug_assert!(
        !::tableam::table_scan_holds_claim_pin(scan),
        "R3 zero-pins-at-settle: cursor settle left a claim pin behind"
    );
    true
}

/// True iff this scan carries an unconsumed cursor park record (unit face).
pub fn seq_scan_cursor_parked(node: &SeqScanState<'_>) -> bool {
    node.lane_park.is_some()
}

/// Settle PROBE (shared-borrow twin of [`seq_scan_cursor_settle`]'s gate):
/// true iff a settle call would write a park record — a lane-staged batch
/// (`lane_n > 0`) on an open scan with a park point. The settle walker
/// calls this BEFORE the claim-release so it can run its slot hygiene
/// (materialize — the emitted slots must survive the page pin going away)
/// while the staged page is still pinned; the subsequent settle call then
/// releases. Probe and settle read the same state and cannot disagree
/// between the two calls (both run under the walker's exclusive borrow) —
/// so the probe mirrors EVERY settle gate, including the SE-R41 v2
/// `lane_hold_pin` refusal (SE14 boarding composition): a cursor-fill-owned
/// scan keeps its page pin across the suspension, so settle writes no park
/// record AND the materialize hygiene is unnecessary — the emitted slots'
/// backing page stays pinned for exactly as long as the suspension lasts.
pub fn seq_scan_cursor_park_pending(node: &SeqScanState<'_>) -> bool {
    !node.lane_hold_pin
        && node.lane_n != 0
        && node
            .ss
            .ss_currentScanDesc
            .as_ref()
            .is_some_and(|s| ::tableam::table_scan_cursor_park_point(s).is_some())
}

/// Mid-page batch adoption (SE-R41 v2, the page-remainder defect fix): the
/// lane batch source calls this BEFORE staging a fresh page. If the
/// per-tuple row walk left this scan mid-page with unreturned visible
/// tuples, adopt the remainder window `[start, n)` over the pinned page's
/// already-collected `rs_vistuples` (the AM parks its per-tuple cursor at
/// page end — the batch consumption convention); the caller sets the lane
/// consume cursor to `(start, n)`. None = nothing to adopt (fresh, drained,
/// batch-owned, or page-exhausted scan): stage the next page as before.
/// Self-limiting: after batch staging or adoption the per-tuple cursor sits
/// at page end, so the probe fires at most once per row-walk→batch handoff.
pub fn seq_scan_adopt_midpage_batch(node: &mut SeqScanState<'_>) -> Option<(u32, u32)> {
    // Adoption serves the PLAIN staging shape only (the cursor fill's:
    // batch_soa unarmed, scalar emit walk). A qual-kernel-armed SoA drive
    // keeps its own selection-bitmap cursor — its staged state cannot be
    // reconstructed from the AM's per-tuple cursor, and a stale bitmap must
    // never be applied to an adopted page. (Those drives' ownership is
    // memoized-sticky from scan start, so they never see a row-walked
    // mid-page scan; this gate makes the invariant local rather than
    // global.)
    if node.batch_soa.as_deref().is_some_and(|b| b.qual_armed) {
        return None;
    }
    let scan = node.ss.ss_currentScanDesc.as_mut()?;
    ::tableam::table_scan_adopt_midpage_batch(scan)
}

/// True iff the ROW drive's own page-batch mode (`scan_batch_probe`) owns
/// this scan's staging (SE-R41 v2 engagement gate): a lane cursor fill must
/// not engage over it — the row-batch drive's position lives in its SoA
/// selection cursor, not the AM per-tuple cursor, so neither fresh staging
/// nor mid-page adoption can continue it correctly. Structurally
/// unreachable today (verdicts on both sides are memoized-sticky from scan
/// start); the gate makes the exclusion local and floor-proof.
pub fn seq_scan_row_batch_mode_on(node: &SeqScanState<'_>) -> bool {
    matches!(node.scan_batch, ScanBatchMode::On)
}

/// Cursor-suspension resume (the §2 "repossess on resume" half): reposition
/// the scan on the parked remainder window (`heap_set_block_range`'s
/// reset-half shape, through the AM dispatch), restage the suspended page
/// batch, and restore the consume cursor. Under the run's MVCC snapshot the
/// restaged visible set is the suspended one (same page, same snapshot —
/// same `page_collect_tuples` answer; pruning removes only all-dead tuples
/// and line-pointer numbering is stable), so the resumed emission is
/// byte-identical; a count mismatch fails LOUD rather than emitting a
/// shifted remainder. Ok(false) = nothing parked.
pub fn seq_scan_cursor_resume<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let Some(SeqScanCursorPark { b0, b1, pos, n }) = node.lane_park.take() else {
        return Ok(false);
    };
    node.ensure_scandesc(estate)?;
    ::tableam::table_scan_set_morsel_range(
        node.ss.ss_currentScanDesc.as_mut().unwrap(),
        b0,
        b1,
    )?;
    let restaged = seq_scan_next_pagebatch(node, estate)?;
    if restaged != n {
        return Err(::types_error::PgError::error(format!(
            "cursor resume restaged a different visible set (block {b0}: {restaged} rows, suspended with {n})"
        ))
        .into());
    }
    node.lane_pos = pos;
    node.lane_n = n;
    Ok(true)
}

/// Position the scan on the morsel claim [g0, g1) (the runtime's
/// boundary-clamped claim contract), dispatching on the AM: pgrcolumnar
/// absolute granules (whole granules within one row group) or heap blocks.
pub fn seq_scan_set_morsel_range<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    g0: u64,
    g1: u64,
) -> PgResult<()> {
    node.ensure_scandesc(estate)?;
    ::tableam::table_scan_set_morsel_range(
        node.ss.ss_currentScanDesc.as_mut().unwrap(),
        g0,
        g1,
    )
}

/// Drive-scaling observability counters of a pgrcolumnar scan (runtime WFIN
/// channel): (rg_switches, dict_builds, granules_scanned, windows_staged).
/// None = heap or scan not opened yet.
/// PGRUST_WFIN=1 gates the SFIN serial counter line (read once).
fn sfin_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_WFIN").map_or(false, |v| v.trim() == "1"))
}

pub fn seq_scan_cb_drive_counters(node: &SeqScanState<'_>) -> Option<(u64, u64, u64, u64)> {
    node.ss
        .ss_currentScanDesc
        .as_ref()
        .and_then(::tableam::table_scan_cb_drive_counters)
}

/// Direct bounded top-N granule drive (the runtime sort arm's
/// PGRUST_RUNTIME_TOPN_HEAP feed): the current morsel claim's next granule
/// whole — (nrows, rowref_base) — no window/SoA staging. The scan must be
/// positioned by `seq_scan_set_morsel_range` first (which opens the desc).
pub fn seq_scan_topn_direct_next_granule(
    node: &mut SeqScanState<'_>,
) -> PgResult<Option<(u32, u64)>> {
    ::tableam::table_scan_topn_direct_next_granule(
        node.ss.ss_currentScanDesc.as_mut().expect("morsel-positioned scan desc"),
    )
}

/// The decoded datum lane of scan column `col` (0-based) for the granule
/// `seq_scan_topn_direct_next_granule` just handed out. None = dict column
/// (the caller fails closed to the staged feed).
pub fn seq_scan_topn_direct_lane<'a>(
    node: &'a mut SeqScanState<'_>,
    col: usize,
) -> Option<&'a [::datum::Datum]> {
    ::tableam::table_scan_topn_direct_lane(
        node.ss.ss_currentScanDesc.as_mut().expect("morsel-positioned scan desc"),
        col,
    )
}

/// GCUT zone summary of a pgrcolumnar scan's key column (night/sort-merge-
/// redesign inc-2): per-granule best direction-folded order words + the
/// zone-max seed word, in the morsel-range granule space. `None` = heap,
/// scan not opened, or no columnar part. See
/// `CbScanDescData::zone_topk_words` for the correctness posture.
pub fn seq_scan_cb_zone_topk_words(
    node: &SeqScanState<'_>,
    col: u16,
    desc: bool,
    bound: u64,
) -> PgResult<Option<(Vec<u64>, Option<u64>)>> {
    match node.ss.ss_currentScanDesc.as_ref() {
        Some(sd) => ::tableam::table_scan_cb_zone_topk_words(sd, col, desc, bound),
        None => Ok(None),
    }
}

/// RG-altitude meta-answerability census over this scan's pushed zone
/// quals (the GL-SERIALTERM-META qual-zone helper; semantics at
/// pgrcolumnar::zone_meta_rg_census). None = heap / no descriptor / no
/// columnar part. Economics signal only — the serial fold-meta arm
/// re-proves every unit itself.
pub fn seq_scan_cb_zone_meta_census(
    node: &SeqScanState<'_>,
    need_sums: bool,
) -> PgResult<Option<(u64, u64)>> {
    match node.ss.ss_currentScanDesc.as_ref() {
        Some(sd) => ::tableam::table_scan_cb_zone_meta_census(sd, need_sums),
        None => Ok(None),
    }
}

// Plan-derived need-set + zone-mappable conjuncts for a pgrcolumnar scan.
fn cb_scan_info<'mcx>(
    node: &SeqScan<'mcx>,
    ss: &ScanState<'mcx>,
) -> PgResult<CbScanInfo> {
    use ::nodes_core::NodeWalker as _;
    use ::types_nodes::NodeTag;

    let rel = ss.ss_currentRelation.as_ref().unwrap();
    let natts = rel.rd_att.natts as usize;
    let scanrelid = node.scan.scanrelid as i32;

    struct Cx {
        scanrelid: i32,
        needed: Vec<bool>,
        wholerow: bool,
        syscol: bool,
    }
    impl<'mcx> ::nodes_core::NodeWalker<'mcx> for Cx {
        fn visit(&mut self, n: ::types_nodes::Node<'mcx>) -> PgResult<bool> {
            if n.node_tag() == NodeTag::T_Var {
                let v = n.as_var().unwrap();
                if v.varno == self.scanrelid && v.varlevelsup == 0 {
                    if v.varattno == 0 {
                        self.wholerow = true;
                    } else if v.varattno < 0 {
                        self.syscol = true;
                    } else if (v.varattno as usize) <= self.needed.len() {
                        self.needed[(v.varattno - 1) as usize] = true;
                    }
                }
                return Ok(false);
            }
            ::nodes_core::expression_tree_walker(n, self)
        }
    }
    let mut cx = Cx { scanrelid, needed: vec![false; natts], wholerow: false, syscol: false };
    for n in node.scan.plan.qual.iter() {
        cx.visit(n)?;
    }
    // Snapshot the qual-only contribution (the narrow-needed floor). A
    // whole-row Var inside the qual forces every column here too.
    let qual_needed: Vec<bool> = if cx.wholerow {
        vec![true; natts]
    } else {
        cx.needed.clone()
    };
    // Exact consumed-column set (SeqScan::cb_scan_cols, pgrust-only): the
    // planner's pre-physical-tlist read set. `use_physical_tlist` hands most
    // scans a whole-row tlist — free on heap (lazy deform), catastrophic
    // here: a one-column qual scan under count(*) decodes/decompresses every
    // column of every surviving granule (the ungrouped min/max 0.9s-serial pathology —
    // 49% decompress_frame_into of columns nothing reads). Prefer the exact
    // set; the qual walk above stays as fail-safe union (zone extraction
    // needs it anyway). Lane-gated so the lane-off arm remains the untouched
    // incumbent oracle; PGRUST_CB_SCANCOLS=0 is the A/B kill switch.
    let exact = match &node.cb_scan_cols {
        Some(cols) if cb_scan_cols_enabled() && ::guc_tables::backing::pgrust_lane_executor() => {
            for a in 1..=natts as i32 {
                if cols.is_member(a) {
                    cx.needed[(a - 1) as usize] = true;
                }
            }
            true
        }
        _ => false,
    };
    if !exact {
        for n in node.scan.plan.targetlist.iter() {
            cx.visit(n)?;
        }
    }
    if cx.syscol {
        return Err(Box::new(PgError::error(
            "cbstore does not support system columns".to_string(),
        )));
    }
    if cx.wholerow {
        cx.needed.iter_mut().for_each(|b| *b = true);
    }

    let mut zone: Vec<::tableam::ZoneQual> = Vec::new();
    let mut nquals = 0usize;
    for n in node.scan.plan.qual.iter() {
        nquals += 1;
        if let Some((attnum, op, val)) = cb_zone_conjunct(n, scanrelid) {
            zone.push(::tableam::ZoneQual { attnum, op, val });
        }
    }
    // v7 zero-count meta qual: the WHOLE qual is one conjunct and that
    // conjunct lowered to an exact stored-domain zero equality test.
    let zero_qual = match (nquals, zone.as_slice()) {
        (1, [q]) if q.val == 0 => match q.op {
            ::tableam::ZoneCmp::Ne => {
                Some(::tableam::MetaZeroQual { col: q.attnum - 1, keep_nonzero: true })
            }
            ::tableam::ZoneCmp::Eq => {
                Some(::tableam::MetaZeroQual { col: q.attnum - 1, keep_nonzero: false })
            }
            _ => None,
        },
        _ => None,
    };
    let zone_covers_qual = nquals > 0 && zone.len() == nquals;
    Ok(CbScanInfo {
        needed: cx.needed,
        qual_needed,
        needed_full: None,
        zone,
        zero_qual,
        zone_covers_qual,
    })
}

/// M3 runtime-sort key-only staged accept (docs/design/m3-sort.md inc-4
/// lever 1): narrow this pgrcolumnar scan's needed column set to the qual's own
/// columns ∪ `keep` (0-based attnos). ONLY for executors whose consumers
/// provably never read any other column from this scan's outputs — the
/// runtime sort WORKER emits nothing but (key, rowref); winners are
/// re-gathered by the LEADER under its own full needed set. Unneeded cells
/// in per-row emit slots read as NULL (the `gather_row` law) — callers must
/// not let them escape. Call BEFORE the first drive; an already-open scan
/// desc gets the narrowed set re-pushed (decoders re-derive via the needed
/// epoch). False = not a pgrcolumnar scan (no-op).
pub fn seq_scan_cb_narrow_needed(node: &mut SeqScanState<'_>, keep: &[u16]) -> bool {
    let Some(cb) = node.cb_scan.as_deref_mut() else { return false };
    let mut needed = cb.qual_needed.clone();
    for &a in keep {
        if (a as usize) < needed.len() {
            needed[a as usize] = true;
        }
    }
    // Stash the plan-derived set once so `seq_scan_cb_restore_needed` can
    // undo the narrowing (serial refsort accept — lazytopn lane). The
    // runtime-sort WORKER path narrows and never restores (its leader owns
    // a separate scan state); the stash is inert there.
    if cb.needed_full.is_none() {
        cb.needed_full = Some(std::mem::replace(&mut cb.needed, needed));
    } else {
        cb.needed = needed;
    }
    if let Some(sd) = node.ss.ss_currentScanDesc.as_mut() {
        ::tableam::table_scan_set_needed_attrs(sd, &cb.needed);
    }
    true
}

/// Undo `seq_scan_cb_narrow_needed`: restore the plan-derived full needed
/// set (re-pushed onto an open scan desc; decoders re-derive via the needed
/// epoch). The serial refsort accept narrows to key ∪ qual and MUST restore
/// before its winner gather (`gather_row` nulls cells outside the CURRENT
/// set and the gather-time guard demotes on them) and before any demote
/// re-feed (the legacy wide feed reads every tlist column). False = not a
/// pgrcolumnar scan or not narrowed (no-op).
pub fn seq_scan_cb_restore_needed(node: &mut SeqScanState<'_>) -> bool {
    let Some(cb) = node.cb_scan.as_deref_mut() else { return false };
    let Some(full) = cb.needed_full.take() else { return false };
    cb.needed = full;
    if let Some(sd) = node.ss.ss_currentScanDesc.as_mut() {
        ::tableam::table_scan_set_needed_attrs(sd, &cb.needed);
    }
    true
}

// Zone-mappable scan-qual conjunct: a top-level `Var CMP Const` OpExpr of
// this relation over the int/date/timestamp cross-type compare families.
fn cb_zone_conjunct(
    n: ::types_nodes::Node<'_>,
    scanrelid: i32,
) -> Option<(u16, ::tableam::ZoneCmp, i64)> {
    use ::types_nodes::NodeTag;
    if n.node_tag() != NodeTag::T_OpExpr {
        return None;
    }
    let op = n.as_op_expr()?;
    if op.args.len() != 2 {
        return None;
    }
    let a = op.args.iter().next()?;
    let b = op.args.iter().nth(1)?;
    let (var, konst, flip) = match (a.node_tag(), b.node_tag()) {
        (NodeTag::T_Var, NodeTag::T_Const) => (a.as_var()?, b.as_const()?, false),
        (NodeTag::T_Const, NodeTag::T_Var) => (b.as_var()?, a.as_const()?, true),
        _ => return None,
    };
    if var.varno != scanrelid || var.varlevelsup != 0 || var.varattno <= 0 || konst.constisnull {
        return None;
    }
    cb_zone_from_parts(var.varattno as u16, op.opfuncid, flip, konst.constvalue)
}

// Shared zone-qual extraction (op/const-width/flip) for a `Var CMP Const`
// with the const on the `commuted` side. attnum is 1-based. Also the staged
// prewhere fold's source, so folded verdicts derive from byte-identical
// (attnum, op, val) to the pruning path.
fn cb_zone_from_parts(
    attnum: u16,
    fn_oid: u32,
    commuted: bool,
    konst: ::datum::Datum,
) -> Option<(u16, ::tableam::ZoneCmp, i64)> {
    use ::tableam::ZoneCmp as Z;
    let (cmp, cw) = cb_zone_cmp(fn_oid)?;
    let val = match cw {
        2 => konst.as_i16() as i64,
        4 => konst.as_i32() as i64,
        _ => konst.as_i64(),
    };
    let cmp = if commuted {
        match cmp {
            Z::Lt => Z::Gt,
            Z::Le => Z::Ge,
            Z::Gt => Z::Lt,
            Z::Ge => Z::Le,
            other => other,
        }
    } else {
        cmp
    };
    Some((attnum, cmp, val))
}

// (comparison, const width) by pg_proc oid; const width is the CONST side
// of the cross-type families (int2/4/8 x int2/4/8, date, timestamp,
// date-vs-timestamp).
#[rustfmt::skip]
fn cb_zone_cmp(fnoid: u32) -> Option<(::tableam::ZoneCmp, u8)> {
    use ::tableam::ZoneCmp as Z;
    Some(match fnoid {
        63 => (Z::Eq, 2), 145 => (Z::Ne, 2), 64 => (Z::Lt, 2), 148 => (Z::Le, 2),
        146 => (Z::Gt, 2), 151 => (Z::Ge, 2),
        65 => (Z::Eq, 4), 144 => (Z::Ne, 4), 66 => (Z::Lt, 4), 149 => (Z::Le, 4),
        147 => (Z::Gt, 4), 150 => (Z::Ge, 4),
        467 => (Z::Eq, 8), 468 => (Z::Ne, 8), 469 => (Z::Lt, 8), 471 => (Z::Le, 8),
        470 => (Z::Gt, 8), 472 => (Z::Ge, 8),
        158 => (Z::Eq, 4), 164 => (Z::Ne, 4), 160 => (Z::Lt, 4), 166 => (Z::Le, 4),
        162 => (Z::Gt, 4), 168 => (Z::Ge, 4),
        159 => (Z::Eq, 2), 165 => (Z::Ne, 2), 161 => (Z::Lt, 2), 167 => (Z::Le, 2),
        163 => (Z::Gt, 2), 169 => (Z::Ge, 2),
        474 => (Z::Eq, 4), 475 => (Z::Ne, 4), 476 => (Z::Lt, 4), 478 => (Z::Le, 4),
        477 => (Z::Gt, 4), 479 => (Z::Ge, 4),
        852 => (Z::Eq, 8), 853 => (Z::Ne, 8), 854 => (Z::Lt, 8), 856 => (Z::Le, 8),
        855 => (Z::Gt, 8), 857 => (Z::Ge, 8),
        1086 => (Z::Eq, 4), 1091 => (Z::Ne, 4), 1087 => (Z::Lt, 4), 1088 => (Z::Le, 4),
        1089 => (Z::Gt, 4), 1090 => (Z::Ge, 4),
        2052 => (Z::Eq, 8), 2053 => (Z::Ne, 8), 2054 => (Z::Lt, 8), 2055 => (Z::Le, 8),
        2057 => (Z::Gt, 8), 2056 => (Z::Ge, 8),
        1152 => (Z::Eq, 8), 1153 => (Z::Ne, 8), 1154 => (Z::Lt, 8), 1155 => (Z::Le, 8),
        1157 => (Z::Gt, 8), 1156 => (Z::Ge, 8),
        _ => return None,
    })
}

/// `ExecEndSeqScan`.
pub fn exec_end_seq_scan(node: &mut SeqScanState<'_>) -> PgResult<()> {
    node.bloom = None;
    node.cb_scan = None;
    // SFIN drive-counter dump (dop1-tax diagnosis, PGRUST_WFIN=1 only): the
    // SERIAL side of the WFIN cb-counter channel — one line per pgrcolumnar
    // scan shutdown so the m0-accept harness can diff per-granule work
    // (rg_switches/dict_builds/granules/windows) serial vs runtime workers
    // directly. Same key=value discipline as MORSEL|WFIN (unknown fields
    // ignored by the parsers); default-off = zero cost.
    if sfin_enabled() {
        if let Some((rgsw, dictb, gscan, wins)) = seq_scan_cb_drive_counters(node) {
            // dz_* = the DOMAIN-WORK tripwire totals (exectuples
            // domain_work, proportionality-audit): bytes/entries of
            // knowingly domain-sized work (full-domain clears, eager
            // whole-dict fills, dense dict pointer-table builds) drained
            // at scan shutdown so gates can watch the domain:touched
            // ratio against cb_granules/cb_windows. Process-wide totals —
            // serial legs read per-query-exact, parallel drives smear
            // across emitters (tripwire semantics, not attribution).
            let (dzb, dze) = ::exectuples::domain_work_take();
            eprintln!(
                "MORSEL|SFIN|cb_rgswitch={rgsw}|cb_dictbuild={dictb}|cb_granules={gscan}|cb_windows={wins}|dz_bytes={dzb}|dz_entries={dze}"
            );
        }
    }
    stitch_trace_summary(node);
    condcache_stats_summary(node);
    // Releases the plan's deform-JIT kernel Rc and the stitched body's code
    // block (forget-exempt in batch.rs / here).
    node.batch_soa = None;
    if let Some(scandesc) = node.ss.ss_currentScanDesc.take() {
        table_endscan(scandesc)?;
    }
    node.parallel = None;
    Ok(())
}

// Condition-cache stats line at scan shutdown (armed scans only): the
// cumulative process counters, DEBUG1 like every lane stats line. Folds
// this scan's per-scan stat cells first so the line includes its own
// counts (the cells otherwise fold at scan-desc drop, which happens after
// this summary in shutdown/park order).
fn condcache_stats_summary(node: &mut SeqScanState<'_>) {
    if node.batch_soa.as_deref().is_some_and(|b| b.cond_armed) {
        if let Some(sd) = node.ss.ss_currentScanDesc.as_mut() {
            ::tableam::table_scan_condcache_fold_stats(sd);
        }
        let (h, m, i, e) = ::tableam::condcache_stats();
        ::laneexec::log_condcache_stats(h, m, i, e);
    }
}

/// Executor-skeleton park gate: EPQ and parallel scans never park.
pub fn skeleton_parkable(node: &SeqScanState<'_>) -> bool {
    !matches!(node.variant, SeqScanVariant::Epq) && !node.parallel_aware && node.parallel.is_none()
}

/// Executor-skeleton park: release everything per-run (scan descriptor,
/// relation pin, pushed filters, staged batches); compiled expressions and
/// slots stay armed. Pairs with `skeleton_rebind`.
pub fn skeleton_park(node: &mut SeqScanState<'_>) -> PgResult<()> {
    node.bloom = None;
    stitch_trace_summary(node);
    condcache_stats_summary(node);
    node.batch_soa = None;
    node.scan_batch = ScanBatchMode::Unknown;
    node.lane_pos = 0;
    node.lane_n = 0;
    node.lane_verdict = None;
    node.cb_standalone = None;
    node.cb_prewhere_refused = false;
    node.cb_tiny = false;
    node.lane_park = None;
    node.lane_hold_pin = false;
    if let Some(scandesc) = node.ss.ss_currentScanDesc.take() {
        table_endscan(scandesc)?;
    }
    node.ss.ss_currentRelation = None;
    Ok(())
}

/// Executor-skeleton re-arm: re-pin the scan relation for a new execution,
/// with C ExecOpenScanRelation's per-run relispopulated probe.
pub fn skeleton_rebind<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert!(node.ss.ss_currentScanDesc.is_none());
    let eflags = estate.es_top_eflags;
    let rel = estate.exec_get_range_table_relation(node.ss.scanrelid, false)?;
    if eflags & (EXEC_FLAG_EXPLAIN_ONLY | EXEC_FLAG_WITH_NO_DATA) == 0
        && !rel.rd_rel.relispopulated
    {
        return Err(unpopulated_matview(rel));
    }
    node.ss.ss_currentRelation = Some(rel.alias());
    Ok(())
}

/// `ExecReScanSeqScan`.
pub fn exec_rescan_seq_scan<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    if let Some(scan) = node.ss.ss_currentScanDesc.as_mut() {
        table_rescan(mcx, scan, None)?;
    }
    node.lane_pos = 0;
    node.lane_n = 0;
    node.lane_park = None;
    node.lane_hold_pin = false;
    if let Some(b) = node.batch_soa.as_deref_mut() {
        b.reset_staged();
    }
    if let Some(b) = node.bloom.as_deref_mut() {
        b.reset_staged();
    }
    execscan::exec_scan_rescan(&mut node.ss, estate);
    Ok(())
}

/// `ExecSeqScanEstimate`: no DSM thread-native (docs/parallel-query-design.md).
pub fn exec_seq_scan_estimate(_node: &mut SeqScanState<'_>) {}

/// `ExecSeqScanInitializeDSM`.
pub fn exec_seq_scan_initialize_dsm<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<std::sync::Arc<ParallelTableScanDescShared>> {
    let mcx = estate.es_query_cxt;
    let mut shared = std::sync::Arc::new(ParallelTableScanDescShared::default());
    let rel = node.ss.ss_currentRelation.as_ref().expect("seqscan has a relation");
    table_parallelscan_initialize(
        rel,
        std::sync::Arc::get_mut(&mut shared).expect("freshly created shared descriptor"),
        &estate.es_snapshot,
    )?;
    debug_assert!(node.ss.ss_currentScanDesc.is_none());
    node.ss.ss_currentScanDesc = Some(table_beginscan_parallel(mcx, rel, &shared)?);
    node.apply_cb_scan_settings();
    node.arm_slot_jit_deform(estate);
    node.parallel = Some(std::sync::Arc::clone(&shared));
    // GL-FIXCOUNT-2: publish that this execution's plan tree is now
    // parallel-wired, so `open_scandesc` can refuse a SECOND, private,
    // whole-relation descriptor over a `parallel_aware` plan node.
    estate.es_parallel_scan_wired = true;
    Ok(shared)
}

/// `ExecSeqScanReInitializeDSM`.
pub fn exec_seq_scan_reinitialize_dsm(node: &mut SeqScanState<'_>) {
    let shared = node.parallel.as_ref().expect("parallel seqscan was initialized");
    let rel = node.ss.ss_currentRelation.as_ref().expect("seqscan has a relation");
    table_parallelscan_reinitialize(rel, &shared.pscan);
}

/// `ExecSeqScanInitializeWorker`.
pub fn exec_seq_scan_initialize_worker<'mcx>(
    node: &mut SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    shared: std::sync::Arc<ParallelTableScanDescShared>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    let rel = node.ss.ss_currentRelation.as_ref().expect("seqscan has a relation");
    debug_assert!(node.ss.ss_currentScanDesc.is_none());
    node.ss.ss_currentScanDesc = Some(table_beginscan_parallel(mcx, rel, &shared)?);
    node.apply_cb_scan_settings();
    node.arm_slot_jit_deform(estate);
    node.parallel = Some(shared);
    // GL-FIXCOUNT-2: see `exec_seq_scan_initialize_dsm`.
    estate.es_parallel_scan_wired = true;
    Ok(())
}

mcx::forget_safe_nodrop!(SeqScanVariant);

mcx::forget_safe_nodrop!(ScanBatchMode);

mcx::forget_safe_nodrop!(SeqScanCursorPark);

// bloom/parallel exempt: released in exec_end_seq_scan / release_parallel.
mcx::forget_safe_struct!(
    SeqScanState<'_> {
        ss, variant, plan_node_id, plan_rows, parallel_aware, batch_soa, scan_batch, batch_allowed,
        lane_pos, lane_n, lane_verdict, cb_standalone, cb_prewhere_refused, cb_tiny, lane_park,
        lane_hold_pin;
        bloom, parallel, cb_scan
    },
    // stitch/proj exempt: the stitched programs (heap Vecs + the W^X code
    // blocks) are released in exec_end_seq_scan / skeleton_park via
    // `batch_soa = None` (the deform-JIT kernel Rc precedent); stage_cols
    // (K1 late-mat narrowed column set, a heap Vec) releases the same way.
    BatchSoa<'_> {
        plan, soa, qual_armed, qual_only, key_col, varkey, key_read_col, publish, quals,
        nquals, lane_requal, bits_only, dict_group, contains, cond_armed, sel, nwords, cur_word, cur_bits; stitch, proj, lane, stage_cols,
    },
    BloomScan<'_> { plan, soa, col, sel, nwords, cur_word, cur_bits, seen, kept; filter },
);
