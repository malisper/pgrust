//! Lane executor v2 — the operator→operator batched execution lane (production
//! rebuild). See `docs/design/lane-executor-v2.md`.
//!
//! Control model: **push** (Source → Operator → Sink), with a pull adapter at
//! the pipeline root because PostgreSQL's executor is Volcano/pull — the lane
//! is a push island that doles one tuple per `exec_proc_node` call out of the
//! root adapter's capacity-one buffer. The skeleton (traits + driver + root
//! adapter) lives in `lanev2/push.rs`; this file owns the per-scan refuse-sets
//! and the scan pipelines (source + scalar filter/project operator). The
//! conversion changes ONLY who calls whom: the batch staging primitives, the
//! one-row-at-a-time scalar emit, their order, and the refuse-sets are exactly
//! the Phase-1 pull drive's — byte-identical output.
//!
//! ALL substantive lane logic lives in this module, kept deliberately separate
//! from the byte-identical Volcano row-executor spine (`procnode.rs`,
//! `nodeseqscan`, `nodeagg`, …). The existing executor is touched in only a
//! handful of thin, mechanical spots:
//!   * `procnode::seq_scan_arm` — a 3-line dispatch hook (`if enabled() { if let
//!     Some(r) = try_own_seq_scan()? { return Ok(r) } }`) that falls through to
//!     the UNCHANGED per-tuple path on refuse;
//!   * `nodeseqscan::SeqScanState` — a two-`u32` page-batch cursor + accessors
//!     (the one-tuple-per-call drive needs its position to survive the Volcano
//!     call boundary, so this state must live on the node);
//!   * `executils::BatchSource` — the shared pull seam trait (it cannot live
//!     here: `nodeagg` re-exports it as `AggBatchSource`, and `nodeagg` cannot
//!     depend on `execmain` without a crate cycle, so the trait sits in the
//!     shared `executils` seam both crates already depend on).
//! Disabling or deleting the lane is therefore local: drop this module + the
//! thin hook, and the C-identical executor is exactly as before.
//!
//! Gated ON by default (as of 2026-07-14) via the `PGRUST_LANE_V2` env var;
//! `PGRUST_LANE_V2=0`/`off` is the explicit kill switch — the operational
//! escape hatch and the A/B lever. Env-var gating mirrors `jit_deform`'s
//! `PGRUST_JIT_DEFORM` switch and is byte-identity-safe (no `pg_settings` /
//! `SHOW ALL` row). Harness OFF arms must set `PGRUST_LANE_V2=0` explicitly.

mod exprkey;
mod push;
mod stats;

pub use exprkey::ExprKeyState;

use std::sync::OnceLock;

use ::executils::{EStateData, ExecSlotId};
use ::types_error::PgResult;

use push::{
    drain_pipeline, drain_pipeline_chain, pull_step, pull_step_chain, Batch, BatchEmit,
    BatchSink, OpStatus, Operator, RootAdapter, Sink, SinkFeed, Source, TupleOp,
};
use stats::{RefuseReason, ShapeClass};

/// Master switch for lane-v2. Default ON since 2026-07-14 (evidence:
/// notes/lane-timed-regress-2026-07-14.md — byte-identical regress ×6,
/// timed-regress median 1.000, all floors green). The primary control is the
/// `pgrust.lane_executor` bool GUC (USERSET; its session TLS backing cell is
/// read here directly, so SET / SET LOCAL re-evaluates the gate on the next
/// query). The `PGRUST_LANE_V2` boot env var sets the GUC's startup default
/// (`=0`/`off` → default off, PGC_S_ENV_VAR) and remains the fleet-harness /
/// kill-switch path.
#[inline]
pub fn enabled() -> bool {
    ::guc_tables::backing::pgrust_lane_executor()
}

/// Engagement trace (verification aid, no perf path): `PGRUST_LANE_V2_TRACE=1`
/// logs lane engagement events to stderr. Resolved once per process.
fn lane_trace(event: &str) {
    static ON: OnceLock<bool> = OnceLock::new();
    if *ON.get_or_init(|| {
        matches!(std::env::var("PGRUST_LANE_V2_TRACE").as_deref(), Ok("1") | Ok("on"))
    }) {
        eprintln!("[lane-v2] {event}");
    }
}

// ===========================================================================
// Phase-3 qual kernel: the vectorized selection-bitmap qual for lane-owned
// filtered scans. This restores the fast path the NON-lane `WithQual` drive
// already has (`scan_batch_probe` → `exec_seq_scan_batch`): a kernel-shaped
// `col CMP const` qual (`Kernel::QualScanVarCmpConst`) is evaluated over the
// whole staged page batch by `qual_bitmap_cmp_const` (execexpr/steps.rs —
// chunked so LLVM can vectorize the compare) into a selection bitmap, and the
// lane's filter/project segment iterates ONLY the survivors instead of
// running `exec_qual` scalar per staged row. All of the staging + bitmap +
// forced-fallback-bit machinery is the EXISTING `BatchSoa` flow in
// `nodeseqscan` (`seq_scan_batch_soa_prepare` / `seq_scan_next_pagebatch` /
// `seq_scan_batch_fetch`); the lane only arms it and consumes the bitmap.
// ===========================================================================

/// Arm the SoA deform + selection-bitmap qual for a lane-owned filtered
/// SeqScan pipeline. Admission generalizes `scan_batch_probe`'s to the
/// clause census: the qual must be an AND of scan-Var-CMP-Const clauses
/// (`scan_cmp_const_clauses` — non-erroring, non-volatile by construction,
/// which is why only these shapes are admitted; 1 clause = the fused
/// kernel), and `seq_scan_batch_soa_prepare` internally refuses a
/// non-fixed-width column prefix (the scalar per-row path then continues
/// unchanged). `qual_only`: single-clause staging deforms the qual column
/// only, multi-clause the clause-covering prefix; surviving rows deform
/// lazily per-row — identical to the non-lane `exec_seq_scan_batch` drive.
/// No-op (memo hit) when already armed, so per-pull callers pay one
/// load+test.
///
/// `stitch`: additionally arm the tier-2 stitched body for the qual — set
/// ONLY by drain-pipeline callers (feeds into breakers). Pull-one-tuple
/// pipelines keep the AOT bitmap tier (design rule: stitched segments exist
/// only on drain pipelines).
fn arm_seq_scan_qual_bitmap<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    ctx: &str,
    stitch: bool,
) {
    if !::nodeseqscan::seq_scan_batch_qual_bitmap_armed(ss) {
        let Some(q) = ss.ss.qual.as_deref() else { return };
        let Some(c) = q.scan_cmp_const_clauses() else {
            // strsearch contains-LIKE kernel (single `col LIKE '%lit%'`
            // clause census): varkey-staged text lane + one memmem bitmap
            // pass per batch; every other admission stays per-row. The
            // stitch arms below no-op on this shape (nquals = 0, unused
            // deform plan), so the registration is this one call.
            if q.scan_contains_clause().is_some()
                && ::nodeseqscan::seq_scan_batch_soa_prepare_contains(ss, estate)
            {
                lane_trace(&format!("seqscan contains qual bitmap armed ({ctx})"));
            }
            return;
        };
        let prefix = c.clauses[..c.n as usize]
            .iter()
            .map(|&(col, _, _)| col as i32 + 1)
            .max()
            .expect("census has at least one clause");
        // Phase-3 projection stitching (drain pipelines only, like every
        // stitched segment): when the scan's projection is census-covered,
        // widen the deform prefix to its read columns so the stitched
        // projection reads the SAME staged lanes as the qual bitmap (the
        // one-deform-two-consumers coupling; the bitmap + output lanes are
        // the only currency between the segments). If the wider prefix is
        // unarmable (a non-fixed-width column inside it), fall back to the
        // qual-only prefix — projection hosting refuses, current per-row
        // projection behavior untouched (fail closed).
        let proj_prefix = if stitch {
            ::nodeseqscan::seq_scan_proj_stitch_prefix(ss).unwrap_or(0)
        } else {
            0
        };
        if proj_prefix > prefix {
            ::nodeseqscan::seq_scan_batch_soa_prepare(ss, estate, proj_prefix, true, false, true);
        }
        if !::nodeseqscan::seq_scan_batch_qual_bitmap_armed(ss) {
            ::nodeseqscan::seq_scan_batch_soa_prepare(ss, estate, prefix, true, false, true);
        }
        if ::nodeseqscan::seq_scan_batch_qual_bitmap_armed(ss) {
            lane_trace(&format!("seqscan qual bitmap armed ({ctx})"));
        }
    }
    if stitch {
        ::nodeseqscan::seq_scan_stitch_arm(ss);
        ::nodeseqscan::seq_scan_proj_stitch_arm(ss, estate);
    }
}

/// The feed shapes that arm heap-scan staging (kernel-qual selection bitmap,
/// SoA prefix deform, stitched tiers, varlane staging) on a SeqScan before a
/// lane pipeline drives it. `arm_scan_staging` is the ONE seam owning the
/// arming decision + staging setup across every feed site (agg fold /
/// per-row build feeds, sort feed, join build and probe feeds), so a second
/// staging backend (cbstore column windows) plugs in by matching the scan's
/// source kind inside that helper — not by growing per-site variants.
enum ScanFeedShape<'a, 'mcx> {
    /// Row-emit feed with no SoA lane reader above the scan: arm the
    /// kernel-qual selection bitmap, and on drain pipelines (`stitch`) the
    /// tier-2 stitched body + projection stitching. `ctx` labels the
    /// lane-trace line.
    RowFeed { ctx: &'static str, stitch: bool },
    /// Hash-agg FOLD drain feed: varlane staging, or the fused full-prefix
    /// deform (forced when the fold reads lane columns or K2 wants the key;
    /// the kernel-qual bitmap is detected inside the prefix), falling back
    /// to the qual-only bitmap when the prefix is unarmable; stitched tiers
    /// armed (drain pipeline).
    HashAggFold { agg: &'a ::nodeagg::AggStateData<'mcx> },
    /// Hash-agg PER-ROW drain feed: unforced fused prefix (bitmap detected
    /// inside), qual-only bitmap fallback; stitched tiers armed.
    HashAggPerRow { agg: &'a ::nodeagg::AggStateData<'mcx> },
    /// Forced fold-prefix deform ONLY (no bitmap fallback, no stitch arm):
    /// the plain-agg fold feed, and the decide-phase admission probes (via
    /// `probe_arm_fold_prefix`). Reaches only unprojected scans (the fold
    /// deciders refuse projected ones before choosing Fold).
    FoldPrefix { agg: &'a ::nodeagg::AggStateData<'mcx> },
}

/// Arm the scan staging a lane feed consumes, per feed shape. Idempotent at
/// every site (re-preparing the same shape is a no-op; the bitmap arm
/// early-returns once armed). This is the single seam the cbstore staging
/// variant (CbstoreSource tranche) plugs into: every arm below stages
/// through `nodeseqscan`'s SoA seam, whose batch primitives dispatch on the
/// scan's AM inside `tableam` — heap scans stage page batches
/// (`heap_getnextpagebatch` + `heap_batch_deform_soa`), cbstore scans stage
/// column windows (`next_window` + `batch_deform`, <= WINDOW_ROWS <=
/// SOA_MAX_ROWS, RG/granule/block pruning inside the staging call) — so the
/// feed sites stay untouched and one arm serves both source kinds. The
/// cbstore-only differences live below the seam: the fill honors
/// `lane_fill_wanted`/`dict_want` (dict-lane publication), the store slot is
/// the scan's virtual slot (`store_slot`, needed columns only), and the
/// prefix publish is a virtual-slot no-op.
fn arm_scan_staging<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    shape: ScanFeedShape<'_, 'mcx>,
) -> PgResult<()> {
    // PREWHERE v1 (cbstore scans with a qual, phase4 design §3): try the
    // lane-qual arm FIRST — it subsumes the kernel-bitmap arms (staged
    // clauses cheapest-first with zone folds + per-clause late
    // materialization, the dict text tier, hybrid requal tails) over the
    // same forced full-prefix deform, widened to the feed's own SoA ask.
    // Varlane fold feeds COEXIST (q22coexist): the fold's one varlena column
    // joins the lane's prefix ask — the cbstore (virtual-)prefix deform
    // hosts any column type, and the lane's completing deform fills it for
    // survivor windows, exactly the rows the fold touches (the fold drain
    // walks the selection bitmap and the guard proof restricts to it).
    // Refusal falls through to the heap-shaped arms below (the varkey
    // staging for varlane folds), byte-safely.
    if ::nodeseqscan::seq_scan_is_cbstore(ss) {
        let min_prefix = match &shape {
            ScanFeedShape::RowFeed { .. } => 0,
            ScanFeedShape::HashAggFold { agg } | ScanFeedShape::HashAggPerRow { agg } => {
                if ss.ss.ps_ProjInfo.is_none() {
                    fused_agg_soa_prefix(agg, ss).unwrap_or(0)
                } else {
                    0
                }
            }
            ScanFeedShape::FoldPrefix { agg } => fused_agg_soa_prefix(agg, ss).unwrap_or(0),
        };
        // Every varlena fold column joins the lane's prefix ask — the single
        // varkey-shaped column AND the multi-varlena set (lane-v2-
        // dictminmax): the cbstore (virtual-)prefix deform hosts any column
        // type, and vguard columns must be staged for the fold + guard proof.
        let vcol = match &shape {
            ScanFeedShape::HashAggFold { agg } => ::nodeagg::agg_lanefold_plan(agg)
                .and_then(|p| p.vguards.iter().copied().max()),
            _ => None,
        };
        let ask = match vcol {
            Some(c) => min_prefix.max(c as i32 + 1),
            None => min_prefix,
        };
        if ::nodeseqscan::seq_scan_cb_prewhere_arm(ss, estate, ask)? {
            if vcol.is_some() {
                lane_trace("cbstore prewhere+varlane dual arm engaged");
            }
            return Ok(());
        }
    }
    match shape {
        ScanFeedShape::RowFeed { ctx, stitch } => {
            arm_seq_scan_qual_bitmap(ss, estate, ctx, stitch);
        }
        ScanFeedShape::HashAggFold { agg } => {
            // Arm the SoA page-batch deform + kernel-qual bitmap for the
            // fused drive when the whole read prefix is knowable
            // (unprojected scans only: with a projection the agg reads
            // output columns, which are not commensurable with scan-column
            // prefixes). ONE deform serves both consumers:
            // `seq_scan_batch_soa_prepare` detects the kernel qual inside
            // the prefix and arms the selection bitmap on the same staged
            // SoA the fold lanes read. When no prefix is knowable
            // (projected / shape-unknown), fall back to the qual-only bitmap
            // arm so a kernel-shaped filter still vectorizes (survivors
            // deform lazily per-row). The fold feed FORCES the deform when
            // the fold reads lane columns (the <3-column break-even is a
            // deform+gather artifact; the fold consumes the columns
            // directly).
            let soa_prefix = if ss.ss.ps_ProjInfo.is_none() {
                fused_agg_soa_prefix(agg, ss).unwrap_or(0)
            } else {
                0
            };
            if let Some(vcol) =
                ::nodeagg::agg_lanefold_plan(agg).and_then(lanefold_varlane_col)
            {
                // Varlena lane: re-arm the varkey staging (idempotent; the
                // decide-phase probe already proved it arms).
                let armed = ::nodeseqscan::seq_scan_batch_soa_prepare_varlane(ss, estate, vcol);
                debug_assert!(armed, "varlane re-arm is idempotent");
            } else if ::nodeagg::agg_lanefold_plan(agg)
                .is_some_and(|p| !p.vguards.is_empty())
            {
                // Multi-varlena fold (Q23-class): re-arm the cbstore
                // virtual-prefix staging the decide-phase probe proved
                // (idempotent). A lost arm leaves the SoA unarmed and the
                // feed's (None, _) route asserts no lane reader — so a
                // failed re-arm here would be a bug, not a silent demote.
                let armed = try_arm_cb_multivar(agg, ss, estate)?;
                debug_assert!(armed, "multi-varlena re-arm is idempotent");
            } else if soa_prefix > 0 {
                // Force the SoA deform when the fold reads lane columns, OR when
                // the K2 deferred probe could host this shape (the K2 key lane
                // must be staged even for count(*)-only plans, whose fold reads
                // nothing — the prefix covers grouping columns, so arming stages
                // the key). A non-fixed-width prefix still refuses to arm, and
                // the feed then keeps the arrival probe — byte-safe either way.
                let force = ::nodeagg::agg_lanefold_plan(agg)
                    .is_some_and(|plan| !plan.cols.is_empty())
                    || scan_k2_wanted(agg)
                    || scan_mk_plan_wanted(agg);
                let was = ::nodeseqscan::seq_scan_batch_qual_bitmap_armed(ss);
                ::nodeseqscan::seq_scan_batch_soa_prepare(ss, estate, soa_prefix, false, force, true);
                if ::nodeseqscan::seq_scan_batch_soa(ss).is_none() {
                    // Full-prefix deform unarmable (non-fixed-width column in
                    // the prefix) or declined (break-even). Try the cbstore
                    // dict-group columnar arm (§2.1) first — count(*)-only
                    // plans reach here without decide's probe-arm (their fold
                    // reads no lane columns, but K2 wants the key staged).
                    // Otherwise: a column-reading fold plan cannot get here —
                    // `decide_agg_lane` probe-armed this prefix (or armed
                    // dict-group, which the re-prepare above keeps) before
                    // choosing Fold — so the SoA has no fold reader and the
                    // qual-only bitmap arm is safe.
                    if !try_arm_cb_dictgroup(agg, ss, estate)
                        && !try_arm_cb_multikey_dict(agg, ss, estate)
                    {
                        arm_seq_scan_qual_bitmap(ss, estate, "agg fold feed, qual-only", true);
                    }
                } else if !was && ::nodeseqscan::seq_scan_batch_qual_bitmap_armed(ss) {
                    lane_trace("seqscan qual bitmap armed (agg fold fused deform)");
                }
            } else {
                // Fold with no knowable prefix = a plan reading no lane
                // columns (count(*)-only); the bitmap is the only SoA user.
                arm_seq_scan_qual_bitmap(ss, estate, "agg fold feed", true);
            }
            // Tier-2 arm for the fused-deform-armed bitmap (drain feed);
            // idempotent, no-op when the bitmap is not armed.
            ::nodeseqscan::seq_scan_stitch_arm(ss);
        }
        ScanFeedShape::HashAggPerRow { agg } => {
            // Same prefix bound and fallbacks as the fold arm (comment
            // there), but the per-row feed reads no SoA columns, so the
            // deform is never forced.
            let soa_prefix = if ss.ss.ps_ProjInfo.is_none() {
                fused_agg_soa_prefix(agg, ss).unwrap_or(0)
            } else {
                0
            };
            if soa_prefix > 0 {
                let was = ::nodeseqscan::seq_scan_batch_qual_bitmap_armed(ss);
                ::nodeseqscan::seq_scan_batch_soa_prepare(ss, estate, soa_prefix, false, false, true);
                if ::nodeseqscan::seq_scan_batch_soa(ss).is_none() {
                    // Unarmable/declined full prefix; the per-row feed reads
                    // no SoA columns, so fall back to the qual-only bitmap.
                    arm_seq_scan_qual_bitmap(ss, estate, "agg per-row feed, qual-only", true);
                } else if !was && ::nodeseqscan::seq_scan_batch_qual_bitmap_armed(ss) {
                    lane_trace("seqscan qual bitmap armed (agg per-row fused deform)");
                }
            } else {
                arm_seq_scan_qual_bitmap(ss, estate, "agg per-row feed", true);
            }
            // Tier-2 arm for the fused-deform-armed bitmap (drain feed).
            ::nodeseqscan::seq_scan_stitch_arm(ss);
        }
        ScanFeedShape::FoldPrefix { agg } => {
            let prefix = fused_agg_soa_prefix(agg, ss).unwrap_or(0);
            ::nodeseqscan::seq_scan_batch_soa_prepare(ss, estate, prefix, false, true, true);
        }
    }
    Ok(())
}

/// Multi-varlena fold staging (lane-v2-dictminmax, the Q23-class
/// `MIN(URL), MIN(Title)` shape): a plan whose lane set carries 2+ varlena
/// columns (or one varlena among fixed-width lanes) is unhostable by the
/// heap paths — the fixed-width prefix deform cannot stage `attlen == -1`
/// and the varkey pass stages exactly one column — but the cbstore
/// virtual-prefix staging hosts ANY column type. Arm it: PREWHERE for
/// qualled scans (it owns the staging + selection bitmap; ask widened to
/// every fold/vguard column), the offset-free columnar arm for bare scans.
/// False = not that shape, or the staging refused — the decider keeps the
/// per-row feed, byte-safely.
fn try_arm_cb_multivar<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if !::nodeseqscan::seq_scan_is_cbstore(ss) {
        return Ok(false);
    }
    let Some(plan) = ::nodeagg::agg_lanefold_plan(agg) else {
        return Ok(false);
    };
    if plan.vguards.is_empty() || lanefold_varlane_col(plan).is_some() {
        return Ok(false);
    }
    let Some(mut prefix) = fused_agg_soa_prefix(agg, ss) else {
        return Ok(false);
    };
    for &c in plan.cols.iter().chain(plan.vguards.iter()) {
        prefix = prefix.max(c as i32 + 1);
    }
    let armed = if ss.ss.qual.is_some() {
        ::nodeseqscan::seq_scan_cb_prewhere_arm(ss, estate, prefix)?
    } else {
        ::nodeseqscan::seq_scan_cb_columnar_arm(ss, estate, prefix, None)
    };
    Ok(armed && ::nodeseqscan::seq_scan_batch_soa(ss).is_some())
}

/// Decide-phase admission probe: arm the forced fold prefix NOW so an
/// unarmable prefix (non-fixed-width column) is known BEFORE committing to
/// ownership. Returns whether the SoA deform armed. Shared by the hashed and
/// plain fold deciders; the plain fold feed re-arms the identical shape (a
/// no-op) through `ScanFeedShape::FoldPrefix`.
fn probe_arm_fold_prefix<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    arm_scan_staging(ss, estate, ScanFeedShape::FoldPrefix { agg })?;
    Ok(::nodeseqscan::seq_scan_batch_soa(ss).is_some())
}

// ===========================================================================
// Standalone scan ownership: DELIBERATELY REFUSED (admission economics,
// design §4; measured on the integration bench 2026-07-11, q9-class).
//
// The `try_own_*` scan entry points are reached only from the per-node
// dispatch arms — i.e. only when the PARENT is a per-tuple Volcano consumer
// (lane breakers drive their scan pipelines directly, never through these
// hooks). A lane-owned scan in that position emits one tuple per pull through
// the capacity-one adapter with NO batch consumer above and NO scan kernels
// wired yet — pure adapter overhead (q9: +3–9%), and for kernel-qual'd scans
// it PREEMPTS the row executor's own fused SoA-bitmap WithQual drive.
//
// Revisited with the Phase-3 qual kernel (2026-07-11): lane-owned filtered
// scans now carry the same selection bitmap, but for a STANDALONE scan the
// incumbent per-node drive is `exec_seq_scan_batch` — the identical bitmap
// over the identical staging, with NO pull-adapter round trip per surviving
// row on top. The lane can therefore only match-or-lose here (the q9-class
// adapter overhead stands), so the refuse stays. It shrinks when standalone
// scans gain a kernel the row drive lacks (dict/PREWHERE-class); the scan
// pipelines stay fully exercised via the agg/sort/join breaker feeds.
const STANDALONE_SCAN_NO_UPSIDE: bool = true;

/// Tiny-input row floor for standalone cbstore scan admission (the
/// `TinyInputFloor` refuse): relations below this never pay the qual-
/// translate/arm admission cascade. Default = one cbstore granule (8,192
/// rows — the store's decode/zone unit; a sub-granule scan is a handful of
/// staged windows either way, bench 2026-07-12: lane-ON == lane-OFF to noise
/// at this size, so the cascade is pure tax). `PGRUST_LANE_V2_TINY_FLOOR`
/// overrides for floor-calibration benches.
fn cb_tiny_floor() -> u64 {
    static FLOOR: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *FLOOR.get_or_init(|| {
        std::env::var("PGRUST_LANE_V2_TINY_FLOOR")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8_192)
    })
}

// ===========================================================================
// SeqScan ownership (Phase 1 first vertical slice, now push-driven). The
// pipeline is source → filter/project operator → root pull-adapter, over the
// same `BatchSource`-seam primitives the pull drive used
// (`seq_scan_next_pagebatch` / `seq_scan_batch_emit`).
// ===========================================================================

/// Try to let the lane *own* a `SeqScan` (scan→filter→project,
/// scalar-within-lane over row batches).
///
/// `Some(result)` = the lane drove this call (`result` is the tuple-or-end,
/// the ordinary `ExecProcNode` return); `None` = refused, and the caller must
/// run the unchanged `exec_seq_scan`. Refusing is always byte-safe.
#[inline]
pub fn try_own_seq_scan<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Standalone scan ownership: refused for heap (STANDALONE_SCAN_NO_UPSIDE
    // — the incumbent row drive carries the identical kernels), but ADMITTED
    // for cbstore scans WITH AN ARMED QUAL KERNEL: the documented exception
    // (phase4 design §7 / design-doc §4 "shrinks when standalone scans gain
    // a kernel the row drive lacks"). The incumbent cbstore per-row drive
    // (`getnextslot`) has NO SoA staging, NO kernel-qual bitmap and NO
    // dict/PREWHERE tier, so lane ownership of a QUAL'D scan is staged-
    // kernel upside. A kernel-less cbstore scan (no qual, or an unarmable
    // one) is the heap case exactly — per-pull capacity-one adapter overhead
    // with nothing to vectorize — and REFUSES: bench-gated 2026-07-12 on the
    // 2M-row cbstore microbench, where unconditional admission ran
    // count-star 1.33x, group-int (sorted-agg pull feed) 1.21x and
    // merge-join-agg 1.10x lane-ON vs lane-OFF while the qual'd shapes won
    // 0.45-0.84x; the qual-armed gate keeps the wins and returns the rest
    // to the per-row drive. Per-PULL tick cadence (once per call).
    let is_cb = ::nodeseqscan::seq_scan_is_cbstore(ss);
    if STANDALONE_SCAN_NO_UPSIDE && !is_cb {
        stats::tick_refused(ShapeClass::SeqScan, RefuseReason::AdmissionEconomicsNoConsumer);
        return Ok(None);
    }
    if is_cb {
        // Memoized per node: the arm outcome is static, and a refused scan
        // must not re-run the fusibility + arm cascade per pulled tuple
        // (measured +20% on kernel-less count(*) shapes). A refusal is
        // byte-safe regardless of the dynamic gates, so the memoized-false
        // path is one branch; the admitted path still re-checks the
        // dynamic gates inside seq_scan_fusible every call.
        // Refusal split (refusal-audit rider, 2026-07-14): a QUAL'D scan
        // that failed to arm any staged kernel is "qual-not-vectorizable"
        // (the walker/translate residual — the countable survivor of the
        // dead fixed-width-prefix refusal); a kernel-less NO-QUAL scan is
        // the plain admission-economics refuse. Stateless per pull off the
        // memoized verdict.
        let refused_reason = if ss.ss.qual.is_some() {
            RefuseReason::QualNotVectorizable
        } else {
            RefuseReason::AdmissionEconomicsNoConsumer
        };
        match ss.cb_standalone_verdict() {
            Some(false) => {
                stats::tick_refused(
                    ShapeClass::CbScan,
                    if ss.cb_standalone_tiny() {
                        RefuseReason::TinyInputFloor
                    } else {
                        refused_reason
                    },
                );
                return Ok(None);
            }
            Some(true) => {
                if !seq_scan_fusible(ss, estate)? {
                    return Ok(None);
                }
            }
            None => {
                // Tiny-input floor (§4 endgame refuse-set, armed with the
                // noqualfeed tranche): below the floor the whole scan fits a
                // handful of windows, so lane ownership can never recover
                // even its own admission cascade (qual walk + translate +
                // arm). Checked BEFORE the cascade — the refuse costs one
                // footer metadata read, memoized. Floor = one granule
                // (8,192 rows, cbstore's zone/decode unit); PGRUST_LANE_V2_
                // TINY_FLOOR overrides for floor-calibration benches.
                if let Some(rows) = ::nodeseqscan::seq_scan_cb_total_rows(ss, estate)? {
                    if rows < cb_tiny_floor() {
                        ss.set_cb_standalone_tiny();
                        ss.set_cb_standalone_verdict(false);
                        stats::tick_refused(ShapeClass::CbScan, RefuseReason::TinyInputFloor);
                        return Ok(None);
                    }
                }
                // First call: never memoize on a dynamic-gate refusal.
                if !seq_scan_fusible(ss, estate)? {
                    return Ok(None);
                }
                // Arm the qual staging (PREWHERE lane or kernel bitmap).
                // Stitch stays off: tier-2 bodies are drain-pipeline-only,
                // and this is a per-pull feed.
                arm_scan_staging(ss, estate, ScanFeedShape::RowFeed { ctx: "standalone cbstore scan", stitch: false })?;
                let armed = ::nodeseqscan::seq_scan_batch_qual_bitmap_armed(ss);
                ss.set_cb_standalone_verdict(armed);
                if !armed {
                    stats::tick_refused(ShapeClass::CbScan, refused_reason);
                    return Ok(None);
                }
            }
        }
    } else if !seq_scan_fusible(ss, estate)? {
        return Ok(None);
    }
    debug_assert!(::types_scan::sdir::ScanDirectionIsForward(estate.es_direction));
    // Assemble the scan-only push pipeline. Stages are stateless unit structs
    // (cross-call position is node-resident), so per-call assembly is free.
    // End-of-stream mirrors ExecScanExtended's projected-slot clear (the
    // non-projected path returns end-of-scan without clearing).
    let clear_on_finish = ss.ss.ps_ProjInfo.as_ref().map(|p| p.pi_result_slot);
    let mut root = RootAdapter::new(clear_on_finish);
    Ok(Some(pull_step(ss, &mut SeqScanSource, &mut SeqScanFilterProject, &mut root, estate)?))
}

/// Refuse-set for the lane-v2 SeqScan pipeline (false → the caller falls
/// through to `exec_seq_scan`, byte-identically). Admits Plain / WithQual /
/// WithProject / WithQualProject over a page-batch-supporting AM.
/// Subplan- and param-bearing quals/projections are admitted (Phase 2):
/// `seq_scan_batch_emit` now runs `exec_scan_impl`'s exact arms for them —
/// pending-initplan param evaluation before the qual (and before the
/// projection, only on qual-passing rows), and the suspension-driven
/// `exec_qual_with_subplans` / `exec_project_with_subplans` drivers — so
/// initplan params demand-evaluate identically and correlated subplans run
/// scalar-per-batch-row through the same `nodesubplan` machinery.
///
/// Disarms on: EPQ, a backward/mark cursor (init eflags) or a non-forward
/// call, EXPLAIN ANALYZE (instrumented), the Bloom/EPQ variants, and AMs
/// without page-batch support. Parallel scans (leader or worker) are
/// admitted: the batched page feed acquires blocks through the shared DSM
/// block cursor (`parallel_next_block`), exactly as the per-tuple pagemode
/// walk does, so per-worker page batches partition the relation without
/// gaps or overlaps.
fn seq_scan_fusible<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // Engagement class: cbstore scans are counted apart (their admission
    // economics and corpus differ — see ShapeClass::CbScan).
    let class = if ::nodeseqscan::seq_scan_is_cbstore(ss) {
        ShapeClass::CbScan
    } else {
        ShapeClass::SeqScan
    };
    // Dynamic per-call gates: these may legitimately vary call to call.
    if estate.es_epq_active {
        stats::tick_refused(class, RefuseReason::Epq);
        return Ok(false);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(class, RefuseReason::Backward);
        return Ok(false);
    }
    // Static verdict, memoized on the node at first evaluation: (a) stability
    // — a mid-scan REFUSE→OWN flip would silently skip the staged remainder
    // of the current page batch; (b) the fusibility cascade (expr walks + AM
    // probe) must not run once per pulled tuple on the Volcano hot path.
    // Engagement accounting ticks exactly here — once per memoized decision.
    if let Some(v) = ss.lane_verdict() {
        return Ok(v);
    }
    let refuse = seq_scan_refuse_reason(ss, estate)?;
    let v = match refuse {
        None => {
            stats::tick_owned(class);
            true
        }
        Some(r) => {
            stats::tick_refused(class, r);
            false
        }
    };
    ss.set_lane_verdict(v);
    Ok(v)
}

/// The call-invariant half of the SeqScan refuse-set: plan shape, init-time
/// eflags, parallel wiring, instrumentation, and AM page-batch support.
/// `None` = admitted; `Some(reason)` = refused (the caller ticks accounting).
fn seq_scan_refuse_reason<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<RefuseReason>> {
    if !ss.batch_allowed() {
        return Ok(Some(RefuseReason::ScrollMark));
    }
    if ss.ss.instr_idx.is_some() {
        return Ok(Some(RefuseReason::Instrumented));
    }
    match ss.variant() {
        ::nodeseqscan::SeqScanVariant::Plain
        | ::nodeseqscan::SeqScanVariant::WithQual
        | ::nodeseqscan::SeqScanVariant::WithProject
        | ::nodeseqscan::SeqScanVariant::WithQualProject => {}
        ::nodeseqscan::SeqScanVariant::PlainBloom => {
            return Ok(Some(RefuseReason::BloomVariant))
        }
        ::nodeseqscan::SeqScanVariant::Epq => return Ok(Some(RefuseReason::Epq)),
    }
    // AM must support the page-batch primitives (opens the scan desc once).
    // The parallel-admitting variant: only this lane routes through it; the
    // fused agg/sort/hash drives keep `seq_scan_batch_supported`'s
    // serial-only gate.
    Ok(if ::nodeseqscan::seq_scan_batch_supported_parallel(ss, estate)? {
        None
    } else {
        Some(RefuseReason::NoPageBatch)
    })
}

/// Push source: stages heap page batches (`seq_scan_next_pagebatch` — the
/// same `BatchSource`-seam primitive `SeqScanBatchSource` wraps). Staging
/// resets the node-resident consume cursor: a fresh batch replaces the staged
/// rows.
struct SeqScanSource;

impl<'mcx> Source<'mcx> for SeqScanSource {
    type Node = ::nodeseqscan::SeqScanState<'mcx>;

    fn produce(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<Batch>> {
        let n = ::nodeseqscan::seq_scan_next_pagebatch(node, estate)?;
        node.set_lane_cursor(0, n);
        if n == 0 {
            // End of scan: the per-tuple path's getnextslot clears the scan
            // slot on exhaustion (dropping its buffer pin); match it so a
            // lane-owned scan does not hold a pin until rescan/end.
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(node.ss.ss_ScanTupleSlot), mcx);
        }
        Ok((n > 0).then_some(Batch { n }))
    }
}

/// Push operator: the scan's filter→project segment. Consumes the staged
/// batch via `seq_scan_batch_emit` — `ExecScanExtended`'s body over a staged
/// batch row (reset per-tuple context, store + apply the scan qual via
/// `execexpr`, project) — pushing each surviving output slot into the sink.
/// Kernel-shaped quals (`QualScanVarCmpConst`, armed by
/// `arm_seq_scan_qual_bitmap` or the agg's fused full-prefix deform) run
/// vectorized: the staging computed a whole-batch selection bitmap
/// (`qual_bitmap_cmp_const`), and this operator walks only the survivors;
/// all other quals run scalar per-row. Filter and projection stay fused
/// within this one segment operator per the operator-model decision (design
/// §1): the push conversion inverts driver control, never the fused per-row
/// segment. Same tuples, same order, same qual/proj/NULL semantics as
/// `exec_seq_scan` → BYTE-IDENTICAL.
///
/// The consume position over the staged page batch lives on the node
/// (`SeqScanState::lane_cursor`), so a `Paused` pipeline survives the Volcano
/// per-call boundary.
struct SeqScanFilterProject;

impl<'mcx> Operator<'mcx> for SeqScanFilterProject {
    type Node = ::nodeseqscan::SeqScanState<'mcx>;

    fn pending(&self, node: &Self::Node) -> Option<Batch> {
        let (pos, n) = node.lane_cursor();
        (pos < n).then_some(Batch { n })
    }

    fn consume(
        &mut self,
        node: &mut Self::Node,
        batch: Batch,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        // Phase-3 qual kernel: when the kernel-shaped qual bitmap is staged
        // for this batch (`seq_scan_next_pagebatch` ran `qual_bitmap_cmp_const`
        // over the SoA qual column at staging), iterate ONLY the selection
        // survivors — bitmap hits plus forced fallback bits, which
        // `seq_scan_batch_fetch` re-checks per-row inside the emit — instead
        // of running the scalar qual on every staged row. Survivors come out
        // in ascending row order: same rows, same order, same per-row
        // emit/projection semantics as the scalar walk → byte-identical (the
        // kernel is non-erroring/non-volatile by admission, so skipped rows
        // have no observable evaluation). The bitmap cursor is node-resident,
        // so a `Paused` pipeline resumes exactly; `lane_cursor` is kept in
        // step for `pending`. Interrupt cadence: one check per survivor and
        // at least one per staged page (no coarser than the page-level check
        // in `heap_fetch_next_buffer` the incumbent batch drive relies on).
        if ::nodeseqscan::seq_scan_batch_qual_bitmap_ready(node) {
            loop {
                ::postgres_seams::check_for_interrupts::call()?;
                let Some(i) = ::nodeseqscan::seq_scan_batch_next_selected(node) else {
                    node.set_lane_cursor(batch.n, batch.n);
                    return Ok(OpStatus::NeedInput);
                };
                node.set_lane_cursor(i + 1, batch.n);
                if let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(node, estate, i)? {
                    if let SinkFeed::Full = out.accept(slot, estate)? {
                        return Ok(OpStatus::Paused);
                    }
                }
            }
        }
        loop {
            let (pos, n) = node.lane_cursor();
            debug_assert_eq!(n, batch.n);
            if pos >= n {
                return Ok(OpStatus::NeedInput);
            }
            // Match the per-tuple path's interrupt cadence: `exec_scan_fetch`
            // runs `check_for_interrupts` once per tuple attempt. Skipping it
            // in the batched drive would process pending interrupts / cache
            // invalidations at a different cadence than the code the lane
            // replaces; keep it identical.
            ::postgres_seams::check_for_interrupts::call()?;
            node.set_lane_cursor(pos + 1, n);
            if let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(node, estate, pos)? {
                if let SinkFeed::Full = out.accept(slot, estate)? {
                    return Ok(OpStatus::Paused);
                }
            }
        }
    }

    fn consume_batch<K: BatchSink<'mcx>>(
        &mut self,
        node: &mut Self::Node,
        batch: Batch,
        out: &mut K,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        let (pos, n) = node.lane_cursor();
        debug_assert_eq!(n, batch.n);
        out.accept_batch(&mut SeqScanBatchEmit { node }, pos, n, estate)?;
        // One cursor save per batch (not per row): breaker sinks never pause,
        // an error mid-batch aborts the query, and a rescan restages.
        node.set_lane_cursor(n, n);
        Ok(OpStatus::NeedInput)
    }

    fn arm_sort_key(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> bool {
        // The incumbent fused sort drive's matcher, shared: no qual, output
        // column 0 is exactly one scan Var the SoA plan covers.
        ::nodeseqscan::seq_scan_sortkey_direct(node, estate)
    }
}

/// `SeqScanFilterProject`'s per-row body as a `BatchEmit` face: identical
/// primitive (`seq_scan_batch_emit`) at the identical per-row interrupt
/// cadence (`consume` runs `check_for_interrupts` once per tuple attempt,
/// matching `exec_scan_fetch`).
struct SeqScanBatchEmit<'a, 'mcx> {
    node: &'a mut ::nodeseqscan::SeqScanState<'mcx>,
}

impl<'mcx> BatchEmit<'mcx> for SeqScanBatchEmit<'_, 'mcx> {
    #[inline]
    fn emit(
        &mut self,
        i: u32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        ::postgres_seams::check_for_interrupts::call()?;
        ::nodeseqscan::seq_scan_batch_emit(self.node, estate, i)
    }

    /// Direct sort-key read (armed by `arm_sort_key`): value/null straight
    /// from the staged SoA key column — no per-row interrupt seam, exactly
    /// the incumbent `SeqScanSortSource::emit_key` cadence (page-level CFI
    /// inside the staging fetch covers the batch).
    #[inline(always)]
    fn emit_key(&mut self, i: u32) -> Option<(::datum::Datum, bool)> {
        ::nodeseqscan::seq_scan_batch_key(self.node, i)
    }

    #[inline]
    fn topk_key_lane(&self, n: u32) -> Option<(&[::datum::Datum], &[bool], &[u64])> {
        ::nodeseqscan::seq_scan_topk_key_lane(self.node, n)
    }

    #[inline]
    fn push_topk_bound(&mut self, key: ::datum::Datum) {
        ::nodeseqscan::seq_scan_adaptive_push_bound(self.node, key);
    }

    #[inline]
    fn key_dict_lane(&self) -> Option<::exectuples::SoaDictLane> {
        ::nodeseqscan::seq_scan_batch_key_dict_lane(self.node)
    }
}

// ===========================================================================
// IndexScan ownership (Phase 1 breadth, now push-driven). Same pipeline shape
// over the SAME batch primitives the fused-agg path uses
// (`index_scan_next_tidrun` / `index_scan_batch_fetch`). The admitted shape is
// deliberately narrow — no qual, no projection, no runtime keys, forward btree
// — so the node's output is exactly the stored scan tuple: `exec_index_scan`
// over that shape is `exec_scan_extended::<false,false>` (reset ctx, fetch,
// return the scan slot). Same visible tuples, same index order → BYTE-IDENTICAL.
// ===========================================================================

/// Try to let the lane own an `IndexScan`. `Some` = lane drove this call;
/// `None` = refused (caller runs the unchanged `exec_index_scan`).
#[inline]
pub fn try_own_index_scan<'mcx>(
    is: &mut ::nodeindexscan::IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Standalone scan ownership: refused, see STANDALONE_SCAN_NO_UPSIDE.
    // Per-PULL tick cadence (this hook runs once per exec_proc_node call).
    if STANDALONE_SCAN_NO_UPSIDE {
        stats::tick_refused(ShapeClass::IndexScan, RefuseReason::AdmissionEconomicsNoConsumer);
        return Ok(None);
    }
    if !index_scan_fusible(is, estate) {
        return Ok(None);
    }
    debug_assert!(::types_scan::sdir::ScanDirectionIsForward(estate.es_direction));
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step(is, &mut IndexScanSource, &mut IndexScanEmit, &mut root, estate)?))
}

/// Refuse-set for the lane-v2 IndexScan pipeline. Admits only the shape the
/// fused-agg index arm admits (no qual / no projection / no runtime keys /
/// forward index order / btree AM / MVCC), plus the lane-specific disarms:
/// EPQ, a non-forward call, a scrollable/backward or mergejoin-mark cursor
/// (`!batch_allowed` — mark/restore + backward desync the tidrun cursor),
/// parallel, EXPLAIN ANALYZE (instrumented), and any amcanorderbyop reorder
/// (`iss_OrderBy`) which the tidrun path does not reorder.
fn index_scan_fusible<'mcx>(
    is: &::nodeindexscan::IndexScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> bool {
    // This gate is per-call (not node-memoized), so accounting ticks are
    // per-pull decisions for this class — see `stats.rs` tick semantics.
    match index_scan_refuse_reason(is, estate) {
        None => {
            stats::tick_owned(ShapeClass::IndexScan);
            true
        }
        Some(r) => {
            stats::tick_refused(ShapeClass::IndexScan, r);
            false
        }
    }
}

/// `None` = admitted; `Some(reason)` = refused.
fn index_scan_refuse_reason<'mcx>(
    is: &::nodeindexscan::IndexScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> Option<RefuseReason> {
    if estate.es_epq_active {
        return Some(RefuseReason::Epq);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        return Some(RefuseReason::Backward);
    }
    if !is.batch_allowed() {
        return Some(RefuseReason::ScrollMark);
    }
    if is.iss_ParallelAware {
        return Some(RefuseReason::ParallelGate);
    }
    if is.ss.instr_idx.is_some() {
        return Some(RefuseReason::Instrumented);
    }
    // Same-block tidrun batching is only sound under an MVCC snapshot (matches
    // the fused-agg gate; non-MVCC keeps the per-tuple path).
    if !estate
        .es_snapshot
        .as_deref()
        .is_some_and(::types_snapshot::IsMVCCSnapshot)
    {
        return Some(RefuseReason::NonMvccSnapshot);
    }
    if is.ss.qual.is_some() || is.ss.ps_ProjInfo.is_some() {
        return Some(RefuseReason::ShapeQualProj);
    }
    if is.iss_Runtime.is_some() {
        return Some(RefuseReason::RuntimeKeys);
    }
    if is.iss_OrderBy.is_some() {
        return Some(RefuseReason::OrderByReorder);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(is.iss_OrderDir) {
        return Some(RefuseReason::Backward);
    }
    if !is
        .iss_RelationDesc
        .as_ref()
        .is_some_and(|r| r.rd_rel.relam == ::types_core::BTREE_AM_OID)
    {
        return Some(RefuseReason::NonBtree);
    }
    None
}

/// Push source: stages a same-block TID run (`index_scan_next_tidrun`, which
/// runs `check_for_interrupts` per run, matching the fused-agg drive this
/// reuses). Staging resets the node-resident consume cursor.
struct IndexScanSource;

impl<'mcx> Source<'mcx> for IndexScanSource {
    type Node = ::nodeindexscan::IndexScanState<'mcx>;

    fn produce(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<Batch>> {
        let n = ::nodeindexscan::index_scan_next_tidrun(node, estate)?;
        node.set_lane_cursor(0, n);
        if n == 0 {
            // End of scan: C's IndexNext clears the scan slot on exhaustion
            // (dropping its buffer pin); match it.
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(node.ss.ss_ScanTupleSlot), mcx);
        }
        Ok((n > 0).then_some(Batch { n }))
    }
}

/// Push operator: replays the staged TID run one visible tuple at a time
/// (`index_scan_batch_fetch`, sequential: entry `i>0` advances the AM cursor,
/// so the run is consumed 0,1,2,… without gaps). No qual/projection → the
/// pushed tuple is the stored scan slot. The run position lives on the node
/// (`IndexScanState::lane_cursor`) to survive the Volcano call boundary.
struct IndexScanEmit;

impl<'mcx> Operator<'mcx> for IndexScanEmit {
    type Node = ::nodeindexscan::IndexScanState<'mcx>;

    fn pending(&self, node: &Self::Node) -> Option<Batch> {
        let (pos, n) = node.lane_cursor();
        (pos < n).then_some(Batch { n })
    }

    fn consume(
        &mut self,
        node: &mut Self::Node,
        batch: Batch,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        let scan_id = node.ss.ss_ScanTupleSlot;
        loop {
            let (pos, n) = node.lane_cursor();
            debug_assert_eq!(n, batch.n);
            if pos >= n {
                return Ok(OpStatus::NeedInput);
            }
            node.set_lane_cursor(pos + 1, n);
            if ::nodeindexscan::index_scan_batch_fetch(node, estate, pos)? {
                if let SinkFeed::Full = out.accept(scan_id, estate)? {
                    return Ok(OpStatus::Paused);
                }
            }
        }
    }

    fn consume_batch<K: BatchSink<'mcx>>(
        &mut self,
        node: &mut Self::Node,
        batch: Batch,
        out: &mut K,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        let (pos, n) = node.lane_cursor();
        debug_assert_eq!(n, batch.n);
        out.accept_batch(&mut IndexScanBatchEmit { node }, pos, n, estate)?;
        node.set_lane_cursor(n, n);
        Ok(OpStatus::NeedInput)
    }
}

/// `IndexScanEmit`'s per-row body as a `BatchEmit` face (no per-row CFI —
/// `index_scan_next_tidrun` runs it per run, exactly as `consume`). The run
/// is consumed sequentially 0,1,2,… by construction (`pos..n`).
struct IndexScanBatchEmit<'a, 'mcx> {
    node: &'a mut ::nodeindexscan::IndexScanState<'mcx>,
}

impl<'mcx> BatchEmit<'mcx> for IndexScanBatchEmit<'_, 'mcx> {
    #[inline]
    fn emit(
        &mut self,
        i: u32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        Ok(::nodeindexscan::index_scan_batch_fetch(self.node, estate, i)?
            .then_some(self.node.ss.ss_ScanTupleSlot))
    }
}

// ===========================================================================
// IndexOnlyScan ownership (push-driven). `index_only_scan_batch_next` advances
// to the next VISIBLE index tuple (VM probe / heap fallback / predicate lock —
// C's IndexOnlyNext order) and returns 0 or 1; `index_only_scan_batch_store`
// stages `xs_itup` into the scan slot. The source produces one-row batches, so
// a batch never outlives the driver round that produced it — no node-resident
// cursor. Narrow shape (no qual / no projection / no runtime keys / forward
// btree) → the output is the stored scan tuple, identical to
// `exec_index_only_scan`.
// ===========================================================================

/// Try to let the lane own an `IndexOnlyScan`.
#[inline]
pub fn try_own_index_only_scan<'mcx>(
    ios: &mut ::nodeindexonlyscan::IndexOnlyScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Standalone scan ownership: refused, see STANDALONE_SCAN_NO_UPSIDE.
    // Per-PULL tick cadence (this hook runs once per exec_proc_node call).
    if STANDALONE_SCAN_NO_UPSIDE {
        stats::tick_refused(
            ShapeClass::IndexOnlyScan,
            RefuseReason::AdmissionEconomicsNoConsumer,
        );
        return Ok(None);
    }
    if !index_only_scan_fusible(ios, estate) {
        return Ok(None);
    }
    debug_assert!(::types_scan::sdir::ScanDirectionIsForward(estate.es_direction));
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step(ios, &mut IndexOnlyScanSource, &mut IndexOnlyScanEmit, &mut root, estate)?))
}

/// Refuse-set for the lane-v2 IndexOnlyScan pipeline (mirrors the fused-agg
/// IOS arm + the lane disarms). `!batch_allowed` refuses a scrollable/backward
/// or mergejoin-mark cursor; `ioss_OrderByKeys` non-empty refuses
/// amcanorderbyop (distance-ordered) scans.
fn index_only_scan_fusible<'mcx>(
    ios: &::nodeindexonlyscan::IndexOnlyScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> bool {
    // Per-call gate: accounting ticks are per-pull decisions for this class.
    match index_only_scan_refuse_reason(ios, estate) {
        None => {
            stats::tick_owned(ShapeClass::IndexOnlyScan);
            true
        }
        Some(r) => {
            stats::tick_refused(ShapeClass::IndexOnlyScan, r);
            false
        }
    }
}

/// `None` = admitted; `Some(reason)` = refused.
fn index_only_scan_refuse_reason<'mcx>(
    ios: &::nodeindexonlyscan::IndexOnlyScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> Option<RefuseReason> {
    if estate.es_epq_active {
        return Some(RefuseReason::Epq);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        return Some(RefuseReason::Backward);
    }
    if !ios.batch_allowed() {
        return Some(RefuseReason::ScrollMark);
    }
    if ios.ioss_ParallelAware {
        return Some(RefuseReason::ParallelGate);
    }
    if ios.ss.instr_idx.is_some() {
        return Some(RefuseReason::Instrumented);
    }
    if !estate
        .es_snapshot
        .as_deref()
        .is_some_and(::types_snapshot::IsMVCCSnapshot)
    {
        return Some(RefuseReason::NonMvccSnapshot);
    }
    if ios.ss.qual.is_some() || ios.ss.ps_ProjInfo.is_some() {
        return Some(RefuseReason::ShapeQualProj);
    }
    if ios.ioss_Runtime.is_some() {
        return Some(RefuseReason::RuntimeKeys);
    }
    if !ios.ioss_OrderByKeys.is_empty() {
        return Some(RefuseReason::OrderByReorder);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(ios.ioss_OrderDir) {
        return Some(RefuseReason::Backward);
    }
    if !ios
        .ioss_RelationDesc
        .as_ref()
        .is_some_and(|r| r.rd_rel.relam == ::types_core::BTREE_AM_OID)
    {
        return Some(RefuseReason::NonBtree);
    }
    None
}

/// Push source: one VISIBLE index tuple per batch (`index_only_scan_batch_next`
/// runs `check_for_interrupts` per tuple).
struct IndexOnlyScanSource;

impl<'mcx> Source<'mcx> for IndexOnlyScanSource {
    type Node = ::nodeindexonlyscan::IndexOnlyScanState<'mcx>;

    fn produce(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<Batch>> {
        let n = ::nodeindexonlyscan::index_only_scan_batch_next(node, estate)?;
        if n == 0 {
            // End of scan: C's IndexOnlyNext clears the scan slot on
            // exhaustion; match it.
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(node.ss.ss_ScanTupleSlot), mcx);
            return Ok(None);
        }
        debug_assert_eq!(n, 1);
        Ok(Some(Batch { n }))
    }
}

/// Push operator: stages `xs_itup` into the scan slot and pushes it. One-row
/// batches are always fully consumed within the producing driver round, so
/// `pending` is statically `None` (the drive is stateless across the Volcano
/// boundary — no cursor).
struct IndexOnlyScanEmit;

impl<'mcx> Operator<'mcx> for IndexOnlyScanEmit {
    type Node = ::nodeindexonlyscan::IndexOnlyScanState<'mcx>;

    fn pending(&self, _node: &Self::Node) -> Option<Batch> {
        None
    }

    fn consume(
        &mut self,
        node: &mut Self::Node,
        batch: Batch,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        debug_assert_eq!(batch.n, 1);
        ::nodeindexonlyscan::index_only_scan_batch_store(node, estate)?;
        Ok(match out.accept(node.ss.ss_ScanTupleSlot, estate)? {
            SinkFeed::Full => OpStatus::Paused,
            SinkFeed::NeedMore => OpStatus::NeedInput,
        })
    }
}

// ===========================================================================
// BitmapHeapScan ownership (push-driven). The bitmap must be built before the
// pipeline runs — the dispatch hook keeps the arm's existing
// `bitmap_table_scan_setup_dispatch` call, then offers the
// (already-initialized) scan to the lane. Same pipeline shape as the SeqScan
// lane over the page-batch primitives (`bitmap_scan_next_pagebatch` /
// `bitmap_scan_batch_fetch`, random-access by `i`); `bitmap_scan_batch_fetch`
// applies the page recheck (`bitmapqualorig`) internally on lossy/recheck
// pages, exactly as `BitmapHeapNext` does. Narrow shape (no scan qual / no
// projection) → the output is the stored scan tuple.
// ===========================================================================

/// Try to let the lane own a `BitmapHeapScan`. The caller must have already
/// run the bitmap setup (the arm does, unconditionally, before this).
#[inline]
pub fn try_own_bitmap_heap_scan<'mcx>(
    bhs: &mut ::nodebitmapheapscan::BitmapHeapScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Standalone scan ownership: refused, see STANDALONE_SCAN_NO_UPSIDE.
    // Per-PULL tick cadence (this hook runs once per exec_proc_node call).
    if STANDALONE_SCAN_NO_UPSIDE {
        stats::tick_refused(
            ShapeClass::BitmapHeapScan,
            RefuseReason::AdmissionEconomicsNoConsumer,
        );
        return Ok(None);
    }
    if !bitmap_heap_scan_fusible(bhs, estate) {
        return Ok(None);
    }
    debug_assert!(::types_scan::sdir::ScanDirectionIsForward(estate.es_direction));
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step(bhs, &mut BitmapHeapScanSource, &mut BitmapHeapScanEmit, &mut root, estate)?))
}

/// Refuse-set for the lane-v2 BitmapHeapScan pipeline (mirrors the fused-agg
/// bitmap arm: no scan qual / no projection). Disarms EPQ, non-forward,
/// parallel (aware or a worker attached to shared state), and EXPLAIN ANALYZE.
/// Also refuses when the page recheck qual (`bitmapqualorig`) carries a subplan
/// or exec-param — the recheck runs a plain `exec_qual` that evaluates neither.
/// Bitmap scans are never scrollable/mark cursors (planner-guaranteed; a SCROLL
/// cursor gets a Material parent), so no eflags gate is needed. Bitmap init
/// asserts an MVCC snapshot, so that is implicit.
fn bitmap_heap_scan_fusible<'mcx>(
    bhs: &::nodebitmapheapscan::BitmapHeapScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> bool {
    // Per-call gate: accounting ticks are per-pull decisions for this class.
    match bitmap_heap_scan_refuse_reason(bhs, estate) {
        None => {
            stats::tick_owned(ShapeClass::BitmapHeapScan);
            true
        }
        Some(r) => {
            stats::tick_refused(ShapeClass::BitmapHeapScan, r);
            false
        }
    }
}

/// `None` = admitted; `Some(reason)` = refused.
fn bitmap_heap_scan_refuse_reason<'mcx>(
    bhs: &::nodebitmapheapscan::BitmapHeapScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> Option<RefuseReason> {
    if estate.es_epq_active {
        return Some(RefuseReason::Epq);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        return Some(RefuseReason::Backward);
    }
    if bhs.parallel_aware || bhs.pstate.is_some() {
        return Some(RefuseReason::ParallelGate);
    }
    if bhs.ss.instr_idx.is_some() {
        return Some(RefuseReason::Instrumented);
    }
    if bhs
        .bitmapqualorig
        .as_deref()
        .is_some_and(|q| q.has_subplan() || !q.param_exec_deps().is_empty())
    {
        return Some(RefuseReason::SubplanParam);
    }
    if bhs.ss.qual.is_some() || bhs.ss.ps_ProjInfo.is_some() {
        return Some(RefuseReason::ShapeQualProj);
    }
    None
}

/// Push source: stages the next bitmap page's tuples
/// (`bitmap_scan_next_pagebatch` runs `check_for_interrupts` per page).
/// Staging resets the node-resident consume cursor.
struct BitmapHeapScanSource;

impl<'mcx> Source<'mcx> for BitmapHeapScanSource {
    type Node = ::nodebitmapheapscan::BitmapHeapScanState<'mcx>;

    fn produce(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<Batch>> {
        let n = ::nodebitmapheapscan::bitmap_scan_next_pagebatch(node, estate)?;
        node.set_lane_cursor(0, n);
        if n == 0 {
            // End of scan: C's BitmapHeapNext returns ExecClearTuple(slot) on
            // exhaustion (dropping its buffer pin); match it.
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(node.ss.ss_ScanTupleSlot), mcx);
        }
        Ok((n > 0).then_some(Batch { n }))
    }
}

/// Push operator: pushes each surviving row of the staged page
/// (`bitmap_scan_batch_fetch` applies the page recheck on lossy pages). The
/// page-batch position lives on the node (`BitmapHeapScanState::lane_cursor`).
struct BitmapHeapScanEmit;

impl<'mcx> Operator<'mcx> for BitmapHeapScanEmit {
    type Node = ::nodebitmapheapscan::BitmapHeapScanState<'mcx>;

    fn pending(&self, node: &Self::Node) -> Option<Batch> {
        let (pos, n) = node.lane_cursor();
        (pos < n).then_some(Batch { n })
    }

    fn consume(
        &mut self,
        node: &mut Self::Node,
        batch: Batch,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        let scan_id = node.ss.ss_ScanTupleSlot;
        loop {
            let (pos, n) = node.lane_cursor();
            debug_assert_eq!(n, batch.n);
            if pos >= n {
                return Ok(OpStatus::NeedInput);
            }
            node.set_lane_cursor(pos + 1, n);
            if ::nodebitmapheapscan::bitmap_scan_batch_fetch(node, estate, pos)? {
                if let SinkFeed::Full = out.accept(scan_id, estate)? {
                    return Ok(OpStatus::Paused);
                }
            }
        }
    }

    fn consume_batch<K: BatchSink<'mcx>>(
        &mut self,
        node: &mut Self::Node,
        batch: Batch,
        out: &mut K,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        let (pos, n) = node.lane_cursor();
        debug_assert_eq!(n, batch.n);
        out.accept_batch(&mut BitmapHeapScanBatchEmit { node }, pos, n, estate)?;
        node.set_lane_cursor(n, n);
        Ok(OpStatus::NeedInput)
    }
}

/// `BitmapHeapScanEmit`'s per-row body as a `BatchEmit` face (no per-row CFI
/// — `bitmap_scan_next_pagebatch` runs it per page, exactly as `consume`).
struct BitmapHeapScanBatchEmit<'a, 'mcx> {
    node: &'a mut ::nodebitmapheapscan::BitmapHeapScanState<'mcx>,
}

impl<'mcx> BatchEmit<'mcx> for BitmapHeapScanBatchEmit<'_, 'mcx> {
    #[inline]
    fn emit(
        &mut self,
        i: u32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        Ok(::nodebitmapheapscan::bitmap_scan_batch_fetch(self.node, estate, i)?
            .then_some(self.node.ss.ss_ScanTupleSlot))
    }
}

// ===========================================================================
// Hash-agg pipeline breaker (Phase-2 vertical slice): the first
// operator→operator composition. Two chained pipelines on one Agg node:
//
//   pipeline N   : SeqScanSource → SeqScanFilterProject → HashAggBuildSink
//   pipeline N+1 : HashAggSource → HashAggEmit → RootAdapter
//
// The breaker node (the Agg) implements Sink for pipeline N (accept = the
// existing per-row transition path via `agg_hash_build_accept`; always
// `NeedMore`) and Source for pipeline N+1 (produce = the existing
// `agg_retrieve_hash_table` read-back — same table, same iteration → same
// output order as C, spill refill included). Chaining is the per-node
// Build→Probe phase flag (`table_filled` — C's own cross-call state), driven
// from the `agg_arm` dispatch hook: the build pipeline drains to completion
// before the first probe tuple, which is C's exact order for free
// (push-executor study, Pattern 3). Spill delegates wholesale to the row-path
// hashagg machinery (§8): `finish()` = spill finish + handoff install; the
// read-back's refill walks PG's spill partitions in PG's order.
// ===========================================================================

/// Memoized structural choice for an Agg-over-SeqScan node, decided at the
/// first call and stable thereafter (a mid-stream flip would desync the
/// build). Dynamic gates (EPQ, direction, the post-build merge handoff) stay
/// per-call in `agg_over_seq_scan_fusible`, evaluated BEFORE the memo.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggLaneChoice {
    /// Admission economics (design §4): no lanefold coverage AND the legacy
    /// fused `exec_agg_batched` arm would engage — the lane must not preempt
    /// the measured-faster fused batch drive (q3/q4-class, integration bench
    /// 2026-07-11). Re-measured with the Phase-3 qual bitmap (2026-07-12):
    /// the lane's per-row breaker feed is STILL slower than the fused arm at
    /// q4's 50% selectivity (+2.5%; only ~-5% at 10% selectivity) — the
    /// dominant cost is the per-row `agg_hash_build_accept` vs the fused
    /// arm's batched drive, which carries the same bitmap. Deliberate
    /// refuse-set entry; shrinks as fold coverage widens.
    Refuse,
    /// Lane owns with the per-row breaker feed: no fold coverage, but no
    /// fused arm to preempt either (shapes the fused arm refuses — scalar
    /// quals, admitted projections).
    PerRow,
    /// Lane owns with the batched build feed: per-batch group probe + the
    /// lanefold whole-batch transition kernels (residual transitions
    /// per-row).
    Fold,
    /// Lane answers the whole AGG_PLAIN node from cbstore part metadata
    /// (footer row counts + zone maps + footer sums) — zero rows staged, end
    /// states finalized by the real finalfns (the metaagg arm; phase4 §7
    /// re-entry, armed 2026-07-14). Structural admission only: the per-call
    /// runtime gates (MVCC snapshot, AM answerability, guard-interval
    /// re-proof) fall back to the per-row drive byte-identically.
    Meta,
}

::mcx::forget_safe_nodrop!(AggLaneChoice);

/// Try to let the lane own an `Agg` over a `SeqScan` child — the fused
/// scan→filter→hash-agg push pipeline. `Some(result)` = the lane drove this
/// call; `None` = refused (the caller falls through to the existing fused /
/// per-tuple agg paths, byte-identically).
#[inline]
pub fn try_own_agg_over_seq_scan<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    choice: &mut Option<AggLaneChoice>,
    stage_slot: &mut Option<ExecSlotId>,
    xk: &mut Option<Box<ExprKeyState>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // AGG_PLAIN (ungrouped) routes to the plain drive: no breaker needed (a
    // single group has no per-group read-back — feed + finalize is the whole
    // node inside one call), but the same staged-batch fold applies, with
    // `lanefold::fold_batch` (the ungrouped kernel, CSE included) in place of
    // the grouped probe+fold. cbstore scans additionally route WITHOUT a
    // classified fold plan (lane-v2-noqualfeed): the plain decider can pick
    // the per-row drain feed there — batch window decode + the full per-row
    // transition program — because the cbstore incumbent is the per-pull
    // Volcano drive, not the fused batched arm the heap refusal defends.
    if ::nodeagg::agg_plain_fold_admissible(agg)
        || (::nodeagg::agg_plain_perrow_admissible(agg)
            && ::nodeseqscan::seq_scan_is_cbstore(ss))
    {
        return try_own_plain_agg_over_seq_scan(agg, ss, choice, estate);
    }
    // AGG_SORTED (the sort-free GroupAggregate shape — clustered/footer-
    // sorted cbstore banks plan `Agg(AGG_SORTED) → SeqScan` with no Sort
    // node): the sorted-agg drive over the scan's staged batches, with
    // fold-admissible transitions run as vectorized per-group-run folds.
    // Section doc at `try_own_sorted_agg_over_seq_scan`. Non-admissible
    // sorted shapes fall through to the hashed gate below, which refuses
    // exactly as before (AggNotDrainable).
    if ::nodeagg::agg_sorted_lane_admissible(agg) {
        return try_own_sorted_agg_over_seq_scan(agg, ss, choice, estate);
    }
    // AGG_PLAIN exact-DISTINCT (count/sum/avg(DISTINCT x) — nodeagg's
    // set-mode admission): NOT batch-drainable (pertrans_sort non-empty), so
    // neither the fold drive above nor the legacy fused arm can host it —
    // the incumbent is the per-tuple pull with a per-group TUPLESORT. The
    // set drive replaces that sort with the exact-distinct hash set
    // (uniqExact analog, cbstore-v2 plan §2.3).
    if ::nodeagg::agg_plain_distinct_set_admissible(agg) {
        return try_own_plain_distinct_agg_over_seq_scan(agg, ss, estate);
    }
    if !agg_over_seq_scan_fusible(agg, ss, estate)? {
        return Ok(None);
    }
    let c = match *choice {
        Some(c) => c,
        None => {
            let c = decide_agg_lane(agg, ss, xk, estate)?;
            *choice = Some(c);
            c
        }
    };
    if c == AggLaneChoice::Refuse {
        return Ok(None);
    }
    // exec_agg's top-of-call guard: a drained agg stays drained (the hash
    // iterator is spent; re-iterating would replay groups).
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    agg_seq_scan_build_if_needed(agg, ss, c, stage_slot, xk, estate)?;
    // Probe phase (every call): the breaker is now the source of pipeline
    // N+1. One qual-passing group per PG pull, in C's retrieve order.
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step(agg, &mut HashAggSource, &mut HashAggEmit, &mut root, estate)?))
}

/// The structural lane choice (see `AggLaneChoice`), decided once at the
/// first (pre-build) call. Fold-readiness = a classified lanefold plan on an
/// unprojected scan, with the SoA deform armed whenever the plan reads lane
/// columns (a plan of pure `count(*)` transitions reads none).
fn decide_agg_lane<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    xk: &mut Option<Box<ExprKeyState>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<AggLaneChoice> {
    // Projected scans: the expression-group-key arm (exprkey module) — the
    // scan computes the (single) grouping key, everything else bare Vars.
    // Refusal (reason ticked there) keeps the per-row/refuse economics below.
    if ss.ss.ps_ProjInfo.is_some() && ::nodeagg::agg_lanefold_plan(agg).is_some() {
        *xk = exprkey::decide_exprkey(agg, ss, estate);
        if xk.is_some() {
            return Ok(AggLaneChoice::Fold);
        }
    }
    let fold_ready = match ::nodeagg::agg_lanefold_plan(agg) {
        Some(plan) if ss.ss.ps_ProjInfo.is_none() => {
            if !plan.vguards.is_empty() {
                // Varlena (str MIN/MAX) lanes: feedable only when the plan
                // reads EXACTLY the one varlena column (the varkey pass
                // stages one column; the fixed-width prefix deform cannot
                // host attlen == -1). Mixed fixed+varlena lane sets refuse —
                // exactly the shapes the prefix probe below already refuses
                // today (the varlena read sits inside the prefix).
                match lanefold_varlane_col(plan) {
                    Some(vcol) => {
                        ::nodeseqscan::seq_scan_batch_soa_prepare_varlane(ss, estate, vcol)
                    }
                    // Multi-varlena (Q23-class): cbstore's virtual-prefix
                    // staging hosts it (lane-v2-dictminmax); heap refuses.
                    None => try_arm_cb_multivar(agg, ss, estate)?,
                }
            } else if plan.cols.is_empty() {
                true
            } else {
                // Probe-arm the deform now so an unarmable prefix (non-fixed-
                // width column) is known BEFORE committing to ownership. A
                // cbstore scan whose prefix refuses only on varlena columns
                // gets the dict-group columnar arm (§2.1) — the text grouping
                // key stages as dict codes, everything else as decoded Datums.
                probe_arm_fold_prefix(agg, ss, estate)?
                    || try_arm_cb_dictgroup(agg, ss, estate)
                    || try_arm_cb_multikey_dict(agg, ss, estate)
            }
        }
        _ => false,
    };
    if fold_ready {
        return Ok(AggLaneChoice::Fold);
    }
    // Admission economics (design §4): without fold coverage the lane's
    // per-row breaker feed is strictly slower than the legacy fused batched
    // drive it would preempt (the agg hook runs first) — measured +5%
    // (q3/q4-class). Never preempt a measured-faster path.
    if crate::procnode::seq_agg_fusible(agg, ss, estate)
        && ::nodeseqscan::seq_scan_batch_supported(ss, estate)?
    {
        // One tick per memoized structural choice (the choice is decided once
        // per node and stable thereafter).
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AdmissionEconomicsFusedDrive);
        // Trace the structural refuse (Q22 serial-dispatch diagnosis,
        // 2026-07-14): the memoized Refuse routes the agg into the legacy
        // FUSED batched drive, which never passes through try_own_seq_scan —
        // so a refused chain shows ZERO lane markers (not even a PREWHERE
        // arm) and reads as "non-attempt" in trace capture. This line makes
        // the attempt+refusal observable.
        lane_trace("agg-over-scan refused (admission economics: legacy fused drive)");
        return Ok(AggLaneChoice::Refuse);
    }
    Ok(AggLaneChoice::PerRow)
}

/// The varlena-lane fold feed's single staged column, when the plan is that
/// shape: every lane read is the one varlena (str MIN/MAX) column. Any other
/// varlena-bearing plan (mixed fixed+varlena lane sets) returns None and the
/// fold refuses — the SoA prefix deform cannot stage an `attlen == -1` column
/// and the varkey pass stages exactly one.
fn lanefold_varlane_col(plan: &::lanefold::LanePlan<'_>) -> Option<u16> {
    match (&plan.vguards[..], &plan.cols[..]) {
        ([v], [c]) if v == c => Some(*v),
        _ => None,
    }
}

/// `LaneCols` remap for the varlena lane feed: the varkey pass stages the
/// single varlena column's per-row datum pointers into SoA column 0, while
/// the plan addresses that column by its scan attno.
struct VarLaneCols<'a, 'mcx> {
    soa: &'a ::exectuples::SoaBatch<'mcx>,
    col: u16,
}

impl ::lanefold::LaneCols for VarLaneCols<'_, '_> {
    fn col_values(&self, c: usize) -> &[::datum::Datum] {
        debug_assert_eq!(c, self.col as usize);
        self.soa.col_values(0)
    }

    fn col_isnull(&self, c: usize) -> &[bool] {
        debug_assert_eq!(c, self.col as usize);
        self.soa.col_isnull(0)
    }
}

/// `LaneCols` wrapper carrying the str MIN/MAX dict-code side channel
/// (lane-v2-dictminmax): delegates the lane reads to `inner` and answers
/// `col_codes` from the per-batch codes list the feed collected through
/// `seq_scan_batch_dict_codes` (which certifies the values-were-gathered
/// half of the contract; the sortedness half is the writer's
/// CHUNK_FLAG_DICT_SORTED, carried in the table). Keys are the PLAN's
/// column indexes (the inner wrapper owns any scan remap).
struct CodesCols<'a, C: ::lanefold::LaneCols> {
    inner: &'a C,
    codes: &'a [(u16, ::exectuples::SoaDictLane)],
}

impl<C: ::lanefold::LaneCols> ::lanefold::LaneCols for CodesCols<'_, C> {
    #[inline(always)]
    fn col_values(&self, c: usize) -> &[::datum::Datum] {
        self.inner.col_values(c)
    }

    #[inline(always)]
    fn col_isnull(&self, c: usize) -> &[bool] {
        self.inner.col_isnull(c)
    }

    #[inline(always)]
    fn col_len_staged(&self, c: usize) -> bool {
        self.inner.col_len_staged(c)
    }

    #[inline(always)]
    fn col_codes(&self, c: usize) -> Option<::exectuples::SoaDictLane> {
        self.codes.iter().find(|(pc, _)| *pc as usize == c).map(|(_, l)| *l)
    }
}

/// The plan's str MIN/MAX (text kinds only — bpchar never rides codes)
/// column list: (plan col, scan col) pairs, deduped. `map` translates plan
/// columns to scan columns (`None` entries never admit str transitions —
/// identity when absent).
fn mm_str_cols(
    plan: &::lanefold::LanePlan<'_>,
    map: impl Fn(u16) -> Option<u16>,
) -> Vec<(u16, u16)> {
    let mut out: Vec<(u16, u16)> = Vec::new();
    for t in plan.trans.iter() {
        if matches!(t.kind, ::lanefold::LaneKind::StrMin | ::lanefold::LaneKind::StrMax)
            && !out.iter().any(|&(pc, _)| pc == t.col)
        {
            if let Some(sc) = map(t.col) {
                out.push((t.col, sc));
            }
        }
    }
    out
}

/// Per-batch dict-code collection for the mm columns: `Some(lane)` per
/// column exactly when the CURRENT staged window certifies the `col_codes`
/// contract (dict window, values gathered — `seq_scan_batch_dict_codes`).
fn collect_mm_codes(
    ss: &::nodeseqscan::SeqScanState<'_>,
    mm_cols: &[(u16, u16)],
    out: &mut Vec<(u16, ::exectuples::SoaDictLane)>,
) {
    out.clear();
    for &(pc, sc) in mm_cols {
        if let Some(lane) = ::nodeseqscan::seq_scan_batch_dict_codes(ss, sc as usize) {
            out.push((pc, lane));
        }
    }
}

/// `LaneCols` for a fold plan that reads no lane columns (pure `count(*)`
/// transitions): the kernels never call these.
struct NoCols;

impl ::lanefold::LaneCols for NoCols {
    fn col_values(&self, _c: usize) -> &[::datum::Datum] {
        unreachable!("count(*)-only fold plans read no lane columns")
    }

    fn col_isnull(&self, _c: usize) -> &[bool] {
        unreachable!("count(*)-only fold plans read no lane columns")
    }
}

/// Build feed for the fold-armed breaker (`AggLaneChoice::Fold`): per staged
/// page batch, run the scan's per-row emit + the per-row group probe (with
/// the residual transitions inside the probe), snapshotting each row's
/// pergroup, then fold the admitted transitions whole-batch with
/// `lanefold::fold_rows_grouped`. One CHECK_FOR_INTERRUPTS per staged batch
/// (design §9 batch-operator cadence). Guarded plans re-prove every batch;
/// `Demote` runs the WHOLE batch through the checked per-row program (never
/// mixing a partial fold with per-row transitions — lanefold contract).
///
/// Byte-identity: the same rows flow through the same qual and the same
/// prepare/lookup/spill per-row machinery in the same order; only the
/// transition arithmetic is batched, and every fold kernel is either
/// commutative or (the str kinds) applied per row in row order, bit-for-bit
/// equal to C's transition semantics (lanefold's tested contract) — str
/// transvalue copies land in the agg context at exactly the per-row path's
/// datumCopy points, so hash-agg memory accounting and spill decisions are
/// unchanged too. Transvalues — and therefore output bytes — are identical.
fn agg_hash_build_fold_feed<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    stage_slot: &mut Option<ExecSlotId>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // K2 admission for the scan feed, decided once per build (mirrors the
    // joined-row feed's `staged_feed_shape` mode choice): unguarded, fully
    // admitted (no residual transitions), single kernel-hostable grouping
    // key, with the key and every spill-replay column staged in the armed
    // SoA lanes. `None` = the per-row arrival probe (byte-identical).
    let k2 = scan_k2_shape(agg, ss, estate);
    // Stage-2.2 compact-table arming, per build, on top of the K2 shape
    // (nodeagg::compact module doc: int-width key kernel, AGGSPLIT_SIMPLE,
    // not spill-eligible by estimate; runtime backstop migrates to the C
    // table). Non-armed verdicts tick their observability reasons; the
    // build itself stays lane-owned either way.
    let compact = k2.is_some()
        && match ::nodeagg::agg_hash_compact_try_arm(agg) {
            ::nodeagg::CompactArm::Armed => true,
            ::nodeagg::CompactArm::KeyKind => {
                stats::tick_refused(ShapeClass::AggBuild, RefuseReason::CompactKeyKind);
                false
            }
            ::nodeagg::CompactArm::SpillRisk => {
                stats::tick_refused(ShapeClass::AggBuild, RefuseReason::CompactSpillRisk);
                false
            }
            ::nodeagg::CompactArm::Off => false,
        };
    // Stage-2.1 dict-group registration (per build): the K2 key column was
    // opted into dict lanes by `try_arm_cb_dictgroup`. Dict-answered windows
    // take the per-epoch code-grouping path inside `scan_k2_batch`; Raw
    // windows keep the Raw keys path — both through the same global table.
    let dictgroup = k2
        .as_ref()
        .map_or(false, |s| ::nodeseqscan::seq_scan_batch_dictgroup_col(ss) == Some(s.key_col));
    let mut dgs = DictGroupScratch::default();
    // Packed multi-key admission + compact arm (multikey spike): only for
    // shapes the single-key K2 machinery does not own.
    let mk = if k2.is_none() { scan_mk_shape(agg, ss, estate) } else { None };
    let mut mks = MkScratch::default();
    trace_feed(if mk.is_some() {
        "agg-over-seqscan: staged fold feed engaged (multi-key packed)"
    } else if dictgroup {
        "agg-over-seqscan: staged fold feed engaged (dict-group armed)"
    } else if compact {
        "agg-over-seqscan: staged fold feed engaged (compact table)"
    } else if k2.is_some() {
        "agg-over-seqscan: staged fold feed engaged (k2 probe)"
    } else {
        "agg-over-seqscan: staged fold feed engaged"
    });
    let mut idxs: Vec<u32> = Vec::new();
    let mut groups: Vec<core::ptr::NonNull<::execexpr::AggPerGroup>> = Vec::new();
    // Varlena-lane plans read their one column through the varkey staging at
    // SoA column 0 (see lanefold_varlane_col / VarLaneCols). Multi-varlena
    // plans (lane-v2-dictminmax, Q23-class) admitted only over the cbstore
    // virtual-prefix staging, which stages every column at its NATURAL index
    // — no remap (vcol None).
    let vcol = {
        let plan = ::nodeagg::agg_lanefold_plan(agg).expect("fold feed without a plan");
        debug_assert!(
            plan.vguards.is_empty()
                || lanefold_varlane_col(plan).is_some()
                || ::nodeseqscan::seq_scan_batch_soa(ss).is_some(),
            "multi-varlena fold without the cbstore staging armed"
        );
        lanefold_varlane_col(plan)
    };
    // Dual arm (q22coexist): when the PREWHERE lane owns the staging, the
    // varlena fold column sits at its NATURAL prefix index (the lane's
    // completing deform fills it for survivor windows) — no varkey remap.
    // The lane fills lazily, so the guard proof below must restrict itself
    // to the selection bitmap (unselected cells may be stale pointers).
    let lane_owned = ::nodeseqscan::seq_scan_batch_lane_armed(ss);
    let vremap = if lane_owned { None } else { vcol };
    // Str MIN/MAX dict-code memo (lane-v2-dictminmax): plan columns == scan
    // columns on this feed (identity map). Codes collect per batch; the
    // scratch invalidates whenever any row advanced str transitions through
    // the per-row program (demote / fallback / arrival-probe routes).
    let mm_cols = {
        let plan = ::nodeagg::agg_lanefold_plan(agg).expect("fold feed without a plan");
        mm_str_cols(plan, Some)
    };
    if !mm_cols.is_empty() && ::nodeseqscan::seq_scan_is_cbstore(ss) {
        trace_feed("fold str min/max dict-code memo armed");
    }
    let mut mm_scratch = ::lanefold::StrMmScratch::default();
    let mut mm_codes: Vec<(u16, ::exectuples::SoaDictLane)> = Vec::new();
    let mut k2s = ScanK2Scratch::default();
    loop {
        let n = ::nodeseqscan::seq_scan_next_pagebatch(ss, estate)?;
        if n == 0 {
            // End of scan: drop the scan slot's buffer pin (SeqScanSource
            // end-of-stream parity).
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(ss.ss.ss_ScanTupleSlot), mcx);
            break;
        }
        ::postgres_seams::check_for_interrupts::call()?;
        // Guarded plans (int2-Var OpExpr admissions): prove the batch before
        // any fold. The proof runs over every staged non-fallback row — a
        // superset of the rows the fold will touch — so a Pass is sound and a
        // Demote at worst conservative (the checked per-row program is always
        // correct; it raises C's error at C's row when a selected row really
        // overflows).
        let mut demote = false;
        {
            let plan = ::nodeagg::agg_lanefold_plan(agg).expect("fold feed without a plan");
            if plan.guarded {
                let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                    .expect("guarded fold plans read lane columns");
                let nwords = (n as usize).div_ceil(64);
                let mut rows = [0u64; ::exectuples::SOA_BM_WORDS];
                // Proof domain: every staged non-fallback row — a superset of
                // the rows the fold will touch, so a Pass is sound and a
                // Demote at worst conservative. Under the PREWHERE lane the
                // staged columns fill lazily (survivor windows only), so the
                // domain must intersect the selection bitmap: unselected
                // cells may be stale pointers, and the fold touches only
                // selected rows anyway (requal survivors ⊆ selected bits).
                match ::nodeseqscan::seq_scan_batch_lane_sel(ss) {
                    Some(sel) if lane_owned => {
                        for ((r, fb), s) in
                            rows[..nwords].iter_mut().zip(soa.fallback_words()).zip(sel)
                        {
                            *r = s & !fb;
                        }
                    }
                    _ => {
                        for (r, fb) in rows[..nwords].iter_mut().zip(soa.fallback_words()) {
                            *r = !fb;
                        }
                    }
                }
                if n % 64 != 0 {
                    rows[nwords - 1] &= (1u64 << (n % 64)) - 1;
                }
                // Empty domain: nothing to prove and nothing will fold —
                // never probe lane cells (a survivor-less lane window ran no
                // completing deform; every cell is stale).
                if rows[..nwords].iter().any(|&w| w != 0) {
                    // SAFETY: proof rows are staged non-fallback rows — under
                    // a varkey/prefix staging every staged row's lane values
                    // are live page datum pointers (staging contract); under
                    // the PREWHERE lane the domain is selected rows of a
                    // survivor window, whose completing deform filled every
                    // prefix column with decoded datums — vguard columns
                    // readable at their varlena header byte either way.
                    demote = unsafe {
                        match vremap {
                            Some(c) => ::lanefold::check_guards(
                                plan,
                                &VarLaneCols { soa, col: c },
                                &rows[..nwords],
                                |_| None,
                            ),
                            None => {
                                ::lanefold::check_guards(plan, soa, &rows[..nwords], |_| None)
                            }
                        }
                    } == ::lanefold::GuardCheck::Demote;
                }
            }
        }
        if demote {
            // The per-row program advances the admitted str transitions
            // behind the memo's back — drop every memo (StrMmScratch doc).
            mm_scratch.invalidate();
            for i in 0..n {
                if let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(ss, estate, i)? {
                    ::nodeagg::agg_hash_build_accept(agg, estate, slot)?;
                }
            }
            continue;
        }
        // K2 deferred batched probe, per batch: only when EVERY staged row
        // carries lane values — a fallback row has no staged key, and probing
        // it at arrival while deferring its neighbors would reorder
        // first-arrival insertions. Batches with any fallback row keep the
        // arrival probe wholesale (both modes probe in row order, so a
        // per-batch mode choice preserves the global insertion sequence).
        if let Some(shape) = &k2 {
            let all_lane = ::nodeseqscan::seq_scan_batch_soa(ss)
                .is_some_and(|soa| soa.fallback_words().iter().all(|&w| w == 0));
            if all_lane {
                scan_k2_batch(
                    agg,
                    ss,
                    shape,
                    stage_slot,
                    &mut k2s,
                    dictgroup.then_some(&mut dgs),
                    &mut idxs,
                    &mut groups,
                    n,
                    estate,
                )?;
                // The K2 fold ran without the memo (str advances bypass it)
                // — keep the memo coherent for any later arrival batch.
                mm_scratch.invalidate();
                continue;
            }
            // A fallback-bearing batch routes through the arrival probe (the
            // C table): the compact table must hand its groups over FIRST so
            // every group lives in exactly one table (states carried over
            // byte-for-byte; no-op when not armed).
            ::nodeagg::agg_hash_compact_disarm(agg, estate)?;
        }
        // Packed multi-key batch (multikey spike): only while the compact
        // table stays armed — after a backstop migration (scan_mk_batch =
        // false) or a fallback-bearing batch, this and every later batch
        // route through the per-row arrival probe below (the C table now
        // holds every group; there is no multi-key staged C probe).
        if let Some(shape) = &mk {
            if ::nodeagg::agg_hash_compact_armed(agg) {
                let all_lane = ::nodeseqscan::seq_scan_batch_soa(ss)
                    .is_some_and(|soa| soa.fallback_words().iter().all(|&w| w == 0));
                if all_lane {
                    if scan_mk_batch(
                        agg, ss, shape, &mut mks, &mut idxs, &mut groups, n, estate,
                    )? {
                        // As the K2 arm: the mk fold bypassed the memo.
                        mm_scratch.invalidate();
                        continue;
                    }
                } else {
                    ::nodeagg::agg_hash_compact_disarm(agg, estate)?;
                }
            }
        }
        idxs.clear();
        groups.clear();
        // Phase-3 qual kernel: with the selection bitmap staged for this
        // batch, walk ONLY the survivors (bitmap hits + forced fallback bits,
        // re-checked per-row inside the emit) — same rows, same ascending
        // order as the full walk, whose emit would have bit-tested each row
        // anyway. Non-kernel quals keep the full per-row walk.
        if ::nodeseqscan::seq_scan_batch_qual_bitmap_ready(ss) {
            while let Some(i) = ::nodeseqscan::seq_scan_batch_next_selected(ss) {
                agg_fold_feed_row(agg, ss, estate, &mut idxs, &mut groups, i)?;
            }
        } else {
            for i in 0..n {
                agg_fold_feed_row(agg, ss, estate, &mut idxs, &mut groups, i)?;
            }
        }
        // Fallback rows advanced str transitions through the full per-row
        // accept above — drop every memo before this batch's fold.
        if !mm_cols.is_empty()
            && ::nodeseqscan::seq_scan_batch_soa(ss)
                .is_some_and(|soa| soa.fallback_words().iter().any(|&w| w != 0))
        {
            mm_scratch.invalidate();
        }
        // SAFETY: non-fallback rows carry valid deformed lane values for
        // every plan column (the SoA prefix covers the evaltrans fetch
        // bound; varlena lanes are page datum pointers from the varkey
        // staging, pinned for the staged batch); guarded plans passed
        // `check_guards` above; dict-code views satisfy the col_codes
        // contract (`seq_scan_batch_dict_codes`); the rest is
        // `agg_fold_staged`'s per-feed contract.
        collect_mm_codes(ss, &mm_cols, &mut mm_codes);
        match (::nodeseqscan::seq_scan_batch_soa(ss), vremap) {
            (Some(soa), Some(cix)) => unsafe {
                agg_fold_staged_mm(
                    agg,
                    &CodesCols { inner: &VarLaneCols { soa, col: cix }, codes: &mm_codes },
                    &idxs,
                    &groups,
                    Some(&mut mm_scratch),
                )?
            },
            (Some(soa), None) => unsafe {
                agg_fold_staged_mm(
                    agg,
                    &CodesCols { inner: soa, codes: &mm_codes },
                    &idxs,
                    &groups,
                    Some(&mut mm_scratch),
                )?
            },
            (None, _) => {
                debug_assert!(
                    ::nodeagg::agg_lanefold_plan(agg).is_some_and(|p| p.cols.is_empty())
                );
                unsafe { agg_fold_staged(agg, &NoCols, &idxs, &groups)? }
            }
        }
    }
    // Combine-before-finish (delegated; the Stage-4 seam): spill finish +
    // merge handoff, then the phase flip.
    ::nodeagg::agg_hash_build_combine(agg, estate)?;
    ::nodeagg::agg_hash_build_finish(agg, estate)
}

/// One staged row of the fold build feed: the per-row emit (per-tuple ctx
/// reset, store, qual), then route — SoA fallback rows to the full per-row
/// transition program (they carry no lane values; the order split across
/// transitions is bit-invisible — commutative kernels), everything else
/// through the group probe with its pergroup snapshotted for the whole-batch
/// fold.
fn agg_fold_feed_row<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    idxs: &mut Vec<u32>,
    groups: &mut Vec<core::ptr::NonNull<::execexpr::AggPerGroup>>,
    i: u32,
) -> PgResult<()> {
    let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(ss, estate, i)? else {
        return Ok(());
    };
    if ::nodeseqscan::seq_scan_batch_soa(ss).is_some_and(|soa| soa.is_fallback(i)) {
        ::nodeagg::agg_hash_build_accept(agg, estate, slot)?;
    } else if let Some(pg) = ::nodeagg::agg_hash_build_probe_resid(agg, estate, slot)? {
        idxs.push(i);
        groups.push(pg);
    }
    Ok(())
}

// ===========================================================================
// Plain-agg (AGG_PLAIN, ungrouped) fold drive — the q2-class
// `SELECT sum(a), avg(b), count(*) FROM t [WHERE ...]` shapes. SIMPLER than
// the hashed breaker: one group, no probe — each staged batch folds straight
// into the single pergroup array via `lanefold::fold_batch` (the ungrouped
// kernel, CSE schedule included), and the retrieve side is the delegated
// `plain_finish` (finalize + HAVING + project, one row, zero-row contract
// included). The whole node runs inside one `exec_proc_node` call, exactly
// like `exec_agg`'s single-group arm.
// ===========================================================================

/// Try to let the lane own an AGG_PLAIN `Agg` over a `SeqScan` child with the
/// batched fold. `Some(result)` = the lane drove this call; `None` = refused
/// (the caller falls through to the fused `exec_agg_batched` / per-tuple
/// paths, byte-identically).
fn try_own_plain_agg_over_seq_scan<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    choice: &mut Option<AggLaneChoice>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Scan-side refuse-set: the Phase-1 gate verbatim (dynamic EPQ/direction
    // gates re-checked per call; structural verdict memoized on the node).
    if !seq_scan_fusible(ss, estate)? {
        return Ok(None);
    }
    let c = match *choice {
        Some(c) => c,
        None => {
            let c = decide_plain_agg_lane(agg, ss, estate)?;
            *choice = Some(c);
            c
        }
    };
    // Metadata-answer arm: the whole node from cbstore footers, zero rows
    // staged. Runtime gates (MVCC snapshot / AM answerability / guard
    // re-proof) are re-checked per call; a runtime refusal falls back to the
    // per-row Volcano drive byte-identically (it may raise C's overflow
    // error at C's row — exactly what the guard re-proof protects).
    if c == AggLaneChoice::Meta {
        // exec_agg's top-of-call guard (exec_agg_meta re-checks it too).
        if ::nodeagg::agg_is_done(agg) {
            return Ok(Some(None));
        }
        return try_meta_agg_answer(agg, ss, estate);
    }
    if c == AggLaneChoice::Refuse {
        return Ok(None);
    }
    // exec_agg's top-of-call guard: the one result row is out; a drained agg
    // stays drained until rescan clears `agg_done`.
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    // One OWNED tick per lane-owned plain-agg build event (the gate's
    // aggbuild floor counts builds, not calls; a plain node builds once per
    // (re)scan — this drive runs the whole feed inside one call).
    stats::tick_owned(ShapeClass::AggBuild);
    if c == AggLaneChoice::Fold {
        agg_plain_fold_feed(agg, ss, estate)?;
    } else {
        agg_plain_perrow_feed(agg, ss, estate)?;
    }
    // Retrieve (delegated): finalize + HAVING + project — one row (or none,
    // when the var-free HAVING rejects it), setting `agg_done`.
    Ok(Some(::nodeagg::agg_plain_finish(agg, estate)?))
}

/// Build feed for the plain PER-ROW drive (`AggLaneChoice::PerRow` — cbstore
/// scans only, lane-v2-noqualfeed): drain the Phase-1 scan pipeline (batch
/// window decode; the PREWHERE/kernel-bitmap arms engage when the qual has a
/// kernel shape) into the FULL per-row transition program. This replaces the
/// per-pull Volcano chain (`exec_agg` → `exec_proc_node` → `getnextslot`)
/// with one drained loop over staged windows; no fold plan is required, so
/// arbitrary transition expressions (the Q30-class SUM(x op k) batteries)
/// are hosted.
///
/// Byte-identity: the same rows flow through the same qual (staged bitmap =
/// the kernel qual's verdict; other quals run scalar per row inside the
/// emit) and the same per-row transition program (`agg_plain_build_accept` =
/// `exec_agg`'s single-group loop body) in the same row order — only the
/// pull chain is elided. The transvalues, and therefore the one finalized
/// output row, are identical.
fn agg_plain_perrow_feed<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert!(::nodeseqscan::seq_scan_is_cbstore(ss));
    // Row-emit staging (drain pipeline): PREWHERE v1 / kernel bitmap when a
    // qual kernel exists; a no-qual scan stages bare batch-decoded windows.
    arm_scan_staging(
        ss,
        estate,
        ScanFeedShape::RowFeed { ctx: "plain agg per-row feed", stitch: true },
    )?;
    // initialize_aggregates (delegated): fresh initval pergroups; a rescan
    // re-enters here with agg_done cleared.
    ::nodeagg::agg_plain_build_begin(agg, estate)?;
    let mut sink = PlainAggBuildSink { agg };
    drain_pipeline(ss, &mut SeqScanSource, &mut SeqScanFilterProject, &mut sink, estate)
}

/// The plain agg as breaker Sink: accept = the full per-row transition
/// program (`exec_agg`'s single-group loop body, delegated); finish = no-op
/// (finalize/HAVING/project is the caller's `agg_plain_finish`, exactly as
/// the fold drive sequences it). Always `NeedMore` — a breaker consumes its
/// whole input.
struct PlainAggBuildSink<'a, 'mcx> {
    agg: &'a mut ::nodeagg::AggStateData<'mcx>,
}

impl<'mcx> Sink<'mcx> for PlainAggBuildSink<'_, 'mcx> {
    fn accept(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<SinkFeed> {
        ::nodeagg::agg_plain_build_accept(self.agg, estate, tuple)?;
        Ok(SinkFeed::NeedMore)
    }

    fn finish(&mut self, _estate: &mut EStateData<'mcx>) -> PgResult<()> {
        Ok(())
    }
}

/// Batch-granular feed: the default loop, monomorphized (same rows, same
/// order; the per-row dyn dispatch elided) — mirrors `HashAggBuildSink`.
impl<'mcx> BatchSink<'mcx> for PlainAggBuildSink<'_, 'mcx> {}

/// The structural lane choice for an AGG_PLAIN Agg over a SeqScan, decided
/// once at the first call.
///
/// Heap scans: Fold or Refuse only — the lane never takes heap plain shapes
/// per-row: the incumbent legacy fused `exec_agg_batched` drive is already
/// batched with per-row transitions, so a per-row lane feed has nothing to
/// win (admission economics, design §4).
///
/// cbstore scans (lane-v2-noqualfeed, phase4 §7 re-entry): the incumbent
/// fused drive is gated OFF (`table_scan_supports_pagebatch` false — lane-OFF
/// stays the per-row Volcano oracle), so the heap Refuse arms take the
/// PER-ROW drain feed instead: batch window decode + the full per-row
/// transition program beats the per-pull Volcano chain regardless of quals
/// (the shape the old kernel-armed gate mis-scoped — its 1.21-1.33x evidence
/// measured the standalone capacity-one RowFeed adapter, not a drained
/// breaker feed). The one cbstore Refuse left is the count(*)-only census
/// shape: transitions reading NO input columns decode nothing on the per-row
/// drive (empty needed set) and are the MetaAggScan footer path's target —
/// a batch-decoded feed has nothing to win there (distinct reason so the
/// gate can watch it).
fn decide_plain_agg_lane<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<AggLaneChoice> {
    let is_cb = ::nodeseqscan::seq_scan_is_cbstore(ss);
    // Heap Refuse = admission economics (§4): the legacy fused
    // `exec_agg_batched` drive (or the per-tuple path) already owns the shape
    // at least as well as a lane feed could. One tick per memoized per-node
    // choice.
    let refuse = || {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AdmissionEconomicsFusedDrive);
        Ok(AggLaneChoice::Refuse)
    };
    // Metadata-answer arm first (phase4 §7 re-entry, armed 2026-07-14): a
    // bare cbstore scan under an all-footer-answerable transition set
    // answers from part metadata — strictly cheaper than any fold feed, so
    // it preempts the fold decision below wherever it admits.
    if meta_agg_admissible(agg, ss, estate)? {
        return Ok(AggLaneChoice::Meta);
    }
    // count(*)-only census shapes (the transition program reads no input
    // columns): heap's incumbent fused drive advances those per batch with
    // zero per-row work (the storeless advance / `qualifying_count` bitmap
    // census); cbstore's per-row drive decodes nothing (empty needed set)
    // and the footer answer (MetaAggScan) is the real lever — a bare-cbstore
    // count(*) is answered by the Meta arm above; one reaching here has a
    // qual/projection/uncovered transition. Deliberate refuse-set entries,
    // one tick per memoized choice.
    if ::nodeagg::agg_batch_outer_prefix(agg) == Some(0) {
        if is_cb {
            stats::tick_refused(ShapeClass::AggBuild, RefuseReason::CountOnlyCensus);
            return Ok(AggLaneChoice::Refuse);
        }
        return refuse();
    }
    // Fold-readiness: a classified fold plan reading lane columns on an
    // unprojected scan (projected scans read output columns, which are not
    // commensurable with scan-column prefixes — the hashed breaker's
    // scoping, verbatim), with the forced prefix deform probe-armed NOW so
    // an unarmable prefix (non-fixed-width column) is known BEFORE
    // committing.
    let fold_ready = match ::nodeagg::agg_lanefold_plan(agg) {
        Some(plan) if ss.ss.ps_ProjInfo.is_none() && !plan.cols.is_empty() => {
            probe_arm_fold_prefix(agg, ss, estate)?
        }
        _ => false,
    };
    if fold_ready {
        return Ok(AggLaneChoice::Fold);
    }
    if is_cb {
        return Ok(AggLaneChoice::PerRow);
    }
    refuse()
}

/// Metadata-answer arm kill switch: default ON when the lane is on;
/// `PGRUST_LANE_V2_METAAGG=0`/`off` disarms (A/B tooling — both sides are
/// value-identical by exec_agg_meta's end-state contract, so the switch is
/// byte-identity-safe like `PGRUST_LANE_V2_K2`).
fn metaagg_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_LANE_V2_METAAGG").as_deref(), Ok("0") | Ok("off"))
    })
}

/// Structural admission for the metadata-answer arm, evaluated once per
/// memoized per-node choice: a BARE cbstore scan (variant Plain — no qual,
/// no projection — and no zone quals; v1 requires literally no qual) under
/// an AGG_PLAIN node whose EVERY transition is footer-answerable
/// (`classify_meta`: count(*)/count(col)/min/max over bare int-family Vars,
/// sum/avg over affine divk==1 int transforms; FILTER/DISTINCT/ORDER BY and
/// the float/bool/bitwise/text tiers refuse). Ticks the metaagg class only
/// for cbstore-backed scans — heap plain aggs are out of the arm's scope
/// (heap has no part metadata) and fall through silently.
fn meta_agg_admissible<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if !::nodeseqscan::seq_scan_is_cbstore(ss) {
        return Ok(false);
    }
    if !metaagg_enabled() {
        stats::tick_refused(ShapeClass::MetaAgg, RefuseReason::EnvOff);
        return Ok(false);
    }
    if ::nodeagg::agg_meta_plan(agg).is_none()
        || !::nodeseqscan::seq_scan_meta_agg_ok(ss, estate)?
    {
        stats::tick_refused(ShapeClass::MetaAgg, RefuseReason::MetaShape);
        return Ok(false);
    }
    Ok(true)
}

/// Per-call runtime half of the metadata-answer arm. `Ok(Some(_))` = the
/// node was answered from footers (one finalized row or a drained None);
/// `Ok(None)` = runtime refusal — the caller falls through to the per-row
/// Volcano drive byte-identically.
fn try_meta_agg_answer<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // RG xmin visibility folds against the scan snapshot: MVCC only (the
    // same gate the fused metacount arm carried on the old branch).
    if !estate
        .es_snapshot
        .as_deref()
        .is_some_and(::types_snapshot::IsMVCCSnapshot)
    {
        stats::tick_refused(ShapeClass::MetaAgg, RefuseReason::NonMvccSnapshot);
        return Ok(None);
    }
    let metas = ::nodeagg::agg_meta_plan(agg).expect("Meta choice requires a meta plan");
    // Guarded sum cols join the minmax request: the guard re-proof below
    // needs the visible rows' exact (min, max).
    let cols: Vec<u16> = metas
        .iter()
        .filter(|t| {
            matches!(t.kind, ::lanefold::MetaKind::Min | ::lanefold::MetaKind::Max)
                || t.guard.is_some()
        })
        .map(|t| t.col)
        .collect();
    let mut sum_cols: Vec<u16> =
        metas.iter().filter(|t| t.kind.needs_sum()).map(|t| t.col).collect();
    sum_cols.sort_unstable();
    sum_cols.dedup();
    let Some(res) = ::nodeseqscan::seq_scan_meta_agg(ss, estate, &cols, &sum_cols)? else {
        // AM declined: parallel scan desc or an uncovered column type.
        stats::tick_refused(ShapeClass::MetaAgg, RefuseReason::MetaRuntime);
        return Ok(None);
    };
    // Data-level guard re-proof against the visible rows' footer min/max: a
    // failed interval falls through to the ordinary drives, whose per-row
    // program raises C's int4 overflow error at C's row. rows == 0 passes
    // vacuously (empty minmax stays (MAX, MIN)).
    let guards_ok = res.rows == 0
        || metas.iter().all(|t| match t.guard {
            None => true,
            Some((lo, hi)) => res
                .minmax
                .iter()
                .find(|e| e.0 == t.col)
                .is_some_and(|&(_, mn, mx)| lo <= mn && mx <= hi),
        });
    if !guards_ok {
        stats::tick_refused(ShapeClass::MetaAgg, RefuseReason::MetaRuntime);
        return Ok(None);
    }
    // One OWNED tick per metadata-answered execution event.
    stats::tick_owned(ShapeClass::MetaAgg);
    lane_trace(&format!("metaagg: footer answer, rows={}", res.rows));
    Ok(Some(::nodeagg::exec_agg_meta(agg, estate, res.rows, &res.minmax, &res.sums)?))
}

/// Feed for the plain fold drive: per staged page batch, compose the row
/// selection and fold the admitted transitions whole-batch with
/// `lanefold::fold_batch` into the single pergroup array. One
/// CHECK_FOR_INTERRUPTS per staged batch (design §9 batch-operator cadence).
/// Guarded plans re-prove every batch; `Demote` runs the WHOLE batch through
/// the checked per-row program (lanefold contract).
///
/// Two per-batch modes:
///   * bitmap: no residual transitions and the qual is absent or staged as
///     the kernel-qual bitmap — the selection is `sel & !fallback` (or
///     `!fallback` with no qual) with NO per-row work for deformed rows (the
///     fold reads the SoA lanes; a per-row emit would only store a slot
///     nothing reads). Forced fallback rows re-check the qual per-row and run
///     the full per-row program off the stored tuple.
///   * per-row emit: a scalar qual and/or residual transitions — the scan's
///     per-row emit applies the qual; surviving deformed rows join the fold
///     selection (+ the residual program per row), fallback rows run the
///     full per-row program.
///
/// Byte-identity: the same rows pass the same qual (the staged bitmap IS the
/// kernel qual's verdict; fallback rows re-run the per-row check), and every
/// fold kernel is commutative and bit-for-bit equal to C's transition
/// semantics on admitted/guard-proven data (lanefold's tested contract), so
/// the single group's transvalues — and the finalized output row — are
/// identical.
fn agg_plain_fold_feed<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Same one-deform staging as the hashed fold feed; the deform is FORCED
    // (count(*)-only plans were refused, so the fold always reads lane
    // columns). Re-preparing with the same shape is a no-op.
    arm_scan_staging(ss, estate, ScanFeedShape::FoldPrefix { agg })?;
    // Fold length lanes (no grouping key, no spill, no staged replay on the
    // plain feed — the staged lanes' only reader is the fold itself).
    arm_fold_len_lanes(agg, ss);
    // initialize_aggregates (delegated): fresh initval pergroups; a rescan
    // re-enters here with agg_done cleared.
    ::nodeagg::agg_plain_build_begin(agg, estate)?;
    let has_resid =
        ::nodeagg::agg_lanefold_plan(agg).is_some_and(|plan| !plan.resid.is_empty());
    // Str MIN/MAX dict-code side channel (lane-v2-dictminmax; identity plan→
    // scan column map on this feed).
    let mm_cols = {
        let plan = ::nodeagg::agg_lanefold_plan(agg).expect("fold feed without a plan");
        mm_str_cols(plan, Some)
    };
    if !mm_cols.is_empty() && ::nodeseqscan::seq_scan_is_cbstore(ss) {
        trace_feed("fold str min/max dict-code memo armed");
    }
    let mut mm_codes: Vec<(u16, ::exectuples::SoaDictLane)> = Vec::new();
    loop {
        let n = ::nodeseqscan::seq_scan_next_pagebatch(ss, estate)?;
        if n == 0 {
            // End of scan: drop the scan slot's buffer pin (SeqScanSource
            // end-of-stream parity).
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(ss.ss.ss_ScanTupleSlot), mcx);
            break;
        }
        ::postgres_seams::check_for_interrupts::call()?;
        let nwords = (n as usize).div_ceil(64);
        // Guarded plans: prove the batch over every staged non-fallback row —
        // a superset of the rows the fold will touch — before any fold (same
        // soundness argument as the hashed fold feed).
        let mut demote = false;
        {
            let plan = ::nodeagg::agg_lanefold_plan(agg).expect("fold feed without a plan");
            if plan.guarded {
                let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                    .expect("plain fold plans read lane columns");
                let mut rows = [0u64; ::exectuples::SOA_BM_WORDS];
                // Proof domain: under the PREWHERE lane the staged columns
                // fill lazily (survivor windows only), so intersect the
                // selection bitmap — the fold touches only selected rows
                // (requal survivors ⊆ selected bits); unselected cells may be
                // stale pointers (vguard columns via the virtual prefix).
                match ::nodeseqscan::seq_scan_batch_lane_sel(ss) {
                    Some(sel) => {
                        for ((r, fb), s) in
                            rows[..nwords].iter_mut().zip(soa.fallback_words()).zip(sel)
                        {
                            *r = s & !fb;
                        }
                    }
                    None => {
                        for (r, fb) in rows[..nwords].iter_mut().zip(soa.fallback_words()) {
                            *r = !fb;
                        }
                    }
                }
                if n % 64 != 0 {
                    rows[nwords - 1] &= (1u64 << (n % 64)) - 1;
                }
                // Empty domain: nothing will fold — never probe lane cells
                // (a survivor-less lane window ran no completing deform).
                if rows[..nwords].iter().any(|&w| w != 0) {
                    // SAFETY: proof rows are staged non-fallback rows with
                    // live deformed lane values (prefix deform contract;
                    // under the PREWHERE lane the domain is selected rows of
                    // a survivor window, whose completing deform filled every
                    // prefix column — vguard columns readable at their
                    // varlena header byte).
                    demote = unsafe {
                        ::lanefold::check_guards(plan, soa, &rows[..nwords], |_| None)
                            == ::lanefold::GuardCheck::Demote
                    };
                }
            }
        }
        if demote {
            for i in 0..n {
                if let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(ss, estate, i)? {
                    ::nodeagg::agg_plain_build_accept(agg, estate, slot)?;
                }
            }
            continue;
        }
        let mut rows = [0u64; ::exectuples::SOA_BM_WORDS];
        let bitmap_qual = ::nodeseqscan::seq_scan_batch_qual_sel(ss).is_some();
        if !has_resid && (bitmap_qual || ss.ss.qual.is_none()) {
            let mut fallback = [0u64; ::exectuples::SOA_BM_WORDS];
            {
                let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                    .expect("plain fold plans read lane columns");
                let fb = soa.fallback_words();
                let sel = ::nodeseqscan::seq_scan_batch_qual_sel(ss);
                for w in 0..nwords {
                    rows[w] = sel.map_or(!fb[w], |s| s[w] & !fb[w]);
                    fallback[w] = fb[w];
                }
                if n % 64 != 0 {
                    rows[nwords - 1] &= (1u64 << (n % 64)) - 1;
                    fallback[nwords - 1] &= (1u64 << (n % 64)) - 1;
                }
            }
            for (w, mut bits) in fallback[..nwords].iter().copied().enumerate() {
                while bits != 0 {
                    let i = (w as u32) * 64 + bits.trailing_zeros();
                    bits &= bits - 1;
                    if let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(ss, estate, i)? {
                        ::nodeagg::agg_plain_build_accept(agg, estate, slot)?;
                    }
                }
            }
        } else {
            for i in 0..n {
                let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(ss, estate, i)? else {
                    continue;
                };
                if ::nodeseqscan::seq_scan_batch_soa(ss).is_some_and(|soa| soa.is_fallback(i))
                {
                    ::nodeagg::agg_plain_build_accept(agg, estate, slot)?;
                } else {
                    rows[(i / 64) as usize] |= 1u64 << (i % 64);
                    if has_resid {
                        ::nodeagg::agg_plain_build_accept_resid(agg, estate, slot)?;
                    }
                }
            }
        }
        if rows[..nwords].iter().any(|w| *w != 0) {
            let plan = ::nodeagg::agg_lanefold_plan(agg).expect("fold feed without a plan");
            let aggcx = ::nodeagg::agg_aggcontext(agg);
            // Str MIN/MAX dict-code views for this batch (lane-v2-
            // dictminmax): the ungrouped fold's batch winner becomes an
            // integer code scan — no scratch (fold_batch needs no memo).
            collect_mm_codes(ss, &mm_cols, &mut mm_codes);
            let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                .expect("plain fold plans read lane columns");
            // SAFETY: pergroup_base is the node's once-allocated single-group
            // pergroup array covering every transno (initialize_aggregates
            // just wrote it); selected rows are non-fallback, carrying valid
            // deformed lane values for every plan column (the SoA prefix
            // covers the evaltrans fetch bound); AvgAccum pergroups hold the
            // catalog's {0,0} int8[2] transarray, datum-copied at
            // initialize_aggregates; Int128AvgAccum pergroups are NULL or
            // hold the aggcontext state the fold/transfn chain installed, and
            // `aggcx` is that same aggcontext; guarded plans passed
            // `check_guards` above; dict-code views satisfy the col_codes
            // contract (`seq_scan_batch_dict_codes`).
            unsafe {
                ::lanefold::fold_batch(
                    plan,
                    &CodesCols { inner: soa, codes: &mm_codes },
                    &rows[..nwords],
                    n as usize,
                    ::nodeagg::agg_plain_pergroup_base(agg),
                    aggcx,
                )?;
            }
        }
    }
    Ok(())
}

// ===========================================================================
// Plain-agg exact-DISTINCT drive (the uniqExact analog — cbstore-v2 plan
// §2.3; nodeagg's distinctset module). Hosts AGG_PLAIN nodes whose every
// DISTINCT aggregate is a set-mode entry (count/sum/avg(DISTINCT x) over
// int2/4/8 or deterministic-collation text — `distinct_set_kind`'s matrix):
// the per-row feed runs the SAME evaltrans park + ordered-input collect the
// per-tuple pull runs (the collect inserts into the per-group set instead of
// a tuplesort), and the delegated finalize replays each distinct value once
// through the real transfn.
//
// Value identity (order-relaxation charter): the set changes only the
// transfn REPLAY ORDER over the identical distinct-value multiset, and the
// admitted transitions are order-insensitive (counting / exact integer /
// Int128 accumulation), so transvalues — and output bytes — match the C
// sort-based path on every input. Memory stays C-shaped: past the work_mem
// budget the group degrades to the very tuplesort it displaced (nodeagg
// `degrade_distinct_set`), whose own spill machinery then applies.
// ===========================================================================

/// The plain exact-DISTINCT build sink: accept = the delegated per-row
/// transition program (`agg_plain_build_accept`, set collect included);
/// finish = nothing (the drive runs the delegated `agg_plain_finish` after
/// the drain, mirroring the fold drive's retrieve step).
///
/// `key_direct` (v2, the batched-insert lever): when the node's one
/// transition is a set-mode integer DISTINCT over exactly outer column 0
/// (`agg_plain_distinct_direct_shape`) AND the scan armed the direct key
/// staging (`seq_scan_sortkey_direct` — the sort breaker's own matcher/
/// staging, shared), `accept_batch` serves each staged row's key straight
/// off the SoA column and hands the whole batch to one staged set insert
/// (batched hashing + row-order probes) — no per-row transition program, no
/// per-row collect scan. Narrow-tuple fallback rows keep the full per-row
/// path. Value identity: the per-row program's entire effect for the
/// admitted shape is "park outer col 0 + set-insert", and set insertion
/// order is replay-invisible (the admission's order-insensitivity grant).
struct PlainDistinctAggBuildSink<'a, 'mcx> {
    agg: &'a mut ::nodeagg::AggStateData<'mcx>,
    key_direct: bool,
    /// Direct key is text (`DistinctKeyKind::Bytes`): staged keys route
    /// through the bytes/dict batch inserts instead of the integer feed.
    key_bytes: bool,
    keys: Vec<::datum::Datum>,
    ints: Vec<i64>,
    hashes: Vec<u64>,
    /// Dict-code insert memo, EPOCH-SCOPED (cleared whenever the staged dict
    /// lane's epoch changes): bit = this code's value was already fed this
    /// epoch. Never carries across epochs — epoch-scoped ids are not stable
    /// value identities (the set stores full bytes; the memo only filters
    /// repeat inserts, which every downstream consumer dedups anyway).
    dict_memo: Vec<u64>,
    dict_epoch: Option<u64>,
}

impl<'a, 'mcx> PlainDistinctAggBuildSink<'a, 'mcx> {
    fn new(
        agg: &'a mut ::nodeagg::AggStateData<'mcx>,
        key_direct: bool,
        key_bytes: bool,
    ) -> Self {
        PlainDistinctAggBuildSink {
            agg,
            key_direct,
            key_bytes,
            keys: Vec::new(),
            ints: Vec::new(),
            hashes: Vec::new(),
            dict_memo: Vec::new(),
            dict_epoch: None,
        }
    }
}

impl<'mcx> Sink<'mcx> for PlainDistinctAggBuildSink<'_, 'mcx> {
    fn accept(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<SinkFeed> {
        ::nodeagg::agg_plain_build_accept(self.agg, estate, tuple)?;
        Ok(SinkFeed::NeedMore)
    }

    fn finish(&mut self, _estate: &mut EStateData<'mcx>) -> PgResult<()> {
        Ok(())
    }
}

impl<'mcx> BatchSink<'mcx> for PlainDistinctAggBuildSink<'_, 'mcx> {
    fn accept_batch<E: BatchEmit<'mcx>>(
        &mut self,
        emit: &mut E,
        pos: u32,
        n: u32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()> {
        if !self.key_direct {
            // Default per-row delegation loop, verbatim.
            for i in pos..n {
                if let Some(slot) = emit.emit(i, estate)? {
                    ::nodeagg::agg_plain_build_accept(self.agg, estate, slot)?;
                }
            }
            return Ok(());
        }
        // Dict-coded text window (the cbstore zero-decode lane): consume
        // codes+dict for the whole window — the key's datum cells are stale
        // while a lane is up, so `emit_key` must not run. The memo dedups
        // per epoch (row group); a repeat code's value was already fed and
        // every downstream consumer dedups exactly.
        if self.key_bytes {
            if let Some(lane) = emit.key_dict_lane() {
                let t = lane.table;
                if self.dict_epoch != Some(t.epoch) {
                    self.dict_memo.clear();
                    self.dict_memo.resize((t.ndict as usize).div_ceil(64), 0);
                    self.dict_epoch = Some(t.epoch);
                }
                // SAFETY: the lane covers the staged window's `n` rows and
                // `ndict` dict entries (the fill's contract); consumed
                // before the next window stages.
                let (codes, dict) = unsafe {
                    (
                        core::slice::from_raw_parts(lane.codes, n as usize),
                        core::slice::from_raw_parts(t.dict, t.ndict as usize),
                    )
                };
                return ::nodeagg::agg_plain_distinct_insert_dict_batch(
                    self.agg,
                    estate,
                    &codes[pos as usize..],
                    dict,
                    &mut self.dict_memo,
                );
            }
        }
        // Direct staged-key feed (page-level CFI in the staging fetch —
        // the sort breaker's emit_key cadence).
        self.keys.clear();
        let mut saw_null = false;
        for i in pos..n {
            match emit.emit_key(i) {
                Some((d, false)) => self.keys.push(d),
                Some((_, true)) => saw_null = true,
                None => {
                    // Narrow-tuple fallback row: the full per-row path.
                    if let Some(slot) = emit.emit(i, estate)? {
                        ::nodeagg::agg_plain_build_accept(self.agg, estate, slot)?;
                    }
                }
            }
        }
        if self.key_bytes {
            return ::nodeagg::agg_plain_distinct_insert_bytes_batch(
                self.agg,
                estate,
                &self.keys,
                saw_null,
            );
        }
        ::nodeagg::agg_plain_distinct_insert_batch(
            self.agg,
            estate,
            &self.keys,
            saw_null,
            &mut self.ints,
            &mut self.hashes,
        )
    }
}

/// Try to let the lane own an AGG_PLAIN exact-DISTINCT `Agg` over a
/// `SeqScan` (section doc above). `Some(result)` = the lane drove this call;
/// `None` = refused (the caller falls to the per-tuple pull — whose
/// collect/replay uses the SAME set state, so a per-call fallback is
/// value-safe in both directions).
fn try_own_plain_distinct_agg_over_seq_scan<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Scan-side refuse-set: the Phase-1 gate verbatim (dynamic EPQ/direction
    // gates re-checked per call; structural verdict memoized on the node).
    if !seq_scan_fusible(ss, estate)? {
        return Ok(None);
    }
    // exec_agg's top-of-call guard: the one result row is out; a drained agg
    // stays drained until rescan clears `agg_done`.
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    // One OWNED tick per lane-owned plain-agg build event (the gate's
    // aggbuild floor counts builds; this drive runs the whole feed inside
    // one call).
    stats::tick_owned(ShapeClass::AggBuild);
    trace_feed("plain-agg distinct-set drive engaged");
    // v2 batched-insert arm: single set-mode DISTINCT over exactly outer
    // column 0 with the scan's direct key staging (no qual, covered column —
    // the sort breaker's own matcher; integer keys stage fixed-width, text
    // keys stage varlena pointers, dict-encoded cbstore text windows answer
    // codes+dict). Probed BEFORE the first produce, exactly as `sort_feed`
    // probes: arming decides staging.
    let key_direct = ::nodeagg::agg_plain_distinct_direct_shape(agg)
        && ::nodeseqscan::seq_scan_sortkey_direct(ss, estate);
    let key_bytes = key_direct && ::nodeagg::agg_plain_distinct_key_is_bytes(agg);
    if key_bytes && ::nodeseqscan::seq_scan_key_dict_arm(ss) {
        trace_feed("distinct-set direct text key feed armed (dict-capable)");
    } else if key_direct {
        trace_feed("distinct-set direct key feed armed");
    } else {
        // Kernel-shaped quals vectorize via the staged selection bitmap; the
        // set feed itself is per-row (the DISTINCT park is per-row).
        arm_seq_scan_qual_bitmap(ss, estate, "agg distinct-set feed", true);
        ::nodeseqscan::seq_scan_stitch_arm(ss);
    }
    // initialize_aggregates (delegated): fresh initval pergroups + cleared
    // sets; a rescan re-enters here with agg_done cleared.
    ::nodeagg::agg_plain_build_begin(agg, estate)?;
    let mut sink = PlainDistinctAggBuildSink::new(agg, key_direct, key_bytes);
    drain_pipeline(ss, &mut SeqScanSource, &mut SeqScanFilterProject, &mut sink, estate)?;
    // Retrieve (delegated): set replay + finalize + HAVING + project — one
    // row (or none, when the var-free HAVING rejects it), setting agg_done.
    Ok(Some(::nodeagg::agg_plain_finish(agg, estate)?))
}

/// Try to let the lane own `Agg(AGG_PLAIN, all-DISTINCT) → Sort → SeqScan`
/// by SKIPPING the Sort — the q9/Q14-family plan shape: the planner serves a
/// single DISTINCT aggregate by sorting the whole input and marking the
/// aggregate `aggpresorted` (adjacent-dedup). When EVERY transition of the
/// node is replayed from an exact-DISTINCT set
/// (`agg_plain_distinct_set_only` — presorted entries get force-armed into
/// set-mode), the Sort's ONLY observable effect is that dedup, so feeding
/// the UNSORTED scan into the sets produces identical values with the whole
/// O(n log n) sort deleted: the order-relaxation charter's headline grant.
/// `None` = refused; the caller falls to the per-tuple `exec_agg` over
/// `exec_sort` (which, if the drive armed set-mode on an earlier call,
/// still computes identical values — the arming doc in nodeagg).
#[inline]
pub fn try_own_plain_distinct_agg_over_sort<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    s: &mut crate::procnode::SortNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Refusals here are SILENT: every refused offer falls through to
    // `try_own_sorted_agg_over_sort`, whose gates tick the identical
    // accounting for this node (a tick here too would double-count the
    // (class, reason) cadence the gate files ratchet).
    if !::nodeagg::agg_plain_distinct_set_only(agg) {
        return Ok(None);
    }
    // Dynamic per-call gates (mirror the sorted-agg-over-sort arm).
    if estate.es_epq_active || !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        return Ok(None);
    }
    // Sort-side structural verdict — the sort arms' shared memo (covers
    // random access + the child scan's own refuse-set, EXPLAIN ANALYZE
    // included).
    let fusible = match s.lane_fusible {
        Some(v) => v,
        None => {
            let refuse = sort_refuse_reason(s, estate)?;
            if let Some(r) = refuse {
                stats::tick_refused(ShapeClass::SortFeed, r);
            }
            let v = refuse.is_none();
            s.lane_fusible = Some(v);
            v
        }
    };
    if !fusible {
        return Ok(None);
    }
    // v1 scope: SeqScan child only (the q9-class shape; index/bitmap-fed
    // sorts under an all-DISTINCT plain agg keep the C drive). Silent for
    // the same fall-through reason as above.
    if !matches!(&*s.outer, crate::procnode::PlanStateNode::SeqScan(_)) {
        return Ok(None);
    }
    // exec_agg's top-of-call guard: a drained agg stays drained.
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    // C's CHECK_FOR_INTERRUPTS at the would-be ExecSort feed entry.
    ::postgres_seams::check_for_interrupts::call()?;
    stats::tick_owned(ShapeClass::AggBuild);
    trace_feed("plain-agg distinct-set skip-sort drive engaged");
    // Arm set-mode for the presorted entries BEFORE any input (sticky;
    // value-safe on later fallbacks — nodeagg's arming doc).
    ::nodeagg::agg_force_distinct_set(agg);
    let crate::procnode::PlanStateNode::SeqScan(ss) = &mut *s.outer else {
        unreachable!("matched SeqScan above")
    };
    // v2 batched-insert arm (the over-SeqScan drive's twin; the Sort node's
    // tlist is its child's, so outer column 0 through the skipped Sort IS
    // scan output column 0 — the same column `seq_scan_sortkey_direct`
    // proves is one covered scan Var with no qual).
    let key_direct = ::nodeagg::agg_plain_distinct_direct_shape(agg)
        && ::nodeseqscan::seq_scan_sortkey_direct(ss, estate);
    let key_bytes = key_direct && ::nodeagg::agg_plain_distinct_key_is_bytes(agg);
    if key_bytes && ::nodeseqscan::seq_scan_key_dict_arm(ss) {
        trace_feed("distinct-set direct text key feed armed (dict-capable)");
    } else if key_direct {
        trace_feed("distinct-set direct key feed armed");
    } else {
        arm_seq_scan_qual_bitmap(ss, estate, "agg distinct-set skip-sort feed", true);
        ::nodeseqscan::seq_scan_stitch_arm(ss);
    }
    ::nodeagg::agg_plain_build_begin(agg, estate)?;
    let mut sink = PlainDistinctAggBuildSink::new(agg, key_direct, key_bytes);
    drain_pipeline(ss, &mut SeqScanSource, &mut SeqScanFilterProject, &mut sink, estate)?;
    Ok(Some(::nodeagg::agg_plain_finish(agg, estate)?))
}

/// Build phase of the hash-agg breaker over a SeqScan feed (once, lazily on
/// the first call), with the choice-dependent feed: drain the scan pipeline
/// into the breaker sink — the lanefold whole-batch feed for
/// `AggLaneChoice::Fold`, the per-row breaker feed otherwise — then finalize
/// (delegated). `table_filled` is the phase flag; a rescan rebuild clears it
/// and re-enters here. Shared by the bare agg hook above and the
/// Limit-over-agg chain (`try_own_limit`), so both drive the identical build.
fn agg_seq_scan_build_if_needed<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    c: AggLaneChoice,
    stage_slot: &mut Option<ExecSlotId>,
    xk: &mut Option<Box<ExprKeyState>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert_ne!(c, AggLaneChoice::Refuse);
    if ::nodeagg::agg_hash_table_filled(agg) {
        return Ok(());
    }
    // One OWNED tick per lane-owned hash-agg build event (the gate's
    // aggbuild floor counts builds, not calls) — fold-fed and per-row
    // feeds alike.
    stats::tick_owned(ShapeClass::AggBuild);
    // Staging arm per feed shape (see `arm_scan_staging` — the one seam for
    // deform + bitmap + stitched-tier setup across the feed sites).
    if c == AggLaneChoice::Fold {
        if let Some(xk) = xk.as_deref_mut() {
            // Expression-group-key feed (projected scans; exprkey module).
            // A staging rebuild that lost the arm falls back per-row inside
            // the feed's per-batch route — byte-safe either way.
            let _ = exprkey::exprkey_rearm(xk, ss, estate);
            return exprkey::exprkey_build_fold_feed(agg, ss, xk, stage_slot, estate);
        }
        arm_scan_staging(ss, estate, ScanFeedShape::HashAggFold { agg })?;
        agg_hash_build_fold_feed(agg, ss, stage_slot, estate)
    } else {
        arm_scan_staging(ss, estate, ScanFeedShape::HashAggPerRow { agg })?;
        let mut sink = HashAggBuildSink { agg };
        drain_pipeline(ss, &mut SeqScanSource, &mut SeqScanFilterProject, &mut sink, estate)
    }
}

/// Whether the K2 deferred probe could host this agg's build (the plan-level
/// half of the scan feed's admission — the SoA half needs the armed batch,
/// checked in `scan_k2_shape`). Used to force the SoA deform for shapes whose
/// fold reads no lane columns (count(*)-only plans) but whose key lane the
/// deferred probe wants staged.
fn scan_k2_wanted<'mcx>(agg: &::nodeagg::AggStateData<'mcx>) -> bool {
    k2_enabled()
        && ::nodeagg::agg_lanefold_plan(agg).is_some_and(|plan| !plan.guarded)
        && !::nodeagg::agg_lanefold_has_resid(agg)
        && ::nodeagg::agg_hash_staged_probe_col(agg).is_some()
}

// ===========================================================================
// Stage-2.1 dict-code grouping (cbstore-v2 plan §2.1 — the LowCardinality /
// DuckDB dict-grouping analog): when the K2 scan feed's single grouping key
// is a dict-encoded cbstore column, the feed opts the key into the dict lane
// (`dict_want`) and groups each dict-answered window ON THE u32 CODES — a
// per-epoch (row-group) DIRECT-INDEXED array maps code → the group's live
// pergroup state in the GLOBAL C-ported tuplehash, resolved LAZILY on the
// first surviving row of each (epoch, code): dict[code] is materialized ONCE
// per epoch and probed through the same `agg_hash_probe_staged` leg the Raw
// K2 path uses (same first-arrival insertion order — the resolve happens AT
// the first row that would have probed — same entry initialization, same
// spill decisions, same read-back). Per-row work drops from hash+probe to
// one array index; the k-per-epoch resolves are off the hot path, which is
// why the GLOBAL table stays the C tuplehash (full semantics/spill/retrieve
// delegation; the compact table's text-arena hosting is a non-blocking
// follow-up — its probe-speed edge is amortized away here).
//
// Rejected alternative (charter option A): per-epoch PARTIAL states merged
// at epoch boundaries — needs combine machinery per transtype and breaks
// first-arrival order; direct global pointers keep the C transition code
// running exactly once per row into the one true state.
//
// NULLs: dict lanes are NULL-free by the cbstore per-chunk proof (the store
// writes no NULLs today) — asserted per batch, never assumed structurally.
// Raw windows (non-dict-encoded key chunks) fall back to the Raw K2 keys
// path within the same build, byte-identically.
// ===========================================================================

/// `PGRUST_LANE_V2_DICTGROUP` kill switch (default ON inside the lane).
fn dictgroup_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_LANE_V2_DICTGROUP").as_deref(), Ok("0") | Ok("off"))
    })
}

/// Dict-group admission + columnar staging arm, tried when the standard
/// fixed-width-prefix arm refused (a varlena column — typically the text
/// grouping key itself — sits inside the fold prefix). Admission (§2.1):
///   * cbstore scan, unprojected (callers gate), lane fold plan classified;
///   * the K2 deferred probe wants the shape (`scan_k2_wanted`: unguarded,
///     no residual transitions, single kernel-hostable grouping key);
///   * no varlena-guard transitions (str MIN/MAX keeps the varkey staging /
///     per-row paths — mixed vguard+dict shapes are a follow-up);
///   * the columnar staging arms (`seq_scan_cb_dictgroup_arm`).
/// True = the SoA staging is armed with the key opted into dict lanes; the
/// fold feed's dict-group batch path consumes the codes. False = fail-open
/// (per-row / Raw paths, byte-identical), ticking the observability reason.
fn try_arm_cb_dictgroup<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> bool {
    if !dictgroup_enabled()
        || !::nodeseqscan::seq_scan_is_cbstore(ss)
        || !scan_k2_wanted(agg)
    {
        return false;
    }
    let refused = || {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::DictGroupShape);
        false
    };
    let Some(plan) = ::nodeagg::agg_lanefold_plan(agg) else { return refused() };
    if !plan.vguards.is_empty() {
        return refused();
    }
    let Some(key) = ::nodeagg::agg_hash_staged_probe_col(agg) else { return refused() };
    // The fold must not read the key column's SoA Datum cells: they are
    // STALE while a dict lane answers (e.g. `count(url) ... GROUP BY url`
    // reads url as a transition arg). Refuse — the Raw paths host it.
    if plan.cols.iter().any(|&c| c == key) {
        return refused();
    }
    let Some(prefix) = fused_agg_soa_prefix(agg, ss) else { return refused() };
    if !::nodeseqscan::seq_scan_cb_dictgroup_arm(ss, estate, prefix, key) {
        return refused();
    }
    true
}

/// Per-build dict-group state: the per-epoch direct-indexed code → global
/// pergroup map (`slots`, `ndict`-sized, cleared at every epoch roll) plus
/// the one-element hash scratch for the lazy per-code resolve.
#[derive(Default)]
struct DictGroupScratch {
    epoch: Option<u64>,
    slots: Vec<Option<core::ptr::NonNull<::execexpr::AggPerGroup>>>,
    hash1: Vec<u32>,
}

/// K2 admission inputs for the scan-fed fold feed. `needed` is the spill
/// replay's column set (`colnos_needed` — exactly what the hashagg spill
/// projection keeps); all of it must lie inside the armed SoA prefix so a
/// spill-mode miss can be replayed from the staged lanes.
struct ScanK2 {
    key_col: u16,
    needed: Vec<u16>,
    natts: usize,
}

/// Reusable per-build scratch for the K2 batch loop (qual-surviving row
/// indices, their gathered key lane, and the batched hashes).
#[derive(Default)]
struct ScanK2Scratch {
    rows: Vec<u32>,
    keys: Vec<::datum::Datum>,
    knull: Vec<bool>,
    hashes: Vec<u32>,
}

/// The scan feed's K2 admission (mirrors the joined-row feed's K2 arm in
/// `staged_feed_shape`): unguarded, no residual transitions, a single
/// kernel-hostable (int4/int8/text) grouping key — plus the scan-side
/// requirement that the key and every needed column are armed SoA lanes
/// (fixed-width prefix; a text key never arms, so this class is int-keyed).
/// `None` = keep the per-row arrival probe.
fn scan_k2_shape<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> Option<ScanK2> {
    if !scan_k2_wanted(agg) {
        return None;
    }
    let key_col = ::nodeagg::agg_hash_staged_probe_col(agg)?;
    let soa = ::nodeseqscan::seq_scan_batch_soa(ss)?;
    let (colnos_needed, max_colno) = ::nodeagg::agg_hash_needed_cols(agg);
    let natts = estate
        .slot(ss.ss.ss_ScanTupleSlot)
        .base()
        .tts_tupleDescriptor
        .as_ref()?
        .attrs
        .len();
    if colnos_needed.len() != natts
        || (key_col as usize) >= soa.ncols() as usize
        || max_colno > soa.ncols() as i32
        || !colnos_needed[key_col as usize]
    {
        return None;
    }
    let needed: Vec<u16> = colnos_needed
        .iter()
        .enumerate()
        .filter(|&(_, &b)| b)
        .map(|(c, _)| c as u16)
        .collect();
    Some(ScanK2 { key_col, needed, natts })
}

/// Survivor collection for the deferred-probe batch arms (K2 / dict-group /
/// multi-key), colagg: when the staged batch is slot-free decidable
/// (`seq_scan_batch_slotfree_filter` — no projection; no qual, or the armed
/// bitmap IS the whole qual with no requal tail), read the verdicts straight
/// off the batch state instead of running the per-row emit — the emit would
/// materialize every row into the scan slot only for the arm to discard it
/// (keys and transition inputs both read the staged SoA lanes). One
/// per-batch ExprContext reset stands in for the emit's per-row reset
/// cadence (nothing on these arms allocates per-tuple memory per row).
/// Same rows, same ascending order as the emit loop by construction; every
/// other batch keeps the per-row emit sequence, byte-identically.
///
/// Callers admit ALL-LANE batches only (no fallback rows), so the bitmap's
/// forced-fallback re-check discipline is vacuous here.
fn scan_collect_survivors<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    n: u32,
    rows: &mut Vec<u32>,
) -> PgResult<()> {
    rows.clear();
    match ::nodeseqscan::seq_scan_batch_slotfree_filter(ss) {
        Some(::nodeseqscan::SlotFreeFilter::All) => {
            estate.ecxt_mut(ss.ss.ps_ExprContext).reset();
            rows.extend(0..n);
        }
        Some(::nodeseqscan::SlotFreeFilter::Bitmap) => {
            debug_assert!(::nodeseqscan::seq_scan_batch_soa(ss)
                .is_some_and(|soa| soa.fallback_words().iter().all(|&w| w == 0)));
            estate.ecxt_mut(ss.ss.ps_ExprContext).reset();
            let sel = ::nodeseqscan::seq_scan_batch_qual_sel(ss)
                .expect("Bitmap filter implies an armed whole-qual sel");
            let nwords = (n as usize).div_ceil(64);
            let tail_mask =
                if n % 64 == 0 { u64::MAX } else { (1u64 << (n % 64)) - 1 };
            for w in 0..nwords {
                let mut bits = sel[w];
                if w == nwords - 1 {
                    bits &= tail_mask;
                }
                while bits != 0 {
                    rows.push(w as u32 * 64 + bits.trailing_zeros());
                    bits &= bits - 1;
                }
            }
        }
        None => {
            for i in 0..n {
                if ::nodeseqscan::seq_scan_batch_emit(ss, estate, i)?.is_some() {
                    rows.push(i);
                }
            }
        }
    }
    Ok(())
}

/// One page batch through the scan feed's K2 deferred probe: (1) survivor
/// collection (`scan_collect_survivors` — slot-free off the batch state when
/// decidable, the arrival loop's exact per-row emit sequence otherwise);
/// (2) one tight batched-hash loop over the survivors' staged key
/// lane (bit-identical per element to the per-row `hash_slot`, by the probe-
/// kernel contract); (3) the IN-ORDER probe of every survivor through the
/// same C-ported tuplehash lookup (kernel `find_staged` fast path for the
/// dominant found-existing case; misses take the full insert/spill leg) — so
/// first-arrival insertion order, entry initialization, memory-limit checks,
/// and spill decisions are exactly the arrival path's; spill-mode misses
/// replay the row from the SoA lanes (needed columns populated, unneeded NULL
/// — the spill projection's own treatment) and spill byte-identically;
/// (4) the whole-batch fold over the resolved pergroups. The batch's CFI ran
/// in the caller (one per staged batch — design §9 cadence).
#[allow(clippy::too_many_arguments)]
fn scan_k2_batch<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    shape: &ScanK2,
    stage_slot: &mut Option<ExecSlotId>,
    k2s: &mut ScanK2Scratch,
    dgs: Option<&mut DictGroupScratch>,
    idxs: &mut Vec<u32>,
    groups: &mut Vec<core::ptr::NonNull<::execexpr::AggPerGroup>>,
    n: u32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let ScanK2Scratch { rows, keys, knull, hashes } = k2s;
    scan_collect_survivors(ss, estate, n, rows)?;
    // Stage-2.1 dict-group window (registered key + a dict-answered window):
    // group on the u32 codes through the per-epoch direct-indexed map — no
    // per-row hashing/probing at all. A Raw-answered window (non-dict key
    // chunk) falls through to the Raw keys path below; both paths resolve
    // into the same global table in the same row order.
    if let Some(dgs) = dgs {
        let lane = ::nodeseqscan::seq_scan_batch_soa(ss)
            .and_then(|soa| soa.dict_lane(shape.key_col as usize));
        if let Some(lane) = lane {
            return scan_dictgroup_batch(
                agg, ss, shape, stage_slot, dgs, idxs, groups, rows, lane, estate,
            );
        }
    }
    keys.clear();
    knull.clear();
    {
        let soa =
            ::nodeseqscan::seq_scan_batch_soa(ss).expect("K2 scan feed requires the armed SoA");
        let kc = shape.key_col as usize;
        let (kv, kn) = (soa.col_values(kc), soa.col_isnull(kc));
        for &i in rows.iter() {
            keys.push(kv[i as usize]);
            knull.push(kn[i as usize]);
        }
    }
    // Stage-2.2 compact-table batch (nodeagg::compact): probe + new-group
    // init inside the compact table — PG hashing bypassed entirely — and the
    // usual whole-batch fold over the returned pergroups. `false` = the
    // runtime backstop migrated the table into the C tuplehash BEFORE this
    // batch; fall through to the staged probe below (same rows, same order,
    // the migrated groups' states carried over byte-for-byte).
    if ::nodeagg::agg_hash_compact_armed(agg)
        && ::nodeagg::agg_hash_compact_batch(agg, estate, keys, knull, groups)?
    {
        idxs.clear();
        idxs.extend_from_slice(rows);
        let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
            .expect("K2 scan feed requires the armed SoA");
        // SAFETY: as the staged-probe fold below — every probed row is
        // non-fallback with valid lane values for every plan column; each
        // pergroup was installed by the compact probe within this batch.
        return unsafe { agg_fold_staged(agg, soa, idxs, groups) };
    }
    ::nodeagg::agg_hash_hash_staged(agg, keys, knull, hashes)?;
    idxs.clear();
    groups.clear();
    for (k, &i) in rows.iter().enumerate() {
        match ::nodeagg::agg_hash_probe_staged(agg, estate, keys[k], knull[k], hashes[k])? {
            Some(pg) => {
                idxs.push(i);
                groups.push(pg);
            }
            None => {
                // Spill-mode miss: replay the row off the SoA lanes and spill
                // it; no transition runs (the per-row path's exact
                // treatment). The replay slot is memoized across rescan
                // rebuilds and allocated only if a build ever spills.
                let slot_id = match *stage_slot {
                    Some(s) => s,
                    None => {
                        let desc = estate
                            .slot(ss.ss.ss_ScanTupleSlot)
                            .base()
                            .tts_tupleDescriptor
                            .clone();
                        let s = estate
                            .exec_init_extra_tuple_slot(desc, ::types_slot::TupleSlotKind::Virtual);
                        *stage_slot = Some(s);
                        s
                    }
                };
                {
                    let mcx = estate.es_query_cxt;
                    let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                        .expect("K2 scan feed requires the armed SoA");
                    let slot = estate.slot_mut(slot_id);
                    ::exectuples::exec_clear_tuple(slot, mcx);
                    let base = slot.base_mut();
                    for c in 0..shape.natts {
                        base.tts_values[c] = ::datum::Datum::null();
                        base.tts_isnull[c] = true;
                    }
                    for &c in &shape.needed {
                        let c = c as usize;
                        base.tts_values[c] = soa.col_values(c)[i as usize];
                        base.tts_isnull[c] = soa.col_isnull(c)[i as usize];
                    }
                    ::exectuples::exec_store_virtual_tuple(slot);
                }
                ::nodeagg::agg_hash_spill_staged(agg, estate, slot_id, hashes[k])?;
            }
        }
    }
    let soa =
        ::nodeseqscan::seq_scan_batch_soa(ss).expect("K2 scan feed requires the armed SoA");
    // SAFETY: every probed row is non-fallback (the caller admits only
    // all-lane batches), so the SoA lanes carry valid deformed values for
    // every plan column (`plan.cols ⊆ colnos_needed ⊆` the armed prefix);
    // the plan is unguarded (K2 admission); each pergroup was installed by
    // the probe within this batch; the rest is agg_fold_staged's contract.
    unsafe { agg_fold_staged(agg, soa, idxs, groups) }
}

/// One dict-answered page batch through the dict-group path (§2.1 header
/// above `dictgroup_enabled`): per surviving row, one direct index into the
/// per-epoch code→pergroup map; unseen codes resolve lazily — dict[code]
/// materialized once per epoch and probed through the SAME staged-probe leg
/// as the Raw K2 path, at exactly the row the Raw path would have probed
/// (first-arrival insertion order, entry initialization, memory limits and
/// spill decisions all identical). Spill-mode misses replay off the SoA
/// lanes with the key materialized from the dictionary (its SoA cells are
/// stale under a dict lane) and are deliberately NOT cached: every later row
/// of that code must also spill, exactly as the per-row path would.
///
/// NULL discipline: dict codes have no NULL representation and cbstore
/// stores no NULLs (per-chunk proof, phase4 §8.3) — every dict-window row
/// probes with `isnull = false`, which is what the Raw fill would have
/// published (`isnull.fill(false)`).
#[allow(clippy::too_many_arguments)]
fn scan_dictgroup_batch<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    shape: &ScanK2,
    stage_slot: &mut Option<ExecSlotId>,
    dgs: &mut DictGroupScratch,
    idxs: &mut Vec<u32>,
    groups: &mut Vec<core::ptr::NonNull<::execexpr::AggPerGroup>>,
    rows: &[u32],
    lane: ::exectuples::SoaDictLane,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Epoch roll (epoch = row-group index, stable per pinned scan): per-RG
    // dictionaries are dense 0..ndict, so the map is a flat array — k
    // entries per epoch, cleared once per RG change.
    let ndict = lane.table.ndict as usize;
    if dgs.epoch != Some(lane.table.epoch) {
        dgs.epoch = Some(lane.table.epoch);
        dgs.slots.clear();
        dgs.slots.resize(ndict, None);
        trace_feed(&format!(
            "dict-group epoch {} (ndict={ndict})",
            lane.table.epoch
        ));
    }
    debug_assert!(dgs.slots.len() >= ndict, "dict size is fixed per epoch");
    idxs.clear();
    groups.clear();
    for &i in rows {
        let code = lane.code(i as usize) as usize;
        debug_assert!(code < ndict, "filler contract: code < ndict");
        let pg = match dgs.slots[code] {
            Some(pg) => pg,
            None => {
                // First surviving row of (epoch, code): materialize + probe
                // once. The hash rides the same probe-kernel leg as the Raw
                // path (bit-identical per the kernel contract).
                let key = lane.table.datum(code as u32);
                ::nodeagg::agg_hash_hash_staged(agg, &[key], &[false], &mut dgs.hash1)?;
                let hash = dgs.hash1[0];
                match ::nodeagg::agg_hash_probe_staged(agg, estate, key, false, hash)? {
                    Some(pg) => {
                        dgs.slots[code] = Some(pg);
                        pg
                    }
                    None => {
                        scan_dictgroup_spill(agg, ss, shape, stage_slot, i, key, hash, estate)?;
                        continue;
                    }
                }
            }
        };
        idxs.push(i);
        groups.push(pg);
    }
    let soa =
        ::nodeseqscan::seq_scan_batch_soa(ss).expect("dict-group feed requires the armed SoA");
    // SAFETY: as the Raw K2 fold — every probed row is non-fallback (cbstore
    // stages none) with valid lane values for every plan column (the
    // columnar fill stages decoded Datums; the key column is NOT in
    // `plan.cols` — grouping keys are not transition args in this shape, and
    // vguard plans refuse dict-group); the plan is unguarded (K2 admission);
    // each pergroup is a live global-table state block (allocation-stable
    // for the table's lifetime — the per-epoch cache only ever holds
    // pointers the probe installed).
    unsafe { agg_fold_staged(agg, soa, idxs, groups) }
}

/// Dict-group spill-mode miss: the Raw K2 path's replay verbatim, except the
/// grouping key cell takes the materialized dictionary datum (the key's SoA
/// cells are stale under a dict lane). `hashagg_spill_tuple` materializes the
/// slot into the spill tape, so the dict-borrowed datum's scan lifetime is
/// long enough by construction.
#[cold]
#[allow(clippy::too_many_arguments)]
fn scan_dictgroup_spill<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    shape: &ScanK2,
    stage_slot: &mut Option<ExecSlotId>,
    i: u32,
    key: ::datum::Datum,
    hash: u32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let slot_id = match *stage_slot {
        Some(s) => s,
        None => {
            let desc = estate.slot(ss.ss.ss_ScanTupleSlot).base().tts_tupleDescriptor.clone();
            let s = estate.exec_init_extra_tuple_slot(desc, ::types_slot::TupleSlotKind::Virtual);
            *stage_slot = Some(s);
            s
        }
    };
    {
        let mcx = estate.es_query_cxt;
        let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
            .expect("dict-group feed requires the armed SoA");
        let slot = estate.slot_mut(slot_id);
        ::exectuples::exec_clear_tuple(slot, mcx);
        let base = slot.base_mut();
        for c in 0..shape.natts {
            base.tts_values[c] = ::datum::Datum::null();
            base.tts_isnull[c] = true;
        }
        for &c in &shape.needed {
            let c = c as usize;
            base.tts_values[c] = soa.col_values(c)[i as usize];
            base.tts_isnull[c] = soa.col_isnull(c)[i as usize];
        }
        base.tts_values[shape.key_col as usize] = key;
        base.tts_isnull[shape.key_col as usize] = false;
        ::exectuples::exec_store_virtual_tuple(slot);
    }
    ::nodeagg::agg_hash_spill_staged(agg, estate, slot_id, hash)
}

// ===========================================================================
// Packed multi-key GROUP BY (multikey spike 2026-07-14 — the Q17/Q18-class
// `GROUP BY UserID, SearchPhrase` shapes): a batch pre-pass packs the N
// fixed-width key components of a staged window into ONE synthetic u64/u128
// key lane (REUSED per-batch scratch — the spike's 5.5ms-vs-45.5ms verdict),
// then ALL single-key compact-table machinery runs unchanged through
// `KeyRepr::Int`/`Int128` + CRC32C. A dict-coded text component is made
// packable by the per-epoch code → scan-lifetime intern-id resolve
// (dictgroup's lazy resolve retargeted from pergroup pointers to stable u32
// ids, spike §2.3); the intern table's reverse map materializes the text at
// read-back/migrate. NULL components (heap) fold into a null-bitmap byte in
// the key image (CH `nullable_keys128`); cbstore rides the no-NULLs proof.
//
// Fallback discipline: multi-key has NO C staged-probe leg (the tuplehash
// kernel is Expr) — the compact table is the ONLY packed host. The runtime
// backstop check runs BEFORE each batch's per-row emit (qual evaluated
// exactly once per row either way); after a migration the feed falls to the
// per-row arrival probe for the batch and the rest of the scan, with every
// group already in the C table (compact_migrate reconstructs component
// datums and inserts through the unmodified `lookup`, C-exact hashes).
// ===========================================================================

/// `PGRUST_LANE_V2_MULTIKEY` kill switch (default ON inside the lane).
fn multikey_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_LANE_V2_MULTIKEY").as_deref(), Ok("0") | Ok("off"))
    })
}

/// The plan-level half of the multi-key admission (the SoA/compact halves
/// need the armed batch — `scan_mk_shape`): unguarded, fully lanefold-
/// admitted, 2..N grouping keys (the single-key kernels own num_cols == 1).
/// Mirrors `scan_k2_wanted`'s role, including forcing the SoA deform so the
/// key lanes stage even for count(*)-only fold plans.
fn scan_mk_plan_wanted<'mcx>(agg: &::nodeagg::AggStateData<'mcx>) -> bool {
    multikey_enabled()
        && ::nodeagg::agg_lanefold_plan(agg).is_some_and(|plan| !plan.guarded)
        && !::nodeagg::agg_lanefold_has_resid(agg)
        && ::nodeagg::agg_hash_staged_probe_col(agg).is_none()
        && ::nodeagg::agg_hash_key_cols(agg).len() >= 2
}

/// The multi-key shapes' dict-text component, when the plan has EXACTLY one
/// raw-bytes text key among Int-class keys: `Some(input colno)`. `None` =
/// pure-int multi-key (no dict lane needed) or an unpackable component mix
/// (the compact arm refuses later with the same taxonomy).
fn scan_mk_dict_att<'mcx>(agg: &::nodeagg::AggStateData<'mcx>) -> Option<u16> {
    let mut dict = None;
    for (att, kind) in ::nodeagg::agg_hash_key_cols(agg) {
        match kind {
            ::nodeagg::GroupKeyKind::Int { .. } | ::nodeagg::GroupKeyKind::Numeric => {}
            ::nodeagg::GroupKeyKind::TextRaw => {
                if dict.is_some() {
                    return None;
                }
                dict = Some(att);
            }
            ::nodeagg::GroupKeyKind::Other => return None,
        }
    }
    dict
}

/// Multi-key dict-component columnar arm, tried when the fixed-width-prefix
/// arm refused (the text key component sits inside the prefix) — the
/// multi-key twin of `try_arm_cb_dictgroup`: arm the cbstore SoA staging
/// with the text component opted into dict lanes; the packing pre-pass
/// consumes the codes through the per-epoch intern resolve. False =
/// fail-open (per-row paths, byte-identical).
fn try_arm_cb_multikey_dict<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> bool {
    if !multikey_enabled()
        || !::nodeseqscan::seq_scan_is_cbstore(ss)
        || !scan_mk_plan_wanted(agg)
    {
        return false;
    }
    let refused = || {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::MultiKeyShape);
        false
    };
    let Some(plan) = ::nodeagg::agg_lanefold_plan(agg) else { return refused() };
    if !plan.vguards.is_empty() {
        return refused();
    }
    // Pure-int multi-key shapes need no dict lane — but a varlena column
    // INSIDE the fixed-width prefix (the reason the standard arm refused)
    // still blocks the staging. The offset-free columnar arm hosts those
    // (Q32-class `GROUP BY WatchID, ClientIP` on cbstore): every staged
    // column fills as decoded Datums, no dict registration.
    let Some(key) = scan_mk_dict_att(agg) else {
        // All-Int keys → plain columnar staging; any Other component means
        // the compact arm will refuse anyway — don't arm for nothing.
        let all_int = ::nodeagg::agg_hash_key_cols(agg).iter().all(|&(_, k)| {
            matches!(
                k,
                ::nodeagg::GroupKeyKind::Int { .. } | ::nodeagg::GroupKeyKind::Numeric
            )
        });
        if !all_int {
            return refused();
        }
        let Some(prefix) = fused_agg_soa_prefix(agg, ss) else { return refused() };
        if !::nodeseqscan::seq_scan_cb_columnar_arm(ss, estate, prefix, None) {
            return refused();
        }
        return true;
    };
    // The fold must not read the dict component's SoA Datum cells: they are
    // STALE while a dict lane answers (the dictgroup rule, unchanged).
    if plan.cols.iter().any(|&c| c == key) {
        return refused();
    }
    let Some(prefix) = fused_agg_soa_prefix(agg, ss) else { return refused() };
    if !::nodeseqscan::seq_scan_cb_dictgroup_arm(ss, estate, prefix, key) {
        return refused();
    }
    true
}

/// Multi-key admission inputs for the scan feed, decided once per build
/// (the compact table is ARMED as a side effect — mirrors the K2 +
/// compact-arm sequence).
struct ScanMk {
    /// The armed packed-key layout (component input colnos + offsets).
    shape: ::nodeagg::MkShape,
    /// The dict/intern text component's input colno, when one exists.
    dict_att: Option<u16>,
}

/// Reusable per-build scratch for the multi-key batch loop: survivors, the
/// u128 pack accumulator, the packed key lanes, and the per-epoch
/// code → intern-id cache (dictgroup's `slots` pattern, retargeted).
#[derive(Default)]
struct MkScratch {
    rows: Vec<u32>,
    packbuf: Vec<u128>,
    keys1: Vec<i64>,
    keys2: Vec<[u64; 2]>,
    epoch: Option<u64>,
    code_ids: Vec<Option<u32>>,
}

/// The scan feed's multi-key admission + compact arm, decided once per
/// build: plan-level gates (`scan_mk_plan_wanted`), the dict component's
/// lane registration (when one exists), key lanes staged in the armed SoA,
/// then the packing admission + table arm in `agg_hash_compact_try_arm_mk`.
/// `None` = keep the per-row arrival probe (byte-identical), refuse reasons
/// ticked per taxonomy.
fn scan_mk_shape<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
    estate: &EStateData<'mcx>,
) -> Option<ScanMk> {
    if !scan_mk_plan_wanted(agg) {
        return None;
    }
    let refused = |r: RefuseReason| {
        stats::tick_refused(ShapeClass::AggBuild, r);
        None
    };
    {
        let plan = ::nodeagg::agg_lanefold_plan(agg)?;
        if !plan.vguards.is_empty() {
            return refused(RefuseReason::MultiKeyShape);
        }
    }
    let is_cb = ::nodeseqscan::seq_scan_is_cbstore(ss);
    // A text component needs its dict-lane registration (cbstore only) and
    // must stay out of the fold's lane reads (stale SoA cells).
    let dict_att = scan_mk_dict_att(agg);
    let has_text = ::nodeagg::agg_hash_key_cols(agg)
        .iter()
        .any(|&(_, k)| k == ::nodeagg::GroupKeyKind::TextRaw);
    if has_text {
        let Some(att) = dict_att else { return refused(RefuseReason::MultiKeyShape) };
        if !is_cb
            || ::nodeseqscan::seq_scan_batch_dictgroup_col(ss) != Some(att)
            || ::nodeagg::agg_lanefold_plan(agg)
                .is_some_and(|plan| plan.cols.iter().any(|&c| c == att))
        {
            return refused(RefuseReason::MultiKeyShape);
        }
    }
    // Every key column must be a staged SoA lane the spillless packed feed
    // can read (colnos_needed always covers grouping columns).
    {
        let soa = ::nodeseqscan::seq_scan_batch_soa(ss)?;
        let (colnos_needed, max_colno) = ::nodeagg::agg_hash_needed_cols(agg);
        let natts = estate
            .slot(ss.ss.ss_ScanTupleSlot)
            .base()
            .tts_tupleDescriptor
            .as_ref()?
            .attrs
            .len();
        if colnos_needed.len() != natts || max_colno > soa.ncols() as i32 {
            return refused(RefuseReason::MultiKeyShape);
        }
        for (att, _) in ::nodeagg::agg_hash_key_cols(agg) {
            if att as usize >= soa.ncols() as usize || !colnos_needed[att as usize] {
                return refused(RefuseReason::MultiKeyShape);
            }
        }
    }
    // Packing admission + table arm (nullable = heap; cbstore rides the
    // no-NULLs per-chunk proof and packs no null byte).
    match ::nodeagg::agg_hash_compact_try_arm_mk(agg, !is_cb, dict_att.filter(|_| is_cb)) {
        ::nodeagg::CompactArm::Armed => {
            let shape =
                ::nodeagg::agg_hash_compact_mk_shape(agg).expect("armed multi-key table");
            Some(ScanMk { shape, dict_att: dict_att.filter(|_| is_cb) })
        }
        ::nodeagg::CompactArm::KeyKind => refused(RefuseReason::MultiKeyShape),
        ::nodeagg::CompactArm::SpillRisk => refused(RefuseReason::CompactSpillRisk),
        ::nodeagg::CompactArm::Off => None,
    }
}

/// One page batch through the packed multi-key feed. Sequence per the
/// section header: (1) backstop check BEFORE any per-row work — a migration
/// returns `false` and the caller runs the WHOLE batch (emit included)
/// through the arrival leg, so the qual runs exactly once per row; (2)
/// survivor collection (`scan_collect_survivors` — slot-free when decidable,
/// the arrival loop's exact per-row emit sequence otherwise); (3) the pack
/// pre-pass over the survivors' staged component
/// lanes into the reused packed-key scratch (dict components through the
/// per-epoch intern resolve); (4) the compact-table batch probe + new-group
/// seeding; (5) the whole-batch fold.
#[allow(clippy::too_many_arguments)]
fn scan_mk_batch<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    mk: &ScanMk,
    mks: &mut MkScratch,
    idxs: &mut Vec<u32>,
    groups: &mut Vec<core::ptr::NonNull<::execexpr::AggPerGroup>>,
    n: u32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if !::nodeagg::agg_hash_compact_backstop(agg, estate)? {
        return Ok(false);
    }
    // Numeric components: per-VALUE packability over the WHOLE batch BEFORE
    // the per-row emit — an unpackable value (range / non-minimal display
    // scale, keypack module doc) migrates to the C table and the caller
    // routes this batch through the arrival leg, so the qual still runs
    // exactly once per row. Checking a superset of the survivors is sound
    // (pack legality is per-value, effect-free).
    let numeric_packable = {
        let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
            .expect("multi-key feed requires the armed SoA");
        mk.shape.comps.iter().all(|comp| {
            let ::nodeagg::MkCompKind::Numeric { width } = comp.kind else { return true };
            let att = comp.att as usize;
            let (values, isnull) = (soa.col_values(att), soa.col_isnull(att));
            (0..n as usize).all(|i| {
                if isnull[i] {
                    // Heap NULLs pack via the null-bitmap byte; a NULL under
                    // the cbstore no-NULLs proof is a staging surprise —
                    // demote instead of asserting in release.
                    return mk.shape.nullable;
                }
                ::nodeagg::mk_numeric_datum_bits(values[i], width).is_some()
            })
        })
    };
    if !numeric_packable {
        ::nodeagg::agg_hash_compact_disarm(agg, estate)?;
        return Ok(false);
    }
    let MkScratch { rows, packbuf, keys1, keys2, epoch, code_ids } = mks;
    scan_collect_survivors(ss, estate, n, rows)?;
    // Pack pre-pass, component-major over the survivors (each component
    // lane streams once), into the REUSED u128 accumulator.
    packbuf.clear();
    packbuf.resize(rows.len(), 0u128);
    let shape = &mk.shape;
    for (j, comp) in shape.comps.iter().enumerate() {
        let att = comp.att as usize;
        let off_bits = comp.off as u32 * 8;
        match comp.kind {
            ::nodeagg::MkCompKind::Int { width } => {
                let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                    .expect("multi-key feed requires the armed SoA");
                let (values, isnull) = (soa.col_values(att), soa.col_isnull(att));
                let mask = if width == 8 { u64::MAX } else { (1u64 << (width * 8)) - 1 };
                for (k, &i) in rows.iter().enumerate() {
                    let i = i as usize;
                    if shape.nullable && isnull[i] {
                        // CH nullable_keys128: bit j set, value bits zero —
                        // NOT-DISTINCT composite NULL semantics hold.
                        packbuf[k] |= 1u128 << (shape.null_off() as u32 * 8 + j as u32);
                        continue;
                    }
                    debug_assert!(
                        shape.nullable || !isnull[i],
                        "cbstore no-NULLs proof violated in a multi-key window"
                    );
                    let v = match width {
                        2 => values[i].as_i16() as i64,
                        4 => values[i].as_i32() as i64,
                        _ => values[i].as_i64(),
                    };
                    packbuf[k] |= (((v as u64) & mask) as u128) << off_bits;
                }
            }
            ::nodeagg::MkCompKind::Numeric { width } => {
                let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                    .expect("multi-key feed requires the armed SoA");
                let (values, isnull) = (soa.col_values(att), soa.col_isnull(att));
                for (k, &i) in rows.iter().enumerate() {
                    let i = i as usize;
                    if shape.nullable && isnull[i] {
                        packbuf[k] |= 1u128 << (shape.null_off() as u32 * 8 + j as u32);
                        continue;
                    }
                    let bits = ::nodeagg::mk_numeric_datum_bits(values[i], width)
                        .expect("numeric packability proven by the batch pre-check");
                    packbuf[k] |= (bits as u128) << off_bits;
                }
            }
            ::nodeagg::MkCompKind::Intern => {
                let mcx = estate.es_query_cxt;
                let lane = ::nodeseqscan::seq_scan_batch_soa(ss)
                    .and_then(|soa| soa.dict_lane(att));
                match lane {
                    Some(lane) => {
                        // Epoch roll (dictgroup's per-RG cache, retargeted
                        // to intern ids — scan-stable, so the PACKED key is
                        // epoch-free).
                        let ndict = lane.table.ndict as usize;
                        if *epoch != Some(lane.table.epoch) {
                            *epoch = Some(lane.table.epoch);
                            code_ids.clear();
                            code_ids.resize(ndict, None);
                        }
                        debug_assert!(code_ids.len() >= ndict);
                        for (k, &i) in rows.iter().enumerate() {
                            let code = lane.code(i as usize) as usize;
                            debug_assert!(code < ndict, "filler contract: code < ndict");
                            let id = match code_ids[code] {
                                Some(id) => id,
                                None => {
                                    // First surviving row of (epoch, code):
                                    // materialize dict[code] once, intern.
                                    let d = lane.table.datum(code as u32);
                                    // SAFETY: dict entries are live non-null
                                    // text varlenas for the staged window
                                    // (dict lane contract; kernel selection
                                    // proved the column type).
                                    let v = unsafe {
                                        ::types_fmgr::datum_varlena_packed(d, mcx)
                                    }?;
                                    let id =
                                        ::nodeagg::agg_hash_compact_intern(agg, v.data());
                                    code_ids[code] = Some(id);
                                    id
                                }
                            };
                            packbuf[k] |= (id as u128) << off_bits;
                        }
                    }
                    None => {
                        // Raw-answered window (non-dict key chunk): intern
                        // the staged text datum per row — the dictgroup Raw
                        // fallback's multi-key analog (correct, colder).
                        let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                            .expect("multi-key feed requires the armed SoA");
                        let values = soa.col_values(att);
                        debug_assert!(
                            rows.iter().all(|&i| !soa.col_isnull(att)[i as usize]),
                            "cbstore no-NULLs proof violated in a multi-key window"
                        );
                        for (k, &i) in rows.iter().enumerate() {
                            let d = values[i as usize];
                            // SAFETY: staged non-null live text varlena (the
                            // columnar fill stages decoded Datums; kernel
                            // selection proved the column type).
                            let v = unsafe { ::types_fmgr::datum_varlena_packed(d, mcx) }?;
                            let id = ::nodeagg::agg_hash_compact_intern(agg, v.data());
                            packbuf[k] |= (id as u128) << off_bits;
                        }
                    }
                }
            }
        }
    }
    // Split the accumulator into the packed key lane and probe.
    if shape.two_words {
        keys2.clear();
        keys2.extend(packbuf.iter().map(|&w| [w as u64, (w >> 64) as u64]));
        ::nodeagg::agg_hash_compact_batch_mk2(agg, keys2, groups)?;
    } else {
        keys1.clear();
        keys1.extend(packbuf.iter().map(|&w| w as u64 as i64));
        ::nodeagg::agg_hash_compact_batch_mk1(agg, keys1, groups)?;
    }
    idxs.clear();
    idxs.extend_from_slice(rows);
    let soa =
        ::nodeseqscan::seq_scan_batch_soa(ss).expect("multi-key feed requires the armed SoA");
    // SAFETY: as the K2 compact fold — every probed row is non-fallback (the
    // caller admits only all-lane batches) with valid lane values for every
    // plan column (a dict component is never in `plan.cols` — admission);
    // the plan is unguarded; each pergroup was installed by the compact
    // probe within this batch.
    unsafe { agg_fold_staged(agg, soa, idxs, groups)? };
    Ok(true)
}

/// Shared fold tail for the staged fold feeds (seqscan page batches and the
/// joined-row staging buffer): the admitted transitions run whole-batch over
/// the probed rows' pergroup snapshots via `lanefold::fold_rows_grouped`,
/// generic over the staged-lanes source (`LaneCols`).
///
/// # Safety
/// `groups[k]` is the live pergroup array the probe just installed for staged
/// row `idxs[k]` (hash entries and their additional blocks are
/// allocation-stable for the table's lifetime; spill mode only redirects NEW
/// groups to the tapes — spilled rows never reach `groups`); `cols` covers
/// every staged row for every plan column; AvgAccum pergroups hold the
/// catalog's `{0,0}` int8[2] transarray, datum-copied per group at entry
/// initialization; Int128AvgAccum pergroups are NULL or hold the aggcontext
/// state the transfn chain installed, and `agg_aggcontext` is that same
/// aggcontext; guarded plans passed `check_guards` on this batch.
unsafe fn agg_fold_staged<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    cols: &impl ::lanefold::LaneCols,
    idxs: &[u32],
    groups: &[core::ptr::NonNull<::execexpr::AggPerGroup>],
) -> PgResult<()> {
    // SAFETY: forwarded caller contract.
    unsafe { agg_fold_staged_mm(agg, cols, idxs, groups, None) }
}

/// `agg_fold_staged` with the str MIN/MAX dict-code memo (lane-v2-
/// dictminmax): a `Some(scratch)` routes str advances whose column carries a
/// sorted dict-code view (`LaneCols::col_codes`) through integer code
/// compares — transvalue bytes and datumCopy sequence provably unchanged
/// (`lanefold::str_advance_coded`). The FEED owns the scratch's
/// invalidation: any row of the build that advances an admitted str
/// transition outside this call (demote, fallback, arrival-probe accept)
/// must `invalidate()` before the next fold.
///
/// # Safety
/// As `agg_fold_staged`, plus the `col_codes` contract for every answered
/// column when `mm` is `Some`.
unsafe fn agg_fold_staged_mm<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    cols: &impl ::lanefold::LaneCols,
    idxs: &[u32],
    groups: &[core::ptr::NonNull<::execexpr::AggPerGroup>],
    mm: Option<&mut ::lanefold::StrMmScratch>,
) -> PgResult<()> {
    if idxs.is_empty() {
        return Ok(());
    }
    let plan = ::nodeagg::agg_lanefold_plan(agg).expect("fold feed without a plan");
    let aggcx = ::nodeagg::agg_aggcontext(agg);
    // SAFETY: caller contract (above) is exactly fold_rows_grouped_mm's.
    unsafe { ::lanefold::fold_rows_grouped_mm(plan, cols, idxs, groups, aggcx, mm) }
}

/// Refuse-set for the lane-v2 hash-agg pipeline. Two halves:
///   * scan side: the Phase-1 `seq_scan_fusible` gate verbatim (page-batch AM,
///     uninstrumented, forward, non-parallel, non-EPQ, non-Bloom; subplan- and
///     param-bearing quals/projections run scalar-within-lane via
///     `seq_scan_batch_emit`'s hosted arms) — WIDER than the legacy fused arm's
///     `seq_agg_fusible` (any scalar qual and any admitted projection run
///     scalar-within-lane, not just kernel quals / outer-read-free tlists);
///   * agg side: `agg_hash_breaker_admissible` (batch-drainable — no grouping
///     sets / DISTINCT-or-ordered-input / merge phase / subplan transitions —
///     AGG_HASHED, initplan-param-free). AGG_PLAIN routes to the fold drive
///     above (`try_own_plain_agg_over_seq_scan`) before this gate runs.
/// A post-build merge handoff flips `agg_batch_drainable` false, so later
/// calls refuse here and fall to `exec_agg`'s merged retrieve — exactly the
/// existing `exec_agg_batched` arm's cross-call behavior.
fn agg_over_seq_scan_fusible<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if !::nodeagg::agg_hash_breaker_admissible(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AggNotDrainable);
        return Ok(false);
    }
    // A scan-side refusal ticks under the SeqScan class inside
    // `seq_scan_fusible` (memoized), so it is counted once, not re-attributed.
    seq_scan_fusible(ss, estate)
}

/// Deform prefix for the SoA page-batch deform under the fused agg drive:
/// everything the per-row consumers read from the scan slot — the agg's
/// outer-column bound (transition args + grouping columns; outer slot == scan
/// slot for unprojected scans) and the scan qual's fetch bound. None = a
/// consumer's shape is unknown; the SoA deform stays disarmed (per-row lazy
/// deform, still correct).
fn fused_agg_soa_prefix<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
) -> Option<i32> {
    let mut p = ::nodeagg::agg_batch_outer_prefix(agg)?;
    if let Some(q) = ss.ss.qual.as_deref() {
        p = p.max(q.max_fetch(::execexpr::SlotSrc::Scan)?);
    }
    Some(p)
}

/// The breaker as Sink of pipeline N: accept = the existing hashagg per-row
/// build (prepare/lookup + transition program, spill-mode spilling included);
/// finish = the existing finalize tail (spill finish, handoff install, phase
/// flip). Always `NeedMore` — a breaker consumes its whole input.
struct HashAggBuildSink<'a, 'mcx> {
    agg: &'a mut ::nodeagg::AggStateData<'mcx>,
}

impl<'mcx> Sink<'mcx> for HashAggBuildSink<'_, 'mcx> {
    fn accept(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<SinkFeed> {
        ::nodeagg::agg_hash_build_accept(self.agg, estate, tuple)?;
        Ok(SinkFeed::NeedMore)
    }

    // Stage-4 combine seam: a parallel worker's partial build hands its
    // whole table to the leader here (merge handoff); idempotent under the
    // following finish (nodeagg's combined flag).
    fn combine(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        ::nodeagg::agg_hash_build_combine(self.agg, estate)
    }

    fn finish(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        ::nodeagg::agg_hash_build_finish(self.agg, estate)
    }
}

/// Batch-granular feed: the default loop, monomorphized — each staged row
/// runs the same `agg_hash_build_accept` in the same order, with the per-row
/// dyn dispatch, `SinkFeed` matching, and consume-cursor saves elided.
impl<'mcx> BatchSink<'mcx> for HashAggBuildSink<'_, 'mcx> {}

/// The breaker as Source of pipeline N+1: produce = the existing
/// `agg_retrieve_hash_table` read-back, one final projected group row per
/// batch (the row lives in the agg's result slot — node-side, per the `Batch`
/// contract). Delegation preserves C's group output order exactly (§7's
/// pragmatic rule for this slice: same table, same iteration, same spill
/// refill → same order, so regress stays byte-comparable without the
/// annotated comparator).
struct HashAggSource;

impl<'mcx> Source<'mcx> for HashAggSource {
    type Node = ::nodeagg::AggStateData<'mcx>;

    fn produce(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<Batch>> {
        Ok(::nodeagg::agg_hash_retrieve(node, estate)?.map(|_| Batch { n: 1 }))
    }
}

/// Pass-through operator for the probe pipeline: pushes the produced group
/// row (already finalized + projected into the result slot) to the root.
/// One-row batches never outlive the producing driver round → no cursor.
struct HashAggEmit;

impl<'mcx> Operator<'mcx> for HashAggEmit {
    type Node = ::nodeagg::AggStateData<'mcx>;

    fn pending(&self, _node: &Self::Node) -> Option<Batch> {
        None
    }

    fn consume(
        &mut self,
        node: &mut Self::Node,
        batch: Batch,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        debug_assert_eq!(batch.n, 1);
        Ok(match out.accept(node.ps_ResultTupleSlot, estate)? {
            SinkFeed::Full => OpStatus::Paused,
            SinkFeed::NeedMore => OpStatus::NeedInput,
        })
    }
}

// ===========================================================================
// Sort pipeline-breaker (Phase 2 operator→operator seam). ONE node
// implementing `Sink` for pipeline N (the feed: scan source → scalar
// filter/project → sort sink) and `Source` for pipeline N+1 (the read-back:
// sort source → RootAdapter), chained by a per-node Feed→Emit phase flag —
// which is exactly the row path's `sort_Done`, so `exec_rescan_sort` resets
// the phase (and delegates tuplesort rescan semantics) unchanged, and falling
// back to `exec_sort` at any call boundary is byte-safe (same node state).
//
// Everything delegates to the row-path `Tuplesort` (design §8: default =
// delegate finalize/read-back to the row-path state): `Sink::accept` =
// `tuplesort_puttupleslot`/`putdatum`, `Sink::finish` =
// `tuplesort_performsort`, `Source::produce` = `tuplesort_gettupleslot`/
// `getdatum` — via `nodesort`'s lane seam, over the SAME `SortState` the
// per-tuple `exec_sort` / fused `exec_sort_batched` use. Output order is
// therefore C's exactly, by construction. The feed is the Phase-1 scan
// pipeline (same sources, same per-row scalar emit) with the breaker as its
// sink instead of the root adapter, so the put sequence equals the per-tuple
// feed's — byte-identical.
// ===========================================================================

/// Try to let the lane own a `Sort` over a lane-fusible scan child. `Some` =
/// the lane drove this call; `None` = refused (caller runs the unchanged
/// `exec_sort`/`exec_sort_batched` paths — byte-safe even mid-stream, since
/// both drive the same node state).
#[inline]
pub fn try_own_sort<'mcx>(
    s: &mut crate::procnode::SortNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic gates, every call (cheap): EPQ can engage between calls on the
    // same node tree, and only forward pulls keep the tuplesort read-back
    // cursor in step.
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::SortFeed, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(ShapeClass::SortFeed, RefuseReason::Backward);
        return Ok(None);
    }
    if !sort_lane_fusible_memo(s, estate)? {
        return Ok(None);
    }
    // C's CHECK_FOR_INTERRUPTS at ExecSort entry.
    ::postgres_seams::check_for_interrupts::call()?;

    let crate::procnode::SortNode { state, outer, outer_desc, .. } = s;
    if !sort_feed_if_needed(state, &mut **outer, outer_desc, None, estate)? {
        // Feed-time refuse (agg-over-join multi-batch spill), before any
        // sort-side effect: the Volcano fallback resumes byte-identically.
        return Ok(None);
    }
    // Emit phase (pipeline N+1): the breaker's Source face streams the
    // tuplesort read-back through the root pull-adapter, one tuple per call.
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step(state, &mut SortEmitSource, &mut SortEmit, &mut root, estate)?))
}

/// Structural sort-breaker verdict, memoized at first call: the fusibility
/// cascade must not run once per pulled tuple, and a mid-stream verdict flip
/// would desync the staged-batch cursors. Shared by the bare sort hook and
/// the Limit/Unique-over-sort chains and the wave-4 chains over the sort
/// breaker (Group / Result / SubqueryScan) — all of which admit exactly the
/// sort shapes the breaker admits.
fn sort_lane_fusible_memo<'mcx>(
    s: &mut crate::procnode::SortNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    match s.lane_fusible {
        Some(v) => Ok(v),
        None => {
            // Refusal accounting ticks exactly here — once per memoized
            // structural verdict (a child-scan refusal's specific reason is
            // ticked under the child's class inside its fusible gate).
            let refuse = sort_refuse_reason(s, estate)?;
            if let Some(r) = refuse {
                stats::tick_refused(ShapeClass::SortFeed, r);
            }
            let v = refuse.is_none();
            s.lane_fusible = Some(v);
            Ok(v)
        }
    }
}

/// Feed phase of the sort breaker (pipeline N), once, lazily: drive the scan
/// pipeline to exhaustion into the breaker sink, then finalize (performsort)
/// — all inside one call, exactly like `exec_sort`'s build leg. `sort_Done`
/// is the phase flag; a rescan clears it and re-enters here. Shared by the
/// bare sort hook, the Limit/Unique-over-sort chains, and the wave-4 chains
/// over the sort breaker.
///
/// `Ok(false)` = feed-time refuse (only the agg-over-hash-join arm's
/// multi-batch spill, BEFORE the sort was touched or any owned tick fired):
/// the caller must refuse ownership; no lane tuple has been emitted and the
/// completed join build is byte-identical to the row path's, so the Volcano
/// fallback (`exec_sort` over the per-tuple agg over `exec_hash_join`)
/// resumes exactly.
fn sort_feed_if_needed<'mcx>(
    state: &mut ::nodesort::SortState<'mcx>,
    outer: &mut crate::procnode::PlanStateNode<'mcx>,
    outer_desc: &Option<std::rc::Rc<::types_tuple::TupleDescData<'static>>>,
    narrow: Option<usize>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if state.sort_done() {
        return Ok(true);
    }
    // Hash-agg breaker child: build the agg FIRST (its own build-event tick
    // cadence), refusing before any sort-side effect on a multi-batch join
    // spill; then the agg's emit face feeds the breaker sink one finalized
    // group row per produce — exactly the row stream `exec_sort`'s feed loop
    // pulls from `exec_agg`, in C's retrieve order (per-row, matching the
    // per-tuple pull cadence: no staged batch exists over agg output).
    //
    // The vectorized topk_cut pre-filter never applies here (it runs over a
    // staged SoA key lane, which only scan feeds stage) — but the EMIT-side
    // boundary cut does: on the admitted `GROUP BY … ORDER BY count-agg
    // LIMIT k` shape, `topn_emit_arm` hoists the bounded sort's
    // compare-and-discard in front of each group's key reconstruction,
    // finalize, projection and tuple-form (see `sort_feed_agg_topn`).
    if let crate::procnode::PlanStateNode::Agg(aps) = outer {
        let aps = &mut **aps;
        // exec_agg's top-of-call guard: a drained agg stays drained (its
        // retrieve below yields EOF immediately — the empty feed C's
        // `exec_sort` would build from a drained child).
        if !::nodeagg::agg_is_done(&aps.agg) {
            let built = match &mut aps.outer {
                crate::procnode::PlanStateNode::SeqScan(ss) => {
                    let c = aps.lane_choice.expect("admission decided the agg lane choice");
                    agg_seq_scan_build_if_needed(
                        &mut aps.agg,
                        ss,
                        c,
                        &mut aps.lane_stage_slot,
                        &mut aps.lane_exprkey,
                        estate,
                    )?;
                    true
                }
                crate::procnode::PlanStateNode::HashJoin(hj) => {
                    agg_hash_join_build_if_needed(
                        &mut aps.agg,
                        &mut **hj,
                        &mut aps.lane_stage_slot,
                        estate,
                    )?
                }
                crate::procnode::PlanStateNode::Gather(g) => {
                    agg_gather_build_if_needed(
                        &mut aps.agg,
                        &mut **g,
                        &mut aps.lane_stage_slot,
                        estate,
                    )?;
                    true
                }
                _ => unreachable!("agg_child_fusible admitted a non-lane agg feed"),
            };
            if !built {
                return Ok(false);
            }
        }
        stats::tick_owned(ShapeClass::SortFeed);
        let outer_desc = outer_desc.as_ref().expect("Sort already ended").clone();
        match topn_emit_arm(state, &aps.agg) {
            Some(spec) => sort_feed_agg_topn(state, &mut aps.agg, outer_desc, spec, estate)?,
            None => sort_feed(
                state,
                &mut aps.agg,
                HashAggSource,
                HashAggEmit,
                outer_desc,
                None,
                estate,
                None,
                false,
            )?,
        }
        return Ok(true);
    }
    // One OWNED tick per lane-owned sort feed event (the gate's sortfeed
    // floor counts feeds, not calls).
    stats::tick_owned(ShapeClass::SortFeed);
    let outer_desc = outer_desc.as_ref().expect("Sort already ended").clone();
    match outer {
        crate::procnode::PlanStateNode::SeqScan(ss) => {
            arm_scan_staging(ss, estate, ScanFeedShape::RowFeed { ctx: "sort feed", stitch: true })?;
            // Zone-adaptive top-N granule order (cbstore bounded sorts; None
            // = physical order, exactly as before). Armed BEFORE topk_cut_arm
            // so both read the staged qual state the staging arm left.
            let adaptive = adaptive_topk_arm(state, &outer_desc, ss)?;
            let tracked = adaptive.is_some_and(|a| a.tracked);
            // Streaming top-k cutoff (bounded sorts over an admitted
            // qual-less seqscan; None = feed unfiltered, exactly as before).
            // Composes with the direct-key put: the keep-mask filters first,
            // then the direct-key arm reads only surviving rows.
            let topk = topk_cut_arm(state, ss, estate);
            sort_feed(
                state,
                ss,
                SeqScanSource,
                SeqScanFilterProject,
                outer_desc.clone(),
                narrow,
                estate,
                topk,
                tracked,
            )?;
            // Tracked adaptive feed: an arrival-order-sensitive tie at the
            // LIMIT cut demotes — fresh tuplesort, adaptive disarmed, full
            // physical-order re-feed, reproducing the never-adaptive feed
            // byte-for-byte. Under the default relaxed mode only
            // CUT-SELECTION ambiguity demotes (which rows are returned must
            // stay exact); retained-tie emit order is the ratified
            // relaxation surface (docs/conformance/tie-ordering.md rule 3)
            // and is accepted as produced.
            let ambiguity = if tracked {
                ::nodesort::sort_lane_topk_tie_ambiguity(state)
            } else {
                None
            };
            let ambiguity = match ambiguity {
                Some(::tuplesort::TopkTieAmbiguity::RetainedOrder)
                    if adaptive_topk_mode() == AdaptiveTopkMode::Relaxed =>
                {
                    stats::tick_adaptive_topk_tie_relaxed();
                    lane_trace("adaptive topk retained-tie order relaxed (rule 3)");
                    None
                }
                other => other,
            };
            if let Some(kind) = ambiguity {
                stats::tick_adaptive_topk_demoted();
                lane_trace(match kind {
                    ::tuplesort::TopkTieAmbiguity::CutSelection => {
                        "adaptive topk demoted (cut-selection tie): physical re-feed"
                    }
                    ::tuplesort::TopkTieAmbiguity::RetainedOrder => {
                        "adaptive topk demoted (retained-tie order): physical re-feed"
                    }
                });
                ::nodesort::sort_lane_reset_for_refeed(state);
                ::nodeseqscan::seq_scan_adaptive_disarm_rescan(ss, estate)?;
                let topk = topk_cut_arm(state, ss, estate);
                sort_feed(
                    state,
                    ss,
                    SeqScanSource,
                    SeqScanFilterProject,
                    outer_desc,
                    narrow,
                    estate,
                    topk,
                    false,
                )?;
            }
        }
        crate::procnode::PlanStateNode::IndexScan(is) => sort_feed(
            state,
            is,
            IndexScanSource,
            IndexScanEmit,
            outer_desc,
            narrow,
            estate,
            None,
            false,
        )?,
        crate::procnode::PlanStateNode::IndexOnlyScan(ios) => sort_feed(
            state,
            &mut **ios,
            IndexOnlyScanSource,
            IndexOnlyScanEmit,
            outer_desc,
            narrow,
            estate,
            None,
            false,
        )?,
        crate::procnode::PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            // The bitmap must be built before the heap drive — the same
            // setup the bitmap arm runs before offering the scan.
            if !b.scan.initialized {
                crate::procnode::bitmap_table_scan_setup_dispatch(b, estate)?;
            }
            sort_feed(
                state,
                &mut b.scan,
                BitmapHeapScanSource,
                BitmapHeapScanEmit,
                outer_desc,
                narrow,
                estate,
                None,
                false,
            )?
        }
        _ => unreachable!("memoized sort verdict admitted a non-scan child"),
    }
    Ok(true)
}

/// Structural refuse-set for the sort breaker. Sort-side: refuse
/// `randomAccess` (EXEC_FLAG_REWIND/BACKWARD/MARK at init — scrollable and
/// backward cursors plus the mergejoin-outer mark/restore protocol need
/// tuplesort random access the forward-only emit pipeline doesn't drive);
/// bounded (top-N) IS admitted — `sort_lane_begin` applies
/// ALLOWBOUNDED/set_bound exactly as `exec_sort`. Child-side: the Phase-1
/// scan refuse-sets, verbatim (the feed is the Phase-1 scan pipeline with the
/// breaker as its sink) — these also cover EXPLAIN ANALYZE, since an
/// instrumented tree wraps every node in the `Instrumented` variant, which
/// matches no scan arm. The admitted checks are all init-stable, so the
/// verdict is memoizable; the caller re-checks the dynamic EPQ/direction
/// gates per call.
fn sort_refuse_reason<'mcx>(
    s: &mut crate::procnode::SortNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<RefuseReason>> {
    if s.state.randomAccess {
        return Ok(Some(RefuseReason::RandomAccess));
    }
    // Hash-agg BREAKER child (the final `ORDER BY agg ... LIMIT k` tail over
    // aggregate output): the agg's Source face (its retrieve/emit) feeds the
    // sort breaker exactly as a scan source would — breaker-composes-breaker,
    // the `try_own_agg_over_hash_join` precedent. Admission is the Limit
    // chain's exact agg-child gate (`agg_child_fusible`: the agg-side breaker
    // gate × the admitted feed children × the economics memo), so the sort
    // admits precisely where a Limit-over-agg chain would. All the admitted
    // checks are init-stable or child-memoized, keeping this verdict
    // memoizable like the scan arms'.
    if let crate::procnode::PlanStateNode::Agg(aps) = &mut *s.outer {
        return Ok(if agg_child_fusible(aps, estate)? {
            None
        } else {
            Some(RefuseReason::ChildNotLaneOwned)
        });
    }
    scan_child_fusible(&mut s.outer, estate)
}

/// Shared child-side gate for breakers fed by a Phase-1 scan pipeline (sort
/// and hash-join build/probe feeds): the Phase-1 scan refuse-sets, verbatim.
/// `None` = admitted; `Some(NonScanChild)` = not a lane-fusible scan node
/// type; `Some(ChildScanRefused)` = the child scan's own refuse-set refused
/// (the specific reason is ticked under the child's class inside its fusible
/// gate). These also cover EXPLAIN ANALYZE (an instrumented tree wraps every
/// node in the `Instrumented` variant, which matches no scan arm).
fn scan_child_fusible<'mcx>(
    child: &mut crate::procnode::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<RefuseReason>> {
    let child_ok = match child {
        crate::procnode::PlanStateNode::SeqScan(ss) => seq_scan_fusible(ss, estate)?,
        crate::procnode::PlanStateNode::IndexScan(is) => index_scan_fusible(is, estate),
        crate::procnode::PlanStateNode::IndexOnlyScan(ios) => {
            index_only_scan_fusible(ios, estate)
        }
        crate::procnode::PlanStateNode::BitmapHeapScan(b) => {
            bitmap_heap_scan_fusible(&b.scan, estate)
        }
        _ => return Ok(Some(RefuseReason::NonScanChild)),
    };
    Ok(if child_ok {
        None
    } else {
        Some(RefuseReason::ChildScanRefused)
    })
}

/// Feed phase driver: build the tuplesort (`sort_lane_begin` — `exec_sort`'s
/// construction verbatim), then run pipeline N to exhaustion into the breaker
/// sink. Mirrors `exec_sort`'s build leg in forcing a forward child read for
/// the feed's duration (restored on success; an error aborts the query).
/// `narrow_keys`: `Some(k)` = the grouped exact-DISTINCT order-relaxation
/// arm — begin the tuplesort with only the first `k` sort keys
/// (`sort_lane_begin_narrowed`; the caller proved the dropped suffix is
/// observation-free). `None` = `exec_sort`'s construction verbatim.
fn sort_feed<'mcx, S, O>(
    sort: &mut ::nodesort::SortState<'mcx>,
    scan: &mut S::Node,
    mut src: S,
    mut op: O,
    outer_desc: std::rc::Rc<::types_tuple::TupleDescData<'static>>,
    narrow_keys: Option<usize>,
    estate: &mut EStateData<'mcx>,
    topk: Option<TopkCut>,
    tie_track: bool,
) -> PgResult<()>
where
    S: Source<'mcx>,
    O: Operator<'mcx, Node = S::Node>,
{
    match narrow_keys {
        Some(k) => ::nodesort::sort_lane_begin_narrowed(sort, outer_desc, k)?,
        None => ::nodesort::sort_lane_begin(sort, outer_desc)?,
    }
    // Zone-adaptive tracked mode: record boundary-tie events so the caller
    // can demote before any output escapes (see the adaptive block above).
    if tie_track {
        ::nodesort::sort_lane_topk_tie_track_arm(sort);
    }
    // Direct sort-key feed (`exec_sort_batched`'s `key_direct` probe,
    // mirrored): probed once per feed, BEFORE the first `produce` (arming
    // decides what the staging pass stages), datum sorts only — exactly the
    // incumbent's probe placement inside its `node.datumSort` arm.
    let key_direct = ::nodesort::sort_lane_is_datum(sort) && op.arm_sort_key(scan, estate);
    let dir = estate.es_direction;
    estate.es_direction = ::types_scan::sdir::ForwardScanDirection;
    let mut sink = SortBreakerSink { sort, key_direct, topk: topk.map(TopkCutState::new) };
    drain_pipeline(scan, &mut src, &mut op, &mut sink, estate)?;
    estate.es_direction = dir;
    Ok(())
}

// ===========================================================================
// Streaming top-k cutoff on the sort-breaker feed (cbstore-v2 plan §2.8;
// ClickHouse PartialSortingTransform's threshold filter, our shape). For a
// bounded (top-N) sort, once the tuplesort's bounded heap is FULL every
// further put is compare-against-the-k-th-boundary-and-usually-discard. The
// pre-filter hoists that discard in front of the breaker: each staged batch
// is compared VECTORIZED (the existing `qual_bitmap_cmp_const` kernel, with
// the tuplesort's live k-th boundary datum as the "const") against the
// staged leading-key lane, and rows that cannot make the top k are skipped
// without an emit or a tuplesort put.
//
// CORRECTNESS INVARIANT (the proof the admission rules exist to keep): the
// pre-filter may skip EXACTLY rows the tuplesort itself would discard with
// no observable side effect. Piecewise:
//   * A bounded tuplesort in TSS_BOUNDED discards an incoming tuple iff
//     full_cmp(tuple, root) >= 0, where `root` (the bounded heap's top under
//     the reversed comparator) is the current WORST surviving member and
//     `full_cmp` is the full multi-key comparator (tuplesort.rs
//     `puttuple_bounded`).
//   * The pre-filter discards row R iff R's LEADING key is STRICTLY worse
//     than the boundary's leading key: `keep = R.k1 <op-order> boundary.k1
//     OR ties` — implemented as the non-strict keep compare (ASC keeps
//     `k1 <= b`, DESC keeps `k1 >= b`). Strictly-worse on the leading key
//     forces full_cmp(R, root) > 0 regardless of later keys — the multi-key
//     comparator is lexicographic — so every skipped row is a row tuplesort
//     would discard. Leading-key TIES ALWAYS PASS (they can still win on
//     later keys); equal-or-better rows always pass. Datum-sort ties also
//     pass and the tuplesort re-judges them — a pure subset, never a
//     different verdict.
//   * NULL leading keys are never pre-filtered (the keep mask ORs the
//     lane's null bits): a NULL's rank depends on NULLS FIRST/LAST, and the
//     tuplesort's own comparator is the authority. A NULL boundary disables
//     the batch's pre-filter entirely (nothing compares strictly-worse
//     against NULL through the kernel; conservative pass-through).
//   * Deform-fallback rows (no staged lane value) always pass.
//   * The boundary only TIGHTENS as puts replace the root (the reversed
//     heap's top is monotonically non-worsening in forward order), so the
//     once-per-batch boundary snapshot is stale-but-conservative: it only
//     lets through rows the tuplesort then judges itself.
//   * Skipping a row skips its emit body, so admission requires the emit to
//     be observation-free per row: NO scan qual (a qual evaluation C would
//     have run — including its possible error — must not be elided) and
//     only pure-Var projections (the single Var-copy kernel or the all-Var
//     census list — never a computing column). Under that shape a
//     skipped row's only C-side effects were the tuplesort compare+discard
//     (and its per-row CHECK_FOR_INTERRUPTS; the filtered path keeps one
//     CFI per staged batch, the lane's page-level cadence floor).
//   * By-value leading keys only (the CmpOp kernel families): the boundary
//     datum read from the heap root must not dangle when a later put in the
//     same batch evicts the root's tuple.
// Net: the same rows reach the tuplesort as would have survived its own
// bounded discards, in the same order, and the sorted output is
// byte-identical. This is a pure skip optimization with zero refusal
// surface — non-admission simply feeds the sort unfiltered, exactly as
// before.
// ===========================================================================

/// Armed pre-filter spec: the vectorized KEEP comparison (`key <= boundary`
/// for ASC / `key >= boundary` for DESC, in the leading key's kernel
/// family). Rows failing it (non-null, staged) are strictly worse than the
/// k-th boundary on the leading key and are skipped.
#[derive(Clone, Copy)]
struct TopkCut {
    keep: ::execexpr::CmpOp,
}

/// Map the sort's leading-key ORDER operator (the `<` or `>` operator's
/// kernel image) to the non-strict KEEP compare of the same family. `None`
/// refuses: cross-width families never appear as sort operators (both sides
/// are the key's own type) and everything else is outside the kernel
/// vocabulary.
fn topk_keep_op(cmp: ::execexpr::CmpOp) -> Option<::execexpr::CmpOp> {
    use ::execexpr::CmpOp::*;
    Some(match cmp {
        Int2Lt => Int2Le,
        Int2Gt => Int2Ge,
        Int4Lt => Int4Le,
        Int4Gt => Int4Ge,
        Int8Lt => Int8Le,
        Int8Gt => Int8Ge,
        OidLt => OidLe,
        OidGt => OidGe,
        Float4Lt => Float4Le,
        Float4Gt => Float4Ge,
        Float8Lt => Float8Le,
        Float8Gt => Float8Ge,
        _ => return None,
    })
}

/// Resolve the sort's leading INPUT column (1-based over the scan's output)
/// to a scan attnum (0-based), through an observation-free projection only:
/// no projection, the lone `JustAssignVar` Var-copy, or an all-Var census
/// projection with no arith columns. `None` = not resolvable under those
/// shapes (computing projections are refused: a row skipped by a top-k
/// pre-filter or a zone-adaptive granule skip elides its projection, and
/// only Var passthroughs are guaranteed observation-free — an elided arith
/// evaluation could elide C's error).
fn sort_leading_key_scan_attnum<'mcx>(
    state: &::nodesort::SortState<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
) -> Option<u16> {
    let plan = state.plan;
    if plan.numCols < 1 || plan.sortColIdx.is_empty() {
        return None;
    }
    let oc = plan.sortColIdx[0];
    if oc < 1 {
        return None;
    }
    match ss.ss.ps_ProjInfo.as_ref() {
        None => Some((oc - 1) as u16),
        Some(p) => match p.pi_state.kernel() {
            ::execexpr::Kernel::JustAssignVar {
                src: ::execexpr::SlotSrc::Scan,
                attnum,
                resultnum: 0,
            } if oc == 1 => Some(attnum),
            _ => {
                // Multi-column projections admit only the pure Var-copy list
                // (the ready-time scan-projection census, subplan/param-free
                // by construction, with NO arith columns). The sort's leading
                // input column then maps through the census to its scan
                // attnum.
                let cols = p.pi_state.scan_proj_cols()?;
                if cols.any_arith() || (oc as usize) > cols.n as usize {
                    return None;
                }
                match cols.cols[(oc - 1) as usize] {
                    ::execexpr::ScanProjCol::Var { attnum } => Some(attnum),
                    _ => None,
                }
            }
        },
    }
}

/// Admission + arming for the top-k cutoff over a seqscan-fed bounded sort.
/// `None` = not admitted; the feed runs unfiltered (never a lane refusal).
/// Admits: bounded sort; leading sort key resolvable to a scan column (no
/// projection, the lone `JustAssignVar` Var-copy, or an all-Var census
/// projection); NO scan qual (skipped
/// rows must have no observable per-row evaluation — see the invariant
/// block); leading-key order operator inside the by-value kernel compare
/// vocabulary (int2/4/8, oid, float4/8; ASC and DESC, any NULLS placement);
/// and the key column stageable by the fixed-width SoA prefix deform.
fn topk_cut_arm<'mcx>(
    state: &::nodesort::SortState<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> Option<TopkCut> {
    if !state.bounded {
        return None;
    }
    let attnum = sort_leading_key_scan_attnum(state, ss)?;
    // Kernel admission: order operator -> its comparison-function kernel ->
    // the keep compare. `get_opcode` is one syscache read per feed.
    let opfn = ::lsyscache::get_opcode(state.plan.sortOperators[0]).ok()?;
    let keep = topk_keep_op(::execexpr::CmpOp::for_fn_oid(opfn)?)?;
    // Key-lane staging (refuses qual-bearing scans and foreign SoA arming).
    if !::nodeseqscan::seq_scan_topk_key_arm(ss, estate, attnum) {
        return None;
    }
    stats::tick_owned(ShapeClass::TopkCut);
    lane_trace("topk cutoff armed (sort feed)");
    Some(TopkCut { keep })
}

// ===========================================================================
// Zone-ordered adaptive top-N traversal (cbstore; cbstore-v2 plan, design in
// docs/design/cbstore-zone-adaptive.md). For `ORDER BY x LIMIT k` over a
// cbstore scan, the granule directory's footer min/max gives a partial order
// on x: visiting granules best-first (zone min ascending for ASC / max
// descending for DESC) and feeding the bounded sort's k-th boundary back to
// the scan lets the scan STOP at the first granule whose bound the boundary
// strictly dominates — every remaining granule is at least as dominated
// (ClickHouse-style read-in-order early termination, but zone-map-driven, so
// it works on non-cluster-key columns).
//
// CORRECTNESS: a bound-skipped granule contains only rows STRICTLY worse
// than the current boundary on the LEADING key — rows the bounded tuplesort
// would discard with no observable side effect (topkcut's invariant, granule-
// granular; equality never skips, `strict=false` at the AM). Observation-
// freedom of the elided per-row work is the arm's admission: pure-Var
// projections (the shared resolution) and no qual / whole-qual staged
// kernels (non-erroring vocabularies; `seq_scan_adaptive_topk_arm`).
//
// TIE EXACTNESS is the residual risk: the adaptive order changes ARRIVAL
// order at the bounded heap, and both the survivor selection at a boundary
// full-key tie and the emit order among retained full-key ties are arrival-
// dependent (heap-shape effects). Shape/mode ladder:
//   * ties-invisible (every non-junk output column IS a sort key column of
//     a byte-equality type): any legal tie selection/order prints identical
//     bytes — nothing to track.
//   * payload-visible, relaxed (DEFAULT; ratified tie-ordering rule 3): the
//     tuplesort's tie tracking (armed via `sort_lane_topk_tie_track_arm`)
//     still runs, and a CUT-SELECTION trigger (which rows made the LIMIT
//     cut is arrival-dependent) DEMOTES — fresh tuplesort, adaptive
//     disarmed, full physical-order re-feed — so the SELECTED SET is always
//     the physical-order feed's. A RETAINED-ORDER trigger (same rows,
//     arrival-dependent order within equal-full-key groups) is accepted:
//     within-tie-group order is not a compatibility surface.
//   * payload-visible, `=tracked`: EITHER trigger demotes — byte-identical
//     to lane-off, the experiment/A-B channel.
// Net: lane-on output is byte-identical to lane-off except, in relaxed
// mode, for the order within equal-full-key tie groups of the emitted rows
// (tie-normalizing gates cover that channel).
// ===========================================================================

/// Armed adaptive top-N traversal for the current sort feed. `tracked` =
/// boundary-tie tracking armed (some visible output byte is not determined
/// by the sort keys; the feed demotes on an observed ambiguous tie).
#[derive(Clone, Copy)]
struct AdaptiveTopk {
    tracked: bool,
}

/// Adaptive top-N modes for `PGRUST_LANE_ADAPTIVE_TOPK` (resolved once).
#[derive(Clone, Copy, PartialEq, Eq)]
enum AdaptiveTopkMode {
    /// `=0|off`: never arm (byte-identical A/B gate channel).
    Off,
    /// `=invisible`: arm only ties-invisible shapes (every non-junk output
    /// column is a byte-equality sort key — any tie handling prints
    /// identical bytes, so the walk can never demote and never loses).
    /// The pre-relaxation default, kept as an A/B channel.
    InvisibleOnly,
    /// Default (ratified 2026-07-12, tie-ordering rule 3): additionally arm
    /// payload-visible shapes with tie tracking, but demote ONLY on
    /// cut-selection ambiguity — retained-tie emit order is accepted as-is
    /// (the ratified relaxation surface: same selected rows, possibly
    /// different order within equal-full-key groups). Q25-class shapes stop
    /// demoting (their boundary tie was pure retained order); the exactness
    /// backstop for WHICH rows are returned stays. The AM-side probe budget
    /// covers the Q24-class sparse-qual degeneration.
    Relaxed,
    /// `=tracked`: payload-visible shapes demote on EITHER trigger
    /// (retained-tie order included) — the byte-exact experiment channel.
    Tracked,
}

fn adaptive_topk_mode() -> AdaptiveTopkMode {
    static MODE: std::sync::OnceLock<AdaptiveTopkMode> = std::sync::OnceLock::new();
    *MODE.get_or_init(|| match std::env::var("PGRUST_LANE_ADAPTIVE_TOPK") {
        Ok(v) if v == "0" || v.eq_ignore_ascii_case("off") => AdaptiveTopkMode::Off,
        Ok(v) if v.eq_ignore_ascii_case("tracked") => AdaptiveTopkMode::Tracked,
        Ok(v) if v.eq_ignore_ascii_case("invisible") => AdaptiveTopkMode::InvisibleOnly,
        _ => AdaptiveTopkMode::Relaxed,
    })
}

/// Bound cap: beyond this a top-N is scan-shaped anyway (the early-stop
/// upside shrinks as k grows) and the demotion re-feed risk isn't worth it.
const ADAPTIVE_TOPK_MAX_BOUND: i64 = 1 << 16;

/// Admission + arming for the zone-adaptive traversal over a seqscan-fed
/// bounded sort. `None` = not armed (never a lane refusal — the feed runs in
/// physical order exactly as before). Admits: bounded sort with a sane
/// bound; leading sort key resolvable to a scan column through an
/// observation-free projection (the shared topk-cut resolution); leading
/// order operator in the int-family kernel vocabulary (maps ASC/DESC; float
/// and cross-width never appear as sort operators on admitted columns);
/// scan-side qual observation-freedom plus the AM's own gates (cbstore,
/// serial, exact int-family zone entries) inside
/// `seq_scan_adaptive_topk_arm`.
fn adaptive_topk_arm<'mcx>(
    state: &::nodesort::SortState<'mcx>,
    outer_desc: &::types_tuple::TupleDescData<'static>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
) -> PgResult<Option<AdaptiveTopk>> {
    let mode = adaptive_topk_mode();
    if mode == AdaptiveTopkMode::Off {
        return Ok(None);
    }
    if !state.bounded || state.bound <= 0 || state.bound > ADAPTIVE_TOPK_MAX_BOUND {
        return Ok(None);
    }
    let Some(attnum) = sort_leading_key_scan_attnum(state, ss) else {
        return Ok(None);
    };
    let Ok(opfn) = ::lsyscache::get_opcode(state.plan.sortOperators[0]) else {
        return Ok(None);
    };
    use ::execexpr::CmpOp::*;
    let desc = match ::execexpr::CmpOp::for_fn_oid(opfn) {
        Some(Int2Lt | Int4Lt | Int8Lt | OidLt) => false,
        Some(Int2Gt | Int4Gt | Int8Gt | OidGt) => true,
        _ => return Ok(None),
    };
    let tracked = !sort_topk_ties_invisible(state, outer_desc);
    if tracked && mode == AdaptiveTopkMode::InvisibleOnly {
        return Ok(None);
    }
    if !::nodeseqscan::seq_scan_adaptive_topk_arm(ss, attnum, desc)? {
        return Ok(None);
    }
    stats::tick_owned(ShapeClass::AdaptiveTopk);
    lane_trace(match (tracked, mode) {
        (false, _) => "adaptive topk armed (sort feed, ties invisible)",
        (true, AdaptiveTopkMode::Relaxed) => {
            "adaptive topk armed (sort feed, relaxed tie order)"
        }
        (true, _) => "adaptive topk armed (sort feed, tie-tracked)",
    });
    Ok(Some(AdaptiveTopk { tracked }))
}

/// True when every visible output byte is determined by the sort keys:
/// every NON-JUNK targetlist column is itself a sort key column, and every
/// sort key's comparator equality implies byte equality of the keyed column
/// (by-value int-family types; text/varchar only under the C collation,
/// where the comparator is memcmp+len). Under that shape any legal
/// selection/order of a full-key tie group prints identical bytes, so the
/// adaptive feed needs no tie tracking.
fn sort_topk_ties_invisible(
    state: &::nodesort::SortState<'_>,
    outer_desc: &::types_tuple::TupleDescData<'static>,
) -> bool {
    let plan = state.plan;
    let nkeys = plan.numCols as usize;
    if plan.sortColIdx.len() < nkeys || plan.collations.len() < nkeys {
        return false;
    }
    let keys = &plan.sortColIdx[..nkeys];
    for tle in plan.plan.targetlist.iter() {
        let Some(te) = tle.as_target_entry() else {
            return false;
        };
        if !te.resjunk && !keys.contains(&te.resno) {
            return false;
        }
    }
    for (i, &k) in keys.iter().enumerate() {
        if k < 1 || k as usize > outer_desc.natts as usize {
            return false;
        }
        use ::types_core::catalog::{
            BOOLOID, DATEOID, INT2OID, INT4OID, INT8OID, OIDOID, TEXTOID, TIMESTAMPOID,
            TIMESTAMPTZOID, VARCHAROID,
        };
        let byte_eq = match outer_desc.attr((k - 1) as usize).atttypid {
            INT2OID | INT4OID | INT8OID | OIDOID | BOOLOID | DATEOID | TIMESTAMPOID
            | TIMESTAMPTZOID => true,
            // Deterministic collations only compare equal on identical bytes
            // (C's varstr_cmp strcmp tiebreak, kept by the ported comparator
            // and the varstr_cmp_locale seam); this resolves the DEFAULT
            // collation through the database locale (the CB banks are
            // initdb'd --no-locale with no per-column COLLATE).
            TEXTOID | VARCHAROID => {
                ::varlena::text_collation_is_raw_bytes(plan.collations[i]).unwrap_or(false)
            }
            _ => false,
        };
        if !byte_eq {
            return false;
        }
    }
    true
}

/// Compute the keep mask for one staged batch, or `None` when the pre-filter
/// is not engaged for this batch (heap not yet full, NULL boundary, or no
/// staged key lane). Bits: `keep = (!isnull && key KEEP-cmp boundary) ||
/// isnull || fallback` over staged rows `0..n`; bits at `n..` are garbage
/// and never consulted.
fn topk_keep_mask<'mcx, E: BatchEmit<'mcx>>(
    cut: TopkCut,
    sort: &::nodesort::SortState<'mcx>,
    emit: &E,
    n: u32,
) -> Option<[u64; ::exectuples::SOA_BM_WORDS]> {
    let (boundary, bnull) = ::nodesort::sort_lane_topk_boundary(sort)?;
    if bnull {
        return None;
    }
    let (values, isnull, fallback) = emit.topk_key_lane(n)?;
    debug_assert!(values.len() == n as usize && isnull.len() == n as usize);
    let mut sel = [0u64; ::exectuples::SOA_BM_WORDS];
    ::execexpr::qual_bitmap_cmp_const(cut.keep, boundary, values, isnull, &mut sel);
    // NULL keys and deform-fallback rows always pass through to the
    // tuplesort's own comparator.
    for (w, (nch, fb)) in isnull.chunks(64).zip(fallback).enumerate() {
        let mut nulls = 0u64;
        for (j, &isn) in nch.iter().enumerate() {
            nulls |= (isn as u64) << j;
        }
        sel[w] |= nulls | fb;
    }
    Some(sel)
}

// ===========================================================================
// Emit-side top-N boundary cut on the hash-agg-fed sort breaker (lane-v2
// topnemit; the emit-side complement of the scan-level topk_cut above). The
// `GROUP BY keys ORDER BY count-agg DESC LIMIT k` tail (CB Q13/Q15/Q16/Q17/
// Q31–Q35 class) today EMITS EVERY GROUP — key reconstruction, finalize,
// projection, minimal-tuple form, sort put — into a bounded sort that keeps
// k. Once the bounded heap is full, each further put is compare-against-the-
// k-th-boundary-and-usually-discard; this arm hoists that compare all the
// way into the agg retrieve, in front of the WHOLE per-group emit body.
//
// CORRECTNESS INVARIANT (same family as topk_cut's, tie-relaxation-free):
//   * The retrieve skips group G iff G's leading-key value is STRICTLY worse
//     than the bounded heap root's leading key (heap FULL — `topk_boundary`
//     returns None otherwise, disabling the cut). A strictly-worse leading
//     key forces full_cmp(G, root) > 0 (lexicographic), so the tuplesort
//     would discard G with NO state change (`puttuple_bounded`'s compare<=0
//     arm frees the tuple and returns). Removing exactly no-state-change
//     puts leaves every heap transition, every tie selection, and the
//     surviving arrival ORDER identical — the sorted output is
//     byte-identical BY CONSTRUCTION, with no reliance on the ratified
//     tie-order relaxation. Leading-key ties and better keys always pass;
//     NULL/pending transvalues always pass (rank depends on NULLS placement;
//     the tuplesort's comparator stays the authority).
//   * The compared value is the group's RAW int8 transvalue; admission
//     (`topn_emit_resolve`) proves it IS the finalized, projected sort-key
//     datum: finalfn-none int8-byval aggregate (count(*)/count(x)/sum-int
//     family) projected as a bare tlist Aggref. The boundary datum is the
//     same column's datum1 in the heap root — an i64/i64 compare in the
//     leading order operator's own direction (Int8Lt/Int8Gt kernel families
//     only), matching btint8cmp exactly.
//   * Skipping a group elides its whole emit body, so admission requires it
//     observation-free: no HAVING qual, every other tlist entry a bare
//     Var/Const/Aggref, and every skipped finalfn in the pure-arithmetic
//     allowlist (`TOPN_SKIPPABLE_FINALFNS`) — nothing C could observably do
//     (no reachable error, no side effect) is elided. Skipped groups keep a
//     per-group CHECK_FOR_INTERRUPTS (the elided sort put's cadence).
//   * The boundary only TIGHTENS as puts replace the root, and it is
//     re-read from the live heap before every retrieve call; within one
//     call's skip run no puts happen, so the held boundary is
//     stale-but-conservative — it only lets through groups the tuplesort
//     then judges itself.
// Net: a pure skip optimization with zero refusal surface — non-admission
// feeds the sort through the unfiltered breaker path, exactly as before.
// Kill switch: PGRUST_LANE_V2_TOPNEMIT=0.
// ===========================================================================

/// `PGRUST_LANE_V2_TOPNEMIT` kill switch (default ON inside the lane).
fn topn_emit_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_LANE_V2_TOPNEMIT").as_deref(), Ok("0") | Ok("off"))
    })
}

/// Admission + arming for the emit-side top-N boundary cut (invariant block
/// above). `None` = not admitted; the feed runs through the unchanged
/// breaker path (never a lane refusal). Sort side: bounded, leading order
/// operator in the int8 kernel family. Agg side: `topn_emit_resolve` (bare
/// finalfn-none int8 Aggref sort key; whole emit body observation-free).
fn topn_emit_arm<'mcx>(
    state: &::nodesort::SortState<'mcx>,
    agg: &::nodeagg::AggStateData<'mcx>,
) -> Option<::nodeagg::TopnEmitSpec> {
    if !topn_emit_enabled() || !state.bounded {
        return None;
    }
    let plan = state.plan;
    if plan.numCols < 1 || plan.sortColIdx.is_empty() {
        return None;
    }
    let oc = plan.sortColIdx[0];
    if oc < 1 {
        return None;
    }
    // The leading order operator's compare kernel fixes both the key type
    // (int8 — the resolve below re-proves it on the agg) and the direction.
    let opfn = ::lsyscache::get_opcode(plan.sortOperators[0]).ok()?;
    let desc = match ::execexpr::CmpOp::for_fn_oid(opfn)? {
        ::execexpr::CmpOp::Int8Gt => true,
        ::execexpr::CmpOp::Int8Lt => false,
        _ => return None,
    };
    let transno = ::nodeagg::topn_emit_resolve(agg, oc)?;
    stats::tick_owned(ShapeClass::TopnEmit);
    lane_trace("topn emit boundary armed (agg sort feed)");
    Some(::nodeagg::TopnEmitSpec { transno, desc })
}

/// The armed agg→sort feed: `sort_feed`'s begin/finish frame around a
/// per-group pull loop that re-reads the tuplesort's live k-th boundary
/// before every retrieve (`sort_lane_topk_boundary` — None until the bounded
/// heap fills, disabling the cut) and hands it to the agg's retrieve, which
/// skips boundary-rejected groups ahead of their whole emit body. Surviving
/// groups take the exact `HashAggSource`/`HashAggEmit`/`SortBreakerSink`
/// row path: retrieve → result slot → `sort_lane_put`.
fn sort_feed_agg_topn<'mcx>(
    sort: &mut ::nodesort::SortState<'mcx>,
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    outer_desc: std::rc::Rc<::types_tuple::TupleDescData<'static>>,
    spec: ::nodeagg::TopnEmitSpec,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    ::nodesort::sort_lane_begin(sort, outer_desc)?;
    let dir = estate.es_direction;
    estate.es_direction = ::types_scan::sdir::ForwardScanDirection;
    let mut emitted: u64 = 0;
    let mut skipped: u64 = 0;
    loop {
        let cut = match ::nodesort::sort_lane_topk_boundary(sort) {
            Some((b, false)) => {
                Some(::nodeagg::TopnEmitCut { spec, bound: b.as_i64(), skipped: &mut skipped })
            }
            // Heap not yet full, or a NULL boundary (a NULL's rank depends
            // on NULLS placement): no cut, the retrieve emits unfiltered.
            _ => None,
        };
        let Some(slot) = ::nodeagg::agg_hash_retrieve_topn(agg, estate, cut)? else {
            break;
        };
        emitted += 1;
        ::nodesort::sort_lane_put(sort, estate, slot)?;
    }
    if stats::armed() {
        stats::tick_topnemit_groups(emitted + skipped, skipped);
    }
    ::nodesort::sort_lane_finish(sort, estate)?;
    estate.es_direction = dir;
    Ok(())
}

/// The breaker's `Sink` face (pipeline N endpoint). Holds the sort node by
/// `&mut` — the driver threads the SCAN node, so a breaker spanning two nodes
/// needs no driver rework: pipeline N's threaded node is the scan, and the
/// sort node rides in its sink.
struct SortBreakerSink<'a, 'mcx> {
    sort: &'a mut ::nodesort::SortState<'mcx>,
    /// Direct sort-key feed armed for this feed (datum sort whose key the
    /// leaf serves straight from its staged column — `sort_feed`'s probe).
    key_direct: bool,
    /// Armed streaming top-k cutoff (see the invariant block above); `None`
    /// = feed unfiltered.
    topk: Option<TopkCutState>,
}

/// Per-feed pre-filter state: the armed spec + the zero-cut back-off. On the
/// adversarial shape (e.g. descending input under an ASC top-k, where every
/// row beats the boundary) the filter can never cut anything and its
/// per-batch mask would be pure overhead; consecutive zero-cut batches back
/// the filter off exponentially (skip 2, 4, … up to 256 batches between
/// attempts), and any batch that cuts a row resets it. A skipped batch takes
/// the exact unfiltered path — correctness is untouched either way (pure
/// skip optimization), this only bounds the overhead of never-winning feeds.
struct TopkCutState {
    cut: TopkCut,
    /// Batches to feed unfiltered before the next filter attempt.
    skip: u32,
    /// Consecutive zero-cut filter attempts (drives the back-off width).
    fails: u32,
}

impl TopkCutState {
    const MAX_BACKOFF_SHIFT: u32 = 8; // cap: retry every 256 batches

    fn new(cut: TopkCut) -> TopkCutState {
        TopkCutState { cut, skip: 0, fails: 0 }
    }
}

impl<'mcx> Sink<'mcx> for SortBreakerSink<'_, 'mcx> {
    fn accept(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<SinkFeed> {
        ::nodesort::sort_lane_put(self.sort, estate, tuple)?;
        Ok(SinkFeed::NeedMore)
    }

    fn finish(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        ::nodesort::sort_lane_finish(self.sort, estate)
    }
}

/// Batch-granular feed: `sort_lane_put_batch` — row-for-row `sort_lane_put`
/// over the same emit stream in the same order, with the tuplesort handle
/// hoisted per batch and the by-val datum batch putter held open across the
/// batch (the `exec_sort`/`exec_sort_batched` feed arms; identical put
/// accounting, see the seam's doc).
impl<'mcx> BatchSink<'mcx> for SortBreakerSink<'_, 'mcx> {
    fn accept_batch<E: BatchEmit<'mcx>>(
        &mut self,
        emit: &mut E,
        pos: u32,
        n: u32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()> {
        struct Feed<'e, E> {
            emit: &'e mut E,
            /// Streaming top-k keep mask (None = unfiltered feed): cut rows
            /// answer `emit` with None (the qual-filtered path) and `emit_key`
            /// with None (which routes them through `emit`, filtering them) —
            /// mask first, then the direct-key put on survivors.
            sel: Option<&'e [u64; ::exectuples::SOA_BM_WORDS]>,
        }
        impl<'e, E> Feed<'e, E> {
            #[inline(always)]
            fn cut(&self, i: u32) -> bool {
                match self.sel {
                    Some(sel) => sel[(i / 64) as usize] & (1u64 << (i % 64)) == 0,
                    None => false,
                }
            }
        }
        impl<'mcx, E: BatchEmit<'mcx>> ::nodesort::SortLaneBatchFeed<'mcx> for Feed<'_, E> {
            #[inline]
            fn emit(
                &mut self,
                i: u32,
                estate: &mut EStateData<'mcx>,
            ) -> PgResult<Option<ExecSlotId>> {
                if self.cut(i) {
                    return Ok(None);
                }
                self.emit.emit(i, estate)
            }
            #[inline(always)]
            fn emit_key(&mut self, i: u32) -> Option<(::datum::Datum, bool)> {
                if self.cut(i) {
                    // Cut row: fall back to `emit`, which filters it.
                    return None;
                }
                self.emit.emit_key(i)
            }
        }
        // Streaming top-k cutoff: once the bounded heap is full, discard the
        // batch's cannot-make-top-k rows (strictly worse than the k-th
        // boundary on the leading key) before any emit or tuplesort put.
        // The mask computation completes before the put loop (the lane
        // borrow ends), and survivors take the EXACT unfiltered put path
        // (including the direct-key arm when q9triage's probe armed it).
        let mut filtered = None;
        if let Some(tk) = self.topk.as_mut() {
            if tk.skip > 0 {
                tk.skip -= 1;
            } else if let Some(sel) = topk_keep_mask(tk.cut, self.sort, &*emit, n) {
                // The skipped rows' per-row CFIs (emit-side and the
                // tuplesort discard's) are elided; keep the lane's
                // page-batch cadence floor of one check per staged batch.
                ::postgres_seams::check_for_interrupts::call()?;
                let kept: u32 = (pos..n)
                    .map(|i| ((sel[(i / 64) as usize] >> (i % 64)) & 1) as u32)
                    .sum();
                let cut_rows = n - pos - kept;
                if cut_rows == 0 {
                    // Nothing cuttable at the current boundary: back off.
                    tk.fails = (tk.fails + 1).min(TopkCutState::MAX_BACKOFF_SHIFT);
                    tk.skip = 1u32 << tk.fails;
                    // All bits set over pos..n — the put loop below would be
                    // the unfiltered feed with a dead bit test; fall through
                    // to the plain path instead.
                } else {
                    tk.fails = 0;
                    if stats::armed() {
                        stats::tick_topkcut_rows((n - pos) as u64, cut_rows as u64);
                    }
                    filtered = Some(sel);
                }
            }
        }
        match filtered {
            Some(sel) => ::nodesort::sort_lane_put_batch(
                self.sort,
                estate,
                pos,
                n,
                self.key_direct,
                &mut Feed { emit, sel: Some(&sel) },
            )?,
            None => ::nodesort::sort_lane_put_batch(
                self.sort,
                estate,
                pos,
                n,
                self.key_direct,
                &mut Feed { emit, sel: None },
            )?,
        }
        // Zone-adaptive bound feedback: hand the (possibly just-tightened)
        // k-th boundary leading-key datum to the scan before it stages the
        // next window. No-op unless the scan armed the adaptive order (the
        // emit face's default and the AM's unarmed path both drop it); a
        // NULL boundary never feeds (cbstore stores no NULLs — conservative
        // guard only).
        if let Some((bkey, false)) = ::nodesort::sort_lane_topk_boundary(self.sort) {
            emit.push_topk_bound(bkey);
        }
        Ok(())
    }
}

/// The breaker's `Source` face (pipeline N+1): each produce streams the next
/// tuple of the tuplesort read-back into `ps_ResultTupleSlot` (one-row
/// batches, like the IndexOnlyScan source — always consumed within the
/// producing driver round, so no node-resident cursor is needed; the
/// tuplesort's own read cursor is the cross-call position).
struct SortEmitSource;

impl<'mcx> Source<'mcx> for SortEmitSource {
    type Node = ::nodesort::SortState<'mcx>;

    fn produce(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<Batch>> {
        // C's per-ExecSort-call CHECK_FOR_INTERRUPTS: when a chained consumer
        // (Unique dedup, Limit's offset skip, Group's duplicate skip,
        // Result's stream) drains several sorted tuples in one PG pull, C
        // would enter ExecSort once per tuple — keep that cadence here
        // rather than once per pull (§9). Pending-gated, exactly C's
        // CHECK_FOR_INTERRUPTS macro: the unconditional seam call per
        // produced tuple measured +17% on the distinct-count shape (q9),
        // where the agg parent pulls this source once per input row.
        if ::init_small::globals::InterruptPending() {
            ::postgres_seams::check_for_interrupts::call()?;
        }
        Ok(::nodesort::sort_lane_next(node, estate)?.map(|_| Batch { n: 1 }))
    }
}

/// Push operator for the emit pipeline: pushes the staged result slot.
struct SortEmit;

impl<'mcx> Operator<'mcx> for SortEmit {
    type Node = ::nodesort::SortState<'mcx>;

    fn pending(&self, _node: &Self::Node) -> Option<Batch> {
        None
    }

    fn consume(
        &mut self,
        node: &mut Self::Node,
        batch: Batch,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        debug_assert_eq!(batch.n, 1);
        Ok(match out.accept(node.ps_ResultTupleSlot, estate)? {
            SinkFeed::Full => OpStatus::Paused,
            SinkFeed::NeedMore => OpStatus::NeedInput,
        })
    }
}

// ===========================================================================
// Sorted-agg (AGG_SORTED) streaming operator (Phase-2 breadth). NOT a
// breaker: input arrives sorted on the grouping keys, so the node emits a
// finalized group at each key boundary and never needs the whole input — it
// sits as a mid-pipeline `TupleOp` between a lane-owned ordered feed and the
// root pull-adapter:
//
//   Agg over Sort (two chained pipelines on the sort breaker):
//     pipeline N   : scan source → filter/project → SortBreakerSink
//     pipeline N+1 : SortEmitSourceCfi → SortEmit → SortedAggOp → RootAdapter
//   Agg over IndexScan / IndexOnlyScan (order from the index, one pipeline):
//     IndexScanSource → IndexScanEmit → SortedAggOp → RootAdapter
//
// The lane owns ONLY control flow; ALL semantics delegate to the row-path
// nodeagg seam (`agg_sorted_*`): the group-boundary comparison is the ported
// grouping-equality ExprState (NULL keys group together through it), the
// per-row transition program, and the finalize/HAVING/project tail are
// `agg_retrieve_sorted`'s own pieces over the node's own persort state
// (first/pending slots + `have_pending`). Because the seam maintains exactly
// the pull loop's node state — every call boundary has the current group
// closed and at most a pending boundary tuple saved — a per-call fallback to
// `exec_agg` (dynamic gates) is byte-safe in both directions.
//
// Per-tuple laziness holds: the capacity-one root buffers the boundary
// group's row, pausing the pipeline (the child feed advances only to the
// boundary tuple, which is saved in the pending slot before the pause).
// End-of-stream uses the driver's `TupleOp::source_exhausted` hook (the
// Finished-vs-more-phases seam) to finalize the last
// open group; `agg_done` (set exactly where the pull loop sets it) makes the
// drained node stay drained.
//
// v1 is deliberately per-row (correctness first): the ordered feeds emit
// one-row batches (sort read-back) or short TID runs, so there is no clean
// whole-batch group-run fold seam here yet; the lanefold `fold_rows_grouped`
// batching over contiguous group runs is a later, measured step.
// ===========================================================================

/// The sorted-agg streaming operator. `group_open` is call-local by
/// construction: the only pauses are group-row emissions (capacity-one root),
/// after which the group is already closed — so at every PG call boundary the
/// open-group flag is false and the cross-call resume state is entirely the
/// node's own `have_pending`/`agg_done`.
struct SortedAggOp<'a, 'mcx> {
    agg: &'a mut ::nodeagg::AggStateData<'mcx>,
    group_open: bool,
}

impl<'mcx> SortedAggOp<'_, 'mcx> {
    /// Start the next group from the saved pending boundary tuple.
    fn begin_from_pending(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        ::nodeagg::agg_sorted_group_begin(self.agg, estate, None)?;
        self.group_open = true;
        Ok(())
    }
}

impl<'mcx> TupleOp<'mcx> for SortedAggOp<'_, 'mcx> {
    fn pending(&self) -> bool {
        // A saved boundary tuple whose group has not started: the resume
        // after the pause that delivered the previous group's row.
        !self.group_open && ::nodeagg::agg_sorted_have_pending(self.agg)
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        if !self.group_open {
            // First row of the stream (or after a HAVING-rejected tail): the
            // group prologue — copy, initialize, first transition.
            debug_assert!(!::nodeagg::agg_sorted_have_pending(self.agg));
            ::nodeagg::agg_sorted_group_begin(self.agg, estate, Some(tuple))?;
            self.group_open = true;
            return Ok(OpStatus::NeedInput);
        }
        if ::nodeagg::agg_sorted_same_group(self.agg, estate, tuple)? {
            ::nodeagg::agg_sorted_accept(self.agg, estate, tuple)?;
            return Ok(OpStatus::NeedInput);
        }
        // Group boundary: save the boundary row first (the pull loop's
        // order), then finalize + HAVING + project the completed group.
        ::nodeagg::agg_sorted_save_pending(self.agg, estate, tuple)?;
        self.group_open = false;
        match ::nodeagg::agg_sorted_emit(self.agg, estate)? {
            Some(row) => match out.accept(row, estate)? {
                SinkFeed::Full => Ok(OpStatus::Paused),
                // Non-root sinks (none wired today): start the next group
                // immediately, as the pull loop's next iteration would.
                SinkFeed::NeedMore => {
                    self.begin_from_pending(estate)?;
                    Ok(OpStatus::NeedInput)
                }
            },
            // HAVING rejected the group: no output row; start the next group
            // from the pending boundary tuple (the pull loop's `continue`).
            None => {
                self.begin_from_pending(estate)?;
                Ok(OpStatus::NeedInput)
            }
        }
    }

    fn resume(
        &mut self,
        _out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        // The paused emit already delivered its row; resuming means starting
        // the next group from the saved boundary tuple, then asking for more
        // input.
        debug_assert!(self.pending());
        self.begin_from_pending(estate)?;
        Ok(OpStatus::NeedInput)
    }

    fn source_exhausted(
        &mut self,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        // Input exhausted: agg_done first (the pull loop's fetch-None arms),
        // then finalize the last open group, if any. Zero input rows emit
        // nothing (C's sorted-agg contract — unlike AGG_PLAIN).
        ::nodeagg::agg_sorted_input_done(self.agg);
        if !self.group_open {
            return Ok(OpStatus::Finished);
        }
        self.group_open = false;
        match ::nodeagg::agg_sorted_emit(self.agg, estate)? {
            Some(row) => match out.accept(row, estate)? {
                SinkFeed::Full => Ok(OpStatus::Paused),
                SinkFeed::NeedMore => Ok(OpStatus::Finished),
            },
            None => Ok(OpStatus::Finished),
        }
    }
}

/// Sort read-back source for the sorted-agg emit chain: `SortEmitSource`
/// plus C's per-fetch CHECK_FOR_INTERRUPTS — each row the agg pulls from the
/// sort is one `ExecSort` call in the per-tuple path, which checks at entry
/// (the bare-sort pipeline's equivalent check lives at `try_own_sort`'s
/// entry, once per pull).
struct SortEmitSourceCfi;

impl<'mcx> Source<'mcx> for SortEmitSourceCfi {
    type Node = ::nodesort::SortState<'mcx>;

    fn produce(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<Batch>> {
        ::postgres_seams::check_for_interrupts::call()?;
        Ok(::nodesort::sort_lane_next(node, estate)?.map(|_| Batch { n: 1 }))
    }
}

// ===========================================================================
// Hash-grouped exact-DISTINCT arm (lane-v2-distincthash; nodeagg
// hashgrouped.rs holds the state machine + the byte-identity argument). For
// the narrow-sort shape (CB Q9/Q10) the group-prefix SORT itself is the
// remaining dominant cost: this arm bypasses the plan's Sort node entirely —
// the scan pipeline drains into a group hash table whose entries own the
// order-insensitive transition state and the per-aggregate exact-DISTINCT
// sets, the groups order by the prefix (groups, not rows — the cheap sort),
// and the unchanged finalize/HAVING/project tail emits one group per pull.
//
// Admission tiers (demote-within-lane): the arm engages only where the
// narrow-sort admission ALREADY holds, plus all-integer group keys +
// integer set kinds + a SeqScan feed + planner-estimate economics
// (`agg_hashgroup_economical`). Anything else falls to the narrow-sort arm
// unchanged. At runtime, crossing the arm's memory budget mid-build
// DEGRADES to the narrow-sort arm exactly once: the narrowed tuplesort is
// begun late, the deferred group representatives + all remaining rows feed
// it, and the narrow emit chain resumes with per-group residual-state
// preload (nodeagg's `initialize_aggregates` hook) — so the arm is
// spill-safe wherever the narrow-sort arm is.
//
// Fallback safety: once the build consumed the scan, the plan's Sort node
// must never be fed again (it would rebuild empty from the exhausted scan).
// The no-degrade emit therefore resumes BEFORE the per-call dynamic gates —
// sound because the emit touches no scan, backward fetches imply
// randomAccess (refused at admission, so the executor never runs this node
// backward), and es_epq_active is constant for the estate this node was
// built in (EPQ rechecks run their own estate). The degraded path needs no
// such care: the sort IS built there, so the existing per-call narrow-arm
// resume (and even the per-tuple C fallback) is byte-safe.
// ===========================================================================

/// `PGRUST_LANE_V2_DISTINCTHASH` kill switch (default ON inside the lane).
fn distincthash_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_LANE_V2_DISTINCTHASH").as_deref(), Ok("0") | Ok("off"))
    })
}

/// `PGRUST_LANE_V2_DISTINCTHASH_TEXT` kill switch (default ON): text group
/// keys for the hash-grouped arm (the Q11/Q12/Q14 shape). Off, text-keyed
/// nodes fall to the narrow-sort arm exactly as before this lane — the A/B
/// attribution channel for the text-key delta.
fn distincthash_text_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_LANE_V2_DISTINCTHASH_TEXT").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// `PGRUST_LANE_V2_DISTINCTHASH_FORCE=1`: skip the planner-estimate
/// economics (e2e harness lever — small tables would otherwise refuse and
/// never exercise the arm; the runtime degrade still bounds memory).
fn distincthash_force() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        matches!(
            std::env::var("PGRUST_LANE_V2_DISTINCTHASH_FORCE").as_deref(),
            Ok("1") | Ok("on")
        )
    })
}

/// Map the plan Sort's k-key prefix onto the grouping columns as the arm's
/// group-emit order. `None` refuses (an operator outside the integer/text
/// asc/desc vocabulary — bool group keys keep the narrow-sort arm). Text
/// keys carry the plan Sort's collation for the group-order comparator
/// (`varstr_cmp`'s authority) and require it valid + DETERMINISTIC (the
/// no-ties total-order invariant; nondeterministic collations keep the C
/// sort path per the textsets rule — the equality-side admission refuses
/// them independently). The multiset equality of prefix and group columns
/// was already proven by the narrow admission; `used` disambiguates
/// duplicated columns.
fn hashgroup_order_spec(
    agg: &::nodeagg::AggStateData<'_>,
    sp: &::types_nodes::plannodes::Sort<'_>,
    k: usize,
) -> Option<Vec<::nodeagg::HashGroupOrderKey>> {
    use ::execexpr::CmpOp;
    /// pg_proc text_lt / text_gt — the btree text opclass's `<` / `>`
    /// support (varchar sorts through the same text operators).
    const F_TEXT_LT: ::types_core::Oid = 740;
    const F_TEXT_GT: ::types_core::Oid = 742;
    let group_cols = ::nodeagg::agg_plan_group_cols(agg);
    debug_assert_eq!(group_cols.len(), k);
    let mut used = vec![false; k];
    let mut out = Vec::with_capacity(k);
    for i in 0..k {
        let col = sp.sortColIdx[i];
        let j = (0..k).find(|&j| !used[j] && group_cols[j] == col)?;
        used[j] = true;
        // Sort operator -> its comparison-kernel image -> ASC/DESC (the
        // top-k cutoff's resolution path), or the text operator pair.
        let opfn = ::lsyscache::get_opcode(sp.sortOperators[i]).ok()?;
        let (desc, collation) = match opfn {
            F_TEXT_LT | F_TEXT_GT => {
                let coll = sp.collations[i];
                if coll == 0 || !::lsyscache::get_collation_isdeterministic(coll).ok()? {
                    return None;
                }
                (opfn == F_TEXT_GT, coll)
            }
            _ => {
                let desc = match CmpOp::for_fn_oid(opfn)? {
                    CmpOp::Int2Lt | CmpOp::Int4Lt | CmpOp::Int8Lt => false,
                    CmpOp::Int2Gt | CmpOp::Int4Gt | CmpOp::Int8Gt => true,
                    _ => return None,
                };
                (desc, 0)
            }
        };
        out.push(::nodeagg::HashGroupOrderKey {
            key_idx: j,
            desc,
            nulls_first: sp.nullsFirst[i],
            collation,
        });
    }
    Some(out)
}

/// The hash-grouped build sink: rows feed the group table until the shared
/// budget crosses, then the sink degrades IN PLACE — the narrowed tuplesort
/// begins late, the deferred representatives dump into it, and every
/// further row goes straight to the sort (the narrow-sort arm's feed,
/// resumed mid-stream; section doc).
struct HashGroupDistinctSink<'a, 'mcx> {
    agg: &'a mut ::nodeagg::AggStateData<'mcx>,
    sort: &'a mut ::nodesort::SortState<'mcx>,
    outer_desc: std::rc::Rc<::types_tuple::TupleDescData<'static>>,
    nkeys: usize,
    degraded: bool,
}

impl<'mcx> Sink<'mcx> for HashGroupDistinctSink<'_, 'mcx> {
    fn accept(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<SinkFeed> {
        if self.degraded {
            ::nodesort::sort_lane_put(self.sort, estate, tuple)?;
        } else if !::nodeagg::agg_hashgroup_accept(self.agg, estate, tuple)? {
            self.degrade_impl(estate)?;
        }
        Ok(SinkFeed::NeedMore)
    }

    fn finish(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        if self.degraded {
            ::nodesort::sort_lane_finish(self.sort, estate)
        } else {
            Ok(())
        }
    }
}

/// Batch-granular feed: the default per-row delegation loop (the arm's
/// accept is per-row by nature — group probe + transition program).
impl<'mcx> BatchSink<'mcx> for HashGroupDistinctSink<'_, 'mcx> {}

impl<'mcx> HashGroupDistinctSink<'_, 'mcx> {
    /// The one-shot degrade (section doc): begin the narrowed sort, dump
    /// every deferred representative, flip the table to residual mode.
    #[cold]
    #[inline(never)]
    fn degrade_impl(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        trace_feed("hash-grouped distinct arm degrading to narrowed sort");
        ::nodesort::sort_lane_begin_narrowed(self.sort, self.outer_desc.clone(), self.nkeys)?;
        let mcx = estate.es_query_cxt;
        while let Some(slot) = ::nodeagg::agg_hashgroup_next_rep(self.agg) {
            ::nodesort::sort_lane_put_slot(self.sort, mcx, slot)?;
        }
        ::nodeagg::agg_hashgroup_set_residual(self.agg);
        self.degraded = true;
        Ok(())
    }
}

/// Build outcome of the hash-grouped arm's probe.
enum HgBuild {
    /// Table built; the arm owns the emit (groups in prefix order).
    Emit,
    /// Budget crossed mid-build: the narrowed sort is fed and finished; the
    /// narrow-sort emit chain resumes over it (residual preload installed).
    Degraded,
    /// Arm not admitted (admission/economics/child shape): the caller runs
    /// the narrow-sort feed exactly as before.
    Refused,
}

/// Probe + build for the hash-grouped arm (called with the narrow admission
/// already proven and `force_distinct_set` armed, BEFORE the sort exists).
fn try_hashgroup_build<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    sort: &mut ::nodesort::SortState<'mcx>,
    outer: &mut crate::procnode::PlanStateNode<'mcx>,
    outer_desc: &Option<std::rc::Rc<::types_tuple::TupleDescData<'static>>>,
    k: usize,
    estate: &mut EStateData<'mcx>,
) -> PgResult<HgBuild> {
    if !distincthash_enabled() {
        return Ok(HgBuild::Refused);
    }
    // v1 feed scope: SeqScan child only (the Q9/Q10 shape; index/bitmap-fed
    // sorts keep the narrow-sort arm).
    let crate::procnode::PlanStateNode::SeqScan(ss) = outer else {
        return Ok(HgBuild::Refused);
    };
    if !::nodeagg::agg_hashgroup_admissible(agg)
        // Density/memory economics: the Sort's row estimate is the arm's
        // input cardinality (the sort passes every input row through).
        || !::nodeagg::agg_hashgroup_economical(
            agg,
            distincthash_force(),
            sort.plan.plan.plan_rows,
        )
    {
        return Ok(HgBuild::Refused);
    }
    let text_keys = ::nodeagg::agg_hashgroup_text_key_count(agg);
    if text_keys > 0 && !distincthash_text_enabled() {
        return Ok(HgBuild::Refused);
    }
    let Some(order) = hashgroup_order_spec(agg, sort.plan, k) else {
        return Ok(HgBuild::Refused);
    };
    ::nodeagg::agg_hashgroup_begin(agg, estate, order)?;
    trace_feed("sorted-agg hash-grouped distinct drive engaged");
    if text_keys > 0 {
        trace_feed("hash-grouped distinct arm: text group keys armed");
    }
    arm_scan_staging(
        ss,
        estate,
        ScanFeedShape::RowFeed { ctx: "hashgroup distinct feed", stitch: true },
    )?;
    let outer_desc = outer_desc.as_ref().expect("Sort already ended").clone();
    // Force a forward child read for the feed's duration (`sort_feed`'s
    // discipline — this drain replaces the sort's own feed).
    let dir = estate.es_direction;
    estate.es_direction = ::types_scan::sdir::ForwardScanDirection;
    let mut sink = HashGroupDistinctSink { agg, sort, outer_desc, nkeys: k, degraded: false };
    let fed = drain_pipeline(ss, &mut SeqScanSource, &mut SeqScanFilterProject, &mut sink, estate);
    let degraded = sink.degraded;
    estate.es_direction = dir;
    fed?;
    if degraded {
        return Ok(HgBuild::Degraded);
    }
    ::nodeagg::agg_hashgroup_finish_build(agg, estate)?;
    Ok(HgBuild::Emit)
}

/// Emit loop over the hash-grouped table: one HAVING-passing group per PG
/// pull, in the plan Sort's prefix order (C's group order). CFI per group —
/// the pull loop's per-ExecSort-fetch cadence.
fn hashgroup_emit<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    loop {
        ::postgres_seams::check_for_interrupts::call()?;
        match ::nodeagg::agg_hashgroup_emit_next(agg, estate)? {
            None => return Ok(None),
            Some(None) => continue,
            Some(Some(id)) => return Ok(Some(id)),
        }
    }
}

/// Try to let the lane own `Agg(AGG_SORTED) → Sort → scan`: the sort breaker
/// feeds once (pipeline N), then the sorted-agg operator streams the
/// read-back into one group row per PG pull. `None` = refused (caller falls
/// to the per-tuple `exec_agg` over `exec_sort`, byte-safely — see the
/// section doc on call-boundary state compatibility).
#[inline]
pub fn try_own_sorted_agg_over_sort<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    s: &mut crate::procnode::SortNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Hash-grouped arm mid-emit resume — BEFORE the dynamic gates (the arm's
    // section doc: the plan's Sort was bypassed and must never be fed from
    // the now-exhausted scan; the gates cannot flip mid-node here).
    if ::nodeagg::agg_hashgroup_emitting(agg) {
        return Ok(Some(hashgroup_emit(agg, estate)?));
    }
    // Dynamic per-call gates (mirror the bare-sort breaker).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::SortFeed, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(ShapeClass::SortFeed, RefuseReason::Backward);
        return Ok(None);
    }
    // Agg-side admission (static shape; ticked per offered call, the hashed
    // breaker's AggNotDrainable cadence).
    //
    // Grouped narrow-sort arm (v2, the ClickBench Q9/Q10 shape): an
    // AGG_SORTED node whose DISTINCT aggregates ride the plan Sort's
    // distinct-arg SUFFIX keys (aggpresorted adjacent-dedup) fails the plain
    // admission — but when every internal-sort entry is set-CAPABLE, every
    // transition is order-insensitive-exact, and the Sort's key prefix is
    // exactly the grouping columns, the suffix's only observable effect is
    // intra-group row order, which nothing observes once the drive arms
    // set-mode. The drive then feeds the sort with the comparator NARROWED
    // to the group prefix (`sort_lane_begin_narrowed`) and the exact sets
    // replace the dedup: byte-identical output (same groups, same group
    // order, same exact values), with the suffix compares and the per-row
    // dedup calls deleted. Armed only BEFORE the sort is built (arming
    // decides the feed's construction); the sticky force keeps the plain
    // admission true on later calls and any per-tuple fallback value-safe.
    let mut narrow: Option<usize> = None;
    let plain_admissible = ::nodeagg::agg_sorted_lane_admissible(agg);
    // Probe the narrow shape when the plain admission failed (the arm's
    // first engagement) OR when a prior call armed it (a rescan-rebuilt sort
    // must narrow again — the sticky force makes the plain admission true).
    if !plain_admissible || ::nodeagg::agg_distinct_set_forced(agg) {
        let sp = s.state.plan;
        let k = ::nodeagg::agg_plan_group_cols(agg).len();
        let ok = ::nodeagg::agg_sorted_distinct_narrow_admissible(agg)
            && !s.state.sort_done()
            && !s.state.bounded
            && k >= 1
            && (sp.numCols as usize) > k
            && sp.sortColIdx.len() >= k
            && {
                // Prefix == group columns as a MULTISET (order within the
                // prefix is free: grouping adjacency only needs the rows
                // prefix-sorted, whichever prefix order).
                let mut a: Vec<i16> = sp.sortColIdx[..k].to_vec();
                let mut b: Vec<i16> = ::nodeagg::agg_plan_group_cols(agg).to_vec();
                a.sort_unstable();
                b.sort_unstable();
                a == b
            };
        if !plain_admissible && !ok {
            stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AggNotDrainable);
            return Ok(None);
        }
        if ok {
            narrow = Some(k);
        }
    }
    // Sort-side structural verdict — the bare-sort arm's memo, shared (the
    // refusal ticks once per node whichever arm probes first).
    let fusible = match s.lane_fusible {
        Some(v) => v,
        None => {
            let refuse = sort_refuse_reason(s, estate)?;
            if let Some(r) = refuse {
                stats::tick_refused(ShapeClass::SortFeed, r);
            }
            let v = refuse.is_none();
            s.lane_fusible = Some(v);
            v
        }
    };
    if !fusible {
        return Ok(None);
    }
    // exec_agg's top-of-call guard: a drained agg stays drained.
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    let crate::procnode::SortNode { state, outer, outer_desc, .. } = s;
    if !state.sort_done() {
        // C's CHECK_FOR_INTERRUPTS at the feed call's ExecSort entry (the
        // emit chain's source checks per subsequent fetch).
        ::postgres_seams::check_for_interrupts::call()?;
        if let Some(k) = narrow {
            // Arm set-mode BEFORE any input (sticky; the arming doc).
            ::nodeagg::agg_sorted_force_distinct_set(agg);
            // Hash-grouped arm first (its own admission tier; Refused keeps
            // the narrow-sort feed below exactly as before).
            match try_hashgroup_build(agg, state, &mut **outer, outer_desc, k, estate)? {
                HgBuild::Emit => {
                    // One OWNED tick per lane-owned build event; the emit
                    // owns the node from here (no sort exists).
                    stats::tick_owned(ShapeClass::AggBuild);
                    return Ok(Some(hashgroup_emit(agg, estate)?));
                }
                HgBuild::Degraded => {
                    // The narrowed sort was fed and finished inside the
                    // degrade (a real sort-feed event); the narrow-sort
                    // emit chain below resumes over it, preloading residual
                    // group state at each group begin (nodeagg's
                    // initialize_aggregates hook).
                    stats::tick_owned(ShapeClass::SortFeed);
                    stats::tick_owned(ShapeClass::AggBuild);
                }
                HgBuild::Refused => {
                    trace_feed("sorted-agg distinct-set narrowed sort feed armed");
                    // The shared feed threads the narrow-key count in.
                    if !sort_feed_if_needed(state, &mut **outer, outer_desc, narrow, estate)? {
                        return Ok(None);
                    }
                    stats::tick_owned(ShapeClass::AggBuild);
                }
            }
        } else {
            // The shared feed (a sort under a sorted agg is never bounded —
            // no LIMIT pushdown crosses the agg — so its seqscan arm's top-k
            // probe no-ops; false = the agg-child arm's spill refuse,
            // byte-safe).
            if !sort_feed_if_needed(state, &mut **outer, outer_desc, narrow, estate)? {
                return Ok(None);
            }
            // One OWNED tick per lane-owned sorted-agg stream start
            // (feed/build EVENTS, once per (re)scan; the sort feed ticked
            // its own class).
            stats::tick_owned(ShapeClass::AggBuild);
        }
    }
    // Emit phase (every call): sort read-back → sorted-agg operator → root,
    // one qual-passing group row per PG pull.
    let mut op = SortedAggOp { agg, group_open: false };
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step_chain(
        state,
        &mut SortEmitSourceCfi,
        &mut SortEmit,
        &mut op,
        &mut root,
        estate,
    )?))
}

/// Try to let the lane own `Agg(AGG_SORTED) → IndexScan` (index order feeds
/// the grouping directly — no Sort node). Engagement accounting: the
/// per-pull indexscan class ticks (owned per admitted feed decision, the
/// class's documented cadence); agg-side refusals tick AggNotDrainable per
/// offered call.
#[inline]
pub fn try_own_sorted_agg_over_index_scan<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    is: &mut ::nodeindexscan::IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if !::nodeagg::agg_sorted_lane_admissible(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AggNotDrainable);
        return Ok(None);
    }
    // Child refuse-set verbatim (dynamic EPQ/direction gates included; ticks
    // under the indexscan class, per call).
    if !index_scan_fusible(is, estate) {
        return Ok(None);
    }
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    let mut op = SortedAggOp { agg, group_open: false };
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step_chain(
        is,
        &mut IndexScanSource,
        &mut IndexScanEmit,
        &mut op,
        &mut root,
        estate,
    )?))
}

/// Try to let the lane own `Agg(AGG_SORTED) → IndexOnlyScan`. Accounting as
/// the IndexScan arm (per-pull indexonlyscan class).
#[inline]
pub fn try_own_sorted_agg_over_index_only_scan<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ios: &mut ::nodeindexonlyscan::IndexOnlyScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if !::nodeagg::agg_sorted_lane_admissible(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AggNotDrainable);
        return Ok(None);
    }
    if !index_only_scan_fusible(ios, estate) {
        return Ok(None);
    }
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    let mut op = SortedAggOp { agg, group_open: false };
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step_chain(
        ios,
        &mut IndexOnlyScanSource,
        &mut IndexOnlyScanEmit,
        &mut op,
        &mut root,
        estate,
    )?))
}

// ===========================================================================
// Sorted-agg over SeqScan (lane-v2-sortedfold): the sort-free GroupAggregate
// shape — clustered/footer-sorted cbstore banks plan `Agg(AGG_SORTED) →
// SeqScan` with NO Sort node (the pathkeys come from the store order), so
// neither the hashed fold breaker (AGG_HASHED only) nor the sorted-agg-over-
// Sort arm can host it. Two drives, chosen once per node:
//
//   * Fold (`sorted_fold_step`): per staged column window, detect the group
//     boundaries over the staged SoA key lanes (width-masked raw-datum
//     compare — exactly the ported grouping-equality program's verdict under
//     the node's representational-equality grant, NULL keys grouping
//     together via the null-pair compare), then run the admitted PLAIN
//     transitions as ONE `lanefold::fold_batch` per group run — the hashed
//     feed's kernels (strlenfold's charlen included), fed per group run
//     instead of per table. Group prologue (first row), boundary emit
//     (finalize + HAVING + project), residual transitions and fallback rows
//     all delegate per row to the same `agg_sorted_*` seams the per-row
//     operator uses.
//   * PerRow (`sorted_perrow_step`): the SortedAggOp chain over the scan's
//     staged batches (SeqScanSource → SeqScanFilterProject → SortedAggOp).
//     The cbstore incumbent is the per-pull Volcano drive, so the staged
//     window decode alone wins (the noqualfeed economics); hosts every
//     `agg_sorted_lane_admissible` shape incl. exact-DISTINCT set entries.
//
// cbstore scans only: the planner produces this shape from cbstore footer
// pathkeys; heap SeqScans are never ordered, and heap's incumbent drives own
// heap agg shapes anyway (admission economics §4).
//
// Byte-identity: same rows through the same qual in the same order (the
// staged bitmap IS the kernel/PREWHERE verdict; per-row emits re-check
// exactly as the per-row drive; requal/resid shapes take the per-row-emit
// fold mode); group boundaries are the grouping-equality verdicts
// (representational grant); every fold kernel is bit-for-bit equal to C's
// transition semantics on admitted/guard-proven data (lanefold contract) and
// guarded batches that fail re-proof demote WHOLESALE to the checked per-row
// program; finalize/HAVING/project is `agg_sorted_emit` per group in input
// order — group emit order = input order = C's order. Cross-call state is
// node-resident (scan lane cursor + persort pending slot): every mid-stream
// pause happens exactly at the pull loop's call boundary (group closed,
// boundary tuple saved in the pending slot), so a per-call fallback to
// `exec_agg` is byte-safe in both directions, and the resume re-derives the
// open group's key from the group's first tuple (`agg_sorted_group_key`).
// ===========================================================================

/// Group-key columns the sorted-fold boundary compare can host: at most this
/// many by-value fixed-width grouping columns (CB shapes carry 1-2).
const SORTED_FOLD_MAX_KEYS: usize = 4;

/// The staged group-key column set for the sorted-fold arm: 0-based scan
/// column + attlen per grouping column, in grpColIdx order. None = a
/// grouping column is by-ref / dropped / out of range — the per-row drive
/// keeps the shape.
#[derive(Clone, Copy)]
struct SortedFoldKeys {
    n: usize,
    cols: [(u16, i16); SORTED_FOLD_MAX_KEYS],
}

fn sorted_fold_key_cols<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &::nodeseqscan::SeqScanState<'mcx>,
) -> Option<SortedFoldKeys> {
    let group = ::nodeagg::agg_plan_group_cols(agg);
    if group.is_empty() || group.len() > SORTED_FOLD_MAX_KEYS {
        return None;
    }
    let rel = ss.ss.ss_currentRelation.as_ref()?;
    let atts: &[_] = &rel.rd_att.compact_attrs;
    let mut cols = [(0u16, 0i16); SORTED_FOLD_MAX_KEYS];
    for (k, &attno) in group.iter().enumerate() {
        if attno < 1 {
            return None;
        }
        let c = (attno - 1) as usize;
        let att = atts.get(c)?;
        // By-value fixed-width only: the raw-datum compare's domain (by-ref
        // keys would need byte-image walks; representational TEXTEQ shapes
        // stay per-row in v1).
        if !att.attbyval || !matches!(att.attlen, 1 | 2 | 4 | 8) || att.attisdropped {
            return None;
        }
        cols[k] = (c as u16, att.attlen);
    }
    Some(SortedFoldKeys { n: group.len(), cols })
}

/// Width-masked by-value datum equality: exactly the representational
/// grouping-equality verdict for the admitted key widths (bool/int2/int4/
/// int8/date/timestamp — `group_eq_representational`'s operator set), with
/// any sign-extension convention differences between producers masked off.
#[inline(always)]
fn sorted_key_datum_eq(a: ::datum::Datum, b: ::datum::Datum, attlen: i16) -> bool {
    let mask = match attlen {
        1 => 0xffu64,
        2 => 0xffffu64,
        4 => 0xffff_ffffu64,
        _ => u64::MAX,
    };
    (a.as_u64() ^ b.as_u64()) & mask == 0
}

/// Arm fold LENGTH lanes (lane-v2-asciilen) on the staged batch: for every
/// fold plan column whose transitions are ALL one length kind (VarLenBytes
/// xor VarLenChars; CountAny rides along — it reads only isnull) and which
/// is not a grouping column, ask the staging to answer the column as i64
/// lengths (`seq_scan_batch_len_want` audits the datum-reading
/// co-consumers). On dict-encoded cbstore chunks the fill then reads ONE
/// per-code length table entry per row (string bytes touched once per
/// distinct value per row group) — the fold never materializes or
/// dereferences a per-row varlena datum; Raw chunks read the varlena header
/// (bytes) or run C's exact mb walk (chars) at the fill. A refused arm
/// keeps the datum-lane kernels byte-identically.
fn arm_fold_len_lanes<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
) {
    let Some(plan) = ::nodeagg::agg_lanefold_plan(agg) else { return };
    let group = ::nodeagg::agg_plan_group_cols(agg);
    for &c in plan.cols.iter() {
        let (mut bytes, mut chars, mut other) = (false, false, false);
        for t in plan.trans.iter() {
            if t.col != c
                || matches!(t.kind, ::lanefold::LaneKind::CountAny | ::lanefold::LaneKind::CountStar)
            {
                continue;
            }
            match t.width {
                ::lanefold::LaneWidth::VarLenBytes => bytes = true,
                ::lanefold::LaneWidth::VarLenChars => chars = true,
                _ => other = true,
            }
        }
        if other || bytes == chars {
            continue; // not a length column, or mixed kinds share the lane
        }
        if group.iter().any(|&a| a >= 1 && (a - 1) as u16 == c) {
            continue;
        }
        if ::nodeseqscan::seq_scan_batch_len_want(ss, c, chars) {
            lane_trace(&format!(
                "fold length lane armed (col {c}, {})",
                if chars { "chars" } else { "bytes" }
            ));
        }
    }
}

/// The structural lane choice for the sorted-agg-over-SeqScan drive, decided
/// once per node: Fold when the node passes the sorted-fold admission AND
/// the group keys are lane-comparable AND the staging (PREWHERE for qualled
/// scans, the offset-free columnar arm otherwise) covers every fold + key
/// column; PerRow otherwise (always available — the cbstore incumbent is the
/// per-pull Volcano drive).
fn decide_sorted_agg_lane<'mcx>(
    agg: &::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<AggLaneChoice> {
    if ss.ss.ps_ProjInfo.is_none() && ::nodeagg::agg_sorted_fold_admissible(agg) {
        if let Some(keys) = sorted_fold_key_cols(agg, ss) {
            let plan =
                ::nodeagg::agg_lanefold_plan(agg).expect("fold admission implies a plan");
            let mut maxcol = 0i32;
            for &c in plan.cols.iter().chain(plan.vguards.iter()) {
                maxcol = maxcol.max(c as i32);
            }
            for &(c, _) in &keys.cols[..keys.n] {
                maxcol = maxcol.max(c as i32);
            }
            let prefix = maxcol + 1;
            // Qualled scans require the PREWHERE lane (it owns the staging
            // and the selection bitmap; its forced prefix is widened to our
            // ask). Bare scans arm the offset-free columnar staging. A
            // refusal keeps the per-row drive — byte-safe either way.
            let armed = if ss.ss.qual.is_some() {
                ::nodeseqscan::seq_scan_cb_prewhere_arm(ss, estate, prefix)?
            } else {
                ::nodeseqscan::seq_scan_cb_columnar_arm(ss, estate, prefix, None)
            };
            if armed && ::nodeseqscan::seq_scan_batch_soa(ss).is_some() {
                arm_fold_len_lanes(agg, ss);
                trace_feed("sorted-agg fold drive armed (seqscan)");
                return Ok(AggLaneChoice::Fold);
            }
        }
    }
    // Per-row drive staging: PREWHERE/kernel-bitmap qual vectorization on
    // the staged windows (the drained per-row feed's own arm shape).
    arm_scan_staging(
        ss,
        estate,
        ScanFeedShape::RowFeed { ctx: "sorted agg per-row feed", stitch: true },
    )?;
    trace_feed("sorted-agg per-row drive armed (seqscan)");
    Ok(AggLaneChoice::PerRow)
}

/// Try to let the lane own `Agg(AGG_SORTED) → SeqScan` (section doc above).
/// `None` = refused — the caller falls to the per-tuple `exec_agg` over
/// `exec_seq_scan`, byte-safely (call-boundary state compatibility, section
/// doc).
#[inline]
pub fn try_own_sorted_agg_over_seq_scan<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    choice: &mut Option<AggLaneChoice>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // cbstore scans only (section doc): heap falls through silently — the
    // shape does not arise there and the fused drives own heap aggs.
    if !::nodeseqscan::seq_scan_is_cbstore(ss) {
        return Ok(None);
    }
    // Scan-side refuse-set: dynamic EPQ/direction gates re-checked per call;
    // structural verdict memoized on the node (ticks under the CbScan class).
    if !seq_scan_fusible(ss, estate)? {
        return Ok(None);
    }
    let c = match *choice {
        Some(c) => c,
        None => {
            let c = decide_sorted_agg_lane(agg, ss, estate)?;
            *choice = Some(c);
            // One OWNED tick per memoized ownership decision (the sorted
            // stream's build event; the per-group pulls all ride it).
            stats::tick_owned(ShapeClass::AggBuild);
            c
        }
    };
    if c == AggLaneChoice::Refuse {
        return Ok(None);
    }
    // exec_agg's top-of-call guard: a drained agg stays drained.
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    Ok(Some(match c {
        AggLaneChoice::Fold => sorted_fold_step(agg, ss, estate)?,
        _ => sorted_perrow_step(agg, ss, estate)?,
    }))
}

/// The per-row sorted drive: one PG pull's worth of the SortedAggOp chain
/// over the scan's staged batches — the sorted-agg-over-IndexScan pipeline
/// with the SeqScan source/emit pair (both proven pieces, composed).
fn sorted_perrow_step<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    let mut op = SortedAggOp { agg, group_open: false };
    let mut root = RootAdapter::new(None);
    pull_step_chain(ss, &mut SeqScanSource, &mut SeqScanFilterProject, &mut op, &mut root, estate)
}

/// One PG pull's worth of the sorted FOLD drive: walk the staged window from
/// the node-resident cursor, folding each group run whole-batch and emitting
/// one qual-passing group row per pull (pausing with the boundary tuple
/// saved pending — the pull loop's own call-boundary state). See the section
/// doc for the mode split and the byte-identity argument.
fn sorted_fold_step<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    // C's per-pull interrupt check (the pull loop's child-fetch entry).
    ::postgres_seams::check_for_interrupts::call()?;
    let keys = sorted_fold_key_cols(agg, ss).expect("Fold choice proved the key shape");
    let nkeys = keys.n;
    let mut cur_key = [(::datum::Datum::null(), false); SORTED_FOLD_MAX_KEYS];
    // Resume: a saved boundary tuple means the previous pull paused right
    // after emitting a group; start the next group from it (the pull loop's
    // next iteration) and re-derive its key from the group's first tuple.
    let mut group_open = false;
    if ::nodeagg::agg_sorted_have_pending(agg) {
        ::nodeagg::agg_sorted_group_begin(agg, estate, None)?;
        ::nodeagg::agg_sorted_group_key(agg, &mut cur_key[..nkeys]);
        group_open = true;
    }
    loop {
        // The staged window (node-resident cursor) or the next one.
        let (pos, n) = {
            let (pos, n) = ss.lane_cursor();
            if pos < n {
                (pos, n)
            } else {
                let n = ::nodeseqscan::seq_scan_next_pagebatch(ss, estate)?;
                ss.set_lane_cursor(0, n);
                if n == 0 {
                    // End of scan: drop the scan slot's pin (source parity),
                    // agg_done BEFORE the last group finalizes (the pull
                    // loop's fetch-None arms), then flush the open group.
                    let mcx = estate.es_query_cxt;
                    ::exectuples::exec_clear_tuple(
                        estate.slot_mut(ss.ss.ss_ScanTupleSlot),
                        mcx,
                    );
                    ::nodeagg::agg_sorted_input_done(agg);
                    if !group_open {
                        return Ok(None);
                    }
                    return ::nodeagg::agg_sorted_emit(agg, estate);
                }
                ::postgres_seams::check_for_interrupts::call()?;
                (0, n)
            }
        };
        match sorted_fold_window(agg, ss, &keys, &mut cur_key, &mut group_open, pos, n, estate)?
        {
            Some(row) => return Ok(Some(row)),
            None => {
                // Window consumed; produce the next one.
                debug_assert_eq!(ss.lane_cursor().0, n);
            }
        }
    }
}

/// Process staged rows `pos..n` of the current window. `Some(row)` = a group
/// row was emitted (the caller returns it to PG; the cursor already points
/// at the first unconsumed row and the boundary tuple is saved pending);
/// `None` = window fully consumed.
#[allow(clippy::too_many_arguments)]
fn sorted_fold_window<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    keys: &SortedFoldKeys,
    cur_key: &mut [(::datum::Datum, bool); SORTED_FOLD_MAX_KEYS],
    group_open: &mut bool,
    pos: u32,
    n: u32,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    let nkeys = keys.n;
    let nwords = (n as usize).div_ceil(64);
    let has_resid;
    let guarded;
    {
        let plan = ::nodeagg::agg_lanefold_plan(agg).expect("fold drive without a plan");
        has_resid = !plan.resid.is_empty();
        guarded = plan.guarded;
    }
    // Selection words for this window: the PREWHERE/kernel bitmap when one
    // owns the qual (final verdicts), all-ones on bare scans. `bitmap` mode
    // additionally requires no residual transitions and no requal tail — the
    // fold then touches selected non-fallback rows with NO per-row emits
    // except group prologues/boundaries; otherwise every row goes through
    // the per-row emit (which applies the full qual) and survivors join the
    // fold selection.
    let mut sel = [u64::MAX; ::exectuples::SOA_BM_WORDS];
    let bitmap_qual = match ::nodeseqscan::seq_scan_batch_qual_sel(ss) {
        Some(s) => {
            sel[..nwords].copy_from_slice(&s[..nwords]);
            true
        }
        None => false,
    };
    if n % 64 != 0 {
        sel[nwords - 1] &= (1u64 << (n % 64)) - 1;
    }
    let bitmap_mode = !has_resid && (bitmap_qual || ss.ss.qual.is_none());
    // Per-window demote verdict (recomputed on every resume of the same
    // window — the inputs are staged and deterministic): guard re-proof over
    // a superset of the rows the fold will touch, key lanes ready (a dict-
    // answered or fill-skipped key lane cannot serve the compare), and in
    // bitmap mode the staged SoA present. Demote = the WHOLE window runs the
    // checked per-row program (never a partial fold — lanefold contract).
    let mut demote = false;
    {
        let soa = ::nodeseqscan::seq_scan_batch_soa(ss).expect("fold drive staged the SoA");
        for &(c, _) in &keys.cols[..nkeys] {
            if !soa.col_datum_ready(c as usize) {
                demote = true;
            }
        }
    }
    if !demote && guarded {
        // Zone answers first (whole-window value intervals from the granule
        // footer), prefetched before the SoA borrow.
        let mut zmm = [(0u16, (0i64, 0i64)); 8];
        let mut nz = 0usize;
        {
            let plan = ::nodeagg::agg_lanefold_plan(agg).unwrap();
            for g in plan.guards.iter() {
                if nz == zmm.len() {
                    break;
                }
                if let Some(mm) = ::nodeseqscan::seq_scan_window_value_minmax(ss, g.col as usize)
                {
                    zmm[nz] = (g.col, mm);
                    nz += 1;
                }
            }
        }
        let plan = ::nodeagg::agg_lanefold_plan(agg).unwrap();
        let soa = ::nodeseqscan::seq_scan_batch_soa(ss).expect("fold drive staged the SoA");
        // Proof domain: staged non-fallback rows of the conservative
        // selection (lane sel under PREWHERE includes requal-pending rows —
        // a superset of everything the fold touches; unselected cells may be
        // stale under the lazy fill).
        let mut rows = [0u64; ::exectuples::SOA_BM_WORDS];
        match ::nodeseqscan::seq_scan_batch_lane_sel(ss) {
            Some(ls) => {
                for ((r, fb), s) in
                    rows[..nwords].iter_mut().zip(soa.fallback_words()).zip(ls)
                {
                    *r = s & !fb;
                }
            }
            None => {
                for ((r, fb), s) in
                    rows[..nwords].iter_mut().zip(soa.fallback_words()).zip(&sel[..nwords])
                {
                    *r = s & !fb;
                }
            }
        }
        if n % 64 != 0 {
            rows[nwords - 1] &= (1u64 << (n % 64)) - 1;
        }
        if rows[..nwords].iter().any(|&w| w != 0) {
            // SAFETY: proof rows are staged non-fallback selected rows with
            // live deformed lane values (the completing deform filled every
            // prefix column for survivor windows; vguard columns readable at
            // their varlena header byte).
            demote = unsafe {
                ::lanefold::check_guards(plan, soa, &rows[..nwords], |c| {
                    zmm[..nz].iter().find(|e| e.0 == c).map(|e| e.1)
                }) == ::lanefold::GuardCheck::Demote
            };
        }
    }
    // Copy the fallback words out so the walk below can interleave emits.
    let mut fb = [0u64; ::exectuples::SOA_BM_WORDS];
    {
        let soa = ::nodeseqscan::seq_scan_batch_soa(ss).expect("fold drive staged the SoA");
        fb[..nwords].copy_from_slice(&soa.fallback_words()[..nwords]);
    }
    // The open group's pending fold selection (contiguous same-key run being
    // accumulated); flushed before every per-row event and at window end.
    let mut run = [0u64; ::exectuples::SOA_BM_WORDS];
    let mut run_any = false;
    macro_rules! flush_run {
        () => {
            if run_any {
                let plan = ::nodeagg::agg_lanefold_plan(agg).unwrap();
                let aggcx = ::nodeagg::agg_aggcontext(agg);
                // Str MIN/MAX dict-code views for this window (lane-v2-
                // dictminmax; identity plan→scan map, no scratch —
                // fold_batch's batch winner is codes-only).
                let mm_cols = mm_str_cols(plan, Some);
                let mut mm_codes: Vec<(u16, ::exectuples::SoaDictLane)> = Vec::new();
                collect_mm_codes(ss, &mm_cols, &mut mm_codes);
                let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                    .expect("fold drive staged the SoA");
                // SAFETY: pergroup_base is the node's once-allocated current-
                // group pergroup array covering every transno
                // (initialize_aggregates re-wrote it at group begin); run
                // rows are selected non-fallback rows carrying valid
                // deformed lane values for every plan column; AvgAccum /
                // Int128AvgAccum pergroup states follow the same
                // initialize/fold/transfn chain contract as the plain feed;
                // guarded plans passed check_guards above (a demoted window
                // never reaches here).
                unsafe {
                    ::lanefold::fold_batch(
                        plan,
                        &CodesCols { inner: soa, codes: &mm_codes },
                        &run[..nwords],
                        n as usize,
                        ::nodeagg::agg_sorted_pergroup_base(agg),
                        aggcx,
                    )?;
                }
                run[..nwords].fill(0);
                run_any = false;
            }
        };
    }
    // One same-key row's full per-row delegation (fallback rows, demoted
    // windows, group prologues and boundaries): exactly the SortedAggOp
    // body. Returns Some(row) on a paused boundary emit.
    macro_rules! per_row {
        ($i:expr, $slot:expr) => {{
            let slot = $slot;
            if *group_open && ::nodeagg::agg_sorted_same_group(agg, estate, slot)? {
                ::nodeagg::agg_sorted_accept(agg, estate, slot)?;
                None
            } else if !*group_open {
                ::nodeagg::agg_sorted_group_begin(agg, estate, Some(slot))?;
                ::nodeagg::agg_sorted_group_key(agg, &mut cur_key[..nkeys]);
                *group_open = true;
                None
            } else {
                // Boundary: save the boundary row first (the pull loop's
                // order), then finalize + HAVING + project the group.
                ::nodeagg::agg_sorted_save_pending(agg, estate, slot)?;
                *group_open = false;
                match ::nodeagg::agg_sorted_emit(agg, estate)? {
                    Some(row) => Some(row),
                    None => {
                        // HAVING rejected: start the next group from the
                        // pending boundary tuple (the pull loop's continue).
                        ::nodeagg::agg_sorted_group_begin(agg, estate, None)?;
                        ::nodeagg::agg_sorted_group_key(agg, &mut cur_key[..nkeys]);
                        *group_open = true;
                        None
                    }
                }
            }
        }};
    }
    if demote {
        // Whole-window per-row program (checked transitions, C's detoast/
        // overflow behavior at C's row).
        for i in pos..n {
            ss.set_lane_cursor(i + 1, n);
            if let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(ss, estate, i)? {
                if let Some(row) = per_row!(i, slot) {
                    return Ok(Some(row));
                }
            }
        }
        ss.set_lane_cursor(n, n);
        return Ok(None);
    }
    let mut i = pos;
    while i < n {
        if bitmap_mode {
            // Phase A (staged reads only): extend the open group's run to
            // the next event — a group boundary, a fallback row, or window
            // end. Skipped rows are qual rejections (the bitmap IS the
            // verdict).
            enum Ev {
                Boundary(u32),
                Fallback(u32),
                End,
            }
            let ev = {
                let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                    .expect("fold drive staged the SoA");
                let mut key_vals: [&[::datum::Datum]; SORTED_FOLD_MAX_KEYS] =
                    [&[]; SORTED_FOLD_MAX_KEYS];
                let mut key_nulls: [&[bool]; SORTED_FOLD_MAX_KEYS] =
                    [&[]; SORTED_FOLD_MAX_KEYS];
                for k in 0..nkeys {
                    key_vals[k] = soa.col_values(keys.cols[k].0 as usize);
                    key_nulls[k] = soa.col_isnull(keys.cols[k].0 as usize);
                }
                let mut ev = Ev::End;
                let mut j = i;
                while j < n {
                    if sel[(j / 64) as usize] & (1u64 << (j % 64)) == 0 {
                        j += 1;
                        continue;
                    }
                    if fb[(j / 64) as usize] & (1u64 << (j % 64)) != 0 {
                        ev = Ev::Fallback(j);
                        break;
                    }
                    if !*group_open {
                        ev = Ev::Boundary(j);
                        break;
                    }
                    let same = (0..nkeys).all(|k| {
                        let (cv, cn) = cur_key[k];
                        let jn = key_nulls[k][j as usize];
                        if cn || jn {
                            cn && jn
                        } else {
                            sorted_key_datum_eq(key_vals[k][j as usize], cv, keys.cols[k].1)
                        }
                    });
                    if !same {
                        ev = Ev::Boundary(j);
                        break;
                    }
                    run[(j / 64) as usize] |= 1u64 << (j % 64);
                    run_any = true;
                    j += 1;
                }
                if matches!(ev, Ev::End) {
                    debug_assert_eq!(j, n);
                }
                ev
            };
            // Phase B: fold the accumulated run, then the per-row event.
            flush_run!();
            match ev {
                Ev::End => {
                    i = n;
                }
                Ev::Fallback(j) | Ev::Boundary(j) => {
                    ss.set_lane_cursor(j + 1, n);
                    if let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(ss, estate, j)? {
                        if let Some(row) = per_row!(j, slot) {
                            return Ok(Some(row));
                        }
                    }
                    i = j + 1;
                }
            }
        } else {
            // Per-row-emit mode (residual transitions and/or a requal/
            // scalar-checked qual): every row goes through the scan's
            // per-row emit — the full qual at the per-row path's cadence —
            // and surviving deformed rows join the fold run (residuals per
            // row, the fold-feed discipline); fallback survivors run the
            // full per-row program.
            let j = i;
            ss.set_lane_cursor(j + 1, n);
            let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(ss, estate, j)? else {
                i = j + 1;
                continue;
            };
            let is_fb = fb[(j / 64) as usize] & (1u64 << (j % 64)) != 0;
            if is_fb {
                flush_run!();
                if let Some(row) = per_row!(j, slot) {
                    return Ok(Some(row));
                }
                i = j + 1;
                continue;
            }
            // Deformed survivor: same-group rows fold; boundaries and group
            // prologues delegate per row.
            let same = *group_open && {
                let soa = ::nodeseqscan::seq_scan_batch_soa(ss)
                    .expect("fold drive staged the SoA");
                (0..nkeys).all(|k| {
                    let (cv, cn) = cur_key[k];
                    let jn = soa.col_isnull(keys.cols[k].0 as usize)[j as usize];
                    if cn || jn {
                        cn && jn
                    } else {
                        sorted_key_datum_eq(
                            soa.col_values(keys.cols[k].0 as usize)[j as usize],
                            cv,
                            keys.cols[k].1,
                        )
                    }
                })
            };
            if same {
                run[(j / 64) as usize] |= 1u64 << (j % 64);
                run_any = true;
                if has_resid {
                    ::nodeagg::agg_sorted_accept_resid(agg, estate, slot)?;
                }
            } else {
                flush_run!();
                if let Some(row) = per_row!(j, slot) {
                    return Ok(Some(row));
                }
            }
            i = j + 1;
        }
    }
    flush_run!();
    let _ = run_any;
    ss.set_lane_cursor(n, n);
    Ok(None)
}

// ===========================================================================
// Hash-join pipeline breaker (Phase 2). The join spans two pipelines plus a
// mid-pipeline streaming stage:
//
//   pipeline N   (build): inner scan source → scalar filter/project →
//                         HashJoinBuildSink   (breaker Sink face)
//   pipeline N+1 (probe): outer scan source → scalar filter/project →
//                         JoinProbe (TupleOp) → sink
//
// The build side is the breaker: `accept` = the row-path per-row hash +
// `ExecHashTableInsert` (`nodehash::lane_build_accept` — spill/growth arms
// included), `finish` = the delegated build tail (`finish_build`,
// empty-build early return, `nbatch_outstart`/`dense_on`, phase flip). The
// probe side is NOT a breaker — it streams: one outer row in, 0..K joined
// rows out, with the intra-row expansion position node-resident on the
// HashJoinState (`hj_CurTuple`/`hj_CurDense` — C's own cross-call state), so
// a mid-expansion pause resumes exactly. The phase flag is `hj_JoinState`
// itself (HJ_BUILD_HASHTABLE → HJ_NEED_NEW_OUTER — C's own state machine).
//
// Spill (§8): the build delegates wholesale to the row-path table, so nbatch
// growth happens exactly as the row path's; the lane then checks the FINAL
// nbatch after the completed build and REFUSES the probe when nbatch > 1 —
// before any lane tuple is emitted, so the fallback `exec_hash_join` resumes
// from HJ_NEED_NEW_OUTER over the identical table (postponing outer tuples
// to batch files exactly as if the row path had built it). Refusing on the
// planner's initial estimate alone would be insufficient: the row path grows
// nbatch mid-build (`ExecHashIncreaseNumBatches`), so only the post-build
// value is authoritative — and checking after a fully delegated build is
// byte-safe precisely because the build is bit-equal to the row path's.
//
// Admitted join types: all eight — INNER, LEFT, SEMI, ANTI plus the
// right-fill family RIGHT, FULL, RIGHT_SEMI, RIGHT_ANTI — with
// joinqual/otherqual residuals evaluated scalar-within-lane through the
// row path's exact `eval_probe_qual` (LEFT/FULL/ANTI null-fill emits happen
// inside `lane_probe_next`'s HJ_FILL_OUTER_TUPLE arm, exactly where C emits
// them). The right-fill types (`hj_fill_inner` — RIGHT/FULL/RIGHT_ANTI) add
// a post-exhaustion phase: when the outer source ends, the probe TupleOp
// becomes a SOURCE of never-matched build tuples (C's HJ_FILL_INNER_TUPLES
// via the driver's `source_exhausted` seam; the walk delegates to the
// row path's exact `ExecScanHashTableForUnmatched` port, so the fill
// emission order is C's bucket order for free; the cursor is C's own
// node-resident `hj_CurBucketNo`/`hj_CurTuple`, so a LIMIT pause mid-fill
// resumes exactly). RIGHT_SEMI needs no fill phase — only the has-match
// skip in the probe arm. Refused join shapes (assert-refuse set):
// multi-batch (above), parallel hash, instrumented, subplan/param-bearing
// hash, residual-qual or projection exprs, non-lane-fusible scan children
// on either side.
// ===========================================================================

/// The breaker's `Sink` face (build pipeline endpoint). Holds the join +
/// hash nodes by `&mut` — the driver threads the inner SCAN node, so the
/// breaker spanning other nodes needs no driver rework (sort-breaker shape).
struct HashJoinBuildSink<'a, 'mcx> {
    hj: &'a mut ::nodehashjoin::HashJoinState<'mcx>,
    hs: &'a mut ::nodehash::HashState<'mcx>,
    done: Option<::nodehashjoin::LaneBuildDone>,
}

impl<'mcx> Sink<'mcx> for HashJoinBuildSink<'_, 'mcx> {
    fn accept(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<SinkFeed> {
        ::nodehash::lane_build_accept(self.hs, estate, tuple)?;
        Ok(SinkFeed::NeedMore)
    }

    fn finish(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        self.done = Some(::nodehashjoin::lane_build_finish(self.hj, self.hs, estate)?);
        Ok(())
    }
}

/// Batch-granular feed: the default loop, monomorphized — each staged row
/// runs the same `lane_build_accept` in the same order, with the per-row dyn
/// dispatch, `SinkFeed` matching, and consume-cursor saves elided.
impl<'mcx> BatchSink<'mcx> for HashJoinBuildSink<'_, 'mcx> {}

/// The join probe as a mid-pipeline `TupleOp`: accept stages one outer row
/// (`lane_probe_accept` — ecxt reset + hash/dense key, C's per-outer-row
/// prologue), then the expansion streams each bucket/dense-chain match
/// through the row-path recheck + projection (`lane_probe_next`) into the
/// downstream sink. Expansion position is node-resident on the join state.
struct JoinProbe<'a, 'mcx> {
    hj: &'a mut ::nodehashjoin::HashJoinState<'mcx>,
    hs: &'a mut ::nodehash::HashState<'mcx>,
}

impl<'mcx> JoinProbe<'_, 'mcx> {
    fn emit(
        &mut self,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        while let Some(j) = ::nodehashjoin::lane_probe_next(self.hj, self.hs, estate)? {
            if let SinkFeed::Full = out.accept(j, estate)? {
                return Ok(OpStatus::Paused);
            }
        }
        Ok(OpStatus::NeedInput)
    }
}

impl<'mcx> TupleOp<'mcx> for JoinProbe<'_, 'mcx> {
    fn pending(&self) -> bool {
        ::nodehashjoin::lane_probe_pending(self.hj)
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        ::nodehashjoin::lane_probe_accept(self.hj, self.hs, estate, tuple)?;
        self.emit(out, estate)
    }

    fn resume(
        &mut self,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        let s = self.emit(out, estate)?;
        // A resumed fill scan that just drained is terminal: the driver must
        // not fall through to another source produce — a pulled-past-end
        // heap scan RESTARTS (C never re-pulls a child after NULL).
        if s == OpStatus::NeedInput && ::nodehashjoin::lane_join_finished(self.hj) {
            return Ok(OpStatus::Finished);
        }
        Ok(s)
    }

    /// Outer exhausted: the right-fill types (`hj_fill_inner` —
    /// RIGHT/FULL/RIGHT_ANTI) flip into the unmatched-BUILD fill scan
    /// (C's HJ_FILL_INNER_TUPLES, sequenced exactly where C enters it:
    /// after the probe fully ends) and become a source of null-extended
    /// unmatched inner tuples into the same sink. The prep is idempotent
    /// (no-op unless the join sits at HJ_NEED_NEW_OUTER), the fill cursor
    /// is C's own node-resident `hj_CurBucketNo`/`hj_CurTuple`, and a
    /// mid-fill pause (`Paused`) resumes through the ordinary
    /// `pending()`/`resume()` protocol. Non-fill types emit nothing here.
    fn source_exhausted(
        &mut self,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        ::nodehashjoin::lane_fill_inner_prep(self.hj);
        Ok(match self.emit(out, estate)? {
            // The fill scan is drained (or there never was one): nothing
            // further will ever be produced.
            OpStatus::NeedInput => OpStatus::Finished,
            s => s,
        })
    }
}

/// Build-pipeline driver, generic over the inner scan: table create
/// (delegated, bit-equal to the row path's), drain the scan pipeline into
/// the breaker sink, delegated finish. Returns the post-build verdict inputs
/// (empty / final nbatch).
fn join_build_feed<'mcx, S, O>(
    hj: &mut ::nodehashjoin::HashJoinState<'mcx>,
    hs: &mut ::nodehash::HashState<'mcx>,
    scan: &mut S::Node,
    mut src: S,
    mut op: O,
    estate: &mut EStateData<'mcx>,
) -> PgResult<::nodehashjoin::LaneBuildDone>
where
    S: Source<'mcx>,
    O: Operator<'mcx, Node = S::Node>,
{
    ::nodehashjoin::lane_build_begin(hj, hs, estate)?;
    let mut sink = HashJoinBuildSink { hj, hs, done: None };
    drain_pipeline(scan, &mut src, &mut op, &mut sink, estate)?;
    Ok(sink.done.expect("build sink finished"))
}

/// Dispatch the build feed over the admitted inner-scan child types.
fn join_build_dispatch<'mcx>(
    hj: &mut ::nodehashjoin::HashJoinState<'mcx>,
    hs: &mut ::nodehash::HashState<'mcx>,
    child: &mut crate::procnode::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<::nodehashjoin::LaneBuildDone> {
    // One OWNED tick per lane-owned join build event (the gate's join floor
    // counts builds, not calls) — bare joins and agg-over-join compositions
    // alike.
    stats::tick_owned(ShapeClass::Join);
    match child {
        crate::procnode::PlanStateNode::SeqScan(ss) => {
            arm_scan_staging(
                ss,
                estate,
                ScanFeedShape::RowFeed { ctx: "join build feed", stitch: true },
            )?;
            join_build_feed(hj, hs, ss, SeqScanSource, SeqScanFilterProject, estate)
        }
        crate::procnode::PlanStateNode::IndexScan(is) => {
            join_build_feed(hj, hs, is, IndexScanSource, IndexScanEmit, estate)
        }
        crate::procnode::PlanStateNode::IndexOnlyScan(ios) => {
            join_build_feed(hj, hs, &mut **ios, IndexOnlyScanSource, IndexOnlyScanEmit, estate)
        }
        crate::procnode::PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            if !b.scan.initialized {
                crate::procnode::bitmap_table_scan_setup_dispatch(b, estate)?;
            }
            join_build_feed(hj, hs, &mut b.scan, BitmapHeapScanSource, BitmapHeapScanEmit, estate)
        }
        _ => unreachable!("memoized join verdict admitted a non-scan build child"),
    }
}

/// Probe-pipeline drain (composition): outer scan → filter/project →
/// JoinProbe → the downstream breaker sink (the agg build), to exhaustion.
fn join_probe_drain_dispatch<'mcx>(
    hj: &mut ::nodehashjoin::HashJoinState<'mcx>,
    hs: &mut ::nodehash::HashState<'mcx>,
    outer: &mut crate::procnode::PlanStateNode<'mcx>,
    sink: &mut dyn Sink<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mut probe = JoinProbe { hj, hs };
    match outer {
        crate::procnode::PlanStateNode::SeqScan(ss) => {
            arm_scan_staging(
                ss,
                estate,
                ScanFeedShape::RowFeed { ctx: "join probe drain", stitch: true },
            )?;
            drain_pipeline_chain(
                ss,
                &mut SeqScanSource,
                &mut SeqScanFilterProject,
                &mut probe,
                sink,
                estate,
            )
        }
        crate::procnode::PlanStateNode::IndexScan(is) => drain_pipeline_chain(
            is,
            &mut IndexScanSource,
            &mut IndexScanEmit,
            &mut probe,
            sink,
            estate,
        ),
        crate::procnode::PlanStateNode::IndexOnlyScan(ios) => drain_pipeline_chain(
            &mut **ios,
            &mut IndexOnlyScanSource,
            &mut IndexOnlyScanEmit,
            &mut probe,
            sink,
            estate,
        ),
        crate::procnode::PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            if !b.scan.initialized {
                crate::procnode::bitmap_table_scan_setup_dispatch(b, estate)?;
            }
            drain_pipeline_chain(
                &mut b.scan,
                &mut BitmapHeapScanSource,
                &mut BitmapHeapScanEmit,
                &mut probe,
                sink,
                estate,
            )
        }
        _ => unreachable!("memoized join verdict admitted a non-scan outer child"),
    }
}

/// Probe-pipeline pull (bare join): one PG pull's worth through the chain
/// into the root adapter — exercising the mid-expansion pause/resume.
fn join_probe_pull_dispatch<'mcx>(
    hj: &mut ::nodehashjoin::HashJoinState<'mcx>,
    hs: &mut ::nodehash::HashState<'mcx>,
    outer: &mut crate::procnode::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    let mut probe = JoinProbe { hj, hs };
    let mut root = RootAdapter::new(None);
    match outer {
        crate::procnode::PlanStateNode::SeqScan(ss) => {
            // Per-pull entry: the bitmap arm early-returns once armed (one
            // load+test), and the first pull arms BEFORE any batch is
            // staged, so a staged batch always matches its bitmap. No
            // stitch: pull-one-tuple pipelines keep the AOT bitmap tier
            // (stitched segments exist only on drain pipelines).
            arm_scan_staging(
                ss,
                estate,
                ScanFeedShape::RowFeed { ctx: "join probe pull", stitch: false },
            )?;
            pull_step_chain(
                ss,
                &mut SeqScanSource,
                &mut SeqScanFilterProject,
                &mut probe,
                &mut root,
                estate,
            )
        }
        crate::procnode::PlanStateNode::IndexScan(is) => pull_step_chain(
            is,
            &mut IndexScanSource,
            &mut IndexScanEmit,
            &mut probe,
            &mut root,
            estate,
        ),
        crate::procnode::PlanStateNode::IndexOnlyScan(ios) => pull_step_chain(
            &mut **ios,
            &mut IndexOnlyScanSource,
            &mut IndexOnlyScanEmit,
            &mut probe,
            &mut root,
            estate,
        ),
        crate::procnode::PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            if !b.scan.initialized {
                crate::procnode::bitmap_table_scan_setup_dispatch(b, estate)?;
            }
            pull_step_chain(
                &mut b.scan,
                &mut BitmapHeapScanSource,
                &mut BitmapHeapScanEmit,
                &mut probe,
                &mut root,
                estate,
            )
        }
        _ => unreachable!("memoized join verdict admitted a non-scan outer child"),
    }
}

/// Structural refuse-set for the lane hash join, memoized on the node at
/// first evaluation (verdict stability: a lane-owned join must stay
/// lane-owned — `lane_join_untouched` in the verdict guarantees the row path
/// never drove this node before the lane, and memoization guarantees the
/// lane drives it ever after). Join side: `lane_join_admissible`
/// (all eight join types, subplan/param-free residual quals admitted,
/// uninstrumented, subplan/param-free hash + projection exprs) + serial hash
/// + subplan/param-free build hash. Child side: the Phase-1 scan refuse-sets
/// on BOTH children. The caller re-checks the dynamic EPQ/direction gates
/// per call.
fn hash_join_lane_fusible<'mcx>(
    hj: &mut crate::procnode::HashJoinNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if let Some(v) = hj.lane_fusible {
        return Ok(v);
    }
    // Engagement accounting for the structural verdict ticks exactly here —
    // once per memoized decision (a child-scan refusal's specific reason is
    // ticked under the child's class inside its fusible gate). OWNED ticks
    // for the join class count build EVENTS, in `join_build_dispatch`.
    let refuse = hash_join_refuse_reason(hj, estate)?;
    if let Some(r) = refuse {
        stats::tick_refused(ShapeClass::Join, r);
    }
    let v = refuse.is_none();
    hj.lane_fusible = Some(v);
    Ok(v)
}

/// `None` = admitted; `Some(reason)` = refused.
fn hash_join_refuse_reason<'mcx>(
    hj: &mut crate::procnode::HashJoinNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<RefuseReason>> {
    let crate::procnode::HashJoinNode { state, outer, hash, .. } = hj;
    let crate::procnode::HashSubNode { state: hstate, child } = &mut **hash;
    // Instrumented, subplan/param-bearing join exprs or projection (all
    // eight join types + residuals are admitted since lane-v2-jointypes /
    // lane-v2-rightjoin) — plus a node the row path already drove (verdict
    // stability demands whole-life ownership).
    if !::nodehashjoin::lane_join_admissible(state)
        || !::nodehashjoin::lane_join_untouched(state, hstate)
    {
        return Ok(Some(RefuseReason::JoinShape));
    }
    if hstate.parallel_state().is_some() || hstate.is_parallel_aware() {
        return Ok(Some(RefuseReason::ParallelGate));
    }
    if !::nodehash::lane_build_hash_admissible(hstate) {
        return Ok(Some(RefuseReason::SubplanParam));
    }
    if let Some(r) = scan_child_fusible(outer, estate)? {
        return Ok(Some(r));
    }
    scan_child_fusible(child, estate)
}

/// Try to let the lane own a bare `HashJoin` (no lane consumer above): build
/// pipeline once (lazily, phase = the node's own HJ_BUILD_HASHTABLE), then
/// one joined tuple per PG pull through the probe chain. `None` = refused
/// (caller runs the unchanged `exec_hash_join` — byte-safe even after a
/// lane-delegated build, which leaves exactly the row path's post-build node
/// state). The dispatch hook gates this on the legacy fused probe drive NOT
/// engaging (admission economics: never preempt the faster existing path).
#[inline]
pub fn try_own_hash_join<'mcx>(
    hj: &mut crate::procnode::HashJoinNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Admission economics (design §4): the legacy fused probe drive already
    // owns this shape better than the v2 pipeline — never preempt the
    // measured-faster path. Per-PULL tick cadence (the dispatch arm resolves
    // the probe mode before offering the join to the lane). Parallel Hash
    // ticks its own gate.
    match hj.probe_batch.mode() {
        crate::procnode::ProbeBatchMode::Off => {}
        crate::procnode::ProbeBatchMode::Parallel => {
            stats::tick_refused(ShapeClass::Join, RefuseReason::ParallelGate);
            return Ok(None);
        }
        crate::procnode::ProbeBatchMode::Unknown | crate::procnode::ProbeBatchMode::On => {
            stats::tick_refused(ShapeClass::Join, RefuseReason::AdmissionEconomicsFusedDrive);
            return Ok(None);
        }
    }
    // Dynamic per-call gates (mirrors the sort breaker).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::Join, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(ShapeClass::Join, RefuseReason::Backward);
        return Ok(None);
    }
    if !hash_join_lane_fusible(hj, estate)? {
        return Ok(None);
    }
    // C's CHECK_FOR_INTERRUPTS at ExecHashJoin entry.
    ::postgres_seams::check_for_interrupts::call()?;
    let crate::procnode::HashJoinNode { state, outer, hash, .. } = hj;
    let crate::procnode::HashSubNode { state: hstate, child } = &mut **hash;
    if ::nodehashjoin::lane_join_phase(state, hstate) == ::nodehashjoin::LaneJoinPhase::Build {
        let done = join_build_dispatch(state, hstate, child, estate)?;
        if done.empty {
            // C's empty-build early return: no output, outer never pulled.
            return Ok(Some(None));
        }
        if done.nbatch > 1 {
            // Spill refuse, before any lane tuple is emitted: the fallback
            // row path resumes from HJ_NEED_NEW_OUTER over the same table.
            stats::tick_refused(ShapeClass::Join, RefuseReason::MultiBatch);
            return Ok(None);
        }
        // Bloom pushdown reclaim: arm the lane probe's prefilter, only
        // where the legacy path's own push seats would (SeqScan outer
        // drives — the fused probe drive and the bare `seq_scan_set_bloom`
        // seat are both SeqScan-only), so lane-vs-legacy comparisons stay
        // apples-to-apples. The arm re-applies the row path's exact push
        // gate (never fill_outer, never dense, hash cover, single batch,
        // density <= 0.25).
        if let crate::procnode::PlanStateNode::SeqScan(_) = &**outer {
            ::nodehashjoin::lane_probe_filter_arm(state, hstate);
        }
    } else {
        match ::nodehashjoin::lane_join_phase(state, hstate) {
            ::nodehashjoin::LaneJoinPhase::EmptyDone => return Ok(Some(None)),
            ::nodehashjoin::LaneJoinPhase::Probe => {
                if hstate.table.as_ref().expect("probe phase has a table").nbatch > 1 {
                    stats::tick_refused(ShapeClass::Join, RefuseReason::MultiBatch);
                    return Ok(None);
                }
            }
            ::nodehashjoin::LaneJoinPhase::Build => unreachable!("handled above"),
        }
    }
    Ok(Some(join_probe_pull_dispatch(state, hstate, outer, estate)?))
}

// ===========================================================================
// NestLoop hosting (the deferred §4 bundle). The join is a mid-pipeline
// streaming `TupleOp` — NOT a breaker: one outer row in, 0..K joined rows
// out.
//
//   pipeline: outer scan source → scalar filter/project → NestLoopProbe
//             (TupleOp) → sink (RootAdapter, or the hash-agg breaker)
//
// Per accepted outer row the op runs C's need-new-outer arm
// (`nodenestloop::lane_accept_outer`): bind the outer tuple, assign the
// join's exec params (nestParams → PARAM_EXEC slots), and RESCAN the inner
// child; the expansion then streams each inner row through the joinqual /
// otherqual / projection (`lane_probe_next` = `exec_nest_loop`'s own loop
// body, LEFT/SEMI/ANTI arms included). The INNER child stays a Volcano
// child, driven per-row through the same `NestLoopChild` calls the row path
// uses (scalar-within-lane, per the design's allowance) — so exec-param-
// driven runtime keys on an inner index scan are evaluated in
// `exec_rescan_index_scan`'s preamble exactly as C's ExecReScan path does,
// AUTOMATICALLY. The Phase-1 lane scan gates therefore KEEP refusing runtime
// keys (`iss_Runtime`/`ioss_Runtime`) for LANE-OWNED scans: that relaxation
// belongs to the inner-as-lane-pipeline follow-up, where the lane would have
// to drive the rescan preamble itself. Expansion position across the Volcano
// pull boundary is the node's own `nl_NeedNewOuter`/`nl_MatchedOuter` — C's
// cross-call state; no new fields.
//
// Admission economics (design §4): NestLoop per-tuple in Volcano is already
// cheap; the lane's value is OWNERSHIP CONTINUITY — the outer side stays a
// lane pipeline feeding breakers above. The hooks engage (a) under the
// hash-agg breaker (`try_own_agg_over_nest_loop` — a lane consumer above,
// no fused competitor exists for this shape) and (b) bare
// (`try_own_nest_loop`) where the outer is a lane-fusible scan the join
// pipeline then owns — the bare hash-join precedent; there is no legacy
// fused NestLoop drive to preempt. Refused (assert-refuse set):
// instrumented, subplan/param-bearing joinqual/otherqual/projection,
// row-path-touched nodes (verdict stability), non-lane-fusible outer
// children, EPQ, non-forward. The inner child is unconstrained — it runs
// the identical Volcano calls at the identical points either way.
// ===========================================================================

/// The NestLoop join as a mid-pipeline `TupleOp`: accept stages one outer
/// row (param assignment + inner rescan — C's per-outer-row prologue), then
/// the expansion streams the inner drain through the row-path joinqual /
/// projection arms into the downstream sink. Expansion position is
/// node-resident on the join state (`nl_NeedNewOuter`).
struct NestLoopProbe<'a, 'mcx> {
    nl: &'a mut ::nodenestloop::NestLoopState<'mcx>,
    inner: &'a mut crate::procnode::PlanStateNode<'mcx>,
}

impl<'mcx> NestLoopProbe<'_, 'mcx> {
    fn emit(
        &mut self,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        while let Some(j) = ::nodenestloop::lane_probe_next(self.nl, self.inner, estate)? {
            if let SinkFeed::Full = out.accept(j, estate)? {
                return Ok(OpStatus::Paused);
            }
        }
        Ok(OpStatus::NeedInput)
    }
}

impl<'mcx> TupleOp<'mcx> for NestLoopProbe<'_, 'mcx> {
    fn pending(&self) -> bool {
        ::nodenestloop::lane_probe_pending(self.nl)
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        // One OWNED tick per accepted outer row — the unit the lane owns
        // (bind params -> rescan the inner -> drain the expansion).
        stats::tick_owned(ShapeClass::NestLoop);
        ::nodenestloop::lane_accept_outer(self.nl, self.inner, estate, tuple)?;
        self.emit(out, estate)
    }

    fn resume(
        &mut self,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        self.emit(out, estate)
    }
}

/// One PG pull through outer scan → filter/project → `top` → root adapter,
/// dispatched over the admitted lane-scan child types (join_probe dispatch
/// shape, generic over the mid-pipeline op).
fn scan_chain_pull_dispatch<'mcx>(
    outer: &mut crate::procnode::PlanStateNode<'mcx>,
    top: &mut dyn TupleOp<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    let mut root = RootAdapter::new(None);
    match outer {
        crate::procnode::PlanStateNode::SeqScan(ss) => pull_step_chain(
            ss,
            &mut SeqScanSource,
            &mut SeqScanFilterProject,
            top,
            &mut root,
            estate,
        ),
        crate::procnode::PlanStateNode::IndexScan(is) => pull_step_chain(
            is,
            &mut IndexScanSource,
            &mut IndexScanEmit,
            top,
            &mut root,
            estate,
        ),
        crate::procnode::PlanStateNode::IndexOnlyScan(ios) => pull_step_chain(
            &mut **ios,
            &mut IndexOnlyScanSource,
            &mut IndexOnlyScanEmit,
            top,
            &mut root,
            estate,
        ),
        crate::procnode::PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            if !b.scan.initialized {
                crate::procnode::bitmap_table_scan_setup_dispatch(b, estate)?;
            }
            pull_step_chain(
                &mut b.scan,
                &mut BitmapHeapScanSource,
                &mut BitmapHeapScanEmit,
                top,
                &mut root,
                estate,
            )
        }
        _ => unreachable!("memoized lane verdict admitted a non-scan outer child"),
    }
}

/// Full drain of outer scan → filter/project → `top` → breaker sink, same
/// dispatch as `scan_chain_pull_dispatch`.
fn scan_chain_drain_dispatch<'mcx>(
    outer: &mut crate::procnode::PlanStateNode<'mcx>,
    top: &mut dyn TupleOp<'mcx>,
    sink: &mut dyn Sink<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    match outer {
        crate::procnode::PlanStateNode::SeqScan(ss) => drain_pipeline_chain(
            ss,
            &mut SeqScanSource,
            &mut SeqScanFilterProject,
            top,
            sink,
            estate,
        ),
        crate::procnode::PlanStateNode::IndexScan(is) => drain_pipeline_chain(
            is,
            &mut IndexScanSource,
            &mut IndexScanEmit,
            top,
            sink,
            estate,
        ),
        crate::procnode::PlanStateNode::IndexOnlyScan(ios) => drain_pipeline_chain(
            &mut **ios,
            &mut IndexOnlyScanSource,
            &mut IndexOnlyScanEmit,
            top,
            sink,
            estate,
        ),
        crate::procnode::PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            if !b.scan.initialized {
                crate::procnode::bitmap_table_scan_setup_dispatch(b, estate)?;
            }
            drain_pipeline_chain(
                &mut b.scan,
                &mut BitmapHeapScanSource,
                &mut BitmapHeapScanEmit,
                top,
                sink,
                estate,
            )
        }
        _ => unreachable!("memoized lane verdict admitted a non-scan outer child"),
    }
}

/// Structural refuse-set for the lane NestLoop, memoized on the node at
/// first evaluation (verdict stability: a lane-owned join must stay
/// lane-owned — `lane_nest_loop_untouched` in the verdict guarantees the row
/// path never drove this node before the lane, and memoization guarantees
/// the lane drives it ever after). Join side: `lane_nest_loop_admissible`
/// (all four ported join types; uninstrumented; subplan/param-free quals +
/// projection). Outer side: the Phase-1 scan refuse-sets. The INNER side is
/// deliberately unconstrained — it stays a Volcano child driven through the
/// identical `NestLoopChild` calls. The caller re-checks the dynamic
/// EPQ/direction gates per call.
fn nest_loop_lane_fusible<'mcx>(
    nl: &mut crate::procnode::NestLoopNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if let Some(v) = nl.lane_fusible {
        return Ok(v);
    }
    // Engagement accounting for the structural verdict ticks exactly here —
    // once per memoized decision (a child-scan refusal's specific reason is
    // ticked under the child's class inside its fusible gate). OWNED ticks
    // for the nestloop class count accepted OUTER ROWS, in
    // `NestLoopProbe::accept`.
    let refuse = nest_loop_refuse_reason(nl, estate)?;
    if let Some(r) = refuse {
        stats::tick_refused(ShapeClass::NestLoop, r);
    }
    let v = refuse.is_none();
    nl.lane_fusible = Some(v);
    Ok(v)
}

/// `None` = admitted; `Some(reason)` = refused.
fn nest_loop_refuse_reason<'mcx>(
    nl: &mut crate::procnode::NestLoopNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<RefuseReason>> {
    let crate::procnode::NestLoopNode { state, outer, .. } = nl;
    // Instrumented, subplan/param-bearing joinqual/otherqual/projection —
    // plus a node the row path already drove (verdict stability demands
    // whole-life ownership).
    if !::nodenestloop::lane_nest_loop_admissible(state)
        || !::nodenestloop::lane_nest_loop_untouched(state, estate)
    {
        return Ok(Some(RefuseReason::JoinShape));
    }
    scan_child_fusible(outer, estate)
}

/// Try to let the lane own a bare `NestLoop` (no lane consumer above): one
/// joined tuple per PG pull through the chain, the mid-inner-drain position
/// riding C's own `nl_NeedNewOuter` across the pull boundary. `None` =
/// refused (caller runs the unchanged `exec_nest_loop` — byte-safe: an
/// untouched-only verdict means the row path owns the node's whole life).
#[inline]
pub fn try_own_nest_loop<'mcx>(
    nl: &mut crate::procnode::NestLoopNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic per-call gates (mirrors the sort/hash-join breakers).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::NestLoop, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(ShapeClass::NestLoop, RefuseReason::Backward);
        return Ok(None);
    }
    if !nest_loop_lane_fusible(nl, estate)? {
        return Ok(None);
    }
    // C's CHECK_FOR_INTERRUPTS at ExecNestLoop entry.
    ::postgres_seams::check_for_interrupts::call()?;
    let crate::procnode::NestLoopNode { state, outer, inner, .. } = nl;
    let mut probe = NestLoopProbe { nl: state, inner };
    Ok(Some(scan_chain_pull_dispatch(outer, &mut probe, estate)?))
}

/// Try to let the lane own `Agg(hashed) → NestLoop → lane outer scan` (the
/// inner stays Volcano): two pipelines on one breaker node —
///
///   1. build: outer scan → filter/project → NestLoopProbe → HashAggBuildSink
///   2. emit:  HashAggSource → HashAggEmit → RootAdapter (one group per pull)
///
/// `None` = refused (caller falls to the per-tuple `exec_agg` over
/// `exec_nest_loop`, byte-identically).
#[inline]
pub fn try_own_agg_over_nest_loop<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    nl: &mut crate::procnode::NestLoopNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic per-call gates, ticked under the nestloop class (the
    // composition's feed pipeline hangs off the join's drive).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::NestLoop, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(ShapeClass::NestLoop, RefuseReason::Backward);
        return Ok(None);
    }
    if !::nodeagg::agg_hash_breaker_admissible(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AggNotDrainable);
        return Ok(None);
    }
    if !nest_loop_lane_fusible(nl, estate)? {
        return Ok(None);
    }
    // exec_agg's top-of-call guard: a drained agg stays drained.
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    // Build phase (once, lazily; a rescan rebuild clears `table_filled` and
    // re-enters — the whole-NestLoop rescan resets `nl_NeedNewOuter` and the
    // outer scan's staged cursor, so the feed restarts coherently).
    if !::nodeagg::agg_hash_table_filled(agg) {
        // One OWNED tick per lane-owned agg build event (here the build is
        // fed by the NestLoop expansion drain).
        stats::tick_owned(ShapeClass::AggBuild);
        let crate::procnode::NestLoopNode { state, outer, inner, .. } = nl;
        let mut probe = NestLoopProbe { nl: state, inner };
        let mut sink = HashAggBuildSink { agg: &mut *agg };
        scan_chain_drain_dispatch(outer, &mut probe, &mut sink, estate)?;
    }
    // Emit phase (every call): one qual-passing group per PG pull, in C's
    // retrieve order.
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step(agg, &mut HashAggSource, &mut HashAggEmit, &mut root, estate)?))
}

// Staged joined-row fold feed (the agg-over-join composition's batched build
// feed). The join probe streams one joined row at a time — there is no page
// batch and no SoA deform on the agg's outer side — so the composition's agg
// breaker previously fed per-row (`HashAggBuildSink`), leaving the lanefold
// kernels disengaged. This sink stages joined rows into `LaneCols`-compatible
// arrays and folds the admitted transitions per staged batch (~the page-batch
// row cap) via the shared fold tail (`agg_fold_staged`), in two modes:
//
// * UNGUARDED plans (no data-level Guard — the common case): the group probe
//   + residual transitions run per row AT ARRIVAL against the incoming joined
//   slot (exactly the per-row sink's own call), snapshotting the pergroup;
//   only the fold lanes (`plan.cols` — always byval int-family by classify
//   construction) are staged, and the flush is just the whole-batch fold.
//   No replay slot, no datum copies: strictly the per-row sink minus the
//   admitted transitions' interpreted per-row steps. This mirrors the seqscan
//   fold feed's probe-then-fold split (residuals per-row inside the batch,
//   commutative fold after) — bit-identical by the lanefold contract.
//
// * GUARDED plans (int2/int4 OpExpr admissions carrying a Guard interval):
//   the guard must be re-proven BEFORE any probe/transition runs, and a
//   Demote must run the WHOLE batch through the checked per-row program — so
//   nothing may run at arrival. The sink stages every build-relevant column
//   (fold lanes + group keys + residual inputs — exactly the `colnos_needed`
//   set the hashagg spill projection keeps), and per batch: `check_guards`
//   (data-scan tier — join output has no zone map), Demote → replay every
//   staged row through `agg_hash_build_accept` (raises C's error at C's
//   row), else replay → probe/residual per row → fold. The replay slot
//   presents the same needed-column values in the same row order the per-row
//   sink would (unneeded columns NULL — the spill projection's own
//   treatment), so probe sequence, spill decisions, residual transitions,
//   and error rows are identical.
//
// Memory: the lanes are fixed-capacity (STAGE_ROWS), reused across batches;
// by-ref staged values on the guarded path (e.g. text group keys — they may
// point into per-tuple memory the probe resets row to row) are datum-copied
// into a dedicated bump context that is reset after every staged batch —
// per-batch, fixed-size, no unbounded growth.
// ===========================================================================

/// Staging window for the joined-row fold feed: the page-batch row cap, so a
/// staged join batch matches the seqscan fold feed's batch magnitude (and the
/// guard bitmask reuses `SOA_BM_WORDS`).
const STAGE_ROWS: usize = ::exectuples::SOA_MAX_ROWS;

/// `LaneCols` over the staged joined-row window: per-column value/isnull
/// lanes indexed by the join output's 0-based attno. Only the needed columns
/// are populated; the fold reads only `plan.cols`, a subset.
struct StagedLanes {
    values: Vec<Vec<::datum::Datum>>,
    isnull: Vec<Vec<bool>>,
}

impl ::lanefold::LaneCols for StagedLanes {
    fn col_values(&self, c: usize) -> &[::datum::Datum] {
        &self.values[c]
    }

    fn col_isnull(&self, c: usize) -> &[bool] {
        &self.isnull[c]
    }
}

/// Staged-feed mode (see the section comment and `staged_feed_shape`).
#[derive(Clone, Copy, PartialEq, Eq)]
enum StagedMode {
    /// Unguarded, arrival probe: only the fold lanes stage; the group probe +
    /// residual transitions run per row at accept.
    Arrival,
    /// Guarded: full needed-column staging with the per-batch guard proof and
    /// the Demote whole-batch per-row replay.
    Guarded,
    /// K2 deferred batched probe (design §3a): full needed-column staging;
    /// per batch — one tight batched-hash loop over the staged grouping-key
    /// lane, then the in-order probe through the same C-ported tuplehash
    /// lookup (bit-identical hashes → identical table layout / iteration /
    /// output order), then the whole-batch fold. Replaces the per-row
    /// expr-program hash+eq walk and per-row slot/context churn. Admitted for
    /// unguarded plans with NO residual transitions over a single
    /// kernel-hostable (int4/int8/text) grouping key.
    K2 {
        /// The grouping key's 0-based colno in the join output.
        key_col: u16,
    },
    /// Packed multi-key deferred batched probe (the scan multikey feed's
    /// slot-stream analog): full needed-column staging; per batch — pack the
    /// staged grouping-key lanes into the armed compact table's ≤16-byte key
    /// image (Int shift/mask, numeric keypack, raw-bytes text through the
    /// build-lifetime intern table), one batched compact-table probe, then
    /// the whole-batch fold. Admitted for unguarded plans with NO residual
    /// transitions over 2..N packable keys (`staged_mk_admit` — the compact
    /// table is ARMED as a side effect). Inadmissible values demote at
    /// runtime (NULL keys on a non-nullable image, unpackable numerics,
    /// backstop migration): the compact groups migrate into the C tuplehash
    /// and the batch (and every later one) replays per-row — byte-safe.
    Mk,
}

/// The composition breaker's fold-armed `Sink` face (three modes, see
/// `StagedMode`). `finish` flushes the tail window and runs the delegated
/// build finalize.
struct StagedFoldAggSink<'a, 'mcx> {
    agg: &'a mut ::nodeagg::AggStateData<'mcx>,
    mode: StagedMode,
    /// Fold lanes (`plan.cols`) + their arrival deform bound — the unguarded
    /// mode's whole staging set (byval by classify construction).
    fold_cols: Vec<u16>,
    fold_bound: i32,
    /// Replay slot (virtual, the join output's tupledesc): guarded mode
    /// re-presents each staged row here for the probe/residual/demote
    /// machinery. Unset in unguarded mode.
    stage_slot: Option<ExecSlotId>,
    natts: usize,
    /// Guarded-mode deform bound for the incoming joined slot
    /// (`max_colno_needed`).
    max_colno: i32,
    /// Guarded mode: 0-based attnos of the needed columns (`colnos_needed`),
    /// with each column's attlen for the by-ref datum copy (attbyval columns
    /// skip the copy). Empty in unguarded mode (only fold lanes stage).
    needed: Vec<(u16, i16, bool)>,
    lanes: StagedLanes,
    nstaged: usize,
    /// Per-batch arena for by-ref staged values (guarded/K2 modes); reset
    /// after every flush.
    stage_cxt: Option<::mcx::MemoryContext>,
    idxs: Vec<u32>,
    groups: Vec<core::ptr::NonNull<::execexpr::AggPerGroup>>,
    /// K2 scratch: the batch's grouping-key hashes (batched hash pre-pass).
    hashes: Vec<u32>,
    /// Mk mode's armed shape + scratch (`None` in every other mode).
    mk: Option<StagedMk>,
}

/// Mk-mode state: the armed packed-key layout plus the reused packing
/// scratch, and the one-way demote flag (after a runtime demote the compact
/// table has migrated into the C tuplehash — every later batch replays
/// per-row through the arrival probe, byte-identically).
struct StagedMk {
    shape: ::nodeagg::MkShape,
    demoted: bool,
    packbuf: Vec<u128>,
    keys1: Vec<i64>,
    keys2: Vec<[u64; 2]>,
}

/// Staged-feed admission inputs for the composition. `None` = the composition
/// keeps the per-row sink (no fold plan, or the join output does not line up
/// with the agg's outer shape — defensive, they are the same tlist by
/// construction).
struct StagedFeedShape {
    mode: StagedMode,
    fold_cols: Vec<u16>,
    fold_bound: i32,
    /// Guarded/K2/Mk modes only (empty in arrival mode): each needed
    /// column's 0-based attno, attlen, attbyval.
    needed: Vec<(u16, i16, bool)>,
    max_colno: i32,
    natts: usize,
    /// Mk mode's armed packed-key layout (`None` in every other mode).
    mk: Option<::nodeagg::MkShape>,
}

/// K2 deferred-probe kill-switch: on by default under the lane;
/// `PGRUST_LANE_V2_K2=0`/`off` forces the arrival probe (A/B tooling — both
/// modes are byte-identical).
fn k2_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_LANE_V2_K2").as_deref(), Ok("0") | Ok("off"))
    })
}

/// Slot-stream multi-key kill switch: on by default under the lane;
/// `PGRUST_LANE_V2_MKSTREAM=0`/`off` forces the arrival probe for the staged
/// join/gather feeds' multi-key shapes (A/B tooling — byte-identical up to
/// the group-order relaxation). The scan-feed switch
/// (`PGRUST_LANE_V2_MULTIKEY`) gates this arm too (shared machinery).
fn mkstream_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_LANE_V2_MKSTREAM").as_deref(), Ok("0") | Ok("off"))
    })
}

/// The slot-stream multi-key admission (`scan_mk_shape`'s analog for the
/// staged join/gather feeds), decided once per build — the compact table is
/// ARMED as a side effect. Caller checked: unguarded plan, no residual
/// transitions, no single-key kernel probe. This adds:
///   * 2..N grouping keys, every one a staged needed column;
///   * packable kinds — Int class / numeric (keypack canonical form, gated
///     per value at flush) / at most ONE raw-bytes text component, hosted
///     through the compact table's build-lifetime intern table (slot streams
///     carry raw varlenas — no dict codes — so the feed interns per row; ids
///     are stable for the whole stream and bounded by the backstop's memory
///     check, which counts the intern arena);
///   * the packing admission + table arm (`agg_hash_compact_try_arm_mk`) —
///     first WITH the null-bitmap byte (slot streams carry no no-NULLs
///     proof), and when that busts the 16-byte image budget (Q19's
///     int8+numeric4+intern4 = 16), WITHOUT it plus `flush_mk`'s runtime
///     NULL-demote pre-check (a NULL grouping key migrates to the C table —
///     byte-safe, never packed wrong).
/// `None` = keep the arrival probe (byte-identical); refuse reasons ticked
/// per the scan feed's taxonomy.
fn staged_mk_admit<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    natts: usize,
    needed: &[(u16, i16, bool)],
) -> Option<::nodeagg::MkShape> {
    if !multikey_enabled() || !mkstream_enabled() {
        return None;
    }
    let key_cols = ::nodeagg::agg_hash_key_cols(agg);
    if key_cols.len() < 2 {
        return None;
    }
    let refused = |r: RefuseReason| {
        stats::tick_refused(ShapeClass::AggBuild, r);
        None
    };
    // Mirror `scan_mk_shape`'s vguard belt (the staged feeds admit no
    // varlena fold lanes, so vguards should be empty on unguarded plans).
    if ::nodeagg::agg_lanefold_plan(agg).is_none_or(|plan| !plan.vguards.is_empty()) {
        return refused(RefuseReason::MultiKeyShape);
    }
    // Every key must be a staged needed column (it always is — the spill
    // projection keeps grouping columns); structural gate.
    for &(att, _) in &key_cols {
        if att as usize >= natts || !needed.iter().any(|&(c, _, _)| c == att) {
            return refused(RefuseReason::MultiKeyShape);
        }
    }
    // Kind census: at most one raw-bytes text component (one intern table).
    let mut dict_att = None;
    for &(att, kind) in &key_cols {
        match kind {
            ::nodeagg::GroupKeyKind::Int { .. } | ::nodeagg::GroupKeyKind::Numeric => {}
            ::nodeagg::GroupKeyKind::TextRaw => {
                if dict_att.is_some() {
                    return refused(RefuseReason::MultiKeyShape);
                }
                dict_att = Some(att);
            }
            ::nodeagg::GroupKeyKind::Other => return refused(RefuseReason::MultiKeyShape),
        }
    }
    // Arm: nullable first (NULL keys ride the bitmap — no demote); a budget
    // refusal retries without the null byte, taking the runtime NULL-demote
    // pre-check instead.
    for nullable in [true, false] {
        match ::nodeagg::agg_hash_compact_try_arm_mk(agg, nullable, dict_att) {
            ::nodeagg::CompactArm::Armed => {
                return Some(
                    ::nodeagg::agg_hash_compact_mk_shape(agg).expect("armed multi-key table"),
                );
            }
            ::nodeagg::CompactArm::KeyKind => continue,
            ::nodeagg::CompactArm::SpillRisk => {
                return refused(RefuseReason::CompactSpillRisk)
            }
            ::nodeagg::CompactArm::Off => return None,
        }
    }
    refused(RefuseReason::MultiKeyShape)
}

fn staged_feed_shape<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    join_result_slot: ExecSlotId,
    estate: &EStateData<'mcx>,
) -> Option<StagedFeedShape> {
    let (guarded, fold_cols): (bool, Vec<u16>) = {
        let plan = ::nodeagg::agg_lanefold_plan(agg)?;
        (plan.guarded, plan.cols.iter().copied().collect())
    };
    let has_resid = ::nodeagg::agg_lanefold_has_resid(agg);
    // Full needed-column census up front (the borrow of `agg` must end
    // before the multi-key arm takes `&mut agg` to arm the compact table).
    let (natts, max_colno, needed_all): (usize, i32, Vec<(u16, i16, bool)>) = {
        let desc = estate.slot(join_result_slot).base().tts_tupleDescriptor.as_ref()?;
        let natts = desc.attrs.len();
        let (colnos_needed, max_colno) = ::nodeagg::agg_hash_needed_cols(agg);
        if colnos_needed.len() != natts {
            return None;
        }
        debug_assert!(fold_cols.iter().all(|&c| colnos_needed[c as usize]));
        let needed_all = colnos_needed
            .iter()
            .enumerate()
            .filter(|&(_, &n)| n)
            .map(|(c, _)| (c as u16, desc.attrs[c].attlen, desc.attrs[c].attbyval))
            .collect();
        (natts, max_colno, needed_all)
    };
    let fold_bound = fold_cols.iter().map(|&c| c as i32 + 1).max().unwrap_or(0);
    // Mode choice: guarded plans keep the proof/Demote staging; unguarded
    // plans with fully-admitted transitions (no residuals — they need the
    // live row at probe time) take the K2 deferred batched probe when the
    // grouping key is a single kernel-hostable column, the packed multi-key
    // deferred probe when 2..N keys pack into the compact table
    // (`staged_mk_admit` — armed as a side effect); otherwise the arrival
    // probe. `PGRUST_LANE_V2_K2=0` / `PGRUST_LANE_V2_MKSTREAM=0` force
    // arrival mode per arm (A/B kill-switches; byte-identical either way up
    // to the compact table's group-order relaxation).
    let mut mk = None;
    let mode = if guarded {
        StagedMode::Guarded
    } else if has_resid {
        StagedMode::Arrival
    } else {
        match ::nodeagg::agg_hash_staged_probe_col(agg).filter(|_| k2_enabled()) {
            // The key must be in the staged needed set (it always is — the
            // spill projection keeps grouping columns); structural gate.
            Some(key_col)
                if (key_col as usize) < natts
                    && needed_all.iter().any(|&(c, _, _)| c == key_col) =>
            {
                StagedMode::K2 { key_col }
            }
            Some(_) => StagedMode::Arrival,
            None => match staged_mk_admit(agg, natts, &needed_all) {
                Some(shape) => {
                    mk = Some(shape);
                    StagedMode::Mk
                }
                None => StagedMode::Arrival,
            },
        }
    };
    let needed: Vec<(u16, i16, bool)> =
        if mode == StagedMode::Arrival { Vec::new() } else { needed_all };
    Some(StagedFeedShape { mode, fold_cols, fold_bound, needed, max_colno, natts, mk })
}

impl<'a, 'mcx> StagedFoldAggSink<'a, 'mcx> {
    /// Construction for an admitted shape (`staged_feed_shape` returned the
    /// inputs). The guarded replay slot is memoized across rescan rebuilds (a
    /// fresh extra slot per rebuild would grow es_tupleTable per rescan).
    fn new(
        agg: &'a mut ::nodeagg::AggStateData<'mcx>,
        join_result_slot: ExecSlotId,
        stage_slot_memo: &mut Option<ExecSlotId>,
        shape: StagedFeedShape,
        estate: &mut EStateData<'mcx>,
    ) -> Self {
        let StagedFeedShape { mode, fold_cols, fold_bound, needed, max_colno, natts, mk } =
            shape;
        // Guarded, K2 and Mk modes stage every needed column (guarded for
        // the Demote replay, K2/Mk for the deferred probe + spill/demote
        // replay) and need the replay slot + by-ref arena; arrival mode
        // stages only the (byval) fold lanes.
        let (stage_slot, stage_cxt) = if mode == StagedMode::Arrival {
            (None, None)
        } else {
            let slot = match *stage_slot_memo {
                Some(s) => s,
                None => {
                    let desc =
                        estate.slot(join_result_slot).base().tts_tupleDescriptor.clone();
                    let s = estate
                        .exec_init_extra_tuple_slot(desc, ::types_slot::TupleSlotKind::Virtual);
                    *stage_slot_memo = Some(s);
                    s
                }
            };
            let cxt = estate
                .es_query_cxt
                .context()
                .new_child_bump("lane-v2 staged join feed");
            (Some(slot), Some(cxt))
        };
        let mut lanes = StagedLanes {
            values: vec![Vec::new(); natts],
            isnull: vec![Vec::new(); natts],
        };
        let staged: Vec<u16> = if mode == StagedMode::Arrival {
            fold_cols.clone()
        } else {
            needed.iter().map(|&(c, _, _)| c).collect()
        };
        for &c in &staged {
            lanes.values[c as usize].reserve_exact(STAGE_ROWS);
            lanes.isnull[c as usize].reserve_exact(STAGE_ROWS);
        }
        StagedFoldAggSink {
            agg,
            mode,
            fold_cols,
            fold_bound,
            stage_slot,
            natts,
            max_colno,
            needed,
            lanes,
            nstaged: 0,
            stage_cxt,
            idxs: Vec::new(),
            groups: Vec::new(),
            hashes: Vec::new(),
            mk: mk.map(|shape| StagedMk {
                shape,
                demoted: false,
                packbuf: Vec::new(),
                keys1: Vec::new(),
                keys2: Vec::new(),
            }),
        }
    }

    /// Re-present staged row `k` in the replay slot: needed columns carry the
    /// staged values, unneeded columns are NULL (the spill projection's own
    /// treatment, so a spilled staged row is byte-identical).
    fn replay_row(&self, k: usize, estate: &mut EStateData<'mcx>) {
        let mcx = estate.es_query_cxt;
        let slot = estate.slot_mut(self.stage_slot.expect("staging mode has a replay slot"));
        ::exectuples::exec_clear_tuple(slot, mcx);
        {
            let base = slot.base_mut();
            for c in 0..self.natts {
                base.tts_values[c] = ::datum::Datum::null();
                base.tts_isnull[c] = true;
            }
            for &(c, _, _) in &self.needed {
                let c = c as usize;
                base.tts_values[c] = self.lanes.values[c][k];
                base.tts_isnull[c] = self.lanes.isnull[c][k];
            }
        }
        ::exectuples::exec_store_virtual_tuple(slot);
    }

    /// Unguarded accept: stage the fold lanes (byval — plain datum copies),
    /// then run the group probe + residual transitions NOW against the
    /// incoming joined slot — exactly the per-row sink's call — snapshotting
    /// the pergroup for the batch fold.
    fn accept_unguarded(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()> {
        {
            let slot = estate.slot_mut(tuple);
            ::exectuples::slot_getsomeattrs(slot, self.fold_bound);
            let base = slot.base();
            for &c in &self.fold_cols {
                let c = c as usize;
                self.lanes.values[c].push(base.tts_values[c]);
                self.lanes.isnull[c].push(base.tts_isnull[c]);
            }
        }
        let k = self.nstaged;
        self.nstaged += 1;
        if let Some(pg) = ::nodeagg::agg_hash_build_probe_resid(self.agg, estate, tuple)? {
            self.idxs.push(k as u32);
            self.groups.push(pg);
        }
        if self.nstaged == STAGE_ROWS {
            self.flush_unguarded(estate)?;
        }
        Ok(())
    }

    /// Unguarded flush: just the whole-batch fold over the snapshotted
    /// pergroups (the probe/residuals already ran at arrival).
    fn flush_unguarded(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        let _ = estate;
        // SAFETY: staged fold lanes cover every plan column for all staged
        // rows (`idxs` indexes this window); the plan is unguarded, so no
        // guard proof is required; the rest is agg_fold_staged's contract
        // (the probe just installed each snapshot within this batch).
        unsafe { agg_fold_staged(self.agg, &self.lanes, &self.idxs, &self.groups)? }
        for &c in &self.fold_cols {
            let c = c as usize;
            self.lanes.values[c].clear();
            self.lanes.isnull[c].clear();
        }
        self.idxs.clear();
        self.groups.clear();
        self.nstaged = 0;
        Ok(())
    }

    /// Stage every needed column of the incoming joined row (guarded and K2
    /// modes — nothing runs at arrival in either).
    fn stage_needed_row(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()> {
        {
            let slot = estate.slot_mut(tuple);
            ::exectuples::slot_getsomeattrs(slot, self.max_colno);
        }
        let StagedFoldAggSink { needed, lanes, stage_cxt, .. } = &mut *self;
        let base = estate.slot(tuple).base();
        for &(c, attlen, byval) in needed.iter() {
            let c = c as usize;
            let (v, isnull) = (base.tts_values[c], base.tts_isnull[c]);
            // By-ref values may point into per-tuple memory the probe resets
            // row to row (and heap pages the outer scan unpins): copy into
            // the per-batch arena so the staged window is self-contained.
            let v = if isnull || byval {
                v
            } else {
                let cxt = stage_cxt.as_ref().expect("staging mode has a stage cxt");
                crate::nodesubplan::datum_copy_in(cxt.mcx(), v, attlen)?
            };
            lanes.values[c].push(v);
            lanes.isnull[c].push(isnull);
        }
        self.nstaged += 1;
        Ok(())
    }

    /// Guarded accept: stage every needed column (nothing may run before the
    /// batch guard proof — a Demote must replay the WHOLE batch through the
    /// checked per-row program).
    fn accept_guarded(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()> {
        self.stage_needed_row(tuple, estate)?;
        if self.nstaged == STAGE_ROWS {
            self.flush_guarded(estate)?;
        }
        Ok(())
    }

    /// K2 accept: stage every needed column; the group probe is deferred to
    /// the batched flush.
    fn accept_k2(&mut self, tuple: ExecSlotId, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        self.stage_needed_row(tuple, estate)?;
        if self.nstaged == STAGE_ROWS {
            self.flush_k2(estate)?;
        }
        Ok(())
    }

    /// K2 flush — the batched group-probe pre-pass: (1) one CFI per batch
    /// (design §9 cadence); (2) the batched hash loop over the staged
    /// grouping-key lane (bit-identical per element to the per-row
    /// `TupleHashTableHash`, by the probe-kernel contract); (3) the in-order
    /// probe of every staged row through the same C-ported tuplehash lookup
    /// (same first-arrival insertion, same entry init, same spill-mode gate —
    /// identical table layout / iteration order / output bytes); spill-mode
    /// misses replay the row (needed cols, unneeded NULL — the spill
    /// projection's own treatment) and spill it byte-identically; (4) the
    /// whole-batch fold over the resolved pergroups.
    fn flush_k2(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        let n = self.nstaged;
        if n == 0 {
            return Ok(());
        }
        ::postgres_seams::check_for_interrupts::call()?;
        let StagedMode::K2 { key_col } = self.mode else {
            unreachable!("flush_k2 outside K2 mode")
        };
        let kc = key_col as usize;
        {
            let StagedFoldAggSink { agg, lanes, hashes, .. } = &mut *self;
            ::nodeagg::agg_hash_hash_staged(
                agg,
                &lanes.values[kc],
                &lanes.isnull[kc],
                hashes,
            )?;
        }
        self.idxs.clear();
        self.groups.clear();
        for k in 0..n {
            let probed = ::nodeagg::agg_hash_probe_staged(
                self.agg,
                estate,
                self.lanes.values[kc][k],
                self.lanes.isnull[kc][k],
                self.hashes[k],
            )?;
            match probed {
                Some(pg) => {
                    self.idxs.push(k as u32);
                    self.groups.push(pg);
                }
                None => {
                    // Spill-mode miss: replay + spill; no transition runs
                    // for the row (the per-row path's exact treatment).
                    let stage_slot =
                        self.stage_slot.expect("staging mode has a replay slot");
                    self.replay_row(k, estate);
                    ::nodeagg::agg_hash_spill_staged(
                        self.agg, estate, stage_slot, self.hashes[k],
                    )?;
                }
            }
        }
        // SAFETY: staged lanes cover every plan column for all staged rows
        // (plan.cols ⊆ colnos_needed); the plan is unguarded (K2 admission);
        // each pergroup was installed by the probe within this batch; the
        // rest is agg_fold_staged's contract.
        unsafe { agg_fold_staged(self.agg, &self.lanes, &self.idxs, &self.groups)? }
        for &(c, _, _) in &self.needed {
            let c = c as usize;
            self.lanes.values[c].clear();
            self.lanes.isnull[c].clear();
        }
        self.nstaged = 0;
        self.stage_cxt.as_mut().expect("staging mode has a stage cxt").reset();
        Ok(())
    }

    /// Mk accept: stage every needed column; the packed multi-key probe is
    /// deferred to the batched flush.
    fn accept_mk(&mut self, tuple: ExecSlotId, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        self.stage_needed_row(tuple, estate)?;
        if self.nstaged == STAGE_ROWS {
            self.flush_mk(estate)?;
        }
        Ok(())
    }

    /// Clear the staged window (all needed lanes + the by-ref arena) after a
    /// staging-mode flush.
    fn clear_staged_window(&mut self) {
        for &(c, _, _) in &self.needed {
            let c = c as usize;
            self.lanes.values[c].clear();
            self.lanes.isnull[c].clear();
        }
        self.nstaged = 0;
        self.stage_cxt.as_mut().expect("staging mode has a stage cxt").reset();
    }

    /// Mk flush — the packed multi-key deferred probe (`scan_mk_batch`'s
    /// slot-stream analog): (1) one CFI per batch (design §9 cadence);
    /// (2) demote decision BEFORE any packing — the runtime backstop
    /// (memory migration), then per-value packability over the staged key
    /// lanes (NULL keys on a non-nullable image; unpackable numerics —
    /// range / non-minimal display scale). A demote migrates the compact
    /// groups into the C tuplehash ONCE; this batch and every later one
    /// replay per-row through the arrival probe (byte-identical, spill
    /// machinery intact). (3) the component-major pack of the staged key
    /// lanes into the reused u128 accumulator — Int shift/mask, numeric
    /// keypack, raw-bytes text through the build-lifetime intern table
    /// (slot streams carry raw varlenas, no dict codes: intern per row;
    /// NULLs on nullable images set the bitmap bit, value bits zero);
    /// (4) the compact-table batched probe + new-group seeding; (5) the
    /// whole-batch fold. Every staged row is a survivor (the feed stages
    /// only emitted rows), so the fold covers `0..n`.
    fn flush_mk(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        let n = self.nstaged;
        if n == 0 {
            return Ok(());
        }
        ::postgres_seams::check_for_interrupts::call()?;
        debug_assert!(self.mode == StagedMode::Mk, "flush_mk outside Mk mode");
        let live = !self.mk.as_ref().expect("Mk mode carries its state").demoted
            && ::nodeagg::agg_hash_compact_backstop(self.agg, estate)?;
        let packable = live && {
            let StagedFoldAggSink { lanes, mk, .. } = &*self;
            let shape = &mk.as_ref().expect("Mk mode carries its state").shape;
            shape.comps.iter().all(|comp| {
                let att = comp.att as usize;
                let (values, isnull) = (&lanes.values[att], &lanes.isnull[att]);
                match comp.kind {
                    ::nodeagg::MkCompKind::Numeric { width } => (0..n).all(|k| {
                        if isnull[k] {
                            shape.nullable
                        } else {
                            ::nodeagg::mk_numeric_datum_bits(values[k], width).is_some()
                        }
                    }),
                    // Int/Intern values always pack; a NULL key needs the
                    // bitmap byte — without it, demote.
                    ::nodeagg::MkCompKind::Int { .. } | ::nodeagg::MkCompKind::Intern => {
                        shape.nullable || (0..n).all(|k| !isnull[k])
                    }
                }
            })
        };
        if live && !packable {
            ::nodeagg::agg_hash_compact_disarm(self.agg, estate)?;
        }
        if !live || !packable {
            self.mk.as_mut().expect("Mk mode carries its state").demoted = true;
            return self.flush_mk_demoted(estate);
        }
        {
            let StagedFoldAggSink { agg, lanes, mk, stage_cxt, idxs, groups, .. } = &mut *self;
            let StagedMk { shape, packbuf, keys1, keys2, .. } =
                mk.as_mut().expect("Mk mode carries its state");
            packbuf.clear();
            packbuf.resize(n, 0u128);
            for (j, comp) in shape.comps.iter().enumerate() {
                let att = comp.att as usize;
                let off_bits = comp.off as u32 * 8;
                let (values, isnull) = (&lanes.values[att], &lanes.isnull[att]);
                // Only read when `shape.nullable` (guarded per row below).
                let null_bit = if shape.nullable {
                    1u128 << (shape.null_off() as u32 * 8 + j as u32)
                } else {
                    0
                };
                match comp.kind {
                    ::nodeagg::MkCompKind::Int { width } => {
                        let mask =
                            if width == 8 { u64::MAX } else { (1u64 << (width * 8)) - 1 };
                        for (k, pb) in packbuf.iter_mut().enumerate() {
                            if shape.nullable && isnull[k] {
                                // CH nullable_keys128: bit j set, value bits
                                // zero — NOT-DISTINCT composite NULLs hold.
                                *pb |= null_bit;
                                continue;
                            }
                            debug_assert!(!isnull[k], "NULL keys demote before packing");
                            let v = match width {
                                2 => values[k].as_i16() as i64,
                                4 => values[k].as_i32() as i64,
                                _ => values[k].as_i64(),
                            };
                            *pb |= (((v as u64) & mask) as u128) << off_bits;
                        }
                    }
                    ::nodeagg::MkCompKind::Numeric { width } => {
                        for (k, pb) in packbuf.iter_mut().enumerate() {
                            if shape.nullable && isnull[k] {
                                *pb |= null_bit;
                                continue;
                            }
                            let bits = ::nodeagg::mk_numeric_datum_bits(values[k], width)
                                .expect("numeric packability proven by the batch pre-check");
                            *pb |= (bits as u128) << off_bits;
                        }
                    }
                    ::nodeagg::MkCompKind::Intern => {
                        let cxt = stage_cxt.as_ref().expect("staging mode has a stage cxt");
                        for (k, pb) in packbuf.iter_mut().enumerate() {
                            if shape.nullable && isnull[k] {
                                *pb |= null_bit;
                                continue;
                            }
                            debug_assert!(!isnull[k], "NULL keys demote before packing");
                            // SAFETY: staged non-null live text varlena,
                            // datum-copied into the batch arena at accept
                            // (kernel selection proved the column type). A
                            // detoast copy lands in the batch arena too —
                            // reset after this flush; the intern table owns
                            // its own copy of the bytes.
                            let v = unsafe {
                                ::types_fmgr::datum_varlena_packed(values[k], cxt.mcx())
                            }?;
                            let id = ::nodeagg::agg_hash_compact_intern(agg, v.data());
                            *pb |= (id as u128) << off_bits;
                        }
                    }
                }
            }
            // Split the accumulator into the packed key lane and probe.
            if shape.two_words {
                keys2.clear();
                keys2.extend(packbuf.iter().map(|&w| [w as u64, (w >> 64) as u64]));
                ::nodeagg::agg_hash_compact_batch_mk2(agg, keys2, groups)?;
            } else {
                keys1.clear();
                keys1.extend(packbuf.iter().map(|&w| w as u64 as i64));
                ::nodeagg::agg_hash_compact_batch_mk1(agg, keys1, groups)?;
            }
            idxs.clear();
            idxs.extend(0..n as u32);
            // SAFETY: staged lanes cover every plan column for all staged
            // rows (plan.cols ⊆ colnos_needed); the plan is unguarded (Mk
            // admission); each pergroup was installed by the compact probe
            // within this batch; the rest is agg_fold_staged's contract.
            unsafe { agg_fold_staged(agg, &*lanes, idxs, groups)? }
        }
        self.clear_staged_window();
        Ok(())
    }

    /// Mk demote leg: the staged window replays per-row through the arrival
    /// probe against the C tuplehash (the compact groups migrated at the
    /// demote) — `flush_guarded`'s replay loop without the guard proof (the
    /// plan is unguarded by Mk admission) — then the whole-batch fold.
    /// Spill-mode misses return no pergroup and run no transition, exactly
    /// the per-row build's treatment.
    fn flush_mk_demoted(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        let n = self.nstaged;
        let stage_slot = self.stage_slot.expect("staging mode has a replay slot");
        self.idxs.clear();
        self.groups.clear();
        for k in 0..n {
            self.replay_row(k, estate);
            if let Some(pg) =
                ::nodeagg::agg_hash_build_probe_resid(self.agg, estate, stage_slot)?
            {
                self.idxs.push(k as u32);
                self.groups.push(pg);
            }
        }
        // SAFETY: staged lanes cover every plan column for all staged rows
        // (plan.cols ⊆ colnos_needed); the plan is unguarded (Mk admission);
        // each pergroup was installed by the probe within this batch; the
        // rest is agg_fold_staged's contract.
        unsafe { agg_fold_staged(self.agg, &self.lanes, &self.idxs, &self.groups)? }
        self.clear_staged_window();
        Ok(())
    }

    /// Guarded flush: one CHECK_FOR_INTERRUPTS per batch (design §9
    /// batch-operator cadence), the guard proof re-run per batch, then the
    /// replayed probe/residual + fold — or the whole batch through the
    /// checked per-row program on Demote.
    fn flush_guarded(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        let n = self.nstaged;
        if n == 0 {
            return Ok(());
        }
        ::postgres_seams::check_for_interrupts::call()?;
        let stage_slot = self.stage_slot.expect("guarded mode has a replay slot");
        // Re-prove per staged batch. Join output has no zone map, so the
        // proof always runs the exact data-scan tier over the staged lanes.
        // Every staged row is selected (the join emits only qual-passing
        // rows).
        let demote = {
            let plan =
                ::nodeagg::agg_lanefold_plan(self.agg).expect("staged feed without a plan");
            let nwords = n.div_ceil(64);
            let mut rows = [u64::MAX; ::exectuples::SOA_BM_WORDS];
            if n % 64 != 0 {
                rows[nwords - 1] = (1u64 << (n % 64)) - 1;
            }
            // SAFETY: every staged lane value is a live datum copied from
            // the joined row at accept time (StagedLanes contract); the
            // staged join feed admits no varlena lanes, so no vguard column
            // is ever probed here.
            unsafe {
                ::lanefold::check_guards(plan, &self.lanes, &rows[..nwords], |_| None)
                    == ::lanefold::GuardCheck::Demote
            }
        };
        if demote {
            // The WHOLE batch runs the checked per-row program (never mixing
            // a partial fold with per-row transitions — lanefold contract);
            // it raises C's error at C's row.
            for k in 0..n {
                self.replay_row(k, estate);
                ::nodeagg::agg_hash_build_accept(self.agg, estate, stage_slot)?;
            }
        } else {
            self.idxs.clear();
            self.groups.clear();
            for k in 0..n {
                self.replay_row(k, estate);
                if let Some(pg) =
                    ::nodeagg::agg_hash_build_probe_resid(self.agg, estate, stage_slot)?
                {
                    self.idxs.push(k as u32);
                    self.groups.push(pg);
                }
            }
            // SAFETY: staged lanes cover every plan column for all staged
            // rows (plan.cols ⊆ colnos_needed); the guard proof passed on
            // this batch; the rest is agg_fold_staged's contract.
            unsafe { agg_fold_staged(self.agg, &self.lanes, &self.idxs, &self.groups)? }
        }
        for &(c, _, _) in &self.needed {
            let c = c as usize;
            self.lanes.values[c].clear();
            self.lanes.isnull[c].clear();
        }
        self.nstaged = 0;
        self.stage_cxt.as_mut().expect("guarded mode has a stage cxt").reset();
        Ok(())
    }
}

impl<'mcx> Sink<'mcx> for StagedFoldAggSink<'_, 'mcx> {
    fn accept(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<SinkFeed> {
        match self.mode {
            StagedMode::Guarded => self.accept_guarded(tuple, estate)?,
            StagedMode::K2 { .. } => self.accept_k2(tuple, estate)?,
            StagedMode::Mk => self.accept_mk(tuple, estate)?,
            StagedMode::Arrival => self.accept_unguarded(tuple, estate)?,
        }
        Ok(SinkFeed::NeedMore)
    }

    // Stage-4 combine seam (see HashAggBuildSink::combine): flush the staged
    // tail first so the handed table is complete, then hand off; the
    // following finish re-flushes nothing (flushes drain their staging) and
    // skips the install (combined flag).
    fn combine(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        match self.mode {
            StagedMode::Guarded => self.flush_guarded(estate)?,
            StagedMode::K2 { .. } => self.flush_k2(estate)?,
            StagedMode::Mk => self.flush_mk(estate)?,
            StagedMode::Arrival => self.flush_unguarded(estate)?,
        }
        ::nodeagg::agg_hash_build_combine(self.agg, estate)
    }

    fn finish(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        match self.mode {
            StagedMode::Guarded => self.flush_guarded(estate)?,
            StagedMode::K2 { .. } => self.flush_k2(estate)?,
            StagedMode::Mk => self.flush_mk(estate)?,
            StagedMode::Arrival => self.flush_unguarded(estate)?,
        }
        ::nodeagg::agg_hash_build_finish(self.agg, estate)
    }
}

/// Engagement trace for the composition feeds, env-gated
/// (`PGRUST_LANE_V2_TRACE=1`): one line per build-feed engagement on stderr.
/// Diagnostics only — never affects execution.
fn trace_feed(msg: &str) {
    static ON: OnceLock<bool> = OnceLock::new();
    if *ON.get_or_init(|| {
        matches!(std::env::var("PGRUST_LANE_V2_TRACE").as_deref(), Ok("1") | Ok("on"))
    }) {
        eprintln!("[lanev2] {msg}");
    }
}

/// Try to let the lane own `Agg(hashed) → HashJoin(admitted type) → scans`
/// — the first breaker-to-breaker composition. Three pipelines on two breaker
/// nodes, all phase flags node-resident row-path state:
///
///   1. build:  inner scan → filter/project → HashJoinBuildSink
///   2. probe:  outer scan → filter/project → JoinProbe → agg build sink
///   3. emit:   HashAggSource → HashAggEmit → RootAdapter (one group per pull)
///
/// The probe-pipeline sink is the staged fold feed (`StagedFoldAggSink`) when
/// the agg carries a lanefold plan — the batched joined-row feed — and the
/// per-row `HashAggBuildSink` otherwise. `stage_slot` memoizes the staged
/// feed's replay slot across rescan rebuilds.
///
/// `None` = refused (caller falls to the per-tuple `exec_agg` over
/// `exec_hash_join`, byte-identically — including after a lane-delegated
/// join build that then spill-refused).
#[inline]
pub fn try_own_agg_over_hash_join<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    hj: &mut crate::procnode::HashJoinNode<'mcx>,
    stage_slot: &mut Option<ExecSlotId>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic per-call gates, ticked under the join class (the composition's
    // pipelines all hang off the join's drive).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::Join, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(ShapeClass::Join, RefuseReason::Backward);
        return Ok(None);
    }
    if !::nodeagg::agg_hash_breaker_admissible(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AggNotDrainable);
        return Ok(None);
    }
    if !hash_join_lane_fusible(hj, estate)? {
        return Ok(None);
    }
    // exec_agg's top-of-call guard: a drained agg stays drained.
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    if !agg_hash_join_build_if_needed(agg, hj, stage_slot, estate)? {
        return Ok(None);
    }
    // Agg emit phase (every call): one qual-passing group per PG pull, in
    // C's retrieve order.
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step(agg, &mut HashAggSource, &mut HashAggEmit, &mut root, estate)?))
}

/// Build phases of the agg-over-join composition (join build, then the probe
/// drain into the agg breaker sink), once, lazily. `Ok(false)` = multi-batch
/// spill refuse — the caller must refuse ownership; no lane tuple has been
/// emitted, so the fallback per-tuple agg over `exec_hash_join` resumes from
/// HJ_NEED_NEW_OUTER over the identical table. Shared by the bare
/// composition hook above and the Limit-over-agg chain (`try_own_limit`).
fn agg_hash_join_build_if_needed<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    hj: &mut crate::procnode::HashJoinNode<'mcx>,
    stage_slot: &mut Option<ExecSlotId>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if ::nodeagg::agg_hash_table_filled(agg) {
        return Ok(true);
    }
    let crate::procnode::HashJoinNode { state, outer, hash, .. } = hj;
    let crate::procnode::HashSubNode { state: hstate, child } = &mut **hash;
    // Join build phase (once, lazily; a rescan that rebuilt the inner
    // side re-enters here via the node's own HJ_BUILD_HASHTABLE).
    if ::nodehashjoin::lane_join_phase(state, hstate)
        == ::nodehashjoin::LaneJoinPhase::Build
    {
        let done = join_build_dispatch(state, hstate, child, estate)?;
        if !done.empty && done.nbatch > 1 {
            // Spill refuse before any lane tuple is emitted; the
            // fallback per-tuple agg over exec_hash_join resumes from
            // HJ_NEED_NEW_OUTER over the identical table.
            stats::tick_refused(ShapeClass::Join, RefuseReason::MultiBatch);
            return Ok(false);
        }
        // Bloom pushdown reclaim (see try_own_hash_join): legacy-seat
        // parity — SeqScan outer drives only; the arm re-applies the
        // row path's exact push gate.
        if !done.empty {
            if let crate::procnode::PlanStateNode::SeqScan(_) = &**outer {
                ::nodehashjoin::lane_probe_filter_arm(state, hstate);
            }
        }
    }
    match ::nodehashjoin::lane_join_phase(state, hstate) {
        ::nodehashjoin::LaneJoinPhase::EmptyDone => {
            // A non-fill-outer join (INNER/SEMI/RIGHT/RIGHT_SEMI/
            // RIGHT_ANTI) over an empty build: emits nothing — an empty
            // build has no unmatched inner tuples to fill either — and
            // the outer child is never pulled (C's early return;
            // LEFT/FULL/ANTI never take this phase — their empty build
            // proceeds to the probe and null-fills). The agg finalizes
            // over an empty input.
            stats::tick_owned(ShapeClass::AggBuild);
            let mut sink = HashAggBuildSink { agg };
            sink.finish(estate)?;
        }
        ::nodehashjoin::LaneJoinPhase::Probe => {
            if hstate.table.as_ref().expect("probe phase has a table").nbatch > 1 {
                stats::tick_refused(ShapeClass::Join, RefuseReason::MultiBatch);
                return Ok(false);
            }
            // One OWNED tick per lane-owned agg build event (here the
            // build is fed by the join probe drain).
            stats::tick_owned(ShapeClass::AggBuild);
            // Batched joined-row feed when the agg carries a fold plan;
            // the per-row breaker sink otherwise.
            match staged_feed_shape(agg, state.ps_ResultTupleSlot, estate) {
                Some(shape) => {
                    trace_feed(match shape.mode {
                        StagedMode::Guarded => {
                            "agg-over-join: staged fold feed engaged (guarded)"
                        }
                        StagedMode::K2 { .. } => {
                            "agg-over-join: staged fold feed engaged (k2 probe)"
                        }
                        StagedMode::Mk => {
                            "agg-over-join: staged fold feed engaged (mk probe)"
                        }
                        StagedMode::Arrival => "agg-over-join: staged fold feed engaged",
                    });
                    let mut sink = StagedFoldAggSink::new(
                        agg,
                        state.ps_ResultTupleSlot,
                        stage_slot,
                        shape,
                        estate,
                    );
                    join_probe_drain_dispatch(state, hstate, outer, &mut sink, estate)?;
                }
                None => {
                    trace_feed("agg-over-join: per-row sink (no fold plan)");
                    let mut sink = HashAggBuildSink { agg: &mut *agg };
                    join_probe_drain_dispatch(state, hstate, outer, &mut sink, estate)?;
                }
            }
        }
        ::nodehashjoin::LaneJoinPhase::Build => unreachable!("build ran above"),
    }
    Ok(true)
}

// ===========================================================================
// Streaming Limit + Unique (Phase-2 breadth): mid-pipeline `TupleOp`s at the
// TOP of an already-lane-owned chain, engaged ONLY where the lane owns the
// child pipeline (admission economics, design §4): a Volcano Limit/Unique is
// already cheap per-tuple and PG's pull already stops a lane pipeline lazily,
// so ownership here buys chain continuity (no per-tuple root adapter between
// the breaker emit and the limit/dedup — and future within-pipeline fusion),
// never a new layer over a refused child.
//
//   Limit  (Pattern 2, DuckDB streaming limit): counts in the node's own
//          LimitState (lstate/position — C's cross-call state, so a Volcano
//          fallback at any call boundary is byte-safe), delivers the boundary
//          tuple via `Paused`, reports `Finished` on the next driver round —
//          the source is never pulled past the boundary tuple's batch, and
//          quals/projections are never evaluated past the limit (C's LIMIT
//          stops calling its child). OFFSET tuples are pulled + discarded,
//          exactly as C's LIMIT_RESCAN skip loop pulls them.
//   Unique (over the sort breaker): adjacent-dedup streaming op — one sorted
//          tuple in, 0..1 group heads out, via `nodeunique::lane_unique_feed`
//          (the SAME grouping-equality program + prev-slot copy exec_unique
//          runs — reused, not reimplemented).
//
// Row-identity note (LIMIT without ORDER BY): C returns whichever rows its
// plan yields first. The lane's owned pipelines emit C's rows in C's order BY
// CONSTRUCTION — scan pipelines walk the same pages/TID runs in the same
// order, and breaker read-backs delegate to the same tuplesort / hash-table
// retrieves — so the lane's first k tuples are C's first k tuples,
// byte-identically (verified by the full regress off/on comparison).
//
// Refused shapes (each byte-safe on the Volcano fallback):
//   * LIMIT ... WITH TIES — needs boundary-tuple retention + the sort-peer
//     equality walk (LIMIT_WINDOWEND_TIES); staged later. (PG's Limit node
//     has no percent-limit form — nothing to gate.)
//   * Limit/Unique over a BARE scan — the scan hooks themselves refuse
//     standalone ownership (per-tuple emission through the pull adapter with
//     no batch consumer above = pure adapter overhead); a Volcano
//     Limit/Unique over the refused scan IS C's shape, so taking ownership
//     adds a layer with no consumer benefit.
//   * Limit over a bare HashJoin — needs a two-TupleOp chain driver
//     (JoinProbe → LimitOp); staged with the next chain generalization.
//   * Backward/scrollable cursors — a scrollable/backward cursor forces
//     randomAccess on the Sort child (refused by `sort_fusible`), Limit
//     never sees EXEC_FLAG_MARK (init assert), Unique never sees
//     BACKWARD/MARK (init assert); the dynamic direction gate refuses any
//     non-forward pull.
//   * Hashed DISTINCT is NOT here: the planner emits Agg (AGG_HASHED, zero
//     aggregates), which the hash-agg breaker already admits
//     (`agg_hash_breaker_admissible` — evaltrans is an empty transition
//     program, subplan- and param-free trivially).
// ===========================================================================

/// The Limit node as a mid-pipeline streaming operator. All window
/// arithmetic delegates to `nodelimit`'s lane seam, which mirrors
/// `exec_limit`'s forward COUNT arms verbatim over the same node state.
struct LimitOp<'a, 'mcx> {
    limit: &'a mut ::nodelimit::LimitState<'mcx>,
}

impl<'mcx> TupleOp<'mcx> for LimitOp<'_, 'mcx> {
    fn pending(&self) -> bool {
        // Window complete (the boundary tuple was already delivered via
        // `Paused`): the next driver round must resume() → `Finished`
        // BEFORE the source is pulled again — the Paused-then-Finished rule.
        ::nodelimit::lane_limit_window_done(self.limit)
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        match ::nodelimit::lane_limit_feed(self.limit, tuple) {
            ::nodelimit::LaneLimitFeed::Skip => Ok(OpStatus::NeedInput),
            ::nodelimit::LaneLimitFeed::Emit => Ok(match out.accept(tuple, estate)? {
                SinkFeed::Full => OpStatus::Paused,
                SinkFeed::NeedMore => OpStatus::NeedInput,
            }),
            ::nodelimit::LaneLimitFeed::EmitBoundary => {
                // Paused-then-Finished (`OpStatus::Finished` contract):
                // deliver the boundary tuple now; pending()/resume() report
                // Finished on the next driver round. The downstream sink is
                // always the capacity-one root here (LimitOp tops the chain),
                // so accept necessarily returns Full.
                let fed = out.accept(tuple, estate)?;
                debug_assert_eq!(fed, SinkFeed::Full, "limit chain must end at the root adapter");
                let _ = fed;
                Ok(OpStatus::Paused)
            }
        }
    }

    fn resume(
        &mut self,
        _out: &mut dyn Sink<'mcx>,
        _estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        // Only reachable via pending() = window done: flip to LIMIT_WINDOWEND
        // (what C's next ExecLimit call would do) and end the stream.
        ::nodelimit::lane_limit_end_window(self.limit);
        Ok(OpStatus::Finished)
    }
}

/// The Unique node as a mid-pipeline streaming operator: never pends (no
/// intra-tuple expansion) and never finishes early.
struct UniqueOp<'a, 'mcx> {
    unique: &'a mut ::nodeunique::UniqueState<'mcx>,
}

impl<'mcx> TupleOp<'mcx> for UniqueOp<'_, 'mcx> {
    fn pending(&self) -> bool {
        false
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        match ::nodeunique::lane_unique_feed(self.unique, estate, tuple)? {
            None => Ok(OpStatus::NeedInput),
            Some(result) => Ok(match out.accept(result, estate)? {
                SinkFeed::Full => OpStatus::Paused,
                SinkFeed::NeedMore => OpStatus::NeedInput,
            }),
        }
    }

    fn resume(
        &mut self,
        _out: &mut dyn Sink<'mcx>,
        _estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        unreachable!("UniqueOp never pends")
    }
}

/// Admission for a hash-agg breaker child under a lane Limit or under the
/// sort breaker (`sort_refuse_reason`'s Agg arm — the `ORDER BY agg` tail):
/// the agg-side breaker gate × the child gates × (for the SeqScan feed) the
/// memoized `AggLaneChoice` — exactly the bare `agg_arm` hooks' admission
/// (`try_own_agg_over_seq_scan` / `try_own_agg_over_hash_join`), including
/// the economics `Refuse` arm, so a Limit- or Sort-owned agg chain admits
/// precisely where the agg hook would.
fn agg_child_fusible<'mcx>(
    aps: &mut crate::procnode::AggPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if !::nodeagg::agg_hash_breaker_admissible(&aps.agg) {
        return Ok(false);
    }
    match &mut aps.outer {
        crate::procnode::PlanStateNode::SeqScan(ss) => {
            if !seq_scan_fusible(ss, estate)? {
                return Ok(false);
            }
            let c = match aps.lane_choice {
                Some(c) => c,
                None => {
                    let c = decide_agg_lane(&aps.agg, ss, &mut aps.lane_exprkey, estate)?;
                    aps.lane_choice = Some(c);
                    c
                }
            };
            Ok(c != AggLaneChoice::Refuse)
        }
        crate::procnode::PlanStateNode::HashJoin(hj) => hash_join_lane_fusible(hj, estate),
        // Agg-over-gather: no child-side structural gate — the build reuses
        // `exec_gather` verbatim (section header), so every gather shape the
        // breaker-admissible agg sits on is drivable.
        crate::procnode::PlanStateNode::Gather(_) => Ok(agg_gather_enabled()),
        _ => Ok(false),
    }
}

/// Try to let the lane own a `Limit` over a lane-owned chain — the streaming
/// limit (see the section header above for the protocol, the row-identity
/// argument, and the documented refusals). Admitted children: the sort
/// breaker, and the hash-agg breaker over its admitted feeds (SeqScan, or
/// the hash-join composition). `None` = refused; falling to `exec_limit` is
/// byte-safe at any boundary because the lane drives the SAME LimitState
/// machine C does (including after the prologue below ran — C's own INITIAL
/// arm would have run the same recompute once).
#[inline]
pub fn try_own_limit<'mcx>(
    l: &mut crate::procnode::LimitNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    use ::nodelimit::LimitStateCond::*;
    // Dynamic per-call gates + the limit-side shape gate (COUNT only; the
    // option is init-stable so this refuse is stable too).
    if estate.es_epq_active
        || !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction)
        || !::nodelimit::lane_limit_admissible(&l.state)
    {
        return Ok(None);
    }
    // Child admission BEFORE any state effect (a refuse must leave the node
    // untouched). Child verdicts are memoized on the child nodes.
    let child_ok = match &mut *l.outer {
        crate::procnode::PlanStateNode::Sort(s) => sort_lane_fusible_memo(s, estate)?,
        crate::procnode::PlanStateNode::Agg(aps) => agg_child_fusible(&mut **aps, estate)?,
        _ => false,
    };
    if !child_ok {
        return Ok(None);
    }
    // C's exec_limit entry: CFI, then the LIMIT_INITIAL recompute (evaluates
    // OFFSET/LIMIT — same negative-value errors — and pushes the tuple bound
    // to the child: the Sort's top-N bound; a no-op for Agg).
    ::nodelimit::lane_limit_prologue(&mut l.state, &mut *l.outer, estate)?;
    match l.state.lstate {
        // Terminal forward states: nothing more to return (C's arms).
        LIMIT_EMPTY | LIMIT_WINDOWEND | LIMIT_SUBPLANEOF => return Ok(Some(None)),
        LIMIT_RESCAN => {
            // LIMIT 0: the window is empty and the child is NEVER pulled
            // (C's `count <= 0 && !noCount` arm) — no feed, no build.
            if ::nodelimit::lane_limit_empty_window(&mut l.state) {
                return Ok(Some(None));
            }
        }
        LIMIT_INWINDOW => {}
        LIMIT_INITIAL => unreachable!("prologue recomputed"),
        // Backward-only states — unreachable under the forward gate + the
        // non-scrollable admitted children; refuse defensively.
        LIMIT_WINDOWEND_TIES | LIMIT_WINDOWSTART => return Ok(None),
    }
    // Run the owned chain: child pipeline → LimitOp → root adapter.
    let r = match &mut *l.outer {
        crate::procnode::PlanStateNode::Sort(s) => {
            // C's first child pull enters ExecSort: entry CFI, then the feed
            // (the tuplesort bound set by the prologue makes it top-N,
            // exactly as C's bounded sort under Limit).
            ::postgres_seams::check_for_interrupts::call()?;
            let crate::procnode::SortNode { state, outer, outer_desc, .. } = s;
            if !sort_feed_if_needed(state, &mut **outer, outer_desc, None, estate)? {
                // Agg-over-join multi-batch spill refuse, before any lane
                // tuple or sort-side effect: exec_limit over the per-tuple
                // sort/agg/join resumes byte-identically (the recompute above
                // ran once, as C's INITIAL arm would have).
                return Ok(None);
            }
            let mut op = LimitOp { limit: &mut l.state };
            let mut root = RootAdapter::new(None);
            pull_step_chain(state, &mut SortEmitSource, &mut SortEmit, &mut op, &mut root, estate)?
        }
        crate::procnode::PlanStateNode::Agg(aps) => {
            let aps = &mut **aps;
            // exec_agg's top-of-call guard: a drained agg stays drained (the
            // hash iterator is spent) — treat as source EOF.
            if ::nodeagg::agg_is_done(&aps.agg) {
                None
            } else {
                let built = match &mut aps.outer {
                    crate::procnode::PlanStateNode::SeqScan(ss) => {
                        let c = aps.lane_choice.expect("admission decided the agg lane choice");
                        agg_seq_scan_build_if_needed(
                            &mut aps.agg,
                            ss,
                            c,
                            &mut aps.lane_stage_slot,
                            &mut aps.lane_exprkey,
                            estate,
                        )?;
                        true
                    }
                    crate::procnode::PlanStateNode::HashJoin(hj) => {
                        agg_hash_join_build_if_needed(
                            &mut aps.agg,
                            &mut **hj,
                            &mut aps.lane_stage_slot,
                            estate,
                        )?
                    }
                    crate::procnode::PlanStateNode::Gather(g) => {
                        agg_gather_build_if_needed(
                            &mut aps.agg,
                            &mut **g,
                            &mut aps.lane_stage_slot,
                            estate,
                        )?;
                        true
                    }
                    _ => unreachable!("agg_child_fusible admitted a non-lane agg feed"),
                };
                if !built {
                    // Join multi-batch spill refuse, before any lane tuple:
                    // exec_limit over the per-tuple agg/join resumes
                    // byte-identically (the recompute above ran once, as C's
                    // INITIAL arm would have).
                    return Ok(None);
                }
                let mut op = LimitOp { limit: &mut l.state };
                let mut root = RootAdapter::new(None);
                pull_step_chain(
                    &mut aps.agg,
                    &mut HashAggSource,
                    &mut HashAggEmit,
                    &mut op,
                    &mut root,
                    estate,
                )?
            }
        }
        _ => unreachable!("admitted a non-lane limit child"),
    };
    if r.is_none() && matches!(l.state.lstate, LIMIT_RESCAN | LIMIT_INWINDOW) {
        // Source exhausted before the window filled — C's subplan-EOF arms.
        ::nodelimit::lane_limit_eof(&mut l.state);
    }
    Ok(Some(r))
}

/// Try to let the lane own a `Unique` over the sort breaker — streaming
/// adjacent-dedup on the sorted emit (see the section header for economics +
/// refusals; hashed DISTINCT plans an Agg and is owned by the agg breaker).
/// `None` = refused; `exec_unique` drives the same UniqueState byte-safely.
#[inline]
pub fn try_own_unique<'mcx>(
    u: &mut crate::procnode::UniqueNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic per-call gates (Unique init asserts !BACKWARD && !MARK, so a
    // non-forward pull should be impossible — gate anyway, like the sort).
    if estate.es_epq_active
        || !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction)
    {
        return Ok(None);
    }
    let crate::procnode::UniqueNode { state, outer } = u;
    let crate::procnode::PlanStateNode::Sort(s) = outer else {
        return Ok(None);
    };
    if !sort_lane_fusible_memo(s, estate)? {
        return Ok(None);
    }
    // C's ExecUnique entry interrupt check (conditional, exactly the
    // Volcano entry's), then the first child pull's ExecSort entry CFI.
    ::nodeunique::lane_unique_cfi()?;
    ::postgres_seams::check_for_interrupts::call()?;
    let crate::procnode::SortNode { state: sstate, outer: souter, outer_desc, .. } = s;
    if !sort_feed_if_needed(sstate, &mut **souter, outer_desc, None, estate)? {
        return Ok(None);
    }
    let mut op = UniqueOp { unique: state };
    let mut root = RootAdapter::new(None);
    let r = pull_step_chain(sstate, &mut SortEmitSource, &mut SortEmit, &mut op, &mut root, estate)?;
    if r.is_none() {
        // exec_unique's end-of-stream arm: drop the retained previous tuple
        // and clear both slots.
        ::nodeunique::lane_unique_eof(state, estate);
    }
    Ok(Some(r))
}

// ===========================================================================
// Wave-4 streaming glue (Volcano-tail triage, 2026-07-12): three small
// streaming operators hosted where the lane already owns the neighboring
// pipeline — never a new layer over a refused child (admission economics,
// design §4; the Limit/Unique precedent):
//
//   Group        adjacent-row grouping over the SORT breaker's emit — a
//                mid-pipeline `TupleOp` running `exec_group`'s own per-tuple
//                body (`nodegroup::lane_group_feed`: the same
//                grouping-equality program, first-tuple copy, HAVING qual and
//                projection — reused, not reimplemented); state = the
//                node-resident first-tuple slot + have-first/grp_done flags,
//                so a Volcano fallback at any call boundary is byte-safe.
//                NOTE: Group the NODE only — AGG_SORTED / the agg breaker
//                admission are owned elsewhere (wave-4 charter split).
//   Result       the gating/projection node: `resconstantqual` evaluated
//                once (C's rs_checkqual arm, via `noderesult`'s seams), then
//                either the degenerate no-child pipeline (the single no-FROM
//                row) or the child stream projected row-by-row through a
//                `TupleOp` over the sort breaker's emit.
//   SubqueryScan a pass-through filter/project `TupleOp` over the child
//                pipeline (`execscan::lane_scan_accept` — `exec_scan_impl`'s
//                per-tuple qual/proj body, subplan/param arms included):
//                bare over the sort breaker, and spliced mid-pipeline in the
//                agg-over-subquery-over-scan composition so lane pipelines
//                chain through subquery boundaries end to end.
//
// Refused shapes (each byte-safe on the Volcano fallback): EPQ and
// non-forward pulls (dynamic gates, ticked per offered call); instrumented
// nodes (EXPLAIN ANALYZE keeps per-node counters — for the chained shapes an
// instrumented tree wraps every node so the child never matches the Sort/scan
// arms, and the Result no-FROM arm gates on the estate's instrumentation
// explicitly); any child that is not a lane-owned pipeline
// (`child-not-lane-owned`; the child's own refusal reason ticks under the
// child's class). Group/Result/SubqueryScan quals and projections run the
// nodes' OWN evaluation arms (subplan-aware where the node's Volcano body is
// — noderesult and execscan host subplans/params; nodegroup's body is reused
// verbatim), so no subplan-param refusal is needed at this layer.
// ===========================================================================

/// The Group node as a mid-pipeline streaming operator: one sorted tuple in,
/// 0..1 projected group heads out, never pends (no intra-tuple expansion) and
/// never finishes early.
struct GroupOp<'a, 'mcx> {
    group: &'a mut ::nodegroup::GroupState<'mcx>,
}

impl<'mcx> TupleOp<'mcx> for GroupOp<'_, 'mcx> {
    fn pending(&self) -> bool {
        false
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        match ::nodegroup::lane_group_feed(self.group, estate, tuple)? {
            None => Ok(OpStatus::NeedInput),
            Some(result) => Ok(match out.accept(result, estate)? {
                SinkFeed::Full => OpStatus::Paused,
                SinkFeed::NeedMore => OpStatus::NeedInput,
            }),
        }
    }

    fn resume(
        &mut self,
        _out: &mut dyn Sink<'mcx>,
        _estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        unreachable!("GroupOp never pends")
    }
}

/// Try to let the lane own a `Group` over the sort breaker — streaming
/// adjacent-row grouping on the sorted emit. `None` = refused; `exec_group`
/// drives the same GroupState byte-safely at any call boundary.
#[inline]
pub fn try_own_group<'mcx>(
    g: &mut ::mcx::PgBox<'mcx, crate::procnode::GroupNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic per-call gates (Group init asserts !BACKWARD && !MARK, so a
    // non-forward pull should be impossible — gate anyway, like the sort).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::Group, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(ShapeClass::Group, RefuseReason::Backward);
        return Ok(None);
    }
    let g = &mut **g;
    // Sorted grouping's input comes from a Sort in the lane-ownable plans
    // (a presorted index path arrives as a standalone scan, which refuses
    // ownership — C's shape there IS the Volcano Group). Instrumented trees
    // wrap every node, so EXPLAIN ANALYZE never matches the Sort arm.
    let crate::procnode::PlanStateNode::Sort(s) = &mut g.outer else {
        stats::tick_refused(ShapeClass::Group, RefuseReason::ChildNotLaneOwned);
        return Ok(None);
    };
    if !sort_lane_fusible_memo(s, estate)? {
        stats::tick_refused(ShapeClass::Group, RefuseReason::ChildNotLaneOwned);
        return Ok(None);
    }
    // C's ExecGroup entry interrupt check (conditional, exactly the Volcano
    // entry's), then the drained guard, then the first child pull's ExecSort
    // entry CFI.
    ::nodegroup::lane_group_cfi()?;
    if ::nodegroup::lane_group_done(&g.state) {
        return Ok(Some(None));
    }
    ::postgres_seams::check_for_interrupts::call()?;
    let crate::procnode::SortNode { state: sstate, outer: souter, outer_desc, .. } = s;
    // One OWNED tick per lane-owned group drive start (= the underlying sort
    // feed event; rescan re-feeds and re-ticks, like the sortfeed class) —
    // after the feed, so a feed-time refuse never ticks owned.
    let feeding = !sstate.sort_done();
    if !sort_feed_if_needed(sstate, &mut **souter, outer_desc, None, estate)? {
        return Ok(None);
    }
    if feeding {
        stats::tick_owned(ShapeClass::Group);
    }
    let mut op = GroupOp { group: &mut g.state };
    let mut root = RootAdapter::new(None);
    let r =
        pull_step_chain(sstate, &mut SortEmitSource, &mut SortEmit, &mut op, &mut root, estate)?;
    if r.is_none() {
        // exec_group's child-exhausted arm: the node stays drained.
        ::nodegroup::lane_group_eof(&mut g.state);
    }
    Ok(Some(r))
}

/// The Result node's per-row projection as a mid-pipeline streaming operator:
/// one child row in, exactly one projected row out (Result has no per-row
/// qual — C's ExecResult projects every child row). Never pends, never
/// finishes early.
struct ResultOp<'a, 'mcx> {
    ps: &'a mut crate::procnode::PlanStateBase<'mcx>,
}

impl<'mcx> TupleOp<'mcx> for ResultOp<'_, 'mcx> {
    fn pending(&self) -> bool {
        false
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        // exec_result's per-call body over one pushed child row: per-tuple
        // context reset, stage the outer tuple, project (param hoist +
        // subplan-aware arm inside the seam).
        let ecxt = self.ps.ps_ExprContext.expect("ResultState without ExprContext");
        estate.reset_expr_context(ecxt);
        estate.ecxt_mut(ecxt).ecxt_outertuple = Some(tuple);
        let result = crate::noderesult::lane_result_project(self.ps, estate)?;
        Ok(match out.accept(result, estate)? {
            SinkFeed::Full => OpStatus::Paused,
            SinkFeed::NeedMore => OpStatus::NeedInput,
        })
    }

    fn resume(
        &mut self,
        _out: &mut dyn Sink<'mcx>,
        _estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        unreachable!("ResultOp never pends")
    }
}

/// Try to let the lane own a `Result`: the no-FROM single-row arm (degenerate
/// no-child pipeline), or the projection stream over the sort breaker. The
/// one-time `resconstantqual` gate runs BEFORE the child is ever fed, via
/// `noderesult::lane_result_gate` — C's rs_checkqual arm verbatim, so a
/// refusal after the gate ran is still byte-safe (`exec_result` sees the
/// same consumed rs_checkqual / rs_done state its own first call would have
/// left).
#[inline]
pub fn try_own_result<'mcx>(
    rs: &mut crate::noderesult::ResultState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic per-call gates.
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::ResultNode, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(ShapeClass::ResultNode, RefuseReason::Backward);
        return Ok(None);
    }
    // EXPLAIN ANALYZE refuses by policy (§4). The no-FROM arm has no child
    // whose Instrumented wrapper would break the match, so gate on the
    // estate's instrumentation table directly (non-empty exactly when the
    // plan is instrumented).
    if !estate.es_instrumentation.is_empty() {
        stats::tick_refused(ShapeClass::ResultNode, RefuseReason::Instrumented);
        return Ok(None);
    }
    match rs.outer.as_deref_mut() {
        None => {
            // The no-FROM row: exec_result's childless body, statement for
            // statement (entry CFI → one-time gate → per-call ctx reset →
            // drained guard → mark done + project the single row).
            crate::cfi()?;
            // One OWNED tick per lane-owned Result execution: the call that
            // consumes the gate and/or emits; the drained tail calls after it
            // don't re-tick.
            if rs.rs_checkqual || !rs.rs_done {
                stats::tick_owned(ShapeClass::ResultNode);
            }
            if rs.rs_checkqual && !crate::noderesult::lane_result_gate(rs, estate)? {
                return Ok(Some(None));
            }
            let ecxt = rs.ps.ps_ExprContext.expect("ResultState without ExprContext");
            estate.reset_expr_context(ecxt);
            if rs.rs_done {
                return Ok(Some(None));
            }
            rs.rs_done = true;
            Ok(Some(Some(crate::noderesult::lane_result_project(&mut rs.ps, estate)?)))
        }
        Some(crate::procnode::PlanStateNode::Sort(_)) => {
            // Child admission BEFORE any state effect. (Instrumented trees
            // wrap every node, so an instrumented Sort never matches — the
            // estate gate above already refused anyway.)
            {
                let Some(crate::procnode::PlanStateNode::Sort(s)) = rs.outer.as_deref_mut()
                else {
                    unreachable!("matched above")
                };
                if !sort_lane_fusible_memo(s, estate)? {
                    stats::tick_refused(ShapeClass::ResultNode, RefuseReason::ChildNotLaneOwned);
                    return Ok(None);
                }
            }
            // exec_result entry: CFI, then the one-time gate (C evaluates it
            // before the child is ever pulled; false = the sort is never fed).
            crate::cfi()?;
            if rs.rs_checkqual && !crate::noderesult::lane_result_gate(rs, estate)? {
                return Ok(Some(None));
            }
            if rs.rs_done {
                return Ok(Some(None));
            }
            let crate::noderesult::ResultState { ps, outer, .. } = rs;
            let Some(crate::procnode::PlanStateNode::Sort(s)) = outer.as_deref_mut() else {
                unreachable!("matched above")
            };
            // C's first child pull enters ExecSort: entry CFI, then the feed.
            ::postgres_seams::check_for_interrupts::call()?;
            let crate::procnode::SortNode { state: sstate, outer: souter, outer_desc, .. } = s;
            // One OWNED tick per lane-owned Result child-feed event — after
            // the feed, so a feed-time refuse never ticks owned.
            let feeding = !sstate.sort_done();
            if !sort_feed_if_needed(sstate, &mut **souter, outer_desc, None, estate)? {
                return Ok(None);
            }
            if feeding {
                stats::tick_owned(ShapeClass::ResultNode);
            }
            let mut op = ResultOp { ps };
            let mut root = RootAdapter::new(None);
            Ok(Some(pull_step_chain(
                sstate,
                &mut SortEmitSource,
                &mut SortEmit,
                &mut op,
                &mut root,
                estate,
            )?))
        }
        Some(_) => {
            stats::tick_refused(ShapeClass::ResultNode, RefuseReason::ChildNotLaneOwned);
            Ok(None)
        }
    }
}

/// The SubqueryScan node as a mid-pipeline streaming operator: one subplan
/// row in, 0..1 filtered/projected rows out, via `execscan::lane_scan_accept`
/// — `exec_scan_impl`'s per-tuple qual/projection body (subplan/param arms
/// included), over the same node state (`ss_ScanTupleSlot` repointed at the
/// subplan's slot exactly as `SubqueryNext` does). Never pends, never
/// finishes early.
struct SubqueryScanOp<'a, 'mcx> {
    ss: &'a mut ::execscan::ScanState<'mcx>,
}

impl<'mcx> TupleOp<'mcx> for SubqueryScanOp<'_, 'mcx> {
    fn pending(&self) -> bool {
        false
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        // exec_scan_fetch's conditional per-tuple interrupt check (§9: same
        // cadence as the per-tuple driver this replaces).
        if ::init_small::globals::InterruptPending() {
            ::postgres_seams::check_for_interrupts::call()?;
        }
        // SubqueryNext: the subplan's slot goes to the driver uncopied.
        self.ss.ss_ScanTupleSlot = tuple;
        match ::execscan::lane_scan_accept(self.ss, estate, tuple)? {
            None => Ok(OpStatus::NeedInput),
            Some(result) => Ok(match out.accept(result, estate)? {
                SinkFeed::Full => OpStatus::Paused,
                SinkFeed::NeedMore => OpStatus::NeedInput,
            }),
        }
    }

    fn resume(
        &mut self,
        _out: &mut dyn Sink<'mcx>,
        _estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        unreachable!("SubqueryScanOp never pends")
    }
}

/// Try to let the lane own a bare `SubqueryScan` over the sort breaker —
/// the pass-through filter/project stream on the sorted emit. `None` =
/// refused; `exec_scan` drives the same ScanState byte-safely.
#[inline]
pub fn try_own_subquery_scan<'mcx>(
    s: &mut ::mcx::PgBox<'mcx, crate::procnode::SubqueryScanNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic per-call gates. EPQ substitutes test tuples in the fetch
    // (exec_scan_epq); the lane refuses it wholesale (§4).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::SubqueryScan, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(ShapeClass::SubqueryScan, RefuseReason::Backward);
        return Ok(None);
    }
    let s = &mut **s;
    if s.ss.instr_idx.is_some() {
        stats::tick_refused(ShapeClass::SubqueryScan, RefuseReason::Instrumented);
        return Ok(None);
    }
    let crate::procnode::PlanStateNode::Sort(sort) = &mut *s.subplan else {
        stats::tick_refused(ShapeClass::SubqueryScan, RefuseReason::ChildNotLaneOwned);
        return Ok(None);
    };
    if !sort_lane_fusible_memo(sort, estate)? {
        stats::tick_refused(ShapeClass::SubqueryScan, RefuseReason::ChildNotLaneOwned);
        return Ok(None);
    }
    // C's first fetch: exec_scan_fetch's conditional CFI runs per tuple in
    // the TupleOp; the subplan pull enters ExecSort — entry CFI here.
    ::postgres_seams::check_for_interrupts::call()?;
    let crate::procnode::SortNode { state: sstate, outer: souter, outer_desc, .. } = sort;
    // One OWNED tick per lane-owned feed event (the child sort feed) — after
    // the feed, so a feed-time refuse never ticks owned.
    let feeding = !sstate.sort_done();
    if !sort_feed_if_needed(sstate, &mut **souter, outer_desc, None, estate)? {
        return Ok(None);
    }
    if feeding {
        stats::tick_owned(ShapeClass::SubqueryScan);
    }
    // End-of-stream mirrors exec_scan_impl's projected-slot clear.
    let clear_on_finish = s.ss.ps_ProjInfo.as_ref().map(|p| p.pi_result_slot);
    let mut op = SubqueryScanOp { ss: &mut s.ss };
    let mut root = RootAdapter::new(clear_on_finish);
    Ok(Some(pull_step_chain(
        sstate,
        &mut SortEmitSource,
        &mut SortEmit,
        &mut op,
        &mut root,
        estate,
    )?))
}

/// Feed pipeline for the agg-over-subquery composition: lane scan source →
/// scalar filter/project → SubqueryScanOp (pass-through filter/project) →
/// the hash-agg breaker sink, to exhaustion — dispatched over the admitted
/// scan child types (join-probe-drain shape).
fn subquery_feed_drain_dispatch<'mcx>(
    sqs: &mut crate::procnode::SubqueryScanNode<'mcx>,
    sink: &mut dyn Sink<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let crate::procnode::SubqueryScanNode { ss, subplan } = sqs;
    let mut op = SubqueryScanOp { ss };
    match &mut **subplan {
        crate::procnode::PlanStateNode::SeqScan(ss2) => drain_pipeline_chain(
            ss2,
            &mut SeqScanSource,
            &mut SeqScanFilterProject,
            &mut op,
            sink,
            estate,
        ),
        crate::procnode::PlanStateNode::IndexScan(is) => {
            drain_pipeline_chain(is, &mut IndexScanSource, &mut IndexScanEmit, &mut op, sink, estate)
        }
        crate::procnode::PlanStateNode::IndexOnlyScan(ios) => drain_pipeline_chain(
            &mut **ios,
            &mut IndexOnlyScanSource,
            &mut IndexOnlyScanEmit,
            &mut op,
            sink,
            estate,
        ),
        crate::procnode::PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            if !b.scan.initialized {
                crate::procnode::bitmap_table_scan_setup_dispatch(b, estate)?;
            }
            drain_pipeline_chain(
                &mut b.scan,
                &mut BitmapHeapScanSource,
                &mut BitmapHeapScanEmit,
                &mut op,
                sink,
                estate,
            )
        }
        _ => unreachable!("composition admitted a non-scan subquery child"),
    }
}

/// Try to let the lane own `Agg(hashed) → SubqueryScan → scan` — lane
/// pipelines chaining through a subquery boundary. Two pipelines on one
/// breaker node:
///
///   1. build: scan source → filter/project → SubqueryScanOp → HashAggBuildSink
///   2. emit:  HashAggSource → HashAggEmit → RootAdapter (one group per pull)
///
/// The agg reads the SUBQUERY's output slot (not the scan slot), so the
/// lanefold/SoA fold feed does not apply — the build is the per-row breaker
/// feed. No admission-economics refuse is needed: the legacy fused
/// `exec_agg_batched` arms never match a SubqueryScan outer, so there is no
/// faster drive to preempt. `None` = refused (the caller falls to the
/// per-tuple `exec_agg` over `exec_scan`, byte-identically).
#[inline]
pub fn try_own_agg_over_subquery_scan<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    sqs: &mut ::mcx::PgBox<'mcx, crate::procnode::SubqueryScanNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic per-call gates, ticked under the subqueryscan class (the
    // composition's feed hangs off the subquery's drive).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::SubqueryScan, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(ShapeClass::SubqueryScan, RefuseReason::Backward);
        return Ok(None);
    }
    if !::nodeagg::agg_hash_breaker_admissible(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AggNotDrainable);
        return Ok(None);
    }
    let sqs = &mut **sqs;
    if sqs.ss.instr_idx.is_some() {
        stats::tick_refused(ShapeClass::SubqueryScan, RefuseReason::Instrumented);
        return Ok(None);
    }
    // The subquery's child must be a lane-fusible scan (the Phase-1 refuse-
    // sets, verbatim; the specific child reason ticks under its class).
    if let Some(r) = scan_child_fusible(&mut sqs.subplan, estate)? {
        stats::tick_refused(ShapeClass::SubqueryScan, r);
        return Ok(None);
    }
    // exec_agg's top-of-call guard: a drained agg stays drained.
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    // Build phase (once, lazily): drain the scan → subquery chain into the
    // breaker sink, then finalize (delegated). `table_filled` is the phase
    // flag; a rescan rebuild clears it and re-enters here.
    if !::nodeagg::agg_hash_table_filled(agg) {
        // One OWNED tick per lane-owned build event, on both classes the
        // event engages (aggbuild counts builds; subqueryscan counts feeds).
        stats::tick_owned(ShapeClass::AggBuild);
        stats::tick_owned(ShapeClass::SubqueryScan);
        {
            let mut agg_sink = HashAggBuildSink { agg: &mut *agg };
            subquery_feed_drain_dispatch(sqs, &mut agg_sink, estate)?;
        }
        // End-of-scan parity with exec_scan_impl: the projected slot is
        // cleared when the subquery's stream ends (byte-invisible; keeps the
        // node state identical to the per-tuple driver's).
        if let Some(p) = sqs.ss.ps_ProjInfo.as_ref() {
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(p.pi_result_slot), mcx);
        }
    }
    // Emit phase (every call): one qual-passing group per PG pull, in C's
    // retrieve order.
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step(agg, &mut HashAggSource, &mut HashAggEmit, &mut root, estate)?))
}

// ===========================================================================
// Agg-over-Gather hosting (lane-v2-aggovergather): the leader-side
// HashAggregate above a Gather — the plan shape the planner picks when
// partial-aggregation costing does not win (common at 10M+: many-group
// GROUP BYs) — as a lane breaker build fed by the GATHER MACHINERY AS A
// SOURCE. The workers stay row-path (they only scan/filter/project into the
// shm_mq); the leader's half becomes lane pipelines on the one breaker node:
//
//   1. build: exec_gather (REUSED VERBATIM, per pull: worker launch,
//      round-robin nowait queue reads, leader participation, projection) →
//      staged fold sink / per-row breaker sink
//   2. emit:  HashAggSource → HashAggEmit → RootAdapter (one group per pull)
//
// The Append house rule applies: the node's OWN drive body is reused, not
// reimplemented — worker launch/teardown, tqueue reads, latch waits,
// leader-participation pulls (`exec_proc_node` on the partial plan — the
// leader's local child stays row-path; parallel-aware scans refuse the lane
// via the parallel gate), deferred-rescan chgParam, and the per-pull CFI are
// all `exec_gather`'s. Only the consumer changes: each returned slot feeds
// the breaker sink instead of returning through the Volcano boundary, so the
// agg consumes C's rows in C's arrival order and the built table is
// byte-identical to `exec_agg` over `exec_gather`'s.
//
// Feed choice mirrors the agg-over-join composition: the staged fold feed
// (`StagedFoldAggSink` — batched transition folds; K2's deferred batched
// probe when the grouping key is single and kernel-hostable) when the agg
// carries a lanefold plan, the per-row `HashAggBuildSink` otherwise. Staged
// by-ref values are copied into the per-batch arena at accept, so the
// funnel slot's transport-lifetime tuple (live only until the next queue
// receive) is never held across rows.
//
// Refuse-set (each byte-safe on the Volcano fallback):
//   * agg-side: `agg_hash_breaker_admissible`, verbatim (grouping sets /
//     DISTINCT / ordered-set / merge-phase — notably the parallel FINALIZE
//     half of a partial-agg plan, whose AGGSPLIT deserialization the breaker
//     does not own; ticked under aggbuild per offered call).
//   * dynamic EPQ / non-forward pulls (§4 model-incompatible; per call).
//   * GatherMerge stays Volcano: the planner puts a hash agg above
//     GatherMerge only when the merge order is useful elsewhere — no such
//     CB shape exists; a sorted GroupAggregate over GatherMerge is a
//     different (sorted-agg) breaker and refuses via the dispatch match.
//   * kill switch `PGRUST_LANE_V2_AGGGATHER=0` (A/B tooling; default ON).
//
// EXPLAIN is unchanged (no planner surface); EXPLAIN ANALYZE trees wrap
// every node in the `Instrumented` variant and never reach the hook.
// ===========================================================================

/// Agg-over-Gather kill switch: on by default under the lane;
/// `PGRUST_LANE_V2_AGGGATHER=0`/`off` forces the Volcano fallback.
fn agg_gather_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_LANE_V2_AGGGATHER").as_deref(), Ok("0") | Ok("off"))
    })
}

/// Drain the gather stream to exhaustion into a breaker sink — pipeline 1's
/// driver with `exec_gather` as the source (the node's own drive, reused
/// verbatim; its per-pull CFI is the loop's interrupt cadence). `finish`
/// runs the sink's finalize tail (staged flush + build finalize).
fn gather_drain<'mcx>(
    g: &mut crate::procnode::GatherNode<'mcx>,
    sink: &mut dyn Sink<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    while let Some(slot) = crate::nodegather::exec_gather(&mut g.state, &mut g.outer, estate)? {
        let fed = sink.accept(slot, estate)?;
        debug_assert_eq!(fed, SinkFeed::NeedMore, "a breaker sink consumes its whole input");
        let _ = fed;
    }
    sink.finish(estate)
}

/// Build phase of the agg-over-gather composition, once, lazily: drain the
/// gather stream into the breaker sink (staged fold feed when the agg
/// carries a fold plan; the per-row sink otherwise), then finalize
/// (delegated). `table_filled` is the phase flag; a rescan rebuild clears it
/// (`exec_rescan_gather` reset the gather side, workers relaunch on the
/// first pull) and re-enters here. Shared by the bare composition hook and
/// the Sort-/Limit-over-agg chains. Unlike the join composition there is no
/// feed-time refuse: the gather stream has no spill analog, so the build
/// always completes.
fn agg_gather_build_if_needed<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    g: &mut crate::procnode::GatherNode<'mcx>,
    stage_slot: &mut Option<ExecSlotId>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if ::nodeagg::agg_hash_table_filled(agg) {
        return Ok(());
    }
    // One OWNED tick per lane-owned build event, on both classes the event
    // engages (aggbuild counts builds; gather counts feeds).
    stats::tick_owned(ShapeClass::AggBuild);
    stats::tick_owned(ShapeClass::Gather);
    // The gather's output slot: the projected result slot when the Gather
    // carries a projection, else the funnel slot (worker rows; leader-local
    // rows arrive in the leader plan's own slot with the same descriptor —
    // the sinks deform from the slot each accept).
    let out_slot = g.state.ps.ps_ResultTupleSlot.unwrap_or(g.state.funnel_slot);
    match staged_feed_shape(agg, out_slot, estate) {
        Some(shape) => {
            trace_feed(match shape.mode {
                StagedMode::Guarded => "agg-over-gather: staged fold feed engaged (guarded)",
                StagedMode::K2 { .. } => "agg-over-gather: staged fold feed engaged (k2 probe)",
                StagedMode::Mk => "agg-over-gather: staged fold feed engaged (mk probe)",
                StagedMode::Arrival => "agg-over-gather: staged fold feed engaged",
            });
            let mut sink = StagedFoldAggSink::new(agg, out_slot, stage_slot, shape, estate);
            gather_drain(g, &mut sink, estate)
        }
        None => {
            trace_feed("agg-over-gather: per-row sink (no fold plan)");
            let mut sink = HashAggBuildSink { agg };
            gather_drain(g, &mut sink, estate)
        }
    }
}

/// Try to let the lane own `Agg(hashed) → Gather → (row-path parallel
/// workers)` — the leader-side aggregation shape (see the section header for
/// the model, the reuse rule, and the refuse-set). `None` = refused (the
/// caller falls to the per-tuple `exec_agg` over `exec_gather`,
/// byte-identically).
#[inline]
pub fn try_own_agg_over_gather<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    g: &mut ::mcx::PgBox<'mcx, crate::procnode::GatherNode<'mcx>>,
    stage_slot: &mut Option<ExecSlotId>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if !agg_gather_enabled() {
        return Ok(None);
    }
    // Dynamic per-call gates, ticked under the gather class (the
    // composition's feed hangs off the gather's drive).
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::Gather, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(ShapeClass::Gather, RefuseReason::Backward);
        return Ok(None);
    }
    if !::nodeagg::agg_hash_breaker_admissible(agg) {
        stats::tick_refused(ShapeClass::AggBuild, RefuseReason::AggNotDrainable);
        return Ok(None);
    }
    // exec_agg's top-of-call guard: a drained agg stays drained.
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    agg_gather_build_if_needed(agg, &mut **g, stage_slot, estate)?;
    // Emit phase (every call): one qual-passing group per PG pull, in C's
    // retrieve order.
    let mut root = RootAdapter::new(None);
    Ok(Some(pull_step(agg, &mut HashAggSource, &mut HashAggEmit, &mut root, estate)?))
}

// ===========================================================================
// Append hosting (wave 5, 2026-07-12): the serial Append as a lane
// concatenation point — the node's OWN `exec_append` body drives, verbatim
// (subplan choice, `as_begun`, runtime pruning via `choose_next_subplan_
// locally`/`identify_valid_subplans`, and the conditional per-fetch CFI are
// all C's, reused not reimplemented — the wave-4 house rule); only the
// `fetch_subplan` closure changes, pulling one tuple per fetch from the
// CHILD's lane pipeline (`pull_step` over the Phase-1 scan stages) instead of
// `exec_proc_node`. Child N's pipeline exhausting returns `None` to
// `exec_append`, which advances to child N+1 — C's exact
// child-EOF-then-advance order for free. Each child's cross-call position
// (staged page batch + cursor) is node-resident, so the one-tuple-per-pull
// Volcano boundary is safe, and each child's output slot goes to the parent
// exactly as `exec_append` would hand it (Append projects nothing; children
// with differing physical descs already carry their own planner-installed
// projections, which run inside the child pipelines — byte-identical).
//
// Refuse-set (each byte-safe on the Volcano fallback):
//   * parallel Append (Leader/Worker choosers over the shared DSM claim
//     table) — non-serial subplan order; the lane refuses anything not
//     provably ordering-identical serially (`lane_choose_local`). Ticked per
//     offered call (the mode is worker/DSM-init-assigned).
//   * async-capable subplans — unported (`exec_init_append` panics), so no
//     gate is needed; recorded here for the C-diff reader.
//   * dynamic EPQ / non-forward pulls (§4 model-incompatible; per call).
//   * any child that is not a lane-fusible Phase-1 scan
//     (`scan_child_fusible`, verbatim — the child's specific refusal reason
//     ticks under the child's class). v1 policy: MIXED children refuse the
//     WHOLE Append — a per-child owned/Volcano split would need per-child
//     verdict pinning across the shared `exec_append` drive for no measured
//     upside; future work when a real mixed shape shows up.
//
// Runtime partition pruning is ADMITTED: the pruning arms run inside the
// reused `exec_append`/`choose_next_subplan_locally` body itself, so the
// subplan order is C's by construction. The structural verdict conservatively
// probes ALL initialized children (a superset of what pruning may run) —
// probing opens each child scan's descriptor once, which C's lazy first-pull
// open would skip for pruned/LIMIT-cut children: pgstat-only divergence,
// same accepted class as the hash-join build-side probe (design §9 F5).
// ===========================================================================

/// One PG pull's worth from a lane-owned scan child pipeline — the
/// `fetch_subplan` face of the hosted Append (join_probe_pull_dispatch's
/// shape, without a mid-pipeline op). The child's staged batch + cursor are
/// node-resident, so consecutive fetches resume exactly.
fn lane_scan_pull_dispatch<'mcx>(
    child: &mut crate::procnode::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    match child {
        crate::procnode::PlanStateNode::SeqScan(ss) => {
            // End-of-stream mirrors ExecScanExtended's projected-slot clear
            // (try_own_seq_scan's shape).
            let clear_on_finish = ss.ss.ps_ProjInfo.as_ref().map(|p| p.pi_result_slot);
            let mut root = RootAdapter::new(clear_on_finish);
            pull_step(ss, &mut SeqScanSource, &mut SeqScanFilterProject, &mut root, estate)
        }
        crate::procnode::PlanStateNode::IndexScan(is) => {
            let mut root = RootAdapter::new(None);
            pull_step(is, &mut IndexScanSource, &mut IndexScanEmit, &mut root, estate)
        }
        crate::procnode::PlanStateNode::IndexOnlyScan(ios) => {
            let mut root = RootAdapter::new(None);
            pull_step(&mut **ios, &mut IndexOnlyScanSource, &mut IndexOnlyScanEmit, &mut root, estate)
        }
        crate::procnode::PlanStateNode::BitmapHeapScan(b) => {
            let b = &mut **b;
            if !b.scan.initialized {
                crate::procnode::bitmap_table_scan_setup_dispatch(b, estate)?;
            }
            let mut root = RootAdapter::new(None);
            pull_step(&mut b.scan, &mut BitmapHeapScanSource, &mut BitmapHeapScanEmit, &mut root, estate)
        }
        _ => unreachable!("memoized append verdict admitted a non-scan child"),
    }
}

/// Structural Append verdict, memoized on the node at first offer (verdict
/// stability: a lane-driven child carries a staged-batch cursor across the
/// Volcano boundary, so ownership must not flip mid-stream; the child scan
/// verdicts are themselves memoized). ALL children must pass the Phase-1
/// scan refuse-sets — mixed children refuse the whole Append (v1 policy,
/// module doc). Owned accounting ticks exactly here — once per memoized
/// admission (per Append node per (re)init, the seqscan class cadence).
fn append_lane_fusible_memo<'mcx>(
    a: &mut crate::procnode::AppendNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if let Some(v) = a.lane_fusible {
        return Ok(v);
    }
    let mut refuse: Option<RefuseReason> = None;
    for child in a.substates.iter_mut() {
        if let Some(r) = scan_child_fusible(child, estate)? {
            refuse = Some(r);
            break;
        }
    }
    match refuse {
        None => stats::tick_owned(ShapeClass::Append),
        Some(r) => stats::tick_refused(ShapeClass::Append, r),
    }
    let v = refuse.is_none();
    a.lane_fusible = Some(v);
    Ok(v)
}

/// Try to let the lane own a serial `Append` over lane-fusible scan children.
/// `Some` = the lane drove this call (via the node's own `exec_append` body
/// over lane child pipelines); `None` = refused (caller runs the unchanged
/// `exec_append` over `exec_proc_node` children, byte-identically).
#[inline]
pub fn try_own_append<'mcx>(
    a: &mut ::mcx::PgBox<'mcx, crate::procnode::AppendNode<'mcx>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Dynamic per-call gates (mirror the sort/join breakers). Backward local
    // Append pulls exist in C only under BACKWARD eflags, which the children
    // refuse structurally (ScrollMark) — gate anyway.
    if estate.es_epq_active {
        stats::tick_refused(ShapeClass::Append, RefuseReason::Epq);
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        stats::tick_refused(ShapeClass::Append, RefuseReason::Backward);
        return Ok(None);
    }
    let a = &mut **a;
    // Parallel Append: the Leader/Worker choosers claim subplans through the
    // shared DSM table in a non-serial order — Volcano keeps it. The mode is
    // assigned at DSM/worker init (before the node's first pull), but gate
    // per call (one flag load) rather than memoize an init-order assumption.
    if !::nodeappend::lane_choose_local(&a.state) {
        stats::tick_refused(ShapeClass::Append, RefuseReason::ParallelGate);
        return Ok(None);
    }
    if !append_lane_fusible_memo(a, estate)? {
        return Ok(None);
    }
    let crate::procnode::AppendNode { state, substates, .. } = a;
    Ok(Some(::nodeappend::exec_append(state, estate, |e, i| {
        lane_scan_pull_dispatch(&mut substates[i], e)
    })?))
}

// ===========================================================================
// ProjectSet: DOCUMENTED WHOLESALE REFUSE (wave-5 evaluation, 2026-07-12).
//
// Verdict: do NOT host. The SRF tlist expansion is per-tuple stateful in
// three ways the lane would have to carry, for zero engagement:
//   * the multi-call protocol itself — `pending_srf_tuples` resumes a
//     half-emitted expansion across `exec_proc_node` calls, `args_valid`
//     pins evaluated arg datums across those calls (query-context armed),
//     and `elemdone` tracks per-element ExprMultipleResult state;
//   * SFRM_Materialize mode parks the whole set in a tuplestore read back
//     one row per call — a second, per-element cross-call cursor;
//   * `ExecProjectSRF` interleaves per-tuple context resets between (not
//     within) expansions — a batched drive would need the exact reset
//     points replayed to keep by-ref datum lifetimes identical.
// An expanding-`TupleOp` hosting (the join-probe pause/resume shape over
// `pending_srf_tuples`) is model-compatible in principle, but it could only
// chain over a lane-owned child pipeline — and ProjectSet children in
// practice are bare scans, which refuse standalone ownership (admission
// economics, STANDALONE_SCAN_NO_UPSIDE), so the hook would engage nowhere.
// Reusing `exec_project_set`'s own body per-tuple would add a lane layer
// over a refused child — exactly the shape §4's economics forbid. Refuse,
// and re-evaluate when the design's "SRFs = expanding operator" phase item
// lands (design doc §4 "Everything else is hostable, staged deliberately").
// ===========================================================================

/// Tick the documented ProjectSet wholesale refuse (module doc above; the
/// `project_set_arm` dispatch hook calls this and always falls through to
/// the unchanged `exec_project_set`).
#[inline]
pub fn refuse_project_set() {
    stats::tick_refused(ShapeClass::ProjectSet, RefuseReason::SrfSetExpansion);
}

// ===========================================================================
// Lane-v2 parallel exact-DISTINCT partials (lane-v2-pardistinct;
// nodeagg/src/pardistinct.rs holds the builder/wire/merge machinery and the
// module-level byte-identity argument). The planner cannot emit a Partial
// Agg for a DISTINCT aggregate (hasNonPartialAggs), so the parallel plan is
// always `Agg ← GatherMerge ← Sort ← ParallelSeqScan`. The LEADER arm below
// owns the Agg node: it registers a build recipe keyed by the Sort plan
// node's address (the merge.rs handoff-registry pattern), launches the
// workers, builds its own partial over the local fragment (leader
// participation on the shared claim), folds any stray queue rows
// (degraded/refused workers emit ordinary sorted rows), merges the handed
// tables (partition-parallel where everything fits in memory), and emits
// through the serial arms' unchanged finalize tails. The WORKER arm hooks
// the Sort fragment: a registry hit replaces the sort with the partial
// build; refusal or budget-crossing keeps/resumes the classic sort feed.
// ===========================================================================

/// `PGRUST_LANE_V2_PARDISTINCT` kill switch (default ON inside the lane).
fn pardistinct_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_LANE_V2_PARDISTINCT").as_deref(), Ok("0") | Ok("off"))
    })
}

/// `PGRUST_LANE_V2_PARDISTINCT_FORCE=1`: skip the planner-estimate
/// economics (e2e harness lever; the runtime freeze/evict still bounds
/// memory).
fn pardistinct_force() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        matches!(
            std::env::var("PGRUST_LANE_V2_PARDISTINCT_FORCE").as_deref(),
            Ok("1") | Ok("on")
        )
    })
}

/// The worker-side build sink: rows feed the partial table; crossing the
/// worker budget freezes + installs the table and degrades the REMAINDER to
/// the plan's real Sort (classic sorted-row emission — pre-freeze rows ride
/// the frozen table, post-freeze rows ride the queue; disjoint, exact).
struct PdWorkerSink<'a, 'mcx> {
    builder: Option<::nodeagg::PdBuilder<'mcx>>,
    handoff: &'a std::sync::Arc<::nodeagg::PdHandoff>,
    sort: &'a mut ::nodesort::SortState<'mcx>,
    outer_desc: std::rc::Rc<::types_tuple::TupleDescData<'static>>,
    tmp: ::executils::EcxtId,
    /// Reset per row only when a bytes set detoasts into per-tuple memory.
    reset_tmp: bool,
    degraded: bool,
}

impl<'mcx> Sink<'mcx> for PdWorkerSink<'_, 'mcx> {
    fn accept(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<SinkFeed> {
        if self.degraded {
            ::nodesort::sort_lane_put(self.sort, estate, tuple)?;
            return Ok(SinkFeed::NeedMore);
        }
        let crossed = {
            let b = self.builder.as_mut().expect("undegraded sink holds the builder");
            b.accept(estate, tuple, self.tmp)? == ::nodeagg::PdFeed::Crossed
        };
        if self.reset_tmp {
            estate.reset_expr_context(self.tmp);
        }
        if crossed {
            self.degrade_impl()?;
        }
        Ok(SinkFeed::NeedMore)
    }

    fn finish(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        if self.degraded {
            ::nodesort::sort_lane_finish(self.sort, estate)
        } else {
            // Whole share absorbed: install the frozen table; the fragment
            // emits nothing.
            let t = self.builder.take().expect("undegraded sink holds the builder").freeze()?;
            self.handoff.install(t);
            Ok(())
        }
    }
}

impl<'mcx> BatchSink<'mcx> for PdWorkerSink<'_, 'mcx> {}

impl PdWorkerSink<'_, '_> {
    #[cold]
    #[inline(never)]
    fn degrade_impl(&mut self) -> PgResult<()> {
        trace_feed("pardistinct worker partial frozen; degrading remainder to classic sort");
        let t = self.builder.take().expect("crossing fires on the live builder").freeze()?;
        self.handoff.install(t);
        ::nodesort::sort_lane_begin(self.sort, self.outer_desc.clone())?;
        self.degraded = true;
        Ok(())
    }
}

/// Worker-fragment hook (procnode `sort_arm`, BEFORE the bare-sort
/// breaker): a parallel worker whose fragment top is a leader-registered
/// Sort builds the parallel-DISTINCT partial instead of sorting.
/// `Ok(Some(None))` = table handed off, fragment emits nothing;
/// `Ok(None)` = not engaged (or degraded — the classic sort emit resumes
/// over the fed tuplesort).
#[inline]
pub fn try_pardistinct_worker_sort<'mcx>(
    s: &mut crate::procnode::SortNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    match s.pd_state {
        Some(true) => return Ok(Some(None)),
        Some(false) => return Ok(None),
        None => {}
    }
    // Cheap static rejects before any registry probe.
    if !pardistinct_enabled()
        || !::parallel::IsParallelWorker()
        || !::nodeagg::pd_registry_nonempty()
    {
        return Ok(None);
    }
    let key = s.state.plan as *const ::types_nodes::plannodes::Sort<'_> as usize;
    let Some(handoff) = ::nodeagg::pd_registry_get(key) else {
        s.pd_state = Some(false);
        return Ok(None);
    };
    if handoff.is_spent() {
        // A rescan relaunch against the original registry snapshot: the
        // fresh leader drive folds our classic sorted rows instead.
        s.pd_state = Some(false);
        return Ok(None);
    }
    // Dynamic / structural guards; refusal emits classic sorted rows, which
    // the leader folds — always sound.
    if estate.es_epq_active
        || estate.es_instrument != 0
        || !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction)
        || s.state.sort_done()
        || s.state.bounded
    {
        s.pd_state = Some(false);
        return Ok(None);
    }
    let crate::procnode::SortNode { state, outer, outer_desc, .. } = s;
    let crate::procnode::PlanStateNode::SeqScan(ss) = &mut **outer else {
        s.pd_state = Some(false);
        return Ok(None);
    };
    let spec = handoff.spec.clone();
    if spec.max_att > outer_desc.as_ref().map_or(0, |d| d.natts) {
        s.pd_state = Some(false);
        return Ok(None);
    }
    trace_feed("pardistinct worker partial build engaged");
    stats::tick_owned(ShapeClass::AggBuild);
    arm_scan_staging(
        ss,
        estate,
        ScanFeedShape::RowFeed { ctx: "pardistinct worker feed", stitch: true },
    )?;
    let outer_desc = outer_desc.as_ref().expect("Sort already ended").clone();
    let tmp = estate.exec_assign_expr_context();
    let reset_tmp = spec.any_bytes_set();
    let budget = spec.worker_budget;
    let mut sink = PdWorkerSink {
        builder: Some(::nodeagg::PdBuilder::new(spec, budget, None)),
        handoff: &handoff,
        sort: state,
        outer_desc,
        tmp,
        reset_tmp,
        degraded: false,
    };
    let dir = estate.es_direction;
    estate.es_direction = ::types_scan::sdir::ForwardScanDirection;
    let fed = drain_pipeline(ss, &mut SeqScanSource, &mut SeqScanFilterProject, &mut sink, estate);
    let degraded = sink.degraded;
    estate.es_direction = dir;
    fed?;
    if degraded {
        // The classic sort emit takes over (sort_done after finish).
        s.pd_state = Some(false);
        return Ok(None);
    }
    s.pd_state = Some(true);
    Ok(Some(None))
}

/// Shared leader drive: launch workers, build the leader's own partial over
/// the local fragment, fold stray queue rows, take the handed tables, and
/// merge. Returns the merged result. The caller has registered `handoff`
/// (keyed by the fragment Sort's plan address) and must deregister after.
fn pd_leader_drive<'mcx>(
    gm: &mut crate::procnode::GatherMergeNode<'mcx>,
    handoff: &std::sync::Arc<::nodeagg::PdHandoff>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<::nodeagg::PdMerged<'mcx>> {
    let spec = handoff.spec.clone();
    // 1. Launch the workers FIRST (they adopt the registry snapshot at
    //    launch), then take over leader participation ourselves.
    crate::nodegathermerge::gather_merge_ensure_launched(&mut gm.state, &mut gm.outer, estate)?;
    gm.state.need_to_scan_locally = false;
    // 2. Leader partial over the local fragment (one more builder on the
    //    shared claim). mcx-backed: crossing evicts sets to spill tapes.
    let tmp = estate.exec_assign_expr_context();
    let reset_tmp = spec.any_bytes_set();
    let budget = spec.worker_budget;
    let mut builder =
        ::nodeagg::PdBuilder::new(spec.clone(), budget, Some(estate.es_query_cxt));
    {
        let crate::procnode::PlanStateNode::Sort(s) = &mut *gm.outer else {
            unreachable!("admission proved the fragment shape")
        };
        let crate::procnode::PlanStateNode::SeqScan(ss) = &mut *s.outer else {
            unreachable!("admission proved the fragment shape")
        };
        arm_scan_staging(
            ss,
            estate,
            ScanFeedShape::RowFeed { ctx: "pardistinct leader feed", stitch: true },
        )?;
        struct PdLeaderSink<'a, 'mcx> {
            builder: &'a mut ::nodeagg::PdBuilder<'mcx>,
            tmp: ::executils::EcxtId,
            reset_tmp: bool,
        }
        impl<'mcx> Sink<'mcx> for PdLeaderSink<'_, 'mcx> {
            fn accept(
                &mut self,
                tuple: ExecSlotId,
                estate: &mut EStateData<'mcx>,
            ) -> PgResult<SinkFeed> {
                // Leader builders never cross (mcx-backed eviction).
                let _ = self.builder.accept(estate, tuple, self.tmp)?;
                if self.reset_tmp {
                    estate.reset_expr_context(self.tmp);
                }
                Ok(SinkFeed::NeedMore)
            }
            fn finish(&mut self, _estate: &mut EStateData<'mcx>) -> PgResult<()> {
                Ok(())
            }
        }
        impl<'mcx> BatchSink<'mcx> for PdLeaderSink<'_, 'mcx> {}
        let mut sink = PdLeaderSink { builder: &mut builder, tmp, reset_tmp };
        let dir = estate.es_direction;
        estate.es_direction = ::types_scan::sdir::ForwardScanDirection;
        let fed =
            drain_pipeline(ss, &mut SeqScanSource, &mut SeqScanFilterProject, &mut sink, estate);
        estate.es_direction = dir;
        fed?;
    }
    // 3. Drain the queues to EOF; stray rows (degraded/refused workers)
    //    fold into the leader builder — order-insensitive, exact.
    loop {
        ::postgres_seams::check_for_interrupts::call()?;
        let Some(slot) =
            crate::nodegathermerge::exec_gather_merge(&mut gm.state, &mut gm.outer, estate)?
        else {
            break;
        };
        let _ = builder.accept(estate, slot, tmp)?;
        if reset_tmp {
            estate.reset_expr_context(tmp);
        }
    }
    // 4. Merge. Fast path: every table in memory and the working set fits
    //    the budget — partition-parallel claim merge on scoped threads.
    //    Slow path: serial fold into the (spill-capable) leader builder.
    let tables = handoff.take_all();
    let handed_bytes: usize = tables.iter().map(|t| t.mem_bytes()).sum();
    let nthreads = ((gm.state.nworkers_launched.max(0) as usize) + 1).clamp(1, 16);
    let plain = spec.nkeys() == 0;
    if !builder.ever_spilled
        && nthreads > 1
        && !tables.is_empty()
        && handed_bytes + builder.mem_bytes() <= budget.saturating_mul(2)
    {
        trace_feed("pardistinct parallel partition merge engaged");
        let mut all = tables;
        all.push(builder.freeze()?);
        let all: Vec<_> = all.into_iter().filter(|t| t.ngroups > 0).collect();
        if plain {
            Ok(::nodeagg::pd_parallel_merge_plain(&spec, all, nthreads))
        } else {
            Ok(::nodeagg::pd_parallel_merge_grouped(&spec, all, nthreads).into_lt())
        }
    } else {
        trace_feed("pardistinct serial merge (spill-capable) engaged");
        for t in &tables {
            builder.merge_handed(t)?;
        }
        Ok(builder.into_merged())
    }
}

/// Shared static admission for both leader arms: fragment shape (GatherMerge
/// passthrough → un-built unbounded Sort → SeqScan), parallel mode, and the
/// per-call dynamic gates. Returns the fragment Sort's plan (the registry
/// key + order-spec source).
fn pd_leader_gates<'mcx>(
    gm: &crate::procnode::GatherMergeNode<'mcx>,
    estate: &EStateData<'mcx>,
) -> Option<&'mcx ::types_nodes::plannodes::Sort<'mcx>> {
    if !pardistinct_enabled() {
        return None;
    }
    if estate.es_epq_active
        || estate.es_instrument != 0
        || !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction)
        || !estate.es_use_parallel_mode
    {
        return None;
    }
    if gm.state.initialized || gm.state.plan.num_workers <= 0 {
        return None;
    }
    // First execution only: a rescan (pei already built) has deferred-rescan
    // and registry-snapshot state this drive does not manage — the classic
    // path (plus spent-handoff worker refusal) owns rescans wholesale.
    if gm.state.pei.is_some() {
        return None;
    }
    // Passthrough GatherMerge only (no projection: worker rows and the
    // fragment share the outer descriptor the spec's attnos index).
    if gm.state.ps.ps_ProjInfo.is_some() {
        return None;
    }
    let crate::procnode::PlanStateNode::Sort(s) = &*gm.outer else {
        return None;
    };
    if s.state.sort_done() || s.state.bounded || s.outer_desc.is_none() {
        return None;
    }
    let crate::procnode::PlanStateNode::SeqScan(_) = &*s.outer else {
        return None;
    };
    Some(s.state.plan)
}

/// Leader arm, grouped shape: `Agg(AGG_SORTED) ← GatherMerge ← Sort ←
/// ParallelSeqScan` with exact-DISTINCT aggregates (ClickBench Q9/Q10).
/// `None` = refused (the unchanged per-tuple agg over exec_gather_merge
/// runs, byte-safely).
#[inline]
pub fn try_own_sorted_distinct_agg_over_gather_merge<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    gm: &mut crate::procnode::GatherMergeNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // Mid-emit resume — BEFORE the dynamic gates (the hashgrouped arm's
    // discipline: the fragment was consumed; nothing may re-pull it).
    if ::nodeagg::agg_hashgroup_emitting(agg) {
        return Ok(Some(hashgroup_emit(agg, estate)?));
    }
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    let Some(sp) = pd_leader_gates(&*gm, estate) else {
        return Ok(None);
    };
    // Narrow-shape admission (the serial arm's multiset-prefix law) +
    // hashgroup admission (integer keys, integer set kinds) + economics.
    let k = ::nodeagg::agg_plan_group_cols(agg).len();
    if k == 0
        || !::nodeagg::agg_sorted_distinct_narrow_admissible(agg)
        || (sp.numCols as usize) <= k
        || sp.sortColIdx.len() < k
    {
        return Ok(None);
    }
    {
        let mut a: Vec<i16> = sp.sortColIdx[..k].to_vec();
        let mut b: Vec<i16> = ::nodeagg::agg_plan_group_cols(agg).to_vec();
        a.sort_unstable();
        b.sort_unstable();
        if a != b {
            return Ok(None);
        }
    }
    if !::nodeagg::agg_hashgroup_admissible(agg)
        // Density/memory economics: the parallel Sort's row estimate is
        // per-worker (conservative for the density tier — a refusal falls
        // back to the byte-identical per-tuple gather-merge path).
        || !::nodeagg::agg_hashgroup_economical(agg, pardistinct_force(), sp.plan.plan_rows)
    {
        return Ok(None);
    }
    let Some(order) = hashgroup_order_spec(agg, sp, k) else {
        return Ok(None);
    };
    let crate::procnode::PlanStateNode::Sort(s) = &*gm.outer else { unreachable!() };
    let desc = s.outer_desc.as_ref().expect("gated non-None").clone();
    let Some(spec) = ::nodeagg::pd_derive_spec(agg, &desc) else {
        return Ok(None);
    };
    // v1 economics: engage the grouped arm only when the DISTINCT sets are
    // the WHOLE transition load (empty vocabulary — the Q9 shape). Measured
    // 2026-07-12 (10m bank, DOP 6): Q9 1.675 -> 0.894s, but the Q10 shape
    // (sum/count/avg companions) REGRESSED 2.14 -> 2.31s — the per-row
    // vocabulary accept underprices the fused classic drives. The batched
    // vocabulary accept is the named follow-up; until then companion-agg
    // shapes keep the classic parallel plan (FORCE overrides for the e2e).
    if !spec.vocab.is_empty() && !pardistinct_force() {
        return Ok(None);
    }
    // Last refusal point passed: arm set-mode (sticky, value-safe; measured
    // 2026-07-12 — arming BEFORE the vocab refusal cost the refused Q10
    // shape ~10% in the classic parallel path, so it must come last).
    ::nodeagg::agg_sorted_force_distinct_set(agg);
    trace_feed("pardistinct grouped leader drive engaged");
    stats::tick_owned(ShapeClass::AggBuild);
    let key = sp as *const ::types_nodes::plannodes::Sort<'_> as usize;
    let handoff = std::sync::Arc::new(::nodeagg::PdHandoff::new(spec.clone()));
    ::nodeagg::pd_registry_insert(key, &handoff);
    let drive = pd_leader_drive(gm, &handoff, estate);
    ::nodeagg::pd_registry_remove(key);
    let merged = drive?;
    ::nodeagg::agg_hashgroup_adopt_merged(agg, estate, merged, &spec.vocab, order)?;
    Ok(Some(hashgroup_emit(agg, estate)?))
}

/// Leader arm, plain shape: `Agg(AGG_PLAIN) ← GatherMerge ← Sort ←
/// ParallelSeqScan` where EVERY transition replays from an exact-DISTINCT
/// set (ClickBench Q5/Q6). `None` = refused.
#[inline]
pub fn try_own_plain_distinct_agg_over_gather_merge<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    gm: &mut crate::procnode::GatherMergeNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }
    if !::nodeagg::agg_plain_distinct_set_only(agg) {
        return Ok(None);
    }
    let Some(sp) = pd_leader_gates(&*gm, estate) else {
        return Ok(None);
    };
    let crate::procnode::PlanStateNode::Sort(s) = &*gm.outer else { unreachable!() };
    let desc = s.outer_desc.as_ref().expect("gated non-None").clone();
    let Some(spec) = ::nodeagg::pd_derive_spec(agg, &desc) else {
        return Ok(None);
    };
    debug_assert_eq!(spec.nkeys(), 0);
    // Last refusal point passed: arm set-mode (the grouped arm's ordering
    // law — a refusal must leave the classic path untouched).
    ::nodeagg::agg_force_distinct_set(agg);
    trace_feed("pardistinct plain leader drive engaged");
    stats::tick_owned(ShapeClass::AggBuild);
    let key = sp as *const ::types_nodes::plannodes::Sort<'_> as usize;
    let handoff = std::sync::Arc::new(::nodeagg::PdHandoff::new(spec.clone()));
    ::nodeagg::pd_registry_insert(key, &handoff);
    let drive = pd_leader_drive(gm, &handoff, estate);
    ::nodeagg::pd_registry_remove(key);
    let merged = drive?;
    if merged.ngroups == 0 {
        // Zero input rows anywhere: the plain finalize's empty-input
        // contract (count = 0, sum NULL) falls out of the untouched init
        // states + empty sets.
        return Ok(Some(::nodeagg::agg_plain_adopt_empty(agg, estate)?));
    }
    Ok(Some(::nodeagg::agg_plain_adopt_merged(agg, estate, merged)?))
}
