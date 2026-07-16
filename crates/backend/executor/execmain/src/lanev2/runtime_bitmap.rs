//! BITMAP-MORSELS — morselized bitmap heap scan on the runtime
//! (bitmap-morsels lane; sibling of the M1 runtime scan arm, whose
//! engage/ceremony/helper machinery this arm REUSES — see
//! `runtime_scan::engage`'s `bitmap_ctx` parameter).
//!
//! Shape: a SERIAL-plan plain Agg over a Bitmap Heap Scan (any table-AM heap
//! relation; the bitmapqual subtree is the ordinary BitmapIndexScan/And/Or
//! tree). The LEADER builds the bitmap ONCE — exactly C's parallel-leader
//! discipline (`BitmapShouldInitializeSharedState` winner) — freezes it with
//! `tbm_prepare_shared_iterate`, and morselizes the HEAP-FETCH phase:
//! granules are frozen ENTRIES (exact pages first, lossy chunks after — the
//! segmented granule space, `TbmRangeIterator`), claims are duration-adaptive
//! granule ranges from the runtime scheduler. Each helper positions its own
//! node's iterator on the claimed window and re-enters the UNCHANGED serial
//! per-row drive (`exec_bitmap_heap_scan`: fetch + recheck + qual +
//! projection, identical per-row programs) feeding
//! `agg_plain_build_accept`; partials ride the ordinary runtime-partial
//! export/combine (order-insensitive-exact kinds only — fail-closed).
//!
//! OWNERSHIP SPLIT (notes/bitmap-morsels-lane.md): this arm owns bitmap
//! BUILD/ITERATE claim structures only. The heap fetch inside a claim is the
//! long-standing serial fetch path (`table_scan_bitmap_next_tuple` lineage),
//! NOT a second morselized TID-fetch stage — the decoupled TID-batch fetch
//! stage + prefetch machinery belong to the index-morsels lane; when theirs
//! lands, prefetch-depth plumbs in through the claimed window.
//!
//! Modes (PGRUST_RUNTIME_BITMAP_MODE, race charter):
//!   A (default) — contiguous window claims over the frozen entry order
//!       (ascending blockno per segment): I/O locality follows the shared
//!       claim frontier.
//!   B — strided claims: a leader-built permutation interleaves S contiguous
//!       segments (PGRUST_RUNTIME_BITMAP_STRIDE), spreading concurrent
//!       claims across the whole relation for NVMe I/O spread.
//!   C (two-stage morselized BUILD) — not yet built; the fetch stage above
//!       is mode-agnostic and C reuses it when chartered in.
//!
//! Arming (all four required; absence of any = byte-identical classic path):
//!   PGRUST_RUNTIME=1                       (pool kill switch, M0)
//!   PGRUST_RUNTIME_BITMAP=1                (this arm's ENABLE, default off)
//!   SET pgrust.runtime_bitmap_pool = <dop> (arming, runtime_pool.rs)
//!   pgrust.lane_executor on                (lane master)
//!
//! Admission floors (fail-closed, traced by name — m5-5-floors philosophy):
//! tiny bitmaps refuse to `"bitmap-tiny-floor"` and hand the ALREADY-BUILT
//! bitmap to the classic private-iterate setup (build work is never wasted;
//! the classic path proceeds byte-identically).

use std::sync::{Arc, OnceLock};

use ::executils::{EStateData, ExecSlotId};
use ::nodeagg::runtime_partial::agg_runtime_partial_admissible;
use ::tidbitmap::TbmSharedIterState;
use ::types_error::{PgError, PgResult, ERROR};
use ::types_nodes::NodeTag;

use super::lane_trace;
use super::runtime_scan::{engage, exprs_parallel_safe};

/// Work-division mode for the frozen-bitmap fetch phase (the race).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum BitmapMode {
    /// Contiguous window claims (locality at the claim frontier).
    Contiguous,
    /// Strided claims via a leader-built permutation (I/O spread).
    Strided,
}

fn bitmap_mode() -> BitmapMode {
    static MODE: OnceLock<BitmapMode> = OnceLock::new();
    *MODE.get_or_init(|| {
        match std::env::var("PGRUST_RUNTIME_BITMAP_MODE").as_deref() {
            Ok("B") | Ok("b") | Ok("strided") => BitmapMode::Strided,
            // C (two-stage build) is not built yet: refuse loudly rather
            // than silently racing the wrong arm.
            Ok("C") | Ok("c") => {
                lane_trace("runtime-bitmap: MODE=C not built, running mode A");
                BitmapMode::Contiguous
            }
            _ => BitmapMode::Contiguous,
        }
    })
}

/// Mode B stride count: the permutation interleaves this many contiguous
/// segments of the entry space.
fn bitmap_stride() -> usize {
    static N: OnceLock<usize> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_BITMAP_STRIDE")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|&n| n >= 2)
            .unwrap_or(64)
    })
}

/// Engagement floor in frozen entries (exact pages + lossy chunks): below
/// it the serial drain wins outright. Traced refusal `"bitmap-tiny-floor"`.
fn bitmap_min_entries() -> u64 {
    static N: OnceLock<u64> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("PGRUST_RUNTIME_BITMAP_MIN_ENTRIES")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(1024)
    })
}

/// The engagement payload's bitmap context: the frozen shared arrays + the
/// mode's granule->entry mapping. Set once at engage, read per claim.
pub(super) struct BitmapMorselCtx {
    pub(super) frozen: Arc<TbmSharedIterState>,
    /// Mode B: permutation of the segmented entry space (granule g visits
    /// entry order[g]). None = mode A (identity: contiguous windows).
    pub(super) order: Option<Vec<u32>>,
}

/// The morsel source: granule = one frozen entry (exact page or lossy
/// chunk), no interior hard boundaries — the duration-adaptive sizer builds
/// multi-page runs and the photo-finish handles the tail. Lossy chunks are
/// heavier granules (up to PAGES_PER_CHUNK heap pages); the sizer absorbs
/// that the same way it absorbs fat pgrcolumnar granules.
struct BitmapEntrySource {
    nentries: u64,
}

impl runtime::MorselSource for BitmapEntrySource {
    fn total_granules(&self) -> u64 {
        self.nentries
    }

    /// A bitmap entry stages one heap page (~50-250 tuples) — the heap
    /// block source's ramp seed fits.
    fn startup_c0(&self) -> u64 {
        16
    }
}

/// Mode B permutation: interleave S contiguous segments round-robin —
/// consecutive granules land ~nentries/S entries apart, so concurrently
/// claimed runs spread across the whole relation.
fn build_strided_order(nentries: usize, s: usize) -> Vec<u32> {
    let s = s.clamp(1, nentries.max(1));
    let base = nentries / s;
    let rem = nentries % s;
    let mut pos: Vec<usize> = (0..s).map(|i| i * base + i.min(rem)).collect();
    let ends: Vec<usize> = (0..s).map(|i| (i + 1) * base + (i + 1).min(rem)).collect();
    let mut order = Vec::with_capacity(nentries);
    while order.len() < nentries {
        for i in 0..s {
            if pos[i] < ends[i] {
                order.push(pos[i] as u32);
                pos[i] += 1;
            }
        }
    }
    order
}

/// Worker/claim side: drain one claimed granule range through the node's
/// UNCHANGED serial per-row drive into the plain-agg build. Called from
/// `runtime_scan`'s `morsel_body` (BitmapHeapScan outer arm).
pub(super) fn drain_claim<'mcx>(
    ctx: &BitmapMorselCtx,
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    b: &mut crate::procnode::BitmapHeapPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    range: runtime::MorselRange,
) -> PgResult<()> {
    match &ctx.order {
        None => {
            ::nodebitmapheapscan::bitmap_scan_install_entry_window(
                &mut b.scan,
                estate,
                &ctx.frozen,
                range.start,
                range.end,
            )?;
            drain_window(agg, b, estate)?;
        }
        Some(order) => {
            for g in range.start..range.end {
                let e = order[g as usize] as u64;
                ::nodebitmapheapscan::bitmap_scan_install_entry_window(
                    &mut b.scan,
                    estate,
                    &ctx.frozen,
                    e,
                    e + 1,
                )?;
                drain_window(agg, b, estate)?;
            }
        }
    }
    Ok(())
}

/// The serial row path unchanged over the installed window: fetch + recheck
/// (`bitmapqualorig`) + scan qual + projection per row (`exec_scan` body),
/// then exec_agg's single-group loop body verbatim. Byte-identity: identical
/// per-row programs and verdicts regardless of which worker visits a page;
/// partials are order-insensitive-exact (admission-gated).
fn drain_window<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    b: &mut crate::procnode::BitmapHeapPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    loop {
        let Some(slot) = ::nodebitmapheapscan::exec_bitmap_heap_scan(&mut b.scan, estate)?
        else {
            return Ok(());
        };
        ::nodeagg::agg_plain_build_accept(agg, estate, slot)?;
    }
}

/// The runtime bitmap arm. `None` = not engaged: either nothing was consumed
/// (pre-build refusals) or the bitmap was built once and handed to the
/// classic setup (floor refusals / engage fallback) — every fall-through is
/// byte-identical to today's serial path.
pub(super) fn try_own_plain_agg_over_bitmap_runtime<'mcx>(
    agg: &mut ::nodeagg::AggStateData<'mcx>,
    b: &mut crate::procnode::BitmapHeapPlanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    // --- Arming + kill-switch layering (all cheap; absent = today's path).
    let dop = ::guc_tables::runtime_pool::runtime_bitmap_pool_dop();
    if dop <= 0 || !runtime::runtime_enabled() {
        return Ok(None);
    }
    let Some(rt) = runtime::global() else { return Ok(None) };

    // --- Shape + session gates (fail-closed; every refusal is the serial
    // arm, byte-identically — nothing consumed before the build step).
    // No EA/instrumentation admission this round (fail-closed observability:
    // the arm's workers run uninstrumented and no tally is threaded).
    if estate.es_instrument != 0 {
        return Ok(None);
    }
    if estate.es_epq_active {
        return Ok(None);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction) {
        return Ok(None);
    }
    // A classic-parallel bitmap plan (Gather + parallel_aware node) keeps
    // its own machinery; this arm morselizes SERIAL plans only.
    if b.scan.parallel_aware || b.scan.pstate.is_some() {
        return Ok(None);
    }
    // Mid-scan re-entry (initialized without our engagement) — the serial
    // drain owns whatever state exists.
    if b.scan.initialized {
        return Ok(None);
    }
    if !agg_runtime_partial_admissible(agg) {
        return Ok(None);
    }
    if parallel::IsParallelWorker() || xact::IsInParallelMode() {
        return Ok(None);
    }
    if estate.es_param_list_info.is_some_and(|p| !p.is_empty()) {
        return Ok(None);
    }
    let Some(leader_pstmt) = estate.es_plannedstmt else { return Ok(None) };
    if leader_pstmt.paramExecTypes.iter().next().is_some() {
        return Ok(None);
    }
    // The Agg must be the plan root (workers ExecutorStart the whole worker
    // pstmt) and its outer node this BitmapHeapScan.
    let Some(root) = leader_pstmt.planTree else { return Ok(None) };
    let Some(root_agg) = root.as_agg() else { return Ok(None) };
    if !std::ptr::eq(root_agg, agg.plan) {
        return Ok(None);
    }
    let Some(scan_node) = agg.plan.plan.lefttree else { return Ok(None) };
    if scan_node.node_tag() != NodeTag::T_BitmapHeapScan {
        return Ok(None);
    }
    let scan_plan = scan_node.as_bitmap_heap_scan().expect("BitmapHeapScan tag");
    // Helpers run the scan qual, the recheck qual (bitmapqualorig), and the
    // projection targetlist — all must be parallel-safe. The bitmapqual
    // INDEX subtree runs only in the leader (serial build): not walked.
    if !exprs_parallel_safe(scan_plan.scan.plan.qual.iter())?
        || !exprs_parallel_safe(scan_plan.scan.plan.targetlist.iter())?
        || !exprs_parallel_safe(scan_plan.bitmapqualorig.iter())?
    {
        return Ok(None);
    }
    if !estate
        .es_snapshot
        .as_deref()
        .is_some_and(::types_snapshot::IsMVCCSnapshot)
    {
        return Ok(None);
    }
    let policy = parallel::query_task_policy_probe();
    if policy.has_params
        || policy.temp_state
        || policy.serializable
        || policy.pending_invalidations
    {
        return Ok(None);
    }
    if ::nodeagg::agg_is_done(agg) {
        return Ok(Some(None));
    }

    // --- Build the bitmap ONCE (the classic serial build, leader session —
    // C's BM_INITIAL winner). From here every fall-through must hand the
    // built bitmap to the classic setup: the build is never re-run.
    let mut tbm = crate::procnode::multi_exec_bitmap_node(&mut b.bitmapqual, estate)?;

    // --- Geometry floor (fail-closed, traced by name).
    let (npages, nchunks) = tbm.entry_counts();
    let nentries = npages + nchunks;
    if nentries < bitmap_min_entries().max(2 * dop as u64) {
        lane_trace(&format!(
            "runtime-bitmap: refused (bitmap-tiny-floor) entries={nentries} \
             (pages={npages} chunks={nchunks})"
        ));
        ::nodebitmapheapscan::bitmap_table_scan_setup(&mut b.scan, estate, Some(tbm))?;
        return Ok(None);
    }

    // --- Freeze + engage.
    let frozen = tbm.prepare_shared_iterate()?;
    // Keep the arena bitmap alive on the node for the query's lifetime
    // (parity with the classic serial node; the frozen arrays are
    // independent std Vecs).
    b.scan.tbm = Some(tbm);
    let mode = bitmap_mode();
    let order = match mode {
        BitmapMode::Contiguous => None,
        BitmapMode::Strided => {
            Some(build_strided_order(nentries as usize, bitmap_stride()))
        }
    };
    let ctx = Arc::new(BitmapMorselCtx { frozen: Arc::clone(&frozen), order });
    let source: Arc<dyn runtime::MorselSource> =
        Arc::new(BitmapEntrySource { nentries });
    lane_trace(&format!(
        "runtime-bitmap: admit mode={mode:?} entries={nentries} \
         (pages={npages} chunks={nchunks} lossy={})",
        nchunks > 0
    ));
    let r = engage(
        agg,
        estate,
        rt,
        dop,
        nentries,
        0,
        source,
        None,
        None,
        false,
        Some(ctx),
    )?;
    if r.is_none() {
        // Engage fallback with NOTHING consumed (all-refused / zero
        // workers): attach the frozen state as an ordinary single-consumer
        // shared iterator — C's merged block order, so the serial drain
        // that now runs is byte-identical to the classic path.
        lane_trace("runtime-bitmap: engage fallback, classic serial drain");
        ::nodebitmapheapscan::bitmap_scan_attach_shared_fallback(
            &mut b.scan,
            estate,
            frozen,
        )?;
    }
    Ok(r)
}

/// Worker-side shape check + drive-mode derivation, called from
/// `build_worker_exec_inner`: the leader proved admission on the identical
/// plan, so a diverged worker shape is an ERROR, never a wrong answer.
pub(super) fn worker_shape_check(agg: &::nodeagg::AggStateData<'_>) -> PgResult<()> {
    if !agg_runtime_partial_admissible(agg) {
        return Err(Box::new(PgError::new(
            ERROR,
            "runtime bitmap worker fold plan diverged from the leader's",
        )));
    }
    Ok(())
}
