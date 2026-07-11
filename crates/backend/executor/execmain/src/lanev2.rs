//! Lane executor v2 — the operator→operator batched execution lane (production
//! rebuild). See `docs/design/lane-executor-v2.md`.
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
//!   * `executils::BatchSource` — the shared seam trait (it cannot live here:
//!     `nodeagg` re-exports it as `AggBatchSource`, and `nodeagg` cannot depend
//!     on `execmain` without a crate cycle, so the trait sits in the shared
//!     `executils` seam both crates already depend on).
//! Disabling or deleting the lane is therefore local: drop this module + the
//! thin hook, and the C-identical executor is exactly as before.
//!
//! Gated OFF by default via the `PGRUST_LANE_V2` env var — deliberately NOT a
//! SQL GUC: a new GUC would add a row to the byte-identical `pg_settings` /
//! `SHOW ALL` output and break the `guc` / `rules` regression tests. Env-var
//! gating mirrors `jit_deform`'s `PGRUST_JIT_DEFORM` switch and is
//! byte-identity-safe. The completeness-gate run sets `PGRUST_LANE_V2=1` to
//! enable the lane across the whole regression suite.

use std::sync::OnceLock;

use ::executils::{EStateData, ExecSlotId};
use ::types_error::PgResult;

/// Master switch for lane-v2. Default OFF; `PGRUST_LANE_V2=1` (or `on`) enables
/// it. Resolved once per process (a boot-time decision, like
/// `jit_deform::available()`).
#[inline]
pub fn enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        matches!(std::env::var("PGRUST_LANE_V2").as_deref(), Ok("1") | Ok("on"))
    })
}

/// Phase 1 (first vertical slice): try to let the lane *own* a `SeqScan`
/// (scan→filter→project, scalar-within-lane over row batches).
///
/// `Some(result)` = the lane drove this call (`result` is the tuple-or-end,
/// the ordinary `ExecProcNode` return); `None` = refused, and the caller must
/// run the unchanged `exec_seq_scan`. Refusing is always byte-safe.
#[inline]
pub fn try_own_seq_scan<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    if !seq_scan_fusible(ss, estate)? {
        return Ok(None);
    }
    Ok(Some(drive_seq_scan(ss, estate)?))
}

/// Refuse-set for the lane-v2 SeqScan driver (false → the caller falls through
/// to `exec_seq_scan`, byte-identically). Admits Plain / WithQual /
/// WithProject / WithQualProject over a page-batch-supporting AM, and only
/// when the qual and projection are subplan-free and param-free: the generic
/// per-row emit path runs neither initplan params nor subplan quals, whereas
/// `exec_scan_extended` does, so those shapes must keep the old path.
///
/// Disarms on: EPQ, a backward/mark cursor (init eflags) or a non-forward
/// call, EXPLAIN ANALYZE (instrumented), parallel scan, the Bloom/EPQ
/// variants, and AMs without page-batch support.
fn seq_scan_fusible<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if estate.es_epq_active
        || !::types_scan::sdir::ScanDirectionIsForward(estate.es_direction)
        || !ss.batch_allowed()
        || ss.is_parallel()
        || ss.ss.instr_idx.is_some()
    {
        return Ok(false);
    }
    match ss.variant() {
        ::nodeseqscan::SeqScanVariant::Plain
        | ::nodeseqscan::SeqScanVariant::WithQual
        | ::nodeseqscan::SeqScanVariant::WithProject
        | ::nodeseqscan::SeqScanVariant::WithQualProject => {}
        ::nodeseqscan::SeqScanVariant::PlainBloom | ::nodeseqscan::SeqScanVariant::Epq => {
            return Ok(false)
        }
    }
    if let Some(q) = ss.ss.qual.as_deref() {
        if q.has_subplan() || !q.param_exec_deps().is_empty() {
            return Ok(false);
        }
    }
    if let Some(p) = ss.ss.ps_ProjInfo.as_ref() {
        if p.pi_state.has_subplan() || !p.pi_state.param_exec_deps().is_empty() {
            return Ok(false);
        }
    }
    // AM must support the page-batch primitives (opens the scan desc once).
    ::nodeseqscan::seq_scan_batch_supported(ss, estate)
}

/// The lane's SeqScan drive. Pulls row batches through the `BatchSource` seam
/// primitives (`seq_scan_next_pagebatch` / `seq_scan_batch_fetch`, the same
/// ones `SeqScanBatchSource: BatchSource` wraps) and emits one row per call
/// via `seq_scan_batch_emit` — which is `ExecScanExtended`'s body over a
/// staged batch row (reset per-tuple context, store + apply the scan qual
/// scalar-per-row via `execexpr`, project). Same tuples, same order, same
/// qual/proj/NULL semantics as `exec_seq_scan` → BYTE-IDENTICAL.
///
/// The one-tuple-per-call cursor over the staged page batch lives on the node
/// (`SeqScanState::lane_cursor`), so the drive survives the Volcano per-call
/// boundary.
fn drive_seq_scan<'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    debug_assert!(::types_scan::sdir::ScanDirectionIsForward(estate.es_direction));
    loop {
        let (mut pos, mut n) = ss.lane_cursor();
        if pos >= n {
            n = ::nodeseqscan::seq_scan_next_pagebatch(ss, estate)?;
            pos = 0;
            ss.set_lane_cursor(pos, n);
            if n == 0 {
                // End of scan: mirror ExecScanExtended's projected-slot clear
                // (the non-projected path returns None without clearing).
                if let Some(proj) = ss.ss.ps_ProjInfo.as_ref() {
                    let mcx = estate.es_query_cxt;
                    let result_id = proj.pi_result_slot;
                    ::exectuples::exec_clear_tuple(estate.slot_mut(result_id), mcx);
                }
                return Ok(None);
            }
        }
        // Match the per-tuple path's interrupt cadence: `exec_scan_fetch`
        // runs `check_for_interrupts` once per tuple attempt. Skipping it in
        // the batched drive would process pending interrupts / cache
        // invalidations at a different cadence than the code the lane
        // replaces; keep it identical.
        ::postgres_seams::check_for_interrupts::call()?;
        let i = pos;
        ss.set_lane_cursor(pos + 1, n);
        if let Some(slot) = ::nodeseqscan::seq_scan_batch_emit(ss, estate, i)? {
            return Ok(Some(slot));
        }
    }
}
