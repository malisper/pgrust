//! Express-lane point-path experiment — single-executor migration Phase 1,
//! WS-J (design + measurement plan: docs/design/rowmode-operators.md §5–§6;
//! the abort criterion there is BINDING on any Phase-2 investment).
//!
//! THE EXPERIMENT: for the admitted point shape (a prepared PK lookup —
//! every scan key a plain btree-equality key), the maximally-degenerate
//! express pipeline's per-row body IS the incumbent fused body:
//! `PointProbeSource::next_row` is a VERBATIM call to
//! `nodeindexscan::exec_index_scan` (runtime-key eval + rescan on the
//! `!ready` arm, `execscan::exec_scan` dispatch, `index_getnext_slot`, the
//! node's own qual/projection — a code SHARE, not a move, so the Volcano
//! path keeps its exact statement stream and there is zero se-entrycost
//! exposure). The measured ON−OFF exact-instruction delta is therefore a
//! pure reading of LANE-HOSTING OVERHEAD (hook + admission verdict +
//! driver) — precisely the quantity the migration doc's §0.5 abort
//! criterion binds. Two knob-selected arms answer two different questions:
//!
//!  * mode 1 EXPRESS (`PGRUST_LANE_V2_EXPRESS=1|on`): `pull_step_point`
//!    direct-return — no `TupleOp`, no `RootAdapter`. Can the hook be free?
//!  * mode 2 STRUCTURED (`PGRUST_LANE_V2_EXPRESS=2`): `pull_step_rows` +
//!    `PassthroughOp` + capacity-one `RootAdapter` — what does the real
//!    row-mode chain machinery cost per pull? (The number Phase 2 must pay
//!    or engineer away.)
//!
//! Both arms are byte-identical to the incumbent BY CONSTRUCTION (the body
//! is the same function; the lane holds zero shadow state, so a Volcano
//! fallback at any pull boundary — refused gate, knob off — resumes the
//! node's own cross-call state exactly). Parity is proven, not assumed:
//! scripts/dualexec/corpus-point.sql + scripts/lane-express-e2e.sh.
//!
//! ADMISSION (pksel-only, contract WS-J(3); UNMEMOIZED per-pull check —
//! `IndexScanState` is under the live subplankey R-LOCKS umbrella, so the
//! node-memoized verdict field is a post-t25-rebase amendment; a nonzero
//! mode-1 delta with this fallback does NOT fire the abort criterion until
//! the memo lever is applied, contract WS-J(2)): every `iss_ScanKeys` entry
//! `sk_strategy == BTEqualStrategyNumber` with no structural flags
//! (row-compare / search-array / null-search), at least one key,
//! `iss_OrderBy` none, forward `iss_OrderDir`, btree relam, not
//! parallel-aware, `batch_allowed`. DELIBERATE deltas vs the tidrun gate
//! (`index_scan_refuse_reason`): runtime keys ADMITTED (the express shape's
//! defining feature — the prepared-statement param probe), qual/projection
//! ADMITTED (the node's own body runs them), MVCC gate NOT NEEDED (no
//! tidrun batching — the body is the per-tuple incumbent).
//!
//! ACCOUNTING (deviation from the design sketch, flagged in
//! notes/se-ws-j-expresslane.md): an express-REFUSED pull ticks NOTHING
//! here — it falls through to the incumbent standalone refuse
//! (`AdmissionEconomicsNoConsumer`), keeping refused-pull accounting
//! byte-identical to today under the knob in BOTH positions and requiring
//! zero lane-gates allowlist rows. An express-OWNED pull ticks
//! `tick_owned(ShapeClass::IndexScan)` once (per-pull cadence) and skips
//! the incumbent refuse tick — the observable knob-ON engagement signal
//! (the WS-E off-refused/on-owned pattern). Knob OFF short-circuits before
//! any tick: today's bytes AND today's accounting.
//!
//! ENGINE mirror (contract §2e / WS-J(6)): the D1 production-verdict
//! convention. `EXPLAIN (ENGINE, ANALYZE)` instruments every node, so the
//! observed express verdict under capture is always `Instrumented`; the
//! mirror re-evaluates with ONLY the instrument gate vacated and records
//! the PRODUCTION verdict (lane-owned, or the production refuse reason).
//! First-record-wins dedup makes this record win over the fall-through
//! standalone capture. Zero records on default paths (the emission-gate
//! law): capture runs only under `estate.engine_capture()`.

use std::sync::atomic::{AtomicU8, Ordering::Relaxed};

use ::executils::{EStateData, ExecSlotId};
use ::types_error::PgResult;
use ::types_scan::scankey::{
    BTEqualStrategyNumber, SK_ROW_HEADER, SK_SEARCHARRAY, SK_SEARCHNOTNULL, SK_SEARCHNULL,
};

use super::push::{pull_step_point, pull_step_rows, PassthroughOp, RootAdapter, RowSource};
use super::stats::{self, RefuseReason, ShapeClass};

/// `PGRUST_LANE_V2_EXPRESS` (registry name, integration contract §1;
/// default OFF): 0 = unresolved (env read on first use), 1 = OFF,
/// 2 = EXPRESS (knob "1"/"on"), 3 = STRUCTURED (knob "2"). An AtomicU8
/// rather than the OnceLock idiom so the unit corpus can A/B all three
/// positions in one process (`express_set_for_tests`, the rowmode.rs
/// pattern); env-var, not GUC (the no-new-SHOW-ALL-rows discipline).
static EXPRESS: AtomicU8 = AtomicU8::new(0);

pub(crate) const EXPRESS_OFF: u8 = 1;
pub(crate) const EXPRESS_POINT: u8 = 2;
pub(crate) const EXPRESS_STRUCTURED: u8 = 3;

/// Resolve the knob (one relaxed load + branch once resolved — the whole
/// knob-OFF hook tax on the incumbent point path).
#[inline]
fn express_mode() -> u8 {
    match EXPRESS.load(Relaxed) {
        0 => {
            let m = match std::env::var("PGRUST_LANE_V2_EXPRESS").as_deref() {
                Ok("1") | Ok("on") => EXPRESS_POINT,
                Ok("2") => EXPRESS_STRUCTURED,
                _ => EXPRESS_OFF,
            };
            EXPRESS.store(m, Relaxed);
            m
        }
        m => m,
    }
}

/// Same-process A/B lever for the unit corpus (`crate::tests::express_ab`).
/// `mode`: `EXPRESS_OFF` / `EXPRESS_POINT` / `EXPRESS_STRUCTURED`.
#[cfg(test)]
pub(crate) fn express_set_for_tests(mode: u8) {
    EXPRESS.store(mode, Relaxed);
}

/// Test-only engagement probe: express-owned pulls (the unit corpus asserts
/// the ON arms actually engaged — stats.rs ticks arm only via process-global
/// envs, unusable per-test; the rowmode.rs pattern).
#[cfg(test)]
pub(crate) static EXPRESS_OWNED_FOR_TESTS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// §5 PointProbeSource: the point probe as a row-mode source whose per-row
/// body is the incumbent fused body VERBATIM — a code SHARE of the pub
/// `nodeindexscan::exec_index_scan` (runtime-key eval + rescan on the
/// `!ready` arm + `execscan::exec_scan` dispatch + `index_getnext_slot` +
/// the node's own qual/projection). Zero code moves → zero se-entrycost
/// exposure on the Volcano path; error unwind and interrupt cadence are the
/// Volcano body's own.
struct PointProbeSource;

impl<'mcx> RowSource<'mcx> for PointProbeSource {
    type Node = ::nodeindexscan::IndexScanState<'mcx>;

    #[inline(always)]
    fn next_row(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        ::nodeindexscan::exec_index_scan(node, estate)
    }
}

/// `None` = admitted (the pksel point shape); `Some(reason)` = refused,
/// using EXISTING vocabulary only (R-VOCAB frozen; reasons feed the ENGINE
/// mirror, they are NOT ticked — module doc). `ignore_instrument` vacates
/// only the instrument gate: the D1 production-verdict re-evaluation.
///
/// Per-pull and unmemoized (the R-LOCKS fallback): ~8 loads/branches plus
/// one pass over the scan keys (a point probe has 1..pknatts keys). The
/// node-memoized u8 verdict is the first §5 parity lever, applied after the
/// subplankey umbrella lifts at the t25 rebase.
fn express_refuse_reason<'mcx>(
    is: &::nodeindexscan::IndexScanState<'mcx>,
    estate: &EStateData<'mcx>,
    ignore_instrument: bool,
) -> Option<RefuseReason> {
    // Dynamic per-pull gates (the try_own_result cadence). (The backward
    // gate retired with the backward-execution wave B11.)
    if estate.es_epq_active {
        return Some(RefuseReason::Epq);
    }
    if !ignore_instrument && is.ss.instr_idx.is_some() {
        return Some(RefuseReason::Instrumented);
    }
    // Static shape gates (plan-static; memo candidates post-t25).
    if !is.batch_allowed() {
        return Some(RefuseReason::ScrollMark);
    }
    if is.iss_ParallelAware {
        return Some(RefuseReason::ParallelGate);
    }
    if is.iss_OrderBy.is_some() {
        return Some(RefuseReason::OrderByReorder);
    }
    if !::types_scan::sdir::ScanDirectionIsForward(is.iss_OrderDir) {
        // DESC-ordered index scan plan - live shape; B11 re-vocab.
        return Some(RefuseReason::DescOrder);
    }
    if !is
        .iss_RelationDesc
        .as_ref()
        .is_some_and(|r| r.rd_rel.relam == ::types_core::BTREE_AM_OID)
    {
        return Some(RefuseReason::NonBtree);
    }
    // The pksel shape: at least one scan key, every key a plain btree
    // equality (no row-compare header/members, no SAOP search arrays, no
    // IS [NOT] NULL searches — those are range/multi-point shapes, outside
    // the experiment's claim). Runtime keys are ADMITTED: their scankeys
    // carry no structural flags at build (`exec_index_build_scan_keys`),
    // and the dynamic SK_ISNULL a NULL param sets at eval is semantics the
    // verbatim body already owns. Refuse reason = the incumbent standalone
    // refuse's own vocabulary (admission economics: a non-point probe has
    // no express upside).
    if is.iss_ScanKeys.is_empty() {
        return Some(RefuseReason::AdmissionEconomicsNoConsumer);
    }
    const STRUCTURAL: i32 = SK_ROW_HEADER | SK_SEARCHARRAY | SK_SEARCHNULL | SK_SEARCHNOTNULL;
    for k in is.iss_ScanKeys.iter() {
        if k.sk_strategy != BTEqualStrategyNumber || (k.sk_flags & STRUCTURAL) != 0 {
            return Some(RefuseReason::AdmissionEconomicsNoConsumer);
        }
    }
    None
}

/// The express hook body, called from the HEAD of `try_own_index_scan`
/// (before the standalone refuse). `Some(result)` = express drove this pull
/// (`result` is the ordinary tuple-or-end); `None` = knob off or refused —
/// the caller falls through to the UNCHANGED refuse + `exec_index_scan`
/// path (byte- and accounting-identical to today).
#[inline]
pub(super) fn try_own_index_scan_express<'mcx>(
    is: &mut ::nodeindexscan::IndexScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<Option<ExecSlotId>>> {
    let mode = express_mode();
    if mode == EXPRESS_OFF {
        return Ok(None);
    }
    if let Some(observed) = express_refuse_reason(is, estate, false) {
        // ENGINE mirror (D1): record the PRODUCTION verdict — an observed
        // Instrumented refusal is re-evaluated with only the instrument
        // gate vacated (ENGINE requires ANALYZE, which instruments every
        // node, so this is the capture path's steady state). First record
        // wins over the fall-through standalone capture.
        if estate.engine_capture() {
            if let Some(idx) = is.ss.instr_idx {
                let production = match observed {
                    RefuseReason::Instrumented => express_refuse_reason(is, estate, true),
                    other => Some(other),
                };
                super::engine_record_verdict(
                    estate,
                    idx as i32,
                    ShapeClass::IndexScan,
                    production,
                );
            }
        }
        // Refused: NO tick here (module doc) — the incumbent standalone
        // refuse ticks AdmissionEconomicsNoConsumer exactly as today.
        return Ok(None);
    }
    // Owned: per-pull cadence, replacing the incumbent's refused tick.
    stats::tick_owned(ShapeClass::IndexScan);
    #[cfg(test)]
    EXPRESS_OWNED_FOR_TESTS.fetch_add(1, Relaxed);
    if mode == EXPRESS_POINT {
        // Mode 1 EXPRESS: source-only direct return.
        return pull_step_point(is, &mut PointProbeSource, estate).map(Some);
    }
    // Mode 2 STRUCTURED: the real row-mode chain machinery — dyn TupleOp
    // dispatch + capacity-one RootAdapter overfill guard + OpStatus arms —
    // priced on the point path. No clear-on-finish: exec_index_scan's own
    // body already clears the scan slot at exhaustion.
    let mut root = RootAdapter::new(None);
    pull_step_rows(is, &mut PointProbeSource, &mut PassthroughOp, &mut root, estate).map(Some)
}
