//! W0 — funnel-into-writer: the write-dest admission slice of the parallel
//! passthrough funnel (parallel-writes design §4 rung W0;
//! scratchpad/night/parallel-writes-design.md).
//!
//! # Shape
//!
//! CTAS / SELECT INTO / matview datafill run their SELECT through the World-B
//! passthrough funnel (`runtime_passthrough.rs`): N launched workers each
//! drive the lane push island over claimed heap-block morsels and stream
//! MinimalTuple images through per-worker rings; the LEADER drain feeds the
//! statement's existing leader-only write DestReceiver
//! (`DestReceiver::IntoRel` / `DestReceiver::TransientRel`) instead of the
//! wire — `dest.receive_slot(slot)` IS `intorel_receive` /
//! `transientrel_receive` (single-tuple `table_tuple_insert` with the
//! startup-captured `output_cid`, `TABLE_INSERT_SKIP_FSM`, bistate). ZERO
//! receiver code changes: the funnel hook was already dest-generic; this
//! module only decides ADMISSION.
//!
//! # Correctness argument (why leader-only writes need no new write theory)
//!
//! Every byte that touches the target heap is written by the SESSION thread,
//! exactly as the serial statement writes it:
//! - xid: assigned by `DefineRelation`/`make_new_heap` in `dest.startup`
//!   (execmain.rs runs startup BEFORE `execute_plan`), so the leader's
//!   `heap_insert` xid fetch never calls `AssignTransactionId` inside the
//!   parallel bracket (xact.rs guards fire only on ASSIGNMENT);
//! - cid: `output_cid` captured once at `intorel_startup`/
//!   `transientrel_startup` on the session thread and threaded down as a
//!   parameter; no `GetCurrentCommandId(true)` inside the bracket;
//! - WAL / TOAST / FSM / extension locks / minimal-WAL pending syncs: the
//!   serial write path verbatim, session thread only. Workers are pure
//!   READERS of the (pre-existing, committed) source relation.
//!
//! The one genuinely new admission: the statement SELF-CREATES its target
//! before the SELECT runs, so the transaction always has PENDING UNCOMMITTED
//! -DDL INVALIDATIONS at hook time — the launched-gang bind path already
//! handles exactly this (`parallel::parallel_worker_body`: warm claim is
//! refused when `shared.leader_pending_invals`, the cold arm runs
//! `InvalidateSystemCaches`, and `note_caches_tainted` covers the
//! abort-poison window — the shipped matview-datafill / legacy-Gather
//! precedent). The hook therefore admits `pending_invalidations` for write
//! dests ONLY; every other dest keeps the conservative refusal.
//!
//! # Status: KILL-SWITCH GATED, DEFAULT OFF.
//!
//! `PGRUST_RUNTIME_CTAS_FUNNEL=1|on` arms write-dest admission, and only on
//! top of the funnel's own `PGRUST_RUNTIME_ROW_FUNNEL` switch. Unset, every
//! write dest fail-closes to the serial per-tuple loop byte-identically —
//! `route_to` / the coverage matrix row `parallel-ctas-matview-write` are
//! unchanged until the fleet letter flips them.
//!
//! Fail-closed carve-outs (inherited from the hook's gates, listed here as
//! the write-shape reading): CTAS WITH NO DATA never runs the executor;
//! EXPLAIN ANALYZE sets `es_instrument` (refused); CREATE TEMP TABLE AS
//! creates the temp namespace at startup, so the binder policy's
//! `temp_state` refusal stands down the funnel; REFRESH ... CONCURRENTLY
//! takes the SPI path (refused by the SPI/cursor budget gates); non-heap
//! AMs refuse at the granule-map probe; count-limited runs refuse (the
//! portal-suspend rule).

use std::sync::atomic::{AtomicU64, Ordering};

use ::types_dest::CommandDest;

/// Kill switch (default OFF): `PGRUST_RUNTIME_CTAS_FUNNEL=1`/`on` admits the
/// write dests below into the passthrough funnel. One process-static resolve
/// (the `PGRUST_RUNTIME_*` precedent).
fn ctas_funnel_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        matches!(std::env::var("PGRUST_RUNTIME_CTAS_FUNNEL").as_deref(), Ok("1") | Ok("on"))
    })
}

/// The hook's write-dest admission verdict.
pub(super) enum WriteDestVerdict {
    /// Not a write dest: the hook's pre-existing rules apply unchanged.
    NotWrite,
    /// Admissible write dest (kill switch armed): engage, and permit the
    /// pending-invalidations carve (see module doc).
    Admit,
    /// Write dest but refused (kill switch off): fail closed to the serial
    /// loop — never engage a write dest through the wire-only switch alone.
    Refuse,
}

/// Classify `dest.mydest()` for the passthrough hook. IntoRel = CTAS /
/// SELECT INTO / EXPLAIN ANALYZE CMV (the latter re-refused by
/// `es_instrument`); TransientRel = REFRESH MATERIALIZED VIEW (non-
/// CONCURRENT) and CREATE MATERIALIZED VIEW's datafill. Everything else is
/// not a write dest.
pub(super) fn classify_write_dest(kind: CommandDest) -> WriteDestVerdict {
    match kind {
        CommandDest::IntoRel | CommandDest::TransientRel => {
            if ctas_funnel_enabled() {
                WriteDestVerdict::Admit
            } else {
                WriteDestVerdict::Refuse
            }
        }
        _ => WriteDestVerdict::NotWrite,
    }
}

/// W0 observability: cumulative (engaged, completed) counters for the write-
/// dest engagements specifically (the funnel's own PT_* counters count these
/// too; the split lets the e2e assert WRITE engagement positively).
static W0_ENGAGED: AtomicU64 = AtomicU64::new(0);
static W0_COMPLETED: AtomicU64 = AtomicU64::new(0);

pub fn ctas_funnel_engagements() -> (u64, u64) {
    (W0_ENGAGED.load(Ordering::SeqCst), W0_COMPLETED.load(Ordering::SeqCst))
}

/// Called by the hook when every gate passed for a write dest and the
/// ceremony is being entered.
pub(super) fn note_engaged(total_granules: u64) {
    W0_ENGAGED.fetch_add(1, Ordering::SeqCst);
    super::lane_trace(&format!("ctas-funnel: engaged granules={total_granules}"));
}

/// Called by the hook when the funnel answered a write-dest run.
pub(super) fn note_completed(rows: u64) {
    W0_COMPLETED.fetch_add(1, Ordering::SeqCst);
    super::lane_trace(&format!("ctas-funnel: completed rows={rows}"));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kill_switch_default_off_refuses_write_dests() {
        if std::env::var("PGRUST_RUNTIME_CTAS_FUNNEL").is_err() {
            assert!(matches!(
                classify_write_dest(CommandDest::IntoRel),
                WriteDestVerdict::Refuse
            ));
            assert!(matches!(
                classify_write_dest(CommandDest::TransientRel),
                WriteDestVerdict::Refuse
            ));
        }
    }

    #[test]
    fn non_write_dests_are_not_write() {
        for k in [
            CommandDest::None,
            CommandDest::Remote,
            CommandDest::Tuplestore,
            CommandDest::Spi,
            CommandDest::CopyOut,
            CommandDest::SqlFunction,
            CommandDest::TupleQueue,
        ] {
            assert!(matches!(classify_write_dest(k), WriteDestVerdict::NotWrite));
        }
    }
}
