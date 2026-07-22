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
//! # Status: FLIPPED ON (GL-W0-2; `PGRUST_RUNTIME_CTAS_FUNNEL=0|off` kills).
//!
//! The GL-W0-2 composition ladder (W0 + W0.1 pure-drain + W1 multi-insert,
//! scratchpad/night/GL-W0-2-letter.md) measured the stack ALWAYS-WIN vs
//! serial in its engagement region — every shape 0.23–0.64 at 2M rows,
//! DOP {2,4,8}, ground-truth row counts verified — and the region is
//! STRICTLY ADDITIVE: this hook engages only on serial-SHAPED plans
//! (`!use_parallel_mode` at the execute_plan call site), so Gather-planned
//! CTAS is untouched and W0 displaces only the serial per-tuple loop.
//! Honest region note for the census confirm leg: at default costs the
//! planner Gathers large CTAS SELECTs, so default-ON yield lives where
//! Gather is priced out or unavailable; the dop>=4 funnel-vs-Gather gap is
//! the drain loop (W2a's charter), not this flip's concern.
//! The kill restores the serial loop byte-identically; it also rides the
//! funnel's own `PGRUST_RUNTIME_ROW_FUNNEL=0` master kill.
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

/// ON BY DEFAULT (GL-W0-2 flip; flipped-kill idiom, the row_funnel_enabled
/// spelling): `PGRUST_RUNTIME_CTAS_FUNNEL=0`/`off` kills write-dest
/// admission, restoring the serial per-tuple loop byte-identically. One
/// process-static resolve (the `PGRUST_RUNTIME_*` precedent).
fn ctas_funnel_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !std::env::var("PGRUST_RUNTIME_CTAS_FUNNEL")
            .is_ok_and(|v| matches!(v.trim(), "0" | "off"))
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

    /// GL-W0-2 flip pin: the DEFAULT world admits write dests (flipped-kill —
    /// only an explicit =0|off kills; the un-gated default-pin test per the
    /// t38 convention).
    #[test]
    fn flipped_default_admits_write_dests() {
        if std::env::var("PGRUST_RUNTIME_CTAS_FUNNEL").is_err() {
            assert!(matches!(
                classify_write_dest(CommandDest::IntoRel),
                WriteDestVerdict::Admit
            ));
            assert!(matches!(
                classify_write_dest(CommandDest::TransientRel),
                WriteDestVerdict::Admit
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

// ---------------------------------------------------------------------------
// W2a increment 1 — pop-K batched write drain (PGRUST_W2A_DRAIN_BATCH,
// default OFF; GL-W2A-1 measures it standalone).
//
// GL-W0-2 isolated the funnel's write-drain deficit to the per-row drain
// loop: image pop -> wire-slot store -> DestReceiver dispatch -> seam hop ->
// receiver body -> W1 buffer copy -> slot clear -> image FREE, every row,
// on the single writer thread. This increment batches K images per flush:
// one receiver dispatch per batch, a monomorphic store->buffer-feed loop
// (the seam hop and receiver body are skipped when the W1 buffer is live),
// one slot clear, and K image frees together at batch clear. The W1 buffer
// itself still flushes on ITS thresholds inside write_buffer_receive — page
// packing and WAL shape are unchanged. Fallback (knob off / W1 killed /
// receiver without a live buffer): the per-element receive_slot path,
// byte-identical to the unbatched drain.
// ---------------------------------------------------------------------------

/// W2a inc-1 knob (default OFF): `PGRUST_W2A_DRAIN_BATCH=1|on` arms the
/// batched write drain. Measurement lever — flips ride GL-W2A-1 + a train.
pub(super) fn w2a_drain_batch_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        matches!(std::env::var("PGRUST_W2A_DRAIN_BATCH").as_deref(), Ok("1") | Ok("on"))
    })
}

/// Batch capacity: bounded leader-side buffering on top of W1's pool
/// (K images + the pool's 1000 slots — the same order as Gather's tuple
/// queues).
pub(super) const DRAIN_BATCH_CAP: usize = 256;

/// Flush `batch` into the write dest. Buffered fast path when the receiver
/// carries a live W1 buffer; per-element `receive_slot` otherwise. The
/// wire slot only BORROWS each image (cleared before the batch drops).
/// Receivers on this path never stop early (intorel/transientrel always
/// continue), so the fast path has no `cont` plumbing.
pub(super) fn flush_write_batch<'d>(
    batch: &mut Vec<super::row_emit::MinImage>,
    dest: &mut ::tcop_dest::DestReceiver<'d>,
    wire_slot: &mut ::types_slot::SlotData<'d>,
    slot_mcx: ::mcx::Mcx<'d>,
) -> ::types_error::PgResult<()> {
    use ::tcop_dest::DestReceiver;
    if batch.is_empty() {
        return Ok(());
    }
    match dest {
        DestReceiver::IntoRel(st) if st.mibuf.is_some() => {
            let mcx = st.mcx; // receiver ctx: buffer copies (intorel parity)
            let rel = st.rel.as_ref().expect("intorel_startup ran");
            let mibuf = st.mibuf.as_mut().expect("checked");
            let (cid, opts) = (st.output_cid, st.ti_options);
            let mut bistate = st.bistate.as_mut();
            for img in batch.iter() {
                // SAFETY: Minimal wire slot (hook contract); the image
                // outlives the buffer's copy (W1 copies into its pool slot
                // inside write_buffer_receive) and the clear below.
                unsafe {
                    ::exectuples::exec_store_minimal_tuple_ptr(
                        wire_slot,
                        slot_mcx,
                        img.as_mtup_ptr(),
                    );
                }
                ::tableam::write_buffer::write_buffer_receive(
                    mcx,
                    rel,
                    mibuf,
                    wire_slot,
                    cid,
                    opts,
                    bistate.as_deref_mut(),
                )?;
            }
            ::exectuples::exec_clear_tuple(wire_slot, slot_mcx);
        }
        DestReceiver::TransientRel(st) if st.mibuf.is_some() => {
            let mcx = st.mcx;
            let rel = st.rel.as_ref().expect("transientrel_startup ran");
            let mibuf = st.mibuf.as_mut().expect("checked");
            let (cid, opts) = (st.output_cid, st.ti_options);
            let mut bistate = st.bistate.as_mut();
            for img in batch.iter() {
                // SAFETY: as the IntoRel arm.
                unsafe {
                    ::exectuples::exec_store_minimal_tuple_ptr(
                        wire_slot,
                        slot_mcx,
                        img.as_mtup_ptr(),
                    );
                }
                ::tableam::write_buffer::write_buffer_receive(
                    mcx,
                    rel,
                    mibuf,
                    wire_slot,
                    cid,
                    opts,
                    bistate.as_deref_mut(),
                )?;
            }
            ::exectuples::exec_clear_tuple(wire_slot, slot_mcx);
        }
        _ => {
            // W1 killed or an unexpected dest: per-element receive_slot,
            // byte-identical to the unbatched drain.
            for img in batch.iter() {
                // SAFETY: as above.
                unsafe {
                    ::exectuples::exec_store_minimal_tuple_ptr(
                        wire_slot,
                        slot_mcx,
                        img.as_mtup_ptr(),
                    );
                }
                dest.receive_slot(wire_slot)?;
                ::exectuples::exec_clear_tuple(wire_slot, slot_mcx);
            }
        }
    }
    // K image frees together (the batched-dealloc half of the increment).
    batch.clear();
    Ok(())
}
