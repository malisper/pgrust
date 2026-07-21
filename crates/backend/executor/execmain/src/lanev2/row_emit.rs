//! World-B: the PARALLEL lane-v2 row-emit boundary (gather-elimination Phase 2
//! wiring into the lane executor).
//!
//! # World-A / World-B / the non-lane-ownable tail
//!
//! - **World-A (already shipped):** the *serial* lane executor row-emits the
//!   single-rel scan + qual + projection shape — the push island
//!   `Source → (qual/project) Operator → RootAdapter` that `try_own_seq_scan`
//!   drives one tuple per `exec_proc_node` (`push.rs`). One thread, no funnel.
//! - **World-B (this module):** the *parallel* version of exactly that
//!   lane-ownable shape. N runtime workers each run the SAME lane push island,
//!   but the terminal sink is [`RowEmitSink`] (this file) instead of
//!   `RootAdapter`: it appends the projected tuple into the worker's funnel
//!   ring ([`runtime::RowFunnel`]) rather than a capacity-one buffer. The
//!   leader drains the rings to the wire with [`drain_lane_funnel`], porting
//!   `nodegather.rs::gather_readnext` (round-robin, stick-until-block). This is
//!   the runtime's first NON-BREAKER (streaming) taskset — see the invariant
//!   analysis in `runtime/src/funnel.rs`.
//! - **The non-lane-ownable tail (explicitly OUT of scope, stays on classic
//!   Gather):** any row-returning shape the lane cannot vectorize — multi-rel
//!   joins emitting rows, SRFs / ProjectSet, volatile or non-parallel-safe
//!   target exprs, WHERE CURRENT OF / EPQ recheck, cursors, and anything the
//!   push path already refuses (`push.rs cursor_store_batch_fill` carve-outs).
//!   Hosting the *row* (Volcano) executor under the funnel — a transport-only
//!   win with no vectorization — is a SEPARATE later step, deliberately not
//!   built here.
//!
//! # Status: FLIPPED ON for the FloorGuard band (GL-FUNNEL-4;
//! `PGRUST_RUNTIME_ROW_FUNNEL=0` kills — [`row_funnel_enabled`]).
//!
//! The fleet A/B ladder (GL-FUNNEL-1..4) proved the band: qualed selective
//! passthrough beats the shipped serial default 6.3x geomean and classic
//! Gather 0.905–1.037 across 1gb/10gb x DOP 4–16 under fair buffer residency
//! (byte-identity + engagement evidence at every point). Everything outside
//! the band (`try_passthrough_funnel`'s fail-closed gates) refuses to the
//! serial loop byte-identically. Matrix row: gap:scan-passthrough.

// Kill-switch-gated integration seam: the producer sink + leader drain compile
// against the real `Sink`/slot APIs but have NO live call site yet (the
// scheduler wiring that publishes the row-emit taskset and runs the drain lands
// with the fleet A/B, per the plan's migration discipline). Allow dead_code so
// the seam can land reviewed and tested ahead of that flip.
#![allow(dead_code)]

use std::alloc::Layout;
use std::ptr::NonNull;
use std::sync::Arc;

use ::executils::{EStateData, ExecSlotId};
use ::types_error::PgResult;
use ::types_tuple::MinimalTupleData;

use runtime::{DrainStep, FunnelProducer, PushOutcome, RowFunnel};

/// THE GL-FUNNEL-4 FLIP (flipped-kill idiom): the parallel row-emit funnel is
/// ON BY DEFAULT for the FloorGuard-scoped band (`try_passthrough_funnel`'s
/// gates: qual required, emit fraction inside the proven band, DOP >= 2,
/// complete-drain only; every other shape refuses to the serial loop
/// byte-identically). `PGRUST_RUNTIME_ROW_FUNNEL=0` is the kill switch.
/// Provenance: GL-FUNNEL-4 warm-equalized decider — funnel-leader
/// 0.905/0.917 vs classic Gather at 10gb-exp DOP 8/16, 1gb parity
/// 1.015–1.037, 6.3x geomean vs the shipped serial default, byte-identity +
/// engagement evidence at every point.
pub(super) fn row_funnel_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !std::env::var("PGRUST_RUNTIME_ROW_FUNNEL").is_ok_and(|v| v.trim() == "0")
    })
}

/// Default per-worker ring capacity (rows). Bounded = the back-pressure knob
/// and the memory budget: at most `RING_CAP` owned images live per worker at
/// once. Modeled on PG's `PARALLEL_TUPLE_QUEUE_SIZE` intent (a small bounded
/// per-worker queue), sized in ROWS here since the transport carries owned
/// images, not a byte ring.
pub(super) const DEFAULT_RING_CAP: usize = 1024;

/// An owned, 8-aligned (MAXALIGN) flat MinimalTuple image — the funnel's
/// transport payload. Owned bytes, no borrow, so it is `Send` and crosses the
/// producer→leader boundary by ownership (research §3: in-process, tuples cross
/// by ownership, not a shm ring copy). Bounded by the ring capacity; freed by
/// whoever drops it (the leader after `receive_slot`, or the ring on teardown).
pub(super) struct MinImage {
    ptr: NonNull<u8>,
    len: usize,
}

// SAFETY: `MinImage` owns a private heap allocation of plain tuple bytes with
// no interior references; sending it to the draining thread transfers sole
// ownership (the producer drops its handle on push).
unsafe impl Send for MinImage {}

impl MinImage {
    fn layout(len: usize) -> Layout {
        // MAXALIGN(8): `exec_store_minimal_tuple_ptr` deforms through this
        // pointer, so the image must satisfy MinimalTupleData alignment.
        Layout::from_size_align(len.max(1), 8).expect("min-image layout")
    }

    /// Copy a formed minimal-tuple image into a fresh owned aligned block.
    fn from_bytes(bytes: &[u8]) -> MinImage {
        let len = bytes.len();
        let layout = Self::layout(len);
        // SAFETY: layout has nonzero size (max(1)) and MAXALIGN.
        let raw = unsafe { std::alloc::alloc(layout) };
        let ptr = NonNull::new(raw).unwrap_or_else(|| std::alloc::handle_alloc_error(layout));
        if len > 0 {
            // SAFETY: `ptr` owns `len` bytes; `bytes` is readable for `len`.
            unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr.as_ptr(), len) };
        }
        MinImage { ptr, len }
    }

    /// The image as a `MinimalTupleData` pointer for `exec_store_minimal_tuple_ptr`.
    pub(super) fn as_mtup_ptr(&self) -> NonNull<MinimalTupleData> {
        self.ptr.cast::<MinimalTupleData>()
    }
}

impl Drop for MinImage {
    fn drop(&mut self) {
        // SAFETY: allocated by `from_bytes` with exactly this layout.
        unsafe { std::alloc::dealloc(self.ptr.as_ptr(), Self::layout(self.len)) };
    }
}

/// Worker-side emitter of the parallel passthrough arm (World-B): materializes
/// each produced tuple into an owned [`MinImage`] and pushes it into this
/// worker's funnel ring via [`emit_blocking`](RowEmitSink::emit_blocking)
/// (blocking on a full ring under the K-standby permit).
///
/// NOTE (review fix): the earlier push-pipeline `Sink` face (`accept` returning
/// `SinkFeed::Full` with a `pending` boundary tuple) was DELETED — its protocol
/// dropped the newly-produced row whenever a pending boundary tuple was being
/// re-pushed (one lost row per ring-full event). The direct-drive
/// `emit_blocking` face is the only producer face; a pause/resume `Sink` face
/// is future work with a re-delivery contract, not a field on this struct.
pub(super) struct RowEmitSink {
    producer: FunnelProducer<MinImage>,
    /// Scratch bump context to FORM the minimal tuple before copying it into an
    /// owned image; reset after each row so it never grows (the images, not the
    /// scratch, carry the bounded memory).
    scratch: ::mcx::MemoryContext,
}

impl RowEmitSink {
    pub(super) fn new(producer: FunnelProducer<MinImage>) -> RowEmitSink {
        RowEmitSink {
            producer,
            scratch: ::mcx::MemoryContext::new_bump("lane-row-emit"),
        }
    }

    /// Copy the produced slot into a fresh owned bounded image (via the reset
    /// scratch bump context).
    fn materialize(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'_>,
    ) -> PgResult<MinImage> {
        let slot_mcx = estate.es_query_cxt;
        let mt = {
            let slot = estate.slot_mut(tuple);
            ::exectuples::exec_copy_slot_minimal_tuple(slot, slot_mcx, self.scratch.mcx(), 0)?
        };
        let img = MinImage::from_bytes(mt.as_bytes());
        drop(mt);
        // Bounded scratch: the owned image carries the bytes now.
        self.scratch.reset();
        Ok(img)
    }

    /// bgworker DIRECT-DRIVE emit (World-B producer body): materialize the
    /// produced slot and push into this worker's ring, BLOCKING on a full ring
    /// under the K-standby permit (`runtime::blocking_io_section`). Returns
    /// `false` iff demand was closed (LIMIT) before the row could be buffered —
    /// the caller must then stop producing. A dedicated producer thread blocks
    /// here — the correct model for the parallel-context worker body.
    pub(super) fn emit_blocking(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'_>,
    ) -> PgResult<bool> {
        // A parallel row-emit must never ride an EPQ recheck (the non-lane
        // tail): the eligibility gates carve these out; assert the invariant.
        debug_assert!(!estate.es_epq_active, "RowEmitSink inside an EPQ drive");
        let img = self.materialize(tuple, estate)?;
        match self.producer.push_blocking(img, ::runtime::blocking_io_section) {
            PushOutcome::Pushed => {
                estate.es_processed += 1;
                Ok(true)
            }
            PushOutcome::DemandClosed => Ok(false),
        }
    }

    /// LEADER-PRODUCER emit (GL-FUNNEL-2 increment 2): materialize and append
    /// to the leader's stash — NEVER parks (the leader is the drainer; a
    /// `push_blocking` park on its own full ring would self-deadlock). The
    /// stash is drained by the leader's own pump between claims; it is bounded
    /// by one claim's rows (the drain-first claim gate). Returns `false` on
    /// demand-closed (LIMIT), like `emit_blocking`.
    pub(super) fn emit_stash(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'_>,
        stash: &std::sync::Mutex<Vec<MinImage>>,
    ) -> PgResult<bool> {
        debug_assert!(!estate.es_epq_active, "RowEmitSink inside an EPQ drive");
        if self.producer.demand_closed() {
            return Ok(false);
        }
        let img = self.materialize(tuple, estate)?;
        stash.lock().unwrap_or_else(|p| p.into_inner()).push(img);
        estate.es_processed += 1;
        Ok(true)
    }
}

/// LEADER-side pure drain of the whole funnel to the wire (World-B). Ports
/// `gather_readnext` via [`runtime::FunnelDrain`]: round-robin, stick-until-
/// block; parks on all-rings-empty; stops at EOF or when `limit` rows are
/// emitted (closing demand so producers stop promptly — the LIMIT path).
///
/// `wire_slot` MUST be a `Minimal` slot with the result descriptor set. Returns
/// the number of rows delivered. The leader is a PURE consumer here (no morsel
/// claiming, no funnel production) — the deadlock-freedom precondition proven
/// in `funnel.rs` invariant #4.
pub(super) fn drain_lane_funnel<'mcx>(
    funnel: &Arc<RowFunnel<MinImage>>,
    wire_slot: ExecSlotId,
    dest: &mut ::tcop_dest::DestReceiver<'mcx>,
    estate: &mut EStateData<'mcx>,
    limit: Option<u64>,
) -> PgResult<u64> {
    let mut drain = funnel.drain();
    let mut emitted: u64 = 0;
    loop {
        crate::cfi()?;
        // The waiter-flag wait pattern (funnel.rs protocol doc): capture the
        // wake epoch, ARM the drain waiter flag, THEN sweep — a producer push
        // ordered after the arm-fence sees the flag and wakes (epoch bump),
        // one ordered before it is seen by the sweep. Park only on Idle.
        let seen = drain.park_epoch();
        drain.arm_wait();
        match drain.next() {
            DrainStep::Row(img) => {
                let cont = {
                    let mcx = estate.es_query_cxt;
                    let slot = estate.slot_mut(wire_slot);
                    // SAFETY: `wire_slot` is a Minimal slot (caller contract);
                    // `img` outlives this store+receive (dropped after). Store
                    // with shouldFree=false — `img` owns the bytes; the drain
                    // frees them after `receive_slot` has copied datums out.
                    unsafe {
                        ::exectuples::exec_store_minimal_tuple_ptr(slot, mcx, img.as_mtup_ptr());
                    }
                    let cont = dest.receive_slot(slot)?;
                    // Clear the borrowed pointer before freeing the image.
                    ::exectuples::exec_clear_tuple(slot, mcx);
                    cont
                };
                drop(img);
                emitted += 1;
                if !cont || limit.is_some_and(|n| emitted >= n) {
                    // Client stop or LIMIT satisfied: close demand → producers
                    // stop; we stop pulling (bounded rings + Drop reclaim tail).
                    funnel.close_demand();
                    break;
                }
            }
            DrainStep::Idle => {
                // All rings currently empty but producers live: park until a
                // producer pushes or marks done (on the epoch captured before
                // the sweep — lost-wakeup-free; see funnel.rs).
                drain.park(seen);
            }
            DrainStep::Eof => break,
        }
    }
    Ok(emitted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kill_switch_default_on_flipped() {
        // GL-FUNNEL-4 flip: default ON for the FloorGuard band; only an
        // explicit "0" kills. (Env unset in the default test process.)
        if std::env::var("PGRUST_RUNTIME_ROW_FUNNEL").is_err() {
            assert!(row_funnel_enabled(), "flipped-kill: default must be ON");
        } else if std::env::var("PGRUST_RUNTIME_ROW_FUNNEL").as_deref() == Ok("0") {
            assert!(!row_funnel_enabled(), "=0 must kill");
        }
    }

    #[test]
    fn min_image_byte_roundtrip() {
        // Owned image preserves bytes and is 8-aligned for MinimalTupleData.
        let bytes: Vec<u8> = (0u8..37).collect();
        let img = MinImage::from_bytes(&bytes);
        assert_eq!(img.len, bytes.len());
        assert_eq!(img.as_mtup_ptr().as_ptr() as usize % 8, 0);
        // SAFETY: img owns len bytes copied from `bytes`.
        let back = unsafe { std::slice::from_raw_parts(img.ptr.as_ptr(), img.len) };
        assert_eq!(back, &bytes[..]);
    }

    #[test]
    fn min_image_empty() {
        let img = MinImage::from_bytes(&[]);
        assert_eq!(img.len, 0);
        assert_eq!(img.as_mtup_ptr().as_ptr() as usize % 8, 0);
    }

    #[test]
    fn funnel_producer_consumer_roundtrip() {
        // The transport itself, exercised with owned images end to end.
        let f: Arc<RowFunnel<MinImage>> = RowFunnel::new(2, 8);
        let p0 = f.producer(0);
        let p1 = f.producer(1);
        p0.try_push(MinImage::from_bytes(&[1, 2, 3])).ok().unwrap();
        p1.try_push(MinImage::from_bytes(&[4, 5])).ok().unwrap();
        p0.mark_done();
        p1.mark_done();
        let mut d = f.drain();
        let mut lens = Vec::new();
        loop {
            match d.next() {
                DrainStep::Row(img) => lens.push(img.len),
                DrainStep::Eof => break,
                DrainStep::Idle => unreachable!("all done"),
            }
        }
        lens.sort();
        assert_eq!(lens, vec![2, 3]);
    }
}
