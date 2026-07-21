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
//! # Status: KILL-SWITCH GATED, DEFAULT OFF ([`row_funnel_enabled`]).
//!
//! This increment provides the producer sink + leader drain + the owned
//! bounded transport payload, compiled against the real `Sink`/slot APIs, but
//! is NOT wired into `route_to` / the coverage matrix and is NOT reachable
//! unless the kill switch is set. The scheduler-side wiring (publishing the
//! row-emit taskset, parking producers on the ring under the K-standby permit,
//! running the leader drain in place of `CompletionWaiter::wait`) lands with
//! the fleet A/B that proves ≥ parity — the discipline the plan mandates.

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

use super::push::{Sink, SinkFeed};

/// Kill switch (default OFF): `PGRUST_RUNTIME_ROW_FUNNEL=1`/`on` arms World-B
/// parallel lane row-emit. One process-static resolve (the
/// `PGRUST_RUNTIME_*` precedent, `runtime/src/sched.rs`).
pub(super) fn row_funnel_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        matches!(std::env::var("PGRUST_RUNTIME_ROW_FUNNEL").as_deref(), Ok("1") | Ok("on"))
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
    fn as_mtup_ptr(&self) -> NonNull<MinimalTupleData> {
        self.ptr.cast::<MinimalTupleData>()
    }
}

impl Drop for MinImage {
    fn drop(&mut self) {
        // SAFETY: allocated by `from_bytes` with exactly this layout.
        unsafe { std::alloc::dealloc(self.ptr.as_ptr(), Self::layout(self.len)) };
    }
}

/// Worker-side terminal sink of the parallel lane push island (World-B). Slots
/// in where the serial lane uses `RootAdapter`: instead of a capacity-one PG
/// pull buffer, it appends the projected tuple into this worker's funnel ring.
///
/// Back-pressure: [`accept`](RowEmitSink::accept) is NON-blocking — a full ring
/// returns [`SinkFeed::Full`], which pauses the push pipeline (`OpStatus::
/// Paused`); the worker loop then parks on the ring (the scheduler wiring
/// donates the permit via `runtime::blocking_io_section`). The boundary tuple
/// is held in `pending` so it is re-pushed, never lost, when the pipeline
/// resumes (the `RootAdapter`-overfill law: no silent row loss).
pub(super) struct RowEmitSink {
    producer: FunnelProducer<MinImage>,
    /// Scratch bump context to FORM the minimal tuple before copying it into an
    /// owned image; reset after each row so it never grows (the images, not the
    /// scratch, carry the bounded memory).
    scratch: ::mcx::MemoryContext,
    /// Boundary tuple saved on `SinkFeed::Full` — re-pushed on resume.
    pending: Option<MinImage>,
    clear_on_finish: Option<ExecSlotId>,
}

impl RowEmitSink {
    pub(super) fn new(
        producer: FunnelProducer<MinImage>,
        clear_on_finish: Option<ExecSlotId>,
    ) -> RowEmitSink {
        RowEmitSink {
            producer,
            scratch: ::mcx::MemoryContext::new_bump("lane-row-emit"),
            pending: None,
            clear_on_finish,
        }
    }
}

impl<'mcx> Sink<'mcx> for RowEmitSink {
    fn accept(&mut self, tuple: ExecSlotId, estate: &mut EStateData<'mcx>) -> PgResult<SinkFeed> {
        // A parallel row-emit must never ride an EPQ recheck (the non-lane
        // tail): the push path already carves these out; assert the invariant.
        debug_assert!(!estate.es_epq_active, "RowEmitSink inside an EPQ drive");

        // Materialize the row to push: reuse a saved boundary tuple, else form
        // a fresh owned image from the produced slot.
        let img = match self.pending.take() {
            Some(p) => p,
            None => {
                let slot_mcx = estate.es_query_cxt;
                let mt = {
                    let slot = estate.slot_mut(tuple);
                    ::exectuples::exec_copy_slot_minimal_tuple(slot, slot_mcx, self.scratch.mcx(), 0)?
                };
                let img = MinImage::from_bytes(mt.as_bytes());
                drop(mt);
                // Bounded scratch: the owned image carries the bytes now.
                self.scratch.reset();
                img
            }
        };

        match self.producer.try_push(img) {
            Ok(()) => {
                estate.es_processed += 1;
                // Capacity-N buffer: unlike RootAdapter it can take more.
                Ok(SinkFeed::NeedMore)
            }
            Err(back) => {
                // Ring full: save the boundary tuple and pause the pipeline.
                self.pending = Some(back);
                Ok(SinkFeed::Full)
            }
        }
    }

    fn finish(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        // Flush a saved boundary tuple before finishing — block (donating the
        // permit) until the leader frees space, or stop if demand closed.
        if let Some(img) = self.pending.take() {
            let _ = self.producer.push_blocking(img, ::runtime::blocking_io_section);
        }
        // Publish producer-done so the leader drain can reach EOF once this
        // ring is also drained (the streaming taskset's finalize contract).
        self.producer.mark_done();
        if let Some(slot) = self.clear_on_finish {
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(slot), mcx);
        }
        Ok(())
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
        // Capture the wake epoch BEFORE the drain sweep (canonical
        // lost-wakeup-free order, matching the pool worker loop): a producer
        // push/mark-done between the sweep and the park bumps the epoch, so the
        // park returns immediately and the outer loop re-drains.
        let seen = drain.park_epoch();
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
    fn kill_switch_default_off() {
        // No env set in the default test process → World-B stays OFF.
        if std::env::var("PGRUST_RUNTIME_ROW_FUNNEL").is_err() {
            assert!(!row_funnel_enabled());
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
