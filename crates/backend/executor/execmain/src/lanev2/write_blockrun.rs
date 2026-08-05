//! W2a increment 2 — worker-direct heap writes over transaction-private
//! block runs (parallel-writes design §4 rung W2;
//! scratchpad/night/w2-worker-writes-design.md).
//!
//! # Shape
//!
//! Increment 1 (pop-K drain batching, `write_funnel.rs`) shaved the funnel's
//! per-row drain tax but left the wall: every heap byte still flows through
//! ONE writer thread (workers ring-emit `MinImage`s, the leader alone runs
//! `write_buffer_receive`). This increment removes the writer from the hot
//! path entirely for the shapes it can own: each funnel WORKER opens the
//! write target itself, claims private disjoint block runs from a shared
//! [`BlockRunAllocator`], and runs the W1 multi-insert fill over its runs —
//! the leader drains nothing (the rings stay empty) and only sums row
//! counts at the seal.
//!
//! # Correctness spine (the design's census, as wired here)
//!
//! - identity: launched gang workers restore the leader's pre-assigned xid /
//!   fixed cids / snapshot / combocid / lock group (`parallel_worker_body`);
//!   the write stamps `output_cid` captured at receiver startup and threaded
//!   through shared state — no worker ever calls `GetCurrentCommandId(true)`
//!   or assigns an xid (the xact guards make either an ERROR).
//! - storage: page supply is the block-run mode of
//!   `RelationGetBufferForTuple` (bistate.block_run) — no FSM, no shared
//!   targblock, extension under the allocator's mutex with
//!   `EB_SKIP_EXTENSION_LOCK` (the target is created by this transaction
//!   under AccessExclusiveLock, so nothing else can extend or scan it).
//!   Disjoint runs mean no two participants ever content-lock the same
//!   target page.
//! - WAL: workers hold leased PGPROCs with real proc numbers — per-thread
//!   insert slots, spinlocked position reservation; the xl_prev chain is
//!   maintained by the reservation itself (multi-backend parity). The
//!   leader's commit record is inserted only after every worker sealed
//!   (`DestroyParallelContext` joins the gang before the engage returns), so
//!   the commit-LSN flush covers every worker record. Under
//!   wal_level=minimal the same-xact-created target skips WAL on EVERY
//!   participant — the worker-side `relation_needs_wal` probe is made
//!   leader-identical by the relcache `rd_firstRelfilelocatorSubid` restore
//!   (C c6b92041 parity, relcache/build.rs), and `begin` asserts agreement,
//!   failing the statement closed rather than splitting the relfilenode
//!   between logged and unlogged pages.
//! - the tripwire stays live: heapam's real `is_parallel_worker` refusal is
//!   wired (release-reachable), and this sink's writes ride an RAII
//!   [`ParallelWriteGuard`] token scoped to exactly its own calls.
//! - crash: the allocator is memory-only; a crash mid-load leaves an orphan
//!   relfilenode exactly like serial CTAS (workers' records target a
//!   relation whose creating transaction never committed).
//!
//! # Admission (STRUCTURAL only — the postmortem's no-estimate-cliffs law)
//!
//! IntoRel / TransientRel dest, heap AM, NO toast table on the target
//! (worker-side toast-relation + toast-index writes are W3), W1 buffering
//! armed, plain SKIP_FSM options (a future FROZEN lane must re-derive the VM
//! story before riding runs). Anything else falls back to increment 1's
//! batched leader drain, byte-identically.
//!
//! Knob: `PGRUST_W2A_BLOCKRUN` (default OFF; =1|on arms — the W2A family
//! spelling). Measurement lever until the GL-W2A-2 ladder rules.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use ::tableam::block_run::BlockRunAllocator;
use ::types_core::{CommandId, Oid};
use ::types_error::{PgError, PgResult, ERROR};

/// W2a inc-2 knob — **DEFAULT ON** since the GL-W2A-2 flip (t43; the
/// ladder's bar cleared, CONFIRM take-2 PASS). Kill spellings exactly
/// `0|off` (t35 flipped-kill idiom). The flip is guarded by the
/// STRUCTURAL min-dop-4 floor in `try_arm` (the letter's flip shape:
/// below dop 4 the run-claim contention eats the win — the floor is
/// structural, not a tuning knob).
pub(super) fn w2a_blockrun_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_W2A_BLOCKRUN").as_deref(), Ok("0") | Ok("off"))
    })
}

/// Run-map tracing for the e2e placement oracle (`PGRUST_W2A_BLOCKRUN_RUNMAP=1`):
/// the allocator records claims and the seal trace dumps them. O(runs), e2e
/// only — never armed on a perf leg.
fn runmap_trace_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        matches!(std::env::var("PGRUST_W2A_BLOCKRUN_RUNMAP").as_deref(), Ok("1") | Ok("on"))
    })
}

/// Per-engagement shared state: the allocator plus the receiver-captured
/// write parameters every worker needs, and the sealed row count.
pub(super) struct BlockRunShared {
    pub reloid: Oid,
    pub output_cid: CommandId,
    pub ti_options: i32,
    /// The LEADER's `relation_needs_wal` verdict for the target; every
    /// worker must agree or the engagement fails closed (§ module doc).
    pub needs_wal: bool,
    pub allocator: Arc<BlockRunAllocator>,
    /// Rows sealed by finished workers (leader reads after the gang join).
    pub rows: AtomicU64,
    /// Workers that BEGAN a write state (engagement witness).
    pub writers: AtomicUsize,
}

/// Structural admission against the started-up write receiver. `None` =
/// fall back to the increment-1 leader drain (fail-closed).
pub(super) fn try_arm(dest: &::tcop_dest::DestReceiver<'_>) -> Option<Arc<BlockRunShared>> {
    if !w2a_blockrun_enabled() {
        return None;
    }
    // GL-W2A-2 flip floor (STRUCTURAL, rides the default-ON posture):
    // below dop 4 the block-run claim path loses to the increment-1
    // leader drain (the ladder's low-dop cells) — fall back closed.
    if ::guc_tables::runtime_pool::runtime_dop() < 4 {
        return None;
    }
    // Both rewrite receivers carry the same field set; destructure per arm.
    let (rel, output_cid, ti_options, mibuf_armed, bistate_armed) = match dest {
        ::tcop_dest::DestReceiver::IntoRel(st) => (
            st.rel.as_ref()?,
            st.output_cid,
            st.ti_options,
            st.mibuf.is_some(),
            st.bistate.is_some(),
        ),
        ::tcop_dest::DestReceiver::TransientRel(st) => (
            st.rel.as_ref()?,
            st.output_cid,
            st.ti_options,
            st.mibuf.is_some(),
            st.bistate.is_some(),
        ),
        _ => return None,
    };
    // W1 buffering + bulk-insert state armed (the worker fill IS the W1
    // fill); killed W1 keeps the whole stack on the per-tuple leader drain.
    if !mibuf_armed || !bistate_armed {
        return None;
    }
    // Heap AM only.
    if !matches!(::tableam::TableAm::of(rel), Some(::tableam::TableAm::Heap)) {
        return None;
    }
    // No toast table: a toasted row from a worker would write the toast
    // relation AND its index from a parallel worker (W3 territory). The
    // rewrite targets get their toast rel at create time, so this is a
    // stable structural fact of the statement.
    if rel.rd_rel.reltoastrelid != ::types_core::InvalidOid {
        return None;
    }
    // Exactly the options the run fill was derived for (SKIP_FSM, no
    // FROZEN): a future frozen matview lane must fold in the per-run
    // VM-batching design before riding runs.
    if ti_options != ::tableam::TABLE_INSERT_SKIP_FSM {
        return None;
    }
    let needs_wal = ::heapam::relation_needs_wal(rel);
    super::lane_trace(&format!(
        "blockrun: armed reloid={} needs_wal={needs_wal}",
        rel.rd_id
    ));
    Some(Arc::new(BlockRunShared {
        reloid: rel.rd_id,
        output_cid,
        ti_options,
        needs_wal,
        allocator: Arc::new(BlockRunAllocator::new(runmap_trace_enabled())),
        rows: AtomicU64::new(0),
        writers: AtomicUsize::new(0),
    }))
}

/// One worker's private write half: its own relcache handle on the target,
/// its W1 multi-insert buffer, and a bulk-insert state whose page supply is
/// the shared run allocator. Field order is the drop order: the buffer,
/// bistate (pin) and relation handle die before `ctx`, which owns every
/// arena allocation they reference.
pub(super) struct WorkerWriteState {
    shared: Arc<BlockRunShared>,
    rel: Option<::types_rel::Relation<'static>>,
    mibuf: ::tableam::WriteMultiInsertBuffer<'static>,
    bistate: ::tableam::BulkInsertStateData,
    rows: u64,
    mcx: ::mcx::Mcx<'static>,
    /// Owns the buffer's pool-slot arena. LAST field (drop order). BOXED:
    /// `Mcx<'a>` is literally `&'a MemoryContext`, so the owner's ADDRESS
    /// must survive this struct moving (begin() return, thread-local
    /// install) — the heap box pins it; `mcx` and every retained allocator
    /// handle inside the pool slots point into the box.
    #[allow(dead_code)]
    ctx: Box<::mcx::MemoryContext>,
}

impl WorkerWriteState {
    /// Open the target and build the write half (first claimed morsel of the
    /// drive). Fails closed on any disagreement with the leader's captured
    /// facts.
    pub(super) fn begin(shared: &Arc<BlockRunShared>) -> PgResult<WorkerWriteState> {
        let ctx = Box::new(::mcx::MemoryContext::new("w2a blockrun worker write"));
        // SAFETY ('static erasure): `Mcx` borrows the (heap-pinned, boxed)
        // MemoryContext, whose address is stable across every move of this
        // struct and which drops LAST per the declared field order; the
        // struct is thread-local and never crosses threads.
        let mcx: ::mcx::Mcx<'static> = unsafe { std::mem::transmute((*ctx).mcx()) };
        // The leader holds AccessExclusiveLock on the self-created target;
        // this worker is a lock-group member, so the ordinary insert
        // lockmode is granted without conflict.
        let rel: ::types_rel::Relation<'static> =
            ::table::table_open(mcx, shared.reloid, ::types_rel::RowExclusiveLock)?;
        // needs_wal agreement (module doc): a split would mix logged and
        // unlogged pages in one relfilenode — refuse, never wing it.
        let worker_needs_wal = ::heapam::relation_needs_wal(&rel);
        if worker_needs_wal != shared.needs_wal {
            // Close before erroring (the Err path drops the handle, which is
            // the abort-path close; be explicit for the refcount).
            let _ = ::table::table_close(rel, ::types_rel::NoLock);
            return Err(Box::new(PgError::new(
                ERROR,
                "blockrun worker disagrees with leader on relation WAL posture",
            )));
        }
        let mut bistate = ::heapam::GetBulkInsertState();
        bistate.block_run = Some(Arc::clone(&shared.allocator));
        shared.writers.fetch_add(1, Ordering::SeqCst);
        super::lane_trace("blockrun: worker begin");
        Ok(WorkerWriteState {
            shared: Arc::clone(shared),
            rel: Some(rel),
            mibuf: ::tableam::WriteMultiInsertBuffer::new(),
            bistate,
            rows: 0,
            mcx,
            ctx,
        })
    }

    /// Buffer one produced row (the worker-side `receive_slot` body): copy
    /// into the W1 pool, flush through `table_multi_insert` on the W1
    /// thresholds — every flushed page comes from this worker's private run.
    pub(super) fn receive(
        &mut self,
        estate: &mut ::executils::EStateData<'_>,
        tuple: ::executils::ExecSlotId,
    ) -> PgResult<()> {
        let slot = estate.slot_mut(tuple);
        // SAFETY (lifetime bridge, the dest-seam pattern): the receive body
        // only COPIES datums out of `slot` into its own pool slot during the
        // call and retains no borrow of it.
        let slot: &mut ::types_slot::SlotData<'static> = unsafe {
            &mut *(slot as *mut ::types_slot::SlotData<'_>).cast::<::types_slot::SlotData<'static>>()
        };
        let rel = self.rel.as_ref().expect("live until seal/abandon");
        // The parallel-write token brackets exactly this call chain.
        let _permit = ::heapam::ParallelWriteGuard::new();
        ::tableam::write_buffer::write_buffer_receive(
            self.mcx,
            rel,
            &mut self.mibuf,
            slot,
            self.shared.output_cid,
            self.shared.ti_options,
            Some(&mut self.bistate),
        )?;
        self.rows += 1;
        Ok(())
    }

    /// Clean-completion seal: flush the buffered tail, release the pin,
    /// close the target, publish the row count. Runs BEFORE the worker
    /// exits, hence before the leader's gang join observes completion.
    pub(super) fn seal(mut self) -> PgResult<()> {
        let rel = self.rel.take().expect("seal runs once");
        {
            let _permit = ::heapam::ParallelWriteGuard::new();
            ::tableam::write_buffer::write_buffer_flush(
                self.mcx,
                &rel,
                &mut self.mibuf,
                self.shared.output_cid,
                self.shared.ti_options,
                Some(&mut self.bistate),
            )?;
        }
        ::heapam::ReleaseBulkInsertStatePin(&mut self.bistate);
        ::table::table_close(rel, ::types_rel::NoLock)?;
        self.shared.rows.fetch_add(self.rows, Ordering::SeqCst);
        super::lane_trace(&format!("blockrun: worker seal rows={}", self.rows));
        Ok(())
    }

    /// Error-path teardown: DROP the unflushed buffer (the receiver abort
    /// discipline — the aborted transaction kills the flushed pages with the
    /// relfilenode), release the pin, close the target, count nothing.
    pub(super) fn abandon(mut self) {
        ::heapam::ReleaseBulkInsertStatePin(&mut self.bistate);
        if let Some(rel) = self.rel.take() {
            let _ = ::table::table_close(rel, ::types_rel::NoLock);
        }
        super::lane_trace("blockrun: worker abandon");
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    /// Flipped-kill pin (GL-W2A-2 flip, t43): default ON; `0|off` kills.
    #[test]
    fn blockrun_default_on_flipped_kill() {
        if std::env::var("PGRUST_W2A_BLOCKRUN").is_err() {
            assert!(w2a_blockrun_enabled(), "blockrun defaults ON since the GL-W2A-2 flip");
        }
    }
}
