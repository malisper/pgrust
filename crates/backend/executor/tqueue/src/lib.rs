use std::sync::{Arc, Mutex};

use elog::ereport;
use latch::SetLatch;
use mcx::MemoryContext;
use shm_mq::{ShmMqHandle, ShmMqRecv, ShmMqResult};
use types_error::{
    ErrorLocation, PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR,
};
use types_slot::SlotData;
use types_storage::latch::LatchHandle;

pub const PARALLEL_TUPLE_QUEUE_SIZE: usize = 65536;

// Batch transport (docs/design/tqueue-batch-transfer.md): workers are threads
// in one process, so tuple payloads cross by pointer. Chunks of tuples are
// accumulated worker-side and handed to the leader through a bounded ledger;
// the shm_mq ring carries only fixed-size chunk indices, keeping C's shm_mq
// control flow (attach waits, counterparty_gone, detach drain) intact.
const CHUNK_CAPACITY: usize = 16384;
const LEDGER_NSLOTS: usize = 16;
const ALIGN: usize = 8;
const IDX_MSG_LEN: usize = core::mem::size_of::<usize>();

const fn maxalign(len: usize) -> usize {
    (len + (ALIGN - 1)) & !(ALIGN - 1)
}

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

// u64 words so tuples deform in place 8-aligned (mq_ring's MAXALIGN reason).
// Layout per tuple: usize byte length, payload, pad to 8.
pub struct TupleChunk {
    words: Box<[u64]>,
    used: usize,
}

impl TupleChunk {
    fn with_capacity(bytes: usize) -> Self {
        TupleChunk {
            words: vec![0u64; bytes.div_ceil(ALIGN)].into_boxed_slice(),
            used: 0,
        }
    }

    fn capacity(&self) -> usize {
        self.words.len() * ALIGN
    }

    fn base(&self) -> *const u8 {
        self.words.as_ptr().cast()
    }

    fn push(&mut self, tuple: &[u8]) {
        let need = ALIGN + maxalign(tuple.len());
        debug_assert!(self.used + need <= self.capacity());
        // SAFETY: in-bounds per the capacity check; used is 8-aligned so the
        // length word write is aligned.
        unsafe {
            let dst = self.words.as_mut_ptr().cast::<u8>().add(self.used);
            dst.cast::<usize>().write(tuple.len());
            core::ptr::copy_nonoverlapping(tuple.as_ptr(), dst.add(ALIGN), tuple.len());
        }
        self.used += need;
    }

    // Tuple starting at 8-aligned `cursor`; returns (payload, next cursor).
    fn tuple_at(&self, cursor: usize) -> (&[u8], usize) {
        debug_assert!(cursor + ALIGN <= self.used);
        // SAFETY: cursor is 8-aligned and in-bounds; push wrote a length word
        // followed by that many payload bytes here.
        unsafe {
            let p = self.base().add(cursor);
            let len = p.cast::<usize>().read();
            debug_assert!(cursor + ALIGN + len <= self.used);
            (core::slice::from_raw_parts(p.add(ALIGN), len), cursor + ALIGN + maxalign(len))
        }
    }
}

// Per-queue chunk handoff: the sender installs a finished chunk into a free
// slot and sends the slot index through the ring; the receiver takes the
// chunk by index. LEDGER_NSLOTS bounds in-flight memory (backpressure analog
// of C's 64KB mq_ring bound). Dropping the ledger frees any chunks a detach
// left in flight — no epoch bookkeeping needed.
pub struct ChunkLedger {
    slots: Mutex<Vec<Option<Box<TupleChunk>>>>,
}

impl ChunkLedger {
    pub fn new() -> Self {
        ChunkLedger { slots: Mutex::new((0..LEDGER_NSLOTS).map(|_| None).collect()) }
    }

    fn try_install(&self, chunk: Box<TupleChunk>) -> Result<usize, Box<TupleChunk>> {
        let mut slots = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        match slots.iter_mut().enumerate().find(|(_, s)| s.is_none()) {
            Some((i, slot)) => {
                *slot = Some(chunk);
                Ok(i)
            }
            None => Err(chunk),
        }
    }

    fn take(&self, idx: usize) -> Option<Box<TupleChunk>> {
        self.slots.lock().unwrap_or_else(|e| e.into_inner())[idx].take()
    }

    // Diagnostics (stall self-report): chunks currently in flight.
    fn in_flight(&self) -> usize {
        self.slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|s| s.is_some())
            .count()
    }
}

impl Default for ChunkLedger {
    fn default() -> Self {
        Self::new()
    }
}

// Engagement evidence, worker-local (no hot-path shared-line traffic);
// reported once at shutdown when PGRUST_TQUEUE_STATS is set.
#[derive(Default)]
struct TqueueStats {
    chunk_tuples: u64,
    byte_tuples: u64,
    chunks: u64,
}

pub struct DrTqueue {
    queue: Option<ShmMqHandle>,
    ledger: Option<Arc<ChunkLedger>>,
    chunk: Option<Box<TupleChunk>>,
    scratch: Option<MemoryContext>,
    stats: TqueueStats,
}

/// `CreateTupleQueueDestReceiver` (tqueue.c) — per-tuple copy path (fail-open
/// fallback; anything that can't batch, e.g. a future cross-process queue).
pub fn tqueue_create_DR(queue: ShmMqHandle) -> DrTqueue {
    DrTqueue {
        queue: Some(queue),
        ledger: None,
        chunk: None,
        scratch: None,
        stats: TqueueStats::default(),
    }
}

/// Batched variant: tuples accumulate into ledger chunks; the ring carries
/// chunk indices.
pub fn tqueue_create_DR_batched(queue: ShmMqHandle, ledger: Arc<ChunkLedger>) -> DrTqueue {
    DrTqueue {
        queue: Some(queue),
        ledger: Some(ledger),
        chunk: None,
        scratch: None,
        stats: TqueueStats::default(),
    }
}

impl DrTqueue {
    pub fn startup(&mut self, _operation: i32, _typeinfo: &types_tuple::TupleDescData<'_>) {}

    /// `tqueueReceiveSlot`: false = queue detached, stop early.
    pub fn receive_slot(&mut self, slot: &mut SlotData<'_>) -> PgResult<bool> {
        let DrTqueue { queue, ledger, chunk, scratch, stats } = self;
        let queue = queue.as_mut().expect("tqueueReceiveSlot after shutdown");
        // ExecFetchSlotMinimalTuple's no-copy arm.
        if let SlotData::Minimal(m) = &*slot {
            if let Some(p) = m.mintuple {
                // SAFETY: a stored minimal tuple is a live flat image of t_len bytes.
                let bytes = unsafe {
                    let t_len = p.as_ref().t_len as usize;
                    core::slice::from_raw_parts(p.as_ptr().cast::<u8>(), t_len)
                };
                return push_bytes(queue, ledger, chunk, stats, bytes);
            }
        }
        exectuples::slot_getallattrs(slot);
        let ctx = scratch.get_or_insert_with(|| MemoryContext::new_bump("tqueue"));
        let sent = {
            let mcx = ctx.mcx();
            let base = slot.base();
            let desc = base
                .tts_tupleDescriptor
                .as_ref()
                .expect("tqueueReceiveSlot: slot without descriptor");
            let natts = desc.natts as usize;
            let tup = heaptuple::heap_form_minimal_tuple(
                mcx,
                desc,
                &base.tts_values[..natts],
                &base.tts_isnull[..natts],
                0,
            )?;
            // SAFETY: a formed minimal tuple is a live flat image of t_len bytes.
            let bytes =
                unsafe { core::slice::from_raw_parts(tup.as_ptr(), tup.t_len() as usize) };
            push_bytes(queue, ledger, chunk, stats, bytes)?
        };
        ctx.reset();
        Ok(sent)
    }

    /// One tuple image into the transport; false = queue detached.
    pub fn push_tuple_bytes(&mut self, tuple: &[u8]) -> PgResult<bool> {
        let DrTqueue { queue, ledger, chunk, stats, .. } = self;
        push_bytes(
            queue.as_mut().expect("tqueueReceiveSlot after shutdown"),
            ledger,
            chunk,
            stats,
            tuple,
        )
    }

    /// Hand the pending chunk to the leader; false = queue detached.
    pub fn flush(&mut self) -> PgResult<bool> {
        let DrTqueue { queue, ledger, chunk, stats, .. } = self;
        let Some(ledger) = ledger else { return Ok(true) };
        flush_chunk(queue.as_mut().expect("tqueue flush after shutdown"), ledger, chunk, stats)
    }

    /// `tqueueShutdownReceiver`: flush the pending chunk, detach the queue.
    pub fn shutdown(&mut self) -> PgResult<()> {
        if self.queue.is_some() {
            self.flush()?;
        }
        if let Some(mut q) = self.queue.take() {
            q.detach();
        }
        if std::env::var_os("PGRUST_TQUEUE_STATS").is_some() {
            eprintln!(
                "tqueue-stats: chunk_tuples={} chunks={} byte_tuples={}",
                self.stats.chunk_tuples, self.stats.chunks, self.stats.byte_tuples
            );
        }
        Ok(())
    }
}

// Batched tqueueReceiveSlot core; false = queue detached.
fn push_bytes(
    queue: &mut ShmMqHandle,
    ledger: &Option<Arc<ChunkLedger>>,
    chunk: &mut Option<Box<TupleChunk>>,
    stats: &mut TqueueStats,
    tuple: &[u8],
) -> PgResult<bool> {
    let Some(ledger) = ledger else {
        stats.byte_tuples += 1;
        return tqueue_send_bytes(queue, tuple);
    };
    let need = ALIGN + maxalign(tuple.len());
    let full = chunk.as_ref().is_some_and(|c| c.used + need > c.capacity());
    if full && !flush_chunk(queue, ledger, chunk, stats)? {
        return Ok(false);
    }
    chunk
        .get_or_insert_with(|| Box::new(TupleChunk::with_capacity(CHUNK_CAPACITY.max(need))))
        .push(tuple);
    stats.chunk_tuples += 1;
    Ok(true)
}

fn flush_chunk(
    queue: &mut ShmMqHandle,
    ledger: &ChunkLedger,
    pending: &mut Option<Box<TupleChunk>>,
    stats: &mut TqueueStats,
) -> PgResult<bool> {
    let Some(mut chunk) = pending.take() else { return Ok(true) };
    stats.chunks += 1;

    // Stall self-report clock for the ledger-full wait; installing a chunk
    // is progress (the loop exits). Untouched when a slot is free.
    let mut stall = shm_mq::stall::StallDetector::new();
    let idx = loop {
        match ledger.try_install(chunk) {
            Ok(idx) => break idx,
            Err(back) => {
                chunk = back;
                if queue.queue().is_detached() {
                    return Ok(false);
                }
                // All slots in flight: the receiver frees one and sets our
                // latch (mirrors shm_mq's full-ring sender wait).
                shm_mq::stall::wait_on_my_latch_reporting(
                    init_small::globals::MyLatch(),
                    shm_mq::WAIT_EVENT_MQ_SEND,
                    &mut stall,
                    &mut |waited_ms| {
                        shm_mq::stall::report_queue_stall(
                            queue.queue(),
                            "send-ledger-full",
                            ledger.in_flight(),
                            waited_ms,
                        )
                    },
                )?;
            }
        }
    };

    match queue.send(&idx.to_ne_bytes(), false, true)? {
        ShmMqResult::Success => Ok(true),
        ShmMqResult::Detached => {
            // Receiver may already have taken it; reclaim if not.
            ledger.take(idx);
            Ok(false)
        }
        ShmMqResult::WouldBlock => {
            ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg("could not send tuple to shared-memory queue")
                .finish(loc("tqueueReceiveSlot"))?;
            unreachable!("ereport(ERROR) returned");
        }
    }
}


// tqueueReceiveSlot's queue-facing core (per-tuple path); false = detached.
pub fn tqueue_send_bytes(queue: &mut ShmMqHandle, tuple: &[u8]) -> PgResult<bool> {
    let result = queue.send(tuple, false, false)?;

    if result == ShmMqResult::Detached {
        return Ok(false);
    }
    if result != ShmMqResult::Success {
        ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("could not send tuple to shared-memory queue")
            .finish(loc("tqueueReceiveSlot"))?;
    }
    Ok(true)
}

pub struct TupleQueueReader {
    queue: ShmMqHandle,
    ledger: Option<Arc<ChunkLedger>>,
    current: Option<Box<TupleChunk>>,
    cursor: usize,
}

impl TupleQueueReader {
    /// Per-tuple copy path (matched to `tqueue_create_DR`).
    pub fn new(queue: ShmMqHandle) -> Self {
        Self { queue, ledger: None, current: None, cursor: 0 }
    }

    /// Batched path (matched to `tqueue_create_DR_batched` on the same queue).
    pub fn new_batched(queue: ShmMqHandle, ledger: Arc<ChunkLedger>) -> Self {
        Self { queue, ledger: Some(ledger), current: None, cursor: 0 }
    }

    /// Diagnostics (Gather leader stall self-report): this reader's queue.
    pub fn mq(&self) -> &shm_mq::ShmMq {
        self.queue.queue()
    }

    // TupleQueueReaderNext: the raw MinimalTuple byte image borrowed from the
    // transport (ring, reassembly buffer, or batch chunk), valid until the
    // next call. Detached => done = true and None; nowait with nothing ready
    // => None.
    pub fn next(&mut self, nowait: bool, done: &mut bool) -> PgResult<Option<&[u8]>> {
        *done = false;
        if self.ledger.is_none() {
            return match self.queue.receive(nowait)? {
                ShmMqRecv::Detached => {
                    *done = true;
                    Ok(None)
                }
                ShmMqRecv::WouldBlock => Ok(None),
                ShmMqRecv::Success(data) => Ok(Some(data)),
            };
        }

        if let Some(chunk) = &self.current {
            if self.cursor >= chunk.used {
                self.current = None;
                self.cursor = 0;
            }
        }
        if self.current.is_none() {
            let idx = match self.queue.receive(nowait)? {
                ShmMqRecv::Detached => {
                    *done = true;
                    return Ok(None);
                }
                ShmMqRecv::WouldBlock => return Ok(None),
                ShmMqRecv::Success(data) => {
                    debug_assert!(data.len() == IDX_MSG_LEN);
                    usize::from_ne_bytes(data.try_into().expect("chunk index message"))
                }
            };
            let ledger = self.ledger.as_ref().expect("batched reader has a ledger");
            let chunk = ledger.take(idx).expect("received chunk index is in flight");
            debug_assert!(chunk.used > 0, "empty chunks are never flushed");
            // A slot came free; wake a sender blocked in flush().
            if let Some(sender) = self.queue.queue().get_sender() {
                SetLatch(LatchHandle::proc(sender));
            }
            self.current = Some(chunk);
            self.cursor = 0;
        }
        let chunk = self.current.as_deref().expect("chunk just installed");
        let (tuple, next) = chunk.tuple_at(self.cursor);
        self.cursor = next;
        Ok(Some(tuple))
    }
}

#[cfg(test)]
mod tests;
