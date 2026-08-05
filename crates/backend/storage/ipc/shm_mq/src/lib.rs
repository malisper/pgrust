use std::cell::UnsafeCell;
use std::mem::size_of;
use std::ptr;
use std::sync::atomic::{compiler_fence, fence, AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::Arc;

use elog::ereport;
use init_small::globals::{MyLatch, MyProcNumber};
use latch::{ResetLatch, SetLatch, WaitLatch};
use types_core::{ProcNumber, INVALID_PROC_NUMBER};
use types_error::{ErrorLocation, PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR};
use types_storage::latch::LatchHandle;
use types_storage::storage::Spinlock;
use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET};

const MAXIMUM_ALIGNOF: usize = 8;
// MaxAllocSize (utils/memutils.h).
const MAX_ALLOC_SIZE: usize = 0x3FFF_FFFF;
const LEN_WORD: usize = size_of::<usize>();

const PG_WAIT_IPC: u32 = 0x0800_0000;
// PG_WAIT_IPC | index in wait_event_names.txt's IPC section.
const WAIT_EVENT_MQ_INTERNAL: u32 = PG_WAIT_IPC + 33;
const WAIT_EVENT_MQ_RECEIVE: u32 = PG_WAIT_IPC + 35;
pub const WAIT_EVENT_MQ_SEND: u32 = PG_WAIT_IPC + 36;

// Divergence from C: the ring is its own Arc'd allocation, not the tail of an
// in-segment header, so the minimum is just one MAXALIGN'd chunk.
pub const fn shm_mq_minimum_size() -> usize {
    MAXIMUM_ALIGNOF
}

const fn maxalign(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

const fn maxalign_down(len: usize) -> usize {
    len & !(MAXIMUM_ALIGNOF - 1)
}

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

// SPSC invariants: mq_receiver/mq_bytes_read change only via the receiver,
// mq_sender/mq_bytes_written only via the sender; identities are set once
// under mq_mutex and lock-free to read once known set; counters are ordered
// against mq_ring access by explicit fences; mq_detached goes false->true.
pub struct ShmMq {
    mq_mutex: Spinlock,
    mq_receiver: AtomicI32,
    mq_sender: AtomicI32,
    mq_bytes_read: AtomicU64,
    mq_bytes_written: AtomicU64,
    mq_detached: AtomicBool,
    mq_ring_size: usize,
    // u64 words: MinimalTuple images are deformed in place out of the ring
    // (tqueue.c returns queue memory), so the base must be MAXALIGN'd.
    mq_ring: Box<[UnsafeCell<u64>]>,
}

// SAFETY: the sender writes only unread ring bytes and the receiver reads
// only written ones (fence-ordered); everything else is atomics + spinlock.
unsafe impl Send for ShmMq {}
unsafe impl Sync for ShmMq {}

pub fn shm_mq_create(size: usize) -> Arc<ShmMq> {
    let ring_size = maxalign_down(size);
    assert!(ring_size >= shm_mq_minimum_size());
    Arc::new(ShmMq {
        mq_mutex: Spinlock::new(),
        mq_receiver: AtomicI32::new(INVALID_PROC_NUMBER),
        mq_sender: AtomicI32::new(INVALID_PROC_NUMBER),
        mq_bytes_read: AtomicU64::new(0),
        mq_bytes_written: AtomicU64::new(0),
        mq_detached: AtomicBool::new(false),
        mq_ring_size: ring_size,
        mq_ring: (0..ring_size / MAXIMUM_ALIGNOF).map(|_| UnsafeCell::new(0)).collect(),
    })
}

impl ShmMq {
    fn spin_acquire(&self) {
        if self.mq_mutex.tas() != 0 {
            let mut delay =
                s_lock_seams::SpinDelayStatus::new(file!(), line!() as i32, "shm_mq");
            while self.mq_mutex.tas_spin() != 0 {
                s_lock_seams::perform_spin_delay::call(&mut delay);
            }
            s_lock_seams::finish_spin_delay::call(&delay);
        }
    }

    fn receiver(&self) -> Option<ProcNumber> {
        proc_opt(self.mq_receiver.load(Ordering::Relaxed))
    }

    fn sender(&self) -> Option<ProcNumber> {
        proc_opt(self.mq_sender.load(Ordering::Relaxed))
    }

    fn detached(&self) -> bool {
        self.mq_detached.load(Ordering::Relaxed)
    }

    // For layered transports (tqueue batch chunks): observable detach state,
    // same Relaxed read the send/receive paths use.
    pub fn is_detached(&self) -> bool {
        self.detached()
    }

    fn set_detached(&self) {
        self.mq_detached.store(true, Ordering::Relaxed);
    }

    fn bytes_read(&self) -> u64 {
        self.mq_bytes_read.load(Ordering::Relaxed)
    }

    fn bytes_written(&self) -> u64 {
        self.mq_bytes_written.load(Ordering::Relaxed)
    }

    fn ring_ptr(&self, offset: usize) -> *mut u8 {
        debug_assert!(offset < self.mq_ring_size);
        // SAFETY: UnsafeCell<u64> is layout-transparent; offset is in bounds.
        unsafe { self.mq_ring.as_ptr().cast::<u8>().cast_mut().add(offset) }
    }

    pub fn set_receiver(&self, proc: ProcNumber) {
        self.spin_acquire();
        debug_assert!(self.receiver().is_none());
        self.mq_receiver.store(proc, Ordering::Relaxed);
        let sender = self.sender();
        self.mq_mutex.unlock();
        if let Some(sender) = sender {
            SetLatch(LatchHandle::proc(sender));
        }
    }

    pub fn set_sender(&self, proc: ProcNumber) {
        self.spin_acquire();
        debug_assert!(self.sender().is_none());
        self.mq_sender.store(proc, Ordering::Relaxed);
        let receiver = self.receiver();
        self.mq_mutex.unlock();
        if let Some(receiver) = receiver {
            SetLatch(LatchHandle::proc(receiver));
        }
    }

    pub fn get_receiver(&self) -> Option<ProcNumber> {
        self.spin_acquire();
        let receiver = self.receiver();
        self.mq_mutex.unlock();
        receiver
    }

    pub fn get_sender(&self) -> Option<ProcNumber> {
        self.spin_acquire();
        let sender = self.sender();
        self.mq_mutex.unlock();
        sender
    }

    fn inc_bytes_read(&self, n: usize) {
        // Release: prior mq_ring reads happen-before the bump that licenses
        // the sender to overwrite; pairs with send_bytes' SeqCst fence.
        // (C's pg_read_barrier relies on a hardware dependent-write argument
        // that has no C11/Rust-model equivalent.)
        fence(Ordering::Release);
        let cur = self.bytes_read();
        self.mq_bytes_read.store(cur + n as u64, Ordering::Relaxed);
        let sender = self
            .sender()
            .expect("bytes to read imply mq_sender is set");
        SetLatch(LatchHandle::proc(sender));
    }

    fn inc_bytes_written(&self, n: usize) {
        // Prior mq_ring writes visible before the bump; pairs with receive_bytes.
        fence(Ordering::Release);
        let cur = self.bytes_written();
        self.mq_bytes_written.store(cur + n as u64, Ordering::Relaxed);
    }

    // `me` is the handle's attach-time identity, NOT MyProcNumber(): handles
    // are dropped during FATAL unwinds after the exit callbacks (ProcKill)
    // already released this thread's proc identity — C never hits this order
    // because _exit skips stack cleanup and dsm_detach runs before ProcKill.
    fn detach_internal(&self, me: ProcNumber) {
        self.spin_acquire();
        let victim = if self.sender() == Some(me) {
            self.receiver()
        } else {
            debug_assert!(self.receiver() == Some(me));
            self.sender()
        };
        self.set_detached();
        self.mq_mutex.unlock();
        if let Some(victim) = victim {
            SetLatch(LatchHandle::proc(victim));
        }
    }
}

fn proc_opt(raw: ProcNumber) -> Option<ProcNumber> {
    if raw == INVALID_PROC_NUMBER {
        None
    } else {
        Some(raw)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShmMqResult {
    Success,
    WouldBlock,
    Detached,
}

#[derive(Debug)]
pub enum ShmMqRecv<'a> {
    Success(&'a [u8]),
    WouldBlock,
    Detached,
}

pub struct ShmMqHandle {
    mqh_queue: Arc<ShmMq>,
    // Reassembly buffer: retained global-heap storage (the handle outlives
    // query contexts, like the Arc'd ring); grows, never shrinks. u64 words
    // for the same MAXALIGN reason as mq_ring.
    mqh_buffer: Vec<u64>,
    // BackgroundWorkerHandle's (slot, generation); status flows through
    // bgworker_seams (direct dep cycles via tcop).
    mqh_handle: Option<(i32, u64)>,
    mqh_consume_pending: usize,
    mqh_send_pending: usize,
    mqh_partial_bytes: usize,
    mqh_expected_bytes: usize,
    mqh_length_word_complete: bool,
    mqh_counterparty_attached: bool,
    mqh_detached: bool,
    my_latch: Option<LatchHandle>,
    // Attach-time identity; detach must not consult MyProcNumber() (above).
    mqh_self: ProcNumber,
}

pub fn shm_mq_attach(mq: Arc<ShmMq>) -> ShmMqHandle {
    debug_assert!({
        let me = MyProcNumber();
        mq.receiver() == Some(me) || mq.sender() == Some(me)
    });
    ShmMqHandle {
        mqh_queue: mq,
        mqh_buffer: Vec::new(),
        mqh_handle: None,
        mqh_consume_pending: 0,
        mqh_send_pending: 0,
        mqh_partial_bytes: 0,
        mqh_expected_bytes: 0,
        mqh_length_word_complete: false,
        mqh_counterparty_attached: false,
        mqh_detached: false,
        my_latch: MyLatch(),
        mqh_self: MyProcNumber(),
    }
}

#[derive(Clone, Copy)]
enum WaitTarget {
    Sender,
    Receiver,
}

impl ShmMqHandle {
    pub fn queue(&self) -> &Arc<ShmMq> {
        &self.mqh_queue
    }

    /// `shm_mq_set_handle`: lets waits notice a counterparty that died
    /// before attaching. Takes the BackgroundWorkerHandle's (slot, generation).
    pub fn set_handle(&mut self, slot: i32, generation: u64) {
        debug_assert!(self.mqh_handle.is_none());
        self.mqh_handle = Some((slot, generation));
    }

    // shm_mq_counterparty_gone.
    fn counterparty_gone(&self) -> bool {
        if self.mqh_queue.detached() {
            return true;
        }
        if let Some((slot, generation)) = self.mqh_handle {
            if bgworker_seams::background_worker_stopped::call(slot, generation) {
                self.mqh_queue.set_detached();
                return true;
            }
        }
        false
    }

    // Once a message is partially written it cannot be aborted except by
    // detach: a nowait WouldBlock return must be retried with the same data.
    pub fn send(
        &mut self,
        data: &[u8],
        nowait: bool,
        force_flush: bool,
    ) -> PgResult<ShmMqResult> {
        debug_assert!(self.mqh_queue.sender() == Some(MyProcNumber()));
        let nbytes = data.len();

        if nbytes > MAX_ALLOC_SIZE {
            ereport(ERROR)
                .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .errmsg(format!(
                    "cannot send a message of size {nbytes} via shared memory queue"
                ))
                .finish(loc("shm_mq_send"))?;
        }

        while !self.mqh_length_word_complete {
            debug_assert!(self.mqh_partial_bytes < LEN_WORD);
            let len_word = nbytes.to_ne_bytes();
            let (res, written) = self.send_bytes(&len_word[self.mqh_partial_bytes..], nowait)?;
            if res == ShmMqResult::Detached {
                self.mqh_partial_bytes = 0;
                self.mqh_length_word_complete = false;
                return Ok(res);
            }
            self.mqh_partial_bytes += written;
            if self.mqh_partial_bytes >= LEN_WORD {
                debug_assert!(self.mqh_partial_bytes == LEN_WORD);
                self.mqh_partial_bytes = 0;
                self.mqh_length_word_complete = true;
            }
            if res != ShmMqResult::Success {
                return Ok(res);
            }
        }

        while self.mqh_partial_bytes < nbytes {
            let (res, written) = self.send_bytes(&data[self.mqh_partial_bytes..], nowait)?;
            if res == ShmMqResult::Detached {
                self.mqh_partial_bytes = 0;
                self.mqh_length_word_complete = false;
                return Ok(res);
            }
            self.mqh_partial_bytes += written;
            if res != ShmMqResult::Success {
                return Ok(res);
            }
        }

        self.mqh_partial_bytes = 0;
        self.mqh_length_word_complete = false;

        if self.mqh_queue.detached() {
            return Ok(ShmMqResult::Detached);
        }

        let receiver = if self.mqh_counterparty_attached {
            self.mqh_queue.receiver()
        } else {
            let receiver = self.mqh_queue.get_receiver();
            if receiver.is_some() {
                self.mqh_counterparty_attached = true;
            }
            receiver
        };

        if force_flush || self.mqh_send_pending > (self.mqh_queue.mq_ring_size >> 2) {
            self.mqh_queue.inc_bytes_written(self.mqh_send_pending);
            if let Some(receiver) = receiver {
                SetLatch(LatchHandle::proc(receiver));
            }
            self.mqh_send_pending = 0;
        }

        Ok(ShmMqResult::Success)
    }

    // On Success the payload borrows the ring (contiguous message) or the
    // reassembly buffer, valid until the next call on this handle.
    pub fn receive(&mut self, nowait: bool) -> PgResult<ShmMqRecv<'_>> {
        debug_assert!(self.mqh_queue.receiver() == Some(MyProcNumber()));

        if !self.mqh_counterparty_attached {
            if nowait {
                // Gone-check BEFORE the sender identity (C's attach/detach
                // race note); a sender that attached and detached still gets
                // drained below.
                let counterparty_gone = self.counterparty_gone();
                if self.mqh_queue.get_sender().is_none() {
                    return Ok(if counterparty_gone {
                        ShmMqRecv::Detached
                    } else {
                        ShmMqRecv::WouldBlock
                    });
                }
            } else if !wait_internal(
                &self.mqh_queue,
                WaitTarget::Sender,
                self.my_latch,
                self.mqh_handle,
            )? && self.mqh_queue.get_sender().is_none()
            {
                self.mqh_queue.set_detached();
                return Ok(ShmMqRecv::Detached);
            }
            self.mqh_counterparty_attached = true;
        }

        if self.mqh_consume_pending > self.mqh_queue.mq_ring_size / 4 {
            self.mqh_queue.inc_bytes_read(self.mqh_consume_pending);
            self.mqh_consume_pending = 0;
        }

        let mut rb: usize = 0;
        let mut rawdata: *const u8 = ptr::null();

        if !self.mqh_length_word_complete {
            // sizeof(Size) == MAXIMUM_ALIGNOF, so the length word is always
            // whole and contiguous (C's split-word path is unreachable here).
            debug_assert!(self.mqh_partial_bytes == 0);
            let (res, this_rb, this_raw) = self.receive_bytes(LEN_WORD, nowait)?;
            match res {
                ShmMqResult::Success => {}
                ShmMqResult::WouldBlock => return Ok(ShmMqRecv::WouldBlock),
                ShmMqResult::Detached => return Ok(ShmMqRecv::Detached),
            }
            rb = this_rb;
            rawdata = this_raw;
            debug_assert!(rb >= LEN_WORD);
            // SAFETY: receive_bytes returned >= LEN_WORD contiguous ring bytes.
            let nbytes = unsafe { read_usize(rawdata) };

            let needed = maxalign(LEN_WORD) + maxalign(nbytes);
            if rb >= needed {
                self.mqh_consume_pending += needed;
                // SAFETY: whole contiguous message, owned by the receiver
                // until marked consumed (not before the next call).
                let payload = unsafe { slice_from_raw(rawdata.add(maxalign(LEN_WORD)), nbytes) };
                return Ok(ShmMqRecv::Success(payload));
            }

            self.mqh_expected_bytes = nbytes;
            self.mqh_length_word_complete = true;
            self.mqh_consume_pending += maxalign(LEN_WORD);
            rb = 0;
            rawdata = ptr::null();
        }
        let nbytes = self.mqh_expected_bytes;

        if nbytes > MAX_ALLOC_SIZE {
            ereport(ERROR)
                .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .errmsg(format!("invalid message size {nbytes} in shared memory queue"))
                .finish(loc("shm_mq_receive"))?;
        }

        if self.mqh_partial_bytes == 0 {
            let (res, this_rb, this_raw) = self.receive_bytes(nbytes, nowait)?;
            match res {
                ShmMqResult::Success => {}
                ShmMqResult::WouldBlock => return Ok(ShmMqRecv::WouldBlock),
                ShmMqResult::Detached => return Ok(ShmMqRecv::Detached),
            }
            rb = this_rb;
            rawdata = this_raw;
            if rb >= nbytes {
                self.mqh_length_word_complete = false;
                self.mqh_consume_pending += maxalign(nbytes);
                // SAFETY: as above — contiguous, unconsumed until the next call.
                let payload = unsafe { slice_from_raw(rawdata, nbytes) };
                return Ok(ShmMqRecv::Success(payload));
            }
        }

        if self.mqh_buffer.len() * MAXIMUM_ALIGNOF < nbytes {
            let newbuflen = usize::min(nbytes.next_power_of_two(), MAX_ALLOC_SIZE);
            self.mqh_buffer.resize(newbuflen.div_ceil(MAXIMUM_ALIGNOF), 0);
        }

        loop {
            debug_assert!(self.mqh_partial_bytes + rb <= nbytes);
            if rb > 0 {
                // SAFETY: rb contiguous receiver-owned ring bytes; destination
                // in bounds (buffer length >= nbytes).
                unsafe {
                    ptr::copy_nonoverlapping(
                        rawdata,
                        self.mqh_buffer.as_mut_ptr().cast::<u8>().add(self.mqh_partial_bytes),
                        rb,
                    );
                }
                self.mqh_partial_bytes += rb;
            }

            debug_assert!(self.mqh_partial_bytes == nbytes || rb == maxalign(rb));
            self.mqh_consume_pending += maxalign(rb);

            if self.mqh_partial_bytes >= nbytes {
                break;
            }

            let still_needed = nbytes - self.mqh_partial_bytes;
            let (res, this_rb, this_raw) = self.receive_bytes(still_needed, nowait)?;
            match res {
                ShmMqResult::Success => {}
                ShmMqResult::WouldBlock => return Ok(ShmMqRecv::WouldBlock),
                ShmMqResult::Detached => return Ok(ShmMqRecv::Detached),
            }
            rb = usize::min(this_rb, still_needed);
            rawdata = this_raw;
        }

        self.mqh_length_word_complete = false;
        self.mqh_partial_bytes = 0;
        // SAFETY: nbytes initialized bytes of the word-backed buffer.
        Ok(ShmMqRecv::Success(unsafe {
            std::slice::from_raw_parts(self.mqh_buffer.as_ptr().cast::<u8>(), nbytes)
        }))
    }

    pub fn detach(&mut self) {
        if self.mqh_detached {
            return;
        }
        self.mqh_detached = true;
        if self.mqh_send_pending > 0 {
            self.mqh_queue.inc_bytes_written(self.mqh_send_pending);
            self.mqh_send_pending = 0;
        }
        self.mqh_queue.detach_internal(self.mqh_self);
    }

    fn send_bytes(&mut self, data: &[u8], nowait: bool) -> PgResult<(ShmMqResult, usize)> {
        let nbytes = data.len();
        let mut sent: usize = 0;
        let ringsize = self.mqh_queue.mq_ring_size;
        // Stall self-report clock for the full-ring wait; ring progress
        // resets it. Untouched unless this call blocks.
        let mut stall = stall::StallDetector::new();

        while sent < nbytes {
            let rb = self.mqh_queue.bytes_read();
            let wb = self.mqh_queue.bytes_written() + self.mqh_send_pending as u64;
            debug_assert!(wb >= rb);
            let used = (wb - rb) as usize;
            debug_assert!(used <= ringsize);
            let available = usize::min(ringsize - used, nbytes - sent);

            // mq_detached must be re-read every iteration.
            compiler_fence(Ordering::SeqCst);
            if self.mqh_queue.detached() {
                return Ok((ShmMqResult::Detached, sent));
            }

            if available == 0 && !self.mqh_counterparty_attached {
                if nowait {
                    if self.counterparty_gone() {
                        return Ok((ShmMqResult::Detached, sent));
                    }
                    if self.mqh_queue.get_receiver().is_none() {
                        return Ok((ShmMqResult::WouldBlock, sent));
                    }
                } else if !wait_internal(
                    &self.mqh_queue,
                    WaitTarget::Receiver,
                    self.my_latch,
                    self.mqh_handle,
                )? {
                    self.mqh_queue.set_detached();
                    return Ok((ShmMqResult::Detached, sent));
                }
                self.mqh_counterparty_attached = true;
            } else if available == 0 {
                self.mqh_queue.inc_bytes_written(self.mqh_send_pending);
                self.mqh_send_pending = 0;
                debug_assert!(self.mqh_counterparty_attached);
                let receiver = self
                    .mqh_queue
                    .receiver()
                    .expect("mq_receiver set once counterparty attached");
                SetLatch(LatchHandle::proc(receiver));

                if nowait {
                    return Ok((ShmMqResult::WouldBlock, sent));
                }
                let mq = &self.mqh_queue;
                let pending = self.mqh_send_pending;
                stall::wait_on_my_latch_reporting(
                    self.my_latch,
                    WAIT_EVENT_MQ_SEND,
                    &mut stall,
                    &mut |waited_ms| stall::report_queue_stall(mq, "send", pending, waited_ms),
                )?;
            } else {
                let offset = (wb % ringsize as u64) as usize;
                let sendnow = usize::min(available, ringsize - offset);

                // Ring writes after the mq_bytes_read load; pairs with inc_bytes_read.
                fence(Ordering::SeqCst);
                // SAFETY: [offset, offset+sendnow) is unread ring space owned
                // by the sender (used <= ringsize accounting above).
                unsafe {
                    ptr::copy_nonoverlapping(
                        data.as_ptr().add(sent),
                        self.mqh_queue.ring_ptr(offset),
                        sendnow,
                    );
                }
                sent += sendnow;
                debug_assert!(sent == nbytes || sendnow == maxalign(sendnow));
                self.mqh_send_pending += maxalign(sendnow);
                stall.reset();
            }
        }

        Ok((ShmMqResult::Success, sent))
    }

    fn receive_bytes(
        &mut self,
        bytes_needed: usize,
        nowait: bool,
    ) -> PgResult<(ShmMqResult, usize, *const u8)> {
        let ringsize = self.mqh_queue.mq_ring_size;
        // Stall self-report clock for this blocked read; any return is
        // progress (stall.rs). Untouched on the nowait/data-ready paths.
        let mut stall = stall::StallDetector::new();

        loop {
            let written = self.mqh_queue.bytes_written();
            let read = self.mqh_queue.bytes_read() + self.mqh_consume_pending as u64;
            let used = (written - read) as usize;
            debug_assert!(used <= ringsize);
            let offset = (read % ringsize as u64) as usize;

            if used >= bytes_needed || offset + used >= ringsize {
                let nbytesp = usize::min(used, ringsize - offset);
                let datap = self.mqh_queue.ring_ptr(offset) as *const u8;
                // Before the caller touches the data; pairs with inc_bytes_written.
                fence(Ordering::Acquire);
                return Ok((ShmMqResult::Success, nbytesp, datap));
            }

            if self.mqh_queue.detached() {
                // mq_bytes_written may have advanced before mq_detached was
                // set; re-read before giving up.
                fence(Ordering::Acquire);
                if written != self.mqh_queue.bytes_written() {
                    continue;
                }
                return Ok((ShmMqResult::Detached, 0, ptr::null()));
            }

            if self.mqh_consume_pending > 0 {
                self.mqh_queue.inc_bytes_read(self.mqh_consume_pending);
                self.mqh_consume_pending = 0;
            }

            if nowait {
                return Ok((ShmMqResult::WouldBlock, 0, ptr::null()));
            }
            let mq = &self.mqh_queue;
            let pending = self.mqh_consume_pending;
            stall::wait_on_my_latch_reporting(
                self.my_latch,
                WAIT_EVENT_MQ_RECEIVE,
                &mut stall,
                &mut |waited_ms| stall::report_queue_stall(mq, "receive", pending, waited_ms),
            )?;
        }
    }
}

impl Drop for ShmMqHandle {
    fn drop(&mut self) {
        // Worker error unwinds drop handles mid-teardown; a panic out of
        // detach here would double-panic and abort the whole postmaster
        // (C's on_dsm_detach never aborts the cluster for one worker).
        if std::thread::panicking() {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.detach()));
        } else {
            self.detach();
        }
    }
}

// Pub for layered transports that block on the same latch discipline
// (tqueue's chunk-slot wait uses the MQ_SEND wait event).
pub fn wait_on_my_latch(my_latch: Option<LatchHandle>, wait_event_info: u32) -> PgResult<()> {
    let latch = my_latch.expect("shm_mq blocking operation requires MyLatch");
    WaitLatch(Some(latch), WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0, wait_event_info)?;
    ResetLatch(latch);
    postgres_seams::check_for_interrupts::call()?;
    Ok(())
}

// shm_mq_wait_internal: false = detached or the handle's worker died before
// attaching.
fn wait_internal(
    mq: &ShmMq,
    target: WaitTarget,
    my_latch: Option<LatchHandle>,
    handle: Option<(i32, u64)>,
) -> PgResult<bool> {
    // Stall self-report + recheck cadence for the counterparty-attach wait
    // (the worker-death recheck above also depends on wakes arriving).
    let mut stall = stall::StallDetector::new();
    loop {
        mq.spin_acquire();
        let attached = match target {
            WaitTarget::Sender => mq.sender().is_some(),
            WaitTarget::Receiver => mq.receiver().is_some(),
        };
        mq.mq_mutex.unlock();

        if attached {
            return Ok(true);
        }
        if mq.detached() {
            return Ok(false);
        }
        if let Some((slot, generation)) = handle {
            if bgworker_seams::background_worker_stopped::call(slot, generation) {
                return Ok(false);
            }
        }

        stall::wait_on_my_latch_reporting(
            my_latch,
            WAIT_EVENT_MQ_INTERNAL,
            &mut stall,
            &mut |waited_ms| stall::report_queue_stall(mq, "attach", 0, waited_ms),
        )?;
    }
}

// SAFETY: caller guarantees ptr points at LEN_WORD readable bytes.
unsafe fn read_usize(ptr: *const u8) -> usize {
    let mut bytes = [0u8; LEN_WORD];
    ptr::copy_nonoverlapping(ptr, bytes.as_mut_ptr(), LEN_WORD);
    usize::from_ne_bytes(bytes)
}

// SAFETY: caller guarantees ptr points at len bytes valid for 'a.
unsafe fn slice_from_raw<'a>(ptr: *const u8, len: usize) -> &'a [u8] {
    if len == 0 {
        return &[];
    }
    std::slice::from_raw_parts(ptr, len)
}

pub mod stall;

#[cfg(test)]
mod tests;
