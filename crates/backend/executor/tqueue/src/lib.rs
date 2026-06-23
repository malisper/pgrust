#![allow(non_snake_case)]
//! Port of `executor/tqueue.c` — use `shm_mq` to send & receive tuples between
//! parallel backends.
//!
//! A `DestReceiver` of type `DestTupleQueue`, which is a [`TQueueDestReceiver`]
//! under the hood, writes tuples from the executor to a `shm_mq`. A
//! [`TupleQueueReader`] reads tuples from a `shm_mq` and returns them.
//!
//! # Model
//!
//! In C the receiver and reader are `palloc`'d objects named by raw pointers;
//! the receiver's four callbacks downcast `DestReceiver *self` back to the
//! private `TQueueDestReceiver` to reach the `shm_mq_handle *queue`. The repo's
//! parallel substrate names a `shm_mq_handle *` by an
//! [`ShmMqAttachHandle`] (a registry id minted by `shm_mq_attach`) and a
//! `DestReceiver *`/`TupleQueueReader *` by a
//! [`DestReceiverHandle`]/[`TupleQueueReaderHandle`]. Following the established
//! OPTION (i) precedent (`backend-storage-ipc-shm-mq`), the owned receiver /
//! reader objects are parked in this crate's process-global registries and the
//! inward seams (`create_tuple_queue_reader`, `destroy_tuple_queue_reader`,
//! `create_tuple_queue_dest_receiver`, `receiver_destroy`,
//! `tuple_queue_reader_next`) name them by id.
//!
//! The queue transport (`shm_mq_send`/`shm_mq_receive`/`shm_mq_detach`) is
//! reached through `backend-storage-ipc-shm-mq-seams`; the slot→minimal-tuple
//! fetch (`ExecFetchSlotMinimalTuple`) through `backend-executor-execTuples-seams`.

extern crate alloc;

use alloc::vec::Vec;

use ::utils_error::ereport;
use ::types_dest::CommandDest;
use types_error::{ErrorLocation, PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR};
use execparallel::{DestReceiverHandle, ShmMqAttachHandle, TupleQueueReaderHandle};
use ::types_parallel::ShmMqResult;

use shm_mq_seams as shmmq;

fn loc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/executor/tqueue.c", 0, funcname)
}

/// `struct TQueueDestReceiver` (`tqueue.c`) — a `DestReceiver` that writes
/// tuples to a `shm_mq`.
///
/// ```c
/// typedef struct TQueueDestReceiver
/// {
///     DestReceiver pub;           /* public fields */
///     shm_mq_handle *queue;       /* shm_mq to send to */
/// } TQueueDestReceiver;
/// ```
pub struct TQueueDestReceiver {
    /// `DestReceiver pub` — `pub.mydest` is `DestTupleQueue`; the four callbacks
    /// are the module's `tqueue*` functions, dispatched by the dest layer.
    pub mydest: CommandDest,
    /// `shm_mq_handle *queue` — the queue to send to (`None` after shutdown,
    /// matching the C `tqueue->queue = NULL`).
    pub queue: Option<ShmMqAttachHandle>,
}

/// `struct TupleQueueReader` (`tqueue.c`/`tqueue.h`) — reads tuples from a
/// `shm_mq`.
///
/// ```c
/// struct TupleQueueReader
/// {
///     shm_mq_handle *queue;       /* shm_mq to receive from */
/// };
/// ```
pub struct TupleQueueReader {
    /// `shm_mq_handle *queue` — the queue to receive from.
    pub queue: ShmMqAttachHandle,
}

// ===========================================================================
// Process-global registries for the owned receiver / reader objects, named
// across the seam by id (OPTION (i), mirroring `backend-storage-ipc-shm-mq`).
// ===========================================================================
mod registry {
    use core::cell::RefCell;

    use super::{TQueueDestReceiver, TupleQueueReader};
    use ::execparallel::TupleQueueReaderHandle;

    struct Receivers {
        slots: alloc::vec::Vec<Option<TQueueDestReceiver>>,
    }

    impl Receivers {
        const fn new() -> Self {
            Self {
                slots: alloc::vec::Vec::new(),
            }
        }
        /// Park the receiver and return its registry token (a 1-based `u64`).
        /// This token is the `state` value the [`TQueueDestReceiver`] is
        /// registered into the tcop-dest router with — the owned-model stand-in
        /// for C's `(TQueueDestReceiver *) self` downcast.
        fn insert(&mut self, r: TQueueDestReceiver) -> u64 {
            if let Some(i) = self.slots.iter().position(Option::is_none) {
                self.slots[i] = Some(r);
                (i + 1) as u64
            } else {
                self.slots.push(Some(r));
                self.slots.len() as u64
            }
        }
        fn idx(token: u64) -> usize {
            debug_assert!(token >= 1, "tqueue receiver token 0 is the NULL sentinel");
            (token - 1) as usize
        }
        fn get_mut(&mut self, token: u64) -> &mut TQueueDestReceiver {
            self.slots[Self::idx(token)]
                .as_mut()
                .expect("live tqueue DestReceiver token")
        }
        fn take(&mut self, token: u64) -> TQueueDestReceiver {
            self.slots[Self::idx(token)]
                .take()
                .expect("live tqueue DestReceiver token")
        }
    }

    struct Readers {
        slots: alloc::vec::Vec<Option<TupleQueueReader>>,
    }

    impl Readers {
        const fn new() -> Self {
            Self {
                slots: alloc::vec::Vec::new(),
            }
        }
        fn insert(&mut self, r: TupleQueueReader) -> TupleQueueReaderHandle {
            if let Some(i) = self.slots.iter().position(Option::is_none) {
                self.slots[i] = Some(r);
                TupleQueueReaderHandle(i + 1)
            } else {
                self.slots.push(Some(r));
                TupleQueueReaderHandle(self.slots.len())
            }
        }
        fn idx(h: TupleQueueReaderHandle) -> usize {
            debug_assert!(h.0 >= 1, "TupleQueueReaderHandle 0 is the NULL sentinel");
            h.0 - 1
        }
        fn get_mut(&mut self, h: TupleQueueReaderHandle) -> &mut TupleQueueReader {
            self.slots[Self::idx(h)]
                .as_mut()
                .expect("live TupleQueueReader id")
        }
        fn take(&mut self, h: TupleQueueReaderHandle) -> TupleQueueReader {
            self.slots[Self::idx(h)]
                .take()
                .expect("live TupleQueueReader id")
        }
    }

    thread_local! {
        static RECEIVERS: RefCell<Receivers> = const { RefCell::new(Receivers::new()) };
        static READERS: RefCell<Readers> = const { RefCell::new(Readers::new()) };
    }

    pub(super) fn insert_receiver(r: TQueueDestReceiver) -> u64 {
        RECEIVERS.with(|c| c.borrow_mut().insert(r))
    }
    pub(super) fn with_receiver_mut<R>(
        token: u64,
        f: impl FnOnce(&mut TQueueDestReceiver) -> R,
    ) -> R {
        RECEIVERS.with(|c| f(c.borrow_mut().get_mut(token)))
    }
    pub(super) fn take_receiver(token: u64) -> TQueueDestReceiver {
        RECEIVERS.with(|c| c.borrow_mut().take(token))
    }

    pub(super) fn insert_reader(r: TupleQueueReader) -> TupleQueueReaderHandle {
        READERS.with(|c| c.borrow_mut().insert(r))
    }
    pub(super) fn with_reader_mut<R>(
        h: TupleQueueReaderHandle,
        f: impl FnOnce(&mut TupleQueueReader) -> R,
    ) -> R {
        READERS.with(|c| f(c.borrow_mut().get_mut(h)))
    }
    pub(super) fn take_reader(h: TupleQueueReaderHandle) -> TupleQueueReader {
        READERS.with(|c| c.borrow_mut().take(h))
    }
}

/// `static bool tqueueReceiveSlot(TupleTableSlot *slot, DestReceiver *self)`
/// (`tqueue.c`) — receive a tuple from the query and send it to the designated
/// `shm_mq`. Returns `true` if successful, `false` if the `shm_mq` has been
/// detached.
///
/// Dispatched through the tcop-dest router's `receiveSlot(mcx, state, slot)`
/// vtable boundary, which carries the live payload-bearing `&mut SlotData`
/// directly (no `EState`); the `MinimalTuple` materialization therefore uses the
/// standalone slot-only fetch form (`exec_fetch_slot_minimal_tuple_copy_standalone`).
pub fn tqueueReceiveSlot<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    slot: &mut nodes::SlotData<'mcx>,
    self_: &mut TQueueDestReceiver,
) -> PgResult<bool> {
    // Send the tuple itself.
    //   tuple = ExecFetchSlotMinimalTuple(slot, &should_free);
    //   result = shm_mq_send(tqueue->queue, tuple->t_len, tuple, false, false);
    //   if (should_free)
    //       pfree(tuple);
    // The owned fetch returns a copy of the slot's contents as the minimal
    // tuple's contiguous C byte image (the flat blob, `tuple->t_len` bytes —
    // exactly what C hands `shm_mq_send`); the C `should_free` / `pfree(tuple)`
    // bookkeeping is internal to the execTuples owner.
    let data =
        execTuples_seams::exec_fetch_slot_minimal_tuple_copy_standalone::call(
            mcx, slot,
        )?;

    let queue = self_.queue.expect("tqueueReceiveSlot: queue is NULL");
    // shm_mq_send takes an owned global-heap Vec; copy the flat blob out of mcx.
    let data: alloc::vec::Vec<u8> = data.iter().copied().collect();
    let result = shmmq::shm_mq_send::call(queue, data, false, false)?;

    // Check for failure.
    //   if (result == SHM_MQ_DETACHED)
    //       return false;
    //   else if (result != SHM_MQ_SUCCESS)
    //       ereport(ERROR, ...);
    //   return true;
    if result == ShmMqResult::Detached {
        return Ok(false);
    } else if result != ShmMqResult::Success {
        ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("could not send tuple to shared-memory queue")
            .finish(loc("tqueueReceiveSlot"))?;
    }
    Ok(true)
}

/// `static void tqueueStartupReceiver(DestReceiver *self, int operation,
/// TupleDesc typeinfo)` (`tqueue.c`) — prepare to receive tuples. Does nothing.
pub fn tqueueStartupReceiver(_self_: &mut TQueueDestReceiver, _operation: i32) {
    /* do nothing */
}

/// `static void tqueueShutdownReceiver(DestReceiver *self)` (`tqueue.c`) — clean
/// up at the end of an executor run: detach from the queue if still attached,
/// then clear `queue` (`tqueue->queue = NULL`).
pub fn tqueueShutdownReceiver(self_: &mut TQueueDestReceiver) {
    if let Some(queue) = self_.queue {
        shmmq::shm_mq_detach::call(queue);
    }
    self_.queue = None;
}

/// `static void tqueueDestroyReceiver(DestReceiver *self)` (`tqueue.c`) — destroy
/// the receiver when done: detach if still attached (probably already done at
/// shutdown, but be sure), then `pfree(self)` (the owned-value drop).
pub fn tqueueDestroyReceiver(mut self_: TQueueDestReceiver) {
    /* We probably already detached from queue, but let's be sure */
    if let Some(queue) = self_.queue {
        shmmq::shm_mq_detach::call(queue);
    }
    self_.queue = None;
    drop(self_);
}

// ===========================================================================
// tcop-dest router vtable for the TupleQueue DestReceiver.
//
// `CreateTupleQueueDestReceiver` registers these three callbacks into the single
// tcop-dest router (mirroring copyto's `CreateCopyDestReceiver`), so the
// executor's `dest->receiveSlot` dispatch (`dest_receive_slot`) reaches
// `tqueueReceiveSlot`. The router threads the per-receiver `state` token — the
// `RECEIVERS` registry token (C's `(TQueueDestReceiver *) self` stand-in) — back
// to each callback; the callbacks recover the live `TQueueDestReceiver` from the
// registry by that token.
// ===========================================================================

/// `tqueueStartupReceiver` as the router `rStartup` slot — does nothing.
fn tqueue_router_startup(
    _mcx: mcx::Mcx<'_>,
    state: u64,
    operation: nodes::nodes::CmdType,
    _typeinfo: &types_tuple::heaptuple::TupleDescData<'_>,
) -> PgResult<()> {
    registry::with_receiver_mut(state, |r| tqueueStartupReceiver(r, operation as i32));
    Ok(())
}

/// `tqueueReceiveSlot` as the router `receiveSlot` slot. Recovers the live
/// `TQueueDestReceiver` from the `RECEIVERS` registry by the `state` token and
/// sends the slot's `MinimalTuple` to the `shm_mq`.
fn tqueue_router_receive_slot<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    state: u64,
    slot: &mut nodes::SlotData<'mcx>,
) -> PgResult<bool> {
    // The receiver's only per-receiver state is its `queue` handle; copy it out
    // so the registry borrow does not span the `shm_mq_send` re-entry (which
    // does not touch this receiver's registry slot).
    let queue = registry::with_receiver_mut(state, |r| r.queue);
    let mut self_ = TQueueDestReceiver {
        mydest: CommandDest::TupleQueue,
        queue,
    };
    tqueueReceiveSlot(mcx, slot, &mut self_)
}

/// `tqueueShutdownReceiver` as the router `rShutdown` slot — detach from the
/// queue if still attached, then clear `queue` (`tqueue->queue = NULL`).
fn tqueue_router_shutdown(_mcx: mcx::Mcx<'_>, state: u64) -> PgResult<()> {
    registry::with_receiver_mut(state, tqueueShutdownReceiver);
    Ok(())
}

/// `DestReceiver *CreateTupleQueueDestReceiver(shm_mq_handle *handle)`
/// (`tqueue.c`) — create a `DestReceiver` that writes tuples to a tuple queue.
///
/// C `palloc0`s the receiver, sets the four callbacks and
/// `pub.mydest = DestTupleQueue`, and `self->queue = handle`. Here the owned
/// receiver is parked in this crate's `RECEIVERS` registry (which holds the
/// per-receiver mutable `queue` state across the run, matching the C
/// `tqueue->queue = NULL` on shutdown), and its three dispatch callbacks are
/// registered into the *single* tcop-dest router via `register_dest_receiver`
/// (mirroring copyto). The router's `state` token is the `RECEIVERS` registry
/// token — C's `(TQueueDestReceiver *) self` stand-in. The returned handle is the
/// router's `DestReceiver *` id, the same one the executor's `dest->receiveSlot`
/// dispatch resolves; it is carried as a `::execparallel::DestReceiverHandle`
/// (the parallel-executor's home for the live `DestReceiver *`).
pub fn CreateTupleQueueDestReceiver(handle: ShmMqAttachHandle) -> DestReceiverHandle {
    let self_ = TQueueDestReceiver {
        mydest: CommandDest::TupleQueue,
        queue: Some(handle),
    };
    let token = registry::insert_receiver(self_);
    let router_handle = tcop_dest::register_dest_receiver(
        CommandDest::TupleQueue,
        tcop_dest::ReceiverVtable {
            rStartup: tqueue_router_startup,
            receiveSlot: tqueue_router_receive_slot,
            rShutdown: tqueue_router_shutdown,
        },
        token,
    );
    // The parallel-executor `DestReceiverHandle` (execparallel) and the
    // executor's `dest.h` `DestReceiverHandle` (nodes::parsestmt) name the
    // same live `DestReceiver *`; carry the router id across the home boundary.
    DestReceiverHandle(router_handle.0 as usize)
}

/// `TupleQueueReader *CreateTupleQueueReader(shm_mq_handle *handle)`
/// (`tqueue.c`) — create a tuple queue reader.
pub fn CreateTupleQueueReader(handle: ShmMqAttachHandle) -> TupleQueueReaderHandle {
    let reader = TupleQueueReader { queue: handle };
    registry::insert_reader(reader)
}

/// `void DestroyTupleQueueReader(TupleQueueReader *reader)` (`tqueue.c`) —
/// destroy a tuple queue reader.
///
/// Note: cleaning up the underlying `shm_mq` is the caller's responsibility; we
/// do not access it here, as it may be detached already. The C `pfree(reader)`
/// is the owned-value drop of the reader (its `queue` handle is left for the
/// caller to clean up — the dropped struct only carries the id, not the queue).
pub fn DestroyTupleQueueReader(reader: TupleQueueReaderHandle) {
    let reader = registry::take_reader(reader);
    drop(reader);
}

/// `MinimalTuple TupleQueueReaderNext(TupleQueueReader *reader, bool nowait,
/// bool *done)` (`tqueue.c`) — fetch a tuple from a tuple queue reader.
///
/// Returns the next tuple's on-wire bytes (`None` if no remaining tuples or
/// `nowait == true` and none ready) and the C `*done` out-parameter (`true`
/// when the queue is detached, otherwise `false`).
pub fn TupleQueueReaderNext(
    reader: TupleQueueReaderHandle,
    nowait: bool,
) -> PgResult<(Option<Vec<u8>>, bool)> {
    // if (done != NULL) *done = false;
    let queue = registry::with_reader_mut(reader, |r| r.queue);

    // Attempt to read a message.
    //   result = shm_mq_receive(reader->queue, &nbytes, &data, nowait);
    let (result, data) = shmmq::shm_mq_receive_nowait::call(queue, nowait)?;

    match result {
        // If queue is detached, set *done and return NULL.
        Some(ShmMqResult::Detached) => Ok((None, true)),
        // In non-blocking mode, bail out if no message ready yet. The seam's
        // `None` (no message available) folds in here, matching the parallel
        // worker's `None => break` treatment of the same receive seam.
        Some(ShmMqResult::WouldBlock) | None => Ok((None, false)),
        // Assert(result == SHM_MQ_SUCCESS);
        //
        // C returns a pointer to the queue memory directly:
        //   tuple = (MinimalTuple) data;
        //   Assert(tuple->t_len == nbytes);
        // The repo's receive seam already copied the message bytes out; hand
        // them to the consumer, which re-stores them into a slot.
        Some(ShmMqResult::Success) => Ok((Some(data), false)),
    }
}

// ===========================================================================
// Inward seam installers (the registry-id surface the parallel executor /
// nodeGather consume across the cycle).
// ===========================================================================

fn create_tuple_queue_reader_seam(handle: ShmMqAttachHandle) -> TupleQueueReaderHandle {
    CreateTupleQueueReader(handle)
}

fn destroy_tuple_queue_reader_seam(reader: TupleQueueReaderHandle) {
    DestroyTupleQueueReader(reader);
}

fn create_tuple_queue_dest_receiver_seam(handle: ShmMqAttachHandle) -> DestReceiverHandle {
    CreateTupleQueueDestReceiver(handle)
}

fn receiver_destroy_seam(receiver: DestReceiverHandle) {
    // `receiver->rDestroy(receiver)`: for a TupleQueue receiver, that is
    // `tqueueDestroyReceiver`. The `receiver` is the tcop-dest router id (the
    // live `DestReceiver *`); recover its `state` token (this crate's `RECEIVERS`
    // registry token, the `(TQueueDestReceiver *) self` stand-in) and take the
    // owned receiver out of the registry.
    let router_handle = nodes::parsestmt::DestReceiverHandle(receiver.0 as u64);
    let token = tcop_dest::dest_receiver_state_token(router_handle);
    let self_ = registry::take_receiver(token);
    tqueueDestroyReceiver(self_);
}

fn tuple_queue_reader_next_seam(
    reader: TupleQueueReaderHandle,
    nowait: bool,
) -> PgResult<(Option<Vec<u8>>, bool)> {
    TupleQueueReaderNext(reader, nowait)
}

/// Install this crate's implementations into `backend-executor-tqueue-seams`.
pub fn init_seams() {
    use tqueue_seams as seams;
    seams::create_tuple_queue_reader::set(create_tuple_queue_reader_seam);
    seams::destroy_tuple_queue_reader::set(destroy_tuple_queue_reader_seam);
    seams::create_tuple_queue_dest_receiver::set(create_tuple_queue_dest_receiver_seam);
    seams::receiver_destroy::set(receiver_destroy_seam);
    seams::tuple_queue_reader_next::set(tuple_queue_reader_next_seam);
}
