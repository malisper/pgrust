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

use backend_utils_error::ereport;
use types_dest::CommandDest;
use types_error::{ErrorLocation, PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR};
use types_execparallel::{DestReceiverHandle, ShmMqAttachHandle, TupleQueueReaderHandle};
use types_parallel::ShmMqResult;

use backend_storage_ipc_shm_mq_seams as shmmq;

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
    use types_execparallel::{DestReceiverHandle, TupleQueueReaderHandle};

    struct Receivers {
        slots: alloc::vec::Vec<Option<TQueueDestReceiver>>,
    }

    impl Receivers {
        const fn new() -> Self {
            Self {
                slots: alloc::vec::Vec::new(),
            }
        }
        fn insert(&mut self, r: TQueueDestReceiver) -> DestReceiverHandle {
            if let Some(i) = self.slots.iter().position(Option::is_none) {
                self.slots[i] = Some(r);
                DestReceiverHandle(i + 1)
            } else {
                self.slots.push(Some(r));
                DestReceiverHandle(self.slots.len())
            }
        }
        fn idx(h: DestReceiverHandle) -> usize {
            debug_assert!(h.0 >= 1, "DestReceiverHandle 0 is the NULL sentinel");
            h.0 - 1
        }
        fn take(&mut self, h: DestReceiverHandle) -> TQueueDestReceiver {
            self.slots[Self::idx(h)]
                .take()
                .expect("live tqueue DestReceiver id")
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

    pub(super) fn insert_receiver(r: TQueueDestReceiver) -> DestReceiverHandle {
        RECEIVERS.with(|c| c.borrow_mut().insert(r))
    }
    pub(super) fn take_receiver(h: DestReceiverHandle) -> TQueueDestReceiver {
        RECEIVERS.with(|c| c.borrow_mut().take(h))
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
pub fn tqueueReceiveSlot<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    slot: &types_nodes::TupleTableSlot,
    self_: &mut TQueueDestReceiver,
) -> PgResult<bool> {
    // Send the tuple itself.
    //   tuple = ExecFetchSlotMinimalTuple(slot, &should_free);
    //   result = shm_mq_send(tqueue->queue, tuple->t_len, tuple, false, false);
    //   if (should_free)
    //       pfree(tuple);
    // The owned fetch always returns a copy into `mcx`; the C `should_free` /
    // `pfree(tuple)` bookkeeping is internal to the execTuples owner. The
    // `tuple->t_len` bytes C hands to `shm_mq_send` are the minimal tuple's
    // on-wire byte image.
    let tuple = backend_executor_execTuples_seams::exec_fetch_slot_minimal_tuple_copy::call(
        mcx, slot,
    )?;
    let data = tuple.to_minimal_bytes();

    let queue = self_.queue.expect("tqueueReceiveSlot: queue is NULL");
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

/// `DestReceiver *CreateTupleQueueDestReceiver(shm_mq_handle *handle)`
/// (`tqueue.c`) — create a `DestReceiver` that writes tuples to a tuple queue.
///
/// C `palloc0`s the receiver, sets the four callbacks and
/// `pub.mydest = DestTupleQueue`, and `self->queue = handle`. Here the owned
/// receiver is parked in the registry and named by the returned id; the
/// callbacks are the module's `tqueue*` functions, dispatched by the dest layer.
pub fn CreateTupleQueueDestReceiver(handle: ShmMqAttachHandle) -> DestReceiverHandle {
    let self_ = TQueueDestReceiver {
        mydest: CommandDest::TupleQueue,
        queue: Some(handle),
    };
    registry::insert_receiver(self_)
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
    // `tqueueDestroyReceiver`.
    let self_ = registry::take_receiver(receiver);
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
    use backend_executor_tqueue_seams as seams;
    seams::create_tuple_queue_reader::set(create_tuple_queue_reader_seam);
    seams::destroy_tuple_queue_reader::set(destroy_tuple_queue_reader_seam);
    seams::create_tuple_queue_dest_receiver::set(create_tuple_queue_dest_receiver_seam);
    seams::receiver_destroy::set(receiver_destroy_seam);
    seams::tuple_queue_reader_next::set(tuple_queue_reader_next_seam);
}
