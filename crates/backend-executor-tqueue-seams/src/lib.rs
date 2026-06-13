//! Seam declarations for the tuple-queue reader / DestReceiver
//! (`executor/tqueue.c`) used by the parallel executor.
//!
//! Installed by the owning unit's `init_seams()` when it lands; until then a
//! call panics loudly.

#![allow(unused_doc_comments)]
use types_execparallel::{DestReceiverHandle, ShmMqAttachHandle, TupleQueueReaderHandle};

/// Result of [`tuple_queue_reader_next`]: the next minimal tuple (or `None`
/// when the queue had none ready / the worker is done) and the `*done`
/// out-flag (`true` once the worker has detached and produced all its tuples).
#[derive(Debug)]
pub struct ReaderNext<'mcx> {
    /// The `MinimalTuple` returned (`None` is the C `NULL`).
    pub tup: types_tuple::heaptuple::MinimalTuple<'mcx>,
    /// `*done` out-parameter.
    pub done: bool,
}

/// `TupleQueueReaderNext(reader, nowait, &done)` (tqueue.c): attempt to read
/// the next tuple from the worker's shm queue. When `nowait` is true it does
/// not block: a `None` tuple with `done == false` means "nothing ready yet".
/// The tuple is read into `mcx` (C: the reader's `MinimalTuple` storage), so
/// the call is fallible on OOM and on a queue/`shm_mq` error.
seam_core::seam!(pub fn tuple_queue_reader_next<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    reader: TupleQueueReaderHandle,
    nowait: bool,
) -> types_error::PgResult<ReaderNext<'mcx>>);

/// `CreateTupleQueueReader(handle)`.
seam_core::seam!(pub fn create_tuple_queue_reader(handle: ShmMqAttachHandle) -> TupleQueueReaderHandle);
/// `DestroyTupleQueueReader(reader)`.
seam_core::seam!(pub fn destroy_tuple_queue_reader(reader: TupleQueueReaderHandle));
/// `CreateTupleQueueDestReceiver(handle)`.
seam_core::seam!(pub fn create_tuple_queue_dest_receiver(handle: ShmMqAttachHandle) -> DestReceiverHandle);
/// `receiver->rDestroy(receiver)`.
seam_core::seam!(pub fn receiver_destroy(receiver: DestReceiverHandle));
