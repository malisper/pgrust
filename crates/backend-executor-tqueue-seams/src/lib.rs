//! Seam declarations for the tuple-queue reader / DestReceiver
//! (`executor/tqueue.c`) used by the parallel executor.
//!
//! Installed by the owning unit's `init_seams()` when it lands; until then a
//! call panics loudly.

#![allow(unused_doc_comments)]
use types_execparallel::{DestReceiverHandle, ShmMqAttachHandle, TupleQueueReaderHandle};

/// `CreateTupleQueueReader(handle)`.
seam_core::seam!(pub fn create_tuple_queue_reader(handle: ShmMqAttachHandle) -> TupleQueueReaderHandle);
/// `DestroyTupleQueueReader(reader)`.
seam_core::seam!(pub fn destroy_tuple_queue_reader(reader: TupleQueueReaderHandle));
/// `CreateTupleQueueDestReceiver(handle)`.
seam_core::seam!(pub fn create_tuple_queue_dest_receiver(handle: ShmMqAttachHandle) -> DestReceiverHandle);
/// `receiver->rDestroy(receiver)`.
seam_core::seam!(pub fn receiver_destroy(receiver: DestReceiverHandle));

/// `TupleQueueReaderNext(reader, nowait, &done)` (tqueue.c) — read the next
/// tuple from the worker's shared-memory queue. Returns `Ok(None)` when the
/// queue would block (`SHM_MQ_WOULD_BLOCK`) or is detached (`SHM_MQ_DETACHED`);
/// sets `done = true` only on detach. The C returns a pointer directly into
/// queue memory; since the caller buffers tuples across calls (and must make a
/// copy anyway), the owned model returns a copy of the `MinimalTuple` into
/// `mcx` so the result outlives the queue read. Fallible on OOM.
seam_core::seam!(pub fn tuple_queue_reader_next<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    reader: TupleQueueReaderHandle,
    nowait: bool,
    done: &mut bool,
) -> types_error::PgResult<types_tuple::heaptuple::MinimalTuple<'mcx>>);
