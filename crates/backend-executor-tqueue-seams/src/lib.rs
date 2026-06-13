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
