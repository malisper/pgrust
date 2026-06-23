//! Seam declarations for the tuple-queue reader / DestReceiver
//! (`executor/tqueue.c`) used by the parallel executor.
//!
//! Installed by the owning unit's `init_seams()` when it lands; until then a
//! call panics loudly.

#![allow(unused_doc_comments)]
extern crate alloc;

use ::types_error::PgResult;
use execparallel::{DestReceiverHandle, ShmMqAttachHandle, TupleQueueReaderHandle};

/// `CreateTupleQueueReader(handle)`.
seam_core::seam!(pub fn create_tuple_queue_reader(handle: ShmMqAttachHandle) -> TupleQueueReaderHandle);
/// `DestroyTupleQueueReader(reader)`.
seam_core::seam!(pub fn destroy_tuple_queue_reader(reader: TupleQueueReaderHandle));
/// `CreateTupleQueueDestReceiver(handle)`.
seam_core::seam!(pub fn create_tuple_queue_dest_receiver(handle: ShmMqAttachHandle) -> DestReceiverHandle);
/// `receiver->rDestroy(receiver)`.
seam_core::seam!(pub fn receiver_destroy(receiver: DestReceiverHandle));
/// `TupleQueueReaderNext(reader, nowait, &done)` (`tqueue.c`) — fetch the next
/// tuple from the reader's queue. Returns the next minimal tuple as its on-wire
/// byte image (`None` when no tuple is available — queue detached or
/// `WouldBlock`) and the C `*done` out-parameter (`true` once the queue is
/// detached, otherwise `false`). The bytes are the canonical minimal-tuple wire
/// image the consumer re-stores into a slot. Fallible: `shm_mq_receive` can
/// `ereport`.
seam_core::seam!(pub fn tuple_queue_reader_next(reader: TupleQueueReaderHandle, nowait: bool) -> PgResult<(Option<alloc::vec::Vec<u8>>, bool)>);
