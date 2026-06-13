//! Seam declarations for `shm_mq` tuple-queue primitives
//! (`storage/ipc/shm_mq.c`) used by the parallel executor.
//!
//! Installed by the owning unit's `init_seams()` when it lands; until then a
//! call panics loudly.

#![allow(unused_doc_comments)]
use types_execparallel::{
    BackgroundWorkerHandle, DsmSegmentHandle, SerializeCursor, ShmMqAttachHandle, ShmMqHandle, Size,
};

/// `shm_mq_create(address + i * size, size)` for the `i`th queue carved out of
/// the tuple-queue DSM chunk.
seam_core::seam!(pub fn shm_mq_create_at(chunk: SerializeCursor, index: i32, size: Size) -> ShmMqHandle);
/// `shm_mq_set_receiver(mq, MyProc)`.
seam_core::seam!(pub fn shm_mq_set_receiver_to_myproc(mq: ShmMqHandle));
/// `shm_mq_set_sender(mq, MyProc)`.
seam_core::seam!(pub fn shm_mq_set_sender_to_myproc(mq: ShmMqHandle));
/// `shm_mq_attach(mq, seg, NULL)`.
seam_core::seam!(pub fn shm_mq_attach(mq: ShmMqHandle, seg: Option<DsmSegmentHandle>) -> ShmMqAttachHandle);
/// `shm_mq_set_handle(mqh, handle)`.
seam_core::seam!(pub fn shm_mq_set_handle(mqh: ShmMqAttachHandle, handle: BackgroundWorkerHandle));
/// `shm_mq_detach(mqh)`.
seam_core::seam!(pub fn shm_mq_detach(mqh: ShmMqAttachHandle));
