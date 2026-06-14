//! Seam declarations for `shm_mq` tuple-queue / error-queue primitives
//! (`storage/ipc/shm_mq.c`) used by the parallel executor and the parallel
//! orchestration (`access/transam/parallel.c`).
//!
//! These follow OPTION (i): the backend-private `shm_mq_handle` is an owned
//! `PgBox<ShmMqHandle>` held in the owner's process-global registry; across the
//! seam it is named by [`ShmMqAttachHandle`] (a small registry id), and the
//! in-segment `shm_mq` is named by [`ShmMqHandle`] (its real base address as a
//! `usize`). The owner (`backend-storage-ipc-shm-mq`) installs these in its
//! `init_seams()`; until then a call panics loudly.
//!
//! Fallibility mirrors the C failure surface: `shm_mq_attach` (`palloc` of the
//! handle + `on_dsm_detach` callback registration) and `shm_mq_receive`
//! (oversized-message `ereport`, interrupt processing) can raise, so they
//! return `PgResult`; the set/get/detach helpers cannot, so they are infallible.

#![allow(unused_doc_comments)]
extern crate alloc;

use types_error::PgResult;
use types_execparallel::{
    BackgroundWorkerHandle, DsmSegmentHandle, SerializeCursor, ShmMqAttachHandle, ShmMqHandle, Size,
};
use types_parallel::ShmMqResult;

/// `shm_mq_create(address + i * size, size)` for the `i`th queue carved out of
/// the queue DSM chunk that starts at `chunk` (the real chunk address). Returns
/// the in-segment queue named by its real base address.
seam_core::seam!(pub fn shm_mq_create_at(chunk: SerializeCursor, index: i32, size: Size) -> ShmMqHandle);
/// `(shm_mq *) (address + i * size)` — name the `i`th *already-created* queue in
/// the queue DSM chunk without re-initializing its header. This is the worker
/// side's plain cast (`parallel_worker_main`: `mq = (shm_mq *) (error_queue_space
/// + ParallelWorkerNumber * SIZE)`); only the leader runs `shm_mq_create`.
seam_core::seam!(pub fn shm_mq_at(chunk: SerializeCursor, index: i32, size: Size) -> ShmMqHandle);
/// `shm_mq_set_receiver(mq, MyProc)`.
seam_core::seam!(pub fn shm_mq_set_receiver_to_myproc(mq: ShmMqHandle));
/// `shm_mq_set_sender(mq, MyProc)`.
seam_core::seam!(pub fn shm_mq_set_sender_to_myproc(mq: ShmMqHandle));
/// `shm_mq_get_sender(mq)` — the sender's `ProcNumber`, or `None` for the C
/// NULL `PGPROC *` (the leader only tests this for NULL).
seam_core::seam!(pub fn shm_mq_get_sender(mq: ShmMqHandle) -> Option<types_core::ProcNumber>);
/// `shm_mq_attach(mq, seg, NULL)` — `palloc`s the backend-private handle and
/// (when `seg` is set) registers the `on_dsm_detach` flag-only callback;
/// either can `ereport`. The handle is parked in the owner's registry and named
/// by the returned id.
seam_core::seam!(pub fn shm_mq_attach(mq: ShmMqHandle, seg: Option<DsmSegmentHandle>) -> PgResult<ShmMqAttachHandle>);
/// `shm_mq_set_handle(mqh, handle)`.
seam_core::seam!(pub fn shm_mq_set_handle(mqh: ShmMqAttachHandle, handle: BackgroundWorkerHandle));
/// `shm_mq_get_queue(mqh)` — the in-segment queue the handle wraps.
seam_core::seam!(pub fn shm_mq_get_queue(mqh: ShmMqAttachHandle) -> ShmMqHandle);
/// `shm_mq_receive(mqh, &nbytes, &data, true)` (nowait) — the message bytes
/// (valid only when `result == Some(Success)`) and its result code. Can
/// `ereport` (oversized message / interrupts).
seam_core::seam!(pub fn shm_mq_receive(mqh: ShmMqAttachHandle) -> PgResult<(Option<ShmMqResult>, alloc::vec::Vec<u8>)>);
/// `shm_mq_detach(mqh)` — destroys the handle (consumes the owned box, freeing
/// its reassembly buffer; cancels the `on_dsm_detach` callback) and drops the
/// registry slot.
seam_core::seam!(pub fn shm_mq_detach(mqh: ShmMqAttachHandle));
/// `shm_mq_send(mqh, nbytes, data, nowait, force_flush)` — write one message of
/// `data.len()` bytes to the queue. Returns the result code. Can `ereport`
/// (oversized message / interrupts). Used by `tqueue.c`'s `tqueueReceiveSlot`
/// (always `nowait = false, force_flush = false`).
seam_core::seam!(pub fn shm_mq_send(mqh: ShmMqAttachHandle, data: alloc::vec::Vec<u8>, nowait: bool, force_flush: bool) -> PgResult<ShmMqResult>);
/// `shm_mq_receive(mqh, &nbytes, &data, nowait)` with caller-chosen `nowait` —
/// the message bytes (valid only when `result == Some(Success)`) and its result
/// code. Can `ereport` (oversized message / interrupts). Used by `tqueue.c`'s
/// `TupleQueueReaderNext`, which threads its own `nowait`.
seam_core::seam!(pub fn shm_mq_receive_nowait(mqh: ShmMqAttachHandle, nowait: bool) -> PgResult<(Option<ShmMqResult>, alloc::vec::Vec<u8>)>);
