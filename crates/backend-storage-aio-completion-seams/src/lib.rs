//! Seam declarations for the AIO completion-callback leaves and the resowner
//! AIO integration — the parts of the AIO subsystem that bottom out in
//! genuinely-unported owners (the buffer manager / md.c shared+local buffer
//! completion callbacks, the smgr synchronous read/write executor, and the
//! resource-owner AIO-handle registry).
//!
//! `storage/aio/aio_callback.c` dispatches each registered
//! `PgAioHandleCallbacks` entry (`aio_md_readv_cb`,
//! `aio_shared_buffer_readv_cb`, `aio_local_buffer_readv_cb`) through a function
//! pointer table whose bodies live in `md.c` / `bufmgr.c` / `localbuf.c` — not
//! yet ported for AIO. `storage/aio/aio_io.c`'s
//! `pgaio_io_perform_synchronously` issues the actual `preadv`/`pwritev` through
//! `FileReadV`/`FileWriteV` against the smgr layer's AIO path. And
//! `ResourceOwnerRememberAioHandle` / `ResourceOwnerForgetAioHandle`
//! (`utils/resowner/resowner.c`) own the per-owner AIO-handle dlist.
//!
//! The owning units install these from their `init_seams()` when their AIO
//! support lands; until then a call panics loudly. The AIO engine itself
//! (`aio.c`/`aio_callback.c` registration + dispatch loop) is real and reaches
//! these only on the async / buffered-IO completion path (never on the
//! `io_method = sync` boot path).

extern crate alloc;

use alloc::string::String;

use types_error::PgResult;
use types_resowner::ResourceOwner;

seam_core::seam!(
    /// `pgaio_io_call_stage(ioh)` leg for one registered callback id
    /// (`aio_handle_cbs[cb_id].cb->stage(ioh, ...)`, aio_callback.c). `cb_id` is
    /// the `PgAioHandleCallbackID`; `ioh_index` indexes `pgaio_ctl->io_handles`.
    /// The callback mutates the handle's target/op data in place, as in C.
    pub fn pgaio_cb_stage(cb_id: u8, ioh_index: u32) -> PgResult<()>
);

seam_core::seam!(
    /// `aio_handle_cbs[cb_id].cb->complete_shared(ioh, prior_result, ...)`
    /// (aio_callback.c) — the shared-stage completion callback for one
    /// registered id. Reads the handle's raw result + target data and writes the
    /// distilled [shared] result back into the handle's `distilled_result`,
    /// threading the prior callback's distilled status, exactly as C threads the
    /// returned `PgAioResult` between callbacks.
    pub fn pgaio_cb_complete_shared(cb_id: u8, ioh_index: u32) -> PgResult<()>
);

seam_core::seam!(
    /// `aio_handle_cbs[cb_id].cb->complete_local(ioh, prior_result, ...)`
    /// (aio_callback.c) — the local-stage completion callback for one registered
    /// id; updates the handle's `distilled_result` for the issuing backend.
    pub fn pgaio_cb_complete_local(cb_id: u8, ioh_index: u32) -> PgResult<()>
);

seam_core::seam!(
    /// `aio_handle_cbs[cb_id].cb->report(result, target_data, elevel)`
    /// (aio_callback.c, reached from `pgaio_result_report`) — emit the
    /// callback-specific ereport for a completed IO's distilled result.
    pub fn pgaio_cb_report(cb_id: u8, ioh_index: u32, elevel: i32) -> PgResult<()>
);

seam_core::seam!(
    /// The raw `pg_preadv`/`pg_pwritev` syscall leg of
    /// `pgaio_io_perform_synchronously(ioh)` (aio_io.c) — execute the handle's
    /// read/write op against `op_data.fd`/`offset`/`iov_length` using the
    /// handle's iovec array, returning the raw `ssize_t` result (`-errno` on
    /// failure). The aio_io.c caller wraps this in a critical section and drives
    /// `pgaio_io_process_completion`. The syscall bottoms out in the unported
    /// fd / smgr AIO read/write layer.
    pub fn pgaio_perform_io_syscall(ioh_index: u32) -> PgResult<i64>
);

seam_core::seam!(
    /// `pgaio_io_reopen(ioh)` (aio_target.c) — re-open the target's file
    /// descriptor in an IO worker (the target's `reopen` vtable entry, smgr).
    pub fn pgaio_io_reopen(ioh_index: u32) -> PgResult<()>
);

seam_core::seam!(
    /// `pgaio_target_info[ioh->target]->describe_identity(&ioh->target_data)`
    /// (aio_target.c, reached from `pgaio_io_get_target_description`) — the smgr
    /// target's `aio_smgr_describe_identity`, returning a localized, current-
    /// memory-context-allocated string describing the IO's target. Bottoms out
    /// in the unported smgr AIO layer.
    pub fn pgaio_io_describe_identity(ioh_index: u32) -> PgResult<String>
);

seam_core::seam!(
    /// `ResourceOwnerRememberAioHandle(owner, &ioh->resowner_node)`
    /// (resowner.c) — register an AIO handle node on a resource owner. The node
    /// identity is the io-handle index; `owner` is the resowner handle.
    pub fn resource_owner_remember_aio_handle(owner: ResourceOwner, ioh_index: u32) -> PgResult<()>
);

seam_core::seam!(
    /// `ResourceOwnerForgetAioHandle(owner, &ioh->resowner_node)` (resowner.c) —
    /// unregister an AIO handle node from a resource owner.
    pub fn resource_owner_forget_aio_handle(owner: ResourceOwner, ioh_index: u32) -> PgResult<()>
);
