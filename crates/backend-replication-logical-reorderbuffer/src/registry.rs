//! Handle layer: the backend-local registry mapping each live
//! [`ReorderBufferHandle`] to its owned [`ReorderBuffer`], plus the
//! [`TxnHandle`] ↔ xid encoding and the inward seam installers.
//!
//! logical.c / snapbuild.c / heapam_visibility hold the reorder buffer only as
//! the opaque `ReorderBufferHandle` (a `usize`) they forward through the inward
//! seams; this crate is the owner and resolves it to the real value. The
//! registry is `thread_local!` because the C `ReorderBuffer *` is per-backend
//! (tied to `MyReplicationSlot`).

extern crate alloc;

use core::cell::RefCell;

use std::collections::HashMap;

use types_core::primitive::{ForkNumber, TransactionId, XLogRecPtr};
use types_core::xact::{CommandId, InvalidCommandId};
use types_error::PgResult;
use types_logical::{ReorderBufferHandle, ReorderBufferStats, TxnHandle};
use types_snapshot::snapshot::ResolveCminCmaxResult;
use types_snapshot::SnapshotData;
use types_storage::sinval::SharedInvalidationMessage;
use types_storage::storage::Buffer;
use types_storage::RelFileLocator;
use types_tuple::heaptuple::HeapTupleData;
use types_tuple::ItemPointerData;

use crate::snapshot::{ReorderBufferTupleCidEnt, ReorderBufferTupleCidKey};
use crate::ReorderBuffer;

::std::thread_local! {
    /// Backend-local table of live reorder buffers. A handle is `1 + slot
    /// index` (`0` is the C `NULL`, never handed out); freed slots become
    /// `None` and are reused.
    static BUFFERS: RefCell<alloc::vec::Vec<Option<ReorderBuffer>>> =
        const { RefCell::new(alloc::vec::Vec::new()) };
}

fn handle_to_index(h: ReorderBufferHandle) -> usize {
    debug_assert!(h.0 != 0, "NULL ReorderBufferHandle");
    h.0 - 1
}
fn index_to_handle(i: usize) -> ReorderBufferHandle {
    ReorderBufferHandle(i + 1)
}

fn register(buffer: ReorderBuffer) -> ReorderBufferHandle {
    BUFFERS.with(|b| {
        let mut tab = b.borrow_mut();
        if let Some(i) = tab.iter().position(|s| s.is_none()) {
            tab[i] = Some(buffer);
            index_to_handle(i)
        } else {
            tab.push(Some(buffer));
            index_to_handle(tab.len() - 1)
        }
    })
}

/// Run `f` against the live reorder buffer behind `handle`.
pub fn with_buffer<R>(handle: ReorderBufferHandle, f: impl FnOnce(&mut ReorderBuffer) -> R) -> R {
    BUFFERS.with(|b| {
        let mut tab = b.borrow_mut();
        let slot = tab
            .get_mut(handle_to_index(handle))
            .and_then(|s| s.as_mut())
            .expect("ReorderBufferHandle refers to a freed/unknown buffer");
        f(slot)
    })
}

/// Like [`with_buffer`] but returns `None` if the handle is unknown/freed.
pub fn with_buffer_opt<R>(
    handle: ReorderBufferHandle,
    f: impl FnOnce(&mut ReorderBuffer) -> R,
) -> Option<R> {
    BUFFERS.with(|b| {
        let mut tab = b.borrow_mut();
        tab.get_mut(handle_to_index(handle))
            .and_then(|s| s.as_mut())
            .map(f)
    })
}

// ---------------------------------------------------------------------------
// TxnHandle <-> xid. The handle layer keys txns by xid (the by_txn table is the
// authority); a TxnHandle carries `1 + xid` so `0` stays the C NULL.
// ---------------------------------------------------------------------------

fn txn_handle_to_xid(h: TxnHandle) -> TransactionId {
    debug_assert!(h.0 != 0, "NULL TxnHandle");
    (h.0 - 1) as TransactionId
}
fn xid_to_txn_handle(xid: TransactionId) -> TxnHandle {
    TxnHandle(xid as usize + 1)
}

// ---------------------------------------------------------------------------
// Inward seam adapters
// ---------------------------------------------------------------------------

fn seam_allocate() -> ReorderBufferHandle {
    register(ReorderBuffer::allocate())
}

fn seam_free(handle: ReorderBufferHandle) {
    BUFFERS.with(|b| {
        let mut tab = b.borrow_mut();
        if let Some(slot) = tab.get_mut(handle_to_index(handle)) {
            *slot = None;
        }
    });
}

fn seam_wire_callbacks(handle: ReorderBufferHandle) {
    with_buffer(handle, |rb| rb.wire_callbacks());
}

fn seam_set_output_rewrites(handle: ReorderBufferHandle, value: bool) {
    with_buffer(handle, |rb| rb.set_output_rewrites(value));
}

fn seam_stats(handle: ReorderBufferHandle) -> ReorderBufferStats {
    with_buffer(handle, |rb| rb.stats())
}

fn seam_reset_stats(handle: ReorderBufferHandle) {
    with_buffer(handle, |rb| rb.reset_stats());
}

fn seam_xid_has_base_snapshot(handle: ReorderBufferHandle, xid: TransactionId) -> bool {
    with_buffer(handle, |rb| rb.xid_has_base_snapshot(xid))
}

fn seam_set_base_snapshot(
    handle: ReorderBufferHandle,
    xid: TransactionId,
    lsn: XLogRecPtr,
    snap: SnapshotData,
) {
    with_buffer(handle, |rb| rb.set_base_snapshot(xid, lsn, snap));
}

fn seam_xid_set_catalog_changes(handle: ReorderBufferHandle, xid: TransactionId, lsn: XLogRecPtr) {
    with_buffer(handle, |rb| rb.xid_set_catalog_changes(xid, lsn));
}

#[allow(clippy::too_many_arguments)]
fn seam_add_new_tuple_cids(
    handle: ReorderBufferHandle,
    xid: TransactionId,
    lsn: XLogRecPtr,
    locator: RelFileLocator,
    tid: ItemPointerData,
    cmin: CommandId,
    cmax: CommandId,
    combocid: CommandId,
) {
    with_buffer(handle, |rb| {
        rb.add_new_tuple_cids(xid, lsn, locator, tid, cmin, cmax, combocid)
    });
}

fn seam_add_new_command_id(
    handle: ReorderBufferHandle,
    xid: TransactionId,
    lsn: XLogRecPtr,
    cid: CommandId,
) {
    with_buffer(handle, |rb| rb.add_new_command_id(xid, lsn, cid));
}

fn seam_xid_has_catalog_changes(handle: ReorderBufferHandle, xid: TransactionId) -> bool {
    with_buffer(handle, |rb| rb.xid_has_catalog_changes(xid))
}

fn seam_get_oldest_xmin(handle: ReorderBufferHandle) -> TransactionId {
    with_buffer(handle, |rb| rb.get_oldest_xmin())
}

fn seam_set_restart_point(handle: ReorderBufferHandle, ptr: XLogRecPtr) {
    with_buffer(handle, |rb| rb.set_restart_point(ptr));
}

fn seam_add_snapshot(
    handle: ReorderBufferHandle,
    xid: TransactionId,
    lsn: XLogRecPtr,
    snap: SnapshotData,
) {
    with_buffer(handle, |rb| rb.add_snapshot(xid, lsn, snap));
}

fn seam_add_distributed_invalidations(
    handle: ReorderBufferHandle,
    xid: TransactionId,
    lsn: XLogRecPtr,
    msgs: alloc::vec::Vec<SharedInvalidationMessage>,
) {
    with_buffer(handle, |rb| rb.add_distributed_invalidations(xid, lsn, msgs));
}

fn seam_get_invalidations(
    handle: ReorderBufferHandle,
    xid: TransactionId,
) -> alloc::vec::Vec<SharedInvalidationMessage> {
    with_buffer(handle, |rb| rb.get_invalidations(xid))
}

fn seam_get_catalog_changes_xacts(
    handle: ReorderBufferHandle,
) -> alloc::vec::Vec<TransactionId> {
    with_buffer(handle, |rb| rb.get_catalog_changes_xacts())
}

fn seam_catchange_count(handle: ReorderBufferHandle) -> usize {
    with_buffer(handle, |rb| rb.catchange_count())
}

fn seam_current_restart_decoding_lsn(handle: ReorderBufferHandle) -> XLogRecPtr {
    with_buffer(handle, |rb| rb.current_restart_decoding_lsn())
}

fn seam_toplevel_txns(handle: ReorderBufferHandle) -> alloc::vec::Vec<TxnHandle> {
    with_buffer(handle, |rb| {
        rb.toplevel_txns()
            .into_iter()
            .map(xid_to_txn_handle)
            .collect()
    })
}

fn seam_get_oldest_txn(handle: ReorderBufferHandle) -> Option<TxnHandle> {
    with_buffer(handle, |rb| rb.get_oldest_txn().map(xid_to_txn_handle))
}

fn seam_txn_xid(handle: ReorderBufferHandle, txn: TxnHandle) -> TransactionId {
    let xid = txn_handle_to_xid(txn);
    with_buffer(handle, |rb| rb.txn_xid(xid))
}

fn seam_txn_restart_decoding_lsn(handle: ReorderBufferHandle, txn: TxnHandle) -> XLogRecPtr {
    let xid = txn_handle_to_xid(txn);
    with_buffer(handle, |rb| rb.txn_restart_decoding_lsn(xid))
}

fn seam_txn_is_prepared(handle: ReorderBufferHandle, txn: TxnHandle) -> bool {
    let xid = txn_handle_to_xid(txn);
    with_buffer(handle, |rb| rb.txn_is_prepared(xid))
}

// ---------------------------------------------------------------------------
// Active tuplecid hash (the `static HTAB *tuplecid_data` that
// SetupHistoricSnapshot points at, owned here because reorderbuffer builds and
// owns the per-txn `tuplecid_hash`). ReorderBufferProcessTXN (change-replay
// family) sets this from `txn->tuplecid_hash` after building it; until that
// family lands the active hash stays `None`, which is exactly the C
// `tuplecid_data == NULL` path that makes ResolveCminCmaxDuringDecoding return
// false ("CID is from the future command").
// ---------------------------------------------------------------------------

::std::thread_local! {
    static ACTIVE_TUPLECID: RefCell<Option<HashMap<ReorderBufferTupleCidKey, ReorderBufferTupleCidEnt>>> =
        const { RefCell::new(None) };
}

/// `SetupHistoricSnapshot(snapshot, tuplecid_hash)` side: make `hash` the
/// active `(relfilelocator, ctid) -> (cmin, cmax)` lookup. Called by
/// ReorderBufferProcessTXN once the change-replay family lands.
pub fn set_active_tuplecid_hash(
    hash: Option<HashMap<ReorderBufferTupleCidKey, ReorderBufferTupleCidEnt>>,
) {
    ACTIVE_TUPLECID.with(|a| *a.borrow_mut() = hash);
}

/// `ResolveCminCmaxDuringDecoding(tuplecid_data, snapshot, htup, buffer, &cmin,
/// &cmax)` — look up the actual cmin/cmax of a tuple seen by a historic
/// (logical-decoding) MVCC snapshot.
fn seam_resolve_cmin_cmax_during_decoding(
    snapshot: SnapshotData,
    htup: HeapTupleData<'_>,
    buffer: Buffer,
    mut cmin: CommandId,
    mut cmax: CommandId,
) -> PgResult<ResolveCminCmaxResult> {
    // Return unresolved if tuplecid_data is not valid. That's because when
    // streaming in-progress transactions we may run into tuples with the CID
    // before actually decoding them (e.g. INSERT followed by TRUNCATE). So we
    // assume the CID is from the future command.
    let active = ACTIVE_TUPLECID.with(|a| a.borrow().is_some());
    if !active {
        return Ok(ResolveCminCmaxResult {
            resolved: false,
            cmin,
            cmax,
        });
    }

    // get relfilelocator from the buffer; no convenient way other than that.
    let (rlocator, forkno, blockno) =
        backend_storage_buffer_bufmgr_seams::buffer_get_tag::call(buffer)?;

    // tuples can only be in the main fork.
    debug_assert!(forkno == ForkNumber::MAIN_FORKNUM);
    debug_assert!(blockno == htup.t_self.ip_blkid.block_number());

    let key = ReorderBufferTupleCidKey {
        rlocator,
        tid: htup.t_self,
    };

    let mut updated_mapping = false;
    loop {
        let found = ACTIVE_TUPLECID.with(|a| a.borrow().as_ref().and_then(|h| h.get(&key).copied()));

        match found {
            Some(ent) => {
                cmin = ent.cmin;
                cmax = ent.cmax;
                return Ok(ResolveCminCmaxResult {
                    resolved: true,
                    cmin,
                    cmax,
                });
            }
            None => {
                // failed to find a mapping; check whether the table was
                // rewritten and apply mappings if so, but only once (we hold a
                // lock on the relation so no new mappings can appear).
                if !updated_mapping {
                    update_logical_mappings(htup.t_tableOid, &snapshot)?;
                    updated_mapping = true;
                    continue;
                }
                let _ = InvalidCommandId;
                return Ok(ResolveCminCmaxResult {
                    resolved: false,
                    cmin,
                    cmax,
                });
            }
        }
    }
}

/// `UpdateLogicalMappings(tuplecid_data, relid, snapshot)` — apply any logical
/// remapping files targeted at our transaction (when a catalog relation was
/// rewritten with `VACUUM FULL`/`CLUSTER` during decoding). This scans
/// `pg_logical/mappings/`, parses each `map-*` filename, and replays
/// `ApplyLogicalMappingFile` in LSN order. It needs the fd/dir layer, the
/// logical-rewrite filename format, and `TransactionIdDidCommit`; those land
/// with the logical-rewrite-mapping family. Until then a rewritten catalog
/// relation during decoding hits this loud panic (mirror-PG-and-panic) rather
/// than silently mis-resolving a CID.
fn update_logical_mappings(_relid: types_core::Oid, _snapshot: &SnapshotData) -> PgResult<()> {
    panic!(
        "UpdateLogicalMappings: logical-rewrite mapping-file replay not yet \
         ported (logical-rewrite-mapping family)"
    );
}

/// Install every inward seam this unit owns (foundational + snapshot-management
/// families). The snapshot-management family installs the 26th inward seam,
/// `resolve_cmin_cmax_during_decoding`.
pub fn init_seams() {
    use backend_replication_logical_reorderbuffer_seams as s;

    s::resolve_cmin_cmax_during_decoding::set(seam_resolve_cmin_cmax_during_decoding);
    s::ReorderBufferAllocate::set(seam_allocate);
    s::ReorderBufferFree::set(seam_free);
    s::wire_reorderbuffer_callbacks::set(seam_wire_callbacks);
    s::set_output_rewrites::set(seam_set_output_rewrites);
    s::reorderbuffer_stats::set(seam_stats);
    s::reorderbuffer_reset_stats::set(seam_reset_stats);

    s::ReorderBufferXidHasBaseSnapshot::set(seam_xid_has_base_snapshot);
    s::ReorderBufferSetBaseSnapshot::set(seam_set_base_snapshot);
    s::ReorderBufferXidSetCatalogChanges::set(seam_xid_set_catalog_changes);
    s::ReorderBufferAddNewTupleCids::set(seam_add_new_tuple_cids);
    s::ReorderBufferAddNewCommandId::set(seam_add_new_command_id);
    s::ReorderBufferXidHasCatalogChanges::set(seam_xid_has_catalog_changes);
    s::ReorderBufferGetOldestXmin::set(seam_get_oldest_xmin);
    s::ReorderBufferSetRestartPoint::set(seam_set_restart_point);
    s::ReorderBufferAddSnapshot::set(seam_add_snapshot);
    s::ReorderBufferAddDistributedInvalidations::set(seam_add_distributed_invalidations);
    s::ReorderBufferGetInvalidations::set(seam_get_invalidations);
    s::ReorderBufferGetCatalogChangesXacts::set(seam_get_catalog_changes_xacts);
    s::reorder_buffer_catchange_count::set(seam_catchange_count);
    s::reorder_buffer_current_restart_decoding_lsn::set(seam_current_restart_decoding_lsn);
    s::reorder_buffer_toplevel_txns::set(seam_toplevel_txns);
    s::ReorderBufferGetOldestTXN::set(seam_get_oldest_txn);
    s::reorder_buffer_txn_xid::set(seam_txn_xid);
    s::reorder_buffer_txn_restart_decoding_lsn::set(seam_txn_restart_decoding_lsn);
    s::reorder_buffer_txn_is_prepared::set(seam_txn_is_prepared);
}
