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

use crate::snapshot::ReorderBufferTupleCidKey;
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

/// `TxnHandle` for `xid`, for the change-replay callbacks that re-enter logical
/// decoding (the `ReorderBufferCallback` variants carry a `TxnHandle` the
/// output-plugin wrappers resolve back to the txn). Same encoding as the
/// registry's seam adapters use.
pub(crate) fn txn_handle_for_xid(xid: TransactionId) -> TxnHandle {
    xid_to_txn_handle(xid)
}

// ---------------------------------------------------------------------------
// Output-plugin handle facade: RelationHandle / RelationsHandle / ChangeHandle.
//
// `ReorderBufferProcessTXN` resolves the change's relation OID(s) via
// `RelidByRelfilenumber`/`RelationIdGetRelation` and dispatches the change to
// the output plugin synchronously. The plugin reads the relation descriptor and
// the decoded old/new tuples through the opaque handles the callback carries (in
// C these are the live `Relation`/`Relation[]`/`ReorderBufferChange *`
// pointers). We publish the live values in a backend-local scratch slot for the
// duration of the synchronous callback so the resolver seams can recover them.
//
// A `RelationHandle` carries `1 + reloid` (`0` is C NULL). A `RelationsHandle`
// indexes the scratch's `relations` vector; a `ChangeHandle` indexes the
// scratch's `change`.
// ---------------------------------------------------------------------------

use types_core::Oid;
use types_logical::{ChangeHandle, MessageHandle, PrefixHandle, RelationHandle, RelationsHandle};

/// Live values published for the in-flight output-plugin callback.
#[derive(Default)]
struct DispatchScratch {
    /// The decoded payload of the change currently being applied
    /// (`ChangeHandle` resolves here). `1` is the only handle handed out.
    change: Option<seams::ResolvedChange>,
    /// The truncate `relations[]` OIDs (`RelationsHandle` resolves here).
    relations: alloc::vec::Vec<Oid>,
    /// The logical-message `prefix` bytes (`PrefixHandle` resolves here).
    prefix: alloc::vec::Vec<u8>,
    /// The logical-message body bytes (`MessageHandle` resolves here).
    message: alloc::vec::Vec<u8>,
}

use backend_replication_logical_reorderbuffer_seams as seams;

::std::thread_local! {
    static DISPATCH: RefCell<DispatchScratch> = RefCell::new(DispatchScratch::default());
}

fn reloid_to_relation_handle(reloid: Oid) -> RelationHandle {
    RelationHandle(reloid as usize + 1)
}
fn relation_handle_to_reloid(h: RelationHandle) -> Oid {
    debug_assert!(h.0 != 0, "NULL RelationHandle");
    (h.0 - 1) as Oid
}

/// Publish `change` as the in-flight change, run `f` (the synchronous callback
/// dispatch), then clear the scratch. Returns the `ChangeHandle` for `change`.
pub(crate) fn with_change_published<R>(
    change: seams::ResolvedChange,
    f: impl FnOnce(ChangeHandle) -> R,
) -> R {
    DISPATCH.with(|d| d.borrow_mut().change = Some(change));
    let out = f(ChangeHandle(1));
    DISPATCH.with(|d| d.borrow_mut().change = None);
    out
}

/// Publish the truncate `relations[]` OIDs, run `f` with the
/// `RelationsHandle`, then clear the scratch.
pub(crate) fn with_relations_published<R>(
    relations: alloc::vec::Vec<Oid>,
    f: impl FnOnce(RelationsHandle) -> R,
) -> R {
    DISPATCH.with(|d| d.borrow_mut().relations = relations);
    let out = f(RelationsHandle(1));
    DISPATCH.with(|d| d.borrow_mut().relations = alloc::vec::Vec::new());
    out
}

/// `RelationHandle` for a resolved relation OID (the change/truncate callbacks
/// carry it; the plugin re-`RelationIdGetRelation`s the OID).
pub(crate) fn relation_handle_for_oid(reloid: Oid) -> RelationHandle {
    reloid_to_relation_handle(reloid)
}

fn seam_resolve_relation_handle(relation: RelationHandle) -> Oid {
    relation_handle_to_reloid(relation)
}
fn seam_resolve_relations_handle(_relations: RelationsHandle, index: i32) -> Oid {
    DISPATCH.with(|d| d.borrow().relations[index as usize])
}
fn seam_resolve_change_handle(_change: ChangeHandle) -> seams::ResolvedChange {
    DISPATCH.with(|d| {
        d.borrow()
            .change
            .clone()
            .expect("ChangeHandle resolved outside an in-flight apply callback")
    })
}

/// Publish the logical-message `prefix`/body bytes, run `f` with their handles,
/// then clear the scratch. Mirrors C passing `change->data.msg.{prefix,message}`
/// (`const char *`) straight into the synchronous message callback.
pub(crate) fn with_message_published<R>(
    prefix: alloc::vec::Vec<u8>,
    message: alloc::vec::Vec<u8>,
    f: impl FnOnce(PrefixHandle, MessageHandle) -> R,
) -> R {
    DISPATCH.with(|d| {
        let mut s = d.borrow_mut();
        s.prefix = prefix;
        s.message = message;
    });
    let out = f(PrefixHandle(1), MessageHandle(1));
    DISPATCH.with(|d| {
        let mut s = d.borrow_mut();
        s.prefix = alloc::vec::Vec::new();
        s.message = alloc::vec::Vec::new();
    });
    out
}

fn seam_resolve_prefix_handle(_prefix: PrefixHandle) -> alloc::vec::Vec<u8> {
    DISPATCH.with(|d| d.borrow().prefix.clone())
}
fn seam_resolve_message_handle(_message: MessageHandle) -> alloc::vec::Vec<u8> {
    DISPATCH.with(|d| d.borrow().message.clone())
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
// Change-replay entry points (consumed by decode.c).
// ---------------------------------------------------------------------------

fn seam_assign_child(
    handle: ReorderBufferHandle,
    xid: TransactionId,
    subxid: TransactionId,
    lsn: XLogRecPtr,
) {
    with_buffer(handle, |rb| rb.assign_child(xid, subxid, lsn));
}

fn seam_commit_child(
    handle: ReorderBufferHandle,
    xid: TransactionId,
    subxid: TransactionId,
    commit_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
) {
    with_buffer(handle, |rb| rb.commit_child(xid, subxid, commit_lsn, end_lsn));
}

#[allow(clippy::too_many_arguments)]
fn seam_commit(
    handle: ReorderBufferHandle,
    xid: TransactionId,
    commit_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
    commit_time: types_core::primitive::TimestampTz,
    origin_id: types_core::primitive::RepOriginId,
    origin_lsn: XLogRecPtr,
) {
    with_buffer(handle, |rb| {
        rb.commit(xid, commit_lsn, end_lsn, commit_time, origin_id, origin_lsn)
    });
}

// ---------------------------------------------------------------------------
// Further change-replay / commit-time entry points (consumed by decode.c).
//
// The owning families (change replay, spill-to-disk, streaming, cleanup /
// commit-time) are not yet landed in this crate (see the crate header). Their
// seams are installed here with loud panics (mirror-PG-and-panic) so decode.c
// can be ported against the complete seam surface and any actual decode of a
// data-bearing record fails loudly rather than silently dropping changes.
// ---------------------------------------------------------------------------

fn seam_process_xid(rb: ReorderBufferHandle, xid: TransactionId, lsn: XLogRecPtr) {
    with_buffer(rb, |b| b.process_xid(xid, lsn));
}

#[allow(clippy::too_many_arguments)]
fn seam_queue_change(
    rb: ReorderBufferHandle,
    xid: TransactionId,
    lsn: XLogRecPtr,
    kind: backend_replication_logical_reorderbuffer_seams::DecodedChangeKind,
    rlocator: RelFileLocator,
    oldtuple: Option<backend_replication_logical_reorderbuffer_seams::DecodedTuple>,
    newtuple: Option<backend_replication_logical_reorderbuffer_seams::DecodedTuple>,
    toast_insert: bool,
) {
    with_buffer(rb, |b| {
        b.queue_decoded_change(xid, lsn, kind, rlocator, oldtuple, newtuple, toast_insert)
    });
}

fn seam_queue_truncate(
    rb: ReorderBufferHandle,
    xid: TransactionId,
    lsn: XLogRecPtr,
    cascade: bool,
    restart_seqs: bool,
    relids: alloc::vec::Vec<types_core::Oid>,
) {
    with_buffer(rb, |b| b.queue_truncate(xid, lsn, cascade, restart_seqs, relids));
}

fn seam_queue_message(
    rb: ReorderBufferHandle,
    xid: TransactionId,
    lsn: XLogRecPtr,
    transactional: bool,
    prefix: alloc::vec::Vec<u8>,
    message: alloc::vec::Vec<u8>,
) {
    with_buffer(rb, |b| b.queue_message(xid, lsn, transactional, prefix, message));
}

fn seam_forget(rb: ReorderBufferHandle, xid: TransactionId, lsn: XLogRecPtr) {
    with_buffer(rb, |b| b.forget(xid, lsn));
}

fn seam_abort(
    rb: ReorderBufferHandle,
    xid: TransactionId,
    lsn: XLogRecPtr,
    abort_time: types_core::primitive::TimestampTz,
) {
    with_buffer(rb, |b| b.abort(xid, lsn, abort_time));
}

fn seam_abort_old(rb: ReorderBufferHandle, oldest_running_xid: TransactionId) {
    with_buffer(rb, |b| b.abort_old(oldest_running_xid));
}

#[allow(clippy::too_many_arguments)]
fn seam_finish_prepared(
    rb: ReorderBufferHandle,
    xid: TransactionId,
    commit_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
    two_phase_at: XLogRecPtr,
    commit_time: types_core::primitive::TimestampTz,
    origin_id: types_core::primitive::RepOriginId,
    origin_lsn: XLogRecPtr,
    gid: alloc::vec::Vec<u8>,
    is_commit: bool,
) {
    with_buffer(rb, |b| {
        b.finish_prepared(
            xid, commit_lsn, end_lsn, two_phase_at, commit_time, origin_id, origin_lsn, gid,
            is_commit,
        )
    });
}

fn seam_prepare(rb: ReorderBufferHandle, xid: TransactionId, gid: alloc::vec::Vec<u8>) {
    with_buffer(rb, |b| b.prepare(xid, gid));
}

fn seam_skip_prepare(rb: ReorderBufferHandle, xid: TransactionId) {
    with_buffer(rb, |b| b.skip_prepare(xid));
}

fn seam_immediate_invalidation(
    rb: ReorderBufferHandle,
    invalidations: alloc::vec::Vec<SharedInvalidationMessage>,
) {
    with_buffer(rb, |b| b.immediate_invalidation(&invalidations));
}

fn seam_add_invalidations(
    rb: ReorderBufferHandle,
    xid: TransactionId,
    lsn: XLogRecPtr,
    invalidations: alloc::vec::Vec<SharedInvalidationMessage>,
) {
    with_buffer(rb, |b| b.add_invalidations(xid, lsn, invalidations));
}

#[allow(clippy::too_many_arguments)]
fn seam_remember_prepare_info(
    rb: ReorderBufferHandle,
    xid: TransactionId,
    prepare_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
    prepare_time: types_core::primitive::TimestampTz,
    origin_id: types_core::primitive::RepOriginId,
    origin_lsn: XLogRecPtr,
) -> bool {
    with_buffer(rb, |b| {
        b.remember_prepare_info(xid, prepare_lsn, end_lsn, prepare_time, origin_id, origin_lsn)
    })
}

fn seam_invalidate(rb: ReorderBufferHandle, xid: TransactionId, lsn: XLogRecPtr) {
    with_buffer(rb, |b| b.invalidate(xid, lsn));
}

// ---------------------------------------------------------------------------
// Active tuplecid hash.
//
// In C the `(relfilelocator, ctid) -> (cmin, cmax)` map is owned by snapmgr's
// file-scope `static HTAB *tuplecid_data`: ReorderBufferProcessTXN builds the
// per-txn `tuplecid_hash` and hands it to `SetupHistoricSnapshot`, then the
// resolver fetches it back with `HistoricSnapshotGetTupleCids()`. We mirror
// that exactly — snapmgr owns the active map (value-typed) and we go through
// its `historic_snapshot_get_tuple_cids` seam. A `None` here is the C
// `tuplecid_data == NULL` path that makes ResolveCminCmaxDuringDecoding return
// false ("CID is from the future command").
// ---------------------------------------------------------------------------

/// `ResolveCminCmaxDuringDecoding(tuplecid_data, snapshot, htup, buffer, &cmin,
/// &cmax)` — look up the actual cmin/cmax of a tuple seen by a historic
/// (logical-decoding) MVCC snapshot. The C caller passes
/// `HistoricSnapshotGetTupleCids()`; here the resolver fetches the active map
/// from snapmgr (its owner) via the same seam.
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
    let tuplecid_data = backend_utils_time_snapmgr_seams::historic_snapshot_get_tuple_cids::call();
    let tuplecid_data = match tuplecid_data {
        Some(h) => h,
        None => {
            return Ok(ResolveCminCmaxResult {
                resolved: false,
                cmin,
                cmax,
            });
        }
    };

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
        let found = tuplecid_data.get(&key).copied();

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
///
/// NOTE: when this lands it must mutate snapmgr's owned `tuplecid_data`
/// (C `HASH_ENTER`s into it), so it needs to take `&mut TupleCidHash` and
/// `seam_resolve_cmin_cmax_during_decoding` must re-fetch / write the map back
/// through a mutating snapmgr seam rather than reading a clone (the current
/// read-only fetch is faithful only because every non-panicking path performs a
/// single lookup with no mapping update).
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

    // Change-replay family entry points (consumed by decode.c).
    s::ReorderBufferAssignChild::set(seam_assign_child);
    s::ReorderBufferCommitChild::set(seam_commit_child);
    s::ReorderBufferCommit::set(seam_commit);

    // Further change-replay / commit-time entry points (panic until their
    // families land; see above).
    s::ReorderBufferProcessXid::set(seam_process_xid);
    s::ReorderBufferQueueChange::set(seam_queue_change);
    s::ReorderBufferQueueTruncate::set(seam_queue_truncate);
    s::ReorderBufferQueueMessage::set(seam_queue_message);
    s::ReorderBufferForget::set(seam_forget);
    s::ReorderBufferAbort::set(seam_abort);
    s::ReorderBufferAbortOld::set(seam_abort_old);
    s::ReorderBufferFinishPrepared::set(seam_finish_prepared);
    s::ReorderBufferPrepare::set(seam_prepare);
    s::ReorderBufferSkipPrepare::set(seam_skip_prepare);
    s::ReorderBufferInvalidate::set(seam_invalidate);
    s::ReorderBufferImmediateInvalidation::set(seam_immediate_invalidation);
    s::ReorderBufferAddInvalidations::set(seam_add_invalidations);
    s::ReorderBufferRememberPrepareInfo::set(seam_remember_prepare_info);

    // Output-plugin handle-resolver facade (the change-replay apply path).
    s::resolve_relation_handle::set(seam_resolve_relation_handle);
    s::resolve_relations_handle::set(seam_resolve_relations_handle);
    s::resolve_change_handle::set(seam_resolve_change_handle);
    s::resolve_prefix_handle::set(seam_resolve_prefix_handle);
    s::resolve_message_handle::set(seam_resolve_message_handle);

    // WAL-startup cleanup of leftover on-disk spill files (StartupXLOG).
    s::startup_reorder_buffer::set(crate::spill::startup_reorder_buffer);

    // GUC `conf->variable` accessors for the two reorderbuffer.c-owned `int`
    // globals (`logical_decoding_work_mem` / `debug_logical_replication_streaming`).
    // The GUC machinery seeds these from boot_val during InitializeGUCOptions and
    // the eviction / streaming-decision paths read the current value back.
    use backend_utils_misc_guc_tables::{vars, GucVarAccessors};
    vars::logical_decoding_work_mem.install(GucVarAccessors {
        get: crate::logical_decoding_work_mem,
        set: crate::set_logical_decoding_work_mem,
    });
    vars::debug_logical_replication_streaming.install(GucVarAccessors {
        get: crate::debug_logical_replication_streaming,
        set: crate::set_debug_logical_replication_streaming,
    });
}
