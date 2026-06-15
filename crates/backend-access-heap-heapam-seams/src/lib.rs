//! Seam declarations for the `backend-access-heap-heapam` unit
//! (`access/heap/heapam.c`), the heap access method.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
use types_core::primitive::Oid;
use types_tuple::backend_access_common_heaptuple::Datum as TupleDatum;
use types_error::PgResult;
use types_rel::{Relation, RelationData};
use types_tuple::heaptuple::FormData_pg_attribute;
use types_tuple::pg_type::FormData_pg_type;
use types_core::primitive::{OffsetNumber, TransactionId};
use types_nbtree::TmIndexDeleteOp;
use types_snapshot::SnapshotData;
use types_storage::Buffer;
use types_tuple::heaptuple::{HeapTupleData, HeapTupleHeaderData, ItemPointerData};
use types_core::xact::CommandId;
use types_tableam::tableam::{
    LockTupleMode, LockWaitPolicy, TM_FailureData, TM_Result, TU_UpdateIndexes,
};
use types_tuple::backend_access_common_heaptuple::FormedTuple;
use types_xlog_records::multixact::MultiXactStatus;
use types_storage::lock::XLTW_Oper;

seam_core::seam!(
    /// `HeapKeyTest(tuple, RelationGetDescr(rel), nkeys, keys)` (`access/valid.h`)
    /// — does the tuple satisfy all `nkeys` scan keys? Owned by the heap AM scan
    /// layer; SANCTIONED panic-until-keystone: the trimmed
    /// [`types_tableam::scankey::ScanKeyData`] has no `sk_func`/`sk_argument`, so
    /// the per-key comparison cannot run until the scan-key carrier keystone
    /// (task #281) widens it. `nkeys == 0` (the executor seqscan path) never
    /// reaches this seam. `Err` carries the comparison-function `ereport(ERROR)`.
    pub fn heap_key_test<'mcx>(
        tuple: &FormedTuple<'mcx>,
        rel: &RelationData<'mcx>,
        keys: &PgVec<'mcx, types_tableam::scankey::ScanKeyData>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// The bootstrap-mode tuple-insert sequence, batched at the heap owner
    /// (bootstrap.c `InsertOneTuple`): `tupDesc = CreateTupleDesc(numattr,
    /// attrtypes); tuple = heap_form_tuple(tupDesc, values, Nulls);
    /// simple_heap_insert(boot_reldesc, tuple); heap_freetuple(tuple)`. The
    /// provider forms the tuple from the borrowed relation's descriptor and
    /// the supplied column data, then `simple_heap_insert`s it.
    /// `attrtypes`/`values`/`nulls` are `numattr` long. `values` are the
    /// canonical `Datum<'mcx>` (`heap_form_tuple`'s element type), so
    /// by-reference column images (`bytea`/`text`/`oidvector`/etc.) carry their
    /// bytes through, not a nulled bare word. `Err` carries the heap-insert
    /// error surface (and OOM).
    pub fn insert_one_tuple<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &RelationData<'mcx>,
        attrtypes: &[FormData_pg_attribute],
        values: &[TupleDatum<'mcx>],
        nulls: &[bool],
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `populate_typ_list()`'s catalog read (bootstrap.c), batched at the heap
    /// owner: `table_open(TypeRelationId, NoLock)` +
    /// `table_beginscan_catalog` + `heap_getnext` loop + `table_endscan` +
    /// `table_close`. Returns each `pg_type` row as `(am_oid, FormData)`,
    /// copied into `mcx`. `Err` carries the scan/open error surface and OOM.
    pub fn read_pg_type<'mcx>(
        mcx: Mcx<'mcx>,
    ) -> PgResult<PgVec<'mcx, (Oid, FormData_pg_type)>>
);

seam_core::seam!(
    /// `simple_heap_insert(relation, tup)` (heapam.c) — insert one heap tuple
    /// with a default command id and no speedup options, stamping `tup`'s
    /// header and toasting if necessary. The repo carries the heap tuple as the
    /// owned `FormedTuple` value (header + user-data area); `tup.tuple.t_self`
    /// is updated in place with the stored TID. `Err` carries the insert
    /// `ereport(ERROR)` surface. **Installed by `backend-access-heap-heapam`.**
    pub fn simple_heap_insert<'mcx>(
        mcx: Mcx<'mcx>,
        relation: &Relation<'mcx>,
        tup: &mut types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `heap_insert(relation, tup, cid, options, bistate)` (heapam.c) — insert
    /// one heap tuple stamped with the current xid and the given `cid`.
    /// `options` is the `HEAP_INSERT_*` bitmask; `bistate` is the optional
    /// bulk-insert carrier. `tup`'s header is stamped in place and
    /// `tup.tuple.t_self` receives the stored TID. `Err` carries the insert
    /// `ereport(ERROR)` surface. **Installed by `backend-access-heap-heapam`.**
    pub fn heap_insert<'mcx>(
        mcx: Mcx<'mcx>,
        relation: &Relation<'mcx>,
        tup: &mut types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
        cid: types_core::xact::CommandId,
        options: i32,
        bistate: Option<&mut types_tableam::tableam::BulkInsertStateData>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `HeapTupleGetUpdateXid(htup)` (heapam.c) — resolve a multixact xmax to
    /// the single update XID it carries (`MultiXactIdGetUpdateXid`), or
    /// `InvalidTransactionId` if the multixact has no updater. Only the header's
    /// xmax/infomask are consulted, so a borrowed header suffices. `Err` carries
    /// the multixact-member-read `ereport` surface.
    pub fn heap_tuple_get_update_xid(
        tuple: &types_tuple::heaptuple::HeapTupleHeaderData<'_>,
    ) -> PgResult<types_core::primitive::TransactionId>
);

seam_core::seam!(
    /// `table_open(IndexRelationId, AccessShareLock)` +
    /// `ScanKeyInit(indisclustered = true)` + `table_beginscan_catalog` +
    /// `heap_getnext(ForwardScanDirection)` loop + `table_endscan` +
    /// `relation_close`, batched (the genam `systable_scan` precedent): the
    /// `(indrelid, indexrelid)` of every pg_index row with `indisclustered`.
    /// Used by CLUSTER's `get_tables_to_cluster`.
    pub fn scan_indisclustered<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, (Oid, Oid)>>
);

seam_core::seam!(
    /// `log_heap_visible(rel, heap_buffer, vm_buffer, snapshotConflictHorizon,
    /// vmflags)` (heapam.c): emit the `XLOG_HEAP2_VISIBLE` WAL record when a
    /// visibility-map bit is *set* during VACUUM, registering the VM buffer
    /// (and, when `XLogHintBitIsNeeded()`, the heap buffer as an FPI) and
    /// returning the record's LSN. The `RelationIsAccessibleInLogicalDecoding`
    /// catalog-rel flag is OR'd into the record flags inside the owner. `Err`
    /// carries the `XLogInsert` `ereport(ERROR)` surface.
    pub fn log_heap_visible(
        rel: &RelationData<'_>,
        heap_buffer: types_storage::Buffer,
        vm_buffer: types_storage::Buffer,
        snapshot_conflict_horizon: types_core::primitive::TransactionId,
        vmflags: u8,
    ) -> PgResult<types_core::primitive::XLogRecPtr>
);

// ===========================================================================
// F6 — INDEX DELETE.  These are the heap-AM's tableam `index_delete_tuples`
// implementation and the two heapam.c primitives it leans on that live in
// *other* (not-yet-ported) heapam families.
// ===========================================================================

seam_core::seam!(
    /// `heap_index_delete_tuples(rel, delstate)` (heapam.c) — the heapam
    /// implementation of tableam's `index_delete_tuples` interface, called by
    /// index AMs during (simple or bottom-up) index tuple deletion. Sorts and
    /// (for bottom-up) shrinks `delstate->deltids`, visits the referenced heap
    /// blocks under share lock to determine which TIDs are deletable, and
    /// returns the operation's `snapshotConflictHorizon`. The `delstate`'s
    /// `ndeltids` (the `deltids`/`status` arrays) is updated in place, so the
    /// op is taken by `&mut`. `Err` carries the index-corruption / buffer
    /// `ereport(ERROR)` surface. **Installed by `backend-access-heap-heapam`.**
    pub fn heap_index_delete_tuples<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        delstate: &mut TmIndexDeleteOp<'mcx>,
    ) -> PgResult<TransactionId>
);

seam_core::seam!(
    /// `index_compute_xid_horizon_for_tuples(irel, hrel, ibuf, itemnos,
    /// nitems)` (heapam.c): visit the heap tuples referenced by the index
    /// line-pointers `itemnos` on index buffer `ibuf` and compute the latest
    /// removed XID (`snapshotConflictHorizon`) for an index-page LP_DEAD
    /// cleanup. Used by the hash AM's `_hash_vacuum_one_page`. `Err` carries the
    /// buffer/heap `ereport(ERROR)` surface. **Installed by
    /// `backend-access-heap-heapam`.**
    pub fn index_compute_xid_horizon_for_tuples<'mcx>(
        irel: &Relation<'mcx>,
        hrel: &Relation<'mcx>,
        ibuf: types_storage::storage::Buffer,
        itemnos: &[types_core::primitive::OffsetNumber],
    ) -> PgResult<TransactionId>
);

/// The result of [`heap_hot_search_buffer`] — mirrors C's by-pointer outputs
/// (`*tid` updated in place, `*heapTuple` filled, `*all_dead` optionally set)
/// plus the `bool` return value.
#[derive(Clone, Debug)]
pub struct HotSearchResult<'mcx> {
    /// C's `bool` return: a chain member satisfying the snapshot was found.
    pub found: bool,
    /// C's updated `*tid` (only meaningful when `found`).
    pub tid: ItemPointerData,
    /// C's `*heapTuple` output (only meaningful when `found`).
    pub heap_tuple: HeapTupleData<'mcx>,
    /// C's `*all_dead` output, when the caller requested it (`all_dead != NULL`).
    pub all_dead: Option<bool>,
}

/// The result of [`heap_fetch`] — C's `bool` return plus the by-pointer
/// outputs (`*userbuf` pinned buffer, `*tuple` filled in place).
#[derive(Clone, Debug)]
pub struct HeapFetchResult<'mcx> {
    /// C's `bool` return: the tuple at `tid` exists and satisfies `snapshot`.
    pub found: bool,
    /// C's `*userbuf` — the buffer pinned (and, on success, holding the tuple).
    /// `InvalidBuffer` when the page could not be read.
    pub userbuf: Buffer,
    /// C's `*tuple` — the filled `HeapTupleData` (t_self / t_data / t_len /
    /// t_tableOid). Only meaningful when `found`.
    pub tuple: HeapTupleData<'mcx>,
}

seam_core::seam!(
    /// `heap_fetch(relation, snapshot, tuple, userbuf, keep_buf)` (heapam.c) —
    /// look up the tuple at `tuple.t_self`, pin its page, and test it against
    /// `snapshot`. On a found+visible tuple the result carries the pinned
    /// `userbuf` (caller must release) and the filled `HeapTupleData`. With
    /// `keep_buf`, the buffer is kept pinned even when the tuple is not visible
    /// (so the caller can inspect it); otherwise it is released on a miss.
    /// Used by `heap_lock_updated_tuple_rec` to walk the update chain under
    /// `SnapshotAny`. `Err` carries the clog/multixact/buffer `ereport(ERROR)`
    /// surface. **Owned by the heapam scan family (heapam.c); uninstalled — and
    /// panics — until that family lands.**
    pub fn heap_fetch<'mcx>(
        mcx: Mcx<'mcx>,
        relation: &Relation<'mcx>,
        snapshot: &SnapshotData,
        tid: ItemPointerData,
        keep_buf: bool,
    ) -> PgResult<HeapFetchResult<'mcx>>
);

seam_core::seam!(
    /// `heap_hot_search_buffer(tid, rel, buf, snapshot, heapTuple, all_dead,
    /// first_call)` (heapam.c) — search the HOT chain rooted at `tid` (on the
    /// already pinned + share-locked `buf`) for the first member satisfying
    /// `snapshot`. `want_all_dead` requests C's `all_dead` output. The pin/lock
    /// on `buf` is retained on return. `Err` carries the clog/multixact
    /// `ereport(ERROR)` surface. **Owned by the heapam scan family (heapam.c);
    /// uninstalled — and panics — until that family lands.**
    pub fn heap_hot_search_buffer<'mcx>(
        mcx: Mcx<'mcx>,
        tid: ItemPointerData,
        rel: &Relation<'mcx>,
        buf: Buffer,
        snapshot: &SnapshotData,
        want_all_dead: bool,
        first_call: bool,
    ) -> PgResult<HotSearchResult<'mcx>>
);

// ===========================================================================
// F3 — UPDATE/DELETE.  `heap_delete` / `simple_heap_delete` are the heap-AM's
// own entry points (installed by `backend-access-heap-heapam`).  The lock-wait
// primitives they lean on (`heap_acquire_tuplock` / `DoesMultiXactIdConflict` /
// `MultiXactIdWait` / `UnlockTupleTuplock`) live in the *not-yet-ported* heapam
// LOCK family (heapam.c); they are declared here and stay uninstalled — and
// panic — until that family lands.
// ===========================================================================

seam_core::seam!(
    /// `heap_delete(relation, tid, cid, crosscheck, wait, tmfd, changingPart)`
    /// (heapam.c) — delete the tuple addressed by `tid`. Stamps the on-page
    /// tuple's xmax/cmax/infomask, clears its HOT-updated flag, points its ctid
    /// at itself (a partition-move sets the moved-partitions ctid), emits the
    /// `XLOG_HEAP_DELETE` WAL record, and (for a logically-decoded rel) logs the
    /// replica-identity old key. `crosscheck` is the optional transaction-snapshot
    /// RI snapshot (`None` == `InvalidSnapshot`). On a non-`TM_Ok` outcome `tmfd`
    /// is filled (ctid/xmax/cmax). `Err` carries the delete `ereport(ERROR)`
    /// surface. **Installed by `backend-access-heap-heapam`.**
    pub fn heap_delete<'mcx>(
        mcx: Mcx<'mcx>,
        relation: &Relation<'mcx>,
        tid: ItemPointerData,
        cid: CommandId,
        crosscheck: Option<&SnapshotData>,
        wait: bool,
        tmfd: &mut TM_FailureData,
        changing_part: bool,
    ) -> PgResult<TM_Result>
);

seam_core::seam!(
    /// `simple_heap_delete(relation, tid)` (heapam.c) — delete the tuple
    /// addressed by `tid` when no concurrent update is expected, reporting any
    /// non-`TM_Ok` outcome via `ereport(ERROR)`. **Installed by
    /// `backend-access-heap-heapam`.**
    pub fn simple_heap_delete<'mcx>(
        mcx: Mcx<'mcx>,
        relation: &Relation<'mcx>,
        tid: ItemPointerData,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `heap_acquire_tuplock(relation, tid, mode, wait_policy, have_tuple_lock)`
    /// (heapam.c) — acquire the lmgr tuple lock that establishes our priority on
    /// the tuple before sleeping on a concurrent locker. Returns the updated
    /// `*have_tuple_lock` (C's `bool` out param + `bool` return; C returns false
    /// only when a conditional/skip-locked acquire fails, which the blocking
    /// callers in delete never use). `Err` carries the lmgr `ereport(ERROR)`
    /// surface. **Owned by the heapam LOCK family (heapam.c); uninstalled — and
    /// panics — until that family lands.**
    pub fn heap_acquire_tuplock<'mcx>(
        relation: &Relation<'mcx>,
        tid: ItemPointerData,
        mode: LockTupleMode,
        wait_policy: LockWaitPolicy,
        have_tuple_lock: bool,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `UnlockTupleTuplock(relation, tid, mode)` (heapam.c macro over
    /// `UnlockTuple`) — release the lmgr tuple lock acquired by
    /// `heap_acquire_tuplock`. `Err` carries the lmgr `ereport(ERROR)` surface.
    /// **Owned by the heapam LOCK family (heapam.c); uninstalled — and panics —
    /// until that family lands.**
    pub fn unlock_tuple_tuplock<'mcx>(
        relation: &Relation<'mcx>,
        tid: ItemPointerData,
        mode: LockTupleMode,
    ) -> PgResult<()>
);

/// The result of [`does_multi_xact_id_conflict`] — C's `bool` return plus the
/// `*current_is_member` out param.
pub use types_storage::multixact::MultiXactConflict;

seam_core::seam!(
    /// `DoesMultiXactIdConflict(multi, infomask, lockmode, &current_is_member)`
    /// (heapam.c) — does any member of `multi` hold a lock that conflicts with
    /// the wanted `lockmode`, and is the current backend a member? `Err` carries
    /// the multixact-member-read `ereport(ERROR)` surface. **Owned by the heapam
    /// LOCK family (heapam.c); uninstalled — and panics — until that family
    /// lands.**
    pub fn does_multi_xact_id_conflict(
        multi: types_core::primitive::MultiXactId,
        infomask: u16,
        lockmode: LockTupleMode,
    ) -> PgResult<MultiXactConflict>
);

seam_core::seam!(
    /// `XactLockTableWait(xwait, rel, tid, XLTW_Delete)` (lmgr.c) — wait for a
    /// regular transaction `xwait` to commit or abort. Reached only from the
    /// heap-AM lock-wait path; declared with the rest of the F4 lock primitives.
    /// `Err` carries the wait `ereport(ERROR)` surface. **Owned by the heapam
    /// LOCK family (heapam.c calls lmgr); uninstalled — and panics — until that
    /// family lands.**
    pub fn xact_lock_table_wait<'mcx>(
        xwait: TransactionId,
        rel: &Relation<'mcx>,
        tid: ItemPointerData,
        oper: XLTW_Oper,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `MultiXactIdWait(multi, status, infomask, rel, tid, oper, NULL)`
    /// (heapam.c) — wait for the members of `multi` whose status conflicts with
    /// `status` to finish. `Err` carries the wait `ereport(ERROR)` surface.
    /// **Owned by the heapam LOCK family (heapam.c); uninstalled — and panics —
    /// until that family lands.**
    pub fn multi_xact_id_wait<'mcx>(
        multi: types_core::primitive::MultiXactId,
        status: MultiXactStatus,
        infomask: u16,
        rel: &Relation<'mcx>,
        tid: ItemPointerData,
        oper: XLTW_Oper,
    ) -> PgResult<()>
);

/// The result of [`heap_update`] — C's `TM_Result` return plus the two
/// by-pointer outputs `*lockmode` (the tuple-lock mode the update acquired) and
/// `*update_indexes` (which index updates the caller must perform).
#[derive(Clone, Copy, Debug)]
pub struct HeapUpdateResult {
    /// C's `TM_Result` return value.
    pub result: TM_Result,
    /// C's `*lockmode` output — always set (even on a non-`TM_Ok` outcome the
    /// caller may read it; C writes it before the `l2:` block).
    pub lockmode: LockTupleMode,
    /// C's `*update_indexes` output.
    pub update_indexes: TU_UpdateIndexes,
}

seam_core::seam!(
    /// `heap_update(relation, otid, newtup, cid, crosscheck, wait, tmfd,
    /// lockmode, update_indexes)` (heapam.c) — replace the tuple addressed by
    /// `otid` with `newtup`. Determines the modified columns (to pick a HOT vs.
    /// non-HOT update and the lock strength), stamps the old tuple's
    /// xmax/cmax/infomask and chains its `t_ctid` to the new tuple, inserts the
    /// (possibly toasted) new tuple on the same or a fresh page, emits the
    /// `XLOG_HEAP_UPDATE` / `XLOG_HEAP_HOT_UPDATE` WAL record (with old/new
    /// tuple images and prefix/suffix compression), clears the visibility-map
    /// bits, and logs the replica-identity old key for a logically-decoded rel.
    /// `newtup`'s header is stamped in place and `newtup.tuple.t_self` receives
    /// the stored TID. `crosscheck` is the optional transaction-snapshot RI
    /// snapshot (`None` == `InvalidSnapshot`). On a non-`TM_Ok` outcome `tmfd`
    /// is filled (ctid/xmax/cmax). `Err` carries the update `ereport(ERROR)`
    /// surface. **Installed by `backend-access-heap-heapam`.**
    pub fn heap_update<'mcx>(
        mcx: Mcx<'mcx>,
        relation: &Relation<'mcx>,
        otid: ItemPointerData,
        newtup: &mut FormedTuple<'mcx>,
        cid: CommandId,
        crosscheck: Option<&SnapshotData>,
        wait: bool,
        tmfd: &mut TM_FailureData,
    ) -> PgResult<HeapUpdateResult>
);

seam_core::seam!(
    /// `simple_heap_update(relation, otid, tup, update_indexes)` (heapam.c) —
    /// replace the tuple at `otid` with `tup` when no concurrent update is
    /// expected, reporting any non-`TM_Ok` outcome via `ereport(ERROR)`. Returns
    /// C's `*update_indexes` output. **Installed by
    /// `backend-access-heap-heapam`.**
    pub fn simple_heap_update<'mcx>(
        mcx: Mcx<'mcx>,
        relation: &Relation<'mcx>,
        otid: ItemPointerData,
        tup: &mut FormedTuple<'mcx>,
    ) -> PgResult<TU_UpdateIndexes>
);

seam_core::seam!(
    /// Deform the on-page heap tuple header at `(buf, offnum)` into the
    /// repo's `HeapTupleHeaderData` value — the faithful analog of C's
    /// `(HeapTupleHeader) PageGetItem(page, PageGetItemId(page, offnum))`
    /// for a normal (`LP_NORMAL`) line pointer, which casts the on-page bytes
    /// to a `HeapTupleHeader`. The caller must already hold the page's content
    /// lock and have validated that `offnum`'s item id is `LP_NORMAL`. `Err`
    /// carries page-format `ereport(ERROR)`s. **Owned by the heapam scan family
    /// (the on-page tuple-deform infrastructure); uninstalled — and panics —
    /// until that family lands.**
    pub fn heap_page_tuple_header<'mcx>(
        mcx: Mcx<'mcx>,
        buf: Buffer,
        offnum: OffsetNumber,
    ) -> PgResult<HeapTupleHeaderData<'mcx>>
);
