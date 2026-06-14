//! Seam declarations for the `backend-access-heap-heapam` unit
//! (`access/heap/heapam.c`), the heap access method.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
use types_core::primitive::Oid;
use types_datum::datum::Datum;
use types_error::PgResult;
use types_rel::{Relation, RelationData};
use types_tuple::heaptuple::FormData_pg_attribute;
use types_tuple::pg_type::FormData_pg_type;
use types_core::primitive::{OffsetNumber, TransactionId};
use types_nbtree::TmIndexDeleteOp;
use types_snapshot::SnapshotData;
use types_storage::Buffer;
use types_tuple::heaptuple::{HeapTupleData, HeapTupleHeaderData, ItemPointerData};

seam_core::seam!(
    /// The bootstrap-mode tuple-insert sequence, batched at the heap owner
    /// (bootstrap.c `InsertOneTuple`): `tupDesc = CreateTupleDesc(numattr,
    /// attrtypes); tuple = heap_form_tuple(tupDesc, values, Nulls);
    /// simple_heap_insert(boot_reldesc, tuple); heap_freetuple(tuple)`. The
    /// provider forms the tuple from the borrowed relation's descriptor and
    /// the supplied column data, then `simple_heap_insert`s it.
    /// `attrtypes`/`values`/`nulls` are `numattr` long. `Err` carries the
    /// heap-insert error surface (and OOM).
    pub fn insert_one_tuple<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &RelationData<'mcx>,
        attrtypes: &[FormData_pg_attribute],
        values: &[Datum],
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
