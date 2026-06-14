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
use types_rel::RelationData;
use types_tuple::heaptuple::FormData_pg_attribute;
use types_tuple::pg_type::FormData_pg_type;

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
