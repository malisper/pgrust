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
