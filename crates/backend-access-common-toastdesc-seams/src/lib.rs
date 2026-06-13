//! Seam declarations for the `backend-access-common-toastdesc` unit's
//! `tupdesc.c` surface (`access/common/tupdesc.c`), the TupleDesc constructors
//! consumers build template descriptors with.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use mcx::{Mcx, PgString};
use types_core::primitive::AttrNumber;
use types_core::Oid;
use types_error::PgResult;
use types_tuple::heaptuple::{TupleDesc, TupleDescData};

seam_core::seam!(
    /// `CreateTemplateTupleDesc(natts)` (access/common/tupdesc.c): allocate an
    /// empty tuple descriptor with `natts` attributes (anonymous record type,
    /// `tdtypmod = -1`, `tdrefcount = -1`). Allocated in `mcx` (C: palloc in
    /// `CurrentMemoryContext`); `Err` carries OOM.
    pub fn create_template_tuple_desc<'mcx>(
        mcx: Mcx<'mcx>,
        natts: i32,
    ) -> PgResult<TupleDescData<'mcx>>
);

seam_core::seam!(
    /// `TupleDescInitEntry(desc, attributeNumber, attributeName, oidtypeid,
    /// typmod, attdim)` (access/common/tupdesc.c): initialize the
    /// `attributeNumber`-th (1-based) attribute of `desc`, filling its
    /// type-derived fields from a `pg_type` syscache lookup. `Err` carries the
    /// `cache lookup failed for type %u` `elog(ERROR)`.
    pub fn tuple_desc_init_entry(
        desc: &mut TupleDescData<'_>,
        attribute_number: AttrNumber,
        attribute_name: &str,
        oidtypeid: Oid,
        typmod: i32,
        attdim: i32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `BuildDescFromLists(names, types, typmods, collations)` (tupdesc.c):
    /// build a `TupleDesc` from parallel column-name / type-OID / typmod /
    /// collation-OID lists (`CreateTemplateTupleDesc` + per-column
    /// `TupleDescInitEntry` / `TupleDescInitEntryCollation`). The four lists
    /// must be the same length. The descriptor is allocated in `mcx`
    /// (fallible on OOM).
    pub fn build_desc_from_lists<'mcx>(
        mcx: Mcx<'mcx>,
        names: &[PgString<'_>],
        types: &[Oid],
        typmods: &[i32],
        collations: &[Oid],
    ) -> PgResult<TupleDesc<'mcx>>
);
