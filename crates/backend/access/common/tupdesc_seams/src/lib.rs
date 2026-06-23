//! Seam declarations for the `backend-access-common-tupdesc` unit
//! (`access/common/tupdesc.c`): the row-type structural hash/equality and
//! tuple-descriptor copy algorithms the typcache's record cache needs, plus
//! the flat descriptor copy the PREPARE/EXECUTE driver uses.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgBox};
use types_core::primitive::AttrNumber;
use types_core::Oid;
use types_error::PgResult;
use types_tuple::heaptuple::{FormData_pg_attribute, TupleDescData};

seam_core::seam!(
    /// `CreateTupleDesc(natts, attrs)` (tupdesc.c): allocate a new `TupleDesc`
    /// in `mcx` by copying the given `Form_pg_attribute` array, re-deriving each
    /// compact attribute. The tuple type id is left anonymous (`RECORDOID`,
    /// typmod `-1`) for the caller to overwrite; the result is non-refcounted
    /// (`tdrefcount = -1`). Used by the blessed-record DSA read-back path to
    /// reconstruct an owned descriptor from the flat DSA-resident attribute
    /// array. `Err` carries OOM / an invalid `attalign`.
    pub fn create_tuple_desc<'mcx>(
        mcx: Mcx<'mcx>,
        attrs: &[FormData_pg_attribute],
    ) -> PgResult<TupleDescData<'mcx>>
);

seam_core::seam!(
    /// `hashRowType(tupdesc)` (tupdesc.c): the structural row-type hash used
    /// as the record-cache key. Pure computation over the descriptor; the
    /// owner's body cannot allocate, so this is infallible.
    pub fn hash_row_type(tupdesc: &TupleDescData<'_>) -> u32
);

seam_core::seam!(
    /// `equalRowTypes(tupdesc1, tupdesc2)` (tupdesc.c): structural equality of
    /// two row types (the record-cache match function). Pure computation.
    pub fn equal_row_types(a: &TupleDescData<'_>, b: &TupleDescData<'_>) -> bool
);

seam_core::seam!(
    /// `CreateTupleDescCopy(tupdesc)` (tupdesc.c): copy WITHOUT constraints or
    /// defaults, resetting the per-attribute constraint/default/identity/
    /// generated flags and re-deriving the compact attrs; the result is a
    /// non-refcounted descriptor allocated in `mcx`. `Err` carries OOM.
    pub fn create_tupledesc_copy<'mcx>(
        mcx: Mcx<'mcx>,
        tupdesc: &TupleDescData<'_>,
    ) -> PgResult<PgBox<'mcx, TupleDescData<'mcx>>>
);

seam_core::seam!(
    /// `CreateTupleDescCopy(tupdesc)` (tupdesc.c) — a flat copy of the
    /// descriptor (dropping constraints/defaults) into `mcx`, returned by
    /// value for the PREPARE/EXECUTE result-descriptor accessor. Allocates.
    pub fn create_tuple_desc_copy<'mcx>(
        mcx: Mcx<'mcx>,
        tupdesc: &TupleDescData<'mcx>,
    ) -> PgResult<TupleDescData<'mcx>>
);

seam_core::seam!(
    /// `TupleDescInitEntryCollation(desc, attributeNumber, collationid)`
    /// (tupdesc.c): assign a nondefault collation to the `attributeNumber`-th
    /// (1-based) already-initialized attribute of `desc`. Fallible on an
    /// out-of-range attribute number.
    pub fn tuple_desc_init_entry_collation(
        desc: &mut TupleDescData<'_>,
        attribute_number: AttrNumber,
        collationid: Oid,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `TupleDescInitEntry(desc, attributeNumber, attributeName, oidtypeid,
    /// typmod, attdim)` (tupdesc.c): initialize the `attributeNumber`-th
    /// (1-based) attribute of `desc` from the catalog type `oidtypeid`, with
    /// the given name (or keep the existing name when `None`), typmod and array
    /// dimension. `CallStmtResultDesc` (functioncmds.c) uses this to re-type each
    /// output column from `exprType(outarg)`. Fallible on the type-cache lookup
    /// `ereport(ERROR)`.
    pub fn tuple_desc_init_entry(
        desc: &mut TupleDescData<'_>,
        attribute_number: AttrNumber,
        attribute_name: Option<&str>,
        oidtypeid: Oid,
        typmod: i32,
        attdim: i32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `TupleDescCopyEntry(dst, dstAttno, src, srcAttno)` (tupdesc.c): copy the
    /// `srcAttno`-th (1-based) attribute of `src` into the `dstAttno`-th slot of
    /// `dst` (dropping constraint/default flags). Fallible on an out-of-range
    /// attribute number.
    pub fn tuple_desc_copy_entry(
        dst: &mut TupleDescData<'_>,
        dst_attno: AttrNumber,
        src: &TupleDescData<'_>,
        src_attno: AttrNumber,
    ) -> PgResult<()>
);
