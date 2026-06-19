//! Seam declarations for the `backend-access-common-indextuple` unit
//! (`access/common/indextuple.c`): index-tuple formation.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
// The canonical unified value type (Datum-unification keystone). The seam
// signature below takes it (`ByVal`/`ByRef`) with the call frame's `'mcx`
// lifetime; the bare-word `types_datum::Datum` shim is retained only at the
// sanctioned ABI edges (none in this crate) per the datum-redesign plan.
use types_tuple::Datum;
use types_rel::Relation;
use types_tuple::heaptuple::ItemPointerData;

seam_core::seam!(
    /// `index_form_tuple(RelationGetDescr(rel), values, isnull)` with
    /// `itup->t_tid = ht_ctid` applied (indextuple.c): build the on-disk
    /// index-tuple bytes for `(values, isnull)` against the index's tuple
    /// descriptor and stamp the heap TID. The formed bytes are returned in
    /// `mcx`. `Err` carries the "index row requires N bytes" oversize ereport
    /// and OOM.
    pub fn index_form_tuple<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        values: &[Datum<'mcx>],
        isnull: &[bool],
        ht_ctid: ItemPointerData,
    ) -> types_error::PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `index_form_tuple(tupdesc, values, isnull)` (indextuple.c) — same as
    /// [`index_form_tuple`] but against a caller-supplied `tupdesc` rather than
    /// the index's `rd_att`. GiST forms internal-page downlinks against its
    /// *truncated* `nonLeafTupdesc` (`gistFormTuple`), which is not the
    /// relation's own descriptor, so it cannot use the `&Relation` variant.
    /// The formed on-disk bytes are returned in `mcx` (the caller stamps
    /// `t_tid` itself). `Err` carries the oversize ereport and OOM.
    pub fn index_form_tuple_desc<'mcx>(
        mcx: Mcx<'mcx>,
        tupdesc: &types_tuple::heaptuple::TupleDescData<'_>,
        values: &[Datum<'mcx>],
        isnull: &[bool],
    ) -> types_error::PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `index_deform_tuple(itup, itupdesc, values, isnull)` (indextuple.c):
    /// deform an index tuple into per-attribute `(value, isnull)` pairs, using
    /// the AM-supplied descriptor `itupdesc` (not the slot's, in case the
    /// datatypes differ — btree name_ops).
    ///
    /// `itup` is the on-disk index-tuple byte image (the widened `xs_itup`
    /// carrier: the 8-byte `IndexTupleData` header, the null bitmap when
    /// present, then the `MAXALIGN`-padded user data — exactly what
    /// `index_form_tuple` produces). The deformed columns are returned
    /// `mcx`-allocated; the caller (`nodeIndexonlyscan::StoreIndexTuple`)
    /// writes them into the scan slot's `tts_values`/`tts_isnull`. Fallible on
    /// detoast / `ereport(ERROR)`.
    pub fn index_deform_tuple<'mcx>(
        mcx: Mcx<'mcx>,
        itup: &[u8],
        itupdesc: &types_tuple::heaptuple::TupleDescData<'_>,
    ) -> types_error::PgResult<PgVec<'mcx, (Datum<'mcx>, bool)>>
);

seam_core::seam!(
    /// `index_truncate_tuple(RelationGetDescr(rel), source, leavenatts)`
    /// (indextuple.c) over a byte-sliced index tuple — the nbtree
    /// suffix-truncation primitive (`_bt_truncate`). nbtree carries index
    /// tuples as on-page byte slices, not `FormedIndexTuple`, so the source is
    /// the contiguous on-disk image (`IndexTupleData` header, optional null
    /// bitmap, `MAXALIGN`-padded user data, exactly as `index_form_tuple`
    /// produces). The descriptor is the index relation's own `rd_att`. Returns
    /// the truncated pivot tuple's on-disk bytes in `mcx`; `t_tid` is copied
    /// from the source. `Err` carries the oversize ereport / OOM.
    pub fn index_truncate_tuple<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        source: &[u8],
        leavenatts: i32,
    ) -> types_error::PgResult<PgVec<'mcx, u8>>
);

seam_core::seam!(
    /// `index_getattr(itup, attnum, tupdesc, &isnull)` (access/itup.h): deform a
    /// *single* (1-based) attribute out of an index tuple's on-disk byte image,
    /// walking only as far as the target column (the `nocache_index_getattr`
    /// path; for the common cached-offset / no-nulls case C's `index_getattr`
    /// macro short-circuits, but the result is identical).
    ///
    /// `itup` is the contiguous byte image exactly as `index_form_tuple`
    /// produces it (the 8-byte `IndexTupleData` header, the null bitmap when
    /// present, then the `MAXALIGN`-padded user data). Used by nbtree's
    /// `_bt_compare` / scankey value extraction, which carry index tuples as
    /// byte slices rather than `FormedIndexTuple`. Returns the canonical
    /// `(value, isnull)`; a by-ref value is copied into `mcx`. Fallible on
    /// detoast / `ereport(ERROR)`.
    pub fn nocache_index_getattr<'mcx>(
        mcx: Mcx<'mcx>,
        itup: &[u8],
        attnum: i32,
        itupdesc: &types_tuple::heaptuple::TupleDescData<'_>,
    ) -> types_error::PgResult<(Datum<'mcx>, bool)>
);
