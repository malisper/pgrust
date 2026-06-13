//! Seam declarations for the `backend-access-common-indextuple` unit
//! (`access/common/indextuple.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `index_deform_tuple(itup, itupdesc, values, isnull)` (indextuple.c):
    /// deform an index tuple into the scan slot's per-attribute value/isnull
    /// arrays, using the AM-supplied descriptor `itupdesc` (not the slot's, in
    /// case the datatypes differ — btree name_ops). The owned model targets
    /// the slot by pool id; the values land in the slot's payload. Fallible on
    /// detoast / `ereport(ERROR)`.
    pub fn index_deform_tuple<'mcx>(
        estate: &mut types_nodes::EStateData<'mcx>,
        slot: types_nodes::SlotId,
        itup: &types_tuple::heaptuple::IndexTupleData,
        itupdesc: &types_tuple::heaptuple::TupleDescData<'_>,
    ) -> types_error::PgResult<()>
);
