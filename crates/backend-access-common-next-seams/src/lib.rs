//! Seam declarations for the `backend-access-common-next` unit
//! (`access/common/attmap.c` + `access/common/tupconvert.c`; the unit also
//! covers `syncscan.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `build_attrmap_by_name_if_req(indesc, outdesc, missing_ok)`
    /// (attmap.c): map input-descriptor columns to output-descriptor columns
    /// by name, returning `None` when the map would be the identity (no
    /// conversion needed). `Err` carries the C `ereport(ERROR)`s ("could not
    /// convert row type") and OOM.
    pub fn build_attrmap_by_name_if_req<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        indesc: &types_tuple::heaptuple::TupleDescData<'_>,
        outdesc: &types_tuple::heaptuple::TupleDescData<'_>,
        missing_ok: bool,
    ) -> types_error::PgResult<Option<mcx::PgBox<'mcx, types_tuple::attmap::AttrMap<'mcx>>>>
);

seam_core::seam!(
    /// `convert_tuples_by_name(indesc, outdesc)` (tupconvert.c): set up for
    /// tuple conversion between rowtypes, matching columns by name. `None`
    /// means no conversion is needed. The map (and its descriptor copies) is
    /// allocated in `mcx`.
    pub fn convert_tuples_by_name<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        indesc: &types_tuple::heaptuple::TupleDescData<'_>,
        outdesc: &types_tuple::heaptuple::TupleDescData<'_>,
    ) -> types_error::PgResult<
        Option<mcx::PgBox<'mcx, types_tuple::tupconvert::TupleConversionMap<'mcx>>>,
    >
);

seam_core::seam!(
    /// `convert_tuples_by_name_attrmap(indesc, outdesc, attrMap)`
    /// (tupconvert.c): as `convert_tuples_by_name`, but with the
    /// caller-provided (known non-identity) attribute map; always builds a
    /// map. The owned descriptors move into the map.
    pub fn convert_tuples_by_name_attrmap<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        indesc: types_tuple::heaptuple::TupleDesc<'mcx>,
        outdesc: types_tuple::heaptuple::TupleDesc<'mcx>,
        attrMap: mcx::PgBox<'mcx, types_tuple::attmap::AttrMap<'mcx>>,
    ) -> types_error::PgResult<mcx::PgBox<'mcx, types_tuple::tupconvert::TupleConversionMap<'mcx>>>
);

seam_core::seam!(
    /// `execute_attr_map_cols(attrMap, in_cols)` (tupconvert.c): remap a
    /// bitmapset of (offset-shifted) attribute numbers through the map,
    /// allocating the result in `mcx`. A `None` input yields `None`.
    pub fn execute_attr_map_cols<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        attrMap: &types_tuple::attmap::AttrMap<'_>,
        in_cols: Option<&types_nodes::Bitmapset<'_>>,
    ) -> types_error::PgResult<Option<mcx::PgBox<'mcx, types_nodes::Bitmapset<'mcx>>>>
);
