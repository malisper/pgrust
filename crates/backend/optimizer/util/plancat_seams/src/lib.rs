//! Seam declarations for the `backend-optimizer-util-plancat` unit
//! (`optimizer/util/plancat.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly. Open relations cross as their `Oid`.

seam_core::seam!(
    /// `estimate_rel_size(rel, attr_widths=NULL, &pages, &tuples, &allvisfrac)`
    /// (plancat.c): estimate the number of pages, live tuples, and
    /// all-visible fraction currently present in a relation. Returns
    /// `(relpages, reltuples, allvisfrac)`. The hash index build only consumes
    /// `reltuples`. `Err` carries the `RelationGetNumberOfBlocks` / syscache
    /// `ereport(ERROR)`s.
    pub fn estimate_rel_size<'mcx>(
        rel: &rel::Relation<'mcx>,
    ) -> types_error::PgResult<(types_core::primitive::BlockNumber, f64, f64)>
);

seam_core::seam!(
    /// `get_rel_data_width(rel, attr_widths)` (plancat.c): estimate the
    /// average width of (the data part of) the relation's tuples. When
    /// `attr_widths` is supplied (the C non-NULL pointer to a
    /// 0..RelationGetNumberOfAttributes cache array, attribute widths at
    /// `attr_widths[attno]`), cached estimates are used and filled in.
    /// `Err` carries `get_attavgwidth`/syscache `ereport(ERROR)`s.
    pub fn get_rel_data_width<'a>(
        rel: types_core::primitive::Oid,
        attr_widths: Option<&'a mut [i32]>,
    ) -> types_error::PgResult<i32>
);
