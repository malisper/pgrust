//! Seam declarations for the `backend-optimizer-util-plancat` unit
//! (`optimizer/util/plancat.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly. Open relations cross as their `Oid`.

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
