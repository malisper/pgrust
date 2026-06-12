//! Seam declarations for the `backend-access-table-toast-helper` unit
//! (`access/table/toast_helper.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. The pass context is the transparent
//! [`types_tuple::toast_helper::ToastTupleContext`], a stack value owned by
//! the caller and mutated in place — exactly as C threads `&ttc`.
//!
//! C's pass functions are void/int with no `ereport` of their own, but they
//! detoast, compress, and store datums (`detoast_attr`, `toast_compress_datum`,
//! `toast_save_datum`, `toast_delete_datum`) and read the relation's
//! descriptor through the by-OID relcache seam, so every one carries that
//! callee error surface (`PgResult`).

seam_core::seam!(
    /// `toast_tuple_init(ttc)` (toast_helper.c): prepare to TOAST a tuple —
    /// initialize `ttc_flags` and `ttc_attr`, detoast any pre-existing
    /// external values, and mark old external values that need deleting.
    pub fn toast_tuple_init(
        ttc: &mut types_tuple::toast_helper::ToastTupleContext<'_>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `toast_tuple_find_biggest_attribute(ttc, for_compression, check_main)`
    /// (toast_helper.c): index of the biggest suitable varlena column, or -1.
    pub fn toast_tuple_find_biggest_attribute(
        ttc: &types_tuple::toast_helper::ToastTupleContext<'_>,
        for_compression: bool,
        check_main: bool,
    ) -> types_error::PgResult<i32>
);

seam_core::seam!(
    /// `toast_tuple_try_compression(ttc, attribute)` (toast_helper.c): try
    /// compressing one attribute in place; marks it `TOASTCOL_INCOMPRESSIBLE`
    /// on failure.
    pub fn toast_tuple_try_compression(
        ttc: &mut types_tuple::toast_helper::ToastTupleContext<'_>,
        attribute: i32,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `toast_tuple_externalize(ttc, attribute, options)` (toast_helper.c):
    /// move one attribute to external storage (`toast_save_datum`).
    pub fn toast_tuple_externalize(
        ttc: &mut types_tuple::toast_helper::ToastTupleContext<'_>,
        attribute: i32,
        options: i32,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `toast_tuple_cleanup(ttc)` (toast_helper.c): free temp values and
    /// delete no-longer-needed old external values (`toast_delete_datum`).
    pub fn toast_tuple_cleanup(
        ttc: &mut types_tuple::toast_helper::ToastTupleContext<'_>,
    ) -> types_error::PgResult<()>
);
