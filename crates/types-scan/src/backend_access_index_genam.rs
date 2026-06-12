//! The row shape crossing the `backend-access-index-genam` systable-scan
//! seam.

use types_datum::datum::Datum;
use types_tuple::heaptuple::ItemPointerData;

/// One catalog row returned by a systable scan: the heap TID (`tup->t_self`)
/// plus the `heap_deform_tuple` projection of the whole row — one datum and
/// null flag per attribute, in catalog attribute order (index
/// `Anum_* - 1`). The calling unit interprets the columns (the C
/// `GETSTRUCT` cast); the scan owner only deforms.
#[derive(Clone, Copy, Debug)]
pub struct SysScanRow<'a> {
    /// `tup->t_self` — the row's heap location, for delete/update legs.
    pub tid: ItemPointerData,
    /// Deformed column values, length `Natts_*`.
    pub values: &'a [Datum],
    /// Per-column null flags, same length as `values`.
    pub isnull: &'a [bool],
}
