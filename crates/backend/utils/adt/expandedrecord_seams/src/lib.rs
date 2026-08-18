//! Seams for expanded-record access from execexpr (a direct dep would cycle
//! via adt_domains), installed by adt_expandedrecord.

use datum::Datum;
use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// ExecEvalFieldSelect's expanded-record leg: `value` is a live
    /// external-expanded record datum; bounds/dropped/type checks then
    /// expanded_record_get_field. Returns (value, isnull).
    pub fn expanded_record_field_select(
        value: Datum,
        fieldnum: i32,
        resulttype: Oid,
    ) -> PgResult<(Datum, bool)>
);
