//! Seam declarations for the `backend-utils-adt-datum` unit
//! (`utils/adt/datum.c`), the abstract-`Datum` helpers (copy / compare /
//! byte-image equality) used across the ADT layer.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! A `Datum` crosses these seams as the owned per-attribute value model
//! ([`types_tuple::backend_access_common_heaptuple::TupleValue`]): a by-value
//! scalar word or the by-reference on-disk bytes (varlena header included).
//! `byval` / `typlen` are the attribute's `attbyval` / `attlen`, exactly the
//! C arguments.

use types_error::PgResult;
use types_tuple::backend_access_common_heaptuple::TupleValue;

seam_core::seam!(
    /// `datum_image_eq(value1, value2, typByVal, typLen)` (datum.c): compare
    /// two non-null Datums for identical byte images (not semantic equality).
    /// For by-value types this is a raw word compare; for fixed-length
    /// by-reference types a `memcmp` of `typLen` bytes; for varlena
    /// (`typLen == -1`) it detoasts as needed and compares raw sizes then the
    /// data bytes; cstring (`typLen == -2`) compares `strlen`+1 bytes. `Err`
    /// carries the detoast `ereport(ERROR)`s and OOM.
    pub fn datum_image_eq(
        value1: &TupleValue<'_>,
        value2: &TupleValue<'_>,
        byval: bool,
        typlen: i16,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// The byte-image three-way comparison of two non-null Datums used by
    /// rowtypes.c `record_image_cmp` (the same byte-image semantics datum.c
    /// owns via `datum_image_eq`): returns the sign of `value1 <=> value2`.
    /// For by-value types it compares the raw words; for fixed-length
    /// by-reference types a `memcmp` of `typLen` bytes; for varlena
    /// (`typLen == -1`) it compares `VARDATA_ANY` over `Min(rawsize)` bytes,
    /// breaking ties by raw length, detoasting as needed. `Err` carries the
    /// detoast `ereport(ERROR)`s, the `elog(ERROR, "unexpected attlen: %d")`
    /// for any other `typLen`, and OOM.
    pub fn datum_image_cmp(
        value1: &TupleValue<'_>,
        value2: &TupleValue<'_>,
        byval: bool,
        typlen: i16,
    ) -> PgResult<i32>
);
