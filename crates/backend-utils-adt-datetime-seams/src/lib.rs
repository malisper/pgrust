//! Seam declarations for the owner `src/backend/utils/adt/datetime.c`.
//!
//! These outward seams were removed: `backend-utils-adt-datetime` is a clean
//! single-owner leaf, so its `date2j`/`j2date`/`ValidateDate`/
//! `DetermineTimeZoneOffset`/`DetermineTimeZoneAbbrevOffset`/
//! `DecodeTimezoneAbbrevPrefix`/`ConvertTimeZoneAbbrevs` adapters (the `seam_*`
//! fns in `backend_utils_adt_datetime::seam_impls`) are now called directly by
//! their consumers (formatting, timeout) instead of through a fn-ptr seam
//! indirection. The behavior is identical; this is faithful de-indirection. The
//! crate is retained as an empty shell so workspace/dependency wiring stays
//! valid.
