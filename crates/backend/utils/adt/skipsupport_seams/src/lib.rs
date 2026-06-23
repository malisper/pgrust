//! Seam declaration for `PrepareSkipSupportFromOpclass`
//! (`utils/adt/skipsupport.c`, PostgreSQL 18.3).
//!
//! `PrepareSkipSupportFromOpclass` is the entry point B-Tree skip scan uses to
//! obtain a [`SkipSupportData`](types_sortsupport::SkipSupportData) for an
//! operator class (opfamily + opcintype). The B-Tree skip-scan preprocessing
//! code (`_bt_preprocess_array_keys` in `nbtutils.c`) calls it; the
//! implementation lives in the owner crate `backend-utils-adt-skipsupport`,
//! which installs this seam from its `init_seams()`.
//!
//! This is an **OUTWARD** seam: it is owned by the skipsupport substrate and
//! *called* by the (unported) nbtree skip-scan consumer. Until the owner's
//! `init_seams()` runs it panics loudly ("seam not installed").

#![allow(non_snake_case)]

use types_core::Oid;
use types_error::PgResult;
use types_sortsupport::SkipSupportData;

seam_core::seam!(
    /// `PrepareSkipSupportFromOpclass(opfamily, opcintype, reverse)`
    /// (`utils/adt/skipsupport.c`): fill in a `SkipSupport` for the given
    /// operator class.
    ///
    /// `Ok(Some(SkipSupportData))` is C's success return (the freshly palloc'd
    /// struct, filled by the opclass `BTSKIPSUPPORT_PROC` and, for the
    /// reverse/`DESC` case, with `low_elem`/`high_elem` and
    /// `decrement`/`increment` swapped). `Ok(None)` is C's `return NULL` when
    /// the operator class has no skip support function. `Err` carries any
    /// `ereport(ERROR)` raised by the underlying syscache / opclass machinery
    /// (C's `longjmp`).
    pub fn prepare_skip_support_from_opclass(
        opfamily: Oid,
        opcintype: Oid,
        reverse: bool
    ) -> PgResult<Option<SkipSupportData>>
);
