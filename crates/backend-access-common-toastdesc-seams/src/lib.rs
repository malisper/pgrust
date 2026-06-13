//! Seam declarations for the `backend-access-common-toastdesc` unit's
//! `tupdesc.c` surface (`access/common/tupdesc.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use mcx::{Mcx, PgString};
use types_core::primitive::Oid;
use types_error::PgResult;
use types_tuple::heaptuple::TupleDesc;

seam_core::seam!(
    /// `BuildDescFromLists(names, types, typmods, collations)` (tupdesc.c):
    /// build a `TupleDesc` from parallel column-name / type-OID / typmod /
    /// collation-OID lists (`CreateTemplateTupleDesc` + per-column
    /// `TupleDescInitEntry` / `TupleDescInitEntryCollation`). The four lists
    /// must be the same length. The descriptor is allocated in `mcx`
    /// (fallible on OOM).
    pub fn build_desc_from_lists<'mcx>(
        mcx: Mcx<'mcx>,
        names: &[PgString<'_>],
        types: &[Oid],
        typmods: &[i32],
        collations: &[Oid],
    ) -> PgResult<TupleDesc<'mcx>>
);

seam_core::seam!(
    /// `toast_get_valid_index(toastoid, lockmode)` (toast_internals.c).
    pub fn toast_get_valid_index(
        toastoid: Oid,
        lockmode: types_storage::lock::LOCKMODE,
    ) -> PgResult<Oid>
);
