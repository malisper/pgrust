//! Seam declarations the `backend-access-gin-gininsert` unit (gininsert.c F1)
//! needs for the **fast-update pending-list** path it does not own.
//!
//! `gininsert.c`'s `gininsert()` dispatches to the fast-update pending list
//! (`ginfast.c`) when the index's `fastupdate` reloption is on. `ginfast.c` is
//! a separate GIN family (F3) with no owner crate yet, and the `fastupdate`
//! reloption itself lives in the GIN-specific `GinOptions` bytea that the
//! trimmed relcache does not carry — so both the decision and the collect+insert
//! cross seams here. Each is installed by the future `ginfast` owner from its
//! own `init_seams()`; until then a call loud-panics (mirror-PG-and-panic).

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

use ::types_core::primitive::Oid;
use ::types_error::PgResult;
use ::types_tuple::heaptuple::Datum;
use ::types_tuple::heaptuple::ItemPointerData;

extern crate alloc;
use alloc::vec::Vec;

seam_core::seam!(
    /// `GinGetUseFastUpdate(index)` (gin_private.h:34): the index's `fastupdate`
    /// reloption (`GIN_DEFAULT_USE_FASTUPDATE = true` when `rd_options` is NULL).
    /// Read off the relation's `rd_options` (the GIN-specific `GinOptions` bytea),
    /// exactly as the C macro reads `relation->rd_options`. Resolved by the owner
    /// (`ginutil`, which owns the `GinOptions` byte layout). `Err` carries any
    /// `ereport(ERROR)`.
    pub fn gin_get_use_fast_update<'mcx>(
        index: &rel::Relation<'mcx>,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `ginHeapTupleFastCollect` + `ginHeapTupleFastInsert` (ginfast.c): collect
    /// one heap tuple's index entries into the fast-update pending list and write
    /// the pending pages. Installed by the `ginfast` owner. The collect + page
    /// I/O need the live index relation and an allocation context, so the seam
    /// threads `mcx` and `&Relation` (the `index` Oid is kept for callers that
    /// only have it). `Err` carries any `ereport(ERROR)`.
    pub fn gin_fast_insert<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        index: &rel::Relation<'mcx>,
        index_oid: Oid,
        values: Vec<Datum<'mcx>>,
        isnull: Vec<bool>,
        ht_ctid: ItemPointerData,
    ) -> PgResult<()>
);
