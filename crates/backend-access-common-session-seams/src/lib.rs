//! Seam declarations for the per-session shared record-typmod registry
//! (`access/common/session.c` + the DSA/dshash substrate), as consumed by the
//! typcache's `SharedRecordTypmodRegistry*` and the shared paths of
//! `lookup_rowtype_tupdesc_internal` / `assign_record_type_typmod`.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgBox};
use types_error::PgResult;
use types_tuple::heaptuple::TupleDescData;

seam_core::seam!(
    /// Whether a `SharedRecordTypmodRegistry` is attached to the current
    /// session (`CurrentSession->shared_typmod_registry != NULL`). Pure read.
    pub fn shared_registry_attached() -> bool
);

seam_core::seam!(
    /// Look up `typmod` in the attached shared typmod table; on hit, return a
    /// copy of the shared (non-refcounted, `tdrefcount == -1`) descriptor
    /// allocated in `mcx` and release the dshash lock. `None` on miss. `Err`
    /// carries OOM from the copy.
    pub fn shared_typmod_table_find<'mcx>(
        mcx: Mcx<'mcx>,
        typmod: i32,
    ) -> PgResult<Option<PgBox<'mcx, TupleDescData<'mcx>>>>
);

seam_core::seam!(
    /// `find_or_make_matching_shared_tupledesc(tupdesc)` — the shared path of
    /// `assign_record_type_typmod`. Returns a copy of the shared descriptor
    /// (with its assigned `tdtypmod`) in `mcx`, or `None` when not attached.
    /// `Err` carries the DSA-allocation failure surface.
    pub fn find_or_make_matching_shared_tupledesc<'mcx>(
        mcx: Mcx<'mcx>,
        tupdesc: &TupleDescData<'_>,
    ) -> PgResult<Option<PgBox<'mcx, TupleDescData<'mcx>>>>
);

seam_core::seam!(
    /// `SharedRecordTypmodRegistryInit(registry, segment, area)` importing the
    /// caller's `RecordCacheArray`: the owner copies each `(typmod, tupdesc)`
    /// into the DSA area (`share_tupledesc`). The descriptors are borrowed
    /// from the typcache's cache context for the duration of the call. `Err`
    /// carries the DSA failure surface.
    pub fn shared_registry_init(
        next_record_typmod: i32,
        entries: &[(i32, &TupleDescData<'_>)],
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `SharedRecordTypmodRegistryAttach(registry)`. `Err` carries the DSA
    /// failure surface.
    pub fn shared_registry_attach() -> PgResult<()>
);

seam_core::seam!(
    /// `SharedRecordTypmodRegistryEstimate()` — `sizeof(struct)`. Pure read.
    pub fn shared_registry_estimate() -> usize
);
