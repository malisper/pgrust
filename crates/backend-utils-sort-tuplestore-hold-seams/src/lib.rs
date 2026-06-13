//! Hold-cursor tuplestore seams for portalmem (`utils/sort/tuplestore.c`).
//!
//! A held cursor's `Tuplestorestate *` is created in the portal's `holdContext`
//! and stored in the long-lived portal record, so it must outlive any single
//! `Mcx<'mcx>` borrow — portalmem therefore threads it as an opaque
//! [`ExternHandle`] rather than the lifetime-bound `types_nodes::Tuplestorestate`
//! the in-band executor seams use. Reconciling the two shapes is DESIGN_DEBT for
//! when held-cursor persistence (portalcmds `PersistHoldablePortal`) lands.

use types_portal::ExternHandle;

seam_core::seam!(
    /// `tuplestore_begin_heap(randomAccess, false, work_mem)` allocated in the
    /// portal's already-switched-to `holdContext`. Returns the store handle.
    pub fn tuplestore_begin_heap(random_access: bool) -> ExternHandle
);

seam_core::seam!(
    /// `tuplestore_end(state)` — frees the store and its temp files.
    pub fn tuplestore_end(state: ExternHandle)
);
