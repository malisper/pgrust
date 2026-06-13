//! Hold-cursor tuplestore seams for portalmem (`utils/sort/tuplestore.c`).
//!
//! A held cursor's `Tuplestorestate *` is created in the portal's `holdContext`
//! and stored in the long-lived portal record (`portal->holdStore`), so it must
//! outlive any single `Mcx<'mcx>` borrow — it carries the `'static` lifetime
//! `types_portal::PortalData::holdStore` declares. The store is the real owned
//! `types_nodes::Tuplestorestate`; releasing it (C `tuplestore_end`) is dropping
//! the value (RAII), which the owner's `Drop` performs. Reconciling the
//! `'static` hold store with the storage unit's `'mcx`-bound begin is DESIGN_DEBT
//! until held-cursor persistence lands.

use types_nodes::Tuplestorestate;

seam_core::seam!(
    /// `tuplestore_begin_heap(randomAccess, false, work_mem)` allocated in the
    /// portal's already-switched-to `holdContext`. Returns the owned store.
    pub fn tuplestore_begin_heap(random_access: bool) -> Tuplestorestate<'static>
);
