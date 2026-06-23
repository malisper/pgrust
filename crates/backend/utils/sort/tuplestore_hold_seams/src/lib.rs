//! Hold-cursor tuplestore seams for portalmem (`utils/sort/tuplestore.c`).
//!
//! A held cursor's `Tuplestorestate *` is created in the portal's `holdContext`
//! and stored in the long-lived portal record (`portal->holdStore`), so it must
//! outlive any single `Mcx<'mcx>` borrow — it carries the `'static` lifetime
//! `portal::PortalData::holdStore` declares. The store is the real owned
//! `nodes::Tuplestorestate`; releasing it (C `tuplestore_end`) is dropping
//! the value (RAII), which the owner's `Drop` performs. The owned engine state
//! is itself self-owned (it carries its own working-memory arena), so the
//! `'static` carrier the storage unit builds (`tuplestore_begin_heap_hold` via
//! `Tuplestorestate::begin_static`) borrows nothing from a caller's `'mcx`.

use types_error::PgResult;
use nodes::Tuplestorestate;

seam_core::seam!(
    /// `tuplestore_begin_heap(randomAccess, false, work_mem)` allocated in the
    /// portal's `holdContext`. Returns the owned `'static` store; `Err` on the
    /// C `palloc` failure path.
    pub fn tuplestore_begin_heap(random_access: bool) -> PgResult<Tuplestorestate<'static>>
);
