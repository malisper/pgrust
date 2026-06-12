//! Seam declarations for the `backend-access-tableam` unit
//! (`access/table/tableam.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Open relations cross as their `Oid` (the
//! relcache owns the entry).

seam_core::seam!(
    /// `table_slot_callbacks(relation)` (tableam.c): which set of slot
    /// callbacks (`&TTSOps*` singleton, carried as the [`TupleSlotKind`]
    /// token) is needed to store full tuples of this relation. `Err` carries
    /// relcache OID-resolution failure.
    ///
    /// [`TupleSlotKind`]: types_nodes::TupleSlotKind
    pub fn table_slot_callbacks(
        relation: types_core::primitive::Oid,
    ) -> types_error::PgResult<types_nodes::TupleSlotKind>
);
