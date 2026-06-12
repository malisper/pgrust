//! Seam declarations for the `backend-utils-sort-storage` unit
//! (`utils/sort/tuplestore.c` and friends): the tuplestore library surface.
//!
//! `Tuplestorestate` is the opaque carrier from `types_nodes::funcapi`; only
//! the owning unit names the concrete engine state inside it. The owning unit
//! installs these from its `init_seams()` when it lands; until then a call
//! panics loudly.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `tuplestore_begin_heap(randomAccess, interXact, maxKBytes)`
    /// (tuplestore.c): create a new tuplestore; `maxKBytes` is the work_mem
    /// budget in kilobytes. The state is allocated in `mcx` (C:
    /// `tuplestore_begin_common` pallocs in `CurrentMemoryContext` and
    /// captures it as `state->context`), so creation is fallible on OOM.
    pub fn tuplestore_begin_heap<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        randomAccess: bool,
        interXact: bool,
        maxKBytes: i32,
    ) -> types_error::PgResult<mcx::PgBox<'mcx, types_nodes::Tuplestorestate>>
);

seam_core::seam!(
    /// `tuplestore_set_eflags(state, eflags)` (tuplestore.c): decree the
    /// capabilities (EXEC_FLAG_REWIND/BACKWARD/MARK) of read pointer 0.
    pub fn tuplestore_set_eflags(
        state: &mut types_nodes::Tuplestorestate,
        eflags: i32,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `tuplestore_alloc_read_pointer(state, eflags)` (tuplestore.c): create a
    /// new read pointer and return its index.
    pub fn tuplestore_alloc_read_pointer(
        state: &mut types_nodes::Tuplestorestate,
        eflags: i32,
    ) -> types_error::PgResult<i32>
);

seam_core::seam!(
    /// `tuplestore_ateof(state)` (tuplestore.c): is the active read pointer at
    /// end of the stored data? A pure field read in C — infallible.
    pub fn tuplestore_ateof(state: &types_nodes::Tuplestorestate) -> bool
);

seam_core::seam!(
    /// `tuplestore_advance(state, forward)` (tuplestore.c): move the active
    /// read pointer one step without fetching; returns `false` if it ran off
    /// the end.
    pub fn tuplestore_advance(
        state: &mut types_nodes::Tuplestorestate,
        forward: bool,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `tuplestore_gettupleslot(state, forward, copy, slot)` (tuplestore.c):
    /// fetch the next tuple into the given slot; returns `true` when a tuple
    /// was fetched.
    pub fn tuplestore_gettupleslot(
        state: &mut types_nodes::Tuplestorestate,
        forward: bool,
        copy: bool,
        slot: &mut types_nodes::TupleTableSlot,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `tuplestore_puttupleslot(state, slot)` (tuplestore.c): append a copy of
    /// the slot's tuple to the store.
    pub fn tuplestore_puttupleslot(
        state: &mut types_nodes::Tuplestorestate,
        slot: &types_nodes::TupleTableSlot,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `tuplestore_copy_read_pointer(state, srcptr, destptr)` (tuplestore.c):
    /// copy one read pointer's position onto another.
    pub fn tuplestore_copy_read_pointer(
        state: &mut types_nodes::Tuplestorestate,
        srcptr: i32,
        destptr: i32,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `tuplestore_trim(state)` (tuplestore.c): discard tuples no longer
    /// needed by any read pointer. Frees/moves memory only — infallible.
    pub fn tuplestore_trim(state: &mut types_nodes::Tuplestorestate)
);

seam_core::seam!(
    /// `tuplestore_rescan(state)` (tuplestore.c): rewind the active read
    /// pointer to the start.
    pub fn tuplestore_rescan(state: &mut types_nodes::Tuplestorestate) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `tuplestore_end(state)` (tuplestore.c): release the tuplestore's
    /// resources. Consumes the carrier (the C caller NULLs its pointer).
    /// `BufFileClose`/`pfree` paths do not `ereport(ERROR)` — infallible.
    pub fn tuplestore_end(state: mcx::PgBox<'_, types_nodes::Tuplestorestate>)
);
