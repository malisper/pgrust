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
    ) -> types_error::PgResult<mcx::PgBox<'mcx, types_nodes::Tuplestorestate<'mcx>>>
);

seam_core::seam!(
    /// `tuplestore_set_eflags(state, eflags)` (tuplestore.c): decree the
    /// capabilities (EXEC_FLAG_REWIND/BACKWARD/MARK) of read pointer 0.
    pub fn tuplestore_set_eflags(
        state: &mut types_nodes::Tuplestorestate<'_>,
        eflags: i32,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `tuplestore_alloc_read_pointer(state, eflags)` (tuplestore.c): create a
    /// new read pointer and return its index.
    pub fn tuplestore_alloc_read_pointer(
        state: &mut types_nodes::Tuplestorestate<'_>,
        eflags: i32,
    ) -> types_error::PgResult<i32>
);

seam_core::seam!(
    /// `tuplestore_ateof(state)` (tuplestore.c): is the active read pointer at
    /// end of the stored data? A pure field read in C — infallible.
    pub fn tuplestore_ateof(state: &types_nodes::Tuplestorestate<'_>) -> bool
);

seam_core::seam!(
    /// `tuplestore_advance(state, forward)` (tuplestore.c): move the active
    /// read pointer one step without fetching; returns `false` if it ran off
    /// the end.
    pub fn tuplestore_advance(
        state: &mut types_nodes::Tuplestorestate<'_>,
        forward: bool,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `tuplestore_gettupleslot(state, forward, copy, slot)` (tuplestore.c):
    /// fetch the next tuple into the given slot; returns `true` when a tuple
    /// was fetched.
    ///
    /// PROVISIONAL: `TupleTableSlot` is currently trimmed to its header bits
    /// (no payload), so this contract cannot yet move tuple data; re-sign it
    /// when the slot payload model lands.
    pub fn tuplestore_gettupleslot(
        state: &mut types_nodes::Tuplestorestate<'_>,
        forward: bool,
        copy: bool,
        slot: &mut types_nodes::TupleTableSlot,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `tuplestore_puttupleslot(state, slot)` (tuplestore.c): append a copy of
    /// the slot's tuple to the store.
    ///
    /// PROVISIONAL: see `tuplestore_gettupleslot` — re-sign when the slot
    /// payload model lands.
    pub fn tuplestore_puttupleslot(
        state: &mut types_nodes::Tuplestorestate<'_>,
        slot: &types_nodes::TupleTableSlot,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `tuplestore_copy_read_pointer(state, srcptr, destptr)` (tuplestore.c):
    /// copy one read pointer's position onto another.
    pub fn tuplestore_copy_read_pointer(
        state: &mut types_nodes::Tuplestorestate<'_>,
        srcptr: i32,
        destptr: i32,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `tuplestore_trim(state)` (tuplestore.c): discard tuples no longer
    /// needed by any read pointer. Frees/moves memory only — infallible.
    pub fn tuplestore_trim(state: &mut types_nodes::Tuplestorestate<'_>)
);

seam_core::seam!(
    /// `tuplestore_rescan(state)` (tuplestore.c): rewind the active read
    /// pointer to the start.
    pub fn tuplestore_rescan(state: &mut types_nodes::Tuplestorestate<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `tuplestore_end(state)` (tuplestore.c): release the tuplestore's
    /// resources. Consumes the carrier (the C caller NULLs its pointer).
    /// `BufFileClose`/`pfree` paths do not `ereport(ERROR)` — infallible.
    pub fn tuplestore_end(state: mcx::PgBox<'_, types_nodes::Tuplestorestate<'_>>)
);

// ===========================================================================
//  sharedtuplestore.c (`utils/sort/sharedtuplestore.c`) — the parallel-hash
//  shared tuplestore surface. Same owning unit (`backend-utils-sort-storage`).
// ===========================================================================

seam_core::seam!(
    /// `sts_estimate(participants)` (sharedtuplestore.c): size of the shared
    /// state object for the given participant count. A pure arithmetic
    /// computation — infallible.
    pub fn sts_estimate(participants: i32) -> types_core::Size
);

seam_core::seam!(
    /// `sts_initialize(sts, participants, my_participant_number, meta_data_size,
    /// flags, fileset, name)` (sharedtuplestore.c): initialize the shared
    /// tuplestore object in place (in DSM) and return this backend's accessor.
    pub fn sts_initialize(
        sts: types_execparallel::SharedTuplestoreHandle,
        participants: i32,
        my_participant_number: i32,
        meta_data_size: types_core::Size,
        flags: i32,
        fileset: types_execparallel::SharedFileSetHandle,
        name: &str,
    ) -> types_error::PgResult<types_execparallel::SharedTuplestoreAccessorHandle>
);

seam_core::seam!(
    /// `sts_attach(sts, my_participant_number, fileset)` (sharedtuplestore.c):
    /// attach to an already-initialized shared tuplestore.
    pub fn sts_attach(
        sts: types_execparallel::SharedTuplestoreHandle,
        my_participant_number: i32,
        fileset: types_execparallel::SharedFileSetHandle,
    ) -> types_error::PgResult<types_execparallel::SharedTuplestoreAccessorHandle>
);

seam_core::seam!(
    /// `sts_end_write(accessor)` (sharedtuplestore.c): finish writing this
    /// participant's partition, flushing its output buffer to the temp file.
    pub fn sts_end_write(
        accessor: types_execparallel::SharedTuplestoreAccessorHandle,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `sts_reinitialize(accessor)` (sharedtuplestore.c): prepare an
    /// already-written shared tuplestore to be read again from the start.
    pub fn sts_reinitialize(
        accessor: types_execparallel::SharedTuplestoreAccessorHandle,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `sts_begin_parallel_scan(accessor)` (sharedtuplestore.c): begin a
    /// cooperative parallel scan of every participant's partition.
    pub fn sts_begin_parallel_scan(
        accessor: types_execparallel::SharedTuplestoreAccessorHandle,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `sts_end_parallel_scan(accessor)` (sharedtuplestore.c): end the
    /// cooperative parallel scan, releasing read buffers. Infallible.
    pub fn sts_end_parallel_scan(
        accessor: types_execparallel::SharedTuplestoreAccessorHandle,
    )
);

seam_core::seam!(
    /// `sts_puttuple(accessor, meta_data, tuple)` (sharedtuplestore.c): write a
    /// tuple plus its fixed-size meta-data (the tuple's hash value, in parallel
    /// hash) to this participant's partition.
    pub fn sts_puttuple(
        accessor: types_execparallel::SharedTuplestoreAccessorHandle,
        meta_data: &[u8],
        tuple: &types_tuple::heaptuple::MinimalTupleData<'_>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `sts_parallel_scan_next(accessor, meta_data)` (sharedtuplestore.c): fetch
    /// the next tuple from the cooperative scan, copying its meta-data into
    /// `meta_data`. Returns `None` at end of the whole store (C `NULL`). The
    /// returned tuple is allocated in `mcx`.
    pub fn sts_parallel_scan_next<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        accessor: types_execparallel::SharedTuplestoreAccessorHandle,
        meta_data: &mut [u8],
    ) -> types_error::PgResult<Option<types_tuple::heaptuple::MinimalTupleData<'mcx>>>
);
