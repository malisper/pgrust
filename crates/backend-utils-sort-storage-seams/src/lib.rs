//! Seam declarations for the `backend-utils-sort-storage` unit
//! (`utils/sort/tuplestore.c` and friends): the tuplestore library surface.
//!
//! `Tuplestorestate` is the opaque carrier from `types_nodes::funcapi`; only
//! the owning unit names the concrete engine state inside it. The owning unit
//! installs these from its `init_seams()` when it lands; until then a call
//! panics loudly.

#![allow(non_snake_case)]

extern crate alloc;

// ===========================================================================
// logtape.c structures — the real owned `LogicalTapeSet` / `LogicalTape`.
//
// `LogicalTapeSet *` / `LogicalTape *` are opaque typedefs in C
// (`logtape.h`); the two working structs are private to `logtape.c`. In this
// repo they are the shared vocabulary the hash-agg spill consumer (nodeAgg)
// holds BY VALUE: the set is an owned `PgBox<LogicalTapeSet<'mcx>>`, and a
// `LogicalTape *` is a `usize` slot index into the set's `tapes` vector (the
// faithful rendering of C's pointer into the set-owned tape array). The
// concrete bodies (block I/O, the free-block min-heap) live in the owner
// crate, which deps this crate and operates on these structs through the
// value-typed seams below — no side-table registry.
// ===========================================================================

/// Per-tape state. Port of `struct LogicalTape` (private to `logtape.c`). The
/// `lt->tapeSet` back-pointer is implicit: a tape lives in its set's `tapes`
/// vector (addressed by its slot index, the owned-model `LogicalTape *`).
#[derive(Debug)]
pub struct LogicalTape<'mcx> {
    /// T while in write phase.
    pub writing: bool,
    /// T if blocks should not be freed when read.
    pub frozen: bool,
    /// does buffer need to be written?
    pub dirty: bool,

    /// block number of the first block of the tape, or -1.
    pub firstBlockNumber: i64,
    /// "current" block number (valid when writing or reading a frozen tape).
    pub curBlockNumber: i64,
    /// next block of the tape, or -1.
    pub nextBlockNumber: i64,
    /// offset applied during reads for leader tapesets.
    pub offsetBlockNumber: i64,

    /// physical buffer (`char *buffer`), empty until lazily allocated.
    pub buffer: mcx::PgVec<'mcx, u8>,
    /// allocated/intended size of the buffer.
    pub buffer_size: usize,
    /// highest useful, safe `buffer_size`.
    pub max_size: usize,
    /// next read/write position in buffer.
    pub pos: usize,
    /// total # of valid bytes in buffer.
    pub nbytes: usize,
    /// has the read/write buffer been allocated yet? (`lt->buffer == NULL`).
    pub buffer_allocated: bool,

    /// preallocated block numbers, sorted descending; consumed from the end.
    pub prealloc: mcx::PgVec<'mcx, i64>,
    /// has the prealloc list been allocated yet? (`lt->prealloc != NULL`).
    pub prealloc_allocated: bool,
    /// number of valid elements in the list.
    pub nprealloc: i32,
    /// number of elements the list can hold.
    pub prealloc_size: i32,
}

/// The mutable state of a set of related "logical tapes" sharing space in a
/// single underlying file. Port of `struct LogicalTapeSet` (private to
/// `logtape.c`), plus the tapes created in it (`tapes`) so the whole set is one
/// owned bundle. nodeAgg holds this by value (`PgBox<LogicalTapeSet<'mcx>>`),
/// retiring the former side-table registry.
#[derive(Debug)]
pub struct LogicalTapeSet<'mcx> {
    /// underlying file for whole tape set (`BufFile *pfile`), or `None`.
    pub pfile: Option<mcx::PgBox<'mcx, types_nodes::nodehashjoin::BufFile>>,
    /// shared fileset (parallel sort), or `None`.
    pub fileset: Option<alloc::boxed::Box<types_storage::fileset::SharedFileSet>>,
    /// worker # if shared, -1 for leader/serial.
    pub worker: i32,

    /// # of blocks allocated.
    pub nBlocksAllocated: i64,
    /// # of blocks used in underlying file.
    pub nBlocksWritten: i64,
    /// # of "hole" blocks left.
    pub nHoleBlocks: i64,

    /// are we remembering free blocks?
    pub forgetFreeSpace: bool,
    /// minheap of free blocks (the first `nFreeBlocks` slots are the heap).
    pub freeBlocks: mcx::PgVec<'mcx, i64>,
    /// # of currently free blocks.
    pub nFreeBlocks: i64,
    /// current allocated length of `freeBlocks`.
    pub freeBlocksLen: usize,
    /// preallocate write blocks?
    pub enable_prealloc: bool,

    /// tapes created in this set, addressed by the tape handle's slot. A
    /// `None` slot is a closed tape.
    pub tapes: alloc::vec::Vec<Option<LogicalTape<'mcx>>>,
}

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
    /// Re-signed off the header-only provisional shape once the MinimalTuple
    /// payload-bearing carrier + the EState slot pool landed: the slot is the
    /// pool [`SlotId`], resolved against `estate` (the payload-bearing
    /// `&mut SlotData` lives in `estate.es_tupleTable`). The owner stores the
    /// fetched MinimalTuple via the `exec_store_minimal_tuple` /
    /// `exec_clear_tuple_by_id` execTuples seams (which build the slot's carrier
    /// in `estate.es_query_cxt`).
    pub fn tuplestore_gettupleslot<'mcx>(
        state: &mut types_nodes::Tuplestorestate<'mcx>,
        forward: bool,
        copy: bool,
        slot: types_nodes::SlotId,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `tuplestore_gettupleslot(state, forward, copy, slot)` (tuplestore.c)
    /// over a *standalone* slot — the form `pquery.c`'s `RunFromStore` needs.
    ///
    /// `RunFromStore` replays a held cursor's tuplestore in the caller's
    /// memory context using a `MakeSingleTupleTableSlot` slot; it has no
    /// `EState`/`SlotId` pool, so it cannot use the `SlotId`+`&mut EStateData`
    /// form above. This takes the payload-bearing [`SlotData`] directly.
    /// Returns `false` when the store is exhausted in the requested direction;
    /// reading a tuple can `ereport(ERROR)`. `mcx` is the context the fetched
    /// `MinimalTuple` is formed in (the caller's hold context).
    pub fn tuplestore_gettupleslot_standalone<'s, 'm>(
        mcx: mcx::Mcx<'m>,
        state: &mut types_nodes::Tuplestorestate<'s>,
        forward: bool,
        copy: bool,
        slot: &mut types_nodes::tuptable::SlotData<'m>,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `tuplestore_puttupleslot(state, slot)` (tuplestore.c): append a copy of
    /// the slot's tuple to the store. Re-signed off the header-only provisional
    /// shape once the MinimalTuple payload-bearing carrier + EState slot pool
    /// landed: the slot is the pool [`SlotId`] resolved against `estate`; the
    /// owner forms the MinimalTuple from the live slot via the
    /// `exec_copy_slot_minimal_tuple` execTuples seam, then copies it into the
    /// store's own context.
    pub fn tuplestore_puttupleslot<'mcx>(
        state: &mut types_nodes::Tuplestorestate<'mcx>,
        slot: types_nodes::SlotId,
        estate: &mut types_nodes::EStateData<'mcx>,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `tuplestore_putvalues(state, tdesc, values, nulls)` (tuplestore.c):
    /// form a `MinimalTuple` from the `(values, nulls)` arrays under `tdesc`
    /// and append it to the store. The tuple is built/copied into the store's
    /// own context (fallible on OOM). `values` and `nulls` are parallel to
    /// `tdesc`'s attributes.
    ///
    /// `values` carries the canonical
    /// [`types_tuple::backend_access_common_heaptuple::Datum`] (the ByVal/ByRef
    /// enum), matching the by-value/by-reference column values the owner forms a
    /// `MinimalTuple` from.
    pub fn tuplestore_putvalues<'mcx>(
        state: &mut types_nodes::Tuplestorestate<'_>,
        tdesc: &types_tuple::heaptuple::TupleDescData<'_>,
        values: &[types_tuple::backend_access_common_heaptuple::Datum<'mcx>],
        nulls: &[bool],
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
    /// `tuplestore_select_read_pointer(state, ptr)` (tuplestore.c): make read
    /// pointer `ptr` the active one (flushing/repositioning as needed). Can
    /// touch the temp file on the seek path, so it is fallible.
    pub fn tuplestore_select_read_pointer(
        state: &mut types_nodes::Tuplestorestate<'_>,
        ptr: i32,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `tuplestore_rescan(state)` (tuplestore.c): rewind the active read
    /// pointer to the start.
    pub fn tuplestore_rescan(state: &mut types_nodes::Tuplestorestate<'_>) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `tuplestore_clear(state)` (tuplestore.c): delete all the stored tuples
    /// and reset every read pointer to the start, but keep the tuplestore
    /// itself allocated and re-usable. Frees/truncates the backing buffer
    /// (BufFile close paths) — infallible.
    pub fn tuplestore_clear(state: &mut types_nodes::Tuplestorestate<'_>)
);

seam_core::seam!(
    /// `tuplestore_end(state)` (tuplestore.c): release the tuplestore's
    /// resources. Consumes the carrier (the C caller NULLs its pointer).
    /// `BufFileClose`/`pfree` paths do not `ereport(ERROR)` — infallible.
    pub fn tuplestore_end(state: mcx::PgBox<'_, types_nodes::Tuplestorestate<'_>>)
);

// ===========================================================================
// SharedTuplestore (utils/sort/sharedtuplestore.c) — the parallel hash join's
// per-batch shared tuplestores. Two opacity models coexist for the same C
// `SharedTuplestoreAccessor *`: nodeHashjoin threads it as the
// `types_nodes::nodehashjoin::SharedTuplestoreAccessor` struct (the `&mut`
// seams below), while nodeHash threads it as the
// `types_execparallel::SharedTuplestoreAccessorHandle` token (the `*_handle`
// seams below). The owning unit installs both from its `init_seams()` when it
// lands; until then a call panics loudly.
// ===========================================================================

seam_core::seam!(
    /// `sts_begin_parallel_scan(accessor)` (sharedtuplestore.c): begin a shared
    /// read of this accessor's partition.
    pub fn sts_begin_parallel_scan(
        accessor: &mut types_nodes::nodehashjoin::SharedTuplestoreAccessor,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `sts_end_parallel_scan(accessor)` (sharedtuplestore.c): finish a shared
    /// read.
    pub fn sts_end_parallel_scan(
        accessor: &mut types_nodes::nodehashjoin::SharedTuplestoreAccessor,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `sts_parallel_scan_next(accessor, &hashvalue)` (sharedtuplestore.c):
    /// read the next tuple, returning it (copied into `mcx`, the caller's
    /// current context) and its meta hash value, or `None` at end. The tuple
    /// crosses as its contiguous C `MinimalTuple` byte image (the flat blob,
    /// `t_len` first), the form sharedtuplestore stores on disk.
    pub fn sts_parallel_scan_next<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        accessor: &mut types_nodes::nodehashjoin::SharedTuplestoreAccessor,
    ) -> types_error::PgResult<Option<(mcx::PgVec<'mcx, u8>, u32)>>
);

seam_core::seam!(
    /// `sts_puttuple(accessor, &hashvalue, tuple)` (sharedtuplestore.c): write a
    /// tuple (with its meta hash value) to the shared partition. The tuple is its
    /// contiguous C `MinimalTuple` byte image (the flat blob, `t_len` first).
    pub fn sts_puttuple(
        accessor: &mut types_nodes::nodehashjoin::SharedTuplestoreAccessor,
        hashvalue: u32,
        tuple: &[u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `sts_end_write(accessor)` (sharedtuplestore.c): flush and make the
    /// partition readable by any backend.
    pub fn sts_end_write(
        accessor: &mut types_nodes::nodehashjoin::SharedTuplestoreAccessor,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `tuplestore_skiptuples(state, ntuples, forward)` (tuplestore.c): skip
    /// over `ntuples` tuples in the given direction without fetching them;
    /// returns false if it ran off the end before skipping them all. Can
    /// `ereport(ERROR)` (read path).
    pub fn tuplestore_skiptuples(
        state: &mut types_nodes::Tuplestorestate<'_>,
        ntuples: i64,
        forward: bool,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `tuplestore_in_memory(state)` (tuplestore.c): true while the tuplestore
    /// has not yet spilled to disk (`state->status == TSS_INMEM`). nodeWindowAgg
    /// uses it to decide whether to force the whole partition to be spooled in
    /// one go (alternating reads and writes is expensive once spilled).
    pub fn tuplestore_in_memory(state: &types_nodes::Tuplestorestate<'_>) -> bool
);

// ---------------------------------------------------------------------------
// logtape.c — the logical-tape spill surface the hash-agg spill path drives.
// `LogicalTapeSet *` is the real owned [`LogicalTapeSet`] (the consumer holds
// it by value); `LogicalTape *` is a `usize` slot index into the set's `tapes`
// vector. No side-table registry.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `LogicalTapeSetCreate(preallocate, fileset, worker)` (logtape.c):
    /// create a tape set. The hash-agg path passes no `SharedFileSet`
    /// (non-parallel spill) — `fileset = NULL`, `worker = -1`. The set is
    /// pallocked in `mcx` (the caller's context), so creation is fallible on
    /// OOM; the owned set is returned for the consumer to hold by value.
    pub fn logical_tape_set_create<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        preallocate: bool,
        worker: i32,
    ) -> types_error::PgResult<mcx::PgBox<'mcx, LogicalTapeSet<'mcx>>>
);

seam_core::seam!(
    /// `LogicalTapeSetClose(lts)` (logtape.c): destroy the tape set and its
    /// underlying `BufFile`. Infallible (close paths do not `ereport(ERROR)`).
    /// Consumes the owned set (the C caller frees the `LogicalTapeSet *`).
    pub fn logical_tape_set_close(lts: mcx::PgBox<'_, LogicalTapeSet<'_>>)
);

seam_core::seam!(
    /// `LogicalTapeSetBlocks(lts)` (logtape.c): number of blocks used by the
    /// set (the agg disk-usage metric reads this).
    pub fn logical_tape_set_blocks(lts: &LogicalTapeSet<'_>) -> i64
);

seam_core::seam!(
    /// `LogicalTapeCreate(lts)` (logtape.c): allocate a new tape in the set;
    /// returns the tape's slot index (the owned-model `LogicalTape *`).
    pub fn logical_tape_create(
        lts: &mut LogicalTapeSet<'_>,
    ) -> types_error::PgResult<usize>
);

seam_core::seam!(
    /// `LogicalTapeClose(lt)` (logtape.c): release a single tape (by slot).
    pub fn logical_tape_close(lts: &mut LogicalTapeSet<'_>, slot: usize)
);

seam_core::seam!(
    /// `LogicalTapeWrite(lt, ptr, size)` (logtape.c): append `data` to the tape
    /// at `slot`. Flushing a full block can `ereport(ERROR)` on a write failure.
    pub fn logical_tape_write(
        lts: &mut LogicalTapeSet<'_>,
        slot: usize,
        data: &[u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `LogicalTapeRewindForRead(lt, buffer_size)` (logtape.c): switch the tape
    /// at `slot` from writing to reading, with the given read buffer size.
    pub fn logical_tape_rewind_for_read(
        lts: &mut LogicalTapeSet<'_>,
        slot: usize,
        buffer_size: usize,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `LogicalTapeRead(lt, ptr, size)` (logtape.c): read up to `dst.len()`
    /// bytes from the tape at `slot`; returns the number of bytes actually read.
    pub fn logical_tape_read(
        lts: &mut LogicalTapeSet<'_>,
        slot: usize,
        dst: &mut [u8],
    ) -> types_error::PgResult<usize>
);

// ---------------------------------------------------------------------------
//  Handle-threaded SharedTuplestore surface (nodeHash / parallel hash build).
//  Same C functions as above; the `SharedTuplestoreAccessor *` is carried as
//  the opaque `SharedTuplestoreAccessorHandle` token.
// ---------------------------------------------------------------------------

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
    /// `sts_reinitialize(accessor)` (sharedtuplestore.c): prepare an
    /// already-written shared tuplestore to be read again from the start.
    pub fn sts_reinitialize(
        accessor: types_execparallel::SharedTuplestoreAccessorHandle,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `sts_begin_parallel_scan(accessor)` (sharedtuplestore.c): begin a
    /// cooperative parallel scan of every participant's partition.
    pub fn sts_begin_parallel_scan_handle(
        accessor: types_execparallel::SharedTuplestoreAccessorHandle,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `sts_end_parallel_scan(accessor)` (sharedtuplestore.c): end the
    /// cooperative parallel scan, releasing read buffers. Infallible.
    pub fn sts_end_parallel_scan_handle(
        accessor: types_execparallel::SharedTuplestoreAccessorHandle,
    )
);

seam_core::seam!(
    /// `sts_puttuple(accessor, meta_data, tuple)` (sharedtuplestore.c): write a
    /// tuple plus its fixed-size meta-data (the tuple's hash value, in parallel
    /// hash) to this participant's partition. The tuple is its contiguous C
    /// `MinimalTuple` byte image (the flat blob, `t_len` first).
    pub fn sts_puttuple_handle(
        accessor: types_execparallel::SharedTuplestoreAccessorHandle,
        meta_data: &[u8],
        tuple: &[u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `sts_end_write(accessor)` (sharedtuplestore.c): finish writing this
    /// participant's partition, flushing its output buffer to the temp file.
    pub fn sts_end_write_handle(
        accessor: types_execparallel::SharedTuplestoreAccessorHandle,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `sts_parallel_scan_next(accessor, meta_data)` (sharedtuplestore.c): fetch
    /// the next tuple from the cooperative scan, copying its meta-data into
    /// `meta_data`. Returns `None` at end of the whole store (C `NULL`). The
    /// returned tuple is its contiguous C `MinimalTuple` byte image (the flat
    /// blob, `t_len` first), allocated in `mcx`.
    pub fn sts_parallel_scan_next_handle<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        accessor: types_execparallel::SharedTuplestoreAccessorHandle,
        meta_data: &mut [u8],
    ) -> types_error::PgResult<Option<mcx::PgVec<'mcx, u8>>>
);
