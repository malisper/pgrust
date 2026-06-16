//! `utils/sort/tuplesort.c` + `utils/sort/tuplesortvariants.c` — the generalized
//! tuple-sort engine nodeSort / nodeAgg / nbtsort / etc. drive through the
//! `tuplesort_*` access methods.
//!
//! STAGE F1 (this crate, in-memory engine):
//!  - the concrete owned engine state [`TuplesortStateImpl`] (`struct
//!    Tuplesortstate` + its embedded `TuplesortPublic base`), stored type-erased
//!    behind the [`types_nodes::Tuplesortstate`] carrier the seams + consumers
//!    already use;
//!  - the closed [`SortVariantKind`] enum that replaces the C function-pointer
//!    method table (`base.comparetup/writetup/readtup/removeabbrev`);
//!  - `tuplesort_begin_common` + `tuplesort_begin_batch`;
//!  - the in-memory state machine: `puttuple_common` (TSS_INITIAL /
//!    TSS_BOUNDED / TSS_BUILDRUNS dispatch), `grow_memtuples`,
//!    `consider_abort_common`, the heap routines (insert / replace_top /
//!    delete_top / make_bounded_heap / sort_bounded_heap), the qsort
//!    (`tuplesort_sort_memtuples`), `performsort` INMEM path, `gettuple_common`
//!    INMEM path, `skiptuples`, `set_bound`, `used_bound`, `reversedirection`,
//!    `free_sort_tuple`, `rescan` / `markpos` / `restorepos` INMEM, `get_stats`,
//!    `updatemax`, `merge_order`, method/space names, `free` / `end` / `reset`;
//!  - the heap + datum variant in-memory `comparetup` (over the landed
//!    `apply_sort_comparator` sort-support seam) so the engine's qsort/heap
//!    actually orders tuples.
//!
//! DEFERRED:
//!  - the external-merge tape engine (`inittapes` / `mergeruns` / `dumptuples` /
//!    etc.) is stubbed as PRIVATE intra-crate fns that loud-panic until F2 fills
//!    them — they are reached only once a sort overflows `workMem`;
//!  - the variant byte-serialization + tuple-forming put/get entry points
//!    (`writetup` / `readtup` / `tuplesort_puttupleslot` / `tuplesort_getdatum`
//!    etc.) and the index/cluster variants are F4;
//!  - the public `tuplesort_*` seams are NOT installed here (that is F3);
//!  - parallel (`Sharedsort` / `SortCoordinate`) is a sanctioned 1:1 seam-panic
//!    (the serial NULL-coordinate path is complete and gate-critical).
//!
//! Reshaped from the C (NOT from src-idiomatic, whose generic `SortVariant<T>` +
//! `no_std` model is incompatible). The qsort / heap / bounded logic mirrors the
//! C 1:1.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// The state struct mirrors the C `struct Tuplesortstate` field-for-field; the
// tape/slab/parallel fields (`slab`, `slabFreeHead`, `inputTapes`,
// `SLAB_SLOT_SIZE`, etc.) are laid out now and read only once F2/F3 fill the
// external-merge and parallel paths. Keep them to match C rather than churn the
// struct across stages.
#![allow(dead_code)]

use mcx::{vec_with_capacity_in, McxOwned, MemoryContext, Mcx, PgBox, PgVec};
use types_core::Oid;
use types_error::{PgError, PgResult};
use types_nodes::{
    Tuplesortstate, TuplesortInstrumentation, TuplesortMethod, TuplesortSpaceType,
    TUPLESORT_ALLOWBOUNDED, TUPLESORT_RANDOMACCESS,
};
use types_sortsupport::SortSupportData;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::TupleDescData;

use backend_utils_sort_storage_seams::LogicalTapeSet;

// ===========================================================================
// Constants (tuplesort.c).
// ===========================================================================

/// `INITIAL_MEMTUPSIZE` (tuplesort.c): `Max(1024, ALLOCSET_SEPARATE_THRESHOLD /
/// sizeof(SortTuple) + 1)`. `ALLOCSET_SEPARATE_THRESHOLD` is 8192; our
/// `SortTuple` is wider than the C 24-byte struct, but the C intent is "at
/// least 1024, and large enough that the array is its own palloc chunk". 1024
/// dominates for any realistic `SortTuple` size, exactly as on 64-bit C.
const INITIAL_MEMTUPSIZE: i32 = {
    let by_threshold = 8192 / core::mem::size_of::<SortTuple<'static>>() + 1;
    if by_threshold > 1024 {
        by_threshold as i32
    } else {
        1024
    }
};

/// `MINORDER` (tuplesort.c) — minimum merge order.
const MINORDER: i32 = 6;
/// `MAXORDER` (tuplesort.c) — maximum merge order.
const MAXORDER: i32 = 500;
/// `BLCKSZ` (pg_config.h).
const BLCKSZ: i64 = types_core::BLCKSZ as i64;
/// `TAPE_BUFFER_OVERHEAD` (tuplesort.c) — one block of buffer per tape.
const TAPE_BUFFER_OVERHEAD: i64 = BLCKSZ;
/// `MERGE_BUFFER_SIZE` (tuplesort.c) — extra pre-read buffer per input tape.
const MERGE_BUFFER_SIZE: i64 = BLCKSZ * 32;

/// `SLAB_SLOT_SIZE` (tuplesort.c) — fixed slab slot byte size during merge.
pub(crate) const SLAB_SLOT_SIZE: usize = 1024;

/// `INT_MAX`.
const INT_MAX: i32 = i32::MAX;

// ===========================================================================
// TupSortStatus (tuplesort.c).
// ===========================================================================

/// `TupSortStatus` (tuplesort.c): the persistent state of a sort object.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TupSortStatus {
    /// `TSS_INITIAL` — loading tuples, still within memory limit.
    Initial,
    /// `TSS_BOUNDED` — loading tuples into a bounded-size heap.
    Bounded,
    /// `TSS_BUILDRUNS` — loading tuples, writing to tape.
    BuildRuns,
    /// `TSS_SORTEDINMEM` — sort completed entirely in memory.
    SortedInMem,
    /// `TSS_SORTEDONTAPE` — sort completed, final run is on tape.
    SortedOnTape,
    /// `TSS_FINALMERGE` — performing the final merge on-the-fly.
    FinalMerge,
}

// ===========================================================================
// SortTuple / TupleBody (tuplesort.h: struct SortTuple).
// ===========================================================================

/// The body a [`SortTuple`] carries (the C `void *tuple`). The five sort
/// variants store different physical tuples; `None` is the C `NULL` (datum-only
/// pass-by-value / NULL Datum sorts).
#[derive(Debug)]
pub enum TupleBody<'mcx> {
    /// Heap "begin_heap" variant: a `MinimalTuple` (stored as the owned
    /// payload-bearing carrier).
    Minimal(types_tuple::backend_access_common_heaptuple::FormedMinimalTuple<'mcx>),
    /// CLUSTER variant: a full `HeapTuple`.
    Heap(types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>),
    /// Index (btree / hash / gist / brin / gin) variant: on-disk `IndexTuple`
    /// (or BRIN/GIN sort-tuple) bytes.
    Index(PgVec<'mcx, u8>),
    /// Datum variant, pass-by-reference: the copied value bytes (the C
    /// separately-`palloc`'d datum the SortTuple's `tuple` points at).
    Datum(Datum<'mcx>),
}

/// `SortTuple` (tuplesort.h):
///
/// ```c
/// typedef struct { void *tuple; Datum datum1; bool isnull1; int srctape; } SortTuple;
/// ```
///
/// `tuple` is modeled as [`TupleBody`] (the C `void *`, NULL → `None`).
#[derive(Debug)]
pub struct SortTuple<'mcx> {
    /// `void *tuple` — the tuple itself (NULL for pass-by-value Datum sorts).
    pub tuple: Option<TupleBody<'mcx>>,
    /// `Datum datum1` — value (or abbreviated key) of the first sort column.
    pub datum1: Datum<'mcx>,
    /// `bool isnull1` — is the first key column NULL?
    pub isnull1: bool,
    /// `int srctape` — source tape number (used during merge).
    pub srctape: i32,
}

impl<'mcx> SortTuple<'mcx> {
    /// Move-clone helper: a `SortTuple` is `*tuple = *src` in C (a struct copy).
    /// Our `TupleBody` is not `Copy` (it owns heap bytes), so cloning into the
    /// memtuples array requires a deep copy. The engine deep-copies through this
    /// helper at the C `state->memtuples[...] = *tuple` assignment sites.
    fn clone_in(&self, mcx: Mcx<'mcx>) -> PgResult<SortTuple<'mcx>> {
        let tuple = match &self.tuple {
            None => None,
            Some(TupleBody::Minimal(m)) => Some(TupleBody::Minimal(m.clone_in(mcx)?)),
            Some(TupleBody::Heap(h)) => Some(TupleBody::Heap(h.clone_in(mcx)?)),
            Some(TupleBody::Index(b)) => Some(TupleBody::Index(mcx::slice_in(mcx, b)?)),
            Some(TupleBody::Datum(d)) => Some(TupleBody::Datum(d.clone_in(mcx)?)),
        };
        Ok(SortTuple {
            tuple,
            datum1: self.datum1.clone_in(mcx)?,
            isnull1: self.isnull1,
            srctape: self.srctape,
        })
    }
}

// ===========================================================================
// SortVariantKind — replaces the C base.comparetup/writetup/readtup fn-ptr table.
// ===========================================================================

/// The closed set of sort variants. C dispatches through the
/// `base.comparetup` / `writetup` / `readtup` / `removeabbrev` function
/// pointers set by each `tuplesort_begin_*`; the five variants are a fixed,
/// closed set, so we tag the state and `match`-dispatch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortVariantKind {
    /// `tuplesort_begin_heap` — MinimalTuple sort by column/operator.
    Heap,
    /// `tuplesort_begin_cluster` — full HeapTuple sort by a btree index def.
    Cluster,
    /// `tuplesort_begin_index_btree` — IndexTuple sort by index sort operators.
    IndexBtree,
    /// `tuplesort_begin_index_hash` — IndexTuple sort by hash bucket.
    IndexHash,
    /// `tuplesort_begin_datum` — single bare Datum sort.
    Datum,
}

// ===========================================================================
// TuplesortPublic (tuplesort.h) — the public base of a sort state.
// ===========================================================================

/// `TuplesortPublic` (tuplesort.h): the public part of a sort operation, which
/// in C carries the variant method pointers + the fields their implementations
/// use. Here the method pointers are replaced by [`TuplesortStateImpl::variant`]
/// match-dispatch; this struct holds the remaining `base.*` data fields.
///
/// The C `maincontext` / `sortcontext` / `tuplecontext` `MemoryContext`s are not
/// modeled as separate arenas: the owned model allocates everything in the one
/// `mcx` arena the carrier was begun in, and uses the C `availMem` bookkeeping
/// (see [`TuplesortStateImpl`]) for the workMem accounting.
pub struct TuplesortPublic<'mcx> {
    /// `bool haveDatum1` — are `SortTuple.datum1` / `isnull1` maintained?
    pub haveDatum1: bool,
    /// `int nKeys` — number of columns in the sort key.
    pub nKeys: i32,
    /// `SortSupport sortKeys` — array of length `nKeys` (empty for index_hash).
    pub sortKeys: PgVec<'mcx, SortSupportData<'mcx>>,
    /// `SortSupport onlyKey` — set (to index 0) when the single-key fast path is
    /// usable; `None` otherwise. (We carry the index, since `sortKeys` is owned
    /// inline and a borrow would alias.)
    pub onlyKey: Option<usize>,
    /// `int sortopt` — bitmask of `TUPLESORT_*` flags.
    pub sortopt: i32,
    /// `bool tuples` — can `SortTuple.tuple` ever be set?
    pub tuples: bool,
    /// `void *arg` — variant-specific data. Modeled per-variant below.
    pub arg: SortVariantArg<'mcx>,
}

/// The C `base.arg` (`void *`), per variant. Only the data the in-memory engine
/// + variant comparetup actually read is carried; the rest fills in F4.
#[derive(Debug)]
pub enum SortVariantArg<'mcx> {
    /// Heap: `arg` is the `TupleDesc`.
    Heap { tupDesc: TupleDescData<'mcx> },
    /// Datum: `TuplesortDatumArg { Oid datumType; int datumTypeLen; }`.
    Datum { datumType: Oid, datumTypeLen: i16 },
    /// Index/cluster variants — F4 fills the concrete `arg` payloads.
    Other,
}

// ===========================================================================
// SlabSlot arena (tuplesort.c: union SlabSlot + the slab free list).
// ===========================================================================

/// `union SlabSlot { SlabSlot *nextfree; char buffer[SLAB_SLOT_SIZE]; }`
/// (tuplesort.c). The C slab is a raw pointer-range arena; we re-model it
/// faithfully (behavior-preserving) as a `Vec<SlabSlot>` arena + an index
/// free-list (`nextfree: Option<usize>` chain). Used only during merge (F2);
/// declared here so the state struct matches C field-for-field.
#[derive(Debug)]
pub(crate) struct SlabSlot {
    /// Free-list link when on the free list (`Some(idx)`); `None` is the end of
    /// the chain. When in use, holds a tuple body elsewhere.
    pub(crate) nextfree: Option<usize>,
}

// ===========================================================================
// TuplesortStateImpl (tuplesort.c: struct Tuplesortstate).
// ===========================================================================

/// `struct Tuplesortstate` (tuplesort.c, private) — the concrete owned engine
/// state. Stored type-erased behind the [`types_nodes::Tuplesortstate`] carrier
/// (`Tuplesortstate::begin` / `payload_mut().downcast`).
///
/// Field-for-field with the C struct (parallel `Sharedsort` fields kept so the
/// serial path matches; the parallel path itself is an F3 seam-panic).
pub struct TuplesortStateImpl<'mcx> {
    /// `TuplesortPublic base`.
    pub base: TuplesortPublic<'mcx>,
    /// `TupSortStatus status`.
    pub status: TupSortStatus,
    /// `bool bounded`.
    pub bounded: bool,
    /// `bool boundUsed`.
    pub boundUsed: bool,
    /// `int bound`.
    pub bound: i32,
    /// `int64 tupleMem`.
    pub tupleMem: i64,
    /// `int64 availMem` — remaining memory available, in bytes.
    pub availMem: i64,
    /// `int64 allowedMem` — total memory allowed, in bytes.
    pub allowedMem: i64,
    /// `int maxTapes`.
    pub maxTapes: i32,
    /// `int64 maxSpace`.
    pub maxSpace: i64,
    /// `bool isMaxSpaceDisk`.
    pub isMaxSpaceDisk: bool,
    /// `TupSortStatus maxSpaceStatus`.
    pub maxSpaceStatus: TupSortStatus,
    /// `LogicalTapeSet *tapeset` — `None` while in-memory.
    pub tapeset: Option<PgBox<'mcx, LogicalTapeSet<'mcx>>>,
    /// The variant tag (replaces the C method-pointer table).
    pub variant: SortVariantKind,

    /// `SortTuple *memtuples` + `int memtupcount` / `memtupsize`: the in-memory
    /// tuple array. The C `memtupsize` (allocated capacity) maps to the Vec's
    /// capacity; we still track it explicitly to mirror `grow_memtuples`.
    pub memtuples: PgVec<'mcx, SortTuple<'mcx>>,
    /// `int memtupcount` — logical tuple count (== `memtuples.len()`).
    pub memtupcount: i32,
    /// `int memtupsize` — allocated length of the memtuples array.
    pub memtupsize: i32,
    /// `bool growmemtuples`.
    pub growmemtuples: bool,

    /// `bool slabAllocatorUsed`.
    pub slabAllocatorUsed: bool,
    /// The slab arena (`slabMemory*` + `slabFreeHead`), re-modeled as an index
    /// arena + free-list head. Empty until merge (F2).
    pub(crate) slab: PgVec<'mcx, SlabSlot>,
    /// `SlabSlot *slabFreeHead` — head index of the free list (`None` = empty).
    pub(crate) slabFreeHead: Option<usize>,

    /// `size_t tape_buffer_mem`.
    pub tape_buffer_mem: usize,
    /// `void *lastReturnedTuple` — modeled as the index/handle the merge path
    /// recycles (F2). Kept as a marker matching C `NULL`/non-NULL semantics.
    pub(crate) lastReturnedTuple: Option<usize>,

    /// `int currentRun`.
    pub currentRun: i32,

    /// Merge tapes (F2): the C `inputTapes`/`outputTapes` are arrays of
    /// `LogicalTape *`; we carry the tape slot indices into `tapeset`.
    pub(crate) inputTapes: PgVec<'mcx, usize>,
    pub(crate) nInputTapes: i32,
    pub(crate) nInputRuns: i32,
    pub(crate) outputTapes: PgVec<'mcx, usize>,
    pub(crate) nOutputTapes: i32,
    pub(crate) nOutputRuns: i32,
    /// `LogicalTape *destTape` — current output tape slot.
    pub(crate) destTape: Option<usize>,
    /// `LogicalTape *result_tape` — the finished output tape slot.
    pub(crate) result_tape: Option<usize>,

    /// `int current` — array index (only used if SORTEDINMEM).
    pub current: i32,
    /// `bool eof_reached`.
    pub eof_reached: bool,

    /// `int64 markpos_block`.
    pub markpos_block: i64,
    /// `int markpos_offset`.
    pub markpos_offset: i32,
    /// `bool markpos_eof`.
    pub markpos_eof: bool,

    /// `int worker` — `-1` for a serial sort (the only path F1 supports).
    pub worker: i32,
    /// `Sharedsort *shared` — always `None` (serial); parallel is F3.
    pub shared_is_some: bool,
    /// `int nParticipants`.
    pub nParticipants: i32,

    /// `int64 abbrevNext`.
    pub abbrevNext: i64,
}

impl<'mcx> TuplesortStateImpl<'mcx> {
    /// The bundle's own context handle (the C `sortcontext`/`maincontext`),
    /// recovered from a charged member so allocations land in the engine
    /// bundle's context with the correct `'mcx` (mirrors tuplestore's `mcx()`).
    #[inline]
    fn mcx(&self) -> Mcx<'mcx> {
        *self.memtuples.allocator()
    }

    /// `SERIAL(state)` — `state->shared == NULL`.
    #[inline]
    fn serial(&self) -> bool {
        !self.shared_is_some
    }
    /// `WORKER(state)` — `state->shared && state->worker != -1`.
    #[inline]
    fn worker(&self) -> bool {
        self.shared_is_some && self.worker != -1
    }
    /// `LEADER(state)` — `state->shared && state->worker == -1`.
    #[inline]
    fn leader(&self) -> bool {
        self.shared_is_some && self.worker == -1
    }
    /// `LACKMEM(state)` — `availMem < 0 && !slabAllocatorUsed`.
    #[inline]
    fn lackmem(&self) -> bool {
        self.availMem < 0 && !self.slabAllocatorUsed
    }
    /// `USEMEM(state, amt)`.
    #[inline]
    fn usemem(&mut self, amt: i64) {
        self.availMem -= amt;
    }
    /// `FREEMEM(state, amt)`.
    #[inline]
    fn freemem(&mut self, amt: i64) {
        self.availMem += amt;
    }
}

// ===========================================================================
// tuplesort_begin_common (tuplesort.c).
// ===========================================================================

/// `tuplesort_begin_common(workMem, coordinate=NULL, sortopt)` (tuplesort.c).
///
/// Serial path only (the `coordinate` parameter is omitted: nodeSort / nbtsort
/// etc. always pass C `NULL` from the grounded path; parallel is F3). Builds the
/// engine state in the supplied build context `mcx` (the engine bundle's own
/// `sortcontext` — see [`begin_state`]); the caller (`tuplesort_begin_heap` /
/// `_datum` / ...) then fills in the variant tag, `base.nKeys`,
/// `base.sortKeys`, etc.
pub fn tuplesort_begin_common<'a>(
    mcx: Mcx<'a>,
    work_mem: i32,
    sortopt: i32,
    variant: SortVariantKind,
) -> PgResult<TuplesortStateImpl<'a>> {
    let mut state = TuplesortStateImpl {
        base: TuplesortPublic {
            haveDatum1: false,
            nKeys: 0,
            sortKeys: PgVec::new_in(mcx),
            onlyKey: None,
            // state->base.sortopt = sortopt;  state->base.tuples = true;
            sortopt,
            tuples: true,
            arg: SortVariantArg::Other,
        },
        status: TupSortStatus::Initial,
        bounded: false,
        boundUsed: false,
        bound: 0,
        tupleMem: 0,
        availMem: 0,
        // workMem is forced to be at least 64KB.
        allowedMem: (work_mem.max(64) as i64) * 1024,
        maxTapes: 0,
        maxSpace: 0,
        isMaxSpaceDisk: false,
        maxSpaceStatus: TupSortStatus::Initial,
        tapeset: None,
        variant,
        memtuples: PgVec::new_in(mcx),
        memtupcount: 0,
        // Initial size of array; the actual allocation happens in begin_batch.
        memtupsize: INITIAL_MEMTUPSIZE,
        growmemtuples: true,
        slabAllocatorUsed: false,
        slab: PgVec::new_in(mcx),
        slabFreeHead: None,
        tape_buffer_mem: 0,
        lastReturnedTuple: None,
        currentRun: 0,
        inputTapes: PgVec::new_in(mcx),
        nInputTapes: 0,
        nInputRuns: 0,
        outputTapes: PgVec::new_in(mcx),
        nOutputTapes: 0,
        nOutputRuns: 0,
        destTape: None,
        result_tape: None,
        current: 0,
        eof_reached: false,
        markpos_block: 0,
        markpos_offset: 0,
        markpos_eof: false,
        // Serial sort: shared == NULL, worker == -1, nParticipants == -1.
        worker: -1,
        shared_is_some: false,
        nParticipants: -1,
        // state->abbrevNext = 10;
        abbrevNext: 10,
    };

    tuplesort_begin_batch(&mut state)?;

    Ok(state)
}

/// `tuplesort_begin_batch(state)` (tuplesort.c): set up (or reset) all per-batch
/// state. The C `tuplecontext` (a bump or aset context) is not a separate arena
/// here; we allocate the memtuples array in `mcx`.
fn tuplesort_begin_batch<'mcx>(state: &mut TuplesortStateImpl<'mcx>) -> PgResult<()> {
    state.status = TupSortStatus::Initial;
    state.bounded = false;
    state.boundUsed = false;

    // state->availMem = state->allowedMem;
    state.availMem = state.allowedMem;

    state.tapeset = None;

    state.memtupcount = 0;

    // Initial size of array must be more than ALLOCSET_SEPARATE_THRESHOLD.
    state.memtupsize = INITIAL_MEMTUPSIZE;
    state.growmemtuples = true;
    state.memtuples = vec_with_capacity_in(state.mcx(), state.memtupsize as usize)?;

    // USEMEM(state, GetMemoryChunkSpace(state->memtuples)): account for the
    // memtuples array. We charge the byte size of the reserved array.
    let chunk = (state.memtupsize as i64) * (core::mem::size_of::<SortTuple<'mcx>>() as i64);
    state.usemem(chunk);

    state.currentRun = 0;

    // C: state->result_tape = NULL; (selected during merge).
    state.result_tape = None;

    Ok(())
}

// ===========================================================================
// tuplesort_set_bound / used_bound (tuplesort.c).
// ===========================================================================

/// `tuplesort_set_bound(state, bound)` (tuplesort.c).
pub fn tuplesort_set_bound<'mcx>(state: &mut TuplesortStateImpl<'mcx>, bound: i64) {
    // Assert we're called before loading any tuples.
    debug_assert!(state.status == TupSortStatus::Initial && state.memtupcount == 0);
    // Assert we allow bounded sorts.
    debug_assert!(state.base.sortopt & TUPLESORT_ALLOWBOUNDED != 0);
    // Can't set the bound twice.
    debug_assert!(!state.bounded);
    // Shouldn't be called in a parallel worker.
    debug_assert!(!state.worker());

    // Parallel leader allows but ignores hint.
    if state.leader() {
        return;
    }

    // We want to be able to compute bound * 2, so limit the setting.
    if bound > (INT_MAX / 2) as i64 {
        return;
    }

    state.bounded = true;
    state.bound = bound as i32;

    // Bounded sorts are not an effective target for abbreviated key
    // optimization. Disable by setting state consistent with no abbreviation.
    if let Some(sk0) = state.base.sortKeys.first_mut() {
        sk0.abbrev_converter = None;
        if sk0.abbrev_full_comparator.is_some() {
            sk0.comparator = sk0.abbrev_full_comparator;
        }
        sk0.abbrev_abort = None;
        sk0.abbrev_full_comparator = None;
    }
}

/// `tuplesort_used_bound(state)` (tuplesort.c).
pub fn tuplesort_used_bound(state: &TuplesortStateImpl<'_>) -> bool {
    state.boundUsed
}

// ===========================================================================
// grow_memtuples (tuplesort.c).
// ===========================================================================

/// `grow_memtuples(state)` (tuplesort.c): enlarge the memtuples array if
/// possible within the memory constraint. Returns true if enlarged.
fn grow_memtuples<'mcx>(state: &mut TuplesortStateImpl<'mcx>) -> PgResult<bool> {
    let memtupsize = state.memtupsize;
    let mem_now_used = state.allowedMem - state.availMem;
    let elem_size = core::mem::size_of::<SortTuple<'mcx>>() as i64;

    // Forget it if we've already maxed out memtuples.
    if !state.growmemtuples {
        return Ok(false);
    }

    let mut newmemtupsize: i32;

    // Select new value of memtupsize.
    if mem_now_used <= state.availMem {
        // We've used no more than half of allowedMem; double, clamp at INT_MAX.
        if memtupsize < INT_MAX / 2 {
            newmemtupsize = memtupsize * 2;
        } else {
            newmemtupsize = INT_MAX;
            state.growmemtuples = false;
        }
    } else {
        // Last increment: increase as much as we safely can (float8 math).
        let grow_ratio = state.allowedMem as f64 / mem_now_used as f64;
        if (memtupsize as f64) * grow_ratio < INT_MAX as f64 {
            newmemtupsize = ((memtupsize as f64) * grow_ratio) as i32;
        } else {
            newmemtupsize = INT_MAX;
        }
        // We won't make any further enlargement attempts.
        state.growmemtuples = false;
    }

    // Must enlarge by at least one element, else report failure.
    if newmemtupsize <= memtupsize {
        state.growmemtuples = false;
        return Ok(false);
    }

    // Clamp to MaxAllocHugeSize / sizeof(SortTuple). MaxAllocHugeSize is
    // SIZE_MAX/2; with realistic SortTuple sizes the work_mem cap keeps us far
    // below this, mirroring C's defensive clamp.
    let max_alloc_huge: i64 = (usize::MAX / 2) as i64;
    if (newmemtupsize as i64) >= max_alloc_huge / elem_size {
        newmemtupsize = (max_alloc_huge / elem_size) as i32;
        state.growmemtuples = false; // can't grow any more
    }

    // Must not cause LACKMEM. Check the growth fits within availMem.
    if state.availMem < ((newmemtupsize - memtupsize) as i64) * elem_size {
        state.growmemtuples = false;
        return Ok(false);
    }

    // OK, do it. FREEMEM the old chunk, repalloc, USEMEM the new chunk.
    state.freemem(state.memtupsize as i64 * elem_size);
    state.memtupsize = newmemtupsize;
    state
        .memtuples
        .try_reserve((newmemtupsize as usize).saturating_sub(state.memtuples.len()))
        .map_err(|_| state.mcx().oom((newmemtupsize as usize) * (elem_size as usize)))?;
    state.usemem(state.memtupsize as i64 * elem_size);
    if state.lackmem() {
        return Err(PgError::error(
            "unexpected out-of-memory situation in tuplesort",
        ));
    }
    Ok(true)
}

// ===========================================================================
// Comparison dispatch (replaces COMPARETUP / base.comparetup fn-ptr).
// ===========================================================================

/// `COMPARETUP(state, a, b)` — `(*state->base.comparetup)(a, b, state)`.
///
/// Dispatches by [`SortVariantKind`] (the C method table). Heap + Datum land on
/// the in-memory `comparetup_heap` / `comparetup_datum` (real, over the landed
/// sort-support seam); the index/cluster comparetup bodies fill in F4.
fn comparetup<'mcx>(
    state: &TuplesortStateImpl<'mcx>,
    a: &SortTuple<'mcx>,
    b: &SortTuple<'mcx>,
) -> PgResult<i32> {
    match state.variant {
        SortVariantKind::Heap => comparetup_heap(state, a, b),
        SortVariantKind::Datum => comparetup_datum(state, a, b),
        SortVariantKind::Cluster
        | SortVariantKind::IndexBtree
        | SortVariantKind::IndexHash => panic!(
            "tuplesort: comparetup for {:?} not yet ported (tuplesortvariants.c, F4)",
            state.variant
        ),
    }
}

/// `comparetup_heap(a, b, state)` (tuplesortvariants.c): compare the leading
/// sort key via `ApplySortComparator`, then the tiebreak keys.
///
/// F1 implements the leading-key comparison (`apply_sort_comparator` seam) which
/// is what the engine's in-memory qsort/heap needs to order the common
/// single-leading-key cases. The full multi-key + abbreviated tiebreak
/// (`comparetup_heap_tiebreak`, which deforms both MinimalTuples) fills in F4.
fn comparetup_heap<'mcx>(
    state: &TuplesortStateImpl<'mcx>,
    a: &SortTuple<'mcx>,
    b: &SortTuple<'mcx>,
) -> PgResult<i32> {
    let sort_key = &state.base.sortKeys[0];

    // Compare the leading sort key.
    let compare = apply_sort_comparator(a.datum1.clone_in(state.mcx())?, a.isnull1,
                                        b.datum1.clone_in(state.mcx())?, b.isnull1,
                                        sort_key)?;
    if compare != 0 {
        return Ok(compare);
    }

    // Compare additional sort keys (tiebreak): F4.
    comparetup_heap_tiebreak(state, a, b)
}

/// `comparetup_heap_tiebreak(a, b, state)` (tuplesortvariants.c) — full-tuple
/// tiebreak (deform both MinimalTuples, compare remaining keys + abbreviated
/// full comparator). F4 fills the deform path; until then, with a single sort
/// key (`onlyKey` set) the tiebreak is never reached, so a multi-key heap sort
/// loud-panics here, matching the F1/F4 split.
fn comparetup_heap_tiebreak<'mcx>(
    state: &TuplesortStateImpl<'mcx>,
    _a: &SortTuple<'mcx>,
    _b: &SortTuple<'mcx>,
) -> PgResult<i32> {
    // No subsequent keys => equal (the C loop body never runs).
    if state.base.nKeys <= 1 && state.base.sortKeys[0].abbrev_converter.is_none() {
        return Ok(0);
    }
    panic!("tuplesort: comparetup_heap_tiebreak (multi-key / abbreviated) not yet ported (tuplesortvariants.c, F4)")
}

/// `comparetup_datum(a, b, state)` (tuplesortvariants.c): a single-key Datum
/// sort always uses `ApplySortComparator` on `datum1` (the `onlyKey` path).
fn comparetup_datum<'mcx>(
    state: &TuplesortStateImpl<'mcx>,
    a: &SortTuple<'mcx>,
    b: &SortTuple<'mcx>,
) -> PgResult<i32> {
    let sort_key = &state.base.sortKeys[0];
    let compare = apply_sort_comparator(a.datum1.clone_in(state.mcx())?, a.isnull1,
                                        b.datum1.clone_in(state.mcx())?, b.isnull1,
                                        sort_key)?;
    if compare != 0 {
        return Ok(compare);
    }
    // comparetup_datum_tiebreak (abbreviated full comparator over stup.tuple): F4.
    if sort_key.abbrev_converter.is_none() {
        return Ok(0);
    }
    panic!("tuplesort: comparetup_datum_tiebreak (abbreviated) not yet ported (tuplesortvariants.c, F4)")
}

/// `ApplySortComparator(datum1, isnull1, datum2, isnull2, ssup)`
/// (sortsupport.h): the NULL-handling wrapper around `ssup->comparator`. Mirrors
/// the C inline exactly (NULLs collate per `ssup_nulls_first`, then the
/// installed comparator, then `ssup_reverse` flips the sign).
fn apply_sort_comparator<'mcx>(
    datum1: Datum<'mcx>,
    isnull1: bool,
    datum2: Datum<'mcx>,
    isnull2: bool,
    ssup: &SortSupportData<'mcx>,
) -> PgResult<i32> {
    let compare: i32;

    if isnull1 {
        if isnull2 {
            compare = 0; // NULL "=" NULL
        } else if ssup.ssup_nulls_first {
            compare = -1; // NULL "<" NOT_NULL
        } else {
            compare = 1; // NULL ">" NOT_NULL
        }
    } else if isnull2 {
        if ssup.ssup_nulls_first {
            compare = 1; // NOT_NULL ">" NULL
        } else {
            compare = -1; // NOT_NULL "<" NULL
        }
    } else {
        let mut c = backend_utils_sort_sortsupport_seams::apply_sort_comparator::call(
            datum1, datum2, ssup,
        )?;
        if ssup.ssup_reverse {
            c = -c;
        }
        return Ok(c);
    }

    Ok(compare)
}

// ===========================================================================
// tuplesort_puttuple_common (tuplesort.c).
// ===========================================================================

/// `tuplesort_puttuple_common(state, tuple, useAbbrev, tuplen)` (tuplesort.c):
/// shared put path for the tuple and datum cases. The variant entry points
/// (`tuplesort_puttupleslot` / `_putdatum` / ...) form the [`SortTuple`] and
/// then call this.
pub fn tuplesort_puttuple_common<'mcx>(
    state: &mut TuplesortStateImpl<'mcx>,
    mut tuple: SortTuple<'mcx>,
    use_abbrev: bool,
    tuplen: i64,
) -> PgResult<()> {
    debug_assert!(!state.leader());

    // account for the memory used for this tuple.
    state.usemem(tuplen);
    state.tupleMem += tuplen;

    if !use_abbrev {
        // Leave ordinary Datum representation, or NULL value.
    } else if !consider_abort_common(state)? {
        // Store abbreviated key representation.
        let abbrev = backend_utils_sort_sortsupport_seams::apply_sort_abbrev_converter::call(
            tuple.datum1.clone_in(state.mcx())?,
            &state.base.sortKeys[0],
        )?;
        // The converter returns a pass-by-value `Datum<'static>`; rehome into mcx.
        tuple.datum1 = abbrev.clone_in(state.mcx())?;
    } else {
        // Set state consistent with never trying abbreviation; rewrite datum1
        // in already-copied tuples (removeabbrev). F4 fills removeabbrev; with
        // `onlyKey` single-key sorts this branch is unreachable (consider_abort
        // is only entered with abbreviation in play). Panic mirrors the F4 split.
        remove_abbrev_all(state)?;
    }

    match state.status {
        TupSortStatus::Initial => {
            // Save the tuple into the unsorted array. Grow when one slot left.
            if state.memtupcount >= state.memtupsize - 1 {
                grow_memtuples(state)?;
                debug_assert!(state.memtupcount < state.memtupsize);
            }
            push_memtuple(state, tuple);

            // Switch to bounded heapsort?
            if state.bounded
                && (state.memtupcount > state.bound * 2
                    || (state.memtupcount > state.bound && state.lackmem()))
            {
                make_bounded_heap(state)?;
                return Ok(());
            }

            // Done if we still fit in available memory and have array slots.
            if state.memtupcount < state.memtupsize && !state.lackmem() {
                return Ok(());
            }

            // Nope; time to switch to tape-based operation.
            inittapes(state, true)?;
            dumptuples(state, false)?;
        }
        TupSortStatus::Bounded => {
            // Discard the new tuple if it's <= the top of the (reversed) heap.
            let cmp = {
                let top = &state.memtuples[0];
                comparetup(state, &tuple, top)?
            };
            if cmp <= 0 {
                free_sort_tuple(state, &mut tuple);
            } else {
                // Discard top of heap, replacing it with the new tuple.
                let mut old_top = core::mem::replace(&mut state.memtuples[0], placeholder_tuple());
                free_sort_tuple(state, &mut old_top);
                tuplesort_heap_replace_top(state, tuple)?;
            }
        }
        TupSortStatus::BuildRuns => {
            // Save the tuple into the unsorted array (there must be space).
            push_memtuple(state, tuple);
            // If we are over the memory limit, dump all tuples.
            dumptuples(state, false)?;
        }
        _ => return Err(PgError::error("invalid tuplesort state")),
    }
    Ok(())
}

/// Push a SortTuple into `memtuples` and bump `memtupcount` (the C
/// `state->memtuples[state->memtupcount++] = *tuple`).
fn push_memtuple<'mcx>(state: &mut TuplesortStateImpl<'mcx>, tuple: SortTuple<'mcx>) {
    if (state.memtupcount as usize) < state.memtuples.len() {
        state.memtuples[state.memtupcount as usize] = tuple;
    } else {
        state.memtuples.push(tuple);
    }
    state.memtupcount += 1;
}

/// A throwaway SortTuple used as a `mem::replace` placeholder (never compared).
fn placeholder_tuple<'mcx>() -> SortTuple<'mcx> {
    SortTuple {
        tuple: None,
        datum1: Datum::ByVal(0),
        isnull1: true,
        srctape: 0,
    }
}

/// `REMOVEABBREV(state, state->memtuples, state->memtupcount)` over all current
/// memtuples — the abbreviation-abort fixup. F4 fills the per-variant
/// removeabbrev; with single-key (`onlyKey`) sorts this is unreachable.
fn remove_abbrev_all<'mcx>(_state: &mut TuplesortStateImpl<'mcx>) -> PgResult<()> {
    panic!("tuplesort: removeabbrev (abbreviation abort fixup) not yet ported (tuplesortvariants.c, F4)")
}

// ===========================================================================
// consider_abort_common (tuplesort.c).
// ===========================================================================

/// `consider_abort_common(state)` (tuplesort.c): poll the abbreviation
/// cost-model abort callback at the doubling `abbrevNext` intervals.
fn consider_abort_common<'mcx>(state: &mut TuplesortStateImpl<'mcx>) -> PgResult<bool> {
    debug_assert!(state.base.sortKeys[0].abbrev_converter.is_some());
    debug_assert!(state.base.sortKeys[0].abbrev_abort.is_some());
    debug_assert!(state.base.sortKeys[0].abbrev_full_comparator.is_some());

    if state.status == TupSortStatus::Initial && state.memtupcount as i64 >= state.abbrevNext {
        state.abbrevNext *= 2;

        // Check opclass-supplied abbreviation abort routine.
        let memtupcount = state.memtupcount;
        let abort = backend_utils_sort_sortsupport_seams::apply_sort_abbrev_abort::call(
            memtupcount,
            &mut state.base.sortKeys[0],
        )?;
        if !abort {
            return Ok(false);
        }

        // Restore authoritative comparator; abbreviation no longer in play.
        let sk0 = &mut state.base.sortKeys[0];
        sk0.comparator = sk0.abbrev_full_comparator;
        sk0.abbrev_converter = None;
        sk0.abbrev_abort = None;
        sk0.abbrev_full_comparator = None;

        // Give up — expect original pass-by-value representation.
        return Ok(true);
    }

    Ok(false)
}

// ===========================================================================
// tuplesort_performsort (tuplesort.c).
// ===========================================================================

/// `tuplesort_performsort(state)` (tuplesort.c): all tuples supplied; finish.
pub fn tuplesort_performsort<'mcx>(state: &mut TuplesortStateImpl<'mcx>) -> PgResult<()> {
    match state.status {
        TupSortStatus::Initial => {
            if state.serial() {
                // Just qsort 'em and we're done.
                tuplesort_sort_memtuples(state)?;
                state.status = TupSortStatus::SortedInMem;
            } else if state.worker() {
                // Parallel worker dumps a single run to tape (F3).
                inittapes(state, false)?;
                dumptuples(state, true)?;
                worker_nomergeruns(state)?;
                state.status = TupSortStatus::SortedOnTape;
            } else {
                // Leader takes over worker tapes and merges (F3).
                leader_takeover_tapes(state)?;
                mergeruns(state)?;
            }
            state.current = 0;
            state.eof_reached = false;
            state.markpos_block = 0;
            state.markpos_offset = 0;
            state.markpos_eof = false;
        }
        TupSortStatus::Bounded => {
            // Transform the bounded heap to a sorted array.
            sort_bounded_heap(state)?;
            state.current = 0;
            state.eof_reached = false;
            state.markpos_offset = 0;
            state.markpos_eof = false;
        }
        TupSortStatus::BuildRuns => {
            // Finish tape-based sort (F2).
            dumptuples(state, true)?;
            mergeruns(state)?;
            state.eof_reached = false;
            state.markpos_block = 0;
            state.markpos_offset = 0;
            state.markpos_eof = false;
        }
        _ => return Err(PgError::error("invalid tuplesort state")),
    }
    Ok(())
}

// ===========================================================================
// tuplesort_gettuple_common (tuplesort.c) — INMEM path.
// ===========================================================================

/// `tuplesort_gettuple_common(state, forward, stup)` (tuplesort.c): fetch the
/// next tuple. F1 implements the `TSS_SORTEDINMEM` path (forward + backward);
/// the tape (`SORTEDONTAPE` / `FINALMERGE`) paths fill in F2.
///
/// Returns the next [`SortTuple`] (a deep copy out of the engine arena, since the
/// owned model can't hand back an aliasing borrow), or `None` at end of sort.
pub fn tuplesort_gettuple_common<'mcx>(
    state: &mut TuplesortStateImpl<'mcx>,
    forward: bool,
) -> PgResult<Option<SortTuple<'mcx>>> {
    debug_assert!(!state.worker());

    match state.status {
        TupSortStatus::SortedInMem => {
            debug_assert!(forward || state.base.sortopt & TUPLESORT_RANDOMACCESS != 0);
            debug_assert!(!state.slabAllocatorUsed);
            if forward {
                if state.current < state.memtupcount {
                    let out = state.memtuples[state.current as usize].clone_in(state.mcx())?;
                    state.current += 1;
                    return Ok(Some(out));
                }
                state.eof_reached = true;

                // Complain if caller retrieves more than a bounded sort allows.
                if state.bounded && state.current >= state.bound {
                    return Err(PgError::error("retrieved too many tuples in a bounded sort"));
                }
                Ok(None)
            } else {
                if state.current <= 0 {
                    return Ok(None);
                }
                // Last returned tuple, or the one before it.
                if state.eof_reached {
                    state.eof_reached = false;
                } else {
                    state.current -= 1; // last returned tuple
                    if state.current <= 0 {
                        return Ok(None);
                    }
                }
                let out = state.memtuples[(state.current - 1) as usize].clone_in(state.mcx())?;
                Ok(Some(out))
            }
        }
        TupSortStatus::SortedOnTape | TupSortStatus::FinalMerge => {
            // The on-tape / final-merge fetch paths (slab recycling, getlen +
            // READTUP, backward seek, mergereadnext) fill in F2.
            panic!("tuplesort: gettuple_common tape path not yet ported (tuplesort.c, F2)")
        }
        _ => Err(PgError::error("invalid tuplesort state")),
    }
}

// ===========================================================================
// tuplesort_skiptuples (tuplesort.c) — INMEM path.
// ===========================================================================

/// `tuplesort_skiptuples(state, ntuples, forward)` (tuplesort.c).
pub fn tuplesort_skiptuples<'mcx>(
    state: &mut TuplesortStateImpl<'mcx>,
    ntuples: i64,
    forward: bool,
) -> PgResult<bool> {
    debug_assert!(forward);
    debug_assert!(ntuples >= 0);
    debug_assert!(!state.worker());

    match state.status {
        TupSortStatus::SortedInMem => {
            if (state.memtupcount - state.current) as i64 >= ntuples {
                state.current += ntuples as i32;
                return Ok(true);
            }
            state.current = state.memtupcount;
            state.eof_reached = true;
            if state.bounded && state.current >= state.bound {
                return Err(PgError::error("retrieved too many tuples in a bounded sort"));
            }
            Ok(false)
        }
        TupSortStatus::SortedOnTape | TupSortStatus::FinalMerge => {
            // gettuple-loop skip over the tape paths (F2).
            let mut n = ntuples;
            while n > 0 {
                if tuplesort_gettuple_common(state, forward)?.is_none() {
                    return Ok(false);
                }
                n -= 1;
            }
            Ok(true)
        }
        _ => Err(PgError::error("invalid tuplesort state")),
    }
}

// ===========================================================================
// tuplesort_merge_order (tuplesort.c).
// ===========================================================================

/// `tuplesort_merge_order(allowedMem)` (tuplesort.c): the number of input tapes
/// to merge in each pass, given `allowedMem` bytes. Exported for the planner.
pub fn tuplesort_merge_order(allowed_mem: i64) -> i32 {
    // We need one tape for each merge input, plus another one for the output,
    // and each of these tapes needs buffer space. In addition we want
    // MERGE_BUFFER_SIZE workspace per input tape (but the output tape doesn't
    // count).
    //
    // mergeorder = (allowedMem - TAPE_BUFFER_OVERHEAD) /
    //              (TAPE_BUFFER_OVERHEAD + MERGE_BUFFER_SIZE)
    let mut mergeorder =
        (allowed_mem - TAPE_BUFFER_OVERHEAD) / (MERGE_BUFFER_SIZE + TAPE_BUFFER_OVERHEAD);

    // Even in minimum memory, use at least a MINORDER merge.
    mergeorder = mergeorder.max(MINORDER as i64);
    // Cap to MAXORDER.
    mergeorder = mergeorder.min(MAXORDER as i64);

    mergeorder as i32
}

// ===========================================================================
// Heap manipulation routines (tuplesort.c, Knuth 5.2.3H).
// ===========================================================================

/// `make_bounded_heap(state)` (tuplesort.c): convert the unordered array to a
/// bounded heap, discarding all but the smallest `bound` tuples.
fn make_bounded_heap<'mcx>(state: &mut TuplesortStateImpl<'mcx>) -> PgResult<()> {
    let tupcount = state.memtupcount;
    debug_assert!(state.status == TupSortStatus::Initial);
    debug_assert!(state.bounded);
    debug_assert!(tupcount >= state.bound);
    debug_assert!(state.serial());

    // Reverse sort direction so largest entry will be at root.
    reversedirection(state);

    // Move the current tuples out (in order) so we can re-insert them into an
    // empty heap. C re-reads `state->memtuples[i]` in place; our heap routines
    // mutate `memtuples`, so we first drain the originals into `src`.
    let mcx = state.mcx();
    let mut src: PgVec<'mcx, SortTuple<'mcx>> = vec_with_capacity_in(mcx, tupcount as usize)?;
    for t in core::mem::replace(&mut state.memtuples, PgVec::new_in(mcx)) {
        src.push(t);
    }
    state.memtupcount = 0; // make the heap empty
    // The live Vec is now empty; subsequent heap inserts start clean.

    for i in 0..(tupcount as usize) {
        if state.memtupcount < state.bound {
            // Insert next tuple into heap (copy to avoid possible overwrite — in
            // our model `src[i]` is owned, so we move it).
            let stup = core::mem::replace(&mut src[i], placeholder_tuple());
            tuplesort_heap_insert(state, stup)?;
        } else {
            // Heap full: replace the largest entry, or discard.
            let cmp = {
                let top = &state.memtuples[0];
                comparetup(state, &src[i], top)?
            };
            if cmp <= 0 {
                free_sort_tuple(state, &mut src[i]);
            } else {
                let stup = core::mem::replace(&mut src[i], placeholder_tuple());
                tuplesort_heap_replace_top(state, stup)?;
            }
        }
    }

    debug_assert!(state.memtupcount == state.bound);
    state.status = TupSortStatus::Bounded;
    Ok(())
}

/// `sort_bounded_heap(state)` (tuplesort.c): convert the bounded heap to a
/// properly-sorted array (in-place delete-top into the freed tail slot).
fn sort_bounded_heap<'mcx>(state: &mut TuplesortStateImpl<'mcx>) -> PgResult<()> {
    let tupcount = state.memtupcount;
    debug_assert!(state.status == TupSortStatus::Bounded);
    debug_assert!(state.bounded);
    debug_assert!(tupcount == state.bound);
    debug_assert!(state.serial());

    // Ensure the Vec has `tupcount` physical slots so the tail-store lands.
    while (state.memtuples.len() as i32) < tupcount {
        state.memtuples.push(placeholder_tuple());
    }

    while state.memtupcount > 1 {
        // stup = memtuples[0]; delete-top sifts up next-largest, decrements count.
        let stup = core::mem::replace(&mut state.memtuples[0], placeholder_tuple());
        tuplesort_heap_delete_top(state)?;
        // memtuples[memtupcount] = stup  (the just-freed tail slot).
        state.memtuples[state.memtupcount as usize] = stup;
    }
    state.memtupcount = tupcount;

    // Reverse sort direction back to the original state.
    reversedirection(state);

    state.status = TupSortStatus::SortedInMem;
    state.boundUsed = true;
    Ok(())
}

/// `tuplesort_sort_memtuples(state)` (tuplesort.c): qsort all memtuples.
///
/// The C dispatches to specialized `qsort_tuple_{unsigned,signed,int32}` /
/// `qsort_ssup` / `qsort_tuple` based on the leading comparator identity. The
/// owned model has no fn-ptr identity to switch on, so we use one comparison
/// routine ([`comparetup`]) routed through the variant dispatch — behavior is
/// identical (the specializations are a pure inlining optimization). We sort
/// with a stable index sort that calls the fallible comparator.
fn tuplesort_sort_memtuples<'mcx>(state: &mut TuplesortStateImpl<'mcx>) -> PgResult<()> {
    debug_assert!(!state.leader());

    if state.memtupcount > 1 {
        let n = state.memtupcount as usize;
        // Trim the physical Vec to the logical count so the sort only orders
        // live tuples (push_memtuple may have left capacity slots).
        // (memtuples.len() == memtupcount here by construction in the INITIAL path.)
        sort_slice_by(state, 0, n)?;
    }
    Ok(())
}

/// In-place sort of `memtuples[lo..hi]` using the fallible [`comparetup`].
/// Heapsort (deterministic, no extra alloc, matches qsort's O(n log n) without
/// needing a comparator that can't error — `sort_unstable_by` can't carry a
/// `PgResult`). Mirrors the C qsort result (the order is fully determined by
/// `comparetup`).
fn sort_slice_by<'mcx>(
    state: &mut TuplesortStateImpl<'mcx>,
    lo: usize,
    hi: usize,
) -> PgResult<()> {
    let n = hi - lo;
    if n < 2 {
        return Ok(());
    }
    // Build a max-heap, then repeatedly extract the max to the end.
    // sift-down helper operates on the [lo..hi) window.
    for start in (0..n / 2).rev() {
        sift_down(state, lo, n, start)?;
    }
    for end in (1..n).rev() {
        state.memtuples.swap(lo, lo + end);
        sift_down(state, lo, end, 0)?;
    }
    Ok(())
}

/// Sift-down within `memtuples[lo .. lo+len)` from logical index `root`.
fn sift_down<'mcx>(
    state: &mut TuplesortStateImpl<'mcx>,
    lo: usize,
    len: usize,
    mut root: usize,
) -> PgResult<()> {
    loop {
        let mut largest = root;
        let l = 2 * root + 1;
        let r = 2 * root + 2;
        if l < len {
            let cmp = comparetup(state, &state.memtuples[lo + l], &state.memtuples[lo + largest])?;
            if cmp > 0 {
                largest = l;
            }
        }
        if r < len {
            let cmp = comparetup(state, &state.memtuples[lo + r], &state.memtuples[lo + largest])?;
            if cmp > 0 {
                largest = r;
            }
        }
        if largest == root {
            break;
        }
        state.memtuples.swap(lo + root, lo + largest);
        root = largest;
    }
    Ok(())
}

/// `tuplesort_heap_insert(state, tuple)` (tuplesort.c): sift-up a new entry.
fn tuplesort_heap_insert<'mcx>(
    state: &mut TuplesortStateImpl<'mcx>,
    tuple: SortTuple<'mcx>,
) -> PgResult<()> {
    debug_assert!(state.memtupcount < state.memtupsize);

    // Ensure physical room.
    while (state.memtuples.len() as i32) <= state.memtupcount {
        state.memtuples.push(placeholder_tuple());
    }

    // j = memtupcount++  (j is the "hole"). The C carries `tuple` in a register
    // and assigns `memtuples[j] = memtuples[i]` to move the parent into the
    // hole; we move the owned parent into the hole with `mem::replace`, keeping
    // `tuple` in the local until the final placement.
    let mut j = state.memtupcount as usize;
    state.memtupcount += 1;
    while j > 0 {
        let i = (j - 1) >> 1;
        // if COMPARETUP(state, tuple, &memtuples[i]) >= 0 break;
        if comparetup(state, &tuple, &state.memtuples[i])? >= 0 {
            break;
        }
        // memtuples[j] = memtuples[i];  (move parent down into the hole)
        let parent = core::mem::replace(&mut state.memtuples[i], placeholder_tuple());
        state.memtuples[j] = parent;
        j = i;
    }
    // memtuples[j] = *tuple;
    state.memtuples[j] = tuple;
    Ok(())
}

/// `tuplesort_heap_delete_top(state)` (tuplesort.c).
fn tuplesort_heap_delete_top<'mcx>(state: &mut TuplesortStateImpl<'mcx>) -> PgResult<()> {
    // if (--memtupcount <= 0) return;
    state.memtupcount -= 1;
    if state.memtupcount <= 0 {
        return Ok(());
    }
    // tuple = &memtuples[memtupcount]; replace top with it.
    let tuple = core::mem::replace(
        &mut state.memtuples[state.memtupcount as usize],
        placeholder_tuple(),
    );
    tuplesort_heap_replace_top(state, tuple)
}

/// `tuplesort_heap_replace_top(state, tuple)` (tuplesort.c): sift-up replace.
fn tuplesort_heap_replace_top<'mcx>(
    state: &mut TuplesortStateImpl<'mcx>,
    tuple: SortTuple<'mcx>,
) -> PgResult<()> {
    debug_assert!(state.memtupcount >= 1);

    let n = state.memtupcount as u32;
    let mut i: u32 = 0; // i is where the "hole" is
    loop {
        let mut j = 2 * i + 1;
        if j >= n {
            break;
        }
        if j + 1 < n
            && comparetup(
                state,
                &state.memtuples[j as usize],
                &state.memtuples[(j + 1) as usize],
            )? > 0
        {
            j += 1;
        }
        if comparetup(state, &tuple, &state.memtuples[j as usize])? <= 0 {
            break;
        }
        // memtuples[i] = memtuples[j];  (move child up into the hole at i)
        let child = core::mem::replace(&mut state.memtuples[j as usize], placeholder_tuple());
        state.memtuples[i as usize] = child;
        i = j;
    }
    // memtuples[i] = *tuple;
    state.memtuples[i as usize] = tuple;
    Ok(())
}

/// `reversedirection(state)` (tuplesort.c): flip `ssup_reverse` /
/// `ssup_nulls_first` on every sort key.
fn reversedirection(state: &mut TuplesortStateImpl<'_>) {
    for sort_key in state.base.sortKeys.iter_mut() {
        sort_key.ssup_reverse = !sort_key.ssup_reverse;
        sort_key.ssup_nulls_first = !sort_key.ssup_nulls_first;
    }
}

/// `free_sort_tuple(state, stup)` (tuplesort.c): release the tuple body and
/// FREEMEM its size. In the owned model dropping the `TupleBody` frees it; we
/// FREEMEM the accounted bytes and clear the field.
fn free_sort_tuple<'mcx>(state: &mut TuplesortStateImpl<'mcx>, stup: &mut SortTuple<'mcx>) {
    if let Some(body) = stup.tuple.take() {
        let space = tuple_body_space(&body) as i64;
        state.freemem(space);
        drop(body);
    }
}

/// `GetMemoryChunkSpace(stup->tuple)` approximation for the FREEMEM in
/// `free_sort_tuple`: the byte footprint of the stored body.
fn tuple_body_space(body: &TupleBody<'_>) -> usize {
    match body {
        TupleBody::Minimal(m) => m.data.len() + core::mem::size_of_val(&*m.tuple),
        TupleBody::Heap(h) => h.data.len(),
        TupleBody::Index(b) => b.len(),
        TupleBody::Datum(Datum::ByRef(b)) => b.len(),
        TupleBody::Datum(Datum::ByVal(_)) => 0,
    }
}

// ===========================================================================
// tuplesort_rescan / markpos / restorepos (tuplesort.c) — INMEM path.
// ===========================================================================

/// `tuplesort_rescan(state)` (tuplesort.c).
pub fn tuplesort_rescan<'mcx>(state: &mut TuplesortStateImpl<'mcx>) -> PgResult<()> {
    debug_assert!(state.base.sortopt & TUPLESORT_RANDOMACCESS != 0);
    match state.status {
        TupSortStatus::SortedInMem => {
            state.current = 0;
            state.eof_reached = false;
            state.markpos_offset = 0;
            state.markpos_eof = false;
            Ok(())
        }
        TupSortStatus::SortedOnTape => {
            // LogicalTapeRewindForRead(result_tape, 0): F2.
            panic!("tuplesort: rescan tape path not yet ported (tuplesort.c, F2)")
        }
        _ => Err(PgError::error("invalid tuplesort state")),
    }
}

/// `tuplesort_markpos(state)` (tuplesort.c).
pub fn tuplesort_markpos<'mcx>(state: &mut TuplesortStateImpl<'mcx>) -> PgResult<()> {
    debug_assert!(state.base.sortopt & TUPLESORT_RANDOMACCESS != 0);
    match state.status {
        TupSortStatus::SortedInMem => {
            state.markpos_offset = state.current;
            state.markpos_eof = state.eof_reached;
            Ok(())
        }
        TupSortStatus::SortedOnTape => {
            // LogicalTapeTell(result_tape, ...): F2.
            panic!("tuplesort: markpos tape path not yet ported (tuplesort.c, F2)")
        }
        _ => Err(PgError::error("invalid tuplesort state")),
    }
}

/// `tuplesort_restorepos(state)` (tuplesort.c).
pub fn tuplesort_restorepos<'mcx>(state: &mut TuplesortStateImpl<'mcx>) -> PgResult<()> {
    debug_assert!(state.base.sortopt & TUPLESORT_RANDOMACCESS != 0);
    match state.status {
        TupSortStatus::SortedInMem => {
            state.current = state.markpos_offset;
            state.eof_reached = state.markpos_eof;
            Ok(())
        }
        TupSortStatus::SortedOnTape => {
            // LogicalTapeSeek(result_tape, ...): F2.
            panic!("tuplesort: restorepos tape path not yet ported (tuplesort.c, F2)")
        }
        _ => Err(PgError::error("invalid tuplesort state")),
    }
}

// ===========================================================================
// tuplesort_get_stats / updatemax / method+space names (tuplesort.c).
// ===========================================================================

/// `tuplesort_updatemax(state)` (tuplesort.c).
fn tuplesort_updatemax(state: &mut TuplesortStateImpl<'_>) {
    let space_used: i64;
    let is_space_disk: bool;

    if let Some(ts) = &state.tapeset {
        is_space_disk = true;
        space_used = backend_utils_sort_storage::logtape::logical_tape_set_blocks(ts) * BLCKSZ;
    } else {
        is_space_disk = false;
        space_used = state.allowedMem - state.availMem;
    }

    if (is_space_disk && !state.isMaxSpaceDisk)
        || (is_space_disk == state.isMaxSpaceDisk && space_used > state.maxSpace)
    {
        state.maxSpace = space_used;
        state.isMaxSpaceDisk = is_space_disk;
        state.maxSpaceStatus = state.status;
    }
}

/// `tuplesort_get_stats(state, stats)` (tuplesort.c).
pub fn tuplesort_get_stats(state: &mut TuplesortStateImpl<'_>) -> TuplesortInstrumentation {
    tuplesort_updatemax(state);

    let space_type = if state.isMaxSpaceDisk {
        TuplesortSpaceType::SORT_SPACE_TYPE_DISK
    } else {
        TuplesortSpaceType::SORT_SPACE_TYPE_MEMORY
    };
    let space_used = (state.maxSpace + 1023) / 1024;

    let sort_method = match state.maxSpaceStatus {
        TupSortStatus::SortedInMem => {
            if state.boundUsed {
                TuplesortMethod::SORT_TYPE_TOP_N_HEAPSORT
            } else {
                TuplesortMethod::SORT_TYPE_QUICKSORT
            }
        }
        TupSortStatus::SortedOnTape => TuplesortMethod::SORT_TYPE_EXTERNAL_SORT,
        TupSortStatus::FinalMerge => TuplesortMethod::SORT_TYPE_EXTERNAL_MERGE,
        _ => TuplesortMethod::SORT_TYPE_STILL_IN_PROGRESS,
    };

    TuplesortInstrumentation {
        sortMethod: sort_method,
        spaceType: space_type,
        spaceUsed: space_used,
    }
}

/// `tuplesort_method_name(m)` (tuplesort.c).
pub fn tuplesort_method_name(m: TuplesortMethod) -> &'static str {
    match m {
        TuplesortMethod::SORT_TYPE_STILL_IN_PROGRESS => "still in progress",
        TuplesortMethod::SORT_TYPE_TOP_N_HEAPSORT => "top-N heapsort",
        TuplesortMethod::SORT_TYPE_QUICKSORT => "quicksort",
        TuplesortMethod::SORT_TYPE_EXTERNAL_SORT => "external sort",
        TuplesortMethod::SORT_TYPE_EXTERNAL_MERGE => "external merge",
    }
}

/// `tuplesort_space_type_name(t)` (tuplesort.c).
pub fn tuplesort_space_type_name(t: TuplesortSpaceType) -> &'static str {
    debug_assert!(
        t == TuplesortSpaceType::SORT_SPACE_TYPE_DISK
            || t == TuplesortSpaceType::SORT_SPACE_TYPE_MEMORY
    );
    if t == TuplesortSpaceType::SORT_SPACE_TYPE_DISK {
        "Disk"
    } else {
        "Memory"
    }
}

// ===========================================================================
// tuplesort_free / end / reset (tuplesort.c).
// ===========================================================================

/// `tuplesort_free(state)` (tuplesort.c): release per-sort resources (the temp
/// tape files; memory is freed by dropping the engine). In the owned model
/// dropping the `LogicalTapeSet` closes it.
fn tuplesort_free<'mcx>(state: &mut TuplesortStateImpl<'mcx>) {
    // Delete temporary "tape" files, if any (LogicalTapeSetClose).
    if let Some(ts) = state.tapeset.take() {
        backend_utils_sort_storage::logtape::logical_tape_set_close(ts);
    }
    // FREESTATE(state) — variant-specific cleanup (cluster closes its estate);
    // F4 fills it. The serial in-memory heap/datum variants have no freestate.
    // Drop the memtuples so their bodies are released (MemoryContextReset).
    state.memtuples.clear();
    state.memtupcount = 0;
}

/// `tuplesort_end(state)` (tuplesort.c): release resources and clean up.
pub fn tuplesort_end<'mcx>(mut state: TuplesortStateImpl<'mcx>) -> PgResult<()> {
    tuplesort_free(&mut state);
    // MemoryContextDelete(maincontext): dropping the owned state frees it.
    drop(state);
    Ok(())
}

/// `tuplesort_reset(state)` (tuplesort.c): reset for a new batch, keeping the
/// meta-information.
pub fn tuplesort_reset<'mcx>(state: &mut TuplesortStateImpl<'mcx>) -> PgResult<()> {
    tuplesort_updatemax(state);
    tuplesort_free(state);

    tuplesort_begin_batch(state)?;

    state.lastReturnedTuple = None;
    state.slab.clear();
    state.slabFreeHead = None;
    Ok(())
}

// ===========================================================================
// Tape engine — DEFERRED to F2 (private loud-panic stubs, NOT seams, NOT todo).
// These are reached only once a sort overflows workMem (`inittapes`) or in the
// parallel paths. F2 fills the bodies over the landed `LogicalTapeSet`.
// ===========================================================================

/// `inittapes(state, mergeruns)` (tuplesort.c) — DEFERRED to F2.
fn inittapes<'mcx>(_state: &mut TuplesortStateImpl<'mcx>, _mergeruns: bool) -> PgResult<()> {
    panic!("tuplesort: inittapes (external-merge tape engine) not yet ported (tuplesort.c, F2)")
}

/// `dumptuples(state, alltuples)` (tuplesort.c) — DEFERRED to F2.
fn dumptuples<'mcx>(_state: &mut TuplesortStateImpl<'mcx>, _alltuples: bool) -> PgResult<()> {
    panic!("tuplesort: dumptuples (external-merge tape engine) not yet ported (tuplesort.c, F2)")
}

/// `mergeruns(state)` (tuplesort.c) — DEFERRED to F2.
fn mergeruns<'mcx>(_state: &mut TuplesortStateImpl<'mcx>) -> PgResult<()> {
    panic!("tuplesort: mergeruns (external-merge tape engine) not yet ported (tuplesort.c, F2)")
}

/// `worker_nomergeruns(state)` (tuplesort.c) — parallel worker, DEFERRED to F3.
fn worker_nomergeruns<'mcx>(_state: &mut TuplesortStateImpl<'mcx>) -> PgResult<()> {
    panic!("tuplesort: worker_nomergeruns (parallel sort) not yet ported (tuplesort.c, F3)")
}

/// `leader_takeover_tapes(state)` (tuplesort.c) — parallel leader, DEFERRED F3.
fn leader_takeover_tapes<'mcx>(_state: &mut TuplesortStateImpl<'mcx>) -> PgResult<()> {
    panic!("tuplesort: leader_takeover_tapes (parallel sort) not yet ported (tuplesort.c, F3)")
}

// ===========================================================================
// Carrier helpers: store/retrieve the concrete engine through the type-erased
// `types_nodes::Tuplesortstate`.
//
// The engine state borrows its own bundle context (`memtuples` etc. are
// allocated in it), which is a self-referential struct safe Rust rejects. The
// `McxOwned` wrapper (the same mechanism `tuplestore` uses for its engine)
// heap-pins the context and erases the borrow soundly: the state is built and
// accessed only through `for<'mcx>`-universal closures. The whole bundle is
// `'static` so it fits the type-erased `Tuplesortstate` carrier.
// ===========================================================================

mcx::bind!(pub TuplesortStateImplBind => TuplesortStateImpl<'mcx>);

/// The self-owned engine bundle (context + state); stored type-erased in the
/// [`types_nodes::Tuplesortstate`] carrier.
pub type OwnedSort = McxOwned<TuplesortStateImplBind>;

/// Build the engine bundle (`tuplesort_begin_common` inside its own
/// `sortcontext`). `work_mem` is in kB. The bundle context is a child of (and
/// accounting-linked to) the C `CurrentMemoryContext` — modeled here as a new
/// limit-carrying context, mirroring `tuplestore_begin_common`.
pub fn begin_state(
    work_mem: i32,
    sortopt: i32,
    variant: SortVariantKind,
) -> PgResult<OwnedSort> {
    // The C `sortcontext` (AllocSetContextCreate) has NO hard allocator cap —
    // the workMem budget is the engine's own soft `availMem`/`allowedMem`
    // accounting (which `grow_memtuples` / `LACKMEM` enforce), NOT a context
    // limit. So the bundle context is unlimited; do not conflate the soft sort
    // budget with a hard allocator ceiling.
    OwnedSort::try_new(MemoryContext::new("TupleSort sort"), |sx| {
        tuplesort_begin_common(sx, work_mem, sortopt, variant)
    })
}

/// Wrap a freshly-built engine bundle in the type-erased [`Tuplesortstate`]
/// carrier (consumers + the 19 seam signatures only ever see the carrier).
pub fn into_carrier<'mcx>(mcx: Mcx<'mcx>, owned: OwnedSort) -> PgResult<Tuplesortstate<'mcx>> {
    Tuplesortstate::begin(mcx, owned)
}

/// Run `f` against the concrete engine state held in the carrier (downcast to
/// the bundle, then `with_mut` for the universal-`'mcx` access). The tuplesort
/// owner's job; loud panic on a mismatched payload, like the C cast.
pub fn with_sort_mut<R>(
    carrier: &mut Tuplesortstate<'_>,
    f: impl for<'mcx> FnOnce(&mut TuplesortStateImpl<'mcx>) -> R,
) -> R {
    let owned = carrier
        .payload_mut()
        .expect("tuplesort: operation on a NULL Tuplesortstate")
        .downcast_mut::<OwnedSort>()
        .expect("tuplesort: carrier payload is not this unit's engine");
    owned.with_mut(f)
}

/// Shared `&` access through the carrier.
pub fn with_sort<R>(
    carrier: &Tuplesortstate<'_>,
    f: impl for<'mcx> FnOnce(&TuplesortStateImpl<'mcx>) -> R,
) -> R {
    let owned = carrier
        .payload()
        .expect("tuplesort: operation on a NULL Tuplesortstate")
        .downcast_ref::<OwnedSort>()
        .expect("tuplesort: carrier payload is not this unit's engine");
    owned.with(f)
}

#[cfg(test)]
mod tests;
