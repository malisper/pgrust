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
//! STAGE F2 (this crate, external-merge tape engine over the real
//! [`LogicalTapeSet`] — direct `logtape::*` calls, no new seams):
//!  - `inittapes` / `inittapestate` / `selectnewtape` / `init_slab_allocator`;
//!  - `mergeruns` (the balanced k-way merge: `TSS_BUILDRUNS` →
//!    `TSS_SORTEDONTAPE` / `TSS_FINALMERGE`), `mergeonerun`, `beginmerge`,
//!    `mergereadnext`, `dumptuples`, `getlen`, `markrunend`,
//!    `merge_read_buffer_size`;
//!  - the `gettuple_common` `TSS_SORTEDONTAPE` (forward + backward) and
//!    `TSS_FINALMERGE` paths, and the `rescan` / `markpos` / `restorepos` tape
//!    paths;
//!  - the gate-critical variant byte-serialization `WRITETUP` / `READTUP` for
//!    the heap (`MinimalTuple`) and datum variants — over the
//!    `backend-access-common-heaptuple` flat-blob codec.
//!
//! The C slab allocator (`union SlabSlot` arena recycled by `READTUP` /
//! `RELEASE_SLAB_SLOT`) is a tuple-body recycling optimization. In the owned
//! model `SortTuple.tuple` owns its bytes and is freed on drop, so the slab is
//! re-modeled as an index free-list whose `RELEASE_SLAB_SLOT` is a body-drop
//! (`lastReturnedTuple` recycling is implicit in Rust ownership); the
//! `slabAllocatorUsed` flag still disables `USEMEM`/`LACKMEM` exactly as C.
//!
//! DEFERRED:
//!  - the tuple-forming put/get entry points (`tuplesort_puttupleslot` /
//!    `_putdatum` / `_gettupleslot` / `_getdatum` / `_putindextuplevalues` /
//!    `_getindextuple`) and the cluster/index variant
//!    `comparetup` / `writetup` / `readtup` / `removeabbrev` are F4 — those
//!    variants' `WRITETUP` / `READTUP` still loud-panic;
//!  - the public `tuplesort_*` seams are NOT installed here (that is F3/F4);
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
use types_core::{AttrNumber, Oid};
use types_error::{PgError, PgResult};
use types_nodes::{
    SlotData, TupleTableSlot, Tuplesortstate, TuplesortInstrumentation, TuplesortMethod,
    TuplesortSpaceType, TUPLESORT_ALLOWBOUNDED, TUPLESORT_RANDOMACCESS,
};
use types_rel::Relation;
use types_sortsupport::SortSupportData;
use types_tuple::backend_access_common_heaptuple::{Datum, FormedMinimalTuple};
use types_tuple::heaptuple::{CompactAttribute, FormData_pg_attribute, TupleDescData};

use backend_utils_sort_storage_seams::LogicalTapeSet;
use backend_utils_sort_storage::logtape;
use backend_access_common_heaptuple as heaptuple;

// ===========================================================================
// GUC variables owned by tuplesort.c.
//
// `bool trace_sort = false;` and `bool optimize_bounded_sort = true;`
// (tuplesort.c globals; declared in guc_tables.c as PGC_USERSET bools with no
// check/assign/show hooks). C reads them straight from the variable, not the
// ControlFile. Each lives in a per-backend `thread_local` here (the Rust home
// for the C global); the `guc-tables` slot reads/writes it through the
// `GucVarAccessors` installed from `init_seams`.
// ===========================================================================

use core::cell::Cell;

thread_local! {
    /// `bool trace_sort` (tuplesort.c) — emit LOG lines about sort resource
    /// usage. Boot default `false`.
    static trace_sort: Cell<bool> = const { Cell::new(false) };

    /// `bool optimize_bounded_sort` (tuplesort.c) — allow the top-N heapsort
    /// optimization for bounded sorts. Boot default `true`.
    static optimize_bounded_sort: Cell<bool> = const { Cell::new(true) };
}

#[inline]
fn trace_sort_get() -> bool {
    trace_sort.with(Cell::get)
}

#[inline]
fn trace_sort_set(v: bool) {
    trace_sort.with(|c| c.set(v));
}

#[inline]
fn optimize_bounded_sort_get() -> bool {
    optimize_bounded_sort.with(Cell::get)
}

#[inline]
fn optimize_bounded_sort_set(v: bool) {
    optimize_bounded_sort.with(|c| c.set(v));
}

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
    /// Index btree/gist subcase: `TuplesortIndexBTreeArg { TuplesortIndexArg
    /// index; bool enforceUnique; bool uniqueNullsNotDistinct; }`
    /// (tuplesortvariants.c). GiST shares this struct with both flags `false`.
    IndexBtree {
        index: TuplesortIndexArg<'mcx>,
        enforceUnique: bool,
        uniqueNullsNotDistinct: bool,
    },
    /// Index hash subcase: `TuplesortIndexHashArg { TuplesortIndexArg index;
    /// uint32 high_mask; uint32 low_mask; uint32 max_buckets; }`
    /// (tuplesortvariants.c).
    IndexHash {
        index: TuplesortIndexArg<'mcx>,
        high_mask: u32,
        low_mask: u32,
        max_buckets: u32,
    },
    /// CLUSTER variant — F4 fills the concrete `arg` payload.
    Other,
}

/// `TuplesortIndexArg { Relation heapRel; Relation indexRel; }`
/// (tuplesortvariants.c): the common index-sort `arg` base. The hot paths
/// (`removeabbrev_index` / `comparetup_index_*` / `readtup_index`) only read
/// `RelationGetDescr(indexRel)`; the index descriptor is deep-cloned into the
/// engine's own context (`indexDesc`). The two `Relation` handles are also kept
/// (their relcache cell `Rc` pins the underlying allocation against eviction,
/// exactly as C's held `Relation` pointers do) for the cold uniqueness-violation
/// error path (`comparetup_index_btree_tiebreak` →
/// `BuildIndexValueDescription` / `RelationGetRelationName` / `errtableconstraint`).
#[derive(Debug)]
pub struct TuplesortIndexArg<'mcx> {
    /// `Relation heapRel` — table the index is being built on.
    pub heapRel: Relation<'mcx>,
    /// `Relation indexRel` — index being built.
    pub indexRel: Relation<'mcx>,
    /// `RelationGetDescr(indexRel)` deep-cloned into the engine context. The C
    /// re-reads this off `indexRel` on every access; we snapshot it once so the
    /// hot paths never need the (lifetime-foreign) relation handle.
    pub indexDesc: TupleDescData<'mcx>,
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
        SortVariantKind::IndexBtree => comparetup_index_btree(state, a, b),
        SortVariantKind::IndexHash => comparetup_index_hash(state, a, b),
        SortVariantKind::Cluster => panic!(
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
/// tiebreak: when the leading key has an abbreviation, run the authoritative
/// full comparator on the leading column; then walk the remaining sort keys
/// (`nkey = 1..nKeys`), deforming both MinimalTuples to fetch each column.
///
/// The C `heap_getattr(&ltup, attno, tupDesc, &isnull)` reads one column of a
/// HeapTupleData laid over the MinimalTuple bytes; here the owned model deforms
/// the whole tuple once per side and indexes the resulting column array. The
/// deform cost is a faithful behaviour match (a single sort comparison can
/// fetch any subset of columns).
fn comparetup_heap_tiebreak<'mcx>(
    state: &TuplesortStateImpl<'mcx>,
    a: &SortTuple<'mcx>,
    b: &SortTuple<'mcx>,
) -> PgResult<i32> {
    // No abbreviation and no subsequent keys => equal (the C loop never runs).
    if state.base.nKeys <= 1 && state.base.sortKeys[0].abbrev_converter.is_none() {
        return Ok(0);
    }

    let mcx = state.mcx();
    let tup_desc = match &state.base.arg {
        SortVariantArg::Heap { tupDesc } => tupDesc,
        _ => {
            return Err(PgError::error(
                "tuplesort comparetup_heap_tiebreak: arg is not a TupleDesc",
            ))
        }
    };

    // Deform both MinimalTuples to the full column arrays (heap_getattr per
    // column in C; one deform per side here).
    let lcols = heap_deform_sort_minimal(mcx, a, tup_desc)?;
    let rcols = heap_deform_sort_minimal(mcx, b, tup_desc)?;

    let sort_key0 = &state.base.sortKeys[0];
    if sort_key0.abbrev_converter.is_some() {
        let attno = sort_key0.ssup_attno;
        let idx = (attno as usize).saturating_sub(1);
        let (datum1, isnull1) = &lcols[idx];
        let (datum2, isnull2) = &rcols[idx];
        let compare = apply_sort_abbrev_full_comparator(
            datum1.clone_in(mcx)?,
            *isnull1,
            datum2.clone_in(mcx)?,
            *isnull2,
            sort_key0,
        )?;
        if compare != 0 {
            return Ok(compare);
        }
    }

    // sortKey++; for (nkey = 1; nkey < base->nKeys; nkey++, sortKey++)
    for nkey in 1..state.base.nKeys as usize {
        let sort_key = &state.base.sortKeys[nkey];
        let idx = (sort_key.ssup_attno as usize).saturating_sub(1);
        let (datum1, isnull1) = &lcols[idx];
        let (datum2, isnull2) = &rcols[idx];
        let compare = apply_sort_comparator(
            datum1.clone_in(mcx)?,
            *isnull1,
            datum2.clone_in(mcx)?,
            *isnull2,
            sort_key,
        )?;
        if compare != 0 {
            return Ok(compare);
        }
    }

    Ok(0)
}

/// Deform the `MinimalTuple` body a heap `SortTuple` carries into its full
/// column array (the C `heap_getattr` over a HeapTupleData laid on the minimal
/// tuple). Returns `(Datum, isnull)` per attribute.
fn heap_deform_sort_minimal<'mcx>(
    mcx: Mcx<'mcx>,
    stup: &SortTuple<'mcx>,
    tup_desc: &TupleDescData<'mcx>,
) -> PgResult<PgVec<'mcx, (Datum<'mcx>, bool)>> {
    let mtup = match &stup.tuple {
        Some(TupleBody::Minimal(m)) => m,
        _ => {
            return Err(PgError::error(
                "tuplesort comparetup_heap_tiebreak: non-minimal tuple body",
            ))
        }
    };
    let blob = backend_access_common_heaptuple::flat::minimal_tuple_to_flat(mcx, mtup)
        .map_err(flat_err)?;
    backend_access_common_heaptuple::flat::heap_deform_minimal_tuple_flat(mcx, &blob, tup_desc)
        .map_err(flat_err)
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
    // comparetup_datum_tiebreak: if we have abbreviations, then `tuple` holds
    // the original value; run the authoritative full comparator on it.
    comparetup_datum_tiebreak(state, a, b)
}

/// `comparetup_datum_tiebreak(a, b, state)` (tuplesortvariants.c): when the
/// (single) sort key has an abbreviation converter, the abbreviated leading
/// comparison was inconclusive, so re-compare the original full values which
/// the datum sort stores in `stup.tuple` (`TupleBody::Datum`). With no
/// abbreviation the result is unconditionally 0 (equal), exactly as in C.
fn comparetup_datum_tiebreak<'mcx>(
    state: &TuplesortStateImpl<'mcx>,
    a: &SortTuple<'mcx>,
    b: &SortTuple<'mcx>,
) -> PgResult<i32> {
    let sort_key = &state.base.sortKeys[0];
    if sort_key.abbrev_converter.is_none() {
        return Ok(0);
    }
    let mcx = state.mcx();
    // PointerGetDatum(a->tuple): the original full value stored alongside.
    let datum1 = datum_body_value(mcx, a)?;
    let datum2 = datum_body_value(mcx, b)?;
    apply_sort_abbrev_full_comparator(
        datum1, a.isnull1, datum2, b.isnull1, sort_key,
    )
}

/// `PointerGetDatum(stup->tuple)` for a datum `SortTuple`: the original
/// (full-representation) value the put path stored in `TupleBody::Datum`.
fn datum_body_value<'mcx>(mcx: Mcx<'mcx>, stup: &SortTuple<'mcx>) -> PgResult<Datum<'mcx>> {
    match &stup.tuple {
        Some(TupleBody::Datum(d)) => d.clone_in(mcx),
        _ => Err(PgError::error(
            "tuplesort comparetup_datum_tiebreak: tuple body is not a Datum",
        )),
    }
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

/// `ApplySortAbbrevFullComparator(datum1, isnull1, datum2, isnull2, ssup)`
/// (sortsupport.h): the NULL-handling wrapper around `ssup->abbrev_full_comparator`
/// (the authoritative comparator used when an abbreviated comparison was
/// inconclusive). Mirrors the C inline exactly.
fn apply_sort_abbrev_full_comparator<'mcx>(
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
        let mut c = backend_utils_sort_sortsupport_seams::apply_sort_abbrev_full_comparator::call(
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
// WRITETUP / READTUP dispatch (replaces base.writetup / base.readtup fn-ptr).
//
// The tape engine (`dumptuples`, `mergeonerun`, `gettuple_common` tape paths)
// drives these. They are per-variant (tuplesortvariants.c); F2 fills the
// gate-critical heap (`MinimalTuple`) + datum variants over the
// `backend-access-common-heaptuple` flat-blob codec, leaving cluster/index for
// F4. The `LEN_WORD_SIZE` length framing (a leading `u32` byte count, plus a
// trailing copy when `TUPLESORT_RANDOMACCESS`) is identical across variants.
// ===========================================================================

/// `sizeof(unsigned int)` — the on-tape length-word framing size.
const LEN_WORD_SIZE: usize = 4;

/// The number of leading bytes of a flat `MinimalTuple` blob that are NOT
/// written to the tape: the C `MINIMAL_TUPLE_DATA_OFFSET` (`offsetof(struct,
/// t_infomask2)` == `t_len`(4) + `mt_padding`(6) == 10). `writetup_heap` writes
/// only the body starting at this offset; `readtup_heap` reconstructs the head.
const MINIMAL_TUPLE_DATA_OFFSET: usize = 10;

/// `WRITETUP(state, tape, stup)` — `(*state->base.writetup)(state, tape, stup)`.
///
/// Split-borrow shape: the tape lives in `state.tapeset`, the tuple in
/// `state.memtuples`; C aliases both through `state`. We pass the tape set +
/// slot and the SortTuple by reference so the caller resolves the borrows.
fn writetup<'mcx>(
    variant: SortVariantKind,
    sortopt: i32,
    tapeset: &mut LogicalTapeSet<'mcx>,
    tape: usize,
    stup: &SortTuple<'mcx>,
) -> PgResult<()> {
    match variant {
        SortVariantKind::Heap => writetup_heap(sortopt, tapeset, tape, stup),
        SortVariantKind::Datum => writetup_datum(sortopt, tapeset, tape, stup),
        SortVariantKind::IndexBtree | SortVariantKind::IndexHash => {
            writetup_index(sortopt, tapeset, tape, stup)
        }
        SortVariantKind::Cluster => panic!(
            "tuplesort: writetup for {variant:?} not yet ported (tuplesortvariants.c, F4)"
        ),
    }
}

/// `READTUP(state, stup, tape, len)` — reconstruct a [`SortTuple`] from `len`
/// on-tape bytes (the length word already consumed by `getlen`). The mcx is the
/// engine bundle's context; the body lands there (the owned-model slab).
fn readtup<'mcx>(
    variant: SortVariantKind,
    base: &TuplesortPublic<'mcx>,
    mcx: Mcx<'mcx>,
    tapeset: &mut LogicalTapeSet<'mcx>,
    tape: usize,
    len: u32,
) -> PgResult<SortTuple<'mcx>> {
    match variant {
        SortVariantKind::Heap => readtup_heap(base, mcx, tapeset, tape, len),
        SortVariantKind::Datum => readtup_datum(base, mcx, tapeset, tape, len),
        SortVariantKind::IndexBtree | SortVariantKind::IndexHash => {
            readtup_index(base, mcx, tapeset, tape, len)
        }
        SortVariantKind::Cluster => panic!(
            "tuplesort: readtup for {variant:?} not yet ported (tuplesortvariants.c, F4)"
        ),
    }
}

/// `LogicalTapeReadExact(tape, ptr, len)` (logtape.h): read exactly `len` bytes
/// or `elog(ERROR, "unexpected end of tape")`.
fn logical_tape_read_exact<'mcx>(
    tapeset: &mut LogicalTapeSet<'mcx>,
    tape: usize,
    dst: &mut [u8],
) -> PgResult<()> {
    let n = logtape::logical_tape_read(tapeset, tape, dst)?;
    if n != dst.len() {
        return Err(PgError::error("unexpected end of tape"));
    }
    Ok(())
}

/// `writetup_heap(state, tape, stup)` (tuplesortvariants.c): write the
/// `MinimalTuple` body to tape. We serialize the owned [`FormedMinimalTuple`] to
/// its flat C-ABI blob (`minimal_tuple_to_flat`), then write the same
/// `tupbody`/`tuplen` framing C does over the raw blob.
fn writetup_heap<'mcx>(
    sortopt: i32,
    tapeset: &mut LogicalTapeSet<'mcx>,
    tape: usize,
    stup: &SortTuple<'mcx>,
) -> PgResult<()> {
    let mcx = *tapeset_mcx(tapeset);
    let mtup = match &stup.tuple {
        Some(TupleBody::Minimal(m)) => m,
        _ => return Err(PgError::error("tuplesort writetup_heap: non-minimal tuple body")),
    };
    let blob = backend_access_common_heaptuple::flat::minimal_tuple_to_flat(mcx, mtup)
        .map_err(flat_err)?;
    // tupbody = blob + MINIMAL_TUPLE_DATA_OFFSET; tupbodylen = t_len - offset.
    let t_len = mtup.tuple.t_len as usize;
    debug_assert_eq!(t_len, blob.len());
    let tupbodylen = t_len - MINIMAL_TUPLE_DATA_OFFSET;
    let tuplen = (tupbodylen + LEN_WORD_SIZE) as u32;

    logtape::logical_tape_write(tapeset, tape, &tuplen.to_ne_bytes())?;
    logtape::logical_tape_write(tapeset, tape, &blob[MINIMAL_TUPLE_DATA_OFFSET..t_len])?;
    if sortopt & TUPLESORT_RANDOMACCESS != 0 {
        logtape::logical_tape_write(tapeset, tape, &tuplen.to_ne_bytes())?;
    }
    Ok(())
}

/// `readtup_heap(state, stup, tape, len)` (tuplesortvariants.c): read the
/// `MinimalTuple` body back, reconstruct the flat blob, decode to a
/// [`FormedMinimalTuple`], and re-extract `datum1` via the leading sort key's
/// `ssup_attno` (`heap_getattr` in C).
fn readtup_heap<'mcx>(
    base: &TuplesortPublic<'mcx>,
    mcx: Mcx<'mcx>,
    tapeset: &mut LogicalTapeSet<'mcx>,
    tape: usize,
    len: u32,
) -> PgResult<SortTuple<'mcx>> {
    let tupbodylen = len as usize - LEN_WORD_SIZE;
    let tuplen = tupbodylen + MINIMAL_TUPLE_DATA_OFFSET;

    // Rebuild the flat blob: leading t_len word (== tuplen), then read tupbody.
    let mut blob: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, tuplen)?;
    blob.resize(tuplen, 0);
    blob[0..4].copy_from_slice(&(tuplen as u32).to_ne_bytes());
    logical_tape_read_exact(
        tapeset,
        tape,
        &mut blob[MINIMAL_TUPLE_DATA_OFFSET..MINIMAL_TUPLE_DATA_OFFSET + tupbodylen],
    )?;
    if base.sortopt & TUPLESORT_RANDOMACCESS != 0 {
        let mut trail = [0u8; LEN_WORD_SIZE];
        logical_tape_read_exact(tapeset, tape, &mut trail)?;
    }

    let tupdesc = match &base.arg {
        SortVariantArg::Heap { tupDesc } => tupDesc,
        _ => return Err(PgError::error("tuplesort readtup_heap: arg is not a TupleDesc")),
    };
    // heap_getattr(&htup, sortKeys[0].ssup_attno, tupDesc, &isnull1): deform the
    // (just-read) tuple and pick the leading sort column.
    let attno = base.sortKeys[0].ssup_attno;
    let cols = backend_access_common_heaptuple::flat::heap_deform_minimal_tuple_flat(
        mcx, &blob, tupdesc,
    )
    .map_err(flat_err)?;
    let (datum1, isnull1) = {
        let idx = (attno as usize).saturating_sub(1);
        let (d, n) = &cols[idx];
        (d.clone_in(mcx)?, *n)
    };
    let mtup =
        backend_access_common_heaptuple::flat::minimal_tuple_from_flat(mcx, &blob).map_err(flat_err)?;
    Ok(SortTuple {
        tuple: Some(TupleBody::Minimal(mtup)),
        datum1,
        isnull1,
        srctape: 0,
    })
}

/// `writetup_datum(state, tape, stup)` (tuplesortvariants.c). The C writes
/// either nothing (NULL), the bare `Datum` word (`!base->tuples`), or the
/// separately-stored pass-by-ref value (`stup->tuple`). In the owned model the
/// pass-by-ref bytes live in `TupleBody::Datum(ByRef(..))` and the by-value word
/// in `datum1` (`tuple == None`).
fn writetup_datum<'mcx>(
    sortopt: i32,
    tapeset: &mut LogicalTapeSet<'mcx>,
    tape: usize,
    stup: &SortTuple<'mcx>,
) -> PgResult<()> {
    // The on-tape payload bytes (waddr/tuplen in C).
    let payload: &[u8];
    let byval_word_bytes;
    if stup.isnull1 {
        // waddr = NULL; tuplen = 0;
        payload = &[];
    } else if stup.tuple.is_none() {
        // !base->tuples: write the Datum word itself (sizeof(Datum) bytes).
        byval_word_bytes = (stup.datum1.as_usize()).to_ne_bytes();
        payload = &byval_word_bytes;
    } else {
        // base->tuples: write the pass-by-ref value bytes.
        match &stup.tuple {
            Some(TupleBody::Datum(Datum::ByRef(b))) => payload = b,
            _ => {
                return Err(PgError::error(
                    "tuplesort writetup_datum: tuple body is not a by-ref Datum",
                ))
            }
        }
    }

    let writtenlen = (payload.len() + LEN_WORD_SIZE) as u32;
    logtape::logical_tape_write(tapeset, tape, &writtenlen.to_ne_bytes())?;
    logtape::logical_tape_write(tapeset, tape, payload)?;
    if sortopt & TUPLESORT_RANDOMACCESS != 0 {
        logtape::logical_tape_write(tapeset, tape, &writtenlen.to_ne_bytes())?;
    }
    Ok(())
}

/// `readtup_datum(state, stup, tape, len)` (tuplesortvariants.c).
fn readtup_datum<'mcx>(
    base: &TuplesortPublic<'mcx>,
    mcx: Mcx<'mcx>,
    tapeset: &mut LogicalTapeSet<'mcx>,
    tape: usize,
    len: u32,
) -> PgResult<SortTuple<'mcx>> {
    let tuplen = len as usize - LEN_WORD_SIZE;

    let stup = if tuplen == 0 {
        // it's NULL.
        SortTuple {
            tuple: None,
            datum1: Datum::ByVal(0),
            isnull1: true,
            srctape: 0,
        }
    } else if !base.tuples {
        // bare Datum word.
        debug_assert_eq!(tuplen, core::mem::size_of::<usize>());
        let mut word = [0u8; core::mem::size_of::<usize>()];
        logical_tape_read_exact(tapeset, tape, &mut word)?;
        SortTuple {
            tuple: None,
            datum1: Datum::ByVal(usize::from_ne_bytes(word)),
            isnull1: false,
            srctape: 0,
        }
    } else {
        // pass-by-ref value bytes.
        let mut raddr: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, tuplen)?;
        raddr.resize(tuplen, 0);
        logical_tape_read_exact(tapeset, tape, &mut raddr)?;
        // stup->datum1 = PointerGetDatum(raddr); stup->tuple = raddr; the owned
        // model carries the bytes once (datum1 mirrors the by-ref value).
        SortTuple {
            tuple: Some(TupleBody::Datum(Datum::ByRef(mcx::slice_in(mcx, &raddr)?))),
            datum1: Datum::ByRef(raddr),
            isnull1: false,
            srctape: 0,
        }
    };

    if base.sortopt & TUPLESORT_RANDOMACCESS != 0 {
        let mut trail = [0u8; LEN_WORD_SIZE];
        logical_tape_read_exact(tapeset, tape, &mut trail)?;
    }
    Ok(stup)
}

/// Map a `MinimalTupleFlatError` to a `PgError` (the flat codec's structural
/// errors become a sort error; a `Pg` variant carries its own error through).
fn flat_err(
    e: backend_access_common_heaptuple::flat::MinimalTupleFlatError,
) -> PgError {
    use backend_access_common_heaptuple::flat::MinimalTupleFlatError;
    match e {
        MinimalTupleFlatError::Pg(err) => err,
        other => PgError::error(format!("tuplesort minimal-tuple codec: {other:?}")),
    }
}

/// The bundle context recovered from the tape set's allocator (used to land the
/// READTUP body in the engine bundle's `'mcx` arena).
#[inline]
fn tapeset_mcx<'a, 'mcx>(tapeset: &'a LogicalTapeSet<'mcx>) -> &'a Mcx<'mcx> {
    tapeset.freeBlocks.allocator()
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
/// memtuples — the abbreviation-abort fixup that rewrites each `datum1` back to
/// the original (non-abbreviated) leading-key value. Dispatched per variant
/// (the C `base.removeabbrev` fn-ptr); heap + datum land here, the
/// index/cluster removeabbrev fill in F4.
fn remove_abbrev_all<'mcx>(state: &mut TuplesortStateImpl<'mcx>) -> PgResult<()> {
    match state.variant {
        SortVariantKind::Heap => removeabbrev_heap(state),
        SortVariantKind::Datum => removeabbrev_datum(state),
        SortVariantKind::IndexBtree | SortVariantKind::IndexHash => removeabbrev_index(state),
        SortVariantKind::Cluster => panic!(
            "tuplesort: removeabbrev for {:?} not yet ported (tuplesortvariants.c, F4)",
            state.variant
        ),
    }
}

/// `removeabbrev_heap(state, stups, count)` (tuplesortvariants.c): for each
/// memtuple, re-extract `datum1` from the leading sort column of the stored
/// MinimalTuple (`heap_getattr(&htup, sortKeys[0].ssup_attno, tupDesc, &isnull1)`).
fn removeabbrev_heap<'mcx>(state: &mut TuplesortStateImpl<'mcx>) -> PgResult<()> {
    let mcx = state.mcx();
    let attno = state.base.sortKeys[0].ssup_attno;
    let idx = (attno as usize).saturating_sub(1);
    let count = state.memtupcount as usize;
    for i in 0..count {
        let tup_desc = match &state.base.arg {
            SortVariantArg::Heap { tupDesc } => tupDesc,
            _ => {
                return Err(PgError::error(
                    "tuplesort removeabbrev_heap: arg is not a TupleDesc",
                ))
            }
        };
        let cols = heap_deform_sort_minimal(mcx, &state.memtuples[i], tup_desc)?;
        let (datum, isnull) = &cols[idx];
        let datum = datum.clone_in(mcx)?;
        let isnull = *isnull;
        state.memtuples[i].datum1 = datum;
        state.memtuples[i].isnull1 = isnull;
    }
    Ok(())
}

/// `removeabbrev_datum(state, stups, count)` (tuplesortvariants.c): each
/// `datum1` is rewritten to `PointerGetDatum(stups[i].tuple)` — the original
/// full value stored alongside in `TupleBody::Datum`.
fn removeabbrev_datum<'mcx>(state: &mut TuplesortStateImpl<'mcx>) -> PgResult<()> {
    let mcx = state.mcx();
    let count = state.memtupcount as usize;
    for i in 0..count {
        let datum = datum_body_value(mcx, &state.memtuples[i])?;
        state.memtuples[i].datum1 = datum;
    }
    Ok(())
}

// ===========================================================================
// Index variant comparetup / removeabbrev / writetup / readtup
// (tuplesortvariants.c). Shared by index_btree, index_gist and index_hash.
// ===========================================================================

/// Read the index `arg` (the C `(TuplesortIndexArg *) base->arg`) — common to
/// btree/gist (`IndexBtree`) and hash (`IndexHash`).
fn index_arg<'a, 'mcx>(arg: &'a SortVariantArg<'mcx>) -> PgResult<&'a TuplesortIndexArg<'mcx>> {
    match arg {
        SortVariantArg::IndexBtree { index, .. } => Ok(index),
        SortVariantArg::IndexHash { index, .. } => Ok(index),
        _ => Err(PgError::error(
            "tuplesort: index variant op on a non-index arg",
        )),
    }
}

/// `index_getattr(tuple, attnum, tupleDescriptor, &isnull)` (access/itup.h) over
/// the on-disk IndexTuple bytes. The C macro fast-paths a non-null leading attr;
/// `nocache_index_getattr` yields the identical value for any attr, so we route
/// through it (behaviour-preserving).
fn index_getattr<'mcx>(
    mcx: Mcx<'mcx>,
    itup: &[u8],
    attnum: i32,
    itupdesc: &TupleDescData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool)> {
    backend_access_common_indextuple_seams::nocache_index_getattr::call(mcx, itup, attnum, itupdesc)
}

/// The on-disk IndexTuple bytes of a [`SortTuple`]'s body.
fn index_tuple_bytes<'a, 'mcx>(stup: &'a SortTuple<'mcx>) -> PgResult<&'a [u8]> {
    match &stup.tuple {
        Some(TupleBody::Index(b)) => Ok(b),
        _ => Err(PgError::error("tuplesort: index op on a non-index tuple body")),
    }
}

/// `ItemPointerGetBlockNumber(&itup->t_tid)` over the on-disk bytes: the
/// `t_tid` is the leading `ItemPointerData` (`ip_blkid` 4 bytes + `ip_posid` 2),
/// laid out exactly as the C struct (`bi_hi` then `bi_lo`).
fn itup_block_number(itup: &[u8]) -> u32 {
    let bi_hi = u16::from_ne_bytes([itup[0], itup[1]]);
    let bi_lo = u16::from_ne_bytes([itup[2], itup[3]]);
    ((bi_hi as u32) << 16) | (bi_lo as u32)
}

/// `ItemPointerGetOffsetNumber(&itup->t_tid)` over the on-disk bytes.
fn itup_offset_number(itup: &[u8]) -> u16 {
    u16::from_ne_bytes([itup[4], itup[5]])
}

/// `_hash_hashkey2bucket(hashkey, maxbucket, highmask, lowmask)` (hashutil.c):
/// pure bit-mask arithmetic with no catalog/state, inlined here (no cross-unit
/// seam needed for a self-contained scalar op).
fn hash_hashkey2bucket(hashkey: u32, maxbucket: u32, highmask: u32, lowmask: u32) -> u32 {
    let mut bucket = hashkey & highmask;
    if bucket > maxbucket {
        bucket &= lowmask;
    }
    bucket
}

/// `removeabbrev_index(state, stups, count)` (tuplesortvariants.c): re-extract
/// each `datum1` from the leading index column.
fn removeabbrev_index<'mcx>(state: &mut TuplesortStateImpl<'mcx>) -> PgResult<()> {
    let mcx = state.mcx();
    let count = state.memtupcount as usize;
    for i in 0..count {
        let (datum1, isnull1) = {
            let arg = index_arg(&state.base.arg)?;
            let itup = index_tuple_bytes(&state.memtuples[i])?;
            index_getattr(mcx, itup, 1, &arg.indexDesc)?
        };
        state.memtuples[i].datum1 = datum1;
        state.memtuples[i].isnull1 = isnull1;
    }
    Ok(())
}

/// `comparetup_index_btree(a, b, state)` (tuplesortvariants.c): compare the
/// leading sort key via `ApplySortComparator`, then the btree tiebreak.
fn comparetup_index_btree<'mcx>(
    state: &TuplesortStateImpl<'mcx>,
    a: &SortTuple<'mcx>,
    b: &SortTuple<'mcx>,
) -> PgResult<i32> {
    let sort_key = &state.base.sortKeys[0];

    // Compare the leading sort key.
    let compare = apply_sort_comparator(
        a.datum1.clone_in(state.mcx())?,
        a.isnull1,
        b.datum1.clone_in(state.mcx())?,
        b.isnull1,
        sort_key,
    )?;
    if compare != 0 {
        return Ok(compare);
    }

    // Compare additional sort keys.
    comparetup_index_btree_tiebreak(state, a, b)
}

/// `comparetup_index_btree_tiebreak(a, b, state)` (tuplesortvariants.c): the
/// full multi-key + abbreviated-key tiebreak, the unique-constraint enforcement,
/// and the final heap-TID (ItemPointer) tiebreak.
fn comparetup_index_btree_tiebreak<'mcx>(
    state: &TuplesortStateImpl<'mcx>,
    a: &SortTuple<'mcx>,
    b: &SortTuple<'mcx>,
) -> PgResult<i32> {
    let mcx = state.mcx();
    let (enforce_unique, unique_nulls_not_distinct) = match &state.base.arg {
        SortVariantArg::IndexBtree {
            enforceUnique,
            uniqueNullsNotDistinct,
            ..
        } => (*enforceUnique, *uniqueNullsNotDistinct),
        // GiST shares the btree arg with both flags false.
        _ => (false, false),
    };
    let arg = index_arg(&state.base.arg)?;
    let tuple1 = index_tuple_bytes(a)?;
    let tuple2 = index_tuple_bytes(b)?;
    let keysz = state.base.nKeys;
    let tup_des = &arg.indexDesc;

    let mut equal_hasnull = false;

    // sortKey[0]: if abbreviated, run the authoritative full comparator.
    if state.base.sortKeys[0].abbrev_converter.is_some() {
        let (datum1, isnull1) = index_getattr(mcx, tuple1, 1, tup_des)?;
        let (datum2, isnull2) = index_getattr(mcx, tuple2, 1, tup_des)?;
        let compare = apply_sort_abbrev_full_comparator(
            datum1,
            isnull1,
            datum2,
            isnull2,
            &state.base.sortKeys[0],
        )?;
        if compare != 0 {
            return Ok(compare);
        }
    }

    // they are equal, so we only need to examine one null flag.
    if a.isnull1 {
        equal_hasnull = true;
    }

    // Remaining sort keys (nkey = 2 .. keysz).
    for nkey in 2..=keysz {
        let sort_key = &state.base.sortKeys[(nkey - 1) as usize];
        let (datum1, isnull1) = index_getattr(mcx, tuple1, nkey, tup_des)?;
        let (datum2, isnull2) = index_getattr(mcx, tuple2, nkey, tup_des)?;

        let compare = apply_sort_comparator(datum1, isnull1, datum2, isnull2, sort_key)?;
        if compare != 0 {
            return Ok(compare); // done when we find unequal attributes.
        }

        // they are equal, so we only need to examine one null flag.
        if isnull1 {
            equal_hasnull = true;
        }
    }

    // Uniqueness enforcement: complain if two equal tuples are detected (unless
    // there was at least one NULL field and NULLS NOT DISTINCT was not set).
    if enforce_unique && !(!unique_nulls_not_distinct && equal_hasnull) {
        // The two compared tuples are never the same physical tuple.
        debug_assert!(!core::ptr::eq(tuple1.as_ptr(), tuple2.as_ptr()));

        // index_deform_tuple(tuple1, tupDes, values, isnull).
        let cols =
            backend_access_common_indextuple_seams::index_deform_tuple::call(mcx, tuple1, tup_des)?;
        let mut values: PgVec<'mcx, Datum<'mcx>> = vec_with_capacity_in(mcx, cols.len())?;
        let mut isnull: PgVec<'mcx, bool> = vec_with_capacity_in(mcx, cols.len())?;
        for (d, n) in cols.iter() {
            values.push(d.clone_in(mcx)?);
            isnull.push(*n);
        }

        let key_desc = backend_access_index_genam_seams::build_index_value_description::call(
            mcx,
            &arg.indexRel,
            &values,
            &isnull,
        )?;

        let index_name = arg.indexRel.name();
        let detail = match &key_desc {
            Some(kd) => format!("Key {} is duplicated.", kd.as_str()),
            None => "Duplicate keys exist.".to_string(),
        };
        return Err(PgError::error(format!(
            "could not create unique index \"{index_name}\": {detail}"
        )));
    }

    // If key values are equal, we sort on ItemPointer (heap TID as the implicit
    // last key attribute — required for btree physical uniqueness).
    {
        let blk1 = itup_block_number(tuple1);
        let blk2 = itup_block_number(tuple2);
        if blk1 != blk2 {
            return Ok(if blk1 < blk2 { -1 } else { 1 });
        }
    }
    {
        let pos1 = itup_offset_number(tuple1);
        let pos2 = itup_offset_number(tuple2);
        if pos1 != pos2 {
            return Ok(if pos1 < pos2 { -1 } else { 1 });
        }
    }

    // ItemPointer values should never be equal.
    debug_assert!(false, "tuplesort: duplicate ItemPointer in index sort");
    Ok(0)
}

/// `comparetup_index_hash(a, b, state)` (tuplesortvariants.c): sort by bucket
/// number, then hash value, then ItemPointer.
fn comparetup_index_hash<'mcx>(
    state: &TuplesortStateImpl<'mcx>,
    a: &SortTuple<'mcx>,
    b: &SortTuple<'mcx>,
) -> PgResult<i32> {
    let (high_mask, low_mask, max_buckets) = match &state.base.arg {
        SortVariantArg::IndexHash {
            high_mask,
            low_mask,
            max_buckets,
            ..
        } => (*high_mask, *low_mask, *max_buckets),
        _ => {
            return Err(PgError::error(
                "tuplesort comparetup_index_hash: arg is not an index-hash arg",
            ))
        }
    };

    // Fetch hash keys and mask off bits we don't want to sort by, so the initial
    // sort is just on the bucket number. The first column is the hash key.
    debug_assert!(!a.isnull1);
    let bucket1 = hash_hashkey2bucket(a.datum1.as_u32(), max_buckets, high_mask, low_mask);
    debug_assert!(!b.isnull1);
    let bucket2 = hash_hashkey2bucket(b.datum1.as_u32(), max_buckets, high_mask, low_mask);
    if bucket1 > bucket2 {
        return Ok(1);
    } else if bucket1 < bucket2 {
        return Ok(-1);
    }

    // If bucket values are equal, sort by hash values.
    let hash1 = a.datum1.as_u32();
    let hash2 = b.datum1.as_u32();
    if hash1 > hash2 {
        return Ok(1);
    } else if hash1 < hash2 {
        return Ok(-1);
    }

    // If hash values are equal, sort on ItemPointer (physical order).
    let tuple1 = index_tuple_bytes(a)?;
    let tuple2 = index_tuple_bytes(b)?;
    {
        let blk1 = itup_block_number(tuple1);
        let blk2 = itup_block_number(tuple2);
        if blk1 != blk2 {
            return Ok(if blk1 < blk2 { -1 } else { 1 });
        }
    }
    {
        let pos1 = itup_offset_number(tuple1);
        let pos2 = itup_offset_number(tuple2);
        if pos1 != pos2 {
            return Ok(if pos1 < pos2 { -1 } else { 1 });
        }
    }

    // ItemPointer values should never be equal.
    debug_assert!(false, "tuplesort: duplicate ItemPointer in hash index sort");
    Ok(0)
}

/// `writetup_index(state, tape, stup)` (tuplesortvariants.c): write the
/// IndexTuple bytes with the C `tuplen = IndexTupleSize(tuple) + sizeof(tuplen)`
/// framing (`IndexTupleSize` == the on-disk byte length).
fn writetup_index<'mcx>(
    sortopt: i32,
    tapeset: &mut LogicalTapeSet<'mcx>,
    tape: usize,
    stup: &SortTuple<'mcx>,
) -> PgResult<()> {
    let tuple = index_tuple_bytes(stup)?;
    // tuplen = IndexTupleSize(tuple) + sizeof(tuplen).
    let tuplen = (tuple.len() + LEN_WORD_SIZE) as u32;
    logtape::logical_tape_write(tapeset, tape, &tuplen.to_ne_bytes())?;
    logtape::logical_tape_write(tapeset, tape, tuple)?;
    if sortopt & TUPLESORT_RANDOMACCESS != 0 {
        logtape::logical_tape_write(tapeset, tape, &tuplen.to_ne_bytes())?;
    }
    Ok(())
}

/// `readtup_index(state, stup, tape, len)` (tuplesortvariants.c): read the
/// IndexTuple bytes back and re-extract `datum1` from the leading column.
fn readtup_index<'mcx>(
    base: &TuplesortPublic<'mcx>,
    mcx: Mcx<'mcx>,
    tapeset: &mut LogicalTapeSet<'mcx>,
    tape: usize,
    len: u32,
) -> PgResult<SortTuple<'mcx>> {
    let tuplen = len as usize - LEN_WORD_SIZE;

    let mut tuple: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, tuplen)?;
    tuple.resize(tuplen, 0);
    logical_tape_read_exact(tapeset, tape, &mut tuple)?;
    if base.sortopt & TUPLESORT_RANDOMACCESS != 0 {
        let mut trail = [0u8; LEN_WORD_SIZE];
        logical_tape_read_exact(tapeset, tape, &mut trail)?;
    }

    // set up first-column key value.
    let arg = index_arg(&base.arg)?;
    let (datum1, isnull1) = index_getattr(mcx, &tuple, 1, &arg.indexDesc)?;
    Ok(SortTuple {
        tuple: Some(TupleBody::Index(tuple)),
        datum1,
        isnull1,
        srctape: 0,
    })
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
        TupSortStatus::SortedOnTape => {
            debug_assert!(forward || state.base.sortopt & TUPLESORT_RANDOMACCESS != 0);
            debug_assert!(state.slabAllocatorUsed);

            // The slot that held the previously-returned tuple can be reused;
            // in the owned model the body is freed by drop — clear the marker.
            state.lastReturnedTuple = None;

            let variant = state.variant;
            let mcx = state.mcx();
            let result = state.result_tape.expect("gettuple_common: no result_tape");

            if forward {
                if state.eof_reached {
                    return Ok(None);
                }
                let tuplen = {
                    let tapeset = state.tapeset.as_mut().expect("gettuple: no tapeset");
                    getlen(tapeset, result, true)?
                };
                if tuplen != 0 {
                    let TuplesortStateImpl { base, tapeset, .. } = state;
                    let tapeset = tapeset.as_mut().expect("gettuple: no tapeset");
                    let stup = readtup(variant, base, mcx, tapeset, result, tuplen)?;
                    state.lastReturnedTuple = Some(0);
                    return Ok(Some(stup));
                } else {
                    state.eof_reached = true;
                    return Ok(None);
                }
            }

            // Backward.
            if state.eof_reached {
                // Back up over the trailing zero length word to the last
                // tuple's ending length word.
                let nmoved = {
                    let tapeset = state.tapeset.as_mut().expect("gettuple: no tapeset");
                    logtape::logical_tape_backspace(tapeset, result, 2 * LEN_WORD_SIZE)?
                };
                if nmoved == 0 {
                    return Ok(None);
                } else if nmoved != 2 * LEN_WORD_SIZE {
                    return Err(PgError::error("unexpected tape position"));
                }
                state.eof_reached = false;
            } else {
                // Back up over the previously-returned tuple's ending length.
                let nmoved = {
                    let tapeset = state.tapeset.as_mut().expect("gettuple: no tapeset");
                    logtape::logical_tape_backspace(tapeset, result, LEN_WORD_SIZE)?
                };
                if nmoved == 0 {
                    return Ok(None);
                } else if nmoved != LEN_WORD_SIZE {
                    return Err(PgError::error("unexpected tape position"));
                }
                let tuplen = {
                    let tapeset = state.tapeset.as_mut().expect("gettuple: no tapeset");
                    getlen(tapeset, result, false)?
                };

                // Back up to the ending length word of the tuple before it.
                let nmoved = {
                    let tapeset = state.tapeset.as_mut().expect("gettuple: no tapeset");
                    logtape::logical_tape_backspace(
                        tapeset,
                        result,
                        tuplen as usize + 2 * LEN_WORD_SIZE,
                    )?
                };
                if nmoved == tuplen as usize + LEN_WORD_SIZE {
                    // The prev tuple is the first in the file.
                    return Ok(None);
                } else if nmoved != tuplen as usize + 2 * LEN_WORD_SIZE {
                    return Err(PgError::error("bogus tuple length in backward scan"));
                }
            }

            let tuplen = {
                let tapeset = state.tapeset.as_mut().expect("gettuple: no tapeset");
                getlen(tapeset, result, false)?
            };

            // Back up to just after the initial length word, then READTUP.
            let nmoved = {
                let tapeset = state.tapeset.as_mut().expect("gettuple: no tapeset");
                logtape::logical_tape_backspace(tapeset, result, tuplen as usize)?
            };
            if nmoved != tuplen as usize {
                return Err(PgError::error("bogus tuple length in backward scan"));
            }
            let TuplesortStateImpl { base, tapeset, .. } = state;
            let tapeset = tapeset.as_mut().expect("gettuple: no tapeset");
            let stup = readtup(variant, base, mcx, tapeset, result, tuplen)?;
            state.lastReturnedTuple = Some(0);
            Ok(Some(stup))
        }
        TupSortStatus::FinalMerge => {
            debug_assert!(forward);
            debug_assert!(state.slabAllocatorUsed);

            // Reusable slot of the previously-returned tuple — drop marker.
            state.lastReturnedTuple = None;

            // This mirrors the inner loop of mergeonerun().
            if state.memtupcount > 0 {
                let src_tape_index = state.memtuples[0].srctape;
                let src_tape = state.inputTapes[src_tape_index as usize];

                // Pull the next tuple from the same tape FIRST (mergereadnext
                // does not touch memtuples[0]).
                let next = mergereadnext(state, src_tape)?;

                // *stup = memtuples[0]: move the heap top out to return it.
                let out = core::mem::replace(&mut state.memtuples[0], placeholder_tuple());
                state.lastReturnedTuple = Some(0);

                match next {
                    Some(mut newtup) => {
                        // Replace the (now-vacated) heap top with the new tuple.
                        // `tuplesort_heap_replace_top` treats slot 0 as the hole
                        // and sifts children up, so the placeholder at slot 0 is
                        // never read.
                        newtup.srctape = src_tape_index;
                        tuplesort_heap_replace_top(state, newtup)?;
                    }
                    None => {
                        // End of run on this tape: remove the top node and close
                        // the tape to release its read buffer early. The slot 0
                        // placeholder is overwritten by delete_top's sift.
                        tuplesort_heap_delete_top(state)?;
                        state.nInputRuns -= 1;
                        let tapeset = state.tapeset.as_mut().expect("gettuple: no tapeset");
                        logtape::logical_tape_close(tapeset, src_tape);
                    }
                }
                return Ok(Some(out));
            }
            Ok(None)
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
    // In the merge phase, we need buffer space for each input and output tape.
    // Each pass reads from M input tapes and writes to N output tapes; each tape
    // consumes TAPE_BUFFER_OVERHEAD bytes, plus we want MERGE_BUFFER_SIZE
    // workspace per input tape. Except for the last passes, M = N, so we choose
    // M giving each input tape (TAPE_BUFFER_OVERHEAD + MERGE_BUFFER_SIZE) of the
    // available memory:
    //
    //   mOrder = allowedMem / (2 * TAPE_BUFFER_OVERHEAD + MERGE_BUFFER_SIZE)
    let mut m_order = allowed_mem / (2 * TAPE_BUFFER_OVERHEAD + MERGE_BUFFER_SIZE);

    // Even in minimum memory, use at least a MINORDER merge; even with lots of
    // memory, never more than a MAXORDER merge.
    m_order = m_order.max(MINORDER as i64);
    m_order = m_order.min(MAXORDER as i64);

    m_order as i32
}

/// Marshal slot for the planner's `tuplesort_merge_order` cost-model seam.
/// C returns `int`; the planner consumes it as a `double`.
fn seam_tuplesort_merge_order(allowed_mem: i64) -> f64 {
    tuplesort_merge_order(allowed_mem) as f64
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
        TupleBody::Datum(Datum::Cstring(_))
        | TupleBody::Datum(Datum::Composite(_))
        | TupleBody::Datum(Datum::Expanded(_))
        | TupleBody::Datum(Datum::Internal(_)) => {
            panic!("tuplesort::tuple_body_space: Cstring/Composite/Expanded/Internal Datum body not yet produced — wave 2")
        }
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
            let result = state.result_tape.expect("rescan: no result_tape");
            let tapeset = state.tapeset.as_mut().expect("rescan: no tapeset");
            logtape::logical_tape_rewind_for_read(tapeset, result, 0)?;
            state.eof_reached = false;
            state.markpos_block = 0;
            state.markpos_offset = 0;
            state.markpos_eof = false;
            Ok(())
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
            let result = state.result_tape.expect("markpos: no result_tape");
            let (block, offset) = {
                let tapeset = state.tapeset.as_mut().expect("markpos: no tapeset");
                logtape::logical_tape_tell(tapeset, result)?
            };
            state.markpos_block = block;
            state.markpos_offset = offset;
            state.markpos_eof = state.eof_reached;
            Ok(())
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
            let result = state.result_tape.expect("restorepos: no result_tape");
            let (block, offset) = (state.markpos_block, state.markpos_offset);
            {
                let tapeset = state.tapeset.as_mut().expect("restorepos: no tapeset");
                logtape::logical_tape_seek(tapeset, result, block, offset)?;
            }
            state.eof_reached = state.markpos_eof;
            Ok(())
        }
        _ => Err(PgError::error("invalid tuplesort state")),
    }
}

// ===========================================================================
// tuplesort_get_stats / updatemax / method+space names (tuplesort.c).
// ===========================================================================

/// `tuplesort_updatemax(state)` (tuplesort.c): persist the running max-space.
fn tuplesort_updatemax(state: &mut TuplesortStateImpl<'_>) {
    let (max_space, is_max_space_disk, max_space_status) = computed_max_space(state);
    state.maxSpace = max_space;
    state.isMaxSpaceDisk = is_max_space_disk;
    state.maxSpaceStatus = max_space_status;
}

/// The (maxSpace, isMaxSpaceDisk, maxSpaceStatus) `tuplesort_updatemax` would
/// settle on, computed WITHOUT mutating `state` (the `&self` core shared by the
/// `&mut` `tuplesort_updatemax` and the `&` get-stats seam path).
fn computed_max_space(
    state: &TuplesortStateImpl<'_>,
) -> (i64, bool, TupSortStatus) {
    let (space_used, is_space_disk) = if let Some(ts) = &state.tapeset {
        (
            backend_utils_sort_storage::logtape::logical_tape_set_blocks(ts) * BLCKSZ,
            true,
        )
    } else {
        (state.allowedMem - state.availMem, false)
    };

    if (is_space_disk && !state.isMaxSpaceDisk)
        || (is_space_disk == state.isMaxSpaceDisk && space_used > state.maxSpace)
    {
        (space_used, is_space_disk, state.status)
    } else {
        (state.maxSpace, state.isMaxSpaceDisk, state.maxSpaceStatus)
    }
}

/// `tuplesort_get_stats(state, stats)` (tuplesort.c).
pub fn tuplesort_get_stats(state: &mut TuplesortStateImpl<'_>) -> TuplesortInstrumentation {
    tuplesort_updatemax(state);
    tuplesort_get_stats_ref(state)
}

/// `tuplesort_get_stats` read-only core (`&self`): the seam contract hands a
/// shared `&Tuplesortstate`. The C body's only mutation is `tuplesort_updatemax`
/// persisting the running max into `state.maxSpace*`; those fields exist only to
/// compare against FUTURE updatemax calls, and a get-stats call reads back
/// exactly the value updatemax would settle on — so computing it locally
/// (without persisting) is observably identical for the stats report.
pub fn tuplesort_get_stats_ref(state: &TuplesortStateImpl<'_>) -> TuplesortInstrumentation {
    let (max_space, is_max_space_disk, max_space_status) = computed_max_space(state);

    let space_type = if is_max_space_disk {
        TuplesortSpaceType::SORT_SPACE_TYPE_DISK
    } else {
        TuplesortSpaceType::SORT_SPACE_TYPE_MEMORY
    };
    let space_used = (max_space + 1023) / 1024;

    let sort_method = match max_space_status {
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
// External-merge tape engine (tuplesort.c) — F2. Over the real LogicalTapeSet
// via direct `logtape::*` calls (no seams; `backend-utils-sort-storage` does not
// depend on tuplesort, so the edge is acyclic).
// ===========================================================================

/// `merge_read_buffer_size(avail_mem, nInputTapes, nInputRuns, maxOutputTapes)`
/// (tuplesort.c): the per-input-tape read-buffer size for one merge pass.
fn merge_read_buffer_size(
    avail_mem: i64,
    n_input_tapes: i32,
    n_input_runs: i32,
    max_output_tapes: i32,
) -> i64 {
    // nOutputRuns = (nInputRuns + nInputTapes - 1) / nInputTapes  (round up)
    let n_output_runs = (n_input_runs + n_input_tapes - 1) / n_input_tapes;
    let n_output_tapes = n_output_runs.min(max_output_tapes);

    // Max((avail_mem - TAPE_BUFFER_OVERHEAD * nOutputTapes) / nInputTapes, 0)
    ((avail_mem - TAPE_BUFFER_OVERHEAD * n_output_tapes as i64) / n_input_tapes as i64).max(0)
}

/// `inittapes(state, mergeruns)` (tuplesort.c): initialize for tape sorting.
/// Called only once a sort overflows `workMem`.
fn inittapes<'mcx>(state: &mut TuplesortStateImpl<'mcx>, mergeruns: bool) -> PgResult<()> {
    debug_assert!(!state.leader());

    if mergeruns {
        // Compute number of input tapes to use when merging.
        state.maxTapes = tuplesort_merge_order(state.allowedMem);
    } else {
        // Workers can sometimes produce a single run, output without merge.
        debug_assert!(state.worker());
        state.maxTapes = MINORDER;
    }

    // Create the tape set + decrease availMem for the tape buffers.
    inittapestate(state, state.maxTapes)?;
    let mcx = state.mcx();
    // shared ? &shared->fileset : NULL → serial path always passes NULL/worker -1.
    let ts = logtape::logical_tape_set_create(mcx, false, state.worker)?;
    state.tapeset = Some(ts);

    state.currentRun = 0;

    // Initialize logical tape arrays.
    state.inputTapes = PgVec::new_in(mcx);
    state.nInputTapes = 0;
    state.nInputRuns = 0;

    // outputTapes = palloc0(maxTapes * sizeof(LogicalTape *)): a slot array of
    // tape indices, grown as `selectnewtape` creates tapes.
    state.outputTapes = PgVec::new_in(mcx);
    state.nOutputTapes = 0;
    state.nOutputRuns = 0;

    state.status = TupSortStatus::BuildRuns;

    selectnewtape(state)?;
    Ok(())
}

/// `inittapestate(state, maxTapes)` (tuplesort.c): generic tape-management
/// state setup; decrease `availMem` for tape buffers.
fn inittapestate<'mcx>(state: &mut TuplesortStateImpl<'mcx>, max_tapes: i32) -> PgResult<()> {
    // tapeSpace = (int64) maxTapes * TAPE_BUFFER_OVERHEAD;
    let tape_space = max_tapes as i64 * TAPE_BUFFER_OVERHEAD;

    // if (tapeSpace + GetMemoryChunkSpace(memtuples) < allowedMem) USEMEM(tapeSpace);
    let memtuples_space =
        state.memtupsize as i64 * core::mem::size_of::<SortTuple<'mcx>>() as i64;
    if tape_space + memtuples_space < state.allowedMem {
        state.usemem(tape_space);
    }

    // PrepareTempTablespaces(): the temp-tablespace selection is a no-op here
    // (logtape creates its BufFile in the default temp tablespace via buffile).
    Ok(())
}

/// `selectnewtape(state)` (tuplesort.c): select the next output tape (creating
/// one until `maxTapes`, then round-robin into the existing tapes).
fn selectnewtape<'mcx>(state: &mut TuplesortStateImpl<'mcx>) -> PgResult<()> {
    if state.nOutputTapes < state.maxTapes {
        // Create a new tape to hold the next run.
        debug_assert!(state.nOutputRuns == state.nOutputTapes);
        let tapeset = state.tapeset.as_mut().expect("selectnewtape: no tapeset");
        let new_tape = logtape::logical_tape_create(tapeset)?;
        state.destTape = Some(new_tape);
        // outputTapes[nOutputTapes] = destTape; nOutputTapes++.
        state.outputTapes.push(new_tape);
        state.nOutputTapes += 1;
        state.nOutputRuns += 1;
    } else {
        // Reached max tapes: append to an existing tape, round-robin.
        let idx = (state.nOutputRuns % state.nOutputTapes) as usize;
        state.destTape = Some(state.outputTapes[idx]);
        state.nOutputRuns += 1;
    }
    Ok(())
}

/// `init_slab_allocator(state, numSlots)` (tuplesort.c). The C arena recycles
/// fixed-size tuple-body slots during merge; the owned model frees tuple bodies
/// by drop, so the arena is a behaviour-preserving index free-list whose only
/// observable effect is `slabAllocatorUsed = true` (which disables
/// USEMEM/LACKMEM tuple accounting, exactly as C). We still `USEMEM` the slot
/// bytes so the disk/memory `maxSpace` accounting matches C.
fn init_slab_allocator<'mcx>(state: &mut TuplesortStateImpl<'mcx>, num_slots: i32) -> PgResult<()> {
    let mcx = state.mcx();
    if num_slots > 0 {
        // palloc(numSlots * SLAB_SLOT_SIZE) + USEMEM the same.
        let mut slab: PgVec<'mcx, SlabSlot> = vec_with_capacity_in(mcx, num_slots as usize)?;
        // Build the free-list chain: slot i -> i+1, last -> None.
        for i in 0..num_slots as usize {
            slab.push(SlabSlot {
                nextfree: if i + 1 < num_slots as usize {
                    Some(i + 1)
                } else {
                    None
                },
            });
        }
        state.slab = slab;
        state.slabFreeHead = Some(0);
        state.usemem(num_slots as i64 * SLAB_SLOT_SIZE as i64);
    } else {
        state.slab = PgVec::new_in(mcx);
        state.slabFreeHead = None;
    }
    state.slabAllocatorUsed = true;
    Ok(())
}

/// `dumptuples(state, alltuples)` (tuplesort.c): sort the in-memory tuples and
/// write the current initial run out to `destTape`.
fn dumptuples<'mcx>(state: &mut TuplesortStateImpl<'mcx>, alltuples: bool) -> PgResult<()> {
    // Nothing to do if we still fit in memory and have array slots (unless this
    // is the final call during initial run generation).
    if state.memtupcount < state.memtupsize && !state.lackmem() && !alltuples {
        return Ok(());
    }

    // Final call might require no sorting; avoid a completely empty run (but a
    // worker must produce at least one tape, even if empty).
    if state.memtupcount == 0 && state.currentRun > 0 {
        return Ok(());
    }

    debug_assert!(state.status == TupSortStatus::BuildRuns);

    if state.currentRun == INT_MAX {
        return Err(PgError::error(
            "cannot have more than 2147483647 runs for an external sort",
        ));
    }

    if state.currentRun > 0 {
        selectnewtape(state)?;
    }

    state.currentRun += 1;

    // Sort all accumulated tuples for this run.
    tuplesort_sort_memtuples(state)?;

    // WRITETUP each tuple to destTape.
    let memtupwrite = state.memtupcount;
    let variant = state.variant;
    let sortopt = state.base.sortopt;
    let dest = state.destTape.expect("dumptuples: no destTape");
    for i in 0..memtupwrite as usize {
        // Split borrow: the tape set + the i-th tuple both live in `state`.
        // Pull the tuple out, write it, put it back (it is dropped after the
        // run anyway when memtupcount resets; C reuses the array slot).
        let stup = core::mem::replace(&mut state.memtuples[i], placeholder_tuple());
        let tapeset = state.tapeset.as_mut().expect("dumptuples: no tapeset");
        let res = writetup(variant, sortopt, tapeset, dest, &stup);
        state.memtuples[i] = stup;
        res?;
    }

    state.memtupcount = 0;

    // FREEMEM the tuple memory we accounted, and reset tupleMem.
    state.freemem(state.tupleMem);
    state.tupleMem = 0;

    // markrunend(destTape): write the zero-length end-of-run marker.
    let tapeset = state.tapeset.as_mut().expect("dumptuples: no tapeset");
    markrunend(tapeset, dest)?;
    Ok(())
}

/// `mergeruns(state)` (tuplesort.c): the balanced k-way merge of all completed
/// initial runs. Ends in `TSS_SORTEDONTAPE` (materialized) or `TSS_FINALMERGE`
/// (on-the-fly final merge).
fn mergeruns<'mcx>(state: &mut TuplesortStateImpl<'mcx>) -> PgResult<()> {
    debug_assert!(state.status == TupSortStatus::BuildRuns);
    debug_assert!(state.memtupcount == 0);

    // If there are multiple runs to merge, abbreviated keys won't have been
    // stored; disable abbreviation from this point on.
    if let Some(sk0) = state.base.sortKeys.first_mut() {
        if sk0.abbrev_converter.is_some() {
            sk0.abbrev_converter = None;
            sk0.comparator = sk0.abbrev_full_comparator;
            sk0.abbrev_abort = None;
            sk0.abbrev_full_comparator = None;
        }
    }

    let mcx = state.mcx();

    // FREEMEM(GetMemoryChunkSpace(memtuples)); pfree(memtuples): we no longer
    // need the large memtuples array. Account the freed bytes, then drop it.
    let old_memtuples_space =
        state.memtupsize as i64 * core::mem::size_of::<SortTuple<'mcx>>() as i64;
    state.freemem(old_memtuples_space);
    state.memtuples = PgVec::new_in(mcx);

    // Initialize the slab allocator (one slot per input tape + one for the
    // last-returned tuple). For pass-by-val Datums no slab is needed.
    if state.base.tuples {
        init_slab_allocator(state, state.nOutputTapes + 1)?;
    } else {
        init_slab_allocator(state, 0)?;
    }

    // Allocate the heap memtuples array (one tuple per input tape).
    state.memtupsize = state.nOutputTapes;
    state.memtuples = vec_with_capacity_in(mcx, state.nOutputTapes as usize)?;
    state.usemem(state.nOutputTapes as i64 * core::mem::size_of::<SortTuple<'mcx>>() as i64);

    // Use all remaining memory for tape buffers; redistributed each pass.
    state.tape_buffer_mem = state.availMem as usize;
    state.usemem(state.tape_buffer_mem as i64);

    loop {
        // Start a new pass when all input runs have been consumed.
        if state.nInputRuns == 0 {
            // Close the old, emptied input tapes.
            if state.nInputTapes > 0 {
                for tapenum in 0..state.nInputTapes as usize {
                    let t = state.inputTapes[tapenum];
                    let tapeset = state.tapeset.as_mut().expect("mergeruns: no tapeset");
                    logtape::logical_tape_close(tapeset, t);
                }
            }

            // Previous pass's outputs become next pass's inputs.
            state.inputTapes = core::mem::replace(&mut state.outputTapes, PgVec::new_in(mcx));
            state.nInputTapes = state.nOutputTapes;
            state.nInputRuns = state.nOutputRuns;

            // Reset output tape variables (tapes created on demand).
            state.outputTapes = PgVec::new_in(mcx);
            state.nOutputTapes = 0;
            state.nOutputRuns = 0;

            // Redistribute tape-buffer memory among the new input/output tapes.
            let input_buffer_size = merge_read_buffer_size(
                state.tape_buffer_mem as i64,
                state.nInputTapes,
                state.nInputRuns,
                state.maxTapes,
            );

            // Prepare the new input tapes for the merge pass.
            for tapenum in 0..state.nInputTapes as usize {
                let t = state.inputTapes[tapenum];
                let tapeset = state.tapeset.as_mut().expect("mergeruns: no tapeset");
                logtape::logical_tape_rewind_for_read(tapeset, t, input_buffer_size as usize)?;
            }

            // If one run left on each input tape and no materialization needed,
            // do the final merge on-the-fly.
            if (state.base.sortopt & TUPLESORT_RANDOMACCESS) == 0
                && state.nInputRuns <= state.nInputTapes
                && !state.worker()
            {
                let tapeset = state.tapeset.as_mut().expect("mergeruns: no tapeset");
                logtape::logical_tape_set_forget_free_space(tapeset);
                beginmerge(state)?;
                state.status = TupSortStatus::FinalMerge;
                return Ok(());
            }
        }

        // Select an output tape, then merge one run from each input tape.
        selectnewtape(state)?;
        mergeonerun(state)?;

        // If inputs are empty and we output only one run, we're done.
        if state.nInputRuns == 0 && state.nOutputRuns <= 1 {
            break;
        }
    }

    // Done: the result is a single run on a single tape.
    let result = state.outputTapes[0];
    state.result_tape = Some(result);
    if !state.worker() {
        let tapeset = state.tapeset.as_mut().expect("mergeruns: no tapeset");
        logtape::logical_tape_freeze(tapeset, result)?;
    } else {
        worker_freeze_result_tape(state)?;
    }
    state.status = TupSortStatus::SortedOnTape;

    // Close all the now-empty input tapes.
    for tapenum in 0..state.nInputTapes as usize {
        let t = state.inputTapes[tapenum];
        let tapeset = state.tapeset.as_mut().expect("mergeruns: no tapeset");
        logtape::logical_tape_close(tapeset, t);
    }
    Ok(())
}

/// `mergeonerun(state)` (tuplesort.c): merge one run from each input tape onto
/// `destTape`.
fn mergeonerun<'mcx>(state: &mut TuplesortStateImpl<'mcx>) -> PgResult<()> {
    beginmerge(state)?;
    debug_assert!(state.slabAllocatorUsed);

    let variant = state.variant;
    let sortopt = state.base.sortopt;
    let dest = state.destTape.expect("mergeonerun: no destTape");

    // Repeatedly extract the lowest tuple in the heap, write it out, replace it
    // with the next tuple from the same input tape.
    while state.memtupcount > 0 {
        let src_tape_index = state.memtuples[0].srctape;
        let src_tape = state.inputTapes[src_tape_index as usize];

        // WRITETUP(state, destTape, &memtuples[0]).
        {
            let stup0 = core::mem::replace(&mut state.memtuples[0], placeholder_tuple());
            let tapeset = state.tapeset.as_mut().expect("mergeonerun: no tapeset");
            let res = writetup(variant, sortopt, tapeset, dest, &stup0);
            state.memtuples[0] = stup0;
            res?;
        }

        // RELEASE_SLAB_SLOT: drop the written-out tuple body (owned model).
        // (handled when memtuples[0] is replaced below.)

        // Pull next tuple from the same tape and replace the heap top.
        match mergereadnext(state, src_tape)? {
            Some(mut stup) => {
                stup.srctape = src_tape_index;
                tuplesort_heap_replace_top(state, stup)?;
            }
            None => {
                tuplesort_heap_delete_top(state)?;
                state.nInputRuns -= 1;
            }
        }
    }

    // Write the end-of-run marker on the output tape.
    let tapeset = state.tapeset.as_mut().expect("mergeonerun: no tapeset");
    markrunend(tapeset, dest)?;
    Ok(())
}

/// `beginmerge(state)` (tuplesort.c): fill the merge heap with the first tuple
/// from each active input tape.
fn beginmerge<'mcx>(state: &mut TuplesortStateImpl<'mcx>) -> PgResult<()> {
    debug_assert!(state.memtupcount == 0);

    let active_tapes = state.nInputTapes.min(state.nInputRuns);

    for src_tape_index in 0..active_tapes {
        let src_tape = state.inputTapes[src_tape_index as usize];
        if let Some(mut tup) = mergereadnext(state, src_tape)? {
            tup.srctape = src_tape_index;
            tuplesort_heap_insert(state, tup)?;
        }
    }
    Ok(())
}

/// `mergereadnext(state, srcTape, stup)` (tuplesort.c): read the next tuple from
/// one merge input tape. Returns `None` on end-of-run (zero length word).
fn mergereadnext<'mcx>(
    state: &mut TuplesortStateImpl<'mcx>,
    src_tape: usize,
) -> PgResult<Option<SortTuple<'mcx>>> {
    let variant = state.variant;
    let mcx = state.mcx();
    // Disjoint field borrows: `base` (shared) is a distinct field from `tapeset`
    // (mutable), so the compiler allows them simultaneously — no `unsafe`.
    let TuplesortStateImpl { base, tapeset, .. } = state;
    let tapeset = tapeset.as_mut().expect("mergereadnext: no tapeset");

    // tuplen = getlen(srcTape, true); if 0 -> EOF.
    let tuplen = getlen(tapeset, src_tape, true)?;
    if tuplen == 0 {
        return Ok(None);
    }
    // READTUP(state, stup, srcTape, tuplen).
    let stup = readtup(variant, base, mcx, tapeset, src_tape, tuplen)?;
    Ok(Some(stup))
}

/// `getlen(tape, eofOK)` (tuplesort.c): read the next length word; `eofOK`
/// tolerates a zero (end-of-run) word, otherwise it is an error.
fn getlen<'mcx>(tapeset: &mut LogicalTapeSet<'mcx>, tape: usize, eof_ok: bool) -> PgResult<u32> {
    let mut buf = [0u8; LEN_WORD_SIZE];
    let n = logtape::logical_tape_read(tapeset, tape, &mut buf)?;
    if n != LEN_WORD_SIZE {
        return Err(PgError::error("unexpected end of tape"));
    }
    let len = u32::from_ne_bytes(buf);
    if len == 0 && !eof_ok {
        return Err(PgError::error("unexpected end of data"));
    }
    Ok(len)
}

/// `markrunend(tape)` (tuplesort.c): write the zero-length end-of-run marker.
fn markrunend<'mcx>(tapeset: &mut LogicalTapeSet<'mcx>, tape: usize) -> PgResult<()> {
    let len: u32 = 0;
    logtape::logical_tape_write(tapeset, tape, &len.to_ne_bytes())
}

/// `worker_nomergeruns(state)` (tuplesort.c) — parallel worker, F3 seam-panic.
fn worker_nomergeruns<'mcx>(_state: &mut TuplesortStateImpl<'mcx>) -> PgResult<()> {
    panic!("tuplesort: worker_nomergeruns (parallel sort) not yet ported (tuplesort.c, F3)")
}

/// `worker_freeze_result_tape(state)` (tuplesort.c) — parallel worker, F3
/// seam-panic. Only reached from `mergeruns` on the `WORKER` path; the serial
/// (gate-critical) path takes the `LogicalTapeFreeze` branch instead.
fn worker_freeze_result_tape<'mcx>(_state: &mut TuplesortStateImpl<'mcx>) -> PgResult<()> {
    panic!("tuplesort: worker_freeze_result_tape (parallel sort) not yet ported (tuplesort.c, F3)")
}

/// `leader_takeover_tapes(state)` (tuplesort.c) — parallel leader, F3 seam-panic.
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

// ===========================================================================
// tuplesortvariants.c — the heap + datum begin entry points + put/get seams.
//
// KEY COST (the F3 blocker): each `begin_*` builds its `base.sortKeys` / `arg`
// inside the engine bundle's OWN context (the C `MemoryContextSwitchTo(
// base->maincontext)`). `OwnedSort::try_new`'s build closure is universal over
// `'mcx`, so it cannot capture a borrow of the caller's `'mcx`-lifetimed
// `TupleDesc`/params. We therefore snapshot the caller params into
// lifetime-free owned values (`std::vec::Vec`, plain scalars) BEFORE the
// closure, then rebuild everything (TupleDesc clone, SortSupport setup) inside
// the closure over the bundle's own `sx` arena — a faithful deep copy
// (`base->arg = tupDesc; /* assume we need not copy tupDesc */` becomes an
// explicit clone into the sort context, behaviour-preserving).
// ===========================================================================

/// A lifetime-free snapshot of a caller `TupleDesc` (the fields a sort needs).
/// `constr` is not carried — a sort `TupleDesc` (a plan result type) never has
/// catalog constraints, and the sort never reads them; we assert that here.
struct TupleDescSnapshot {
    natts: i32,
    tdtypeid: Oid,
    tdtypmod: i32,
    tdrefcount: i32,
    attrs: std::vec::Vec<FormData_pg_attribute>,
    compact_attrs: std::vec::Vec<CompactAttribute>,
}

impl TupleDescSnapshot {
    fn capture(td: &TupleDescData<'_>) -> PgResult<Self> {
        if td.constr.is_some() {
            return Err(PgError::error(
                "tuplesort: a sort TupleDesc unexpectedly carries constraints",
            ));
        }
        Ok(TupleDescSnapshot {
            natts: td.natts,
            tdtypeid: td.tdtypeid,
            tdtypmod: td.tdtypmod,
            tdrefcount: td.tdrefcount,
            attrs: td.attrs.iter().copied().collect(),
            compact_attrs: td.compact_attrs.iter().copied().collect(),
        })
    }

    /// Rebuild a `TupleDescData<'sx>` in the bundle's own context.
    fn rebuild<'sx>(&self, sx: Mcx<'sx>) -> PgResult<TupleDescData<'sx>> {
        Ok(TupleDescData {
            natts: self.natts,
            tdtypeid: self.tdtypeid,
            tdtypmod: self.tdtypmod,
            tdrefcount: self.tdrefcount,
            constr: None,
            compact_attrs: mcx::slice_in(sx, &self.compact_attrs)?,
            attrs: mcx::slice_in(sx, &self.attrs)?,
        })
    }
}

/// `tuplesort_begin_heap(tupDesc, nkeys, attNums, sortOperators, sortCollations,
/// nullsFirstFlags, workMem, coordinate=NULL, sortopt)` (tuplesortvariants.c).
///
/// Returns the carrier the consumers see; the engine bundle is built in its own
/// context with a deep-cloned `tupDesc` + per-column SortSupport.
fn tuplesort_begin_heap_state(
    tup_desc: &TupleDescData<'_>,
    nkeys: i32,
    att_nums: &[AttrNumber],
    sort_operators: &[Oid],
    sort_collations: &[Oid],
    nulls_first_flags: &[bool],
    work_mem: i32,
    sortopt: i32,
) -> PgResult<OwnedSort> {
    debug_assert!(nkeys > 0);

    // Snapshot the caller params (lifetime-free) for the universal closure.
    let snap = TupleDescSnapshot::capture(tup_desc)?;
    let att_nums: std::vec::Vec<AttrNumber> = att_nums.to_vec();
    let sort_operators: std::vec::Vec<Oid> = sort_operators.to_vec();
    let sort_collations: std::vec::Vec<Oid> = sort_collations.to_vec();
    let nulls_first_flags: std::vec::Vec<bool> = nulls_first_flags.to_vec();

    OwnedSort::try_new(MemoryContext::new("TupleSort sort"), move |sx| {
        let mut state = tuplesort_begin_common(sx, work_mem, sortopt, SortVariantKind::Heap)?;

        state.base.nKeys = nkeys;
        // base.removeabbrev/comparetup/writetup/readtup are the variant tag.
        state.base.haveDatum1 = true;
        // base->arg = tupDesc (deep clone into the sort context).
        let tupdesc = snap.rebuild(sx)?;

        // Prepare SortSupport data for each column.
        let mut sort_keys: PgVec<'_, SortSupportData<'_>> = vec_with_capacity_in(sx, nkeys as usize)?;
        for i in 0..nkeys as usize {
            debug_assert!(att_nums[i] != 0);
            debug_assert!(sort_operators[i] != 0);

            let mut sort_key = SortSupportData::new(sx);
            sort_key.ssup_collation = sort_collations[i];
            sort_key.ssup_nulls_first = nulls_first_flags[i];
            sort_key.ssup_attno = att_nums[i];
            // Convey if abbreviation optimization is applicable in principle.
            sort_key.abbreviate = i == 0 && state.base.haveDatum1;

            backend_utils_sort_sortsupport_seams::prepare_sort_support_from_ordering_op::call(
                sort_operators[i],
                &mut sort_key,
            )?;
            sort_keys.push(sort_key);
        }

        // The "onlyKey" optimization cannot be used with abbreviated keys.
        if nkeys == 1 && sort_keys[0].abbrev_converter.is_none() {
            state.base.onlyKey = Some(0);
        }

        state.base.arg = SortVariantArg::Heap { tupDesc: tupdesc };
        state.base.sortKeys = sort_keys;

        Ok(state)
    })
}

/// `tuplesort_begin_datum(datumType, sortOperator, sortCollation, nullsFirstFlag,
/// workMem, coordinate=NULL, sortopt)` (tuplesortvariants.c).
fn tuplesort_begin_datum_state(
    datum_type: Oid,
    sort_operator: Oid,
    sort_collation: Oid,
    nulls_first_flag: bool,
    work_mem: i32,
    sortopt: i32,
) -> PgResult<OwnedSort> {
    // lookup necessary attributes of the datum type (outside the closure — the
    // seam call is lifetime-agnostic).
    let (typlen, typbyval) =
        backend_utils_cache_lsyscache_seams::get_typlenbyval::call(datum_type)?;

    OwnedSort::try_new(MemoryContext::new("TupleSort sort"), move |sx| {
        let mut state = tuplesort_begin_common(sx, work_mem, sortopt, SortVariantKind::Datum)?;

        state.base.nKeys = 1; // always a one-column sort
        state.base.haveDatum1 = true;
        // arg->datumType / arg->datumTypeLen; base->tuples = !typbyval.
        state.base.arg = SortVariantArg::Datum {
            datumType: datum_type,
            datumTypeLen: typlen,
        };
        state.base.tuples = !typbyval;

        // Prepare SortSupport data (single key).
        let mut sort_key = SortSupportData::new(sx);
        sort_key.ssup_collation = sort_collation;
        sort_key.ssup_nulls_first = nulls_first_flag;
        // Abbreviation is possible here only for by-reference types.
        sort_key.abbreviate = !typbyval;

        backend_utils_sort_sortsupport_seams::prepare_sort_support_from_ordering_op::call(
            sort_operator,
            &mut sort_key,
        )?;

        // The "onlyKey" optimization cannot be used with abbreviated keys.
        if sort_key.abbrev_converter.is_none() {
            state.base.onlyKey = Some(0);
        }

        let mut sort_keys: PgVec<'_, SortSupportData<'_>> = vec_with_capacity_in(sx, 1)?;
        sort_keys.push(sort_key);
        state.base.sortKeys = sort_keys;

        Ok(state)
    })
}

/// A lifetime-free snapshot of a [`SortSupportData`]. Every field except
/// `ssup_cxt` is already lifetime-free (`Copy` registry tokens / scalars); the
/// snapshot drops the context and the rebuild re-homes it to the engine arena.
#[derive(Clone, Copy)]
struct SortSupportSnapshot {
    ssup_collation: Oid,
    ssup_reverse: bool,
    ssup_nulls_first: bool,
    ssup_attno: AttrNumber,
    abbreviate: bool,
    comparator: Option<types_sortsupport::SortComparatorId>,
    abbrev_converter: Option<types_sortsupport::AbbrevConverterId>,
    abbrev_abort: Option<types_sortsupport::AbbrevAbortId>,
    abbrev_full_comparator: Option<types_sortsupport::SortComparatorId>,
}

impl SortSupportSnapshot {
    fn capture(s: &SortSupportData<'_>) -> Self {
        SortSupportSnapshot {
            ssup_collation: s.ssup_collation,
            ssup_reverse: s.ssup_reverse,
            ssup_nulls_first: s.ssup_nulls_first,
            ssup_attno: s.ssup_attno,
            abbreviate: s.abbreviate,
            comparator: s.comparator,
            abbrev_converter: s.abbrev_converter,
            abbrev_abort: s.abbrev_abort,
            abbrev_full_comparator: s.abbrev_full_comparator,
        }
    }

    fn rebuild<'sx>(&self, sx: Mcx<'sx>) -> SortSupportData<'sx> {
        SortSupportData {
            ssup_cxt: sx,
            ssup_collation: self.ssup_collation,
            ssup_reverse: self.ssup_reverse,
            ssup_nulls_first: self.ssup_nulls_first,
            ssup_attno: self.ssup_attno,
            abbreviate: self.abbreviate,
            comparator: self.comparator,
            abbrev_converter: self.abbrev_converter,
            abbrev_abort: self.abbrev_abort,
            abbrev_full_comparator: self.abbrev_full_comparator,
        }
    }
}

/// Carry a caller-`'mcx` [`Relation`] into the engine's self-owned arena.
///
/// The index sort engine is self-owning over its own context (`OwnedSort`), so a
/// caller-lifetimed handle cannot be moved into the `for<'sx>` build closure.
/// The relation's data outlives the sort (its relcache cell `Rc` pins the
/// allocation against eviction for as long as the handle is held — exactly the C
/// `Relation` pointer's lifetime), so extending the apparent lifetime is sound.
/// This mirrors the established seam transmute pattern used by `seam_puttupleslot`
/// / `seam_putdatum` in this crate.
///
/// SAFETY: the returned handle (and its clone of the cell `Rc`) keep the
/// relation's backing allocation alive for the engine's whole life; nothing from
/// the relation is mutated, and the handle is dropped when the engine bundle is.
unsafe fn relation_into_engine<'sx>(rel: &Relation<'_>) -> Relation<'sx> {
    // alias() bumps the cell refcount (a second live `Relation *`); re-tie its
    // phantom lifetime to the engine arena.
    core::mem::transmute::<Relation<'_>, Relation<'sx>>(rel.alias())
}

/// `tuplesort_begin_index_btree(heapRel, indexRel, enforceUnique,
/// uniqueNullsNotDistinct, workMem, coordinate=NULL, sortopt)`
/// (tuplesortvariants.c).
fn tuplesort_begin_index_btree_state(
    mcx: Mcx<'_>,
    heap_rel: &Relation<'_>,
    index_rel: &Relation<'_>,
    enforce_unique: bool,
    unique_nulls_not_distinct: bool,
    work_mem: i32,
    sortopt: i32,
) -> PgResult<OwnedSort> {
    // nKeys = IndexRelationGetNumberOfKeyAttributes(indexRel).
    let nkeys = index_rel.indnkeyatts();

    // Snapshot the index descriptor for the engine's hot paths.
    let index_desc_snap = TupleDescSnapshot::capture(&index_rel.rd_att)?;

    // indexScanKey = _bt_mkscankey(indexRel, NULL): one scankey per key column.
    let index_scan_key = backend_access_nbtree_core_seams::bt_mkscankey::call(index_rel, None)?
        .ok_or_else(|| PgError::error("tuplesort_begin_index_btree: _bt_mkscankey returned NULL"))?;

    // Prepare SortSupport data for each column (against the live caller relation),
    // then snapshot to lifetime-free for the universal build closure.
    let mut sort_key_snaps: std::vec::Vec<SortSupportSnapshot> =
        std::vec::Vec::with_capacity(nkeys as usize);
    for i in 0..nkeys as usize {
        let scan_key = &index_scan_key.scankeys[i];
        let mut sort_key = SortSupportData::new(mcx);
        sort_key.ssup_collation = scan_key.sk_collation;
        sort_key.ssup_nulls_first =
            (scan_key.sk_flags & types_scan::scankey::SK_BT_NULLS_FIRST) != 0;
        sort_key.ssup_attno = scan_key.sk_attno;
        // Convey if abbreviation optimization is applicable in principle.
        sort_key.abbreviate = i == 0; // haveDatum1 is always true here.
        debug_assert!(sort_key.ssup_attno != 0);

        let reverse = (scan_key.sk_flags & types_scan::scankey::SK_BT_DESC) != 0;
        backend_utils_sort_sortsupport_seams::prepare_sort_support_from_index_rel::call(
            index_rel,
            reverse,
            &mut sort_key,
        )?;
        sort_key_snaps.push(SortSupportSnapshot::capture(&sort_key));
    }

    // SAFETY: extend the relation handles' lifetime into the engine arena (see
    // `relation_into_engine`).
    let heap_rel_engine: Relation<'static> = unsafe { relation_into_engine(heap_rel) };
    let index_rel_engine: Relation<'static> = unsafe { relation_into_engine(index_rel) };

    OwnedSort::try_new(MemoryContext::new("TupleSort sort"), move |sx| {
        let mut state =
            tuplesort_begin_common(sx, work_mem, sortopt, SortVariantKind::IndexBtree)?;

        state.base.nKeys = nkeys;
        state.base.haveDatum1 = true;

        let index_desc = index_desc_snap.rebuild(sx)?;
        let mut sort_keys: PgVec<'_, SortSupportData<'_>> =
            vec_with_capacity_in(sx, nkeys as usize)?;
        for snap in &sort_key_snaps {
            sort_keys.push(snap.rebuild(sx));
        }

        // SAFETY: re-tie the engine-bound relation aliases to this closure's `sx`.
        let heap_rel: Relation<'_> = unsafe { core::mem::transmute(heap_rel_engine) };
        let index_rel: Relation<'_> = unsafe { core::mem::transmute(index_rel_engine) };

        state.base.arg = SortVariantArg::IndexBtree {
            index: TuplesortIndexArg {
                heapRel: heap_rel,
                indexRel: index_rel,
                indexDesc: index_desc,
            },
            enforceUnique: enforce_unique,
            uniqueNullsNotDistinct: unique_nulls_not_distinct,
        };
        state.base.sortKeys = sort_keys;

        Ok(state)
    })
}

/// `tuplesort_begin_index_hash(heapRel, indexRel, high_mask, low_mask,
/// max_buckets, workMem, coordinate=NULL, sortopt)` (tuplesortvariants.c).
fn tuplesort_begin_index_hash_state(
    _mcx: Mcx<'_>,
    heap_rel: &Relation<'_>,
    index_rel: &Relation<'_>,
    high_mask: u32,
    low_mask: u32,
    max_buckets: u32,
    work_mem: i32,
    sortopt: i32,
) -> PgResult<OwnedSort> {
    let index_desc_snap = TupleDescSnapshot::capture(&index_rel.rd_att)?;
    let heap_rel_engine: Relation<'static> = unsafe { relation_into_engine(heap_rel) };
    let index_rel_engine: Relation<'static> = unsafe { relation_into_engine(index_rel) };

    OwnedSort::try_new(MemoryContext::new("TupleSort sort"), move |sx| {
        let mut state =
            tuplesort_begin_common(sx, work_mem, sortopt, SortVariantKind::IndexHash)?;

        state.base.nKeys = 1; // Only one sort column, the hash code.
        state.base.haveDatum1 = true;
        // index_hash builds no SortSupport array (comparetup uses the masks).

        let index_desc = index_desc_snap.rebuild(sx)?;
        // SAFETY: re-tie the engine-bound relation aliases to `sx`.
        let heap_rel: Relation<'_> = unsafe { core::mem::transmute(heap_rel_engine) };
        let index_rel: Relation<'_> = unsafe { core::mem::transmute(index_rel_engine) };

        state.base.arg = SortVariantArg::IndexHash {
            index: TuplesortIndexArg {
                heapRel: heap_rel,
                indexRel: index_rel,
                indexDesc: index_desc,
            },
            high_mask,
            low_mask,
            max_buckets,
        };

        Ok(state)
    })
}

/// `tuplesort_begin_index_gist(heapRel, indexRel, workMem, coordinate=NULL,
/// sortopt)` (tuplesortvariants.c). Shares the btree arg with both uniqueness
/// flags `false`; keys the sort by the opclass' sortsupport
/// (`PrepareSortSupportFromGistIndexRel`), per-column collation from
/// `rd_indcollation`.
fn tuplesort_begin_index_gist_state(
    mcx: Mcx<'_>,
    heap_rel: &Relation<'_>,
    index_rel: &Relation<'_>,
    work_mem: i32,
    sortopt: i32,
) -> PgResult<OwnedSort> {
    let nkeys = index_rel.indnkeyatts();
    let index_desc_snap = TupleDescSnapshot::capture(&index_rel.rd_att)?;

    // Prepare SortSupport per column against the live relation, then snapshot.
    let mut sort_key_snaps: std::vec::Vec<SortSupportSnapshot> =
        std::vec::Vec::with_capacity(nkeys as usize);
    for i in 0..nkeys as usize {
        let mut sort_key = SortSupportData::new(mcx);
        sort_key.ssup_collation = index_rel.rd_indcollation[i];
        sort_key.ssup_nulls_first = false;
        sort_key.ssup_attno = (i + 1) as AttrNumber;
        // Convey if abbreviation optimization is applicable in principle.
        sort_key.abbreviate = i == 0; // haveDatum1 is always true here.
        debug_assert!(sort_key.ssup_attno != 0);

        // Look for a sort support function.
        backend_utils_sort_sortsupport_seams::prepare_sort_support_from_gist_index_rel::call(
            index_rel,
            &mut sort_key,
        )?;
        sort_key_snaps.push(SortSupportSnapshot::capture(&sort_key));
    }

    let heap_rel_engine: Relation<'static> = unsafe { relation_into_engine(heap_rel) };
    let index_rel_engine: Relation<'static> = unsafe { relation_into_engine(index_rel) };

    OwnedSort::try_new(MemoryContext::new("TupleSort sort"), move |sx| {
        // GiST shares the index_btree subcase comparetup/writetup/readtup.
        let mut state =
            tuplesort_begin_common(sx, work_mem, sortopt, SortVariantKind::IndexBtree)?;

        state.base.nKeys = nkeys;
        state.base.haveDatum1 = true;

        let index_desc = index_desc_snap.rebuild(sx)?;
        let mut sort_keys: PgVec<'_, SortSupportData<'_>> =
            vec_with_capacity_in(sx, nkeys as usize)?;
        for snap in &sort_key_snaps {
            sort_keys.push(snap.rebuild(sx));
        }

        // SAFETY: re-tie the engine-bound relation aliases to `sx`.
        let heap_rel: Relation<'_> = unsafe { core::mem::transmute(heap_rel_engine) };
        let index_rel: Relation<'_> = unsafe { core::mem::transmute(index_rel_engine) };

        state.base.arg = SortVariantArg::IndexBtree {
            index: TuplesortIndexArg {
                heapRel: heap_rel,
                indexRel: index_rel,
                indexDesc: index_desc,
            },
            enforceUnique: false,
            uniqueNullsNotDistinct: false,
        };
        state.base.sortKeys = sort_keys;

        Ok(state)
    })
}

/// `tuplesort_putindextuplevalues(state, rel, self, values, isnull)`
/// (tuplesortvariants.c): form an index tuple from `values`/`isnull` with heap
/// TID `self`, set up `datum1` from the leading index column, and feed it in.
fn tuplesort_putindextuplevalues_impl<'mcx>(
    state: &mut TuplesortStateImpl<'mcx>,
    rel: &Relation<'mcx>,
    self_tid: types_tuple::heaptuple::ItemPointerData,
    values: &[Datum<'mcx>],
    isnull: &[bool],
) -> PgResult<()> {
    let mcx = state.mcx();

    // stup.tuple = index_form_tuple_context(RelationGetDescr(rel), values, isnull,
    // base->tuplecontext); tuple->t_tid = *self. The seam forms the tuple and
    // stamps the heap TID in one shot.
    let tuple: PgVec<'mcx, u8> = backend_access_common_indextuple_seams::index_form_tuple::call(
        mcx, rel, values, isnull, self_tid,
    )?;

    // set up first-column key value (index_getattr(tuple, 1,
    // RelationGetDescr(arg->indexRel), &stup.isnull1)).
    let (datum1, isnull1) = {
        let arg = index_arg(&state.base.arg)?;
        index_getattr(mcx, &tuple, 1, &arg.indexDesc)?
    };

    // tuplen = GetMemoryChunkSpace(tuple): the engine's mem accounting charges
    // the stored byte size (the bump-context fast path uses the same value).
    let tuplen = tuple.len() as i64;
    let use_abbrev = !state.base.sortKeys.is_empty()
        && state.base.sortKeys[0].abbrev_converter.is_some()
        && !isnull1;

    let stup = SortTuple {
        tuple: Some(TupleBody::Index(tuple)),
        datum1,
        isnull1,
        srctape: 0,
    };
    tuplesort_puttuple_common(state, stup, use_abbrev, tuplen)
}

/// `tuplesort_getindextuple(state, forward)` (tuplesortvariants.c): fetch the
/// next sorted IndexTuple's on-disk bytes; `None` at end of sort.
fn tuplesort_getindextuple_impl<'mcx>(
    state: &mut TuplesortStateImpl<'mcx>,
    forward: bool,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let mcx = state.mcx();
    match tuplesort_gettuple_common(state, forward)? {
        Some(SortTuple {
            tuple: Some(TupleBody::Index(bytes)),
            ..
        }) => Ok(Some(mcx::slice_in(mcx, &bytes)?)),
        _ => Ok(None),
    }
}

/// `tuplesort_puttupleslot(state, slot)` (tuplesortvariants.c): copy the slot's
/// tuple into sort storage as a `MinimalTuple` and set up `datum1` from the
/// leading sort column.
fn tuplesort_puttupleslot_impl<'mcx>(
    state: &mut TuplesortStateImpl<'mcx>,
    slot: &TupleTableSlot<'mcx>,
) -> PgResult<()> {
    let mcx = state.mcx();
    let tup_desc = match &state.base.arg {
        SortVariantArg::Heap { tupDesc } => tupDesc,
        _ => {
            return Err(PgError::error(
                "tuplesort_puttupleslot: arg is not a TupleDesc",
            ))
        }
    };

    // copy the tuple into sort storage: ExecCopySlotMinimalTuple(slot). The
    // owned slot carries the deformed value/null arrays; form a MinimalTuple
    // over them in the sort context.
    let tuple: FormedMinimalTuple<'mcx> = heaptuple::heap_form_minimal_tuple(
        mcx,
        tup_desc,
        &slot.tts_values,
        &slot.tts_isnull,
        0,
    )
    .map_err(|e| PgError::error(format!("tuplesort_puttupleslot heap_form_minimal_tuple: {e:?}")))?;

    // set up first-column key value (heap_getattr(&htup, sortKeys[0].ssup_attno,
    // tupDesc, &isnull1)): the deformed value is already in the slot's array.
    let attno = state.base.sortKeys[0].ssup_attno;
    let idx = (attno as usize).saturating_sub(1);
    let datum1 = slot.tts_values[idx].clone_in(mcx)?;
    let isnull1 = slot.tts_isnull[idx];

    let tuplen = tuple.data.len() as i64;
    let use_abbrev = state.base.sortKeys[0].abbrev_converter.is_some() && !isnull1;

    let stup = SortTuple {
        tuple: Some(TupleBody::Minimal(tuple)),
        datum1,
        isnull1,
        srctape: 0,
    };
    tuplesort_puttuple_common(state, stup, use_abbrev, tuplen)
}

/// `tuplesort_putdatum(state, val, isNull)` (tuplesortvariants.c).
fn tuplesort_putdatum_impl<'mcx>(
    state: &mut TuplesortStateImpl<'mcx>,
    val: Datum<'mcx>,
    is_null: bool,
) -> PgResult<()> {
    let mcx = state.mcx();
    let tuples = state.base.tuples;

    let stup = if is_null || !tuples {
        // Pass-by-value types or null values stored directly in datum1.
        let datum1 = if !is_null {
            val.clone_in(mcx)?
        } else {
            Datum::ByVal(0) // zeroed representation for NULLs
        };
        SortTuple {
            tuple: None, // no separate storage
            datum1,
            isnull1: is_null,
            srctape: 0,
        }
    } else {
        // Non-null pass-by-reference: copy into memory we control (datumCopy).
        // The copied value is the canonical copy; datum1 mirrors it (or the
        // abbreviated key when abbreviation is in play).
        let copy = val.clone_in(mcx)?;
        SortTuple {
            tuple: Some(TupleBody::Datum(copy.clone_in(mcx)?)),
            datum1: copy,
            isnull1: false,
            srctape: 0,
        }
    };

    let use_abbrev =
        tuples && state.base.sortKeys[0].abbrev_converter.is_some() && !is_null;
    tuplesort_puttuple_common(state, stup, use_abbrev, 0)
}

/// `tuplesort_gettupleslot(state, forward, copy, slot, abbrev=NULL)`
/// (tuplesortvariants.c): fetch the next tuple into `slot`; `false` (slot
/// cleared) at end of sort. The owned slot is filled by deforming the fetched
/// MinimalTuple into its value/null arrays (the C `ExecStoreMinimalTuple`).
fn tuplesort_gettupleslot_impl<'mcx>(
    state: &mut TuplesortStateImpl<'mcx>,
    forward: bool,
    _copy: bool,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    let mcx = state.mcx();
    let stup = tuplesort_gettuple_common(state, forward)?;

    match stup {
        Some(SortTuple {
            tuple: Some(TupleBody::Minimal(mtup)),
            ..
        }) => {
            // ExecStoreMinimalTuple(mtup, slot, copy). `copy` is a no-op here —
            // the fetched tuple is already an owned copy out of the engine arena.
            store_minimal_into_slot(mcx, slot, mtup)?;
            Ok(true)
        }
        // Non-minimal body / no body at end of sort: clear the slot.
        _ => {
            clear_slot(slot.base_mut());
            Ok(false)
        }
    }
}

/// `tuplesort_getdatum(state, forward, copy, &val, &isNull, abbrev=NULL)`
/// (tuplesortvariants.c): returns `(found, val, isNull)`.
fn tuplesort_getdatum_impl<'mcx>(
    state: &mut TuplesortStateImpl<'mcx>,
    forward: bool,
    _copy: bool,
) -> PgResult<(bool, Datum<'mcx>, bool)> {
    let mcx = state.mcx();
    let tuples = state.base.tuples;
    let stup = match tuplesort_gettuple_common(state, forward)? {
        None => return Ok((false, Datum::ByVal(0), false)),
        Some(s) => s,
    };

    if stup.isnull1 || !tuples {
        Ok((true, stup.datum1.clone_in(mcx)?, stup.isnull1))
    } else {
        // use stup.tuple because stup.datum1 may be an abbreviation.
        let val = match &stup.tuple {
            Some(TupleBody::Datum(d)) => d.clone_in(mcx)?,
            _ => {
                return Err(PgError::error(
                    "tuplesort_getdatum: by-ref tuple body missing",
                ))
            }
        };
        Ok((true, val, false))
    }
}

/// Deform a `MinimalTuple` into the slot's value/null arrays + mark it stored
/// (the owned-model `ExecStoreMinimalTuple` over a virtual slot). The slot's
/// descriptor governs the column count.
fn store_minimal_into_slot<'mcx>(
    mcx: Mcx<'mcx>,
    slot: &mut SlotData<'mcx>,
    mtup: FormedMinimalTuple<'mcx>,
) -> PgResult<()> {
    match slot {
        // `ExecStoreMinimalTuple` into a minimal-tuple slot (the Sort node's
        // result slot, created with &TTSOpsMinimalTuple): store the MinimalTuple
        // into mslot->mintuple + set up the minhdr/tuple heap-tuple view and clear
        // tts_nvalid (lazy deform). This is what makes a later
        // ExecCopySlotHeapTuple / get_minimal_tuple read the *current* tuple — a
        // virtual-style deform-into-tts_values leaves mintuple stale.
        SlotData::Minimal(mslot) => {
            // C `tuplesort_gettupleslot` does `ExecStoreMinimalTuple(stup.tuple,
            // slot, false)`: the fetched tuple lives in the *tuplesort* memory
            // context (freed by `tuplesort_end`) and the slot borrows it with
            // `shouldFree=false`, so `tts_minimal_clear` never frees it. The
            // owned model cannot store a borrowed Box: `mslot.mintuple`'s drop
            // (in `tts_minimal_clear`, run at `ExecResetTupleTable`) always
            // deallocates through the Box's allocator — and that allocator is the
            // engine's `sortcontext`, already destroyed by `ExecEndSort`'s
            // `tuplesort_end`, giving a use-after-free in the context accounting.
            //
            // Faithful owned-model equivalent: copy the minimal tuple into the
            // *slot's own* memory context (which outlives the sort, exactly like
            // `tts_minimal_copyslot`'s `MemoryContextSwitchTo(dstslot->tts_mcxt)`)
            // and store it with `shouldFree=true`, so the slot owns it and frees
            // it from a live context. The slot's context is recovered from its
            // own charged member (the `tts_values` Vec's allocator), mirroring how
            // the engine recovers its context from `memtuples`.
            let slot_mcx: Mcx<'mcx> = *mslot.base.tts_values.allocator();
            let owned = mtup.clone_in(slot_mcx)?;
            // tts_minimal_clear(slot): drop any previously-stored tuple (from a
            // still-live context) before installing the new one.
            mslot.mintuple = None;
            mslot.tuple = None;
            mslot.base.mark_empty();

            mslot.base.mark_not_empty();
            mslot.base.tts_nvalid = 0;
            mslot.off = 0;
            mslot.mintuple = Some(owned);
            // mslot->minhdr / mslot->tuple = heap-tuple-shaped view over the body.
            let view = heaptuple::heap_tuple_from_minimal_tuple(
                slot_mcx,
                mslot.mintuple.as_ref().unwrap(),
            )?;
            mslot.minhdr = view.tuple.as_ref().clone();
            mslot.tuple = Some(view);
            // shouldFree=true: the slot owns this copy and frees it (from its own
            // live context) on the next clear.
            mslot.base.tts_flags |= types_slot::TTS_FLAG_SHOULDFREE;
            Ok(())
        }
        // Other slot kinds (e.g. a standalone virtual slot): deform the
        // MinimalTuple into the slot's value/null arrays (the owned virtual-slot
        // representation, ExecStoreVirtualTuple-equivalent).
        other => {
            let base = other.base_mut();
            let tup_desc = base
                .tts_tupleDescriptor
                .as_ref()
                .ok_or_else(|| PgError::error("tuplesort gettupleslot: slot has no descriptor"))?;
            let blob = heaptuple::flat::minimal_tuple_to_flat(mcx, &mtup).map_err(flat_err)?;
            let cols = heaptuple::flat::heap_deform_minimal_tuple_flat(mcx, &blob, tup_desc)
                .map_err(flat_err)?;

            let natts = tup_desc.natts as usize;
            let mut values: PgVec<'mcx, Datum<'mcx>> = vec_with_capacity_in(mcx, natts)?;
            let mut isnull: PgVec<'mcx, bool> = vec_with_capacity_in(mcx, natts)?;
            for (d, n) in cols.iter() {
                values.push(d.clone_in(mcx)?);
                isnull.push(*n);
            }
            base.tts_values = values;
            base.tts_isnull = isnull;
            base.tts_nvalid = tup_desc.natts as AttrNumber;
            base.mark_not_empty();
            Ok(())
        }
    }
}

/// `ExecClearTuple(slot)` over the owned slot: empty the payload arrays + mark
/// empty.
fn clear_slot(slot: &mut TupleTableSlot<'_>) {
    slot.tts_values.clear();
    slot.tts_isnull.clear();
    slot.mark_empty();
}

// ===========================================================================
// init_seams() — install this unit's inward `tuplesort_*` access-method seams.
//
// Installed (F3a): the variant-agnostic engine seams + the heap/datum-reachable
// begin entry points. F3b adds the INDEX variants (begin_index_btree/hash/gist,
// putindextuplevalues, getindextuple) — their variant comparetup/writetup/
// readtup/removeabbrev now dispatch to real bodies, and their begin entry points
// are installed here, retiring the 5 CONTRACT_RECONCILE_PENDING allowlist
// entries. (CLUSTER is still F4.)
// ===========================================================================

/// Install every inward seam this unit owns and can serve (F3a + F3b surface).
pub fn init_seams() {
    use backend_utils_sort_tuplesort_seams as sx;

    sx::tuplesort_begin_heap::set(seam_begin_heap);
    sx::tuplesort_begin_datum::set(seam_begin_datum);
    sx::tuplesort_set_bound::set(seam_set_bound);
    sx::tuplesort_puttupleslot::set(seam_puttupleslot);
    sx::tuplesort_putdatum::set(seam_putdatum);
    sx::tuplesort_performsort::set(seam_performsort);
    sx::tuplesort_gettupleslot::set(seam_gettupleslot);
    sx::tuplesort_getdatum::set(seam_getdatum);
    sx::tuplesort_get_stats::set(seam_get_stats);
    sx::tuplesort_end::set(seam_end);
    sx::tuplesort_rescan::set(seam_rescan);
    sx::tuplesort_reset::set(seam_reset);
    sx::tuplesort_used_bound::set(seam_used_bound);
    sx::tuplesort_puttupleslot_standalone::set(seam_puttupleslot_standalone);
    sx::tuplesort_gettupleslot_standalone::set(seam_gettupleslot_standalone);
    sx::tuplesort_markpos::set(seam_markpos);
    sx::tuplesort_restorepos::set(seam_restorepos);

    // F3b: the index sort variants.
    sx::tuplesort_begin_index_btree::set(seam_begin_index_btree);
    sx::tuplesort_begin_index_hash::set(seam_begin_index_hash);
    sx::tuplesort_begin_index_gist::set(seam_begin_index_gist);
    sx::tuplesort_putindextuplevalues::set(seam_putindextuplevalues);
    sx::tuplesort_getindextuple::set(seam_getindextuple);

    // Exported to the planner's cost model (cost_tuplesort in costsize.c). C
    // returns `int`; the planner uses it as a `double`, so the seam carries f64.
    backend_optimizer_path_costsize_seams::tuplesort_merge_order::set(
        seam_tuplesort_merge_order,
    );

    // The two `bool` GUC variables tuplesort.c owns. C reads them from the
    // variable itself (no ControlFile, no check/assign/show hooks); each lives
    // in a thread-local backing store here.
    //
    // `trace_sort` has a `guc-tables` slot, so the GUC machinery reaches its
    // store through the installed `GucVarAccessors` (the get/set path the
    // boot/SET/SHOW machinery uses to read and write the variable).
    use backend_utils_misc_guc_tables::{vars, GucVarAccessors};
    vars::trace_sort.install(GucVarAccessors {
        get: trace_sort_get,
        set: trace_sort_set,
    });

    // `optimize_bounded_sort` has no `guc-tables` slot in this port, so its
    // value is exposed through this unit's own read seam instead.
    sx::optimize_bounded_sort::set(optimize_bounded_sort_get);
}

fn seam_begin_index_btree<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rel: &Relation<'mcx>,
    index_rel: &Relation<'mcx>,
    enforce_unique: bool,
    unique_nulls_not_distinct: bool,
    work_mem: i32,
    sortopt: i32,
) -> PgResult<Tuplesortstate<'mcx>> {
    let owned = tuplesort_begin_index_btree_state(
        mcx,
        heap_rel,
        index_rel,
        enforce_unique,
        unique_nulls_not_distinct,
        work_mem,
        sortopt,
    )?;
    into_carrier(mcx, owned)
}

fn seam_begin_index_hash<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rel: &Relation<'mcx>,
    index_rel: &Relation<'mcx>,
    high_mask: u32,
    low_mask: u32,
    max_buckets: u32,
    work_mem: i32,
    sortopt: i32,
) -> PgResult<Tuplesortstate<'mcx>> {
    let owned = tuplesort_begin_index_hash_state(
        mcx,
        heap_rel,
        index_rel,
        high_mask,
        low_mask,
        max_buckets,
        work_mem,
        sortopt,
    )?;
    into_carrier(mcx, owned)
}

fn seam_begin_index_gist<'mcx>(
    mcx: Mcx<'mcx>,
    heap_rel: &Relation<'mcx>,
    index_rel: &Relation<'mcx>,
    work_mem: i32,
    sortopt: i32,
) -> PgResult<Tuplesortstate<'mcx>> {
    let owned = tuplesort_begin_index_gist_state(mcx, heap_rel, index_rel, work_mem, sortopt)?;
    into_carrier(mcx, owned)
}

fn seam_putindextuplevalues<'mcx>(
    state: &mut Tuplesortstate<'mcx>,
    rel: &Relation<'mcx>,
    self_tid: types_tuple::heaptuple::ItemPointerData,
    values: &[Datum<'mcx>],
    isnull: &[bool],
) -> PgResult<()> {
    with_sort_mut(state, |s| {
        // SAFETY: re-tie the relation / value slices to the engine's universal
        // `'mcx`. They are only READ (the tuple bytes + datum1 are cloned into the
        // engine arena), so no borrow escapes the call. Mirrors C `void *`
        // aliasing, like `seam_puttupleslot`.
        let rel: &Relation = unsafe { core::mem::transmute(rel) };
        let values: &[Datum] = unsafe { core::mem::transmute(values) };
        tuplesort_putindextuplevalues_impl(s, rel, self_tid, values, isnull)
    })
}

fn seam_getindextuple<'mcx>(
    state: &mut Tuplesortstate<'mcx>,
    forward: bool,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    with_sort_mut(state, |s| {
        let out = tuplesort_getindextuple_impl(s, forward)?;
        // SAFETY: re-tie the returned bytes' lifetime to the carrier's `'mcx`;
        // they were allocated in the engine bundle's context, which the carrier
        // keeps alive at least as long as the caller's `'mcx`.
        let out: Option<PgVec<'mcx, u8>> = unsafe { core::mem::transmute(out) };
        Ok(out)
    })
}

fn seam_begin_heap<'mcx>(
    mcx: Mcx<'mcx>,
    tup_desc: &TupleDescData<'mcx>,
    nkeys: i32,
    att_nums: &[AttrNumber],
    sort_operators: &[Oid],
    sort_collations: &[Oid],
    nulls_first_flags: &[bool],
    work_mem: i32,
    sortopt: i32,
) -> PgResult<Tuplesortstate<'mcx>> {
    let owned = tuplesort_begin_heap_state(
        tup_desc,
        nkeys,
        att_nums,
        sort_operators,
        sort_collations,
        nulls_first_flags,
        work_mem,
        sortopt,
    )?;
    into_carrier(mcx, owned)
}

fn seam_begin_datum<'mcx>(
    mcx: Mcx<'mcx>,
    datum_type: Oid,
    sort_operator: Oid,
    sort_collation: Oid,
    nulls_first_flag: bool,
    work_mem: i32,
    sortopt: i32,
) -> PgResult<Tuplesortstate<'mcx>> {
    let owned = tuplesort_begin_datum_state(
        datum_type,
        sort_operator,
        sort_collation,
        nulls_first_flag,
        work_mem,
        sortopt,
    )?;
    into_carrier(mcx, owned)
}

fn seam_set_bound<'mcx>(state: &mut Tuplesortstate<'mcx>, bound: i64) -> PgResult<()> {
    with_sort_mut(state, |s| {
        tuplesort_set_bound(s, bound);
        Ok(())
    })
}

fn seam_puttupleslot<'mcx>(
    state: &mut Tuplesortstate<'mcx>,
    slot: &TupleTableSlot,
) -> PgResult<()> {
    // The carrier's engine is `for<'mcx>`-universal; the slot is borrowed at the
    // caller's lifetime. Re-borrow it inside the universal closure: the slot's
    // payload is consumed (cloned) into the engine arena, so a transient
    // lifetime mismatch is sound (no borrow escapes).
    with_sort_mut(state, |s| {
        // SAFETY: re-tie the slot ref to the engine's universal `'mcx`. The slot
        // is only READ (cloned) here; nothing from `s` is stored into it, so no
        // borrow outlives the call. Mirrors the C `void *` aliasing.
        let slot: &TupleTableSlot = unsafe { core::mem::transmute(slot) };
        tuplesort_puttupleslot_impl(s, slot)
    })
}

fn seam_putdatum<'mcx>(
    state: &mut Tuplesortstate<'mcx>,
    val: Datum<'mcx>,
    is_null: bool,
) -> PgResult<()> {
    with_sort_mut(state, move |s| {
        // SAFETY: re-tie the value's lifetime to the engine's universal `'mcx`;
        // it is immediately cloned (datumCopy) into the engine arena.
        let val: Datum = unsafe { core::mem::transmute(val) };
        tuplesort_putdatum_impl(s, val, is_null)
    })
}

fn seam_performsort<'mcx>(state: &mut Tuplesortstate<'mcx>) -> PgResult<()> {
    with_sort_mut(state, tuplesort_performsort)
}

fn seam_gettupleslot<'mcx>(
    state: &mut Tuplesortstate<'mcx>,
    forward: bool,
    copy: bool,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    with_sort_mut(state, |s| {
        // SAFETY: re-tie the slot's lifetime to the engine's universal `'mcx`;
        // the engine writes freshly-allocated (engine-arena) values into the
        // slot, which lives at least as long as the carrier. Mirrors C aliasing.
        let slot: &mut SlotData = unsafe { core::mem::transmute(slot) };
        tuplesort_gettupleslot_impl(s, forward, copy, slot)
    })
}

fn seam_getdatum<'mcx>(
    state: &mut Tuplesortstate<'mcx>,
    forward: bool,
    copy: bool,
) -> PgResult<(bool, Datum<'mcx>, bool)> {
    with_sort_mut(state, |s| {
        let (found, val, isnull) = tuplesort_getdatum_impl(s, forward, copy)?;
        // SAFETY: re-tie the returned value's lifetime to the carrier's `'mcx`.
        // The value was allocated in the engine bundle's context, which the
        // carrier keeps alive at least as long as the caller's `'mcx`.
        let val: Datum<'mcx> = unsafe { core::mem::transmute(val) };
        Ok((found, val, isnull))
    })
}

fn seam_get_stats<'mcx>(state: &Tuplesortstate<'mcx>) -> TuplesortInstrumentation {
    // The seam contract is `&Tuplesortstate`; the read-only stats core computes
    // the same instrumentation the `&mut` path would, without persisting the
    // running-max fields (see `tuplesort_get_stats_ref`).
    with_sort(state, tuplesort_get_stats_ref)
}

fn seam_end<'mcx>(mut state: PgBox<'mcx, Tuplesortstate<'mcx>>) -> PgResult<()> {
    // `tuplesort_end` = `tuplesort_free` (close tape files via
    // LogicalTapeSetClose, release memtuples) + MemoryContextDelete. Run the
    // explicit free through the carrier first (so temp files are closed), then
    // drop the carrier (which drops the engine bundle + its context).
    with_sort_mut(&mut state, |s| {
        tuplesort_free(s);
    });
    drop(state);
    Ok(())
}

fn seam_rescan<'mcx>(state: &mut Tuplesortstate<'mcx>) -> PgResult<()> {
    with_sort_mut(state, tuplesort_rescan)
}

fn seam_markpos<'mcx>(state: &mut Tuplesortstate<'mcx>) -> PgResult<()> {
    with_sort_mut(state, tuplesort_markpos)
}

fn seam_restorepos<'mcx>(state: &mut Tuplesortstate<'mcx>) -> PgResult<()> {
    with_sort_mut(state, tuplesort_restorepos)
}

fn seam_reset<'mcx>(state: &mut Tuplesortstate<'mcx>) -> PgResult<()> {
    with_sort_mut(state, tuplesort_reset)
}

fn seam_used_bound<'mcx>(state: &Tuplesortstate<'mcx>) -> bool {
    with_sort(state, tuplesort_used_bound)
}

/// `tuplesort_puttupleslot` for a standalone [`SlotData`] (incremental sort's
/// group_pivot / transfer_tuple). The deformed value/null arrays live on the
/// slot's base header, so this delegates to the same impl as the pool form
/// through `slot.base()`.
fn seam_puttupleslot_standalone<'mcx>(
    state: &mut Tuplesortstate<'mcx>,
    slot: &SlotData<'mcx>,
) -> PgResult<()> {
    with_sort_mut(state, |s| {
        // SAFETY: re-tie the slot ref to the engine's universal `'mcx`. The slot
        // is only READ (cloned) here; nothing from `s` is stored into it, so no
        // borrow outlives the call. Mirrors the pool `seam_puttupleslot`.
        let base: &TupleTableSlot = unsafe { core::mem::transmute(slot.base()) };
        tuplesort_puttupleslot_impl(s, base)
    })
}

/// `tuplesort_gettupleslot` into a standalone [`SlotData`] (incremental sort's
/// transfer_tuple). Delegates through the slot's base header.
fn seam_gettupleslot_standalone<'mcx>(
    state: &mut Tuplesortstate<'mcx>,
    forward: bool,
    copy: bool,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    with_sort_mut(state, |s| {
        // SAFETY: re-tie the slot's lifetime to the engine's universal `'mcx`;
        // the engine writes freshly-allocated values into the slot, which lives
        // at least as long as the carrier. Mirrors the pool `seam_gettupleslot`.
        let slot: &mut SlotData = unsafe { core::mem::transmute(slot) };
        tuplesort_gettupleslot_impl(s, forward, copy, slot)
    })
}

#[cfg(test)]
mod tests;
