//! Seam declarations for the `backend-utils-sort-tuplesort` unit
//! (`utils/sort/tuplesort.c` + `utils/sort/tuplesortvariants.c`): the
//! `tuplesort_*` access method nodeSort drives.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. `Tuplesortstate` is type-erased
//! ([`types_nodes::Tuplesortstate`]); only the tuplesort owner downcasts.
//!
//! nodeSort never passes a parallel `SortCoordinate` (it always supplies the C
//! `NULL`), so the begin seams omit that parameter — the parallel-coordinated
//! path is a separate concern when its callers land (narrowest capability).

#![allow(non_snake_case)]

use types_core::{AttrNumber, Oid};
use types_error::PgResult;
use types_nodes::{TupleTableSlot, Tuplesortstate, TuplesortInstrumentation};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::{ItemPointerData, TupleDescData};

seam_core::seam!(
    /// `tuplesort_begin_index_hash(heapRel, indexRel, high_mask, low_mask,
    /// max_buckets, workMem, coordinate=NULL, sortopt)` (tuplesortvariants.c):
    /// begin a hash-index tuple sort keyed by bucket number (the masks feed
    /// `_hash_hashkey2bucket`). Allocates the sort state in `mcx`, fallible on
    /// OOM.
    pub fn tuplesort_begin_index_hash<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        heap_rel: &types_rel::Relation<'mcx>,
        index_rel: &types_rel::Relation<'mcx>,
        high_mask: u32,
        low_mask: u32,
        max_buckets: u32,
        work_mem: i32,
        sortopt: i32,
    ) -> PgResult<Tuplesortstate<'mcx>>
);

seam_core::seam!(
    /// `tuplesort_putindextuplevalues(state, rel, self, values, isnull)`
    /// (tuplesortvariants.c): form an index tuple from `values`/`isnull` with
    /// heap TID `self` and feed it into the sort. Can allocate, fallible.
    pub fn tuplesort_putindextuplevalues<'mcx>(
        state: &mut Tuplesortstate<'mcx>,
        rel: &types_rel::Relation<'mcx>,
        self_tid: ItemPointerData,
        values: &[Datum<'mcx>],
        isnull: &[bool],
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `tuplesort_getindextuple(state, forward=true)` (tuplesortvariants.c):
    /// fetch the next sorted IndexTuple as its on-disk bytes; `None` at end of
    /// sort. Can allocate, fallible.
    pub fn tuplesort_getindextuple<'mcx>(
        state: &mut Tuplesortstate<'mcx>,
        forward: bool,
    ) -> PgResult<Option<mcx::PgVec<'mcx, u8>>>
);

seam_core::seam!(
    /// `tuplesort_begin_heap(tupDesc, nkeys, attNums, sortOperators,
    /// sortCollations, nullsFirstFlags, workMem, coordinate=NULL, sortopt)`
    /// (tuplesortvariants.c): begin a multi-column heap-tuple sort. Allocates
    /// the sort state in `mcx` (C: palloc in `CurrentMemoryContext`), so
    /// fallible on OOM.
    pub fn tuplesort_begin_heap<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        tup_desc: &TupleDescData<'mcx>,
        nkeys: i32,
        att_nums: &[AttrNumber],
        sort_operators: &[Oid],
        sort_collations: &[Oid],
        nulls_first_flags: &[bool],
        work_mem: i32,
        sortopt: i32,
    ) -> PgResult<Tuplesortstate<'mcx>>
);

seam_core::seam!(
    /// `tuplesort_begin_datum(datumType, sortOperator, sortCollation,
    /// nullsFirstFlag, workMem, coordinate=NULL, sortopt)`
    /// (tuplesortvariants.c): begin a single-column Datum sort. Allocates in
    /// `mcx`, fallible on OOM.
    pub fn tuplesort_begin_datum<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        datum_type: Oid,
        sort_operator: Oid,
        sort_collation: Oid,
        nulls_first_flag: bool,
        work_mem: i32,
        sortopt: i32,
    ) -> PgResult<Tuplesortstate<'mcx>>
);

seam_core::seam!(
    /// `tuplesort_set_bound(state, bound)` (tuplesort.c): set the bound for a
    /// bounded (top-N) sort.
    pub fn tuplesort_set_bound<'mcx>(
        state: &mut Tuplesortstate<'mcx>,
        bound: i64,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `tuplesort_puttupleslot(state, slot)` (tuplesortvariants.c): copy the
    /// slot's tuple into the sort. Allocates the stored tuple, fallible on OOM.
    pub fn tuplesort_puttupleslot<'mcx>(
        state: &mut Tuplesortstate<'mcx>,
        slot: &TupleTableSlot,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `tuplesort_putdatum(state, val, isNull)` (tuplesortvariants.c): feed one
    /// Datum to a Datum sort. Allocates (copies pass-by-ref data), fallible.
    pub fn tuplesort_putdatum<'mcx>(
        state: &mut Tuplesortstate<'mcx>,
        val: Datum<'mcx>,
        is_null: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `tuplesort_performsort(state)` (tuplesort.c): all tuples have been
    /// supplied; complete the sort. May spill to disk / allocate, fallible.
    pub fn tuplesort_performsort<'mcx>(state: &mut Tuplesortstate<'mcx>) -> PgResult<()>
);

seam_core::seam!(
    /// `tuplesort_gettupleslot(state, forward, copy, slot, abbrev=NULL)`
    /// (tuplesortvariants.c): fetch the next tuple into `slot`. Returns `false`
    /// (and stores an empty slot) at end of sort. Can detoast/allocate,
    /// fallible.
    pub fn tuplesort_gettupleslot<'mcx>(
        state: &mut Tuplesortstate<'mcx>,
        forward: bool,
        copy: bool,
        slot: &mut TupleTableSlot,
    ) -> PgResult<bool>
);

seam_core::seam!(
    /// `tuplesort_getdatum(state, forward, copy, &val, &isNull, abbrev=NULL)`
    /// (tuplesortvariants.c): fetch the next Datum. Returns `(found, val,
    /// isNull)`; `found == false` means end of sort. Can allocate, fallible.
    pub fn tuplesort_getdatum<'mcx>(
        state: &mut Tuplesortstate<'mcx>,
        forward: bool,
        copy: bool,
    ) -> PgResult<(bool, Datum<'mcx>, bool)>
);

seam_core::seam!(
    /// `tuplesort_get_stats(state, stats)` (tuplesort.c): report sort
    /// algorithm / space statistics into a `TuplesortInstrumentation`.
    pub fn tuplesort_get_stats<'mcx>(
        state: &Tuplesortstate<'mcx>,
    ) -> TuplesortInstrumentation
);

seam_core::seam!(
    /// `tuplesort_end(state)` (tuplesort.c): release all the sort's resources
    /// (temp files, memory). Closing temp files can `elog(ERROR)`, carried on
    /// `Err`.
    pub fn tuplesort_end<'mcx>(state: mcx::PgBox<'mcx, Tuplesortstate<'mcx>>) -> PgResult<()>
);

seam_core::seam!(
    /// `tuplesort_rescan(state)` (tuplesort.c): rewind a randomAccess sort to
    /// the start so the output can be re-read.
    pub fn tuplesort_rescan<'mcx>(state: &mut Tuplesortstate<'mcx>) -> PgResult<()>
);

seam_core::seam!(
    /// `tuplesort_markpos(state)` (tuplesort.c): save the current sort-output
    /// position (randomAccess only).
    pub fn tuplesort_markpos<'mcx>(state: &mut Tuplesortstate<'mcx>) -> PgResult<()>
);

seam_core::seam!(
    /// `tuplesort_restorepos(state)` (tuplesort.c): restore the last saved
    /// sort-output position (randomAccess only).
    pub fn tuplesort_restorepos<'mcx>(state: &mut Tuplesortstate<'mcx>) -> PgResult<()>
);

// === index-btree build sort (tuplesortvariants.c) ==========================
//
// Consumed by `backend-access-nbtree-nbtsort` (`_bt_leafbuild` / `_bt_load`).
// The nbtree build never passes a parallel `SortCoordinate` from the grounded
// (serial) path, so the begin seam omits that parameter like the heap/datum
// variants above. Owned by the (still-`todo`) tuplesort unit; panics until it
// lands and installs them from `init_seams()`.

seam_core::seam!(
    /// `tuplesort_begin_index_btree(heapRel, indexRel, enforceUnique,
    /// uniqueNullsNotDistinct, workMem, coordinate=NULL, sortopt)`
    /// (tuplesortvariants.c): begin a btree-index build sort keyed by the
    /// index's sort operators (with heap TID as the implicit final tiebreaker).
    /// Allocates the sort state in `mcx`; fallible on OOM.
    pub fn tuplesort_begin_index_btree<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        heap_rel: &types_rel::Relation<'mcx>,
        index_rel: &types_rel::Relation<'mcx>,
        enforce_unique: bool,
        unique_nulls_not_distinct: bool,
        work_mem: i32,
        sortopt: i32,
    ) -> PgResult<Tuplesortstate<'mcx>>
);

seam_core::seam!(
    /// `tuplesort_getindextuple(state, forward)` (tuplesortvariants.c): fetch
    /// the next `IndexTuple` from a btree-index build sort, returned as owned
    /// on-disk index-tuple bytes in `mcx`. `Ok(None)` at end of sort. Can
    /// detoast / allocate, fallible.
    pub fn tuplesort_getindextuple<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        state: &mut Tuplesortstate<'mcx>,
        forward: bool,
    ) -> PgResult<Option<mcx::PgVec<'mcx, u8>>>
);
