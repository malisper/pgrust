//! Seam declarations for the `backend-utils-sort-tuplesort` unit
//! (`utils/sort/tuplesort.c` + `tuplesortvariants.c`): the sort-object surface
//! the agg ORDER BY / DISTINCT paths drive.
//!
//! `Tuplesortstate *` crosses the seam as the opaque
//! [`TuplesortstateHandle`] from `types_nodes::nodeagg` (the sort owner names
//! the concrete state when it lands). The `SortCoordinate` parallel-sort
//! coordinator argument is `None` here (agg uses non-parallel sorts).
//! The owning unit installs these from its `init_seams()`; until then a call
//! panics loudly.

#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::primitive::{AttrNumber, Oid};
use types_datum::Datum;
use types_error::PgResult;
use types_nodes::nodeagg::TuplesortstateHandle;
use types_tuple::heaptuple::TupleDescData;

seam_core::seam!(
    /// `tuplesort_begin_heap(...)` (tuplesort.c): create a heap-tuple sort.
    /// `att_nums`/`sort_operators`/`sort_collations`/`nulls_first` are the
    /// `nkeys`-long sort-key descriptors; `work_mem` is in kilobytes,
    /// `sortopt` the `TUPLESORT_*` option bits. C pallocs the state in its
    /// sort context, so creation is fallible on OOM.
    pub fn tuplesort_begin_heap<'mcx>(
        mcx: Mcx<'mcx>,
        tup_desc: &TupleDescData<'_>,
        nkeys: i32,
        att_nums: &[AttrNumber],
        sort_operators: &[Oid],
        sort_collations: &[Oid],
        nulls_first: &[bool],
        work_mem: i32,
        sortopt: i32,
    ) -> PgResult<TuplesortstateHandle>
);

seam_core::seam!(
    /// `tuplesort_begin_datum(...)` (tuplesort.c): create a single-Datum sort
    /// (single-column DISTINCT/ORDER BY aggregates).
    pub fn tuplesort_begin_datum<'mcx>(
        mcx: Mcx<'mcx>,
        datum_type: Oid,
        sort_operator: Oid,
        sort_collation: Oid,
        nulls_first_flag: bool,
        work_mem: i32,
        sortopt: i32,
    ) -> PgResult<TuplesortstateHandle>
);

seam_core::seam!(
    /// `tuplesort_puttupleslot(state, slot)` (tuplesortvariants.c): feed the
    /// slot's current tuple into the sort. Spilling to tape can ereport.
    pub fn tuplesort_puttupleslot(
        state: TuplesortstateHandle,
        slot: types_nodes::SlotId,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `tuplesort_putdatum(state, val, isNull)` (tuplesortvariants.c): feed a
    /// single Datum into the sort.
    pub fn tuplesort_putdatum(
        state: TuplesortstateHandle,
        val: Datum,
        is_null: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `tuplesort_performsort(state)` (tuplesort.c): finish input and sort.
    pub fn tuplesort_performsort(state: TuplesortstateHandle) -> PgResult<()>
);

seam_core::seam!(
    /// `tuplesort_gettupleslot(state, forward, copy, slot, abbrev)`
    /// (tuplesortvariants.c): fetch the next sorted tuple into `slot`.
    /// Returns false at end of sort; the optional abbreviated key is the
    /// second tuple of the result.
    pub fn tuplesort_gettupleslot(
        state: TuplesortstateHandle,
        forward: bool,
        copy: bool,
        slot: types_nodes::SlotId,
    ) -> PgResult<(bool, Option<Datum>)>
);

seam_core::seam!(
    /// `tuplesort_getdatum(state, forward, copy, val, isNull, abbrev)`
    /// (tuplesortvariants.c): fetch the next sorted Datum. Returns
    /// `(found, val, isNull, abbrev)`; `found` is false at end of sort.
    pub fn tuplesort_getdatum(
        state: TuplesortstateHandle,
        forward: bool,
        copy: bool,
    ) -> PgResult<(bool, Datum, bool, Option<Datum>)>
);

seam_core::seam!(
    /// `tuplesort_end(state)` (tuplesort.c): release the sort object and its
    /// memory/tapes.
    pub fn tuplesort_end(state: TuplesortstateHandle) -> PgResult<()>
);
