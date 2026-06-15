//! Seam declarations for the `backend-access-heap-heaptoast` unit
//! (`access/heap/heaptoast.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `HeapTupleHeaderGetDatum(tuple)` (execTuples.c): convert a freshly-formed
    /// `HeapTupleHeader` to a composite `Datum`. If the tuple has no external
    /// TOAST pointers this is C's `PointerGetDatum(tuple)` (the tuple is returned
    /// unchanged and the `Datum` references it); otherwise the rowtype is looked
    /// up (`lookup_rowtype_tupdesc(HeapTupleHeaderGetTypeId/TypMod)`) and the
    /// tuple flattened via `toast_flatten_tuple_to_datum`, returning the new
    /// inlined tuple and a `Datum` referencing it. Producing a composite `Datum`
    /// token from a tuple is the heap/datum owner's concern; the execTuples
    /// caller reaches it through this seam. `Err` carries the detoast
    /// `ereport(ERROR)` surface and OOM.
    ///
    /// The composite `Datum` references the returned tuple's bytes, so in the
    /// canonical model it is a by-reference value:
    /// [`types_tuple::backend_access_common_heaptuple::Datum::ByRef`].
    ///
    /// The tuple crosses as the data-carrying [`FormedTuple`] (owned header +
    /// user-data area), NOT the header-only `HeapTuple` — a composite Datum is
    /// the whole contiguous `HeapTupleHeader` image, so its source must carry the
    /// column bytes. The returned `FormedTuple` is the (possibly flattened) tuple
    /// the Datum references; the composite/record-Datum carrier bridge (task #161)
    /// mints the `Datum::ByRef` image from it.
    pub fn heap_tuple_header_get_datum<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        tuple: types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
    ) -> types_error::PgResult<(
        types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
        types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
    )>
);

seam_core::seam!(
    /// `toast_flatten_tuple_to_datum(tup, tup_len, tupleDesc)`
    /// (access/heap/heaptoast.c): inline a tuple's external TOAST pointers to
    /// produce a self-contained composite-type Datum. Reached from
    /// `heap_copy_tuple_as_datum` when the tuple has `HEAP_HASEXTERNAL` set.
    /// `Err` carries the detoasting `ereport(ERROR)`s (e.g. `detoast_attr` /
    /// `toast_fetch_datum`'s `missing chunk number ...`) and OOM. The result
    /// is allocated in `mcx` (C: palloc in `CurrentMemoryContext`).
    pub fn toast_flatten_tuple_to_datum<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        tuple: &types_tuple::backend_access_common_heaptuple::FormedTuple<'_>,
        tuple_desc: &types_tuple::heaptuple::TupleDescData<'_>,
    ) -> types_error::PgResult<types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>>
);

seam_core::seam!(
    /// `toast_flatten_tuple(tup, tupleDesc)` (access/heap/heaptoast.c):
    /// "flatten" a tuple to contain no out-of-line toasted fields (does not
    /// eliminate compressed or short-header datums). The caller already
    /// checked `HeapTupleHasExternal(tup)`. `Err` carries the detoasting
    /// `ereport(ERROR)`s and OOM; the result is allocated in `mcx` (C: palloc
    /// in `CurrentMemoryContext`).
    pub fn toast_flatten_tuple<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        tup: &types_tuple::backend_access_common_heaptuple::FormedTuple<'_>,
        tuple_desc: &types_tuple::heaptuple::TupleDescData<'_>,
    ) -> types_error::PgResult<types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>>
);
