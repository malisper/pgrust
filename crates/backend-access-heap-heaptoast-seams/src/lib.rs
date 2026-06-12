//! Seam declarations for the `backend-access-heap-heaptoast` unit
//! (`access/heap/heaptoast.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

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
