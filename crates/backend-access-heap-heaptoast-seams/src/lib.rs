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
    pub fn toast_flatten_tuple_to_datum(
        tuple: &types::backend_access_common_heaptuple::FormedTuple,
        tuple_desc: &types::heaptuple::TupleDescData,
    ) -> types::backend_access_common_heaptuple::FormedTuple
);
