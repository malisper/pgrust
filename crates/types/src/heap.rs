//! Shared definitions for the heap access method (`access/heap`).
//!
//! Populated incrementally from ../pgrust/src-idiomatic/crates/types/src/heap.rs
//! as ports need items; only the items currently consumed are present.

/// `SizeofHeapTupleHeader` == `offsetof(HeapTupleHeaderData, t_bits)`.
pub const SizeofHeapTupleHeader: usize = 23;
