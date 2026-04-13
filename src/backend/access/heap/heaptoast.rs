pub use crate::include::access::heaptoast::*;

// :HACK: Heap-side TOAST insert/update/delete policy is not wired yet.
// The module boundary is added now so the eventual implementation can match
// PostgreSQL's `heaptoast.c` layout without growing `heapam.rs` further.
