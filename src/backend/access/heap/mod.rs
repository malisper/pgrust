pub mod heapam;
pub mod heapam_visibility;
pub mod heapam_xlog;
pub mod heaptoast;
pub mod pruneheap;
pub mod vacuumlazy;
pub mod visibilitymap;

// :HACK: Compatibility re-export while heap runtime lives in `pgrust_access`.
pub use pgrust_access::heap::HeapWalPolicy;
