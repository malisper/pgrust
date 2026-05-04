// :HACK: Compatibility shim while root heap callers are migrated to
// `pgrust_access::heap::pruneheap` directly.
pub use pgrust_access::heap::pruneheap::*;
