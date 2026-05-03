// :HACK: Compatibility shim while btree WAL redo lives in `pgrust_access`.
pub use pgrust_access::nbtree::nbtxlog::*;
