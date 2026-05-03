// :HACK: root compatibility shim while btree scan-key preprocessing lives in
// `pgrust_access`.
pub use pgrust_access::nbtree::nbtpreprocesskeys::*;
