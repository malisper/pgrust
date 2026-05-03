// :HACK: root compatibility shim while btree support code lives in `pgrust_access`.
#[allow(unused_imports)]
pub use pgrust_access::nbtree::nbtdedup::*;
