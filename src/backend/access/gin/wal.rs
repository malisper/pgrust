// :HACK: Compatibility shim while GIN WAL redo lives in `pgrust_access`.
#![allow(unused_imports)]
pub use pgrust_access::gin::wal::*;
