// :HACK: Compatibility shim while GiST WAL redo lives in `pgrust_access`.
pub use pgrust_access::gist::wal::*;
