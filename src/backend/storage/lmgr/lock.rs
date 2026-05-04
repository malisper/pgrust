// :HACK: root compatibility shim while lock-manager code lives in `pgrust_storage`.
pub use pgrust_storage::lmgr::lock::*;
