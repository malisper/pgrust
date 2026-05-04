// :HACK: root compatibility shim while lock-manager code lives in
// `pgrust_storage`. Long term callers should import from `pgrust_storage`.
pub mod advisory;
pub mod lock;
pub mod predicate;
pub mod proc;
pub mod row;

pub use pgrust_storage::lmgr::*;
