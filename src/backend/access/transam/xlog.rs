// :HACK: Compatibility shim for old root WAL paths. Recovery replay stays
// rooted here until access-method redo modules move with transam.
pub use pgrust_access::transam::xlog::*;

pub mod replay {
    pub use crate::backend::access::transam::xlogrecovery::*;
}
