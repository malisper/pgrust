// :HACK: root compatibility shim while storage lives in `pgrust_storage`.
pub mod bufmgr {
    pub use pgrust_storage::buffer::bufmgr::*;
}

pub mod storage_backend {
    pub use pgrust_storage::buffer::storage_backend::*;
}

pub use pgrust_storage::buffer::*;
