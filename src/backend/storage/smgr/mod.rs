// :HACK: root compatibility shim while storage lives in `pgrust_storage`.
pub mod any {
    pub use pgrust_storage::smgr::any::*;
}

pub mod md {
    pub use pgrust_storage::smgr::md::*;
}

pub mod mem {
    pub use pgrust_storage::smgr::mem::*;
}

pub mod smgr {
    pub use pgrust_storage::smgr::smgr::*;
}

pub use pgrust_storage::smgr::*;
