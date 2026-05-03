pub mod buffer;
pub mod fsm;
pub mod lmgr;
pub mod page;
pub mod smgr;
pub mod sync;

// :HACK: root compatibility shim while storage lives in `pgrust_storage`.
pub use pgrust_storage::{fsync_dir, fsync_file, sync_file_data};
