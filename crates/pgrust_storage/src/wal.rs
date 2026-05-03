use std::any::Any;

use crate::include::storage::buf_internals::{BufferTag, Page};

pub use pgrust_core::{
    INVALID_LSN, Lsn, RM_BTREE_ID, RM_GIN_ID, RM_GIST_ID, RM_HASH_ID, RM_HEAP_ID, RM_HEAP2_ID,
    RM_XACT_ID, RM_XLOG_ID, WAL_SEG_SIZE_BYTES,
};

pub trait WalSink: Send + Sync + Any {
    fn flush(&self) -> Result<Lsn, String>;
    fn flush_to(&self, lsn: Lsn) -> Result<Lsn, String>;
    fn write_commit(&self, xid: u32) -> Result<Lsn, String>;
    fn write_prepare(&self, xid: u32, data: &[u8]) -> Result<Lsn, String>;
    fn write_abort(&self, xid: u32) -> Result<Lsn, String>;
    fn write_insert(
        &self,
        xid: u32,
        tag: BufferTag,
        page: &Page,
        offset_number: u16,
        tuple_data: &[u8],
    ) -> Result<Lsn, String>;
    fn write_record(&self, xid: u32, tag: BufferTag, page: &Page) -> Result<Lsn, String>;
    fn write_record_with_rmgr(
        &self,
        xid: u32,
        tag: BufferTag,
        page: &Page,
        rmid: u8,
    ) -> Result<Lsn, String>;
}
