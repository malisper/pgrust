pub use crate::include::access::heapam_xlog::*;

use crate::backend::access::transam::xlog::{Lsn, WalError, WalWriter};
use crate::backend::storage::buffer::{BufferTag, PAGE_SIZE};

pub fn log_heap_visible(
    wal: &WalWriter,
    xid: u32,
    vm_tag: BufferTag,
    vm_page: &[u8; PAGE_SIZE],
    heap_tag: BufferTag,
    heap_page: &[u8; PAGE_SIZE],
    record: XlHeapVisible,
) -> Result<Lsn, WalError> {
    wal.write_heap_visible(xid, vm_tag, vm_page, heap_tag, heap_page, record)
}
