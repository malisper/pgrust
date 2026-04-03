use crate::access::heap::tuple::{HeapTuple, ItemPointerData, TupleError, heap_page_add_tuple, heap_page_get_tuple, heap_page_init};
use crate::storage::page::PageError;
use crate::storage::smgr::{ForkNumber, RelFileLocator, SmgrError, StorageManager};
use crate::{BufferPool, ClientId, Error, RequestPageResult, SmgrStorageBackend};

#[derive(Debug)]
pub enum HeapError {
    Buffer(Error),
    Tuple(TupleError),
    Storage(SmgrError),
    NoBufferAvailable,
}

impl From<Error> for HeapError {
    fn from(value: Error) -> Self {
        Self::Buffer(value)
    }
}

impl From<TupleError> for HeapError {
    fn from(value: TupleError) -> Self {
        Self::Tuple(value)
    }
}

impl From<SmgrError> for HeapError {
    fn from(value: SmgrError) -> Self {
        Self::Storage(value)
    }
}

pub fn heap_insert(
    pool: &mut BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    tuple: &HeapTuple,
) -> Result<ItemPointerData, HeapError> {
    ensure_relation_exists(pool, rel)?;

    loop {
        let nblocks = pool.storage_mut().smgr.nblocks(rel, ForkNumber::Main)?;
        let target_block = if nblocks == 0 {
            bootstrap_first_page(pool, rel)?;
            0
        } else {
            nblocks - 1
        };

        let buffer_id = pin_existing_block(pool, client_id, rel, target_block)?;
        let page = *pool.read_page(buffer_id).ok_or(Error::InvalidBuffer)?;
        let mut new_page = page;

        match heap_page_add_tuple(&mut new_page, target_block, tuple) {
            Ok(offset_number) => {
                pool.write_page_image(buffer_id, &new_page)?;
                pool.unpin(client_id, buffer_id)?;
                return Ok(ItemPointerData {
                    block_number: target_block,
                    offset_number,
                });
            }
            Err(TupleError::Page(PageError::NoSpace)) => {
                pool.unpin(client_id, buffer_id)?;
                append_empty_heap_page(pool, rel, nblocks)?;
            }
            Err(e) => {
                pool.unpin(client_id, buffer_id)?;
                return Err(e.into());
            }
        }
    }
}

pub fn heap_fetch(
    pool: &mut BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    tid: ItemPointerData,
) -> Result<HeapTuple, HeapError> {
    let buffer_id = pin_existing_block(pool, client_id, rel, tid.block_number)?;
    let tuple = heap_page_get_tuple(
        pool.read_page(buffer_id).ok_or(Error::InvalidBuffer)?,
        tid.offset_number,
    )?;
    pool.unpin(client_id, buffer_id)?;
    Ok(tuple)
}

pub fn heap_flush(
    pool: &mut BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    block_number: u32,
) -> Result<(), HeapError> {
    let buffer_id = pin_existing_block(pool, client_id, rel, block_number)?;
    let _ = pool.flush_buffer(buffer_id)?;
    pool.complete_write(buffer_id)?;
    pool.unpin(client_id, buffer_id)?;
    Ok(())
}

fn ensure_relation_exists(
    pool: &mut BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
) -> Result<(), HeapError> {
    pool.storage_mut().smgr.open(rel)?;
    match pool.storage_mut().smgr.create(rel, ForkNumber::Main, false) {
        Ok(()) => {}
        Err(SmgrError::AlreadyExists { .. }) => {}
        Err(e) => return Err(e.into()),
    }
    Ok(())
}

fn bootstrap_first_page(
    pool: &mut BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
) -> Result<(), HeapError> {
    let mut page = [0u8; crate::BLCKSZ];
    heap_page_init(&mut page);
    pool.storage_mut()
        .smgr
        .extend(rel, ForkNumber::Main, 0, &page, true)?;
    Ok(())
}

fn append_empty_heap_page(
    pool: &mut BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
    block_number: u32,
) -> Result<(), HeapError> {
    let mut page = [0u8; crate::BLCKSZ];
    heap_page_init(&mut page);
    pool.storage_mut()
        .smgr
        .extend(rel, ForkNumber::Main, block_number, &page, true)?;
    Ok(())
}

fn pin_existing_block(
    pool: &mut BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    block_number: u32,
) -> Result<usize, HeapError> {
    let tag = crate::BufferTag {
        rel,
        fork: ForkNumber::Main,
        block: block_number,
    };
    let buffer_id = match pool.request_page(client_id, tag) {
        RequestPageResult::Hit { buffer_id }
        | RequestPageResult::WaitingOnRead { buffer_id }
        | RequestPageResult::ReadIssued { buffer_id } => buffer_id,
        RequestPageResult::AllBuffersPinned => return Err(HeapError::NoBufferAvailable),
    };

    if matches!(pool.pending_io(buffer_id), Some(crate::PendingIo { op: crate::IoOp::Read, .. })) {
        pool.complete_read(buffer_id)?;
    }

    Ok(buffer_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SmgrStorageBackend;
    use std::fs;
    use std::path::PathBuf;

    fn temp_dir(label: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("pgrust_heapam_{}", label));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn rel(n: u32) -> RelFileLocator {
        RelFileLocator {
            spc_oid: 0,
            db_oid: 1,
            rel_number: n,
        }
    }

    #[test]
    fn heap_insert_and_fetch_roundtrip() {
        let base = temp_dir("insert_fetch_roundtrip");
        let smgr = crate::storage::smgr::MdStorageManager::new(&base);
        let mut pool = BufferPool::new(SmgrStorageBackend::new(smgr), 4);
        let tuple = HeapTuple::new_raw(2, b"hello|heap".to_vec());

        let tid = heap_insert(&mut pool, 1, rel(5000), &tuple).unwrap();
        let fetched = heap_fetch(&mut pool, 2, rel(5000), tid).unwrap();

        assert_eq!(fetched.data, tuple.data);
        assert_eq!(fetched.header.ctid, tid);
    }

    #[test]
    fn heap_insert_persists_after_flush_and_reload() {
        let base = temp_dir("persist_after_flush");
        let rel = rel(5001);
        let tid = {
            let smgr = crate::storage::smgr::MdStorageManager::new(&base);
            let mut pool = BufferPool::new(SmgrStorageBackend::new(smgr), 4);
            let tuple = HeapTuple::new_raw(2, b"persisted-tuple".to_vec());
            let tid = heap_insert(&mut pool, 1, rel, &tuple).unwrap();
            heap_flush(&mut pool, 1, rel, tid.block_number).unwrap();
            tid
        };

        let smgr = crate::storage::smgr::MdStorageManager::new(&base);
        let mut pool = BufferPool::new(SmgrStorageBackend::new(smgr), 4);
        let fetched = heap_fetch(&mut pool, 2, rel, tid).unwrap();
        assert_eq!(fetched.data, b"persisted-tuple".to_vec());
    }

    #[test]
    fn heap_insert_spills_to_new_page_when_full() {
        let base = temp_dir("spill_to_new_page");
        let smgr = crate::storage::smgr::MdStorageManager::new(&base);
        let mut pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);
        let rel = rel(5002);

        let large = HeapTuple::new_raw(1, vec![0xAB; 7000]);
        let first = heap_insert(&mut pool, 1, rel, &large).unwrap();
        let second = heap_insert(&mut pool, 1, rel, &large).unwrap();
        let third = heap_insert(&mut pool, 1, rel, &large).unwrap();

        assert_eq!(first.block_number, 0);
        assert!(second.block_number > first.block_number);
        assert!(third.block_number > second.block_number);
    }
}
