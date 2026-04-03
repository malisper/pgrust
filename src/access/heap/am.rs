use crate::access::heap::mvcc::{MvccError, Snapshot, TransactionId, TransactionManager};
use crate::access::heap::tuple::{
    HeapTuple, ItemPointerData, TupleError, heap_page_add_tuple, heap_page_get_tuple,
    heap_page_init, heap_page_replace_tuple,
};
use crate::storage::page::{ItemIdFlags, PageError, page_get_item_id, page_get_max_offset_number};
use crate::storage::smgr::{ForkNumber, RelFileLocator, SmgrError, StorageManager};
use crate::{BufferPool, ClientId, Error, RequestPageResult, SmgrStorageBackend};

#[derive(Debug)]
pub enum HeapError {
    Buffer(Error),
    Tuple(TupleError),
    Storage(SmgrError),
    Mvcc(MvccError),
    NoBufferAvailable,
    TupleNotVisible(ItemPointerData),
    TupleAlreadyModified(ItemPointerData),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeapScan {
    rel: RelFileLocator,
    nblocks: u32,
    current_block: u32,
    current_offset: u16,
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

impl From<MvccError> for HeapError {
    fn from(value: MvccError) -> Self {
        Self::Mvcc(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VisibleHeapScan {
    scan: HeapScan,
    snapshot: Snapshot,
}

pub fn heap_scan_begin(
    pool: &mut BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
) -> Result<HeapScan, HeapError> {
    let nblocks = pool.storage_mut().smgr.nblocks(rel, ForkNumber::Main)?;
    Ok(HeapScan {
        rel,
        nblocks,
        current_block: 0,
        current_offset: 1,
    })
}

pub fn heap_scan_next(
    pool: &mut BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    scan: &mut HeapScan,
) -> Result<Option<(ItemPointerData, HeapTuple)>, HeapError> {
    while scan.current_block < scan.nblocks {
        let block = scan.current_block;
        let buffer_id = pin_existing_block(pool, client_id, scan.rel, block)?;
        let page = *pool.read_page(buffer_id).ok_or(Error::InvalidBuffer)?;
        let max_offset = page_get_max_offset_number(&page).map_err(TupleError::from)?;

        while scan.current_offset <= max_offset {
            let off = scan.current_offset;
            scan.current_offset += 1;

            let item_id = page_get_item_id(&page, off).map_err(TupleError::from)?;
            if item_id.lp_flags != ItemIdFlags::Normal || !item_id.has_storage() {
                continue;
            }

            let tuple = heap_page_get_tuple(&page, off)?;
            pool.unpin(client_id, buffer_id)?;
            return Ok(Some((
                ItemPointerData {
                    block_number: block,
                    offset_number: off,
                },
                tuple,
            )));
        }

        pool.unpin(client_id, buffer_id)?;
        scan.current_block += 1;
        scan.current_offset = 1;
    }

    Ok(None)
}

pub fn heap_scan_begin_visible(
    pool: &mut BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
    snapshot: Snapshot,
) -> Result<VisibleHeapScan, HeapError> {
    Ok(VisibleHeapScan {
        scan: heap_scan_begin(pool, rel)?,
        snapshot,
    })
}

pub fn heap_scan_next_visible(
    pool: &mut BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    scan: &mut VisibleHeapScan,
) -> Result<Option<(ItemPointerData, HeapTuple)>, HeapError> {
    while let Some((tid, tuple)) = heap_scan_next(pool, client_id, &mut scan.scan)? {
        if scan.snapshot.tuple_visible(&tuple) {
            return Ok(Some((tid, tuple)));
        }
    }
    Ok(None)
}

pub fn heap_insert(
    pool: &mut BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    tuple: &HeapTuple,
) -> Result<ItemPointerData, HeapError> {
    heap_insert_version(pool, client_id, rel, tuple, 0)
}

pub fn heap_insert_mvcc(
    pool: &mut BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    xid: TransactionId,
    tuple: &HeapTuple,
) -> Result<ItemPointerData, HeapError> {
    heap_insert_version(pool, client_id, rel, tuple, xid)
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

pub fn heap_fetch_visible(
    pool: &mut BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    tid: ItemPointerData,
    snapshot: &Snapshot,
) -> Result<Option<HeapTuple>, HeapError> {
    let tuple = heap_fetch(pool, client_id, rel, tid)?;
    if snapshot.tuple_visible(&tuple) {
        Ok(Some(tuple))
    } else {
        Ok(None)
    }
}

pub fn heap_delete(
    pool: &mut BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    txns: &TransactionManager,
    xid: TransactionId,
    tid: ItemPointerData,
) -> Result<(), HeapError> {
    let snapshot = txns.snapshot(xid)?;
    let buffer_id = pin_existing_block(pool, client_id, rel, tid.block_number)?;
    let page = *pool.read_page(buffer_id).ok_or(Error::InvalidBuffer)?;
    let mut new_page = page;
    let mut tuple = heap_page_get_tuple(&new_page, tid.offset_number)?;

    if !snapshot.tuple_visible(&tuple) {
        pool.unpin(client_id, buffer_id)?;
        return Err(HeapError::TupleNotVisible(tid));
    }
    if tuple.header.xmax != 0 {
        pool.unpin(client_id, buffer_id)?;
        return Err(HeapError::TupleAlreadyModified(tid));
    }

    tuple.header.xmax = xid;
    heap_page_replace_tuple(&mut new_page, tid.offset_number, &tuple)?;
    pool.write_page_image(buffer_id, &new_page)?;
    pool.unpin(client_id, buffer_id)?;
    Ok(())
}

pub fn heap_update(
    pool: &mut BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    txns: &TransactionManager,
    xid: TransactionId,
    tid: ItemPointerData,
    replacement: &HeapTuple,
) -> Result<ItemPointerData, HeapError> {
    let snapshot = txns.snapshot(xid)?;
    let old = heap_fetch(pool, client_id, rel, tid)?;
    if !snapshot.tuple_visible(&old) {
        return Err(HeapError::TupleNotVisible(tid));
    }
    if old.header.xmax != 0 {
        return Err(HeapError::TupleAlreadyModified(tid));
    }

    let new_tid = heap_insert_version(pool, client_id, rel, replacement, xid)?;

    let buffer_id = pin_existing_block(pool, client_id, rel, tid.block_number)?;
    let page = *pool.read_page(buffer_id).ok_or(Error::InvalidBuffer)?;
    let mut new_page = page;
    let mut old_version = heap_page_get_tuple(&new_page, tid.offset_number)?;
    old_version.header.xmax = xid;
    old_version.header.ctid = new_tid;
    heap_page_replace_tuple(&mut new_page, tid.offset_number, &old_version)?;
    pool.write_page_image(buffer_id, &new_page)?;
    pool.unpin(client_id, buffer_id)?;

    Ok(new_tid)
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

fn heap_insert_version(
    pool: &mut BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    tuple: &HeapTuple,
    xmin: TransactionId,
) -> Result<ItemPointerData, HeapError> {
    ensure_relation_exists(pool, rel)?;

    loop {
        let nblocks = pool.storage_mut().smgr.nblocks(rel, ForkNumber::Main)?;
        // This insert path still uses a deliberately simple placement policy:
        // try the tail page, and if it cannot fit the tuple, extend the relation
        // with one brand-new heap page and retry there.
        let target_block = if nblocks == 0 {
            bootstrap_first_page(pool, rel)?;
            0
        } else {
            nblocks - 1
        };

        let buffer_id = pin_existing_block(pool, client_id, rel, target_block)?;
        let page = *pool.read_page(buffer_id).ok_or(Error::InvalidBuffer)?;
        let mut new_page = page;
        let mut stored = tuple.clone();
        stored.header.xmin = xmin;
        stored.header.xmax = 0;

        match heap_page_add_tuple(&mut new_page, target_block, &stored) {
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

    if matches!(
        pool.pending_io(buffer_id),
        Some(crate::PendingIo {
            op: crate::IoOp::Read,
            ..
        })
    ) {
        pool.complete_read(buffer_id)?;
    }

    Ok(buffer_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SmgrStorageBackend;
    use crate::access::heap::mvcc::INVALID_TRANSACTION_ID;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

    fn temp_dir(label: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "pgrust_heapam_{}_{}_{}",
            label,
            std::process::id(),
            NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
        ));
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

    #[test]
    fn heap_scan_returns_inserted_tuples_in_physical_order() {
        let base = temp_dir("scan_physical_order");
        let smgr = crate::storage::smgr::MdStorageManager::new(&base);
        let mut pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);
        let rel = rel(5003);

        let large = HeapTuple::new_raw(1, vec![0xAA; 7000]);
        let small = HeapTuple::new_raw(1, b"tail".to_vec());

        let t1 = heap_insert(&mut pool, 1, rel, &large).unwrap();
        let t2 = heap_insert(&mut pool, 1, rel, &large).unwrap();
        let t3 = heap_insert(&mut pool, 1, rel, &small).unwrap();

        let mut scan = heap_scan_begin(&mut pool, rel).unwrap();
        let mut seen = Vec::new();
        while let Some((tid, tuple)) = heap_scan_next(&mut pool, 2, &mut scan).unwrap() {
            seen.push((tid, tuple.data));
        }

        assert_eq!(seen.len(), 3);
        assert_eq!(seen[0].0, t1);
        assert_eq!(seen[1].0, t2);
        assert_eq!(seen[2].0, t3);
        assert_eq!(seen[2].1, b"tail".to_vec());
    }

    #[test]
    fn heap_scan_skips_unused_line_pointers() {
        let base = temp_dir("scan_skips_unused");
        let rel = rel(5004);
        let mut smgr = crate::storage::smgr::MdStorageManager::new(&base);
        smgr.open(rel).unwrap();
        smgr.create(rel, ForkNumber::Main, false).unwrap();

        let mut page = [0u8; crate::BLCKSZ];
        heap_page_init(&mut page);
        let _ =
            heap_page_add_tuple(&mut page, 0, &HeapTuple::new_raw(1, b"first".to_vec())).unwrap();
        let off2 =
            heap_page_add_tuple(&mut page, 0, &HeapTuple::new_raw(1, b"second".to_vec())).unwrap();

        // Mark the second line pointer unused to simulate a hole on the page.
        let idx = crate::storage::page::max_align(crate::storage::page::SIZE_OF_PAGE_HEADER_DATA)
            + (usize::from(off2) - 1) * crate::storage::page::ITEM_ID_SIZE;
        page[idx..idx + crate::storage::page::ITEM_ID_SIZE]
            .copy_from_slice(&crate::storage::page::ItemIdData::unused().encode());

        smgr.extend(rel, ForkNumber::Main, 0, &page, true).unwrap();

        let smgr = crate::storage::smgr::MdStorageManager::new(&base);
        let mut pool = BufferPool::new(SmgrStorageBackend::new(smgr), 4);
        let mut scan = heap_scan_begin(&mut pool, rel).unwrap();

        let first = heap_scan_next(&mut pool, 1, &mut scan).unwrap().unwrap();
        assert_eq!(first.1.data, b"first".to_vec());
        assert!(heap_scan_next(&mut pool, 1, &mut scan).unwrap().is_none());
    }

    #[test]
    fn heap_delete_hides_tuple_after_commit() {
        let base = temp_dir("mvcc_delete");
        let rel = rel(5005);
        let smgr = crate::storage::smgr::MdStorageManager::new(&base);
        let mut pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);
        let mut txns = TransactionManager::default();

        let inserter = txns.begin();
        let tid = heap_insert_mvcc(
            &mut pool,
            1,
            rel,
            inserter,
            &HeapTuple::new_raw(1, b"row".to_vec()),
        )
        .unwrap();
        txns.commit(inserter).unwrap();

        let deleter = txns.begin();
        heap_delete(&mut pool, 2, rel, &txns, deleter, tid).unwrap();

        let other = txns.begin();
        let other_snapshot = txns.snapshot(other).unwrap();
        let before_commit = heap_fetch_visible(&mut pool, 3, rel, tid, &other_snapshot).unwrap();
        assert!(before_commit.is_some());

        txns.commit(deleter).unwrap();
        let after_commit = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
        assert!(
            heap_fetch_visible(&mut pool, 4, rel, tid, &after_commit)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn heap_update_preserves_old_version_until_commit_and_new_version_after() {
        let base = temp_dir("mvcc_update");
        let rel = rel(5006);
        let smgr = crate::storage::smgr::MdStorageManager::new(&base);
        let mut pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);
        let mut txns = TransactionManager::default();

        let inserter = txns.begin();
        let old_tid = heap_insert_mvcc(
            &mut pool,
            1,
            rel,
            inserter,
            &HeapTuple::new_raw(1, b"old".to_vec()),
        )
        .unwrap();
        txns.commit(inserter).unwrap();

        let updater = txns.begin();
        let new_tid = heap_update(
            &mut pool,
            2,
            rel,
            &txns,
            updater,
            old_tid,
            &HeapTuple::new_raw(1, b"new".to_vec()),
        )
        .unwrap();

        let concurrent = txns.begin();
        let concurrent_snapshot = txns.snapshot(concurrent).unwrap();
        let old_visible = heap_fetch_visible(&mut pool, 3, rel, old_tid, &concurrent_snapshot)
            .unwrap()
            .unwrap();
        assert_eq!(old_visible.data, b"old".to_vec());
        assert!(
            heap_fetch_visible(&mut pool, 3, rel, new_tid, &concurrent_snapshot)
                .unwrap()
                .is_none()
        );

        txns.commit(updater).unwrap();
        let committed_snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
        assert!(
            heap_fetch_visible(&mut pool, 4, rel, old_tid, &committed_snapshot)
                .unwrap()
                .is_none()
        );
        let new_visible = heap_fetch_visible(&mut pool, 4, rel, new_tid, &committed_snapshot)
            .unwrap()
            .unwrap();
        assert_eq!(new_visible.data, b"new".to_vec());

        let old_stored = heap_fetch(&mut pool, 5, rel, old_tid).unwrap();
        assert_eq!(old_stored.header.xmax, updater);
        assert_eq!(old_stored.header.ctid, new_tid);
    }

    #[test]
    fn visible_scan_filters_dead_versions() {
        let base = temp_dir("mvcc_scan");
        let rel = rel(5007);
        let smgr = crate::storage::smgr::MdStorageManager::new(&base);
        let mut pool = BufferPool::new(SmgrStorageBackend::new(smgr), 8);
        let mut txns = TransactionManager::default();

        let xid1 = txns.begin();
        let tid1 = heap_insert_mvcc(
            &mut pool,
            1,
            rel,
            xid1,
            &HeapTuple::new_raw(1, b"first".to_vec()),
        )
        .unwrap();
        txns.commit(xid1).unwrap();

        let xid2 = txns.begin();
        let _tid2 = heap_update(
            &mut pool,
            2,
            rel,
            &txns,
            xid2,
            tid1,
            &HeapTuple::new_raw(1, b"second".to_vec()),
        )
        .unwrap();
        txns.commit(xid2).unwrap();

        let snapshot = txns.snapshot(INVALID_TRANSACTION_ID).unwrap();
        let mut scan = heap_scan_begin_visible(&mut pool, rel, snapshot).unwrap();
        let mut rows = Vec::new();
        while let Some((_tid, tuple)) = heap_scan_next_visible(&mut pool, 3, &mut scan).unwrap() {
            rows.push(tuple.data);
        }

        assert_eq!(rows, vec![b"second".to_vec()]);
    }
}
