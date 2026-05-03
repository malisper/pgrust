use std::collections::BTreeSet;

use crate::SmgrStorageBackend;
use crate::backend::storage::buffer::{BufferPool, PAGE_SIZE};
use crate::backend::storage::smgr::{ForkNumber, RelFileLocator, StorageManager};

fn load_free_pages(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
) -> Result<BTreeSet<u32>, String> {
    pool.ensure_relation_fork(rel, ForkNumber::Fsm)
        .map_err(|err| format!("fsm ensure fork failed: {err:?}"))?;
    pool.with_storage_mut(|storage| -> Result<BTreeSet<u32>, String> {
        let nblocks = storage
            .smgr
            .nblocks(rel, ForkNumber::Fsm)
            .map_err(|err| err.to_string())?;
        if nblocks == 0 {
            return Ok(BTreeSet::new());
        }
        let mut buf = [0u8; PAGE_SIZE];
        storage
            .smgr
            .read_block(rel, ForkNumber::Fsm, 0, &mut buf)
            .map_err(|err| err.to_string())?;
        let count = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
        let mut free = BTreeSet::new();
        for idx in 0..count {
            let start = 4 + idx * 4;
            if start + 4 > PAGE_SIZE {
                break;
            }
            free.insert(u32::from_le_bytes(
                buf[start..start + 4].try_into().unwrap(),
            ));
        }
        Ok(free)
    })
}

fn store_free_pages(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
    free: &BTreeSet<u32>,
) -> Result<(), String> {
    pool.ensure_relation_fork(rel, ForkNumber::Fsm)
        .map_err(|err| format!("fsm ensure fork failed: {err:?}"))?;
    let mut page = [0u8; PAGE_SIZE];
    let capped = free.len().min((PAGE_SIZE - 4) / 4);
    page[0..4].copy_from_slice(&(capped as u32).to_le_bytes());
    for (idx, block) in free.iter().copied().take(capped).enumerate() {
        let start = 4 + idx * 4;
        page[start..start + 4].copy_from_slice(&block.to_le_bytes());
    }
    pool.with_storage_mut(|storage| -> Result<(), String> {
        let nblocks = storage
            .smgr
            .nblocks(rel, ForkNumber::Fsm)
            .map_err(|err| err.to_string())?;
        if nblocks == 0 {
            storage
                .smgr
                .extend(rel, ForkNumber::Fsm, 0, &page, true)
                .map_err(|err| err.to_string())
        } else {
            storage
                .smgr
                .write_block(rel, ForkNumber::Fsm, 0, &page, true)
                .map_err(|err| err.to_string())
        }
    })?;
    Ok(())
}

pub fn get_free_index_page(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
) -> Result<Option<u32>, String> {
    let mut free = load_free_pages(pool, rel)?;
    let Some(block) = free.iter().next().copied() else {
        return Ok(None);
    };
    free.remove(&block);
    store_free_pages(pool, rel, &free)?;
    Ok(Some(block))
}

pub fn record_free_index_page(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
    block: u32,
) -> Result<(), String> {
    let mut free = load_free_pages(pool, rel)?;
    free.insert(block);
    store_free_pages(pool, rel, &free)
}

pub fn clear_free_index_pages(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
) -> Result<(), String> {
    store_free_pages(pool, rel, &BTreeSet::new())
}

pub fn finalize_pending_index_pages(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
    pages: &[u32],
) -> Result<(), String> {
    let mut free = load_free_pages(pool, rel)?;
    free.extend(pages.iter().copied());
    store_free_pages(pool, rel, &free)
}
