use crate::access::visibilitymapdefs::{
    BITS_PER_HEAPBLOCK, VISIBILITYMAP_ALL_FROZEN, VISIBILITYMAP_ALL_VISIBLE,
    VISIBILITYMAP_VALID_BITS,
};
use crate::heap::HeapWalPolicy;
use pgrust_core::RM_HEAP2_ID;
use pgrust_storage::Error as BufferError;
use pgrust_storage::Page;
use pgrust_storage::SmgrStorageBackend;
use pgrust_storage::page::bufpage::{MAXALIGN, PageError, SIZE_OF_PAGE_HEADER_DATA, page_init};
use pgrust_storage::smgr::{BLCKSZ, ForkNumber, RelFileLocator, SmgrError, StorageManager};
use pgrust_storage::{BufferPool, ClientId};

const MAP_HEADER_SIZE: usize = (SIZE_OF_PAGE_HEADER_DATA + (MAXALIGN - 1)) & !(MAXALIGN - 1);
const MAPSIZE: usize = BLCKSZ - MAP_HEADER_SIZE;
const HEAPBLOCKS_PER_BYTE: usize = 8 / BITS_PER_HEAPBLOCK;
const HEAPBLOCKS_PER_PAGE: usize = MAPSIZE * HEAPBLOCKS_PER_BYTE;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VisibilityMapBuffer {
    map_block: u32,
}

impl VisibilityMapBuffer {
    pub fn for_heap_block(heap_block: u32) -> Self {
        Self {
            map_block: heapblk_to_mapblock(heap_block),
        }
    }

    pub fn map_block(self) -> u32 {
        self.map_block
    }
}

#[derive(Debug)]
pub enum VisibilityMapError {
    Buffer(BufferError),
    Storage(SmgrError),
    Page(PageError),
    WrongMapBuffer,
    InvalidFlags(u8),
}

impl From<BufferError> for VisibilityMapError {
    fn from(value: BufferError) -> Self {
        Self::Buffer(value)
    }
}

impl From<SmgrError> for VisibilityMapError {
    fn from(value: SmgrError) -> Self {
        Self::Storage(value)
    }
}

impl From<PageError> for VisibilityMapError {
    fn from(value: PageError) -> Self {
        Self::Page(value)
    }
}

impl From<VisibilityMapError> for crate::heap::heapam::HeapError {
    fn from(value: VisibilityMapError) -> Self {
        match value {
            VisibilityMapError::Buffer(err) => Self::Buffer(err),
            VisibilityMapError::Storage(err) => Self::Storage(err),
            VisibilityMapError::Page(err) => Self::from(err),
            VisibilityMapError::WrongMapBuffer | VisibilityMapError::InvalidFlags(_) => {
                Self::VisibilityMap(value)
            }
        }
    }
}

pub fn visibilitymap_pin(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
    heap_blk: u32,
    vmbuf: &mut Option<VisibilityMapBuffer>,
) -> Result<(), VisibilityMapError> {
    let map_block = heapblk_to_mapblock(heap_blk);
    if vmbuf.is_some_and(|buffer| buffer.map_block == map_block) {
        return Ok(());
    }
    pool.ensure_block_exists(rel, ForkNumber::VisibilityMap, map_block)?;
    *vmbuf = Some(VisibilityMapBuffer { map_block });
    Ok(())
}

pub fn visibilitymap_pin_ok(heap_blk: u32, vmbuf: &Option<VisibilityMapBuffer>) -> bool {
    vmbuf.is_some_and(|buffer| buffer.map_block == heapblk_to_mapblock(heap_blk))
}

pub fn visibilitymap_clear(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    heap_blk: u32,
    vmbuf: &Option<VisibilityMapBuffer>,
    flags: u8,
) -> Result<bool, VisibilityMapError> {
    visibilitymap_clear_with_wal_policy(
        pool,
        client_id,
        rel,
        heap_blk,
        vmbuf,
        flags,
        HeapWalPolicy::Wal,
    )
}

pub fn visibilitymap_clear_with_wal_policy(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    heap_blk: u32,
    vmbuf: &Option<VisibilityMapBuffer>,
    flags: u8,
    wal_policy: HeapWalPolicy,
) -> Result<bool, VisibilityMapError> {
    if flags & VISIBILITYMAP_VALID_BITS == 0 || flags == VISIBILITYMAP_ALL_VISIBLE {
        return Err(VisibilityMapError::InvalidFlags(flags));
    }
    let buffer = require_map_buffer(heap_blk, vmbuf)?;
    let pin =
        pool.pin_existing_block(client_id, rel, ForkNumber::VisibilityMap, buffer.map_block)?;
    let mut guard = pool.lock_buffer_exclusive(pin.buffer_id())?;
    ensure_vm_page_initialized(&mut guard);
    let (map_byte, map_offset) = heapblk_to_byte_offset(heap_blk);
    let idx = MAP_HEADER_SIZE + map_byte;
    let mask = flags << map_offset;
    let current = guard[idx];
    if current & mask == 0 {
        return Ok(false);
    }
    guard[idx] &= !mask;
    let page = *guard;
    match wal_policy {
        HeapWalPolicy::Wal => {
            pool.write_page_image_locked_with_rmgr(
                pin.buffer_id(),
                0,
                &page,
                &mut guard,
                RM_HEAP2_ID,
            )?;
        }
        HeapWalPolicy::NoWal => {
            pool.write_page_no_wal_locked(pin.buffer_id(), &page, &mut guard)?;
        }
    }
    Ok(true)
}

pub fn visibilitymap_set(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    heap_blk: u32,
    vmbuf: &Option<VisibilityMapBuffer>,
    flags: u8,
) -> Result<u8, VisibilityMapError> {
    if flags & VISIBILITYMAP_VALID_BITS != flags || flags == 0 {
        return Err(VisibilityMapError::InvalidFlags(flags));
    }
    let buffer = require_map_buffer(heap_blk, vmbuf)?;
    let pin =
        pool.pin_existing_block(client_id, rel, ForkNumber::VisibilityMap, buffer.map_block)?;
    let mut guard = pool.lock_buffer_exclusive(pin.buffer_id())?;
    ensure_vm_page_initialized(&mut guard);
    let (map_byte, map_offset) = heapblk_to_byte_offset(heap_blk);
    let idx = MAP_HEADER_SIZE + map_byte;
    let status = (guard[idx] >> map_offset) & VISIBILITYMAP_VALID_BITS;
    if status != flags {
        guard[idx] |= flags << map_offset;
        let page = *guard;
        pool.write_page_image_locked_with_rmgr(pin.buffer_id(), 0, &page, &mut guard, RM_HEAP2_ID)?;
    }
    Ok(status)
}

pub fn visibilitymap_get_status(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    heap_blk: u32,
    vmbuf: &mut Option<VisibilityMapBuffer>,
) -> Result<u8, VisibilityMapError> {
    let map_block = heapblk_to_mapblock(heap_blk);
    if !visibilitymap_pin_ok(heap_blk, vmbuf) {
        let nblocks =
            pool.with_storage_mut(|storage| storage.smgr.nblocks(rel, ForkNumber::VisibilityMap))?;
        if map_block >= nblocks {
            *vmbuf = None;
            return Ok(0);
        }
        *vmbuf = Some(VisibilityMapBuffer { map_block });
    }
    let pin = pool.pin_existing_block(client_id, rel, ForkNumber::VisibilityMap, map_block)?;
    let guard = pool.lock_buffer_shared(pin.buffer_id())?;
    let (map_byte, map_offset) = heapblk_to_byte_offset(heap_blk);
    Ok((guard[MAP_HEADER_SIZE + map_byte] >> map_offset) & VISIBILITYMAP_VALID_BITS)
}

pub fn visibilitymap_count(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
) -> Result<(u32, u32), VisibilityMapError> {
    let nblocks =
        pool.with_storage_mut(|storage| storage.smgr.nblocks(rel, ForkNumber::VisibilityMap))?;
    let mut all_visible = 0u32;
    let mut all_frozen = 0u32;
    for block in 0..nblocks {
        let pin = pool.pin_existing_block(client_id, rel, ForkNumber::VisibilityMap, block)?;
        let guard = pool.lock_buffer_shared(pin.buffer_id())?;
        for byte in guard[MAP_HEADER_SIZE..].iter().copied() {
            all_visible += (0..4)
                .filter(|pair| ((byte >> (pair * 2)) & VISIBILITYMAP_ALL_VISIBLE) != 0)
                .count() as u32;
            all_frozen += (0..4)
                .filter(|pair| ((byte >> (pair * 2)) & VISIBILITYMAP_ALL_FROZEN) != 0)
                .count() as u32;
        }
    }
    Ok((all_visible, all_frozen))
}

pub fn visibilitymap_prepare_truncate(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    nheapblocks: u32,
) -> Result<Option<u32>, VisibilityMapError> {
    let newnblocks = visibilitymap_truncation_length(nheapblocks);
    let current =
        pool.with_storage_mut(|storage| storage.smgr.nblocks(rel, ForkNumber::VisibilityMap))?;
    if current <= newnblocks {
        return Ok(None);
    }
    if nheapblocks == 0 {
        return Ok(Some(0));
    }

    let trunc_block = heapblk_to_mapblock(nheapblocks);
    let (trunc_byte, trunc_offset) = heapblk_to_byte_offset(nheapblocks);
    if trunc_byte != 0 || trunc_offset != 0 {
        let pin =
            pool.pin_existing_block(client_id, rel, ForkNumber::VisibilityMap, trunc_block)?;
        let mut guard = pool.lock_buffer_exclusive(pin.buffer_id())?;
        ensure_vm_page_initialized(&mut guard);
        let start = MAP_HEADER_SIZE + trunc_byte + 1;
        guard[start..].fill(0);
        guard[MAP_HEADER_SIZE + trunc_byte] &= (1 << trunc_offset) - 1;
        let page = *guard;
        pool.write_page_image_locked_with_rmgr(pin.buffer_id(), 0, &page, &mut guard, RM_HEAP2_ID)?;
    }
    Ok(Some(newnblocks))
}

pub fn visibilitymap_truncation_length(nheapblocks: u32) -> u32 {
    heapblk_to_mapblock_limit(nheapblocks)
}

fn require_map_buffer(
    heap_blk: u32,
    vmbuf: &Option<VisibilityMapBuffer>,
) -> Result<VisibilityMapBuffer, VisibilityMapError> {
    let Some(buffer) = *vmbuf else {
        return Err(VisibilityMapError::WrongMapBuffer);
    };
    if buffer.map_block != heapblk_to_mapblock(heap_blk) {
        return Err(VisibilityMapError::WrongMapBuffer);
    }
    Ok(buffer)
}

fn ensure_vm_page_initialized(page: &mut Page) {
    if pgrust_storage::page::bufpage::page_header(page).is_err() {
        page_init(page, 0);
    }
}

fn heapblk_to_mapblock(heap_block: u32) -> u32 {
    heap_block / HEAPBLOCKS_PER_PAGE as u32
}

fn heapblk_to_mapblock_limit(heap_block: u32) -> u32 {
    heap_block.saturating_add(HEAPBLOCKS_PER_PAGE as u32 - 1) / HEAPBLOCKS_PER_PAGE as u32
}

fn heapblk_to_byte_offset(heap_block: u32) -> (usize, u8) {
    let page_index = heap_block as usize % HEAPBLOCKS_PER_PAGE;
    let map_byte = page_index / HEAPBLOCKS_PER_BYTE;
    let map_offset = ((page_index % HEAPBLOCKS_PER_BYTE) * BITS_PER_HEAPBLOCK) as u8;
    (map_byte, map_offset)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heap_block_mapping_stays_on_expected_boundaries() {
        assert_eq!(heapblk_to_mapblock(0), 0);
        assert_eq!(heapblk_to_byte_offset(0), (0, 0));
        assert_eq!(heapblk_to_byte_offset(1), (0, 2));
        assert_eq!(heapblk_to_byte_offset(3), (0, 6));
        assert_eq!(heapblk_to_byte_offset(4), (1, 0));
        assert_eq!(heapblk_to_mapblock(HEAPBLOCKS_PER_PAGE as u32), 1);
    }
}
