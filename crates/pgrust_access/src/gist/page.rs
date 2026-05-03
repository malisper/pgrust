use pgrust_core::{
    INVALID_LSN, REGBUF_FORCE_IMAGE, REGBUF_STANDARD, REGBUF_WILL_INIT, RM_GIST_ID, XLOG_FPI,
    XLOG_GIST_PAGE_INIT, XLOG_GIST_SPLIT_COMPLETE,
};
use pgrust_storage::page::bufpage::page_header;
use pgrust_storage::smgr::StorageManager;
use pgrust_storage::{
    BufferPool, ClientId, ForkNumber, PinnedBuffer, RelFileLocator, SmgrStorageBackend,
};

use crate::access::gist::{
    F_FOLLOW_RIGHT, F_LEAF, GIST_ROOT_BLKNO, GistPageOpaqueData, gist_page_get_opaque,
    gist_page_init, gist_page_set_opaque,
};
use crate::{AccessError, AccessWalBlockRef, AccessWalRecord, AccessWalServices};

pub(crate) struct GistLoggedPage<'a> {
    pub(crate) block: u32,
    pub(crate) page: &'a [u8; pgrust_storage::BLCKSZ],
    pub(crate) will_init: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GistPageWriteMode {
    Normal,
    Build,
    BuildNoExtend,
}

const GIST_BUILD_RANGE_LOG_CHUNK_BLOCKS: u32 = 32;

fn pin_gist_block<'a>(
    pool: &'a BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    block: u32,
) -> Result<PinnedBuffer<'a, SmgrStorageBackend>, AccessError> {
    pool.pin_existing_block(client_id, rel, ForkNumber::Main, block)
        .map_err(|err| AccessError::Scalar(format!("gist pin block failed: {err:?}")))
}

pub(crate) fn read_buffered_page(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    block: u32,
) -> Result<[u8; pgrust_storage::BLCKSZ], AccessError> {
    let pin = pin_gist_block(pool, client_id, rel, block)?;
    let guard = pool
        .lock_buffer_shared(pin.buffer_id())
        .map_err(|err| AccessError::Scalar(format!("gist shared lock failed: {err:?}")))?;
    let page = *guard;
    drop(guard);
    drop(pin);
    Ok(page)
}

pub(crate) fn write_buffered_page(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    xid: u32,
    rel: RelFileLocator,
    block: u32,
    page: &[u8; pgrust_storage::BLCKSZ],
    wal_info: u8,
    wal: &dyn AccessWalServices,
) -> Result<u64, AccessError> {
    write_buffered_page_with_mode(
        pool,
        client_id,
        xid,
        rel,
        block,
        page,
        wal_info,
        GistPageWriteMode::Normal,
        wal,
    )
}

pub(crate) fn write_buffered_page_with_mode(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    xid: u32,
    rel: RelFileLocator,
    block: u32,
    page: &[u8; pgrust_storage::BLCKSZ],
    wal_info: u8,
    mode: GistPageWriteMode,
    wal: &dyn AccessWalServices,
) -> Result<u64, AccessError> {
    write_logged_pages_with_mode(
        pool,
        client_id,
        xid,
        rel,
        wal_info,
        &[GistLoggedPage {
            block,
            page,
            will_init: false,
        }],
        mode,
        wal,
    )
}

pub(crate) fn write_logged_pages(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    xid: u32,
    rel: RelFileLocator,
    wal_info: u8,
    pages: &[GistLoggedPage<'_>],
    wal: &dyn AccessWalServices,
) -> Result<u64, AccessError> {
    write_logged_pages_with_mode(
        pool,
        client_id,
        xid,
        rel,
        wal_info,
        pages,
        GistPageWriteMode::Normal,
        wal,
    )
}

pub(crate) fn write_logged_pages_with_mode(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    xid: u32,
    rel: RelFileLocator,
    wal_info: u8,
    pages: &[GistLoggedPage<'_>],
    mode: GistPageWriteMode,
    wal: &dyn AccessWalServices,
) -> Result<u64, AccessError> {
    for logged in pages {
        if mode == GistPageWriteMode::BuildNoExtend
            || (mode == GistPageWriteMode::Build && !logged.will_init)
        {
            continue;
        }
        if mode == GistPageWriteMode::Build {
            reserve_blocks_through(pool, rel, logged.block)?;
        } else {
            pool.ensure_block_exists(rel, ForkNumber::Main, logged.block)
                .map_err(|err| AccessError::Scalar(format!("gist extend failed: {err:?}")))?;
        }
    }
    let lsn = if matches!(
        mode,
        GistPageWriteMode::Build | GistPageWriteMode::BuildNoExtend
    ) {
        INVALID_LSN
    } else {
        let blocks = pages
            .iter()
            .enumerate()
            .map(|(_index, logged)| {
                let mut flags = REGBUF_STANDARD | REGBUF_FORCE_IMAGE;
                if logged.will_init {
                    flags |= REGBUF_WILL_INIT;
                }
                AccessWalBlockRef {
                    tag: pgrust_storage::BufferTag {
                        rel,
                        fork: ForkNumber::Main,
                        block: logged.block,
                    },
                    flags,
                    data: logged.page.to_vec(),
                    buffer_data: Vec::new(),
                }
            })
            .collect::<Vec<_>>();
        wal.log_access_record(AccessWalRecord {
            xid,
            rmid: RM_GIST_ID,
            info: wal_info,
            payload: Vec::new(),
            blocks,
        })?
    };
    for logged in pages {
        let pin = pin_gist_block(pool, client_id, rel, logged.block)?;
        let mut guard = pool
            .lock_buffer_exclusive(pin.buffer_id())
            .map_err(|err| AccessError::Scalar(format!("gist exclusive lock failed: {err:?}")))?;
        pool.install_page_image_locked(pin.buffer_id(), logged.page, lsn, &mut guard)
            .map_err(|err| AccessError::Scalar(format!("gist buffered write failed: {err:?}")))?;
    }
    Ok(lsn)
}

pub(crate) fn ensure_relation_exists(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
) -> Result<(), AccessError> {
    pool.ensure_relation_fork(rel, ForkNumber::Main)
        .map_err(|err| AccessError::Scalar(format!("gist ensure relation failed: {err:?}")))
}

fn truncate_relation(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
) -> Result<(), AccessError> {
    pool.with_storage_mut(|storage| {
        storage.smgr.truncate(rel, ForkNumber::Main, 0)?;
        let _ = storage.smgr.truncate(rel, ForkNumber::Fsm, 0);
        Ok::<(), pgrust_storage::smgr::SmgrError>(())
    })
    .map_err(|err| AccessError::Scalar(err.to_string()))
}

pub(crate) fn relation_nblocks(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
) -> Result<u32, AccessError> {
    pool.with_storage_mut(|storage| storage.smgr.nblocks(rel, ForkNumber::Main))
        .map_err(|err| AccessError::Scalar(err.to_string()))
}

pub(crate) fn allocate_new_block(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
) -> Result<u32, AccessError> {
    let block = relation_nblocks(pool, rel)?;
    pool.ensure_block_exists(rel, ForkNumber::Main, block)
        .map_err(|err| AccessError::Scalar(format!("gist extend failed: {err:?}")))?;
    Ok(block)
}

pub(crate) fn allocate_new_block_with_mode(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
    mode: GistPageWriteMode,
) -> Result<u32, AccessError> {
    if !matches!(
        mode,
        GistPageWriteMode::Build | GistPageWriteMode::BuildNoExtend
    ) {
        return allocate_new_block(pool, rel);
    }

    pool.with_storage_mut(|storage| {
        let block = storage.smgr.nblocks(rel, ForkNumber::Main)?;
        storage
            .smgr
            .reserve_block(rel, ForkNumber::Main, block, true)?;
        Ok::<_, pgrust_storage::smgr::SmgrError>(block)
    })
    .map_err(|err| AccessError::Scalar(format!("gist reserve block failed: {err}")))
}

fn reserve_blocks_through(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
    block: u32,
) -> Result<(), AccessError> {
    pool.ensure_relation_fork(rel, ForkNumber::Main)
        .map_err(|err| AccessError::Scalar(format!("gist ensure relation failed: {err:?}")))?;
    pool.with_storage_mut(|storage| {
        let mut nblocks = storage.smgr.nblocks(rel, ForkNumber::Main)?;
        while nblocks <= block {
            storage
                .smgr
                .reserve_block(rel, ForkNumber::Main, nblocks, true)?;
            nblocks += 1;
        }
        Ok::<_, pgrust_storage::smgr::SmgrError>(())
    })
    .map_err(|err| AccessError::Scalar(format!("gist reserve block failed: {err}")))
}

pub(crate) fn ensure_empty_gist(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    xid: u32,
    rel: RelFileLocator,
    wal: &dyn AccessWalServices,
) -> Result<(), AccessError> {
    ensure_empty_gist_with_mode(pool, client_id, xid, rel, GistPageWriteMode::Normal, wal)
}

pub(crate) fn ensure_empty_gist_with_mode(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    xid: u32,
    rel: RelFileLocator,
    mode: GistPageWriteMode,
    wal: &dyn AccessWalServices,
) -> Result<(), AccessError> {
    ensure_relation_exists(pool, rel)?;
    truncate_relation(pool, rel)?;
    let mut root = [0u8; pgrust_storage::BLCKSZ];
    gist_page_init(&mut root, F_LEAF)
        .map_err(|err| AccessError::Scalar(format!("gist root init failed: {err:?}")))?;
    write_logged_pages_with_mode(
        pool,
        client_id,
        xid,
        rel,
        XLOG_GIST_PAGE_INIT,
        &[GistLoggedPage {
            block: GIST_ROOT_BLKNO,
            page: &root,
            will_init: true,
        }],
        mode,
        wal,
    )?;
    Ok(())
}

pub(crate) fn log_gist_build_newpage_range(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    xid: u32,
    rel: RelFileLocator,
    wal: &dyn AccessWalServices,
) -> Result<(), AccessError> {
    let nblocks = relation_nblocks(pool, rel)?;
    let mut start = 0u32;
    while start < nblocks {
        let end = start
            .saturating_add(GIST_BUILD_RANGE_LOG_CHUNK_BLOCKS)
            .min(nblocks);
        let mut images = Vec::with_capacity((end - start) as usize);
        for block in start..end {
            images.push((block, read_buffered_page(pool, client_id, rel, block)?));
        }
        let logged_pages = images
            .iter()
            .map(|(block, page)| GistLoggedPage {
                block: *block,
                page,
                will_init: true,
            })
            .collect::<Vec<_>>();
        write_logged_pages(pool, client_id, xid, rel, XLOG_FPI, &logged_pages, wal)?;
        start = end;
    }
    Ok(())
}

pub(crate) fn page_lsn(page: &[u8; pgrust_storage::BLCKSZ]) -> u64 {
    page_header(page).map(|header| header.pd_lsn).unwrap_or(0)
}

pub(crate) fn clear_follow_right_with_mode(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    xid: u32,
    rel: RelFileLocator,
    block: u32,
    nsn: u64,
    mode: GistPageWriteMode,
    wal: &dyn AccessWalServices,
) -> Result<(), AccessError> {
    let mut page = read_buffered_page(pool, client_id, rel, block)?;
    let mut opaque = gist_page_get_opaque(&page)
        .map_err(|err| AccessError::Scalar(format!("gist opaque read failed: {err:?}")))?;
    if opaque.flags & F_FOLLOW_RIGHT == 0 {
        return Ok(());
    }
    opaque.flags &= !F_FOLLOW_RIGHT;
    opaque.nsn = opaque.nsn.max(nsn);
    gist_page_set_opaque(&mut page, opaque)
        .map_err(|err| AccessError::Scalar(format!("gist opaque write failed: {err:?}")))?;
    write_buffered_page_with_mode(
        pool,
        client_id,
        xid,
        rel,
        block,
        &page,
        XLOG_GIST_SPLIT_COMPLETE,
        mode,
        wal,
    )?;
    Ok(())
}

pub(crate) fn init_opaque(flags: u16, rightlink: u32, nsn: u64) -> GistPageOpaqueData {
    let mut opaque = GistPageOpaqueData::new(flags);
    opaque.rightlink = rightlink;
    opaque.nsn = nsn;
    opaque
}
