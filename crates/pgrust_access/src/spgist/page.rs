use pgrust_core::{
    REGBUF_FORCE_IMAGE, REGBUF_STANDARD, REGBUF_WILL_INIT, RM_GIST_ID, XLOG_GIST_PAGE_INIT,
};
use pgrust_storage::smgr::StorageManager;
use pgrust_storage::{
    BLCKSZ, BufferPool, ClientId, ForkNumber, PinnedBuffer, RelFileLocator, SmgrStorageBackend,
};

use crate::access::spgist::{
    F_LEAF, SPGIST_ROOT_BLKNO, SpgistPageOpaqueData, spgist_page_get_opaque, spgist_page_init,
};
use crate::{AccessError, AccessWalBlockRef, AccessWalRecord, AccessWalServices};

pub(crate) struct SpgistLoggedPage<'a> {
    pub(crate) block: u32,
    pub(crate) page: &'a [u8; BLCKSZ],
    pub(crate) will_init: bool,
}

fn pin_spgist_block<'a>(
    pool: &'a BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    block: u32,
) -> Result<PinnedBuffer<'a, SmgrStorageBackend>, AccessError> {
    pool.pin_existing_block(client_id, rel, ForkNumber::Main, block)
        .map_err(|err| AccessError::Scalar(format!("spgist pin block failed: {err:?}")))
}

pub(crate) fn read_buffered_page(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    block: u32,
) -> Result<[u8; BLCKSZ], AccessError> {
    let pin = pin_spgist_block(pool, client_id, rel, block)?;
    let guard = pool
        .lock_buffer_shared(pin.buffer_id())
        .map_err(|err| AccessError::Scalar(format!("spgist shared lock failed: {err:?}")))?;
    let page = *guard;
    drop(guard);
    drop(pin);
    Ok(page)
}

pub(crate) fn write_logged_pages(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    xid: u32,
    rel: RelFileLocator,
    wal_info: u8,
    pages: &[SpgistLoggedPage<'_>],
    wal: &dyn AccessWalServices,
) -> Result<u64, AccessError> {
    for logged in pages {
        pool.ensure_block_exists(rel, ForkNumber::Main, logged.block)
            .map_err(|err| AccessError::Scalar(format!("spgist extend failed: {err:?}")))?;
    }
    let blocks = pages
        .iter()
        .map(|logged| {
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
    let lsn = wal.log_access_record(AccessWalRecord {
        xid,
        rmid: RM_GIST_ID,
        info: wal_info,
        payload: Vec::new(),
        blocks,
    })?;
    for logged in pages {
        let pin = pin_spgist_block(pool, client_id, rel, logged.block)?;
        let mut guard = pool
            .lock_buffer_exclusive(pin.buffer_id())
            .map_err(|err| AccessError::Scalar(format!("spgist exclusive lock failed: {err:?}")))?;
        pool.install_page_image_locked(pin.buffer_id(), logged.page, lsn, &mut guard)
            .map_err(|err| AccessError::Scalar(format!("spgist buffered write failed: {err:?}")))?;
    }
    Ok(lsn)
}

pub(crate) fn write_buffered_page(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    xid: u32,
    rel: RelFileLocator,
    block: u32,
    page: &[u8; BLCKSZ],
    wal_info: u8,
    wal: &dyn AccessWalServices,
) -> Result<u64, AccessError> {
    write_logged_pages(
        pool,
        client_id,
        xid,
        rel,
        wal_info,
        &[SpgistLoggedPage {
            block,
            page,
            will_init: false,
        }],
        wal,
    )
}

pub(crate) fn ensure_relation_exists(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
) -> Result<(), AccessError> {
    pool.ensure_relation_fork(rel, ForkNumber::Main)
        .map_err(|err| AccessError::Scalar(format!("spgist ensure relation failed: {err:?}")))
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
        .map_err(|err| AccessError::Scalar(format!("spgist extend failed: {err:?}")))?;
    Ok(block)
}

pub(crate) fn ensure_empty_spgist(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    xid: u32,
    rel: RelFileLocator,
    wal: &dyn AccessWalServices,
) -> Result<(), AccessError> {
    ensure_relation_exists(pool, rel)?;
    truncate_relation(pool, rel)?;
    let mut root = [0u8; BLCKSZ];
    spgist_page_init(&mut root, F_LEAF)
        .map_err(|err| AccessError::Io(format!("spgist root init failed: {err:?}")))?;
    write_logged_pages(
        pool,
        client_id,
        xid,
        rel,
        XLOG_GIST_PAGE_INIT,
        &[SpgistLoggedPage {
            block: SPGIST_ROOT_BLKNO,
            page: &root,
            will_init: true,
        }],
        wal,
    )?;
    Ok(())
}

pub(crate) fn page_opaque(page: &[u8; BLCKSZ]) -> Result<SpgistPageOpaqueData, AccessError> {
    spgist_page_get_opaque(page)
        .map_err(|err| AccessError::Io(format!("spgist page parse failed: {err:?}")))
}
