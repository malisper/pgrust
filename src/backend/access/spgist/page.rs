use crate::backend::access::gist::wal::{LoggedGistBlock, log_gist_record};
use crate::backend::access::transam::xlog::{INVALID_LSN, XLOG_GIST_PAGE_INIT};
use crate::backend::catalog::CatalogError;
use crate::backend::storage::smgr::{ForkNumber, RelFileLocator, StorageManager};
use crate::include::access::spgist::{
    F_LEAF, SPGIST_ROOT_BLKNO, SpgistPageOpaqueData, spgist_page_get_opaque, spgist_page_init,
};
use crate::{BufferPool, ClientId, PinnedBuffer, SmgrStorageBackend};

pub(crate) struct SpgistLoggedPage<'a> {
    pub(crate) block: u32,
    pub(crate) page: &'a [u8; crate::backend::storage::smgr::BLCKSZ],
    pub(crate) will_init: bool,
}

fn pin_spgist_block<'a>(
    pool: &'a BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    block: u32,
) -> Result<PinnedBuffer<'a, SmgrStorageBackend>, CatalogError> {
    pool.pin_existing_block(client_id, rel, ForkNumber::Main, block)
        .map_err(|err| CatalogError::Io(format!("spgist pin block failed: {err:?}")))
}

pub(crate) fn read_buffered_page(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: RelFileLocator,
    block: u32,
) -> Result<[u8; crate::backend::storage::smgr::BLCKSZ], CatalogError> {
    let pin = pin_spgist_block(pool, client_id, rel, block)?;
    let guard = pool
        .lock_buffer_shared(pin.buffer_id())
        .map_err(|err| CatalogError::Io(format!("spgist shared lock failed: {err:?}")))?;
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
) -> Result<u64, CatalogError> {
    for logged in pages {
        pool.ensure_block_exists(rel, ForkNumber::Main, logged.block)
            .map_err(|err| CatalogError::Io(format!("spgist extend failed: {err:?}")))?;
    }
    let lsn = if let Some(wal) = pool.wal_writer() {
        let blocks = pages
            .iter()
            .enumerate()
            .map(|(index, logged)| LoggedGistBlock {
                block_id: index as u8,
                tag: crate::backend::storage::buffer::BufferTag {
                    rel,
                    fork: ForkNumber::Main,
                    block: logged.block,
                },
                page: logged.page,
                will_init: logged.will_init,
            })
            .collect::<Vec<_>>();
        log_gist_record(&wal, xid, wal_info, &blocks, &[])
            .map_err(|err| CatalogError::Io(format!("spgist WAL log failed: {err}")))?
    } else {
        INVALID_LSN
    };
    for logged in pages {
        let pin = pin_spgist_block(pool, client_id, rel, logged.block)?;
        let mut guard = pool
            .lock_buffer_exclusive(pin.buffer_id())
            .map_err(|err| CatalogError::Io(format!("spgist exclusive lock failed: {err:?}")))?;
        pool.install_page_image_locked(pin.buffer_id(), logged.page, lsn, &mut guard)
            .map_err(|err| CatalogError::Io(format!("spgist buffered write failed: {err:?}")))?;
    }
    Ok(lsn)
}

pub(crate) fn write_buffered_page(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    xid: u32,
    rel: RelFileLocator,
    block: u32,
    page: &[u8; crate::backend::storage::smgr::BLCKSZ],
    wal_info: u8,
) -> Result<u64, CatalogError> {
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
    )
}

pub(crate) fn ensure_relation_exists(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
) -> Result<(), CatalogError> {
    pool.ensure_relation_fork(rel, ForkNumber::Main)
        .map_err(|err| CatalogError::Io(format!("spgist ensure relation failed: {err:?}")))
}

fn truncate_relation(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
) -> Result<(), CatalogError> {
    pool.with_storage_mut(|storage| {
        storage.smgr.truncate(rel, ForkNumber::Main, 0)?;
        let _ = storage.smgr.truncate(rel, ForkNumber::Fsm, 0);
        Ok::<(), crate::backend::storage::smgr::SmgrError>(())
    })
    .map_err(|err| CatalogError::Io(err.to_string()))
}

pub(crate) fn relation_nblocks(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
) -> Result<u32, CatalogError> {
    pool.with_storage_mut(|storage| storage.smgr.nblocks(rel, ForkNumber::Main))
        .map_err(|err| CatalogError::Io(err.to_string()))
}

pub(crate) fn allocate_new_block(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: RelFileLocator,
) -> Result<u32, CatalogError> {
    let block = relation_nblocks(pool, rel)?;
    pool.ensure_block_exists(rel, ForkNumber::Main, block)
        .map_err(|err| CatalogError::Io(format!("spgist extend failed: {err:?}")))?;
    Ok(block)
}

pub(crate) fn ensure_empty_spgist(
    pool: &BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    xid: u32,
    rel: RelFileLocator,
) -> Result<(), CatalogError> {
    ensure_relation_exists(pool, rel)?;
    truncate_relation(pool, rel)?;
    let mut root = [0u8; crate::backend::storage::smgr::BLCKSZ];
    spgist_page_init(&mut root, F_LEAF)
        .map_err(|err| CatalogError::Io(format!("spgist root init failed: {err:?}")))?;
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
    )?;
    Ok(())
}

pub(crate) fn page_opaque(
    page: &[u8; crate::backend::storage::smgr::BLCKSZ],
) -> Result<SpgistPageOpaqueData, CatalogError> {
    spgist_page_get_opaque(page)
        .map_err(|err| CatalogError::Io(format!("spgist page parse failed: {err:?}")))
}
