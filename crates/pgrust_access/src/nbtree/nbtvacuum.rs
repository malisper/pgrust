use crate::access::amapi::{IndexBulkDeleteCallback, IndexBulkDeleteResult, IndexVacuumContext};
use crate::access::nbtree::{
    BTP_DELETED, BTP_HALF_DEAD, BTREE_METAPAGE, P_NONE, bt_page_data_items, bt_page_get_meta,
    bt_page_get_opaque, bt_page_high_key, bt_page_init, bt_page_is_recyclable, bt_page_items,
    bt_page_replace_items, bt_page_set_deleted, bt_page_set_high_key, bt_page_set_meta,
    bt_page_set_opaque,
};
use crate::nbtree::runtime::{LoggedBtreeBlock, ensure_empty_btree, log_btree_record};
use crate::{AccessError, AccessTransactionServices, AccessWalServices};
use pgrust_core::INVALID_TRANSACTION_ID;
use pgrust_core::{
    XLOG_BTREE_DELETE, XLOG_BTREE_MARK_PAGE_HALFDEAD, XLOG_BTREE_UNLINK_PAGE, XLOG_BTREE_VACUUM,
};
use pgrust_storage::fsm::{finalize_pending_index_pages, record_free_index_page};
use pgrust_storage::page::bufpage::{PageError, page_header};
use pgrust_storage::smgr::{ForkNumber, StorageManager};
use pgrust_storage::{BufferPool, ClientId, PinnedBuffer, SmgrStorageBackend};

type CatalogError = AccessError;

fn pin_btree_block<'a>(
    pool: &'a BufferPool<SmgrStorageBackend>,
    client_id: ClientId,
    rel: pgrust_storage::smgr::RelFileLocator,
    block: u32,
) -> Result<PinnedBuffer<'a, SmgrStorageBackend>, CatalogError> {
    pool.pin_existing_block(client_id, rel, ForkNumber::Main, block)
        .map_err(|err| CatalogError::Io(format!("btree pin block failed: {err:?}")))
}

fn relation_nblocks(
    pool: &BufferPool<SmgrStorageBackend>,
    rel: pgrust_storage::smgr::RelFileLocator,
) -> Result<u32, CatalogError> {
    pool.with_storage_mut(|storage| storage.smgr.nblocks(rel, ForkNumber::Main))
        .map_err(|err| CatalogError::Io(err.to_string()))
}

fn bt_page_is_new(page: &[u8; pgrust_storage::smgr::BLCKSZ]) -> Result<bool, CatalogError> {
    match page_header(page) {
        Ok(_) => Ok(false),
        Err(PageError::NotInitialized) => Ok(true),
        Err(err) => Err(CatalogError::Io(format!(
            "btree page header read failed: {err:?}"
        ))),
    }
}

fn write_cleanup_info(
    ctx: &IndexVacuumContext,
    deleted_pages: u32,
    wal: &dyn AccessWalServices,
) -> Result<(), CatalogError> {
    let pin = pin_btree_block(&ctx.pool, ctx.client_id, ctx.index_relation, BTREE_METAPAGE)?;
    let mut guard = ctx
        .pool
        .lock_buffer_exclusive(pin.buffer_id())
        .map_err(|err| CatalogError::Io(format!("btree meta lock failed: {err:?}")))?;
    let mut page = *guard;
    let mut meta = bt_page_get_meta(&page)
        .map_err(|err| CatalogError::Io(format!("btree metapage read failed: {err:?}")))?;
    meta.btm_last_cleanup_num_delpages = deleted_pages;
    meta.btm_last_cleanup_num_heap_tuples = -1.0;
    bt_page_set_meta(&mut page, meta)
        .map_err(|err| CatalogError::Io(format!("btree metapage write failed: {err:?}")))?;
    let lsn = log_btree_record(
        wal,
        INVALID_TRANSACTION_ID,
        XLOG_BTREE_VACUUM,
        &[LoggedBtreeBlock {
            block_id: 0,
            tag: pgrust_storage::BufferTag {
                rel: ctx.index_relation,
                fork: ForkNumber::Main,
                block: BTREE_METAPAGE,
            },
            page: &page,
            will_init: false,
            force_image: true,
            data: &[],
        }],
        &[],
    )
    .map_err(|err| CatalogError::Io(format!("btree WAL log failed: {err}")))?;
    ctx.pool
        .install_page_image_locked(pin.buffer_id(), &page, lsn, &mut guard)
        .map_err(|err| CatalogError::Io(format!("btree metapage write failed: {err:?}")))
}

fn log_page_image(
    ctx: &IndexVacuumContext,
    xid: u32,
    info: u8,
    block: u32,
    page: &[u8; pgrust_storage::BLCKSZ],
    wal: &dyn AccessWalServices,
) -> Result<u64, CatalogError> {
    log_btree_record(
        wal,
        xid,
        info,
        &[LoggedBtreeBlock {
            block_id: 0,
            tag: pgrust_storage::BufferTag {
                rel: ctx.index_relation,
                fork: ForkNumber::Main,
                block,
            },
            page,
            will_init: false,
            force_image: true,
            data: &[],
        }],
        &[],
    )
    .map_err(|err| CatalogError::Io(format!("btree WAL log failed: {err}")))
}

fn find_parent_block(
    ctx: &IndexVacuumContext,
    child_block: u32,
) -> Result<Option<u32>, CatalogError> {
    let nblocks = relation_nblocks(&ctx.pool, ctx.index_relation)?;
    for block in 1..nblocks {
        if block == child_block {
            continue;
        }
        let pin = pin_btree_block(&ctx.pool, ctx.client_id, ctx.index_relation, block)?;
        let guard = ctx
            .pool
            .lock_buffer_shared(pin.buffer_id())
            .map_err(|err| CatalogError::Io(format!("btree shared lock failed: {err:?}")))?;
        let opaque = bt_page_get_opaque(&guard)
            .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
        if opaque.is_meta() || opaque.is_leaf() || opaque.btpo_flags & BTP_DELETED != 0 {
            continue;
        }
        let items = bt_page_data_items(&guard)
            .map_err(|err| CatalogError::Io(format!("btree page parse failed: {err:?}")))?;
        if items
            .iter()
            .any(|item| item.t_tid.block_number == child_block)
        {
            return Ok(Some(block));
        }
    }
    Ok(None)
}

fn remove_child_from_parent(
    ctx: &IndexVacuumContext,
    parent_block: u32,
    child_block: u32,
    wal: &dyn AccessWalServices,
) -> Result<usize, CatalogError> {
    let pin = pin_btree_block(&ctx.pool, ctx.client_id, ctx.index_relation, parent_block)?;
    let mut guard = ctx
        .pool
        .lock_buffer_exclusive(pin.buffer_id())
        .map_err(|err| CatalogError::Io(format!("btree exclusive lock failed: {err:?}")))?;
    let mut page = *guard;
    let opaque = bt_page_get_opaque(&page)
        .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
    let mut items = bt_page_items(&page)
        .map_err(|err| CatalogError::Io(format!("btree page parse failed: {err:?}")))?;
    let before = items.len();
    if before <= 1 {
        return Ok(0);
    }
    items.retain(|item| item.t_tid.block_number != child_block);
    if items.len() == before {
        return Ok(0);
    }
    bt_page_replace_items(&mut page, &items, opaque)
        .map_err(|err| CatalogError::Io(format!("btree parent rebuild failed: {err:?}")))?;
    let lsn = log_page_image(
        ctx,
        INVALID_TRANSACTION_ID,
        XLOG_BTREE_UNLINK_PAGE,
        parent_block,
        &page,
        wal,
    )?;
    ctx.pool
        .install_page_image_locked(pin.buffer_id(), &page, lsn, &mut guard)
        .map_err(|err| CatalogError::Io(format!("btree buffered write failed: {err:?}")))?;
    Ok(items.len())
}

fn delete_empty_leaf(
    ctx: &IndexVacuumContext,
    block: u32,
    oldest_active_xid: u32,
    wal: &dyn AccessWalServices,
) -> Result<bool, CatalogError> {
    let pin = pin_btree_block(&ctx.pool, ctx.client_id, ctx.index_relation, block)?;
    let mut guard = ctx
        .pool
        .lock_buffer_exclusive(pin.buffer_id())
        .map_err(|err| CatalogError::Io(format!("btree exclusive lock failed: {err:?}")))?;
    let mut page = *guard;
    let mut opaque = bt_page_get_opaque(&page)
        .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
    if opaque.is_root() || !opaque.is_leaf() || opaque.btpo_flags & BTP_DELETED != 0 {
        return Ok(false);
    }

    let Some(parent_block) = find_parent_block(ctx, block)? else {
        return Ok(false);
    };
    let remaining_parent = remove_child_from_parent(ctx, parent_block, block, wal)?;
    if remaining_parent == 0 {
        return Ok(false);
    }

    if opaque.btpo_prev != P_NONE {
        let prev_block = opaque.btpo_prev;
        let prev_pin = pin_btree_block(&ctx.pool, ctx.client_id, ctx.index_relation, prev_block)?;
        let mut prev_guard = ctx
            .pool
            .lock_buffer_exclusive(prev_pin.buffer_id())
            .map_err(|err| CatalogError::Io(format!("btree exclusive lock failed: {err:?}")))?;
        let mut prev_page = *prev_guard;
        let mut prev_opaque = bt_page_get_opaque(&prev_page)
            .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
        prev_opaque.btpo_next = opaque.btpo_next;
        bt_page_set_opaque(&mut prev_page, prev_opaque)
            .map_err(|err| CatalogError::Io(format!("btree opaque write failed: {err:?}")))?;
        let lsn = log_page_image(
            ctx,
            INVALID_TRANSACTION_ID,
            XLOG_BTREE_UNLINK_PAGE,
            prev_block,
            &prev_page,
            wal,
        )?;
        ctx.pool
            .install_page_image_locked(prev_pin.buffer_id(), &prev_page, lsn, &mut prev_guard)
            .map_err(|err| CatalogError::Io(format!("btree buffered write failed: {err:?}")))?;
    }

    if opaque.btpo_next != P_NONE {
        let next_block = opaque.btpo_next;
        let next_pin = pin_btree_block(&ctx.pool, ctx.client_id, ctx.index_relation, next_block)?;
        let mut next_guard = ctx
            .pool
            .lock_buffer_exclusive(next_pin.buffer_id())
            .map_err(|err| CatalogError::Io(format!("btree exclusive lock failed: {err:?}")))?;
        let mut next_page = *next_guard;
        let mut next_opaque = bt_page_get_opaque(&next_page)
            .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
        next_opaque.btpo_prev = opaque.btpo_prev;
        bt_page_set_opaque(&mut next_page, next_opaque)
            .map_err(|err| CatalogError::Io(format!("btree opaque write failed: {err:?}")))?;
        let lsn = log_page_image(
            ctx,
            INVALID_TRANSACTION_ID,
            XLOG_BTREE_UNLINK_PAGE,
            next_block,
            &next_page,
            wal,
        )?;
        ctx.pool
            .install_page_image_locked(next_pin.buffer_id(), &next_page, lsn, &mut next_guard)
            .map_err(|err| CatalogError::Io(format!("btree buffered write failed: {err:?}")))?;
    }

    opaque.btpo_flags |= BTP_HALF_DEAD;
    bt_page_set_opaque(&mut page, opaque)
        .map_err(|err| CatalogError::Io(format!("btree opaque write failed: {err:?}")))?;
    let lsn = log_page_image(
        ctx,
        INVALID_TRANSACTION_ID,
        XLOG_BTREE_MARK_PAGE_HALFDEAD,
        block,
        &page,
        wal,
    )?;
    ctx.pool
        .install_page_image_locked(pin.buffer_id(), &page, lsn, &mut guard)
        .map_err(|err| CatalogError::Io(format!("btree buffered write failed: {err:?}")))?;

    let mut deleted = [0u8; pgrust_storage::smgr::BLCKSZ];
    bt_page_init(&mut deleted, opaque.btpo_flags, opaque.btpo_level)
        .map_err(|err| CatalogError::Io(format!("btree deleted page init failed: {err:?}")))?;
    bt_page_set_deleted(&mut deleted, opaque, oldest_active_xid.saturating_sub(1))
        .map_err(|err| CatalogError::Io(format!("btree deleted page write failed: {err:?}")))?;
    let lsn = log_page_image(
        ctx,
        INVALID_TRANSACTION_ID,
        XLOG_BTREE_DELETE,
        block,
        &deleted,
        wal,
    )?;
    ctx.pool
        .install_page_image_locked(pin.buffer_id(), &deleted, lsn, &mut guard)
        .map_err(|err| CatalogError::Io(format!("btree buffered write failed: {err:?}")))?;
    record_free_index_page(&ctx.pool, ctx.index_relation, block).map_err(CatalogError::Io)?;
    Ok(true)
}

pub fn btbulkdelete(
    ctx: &IndexVacuumContext,
    callback: &IndexBulkDeleteCallback<'_>,
    stats: Option<IndexBulkDeleteResult>,
    txns: &dyn AccessTransactionServices,
    wal: &dyn AccessWalServices,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    let mut stats = stats.unwrap_or_default();
    let oldest_active_xid = txns.oldest_active_xid();
    let nblocks = relation_nblocks(&ctx.pool, ctx.index_relation)?;
    for block in 1..nblocks {
        let pin = pin_btree_block(&ctx.pool, ctx.client_id, ctx.index_relation, block)?;
        let mut guard = ctx
            .pool
            .lock_buffer_exclusive(pin.buffer_id())
            .map_err(|err| CatalogError::Io(format!("btree exclusive lock failed: {err:?}")))?;
        let mut page = *guard;
        if bt_page_is_new(&page)? {
            record_free_index_page(&ctx.pool, ctx.index_relation, block)
                .map_err(CatalogError::Io)?;
            stats.num_deleted_pages += 1;
            continue;
        }
        let opaque = bt_page_get_opaque(&page)
            .map_err(|err| CatalogError::Io(format!("btree opaque read failed: {err:?}")))?;
        if opaque.is_meta() {
            continue;
        }
        if bt_page_is_recyclable(&page, oldest_active_xid)
            .map_err(|err| CatalogError::Io(format!("btree recyclable check failed: {err:?}")))?
        {
            record_free_index_page(&ctx.pool, ctx.index_relation, block)
                .map_err(CatalogError::Io)?;
            stats.num_deleted_pages += 1;
            continue;
        }
        if !opaque.is_leaf() || opaque.btpo_flags & BTP_DELETED != 0 {
            continue;
        }
        let items = bt_page_data_items(&page)
            .map_err(|err| CatalogError::Io(format!("btree page parse failed: {err:?}")))?;
        let high_key = bt_page_high_key(&page)
            .map_err(|err| CatalogError::Io(format!("btree high-key read failed: {err:?}")))?;
        stats.num_pages += 1;
        stats.num_index_tuples += items.len() as u64;

        let mut live = Vec::with_capacity(items.len());
        let mut removed = 0u64;
        for item in items {
            if callback(item.t_tid) {
                removed += 1;
            } else {
                live.push(item);
            }
        }

        if removed == 0 {
            continue;
        }

        stats.num_removed_tuples += removed;
        if live.is_empty() {
            drop(guard);
            drop(pin);
            if delete_empty_leaf(ctx, block, txns.oldest_active_xid(), wal)? {
                stats.num_deleted_pages += 1;
            }
            continue;
        }

        if let Some(high_key) = high_key.as_ref() {
            bt_page_set_high_key(&mut page, high_key, live, opaque)
                .map_err(|err| CatalogError::Io(format!("btree vacuum rebuild failed: {err:?}")))?;
        } else {
            bt_page_replace_items(&mut page, &live, opaque)
                .map_err(|err| CatalogError::Io(format!("btree vacuum rebuild failed: {err:?}")))?;
        }
        let lsn = log_page_image(
            ctx,
            INVALID_TRANSACTION_ID,
            XLOG_BTREE_VACUUM,
            block,
            &page,
            wal,
        )?;
        ctx.pool
            .install_page_image_locked(pin.buffer_id(), &page, lsn, &mut guard)
            .map_err(|err| CatalogError::Io(format!("btree buffered write failed: {err:?}")))?;
    }
    // :HACK: Until btree page deletion/recycling is as complete as PostgreSQL's,
    // compact a fully emptied index to a fresh root page. This keeps later
    // inserts from walking stale deleted-page paths after VACUUM.
    if stats.num_index_tuples > 0 && stats.num_removed_tuples == stats.num_index_tuples {
        ensure_empty_btree(
            &ctx.pool,
            ctx.client_id,
            INVALID_TRANSACTION_ID,
            ctx.index_relation,
            wal,
        )?;
    }
    Ok(stats)
}

pub fn btvacuumcleanup(
    ctx: &IndexVacuumContext,
    stats: Option<IndexBulkDeleteResult>,
    txns: &dyn AccessTransactionServices,
    wal: &dyn AccessWalServices,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    let scanned_by_bulkdelete = stats.is_some();
    let mut stats = stats.unwrap_or_default();
    let oldest_active_xid = txns.oldest_active_xid();
    let mut cleanup_deleted_pages = 0;
    if !scanned_by_bulkdelete {
        let nblocks = relation_nblocks(&ctx.pool, ctx.index_relation)?;
        let mut recyclable = Vec::new();
        for block in 1..nblocks {
            let pin = pin_btree_block(&ctx.pool, ctx.client_id, ctx.index_relation, block)?;
            let guard = ctx
                .pool
                .lock_buffer_shared(pin.buffer_id())
                .map_err(|err| CatalogError::Io(format!("btree shared lock failed: {err:?}")))?;
            if bt_page_is_new(&guard)?
                || bt_page_is_recyclable(&guard, oldest_active_xid).map_err(|err| {
                    CatalogError::Io(format!("btree recyclable check failed: {err:?}"))
                })?
            {
                recyclable.push(block);
            }
        }
        finalize_pending_index_pages(&ctx.pool, ctx.index_relation, &recyclable)
            .map_err(CatalogError::Io)?;
        stats.num_deleted_pages += recyclable.len() as u64;
        cleanup_deleted_pages = recyclable.len() as u32;
    }
    write_cleanup_info(ctx, cleanup_deleted_pages, wal)?;
    Ok(stats)
}
