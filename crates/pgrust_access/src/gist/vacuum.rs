use crate::access::amapi::{IndexBulkDeleteCallback, IndexBulkDeleteResult, IndexVacuumContext};
use crate::access::gist::{
    F_HAS_GARBAGE, F_TUPLES_DELETED, gist_page_get_opaque, gist_page_items,
    gist_page_items_with_offsets, gist_page_replace_items,
};
use crate::{AccessError, AccessWalServices};
use pgrust_core::{INVALID_TRANSACTION_ID, XLOG_GIST_VACUUM};

use super::page::{read_buffered_page, relation_nblocks, write_buffered_page};

fn gist_tuple_count(
    pool: &pgrust_storage::BufferPool<pgrust_storage::SmgrStorageBackend>,
    client_id: pgrust_core::ClientId,
    rel: pgrust_storage::RelFileLocator,
) -> Result<u64, AccessError> {
    let nblocks = relation_nblocks(pool, rel)?;
    let mut count = 0u64;
    for block in 0..nblocks {
        let page = read_buffered_page(pool, client_id, rel, block)?;
        let opaque = gist_page_get_opaque(&page)
            .map_err(|err| AccessError::Scalar(format!("gist page parse failed: {err:?}")))?;
        if opaque.is_deleted() || !opaque.is_leaf() {
            continue;
        }
        count += gist_page_items(&page)
            .map_err(|err| AccessError::Scalar(format!("gist tuple parse failed: {err:?}")))?
            .len() as u64;
    }
    Ok(count)
}

fn vacuum_leaf_page(
    ctx: &IndexVacuumContext,
    block: u32,
    callback: &IndexBulkDeleteCallback<'_>,
    wal: &dyn AccessWalServices,
) -> Result<(u64, u64), AccessError> {
    let page = read_buffered_page(&ctx.pool, ctx.client_id, ctx.index_relation, block)?;
    let mut opaque = gist_page_get_opaque(&page)
        .map_err(|err| AccessError::Scalar(format!("gist page parse failed: {err:?}")))?;
    if opaque.is_deleted() || !opaque.is_leaf() {
        return Ok((0, 0));
    }

    let items = gist_page_items_with_offsets(&page)
        .map_err(|err| AccessError::Scalar(format!("gist tuple parse failed: {err:?}")))?;
    let mut survivors = Vec::with_capacity(items.len());
    let mut removed = 0u64;
    for (_, tuple) in items {
        if callback(tuple.t_tid) {
            removed += 1;
        } else {
            survivors.push(tuple);
        }
    }
    if removed == 0 {
        return Ok((survivors.len() as u64, 0));
    }

    opaque.flags |= F_TUPLES_DELETED;
    opaque.flags &= !F_HAS_GARBAGE;

    let mut rebuilt = [0u8; pgrust_storage::BLCKSZ];
    gist_page_replace_items(&mut rebuilt, &survivors, opaque)
        .map_err(|err| AccessError::Scalar(format!("gist vacuum rebuild failed: {err:?}")))?;
    write_buffered_page(
        &ctx.pool,
        ctx.client_id,
        INVALID_TRANSACTION_ID,
        ctx.index_relation,
        block,
        &rebuilt,
        XLOG_GIST_VACUUM,
        wal,
    )?;
    Ok((survivors.len() as u64, removed))
}

pub fn gistbulkdelete(
    ctx: &IndexVacuumContext,
    callback: &IndexBulkDeleteCallback<'_>,
    stats: Option<IndexBulkDeleteResult>,
    wal: &dyn AccessWalServices,
) -> Result<IndexBulkDeleteResult, AccessError> {
    let mut stats = stats.unwrap_or_default();
    let nblocks = relation_nblocks(&ctx.pool, ctx.index_relation)?;
    stats.num_pages = nblocks as u64;
    // :HACK: VACUUM prunes dead tuples in place, but empty-page unlink/reuse is
    // still deferred until pgrust has generic safe page deletion machinery.
    stats.num_deleted_pages = 0;
    stats.num_index_tuples = 0;
    for block in 0..nblocks {
        let (remaining, removed) = vacuum_leaf_page(ctx, block, callback, wal)?;
        stats.num_index_tuples += remaining;
        stats.num_removed_tuples += removed;
    }
    Ok(stats)
}

pub fn gistvacuumcleanup(
    ctx: &IndexVacuumContext,
    stats: Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, AccessError> {
    match stats {
        Some(stats) => Ok(stats),
        None => {
            let mut stats = IndexBulkDeleteResult::default();
            stats.num_pages = relation_nblocks(&ctx.pool, ctx.index_relation)? as u64;
            stats.num_index_tuples =
                gist_tuple_count(&ctx.pool, ctx.client_id, ctx.index_relation)?;
            Ok(stats)
        }
    }
}
