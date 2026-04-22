use crate::backend::access::transam::xact::INVALID_TRANSACTION_ID;
use crate::backend::access::transam::xlog::XLOG_GIST_VACUUM;
use crate::backend::catalog::CatalogError;
use crate::include::access::amapi::{
    IndexBulkDeleteCallback, IndexBulkDeleteResult, IndexVacuumContext,
};
use crate::include::access::spgist::{
    spgist_page_items, spgist_page_items_with_offsets, spgist_page_replace_items, F_HAS_GARBAGE,
    F_TUPLES_DELETED,
};

use super::page::{page_opaque, read_buffered_page, relation_nblocks, write_buffered_page};

fn spgist_tuple_count(
    pool: &crate::BufferPool<crate::SmgrStorageBackend>,
    client_id: crate::ClientId,
    rel: crate::backend::storage::smgr::RelFileLocator,
) -> Result<u64, CatalogError> {
    let nblocks = relation_nblocks(pool, rel)?;
    let mut count = 0u64;
    for block in 0..nblocks {
        let page = read_buffered_page(pool, client_id, rel, block)?;
        let opaque = page_opaque(&page)?;
        if opaque.is_deleted() || !opaque.is_leaf() {
            continue;
        }
        count += spgist_page_items(&page)
            .map_err(|err| CatalogError::Io(format!("spgist tuple parse failed: {err:?}")))?
            .len() as u64;
    }
    Ok(count)
}

fn vacuum_leaf_page(
    ctx: &IndexVacuumContext,
    block: u32,
    callback: &IndexBulkDeleteCallback<'_>,
) -> Result<(u64, u64), CatalogError> {
    let page = read_buffered_page(&ctx.pool, ctx.client_id, ctx.index_relation, block)?;
    let mut opaque = page_opaque(&page)?;
    if opaque.is_deleted() || !opaque.is_leaf() {
        return Ok((0, 0));
    }
    let items = spgist_page_items_with_offsets(&page)
        .map_err(|err| CatalogError::Io(format!("spgist tuple parse failed: {err:?}")))?;
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
    let mut rebuilt = [0u8; crate::backend::storage::smgr::BLCKSZ];
    spgist_page_replace_items(&mut rebuilt, &survivors, opaque)
        .map_err(|err| CatalogError::Io(format!("spgist vacuum rebuild failed: {err:?}")))?;
    write_buffered_page(
        &ctx.pool,
        ctx.client_id,
        INVALID_TRANSACTION_ID,
        ctx.index_relation,
        block,
        &rebuilt,
        XLOG_GIST_VACUUM,
    )?;
    Ok((survivors.len() as u64, removed))
}

pub(crate) fn spgbulkdelete(
    ctx: &IndexVacuumContext,
    callback: &IndexBulkDeleteCallback<'_>,
    stats: Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    let mut stats = stats.unwrap_or_default();
    let nblocks = relation_nblocks(&ctx.pool, ctx.index_relation)?;
    stats.num_pages = nblocks as u64;
    // :HACK: VACUUM prunes dead leaf tuples in place, but page reclamation can
    // stay deferred until the SP-GiST runtime grows a real split/delete story.
    stats.num_deleted_pages = 0;
    stats.num_index_tuples = 0;
    for block in 0..nblocks {
        let (remaining, removed) = vacuum_leaf_page(ctx, block, callback)?;
        stats.num_index_tuples += remaining;
        stats.num_removed_tuples += removed;
    }
    Ok(stats)
}

pub(crate) fn spgvacuumcleanup(
    ctx: &IndexVacuumContext,
    stats: Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    match stats {
        Some(stats) => Ok(stats),
        None => {
            let mut stats = IndexBulkDeleteResult::default();
            stats.num_pages = relation_nblocks(&ctx.pool, ctx.index_relation)? as u64;
            stats.num_index_tuples =
                spgist_tuple_count(&ctx.pool, ctx.client_id, ctx.index_relation)?;
            Ok(stats)
        }
    }
}
