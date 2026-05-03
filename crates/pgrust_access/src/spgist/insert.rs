use std::sync::OnceLock;

use pgrust_core::{XLOG_GIST_INSERT, XLOG_GIST_PAGE_INIT};
use pgrust_nodes::datum::Value;
use pgrust_storage::BLCKSZ;
use pgrust_storage::page::bufpage::PageError;

use crate::access::amapi::IndexInsertContext;
use crate::access::spgist::{F_LEAF, spgist_page_append_tuple, spgist_page_init};
use crate::{AccessError, AccessScalarServices, AccessWalServices};

use super::page::{
    SpgistLoggedPage, allocate_new_block, ensure_empty_spgist, read_buffered_page,
    relation_nblocks, write_logged_pages,
};
use super::state::SpgistState;
use super::tuple::make_leaf_tuple;

fn spgist_insert_mutex() -> &'static parking_lot::Mutex<()> {
    static SPGIST_INSERT_MUTEX: OnceLock<parking_lot::Mutex<()>> = OnceLock::new();
    SPGIST_INSERT_MUTEX.get_or_init(|| parking_lot::Mutex::new(()))
}

pub fn spginsert(
    ctx: &IndexInsertContext,
    scalar: &dyn AccessScalarServices,
    wal: &dyn AccessWalServices,
) -> Result<bool, AccessError> {
    let _guard = spgist_insert_mutex().lock();
    if relation_nblocks(&ctx.pool, ctx.index_relation)? == 0 {
        ensure_empty_spgist(
            &ctx.pool,
            ctx.client_id,
            ctx.snapshot.current_xid,
            ctx.index_relation,
            wal,
        )?;
    }
    let state = SpgistState::new(&ctx.index_desc, &ctx.index_meta, scalar)?;
    let _ = state.config(0)?;
    if !ctx
        .values
        .first()
        .is_none_or(|value| matches!(value, Value::Null))
    {
        let _ = state.choose(0, &ctx.values[0], &ctx.values[0])?;
        let _ = state.picksplit(0, &ctx.values)?;
    }
    let tuple = make_leaf_tuple(&ctx.index_desc, &ctx.values, ctx.heap_tid, scalar)?;

    let last_block = relation_nblocks(&ctx.pool, ctx.index_relation)?.saturating_sub(1);
    let mut page = read_buffered_page(&ctx.pool, ctx.client_id, ctx.index_relation, last_block)?;
    let needs_new_page = match spgist_page_append_tuple(&mut page, &tuple) {
        Ok(_) => false,
        Err(crate::access::spgist::SpgistPageError::Page(PageError::NoSpace)) => true,
        Err(err) => {
            return Err(AccessError::Io(format!(
                "spgist tuple append failed: {err:?}"
            )));
        }
    };
    if !needs_new_page {
        super::page::write_buffered_page(
            &ctx.pool,
            ctx.client_id,
            ctx.snapshot.current_xid,
            ctx.index_relation,
            last_block,
            &page,
            XLOG_GIST_INSERT,
            wal,
        )?;
        return Ok(true);
    }

    let new_block = allocate_new_block(&ctx.pool, ctx.index_relation)?;
    let mut new_page = [0u8; BLCKSZ];
    spgist_page_init(&mut new_page, F_LEAF)
        .map_err(|err| AccessError::Io(format!("spgist new-page init failed: {err:?}")))?;
    spgist_page_append_tuple(&mut new_page, &tuple)
        .map_err(|err| AccessError::Io(format!("spgist new-page append failed: {err:?}")))?;
    write_logged_pages(
        &ctx.pool,
        ctx.client_id,
        ctx.snapshot.current_xid,
        ctx.index_relation,
        XLOG_GIST_PAGE_INIT,
        &[SpgistLoggedPage {
            block: new_block,
            page: &new_page,
            will_init: true,
        }],
        wal,
    )?;
    Ok(true)
}
