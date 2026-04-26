use crate::backend::access::heap::heapam::{heap_scan_begin_visible, heap_scan_next_visible};
use crate::backend::access::index::buildkeys::{
    IndexBuildKeyProjector, materialize_heap_row_values,
};
use crate::backend::access::transam::xlog::{XLOG_GIST_INSERT, XLOG_GIST_PAGE_INIT};
use crate::backend::catalog::CatalogError;
use crate::backend::storage::page::bufpage::PageError;
use crate::include::access::amapi::{IndexBuildContext, IndexBuildEmptyContext, IndexBuildResult};
use crate::include::access::itemptr::ItemPointerData;
use crate::include::access::spgist::{F_LEAF, SPGIST_ROOT_BLKNO, spgist_page_append_tuple};
use crate::include::nodes::datum::Value;

use super::page::{SpgistLoggedPage, allocate_new_block, ensure_empty_spgist, write_logged_pages};
use super::state::SpgistState;
use super::tuple::make_leaf_tuple;

pub(crate) fn spgbuild(ctx: &IndexBuildContext) -> Result<IndexBuildResult, CatalogError> {
    if ctx.index_meta.indisunique {
        return Err(CatalogError::Io(
            "SP-GiST does not support unique indexes".into(),
        ));
    }
    ensure_empty_spgist(
        &ctx.pool,
        ctx.client_id,
        ctx.snapshot.current_xid,
        ctx.index_relation,
    )?;
    let mut builder = SpgistBulkBuilder::new(ctx)?;
    let mut result = scan_visible_heap(ctx, |tid, key_values| builder.insert(tid, key_values))?;
    builder.finish()?;
    result.index_tuples = builder.index_tuples;
    Ok(result)
}

pub(crate) fn spgbuildempty(ctx: &IndexBuildEmptyContext) -> Result<(), CatalogError> {
    ensure_empty_spgist(&ctx.pool, ctx.client_id, ctx.xid, ctx.index_relation)
}

fn scan_visible_heap(
    ctx: &IndexBuildContext,
    mut visit: impl FnMut(ItemPointerData, Vec<Value>) -> Result<bool, CatalogError>,
) -> Result<IndexBuildResult, CatalogError> {
    let mut scan = heap_scan_begin_visible(
        &ctx.pool,
        ctx.client_id,
        ctx.heap_relation,
        ctx.snapshot.clone(),
    )
    .map_err(|err| CatalogError::Io(format!("heap scan begin failed: {err:?}")))?;
    let attr_descs = ctx.heap_desc.attribute_descs();
    let mut key_projector = IndexBuildKeyProjector::new(ctx)?;
    let mut result = IndexBuildResult::default();
    loop {
        crate::backend::utils::misc::interrupts::check_for_interrupts(ctx.interrupts.as_ref())
            .map_err(CatalogError::Interrupted)?;
        let next = {
            let txns = ctx.txns.read();
            heap_scan_next_visible(&ctx.pool, ctx.client_id, &txns, &mut scan)
        };
        let Some((tid, tuple)) =
            next.map_err(|err| CatalogError::Io(format!("heap scan failed: {err:?}")))?
        else {
            break;
        };
        let datums = tuple
            .deform(&attr_descs)
            .map_err(|err| CatalogError::Io(format!("heap deform failed: {err:?}")))?;
        let row_values = materialize_heap_row_values(&ctx.heap_desc, &datums)?;
        let key_values = key_projector.project(ctx, &row_values, tid)?;
        result.heap_tuples += 1;
        if let Some(key_values) = key_values {
            if visit(tid, key_values)? {
                result.index_tuples += 1;
            }
        }
    }
    Ok(result)
}

struct SpgistBulkBuilder<'a> {
    ctx: &'a IndexBuildContext,
    current_block: u32,
    current_page: [u8; crate::backend::storage::smgr::BLCKSZ],
    page_dirty: bool,
    index_tuples: u64,
}

impl<'a> SpgistBulkBuilder<'a> {
    fn new(ctx: &'a IndexBuildContext) -> Result<Self, CatalogError> {
        let state = SpgistState::new(&ctx.index_desc, &ctx.index_meta)?;
        let _ = state.config(0)?;

        let mut current_page = [0u8; crate::backend::storage::smgr::BLCKSZ];
        crate::include::access::spgist::spgist_page_init(&mut current_page, F_LEAF)
            .map_err(|err| CatalogError::Io(format!("spgist build page init failed: {err:?}")))?;
        Ok(Self {
            ctx,
            current_block: SPGIST_ROOT_BLKNO,
            current_page,
            page_dirty: false,
            index_tuples: 0,
        })
    }

    fn insert(
        &mut self,
        heap_tid: ItemPointerData,
        values: Vec<Value>,
    ) -> Result<bool, CatalogError> {
        let tuple = make_leaf_tuple(&self.ctx.index_desc, &values, heap_tid)?;
        match spgist_page_append_tuple(&mut self.current_page, &tuple) {
            Ok(_) => {
                self.page_dirty = true;
            }
            Err(crate::include::access::spgist::SpgistPageError::Page(PageError::NoSpace)) => {
                self.flush_current_page()?;
                self.current_block = allocate_new_block(&self.ctx.pool, self.ctx.index_relation)?;
                crate::include::access::spgist::spgist_page_init(&mut self.current_page, F_LEAF)
                    .map_err(|err| {
                        CatalogError::Io(format!("spgist build page init failed: {err:?}"))
                    })?;
                spgist_page_append_tuple(&mut self.current_page, &tuple).map_err(|err| {
                    CatalogError::Io(format!("spgist build tuple append failed: {err:?}"))
                })?;
                self.page_dirty = true;
            }
            Err(err) => {
                return Err(CatalogError::Io(format!(
                    "spgist build tuple append failed: {err:?}"
                )));
            }
        }
        self.index_tuples += 1;
        Ok(true)
    }

    fn finish(&mut self) -> Result<(), CatalogError> {
        self.flush_current_page()
    }

    fn flush_current_page(&mut self) -> Result<(), CatalogError> {
        if !self.page_dirty {
            return Ok(());
        }
        let wal_info = if self.current_block == SPGIST_ROOT_BLKNO {
            XLOG_GIST_INSERT
        } else {
            XLOG_GIST_PAGE_INIT
        };
        write_logged_pages(
            &self.ctx.pool,
            self.ctx.client_id,
            self.ctx.snapshot.current_xid,
            self.ctx.index_relation,
            wal_info,
            &[SpgistLoggedPage {
                block: self.current_block,
                page: &self.current_page,
                will_init: self.current_block != SPGIST_ROOT_BLKNO,
            }],
        )?;
        self.page_dirty = false;
        Ok(())
    }
}
