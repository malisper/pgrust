use pgrust_core::{XLOG_GIST_INSERT, XLOG_GIST_PAGE_INIT};
use pgrust_nodes::datum::Value;
use pgrust_storage::BLCKSZ;
use pgrust_storage::page::bufpage::PageError;

use crate::access::amapi::{IndexBuildContext, IndexBuildEmptyContext, IndexBuildResult};
use crate::access::itemptr::ItemPointerData;
use crate::access::spgist::{F_LEAF, SPGIST_ROOT_BLKNO, spgist_page_append_tuple};
use crate::{AccessError, AccessScalarServices, AccessWalServices};

use super::page::{SpgistLoggedPage, allocate_new_block, ensure_empty_spgist, write_logged_pages};
use super::state::SpgistState;
use super::tuple::make_leaf_tuple;

pub fn spgbuild_projected(
    ctx: &IndexBuildContext,
    heap_tuples: u64,
    projected: impl IntoIterator<Item = (ItemPointerData, Vec<Value>)>,
    scalar: &dyn AccessScalarServices,
    wal: &dyn AccessWalServices,
) -> Result<IndexBuildResult, AccessError> {
    if ctx.index_meta.indisunique {
        return Err(AccessError::Io(
            "SP-GiST does not support unique indexes".into(),
        ));
    }
    ensure_empty_spgist(
        &ctx.pool,
        ctx.client_id,
        ctx.snapshot.current_xid,
        ctx.index_relation,
        wal,
    )?;
    let mut builder = SpgistBulkBuilder::new(ctx, scalar, wal)?;
    for (tid, key_values) in projected {
        let _ = builder.insert(tid, key_values)?;
    }
    builder.finish()?;
    Ok(IndexBuildResult {
        heap_tuples,
        index_tuples: builder.index_tuples,
    })
}

pub fn spgbuildempty(
    ctx: &IndexBuildEmptyContext,
    wal: &dyn AccessWalServices,
) -> Result<(), AccessError> {
    ensure_empty_spgist(&ctx.pool, ctx.client_id, ctx.xid, ctx.index_relation, wal)
}

struct SpgistBulkBuilder<'a> {
    ctx: &'a IndexBuildContext,
    scalar: &'a dyn AccessScalarServices,
    wal: &'a dyn AccessWalServices,
    current_block: u32,
    current_page: [u8; BLCKSZ],
    page_dirty: bool,
    index_tuples: u64,
}

impl<'a> SpgistBulkBuilder<'a> {
    fn new(
        ctx: &'a IndexBuildContext,
        scalar: &'a dyn AccessScalarServices,
        wal: &'a dyn AccessWalServices,
    ) -> Result<Self, AccessError> {
        let state = SpgistState::new(&ctx.index_desc, &ctx.index_meta, scalar)?;
        let _ = state.config(0)?;

        let mut current_page = [0u8; BLCKSZ];
        crate::access::spgist::spgist_page_init(&mut current_page, F_LEAF)
            .map_err(|err| AccessError::Io(format!("spgist build page init failed: {err:?}")))?;
        Ok(Self {
            ctx,
            scalar,
            wal,
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
    ) -> Result<bool, AccessError> {
        let tuple = make_leaf_tuple(&self.ctx.index_desc, &values, heap_tid, self.scalar)?;
        match spgist_page_append_tuple(&mut self.current_page, &tuple) {
            Ok(_) => {
                self.page_dirty = true;
            }
            Err(crate::access::spgist::SpgistPageError::Page(PageError::NoSpace)) => {
                self.flush_current_page()?;
                self.current_block = allocate_new_block(&self.ctx.pool, self.ctx.index_relation)?;
                crate::access::spgist::spgist_page_init(&mut self.current_page, F_LEAF).map_err(
                    |err| AccessError::Io(format!("spgist build page init failed: {err:?}")),
                )?;
                spgist_page_append_tuple(&mut self.current_page, &tuple).map_err(|err| {
                    AccessError::Io(format!("spgist build tuple append failed: {err:?}"))
                })?;
                self.page_dirty = true;
            }
            Err(err) => {
                return Err(AccessError::Io(format!(
                    "spgist build tuple append failed: {err:?}"
                )));
            }
        }
        self.index_tuples += 1;
        Ok(true)
    }

    fn finish(&mut self) -> Result<(), AccessError> {
        self.flush_current_page()
    }

    fn flush_current_page(&mut self) -> Result<(), AccessError> {
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
            self.wal,
        )?;
        self.page_dirty = false;
        Ok(())
    }
}
