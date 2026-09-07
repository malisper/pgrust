//! spginsert.c spgbuild (plain insert-based build).
#![allow(non_snake_case)]

use ::mcx::{Mcx, MemoryContext};
use ::types_core::{Buffer, ForkNumber};
use ::types_error::{PgError, PgResult};
use ::types_rel::Relation;
use ::types_spgist::{SPGIST_LEAF, SPGIST_METAPAGE_BLKNO, SPGIST_NULLS, SPGIST_NULL_BLKNO, SPGIST_ROOT_BLKNO};
use execindexing::IndexInfo;
use init_small::globals::{EndCriticalSection, StartCriticalSection};

#[cfg(test)]
mod tests;

pub struct IndexBuildResult {
    pub heap_tuples: f64,
    pub index_tuples: f64,
}

fn page_mut_of(buf: &mut bulkwrite::BulkWriteBuffer) -> ::types_storage::bufpage::PageMut<'_> {
    // SAFETY: exclusively owned, aligned build page.
    unsafe {
        ::types_storage::bufpage::PageMut::from_raw(core::ptr::NonNull::new_unchecked(
            buf.page_mut().as_mut_ptr(),
        ))
    }
}

/// spgbuildempty (spginsert.c): unlogged indexes' three INIT_FORKNUM pages.
pub fn spgbuildempty(index: &Relation<'_>) -> PgResult<()> {
    let mut bulkstate = bulkwrite::smgr_bulk_start_rel(index, ForkNumber::INIT_FORKNUM)?;

    let mut buf = bulkwrite::smgr_bulk_get_buf(&bulkstate);
    spgist::SpGistInitMetapage(&mut page_mut_of(&mut buf));
    bulkwrite::smgr_bulk_write(&mut bulkstate, SPGIST_METAPAGE_BLKNO, buf, true)?;

    let mut buf = bulkwrite::smgr_bulk_get_buf(&bulkstate);
    ::types_spgist::SpGistInitPage(&mut page_mut_of(&mut buf), SPGIST_LEAF);
    bulkwrite::smgr_bulk_write(&mut bulkstate, SPGIST_ROOT_BLKNO, buf, true)?;

    let mut buf = bulkwrite::smgr_bulk_get_buf(&bulkstate);
    ::types_spgist::SpGistInitPage(&mut page_mut_of(&mut buf), SPGIST_LEAF | SPGIST_NULLS);
    bulkwrite::smgr_bulk_write(&mut bulkstate, SPGIST_NULL_BLKNO, buf, true)?;

    bulkwrite::smgr_bulk_finish(bulkstate)
}

/// spginsert.c:82-84: a build starts on an empty main fork.
pub(crate) fn spgbuild_check_empty(index: &Relation<'_>) -> PgResult<()> {
    if bufmgr_seams::relation_get_number_of_blocks_in_fork::call(index, ForkNumber::MAIN_FORKNUM)?
        != 0
    {
        // spginsert.c:83 elog(ERROR): catchable XX000.
        return Err(Box::new(PgError::error(format!(
            "index \"{}\" already contains data",
            index.name()
        ))));
    }
    Ok(())
}

/// spginsert.c:97-107: initialize and dirty the meta, root and nulls pages.
pub(crate) fn spgbuild_init_pages(
    metabuffer: Buffer,
    rootbuffer: Buffer,
    nullbuffer: Buffer,
) -> PgResult<()> {
    // spginsert.c:97 START_CRIT_SECTION
    StartCriticalSection();
    {
        let mut pm = spgist::spg_buf_page_mut(metabuffer);
        spgist::SpGistInitMetapage(&mut pm);
    }
    bufmgr_seams::mark_buffer_dirty::call(metabuffer)?;
    spgist::SpGistInitBuffer(rootbuffer, SPGIST_LEAF);
    bufmgr_seams::mark_buffer_dirty::call(rootbuffer)?;
    spgist::SpGistInitBuffer(nullbuffer, SPGIST_LEAF | SPGIST_NULLS);
    bufmgr_seams::mark_buffer_dirty::call(nullbuffer)?;
    // spginsert.c:107
    EndCriticalSection();
    Ok(())
}

/// spgbuild.
pub fn spgbuild<'mcx>(
    mcx: Mcx<'mcx>,
    heap: &Relation<'mcx>,
    index: &Relation<'mcx>,
    indexInfo: &mut IndexInfo<'mcx>,
) -> PgResult<IndexBuildResult> {
    spgbuild_check_empty(index)?;

    let metabuffer = spgist::SpGistNewBuffer(index)?;
    let rootbuffer = spgist::SpGistNewBuffer(index)?;
    let nullbuffer = spgist::SpGistNewBuffer(index)?;

    debug_assert!(bufmgr_seams::buffer_get_block_number::call(metabuffer) == SPGIST_METAPAGE_BLKNO);
    debug_assert!(bufmgr_seams::buffer_get_block_number::call(rootbuffer) == SPGIST_ROOT_BLKNO);
    debug_assert!(bufmgr_seams::buffer_get_block_number::call(nullbuffer) == SPGIST_NULL_BLKNO);

    spgbuild_init_pages(metabuffer, rootbuffer, nullbuffer)?;

    spgist::spg_unlock_release(metabuffer)?;
    spgist::spg_unlock_release(rootbuffer)?;
    spgist::spg_unlock_release(nullbuffer)?;

    let mut state = spgist::initSpGistState(mcx, index)?;
    state.isBuild = true;
    let mut indtuples: u64 = 0;
    let mut temp = MemoryContext::new_bump("SP-GiST build temporary context");

    let reltuples = execindexing::table_index_build_scan(
        mcx,
        heap,
        index,
        indexInfo,
        true,
        /* progress */ true,
        |index_rel, tid, values, isnull, _tuple_is_alive| {
            loop {
                let done = {
                    let tmcx = temp.mcx();
                    spgist::spgdoinsert(tmcx, index_rel, &mut state, tid, values, isnull)?
                };
                temp.reset();
                if done {
                    break;
                }
            }
            indtuples += 1;
            Ok(())
        },
    )?;

    spgist::SpGistUpdateMetaPage(index)?;

    if spgist::spg_relation_needs_wal(index) {
        let nblocks = bufmgr::RelationGetNumberOfBlocksInFork(index, ForkNumber::MAIN_FORKNUM)?;
        xloginsert::log_newpage_range(index, ForkNumber::MAIN_FORKNUM, 0, nblocks, true)?;
    }

    Ok(IndexBuildResult {
        heap_tuples: reltuples,
        index_tuples: indtuples as f64,
    })
}
