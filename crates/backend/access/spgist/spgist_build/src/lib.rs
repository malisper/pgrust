//! spginsert.c spgbuild (plain insert-based build).
#![allow(non_snake_case)]

use ::mcx::{Mcx, MemoryContext};
use ::types_core::ForkNumber;
use ::types_error::PgResult;
use ::types_rel::Relation;
use ::types_spgist::{SPGIST_LEAF, SPGIST_METAPAGE_BLKNO, SPGIST_NULLS, SPGIST_NULL_BLKNO, SPGIST_ROOT_BLKNO};
use execindexing::IndexInfo;

pub struct IndexBuildResult {
    pub heap_tuples: f64,
    pub index_tuples: f64,
}

/// spgbuild.
pub fn spgbuild<'mcx>(
    mcx: Mcx<'mcx>,
    heap: &Relation<'mcx>,
    index: &Relation<'mcx>,
    indexInfo: &mut IndexInfo,
) -> PgResult<IndexBuildResult> {
    if bufmgr::RelationGetNumberOfBlocksInFork(index, ForkNumber::MAIN_FORKNUM)? != 0 {
        panic!("index \"{}\" already contains data", index.name());
    }

    let metabuffer = spgist::SpGistNewBuffer(index)?;
    let rootbuffer = spgist::SpGistNewBuffer(index)?;
    let nullbuffer = spgist::SpGistNewBuffer(index)?;

    debug_assert!(bufmgr_seams::buffer_get_block_number::call(metabuffer) == SPGIST_METAPAGE_BLKNO);
    debug_assert!(bufmgr_seams::buffer_get_block_number::call(rootbuffer) == SPGIST_ROOT_BLKNO);
    debug_assert!(bufmgr_seams::buffer_get_block_number::call(nullbuffer) == SPGIST_NULL_BLKNO);

    {
        let mut pm = spgist::spg_buf_page_mut(metabuffer);
        spgist::SpGistInitMetapage(&mut pm);
    }
    bufmgr_seams::mark_buffer_dirty::call(metabuffer)?;
    spgist::SpGistInitBuffer(rootbuffer, SPGIST_LEAF);
    bufmgr_seams::mark_buffer_dirty::call(rootbuffer)?;
    spgist::SpGistInitBuffer(nullbuffer, SPGIST_LEAF | SPGIST_NULLS);
    bufmgr_seams::mark_buffer_dirty::call(nullbuffer)?;

    spgist::spg_unlock_release(metabuffer)?;
    spgist::spg_unlock_release(rootbuffer)?;
    spgist::spg_unlock_release(nullbuffer)?;

    let mut state = spgist::initSpGistState(index)?;
    state.isBuild = true;
    let mut indtuples: u64 = 0;
    let mut temp = MemoryContext::new_bump("SP-GiST build temporary context");

    let reltuples = execindexing::table_index_build_scan(
        mcx,
        heap,
        index,
        indexInfo,
        true,
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
