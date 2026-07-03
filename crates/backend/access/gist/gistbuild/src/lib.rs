//! gistbuild.c, insert-based (plain) build. LOUD lanes: buffered build
//! (buffering=on / auto-switch) and sorted build (opclasses with
//! GIST_SORTSUPPORT_PROC, i.e. point_ops).
#![allow(non_snake_case)]

use ::mcx::{Mcx, MemoryContext};
use ::types_core::{ForkNumber, BLCKSZ, RELPERSISTENCE_UNLOGGED};
use ::types_error::PgResult;
use ::types_gist::{GistBuildLSN, F_LEAF, GIST_DEFAULT_FILLFACTOR, GIST_ROOT_BLKNO, GIST_SORTSUPPORT_PROC};
use ::types_rel::Relation;
use ::types_tuple::itemptr::ItemPointerData;
use execindexing::IndexInfo;

use gist::state::{index_getprocid, initGISTstate};
use gist::util::{gist_init_buffer, gistFormTuple, gistNewBuffer};

pub struct IndexBuildResult {
    pub heap_tuples: f64,
    pub index_tuples: f64,
}

const BUFFERING_MODE_SWITCH_CHECK_STEP: u64 = 256;

/// gistbuild.
pub fn gistbuild<'mcx>(
    mcx: Mcx<'mcx>,
    heap: &Relation<'mcx>,
    index: &Relation<'mcx>,
    indexInfo: &mut IndexInfo,
) -> PgResult<IndexBuildResult> {
    if bufmgr::RelationGetNumberOfBlocksInFork(index, ForkNumber::MAIN_FORKNUM)? != 0 {
        panic!("index \"{}\" already contains data", index.name());
    }

    let fillfactor = index.get_fillfactor(GIST_DEFAULT_FILLFACTOR);
    let freespace = BLCKSZ * (100 - fillfactor as usize) / 100;

    let nkeys = index.indnkeyatts() as usize;
    let hasallsortsupports =
        (0..nkeys).all(|i| index_getprocid(index, i, GIST_SORTSUPPORT_PROC) != 0);
    if hasallsortsupports {
        panic!(
            "unported: gist sorted build (GIST_SORTSUPPORT_PROC opclasses, \
             e.g. point_ops; tuplesort_begin_index_gist lane)"
        );
    }

    let mut giststate = initGISTstate(index)?;
    let mut temp = MemoryContext::new("GiST temporary context");

    {
        let buffer = gistNewBuffer(index, heap)?;
        debug_assert!(buffer.block_number() == GIST_ROOT_BLKNO);
        gist_init_buffer(&buffer, F_LEAF);
        bufmgr_seams::mark_buffer_dirty::call(buffer.buffer())?;
        gist::buf_page_mut_pub(buffer.buffer()).set_lsn(GistBuildLSN);
        bufmgr_seams::lock_buffer::call(buffer.buffer(), bufmgr_seams::BUFFER_LOCK_UNLOCK)?;
        drop(buffer);
    }

    let mut indtuples: u64 = 0;

    let reltuples = execindexing::table_index_build_scan(
        mcx,
        heap,
        index,
        indexInfo,
        true,
        |index_rel, tid, values, isnull, _tuple_is_alive| {
            let tmcx = temp.mcx();
            let mut itup = gistFormTuple(tmcx, &mut giststate, index_rel, values, isnull, true)?;
            unsafe {
                itup.as_mut_ptr()
                    .cast::<ItemPointerData>()
                    .write_unaligned(*tid);
            }

            indtuples += 1;

            gist::insert::gistdoinsert(
                tmcx,
                index_rel,
                itup.as_ptr(),
                freespace,
                &mut giststate,
                heap,
                true,
            )?;
            drop(itup);
            temp.reset();

            if indtuples % BUFFERING_MODE_SWITCH_CHECK_STEP == 0 {
                let nblocks =
                    bufmgr::RelationGetNumberOfBlocksInFork(index_rel, ForkNumber::MAIN_FORKNUM)?;
                let effective_cache_size = (guc_tables::vars::effective_cache_size.get().get)() as u64;
                if effective_cache_size < nblocks as u64 {
                    panic!(
                        "unported: gist buffered build (index grew past \
                         effective_cache_size; gistbuildbuffers lane)"
                    );
                }
            }
            Ok(())
        },
    )?;

    if gist::relation_needs_wal_pub(index) {
        let nblocks = bufmgr::RelationGetNumberOfBlocksInFork(index, ForkNumber::MAIN_FORKNUM)?;
        xloginsert::log_newpage_range(index, ForkNumber::MAIN_FORKNUM, 0, nblocks, true)?;
    }

    Ok(IndexBuildResult {
        heap_tuples: reltuples,
        index_tuples: indtuples as f64,
    })
}

/// gistbuildempty (INIT_FORKNUM arm for unlogged indexes).
pub fn gistbuildempty(index: &Relation<'_>) -> PgResult<()> {
    debug_assert!(index.rd_rel.relpersistence == RELPERSISTENCE_UNLOGGED);
    panic!("unported: gistbuildempty (unlogged gist INIT_FORKNUM lane)");
}
