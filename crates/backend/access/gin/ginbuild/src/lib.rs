//! gininsert.c, ginbuild half (nbtsort-style crate split: execindexing
//! would cycle through indexam).
#![allow(non_snake_case)]

use ::bufmgr_seams as bm;
use ::gin_vocab::*;
use ::mcx::{Mcx, MemoryContext};
use ::types_core::ForkNumber;
use ::types_error::PgResult;
use ::types_rel::Relation;

use gin::build::{
    check_for_interrupts, ginExtractEntries, ginUpdateStats, initGinState, relation_needs_wal,
    BuildAccumulator, GinInitBuffer, GinInitMetabuffer, GinNewBuffer,
};
use gin::ginEntryInsert;

const GIN_UNLOCK: i32 = bm::BUFFER_LOCK_UNLOCK;

pub struct IndexBuildResult {
    pub heap_tuples: f64,
    pub index_tuples: f64,
}

/// ginbuildempty (gininsert.c): unlogged indexes' two INIT_FORKNUM pages.
pub fn ginbuildempty(index: &Relation<'_>) -> PgResult<()> {
    let flags = bm::EB_LOCK_FIRST | bm::EB_SKIP_EXTENSION_LOCK;
    let (meta_buffer, _) =
        bm::extend_buffered_rel_by::call(index, ForkNumber::INIT_FORKNUM, None, flags, 1)?;
    let (root_buffer, _) =
        bm::extend_buffered_rel_by::call(index, ForkNumber::INIT_FORKNUM, None, flags, 1)?;

    init_small::globals::StartCriticalSection();
    GinInitMetabuffer(meta_buffer);
    bm::mark_buffer_dirty::call(meta_buffer)?;
    xloginsert::log_newpage_buffer(meta_buffer, true)?;
    GinInitBuffer(root_buffer, GIN_LEAF);
    bm::mark_buffer_dirty::call(root_buffer)?;
    xloginsert::log_newpage_buffer(root_buffer, false)?;
    init_small::globals::EndCriticalSection();

    bm::lock_buffer::call(meta_buffer, GIN_UNLOCK)?;
    bm::release_buffer::call(meta_buffer)?;
    bm::lock_buffer::call(root_buffer, GIN_UNLOCK)?;
    bm::release_buffer::call(root_buffer)?;
    Ok(())
}

/// ginbuild: serial accumulate + dump (parallel workers unported).
pub fn ginbuild<'mcx>(
    mcx: Mcx<'mcx>,
    heap: &Relation<'mcx>,
    index: &Relation<'mcx>,
    indexInfo: &mut execindexing::IndexInfo<'mcx>,
) -> PgResult<IndexBuildResult> {
    if bm::relation_get_number_of_blocks_in_fork::call(index, ForkNumber::MAIN_FORKNUM)? != 0 {
        panic!("index \"{}\" already contains data", index.name());
    }

    let state = initGinState(index)?;
    let mut build_stats = GinStatsData::default();
    let mut indtuples = 0.0f64;

    let meta_buffer = GinNewBuffer(index)?;
    let root_buffer = GinNewBuffer(index)?;
    GinInitMetabuffer(meta_buffer);
    bm::mark_buffer_dirty::call(meta_buffer)?;
    GinInitBuffer(root_buffer, GIN_LEAF);
    bm::mark_buffer_dirty::call(root_buffer)?;
    bm::lock_buffer::call(meta_buffer, GIN_UNLOCK)?;
    bm::release_buffer::call(meta_buffer)?;
    bm::lock_buffer::call(root_buffer, GIN_UNLOCK)?;
    bm::release_buffer::call(root_buffer)?;

    build_stats.nEntryPages += 1;

    let mut tmp_ctx = MemoryContext::new_bump("Gin build temporary context");
    let mut func_ctx = MemoryContext::new_bump("Gin build extract context");

    // SAFETY: the accumulator never outlives tmp_ctx: it is dropped and
    // rebuilt before every reset, and dropped before tmp_ctx's final drop.
    unsafe fn erase<'x>(a: BuildAccumulator<'x>) -> BuildAccumulator<'static> {
        unsafe { core::mem::transmute(a) }
    }
    // SAFETY: per erase contract below.
    let mut accum = unsafe { erase(BuildAccumulator::new(tmp_ctx.mcx(), state)) };

    let reltuples = execindexing::table_index_build_scan(
        mcx,
        heap,
        index,
        indexInfo,
        false,
        |_rel, tid, values, isnull, _tuple_is_alive| {
            func_ctx.reset();
            {
                let fmcx = func_ctx.mcx();
                for i in 0..state.natts as usize {
                    let attnum = (i + 1) as ::types_core::OffsetNumber;
                    let (entries, categories) =
                        ginExtractEntries(fmcx, &state, attnum, values[i], isnull[i])?;
                    accum.insert_entries(tid, attnum, entries.as_slice(), categories.as_slice())?;
                    indtuples += entries.len() as f64;
                }
            }

            if accum.allocated_memory
                >= init_small::globals::maintenance_work_mem() as usize * 1024
            {
                accum.begin_scan()?;
                loop {
                    let Some((attnum, key, category, list)) = accum.next_entry() else {
                        break;
                    };
                    check_for_interrupts()?;
                    let dump_ctx = MemoryContext::new_bump("gin build dump scratch");
                    ginEntryInsert(
                        dump_ctx.mcx(),
                        index,
                        &state,
                        attnum,
                        key,
                        category,
                        list,
                        Some(&mut build_stats),
                    )?;
                }
                accum = unsafe { erase(BuildAccumulator::new(tmp_ctx.mcx(), state)) };
                // C resets tmpCtx here; the arena grows across rounds
                // instead (freed at build end).
            }
            Ok(())
        },
    )?;

    accum.begin_scan()?;
    loop {
        let Some((attnum, key, category, list)) = accum.next_entry() else {
            break;
        };
        check_for_interrupts()?;
        let dump_ctx = MemoryContext::new_bump("gin build dump scratch");
        ginEntryInsert(
            dump_ctx.mcx(),
            index,
            &state,
            attnum,
            key,
            category,
            list,
            Some(&mut build_stats),
        )?;
    }
    drop(accum);
    tmp_ctx.reset();
    let _ = &mut tmp_ctx;

    build_stats.nTotalPages =
        bm::relation_get_number_of_blocks_in_fork::call(index, ForkNumber::MAIN_FORKNUM)?;
    ginUpdateStats(index, &build_stats, true)?;

    if relation_needs_wal(index) {
        xloginsert::log_newpage_range(
            index,
            ForkNumber::MAIN_FORKNUM,
            0,
            bm::relation_get_number_of_blocks_in_fork::call(index, ForkNumber::MAIN_FORKNUM)?,
            true,
        )?;
    }

    Ok(IndexBuildResult {
        heap_tuples: reltuples,
        index_tuples: indtuples,
    })
}


