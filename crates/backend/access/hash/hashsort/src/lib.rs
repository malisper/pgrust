//! hash.c build half (hashbuild/hashbuildempty) + hashsort.c (HSpool).
//! Loud: parallel build, progress reporting.
#![allow(non_snake_case)]

use ::mcx::Mcx;
use ::types_core::ForkNumber;
use ::types_error::PgResult;
use ::types_rel::Relation;
use ::types_tuple::itemptr::ItemPointerData;
use execindexing::IndexInfo;

pub struct IndexBuildResult {
    pub heap_tuples: f64,
    pub index_tuples: f64,
}

struct HSpool {
    sortstate: tuplesort::Tuplesort,
    #[cfg(debug_assertions)]
    high_mask: u32,
    #[cfg(debug_assertions)]
    low_mask: u32,
    #[cfg(debug_assertions)]
    max_buckets: u32,
}

/// hashbuild.
pub fn hashbuild<'mcx>(
    mcx: Mcx<'mcx>,
    heap: &Relation<'mcx>,
    index: &Relation<'mcx>,
    indexInfo: &mut IndexInfo<'mcx>,
) -> PgResult<IndexBuildResult> {
    if bufmgr::RelationGetNumberOfBlocksInFork(index, ForkNumber::MAIN_FORKNUM)? != 0 {
        panic!("index \"{}\" already contains data", index.name());
    }

    let (_, reltuples_est, _) = planner::plancat::estimate_rel_size(heap, None, 0)?;

    let num_buckets = hash::_hash_init(index, reltuples_est, ForkNumber::MAIN_FORKNUM)?;

    let mut sort_threshold =
        (init_small::globals::maintenance_work_mem() as u64 * 1024) / ::types_core::BLCKSZ as u64;
    if index.rd_rel.relpersistence != ::types_core::RELPERSISTENCE_TEMP {
        sort_threshold = sort_threshold.min(init_small::globals::NBuffers() as u64);
    } else {
        sort_threshold = sort_threshold.min(bufmgr::n_loc_buffer().max(0) as u64);
    }

    let mut spool = if num_buckets as u64 >= sort_threshold {
        Some(_h_spoolinit(heap, index, num_buckets))
    } else {
        None
    };

    let mut indtuples = 0.0f64;

    let reltuples = execindexing::table_index_build_scan(
        mcx,
        heap,
        index,
        indexInfo,
        true,
        |index_rel, tid, values, isnull, _tuple_is_alive| {
            let Some(hash_datum) = hash::_hash_convert_tuple(index_rel, values, isnull)? else {
                return Ok(());
            };
            match spool.as_mut() {
                Some(sp) => sp.sortstate.putindextuplevalues(*tid, &[hash_datum], &[false])?,
                None => {
                    let mut itup = nbtree::itup::index_form_tuple(
                        mcx,
                        &index_rel.rd_att,
                        &[hash_datum],
                        &[false],
                    )?;
                    // SAFETY: t_tid = first 6 bytes of the owned image.
                    unsafe {
                        itup.as_mut_ptr().cast::<ItemPointerData>().write_unaligned(*tid);
                    }
                    // SAFETY: itup.size() bytes of the live owned image.
                    let image =
                        unsafe { core::slice::from_raw_parts(itup.as_ptr(), itup.size()) };
                    hash::_hash_doinsert(index_rel, image, heap, false)?;
                }
            }
            indtuples += 1.0;
            Ok(())
        },
    )?;

    if let Some(mut sp) = spool.take() {
        _h_indexbuild(&mut sp, heap, index)?;
    }

    Ok(IndexBuildResult { heap_tuples: reltuples, index_tuples: indtuples })
}

/// hashbuildempty (INIT_FORKNUM arm for unlogged indexes).
pub fn hashbuildempty(index: &Relation<'_>) -> PgResult<()> {
    hash::_hash_init(index, 0.0, ForkNumber::INIT_FORKNUM)?;
    Ok(())
}

/// _h_spoolinit.
fn _h_spoolinit(heap: &Relation<'_>, index: &Relation<'_>, num_buckets: u32) -> HSpool {
    // Stays in sync with _hash_init_metabuffer.
    let high_mask = (num_buckets + 1).next_power_of_two() - 1;
    let low_mask = high_mask >> 1;
    let max_buckets = num_buckets - 1;
    let sortstate = tuplesort::Tuplesort::begin_index_hash(
        heap,
        index,
        high_mask,
        low_mask,
        max_buckets,
        init_small::globals::maintenance_work_mem(),
        tuplesort::TUPLESORT_NONE,
    );
    HSpool {
        sortstate,
        #[cfg(debug_assertions)]
        high_mask,
        #[cfg(debug_assertions)]
        low_mask,
        #[cfg(debug_assertions)]
        max_buckets,
    }
}

/// _h_indexbuild: sort, then insert in hashkey order.
fn _h_indexbuild(
    hspool: &mut HSpool,
    heap_rel: &Relation<'_>,
    index: &Relation<'_>,
) -> PgResult<()> {
    hspool.sortstate.performsort()?;

    #[cfg(debug_assertions)]
    let mut lastbucket = 0u32;

    while let Some(itup) = hspool.sortstate.getindextuple(true)? {
        // SAFETY: sorted-run image stays live until the next getindextuple.
        let image = unsafe {
            core::slice::from_raw_parts(itup, nbtree::itup::index_tuple_size(itup))
        };
        #[cfg(debug_assertions)]
        {
            // SAFETY: as above; single non-null uint32 key at the data offset.
            let hashkey = unsafe {
                let off =
                    nbtree::itup::index_info_find_data_offset(nbtree::itup::t_info(itup));
                itup.add(off).cast::<u32>().read_unaligned()
            };
            let bucket = ::types_hash::_hash_hashkey2bucket(
                hashkey,
                hspool.max_buckets,
                hspool.high_mask,
                hspool.low_mask,
            );
            debug_assert!(bucket >= lastbucket, "tuplesort hash order violated");
            lastbucket = bucket;
        }

        hash::_hash_doinsert(index, image, heap_rel, true)?;
    }
    Ok(())
}
