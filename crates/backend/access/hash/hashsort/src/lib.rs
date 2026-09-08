//! hash.c build half (hashbuild/hashbuildempty) + hashsort.c (HSpool).
//! Loud: parallel build, progress reporting.
#![allow(non_snake_case)]

use ::mcx::Mcx;
use ::types_core::ForkNumber;
use ::types_error::{PgError, PgResult};
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
    // RelationGetNumberOfBlocks through the bufmgr seam, as the hash crate's
    // _hash_init reads it (unit tests stand a fake pool behind it).
    if bufmgr_seams::relation_get_number_of_blocks_in_fork::call(index, ForkNumber::MAIN_FORKNUM)?
        != 0
    {
        // hash.c:137 elog(ERROR).
        return Err(Box::new(PgError::error(format!(
            "index \"{}\" already contains data",
            index.name()
        ))));
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
        /* progress */ true,
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

        // allow insertion phase to be interrupted (hashsort.c:152)
        hash::check_for_interrupts()?;
    }
    Ok(())
}

#[cfg(test)]
mod audit_b004_tests {
    //! Unit witness for hash.c:137 (audit-18.6 remediation batch b004).
    use std::cell::Cell;
    use std::rc::Rc;
    use std::sync::Once;

    use ::mcx::{Mcx, MemoryContext, PgVec};
    use ::types_core::{
        Oid, HASH_AM_OID, INDEX_MAX_KEYS, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT,
    };
    use ::types_error::{PgResult, ERRCODE_INTERNAL_ERROR, ERROR};
    use ::types_rel::{
        FormData_pg_class, FormData_pg_index, LockInfoData, LockRelId, Relation, RelationData,
        LOCKMODE, RELKIND_INDEX, RELKIND_RELATION, REPLICA_IDENTITY_DEFAULT,
    };
    use ::types_tuple::tupdesc::CompactAttribute;
    use ::types_tuple::TupleDescData;
    use execindexing::IndexInfo;

    const INDEX_OID: Oid = 5000;
    const HEAP_OID: Oid = 4999;

    fn install() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            // The index's main fork already holds a block.
            bufmgr_seams::relation_get_number_of_blocks_in_fork::set(|_rel, _fork| Ok(1));
        });
    }

    fn int4_tupdesc(mcx: Mcx<'_>) -> TupleDescData<'_> {
        let mut compact = PgVec::new_in(mcx);
        compact.push(CompactAttribute {
            attcacheoff: Cell::new(-1),
            attlen: 4,
            attbyval: true,
            attispackable: false,
            atthasmissing: false,
            attisdropped: false,
            attgenerated: false,
            attnullability: 0,
            attalignby: 4,
        });
        TupleDescData {
            natts: 1,
            tdtypeid: 0,
            tdtypmod: -1,
            tdrefcount: 1,
            constr: None,
            compact_attrs: compact,
            attrs: PgVec::new_in(mcx),
        }
    }

    fn noop_close(_oid: Oid, _mode: LOCKMODE) -> PgResult<()> {
        Ok(())
    }

    fn pg_class(relname: &str, relam: Oid, relkind: u8, oid: Oid) -> FormData_pg_class {
        let mut name = ::types_tuple::NameData::default();
        name.namestrcpy(relname);
        FormData_pg_class {
            relname: name,
            relnamespace: 2200,
            reltype: 0,
            relowner: 10,
            relam,
            relfilenode: oid,
            reltablespace: 0,
            relpages: 0,
            reltuples: -1.0,
            relallvisible: 0,
            reltoastrelid: 0,
            relhasindex: relkind == RELKIND_RELATION,
            relisshared: false,
            relpersistence: RELPERSISTENCE_PERMANENT,
            relkind,
            relhassubclass: false,
            relrowsecurity: false,
            relispopulated: true,
            relreplident: REPLICA_IDENTITY_DEFAULT,
            relispartition: false,
            relfrozenxid: 3,
            relminmxid: 1,
        }
    }

    fn rel(mcx: Mcx<'_>, index: bool) -> Relation<'_> {
        let one = |v: Oid| {
            let mut vec = PgVec::new_in(mcx);
            vec.push(v);
            vec
        };
        let oid = if index { INDEX_OID } else { HEAP_OID };
        let rd_index = index.then(|| {
            let mut indkey = PgVec::new_in(mcx);
            indkey.push(1);
            FormData_pg_index {
                indexrelid: INDEX_OID,
                indrelid: HEAP_OID,
                indnatts: 1,
                indnkeyatts: 1,
                indisunique: false,
                indnullsnotdistinct: false,
                indisprimary: false,
                indisexclusion: false,
                indimmediate: true,
                indisvalid: true,
                indisready: true,
                indkey,
                has_indpred: false,
                indexprs_src: None,
                indpred_src: None,
            }
        });
        let mut indoption = PgVec::new_in(mcx);
        if index {
            indoption.push(0i16);
        }
        let data = RelationData {
            rd_locator: Default::default(),
            rd_smgr: Default::default(),
            rd_id: oid,
            rd_backend: INVALID_PROC_NUMBER,
            rd_islocaltemp: false,
            rd_isvalid: Cell::new(true),
            rd_createSubid: Cell::new(0),
            rd_newRelfilelocatorSubid: Cell::new(0),
            rd_firstRelfilelocatorSubid: Cell::new(0),
            rd_droppedSubid: Cell::new(0),
            rd_lockInfo: LockInfoData { lockRelId: LockRelId { relId: oid, dbId: 5 } },
            rd_rel: if index {
                pg_class("t_hidx", HASH_AM_OID, RELKIND_INDEX, oid)
            } else {
                // 2 = HEAP_TABLE_AM_OID.
                pg_class("t", 2, RELKIND_RELATION, oid)
            },
            rd_att: Rc::new(int4_tupdesc(mcx)),
            rd_index,
            rd_opcintype: if index { one(23) } else { PgVec::new_in(mcx) },
            rd_opfamily: if index { one(1977) } else { PgVec::new_in(mcx) },
            rd_indoption: indoption,
            rd_indcollation: if index { one(0) } else { PgVec::new_in(mcx) },
            rd_options: None,
            pgstat_enabled: Cell::new(false),
            pgstat_link: Cell::new((0, core::ptr::null_mut())),
            rd_amcache: Default::default(),
            rd_amcache_hash: Default::default(),
            rd_amcache_gin: Default::default(),
            rd_amcache_spgist: Default::default(),
            rd_support: PgVec::new_in(mcx),
            rd_supportinfo: Default::default(),
            rd_opcoptions: Default::default(),
            rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false,
            rd_hasrules: false,
        };
        Relation::open(data, Some(noop_close))
    }

    fn index_info(mcx: Mcx<'_>) -> IndexInfo<'_> {
        let mut attnums = [0; INDEX_MAX_KEYS as usize];
        attnums[0] = 1;
        IndexInfo {
            ii_NumIndexAttrs: 1,
            ii_AmCache: None,
            ii_NumIndexKeyAttrs: 1,
            ii_IndexAttrNumbers: attnums,
            ii_Expressions: ::types_nodes::NodeList::nil(),
            ii_ExpressionsState: PgVec::new_in(mcx),
            ii_Predicate: ::types_nodes::NodeList::nil(),
            ii_PredicateState: None,
            ii_Unique: false,
            ii_NullsNotDistinct: false,
            ii_ReadyForInserts: true,
            ii_Summarizing: false,
            ii_Concurrent: false,
            ii_BrokenHotChain: false,
            ii_UniqueOps: [0; INDEX_MAX_KEYS as usize],
            ii_UniqueProcs: [0; INDEX_MAX_KEYS as usize],
            ii_UniqueStrats: [0; INDEX_MAX_KEYS as usize],
            ii_HasExclusion: false,
            ii_ExclusionOps: [0; INDEX_MAX_KEYS as usize],
            ii_ExclusionProcs: [0; INDEX_MAX_KEYS as usize],
            ii_ExclusionStrats: [0; INDEX_MAX_KEYS as usize],
            ii_WithoutOverlaps: false,
            ii_CheckedUnchanged: false,
            ii_IndexUnchanged: false,
        }
    }

    // hash.c:137 elog(ERROR, "index \"%s\" already contains data"): a
    // catchable XX000, not a backend panic.
    #[test]
    fn hashbuild_on_a_populated_index_is_a_catchable_error() {
        install();
        let cx = MemoryContext::new("t");
        let mcx = cx.mcx();
        let heap = rel(mcx, false);
        let idx = rel(mcx, true);
        let mut ii = index_info(mcx);
        let err = match crate::hashbuild(mcx, &heap, &idx, &mut ii) {
            Ok(_) => panic!("hash.c:137: hashbuild must refuse a populated index"),
            Err(e) => e,
        };
        assert_eq!(err.level(), ERROR, "{err:?}");
        assert_eq!(err.sqlstate(), ERRCODE_INTERNAL_ERROR, "{err:?}");
        assert_eq!(err.message(), "index \"t_hidx\" already contains data");
        assert_eq!(err.hint(), None);
    }
}
