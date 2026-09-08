//! hash.c build half (hashbuild/hashbuildempty) + hashsort.c (HSpool).
//! Loud: parallel build.
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
    // hash.c:183-184.
    backend_progress::pgstat_progress_update_param(
        backend_progress::progress::PROGRESS_CREATEIDX_TUPLES_TOTAL,
        indtuples as i64,
    );

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
    let mut tups_done: i64 = 0;

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

        // allow insertion phase to be interrupted, and track progress
        // (hashsort.c:152-155)
        hash::check_for_interrupts()?;

        tups_done += 1;
        backend_progress::pgstat_progress_update_param(
            backend_progress::progress::PROGRESS_CREATEIDX_TUPLES_DONE,
            tups_done,
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    //! Unit witnesses for the hash build half (audit-18.6 remediation
    //! batches b004 and w2-029): a fake buffer pool (the hash crate tests'
    //! shape, keyed by thread so parallel tests never share pages) under a
    //! real two-bucket index built by `_hash_init`, and a bound
    //! PgBackendStatus entry so the CREATE INDEX progress params can be read
    //! back. Each test names the C site it witnesses.
    use std::cell::Cell;
    use std::collections::HashMap;
    use std::rc::Rc;
    use std::sync::{Mutex, Once};
    use std::thread::ThreadId;

    use ::datum::Datum;
    use ::mcx::{Mcx, MemoryContext, PgVec};
    use ::types_core::{
        BlockNumber, Buffer, ForkNumber, Oid, BLCKSZ, HASH_AM_OID, INDEX_MAX_KEYS,
        INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT, RELPERSISTENCE_UNLOGGED,
    };
    use ::types_error::{PgResult, ERRCODE_INTERNAL_ERROR, ERROR};
    use ::types_fmgr::{FmgrInfo, FunctionCallInfoBaseData};
    use ::types_rel::{
        FormData_pg_class, FormData_pg_index, LockInfoData, LockRelId, Relation, RelationData,
        LOCKMODE, RELKIND_INDEX, RELKIND_RELATION, REPLICA_IDENTITY_DEFAULT,
    };
    use ::types_tuple::itemptr::ItemPointerData;
    use ::types_tuple::tupdesc::CompactAttribute;
    use ::types_tuple::TupleDescData;
    use execindexing::IndexInfo;

    const INDEX_OID: Oid = 5000;
    const HEAP_OID: Oid = 4999;

    // Fake buffer manager: pages are leaked 8KB boxes; Buffer = block + 1.
    // One pool per thread (the seams are process-global and set once).
    #[repr(C, align(8))]
    struct FakePage([u8; BLCKSZ]);

    static POOLS: Mutex<Option<HashMap<ThreadId, Vec<usize>>>> = Mutex::new(None);

    fn with_pool<R>(f: impl FnOnce(&mut Vec<usize>) -> R) -> R {
        let mut guard = POOLS.lock().unwrap_or_else(|e| e.into_inner());
        let pools = guard.get_or_insert_with(HashMap::new);
        f(pools.entry(std::thread::current().id()).or_default())
    }

    fn push_page() -> Buffer {
        with_pool(|pages| {
            pages.push(Box::leak(Box::new(FakePage([0u8; BLCKSZ]))) as *mut FakePage as usize);
            pages.len() as Buffer
        })
    }

    fn page_bytes(buf: Buffer) -> core::ptr::NonNull<u8> {
        with_pool(|pages| {
            let idx = (buf - 1) as usize;
            assert!(idx < pages.len(), "fake pool has no buffer {buf}");
            core::ptr::NonNull::new(pages[idx] as *mut u8).expect("leaked page")
        })
    }

    fn reset_pool() {
        with_pool(|pages| pages.clear());
        init_small::globals::SetCritSectionCount(0);
    }

    fn install() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            bufmgr_seams::read_buffer::set(|rel, blkno| {
                assert_eq!(rel.rd_id, INDEX_OID, "only the index is read through the fake pool");
                let _ = page_bytes(blkno as Buffer + 1);
                Ok(blkno as Buffer + 1)
            });
            bufmgr_seams::read_buffer_extended::set(|rel, _fork, blkno, _mode, _strategy| {
                assert_eq!(rel.rd_id, INDEX_OID);
                let _ = page_bytes(blkno as Buffer + 1);
                Ok(blkno as Buffer + 1)
            });
            bufmgr_seams::extend_buffered_rel_by::set(|rel, _fork, _strategy, flags, n| {
                assert_eq!(rel.rd_id, INDEX_OID);
                assert_eq!(n, 1);
                assert!(flags & bufmgr_seams::EB_LOCK_FIRST != 0);
                Ok((push_page(), 1))
            });
            // RelationGetNumberOfBlocks = the thread's pool size (hashbuild's
            // hash.c:137 check and _hash_init's hashpage.c:344 check).
            bufmgr_seams::relation_get_number_of_blocks_in_fork::set(|_rel, _fork| {
                Ok(with_pool(|pages| pages.len() as BlockNumber))
            });
            bufmgr_seams::release_buffer::set(|_buf| Ok(()));
            bufmgr_seams::lock_buffer::set(|_buf, _mode| Ok(()));
            bufmgr_seams::lock_buffer_for_cleanup::set(|_buf| Ok(()));
            bufmgr_seams::conditional_lock_buffer_for_cleanup::set(|_buf| Ok(true));
            bufmgr_seams::mark_buffer_dirty::set(|_buf| Ok(()));
            bufmgr_seams::buffer_get_block_number::set(|buf| (buf - 1) as BlockNumber);
            bufmgr_seams::buffer_get_page::set(page_bytes);
            predicate_seams::check_for_serializable_conflict_in::set(|_rel, _tid, _blk| Ok(()));
        });
    }

    // A bound PgBackendStatus entry with track_activities on, so the
    // progress params written by the build can be read back (the lmgr
    // tests' shape: one fixed slot, serialized by the returned guard).
    fn progress_beentry(
    ) -> (&'static backend_status::PgBackendStatus, std::sync::MutexGuard<'static, ()>) {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            init_small::globals::SetMaxBackends(8);
            ipc_seams::on_shmem_exit::set(|_, _| {});
            backend_status::init_seams();
            backend_progress::init_seams();
            backend_status::BackendStatusShmemInit().unwrap();
        });
        static SLOT: Mutex<()> = Mutex::new(());
        let guard = SLOT.lock().unwrap_or_else(|e| e.into_inner());
        init_small::globals::SetMyProcNumber(3);
        backend_status::pgstat_beinit().unwrap();
        backend_status::set_pgstat_track_activities_backing(true);
        (backend_status::MyBEEntry().expect("pgstat_beinit bound a beentry"), guard)
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

    fn stub_hashint4(
        _flinfo: Option<&mut FmgrInfo>,
        fcinfo: &mut FunctionCallInfoBaseData,
    ) -> PgResult<Datum> {
        Ok(Datum::from_i32(fcinfo.arg(0).as_i32()))
    }

    fn pg_class(relname: &str, relam: Oid, relkind: u8, oid: Oid, persistence: u8) -> FormData_pg_class {
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
            relpersistence: persistence,
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

    fn rel(mcx: Mcx<'_>, index: bool, persistence: u8) -> Relation<'_> {
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
                pg_class("t_hidx", HASH_AM_OID, RELKIND_INDEX, oid, persistence)
            } else {
                // 2 = HEAP_TABLE_AM_OID.
                pg_class("t", 2, RELKIND_RELATION, oid, persistence)
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
        let rel = Relation::open(data, Some(noop_close));
        if index {
            // index_getprocid(HASHSTANDARD_PROC) at _hash_init reads the
            // primed support info (no syscache behind the fake pool).
            rel.rd_supportinfo
                .borrow_mut()
                .push(Some(FmgrInfo::new(stub_hashint4, 425, 1, true, false)));
        }
        rel
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
        reset_pool();
        // The index's main fork already holds a block.
        push_page();
        let cx = MemoryContext::new("t");
        let mcx = cx.mcx();
        let heap = rel(mcx, false, RELPERSISTENCE_PERMANENT);
        let idx = rel(mcx, true, RELPERSISTENCE_PERMANENT);
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

    // hashsort.c:154-155 pgstat_progress_update_param(PROGRESS_CREATEIDX_TUPLES_DONE,
    // ++tups_done) after every spooled insert: pg_stat_progress_create_index
    // .tuples_done climbs to the spool size during the insertion phase.
    #[test]
    fn indexbuild_reports_every_spooled_tuple_as_progress() {
        use backend_progress::progress::PROGRESS_CREATEIDX_TUPLES_DONE;

        install();
        reset_pool();
        let (be, _slot_guard) = progress_beentry();
        backend_progress::pgstat_progress_update_param(PROGRESS_CREATEIDX_TUPLES_DONE, 0);

        let cx = MemoryContext::new("t");
        let mcx = cx.mcx();
        // Unlogged: the fake pool has no WAL (hashbuild's spool arm and
        // _hash_doinsert's sorted arm are the same code either way).
        let heap = rel(mcx, false, RELPERSISTENCE_UNLOGGED);
        let idx = rel(mcx, true, RELPERSISTENCE_UNLOGGED);
        let num_buckets = hash::_hash_init(&idx, 0.0, ForkNumber::MAIN_FORKNUM)
            .unwrap_or_else(|e| panic!("_hash_init on the fake pool: {e:?}"));
        assert_eq!(num_buckets, 2);

        const N: u32 = 20;
        let mut spool = crate::_h_spoolinit(&heap, &idx, num_buckets);
        for i in 1..=N {
            // Hash values spread over both buckets; the sort orders them.
            let hashkey = i.wrapping_mul(0x9e37_79b9);
            spool
                .sortstate
                .putindextuplevalues(
                    ItemPointerData::new(10, i as u16),
                    &[Datum::from_u32(hashkey)],
                    &[false],
                )
                .unwrap_or_else(|e| panic!("_h_spool: {e:?}"));
        }
        crate::_h_indexbuild(&mut spool, &heap, &idx)
            .unwrap_or_else(|e| panic!("_h_indexbuild on the fake pool: {e:?}"));

        // SAFETY: this backend's own entry; single-writer cell read.
        let done = unsafe { be.st_progress_param[PROGRESS_CREATEIDX_TUPLES_DONE].get() };
        assert_eq!(
            done, N as i64,
            "hashsort.c:154: PROGRESS_CREATEIDX_TUPLES_DONE must reach the spool size \
             (pg_stat_progress_create_index.tuples_done stays 0 without it)"
        );
        backend_progress::pgstat_progress_update_param(PROGRESS_CREATEIDX_TUPLES_DONE, 0);
    }
}
