// VACUUM error-path pin release: real bufmgr over an in-memory smgr, an
// injected WAL failure mid lazy_scan_heap (heap buffer pinned), then
// AbortCurrentTransaction must leave every buffer unpinned — C's mechanism is
// ResourceOwnerReleaseAll(BEFORE_LOCKS), not a manual unpin on the error path.
// Harness cloned from vacuum_e2e.rs (separate test binary, own data dir).
use std::cell::Cell;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering::Relaxed};
use std::sync::Mutex;

use mcx::{Mcx, MemoryContext, PgVec};
use transam_xlog::control_file::{
    FirstNormalUnloggedLSN, FLOATFORMAT_VALUE, PG_CONTROL_FILE_SIZE, PG_CONTROL_VERSION,
    TOAST_MAX_CHUNK_SIZE,
};
use transam_xlog::{XLogRecPtrToBytePos, DB_IN_PRODUCTION, RECOVERY_STATE_DONE};
use types_core::{
    BackendType, BlockNumber, ForkNumber, Oid, XLogRecPtr, BLCKSZ, INVALID_PROC_NUMBER,
    RELPERSISTENCE_PERMANENT,
};
use types_nodes::nodes_enums::CmdType;
use types_rel::{
    FormData_pg_class, LockInfoData, LockRelId, Relation, RelationData, LOCKMODE,
    RELKIND_RELATION,
};
use types_storage::{RelFileLocator, RelFileLocatorBackend};
use types_tuple::{CompactAttribute, FormData_pg_attribute, NameData, TupleDescData};

const SEG: i32 = 16 * 1024 * 1024;
const SYS_ID: u64 = 0x5544_3322_1100_ACDF;
const REL_OID: Oid = 61012;
const RLOC: RelFileLocator = RelFileLocator::new(1663, 5, REL_OID);
const INT4OID: Oid = 23;
const TEST_NBUFFERS: i32 = 512;

const RM_HEAP2_ID: u8 = rmgr::RmgrIds::RM_HEAP2_ID as u8;
const XLOG_HEAP2_PRUNE_VACUUM_SCAN: u8 = 0x20;

// In-memory per-fork block images behind the smgr seams; the real bufmgr
// provides pages, pins, and locks on top.
struct Disk {
    forks: Vec<Option<Vec<Box<[u8; BLCKSZ]>>>>,
}

static DISK: Mutex<Disk> = Mutex::new(Disk { forks: Vec::new() });

fn with_disk<R>(f: impl FnOnce(&mut Disk) -> R) -> R {
    f(&mut DISK.lock().unwrap_or_else(|e| e.into_inner()))
}

impl Disk {
    fn fork(&mut self, fork: ForkNumber) -> &mut Option<Vec<Box<[u8; BLCKSZ]>>> {
        let i = fork as usize;
        if self.forks.len() <= i {
            self.forks.resize_with(i + 1, || None);
        }
        &mut self.forks[i]
    }
}

fn install_smgr_disk() {
    smgr_seams::smgr_create::set(|loc, fork, _is_redo| {
        assert_eq!(loc.locator, RLOC);
        with_disk(|d| {
            let f = d.fork(fork);
            if f.is_none() {
                *f = Some(Vec::new());
            }
        });
        Ok(())
    });
    smgr_seams::smgr_exists::set(|_loc, fork| {
        Ok(with_disk(|d| d.fork(fork).is_some()))
    });
    smgr_seams::smgr_nblocks::set(|_loc, fork| {
        Ok(with_disk(|d| {
            d.fork(fork).as_ref().expect("smgr_nblocks on missing fork").len() as BlockNumber
        }))
    });
    smgr_seams::rel_smgr_nblocks::set(|_rel, fork| {
        Ok(with_disk(|d| {
            d.fork(fork).as_ref().expect("rel_smgr_nblocks on missing fork").len() as BlockNumber
        }))
    });
    smgr_seams::smgr_zeroextend::set(|_loc, fork, blocknum, nblocks, _skip_fsync| {
        with_disk(|d| {
            let f = d.fork(fork).as_mut().expect("zeroextend on missing fork");
            assert_eq!(f.len() as BlockNumber, blocknum);
            for _ in 0..nblocks {
                f.push(Box::new([0u8; BLCKSZ]));
            }
        });
        Ok(())
    });
    smgr_seams::smgr_read::set(|_loc, fork, blocknum, buffer| {
        with_disk(|d| {
            let f = d.fork(fork).as_ref().expect("smgr_read on missing fork");
            buffer.copy_from_slice(&f[blocknum as usize][..]);
        });
        Ok(())
    });
    smgr_seams::smgr_write::set(|_loc, fork, blocknum, buffer, _skip_fsync| {
        // SAFETY: single-threaded unit test — no other backend exists to write
        // the image (the excluding mechanism WriteChunk asks for).
        let buffer = unsafe { buffer.as_slice_unchecked() };
        with_disk(|d| {
            let f = d.fork(fork).as_mut().expect("smgr_write on missing fork");
            f[blocknum as usize].copy_from_slice(buffer);
        });
        Ok(())
    });
    smgr_seams::smgr_writeback::set(|_, _, _, _| Ok(()));
    smgr_seams::smgr_cached_nblocks::set(|_loc, _fork| types_core::InvalidBlockNumber);
    smgr_seams::smgr_set_cached_nblocks::set(|_loc, _fork, _n| Ok(()));
}

// Fail the Nth HEAP2 PRUNE_VACUUM_SCAN record while armed: an ereport-shaped
// error surfacing from lazy_scan_prune with the heap buffer pinned.
static INJECT_ARMED: AtomicBool = AtomicBool::new(false);
static PRUNE_RECORDS: AtomicU32 = AtomicU32::new(0);
const FAIL_ON_PRUNE: u32 = 2;

fn maybe_inject(rmid: u8, info: u8) -> types_error::PgResult<()> {
    if INJECT_ARMED.load(Relaxed)
        && rmid == RM_HEAP2_ID
        && info & 0xF0 == XLOG_HEAP2_PRUNE_VACUUM_SCAN
        && PRUNE_RECORDS.fetch_add(1, Relaxed) + 1 == FAIL_ON_PRUNE
    {
        return Err(types_error::PgError::error("injected mid-scan vacuum failure").into());
    }
    Ok(())
}

fn install_wal_seams() {
    xloginsert_seams::xlog_check_buffer_needs_backup::set(|_| false);
    xloginsert_seams::xlog_reset_insertion::set(xloginsert::XLogResetInsertion);
    xloginsert_seams::xlog_insert::set(|rmid, info, fragments| {
        maybe_inject(rmid, info)?;
        xloginsert::insert_record(rmid, info, 0, fragments, &[])
    });
    xloginsert_seams::xlog_insert_with_flags::set(|rmid, info, _flags, fragments| {
        maybe_inject(rmid, info)?;
        xloginsert::insert_record(rmid, info, 0, fragments, &[])
    });
    xloginsert_seams::xlog_insert_record::set(|rmid, info, flags, main_data, bufs| {
        maybe_inject(rmid, info)?;
        let mut blocks: Vec<xloginsert::RegBlock<'_>> = Vec::with_capacity(bufs.len());
        for b in bufs {
            let tag = bufmgr::BufferGetTag(b.buffer);
            blocks.push(xloginsert::RegBlock {
                block_id: b.block_id,
                rlocator: RelFileLocator::new(tag.spcOid, tag.dbOid, tag.relNumber),
                forknum: tag.forkNum,
                block: tag.blockNum,
                // SAFETY: registered buffers are pinned by the caller for the
                // duration of the insert; BLCKSZ page image.
                page: unsafe {
                    core::slice::from_raw_parts(bufmgr::BufferGetPagePtr(b.buffer).as_ptr(), BLCKSZ)
                },
                flags: b.flags,
                bufdata: b.bufdata,
            });
        }
        xloginsert::insert_record(rmid, info, flags, main_data, &blocks)
    });
}

fn install_proc_boot_seams() {
    use init_small::globals as g;
    g::SetMaxConnections(16);
    g::set_max_worker_processes(2);
    g::SetMaxBackends(16 + 3 + 2 + 2 + 2);
    g::SetMyProcPid(784);
    g::SetMyDatabaseId(5);
    g::set_transaction_buffers(64);
    g::set_subtransaction_buffers(64);
    g::set_multixact_offset_buffers(16);
    g::set_multixact_member_buffers(16);
    g::SetNBuffers(TEST_NBUFFERS);

    pg_sema_seams::pg_semaphore_create::set(|_| {});
    pg_sema_seams::pg_semaphore_reset::set(|_| {});
    pg_sema_seams::pg_semaphore_lock::set(|_| {});
    pg_sema_seams::pg_semaphore_unlock::set(|_| {});
    s_lock_seams::perform_spin_delay::set(|_| std::thread::yield_now());
    s_lock_seams::finish_spin_delay::set(|_| {});
    s_lock_seams::set_spins_per_delay::set(|_| {});
    s_lock_seams::update_spins_per_delay::set(|v| v);
    latch_seams::own_latch::set(|_| {});
    latch_seams::disown_latch::set(|_| {});
    latch_seams::set_latch::set(|_| {});
    latch_seams::set_latch_my_latch::set(|| {});
    latch_seams::wait_latch_my_latch::set(|_, _, _| 0);
    latch_seams::reset_latch_my_latch::set(|| {});
    miscinit_seams::switch_to_shared_latch::set(|| {});
    miscinit_seams::switch_back_to_local_latch::set(|| {});
    miscinit_seams::get_user_id::set(|| 10);
    // vacuum_is_permitted_for_relation calls miscinit::GetUserId() directly
    // (not the seam); arm the thread-local too.
    miscinit::SetUserIdAndSecContext(10, 0);
    miscinit_seams::is_bootstrap_processing_mode::set(|| false);
    waitevent_seams::pgstat_set_wait_event_storage::set(|_| {});
    waitevent_seams::pgstat_report_wait_start::set(|_| {});
    waitevent_seams::pgstat_report_wait_end::set(|| {});
    waitevent_seams::pgstat_reset_wait_event_storage::set(|| {});
    ipc_seams::on_shmem_exit::set(|_, _| {});
    deadlock_seams::init_dead_lock_checking::set(|| Ok(()));
    execreplication_seams::check_cmd_replica_identity::set(|_mcx, _rel, _cmd| Ok(()));
    pmsignal_seams::register_postmaster_child_active::set(|| {});
    syncrep_seams::sync_rep_cleanup_at_proc_exit::set(|| {});
    condition_variable_seams::condition_variable_cancel_sleep::set(|| false);
    lock_seams::abort_strong_lock_acquire::set(|| {});
    lock_seams::get_awaited_lock_hashcode::set(|| None);
    lock_seams::lock_release_all::set(|_, _| lock::VirtualXactLockTableCleanup());
    lock_seams::lock_acquire_extended::set(|_, _, _, _, _, _| {
        Ok(types_storage::lock::LOCKACQUIRE_OK)
    });
    lock_seams::lock_release::set(|_, _, _| Ok(true));
    timeout_seams::disable_timeouts::set(|_| {});
    aio_seams::pgaio_closing_fd::set(|_| {});
    sync_seams::register_sync_request::set(|_, _, _| Ok(true));
    sinval_seams::receive_shared_invalid_messages::set(|_, _| Ok(()));
    sinval_seams::send_shared_invalid_messages::set(|_| Ok(()));
    aio_seams::pgaio_io_start_readv::set(|_, _, _| Ok(()));
    aio_seams::at_eoxact_aio::set(|_| {});
    aio_seams::pgaio_error_cleanup::set(|| {});
    logical_worker_seams::at_eoxact_logical_rep_workers::set(|_| {});
}

fn install_xact_periphery_seams() {
    timestamp_seams::get_current_timestamp::set(|| 777_000_000);
    trigger_seams::after_trigger_begin_xact::set(|| Ok(()));
    trigger_seams::after_trigger_end_xact::set(|_| Ok(()));
    trigger_seams::after_trigger_fire_deferred::set(|| Ok(()));
    async_seams::pre_commit_notify::set(|| Ok(()));
    async_seams::at_commit_notify::set(|| Ok(()));
    async_seams::at_abort_notify::set(|| {});
    tablecmds_seams::pre_commit_on_commit_actions::set(|| Ok(()));
    tablecmds_seams::at_eoxact_on_commit_actions::set(|_| {});
    spi_seams::at_eoxact_spi::set(|_| Ok(()));
    spi_seams::spi_inside_nonatomic_context::set(|| false);
    be_fsstubs_seams::at_eoxact_large_object::set(|_| Ok(()));
    namespace_seams::at_eoxact_namespace::set(|_, _| {});
    catalog_index_seams::reset_reindex_state::set(|_| {});
    catalog_storage_seams::smgr_get_pending_deletes::set(|mcx, _for_commit| {
        Ok(PgVec::new_in(mcx))
    });
    catalog_storage_seams::smgr_do_pending_deletes::set(|_| Ok(()));
    catalog_storage_seams::smgr_do_pending_syncs::set(|_, _| Ok(()));
    combocid_seams::at_eoxact_combocid::set(|| {});
    combocid_seams::heap_tuple_header_adjust_cmax::set(|_hdr, cid| Ok((cid, false)));
    combocid_seams::heap_tuple_header_get_cmax::set(|hdr| hdr.raw_command_id());
    combocid_seams::heap_tuple_header_get_cmin::set(|hdr| hdr.raw_command_id());
    pg_enum_seams::at_eoxact_enum::set(|| {});
    relcache_seams::at_eoxact_relation_cache::set(|_| Ok(()));
    typcache_seams::at_eoxact_type_cache::set(|| {});
    logical_seams::reset_logical_streaming_state::set(|| {});
    snapbuild_seams::snap_build_reset_exported_snapshot_state::set(|| {});
    parallel_seams::is_parallel_worker::set(|| false);
    parallel_seams::at_eoxact_parallel::set(|_| Ok(()));
    origin_seams::replorigin_session_origin::set(|| types_core::InvalidRepOriginId);
    origin_seams::replorigin_session_origin_lsn::set(|| 0);
    origin_seams::replorigin_session_origin_timestamp::set(|| 0);
    origin_seams::set_replorigin_session_origin_timestamp::set(|_| {});
    commit_ts_seams::transaction_tree_set_commit_ts_data::set(|_, _, _, _| Ok(()));
    commit_ts_seams::extend_commit_ts::set(|_| Ok(()));
    syncrep_seams::sync_rep_wait_for_lsn::set(|_, _| Ok(()));
    backend_status_seams::pgstat_report_xact_timestamp::set(|_| {});
    backend_status_seams::pgstat_report_query_id::set(|_, _| {});
    backend_status_seams::pgstat_report_plan_id::set(|_, _| {});
    backend_status_seams::pgstat_clear_backend_status_snapshot::set(|| {});
    backend_progress_seams::pgstat_progress_end_command::set(|| {});
    predicate_seams::pre_commit_check_for_serialization_failure::set(|| Ok(()));
    predicate_seams::release_predicate_locks::set(|_, _| Ok(()));
    predicate_seams::register_predicate_locking_xid::set(|_| Ok(()));
    predicate_seams::check_for_serializable_conflict_in::set(|_rel, _tid, _blk| Ok(()));
    predicate_seams::check_table_for_serializable_conflict_in::set(|_rel| Ok(()));
    predicate_seams::transfer_predicate_locks_to_heap_relation::set(|_rel| Ok(()));
    predicate_seams::check_for_serializable_conflict_out_needed::set(|_r, _s| Ok(false));
    predicate_seams::predicate_lock_relation::set(|_r, _s| Ok(()));
    predicate_seams::predicate_lock_tid::set(|_r, _t, _s, _x| Ok(()));
    pruneheap_seams::heap_page_prune_opt::set(|_r, _b| Ok(()));
    catalog_seams::is_catalog_relation::set(|_rel| false);
    dbcommands_seams::get_database_name::set(|_| Ok(Some("testdb".to_string())));
    syscache_seams::search_syscache_exists_databaseoid::set(|_| Ok(true));
    aclchk_seams::pg_class_aclmask::set(|_relid, _roleid, mask, _how_all| Ok(mask));
    aclchk_seams::object_aclcheck::set(|_classid, _objid, _roleid, _mode| Ok(0));
    lmgr_seams::check_relation_locked_by_me::set(|_, _, _| true);
    syscache_seams::lookup_pg_class_ls_shape::set(|_relid| {
        Ok(Some(syscache_seams::PgClassLsShape {
            relnamespace: 2200,
            reltype: 0,
            relam: 2,
            reltablespace: 0,
            relnatts: 2,
            relkind: b'r' as i8,
            relpersistence: b'p' as i8,
            relispartition: false,
            relhassubclass: false,
        }))
    });
    syscache_seams::lookup_pg_class_by_relid::set(|relid| {
        Ok(Some(types_storage::inval::PgClassShape {
            oid: relid,
            relnamespace: 2200,
            relfilenode: relid,
            reltablespace: 0,
            relisshared: false,
            relpersistence: b'p' as i8,
            relkind: b'r' as i8,
        }))
    });
    namespace_seams::range_var_get_relid::set(|_mcx, rv, _lockmode, missing_ok| {
        if rv.relname == "t" {
            Ok(REL_OID)
        } else if missing_ok {
            Ok(0)
        } else {
            Err(types_error::PgError::error("no such relation").into())
        }
    });
    namespace_seams::range_var_get_relid_extended::set(|_mcx, rv, _lockmode, flags| {
        if rv.relname == "t" {
            Ok(REL_OID)
        } else if flags & namespace_seams::RVR_MISSING_OK != 0 {
            Ok(0)
        } else {
            Err(types_error::PgError::error("no such relation").into())
        }
    });
    lmgr_seams::lock_relation_oid::set(|_, _| Ok(()));
    lmgr_seams::unlock_relation_oid::set(|_, _| Ok(()));
    syscache_seams::search_syscache_exists_reloid::set(|relid| Ok(relid == REL_OID));
    relcache_seams::relation_get_index_list::set(|mcx, _relid| Ok(PgVec::new_in(mcx)));
    relcache_seams::relation_get_stat_ext_list::set(|mcx, _relid| Ok(PgVec::new_in(mcx)));
    relcache_seams::relation_id_get_relation::set(|relid| {
        assert_eq!(relid, REL_OID);
        let ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("rel")));
        Ok(Some(Rc::new(test_relation(ctx.mcx()))))
    });
}

fn int4_x2_tupdesc<'mcx>(mcx: Mcx<'mcx>) -> Rc<TupleDescData<'mcx>> {
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    for (i, name) in ["a", "b"].iter().enumerate() {
        let mut att = FormData_pg_attribute {
            attrelid: REL_OID,
            attnum: i as i16 + 1,
            atttypid: INT4OID,
            atttypmod: -1,
            attlen: 4,
            attbyval: true,
            attalign: types_tuple::TYPALIGN_INT,
            attstorage: types_tuple::TYPSTORAGE_PLAIN,
            attislocal: true,
            ..Default::default()
        };
        att.attname.namestrcpy(name);
        compact.push(CompactAttribute::populate_from(&att));
        attrs.push(att);
    }
    Rc::new(TupleDescData {
        natts: 2,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

fn test_relation<'mcx>(mcx: Mcx<'mcx>) -> RelationData<'mcx> {
    let mut relname = NameData::default();
    relname.namestrcpy("t");
    let rd_rel = FormData_pg_class {
        relname,
        relnamespace: 2200,
        reltype: 0,
        relowner: 10,
        relam: tableam_vocab::HEAP_TABLE_AM_OID,
        relfilenode: REL_OID,
        reltablespace: 0,
        relpages: 0,
        reltuples: -1.0,
        relallvisible: 0,
        reltoastrelid: 0,
        relhasindex: false,
        relisshared: false,
        relpersistence: RELPERSISTENCE_PERMANENT,
        relkind: RELKIND_RELATION,
        relhassubclass: false,
        relrowsecurity: false,
        relispopulated: true,
        relreplident: b'd',
        relispartition: false,
        relfrozenxid: 3,
        relminmxid: 1,
    };
    RelationData {
        // Real bufmgr resolves the smgr locator from rd_locator.
        rd_locator: Cell::new(RLOC),
        rd_smgr: Default::default(),
        rd_id: REL_OID,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: LockInfoData {
            lockRelId: LockRelId { relId: REL_OID, dbId: 5 },
        },
        rd_rel,
        rd_att: int4_x2_tupdesc(mcx),
        rd_index: None,
        rd_opcintype: PgVec::new_in(mcx),
        rd_opfamily: PgVec::new_in(mcx),
        rd_indoption: PgVec::new_in(mcx),
        rd_indcollation: PgVec::new_in(mcx),
        rd_options: None,
        pgstat_enabled: Cell::new(false),
        pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(), rd_amcache_gin: Default::default(), rd_amcache_spgist: Default::default(),
        rd_support: PgVec::new_in(mcx),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false, rd_hasrules: false,
    }
}

fn install_relation_seams() {
    relation_seams::relation_open::set(|mcx, relid, _lockmode| {
        assert_eq!(relid, REL_OID, "unknown relation oid {relid}");
        Ok(Relation::open(test_relation(mcx), None))
    });
    relation_seams::relation_openrv_extended::set(
        |mcx, rv: &rel_vocab::RangeVar, _lockmode: LOCKMODE, missing_ok: bool| {
            if rv.relname == "t" {
                Ok(Some(Relation::open(test_relation(mcx), None)))
            } else if missing_ok {
                Ok(None)
            } else {
                Err(types_error::PgError::error("no such relation").into())
            }
        },
    );
}

fn install_parser_fixture_seams() {
    syscache_seams::lookup_pg_statistic_shape::set(|_, _, _| Ok(None));
    syscache_seams::lookup_pg_statistic_bundle::set(|_, _, _, _| Ok(None));
    syscache_seams::pg_statistic_stawidth::set(|_, _, _| Ok(None));
    indexcmds_seams::get_default_opclass::set(|_typid, _am| Ok(0));
    syscache_seams::syscache_hash_value_typeoid::set(|typid| {
        Ok(typid.wrapping_mul(0x9e37_79b1))
    });
    syscache_seams::syscache_hash_value_procoid::set(|funcid| {
        Ok(funcid.wrapping_mul(0x9e37_79b1))
    });
    syscache_seams::lookup_pg_type_typcache_shape::set(|_typid| {
        Ok(Some(syscache_seams::PgTypeTypcacheShape {
            typname: types_tuple::NameData::default(),
            typlen: 4,
            typbyval: true,
            typalign: b'i' as i8,
            typstorage: b'p' as i8,
            typtype: b'b' as i8,
            typisdefined: true,
            typrelid: 0,
            typsubscript: 0,
            typelem: 0,
            typarray: 0,
            typcollation: 0,
        }))
    });
    syscache_seams::lookup_pg_type_shape::set(|_typid| {
        Ok(Some(types_tuple::PgTypeShape {
            typlen: 4,
            typbyval: true,
            typalign: b'i' as i8,
            typstorage: b'p' as i8,
            typcollation: 0,
        }))
    });
    syscache_seams::pg_type_base_shape::set(|typid| {
        Ok(Some(syscache_seams::PgTypeBaseShape {
            typtype: if typid == 705 { b'p' as i8 } else { b'b' as i8 },
            typbasetype: 0,
            typtypmod: -1,
            typelem: 0,
            typsubscript: 0,
        }))
    });
    const INT4LE_OP: Oid = 523;
    const INT4LE_PROC: Oid = 149;
    syscache_seams::lookup_pg_operator_candidates::set(|mcx, name, l, r| {
        let mut v = mcx::vec_with_capacity_in(mcx, 1)?;
        if name == "<=" && l == INT4OID && r == INT4OID {
            v.push((INT4LE_OP, 11));
        }
        Ok(v)
    });
    syscache_seams::pg_operator_name_candidates_exist::set(|_, _| Ok(false));
    syscache_seams::lookup_pg_operator_shape::set(|opno| {
        Ok(match opno {
            INT4LE_OP => Some(syscache_seams::PgOperatorShape { oprnamespace: 11,
                oprleft: 23,
                oprright: 23,
                oprresult: 16,
                oprcom: 525,
                oprnegate: 521,
                oprcode: INT4LE_PROC,
                oprrest: 336,
                oprjoin: 386,
                oprcanmerge: false,
                oprcanhash: false,
            }),
            _ => None,
        })
    });
    syscache_seams::lookup_pg_proc_shape::set(|funcid| {
        Ok(match funcid {
            INT4LE_PROC => Some(syscache_seams::PgProcShape {
                prolang: 12,
                prosecdef: false,
                proconfig_isnull: true,
                pronamespace: 11,
                prorettype: 16,
                provariadic: 0,
                prosupport: 0,
                pronargs: 2,
                prokind: b'f' as i8,
                provolatile: b'i' as i8,
                proparallel: b's' as i8,
                proretset: false,
                proisstrict: true,
                proleakproof: true,
            }),
            _ => None,
        })
    });
    syscache_seams::pg_proc_cost_shape::set(|funcid| {
        Ok(match funcid {
            INT4LE_PROC => Some(syscache_seams::PgProcCostShape {
                procost: 1.0,
                prorows: 0.0,
                prosupport: 0,
            }),
            _ => None,
        })
    });
}

fn write_control_file(dir: &std::path::Path) {
    let mut cf = controldata_utils::ControlFileData::ZEROED;
    cf.system_identifier = SYS_ID;
    cf.pg_control_version = PG_CONTROL_VERSION;
    cf.catalog_version_no = controldata_utils::CATALOG_VERSION_NO;
    cf.state = DB_IN_PRODUCTION;
    cf.checkPoint = SEG as u64 + 40;
    cf.checkPointCopy.redo = SEG as u64 + 40;
    cf.checkPointCopy.ThisTimeLineID = 1;
    cf.checkPointCopy.PrevTimeLineID = 1;
    cf.checkPointCopy.nextXid = types_core::FullTransactionId::from_epoch_and_xid(0, 3);
    cf.unloggedLSN = FirstNormalUnloggedLSN;
    cf.maxAlign = 8;
    cf.floatFormat = FLOATFORMAT_VALUE;
    cf.blcksz = 8192;
    cf.relseg_size = 131072;
    cf.xlog_blcksz = 8192;
    cf.xlog_seg_size = SEG as u32;
    cf.nameDataLen = 64;
    cf.indexMaxKeys = 32;
    cf.toast_max_chunk_size = TOAST_MAX_CHUNK_SIZE;
    cf.loblksize = 2048;
    cf.float8ByVal = true;
    cf.crc = controldata_utils::crc_of_image(&cf.to_disk_bytes());
    let mut image = vec![0u8; PG_CONTROL_FILE_SIZE];
    image[..controldata_utils::SIZEOF_CONTROL_FILE_DATA].copy_from_slice(&cf.to_disk_bytes());
    std::fs::write(dir.join("global/pg_control"), &image).unwrap();
}

fn run_stmt(sql: &str) -> (CmdType, u64) {
    let sql: &'static str = Box::leak(sql.to_string().into_boxed_str());
    let ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("stmt")));
    let mcx = ctx.mcx();
    let list =
        gram_core::raw_parser(mcx, sql, parser_seams::RawParseMode::RAW_PARSE_DEFAULT).unwrap();
    assert_eq!(list.len(), 1);
    let raw = list.nth(0).as_raw_stmt().unwrap();
    let query =
        parser_analyze::parse_analyze_fixedparams(mcx, raw, sql, &[], Default::default()).unwrap();
    let mut rewritten = rewrite_handler::QueryRewrite(mcx, query).unwrap();
    assert_eq!(rewritten.len(), 1);
    let query = rewritten.pop().unwrap();
    let pstmt =
        planner::planner(mcx, mcx::leak_in(mcx::alloc_in(mcx, query).unwrap()), sql, 0, types_portal::ParamListHandle::NULL).unwrap();
    let pstmt: &'static types_nodes::plannodes::PlannedStmt<'static> =
        mcx::leak_in(mcx::alloc_in(mcx, pstmt).unwrap());

    let snapshot = snapmgr::GetTransactionSnapshot().unwrap();
    let qd = execmain_seams::create_query_desc::call(
        pstmt,
        sql,
        Some(snapshot),
        None,
        types_dest::CommandDest::None,
        types_portal::ParamListHandle::NULL,
        types_portal::QueryEnvHandle::NULL,
        0,
    )
    .unwrap();
    execmain_seams::executor_start::call(qd, 0).unwrap();
    let operation = execmain_seams::query_desc_operation::call(qd);
    let mut dest = tcop_dest::DestReceiver::DoNothing;
    execmain_seams::executor_run::call(qd, types_scan::sdir::ForwardScanDirection, 0, &mut dest)
        .unwrap();
    let processed = execmain_seams::query_desc_es_processed::call(qd);
    execmain_seams::executor_finish::call(qd).unwrap();
    execmain_seams::executor_end::call(qd).unwrap();
    execmain_seams::free_query_desc::call(qd);
    (operation, processed)
}

fn parse_vacuum(sql: &'static str, mcx: Mcx<'static>) -> &'static types_nodes::parsenodes::VacuumStmt<'static> {
    let list =
        gram_core::raw_parser(mcx, sql, parser_seams::RawParseMode::RAW_PARSE_DEFAULT).unwrap();
    assert_eq!(list.len(), 1);
    let raw = list.nth(0).as_raw_stmt().unwrap();
    raw.stmt.unwrap().as_vacuum_stmt().expect("VacuumStmt")
}

fn assert_no_pins(when: &str) {
    for b in 1..=TEST_NBUFFERS {
        assert_eq!(bufmgr::GetPrivateRefCount(b), 0, "buffer {b} still pinned {when}");
    }
}

fn pinned_count() -> usize {
    (1..=TEST_NBUFFERS).filter(|&b| bufmgr::GetPrivateRefCount(b) > 0).count()
}

#[test]
fn vacuum_error_mid_scan_abort_releases_all_pins() {
    let dir = std::env::temp_dir().join(format!("pgrust_vacuum_pin_abort_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    for sub in [
        "global",
        "pg_wal",
        "pg_xact",
        "pg_subtrans",
        "pg_multixact/offsets",
        "pg_multixact/members",
    ] {
        std::fs::create_dir_all(dir.join(sub)).unwrap();
    }
    std::env::set_current_dir(&dir).unwrap();
    init_small::globals::SetDataDir(dir.to_str().unwrap());
    init_small::globals::set_enableFsync(false);

    install_proc_boot_seams();
    shmem::init_seams();
    fd::init_seams();
    guc_tables::init_seams();
    postgres_seams::check_for_interrupts::set(|| Ok(()));
    guc_tables::vars::VacuumCostDelay.install_if_absent(guc_tables::GucVarAccessors {
        get: init_small::globals::VacuumCostDelay,
        set: init_small::globals::SetVacuumCostDelay,
    });
    guc_tables::vars::VacuumCostLimit.install_if_absent(guc_tables::GucVarAccessors {
        get: init_small::globals::VacuumCostLimit,
        set: init_small::globals::SetVacuumCostLimit,
    });
    guc::init_seams();
    adt_bool::init_seams();
    adt_float::init_seams();
    transam_xlog::init_seams();
    xlogutils::init_seams();
    heapam_visibility::init_seams();
    clog::init_seams();
    subtrans::init_seams();
    transam::init_seams();
    varsup::init_seams();
    xact::init_seams();
    snapmgr::init_seams();
    resowner::init_seams();
    procarray::init_seams();
    inval::init_seams();
    pgstat::init_seams();
    table::init_seams();
    tableam::init_seams();
    multixact::init_seams();
    freespace::init_seams();
    vacuumlazy::init_seams();
    // pg_class isn't in this fixture harness: swallow the relstats write.
    vacuum_seams::vac_update_relstats::set(|_, _, _, _, _, _, _, _, _| Ok((false, false)));
    commands_vacuum::init_seams();
    autovacuum::init_seams();
    walwriter::init_seams();
    execmain::init_seams();
    nodeseqscan::init_seams();
    scan_fgram::init_seams();
    parser_driver::init_seams();
    parse_expr::init_seams();
    parser_analyze::init_seams();
    rewrite_handler::init_seams();
    planner::init_seams();
    install_smgr_disk();
    install_wal_seams();
    install_relation_seams();
    install_parser_fixture_seams();
    install_xact_periphery_seams();
    guc::store::initialize_guc_options().unwrap();

    fd::InitFileAccess();
    lwlock::CreateLWLocks(false).unwrap();
    lmgr_proc::init_seams();
    lmgr_proc::InitProcGlobal(&lmgr_proc::ProcGlobalConfig {
        autovacuum_worker_slots: 3,
        max_wal_senders: 2,
        max_prepared_xacts: 2,
        fastpath_lock_groups_per_backend: 1,
    });
    varsup::VarsupShmemInit();
    procarray::ProcArrayShmemInit();
    clog::CLOGShmemInit().unwrap();
    clog::BootStrapCLOG().unwrap();
    subtrans::SUBTRANSShmemInit().unwrap();
    subtrans::BootStrapSUBTRANS().unwrap();
    {
        use std::sync::atomic::{AtomicI32, Ordering::Relaxed as R};
        static MAX_PREPARED: AtomicI32 = AtomicI32::new(2);
        guc_tables::vars::max_prepared_xacts.install(guc_tables::GucVarAccessors {
            get: || MAX_PREPARED.load(R),
            set: |v| MAX_PREPARED.store(v, R),
        });
    }
    multixact::MultiXactShmemInit().unwrap();
    multixact_seams::multixact_set_next_mxact::call(1, 0);
    multixact::BootStrapMultiXact().unwrap();
    multixact_seams::set_multixact_id_limit::call(1, 5, true);
    multixact_seams::startup_multixact::call().unwrap();
    multixact_seams::trim_multixact::call().unwrap();
    lmgr_proc::InitProcess(BackendType::Backend).unwrap();
    procarray::ProcArrayAdd(lmgr_proc::MyProc().unwrap()).unwrap();

    init_small::globals::SetNBuffers(TEST_NBUFFERS);
    bufmgr::BufferManagerShmemInit().unwrap();
    bufmgr::init_seams();
    smgr_seams::smgr_create::call(
        RelFileLocatorBackend { locator: RLOC, backend: INVALID_PROC_NUMBER },
        ForkNumber::MAIN_FORKNUM,
        false,
    )
    .unwrap();

    write_control_file(&dir);
    transam_xlog::ReadControlFile().unwrap();
    transam_xlog::XLOGShmemInit();

    let end_of_log: XLogRecPtr = 2 * SEG as u64;
    let prev_rec: XLogRecPtr = SEG as u64 + 40;
    let ctl = transam_xlog::ctl::XLogCtl();
    ctl.InsertTimeLineID.store(1, Relaxed);
    ctl.PrevTimeLineID.store(1, Relaxed);
    ctl.Insert.CurrBytePos.store(XLogRecPtrToBytePos(end_of_log), Relaxed);
    ctl.Insert.PrevBytePos.store(XLogRecPtrToBytePos(prev_rec), Relaxed);
    ctl.Insert.fullPageWrites.store(true, Relaxed);
    ctl.Insert.RedoRecPtr.store(prev_rec, Relaxed);
    ctl.RedoRecPtr.store(prev_rec, Relaxed);
    ctl.InitializedUpTo.store(end_of_log, Relaxed);
    ctl.logInsertResult.store(end_of_log, Relaxed);
    ctl.logWriteResult.store(end_of_log, Relaxed);
    ctl.logFlushResult.store(end_of_log, Relaxed);
    ctl.LogwrtRqstWrite.store(end_of_log, Relaxed);
    ctl.LogwrtRqstFlush.store(end_of_log, Relaxed);
    ctl.SharedRecoveryState.store(RECOVERY_STATE_DONE, Relaxed);
    ctl.InstallXLogFileSegmentActive.store(true, Relaxed);
    xlogutils::set_in_recovery(false);
    subtrans::StartupSUBTRANS(3).unwrap();
    assert!(transam_xlog::XLogInsertAllowed());

    // Seed 700 rows (~4 heap pages), delete 600 so blocks 0..2 all prune.
    xact::StartTransactionCommand().unwrap();
    for i in 1..=700 {
        let (op, n) = run_stmt(&format!("INSERT INTO t VALUES ({i}, 0)"));
        assert_eq!((op, n), (CmdType::CMD_INSERT, 1));
    }
    xact::CommitTransactionCommand().unwrap();

    xact::StartTransactionCommand().unwrap();
    let (op, n) = run_stmt("DELETE FROM t WHERE a <= 600");
    assert_eq!((op, n), (CmdType::CMD_DELETE, 600), "DELETE 600");
    xact::CommitTransactionCommand().unwrap();
    assert_no_pins("after seeding");

    // VACUUM through the real grammar; the injected WAL failure surfaces from
    // lazy_scan_prune with the heap buffer still pinned.
    let ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("vac")));
    let stmt = parse_vacuum("VACUUM (SKIP_DATABASE_STATS) t", ctx.mcx());
    xact::StartTransactionCommand().unwrap();
    INJECT_ARMED.store(true, Relaxed);
    let err = commands_vacuum::ExecVacuum(ctx.mcx(), stmt, "", true)
        .expect_err("injected failure must surface");
    INJECT_ARMED.store(false, Relaxed);
    assert!(
        err.to_string().contains("injected mid-scan vacuum failure"),
        "unexpected error: {err}"
    );
    assert!(
        PRUNE_RECORDS.load(Relaxed) >= FAIL_ON_PRUNE,
        "fault fired mid-scan (saw {} prune records)",
        PRUNE_RECORDS.load(Relaxed)
    );
    assert!(pinned_count() > 0, "error path left a pinned buffer (the leak under test)");

    // C: no manual unpin on this path — AbortTransaction's
    // ResourceOwnerRelease(BEFORE_LOCKS) must drop the pins, and
    // AtEOXact_Buffers(false) inside it asserts none survive.
    xact::AbortCurrentTransaction().unwrap();
    assert_no_pins("after AbortCurrentTransaction");

    // The system stays usable: the same VACUUM now completes.
    xact::StartTransactionCommand().unwrap();
    commands_vacuum::ExecVacuum(ctx.mcx(), stmt, "", true).unwrap();
    xact::CommitTransactionCommand().unwrap();
    assert_no_pins("after successful VACUUM");
}
