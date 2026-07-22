// SPI end-to-end over a real data directory (harness cloned from
// nodemodifytable/tests/update_delete.rs): connect -> execute -> tuptable ->
// prepare/execute_plan with params -> DML visibility across commits -> error
// unwind via AtEOXact_SPI -> context/leak discipline.
use std::cell::Cell;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Mutex;

use datum::Datum;
use mcx::{Mcx, PgVec};
use transam_xlog::control_file::{
    FirstNormalUnloggedLSN, FLOATFORMAT_VALUE, PG_CONTROL_FILE_SIZE, PG_CONTROL_VERSION,
    TOAST_MAX_CHUNK_SIZE,
};
use transam_xlog::{XLogRecPtrToBytePos, DB_IN_PRODUCTION, RECOVERY_STATE_DONE};
use types_core::{
    BackendType, BlockNumber, Buffer, InvalidBlockNumber, InvalidOid, Oid, XLogRecPtr, BLCKSZ,
    INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT,
};
use types_rel::{
    FormData_pg_class, LockInfoData, LockRelId, Relation, RelationData, LOCKMODE,
    RELKIND_RELATION,
};
use types_storage::bufpage::PageRef;
use types_storage::RelFileLocator;
use types_tuple::{CompactAttribute, FormData_pg_attribute, NameData, TupleDescData};

const SEG: i32 = 16 * 1024 * 1024;
const SYS_ID: u64 = 0x5544_3322_1100_51AE;
const REL_OID: Oid = 61001;
const INT4OID: Oid = 23;

#[repr(align(8))]
struct TestPage([u8; BLCKSZ]);

struct Fake {
    pages: Vec<usize>,
    pins: Vec<i32>,
    locks: Vec<i32>,
}

static FAKE: Mutex<Fake> = Mutex::new(Fake {
    pages: Vec::new(),
    pins: Vec::new(),
    locks: Vec::new(),
});

fn with_fake<R>(f: impl FnOnce(&mut Fake) -> R) -> R {
    f(&mut FAKE.lock().unwrap_or_else(|e| e.into_inner()))
}

fn install_bufmgr_seams() {
    bufmgr_seams::read_buffer::set(|_rel, block| {
        with_fake(|f| {
            assert!((block as usize) < f.pages.len());
            f.pins[block as usize] += 1;
            Ok(block as Buffer + 1)
        })
    });
    bufmgr_seams::read_buffer_strategy::set(|rel, block, _strategy| {
        bufmgr_seams::read_buffer::call(rel, block)
    });
    bufmgr_seams::buffer_get_block_number::set(|buf| (buf - 1) as BlockNumber);
    bufmgr_seams::buffer_get_page::set(|buf| {
        let addr = with_fake(|f| {
            assert!(f.pins[(buf - 1) as usize] > 0, "page access without pin");
            f.pages[(buf - 1) as usize]
        });
        NonNull::new(addr as *mut u8).unwrap()
    });
    bufmgr_seams::release_buffer::set(|buf| {
        with_fake(|f| {
            let p = &mut f.pins[(buf - 1) as usize];
            assert!(*p > 0, "double release of buffer {buf}");
            *p -= 1;
        });
        Ok(())
    });
    bufmgr_seams::incr_buffer_ref_count::set(|buf| {
        with_fake(|f| f.pins[(buf - 1) as usize] += 1);
    });
    bufmgr_seams::lock_buffer::set(|buf, mode| {
        with_fake(|f| {
            let l = &mut f.locks[(buf - 1) as usize];
            match mode {
                bufmgr_seams::BUFFER_LOCK_UNLOCK => {
                    assert!(*l > 0, "unlock without lock");
                    *l -= 1;
                }
                _ => {
                    assert_eq!(*l, 0, "double content lock");
                    *l += 1;
                }
            }
        });
        Ok(())
    });
    bufmgr_seams::mark_buffer_dirty::set(|_buf| Ok(()));
    bufmgr_seams::mark_buffer_dirty_hint::set(|_buf, _std| Ok(()));
    bufmgr_seams::buffer_is_permanent::set(|_buf| true);
    bufmgr_seams::buffer_get_lsn_atomic::set(|buf| {
        let addr = with_fake(|f| f.pages[(buf - 1) as usize]);
        // SAFETY: leaked test page, always live.
        unsafe { PageRef::from_raw(NonNull::new(addr as *mut u8).unwrap()) }.lsn()
    });
    bufmgr_seams::relation_get_number_of_blocks_in_fork::set(|_rel, _fork| {
        with_fake(|f| Ok(f.pages.len() as BlockNumber))
    });
    bufmgr_seams::extend_buffered_rel_by::set(|_rel, _fork, _strategy, flags, extend_by| {
        assert_eq!(extend_by, 1);
        assert!(flags & bufmgr_seams::EB_LOCK_FIRST != 0);
        Ok(with_fake(|f| {
            let addr = Box::leak(Box::new(TestPage([0u8; BLCKSZ]))).0.as_mut_ptr() as usize;
            f.pages.push(addr);
            f.pins.push(1);
            f.locks.push(1);
            (f.pages.len() as Buffer, 1)
        }))
    });

    xloginsert_seams::xlog_reset_insertion::set(xloginsert::XLogResetInsertion);
    xloginsert_seams::xlog_insert::set(|rmid, info, fragments| {
        xloginsert::insert_record(rmid, info, 0, fragments, &[])
    });
    xloginsert_seams::xlog_insert_with_flags::set(|rmid, info, _flags, fragments| {
        xloginsert::insert_record(rmid, info, 0, fragments, &[])
    });
    xloginsert_seams::xlog_insert_record::set(|rmid, info, flags, main_data, bufs| {
        let mut blocks: Vec<xloginsert::RegBlock<'_>> = Vec::with_capacity(bufs.len());
        for b in bufs {
            let addr = with_fake(|f| f.pages[(b.buffer - 1) as usize]);
            blocks.push(xloginsert::RegBlock {
                block_id: b.block_id,
                rlocator: RelFileLocator::new(1663, 5, REL_OID),
                forknum: types_core::ForkNumber::MAIN_FORKNUM,
                block: (b.buffer - 1) as BlockNumber,
                // SAFETY: leaked test page, BLCKSZ, pinned by the caller.
                page: unsafe { core::slice::from_raw_parts(addr as *const u8, BLCKSZ) },
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
    g::SetMyProcPid(781);
    g::SetMyDatabaseId(5);
    g::set_transaction_buffers(64);
    g::set_subtransaction_buffers(64);

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
    miscinit_seams::is_bootstrap_processing_mode::set(|| false);
    waitevent_seams::pgstat_set_wait_event_storage::set(|_| {});
    waitevent_seams::pgstat_report_wait_start::set(|_| {});
    waitevent_seams::pgstat_report_wait_end::set(|| {});
    waitevent_seams::pgstat_reset_wait_event_storage::set(|| {});
    ipc_seams::on_shmem_exit::set(|_, _| {});
    deadlock_seams::init_dead_lock_checking::set(|| Ok(()));
    pmsignal_seams::register_postmaster_child_active::set(|| {});
    syncrep_seams::sync_rep_cleanup_at_proc_exit::set(|| {});
    condition_variable_seams::condition_variable_cancel_sleep::set(|| false);
    autovacuum_seams::wake_autovacuum_launcher::set(|| {});
    lock_seams::abort_strong_lock_acquire::set(|| {});
    lock_seams::get_awaited_lock_hashcode::set(|| None);
    lock_seams::lock_release_all::set(|_, _| lock::VirtualXactLockTableCleanup());
    lock_seams::lock_acquire_extended::set(|_, _, _, _, _, _| {
        Ok(types_storage::lock::LOCKACQUIRE_OK)
    });
    lock_seams::lock_release::set(|_, _, _| Ok(true));
    lock_seams::mark_lock_clear::set(|_, _| {});
    timeout_seams::disable_timeouts::set(|_| {});
    aio_seams::pgaio_closing_fd::set(|_| {});
    sync_seams::register_sync_request::set(|_, _, _| Ok(true));
    sinval_seams::receive_shared_invalid_messages::set(|_, _| Ok(()));
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
    // CheckCmdReplicaIdentity (execReplication.c), C-faithful under the
    // rig's ground truth: no pg_publication rows exist, so PublicationDesc
    // is all-valid with every pubaction false — rf/cols/gencols checks pass
    // and publishes=false admits UPDATE/DELETE without a replica identity.
    // C's early exits are kept explicit; the catalog-backed middle cannot
    // run against this rig's seamed syscache.
    execreplication_seams::check_cmd_replica_identity::set(|_mcx, rel, cmd| {
        use types_nodes::nodes_enums::CmdType;
        if rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_TABLE {
            return Ok(());
        }
        if cmd != CmdType::CMD_UPDATE && cmd != CmdType::CMD_DELETE {
            return Ok(());
        }
        Ok(())
    });
    async_seams::pre_commit_notify::set(|| Ok(()));
    async_seams::at_commit_notify::set(|| Ok(()));
    async_seams::at_abort_notify::set(|| {});
    tablecmds_seams::pre_commit_on_commit_actions::set(|| Ok(()));
    tablecmds_seams::at_eoxact_on_commit_actions::set(|_| {});
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
    multixact_seams::at_eoxact_multixact::set(|| {});
    multixact_seams::multi_xact_id_set_oldest_member::set(|| Ok(()));
    multixact_seams::multi_xact_id_is_running::set(|_, _| Ok(false));
    pg_enum_seams::at_eoxact_enum::set(|| {});
    relcache_seams::at_eoxact_relation_cache::set(|_| Ok(()));
    relcache_seams::relation_get_stat_ext_list::set(|mcx, _relid| Ok(PgVec::new_in(mcx)));
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
    freespace_seams::get_page_with_free_space::set(|_rel, _need| Ok(InvalidBlockNumber));
    freespace_seams::record_and_get_page_with_free_space::set(|_rel, _old, _avail, _need| {
        Ok(InvalidBlockNumber)
    });
    catalog_seams::is_catalog_relation::set(|_rel| false);
    catalog_seams::is_shared_relation::set(|_relid| false);
    dbcommands_seams::get_database_name::set(|_| Ok(Some("testdb".to_string())));
    syscache_seams::search_syscache_exists_databaseoid::set(|_| Ok(true));
    aclchk_seams::object_aclcheck::set(|_classid, _objid, _roleid, _mode| Ok(0));
    aclchk_seams::pg_class_aclmask::set(|_relid, _roleid, mask, _how_all| Ok(mask));
    lmgr_seams::check_relation_locked_by_me::set(|_, _, _| true);
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
        rd_locator: Default::default(),
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
    if !guc_tables::vars::cpu_operator_cost.installed() {
        guc_tables::vars::cpu_operator_cost
            .install(guc_tables::GucVarAccessors { get: || 0.0025, set: |_| {} });
    }
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
    // plancache search-path capture: empty resolved path is fine here.
    syscache_seams::lookup_authid_rolname::set(|_mcx, _roleid| Ok(None));
    syscache_seams::lookup_pg_namespace_oid_by_name::set(|_name| Ok(InvalidOid));

    // int4 `=` (96 -> int4eq 65) and `+` (551 -> int4pl 177), values per
    // pg_operator.dat/pg_proc.dat (parser_analyze test fixture precedent).
    const BOOLOID: Oid = 16;
    syscache_seams::lookup_pg_operator_candidates::set(|mcx, name, l, r| {
        let mut v = mcx::vec_with_capacity_in(mcx, 1)?;
        if l == INT4OID && r == INT4OID {
            match name {
                "=" => v.push((96, 11)),
                "+" => v.push((551, 11)),
                _ => {}
            }
        }
        Ok(v)
    });
    syscache_seams::pg_operator_name_candidates_exist::set(|name, _| {
        Ok(name == "=" || name == "+")
    });
    syscache_seams::lookup_pg_operator_shape::set(|opno| {
        Ok(match opno {
            96 => Some(syscache_seams::PgOperatorShape { oprnamespace: 11,
                oprleft: INT4OID,
                oprright: INT4OID,
                oprresult: BOOLOID,
                oprcom: 96,
                oprnegate: 518,
                oprcode: 65,
                oprrest: 101,
                oprjoin: 105,
                oprcanmerge: true,
                oprcanhash: true,
            }),
            551 => Some(syscache_seams::PgOperatorShape { oprnamespace: 11,
                oprleft: INT4OID,
                oprright: INT4OID,
                oprresult: INT4OID,
                oprcom: 551,
                oprnegate: InvalidOid,
                oprcode: 177,
                oprrest: InvalidOid,
                oprjoin: InvalidOid,
                oprcanmerge: false,
                oprcanhash: false,
            }),
            _ => None,
        })
    });
    // int4in 42 / int4out 43 (pg_proc.dat).
    syscache_seams::pg_type_io_shape::set(|typid| {
        Ok((typid == INT4OID).then(|| syscache_seams::PgTypeIoShape {
            oid: INT4OID,
            typinput: 42,
            typoutput: 43,
            typreceive: 2406,
            typsend: 2407,
            typmodin: 0,
            typmodout: 0,
            typelem: 0,
            typlen: 4,
            typbyval: true,
            typalign: b'i' as i8,
            typdelim: b',' as i8,
            typisdefined: true,
        }))
    });
    syscache_seams::lookup_pg_amop_members_by_operator::set(|mcx, opno| {
        let mut v = mcx::PgVec::new_in(mcx);
        if opno == 96 {
            v.push(syscache_seams::PgAmopMemberShape {
                amopfamily: 1976,
                amoplefttype: INT4OID,
                amoprighttype: INT4OID,
                amopstrategy: 3,
                amopmethod: 403,
            });
        }
        Ok(v)
    });
    syscache_seams::lookup_pg_amop_by_operator::set(|opno, purpose, opfamily| {
        Ok((opno == 96 && purpose == b's' && opfamily == 1976).then(|| {
            syscache_seams::PgAmopShape {
                amopstrategy: 3,
                amopsortfamily: 0,
                amoplefttype: INT4OID,
                amoprighttype: INT4OID,
            }
        }))
    });
    syscache_seams::lookup_pg_opfamily_shape::set(|opfid| {
        Ok((opfid == 1976).then(|| syscache_seams::PgOpfamilyShape {
            opfmethod: 403,
            opfname: types_tuple::NameData::default(),
        }))
    });
    syscache_seams::lookup_pg_amop_by_strategy::set(|opfamily, left, right, strategy| {
        Ok(match (opfamily, left, right, strategy) {
            (1976, INT4OID, INT4OID, 3) => 96,
            _ => 0,
        })
    });
    syscache_seams::pg_proc_cost_shape::set(|funcid| {
        Ok(matches!(funcid, 65 | 177).then(|| syscache_seams::PgProcCostShape {
            procost: 1.0,
            prorows: 0.0,
            prosupport: 0,
        }))
    });
    syscache_seams::lookup_pg_proc_shape::set(|funcid| {
        Ok(match funcid {
            65 | 177 => Some(syscache_seams::PgProcShape {
                prolang: 12,
                prosecdef: false,
                proconfig_isnull: true,
                pronamespace: 11,
                prorettype: if funcid == 65 { BOOLOID } else { INT4OID },
                provariadic: InvalidOid,
                prosupport: InvalidOid,
                pronargs: 2,
                prokind: b'f' as i8,
                provolatile: b'i' as i8,
                proparallel: b's' as i8,
                proretset: false,
                proisstrict: true,
                proleakproof: false,
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

fn boot() {
    let dir = std::env::temp_dir().join(format!("pgrust_spi_e2e_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    for sub in ["global", "pg_wal", "pg_xact", "pg_subtrans"] {
        std::fs::create_dir_all(dir.join(sub)).unwrap();
    }
    std::env::set_current_dir(&dir).unwrap();
    init_small::globals::SetDataDir(dir.to_str().unwrap());
    init_small::globals::set_enableFsync(false);

    install_proc_boot_seams();
    shmem::init_seams();
    fd::init_seams();
    guc_tables::init_seams();
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
    execmain::init_seams();
    nodeseqscan::init_seams();
    scan_fgram::init_seams();
    parser_driver::init_seams();
    parse_expr::init_seams();
    parser_analyze::init_seams();
    rewrite_handler::init_seams();
    planner::init_seams();
    plancache::init_seams();
    mbutils::init_seams();
    utility::init_seams();
    pquery::init_seams();
    spi::init_seams();
    install_bufmgr_seams();
    install_relation_seams();
    install_parser_fixture_seams();
    install_xact_periphery_seams();
    guc::store::initialize_guc_options().unwrap();
    miscinit::SetUserIdAndSecContext(types_core::BOOTSTRAP_SUPERUSERID, 0);

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
    lmgr_proc::InitProcess(BackendType::Backend).unwrap();
    procarray::ProcArrayAdd(lmgr_proc::MyProc().unwrap()).unwrap();

    write_control_file(&std::path::PathBuf::from(&dir));
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
}

fn begin() {
    xact::StartTransactionCommand().unwrap();
    let snap = snapmgr::GetTransactionSnapshot().unwrap();
    snapmgr::PushActiveSnapshot(&snap).unwrap();
}

fn commit() {
    snapmgr::PopActiveSnapshot().unwrap();
    xact::CommitTransactionCommand().unwrap();
}

fn select_pairs(sql: &str) -> Vec<(i32, i32)> {
    let res = spi::SPI_execute(sql, false, 0).unwrap();
    assert_eq!(res, spi::SPI_OK_SELECT);
    let h = spi::SPI_tuptable().expect("SELECT produced a tuptable");
    let rows = spi::tuptable_with(h, |tt| {
        assert_eq!(tt.tupdesc.natts, 2);
        tt.vals
            .iter()
            .map(|tup| {
                let (a, an) = spi::SPI_getbinval(tup, &tt.tupdesc, 1);
                let (b, bn) = spi::SPI_getbinval(tup, &tt.tupdesc, 2);
                assert!(!an && !bn);
                (a.as_i32(), b.as_i32())
            })
            .collect::<Vec<_>>()
    });
    assert_eq!(spi::SPI_processed(), rows.len() as u64);
    spi::SPI_freetuptable(h).unwrap();
    rows
}

#[test]
fn spi_end_to_end() {
    boot();

    // --- Txn 1: DML + SELECT + nesting + prepared plan, all through SPI. ---
    begin();
    assert_eq!(spi::SPI_connect().unwrap(), spi::SPI_OK_CONNECT);

    assert_eq!(spi::SPI_execute("INSERT INTO t VALUES (1, 10)", false, 0).unwrap(), spi::SPI_OK_INSERT);
    assert_eq!(spi::SPI_processed(), 1);
    assert!(spi::SPI_tuptable().is_none(), "INSERT without RETURNING has no tuptable");
    assert_eq!(spi::SPI_execute("INSERT INTO t VALUES (2, 20)", false, 0).unwrap(), spi::SPI_OK_INSERT);
    assert_eq!(spi::SPI_execute("INSERT INTO t VALUES (3, 30)", false, 0).unwrap(), spi::SPI_OK_INSERT);

    let mut rows = select_pairs("SELECT a, b FROM t");
    rows.sort();
    assert_eq!(rows, vec![(1, 10), (2, 20), (3, 30)]);

    // read-only arm: no CCI, the entry-time snapshot governs — this txn's own
    // inserts are invisible (C-exact), and DML is rejected with the C message.
    assert_eq!(spi::SPI_execute("SELECT a, b FROM t", true, 0).unwrap(), spi::SPI_OK_SELECT);
    assert_eq!(spi::SPI_processed(), 0, "read-only uses the pre-insert snapshot");
    let err = spi::SPI_execute("INSERT INTO t VALUES (9, 90)", true, 0).unwrap_err();
    assert!(
        err.message().contains("is not allowed in a non-volatile function"),
        "unexpected: {}",
        err.message()
    );

    // tcount limit arm.
    assert_eq!(spi::SPI_execute("SELECT a, b FROM t", false, 2).unwrap(), spi::SPI_OK_SELECT);
    assert_eq!(spi::SPI_processed(), 2);

    // Nested connection: inner results, outer globals restored on finish.
    let outer_processed = spi::SPI_processed();
    assert_eq!(spi::SPI_connect().unwrap(), spi::SPI_OK_CONNECT);
    assert_eq!(spi::debug_stack_depth(), 2);
    let inner = select_pairs("SELECT a, b FROM t WHERE a = 1");
    assert_eq!(inner, vec![(1, 10)]);
    assert_eq!(spi::SPI_finish().unwrap(), spi::SPI_OK_FINISH);
    assert_eq!(spi::SPI_processed(), outer_processed);
    assert_eq!(spi::debug_stack_depth(), 1);

    // SPI_prepare / SPI_execute_plan with a $1 parameter.
    let plan = spi::SPI_prepare("SELECT b FROM t WHERE a = $1", &[INT4OID]).unwrap();
    assert!(!plan.is_null());
    assert_eq!(spi::SPI_getargcount(plan), 1);
    assert_eq!(spi::SPI_getargtypeid(plan, 0), INT4OID);
    assert!(spi::SPI_plan_is_valid(plan));
    for (arg, want) in [(2, 20), (3, 30)] {
        let res =
            spi::SPI_execute_plan(plan, &[Datum::from_i32(arg)], &[false], false, 0).unwrap();
        assert_eq!(res, spi::SPI_OK_SELECT);
        assert_eq!(spi::SPI_processed(), 1);
        let h = spi::SPI_tuptable().unwrap();
        let b = spi::tuptable_with(h, |tt| {
            let (b, isnull) = spi::SPI_getbinval(&tt.vals[0], &tt.tupdesc, 1);
            assert!(!isnull);
            b.as_i32()
        });
        assert_eq!(b, want);
    }
    // SPI_getvalue renders through the real int4 output function.
    let res = spi::SPI_execute_plan(plan, &[Datum::from_i32(1)], &[false], false, 0).unwrap();
    assert_eq!(res, spi::SPI_OK_SELECT);
    let h = spi::SPI_tuptable().unwrap();
    let ctx = mcx::MemoryContext::new("getvalue");
    let rendered = spi::tuptable_with(h, |tt| {
        spi::SPI_getvalue(ctx.mcx(), &tt.vals[0], &tt.tupdesc, 1).unwrap().unwrap().to_vec()
    });
    assert_eq!(rendered, b"10");
    drop(ctx);

    // Unsaved plan dies with SPI_finish (procCxt discipline); freed explicitly
    // here to also exercise SPI_freeplan.
    assert_eq!(spi::SPI_freeplan(plan), 0);
    assert!(!spi::SPI_plan_is_valid(plan));

    // UPDATE through SPI.
    assert_eq!(
        spi::SPI_execute("UPDATE t SET b = b + 1 WHERE a = 1", false, 0).unwrap(),
        spi::SPI_OK_UPDATE
    );
    assert_eq!(spi::SPI_processed(), 1);

    assert_eq!(spi::SPI_finish().unwrap(), spi::SPI_OK_FINISH);
    assert_eq!(spi::debug_stack_depth(), 0);
    assert_eq!(spi::debug_live_counts(), (0, 0));
    commit();

    // --- Txn 2: committed DML is visible to a fresh snapshot. ---
    begin();
    spi::SPI_connect().unwrap();
    let mut rows = select_pairs("SELECT a, b FROM t");
    rows.sort();
    assert_eq!(rows, vec![(1, 11), (2, 20), (3, 30)]);

    // read-only arm with a fresh snapshot sees the committed rows.
    assert_eq!(spi::SPI_execute("SELECT a, b FROM t", true, 0).unwrap(), spi::SPI_OK_SELECT);
    assert_eq!(spi::SPI_processed(), 3);

    // A plan kept with SPI_keepplan survives SPI_finish.
    let kept = spi::SPI_prepare("SELECT b FROM t WHERE a = $1", &[INT4OID]).unwrap();
    assert_eq!(spi::SPI_keepplan(kept), 0);
    spi::SPI_finish().unwrap();
    commit();

    begin();
    spi::SPI_connect().unwrap();
    assert!(spi::SPI_plan_is_valid(kept));
    let res = spi::SPI_execute_plan(kept, &[Datum::from_i32(1)], &[false], false, 0).unwrap();
    assert_eq!(res, spi::SPI_OK_SELECT);
    let h = spi::SPI_tuptable().unwrap();
    let b = spi::tuptable_with(h, |tt| spi::SPI_getbinval(&tt.vals[0], &tt.tupdesc, 1).0.as_i32());
    assert_eq!(b, 11);
    assert_eq!(spi::SPI_freeplan(kept), 0);
    spi::SPI_finish().unwrap();
    commit();

    // --- Txn 3: error inside SPI_execute propagates; abort unwinds the
    // connect stack through AtEOXact_SPI exactly as C's longjmp path. ---
    begin();
    spi::SPI_connect().unwrap();
    spi::SPI_connect().unwrap();
    let err = spi::SPI_execute("SELECT nosuchcol FROM t", false, 0).unwrap_err();
    assert!(err.message().contains("nosuchcol"), "unexpected: {}", err.message());
    assert_eq!(spi::debug_stack_depth(), 2, "error alone does not pop the stack");
    xact::AbortCurrentTransaction().unwrap();
    assert_eq!(spi::debug_stack_depth(), 0, "abort unwound both SPI levels");
    assert_eq!(spi::debug_live_counts(), (0, 0));

    // --- Txn 4: the stack is reusable after the abort. ---
    begin();
    spi::SPI_connect().unwrap();
    let mut rows = select_pairs("SELECT a, b FROM t");
    rows.sort();
    assert_eq!(rows, vec![(1, 11), (2, 20), (3, 30)]);
    spi::SPI_finish().unwrap();
    commit();

    with_fake(|f| {
        assert!(f.pins.iter().all(|p| *p == 0), "leaked pins: {:?}", f.pins);
        assert!(f.locks.iter().all(|l| *l == 0), "leaked locks: {:?}", f.locks);
    });
}
