// SELECT-from-view end to end: pg_rewrite fixture rule (live-PG-18.3-captured
// ev_action shape, column b's type ids adjusted to the int4 fixture schema) ->
// QueryRewrite expands v into an RTE_SUBQUERY -> pull_up_simple_subquery
// flattens it -> the plan is EXPLAIN-identical to the direct-table query and
// executes to the same rows. Catalog access is faked at the relcache/syscache
// boundaries (insert_select.rs harness); everything between is shipped code.
use std::cell::Cell;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Mutex;

use mcx::{Mcx, MemoryContext, PgVec};
use transam_xlog::control_file::{
    FirstNormalUnloggedLSN, FLOATFORMAT_VALUE, PG_CONTROL_FILE_SIZE, PG_CONTROL_VERSION,
    TOAST_MAX_CHUNK_SIZE,
};
use transam_xlog::{XLogRecPtrToBytePos, DB_IN_PRODUCTION, RECOVERY_STATE_DONE};
use types_core::{
    BackendType, BlockNumber, Buffer, InvalidBlockNumber, Oid, XLogRecPtr, BLCKSZ,
    INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT,
};
use types_nodes::nodes_enums::CmdType;
use types_nodes::plannodes::PlannedStmt;
use types_rel::{
    FormData_pg_class, LockInfoData, LockRelId, Relation, RelationData, LOCKMODE,
    RELKIND_RELATION, RELKIND_VIEW,
};
use types_storage::RelFileLocator;
use types_tuple::{CompactAttribute, FormData_pg_attribute, NameData, TupleDescData};

const SEG: i32 = 16 * 1024 * 1024;
const SYS_ID: u64 = 0x5544_3322_1100_AACE;
const T_OID: Oid = 16384;
const V_OID: Oid = 16385;
const RLOC: RelFileLocator = RelFileLocator::new(1663, 5, T_OID);
const INT4OID: Oid = 23;
const INT4GT_OP: Oid = 521;
const INT4GT_PROC: Oid = 147;

// Live PG 18.3 capture for CREATE VIEW v AS SELECT a, b FROM t (t = 16384);
// b's vartype 25/varcollid 100 adjusted to 23/0 for the int4 fixture schema.
const EV_ACTION_V: &str = r#"({QUERY :commandType 1 :querySource 0 :canSetTag true :utilityStmt <> :resultRelation 0 :hasAggs false :hasWindowFuncs false :hasTargetSRFs false :hasSubLinks false :hasDistinctOn false :hasRecursive false :hasModifyingCTE false :hasForUpdate false :hasRowSecurity false :hasGroupRTE false :isReturn false :cteList <> :rtable ({RANGETBLENTRY :alias <> :eref {ALIAS :aliasname t :colnames ("a" "b")} :rtekind 0 :relid 16384 :inh true :relkind r :rellockmode 1 :perminfoindex 1 :tablesample <> :lateral false :inFromCl true :securityQuals <>}) :rteperminfos ({RTEPERMISSIONINFO :relid 16384 :inh true :requiredPerms 2 :checkAsUser 0 :selectedCols (b 8 9) :insertedCols (b) :updatedCols (b)}) :jointree {FROMEXPR :fromlist ({RANGETBLREF :rtindex 1}) :quals <>} :mergeActionList <> :mergeTargetRelation 0 :mergeJoinCondition <> :targetList ({TARGETENTRY :expr {VAR :varno 1 :varattno 1 :vartype 23 :vartypmod -1 :varcollid 0 :varnullingrels (b) :varlevelsup 0 :varreturningtype 0 :varnosyn 1 :varattnosyn 1 :location -1} :resno 1 :resname a :ressortgroupref 0 :resorigtbl 16384 :resorigcol 1 :resjunk false} {TARGETENTRY :expr {VAR :varno 1 :varattno 2 :vartype 23 :vartypmod -1 :varcollid 0 :varnullingrels (b) :varlevelsup 0 :varreturningtype 0 :varnosyn 1 :varattnosyn 2 :location -1} :resno 2 :resname b :ressortgroupref 0 :resorigtbl 16384 :resorigcol 2 :resjunk false}) :override 0 :onConflict <> :returningOldAlias <> :returningNewAlias <> :returningList <> :groupClause <> :groupDistinct false :groupingSets <> :havingQual <> :windowClause <> :distinctClause <> :sortClause <> :limitOffset <> :limitCount <> :limitOption 0 :rowMarks <> :setOperations <> :constraintDeps <> :withCheckOptions <> :stmt_location -1 :stmt_len -1})"#;

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
        unsafe { types_storage::bufpage::PageRef::from_raw(NonNull::new(addr as *mut u8).unwrap()) }
            .lsn()
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
                rlocator: RLOC,
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

    backend_status_seams::pgstat_clear_backend_status_snapshot::set(|| {});
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
    timestamp_seams::get_current_timestamp::set(|| 778_000_000);
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
    multixact_seams::at_eoxact_multixact::set(|| {});
    multixact_seams::multi_xact_id_set_oldest_member::set(|| Ok(()));
    multixact_seams::multi_xact_id_is_running::set(|_, _| Ok(false));
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
    dbcommands_seams::get_database_name::set(|_| Ok(Some("testdb".to_string())));
    syscache_seams::search_syscache_exists_databaseoid::set(|_| Ok(true));
    aclchk_seams::pg_class_aclmask::set(|_relid, _roleid, mask, _how_all| Ok(mask));
    aclchk_seams::object_aclcheck::set(|_classid, _objid, _roleid, _mode| Ok(0));
    lmgr_seams::check_relation_locked_by_me::set(|_, _, _| true);
}

fn int4_x2_tupdesc<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> Rc<TupleDescData<'mcx>> {
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    for (i, name) in ["a", "b"].iter().enumerate() {
        let mut att = FormData_pg_attribute {
            attrelid: relid,
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

fn test_relation<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> RelationData<'mcx> {
    let is_view = relid == V_OID;
    let mut relname = NameData::default();
    relname.namestrcpy(if is_view { "v" } else { "t" });
    let rd_rel = FormData_pg_class {
        relname,
        relnamespace: 2200,
        reltype: 0,
        relowner: 10,
        relam: if is_view { 0 } else { tableam_vocab::HEAP_TABLE_AM_OID },
        relfilenode: if is_view { 0 } else { relid },
        reltablespace: 0,
        relpages: 0,
        reltuples: -1.0,
        relallvisible: 0,
        reltoastrelid: 0,
        relhasindex: false,
        relisshared: false,
        relpersistence: RELPERSISTENCE_PERMANENT,
        relkind: if is_view { RELKIND_VIEW } else { RELKIND_RELATION },
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
        rd_id: relid,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: LockInfoData { lockRelId: LockRelId { relId: relid, dbId: 5 } },
        rd_rel,
        rd_att: int4_x2_tupdesc(mcx, relid),
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
            rd_hastriggers: false,
            rd_hasrules: is_view,
    }
}

fn install_relation_seams() {
    relation_seams::relation_open::set(|mcx, relid, _lockmode| {
        assert!(relid == T_OID || relid == V_OID, "unknown relation oid {relid}");
        Ok(Relation::open(test_relation(mcx, relid), None))
    });
    relation_seams::relation_openrv_extended::set(
        |mcx, rv: &rel_vocab::RangeVar, _lockmode: LOCKMODE, missing_ok: bool| {
            match rv.relname {
                "t" => Ok(Some(Relation::open(test_relation(mcx, T_OID), None))),
                "v" => Ok(Some(Relation::open(test_relation(mcx, V_OID), None))),
                _ if missing_ok => Ok(None),
                _ => Err(types_error::PgError::error("no such relation").into()),
            }
        },
    );
    relcache_seams::relation_get_stat_ext_list::set(|mcx, _relid| Ok(mcx::PgVec::new_in(mcx)));
    relcache_seams::relation_get_fkey_list::set(|_relid| Ok(std::rc::Rc::from(Vec::new())));
    relcache_build_seams::scan_pg_rewrite::set(|mcx, ev_class| {
        let mut rows = mcx::vec_with_capacity_in(mcx, 1)?;
        if ev_class == V_OID {
            rows.push(relcache_build_seams::PgRewriteRuleShape {
                rule_id: 31000,
                ev_type: b'1',
                ev_enabled: b'O',
                is_instead: true,
                ev_qual: "<>",
                ev_action: EV_ACTION_V,
            });
        }
        Ok(rows)
    });
}

fn install_parser_fixture_seams() {
    syscache_seams::lookup_pg_statistic_shape::set(|_, _, _| Ok(None));
    syscache_seams::lookup_pg_statistic_bundle::set(|_, _, _, _| Ok(None));
    syscache_seams::pg_statistic_stawidth::set(|_, _, _| Ok(None));
    indexcmds_seams::get_default_opclass::set(|_typid, _am| Ok(0));
    syscache_seams::lookup_pg_class_ls_shape::set(|relid| {
        Ok((relid == T_OID || relid == V_OID).then(|| {
            let is_view = relid == V_OID;
            syscache_seams::PgClassLsShape {
                relnamespace: 2200,
                reltype: 0,
                relam: if is_view { 0 } else { tableam_vocab::HEAP_TABLE_AM_OID },
                reltablespace: 0,
                relnatts: 2,
                relkind: if is_view { b'v' as i8 } else { b'r' as i8 },
                relpersistence: RELPERSISTENCE_PERMANENT as i8,
                relispartition: false,
                relhassubclass: false,
            }
        }))
    });
    syscache_seams::lookup_pg_attribute_shape::set(|relid, attnum| {
        if relid != T_OID && relid != V_OID {
            return Ok(None);
        }
        let mut n = types_tuple::NameData::default();
        match attnum {
            1 => n.namestrcpy("a"),
            2 => n.namestrcpy("b"),
            _ => return Ok(None),
        }
        Ok(Some(syscache_seams::PgAttributeLsShape {
            attname: n,
            atttypid: INT4OID,
            atttypmod: -1,
            attcollation: 0,
            attgenerated: 0,
        }))
    });
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
    // "a > 5": int4gt resolution (pg_operator.dat oid 521, proc 147,
    // oprrest scalargtsel 104) + its pg_proc row.
    syscache_seams::lookup_pg_operator_candidates::set(|mcx, name, l, r| {
        let mut v = mcx::vec_with_capacity_in(mcx, 1)?;
        if name == ">" && l == INT4OID && r == INT4OID {
            v.push((INT4GT_OP, 11));
        }
        Ok(v)
    });
    syscache_seams::pg_operator_name_candidates_exist::set(|_, _| Ok(false));
    syscache_seams::lookup_pg_operator_shape::set(|opno| {
        Ok(match opno {
            INT4GT_OP => Some(syscache_seams::PgOperatorShape { oprnamespace: 11,
                oprleft: 23,
                oprright: 23,
                oprresult: 16,
                oprcom: 97,
                oprnegate: 523,
                oprcode: INT4GT_PROC,
                oprrest: 104,
                oprjoin: 107,
                oprcanmerge: false,
                oprcanhash: false,
            }),
            _ => None,
        })
    });
    syscache_seams::lookup_pg_proc_shape::set(|funcid| {
        Ok(match funcid {
            INT4GT_PROC => Some(syscache_seams::PgProcShape {
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
            INT4GT_PROC => Some(syscache_seams::PgProcCostShape { procost: 1.0, prorows: 0.0, prosupport: 0 }),
            _ => None,
        })
    });
    syscache_seams::lookup_pg_amop_members_by_operator::set(|mcx, _opno| {
        Ok(mcx::PgVec::new_in(mcx))
    });
    syscache_seams::pg_class_relname::set(|relid| {
        let mut name = NameData::default();
        name.namestrcpy(match relid {
            T_OID => "t",
            V_OID => "v",
            _ => return Ok(None),
        });
        Ok(Some(name))
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

// One statement through parse -> analyze -> rewrite -> plan -> executor;
// returns (operation, es_processed, planned statement).
fn run_stmt(sql: &'static str) -> (CmdType, u64, &'static PlannedStmt<'static>) {
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
    let pstmt: &'static PlannedStmt<'static> = mcx::leak_in(mcx::alloc_in(mcx, pstmt).unwrap());

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
    (operation, processed, pstmt)
}

// Re-plan the statement and render its EXPLAIN text (ExplainOnePlan consumes
// the PlannedStmt by value; planning is deterministic under fixed fixtures).
fn explain_stmt(sql: &'static str) -> String {
    let ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("explain")));
    let mcx = ctx.mcx();
    let list =
        gram_core::raw_parser(mcx, sql, parser_seams::RawParseMode::RAW_PARSE_DEFAULT).unwrap();
    let raw = list.nth(0).as_raw_stmt().unwrap();
    let query =
        parser_analyze::parse_analyze_fixedparams(mcx, raw, sql, &[], Default::default()).unwrap();
    let mut rewritten = rewrite_handler::QueryRewrite(mcx, query).unwrap();
    let query = rewritten.pop().unwrap();
    let pstmt = planner::planner(mcx, mcx::leak_in(mcx::alloc_in(mcx, query).unwrap()), sql, 0, types_portal::ParamListHandle::NULL).unwrap();

    let snapshot = snapmgr::GetTransactionSnapshot().unwrap();
    snapmgr::PushActiveSnapshot(&snapshot).unwrap();
    let mut es = explain::NewExplainState(mcx).unwrap();
    explain::ExplainOnePlan(
        mcx,
        pstmt,
        None,
        &mut es,
        sql,
        types_portal::ParamListHandle::NULL,
        types_portal::QueryEnvHandle::NULL,
        std::time::Duration::from_millis(0),
        None,
        None,
    )
    .unwrap();
    snapmgr::PopActiveSnapshot().unwrap();
    String::from_utf8(es.str.as_bytes().to_vec()).unwrap()
}

#[test]
fn select_from_view_matches_direct_query() {
    let dir = std::env::temp_dir().join(format!("pgrust_view_e2e_{}", std::process::id()));
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
    execreplication::init_seams();
    install_bufmgr_seams();
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
    lmgr_proc::InitProcess(BackendType::Backend).unwrap();
    procarray::ProcArrayAdd(lmgr_proc::MyProc().unwrap()).unwrap();

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

    xact::StartTransactionCommand().unwrap();
    let (op, n, _) = run_stmt("INSERT INTO t VALUES (1, 2)");
    assert_eq!((op, n), (CmdType::CMD_INSERT, 1));
    let (op, n, _) = run_stmt("INSERT INTO t VALUES (7, 8)");
    assert_eq!((op, n), (CmdType::CMD_INSERT, 1));
    xact::CommitTransactionCommand().unwrap();

    xact::StartTransactionCommand().unwrap();
    let (op, n, direct_pstmt) = run_stmt("SELECT a, b FROM t WHERE a > 5");
    assert_eq!(op, CmdType::CMD_SELECT);
    assert_eq!(n, 1, "direct query returns exactly the a=7 row");

    let (op, n, view_pstmt) = run_stmt("SELECT * FROM v WHERE a > 5");
    assert_eq!(op, CmdType::CMD_SELECT);
    assert_eq!(n, 1, "view query returns exactly the a=7 row");

    // The pulled-up view plans to the same SeqScan-on-t as the direct query.
    let direct_scan = direct_pstmt.planTree.unwrap().as_seq_scan().unwrap();
    let view_scan = view_pstmt.planTree.unwrap().as_seq_scan().unwrap();
    let scan_relid = |p: &'static PlannedStmt<'static>, rti: u32| {
        p.rtable
            .nth(rti as usize - 1)
            .as_range_tbl_entry()
            .unwrap()
            .relid
    };
    assert_eq!(scan_relid(direct_pstmt, direct_scan.scan.scanrelid), T_OID);
    assert_eq!(scan_relid(view_pstmt, view_scan.scan.scanrelid), T_OID);
    assert_eq!(
        direct_scan.scan.plan.targetlist.len(),
        view_scan.scan.plan.targetlist.len()
    );
    assert_eq!(direct_scan.scan.plan.qual.len(), view_scan.scan.plan.qual.len());
    assert_eq!(direct_scan.scan.plan.total_cost, view_scan.scan.plan.total_cost);
    assert_eq!(direct_scan.scan.plan.plan_rows, view_scan.scan.plan.plan_rows);

    // The view plan additionally carries v's RTE (lock + ACL surface) and
    // its perminfo checked as the view owner path.
    assert_eq!(direct_pstmt.rtable.len(), 1);
    assert_eq!(view_pstmt.rtable.len(), 2);
    assert_eq!(view_pstmt.permInfos.len(), 2);

    // The quals are the same int4gt(Var, 5) opexpr.
    let qual_pair = |scan: &types_nodes::plannodes::SeqScan<'static>| {
        let op = scan.scan.plan.qual.nth(0).as_op_expr().unwrap();
        let v = op.args.nth(0).as_var().unwrap();
        let c = op.args.nth(1).as_const().unwrap();
        (op.opno, v.varattno, c.constvalue.as_i32())
    };
    assert_eq!(qual_pair(direct_scan), (INT4GT_OP, 1, 5));
    assert_eq!(qual_pair(view_scan), (INT4GT_OP, 1, 5));

    // EXPLAIN-compared: byte-identical rendering. (The WHERE variant's
    // Filter line needs ruleutils OpExpr deparse — unported rock — so the
    // textual comparison runs on the no-qual pair; the WHERE pair is
    // field-compared above.)
    let direct_explain = explain_stmt("SELECT a, b FROM t");
    let view_explain = explain_stmt("SELECT * FROM v");
    assert!(!direct_explain.is_empty());
    assert_eq!(direct_explain, view_explain);
    assert!(direct_explain.contains("Seq Scan on t"), "{direct_explain}");

    xact::CommitTransactionCommand().unwrap();

    with_fake(|f| {
        assert!(f.pins.iter().all(|p| *p == 0), "leaked pins: {:?}", f.pins);
        assert!(f.locks.iter().all(|l| *l == 0), "leaked locks: {:?}", f.locks);
    });
}
