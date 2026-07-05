use std::cell::{Cell, RefCell};
use std::sync::Once;

use datum::Datum;
use types_core::{InvalidOid, Oid};
use types_storage::{PgClassShape, SharedInvalidationMessage};

use crate::eoxact::*;
use crate::invalidate::*;
use crate::local::*;
use crate::registration;
use crate::with_state;

thread_local! {
    static EVENTS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
    static SENT: RefCell<Vec<Vec<SharedInvalidationMessage>>> = const { RefCell::new(Vec::new()) };
    static PENDING: RefCell<Vec<SharedInvalidationMessage>> = const { RefCell::new(Vec::new()) };
    static NEST_LEVEL: Cell<i32> = const { Cell::new(1) };
}

fn log(event: String) {
    EVENTS.with(|e| e.borrow_mut().push(event));
}

fn events() -> Vec<String> {
    EVENTS.with(|e| e.borrow().clone())
}

fn clear_events() {
    EVENTS.with(|e| e.borrow_mut().clear());
}

fn sent() -> Vec<Vec<SharedInvalidationMessage>> {
    SENT.with(|s| s.borrow().clone())
}

const INIT_FILE_REL: Oid = 4242;
const SHARED_REL: Oid = 1262;

fn install() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        crate::init_seams();
        xact_seams::get_current_transaction_nest_level::set(|| NEST_LEVEL.get());
        xact_seams::get_current_command_id::set(|_used| Ok(0));
        catalog_seams::is_shared_relation::set(|relid| relid == SHARED_REL);
        miscinit_seams::is_bootstrap_processing_mode::set(|| false);
        transam_xlog_seams::xlog_logical_info_active::set(|| false);
        syscache_seams::relation_invalidates_snapshots_only::set(|_| false);
        syscache_seams::sys_cache_invalidate::set(|id, hash| {
            log(format!("syscache_invalidate({id},{hash})"));
            Ok(())
        });
        syscache_seams::lookup_pg_class_by_relid::set(|relid| {
            Ok(Some(PgClassShape {
                oid: relid,
                relnamespace: 0,
                relfilenode: relid,
                reltablespace: 0,
                relisshared: relid == SHARED_REL,
                relpersistence: b'p' as i8,
                relkind: b'r' as i8,
            }))
        });
        catcache_seams::catalog_cache_flush_catalog::set(|cat| {
            log(format!("flush_catalog({cat})"));
            Ok(())
        });
        catcache_seams::reset_catalog_caches_ext::set(|dd| {
            log(format!("reset_catalog_caches({dd})"));
            Ok(())
        });
        snapmgr_seams::invalidate_catalog_snapshot::set(|| log("snapshot".to_string()));
        relcache_seams::relation_cache_invalidate::set(|dd| {
            log(format!("relcache_invalidate({dd})"));
            Ok(())
        });
        relcache_seams::relation_cache_invalidate_entry::set(|relid| {
            log(format!("relcache_entry({relid})"));
            Ok(())
        });
        relcache_seams::relation_id_is_in_init_file::set(|relid| relid == INIT_FILE_REL);
        relcache_seams::relation_cache_init_file_pre_invalidate::set(|| {
            log("initfile_pre".to_string());
            Ok(())
        });
        relcache_seams::relation_cache_init_file_post_invalidate::set(|| {
            log("initfile_post".to_string());
            Ok(())
        });
        relmapper_seams::relation_map_invalidate::set(|shared| {
            log(format!("relmap({shared})"));
            Ok(())
        });
        smgr_seams::smgr_release_rel_locator::set(|rl| {
            log(format!("smgr({},{})", rl.locator.relNumber, rl.backend));
            Ok(())
        });
        sinval_seams::send_shared_invalid_messages::set(|msgs| {
            SENT.with(|s| s.borrow_mut().push(msgs.to_vec()));
            Ok(())
        });
        sinval_seams::receive_shared_invalid_messages::set(|inval_fn, _reset_fn| {
            loop {
                let msg = PENDING.with(|p| {
                    let mut p = p.borrow_mut();
                    if p.is_empty() { None } else { Some(p.remove(0)) }
                });
                match msg {
                    Some(m) => inval_fn(&m)?,
                    None => return Ok(()),
                }
            }
        });
    });
    init_small::globals::SetMyDatabaseId(5);
    NEST_LEVEL.set(1);
}

fn sent_flat() -> Vec<SharedInvalidationMessage> {
    sent().into_iter().flatten().collect()
}

#[test]
fn commit_sends_catalog_message_after_local_processing() {
    install();
    CacheInvalidateCatalog(1259).unwrap();

    clear_events();
    CommandEndInvalidationMessages().unwrap();
    assert_eq!(events(), vec!["snapshot".to_string(), "flush_catalog(1259)".to_string()]);

    AtEOXact_Inval(true).unwrap();
    let flat = sent_flat();
    assert_eq!(flat.len(), 1);
    match flat[0] {
        SharedInvalidationMessage::Catalog(m) => {
            assert_eq!(m.dbId, 5);
            assert_eq!(m.catId, 1259);
        }
        other => panic!("unexpected message {other:?}"),
    }
}

#[test]
fn abort_locally_processes_prior_and_sends_nothing() {
    install();
    CacheInvalidateCatalog(1259).unwrap();
    CommandEndInvalidationMessages().unwrap();

    clear_events();
    AtEOXact_Inval(false).unwrap();
    assert_eq!(events(), vec!["snapshot".to_string(), "flush_catalog(1259)".to_string()]);
    assert!(sent().is_empty());
    assert!(with_state(|s| s.trans_stack.is_empty() && s.msg_arrays[0].is_empty()));
}

#[test]
fn relcache_dedup_and_catcache_first_ordering() {
    install();
    CacheInvalidateCatalog(1247).unwrap();
    CacheInvalidateRelcacheByRelid(100).unwrap();
    CacheInvalidateRelcacheByRelid(100).unwrap();
    CacheInvalidateRelcacheByRelid(200).unwrap();

    AtEOXact_Inval(true).unwrap();
    let flat = sent_flat();
    assert_eq!(flat.len(), 3);
    assert!(matches!(flat[0], SharedInvalidationMessage::Catalog(_)));
    assert!(
        matches!(flat[1], SharedInvalidationMessage::Relcache(m) if m.relId == 100 && m.dbId == 5)
    );
    assert!(matches!(flat[2], SharedInvalidationMessage::Relcache(m) if m.relId == 200));
}

#[test]
fn subxact_commit_merges_into_parent() {
    install();
    CacheInvalidateRelcacheByRelid(100).unwrap();
    CommandEndInvalidationMessages().unwrap();

    NEST_LEVEL.set(2);
    CacheInvalidateRelcacheByRelid(200).unwrap();
    AtEOSubXact_Inval(true).unwrap();
    NEST_LEVEL.set(1);

    AtEOXact_Inval(true).unwrap();
    let rel_ids: Vec<Oid> = sent_flat()
        .iter()
        .filter_map(|m| match m {
            SharedInvalidationMessage::Relcache(rc) => Some(rc.relId),
            _ => None,
        })
        .collect();
    assert_eq!(rel_ids, vec![100, 200]);
}

#[test]
fn subxact_abort_discards_messages() {
    install();
    CacheInvalidateRelcacheByRelid(100).unwrap();
    CommandEndInvalidationMessages().unwrap();

    NEST_LEVEL.set(2);
    CacheInvalidateRelcacheByRelid(200).unwrap();
    CommandEndInvalidationMessages().unwrap();
    AtEOSubXact_Inval(false).unwrap();
    NEST_LEVEL.set(1);

    AtEOXact_Inval(true).unwrap();
    let rel_ids: Vec<Oid> = sent_flat()
        .iter()
        .filter_map(|m| match m {
            SharedInvalidationMessage::Relcache(rc) => Some(rc.relId),
            _ => None,
        })
        .collect();
    assert_eq!(rel_ids, vec![100]);
}

#[test]
fn forget_inplace_rolls_dense_arrays_back() {
    install();
    CacheInvalidateCatalog(1259).unwrap();

    with_state(|state| {
        let mcx = state.mcx;
        let info = registration::prepare_inplace_invalidation_state(state);
        registration::register_catalog_invalidation(mcx, state, info, 5, 2606).unwrap();
        assert_eq!(state.msg_arrays[0].len(), 2);
    });
    ForgetInplace_Inval();
    with_state(|state| assert_eq!(state.msg_arrays[0].len(), 1));

    CacheInvalidateCatalog(1247).unwrap();
    AtEOXact_Inval(true).unwrap();
    let cat_ids: Vec<Oid> = sent_flat()
        .iter()
        .filter_map(|m| match m {
            SharedInvalidationMessage::Catalog(c) => Some(c.catId),
            _ => None,
        })
        .collect();
    assert_eq!(cat_ids, vec![1259, 1247]);
}

#[test]
fn forget_inplace_tolerates_aborted_subxact_tail() {
    install();
    CacheInvalidateCatalog(1259).unwrap();
    CommandEndInvalidationMessages().unwrap();

    NEST_LEVEL.set(2);
    CacheInvalidateCatalog(2606).unwrap();
    CommandEndInvalidationMessages().unwrap();
    AtEOSubXact_Inval(false).unwrap();
    NEST_LEVEL.set(1);

    with_state(|state| {
        registration::prepare_inplace_invalidation_state(state);
        assert!(state.msg_arrays[0].len() > state.trans_stack[0].prior_cmd_invalid_msgs.nextmsg[0]);
    });
    ForgetInplace_Inval();

    AtEOXact_Inval(true).unwrap();
    let cat_ids: Vec<Oid> = sent_flat()
        .iter()
        .filter_map(|m| match m {
            SharedInvalidationMessage::Catalog(c) => Some(c.catId),
            _ => None,
        })
        .collect();
    assert_eq!(cat_ids, vec![1259]);
}

#[test]
fn init_file_relcache_inval_brackets_the_send() {
    install();
    CacheInvalidateRelcacheByRelid(INIT_FILE_REL).unwrap();
    CommandEndInvalidationMessages().unwrap();

    clear_events();
    AtEOXact_Inval(true).unwrap();
    let ev = events();
    assert_eq!(ev.first().map(String::as_str), Some("initfile_pre"));
    assert_eq!(ev.last().map(String::as_str), Some("initfile_post"));
    assert!(!sent().is_empty());
}

#[test]
fn xact_get_committed_messages_keeps_ateoxact_order() {
    install();
    CacheInvalidateCatalog(1259).unwrap();
    CacheInvalidateRelcacheByRelid(100).unwrap();
    CommandEndInvalidationMessages().unwrap();
    CacheInvalidateCatalog(1247).unwrap();
    CacheInvalidateRelcacheByRelid(200).unwrap();

    let ctx = mcx::MemoryContext::new("test");
    let (msgs, init_inval) = xactGetCommittedInvalidationMessages(ctx.mcx()).unwrap();
    assert!(!init_inval);
    let kinds: Vec<(bool, Oid)> = msgs
        .iter()
        .map(|m| match m {
            SharedInvalidationMessage::Catalog(c) => (true, c.catId),
            SharedInvalidationMessage::Relcache(rc) => (false, rc.relId),
            other => panic!("unexpected {other:?}"),
        })
        .collect();
    // Prior:Cat, Current:Cat, Prior:Rel, Current:Rel.
    assert_eq!(kinds, vec![(true, 1259), (true, 1247), (false, 100), (false, 200)]);
    AtEOXact_Inval(false).unwrap();
}

#[test]
fn syscache_callback_chain_runs_in_registration_order() {
    install();
    fn cb_a(arg: Datum, cacheid: i32, hash: u32) {
        log(format!("A({},{cacheid},{hash})", arg.as_u32()));
    }
    fn cb_b(_arg: Datum, cacheid: i32, hash: u32) {
        log(format!("B({cacheid},{hash})"));
    }
    fn cb_other(_arg: Datum, _cacheid: i32, _hash: u32) {
        log("OTHER".to_string());
    }
    CacheRegisterSyscacheCallback(11, cb_a, Datum::from_u32(7)).unwrap();
    CacheRegisterSyscacheCallback(12, cb_other, Datum::null()).unwrap();
    CacheRegisterSyscacheCallback(11, cb_b, Datum::null()).unwrap();

    clear_events();
    CallSyscacheCallbacks(11, 42).unwrap();
    assert_eq!(events(), vec!["A(7,11,42)".to_string(), "B(11,42)".to_string()]);

    assert!(CallSyscacheCallbacks(99, 0).is_err());
    assert_eq!(
        CacheRegisterSyscacheCallback(-1, cb_a, Datum::null()).unwrap_err().level(),
        types_error::FATAL
    );
}

#[test]
fn callback_may_reenter_registration_mid_dispatch() {
    install();
    thread_local! {
        static REENTERED: Cell<bool> = const { Cell::new(false) };
    }
    fn cb_late(_arg: Datum, _cacheid: i32, _hash: u32) {
        log("late".to_string());
    }
    fn cb_registering(_arg: Datum, cacheid: i32, _hash: u32) {
        log("registering".to_string());
        if !REENTERED.replace(true) {
            CacheRegisterSyscacheCallback(cacheid, cb_late, Datum::null()).unwrap();
        }
    }
    CacheRegisterSyscacheCallback(20, cb_registering, Datum::null()).unwrap();

    clear_events();
    // C re-reads ccitem->link after each invocation, so the callback
    // registered mid-dispatch runs in the same walk.
    CallSyscacheCallbacks(20, 1).unwrap();
    assert_eq!(events(), vec!["registering".to_string(), "late".to_string()]);
}

#[test]
fn invalidate_system_caches_extended_fires_everything() {
    install();
    fn sys_cb(_arg: Datum, cacheid: i32, hash: u32) {
        log(format!("sys({cacheid},{hash})"));
    }
    fn rel_cb(_arg: Datum, relid: Oid) {
        log(format!("rel({relid})"));
    }
    fn relsync_cb(_arg: Datum, relid: Oid) {
        log(format!("relsync({relid})"));
    }
    CacheRegisterSyscacheCallback(30, sys_cb, Datum::null()).unwrap();
    CacheRegisterRelcacheCallback(rel_cb, Datum::null()).unwrap();
    CacheRegisterRelSyncCallback(relsync_cb, Datum::null()).unwrap();

    clear_events();
    InvalidateSystemCachesExtended(false).unwrap();
    let ev = events();
    assert!(ev.contains(&"snapshot".to_string()));
    assert!(ev.contains(&"reset_catalog_caches(false)".to_string()));
    assert!(ev.contains(&"relcache_invalidate(false)".to_string()));
    assert!(ev.contains(&"sys(30,0)".to_string()));
    assert!(ev.contains(&format!("rel({InvalidOid})")));
    assert!(ev.contains(&format!("relsync({InvalidOid})")));
}

#[test]
fn smgr_message_roundtrips_the_packed_proc_number() {
    install();
    let rlocator = types_storage::RelFileLocatorBackend {
        locator: types_storage::RelFileLocator { spcOid: 1663, dbOid: 5, relNumber: 16384 },
        backend: 0x12345,
    };
    CacheInvalidateSmgr(rlocator).unwrap();
    let flat = sent_flat();
    assert_eq!(flat.len(), 1);

    clear_events();
    LocalExecuteInvalidationMessage(&flat[0]).unwrap();
    assert_eq!(events(), vec![format!("smgr(16384,{})", 0x12345)]);
}

#[test]
fn accept_drains_queue_filtering_foreign_databases() {
    install();
    PENDING.with(|p| {
        p.borrow_mut().extend([
            SharedInvalidationMessage::Catcache(types_storage::SharedInvalCatcacheMsg {
                id: 4,
                dbId: 5,
                hashValue: 99,
            }),
            SharedInvalidationMessage::Catcache(types_storage::SharedInvalCatcacheMsg {
                id: 4,
                dbId: 6,
                hashValue: 77,
            }),
        ]);
    });
    clear_events();
    AcceptInvalidationMessages().unwrap();
    assert_eq!(
        events(),
        vec!["snapshot".to_string(), "syscache_invalidate(4,99)".to_string()]
    );
}

#[test]
fn relsync_and_snapshot_dedup_in_rel_subgroup() {
    install();
    CacheInvalidateRelSync(10).unwrap();
    CacheInvalidateRelSync(10).unwrap();
    CacheInvalidateRelSyncAll().unwrap();
    AtEOXact_Inval(true).unwrap();
    let flat = sent_flat();
    assert_eq!(flat.len(), 2);
    assert!(matches!(flat[0], SharedInvalidationMessage::RelSync(m) if m.relid == 10));
    assert!(matches!(flat[1], SharedInvalidationMessage::RelSync(m) if m.relid == InvalidOid));
}
