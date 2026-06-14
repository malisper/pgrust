//! Unit tests for the `relation.c` port.
//!
//! The genuine externals (relcache / lock manager / syscache / namespace /
//! invalidation / pgstat / xact) are installed as per-owner seam
//! implementations. A seam slot is a process-wide captureless `fn` pointer, so
//! the fixtures keep their scripted state and a call log in module-level
//! statics. Tests serialize on `TEST_LOCK` and reset scripted state per test;
//! the slots are installed once via a `Once`.
//!
//! These pin the load-bearing C parity properties: lock-then-open ordering, the
//! useless-lock release in `try_relation_open`, the `RangeVar` invalidation
//! handling, the `missing_ok` short-circuit, the `could not open relation with
//! OID %u` error, and the relation_close (RelationClose then unlock) ordering.

use super::*;

use core::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::string::{String, ToString};
use std::sync::{Mutex, Once};
use std::vec::Vec;

use mcx::{MemoryContext, PgString};
use types_core::primitive::InvalidOid;
use types_storage::lock::AccessShareLock;
use types_tuple::access::RangeVar;
use types_tuple::heaptuple::TupleDescData;

static CALLS: Mutex<Vec<String>> = Mutex::new(Vec::new());
/// Whether `relation_id_get_relation` returns `Some` (the relcache descriptor)
/// or `None` (the C NULL `Relation`).
static REL_FOUND: AtomicBool = AtomicBool::new(true);
/// What `search_syscache_exists_reloid` reports.
static EXISTS: AtomicBool = AtomicBool::new(true);
/// What `range_var_get_relid` returns.
static RELID: AtomicU32 = AtomicU32::new(42);
/// Whether the opened relation is a temp relation (`relpersistence == 't'`).
static USES_LOCAL_BUFFERS: AtomicBool = AtomicBool::new(false);

static TEST_LOCK: Mutex<()> = Mutex::new(());
static INSTALL: Once = Once::new();

fn record(s: impl Into<String>) {
    CALLS.lock().unwrap().push(s.into());
}

fn calls() -> Vec<String> {
    CALLS.lock().unwrap().clone()
}

fn build_reldata(mcx: Mcx<'_>, relid: Oid) -> RelationData<'_> {
    let persistence = if USES_LOCAL_BUFFERS.load(Ordering::SeqCst) {
        b't'
    } else {
        b'p'
    };
    let td = TupleDescData {
        natts: 0,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: 1,
        constr: None,
        compact_attrs: mcx::PgVec::new_in(mcx),
        attrs: mcx::PgVec::new_in(mcx),
    };
    RelationData {
        rd_id: relid,
        rd_locator: types_storage::RelFileLocator {
            spcOid: 0,
            dbOid: 0,
            relNumber: 0,
        },
        rd_backend: types_core::primitive::INVALID_PROC_NUMBER,
        rd_rel: types_rel::FormData_pg_class {
            relname: PgString::from_str_in("t", mcx).unwrap(),
            relnamespace: 0,
            relowner: 0,
            relrowsecurity: false,
            relpages: 0,
            reltuples: 0.0,
            relallvisible: 0,
            reltoastrelid: 0,
            reltablespace: 0,
            relfilenode: 0,
            relisshared: false,
            relhasindex: false,
            relhassubclass: false,
            relpersistence: persistence,
            relkind: b'r',
            relam: 0,
            relispopulated: true,
            relreplident: b'd',
            relispartition: false,
            relfrozenxid: 0,
        },
        rd_att: mcx::alloc_in(mcx, td).unwrap(),
        rd_options: None,
        rd_index: None,
        rd_opcintype: mcx::PgVec::new_in(mcx),
    }
}

fn setup() {
    INSTALL.call_once(|| {
        backend_storage_lmgr_lmgr_seams::lock_relation_oid::set(|relid, lockmode| {
            record(std::format!("lock_relation_oid({relid},{lockmode})"));
            Ok(backend_storage_lmgr_lmgr_seams::LockGuard::relation(relid, lockmode))
        });
        backend_storage_lmgr_lmgr_seams::unlock_relation_oid::set(|relid, lockmode| {
            record(std::format!("unlock_relation_oid({relid},{lockmode})"));
            Ok(())
        });
        backend_storage_lmgr_lmgr_seams::check_relation_locked_by_me::set(
            |_relid, _lockmode, _orstronger| {
                record("check_relation_locked_by_me");
                true
            },
        );
        backend_utils_cache_relcache_seams::relation_id_get_relation::set(|mcx, relid| {
            record(std::format!("relation_id_get_relation({relid})"));
            if REL_FOUND.load(Ordering::SeqCst) {
                Ok(Some(build_reldata(mcx, relid)))
            } else {
                Ok(None)
            }
        });
        backend_utils_cache_relcache_seams::relation_id_get_relation_shared::set(|relid| {
            record(std::format!("relation_id_get_relation_shared({relid})"));
            if REL_FOUND.load(Ordering::SeqCst) {
                let mut entry = types_relcache_entry::RelationData::default();
                entry.rd_id = relid;
                Ok(Some(std::rc::Rc::new(std::cell::RefCell::new(entry))))
            } else {
                Ok(None)
            }
        });
        backend_utils_cache_relcache_seams::relation_close::set(|relid| {
            record(std::format!("relation_close({relid})"));
            Ok(())
        });
        backend_utils_cache_syscache_seams::search_syscache_exists_reloid::set(|relid| {
            record(std::format!("search_syscache_exists_reloid({relid})"));
            Ok(EXISTS.load(Ordering::SeqCst))
        });
        backend_catalog_namespace_seams::range_var_get_relid::set(
            |_mcx, _relation, lockmode, missing_ok| {
                record(std::format!("range_var_get_relid({lockmode},{missing_ok})"));
                Ok(RELID.load(Ordering::SeqCst))
            },
        );
        backend_utils_cache_inval_seams::accept_invalidation_messages::set(|| {
            record("accept_invalidation_messages");
            Ok(())
        });
        backend_utils_init_miscinit_seams::is_bootstrap_processing_mode::set(|| {
            record("is_bootstrap_processing_mode");
            false
        });
        backend_utils_activity_pgstat_seams::pgstat_init_relation::set(|relid| {
            record(std::format!("pgstat_init_relation({relid})"));
            Ok(())
        });
        backend_access_transam_xact_seams::set_xact_accessed_temp_namespace::set(|| {
            record("set_xact_accessed_temp_namespace");
        });
    });

    CALLS.lock().unwrap().clear();
    REL_FOUND.store(true, Ordering::SeqCst);
    EXISTS.store(true, Ordering::SeqCst);
    RELID.store(42, Ordering::SeqCst);
    USES_LOCAL_BUFFERS.store(false, Ordering::SeqCst);
}

#[test]
fn relation_open_locks_before_opening() {
    let _g = TEST_LOCK.lock().unwrap();
    setup();
    let ctx = MemoryContext::new("test");
    let r = relation_open(ctx.mcx(), 100, AccessShareLock).unwrap();
    assert_eq!(r.rd_id, 100);
    let c = calls();
    // Lock acquired strictly before the relcache load.
    let i_lock = c.iter().position(|s| s == "lock_relation_oid(100,1)").unwrap();
    let i_open = c
        .iter()
        .position(|s| s == "relation_id_get_relation(100)")
        .unwrap();
    let i_stat = c
        .iter()
        .position(|s| s == "pgstat_init_relation(100)")
        .unwrap();
    assert!(i_lock < i_open && i_open < i_stat);
    r.close(AccessShareLock).unwrap();
}

#[test]
fn relation_open_invalid_relation_errors() {
    let _g = TEST_LOCK.lock().unwrap();
    setup();
    REL_FOUND.store(false, Ordering::SeqCst);
    let ctx = MemoryContext::new("test");
    let err = relation_open(ctx.mcx(), 100, NoLock).unwrap_err();
    assert!(err
        .message()
        .contains("could not open relation with OID 100"));
}

#[test]
fn relation_open_temp_relation_sets_xact_flag() {
    let _g = TEST_LOCK.lock().unwrap();
    setup();
    USES_LOCAL_BUFFERS.store(true, Ordering::SeqCst);
    let ctx = MemoryContext::new("test");
    let r = relation_open(ctx.mcx(), 100, AccessShareLock).unwrap();
    let c = calls();
    let i_flag = c
        .iter()
        .position(|s| s == "set_xact_accessed_temp_namespace")
        .unwrap();
    let i_pgstat = c
        .iter()
        .position(|s| s == "pgstat_init_relation(100)")
        .unwrap();
    assert!(i_flag < i_pgstat);
    r.close(AccessShareLock).unwrap();
}

#[test]
fn try_relation_open_releases_useless_lock() {
    let _g = TEST_LOCK.lock().unwrap();
    setup();
    EXISTS.store(false, Ordering::SeqCst);
    let ctx = MemoryContext::new("test");
    let r = try_relation_open(ctx.mcx(), 100, AccessShareLock).unwrap();
    assert!(r.is_none());
    assert_eq!(
        calls(),
        std::vec![
            "lock_relation_oid(100,1)".to_string(),
            "search_syscache_exists_reloid(100)".to_string(),
            "unlock_relation_oid(100,1)".to_string(),
        ]
    );
}

#[test]
fn try_relation_open_exists_loads_relcache() {
    let _g = TEST_LOCK.lock().unwrap();
    setup();
    let ctx = MemoryContext::new("test");
    let r = try_relation_open(ctx.mcx(), 100, AccessShareLock).unwrap();
    assert!(r.is_some());
    let c = calls();
    let i_exists = c
        .iter()
        .position(|s| s == "search_syscache_exists_reloid(100)")
        .unwrap();
    let i_open = c
        .iter()
        .position(|s| s == "relation_id_get_relation(100)")
        .unwrap();
    assert!(i_exists < i_open);
    r.unwrap().close(AccessShareLock).unwrap();
}

#[test]
fn relation_openrv_accepts_inval_then_opens_with_nolock() {
    let _g = TEST_LOCK.lock().unwrap();
    setup();
    let ctx = MemoryContext::new("test");
    let rv = RangeVar::default();
    let r = relation_openrv(ctx.mcx(), &rv, AccessShareLock).unwrap();
    let c = calls();
    let i_inval = c
        .iter()
        .position(|s| s == "accept_invalidation_messages")
        .unwrap();
    // RangeVarGetRelid takes the real lockmode + missing_ok=false.
    let i_rvg = c
        .iter()
        .position(|s| s == "range_var_get_relid(1,false)")
        .unwrap();
    // relation_open is then called with NoLock (lock already held), opening
    // the relid RangeVarGetRelid resolved (42).
    let i_open = c
        .iter()
        .position(|s| s == "relation_id_get_relation(42)")
        .unwrap();
    assert!(i_inval < i_rvg && i_rvg < i_open);
    r.close(NoLock).unwrap();
}

#[test]
fn relation_openrv_nolock_skips_inval() {
    let _g = TEST_LOCK.lock().unwrap();
    setup();
    let ctx = MemoryContext::new("test");
    let rv = RangeVar::default();
    let r = relation_openrv(ctx.mcx(), &rv, NoLock).unwrap();
    let c = calls();
    assert!(!c.iter().any(|s| s == "accept_invalidation_messages"));
    assert!(c.iter().any(|s| s == "range_var_get_relid(0,false)"));
    r.close(NoLock).unwrap();
}

#[test]
fn relation_openrv_extended_missing_returns_none() {
    let _g = TEST_LOCK.lock().unwrap();
    setup();
    RELID.store(InvalidOid, Ordering::SeqCst);
    let ctx = MemoryContext::new("test");
    let rv = RangeVar::default();
    let r = relation_openrv_extended(ctx.mcx(), &rv, AccessShareLock, true).unwrap();
    assert!(r.is_none());
    assert_eq!(
        calls(),
        std::vec![
            "accept_invalidation_messages".to_string(),
            "range_var_get_relid(1,true)".to_string(),
        ]
    );
}

#[test]
fn relation_close_closes_then_unlocks() {
    let _g = TEST_LOCK.lock().unwrap();
    setup();
    let ctx = MemoryContext::new("test");
    let r = relation_open(ctx.mcx(), 5, NoLock).unwrap();
    CALLS.lock().unwrap().clear();
    // The handle's close: RelationClose then, with a real lockmode, the
    // unlock (C relation_close ordering).
    r.close(AccessShareLock).unwrap();
    assert_eq!(
        calls(),
        std::vec![
            "relation_close(5)".to_string(),
            "unlock_relation_oid(5,1)".to_string(),
        ]
    );
}

#[test]
fn relation_close_nolock_skips_unlock() {
    let _g = TEST_LOCK.lock().unwrap();
    setup();
    let ctx = MemoryContext::new("test");
    let r = relation_open(ctx.mcx(), 5, NoLock).unwrap();
    CALLS.lock().unwrap().clear();
    r.close(NoLock).unwrap();
    assert_eq!(calls(), std::vec!["relation_close(5)".to_string()]);
}
