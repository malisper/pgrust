use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Once;

use mcx::{Mcx, MemoryContext, PgVec};
use rel_vocab::RangeVar;
use types_core::{InvalidOid, Oid, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP};
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_rel::{
    AccessShareLock, FormData_pg_class, LockInfoData, LockRelId, NoLock, RelationData,
    RowExclusiveLock, LOCKMODE, RELKIND_RELATION, REPLICA_IDENTITY_DEFAULT,
};
use types_tuple::{NameData, TupleDescData};

use crate::{relation_open, relation_openrv, relation_openrv_extended, try_relation_open};

const TBL: Oid = 16384;
const TMP: Oid = 16385;
const MISSING: Oid = 42;

thread_local! {
    static EVENTS: RefCell<Vec<(&'static str, Oid, LOCKMODE)>> = const { RefCell::new(Vec::new()) };
    static PGSTAT_RET: Cell<bool> = const { Cell::new(false) };
    static REGISTRY: RefCell<HashMap<Oid, Rc<RelationData<'static>>>> =
        RefCell::new(HashMap::new());
}

fn log(what: &'static str, oid: Oid, mode: LOCKMODE) {
    EVENTS.with_borrow_mut(|e| e.push((what, oid, mode)));
}

fn take_events() -> Vec<(&'static str, Oid, LOCKMODE)> {
    EVENTS.with_borrow_mut(std::mem::take)
}

fn make_entry(mcx: Mcx<'static>, oid: Oid, name: &str, relpersistence: u8) -> RelationData<'static> {
    let mut relname = NameData::default();
    relname.namestrcpy(name);
    RelationData { rd_locator: Default::default(), rd_smgr: Default::default(),
        rd_id: oid,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: LockInfoData {
            lockRelId: LockRelId { relId: oid, dbId: 5 },
        },
        rd_rel: FormData_pg_class {
            relname,
            relnamespace: 2200,
            reltype: 0,
            relowner: 10,
            relam: 2,
            relfilenode: oid,
            reltablespace: 0,
            relpages: 0,
            reltuples: -1.0,
            relallvisible: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relisshared: false,
            relpersistence,
            relkind: RELKIND_RELATION,
            relhassubclass: false,
            relrowsecurity: false,
            relispopulated: true,
            relreplident: REPLICA_IDENTITY_DEFAULT,
            relispartition: false,
            relfrozenxid: 3,
            relminmxid: 1,
        },
        rd_att: Rc::new(TupleDescData {
            natts: 0,
            tdtypeid: 0,
            tdtypmod: -1,
            tdrefcount: 1,
            constr: None,
            compact_attrs: PgVec::new_in(mcx),
            attrs: PgVec::new_in(mcx),
        }),
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

fn ensure_registry() {
    REGISTRY.with_borrow_mut(|reg| {
        if reg.is_empty() {
            let mcx: Mcx<'static> = Box::leak(Box::new(MemoryContext::new("relcache"))).mcx();
            reg.insert(TBL, Rc::new(make_entry(mcx, TBL, "tbl", RELPERSISTENCE_PERMANENT)));
            reg.insert(TMP, Rc::new(make_entry(mcx, TMP, "tmp", RELPERSISTENCE_TEMP)));
        }
    });
}

fn registry_strong_count(oid: Oid) -> usize {
    REGISTRY.with_borrow(|reg| Rc::strong_count(&reg[&oid]))
}

fn fake_lock(relid: Oid, lockmode: LOCKMODE) -> PgResult<()> {
    log("lock", relid, lockmode);
    Ok(())
}

fn fake_unlock(relid: Oid, lockmode: LOCKMODE) -> PgResult<()> {
    log("unlock", relid, lockmode);
    Ok(())
}

fn fake_get_relation(oid: Oid) -> PgResult<Option<Rc<RelationData<'static>>>> {
    ensure_registry();
    log("relcache", oid, NoLock);
    Ok(REGISTRY.with_borrow(|reg| reg.get(&oid).cloned()))
}

fn fake_exists(oid: Oid) -> PgResult<bool> {
    ensure_registry();
    log("sysprobe", oid, NoLock);
    Ok(REGISTRY.with_borrow(|reg| reg.contains_key(&oid)))
}

fn fake_inval() -> PgResult<()> {
    log("inval", InvalidOid, NoLock);
    Ok(())
}

fn fake_rv_get_relid(_: Mcx<'_>, rv: &RangeVar, lockmode: LOCKMODE, missing_ok: bool) -> PgResult<Oid> {
    log("rvlookup", InvalidOid, lockmode);
    match rv.relname {
        "tbl" => Ok(TBL),
        _ if missing_ok => Ok(InvalidOid),
        _ => Err(PgError::error(format!("relation \"{}\" does not exist", rv.relname)).into()),
    }
}

fn fake_pgstat_init(relid: Oid, _relkind: u8) -> bool {
    log("pgstat", relid, NoLock);
    PGSTAT_RET.get()
}

fn install() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        lmgr_seams::lock_relation_oid::set(fake_lock);
        lmgr_seams::unlock_relation_oid::set(fake_unlock);
        lmgr_seams::check_relation_locked_by_me::set(|_, _, _| true);
        miscinit_seams::is_bootstrap_processing_mode::set(|| false);
        relcache_seams::relation_id_get_relation::set(fake_get_relation);
        syscache_seams::search_syscache_exists_reloid::set(fake_exists);
        inval_seams::accept_invalidation_messages::set(fake_inval);
        namespace_seams::range_var_get_relid::set(fake_rv_get_relid);
        xact_seams::set_xact_accessed_temp_namespace::set(|| log("tempflag", InvalidOid, NoLock));
        pgstat_seams::pgstat_init_relation::set(fake_pgstat_init);
        crate::init_seams();
    });
    take_events();
}

fn rv(relname: &'static str) -> RangeVar<'static> {
    RangeVar {
        catalogname: None,
        schemaname: None,
        relname,
        inh: true,
        relpersistence: RELPERSISTENCE_PERMANENT,
        location: -1,
    }
}

#[test]
fn open_locks_before_relcache_and_close_unlocks() {
    install();
    let ctx = MemoryContext::new("t");
    let r = relation_open(ctx.mcx(), TBL, AccessShareLock).unwrap();
    assert_eq!(r.name(), "tbl");
    assert_eq!(
        take_events(),
        vec![
            ("lock", TBL, AccessShareLock),
            ("relcache", TBL, NoLock),
            ("pgstat", TBL, NoLock),
        ]
    );
    assert_eq!(registry_strong_count(TBL), 2);
    r.close(AccessShareLock).unwrap();
    assert_eq!(take_events(), vec![("unlock", TBL, AccessShareLock)]);
    assert_eq!(registry_strong_count(TBL), 1);
}

#[test]
fn open_missing_errors_and_leaves_lock_for_xact_cleanup() {
    install();
    let ctx = MemoryContext::new("t");
    let err = relation_open(ctx.mcx(), MISSING, AccessShareLock).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INTERNAL_ERROR);
    assert_eq!(err.message, "could not open relation with OID 42");
    let events = take_events();
    assert!(events.contains(&("lock", MISSING, AccessShareLock)));
    assert!(!events.iter().any(|e| e.0 == "unlock"));
}

#[test]
fn try_open_missing_releases_useless_lock() {
    install();
    let ctx = MemoryContext::new("t");
    assert!(try_relation_open(ctx.mcx(), MISSING, AccessShareLock).unwrap().is_none());
    assert_eq!(
        take_events(),
        vec![
            ("lock", MISSING, AccessShareLock),
            ("sysprobe", MISSING, NoLock),
            ("unlock", MISSING, AccessShareLock),
        ]
    );
}

#[test]
fn try_open_missing_nolock_skips_lock_traffic() {
    install();
    let ctx = MemoryContext::new("t");
    assert!(try_relation_open(ctx.mcx(), MISSING, NoLock).unwrap().is_none());
    assert_eq!(take_events(), vec![("sysprobe", MISSING, NoLock)]);
}

#[test]
fn try_open_existing_returns_handle() {
    install();
    let ctx = MemoryContext::new("t");
    let r = try_relation_open(ctx.mcx(), TBL, RowExclusiveLock).unwrap().unwrap();
    assert_eq!(r.rd_id, TBL);
    r.close(RowExclusiveLock).unwrap();
}

#[test]
fn openrv_accepts_inval_then_opens_with_nolock() {
    install();
    let ctx = MemoryContext::new("t");
    let r = relation_openrv(ctx.mcx(), &rv("tbl"), RowExclusiveLock).unwrap();
    let events = take_events();
    assert_eq!(events[0].0, "inval");
    assert_eq!(events[1], ("rvlookup", InvalidOid, RowExclusiveLock));
    // RangeVarGetRelid took the lock; the inner open must not lock again.
    assert!(!events.iter().any(|e| e.0 == "lock"));
    drop(r);
}

#[test]
fn openrv_nolock_skips_inval() {
    install();
    let ctx = MemoryContext::new("t");
    let r = relation_openrv(ctx.mcx(), &rv("tbl"), NoLock).unwrap();
    assert!(!take_events().iter().any(|e| e.0 == "inval"));
    drop(r);
}

#[test]
fn openrv_extended_missing_ok() {
    install();
    let ctx = MemoryContext::new("t");
    assert!(relation_openrv_extended(ctx.mcx(), &rv("gone"), AccessShareLock, true)
        .unwrap()
        .is_none());
    assert!(relation_openrv_extended(ctx.mcx(), &rv("gone"), AccessShareLock, false).is_err());
    let r = relation_openrv_extended(ctx.mcx(), &rv("tbl"), AccessShareLock, true)
        .unwrap()
        .unwrap();
    assert_eq!(r.rd_id, TBL);
    drop(r);
}

#[test]
fn temp_relation_sets_xact_flag() {
    install();
    let ctx = MemoryContext::new("t");
    let r = relation_open(ctx.mcx(), TMP, AccessShareLock).unwrap();
    assert!(take_events().contains(&("tempflag", InvalidOid, NoLock)));
    drop(r);
    let r = relation_open(ctx.mcx(), TBL, AccessShareLock).unwrap();
    assert!(!take_events().contains(&("tempflag", InvalidOid, NoLock)));
    drop(r);
}

#[test]
fn pgstat_enabled_bit_stored_on_handle() {
    install();
    let ctx = MemoryContext::new("t");
    PGSTAT_RET.set(true);
    let r = relation_open(ctx.mcx(), TBL, AccessShareLock).unwrap();
    assert!(r.pgstat_enabled.get());
    drop(r);
    PGSTAT_RET.set(false);
    let r = relation_open(ctx.mcx(), TBL, AccessShareLock).unwrap();
    assert!(!r.pgstat_enabled.get());
    drop(r);
}

#[test]
fn drop_is_abort_path_nolock_and_releases_pin() {
    install();
    let ctx = MemoryContext::new("t");
    let r = relation_open(ctx.mcx(), TBL, AccessShareLock).unwrap();
    take_events();
    assert_eq!(registry_strong_count(TBL), 2);
    drop(r);
    assert!(!take_events().iter().any(|e| e.0 == "unlock"));
    assert_eq!(registry_strong_count(TBL), 1);
}

#[test]
fn seams_installed_by_init() {
    install();
    assert!(relation_seams::relation_open::is_installed());
    assert!(relation_seams::try_relation_open::is_installed());
    assert!(relation_seams::relation_openrv::is_installed());
    assert!(relation_seams::relation_openrv_extended::is_installed());
    let ctx = MemoryContext::new("t");
    let r = relation_seams::relation_open::call(ctx.mcx(), TBL, AccessShareLock).unwrap();
    assert_eq!(r.name(), "tbl");
    r.close(AccessShareLock).unwrap();
}
