use std::cell::Cell;
use std::rc::Rc;
use std::sync::Once;

use mcx::{Mcx, MemoryContext, PgString, PgVec};
use types_core::{InvalidOid, Oid, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT};
use types_error::PgResult;
use types_portal::TuplestoreHandle;
use types_rel::{
    FormData_pg_class, LockInfoData, LockRelId, Relation, RelationData, NoLock,
    RELKIND_RELATION, REPLICA_IDENTITY_DEFAULT, LOCKMODE,
};
use types_tuple::{NameData, TupleDescData};

use crate::*;

fn make_enr<'mcx>(mcx: Mcx<'mcx>, name: &str) -> EphemeralNamedRelationData<'mcx> {
    EphemeralNamedRelationData {
        md: EphemeralNamedRelationMetadataData {
            name: PgString::from_str_in(name, mcx).unwrap(),
            reliddesc: InvalidOid,
            tupdesc: None,
            enrtype: ENR_NAMED_TUPLESTORE,
            enrtuples: 0.0,
        },
        reldata: TuplestoreHandle::NULL,
    }
}

fn empty_desc<'mcx>(mcx: Mcx<'mcx>, natts: i32) -> TupleDescData<'mcx> {
    TupleDescData {
        natts,
        tdtypeid: InvalidOid,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: PgVec::new_in(mcx),
        attrs: PgVec::new_in(mcx),
    }
}

#[test]
fn create_is_empty() {
    let ctx = MemoryContext::new("test");
    let env = create_queryEnv(ctx.mcx());
    assert!(env.namedRelList.is_empty());
    assert_eq!(ctx.used(), 0);
}

#[test]
fn register_then_get() {
    let ctx = MemoryContext::new("test");
    let mut env = create_queryEnv(ctx.mcx());
    register_ENR(&mut env, make_enr(ctx.mcx(), "delta")).unwrap();
    assert!(ctx.used() > 0, "registered ENR is charged to the context");

    let found = get_ENR(&env, "delta").expect("registered ENR must be found");
    assert_eq!(found.md.name.as_str(), "delta");
    assert!(get_ENR(&env, "missing").is_none());
}

#[test]
fn get_visible_metadata_borrows_md() {
    let ctx = MemoryContext::new("test");
    let mut env = create_queryEnv(ctx.mcx());
    register_ENR(&mut env, make_enr(ctx.mcx(), "trans")).unwrap();

    let used_before = ctx.used();
    let md = get_visible_ENR_metadata(Some(&env), "trans").expect("must find metadata");
    assert_eq!(md.name.as_str(), "trans");
    assert_eq!(ctx.used(), used_before, "lookup must not allocate");

    assert!(get_visible_ENR_metadata(None, "trans").is_none());
    assert!(get_visible_ENR_metadata(Some(&env), "nope").is_none());
}

#[test]
fn unregister_removes_first_match_only() {
    let ctx = MemoryContext::new("test");
    let mut env = create_queryEnv(ctx.mcx());
    register_ENR(&mut env, make_enr(ctx.mcx(), "a")).unwrap();
    register_ENR(&mut env, make_enr(ctx.mcx(), "b")).unwrap();

    unregister_ENR(&mut env, "a");
    assert!(get_ENR(&env, "a").is_none());
    assert!(get_ENR(&env, "b").is_some());

    unregister_ENR(&mut env, "ghost");
    assert_eq!(env.namedRelList.len(), 1);
}

#[test]
fn get_enr_walk_order_preserved() {
    let ctx = MemoryContext::new("test");
    let mut env = create_queryEnv(ctx.mcx());
    for n in ["x", "y", "z"] {
        register_ENR(&mut env, make_enr(ctx.mcx(), n)).unwrap();
    }
    unregister_ENR(&mut env, "y");
    let got: Vec<&str> = env
        .namedRelList
        .iter()
        .map(|e| e.md.name.as_str())
        .collect();
    assert_eq!(got, ["x", "z"]);
}

#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "duplicate ephemeral named relation")]
fn duplicate_register_asserts() {
    let ctx = MemoryContext::new("test");
    let mut env = create_queryEnv(ctx.mcx());
    register_ENR(&mut env, make_enr(ctx.mcx(), "dup")).unwrap();
    let _ = register_ENR(&mut env, make_enr(ctx.mcx(), "dup"));
}

#[test]
fn tupdesc_inline_branch_shares_stored_descriptor() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let desc = Rc::new(empty_desc(mcx, 2));
    let md = EphemeralNamedRelationMetadataData {
        name: PgString::from_str_in("d", mcx).unwrap(),
        reliddesc: InvalidOid,
        tupdesc: Some(Rc::clone(&desc)),
        enrtype: ENR_NAMED_TUPLESTORE,
        enrtuples: 0.0,
    };

    let used_before = ctx.used();
    let out = ENRMetadataGetTupDesc(mcx, &md).unwrap();
    assert!(Rc::ptr_eq(&out, &desc), "C returns enrmd->tupdesc unchanged");
    assert_eq!(ctx.used(), used_before, "inline path must not allocate");
}

#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "exactly one of reliddesc/tupdesc")]
fn tupdesc_requires_exactly_one_source() {
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let md = EphemeralNamedRelationMetadataData {
        name: PgString::from_str_in("d", mcx).unwrap(),
        reliddesc: InvalidOid,
        tupdesc: None,
        enrtype: ENR_NAMED_TUPLESTORE,
        enrtuples: 0.0,
    };
    let _ = ENRMetadataGetTupDesc(mcx, &md);
}

const TBL: Oid = 7;
const TBL_NATTS: i32 = 3;

thread_local! {
    static LAST_CLOSED: Cell<(Oid, LOCKMODE)> = const { Cell::new((0, -1)) };
    static LAST_OPEN_LOCKMODE: Cell<LOCKMODE> = const { Cell::new(-1) };
}

fn record_close(oid: Oid, lockmode: LOCKMODE) -> PgResult<()> {
    LAST_CLOSED.set((oid, lockmode));
    Ok(())
}

fn fake_relation_open(mcx: Mcx<'_>, oid: Oid, lockmode: LOCKMODE) -> PgResult<Relation<'_>> {
    LAST_OPEN_LOCKMODE.set(lockmode);
    assert_eq!(oid, TBL);
    let mut relname = NameData::default();
    relname.namestrcpy("tbl");
    let data = RelationData { rd_locator: Default::default(), rd_smgr: Default::default(),
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
            relpersistence: RELPERSISTENCE_PERMANENT,
            relkind: RELKIND_RELATION,
            relhassubclass: false,
            relrowsecurity: false,
            relispopulated: true,
            relreplident: REPLICA_IDENTITY_DEFAULT,
            relispartition: false,
            relfrozenxid: 3,
            relminmxid: 1,
        },
        rd_att: Rc::new(empty_desc(mcx, TBL_NATTS)),
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
    };
    Ok(Relation::open(data, Some(record_close)))
}

fn install() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        relation_seams::relation_open::set(fake_relation_open);
    });
}

#[test]
fn tupdesc_catalog_branch_opens_and_closes_with_nolock() {
    install();
    let ctx = MemoryContext::new("test");
    let mcx = ctx.mcx();
    let md = EphemeralNamedRelationMetadataData {
        name: PgString::from_str_in("cat", mcx).unwrap(),
        reliddesc: TBL,
        tupdesc: None,
        enrtype: ENR_NAMED_TUPLESTORE,
        enrtuples: 0.0,
    };

    let out = ENRMetadataGetTupDesc(mcx, &md).unwrap();
    assert_eq!(out.natts, TBL_NATTS, "descriptor comes from relation rd_att");
    assert_eq!(LAST_OPEN_LOCKMODE.get(), NoLock);
    assert_eq!(LAST_CLOSED.get(), (TBL, NoLock));
}
