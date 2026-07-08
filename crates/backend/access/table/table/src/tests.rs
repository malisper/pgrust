use std::cell::Cell;
use std::rc::Rc;
use std::sync::Once;

use mcx::{Mcx, MemoryContext, PgVec};
use rel_vocab::RangeVar;
use types_core::{Oid, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT};
use types_error::{PgError, PgResult, ERRCODE_WRONG_OBJECT_TYPE};
use types_rel::{
    AccessExclusiveLock, AccessShareLock, FormData_pg_class, LockInfoData, LockRelId, NoLock,
    Relation, RelationData, RowExclusiveLock, LOCKMODE, RELKIND_COMPOSITE_TYPE, RELKIND_INDEX,
    RELKIND_PARTITIONED_INDEX, RELKIND_RELATION, RELKIND_VIEW, REPLICA_IDENTITY_DEFAULT,
};
use types_tuple::{NameData, TupleDescData};

use crate::{table_close, table_open, table_openrv, table_openrv_extended, try_table_open};

const TBL: Oid = 1;
const IDX: Oid = 2;
const COMP: Oid = 3;
const PIDX: Oid = 4;
const VIEW: Oid = 5;

thread_local! {
    static LAST_CLOSED: Cell<(Oid, LOCKMODE)> = const { Cell::new((0, -1)) };
    static LAST_OPEN_LOCKMODE: Cell<LOCKMODE> = const { Cell::new(-1) };
}

fn entry(oid: Oid) -> Option<(&'static str, u8)> {
    match oid {
        TBL => Some(("tbl", RELKIND_RELATION)),
        IDX => Some(("idx", RELKIND_INDEX)),
        COMP => Some(("comp", RELKIND_COMPOSITE_TYPE)),
        PIDX => Some(("pidx", RELKIND_PARTITIONED_INDEX)),
        VIEW => Some(("vw", RELKIND_VIEW)),
        _ => None,
    }
}

fn record_close(oid: Oid, lockmode: LOCKMODE) -> PgResult<()> {
    LAST_CLOSED.set((oid, lockmode));
    Ok(())
}

fn make<'mcx>(mcx: Mcx<'mcx>, oid: Oid, name: &str, relkind: u8) -> Relation<'mcx> {
    let mut relname = NameData::default();
    relname.namestrcpy(name);
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
            relkind,
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
    };
    Relation::open(data, Some(record_close))
}

fn fake_relation_open(mcx: Mcx<'_>, oid: Oid, lockmode: LOCKMODE) -> PgResult<Relation<'_>> {
    LAST_OPEN_LOCKMODE.set(lockmode);
    match entry(oid) {
        Some((name, relkind)) => Ok(make(mcx, oid, name, relkind)),
        None => Err(PgError::error(format!("relation {oid} does not exist")).into()),
    }
}

fn fake_try_relation_open(
    mcx: Mcx<'_>,
    oid: Oid,
    lockmode: LOCKMODE,
) -> PgResult<Option<Relation<'_>>> {
    match entry(oid) {
        Some(_) => fake_relation_open(mcx, oid, lockmode).map(Some),
        None => Ok(None),
    }
}

fn by_name(relname: &str) -> Option<Oid> {
    [TBL, IDX, COMP, PIDX, VIEW]
        .into_iter()
        .find(|&oid| entry(oid).unwrap().0 == relname)
}

fn fake_relation_openrv<'mcx>(
    mcx: Mcx<'mcx>,
    rv: &RangeVar,
    lockmode: LOCKMODE,
) -> PgResult<Relation<'mcx>> {
    match by_name(rv.relname) {
        Some(oid) => fake_relation_open(mcx, oid, lockmode),
        None => Err(PgError::error(format!("relation \"{}\" does not exist", rv.relname)).into()),
    }
}

fn fake_relation_openrv_extended<'mcx>(
    mcx: Mcx<'mcx>,
    rv: &RangeVar,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> PgResult<Option<Relation<'mcx>>> {
    match by_name(rv.relname) {
        Some(oid) => fake_relation_open(mcx, oid, lockmode).map(Some),
        None if missing_ok => Ok(None),
        None => Err(PgError::error(format!("relation \"{}\" does not exist", rv.relname)).into()),
    }
}

fn fake_errdetail(relkind: u8) -> PgResult<String> {
    let kind = match relkind {
        RELKIND_INDEX => "indexes",
        RELKIND_PARTITIONED_INDEX => "partitioned indexes",
        RELKIND_COMPOSITE_TYPE => "composite types",
        _ => {
            return Err(
                PgError::error(format!("unrecognized relkind: '{}'", relkind as char)).into(),
            )
        }
    };
    Ok(format!("This operation is not supported for {kind}."))
}

fn install() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        relation_seams::relation_open::set(fake_relation_open);
        relation_seams::try_relation_open::set(fake_try_relation_open);
        relation_seams::relation_openrv::set(fake_relation_openrv);
        relation_seams::relation_openrv_extended::set(fake_relation_openrv_extended);
        pg_class_seams::errdetail_relkind_not_supported::set(fake_errdetail);
        crate::init_seams();
    });
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

fn assert_wrong_kind(err: Box<PgError>, relname: &str, detail_kind: &str) {
    assert_eq!(err.sqlstate(), ERRCODE_WRONG_OBJECT_TYPE);
    assert_eq!(err.message, format!("cannot open relation \"{relname}\""));
    assert_eq!(
        err.detail(),
        Some(format!("This operation is not supported for {detail_kind}.").as_str())
    );
}

#[test]
fn open_accepts_tables_and_views() {
    install();
    let ctx = MemoryContext::new("t");
    let r = table_open(ctx.mcx(), TBL, AccessShareLock).unwrap();
    assert_eq!(r.name(), "tbl");
    assert_eq!(LAST_OPEN_LOCKMODE.get(), AccessShareLock);
    // table.c does not reject views; that is the caller's storage check.
    assert!(table_open(ctx.mcx(), VIEW, AccessShareLock).is_ok());
}

#[test]
fn open_rejects_index_partitioned_index_composite() {
    install();
    let ctx = MemoryContext::new("t");
    for (oid, name, kind) in [
        (IDX, "idx", "indexes"),
        (PIDX, "pidx", "partitioned indexes"),
        (COMP, "comp", "composite types"),
    ] {
        let err = table_open(ctx.mcx(), oid, AccessShareLock).unwrap_err();
        assert_wrong_kind(err, name, kind);
    }
}

#[test]
fn try_open_missing_is_none_but_wrong_kind_still_errors() {
    install();
    let ctx = MemoryContext::new("t");
    assert!(try_table_open(ctx.mcx(), 999, AccessShareLock).unwrap().is_none());
    let r = try_table_open(ctx.mcx(), TBL, AccessShareLock).unwrap().unwrap();
    assert_eq!(r.rd_id, TBL);
    let err = try_table_open(ctx.mcx(), IDX, AccessShareLock).unwrap_err();
    assert_wrong_kind(err, "idx", "indexes");
}

#[test]
fn openrv_validates_kind() {
    install();
    let ctx = MemoryContext::new("t");
    assert_eq!(
        table_openrv(ctx.mcx(), &rv("tbl"), RowExclusiveLock).unwrap().name(),
        "tbl"
    );
    let err = table_openrv(ctx.mcx(), &rv("idx"), RowExclusiveLock).unwrap_err();
    assert_wrong_kind(err, "idx", "indexes");
    assert!(table_openrv(ctx.mcx(), &rv("gone"), RowExclusiveLock).is_err());
}

#[test]
fn openrv_extended_missing_ok() {
    install();
    let ctx = MemoryContext::new("t");
    assert!(table_openrv_extended(ctx.mcx(), &rv("gone"), AccessShareLock, true)
        .unwrap()
        .is_none());
    assert!(table_openrv_extended(ctx.mcx(), &rv("gone"), AccessShareLock, false).is_err());
    let err = table_openrv_extended(ctx.mcx(), &rv("comp"), AccessShareLock, true).unwrap_err();
    assert_wrong_kind(err, "comp", "composite types");
}

#[test]
fn close_routes_lockmode_through_armed_closer() {
    install();
    let ctx = MemoryContext::new("t");
    let r = table_open(ctx.mcx(), TBL, AccessExclusiveLock).unwrap();
    table_close(r, AccessExclusiveLock).unwrap();
    assert_eq!(LAST_CLOSED.get(), (TBL, AccessExclusiveLock));
    let r = table_open(ctx.mcx(), TBL, NoLock).unwrap();
    table_close(r, NoLock).unwrap();
    assert_eq!(LAST_CLOSED.get(), (TBL, NoLock));
}

#[test]
fn rejected_open_still_releases_via_drop() {
    install();
    let ctx = MemoryContext::new("t");
    LAST_CLOSED.set((0, -1));
    let _ = table_open(ctx.mcx(), IDX, AccessShareLock).unwrap_err();
    // The Err path drops the handle: C's abort-path close with NoLock.
    assert_eq!(LAST_CLOSED.get(), (IDX, NoLock));
}

#[test]
fn seams_installed_by_init() {
    install();
    assert!(table_seams::table_open::is_installed());
    assert!(table_seams::try_table_open::is_installed());
    let ctx = MemoryContext::new("t");
    let r = table_seams::table_open::call(ctx.mcx(), TBL, AccessShareLock).unwrap();
    assert_eq!(r.name(), "tbl");
}
