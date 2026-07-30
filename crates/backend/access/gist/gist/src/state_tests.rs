//! Unit tests for the initGISTstate not-ported guard (state.rs). The same
//! initGISTstate call sits on the CREATE INDEX (gistbuild), INSERT
//! (gistinsert amcache fill), and scan (gistbeginscan) paths, so exercising
//! it directly covers all three.
use crate::state::initGISTstate;
use ::mcx::{MemoryContext, PgVec};
use ::types_core::Oid;
use ::types_fmgr::FmgrInfo;
use ::types_gist::{GISTNProcs, GIST_EQUAL_PROC, GIST_PICKSPLIT_PROC};
use ::types_rel::{FormData_pg_class, FormData_pg_index, Relation, RelationData};
use ::types_tuple::{NameData, TupleDescData};
use std::cell::Cell;
use std::rc::Rc;

// tsquery_ops-shaped support layout at pgrust v0.2: consistent(1) and
// compress(3) registered, union(2)/penalty(5)/picksplit(6)/same(7) present in
// the catalog but resolving to the not-ported stub.
const GTSQUERY_CONSISTENT: Oid = 3701;
const GTSQUERY_UNION: Oid = 3698;
const GTSQUERY_COMPRESS: Oid = 3695;
const GTSQUERY_PENALTY: Oid = 3700;
const GTSQUERY_PICKSPLIT: Oid = 3697;
const GTSQUERY_SAME: Oid = 3699;
// A fully-ported layout stand-in (any oids the mock treats as ported).
const PORTED_BASE: Oid = 900_000;

fn install_mock_seams() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // Resolution always succeeds (as fmgr_info does for stub rows: the
        // carrier is filled, the failure is deferred to call time).
        fmgr_seams::fmgr_info::set(|oid| Ok(FmgrInfo::new(mock_body, oid, 1, false, false)));
        // The predicate: the four unported gtsquery procs report their names.
        fmgr_seams::fmgr_info_not_ported_name::set(|f| match f.fn_oid {
            GTSQUERY_UNION => Some("gtsquery_union"),
            GTSQUERY_PENALTY => Some("gtsquery_penalty"),
            GTSQUERY_PICKSPLIT => Some("gtsquery_picksplit"),
            GTSQUERY_SAME => Some("gtsquery_same"),
            _ => None,
        });
    });
}

fn mock_body(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut ::types_fmgr::FunctionCallInfoBaseData,
) -> ::types_error::PgResult<::datum::Datum> {
    Ok(::datum::Datum::from_i32(0))
}

fn index_rel<'mcx>(mcx: ::mcx::Mcx<'mcx>, support: [Oid; GISTNProcs]) -> Relation<'mcx> {
    let mut relname = NameData::default();
    relname.namestrcpy("t_q_idx");
    let td = TupleDescData {
        natts: 1,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: 1,
        constr: None,
        compact_attrs: PgVec::new_in(mcx),
        attrs: PgVec::new_in(mcx),
    };
    let mut rd_support = PgVec::new_in(mcx);
    rd_support.extend_from_slice(&support);
    let mut indkey = PgVec::new_in(mcx);
    indkey.push(1);
    let data = RelationData {
        rd_locator: Default::default(),
        rd_smgr: Default::default(),
        rd_id: 424242,
        rd_backend: ::types_core::INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: Default::default(),
        rd_rel: FormData_pg_class {
            relname,
            relnamespace: 2200,
            reltype: 0,
            relowner: 10,
            relam: 783, // GIST_AM_OID
            relfilenode: 424242,
            reltablespace: 0,
            relpages: 0,
            reltuples: 0.0,
            relallvisible: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relisshared: false,
            relpersistence: b'p',
            relkind: b'i',
            relhassubclass: false,
            relrowsecurity: false,
            relispopulated: true,
            relreplident: b'n',
            relispartition: false,
            relfrozenxid: 0,
            relminmxid: 0,
        },
        rd_att: Rc::new(td),
        rd_index: Some(FormData_pg_index {
            indexrelid: 424242,
            indrelid: 424241,
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
        }),
        rd_opcintype: PgVec::new_in(mcx),
        rd_opfamily: PgVec::new_in(mcx),
        rd_indoption: PgVec::new_in(mcx),
        rd_indcollation: PgVec::new_in(mcx),
        rd_options: None,
        pgstat_enabled: Cell::new(false),
        pgstat_link: Cell::new((0, core::ptr::null_mut())),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(),
        rd_amcache_gin: Default::default(),
        rd_amcache_spgist: Default::default(),
        rd_support,
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
        rd_trigdesc: Default::default(),
        rd_hastriggers: false,
        rd_hasrules: false,
    };
    Relation::open(data, None)
}

// rd_support layout: procnum 1..=GISTNProcs, 0 = absent.
fn tsquery_support() -> [Oid; GISTNProcs] {
    let mut s = [0; GISTNProcs];
    s[0] = GTSQUERY_CONSISTENT;
    s[1] = GTSQUERY_UNION;
    s[2] = GTSQUERY_COMPRESS;
    s[4] = GTSQUERY_PENALTY;
    s[5] = GTSQUERY_PICKSPLIT;
    s[6] = GTSQUERY_SAME;
    s
}

fn ported_support() -> [Oid; GISTNProcs] {
    let mut s = [0; GISTNProcs];
    for (i, slot) in s.iter_mut().enumerate().take(7) {
        *slot = PORTED_BASE + i as Oid;
    }
    s
}

#[test]
fn unported_mandatory_proc_fails_initgiststate() {
    install_mock_seams();
    let cx = MemoryContext::new("test");
    let rel = index_rel(cx.mcx(), tsquery_support());
    let err = match initGISTstate(cx.mcx(), &rel) {
        Ok(_) => panic!("expected initGISTstate to fail"),
        Err(e) => e,
    };
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
    // Union (procnum 2) is the first mandatory proc resolved after
    // consistent; the error names it, its oid, and the index attribute.
    let msg = err.message();
    assert!(msg.contains("gtsquery_union"), "{msg}");
    assert!(msg.contains("3698"), "{msg}");
    assert!(msg.contains("support function 2"), "{msg}");
    assert!(msg.contains("not yet implemented"), "{msg}");
}

#[test]
fn unported_picksplit_alone_fails_too() {
    install_mock_seams();
    // Only picksplit unported (union/penalty/same swapped for ported oids):
    // the split-time bomb specifically is caught at state init.
    let mut s = tsquery_support();
    s[1] = PORTED_BASE + 1;
    s[4] = PORTED_BASE + 4;
    s[6] = PORTED_BASE + 6;
    let cx = MemoryContext::new("test");
    let rel = index_rel(cx.mcx(), s);
    let err = match initGISTstate(cx.mcx(), &rel) {
        Ok(_) => panic!("expected initGISTstate to fail"),
        Err(e) => e,
    };
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
    let msg = err.message();
    assert!(msg.contains("gtsquery_picksplit"), "{msg}");
    assert_eq!(GIST_PICKSPLIT_PROC, 6);
    assert!(msg.contains("support function 6"), "{msg}");
}

#[test]
fn fully_ported_opclass_is_untouched() {
    install_mock_seams();
    let cx = MemoryContext::new("test");
    let rel = index_rel(cx.mcx(), ported_support());
    let st = match initGISTstate(cx.mcx(), &rel) {
        Ok(st) => st,
        Err(e) => panic!("ported opclass must init: {}", e.message()),
    };
    assert_eq!(st.consistentFn.len(), 1);
    assert_eq!(GIST_EQUAL_PROC, 7);
}
