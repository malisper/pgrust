//! Unit tests for two formerly-fenced gin lanes:
//! - ginNewScanKey's version-0 (pre-9.1) index gate: whole-index/null scans
//!   raise C's clean 0A000 with the REINDEX hint (ginscan.c:480) instead of
//!   panicking.
//! - array_ops element compares over a non-hardwired element type dispatch
//!   through the typcache-resolved btree comparator via fmgr
//!   (initGinState's lookup_type_cache fallback, ginutil.c:147).
use std::cell::Cell;
use std::rc::Rc;
use std::sync::Once;

use ::datum::Datum;
use ::gin_vocab::{
    GinColState, GinElemCmp, GinMetaPageData, GinOpclass, GinScanOpaqueData, GinState,
    GIN_CURRENT_VERSION, GIN_MAX_KEY_COLS,
};
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_core::{Oid, BLCKSZ, GIN_AM_OID, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT};
use ::types_error::{ERRCODE_FEATURE_NOT_SUPPORTED, PgResult};
use ::types_fmgr::FmgrInfo;
use ::types_rel::{
    FormData_pg_class, LockInfoData, LockRelId, Relation, RelationData, LOCKMODE, RELKIND_INDEX,
    REPLICA_IDENTITY_DEFAULT,
};
use ::types_tuple::{CompactAttribute, FormData_pg_attribute, NameData, TupleDescData};

const IDX: Oid = 26000;

// One-page fake buffer world holding the gin metapage.
#[repr(C, align(8))]
struct FakePage([u8; BLCKSZ]);

thread_local! {
    static META_PAGE: core::ptr::NonNull<FakePage> =
        core::ptr::NonNull::from(Box::leak(Box::new(FakePage([0u8; BLCKSZ]))));
}

fn set_meta_version(version: i32) {
    let meta = GinMetaPageData {
        head: 0,
        tail: 0,
        tailFreeSize: 0,
        nPendingPages: 0,
        nPendingHeapTuples: 0,
        nTotalPages: 1,
        nEntryPages: 1,
        nDataPages: 0,
        nEntries: 0,
        ginVersion: version,
    };
    META_PAGE.with(|p| {
        // SAFETY: leaked page, single-threaded test access.
        let bytes = unsafe { &mut (*p.as_ptr()).0 };
        crate::write_meta_to(bytes, &meta);
    });
}

fn install() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        bufmgr_seams::read_buffer::set(|_rel, blkno| {
            assert_eq!(blkno, 0, "only the metapage is read in these tests");
            Ok(1)
        });
        bufmgr_seams::lock_buffer::set(|_buf, _mode| Ok(()));
        bufmgr_seams::release_buffer::set(|_buf| Ok(()));
        bufmgr_seams::buffer_get_page::set(|buf| {
            assert_eq!(buf, 1);
            META_PAGE.with(|p| p.cast::<u8>())
        });
        // array_ops Fmgr compare arm: btint8cmp stand-in resolved by oid.
        fmgr_seams::fmgr_info::set(|oid| Ok(FmgrInfo::new(fake_int8_cmp, oid, 2, true, false)));
    });
}

fn fake_int8_cmp(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut ::types_fmgr::FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let a = fcinfo.arg(0).as_i64();
    let b = fcinfo.arg(1).as_i64();
    Ok(Datum::from_i32(if a < b {
        -1
    } else {
        (a > b) as i32
    }))
}

fn dummy_col() -> GinColState {
    GinColState {
        opclass: GinOpclass::ArrayOps,
        elem_cmp: GinElemCmp::Int4,
        support_collation: 0,
        can_partial_match: false,
        key_byval: true,
        key_len: 4,
    }
}

fn gin_state() -> GinState {
    GinState {
        natts: 1,
        one_col: true,
        cols: [dummy_col(); GIN_MAX_KEY_COLS],
    }
}

fn index_rel(mcx: Mcx<'_>) -> Relation<'_> {
    let mut relname = NameData::default();
    relname.namestrcpy("old_gin_idx");
    let att = FormData_pg_attribute {
        attnum: 1,
        atttypid: 23,
        attlen: 4,
        attbyval: true,
        attalign: ::types_tuple::TYPALIGN_INT,
        ..Default::default()
    };
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    compact.push(CompactAttribute::populate_from(&att));
    attrs.push(att);
    let rd_att = Rc::new(TupleDescData {
        natts: 1,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    });
    let data = RelationData {
        rd_locator: Default::default(),
        rd_smgr: Default::default(),
        rd_id: IDX,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: LockInfoData {
            lockRelId: LockRelId { relId: IDX, dbId: 5 },
        },
        rd_rel: {
            let mut c = FormData_pg_class {
                relname,
                relnamespace: 99,
                reltype: 0,
                relowner: 10,
                relam: GIN_AM_OID,
                relfilenode: IDX,
                reltablespace: 0,
                relpages: 0,
                reltuples: -1.0,
                relallvisible: 0,
                reltoastrelid: 0,
                relhasindex: false,
                relisshared: false,
                relpersistence: RELPERSISTENCE_PERMANENT,
                relkind: RELKIND_INDEX,
                relhassubclass: false,
                relrowsecurity: false,
                relispopulated: true,
                relreplident: REPLICA_IDENTITY_DEFAULT,
                relispartition: false,
                relfrozenxid: 3,
                relminmxid: 1,
            };
            c.relname.namestrcpy("old_gin_idx");
            c
        },
        rd_att,
        rd_index: None,
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
        rd_support: PgVec::new_in(mcx),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
        rd_trigdesc: Default::default(),
        rd_hastriggers: false,
        rd_hasrules: false,
    };
    Relation::open(data, Some(noop_close))
}

fn noop_close(_oid: Oid, _mode: LOCKMODE) -> PgResult<()> {
    Ok(())
}

#[test]
fn version0_whole_index_scan_raises_clean_reindex_error() {
    install();
    let cx = MemoryContext::new("t");
    let rel = index_rel(cx.mcx());

    // ginVersion 0 (pre-9.1): the keyless (whole-index) scan errors cleanly.
    set_meta_version(0);
    let mut so = GinScanOpaqueData {
        ginstate: Some(gin_state()),
        work: None,
        isVoidRes: false,
    };
    let err = crate::scan::ginNewScanKey(&rel, &[], &mut so).unwrap_err();
    assert_eq!(
        err.message(),
        "old GIN indexes do not support whole-index scans nor searches for nulls"
    );
    assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
    assert_eq!(err.hint(), Some("To fix this, do REINDEX INDEX \"old_gin_idx\"."));

    // A current-version index takes the same path without error.
    set_meta_version(GIN_CURRENT_VERSION);
    let mut so = GinScanOpaqueData {
        ginstate: Some(gin_state()),
        work: None,
        isVoidRes: false,
    };
    crate::scan::ginNewScanKey(&rel, &[], &mut so).unwrap();
}

#[test]
fn array_ops_fmgr_elem_compare_dispatches_btree_cmp_proc() {
    install();
    // A non-hardwired element type (e.g. int8's btint8cmp, proc oid 351
    // stand-in): compare() routes through fmgr with the stored cmp proc.
    let col = GinColState {
        opclass: GinOpclass::ArrayOps,
        elem_cmp: GinElemCmp::Fmgr(842),
        support_collation: 0,
        can_partial_match: false,
        key_byval: true,
        key_len: 8,
    };
    let cmp = |x: i64, y: i64| {
        crate::opclass::compare(&col, Datum::from_i64(x), Datum::from_i64(y))
    };
    assert_eq!(cmp(1, 2), -1);
    assert_eq!(cmp(2, 1), 1);
    assert_eq!(cmp(7, 7), 0);
    assert_eq!(cmp(-3, 4), -1);
}
