use std::cell::Cell;
use std::rc::Rc;

use ::mcx::{MemoryContext, PgVec};
use ::types_core::{INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT, BLCKSZ};
use ::types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED;
use ::types_nbtree::{BTMaxItemSize, BTPageOpaqueData, BTP_LEAF};
use ::types_rel::{
    FormData_pg_class, LockInfoData, LockRelId, Relation, RelationData, RELKIND_RELATION,
    REPLICA_IDENTITY_DEFAULT,
};
use ::types_storage::bufpage::PageMut;
use ::types_tuple::itemptr::ItemPointerData;
use ::types_tuple::{NameData, TupleDescData};

use super::*;

fn noop_close(_oid: ::types_core::Oid, _mode: ::types_rel::LOCKMODE) -> PgResult<()> {
    Ok(())
}

fn named_rel<'mcx>(mcx: ::mcx::Mcx<'mcx>, name: &str, oid: ::types_core::Oid) -> Relation<'mcx> {
    let mut relname = NameData::default();
    relname.namestrcpy(name);
    let td = TupleDescData {
        natts: 0,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: 1,
        constr: None,
        compact_attrs: PgVec::new_in(mcx),
        attrs: PgVec::new_in(mcx),
    };
    let data = RelationData {
        rd_locator: Default::default(),
        rd_smgr: Default::default(),
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
        rd_att: Rc::new(td),
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

#[repr(C, align(8))]
struct AlignedPage([u8; BLCKSZ]);

#[repr(C, align(8))]
struct Oversized([u8; 2712]);

#[test]
fn buildadd_oversized_detail_names_heap_not_index() {
    let cx = MemoryContext::new("d233");
    let mcx = cx.mcx();
    let heap = named_rel(mcx, "heap_tbl", 4999);
    let index = named_rel(mcx, "idx", 5000);

    let mut raw = AlignedPage([0u8; BLCKSZ]);
    let mut page = unsafe {
        PageMut::from_raw(core::ptr::NonNull::new_unchecked(raw.0.as_mut_ptr()))
    };
    nbtree::bt_pageinit(&mut page);
    write_opaque(
        &mut page,
        &BTPageOpaqueData {
            btpo_prev: P_NONE,
            btpo_next: P_NONE,
            btpo_level: 0,
            btpo_flags: BTP_LEAF,
            btpo_cycleid: 0,
        },
    );

    let mut img = Oversized([0u8; 2712]);
    assert!(2712 > BTMaxItemSize);
    unsafe {
        set_t_tid(img.0.as_mut_ptr(), ItemPointerData::new(42, 7));
        set_t_info(img.0.as_mut_ptr(), 2712);
    }

    // errtableconstraint's schema lookup (nbtutils.c:4245) goes through syscache.
    static NSP: std::sync::Once = std::sync::Once::new();
    NSP.call_once(|| syscache_seams::pg_namespace_nspname::set(|_| Ok(None)));
    let err = unsafe {
        nbtree::bt_check_third_page(mcx, &index, &heap, true, &page.as_ref(), img.0.as_ptr())
    }
    .unwrap_err();

    assert_eq!(err.sqlstate(), ERRCODE_PROGRAM_LIMIT_EXCEEDED);
    let detail = err.detail().expect("C errdetail names the heap");
    assert!(
        detail.contains("in relation \"heap_tbl\""),
        "detail={detail}"
    );
    assert!(
        !detail.contains("in relation \"idx\""),
        "buildadd used to pass the index as heap; detail={detail}"
    );
}
