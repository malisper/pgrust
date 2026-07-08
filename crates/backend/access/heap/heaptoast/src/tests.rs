use super::*;
use ::datum::Datum;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_core::{
    BlockNumber, Buffer, Oid, BLCKSZ, BTREE_AM_OID, INVALID_PROC_NUMBER,
    RELPERSISTENCE_PERMANENT,
};
use ::types_fmgr::FmgrInfo;
use ::types_nbtree::{
    BTMetaPageData, BTPageOpaqueData, BTP_LEAF, BTP_META, BTP_ROOT, BTREE_MAGIC, BTREE_VERSION,
    P_NONE,
};
use ::types_rel::{
    FormData_pg_class, FormData_pg_index, LockInfoData, LockRelId, Relation, RelationData,
    LOCKMODE, RELKIND_INDEX, RELKIND_RELATION, RELKIND_TOASTVALUE, REPLICA_IDENTITY_DEFAULT,
};
use ::types_snapshot::{SnapshotData, SnapshotType};
use ::types_storage::bufpage::PageRef;
use ::types_tuple::itemptr::ItemPointerData;
use ::types_tuple::varatt::{set_varsize_4b_word, VARHDRSZ};
use ::types_tuple::{
    heap_deform_tuple, CompactAttribute, FormData_pg_attribute, HeapTupleData, NameData,
    TupleDescData, TYPALIGN_INT, TYPSTORAGE_EXTENDED, TYPSTORAGE_EXTERNAL, TYPSTORAGE_PLAIN,
    HEAP_XMAX_INVALID,
};
use std::cell::Cell;
use std::collections::HashMap;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, Once};

const MAIN_REL: Oid = 19999;
const TOAST_REL: Oid = 20000;
const TOAST_IDX: Oid = 20001;
const MAIN3_REL: Oid = 21000;
const FAKE_XID: u32 = 100;
const CID: u32 = 7;

// ---------------- fake bufmgr (heapam tests model) ----------------

#[repr(align(8))]
struct TestPage([u8; BLCKSZ]);

struct Fake {
    tables: HashMap<Oid, Vec<Buffer>>,
    pages: Vec<usize>,
    pins: Vec<i32>,
}

static FAKE: Mutex<Option<Fake>> = Mutex::new(None);
static SERIAL: Mutex<()> = Mutex::new(());
static NEXT_VALUEID: AtomicUsize = AtomicUsize::new(16400);

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn with_fake<R>(f: impl FnOnce(&mut Fake) -> R) -> R {
    let mut g = FAKE.lock().unwrap_or_else(|e| e.into_inner());
    f(g.get_or_insert_with(|| Fake {
        tables: HashMap::new(),
        pages: Vec::new(),
        pins: Vec::new(),
    }))
}

fn reset_tables() {
    with_fake(|f| {
        f.tables.insert(MAIN_REL, Vec::new());
        f.tables.insert(TOAST_REL, Vec::new());
        f.tables.insert(TOAST_IDX, Vec::new());
        f.tables.insert(MAIN3_REL, Vec::new());
    });
}

fn install_seams() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        crate::init_seams();

        bufmgr_seams::read_buffer::set(|rel, block| {
            with_fake(|f| {
                let buf = f.tables[&rel.rd_id][block as usize];
                f.pins[(buf - 1) as usize] += 1;
                Ok(buf)
            })
        });
        bufmgr_seams::read_buffer_strategy::set(|rel, block, _s| {
            bufmgr_seams::read_buffer::call(rel, block)
        });
        bufmgr_seams::release_buffer::set(|buf| {
            with_fake(|f| f.pins[(buf - 1) as usize] -= 1);
            Ok(())
        });
        bufmgr_seams::release_and_read_buffer::set(|buf, rel, blkno| {
            if buf != ::types_core::InvalidBuffer {
                if with_fake(|f| f.tables[&rel.rd_id].get(blkno as usize) == Some(&buf)) {
                    return Ok(buf);
                }
                bufmgr_seams::release_buffer::call(buf)?;
            }
            bufmgr_seams::read_buffer::call(rel, blkno)
        });
        bufmgr_seams::incr_buffer_ref_count::set(|buf| {
            with_fake(|f| f.pins[(buf - 1) as usize] += 1)
        });
        bufmgr_seams::lock_buffer::set(|_buf, _mode| Ok(()));
        bufmgr_seams::buffer_get_block_number::set(|buf| {
            with_fake(|f| {
                for pages in f.tables.values() {
                    if let Some(i) = pages.iter().position(|b| *b == buf) {
                        return i as BlockNumber;
                    }
                }
                panic!("unknown buffer {buf}")
            })
        });
        bufmgr_seams::buffer_get_page::set(|buf| {
            let addr = with_fake(|f| f.pages[(buf - 1) as usize]);
            NonNull::new(addr as *mut u8).unwrap()
        });
        bufmgr_seams::mark_buffer_dirty::set(|_| Ok(()));
        bufmgr_seams::mark_buffer_dirty_hint::set(|_, _| Ok(()));
        bufmgr_seams::buffer_get_lsn_atomic::set(|_| 0x1234);
        bufmgr_seams::get_access_strategy::set(|_| None);
        bufmgr_seams::free_access_strategy::set(|_| {});
        bufmgr_seams::relation_get_number_of_blocks_in_fork::set(|rel, _fork| {
            with_fake(|f| Ok(f.tables[&rel.rd_id].len() as BlockNumber))
        });
        bufmgr_seams::extend_buffered_rel_by::set(|rel, _fork, _strategy, _flags, extend_by| {
            assert_eq!(extend_by, 1);
            let page = Box::new(TestPage([0u8; BLCKSZ]));
            let rd_id = rel.rd_id;
            Ok(with_fake(|f| {
                let addr = Box::leak(page).0.as_mut_ptr() as usize;
                f.pages.push(addr);
                f.pins.push(1);
                let buf = f.pages.len() as Buffer;
                f.tables.get_mut(&rd_id).unwrap().push(buf);
                (buf, 1)
            }))
        });

        xact_seams::get_current_transaction_id::set(|| Ok(FAKE_XID));
        xact_seams::get_current_command_id::set(|_| Ok(CID));
        xact_seams::is_in_parallel_mode::set(|| false);
        xact_seams::get_current_transaction_nest_level::set(|| 1);
        xact_seams::transaction_id_is_current_transaction_id::set(|xid| xid == FAKE_XID);

        heapam_visibility_seams::heap_tuple_satisfies_visibility::set(|htup, _snap, _buf| {
            Ok((htup.t_data().t_infomask & HEAP_XMAX_INVALID) != 0)
        });
        heapam_visibility_seams::heap_tuple_satisfies_mvcc_page::set(|htup, _snap, _buf, _memo| {
            Ok((htup.t_data().t_infomask & HEAP_XMAX_INVALID) != 0)
        });
        heapam_visibility_seams::heap_tuple_is_surely_dead::set(|_, _| Ok(false));
        heapam_visibility_seams::heap_tuple_header_is_only_locked::set(|_| Ok(false));
        heapam_visibility_seams::heap_tuple_satisfies_update::set(|htup, _cid, _buf| {
            Ok(
                if (htup.t_data().t_infomask & HEAP_XMAX_INVALID) != 0 {
                    ::tableam_vocab::TM_Result::TM_Ok
                } else {
                    ::tableam_vocab::TM_Result::TM_SelfModified
                },
            )
        });
        heapam_visibility_seams::heap_tuple_set_hint_bits::set(|hdr, _buf, infomask, _xid| {
            hdr.t_infomask |= infomask;
            Ok(())
        });

        combocid_seams::heap_tuple_header_adjust_cmax::set(|_hdr, cid| Ok((cid, false)));
        combocid_seams::heap_tuple_header_get_cmax::set(|hdr| hdr.raw_command_id());
        multixact_seams::multi_xact_id_set_oldest_member::set(|| Ok(()));
        predicate_seams::check_for_serializable_conflict_in::set(|_, _, _| Ok(()));
        predicate_seams::check_table_for_serializable_conflict_in::set(|_rel| Ok(()));
        predicate_seams::transfer_predicate_locks_to_heap_relation::set(|_rel| Ok(()));
        predicate_seams::check_for_serializable_conflict_out_needed::set(|_, _| Ok(false));
        predicate_seams::predicate_lock_relation::set(|_, _| Ok(()));
        predicate_seams::predicate_lock_tid::set(|_, _, _, _| Ok(()));
        predicate_seams::predicate_lock_page::set(|_, _, _| Ok(()));
        pruneheap_seams::heap_page_prune_opt::set(|_, _| Ok(()));
        procarray_seams::global_vis_test_for::set(|_| ::types_core::GlobalVisStateHandle::new(0));
        freespace_seams::get_page_with_free_space::set(|_, _| Ok(::types_core::InvalidBlockNumber));
        freespace_seams::record_and_get_page_with_free_space::set(|_, _, _, _| {
            Ok(::types_core::InvalidBlockNumber)
        });
        freespace_seams::record_page_with_free_space::set(|_, _, _| Ok(()));
        xloginsert_seams::xlog_insert_record::set(|_, _, _, _, _| Ok(0x1000));
        miscinit_seams::is_bootstrap_processing_mode::set(|| false);
        catalog_seams::is_catalog_relation::set(|_| false);
        catalog_seams::get_new_oid_with_index::set(|_mcx, _rel, _idx, _col| {
            Ok(NEXT_VALUEID.fetch_add(1, Ordering::Relaxed) as Oid)
        });
        relcache_seams::relation_get_index_list::set(|mcx, relid| {
            assert_eq!(relid, TOAST_REL);
            let mut v = ::mcx::vec_with_capacity_in(mcx, 1)?;
            v.push(TOAST_IDX);
            Ok(v)
        });
        relation_seams::relation_open::set(|mcx, relid, _lockmode| Ok(fixture_rel(mcx, relid)));

    });
    test_boot::boot_wal("heaptoast");
    ensure_active_snapshot();
}

// snapmgr state is thread-local: arm an active snapshot on every test thread
// so get_toast_snapshot's C precondition holds.
fn ensure_active_snapshot() {
    std::thread_local! {
        static ARMED: Cell<bool> = const { Cell::new(false) };
    }
    ARMED.with(|armed| {
        if !armed.get() {
            let leaked: &'static MemoryContext =
                Box::leak(Box::new(MemoryContext::new("snap")));
            snapmgr::PushActiveSnapshot(&Rc::new(SnapshotData::sentinel(
                leaked.mcx(),
                SnapshotType::SNAPSHOT_MVCC,
            )))
            .unwrap();
            armed.set(true);
        }
    });
}

// ---------------- relation fixtures ----------------

fn att(attnum: i16, attlen: i16, attbyval: bool, attstorage: i8) -> FormData_pg_attribute {
    FormData_pg_attribute {
        attnum,
        attlen,
        attbyval,
        attalign: TYPALIGN_INT,
        attstorage,
        ..Default::default()
    }
}

fn tupdesc<'mcx>(mcx: Mcx<'mcx>, atts: &[FormData_pg_attribute]) -> Rc<TupleDescData<'mcx>> {
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    for a in atts {
        compact.push(CompactAttribute::populate_from(a));
        attrs.push(a.clone());
    }
    Rc::new(TupleDescData {
        natts: atts.len() as i32,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

fn noop_close(_oid: Oid, _mode: LOCKMODE) -> ::types_error::PgResult<()> {
    Ok(())
}

fn base_class(oid: Oid, relkind: u8, relam: Oid, reltoastrelid: Oid) -> FormData_pg_class {
    let mut relname = NameData::default();
    relname.namestrcpy("t");
    FormData_pg_class {
        relname,
        relnamespace: 99,
        reltype: 0,
        relowner: 10,
        relam,
        relfilenode: oid,
        reltablespace: 0,
        relpages: 0,
        reltuples: -1.0,
        relallvisible: 0,
        reltoastrelid,
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
    }
}

fn rel_from<'mcx>(
    mcx: Mcx<'mcx>,
    oid: Oid,
    rd_rel: FormData_pg_class,
    rd_att: Rc<TupleDescData<'mcx>>,
    rd_index: Option<FormData_pg_index<'mcx>>,
    opcintype: &[Oid],
) -> Relation<'mcx> {
    let vec_of = |vals: &[Oid]| {
        let mut v = PgVec::new_in(mcx);
        v.extend_from_slice(vals);
        v
    };
    let mut indoption = PgVec::new_in(mcx);
    indoption.extend_from_slice(&vec![0i16; opcintype.len()]);
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
        rd_rel,
        rd_att,
        rd_index,
        rd_opcintype: vec_of(opcintype),
        rd_opfamily: vec_of(&vec![0; opcintype.len()]),
        rd_indoption: indoption,
        rd_indcollation: vec_of(&vec![0; opcintype.len()]),
        rd_options: None,
        pgstat_enabled: Cell::new(false),
        pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(), rd_amcache_gin: Default::default(), rd_amcache_spgist: Default::default(),
        rd_support: vec_of(&vec![0; opcintype.len()]),
        rd_supportinfo: Default::default(),
        rd_opcoptions: Default::default(),
        rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false, rd_hasrules: false,
    };
    Relation::open(data, Some(noop_close))
}

fn fixture_rel<'mcx>(mcx: Mcx<'mcx>, oid: Oid) -> Relation<'mcx> {
    match oid {
        MAIN_REL => rel_from(
            mcx,
            oid,
            base_class(oid, RELKIND_RELATION, ::tableam_vocab::HEAP_TABLE_AM_OID, TOAST_REL),
            tupdesc(mcx, &[att(1, -1, false, TYPSTORAGE_EXTENDED)]),
            None,
            &[],
        ),
        MAIN3_REL => rel_from(
            mcx,
            oid,
            base_class(oid, RELKIND_RELATION, ::tableam_vocab::HEAP_TABLE_AM_OID, TOAST_REL),
            tupdesc(
                mcx,
                &[
                    att(1, -1, false, TYPSTORAGE_EXTENDED),
                    att(2, -1, false, TYPSTORAGE_EXTERNAL),
                    att(3, -1, false, TYPSTORAGE_EXTENDED),
                ],
            ),
            None,
            &[],
        ),
        TOAST_REL => rel_from(
            mcx,
            oid,
            base_class(oid, RELKIND_TOASTVALUE, ::tableam_vocab::HEAP_TABLE_AM_OID, 0),
            tupdesc(
                mcx,
                &[
                    att(1, 4, true, TYPSTORAGE_PLAIN),
                    att(2, 4, true, TYPSTORAGE_PLAIN),
                    att(3, -1, false, TYPSTORAGE_PLAIN),
                ],
            ),
            None,
            &[],
        ),
        // indisready=false: the production toast-index insert lane is loud at
        // indexam btinsert (phase 2); C's indisready gate skips it here and
        // the tests build the leaf image below instead.
        TOAST_IDX => {
            let mut indkey = PgVec::new_in(mcx);
            indkey.extend_from_slice(&[1i16, 2]);
            let idx = rel_from(
                mcx,
                oid,
                base_class(oid, RELKIND_INDEX, BTREE_AM_OID, 0),
                tupdesc(
                    mcx,
                    &[att(1, 4, true, TYPSTORAGE_PLAIN), att(2, 4, true, TYPSTORAGE_PLAIN)],
                ),
                Some(FormData_pg_index {
                    indexrelid: oid,
                    indrelid: TOAST_REL,
                    indnatts: 2,
                    indnkeyatts: 2,
                    indisunique: true,
                    indnullsnotdistinct: false,
                    indisprimary: false,
                    indisexclusion: false,
                    indimmediate: true,
                    indisvalid: true,
                    indisready: false,
                    indkey,
                    has_indpred: false,
        indexprs_src: None,
        indpred_src: None,
                }),
                &[26, 23],
            );
            idx.rd_supportinfo.borrow_mut().extend([
                Some(FmgrInfo::new(nbt_compare::builtins::fc_btoidcmp, 356, 2, true, false)),
                Some(FmgrInfo::new(nbt_compare::builtins::fc_btint4cmp, 351, 2, true, false)),
            ]);
            idx
        }
        _ => panic!("fixture_rel: unknown oid {oid}"),
    }
}

// ---------------- toast-index page builder ----------------

fn put_u16(p: &mut TestPage, off: usize, v: u16) {
    p.0[off..off + 2].copy_from_slice(&v.to_ne_bytes());
}

fn new_btpage(flags: u16, level: u32) -> Box<TestPage> {
    let mut p = Box::new(TestPage([0u8; BLCKSZ]));
    let special = BLCKSZ - core::mem::size_of::<BTPageOpaqueData>();
    put_u16(&mut p, 12, 24);
    put_u16(&mut p, 14, special as u16);
    put_u16(&mut p, 16, special as u16);
    let opaque = BTPageOpaqueData {
        btpo_prev: P_NONE,
        btpo_next: P_NONE,
        btpo_level: level,
        btpo_flags: flags,
        btpo_cycleid: 0,
    };
    // SAFETY: in-bounds aligned special-area write on an owned page.
    unsafe { p.0.as_mut_ptr().add(special).cast::<BTPageOpaqueData>().write(opaque) };
    p
}

fn bt_meta() -> Box<TestPage> {
    let mut p = new_btpage(BTP_META, 0);
    let metad = BTMetaPageData {
        btm_magic: BTREE_MAGIC,
        btm_version: BTREE_VERSION,
        btm_root: 1,
        btm_level: 0,
        btm_fastroot: 1,
        btm_fastlevel: 0,
        btm_last_cleanup_num_delpages: 0,
        btm_last_cleanup_num_heap_tuples: -1.0,
        btm_allequalimage: true,
    };
    // SAFETY: metapage payload write on an owned page.
    unsafe { p.0.as_mut_ptr().add(24).cast::<BTMetaPageData>().write(metad) };
    p
}

fn bt_add_tuple(p: &mut TestPage, tid: ItemPointerData, valueid: Oid, seq: i32) {
    let itupsz = 16usize;
    let pd_lower = u16::from_ne_bytes([p.0[12], p.0[13]]) as usize;
    let pd_upper = u16::from_ne_bytes([p.0[14], p.0[15]]) as usize;
    let off = pd_upper - itupsz;
    // SAFETY: owned page bytes; ItemPointerData is a 6B POD.
    unsafe { p.0.as_mut_ptr().add(off).cast::<ItemPointerData>().write_unaligned(tid) };
    p.0[off + 6..off + 8].copy_from_slice(&(itupsz as u16).to_ne_bytes());
    p.0[off + 8..off + 12].copy_from_slice(&valueid.to_ne_bytes());
    p.0[off + 12..off + 16].copy_from_slice(&seq.to_ne_bytes());
    let mut iid = ::types_storage::bufpage::ItemIdData::new(0, 0, 0);
    iid.set_normal(off as u16, itupsz as u16);
    // SAFETY: line-pointer slot in the owned page.
    unsafe {
        p.0.as_mut_ptr()
            .add(pd_lower)
            .cast::<::types_storage::bufpage::ItemIdData>()
            .write(iid)
    };
    put_u16(p, 12, (pd_lower + 4) as u16);
    put_u16(p, 14, off as u16);
}

// live (xmax-invalid) chunk rows as (valueid, chunk_seq, tid), index order
fn toast_heap_entries(mcx: Mcx<'_>) -> Vec<(Oid, i32, ItemPointerData)> {
    let toastrel = fixture_rel(mcx, TOAST_REL);
    let desc = toastrel.rd_att.clone();
    let bufs = with_fake(|f| f.tables[&TOAST_REL].clone());
    let mut out = Vec::new();
    for (blk, buf) in bufs.iter().enumerate() {
        let addr = with_fake(|f| f.pages[(buf - 1) as usize]);
        // SAFETY: leaked test page, always live.
        let page = unsafe { PageRef::from_raw(NonNull::new(addr as *mut u8).unwrap()) };
        for off in 1..=page.max_offset_number() {
            let id = page.item_id(off);
            if !id.is_normal() {
                continue;
            }
            let (ptr, len) = page.item_raw(id);
            let tid = ItemPointerData::new(blk as u32, off);
            // SAFETY: in-page tuple image.
            let tup =
                unsafe { HeapTupleData::from_raw_parts(ptr.cast_mut(), len, tid, TOAST_REL) };
            if (tup.t_data().t_infomask & HEAP_XMAX_INVALID) == 0 {
                continue;
            }
            let mut isnull = false;
            // SAFETY: chunk tuples match the toast descriptor.
            let valueid =
                unsafe { ::types_tuple::heap_getattr(&tup, 1, &desc, &mut isnull) }.as_oid();
            let seq = unsafe { ::types_tuple::heap_getattr(&tup, 2, &desc, &mut isnull) }
                .as_usize() as i32;
            out.push((valueid, seq, tid));
        }
    }
    out.sort_by_key(|(v, s, _)| (*v, *s));
    out
}

fn rebuild_toast_index(mcx: Mcx<'_>) -> usize {
    let entries = toast_heap_entries(mcx);
    let mut leaf = new_btpage(BTP_LEAF | BTP_ROOT, 0);
    for (valueid, seq, tid) in &entries {
        bt_add_tuple(&mut leaf, *tid, *valueid, *seq);
    }
    let n = entries.len();
    with_fake(|f| {
        let mut bufs = Vec::new();
        for p in [bt_meta(), leaf] {
            let addr = Box::leak(p).0.as_mut_ptr() as usize;
            f.pages.push(addr);
            f.pins.push(0);
            bufs.push(f.pages.len() as Buffer);
        }
        f.tables.insert(TOAST_IDX, bufs);
    });
    n
}

// ---------------- value builders / helpers ----------------

fn text_value<'mcx>(mcx: Mcx<'mcx>, payload: &[u8]) -> PgVec<'mcx, u8> {
    let mut v = ::mcx::vec_with_capacity_in(mcx, VARHDRSZ + payload.len()).unwrap();
    ::mcx::vec_append_bytes(
        &mut v,
        &set_varsize_4b_word((VARHDRSZ + payload.len()) as u32).to_ne_bytes(),
    )
    .unwrap();
    ::mcx::vec_append_bytes(&mut v, payload).unwrap();
    v
}

fn prng_bytes(n: usize) -> Vec<u8> {
    prng_bytes_seeded(n, 0x9e3779b97f4a7c15)
}

fn prng_bytes_seeded(n: usize, seed: u64) -> Vec<u8> {
    let mut x = seed;
    let mut out = Vec::with_capacity(n + 8);
    while out.len() < n {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        out.extend_from_slice(&x.to_le_bytes());
    }
    out.truncate(n);
    out
}

fn insert_row(mcx: Mcx<'_>, rel: &Relation<'_>, payloads: &[&[u8]]) -> ItemPointerData {
    let values: Vec<Datum> = payloads
        .iter()
        .map(|p| Datum::from_usize(text_value(mcx, p).leak().as_ptr() as usize))
        .collect();
    let isnull = vec![false; payloads.len()];
    let mut tup = heaptuple::heap_form_tuple(mcx, &rel.rd_att, &values, &isnull).unwrap();
    heapam::dml::heap_insert(rel, tup.as_tuple_mut(), CID, 0, None).unwrap();
    tup.as_tuple().t_self
}

fn stored_attrs(mcx: Mcx<'_>, rel: &Relation<'_>, tid: ItemPointerData) -> Vec<Vec<u8>> {
    let buf = with_fake(|f| f.tables[&rel.rd_id][::types_tuple::itemptr::ItemPointerGetBlockNumberNoCheck(&tid) as usize]);
    let addr = with_fake(|f| f.pages[(buf - 1) as usize]);
    // SAFETY: leaked test page, always live.
    let page = unsafe { PageRef::from_raw(NonNull::new(addr as *mut u8).unwrap()) };
    let id = page.item_id(tid.ip_posid);
    let (ptr, len) = page.item_raw(id);
    // SAFETY: in-page tuple image.
    let tup = unsafe { HeapTupleData::from_raw_parts(ptr.cast_mut(), len, tid, rel.rd_id) };
    let natts = rel.rd_att.natts as usize;
    let mut values = ::mcx::vec_from_elem_in(mcx, Datum::null(), natts);
    let mut isnull = ::mcx::vec_from_elem_in(mcx, false, natts);
    heap_deform_tuple(&tup, &rel.rd_att, &mut values, &mut isnull);
    values
        .iter()
        // SAFETY: non-null deformed varlena datums point into the live page.
        .map(|d| unsafe { crate::helper::va_slice(*d) }.to_vec())
        .collect()
}

// ---------------- tests ----------------

#[test]
fn constants_match_c() {
    assert_eq!(TOAST_TUPLE_THRESHOLD, 2032);
    assert_eq!(TOAST_TUPLE_TARGET, 2032);
    assert_eq!(TOAST_TUPLE_TARGET_MAIN, 8160);
    assert_eq!(TOAST_MAX_CHUNK_SIZE, 1996);
    assert_eq!(toastdesc::TOAST_POINTER_SIZE, 18);
}

#[test]
fn compress_datum_thresholds() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();

    // below PGLZ_strategy_default->min_input_size (32): C skips compression
    let tiny = text_value(mcx, &[b'a'; 8]);
    assert!(toast_compress_datum(mcx, &tiny, 0).unwrap().is_none());

    // incompressible random data: pglz gives up
    let rand = text_value(mcx, &prng_bytes(1000));
    assert!(toast_compress_datum(mcx, &rand, 0).unwrap().is_none());

    // compressible: tcinfo carries rawsize + pglz method id, and round-trips
    let comp = text_value(mcx, &[b'a'; 1000]);
    let out = toast_compress_datum(mcx, &comp, 0).unwrap().unwrap();
    assert!(out.len() < 1000 - 2);
    assert_eq!(toastdesc::toast_compress_extsize(&out).unwrap(), 1000);
    assert_eq!(
        toastdesc::toast_compress_method(&out).unwrap(),
        toastdesc::TOAST_PGLZ_COMPRESSION_ID
    );
    let back = detoast::toast_decompress_datum(mcx, &out).unwrap();
    assert_eq!(&back[VARHDRSZ..], &[b'a'; 1000][..]);
}

#[test]
fn small_tuple_does_not_toast() {
    install_seams();
    let _s = serial();
    reset_tables();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let rel = fixture_rel(mcx, MAIN_REL);

    let payload = prng_bytes(1900);
    let tid = insert_row(mcx, &rel, &[&payload]);

    let attrs = stored_attrs(mcx, &rel, tid);
    assert_eq!(&attrs[0][VARHDRSZ..], &payload[..]);
    assert!(with_fake(|f| f.tables[&TOAST_REL].is_empty()));
}

#[test]
fn compressible_value_stays_inline_compressed() {
    install_seams();
    let _s = serial();
    reset_tables();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let rel = fixture_rel(mcx, MAIN_REL);

    let payload = vec![b'x'; 8000];
    let tid = insert_row(mcx, &rel, &[&payload]);

    let attrs = stored_attrs(mcx, &rel, tid);
    let stored = &attrs[0];
    assert_eq!(stored[0] & 0x03, 0x02, "expected inline-compressed varlena");
    assert!(with_fake(|f| f.tables[&TOAST_REL].is_empty()));

    let back = detoast::detoast_attr(mcx, stored).unwrap();
    assert_eq!(&back[VARHDRSZ..], &payload[..]);
}

#[test]
fn oversized_incompressible_value_round_trips_externally() {
    install_seams();
    let _s = serial();
    reset_tables();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let rel = fixture_rel(mcx, MAIN_REL);

    let payload = prng_bytes(6000);
    let tid = insert_row(mcx, &rel, &[&payload]);

    let attrs = stored_attrs(mcx, &rel, tid);
    let pointer = &attrs[0];
    assert_eq!(pointer.len(), toastdesc::TOAST_POINTER_SIZE);
    assert!(toastdesc::varatt_is_external_ondisk(pointer));
    let tp = toastdesc::VarattExternal::from_image(pointer).unwrap();
    assert_eq!(tp.va_rawsize as usize, 6000 + VARHDRSZ);
    assert_eq!(tp.extsize() as usize, 6000);
    assert!(!tp.is_compressed());
    assert_eq!(tp.va_toastrelid, TOAST_REL);

    // chunk shape: ceil(6000 / 1996) = 4 chunks in sequence order
    let entries = toast_heap_entries(mcx);
    assert_eq!(entries.len(), 4);
    assert_eq!(
        entries.iter().map(|(_, s, _)| *s).collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );

    assert_eq!(rebuild_toast_index(mcx), 4);

    // write-then-read: the committed detoast path over the real index+heap
    let back = detoast::detoast_attr(mcx, pointer).unwrap();
    assert_eq!(back.len(), 6000 + VARHDRSZ);
    assert_eq!(&back[VARHDRSZ..], &payload[..]);

    // slice lane (uncompressed external): crosses the chunk-0/1 boundary
    let slice = detoast::detoast_attr_slice(mcx, pointer, 1990, 20).unwrap();
    assert_eq!(&slice[VARHDRSZ..], &payload[1990..2010]);
}

#[test]
fn oversized_compressible_value_round_trips_externally_compressed() {
    install_seams();
    let _s = serial();
    reset_tables();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let rel = fixture_rel(mcx, MAIN_REL);

    // repeats within pglz's first_success_by window (1024) so compression
    // engages, but the compressed image still exceeds the 2032 target:
    // three distinct 900B blocks, each doubled -> ~2.7KB compressed
    let mut payload = Vec::new();
    for i in 0..3u64 {
        let block = prng_bytes_seeded(900, 0x1234_5678 + i);
        payload.extend_from_slice(&block);
        payload.extend_from_slice(&block);
    }
    let tid = insert_row(mcx, &rel, &[&payload]);

    let attrs = stored_attrs(mcx, &rel, tid);
    let pointer = &attrs[0];
    assert!(toastdesc::varatt_is_external_ondisk(pointer));
    let tp = toastdesc::VarattExternal::from_image(pointer).unwrap();
    assert_eq!(tp.va_rawsize as usize, payload.len() + VARHDRSZ);
    assert!(tp.is_compressed());
    assert_eq!(tp.compress_method(), toastdesc::TOAST_PGLZ_COMPRESSION_ID);
    assert!((tp.extsize() as usize) < payload.len());

    rebuild_toast_index(mcx);
    let back = detoast::detoast_attr(mcx, pointer).unwrap();
    assert_eq!(back.len(), payload.len() + VARHDRSZ);
    assert_eq!(&back[VARHDRSZ..], &payload[..]);
}

#[test]
fn four_pass_ordering_multi_column() {
    install_seams();
    let _s = serial();
    reset_tables();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let rel = fixture_rel(mcx, MAIN3_REL);

    // c1 EXTENDED compressible 3000B -> pass 1 compresses it inline;
    // c2 EXTERNAL incompressible 2500B -> INCOMPRESSIBLE, then externalized;
    // c3 EXTENDED small 200B -> untouched inline.
    let c1 = vec![b'y'; 3000];
    let c2 = prng_bytes(2500);
    let c3 = prng_bytes(200);
    let tid = insert_row(mcx, &rel, &[&c1, &c2, &c3]);

    let attrs = stored_attrs(mcx, &rel, tid);
    assert_eq!(attrs[0][0] & 0x03, 0x02, "c1 inline-compressed");
    assert!(
        toastdesc::varatt_is_external_ondisk(&attrs[1]),
        "c2 externalized"
    );
    let tp2 = toastdesc::VarattExternal::from_image(&attrs[1]).unwrap();
    assert!(!tp2.is_compressed(), "EXTERNAL storage never compresses");
    assert_eq!(&attrs[2][VARHDRSZ..], &c3[..], "c3 left inline unchanged");

    rebuild_toast_index(mcx);
    assert_eq!(
        &detoast::detoast_attr(mcx, &attrs[0]).unwrap()[VARHDRSZ..],
        &c1[..]
    );
    assert_eq!(
        &detoast::detoast_attr(mcx, &attrs[1]).unwrap()[VARHDRSZ..],
        &c2[..]
    );
}

#[test]
fn heap_delete_cascades_into_toast_chunks() {
    install_seams();
    let _s = serial();
    reset_tables();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let rel = fixture_rel(mcx, MAIN_REL);

    let payload = prng_bytes(5000);
    let tid = insert_row(mcx, &rel, &[&payload]);
    assert_eq!(toast_heap_entries(mcx).len(), 3);
    rebuild_toast_index(mcx);

    let mut tmfd = ::tableam_vocab::TM_FailureData::default();
    let r = heapam::dml::heap_delete(&rel, &tid, CID, None, true, &mut tmfd, false).unwrap();
    assert_eq!(r, ::tableam_vocab::TM_Result::TM_Ok);

    // every chunk got simple_heap_delete'd (xmax stamped -> not "live")
    assert_eq!(toast_heap_entries(mcx).len(), 0);
}
