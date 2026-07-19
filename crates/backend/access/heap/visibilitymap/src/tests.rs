use super::*;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_core::{Oid, INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT};
use ::types_rel::{FormData_pg_class, LockInfoData, LockRelId, RELKIND_RELATION};
use ::types_storage::{RelFileLocator, RelFileLocatorBackend};
use ::types_tuple::{CompactAttribute, FormData_pg_attribute, NameData, TupleDescData};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;
use std::sync::{Mutex, MutexGuard, Once};

#[repr(align(8))]
struct AlignedPage([u8; BLCKSZ]);

impl AlignedPage {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.0.as_mut_ptr()
    }
}

struct Fake {
    pages: Vec<Box<AlignedPage>>,
    pins: Vec<i32>,
    locks: Vec<i32>,
    dirty: Vec<u32>,
    lock_calls: usize,
    fork_exists: bool,
    fork_nblocks: BlockNumber,
    cached_nblocks: BlockNumber,
    exists_calls: usize,
    read_calls: usize,
    checksums: bool,
    wal: Vec<WalRec>,
    newpage_calls: Vec<Buffer>,
    next_lsn: XLogRecPtr,
}

struct WalRec {
    rmid: u8,
    info: u8,
    main: Vec<u8>,
    bufs: Vec<(u8, Buffer, u8)>,
}

static FAKE: Mutex<Option<Fake>> = Mutex::new(None);
// Seam-backed tests share the fake bufmgr; run them serially.
static SERIAL: Mutex<()> = Mutex::new(());
static INIT: Once = Once::new();

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn with_fake<R>(f: impl FnOnce(&mut Fake) -> R) -> R {
    let mut g = FAKE.lock().unwrap_or_else(|e| e.into_inner());
    f(g.as_mut().expect("fake not set up"))
}

fn setup(pages: Vec<Box<AlignedPage>>, fork_exists: bool) {
    let n = pages.len();
    setup_n(pages, fork_exists, n as BlockNumber);
}

fn setup_n(pages: Vec<Box<AlignedPage>>, fork_exists: bool, fork_nblocks: BlockNumber) {
    install_seams();
    let n = pages.len();
    *FAKE.lock().unwrap_or_else(|e| e.into_inner()) = Some(Fake {
        pins: vec![0; n],
        locks: vec![0; n],
        dirty: vec![0; n],
        lock_calls: 0,
        fork_exists,
        fork_nblocks,
        cached_nblocks: InvalidBlockNumber,
        exists_calls: 0,
        read_calls: 0,
        checksums: false,
        wal: Vec::new(),
        newpage_calls: Vec::new(),
        next_lsn: 0x0100_0000,
        pages,
    });
}

thread_local! {
    // Per-test switches for RelationIsAccessibleInLogicalDecoding's inputs.
    static LOGICAL_ACTIVE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static IS_CATALOG_REL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn install_seams() {
    INIT.call_once(|| {
        transam_xlog_seams::xlog_standby_info_active::set(|| false);
        transam_xlog_seams::xlog_logical_info_active::set(|| LOGICAL_ACTIVE.with(|c| c.get()));
        catalog_seams::is_catalog_relation::set(|_rel| IS_CATALOG_REL.with(|c| c.get()));
        bufmgr_seams::relation_smgr_locator::set(|rel| RelFileLocatorBackend {
            locator: RelFileLocator { spcOid: 1663, dbOid: 5, relNumber: rel.rd_id },
            backend: INVALID_PROC_NUMBER,
        });
        bufmgr_seams::read_buffer_extended::set(|_rel, fork, blkno, mode, _strategy| {
            assert_eq!(fork, ForkNumber::VISIBILITYMAP_FORKNUM);
            assert_eq!(mode, ReadBufferMode::ZeroOnError);
            with_fake(|f| {
                assert!(blkno < f.fork_nblocks, "read past VM fork EOF");
                f.read_calls += 1;
                f.pins[blkno as usize] += 1;
                Ok(blkno as Buffer + 1)
            })
        });
        bufmgr_seams::buffer_get_page::set(|buf| {
            with_fake(|f| {
                core::ptr::NonNull::new(f.pages[(buf - 1) as usize].as_mut_ptr()).unwrap()
            })
        });
        bufmgr_seams::release_buffer::set(|buf| {
            with_fake(|f| {
                let p = &mut f.pins[(buf - 1) as usize];
                assert!(*p > 0, "releasing unpinned buffer {buf}");
                *p -= 1;
            });
            Ok(())
        });
        bufmgr_seams::lock_buffer::set(|buf, mode| {
            with_fake(|f| {
                f.lock_calls += 1;
                let l = &mut f.locks[(buf - 1) as usize];
                match mode {
                    bufmgr_seams::BUFFER_LOCK_UNLOCK => {
                        assert!(*l != 0);
                        *l = 0;
                    }
                    bufmgr_seams::BUFFER_LOCK_EXCLUSIVE => {
                        assert!(*l == 0);
                        *l = -1;
                    }
                    other => panic!("unexpected lock mode {other}"),
                }
            });
            Ok(())
        });
        bufmgr_seams::extend_buffered_rel_to::set(|_, _, _, _, _, _| {
            panic!("unported callee reached from bufmgr.c: ExtendBufferedRelTo (extend machinery, phase 2)")
        });
        bufmgr_seams::extend_buffered_rel_to_rel::set(|rel, fork, strategy, flags, extend_to, mode| {
            bufmgr_seams::extend_buffered_rel_to::call(
                bufmgr_seams::relation_smgr_locator::call(rel),
                fork, strategy, flags, extend_to, mode,
            )
        });
        smgr_seams::smgr_exists::set(|_rloc, fork| {
            assert_eq!(fork, ForkNumber::VISIBILITYMAP_FORKNUM);
            with_fake(|f| {
                f.exists_calls += 1;
                Ok(f.fork_exists)
            })
        });
        smgr_seams::smgr_cached_nblocks::set(|_rloc, _fork| with_fake(|f| f.cached_nblocks));
        smgr_seams::smgr_set_cached_nblocks::set(|_rloc, _fork, v| {
            with_fake(|f| f.cached_nblocks = v);
            Ok(())
        });
        smgr_seams::smgr_nblocks::set(|_rloc, _fork| {
            with_fake(|f| {
                f.cached_nblocks = f.fork_nblocks;
                Ok(f.fork_nblocks)
            })
        });
        bufmgr_seams::mark_buffer_dirty::set(|buf| {
            with_fake(|f| {
                assert!(f.pins[(buf - 1) as usize] > 0, "dirtying unpinned buffer");
                f.dirty[(buf - 1) as usize] += 1;
            });
            Ok(())
        });
        bufmgr_seams::buffer_get_block_number::set(|buf| (buf - 1) as BlockNumber);
        transam_xlog_seams::data_checksums_enabled::set(|| with_fake(|f| f.checksums));
        xlogutils_seams::in_recovery::set(|| false);
        guc_tables::vars::wal_log_hints
            .install(guc_tables::GucVarAccessors { get: || false, set: |_| {} });
        xloginsert_seams::xlog_insert_record::set(|rmid, info, _flags, main_data, bufs| {
            with_fake(|f| {
                f.wal.push(WalRec {
                    rmid,
                    info,
                    main: main_data.concat(),
                    bufs: bufs.iter().map(|b| (b.block_id, b.buffer, b.flags)).collect(),
                });
                f.next_lsn += 8;
                Ok(f.next_lsn)
            })
        });
        xloginsert_seams::log_newpage_buffer::set(|buf, _std| {
            with_fake(|f| {
                f.newpage_calls.push(buf);
                f.next_lsn += 8;
                Ok(f.next_lsn)
            })
        });
    });
}

fn vm_page(bytes: &[(usize, u8)]) -> Box<AlignedPage> {
    let mut page = Box::new(AlignedPage([0u8; BLCKSZ]));
    // SAFETY: local BLCKSZ buffer, exclusively owned.
    unsafe { PageMut::from_raw(core::ptr::NonNull::new(page.as_mut_ptr()).unwrap()) }.init(0);
    for &(i, b) in bytes {
        assert!(i < MAPSIZE as usize);
        page.0[CONTENTS_OFF + i] = b;
    }
    page
}

fn test_relation<'mcx>(mcx: Mcx<'mcx>, oid: Oid) -> RelationData<'mcx> {
    let att = FormData_pg_attribute {
        attnum: 1,
        attlen: 4,
        attbyval: true,
        attalign: ::types_tuple::TYPALIGN_INT,
        attstorage: ::types_tuple::TYPSTORAGE_PLAIN,
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
    let mut relname = NameData::default();
    relname.namestrcpy("t");
    RelationData { rd_locator: Default::default(), rd_smgr: Default::default(),
        rd_id: oid,
        rd_backend: INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: std::cell::Cell::new(true),
        rd_createSubid: std::cell::Cell::new(0),
        rd_newRelfilelocatorSubid: std::cell::Cell::new(0),
        rd_firstRelfilelocatorSubid: std::cell::Cell::new(0),
        rd_droppedSubid: std::cell::Cell::new(0),
        rd_lockInfo: LockInfoData { lockRelId: LockRelId { relId: oid, dbId: 5 } },
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
            relreplident: b'd',
            relispartition: false,
            relfrozenxid: 3,
            relminmxid: 1,
        },
        rd_att,
        rd_index: None,
        rd_opcintype: PgVec::new_in(mcx),
        rd_opfamily: PgVec::new_in(mcx),
        rd_indoption: PgVec::new_in(mcx),
        rd_indcollation: PgVec::new_in(mcx),
        rd_options: None,
        pgstat_enabled: std::cell::Cell::new(true),
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

#[test]
fn get_status_bit_math() {
    let _s = serial();
    let ctx = MemoryContext::new("test");
    let rel = test_relation(ctx.mcx(), 4242);
    setup(
        vec![vm_page(&[
            (0, 0b11_00_01_10),
            (1, 0x01),
            (MAPSIZE as usize - 1, 0b11_00_00_00),
        ])],
        true,
    );
    let mut vmbuf = VmBuffer::new();
    assert_eq!(visibilitymap_get_status(&rel, 0, &mut vmbuf).unwrap(), 0b10);
    assert_eq!(visibilitymap_get_status(&rel, 1, &mut vmbuf).unwrap(), 0b01);
    assert_eq!(visibilitymap_get_status(&rel, 2, &mut vmbuf).unwrap(), 0b00);
    assert_eq!(visibilitymap_get_status(&rel, 3, &mut vmbuf).unwrap(), 0b11);
    assert_eq!(visibilitymap_get_status(&rel, 4, &mut vmbuf).unwrap(), 0b01);
    let last = HEAPBLOCKS_PER_PAGE - 1;
    assert_eq!(visibilitymap_get_status(&rel, last, &mut vmbuf).unwrap(), 0b11);

    assert!(vm_all_visible(&rel, 3, &mut vmbuf).unwrap());
    assert!(vm_all_frozen(&rel, 3, &mut vmbuf).unwrap());
    assert!(vm_all_visible(&rel, 1, &mut vmbuf).unwrap());
    assert!(!vm_all_frozen(&rel, 1, &mut vmbuf).unwrap());
    assert!(!vm_all_visible(&rel, 2, &mut vmbuf).unwrap());

    with_fake(|f| {
        assert_eq!(f.read_calls, 1);
        assert_eq!(f.pins[0], 1);
    });
    vmbuf.release();
    with_fake(|f| assert_eq!(f.pins[0], 0));
}

#[test]
fn get_status_map_page_boundary() {
    let _s = serial();
    let ctx = MemoryContext::new("test");
    let rel = test_relation(ctx.mcx(), 4242);
    setup(
        vec![
            vm_page(&[(MAPSIZE as usize - 1, 0b01_00_00_00)]),
            vm_page(&[(0, 0b11)]),
        ],
        true,
    );

    let mut vmbuf = VmBuffer::new();
    let last_on_first = HEAPBLOCKS_PER_PAGE - 1;
    assert_eq!(
        visibilitymap_get_status(&rel, last_on_first, &mut vmbuf).unwrap(),
        0b01
    );
    with_fake(|f| assert_eq!((f.pins[0], f.pins[1]), (1, 0)));

    assert_eq!(
        visibilitymap_get_status(&rel, HEAPBLOCKS_PER_PAGE, &mut vmbuf).unwrap(),
        0b11
    );
    with_fake(|f| {
        assert_eq!((f.pins[0], f.pins[1]), (0, 1));
        assert_eq!(f.read_calls, 2);
    });
    vmbuf.release();
    with_fake(|f| assert_eq!((f.pins[0], f.pins[1]), (0, 0)));
}

#[test]
fn get_status_missing_fork_caches_zero() {
    let _s = serial();
    let ctx = MemoryContext::new("test");
    let rel = test_relation(ctx.mcx(), 4242);
    setup(vec![], false);

    let mut vmbuf = VmBuffer::new();
    assert_eq!(visibilitymap_get_status(&rel, 7, &mut vmbuf).unwrap(), 0);
    assert!(!vmbuf.is_valid());
    with_fake(|f| {
        assert_eq!(f.exists_calls, 1);
        assert_eq!(f.cached_nblocks, 0);
    });
    assert_eq!(visibilitymap_get_status(&rel, 7, &mut vmbuf).unwrap(), 0);
    with_fake(|f| assert_eq!(f.exists_calls, 1));
}

#[test]
fn pin_and_pin_ok() {
    let _s = serial();
    let ctx = MemoryContext::new("test");
    let rel = test_relation(ctx.mcx(), 4242);
    setup(vec![vm_page(&[]), vm_page(&[])], true);

    let mut vmbuf = VmBuffer::new();
    assert!(!visibilitymap_pin_ok(0, &vmbuf));

    visibilitymap_pin(&rel, 0, &mut vmbuf).unwrap();
    with_fake(|f| assert_eq!(f.pins[0], 1));
    assert!(visibilitymap_pin_ok(0, &vmbuf));
    assert!(visibilitymap_pin_ok(HEAPBLOCKS_PER_PAGE - 1, &vmbuf));
    assert!(!visibilitymap_pin_ok(HEAPBLOCKS_PER_PAGE, &vmbuf));

    let reads = with_fake(|f| f.read_calls);
    visibilitymap_pin(&rel, 5, &mut vmbuf).unwrap();
    with_fake(|f| assert_eq!(f.read_calls, reads));

    visibilitymap_pin(&rel, HEAPBLOCKS_PER_PAGE, &mut vmbuf).unwrap();
    with_fake(|f| assert_eq!((f.pins[0], f.pins[1]), (0, 1)));
    assert!(visibilitymap_pin_ok(HEAPBLOCKS_PER_PAGE, &vmbuf));
    vmbuf.release();
}

#[test]
fn pin_past_eof_reaches_extend_panic() {
    let _s = serial();
    let ctx = MemoryContext::new("test");
    let rel = test_relation(ctx.mcx(), 4242);
    setup(vec![], true);

    let mut vmbuf = VmBuffer::new();
    let err = catch_unwind(AssertUnwindSafe(|| visibilitymap_pin(&rel, 0, &mut vmbuf)))
        .unwrap_err();
    let msg = err.downcast_ref::<&str>().copied().unwrap_or("");
    assert!(msg.contains("ExtendBufferedRelTo"), "unexpected panic: {msg}");
}

#[test]
fn count_popcounts_masked() {
    let _s = serial();
    let ctx = MemoryContext::new("test");
    let rel = test_relation(ctx.mcx(), 4242);
    setup(
        vec![
            vm_page(&[(0, 0xff), (10, 0x55)]),
            vm_page(&[(3, 0xaa)]),
        ],
        true,
    );

    let (nvisible, nfrozen) = visibilitymap_count(&rel).unwrap();
    assert_eq!(nvisible, 8); // 0xff -> 4, 0x55 -> 4, 0xaa -> 0
    assert_eq!(nfrozen, 8); // 0xff -> 4, 0x55 -> 0, 0xaa -> 4
    with_fake(|f| assert_eq!((f.pins[0], f.pins[1]), (0, 0)));
}

#[test]
fn new_page_initialized_under_lock() {
    let _s = serial();
    let ctx = MemoryContext::new("test");
    let rel = test_relation(ctx.mcx(), 4242);
    setup(vec![Box::new(AlignedPage([0u8; BLCKSZ]))], true);

    let mut vmbuf = VmBuffer::new();
    assert_eq!(visibilitymap_get_status(&rel, 0, &mut vmbuf).unwrap(), 0);
    with_fake(|f| {
        assert_eq!(f.lock_calls, 2); // exclusive + unlock around PageInit
        assert_eq!(f.locks[0], 0);
        // SAFETY: the fake page is live for the fake's lifetime.
        let page = unsafe {
            PageRef::from_raw(core::ptr::NonNull::new(f.pages[0].as_mut_ptr()).unwrap())
        };
        assert!(!page.is_new());
    });

    let mut vmbuf2 = VmBuffer::new();
    assert_eq!(visibilitymap_get_status(&rel, 1, &mut vmbuf2).unwrap(), 0);
    with_fake(|f| assert_eq!(f.lock_calls, 2));
    vmbuf.release();
    vmbuf2.release();
}

fn map_byte(i: usize) -> u8 {
    with_fake(|f| f.pages[0].0[CONTENTS_OFF + i])
}

fn heap_pages() -> (Box<AlignedPage>, Box<AlignedPage>) {
    let vm = vm_page(&[]);
    let mut heap = Box::new(AlignedPage([0u8; BLCKSZ]));
    // SAFETY: local BLCKSZ buffer, exclusively owned.
    let mut pm =
        unsafe { PageMut::from_raw(core::ptr::NonNull::new(heap.as_mut_ptr()).unwrap()) };
    pm.init(0);
    pm.set_all_visible();
    (vm, heap)
}

#[test]
fn set_bits_wal_record_and_lsns() {
    let _s = serial();
    let ctx = MemoryContext::new("test");
    let rel = test_relation(ctx.mcx(), 4242);
    let (vm, heap) = heap_pages();
    setup_n(vec![vm, heap], true, 1); // heap page = buffer 2, block 1

    let heap_buf: Buffer = 2;
    let heap_blk: BlockNumber = 1; // mapByte 0, offset 2
    let mut vmbuf = VmBuffer::new();
    visibilitymap_pin(&rel, heap_blk, &mut vmbuf).unwrap();

    let prev = visibilitymap_set(
        &rel, heap_blk, heap_buf, 0, &vmbuf, 57, VISIBILITYMAP_ALL_VISIBLE,
    )
    .unwrap();
    assert_eq!(prev, 0);
    assert_eq!(map_byte(0), 0b0100);
    with_fake(|f| {
        assert_eq!(f.dirty[0], 1);
        assert_eq!(f.wal.len(), 1);
        let rec = &f.wal[0];
        assert_eq!(rec.rmid, 9); // RM_HEAP2_ID
        assert_eq!(rec.info, 0x40); // XLOG_HEAP2_VISIBLE
        // xl_heap_visible { snapshotConflictHorizon; flags }
        assert_eq!(rec.main.len(), 5);
        assert_eq!(u32::from_ne_bytes(rec.main[0..4].try_into().unwrap()), 57);
        assert_eq!(rec.main[4], VISIBILITYMAP_ALL_VISIBLE);
        assert_eq!(rec.bufs.len(), 2);
        assert_eq!(rec.bufs[0], (0, 1, 0));
        assert_eq!(
            rec.bufs[1],
            (1, heap_buf, xloginsert_seams::REGBUF_STANDARD | xloginsert_seams::REGBUF_NO_IMAGE)
        );
        let vm_lsn = unsafe {
            PageRef::from_raw(core::ptr::NonNull::new(f.pages[0].as_mut_ptr()).unwrap())
        }
        .lsn();
        assert_eq!(vm_lsn, f.next_lsn);
        let heap_lsn = unsafe {
            PageRef::from_raw(core::ptr::NonNull::new(f.pages[1].as_mut_ptr()).unwrap())
        }
        .lsn();
        assert_eq!(heap_lsn, 0);
        assert_eq!(f.locks[0], 0);
    });

    let prev = visibilitymap_set(
        &rel, heap_blk, heap_buf, 0, &vmbuf, 57, VISIBILITYMAP_ALL_VISIBLE,
    )
    .unwrap();
    assert_eq!(prev, VISIBILITYMAP_ALL_VISIBLE);
    with_fake(|f| {
        assert_eq!(f.wal.len(), 1);
        assert_eq!(f.dirty[0], 1);
    });

    with_fake(|f| f.checksums = true);
    let prev = visibilitymap_set(
        &rel, heap_blk, heap_buf, 0, &vmbuf, 0, VISIBILITYMAP_VALID_BITS,
    )
    .unwrap();
    assert_eq!(prev, VISIBILITYMAP_ALL_VISIBLE);
    assert_eq!(map_byte(0), 0b1100);
    with_fake(|f| {
        assert_eq!(f.wal.len(), 2);
        assert_eq!(f.wal[1].bufs[1], (1, heap_buf, xloginsert_seams::REGBUF_STANDARD));
        let heap_lsn = unsafe {
            PageRef::from_raw(core::ptr::NonNull::new(f.pages[1].as_mut_ptr()).unwrap())
        }
        .lsn();
        assert_eq!(heap_lsn, f.next_lsn);
    });

    vmbuf.release();
}

#[test]
fn set_rejects_wrong_buffers() {
    let _s = serial();
    let ctx = MemoryContext::new("test");
    let rel = test_relation(ctx.mcx(), 4242);
    let (vm, heap) = heap_pages();
    setup_n(vec![vm, heap], true, 1);

    let mut vmbuf = VmBuffer::new();
    visibilitymap_pin(&rel, 1, &mut vmbuf).unwrap();

    let err = visibilitymap_set(&rel, 3, 2, 0, &vmbuf, 0, VISIBILITYMAP_ALL_VISIBLE)
        .unwrap_err();
    assert!(err.message.contains("wrong heap buffer"), "{}", err.message);

    // Unpinned VmBuffer.
    let empty = VmBuffer::new();
    let err = visibilitymap_set(&rel, 1, 2, 0, &empty, 0, VISIBILITYMAP_ALL_VISIBLE)
        .unwrap_err();
    assert!(err.message.contains("wrong VM buffer"), "{}", err.message);

    let err = visibilitymap_clear(&rel, 1, &empty, VISIBILITYMAP_VALID_BITS).unwrap_err();
    assert!(
        err.message.contains("wrong buffer passed to visibilitymap_clear"),
        "{}",
        err.message
    );
    vmbuf.release();
}

#[test]
fn clear_bit_operations() {
    let _s = serial();
    let ctx = MemoryContext::new("test");
    let rel = test_relation(ctx.mcx(), 4242);
    setup(vec![vm_page(&[(0, 0b1111)])], true);

    let mut vmbuf = VmBuffer::new();
    visibilitymap_pin(&rel, 0, &mut vmbuf).unwrap();

    assert!(visibilitymap_clear(&rel, 0, &vmbuf, VISIBILITYMAP_ALL_FROZEN).unwrap());
    assert_eq!(map_byte(0), 0b1101);
    with_fake(|f| assert_eq!(f.dirty[0], 1));

    assert!(visibilitymap_clear(&rel, 1, &vmbuf, VISIBILITYMAP_VALID_BITS).unwrap());
    assert_eq!(map_byte(0), 0b0001);

    let locks_before = with_fake(|f| f.lock_calls);
    assert!(!visibilitymap_clear(&rel, 1, &vmbuf, VISIBILITYMAP_VALID_BITS).unwrap());
    with_fake(|f| {
        assert_eq!(f.dirty[0], 2);
        assert_eq!(f.lock_calls, locks_before + 2);
        assert_eq!(f.locks[0], 0);
    });
    vmbuf.release();
}

#[test]
fn prepare_truncate_clears_tail() {
    let _s = serial();
    let ctx = MemoryContext::new("test");
    let rel = test_relation(ctx.mcx(), 4242);

    setup(vec![], false);
    assert_eq!(visibilitymap_prepare_truncate(&rel, 5).unwrap(), InvalidBlockNumber);

    setup(
        vec![vm_page(&[(0, 0xff), (1, 0xff), (2, 0xff), (MAPSIZE as usize - 1, 0xff)]), vm_page(&[])],
        true,
    );
    assert_eq!(visibilitymap_prepare_truncate(&rel, 5).unwrap(), 1);
    assert_eq!(map_byte(0), 0xff);
    assert_eq!(map_byte(1), 0x03);
    assert_eq!(map_byte(2), 0);
    assert_eq!(map_byte(MAPSIZE as usize - 1), 0);
    with_fake(|f| {
        assert_eq!(f.dirty[0], 1);
        assert!(f.newpage_calls.is_empty()); // no checksums/wal_log_hints
        assert_eq!(f.pins[0], 0);
        assert_eq!(f.locks[0], 0);
    });

    setup(vec![vm_page(&[(1, 0xff)]), vm_page(&[])], true);
    with_fake(|f| f.checksums = true);
    assert_eq!(visibilitymap_prepare_truncate(&rel, 5).unwrap(), 1);
    with_fake(|f| assert_eq!(f.newpage_calls, vec![1]));

    setup(vec![vm_page(&[])], true);
    assert_eq!(
        visibilitymap_prepare_truncate(&rel, HEAPBLOCKS_PER_PAGE).unwrap(),
        InvalidBlockNumber
    );

    setup(vec![vm_page(&[]), vm_page(&[])], true);
    assert_eq!(
        visibilitymap_prepare_truncate(&rel, HEAPBLOCKS_PER_PAGE).unwrap(),
        1
    );

    setup(vec![vm_page(&[])], true);
    assert_eq!(
        visibilitymap_prepare_truncate(&rel, HEAPBLOCKS_PER_PAGE + 5).unwrap(),
        InvalidBlockNumber
    );
}

#[test]
fn map_geometry_matches_c() {
    assert_eq!(CONTENTS_OFF, 24);
    assert_eq!(MAPSIZE, 8168);
    assert_eq!(HEAPBLOCKS_PER_PAGE, 32672);
    assert_eq!(HEAPBLK_TO_MAPBLOCK(32671), 0);
    assert_eq!(HEAPBLK_TO_MAPBLOCK(32672), 1);
    assert_eq!(HEAPBLK_TO_MAPBYTE(32672), 0);
    assert_eq!(HEAPBLK_TO_MAPBYTE(32671), 8167);
    assert_eq!(HEAPBLK_TO_OFFSET(32671), 6);
    assert_eq!(HEAPBLK_TO_OFFSET(4), 0);
}

#[test]
fn set_bits_catalog_rel_flags_wal_record() {
    // A catalog relation under wal_level=logical stamps
    // VISIBILITYMAP_XLOG_CATALOG_REL into xl_heap_visible.flags — the bit a
    // standby uses to invalidate logical slots the cutoff overtook.
    let _s = serial();
    let ctx = MemoryContext::new("test");
    let rel = test_relation(ctx.mcx(), 1259);
    let (vm, heap) = heap_pages();
    setup_n(vec![vm, heap], true, 1);
    LOGICAL_ACTIVE.with(|c| c.set(true));
    IS_CATALOG_REL.with(|c| c.set(true));

    let heap_buf: Buffer = 2;
    let heap_blk: BlockNumber = 1;
    let mut vmbuf = VmBuffer::new();
    visibilitymap_pin(&rel, heap_blk, &mut vmbuf).unwrap();
    visibilitymap_set(&rel, heap_blk, heap_buf, 0, &vmbuf, 57, VISIBILITYMAP_ALL_VISIBLE)
        .unwrap();
    LOGICAL_ACTIVE.with(|c| c.set(false));
    IS_CATALOG_REL.with(|c| c.set(false));

    with_fake(|f| {
        let rec = f.wal.last().expect("visible record logged");
        assert_eq!(rec.info, 0x40);
        // flags byte = VM bits | VISIBILITYMAP_XLOG_CATALOG_REL; the map
        // itself must NOT carry the xlog-only bit.
        assert_eq!(rec.main[4], VISIBILITYMAP_ALL_VISIBLE | 0x04);
    });
    assert_eq!(map_byte(0), 0b0100);
}
