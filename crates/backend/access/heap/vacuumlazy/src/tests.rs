use core::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Mutex, MutexGuard, OnceLock};

use ::mcx::MemoryContext;
use ::types_core::{Buffer, GlobalVisStateHandle};
use ::types_rel::{FormData_pg_class, RELKIND_RELATION};
use ::types_storage::bufpage::PAI_IS_HEAP;
use ::types_tuple::NameData;

use super::*;

const REL_OID: ::types_core::Oid = 61077;

#[repr(align(8))]
struct TestPage([u8; BLCKSZ]);

struct Fake {
    // (fork, block) -> leaked page address; buffer id = index + 1.
    entries: Vec<(u8, BlockNumber, usize)>,
    pins: Vec<i32>,
    locks: Vec<i32>,
    wal: Vec<(u8, u8, Vec<u8>)>,
}

static FAKE: Mutex<Option<Fake>> = Mutex::new(None);

fn with_fake<R>(f: impl FnOnce(&mut Fake) -> R) -> R {
    let mut g = FAKE.lock().unwrap_or_else(|e| e.into_inner());
    f(g.as_mut().expect("fake installed"))
}

fn fake_page(fork: u8, block: BlockNumber) -> Buffer {
    with_fake(|f| {
        if let Some(i) = f.entries.iter().position(|&(fk, b, _)| fk == fork && b == block) {
            f.pins[i] += 1;
            return (i + 1) as Buffer;
        }
        let addr = Box::leak(Box::new(TestPage([0u8; BLCKSZ]))).0.as_mut_ptr() as usize;
        f.entries.push((fork, block, addr));
        f.pins.push(1);
        f.locks.push(0);
        f.entries.len() as Buffer
    })
}

fn fork_nblocks(fork: u8) -> BlockNumber {
    with_fake(|f| {
        f.entries
            .iter()
            .filter(|&&(fk, _, _)| fk == fork)
            .map(|&(_, b, _)| b + 1)
            .max()
            .unwrap_or(0)
    })
}

fn install_seams() {
    static INSTALL: OnceLock<()> = OnceLock::new();
    INSTALL.get_or_init(|| {
        bufmgr_seams::buffer_get_page::set(|buf| {
            let addr = with_fake(|f| {
                assert!(f.pins[(buf - 1) as usize] > 0, "page access without pin");
                f.entries[(buf - 1) as usize].2
            });
            NonNull::new(addr as *mut u8).unwrap()
        });
        bufmgr_seams::buffer_get_block_number::set(|buf| {
            with_fake(|f| f.entries[(buf - 1) as usize].1)
        });
        bufmgr_seams::read_buffer_extended::set(|_rel, fork, block, _mode, _strat| {
            Ok(fake_page(fork as u8, block))
        });
        bufmgr_seams::extend_buffered_rel_to::set(
            |_smgr, fork, _strat, _flags, extend_to, _mode| {
                for b in fork_nblocks(fork as u8)..extend_to {
                    let buf = fake_page(fork as u8, b);
                    if b + 1 < extend_to {
                        bufmgr_seams::release_buffer::call(buf)?;
                    }
                }
                with_fake(|f| {
                    let i = f
                        .entries
                        .iter()
                        .position(|&(fk, b, _)| fk == fork as u8 && b == extend_to - 1)
                        .unwrap();
                    Ok((i + 1) as Buffer)
                })
            },
        );
        bufmgr_seams::extend_buffered_rel_to_rel::set(|rel, fork, strategy, flags, extend_to, mode| {
            bufmgr_seams::extend_buffered_rel_to::call(
                bufmgr_seams::relation_smgr_locator::call(rel),
                fork, strategy, flags, extend_to, mode,
            )
        });
        bufmgr_seams::lock_buffer::set(|buf, mode| {
            with_fake(|f| {
                let l = &mut f.locks[(buf - 1) as usize];
                if mode == bufmgr_seams::BUFFER_LOCK_UNLOCK {
                    assert!(*l > 0, "unlock without lock");
                    *l -= 1;
                } else {
                    assert_eq!(*l, 0, "double content lock");
                    *l += 1;
                }
            });
            Ok(())
        });
        bufmgr_seams::conditional_lock_buffer::set(|buf| {
            with_fake(|f| {
                let l = &mut f.locks[(buf - 1) as usize];
                if *l != 0 {
                    return Ok(false);
                }
                *l += 1;
                Ok(true)
            })
        });
        bufmgr_seams::release_buffer::set(|buf| {
            with_fake(|f| {
                let p = &mut f.pins[(buf - 1) as usize];
                assert!(*p > 0, "double release");
                *p -= 1;
            });
            Ok(())
        });
        bufmgr_seams::mark_buffer_dirty::set(|_| Ok(()));
        bufmgr_seams::mark_buffer_dirty_hint::set(|_, _| Ok(()));
        bufmgr_seams::relation_get_number_of_blocks_in_fork::set(|_rel, fork| {
            Ok(fork_nblocks(fork as u8))
        });
        bufmgr_seams::relation_smgr_locator::set(|rel| ::types_storage::RelFileLocatorBackend {
            locator: ::types_storage::RelFileLocator::new(1663, 5, rel.rd_id),
            backend: ::types_core::INVALID_PROC_NUMBER,
        });

        smgr_seams::smgr_cached_nblocks::set(|_loc, fork| fork_nblocks(fork as u8));
        smgr_seams::smgr_set_cached_nblocks::set(|_loc, _fork, _n| Ok(()));
        smgr_seams::smgr_exists::set(|_loc, fork| Ok(fork_nblocks(fork as u8) > 0));
        smgr_seams::smgr_nblocks::set(|_loc, fork| Ok(fork_nblocks(fork as u8)));

        static NEXT_LSN: AtomicU64 = AtomicU64::new(0x0100_0000);
        xloginsert_seams::xlog_insert_record::set(|rmid, info, _flags, main_data, _bufs| {
            let main: Vec<u8> = main_data.iter().flat_map(|d| d.iter().copied()).collect();
            with_fake(|f| f.wal.push((rmid, info, main)));
            Ok(NEXT_LSN.fetch_add(64, Relaxed))
        });
        xlogutils_seams::in_recovery::set(|| false);
        transam_xlog_seams::data_checksums_enabled::set(|| false);
        transam_xlog_seams::recovery_in_progress::set(|| false);
        sinval_seams::send_shared_invalid_messages::set(|_| Ok(()));
        guc_tables::vars::wal_log_hints
            .install(guc_tables::GucVarAccessors { get: || false, set: |_| {} });
    });
    let mut g = FAKE.lock().unwrap_or_else(|e| e.into_inner());
    *g = Some(Fake { entries: Vec::new(), pins: Vec::new(), locks: Vec::new(), wal: Vec::new() });
}

fn test_relation<'mcx>(mcx: Mcx<'mcx>) -> RelationData<'mcx> {
    use std::cell::Cell;
    use std::rc::Rc;
    use ::types_rel::{LockInfoData, LockRelId};
    use ::types_tuple::{CompactAttribute, FormData_pg_attribute, TupleDescData};
    let att = FormData_pg_attribute {
        attnum: 1,
        attlen: 4,
        attbyval: true,
        attalign: ::types_tuple::TYPALIGN_INT,
        attstorage: ::types_tuple::TYPSTORAGE_PLAIN,
        ..Default::default()
    };
    let mut attrs = ::mcx::PgVec::new_in(mcx);
    let mut compact = ::mcx::PgVec::new_in(mcx);
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
    let rd_rel = FormData_pg_class {
        relname,
        relnamespace: 2200,
        reltype: 0,
        relowner: 10,
        relam: 2,
        relfilenode: REL_OID,
        reltablespace: 0,
        relpages: 0,
        reltuples: -1.0,
        relallvisible: 0,
        reltoastrelid: 0,
        relhasindex: false,
        relisshared: false,
        relpersistence: ::types_core::RELPERSISTENCE_PERMANENT,
        relkind: RELKIND_RELATION,
        relhassubclass: false,
        relrowsecurity: false,
        relispopulated: true,
        relreplident: b'd',
        relispartition: false,
        relfrozenxid: 3,
        relminmxid: 1,
    };
    RelationData {
        rd_locator: Default::default(),
        rd_smgr: Default::default(),
        rd_id: REL_OID,
        rd_backend: ::types_core::INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: LockInfoData {
            lockRelId: LockRelId { relId: REL_OID, dbId: 5 },
        },
        rd_rel,
        rd_att,
        rd_index: None,
        rd_opcintype: ::mcx::PgVec::new_in(mcx),
        rd_opfamily: ::mcx::PgVec::new_in(mcx),
        rd_indoption: ::mcx::PgVec::new_in(mcx),
        rd_indcollation: ::mcx::PgVec::new_in(mcx),
        rd_options: None,
        pgstat_enabled: Cell::new(false),
        rd_amcache: Default::default(),
        rd_amcache_hash: Default::default(),
        rd_supportinfo: Default::default(),
        rd_indexlist: Default::default(),
    }
}

fn vacrel<'a, 'mcx>(rel: &'a RelationData<'mcx>, mcx: Mcx<'mcx>) -> LVRelState<'a, 'mcx> {
    LVRelState {
        rel,
        nindexes: 1,
        index_oids: PgVec::new_in(mcx),
        bstrategy: None,
        aggressive: false,
        skipwithvm: true,
        consider_bypass_optimization: false,
        do_index_vacuuming: true,
        do_index_cleanup: true,
        do_rel_truncate: false,
        cutoffs: VacuumCutoffs {
            relfrozenxid: 3,
            relminmxid: 1,
            OldestXmin: 1000,
            OldestMxact: 1,
            FreezeLimit: 1000,
            MultiXactCutoff: 1,
        },
        vistest: GlobalVisStateHandle::new(0),
        skippedallvis: false,
        rel_pages: 1,
        scanned_pages: 0,
        new_frozen_tuple_pages: 0,
        lpdead_item_pages: 1,
        nonempty_pages: 0,
        dead_items: PgVec::new_in(mcx),
        dead_items_max_bytes: 64 * 1024 * 1024,
        num_index_scans: 1,
        tuples_deleted: 0,
        tuples_frozen: 0,
        lpdead_items: 0,
        live_tuples: 0,
        recently_dead_tuples: 0,
        missed_dead_tuples: 0,
        vm_new_visible_pages: 0,
        vm_new_visible_frozen_pages: 0,
        vm_new_frozen_pages: 0,
        new_rel_tuples: 0.0,
        new_live_tuples: 0.0,
        offnum: InvalidOffsetNumber,
        current_block: InvalidBlockNumber,
        next_unskippable_block: InvalidBlockNumber,
        next_unskippable_allvis: false,
        next_unskippable_vmbuffer: VmBuffer::new(),
    }
}

static SERIAL: Mutex<()> = Mutex::new(());
fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

// Phase III on one page: three LP_DEAD items become LP_UNUSED, the line
// pointer array truncates to one entry, an xl_heap_prune VACUUM_CLEANUP
// record is emitted, and the now-empty page is set all-visible + all-frozen
// in the VM with PD_ALL_VISIBLE first.
#[test]
fn lazy_vacuum_heap_rel_reaps_dead_items() {
    let _s = serial();
    install_seams();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let rel = test_relation(mcx);

    let heap_buf = fake_page(0, 0);
    {
        // SAFETY: freshly created exclusive test page.
        let mut pm = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(heap_buf)) };
        pm.init(0);
        let tuple = [0u8; 32];
        for expected in 1..=3u16 {
            let off = pm.add_item(&tuple, InvalidOffsetNumber, PAI_IS_HEAP).unwrap();
            assert_eq!(off, expected);
            let mut lp = pm.as_ref().item_id(off);
            lp.set_dead();
            pm.set_item_id(off, lp);
        }
    }
    bufmgr_seams::release_buffer::call(heap_buf).unwrap();

    let mut vr = vacrel(&rel, mcx);
    dead_items_add(&mut vr, 0, &[1, 2, 3]).unwrap();
    vr.lpdead_items = 3;

    lazy_vacuum_heap_rel(&mut vr).unwrap();

    let buf = fake_page(0, 0);
    // SAFETY: test page, live.
    let page = unsafe { PageRef::from_raw(bufmgr_seams::buffer_get_page::call(buf)) };
    assert_eq!(page.max_offset_number(), 1, "lp array truncated to one entry");
    assert!(!page.item_id(1).is_used(), "remaining lp is LP_UNUSED");
    assert!(page.is_all_visible(), "PD_ALL_VISIBLE set");
    assert!(page.heap_free_space() > BLCKSZ / 2);
    bufmgr_seams::release_buffer::call(buf).unwrap();

    let mut vmb = VmBuffer::new();
    let status = visibilitymap_get_status(&rel, 0, &mut vmb).unwrap();
    assert_eq!(
        status,
        VISIBILITYMAP_ALL_VISIBLE | VISIBILITYMAP_ALL_FROZEN,
        "VM shows all-visible + all-frozen through the read side"
    );
    vmb.release();

    let (rm_heap2, prune_cleanup_info) = (9u8, 0x30u8);
    let prune = with_fake(|f| {
        f.wal
            .iter()
            .filter(|&&(rmid, info, _)| rmid == rm_heap2 && (info & 0xF0) == prune_cleanup_info)
            .count()
    });
    assert_eq!(prune, 1, "one XLOG_HEAP2_PRUNE_VACUUM_CLEANUP record");

    with_fake(|f| {
        assert!(f.pins.iter().all(|&p| p == 0), "leaked pins: {:?}", f.pins);
        assert!(f.locks.iter().all(|&l| l == 0), "leaked locks: {:?}", f.locks);
    });
}

// Mixed pattern: a used item behind trailing unused ones stops truncation.
#[test]
fn truncate_line_pointer_array_keeps_used_prefix() {
    let _s = serial();
    install_seams();
    let buf = fake_page(0, 7);
    // SAFETY: freshly created exclusive test page.
    let mut pm = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(buf)) };
    pm.init(0);
    let tuple = [0u8; 16];
    for _ in 1..=5u16 {
        pm.add_item(&tuple, InvalidOffsetNumber, PAI_IS_HEAP).unwrap();
    }
    for off in [2u16, 4, 5] {
        let mut lp = pm.as_ref().item_id(off);
        lp.set_unused();
        pm.set_item_id(off, lp);
    }

    pm.truncate_line_pointer_array();

    let page = pm.as_ref();
    assert_eq!(page.max_offset_number(), 3, "trailing unused pair dropped");
    assert!(page.item_id(1).is_used() && page.item_id(3).is_used());
    assert!(!page.item_id(2).is_used());
    assert!(page.has_free_line_pointers(), "hint set: unused lp remains");
    bufmgr_seams::release_buffer::call(buf).unwrap();
}
