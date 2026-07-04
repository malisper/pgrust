use super::*;
use ::bufmgr_seams::BufferPin;
use ::types_storage::bufpage::PageMut;
use core::ptr::NonNull;
use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering::Relaxed};
use std::sync::{Mutex, MutexGuard, Once};

#[repr(align(8))]
struct AlignedPage([u8; BLCKSZ]);

fn fsm_test_page() -> Box<AlignedPage> {
    let mut page = Box::new(AlignedPage([0u8; BLCKSZ]));
    // SAFETY: local BLCKSZ buffer, exclusively owned.
    unsafe { PageMut::from_raw(NonNull::new(page.0.as_mut_ptr()).unwrap()) }.init(0);
    page
}

fn view(page: &mut AlignedPage) -> FsmPage {
    // SAFETY: exclusive borrow of a live init'ed page.
    unsafe { FsmPage::from_raw(NonNull::new(page.0.as_mut_ptr()).unwrap()) }
}

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 16
    }
}

static SERIAL: Mutex<()> = Mutex::new(());
static PAGE_ADDR: AtomicUsize = AtomicUsize::new(0);
static LOCK_STATE: AtomicI32 = AtomicI32::new(0); // 0 none, 1 share, 2 excl
static RELOCKS: AtomicUsize = AtomicUsize::new(0);
static DIRTY_HINTS: AtomicUsize = AtomicUsize::new(0);
static INIT: Once = Once::new();

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

fn install_page_seams() {
    INIT.call_once(|| {
        bufmgr_seams::buffer_get_page::set(|_buf| {
            NonNull::new(PAGE_ADDR.load(Relaxed) as *mut u8).unwrap()
        });
        bufmgr_seams::lock_buffer::set(|_buf, mode| {
            match mode {
                bufmgr_seams::BUFFER_LOCK_UNLOCK => {
                    assert!(LOCK_STATE.swap(0, Relaxed) != 0, "unlock without lock");
                }
                bufmgr_seams::BUFFER_LOCK_EXCLUSIVE => {
                    assert_eq!(LOCK_STATE.swap(2, Relaxed), 0, "double lock");
                    RELOCKS.fetch_add(1, Relaxed);
                }
                _ => {
                    assert_eq!(LOCK_STATE.swap(1, Relaxed), 0, "double lock");
                }
            }
            Ok(())
        });
        bufmgr_seams::mark_buffer_dirty_hint::set(|_buf, _std| {
            DIRTY_HINTS.fetch_add(1, Relaxed);
            Ok(())
        });
        bufmgr_seams::release_buffer::set(|_buf| Ok(()));
    });
}

fn search(page: &mut AlignedPage, minvalue: u8, advancenext: bool, excl: bool) -> i32 {
    install_page_seams();
    PAGE_ADDR.store(page.0.as_mut_ptr() as usize, Relaxed);
    LOCK_STATE.store(if excl { 2 } else { 1 }, Relaxed);
    let pin = BufferPin::adopt(1).unwrap();
    let slot = fsm_search_avail(&pin, minvalue, advancenext, excl).unwrap();
    LOCK_STATE.store(0, Relaxed);
    pin.release();
    slot
}

// First slot >= min at/after fp_next_slot, wrapping (fsmpage.c's result).
fn model_search(leaves: &[u8; SLOTS_PER_FSM_PAGE as usize], target: i32, min: u8) -> i32 {
    let n = SLOTS_PER_FSM_PAGE as usize;
    let start = if (0..SLOTS_PER_FSM_PAGE).contains(&target) { target as usize } else { 0 };
    for i in 0..n {
        let s = (start + i) % n;
        if leaves[s] >= min {
            return s as i32;
        }
    }
    -1
}

#[test]
fn set_get_and_search_match_model() {
    let _s = serial();
    let mut page = fsm_test_page();
    let mut model = [0u8; SLOTS_PER_FSM_PAGE as usize];
    let mut rng = Lcg(42);

    for round in 0..2000 {
        let slot = (rng.next() % SLOTS_PER_FSM_PAGE as u64) as i32;
        let value = (rng.next() % 256) as u8;
        fsm_set_avail(view(&mut page), slot, value);
        model[slot as usize] = value;

        if round % 50 == 0 {
            let v = view(&mut page);
            assert_eq!(fsm_get_max_avail(v), model.iter().copied().max().unwrap());
            for probe in [0, slot, SLOTS_PER_FSM_PAGE - 1] {
                assert_eq!(fsm_get_avail(v, probe), model[probe as usize]);
            }

            let min = (rng.next() % 256) as u8;
            let next = (rng.next() % (SLOTS_PER_FSM_PAGE as u64 + 7)) as i32;
            v.set_next_slot(next);
            let got = search(&mut page, min, false, true);
            assert_eq!(got, model_search(&model, next, min), "min={min} next={next}");
            if got != -1 {
                assert_eq!(view(&mut page).next_slot(), got);
            }
        }
    }
    assert!(!fsm_rebuild_page(view(&mut page)), "propagation left the tree inconsistent");
}

#[test]
fn search_advancenext_and_wraparound() {
    let _s = serial();
    let mut page = fsm_test_page();
    fsm_set_avail(view(&mut page), 3, 200);
    fsm_set_avail(view(&mut page), SLOTS_PER_FSM_PAGE - 1, 200);

    view(&mut page).set_next_slot(0);
    assert_eq!(search(&mut page, 100, true, true), 3);
    assert_eq!(view(&mut page).next_slot(), 4);

    assert_eq!(search(&mut page, 100, true, true), SLOTS_PER_FSM_PAGE - 1);
    assert_eq!(view(&mut page).next_slot(), SLOTS_PER_FSM_PAGE);
    // Out-of-range hint wraps to slot 0's side on the next call.
    assert_eq!(search(&mut page, 100, true, true), 3);

    assert_eq!(search(&mut page, 201, false, true), -1);
}

#[test]
fn search_torn_page_repairs_under_exclusive_and_restarts() {
    let _s = serial();
    let mut page = fsm_test_page();
    fsm_set_avail(view(&mut page), 7, 90);
    // Corrupt one mid-level node on the path to slot 7 to promise 255.
    let mut nodeno = NON_LEAF_NODES_PER_PAGE + 7;
    for _ in 0..3 {
        nodeno = (nodeno - 1) / 2;
    }
    view(&mut page).set_node(nodeno, 255);
    view(&mut page).set_node(0, 255);
    view(&mut page).set_next_slot(0);

    RELOCKS.store(0, Relaxed);
    DIRTY_HINTS.store(0, Relaxed);
    // Shared-lock caller: the repair relocks exclusive, rebuilds, restarts,
    // and still finds the genuine slot.
    assert_eq!(search(&mut page, 200, false, false), -1);
    assert!(RELOCKS.load(Relaxed) >= 1, "repair did not take the exclusive lock");
    assert!(DIRTY_HINTS.load(Relaxed) >= 1);
    assert_eq!(view(&mut page).node(0), 90, "rebuild did not fix the root");
    assert_eq!(search(&mut page, 90, false, true), 7);
}

#[test]
fn truncate_avail_clears_tail() {
    let _s = serial();
    let mut page = fsm_test_page();
    for slot in 0..SLOTS_PER_FSM_PAGE {
        fsm_set_avail(view(&mut page), slot, 10);
    }
    fsm_set_avail(view(&mut page), 5, 250);
    fsm_set_avail(view(&mut page), 100, 251);

    assert!(fsm_truncate_avail(view(&mut page), 100));
    let v = view(&mut page);
    assert_eq!(fsm_get_avail(v, 99), 10);
    assert_eq!(fsm_get_avail(v, 100), 0);
    assert_eq!(fsm_get_avail(v, SLOTS_PER_FSM_PAGE - 1), 0);
    assert_eq!(fsm_get_max_avail(v), 250);
    assert!(!fsm_truncate_avail(view(&mut page), 100));
}

#[test]
fn category_math_matches_c() {
    assert_eq!(fsm_space_avail_to_cat(0), 0);
    assert_eq!(fsm_space_avail_to_cat(31), 0);
    assert_eq!(fsm_space_avail_to_cat(32), 1);
    assert_eq!(fsm_space_avail_to_cat(8127), 253);
    assert_eq!(fsm_space_avail_to_cat(8128), 254);
    assert_eq!(fsm_space_avail_to_cat(8159), 254);
    assert_eq!(fsm_space_avail_to_cat(8160), 255);
    assert_eq!(fsm_space_avail_to_cat(8191), 255);

    assert_eq!(fsm_space_cat_to_avail(0), 0);
    assert_eq!(fsm_space_cat_to_avail(1), 32);
    assert_eq!(fsm_space_cat_to_avail(254), 8128);
    assert_eq!(fsm_space_cat_to_avail(255), 8160);

    assert_eq!(fsm_space_needed_to_cat(0).unwrap(), 1);
    assert_eq!(fsm_space_needed_to_cat(1).unwrap(), 1);
    assert_eq!(fsm_space_needed_to_cat(32).unwrap(), 1);
    assert_eq!(fsm_space_needed_to_cat(33).unwrap(), 2);
    assert_eq!(fsm_space_needed_to_cat(8128).unwrap(), 254);
    assert_eq!(fsm_space_needed_to_cat(8129).unwrap(), 255);
    assert_eq!(fsm_space_needed_to_cat(8160).unwrap(), 255);
    let err = fsm_space_needed_to_cat(8161).unwrap_err();
    assert!(err.message().contains("invalid FSM request size 8161"), "{err:?}");

    // avail_to_cat rounds down: the represented lower bound never overstates.
    for avail in [0usize, 1, 31, 32, 4000, 8128, 8159, 8160, 8191] {
        assert!(
            fsm_space_cat_to_avail(fsm_space_avail_to_cat(avail)) <= avail,
            "avail {avail}"
        );
    }
}

#[test]
fn logical_to_physical_fixtures() {
    let a = |level, logpageno| FSMAddress { level, logpageno };
    assert_eq!(fsm_logical_to_physical(a(2, 0)), 0);
    assert_eq!(fsm_logical_to_physical(a(1, 0)), 1);
    assert_eq!(fsm_logical_to_physical(a(0, 0)), 2);
    assert_eq!(fsm_logical_to_physical(a(0, 1)), 3);
    assert_eq!(fsm_logical_to_physical(a(0, 4068)), 4070);
    assert_eq!(fsm_logical_to_physical(a(1, 1)), 4071);
    assert_eq!(fsm_logical_to_physical(a(0, 4069)), 4072);
}

#[test]
fn physical_addresses_are_dense_dfs() {
    // The physical layout is depth-first: root, then per level-1 subtree the
    // level-1 page followed by its leaves. Two full subtrees = a dense 0..n.
    let mut phys = Vec::new();
    phys.push(fsm_logical_to_physical(FSM_ROOT_ADDRESS));
    for l1 in 0..2 {
        phys.push(fsm_logical_to_physical(FSMAddress { level: 1, logpageno: l1 }));
        for leaf in 0..SLOTS_PER_FSM_PAGE {
            phys.push(fsm_logical_to_physical(FSMAddress {
                level: 0,
                logpageno: l1 * SLOTS_PER_FSM_PAGE + leaf,
            }));
        }
    }
    let mut sorted = phys.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), phys.len(), "duplicate physical blocks");
    assert_eq!(sorted, (0..phys.len() as BlockNumber).collect::<Vec<_>>());
}

#[test]
fn addressing_roundtrips() {
    for heapblk in [0u32, 1, 4068, 4069, 4070, 16_555_961, 4_294_967_294] {
        let (addr, slot) = fsm_get_location(heapblk);
        assert_eq!(addr.level, FSM_BOTTOM_LEVEL);
        assert_eq!(fsm_get_heap_blk(addr, slot), heapblk);

        let (parent, pslot) = fsm_get_parent(addr);
        assert_eq!(fsm_get_child(parent, pslot), addr);
        let (root, rslot) = fsm_get_parent(parent);
        assert_eq!(root.level, FSM_ROOT_LEVEL);
        assert_eq!(root.logpageno, 0);
        assert_eq!(fsm_get_child(root, rslot), parent);
    }
}
