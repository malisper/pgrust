//! Pool laws: budget floor, eviction, all-pinned exhaustion, jumbo
//! overshoot, stable-slot in-place rewrite, fail-closed reload.

use std::sync::Arc;

use crate::page::{PageKind, RowLayout, PAGE_SIZE};
use crate::pool::{PageId, SpillPool};
use crate::set::SpillFile;
use crate::SpillSet;

fn pool_with_budget(set: &Arc<SpillSet>, name: &str, budget: usize) -> SpillPool {
    SpillPool::new(budget, SpillFile::new(Arc::clone(set), name.to_string()))
}

#[test]
fn budget_floor_is_two_pages() {
    let (set, _dir, _cwd) = super::rig("pool-floor");
    let pool = pool_with_budget(&set, "floor", 0);
    assert_eq!(pool.budget_bytes(), 2 * PAGE_SIZE);
    let pool = pool_with_budget(&set, "floor2", 3 * PAGE_SIZE);
    assert_eq!(pool.budget_bytes(), 3 * PAGE_SIZE);
}

#[test]
fn eviction_keeps_resident_under_budget() {
    let (set, _dir, _cwd) = super::rig("pool-evict");
    let mut pool = pool_with_budget(&set, "evict", 2 * PAGE_SIZE);
    let layout = RowLayout::new(16, &[]).unwrap();

    // Alloc 5 pages, unpinning each: eviction must hold residency ≤ 2.
    let mut ids: Vec<PageId> = Vec::new();
    for i in 0..5 {
        let pin = pool.alloc_row(&layout).unwrap();
        let mut rp = pool.row_page_mut(&pin);
        let mut row = [0u8; 16];
        row[0..8].copy_from_slice(&(i as u64).to_ne_bytes());
        rp.try_push_row(&row).unwrap();
        ids.push(pin.id());
        pool.unpin(pin);
        assert!(pool.resident_bytes() <= pool.budget_bytes());
    }
    let m = pool.metrics();
    assert!(m.pool_evictions >= 3, "evictions: {}", m.pool_evictions);
    assert!(m.pages_unloaded >= 3);

    // Every page reloads with its content intact.
    for (i, id) in ids.iter().enumerate() {
        let pin = pool.pin(*id).unwrap();
        let got = u64::from_ne_bytes(pool.row_page(&pin).row(0)[0..8].try_into().unwrap());
        assert_eq!(got, i as u64);
        pool.unpin(pin);
    }
    assert!(pool.metrics().pages_reloaded >= 3);
}

#[test]
fn all_pinned_exhaustion_is_a_typed_error() {
    let (set, _dir, _cwd) = super::rig("pool-exhaust");
    let mut pool = pool_with_budget(&set, "exhaust", 2 * PAGE_SIZE);
    let layout = RowLayout::new(16, &[]).unwrap();
    let p1 = pool.alloc_row(&layout).unwrap();
    let p2 = pool.alloc_var().unwrap();
    // Both frames pinned: a third demand has nothing to evict.
    let err = pool.alloc_var().unwrap_err();
    let msg = format!("{err:?}");
    assert!(msg.contains("pool exhausted"), "unexpected error: {msg}");
    pool.unpin(p1);
    pool.unpin(p2);
    // With a pin returned the same demand succeeds (evicts the unpinned).
    let p3 = pool.alloc_var().unwrap();
    pool.unpin(p3);
}

#[test]
fn stable_slots_rewrite_in_place() {
    let (set, _dir, _cwd) = super::rig("pool-inplace");
    let mut pool = pool_with_budget(&set, "inplace", 4 * PAGE_SIZE);
    let layout = RowLayout::new(16, &[]).unwrap();
    let pin = pool.alloc_row(&layout).unwrap();
    let id = pin.id();
    {
        let mut rp = pool.row_page_mut(&pin);
        rp.try_push_row(&[1u8; 16]).unwrap();
    }
    pool.unpin(pin);
    pool.unload(id).unwrap();

    // The pool file's segment holds exactly one page.
    let seg = find_pool_seg("inplace");
    assert_eq!(std::fs::metadata(&seg).unwrap().len(), PAGE_SIZE as u64);

    // Mutate + unload again: same slot, same physical size (in-place, no
    // growth — temp_file_limit sees each page once).
    let pin = pool.pin(id).unwrap();
    pool.row_page_mut(&pin).row_mut(0)[0] = 9;
    pool.unpin(pin);
    pool.unload(id).unwrap();
    assert_eq!(std::fs::metadata(&seg).unwrap().len(), PAGE_SIZE as u64);

    // The mutation persisted through the rewrite.
    let pin = pool.pin(id).unwrap();
    assert_eq!(pool.row_page(&pin).row(0)[0], 9);
    pool.unpin(pin);

    // A clean page re-unloads WITHOUT a write (pages_unloaded stable).
    let unloads_before = pool.metrics().pages_unloaded;
    pool.unload(id).unwrap();
    assert_eq!(pool.metrics().pages_unloaded, unloads_before, "clean page must not rewrite");
}

#[test]
fn jumbo_var_page_roundtrip_and_cap() {
    let (set, _dir, _cwd) = super::rig("pool-jumbo");
    let mut pool = pool_with_budget(&set, "jumbo", 8 * PAGE_SIZE);

    // A payload larger than a standard page.
    let big: Vec<u8> = (0..PAGE_SIZE * 2 + 12345).map(|i| (i % 251) as u8).collect();
    let pin = pool.alloc_var_for(big.len()).unwrap();
    let id = pin.id();
    assert_eq!(pool.page_kind(id), PageKind::Var);
    let off = pool.var_page_mut(&pin).try_append(&big).expect("jumbo frame fits its value");
    pool.unpin(pin);

    pool.unload(id).unwrap();
    let pin = pool.pin(id).unwrap();
    assert_eq!(pool.var_page(&pin).get(off), &big[..]);
    pool.unpin(pin);

    // The cap is MaxAllocSize-parity: past it is a typed error.
    assert!(pool.alloc_var_for(usize::MAX / 2).is_err());
}

#[test]
fn jumbo_overshoot_is_witnessed_not_refused() {
    let (set, _dir, _cwd) = super::rig("pool-overshoot");
    // Budget at the floor; the jumbo alone exceeds it.
    let mut pool = pool_with_budget(&set, "overshoot", 0);
    assert_eq!(pool.budget_bytes(), 2 * PAGE_SIZE);
    let need = 5 * PAGE_SIZE;
    let pin = pool.alloc_var_for(need - 64).unwrap();
    assert!(pool.resident_bytes() >= need, "overshoot proceeds");
    assert!(pool.metrics().peak_resident_bytes >= need as u64, "and is witnessed");
    pool.unpin(pin);
}

#[test]
fn reload_validation_fails_closed_on_torn_bytes() {
    let (set, _dir, _cwd) = super::rig("pool-torn");
    let mut pool = pool_with_budget(&set, "torn", 4 * PAGE_SIZE);
    let layout = RowLayout::new(16, &[]).unwrap();
    let pin = pool.alloc_row(&layout).unwrap();
    let id = pin.id();
    pool.row_page_mut(&pin).try_push_row(&[3u8; 16]).unwrap();
    pool.unpin(pin);
    pool.unload(id).unwrap();

    // Corrupt the on-disk header magic.
    let seg = find_pool_seg("torn");
    let mut bytes = std::fs::read(&seg).unwrap();
    bytes[0] ^= 0xFF;
    std::fs::write(&seg, &bytes).unwrap();

    let err = pool.pin(id).unwrap_err();
    let msg = format!("{err:?}");
    assert!(msg.contains("invalid spill page header"), "unexpected error: {msg}");
    // Fail-closed reload left the page unloaded, the pool coherent.
    assert!(!pool.is_resident(id));
    assert_eq!(pool.live_pins(), 0);
}

#[test]
fn unload_refuses_pinned_pages() {
    let (set, _dir, _cwd) = super::rig("pool-unload-law");
    let mut pool = pool_with_budget(&set, "unload-law", 4 * PAGE_SIZE);
    let pin = pool.alloc_var().unwrap();
    let id = pin.id();
    let err = pool.unload(id).unwrap_err();
    assert!(format!("{err:?}").contains("staging-point law"));
    pool.unpin(pin);
    pool.unload(id).unwrap();
    // Idempotent on an already-unloaded page.
    pool.unload(id).unwrap();
}

/// Locate the single fileset dir and return the path of pool file
/// `<name>`'s first segment.
fn find_pool_seg(name: &str) -> String {
    for e in super::tmp_entries() {
        if e.ends_with(".fileset") {
            let dir = format!("{}/{e}", super::TMP_DIR);
            return format!("{dir}/{name}.0");
        }
    }
    panic!("no fileset dir under {}", super::TMP_DIR);
}
