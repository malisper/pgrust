//! Page views + the SWIZZLE METAMORPHIC suites (charter §5 M2-B:
//! "write/unload/reload/probe equivalence under permuted unload points").
//!
//! The metamorphic law: for any interleaving of unload/reload across the
//! row page and its var pages, `swizzle` resolves every ref to payload
//! bytes identical to what was written, and `unswizzle` restores the
//! at-rest page image BYTE-EXACTLY. Unload order, unload subset, and
//! reload order are all non-surfaces.

use std::sync::Arc;

use crate::page::{PageKind, RowLayout, VarRef};
use crate::pool::{PageId, SpillPool};
use crate::set::SpillFile;
use crate::SpillSet;

/// Deterministic permutation source (no rand dependency): an LCG-driven
/// Fisher–Yates.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0 >> 16
    }

    fn shuffle<T>(&mut self, v: &mut [T]) {
        for i in (1..v.len()).rev() {
            let j = (self.next() as usize) % (i + 1);
            v.swap(i, j);
        }
    }
}

fn payload(row: usize, slot: usize) -> Vec<u8> {
    let len = 1 + (row * 31 + slot * 7) % 197;
    (0..len).map(|k| ((row * 131 + slot * 17 + k) % 251) as u8).collect()
}

/// Build one row page whose rows reference payloads spread over `nvars`
/// var pages; returns (pool, row page, var pages, expected payloads per
/// (row, slot); None = NULL ref).
#[allow(clippy::type_complexity)]
fn build_state(
    set: &Arc<SpillSet>,
    budget_pages: usize,
    nrows: usize,
    nvars: usize,
) -> (SpillPool, PageId, Vec<PageId>, Vec<[Option<Vec<u8>>; 2]>) {
    let file = SpillFile::new(Arc::clone(set), "pool-meta".to_string());
    let mut pool = SpillPool::new(budget_pages * crate::PAGE_SIZE, file);
    let layout = RowLayout::new(32, &[8, 24]).unwrap();

    let row_pin = pool.alloc_row(&layout).unwrap();
    let row_id = row_pin.id();
    let mut var_ids: Vec<PageId> = Vec::new();
    for _ in 0..nvars {
        let pin = pool.alloc_var().unwrap();
        var_ids.push(pin.id());
        pool.unpin(pin);
    }

    let mut expected: Vec<[Option<Vec<u8>>; 2]> = Vec::new();
    for i in 0..nrows {
        let mut refs = [VarRef::NULL; 2];
        let mut exp: [Option<Vec<u8>>; 2] = [None, None];
        for slot in 0..2 {
            // Sprinkle NULLs (slot 1 of every third row).
            if slot == 1 && i % 3 == 0 {
                continue;
            }
            let p = payload(i, slot);
            let vp = var_ids[(i + slot) % nvars];
            let vpin = pool.pin(vp).unwrap();
            let off = pool
                .var_page_mut(&vpin)
                .try_append(&p)
                .expect("var page has room at this scale");
            pool.unpin(vpin);
            refs[slot] = VarRef::encode(vp.0, off);
            exp[slot] = Some(p);
        }
        let mut row = [0u8; 32];
        row[0..8].copy_from_slice(&(i as u64).to_ne_bytes());
        row[8..16].copy_from_slice(&refs[0].0.to_ne_bytes());
        row[24..32].copy_from_slice(&refs[1].0.to_ne_bytes());
        let mut rp = pool.row_page_mut(&row_pin);
        rp.try_push_row(&row).expect("row page has room at this scale");
        expected.push(exp);
    }
    pool.unpin(row_pin);
    (pool, row_id, var_ids, expected)
}

/// Verify every ref resolves to its expected payload under a live token.
fn verify_resolution(
    pool: &mut SpillPool,
    row_id: PageId,
    expected: &[[Option<Vec<u8>>; 2]],
) {
    let tok = pool.swizzle(row_id).unwrap();
    {
        let rpin = pool.pin(row_id).unwrap();
        let rp = pool.row_page(&rpin);
        assert_eq!(rp.count() as usize, expected.len());
        for (i, exp) in expected.iter().enumerate() {
            for slot in 0..2 {
                let w = rp.ref_word(i as u32, slot);
                match &exp[slot] {
                    None => assert!(w.is_null(), "row {i} slot {slot} should be NULL"),
                    Some(p) => {
                        assert!(!w.is_unswizzled(), "row {i} slot {slot} should be swizzled");
                        // SAFETY: inside the token scope; the target var
                        // page is pinned by the token.
                        let got = unsafe { w.payload() };
                        assert_eq!(got, &p[..], "row {i} slot {slot} payload");
                    }
                }
            }
        }
        pool.unpin(rpin);
    }
    pool.unswizzle(tok).unwrap();
}

/// Snapshot a page's at-rest bytes (must be resident + unswizzled).
fn snapshot(pool: &mut SpillPool, id: PageId) -> Vec<u8> {
    let pin = pool.pin(id).unwrap();
    let bytes = match pool.page_kind(id) {
        PageKind::Row => {
            let rp = pool.row_page(&pin);
            let mut v = Vec::new();
            for i in 0..rp.count() {
                v.extend_from_slice(rp.row(i));
            }
            v
        }
        PageKind::Var => Vec::new(), // var snapshots compared via payloads
    };
    pool.unpin(pin);
    bytes
}

#[test]
fn resolution_before_any_unload() {
    let (set, _dir, _cwd) = super::rig("pages-base");
    let (mut pool, row_id, _vars, expected) = build_state(&set, 64, 40, 3);
    verify_resolution(&mut pool, row_id, &expected);
}

#[test]
fn swizzle_unswizzle_restores_at_rest_bytes_exactly() {
    let (set, _dir, _cwd) = super::rig("pages-roundtrip");
    let (mut pool, row_id, _vars, expected) = build_state(&set, 64, 24, 2);
    let before = snapshot(&mut pool, row_id);
    verify_resolution(&mut pool, row_id, &expected);
    let after = snapshot(&mut pool, row_id);
    assert_eq!(before, after, "swizzle round-trip must restore at-rest bytes");
}

#[test]
fn metamorphic_permuted_unload_points() {
    let (set, _dir, _cwd) = super::rig("pages-metamorphic");
    let (mut pool, row_id, var_ids, expected) = build_state(&set, 64, 40, 4);
    let before = snapshot(&mut pool, row_id);

    let mut all_pages: Vec<PageId> = vec![row_id];
    all_pages.extend(var_ids.iter().copied());

    let mut lcg = Lcg(0x5EED_1234_ABCD_0001);
    for round in 0..12 {
        // Permuted unload ORDER over a permuted SUBSET size: rounds cycle
        // through unloading 1..=all pages, in shuffled order.
        let mut order = all_pages.clone();
        lcg.shuffle(&mut order);
        let k = 1 + (round % order.len());
        for id in &order[..k] {
            pool.unload(*id).unwrap();
            assert!(!pool.is_resident(*id));
        }
        // Probe equivalence: swizzle reloads exactly what it needs.
        verify_resolution(&mut pool, row_id, &expected);
        // Reload identity: the at-rest row image is byte-stable across
        // every unload/reload interleaving.
        let now = snapshot(&mut pool, row_id);
        assert_eq!(before, now, "round {round}: reload must be byte-identical");
    }
    let m = pool.metrics();
    assert!(m.pages_unloaded > 0 && m.pages_reloaded > 0, "the suite must exercise I/O");
}

#[test]
fn dirty_mutation_survives_unload_reload() {
    let (set, _dir, _cwd) = super::rig("pages-dirty");
    let (mut pool, row_id, _vars, expected) = build_state(&set, 64, 10, 2);

    // Mutate a non-ref field through the dirty-marking view.
    let pin = pool.pin(row_id).unwrap();
    pool.row_page_mut(&pin).row_mut(4)[0..8].copy_from_slice(&0xFEED_FACE_u64.to_ne_bytes());
    pool.unpin(pin);

    pool.unload(row_id).unwrap();
    let pin = pool.pin(row_id).unwrap();
    let got = u64::from_ne_bytes(pool.row_page(&pin).row(4)[0..8].try_into().unwrap());
    pool.unpin(pin);
    assert_eq!(got, 0xFEED_FACE);

    // Refs are untouched by the field mutation.
    verify_resolution(&mut pool, row_id, &expected);
}

#[test]
fn page_capacity_boundaries() {
    let (set, _dir, _cwd) = super::rig("pages-capacity");
    let file = SpillFile::new(Arc::clone(&set), "pool-cap".to_string());
    let mut pool = SpillPool::new(64 * crate::PAGE_SIZE, file);

    // Row page: exactly rows_per_page rows, then None.
    let layout = RowLayout::new(64, &[]).unwrap();
    let pin = pool.alloc_row(&layout).unwrap();
    let cap = layout.rows_per_page();
    {
        let mut rp = pool.row_page_mut(&pin);
        let row = [7u8; 64];
        for i in 0..cap {
            assert_eq!(rp.try_push_row(&row), Some(i));
        }
        assert_eq!(rp.try_push_row(&row), None, "page-full must be a rotation signal");
    }
    pool.unpin(pin);

    // Var page: fill until None, verify every stored cell.
    let vpin = pool.alloc_var().unwrap();
    let mut cells: Vec<(u32, Vec<u8>)> = Vec::new();
    {
        let mut vp = pool.var_page_mut(&vpin);
        let mut i = 0usize;
        loop {
            let p = payload(i, 0);
            match vp.try_append(&p) {
                Some(off) => cells.push((off, p)),
                None => break,
            }
            i += 1;
        }
    }
    assert!(cells.len() > 300, "64K page should hold many small cells");
    {
        let vp = pool.var_page(&vpin);
        for (off, p) in &cells {
            assert_eq!(vp.get(*off), &p[..]);
        }
    }
    pool.unpin(vpin);
}

#[test]
#[should_panic(expected = "out of bounds")]
fn stale_var_offset_fails_loudly() {
    let (set, _dir, _cwd) = super::rig("pages-stale");
    let file = SpillFile::new(Arc::clone(&set), "pool-stale".to_string());
    let mut pool = SpillPool::new(64 * crate::PAGE_SIZE, file);
    let vpin = pool.alloc_var().unwrap();
    let _ = pool.var_page(&vpin).get(40_000); // nothing appended there
}
