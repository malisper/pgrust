//! Unit-level checks for the index-AM dispatch layer. The `am*` callbacks and
//! the relcache/predicate/snapshot/pgstat seams are owned by unported crates
//! and panic until they land, so these tests exercise only the pure arithmetic
//! this layer owns.

use super::*;

#[test]
fn maxalign_rounds_up_to_8() {
    assert_eq!(maxalign(0), 0);
    assert_eq!(maxalign(1), 8);
    assert_eq!(maxalign(8), 8);
    assert_eq!(maxalign(9), 16);
}

#[test]
fn add_size_sums() {
    assert_eq!(add_size(3, 4), 7);
}

#[test]
fn parallel_header_is_two_locators_plus_two_sizes() {
    let expect = 2 * core::mem::size_of::<types_storage::RelFileLocator>()
        + 2 * core::mem::size_of::<usize>();
    assert_eq!(parallel_index_scan_desc_header_size(), expect);
}

#[test]
fn shared_instrument_header_is_aligned() {
    let h = shared_index_scan_instrumentation_header_size();
    let align = core::mem::align_of::<IndexScanInstrumentation>();
    assert_eq!(h % align, 0);
    assert!(h >= core::mem::size_of::<i32>());
}
