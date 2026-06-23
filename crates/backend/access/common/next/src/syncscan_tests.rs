//! Unit tests for the Sync Scan LRU, pinned to PostgreSQL 18.3's
//! `access/common/syncscan.c` (`ss_search`, `syncscan.c:190-241`). The LRU is
//! fully in-crate and deterministic; these replay the exact state transitions
//! the C body performs, computed by hand from the C source.

use super::syncscan::*;

// `ss_search`/`ss_scan_locations_t` are crate-private; re-expose just enough for
// the tests through this module's parent visibility.
use crate::syncscan::test_support::{fresh_locations, search};
use ::types_core::primitive::{BlockNumber, InvalidBlockNumber, InvalidOid};

#[test]
fn report_interval_matches_c() {
    // 128 * 1024 / 8192 = 16.
    assert_eq!(SYNC_SCAN_REPORT_INTERVAL, 16);
}

#[test]
fn shmem_size_is_c_abi_footprint() {
    // offsetof(items)=16 + 20 * sizeof(ss_lru_item_t)=32  => 16 + 640 = 656.
    assert_eq!(sync_scan_shmem_size().unwrap(), 16 + 20 * 32);
}

#[test]
fn fresh_lru_is_all_invalid_and_linked() {
    let locs = fresh_locations();
    assert_eq!(locs.head, 0);
    assert_eq!(locs.tail, SYNC_SCAN_NELEM - 1);
    for i in 0..SYNC_SCAN_NELEM {
        assert_eq!(locs.items[i].location.relid, InvalidOid);
        assert_eq!(locs.items[i].location.location, InvalidBlockNumber);
        assert_eq!(locs.items[i].prev, if i > 0 { Some(i - 1) } else { None });
        assert_eq!(
            locs.items[i].next,
            if i < SYNC_SCAN_NELEM - 1 { Some(i + 1) } else { None }
        );
    }
}

#[test]
fn first_lookup_takes_over_tail_and_promotes_to_head() {
    let mut locs = fresh_locations();
    // No entry for relid 42: take over the last entry, set location, move to
    // the front. set=false, so the taken-over entry's location is `location`
    // arg (0) per the C "if (!match)" branch.
    let got = search(&mut locs, 42, 0, false);
    assert_eq!(got, 0);
    assert_eq!(locs.head, SYNC_SCAN_NELEM - 1); // old tail became head
    assert_eq!(locs.items[locs.head].location.relid, 42);
}

#[test]
fn report_then_get_roundtrips_the_location() {
    let mut locs = fresh_locations();
    // ss_report_location path: set=true updates the location.
    let _ = search(&mut locs, 7, 128, true);
    // ss_get_location path: set=false returns the stored location.
    let got: BlockNumber = search(&mut locs, 7, 0, false);
    assert_eq!(got, 128);
}

#[test]
fn distinct_relations_each_get_their_own_slot() {
    let mut locs = fresh_locations();
    let _ = search(&mut locs, 1, 10, true);
    let _ = search(&mut locs, 2, 20, true);
    let _ = search(&mut locs, 3, 30, true);
    assert_eq!(search(&mut locs, 1, 0, false), 10);
    assert_eq!(search(&mut locs, 2, 0, false), 20);
    assert_eq!(search(&mut locs, 3, 0, false), 30);
}
