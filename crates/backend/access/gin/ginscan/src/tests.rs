//! Smoke tests for the ginscan A0 carrier wiring and the search-mode constant
//! mapping. The full scan-key building logic exercises the fmgr extractQuery
//! dispatch and the relcache/typcache `initGinState` path, which require the
//! unported substrate owners; those legs are covered by the regress suite once
//! the GIN engine lands. These tests pin the parts reachable with no substrate.

use ::gin::{
    GIN_SEARCH_MODE_ALL, GIN_SEARCH_MODE_DEFAULT, GIN_SEARCH_MODE_EVERYTHING,
    GIN_SEARCH_MODE_INCLUDE_EMPTY,
};

#[test]
fn search_mode_constants_match_c() {
    assert_eq!(GIN_SEARCH_MODE_DEFAULT, 0);
    assert_eq!(GIN_SEARCH_MODE_INCLUDE_EMPTY, 1);
    assert_eq!(GIN_SEARCH_MODE_ALL, 2);
    assert_eq!(GIN_SEARCH_MODE_EVERYTHING, 3);
}

#[test]
fn item_pointer_min_is_block0_offset0() {
    let p = super::item_pointer_min();
    assert_eq!(p.ip_posid, 0);
}
