//! Logic tests for the in-crate (no genuine-external) GIN routines. Functions
//! that cross a seam (the array extract functions and the validator) are
//! exercised end-to-end only once their owner subsystems land; here we cover the
//! pure-logic surface, plus the ternary-logic shims via installed test seams.

use ::tsearch::gin::{GIN_FALSE, GIN_MAYBE, GIN_SEARCH_MODE_EVERYTHING, GIN_TRUE};
use ::types_tuple::heaptuple::ItemPointerData;

// ---------------------------------------------------------------------------
// ginarrayproc: consistent / triConsistent
// ---------------------------------------------------------------------------

use crate::ginarrayproc::{
    ginarrayconsistent, ginarraytriconsistent, GinContainedStrategy, GinContainsStrategy,
    GinEqualStrategy, GinOverlapStrategy,
};

#[test]
fn array_consistent_overlap() {
    let mut recheck = true;
    // one non-null match -> true, not lossy.
    let r = ginarrayconsistent(&[false, true], GinOverlapStrategy, 2, &[false, false], &mut recheck)
        .unwrap();
    assert!(r);
    assert!(!recheck);
    // the only matching key is null -> no match.
    let mut recheck2 = true;
    let r2 =
        ginarrayconsistent(&[true], GinOverlapStrategy, 1, &[true], &mut recheck2).unwrap();
    assert!(!r2);
}

#[test]
fn array_consistent_contains() {
    let mut recheck = true;
    // all present, no nulls -> true, not lossy.
    let r = ginarrayconsistent(&[true, true], GinContainsStrategy, 2, &[false, false], &mut recheck)
        .unwrap();
    assert!(r);
    assert!(!recheck);
    // a null key fails contains.
    let mut recheck2 = true;
    let r2 = ginarrayconsistent(&[true, true], GinContainsStrategy, 2, &[false, true], &mut recheck2)
        .unwrap();
    assert!(!r2);
}

#[test]
fn array_consistent_contained_and_equal_force_recheck() {
    let mut recheck = false;
    let r = ginarrayconsistent(&[false], GinContainedStrategy, 1, &[false], &mut recheck).unwrap();
    assert!(r);
    assert!(recheck);

    let mut recheck2 = false;
    // equal: all true, nulls ignored.
    let r2 = ginarrayconsistent(&[true, true], GinEqualStrategy, 2, &[true, false], &mut recheck2)
        .unwrap();
    assert!(r2);
    assert!(recheck2);
    let mut recheck3 = false;
    let r3 = ginarrayconsistent(&[true, false], GinEqualStrategy, 2, &[false, false], &mut recheck3)
        .unwrap();
    assert!(!r3);
}

#[test]
fn array_consistent_unknown_strategy_errors() {
    let mut recheck = false;
    assert!(ginarrayconsistent(&[true], 99, 1, &[false], &mut recheck).is_err());
}

#[test]
fn array_tri_consistent_overlap() {
    // A non-null TRUE => GIN_TRUE.
    let r = ginarraytriconsistent(&[GIN_FALSE, GIN_TRUE], GinOverlapStrategy, 2, &[false, false])
        .unwrap();
    assert_eq!(r, GIN_TRUE);
    // Only MAYBE among non-nulls => GIN_MAYBE.
    let r2 = ginarraytriconsistent(&[GIN_MAYBE], GinOverlapStrategy, 1, &[false]).unwrap();
    assert_eq!(r2, GIN_MAYBE);
    // The MAYBE is null => stays GIN_FALSE.
    let r3 = ginarraytriconsistent(&[GIN_MAYBE], GinOverlapStrategy, 1, &[true]).unwrap();
    assert_eq!(r3, GIN_FALSE);
}

#[test]
fn array_tri_consistent_contains() {
    // A FALSE or a null => GIN_FALSE.
    let r = ginarraytriconsistent(&[GIN_TRUE, GIN_FALSE], GinContainsStrategy, 2, &[false, false])
        .unwrap();
    assert_eq!(r, GIN_FALSE);
    // A MAYBE (no FALSE/null) => GIN_MAYBE.
    let r2 = ginarraytriconsistent(&[GIN_TRUE, GIN_MAYBE], GinContainsStrategy, 2, &[false, false])
        .unwrap();
    assert_eq!(r2, GIN_MAYBE);
    // All TRUE, no nulls => GIN_TRUE.
    let r3 = ginarraytriconsistent(&[GIN_TRUE, GIN_TRUE], GinContainsStrategy, 2, &[false, false])
        .unwrap();
    assert_eq!(r3, GIN_TRUE);
}

#[test]
fn array_tri_consistent_contained_is_maybe_and_equal() {
    assert_eq!(
        ginarraytriconsistent(&[GIN_TRUE], GinContainedStrategy, 1, &[false]).unwrap(),
        GIN_MAYBE
    );
    // equal: a FALSE => GIN_FALSE; otherwise GIN_MAYBE.
    assert_eq!(
        ginarraytriconsistent(&[GIN_TRUE, GIN_FALSE], GinEqualStrategy, 2, &[false, false]).unwrap(),
        GIN_FALSE
    );
    assert_eq!(
        ginarraytriconsistent(&[GIN_TRUE, GIN_TRUE], GinEqualStrategy, 2, &[false, false]).unwrap(),
        GIN_MAYBE
    );
}

// ---------------------------------------------------------------------------
// ginpostinglist: codec round-trips and merge
// ---------------------------------------------------------------------------

use crate::ginpostinglist::{
    ginCompareItemPointers, ginCompressPostingList, ginMergeItemPointers,
    ginPostingListDecode, ginPostingListDecodeAllSegments,
};

fn ip(blk: u32, off: u16) -> ItemPointerData {
    ItemPointerData::new(blk, off)
}

#[test]
fn compress_decode_round_trip_single_segment() {
    let items = [ip(0, 1), ip(0, 5), ip(3, 2), ip(100, 7), ip(100, 8)];
    let pl = ginCompressPostingList(&items, items.len() as i32, 1024, None);
    // first is preserved; size is short-aligned.
    assert_eq!(pl.first(), items[0]);
    assert_eq!(pl.size() % 2, 0);

    let mut ndec = 0;
    let decoded = ginPostingListDecode(&pl.bytes, Some(&mut ndec));
    assert_eq!(ndec as usize, items.len());
    assert_eq!(decoded, items);
}

#[test]
fn compress_respects_maxsize_via_nwritten() {
    // Many widely-spaced items but a tiny maxsize: only a prefix fits.
    let items: Vec<ItemPointerData> = (0..50).map(|i| ip(i * 1000, 1)).collect();
    let mut nwritten = 0;
    let pl = ginCompressPostingList(&items, items.len() as i32, 16, Some(&mut nwritten));
    assert!(nwritten >= 1 && (nwritten as usize) < items.len());
    assert!(pl.size() <= 16);
    // Decoding the truncated list yields exactly the first `nwritten` items.
    let decoded = ginPostingListDecode(&pl.bytes, None);
    assert_eq!(decoded.len(), nwritten as usize);
    assert_eq!(decoded[..], items[..nwritten as usize]);
}

#[test]
fn decode_all_segments_walks_concatenated_lists() {
    let a = [ip(0, 1), ip(0, 9)];
    let b = [ip(10, 1), ip(10, 2), ip(11, 1)];
    let pla = ginCompressPostingList(&a, a.len() as i32, 1024, None);
    let plb = ginCompressPostingList(&b, b.len() as i32, 1024, None);
    let mut buf = pla.bytes.clone();
    buf.extend_from_slice(&plb.bytes);

    let mut ndec = 0;
    let decoded = ginPostingListDecodeAllSegments(&buf, buf.len() as i32, Some(&mut ndec));
    let expected: Vec<ItemPointerData> = a.iter().chain(b.iter()).copied().collect();
    assert_eq!(ndec as usize, expected.len());
    assert_eq!(decoded, expected);
}

#[test]
fn compare_item_pointers_orders_by_block_then_offset() {
    assert!(ginCompareItemPointers(&ip(1, 2), &ip(1, 3)) < 0);
    assert!(ginCompareItemPointers(&ip(2, 1), &ip(1, 9)) > 0);
    assert_eq!(ginCompareItemPointers(&ip(5, 5), &ip(5, 5)), 0);
}

#[test]
fn merge_item_pointers_disjoint_and_overlapping() {
    // Disjoint, a before b.
    let a = [ip(0, 1), ip(0, 2)];
    let b = [ip(1, 1), ip(1, 2)];
    let mut nm = 0;
    let m = ginMergeItemPointers(&a, a.len() as u32, &b, b.len() as u32, &mut nm);
    assert_eq!(nm, 4);
    assert_eq!(m, [ip(0, 1), ip(0, 2), ip(1, 1), ip(1, 2)]);

    // Disjoint, b before a (reversed append).
    let mut nm2 = 0;
    let m2 = ginMergeItemPointers(&b, b.len() as u32, &a, a.len() as u32, &mut nm2);
    assert_eq!(nm2, 4);
    assert_eq!(m2, [ip(0, 1), ip(0, 2), ip(1, 1), ip(1, 2)]);

    // Overlapping with a shared element -> dedup.
    let c = [ip(0, 1), ip(0, 3), ip(0, 5)];
    let d = [ip(0, 3), ip(0, 4)];
    let mut nm3 = 0;
    let m3 = ginMergeItemPointers(&c, c.len() as u32, &d, d.len() as u32, &mut nm3);
    assert_eq!(m3, [ip(0, 1), ip(0, 3), ip(0, 4), ip(0, 5)]);
    assert_eq!(nm3 as usize, m3.len());
}

// ---------------------------------------------------------------------------
// ginlogic: dummy fns, routing, and the shims via installed test seams
// ---------------------------------------------------------------------------

use crate::ginlogic::{
    callTriConsistentFn, directBoolConsistentFn, ginInitConsistentFunction, shimBoolConsistentFn,
    shimTriConsistentFn, trueConsistentFn, trueTriConsistentFn, GinState, MAX_MAYBE_ENTRIES,
};
use ::tsearch::backend_access_gin_ginlogic::{
    GinBoolConsistentKind, GinScanKey, GinTriConsistentKind,
};

use std::cell::RefCell;
use std::sync::Once;

type BoolHook = Box<dyn FnMut(&mut GinScanKey) -> bool>;
type TriHook = Box<dyn FnMut(&mut GinScanKey) -> ::tsearch::gin::GinTernaryValue>;

thread_local! {
    static BOOL_HOOK: RefCell<Option<BoolHook>> = const { RefCell::new(None) };
    static TRI_HOOK: RefCell<Option<TriHook>> = const { RefCell::new(None) };
}

static INSTALL: Once = Once::new();

fn install(boolfn: Option<BoolHook>, trifn: Option<TriHook>) {
    INSTALL.call_once(|| {
        core_probe_seams::gin_consistent_call_bool::set(|key| {
            BOOL_HOOK.with(|h| {
                let mut slot = h.borrow_mut();
                let f = slot.as_mut().expect("bool hook not set for this test/thread");
                f(key)
            })
        });
        core_probe_seams::gin_consistent_call_tri::set(|key| {
            TRI_HOOK.with(|h| {
                let mut slot = h.borrow_mut();
                let f = slot.as_mut().expect("tri hook not set for this test/thread");
                f(key)
            })
        });
    });
    BOOL_HOOK.with(|h| *h.borrow_mut() = boolfn);
    TRI_HOOK.with(|h| *h.borrow_mut() = trifn);
}

fn install_bool(f: fn(&[::tsearch::gin::GinTernaryValue]) -> (bool, bool)) {
    install(
        Some(Box::new(move |key| {
            let (m, r) = f(&key.entryRes);
            key.recheckCurItem = r;
            m
        })),
        None,
    );
}

fn install_tri(f: fn(&[::tsearch::gin::GinTernaryValue]) -> ::tsearch::gin::GinTernaryValue) {
    install(None, Some(Box::new(move |key| f(&key.entryRes))));
}

#[test]
fn true_consistent_clears_recheck_and_tri_is_true() {
    let mut key = GinScanKey::from_entry_res(vec![GIN_MAYBE]);
    key.recheckCurItem = true;
    assert!(trueConsistentFn(&mut key));
    assert!(!key.recheckCurItem);
    assert_eq!(trueTriConsistentFn(&mut key), GIN_TRUE);
}

#[test]
fn direct_bool_presets_recheck_then_calls() {
    install_bool(|_| (true, false));
    let mut key = GinScanKey::from_entry_res(vec![GIN_TRUE]);
    assert!(directBoolConsistentFn(&mut key));
    assert!(!key.recheckCurItem);

    // A bool fn that leaves recheck untouched keeps the pre-set true.
    install(Some(Box::new(|_| true)), None);
    let mut key2 = GinScanKey::from_entry_res(vec![GIN_TRUE]);
    assert!(directBoolConsistentFn(&mut key2));
    assert!(key2.recheckCurItem);
}

#[test]
fn shim_bool_maps_maybe_to_true_recheck() {
    install_tri(|_| GIN_MAYBE);
    let mut key = GinScanKey::from_entry_res(vec![GIN_MAYBE]);
    assert!(shimBoolConsistentFn(&mut key));
    assert!(key.recheckCurItem);

    install_tri(|_| GIN_TRUE);
    let mut key_t = GinScanKey::from_entry_res(vec![GIN_TRUE]);
    assert!(shimBoolConsistentFn(&mut key_t));
    assert!(!key_t.recheckCurItem);

    install_tri(|_| GIN_FALSE);
    let mut key_f = GinScanKey::from_entry_res(vec![GIN_FALSE]);
    assert!(!shimBoolConsistentFn(&mut key_f));
    assert!(!key_f.recheckCurItem);
}

#[test]
fn shim_tri_no_maybe_calls_direct() {
    install_bool(|e| (e[0] == GIN_TRUE, false));
    let mut key = GinScanKey::from_entry_res(vec![GIN_TRUE]);
    assert_eq!(shimTriConsistentFn(&mut key), GIN_TRUE);
    let mut key0 = GinScanKey::from_entry_res(vec![GIN_FALSE]);
    assert_eq!(shimTriConsistentFn(&mut key0), GIN_FALSE);
}

#[test]
fn shim_tri_too_many_maybe_returns_maybe() {
    install_bool(|_| (true, false));
    let mut key = GinScanKey::from_entry_res(vec![GIN_MAYBE; MAX_MAYBE_ENTRIES + 1]);
    assert_eq!(shimTriConsistentFn(&mut key), GIN_MAYBE);
    assert!(key.entryRes.iter().all(|&v| v == GIN_MAYBE));
}

#[test]
fn shim_tri_combinations_agree_disagree_and_recheck() {
    // All combinations agree TRUE, no recheck -> TRUE.
    install_bool(|_| (true, false));
    let mut key = GinScanKey::from_entry_res(vec![GIN_MAYBE, GIN_MAYBE]);
    assert_eq!(shimTriConsistentFn(&mut key), GIN_TRUE);
    assert_eq!(key.entryRes, vec![GIN_MAYBE, GIN_MAYBE]); // restored

    // Disagreement -> MAYBE.
    install_bool(|e| (e[0] == GIN_TRUE, false));
    let mut key2 = GinScanKey::from_entry_res(vec![GIN_MAYBE]);
    assert_eq!(shimTriConsistentFn(&mut key2), GIN_MAYBE);
    assert_eq!(key2.entryRes, vec![GIN_MAYBE]);

    // TRUE with recheck collapses to MAYBE.
    install_bool(|_| (true, true));
    let mut key3 = GinScanKey::from_entry_res(vec![GIN_MAYBE]);
    assert_eq!(shimTriConsistentFn(&mut key3), GIN_MAYBE);
}

#[test]
fn init_everything_uses_true_fns() {
    let gs = GinState::new();
    let mut key = GinScanKey::from_entry_res(vec![GIN_TRUE]);
    key.searchMode = GIN_SEARCH_MODE_EVERYTHING;
    ginInitConsistentFunction(&gs, &mut key);
    assert_eq!(key.boolConsistentFn, GinBoolConsistentKind::True);
    assert_eq!(key.triConsistentFn, GinTriConsistentKind::True);
}

#[test]
fn init_selects_direct_vs_shim_per_oid() {
    let mut gs = GinState::new();
    // attno 0: opclass provides a boolean consistent fn but no ternary one.
    gs.consistentFn[0].fn_oid = 1234;
    gs.triConsistentFn[0].fn_oid = types_core::InvalidOid;
    gs.supportCollation[0] = 100;
    let mut key = GinScanKey::from_entry_res(vec![GIN_TRUE]);
    key.attnum = 1;
    key.searchMode = 0;
    ginInitConsistentFunction(&gs, &mut key);
    assert_eq!(key.boolConsistentFn, GinBoolConsistentKind::Direct);
    assert_eq!(key.triConsistentFn, GinTriConsistentKind::Shim);
    assert_eq!(key.collation, 100);
    assert_eq!(key.consistent_fmgr_oid, 1234);

    // attno 1: opclass provides a ternary consistent fn but no boolean one.
    gs.consistentFn[1].fn_oid = types_core::InvalidOid;
    gs.triConsistentFn[1].fn_oid = 5678;
    gs.supportCollation[1] = 0;
    let mut key2 = GinScanKey::from_entry_res(vec![GIN_TRUE]);
    key2.attnum = 2;
    key2.searchMode = 0;
    ginInitConsistentFunction(&gs, &mut key2);
    assert_eq!(key2.boolConsistentFn, GinBoolConsistentKind::Shim);
    assert_eq!(key2.triConsistentFn, GinTriConsistentKind::Direct);
}

#[test]
fn dispatch_round_trips_through_tag() {
    install_tri(|_| GIN_TRUE);
    let mut key = GinScanKey::from_entry_res(vec![GIN_FALSE]);
    key.triConsistentFn = GinTriConsistentKind::Direct;
    assert_eq!(callTriConsistentFn(&mut key), GIN_TRUE);
}
