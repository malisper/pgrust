//! The byref enumeration belt (§7.1 law 2; the 9-incident class): every
//! byref state class this crate can strand is enumerated with a declared
//! teardown home, the exhaustive match makes a NEW class a compile error
//! until it declares one, and the Send/Sync surface matches the contract
//! (plain data crosses threads; raw-address holders do not).

use crate::{byref_teardown_home, ByrefClass, SpillMetrics};

/// The documented class list — must stay in lockstep with the crate-doc
/// enumeration AND the match in `byref_teardown_home` (which the compiler
/// keeps exhaustive).
const ALL: [ByrefClass; 6] = [
    ByrefClass::PagePin,
    ByrefClass::SwizzledRefs,
    ByrefClass::OpenHandle,
    ByrefClass::OnDiskBytes,
    ByrefClass::PlainDirectory,
    ByrefClass::StrView,
];

/// COMPILE-forced census completeness (M4-N hardening): the belt forced a
/// home for every variant but nothing forced THIS census's `ALL` array to
/// grow with the enum — a new class could silently escape the census.
/// This match is the tooth: adding a variant fails compile here until the
/// census counts it.
fn census_counts(c: ByrefClass) -> () {
    match c {
        ByrefClass::PagePin
        | ByrefClass::SwizzledRefs
        | ByrefClass::OpenHandle
        | ByrefClass::OnDiskBytes
        | ByrefClass::PlainDirectory
        | ByrefClass::StrView => (),
    }
}

#[test]
fn every_class_declares_a_teardown_home() {
    for c in ALL {
        let home = byref_teardown_home(c);
        assert!(!home.is_empty(), "{c:?} must declare a home");
        census_counts(c);
    }
    // The scoped classes name their scope; the engagement-scoped class
    // names the reaper; the plain class names fail-closed opens; the
    // string-view class names the claim scope and the never-serialized law.
    assert!(byref_teardown_home(ByrefClass::PagePin).contains("unpin"));
    assert!(byref_teardown_home(ByrefClass::SwizzledRefs).contains("unswizzle"));
    assert!(byref_teardown_home(ByrefClass::OpenHandle).contains("resowner"));
    assert!(byref_teardown_home(ByrefClass::OnDiskBytes).contains("reaper"));
    assert!(byref_teardown_home(ByrefClass::PlainDirectory).contains("fail closed"));
    assert!(byref_teardown_home(ByrefClass::StrView).contains("Batch::begin"));
    assert!(byref_teardown_home(ByrefClass::StrView).contains("never pointers"));
}

/// The Send/Sync surface IS the byref contract: everything that rides
/// Locals/Seals crosses threads; everything holding raw addresses or VFDs
/// is thread-affine.
#[test]
fn send_sync_surface_matches_the_contract() {
    fn send_sync<T: Send + Sync>() {}
    // Plain data that rides Locals (byref class PlainDirectory) + the
    // pool itself (single-owner &mut state, moves through seal).
    send_sync::<crate::SpillFile>();
    send_sync::<crate::SpillExtent>();
    send_sync::<crate::EpochDir>();
    send_sync::<crate::StreamDir>();
    send_sync::<crate::SpillPool>();
    send_sync::<SpillMetrics>();
    send_sync::<crate::SpillSet>();
    // NOT Send (compile-time law, witnessed by the doc-tests on the types
    // themselves): PagePin, SwizzleToken — raw-address holders;
    // SpillWriter/SpillReader — VFD holders. A `send_sync::<PagePin>()`
    // line here does not compile; the negative witness lives as
    // `compile_fail` doctests in pool.rs/set.rs.
}

/// Metrics are the merge-at-seal vocabulary: additive counters, max peak.
#[test]
fn metrics_merge_law() {
    let mut a = SpillMetrics {
        spill_events: 1,
        bytes_written: 100,
        bytes_read: 10,
        extents_written: 2,
        pages_unloaded: 3,
        pages_reloaded: 4,
        pool_evictions: 5,
        peak_resident_bytes: 1000,
    };
    let b = SpillMetrics {
        spill_events: 2,
        bytes_written: 200,
        bytes_read: 20,
        extents_written: 3,
        pages_unloaded: 4,
        pages_reloaded: 5,
        pool_evictions: 6,
        peak_resident_bytes: 700,
    };
    a.merge(&b);
    assert_eq!(a.spill_events, 3);
    assert_eq!(a.bytes_written, 300);
    assert_eq!(a.bytes_read, 30);
    assert_eq!(a.extents_written, 5);
    assert_eq!(a.pages_unloaded, 7);
    assert_eq!(a.pages_reloaded, 9);
    assert_eq!(a.pool_evictions, 11);
    assert_eq!(a.peak_resident_bytes, 1000, "peak folds by max, never sum");
}
