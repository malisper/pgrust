//! Reader-currency layout pins (issue-#69 template): the sizes that price
//! engagement — the error word every face returns, the fault-witness entry,
//! the shared section buffer handle, and the cross-crate dict-epoch
//! vocabulary. A change is a visible diff here, never silent drift.
//!
//! Every pin is `const`-evaluated (fails `cargo check`, not just the test
//! run); the `#[test]` restates them so the suite carries the witness.

use core::mem::{align_of, size_of};

use pgrc2_format::FormatError;

use crate::dicthandle::DictEpochKey;
use crate::openpart::{FaultEntry, FaultTag, PartExpect, SegBuf};
use crate::registry::{PartKey, RegistryCounters};
use crate::streams::ParsedStream;
use crate::ReadError;

const _: () = {
    // The error word: every reader face returns Result<_, ReadError>. The
    // frozen FormatError is 24 B; the reader's `Io { at, errno }` variant
    // (fat &'static str + errno beside the discriminant) prices one more
    // word — 32 B is the accepted cost, pinned so growth is a visible diff.
    assert!(size_of::<FormatError>() == 24);
    assert!(size_of::<ReadError>() == 32);

    // The fault witness: one entry per resident region, Vec-logged per part.
    assert!(size_of::<FaultTag>() == 16);
    assert!(size_of::<FaultEntry>() == 32);
    assert!(align_of::<FaultEntry>() == 8);

    // The shared section buffer handle (Arc<[u64]> + len): cloned per
    // section access.
    assert!(size_of::<SegBuf>() == 24);

    // The cross-crate dict-epoch vocabulary (spec §7 Law A; module doc says
    // "layout-pinned (24 B)" — this is that pin).
    assert!(size_of::<DictEpochKey>() == 24);
    assert!(align_of::<DictEpochKey>() == 4);

    // Registry currency: the stat-only key and the measured-only counters.
    assert!(size_of::<PartKey>() == 24);
    assert!(size_of::<RegistryCounters>() == 32);

    // Open-time validation facts (a per-open stack value).
    assert!(size_of::<PartExpect>() == 104);

    // Per-stream parsed facts (entry 48 + role + extent vec), cloned into
    // every cursor.
    assert!(size_of::<ParsedStream>() == 80);
};

#[test]
fn reader_currency_sizes_pinned() {
    assert_eq!(size_of::<FormatError>(), 24);
    assert_eq!(size_of::<ReadError>(), 32);
    assert_eq!(size_of::<FaultTag>(), 16);
    assert_eq!(size_of::<FaultEntry>(), 32);
    assert_eq!(size_of::<SegBuf>(), 24);
    assert_eq!(size_of::<DictEpochKey>(), 24);
    assert_eq!(size_of::<PartKey>(), 24);
    assert_eq!(size_of::<RegistryCounters>(), 32);
    assert_eq!(size_of::<PartExpect>(), 104);
    assert_eq!(size_of::<ParsedStream>(), 80);
}
