//! ABI conformance pins for the v4 deltas (the copied modules carry their
//! own in-file suites from the lx_vec lineage; these pin what THIS crate
//! adds — the frozen constants and layouts a later edit must not drift).

use core::mem::{align_of, offset_of, size_of};

use crate::guard::*;
use crate::rep::{code_width_for, CodeWidth, DictEpochKey, ESCAPE_CODE_U16, ESCAPE_CODE_U32};
use crate::strview::StrCell;
use crate::{GuardWord, SEL_DENSITY_CROSSOVER_DIV};

/// AB-2.4: the StrView cell layout is CONTRACT TEXT — 16 bytes, align 8,
/// offsets 0/4/8 (`strview.rs:86-98` of the copy source).
#[test]
fn strcell_layout_is_pinned() {
    assert_eq!(size_of::<StrCell>(), 16);
    assert_eq!(align_of::<StrCell>(), 8);
}

/// AB-2.2: the structural epoch tag is layout-pinned 24 B (repr(C),
/// offsets 0/16/20) — the cross-crate epoch vocabulary.
#[test]
fn dict_epoch_key_layout_is_pinned() {
    assert_eq!(size_of::<DictEpochKey>(), 24);
    assert_eq!(align_of::<DictEpochKey>(), 4);
    assert_eq!(offset_of!(DictEpochKey, part_uuid), 0);
    assert_eq!(offset_of!(DictEpochKey, attno), 16);
    assert_eq!(offset_of!(DictEpochKey, path_ord), 20);
}

/// AB-7.3: guard bit assignments are FROZEN append-only; a renumbering is
/// an ABI break this test exists to catch.
#[test]
fn guard_bits_are_frozen() {
    assert_eq!(GUARD_OVERFLOW, 1 << 0);
    assert_eq!(GUARD_COLLATION, 1 << 1);
    assert_eq!(GUARD_ENCODING, 1 << 2);
    assert_eq!(GUARD_EPOCH, 1 << 3);
    assert_eq!(GUARD_CODE_BOUND, 1 << 4);
    assert_eq!(GUARD_NUMERIC_DOMAIN, 1 << 5);
}

/// AB-7.3: any set bit demotes; a clear word never does.
#[test]
fn guard_word_demotion_semantics() {
    let mut w = GuardWord::clear();
    assert!(!w.demotes());
    w.raise(GUARD_CODE_BOUND);
    assert!(w.demotes());
    assert!(w.has(GUARD_CODE_BOUND));
    assert!(!w.has(GUARD_EPOCH));
    w.raise(GUARD_EPOCH);
    assert!(w.has(GUARD_CODE_BOUND) && w.has(GUARD_EPOCH));
}

/// AB-2.2/2.3: width election reserves the escape code — the domain
/// maximum is never assignable, so the u16→u32 crossover sits at
/// `u16::MAX` entries, not `u16::MAX + 1`.
#[test]
fn code_width_election_reserves_the_escape() {
    assert_eq!(ESCAPE_CODE_U16, u16::MAX);
    assert_eq!(ESCAPE_CODE_U32, u32::MAX);
    assert_eq!(code_width_for(0), CodeWidth::U16);
    assert_eq!(code_width_for(u16::MAX as u32 - 1), CodeWidth::U16);
    // Exactly u16::MAX entries would need code u16::MAX-1 assignable AND
    // the escape — the escape reservation forces u32 here.
    assert_eq!(code_width_for(u16::MAX as u32), CodeWidth::U32);
    assert_eq!(code_width_for(u32::MAX), CodeWidth::U32);
}

/// AB-3.4: the crossover constant is contract text.
#[test]
fn density_crossover_is_one_eighth() {
    assert_eq!(SEL_DENSITY_CROSSOVER_DIV, 8);
}
