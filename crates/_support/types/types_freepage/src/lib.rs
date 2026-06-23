//! Free-page-map vocabulary (`utils/freepage.h`, `utils/relptr.h`): the
//! shared-memory `FreePageManager` layout and its page-size constants.
//!
//! The struct lives in shared memory (e.g. ahead of the preallocated DSM
//! region), so the layout is the full C layout — `repr(C)`, every field —
//! because consumers size reservations with `size_of::<FreePageManager>()`.
//! Only `freepage.c`'s own port mutates the internals.

#![no_std]
#![allow(non_camel_case_types)]

use ::types_core::Size;

/// `FPM_PAGE_SIZE` (`utils/freepage.h`).
pub const FPM_PAGE_SIZE: Size = 4096;

/// `FPM_NUM_FREELISTS` (`utils/freepage.h`).
pub const FPM_NUM_FREELISTS: usize = 129;

/// `relptr(type)` (`utils/relptr.h`) — a self-relative pointer stored as a
/// `Size` offset (`0` is NULL; otherwise `offset + 1` from the base). The C
/// union's pointer member exists only for macro type-checking; the stored
/// representation is always the `Size`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct RelPtr {
    pub relptr_off: Size,
}

/// `RelptrFreePageManager`.
pub type RelptrFreePageManager = RelPtr;
/// `RelptrFreePageBtree`.
pub type RelptrFreePageBtree = RelPtr;
/// `RelptrFreePageSpanLeader`.
pub type RelptrFreePageSpanLeader = RelPtr;

/// `struct FreePageManager` (`utils/freepage.h`) — everything needed to
/// manage free pages. The `FPM_EXTRA_ASSERTS`-only `free_pages` field is
/// omitted, matching the default build.
#[repr(C)]
pub struct FreePageManager {
    /// `RelptrFreePageManager self`.
    pub self_: RelptrFreePageManager,
    pub btree_root: RelptrFreePageBtree,
    pub btree_recycle: RelptrFreePageSpanLeader,
    pub btree_depth: u32,
    pub btree_recycle_count: u32,
    pub singleton_first_page: Size,
    pub singleton_npages: Size,
    pub contiguous_pages: Size,
    pub contiguous_pages_dirty: bool,
    pub freelist: [RelptrFreePageSpanLeader; FPM_NUM_FREELISTS],
}
