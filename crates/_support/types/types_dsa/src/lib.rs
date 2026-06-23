//! Dynamic shared memory area (DSA) vocabulary (`utils/dsa.h` and the
//! file-private constants at the top of `utils/mmgr/dsa.c`).
//!
//! Only the representation-neutral pieces live here: the `dsa_pointer` /
//! `dsa_handle` types, the area-geometry constants, the allocation flags, and
//! the small-object size-class tables. The `dsa_area_control` /
//! `dsa_area_span` / `dsa_segment_header` aggregates are file-private to the
//! allocator and live (as `repr(C)` mirrors) in the owning crate, since they
//! are laid out at computed byte offsets inside the shared segment.

#![no_std]

use types_core::Size;

/// `dsa_pointer` (`utils/dsa.h`) — an address relative to a DSA area, valid in
/// any backend attached to that area. `SIZEOF_DSA_POINTER == 8` on 64-bit
/// builds.
pub type DsaPointer = u64;

/// `dsa_handle` (`utils/dsa.h`) — a handle by which other backends attach to an
/// area; it is the `dsm_handle` of the area's first segment.
pub type DsaHandle = u32;

/// `dsa_segment_index` — zero-based segment index (`typedef size_t`).
pub type DsaSegmentIndex = Size;

/// `InvalidDsaPointer` — the null relative pointer (`(dsa_pointer) 0`).
pub const INVALID_DSA_POINTER: DsaPointer = 0;

/// `DSA_HANDLE_INVALID` — invalid area handle (`DSM_HANDLE_INVALID`).
pub const DSA_HANDLE_INVALID: DsaHandle = 0;

/// `DSA_SEGMENT_INDEX_NONE` — sentinel meaning "none"/"end" (`~(size_t)0`).
pub const DSA_SEGMENT_INDEX_NONE: DsaSegmentIndex = Size::MAX;

/// `DSA_OFFSET_WIDTH` — number of low bits of a `dsa_pointer` used for the
/// in-segment offset (the high bits hold the segment number). 40 on the 64-bit
/// build (`SIZEOF_DSA_POINTER == 8`).
pub const DSA_OFFSET_WIDTH: u32 = 40;

/// `DSA_OFFSET_BITMASK` — bitmask for the offset part of a `dsa_pointer`.
pub const DSA_OFFSET_BITMASK: DsaPointer = (1u64 << DSA_OFFSET_WIDTH) - 1;

/// `DSA_MAX_SEGMENT_SIZE` — `(size_t) 1 << DSA_OFFSET_WIDTH`.
pub const DSA_MAX_SEGMENT_SIZE: Size = 1usize << DSA_OFFSET_WIDTH;

/// `DSA_MAX_SEGMENTS` — `Min(1024, 1 << ((SIZEOF_DSA_POINTER*8) - DSA_OFFSET_WIDTH))`.
/// On the 64-bit build `1 << 24 = 16M`, capped at 1024.
pub const DSA_MAX_SEGMENTS: usize = 1024;

/// `DSA_NUM_SEGMENT_BINS` — number of segment bins keyed by largest free run.
pub const DSA_NUM_SEGMENT_BINS: usize = 16;

/// `DSA_NUM_SEGMENTS_AT_EACH_SIZE` — segments created before doubling size.
pub const DSA_NUM_SEGMENTS_AT_EACH_SIZE: usize = 2;

/// `DSA_PAGES_PER_SUPERBLOCK` — pages (of `FPM_PAGE_SIZE`) per regular superblock.
pub const DSA_PAGES_PER_SUPERBLOCK: usize = 16;

/// `DSA_FULLNESS_CLASSES` — superblock fullness quartiles.
pub const DSA_FULLNESS_CLASSES: usize = 4;

/// `DSA_SEGMENT_HEADER_MAGIC` — XORed with the handle and the segment index for
/// the per-segment sanity-check magic value.
pub const DSA_SEGMENT_HEADER_MAGIC: u32 = 0x0ce2_6608;

/// `DSA_SPAN_NOTHING_FREE` — span freelist sentinel (`(uint16) -1`).
pub const DSA_SPAN_NOTHING_FREE: u16 = u16::MAX;

/// Special size classes.
pub const DSA_SCLASS_BLOCK_OF_SPANS: usize = 0;
pub const DSA_SCLASS_SPAN_LARGE: usize = 1;

/// `DSA_SIZE_CLASS_MAP_QUANTUM` — small-object rounding granularity for the
/// `dsa_size_class_map` lookup table.
pub const DSA_SIZE_CLASS_MAP_QUANTUM: usize = 8;

/// `DSA_DEFAULT_INIT_SEGMENT_SIZE` (`utils/dsa.h`) — 1 MB.
pub const DSA_DEFAULT_INIT_SEGMENT_SIZE: Size = 1024 * 1024;

/// `DSA_MIN_SEGMENT_SIZE` (`utils/dsa.h`) — 256 kB.
pub const DSA_MIN_SEGMENT_SIZE: Size = 256 * 1024;

/// Allocation flags for `dsa_allocate_extended` (mirroring
/// `MemoryContextAllocExtended`).
pub const DSA_ALLOC_HUGE: i32 = 0x01;
pub const DSA_ALLOC_NO_OOM: i32 = 0x02;
pub const DSA_ALLOC_ZERO: i32 = 0x04;

/// `dsa_size_classes[]` — the possible small-object allocation sizes. The first
/// two entries are the special size classes: `sizeof(dsa_area_span)` (56 on the
/// 64-bit build) followed by `0`.
pub const DSA_SIZE_CLASSES: &[u16] = &[
    56, 0, // special size classes: sizeof(dsa_area_span), then 0
    8, 16, 24, 32, 40, 48, 56, 64, // 8 classes separated by 8 bytes
    80, 96, 112, 128, // 4 classes separated by 16 bytes
    160, 192, 224, 256, // 4 classes separated by 32 bytes
    320, 384, 448, 512, // 4 classes separated by 64 bytes
    640, 768, 896, 1024, // 4 classes separated by 128 bytes
    1280, 1560, 1816, 2048, // 4 classes separated by ~256 bytes
    2616, 3120, 3640, 4096, // 4 classes separated by ~512 bytes
    5456, 6552, 7280, 8192, // 4 classes separated by ~1024 bytes
];

/// `dsa_size_class_map[]` — maps `(size rounded up to a multiple of 8) - 1`
/// onto a `dsa_size_classes` index, for objects under 1 kB.
pub const DSA_SIZE_CLASS_MAP: &[u8] = &[
    2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 11, 11, 12, 12, 13, 13, //
    14, 14, 14, 14, 15, 15, 15, 15, 16, 16, 16, 16, 17, 17, 17, 17, //
    18, 18, 18, 18, 18, 18, 18, 18, 19, 19, 19, 19, 19, 19, 19, 19, //
    20, 20, 20, 20, 20, 20, 20, 20, 21, 21, 21, 21, 21, 21, 21, 21, //
    22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, 22, //
    23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, //
    24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, //
    25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, 25, //
];
