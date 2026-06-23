//! Port of `backend/utils/mmgr/dsa.c` — dynamic shared memory areas.
//!
//! A DSA area is a shared-memory heap built on top of DSM segments. It deals in
//! *pseudo-pointers* ([`DsaPointer`]) valid in any backend attached to the area;
//! [`runtime::dsa_get_address`] converts one to a backend-local address. The
//! allocator's bookkeeping — the [`runtime::DsaAreaControl`] struct, the
//! [`runtime::DsaAreaSpan`] descriptors, the per-segment `FreePageManager`, the
//! page map, and the per-pool freelists — lives **in place inside the DSM
//! segments**, addressed by relative `dsa_pointer`s, so peer backends attach to
//! the same physical memory and follow the same links.
//!
//! ## Shared-memory substrate exception
//!
//! A cross-backend allocator requires byte-addressable shared memory: the
//! in-segment structs are crate-local `repr(C)` mirrors of `dsa.c`'s
//! file-private structs, read/written through the segment's resolved
//! backend-local base ([`dsm_core::dsm_segment_address`],
//! a real `*mut u8`). This is the same blessed `*mut`/`*const` exception that
//! the in-segment `FreePageManager` ([`types_freepage::FreePageManager`]) and
//! `LWLock` ([`types_storage::LWLock`]) take — both are embedded in the
//! `repr(C)` control/segment structs and operated through the real freepage and
//! lwlock seams (`*mut FreePageManager` / `&LWLock`).
//!
//! The per-backend `dsa_area` struct is backend-local state, held in a
//! `thread_local` registry (the established backend-local pattern, as in the
//! ported `dsm.c`).

use ::types_core::Size;
use types_error::{PgError, PgResult};

pub use types_dsa::{
    DsaHandle, DsaPointer, DsaSegmentIndex, DSA_ALLOC_HUGE, DSA_ALLOC_NO_OOM, DSA_ALLOC_ZERO,
    DSA_DEFAULT_INIT_SEGMENT_SIZE, DSA_FULLNESS_CLASSES, DSA_HANDLE_INVALID,
    DSA_MAX_SEGMENT_SIZE, DSA_MAX_SEGMENTS, DSA_MIN_SEGMENT_SIZE, DSA_NUM_SEGMENTS_AT_EACH_SIZE,
    DSA_NUM_SEGMENT_BINS, DSA_OFFSET_BITMASK, DSA_OFFSET_WIDTH, DSA_PAGES_PER_SUPERBLOCK,
    DSA_SCLASS_BLOCK_OF_SPANS, DSA_SCLASS_SPAN_LARGE, DSA_SEGMENT_HEADER_MAGIC,
    DSA_SEGMENT_INDEX_NONE, DSA_SIZE_CLASSES, DSA_SIZE_CLASS_MAP, DSA_SIZE_CLASS_MAP_QUANTUM,
    DSA_SPAN_NOTHING_FREE, INVALID_DSA_POINTER,
};

pub mod runtime;
pub mod wire;

/// `MaxAllocSize` (`memutils.h`) — the cap a non-huge `palloc` enforces:
/// `((Size) 0x3fffffff)`, 1 GB minus one.
pub const MAX_ALLOC_SIZE: Size = 0x3FFF_FFFF;

/// `MaxAllocHugeSize` (`memutils.h`) — `SIZE_MAX / 2`, the cap for a huge
/// (`DSA_ALLOC_HUGE`) request.
pub const MAX_ALLOC_HUGE_SIZE: Size = Size::MAX / 2;

/// `fpm_size_to_pages(sz)` (`utils/freepage.h`) —
/// `(sz + FPM_PAGE_SIZE - 1) / FPM_PAGE_SIZE`.
#[inline]
pub fn fpm_size_to_pages(sz: Size) -> Size {
    (sz + types_freepage::FPM_PAGE_SIZE - 1) / types_freepage::FPM_PAGE_SIZE
}

/// Validate an allocation request size against PostgreSQL's per-allocation
/// limits, exactly as the top of `dsa_allocate_extended` does:
///
/// ```c
/// Assert(size > 0);
/// if (((flags & DSA_ALLOC_HUGE) != 0 && !AllocHugeSizeIsValid(size)) ||
///     ((flags & DSA_ALLOC_HUGE) == 0 && !AllocSizeIsValid(size)))
///     elog(ERROR, "invalid DSA memory alloc request size %zu", size);
/// ```
pub fn validate_alloc_request(size: Size, flags: i32) -> PgResult<()> {
    let valid = if flags & DSA_ALLOC_HUGE != 0 {
        size > 0 && size <= MAX_ALLOC_HUGE_SIZE
    } else {
        size > 0 && size <= MAX_ALLOC_SIZE
    };
    if valid {
        Ok(())
    } else {
        Err(PgError::error(format!(
            "invalid DSA memory alloc request size {size}"
        )))
    }
}

/// The largest single object size servable from a pool size class — the last
/// entry of [`DSA_SIZE_CLASSES`]. Allocations strictly larger take the
/// large-object / free-page-manager path in `dsa_allocate_extended`.
#[inline]
pub fn largest_pool_object_size() -> Size {
    DSA_SIZE_CLASSES[DSA_SIZE_CLASSES.len() - 1] as Size
}

/// Map an allocation size onto the pool size class index, mirroring the
/// "Map allocation to a size class" block of `dsa_allocate_extended`. Caller
/// must already have ruled out the large-object path (`size <=
/// largest_pool_object_size()`).
pub fn dsa_size_class_index(size: Size) -> usize {
    if size < DSA_SIZE_CLASS_MAP.len() * DSA_SIZE_CLASS_MAP_QUANTUM {
        // For smaller sizes we have a lookup table.
        let mapidx = (size + DSA_SIZE_CLASS_MAP_QUANTUM - 1) / DSA_SIZE_CLASS_MAP_QUANTUM - 1;
        return DSA_SIZE_CLASS_MAP[mapidx] as usize;
    }
    // ... and for the rest we search by binary chop.
    let mut min = DSA_SIZE_CLASS_MAP[DSA_SIZE_CLASS_MAP.len() - 1] as usize;
    let mut max = DSA_SIZE_CLASSES.len() - 1;
    while min < max {
        let mid = (min + max) / 2;
        if (DSA_SIZE_CLASSES[mid] as Size) < size {
            min = mid + 1;
        } else {
            max = mid;
        }
    }
    min
}

/// `contiguous_pages_to_segment_bin(n)` — the lowest segment bin that *might*
/// hold a segment with `n` contiguous free pages:
///
/// ```c
/// if (n == 0) bin = 0;
/// else bin = pg_leftmost_one_pos_size_t(n) + 1;
/// return Min(bin, DSA_NUM_SEGMENT_BINS - 1);
/// ```
pub fn contiguous_pages_to_segment_bin(n: Size) -> Size {
    let bin = if n == 0 {
        0
    } else {
        (Size::BITS - n.leading_zeros()) as Size
    };
    bin.min(DSA_NUM_SEGMENT_BINS - 1)
}

/// `DsaPointerIsValid(dp)`.
#[inline]
pub fn dsa_pointer_is_valid(dp: DsaPointer) -> bool {
    dp != INVALID_DSA_POINTER
}

/// `DSA_MAKE_POINTER(segment_number, offset)`.
#[inline]
pub fn make_pointer(segment_number: DsaSegmentIndex, offset: Size) -> DsaPointer {
    ((segment_number as DsaPointer) << DSA_OFFSET_WIDTH) | (offset as DsaPointer)
}

/// Install every seam in `backend-utils-mmgr-dsa-seams` onto the real runtime.
pub fn init_seams() {
    wire::install_dsa_seams();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pointer_round_trips() {
        let dp = make_pointer(7, 1234);
        assert_eq!(dp >> DSA_OFFSET_WIDTH, 7);
        assert_eq!(dp & DSA_OFFSET_BITMASK, 1234);
        assert_eq!(dp, (7u64 << DSA_OFFSET_WIDTH) | 1234);
    }

    #[test]
    fn alloc_request_validation_matches_pg_limits() {
        assert!(validate_alloc_request(0, 0).is_err());
        assert!(validate_alloc_request(MAX_ALLOC_SIZE, 0).is_ok());
        assert!(validate_alloc_request(MAX_ALLOC_SIZE + 1, 0).is_err());
        assert!(validate_alloc_request(MAX_ALLOC_SIZE + 1, DSA_ALLOC_HUGE).is_ok());
        assert!(validate_alloc_request(MAX_ALLOC_HUGE_SIZE, DSA_ALLOC_HUGE).is_ok());
        assert!(validate_alloc_request(0, DSA_ALLOC_HUGE).is_err());
    }

    #[test]
    fn small_sizes_map_to_postgres_size_classes() {
        assert_eq!(dsa_size_class_index(1), 2);
        assert_eq!(DSA_SIZE_CLASSES[dsa_size_class_index(8)], 8);
        assert_eq!(DSA_SIZE_CLASSES[dsa_size_class_index(9)], 16);
        assert_eq!(DSA_SIZE_CLASSES[dsa_size_class_index(1024)], 1024);
        assert_eq!(DSA_SIZE_CLASSES[dsa_size_class_index(1025)], 1280);
        assert_eq!(
            DSA_SIZE_CLASSES[dsa_size_class_index(largest_pool_object_size())],
            8192
        );
    }

    #[test]
    fn size_class_invariants_hold_like_c_asserts() {
        for size in 1..=largest_pool_object_size() {
            let sc = dsa_size_class_index(size);
            assert!(size <= DSA_SIZE_CLASSES[sc] as Size, "size {size} class {sc}");
            if sc != 0 {
                assert!(size > DSA_SIZE_CLASSES[sc - 1] as Size, "size {size} class {sc}");
            }
        }
    }

    #[test]
    fn segment_bins_match_postgres_leftmost_one_logic() {
        assert_eq!(contiguous_pages_to_segment_bin(0), 0);
        assert_eq!(contiguous_pages_to_segment_bin(1), 1);
        assert_eq!(contiguous_pages_to_segment_bin(2), 2);
        assert_eq!(contiguous_pages_to_segment_bin(3), 2);
        assert_eq!(contiguous_pages_to_segment_bin(4), 3);
        assert_eq!(contiguous_pages_to_segment_bin(1 << 20), 15);
        assert_eq!(contiguous_pages_to_segment_bin(Size::MAX), DSA_NUM_SEGMENT_BINS - 1);
    }

    #[test]
    fn fpm_size_to_pages_rounds_up() {
        assert_eq!(fpm_size_to_pages(1), 1);
        assert_eq!(fpm_size_to_pages(types_freepage::FPM_PAGE_SIZE), 1);
        assert_eq!(fpm_size_to_pages(types_freepage::FPM_PAGE_SIZE + 1), 2);
    }
}
