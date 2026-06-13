use super::*;

/// Build a heap-backed stand-in segment (cache-line aligned, zeroed) and
/// attach the thread-locals to it. Leaked: the thread-local pointers outlive
/// the test body's scope checks.
fn make_segment(total_size: usize) -> *mut PGShmemHeader {
    let layout = std::alloc::Layout::from_size_align(total_size, PG_CACHE_LINE_SIZE).unwrap();
    // SAFETY: nonzero size, valid alignment.
    let base = unsafe { std::alloc::alloc_zeroed(layout) };
    assert!(!base.is_null());
    let hdr = base.cast::<PGShmemHeader>();
    // SAFETY: fresh zeroed allocation large enough for the header.
    unsafe {
        (*hdr).totalsize = total_size;
        (*hdr).freeoffset = MAXALIGN(core::mem::size_of::<PGShmemHeader>());
        (*hdr).index = std::ptr::null_mut();
        InitShmemAccess(hdr);
    }
    hdr
}

#[test]
fn add_size_checks_overflow() {
    assert_eq!(add_size(2, 3).unwrap(), 5);
    assert!(add_size(usize::MAX, 1).is_err());
}

#[test]
fn mul_size_checks_overflow_and_zero() {
    assert_eq!(mul_size(6, 7).unwrap(), 42);
    assert_eq!(mul_size(0, usize::MAX).unwrap(), 0);
    assert_eq!(mul_size(usize::MAX, 0).unwrap(), 0);
    assert!(mul_size(usize::MAX, 2).is_err());
}

#[test]
fn allocator_carves_cacheline_aligned_chunks() {
    let hdr = make_segment(64 * 1024);
    InitShmemAllocation().unwrap();

    let a = ShmemAlloc(100).unwrap();
    assert_eq!(a.as_ptr() as usize % PG_CACHE_LINE_SIZE, 0);
    assert!(ShmemAddrIsValid(a.as_ptr()));

    let mut allocated = 0;
    let b = ShmemAllocRaw(1, &mut allocated);
    assert_eq!(allocated, PG_CACHE_LINE_SIZE);
    assert!(!b.is_null());
    assert_ne!(a.as_ptr(), b);

    // Exhaustion: NoError returns null, ShmemAlloc errors, and the
    // freeoffset is unchanged by the failed attempts.
    // SAFETY: hdr is the live test segment.
    let before = unsafe { (*hdr).freeoffset };
    assert!(ShmemAllocNoError(1 << 20).is_null());
    assert!(ShmemAlloc(1 << 20).is_err());
    assert!(ShmemAllocUnlocked(1 << 20).is_err());
    assert_eq!(unsafe { (*hdr).freeoffset }, before);
}

#[test]
fn addr_validity_tracks_segment_bounds() {
    let hdr = make_segment(8 * 1024);
    InitShmemAllocation().unwrap();
    let base = hdr.cast::<u8>();
    assert!(ShmemAddrIsValid(base));
    // SAFETY: in-bounds / one-past-end pointer arithmetic over the live
    // 8 KiB test segment.
    unsafe {
        assert!(ShmemAddrIsValid(base.add(8 * 1024 - 1)));
        assert!(!ShmemAddrIsValid(base.add(8 * 1024)));
    }
    assert!(!ShmemAddrIsValid(std::ptr::null()));
}
