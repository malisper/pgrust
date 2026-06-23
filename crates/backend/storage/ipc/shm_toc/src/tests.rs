//! Unit tests for the `shm_toc` port.
//!
//! Each test builds a fake shared-memory segment (a heap allocation aligned
//! for the in-segment header) and drives the [`ShmToc`] handle over it.

use super::*;

const MAGIC: uint64 = 0x0051_4854_4f43; // "QHTOC"

/// Install test implementations of the not-yet-ported shmem.c size seams,
/// mirroring the C `add_size`/`mul_size` overflow checks.
fn install_test_seams() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        add_size::set(|s1, s2| {
            s1.checked_add(s2).ok_or_else(|| {
                PgError::error("requested shared memory size overflows size_t")
            })
        });
        mul_size::set(|s1, s2| {
            s1.checked_mul(s2).ok_or_else(|| {
                PgError::error("requested shared memory size overflows size_t")
            })
        });
    });
}

/// Allocate a buffer aligned to `align_of::<InSegmentShmToc>()` for use as a
/// fake shared-memory segment, returning the leaked slice (tests are
/// short-lived). A `Vec<u64>` guarantees 8-byte alignment for the header.
fn make_segment(nbytes: usize) -> &'static mut [u8] {
    let words = nbytes.div_ceil(8);
    let v: Vec<u64> = vec![0u64; words];
    let boxed = v.into_boxed_slice();
    let raw = Box::into_raw(boxed) as *mut u8;
    // SAFETY: `raw` points at `words * 8` writable, aligned bytes that we leak.
    unsafe { core::slice::from_raw_parts_mut(raw, words * 8) }
}

fn segment_base(seg: &mut [u8]) -> NonNull<u8> {
    NonNull::new(seg.as_mut_ptr()).expect("segment base is non-null")
}

#[test]
fn header_layout_matches_c() {
    // offsetof(shm_toc, toc_entry) == 40 and sizeof(shm_toc_entry) == 16 on
    // the 64-bit target, exactly as the C layout requires.
    assert_eq!(toc_entry_offset(), 40);
    assert_eq!(size_of::<ShmTocEntry>(), 16);
}

#[test]
fn create_initializes_header() {
    let seg = make_segment(1024);
    let len = seg.len();
    let toc = unsafe { ShmToc::create(MAGIC, segment_base(seg), len) };
    let header = unsafe { toc.header() };
    assert_eq!(header.toc_magic, MAGIC);
    assert_eq!(header.toc_total_bytes, BUFFERALIGN_DOWN(len));
    assert_eq!(header.toc_allocated_bytes, 0);
    assert_eq!(header.toc_nentry, 0);
}

#[test]
fn attach_checks_magic() {
    let seg = make_segment(1024);
    let len = seg.len();
    let base = segment_base(seg);
    unsafe {
        ShmToc::create(MAGIC, base, len);
        assert!(ShmToc::attach(MAGIC + 1, base).is_none());
        assert!(ShmToc::attach(MAGIC, base).is_some());
    }
}

#[test]
fn allocate_grows_backward_and_is_buffer_aligned() {
    let seg = make_segment(1024);
    let len = seg.len();
    let base = segment_base(seg);
    let toc = unsafe { ShmToc::create(MAGIC, base, len) };
    let total = unsafe { toc.header() }.toc_total_bytes;

    let first = toc.allocate(1).unwrap();
    let second = toc.allocate(64).unwrap();

    let base_addr = base.as_ptr() as usize;
    // BUFFERALIGN(1) == 32, BUFFERALIGN(64) == 64.
    assert_eq!(first.as_ptr() as usize - base_addr, total - 32);
    assert_eq!(second.as_ptr() as usize - base_addr, total - 32 - 64);
    assert_eq!(unsafe { toc.header() }.toc_allocated_bytes, 96);
}

#[test]
fn insert_and_lookup_use_relative_offsets() {
    let seg = make_segment(1024);
    let len = seg.len();
    let base = segment_base(seg);
    let toc = unsafe { ShmToc::create(MAGIC, base, len) };
    let chunk = toc.allocate(8).unwrap();

    unsafe { toc.insert(42, chunk).unwrap() };
    assert_eq!(unsafe { toc.header() }.toc_nentry, 1);
    assert_eq!(toc.lookup(42, false).unwrap(), Some(chunk));
    assert_eq!(toc.lookup(7, true).unwrap(), None);
}

#[test]
fn lookup_missing_required_key_errors_like_postgres() {
    let seg = make_segment(1024);
    let len = seg.len();
    let base = segment_base(seg);
    let toc = unsafe { ShmToc::create(MAGIC, base, len) };
    let err = toc.lookup(42, false).unwrap_err();
    assert!(err
        .message()
        .starts_with("could not find key 42 in shm TOC at 0x"));
}

#[test]
fn allocate_reports_out_of_shared_memory() {
    // 96-byte segment: header (40), BUFFERALIGN_DOWN(96)=96 total; a 128-byte
    // request cannot fit.
    let seg = make_segment(96);
    let len = seg.len();
    let base = segment_base(seg);
    let toc = unsafe { ShmToc::create(MAGIC, base, len) };
    let err = toc.allocate(128).unwrap_err();
    assert_eq!(err.message(), "out of shared memory");
    assert_eq!(err.sqlstate(), ERRCODE_OUT_OF_MEMORY);
}

#[test]
fn insert_reports_out_of_shared_memory_when_entries_cannot_fit() {
    // 96-byte segment: header (40) + one 32-byte chunk + one 16-byte entry
    // leaves 8 bytes — not enough for a second entry.
    let seg = make_segment(96);
    let len = seg.len();
    let base = segment_base(seg);
    let toc = unsafe { ShmToc::create(MAGIC, base, len) };
    let chunk = toc.allocate(32).unwrap();
    unsafe { toc.insert(1, chunk).unwrap() };
    let err = unsafe { toc.insert(2, chunk).unwrap_err() };
    assert_eq!(err.message(), "out of shared memory");
    assert_eq!(err.sqlstate(), ERRCODE_OUT_OF_MEMORY);
}

#[test]
fn freespace_matches_postgres_formula() {
    let seg = make_segment(1024);
    let len = seg.len();
    let base = segment_base(seg);
    let toc = unsafe { ShmToc::create(MAGIC, base, len) };
    let total = unsafe { toc.header() }.toc_total_bytes;
    toc.allocate(1).unwrap(); // 32 bytes
    toc.allocate(64).unwrap(); // 64 bytes
    let free = toc.freespace();
    // allocated_bytes (96) + BUFFERALIGN(header, no entries).
    let toc_bytes = BUFFERALIGN(toc_entry_offset());
    assert_eq!(free, total - (96 + toc_bytes));
}

#[test]
fn estimate_matches_postgres_alignment() {
    install_test_seams();
    let mut e = shm_toc_estimator::default();
    shm_toc_initialize_estimator(&mut e);
    shm_toc_estimate_chunk(&mut e, 1).unwrap();
    shm_toc_estimate_keys(&mut e, 2).unwrap();

    assert_eq!(e.space_for_chunks, 32);
    assert_eq!(e.number_of_keys, 2);
    // sz = offsetof(toc_entry)=40 + 2*16 + 32 = 104; BUFFERALIGN(104)=128.
    assert_eq!(shm_toc_estimate(&e).unwrap(), 128);
}
