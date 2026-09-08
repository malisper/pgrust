use super::*;
use types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED;

#[test]
fn init_struct_create_then_attach() {
    let (p1, found1) = ShmemInitStruct("test_create_attach", 256).unwrap();
    assert!(!found1);
    assert_eq!(p1 as usize % PG_CACHE_LINE_SIZE, 0);
    // SAFETY: fresh 256-byte allocation owned by the registry.
    unsafe {
        assert!(std::slice::from_raw_parts(p1, 256).iter().all(|&b| b == 0));
        p1.write(0xAB);
    }

    let (p2, found2) = ShmemInitStruct("test_create_attach", 256).unwrap();
    assert!(found2);
    assert_eq!(p1, p2);
    // SAFETY: same allocation as above.
    unsafe { assert_eq!(p2.read(), 0xAB) };
}

#[test]
fn init_struct_size_mismatch_errors() {
    ShmemInitStruct("test_size_mismatch", 128).unwrap();
    let err = ShmemInitStruct("test_size_mismatch", 192).unwrap_err();
    assert!(err.message.contains("ShmemIndex entry size is wrong"));
    assert!(err.message.contains("expected 192, actual 128"));
}

#[test]
fn alloc_is_aligned_and_zeroed() {
    let p = ShmemAlloc(1).unwrap();
    assert_eq!(p as usize % PG_CACHE_LINE_SIZE, 0);
    let q = ShmemAllocNoError(4096);
    assert!(!q.is_null());
    // SAFETY: 4096 bytes freshly allocated above.
    unsafe { assert!(std::slice::from_raw_parts(q, 4096).iter().all(|&b| b == 0)) };
}

#[test]
fn alloc_overflow_is_oom() {
    let err = ShmemAlloc(usize::MAX - 7).unwrap_err();
    assert_eq!(err.sqlstate, ERRCODE_OUT_OF_MEMORY);
    assert!(err.message.contains("out of shared memory"));
    assert!(ShmemAllocNoError(usize::MAX - 7).is_null());
}

#[test]
fn size_arithmetic_checks_overflow() {
    assert_eq!(add_size(3, 4).unwrap(), 7);
    assert_eq!(mul_size(3, 4).unwrap(), 12);
    assert_eq!(mul_size(0, usize::MAX).unwrap(), 0);
    assert_eq!(mul_size(usize::MAX, 0).unwrap(), 0);

    // mcxt.c:1694-1700 add_size_error / :1713-1719 mul_size_error (18.6 moved
    // both out of shmem.c; the pre-18 "requested shared memory size overflows
    // size_t" text is gone).
    let err = add_size(usize::MAX, 1).unwrap_err();
    assert_eq!(err.sqlstate, ERRCODE_PROGRAM_LIMIT_EXCEEDED);
    assert_eq!(
        err.message,
        format!("invalid memory allocation request size {} + 1", usize::MAX)
    );
    let err = mul_size(usize::MAX, 2).unwrap_err();
    assert_eq!(err.sqlstate, ERRCODE_PROGRAM_LIMIT_EXCEEDED);
    assert_eq!(
        err.message,
        format!("invalid memory allocation request size {} * 2", usize::MAX)
    );
}

#[test]
fn shmem_lock_excludes() {
    init_seams();
    if !s_lock_seams::perform_spin_delay::is_installed() {
        s_lock::init_seams();
    }
    if !waitevent_seams::pgstat_report_wait_start::is_installed() {
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
    }
    static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let threads: Vec<_> = (0..4)
        .map(|_| {
            std::thread::spawn(|| {
                for _ in 0..1000 {
                    shmem_seams::shmem_lock_acquire::call();
                    let v = COUNTER.load(Ordering::Relaxed);
                    COUNTER.store(v + 1, Ordering::Relaxed);
                    shmem_seams::shmem_lock_release::call();
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    assert_eq!(COUNTER.load(Ordering::Relaxed), 4000);

    assert_eq!(shmem_seams::add_size::call(1, 2).unwrap(), 3);
    assert_eq!(shmem_seams::mul_size::call(2, 3).unwrap(), 6);
    let (p, found) = shmem_seams::shmem_init_struct::call("test_via_seam", 64).unwrap();
    assert!(!p.is_null() && !found);
    assert!(!shmem_seams::shmem_alloc::call(64).unwrap().is_null());
}

#[test]
fn concurrent_init_struct_single_creation() {
    let results: Vec<_> = (0..8)
        .map(|_| {
            std::thread::spawn(|| {
                let (p, found) = ShmemInitStruct("test_concurrent", 512).unwrap();
                (p.expose_provenance(), found)
            })
        })
        .collect();
    let results: Vec<_> = results.into_iter().map(|t| t.join().unwrap()).collect();
    let addr = results[0].0;
    assert!(results.iter().all(|&(p, _)| p == addr));
    assert_eq!(results.iter().filter(|&&(_, found)| !found).count(), 1);
}

// shmem.c:159 ShmemAlloc: a request the segment cannot satisfy is
// ERRCODE_OUT_OF_MEMORY "out of shared memory (%zu bytes requested)" (and
// ShmemInitStruct's "not enough shared memory for data structure"), never a
// panic — ShmemAllocRaw's Layout::expect on an unrepresentable padded size
// (> isize::MAX) was one.
#[test]
fn alloc_of_unrepresentable_size_is_out_of_memory_error_not_panic() {
    let size = isize::MAX as usize;
    let err = ShmemAlloc(size).unwrap_err();
    assert_eq!(err.sqlstate, ERRCODE_OUT_OF_MEMORY);
    assert_eq!(err.message, format!("out of shared memory ({size} bytes requested)"));
    assert!(ShmemAllocNoError(size).is_null());
    let err = ShmemInitStruct("b118_unrepresentable", size).unwrap_err();
    assert_eq!(err.sqlstate, ERRCODE_OUT_OF_MEMORY);
    assert_eq!(
        err.message,
        format!(
            "not enough shared memory for data structure \"b118_unrepresentable\" ({size} bytes requested)"
        )
    );
}

// shmem.c:428-436: an index entry that cannot be created is
// ERRCODE_OUT_OF_MEMORY "could not create ShmemIndex entry for data
// structure \"%s\"", not an allocator abort. The reservation helper is the
// one arm Vec::push would have aborted in; a capacity-overflow reservation
// drives it deterministically.
#[test]
fn index_entry_allocation_failure_is_out_of_memory_error() {
    let mut index: Vec<ShmemIndexEnt> = Vec::new();
    let err = shmem_index_reserve(&mut index, usize::MAX, "b245_index_full").unwrap_err();
    assert_eq!(err.sqlstate, ERRCODE_OUT_OF_MEMORY);
    assert_eq!(
        err.message,
        "could not create ShmemIndex entry for data structure \"b245_index_full\""
    );
    shmem_index_reserve(&mut index, 1, "b245_index_ok").unwrap();
}

// shmem.c:210-215: an entry's off is the bump value its own allocation
// consumed (C derives it from the pointer). Bumps from other threads landing
// between the carve and the index insert must not shift it: every bump --
// anonymous or indexed -- owns a distinct cache-line-aligned offset.
#[test]
fn index_entry_off_is_the_entrys_own_bump() {
    let stop = std::sync::Arc::new(AtomicBool::new(false));
    let hammers: Vec<_> = (0..4)
        .map(|_| {
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut offs = Vec::new();
                while !stop.load(Ordering::Relaxed) {
                    let mut allocated = 0;
                    let (p, off) = ShmemAllocRaw(1, &mut allocated);
                    assert!(!p.is_null());
                    offs.push(off);
                }
                offs
            })
        })
        .collect();
    for i in 0..256 {
        let name = format!("b245_off_{i}");
        let (_, found) = ShmemInitStruct(&name, 1).unwrap();
        assert!(!found);
    }
    stop.store(true, Ordering::Relaxed);
    let mut offs: Vec<usize> = hammers
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    let index = SHMEM_INDEX.lock().unwrap();
    let entries: Vec<&ShmemIndexEnt> =
        index.iter().filter(|e| e.name.starts_with("b245_off_")).collect();
    assert_eq!(entries.len(), 256);
    for e in &entries {
        assert_eq!(e.allocated_size, PG_CACHE_LINE_SIZE);
        offs.push(e.off);
    }
    let total = offs.len();
    offs.sort_unstable();
    offs.dedup();
    assert_eq!(offs.len(), total, "a bump offset was attributed twice");
    assert!(offs.iter().all(|off| off % PG_CACHE_LINE_SIZE == 0));
}

// shmem.c:332-380: ShmemInitHash fixes the directory at
// hash_select_dirsize(max_size) slots, registers the header + directory block
// (hash_get_shared_size) under the table's name, and hands its location to
// hash_create as the preallocated header (dynahash.c:478-487); elements are
// carved with ShmemAllocNoError. The table works, and the index row carries
// the C size.
#[test]
fn init_hash_registers_header_and_directory_under_the_table_name() {
    use types_hash::hsearch::{HASH_BLOBS, HASH_ELEM, HASH_ENTER, HASH_FIXED_SIZE};
    let mut info = HASHCTL::new();
    info.keysize = 4;
    info.entrysize = 16;
    let table =
        ShmemInitHash("test_init_hash", 32, 32, &mut info, HASH_ELEM | HASH_BLOBS | HASH_FIXED_SIZE)
            .unwrap();
    assert_eq!(info.dsize, dynahash::hash_select_dirsize(32));
    assert_eq!(info.max_dsize, info.dsize);
    let expected = core::mem::size_of::<HASHHDR>() + info.dsize as usize * 8;
    let ent_size = {
        let index = SHMEM_INDEX.lock().unwrap();
        index.iter().find(|e| e.name == "test_init_hash").map(|e| (e.size, e.location))
    };
    let (size, location) = ent_size.expect("ShmemInitHash registers the table name");
    assert_eq!(size, expected);
    assert_eq!(size, dynahash::hash_get_shared_size(&info, HASH_DIRSIZE));
    // dynahash.c:484: the header IS the registered block.
    assert_eq!(info.hctl as usize, location);
    unsafe {
        assert!((*table).isshared);
        assert_eq!((*table).hctl as usize, location);
        let mut found = false;
        let key = 7u32.to_ne_bytes();
        let p = dynahash::hash_search(table, key.as_ptr(), HASH_ENTER, Some(&mut found)).unwrap();
        assert!(!p.is_null() && !found);
        let p2 = dynahash::hash_search(table, key.as_ptr(), HASH_ENTER, Some(&mut found)).unwrap();
        assert!(found);
        assert_eq!(p, p2);
    }
}

// shmem.c:369-373 + dynahash.c:491: a name already in the index is the
// HASH_ATTACH arm, which one address space has no use for (dynahash's typed
// refusal, carve-ratifications §4).
#[test]
fn init_hash_of_a_registered_name_is_the_attach_refusal() {
    use types_hash::hsearch::{HASH_BLOBS, HASH_ELEM, HASH_FIXED_SIZE};
    let mut info = HASHCTL::new();
    info.keysize = 4;
    info.entrysize = 16;
    let flags = HASH_ELEM | HASH_BLOBS | HASH_FIXED_SIZE;
    ShmemInitHash("test_init_hash_twice", 8, 8, &mut info, flags).unwrap();
    let err = ShmemInitHash("test_init_hash_twice", 8, 8, &mut info, flags).unwrap_err();
    assert_eq!(
        err.message(),
        "attaching to hash table \"test_init_hash_twice\" is not supported"
    );
}

// sysv_shmem.c:855-856 + shmem.c:115-142: the segment starts with the
// MAXALIGN'd PGShmemHeader (56 bytes on every LP64 target), then
// ShmemAllocUnlocked's slock_t (MAXALIGN(1) = 8), then the first ShmemAlloc
// is cache-line aligned: the first indexed block sits at off 128 in C.
#[test]
fn init_shmem_allocation_seeds_freeoffset_like_c() {
    assert_eq!(core::mem::size_of::<types_storage::PGShmemHeader>(), 56);
    assert_eq!(initial_freeoffset(), 128);
}
