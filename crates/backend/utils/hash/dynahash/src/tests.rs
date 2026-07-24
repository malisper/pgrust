#![allow(unused_unsafe)]

use super::*;
use ::types_hash::hsearch::{HASH_FUNCTION, HASH_PARTITION, HASH_SHARED_MEM};
use std::sync::Once;

fn search(t: *mut HTAB, k: *const u8, a: HASHACTION) -> PgResult<(*mut u8, bool)> {
    let mut f = false;
    let p = hash_search(t, k, a, Some(&mut f))?;
    Ok((p, f))
}

fn search_hv(t: *mut HTAB, k: *const u8, hv: u32, a: HASHACTION) -> PgResult<(*mut u8, bool)> {
    let mut f = false;
    let p = hash_search_with_hash_value(t, k, hv, a, Some(&mut f))?;
    Ok((p, f))
}


static SEAMS: Once = Once::new();

fn install_test_seams() {
    SEAMS.call_once(|| {
        xact_seams::get_current_transaction_nest_level::set(|| 1);
    });
}

fn ctl(keysize: usize, entrysize: usize) -> HASHCTL {
    HASHCTL {
        keysize,
        entrysize,
        ..Default::default()
    }
}

unsafe fn entry(p: *mut u8) -> &'static mut [u8] {
    core::slice::from_raw_parts_mut(p, 8)
}

#[test]
fn enter_find_and_remove_blob_key() {
    install_test_seams();
    let ctl = ctl(4, 12);
    let table = hash_create("test", 8, &ctl, HASH_ELEM | HASH_BLOBS).unwrap();
    let key = 42u32.to_ne_bytes();
    unsafe {
        let (p, found) = search(table, key.as_ptr(), HASH_ENTER).unwrap();
        assert!(!found);
        let e = core::slice::from_raw_parts_mut(p, 12);
        e[4..8].copy_from_slice(&99u32.to_ne_bytes());

        let (p, found) = search(table, key.as_ptr(), HASH_FIND).unwrap();
        assert!(found);
        let e = core::slice::from_raw_parts(p, 12);
        assert_eq!(&e[4..8], &99u32.to_ne_bytes());

        let (_, found) = search(table, key.as_ptr(), HASH_REMOVE).unwrap();
        assert!(found);
        let (_, found) = search(table, key.as_ptr(), HASH_FIND).unwrap();
        assert!(!found);
        assert_eq!(hash_get_num_entries(table), 0);
    }
    hash_destroy(table);
}

#[test]
fn freelist_recycles_removed_element() {
    install_test_seams();
    let ctl = ctl(4, 8);
    let table = hash_create("recycle", 8, &ctl, HASH_ELEM | HASH_BLOBS).unwrap();
    unsafe {
        let (p1, _) = search(table, 1u32.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
        let (r, found) = search(table, 1u32.to_ne_bytes().as_ptr(), HASH_REMOVE).unwrap();
        assert!(found);
        assert_eq!(p1, r);
        let (p2, found) = search(table, 2u32.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
        assert!(!found);
        assert_eq!(p1, p2, "freed element must be recycled LIFO from the freelist");
    }
    hash_destroy(table);
}

#[test]
fn entries_are_pointer_stable_across_growth() {
    install_test_seams();
    let ctl = ctl(4, 8);
    let table = hash_create("stable", 4, &ctl, HASH_ELEM | HASH_BLOBS).unwrap();
    unsafe {
        let (p0, _) = search(table, 0u32.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
        for i in 1u32..500 {
            search(table, i.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
        }
        let (p0b, found) = search(table, 0u32.to_ne_bytes().as_ptr(), HASH_FIND).unwrap();
        assert!(found);
        assert_eq!(p0, p0b);
    }
    hash_destroy(table);
}

#[test]
fn string_keys_truncate_at_nul_and_keysize_minus_one() {
    install_test_seams();
    let ctl = ctl(16, 24);
    let table = hash_create("strings", 8, &ctl, HASH_ELEM | HASH_STRINGS).unwrap();
    unsafe {
        search(table, b"abc\0tail\0\0\0\0\0\0\0\0".as_ptr(), HASH_ENTER).unwrap();
        let (_, found) =
            search(table, b"abc\0zzzz\0\0\0\0\0\0\0\0".as_ptr(), HASH_FIND).unwrap();
        assert!(found);
        let (_, found) =
            search(table, b"abcdzzzz\0\0\0\0\0\0\0\0".as_ptr(), HASH_FIND).unwrap();
        assert!(!found);
    }
    hash_destroy(table);
}

#[test]
fn fixed_size_enter_null_returns_none() {
    install_test_seams();
    let ctl = ctl(4, 8);
    let table = hash_create("fixed", 1, &ctl, HASH_ELEM | HASH_FIXED_SIZE | HASH_BLOBS).unwrap();
    unsafe {
        search(table, 1u32.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
        let (p, found) = search(table, 2u32.to_ne_bytes().as_ptr(), HASH_ENTER_NULL).unwrap();
        assert!(!found);
        assert!(p.is_null());
    }
    hash_destroy(table);
}

#[test]
fn fixed_size_enter_overflow_errors() {
    install_test_seams();
    let ctl = ctl(4, 8);
    let table = hash_create("fixed", 1, &ctl, HASH_ELEM | HASH_FIXED_SIZE | HASH_BLOBS).unwrap();
    unsafe {
        search(table, 1u32.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
        let err = search(table, 2u32.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_OUT_OF_MEMORY);
    }
    hash_destroy(table);
}

#[test]
fn collision_chain_survives_middle_removal() {
    install_test_seams();
    fn const_hash(_key: &[u8], _keysize: Size) -> u32 {
        7
    }
    let mut info = ctl(4, 8);
    info.hash = Some(const_hash);
    let table = hash_create("collide", 8, &info, HASH_ELEM | HASH_FUNCTION).unwrap();
    unsafe {
        for i in 0u32..5 {
            let (_, found) = search(table, i.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
            assert!(!found);
        }
        let (_, found) = search(table, 2u32.to_ne_bytes().as_ptr(), HASH_REMOVE).unwrap();
        assert!(found);
        for i in [0u32, 1, 3, 4] {
            let (_, found) = search(table, i.to_ne_bytes().as_ptr(), HASH_FIND).unwrap();
            assert!(found, "key {i} lost after removing a chain neighbor");
        }
        let (_, found) = search(table, 2u32.to_ne_bytes().as_ptr(), HASH_FIND).unwrap();
        assert!(!found);
        assert_eq!(hash_get_num_entries(table), 4);
    }
    hash_destroy(table);
}

#[test]
fn expansion_tracks_fill_factor_and_round_trips() {
    install_test_seams();
    let ctl = ctl(4, 8);
    let table = hash_create("grow", 4, &ctl, HASH_ELEM | HASH_BLOBS).unwrap();
    let n: u32 = 1000;
    unsafe {
        let initial_max_bucket = (*(*table).hctl).max_bucket;
        assert_eq!(initial_max_bucket, 3);
        for i in 0..n {
            let key = i.to_ne_bytes();
            let (p, found) = search(table, key.as_ptr(), HASH_ENTER).unwrap();
            assert!(!found, "key {i} should be new");
            entry(p)[4..8].copy_from_slice(&(i.wrapping_mul(7)).to_ne_bytes());
        }
        assert_eq!(hash_get_num_entries(table), n as i64);
        let hctl = (*table).hctl;
        assert!((*hctl).max_bucket as i64 + 1 >= n as i64, "ffactor 1: nentries <= max_bucket+1");
        assert!((*hctl).nsegs > 1, "growth past ssize=256 buckets allocates segments");
        for i in 0..n {
            let key = i.to_ne_bytes();
            let (p, found) = search(table, key.as_ptr(), HASH_FIND).unwrap();
            assert!(found, "key {i} should be found after growth");
            assert_eq!(&entry(p)[4..8], &(i.wrapping_mul(7)).to_ne_bytes());
        }
        let mut scan = HASH_SEQ_STATUS::new();
        hash_seq_init(&mut scan, table).unwrap();
        let mut seen = 0;
        while !hash_seq_search(&mut scan).unwrap().is_null() {
            seen += 1;
        }
        assert_eq!(seen, n as usize);
    }
    hash_destroy(table);
}

#[test]
fn sequence_scan_sees_entries_and_terms() {
    install_test_seams();
    let ctl = ctl(4, 8);
    let table = hash_create("scan", 8, &ctl, HASH_ELEM | HASH_BLOBS).unwrap();
    unsafe {
        search(table, 1u32.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
        search(table, 2u32.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
        let mut scan = HASH_SEQ_STATUS::new();
        hash_seq_init(&mut scan, table).unwrap();
        let mut count = 0;
        while !hash_seq_search(&mut scan).unwrap().is_null() {
            count += 1;
        }
        assert_eq!(count, 2);
        AtEOXact_HashTables(false);
    }
    hash_destroy(table);
}

#[test]
fn seq_scan_blocks_expansion_until_terminated() {
    install_test_seams();
    let ctl = ctl(4, 8);
    let table = hash_create("noexpand", 4, &ctl, HASH_ELEM | HASH_BLOBS).unwrap();
    unsafe {
        let mut scan = HASH_SEQ_STATUS::new();
        hash_seq_init(&mut scan, table).unwrap();
        for i in 0u32..64 {
            search(table, i.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
        }
        assert_eq!((*(*table).hctl).max_bucket, 3, "active scan must inhibit splits");
        hash_seq_term(&mut scan).unwrap();
        search(table, 64u32.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
        assert!((*(*table).hctl).max_bucket > 3, "split resumes after scan ends");
    }
    hash_destroy(table);
}

#[test]
fn seq_scan_with_hash_value_filters_bucket() {
    install_test_seams();
    let ctl = ctl(4, 8);
    let table = hash_create("hvscan", 32, &ctl, HASH_ELEM | HASH_BLOBS).unwrap();
    unsafe {
        for i in 0u32..32 {
            search(table, i.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
        }
        let hv = get_hash_value(table, 5u32.to_ne_bytes().as_ptr());
        let mut scan = HASH_SEQ_STATUS::new();
        hash_seq_init_with_hash_value(&mut scan, table, hv).unwrap();
        let mut hits = 0;
        loop {
            let p = hash_seq_search(&mut scan).unwrap();
            if p.is_null() {
                break;
            }
            assert_eq!(get_hash_value(table, p), hv);
            hits += 1;
        }
        assert!(hits >= 1);
    }
    hash_destroy(table);
}

#[test]
fn update_hash_key_moves_entry() {
    install_test_seams();
    let ctl = ctl(4, 8);
    let table = hash_create("update", 8, &ctl, HASH_ELEM | HASH_BLOBS).unwrap();
    unsafe {
        let (p, _) = search(table, 1u32.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
        assert!(hash_update_hash_key(table, p, 2u32.to_ne_bytes().as_ptr()).unwrap());
        let (_, found) = search(table, 1u32.to_ne_bytes().as_ptr(), HASH_FIND).unwrap();
        assert!(!found);
        let (_, found) = search(table, 2u32.to_ne_bytes().as_ptr(), HASH_FIND).unwrap();
        assert!(found);
    }
    hash_destroy(table);
}

#[test]
fn update_hash_key_refuses_clobber() {
    install_test_seams();
    let ctl = ctl(4, 8);
    let table = hash_create("update2", 8, &ctl, HASH_ELEM | HASH_BLOBS).unwrap();
    unsafe {
        let (p, _) = search(table, 1u32.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
        search(table, 2u32.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
        assert!(!hash_update_hash_key(table, p, 2u32.to_ne_bytes().as_ptr()).unwrap());
        assert_eq!(hash_get_num_entries(table), 2);
    }
    hash_destroy(table);
}

#[test]
fn freeze_blocks_inserts() {
    install_test_seams();
    let ctl = ctl(4, 8);
    let table = hash_create("freeze", 8, &ctl, HASH_ELEM | HASH_BLOBS).unwrap();
    unsafe {
        search(table, 1u32.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
        hash_freeze(table).unwrap();
        let (_, found) = search(table, 1u32.to_ne_bytes().as_ptr(), HASH_FIND).unwrap();
        assert!(found);
        assert!(search(table, 2u32.to_ne_bytes().as_ptr(), HASH_ENTER).is_err());
    }
    hash_destroy(table);
}

#[test]
fn freeze_with_active_scan_errors() {
    install_test_seams();
    let ctl = ctl(4, 8);
    let table = hash_create("freeze2", 8, &ctl, HASH_ELEM | HASH_BLOBS).unwrap();
    unsafe {
        search(table, 1u32.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
        let mut scan = HASH_SEQ_STATUS::new();
        hash_seq_init(&mut scan, table).unwrap();
        assert!(hash_freeze(table).is_err());
        hash_seq_term(&mut scan).unwrap();
    }
    hash_destroy(table);
}

#[test]
fn partitioned_freelists_spread_lock_and_borrow() {
    install_test_seams();
    let ctl = ctl(4, 8);
    // HASH_PARTITION requires shmem in hash_create; flip the header directly to
    // exercise the partitioned freelist/spinlock/borrow machinery.
    let table = hash_create("part", 32, &ctl, HASH_ELEM | HASH_BLOBS | HASH_FIXED_SIZE).unwrap();
    unsafe {
        let hctl = (*table).hctl;
        (*hctl).num_partitions = 4;
        for i in 0..NUM_FREELISTS {
            SpinLockInit(&mut (*hctl).freeList[i].mutex);
        }

        let mut hit_nonzero_freelist = false;
        for i in 0u32..24 {
            let hv = get_hash_value(table, i.to_ne_bytes().as_ptr());
            hit_nonzero_freelist |= (hv as usize) % NUM_FREELISTS != 0;
            let (p, found) =
                search_hv(table, i.to_ne_bytes().as_ptr(), hv, HASH_ENTER)
                    .unwrap();
            assert!(!found);
            assert!(!p.is_null(), "isfixed borrow path must scavenge freelist 0");
        }
        assert!(hit_nonzero_freelist);
        assert_eq!(hash_get_num_entries(table), 24);
        for i in 0u32..24 {
            let (_, found) = search(table, i.to_ne_bytes().as_ptr(), HASH_FIND).unwrap();
            assert!(found);
        }
        for i in 0u32..24 {
            let (_, found) = search(table, i.to_ne_bytes().as_ptr(), HASH_REMOVE).unwrap();
            assert!(found);
        }
        assert_eq!(hash_get_num_entries(table), 0);
    }
    hash_destroy(table);
}

#[test]
fn partitioned_shared_fixed_create_works() {
    install_test_seams();
    let mut info = ctl(4, 8);
    info.num_partitions = 4;
    let table = hash_create(
        "part_shared",
        128,
        &info,
        HASH_ELEM | HASH_BLOBS | HASH_PARTITION | HASH_SHARED_MEM | HASH_FIXED_SIZE,
    )
    .unwrap();
    unsafe {
        assert!((*table).isshared);
        assert!((*table).isfixed);
        for i in 0u32..96 {
            let hv = get_hash_value(table, i.to_ne_bytes().as_ptr());
            let (p, found) = search_hv(table, i.to_ne_bytes().as_ptr(), hv, HASH_ENTER).unwrap();
            assert!(!found);
            assert!(!p.is_null());
        }
        assert_eq!(hash_get_num_entries(table), 96);
        let before = (*(*table).hctl).max_bucket;
        for i in 0u32..96 {
            let (_, found) = search(table, i.to_ne_bytes().as_ptr(), HASH_FIND).unwrap();
            assert!(found);
        }
        assert_eq!((*(*table).hctl).max_bucket, before, "partitioned tables never split");
        for i in 0u32..96 {
            let (_, found) = search(table, i.to_ne_bytes().as_ptr(), HASH_REMOVE).unwrap();
            assert!(found);
        }
        assert_eq!(hash_get_num_entries(table), 0);
    }
    hash_destroy(table);
}

#[test]
fn reset_after_crash_restores_boot_image() {
    install_test_seams();
    let mut info = ctl(4, 8);
    info.num_partitions = 4;
    let table = hash_create(
        "part_shared_reset",
        128,
        &info,
        HASH_ELEM | HASH_BLOBS | HASH_PARTITION | HASH_SHARED_MEM | HASH_FIXED_SIZE,
    )
    .unwrap();
    unsafe {
        for i in 0u32..128 {
            let (p, _) = search(table, i.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
            assert!(!p.is_null());
        }
        assert_eq!(hash_get_num_entries(table), 128);
        // Crash leaves a freelist spinlock held; reset must re-arm it.
        SpinLockAcquire(&mut (*(*table).hctl).freeList[0].mutex);

        hash_reset_after_crash(table);

        assert_eq!(hash_get_num_entries(table), 0);
        for i in 0u32..128 {
            let (p, found) = search(table, i.to_ne_bytes().as_ptr(), HASH_FIND).unwrap();
            assert!(!found);
            assert!(p.is_null());
        }
        // The fixed table must again hold its full preallocated population.
        for i in 1000u32..1128 {
            let (p, found) = search(table, i.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
            assert!(!found);
            assert!(!p.is_null());
        }
        assert_eq!(hash_get_num_entries(table), 128);
    }
    hash_destroy(table);
}

#[test]
#[should_panic(expected = "HASH_SHARED_MEM requires HASH_FIXED_SIZE")]
fn shared_without_fixed_size_panics() {
    install_test_seams();
    let ctl = ctl(4, 8);
    let _ = hash_create("shpanic", 8, &ctl, HASH_ELEM | HASH_BLOBS | HASH_SHARED_MEM);
}

#[test]
fn hash_context_links_accounting_parent() {
    install_test_seams();
    let parent = MemoryContext::new("dynahash-test-parent");
    let mut info = ctl(4, 8);
    info.hcxt = &parent as *const MemoryContext as *mut u8;
    let table = hash_create("child", 8, &info, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT).unwrap();
    unsafe {
        search(table, 1u32.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
    }
    assert!(parent.subtree_used() > 0);
    hash_destroy(table);
}

#[test]
fn get_hash_value_matches_search_lane() {
    install_test_seams();
    let ctl = ctl(4, 8);
    let table = hash_create("hv", 8, &ctl, HASH_ELEM | HASH_BLOBS).unwrap();
    let key = 7u32.to_ne_bytes();
    unsafe {
        search(table, key.as_ptr(), HASH_ENTER).unwrap();
        let hv = get_hash_value(table, key.as_ptr());
        let (_, found) =
            search_hv(table, key.as_ptr(), hv, HASH_FIND).unwrap();
        assert!(found);
    }
    hash_destroy(table);
}

#[test]
fn estimates_have_expected_monotonicity() {
    assert!(hash_estimate_size(100, 16) < hash_estimate_size(200, 16));
    assert_eq!(hash_select_dirsize(1), DEF_DIRSIZE);
    assert_eq!(my_log2(1), 0);
    assert_eq!(my_log2(8), 3);
    assert_eq!(my_log2(9), 4);
    let mut info = ctl(8, 32);
    info.dsize = hash_select_dirsize(1000);
    info.max_dsize = info.dsize;
    let sz = hash_get_shared_size(&info, HASH_DIRSIZE);
    assert!(sz > size_of::<HASHHDR>());
}

#[test]
fn subxact_cleanup_drops_only_deeper_scans() {
    install_test_seams();
    let ctl = ctl(4, 8);
    let table = hash_create("subxact", 8, &ctl, HASH_ELEM | HASH_BLOBS).unwrap();
    unsafe {
        search(table, 1u32.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
        let mut scan = HASH_SEQ_STATUS::new();
        hash_seq_init(&mut scan, table).unwrap();
        AtEOSubXact_HashTables(false, 2);
        hash_seq_term(&mut scan).unwrap();
        let mut scan2 = HASH_SEQ_STATUS::new();
        hash_seq_init(&mut scan2, table).unwrap();
        AtEOSubXact_HashTables(false, 1);
        assert!(hash_seq_term(&mut scan2).is_err(), "scan at level>=depth was dropped");
    }
    hash_destroy(table);
}

// GL-VACGUARD-1 row 5: the freeList spinlock's contended path must go through
// C's perform_spin_delay backoff (s_lock.c:97), not an unbounded busy-spin.
//
// The seam IS the observation point, which is what makes this deterministic: the
// stub below is the only thing that ever releases the lock word, so a build that
// backs off completes and a build that pure-spins never does. Before the fix the
// counter reads 0 and the acquire never returns; the bounded join turns that
// hang into a reported failure instead of a stuck test binary.
//
// The stuck-spinlock valve itself (NUM_DELAYS, C's `PANIC: stuck spinlock
// detected`) is deliberately NOT exercised here: with MIN_DELAY_USEC=1000 growing
// to a 1s cap, 1000 delays is several minutes of wall time. It is covered by
// construction -- the fix routes through the same perform_spin_delay that owns
// the valve, which is exactly what this test proves.
#[test]
fn freelist_spin_uses_perform_spin_delay_backoff() {
    use std::sync::atomic::{AtomicI32, AtomicU32, Ordering};

    static DELAY_CALLS: AtomicU32 = AtomicU32::new(0);
    static LOCKWORD: AtomicI32 = AtomicI32::new(0);

    // Seams are set-once per process; this is the only test that sets it.
    s_lock_seams::perform_spin_delay::set(|_status| {
        DELAY_CALLS.fetch_add(1, Ordering::Relaxed);
        // Stand in for the real holder releasing: the ONLY release in this test.
        LOCKWORD.store(0, Ordering::Release);
    });
    s_lock_seams::finish_spin_delay::set(|_status| {});

    // Hold the lock, then have a waiter contend for it.
    LOCKWORD.store(1, Ordering::Release);
    let handle = std::thread::spawn(|| {
        // SAFETY: LOCKWORD is a 'static AtomicI32; nothing else writes it once
        // the waiter starts except the delay stub above.
        unsafe { super::SpinLockAcquire(&LOCKWORD as *const AtomicI32 as *mut AtomicI32) };
    });

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !handle.is_finished() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(
        handle.is_finished(),
        "contended acquire never returned: the spin path is not calling \
         perform_spin_delay (delay calls: {})",
        DELAY_CALLS.load(Ordering::Relaxed)
    );
    handle.join().expect("waiter panicked");
    assert!(
        DELAY_CALLS.load(Ordering::Relaxed) >= 1,
        "contended acquire completed without any backoff call — it busy-spun"
    );
    assert_eq!(LOCKWORD.load(Ordering::Relaxed), 1, "acquire must leave the lock held");
}
