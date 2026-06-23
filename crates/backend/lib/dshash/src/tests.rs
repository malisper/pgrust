//! End-to-end tests of the ported algorithm against a backend-local DSA
//! simulator installed behind the DSA / lwlock / hashfn owner seams.
//!
//! The simulator maps each `dsa_pointer` to an owned heap allocation whose
//! address `dsa_get_address` returns, so the ported raw-pointer reads/writes hit
//! real memory; the per-partition `LWLock`s are exercised single-threaded (the
//! lwlock seam is a no-op acquire/release here). All tests serialize on a
//! process `TEST_LOCK` because the simulator and the seam SLOTs are shared.

use super::*;

use alloc::vec;
use alloc::vec::Vec;
use std::collections::HashMap;
use std::sync::{Mutex, Once, OnceLock};

use types_storage::DshashKeyKind;

/// Serializes every test (shared simulator + global seam slots).
fn test_lock() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
}

/// The simulated DSA arena: `dsa_pointer` -> owned, leaked allocation. A pointer
/// is the allocation's heap address (nonzero, so a valid `DsaPointer`).
struct Arena {
    /// Live allocations: address -> boxed bytes (kept alive; freed on dsa_free).
    live: HashMap<u64, Vec<u8>>,
}

fn arena() -> std::sync::MutexGuard<'static, Arena> {
    static ARENA: OnceLock<Mutex<Arena>> = OnceLock::new();
    ARENA
        .get_or_init(|| {
            Mutex::new(Arena {
                live: HashMap::new(),
            })
        })
        .lock()
        .unwrap()
}

fn install_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // DSA substrate: a `dsa_pointer` is the heap address of a leaked Vec.
        dsa::dsa_allocate_extended::set(|_area, size, flags| {
            let mut buf = vec![0u8; size];
            if flags & DSA_ALLOC_ZERO != 0 {
                buf.iter_mut().for_each(|b| *b = 0);
            }
            let addr = buf.as_ptr() as u64;
            arena().live.insert(addr, buf);
            Ok(addr)
        });
        dsa::dsa_free_ptr::set(|_area, dp| {
            arena().live.remove(&dp);
            Ok(())
        });
        dsa::dsa_get_address_ptr::set(|_area, dp| Ok(dp));

        // LWLock: single-threaded no-op acquire/release.
        lwlock::lwlock_initialize::set(|_lock, _tranche| {});
        lwlock::lwlock_acquire::set(|lock, _mode, _proc| {
            Ok(lwlock::LWLockGuard::new(lock, true))
        });
        lwlock::lwlock_release::set(|_lock| Ok(()));

        // MyProcNumber.
        my_proc_number::set(|| 0);

        // hashfn: deterministic byte-tag / string hashes for the test.
        hashfn::tag_hash::set(|key, size| fnv(&key[..size.min(key.len())]));
        hashfn::string_hash::set(|key, size| {
            let n = key[..size.min(key.len())]
                .iter()
                .position(|&b| b == 0)
                .unwrap_or(size.min(key.len()));
            fnv(&key[..n])
        });
    });
}

/// A small deterministic 32-bit hash (FNV-1a) — not PG's, but the test only
/// needs a stable distribution across partitions.
fn fnv(bytes: &[u8]) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for &b in bytes {
        h ^= b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

fn binary_params(key_size: usize, entry_size: usize) -> DshashParameters {
    DshashParameters {
        key_size,
        entry_size,
        key_kind: DshashKeyKind::Binary,
        tranche_id: 0,
    }
}

const AREA: *mut DsaArea = 1 as *mut DsaArea;

/// Read the `entry_size` bytes at an entry pointer.
unsafe fn entry_bytes(entry: *mut u8, n: usize) -> Vec<u8> {
    core::slice::from_raw_parts(entry, n).to_vec()
}

#[test]
fn item_header_layout_matches_c() {
    // sizeof(dshash_table_item) == 16 on a 64-bit build (8 next + 4 hash + pad);
    // MAXALIGN == 16.
    assert_eq!(core::mem::size_of::<dshash_table_item>(), 16);
    assert_eq!(item_header_size(), 16);
    assert_eq!(DSHASH_NUM_PARTITIONS, 128);
}

#[test]
fn partition_and_bucket_math() {
    // PARTITION_FOR_HASH uses the top 7 bits.
    assert_eq!(partition_for_hash(0xFE00_0000), 0x7F);
    assert_eq!(partition_for_hash(0x0000_0000), 0);
    // At the initial size (log2 == 7) there is one bucket per partition.
    assert_eq!(num_buckets(7), 128);
    assert_eq!(buckets_per_partition(7), 1);
    assert_eq!(num_splits(8), 1);
    assert_eq!(buckets_per_partition(8), 2);
}

#[test]
fn create_insert_find_mutate_delete() {
    let _g = test_lock();
    install_seams();

    // key = first 4 bytes (a u32), entry = key + 4-byte value.
    let params = binary_params(4, 8);
    let table = dshash_create(AREA, &params).unwrap();

    let key = 42u32.to_ne_bytes();
    let mut found = true;
    let entry = dshash_find_or_insert(table, &key, &mut found).unwrap();
    assert!(!found);
    // Write a value into the entry's value field.
    unsafe {
        core::slice::from_raw_parts_mut(entry, 8)[4..].copy_from_slice(&7u32.to_ne_bytes());
    }
    dshash_release_lock(table, entry).unwrap();

    // Find it again and read the value back.
    let entry = dshash_find(table, &key, false).unwrap().unwrap();
    let bytes = unsafe { entry_bytes(entry, 8) };
    assert_eq!(&bytes[..4], &key);
    assert_eq!(&bytes[4..], &7u32.to_ne_bytes());
    dshash_release_lock(table, entry).unwrap();

    // find_or_insert again => found.
    let mut found = false;
    let entry = dshash_find_or_insert(table, &key, &mut found).unwrap();
    assert!(found);
    dshash_release_lock(table, entry).unwrap();

    // Delete it.
    assert!(dshash_delete_key(table, &key).unwrap());
    assert!(dshash_find(table, &key, false).unwrap().is_none());
    assert!(!dshash_delete_key(table, &key).unwrap());

    dshash_destroy(table).unwrap();
}

#[test]
fn handle_round_trip() {
    let _g = test_lock();
    install_seams();

    let params = binary_params(4, 8);
    let table = dshash_create(AREA, &params).unwrap();
    let handle = dshash_get_hash_table_handle(table);
    assert!(handle != INVALID_DSA_POINTER);

    // Attach a second view from the handle and find an inserted key.
    let key = 5u32.to_ne_bytes();
    let mut found = false;
    let e = dshash_find_or_insert(table, &key, &mut found).unwrap();
    dshash_release_lock(table, e).unwrap();

    let attached = dshash_attach(AREA, &params, handle, ).unwrap();
    let e = dshash_find(attached, &key, false).unwrap();
    assert!(e.is_some());
    dshash_release_lock(attached, e.unwrap()).unwrap();
    dshash_detach(attached);

    dshash_destroy(table).unwrap();
}

#[test]
fn delete_entry_on_locked_entry() {
    let _g = test_lock();
    install_seams();

    let params = binary_params(4, 8);
    let table = dshash_create(AREA, &params).unwrap();

    let key = 99u32.to_ne_bytes();
    let mut found = false;
    let entry = dshash_find_or_insert(table, &key, &mut found).unwrap();
    assert!(!found);
    dshash_delete_entry(table, entry).unwrap();
    assert!(dshash_find(table, &key, false).unwrap().is_none());

    dshash_destroy(table).unwrap();
}

#[test]
fn resize_keeps_all_keys() {
    let _g = test_lock();
    install_seams();

    let params = binary_params(4, 8);
    let table = dshash_create(AREA, &params).unwrap();

    let n = 4000u32;
    for k in 0..n {
        let key = k.to_ne_bytes();
        let mut found = false;
        let entry = dshash_find_or_insert(table, &key, &mut found).unwrap();
        assert!(!found, "key {k} unexpectedly found");
        unsafe {
            core::slice::from_raw_parts_mut(entry, 8)[4..]
                .copy_from_slice(&k.wrapping_mul(3).to_ne_bytes());
        }
        dshash_release_lock(table, entry).unwrap();
    }

    for k in 0..n {
        let key = k.to_ne_bytes();
        let entry = dshash_find(table, &key, false).unwrap().unwrap();
        let bytes = unsafe { entry_bytes(entry, 8) };
        assert_eq!(&bytes[4..], &k.wrapping_mul(3).to_ne_bytes(), "value for {k}");
        dshash_release_lock(table, entry).unwrap();
    }

    dshash_destroy(table).unwrap();
}

#[test]
fn seq_scan_with_delete_current() {
    let _g = test_lock();
    install_seams();

    let params = binary_params(4, 8);
    let table = dshash_create(AREA, &params).unwrap();

    let n = 200u32;
    for k in 0..n {
        let key = k.to_ne_bytes();
        let mut found = false;
        let e = dshash_find_or_insert(table, &key, &mut found).unwrap();
        dshash_release_lock(table, e).unwrap();
    }

    // Scan, deleting even keys mid-scan.
    let mut status = dshash_seq_init(table, true);
    let mut seen = 0usize;
    while let Some(entry) = dshash_seq_next(&mut status).unwrap() {
        let bytes = unsafe { entry_bytes(entry, 4) };
        let k = u32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        seen += 1;
        if k % 2 == 0 {
            dshash_delete_current(&mut status).unwrap();
        }
    }
    assert_eq!(seen, n as usize);
    dshash_seq_term(&mut status).unwrap();

    // Only odd keys remain.
    for k in 0..n {
        let key = k.to_ne_bytes();
        let present = dshash_find(table, &key, false).unwrap();
        if k % 2 == 0 {
            assert!(present.is_none(), "even key {k} should be gone");
        } else {
            let e = present.expect("odd key missing");
            dshash_release_lock(table, e).unwrap();
        }
    }

    dshash_destroy(table).unwrap();
}

#[test]
fn convenience_callbacks() {
    let _g = test_lock();
    install_seams();

    assert_eq!(dshash_memcmp(b"abc", b"abc", 3), 0);
    assert!(dshash_memcmp(b"abd", b"abc", 3) > 0);

    let mut dst = [0u8; 4];
    dshash_memcpy(&mut dst, b"abcd", 4);
    assert_eq!(&dst, b"abcd");

    assert_eq!(dshash_strcmp(b"hi\0\0", b"hi\0\0", 4), 0);
    assert!(dshash_strcmp(b"hj\0\0", b"hi\0\0", 4) > 0);

    let mut dst = [0xFFu8; 4];
    dshash_strcpy(&mut dst, b"ab\0", 4);
    assert_eq!(&dst[..3], b"ab\0");

    assert_eq!(dshash_memhash(b"abc", 3), fnv(b"abc"));
    assert_eq!(dshash_strhash(b"ab\0x", 4), fnv(b"ab"));
}
