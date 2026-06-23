//! Unit tests for the dynahash port. They exercise the local-table path (the
//! shared path needs the IPC segment) over the raw-pointer C-faithful API.
//!
//! dynahash routes its key hashing through `common-hashfn` and scan
//! registration through `xact`; neither is ported in this crate's build, so the
//! tests install deterministic seam stubs first (the table is correct for any
//! consistent hash function).

// Several tests touch raw entry bytes (genuinely unsafe); others only call the
// raw-pointer public API (which is safe to invoke), so keep one `unsafe` block
// per test for uniformity.
#![allow(unused_unsafe)]

use super::*;
use core::sync::atomic::{AtomicBool, Ordering};
use ::hash::hsearch::{
    HASHCTL, HASH_BLOBS, HASH_DIRSIZE, HASH_ELEM, HASH_ENTER, HASH_ENTER_NULL, HASH_FIND,
    HASH_FIXED_SIZE, HASH_REMOVE, HASH_STRINGS,
};

static SEAMS_READY: AtomicBool = AtomicBool::new(false);

fn install_test_seams() {
    if SEAMS_READY.swap(true, Ordering::SeqCst) {
        return;
    }
    // A simple FNV-1a over the first `keysize` bytes (string: stop at NUL).
    fn fnv(key: &[u8], keysize: Size) -> u32 {
        let mut h: u32 = 0x811c_9dc5;
        for &b in &key[..keysize] {
            h ^= b as u32;
            h = h.wrapping_mul(0x0100_0193);
        }
        h
    }
    fn strhash(key: &[u8], keysize: Size) -> u32 {
        let n = key[..keysize].iter().position(|&b| b == 0).unwrap_or(keysize);
        fnv(key, n)
    }
    hashfn_seams::tag_hash::set(fnv);
    hashfn_seams::string_hash::set(strhash);
    hashfn_seams::hash_bytes_uint32::set(|k| k.wrapping_mul(0x9e37_79b1));
    transam_xact_seams::get_current_transaction_nest_level::set(|| 1);
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
        let (p, found) = hash_search(table, key.as_ptr(), HASH_ENTER).unwrap();
        assert!(!found);
        let e = core::slice::from_raw_parts_mut(p, 12);
        e[4..8].copy_from_slice(&99u32.to_ne_bytes());

        let (p, found) = hash_search(table, key.as_ptr(), HASH_FIND).unwrap();
        assert!(found);
        let e = core::slice::from_raw_parts(p, 12);
        assert_eq!(&e[4..8], &99u32.to_ne_bytes());

        let (_, found) = hash_search(table, key.as_ptr(), HASH_REMOVE).unwrap();
        assert!(found);
        let (_, found) = hash_search(table, key.as_ptr(), HASH_FIND).unwrap();
        assert!(!found);
        assert_eq!(hash_get_num_entries(table), 0);
    }
    hash_destroy(table);
}

#[test]
fn string_keys_truncate_at_nul_and_keysize_minus_one() {
    install_test_seams();
    let ctl = ctl(16, 24);
    let table = hash_create("strings", 8, &ctl, HASH_ELEM | HASH_STRINGS).unwrap();
    unsafe {
        hash_search(table, b"abc\0tail\0\0\0\0\0\0\0\0".as_ptr(), HASH_ENTER).unwrap();
        let (_, found) =
            hash_search(table, b"abc\0zzzz\0\0\0\0\0\0\0\0".as_ptr(), HASH_FIND).unwrap();
        assert!(found);
        let (_, found) =
            hash_search(table, b"abcdzzzz\0\0\0\0\0\0\0\0".as_ptr(), HASH_FIND).unwrap();
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
        hash_search(table, 1u32.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
        let (p, found) = hash_search(table, 2u32.to_ne_bytes().as_ptr(), HASH_ENTER_NULL).unwrap();
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
        hash_search(table, 1u32.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
        assert!(hash_search(table, 2u32.to_ne_bytes().as_ptr(), HASH_ENTER).is_err());
    }
    hash_destroy(table);
}

#[test]
fn sequence_scan_sees_entries_and_terms() {
    install_test_seams();
    let ctl = ctl(4, 8);
    let table = hash_create("scan", 8, &ctl, HASH_ELEM | HASH_BLOBS).unwrap();
    unsafe {
        hash_search(table, 1u32.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
        hash_search(table, 2u32.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
        let mut scan = HASH_SEQ_STATUS::new();
        hash_seq_init(&mut scan, table);
        let mut count = 0;
        while !hash_seq_search(&mut scan).unwrap().is_null() {
            count += 1;
        }
        assert_eq!(count, 2);
        AtEOXact_HashTables(true);
    }
    hash_destroy(table);
}

#[test]
fn many_entries_grow_and_round_trip() {
    install_test_seams();
    let ctl = ctl(4, 8);
    let table = hash_create("grow", 4, &ctl, HASH_ELEM | HASH_BLOBS).unwrap();
    let n: u32 = 1000;
    unsafe {
        for i in 0..n {
            let key = i.to_ne_bytes();
            let (p, found) = hash_search(table, key.as_ptr(), HASH_ENTER).unwrap();
            assert!(!found, "key {i} should be new");
            entry(p)[4..8].copy_from_slice(&(i.wrapping_mul(7)).to_ne_bytes());
        }
        assert_eq!(hash_get_num_entries(table), n as i64);
        for i in 0..n {
            let key = i.to_ne_bytes();
            let (p, found) = hash_search(table, key.as_ptr(), HASH_FIND).unwrap();
            assert!(found, "key {i} should be found after growth");
            assert_eq!(&entry(p)[4..8], &(i.wrapping_mul(7)).to_ne_bytes());
        }
        let mut scan = HASH_SEQ_STATUS::new();
        hash_seq_init(&mut scan, table);
        let mut seen = 0;
        while !hash_seq_search(&mut scan).unwrap().is_null() {
            seen += 1;
        }
        assert_eq!(seen, n as usize);
    }
    hash_destroy(table);
}

#[test]
fn update_hash_key_moves_entry() {
    install_test_seams();
    let ctl = ctl(4, 8);
    let table = hash_create("update", 8, &ctl, HASH_ELEM | HASH_BLOBS).unwrap();
    unsafe {
        let (p, _) = hash_search(table, 1u32.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
        assert!(hash_update_hash_key(table, p, 2u32.to_ne_bytes().as_ptr()).unwrap());
        let (_, found) = hash_search(table, 1u32.to_ne_bytes().as_ptr(), HASH_FIND).unwrap();
        assert!(!found);
        let (_, found) = hash_search(table, 2u32.to_ne_bytes().as_ptr(), HASH_FIND).unwrap();
        assert!(found);
    }
    hash_destroy(table);
}

#[test]
fn update_hash_key_refuses_clobber() {
    install_test_seams();
    let ctl = ctl(4, 8);
    let table = hash_create("update", 8, &ctl, HASH_ELEM | HASH_BLOBS).unwrap();
    unsafe {
        let (p, _) = hash_search(table, 1u32.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
        hash_search(table, 2u32.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
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
        hash_search(table, 1u32.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
        hash_freeze(table).unwrap();
        let (_, found) = hash_search(table, 1u32.to_ne_bytes().as_ptr(), HASH_FIND).unwrap();
        assert!(found);
        assert!(hash_search(table, 2u32.to_ne_bytes().as_ptr(), HASH_ENTER).is_err());
        let _ = hash_search(table, 3u32.to_ne_bytes().as_ptr(), HASH_ENTER_NULL);
    }
    hash_destroy(table);
}

#[test]
fn freeze_with_active_scan_errors() {
    install_test_seams();
    let ctl = ctl(4, 8);
    let table = hash_create("freeze2", 8, &ctl, HASH_ELEM | HASH_BLOBS).unwrap();
    unsafe {
        hash_search(table, 1u32.to_ne_bytes().as_ptr(), HASH_ENTER).unwrap();
        let mut scan = HASH_SEQ_STATUS::new();
        hash_seq_init(&mut scan, table);
        assert!(hash_freeze(table).is_err());
        hash_seq_term(&mut scan).unwrap();
    }
    hash_destroy(table);
}

#[test]
fn get_hash_value_matches_internal() {
    install_test_seams();
    let ctl = ctl(4, 8);
    let table = hash_create("hv", 8, &ctl, HASH_ELEM | HASH_BLOBS).unwrap();
    let key = 7u32.to_ne_bytes();
    assert_eq!(
        get_hash_value(table, key.as_ptr()),
        get_hash_value(table, key.as_ptr())
    );
    hash_destroy(table);
}

#[test]
fn estimates_have_expected_monotonicity() {
    assert!(hash_estimate_size(100, 16) < hash_estimate_size(200, 16));
    assert_eq!(hash_select_dirsize(1), DEF_DIRSIZE);
    assert_eq!(my_log2(1), 0);
    assert_eq!(my_log2(8), 3);
    let mut info = ctl(8, 32);
    info.dsize = hash_select_dirsize(1000);
    info.max_dsize = info.dsize;
    let sz = hash_get_shared_size(&info, HASH_DIRSIZE);
    assert!(sz > core::mem::size_of::<HASHHDR>());
}
