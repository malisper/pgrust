//! Tests for the extensible-node / custom-scan registry port. They exercise the
//! exact `extensible.c` control flow: lazy `HTAB` creation, the
//! `EXTNODENAME_MAX_LEN` length guard, duplicate detection
//! (`ERRCODE_DUPLICATE_OBJECT`), and `missing_ok` lookups vs. the
//! `ERRCODE_UNDEFINED_OBJECT` "was not registered" error.

use std::ffi::CString;
use std::sync::Once;

use types_error::{ERRCODE_DUPLICATE_OBJECT, ERRCODE_UNDEFINED_OBJECT};
use types_extensible::{CustomScan, ExtensibleNode, Node, StringInfoData};

use super::*;

// Dummy mandatory callbacks. The registry never invokes them; they exist only
// so the (non-nullable) method-table fn-pointer fields are valid.
extern "C" fn dummy_copy(_n: *mut ExtensibleNode, _o: *const ExtensibleNode) {}
extern "C" fn dummy_equal(_a: *const ExtensibleNode, _b: *const ExtensibleNode) -> bool {
    false
}
extern "C" fn dummy_out(_s: *mut StringInfoData, _n: *const ExtensibleNode) {}
extern "C" fn dummy_read(_n: *mut ExtensibleNode) {}
extern "C" fn dummy_create_state(_c: *mut CustomScan) -> *mut Node {
    core::ptr::null_mut()
}

// dynahash routes hashing/comparison through `common_hashfn_seams` and reads
// the transaction nest level via `backend_access_transam_xact_seams`. Install
// simple stand-ins once so the local registries can be created and searched.
static SEAMS: Once = Once::new();

fn install_seams() {
    SEAMS.call_once(|| {
        fn fnv(key: &[u8], keysize: usize) -> u32 {
            let mut h: u32 = 0x811c_9dc5;
            for &b in &key[..keysize] {
                h ^= b as u32;
                h = h.wrapping_mul(0x0100_0193);
            }
            h
        }
        fn strhash(key: &[u8], keysize: usize) -> u32 {
            let n = key[..keysize].iter().position(|&b| b == 0).unwrap_or(keysize);
            fnv(key, n)
        }
        common_hashfn_seams::tag_hash::set(fnv);
        common_hashfn_seams::string_hash::set(strhash);
        common_hashfn_seams::hash_bytes_uint32::set(|k| k.wrapping_mul(0x9e37_79b1));
        backend_access_transam_xact_seams::get_current_transaction_nest_level::set(|| 1);
    });
}

// Build an `ExtensibleNodeMethods` table named `name`. The callbacks are never
// invoked by the registry, so we leave the table's pointer fields uninspected;
// only `extnodename` (the key) matters here.
fn make_methods(name: &CString) -> ExtensibleNodeMethods {
    ExtensibleNodeMethods {
        extnodename: name.as_ptr(),
        node_size: 0,
        nodeCopy: dummy_copy,
        nodeEqual: dummy_equal,
        nodeOut: dummy_out,
        nodeRead: dummy_read,
    }
}

fn make_custom(name: &CString) -> CustomScanMethods {
    CustomScanMethods {
        CustomName: name.as_ptr(),
        CreateCustomScanState: dummy_create_state,
    }
}

#[test]
fn register_then_get_roundtrips_the_table() {
    install_seams();
    let name = CString::new("acme_node").unwrap();
    let methods = make_methods(&name);
    RegisterExtensibleNodeMethods(&methods).unwrap();

    let found = GetExtensibleNodeMethods(name.as_ptr(), false).unwrap();
    assert_eq!(found, &methods as *const ExtensibleNodeMethods);
}

#[test]
fn get_missing_ok_returns_null() {
    install_seams();
    // Table not yet created -> missing_ok must return NULL.
    let absent = CString::new("never_registered_a").unwrap();
    let found = GetExtensibleNodeMethods(absent.as_ptr(), true).unwrap();
    assert!(found.is_null());

    // After a different name is registered (table now exists), a still-absent
    // name with missing_ok must also return NULL (HASH_FIND miss).
    let present = CString::new("present_a").unwrap();
    let m = make_methods(&present);
    RegisterExtensibleNodeMethods(&m).unwrap();
    let found = GetExtensibleNodeMethods(absent.as_ptr(), true).unwrap();
    assert!(found.is_null());
}

#[test]
fn get_missing_not_ok_raises_undefined_object() {
    install_seams();
    let absent = CString::new("absent_node").unwrap();
    let err = GetExtensibleNodeMethods(absent.as_ptr(), false).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_OBJECT);
    assert_eq!(
        err.message(),
        "ExtensibleNodeMethods \"absent_node\" was not registered"
    );
}

#[test]
fn duplicate_registration_raises_duplicate_object() {
    install_seams();
    let name = CString::new("dup_node").unwrap();
    let m1 = make_methods(&name);
    let m2 = make_methods(&name);
    RegisterExtensibleNodeMethods(&m1).unwrap();
    let err = RegisterExtensibleNodeMethods(&m2).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_DUPLICATE_OBJECT);
    assert_eq!(
        err.message(),
        "extensible node type \"dup_node\" already exists"
    );
}

#[test]
fn name_too_long_is_rejected() {
    install_seams();
    // len == EXTNODENAME_MAX_LEN (64) is >= the limit -> "too long".
    let long = CString::new("x".repeat(EXTNODENAME_MAX_LEN)).unwrap();
    let m = make_methods(&long);
    let err = RegisterExtensibleNodeMethods(&m).unwrap_err();
    assert_eq!(err.message(), "extensible node name is too long");

    // len == EXTNODENAME_MAX_LEN - 1 (63) is accepted.
    let ok = CString::new("y".repeat(EXTNODENAME_MAX_LEN - 1)).unwrap();
    let m = make_methods(&ok);
    RegisterExtensibleNodeMethods(&m).unwrap();
    assert!(!GetExtensibleNodeMethods(ok.as_ptr(), false)
        .unwrap()
        .is_null());
}

#[test]
fn custom_scan_registry_is_independent() {
    install_seams();
    // Same name in both registries must not collide (two separate tables).
    let shared = CString::new("shared_name").unwrap();
    let ext = make_methods(&shared);
    let cust = make_custom(&shared);
    RegisterExtensibleNodeMethods(&ext).unwrap();
    RegisterCustomScanMethods(&cust).unwrap();

    assert_eq!(
        GetExtensibleNodeMethods(shared.as_ptr(), false).unwrap(),
        &ext as *const ExtensibleNodeMethods
    );
    assert_eq!(
        GetCustomScanMethods(shared.as_ptr(), false).unwrap(),
        &cust as *const CustomScanMethods
    );

    // Custom-scan duplicate also raises ERRCODE_DUPLICATE_OBJECT.
    let cust2 = make_custom(&shared);
    let err = RegisterCustomScanMethods(&cust2).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_DUPLICATE_OBJECT);

    // Unregistered custom-scan name raises ERRCODE_UNDEFINED_OBJECT.
    let nope = CString::new("no_such_custom").unwrap();
    let err = GetCustomScanMethods(nope.as_ptr(), false).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_OBJECT);
}

#[test]
fn custom_scan_missing_ok_returns_null() {
    install_seams();
    let absent = CString::new("absent_custom").unwrap();
    assert!(GetCustomScanMethods(absent.as_ptr(), true)
        .unwrap()
        .is_null());
}
