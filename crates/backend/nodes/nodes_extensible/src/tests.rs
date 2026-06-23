//! Tests for the extensible-node / custom-scan registry port. They exercise the
//! exact C control flow of `extensible.c`: lazy table creation (an empty `Vec`
//! standing in for the NULL `HTAB`), the `EXTNODENAME_MAX_LEN` length guard,
//! duplicate detection (`ERRCODE_DUPLICATE_OBJECT`), and `missing_ok` lookups
//! vs. the `ERRCODE_UNDEFINED_OBJECT` "was not registered" error.
//!
//! This port keeps the registries in `thread_local!` state and depends on no
//! seams (the owned `Vec` replaces dynahash), so no seam installation is needed.

use ::types_error::{ERRCODE_DUPLICATE_OBJECT, ERRCODE_UNDEFINED_OBJECT};
use ::types_extensible::{CustomScanMethods, ExtensibleNodeMethods};

use super::*;

// Every test mutates the per-thread registries. cargo's harness runs tests in
// parallel across worker threads; the registries are `thread_local!` (one
// backend == one thread), so distinct test threads see distinct registries. To
// keep a thread reused by the harness from leaking rows between tests, each test
// clears the registries up front via [`RegistryGuard`].
struct RegistryGuard;

impl RegistryGuard {
    fn new() -> Self {
        clear_registries();
        RegistryGuard
    }
}

impl Drop for RegistryGuard {
    fn drop(&mut self) {
        clear_registries();
    }
}

/// Reset the two thread-local registries to empty (the freshly-started-backend
/// state, C's NULL `HTAB`) so each test sees lazy creation from scratch.
fn clear_registries() {
    EXTENSIBLE_NODE_METHODS.with(|r| r.borrow_mut().clear());
    CUSTOM_SCAN_METHODS.with(|r| r.borrow_mut().clear());
}

/// Build an [`ExtensibleNodeMethods`] table named `name`. The callbacks are all
/// `None` (this port stores the table; dispatch through the callbacks is the
/// caller's concern, exactly as `extensible.c` only ever stores/returns them).
fn make_methods(name: &str) -> ExtensibleNodeMethods {
    ExtensibleNodeMethods {
        extnodename: Some(name.to_string()),
        node_size: 0,
        nodeCopy: None,
        nodeEqual: None,
        nodeOut: None,
        nodeRead: None,
    }
}

/// As [`make_methods`] but for the custom-scan registry.
fn make_custom(name: &str) -> CustomScanMethods {
    CustomScanMethods {
        CustomName: Some(name.to_string()),
        CreateCustomScanState: None,
    }
}

#[test]
fn register_then_get_roundtrips_the_table() {
    let _g = RegistryGuard::new();
    let methods = make_methods("acme_node");
    RegisterExtensibleNodeMethods(&methods).unwrap();

    let found = GetExtensibleNodeMethods("acme_node", false)
        .unwrap()
        .expect("registered name must be found");
    assert_eq!(found.extnodename.as_deref(), Some("acme_node"));
}

#[test]
fn get_missing_ok_returns_none() {
    let _g = RegistryGuard::new();
    // Table not yet created (empty == NULL) -> missing_ok must return None.
    let found = GetExtensibleNodeMethods("never_registered", true).unwrap();
    assert!(found.is_none());

    // After a different name is registered (table now exists), a still-absent
    // name with missing_ok must also return None (HASH_FIND miss).
    RegisterExtensibleNodeMethods(&make_methods("present")).unwrap();
    let found = GetExtensibleNodeMethods("never_registered", true).unwrap();
    assert!(found.is_none());
}

#[test]
fn get_missing_not_ok_raises_undefined_object() {
    let _g = RegistryGuard::new();
    let err = GetExtensibleNodeMethods("absent", false).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_OBJECT);
    assert_eq!(
        err.message(),
        "ExtensibleNodeMethods \"absent\" was not registered"
    );
}

#[test]
fn duplicate_registration_raises_duplicate_object() {
    let _g = RegistryGuard::new();
    RegisterExtensibleNodeMethods(&make_methods("dup")).unwrap();
    let err = RegisterExtensibleNodeMethods(&make_methods("dup")).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_DUPLICATE_OBJECT);
    assert_eq!(err.message(), "extensible node type \"dup\" already exists");
}

#[test]
fn name_too_long_is_rejected() {
    let _g = RegistryGuard::new();
    // len == EXTNODENAME_MAX_LEN (64) is >= the limit -> "too long".
    let long = "x".repeat(EXTNODENAME_MAX_LEN);
    let err = RegisterExtensibleNodeMethods(&make_methods(&long)).unwrap_err();
    assert_eq!(err.message(), "extensible node name is too long");

    // len == EXTNODENAME_MAX_LEN - 1 (63) is accepted.
    let ok = "y".repeat(EXTNODENAME_MAX_LEN - 1);
    RegisterExtensibleNodeMethods(&make_methods(&ok)).unwrap();
    assert!(GetExtensibleNodeMethods(&ok, false).unwrap().is_some());
}

#[test]
fn custom_scan_registry_is_independent() {
    let _g = RegistryGuard::new();
    // Same name in both registries must not collide (two separate tables).
    RegisterExtensibleNodeMethods(&make_methods("shared_name")).unwrap();
    RegisterCustomScanMethods(&make_custom("shared_name")).unwrap();

    let got_ext = GetExtensibleNodeMethods("shared_name", false)
        .unwrap()
        .expect("ext registered");
    let got_cust = GetCustomScanMethods("shared_name", false)
        .unwrap()
        .expect("custom registered");
    assert_eq!(got_ext.extnodename.as_deref(), Some("shared_name"));
    assert_eq!(got_cust.CustomName.as_deref(), Some("shared_name"));

    // Custom-scan duplicate also raises ERRCODE_DUPLICATE_OBJECT.
    let err = RegisterCustomScanMethods(&make_custom("shared_name")).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_DUPLICATE_OBJECT);

    // Unregistered custom-scan name raises ERRCODE_UNDEFINED_OBJECT.
    let err = GetCustomScanMethods("no_such_custom", false).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_OBJECT);
}

#[test]
fn custom_scan_missing_ok_returns_none() {
    let _g = RegistryGuard::new();
    assert!(GetCustomScanMethods("absent", true).unwrap().is_none());
}
