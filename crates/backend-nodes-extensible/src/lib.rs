#![allow(non_snake_case)]
// The project-wide error contract is the un-boxed `PgResult` carrying `PgError`.
#![allow(clippy::result_large_err)]

//! Port of PostgreSQL's `extensible.c` (`src/backend/nodes/extensible.c`):
//! registration and lookup of extension-defined node types
//! (`T_ExtensibleNode`) and custom-scan providers. Loadable modules call
//! [`RegisterExtensibleNodeMethods`] / [`RegisterCustomScanMethods`] and the
//! core system looks them up via [`GetExtensibleNodeMethods`] /
//! [`GetCustomScanMethods`].
//!
//! Like C, the two registries are per-backend `HTAB`s
//! (`HASH_ELEM | HASH_STRINGS`, `EXTNODENAME_MAX_LEN`-keyed) whose rows are
//! `ExtensibleNodeEntry { char extnodename[EXTNODENAME_MAX_LEN]; const void
//! *extnodemethods; }`. The registrant passes a process-lifetime method table
//! across the boundary and the table stores the raw pointer to it (`const void
//! *`), exactly as C does — the registry never dereferences the method-table
//! fields. The per-backend `static HTAB *` statics become `thread_local`
//! `*mut HTAB` (one backend == one thread), NULL until first registration.
//!
//! The register/get functions take/return `const char *` and method-table
//! pointers (C's raw-pointer ABI); the pointer derefs are confined to `unsafe`
//! blocks, so the functions coerce to plain `fn` seam slots.

use core::ffi::c_void;
use core::ptr;
use std::cell::Cell;

use backend_utils_error::{ereport, PgResult};
use backend_utils_hash_dynahash::{hash_create, hash_search};
use types_error::{ERRCODE_DUPLICATE_OBJECT, ERRCODE_UNDEFINED_OBJECT, ERROR};
use types_extensible::{
    CustomScanMethods, ExtensibleNodeEntry, ExtensibleNodeMethods, EXTNODENAME_MAX_LEN,
};
use types_hash::hsearch::{
    HASHACTION::{HASH_ENTER, HASH_FIND},
    HASHCTL, HASH_ELEM, HASH_STRINGS, HTAB,
};

thread_local! {
    /// `static HTAB *extensible_node_methods` (extensible.c). NULL == not yet
    /// created.
    static EXTENSIBLE_NODE_METHODS: Cell<*mut HTAB> = const { Cell::new(ptr::null_mut()) };

    /// `static HTAB *custom_scan_methods` (extensible.c). NULL == not yet
    /// created.
    static CUSTOM_SCAN_METHODS: Cell<*mut HTAB> = const { Cell::new(ptr::null_mut()) };
}

/// `strlen` of a NUL-terminated C string (`const char *`).
fn c_strlen(s: *const core::ffi::c_char) -> usize {
    let mut n = 0;
    while unsafe { *s.add(n) } != 0 {
        n += 1;
    }
    n
}

/// Copy a NUL-terminated C key string into a fixed `EXTNODENAME_MAX_LEN` buffer
/// for use as the dynahash key (string keys match on the leading bytes up to
/// the first NUL, within `keysize`).
fn key_buffer(extnodename: *const core::ffi::c_char) -> [u8; EXTNODENAME_MAX_LEN] {
    let mut buf = [0u8; EXTNODENAME_MAX_LEN];
    let mut i = 0;
    while i < EXTNODENAME_MAX_LEN {
        let c = unsafe { *extnodename.add(i) };
        if c == 0 {
            break;
        }
        buf[i] = c as u8;
        i += 1;
    }
    buf
}

/// `extnodename` rendered to a `String` for an error message.
fn name_string(extnodename: *const core::ffi::c_char) -> String {
    let len = c_strlen(extnodename);
    String::from_utf8_lossy(&key_buffer(extnodename)[..len]).into_owned()
}

/// `RegisterExtensibleNodeEntry` (`extensible.c`, file-local): register a new
/// callback structure under `extnodename` in the `*p_htable` registry, whose
/// `hash_create` label is `htable_label`. `extnodemethods` is stored opaquely
/// (`const void *`).
fn register_extensible_node_entry(
    p_htable: &Cell<*mut HTAB>,
    htable_label: &str,
    extnodename: *const core::ffi::c_char,
    extnodemethods: *const c_void,
) -> PgResult<()> {
    // if (*p_htable == NULL) { HASHCTL ctl; ctl.keysize = EXTNODENAME_MAX_LEN;
    //   ctl.entrysize = sizeof(ExtensibleNodeEntry);
    //   *p_htable = hash_create(htable_label, 100, &ctl,
    //                           HASH_ELEM | HASH_STRINGS); }
    if p_htable.get().is_null() {
        let ctl = HASHCTL {
            keysize: EXTNODENAME_MAX_LEN,
            entrysize: core::mem::size_of::<ExtensibleNodeEntry>(),
            ..HASHCTL::new()
        };
        let htab = hash_create(htable_label, 100, &ctl, HASH_ELEM | HASH_STRINGS)?;
        p_htable.set(htab);
    }

    // if (strlen(extnodename) >= EXTNODENAME_MAX_LEN)
    //     elog(ERROR, "extensible node name is too long");
    if c_strlen(extnodename) >= EXTNODENAME_MAX_LEN {
        return Err(ereport(ERROR)
            .errmsg_internal("extensible node name is too long")
            .into_error());
    }

    // entry = (ExtensibleNodeEntry *) hash_search(*p_htable, extnodename,
    //                                             HASH_ENTER, &found);
    let key = key_buffer(extnodename);
    let (entry, found) = hash_search(p_htable.get(), key.as_ptr(), HASH_ENTER)?;
    let entry = entry as *mut ExtensibleNodeEntry;

    // if (found) ereport(ERROR, errcode(ERRCODE_DUPLICATE_OBJECT),
    //     errmsg("extensible node type \"%s\" already exists", extnodename));
    if found {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!(
                "extensible node type \"{}\" already exists",
                name_string(extnodename)
            ))
            .into_error());
    }

    // entry->extnodemethods = extnodemethods;
    unsafe { (*entry).extnodemethods = extnodemethods };
    Ok(())
}

/// `RegisterExtensibleNodeMethods` (`extensible.c`): register a new type of
/// extensible node. `methods` is a process-lifetime `ExtensibleNodeMethods`
/// whose `extnodename` is the registry key.
pub fn RegisterExtensibleNodeMethods(methods: *const ExtensibleNodeMethods) -> PgResult<()> {
    // RegisterExtensibleNodeEntry(&extensible_node_methods, "Extensible Node Methods",
    //                             methods->extnodename, methods);
    let extnodename = unsafe { (*methods).extnodename };
    EXTENSIBLE_NODE_METHODS.with(|registry| {
        register_extensible_node_entry(
            registry,
            "Extensible Node Methods",
            extnodename,
            methods as *const c_void,
        )
    })
}

/// `RegisterCustomScanMethods` (`extensible.c`): register a new custom-scan
/// provider. `methods` is a process-lifetime `CustomScanMethods` whose
/// `CustomName` is the registry key.
pub fn RegisterCustomScanMethods(methods: *const CustomScanMethods) -> PgResult<()> {
    // RegisterExtensibleNodeEntry(&custom_scan_methods, "Custom Scan Methods",
    //                             methods->CustomName, methods);
    let custom_name = unsafe { (*methods).CustomName };
    CUSTOM_SCAN_METHODS.with(|registry| {
        register_extensible_node_entry(
            registry,
            "Custom Scan Methods",
            custom_name,
            methods as *const c_void,
        )
    })
}

/// `GetExtensibleNodeEntry` (`extensible.c`, file-local): look an entry up by
/// `extnodename` in the `htable` registry. Returns the stored `const void *`
/// (NULL if `missing_ok` and not found, else `ERRCODE_UNDEFINED_OBJECT`).
fn get_extensible_node_entry(
    htable: *mut HTAB,
    extnodename: *const core::ffi::c_char,
    missing_ok: bool,
) -> PgResult<*const c_void> {
    // ExtensibleNodeEntry *entry = NULL;
    // if (htable != NULL)
    //     entry = hash_search(htable, extnodename, HASH_FIND, NULL);
    let mut entry: *mut ExtensibleNodeEntry = ptr::null_mut();
    if !htable.is_null() {
        let key = key_buffer(extnodename);
        let (p, found) = hash_search(htable, key.as_ptr(), HASH_FIND)?;
        if found {
            entry = p as *mut ExtensibleNodeEntry;
        }
    }

    // if (!entry) { if (missing_ok) return NULL;
    //   ereport(ERROR, errcode(ERRCODE_UNDEFINED_OBJECT),
    //     errmsg("ExtensibleNodeMethods \"%s\" was not registered", extnodename)); }
    if entry.is_null() {
        if missing_ok {
            return Ok(ptr::null());
        }
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!(
                "ExtensibleNodeMethods \"{}\" was not registered",
                name_string(extnodename)
            ))
            .into_error());
    }

    // return entry->extnodemethods;
    Ok(unsafe { (*entry).extnodemethods })
}

/// `GetExtensibleNodeMethods` (`extensible.c`): look up the methods for an
/// extensible node type by name. NULL if `missing_ok` and not found, otherwise
/// raises `ERRCODE_UNDEFINED_OBJECT`.
pub fn GetExtensibleNodeMethods(
    extnodename: *const core::ffi::c_char,
    missing_ok: bool,
) -> PgResult<*const ExtensibleNodeMethods> {
    // return (const ExtensibleNodeMethods *)
    //     GetExtensibleNodeEntry(extensible_node_methods, extnodename, missing_ok);
    EXTENSIBLE_NODE_METHODS.with(|registry| {
        get_extensible_node_entry(registry.get(), extnodename, missing_ok)
            .map(|p| p as *const ExtensibleNodeMethods)
    })
}

/// `GetCustomScanMethods` (`extensible.c`): look up the methods for a
/// custom-scan provider by name. NULL if `missing_ok` and not found, otherwise
/// raises `ERRCODE_UNDEFINED_OBJECT`.
pub fn GetCustomScanMethods(
    CustomName: *const core::ffi::c_char,
    missing_ok: bool,
) -> PgResult<*const CustomScanMethods> {
    // return (const CustomScanMethods *)
    //     GetExtensibleNodeEntry(custom_scan_methods, CustomName, missing_ok);
    CUSTOM_SCAN_METHODS.with(|registry| {
        get_extensible_node_entry(registry.get(), CustomName, missing_ok)
            .map(|p| p as *const CustomScanMethods)
    })
}

/// Install this crate's seams (`crates/backend-nodes-extensible-seams`).
pub fn init_seams() {
    backend_nodes_extensible_seams::RegisterExtensibleNodeMethods::set(RegisterExtensibleNodeMethods);
    backend_nodes_extensible_seams::RegisterCustomScanMethods::set(RegisterCustomScanMethods);
    backend_nodes_extensible_seams::GetExtensibleNodeMethods::set(GetExtensibleNodeMethods);
    backend_nodes_extensible_seams::GetCustomScanMethods::set(GetCustomScanMethods);
}

#[cfg(test)]
mod tests;
