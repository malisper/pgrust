#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// The project-wide error contract is the un-boxed `PgResult` carrying `PgError`.
#![allow(clippy::result_large_err)]
#![forbid(unsafe_code)]

//! Port of PostgreSQL's `extensible.c` (`src/backend/nodes/extensible.c`):
//! registration and lookup of extension-defined node types
//! (`T_ExtensibleNode`) and custom-scan providers. Loadable modules call
//! [`RegisterExtensibleNodeMethods`] / [`RegisterCustomScanMethods`] and the
//! core system looks them up via [`GetExtensibleNodeMethods`] /
//! [`GetCustomScanMethods`].
//!
//! # Representation & ownership
//!
//! The C file keeps two file-local registries — `static HTAB
//! *extensible_node_methods` and `static HTAB *custom_scan_methods` — each a
//! `HASH_ELEM | HASH_STRINGS`, `EXTNODENAME_MAX_LEN`-keyed table whose rows are
//! `ExtensibleNodeEntry { char extnodename[EXTNODENAME_MAX_LEN]; const void
//! *extnodemethods; }`. The registrant passes a process-lifetime method table
//! across the boundary and C stores the raw pointer to it.
//!
//! In this owned-tree port there are no raw pointers and no dynahash dependency:
//! the method tables ([`ExtensibleNodeMethods`] / [`CustomScanMethods`]) are
//! owned, `Clone` values, so each registry stores the owned table directly
//! under its `String` name key. The map *is* the entry table — there is no
//! separate `ExtensibleNodeEntry` struct and no pointer to dereference. A lookup
//! returns a clone of the registered table (the callbacks inside are `Copy`
//! `Option<fn(..)>`s), the faithful analogue of C returning the stored `const
//! ... *`. Because C keeps the registries in per-backend statics that every
//! code path in a single backend shares, the port parks each registry in a
//! `thread_local!` `RefCell` — one backend == one thread of control here.
//!
//! # Allocation safety
//!
//! The registry is the only data-derived growable structure. Each registration
//! grows it by exactly one entry; before pushing, the code `try_reserve`s the
//! one slot (and the owning `String` key), turning an allocation failure into a
//! recoverable [`PgError`] (`ERRCODE_OUT_OF_MEMORY`, the error C's
//! `palloc`/dynahash would raise) rather than an abort, keeping the seam's
//! `PgResult` OOM signature.

use std::cell::RefCell;

use backend_utils_error::{ereport, PgError, PgResult};
use types_error::{
    ERRCODE_DUPLICATE_OBJECT, ERRCODE_OUT_OF_MEMORY, ERRCODE_UNDEFINED_OBJECT, ERROR,
};
use types_extensible::{CustomScanMethods, ExtensibleNodeMethods, EXTNODENAME_MAX_LEN};

thread_local! {
    /// `static HTAB *extensible_node_methods` (extensible.c): the
    /// "Extensible Node Methods" registry. Empty == NULL == "not created yet".
    static EXTENSIBLE_NODE_METHODS: RefCell<Vec<(String, ExtensibleNodeMethods)>> =
        const { RefCell::new(Vec::new()) };

    /// `static HTAB *custom_scan_methods` (extensible.c): the
    /// "Custom Scan Methods" registry. Empty == NULL == "not created yet".
    static CUSTOM_SCAN_METHODS: RefCell<Vec<(String, CustomScanMethods)>> =
        const { RefCell::new(Vec::new()) };
}

/// Translate an out-of-memory allocation failure into the recoverable PG error
/// C's `palloc`/dynahash would raise (`ERRCODE_OUT_OF_MEMORY`, "out of memory"),
/// instead of aborting. Used by the registry's `try_reserve` growth path.
fn oom() -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_OUT_OF_MEMORY)
        .errmsg("out of memory")
        .into_error()
}

/// `RegisterExtensibleNodeEntry` (`extensible.c`, file-local): register a new
/// callback structure under `extnodename` in the `entries` registry, whose
/// `hash_create` label is `htable_label`. Ports the C body 1:1 — reject
/// over-long names, treat a present key as the `found` duplicate
/// (`ERRCODE_DUPLICATE_OBJECT`), otherwise insert the table.
///
/// `entries` is the resolved per-backend registry (C's `*p_htable`); lazy
/// table creation is implicit (an empty `Vec` == NULL `HTAB`), so the
/// `if (*p_htable == NULL) hash_create(...)` step has no separate body here.
/// `htable_label` is unused beyond documenting which registry this is, exactly
/// as in C once the table exists.
fn register_extensible_node_entry<M: Clone>(
    entries: &mut Vec<(String, M)>,
    _htable_label: &str,
    extnodename: &str,
    extnodemethods: &M,
) -> PgResult<()> {
    // if (strlen(extnodename) >= EXTNODENAME_MAX_LEN)
    //     elog(ERROR, "extensible node name is too long");
    if extnodename.len() >= EXTNODENAME_MAX_LEN {
        return Err(ereport(ERROR)
            .errmsg_internal("extensible node name is too long")
            .into_error());
    }

    // entry = hash_search(*p_htable, extnodename, HASH_ENTER, &found);
    // if (found) ereport(ERROR, ERRCODE_DUPLICATE_OBJECT,
    //     "extensible node type \"%s\" already exists", extnodename);
    if entries.iter().any(|(name, _)| name == extnodename) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!(
                "extensible node type \"{extnodename}\" already exists"
            ))
            .into_error());
    }

    // entry->extnodemethods = extnodemethods;
    //
    // Allocation-safety: reserve the one new slot before growing (data-derived
    // growth -> recoverable OOM, never abort). The key is bounded to
    // < EXTNODENAME_MAX_LEN bytes by the guard above.
    let mut key = String::new();
    key.try_reserve(extnodename.len()).map_err(|_| oom())?;
    key.push_str(extnodename);
    entries.try_reserve(1).map_err(|_| oom())?;
    entries.push((key, extnodemethods.clone()));
    Ok(())
}

/// `RegisterExtensibleNodeMethods` (`extensible.c`): register a new type of
/// extensible node, keyed by `methods.extnodename` in the
/// "Extensible Node Methods" registry.
pub fn RegisterExtensibleNodeMethods(methods: &ExtensibleNodeMethods) -> PgResult<()> {
    // RegisterExtensibleNodeEntry(&extensible_node_methods, "Extensible Node Methods",
    //                             methods->extnodename, methods);
    let extnodename = method_name(methods.extnodename.as_deref())?;
    EXTENSIBLE_NODE_METHODS.with(|registry| {
        register_extensible_node_entry(
            &mut registry.borrow_mut(),
            "Extensible Node Methods",
            extnodename,
            methods,
        )
    })
}

/// `RegisterCustomScanMethods` (`extensible.c`): register a new custom-scan
/// provider, keyed by `methods.CustomName` in the "Custom Scan Methods"
/// registry.
pub fn RegisterCustomScanMethods(methods: &CustomScanMethods) -> PgResult<()> {
    // RegisterExtensibleNodeEntry(&custom_scan_methods, "Custom Scan Methods",
    //                             methods->CustomName, methods);
    let custom_name = method_name(methods.CustomName.as_deref())?;
    CUSTOM_SCAN_METHODS.with(|registry| {
        register_extensible_node_entry(
            &mut registry.borrow_mut(),
            "Custom Scan Methods",
            custom_name,
            methods,
        )
    })
}

/// `GetExtensibleNodeEntry` (`extensible.c`, file-local): look an entry up by
/// `extnodename` in the `entries` registry. Ported 1:1 — find the row when the
/// table exists, then either return the stored table (cloned), return `None`
/// (`missing_ok`), or raise `ERRCODE_UNDEFINED_OBJECT`.
///
/// A `None` return is C's `return NULL;` under `missing_ok`; the cloned `Some`
/// table is C returning `entry->extnodemethods`.
fn get_extensible_node_entry<M: Clone>(
    entries: &[(String, M)],
    extnodename: &str,
    missing_ok: bool,
) -> PgResult<Option<M>> {
    // ExtensibleNodeEntry *entry = NULL;
    // if (htable != NULL)
    //     entry = hash_search(htable, extnodename, HASH_FIND, NULL);
    let entry = entries
        .iter()
        .find(|(name, _)| name == extnodename)
        .map(|(_, methods)| methods.clone());

    match entry {
        Some(methods) => Ok(Some(methods)),
        // if (!entry) { if (missing_ok) return NULL;
        //               ereport(ERROR, ERRCODE_UNDEFINED_OBJECT, ...); }
        None if missing_ok => Ok(None),
        None => Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!(
                "ExtensibleNodeMethods \"{extnodename}\" was not registered"
            ))
            .into_error()),
    }
}

/// `GetExtensibleNodeMethods` (`extensible.c`): look up the methods for an
/// extensible node type by name. Returns `None` if `missing_ok` and not found,
/// otherwise raises `ERRCODE_UNDEFINED_OBJECT`.
pub fn GetExtensibleNodeMethods(
    extnodename: &str,
    missing_ok: bool,
) -> PgResult<Option<ExtensibleNodeMethods>> {
    // return (const ExtensibleNodeMethods *)
    //     GetExtensibleNodeEntry(extensible_node_methods, extnodename, missing_ok);
    EXTENSIBLE_NODE_METHODS
        .with(|registry| get_extensible_node_entry(&registry.borrow(), extnodename, missing_ok))
}

/// `GetCustomScanMethods` (`extensible.c`): look up the methods for a
/// custom-scan provider by name. Returns `None` if `missing_ok` and not found,
/// otherwise raises `ERRCODE_UNDEFINED_OBJECT`.
pub fn GetCustomScanMethods(
    CustomName: &str,
    missing_ok: bool,
) -> PgResult<Option<CustomScanMethods>> {
    // return (const CustomScanMethods *)
    //     GetExtensibleNodeEntry(custom_scan_methods, CustomName, missing_ok);
    CUSTOM_SCAN_METHODS
        .with(|registry| get_extensible_node_entry(&registry.borrow(), CustomName, missing_ok))
}

/// Resolve a method table's identifier (`methods->extnodename` /
/// `methods->CustomName`). C dereferences the `const char *` field
/// unconditionally; in the owned tree the field is an `Option<String>`, so a
/// `None` (absent) name is a programming error and raises the same kind of
/// internal error C's NULL-deref would surface, rather than panicking.
fn method_name(name: Option<&str>) -> PgResult<&str> {
    name.ok_or_else(|| PgError::error("extensible node methods has no name"))
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
