//! Install this crate's inward seams (declared in
//! `backend-utils-adt-ruleutils-seams`) and the ruleutils-owned GUC variable
//! accessors.
//!
//! F0a (the deparse name-resolution engine) installs exactly the one seam it
//! can faithfully provide from the engine alone: `select_rtable_names_for_explain`,
//! the EXPLAIN frontend to `set_rtable_names`. The other declared ruleutils
//! seams (the expression deparser, the catalog def-builders, the plan-tree
//! deparse context) belong to later families (F1/F2/F3-cat / F0b) and stay
//! uninstalled (mirror-PG-and-panic) until those land.
//!
//! `ruleutils.c` is also the defining module of the `quote_all_identifiers`
//! GUC (`bool quote_all_identifiers = false;`, registered in `guc_tables.c`
//! pointing at this file's global). The GUC machinery reaches that backend-local
//! variable through the slot accessors installed here, and `quote_identifier`
//! reads the very same store — exactly as C reads the global directly.

// The C global `bool quote_all_identifiers` is a per-backend GUC variable
// (PGC_USERSET). Mirror it with a backend-local `thread_local` `Cell`, exposed
// to the GUC machinery through the accessors installed below; this is the Rust
// home for the C file-scope global the GUC slot's `variable` pointer targets.
extern crate std;
use core::cell::Cell;
use std::thread_local;

thread_local! {
    /// `bool quote_all_identifiers = false;` (ruleutils.c).
    static QUOTE_ALL_IDENTIFIERS: Cell<bool> = const { Cell::new(false) };
}

/// Read `quote_all_identifiers` (`*conf->variable`).
#[inline]
pub fn quote_all_identifiers() -> bool {
    QUOTE_ALL_IDENTIFIERS.with(Cell::get)
}

#[inline]
fn set_quote_all_identifiers(value: bool) {
    QUOTE_ALL_IDENTIFIERS.with(|c| c.set(value));
}

pub fn init_seams() {
    use backend_utils_misc_guc_tables::{vars, GucVarAccessors};

    backend_utils_adt_ruleutils_seams::select_rtable_names_for_explain::set(
        crate::select_rtable_names_for_explain,
    );

    // Install the `quote_all_identifiers` GUC slot's variable accessors so the
    // GUC machinery can read/write the backend-local store above. Guarded so a
    // re-run (or a future second installer) does not panic on double-install.
    if !vars::quote_all_identifiers.installed() {
        vars::quote_all_identifiers.install(GucVarAccessors {
            get: quote_all_identifiers,
            set: set_quote_all_identifiers,
        });
    }
}
