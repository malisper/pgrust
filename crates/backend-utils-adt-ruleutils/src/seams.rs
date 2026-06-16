//! Install this crate's inward seams (declared in
//! `backend-utils-adt-ruleutils-seams`).
//!
//! F0a (the deparse name-resolution engine) installs exactly the one seam it
//! can faithfully provide from the engine alone: `select_rtable_names_for_explain`,
//! the EXPLAIN frontend to `set_rtable_names`. The other declared ruleutils
//! seams (the expression deparser, the catalog def-builders, the plan-tree
//! deparse context) belong to later families (F1/F2/F3-cat / F0b) and stay
//! uninstalled (mirror-PG-and-panic) until those land.

pub fn init_seams() {
    backend_utils_adt_ruleutils_seams::select_rtable_names_for_explain::set(
        crate::select_rtable_names_for_explain,
    );
}
