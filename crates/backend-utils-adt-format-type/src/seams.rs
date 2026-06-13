//! Install this crate's inward seams (declared in
//! `backend-utils-adt-format-type-seams`).
//!
//! Only `set()` calls — no logic. `format_type_be` is the one function other
//! units reach across a cycle; the rest of this unit's surface is called
//! directly.

pub fn init_seams() {
    backend_utils_adt_format_type_seams::format_type_be::set(crate::format_type_be);
}
