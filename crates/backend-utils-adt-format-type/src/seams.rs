//! Install this crate's inward seams (declared in
//! `backend-utils-adt-format-type-seams`).
//!
//! Only `set()` calls — no logic. `format_type_be` is the one function other
//! units reach across a cycle; the rest of this unit's surface is called
//! directly.

pub fn init_seams() {
    backend_utils_adt_format_type_seams::format_type_be::set(crate::format_type_be);
    backend_utils_adt_format_type_seams::format_type_be_str::set(crate::format_type_be_str);
    backend_utils_adt_format_type_seams::format_type_be_owned::set(crate::format_type_be_owned);
    backend_utils_adt_format_type_seams::format_type_be_qualified::set(
        crate::format_type_be_qualified,
    );
    backend_utils_adt_format_type_seams::type_maximum_size::set(crate::type_maximum_size);
    backend_utils_adt_format_type_seams::format_type_extended::set(crate::format_type_extended);
}
