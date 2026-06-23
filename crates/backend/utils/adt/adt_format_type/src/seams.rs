//! Install this crate's inward seams (declared in
//! `backend-utils-adt-format-type-seams`).
//!
//! Only `set()` calls — no logic. `format_type_be` is the one function other
//! units reach across a cycle; the rest of this unit's surface is called
//! directly.

pub fn init_seams() {
    format_type_seams::format_type_be::set(crate::format_type_be);
    format_type_seams::format_type_be_str::set(crate::format_type_be_str);
    format_type_seams::format_type_be_owned::set(crate::format_type_be_owned);
    format_type_seams::format_type_be_qualified::set(
        crate::format_type_be_qualified,
    );
    format_type_seams::type_maximum_size::set(crate::type_maximum_size);
    format_type_seams::format_type_extended::set(crate::format_type_extended);

    // Register this unit's fmgr builtins into the fmgr-core table (C:
    // `fmgr_builtins[]`), so by-OID dispatch resolves `format_type`.
    crate::fmgr_builtins::register_format_type_builtins();
}
