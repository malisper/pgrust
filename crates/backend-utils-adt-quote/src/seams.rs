//! Install this crate's inward seams (declared in
//! `backend-utils-adt-quote-seams`).
//!
//! Only `set()` calls — no logic. `quote_literal_cstr` is the one function
//! other units reach across a cycle (e.g. `varlena`'s `format()` `%L`,
//! `slotsync`'s remote-slot query); the rest of this unit's surface is called
//! directly.

pub fn init_seams() {
    backend_utils_adt_quote_seams::quote_literal_cstr::set(crate::quote_literal_cstr);
    // Register the SQL-callable quote.c builtins into the fmgr-core table
    // (C: fmgr_builtins[]) so by-OID dispatch resolves them.
    crate::fmgr_builtins::register_quote_builtins();
}
