//! Seam declarations for the `backend-utils-adt-quote` unit
//! (`utils/adt/quote.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `quote_literal_cstr(rawstr)` (utils/adt/quote.c): return a properly
    /// SQL-quoted literal for `rawstr`. The C result is a `palloc`'d C string in
    /// the caller's current context; it is consumed transiently here (folded
    /// into a query string), so it crosses as an owned `String`. Infallible
    /// apart from the underlying allocation.
    pub fn quote_literal_cstr(rawstr: &str) -> String
);
