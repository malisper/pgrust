//! Seam declarations for the `backend-tsearch-ts-locale` unit
//! (`tsearch/ts_locale.c`): the locale-aware tsearch character predicates.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! In C `t_isalpha_cstr(c)` takes a `const char *` and inspects only the
//! leading multibyte character (calling `pg_mblen_cstr` to find its length).
//! The idiomatic surface hands the seam a `&[u8]` view positioned at the
//! character under test; the implementation reads exactly the leading
//! character. Infallible (a pure ctype/wctype classification).

seam_core::seam!(
    /// `t_isalpha_cstr(c)` (`ts_locale.c`) — true iff the leading character of
    /// `s` is alphabetic under the database ctype/locale. `s` is positioned at
    /// the character under test and is never empty at a call site.
    pub fn t_isalpha(s: &[u8]) -> bool
);
