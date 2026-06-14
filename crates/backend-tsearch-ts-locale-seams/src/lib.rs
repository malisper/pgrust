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

extern crate alloc;

seam_core::seam!(
    /// `t_isalpha_cstr(c)` (`ts_locale.c`) — true iff the leading character of
    /// `s` is alphabetic under the database ctype/locale. `s` is positioned at
    /// the character under test and is never empty at a call site.
    pub fn t_isalpha(s: &[u8]) -> bool
);

seam_core::seam!(
    /// `t_isalnum_cstr(c)` (`ts_locale.c`) — true iff the leading character of
    /// `s` is alphanumeric under the database ctype/locale. `s` is positioned
    /// at the character under test and is never empty at a call site.
    pub fn t_isalnum(s: &[u8]) -> bool
);

seam_core::seam!(
    /// `tsearch_readline_begin` + `tsearch_readline` loop (`ts_locale.c`) — open
    /// the tsearch config file at `filename` (a NUL-free path in the database
    /// encoding) and return its whole content, already recoded to the database
    /// encoding (the C reader recodes each line as it returns it). The lines are
    /// `\n`-terminated; the caller splits and parses them.
    ///
    /// `Err(msg)` carries the C `%m` text for the
    /// `could not open dictionary/affix file "%s": %m` `ereport`; the caller
    /// composes the full message. The whole-file return is allocated by the
    /// seam owner, not charged to a caller context, since the bytes are scratch
    /// the caller copies the parsed pieces out of.
    pub fn readfile(
        filename: &[u8],
    ) -> core::result::Result<alloc::vec::Vec<u8>, alloc::string::String>
);
