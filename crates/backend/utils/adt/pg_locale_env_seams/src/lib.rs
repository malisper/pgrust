//! Encoding/message-domain seam declarations for `backend-utils-adt-pg-locale`
//! (`utils/adt/pg_locale.c`).
//!
//! `pg_perm_setlocale`, `PGLC_localeconv`, and `cache_locale_time` reach a few
//! PostgreSQL-specific (not OS-FFI) encoding helpers whose owners are not yet
//! ported:
//!
//! * `SetMessageEncoding` (mbutils.c) — set the encoding used for translated
//!   messages, called from `pg_perm_setlocale` on `LC_CTYPE`;
//! * `pg_get_encoding_from_locale` (chklocale.c) — map a locale name to its
//!   implied encoding (or -1), used by `PGLC_localeconv`/`cache_locale_time` to
//!   pick the source encoding for conversion;
//! * `pg_any_to_server` (mbutils.c) — convert bytes from `encoding` to the
//!   database encoding (or validate when already in it), used by both
//!   `db_encoding_convert` and `cache_single_string`.
//!
//! Each crosses here and panics until its owner lands. (The pure libc primitives
//! `setlocale`/`setenv`/`localeconv`/`newlocale` are bound directly in the owner
//! crate as OS FFI, not seamed.)

use mcx::{Mcx, PgVec};
use ::types_error::PgResult;

seam_core::seam!(
    /// `SetMessageEncoding(encoding)` (mbutils.c): set the encoding used for
    /// translated-message text. Called from `pg_perm_setlocale` after a
    /// successful `LC_CTYPE` change.
    pub fn set_message_encoding(encoding: i32)
);

seam_core::seam!(
    /// `pg_get_encoding_from_locale(locale, write_message)` (chklocale.c): the
    /// encoding implied by `locale`, or `-1` if it can't be determined. The C
    /// `write_message` flag is always `true` here.
    pub fn pg_get_encoding_from_locale(locale: &str) -> PgResult<i32>
);

seam_core::seam!(
    /// `pg_any_to_server(s, len, encoding)` (mbutils.c): convert `src` from
    /// `encoding` to the database encoding, or validate it in place when already
    /// in the database encoding. Returns the converted bytes copied into `mcx`.
    pub fn pg_any_to_server<'mcx>(
        mcx: Mcx<'mcx>,
        src: &[u8],
        encoding: i32,
    ) -> PgResult<PgVec<'mcx, u8>>
);
