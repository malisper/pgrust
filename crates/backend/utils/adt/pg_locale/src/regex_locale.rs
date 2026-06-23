//! The `pg_wc_is*` / `pg_wc_toupper` / `pg_wc_tolower` probe family
//! (`regc_pg_locale.c`), the non-C-strategy legs the regex engine delegates to
//! the locale owner.
//!
//! `regc_pg_locale.c` selects a strategy in `pg_set_regex_collation`
//! (C / BUILTIN / LIBC_WIDE / LIBC_1BYTE / ICU) and keeps the C-strategy
//! hard-wired in the engine. The BUILTIN / LIBC / ICU legs reach into the
//! locale's provider-specific `info` union, which `pg_locale.c`'s permanent
//! cache owns — so they cross to this owner keyed on the collation OID. The
//! engine also handles the LIBC `is_default` ASCII-forcing for toupper/tolower
//! before the seam, so this code is the raw provider `info.lt` reach.

use ::types_core::primitive::PgWChar;
use ::locale::CollProvider;

use pg_locale_builtin_seams as builtin;
use ::pg_locale_seams::RegexWcClass;
use mbutils_seams as mb;

use crate::cache::{resolve, LocaleInfo};

/// Whether the LIBC strategy is `LIBC_WIDE` (`<wctype.h>`) vs `LIBC_1BYTE`
/// (`<ctype.h>`) — C picks WIDE when the database encoding's max length > 1
/// (`pg_set_regex_collation`: `database_ctype_is_c ? ... : (max len > 1 ? WIDE :
/// 1BYTE)`).
fn libc_is_wide() -> bool {
    mb::pg_database_encoding_max_length::call() > 1
}

/// `pg_wc_is<class>(c)` for the active non-C-strategy `collation`
/// (`regc_pg_locale.c`).
pub fn regex_wc_isclass(collation: ::types_core::primitive::Oid, class: RegexWcClass, c: PgWChar) -> bool {
    let entry = match resolve(collation) {
        Ok(e) => e,
        // C cannot reach here with an unresolvable collation (the engine resolved
        // it in pg_set_regex_collation); be conservative.
        Err(_) => return false,
    };
    match (&entry.info, entry.view.provider) {
        (LocaleInfo::Libc(l), _) => {
            crate::libc_provider::regex_wc_isclass_libc(l, class, c, libc_is_wide())
        }
        (LocaleInfo::Builtin { casemap_full }, _) => {
            builtin::regex_wc_isclass_builtin::call(class, c, !casemap_full)
        }
        // Provider says builtin but info isn't (cannot happen for a resolved
        // builtin entry); C/POSIX builtin locales use posix = !casemap_full = true.
        (_, CollProvider::Builtin) => builtin::regex_wc_isclass_builtin::call(class, c, true),
        // ICU is disabled; the C-locale strategy never crosses this seam.
        _ => false,
    }
}

/// `pg_wc_toupper(c)` for the active non-C-strategy `collation`.
pub fn regex_wc_toupper(collation: ::types_core::primitive::Oid, c: PgWChar) -> PgWChar {
    let entry = match resolve(collation) {
        Ok(e) => e,
        Err(_) => return c,
    };
    match (&entry.info, entry.view.provider) {
        (LocaleInfo::Libc(l), _) => {
            crate::libc_provider::regex_wc_toupper_libc(l, c, libc_is_wide())
        }
        (LocaleInfo::Builtin { .. }, _) | (_, CollProvider::Builtin) => {
            builtin::regex_wc_toupper_builtin::call(c)
        }
        _ => c,
    }
}

/// `pg_wc_tolower(c)` for the active non-C-strategy `collation`.
pub fn regex_wc_tolower(collation: ::types_core::primitive::Oid, c: PgWChar) -> PgWChar {
    let entry = match resolve(collation) {
        Ok(e) => e,
        Err(_) => return c,
    };
    match (&entry.info, entry.view.provider) {
        (LocaleInfo::Libc(l), _) => {
            crate::libc_provider::regex_wc_tolower_libc(l, c, libc_is_wide())
        }
        (LocaleInfo::Builtin { .. }, _) | (_, CollProvider::Builtin) => {
            builtin::regex_wc_tolower_builtin::call(c)
        }
        _ => c,
    }
}
