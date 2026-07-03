//! Locale vocabulary from `utils/pg_locale.h`: `pg_locale_struct` and the
//! `pg_locale_t` open handle, plus the `pg_collation.h` provider codes.
//!
//! The owner of this vocabulary is `pg_locale.c` (`backend-utils-adt-pg-locale`).
//! The struct is populated incrementally as ports consume its fields — the
//! flag fields and `provider` are spelled out here because they are the
//! collation-independent core every provider sets. Two members the C header
//! defines are deferred to the owning unit's port:
//!
//! * `collate` — a `const struct collate_methods *` fn-pointer vtable
//!   (`strncoll`/`strnxfrm`/`strnxfrm_prefix`) keyed on `pg_locale_t`; the
//!   provider units install their own table. It belongs to `pg_locale.c`,
//!   which lands the vtable type and the comparison entry points together.
//! * `info` — a discriminated union (`builtin` / `lt` / `#ifdef USE_ICU` `icu`)
//!   whose payload types (`locale_t`, ICU `UCollator`) belong to the
//!   per-provider units. The active migration profile has ICU disabled, so the
//!   `icu` arm does not exist in this build.
//!
//! `create_pg_locale_icu` (this crate's consumer) never constructs or reads a
//! `pg_locale_struct` in the ICU-disabled profile — its only compiled branch
//! reports the unsupported-ICU error — so the trimmed shape above is all the
//! port needs to name the return type.

#![no_std]

use ::mcx::PgBox;

/// `COLLPROVIDER_*` codes (`catalog/pg_collation.h`), the single `char`
/// `pg_locale_struct.provider` holds.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum CollProvider {
    /// `COLLPROVIDER_DEFAULT` (`'d'`).
    Default = b'd',
    /// `COLLPROVIDER_BUILTIN` (`'b'`).
    Builtin = b'b',
    /// `COLLPROVIDER_ICU` (`'i'`).
    Icu = b'i',
    /// `COLLPROVIDER_LIBC` (`'c'`).
    Libc = b'c',
}

/// `struct pg_locale_struct` (`utils/pg_locale.h`), trimmed to the
/// provider-independent flag core. See the module docs for the `collate`
/// vtable and `info` union deferred to `pg_locale.c`.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct PgLocaleStruct {
    /// `char provider` — which collation provider built this locale.
    pub provider: CollProvider,
    /// `bool deterministic` — deterministic collation (no equal-but-distinct
    /// byte sequences).
    pub deterministic: bool,
    /// `bool collate_is_c` — LC_COLLATE is C/POSIX.
    pub collate_is_c: bool,
    /// `bool ctype_is_c` — LC_CTYPE is C/POSIX.
    pub ctype_is_c: bool,
    /// `bool is_default` — this is the database default collation.
    pub is_default: bool,
}

/// `pg_locale_t` = `struct pg_locale_struct *` (`utils/pg_locale.h`).
///
/// The C value is a pointer into the caller's memory context; the idiomatic
/// handle is a context-allocated box carrying the `'mcx` lifetime. `pg_locale_t`
/// is occasionally checked for truth in C, so callers use `Option<PgLocale>`
/// where C uses a possibly-NULL pointer.
pub type PgLocale<'mcx> = PgBox<'mcx, PgLocaleStruct>;
