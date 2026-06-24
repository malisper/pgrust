//! The `pgcrypto.builtin_crypto_enabled` custom GUC (pgcrypto.c) and the
//! `CheckBuiltinCryptoMode()` gate (openssl.c) that `px_crypt` / `px_gen_salt`
//! call before doing built-in crypto.

use core::cell::Cell;

/// `BC_OFF` / `BC_ON` / `BC_FIPS` (pgcrypto.c). Discriminants match the C
/// `config_enum_entry` order.
pub const BC_OFF: i32 = 0;
pub const BC_ON: i32 = 1;
pub const BC_FIPS: i32 = 2;

thread_local! {
    /// `int builtin_crypto_enabled = BC_ON;` (pgcrypto.c).
    static BUILTIN_CRYPTO_ENABLED: Cell<i32> = const { Cell::new(BC_ON) };
}

fn get_mode() -> i32 {
    BUILTIN_CRYPTO_ENABLED.with(Cell::get)
}

fn set_mode(v: i32) {
    BUILTIN_CRYPTO_ENABLED.with(|c| c.set(v));
}

/// `builtin_crypto_options[]` (pgcrypto.c).
static BUILTIN_CRYPTO_OPTIONS: &[::types_guc::config_enum_entry] = &[
    ::types_guc::config_enum_entry {
        name: "off",
        val: BC_OFF,
        hidden: false,
    },
    ::types_guc::config_enum_entry {
        name: "on",
        val: BC_ON,
        hidden: false,
    },
    ::types_guc::config_enum_entry {
        name: "fips",
        val: BC_FIPS,
        hidden: false,
    },
];

/// `_PG_init`'s `DefineCustomEnumVariable("pgcrypto.builtin_crypto_enabled", …)`.
pub fn register() {
    use ::guc_tables::GucVarAccessors;
    use ::types_guc::PGC_SUSET;

    let _ = ::misc_guc::custom::define_custom_enum_variable(
        "pgcrypto.builtin_crypto_enabled",
        Some("Sets if builtin crypto functions are enabled."),
        None,
        GucVarAccessors {
            get: get_mode,
            set: set_mode,
        },
        BC_ON,
        BUILTIN_CRYPTO_OPTIONS,
        PGC_SUSET,
        0,
        None,
        None,
        None,
    );
}

/// `CheckBuiltinCryptoMode()` (openssl.c) — error out if built-in crypto is
/// disabled. `Ok(())` when permitted; `Err(message)` with the exact pgcrypto
/// error text otherwise. (The FIPS branch never errors here since this build
/// has no OpenSSL FIPS provider — `CheckFIPSMode()` is always false.)
pub fn check_builtin_crypto_mode() -> Result<(), String> {
    match get_mode() {
        BC_ON => Ok(()),
        BC_OFF => Err("use of built-in crypto functions is disabled".to_string()),
        // BC_FIPS: CheckFIPSMode() is false in this build, so it proceeds.
        _ => Ok(()),
    }
}
