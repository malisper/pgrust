use crate::setup::monetary_and_numeric_are_c;

pub const CHAR_MAX: i8 = 127;

pub struct PgLconv {
    /// LC_NUMERIC (not LC_MONETARY): to_char's `D` and `G`. C's
    /// PGLC_localeconv copies these alongside the monetary set.
    pub decimal_point: &'static str,
    pub thousands_sep: &'static str,
    pub mon_decimal_point: &'static str,
    pub mon_thousands_sep: &'static str,
    pub mon_grouping: &'static str,
    pub currency_symbol: &'static str,
    pub positive_sign: &'static str,
    pub negative_sign: &'static str,
    pub frac_digits: i8,
    pub p_cs_precedes: i8,
    pub n_cs_precedes: i8,
    pub p_sep_by_space: i8,
    pub n_sep_by_space: i8,
    pub p_sign_posn: i8,
    pub n_sign_posn: i8,
}

static C_LOCALE_LCONV: PgLconv = PgLconv {
    decimal_point: "",
    thousands_sep: "",
    mon_decimal_point: "",
    mon_thousands_sep: "",
    mon_grouping: "",
    currency_symbol: "",
    positive_sign: "",
    negative_sign: "",
    frac_digits: CHAR_MAX,
    p_cs_precedes: CHAR_MAX,
    n_cs_precedes: CHAR_MAX,
    p_sep_by_space: CHAR_MAX,
    n_sep_by_space: CHAR_MAX,
    p_sign_posn: CHAR_MAX,
    n_sign_posn: CHAR_MAX,
};

// C caches one converted copy guarded by CurrentLocaleConvValid (the monetary/
// numeric assign hooks invalidate it). We key a process-wide cache on the
// (lc_monetary, lc_numeric) pair instead: the values are immutable for a given
// pair, so a stale entry is impossible and no invalidation hook is needed.
pub fn pglc_localeconv() -> ::types_error::PgResult<&'static PgLconv> {
    if monetary_and_numeric_are_c() {
        return Ok(&C_LOCALE_LCONV);
    }
    localeconv_non_c()
}

// wasi-libc's locale is C/POSIX-only — newlocale fails for every other name,
// so there is nothing to read and the feature error remains correct.
#[cfg(target_family = "wasm")]
fn localeconv_non_c() -> ::types_error::PgResult<&'static PgLconv> {
    Err(Box::new(
        ::types_error::PgError::error(
            "money/number formatting under a non-C lc_monetary or \
             lc_numeric locale is not supported on this platform",
        )
        .with_sqlstate(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
    ))
}

#[cfg(not(target_family = "wasm"))]
fn localeconv_non_c() -> ::types_error::PgResult<&'static PgLconv> {
    use std::collections::HashMap;
    use pgsync::{Mutex, OnceLock};

    static CACHE: OnceLock<Mutex<HashMap<(String, String), &'static PgLconv>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));

    let key = crate::setup::monetary_and_numeric_names();
    if let Some(hit) = cache.lock().expect("lconv cache").get(&key) {
        return Ok(hit);
    }
    let built: &'static PgLconv = Box::leak(Box::new(read_lconv(&key.0, &key.1)?));
    cache.lock().expect("lconv cache").insert(key, built);
    Ok(built)
}

/// PGLC_localeconv's non-C arm. C swaps LC_MONETARY/LC_NUMERIC with
/// setlocale() around localeconv(); that is process-global and this backend is
/// a THREAD, so a swap here would corrupt every concurrent session's
/// formatting. We use the per-thread locale instead (newlocale + uselocale),
/// which is what PG18's own pg_localeconv_r does for the same reason.
#[cfg(not(target_family = "wasm"))]
fn read_lconv(monetary: &str, numeric: &str) -> ::types_error::PgResult<PgLconv> {
    use core::ffi::{c_char, CStr};

    fn bad(param: &str, value: &str) -> Box<::types_error::PgError> {
        Box::new(
            ::types_error::PgError::error(format!(
                "invalid value for parameter \"{param}\": \"{value}\""
            ))
            .with_sqlstate(::types_error::ERRCODE_INVALID_PARAMETER_VALUE),
        )
    }
    fn cstring(s: &str) -> Vec<u8> {
        let mut v = Vec::with_capacity(s.len() + 1);
        v.extend_from_slice(s.as_bytes());
        v.push(0);
        v
    }
    // Copy while the locale is still installed: localeconv's pointers are only
    // valid until we restore/free it. Lossy because a locale whose encoding
    // differs from the database encoding can yield non-UTF-8 bytes (C converts
    // these with db_encoding_strdup; unported).
    unsafe fn own(p: *const c_char) -> String {
        if p.is_null() {
            String::new()
        } else {
            unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
        }
    }

    let mon_c = cstring(monetary);
    let num_c = cstring(numeric);

    // SAFETY: both buffers are NUL-terminated and outlive the newlocale calls.
    // The locale object is used only on this thread and freed before return.
    unsafe {
        let loc = libc::newlocale(
            crate::lc::LC_MONETARY_MASK,
            mon_c.as_ptr() as *const c_char,
            core::ptr::null_mut(),
        );
        if loc.is_null() {
            return Err(bad("lc_monetary", monetary));
        }
        // newlocale consumes `loc` on success; on failure it is untouched.
        let loc = match libc::newlocale(
            crate::lc::LC_NUMERIC_MASK,
            num_c.as_ptr() as *const c_char,
            loc,
        ) {
            l if l.is_null() => {
                libc::freelocale(loc);
                return Err(bad("lc_numeric", numeric));
            }
            l => l,
        };

        let prev = libc::uselocale(loc);
        let lc = libc::localeconv();
        let out = if lc.is_null() {
            None
        } else {
            let lc = &*lc;
            Some((
                own(lc.decimal_point),
                own(lc.thousands_sep),
                own(lc.mon_decimal_point),
                own(lc.mon_thousands_sep),
                own(lc.mon_grouping),
                own(lc.currency_symbol),
                own(lc.positive_sign),
                own(lc.negative_sign),
                lc.frac_digits as i8,
                lc.p_cs_precedes as i8,
                lc.n_cs_precedes as i8,
                lc.p_sep_by_space as i8,
                lc.n_sep_by_space as i8,
                lc.p_sign_posn as i8,
                lc.n_sign_posn as i8,
            ))
        };
        // Restore this thread's previous locale (possibly LC_GLOBAL_LOCALE)
        // before freeing ours — freeing the installed locale is undefined.
        libc::uselocale(prev);
        libc::freelocale(loc);

        let Some(v) = out else {
            return Err(bad("lc_monetary", monetary));
        };
        let leak = |s: String| -> &'static str {
            if s.is_empty() {
                ""
            } else {
                Box::leak(s.into_boxed_str())
            }
        };
        Ok(PgLconv {
            decimal_point: leak(v.0),
            thousands_sep: leak(v.1),
            mon_decimal_point: leak(v.2),
            mon_thousands_sep: leak(v.3),
            mon_grouping: leak(v.4),
            currency_symbol: leak(v.5),
            positive_sign: leak(v.6),
            negative_sign: leak(v.7),
            frac_digits: v.8,
            p_cs_precedes: v.9,
            n_cs_precedes: v.10,
            p_sep_by_space: v.11,
            n_sep_by_space: v.12,
            p_sign_posn: v.13,
            n_sign_posn: v.14,
        })
    }
}
