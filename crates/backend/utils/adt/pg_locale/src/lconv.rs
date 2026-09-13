use crate::setup::monetary_and_numeric_are_c;

pub const CHAR_MAX: i8 = 127;

pub struct PgLconv {
    /// LC_NUMERIC (not LC_MONETARY): to_char's `D` and `G`. C's
    /// PGLC_localeconv copies these alongside the monetary set.
    pub decimal_point: &'static [u8],
    pub thousands_sep: &'static [u8],
    pub mon_decimal_point: &'static [u8],
    pub mon_thousands_sep: &'static [u8],
    pub mon_grouping: &'static str,
    pub currency_symbol: &'static [u8],
    pub positive_sign: &'static [u8],
    pub negative_sign: &'static [u8],
    pub frac_digits: i8,
    pub p_cs_precedes: i8,
    pub n_cs_precedes: i8,
    pub p_sep_by_space: i8,
    pub n_sep_by_space: i8,
    pub p_sign_posn: i8,
    pub n_sign_posn: i8,
}

static C_LOCALE_LCONV: PgLconv = PgLconv {
    decimal_point: b"",
    thousands_sep: b"",
    mon_decimal_point: b"",
    mon_thousands_sep: b"",
    mon_grouping: "",
    currency_symbol: b"",
    positive_sign: b"",
    negative_sign: b"",
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

    // The strings are converted into the database encoding (pg_locale.c:593
    // db_encoding_convert), so the key carries it: backends of databases with
    // different encodings share this process.
    static CACHE: OnceLock<Mutex<HashMap<(String, String, i32), &'static PgLconv>>> =
        OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));

    let names = crate::setup::monetary_and_numeric_names();
    let key = (names.0, names.1, mbutils::GetDatabaseEncoding());
    if let Some(hit) = cache.lock().expect("lconv cache").get(&key) {
        return Ok(hit);
    }
    let built: &'static PgLconv = Box::leak(Box::new(read_lconv(&key.0, &key.1)?));
    cache.lock().expect("lconv cache").insert(key, built);
    Ok(built)
}

// db_encoding_convert (pg_locale.c:502): convert one lconv string from the
// locale's encoding into the database encoding (pg_any_to_server also
// validates it when no conversion applies). The bytes stay as converted: a
// SQL_ASCII database keeps them verbatim, UTF-8 or not.
#[cfg(not(target_family = "wasm"))]
pub(crate) fn db_encoding_convert(
    mcx: ::mcx::Mcx<'_>,
    encoding: i32,
    raw: &[u8],
) -> ::types_error::PgResult<Vec<u8>> {
    Ok(match mbutils::pg_any_to_server(mcx, raw, encoding)? {
        Some(v) => v.as_slice().to_vec(),
        None => raw.to_vec(),
    })
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
    // Copy the raw bytes while the locale is still installed: localeconv's
    // pointers are only valid until we restore/free it. They are in the
    // locale's encoding; the conversion into the database encoding happens
    // below (pg_locale.c:593-618).
    unsafe fn own(p: *const c_char) -> Vec<u8> {
        if p.is_null() {
            Vec::new()
        } else {
            unsafe { CStr::from_ptr(p) }.to_bytes().to_vec()
        }
    }

    let mon_c = cstring(monetary);
    let num_c = cstring(numeric);

    // localeconv() returns a pointer to a process-global static struct (POSIX
    // does not require it to be thread-safe, and glibc's is not). Even with the
    // per-thread uselocale below, two backend threads that interleave here would
    // race on that shared static — one thread's copy could observe another
    // thread's locale, and freelocale() could run while another thread's read of
    // the static still referenced it (cross-session corruption + use-after-free).
    // Serialize the entire newlocale/uselocale/localeconv/copy/freelocale
    // critical section under a process-global mutex, matching how other
    // non-reentrant libc calls are wrapped in this codebase. The guard is held
    // until the function returns, i.e. until every field has been copied out of
    // the static and the locale object has been freed.
    use pgsync::{Mutex, OnceLock};
    static LOCALECONV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let _localeconv_guard = LOCALECONV_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("localeconv lock");

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
        // pg_locale.c:593-618: convert from the encoding implied by
        // LC_NUMERIC (decimal_point, thousands_sep) / LC_MONETARY (the rest)
        // into the database encoding; an unidentifiable encoding (-1) means
        // PG_SQL_ASCII, which only validates the bytes. grouping strings are
        // not text and are not converted.
        let cx = ::mcx::MemoryContext::new("PGLC_localeconv");
        let mcx = cx.mcx();
        let numeric_enc = match crate::chklocale::pg_get_encoding_from_locale(Some(numeric), true)? {
            e if e < 0 => wchar::PG_SQL_ASCII,
            e => e,
        };
        let monetary_enc =
            match crate::chklocale::pg_get_encoding_from_locale(Some(monetary), true)? {
                e if e < 0 => wchar::PG_SQL_ASCII,
                e => e,
            };
        let decimal_point = db_encoding_convert(mcx, numeric_enc, &v.0)?;
        let thousands_sep = db_encoding_convert(mcx, numeric_enc, &v.1)?;
        let mon_decimal_point = db_encoding_convert(mcx, monetary_enc, &v.2)?;
        let mon_thousands_sep = db_encoding_convert(mcx, monetary_enc, &v.3)?;
        let mon_grouping = String::from_utf8_lossy(&v.4).into_owned();
        let currency_symbol = db_encoding_convert(mcx, monetary_enc, &v.5)?;
        let positive_sign = db_encoding_convert(mcx, monetary_enc, &v.6)?;
        let negative_sign = db_encoding_convert(mcx, monetary_enc, &v.7)?;
        let leak = |s: Vec<u8>| -> &'static [u8] {
            if s.is_empty() {
                b""
            } else {
                Box::leak(s.into_boxed_slice())
            }
        };
        let mon_grouping: &'static str =
            if mon_grouping.is_empty() { "" } else { Box::leak(mon_grouping.into_boxed_str()) };
        Ok(PgLconv {
            decimal_point: leak(decimal_point),
            thousands_sep: leak(thousands_sep),
            mon_decimal_point: leak(mon_decimal_point),
            mon_thousands_sep: leak(mon_thousands_sep),
            mon_grouping,
            currency_symbol: leak(currency_symbol),
            positive_sign: leak(positive_sign),
            negative_sign: leak(negative_sign),
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
