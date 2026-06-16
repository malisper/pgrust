//! The libc collation provider (`pg_locale_libc.c`), the half `pg_locale.c`'s
//! dispatch reaches for `COLLPROVIDER_LIBC` collations.
//!
//! `create_pg_locale_libc` reads the collation/database `(collate, ctype)` pair,
//! builds an OS `locale_t` via `make_libc_collator`, and assembles the flag core
//! + a [`LibcLocale`] holding the owned handle. The comparison primitives
//! `strncoll_libc`/`strnxfrm_libc` NUL-terminate their inputs and call libc
//! `strcoll_l`/`strxfrm_l`. `get_collation_actual_version_libc` reports the glibc
//! version for a non-C/POSIX collation.
//!
//! The libc locale protocol (`newlocale`/`freelocale`/`strcoll_l`/`strxfrm_l`/
//! `tolower_l`) is OS FFI, bound here directly (like other OS-syscall sites in
//! the tree). The WIN32 `_create_locale`/`wcscoll_l` and the BSD
//! `querylocale(LC_VERSION_MASK)` branches are not part of the active (glibc)
//! profile and are not compiled.

extern crate alloc;

use alloc::format;
use core::ffi::c_char;

use mcx::{Mcx, PgVec};
use types_core::primitive::Oid;
use types_error::{ErrorLocation, PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use types_locale::{CollProvider, PgLocaleStruct};
use types_tuple::heaptuple::DEFAULT_COLLATION_OID;

use backend_utils_adt_pg_locale_catalog_seams as catalog;

use crate::cache::{LocaleEntry, LocaleInfo};

/// `TEXTBUFLEN` (`pg_locale_libc.c:42`): stack buffer for NUL-terminating
/// short comparison inputs without a heap allocation.
const TEXTBUFLEN: usize = 1024;

// The `*_l` locale-aware C library functions. POSIX defines these; the `libc`
// crate does not surface them uniformly across platforms, so they are declared
// here directly (OS FFI, like the other locale primitives).
extern "C" {
    fn strcoll_l(s1: *const c_char, s2: *const c_char, loc: libc::locale_t) -> libc::c_int;
    fn strxfrm_l(
        dst: *mut c_char,
        src: *const c_char,
        n: libc::size_t,
        loc: libc::locale_t,
    ) -> libc::size_t;
    fn tolower_l(c: libc::c_int, loc: libc::locale_t) -> libc::c_int;
}

/// An owned libc `locale_t` (the C `info.lt`). NULL/`0` means the C/POSIX
/// locale, which libc does not actually represent with a handle. The handle is
/// freed on drop (`freelocale`), so no path leaks a `locale_t`.
pub struct LibcLocale {
    /// The libc `locale_t` (`info.lt`), or null for C/POSIX.
    lt: libc::locale_t,
    /// Whether the collation is NOT C/POSIX — i.e. whether the C `collate`
    /// method vtable would be non-NULL (`result->collate = &collate_methods_libc`).
    has_collate: bool,
}

// SAFETY: a `locale_t` is touched only by the owning backend thread; the cache
// holds it for the backend lifetime and never shares it across threads.
unsafe impl Send for LibcLocale {}
unsafe impl Sync for LibcLocale {}

impl Drop for LibcLocale {
    fn drop(&mut self) {
        if !self.lt.is_null() {
            // SAFETY: `lt` was produced by `newlocale` and is owned here.
            unsafe { libc::freelocale(self.lt) };
        }
    }
}

impl LibcLocale {
    /// Whether this libc locale has the `collate` method vtable (C:
    /// `result->collate != NULL`, set iff `!collate_is_c`).
    pub fn has_collate_methods(&self) -> bool {
        self.has_collate
    }

    /// The owned `locale_t` (`info.lt`).
    fn handle(&self) -> libc::locale_t {
        self.lt
    }
}

/// `create_pg_locale_libc(collid, context)` (`pg_locale_libc.c:421`): read the
/// `(collate, ctype)` strings (from `pg_database` for the default, else
/// `pg_collation`), build the OS collator, and assemble the locale entry.
pub fn create_pg_locale_libc(collid: Oid) -> PgResult<LocaleEntry> {
    let (collate, ctype) = if collid == DEFAULT_COLLATION_OID {
        // C: SearchSysCache1(DATABASEOID, MyDatabaseId);
        //    datcollate = SysCacheGetAttrNotNull(datcollate);
        //    datctype   = SysCacheGetAttrNotNull(datctype);
        let row = catalog::database_locale_row::call()?.ok_or_else(|| {
            PgError::error(format!(
                "cache lookup failed for database {}",
                catalog::my_database_id::call()
            ))
        })?;
        (row.collate, row.ctype)
    } else {
        // C: SearchSysCache1(COLLOID, collid);
        //    collate = SysCacheGetAttrNotNull(collcollate);
        //    ctype   = SysCacheGetAttrNotNull(collctype);
        let row = catalog::collation_locale_row::call(collid)?.ok_or_else(|| {
            PgError::error(format!("cache lookup failed for collation {collid}"))
        })?;
        // collcollate/collctype are NOT NULL for libc collations
        // (SysCacheGetAttrNotNull); a NULL here is a catalog inconsistency.
        let collate = row.collate.ok_or_else(|| {
            PgError::error(format!("null collcollate for libc collation {collid}"))
        })?;
        let ctype = row.ctype.ok_or_else(|| {
            PgError::error(format!("null collctype for libc collation {collid}"))
        })?;
        (collate, ctype)
    };

    let (lt, has_collate) = make_libc_collator(&collate, &ctype)?;

    // C: result->collate_is_c / ctype_is_c.
    let collate_is_c = collate == "C" || collate == "POSIX";
    let ctype_is_c = ctype == "C" || ctype == "POSIX";

    let view = PgLocaleStruct {
        provider: CollProvider::Libc,
        // C: result->deterministic = true (only ICU honors collisdeterministic).
        deterministic: true,
        collate_is_c,
        ctype_is_c,
        is_default: false,
    };

    Ok(LocaleEntry {
        view,
        info: LocaleInfo::Libc(LibcLocale { lt, has_collate }),
    })
}

/// `make_libc_collator(collate, ctype)` (`pg_locale_libc.c:497`): create a
/// `locale_t` for the given collation+ctype. C/POSIX returns null (libc does not
/// represent it). Returns `(locale_t, has_collate)` where `has_collate` is
/// `!collate_is_c`.
fn make_libc_collator(collate: &str, ctype: &str) -> PgResult<(libc::locale_t, bool)> {
    let collate_is_c = collate == "C" || collate == "POSIX";
    let ctype_is_c = ctype == "C" || ctype == "POSIX";

    let lt: libc::locale_t = if collate == ctype {
        if !ctype_is_c {
            // C: normal case where they're the same.
            new_locale(libc::LC_COLLATE_MASK | libc::LC_CTYPE_MASK, collate)?
        } else {
            core::ptr::null_mut()
        }
    } else {
        // C: two newlocale() steps.
        let loc1 = if !collate_is_c {
            Some(new_locale(libc::LC_COLLATE_MASK, collate)?)
        } else {
            None
        };

        if !ctype_is_c {
            // newlocale(LC_CTYPE_MASK, ctype, loc1)
            let base = loc1.unwrap_or(core::ptr::null_mut());
            set_errno(0);
            // SAFETY: ctype is a NUL-free Rust string; base is owned (or null).
            let cname = cstr(ctype);
            let loc = unsafe {
                libc::newlocale(libc::LC_CTYPE_MASK, cname.as_ptr() as *const c_char, base)
            };
            if loc.is_null() {
                if let Some(l1) = loc1 {
                    if !l1.is_null() {
                        // SAFETY: l1 is owned here.
                        unsafe { libc::freelocale(l1) };
                    }
                }
                return Err(report_newlocale_failure(ctype));
            }
            loc
        } else {
            loc1.unwrap_or(core::ptr::null_mut())
        }
    };

    Ok((lt, !collate_is_c))
}

/// `newlocale(mask, name, NULL)` with C's `errno = 0` + failure reporting.
fn new_locale(mask: libc::c_int, name: &str) -> PgResult<libc::locale_t> {
    set_errno(0);
    let cname = cstr(name);
    // SAFETY: cname is NUL-terminated; a null base is the documented "fresh"
    // request.
    let loc = unsafe {
        libc::newlocale(mask, cname.as_ptr() as *const c_char, core::ptr::null_mut())
    };
    if loc.is_null() {
        return Err(report_newlocale_failure(name));
    }
    Ok(loc)
}

/// `strncoll_libc(arg1, len1, arg2, len2, locale)` (`pg_locale_libc.c:567`):
/// NUL-terminate the inputs if necessary, then `strcoll_l`. The repo seam passes
/// the payload byte slices (no embedded NUL); both lengths are concrete (the C
/// `-1` "already terminated" case is the NUL-terminated input, handled by the
/// `arg1n`/`arg2n` legs).
pub fn strncoll_libc(arg1: &[u8], arg2: &[u8], locale: &LibcLocale) -> i32 {
    // C: bufsize = len + 1 for each concrete-length leg.
    let bufsize1 = arg1.len() + 1;
    let bufsize2 = arg2.len() + 1;

    let mut sbuf = [0u8; TEXTBUFLEN];
    let mut heap;
    let buf: &mut [u8] = if bufsize1 + bufsize2 > TEXTBUFLEN {
        heap = alloc::vec![0u8; bufsize1 + bufsize2];
        &mut heap
    } else {
        &mut sbuf
    };

    // buf1 = buf[..bufsize1], buf2 = buf[bufsize1..].
    buf[..arg1.len()].copy_from_slice(arg1);
    buf[arg1.len()] = 0;
    buf[bufsize1..bufsize1 + arg2.len()].copy_from_slice(arg2);
    buf[bufsize1 + arg2.len()] = 0;

    let p1 = buf.as_ptr() as *const c_char;
    // SAFETY: both legs are NUL-terminated; `locale->info.lt` is the owned
    // handle (or null for the C locale, which strcoll_l accepts).
    unsafe {
        let p2 = buf.as_ptr().add(bufsize1) as *const c_char;
        strcoll_l(p1, p2, locale.handle())
    }
}

/// `strnxfrm_libc(dest, destsize, src, srclen, locale)`
/// (`pg_locale_libc.c:627`): NUL-terminate `src`, then `strxfrm_l` into a
/// `destsize`-byte buffer. The repo seam returns the full transformed blob (no
/// trailing NUL) charged to `mcx`: run `strxfrm_l` once to size, then once to
/// fill, mirroring C's "return the full length, contents undefined if it
/// overflows `destsize`" contract by always allocating exactly enough.
pub fn strnxfrm_libc<'mcx>(
    mcx: Mcx<'mcx>,
    src: &[u8],
    locale: &LibcLocale,
) -> PgResult<PgVec<'mcx, u8>> {
    // NUL-terminate the source.
    let mut srcbuf = alloc::vec::Vec::with_capacity(src.len() + 1);
    srcbuf.extend_from_slice(src);
    srcbuf.push(0);
    let srcp = srcbuf.as_ptr() as *const c_char;

    // First call with destsize 0 to learn the needed length (C contract:
    // returns the length needed, excluding the terminating NUL).
    // SAFETY: srcp is NUL-terminated; null dest with size 0 is permitted.
    let needed = unsafe { strxfrm_l(core::ptr::null_mut(), srcp, 0, locale.handle()) };

    let mut out = mcx::vec_with_capacity_in::<u8>(mcx, needed + 1)?;
    // Reserve `needed + 1` (room for the NUL strxfrm writes), fill, then trim.
    out.resize(needed + 1, 0u8);
    // SAFETY: out has needed+1 bytes; srcp NUL-terminated.
    let written = unsafe {
        strxfrm_l(out.as_mut_ptr() as *mut c_char, srcp, needed + 1, locale.handle())
    };
    // written should equal needed; trim the trailing NUL.
    out.truncate(written.min(needed));
    Ok(out)
}

/// `tolower_l(c, locale->info.lt)` (via `SB_lower_char` in like.c): single-byte
/// lower-case fold of `c` through the libc `locale_t`. Reached only for a
/// non-default, non-C libc collation.
pub fn char_tolower_libc(c: u8, locale: &LibcLocale) -> u8 {
    // SAFETY: tolower_l accepts any int; the handle is owned (or null).
    unsafe { tolower_l(c as libc::c_int, locale.handle()) as u8 }
}

/// `get_collation_actual_version_libc(collcollate)` (`pg_locale_libc.c:659`):
/// the glibc collation version (`gnu_get_libc_version`) for a non-C/POSIX
/// collation, else `None`.
pub fn get_collation_actual_version_libc<'mcx>(
    mcx: Mcx<'mcx>,
    collcollate: &str,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // C: pg_strcasecmp("C", x) && pg_strncasecmp("C.", x, 2) && pg_strcasecmp("POSIX", x).
    let is_c = collcollate.eq_ignore_ascii_case("C")
        || collcollate.len() >= 2 && collcollate[..2].eq_ignore_ascii_case("C.")
        || collcollate.eq_ignore_ascii_case("POSIX");
    if is_c {
        return Ok(None);
    }

    // #if defined(__GLIBC__): collversion = pstrdup(gnu_get_libc_version());
    #[cfg(target_env = "gnu")]
    {
        // SAFETY: gnu_get_libc_version returns a static NUL-terminated string.
        let ver = unsafe {
            let p = gnu_get_libc_version();
            core::ffi::CStr::from_ptr(p)
        };
        let bytes = ver.to_bytes();
        let mut out = mcx::vec_with_capacity_in::<u8>(mcx, bytes.len())?;
        out.extend_from_slice(bytes);
        return Ok(Some(out));
    }

    // Off-glibc (e.g. macOS) without LC_VERSION_MASK: C leaves collversion NULL.
    #[cfg(not(target_env = "gnu"))]
    {
        let _ = mcx;
        Ok(None)
    }
}

#[cfg(target_env = "gnu")]
extern "C" {
    fn gnu_get_libc_version() -> *const c_char;
}

/// `report_newlocale_failure(localename)` (`pg_locale_libc.c:804`): the
/// `ereport(ERROR)` for a failed `newlocale`, defaulting `errno` to `ENOENT`
/// with the "could not find any locale data" errdetail.
fn report_newlocale_failure(localename: &str) -> PgError {
    let mut save_errno = errno();
    // C: if (errno == 0) errno = ENOENT;
    if save_errno == 0 {
        save_errno = libc::ENOENT;
    }

    let mut err = PgError::new(types_error::ERROR, format!(
        "could not create locale \"{localename}\": {}",
        os_error_string(save_errno)
    ))
    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
    .with_error_location(ErrorLocation::new(
        "../src/backend/utils/adt/pg_locale_libc.c",
        823,
        "report_newlocale_failure",
    ));
    if save_errno == libc::ENOENT {
        err = err.with_detail(format!(
            "The operating system could not find any locale data for the locale name \"{localename}\"."
        ));
    }
    err
}

/// `%m` rendering of an errno (C: `strerror`).
fn os_error_string(err: libc::c_int) -> alloc::string::String {
    let s = std::io::Error::from_raw_os_error(err);
    alloc::string::ToString::to_string(&s)
}

/// Build a NUL-terminated byte vector for an FFI `*const c_char` argument.
fn cstr(s: &str) -> alloc::vec::Vec<u8> {
    let mut v = alloc::vec::Vec::with_capacity(s.len() + 1);
    v.extend_from_slice(s.as_bytes());
    v.push(0);
    v
}

/// Read `errno`.
fn errno() -> libc::c_int {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

/// Set `errno` (C: `errno = 0` before `newlocale`).
fn set_errno(value: libc::c_int) {
    // SAFETY: the platform errno location is valid for the current thread.
    #[cfg(target_os = "macos")]
    unsafe {
        *libc::__error() = value;
    }
    #[cfg(target_os = "linux")]
    unsafe {
        *libc::__errno_location() = value;
    }
}
