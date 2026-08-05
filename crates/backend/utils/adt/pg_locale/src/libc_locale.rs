//! pg_locale_libc.c: locale_t creation, strcoll_l/strxfrm_l comparison, and
//! the tolower_l/towlower_l case workers (SB and wchar MB arms).

use core::ffi::CStr;

use libc::{c_char, c_int, locale_t, size_t, wchar_t};

#[allow(non_camel_case_types)]
type wint_t = i32;
use mcx::Mcx;
use types_error::{
    PgError, PgResult, ERRCODE_CHARACTER_NOT_IN_REPERTOIRE, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_OUT_OF_MEMORY,
};

use crate::PgLocale;

const TEXTBUFLEN: usize = 1024;

extern "C" {
    fn strcoll_l(s1: *const c_char, s2: *const c_char, loc: locale_t) -> c_int;
    fn strxfrm_l(dest: *mut c_char, src: *const c_char, n: size_t, loc: locale_t) -> size_t;
    fn tolower_l(c: c_int, loc: locale_t) -> c_int;
    fn toupper_l(c: c_int, loc: locale_t) -> c_int;
    fn isalnum_l(c: c_int, loc: locale_t) -> c_int;
    fn towlower_l(c: wint_t, loc: locale_t) -> wint_t;
    fn towupper_l(c: wint_t, loc: locale_t) -> wint_t;
    fn iswalnum_l(c: wint_t, loc: locale_t) -> c_int;
    fn iswdigit_l(c: wint_t, loc: locale_t) -> c_int;
    fn iswalpha_l(c: wint_t, loc: locale_t) -> c_int;
    fn iswupper_l(c: wint_t, loc: locale_t) -> c_int;
    fn iswlower_l(c: wint_t, loc: locale_t) -> c_int;
    fn iswgraph_l(c: wint_t, loc: locale_t) -> c_int;
    fn iswprint_l(c: wint_t, loc: locale_t) -> c_int;
    fn iswpunct_l(c: wint_t, loc: locale_t) -> c_int;
    fn iswspace_l(c: wint_t, loc: locale_t) -> c_int;
    fn isdigit_l(c: c_int, loc: locale_t) -> c_int;
    fn isalpha_l(c: c_int, loc: locale_t) -> c_int;
    fn isupper_l(c: c_int, loc: locale_t) -> c_int;
    fn islower_l(c: c_int, loc: locale_t) -> c_int;
    fn isgraph_l(c: c_int, loc: locale_t) -> c_int;
    fn isprint_l(c: c_int, loc: locale_t) -> c_int;
    fn ispunct_l(c: c_int, loc: locale_t) -> c_int;
    fn isspace_l(c: c_int, loc: locale_t) -> c_int;
    #[cfg(target_os = "macos")]
    fn mbstowcs_l(dest: *mut wchar_t, src: *const c_char, n: size_t, loc: locale_t) -> size_t;
    #[cfg(target_os = "macos")]
    fn wcstombs_l(dest: *mut c_char, src: *const wchar_t, n: size_t, loc: locale_t) -> size_t;
}

// locale_t stored as usize so PgLocale entries stay Copy + Sync ('static
// cache entries); 0 encodes C's NULL ("C"/"POSIX", never passed to libc).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LibcLocale(usize);

impl LibcLocale {
    pub const NONE: LibcLocale = LibcLocale(0);

    fn get(self) -> locale_t {
        debug_assert!(self.0 != 0, "libc case/collate op on a C-locale locale_t");
        self.0 as locale_t
    }
}

// glibc has no mbstowcs_l/wcstombs_l and the libc crate binds neither
// mbstowcs nor wcstombs on linux-gnu; declare the C89 functions directly.
#[cfg(not(target_os = "macos"))]
extern "C" {
    fn mbstowcs(dest: *mut wchar_t, src: *const c_char, n: size_t) -> size_t;
    fn wcstombs(dest: *mut c_char, src: *const wchar_t, n: size_t) -> size_t;
}

#[cfg(not(target_os = "macos"))]
unsafe fn mbstowcs_l(dest: *mut wchar_t, src: *const c_char, n: size_t, loc: locale_t) -> size_t {
    let save = libc::uselocale(loc);
    let result = mbstowcs(dest, src, n);
    libc::uselocale(save);
    result
}

#[cfg(not(target_os = "macos"))]
unsafe fn wcstombs_l(dest: *mut c_char, src: *const wchar_t, n: size_t, loc: locale_t) -> size_t {
    let save = libc::uselocale(loc);
    let result = wcstombs(dest, src, n);
    libc::uselocale(save);
    result
}

#[track_caller]
#[cold]
fn report_newlocale_failure(localename: &str) -> Box<PgError> {
    // BSD-derived platforms may not set errno; assume ENOENT then.
    // SAFETY: errno is thread-local per POSIX.
    let mut errno = unsafe { *errno_location() };
    if errno == 0 {
        errno = libc::ENOENT;
    }
    let strerror = unsafe { CStr::from_ptr(libc::strerror(errno)) }.to_string_lossy();
    let mut err = PgError::error(format!(
        "could not create locale \"{localename}\": {strerror}"
    ))
    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE);
    if errno == libc::ENOENT {
        err = err.with_detail(format!(
            "The operating system could not find any locale data for the locale name \"{localename}\"."
        ));
    }
    err.into()
}

#[cfg(not(target_os = "macos"))]
unsafe fn errno_location() -> *mut c_int {
    libc::__errno_location()
}
#[cfg(target_os = "macos")]
unsafe fn errno_location() -> *mut c_int {
    libc::__error()
}

fn set_errno(v: c_int) {
    // SAFETY: errno is thread-local per POSIX.
    unsafe { *errno_location() = v }
}

fn cstr(s: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(s.len() + 1);
    v.extend_from_slice(s.as_bytes());
    v.push(0);
    v
}

fn newlocale(mask: c_int, locale: &str, base: locale_t) -> Option<locale_t> {
    let c = cstr(locale);
    set_errno(0);
    // SAFETY: c is NUL-terminated and outlives the call.
    let loc = unsafe { libc::newlocale(mask, c.as_ptr() as *const c_char, base) };
    if loc.is_null() {
        None
    } else {
        Some(loc)
    }
}

// cache_locale_time's newlocale(LC_ALL_MASK, ...) leg (pg_locale.c:752).
pub(crate) fn newlocale_all(locale: &str) -> PgResult<locale_t> {
    newlocale(crate::lc::LC_ALL_MASK, locale, core::ptr::null_mut())
        .ok_or_else(|| report_newlocale_failure(locale))
}

fn is_c_or_posix(name: &str) -> bool {
    name == "C" || name == "POSIX"
}

// make_libc_collator: "C"/"POSIX" are not handled by libc (NONE); no path
// leaks a locale_t.
pub(crate) fn make_libc_collator(collate: &str, ctype: &str) -> PgResult<LibcLocale> {
    if collate == ctype {
        if is_c_or_posix(ctype) {
            return Ok(LibcLocale::NONE);
        }
        let loc = newlocale(crate::lc::LC_COLLATE_MASK | crate::lc::LC_CTYPE_MASK, collate, core::ptr::null_mut())
            .ok_or_else(|| report_newlocale_failure(collate))?;
        return Ok(LibcLocale(loc as usize));
    }

    let loc1 = if !is_c_or_posix(collate) {
        Some(newlocale(crate::lc::LC_COLLATE_MASK, collate, core::ptr::null_mut())
            .ok_or_else(|| report_newlocale_failure(collate))?)
    } else {
        None
    };

    if !is_c_or_posix(ctype) {
        match newlocale(crate::lc::LC_CTYPE_MASK, ctype, loc1.unwrap_or(core::ptr::null_mut())) {
            Some(loc) => Ok(LibcLocale(loc as usize)),
            None => {
                let err = report_newlocale_failure(ctype);
                if let Some(l1) = loc1 {
                    // SAFETY: l1 came from newlocale and was not consumed
                    // (a failed newlocale does not free its base).
                    unsafe { libc::freelocale(l1) };
                }
                Err(err)
            }
        }
    } else {
        Ok(loc1.map_or(LibcLocale::NONE, |l| LibcLocale(l as usize)))
    }
}

thread_local! {
    // C's sbuf[TEXTBUFLEN]-else-palloc: retained scratch, no per-call malloc.
    static COLL_SCRATCH: core::cell::RefCell<Vec<u8>> =
        core::cell::RefCell::new(Vec::with_capacity(2 * TEXTBUFLEN));
}

pub(crate) fn strncoll_libc(arg1: &[u8], arg2: &[u8], lt: LibcLocale) -> i32 {
    COLL_SCRATCH.with(|cell| {
        let mut buf = cell.borrow_mut();
        buf.clear();
        buf.reserve(arg1.len() + arg2.len() + 2);
        buf.extend_from_slice(arg1);
        buf.push(0);
        buf.extend_from_slice(arg2);
        buf.push(0);
        let p1 = buf.as_ptr() as *const c_char;
        // SAFETY: both slices were just NUL-terminated into one buffer.
        unsafe { strcoll_l(p1, p1.add(arg1.len() + 1), lt.get()) }
    })
}

pub(crate) fn strnxfrm_libc(dest: &mut [u8], src: &[u8], lt: LibcLocale) -> usize {
    COLL_SCRATCH.with(|cell| {
        let mut buf = cell.borrow_mut();
        buf.clear();
        buf.reserve(src.len() + 1);
        buf.extend_from_slice(src);
        buf.push(0);
        // SAFETY: src is NUL-terminated; strxfrm_l writes at most dest.len().
        unsafe {
            strxfrm_l(
                dest.as_mut_ptr() as *mut c_char,
                buf.as_ptr() as *const c_char,
                dest.len(),
                lt.get(),
            )
        }
    })
}

// pg_tolower/pg_toupper (port/pgstrcasecmp.c): ASCII-forced A-Z/a-z, libc
// global-locale fold for high-bit bytes (SB encodings only).
pub fn pg_tolower(ch: u8) -> u8 {
    if ch.is_ascii_uppercase() {
        ch + b'a' - b'A'
    } else if ch >= 0x80 && unsafe { libc::isupper(ch as c_int) } != 0 {
        unsafe { libc::tolower(ch as c_int) as u8 }
    } else {
        ch
    }
}

pub fn pg_toupper(ch: u8) -> u8 {
    if ch.is_ascii_lowercase() {
        ch - (b'a' - b'A')
    } else if ch >= 0x80 && unsafe { libc::islower(ch as c_int) } != 0 {
        unsafe { libc::toupper(ch as c_int) as u8 }
    } else {
        ch
    }
}

pub(crate) fn tolower_l_byte(c: u8, lt: LibcLocale) -> u8 {
    // SAFETY: pure ctype call; c promoted as unsigned char per C.
    unsafe { tolower_l(c as c_int, lt.get()) as u8 }
}

/// wc-ctype classes probed by the regex engine (regc_pg_locale.c).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WcClass {
    Digit,
    Alpha,
    Alnum,
    Upper,
    Lower,
    Graph,
    Print,
    Punct,
    Space,
}

// regc_pg_locale.c LIBC_WIDE arms: wchar_t is 4 bytes on every supported
// target, so C's `sizeof(wchar_t) >= 4 || c <= 0xFFFF` wide gate is always
// taken and the in-function 1-byte fall-thru is dead here.
pub(crate) fn wc_isclass_wide(c: u32, class: WcClass, lt: LibcLocale) -> bool {
    let c = c as wint_t;
    let l = lt.get();
    // SAFETY: pure wctype calls.
    (unsafe {
        match class {
            WcClass::Digit => iswdigit_l(c, l),
            WcClass::Alpha => iswalpha_l(c, l),
            WcClass::Alnum => iswalnum_l(c, l),
            WcClass::Upper => iswupper_l(c, l),
            WcClass::Lower => iswlower_l(c, l),
            WcClass::Graph => iswgraph_l(c, l),
            WcClass::Print => iswprint_l(c, l),
            WcClass::Punct => iswpunct_l(c, l),
            WcClass::Space => iswspace_l(c, l),
        }
    }) != 0
}

// regc_pg_locale.c LIBC_1BYTE arms: c <= UCHAR_MAX guard lives at the caller.
pub(crate) fn wc_isclass_1byte(c: u8, class: WcClass, lt: LibcLocale) -> bool {
    let c = c as c_int;
    let l = lt.get();
    // SAFETY: pure ctype calls.
    (unsafe {
        match class {
            WcClass::Digit => isdigit_l(c, l),
            WcClass::Alpha => isalpha_l(c, l),
            WcClass::Alnum => isalnum_l(c, l),
            WcClass::Upper => isupper_l(c, l),
            WcClass::Lower => islower_l(c, l),
            WcClass::Graph => isgraph_l(c, l),
            WcClass::Print => isprint_l(c, l),
            WcClass::Punct => ispunct_l(c, l),
            WcClass::Space => isspace_l(c, l),
        }
    }) != 0
}

pub(crate) fn wc_toupper_wide(c: u32, lt: LibcLocale) -> u32 {
    // SAFETY: pure wctype call.
    unsafe { towupper_l(c as wint_t, lt.get()) as u32 }
}

pub(crate) fn wc_tolower_wide(c: u32, lt: LibcLocale) -> u32 {
    // SAFETY: pure wctype call.
    unsafe { towlower_l(c as wint_t, lt.get()) as u32 }
}

pub(crate) fn wc_toupper_1byte(c: u8, lt: LibcLocale) -> u32 {
    // SAFETY: pure ctype call.
    unsafe { toupper_l(c as c_int, lt.get()) as u32 }
}

pub(crate) fn wc_tolower_1byte(c: u8, lt: LibcLocale) -> u32 {
    // SAFETY: pure ctype call.
    unsafe { tolower_l(c as c_int, lt.get()) as u32 }
}

fn write_prefix(dest: &mut [u8], src: &[u8]) {
    // C's SB arms: no-op unless srclen+1 fits (caller retries with a bigger
    // buffer using the returned length).
    if src.len() + 1 <= dest.len() {
        dest[..src.len()].copy_from_slice(src);
        dest[src.len()] = 0;
    }
}

pub(crate) fn strlower_libc_sb(dest: &mut [u8], src: &[u8], locale: &PgLocale) -> usize {
    write_prefix(dest, src);
    if src.len() + 1 <= dest.len() {
        for p in &mut dest[..src.len()] {
            if *p == 0 {
                break;
            }
            *p = if locale.is_default {
                pg_tolower(*p)
            } else {
                tolower_l_byte(*p, locale.lt)
            };
        }
    }
    src.len()
}

pub(crate) fn strupper_libc_sb(dest: &mut [u8], src: &[u8], locale: &PgLocale) -> usize {
    write_prefix(dest, src);
    if src.len() + 1 <= dest.len() {
        for p in &mut dest[..src.len()] {
            if *p == 0 {
                break;
            }
            *p = if locale.is_default {
                pg_toupper(*p)
            } else {
                // SAFETY: pure ctype call.
                unsafe { toupper_l(*p as c_int, locale.lt.get()) as u8 }
            };
        }
    }
    src.len()
}

pub(crate) fn strtitle_libc_sb(dest: &mut [u8], src: &[u8], locale: &PgLocale) -> usize {
    write_prefix(dest, src);
    if src.len() + 1 <= dest.len() {
        let mut wasalnum = false;
        for p in &mut dest[..src.len()] {
            if *p == 0 {
                break;
            }
            let c = if locale.is_default {
                if wasalnum {
                    pg_tolower(*p)
                } else {
                    pg_toupper(*p)
                }
            } else if wasalnum {
                tolower_l_byte(*p, locale.lt)
            } else {
                // SAFETY: pure ctype call.
                unsafe { toupper_l(*p as c_int, locale.lt.get()) as u8 }
            };
            *p = c;
            // SAFETY: pure ctype call.
            wasalnum = unsafe { isalnum_l(c as c_int, locale.lt.get()) } != 0;
        }
    }
    src.len()
}

#[track_caller]
#[cold]
fn oom() -> Box<PgError> {
    PgError::error("out of memory")
        .with_sqlstate(ERRCODE_OUT_OF_MEMORY)
        .into()
}

#[track_caller]
#[cold]
fn invalid_multibyte_for_locale() -> Box<PgError> {
    PgError::error("invalid multibyte character for locale")
        .with_sqlstate(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE)
        .with_hint(
            "The server's LC_CTYPE locale is probably incompatible with the database encoding.",
        )
        .into()
}

// char2wchar (pg_locale_libc.c): libc wchar_t, not pg_wchar; ereports on
// invalid input via pg_verifymbstr.
fn char2wchar(to: &mut [wchar_t], from: &[u8], lt: LibcLocale) -> PgResult<usize> {
    if to.is_empty() {
        return Ok(0);
    }
    let str_nul = cstr_bytes(from);
    // SAFETY: str_nul is NUL-terminated; mbstowcs_l writes at most to.len().
    let result = unsafe {
        mbstowcs_l(
            to.as_mut_ptr(),
            str_nul.as_ptr() as *const c_char,
            to.len(),
            lt.get(),
        )
    };
    if result == usize::MAX {
        mbutils::pg_verifymbstr(from, false)?;
        return Err(invalid_multibyte_for_locale());
    }
    Ok(result)
}

fn wchar2char(to: &mut [u8], from: &[wchar_t], lt: LibcLocale) -> usize {
    if to.is_empty() {
        return 0;
    }
    // SAFETY: from is NUL-terminated (workspace convention); at most
    // to.len() bytes written.
    unsafe {
        wcstombs_l(
            to.as_mut_ptr() as *mut c_char,
            from.as_ptr(),
            to.len(),
            lt.get(),
        )
    }
}

fn cstr_bytes(b: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(b.len() + 1);
    v.extend_from_slice(b);
    v.push(0);
    v
}

enum MbCase {
    Lower,
    Upper,
    Title,
}

fn case_libc_mb(
    mcx: Mcx<'_>,
    dest: &mut [u8],
    src: &[u8],
    locale: &PgLocale,
    kind: MbCase,
) -> PgResult<usize> {
    if src.len() + 1 > (i32::MAX as usize) / core::mem::size_of::<wchar_t>() {
        return Err(oom());
    }
    let lt = locale.lt;
    let mut workspace: mcx::PgVec<'_, wchar_t> = mcx::vec_with_capacity_in(mcx, src.len() + 1)?;
    workspace.resize(src.len() + 1, 0);
    char2wchar(&mut workspace, src, lt)?;

    let mut curr_char = 0usize;
    let mut wasalnum = false;
    while workspace[curr_char] != 0 {
        let wc = workspace[curr_char] as wint_t;
        // SAFETY: pure wctype calls.
        workspace[curr_char] = unsafe {
            match kind {
                MbCase::Lower => towlower_l(wc, lt.get()),
                MbCase::Upper => towupper_l(wc, lt.get()),
                MbCase::Title => {
                    if wasalnum {
                        towlower_l(wc, lt.get())
                    } else {
                        towupper_l(wc, lt.get())
                    }
                }
            }
        } as wchar_t;
        if matches!(kind, MbCase::Title) {
            // SAFETY: pure wctype call.
            wasalnum =
                unsafe { iswalnum_l(workspace[curr_char] as wint_t, lt.get()) } != 0;
        }
        curr_char += 1;
    }

    let max_size = curr_char * mbutils::pg_database_encoding_max_length() as usize;
    let mut result: mcx::PgVec<'_, u8> = mcx::vec_with_capacity_in(mcx, max_size + 1)?;
    result.resize(max_size + 1, 0);
    let result_size = wchar2char(&mut result, &workspace, lt);
    // wcstombs failure would mean the towlower output can't re-encode.
    debug_assert!(result_size != usize::MAX);

    if dest.len() >= result_size + 1 {
        dest[..result_size].copy_from_slice(&result[..result_size]);
        dest[result_size] = 0;
    }
    Ok(result_size)
}

pub(crate) fn strlower_libc<'mcx>(
    mcx: Mcx<'mcx>,
    dest: &mut [u8],
    src: &[u8],
    locale: &PgLocale,
) -> PgResult<usize> {
    if mbutils::pg_database_encoding_max_length() > 1 {
        case_libc_mb(mcx, dest, src, locale, MbCase::Lower)
    } else {
        Ok(strlower_libc_sb(dest, src, locale))
    }
}

pub(crate) fn strtitle_libc<'mcx>(
    mcx: Mcx<'mcx>,
    dest: &mut [u8],
    src: &[u8],
    locale: &PgLocale,
) -> PgResult<usize> {
    if mbutils::pg_database_encoding_max_length() > 1 {
        case_libc_mb(mcx, dest, src, locale, MbCase::Title)
    } else {
        Ok(strtitle_libc_sb(dest, src, locale))
    }
}

pub(crate) fn strupper_libc<'mcx>(
    mcx: Mcx<'mcx>,
    dest: &mut [u8],
    src: &[u8],
    locale: &PgLocale,
) -> PgResult<usize> {
    if mbutils::pg_database_encoding_max_length() > 1 {
        case_libc_mb(mcx, dest, src, locale, MbCase::Upper)
    } else {
        Ok(strupper_libc_sb(dest, src, locale))
    }
}
