//! pg_locale_icu.c: collator open (ICU >= 55 keyword-locale semantics; the
//! pre-55 und/root + attribute-emulation legs are loud), strncoll/strnxfrm
//! (+prefix), the u_strTo* case workers, language-tag canonicalization and
//! locale validation for CREATE COLLATION, and collversion via ucol_getVersion.

use core::cell::{Cell, RefCell};
use core::ffi::c_char;

use mcx::Mcx;
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE, ErrorLevel,
};

use crate::icu_ffi::{self as ffi, IcuApi, UChar, UCharIterator, UErrorCode};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IcuLocale {
    ucol: usize,
    // NUL-terminated locale string interned in the collation cache context.
    loc: Option<&'static [u8]>,
    utf8: bool,
}

impl IcuLocale {
    pub const NONE: IcuLocale = IcuLocale {
        ucol: 0,
        loc: None,
        utf8: false,
    };

    #[inline]
    fn ucol(self) -> *const core::ffi::c_void {
        debug_assert!(self.ucol != 0, "ICU op on an unopened collator");
        self.ucol as *const core::ffi::c_void
    }

    fn loc_ptr(self) -> *const c_char {
        self.loc.expect("ICU case op without locale string").as_ptr() as *const c_char
    }
}

thread_local! {
    static ICU_CONVERTER: Cell<usize> = const { Cell::new(0) };
    static UBUF1: RefCell<Vec<UChar>> = const { RefCell::new(Vec::new()) };
    static UBUF2: RefCell<Vec<UChar>> = const { RefCell::new(Vec::new()) };
}

#[track_caller]
#[cold]
fn icu_error(msg: String) -> Box<PgError> {
    PgError::error(msg).into()
}

fn errname(api: &IcuApi, status: UErrorCode) -> String {
    ffi::u_errorName_str(api, status)
}

// pg_enc2icu_tbl (encnames.c); None entries are not supported by ICU.
fn get_encoding_name_for_icu(encoding: i32) -> Option<&'static str> {
    Some(match encoding {
        1 => "EUC-JP",
        2 => "EUC-CN",
        3 => "EUC-KR",
        4 => "EUC-TW",
        6 => "UTF-8",
        8 => "ISO-8859-1",
        9 => "ISO-8859-2",
        10 => "ISO-8859-3",
        11 => "ISO-8859-4",
        12 => "ISO-8859-9",
        13 => "ISO-8859-10",
        14 => "ISO-8859-13",
        15 => "ISO-8859-14",
        16 => "ISO-8859-15",
        18 => "CP1256",
        19 => "CP1258",
        20 => "CP866",
        22 => "KOI8-R",
        23 => "CP1251",
        24 => "CP1252",
        25 => "ISO-8859-5",
        26 => "ISO-8859-6",
        27 => "ISO-8859-7",
        28 => "ISO-8859-8",
        29 => "CP1250",
        30 => "CP1253",
        31 => "CP1254",
        32 => "CP1255",
        33 => "CP1257",
        34 => "KOI8-U",
        _ => return None,
    })
}

fn init_icu_converter() -> PgResult<*mut ffi::UConverter> {
    let cached = ICU_CONVERTER.with(Cell::get);
    if cached != 0 {
        return Ok(cached as *mut ffi::UConverter);
    }
    let api = ffi::icu();
    let encoding = mbutils::GetDatabaseEncoding();
    let Some(name) = get_encoding_name_for_icu(encoding) else {
        return Err(PgError::error(format!(
            "encoding \"{}\" not supported by ICU",
            mbutils::GetDatabaseEncodingName()
        ))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
        .into());
    };
    let cname = format!("{name}\0");
    let mut status = ffi::U_ZERO_ERROR;
    // SAFETY: cname is NUL-terminated.
    let conv = unsafe { (api.ucnv_open)(cname.as_ptr() as *const c_char, &mut status) };
    if ffi::U_FAILURE(status) {
        return Err(icu_error(format!(
            "could not open ICU converter for encoding \"{name}\": {}",
            errname(api, status)
        )));
    }
    ICU_CONVERTER.with(|c| c.set(conv as usize));
    Ok(conv)
}

// uchar_length + uchar_convert fused: converts src into buf (terminated),
// returning the UChar length.
fn to_uchars(
    api: &IcuApi,
    conv: *mut ffi::UConverter,
    buf: &mut Vec<UChar>,
    src: &[u8],
) -> PgResult<i32> {
    let mut status = ffi::U_ZERO_ERROR;
    // SAFETY: sizing call (null dest, zero capacity) per ICU preflight rules.
    let ulen = unsafe {
        (api.ucnv_toUChars)(
            conv,
            core::ptr::null_mut(),
            0,
            src.as_ptr() as *const c_char,
            src.len() as i32,
            &mut status,
        )
    };
    if ffi::U_FAILURE(status) && status != ffi::U_BUFFER_OVERFLOW_ERROR {
        return Err(icu_error(format!(
            "ucnv_toUChars failed: {}",
            errname(api, status)
        )));
    }
    buf.clear();
    buf.resize(ulen as usize + 1, 0);
    let mut status = ffi::U_ZERO_ERROR;
    // SAFETY: buf holds ulen+1 UChars.
    let ulen = unsafe {
        (api.ucnv_toUChars)(
            conv,
            buf.as_mut_ptr(),
            ulen + 1,
            src.as_ptr() as *const c_char,
            src.len() as i32,
            &mut status,
        )
    };
    if ffi::U_FAILURE(status) {
        return Err(icu_error(format!(
            "ucnv_toUChars failed: {}",
            errname(api, status)
        )));
    }
    Ok(ulen)
}

fn major_supported(api: &IcuApi) {
    if api.major != 0 && api.major < 55 {
        panic!(
            "pg_locale_icu: libicu major {} < 55 unsupported (pre-55 und/root and \
             attribute-emulation legs unported)",
            api.major
        );
    }
}

/// pg_ucol_open; the returned collator is never closed (collation cache
/// entries live for the backend, as in C).
fn pg_ucol_open(loc_str: &str) -> PgResult<*mut ffi::UCollator> {
    let api = ffi::icu();
    major_supported(api);
    let cloc = format!("{loc_str}\0");
    let mut status = ffi::U_ZERO_ERROR;
    // SAFETY: cloc is NUL-terminated.
    let collator = unsafe { (api.ucol_open)(cloc.as_ptr() as *const c_char, &mut status) };
    if ffi::U_FAILURE(status) {
        return Err(PgError::error(format!(
            "could not open collator for locale \"{loc_str}\": {}",
            errname(api, status)
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
        .into());
    }
    Ok(collator)
}

fn make_icu_collator(iculocstr: &str, icurules: Option<&str>) -> PgResult<*mut ffi::UCollator> {
    let Some(icurules) = icurules else {
        return pg_ucol_open(iculocstr);
    };
    let api = ffi::icu();
    let conv = init_icu_converter()?;
    let mut my_rules: Vec<UChar> = Vec::new();
    let my_len = to_uchars(api, conv, &mut my_rules, icurules.as_bytes())?;

    let collator_std_rules = pg_ucol_open(iculocstr)?;
    let mut std_len: i32 = 0;
    // SAFETY: live collator; getRules returns a collator-owned buffer.
    let std_rules = unsafe { (api.ucol_getRules)(collator_std_rules, &mut std_len) };
    let mut all_rules: Vec<UChar> =
        Vec::with_capacity(std_len as usize + my_len as usize + 1);
    // SAFETY: std_rules points at std_len UChars.
    all_rules
        .extend_from_slice(unsafe { core::slice::from_raw_parts(std_rules, std_len as usize) });
    all_rules.extend_from_slice(&my_rules[..my_len as usize]);
    // SAFETY: closing the intermediate collator C closes too.
    unsafe { (api.ucol_close)(collator_std_rules) };

    let mut status = ffi::U_ZERO_ERROR;
    // SAFETY: all_rules holds the combined rule UChars.
    let collator = unsafe {
        (api.ucol_openRules)(
            all_rules.as_ptr(),
            all_rules.len() as i32,
            ffi::UCOL_DEFAULT,
            ffi::UCOL_DEFAULT_STRENGTH,
            core::ptr::null_mut(),
            &mut status,
        )
    };
    if ffi::U_FAILURE(status) {
        return Err(PgError::error(format!(
            "could not open collator for locale \"{iculocstr}\" with rules \"{}\": {}",
            icurules,
            errname(api, status)
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
        .into());
    }
    Ok(collator)
}

pub(crate) fn create_icu_locale(
    cache_mcx: Mcx<'static>,
    iculocstr: &str,
    icurules: Option<&str>,
) -> PgResult<IcuLocale> {
    let collator = make_icu_collator(iculocstr, icurules)?;
    let mut owned = iculocstr.as_bytes().to_vec();
    owned.push(0);
    let loc = mcx::slice_borrow_in(cache_mcx, &owned)?;
    Ok(IcuLocale {
        ucol: collator as usize,
        loc: Some(loc),
        utf8: mbutils::GetDatabaseEncoding() == crate::PG_UTF8,
    })
}

/// strncoll_icu_utf8 / strncoll_icu. ICU failures are C ereport(ERROR)s on
/// can't-happen inputs (server-validated encoding); loud here.
pub(crate) fn strncoll(arg1: &[u8], arg2: &[u8], locale: IcuLocale) -> i32 {
    let api = ffi::icu();
    if locale.utf8 {
        let mut status = ffi::U_ZERO_ERROR;
        // SAFETY: live collator; args are byte slices with explicit lengths.
        let result = unsafe {
            (api.ucol_strcollUTF8)(
                locale.ucol() as *const ffi::UCollator,
                arg1.as_ptr() as *const c_char,
                arg1.len() as i32,
                arg2.as_ptr() as *const c_char,
                arg2.len() as i32,
                &mut status,
            )
        };
        if ffi::U_FAILURE(status) {
            panic!("collation failed: {}", errname(api, status));
        }
        return result;
    }
    let conv = match init_icu_converter() {
        Ok(c) => c,
        Err(e) => panic!("pg_locale_icu strncoll: {e:?}"),
    };
    UBUF1.with(|c1| {
        UBUF2.with(|c2| {
            let (mut b1, mut b2) = (c1.borrow_mut(), c2.borrow_mut());
            let ulen1 = to_uchars(api, conv, &mut b1, arg1)
                .unwrap_or_else(|e| panic!("pg_locale_icu strncoll: {e:?}"));
            let ulen2 = to_uchars(api, conv, &mut b2, arg2)
                .unwrap_or_else(|e| panic!("pg_locale_icu strncoll: {e:?}"));
            // SAFETY: both buffers hold ulen+1 terminated UChars.
            unsafe {
                (api.ucol_strcoll)(
                    locale.ucol() as *const ffi::UCollator,
                    b1.as_ptr(),
                    ulen1,
                    b2.as_ptr(),
                    ulen2,
                )
            }
        })
    })
}

/// strnxfrm_icu: returns the sort-key size excluding its NUL; dest content is
/// valid only when the result fits (result < dest.len()).
pub(crate) fn strnxfrm(dest: &mut [u8], src: &[u8], locale: IcuLocale) -> usize {
    let api = ffi::icu();
    let conv = match init_icu_converter() {
        Ok(c) => c,
        Err(e) => panic!("pg_locale_icu strnxfrm: {e:?}"),
    };
    UBUF1.with(|c1| {
        let mut b1 = c1.borrow_mut();
        let ulen = to_uchars(api, conv, &mut b1, src)
            .unwrap_or_else(|e| panic!("pg_locale_icu strnxfrm: {e:?}"));
        // SAFETY: b1 holds ulen terminated UChars; getSortKey writes at most
        // dest.len() bytes.
        let result_bsize = unsafe {
            (api.ucol_getSortKey)(
                locale.ucol() as *const ffi::UCollator,
                b1.as_ptr(),
                ulen,
                dest.as_mut_ptr(),
                dest.len() as i32,
            )
        };
        debug_assert!(result_bsize > 0);
        // ucol_getSortKey counts the NUL terminator; pg_strnxfrm does not.
        (result_bsize - 1) as usize
    })
}

/// strnxfrm_prefix_icu(_utf8): state-machine prefix of the sort key.
pub(crate) fn strnxfrm_prefix(dest: &mut [u8], src: &[u8], locale: IcuLocale) -> usize {
    let api = ffi::icu();
    let mut iter = UCharIterator::zeroed();
    let mut state = [0u32; 2];
    let mut status = ffi::U_ZERO_ERROR;
    let result = if locale.utf8 {
        // SAFETY: iter is a zeroed blob uiter_setUTF8 initializes; src outlives
        // the nextSortKeyPart call.
        unsafe {
            (api.uiter_setUTF8)(&mut iter, src.as_ptr() as *const c_char, src.len() as i32);
            (api.ucol_nextSortKeyPart)(
                locale.ucol() as *const ffi::UCollator,
                &mut iter,
                state.as_mut_ptr(),
                dest.as_mut_ptr(),
                dest.len() as i32,
                &mut status,
            )
        }
    } else {
        let conv = match init_icu_converter() {
            Ok(c) => c,
            Err(e) => panic!("pg_locale_icu strnxfrm_prefix: {e:?}"),
        };
        UBUF1.with(|c1| {
            let mut b1 = c1.borrow_mut();
            let ulen = to_uchars(api, conv, &mut b1, src)
                .unwrap_or_else(|e| panic!("pg_locale_icu strnxfrm_prefix: {e:?}"));
            // SAFETY: as the UTF-8 arm, over the converted UChar buffer.
            unsafe {
                (api.uiter_setString)(&mut iter, b1.as_ptr(), ulen);
                (api.ucol_nextSortKeyPart)(
                    locale.ucol() as *const ffi::UCollator,
                    &mut iter,
                    state.as_mut_ptr(),
                    dest.as_mut_ptr(),
                    dest.len() as i32,
                    &mut status,
                )
            }
        })
    };
    if ffi::U_FAILURE(status) {
        panic!("sort key generation failed: {}", errname(api, status));
    }
    result as usize
}

type CaseKind = u8;
pub(crate) const CASE_LOWER: CaseKind = 0;
pub(crate) const CASE_TITLE: CaseKind = 1;
pub(crate) const CASE_UPPER: CaseKind = 2;
pub(crate) const CASE_FOLD: CaseKind = 3;

// icu_convert_case: try at source length, retry once on overflow.
fn convert_case(
    api: &IcuApi,
    kind: CaseKind,
    locale: IcuLocale,
    dest: &mut Vec<UChar>,
    src: &[UChar],
) -> PgResult<i32> {
    let call = |dest: &mut Vec<UChar>, cap: i32, status: &mut UErrorCode| -> i32 {
        // SAFETY: dest holds cap UChars; locale.loc is NUL-terminated.
        unsafe {
            match kind {
                CASE_LOWER => (api.u_strToLower)(
                    dest.as_mut_ptr(), cap, src.as_ptr(), src.len() as i32,
                    locale.loc_ptr(), status,
                ),
                CASE_UPPER => (api.u_strToUpper)(
                    dest.as_mut_ptr(), cap, src.as_ptr(), src.len() as i32,
                    locale.loc_ptr(), status,
                ),
                CASE_TITLE => (api.u_strToTitle)(
                    dest.as_mut_ptr(), cap, src.as_ptr(), src.len() as i32,
                    core::ptr::null_mut(), locale.loc_ptr(), status,
                ),
                _ => {
                    // u_strFoldCase has no locale; Turkic 'tr'/'az' get the
                    // 'T' mappings via U_FOLD_CASE_EXCLUDE_SPECIAL_I.
                    let mut options = ffi::U_FOLD_CASE_DEFAULT;
                    let mut lang = [0 as c_char; 3];
                    let mut st = ffi::U_ZERO_ERROR;
                    (api.uloc_getLanguage)(locale.loc_ptr(), lang.as_mut_ptr(), 3, &mut st);
                    if ffi::U_SUCCESS(st)
                        && (lang[..2] == [b't' as c_char, b'r' as c_char] && lang[2] == 0
                            || lang[..2] == [b'a' as c_char, b'z' as c_char] && lang[2] == 0)
                    {
                        options = ffi::U_FOLD_CASE_EXCLUDE_SPECIAL_I;
                    }
                    (api.u_strFoldCase)(
                        dest.as_mut_ptr(), cap, src.as_ptr(), src.len() as i32, options, status,
                    )
                }
            }
        }
    };
    let mut len_dest = src.len() as i32;
    dest.clear();
    dest.resize(len_dest as usize, 0);
    let mut status = ffi::U_ZERO_ERROR;
    len_dest = call(dest, len_dest, &mut status);
    if status == ffi::U_BUFFER_OVERFLOW_ERROR {
        dest.clear();
        dest.resize(len_dest as usize, 0);
        status = ffi::U_ZERO_ERROR;
        len_dest = call(dest, len_dest, &mut status);
    }
    if ffi::U_FAILURE(status) {
        return Err(icu_error(format!(
            "case conversion failed: {}",
            errname(api, status)
        )));
    }
    Ok(len_dest)
}

// icu_from_uchar: write into dest if it fits (with NUL); return the byte
// length excluding NUL either way.
fn from_uchars(
    api: &IcuApi,
    conv: *mut ffi::UConverter,
    dest: &mut [u8],
    src: &[UChar],
) -> PgResult<usize> {
    let mut status = ffi::U_ZERO_ERROR;
    // SAFETY: sizing call.
    let len_result = unsafe {
        (api.ucnv_fromUChars)(
            conv,
            core::ptr::null_mut(),
            0,
            src.as_ptr(),
            src.len() as i32,
            &mut status,
        )
    };
    if ffi::U_FAILURE(status) && status != ffi::U_BUFFER_OVERFLOW_ERROR {
        return Err(icu_error(format!(
            "ucnv_fromUChars failed: {}",
            errname(api, status)
        )));
    }
    if len_result as usize + 1 > dest.len() {
        return Ok(len_result as usize);
    }
    let mut status = ffi::U_ZERO_ERROR;
    // SAFETY: dest holds len_result+1 bytes.
    let len_result = unsafe {
        (api.ucnv_fromUChars)(
            conv,
            dest.as_mut_ptr() as *mut c_char,
            len_result + 1,
            src.as_ptr(),
            src.len() as i32,
            &mut status,
        )
    };
    if ffi::U_FAILURE(status) || status == ffi::U_STRING_NOT_TERMINATED_WARNING {
        return Err(icu_error(format!(
            "ucnv_fromUChars failed: {}",
            errname(api, status)
        )));
    }
    Ok(len_result as usize)
}

/// strlower_icu/strtitle_icu/strupper_icu/strfold_icu.
pub(crate) fn str_case(
    kind: CaseKind,
    dest: &mut [u8],
    src: &[u8],
    locale: IcuLocale,
) -> PgResult<usize> {
    let api = ffi::icu();
    let conv = init_icu_converter()?;
    UBUF1.with(|c1| {
        UBUF2.with(|c2| {
            let (mut b1, mut b2) = (c1.borrow_mut(), c2.borrow_mut());
            let ulen = to_uchars(api, conv, &mut b1, src)?;
            let conv_len = convert_case(api, kind, locale, &mut b2, &b1[..ulen as usize])?;
            from_uchars(api, conv, dest, &b2[..conv_len as usize])
        })
    })
}

/// get_collation_actual_version_icu.
pub(crate) fn get_collation_actual_version_icu(collcollate: &str) -> PgResult<String> {
    let api = ffi::icu();
    let collator = pg_ucol_open(collcollate)?;
    let mut versioninfo = [0u8; 4];
    let mut buf = [0u8; ffi::U_MAX_VERSION_STRING_LENGTH];
    // SAFETY: live collator; buf covers U_MAX_VERSION_STRING_LENGTH.
    unsafe {
        (api.ucol_getVersion)(collator, versioninfo.as_mut_ptr());
        (api.ucol_close)(collator);
        (api.u_versionToString)(versioninfo.as_ptr(), buf.as_mut_ptr() as *mut c_char);
    }
    let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    Ok(String::from_utf8_lossy(&buf[..len]).into_owned())
}

/// icu_language_tag (pg_locale.c): BCP47 canonicalization; Ok(None) on
/// conversion failure at sub-ERROR elevels (reported at elevel per C).
pub fn icu_language_tag(loc_str: &str, elevel: ErrorLevel) -> PgResult<Option<String>> {
    let api = ffi::icu();
    let cloc = format!("{loc_str}\0");
    let mut buflen: usize = 32;
    let mut langtag: Vec<u8>;
    let mut status;
    loop {
        langtag = vec![0; buflen];
        status = ffi::U_ZERO_ERROR;
        // SAFETY: cloc NUL-terminated; langtag holds buflen bytes.
        unsafe {
            (api.uloc_toLanguageTag)(
                cloc.as_ptr() as *const c_char,
                langtag.as_mut_ptr() as *mut c_char,
                buflen as i32,
                1,
                &mut status,
            );
        }
        if (status == ffi::U_BUFFER_OVERFLOW_ERROR
            || status == ffi::U_STRING_NOT_TERMINATED_WARNING)
            && buflen < 0x3fffffff
        {
            buflen *= 2;
            continue;
        }
        break;
    }
    if ffi::U_FAILURE(status) {
        if elevel.0 > 0 {
            elog::ereport(elevel)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "could not convert locale name \"{loc_str}\" to language tag: {}",
                    errname(api, status)
                ))
                .finish(crate::loc(1590, "icu_language_tag"))?;
        }
        return Ok(None);
    }
    let len = langtag.iter().position(|&b| b == 0).unwrap_or(langtag.len());
    Ok(Some(String::from_utf8_lossy(&langtag[..len]).into_owned()))
}

/// icu_validate_locale (pg_locale.c); elevel = icu_validation_level GUC.
pub fn icu_validate_locale(loc_str: &str) -> PgResult<()> {
    let api = ffi::icu();
    let elevel = ErrorLevel(guc_tables::vars::icu_validation_level.read());
    if elevel.0 < 0 {
        return Ok(());
    }
    let hint = || {
        "To disable ICU locale validation, set the parameter \
         \"icu_validation_level\" to \"disabled\"."
            .to_string()
    };
    let cloc = format!("{loc_str}\0");
    let mut lang = [0 as c_char; ffi::ULOC_LANG_CAPACITY];
    let mut status = ffi::U_ZERO_ERROR;
    // SAFETY: cloc NUL-terminated; lang covers ULOC_LANG_CAPACITY.
    unsafe {
        (api.uloc_getLanguage)(
            cloc.as_ptr() as *const c_char,
            lang.as_mut_ptr(),
            ffi::ULOC_LANG_CAPACITY as i32,
            &mut status,
        );
    }
    if ffi::U_FAILURE(status) || status == ffi::U_STRING_NOT_TERMINATED_WARNING {
        elog::ereport(elevel)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "could not get language from ICU locale \"{loc_str}\": {}",
                errname(api, status)
            ))
            .errhint(hint())
            .finish(crate::loc(1633, "icu_validate_locale"))?;
        return Ok(());
    }
    let lang_bytes: Vec<u8> = lang
        .iter()
        .take_while(|&&b| b != 0)
        .map(|&b| b as u8)
        .collect();
    let lang_str = String::from_utf8_lossy(&lang_bytes).into_owned();
    let mut found = lang_str.is_empty() || lang_str == "root" || lang_str == "und";
    if !found {
        // SAFETY: uloc_getAvailable returns static NUL-terminated ids.
        unsafe {
            let n = (api.uloc_countAvailable)();
            for i in 0..n {
                let otherloc = (api.uloc_getAvailable)(i);
                let mut otherlang = [0 as c_char; ffi::ULOC_LANG_CAPACITY];
                let mut st = ffi::U_ZERO_ERROR;
                (api.uloc_getLanguage)(
                    otherloc,
                    otherlang.as_mut_ptr(),
                    ffi::ULOC_LANG_CAPACITY as i32,
                    &mut st,
                );
                if ffi::U_FAILURE(st) || st == ffi::U_STRING_NOT_TERMINATED_WARNING {
                    continue;
                }
                if otherlang[..lang_bytes.len()]
                    .iter()
                    .map(|&b| b as u8)
                    .eq(lang_bytes.iter().copied())
                    && otherlang[lang_bytes.len()] == 0
                {
                    found = true;
                    break;
                }
            }
        }
    }
    if !found {
        elog::ereport(elevel)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "ICU locale \"{loc_str}\" has unknown language \"{lang_str}\""
            ))
            .errhint(hint())
            .finish(crate::loc(1660, "icu_validate_locale"))?;
    }
    let collator = pg_ucol_open(loc_str)?;
    // SAFETY: closing the validation-probe collator, as C does.
    unsafe { (ffi::icu().ucol_close)(collator) };
    Ok(())
}

// regc_pg_locale.c PG_REGEX_STRATEGY_ICU arms: uchar.h ctype probes over the
// codepoint, locale-independent.
pub fn icu_wc_isclass(c: u32, class: crate::WcClass) -> bool {
    use crate::WcClass;
    let api = ffi::icu();
    let c = c as i32;
    // SAFETY: pure uchar.h ctype calls.
    (unsafe {
        match class {
            WcClass::Digit => (api.u_isdigit)(c),
            WcClass::Alpha => (api.u_isalpha)(c),
            WcClass::Alnum => (api.u_isalnum)(c),
            WcClass::Upper => (api.u_isupper)(c),
            WcClass::Lower => (api.u_islower)(c),
            WcClass::Graph => (api.u_isgraph)(c),
            WcClass::Print => (api.u_isprint)(c),
            WcClass::Punct => (api.u_ispunct)(c),
            WcClass::Space => (api.u_isspace)(c),
        }
    }) != 0
}

pub fn icu_wc_toupper(c: u32) -> u32 {
    // SAFETY: pure uchar.h case mapping.
    (unsafe { (ffi::icu().u_toupper)(c as i32) }) as u32
}

pub fn icu_wc_tolower(c: u32) -> u32 {
    // SAFETY: pure uchar.h case mapping.
    (unsafe { (ffi::icu().u_tolower)(c as i32) }) as u32
}
