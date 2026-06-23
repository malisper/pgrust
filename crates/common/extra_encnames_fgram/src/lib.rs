#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::ffi::{c_char, CStr};

pub use ::pg_ffi_fgram::{
    pg_enc, pg_valid_be_encoding, pg_valid_encoding, pg_valid_fe_encoding, PG_BIG5, PG_EUC_CN,
    PG_EUC_JIS_2004, PG_EUC_JP, PG_EUC_KR, PG_EUC_TW, PG_GB18030, PG_GBK, PG_ISO_8859_5,
    PG_ISO_8859_6, PG_ISO_8859_7, PG_ISO_8859_8, PG_JOHAB, PG_KOI8R, PG_KOI8U, PG_LATIN1,
    PG_LATIN10, PG_LATIN2, PG_LATIN3, PG_LATIN4, PG_LATIN5, PG_LATIN6, PG_LATIN7, PG_LATIN8,
    PG_LATIN9, PG_MULE_INTERNAL, PG_SHIFT_JIS_2004, PG_SJIS, PG_SQL_ASCII, PG_UHC, PG_UTF8,
    PG_WIN1250, PG_WIN1251, PG_WIN1252, PG_WIN1253, PG_WIN1254, PG_WIN1255, PG_WIN1256, PG_WIN1257,
    PG_WIN1258, PG_WIN866, PG_WIN874, _PG_LAST_ENCODING_,
};

/// Install the `common/encnames.c` seams (`pg_char_to_encoding` /
/// `pg_encoding_to_char`) from this owning unit. `pg_enc` is `i32`, so the seam
/// `i32` boundary forwards verbatim to the value cores.
pub fn init_seams() {
    encnames_seams::pg_char_to_encoding::set(pg_char_to_encoding);
    encnames_seams::pg_encoding_to_char::set(pg_encoding_to_char);
    // `is_encoding_supported_by_icu` (encnames.c: reads pg_enc2icu_tbl) is
    // declared in the mbutils seam slice but is encnames.c logic; this owning
    // unit installs it (the mbutils owner deliberately defers to encnames).
    // `pg_enc` is `i32`, matching the seam boundary verbatim.
    mbutils_seams::is_encoding_supported_by_icu::set(is_encoding_supported_by_icu);
    // collationcmds.c (CREATE COLLATION, ICU provider) re-declares the same
    // encnames.c probe in its own seam crate; this owning unit installs it too.
    collationcmds_seams::is_encoding_supported_by_icu::set(|encoding| {
        Ok(is_encoding_supported_by_icu(encoding))
    });
}

const NAMEDATALEN: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EncodingName {
    name: &'static str,
    encoding: pg_enc,
}

impl EncodingName {
    pub const fn new(name: &'static str, encoding: pg_enc) -> Self {
        Self { name, encoding }
    }

    pub const fn name(&self) -> &'static str {
        self.name
    }

    pub const fn encoding(&self) -> pg_enc {
        self.encoding
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct pg_enc2name {
    name: *const c_char,
    encoding: pg_enc,
}

unsafe impl Sync for pg_enc2name {}

impl pg_enc2name {
    pub const fn new(name: *const c_char, encoding: pg_enc) -> Self {
        Self { name, encoding }
    }

    pub fn name(&self) -> &'static str {
        let name = unsafe { CStr::from_ptr(self.name) };
        name.to_str()
            .expect("PostgreSQL encoding names are static ASCII")
    }

    pub const fn encoding(&self) -> pg_enc {
        self.encoding
    }
}

#[derive(Clone, Copy)]
struct pg_encname {
    name: &'static str,
    encoding: pg_enc,
}

const fn encname(name: &'static str, encoding: pg_enc) -> pg_encname {
    pg_encname { name, encoding }
}

const fn enc2name(name: &'static str, encoding: pg_enc) -> pg_enc2name {
    pg_enc2name::new(name.as_ptr().cast(), encoding)
}

const fn encoding_name(name: &'static str, encoding: pg_enc) -> EncodingName {
    EncodingName::new(name, encoding)
}

const PG_ENCNAME_TBL: &[pg_encname] = &[
    encname("abc", PG_WIN1258),
    encname("alt", PG_WIN866),
    encname("big5", PG_BIG5),
    encname("euccn", PG_EUC_CN),
    encname("eucjis2004", PG_EUC_JIS_2004),
    encname("eucjp", PG_EUC_JP),
    encname("euckr", PG_EUC_KR),
    encname("euctw", PG_EUC_TW),
    encname("gb18030", PG_GB18030),
    encname("gbk", PG_GBK),
    encname("iso88591", PG_LATIN1),
    encname("iso885910", PG_LATIN6),
    encname("iso885913", PG_LATIN7),
    encname("iso885914", PG_LATIN8),
    encname("iso885915", PG_LATIN9),
    encname("iso885916", PG_LATIN10),
    encname("iso88592", PG_LATIN2),
    encname("iso88593", PG_LATIN3),
    encname("iso88594", PG_LATIN4),
    encname("iso88595", PG_ISO_8859_5),
    encname("iso88596", PG_ISO_8859_6),
    encname("iso88597", PG_ISO_8859_7),
    encname("iso88598", PG_ISO_8859_8),
    encname("iso88599", PG_LATIN5),
    encname("johab", PG_JOHAB),
    encname("koi8", PG_KOI8R),
    encname("koi8r", PG_KOI8R),
    encname("koi8u", PG_KOI8U),
    encname("latin1", PG_LATIN1),
    encname("latin10", PG_LATIN10),
    encname("latin2", PG_LATIN2),
    encname("latin3", PG_LATIN3),
    encname("latin4", PG_LATIN4),
    encname("latin5", PG_LATIN5),
    encname("latin6", PG_LATIN6),
    encname("latin7", PG_LATIN7),
    encname("latin8", PG_LATIN8),
    encname("latin9", PG_LATIN9),
    encname("mskanji", PG_SJIS),
    encname("muleinternal", PG_MULE_INTERNAL),
    encname("shiftjis", PG_SJIS),
    encname("shiftjis2004", PG_SHIFT_JIS_2004),
    encname("sjis", PG_SJIS),
    encname("sqlascii", PG_SQL_ASCII),
    encname("tcvn", PG_WIN1258),
    encname("tcvn5712", PG_WIN1258),
    encname("uhc", PG_UHC),
    encname("unicode", PG_UTF8),
    encname("utf8", PG_UTF8),
    encname("vscii", PG_WIN1258),
    encname("win", PG_WIN1251),
    encname("win1250", PG_WIN1250),
    encname("win1251", PG_WIN1251),
    encname("win1252", PG_WIN1252),
    encname("win1253", PG_WIN1253),
    encname("win1254", PG_WIN1254),
    encname("win1255", PG_WIN1255),
    encname("win1256", PG_WIN1256),
    encname("win1257", PG_WIN1257),
    encname("win1258", PG_WIN1258),
    encname("win866", PG_WIN866),
    encname("win874", PG_WIN874),
    encname("win932", PG_SJIS),
    encname("win936", PG_GBK),
    encname("win949", PG_UHC),
    encname("win950", PG_BIG5),
    encname("windows1250", PG_WIN1250),
    encname("windows1251", PG_WIN1251),
    encname("windows1252", PG_WIN1252),
    encname("windows1253", PG_WIN1253),
    encname("windows1254", PG_WIN1254),
    encname("windows1255", PG_WIN1255),
    encname("windows1256", PG_WIN1256),
    encname("windows1257", PG_WIN1257),
    encname("windows1258", PG_WIN1258),
    encname("windows866", PG_WIN866),
    encname("windows874", PG_WIN874),
    encname("windows932", PG_SJIS),
    encname("windows936", PG_GBK),
    encname("windows949", PG_UHC),
    encname("windows950", PG_BIG5),
];

pub const pg_enc2name_tbl: &[pg_enc2name] = &[
    enc2name("SQL_ASCII\0", PG_SQL_ASCII),
    enc2name("EUC_JP\0", PG_EUC_JP),
    enc2name("EUC_CN\0", PG_EUC_CN),
    enc2name("EUC_KR\0", PG_EUC_KR),
    enc2name("EUC_TW\0", PG_EUC_TW),
    enc2name("EUC_JIS_2004\0", PG_EUC_JIS_2004),
    enc2name("UTF8\0", PG_UTF8),
    enc2name("MULE_INTERNAL\0", PG_MULE_INTERNAL),
    enc2name("LATIN1\0", PG_LATIN1),
    enc2name("LATIN2\0", PG_LATIN2),
    enc2name("LATIN3\0", PG_LATIN3),
    enc2name("LATIN4\0", PG_LATIN4),
    enc2name("LATIN5\0", PG_LATIN5),
    enc2name("LATIN6\0", PG_LATIN6),
    enc2name("LATIN7\0", PG_LATIN7),
    enc2name("LATIN8\0", PG_LATIN8),
    enc2name("LATIN9\0", PG_LATIN9),
    enc2name("LATIN10\0", PG_LATIN10),
    enc2name("WIN1256\0", PG_WIN1256),
    enc2name("WIN1258\0", PG_WIN1258),
    enc2name("WIN866\0", PG_WIN866),
    enc2name("WIN874\0", PG_WIN874),
    enc2name("KOI8R\0", PG_KOI8R),
    enc2name("WIN1251\0", PG_WIN1251),
    enc2name("WIN1252\0", PG_WIN1252),
    enc2name("ISO_8859_5\0", PG_ISO_8859_5),
    enc2name("ISO_8859_6\0", PG_ISO_8859_6),
    enc2name("ISO_8859_7\0", PG_ISO_8859_7),
    enc2name("ISO_8859_8\0", PG_ISO_8859_8),
    enc2name("WIN1250\0", PG_WIN1250),
    enc2name("WIN1253\0", PG_WIN1253),
    enc2name("WIN1254\0", PG_WIN1254),
    enc2name("WIN1255\0", PG_WIN1255),
    enc2name("WIN1257\0", PG_WIN1257),
    enc2name("KOI8U\0", PG_KOI8U),
    enc2name("SJIS\0", PG_SJIS),
    enc2name("BIG5\0", PG_BIG5),
    enc2name("GBK\0", PG_GBK),
    enc2name("UHC\0", PG_UHC),
    enc2name("GB18030\0", PG_GB18030),
    enc2name("JOHAB\0", PG_JOHAB),
    enc2name("SHIFT_JIS_2004\0", PG_SHIFT_JIS_2004),
];

pub const PG_ENCODING_NAMES: &[EncodingName] = &[
    encoding_name("SQL_ASCII", PG_SQL_ASCII),
    encoding_name("EUC_JP", PG_EUC_JP),
    encoding_name("EUC_CN", PG_EUC_CN),
    encoding_name("EUC_KR", PG_EUC_KR),
    encoding_name("EUC_TW", PG_EUC_TW),
    encoding_name("EUC_JIS_2004", PG_EUC_JIS_2004),
    encoding_name("UTF8", PG_UTF8),
    encoding_name("MULE_INTERNAL", PG_MULE_INTERNAL),
    encoding_name("LATIN1", PG_LATIN1),
    encoding_name("LATIN2", PG_LATIN2),
    encoding_name("LATIN3", PG_LATIN3),
    encoding_name("LATIN4", PG_LATIN4),
    encoding_name("LATIN5", PG_LATIN5),
    encoding_name("LATIN6", PG_LATIN6),
    encoding_name("LATIN7", PG_LATIN7),
    encoding_name("LATIN8", PG_LATIN8),
    encoding_name("LATIN9", PG_LATIN9),
    encoding_name("LATIN10", PG_LATIN10),
    encoding_name("WIN1256", PG_WIN1256),
    encoding_name("WIN1258", PG_WIN1258),
    encoding_name("WIN866", PG_WIN866),
    encoding_name("WIN874", PG_WIN874),
    encoding_name("KOI8R", PG_KOI8R),
    encoding_name("WIN1251", PG_WIN1251),
    encoding_name("WIN1252", PG_WIN1252),
    encoding_name("ISO_8859_5", PG_ISO_8859_5),
    encoding_name("ISO_8859_6", PG_ISO_8859_6),
    encoding_name("ISO_8859_7", PG_ISO_8859_7),
    encoding_name("ISO_8859_8", PG_ISO_8859_8),
    encoding_name("WIN1250", PG_WIN1250),
    encoding_name("WIN1253", PG_WIN1253),
    encoding_name("WIN1254", PG_WIN1254),
    encoding_name("WIN1255", PG_WIN1255),
    encoding_name("WIN1257", PG_WIN1257),
    encoding_name("KOI8U", PG_KOI8U),
    encoding_name("SJIS", PG_SJIS),
    encoding_name("BIG5", PG_BIG5),
    encoding_name("GBK", PG_GBK),
    encoding_name("UHC", PG_UHC),
    encoding_name("GB18030", PG_GB18030),
    encoding_name("JOHAB", PG_JOHAB),
    encoding_name("SHIFT_JIS_2004", PG_SHIFT_JIS_2004),
];

pub const pg_enc2gettext_tbl: &[Option<&str>] = &[
    Some("US-ASCII"),
    Some("EUC-JP"),
    Some("EUC-CN"),
    Some("EUC-KR"),
    Some("EUC-TW"),
    Some("EUC-JP"),
    Some("UTF-8"),
    None,
    Some("LATIN1"),
    Some("LATIN2"),
    Some("LATIN3"),
    Some("LATIN4"),
    Some("LATIN5"),
    Some("LATIN6"),
    Some("LATIN7"),
    Some("LATIN8"),
    Some("LATIN-9"),
    Some("LATIN10"),
    Some("CP1256"),
    Some("CP1258"),
    Some("CP866"),
    Some("CP874"),
    Some("KOI8-R"),
    Some("CP1251"),
    Some("CP1252"),
    Some("ISO-8859-5"),
    Some("ISO_8859-6"),
    Some("ISO-8859-7"),
    Some("ISO-8859-8"),
    Some("CP1250"),
    Some("CP1253"),
    Some("CP1254"),
    Some("CP1255"),
    Some("CP1257"),
    Some("KOI8-U"),
    Some("SHIFT-JIS"),
    Some("BIG5"),
    Some("GBK"),
    Some("UHC"),
    Some("GB18030"),
    Some("JOHAB"),
    Some("SHIFT_JISX0213"),
];

const PG_ENC2ICU_TBL: &[Option<&str>] = &[
    None,
    Some("EUC-JP"),
    Some("EUC-CN"),
    Some("EUC-KR"),
    Some("EUC-TW"),
    None,
    Some("UTF-8"),
    None,
    Some("ISO-8859-1"),
    Some("ISO-8859-2"),
    Some("ISO-8859-3"),
    Some("ISO-8859-4"),
    Some("ISO-8859-9"),
    Some("ISO-8859-10"),
    Some("ISO-8859-13"),
    Some("ISO-8859-14"),
    Some("ISO-8859-15"),
    None,
    Some("CP1256"),
    Some("CP1258"),
    Some("CP866"),
    None,
    Some("KOI8-R"),
    Some("CP1251"),
    Some("CP1252"),
    Some("ISO-8859-5"),
    Some("ISO-8859-6"),
    Some("ISO-8859-7"),
    Some("ISO-8859-8"),
    Some("CP1250"),
    Some("CP1253"),
    Some("CP1254"),
    Some("CP1255"),
    Some("CP1257"),
    Some("KOI8-U"),
];

pub fn is_encoding_supported_by_icu(encoding: pg_enc) -> bool {
    get_encoding_name_for_icu(encoding).is_some()
}

pub fn get_encoding_name_for_icu(encoding: pg_enc) -> Option<&'static str> {
    if !pg_valid_be_encoding(encoding) {
        return None;
    }
    PG_ENC2ICU_TBL[encoding as usize]
}

pub fn pg_valid_client_encoding(name: &str) -> pg_enc {
    let enc = pg_char_to_encoding(name);
    if pg_valid_fe_encoding(enc) {
        enc
    } else {
        -1
    }
}

pub fn pg_valid_server_encoding(name: &str) -> pg_enc {
    let enc = pg_char_to_encoding(name);
    if pg_valid_be_encoding(enc) {
        enc
    } else {
        -1
    }
}

pub fn pg_valid_server_encoding_id(encoding: pg_enc) -> bool {
    pg_valid_be_encoding(encoding)
}

pub fn pg_char_to_encoding(name: &str) -> pg_enc {
    let name = name.split('\0').next().unwrap_or("");
    if name.is_empty() || name.len() >= NAMEDATALEN {
        return -1;
    }

    let key = clean_encoding_name(name.as_bytes());
    if key.is_empty() {
        return -1;
    }

    PG_ENCNAME_TBL
        .binary_search_by(|candidate| candidate.name.as_bytes().cmp(key.as_slice()))
        .map(|index| PG_ENCNAME_TBL[index].encoding)
        .unwrap_or(-1)
}

pub fn pg_encoding_to_char(encoding: pg_enc) -> &'static str {
    if pg_valid_encoding(encoding) {
        let enc2name = &PG_ENCODING_NAMES[encoding as usize];
        debug_assert_eq!(encoding, enc2name.encoding);
        enc2name.name
    } else {
        ""
    }
}

fn clean_encoding_name(name: &[u8]) -> Vec<u8> {
    name.iter()
        .copied()
        .filter(u8::is_ascii_alphanumeric)
        .map(|byte| byte.to_ascii_lowercase())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_aliases_and_ignores_punctuation() {
        assert_eq!(pg_char_to_encoding("UTF8"), PG_UTF8);
        assert_eq!(pg_char_to_encoding("unicode"), PG_UTF8);
        assert_eq!(pg_char_to_encoding("ISO-8859-1"), PG_LATIN1);
        assert_eq!(pg_char_to_encoding("iso_8859.1"), PG_LATIN1);
        assert_eq!(pg_char_to_encoding("windows-1251"), PG_WIN1251);
        assert_eq!(pg_char_to_encoding("ms-kanji"), PG_SJIS);
    }

    #[test]
    fn rejects_empty_too_long_and_unknown_names() {
        assert_eq!(pg_char_to_encoding(""), -1);
        assert_eq!(pg_char_to_encoding("not-an-encoding"), -1);
        assert_eq!(pg_char_to_encoding(&"a".repeat(NAMEDATALEN)), -1);
    }

    #[test]
    fn validates_frontend_and_backend_ranges() {
        assert_eq!(
            pg_valid_client_encoding("SHIFT_JIS_2004"),
            PG_SHIFT_JIS_2004
        );
        assert_eq!(pg_valid_server_encoding("SHIFT_JIS_2004"), -1);
        assert!(pg_valid_server_encoding_id(PG_UTF8));
        assert!(!pg_valid_server_encoding_id(PG_SHIFT_JIS_2004));
    }

    #[test]
    fn maps_encoding_ids_to_official_names() {
        assert_eq!(pg_encoding_to_char(PG_SQL_ASCII), "SQL_ASCII");
        assert_eq!(pg_encoding_to_char(PG_UTF8), "UTF8");
        assert_eq!(pg_encoding_to_char(PG_SHIFT_JIS_2004), "SHIFT_JIS_2004");
        assert_eq!(pg_encoding_to_char(-1), "");
        assert_eq!(pg_encoding_to_char(_PG_LAST_ENCODING_), "");
    }

    #[test]
    fn reports_gettext_and_icu_names() {
        assert_eq!(pg_enc2gettext_tbl[PG_UTF8 as usize], Some("UTF-8"));
        assert_eq!(pg_enc2gettext_tbl[PG_MULE_INTERNAL as usize], None);
        assert_eq!(get_encoding_name_for_icu(PG_UTF8), Some("UTF-8"));
        assert_eq!(get_encoding_name_for_icu(PG_SQL_ASCII), None);
        assert_eq!(get_encoding_name_for_icu(PG_SJIS), None);
        assert!(is_encoding_supported_by_icu(PG_LATIN1));
        assert!(!is_encoding_supported_by_icu(PG_WIN874));
    }

    #[test]
    fn tables_have_postgres_lengths() {
        assert_eq!(PG_ENCNAME_TBL.len(), 81);
        assert_eq!(pg_enc2name_tbl.len(), _PG_LAST_ENCODING_ as usize);
        assert_eq!(pg_enc2gettext_tbl.len(), _PG_LAST_ENCODING_ as usize);
        assert_eq!(
            PG_ENC2ICU_TBL.len(),
            (::pg_ffi_fgram::PG_ENCODING_BE_LAST + 1) as usize
        );
    }
}
