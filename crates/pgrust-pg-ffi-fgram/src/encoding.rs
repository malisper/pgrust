pub type pg_enc = i32;

pub const PG_SQL_ASCII: pg_enc = 0;
pub const PG_EUC_JP: pg_enc = 1;
pub const PG_EUC_CN: pg_enc = 2;
pub const PG_EUC_KR: pg_enc = 3;
pub const PG_EUC_TW: pg_enc = 4;
pub const PG_EUC_JIS_2004: pg_enc = 5;
pub const PG_UTF8: pg_enc = 6;
pub const PG_MULE_INTERNAL: pg_enc = 7;
pub const PG_LATIN1: pg_enc = 8;
pub const PG_LATIN2: pg_enc = 9;
pub const PG_LATIN3: pg_enc = 10;
pub const PG_LATIN4: pg_enc = 11;
pub const PG_LATIN5: pg_enc = 12;
pub const PG_LATIN6: pg_enc = 13;
pub const PG_LATIN7: pg_enc = 14;
pub const PG_LATIN8: pg_enc = 15;
pub const PG_LATIN9: pg_enc = 16;
pub const PG_LATIN10: pg_enc = 17;
pub const PG_WIN1256: pg_enc = 18;
pub const PG_WIN1258: pg_enc = 19;
pub const PG_WIN866: pg_enc = 20;
pub const PG_WIN874: pg_enc = 21;
pub const PG_KOI8R: pg_enc = 22;
pub const PG_WIN1251: pg_enc = 23;
pub const PG_WIN1252: pg_enc = 24;
pub const PG_ISO_8859_5: pg_enc = 25;
pub const PG_ISO_8859_6: pg_enc = 26;
pub const PG_ISO_8859_7: pg_enc = 27;
pub const PG_ISO_8859_8: pg_enc = 28;
pub const PG_WIN1250: pg_enc = 29;
pub const PG_WIN1253: pg_enc = 30;
pub const PG_WIN1254: pg_enc = 31;
pub const PG_WIN1255: pg_enc = 32;
pub const PG_WIN1257: pg_enc = 33;
pub const PG_KOI8U: pg_enc = 34;
pub const PG_SJIS: pg_enc = 35;
pub const PG_BIG5: pg_enc = 36;
pub const PG_GBK: pg_enc = 37;
pub const PG_UHC: pg_enc = 38;
pub const PG_GB18030: pg_enc = 39;
pub const PG_JOHAB: pg_enc = 40;
pub const PG_SHIFT_JIS_2004: pg_enc = 41;
pub const _PG_LAST_ENCODING_: pg_enc = 42;
pub const PG_ENCODING_BE_LAST: pg_enc = PG_KOI8U;

pub const fn pg_valid_be_encoding(encoding: pg_enc) -> bool {
    encoding >= 0 && encoding <= PG_ENCODING_BE_LAST
}

pub const fn pg_valid_fe_encoding(encoding: pg_enc) -> bool {
    pg_valid_encoding(encoding)
}

pub const fn pg_valid_encoding(encoding: pg_enc) -> bool {
    encoding >= 0 && encoding < _PG_LAST_ENCODING_
}

/// `PG_ENCODING_IS_CLIENT_ONLY(_enc)` (mb/pg_wchar.h) — true if the encoding may
/// only be used client-side (i.e. it embeds ASCII in trailing bytes).
pub const fn PG_ENCODING_IS_CLIENT_ONLY(encoding: pg_enc) -> bool {
    encoding > PG_ENCODING_BE_LAST && encoding < _PG_LAST_ENCODING_
}
