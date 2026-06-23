#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

use core::ffi::c_int;
use std::sync::atomic::{AtomicI32, Ordering};

use error_fgram::{
    elog, ereport, errmsg_internal, PgError, PgResult, ERRCODE_CHARACTER_NOT_IN_REPERTOIRE,
    ERRCODE_UNTRANSLATABLE_CHARACTER, ERROR,
};
use extra_encnames_fgram::pg_encoding_to_char;
use wchar_fgram::{
    pg_encoding_dsplen as wchar_encoding_dsplen, pg_encoding_max_length,
    pg_encoding_mblen as wchar_encoding_mblen, pg_encoding_mblen_or_incomplete,
    pg_encoding_verifymbchar, pg_encoding_verifymbstr, pg_wchar_table,
};
use pg_ffi_fgram::{
    pg_enc, pg_valid_be_encoding, pg_valid_encoding, pg_wchar, PG_EUC_JP, PG_SQL_ASCII, PG_UTF8,
};

const HIGHBIT: u8 = 0x80;
const SS2: u8 = 0x8e;
const SS3: u8 = 0x8f;

static CLIENT_ENCODING: AtomicI32 = AtomicI32::new(PG_SQL_ASCII);
static DATABASE_ENCODING: AtomicI32 = AtomicI32::new(PG_SQL_ASCII);
static MESSAGE_ENCODING: AtomicI32 = AtomicI32::new(PG_SQL_ASCII);

pub type mbcharacter_incrementer = fn(&mut [u8]) -> bool;

pub fn pg_get_client_encoding() -> pg_enc {
    CLIENT_ENCODING.load(Ordering::Relaxed)
}

pub fn pg_get_client_encoding_name() -> &'static str {
    encoding_name(pg_get_client_encoding())
}

pub fn GetDatabaseEncoding() -> pg_enc {
    DATABASE_ENCODING.load(Ordering::Relaxed)
}

pub fn GetDatabaseEncodingName() -> &'static str {
    encoding_name(GetDatabaseEncoding())
}

pub fn SetDatabaseEncoding(encoding: pg_enc) -> PgResult<()> {
    if !pg_valid_be_encoding(encoding) {
        return elog(ERROR, format!("invalid database encoding: {encoding}"));
    }

    DATABASE_ENCODING.store(encoding, Ordering::Relaxed);
    Ok(())
}

pub fn GetMessageEncoding() -> pg_enc {
    MESSAGE_ENCODING.load(Ordering::Relaxed)
}

pub fn SetMessageEncoding(encoding: pg_enc) -> PgResult<()> {
    if !pg_valid_encoding(encoding) {
        return Err(errmsg_internal(format!(
            "invalid message encoding: {encoding}"
        )));
    }

    MESSAGE_ENCODING.store(encoding, Ordering::Relaxed);
    Ok(())
}

pub fn pg_database_encoding_max_length() -> c_int {
    pg_encoding_max_length(GetDatabaseEncoding())
}

pub fn pg_mb2wchar(from: &[u8]) -> PgResult<Vec<pg_wchar>> {
    pg_mb2wchar_with_len(from, c_string_len(from) as c_int)
}

pub fn pg_mb2wchar_with_len(from: &[u8], len: c_int) -> PgResult<Vec<pg_wchar>> {
    pg_encoding_mb2wchar_with_len(GetDatabaseEncoding(), from, len)
}

pub fn pg_encoding_mb2wchar_with_len(
    encoding: pg_enc,
    from: &[u8],
    len: c_int,
) -> PgResult<Vec<pg_wchar>> {
    let input = bounded_prefix(from, len);
    let table = wchar_table_for_encoding(encoding)?;
    let convert = table
        .mb2wchar_with_len
        .ok_or_else(|| unsupported_encoding_function("mb2wchar", encoding))?;
    let mut output = vec![0; input.len() + 1];
    let count = unsafe { convert(input.as_ptr(), output.as_mut_ptr(), input.len() as c_int) };
    let keep = (count.max(0) as usize + 1).min(output.len());
    output.truncate(keep);
    Ok(output)
}

pub fn pg_wchar2mb(from: &[pg_wchar]) -> PgResult<Vec<u8>> {
    pg_wchar2mb_with_len(from, pg_wchar_strlen(from) as c_int)
}

pub fn pg_wchar2mb_with_len(from: &[pg_wchar], len: c_int) -> PgResult<Vec<u8>> {
    pg_encoding_wchar2mb_with_len(GetDatabaseEncoding(), from, len)
}

pub fn pg_encoding_wchar2mb_with_len(
    encoding: pg_enc,
    from: &[pg_wchar],
    len: c_int,
) -> PgResult<Vec<u8>> {
    let input = bounded_wchar_prefix(from, len);
    let table = wchar_table_for_encoding(encoding)?;
    let convert = table
        .wchar2mb_with_len
        .ok_or_else(|| unsupported_encoding_function("wchar2mb", encoding))?;
    let max_len = pg_encoding_max_length(encoding).max(1) as usize;
    let mut output = vec![0; input.len() * max_len + 1];
    let count = unsafe { convert(input.as_ptr(), output.as_mut_ptr(), input.len() as c_int) };
    let keep = (count.max(0) as usize + 1).min(output.len());
    output.truncate(keep);
    Ok(output)
}

pub fn pg_mblen_cstr(mbstr: &[u8]) -> PgResult<c_int> {
    let encoding = GetDatabaseEncoding();
    let length = mblen_for_encoding(encoding, mbstr)?;

    for idx in 1..length as usize {
        if idx >= mbstr.len() || mbstr[idx] == 0 {
            return report_invalid_encoding_db(mbstr, length, idx as c_int);
        }
    }

    Ok(length)
}

pub fn pg_mblen_range(mbstr: &[u8]) -> PgResult<c_int> {
    let encoding = GetDatabaseEncoding();
    let length = mblen_for_encoding(encoding, mbstr)?;
    if length as usize > mbstr.len() {
        return report_invalid_encoding_db(mbstr, length, mbstr.len() as c_int);
    }
    Ok(length)
}

pub fn pg_mblen_with_len(mbstr: &[u8], limit: c_int) -> PgResult<c_int> {
    assert!(limit >= 1);

    let encoding = GetDatabaseEncoding();
    let length = mblen_for_encoding(encoding, mbstr)?;
    if length > limit {
        return report_invalid_encoding_db(mbstr, length, limit);
    }
    Ok(length)
}

pub fn pg_mblen_unbounded(mbstr: &[u8]) -> PgResult<c_int> {
    mblen_for_encoding(GetDatabaseEncoding(), mbstr)
}

pub fn pg_mblen(mbstr: &[u8]) -> PgResult<c_int> {
    pg_mblen_unbounded(mbstr)
}

pub fn pg_dsplen(mbstr: &[u8]) -> PgResult<c_int> {
    let encoding = GetDatabaseEncoding();
    if !pg_valid_encoding(encoding) {
        return Err(invalid_encoding_error("encoding", encoding));
    }
    wchar_encoding_dsplen(encoding, mbstr)
        .ok_or_else(|| invalid_encoding_error("encoding", encoding))
}

pub fn pg_mbstrlen(mbstr: &[u8]) -> PgResult<c_int> {
    if pg_database_encoding_max_length() == 1 {
        return Ok(c_string_len(mbstr) as c_int);
    }

    let mut len = 0;
    let mut pos = 0;
    while pos < mbstr.len() && mbstr[pos] != 0 {
        let char_len = pg_mblen_cstr(&mbstr[pos..])? as usize;
        pos += char_len;
        len += 1;
    }
    Ok(len)
}

pub fn pg_mbstrlen_with_len(mbstr: &[u8], limit: c_int) -> PgResult<c_int> {
    if limit <= 0 {
        return Ok(0);
    }

    if pg_database_encoding_max_length() == 1 {
        return Ok(limit);
    }

    let mut remaining = (limit as usize).min(mbstr.len());
    let mut pos = 0;
    let mut len = 0;
    while remaining > 0 && pos < mbstr.len() && mbstr[pos] != 0 {
        let char_len = pg_mblen_with_len(&mbstr[pos..], remaining as c_int)? as usize;
        remaining -= char_len;
        pos += char_len;
        len += 1;
    }
    Ok(len)
}

pub fn pg_mbcliplen(mbstr: &[u8], len: c_int, limit: c_int) -> PgResult<c_int> {
    pg_encoding_mbcliplen(GetDatabaseEncoding(), mbstr, len, limit)
}

pub fn pg_encoding_mbcliplen(
    encoding: pg_enc,
    mbstr: &[u8],
    len: c_int,
    limit: c_int,
) -> PgResult<c_int> {
    if !pg_valid_encoding(encoding) {
        return Err(invalid_encoding_error("encoding", encoding));
    }

    if pg_encoding_max_length(encoding) == 1 {
        return Ok(cliplen(mbstr, len, limit));
    }

    let mut remaining = bounded_len(len, mbstr.len());
    let mut pos = 0;
    let mut clipped = 0;
    while remaining > 0 && pos < mbstr.len() && mbstr[pos] != 0 {
        let char_len = mblen_for_encoding(encoding, &mbstr[pos..])?;
        if char_len as usize > remaining {
            break;
        }
        if clipped + char_len > limit {
            break;
        }
        clipped += char_len;
        if clipped == limit {
            break;
        }
        let char_len = char_len as usize;
        remaining = remaining.saturating_sub(char_len);
        pos += char_len;
    }
    Ok(clipped)
}

pub fn pg_mbcharcliplen(mbstr: &[u8], len: c_int, limit: c_int) -> PgResult<c_int> {
    if limit <= 0 {
        return Ok(0);
    }

    if pg_database_encoding_max_length() == 1 {
        return Ok(cliplen(mbstr, len, limit));
    }

    let mut remaining = bounded_len(len, mbstr.len());
    let mut pos = 0;
    let mut clipped = 0;
    let mut nchars = 0;
    while remaining > 0 && pos < mbstr.len() && mbstr[pos] != 0 {
        let char_len = pg_mblen_with_len(&mbstr[pos..], remaining as c_int)?;
        nchars += 1;
        if nchars > limit {
            break;
        }
        clipped += char_len;
        let char_len = char_len as usize;
        remaining = remaining.saturating_sub(char_len);
        pos += char_len;
    }
    Ok(clipped)
}

pub fn pg_verifymbstr(mbstr: &[u8], noError: bool) -> PgResult<bool> {
    pg_verify_mbstr(GetDatabaseEncoding(), mbstr, noError)
}

pub fn pg_verify_mbstr(encoding: pg_enc, mbstr: &[u8], noError: bool) -> PgResult<bool> {
    if !pg_valid_encoding(encoding) {
        return Err(invalid_encoding_error("encoding", encoding));
    }

    let oklen = pg_encoding_verifymbstr(encoding, mbstr);
    if oklen != mbstr.len() as c_int {
        if noError {
            return Ok(false);
        }
        report_invalid_encoding(encoding, &mbstr[oklen.max(0) as usize..])?;
    }
    Ok(true)
}

pub fn pg_verify_mbstr_len(
    encoding: pg_enc,
    mbstr: &[u8],
    len: c_int,
    noError: bool,
) -> PgResult<c_int> {
    if !pg_valid_encoding(encoding) {
        return Err(invalid_encoding_error("encoding", encoding));
    }

    let mut remaining = bounded_len(len, mbstr.len());
    let mut pos = 0;

    if pg_encoding_max_length(encoding) <= 1 {
        if let Some(nullpos) = mbstr[..remaining].iter().position(|byte| *byte == 0) {
            if noError {
                return Ok(-1);
            }
            report_invalid_encoding(encoding, &mbstr[nullpos..nullpos + 1])?;
        }
        return Ok(remaining as c_int);
    }

    let mut mb_len = 0;
    while remaining > 0 {
        if mbstr[pos] & HIGHBIT == 0 {
            if mbstr[pos] != 0 {
                mb_len += 1;
                pos += 1;
                remaining -= 1;
                continue;
            }
            if noError {
                return Ok(-1);
            }
            report_invalid_encoding(encoding, &mbstr[pos..pos + remaining])?;
        }

        let char_len = pg_encoding_verifymbchar(encoding, &mbstr[pos..pos + remaining]);
        if char_len <= 0 {
            if noError {
                return Ok(-1);
            }
            report_invalid_encoding(encoding, &mbstr[pos..pos + remaining])?;
        }

        let char_len = char_len as usize;
        pos += char_len;
        remaining -= char_len;
        mb_len += 1;
    }
    Ok(mb_len)
}

pub fn check_encoding_conversion_args(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    len: c_int,
    expected_src_encoding: pg_enc,
    expected_dest_encoding: pg_enc,
) -> PgResult<()> {
    if !pg_valid_encoding(src_encoding) {
        return elog(ERROR, format!("invalid source encoding ID: {src_encoding}"));
    }
    if src_encoding != expected_src_encoding && expected_src_encoding >= 0 {
        return elog(
            ERROR,
            format!(
                "expected source encoding \"{}\", but got \"{}\"",
                encoding_name(expected_src_encoding),
                encoding_name(src_encoding)
            ),
        );
    }
    if !pg_valid_encoding(dest_encoding) {
        return elog(
            ERROR,
            format!("invalid destination encoding ID: {dest_encoding}"),
        );
    }
    if dest_encoding != expected_dest_encoding && expected_dest_encoding >= 0 {
        return elog(
            ERROR,
            format!(
                "expected destination encoding \"{}\", but got \"{}\"",
                encoding_name(expected_dest_encoding),
                encoding_name(dest_encoding)
            ),
        );
    }
    if len < 0 {
        return elog(ERROR, "encoding conversion length must not be negative");
    }
    Ok(())
}

pub fn report_invalid_encoding(encoding: pg_enc, mbstr: &[u8]) -> PgResult<()> {
    let mblen = pg_encoding_mblen_or_incomplete(encoding, mbstr);
    report_invalid_encoding_int(encoding, mbstr, mblen, mbstr.len() as c_int)
}

pub fn report_untranslatable_char(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    mbstr: &[u8],
) -> PgResult<()> {
    let mblen = pg_encoding_mblen_or_incomplete(src_encoding, mbstr);
    let bytes = byte_sequence(mbstr, mblen, mbstr.len() as c_int);
    Err(ereport(ERROR)
        .errcode(ERRCODE_UNTRANSLATABLE_CHARACTER)
        .errmsg(format!(
            "character with byte sequence {bytes} in encoding \"{}\" has no equivalent in encoding \"{}\"",
            encoding_name(src_encoding),
            encoding_name(dest_encoding)
        ))
        .into_error())
}

pub fn pg_database_encoding_character_incrementer() -> mbcharacter_incrementer {
    match GetDatabaseEncoding() {
        PG_UTF8 => pg_utf8_increment,
        PG_EUC_JP => pg_eucjp_increment,
        _ => pg_generic_charinc,
    }
}

pub fn pg_generic_charinc(charptr: &mut [u8]) -> bool {
    let Some(lastbyte) = charptr.len().checked_sub(1) else {
        return false;
    };
    let encoding = GetDatabaseEncoding();

    while charptr[lastbyte] < 255 {
        charptr[lastbyte] += 1;
        if pg_encoding_verifymbchar(encoding, charptr) == charptr.len() as c_int {
            return true;
        }
    }
    false
}

pub fn pg_utf8_increment(charptr: &mut [u8]) -> bool {
    match charptr.len() {
        4 => {
            if charptr[3] < 0xbf {
                charptr[3] += 1;
                return true;
            }
        }
        3 => {}
        2 => {}
        1 => {}
        _ => return false,
    }

    if charptr.len() >= 3 && charptr[2] < 0xbf {
        charptr[2] += 1;
        return true;
    }

    if charptr.len() >= 2 {
        let limit = match charptr[0] {
            0xed => 0x9f,
            0xf4 => 0x8f,
            _ => 0xbf,
        };
        if charptr[1] < limit {
            charptr[1] += 1;
            return true;
        }
    }

    if charptr.is_empty() || matches!(charptr[0], 0x7f | 0xdf | 0xef | 0xf4) {
        return false;
    }
    charptr[0] += 1;
    true
}

pub fn pg_eucjp_increment(charptr: &mut [u8]) -> bool {
    let Some(&c1) = charptr.first() else {
        return false;
    };

    match c1 {
        SS2 => {
            if charptr.len() != 2 {
                return false;
            }
            let c2 = charptr[1];
            if c2 >= 0xdf {
                charptr[0] = 0xa1;
                charptr[1] = 0xa1;
            } else if c2 < 0xa1 {
                charptr[1] = 0xa1;
            } else {
                charptr[1] += 1;
            }
        }
        SS3 => {
            if charptr.len() != 3 {
                return false;
            }
            for idx in (1..=2).rev() {
                let c2 = charptr[idx];
                if c2 < 0xa1 {
                    charptr[idx] = 0xa1;
                    return true;
                }
                if c2 < 0xfe {
                    charptr[idx] += 1;
                    return true;
                }
            }
            return false;
        }
        value if value & HIGHBIT != 0 => {
            if charptr.len() != 2 {
                return false;
            }
            for idx in (0..=1).rev() {
                let c2 = charptr[idx];
                if c2 < 0xa1 {
                    charptr[idx] = 0xa1;
                    return true;
                }
                if c2 < 0xfe {
                    charptr[idx] += 1;
                    return true;
                }
            }
            return false;
        }
        value => {
            if value > 0x7e {
                return false;
            }
            charptr[0] += 1;
        }
    }
    true
}

fn report_invalid_encoding_int<T>(
    encoding: pg_enc,
    mbstr: &[u8],
    mblen: c_int,
    len: c_int,
) -> PgResult<T> {
    let bytes = byte_sequence(mbstr, mblen, len);
    Err(ereport(ERROR)
        .errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE)
        .errmsg(format!(
            "invalid byte sequence for encoding \"{}\": {bytes}",
            encoding_name(encoding)
        ))
        .into_error())
}

fn report_invalid_encoding_db<T>(mbstr: &[u8], mblen: c_int, len: c_int) -> PgResult<T> {
    report_invalid_encoding_int(GetDatabaseEncoding(), mbstr, mblen, len)
}

fn mblen_for_encoding(encoding: pg_enc, mbstr: &[u8]) -> PgResult<c_int> {
    if !pg_valid_encoding(encoding) {
        return Err(invalid_encoding_error("encoding", encoding));
    }
    wchar_encoding_mblen(encoding, mbstr)
        .ok_or_else(|| invalid_encoding_error("encoding", encoding))
}

fn wchar_table_for_encoding(encoding: pg_enc) -> PgResult<&'static pg_ffi_fgram::pg_wchar_tbl> {
    if !pg_valid_encoding(encoding) {
        return Err(invalid_encoding_error("encoding", encoding));
    }
    Ok(&pg_wchar_table[encoding as usize])
}

fn unsupported_encoding_function(function: &str, encoding: pg_enc) -> PgError {
    errmsg_internal(format!(
        "{function} is not supported for encoding \"{}\"",
        encoding_name(encoding)
    ))
}

fn invalid_encoding_error(label: &str, encoding: pg_enc) -> PgError {
    errmsg_internal(format!("invalid {label} ID: {encoding}"))
}

fn encoding_name(encoding: pg_enc) -> &'static str {
    if pg_valid_encoding(encoding) {
        pg_encoding_to_char(encoding)
    } else {
        ""
    }
}

fn byte_sequence(mbstr: &[u8], mblen: c_int, len: c_int) -> String {
    let limit = (mblen.max(0) as usize).min(len.max(0) as usize).min(8);
    mbstr
        .iter()
        .take(limit)
        .map(|byte| format!("0x{byte:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn bounded_len(len: c_int, max: usize) -> usize {
    (len.max(0) as usize).min(max)
}

fn bounded_prefix(bytes: &[u8], len: c_int) -> &[u8] {
    &bytes[..bounded_len(len, bytes.len())]
}

fn bounded_wchar_prefix(chars: &[pg_wchar], len: c_int) -> &[pg_wchar] {
    &chars[..bounded_len(len, chars.len())]
}

fn c_string_len(bytes: &[u8]) -> usize {
    bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len())
}

fn pg_wchar_strlen(chars: &[pg_wchar]) -> usize {
    chars
        .iter()
        .position(|wchar| *wchar == 0)
        .unwrap_or(chars.len())
}

fn cliplen(str: &[u8], len: c_int, limit: c_int) -> c_int {
    let len = bounded_len(len.min(limit), str.len());
    str.iter()
        .take(len)
        .position(|byte| *byte == 0)
        .unwrap_or(len) as c_int
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use pg_ffi_fgram::{PG_EUC_CN, PG_LATIN1};

    static TEST_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn tracks_database_and_message_encoding() {
        let _guard = TEST_LOCK.lock().unwrap();
        SetDatabaseEncoding(PG_UTF8).unwrap();
        SetMessageEncoding(PG_LATIN1).unwrap();

        assert_eq!(GetDatabaseEncoding(), PG_UTF8);
        assert_eq!(GetDatabaseEncodingName(), "UTF8");
        assert_eq!(GetMessageEncoding(), PG_LATIN1);

        let error = SetDatabaseEncoding(-1).unwrap_err();
        assert_eq!(error.message(), "invalid database encoding: -1");
    }

    #[test]
    fn computes_multibyte_lengths_and_clip_points() {
        let _guard = TEST_LOCK.lock().unwrap();
        SetDatabaseEncoding(PG_UTF8).unwrap();

        let text = "aéz".as_bytes();
        assert_eq!(pg_mblen_with_len("é".as_bytes(), 2).unwrap(), 2);
        assert_eq!(pg_mbstrlen(text).unwrap(), 3);
        assert_eq!(pg_mbstrlen_with_len(text, text.len() as c_int).unwrap(), 3);
        assert_eq!(pg_mbcliplen(text, text.len() as c_int, 2).unwrap(), 1);
        assert_eq!(pg_mbcliplen(text, text.len() as c_int, 3).unwrap(), 3);
        assert_eq!(pg_mbcharcliplen(text, text.len() as c_int, 2).unwrap(), 3);
    }

    #[test]
    fn reports_invalid_encoding_with_postgres_message_shape() {
        let _guard = TEST_LOCK.lock().unwrap();
        SetDatabaseEncoding(PG_UTF8).unwrap();

        let error = pg_mblen_with_len(&[0xc3], 1).unwrap_err();
        assert_eq!(error.sqlstate(), ERRCODE_CHARACTER_NOT_IN_REPERTOIRE);
        assert_eq!(
            error.message(),
            "invalid byte sequence for encoding \"UTF8\": 0xc3"
        );

        assert!(!pg_verify_mbstr(PG_UTF8, &[0xc0, b' '], true).unwrap());
        let error = pg_verify_mbstr(PG_UTF8, &[0xc0, b' '], false).unwrap_err();
        assert_eq!(error.sqlstate(), ERRCODE_CHARACTER_NOT_IN_REPERTOIRE);
    }

    #[test]
    fn converts_between_multibyte_and_wchar_with_terminators() {
        let _guard = TEST_LOCK.lock().unwrap();
        SetDatabaseEncoding(PG_UTF8).unwrap();

        let wide = pg_mb2wchar("az".as_bytes()).unwrap();
        assert_eq!(wide, vec![b'a' as pg_wchar, b'z' as pg_wchar, 0]);

        let bytes = pg_wchar2mb(&wide).unwrap();
        assert_eq!(bytes, b"az\0");
    }

    #[test]
    fn validates_conversion_arguments() {
        let _guard = TEST_LOCK.lock().unwrap();

        assert!(check_encoding_conversion_args(PG_UTF8, PG_LATIN1, 3, PG_UTF8, -1).is_ok());
        let error =
            check_encoding_conversion_args(PG_EUC_CN, PG_LATIN1, 3, PG_UTF8, -1).unwrap_err();
        assert_eq!(
            error.message(),
            "expected source encoding \"UTF8\", but got \"EUC_CN\""
        );

        let error =
            check_encoding_conversion_args(PG_UTF8, PG_LATIN1, -1, PG_UTF8, -1).unwrap_err();
        assert_eq!(
            error.message(),
            "encoding conversion length must not be negative"
        );
    }

    #[test]
    fn reports_untranslatable_characters() {
        let _guard = TEST_LOCK.lock().unwrap();

        let error = report_untranslatable_char(PG_UTF8, PG_LATIN1, "€".as_bytes()).unwrap_err();
        assert_eq!(error.sqlstate(), ERRCODE_UNTRANSLATABLE_CHARACTER);
        assert_eq!(
            error.message(),
            "character with byte sequence 0xe2 0x82 0xac in encoding \"UTF8\" has no equivalent in encoding \"LATIN1\""
        );
    }

    #[test]
    fn chooses_database_character_incrementer() {
        let _guard = TEST_LOCK.lock().unwrap();
        SetDatabaseEncoding(PG_UTF8).unwrap();

        let mut ascii = [b'a'];
        assert!(pg_database_encoding_character_incrementer()(&mut ascii));
        assert_eq!(ascii, [b'b']);
    }

    #[test]
    fn single_byte_strlen_matches_postgres_fast_path() {
        let _guard = TEST_LOCK.lock().unwrap();
        SetDatabaseEncoding(PG_SQL_ASCII).unwrap();

        assert_eq!(pg_mbstrlen_with_len(b"a\0bc", 4).unwrap(), 4);
        assert_eq!(pg_mbstrlen(b"a\0bc").unwrap(), 1);
    }
}
