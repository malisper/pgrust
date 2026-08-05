//! varlena.c Unicode tail: unicode_version/unicode_assigned and the
//! normalize()/IS NORMALIZED value cores over common/unicode_norm.

use datum::Varlena;
use mcx::{Mcx, PgVec};
use types_error::{
    PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_SYNTAX_ERROR,
};
use unicode_norm::{
    unicode_is_normalized_quickcheck, unicode_normalize, UnicodeNormalizationForm, UNICODE_NFC,
    UNICODE_NFD, UNICODE_NFKC, UNICODE_NFKD, UNICODE_NORM_QC_NO, UNICODE_NORM_QC_YES,
};
use wchar::{pg_utf_mblen, pg_wchar, unicode_to_utf8, utf8_to_unicode, PG_UTF8};

// common/unicode_version.h (PG 18.3).
const PG_UNICODE_VERSION: &[u8] = b"16.0";

pub fn unicode_version<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Varlena<'mcx>> {
    crate::cstring_to_text(mcx, PG_UNICODE_VERSION)
}

fn unicode_norm_form_from_string(formstr: &[u8]) -> PgResult<UnicodeNormalizationForm> {
    if mbutils::GetDatabaseEncoding() != PG_UTF8 {
        return Err(PgError::error(
            "Unicode normalization can only be performed if server encoding is UTF8",
        )
        .with_sqlstate(ERRCODE_SYNTAX_ERROR)
        .into());
    }
    if formstr.eq_ignore_ascii_case(b"NFC") {
        Ok(UNICODE_NFC)
    } else if formstr.eq_ignore_ascii_case(b"NFD") {
        Ok(UNICODE_NFD)
    } else if formstr.eq_ignore_ascii_case(b"NFKC") {
        Ok(UNICODE_NFKC)
    } else if formstr.eq_ignore_ascii_case(b"NFKD") {
        Ok(UNICODE_NFKD)
    } else {
        Err(PgError::error(format!(
            "invalid normalization form: {}",
            String::from_utf8_lossy(formstr)
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
        .into())
    }
}

pub fn unicode_assigned(input: &[u8]) -> PgResult<bool> {
    if mbutils::GetDatabaseEncoding() != PG_UTF8 {
        return Err(PgError::error(
            "Unicode categorization can only be performed if server encoding is UTF8",
        )
        .into());
    }
    let mut off = 0usize;
    while off < input.len() {
        let uchar = utf8_to_unicode(&input[off..]);
        if unicode_category::unicode_category(uchar) == unicode_category::PG_U_UNASSIGNED {
            return Ok(false);
        }
        off += pg_utf_mblen(&input[off..]) as usize;
    }
    Ok(true)
}

fn utf8_to_wchars<'mcx>(mcx: Mcx<'mcx>, input: &[u8]) -> PgResult<PgVec<'mcx, pg_wchar>> {
    let size = mbutils::pg_mbstrlen_with_len(input)? as usize;
    let mut chars = mcx::vec_with_capacity_in(mcx, size)?;
    let mut off = 0usize;
    while off < input.len() {
        chars.push(utf8_to_unicode(&input[off..]));
        off += pg_utf_mblen(&input[off..]) as usize;
    }
    Ok(chars)
}

pub fn unicode_normalize_func<'mcx>(
    mcx: Mcx<'mcx>,
    input: &[u8],
    formstr: &[u8],
) -> PgResult<Varlena<'mcx>> {
    let form = unicode_norm_form_from_string(formstr)?;
    let input_chars = utf8_to_wchars(mcx, input)?;

    let output_chars = unicode_normalize(mcx, form, &input_chars)?;

    let mut size = 0usize;
    for &wp in output_chars.iter() {
        let mut buf = [0u8; 4];
        unicode_to_utf8(wp, &mut buf);
        size += pg_utf_mblen(&buf) as usize;
    }

    let mut image = crate::image_with_header(mcx, size)?;
    for &wp in output_chars.iter() {
        let mut buf = [0u8; 4];
        unicode_to_utf8(wp, &mut buf);
        let len = pg_utf_mblen(&buf) as usize;
        mcx::vec_append_bytes(&mut image, &buf[..len])?;
    }
    Ok(Varlena::from_image(image))
}

pub fn unicode_is_normalized(mcx: Mcx<'_>, input: &[u8], formstr: &[u8]) -> PgResult<bool> {
    let form = unicode_norm_form_from_string(formstr)?;
    let input_chars = utf8_to_wchars(mcx, input)?;

    let quickcheck = unicode_is_normalized_quickcheck(form, &input_chars);
    if quickcheck == UNICODE_NORM_QC_YES {
        return Ok(true);
    } else if quickcheck == UNICODE_NORM_QC_NO {
        return Ok(false);
    }

    let output_chars = unicode_normalize(mcx, form, &input_chars)?;
    Ok(input_chars.as_slice() == output_chars.as_slice())
}
