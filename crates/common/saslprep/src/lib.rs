#![allow(non_snake_case)]

mod tables;
#[cfg(test)]
mod tests;

use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_error::PgResult;
use unicode_norm::{unicode_normalize, UNICODE_NFKC};
use wchar::{pg_utf8_islegal, pg_utf_mblen, pg_wchar, unicode_to_utf8};

use tables::{
    COMMONLY_MAPPED_TO_NOTHING_RANGES, L_CAT_CODEPOINT_RANGES, NON_ASCII_SPACE_RANGES,
    PROHIBITED_OUTPUT_RANGES, RAND_A_L_CAT_CODEPOINT_RANGES, UNASSIGNED_CODEPOINT_RANGES,
};

// C pg_saslprep_rc maps to: Ok(Some) = SASLPREP_SUCCESS, Ok(None) =
// SASLPREP_INVALID_UTF8 / SASLPREP_PROHIBITED, Err = backend OOM ereport.
pub fn pg_saslprep<'mcx>(mcx: Mcx<'mcx>, input: &[u8]) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // C pg_is_ascii tests only the high bit; a pure-ASCII input is STRDUPed
    // verbatim with no prohibited-output check.
    if !input.iter().any(|&b| b & 0x80 != 0) {
        let mut copy = vec_with_capacity_in(mcx, input.len())?;
        copy.extend_from_slice(input);
        return Ok(Some(copy));
    }

    let input_chars = match pg_utf8_string_to_codepoints(mcx, input)? {
        Some(v) => v,
        None => return Ok(None), // SASLPREP_INVALID_UTF8
    };

    // Step 1) Map. The mapped length never exceeds the input length (C maps in
    // place into the front of input_chars).
    let mut mapped: PgVec<'mcx, pg_wchar> = vec_with_capacity_in(mcx, input_chars.len())?;
    for &code in input_chars.iter() {
        if is_code_in_table(code, NON_ASCII_SPACE_RANGES) {
            mapped.push(0x0020);
        } else if is_code_in_table(code, COMMONLY_MAPPED_TO_NOTHING_RANGES) {
            // map to nothing
        } else {
            mapped.push(code);
        }
    }

    if mapped.is_empty() {
        return Ok(None); // SASLPREP_PROHIBITED: don't allow empty password
    }

    // Step 2) Normalize with NFKC.
    let output_chars = unicode_normalize(mcx, UNICODE_NFKC, &mapped)?;

    // Step 3) Prohibit. C runs this over input_chars (the MAPPED codepoints),
    // not the normalized output.
    for &code in mapped.iter() {
        if is_code_in_table(code, PROHIBITED_OUTPUT_RANGES)
            || is_code_in_table(code, UNASSIGNED_CODEPOINT_RANGES)
        {
            return Ok(None); // SASLPREP_PROHIBITED
        }
    }

    // Step 4) Check bidi (RFC 3454 section 6), also over the MAPPED codepoints.
    let contains_RandALCat = mapped
        .iter()
        .any(|&code| is_code_in_table(code, RAND_A_L_CAT_CODEPOINT_RANGES));

    if contains_RandALCat {
        let first = mapped[0];
        let last = mapped[mapped.len() - 1];

        if mapped
            .iter()
            .any(|&code| is_code_in_table(code, L_CAT_CODEPOINT_RANGES))
        {
            return Ok(None); // SASLPREP_PROHIBITED
        }

        if !is_code_in_table(first, RAND_A_L_CAT_CODEPOINT_RANGES)
            || !is_code_in_table(last, RAND_A_L_CAT_CODEPOINT_RANGES)
        {
            return Ok(None); // SASLPREP_PROHIBITED
        }
    }

    // C sizes the result buffer in a first pass and fills it in a second, so
    // the OOM check happens before any bytes are written.
    let mut result_size = 0usize;
    for &code in output_chars.iter() {
        let mut buf = [0u8; 4];
        unicode_to_utf8(code, &mut buf);
        result_size += pg_utf_mblen(&buf) as usize;
    }

    let mut result: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, result_size)?;
    for &code in output_chars.iter() {
        let mut buf = [0u8; 4];
        unicode_to_utf8(code, &mut buf);
        let len = pg_utf_mblen(&buf) as usize;
        result.extend_from_slice(&buf[..len]);
    }

    Ok(Some(result))
}

// C pg_utf8_string_len validation fused with the decode loop; None is
// SASLPREP_INVALID_UTF8.
fn pg_utf8_string_to_codepoints<'mcx>(
    mcx: Mcx<'mcx>,
    input: &[u8],
) -> PgResult<Option<PgVec<'mcx, pg_wchar>>> {
    let mut num_chars = 0usize;
    {
        let mut remaining = input;
        while !remaining.is_empty() {
            let l = pg_utf_mblen(remaining) as usize;
            if remaining.len() < l || !pg_utf8_islegal(&remaining[..l], l as i32) {
                return Ok(None);
            }
            remaining = &remaining[l..];
            num_chars += 1;
        }
    }

    let mut result = vec_with_capacity_in(mcx, num_chars)?;
    let mut remaining = input;
    while !remaining.is_empty() {
        let l = pg_utf_mblen(remaining) as usize;
        result.push(wchar::utf8_to_unicode(&remaining[..l]));
        remaining = &remaining[l..];
    }

    Ok(Some(result))
}

// `ranges` is the sorted, non-overlapping inclusive (first, last) pair table;
// mirrors the C is_code_in_table bsearch over the flat pg_wchar arrays.
fn is_code_in_table(code: pg_wchar, ranges: &[(u32, u32)]) -> bool {
    if ranges.is_empty() || code < ranges[0].0 || code > ranges[ranges.len() - 1].1 {
        return false;
    }

    ranges
        .binary_search_by(|&(first, last)| {
            if code < first {
                core::cmp::Ordering::Greater
            } else if code > last {
                core::cmp::Ordering::Less
            } else {
                core::cmp::Ordering::Equal
            }
        })
        .is_ok()
}
