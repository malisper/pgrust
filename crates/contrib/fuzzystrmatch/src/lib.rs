//! `contrib/fuzzystrmatch` — soundex, metaphone, double metaphone,
//! Daitch-Mokotoff soundex, and the levenshtein SQL wrappers (the levenshtein
//! core is `utils/adt/levenshtein.c`, ported in the `varlena` crate).

mod daitch_mokotoff;
mod dm_table;
mod dmetaphone;

use datum::Datum;
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use types_fmgr::{varlena_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use varlena::levenshtein::{varstr_levenshtein, varstr_levenshtein_less_equal};

const LIBRARY: &str = "fuzzystrmatch";

const SOUNDEX_LEN: usize = 4;
const MAX_METAPHONE_STRLEN: usize = 255;

//                                     ABCDEFGHIJKLMNOPQRSTUVWXYZ
const SOUNDEX_TABLE: &[u8; 26] = b"01230120022455012623010202";

fn ascii_upper(c: u8) -> u8 {
    // C-locale toupper: ASCII-only folding.
    if c.is_ascii_lowercase() {
        c - b'a' + b'A'
    } else {
        c
    }
}

fn soundex_code(letter: u8) -> u8 {
    let letter = ascii_upper(letter);
    if letter.is_ascii_uppercase() {
        SOUNDEX_TABLE[(letter - b'A') as usize]
    } else {
        letter
    }
}

fn soundex(instr: &[u8]) -> [u8; SOUNDEX_LEN] {
    let mut out = [0u8; SOUNDEX_LEN];
    let mut i = 0;
    while i < instr.len() && !instr[i].is_ascii_alphabetic() {
        i += 1;
    }
    if i == instr.len() {
        return out;
    }
    out[0] = ascii_upper(instr[i]);
    i += 1;
    let mut count = 1;
    while i < instr.len() && count < SOUNDEX_LEN {
        if instr[i].is_ascii_alphabetic() && soundex_code(instr[i]) != soundex_code(instr[i - 1]) {
            let c = soundex_code(instr[i]);
            if c != b'0' {
                out[count] = c;
                count += 1;
            }
        }
        i += 1;
    }
    while count < SOUNDEX_LEN {
        out[count] = b'0';
        count += 1;
    }
    out
}

fn soundex_text_len(out: &[u8; SOUNDEX_LEN]) -> usize {
    if out[0] == 0 {
        0
    } else {
        SOUNDEX_LEN
    }
}

// Metaphone character-class codes (fuzzystrmatch.c `_codes`).
const METAPHONE_CODES: [u8; 26] = [
    1, 16, 4, 16, 9, 2, 4, 16, 9, 2, 0, 2, 2, 2, 1, 4, 0, 2, 4, 4, 1, 0, 0, 0, 8, 0,
];

fn getcode(c: u8) -> u8 {
    if c.is_ascii_alphabetic() {
        let c = ascii_upper(c);
        if c.is_ascii_uppercase() {
            return METAPHONE_CODES[(c - b'A') as usize];
        }
    }
    0
}

fn isvowel(c: u8) -> bool {
    getcode(c) & 1 != 0 // AEIOU
}
fn affecth(c: u8) -> bool {
    getcode(c) & 4 != 0 // CGPST
}
fn makesoft(c: u8) -> bool {
    getcode(c) & 8 != 0 // EIY
}
fn noghtof(c: u8) -> bool {
    getcode(c) & 16 != 0 // BDH
}

const SH: u8 = b'X';
const TH: u8 = b'0';

fn metaphone(word: &[u8], max_phonemes: usize, out: &mut Vec<u8>) {
    debug_assert!(max_phonemes > 0 && !word.is_empty());

    let at = |i: isize| -> u8 {
        if i >= 0 && (i as usize) < word.len() {
            ascii_upper(word[i as usize])
        } else {
            0
        }
    };
    // C's Look_Ahead_Letter: advances at most `how_far` bytes, stopping at NUL.
    let look_ahead = |w_idx: isize, how_far: isize| -> u8 {
        let mut idx = 0;
        while at(w_idx + idx) != 0 && idx < how_far {
            idx += 1;
        }
        at(w_idx + idx)
    };

    let mut w_idx: isize = 0;

    loop {
        if at(w_idx) == 0 {
            return;
        }
        if at(w_idx).is_ascii_alphabetic() {
            break;
        }
        w_idx += 1;
    }

    match at(w_idx) {
        b'A' => {
            if at(w_idx + 1) == b'E' {
                out.push(b'E');
                w_idx += 2;
            } else {
                out.push(b'A');
                w_idx += 1;
            }
        }
        b'G' | b'K' | b'P' => {
            if at(w_idx + 1) == b'N' {
                out.push(b'N');
                w_idx += 2;
            }
        }
        b'W' => {
            if at(w_idx + 1) == b'H' || at(w_idx + 1) == b'R' {
                out.push(at(w_idx + 1));
                w_idx += 2;
            } else if isvowel(at(w_idx + 1)) {
                out.push(b'W');
                w_idx += 2;
            }
        }
        b'X' => {
            out.push(b'S');
            w_idx += 1;
        }
        b'E' | b'I' | b'O' | b'U' => {
            out.push(at(w_idx));
            w_idx += 1;
        }
        _ => {}
    }

    while at(w_idx) != 0 && out.len() < max_phonemes {
        let mut skip_letter: isize = 0;
        let curr = at(w_idx);
        let prev = at(w_idx - 1);
        let next = at(w_idx + 1);
        let after_next = if next != 0 { at(w_idx + 2) } else { 0 };

        if !curr.is_ascii_alphabetic() || (curr == prev && curr != b'C') {
            w_idx += 1;
            continue;
        }

        match curr {
            b'B' => {
                if prev != b'M' {
                    out.push(b'B');
                }
            }
            b'C' => {
                if makesoft(next) {
                    if after_next == b'A' && next == b'I' {
                        out.push(SH);
                    } else if prev == b'S' {
                        // dropped
                    } else {
                        out.push(b'S');
                    }
                } else if next == b'H' {
                    if after_next == b'R' || prev == b'S' {
                        out.push(b'K');
                    } else {
                        out.push(SH);
                    }
                    skip_letter += 1;
                } else {
                    out.push(b'K');
                }
            }
            b'D' => {
                if next == b'G' && makesoft(after_next) {
                    out.push(b'J');
                    skip_letter += 1;
                } else {
                    out.push(b'T');
                }
            }
            b'G' => {
                if next == b'H' {
                    if !(noghtof(at(w_idx - 3)) || at(w_idx - 4) == b'H') {
                        out.push(b'F');
                        skip_letter += 1;
                    }
                } else if next == b'N' {
                    if !after_next.is_ascii_alphabetic()
                        || (after_next == b'E' && look_ahead(w_idx, 3) == b'D')
                    {
                        // dropped
                    } else {
                        out.push(b'K');
                    }
                } else if makesoft(next) && prev != b'G' {
                    out.push(b'J');
                } else {
                    out.push(b'K');
                }
            }
            b'H' => {
                if isvowel(next) && !affecth(prev) {
                    out.push(b'H');
                }
            }
            b'K' => {
                if prev != b'C' {
                    out.push(b'K');
                }
            }
            b'P' => {
                if next == b'H' {
                    out.push(b'F');
                } else {
                    out.push(b'P');
                }
            }
            b'Q' => out.push(b'K'),
            b'S' => {
                if next == b'I' && (after_next == b'O' || after_next == b'A') {
                    out.push(SH);
                } else if next == b'H' {
                    out.push(SH);
                    skip_letter += 1;
                } else if next == b'C'
                    && look_ahead(w_idx, 2) == b'H'
                    && look_ahead(w_idx, 3) == b'W'
                {
                    out.push(SH);
                    skip_letter += 2;
                } else {
                    out.push(b'S');
                }
            }
            b'T' => {
                if next == b'I' && (after_next == b'O' || after_next == b'A') {
                    out.push(SH);
                } else if next == b'H' {
                    out.push(TH);
                    skip_letter += 1;
                } else {
                    out.push(b'T');
                }
            }
            b'V' => out.push(b'F'),
            b'W' => {
                if isvowel(next) {
                    out.push(b'W');
                }
            }
            b'X' => {
                out.push(b'K');
                if out.len() < max_phonemes {
                    out.push(b'S');
                }
            }
            b'Y' => {
                if isvowel(next) {
                    out.push(b'Y');
                }
            }
            b'Z' => out.push(b'S'),
            b'F' | b'J' | b'L' | b'M' | b'N' | b'R' => out.push(curr),
            _ => {}
        }
        w_idx += 1 + skip_letter;
    }
}

#[cold]
#[inline(never)]
fn param_err(msg: String, sqlstate: types_error::SqlState) -> PgError {
    PgError::error(msg).with_sqlstate(sqlstate)
}

fn levenshtein_args<'a>(fcinfo: &'a Fcinfo) -> PgResult<(&'a [u8], &'a [u8])> {
    // SAFETY: catalog args are non-null text varlenas (strict fns).
    let (s, t) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    Ok((s.data(), t.data()))
}

fn fc_levenshtein(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (s, t) = levenshtein_args(fcinfo)?;
    let d = varstr_levenshtein(fcinfo.result_mcx(), s, t, 1, 1, 1, false)?;
    Ok(Datum::from_i32(d))
}

fn fc_levenshtein_with_costs(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (s, t) = levenshtein_args(fcinfo)?;
    let (ins_c, del_c, sub_c) = (fcinfo.arg_i32(2), fcinfo.arg_i32(3), fcinfo.arg_i32(4));
    let d = varstr_levenshtein(fcinfo.result_mcx(), s, t, ins_c, del_c, sub_c, false)?;
    Ok(Datum::from_i32(d))
}

fn fc_levenshtein_less_equal(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (s, t) = levenshtein_args(fcinfo)?;
    let max_d = fcinfo.arg_i32(2);
    let d = varstr_levenshtein_less_equal(fcinfo.result_mcx(), s, t, 1, 1, 1, max_d, false)?;
    Ok(Datum::from_i32(d))
}

fn fc_levenshtein_less_equal_with_costs(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let (s, t) = levenshtein_args(fcinfo)?;
    let (ins_c, del_c, sub_c) = (fcinfo.arg_i32(2), fcinfo.arg_i32(3), fcinfo.arg_i32(4));
    let max_d = fcinfo.arg_i32(5);
    let d =
        varstr_levenshtein_less_equal(fcinfo.result_mcx(), s, t, ins_c, del_c, sub_c, max_d, false)?;
    Ok(Datum::from_i32(d))
}

fn metaphone_check_limits(word_len: usize, reqlen: i32) -> PgResult<()> {
    if word_len > MAX_METAPHONE_STRLEN {
        return Err(param_err(
            format!("argument exceeds the maximum length of {MAX_METAPHONE_STRLEN} bytes"),
            ERRCODE_INVALID_PARAMETER_VALUE,
        )
        .into());
    }
    if reqlen > MAX_METAPHONE_STRLEN as i32 {
        return Err(param_err(
            format!("output exceeds the maximum length of {MAX_METAPHONE_STRLEN} bytes"),
            ERRCODE_INVALID_PARAMETER_VALUE,
        )
        .into());
    }
    if reqlen <= 0 {
        return Err(param_err(
            "output cannot be empty string".to_string(),
            types_error::ERRCODE_ZERO_LENGTH_CHARACTER_STRING,
        )
        .into());
    }
    Ok(())
}

fn fc_metaphone(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg is a non-null text varlena (strict fn).
    let src = unsafe { fcinfo.arg_varlena_packed(0)? };
    let word = src.data();
    let mcx = fcinfo.result_mcx();

    if word.is_empty() {
        return Ok(varlena_result(varlena::cstring_to_text(mcx, b"")?));
    }
    let reqlen = fcinfo.arg_i32(1);
    metaphone_check_limits(word.len(), reqlen)?;

    let mut out = Vec::with_capacity(reqlen as usize + 1);
    metaphone(word, reqlen as usize, &mut out);
    Ok(varlena_result(varlena::cstring_to_text(mcx, &out)?))
}

fn fc_soundex(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg is a non-null text varlena (strict fn).
    let src = unsafe { fcinfo.arg_varlena_packed(0)? };
    let out = soundex(src.data());
    let n = soundex_text_len(&out);
    Ok(varlena_result(varlena::cstring_to_text(
        fcinfo.result_mcx(),
        &out[..n],
    )?))
}

fn fc_difference(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null text varlenas (strict fn).
    let (a, b) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    let s1 = soundex(a.data());
    let s2 = soundex(b.data());
    let mut result = 0;
    for i in 0..SOUNDEX_LEN {
        if s1[i] == s2[i] {
            result += 1;
        }
    }
    Ok(Datum::from_i32(result))
}

fn fc_dmetaphone(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg is a non-null text varlena (strict fn).
    let src = unsafe { fcinfo.arg_varlena_packed(0)? };
    let (primary, _alt) = dmetaphone::double_metaphone(src.data());
    Ok(varlena_result(varlena::cstring_to_text(
        fcinfo.result_mcx(),
        &primary,
    )?))
}

fn fc_dmetaphone_alt(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg is a non-null text varlena (strict fn).
    let src = unsafe { fcinfo.arg_varlena_packed(0)? };
    let (_primary, alt) = dmetaphone::double_metaphone(src.data());
    Ok(varlena_result(varlena::cstring_to_text(
        fcinfo.result_mcx(),
        &alt,
    )?))
}

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "levenshtein" => fc_levenshtein,
        "levenshtein_with_costs" => fc_levenshtein_with_costs,
        "levenshtein_less_equal" => fc_levenshtein_less_equal,
        "levenshtein_less_equal_with_costs" => fc_levenshtein_less_equal_with_costs,
        "metaphone" => fc_metaphone,
        "soundex" => fc_soundex,
        "difference" => fc_difference,
        "dmetaphone" => fc_dmetaphone,
        "dmetaphone_alt" => fc_dmetaphone_alt,
        "daitch_mokotoff" => daitch_mokotoff::fc_daitch_mokotoff,
        _ => return None,
    })
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: None,
    });
}

#[cfg(test)]
mod tests;
