//! Port of `contrib/isn` — EAN13/ISBN/ISMN/ISSN/UPC types over an int8-passed
//! `ean13` (bit 0 = "invalid check digit on input" flag, value in bits 1..).
//! Only the I/O, cast, and flag functions are C code in isn.c; every
//! comparison/btree/hash function is `LANGUAGE internal` over int8 in the SQL
//! script, so none of those live here.
//!
//! DIVERGENCE (GUC): C's `_PG_init` runs `DefineCustomBoolVariable("isn.weak")`
//! + `MarkGUCPrefixReserved("isn")`. pgrust has no typed custom-GUC store, so
//! `isn.weak` rides the placeholder string store (the pg_trgm pattern): reads
//! parse the placeholder or default to false; the prefix is not reserved; SHOW
//! echoes the SET spelling rather than canonical on/off until `isn_weak(bool)`
//! stores "on"/"off".

pub mod builtins;
mod tables;

pub use builtins::init_seams;

use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};

use tables::{
    Range, EAN13_INDEX, EAN13_RANGE, ISBN_INDEX, ISBN_INDEX_NEW, ISBN_RANGE, ISBN_RANGE_NEW,
    ISMN_INDEX, ISMN_RANGE, ISSN_INDEX, ISSN_RANGE, UPC_INDEX, UPC_RANGE,
};

pub const MAXEAN13LEN: usize = 18;

/// The int8-carried isn value; C's `typedef uint64 ean13`.
pub type Ean13 = u64;

const EAN13_MAX: u64 = 9_999_999_999_999;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum IsnType {
    Invalid = 0,
    Any,
    Ean13,
    Isbn,
    Ismn,
    Issn,
    Upc,
}

const ISN_NAMES: [&str; 7] = [
    "EAN13/UPC/ISxN",
    "EAN13/UPC/ISxN",
    "EAN13",
    "ISBN",
    "ISMN",
    "ISSN",
    "UPC",
];

fn isn_name(t: IsnType) -> &'static str {
    ISN_NAMES[t as usize]
}

type HyphTable = (&'static [Range], &'static [[u32; 2]; 10]);

// ---------------------------------------------------------------------------
// Formatting and conversion routines (isn.c statics), buffer-index form: C's
// overlapping bufO/bufI pointers become (out, inp) indices into one buffer.
// All buffers are NUL-terminated within their fixed arrays, like C.
// ---------------------------------------------------------------------------

fn dehyphenate(buf: &mut [u8], mut out: usize, mut inp: usize) -> u32 {
    let mut ret = 0;
    while buf[inp] != 0 {
        let c = buf[inp];
        if c.is_ascii_digit() {
            buf[out] = c;
            out += 1;
            ret += 1;
        }
        inp += 1;
    }
    buf[out] = 0;
    ret
}

/// `hyphenate`: in-place hyphenation of the digit string at `inp` into `out`
/// via binary search over a range table; returns chars hyphenated + 1, or 0
/// when no range matches. `None` table = plain compress (C's NULL TABLE).
/// The u32 index arithmetic wraps exactly like C's unsigned locals: an index
/// row `{0, n}` makes `lower` wrap to `u32::MAX` and the math still lands on
/// entry `(n-1)/2`; `{x, 0}` rows collapse `step` to 0 = "no ranges".
fn hyphenate(buf: &mut [u8], out_start: usize, in_start: usize, table: Option<HyphTable>) -> u32 {
    let Some((table, index)) = table else {
        let mut ret = 0u32;
        let (mut o, mut i) = (out_start, in_start);
        while buf[i] != 0 {
            buf[o] = buf[i];
            o += 1;
            i += 1;
            ret += 1;
        }
        buf[o] = 0;
        return ret + 1;
    };

    let row = index[(buf[in_start] - b'0') as usize];
    let mut lower = row[0].wrapping_sub(1);
    let mut upper = row[0].wrapping_add(row[1]);
    let mut step = upper.wrapping_sub(lower) / 2;
    if step == 0 {
        return 0;
    }
    let mut search = lower.wrapping_add(step);

    let mut firstdig = in_start;
    let (mut ean_in1, mut ean_in2) = (false, false);
    let mut aux1 = table[search as usize][0].as_bytes();
    let mut aux2 = table[search as usize][1].as_bytes();
    let (mut p1, mut p2) = (0usize, 0usize);
    loop {
        let fd = buf[firstdig];
        let (c1, c2) = (aux1[p1], aux2[p2]);
        if (ean_in1 || fd >= c1) && (ean_in2 || fd <= c2) {
            if fd > c1 {
                ean_in1 = true;
            }
            if fd < c2 {
                ean_in2 = true;
            }
            if ean_in1 && ean_in2 {
                break;
            }
            firstdig += 1;
            p1 += 1;
            p2 += 1;
            let e1 = if p1 < aux1.len() { aux1[p1] } else { 0 };
            let e2 = if p2 < aux2.len() { aux2[p2] } else { 0 };
            if e1 == 0 || e2 == 0 || buf[firstdig] == 0 {
                break;
            }
            if !e1.is_ascii_digit() {
                p1 += 1;
                p2 += 1;
            }
        } else {
            if fd < c1 && !ean_in1 {
                upper = search;
            } else {
                lower = search;
            }
            step = upper.wrapping_sub(lower) / 2;
            search = lower.wrapping_add(step);
            firstdig = in_start;
            ean_in1 = false;
            ean_in2 = false;
            aux1 = table[search as usize][0].as_bytes();
            p1 = 0;
            aux2 = table[search as usize][1].as_bytes();
            p2 = 0;
        }
        if step == 0 {
            break;
        }
    }

    if step != 0 {
        // Found: copy digits in the matched entry's shape, then the trailing
        // hyphen and one lookahead char (C's in-place shift-by-zero trick).
        let pat = table[search as usize][0].as_bytes();
        let (mut o, mut i) = (out_start, in_start);
        let mut ret = 0u32;
        let mut pp = 0usize;
        while pp < pat.len() && buf[i] != 0 {
            if pat[pp] != b'-' {
                buf[o] = buf[i];
                i += 1;
            } else {
                buf[o] = b'-';
            }
            o += 1;
            pp += 1;
            ret += 1;
        }
        buf[o] = b'-';
        buf[o + 1] = buf[i];
        return ret + 1;
    }
    0
}

/// `weight_checkdig`: ISBN-10/ISSN mod-11 check value (0-10) over the first
/// `size - 1` digits.
pub fn weight_checkdig(isn: &[u8], mut size: u32) -> u32 {
    let mut weight = 0u32;
    for &c in isn {
        if c == 0 || size <= 1 {
            break;
        }
        if c.is_ascii_digit() {
            weight += size * (c - b'0') as u32;
            size -= 1;
        }
    }
    weight %= 11;
    if weight != 0 {
        weight = 11 - weight;
    }
    weight
}

/// `checkdig`: EAN13 mod-10 check digit over the first `size - 1` digits
/// (ISMN's leading 'M' counts as 3 in position 0).
pub fn checkdig(num: &[u8], mut size: u32) -> u32 {
    let (mut check, mut check3) = (0u32, 0u32);
    let mut pos = 0u32;
    if num.first() == Some(&b'M') {
        check3 = 3;
        pos = 1;
    }
    for &c in num {
        if c == 0 || size <= 1 {
            break;
        }
        if c.is_ascii_digit() {
            if pos % 2 == 1 {
                check3 += (c - b'0') as u32;
            } else {
                check += (c - b'0') as u32;
            }
            pos += 1;
            size -= 1;
        }
    }
    let mut result = (check + 3 * check3) % 10;
    if result != 0 {
        result = 10 - result;
    }
    result
}

/// `str2ean`: digits of a normalized string -> ean13 value shifted left one
/// bit (room for the invalid flag).
fn str2ean(num: &[u8]) -> Ean13 {
    let mut ean: u64 = 0;
    for &c in num {
        if c == 0 {
            break;
        }
        if c.is_ascii_digit() {
            ean = 10 * ean + (c - b'0') as u64;
        }
    }
    ean << 1
}

fn out_of_range_err(shown: &str, ty: IsnType) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "value \"{shown}\" is out of range for {} type",
            isn_name(ty)
        ))
        .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
    )
}

fn write_13_digits(buf: &mut [u8], mut ean: u64) {
    for i in (0..13).rev() {
        buf[i] = b'0' + (ean % 10) as u8;
        ean /= 10;
    }
}

/// `ean2isn`: re-type an ean13 value as `accept`, validating only the prefix
/// class (not the check digit). Always the errorOK=false lane (both C callers).
pub fn ean2isn(ean_in: Ean13, accept: IsnType) -> PgResult<Ean13> {
    let ean = ean_in >> 1;
    if ean > EAN13_MAX {
        // C reaches eantoobig with type still INVALID.
        return Err(out_of_range_err(&ean.to_string(), IsnType::Invalid));
    }

    let mut buf = [0u8; 14];
    write_13_digits(&mut buf, ean);

    let ty = if buf.starts_with(b"978") {
        IsnType::Isbn
    } else if buf.starts_with(b"977") {
        IsnType::Issn
    } else if buf.starts_with(b"9790") {
        IsnType::Ismn
    } else if buf.starts_with(b"979") {
        IsnType::Isbn
    } else if buf[0] == b'0' {
        IsnType::Upc
    } else {
        IsnType::Ean13
    };
    if accept != IsnType::Any && accept != IsnType::Ean13 && accept != ty {
        let num = core::str::from_utf8(&buf[..13]).expect("all ascii digits");
        let msg = if ty != IsnType::Ean13 {
            format!(
                "cannot cast EAN13({}) to {} for number: \"{num}\"",
                isn_name(ty),
                isn_name(accept)
            )
        } else {
            format!(
                "cannot cast {} to {} for number: \"{num}\"",
                isn_name(ty),
                isn_name(accept)
            )
        };
        return Err(Box::new(
            PgError::error(msg).with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
        ));
    }
    Ok(ean_in)
}

// ean2ISBN/ean2ISMN/ean2ISSN/ean2UPC: convert the hyphenated long form in
// `buf` to the short type form, in place.

fn ean2isbn_short(buf: &mut [u8]) {
    if buf.starts_with(b"978-") {
        // Strip the 978- prefix and rewrite the last digit with the ISBN-10
        // check (the walk back skips the '!' flag and NUL like C's strchr).
        hyphenate(buf, 0, 4, None);
        let check = weight_checkdig(buf, 10);
        let mut aux = buf.iter().position(|&c| c == 0).expect("NUL-terminated");
        loop {
            aux -= 1;
            if buf[aux].is_ascii_digit() {
                break;
            }
        }
        buf[aux] = if check == 10 { b'X' } else { b'0' + check as u8 };
    }
}

fn ean2ismn_short(buf: &mut [u8]) {
    hyphenate(buf, 0, 4, None);
    buf[0] = b'M';
}

fn ean2issn_short(buf: &mut [u8]) {
    hyphenate(buf, 0, 4, None);
    let check = weight_checkdig(buf, 8);
    buf[8] = if check == 10 { b'X' } else { b'0' + check as u8 };
    buf[9] = 0;
}

fn ean2upc_short(buf: &mut [u8]) {
    dehyphenate(buf, 0, 1);
    buf[12] = 0;
}

/// `ean2string`: hyphenated text form of an ean13 into `result`
/// (`short_type` = the legacy ISxN short output). Always errorOK=false.
pub fn ean2string(
    ean_in: Ean13,
    result: &mut [u8; MAXEAN13LEN + 1],
    short_type: bool,
) -> PgResult<()> {
    let mut ty = IsnType::Invalid;
    let valid = if ean_in & 1 != 0 { b'!' } else { 0u8 };
    let mut ean = ean_in >> 1;
    if ean > EAN13_MAX {
        return Err(out_of_range_err(&ean.to_string(), ty));
    }

    // Build "???DDDDDDDDDDDD-D<valid>" back-to-front (C's do/while with the
    // check-digit hyphen on the first iteration).
    let mut count = 0u32;
    let mut aux = MAXEAN13LEN;
    result[aux] = 0;
    aux -= 1;
    result[aux] = valid;
    loop {
        aux -= 1;
        result[aux] = b'0' + (ean % 10) as u8;
        ean /= 10;
        if count == 0 {
            aux -= 1;
            result[aux] = b'-';
        }
        if ean == 0 {
            break;
        }
        count += 1;
        if count > 13 {
            break;
        }
    }
    while count < 13 {
        count += 1;
        aux -= 1;
        result[aux] = b'0';
    }

    // Country-prefix hyphenation, then the per-type publisher hyphenation.
    let search = hyphenate(result, 0, 3, Some((EAN13_RANGE, &EAN13_INDEX)));
    if search == 0 {
        hyphenate(result, 0, 3, None);
    } else {
        let n = search as usize;
        let prefix_is = |lit: &[u8]| n <= lit.len() && result[..n] == lit[..n];
        let mut table: Option<HyphTable> = None;
        if prefix_is(b"978-") {
            ty = IsnType::Isbn;
            table = Some((ISBN_RANGE, &ISBN_INDEX));
        } else if prefix_is(b"977-") {
            ty = IsnType::Issn;
            table = Some((ISSN_RANGE, &ISSN_INDEX));
        } else if n + 1 <= 5 && result[..n + 1] == b"979-0"[..n + 1] {
            ty = IsnType::Ismn;
            table = Some((ISMN_RANGE, &ISMN_INDEX));
        } else if prefix_is(b"979-") {
            ty = IsnType::Isbn;
            table = Some((ISBN_RANGE_NEW, &ISBN_INDEX_NEW));
        } else if result[0] == b'0' {
            ty = IsnType::Upc;
            table = Some((UPC_RANGE, &UPC_INDEX));
        } else {
            ty = IsnType::Ean13;
        }

        let digval = search as usize;
        let search2 = hyphenate(result, digval, digval + 2, table);
        if search2 == 0 {
            hyphenate(result, digval, digval + 2, None);
        }
    }

    if short_type {
        match ty {
            IsnType::Isbn => ean2isbn_short(result),
            IsnType::Ismn => ean2ismn_short(result),
            IsnType::Issn => ean2issn_short(result),
            IsnType::Upc => ean2upc_short(result),
            _ => {}
        }
    }
    Ok(())
}

/// `string2ean`: parse text into an ean13. `Ok(None)` = soft error saved into
/// `escontext` (C's ereturn(false)). `weak` is the isn.weak GUC value (C reads
/// the g_weak global here).
pub fn string2ean(
    input: &[u8],
    escontext: Option<&mut SoftErrorContext>,
    accept: IsnType,
    weak: bool,
) -> PgResult<Option<Ean13>> {
    let mut buf: [u8; 17] = *b"                \0";
    let mut w = 3usize; // C's aux1
    let mut ty = IsnType::Invalid;
    let check: u32;
    let mut rcheck: u32 = u32::MAX;
    let mut length: u32 = 0;
    let mut magic = false;
    let mut valid = true;

    let at = |i: usize| -> u8 {
        if i < input.len() {
            input[i]
        } else {
            0
        }
    };
    let shown = || String::from_utf8_lossy(input).into_owned();

    let eaninvalid = |escontext: Option<&mut SoftErrorContext>| {
        ereturn(
            escontext,
            None,
            PgError::error(format!(
                "invalid input syntax for {} number: \"{}\"",
                isn_name(accept),
                String::from_utf8_lossy(input)
            ))
            .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
        )
    };
    let eanwrongtype = |escontext: Option<&mut SoftErrorContext>, ty: IsnType| {
        ereturn(
            escontext,
            None,
            PgError::error(format!(
                "cannot cast {} to {} for number: \"{}\"",
                isn_name(ty),
                isn_name(accept),
                String::from_utf8_lossy(input)
            ))
            .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION),
        )
    };

    // Recognize and validate the number.
    let mut i = 0usize;
    while at(i) != 0 && length <= 13 {
        let c = at(i);
        let last = at(i + 1) == b'!' || at(i + 1) == 0;
        let mut digit = c.is_ascii_digit();
        if c == b'?' && last {
            // Automagically compute the check digit.
            magic = true;
            digit = true;
        }
        if length == 0 && (c == b'M' || c == b'm') {
            if ty != IsnType::Invalid {
                return eaninvalid(escontext);
            }
            ty = IsnType::Ismn;
            buf[w] = b'M';
            w += 1;
            length += 1;
        } else if length == 7 && (digit || c == b'X' || c == b'x') && last {
            if ty != IsnType::Invalid {
                return eaninvalid(escontext);
            }
            ty = IsnType::Issn;
            buf[w] = c.to_ascii_uppercase();
            w += 1;
            length += 1;
        } else if length == 9 && (digit || c == b'X' || c == b'x') && last {
            if ty != IsnType::Invalid && ty != IsnType::Ismn {
                return eaninvalid(escontext);
            }
            if ty == IsnType::Invalid {
                ty = IsnType::Isbn; // ISMN must start with 'M'
            }
            buf[w] = c.to_ascii_uppercase();
            w += 1;
            length += 1;
        } else if length == 11 && digit && last {
            if ty != IsnType::Invalid {
                return eaninvalid(escontext);
            }
            ty = IsnType::Upc;
            buf[w] = c;
            w += 1;
            length += 1;
        } else if c == b'-' || c == b' ' {
            // Skip.
        } else if c == b'!' && at(i + 1) == 0 {
            // The invalid-check-digit suffix.
            if !magic {
                valid = false;
            }
            magic = true;
        } else if !digit {
            return eaninvalid(escontext);
        } else {
            buf[w] = c;
            w += 1;
            length += 1;
            if length > 13 {
                return ereturn(
                    escontext,
                    None,
                    PgError::error(format!(
                        "value \"{}\" is out of range for {} type",
                        shown(),
                        isn_name(accept)
                    ))
                    .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                );
            }
        }
        i += 1;
    }
    buf[w] = 0;

    // Find the given check digit value.
    if length == 13 {
        if ty != IsnType::Invalid {
            return eaninvalid(escontext);
        }
        ty = IsnType::Ean13;
        check = (buf[15].wrapping_sub(b'0')) as u32;
    } else if length == 12 {
        if ty != IsnType::Upc {
            return eaninvalid(escontext);
        }
        check = (buf[14].wrapping_sub(b'0')) as u32;
    } else if length == 10 {
        if ty != IsnType::Isbn && ty != IsnType::Ismn {
            return eaninvalid(escontext);
        }
        check = if buf[12] == b'X' {
            10
        } else {
            (buf[12].wrapping_sub(b'0')) as u32
        };
    } else if length == 8 {
        if ty != IsnType::Invalid && ty != IsnType::Issn {
            return eaninvalid(escontext);
        }
        ty = IsnType::Issn;
        check = if buf[10] == b'X' {
            10
        } else {
            (buf[10].wrapping_sub(b'0')) as u32
        };
    } else {
        return eaninvalid(escontext);
    }

    if ty == IsnType::Invalid {
        return eaninvalid(escontext);
    }

    // Validate and normalize to EAN13.
    if accept == IsnType::Ean13 && ty != accept {
        return eanwrongtype(escontext, ty);
    }
    if accept != IsnType::Any && ty != IsnType::Ean13 && ty != accept {
        return eanwrongtype(escontext, ty);
    }
    match ty {
        IsnType::Ean13 => {
            rcheck = checkdig(&buf[3..], 13);
            valid = valid && (rcheck == check || magic);
            // Get the subtype of EAN13.
            ty = if buf[3] == b'0' {
                IsnType::Upc
            } else if buf[3..].starts_with(b"977") {
                IsnType::Issn
            } else if buf[3..].starts_with(b"978") {
                IsnType::Isbn
            } else if buf[3..].starts_with(b"9790") {
                IsnType::Ismn
            } else if buf[3..].starts_with(b"979") {
                IsnType::Isbn
            } else {
                ty
            };
            if accept != IsnType::Ean13 && accept != IsnType::Any && ty != accept {
                return eanwrongtype(escontext, ty);
            }
        }
        IsnType::Ismn => {
            buf[..4].copy_from_slice(b"9790"); // ISMN is only 9790 for now
            rcheck = checkdig(&buf, 13);
            valid = valid && (rcheck == check || magic);
        }
        IsnType::Isbn => {
            buf[..3].copy_from_slice(b"978");
            rcheck = weight_checkdig(&buf[3..], 10);
            valid = valid && (rcheck == check || magic);
        }
        IsnType::Issn => {
            buf[10..12].copy_from_slice(b"00"); // the normal issue publication code
            buf[..3].copy_from_slice(b"977");
            rcheck = weight_checkdig(&buf[3..], 8);
            valid = valid && (rcheck == check || magic);
        }
        IsnType::Upc => {
            buf[2] = b'0';
            rcheck = checkdig(&buf[2..], 13);
            valid = valid && (rcheck == check || magic);
        }
        _ => {}
    }

    // Fix the check digit.
    let mut start = 0usize;
    while buf[start] != 0 && buf[start] <= b' ' {
        start += 1;
    }
    buf[start + 12] = b'0' + checkdig(&buf[start..], 13) as u8;
    buf[start + 13] = 0;

    if !valid && !magic {
        // eanbadcheck
        if weak {
            return Ok(Some(str2ean(&buf[start..]) | 1));
        }
        let err = if rcheck == u32::MAX {
            PgError::error(format!(
                "invalid {} number: \"{}\"",
                isn_name(accept),
                shown()
            ))
            .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
        } else {
            let rc = if rcheck == 10 {
                'X'
            } else {
                (b'0' + rcheck as u8) as char
            };
            PgError::error(format!(
                "invalid check digit for {} number: \"{}\", should be {rc}",
                isn_name(accept),
                shown()
            ))
            .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
        };
        return ereturn(escontext, None, err);
    }

    let mut result = str2ean(&buf[start..]);
    if !valid {
        result |= 1;
    }
    Ok(Some(result))
}

#[cfg(test)]
mod tests;
