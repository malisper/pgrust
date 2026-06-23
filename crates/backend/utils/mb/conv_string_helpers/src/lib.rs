//! Port of `src/backend/utils/mb/conv.c` (the generic encoding-conversion
//! helpers used by every per-encoding conversion module) and
//! `src/backend/utils/mb/stringinfo_mb.c` (`appendStringInfoStringQuoted`).
//!
//! The two character-comparison leaves of this unit (`wstrcmp.c` /
//! `wstrncmp.c`) are ported in their own crates `backend-utils-mb-wstrcmp` and
//! `backend-utils-mb-wstrncmp`; their functions are re-exported here so this
//! crate presents the complete unit surface.
//!
//! Every function keeps its C name and logic/branch-order/message-text/SQLSTATE
//! 1:1. In the C build the radix tree (`pg_mb_radix_tree`) carries raw
//! `*const uint16`/`*const uint32` char arrays; [`pg_mb_radix_tree`] owns its
//! `chars16`/`chars32` as `Vec`s, so this crate indexes them directly.
//!
//! `report_invalid_encoding`/`report_untranslatable_char` live in the as-yet
//! unported `utils/mb/mbutils.c`; they are reached through that owner's seam
//! crate (`backend-utils-mb-mbutils-seams`) and panic loudly until it lands.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

use pqformat::enlarge_string_info;
use utils_error::{elog, ereport, PgError, PgResult};
use mbutils_seams::{
    pg_mbcliplen, report_invalid_encoding, report_untranslatable_char,
};
use common_wchar::{
    pg_encoding_verifymbchar, pg_mule_mblen, pg_utf8_islegal, pg_utf_mblen_private,
};
use types_error::{ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_OUT_OF_MEMORY, ERROR};
use stringinfo::StringInfo;
use types_wchar::encoding::{pg_valid_encoding, pg_enc, PG_MULE_INTERNAL, PG_UTF8};
use types_wchar::{pg_local_to_utf_combined, pg_mb_radix_tree, pg_utf_to_local_combined};

pub use wstrcmp::pg_char_and_wchar_strcmp;
pub use wstrncmp::{
    pg_char_and_wchar_strncmp, pg_wchar_strlen, pg_wchar_strncmp,
};

const HIGHBIT: u8 = 0x80;

/// The result of a conversion driver: the produced bytes and the number of
/// *source* bytes consumed (`*converted` in the C `cstr`-tail protocol).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConversionResult {
    pub bytes: Vec<u8>,
    pub converted: i32,
}

impl ConversionResult {
    fn new(bytes: Vec<u8>, converted: usize) -> Self {
        Self {
            bytes,
            converted: converted as i32,
        }
    }
}

/// A per-encoding code-point conversion callback (`utf_local_conversion_func`).
pub type UtfLocalConversionFunc = fn(u32) -> u32;

/// `local2local()` (conv.c): translate a single-byte charset to another via a
/// 128-entry high-byte table.
pub fn local2local(
    src: &[u8],
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    tab: &[u8; 128],
    no_error: bool,
) -> PgResult<ConversionResult> {
    let mut dest = oom_safe_buf(src.len())?;

    for (pos, &byte) in src.iter().enumerate() {
        if byte == 0 {
            if no_error {
                return Ok(ConversionResult::new(dest, pos));
            }
            report_invalid_encoding::call(src_encoding, &src[pos..])?;
        }

        if !high_bit_set(byte) {
            dest.push(byte);
        } else {
            let converted = tab[(byte - HIGHBIT) as usize];
            if converted != 0 {
                dest.push(converted);
            } else {
                if no_error {
                    return Ok(ConversionResult::new(dest, pos));
                }
                report_untranslatable_char::call(src_encoding, dest_encoding, &src[pos..])?;
            }
        }
    }

    Ok(ConversionResult::new(dest, src.len()))
}

/// `latin2mic()` (conv.c): convert a Latin charset to mule-internal by prefixing
/// the lead-charset byte `lc`.
pub fn latin2mic(
    src: &[u8],
    lc: u8,
    encoding: pg_enc,
    no_error: bool,
) -> PgResult<ConversionResult> {
    let mut dest = oom_safe_buf(src.len().saturating_mul(2))?;

    for (pos, &byte) in src.iter().enumerate() {
        if byte == 0 {
            if no_error {
                return Ok(ConversionResult::new(dest, pos));
            }
            report_invalid_encoding::call(encoding, &src[pos..])?;
        }

        if high_bit_set(byte) {
            dest.push(lc);
        }
        dest.push(byte);
    }

    Ok(ConversionResult::new(dest, src.len()))
}

/// `mic2latin()` (conv.c): strip a matching mule lead-charset prefix.
pub fn mic2latin(
    src: &[u8],
    lc: u8,
    encoding: pg_enc,
    no_error: bool,
) -> PgResult<ConversionResult> {
    let mut dest = oom_safe_buf(src.len())?;
    let mut pos = 0;

    while pos < src.len() {
        let byte = src[pos];
        if byte == 0 {
            if no_error {
                break;
            }
            report_invalid_encoding::call(PG_MULE_INTERNAL, &src[pos..])?;
        }

        if !high_bit_set(byte) {
            dest.push(byte);
            pos += 1;
            continue;
        }

        let char_len = pg_mule_mblen(&src[pos..]).unwrap_or(0) as usize;
        if src.len() - pos < char_len {
            if no_error {
                break;
            }
            report_invalid_encoding::call(PG_MULE_INTERNAL, &src[pos..])?;
        }

        if char_len != 2 || byte != lc || !high_bit_set(src[pos + 1]) {
            if no_error {
                break;
            }
            report_untranslatable_char::call(PG_MULE_INTERNAL, encoding, &src[pos..])?;
        }

        dest.push(src[pos + 1]);
        pos += 2;
    }

    Ok(ConversionResult::new(dest, pos))
}

/// `latin2mic_with_table()` (conv.c).
pub fn latin2mic_with_table(
    src: &[u8],
    lc: u8,
    encoding: pg_enc,
    tab: &[u8; 128],
    no_error: bool,
) -> PgResult<ConversionResult> {
    let mut dest = oom_safe_buf(src.len().saturating_mul(2))?;

    for (pos, &byte) in src.iter().enumerate() {
        if byte == 0 {
            if no_error {
                return Ok(ConversionResult::new(dest, pos));
            }
            report_invalid_encoding::call(encoding, &src[pos..])?;
        }

        if !high_bit_set(byte) {
            dest.push(byte);
        } else {
            let converted = tab[(byte - HIGHBIT) as usize];
            if converted != 0 {
                dest.push(lc);
                dest.push(converted);
            } else {
                if no_error {
                    return Ok(ConversionResult::new(dest, pos));
                }
                report_untranslatable_char::call(encoding, PG_MULE_INTERNAL, &src[pos..])?;
            }
        }
    }

    Ok(ConversionResult::new(dest, src.len()))
}

/// `mic2latin_with_table()` (conv.c).
pub fn mic2latin_with_table(
    src: &[u8],
    lc: u8,
    encoding: pg_enc,
    tab: &[u8; 128],
    no_error: bool,
) -> PgResult<ConversionResult> {
    let mut dest = oom_safe_buf(src.len())?;
    let mut pos = 0;

    while pos < src.len() {
        let byte = src[pos];
        if byte == 0 {
            if no_error {
                break;
            }
            report_invalid_encoding::call(PG_MULE_INTERNAL, &src[pos..])?;
        }

        if !high_bit_set(byte) {
            dest.push(byte);
            pos += 1;
            continue;
        }

        let char_len = pg_mule_mblen(&src[pos..]).unwrap_or(0) as usize;
        if src.len() - pos < char_len {
            if no_error {
                break;
            }
            report_invalid_encoding::call(PG_MULE_INTERNAL, &src[pos..])?;
        }

        let converted = if char_len == 2 && byte == lc && high_bit_set(src[pos + 1]) {
            tab[(src[pos + 1] - HIGHBIT) as usize]
        } else {
            0
        };

        if converted == 0 {
            if no_error {
                break;
            }
            report_untranslatable_char::call(PG_MULE_INTERNAL, encoding, &src[pos..])?;
        }

        dest.push(converted);
        pos += 2;
    }

    Ok(ConversionResult::new(dest, pos))
}

/// `UtfToLocal()` (conv.c): convert UTF-8 to a local encoding via a radix tree,
/// a combined-character map and/or a callback.
pub fn UtfToLocal(
    utf: &[u8],
    map: Option<&pg_mb_radix_tree>,
    cmap: &[pg_utf_to_local_combined],
    conv_func: Option<UtfLocalConversionFunc>,
    encoding: pg_enc,
    no_error: bool,
) -> PgResult<ConversionResult> {
    validate_encoding(encoding)?;

    let mut dest = oom_safe_buf(utf.len())?;
    let mut pos = 0;
    // When a break path needs to report a tail at a position/length that the
    // generic post-loop report (which derives length from utf.len() - pos)
    // cannot express, it records the exact slice here. Mirrors C's combined-map
    // "need more data" break, which restores utf = first-char-start but leaves
    // len = len_save - l_save (conv.c:583/683-684).
    let mut tail_report: Option<&[u8]> = None;

    while pos < utf.len() {
        let start = pos;
        let mut current_len = utf_char_len_or_break(utf, pos);
        if current_len == 0 || pos + current_len > utf.len() {
            break;
        }
        if !pg_utf8_islegal(&utf[pos..pos + current_len]) {
            break;
        }

        if current_len == 1 {
            dest.push(utf[pos]);
            pos += 1;
            continue;
        }

        let first_code = collect_coded_char(&utf[pos..pos + current_len], "UtfToLocal")?;
        pos += current_len;

        if !cmap.is_empty() && utf.len() - start > current_len {
            let second_start = pos;
            // C computes l = pg_utf_mblen(utf) for the second char (conv.c:585).
            // pg_utf_mblen never returns 0 (pg_utf_mblen('\0') == 1), so unlike
            // the first-char site we must NOT use utf_char_len_or_break here:
            // its '\0'-returns-0 special-case would wrongly abandon the
            // already-decoded first char. We replicate C: treat '\0' as a legal
            // 1-byte char so the l>1 combined-test below is simply skipped and
            // the first char falls through to the ordinary map.
            let second_len = pg_utf_mblen_private(&utf[second_start..]).unwrap_or(0) as usize;
            if second_start + second_len > utf.len() {
                // C: len < l -> "need more data"; utf -= l_save (back to first
                // char), break with len = len_save - l_save (= utf.len() -
                // second_start). The post-loop report then shows that tail.
                tail_report = Some(&utf[start..start + (utf.len() - second_start)]);
                pos = start;
                break;
            }

            if !pg_utf8_islegal(&utf[second_start..second_start + second_len]) {
                if !no_error {
                    report_invalid_encoding::call(PG_UTF8, &utf[second_start..])?;
                }
                pos = start;
                break;
            }

            if second_len > 1 {
                let second_code = collect_coded_char(
                    &utf[second_start..second_start + second_len],
                    "UtfToLocal",
                )?;
                if let Some(entry) = cmap
                    .binary_search_by(|entry| {
                        (entry.utf1, entry.utf2).cmp(&(first_code, second_code))
                    })
                    .ok()
                    .map(|idx| &cmap[idx])
                {
                    store_coded_char(&mut dest, entry.code);
                    pos = second_start + second_len;
                    continue;
                }
            }

            pos = second_start;
            current_len = second_start - start;
        }

        if let Some(tree) = map {
            let converted = pg_mb_radix_conv(tree, &utf[start..start + current_len]);
            if converted != 0 {
                store_coded_char(&mut dest, converted);
                continue;
            }
        }

        if let Some(conv_func) = conv_func {
            let converted = conv_func(first_code);
            if converted != 0 {
                store_coded_char(&mut dest, converted);
                continue;
            }
        }

        pos = start;
        if no_error {
            break;
        }
        report_untranslatable_char::call(PG_UTF8, encoding, &utf[pos..])?;
    }

    if pos < utf.len() && !no_error {
        // C reports report_invalid_encoding(PG_UTF8, utf, len) post-loop. For
        // the combined-map "need more data" break, C's utf/len pair differs
        // from (&utf[pos..]): utf = first-char-start but len = len_save -
        // l_save (a shorter tail). Honor that exact slice when recorded.
        let report = tail_report.unwrap_or(&utf[pos..]);
        report_invalid_encoding::call(PG_UTF8, report)?;
    }

    Ok(ConversionResult::new(dest, pos))
}

/// `LocalToUtf()` (conv.c): convert a local encoding to UTF-8.
pub fn LocalToUtf(
    iso: &[u8],
    map: Option<&pg_mb_radix_tree>,
    cmap: &[pg_local_to_utf_combined],
    conv_func: Option<UtfLocalConversionFunc>,
    encoding: pg_enc,
    no_error: bool,
) -> PgResult<ConversionResult> {
    validate_encoding(encoding)?;

    let mut dest = oom_safe_buf(iso.len().saturating_mul(2))?;
    let mut pos = 0;

    while pos < iso.len() {
        let start = pos;
        let byte = iso[pos];
        if byte == 0 {
            break;
        }

        if !high_bit_set(byte) {
            dest.push(byte);
            pos += 1;
            continue;
        }

        let char_len = pg_encoding_verifymbchar(encoding, &iso[pos..]);
        if char_len < 0 {
            break;
        }

        let char_len = char_len as usize;
        let code = collect_coded_char(&iso[pos..pos + char_len], "LocalToUtf")?;
        pos += char_len;

        if let Some(tree) = map {
            let converted = pg_mb_radix_conv(tree, &iso[start..start + char_len]);
            if converted != 0 {
                store_coded_char(&mut dest, converted);
                continue;
            }

            if let Some(entry) = cmap
                .binary_search_by(|entry| entry.code.cmp(&code))
                .ok()
                .map(|idx| &cmap[idx])
            {
                store_coded_char(&mut dest, entry.utf1);
                store_coded_char(&mut dest, entry.utf2);
                continue;
            }
        }

        if let Some(conv_func) = conv_func {
            let converted = conv_func(code);
            if converted != 0 {
                store_coded_char(&mut dest, converted);
                continue;
            }
        }

        pos = start;
        if no_error {
            break;
        }
        report_untranslatable_char::call(encoding, PG_UTF8, &iso[pos..])?;
    }

    if pos < iso.len() && !no_error {
        report_invalid_encoding::call(encoding, &iso[pos..])?;
    }

    Ok(ConversionResult::new(dest, pos))
}

/// `pg_mb_radix_conv()` (conv.c): radix-tree lookup of a 1..4-byte code.
pub fn pg_mb_radix_conv(tree: &pg_mb_radix_tree, input: &[u8]) -> u32 {
    let [b1, b2, b3, b4] = padded_code_bytes(input);
    match input.len() {
        4 => radix_lookup4(tree, b1, b2, b3, b4),
        3 => radix_lookup3(tree, b2, b3, b4),
        2 => radix_lookup2(tree, b3, b4),
        1 => radix_lookup1(tree, b4),
        _ => 0,
    }
}

/// `appendStringInfoStringQuoted` (`stringinfo_mb.c`): append up to `maxlen`
/// bytes from `s` to `str`, or the whole string if `maxlen < 0`, adding single
/// quotes around it and doubling all embedded single quotes. An ellipsis marks
/// a truncated copy.
///
/// `s` is the raw byte run (C `const char *`, NUL-terminated); the trailing NUL
/// is not part of `s` here. The clip length for the `maxlen` case uses the
/// `pg_mbcliplen` seam (owned by `utils/mb/mbutils.c`).
pub fn appendStringInfoStringQuoted(
    str: &mut StringInfo<'_>,
    s: &[u8],
    maxlen: i32,
) -> PgResult<()> {
    let slen = s.len();

    // C: maxlen >= 0 && maxlen < slen -> clip to pg_mbcliplen(s, slen, maxlen)
    // and flag an ellipsis. `copy` in C is a pnstrdup; here we just bound the
    // working slice and carry the ellipsis flag.
    let (work, ellipsis) = if maxlen >= 0 && (maxlen as usize) < slen {
        let finallen = pg_mbcliplen::call(s, slen as i32, maxlen);
        (&s[..finallen as usize], true)
    } else {
        (s, false)
    };

    append_string_info_char(str, b'\'')?;

    // Walk chunks delimited by each embedded '\'', emitting the chunk including
    // the found quote, then doubling that quote by starting the next chunk on it
    // (C's chunk_copy_start = chunk_end; chunk_search_start = chunk_end + 1).
    let mut chunk_copy_start = 0usize;
    let mut chunk_search_start = 0usize;
    while let Some(rel) = work[chunk_search_start..].iter().position(|&b| b == b'\'') {
        let chunk_end = chunk_search_start + rel;
        // copy including the found delimiting '
        append_binary_string_info_nt(str, &work[chunk_copy_start..=chunk_end])?;
        // in order to double it, include this ' into the next chunk as well
        chunk_copy_start = chunk_end;
        chunk_search_start = chunk_end + 1;
    }

    // copy the last chunk and terminate
    append_binary_string_info_nt(str, &work[chunk_copy_start..])?;
    if ellipsis {
        append_binary_string_info_nt(str, b"...'")?;
    } else {
        append_binary_string_info_nt(str, b"'")?;
    }

    Ok(())
}

/// `check_encoding_conversion_args` (`utils/mb/mbutils.c`): validate the
/// source/destination encodings handed to a conversion procedure. The
/// `expected_*` parameters may be `-1` ("wildcard", any valid encoding); a
/// `< 0` length is rejected. Encoding names for the mismatch messages come from
/// `pg_encoding_to_char` (encnames.c). Logic/branch-order/message-text 1:1 with
/// the C, which raises plain `elog(ERROR, ...)` (internal error, no SQLSTATE).
pub fn check_encoding_conversion_args(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    len: i32,
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
                encnames_seams::pg_encoding_to_char::call(expected_src_encoding),
                encnames_seams::pg_encoding_to_char::call(src_encoding)
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
                encnames_seams::pg_encoding_to_char::call(expected_dest_encoding),
                encnames_seams::pg_encoding_to_char::call(dest_encoding)
            ),
        );
    }
    if len < 0 {
        return elog(ERROR, "encoding conversion length must not be negative");
    }
    Ok(())
}

fn validate_encoding(encoding: pg_enc) -> PgResult<()> {
    if pg_valid_encoding(encoding) {
        Ok(())
    } else {
        Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!("invalid encoding number: {encoding}"))
            .into_error())
    }
}

fn collect_coded_char(bytes: &[u8], _function_name: &'static str) -> PgResult<u32> {
    match bytes.len() {
        1..=4 => Ok(bytes
            .iter()
            .fold(0, |code, &byte| (code << 8) | byte as u32)),
        len => elog(ERROR, format!("unsupported character length {len}")).map(|()| 0),
    }
}

fn store_coded_char(dest: &mut Vec<u8>, code: u32) {
    if code & 0xff00_0000 != 0 {
        dest.push((code >> 24) as u8);
    }
    if code & 0x00ff_0000 != 0 {
        dest.push((code >> 16) as u8);
    }
    if code & 0x0000_ff00 != 0 {
        dest.push((code >> 8) as u8);
    }
    if code & 0x0000_00ff != 0 {
        dest.push(code as u8);
    }
}

fn utf_char_len_or_break(bytes: &[u8], pos: usize) -> usize {
    if bytes[pos] == 0 {
        0
    } else {
        pg_utf_mblen_private(&bytes[pos..]).unwrap_or(0) as usize
    }
}

fn padded_code_bytes(input: &[u8]) -> [u8; 4] {
    let mut bytes = [0; 4];
    let offset = 4usize.saturating_sub(input.len());
    for (idx, byte) in input.iter().take(4).enumerate() {
        bytes[offset + idx] = *byte;
    }
    bytes
}

fn radix_lookup4(tree: &pg_mb_radix_tree, b1: u8, b2: u8, b3: u8, b4: u8) -> u32 {
    if b1 < tree.b4_1_lower
        || b1 > tree.b4_1_upper
        || b2 < tree.b4_2_lower
        || b2 > tree.b4_2_upper
        || b3 < tree.b4_3_lower
        || b3 > tree.b4_3_upper
        || b4 < tree.b4_4_lower
        || b4 > tree.b4_4_upper
    {
        return 0;
    }

    let idx = tree.b4root;
    let idx = radix_get(tree, b1 as u32 + idx - tree.b4_1_lower as u32);
    let idx = radix_get(tree, b2 as u32 + idx - tree.b4_2_lower as u32);
    let idx = radix_get(tree, b3 as u32 + idx - tree.b4_3_lower as u32);
    radix_get(tree, b4 as u32 + idx - tree.b4_4_lower as u32)
}

fn radix_lookup3(tree: &pg_mb_radix_tree, b2: u8, b3: u8, b4: u8) -> u32 {
    if b2 < tree.b3_1_lower
        || b2 > tree.b3_1_upper
        || b3 < tree.b3_2_lower
        || b3 > tree.b3_2_upper
        || b4 < tree.b3_3_lower
        || b4 > tree.b3_3_upper
    {
        return 0;
    }

    let idx = tree.b3root;
    let idx = radix_get(tree, b2 as u32 + idx - tree.b3_1_lower as u32);
    let idx = radix_get(tree, b3 as u32 + idx - tree.b3_2_lower as u32);
    radix_get(tree, b4 as u32 + idx - tree.b3_3_lower as u32)
}

fn radix_lookup2(tree: &pg_mb_radix_tree, b3: u8, b4: u8) -> u32 {
    if b3 < tree.b2_1_lower
        || b3 > tree.b2_1_upper
        || b4 < tree.b2_2_lower
        || b4 > tree.b2_2_upper
    {
        return 0;
    }

    let idx = tree.b2root;
    let idx = radix_get(tree, b3 as u32 + idx - tree.b2_1_lower as u32);
    radix_get(tree, b4 as u32 + idx - tree.b2_2_lower as u32)
}

fn radix_lookup1(tree: &pg_mb_radix_tree, b4: u8) -> u32 {
    if b4 < tree.b1_lower || b4 > tree.b1_upper {
        return 0;
    }

    radix_get(tree, b4 as u32 + tree.b1root - tree.b1_lower as u32)
}

/// Index into the owned `chars32`/`chars16` array (`pg_mb_radix_tree.chars32`
/// is preferred; `chars16` is the 16-bit fallback). An out-of-range index
/// yields 0 (an unmapped code) -- matching how the C radix bounds never index
/// past the table.
fn radix_get(tree: &pg_mb_radix_tree, index: u32) -> u32 {
    let index = index as usize;
    if !tree.chars32.is_empty() {
        tree.chars32.get(index).copied().unwrap_or(0)
    } else if !tree.chars16.is_empty() {
        tree.chars16.get(index).copied().unwrap_or(0) as u32
    } else {
        0
    }
}

fn high_bit_set(byte: u8) -> bool {
    byte & HIGHBIT != 0
}

/// `palloc`-analog destination byte buffer with OOM-safe growth: reserves the
/// estimated `capacity` up front via `try_reserve` (the C drivers `palloc` a
/// worst-case `len * MAX_CONVERSION_GROWTH + 1` destination), surfacing a refusal
/// as a recoverable `ERRCODE_OUT_OF_MEMORY` error rather than aborting.
fn oom_safe_buf(capacity: usize) -> PgResult<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    buf.try_reserve(capacity).map_err(|_| oom_error())?;
    Ok(buf)
}

/// The recoverable OOM error for a `try_reserve` refusal.
fn oom_error() -> PgError {
    PgError::error("out of memory").with_sqlstate(ERRCODE_OUT_OF_MEMORY)
}

/// `appendStringInfoCharMacro(str, ch)` (`lib/stringinfo.h`): append a single
/// byte to the StringInfo. Growth (and the 1GB `MaxAllocSize` cap with its exact
/// error) is the `enlargeStringInfo` logic homed in `backend-libpq-pqformat`
/// until `common/stringinfo.c` is ported.
fn append_string_info_char(str: &mut StringInfo<'_>, ch: u8) -> PgResult<()> {
    enlarge_string_info(str, 1)?;
    str.data.push(ch);
    Ok(())
}

/// `appendBinaryStringInfoNT(str, data, datalen)` (`common/stringinfo.c`):
/// append `data` to the StringInfo without re-NUL-terminating (this repo's
/// StringInfo stores no trailing-NUL sentinel; see types-stringinfo).
fn append_binary_string_info_nt(str: &mut StringInfo<'_>, data: &[u8]) -> PgResult<()> {
    enlarge_string_info(str, data.len())?;
    str.data.extend_from_slice(data);
    Ok(())
}

/// Wires this crate's seams. It declares none of its own, so this is a no-op
/// kept for the uniform `seams-init` startup convention.
pub fn init_seams() {}

// ===========================================================================
// fmgr-builtin shim adapter for ported encoding-conversion procedures.
//
// Every per-encoding conversion module's `PG_FUNCTION_ARGS` entry point has the
// uniform C signature:
//
//   Datum conv(PG_FUNCTION_ARGS)
//   {
//       int      src_encoding = PG_GETARG_INT32(0);
//       int      dest_encoding = PG_GETARG_INT32(1);
//       unsigned char *src = (unsigned char *) PG_GETARG_CSTRING(2);
//       unsigned char *dest = (unsigned char *) PG_GETARG_CSTRING(3);
//       int      len = PG_GETARG_INT32(4);
//       bool     noError = PG_GETARG_BOOL(5);
//       int      converted = <do the conversion, writing into dest>;
//       PG_RETURN_INT32(converted);
//   }
//
// The ported Rust bodies are all `fn(pg_enc, pg_enc, &[u8], bool) ->
// PgResult<ConversionResult>` (the produced bytes + source-bytes-consumed).
// `make_conversion_builtin` wraps one of those into the bare-`Datum`
// `PGFunction` ABI the function manager dispatches, so the conversion proc OID
// resolves to the in-process Rust body (no `dlopen` of a `$libdir/utf8_and_*`
// shared library). This mirrors the `convert_via_proc_counted_seam` packing in
// `backend-utils-fmgr-core` exactly:
//   * src_encoding / dest_encoding arrive as by-value int4 args (0, 1),
//   * the source `cstring` arrives in `ref_args[2]`,
//   * the destination `cstring` is written back into `ref_args[3]`,
//   * the int4 return is the count of source bytes consumed.
// ===========================================================================

/// The uniform ported conversion-procedure body signature.
pub type ConversionFn = fn(pg_enc, pg_enc, &[u8], bool) -> PgResult<ConversionResult>;

/// Build the `BuiltinFunction` (C `fmgr_builtins[]` row) for a ported encoding
/// conversion procedure, dispatching the bare-`Datum` `PGFunction` ABI into
/// `conv`. `foid`/`name` are transcribed from `pg_proc.dat`; all conversion
/// procedures are `nargs = 6`, strict, non-set-returning.
pub fn make_conversion_builtin(
    foid: u32,
    name: &str,
    conv: ConversionFn,
) -> (fmgr::BuiltinFunction, fmgr::PgFnNative) {
    // The conversion fn is stored in the closure via a generated dispatcher.
    // Since `PGFunction` is a plain `fn` pointer (no captured environment), the
    // conversion body must be threaded through a per-OID wrapper. We instead key
    // the dispatch on a thread-local registry the wrapper consults by fn_oid.
    register_conversion_body(foid, conv);
    (
        fmgr::BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs: 6,
            strict: true,
            retset: false,
            func: None,
        },
        conversion_dispatch as fmgr::PgFnNative,
    )
}

thread_local! {
    static CONVERSION_BODIES: core::cell::RefCell<
        alloc_map::Map,
    > = core::cell::RefCell::new(alloc_map::Map::new());
}

/// Tiny OID→ConversionFn map (std `HashMap` wrapper kept local so the crate's
/// no_std-ish surface is unaffected). Per-backend (thread_local), mirroring the
/// fmgr builtin registry's own backend-private model.
mod alloc_map {
    use super::ConversionFn;
    use std::collections::HashMap;
    pub struct Map(HashMap<u32, ConversionFn>);
    impl Map {
        pub fn new() -> Self {
            Map(HashMap::new())
        }
        pub fn insert(&mut self, k: u32, v: ConversionFn) {
            self.0.insert(k, v);
        }
        pub fn get(&self, k: u32) -> Option<ConversionFn> {
            self.0.get(&k).copied()
        }
    }
}

fn register_conversion_body(foid: u32, conv: ConversionFn) {
    CONVERSION_BODIES.with(|m| m.borrow_mut().insert(foid, conv));
}

/// The shared `PGFunction` dispatcher: look up the conversion body by the
/// resolved `fn_oid` and run it over the fmgr boundary.
fn conversion_dispatch(
    fcinfo: &mut fmgr::FunctionCallInfoBaseData,
) -> PgResult<datum::Datum> {
    let foid = fcinfo.flinfo.as_ref().map(|f| f.fn_oid).unwrap_or(0);
    let conv = match CONVERSION_BODIES.with(|m| m.borrow().get(foid)) {
        Some(c) => c,
        None => {
            return Err(PgError::error(format!(
                "encoding conversion procedure {foid} is not registered"
            )))
        }
    };

    // PG_GETARG_INT32(0) / PG_GETARG_INT32(1).
    let src_encoding = fcinfo
        .arg(0)
        .expect("conversion proc: missing src_encoding arg")
        .value
        .as_i32();
    let dest_encoding = fcinfo
        .arg(1)
        .expect("conversion proc: missing dest_encoding arg")
        .value
        .as_i32();

    // PG_GETARG_CSTRING(2): the source bytes arrive on the raw byte lane
    // (`RefPayload::Varlena`). A C conversion-proc `cstring` is an arbitrary
    // NUL-terminated byte buffer in the *source* encoding — not necessarily
    // valid UTF-8 — so `convert_via_proc_counted_seam` carries it as raw bytes;
    // read them back the same way (mirroring the euc-* fc_ adapters).
    let src: Vec<u8> = match fcinfo.ref_args.get(2).and_then(|r| r.as_ref()) {
        Some(p) => p.as_varlena().map(|b| b.to_vec()).unwrap_or_default(),
        None => Vec::new(),
    };

    let no_error = fcinfo
        .arg(5)
        .map(|a| a.value.as_bool())
        .unwrap_or(false);

    let result = conv(
        src_encoding as pg_enc,
        dest_encoding as pg_enc,
        &src,
        no_error,
    )?;

    // Write the converted raw output bytes back into the ref_args[3] destination
    // referent on the same raw byte lane the seam recovers them from. The result
    // bytes are in the *destination* encoding and likewise need not be valid
    // UTF-8, so they cross as raw bytes (`RefPayload::Varlena`), not a String.
    if let Some(slot) = fcinfo.ref_args.get_mut(3) {
        *slot = Some(fmgr::boundary::RefPayload::Varlena(result.bytes));
    }

    // PG_RETURN_INT32(converted).
    Ok(datum::Datum::from_i32(result.converted))
}

#[cfg(test)]
mod tests;
