//! backup_label / tablespace_map readers (xlogrecovery.c). The fscanf-based
//! C parsers are matched field-for-field; format deviations are FATAL like C.
//!
//! fscanf modeling (C read_backup_label, xlogrecovery.c):
//! - a literal non-space byte in the format must match the next input byte
//!   exactly (no whitespace skip);
//! - a space in the format is a whitespace directive: it skips a run of ZERO
//!   or more C-locale isspace bytes (so "(file  X" with two spaces matches,
//!   and so does "(fileX" with none);
//! - %X / %u / %s each skip leading C-locale whitespace themselves; %s stops
//!   at C-locale whitespace (a Unicode space such as NBSP is part of the
//!   token); a numeric conversion with no digits is a matching failure
//!   (fscanf returns a short count).
//! - %X / %u convert with strtoul semantics: optional +/- sign ('-' wraps the
//!   magnitude), for %X an optional 0x/0X prefix, digits; the value is
//!   assigned to a uint32 with truncation, and overflow saturates to
//!   ULONG_MAX first (glibc ERANGE behavior) — so it truncates to
//!   0xFFFFFFFF.
//!
//! The file is one byte stream, as for C's fscanf calls: whitespace
//! directives and the leading skip of %X/%u/%s cross newlines
//! ("BACKUP METHOD:\nstreamed" matches), and the optional trailer fields are
//! tried in C's fixed order (BACKUP METHOD, BACKUP FROM, START TIME, LABEL,
//! START TIMELINE, INCREMENTAL FROM LSN) where a literal mismatch consumes
//! the matched prefix and pushes back only the mismatching byte — so an
//! out-of-order trailer silently loses fields exactly as it does in C
//! (xlogrecovery.c:1298-1358; a label starting with "START TIMELINE" leaves
//! the stream at "LINE: ..." after "START TIME" partially matches).
//!
//! Residual divergence from glibc (deliberately out of scope): glibc scanf
//! treats "0x" NOT followed by a hex digit as a matching failure
//! mid-conversion; we follow strtoul instead and parse the "0" (value 0),
//! leaving the 'x' as trailing input.

use elog::{elog, ereport};
use pg_string::isspace_c_locale;
use types_core::{TimeLineID, XLogRecPtr};
use types_error::{PgResult, DEBUG1, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, FATAL};

use crate::{data_path, loc, InvalidXLogRecPtr, BACKUP_LABEL_FILE, TABLESPACE_MAP};

#[cfg_attr(test, derive(Debug))]
pub(crate) struct BackupLabel {
    pub checkpoint_loc: XLogRecPtr,
    pub backup_label_tli: TimeLineID,
    pub backup_end_required: bool,
    pub backup_from_standby: bool,
    pub redo_start_lsn: XLogRecPtr,
    pub redo_start_tli: TimeLineID,
}

// xlogrecovery.c:1279/1286/1345/1357 and 1433/1442/1462: every "invalid data
// in file" FATAL carries errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE).
fn invalid_data<T>(file: &str, func: &'static str) -> PgResult<T> {
    ereport(FATAL)
        .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .errmsg(format!("invalid data in file \"{file}\""))
        .finish(loc(func))?;
    unreachable!()
}

// xlogrecovery.c:1262-1266 / 1466-1469: a read failure other than ENOENT is
// ereport(FATAL, errcode_for_file_access(), "could not read file \"%s\": %m").
fn could_not_read<T>(file: &str, e: &std::io::Error, func: &'static str) -> PgResult<T> {
    ereport(FATAL)
        .with_saved_errno(e.raw_os_error().unwrap_or(0))
        .errcode_for_file_access()
        .errmsg(format!("could not read file \"{file}\": %m"))
        .finish(loc(func))?;
    unreachable!()
}

/// fscanf whitespace directive: skip zero or more C-locale isspace bytes
/// (newlines included).
fn skip_ws(mut s: &[u8]) -> &[u8] {
    while let [b, rest @ ..] = s {
        if !isspace_c_locale(*b) {
            break;
        }
        s = rest;
    }
    s
}

/// fscanf format-literal matcher. A space in `fmt` is a whitespace directive
/// (skips zero or more C-locale isspace bytes); any other byte in `fmt` must
/// match the next input byte exactly. `Ok(rest)` on a full match; on a
/// mismatch `Err(rest)` positioned AFTER the matched prefix — like a stdio
/// stream, where the consumed bytes are gone and only the mismatching byte
/// is pushed back.
fn scan_literal<'a>(mut s: &'a [u8], fmt: &str) -> Result<&'a [u8], &'a [u8]> {
    for &f in fmt.as_bytes() {
        if f == b' ' {
            s = skip_ws(s);
        } else {
            match s {
                [b, rest @ ..] if *b == f => s = rest,
                _ => return Err(s),
            }
        }
    }
    Ok(s)
}

/// fscanf `%<width>[^\n]`: no leading whitespace skip; up to `width` bytes
/// other than '\n'. Zero bytes => matching failure (`None`).
fn scan_line_field(s: &[u8], width: usize) -> Option<(&[u8], &[u8])> {
    let n = s.iter().take(width).take_while(|&&b| b != b'\n').count();
    if n == 0 {
        return None;
    }
    Some((&s[..n], &s[n..]))
}

/// fscanf %X (base 16) / %u (base 10) with an optional field width, i.e.
/// strtoul: skip leading C-locale whitespace, optional single +/- ('-' wraps
/// the magnitude modulo 2^64, like glibc, without ERANGE), for base 16 an
/// optional 0x/0X prefix when a hex digit follows within the width, then
/// digits. The width bounds the bytes consumed after the whitespace skip,
/// including sign and prefix. Overflow saturates to ULONG_MAX with
/// `range_err` (glibc ERANGE). No digits => matching failure => `None`.
/// Returns `(value, range_err, rest)`.
fn scan_uint(s: &[u8], base: u64, width: usize) -> Option<(u64, bool, &[u8])> {
    let mut i = 0;
    while i < s.len() && isspace_c_locale(s[i]) {
        i += 1;
    }
    let limit = i.saturating_add(width);
    let mut neg = false;
    match s.get(i) {
        Some(b'-') if i < limit => {
            neg = true;
            i += 1;
        }
        Some(b'+') if i < limit => i += 1,
        _ => {}
    }
    // Optional 0x/0X for hex, only when an in-width hex digit follows
    // (strtoul: "0x" with no hex digit after it parses as just "0").
    if base == 16
        && s.get(i) == Some(&b'0')
        && matches!(s.get(i + 1), Some(b'x') | Some(b'X'))
        && i + 2 < limit // room within the width for a digit after "0x"
        && s.get(i + 2).is_some_and(|b| b.is_ascii_hexdigit())
    {
        i += 2;
    }
    let digits_start = i;
    let mut acc: u64 = 0;
    let mut range_err = false;
    while i < s.len() && i < limit {
        let d = match s[i] {
            b @ b'0'..=b'9' => u64::from(b - b'0'),
            b @ b'a'..=b'f' if base == 16 => u64::from(b - b'a' + 10),
            b @ b'A'..=b'F' if base == 16 => u64::from(b - b'A' + 10),
            _ => break,
        };
        if !range_err {
            match acc.checked_mul(base).and_then(|v| v.checked_add(d)) {
                Some(v) => acc = v,
                None => range_err = true,
            }
        }
        i += 1;
    }
    if i == digits_start {
        return None;
    }
    let value = if range_err {
        u64::MAX
    } else if neg {
        acc.wrapping_neg()
    } else {
        acc
    };
    Some((value, range_err, &s[i..]))
}

/// fscanf %s with a field width: skip leading C-locale whitespace, then read
/// up to `max` non-whitespace bytes. Zero bytes read => matching failure.
fn scan_token(s: &[u8], max: usize) -> Option<(&[u8], &[u8])> {
    let mut i = 0;
    while i < s.len() && isspace_c_locale(s[i]) {
        i += 1;
    }
    let start = i;
    while i < s.len() && i - start < max && !isspace_c_locale(s[i]) {
        i += 1;
    }
    if i == start {
        return None;
    }
    Some((&s[start..i], &s[i..]))
}

#[derive(Debug, PartialEq, Eq)]
enum LabelError {
    /// "invalid data in file" FATAL.
    Invalid,
    /// START TIMELINE cross-check failed (FATAL with errdetail).
    TimelineMismatch { file: u32, walseg: u32 },
    /// INCREMENTAL FROM LSN present (FATAL, pg_combinebackup hint).
    Incremental,
}

fn parse_backup_label_content(content: &[u8]) -> Result<BackupLabel, LabelError> {
    let mut out = BackupLabel {
        checkpoint_loc: InvalidXLogRecPtr,
        backup_label_tli: 0,
        backup_end_required: false,
        backup_from_standby: false,
        redo_start_lsn: InvalidXLogRecPtr,
        redo_start_tli: 0,
    };
    // One stream, as for C's successive fscanf calls on the same FILE.
    let s = content;

    // "START WAL LOCATION: %X/%X (file %08X%16s)%c", 5 fields, ch == '\n'
    // (a '\r' there, or EOF, is FATAL).
    let rest = scan_literal(s, "START WAL LOCATION: ").map_err(|_| LabelError::Invalid)?;
    let (hi, _, rest) = scan_uint(rest, 16, usize::MAX).ok_or(LabelError::Invalid)?;
    let rest = scan_literal(rest, "/").map_err(|_| LabelError::Invalid)?;
    let (lo, _, rest) = scan_uint(rest, 16, usize::MAX).ok_or(LabelError::Invalid)?;
    let rest = scan_literal(rest, " (file ").map_err(|_| LabelError::Invalid)?;
    let (tli, _, rest) = scan_uint(rest, 16, 8).ok_or(LabelError::Invalid)?;
    let tli_from_walseg = tli as u32;
    let (_fname, rest) = scan_token(rest, 16).ok_or(LabelError::Invalid)?;
    let rest = scan_literal(rest, ")").map_err(|_| LabelError::Invalid)?;
    let [b'\n', rest @ ..] = rest else {
        return Err(LabelError::Invalid);
    };
    out.redo_start_lsn = (u64::from(hi as u32)) << 32 | u64::from(lo as u32);
    out.redo_start_tli = tli_from_walseg;
    out.backup_label_tli = tli_from_walseg;

    // "CHECKPOINT LOCATION: %X/%X%c", 3 fields, ch == '\n'.
    let rest = scan_literal(rest, "CHECKPOINT LOCATION: ").map_err(|_| LabelError::Invalid)?;
    let (hi, _, rest) = scan_uint(rest, 16, usize::MAX).ok_or(LabelError::Invalid)?;
    let rest = scan_literal(rest, "/").map_err(|_| LabelError::Invalid)?;
    let (lo, _, rest) = scan_uint(rest, 16, usize::MAX).ok_or(LabelError::Invalid)?;
    let [b'\n', rest @ ..] = rest else {
        return Err(LabelError::Invalid);
    };
    out.checkpoint_loc = (u64::from(hi as u32)) << 32 | u64::from(lo as u32);

    // The optional trailer, one fscanf per field in C's fixed order. Each
    // call resumes where the previous one left the stream: a failed literal
    // has consumed its matched prefix; a conversion's leading whitespace skip
    // is consumed even when the conversion then fails; the trailing "\n"
    // directive (reached only after a successful conversion) skips any
    // whitespace run.
    let mut s = rest;

    // "BACKUP METHOD: %19s\n": the FIRST whitespace-delimited token (<= 19
    // bytes) is compared, so "streamed junk" still sets backupEndRequired.
    s = match scan_literal(s, "BACKUP METHOD: ") {
        Ok(r) => match scan_token(r, 19) {
            Some((tok, r)) => {
                if tok == b"streamed" {
                    out.backup_end_required = true;
                }
                skip_ws(r)
            }
            None => skip_ws(r),
        },
        Err(r) => r,
    };

    // "BACKUP FROM: %19s\n".
    s = match scan_literal(s, "BACKUP FROM: ") {
        Ok(r) => match scan_token(r, 19) {
            Some((tok, r)) => {
                if tok == b"standby" {
                    out.backup_from_standby = true;
                }
                skip_ws(r)
            }
            None => skip_ws(r),
        },
        Err(r) => r,
    };

    // "START TIME: %127[^\n]\n" / "LABEL: %1023[^\n]\n": not mandatory;
    // present values are logged at DEBUG1 (errmsg_internal).
    s = match scan_literal(s, "START TIME: ") {
        Ok(r) => match scan_line_field(r, 127) {
            Some((value, r)) => {
                let _ = elog(
                    DEBUG1,
                    format!(
                        "backup time {} in file \"{BACKUP_LABEL_FILE}\"",
                        String::from_utf8_lossy(value)
                    ),
                );
                skip_ws(r)
            }
            None => r,
        },
        Err(r) => r,
    };
    s = match scan_literal(s, "LABEL: ") {
        Ok(r) => match scan_line_field(r, 1023) {
            Some((value, r)) => {
                let _ = elog(
                    DEBUG1,
                    format!(
                        "backup label {} in file \"{BACKUP_LABEL_FILE}\"",
                        String::from_utf8_lossy(value)
                    ),
                );
                skip_ws(r)
            }
            None => r,
        },
        Err(r) => r,
    };

    // "START TIMELINE: %u\n": strtoul base 10; trailing junk after the digits
    // is left unread by the conversion ("2junk" parses as 2 and still feeds
    // the cross-check). No digits => fscanf returns 0 and the whole check is
    // silently skipped, like C.
    s = match scan_literal(s, "START TIMELINE: ") {
        Ok(r) => match scan_uint(r, 10, usize::MAX) {
            Some((v, _, r)) => {
                let tli_from_file = v as u32;
                if tli_from_walseg != tli_from_file {
                    return Err(LabelError::TimelineMismatch {
                        file: tli_from_file,
                        walseg: tli_from_walseg,
                    });
                }
                let _ = elog(
                    DEBUG1,
                    format!("backup timeline {tli_from_file} in file \"{BACKUP_LABEL_FILE}\""),
                );
                skip_ws(r)
            }
            None => skip_ws(r),
        },
        Err(r) => r,
    };

    // "INCREMENTAL FROM LSN: %X/%X\n" > 0: at least the first %X must
    // convert; a prefix match with no hex digits is NOT the
    // incremental-backup FATAL.
    if let Ok(r) = scan_literal(s, "INCREMENTAL FROM LSN: ") {
        if scan_uint(r, 16, usize::MAX).is_some() {
            return Err(LabelError::Incremental);
        }
    }
    Ok(out)
}

pub(crate) fn read_backup_label() -> PgResult<Option<BackupLabel>> {
    let path = data_path(BACKUP_LABEL_FILE);
    // Raw bytes, as C's fscanf reads them.
    let content = match std::fs::read(&path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return could_not_read(BACKUP_LABEL_FILE, &e, "read_backup_label"),
    };
    match parse_backup_label_content(&content) {
        Ok(out) => Ok(Some(out)),
        Err(LabelError::Invalid) => invalid_data(BACKUP_LABEL_FILE, "read_backup_label"),
        Err(LabelError::TimelineMismatch { file, walseg }) => {
            ereport(FATAL)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!("invalid data in file \"{BACKUP_LABEL_FILE}\""))
                .errdetail(format!(
                    "Timeline ID parsed is {file}, but expected {walseg}."
                ))
                .finish(loc("read_backup_label"))?;
            unreachable!()
        }
        Err(LabelError::Incremental) => {
            ereport(FATAL)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg("this is an incremental backup, not a data directory")
                .errhint("Use pg_combinebackup to reconstruct a valid data directory.")
                .finish(loc("read_backup_label"))?;
            unreachable!()
        }
    }
}

pub(crate) struct TablespaceInfo {
    pub oid: u32,
    pub path: Vec<u8>,
}

fn parse_tablespace_map_content(content: &[u8]) -> Result<Vec<TablespaceInfo>, ()> {
    let mut tablespaces = Vec::new();
    let mut buf = Vec::new();
    let mut was_backslash = false;
    for &ch in content {
        if !was_backslash && (ch == b'\n' || ch == b'\r') {
            if buf.is_empty() {
                continue;
            }
            let line = std::mem::take(&mut buf);
            let Some(sp) = line.iter().position(|&b| b == b' ') else {
                return Err(());
            };
            if sp < 1 || sp >= line.len() - 1 {
                return Err(());
            }
            // C: strtoul(str, &endp, 10) with `*endp != '\0' || errno ==
            // EINVAL || errno == ERANGE` as the reject test. strtoul skips
            // leading C-locale whitespace (a leading '\t' is fine — only a
            // leading ' ' would have tripped the split above), accepts one
            // +/- sign ('-' wraps modulo 2^64 without ERANGE), and the
            // unsigned long is then truncated to the uint32 Oid
            // ("4294967296" is oid 0 in C). ERANGE (magnitude > ULONG_MAX)
            // is FATAL; trailing junk (endp not at NUL) is FATAL.
            let oid = match scan_uint(&line[..sp], 10, usize::MAX) {
                Some((v, false, rest)) if rest.is_empty() => v as u32,
                _ => return Err(()),
            };
            tablespaces.push(TablespaceInfo {
                oid,
                path: line[sp + 1..].to_vec(),
            });
        } else if !was_backslash && ch == b'\\' {
            was_backslash = true;
        } else {
            // C: `if (i < sizeof(str) - 1) str[i++] = ch;` — the de-escaped
            // line is silently truncated to MAXPGPATH - 1 = 1023 bytes.
            if buf.len() < 1023 {
                buf.push(ch);
            }
            was_backslash = false;
        }
    }
    if !buf.is_empty() || was_backslash {
        return Err(());
    }
    Ok(tablespaces)
}

pub(crate) fn read_tablespace_map() -> PgResult<Option<Vec<TablespaceInfo>>> {
    let path = data_path(TABLESPACE_MAP);
    let content = match std::fs::read(&path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return could_not_read(TABLESPACE_MAP, &e, "read_tablespace_map"),
    };
    match parse_tablespace_map_content(&content) {
        Ok(tablespaces) => Ok(Some(tablespaces)),
        Err(()) => invalid_data(TABLESPACE_MAP, "read_tablespace_map"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LINE1: &str = "START WAL LOCATION: 0/16000028 (file 000000010000000000000016)\n";
    const LINE2: &str = "CHECKPOINT LOCATION: 0/16000060\n";

    fn label(content: &str) -> Result<BackupLabel, LabelError> {
        parse_backup_label_content(content.as_bytes())
    }

    fn head(rest: &str) -> String {
        format!("{LINE1}{LINE2}{rest}")
    }

    // The trailer as pg_backup_stop / pg_basebackup write it, in C's fscanf
    // order, so that a START TIMELINE / INCREMENTAL FROM LSN line appended
    // after `full("")` is actually reached by the fixed fscanf sequence
    // (xlogrecovery.c:1298-1358).
    const TRAILER: &str = "BACKUP METHOD: streamed\nBACKUP FROM: primary\nSTART TIME: 2026-09-03 03:49:34 PDT\nLABEL: fp label\n";

    fn full(rest: &str) -> String {
        format!("{LINE1}{LINE2}{TRAILER}{rest}")
    }

    #[test]
    fn standard_file_parses() {
        let out = label(&head("BACKUP METHOD: streamed\nBACKUP FROM: primary\nSTART TIMELINE: 1\n"))
            .unwrap();
        assert_eq!(out.redo_start_lsn, 0x16000028);
        assert_eq!(out.checkpoint_loc, 0x16000060);
        assert_eq!(out.redo_start_tli, 1);
        assert_eq!(out.backup_label_tli, 1);
        assert!(out.backup_end_required);
        assert!(!out.backup_from_standby);
    }

    // --- "START WAL LOCATION: %X/%X (file %08X%16s)%c" ---

    #[test]
    fn space_before_slash_fails_like_c() {
        // fscanf: literal '/' must match the next byte exactly; "0 /16000028"
        // leaves the '/' unmatched after %X stops at the space.
        let c = "START WAL LOCATION: 0 /16000028 (file 000000010000000000000016)\n".to_string()
            + LINE2;
        assert_eq!(label(&c).unwrap_err(), LabelError::Invalid);
    }

    #[test]
    fn leading_nbsp_fails_like_c() {
        // %X skips only C-locale isspace; U+00A0 is not in that set.
        let c = "START WAL LOCATION: \u{00A0}0/16000028 (file 000000010000000000000016)\n"
            .to_string()
            + LINE2;
        assert_eq!(label(&c).unwrap_err(), LabelError::Invalid);
    }

    #[test]
    fn leading_vt_ff_skipped_like_c() {
        // VT (0x0b) and FF (0x0c) ARE C-locale isspace; %X skips them.
        let c = "START WAL LOCATION: \x0b\x0c0/16000028 (file 000000010000000000000016)\n"
            .to_string()
            + LINE2;
        assert_eq!(label(&c).unwrap().redo_start_lsn, 0x16000028);
    }

    #[test]
    fn double_space_and_seven_hex_digits_accepted_like_c() {
        // The space before "(file" and inside it are whitespace directives
        // (any run matches); %08X takes up to 8 hex chars — 7 is fine — and
        // %16s then reads exactly 16 non-whitespace bytes before the ')'.
        let c = "START WAL LOCATION: 0/16000028 (file  0000001GGGGGGGGGGGGGGGG)\n".to_string()
            + LINE2;
        let out = label(&c).unwrap();
        assert_eq!(out.redo_start_tli, 1);
        assert_eq!(out.backup_label_tli, 1);
    }

    #[test]
    fn zero_width_whitespace_directive_accepted_like_c() {
        // A whitespace directive matches an EMPTY run too: no space before
        // "(file" still matches fscanf's " (file ".
        let c = "START WAL LOCATION: 0/16000028(file 000000010000000000000016)\n".to_string()
            + LINE2;
        assert!(label(&c).is_ok());
    }

    #[test]
    fn strtoul_hex_prefix_and_sign_accepted() {
        // glibc %X converts via strtoul base 16: optional 0x prefix, and a
        // '-' sign wraps (then truncates to uint32 on assignment).
        let c = "START WAL LOCATION: 0x1/-1 (file 000000010000000000000016)\n".to_string() + LINE2;
        let out = label(&c).unwrap();
        assert_eq!(out.redo_start_lsn, (1u64 << 32) | 0xFFFF_FFFF);
    }

    #[test]
    fn missing_newline_after_paren_fails_like_c() {
        // The %c must read '\n'; EOF there is a short count => FATAL.
        let c = "START WAL LOCATION: 0/16000028 (file 000000010000000000000016)";
        assert_eq!(label(c).unwrap_err(), LabelError::Invalid);
    }

    #[test]
    fn junk_after_paren_fails_like_c() {
        let c = "START WAL LOCATION: 0/16000028 (file 000000010000000000000016)x\n".to_string()
            + LINE2;
        assert_eq!(label(&c).unwrap_err(), LabelError::Invalid);
    }

    // --- "CHECKPOINT LOCATION: %X/%X%c" ---

    #[test]
    fn checkpoint_trailing_junk_fails_like_c() {
        // %c after the second %X must be '\n'.
        let c = format!("{LINE1}CHECKPOINT LOCATION: 0/16000060 junk\n");
        assert_eq!(label(&c).unwrap_err(), LabelError::Invalid);
    }

    // --- "BACKUP METHOD: %19s" / "BACKUP FROM: %19s" ---

    #[test]
    fn backup_method_first_token_wins_like_c() {
        // %19s reads the first whitespace-delimited token; trailing junk on
        // the line does not defeat the "streamed" comparison.
        let out = label(&head("BACKUP METHOD: streamed junk\n")).unwrap();
        assert!(out.backup_end_required);
    }

    #[test]
    fn backup_method_nbsp_is_part_of_the_token_like_c() {
        // U+00A0 is not C-locale whitespace, so it is PART of the %s token
        // and strcmp against "streamed" fails.
        let out = label(&head("BACKUP METHOD: streamed\u{00A0}\n")).unwrap();
        assert!(!out.backup_end_required);
    }

    #[test]
    fn backup_method_missing_format_space_matches_like_c() {
        // The space in the format between "BACKUP" and "METHOD:" is a
        // whitespace directive; an empty run matches.
        let out = label(&head("BACKUPMETHOD: streamed\n")).unwrap();
        assert!(out.backup_end_required);
    }

    #[test]
    fn backup_from_first_token_wins_like_c() {
        let out =
            label(&head("BACKUP METHOD: streamed\nBACKUP FROM: standby whatever\n")).unwrap();
        assert!(out.backup_from_standby);
    }

    // --- C's fixed fscanf sequence over the trailer (xlogrecovery.c:1298-1358) ---
    // Audit a186-verified-fp-transam-xlogrecovery-p1-10ebc5f875e333d6509a-1.

    #[test]
    fn out_of_order_start_timeline_is_skipped_like_c() {
        // C tries "BACKUP METHOD: %19s\n" first: 'B' != 'S' fails without
        // consuming; "START TIME: %127[^\n]\n" then matches the literal
        // "START TIME" prefix of "START TIMELINE" and fails at ':' vs 'L',
        // leaving the stream at "LINE: 2\n..." — the START TIMELINE line is
        // never seen again, so a TLI mismatch that C would enforce in order is
        // silently skipped, and BACKUP METHOD/FROM are lost too (C came up
        // on the live pair; pre-fix pgrust FATALed "Timeline ID parsed is 2").
        let out = label(&head(
            "START TIMELINE: 2\nBACKUP METHOD: streamed\nBACKUP FROM: primary\nSTART TIME: 2026-09-03 03:49:34 PDT\nLABEL: fp label\n",
        ))
        .unwrap();
        assert!(!out.backup_end_required);
        assert!(!out.backup_from_standby);
    }

    #[test]
    fn in_order_start_timeline_mismatch_is_fatal() {
        assert_eq!(
            label(&full("START TIMELINE: 2\n")).unwrap_err(),
            LabelError::TimelineMismatch { file: 2, walseg: 1 }
        );
    }

    #[test]
    fn backup_from_without_backup_method_is_lost_like_c() {
        // "BACKUP METHOD: " consumes the matched prefix "BACKUP " before
        // failing at 'M' vs 'F'; the next format ("BACKUP FROM: ") then sees
        // "FROM: standby" and fails at its first byte.
        let out = label(&head("BACKUP FROM: standby\n")).unwrap();
        assert!(!out.backup_from_standby);
    }

    #[test]
    fn missing_optional_lines_keep_the_sequence_like_c() {
        // BACKUP FROM and LABEL absent: each failed literal only consumes what
        // it matched (nothing here), so START TIME and START TIMELINE are
        // still reached in order.
        assert_eq!(
            label(&head("BACKUP METHOD: streamed\nSTART TIME: t\nSTART TIMELINE: 2\n"))
                .unwrap_err(),
            LabelError::TimelineMismatch { file: 2, walseg: 1 }
        );
    }

    #[test]
    fn whitespace_directive_crosses_newlines_like_c() {
        // A space in the format (and %s's leading skip) matches any run of
        // C-locale whitespace, newlines included.
        let out = label(&head("BACKUP METHOD:\nstreamed\n")).unwrap();
        assert!(out.backup_end_required);
    }

    // --- "START TIMELINE: %u" ---

    #[test]
    fn timeline_trailing_junk_still_feeds_cross_check() {
        // %u converts the leading digits; "2junk" parses as 2 and the
        // tli mismatch against the walseg TLI (1) is FATAL in C.
        assert_eq!(
            label(&full("START TIMELINE: 2junk\n")).unwrap_err(),
            LabelError::TimelineMismatch { file: 2, walseg: 1 }
        );
    }

    #[test]
    fn timeline_matching_with_trailing_junk_is_ok() {
        assert!(label(&full("START TIMELINE: 1junk\n")).is_ok());
    }

    #[test]
    fn timeline_no_digits_skips_check_like_c() {
        // fscanf returns 0; the cross-check is silently skipped.
        assert!(label(&full("START TIMELINE: junk\n")).is_ok());
    }

    // --- "INCREMENTAL FROM LSN: %X/%X" ---

    #[test]
    fn incremental_lsn_is_fatal() {
        assert_eq!(
            label(&full("INCREMENTAL FROM LSN: 0/1\n")).unwrap_err(),
            LabelError::Incremental
        );
    }

    #[test]
    fn incremental_without_hex_is_not_fatal_like_c() {
        // fscanf(...) > 0 needs at least the first %X to convert.
        assert!(label(&full("INCREMENTAL FROM LSN: zz\n")).is_ok());
    }

    // --- read_tablespace_map ---

    #[test]
    fn tablespace_map_basic() {
        let ts = parse_tablespace_map_content(b"16384 /path/one\n16385 /path two\n").unwrap();
        assert_eq!(ts.len(), 2);
        assert_eq!(ts[0].oid, 16384);
        assert_eq!(ts[0].path, b"/path/one");
        assert_eq!(ts[1].oid, 16385);
        assert_eq!(ts[1].path, b"/path two");
    }

    #[test]
    fn tablespace_path_non_utf8_bytes_preserved_like_c() {
        let ts = parse_tablespace_map_content(b"16384 /data/caf\xe9\n16385 /tbs_\xc3\xa9\n").unwrap();
        assert_eq!(ts[0].path, b"/data/caf\xe9");
        assert_eq!(ts[1].path, "/tbs_\u{e9}".as_bytes());
    }

    #[test]
    fn tablespace_oid_strtoul_leading_tab_accepted_like_c() {
        // strtoul skips leading C-locale whitespace; only a leading ' '
        // would have been caught by the space-split.
        let ts = parse_tablespace_map_content(b"\t16384 /p\n").unwrap();
        assert_eq!(ts[0].oid, 16384);
    }

    #[test]
    fn tablespace_oid_plus_sign_accepted_like_c() {
        let ts = parse_tablespace_map_content(b"+16384 /p\n").unwrap();
        assert_eq!(ts[0].oid, 16384);
    }

    #[test]
    fn tablespace_oid_minus_wraps_like_c() {
        // strtoul("-1") wraps to ULONG_MAX without ERANGE; the Oid
        // assignment truncates to 0xFFFFFFFF.
        let ts = parse_tablespace_map_content(b"-1 /p\n").unwrap();
        assert_eq!(ts[0].oid, u32::MAX);
    }

    #[test]
    fn tablespace_oid_truncates_to_u32_like_c() {
        // 2^32 fits in unsigned long; the Oid assignment truncates to 0.
        let ts = parse_tablespace_map_content(b"4294967296 /p\n").unwrap();
        assert_eq!(ts[0].oid, 0);
    }

    #[test]
    fn tablespace_oid_erange_is_fatal_like_c() {
        // Magnitude > ULONG_MAX sets ERANGE => FATAL in C.
        assert!(parse_tablespace_map_content(b"99999999999999999999999 /p\n").is_err());
    }

    #[test]
    fn tablespace_oid_trailing_junk_is_fatal_like_c() {
        assert!(parse_tablespace_map_content(b"16384x /p\n").is_err());
    }

    #[test]
    fn tablespace_line_truncated_at_1023_bytes_like_c() {
        // C caps the de-escaped line at MAXPGPATH - 1 = 1023 bytes.
        let mut line = b"1 ".to_vec();
        line.extend(std::iter::repeat(b'a').take(1500));
        line.push(b'\n');
        let ts = parse_tablespace_map_content(&line).unwrap();
        assert_eq!(ts[0].path.len(), 1021);
    }

    #[test]
    fn tablespace_unterminated_last_line_is_fatal_like_c() {
        assert!(parse_tablespace_map_content(b"16384 /p").is_err());
    }
}
