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
//! Residual divergences from the C stream parser (all deliberately out of
//! scope; the diff is kept line-based like the pre-existing code):
//! - C's whitespace directives and the leading skip of %X/%u/%s can cross
//!   newlines ("BACKUP METHOD:\nstreamed" matches in C); we match within a
//!   single line.
//! - C tries the optional trailer fields in one fixed order against the
//!   stream, and a partial literal match consumes input (an out-of-order
//!   file silently loses fields in C); we recognize the trailer lines in any
//!   order, one per line.
//! - glibc scanf treats "0x" NOT followed by a hex digit as a matching
//!   failure mid-conversion; we follow strtoul instead and parse the "0"
//!   (value 0), leaving the 'x' as trailing input.
//! - C reads raw bytes; we read the file as UTF-8 (pre-existing; a non-UTF-8
//!   backup_label FATALs as "could not read file" rather than being parsed
//!   bytewise).

use elog::{elog, ereport};
use pg_string::isspace_c_locale;
use types_core::{TimeLineID, XLogRecPtr};
use types_error::{PgResult, DEBUG1, FATAL};

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

fn invalid_data<T>(file: &str, func: &'static str) -> PgResult<T> {
    ereport(FATAL)
        .errmsg(format!("invalid data in file \"{file}\""))
        .finish(loc(func))?;
    unreachable!()
}

/// fscanf format-literal matcher. A space in `fmt` is a whitespace directive
/// (skips zero or more C-locale isspace bytes); any other byte in `fmt` must
/// match the next input byte exactly. Returns the remaining input.
fn scan_literal<'a>(mut s: &'a [u8], fmt: &str) -> Option<&'a [u8]> {
    for &f in fmt.as_bytes() {
        if f == b' ' {
            while let [b, rest @ ..] = s {
                if !isspace_c_locale(*b) {
                    break;
                }
                s = rest;
            }
        } else {
            match s {
                [b, rest @ ..] if *b == f => s = rest,
                _ => None?,
            }
        }
    }
    Some(s)
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

fn parse_backup_label_content(content: &str) -> Result<BackupLabel, LabelError> {
    let mut out = BackupLabel {
        checkpoint_loc: InvalidXLogRecPtr,
        backup_label_tli: 0,
        backup_end_required: false,
        backup_from_standby: false,
        redo_start_lsn: InvalidXLogRecPtr,
        redo_start_tli: 0,
    };
    // Keep the terminating '\n' on each line: the first two formats end in
    // "%c" and C checks ch == '\n' (a '\r' there, or EOF, is FATAL).
    let mut lines = content.split_inclusive('\n').map(str::as_bytes);

    // "START WAL LOCATION: %X/%X (file %08X%16s)%c", 5 fields, ch == '\n'.
    let l1 = lines.next().unwrap_or(b"");
    let rest = scan_literal(l1, "START WAL LOCATION: ").ok_or(LabelError::Invalid)?;
    let (hi, _, rest) = scan_uint(rest, 16, usize::MAX).ok_or(LabelError::Invalid)?;
    let rest = scan_literal(rest, "/").ok_or(LabelError::Invalid)?;
    let (lo, _, rest) = scan_uint(rest, 16, usize::MAX).ok_or(LabelError::Invalid)?;
    let rest = scan_literal(rest, " (file ").ok_or(LabelError::Invalid)?;
    let (tli, _, rest) = scan_uint(rest, 16, 8).ok_or(LabelError::Invalid)?;
    let tli_from_walseg = tli as u32;
    let (_fname, rest) = scan_token(rest, 16).ok_or(LabelError::Invalid)?;
    let rest = scan_literal(rest, ")").ok_or(LabelError::Invalid)?;
    if rest.first() != Some(&b'\n') {
        return Err(LabelError::Invalid);
    }
    out.redo_start_lsn = (u64::from(hi as u32)) << 32 | u64::from(lo as u32);
    out.redo_start_tli = tli_from_walseg;
    out.backup_label_tli = tli_from_walseg;

    // "CHECKPOINT LOCATION: %X/%X%c", 3 fields, ch == '\n'.
    let l2 = lines.next().unwrap_or(b"");
    let rest = scan_literal(l2, "CHECKPOINT LOCATION: ").ok_or(LabelError::Invalid)?;
    let (hi, _, rest) = scan_uint(rest, 16, usize::MAX).ok_or(LabelError::Invalid)?;
    let rest = scan_literal(rest, "/").ok_or(LabelError::Invalid)?;
    let (lo, _, rest) = scan_uint(rest, 16, usize::MAX).ok_or(LabelError::Invalid)?;
    if rest.first() != Some(&b'\n') {
        return Err(LabelError::Invalid);
    }
    out.checkpoint_loc = (u64::from(hi as u32)) << 32 | u64::from(lo as u32);

    for line in lines {
        if let Some(rest) = scan_literal(line, "BACKUP METHOD: ") {
            // "%19s": the FIRST whitespace-delimited token (<= 19 bytes) is
            // compared, so "streamed junk" still sets backupEndRequired.
            if let Some((tok, _)) = scan_token(rest, 19) {
                if tok == b"streamed" {
                    out.backup_end_required = true;
                }
                continue;
            }
        }
        if let Some(rest) = scan_literal(line, "BACKUP FROM: ") {
            if let Some((tok, _)) = scan_token(rest, 19) {
                if tok == b"standby" {
                    out.backup_from_standby = true;
                }
                continue;
            }
        }
        if let Some(rest) = scan_literal(line, "START TIMELINE: ") {
            // "%u": strtoul base 10; trailing junk after the digits is left
            // unread by the conversion ("2junk" parses as 2 and still feeds
            // the cross-check). No digits => fscanf returns 0 and the whole
            // check is silently skipped, like C.
            if let Some((v, _, _)) = scan_uint(rest, 10, usize::MAX) {
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
                continue;
            }
        }
        if let Some(rest) = scan_literal(line, "INCREMENTAL FROM LSN: ") {
            // C: fscanf(..., "%X/%X\n", ...) > 0 — at least the first %X
            // must convert; a prefix match with no hex digits is NOT the
            // incremental-backup FATAL.
            if scan_uint(rest, 16, usize::MAX).is_some() {
                return Err(LabelError::Incremental);
            }
        }
    }
    Ok(out)
}

pub(crate) fn read_backup_label() -> PgResult<Option<BackupLabel>> {
    let path = data_path(BACKUP_LABEL_FILE);
    let content = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            { ereport(FATAL)
                .errmsg(format!("could not read file \"{BACKUP_LABEL_FILE}\": {e}"))
                .finish(loc("read_backup_label"))?; unreachable!() }
        }
    };
    match parse_backup_label_content(&content) {
        Ok(out) => Ok(Some(out)),
        Err(LabelError::Invalid) => invalid_data(BACKUP_LABEL_FILE, "read_backup_label"),
        Err(LabelError::TimelineMismatch { file, walseg }) => {
            ereport(FATAL)
                .errmsg(format!("invalid data in file \"{BACKUP_LABEL_FILE}\""))
                .errdetail(format!(
                    "Timeline ID parsed is {file}, but expected {walseg}."
                ))
                .finish(loc("read_backup_label"))?;
            unreachable!()
        }
        Err(LabelError::Incremental) => {
            ereport(FATAL)
                .errmsg("this is an incremental backup, not a data directory")
                .errhint("Use pg_combinebackup to reconstruct a valid data directory.")
                .finish(loc("read_backup_label"))?;
            unreachable!()
        }
    }
}

pub(crate) struct TablespaceInfo {
    pub oid: u32,
    pub path: String,
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
                path: String::from_utf8_lossy(&line[sp + 1..]).into_owned(),
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
        Err(e) => {
            { ereport(FATAL)
                .errmsg(format!("could not read file \"{TABLESPACE_MAP}\": {e}"))
                .finish(loc("read_tablespace_map"))?; unreachable!() }
        }
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
        parse_backup_label_content(content)
    }

    fn head(rest: &str) -> String {
        format!("{LINE1}{LINE2}{rest}")
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
        let out = label(&head("BACKUP FROM: standby whatever\n")).unwrap();
        assert!(out.backup_from_standby);
    }

    // --- "START TIMELINE: %u" ---

    #[test]
    fn timeline_trailing_junk_still_feeds_cross_check() {
        // %u converts the leading digits; "2junk" parses as 2 and the
        // tli mismatch against the walseg TLI (1) is FATAL in C.
        assert_eq!(
            label(&head("START TIMELINE: 2junk\n")).unwrap_err(),
            LabelError::TimelineMismatch { file: 2, walseg: 1 }
        );
    }

    #[test]
    fn timeline_matching_with_trailing_junk_is_ok() {
        assert!(label(&head("START TIMELINE: 1junk\n")).is_ok());
    }

    #[test]
    fn timeline_no_digits_skips_check_like_c() {
        // fscanf returns 0; the cross-check is silently skipped.
        assert!(label(&head("START TIMELINE: junk\n")).is_ok());
    }

    // --- "INCREMENTAL FROM LSN: %X/%X" ---

    #[test]
    fn incremental_lsn_is_fatal() {
        assert_eq!(
            label(&head("INCREMENTAL FROM LSN: 0/1\n")).unwrap_err(),
            LabelError::Incremental
        );
    }

    #[test]
    fn incremental_without_hex_is_not_fatal_like_c() {
        // fscanf(...) > 0 needs at least the first %X to convert.
        assert!(label(&head("INCREMENTAL FROM LSN: zz\n")).is_ok());
    }

    // --- read_tablespace_map ---

    #[test]
    fn tablespace_map_basic() {
        let ts = parse_tablespace_map_content(b"16384 /path/one\n16385 /path two\n").unwrap();
        assert_eq!(ts.len(), 2);
        assert_eq!(ts[0].oid, 16384);
        assert_eq!(ts[0].path, "/path/one");
        assert_eq!(ts[1].oid, 16385);
        assert_eq!(ts[1].path, "/path two");
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
