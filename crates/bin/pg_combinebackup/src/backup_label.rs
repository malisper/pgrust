//! C: src/bin/pg_combinebackup/backup_label.c — read and manipulate
//! backup_label files. Error message identity pinned to C.

use std::io::Write;
use std::path::Path;

use manifest::{PgChecksumContext, PgChecksumType};
use types_core::{TimeLineID, XLogRecPtr};

use crate::flog::{errno_message, pg_fatal};
use crate::write_manifest::ManifestWriter;

/// C: parse_backup_label. Scans the label line by line; extracts
/// START WAL LOCATION, START TIMELINE, and the INCREMENTAL FROM LSN/TLI pair
/// (both or neither). Errors are fatal with C's exact texts.
pub fn parse_backup_label(
    filename: &str,
    buf: &[u8],
) -> (TimeLineID, XLogRecPtr, TimeLineID, XLogRecPtr) {
    let mut start_tli: TimeLineID = 0;
    let mut start_lsn: XLogRecPtr = 0;
    let mut previous_tli: TimeLineID = 0;
    let mut previous_lsn: XLogRecPtr = 0;
    let mut found = 0u32;

    let mut cursor = 0usize;
    while cursor < buf.len() {
        let eo = get_eol_offset(buf, cursor);
        let line = &buf[cursor..eo];

        if let Some(rest) = line_strip_prefix(line, b"START WAL LOCATION: ") {
            let Some((lsn, nchars)) = parse_lsn(rest) else {
                pg_fatal!("{}: could not parse {}", filename, "START WAL LOCATION");
            };
            start_lsn = lsn;
            if rest.get(nchars) != Some(&b' ') {
                pg_fatal!("{}: improper terminator for {}", filename, "START WAL LOCATION");
            }
            found |= 1;
        } else if let Some(rest) = line_strip_prefix(line, b"START TIMELINE: ") {
            let Some(tli) = parse_tli(rest) else {
                pg_fatal!("{}: could not parse TLI for {}", filename, "START TIMELINE");
            };
            start_tli = tli;
            if start_tli == 0 {
                pg_fatal!("{}: invalid TLI", filename);
            }
            found |= 2;
        } else if let Some(rest) = line_strip_prefix(line, b"INCREMENTAL FROM LSN: ") {
            let Some((lsn, nchars)) = parse_lsn(rest) else {
                pg_fatal!("{}: could not parse {}", filename, "INCREMENTAL FROM LSN");
            };
            previous_lsn = lsn;
            if rest.get(nchars) != Some(&b'\n') {
                pg_fatal!("{}: improper terminator for {}", filename, "INCREMENTAL FROM LSN");
            }
            found |= 4;
        } else if let Some(rest) = line_strip_prefix(line, b"INCREMENTAL FROM TLI: ") {
            let Some(tli) = parse_tli(rest) else {
                pg_fatal!("{}: could not parse {}", filename, "INCREMENTAL FROM TLI");
            };
            previous_tli = tli;
            if previous_tli == 0 {
                pg_fatal!("{}: invalid TLI", filename);
            }
            found |= 8;
        }

        cursor = eo;
    }

    if (found & 1) == 0 {
        pg_fatal!("{}: could not find {}", filename, "START WAL LOCATION");
    }
    if (found & 2) == 0 {
        pg_fatal!("{}: could not find {}", filename, "START TIMELINE");
    }
    if (found & 4) != 0 && (found & 8) == 0 {
        pg_fatal!(
            "{}: {} requires {}",
            filename,
            "INCREMENTAL FROM LSN",
            "INCREMENTAL FROM TLI"
        );
    }
    if (found & 8) != 0 && (found & 4) == 0 {
        pg_fatal!(
            "{}: {} requires {}",
            filename,
            "INCREMENTAL FROM TLI",
            "INCREMENTAL FROM LSN"
        );
    }

    (start_tli, start_lsn, previous_tli, previous_lsn)
}

/// C: write_backup_label. Writes the given label into
/// `<output_directory>/backup_label`, dropping the INCREMENTAL FROM LSN/TLI
/// lines, checksumming what is written, and adding it to the manifest.
pub fn write_backup_label(
    output_directory: &Path,
    buf: &[u8],
    checksum_type: PgChecksumType,
    mwriter: Option<&mut ManifestWriter>,
) {
    let output_filename = output_directory.join("backup_label");
    let mut checksum_ctx = PgChecksumContext::init(checksum_type);

    let mut file = match crate::copy_file::open_excl_create(&output_filename, false) {
        Ok(f) => f,
        Err(e) => pg_fatal!(
            "could not open file \"{}\": {}",
            output_filename.display(),
            errno_message(&e)
        ),
    };

    let mut cursor = 0usize;
    while cursor < buf.len() {
        let eo = get_eol_offset(buf, cursor);
        let line = &buf[cursor..eo];

        if line_strip_prefix(line, b"INCREMENTAL FROM LSN: ").is_none()
            && line_strip_prefix(line, b"INCREMENTAL FROM TLI: ").is_none()
        {
            match file.write_all(line) {
                Ok(()) => {}
                Err(e) => pg_fatal!(
                    "could not write file \"{}\": {}",
                    output_filename.display(),
                    errno_message(&e)
                ),
            }
            checksum_ctx.update(line);
        }

        cursor = eo;
    }

    // C: close(output_fd) with a pinned error message on failure.
    if let Err(e) = crate::copy_file::close_file(file) {
        pg_fatal!(
            "could not close file \"{}\": {}",
            output_filename.display(),
            errno_message(&e)
        );
    }

    let mut payload = [0u8; manifest::PG_CHECKSUM_MAX_LENGTH];
    let checksum_length = checksum_ctx.finalize(&mut payload);

    if let Some(mwriter) = mwriter {
        let (size, mtime) = match std::fs::metadata(&output_filename) {
            Ok(md) => (md.len(), crate::write_manifest::mtime_of(&md)),
            Err(e) => pg_fatal!(
                "could not stat file \"{}\": {}",
                output_filename.display(),
                errno_message(&e)
            ),
        };
        mwriter.add_file(
            b"backup_label",
            size,
            mtime,
            checksum_type,
            &payload[..checksum_length],
        );
    }
}

/// C: get_eol_offset — offset just past the next newline, or end of buffer.
fn get_eol_offset(buf: &[u8], cursor: usize) -> usize {
    let mut eo = cursor;
    while eo < buf.len() {
        if buf[eo] == b'\n' {
            return eo + 1;
        }
        eo += 1;
    }
    eo
}

/// C: line_starts_with.
fn line_strip_prefix<'a>(line: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
    line.strip_prefix(prefix)
}

/// C: parse_lsn — sscanf(s, "%X/%X%n") == 2. Returns the LSN and the number
/// of bytes consumed. sscanf's %X skips leading whitespace and accepts an
/// optional 0x/0X prefix; the '/' must immediately follow the first number.
pub fn parse_lsn(s: &[u8]) -> Option<(XLogRecPtr, usize)> {
    let (hi, n1) = scan_hex_u32(s)?;
    if s.get(n1) != Some(&b'/') {
        return None;
    }
    let (lo, n2) = scan_hex_u32(&s[n1 + 1..])?;
    Some(((u64::from(hi)) << 32 | u64::from(lo), n1 + 1 + n2))
}

/// C: parse_tli — sscanf(s, "%u%n") == 1 and the next character must be a
/// newline.
fn parse_tli(s: &[u8]) -> Option<TimeLineID> {
    let mut i = 0usize;
    while i < s.len() && (s[i] == b' ' || (0x09..=0x0d).contains(&s[i])) {
        i += 1;
    }
    let start = i;
    let mut acc: u64 = 0;
    while i < s.len() && s[i].is_ascii_digit() {
        acc = acc.saturating_mul(10).saturating_add(u64::from(s[i] - b'0'));
        i += 1;
    }
    if i == start {
        return None;
    }
    if s.get(i) != Some(&b'\n') {
        return None;
    }
    Some(acc as TimeLineID)
}

/// sscanf %X: skip whitespace, optional 0x/0X prefix, then hex digits.
/// Overflow wraps to u32 like strtoul-to-uint conversion in practice; label
/// values are always in range.
fn scan_hex_u32(s: &[u8]) -> Option<(u32, usize)> {
    let mut i = 0usize;
    while i < s.len() && (s[i] == b' ' || (0x09..=0x0d).contains(&s[i])) {
        i += 1;
    }
    if i + 1 < s.len() && s[i] == b'0' && (s[i + 1] == b'x' || s[i + 1] == b'X') {
        i += 2;
    }
    let start = i;
    let mut acc: u64 = 0;
    while i < s.len() {
        let d = match s[i] {
            b'0'..=b'9' => u64::from(s[i] - b'0'),
            b'a'..=b'f' => u64::from(s[i] - b'a' + 10),
            b'A'..=b'F' => u64::from(s[i] - b'A' + 10),
            _ => break,
        };
        acc = (acc << 4) | d;
        acc &= 0xffff_ffff_ffff_ffff;
        i += 1;
    }
    if i == start {
        return None;
    }
    Some((acc as u32, i))
}
