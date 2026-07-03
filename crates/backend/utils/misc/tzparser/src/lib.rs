//! tzparser.c: timezone_abbreviations GUC check-hook parsing. Failures are
//! soft — GUC_check_errmsg + None, never ERROR (C's contract with guc.c).

use std::sync::OnceLock;

use adt_datetime::tz::{ConvertTimeZoneAbbrevs, TzEntry as TzView, ZoneAbbrevTable};
use guc::{GUC_check_errhint, GUC_check_errmsg};
use mcx::{slice_borrow_in, vec_with_capacity_in, Mcx, MemoryContext, PgVec};

#[cfg(test)]
mod tests;

const TOKMAXLEN: usize = adt_datetime::consts::TOKMAXLEN;
const SECS_PER_HOUR: i32 = 3600;
const TZBUF_SIZE: usize = 1024;

fn is_tok_delim(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r')
}

fn tokens(line: &[u8]) -> impl Iterator<Item = &[u8]> {
    line.split(|&b| is_tok_delim(b)).filter(|t| !t.is_empty())
}

fn is_c_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

fn starts_with_ci(line: &[u8], prefix: &[u8]) -> bool {
    line.len() >= prefix.len()
        && line[..prefix.len()]
            .iter()
            .zip(prefix)
            .all(|(a, b)| a.eq_ignore_ascii_case(b))
}

// Drop-free (arena rule): all fields borrow the parse context.
#[derive(Clone, Copy)]
struct TzParsedEntry<'mcx> {
    abbrev: &'mcx [u8],
    zone: Option<&'mcx [u8]>,
    offset: i32,
    is_dst: bool,
    lineno: i32,
    filename: &'mcx str,
}

// C assigns strtol's long into the int offset field: saturate at long range,
// then wrap to i32 (bug-compat).
fn strtol10(tok: &[u8]) -> Option<i32> {
    let (neg, digits) = match tok.first() {
        Some(b'+') => (false, &tok[1..]),
        Some(b'-') => (true, &tok[1..]),
        _ => (false, tok),
    };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let mut v: i64 = 0;
    for &d in digits {
        v = v.saturating_mul(10).saturating_sub((d - b'0') as i64);
    }
    let v = if neg { v } else { v.checked_neg().unwrap_or(i64::MAX) };
    Some(v as i32)
}

fn split_tz_line<'mcx>(
    mcx: Mcx<'mcx>,
    filename: &'mcx str,
    lineno: i32,
    line: &[u8],
) -> Option<TzParsedEntry<'mcx>> {
    let mut toks = tokens(line);

    let Some(abbrev) = toks.next() else {
        GUC_check_errmsg(format!(
            "missing time zone abbreviation in time zone file \"{filename}\", line {lineno}"
        ));
        return None;
    };
    let abbrev = slice_borrow_in(mcx, abbrev).ok()?;

    let Some(offset_tok) = toks.next() else {
        GUC_check_errmsg(format!(
            "missing time zone offset in time zone file \"{filename}\", line {lineno}"
        ));
        return None;
    };

    let mut entry = TzParsedEntry { abbrev, zone: None, offset: 0, is_dst: false, lineno, filename };

    // We assume zone names don't begin with a digit or sign.
    let first = offset_tok[0];
    let remain = if first.is_ascii_digit() || first == b'+' || first == b'-' {
        let Some(offset) = strtol10(offset_tok) else {
            GUC_check_errmsg(format!(
                "invalid number for time zone offset in time zone file \"{filename}\", line {lineno}"
            ));
            return None;
        };
        entry.offset = offset;
        match toks.next() {
            Some(t) if t.eq_ignore_ascii_case(b"D") => {
                entry.is_dst = true;
                toks.next()
            }
            other => other,
        }
    } else {
        // A zone name; not validated by lookup so unused zones stay unloaded.
        entry.zone = Some(slice_borrow_in(mcx, offset_tok).ok()?);
        toks.next()
    };

    match remain {
        None => Some(entry),
        Some(t) if t[0] == b'#' => Some(entry),
        Some(_) => {
            GUC_check_errmsg(format!(
                "invalid syntax in time zone file \"{filename}\", line {lineno}"
            ));
            None
        }
    }
}

fn validate_tz_entry<'mcx>(mcx: Mcx<'mcx>, entry: &mut TzParsedEntry<'mcx>) -> bool {
    if entry.abbrev.len() > TOKMAXLEN {
        GUC_check_errmsg(format!(
            "time zone abbreviation \"{}\" is too long (maximum {} characters) in time zone file \"{}\", line {}",
            String::from_utf8_lossy(entry.abbrev),
            TOKMAXLEN,
            entry.filename,
            entry.lineno
        ));
        return false;
    }
    if entry.offset > 14 * SECS_PER_HOUR || entry.offset < -14 * SECS_PER_HOUR {
        GUC_check_errmsg(format!(
            "time zone offset {} is out of range in time zone file \"{}\", line {}",
            entry.offset, entry.filename, entry.lineno
        ));
        return false;
    }
    // Downcase must match datetime.c's conversion (a fresh copy; the entry
    // slices are shared borrows of the parse arena).
    let mut low = [0u8; TOKMAXLEN];
    let low = &mut low[..entry.abbrev.len()];
    low.copy_from_slice(entry.abbrev);
    low.make_ascii_lowercase();
    let Ok(low) = slice_borrow_in(mcx, low) else {
        return false;
    };
    entry.abbrev = low;
    true
}

fn add_to_array<'mcx>(
    array: &mut PgVec<'mcx, TzParsedEntry<'mcx>>,
    entry: TzParsedEntry<'mcx>,
    over_ride: bool,
) -> bool {
    let mut low = 0usize;
    let mut high = array.len();
    while low < high {
        let mid = (low + high) >> 1;
        let midptr = &array[mid];
        match entry.abbrev.cmp(midptr.abbrev) {
            core::cmp::Ordering::Less => high = mid,
            core::cmp::Ordering::Greater => low = mid + 1,
            core::cmp::Ordering::Equal => {
                let same = match (midptr.zone, entry.zone) {
                    (None, None) => {
                        midptr.offset == entry.offset && midptr.is_dst == entry.is_dst
                    }
                    (Some(a), Some(b)) => a == b,
                    _ => false,
                };
                if same {
                    return true;
                }
                if over_ride {
                    let midptr = &mut array[mid];
                    midptr.zone = entry.zone;
                    midptr.offset = entry.offset;
                    midptr.is_dst = entry.is_dst;
                    return true;
                }
                GUC_check_errmsg(format!(
                    "time zone abbreviation \"{}\" is multiply defined",
                    String::from_utf8_lossy(entry.abbrev)
                ));
                guc::GUC_check_errdetail(format!(
                    "Entry in time zone file \"{}\", line {}, conflicts with entry in file \"{}\", line {}.",
                    midptr.filename, midptr.lineno, entry.filename, entry.lineno
                ));
                return false;
            }
        }
    }
    array.insert(low, entry);
    true
}

// DIVERGENCE from ParseTzFile's get_share_path(my_exec_path): the PGRUST_*
// runtime env / PGRUST_TZDIR's parent take precedence over C's get_share_path
// resolution (harness override; boot-smoke.sh exports them).
fn tzsets_dir() -> &'static str {
    static DIR: OnceLock<String> = OnceLock::new();
    DIR.get_or_init(|| {
        if let Ok(share) = std::env::var("PGRUST_PGSHAREDIR") {
            return format!("{share}/timezonesets");
        }
        if let Ok(tzdir) = std::env::var("PGRUST_TZDIR") {
            if let Some((parent, _)) = tzdir.rsplit_once('/') {
                return format!("{parent}/timezonesets");
            }
        }
        if let Some(share) = option_env!("PGRUST_PGSHAREDIR") {
            return format!("{share}/timezonesets");
        }
        let exec = init_small::globals::my_exec_path();
        let len = exec.iter().position(|&b| b == 0).unwrap_or(exec.len());
        let share = pg_path::get_share_path(&String::from_utf8_lossy(&exec[..len]));
        format!("{share}/timezonesets")
    })
}

fn parse_tz_file<'mcx>(
    mcx: Mcx<'mcx>,
    dir: &str,
    filename: &[u8],
    depth: i32,
    array: &mut PgVec<'mcx, TzParsedEntry<'mcx>>,
) -> bool {
    // All-alpha filenames only: '/' must not escape the timezonesets dir.
    if !filename.iter().all(u8::is_ascii_alphabetic) {
        // At level 0 guc.c's regular "invalid value" message suffices.
        if depth > 0 {
            GUC_check_errmsg(format!(
                "invalid time zone file name \"{}\"",
                String::from_utf8_lossy(filename)
            ));
        }
        return false;
    }
    let filename = core::str::from_utf8(filename).expect("all-alpha filename");

    if depth > 3 {
        GUC_check_errmsg(format!(
            "time zone file recursion limit exceeded in file \"{filename}\""
        ));
        return false;
    }

    // Boot-cold fs path: std path/read justified (pgtz precedent).
    let file_path = format!("{dir}/{filename}");
    let contents = match std::fs::read(&file_path) {
        Ok(c) => c,
        Err(e) => {
            // If share/timezonesets itself is missing, say so: it is likely
            // the first sign of a broken installation during startup.
            if let Err(de) = std::fs::read_dir(dir) {
                GUC_check_errmsg(format!("could not open directory \"{dir}\": {de}"));
                GUC_check_errhint(format!(
                    "This may indicate an incomplete PostgreSQL installation, or that the directory \"{dir}\" has been moved away from its proper location."
                ));
                return false;
            }
            if e.kind() != std::io::ErrorKind::NotFound || depth > 0 {
                GUC_check_errmsg(format!("could not read time zone file \"{filename}\": {e}"));
            }
            return false;
        }
    };

    let Ok(filename) = slice_borrow_in(mcx, filename.as_bytes()) else {
        return false;
    };
    let filename = core::str::from_utf8(filename).expect("all-alpha filename");

    let mut over_ride = false;
    let mut lineno = 0;
    for raw in contents.split_inclusive(|&b| b == b'\n') {
        lineno += 1;
        // fgets fills tzbuf[1024]; a full buffer means the line didn't fit.
        if raw.len() >= TZBUF_SIZE - 1 {
            GUC_check_errmsg(format!(
                "line is too long in time zone file \"{filename}\", line {lineno}"
            ));
            return false;
        }

        let mut line: &[u8] = raw;
        while let Some((&b, rest)) = line.split_first() {
            if !is_c_space(b) {
                break;
            }
            line = rest;
        }
        if line.is_empty() || line[0] == b'#' {
            continue;
        }

        if starts_with_ci(line, b"@INCLUDE") {
            let Some(include_file) = tokens(&line[b"@INCLUDE".len()..]).next() else {
                GUC_check_errmsg(format!(
                    "@INCLUDE without file name in time zone file \"{filename}\", line {lineno}"
                ));
                return false;
            };
            if !parse_tz_file(mcx, dir, include_file, depth + 1, array) {
                return false;
            }
            continue;
        }

        if starts_with_ci(line, b"@OVERRIDE") {
            over_ride = true;
            continue;
        }

        let Some(mut entry) = split_tz_line(mcx, filename, lineno, line) else {
            return false;
        };
        if !validate_tz_entry(mcx, &mut entry) {
            return false;
        }
        if !add_to_array(array, entry, over_ride) {
            return false;
        }
    }

    true
}

fn load_tzoffsets_from(dir: &str, filename: &str) -> Option<&'static ZoneAbbrevTable> {
    let ctx = MemoryContext::new("TZParserMemory");
    let mcx = ctx.mcx();
    let mut array: PgVec<'_, TzParsedEntry<'_>> = vec_with_capacity_in(mcx, 128).ok()?;

    if !parse_tz_file(mcx, dir, filename.as_bytes(), 0, &mut array) {
        return None;
    }

    let mut views: PgVec<'_, TzView<'_>> = vec_with_capacity_in(mcx, array.len()).ok()?;
    for e in &array {
        views.push(TzView {
            abbrev: e.abbrev,
            zone: e.zone,
            offset: e.offset,
            is_dst: e.is_dst,
        });
    }
    Some(ConvertTimeZoneAbbrevs(&views))
}

/// On failure returns None with the details in the GUC check-error slots.
pub fn load_tzoffsets(filename: &str) -> Option<&'static ZoneAbbrevTable> {
    let dir = tzsets_dir();
    load_tzoffsets_from(dir, filename)
}
