//! Port of `src/backend/utils/misc/tzparser.c` — parsing of timezone
//! abbreviation offset files (invoked from the `timezone_abbreviations` GUC
//! check hook).
//!
//! Per the C file's contract, problems are reported "softly": the C calls
//! `GUC_check_errmsg`/`errdetail`/`errhint` and returns `NULL`/`-1` rather than
//! throwing `elog(ERROR)`. Those become a [`TzParseError`] carrying the
//! message/detail/hint, surfaced through `Result`.
//!
//! Two genuine externals are routed through seams:
//!   * `read_tz_file` (`port-path-seams`) — open `"<share>/timezonesets/<name>"`
//!     relative to `my_exec_path` (`get_share_path` + `AllocateFile`), returning
//!     the file's lines or a classified open/read failure.
//!   * `convert_time_zone_abbrevs` (`backend-utils-adt-datetime-seams`) —
//!     `ConvertTimeZoneAbbrevs` (`utils/adt/datetime.c`), which re-allocates the
//!     entries into the `guc_malloc`'d `TimeZoneAbbrevTable`.

use adt_datetime::seam_impls::convert_time_zone_abbrevs;
use port_path_seams::read_tz_file;
use misc_more2::{TimeZoneAbbrevTable, TzEntry, TzFileOpenError, TzFileResult, SECS_PER_HOUR, TOKMAXLEN};

/// `#define WHITESPACE " \t\n\r"` (tzparser.c) — the `strtok_r` delimiters.
const WHITESPACE: &[char] = &[' ', '\t', '\n', '\r'];

/// Maximal `@INCLUDE` recursion depth (tzparser.c: `if (depth > 3)`).
const MAX_DEPTH: i32 = 3;

/// A soft parse failure: the `GUC_check_errmsg`/`errdetail`/`errhint` the C code
/// would have reported before returning `NULL`/`-1`. An empty `message` is the
/// depth-0 "let guc.c's own invalid-value message stand" case.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TzParseError {
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
}

impl TzParseError {
    fn msg(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            detail: None,
            hint: None,
        }
    }
}

/// `pg_tolower` (`port/pgstrcasecmp.c`) restricted to the ASCII fold used by
/// timezone abbreviations. High-bit bytes (which `isalpha`-gated parsing never
/// admits into an abbreviation) pass through unchanged.
fn pg_tolower(ch: u8) -> u8 {
    if ch.is_ascii_uppercase() {
        ch + (b'a' - b'A')
    } else {
        ch
    }
}

/// `pg_strncasecmp(line, prefix, strlen(prefix)) == 0` over the leading bytes,
/// ASCII-case-insensitive (matching the directive matching in `ParseTzFile`).
fn starts_with_ci(line: &str, prefix: &str) -> bool {
    let line = line.as_bytes();
    let prefix = prefix.as_bytes();
    line.len() >= prefix.len()
        && line[..prefix.len()]
            .iter()
            .zip(prefix)
            .all(|(a, b)| pg_tolower(*a) == pg_tolower(*b))
}

/// `validateTzEntry` — apply datetime.c storage-format checks and downcase the
/// abbreviation in place. Returns `Ok(())` if OK, else the soft error.
pub fn validate_tz_entry(entry: &mut TzEntry) -> Result<(), TzParseError> {
    // C: restriction imposed by datetktbl storage (datetime.c) — abbrev must fit
    // in TOKMAXLEN characters.
    if entry.abbrev.len() > TOKMAXLEN as usize {
        return Err(TzParseError::msg(format!(
            "time zone abbreviation \"{}\" is too long (maximum {} characters) in time zone file \"{}\", line {}",
            entry.abbrev, TOKMAXLEN, entry.filename, entry.lineno
        )));
    }

    // C: sanity-check the offset — shouldn't exceed 14 hours either way.
    if entry.offset > 14 * SECS_PER_HOUR || entry.offset < -14 * SECS_PER_HOUR {
        return Err(TzParseError::msg(format!(
            "time zone offset {} is out of range in time zone file \"{}\", line {}",
            entry.offset, entry.filename, entry.lineno
        )));
    }

    // C: convert abbrev to lowercase (must match datetime.c's conversion).
    let folded: String = entry.abbrev.bytes().map(|b| pg_tolower(b) as char).collect();
    entry.abbrev = folded;
    Ok(())
}

/// `splitTzLine` — parse one line as `name zone` or `name offset [D]`.
///
/// Mirrors `strtok_r` tokenization on whitespace, returning the populated entry
/// or the soft error. `line` is the line content (C passes the buffer after
/// skipping leading whitespace).
pub fn split_tz_line(filename: &str, lineno: i32, line: &str) -> Result<TzEntry, TzParseError> {
    let mut tokens = line.split(WHITESPACE).filter(|token| !token.is_empty());

    // C: abbrev = strtok_r(line, WHITESPACE, &brkl); if (!abbrev) ...
    let abbrev = tokens.next().ok_or_else(|| {
        TzParseError::msg(format!(
            "missing time zone abbreviation in time zone file \"{filename}\", line {lineno}"
        ))
    })?;

    let mut entry = TzEntry {
        // C: tzentry->abbrev = pstrdup(abbrev);
        abbrev: abbrev.to_owned(),
        zone: None,
        offset: 0,
        is_dst: false,
        lineno,
        filename: filename.to_owned(),
    };

    // C: offset = strtok_r(NULL, WHITESPACE, &brkl); if (!offset) ...
    let offset = tokens.next().ok_or_else(|| {
        TzParseError::msg(format!(
            "missing time zone offset in time zone file \"{filename}\", line {lineno}"
        ))
    })?;

    // C: we assume zone names don't begin with a digit or sign.
    let first = offset.as_bytes()[0];
    let remain = if first.is_ascii_digit() || first == b'+' || first == b'-' {
        // C: strtol(offset, &offset_endptr, 10); reject partial/non-numeric.
        let Some(value) = parse_strtol_base10(offset) else {
            return Err(TzParseError::msg(format!(
                "invalid number for time zone offset in time zone file \"{filename}\", line {lineno}"
            )));
        };
        entry.zone = None;
        entry.offset = value;

        // C: is_dst = strtok_r(NULL, ...); if (is_dst && pg_strcasecmp(is_dst,"D")==0)
        let is_dst = tokens.next();
        match is_dst {
            Some(token) if token.eq_ignore_ascii_case("D") => {
                entry.is_dst = true;
                tokens.next()
            }
            // C: there was no 'D' dst specifier — remain = is_dst.
            other => {
                entry.is_dst = false;
                other
            }
        }
    } else {
        // C: assume entry is a zone name; don't validate by looking it up.
        entry.zone = Some(offset.to_owned());
        entry.offset = 0 * SECS_PER_HOUR;
        entry.is_dst = false;
        tokens.next()
    };

    // C: if (!remain) return true; if (remain[0] != '#') ... return false.
    match remain {
        None => Ok(entry),
        Some(token) if token.starts_with('#') => Ok(entry),
        Some(_) => Err(TzParseError::msg(format!(
            "invalid syntax in time zone file \"{filename}\", line {lineno}"
        ))),
    }
}

/// `strtol(token, &end, 10)` requiring a non-empty parse with no trailing
/// garbage, matching C's `offset_endptr == offset || *offset_endptr != '\0'`
/// rejection. C `strtol` accepts a leading `+`; Rust's parser does not, so strip
/// it. Out-of-`i32`-range values are range-checked afterward by
/// `validate_tz_entry`, so saturate rather than reject here.
fn parse_strtol_base10(token: &str) -> Option<i32> {
    let trimmed = token.strip_prefix('+').unwrap_or(token);
    if trimmed.is_empty() {
        return None;
    }
    let value: i64 = trimmed.parse().ok()?;
    Some(value.clamp(i32::MIN as i64, i32::MAX as i64) as i32)
}

/// `addToArray` — insert `entry` into the sorted-by-abbrev `array`, handling
/// duplicates per the C rules. The array is kept sorted via binary search using
/// byte-wise `strcmp` (to match the order datetime.c expects). Returns `Ok(())`
/// or the soft error.
pub fn add_to_array(
    array: &mut Vec<TzEntry>,
    entry: TzEntry,
    override_dup: bool,
) -> Result<(), TzParseError> {
    let mut low: isize = 0;
    let mut high: isize = array.len() as isize - 1;
    while low <= high {
        let mid = ((low + high) >> 1) as usize;
        match entry.abbrev.as_bytes().cmp(array[mid].abbrev.as_bytes()) {
            std::cmp::Ordering::Less => high = mid as isize - 1,
            std::cmp::Ordering::Greater => low = mid as isize + 1,
            std::cmp::Ordering::Equal => {
                let midptr = &array[mid];
                // C: found a duplicate; complain unless it's effectively the same.
                let same = (midptr.zone.is_none()
                    && entry.zone.is_none()
                    && midptr.offset == entry.offset
                    && midptr.is_dst == entry.is_dst)
                    || (midptr.zone.is_some()
                        && entry.zone.is_some()
                        && midptr.zone == entry.zone);
                if same {
                    return Ok(()); // C: return unchanged array
                }
                if override_dup {
                    // C: same abbrev but something differs — override.
                    let midptr = &mut array[mid];
                    midptr.zone = entry.zone;
                    midptr.offset = entry.offset;
                    midptr.is_dst = entry.is_dst;
                    return Ok(());
                }
                // C: same abbrev but something is different — complain.
                return Err(TzParseError {
                    message: format!(
                        "time zone abbreviation \"{}\" is multiply defined",
                        entry.abbrev
                    ),
                    detail: Some(format!(
                        "Entry in time zone file \"{}\", line {}, conflicts with entry in file \"{}\", line {}.",
                        midptr.filename, midptr.lineno, entry.filename, entry.lineno
                    )),
                    hint: None,
                });
            }
        }
    }

    // C: no match, insert at position "low" (memmove + memcpy after repalloc).
    // Reserve fallibly so a large file cannot abort on OOM.
    array
        .try_reserve(1)
        .map_err(|_| TzParseError::msg("out of memory"))?;
    array.insert(low as usize, entry);
    Ok(())
}

/// `ParseTzFile` — parse one timezone file, recursing for `@INCLUDE`.
///
/// `filename` is user-specified (no path); `depth` is the current recursion
/// depth. Entries accumulate into `array` (kept sorted). The `@OVERRIDE` flag is
/// function-local (C's `bool override = false;` at tzparser.c:287); it is NOT
/// passed into an `@INCLUDE`'d child, so each file gets its own `override` and
/// includes do not leak override state. Returns `Ok(())` or the soft error.
pub fn parse_tz_file(
    filename: &str,
    depth: i32,
    array: &mut Vec<TzEntry>,
) -> Result<(), TzParseError> {
    // C: bool override = false; — function-local, reset per ParseTzFile call.
    let mut override_dup = false;
    // C: enforce all-alpha filename so '/' and the like can't escape the
    // timezonesets directory.
    if !filename.bytes().all(|b| b.is_ascii_alphabetic()) {
        // C: at level 0, just use guc.c's regular "invalid value" message.
        if depth > 0 {
            return Err(TzParseError::msg(format!(
                "invalid time zone file name \"{filename}\""
            )));
        }
        return Err(TzParseError {
            message: String::new(),
            detail: None,
            hint: None,
        });
    }

    // C: if (depth > 3) recursion limit exceeded.
    if depth > MAX_DEPTH {
        return Err(TzParseError::msg(format!(
            "time zone file recursion limit exceeded in file \"{filename}\""
        )));
    }

    // C: get_share_path + snprintf + AllocateFile, with the missing-directory
    // probe and errno-based message selection — bundled into read_tz_file.
    let lines = match read_tz_file::call(filename) {
        TzFileResult::Lines(lines) => lines,
        TzFileResult::Open(open_error) => return Err(open_error_message(open_error, depth)),
        TzFileResult::ReadError => {
            return Err(TzParseError::msg(format!(
                "could not read time zone file \"{filename}\": "
            )))
        }
        TzFileResult::LineTooLong { lineno } => {
            return Err(TzParseError::msg(format!(
                "line is too long in time zone file \"{filename}\", line {lineno}"
            )))
        }
    };

    let mut lineno = 0;
    for raw in &lines {
        lineno += 1;

        // C: skip over whitespace (isspace).
        let line = raw.trim_start_matches(|c: char| c.is_ascii_whitespace());

        if line.is_empty() {
            continue; // C: empty line
        }
        if line.starts_with('#') {
            continue; // C: comment line
        }

        if starts_with_ci(line, "@INCLUDE") {
            // C: pstrdup(line + strlen("@INCLUDE")); strtok_r(..., WHITESPACE).
            let rest = &line["@INCLUDE".len()..];
            let include_file = rest.split(WHITESPACE).find(|token| !token.is_empty());
            let Some(include_file) = include_file else {
                return Err(TzParseError::msg(format!(
                    "@INCLUDE without file name in time zone file \"{filename}\", line {lineno}"
                )));
            };
            parse_tz_file(include_file, depth + 1, array)?;
            continue;
        }

        if starts_with_ci(line, "@OVERRIDE") {
            override_dup = true;
            continue;
        }

        let mut entry = split_tz_line(filename, lineno, line)?;
        validate_tz_entry(&mut entry)?;
        add_to_array(array, entry, override_dup)?;
    }

    Ok(())
}

/// Translate a [`TzFileOpenError`] into the soft message `ParseTzFile` would
/// emit at the given depth.
fn open_error_message(open_error: TzFileOpenError, depth: i32) -> TzParseError {
    match open_error {
        // C: AllocateDir(timezonesets) == NULL -> could not open directory + hint.
        TzFileOpenError::DirectoryMissing {
            dir_error,
            exec_path,
        } => TzParseError {
            message: format!("could not open directory \"{dir_error}\": "),
            detail: None,
            hint: Some(format!(
                "This may indicate an incomplete PostgreSQL installation, or that the file \"{exec_path}\" has been moved away from its proper location."
            )),
        },
        // C: ENOENT at depth 0 -> guc.c's complaint is enough, emit no message.
        TzFileOpenError::FileNotFound if depth == 0 => TzParseError {
            message: String::new(),
            detail: None,
            hint: None,
        },
        TzFileOpenError::FileNotFound => {
            TzParseError::msg("could not read time zone file: file not found")
        }
        TzFileOpenError::FileUnreadable { file_error } => {
            TzParseError::msg(format!("could not read time zone file \"{file_error}\": "))
        }
    }
}

/// `load_tzoffsets` — read and parse `filename`, returning the converted
/// [`TimeZoneAbbrevTable`] on success (the C `guc_malloc`'d table), or the soft
/// error. The C works in a temporary memory context and hands the parsed array
/// to `ConvertTimeZoneAbbrevs`; here the array is an owned `Vec` and the
/// conversion is the `convert_time_zone_abbrevs` seam.
pub fn load_tzoffsets(filename: &str) -> Result<TimeZoneAbbrevTable, TzParseError> {
    // C: arraysize = 128; array = palloc(...). We grow on demand instead.
    let mut array: Vec<TzEntry> = Vec::new();

    parse_tz_file(filename, 0, &mut array)?;

    // C: if (n >= 0) result = ConvertTimeZoneAbbrevs(array, n);
    //    if (!result) GUC_check_errmsg("out of memory");
    convert_time_zone_abbrevs(array).ok_or_else(|| TzParseError::msg("out of memory"))
}
