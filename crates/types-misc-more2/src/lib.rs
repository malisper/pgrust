//! Vocabulary for the `backend-utils-misc-more2` unit's non-timeout files
//! (`pg_config.c`, `tzparser.c`): the row/entry/table structs and the
//! file-read result classification that appear in the seam signatures of the
//! unported callees (`common/config_info.c`, `utils/adt/datetime.c`) and of the
//! file-access helper.

#![allow(non_upper_case_globals)]

/// `SECS_PER_HOUR` (`datatype/timestamp.h`).
pub const SECS_PER_HOUR: i32 = 3600;

/// `TOKMAXLEN` (`utils/datetime.h`) — maximum length of a date/time token,
/// which bounds a timezone abbreviation's stored length.
pub const TOKMAXLEN: i32 = 10;

// ---- pg_config.c --------------------------------------------------------

/// One row of `pg_config()` output: a `ConfigData` entry (`common/config_info.h`).
///
/// Returned by the `get_configdata` seam.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigDataRow {
    /// `configdata[i].name` — the setting name (e.g. `BINDIR`).
    pub name: String,
    /// `configdata[i].setting` — the value, never NULL in C output.
    pub setting: String,
}

// ---- tzparser.c ---------------------------------------------------------

/// A parsed timezone abbreviation entry (`struct tzEntry`).
///
/// `zone` is `Some` for a dynamic abbreviation (a zone name); for that case
/// `offset`/`is_dst` are unused, exactly as in C.  `lineno`/`filename` are kept
/// for duplicate-conflict diagnostics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TzEntry {
    /// TZ abbreviation (downcased by `validate_tz_entry`).
    pub abbrev: String,
    /// Zone name if a dynamic abbreviation, else `None`.
    pub zone: Option<String>,
    /// Offset in seconds from UTC (only meaningful when `zone` is `None`).
    pub offset: i32,
    /// True if a DST abbreviation.
    pub is_dst: bool,
    /// Source line number (for error messages).
    pub lineno: i32,
    /// Source file name (for error messages).
    pub filename: String,
}

/// The opaque, `datetime.c`-owned result of the `convert_time_zone_abbrevs`
/// seam.  In C this is a `guc_malloc`'d `TimeZoneAbbrevTable`; here it is the
/// owned value the seam implementation produces from the parsed entries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TimeZoneAbbrevTable {
    /// The converted entries, in the `datetime.c` storage format.
    pub abbrevs: Vec<TzEntry>,
}

/// How the `read_tz_file` seam reports a failure to open the timezone file, so
/// the caller can reproduce the C error-message selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TzFileOpenError {
    /// The `timezonesets` directory itself could not be opened (likely an
    /// incomplete installation).  Carries the `%m`-expanded directory error and
    /// the `my_exec_path` used in the hint.
    DirectoryMissing { dir_error: String, exec_path: String },
    /// The directory exists but the file is missing (`ENOENT`).  At depth 0 the
    /// GUC machinery's own "invalid value" message suffices, so no message is
    /// emitted here.
    FileNotFound,
    /// The directory exists but the file could not be read for another reason;
    /// carries the `%m`-expanded file error.
    FileUnreadable { file_error: String },
}

/// Outcome of attempting to read a timezone file (the `read_tz_file` seam's
/// return type).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TzFileResult {
    /// The file's lines, in order (without trailing newlines).
    Lines(Vec<String>),
    /// The file could not be opened.
    Open(TzFileOpenError),
    /// A read error occurred partway through (the C `ferror` path).
    ReadError,
    /// A line exceeded the 1023-byte `tzbuf` limit.
    LineTooLong { lineno: i32 },
}
