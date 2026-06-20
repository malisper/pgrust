//! Port of `src/backend/tsearch/ts_utils.c` — tsearch config-file path
//! resolution and stop-word list management.
//!
//! Three functions are exposed through `backend-tsearch-ts-utils-seams` and
//! installed by [`init_seams`]:
//!
//! * `get_tsearch_config_filename` — resolve a user-supplied basename +
//!   extension to an absolute path under `$SHAREDIR/tsearch_data`, rejecting
//!   anything that could escape that directory;
//! * `readstoplist` — read a `<name>.stop` file into a sorted [`StopList`];
//! * `searchstoplist` — binary-search a sorted [`StopList`].

#![no_std]
#![allow(non_snake_case)]

extern crate alloc;

use alloc::format;
use alloc::string::String;

use mcx::{Mcx, PgString, PgVec};
use types_core::primitive::MAXPGPATH;
use types_error::{PgResult, ERRCODE_CONFIG_FILE_ERROR, ERRCODE_INVALID_PARAMETER_VALUE, ERROR};
use types_tsearch::StopList;

use backend_tsearch_ts_locale_seams::readfile;
use backend_utils_adt_formatting_seams::str_tolower;
use backend_utils_error::ereport;
use backend_utils_mb_mbutils_seams::pg_mblen_range;
use common_path_seams::get_share_path;

/// `DEFAULT_COLLATION_OID` (`pg_collation_d.h`) — the collation the stop-word
/// `str_tolower` runs under, matching the C `wordop(line, strlen(line),
/// DEFAULT_COLLATION_OID)`.
const DEFAULT_COLLATION_OID: types_core::Oid = 100;

/// `isspace((unsigned char) c)` for the C `"C"` locale (what the stop-word
/// trimming loop uses).
fn is_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

/// Read `my_exec_path` (a NUL-padded `[u8; MAXPGPATH]` global) as a string.
/// Mirrors the timezone unit's accessor.
fn my_exec_path_str() -> String {
    let buf = backend_utils_init_small::globals::my_exec_path();
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[..end]).into_owned()
}

/// `get_tsearch_config_filename(basename, extension)` (ts_utils.c:34): build the
/// absolute `$SHAREDIR/tsearch_data/<basename>.<extension>` path, validating
/// that `basename` contains only `[a-z0-9_]` so it cannot escape the
/// `tsearch_data` directory.
fn get_tsearch_config_filename<'mcx>(
    mcx: Mcx<'mcx>,
    basename: &[u8],
    extension: &[u8],
) -> PgResult<PgVec<'mcx, u8>> {
    /*
     * We limit the basename to contain a-z, 0-9, and underscores.  '/' (and on
     * some platforms '\' and ':') *must* be rejected so nothing outside
     * tsearch_data is reachable; uppercase and non-ASCII are excluded too.
     *
     * strspn(basename, "...") != strlen(basename) when any byte is not in the
     * allowed set.
     */
    let ok = basename
        .iter()
        .all(|&c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_');
    if !ok {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "invalid text search configuration file name \"{}\"",
                String::from_utf8_lossy(basename)
            ))
            .into_error());
    }

    // get_share_path(my_exec_path, sharepath);
    let sharepath = get_share_path::call(&my_exec_path_str());

    // snprintf(result, MAXPGPATH, "%s/tsearch_data/%s.%s", sharepath, basename, extension);
    let mut result: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    result
        .try_reserve(sharepath.len() + basename.len() + extension.len() + 16)
        .map_err(|_| mcx.oom(MAXPGPATH))?;
    result.extend_from_slice(sharepath.as_bytes());
    result.extend_from_slice(b"/tsearch_data/");
    result.extend_from_slice(basename);
    result.push(b'.');
    result.extend_from_slice(extension);

    /* snprintf truncates to MAXPGPATH - 1 bytes plus the NUL; we carry no NUL. */
    if result.len() > MAXPGPATH - 1 {
        result.truncate(MAXPGPATH - 1);
    }

    Ok(result)
}

/// `readstoplist(fname, s, wordop)` (ts_utils.c:69): read the `<fname>.stop`
/// config file, trim each line to its first word, drop empties, optionally
/// lowercase (the callers always pass `str_tolower`), and return the words
/// sorted so [`searchstoplist`] can binary-search.
fn readstoplist<'mcx>(mcx: Mcx<'mcx>, fname: &[u8], lowercase: bool) -> PgResult<StopList<'mcx>> {
    let mut stop: PgVec<'mcx, PgString<'mcx>> = PgVec::new_in(mcx);

    // if (fname && *fname) { ... }
    if !fname.is_empty() {
        // filename = get_tsearch_config_filename(fname, "stop");
        let filename = get_tsearch_config_filename(mcx, fname, b"stop")?;

        // if (!tsearch_readline_begin(&trst, filename)) ereport(... could not open ...);
        let content = readfile::call(&filename).map_err(|_| {
            ereport(ERROR)
                .errcode(ERRCODE_CONFIG_FILE_ERROR)
                .errmsg(format!(
                    "could not open stop-word file \"{}\": %m",
                    String::from_utf8_lossy(&filename)
                ))
                .into_error()
        })?;

        // while ((line = tsearch_readline(&trst)) != NULL) { ... }
        for line in content.split(|&b| b == b'\n') {
            /*
             * Trim trailing space: advance pbuf while *pbuf && !isspace(*pbuf),
             * then *pbuf = '\0'.  This keeps only the leading word of the line.
             */
            let mut end = 0usize;
            while end < line.len() && line[end] != 0 && !is_space(line[end]) {
                let adv = pg_mblen_range::call(&line[end..]) as usize;
                end += adv.max(1);
            }
            if end > line.len() {
                end = line.len();
            }
            let word = &line[..end];

            // Skip empty lines: if (*line == '\0') continue;
            if word.is_empty() {
                continue;
            }

            if lowercase {
                // wordop(line, strlen(line), DEFAULT_COLLATION_OID) == str_tolower
                let lowered = str_tolower::call(mcx, word, DEFAULT_COLLATION_OID)?;
                let s = bytes_to_pgstring(mcx, &lowered)?;
                stop.try_reserve(1).map_err(|_| mcx.oom(s.len()))?;
                stop.push(s);
            } else {
                let s = bytes_to_pgstring(mcx, word)?;
                stop.try_reserve(1).map_err(|_| mcx.oom(s.len()))?;
                stop.push(s);
            }
        }
    }

    /*
     * Sort to allow binary searching: qsort(s->stop, s->len, sizeof(char *),
     * pg_qsort_strcmp).  pg_qsort_strcmp is strcmp, i.e. unsigned-byte
     * lexicographic order over the NUL-terminated strings; a plain byte-slice
     * compare is identical since none of the words contain a NUL.
     */
    stop.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));

    Ok(StopList { stop })
}

/// `searchstoplist(s, key)` (ts_utils.c:141): true iff `key` is present in the
/// sorted stop list (binary search with the same `strcmp` ordering used to
/// sort).
fn searchstoplist(s: &StopList<'_>, key: &[u8]) -> bool {
    // (s->stop && s->len > 0 && bsearch(&key, s->stop, s->len, ...))
    s.stop
        .binary_search_by(|probe| probe.as_bytes().cmp(key))
        .is_ok()
}

/// `bytes_to_pgstring` — the words are database-encoding text; build a
/// `PgString` in `mcx` (mirrors the dict crates' helper).
fn bytes_to_pgstring<'mcx>(mcx: Mcx<'mcx>, b: &[u8]) -> PgResult<PgString<'mcx>> {
    let s = core::str::from_utf8(b).map_err(|_| {
        ereport(ERROR)
            .errcode(ERRCODE_CONFIG_FILE_ERROR)
            .errmsg("ts_utils: non-UTF-8 stop word")
            .into_error()
    })?;
    PgString::from_str_in(s, mcx)
}

/// Install the `ts_utils.c` seams.
pub fn init_seams() {
    backend_tsearch_ts_utils_seams::get_tsearch_config_filename::set(get_tsearch_config_filename);
    backend_tsearch_ts_utils_seams::readstoplist::set(readstoplist);
    backend_tsearch_ts_utils_seams::searchstoplist::set(searchstoplist);
}
