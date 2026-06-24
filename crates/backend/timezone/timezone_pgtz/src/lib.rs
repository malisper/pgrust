//! Port of PostgreSQL's `src/timezone/pgtz.c`: the timezone glue / cache /
//! filesystem island.
//!
//! Owns the timezone data-directory lookup (`pg_TZDIR`), case-insensitive
//! timezone file open (`pg_open_tzfile`), the per-backend timezone cache
//! (`pg_tzset` / `pg_tzset_offset`), initial GMT setup
//! (`pg_timezone_initialize`), and timezone enumeration
//! (`pg_tzenumerate_*`). The `session_timezone` / `log_timezone` globals live
//! in `state-pgtz`.
//!
//! TZif parsing / transition math (`tzload`/`tzparse`/`pg_tz_acceptable`) lives
//! in `backend-timezone-localtime` and is called directly. The OS directory /
//! file edges (`AllocateDir`/`ReadDir`/`get_dirent_type`) and the share-path
//! derivation (`get_share_path`, in unported `common/path.c`) are reached
//! through their owners' seam crates.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use ::localtime::{pg_tz_acceptable, tzload, tzparse};
use ::pgstrcasecmp::{pg_strncasecmp, pg_toupper};
use ::types_core::MAXPGPATH;
use ::types_error::{PgError, PgResult};
use ::pgtime::pgtime::state;
use ::pgtime::{pg_tz, TZ_STRLEN_MAX};

// From `datatype/timestamp.h`.
const SECS_PER_HOUR: i64 = 3600;
const SECS_PER_MINUTE: i64 = 60;

/// `static char tzdir[MAXPGPATH]` + `static bool done_tzdir` from `pg_TZDIR`:
/// the resolved timezone data directory, computed once per backend.
thread_local! {
    static TZDIR: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// Return the full pathname of the timezone data directory.
///
/// C `pg_TZDIR(void)` (non-`SYSTEMTZDIR` configuration): the timezone data is
/// under the install's share directory. The result is memoized.
fn pg_tzdir() -> String {
    TZDIR.with(|cell| {
        if let Some(dir) = cell.borrow().as_ref() {
            return dir.clone();
        }

        // get_share_path(my_exec_path, tzdir);
        let my_exec_path = my_exec_path_str();
        let mut tzdir = common_path_seams::get_share_path::call(&my_exec_path);
        // strlcpy(tzdir + strlen(tzdir), "/timezone", MAXPGPATH - strlen(tzdir));
        tzdir.push_str("/timezone");
        if tzdir.len() >= MAXPGPATH {
            tzdir.truncate(MAXPGPATH - 1);
        }

        *cell.borrow_mut() = Some(tzdir.clone());
        tzdir
    })
}

/// Read `my_exec_path` (a NUL-padded `[u8; MAXPGPATH]` global) as a string.
fn my_exec_path_str() -> String {
    let buf = init_small::globals::my_exec_path();
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[..end]).into_owned()
}

/// Given a timezone name, open the timezone data file. Returns the open file
/// (and, when `want_canonical`, the canonical spelling of the name) on success,
/// `None` if the file is not found / not openable (C returns the fd, or -1).
///
/// C `pg_open_tzfile(name, canonname)` (`want_canonical` mirrors a non-NULL
/// `canonname` out-buffer). The name is searched for case-insensitively.
/// Read an entire tz file's bytes. Natively `std::fs::read`; on wasm (where
/// `std::fs` is inert) the host VFS via the libc shim's `fscompat::read`.
fn read_tzfile(path: &str) -> std::io::Result<Vec<u8>> {
    #[cfg(not(target_family = "wasm"))]
    {
        std::fs::read(path)
    }
    #[cfg(target_family = "wasm")]
    {
        wasm_libc_shim::fscompat::read(path)
    }
}

pub fn pg_open_tzfile(name: &str, want_canonical: bool) -> Option<(Vec<u8>, Option<String>)> {
    // strlcpy(fullname, pg_TZDIR(), sizeof(fullname));
    let tzdir = pg_tzdir();
    let orignamelen = tzdir.len();

    // if (fullnamelen + 1 + strlen(name) >= MAXPGPATH) return -1;
    if orignamelen + 1 + name.len() >= MAXPGPATH {
        return None;
    }

    // If the caller doesn't need the canonical spelling, first just try to open
    // the name as-is.
    if !want_canonical {
        let asis = format!("{tzdir}/{name}");
        if let Ok(bytes) = read_tzfile(&asis) {
            return Some((bytes, None));
        }
        // Fall through to do it the hard way.
    }

    // Loop to split the given name into directory levels; for each level,
    // search using scan_directory_ci().
    let mut fullname = tzdir;
    let mut fname = name;
    loop {
        let (level, rest) = match fname.find('/') {
            Some(slash) => (&fname[..slash], Some(&fname[slash + 1..])),
            None => (fname, None),
        };

        // scan_directory_ci(fullname, fname, fnamelen, fullname + .. + 1, ..)
        let Some(canon) = scan_directory_ci(&fullname, level) else {
            return None;
        };

        fullname.push('/');
        fullname.push_str(&canon);

        match rest {
            Some(r) => fname = r,
            None => break,
        }
    }

    // canonname = fullname + orignamelen + 1 (the part after the tzdir prefix
    // and its '/'), truncated to TZ_STRLEN_MAX.
    let canonname = if want_canonical {
        let mut c = fullname[orignamelen + 1..].to_string();
        if c.len() > TZ_STRLEN_MAX {
            c.truncate(TZ_STRLEN_MAX);
        }
        Some(c)
    } else {
        None
    };

    let bytes = read_tzfile(&fullname).ok()?;
    Some((bytes, canonname))
}

/// Scan `dirname` for a case-insensitive match to `fname`. Returns the actual
/// (canonical) file name if found.
///
/// C `scan_directory_ci(dirname, fname, fnamelen, canonname, canonnamelen)`.
/// The directory read uses the LOG-severity variant (C `ReadDirExtended(..,
/// LOG)`), so read failures are logged and skipped rather than raised.
fn scan_directory_ci(dirname: &str, fname: &str) -> Option<String> {
    let entries = fd_seams::read_dir_names_logged::call(dirname);
    let fbytes = fname.as_bytes();
    for entry in entries {
        // Ignore . and .., plus any other "hidden" files. Security measure to
        // prevent access to files outside the timezone directory.
        if entry.as_bytes().first() == Some(&b'.') {
            continue;
        }
        let ebytes = entry.as_bytes();
        if ebytes.len() == fbytes.len() && pg_strncasecmp(ebytes, fbytes, fbytes.len()) == 0 {
            return Some(entry);
        }
    }
    None
}

/// We keep loaded timezones in a per-backend cache so we don't reload/parse the
/// TZ definition file every time one is selected. The key is the uppercased
/// name (timezone names are matched case-insensitively).
///
/// C uses a dynahash `HTAB` of `pg_tz_cache { tznameupper, pg_tz }`; the value
/// is shared as `pg_tz *`, mirrored here by `Rc<pg_tz>`.
thread_local! {
    static TIMEZONE_CACHE: RefCell<HashMap<String, Rc<pg_tz>>> =
        RefCell::new(HashMap::new());
}

/// Load a timezone from file or from cache. Does not verify that the timezone
/// is acceptable. Returns `None` on an unknown / oversized name.
///
/// "GMT" is always interpreted via `tzparse()`, without touching the
/// filesystem (guaranteed to succeed, available before `my_exec_path` is
/// known, and quick).
///
/// C `pg_tzset(tzname)`.
pub fn pg_tzset(tzname: &str) -> PgResult<Option<Rc<pg_tz>>> {
    // if (strlen(tzname) > TZ_STRLEN_MAX) return NULL;
    if tzname.len() > TZ_STRLEN_MAX {
        return Ok(None);
    }

    // Upcase the given name for the case-insensitive cache lookup.
    let uppername: String = tzname
        .bytes()
        .map(|b| pg_toupper(b) as char)
        .collect();

    // hash_search(timezone_cache, uppername, HASH_FIND, NULL)
    if let Some(tz) = TIMEZONE_CACHE.with(|c| c.borrow().get(&uppername).cloned()) {
        return Ok(Some(tz));
    }

    let mut tzstate = state::default();
    let canonname: String;

    if uppername == "GMT" {
        // "GMT" is always sent to tzparse(), as per discussion above.
        if !tzparse(&uppername, &mut tzstate, true) {
            // This really, really should not happen ...
            return Err(PgError::error("could not initialize GMT time zone"));
        }
        // Use uppercase name as canonical.
        canonname = uppername.clone();
    } else {
        match tzload(&uppername, &mut tzstate, true, true) {
            Ok(canon) => {
                // tzload sets canonname (it was asked for the canonical name).
                canonname = canon.unwrap_or_else(|| uppername.clone());
            }
            Err(_) => {
                // if (uppername[0] == ':' || !tzparse(uppername, &tzstate, false))
                if uppername.as_bytes().first() == Some(&b':')
                    || !tzparse(&uppername, &mut tzstate, false)
                {
                    // Unknown timezone. Fail our call instead of loading GMT!
                    return Ok(None);
                }
                // For POSIX timezone specs, use uppercase name as canonical.
                canonname = uppername.clone();
            }
        }
    }

    // Save timezone in the cache (hash key is the uppercased name).
    let tz = Rc::new(pg_tz::new(canonname, tzstate));
    TIMEZONE_CACHE.with(|c| c.borrow_mut().insert(uppername, tz.clone()));

    Ok(Some(tz))
}

/// Load a fixed-GMT-offset timezone (SQL `SET TIME ZONE INTERVAL 'foo'`).
/// Otherwise equivalent to `pg_tzset()`. The offset is in seconds, positive =
/// west of Greenwich (POSIX sign convention); the displayable abbreviation
/// uses ISO sign convention. Can fail (`None`) if the offset is out of the
/// range the zic library accepts.
///
/// C `pg_tzset_offset(gmtoffset)`.
pub fn pg_tzset_offset(gmtoffset: i64) -> PgResult<Option<Rc<pg_tz>>> {
    let mut absoffset = if gmtoffset < 0 { -gmtoffset } else { gmtoffset };

    // snprintf(offsetstr, "%02ld", absoffset / SECS_PER_HOUR);
    let mut offsetstr = format!("{:02}", absoffset / SECS_PER_HOUR);
    absoffset %= SECS_PER_HOUR;
    if absoffset != 0 {
        offsetstr.push_str(&format!(":{:02}", absoffset / SECS_PER_MINUTE));
        absoffset %= SECS_PER_MINUTE;
        if absoffset != 0 {
            offsetstr.push_str(&format!(":{:02}", absoffset));
        }
    }

    let tzname = if gmtoffset > 0 {
        format!("<-{offsetstr}>+{offsetstr}")
    } else {
        format!("<+{offsetstr}>-{offsetstr}")
    };

    pg_tzset(&tzname)
}

/// Initialize the timezone library. Called before GUC initialization so that
/// `log_timezone` has a valid value before anything could need elog.c to
/// timestamp output. `session_timezone` is set valid too.
///
/// Uses "GMT", which `pg_tzset` forces to be interpreted without touching the
/// filesystem (so this works even before `my_exec_path` is known, e.g. in an
/// EXEC_BACKEND subprocess).
///
/// C `pg_timezone_initialize(void)`.
pub fn pg_timezone_initialize() -> PgResult<()> {
    let tz = pg_tzset("GMT")?
        .expect("pg_tzset(\"GMT\") cannot fail (parsed, not loaded)");
    state_pgtz::set_session_timezone(tz.clone());
    state_pgtz::set_log_timezone(tz);
    Ok(())
}

/// Functions to enumerate available timezones.
///
/// `pg_tzenumerate_next` returns a reference into the `PgTzEnum`, so the data is
/// valid only until the next call. All data is allocated in the current memory
/// context in C; here the enumerator owns it.
const MAX_TZDIR_DEPTH: usize = 10;

/// C `struct pg_tzenum`. Where C holds open `DIR *` handles and reads one entry
/// per `pg_tzenumerate_next` call, we materialize each directory's entry names
/// up front (the directory-read OS edge is owned by the fd seam crate) and walk
/// them by index — behavior-preserving for enumeration.
pub struct PgTzEnum {
    baselen: usize,
    depth: isize,
    /// Per-depth directory entry lists and the next index to read.
    entries: Vec<(Vec<String>, usize)>,
    /// Per-depth absolute directory path (C `dirname[]`).
    dirname: Vec<String>,
    tz: pg_tz,
}

/// C `pg_tzenumerate_start(void)`.
pub fn pg_tzenumerate_start() -> PgResult<PgTzEnum> {
    let startdir = pg_tzdir();
    let baselen = startdir.len() + 1;

    // AllocateDir(startdir); the fd seam ereports if it can't be opened.
    let names = open_dir(&startdir)?;

    Ok(PgTzEnum {
        baselen,
        depth: 0,
        entries: vec![(names, 0)],
        dirname: vec![startdir],
        tz: pg_tz::new(String::new(), state::default()),
    })
}

/// C `pg_tzenumerate_next(pg_tzenum *dir)`. Returns the next loaded, acceptable
/// timezone, or `None` when enumeration is exhausted.
pub fn pg_tzenumerate_next(dir: &mut PgTzEnum) -> PgResult<Option<&pg_tz>> {
    while dir.depth >= 0 {
        let d = dir.depth as usize;

        // direntry = ReadDir(...)
        let name = {
            let (names, idx) = &mut dir.entries[d];
            let name = names.get(*idx).cloned();
            if name.is_some() {
                *idx += 1;
            }
            name
        };
        let Some(name) = name else {
            // End of this directory: FreeDir + pfree + depth--.
            dir.entries.pop();
            dir.dirname.pop();
            dir.depth -= 1;
            continue;
        };

        if name.as_bytes().first() == Some(&b'.') {
            continue;
        }

        // snprintf(fullname, "%s/%s", dir->dirname[depth], d_name);
        let fullname = format!("{}/{}", dir.dirname[d], name);

        // if (get_dirent_type(fullname, ..) == PGFILETYPE_DIR)
        if fd_seams::get_dirent_type::call(&fullname) == PGFILETYPE_DIR {
            // Step into the subdirectory.
            if dir.depth >= (MAX_TZDIR_DEPTH - 1) as isize {
                return Err(PgError::error("timezone directory stack overflow"));
            }
            let names = open_dir(&fullname)?;
            dir.depth += 1;
            dir.dirname.push(fullname);
            dir.entries.push((names, 0));
            // Start over reading in the new directory.
            continue;
        }

        // Load this timezone using tzload() not pg_tzset(), so we don't fill the
        // cache. Don't ask for the canonical spelling: we already know it.
        let relname = &fullname[dir.baselen..];
        let mut tzstate = state::default();
        // C: `if (tzload(fullname + dir->baselen, NULL, &dir->tz.state, true)
        // != 0) { continue; }` — ANY nonzero errno (ENOENT/EINVAL/ENOMEM)
        // causes the zone to be silently skipped, not raised.
        if tzload(relname, &mut tzstate, false, true).is_err() {
            // Zone could not be loaded, ignore it.
            continue;
        }

        // OK, return the canonical zone name spelling.
        dir.tz = pg_tz::new(relname.to_string(), tzstate);

        if !pg_tz_acceptable(&dir.tz) {
            // Ignore leap-second zones.
            continue;
        }

        return Ok(Some(&dir.tz));
    }

    // Nothing more found.
    Ok(None)
}

/// `pg_tzenumerate_end` is a no-op in this port: `PgTzEnum` owns all its data
/// and frees it on drop (the C version pfrees its dirname strings and FreeDir's
/// its open handles). Provided for API parity.
pub fn pg_tzenumerate_end(dir: PgTzEnum) {
    drop(dir);
}

/// `AllocateDir(name)` + the full `ReadDir` loop at ERROR severity. Returns the
/// directory's entry names (excluding `.`/`..`). Ereports if the directory
/// cannot be opened (C `AllocateDir` failure -> `ereport(ERROR, ...)`).
fn open_dir(name: &str) -> PgResult<Vec<String>> {
    fd_seams::read_dir_names::call(name)
}

/// `PGFILETYPE_DIR` from `common/file_utils.h` (the `get_dirent_type` seam
/// returns the raw `PGFileType` code).
const PGFILETYPE_DIR: i32 = 3;

/// Install this crate's inward seams.
pub fn init_seams() {
    pgtz_seams::pg_open_tzfile::set(pg_open_tzfile);
}
