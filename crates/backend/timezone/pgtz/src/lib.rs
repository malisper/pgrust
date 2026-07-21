//! pgtz.c: tz directory, pg_tzset cache, session/log timezone globals.
//! Cache entries are leaked (C's dynahash entries are permanent; a `pg_tz *`
//! stays valid for the backend's life) — `&'static PgTz`, no refcounts.
//!
//! The cache is PROCESS-global and never freed. It must be: `&'static PgTz`
//! pointers escape the owning thread — `DynamicZoneAbbrev` (adt_datetime
//! tz.rs) caches them in the process-shared zone-abbreviation table, and GUC
//! extras carry them — so a session- or thread-scoped arena here turns every
//! such consumer into a use-after-free once the first resolving session ends
//! (the pg_timezone_abbrevs localtime/clock.rs:32 garbage-`defaulttype`
//! panic). C never has this problem because its dynahash lives in
//! TopMemoryContext for the life of the (single-session) process.

use core::cell::Cell;
use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

use localtime::{pg_tz_acceptable, tzload, tzparse, PgTz, TzLoadError, TzState, TZ_STRLEN_MAX};
use pgstrcasecmp::pg_toupper;
use types_core::primitive::MAXPGPATH;
use types_error::{PgError, PgResult, ERROR, LOG};

#[cfg(test)]
mod tests;

const SECS_PER_HOUR: i64 = 3600;
const SECS_PER_MINUTE: i64 = 60;

thread_local! {
    static SESSION_TIMEZONE: Cell<Option<&'static PgTz>> = const { Cell::new(None) };
    static LOG_TIMEZONE: Cell<Option<&'static PgTz>> = const { Cell::new(None) };
}

#[inline]
pub fn session_timezone() -> Option<&'static PgTz> {
    SESSION_TIMEZONE.with(Cell::get)
}

pub fn set_session_timezone(tz: Option<&'static PgTz>) {
    SESSION_TIMEZONE.with(|c| c.set(tz));
}

#[inline]
pub fn log_timezone() -> Option<&'static PgTz> {
    LOG_TIMEZONE.with(Cell::get)
}

pub fn set_log_timezone(tz: Option<&'static PgTz>) {
    LOG_TIMEZONE.with(|c| c.set(tz));
}

// DIVERGENCE from pg_TZDIR: PGRUST_TZDIR (runtime) / PGRUST_PGSHAREDIR
// (build) take precedence over C's get_share_path resolution (harness
// override; boot-smoke.sh exports them).
fn pg_tzdir() -> &'static str {
    static TZDIR: OnceLock<String> = OnceLock::new();
    TZDIR.get_or_init(|| {
        if let Ok(dir) = std::env::var("PGRUST_TZDIR") {
            return dir;
        }
        let mut dir = if let Some(share) = option_env!("PGRUST_PGSHAREDIR") {
            String::from(share)
        } else {
            let exec = init_small::globals::my_exec_path();
            let len = exec.iter().position(|&b| b == 0).unwrap_or(exec.len());
            pg_path::get_share_path(&String::from_utf8_lossy(&exec[..len]))
        };
        dir.push_str("/timezone");
        dir.truncate(MAXPGPATH - 1);
        dir
    })
}

fn read_file_into(path: &str, buf: &mut [u8]) -> Option<usize> {
    use std::io::Read;
    let mut file = std::fs::File::open(path).ok()?;
    let mut nread = 0usize;
    while nread < buf.len() {
        match file.read(&mut buf[nread..]) {
            Ok(0) => break,
            Ok(n) => nread += n,
            Err(_) => return None,
        }
    }
    Some(nread)
}

/// pg_open_tzfile + tzload's single read; `Ok(None)` is C's -1.
pub fn pg_open_tzfile(
    name: &[u8],
    canonname: Option<&mut [u8; TZ_STRLEN_MAX + 1]>,
    buf: &mut [u8],
) -> PgResult<Option<usize>> {
    let tzdir = pg_tzdir();
    let orignamelen = tzdir.len();

    if orignamelen + 1 + name.len() >= MAXPGPATH {
        return Ok(None); /* not gonna fit */
    }

    // Zone names are ASCII; non-UTF-8 cannot match an openable file.
    let Ok(name_str) = core::str::from_utf8(name) else {
        return Ok(None);
    };

    if canonname.is_none() {
        let mut asis = String::with_capacity(orignamelen + 1 + name.len());
        asis.push_str(tzdir);
        asis.push('/');
        asis.push_str(name_str);
        if let Some(n) = read_file_into(&asis, buf) {
            return Ok(Some(n));
        }
    }

    let mut fullname = String::with_capacity(MAXPGPATH);
    fullname.push_str(tzdir);
    let mut fname = name_str;
    loop {
        let (level, rest) = match fname.find('/') {
            Some(slash) => (&fname[..slash], Some(&fname[slash + 1..])),
            None => (fname, None),
        };
        let Some(canon) = scan_directory_ci(&fullname, level.as_bytes())? else {
            return Ok(None);
        };
        fullname.push('/');
        fullname.push_str(&canon);
        match rest {
            Some(r) => fname = r,
            None => break,
        }
    }

    if let Some(canonname) = canonname {
        let canonical = fullname[orignamelen + 1..].as_bytes();
        let n = canonical.len().min(TZ_STRLEN_MAX);
        canonname[..n].copy_from_slice(&canonical[..n]);
        canonname[n] = 0;
    }

    Ok(read_file_into(&fullname, buf))
}

// Hidden entries are skipped (security: no escape from the tz directory);
// read failures are LOG severity, as in C.
fn scan_directory_ci(dirname: &str, fname: &[u8]) -> PgResult<Option<String>> {
    let dir = fd::AllocateDir(dirname)?;
    let mut found = None;
    while let Some(entry) = fd::ReadDirExtended(dir, dirname, LOG)? {
        if entry.d_name.as_bytes().first() == Some(&b'.') {
            continue;
        }
        let ebytes = entry.d_name.as_bytes();
        if ebytes.len() == fname.len() && ebytes.eq_ignore_ascii_case(fname) {
            found = Some(entry.d_name);
            break;
        }
    }
    fd::FreeDir(dir)?;
    Ok(found)
}

// Process-lifetime cache (see the module doc): entries are Box::leak'd, the
// map itself is never dropped. Lookups are cold (SET timezone, zone-name
// decode), so a plain Mutex is fine; BTreeMap keeps the init const (no once
// site) and the iteration order deterministic.
static TIMEZONE_CACHE: Mutex<BTreeMap<Box<[u8]>, &'static PgTz>> = Mutex::new(BTreeMap::new());

#[cold]
fn escaped_report(what: &str, e: Box<PgError>) -> ! {
    panic!("pgtz: ereport escaped {what}: {}", e.message());
}

/// Load a timezone from file or cache; does not verify acceptability. "GMT"
/// always goes to tzparse(), never the filesystem, as in C.
pub fn pg_tzset(tzname: &[u8]) -> Option<&'static PgTz> {
    if tzname.len() > TZ_STRLEN_MAX {
        return None; /* not going to fit */
    }

    let mut upper = [0u8; TZ_STRLEN_MAX + 1];
    for (dst, src) in upper.iter_mut().zip(tzname.iter()) {
        *dst = pg_toupper(*src);
    }
    let uppername = &upper[..tzname.len()];

    if let Some(tz) = TIMEZONE_CACHE.lock().unwrap().get(uppername).copied() {
        return Some(tz);
    }

    let mut tzstate = Box::new(TzState::new());
    let mut canonname = [0u8; TZ_STRLEN_MAX + 1];

    if uppername == b"GMT" {
        if !tzparse(uppername, &mut tzstate, true) {
            panic!("pgtz: could not initialize GMT time zone");
        }
        canonname[..3].copy_from_slice(b"GMT");
    } else {
        match tzload(uppername, Some(&mut canonname), &mut tzstate, true) {
            Ok(()) => {}
            Err(TzLoadError::Report(e)) => escaped_report("pg_tzset", e),
            Err(_) => {
                if uppername.first() == Some(&b':') || !tzparse(uppername, &mut tzstate, false) {
                    return None;
                }
                canonname[..uppername.len()].copy_from_slice(uppername);
            }
        }
    }

    // Two threads racing on the same uncached zone both build it; the first
    // insert wins and the loser's build is dropped, so every caller — and the
    // process-shared pointer caches downstream — sees ONE permanent entry.
    let mut map = TIMEZONE_CACHE.lock().unwrap();
    Some(*map.entry(uppername.into()).or_insert_with(|| {
        Box::leak(Box::new(PgTz {
            tzname: canonname,
            state: *tzstate,
        }))
    }))
}

/// Fixed-GMT-offset zone: seconds, positive = west of Greenwich (POSIX sign
/// convention); the displayable abbreviation uses the ISO convention.
pub fn pg_tzset_offset(gmtoffset: i64) -> Option<&'static PgTz> {
    let mut absoffset = if gmtoffset < 0 { -gmtoffset } else { gmtoffset };

    let mut offsetstr = [0u8; 64];
    let mut olen = 0usize;
    push_2d(&mut offsetstr, &mut olen, absoffset / SECS_PER_HOUR);
    absoffset %= SECS_PER_HOUR;
    if absoffset != 0 {
        offsetstr[olen] = b':';
        olen += 1;
        push_2d(&mut offsetstr, &mut olen, absoffset / SECS_PER_MINUTE);
        absoffset %= SECS_PER_MINUTE;
        if absoffset != 0 {
            offsetstr[olen] = b':';
            olen += 1;
            push_2d(&mut offsetstr, &mut olen, absoffset);
        }
    }

    let mut tzname = [0u8; 136];
    let mut tlen = 0usize;
    let (open, join) = if gmtoffset > 0 {
        (b"<-".as_slice(), b">+".as_slice())
    } else {
        (b"<+".as_slice(), b">-".as_slice())
    };
    for part in [open, &offsetstr[..olen], join, &offsetstr[..olen]] {
        tzname[tlen..tlen + part.len()].copy_from_slice(part);
        tlen += part.len();
    }

    pg_tzset(&tzname[..tlen])
}

// snprintf "%02ld": two-digit zero pad, longer values unpadded.
fn push_2d(buf: &mut [u8], len: &mut usize, v: i64) {
    if v >= 100 {
        let mut digits = [0u8; 20];
        let mut n = 0usize;
        let mut v = v;
        while v > 0 {
            digits[n] = b'0' + (v % 10) as u8;
            n += 1;
            v /= 10;
        }
        for d in (0..n).rev() {
            buf[*len] = digits[d];
            *len += 1;
        }
    } else {
        buf[*len] = b'0' + (v / 10) as u8;
        buf[*len + 1] = b'0' + (v % 10) as u8;
        *len += 2;
    }
}

/// Called before GUC init so log_timezone is valid for elog timestamps.
pub fn pg_timezone_initialize() {
    let tz = pg_tzset(b"GMT");
    set_session_timezone(tz);
    set_log_timezone(tz);
}

const MAX_TZDIR_DEPTH: usize = 10;

/// C pg_tzenum: one open directory handle per depth.
pub struct PgTzEnum {
    baselen: usize,
    depth: isize,
    dirdesc: [Option<types_storage::Dir>; MAX_TZDIR_DEPTH],
    dirname: [Option<String>; MAX_TZDIR_DEPTH],
    tz: Box<PgTz>,
}

pub fn pg_tzenumerate_start() -> PgResult<PgTzEnum> {
    let startdir = pg_tzdir().to_string();
    let baselen = startdir.len() + 1;
    let dirdesc = fd::AllocateDir(&startdir)?;
    if dirdesc.is_none() {
        return Err(elog::ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not open directory \"{startdir}\": %m"))
            .into_error()
            .into());
    }
    let mut e = PgTzEnum {
        baselen,
        depth: 0,
        dirdesc: [None; MAX_TZDIR_DEPTH],
        dirname: [const { None }; MAX_TZDIR_DEPTH],
        tz: Box::new(PgTz::new(&[], TzState::new())),
    };
    e.dirdesc[0] = dirdesc;
    e.dirname[0] = Some(startdir);
    Ok(e)
}

pub fn pg_tzenumerate_end(dir: PgTzEnum) -> PgResult<()> {
    let mut dir = dir;
    while dir.depth >= 0 {
        fd::FreeDir(dir.dirdesc[dir.depth as usize].take())?;
        dir.dirname[dir.depth as usize] = None;
        dir.depth -= 1;
    }
    Ok(())
}

pub fn pg_tzenumerate_next(dir: &mut PgTzEnum) -> PgResult<Option<&PgTz>> {
    while dir.depth >= 0 {
        let d = dir.depth as usize;
        let dirname = dir.dirname[d].clone().unwrap();

        let Some(entry) = fd::ReadDir(dir.dirdesc[d], &dirname)? else {
            fd::FreeDir(dir.dirdesc[d].take())?;
            dir.dirname[d] = None;
            dir.depth -= 1;
            continue;
        };

        if entry.d_name.as_bytes().first() == Some(&b'.') {
            continue;
        }

        let mut fullname = dirname;
        fullname.push('/');
        fullname.push_str(&entry.d_name);

        // C get_dirent_type (unported common/file_utils.c): symlink-following
        // stat here.
        let meta = std::fs::metadata(&fullname).map_err(|e| {
            Box::new(
                elog::ereport(ERROR)
                    .errcode_for_file_access()
                    .errmsg(format!("could not stat file \"{fullname}\": {e}"))
                    .into_error(),
            )
        })?;
        if meta.is_dir() {
            if dir.depth >= (MAX_TZDIR_DEPTH - 1) as isize {
                return Err(
                    PgError::error("timezone directory stack overflow".to_string()).into(),
                );
            }
            let sub = fd::AllocateDir(&fullname)?;
            if sub.is_none() {
                return Err(elog::ereport(ERROR)
                    .errcode_for_file_access()
                    .errmsg(format!("could not open directory \"{fullname}\": %m"))
                    .into_error()
                    .into());
            }
            dir.depth += 1;
            dir.dirdesc[dir.depth as usize] = sub;
            dir.dirname[dir.depth as usize] = Some(fullname);
            continue;
        }

        // tzload() not pg_tzset(), so the cache isn't filled.
        let relname = fullname[dir.baselen..].as_bytes();
        match tzload(relname, None, &mut dir.tz.state, true) {
            Ok(()) => {}
            Err(TzLoadError::Report(e)) => return Err(e),
            Err(_) => continue, /* zone could not be loaded, ignore it */
        }

        let n = relname.len().min(TZ_STRLEN_MAX);
        dir.tz.tzname[..n].copy_from_slice(&relname[..n]);
        dir.tz.tzname[n] = 0;

        if !pg_tz_acceptable(&dir.tz) {
            continue; /* ignore leap-second zones */
        }

        return Ok(Some(&dir.tz));
    }

    Ok(None)
}

pub fn init_seams() {
    pgtz_seams::pg_open_tzfile::set(pg_open_tzfile);
}
