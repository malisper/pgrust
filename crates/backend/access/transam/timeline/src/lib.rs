#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use elog::ereport;
use elog::errno::current_errno;
use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_core::{TimeLineID, XLogRecPtr};
use types_error::{ErrorLocation, PgResult, ERROR, FATAL};

pub use timeline_seams::TimeLineHistoryEntry;

const InvalidXLogRecPtr: XLogRecPtr = 0;
const XLOGDIR: &str = "pg_wal";
const BLCKSZ: usize = 8192;
// fgets(fline, MAXPGPATH, fd): a physical line longer than MAXPGPATH-1 bytes is
// split into chunks, each parsed as its own line.
const FGETS_MAX: usize = 1024 - 1;

const PG_WAIT_IO: u32 = 0x0A00_0000;
const WAIT_EVENT_TIMELINE_HISTORY_FILE_SYNC: u32 = PG_WAIT_IO + 57;
const WAIT_EVENT_TIMELINE_HISTORY_FILE_WRITE: u32 = PG_WAIT_IO + 58;
const WAIT_EVENT_TIMELINE_HISTORY_READ: u32 = PG_WAIT_IO + 59;
const WAIT_EVENT_TIMELINE_HISTORY_SYNC: u32 = PG_WAIT_IO + 60;
const WAIT_EVENT_TIMELINE_HISTORY_WRITE: u32 = PG_WAIT_IO + 61;

#[track_caller]
fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

// The shared errno TLS cell + unlink, through the fd-crate front (DST P1).
use fd::set_errno;

fn unlink_path(path: &str) {
    let _ = fd::pg_unlink(path);
}

pub fn TLHistoryFileName(tli: TimeLineID) -> String {
    format!("{tli:08X}.history")
}

pub fn TLHistoryFilePath(tli: TimeLineID) -> String {
    format!("{XLOGDIR}/{tli:08X}.history")
}

// RestoreArchivedFile(path, histfname, "RECOVERYHISTORY", 0, false), reduced to
// (path, restored): the uninstalled seam is C's no-restore_command behavior
// (nothing restored, path left pointing into pg_wal).
fn restore_history_from_archive(tli: TimeLineID) -> PgResult<(String, bool)> {
    let histfname = TLHistoryFileName(tli);
    if xlogarchive_seams::restore_archived_file::is_installed() {
        if let Some(path) =
            xlogarchive_seams::restore_archived_file::call(&histfname, "RECOVERYHISTORY", 0, false)?
        {
            return Ok((path, true));
        }
    }
    Ok((TLHistoryFilePath(tli), false))
}

pub fn restoreTimeLineHistoryFiles(begin: TimeLineID, end: TimeLineID) -> PgResult<()> {
    let mut tli = begin;
    while tli < end {
        if tli != 1 {
            let histfname = TLHistoryFileName(tli);
            let (path, restored) = restore_history_from_archive(tli)?;
            if restored {
                xlogarchive_seams::keep_file_restored_from_archive::call(&path, &histfname)?;
            }
        }
        tli = tli.wrapping_add(1);
    }
    Ok(())
}

fn single_entry_history(
    mcx: Mcx<'_>,
    tli: TimeLineID,
) -> PgResult<PgVec<'_, TimeLineHistoryEntry>> {
    let mut result = vec_with_capacity_in(mcx, 1)?;
    result.push(TimeLineHistoryEntry {
        tli,
        begin: InvalidXLogRecPtr,
        end: InvalidXLogRecPtr,
    });
    Ok(result)
}

fn read_history_file_or_absent(path: &str, func: &'static str) -> PgResult<Option<Vec<u8>>> {
    let fdnum = fd::AllocateFile(path, "r")?;
    if fdnum < 0 {
        if current_errno() != libc::ENOENT {
            ereport(FATAL)
                .with_saved_errno(current_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not open file \"{path}\": %m"))
                .finish(loc(func))?;
        }
        return Ok(None);
    }
    waitevent_seams::pgstat_report_wait_start::call(WAIT_EVENT_TIMELINE_HISTORY_READ);
    let read = fd::with_allocated_stdio(fdnum, |f| {
        use std::io::Read;
        let mut contents = Vec::new();
        f.read_to_end(&mut contents).map(|_| contents)
    });
    waitevent_seams::pgstat_report_wait_end::call();
    match read {
        Some(Ok(contents)) => {
            fd::FreeFile(fdnum)?;
            Ok(Some(contents))
        }
        other => {
            let en = other
                .and_then(|r| r.err())
                .and_then(|e| e.raw_os_error())
                .unwrap_or(0);
            fd::FreeFile(fdnum)?;
            ereport(ERROR)
                .with_saved_errno(en)
                .errcode_for_file_access()
                .errmsg(format!("could not read file \"{path}\": %m"))
                .finish(loc(func))?;
            unreachable!()
        }
    }
}

// Returns the component TLIs newest-first (C builds the List with lcons), the
// given TLI followed by its ancestors; a missing history file means the
// timeline has no parents.
pub fn readTimeLineHistory<'mcx>(
    mcx: Mcx<'mcx>,
    targetTLI: TimeLineID,
    archive_recovery_requested: bool,
) -> PgResult<PgVec<'mcx, TimeLineHistoryEntry>> {
    // Timeline 1 does not have a history file
    if targetTLI == 1 {
        return single_entry_history(mcx, targetTLI);
    }

    let (path, from_archive) = if archive_recovery_requested {
        restore_history_from_archive(targetTLI)?
    } else {
        (TLHistoryFilePath(targetTLI), false)
    };

    let contents = match read_history_file_or_absent(&path, "readTimeLineHistory")? {
        Some(contents) => contents,
        None => return single_entry_history(mcx, targetTLI),
    };

    let mut result: PgVec<'mcx, TimeLineHistoryEntry> = PgVec::new_in(mcx);
    let mut lasttli: TimeLineID = 0;
    let mut prevend: XLogRecPtr = InvalidXLogRecPtr;

    for fline in fgets_lines(&contents) {
        let first_nonspace = fline.iter().copied().find(|&b| !is_c_space(b));
        match first_nonspace {
            None | Some(b'#') => continue,
            _ => {}
        }

        let (nfields, tli, switchpoint_hi, switchpoint_lo) = sscanf_history_line(fline);
        let fline_str = latin1(fline);

        if nfields < 1 {
            ereport(FATAL)
                .errmsg(format!("syntax error in history file: {fline_str}"))
                .errhint("Expected a numeric timeline ID.")
                .finish(loc("readTimeLineHistory"))?;
        }
        if nfields != 3 {
            ereport(FATAL)
                .errmsg(format!("syntax error in history file: {fline_str}"))
                .errhint("Expected a write-ahead log switchpoint location.")
                .finish(loc("readTimeLineHistory"))?;
        }
        if !result.is_empty() && tli <= lasttli {
            ereport(FATAL)
                .errmsg(format!("invalid data in history file: {fline_str}"))
                .errhint("Timeline IDs must be in increasing sequence.")
                .finish(loc("readTimeLineHistory"))?;
        }

        lasttli = tli;
        let end = ((switchpoint_hi as u64) << 32) | switchpoint_lo as u64;
        result
            .try_reserve(1)
            .map_err(|_| mcx.oom(std::mem::size_of::<TimeLineHistoryEntry>()))?;
        result.insert(
            0,
            TimeLineHistoryEntry {
                tli,
                begin: prevend,
                end,
            },
        );
        prevend = end;
    }

    if !result.is_empty() && targetTLI <= lasttli {
        ereport(FATAL)
            .errmsg(format!("invalid data in history file \"{path}\""))
            .errhint("Timeline IDs must be less than child timeline's ID.")
            .finish(loc("readTimeLineHistory"))?;
    }

    result
        .try_reserve(1)
        .map_err(|_| mcx.oom(std::mem::size_of::<TimeLineHistoryEntry>()))?;
    result.insert(
        0,
        TimeLineHistoryEntry {
            tli: targetTLI,
            begin: prevend,
            end: InvalidXLogRecPtr,
        },
    );

    if from_archive {
        let histfname = TLHistoryFileName(targetTLI);
        xlogarchive_seams::keep_file_restored_from_archive::call(&path, &histfname)?;
    }

    Ok(result)
}

pub fn existsTimeLineHistory(
    probeTLI: TimeLineID,
    archive_recovery_requested: bool,
) -> PgResult<bool> {
    // Timeline 1 does not have a history file
    if probeTLI == 1 {
        return Ok(false);
    }

    let path = if archive_recovery_requested {
        restore_history_from_archive(probeTLI)?.0
    } else {
        TLHistoryFilePath(probeTLI)
    };

    let fdnum = fd::AllocateFile(&path, "r")?;
    if fdnum >= 0 {
        fd::FreeFile(fdnum)?;
        Ok(true)
    } else {
        if current_errno() != libc::ENOENT {
            ereport(FATAL)
                .with_saved_errno(current_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not open file \"{path}\": %m"))
                .finish(loc("existsTimeLineHistory"))?;
        }
        Ok(false)
    }
}

pub fn findNewestTimeLine(
    startTLI: TimeLineID,
    archive_recovery_requested: bool,
) -> PgResult<TimeLineID> {
    let mut newestTLI = startTLI;
    let mut probeTLI = startTLI.wrapping_add(1);
    while existsTimeLineHistory(probeTLI, archive_recovery_requested)? {
        newestTLI = probeTLI;
        probeTLI = probeTLI.wrapping_add(1);
    }
    Ok(newestTLI)
}

fn xlog_temp_path() -> String {
    format!("{XLOGDIR}/xlogtemp.{}", init_small::globals::process_id())
}

fn create_temp_history_file(tmppath: &str, func: &'static str) -> PgResult<i32> {
    unlink_path(tmppath);
    // do not use get_sync_bit() here --- want to fsync only at end of fill
    let fd_ = fd::OpenTransientFile(tmppath, libc::O_RDWR | libc::O_CREAT | libc::O_EXCL)?;
    if fd_ < 0 {
        ereport(ERROR)
            .with_saved_errno(current_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not create file \"{tmppath}\": %m"))
            .finish(loc(func))?;
    }
    Ok(fd_)
}

fn write_all_or_unlink(
    fd_: i32,
    bytes: &[u8],
    write_off: &mut i64,
    tmppath: &str,
    wait_event: u32,
    func: &'static str,
) -> PgResult<()> {
    set_errno(0);
    waitevent_seams::pgstat_report_wait_start::call(wait_event);
    // Positional write at the tracked append offset (fresh temp file opened
    // by this module; pwrite moves the same bytes C's write did).
    let written = fd::pg_pwrite(fd_, bytes, *write_off);
    if written == bytes.len() as isize {
        *write_off += written as i64;
    }
    if written != bytes.len() as isize {
        let save_errno = current_errno();
        // If we fail to make the file, delete it to release disk space
        unlink_path(tmppath);
        // if write didn't set errno, assume problem is no disk space
        let en = if save_errno != 0 {
            save_errno
        } else {
            libc::ENOSPC
        };
        ereport(ERROR)
            .with_saved_errno(en)
            .errcode_for_file_access()
            .errmsg(format!("could not write to file \"{tmppath}\": %m"))
            .finish(loc(func))?;
    }
    waitevent_seams::pgstat_report_wait_end::call();
    Ok(())
}

fn fsync_and_close_temp(
    fd_: i32,
    tmppath: &str,
    sync_wait_event: u32,
    func: &'static str,
) -> PgResult<()> {
    waitevent_seams::pgstat_report_wait_start::call(sync_wait_event);
    if fd::pg_fsync(fd_) != 0 {
        ereport(fd::data_sync_elevel(ERROR))
            .with_saved_errno(current_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not fsync file \"{tmppath}\": %m"))
            .finish(loc(func))?;
    }
    waitevent_seams::pgstat_report_wait_end::call();

    if fd::CloseTransientFile(fd_) != 0 {
        ereport(ERROR)
            .with_saved_errno(current_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not close file \"{tmppath}\": %m"))
            .finish(loc(func))?;
    }
    Ok(())
}

pub fn writeTimeLineHistory(
    newTLI: TimeLineID,
    parentTLI: TimeLineID,
    switchpoint: XLogRecPtr,
    reason: &str,
    archive_recovery_requested: bool,
    xlog_archiving_active: bool,
) -> PgResult<()> {
    debug_assert!(newTLI > parentTLI); // else bad selection of newTLI

    let tmppath = xlog_temp_path();
    let fd_ = create_temp_history_file(&tmppath, "writeTimeLineHistory")?;
    let mut write_off: i64 = 0;

    // If a history file exists for the parent, copy it verbatim
    let path = if archive_recovery_requested {
        restore_history_from_archive(parentTLI)?.0
    } else {
        TLHistoryFilePath(parentTLI)
    };

    let srcfd = fd::OpenTransientFile(&path, libc::O_RDONLY)?;
    if srcfd < 0 {
        if current_errno() != libc::ENOENT {
            ereport(ERROR)
                .with_saved_errno(current_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not open file \"{path}\": %m"))
                .finish(loc("writeTimeLineHistory"))?;
        }
        // Not there, so assume parent has no parents
    } else {
        let mut buffer = [0u8; BLCKSZ];
        let mut src_off: i64 = 0;
        loop {
            set_errno(0);
            waitevent_seams::pgstat_report_wait_start::call(WAIT_EVENT_TIMELINE_HISTORY_READ);
            // Positional read at the tracked offset (regular history file).
            let nbytes = fd::pg_pread(srcfd, &mut buffer, src_off);
            if nbytes > 0 {
                src_off += nbytes as i64;
            }
            waitevent_seams::pgstat_report_wait_end::call();
            if nbytes < 0 || current_errno() != 0 {
                ereport(ERROR)
                    .with_saved_errno(current_errno())
                    .errcode_for_file_access()
                    .errmsg(format!("could not read file \"{path}\": %m"))
                    .finish(loc("writeTimeLineHistory"))?;
            }
            if nbytes == 0 {
                break;
            }
            write_all_or_unlink(
                fd_,
                &buffer[..nbytes as usize],
                &mut write_off,
                &tmppath,
                WAIT_EVENT_TIMELINE_HISTORY_WRITE,
                "writeTimeLineHistory",
            )?;
        }

        if fd::CloseTransientFile(srcfd) != 0 {
            ereport(ERROR)
                .with_saved_errno(current_errno())
                .errcode_for_file_access()
                .errmsg(format!("could not close file \"{path}\": %m"))
                .finish(loc("writeTimeLineHistory"))?;
        }
    }

    // If we did have a parent file, insert an extra newline just in case the
    // parent file failed to end with one.
    let line = format!(
        "{}{}\t{:X}/{:X}\t{}\n",
        if srcfd < 0 { "" } else { "\n" },
        parentTLI,
        (switchpoint >> 32) as u32,
        switchpoint as u32,
        reason,
    );
    write_all_or_unlink(
        fd_,
        line.as_bytes(),
        &mut write_off,
        &tmppath,
        WAIT_EVENT_TIMELINE_HISTORY_WRITE,
        "writeTimeLineHistory",
    )?;

    fsync_and_close_temp(
        fd_,
        &tmppath,
        WAIT_EVENT_TIMELINE_HISTORY_SYNC,
        "writeTimeLineHistory",
    )?;

    let path = TLHistoryFilePath(newTLI);
    debug_assert!({
        let mut fi = fd::FileInfo::zeroed();
        fd::pg_stat(&path, &mut fi) != 0
    });
    fd::durable_rename(&tmppath, &path, ERROR)?;

    // The history file can be archived immediately.
    if xlog_archiving_active {
        let histfname = TLHistoryFileName(newTLI);
        xlogarchive_seams::xlog_archive_notify::call(&histfname)?;
    }

    Ok(())
}

pub fn writeTimeLineHistoryFile(tli: TimeLineID, content: &[u8]) -> PgResult<()> {
    let tmppath = xlog_temp_path();
    let fd_ = create_temp_history_file(&tmppath, "writeTimeLineHistoryFile")?;

    let mut write_off: i64 = 0;
    write_all_or_unlink(
        fd_,
        content,
        &mut write_off,
        &tmppath,
        WAIT_EVENT_TIMELINE_HISTORY_FILE_WRITE,
        "writeTimeLineHistoryFile",
    )?;
    fsync_and_close_temp(
        fd_,
        &tmppath,
        WAIT_EVENT_TIMELINE_HISTORY_FILE_SYNC,
        "writeTimeLineHistoryFile",
    )?;

    let path = TLHistoryFilePath(tli);
    fd::durable_rename(&tmppath, &path, ERROR)?;
    Ok(())
}

pub fn tliInHistory(tli: TimeLineID, expectedTLEs: &[TimeLineHistoryEntry]) -> bool {
    expectedTLEs.iter().any(|tle| tle.tli == tli)
}

pub fn tliOfPointInHistory(
    ptr: XLogRecPtr,
    history: &[TimeLineHistoryEntry],
) -> PgResult<TimeLineID> {
    for tle in history {
        if (tle.begin == InvalidXLogRecPtr || tle.begin <= ptr)
            && (tle.end == InvalidXLogRecPtr || ptr < tle.end)
        {
            return Ok(tle.tli);
        }
    }

    ereport(ERROR)
        .errmsg_internal("timeline history was not contiguous")
        .finish(loc("tliOfPointInHistory"))?;
    unreachable!()
}

// Returns (switchpoint, nextTLI); history is newest-first, so the last
// non-matching entry seen before the match is the timeline we branched to.
pub fn tliSwitchPoint(
    tli: TimeLineID,
    history: &[TimeLineHistoryEntry],
) -> PgResult<(XLogRecPtr, TimeLineID)> {
    let mut nextTLI: TimeLineID = 0;
    for tle in history {
        if tle.tli == tli {
            return Ok((tle.end, nextTLI));
        }
        nextTLI = tle.tli;
    }

    ereport(ERROR)
        .errmsg(format!(
            "requested timeline {tli} is not in this server's history"
        ))
        .finish(loc("tliSwitchPoint"))?;
    unreachable!()
}

fn archive_recovery_requested() -> bool {
    xlogrecovery_seams::archive_recovery_requested::is_installed()
        && xlogrecovery_seams::archive_recovery_requested::call()
}

fn xlog_archiving_active() -> bool {
    xlogarchive_seams::xlog_archiving_active::is_installed()
        && xlogarchive_seams::xlog_archiving_active::call()
}

pub fn init_seams() {
    use timeline_seams as s;
    s::read_timeline_history::set(|mcx, target_tli| {
        readTimeLineHistory(mcx, target_tli, archive_recovery_requested())
    });
    s::tli_of_point_in_history::set(tliOfPointInHistory);
    s::tli_switch_point::set(tliSwitchPoint);
    s::restore_timeline_history_files::set(restoreTimeLineHistoryFiles);
    s::find_newest_timeline::set(|start_tli| {
        findNewestTimeLine(start_tli, archive_recovery_requested())
    });
    s::exists_timeline_history::set(|probe_tli| {
        existsTimeLineHistory(probe_tli, archive_recovery_requested())
    });
    s::write_timeline_history::set(|new_tli, parent_tli, switchpoint, reason| {
        writeTimeLineHistory(
            new_tli,
            parent_tli,
            switchpoint,
            reason,
            archive_recovery_requested(),
            xlog_archiving_active(),
        )
    });
}

// fgets(fline, MAXPGPATH, fd) line splitting: each chunk keeps its '\n', splits
// early when the buffer fills, and every C string op stops at an embedded NUL.
fn fgets_lines(contents: &[u8]) -> impl Iterator<Item = &[u8]> {
    contents
        .split_inclusive(|&b| b == b'\n')
        .flat_map(|line| line.chunks(FGETS_MAX))
        .map(|chunk| {
            let end = chunk.iter().position(|&b| b == 0).unwrap_or(chunk.len());
            &chunk[..end]
        })
}

fn latin1(bytes: &[u8]) -> String {
    bytes.iter().map(|&b| b as char).collect()
}

// isspace((unsigned char) c) in the C locale.
fn is_c_space(b: u8) -> bool {
    matches!(b, b' ' | 0x0c | b'\n' | b'\r' | b'\t' | 0x0b)
}

// sscanf(fline, "%u\t%X/%X", &tli, &switchpoint_hi, &switchpoint_lo): returns
// (conversion count, tli, hi, lo). Whitespace in the format matches any run of
// whitespace; the '/' literal must match exactly; %u/%X wrap on overflow and
// accept an optional sign (glibc).
fn sscanf_history_line(fline: &[u8]) -> (i32, TimeLineID, u32, u32) {
    let Some((tli, pos)) = scan_u32(fline, 0, 10) else {
        return (0, 0, 0, 0);
    };
    let pos = skip_whitespace(fline, pos);
    let Some((hi, pos)) = scan_u32(fline, pos, 16) else {
        return (1, tli, 0, 0);
    };
    if pos >= fline.len() || fline[pos] != b'/' {
        return (2, tli, hi, 0);
    }
    let Some((lo, _)) = scan_u32(fline, pos + 1, 16) else {
        return (2, tli, hi, 0);
    };
    (3, tli, hi, lo)
}

fn skip_whitespace(bytes: &[u8], mut pos: usize) -> usize {
    while pos < bytes.len() && is_c_space(bytes[pos]) {
        pos += 1;
    }
    pos
}

fn scan_u32(bytes: &[u8], pos: usize, radix: u32) -> Option<(u32, usize)> {
    let mut i = skip_whitespace(bytes, pos);
    let mut negate = false;
    if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
        negate = bytes[i] == b'-';
        i += 1;
    }
    if radix == 16
        && i + 2 < bytes.len()
        && bytes[i] == b'0'
        && (bytes[i + 1] == b'x' || bytes[i + 1] == b'X')
        && bytes[i + 2].is_ascii_hexdigit()
    {
        i += 2;
    }
    let start = i;
    let mut value: u32 = 0;
    while i < bytes.len() {
        let Some(digit) = (bytes[i] as char).to_digit(radix) else {
            break;
        };
        value = value.wrapping_mul(radix).wrapping_add(digit);
        i += 1;
    }
    if i == start {
        None
    } else {
        Some((if negate { value.wrapping_neg() } else { value }, i))
    }
}

#[cfg(test)]
mod tests;
