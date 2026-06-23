//! `backend-storage-file-reinit` — a faithful port of
//! `src/backend/storage/file/reinit.c` (reinitialization of unlogged
//! relations).
//!
//! Unlogged relations are not WAL-logged, so after a crash their contents are
//! untrustworthy. During crash recovery the startup process calls
//! [`ResetUnloggedRelations`] to walk every tablespace's per-database
//! directories and, for each unlogged relation, either (CLEANUP pass) delete the
//! stale main/FSM/VM forks or (INIT pass) re-create an empty main fork by
//! copying the relation's `_init` fork over the main fork file. The two passes
//! are driven by the [`UNLOGGED_RELATION_CLEANUP`] / [`UNLOGGED_RELATION_INIT`]
//! op flags.
//!
//! The directory walk, the relation-filename classification
//! ([`parse_filename_for_nontemp_relation`]) and the cleanup/copy bookkeeping
//! are ported 1:1 here, driving the already-ported `fd` directory/fsync API
//! (`AllocateDir`/`ReadDir`/`FreeDir`/`fsync_fname`) directly. `copy_file` is the
//! sibling `copydir` owner's function (this same unit), called directly.
//!
//! # Deviations from C (each functionally inert)
//!
//! C wraps the whole driver in a temporary `MemoryContext` and a
//! startup-progress reporting phase (`begin_startup_progress_phase` /
//! `ereport_startup_progress`). Both are pure bookkeeping with no effect on the
//! directory work, so — exactly as the src-idiomatic port documents — they are
//! omitted here. Everything that touches the filesystem or decides which files
//! to remove/copy is ported faithfully. The CLEANUP-pass init-fork set, a
//! `HASH_BLOBS` hashtable of `RelFileNumber`s in C, is a sorted `Vec` here
//! (`HASH_ENTER` = insert-if-absent, `HASH_FIND` = binary search).
//!
//! This crate owns `ResetUnloggedRelations` and
//! `parse_filename_for_nontemp_relation`; nothing in the repo reaches them
//! through a seam yet (the startup process is not ported), so its `init_seams`
//! is empty.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// `PgError` is a large owned struct, so the un-boxed `Err` variant trips
// `clippy::result_large_err`; the un-boxed return is the project's error
// contract, so accept the lint crate-wide.
#![allow(clippy::result_large_err)]

use std::collections::TryReserveError;

use copydir::copy_file;
use fd::allocated_desc::{AllocateDir, FreeDir, ReadDir};
use fd::sync_cleanup::fsync_fname;
use utils_error::{ereport, errno::sqlstate_for_file_access, elog};
use types_core::primitive::{
    ForkNumber, RelFileNumber, FSM_FORKNUM, INIT_FORKNUM, MAIN_FORKNUM, VISIBILITYMAP_FORKNUM,
};
use types_error::{PgError, PgResult, DEBUG2, ERRCODE_OUT_OF_MEMORY, ERROR, LOG};
use types_storage::file::{PG_TBLSPC_DIR, TABLESPACE_VERSION_DIRECTORY};

/// `ResetUnloggedRelationsOp` (reinit.h) — the bitmask of passes to run.
pub type ResetUnloggedRelationsOp = i32;

/// `#define UNLOGGED_RELATION_CLEANUP 0x0001` — delete the stale main/FSM/VM
/// forks of unlogged relations.
pub const UNLOGGED_RELATION_CLEANUP: ResetUnloggedRelationsOp = 0x0001;
/// `#define UNLOGGED_RELATION_INIT 0x0002` — re-create empty main forks from the
/// relations' `_init` forks.
pub const UNLOGGED_RELATION_INIT: ResetUnloggedRelationsOp = 0x0002;

/// A decoded non-temporary relation filename
/// (`parse_filename_for_nontemp_relation`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParsedRelationFilename {
    /// The relation's `RelFileNumber` (the leading decimal field).
    pub relnumber: RelFileNumber,
    /// The fork the file belongs to (`MAIN`/`FSM`/`VM`/`INIT`).
    pub fork: ForkNumber,
    /// The segment number (`0` when there is no `.<segno>` suffix).
    pub segno: u32,
}

/// This crate owns no inward seams (the startup process that drives
/// `ResetUnloggedRelations` is not ported, so nothing reaches it through a
/// seam). The empty `init_seams` keeps the every-crate-installs-its-seams
/// contract uniform.
pub fn init_seams() {}

/// `ResetUnloggedRelations(op)` (reinit.c:46-100) — the crash-recovery driver.
///
/// Processes the default tablespace (`$PGDATA/base`) and then every non-default
/// tablespace under `$PGDATA/pg_tblspc`. C wraps this in a temporary memory
/// context + startup-progress phase (omitted here; see the module docs). The
/// per-tablespace work is [`ResetUnloggedRelationsInTablespaceDir`].
pub fn ResetUnloggedRelations(op: ResetUnloggedRelationsOp) -> PgResult<()> {
    // reinit.c:56-58 -- log the requested passes.
    elog(
        DEBUG2,
        format!(
            "resetting unlogged relations: cleanup {} init {}",
            i32::from((op & UNLOGGED_RELATION_CLEANUP) != 0),
            i32::from((op & UNLOGGED_RELATION_INIT) != 0),
        ),
    )?;

    // reinit.c:60-70 -- C creates a temporary memory context and begins a
    // startup-progress reporting phase. Both are bookkeeping with no functional
    // effect on the directory work, so they are omitted in the port.

    // reinit.c:73-75 -- first process unlogged files in the default tablespace
    // ($PGDATA/base).
    ResetUnloggedRelationsInTablespaceDir("base", op)?;

    // reinit.c:77-93 -- then cycle through every non-default tablespace under
    // pg_tblspc. AllocateDir resolves "pg_tblspc" relative to $PGDATA's cwd,
    // exactly as in C.
    let spc_dir = AllocateDir(PG_TBLSPC_DIR)?;
    while let Some(spc_de) = ReadDir(spc_dir, PG_TBLSPC_DIR)? {
        // reinit.c:84-86 -- skip "." and "..".
        if spc_de.d_name == "." || spc_de.d_name == ".." {
            continue;
        }
        // reinit.c:88-90 -- temp_path = "pg_tblspc/<name>/<version>".
        let temp_path = format!(
            "{PG_TBLSPC_DIR}/{}/{TABLESPACE_VERSION_DIRECTORY}",
            spc_de.d_name
        );
        ResetUnloggedRelationsInTablespaceDir(&temp_path, op)?;
    }
    FreeDir(spc_dir)?;

    // reinit.c:95-99 -- restore/delete the temporary memory context (omitted;
    // see above).
    Ok(())
}

/// `ResetUnloggedRelationsInTablespaceDir(tsdirname, op)` (reinit.c:105-155) —
/// walk the per-database directories under one tablespace.
///
/// Each child whose name is all-decimal-digits is a per-database directory and
/// is handed to [`ResetUnloggedRelationsInDbspaceDir`]. A missing tablespace
/// directory (`ENOENT`) is logged and treated as empty, matching C.
fn ResetUnloggedRelationsInTablespaceDir(
    tsdirname: &str,
    op: ResetUnloggedRelationsOp,
) -> PgResult<()> {
    // reinit.c:112-129 -- AllocateDir returning NULL with errno==ENOENT is the
    // "tablespace went away" case: log at LOG and return. The ported AllocateDir
    // signals "could not open" as Ok(None); reading it at LOG reproduces the
    // ereport(LOG, errcode_for_file_access, "could not open directory") and the
    // early return. Any other open error is reported (and propagated) by
    // AllocateDir itself at ERROR.
    let ts_dir = AllocateDir(tsdirname)?;
    if ts_dir.is_none() {
        // reinit.c:122-128 -- ereport(LOG, ...) "could not open directory".
        elog(LOG, format!("could not open directory \"{tsdirname}\""))?;
        return Ok(());
    }

    while let Some(de) = ReadDir(ts_dir, tsdirname)? {
        // reinit.c:131-139 -- skip any name that is not entirely decimal digits
        // (this also skips "." and ".."); only per-database (numeric) dirs are
        // processed.
        if de.d_name.is_empty() || !de.d_name.bytes().all(|b: u8| b.is_ascii_digit()) {
            continue;
        }

        // reinit.c:141-142 -- dbspace_path = "<tsdirname>/<dbname>".
        let dbspace_path = format!("{tsdirname}/{}", de.d_name);

        // reinit.c:144-149 -- C reports startup progress (init / cleanup) here.
        // That is pure progress logging with no functional effect, so it is
        // omitted in the port (see the module docs).

        ResetUnloggedRelationsInDbspaceDir(&dbspace_path, op)?;
    }
    FreeDir(ts_dir)?;

    Ok(())
}

/// `ResetUnloggedRelationsInDbspaceDir(dbspacedirname, op)` (reinit.c:160-367) —
/// do the per-database fork work.
///
/// CLEANUP pass: collect the set of relations that have an `_init` fork, then
/// delete every non-init fork file belonging to one of those relations.
///
/// INIT pass: copy each `_init` fork over the corresponding main fork (creating
/// an empty main fork), then fsync the new main forks and the directory.
fn ResetUnloggedRelationsInDbspaceDir(
    dbspacedirname: &str,
    op: ResetUnloggedRelationsOp,
) -> PgResult<()> {
    // reinit.c:168 -- Assert at least one operation is specified.
    debug_assert!(op & (UNLOGGED_RELATION_CLEANUP | UNLOGGED_RELATION_INIT) != 0);

    // -----------------------------------------------------------------------
    // reinit.c:170-271 -- First pass: clean up unlogged relations' forks.
    // -----------------------------------------------------------------------
    if op & UNLOGGED_RELATION_CLEANUP != 0 {
        // reinit.c:177-191 -- a hash of the RelFileNumbers that have an _init
        // fork. We use a sorted Vec as the membership set (HASH_BLOBS over an
        // Oid key): try_reserve keeps it OOM-safe, and binary search gives the
        // HASH_FIND lookup. Duplicates are not inserted, matching HASH_ENTER.
        let mut init_relnumbers: Vec<RelFileNumber> = Vec::new();

        // reinit.c:193-215 -- first dir scan: record every relation that has an
        // _init fork.
        let dbspace_dir = AllocateDir(dbspacedirname)?;
        while let Some(de) = ReadDir(dbspace_dir, dbspacedirname)? {
            let Some(parsed) = parse_filename_for_nontemp_relation(&de.d_name) else {
                continue;
            };
            // reinit.c:207-208 -- we're only interested in the init forks.
            if parsed.fork != INIT_FORKNUM {
                continue;
            }
            // reinit.c:214 -- hash_search(HASH_ENTER): record the relnumber.
            if let Err(pos) = init_relnumbers.binary_search(&parsed.relnumber) {
                reserve_one(&mut init_relnumbers)?;
                init_relnumbers.insert(pos, parsed.relnumber);
            }
        }
        FreeDir(dbspace_dir)?;

        // reinit.c:217-228 -- if we didn't find any init forks, there's no
        // point in continuing; bail out now (C `return`s from the whole
        // function here).
        if init_relnumbers.is_empty() {
            return Ok(());
        }

        // reinit.c:230-271 -- second dir scan: remove the non-init forks of any
        // relation that has an _init fork.
        let dbspace_dir = AllocateDir(dbspacedirname)?;
        while let Some(de) = ReadDir(dbspace_dir, dbspacedirname)? {
            let Some(parsed) = parse_filename_for_nontemp_relation(&de.d_name) else {
                continue;
            };
            // reinit.c:246-248 -- never remove an init fork.
            if parsed.fork == INIT_FORKNUM {
                continue;
            }
            // reinit.c:250-254 -- skip files of relations that have no init fork
            // (i.e. logged relations sharing the directory).
            if init_relnumbers.binary_search(&parsed.relnumber).is_err() {
                continue;
            }

            // reinit.c:256-265 -- this is an unlogged relation's non-init fork:
            // remove it.
            let rm_path = format!("{dbspacedirname}/{}", de.d_name);
            match std::fs::remove_file(&rm_path) {
                Err(error) => {
                    // reinit.c:258-262 -- ereport(ERROR, ...) "could not remove
                    // file".
                    return Err(io_error(format!("could not remove file \"{rm_path}\""), &error));
                }
                Ok(()) => {
                    // reinit.c:264 -- elog(DEBUG2, "unlinked file \"%s\"").
                    elog(DEBUG2, format!("unlinked file \"{rm_path}\""))?;
                }
            }
        }
        FreeDir(dbspace_dir)?;
    }

    // -----------------------------------------------------------------------
    // reinit.c:273-366 -- Second pass: copy each init fork over the main fork.
    // -----------------------------------------------------------------------
    if op & UNLOGGED_RELATION_INIT != 0 {
        // reinit.c:282-318 -- scan and, for each _init fork, copy it to the
        // matching main fork path.
        let dbspace_dir = AllocateDir(dbspacedirname)?;
        while let Some(de) = ReadDir(dbspace_dir, dbspacedirname)? {
            let Some(parsed) = parse_filename_for_nontemp_relation(&de.d_name) else {
                continue;
            };
            // reinit.c:297-299 -- we're only interested in the init forks.
            if parsed.fork != INIT_FORKNUM {
                continue;
            }

            // reinit.c:301-303 -- srcpath = "<dir>/<initforkname>".
            let srcpath = format!("{dbspacedirname}/{}", de.d_name);

            // reinit.c:305-311 -- dstpath = "<dir>/<relnumber>" or
            // "<dir>/<relnumber>.<segno>".
            let dstpath = main_fork_path(dbspacedirname, parsed.relnumber, parsed.segno);

            // reinit.c:314 -- elog(DEBUG2, "copying %s to %s").
            elog(DEBUG2, format!("copying {srcpath} to {dstpath}"))?;
            // reinit.c:315 -- copy_file(srcpath, dstpath); owned by copydir.c.
            copy_file(&srcpath, &dstpath)?;
        }
        FreeDir(dbspace_dir)?;

        // reinit.c:320-355 -- copy_file() above has already called
        // pg_flush_data() on the files it created. Now fsync those files in a
        // separate pass, so the kernel can perform all flushes (especially the
        // metadata ones) at once.
        let dbspace_dir = AllocateDir(dbspacedirname)?;
        while let Some(de) = ReadDir(dbspace_dir, dbspacedirname)? {
            let Some(parsed) = parse_filename_for_nontemp_relation(&de.d_name) else {
                continue;
            };
            // reinit.c:340-342 -- we're only interested in the init forks.
            if parsed.fork != INIT_FORKNUM {
                continue;
            }

            // reinit.c:344-350 -- mainpath = "<dir>/<relnumber>[.<segno>]".
            let mainpath = main_fork_path(dbspacedirname, parsed.relnumber, parsed.segno);

            // reinit.c:352 -- fsync_fname(mainpath, false).
            fsync_fname(&mainpath, false)?;
        }
        FreeDir(dbspace_dir)?;

        // reinit.c:357-365 -- fsync the database directory itself so the file
        // creations are durably visible.
        fsync_fname(dbspacedirname, true)?;
    }

    Ok(())
}

/// reinit.c:305-311 / :344-350 -- the main-fork pathname: `<dir>/<relnumber>`
/// for segment 0, `<dir>/<relnumber>.<segno>` otherwise.
fn main_fork_path(dbspacedirname: &str, relnumber: RelFileNumber, segno: u32) -> String {
    if segno == 0 {
        format!("{dbspacedirname}/{relnumber}")
    } else {
        format!("{dbspacedirname}/{relnumber}.{segno}")
    }
}

/// `parse_filename_for_nontemp_relation(name, ...)` (reinit.c:379-453).
///
/// Returns `Some` with the decoded `(relnumber, fork, segno)` iff `name` is a
/// valid non-temporary relation filename of the form
/// `<relnumber>[ _<forkname>][ .<segno>]`, where `<relnumber>` and `<segno>` are
/// non-zero decimal numbers with no leading zero (the leading character must be
/// `1..=9`) that fit in a `uint32`, and `<forkname>` is one of `fsm`/`vm`/`init`
/// (the main fork has no suffix). Anything else (temp files, dirs, junk) yields
/// `None`. Mirrors C's `bool` return + out-params.
pub fn parse_filename_for_nontemp_relation(name: &str) -> Option<ParsedRelationFilename> {
    let bytes = name.as_bytes();

    // reinit.c:402-403 -- if it doesn't start with 1..=9, it isn't a relation
    // file (this also rejects "." / ".." and a leading-zero relnumber).
    if !matches!(bytes.first(), Some(b'1'..=b'9')) {
        return None;
    }

    // reinit.c:409-413 -- strtoul the relnumber; reject overflow / out-of-range
    // (n > PG_UINT32_MAX) and n == 0 (n is unsigned, so `n <= 0` is `n == 0`).
    let mut pos = 0;
    while bytes.get(pos).is_some_and(u8::is_ascii_digit) {
        pos += 1;
    }
    let relnumber = parse_u32(&name[..pos])?;
    if relnumber == 0 {
        return None;
    }

    // reinit.c:415-426 -- an optional "_<forkname>" suffix; absence means the
    // main fork.
    let fork = if bytes.get(pos) == Some(&b'_') {
        let fork_text = &name[pos + 1..];
        // reinit.c:422-424 -- forkname_chars returns the matched fork name
        // length, or <= 0 on no match.
        let (consumed, fork) = forkname_chars(fork_text)?;
        pos += consumed + 1;
        fork
    } else {
        MAIN_FORKNUM
    };

    // reinit.c:428-442 -- an optional ".<segno>" suffix; absence means segment 0.
    let segno = if bytes.get(pos) == Some(&b'.') {
        pos += 1;
        let start = pos;
        // reinit.c:434-435 -- the segment must start with 1..=9 (no leading
        // zero, and not zero).
        if !matches!(bytes.get(pos), Some(b'1'..=b'9')) {
            return None;
        }
        while bytes.get(pos).is_some_and(u8::is_ascii_digit) {
            pos += 1;
        }
        // reinit.c:437-441 -- strtoul; reject overflow / out-of-range.
        parse_u32(&name[start..pos])?
    } else {
        0
    };

    // reinit.c:444-446 -- the file name must be fully consumed (no trailing
    // junk).
    if pos != bytes.len() {
        return None;
    }

    Some(ParsedRelationFilename {
        relnumber,
        fork,
        segno,
    })
}

/// `forkname_chars(str, fork)` (relpath.c:81) restricted to the suffixes
/// reinit.c cares about: returns `(matched_len, fork)` for `fsm`/`vm`/`init`.
///
/// reinit.c only reaches this on a relation file's `_<suffix>` part, where the
/// only valid forks are `fsm`, `vm`, and `init` (the main fork has no suffix).
/// C's `forkname_chars` walks `forkNames[]` and matches the leading prefix; here
/// the three relevant fork names are matched directly. The shared `fd` module
/// owns a private `forkname_chars`, so this re-states the small lookup in-crate.
fn forkname_chars(name: &str) -> Option<(usize, ForkNumber)> {
    for (forkname, fork) in [
        ("fsm", FSM_FORKNUM),
        ("vm", VISIBILITYMAP_FORKNUM),
        ("init", INIT_FORKNUM),
    ] {
        if name.starts_with(forkname) {
            return Some((forkname.len(), fork));
        }
    }
    None
}

/// Parse a non-negative decimal that fits in a `u32`. The callers already
/// guarantee the first character is `1..=9` (so there is never a leading zero
/// and the value is non-zero), matching C's `strtoul` + range check.
fn parse_u32(text: &str) -> Option<u32> {
    let value = text.parse::<u64>().ok()?;
    if value > u32::MAX as u64 {
        return None;
    }
    Some(value as u32)
}

/// Build the `ereport(ERROR, (errcode_for_file_access(), errmsg("..%m")))`
/// `PgError` from a `std::io::Error`: the SQLSTATE and the `%m` rendering both
/// come from the OS errno (defaulting to `EIO` when `std` recorded none).
fn io_error(message: String, error: &std::io::Error) -> PgError {
    let errno = error
        .raw_os_error()
        .unwrap_or(utils_error::errno::EIO);
    ereport(ERROR)
        .errcode(sqlstate_for_file_access(errno))
        .with_saved_errno(errno)
        .errmsg(format!("{message}: {error}"))
        .into_error()
}

/// Reserve room for one more element OOM-safely (never let a data-derived
/// allocation `panic` on OOM).
fn reserve_one<T>(vec: &mut Vec<T>) -> PgResult<()> {
    vec.try_reserve(1).map_err(out_of_memory)
}

fn out_of_memory(_: TryReserveError) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_OUT_OF_MEMORY)
        .errmsg("out of memory")
        .into_error()
}
