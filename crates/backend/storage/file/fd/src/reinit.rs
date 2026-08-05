//! reinit.c: reset unlogged relations from before the last restart.
#![allow(non_snake_case)]

use elog::ereport;
use types_core::ForkNumber;
use types_error::{ErrorLocation, PgResult, ERROR, LOG};
use types_storage::{PG_TBLSPC_DIR, TABLESPACE_VERSION_DIRECTORY};

use crate::copydir::copy_file;
use crate::desc::{AllocateDir, FreeDir, ReadDir};
use crate::vfd::get_errno;
use crate::sync::fsync_fname;

pub const UNLOGGED_RELATION_CLEANUP: i32 = 1 << 0;
pub const UNLOGGED_RELATION_INIT: i32 = 1 << 1;

#[cold]
#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

/// CLEANUP removes every non-init fork of any relation that has an init
/// fork; INIT copies each init fork over the main fork.
pub fn ResetUnloggedRelations(op: i32) -> PgResult<()> {
    if startup_seams::begin_startup_progress_phase::is_installed() {
        startup_seams::begin_startup_progress_phase::call();
    }

    ResetUnloggedRelationsInTablespaceDir("base", op)?;

    let spc_dir = AllocateDir(PG_TBLSPC_DIR)?;
    while let Some(de) = ReadDir(spc_dir, PG_TBLSPC_DIR)? {
        if de.d_name == "." || de.d_name == ".." {
            continue;
        }
        let temp_path =
            format!("{PG_TBLSPC_DIR}/{}/{TABLESPACE_VERSION_DIRECTORY}", de.d_name);
        ResetUnloggedRelationsInTablespaceDir(&temp_path, op)?;
    }
    FreeDir(spc_dir)?;
    Ok(())
}

fn ResetUnloggedRelationsInTablespaceDir(tsdirname: &str, op: i32) -> PgResult<()> {
    let ts_dir = AllocateDir(tsdirname)?;
    // A DROP TABLESPACE crash can leave the symlink without the directory;
    // don't block startup on it (C logs and returns).
    if ts_dir.is_none() && get_errno() == libc::ENOENT {
        ereport(LOG)
            .with_saved_errno(get_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not open directory \"{tsdirname}\": %m"))
            .finish(loc("ResetUnloggedRelationsInTablespaceDir"))?;
        return Ok(());
    }
    while let Some(de) = ReadDir(ts_dir, tsdirname)? {
        // Only the per-database directories (all-numeric names).
        if de.d_name.is_empty() || !de.d_name.bytes().all(|b| b.is_ascii_digit()) {
            continue;
        }
        let dbspace_path = format!("{tsdirname}/{}", de.d_name);
        // C also emits timeout-driven ereport_startup_progress lines here.
        ResetUnloggedRelationsInDbspaceDir(&dbspace_path, op)?;
    }
    FreeDir(ts_dir)?;
    Ok(())
}

fn ResetUnloggedRelationsInDbspaceDir(dbspacedirname: &str, op: i32) -> PgResult<()> {
    debug_assert!(op & (UNLOGGED_RELATION_CLEANUP | UNLOGGED_RELATION_INIT) != 0);

    if op & UNLOGGED_RELATION_CLEANUP != 0 {
        // Startup-lifetime scratch set mirroring C's throwaway HTAB in a
        // temporary context (bare std collection justified: no arena exists
        // this early and the set dies with this call).
        let mut init_rels = std::collections::HashSet::new();

        let dbspace_dir = AllocateDir(dbspacedirname)?;
        while let Some(de) = ReadDir(dbspace_dir, dbspacedirname)? {
            let Some((relnumber, fork, _segno)) =
                parse_filename_for_nontemp_relation(&de.d_name)
            else {
                continue;
            };
            if fork != ForkNumber::INIT_FORKNUM {
                continue;
            }
            init_rels.insert(relnumber);
        }
        FreeDir(dbspace_dir)?;

        if init_rels.is_empty() {
            return Ok(());
        }

        let dbspace_dir = AllocateDir(dbspacedirname)?;
        while let Some(de) = ReadDir(dbspace_dir, dbspacedirname)? {
            let Some((relnumber, fork, _segno)) =
                parse_filename_for_nontemp_relation(&de.d_name)
            else {
                continue;
            };
            if fork == ForkNumber::INIT_FORKNUM {
                continue;
            }
            if init_rels.contains(&relnumber) {
                let rm_path = format!("{dbspacedirname}/{}", de.d_name);
                if vfs::unlink(&crate::vfd::cpath(&rm_path)) != 0 {
                    return Err(ereport(ERROR)
                        .with_saved_errno(get_errno())
                        .errcode_for_file_access()
                        .errmsg(format!("could not remove file \"{rm_path}\": %m"))
                        .finish(loc("ResetUnloggedRelationsInDbspaceDir"))
                        .unwrap_err());
                }
            }
        }
        FreeDir(dbspace_dir)?;
    }

    if op & UNLOGGED_RELATION_INIT != 0 {
        let dbspace_dir = AllocateDir(dbspacedirname)?;
        while let Some(de) = ReadDir(dbspace_dir, dbspacedirname)? {
            let Some((relnumber, fork, segno)) =
                parse_filename_for_nontemp_relation(&de.d_name)
            else {
                continue;
            };
            if fork != ForkNumber::INIT_FORKNUM {
                continue;
            }
            let srcpath = format!("{dbspacedirname}/{}", de.d_name);
            let dstpath = main_fork_path(dbspacedirname, relnumber, segno);
            copy_file(&srcpath, &dstpath)?;
        }
        FreeDir(dbspace_dir)?;

        // copy_file flushed data; fsync in a separate pass (no checkpoint
        // will do it during recovery), then the directory itself.
        let dbspace_dir = AllocateDir(dbspacedirname)?;
        while let Some(de) = ReadDir(dbspace_dir, dbspacedirname)? {
            let Some((relnumber, fork, segno)) =
                parse_filename_for_nontemp_relation(&de.d_name)
            else {
                continue;
            };
            if fork != ForkNumber::INIT_FORKNUM {
                continue;
            }
            fsync_fname(&main_fork_path(dbspacedirname, relnumber, segno), false)?;
        }
        FreeDir(dbspace_dir)?;

        fsync_fname(dbspacedirname, true)?;
    }
    Ok(())
}

fn main_fork_path(dbspacedirname: &str, relnumber: u32, segno: u32) -> String {
    if segno == 0 {
        format!("{dbspacedirname}/{relnumber}")
    } else {
        format!("{dbspacedirname}/{relnumber}.{segno}")
    }
}

/// parse_filename_for_nontemp_relation: `<relnumber>[_<fork>][.<segno>]`,
/// leading zeroes rejected so each value has one spelling.
pub fn parse_filename_for_nontemp_relation(name: &str) -> Option<(u32, ForkNumber, u32)> {
    let bytes = name.as_bytes();
    if !(b'1'..=b'9').contains(bytes.first()?) {
        return None;
    }
    let mut pos = 1;
    while bytes.get(pos).is_some_and(u8::is_ascii_digit) {
        pos += 1;
    }
    let relnumber: u32 = name[..pos].parse().ok()?;

    let mut fork = ForkNumber::MAIN_FORKNUM;
    if bytes.get(pos) == Some(&b'_') {
        let rest = &name[pos + 1..];
        let (f, len) = if rest.starts_with("fsm") {
            (ForkNumber::FSM_FORKNUM, 3)
        } else if rest.starts_with("vm") {
            (ForkNumber::VISIBILITYMAP_FORKNUM, 2)
        } else if rest.starts_with("init") {
            (ForkNumber::INIT_FORKNUM, 4)
        } else {
            return None;
        };
        fork = f;
        pos += len + 1;
    }

    let mut segno: u32 = 0;
    if bytes.get(pos) == Some(&b'.') {
        if !(b'1'..=b'9').contains(bytes.get(pos + 1)?) {
            return None;
        }
        let seg_start = pos + 1;
        let mut p = seg_start;
        while bytes.get(p).is_some_and(u8::is_ascii_digit) {
            p += 1;
        }
        segno = name[seg_start..p].parse().ok()?;
        pos = p;
    }

    if pos != name.len() {
        return None;
    }
    Some((relnumber, fork, segno))
}
