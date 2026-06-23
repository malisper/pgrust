//! Port of PostgreSQL's `src/common/relpath.c`.
//!
//! Shared frontend/backend code to compute pathnames of relation files, plus
//! some logic associated with fork names. The module is **pure string and
//! arithmetic computation**: it maps a `(dbOid, spcOid, RelFileNumber,
//! procNumber, ForkNumber)` plus a static fork-name table to the on-disk path
//! (`base/<db>/<rel>`, `global/<rel>`, or
//! `pg_tblspc/<spc>/<TABLESPACE_VERSION_DIRECTORY>/<db>/...`), and
//! [`GetDatabasePath`] likewise. It reaches **no** operating-system, catalog,
//! or other-subsystem surface.
//!
//! This crate is the genuine owner of `common/relpath.c` and installs every
//! relpath seam from [`init_seams`]: the value-shaped (owned `String`) seams in
//! `common-relpath-seams` (`relpathbackend`, `get_database_path`) consumed by
//! WAL-redo/invalidation paths that have no ambient memory context, and the
//! `Mcx`/`PgString` seams in `backend-common-relpath-seams`
//! (`get_database_path`, `relpath_backend`) consumed by call sites that thread a
//! memory context.
//!
//! Behavior is faithful to PostgreSQL 18.3 (`src/common/relpath.c`).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use ::mcx::{Mcx, PgString};
use ::types_catalog::catalog::{DEFAULTTABLESPACE_OID, GLOBALTABLESPACE_OID};
use ::types_core::primitive::{
    ForkNumber, Oid, ProcNumber, RelFileNumber, INVALID_PROC_NUMBER, MAIN_FORKNUM, MAX_FORKNUM,
};
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use ::types_storage::file::{
    FORKNAMECHARS, OIDCHARS, PG_TBLSPC_DIR, TABLESPACE_VERSION_DIRECTORY,
};
use ::types_storage::RelFileLocator;

/// `PROCNUMBER_CHARS` (`common/relpath.h`) — max chars to allow for a
/// `procNumber` in a relation path. `MAX_BACKENDS` is `2^18 - 1`, so the widest
/// decimal is `262143` (6 chars).
pub const PROCNUMBER_CHARS: usize = 6;

/// `REL_PATH_STR_MAXLEN` (`common/relpath.h`) — the longest possible relation
/// path length, **not** counting the trailing NUL.
///
/// The longest path has the form
/// `sprintf(rp.str, "%s/%u/%s/%u/t%d_%u_%s", PG_TBLSPC_DIR, spcOid,
/// TABLESPACE_VERSION_DIRECTORY, dbOid, procNumber, relNumber,
/// forkNames[forkNumber])`.
pub const REL_PATH_STR_MAXLEN: usize = PG_TBLSPC_DIR.len()
    + 1 // '/'
    + OIDCHARS // spcOid
    + 1 // '/'
    + TABLESPACE_VERSION_DIRECTORY.len()
    + 1 // '/'
    + OIDCHARS // dbOid
    + 1 // '/'
    + 1 // 't' temporary-table indicator
    + PROCNUMBER_CHARS // procNumber
    + 1 // '_'
    + OIDCHARS // relNumber
    + 1 // '_'
    + FORKNAMECHARS; // forkNames[forkNumber]

/// Lookup table of fork name by fork number (`forkNames[]` in C), indexed by
/// the *non-negative* fork numbers `MAIN_FORKNUM=0 ..= MAX_FORKNUM`.
///
/// If you add a new entry, remember to update the errhint in
/// [`forkname_to_number`] below, and update the SGML documentation for
/// `pg_relation_size()`.
pub const forkNames: [&str; (MAX_FORKNUM as i32 + 1) as usize] = ["main", "fsm", "vm", "init"];

// Mirrors C's `StaticAssertDecl(lengthof(forkNames) == (MAX_FORKNUM + 1), ...)`.
const _: () = assert!(forkNames.len() == (MAX_FORKNUM as i32 + 1) as usize);

/// `forkNames[forkNumber]` — the fork name for one of the well-known forks.
/// Mirrors C's direct array index `forkNames[forkNumber]` (a bare `int` index in
/// C), so an out-of-range value is a programming error and panics like an
/// out-of-bounds C access would corrupt memory.
fn fork_name(fork_number: ForkNumber) -> &'static str {
    let idx = fork_number as i32;
    forkNames[idx as usize]
}

/// `forkname_to_number` — look up fork number by name.
///
/// In the backend, C throws `ERROR` on no match; the only well-defined results
/// are the four known forks, so we mirror the lookup and (matching the backend
/// `ereport(ERROR)` form via the error spine) return `Err` on a miss.
pub fn forkname_to_number(forkName: &str) -> PgResult<ForkNumber> {
    let mut forkNum: i32 = 0;
    while forkNum <= MAX_FORKNUM as i32 {
        if forkNames[forkNum as usize] == forkName {
            return Ok(ForkNumber::from_i32(forkNum).expect("forkNum in 0..=MAX_FORKNUM"));
        }
        forkNum += 1;
    }

    Err(PgError::error("invalid fork name")
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
        .with_hint("Valid fork names are \"main\", \"fsm\", \"vm\", and \"init\"."))
}

/// `forkname_chars` — figure out whether a filename could be a relation fork
/// (as opposed to an oddly named stray file).
///
/// If `str_` begins with a fork name other than the main fork name, returns its
/// length and sets `*fork` (if `Some`) to the fork number; otherwise returns `0`
/// and sets `*fork` to [`ForkNumber::InvalidForkNumber`].
///
/// Assumes (as the C code does) that no fork name is a prefix of another.
pub fn forkname_chars(str_: &str, fork: Option<&mut ForkNumber>) -> usize {
    let mut forkNum: i32 = 1;
    while forkNum <= MAX_FORKNUM as i32 {
        let name = forkNames[forkNum as usize];
        let len = name.len();
        if str_.as_bytes().starts_with(name.as_bytes()) {
            if let Some(fork) = fork {
                *fork = ForkNumber::from_i32(forkNum).expect("forkNum in 1..=MAX_FORKNUM");
            }
            return len;
        }
        forkNum += 1;
    }
    if let Some(fork) = fork {
        *fork = ForkNumber::InvalidForkNumber;
    }
    0
}

/// `GetDatabasePath` — construct the path to a database directory.
///
/// Result is a `palloc`'d string in C; here an owned [`String`] (the
/// allocation-free, no-memory-context form used by WAL-redo and invalidation
/// callers).
///
/// XXX this must agree with [`GetRelationPath`]!
pub fn GetDatabasePath(dbOid: Oid, spcOid: Oid) -> String {
    if spcOid == GLOBALTABLESPACE_OID {
        // Shared system relations live in {datadir}/global. (C asserts dbOid == 0.)
        String::from("global")
    } else if spcOid == DEFAULTTABLESPACE_OID {
        // The default tablespace is {datadir}/base.
        format!("base/{dbOid}")
    } else {
        // All other tablespaces are accessed via symlinks.
        format!("{PG_TBLSPC_DIR}/{spcOid}/{TABLESPACE_VERSION_DIRECTORY}/{dbOid}")
    }
}

/// `GetRelationPath` — construct the path to a relation's file.
///
/// C returns the path in-place as a fixed-buffer `RelPathStr` struct (so it is
/// usable in critical sections without allocating). The seam consumers in this
/// repo take the resulting string, so this port returns an owned [`String`]
/// directly; the [`REL_PATH_STR_MAXLEN`] bound is documented above and the
/// formatting cases mirror C exactly.
///
/// Note (matching C): ideally `procNumber` would be typed `ProcNumber`, but in C
/// it is just `int`; here it is [`ProcNumber`] (an `i32` alias), which is the
/// same.
pub fn GetRelationPath(
    dbOid: Oid,
    spcOid: Oid,
    relNumber: RelFileNumber,
    procNumber: ProcNumber,
    forkNumber: ForkNumber,
) -> String {
    let fork = fork_name(forkNumber);

    if spcOid == GLOBALTABLESPACE_OID {
        // Shared system relations live in {datadir}/global.
        // (C asserts dbOid == 0 and procNumber == INVALID_PROC_NUMBER.)
        if forkNumber != MAIN_FORKNUM {
            format!("global/{relNumber}_{fork}")
        } else {
            format!("global/{relNumber}")
        }
    } else if spcOid == DEFAULTTABLESPACE_OID {
        // The default tablespace is {datadir}/base.
        if procNumber == INVALID_PROC_NUMBER {
            if forkNumber != MAIN_FORKNUM {
                format!("base/{dbOid}/{relNumber}_{fork}")
            } else {
                format!("base/{dbOid}/{relNumber}")
            }
        } else if forkNumber != MAIN_FORKNUM {
            format!("base/{dbOid}/t{procNumber}_{relNumber}_{fork}")
        } else {
            format!("base/{dbOid}/t{procNumber}_{relNumber}")
        }
    } else {
        // All other tablespaces are accessed via symlinks.
        if procNumber == INVALID_PROC_NUMBER {
            if forkNumber != MAIN_FORKNUM {
                format!(
                    "{PG_TBLSPC_DIR}/{spcOid}/{TABLESPACE_VERSION_DIRECTORY}/{dbOid}/{relNumber}_{fork}"
                )
            } else {
                format!("{PG_TBLSPC_DIR}/{spcOid}/{TABLESPACE_VERSION_DIRECTORY}/{dbOid}/{relNumber}")
            }
        } else if forkNumber != MAIN_FORKNUM {
            format!(
                "{PG_TBLSPC_DIR}/{spcOid}/{TABLESPACE_VERSION_DIRECTORY}/{dbOid}/t{procNumber}_{relNumber}_{fork}"
            )
        } else {
            format!(
                "{PG_TBLSPC_DIR}/{spcOid}/{TABLESPACE_VERSION_DIRECTORY}/{dbOid}/t{procNumber}_{relNumber}"
            )
        }
    }
}

/// `relpathbackend(rlocator, backend, forknum)` — the `relpath.h` macro over
/// [`GetRelationPath`]; the on-disk path string of a relation fork.
pub fn relpathbackend(
    rlocator: RelFileLocator,
    backend: ProcNumber,
    forknum: ForkNumber,
) -> String {
    GetRelationPath(
        rlocator.dbOid,
        rlocator.spcOid,
        rlocator.relNumber,
        backend,
        forknum,
    )
}

// ----------------------------------------------------------------------------
// Seam installers.
// ----------------------------------------------------------------------------

/// Install every relpath seam owned by `common/relpath.c`.
pub fn init_seams() {
    // Value-shaped (owned String, no memory context) seams — `common-relpath-seams`.
    common_relpath_seams::relpathbackend::set(|rlocator, backend, forknum| {
        relpathbackend(rlocator, backend, forknum)
    });
    common_relpath_seams::get_database_path::set(|db_oid, spc_oid| {
        GetDatabasePath(db_oid, spc_oid)
    });

    // `Mcx`/`PgString`-shaped seams — `backend-common-relpath-seams`.
    backend_common_relpath_seams::get_database_path::set(
        |mcx: Mcx<'_>, db_oid: Oid, spc_oid: Oid| -> PgResult<PgString<'_>> {
            PgString::from_str_in(&GetDatabasePath(db_oid, spc_oid), mcx)
        },
    );
    backend_common_relpath_seams::relpath_backend::set(
        |mcx: Mcx<'_>,
         db_oid: Oid,
         spc_oid: Oid,
         rel_number: RelFileNumber,
         backend: ProcNumber,
         forknum: ForkNumber|
         -> PgResult<PgString<'_>> {
            PgString::from_str_in(
                &GetRelationPath(db_oid, spc_oid, rel_number, backend, forknum),
                mcx,
            )
        },
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::types_core::primitive::{FSM_FORKNUM, INIT_FORKNUM, VISIBILITYMAP_FORKNUM};

    #[test]
    fn rel_path_str_maxlen_matches_c() {
        assert_eq!(REL_PATH_STR_MAXLEN, 71);
    }

    #[test]
    fn get_database_path_matches_tablespace_cases() {
        assert_eq!(GetDatabasePath(0, GLOBALTABLESPACE_OID), "global");
        assert_eq!(GetDatabasePath(5, DEFAULTTABLESPACE_OID), "base/5");
        assert_eq!(GetDatabasePath(5, 999), "pg_tblspc/999/PG_18_202506291/5");
    }

    #[test]
    fn get_relation_path_global_cases() {
        assert_eq!(
            GetRelationPath(0, GLOBALTABLESPACE_OID, 1259, INVALID_PROC_NUMBER, MAIN_FORKNUM),
            "global/1259"
        );
        assert_eq!(
            GetRelationPath(0, GLOBALTABLESPACE_OID, 1259, INVALID_PROC_NUMBER, FSM_FORKNUM),
            "global/1259_fsm"
        );
    }

    #[test]
    fn get_relation_path_default_tablespace_cases() {
        assert_eq!(
            GetRelationPath(5, DEFAULTTABLESPACE_OID, 1259, INVALID_PROC_NUMBER, MAIN_FORKNUM),
            "base/5/1259"
        );
        assert_eq!(
            GetRelationPath(5, DEFAULTTABLESPACE_OID, 1259, INVALID_PROC_NUMBER, INIT_FORKNUM),
            "base/5/1259_init"
        );
        assert_eq!(
            GetRelationPath(5, DEFAULTTABLESPACE_OID, 1259, 7, MAIN_FORKNUM),
            "base/5/t7_1259"
        );
        assert_eq!(
            GetRelationPath(5, DEFAULTTABLESPACE_OID, 1259, 7, VISIBILITYMAP_FORKNUM),
            "base/5/t7_1259_vm"
        );
    }

    #[test]
    fn get_relation_path_custom_tablespace_cases() {
        assert_eq!(
            GetRelationPath(5, 999, 1259, INVALID_PROC_NUMBER, MAIN_FORKNUM),
            "pg_tblspc/999/PG_18_202506291/5/1259"
        );
        assert_eq!(
            GetRelationPath(5, 999, 1259, 7, INIT_FORKNUM),
            "pg_tblspc/999/PG_18_202506291/5/t7_1259_init"
        );
    }

    #[test]
    fn max_length_path_fits_maxlen() {
        let path = GetRelationPath(u32::MAX, u32::MAX, u32::MAX, 262_143, INIT_FORKNUM);
        assert_eq!(path.len(), REL_PATH_STR_MAXLEN);
        assert_eq!(
            path,
            "pg_tblspc/4294967295/PG_18_202506291/4294967295/t262143_4294967295_init"
        );
    }

    #[test]
    fn forkname_to_number_known_forks() {
        assert_eq!(forkname_to_number("main").unwrap(), MAIN_FORKNUM);
        assert_eq!(forkname_to_number("fsm").unwrap(), FSM_FORKNUM);
        assert_eq!(forkname_to_number("vm").unwrap(), VISIBILITYMAP_FORKNUM);
        assert_eq!(forkname_to_number("init").unwrap(), INIT_FORKNUM);
        assert!(forkname_to_number("bad").is_err());
    }

    #[test]
    fn forkname_chars_ignores_main_matches_prefixes() {
        let mut fork = MAIN_FORKNUM;
        assert_eq!(forkname_chars("main", Some(&mut fork)), 0);
        assert_eq!(fork, ForkNumber::InvalidForkNumber);
        assert_eq!(forkname_chars("fsm.1", Some(&mut fork)), 3);
        assert_eq!(fork, FSM_FORKNUM);
        assert_eq!(forkname_chars("vm", Some(&mut fork)), 2);
        assert_eq!(fork, VISIBILITYMAP_FORKNUM);
        assert_eq!(forkname_chars("initfork", Some(&mut fork)), 4);
        assert_eq!(fork, INIT_FORKNUM);
        assert_eq!(forkname_chars("stray", Some(&mut fork)), 0);
        assert_eq!(fork, ForkNumber::InvalidForkNumber);
        assert_eq!(forkname_chars("fsm", None), 3);
    }
}
