//! `pgrust_pin_database(text)` / `pgrust_unpin_database(text)` /
//! `pgrust_seal_template(text)` — pgrust-native internal builtins on the
//! reserved-oid EXTRA_BUILTINS path (the `pgrust_lane_coverage` precedent:
//! execmain/src/lanev2/coverage.rs documents the 9000..=9099 range; 9000 is
//! taken by the coverage SRF, this table claims 9001, 9002 and 9005 —
//! 9003/9004 were pgrust_janitor_unpause and pgrust_set_template_grace,
//! deleted 2026-08-05 and permanently retired, never reassigned).
//!
//! These three are TRUE builtins: bootstrap.rs backfills their pg_proc rows
//! (same oids, pg_catalog namespace) into every database a session reaches,
//! so no install script exists and clones inherit nothing they didn't
//! already have — the total janitor SQL surface is these three functions.
//!
//! Privileges (spec "Security posture"): pin/unpin = database owner or
//! superuser (`object_ownercheck`, which passes superusers); seal = owner
//! of the target database or superuser.

use datum::Datum;
use elog::ereport;
use types_core::catalog::DATABASE_RELATION_ID;
use types_core::Oid;
use types_error::{
    PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_UNDEFINED_DATABASE, ERROR,
};
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

use crate::registry;

/// Reserved pg_proc-style oids (see PGRUST_FOID_RANGE, 9000..=9099; the
/// range's reservation rationale lives on the coverage builtin). 9003 and
/// 9004 are RETIRED (module doc) — do not reassign them.
pub const PGRUST_PIN_DATABASE_FOID: Oid = 9001;
pub const PGRUST_UNPIN_DATABASE_FOID: Oid = 9002;
pub const PGRUST_SEAL_TEMPLATE_FOID: Oid = 9005;

/// Decode the text arg of a STRICT single-arg builtin into an owned name.
fn text_arg0(fcinfo: &mut Fcinfo) -> PgResult<String> {
    // SAFETY: null-checked by strictness; arg 0 is a text datum.
    let v = unsafe { fcinfo.arg_varlena_packed(0)? };
    let bytes = v.data().to_vec();
    String::from_utf8(bytes).map_err(|_| {
        Box::new(
            PgError::error("database name is not valid UTF-8".to_string())
                .with_sqlstate(ERRCODE_UNDEFINED_DATABASE),
        )
    })
}

/// Owner-or-superuser check against a live database (the alterdb.rs
/// precedent). Returns the database's CATALOG name (datname), which is what
/// pin state must key on: the name lookup's scan key truncates to
/// NAMEDATALEN-1 bytes (matching CREATE DATABASE's own truncation), so an
/// over-long argument can RESOLVE a database whose datname it does not
/// byte-equal — pinning the raw argument would return true yet never match
/// the datname the reap loop compares. ERRORs undefined_database on a miss,
/// insufficient_privilege on a failed check.
fn owner_or_superuser_check(fcinfo: &Fcinfo, name: &str, func: &'static str) -> PgResult<String> {
    let mcx = fcinfo.result_mcx();
    let Some(db) = pg_database::get_database_tuple_by_name(mcx, name)? else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_DATABASE)
            .errmsg(format!("database \"{name}\" does not exist"))
            .into_error()
            .into());
    };
    if !aclchk::object_ownercheck(DATABASE_RELATION_ID, db.oid, miscinit::GetUserId())? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "must be owner of database {name} or superuser to call {func}"
            ))
            .into_error()
            .into());
    }
    Ok(db.datname.as_str().to_owned())
}

/// pgrust_pin_database(text) -> bool: exempt the named database from
/// reaping for the rest of this postmaster lifetime (restart-lossy BY
/// DESIGN — rename out of the prefix for durable protection). Returns true
/// if newly pinned, false if it was already pinned.
pub fn fc_pgrust_pin_database(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let name = text_arg0(fcinfo)?;
    let datname = owner_or_superuser_check(fcinfo, &name, "pgrust_pin_database")?;
    Ok(Datum::from_bool(registry::pin(&datname)?))
}

/// pgrust_unpin_database(text) -> bool: drop the pin; the database becomes
/// reapable again after a fresh full grace period of idleness. Returns true
/// if a pin was removed. A pin whose database no longer exists (manual DROP
/// while pinned) has no owner to check: superusers may clear such stale
/// entries — otherwise a same-named future database would be born pinned.
pub fn fc_pgrust_unpin_database(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let name = text_arg0(fcinfo)?;
    let key = {
        let mcx = fcinfo.result_mcx();
        match pg_database::get_database_tuple_by_name(mcx, &name)? {
            Some(db) => {
                if !aclchk::object_ownercheck(DATABASE_RELATION_ID, db.oid, miscinit::GetUserId())?
                {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                        .errmsg(format!(
                            "must be owner of database {name} or superuser to call pgrust_unpin_database"
                        ))
                        .into_error()
                        .into());
                }
                // Unpin by the resolved catalog datname, mirroring pin
                // (owner_or_superuser_check's rationale).
                db.datname.as_str().to_owned()
            }
            None => {
                if !superuser_seams::superuser::call()? {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_UNDEFINED_DATABASE)
                        .errmsg(format!("database \"{name}\" does not exist"))
                        .into_error()
                        .into());
                }
                // Superuser clearing a stale pin (dropped-while-pinned):
                // pins are stored as catalog datnames, so the raw argument
                // compares exactly.
                name
            }
        }
    };
    Ok(Datum::from_bool(registry::unpin(&key)))
}

/// pgrust_seal_template(text) -> void: janitor-executed one-call sealing —
/// VACUUM (FREEZE, ANALYZE) inside the target through an internal session,
/// then IS_TEMPLATE true ALLOW_CONNECTIONS false, in the manual recipe's
/// exact order (seal.rs owns the choreography and the why-the-janitor
/// rationale). Callable from ANY database. Privilege: owner of the TARGET
/// database or superuser (the pgrust_set_template_grace style). The
/// backend-side already-a-template check gives callers the cheap ERROR;
/// the janitor re-validates under its own serialization (mutations of the
/// target's lifecycle all serialize in its loop).
pub fn fc_pgrust_seal_template(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let name = text_arg0(fcinfo)?;
    let datname = {
        let mcx = fcinfo.result_mcx();
        let Some(db) = pg_database::get_database_tuple_by_name(mcx, &name)? else {
            return Err(crate::seal::seal_target_missing_error(&name));
        };
        if !aclchk::object_ownercheck(DATABASE_RELATION_ID, db.oid, miscinit::GetUserId())? {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg(format!(
                    "must be owner of database {name} or superuser to call pgrust_seal_template"
                ))
                .into_error()
                .into());
        }
        if db.datistemplate {
            return Err(crate::seal::already_template_error(&name));
        }
        // Resolved catalog datname (the owner_or_superuser_check rationale:
        // the scan key truncates, the seal keys must not).
        db.datname.as_str().to_owned()
    };
    crate::seal::request_seal(&datname)?;
    // RETURNS void (the fc_pg_sleep convention).
    Ok(Datum::null())
}

/// The extra-builtin table seams_init appends to EXTRA_BUILTINS.
pub static JANITOR_BUILTINS: &[FmgrBuiltin] = &[
    FmgrBuiltin {
        foid: PGRUST_PIN_DATABASE_FOID,
        name: "pgrust_pin_database",
        nargs: 1,
        strict: true,
        retset: false,
        func: fc_pgrust_pin_database,
    },
    FmgrBuiltin {
        foid: PGRUST_UNPIN_DATABASE_FOID,
        name: "pgrust_unpin_database",
        nargs: 1,
        strict: true,
        retset: false,
        func: fc_pgrust_unpin_database,
    },
    FmgrBuiltin {
        foid: PGRUST_SEAL_TEMPLATE_FOID,
        name: "pgrust_seal_template",
        nargs: 1,
        strict: true,
        retset: false,
        func: fc_pgrust_seal_template,
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    /// Reserved-oid law (the coverage.rs test, replicated for this table):
    /// every janitor foid sits inside the documented pgrust range
    /// (9000..=9099), below the user oid space, is distinct within the
    /// table, avoids 9000 (pgrust_lane_coverage), and collides with no
    /// canonical C 18.3 builtin by oid or name. `install_extra_builtins`
    /// re-asserts the canonical half against live rows at startup, and the
    /// e2e probes the initdb'd pg_proc for the whole range.
    #[test]
    fn reserved_oids_are_clear_of_canonical() {
        let range = 9000u32..=9099;
        let mut seen = Vec::new();
        for b in JANITOR_BUILTINS {
            assert!(
                range.contains(&b.foid),
                "{} outside the pgrust reserved range",
                b.foid
            );
            assert!(
                b.foid < 16384,
                "user oid space starts at FirstNormalObjectId"
            );
            assert_ne!(b.foid, 9000, "9000 belongs to pgrust_lane_coverage");
            assert!(!seen.contains(&b.foid), "duplicate foid {}", b.foid);
            seen.push(b.foid);
        }
        for &(oid, name, ..) in ::fmgr_core::CANONICAL.iter() {
            assert!(!range.contains(&oid), "CANONICAL claims reserved oid {oid}");
            for b in JANITOR_BUILTINS {
                assert_ne!(name, b.name, "CANONICAL claims the name {name}");
            }
        }
    }
}
