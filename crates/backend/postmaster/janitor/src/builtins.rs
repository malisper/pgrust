//! `pgrust_pin_database(text)` / `pgrust_unpin_database(text)` /
//! `pgrust_janitor_unpause()` — pgrust-native internal builtins on the
//! reserved-oid EXTRA_BUILTINS path (the `pgrust_lane_coverage` precedent:
//! execmain/src/lanev2/coverage.rs documents the 9000..=9099 range; 9000 is
//! taken by the coverage SRF, this table claims 9001-9003).
//!
//! The catalog is NEVER touched by default:
//! scripts/testmode/janitor-functions.sql creates the functions on demand
//! (`LANGUAGE internal`), and the test-server recipe installs them into
//! templates so clones inherit them. Stock PostgreSQL errors identically on
//! the unknown internal names.
//!
//! Privileges (spec "Security posture"): pin/unpin = database owner or
//! superuser (`object_ownercheck`, which passes superusers); unpause =
//! superuser only.

use datum::Datum;
use elog::ereport;
use types_core::catalog::DATABASE_RELATION_ID;
use types_core::Oid;
use types_error::{
    PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_UNDEFINED_DATABASE, ERROR,
};
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

use crate::{marker, registry};

/// Reserved pg_proc-style oids (see PGRUST_FOID_RANGE, 9000..=9099; the
/// range's reservation rationale lives on the coverage builtin).
pub const PGRUST_PIN_DATABASE_FOID: Oid = 9001;
pub const PGRUST_UNPIN_DATABASE_FOID: Oid = 9002;
pub const PGRUST_JANITOR_UNPAUSE_FOID: Oid = 9003;
pub const PGRUST_SET_TEMPLATE_GRACE_FOID: Oid = 9004;

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

/// pgrust_janitor_unpause() -> bool: superuser-only acknowledgement of the
/// adoption guard. Durably writes the marker FIRST, then clears the pause
/// flag, requests the deferred startup sweep, and wakes the janitor (the
/// sweep itself runs in the janitor loop — all lifecycle mutations
/// serialize there). The check-write-flip sequence runs under the unpause
/// lock: concurrent superuser callers would otherwise race on the marker's
/// fixed temp path (registry::with_unpause_lock's rationale). Returns true
/// if the janitor was paused, false (no-op) otherwise.
pub fn fc_pgrust_janitor_unpause(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    if !superuser_seams::superuser::call()? {
        return Err(Box::new(
            PgError::error("must be superuser to call pgrust_janitor_unpause()")
                .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }
    registry::with_unpause_lock(|| {
        if !registry::is_paused() {
            return Ok(Datum::from_bool(false));
        }
        let prefix = crate::ephemeral_db_prefix();
        // Marker before unpause: if the durable write fails the guard stays
        // up.
        marker::write(&prefix)?;
        registry::set_paused(false);
        registry::request_sweep();
        registry::wake_janitor();
        let _ = elog::elog(
            types_error::LOG,
            format!(
                "pgrust ephemeral-db janitor: unpaused; prefix \"{prefix}\" acknowledged in \
                 \"{}\"; deferred startup sweep requested",
                marker::MARKER_FILE
            ),
        );
        Ok(Datum::from_bool(true))
    })
}

/// Ceiling for per-template grace overrides: the same bound as
/// pgrust.ephemeral_db_grace's GUC max (guc_tables). Without it a
/// NON-SUPERUSER template owner could set an ~68-year override
/// (i32 seconds) and make that template's clones effectively unreapable,
/// escaping the operator-facing knob's 0..=86400 range.
const MAX_TEMPLATE_GRACE_SECS: i32 = 86_400;

/// pgrust_set_template_grace(text, integer) -> bool (D2): set — or clear,
/// with a negative argument — the reap-grace override for clones of the
/// named template (databases matching `<prefix><template>__<token>`).
/// Seconds as a plain integer, DEVIATION from the spec's `interval` arg,
/// recorded in the M3 addendum: the grace GUC itself is integer seconds
/// (GUC_UNIT_S), and an int keeps the builtin free of interval-datum
/// plumbing. Bounded above by the grace GUC's own ceiling (86400s), so
/// the override can never exceed what the operator-facing knob allows.
/// Restart-lossy like pins. Privilege (spec security posture):
/// owner of the TEMPLATE database or superuser; the check resolves the
/// catalog datname and the override is keyed on it (the pin rationale —
/// NAMEDATALEN truncation). Returns true when an override is in place
/// after the call, false when it cleared.
pub fn fc_pgrust_set_template_grace(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let name = text_arg0(fcinfo)?;
    let secs = fcinfo.arg_i32(1);
    if secs > MAX_TEMPLATE_GRACE_SECS {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "grace override {secs} is out of range for pgrust_set_template_grace \
                 (maximum {MAX_TEMPLATE_GRACE_SECS} seconds, the pgrust.ephemeral_db_grace \
                 ceiling)"
            ))
            .into_error()
            .into());
    }
    let datname = owner_or_superuser_check(fcinfo, &name, "pgrust_set_template_grace")?;
    Ok(Datum::from_bool(registry::set_template_grace(
        &datname, secs,
    )?))
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
        foid: PGRUST_JANITOR_UNPAUSE_FOID,
        name: "pgrust_janitor_unpause",
        nargs: 0,
        strict: true,
        retset: false,
        func: fc_pgrust_janitor_unpause,
    },
    FmgrBuiltin {
        foid: PGRUST_SET_TEMPLATE_GRACE_FOID,
        name: "pgrust_set_template_grace",
        nargs: 2,
        strict: true,
        retset: false,
        func: fc_pgrust_set_template_grace,
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
