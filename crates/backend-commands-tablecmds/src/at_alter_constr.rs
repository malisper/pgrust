//! `ATExecAlterConstraint` family (commands/tablecmds.c:12198-12911): the
//! execution-phase handler for `ALTER TABLE ... ALTER CONSTRAINT`.
//!
//! Fully ported: the deferrability path (`ALTER CONSTRAINT ... [NOT]
//! DEFERRABLE / INITIALLY {DEFERRED,IMMEDIATE}`) for non-partitioned tables,
//! including the `pg_constraint` and `pg_trigger` catalog writes. The
//! enforceability path (which depends on the unported phase-3
//! `validateForeignKeyConstraint` final pass) and the inheritability path
//! (NOT NULL `connoinherit` child propagation) faithfully seam-and-panic, as
//! does recursion into partition children.

use mcx::{Mcx, PgVec};
use types_catalog::catalog_dependency::ObjectAddress;
use types_catalog::pg_constraint::{CONSTRAINT_FOREIGN, CONSTRAINT_NOTNULL};
use types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_error::{PgResult, ERRCODE_INVALID_TABLE_DEFINITION, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, ERROR};
use types_nodes::ddlnodes::ATAlterConstraint;
use types_rel::Relation;
use types_storage::lock::LOCKMODE;
use types_tuple::access::RELKIND_PARTITIONED_TABLE;

use backend_catalog_objectaddress::consts::ConstraintRelationId;
use backend_catalog_pg_constraint::{AlterConstrFlags, AlterConstrUpdateConstraintEntry};
use backend_utils_cache_lsyscache_seams as lsyscache_seams;
use backend_utils_error::ereport;

use crate::at_phase::AlteredTableInfo;

fn unported(what: &str) -> ! {
    panic!(
        "ALTER TABLE: {what} is not yet ported in backend-commands-tablecmds \
         (faithful seam-and-panic: the executed family is unported — see at_phase.rs)"
    );
}

/// `name_str` helper for `NameData` images (NUL-padded 64-byte buffers).
fn name_str(buf: &[u8; 64]) -> &str {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    std::str::from_utf8(&buf[..end]).unwrap_or("")
}

/// `ATExecAlterConstraint(wqueue, rel, cmdcon, recurse, lockmode)`
/// (tablecmds.c:12198).
pub fn ATExecAlterConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    _wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    rel: &Relation<'mcx>,
    cmdcon: &ATAlterConstraint<'mcx>,
    recurse: bool,
    lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    let conname = cmdcon
        .conname
        .as_ref()
        .map(|s| s.as_str())
        .unwrap_or("");

    // Disallow altering ONLY a partitioned table; it would make no sense.
    if rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE && !recurse {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
            .errmsg("constraint must be altered in child tables too".to_string())
            .errhint("Do not specify the ONLY keyword.".to_string())
            .into_error());
    }

    // Find and check the target constraint.
    let con = backend_catalog_pg_constraint::find_relation_constraint_by_name(mcx, rel.rd_id, conname)?;
    let con = match con {
        Some(c) => c,
        None => {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!(
                    "constraint \"{}\" of relation \"{}\" does not exist",
                    conname,
                    rel.name()
                ))
                .into_error());
        }
    };
    let currcon = con.form;

    if cmdcon.alterDeferrability && currcon.contype != CONSTRAINT_FOREIGN {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "constraint \"{}\" of relation \"{}\" is not a foreign key constraint",
                conname,
                rel.name()
            ))
            .into_error());
    }
    if cmdcon.alterEnforceability && currcon.contype != CONSTRAINT_FOREIGN {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "cannot alter enforceability of constraint \"{}\" of relation \"{}\"",
                conname,
                rel.name()
            ))
            .into_error());
    }
    if cmdcon.alterInheritability && currcon.contype != CONSTRAINT_NOTNULL {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "constraint \"{}\" of relation \"{}\" is not a not-null constraint",
                conname,
                rel.name()
            ))
            .into_error());
    }

    // Refuse to modify inheritability of inherited constraints.
    if cmdcon.alterInheritability && cmdcon.noinherit && currcon.coninhcount > 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "cannot alter inherited constraint \"{}\" on relation \"{}\"",
                name_str(&currcon.conname),
                rel.name()
            ))
            .into_error());
    }

    // If it's not the topmost constraint, raise an error: altering a non-topmost
    // constraint leaves some triggers untouched and pg_dump only dumps topmost
    // constraints.
    if OidIsValid(currcon.conparentid) {
        let mut parent = currcon.conparentid;
        let mut ancestorname: Option<String> = None;
        let mut ancestortable: Option<String> = None;

        // Loop to find the topmost constraint.
        loop {
            let tp = backend_utils_cache_syscache_seams::search_constraint_form_by_oid::call(parent)?;
            let Some(tp) = tp else { break };
            let contup = tp.form;
            if !OidIsValid(contup.conparentid) {
                ancestorname = Some(name_str(&contup.conname).to_string());
                ancestortable = lsyscache_seams::get_rel_name::call(mcx, contup.conrelid)?
                    .map(|s| s.to_string());
                break;
            }
            parent = contup.conparentid;
        }

        let mut b = ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "cannot alter constraint \"{}\" on relation \"{}\"",
                conname,
                rel.name()
            ));
        if let (Some(an), Some(at)) = (&ancestorname, &ancestortable) {
            b = b.errdetail(format!(
                "Constraint \"{conname}\" is derived from constraint \"{an}\" of relation \"{at}\"."
            ));
        }
        return Err(b
            .errhint("You may alter the constraint it derives from instead.".to_string())
            .into_error());
    }

    let mut address = ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    };

    // Do the actual catalog work, and recurse if necessary.
    if ATExecAlterConstraintInternal(mcx, cmdcon, rel, &con, recurse, lockmode)? {
        address = ObjectAddress {
            classId: ConstraintRelationId,
            objectId: currcon.oid,
            objectSubId: 0,
        };
    }

    Ok(address)
}

/// `ATExecAlterConstraintInternal(...)` (tablecmds.c:12341) — dispatch to the
/// enforceability / deferrability / inheritability subroutines.
fn ATExecAlterConstraintInternal<'mcx>(
    mcx: Mcx<'mcx>,
    cmdcon: &ATAlterConstraint<'mcx>,
    rel: &Relation<'mcx>,
    con: &types_catalog::pg_constraint::ConstraintFormCopy,
    recurse: bool,
    lockmode: LOCKMODE,
) -> PgResult<bool> {
    let mut changed = false;

    if cmdcon.alterEnforceability {
        if ATExecAlterConstrEnforceability(mcx, cmdcon, rel, con, lockmode)? {
            changed = true;
        }
    } else if cmdcon.alterDeferrability {
        let mut otherrelids: Vec<Oid> = Vec::new();
        if ATExecAlterConstrDeferrability(mcx, cmdcon, rel, con, recurse, &mut otherrelids, lockmode)?
        {
            // Invalidate relcache for other relations that have triggers which
            // are part of the constraint.
            for relid in otherrelids {
                backend_utils_cache_inval::cache_invalidate::CacheInvalidateRelcacheByRelid(relid)?;
            }
            changed = true;
        }
    }

    if cmdcon.alterInheritability
        && ATExecAlterConstrInheritability(mcx, cmdcon, rel, con, lockmode)?
    {
        changed = true;
    }

    Ok(changed)
}

/// `ATExecAlterConstrDeferrability(...)` (tablecmds.c:12558).
#[allow(clippy::too_many_arguments)]
fn ATExecAlterConstrDeferrability<'mcx>(
    mcx: Mcx<'mcx>,
    cmdcon: &ATAlterConstraint<'mcx>,
    rel: &Relation<'mcx>,
    con: &types_catalog::pg_constraint::ConstraintFormCopy,
    recurse: bool,
    otherrelids: &mut Vec<Oid>,
    _lockmode: LOCKMODE,
) -> PgResult<bool> {
    let currcon = con.form;
    let refrelid = currcon.confrelid;
    debug_assert_eq!(currcon.contype, CONSTRAINT_FOREIGN);

    let mut changed = false;

    // If already in the desired state, silently do nothing.
    if currcon.condeferrable != cmdcon.deferrable || currcon.condeferred != cmdcon.initdeferred {
        let flags = AlterConstrFlags {
            alter_enforceability: false,
            is_enforced: currcon.conenforced,
            alter_deferrability: true,
            deferrable: cmdcon.deferrable,
            initdeferred: cmdcon.initdeferred,
            alter_inheritability: false,
            noinherit: currcon.connoinherit,
        };
        let conrelid = AlterConstrUpdateConstraintEntry(mcx, currcon.oid, &flags)?;
        // The C invalidates the constraint relation's relcache inside
        // AlterConstrUpdateConstraintEntry; the relcache facet lives here.
        backend_utils_cache_inval::cache_invalidate::CacheInvalidateRelcacheByRelid(conrelid)?;
        changed = true;

        // Update the pg_trigger entries that implement the constraint.
        backend_commands_trigger::alter_constr::AlterConstrTriggerDeferrability(
            currcon.oid,
            rel.rd_id,
            cmdcon.deferrable,
            cmdcon.initdeferred,
            otherrelids,
        )?;
    }

    // If the table at either end of the constraint is partitioned, we need to
    // handle every constraint that is a child of this one.
    if recurse
        && changed
        && (rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE
            || lsyscache_seams::get_rel_relkind::call(refrelid)? == RELKIND_PARTITIONED_TABLE)
    {
        unported("ALTER CONSTRAINT DEFERRABILITY recursion into partition children (AlterConstrDeferrabilityRecurse)");
    }

    Ok(changed)
}

/// `ATExecAlterConstrEnforceability(...)` (tablecmds.c:12412). Faithfully
/// seam-and-panicked: the ENFORCED leg requires the phase-3
/// `validateForeignKeyConstraint` final pass (still unported, see
/// `ATRewriteTables`), and the NOT ENFORCED leg requires
/// `DropForeignKeyConstraintTriggers`.
fn ATExecAlterConstrEnforceability<'mcx>(
    _mcx: Mcx<'mcx>,
    _cmdcon: &ATAlterConstraint<'mcx>,
    _rel: &Relation<'mcx>,
    _con: &types_catalog::pg_constraint::ConstraintFormCopy,
    _lockmode: LOCKMODE,
) -> PgResult<bool> {
    unported("ALTER CONSTRAINT ENFORCED/NOT ENFORCED (ATExecAlterConstrEnforceability — needs validateForeignKeyConstraint phase-3 pass / DropForeignKeyConstraintTriggers)");
}

/// `ATExecAlterConstrInheritability(...)` (tablecmds.c:12617). Faithfully
/// seam-and-panicked: the NOT NULL `connoinherit` child propagation
/// (coninhcount decrement / ATExecSetNotNull on children) is a complete but
/// separate branch not exercised by ALTER CONSTRAINT on foreign keys.
fn ATExecAlterConstrInheritability<'mcx>(
    _mcx: Mcx<'mcx>,
    _cmdcon: &ATAlterConstraint<'mcx>,
    _rel: &Relation<'mcx>,
    _con: &types_catalog::pg_constraint::ConstraintFormCopy,
    _lockmode: LOCKMODE,
) -> PgResult<bool> {
    unported("ALTER CONSTRAINT [NO] INHERIT (ATExecAlterConstrInheritability — NOT NULL connoinherit child propagation)");
}
