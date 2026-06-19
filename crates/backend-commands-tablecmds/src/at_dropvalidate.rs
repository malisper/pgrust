//! `commands/tablecmds.c` — the ALTER TABLE DROP CONSTRAINT and VALIDATE
//! CONSTRAINT subcommand families.
//!
//! Ported faithfully from PostgreSQL 18.3:
//!   - `ATExecDropConstraint` (tablecmds.c:14012) + `dropconstraint_internal`
//!     (tablecmds.c:14075) — find the target `pg_constraint` row and drop it
//!     (and its dependent objects) via `performDeletion`.
//!   - `ATExecValidateConstraint` (tablecmds.c:12908) + the three
//!     `Queue{FK,Check,NN}ConstraintValidation` helpers — queue a phase-3
//!     verification scan for a NOT VALID constraint and flip its
//!     `convalidated` flag.
//!   - `validateForeignKeyConstraint` (tablecmds.c:13694) — validate an FK by a
//!     full table scan (single LEFT JOIN via `RI_Initial_Check`, else firing
//!     `RI_FKey_check_ins` per row).
//!
//! The partition / multi-level-inheritance recursion legs and the NOT NULL
//! attnotnull-reset guards on DROP are faithfully seam-and-panicked where their
//! supporting substrate (one-level child recursion of a `dropconstraint_internal`
//! over inherited CHECK/NOT NULL rows; the pg_attribute attnotnull clear) is not
//! reachable from this crate.

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

use mcx::{Mcx, PgVec};

use types_catalog::catalog_dependency::ObjectAddress;
use types_catalog::pg_constraint::{
    CONSTRAINT_CHECK, CONSTRAINT_FOREIGN, CONSTRAINT_NOTNULL,
};
use types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_error::{
    PgResult, ERRCODE_INVALID_TABLE_DEFINITION, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, ERROR, NOTICE,
};
use types_nodes::parsenodes::DropBehavior;
use types_rel::Relation;
use types_storage::lock::{AccessExclusiveLock, LOCKMODE, NoLock};

use backend_access_common_relation::relation_open;
use backend_catalog_objectaddress::consts::ConstraintRelationId;
use backend_catalog_pg_inherits::{find_all_inheritors, find_inheritance_children};
use backend_utils_misc_stack_depth::check_stack_depth;

use crate::at_phase::{
    AlteredTableInfo, ATGetQueueEntry, ATSimplePermissions, CheckAlterTableIsSafe, NewConstraint,
};
use crate::helpers::here;

const ATT_TABLE: i32 = 1 << 0;
const ATT_PARTITIONED_TABLE: i32 = 1 << 4;
const ATT_FOREIGN_TABLE: i32 = 1 << 6;

const RELKIND_RELATION: u8 = b'r';
const RELKIND_PARTITIONED_TABLE: u8 = b'p';

/// `NewConstraint.contype` is the `ConstrType` enum-as-i32; the FK value the
/// validate-queue uses.
const CONSTR_FOREIGN_I32: i32 = types_nodes::ddlnodes::ConstrType::CONSTR_FOREIGN as i32;

fn unported(what: &str) -> ! {
    panic!(
        "{what} is not yet ported in backend-commands-tablecmds (faithful seam-and-panic)"
    );
}

// ===========================================================================
// ATExecDropConstraint (tablecmds.c:14012)
// ===========================================================================

/// `ATExecDropConstraint(rel, constrName, behavior, recurse, missing_ok,
/// lockmode)` (tablecmds.c:14012) — find and drop the target constraint.
pub fn ATExecDropConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    constr_name: &str,
    behavior: DropBehavior,
    recurse: bool,
    missing_ok: bool,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    // conrel = table_open(ConstraintRelationId, RowExclusiveLock);
    // scan for (conrelid = rel, contypid = Invalid, conname = constrName).
    // There can be at most one matching row.
    let conoid = backend_catalog_pg_constraint::get_relation_constraint_oid(
        mcx,
        rel.rd_id,
        constr_name,
        true, // missing_ok at this level — we issue our own NOTICE/ERROR below
    )?;

    if !OidIsValid(conoid) {
        // not found
        if !missing_ok {
            return Err(backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!(
                    "constraint \"{constr_name}\" of relation \"{}\" does not exist",
                    rel.name()
                ))
                .into_error());
        } else {
            return backend_utils_error::ereport(NOTICE)
                .errmsg(format!(
                    "constraint \"{constr_name}\" of relation \"{}\" does not exist, skipping",
                    rel.name()
                ))
                .finish(here("ATExecDropConstraint"));
        }
    }

    dropconstraint_internal(mcx, rel, conoid, behavior, recurse, false, missing_ok, lockmode)?;

    Ok(())
}

// ===========================================================================
// dropconstraint_internal (tablecmds.c:14075)
// ===========================================================================

/// `dropconstraint_internal(rel, constraintTup, behavior, recurse, recursing,
/// missing_ok, lockmode)` (tablecmds.c:14075) — remove a constraint, given its
/// `pg_constraint` OID (we re-fetch the form by OID rather than carry the heap
/// tuple, since the driver lives above the deform substrate).
pub fn dropconstraint_internal<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    conoid: Oid,
    behavior: DropBehavior,
    _recurse: bool,
    recursing: bool,
    _missing_ok: bool,
    lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    // Guard against stack overflow due to overly deep inheritance tree.
    check_stack_depth()?;

    // At top level, permission check was done in ATPrepCmd, else do it.
    if recursing {
        ATSimplePermissions(
            types_nodes::ddlnodes::AlterTableType::AT_DropConstraint,
            rel,
            ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
        )?;
    }

    // con = GETSTRUCT(constraintTup);  constrName = NameStr(con->conname);
    let con = backend_utils_cache_syscache_seams::search_constraint_form_by_oid::call(conoid)?
        .ok_or_else(|| {
            backend_utils_error::ereport(ERROR)
                .errmsg_internal(format!("cache lookup failed for constraint {conoid}"))
                .into_error()
        })?;
    let conform = con.form;

    // Don't allow drop of inherited constraints.
    if conform.coninhcount > 0 && !recursing {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
            .errmsg(format!(
                "cannot drop inherited constraint \"{constrname}\" of relation \"{}\"",
                rel.name(),
                constrname = conform.conname_str()
            ))
            .into_error());
    }

    // Reset pg_constraint.attnotnull, if this is a not-null constraint, plus the
    // PK / replica-identity / identity-column guards. That sub-path needs the
    // pg_attribute attnotnull-clear substrate, which is not wired into this
    // crate; faithfully seam-and-panic.
    if conform.contype == CONSTRAINT_NOTNULL {
        unported(
            "DROP CONSTRAINT for a NOT NULL constraint (dropconstraint_internal attnotnull reset)",
        );
    }

    let is_no_inherit_constraint = conform.connoinherit;

    // If it's a foreign-key constraint, we'd better lock the referenced table
    // and check that that's not in use (unless self-referential).
    if conform.contype == CONSTRAINT_FOREIGN && conform.confrelid != rel.rd_id {
        // Must match lock taken by RemoveTriggerById:
        let frel = relation_open(mcx, conform.confrelid, AccessExclusiveLock)?;
        CheckAlterTableIsSafe(&frel)?;
        frel.close(NoLock)?;
    }

    // Perform the actual constraint deletion.
    let conobj = ObjectAddress {
        classId: ConstraintRelationId,
        objectId: conform.oid,
        objectSubId: 0,
    };
    backend_catalog_dependency_seams::perform_deletion::call(
        conobj.classId,
        conobj.objectId,
        conobj.objectSubId,
        behavior,
        0,
    )?;

    // For partitioned tables, non-CHECK, non-NOT-NULL inherited constraints are
    // dropped via the dependency mechanism, so we're done here.
    if conform.contype != CONSTRAINT_CHECK
        && conform.contype != CONSTRAINT_NOTNULL
        && rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE
    {
        return Ok(conobj);
    }

    // Propagate to children as appropriate. CHECK is the only type (besides
    // NOT NULL, handled above) that can be inherited here. For a leaf table
    // (no children) this loop is empty; the multi-level inheritance recursion
    // over inherited CHECK rows needs a by-name/by-OID child constraint lookup
    // that is not reachable from this crate, so we faithfully seam-and-panic
    // when children exist.
    let children = if !is_no_inherit_constraint {
        find_inheritance_children(mcx, rel.rd_id, lockmode)?
    } else {
        PgVec::new_in(mcx)
    };

    if !children.is_empty() {
        unported("DROP CONSTRAINT recursion to inheritance children (dropconstraint_internal child loop)");
    }

    Ok(conobj)
}

// ===========================================================================
// ATExecValidateConstraint (tablecmds.c:12908)
// ===========================================================================

/// `ATExecValidateConstraint(wqueue, rel, constrName, recurse, recursing,
/// lockmode)` (tablecmds.c:12908) — find the target constraint and, if not yet
/// validated, queue a phase-3 verification scan and flip `convalidated`.
pub fn ATExecValidateConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    rel: &Relation<'mcx>,
    constr_name: &str,
    _recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    // Find and check the target constraint.
    let conoid = backend_catalog_pg_constraint::get_relation_constraint_oid(
        mcx, rel.rd_id, constr_name, false,
    )?;
    // get_relation_constraint_oid raises "constraint ... does not exist" itself
    // when missing_ok=false; here it always returns a valid OID.

    let con = backend_utils_cache_syscache_seams::search_constraint_form_by_oid::call(conoid)?
        .ok_or_else(|| {
            backend_utils_error::ereport(ERROR)
                .errmsg_internal(format!("cache lookup failed for constraint {conoid}"))
                .into_error()
        })?;
    let conform = con.form;

    if conform.contype != CONSTRAINT_FOREIGN
        && conform.contype != CONSTRAINT_CHECK
        && conform.contype != CONSTRAINT_NOTNULL
    {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "cannot validate constraint \"{constr_name}\" of relation \"{}\"",
                rel.name()
            ))
            .errdetail("This operation is not supported for this type of constraint.".to_string())
            .into_error());
    }

    if !conform.conenforced {
        return Err(backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("cannot validate NOT ENFORCED constraint".to_string())
            .into_error());
    }

    let mut address = ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    };

    if !conform.convalidated {
        if conform.contype == CONSTRAINT_FOREIGN {
            QueueFKConstraintValidation(
                mcx, wqueue, rel, conform.confrelid, conoid, &conform, constr_name, lockmode,
            )?;
        } else if conform.contype == CONSTRAINT_CHECK {
            QueueCheckConstraintValidation(mcx, wqueue, constr_name, conoid, recursing, lockmode)?;
        } else {
            // CONSTRAINT_NOTNULL
            QueueNNConstraintValidation(mcx, wqueue, rel, conoid, recursing, lockmode)?;
        }

        address = ObjectAddress {
            classId: ConstraintRelationId,
            objectId: conform.oid,
            objectSubId: 0,
        };
    }
    // else: already validated => InvalidObjectAddress.

    Ok(address)
}

// ===========================================================================
// QueueFKConstraintValidation (tablecmds.c:12990)
// ===========================================================================

/// `QueueFKConstraintValidation(...)` — add an FK validation entry to phase 3
/// and flip `convalidated`. The partition-recursion leg (when either end is
/// partitioned) faithfully seam-and-panics.
fn QueueFKConstraintValidation<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    fkrel: &Relation<'mcx>,
    pkrelid: Oid,
    conoid: Oid,
    conform: &types_catalog::pg_constraint::FormData_pg_constraint,
    constr_name: &str,
    _lockmode: LOCKMODE,
) -> PgResult<()> {
    debug_assert_eq!(conform.contype, CONSTRAINT_FOREIGN);
    debug_assert!(!conform.convalidated);

    // Add the validation to phase 3's queue; only for plain tables whose FK row
    // points directly at the referenced (root) table.
    if fkrel.rd_rel.relkind == RELKIND_RELATION && conform.confrelid == pkrelid {
        let newcon = NewConstraint {
            name: Some(mcx::PgString::from_str_in(constr_name, mcx)?),
            contype: CONSTR_FOREIGN_I32,
            refrelid: conform.confrelid,
            refindid: conform.conindid,
            conid: conoid,
            qual: None,
        };
        let tab = ATGetQueueEntry(mcx, wqueue, fkrel)?;
        wqueue[tab].constraints.push(newcon);
    }

    // If either end is partitioned, recurse over child constraints.
    if fkrel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE
        || backend_utils_cache_lsyscache_seams::get_rel_relkind::call(conform.confrelid)?
            == RELKIND_PARTITIONED_TABLE
    {
        unported("VALIDATE CONSTRAINT FK recursion over partition child constraints");
    }

    // Mark the pg_constraint row as validated.
    backend_catalog_pg_constraint::set_constraint_validated(mcx, conoid)?;

    Ok(())
}

// ===========================================================================
// QueueCheckConstraintValidation (tablecmds.c:13110)
// ===========================================================================

/// `QueueCheckConstraintValidation(...)` — queue a CHECK validation entry and
/// flip `convalidated`. The cooked-expr (`conbin`) extraction and the
/// inheritance-child recursion legs are faithfully seam-and-panicked.
fn QueueCheckConstraintValidation<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    constr_name: &str,
    conoid: Oid,
    _recursing: bool,
    _lockmode: LOCKMODE,
) -> PgResult<()> {
    // The phase-3 CHECK revalidation requires the cooked-expr (conbin) of the
    // constraint to be deparsed and evaluated over the existing rows in
    // ATRewriteTable; that expression-eval phase-3 path is not yet ported (see
    // ATRewriteTables). Faithfully seam-and-panic rather than silently mark the
    // constraint valid without scanning.
    let _ = (mcx, wqueue, constr_name, conoid);
    unported("VALIDATE CONSTRAINT for a CHECK constraint (phase-3 CHECK revalidation scan)");
}

// ===========================================================================
// QueueNNConstraintValidation (tablecmds.c:13216)
// ===========================================================================

/// `QueueNNConstraintValidation(...)` — queue a NOT NULL validation scan and
/// flip `convalidated`. The inheritance-child recursion leg faithfully
/// seam-and-panics; the single-table leg reuses the phase-3 NOT NULL verify
/// scan (`at_verify_not_null`) by setting `verify_new_notnull`.
fn QueueNNConstraintValidation<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    rel: &Relation<'mcx>,
    conoid: Oid,
    recursing: bool,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    // If we're recursing, the parent already handled children; also, a NO
    // INHERIT constraint isn't looked for in children.
    let con = backend_utils_cache_syscache_seams::search_constraint_form_by_oid::call(conoid)?
        .ok_or_else(|| {
            backend_utils_error::ereport(ERROR)
                .errmsg_internal(format!("cache lookup failed for constraint {conoid}"))
                .into_error()
        })?;

    if !recursing && !con.form.connoinherit {
        let (children, _) = find_all_inheritors(mcx, rel.rd_id, lockmode, false)?;
        // children always contains the parent rel itself; >1 means real children.
        if children.len() > 1 {
            unported("VALIDATE CONSTRAINT NOT NULL recursion to inheritance children");
        }
    }

    // Queue validation for phase 3: a full-table NOT NULL recheck.
    let tab = ATGetQueueEntry(mcx, wqueue, rel)?;
    wqueue[tab].verify_new_notnull = true;

    // Mark the pg_constraint row as validated.
    backend_catalog_pg_constraint::set_constraint_validated(mcx, conoid)?;

    Ok(())
}
