//! `ATExecAlterConstraint` family (commands/tablecmds.c:12198-12911): the
//! execution-phase handler for `ALTER TABLE ... ALTER CONSTRAINT`.
//!
//! Fully ported: the deferrability path (`ALTER CONSTRAINT ... [NOT]
//! DEFERRABLE / INITIALLY {DEFERRED,IMMEDIATE}`) for non-partitioned tables,
//! including the `pg_constraint` and `pg_trigger` catalog writes; and the
//! enforceability ENFORCED leg (NOT ENFORCED → ENFORCED) for non-partitioned
//! tables: it updates `pg_constraint`, recreates the FK action / check triggers
//! (`createForeignKey{Action,Check}Triggers`), and queues the phase-3
//! `validateForeignKeyConstraint` recheck (driven by the FK final pass in
//! `ATRewriteTables`). The NOT ENFORCED leg's `DropForeignKeyConstraintTriggers`,
//! the inheritability path (NOT NULL `connoinherit` child propagation), and
//! recursion into partition children faithfully seam-and-panic.

use mcx::{Mcx, PgVec};
use types_catalog::catalog_dependency::ObjectAddress;
use types_catalog::pg_constraint::{CONSTRAINT_FOREIGN, CONSTRAINT_NOTNULL};
use types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_error::{PgResult, ERRCODE_INVALID_TABLE_DEFINITION, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, ERROR};
use types_nodes::ddlnodes::ATAlterConstraint;
use types_rel::Relation;
use types_storage::lock::{LOCKMODE, NoLock, RowExclusiveLock};
use types_tuple::access::{RELKIND_PARTITIONED_TABLE, RELKIND_RELATION};

use backend_access_common_heaptuple::heap_deform_tuple;
use backend_access_common_scankey::ScanKeyInit;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

use backend_access_common_relation::relation_open;
use backend_catalog_objectaddress::consts::ConstraintRelationId;
use backend_catalog_pg_constraint::{AlterConstrFlags, AlterConstrUpdateConstraintEntry};
use backend_utils_cache_lsyscache_seams as lsyscache_seams;
use backend_utils_error::ereport;

use crate::at_phase::AlteredTableInfo;

/// A `makeNode(Constraint)` skeleton (palloc0 baseline) carrying only the FK
/// fields the trigger-creation helpers read, used by the ENFORCED transition.
fn make_fk_constraint<'mcx>(
    mcx: Mcx<'mcx>,
    conname: &str,
    fk_matchtype: i8,
    fk_upd_action: i8,
    fk_del_action: i8,
) -> PgResult<types_nodes::ddlnodes::Constraint<'mcx>> {
    use types_nodes::ddlnodes::ConstrType;
    Ok(types_nodes::ddlnodes::Constraint {
        contype: ConstrType::CONSTR_FOREIGN,
        conname: Some(mcx::PgString::from_str_in(conname, mcx)?),
        deferrable: false,
        initdeferred: false,
        is_enforced: true,
        skip_validation: false,
        initially_valid: false,
        is_no_inherit: false,
        raw_expr: None,
        cooked_expr: None,
        generated_when: 0,
        generated_kind: 0,
        nulls_not_distinct: false,
        keys: PgVec::new_in(mcx),
        without_overlaps: false,
        including: PgVec::new_in(mcx),
        exclusions: PgVec::new_in(mcx),
        options: PgVec::new_in(mcx),
        indexname: None,
        indexspace: None,
        reset_default_tblspc: false,
        access_method: None,
        where_clause: None,
        pktable: None,
        fk_attrs: PgVec::new_in(mcx),
        pk_attrs: PgVec::new_in(mcx),
        fk_with_period: false,
        pk_with_period: false,
        fk_matchtype,
        fk_upd_action,
        fk_del_action,
        fk_del_set_cols: PgVec::new_in(mcx),
        old_conpfeqop: PgVec::new_in(mcx),
        old_pktable_oid: 0,
        location: -1,
    })
}

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
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
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
    if ATExecAlterConstraintInternal(mcx, wqueue, cmdcon, rel, &con, recurse, lockmode)? {
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
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    cmdcon: &ATAlterConstraint<'mcx>,
    rel: &Relation<'mcx>,
    con: &types_catalog::pg_constraint::ConstraintFormCopy,
    recurse: bool,
    lockmode: LOCKMODE,
) -> PgResult<bool> {
    let mut changed = false;

    if cmdcon.alterEnforceability {
        // Top-level call: the root FK / PK relids are the constraint's own
        // conrelid / confrelid; no parent triggers yet (InvalidOid).
        if ATExecAlterConstrEnforceability(
            mcx,
            wqueue,
            cmdcon,
            rel,
            con,
            con.form.conrelid,
            con.form.confrelid,
            lockmode,
            InvalidOid,
            InvalidOid,
            InvalidOid,
            InvalidOid,
        )? {
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

/// `ATExecAlterConstrEnforceability(...)` (tablecmds.c:12412) — apply a
/// `[NOT] ENFORCED` change to a foreign-key constraint.
///
/// The ENFORCED leg (NOT ENFORCED → ENFORCED) updates `pg_constraint`, recreates
/// the FK action / check triggers, and queues the phase-3
/// `validateForeignKeyConstraint` recheck against existing rows (driven by the FK
/// final pass in `ATRewriteTables`). The NOT ENFORCED leg's
/// `DropForeignKeyConstraintTriggers` and the partition-recursion
/// (`AlterConstrEnforceabilityRecurse`) are distinct unported functions and
/// faithfully seam-and-panic.
#[allow(clippy::too_many_arguments)]
fn ATExecAlterConstrEnforceability<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    cmdcon: &ATAlterConstraint<'mcx>,
    rel: &Relation<'mcx>,
    con: &types_catalog::pg_constraint::ConstraintFormCopy,
    fkrelid: Oid,
    pkrelid: Oid,
    lockmode: LOCKMODE,
    referenced_parent_del_trigger: Oid,
    referenced_parent_upd_trigger: Oid,
    referencing_parent_ins_trigger: Oid,
    referencing_parent_upd_trigger: Oid,
) -> PgResult<bool> {
    // Since this function recurses, it could be driven to stack overflow.
    backend_utils_misc_stack_depth::check_stack_depth()?;

    let currcon = con.form;
    let conoid = currcon.oid;
    debug_assert_eq!(currcon.contype, CONSTRAINT_FOREIGN);

    // fkrelid / pkrelid are the root FK / PK relids threaded down the recursion
    // (the top-level driver passes the constraint's own conrelid / confrelid).

    // rel = table_open(currcon->conrelid, lockmode). The caller already holds the
    // relation as `rel` (= conrelid for a top-level alter); reuse it.
    let crel = relation_open(mcx, currcon.conrelid, lockmode)?;
    let crel_relkind = crel.rd_rel.relkind;

    let result = (|| -> PgResult<bool> {
        let mut changed = false;

        if currcon.conenforced != cmdcon.is_enforced {
            let flags = AlterConstrFlags {
                alter_enforceability: true,
                is_enforced: cmdcon.is_enforced,
                alter_deferrability: false,
                deferrable: currcon.condeferrable,
                initdeferred: currcon.condeferred,
                alter_inheritability: false,
                noinherit: currcon.connoinherit,
            };
            let conrelid = AlterConstrUpdateConstraintEntry(mcx, conoid, &flags)?;
            backend_utils_cache_inval::cache_invalidate::CacheInvalidateRelcacheByRelid(conrelid)?;
            changed = true;
        }

        if !cmdcon.is_enforced {
            // Setting a constraint to NOT ENFORCED: its constraint triggers must
            // be dropped. The partition recursion + DropForeignKeyConstraintTriggers
            // are distinct unported functions.
            if crel_relkind == RELKIND_PARTITIONED_TABLE
                || lsyscache_seams::get_rel_relkind::call(currcon.confrelid)?
                    == RELKIND_PARTITIONED_TABLE
            {
                unported("ALTER CONSTRAINT NOT ENFORCED partition-child recursion (AlterConstrEnforceabilityRecurse)");
            }
            unported("ALTER CONSTRAINT NOT ENFORCED (DropForeignKeyConstraintTriggers)");
        } else if changed {
            // Create triggers. Prepare the minimal Constraint the trigger-creation
            // helpers read (conname / fk_matchtype / fk_upd_action / fk_del_action).
            // C: fkconstraint = makeNode(Constraint) (palloc0) with those four
            // fields filled in.
            let fkconstraint = make_fk_constraint(
                mcx,
                name_str(&currcon.conname),
                currcon.confmatchtype,
                currcon.confupdtype,
                currcon.confdeltype,
            )?;

            let mut referenced_del_trigger_oid = InvalidOid;
            let mut referenced_upd_trigger_oid = InvalidOid;
            let mut referencing_ins_trigger_oid = InvalidOid;
            let mut referencing_upd_trigger_oid = InvalidOid;

            // Create referenced (action) triggers when this row is the FK row of
            // the root pair.
            if currcon.conrelid == fkrelid {
                let (del, upd) = crate::at_fk::createForeignKeyActionTriggers(
                    mcx,
                    currcon.conrelid,
                    currcon.confrelid,
                    &fkconstraint,
                    conoid,
                    currcon.conindid,
                    referenced_parent_del_trigger,
                    referenced_parent_upd_trigger,
                )?;
                referenced_del_trigger_oid = del;
                referenced_upd_trigger_oid = upd;
            }

            // Create referencing (check) triggers when this row points at the
            // root PK.
            if currcon.confrelid == pkrelid {
                let (ins, upd) = crate::at_fk::createForeignKeyCheckTriggers(
                    mcx,
                    currcon.conrelid,
                    pkrelid,
                    &fkconstraint,
                    conoid,
                    currcon.conindid,
                    referencing_parent_ins_trigger,
                    referencing_parent_upd_trigger,
                )?;
                referencing_ins_trigger_oid = ins;
                referencing_upd_trigger_oid = upd;
            }

            // Tell Phase 3 to check that the constraint is satisfied by existing
            // rows. Only for plain tables whose FK row points at the root PK.
            if crel_relkind == RELKIND_RELATION && currcon.confrelid == pkrelid {
                let newcon = crate::at_phase::NewConstraint {
                    name: Some(mcx::PgString::from_str_in(
                        name_str(&currcon.conname),
                        mcx,
                    )?),
                    contype: types_nodes::ddlnodes::ConstrType::CONSTR_FOREIGN as i32,
                    refrelid: currcon.confrelid,
                    refindid: currcon.conindid,
                    conid: conoid,
                    qual: None,
                };
                let tab = crate::at_phase::ATGetQueueEntry(mcx, wqueue, &crel)?;
                wqueue[tab].constraints.push(newcon);
            }

            // If the table at either end of the constraint is partitioned, we
            // need to recurse and create triggers for each constraint that is a
            // child of this one, threading the just-created trigger OIDs as the
            // children's tgparentid.
            if crel_relkind == RELKIND_PARTITIONED_TABLE
                || lsyscache_seams::get_rel_relkind::call(currcon.confrelid)?
                    == RELKIND_PARTITIONED_TABLE
            {
                AlterConstrEnforceabilityRecurse(
                    mcx,
                    wqueue,
                    cmdcon,
                    conoid,
                    fkrelid,
                    pkrelid,
                    lockmode,
                    referenced_del_trigger_oid,
                    referenced_upd_trigger_oid,
                    referencing_ins_trigger_oid,
                    referencing_upd_trigger_oid,
                )?;
            }
        }

        Ok(changed)
    })();

    // table_close(rel, NoLock).
    crel.close(NoLock)?;
    result
}

/// `AlterConstrEnforceabilityRecurse(...)` (tablecmds.c:12763) — scan
/// `pg_constraint` for every row whose `conparentid` equals `conoid` (the
/// children of this constraint) and recursively apply the enforceability change,
/// preserving the root `fkrelid`/`pkrelid` and threading the parent trigger OIDs
/// so each child trigger gets the correct `tgparentid`.
#[allow(clippy::too_many_arguments)]
fn AlterConstrEnforceabilityRecurse<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    cmdcon: &ATAlterConstraint<'mcx>,
    conoid: Oid,
    fkrelid: Oid,
    pkrelid: Oid,
    lockmode: LOCKMODE,
    referenced_parent_del_trigger: Oid,
    referenced_parent_upd_trigger: Oid,
    referencing_parent_ins_trigger: Oid,
    referencing_parent_upd_trigger: Oid,
) -> PgResult<()> {
    let conrel = backend_access_table_table_seams::table_open::call(
        mcx,
        ConstraintRelationId,
        RowExclusiveLock,
    )?;

    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        types_catalog::pg_constraint::Anum_pg_constraint_conparentid,
        BTEqualStrategyNumber,
        types_core::fmgr::F_OIDEQ,
        types_tuple::backend_access_common_heaptuple::Datum::from_oid(conoid),
    )?;
    let keys = [key];

    let mut scan = backend_access_index_genam_seams::systable_beginscan::call(
        &conrel,
        types_catalog::pg_constraint::ConstraintParentIndexId,
        true,
        None,
        &keys,
    )?;

    // Collect the child constraint OIDs first; recursion below re-opens
    // pg_constraint (for its own trigger creation), so we don't hold the scan
    // across the recursive calls.
    let mut child_oids: Vec<Oid> = Vec::new();
    while let Some(tup) =
        backend_access_index_genam_seams::systable_getnext::call(mcx, scan.desc_mut())?
    {
        let cols = heap_deform_tuple(mcx, &tup.tuple, &conrel.rd_att, &tup.data)?;
        let child_oid =
            cols[types_catalog::pg_constraint::Anum_pg_constraint_oid as usize - 1].0.as_oid();
        child_oids.push(child_oid);
    }
    drop(scan);
    conrel.close(NoLock)?;

    for child_oid in child_oids {
        let childcon =
            backend_utils_cache_syscache_seams::search_constraint_form_by_oid::call(child_oid)?
                .ok_or_else(|| {
                    backend_utils_error::ereport(ERROR)
                        .errmsg_internal(format!(
                            "could not find tuple for constraint {child_oid}"
                        ))
                        .into_error()
                })?;
        let crel = relation_open(mcx, childcon.form.conrelid, lockmode)?;
        let res = ATExecAlterConstrEnforceability(
            mcx,
            wqueue,
            cmdcon,
            &crel,
            &childcon,
            fkrelid,
            pkrelid,
            lockmode,
            referenced_parent_del_trigger,
            referenced_parent_upd_trigger,
            referencing_parent_ins_trigger,
            referencing_parent_upd_trigger,
        );
        crel.close(NoLock)?;
        res?;
    }

    Ok(())
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
