//! `commands/tablecmds.c` — the ALTER TABLE ADD PRIMARY KEY / ADD CONSTRAINT /
//! ADD INDEX subcommand families.
//!
//! Ported faithfully from PostgreSQL 18.3:
//!   - `ATPrepAddPrimaryKey` (tablecmds.c:9499) — coerce the PK columns to NOT
//!     NULL (queuing an `AT_AddConstraint` NOT NULL for each column that lacks a
//!     suitable not-null constraint).
//!   - `ATExecAddIndex` (tablecmds.c:9620) — build the (unique) index for a
//!     transformed PRIMARY KEY / UNIQUE / EXCLUDE constraint via `DefineIndex`.
//!   - `ATExecAddConstraint` (tablecmds.c:9799) + `ATAddCheckNNConstraint`
//!     (tablecmds.c:9911) — add a CHECK or NOT NULL constraint (recursing to
//!     children for NOT NULL), via `AddRelationNewConstraints`.
//!   - `verifyNotNullPKCompatible` (tablecmds.c:9576) lives in
//!     `backend-catalog-pg-constraint` (it needs the `Form_pg_constraint`
//!     deform substrate owned there).
//!   - The phase-3 NOT NULL verification scan ([`at_verify_not_null`]) — the
//!     `newrel == NULL`, NOT-NULL-only path of `ATRewriteTable`
//!     (tablecmds.c:6126), invoked from [`crate::at_phase::ATRewriteTables`].
//!
//!   - `ATExecAddIndexConstraint` (tablecmds.c:9704) — ADD CONSTRAINT ... USING
//!     INDEX: promote an existing unique index into a PRIMARY KEY / UNIQUE
//!     constraint (`BuildIndexInfo` + `index_check_primary_key` +
//!     `index_constraint_create`, optionally renaming the index to the
//!     constraint name).

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

use ::mcx::{Mcx, PgString, PgVec};

use ::types_catalog::catalog_dependency::ObjectAddress;
use ::types_core::primitive::{AttrNumber, InvalidOid, Oid, OidIsValid};
use ::types_error::{PgResult, ERRCODE_INVALID_TABLE_DEFINITION, ERROR};
use ::nodes::ddlnodes::{
    AlterTableCmd, AlterTableType, Constraint, ConstrType, IndexStmt,
};
use ::nodes::nodes::{Node, NodePtr};
use ::rel::Relation;
use ::types_storage::lock::{AccessShareLock, LOCKMODE, NoLock};

use ::common_relation::relation_open;
use ::transam_xact::CommandCounterIncrement;
use ::objectaddress::consts::ConstraintRelationId;
use ::pg_inherits::{find_all_inheritors, find_inheritance_children};
use ::stack_depth::check_stack_depth;

use crate::at_phase::{
    AlteredTableInfo, AlterTableUtilityContext, NewConstraint, ATGetQueueEntry, ATSimplePermissions,
    CheckAlterTableIsSafe, AT_PASS_ADD_INDEX, ATT_FOREIGN_TABLE, ATT_PARTITIONED_TABLE, ATT_TABLE,
};
use crate::helpers::here;

/// `index_constraint_create` flag bits (catalog/index.h). Mirrored locally
/// (the owning crate keeps them private).
const INDEX_CONSTR_CREATE_MARK_AS_PRIMARY: u16 = 1 << 0;
const INDEX_CONSTR_CREATE_DEFERRABLE: u16 = 1 << 1;
const INDEX_CONSTR_CREATE_INIT_DEFERRED: u16 = 1 << 2;
const INDEX_CONSTR_CREATE_UPDATE_INDEX: u16 = 1 << 3;
const INDEX_CONSTR_CREATE_REMOVE_OLD_DEPS: u16 = 1 << 4;

/// Deep-copy an `Option<PgString>` into `mcx`.
fn opt_str_clone<'mcx>(
    mcx: Mcx<'mcx>,
    s: &Option<PgString<'mcx>>,
) -> PgResult<Option<PgString<'mcx>>> {
    match s {
        Some(v) => Ok(Some(v.clone_in(mcx)?)),
        None => Ok(None),
    }
}

fn unported(what: &str) -> ! {
    panic!(
        "{what} is not yet ported in backend-commands-tablecmds (faithful seam-and-panic)"
    );
}

// ===========================================================================
// ATPrepAddPrimaryKey (tablecmds.c:9499)
// ===========================================================================

/// `ATPrepAddPrimaryKey(wqueue, rel, cmd, recurse, lockmode, context)`
/// (tablecmds.c:9499) — for an ADD CONSTRAINT whose constraint is a PRIMARY
/// KEY, verify each key column has a suitable (validated, inheritable) not-null
/// constraint, or queue an `AT_AddConstraint` NOT NULL to create one.
pub fn ATPrepAddPrimaryKey<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
    recurse: bool,
    lockmode: LOCKMODE,
    context: &AlterTableUtilityContext<'_>,
) -> PgResult<()> {
    // pkconstr = castNode(Constraint, cmd->def);
    let pkconstr = match cmd.def.as_deref() {
        Some(n) => n.expect_constraint(),
        None => return Ok(()),
    };
    if pkconstr.contype != ConstrType::CONSTR_PRIMARY {
        return Ok(());
    }

    let relid = rel.rd_id;

    let mut children: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    let mut got_children = false;

    // foreach_node(String, column, pkconstr->keys)
    for column_node in pkconstr.keys.iter() {
        let colname = column_node.expect_string().sval.as_str();

        // First check if a suitable constraint exists.
        let tuple =
            pg_constraint::findNotNullConstraint(mcx, relid, colname)?;
        if let Some(tuple) = tuple {
            pg_constraint::verifyNotNullPKCompatible(mcx, &tuple, colname)?;
            // All good with this one; don't request another.
            continue;
        } else if !recurse {
            // No constraint on this column.  Asked not to recurse, we won't
            // create one here, but verify that all children have one.
            if !got_children {
                children = find_inheritance_children(mcx, relid, lockmode)?;
                got_children = true;
            }

            for &childrelid in children.iter() {
                let tup = pg_constraint::findNotNullConstraint(
                    mcx, childrelid, colname,
                )?;
                match tup {
                    None => {
                        let childname = lsyscache_seams::get_rel_name::call(
                            mcx, childrelid,
                        )?
                        .map(|s| s.as_str().to_string())
                        .unwrap_or_default();
                        return utils_error::ereport(ERROR)
                            .errmsg(format!(
                                "column \"{colname}\" of table \"{childname}\" is not marked NOT NULL"
                            ))
                            .finish(here("ATPrepAddPrimaryKey"));
                    }
                    Some(tup) => {
                        pg_constraint::verifyNotNullPKCompatible(
                            mcx, &tup, colname,
                        )?;
                    }
                }
            }
        }

        // This column is not already not-null, so add it to the queue.
        // nnconstr = makeNotNullConstraint(column);
        let nnconstr = make_not_null_constraint(mcx, colname)?;
        let nndef = ::mcx::alloc_in(mcx, Node::mk_constraint(mcx, nnconstr)?)?;

        // newcmd = makeNode(AlterTableCmd);
        // newcmd->subtype = AT_AddConstraint;
        // newcmd->recurse = true;  /* force recurse=true; see above */
        // newcmd->def = (Node *) nnconstr;
        let newcmd = AlterTableCmd {
            subtype: AlterTableType::AT_AddConstraint,
            name: None,
            num: 0,
            newowner: None,
            def: Some(nndef),
            behavior: cmd.behavior,
            missing_ok: false,
            recurse: true,
        };

        // ATPrepCmd(wqueue, rel, newcmd, true, false, lockmode, context);
        crate::at_phase::ATPrepCmd(mcx, wqueue, rel, &newcmd, true, false, lockmode, context)?;
    }

    Ok(())
}

/// `makeNotNullConstraint(makeString(colname))` (nodes/makefuncs.c:493) — build
/// a fresh CONSTR_NOTNULL constraint over a single column key.
fn make_not_null_constraint<'mcx>(mcx: Mcx<'mcx>, colname: &str) -> PgResult<Constraint<'mcx>> {
    let mut keys: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);
    let sval = PgString::from_str_in(colname, mcx)?;
    keys.push(::mcx::alloc_in(
        mcx,
        Node::mk_string(mcx, ::nodes::value::StringNode { sval })?,
    )?);
    Ok(Constraint {
        contype: ConstrType::CONSTR_NOTNULL,
        conname: None,
        deferrable: false,
        initdeferred: false,
        is_enforced: true,
        skip_validation: false,
        initially_valid: true,
        is_no_inherit: false,
        raw_expr: None,
        cooked_expr: None,
        generated_when: 0,
        generated_kind: 0,
        nulls_not_distinct: false,
        keys,
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
        fk_matchtype: 0,
        fk_upd_action: 0,
        fk_del_action: 0,
        fk_del_set_cols: PgVec::new_in(mcx),
        old_conpfeqop: PgVec::new_in(mcx),
        old_pktable_oid: InvalidOid,
        location: -1,
    })
}

// ===========================================================================
// ATExecSetNotNull (tablecmds.c:7457)
// ===========================================================================

const RELKIND_PARTITIONED_TABLE: u8 = b'p';

/// `pg_add_s16_overflow(a, b, *result)` (`common/int.h`) — returns true on
/// overflow.
fn pg_add_s16_overflow(a: i16, b: i16, result: &mut i16) -> bool {
    match a.checked_add(b) {
        Some(v) => {
            *result = v;
            false
        }
        None => true,
    }
}

/// `ATExecSetNotNull(wqueue, rel, conName, colName, recurse, recursing,
/// lockmode)` (tablecmds.c) — ALTER COLUMN SET NOT NULL. Add a not-null
/// constraint to a single table and its children, marking
/// `pg_attribute.attnotnull` and queuing the phase-3 existing-rows verification.
/// Returns the address of the constraint added to the parent relation, if one
/// gets added, or `InvalidObjectAddress` otherwise.
///
/// Recurses to child tables during execution (not via ALTER TABLE's prep-time
/// recursion).
pub fn ATExecSetNotNull<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    rel: &Relation<'mcx>,
    con_name: Option<&str>,
    col_name: &str,
    recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    // Guard against stack overflow due to overly deep inheritance tree.
    check_stack_depth()?;

    // At top level, permission check was done in ATPrepCmd, else do it.
    if recursing {
        ATSimplePermissions(
            AlterTableType::AT_AddConstraint,
            rel,
            ATT_PARTITIONED_TABLE | ATT_TABLE | ATT_FOREIGN_TABLE,
        )?;
        debug_assert!(con_name.is_some());
    }

    // attnum = get_attnum(RelationGetRelid(rel), colName);
    let attnum: AttrNumber =
        lsyscache_seams::get_attnum::call(rel.rd_id, col_name)?;
    if attnum == 0 {
        return Err(utils_error::ereport(ERROR)
            .errcode(::types_error::ERRCODE_UNDEFINED_COLUMN)
            .errmsg(format!(
                "column \"{col_name}\" of relation \"{}\" does not exist",
                rel.name()
            ))
            .into_error());
    }

    // Prevent them from altering a system attribute.
    if attnum <= 0 {
        return Err(utils_error::ereport(ERROR)
            .errcode(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("cannot alter system column \"{col_name}\""))
            .into_error());
    }

    // See if there's already a constraint.
    let tuple =
        pg_constraint::findNotNullConstraintAttnum(mcx, rel.rd_id, attnum)?;
    if let Some(tuple) = tuple {
        let mut con_form =
            syscache_seams::read_constraint_form::call(&tuple)?;
        let mut changed = false;

        // Don't let a NO INHERIT constraint be changed into inherit.
        if con_form.connoinherit && recurse {
            return Err(utils_error::ereport(ERROR)
                .errcode(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!(
                    "cannot change NO INHERIT status of NOT NULL constraint \"{}\" on relation \"{}\"",
                    con_form.conname_str(),
                    rel.name()
                ))
                .into_error());
        }

        // If we find an appropriate constraint: if recursing, increment
        // coninhcount; if not, set conislocal if not already set; otherwise if
        // it isn't validated yet, validate it.
        if recursing {
            let mut newcount = con_form.coninhcount;
            if pg_add_s16_overflow(con_form.coninhcount, 1, &mut newcount) {
                return Err(utils_error::ereport(ERROR)
                    .errcode(::types_error::ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                    .errmsg("too many inheritance parents".to_string())
                    .into_error());
            }
            con_form.coninhcount = newcount;
            changed = true;
        } else if !con_form.conislocal {
            con_form.conislocal = true;
            changed = true;
        } else if !con_form.convalidated {
            // Flip attnotnull and convalidated, and also validate the
            // constraint.
            let conname = con_form.conname_str().to_string();
            return crate::at_dropvalidate::ATExecValidateConstraint(
                mcx, wqueue, rel, &conname, recurse, recursing, lockmode,
            );
        }

        if changed {
            // constr_rel = table_open(ConstraintRelationId, RowExclusiveLock);
            let constr_rel = relation_open(
                mcx,
                ConstraintRelationId,
                ::types_storage::lock::RowExclusiveLock,
            )?;
            let fields = ::types_catalog::pg_constraint::ConstraintFieldUpdate {
                conname: con_form.conname,
                connamespace: con_form.connamespace,
                conislocal: con_form.conislocal,
                coninhcount: con_form.coninhcount,
                conparentid: con_form.conparentid,
                convalidated: con_form.convalidated,
                connoinherit: con_form.connoinherit,
                conenforced: con_form.conenforced,
                condeferrable: con_form.condeferrable,
                condeferred: con_form.condeferred,
                conindid: con_form.conindid,
            };
            indexing_seams::catalog_tuple_update_pg_constraint::call(
                &constr_rel,
                tuple.tuple.t_self,
                &fields,
            )?;
            let address = ObjectAddress {
                classId: ConstraintRelationId,
                objectId: con_form.oid,
                objectSubId: 0,
            };
            drop(constr_rel);
            return Ok(address);
        } else {
            return Ok(ObjectAddress {
                classId: InvalidOid,
                objectId: InvalidOid,
                objectSubId: 0,
            });
        }
    }

    // If we're asked not to recurse, and children exist, raise an error for
    // partitioned tables.  For inheritance, we act as if NO INHERIT had been
    // specified.
    let mut is_no_inherit = false;
    if !recurse {
        let children = find_inheritance_children(mcx, rel.rd_id, NoLock)?;
        if !children.is_empty() {
            if rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
                return Err(utils_error::ereport(ERROR)
                    .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                    .errmsg("constraint must be added to child tables too".to_string())
                    .errhint("Do not specify the ONLY keyword.".to_string())
                    .into_error());
            } else {
                is_no_inherit = true;
            }
        }
    }

    // No constraint exists; we must add one. First determine a name to use, if
    // we haven't already.
    let chosen_name: String = if !recursing {
        debug_assert!(con_name.is_none());
        pg_constraint::ChooseConstraintName(
            mcx,
            &rel.name(),
            col_name,
            "not_null",
            rel.rd_rel.relnamespace,
            &[],
        )?
    } else {
        con_name.expect("recursing SET NOT NULL requires a constraint name").to_string()
    };

    // constraint = makeNotNullConstraint(makeString(colName));
    // constraint->is_no_inherit = is_no_inherit; constraint->conname = conName;
    let mut constraint = make_not_null_constraint(mcx, col_name)?;
    constraint.is_no_inherit = is_no_inherit;
    constraint.conname = Some(PgString::from_str_in(&chosen_name, mcx)?);

    // cooked = AddRelationNewConstraints(rel, NIL, list_make1(constraint),
    //     false, !recursing, false, NULL);
    let constr_node = ::mcx::alloc_in(mcx, Node::mk_constraint(mcx, constraint.clone_in(mcx)?)?)?;
    let new_constraints = [constr_node];
    let cooked = heap::AddRelationNewConstraints(
        mcx,
        rel,
        &[],
        &new_constraints,
        false,      // allow_merge
        !recursing, // is_local
        false,      // is_internal
        None,       // queryString
    )?;

    // ccon = linitial(cooked); ObjectAddressSet(address, ConstraintRelationId, ccon->conoid);
    // The cooked-constraint carrier stashes the freshly-created constraint OID in
    // its `old_pktable_oid` slot (AddRelationNewConstraints / make_cooked_node).
    // A valid objectId is load-bearing: callers (e.g.
    // ATExecAlterConstrInheritability) gate a CommandCounterIncrement on
    // OidIsValid(addr.objectId), and the C code always returns a valid OID from
    // this path. Returning Invalid here suppressed that CCI and caused "tuple
    // already updated by self" when a child was reached twice through diamond
    // inheritance.
    debug_assert!(!cooked.is_empty());
    let ccon_oid = cooked[0].expect_constraint().old_pktable_oid;
    let address = ObjectAddress {
        classId: ConstraintRelationId,
        objectId: ccon_oid,
        objectSubId: 0,
    };

    // Mark pg_attribute.attnotnull for the column and queue validation.
    // C: set_attnotnull(wqueue, rel, attnum, true, true). The owned
    // set_attnotnull cannot take wqueue, so it sets the flag and we queue the
    // phase-3 verify here (NotNullImpliedByRelConstraints is a pure skip
    // optimization; conservatively scanning is always correct).
    crate::create::set_attnotnull(mcx, rel, attnum, true, true)?;
    let tab = ATGetQueueEntry(mcx, wqueue, rel)?;
    wqueue[tab].verify_new_notnull = true;

    // Recurse to propagate the constraint to children that don't have one.
    if recurse {
        let children = find_inheritance_children(mcx, rel.rd_id, lockmode)?;
        for &childoid in children.iter() {
            let childrel = relation_open(mcx, childoid, NoLock)?;

            CommandCounterIncrement()?;

            ATExecSetNotNull(
                mcx,
                wqueue,
                &childrel,
                Some(&chosen_name),
                col_name,
                recurse,
                true,
                lockmode,
            )?;

            drop(childrel);
        }
    }

    Ok(address)
}

// ===========================================================================
// ATExecAddIndex (tablecmds.c:9620)
// ===========================================================================

/// `ATExecAddIndex(tab, rel, stmt, is_rebuild, lockmode)` (tablecmds.c:9620) —
/// create the (unique) index for a transformed constraint via `DefineIndex`.
pub fn ATExecAddIndex<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    rel: &Relation<'mcx>,
    stmt: &IndexStmt<'mcx>,
    is_rebuild: bool,
    _lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    // Assert(!stmt->concurrent);  Assert(stmt->transformed);
    debug_assert!(!stmt.concurrent);
    debug_assert!(stmt.transformed, "ATExecAddIndex: IndexStmt not transformed");

    // suppress schema rights check when rebuilding existing index
    let check_rights = !is_rebuild;
    // skip index build if phase 3 will do it or we're reusing an old one
    let skip_build = tab.rewrite > 0 || OidIsValid(stmt.oldNumber);
    // suppress notices when rebuilding existing index
    let quiet = is_rebuild;

    // address = DefineIndex(RelationGetRelid(rel), stmt, InvalidOid, InvalidOid,
    //     InvalidOid, -1, true, check_rights, false, skip_build, quiet);
    let args = indexcmds_seams::DefineIndexArgs {
        table_id: rel.rd_id,
        stmt: stmt.clone_in(mcx)?,
        index_relation_id: InvalidOid,
        parent_index_id: InvalidOid,
        parent_constraint_id: InvalidOid,
        total_parts: -1,
        is_alter_table: true,
        check_rights,
        check_not_in_use: false, // we did it already
        skip_build,
        quiet,
    };
    let address = indexcmds_seams::define_index_full::call(mcx, args)?;

    // If TryReuseIndex() stashed a relfilenumber for us, we used it for the new
    // index instead of building from scratch.  Restore associated fields. This
    // may store InvalidSubTransactionId in both fields, in which case relcache.c
    // will assume it can rebuild the relcache entry.  Hence, do this after the
    // CCI that made catalog rows visible to any rebuild.  The DROP of the old
    // edition of this index will have scheduled the storage for deletion at
    // commit, so cancel that pending deletion.
    if OidIsValid(stmt.oldNumber) {
        let irel = relation_open(mcx, address.objectId, NoLock)?;
        // C also restores irel->rd_createSubid / rd_firstRelfilelocatorSubid
        // from the stmt; those relcache subid fields are not carried on the
        // trimmed RelationData, and for the reachable path both are
        // InvalidSubTransactionId (the default), so the restore is a no-op.
        catalog_storage_seams::relation_preserve_storage::call(irel.rd_locator, true)?;
        irel.close(NoLock)?;
    }

    Ok(address)
}

// ===========================================================================
// ATExecAddIndexConstraint (tablecmds.c:9704)
// ===========================================================================

/// `ATExecAddIndexConstraint(tab, rel, stmt, lockmode)` (tablecmds.c:9704) —
/// ADD CONSTRAINT ... USING INDEX: promote an existing unique index into a
/// PRIMARY KEY / UNIQUE constraint. The index was already validated at parse
/// time (`transformIndexConstraint` USING-INDEX leg); here we build its
/// `IndexInfo`, run the PRIMARY-KEY checks, and create the catalog entries via
/// `index_constraint_create`.
pub fn ATExecAddIndexConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    _tab: &mut AlteredTableInfo<'mcx>,
    rel: &Relation<'mcx>,
    stmt: &IndexStmt<'mcx>,
    _lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    use ::types_tuple::access::RELKIND_PARTITIONED_TABLE;

    let index_oid = stmt.indexOid;

    // Assert(IsA(stmt, IndexStmt)); Assert(OidIsValid(index_oid));
    // Assert(stmt->isconstraint);
    debug_assert!(OidIsValid(index_oid));
    debug_assert!(stmt.isconstraint);

    // Doing this on partitioned tables is not a simple feature to implement,
    // so let's punt for now.
    if rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        return Err(utils_error::ereport(ERROR)
            .errcode(::types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(
                "ALTER TABLE / ADD CONSTRAINT USING INDEX is not supported on partitioned tables"
                    .to_string(),
            )
            .into_error());
    }

    // index_rel = index_open(index_oid, AccessShareLock);
    let index_rel = relation_open(mcx, index_oid, AccessShareLock)?;

    // indexName = pstrdup(RelationGetRelationName(index_rel));
    let index_name = index_rel.name().to_string();

    // indexInfo = BuildIndexInfo(index_rel);
    let index_info =
        index_seams::build_index_info::call(mcx, &index_rel)?;

    // this should have been checked at parse time
    if !index_info.ii_Unique {
        return Err(utils_error::ereport(ERROR)
            .errmsg_internal(format!("index \"{index_name}\" is not unique"))
            .into_error());
    }

    // Determine name to assign to constraint.  We require a constraint to have
    // the same name as the underlying index; therefore, use the index's
    // existing name as the default constraint name, and if the user explicitly
    // gives some other name for the constraint, rename the index to match.
    let constraint_name: String = match &stmt.idxname {
        Some(n) => {
            let cn = n.as_str().to_string();
            if cn != index_name {
                utils_error::ereport(::types_error::NOTICE)
                    .errmsg(format!(
                        "ALTER TABLE / ADD CONSTRAINT USING INDEX will rename index \"{index_name}\" to \"{cn}\""
                    ))
                    .finish(here("ATExecAddIndexConstraint"))?;
                crate::rename::RenameRelationInternal(mcx, index_oid, &cn, false, true)?;
            }
            cn
        }
        None => index_name.clone(),
    };

    // Extra checks needed if making primary key.
    if stmt.primary {
        index_seams::index_check_primary_key::call(mcx, rel, &index_info, true)?;
    }

    // Note we currently don't support EXCLUSION constraints here.
    let constraint_type = if stmt.primary {
        ::types_catalog::pg_constraint::CONSTRAINT_PRIMARY
    } else {
        ::types_catalog::pg_constraint::CONSTRAINT_UNIQUE
    };

    // Create the catalog entries for the constraint.
    let mut flags: u16 =
        INDEX_CONSTR_CREATE_UPDATE_INDEX | INDEX_CONSTR_CREATE_REMOVE_OLD_DEPS;
    if stmt.initdeferred {
        flags |= INDEX_CONSTR_CREATE_INIT_DEFERRED;
    }
    if stmt.deferrable {
        flags |= INDEX_CONSTR_CREATE_DEFERRABLE;
    }
    if stmt.primary {
        flags |= INDEX_CONSTR_CREATE_MARK_AS_PRIMARY;
    }

    let allow_system_table_mods =
        tablespace_globals_seams::allowSystemTableMods::call()?;

    let address = index_seams::index_constraint_create::call(
        rel,
        index_oid,
        InvalidOid,
        &index_info,
        &constraint_name,
        constraint_type,
        flags,
        allow_system_table_mods,
        false, // is_internal
    )?;

    // index_close(index_rel, NoLock);
    index_rel.close(NoLock)?;

    Ok(address)
}

// ===========================================================================
// ATExecAddConstraint (tablecmds.c:9799)
// ===========================================================================

/// `ATExecAddConstraint(wqueue, tab, rel, newConstraint, recurse, is_readd,
/// lockmode)` (tablecmds.c:9799). Currently we only expect CONSTR_CHECK /
/// CONSTR_NOTNULL nodes here (the PK path coerces NOT NULL through this).
pub fn ATExecAddConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    ti: usize,
    rel: &Relation<'mcx>,
    new_constraint: &Constraint<'mcx>,
    recurse: bool,
    is_readd: bool,
    lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    match new_constraint.contype {
        ConstrType::CONSTR_CHECK | ConstrType::CONSTR_NOTNULL => ATAddCheckNNConstraint(
            mcx,
            wqueue,
            ti,
            rel,
            new_constraint,
            recurse,
            false,
            is_readd,
            lockmode,
        ),
        ConstrType::CONSTR_FOREIGN => {
            // Assign or validate constraint name (tablecmds.c:9819-9841). C
            // scribbles the chosen name onto newConstraint->conname before
            // ATAddForeignKeyConstraint; here we work on an owned copy.
            let mut new_constraint = new_constraint.clone_in(mcx)?;
            match &new_constraint.conname {
                Some(name) => {
                    if pg_constraint::ConstraintNameIsUsed(
                        mcx,
                        ::types_catalog::pg_constraint::ConstraintCategory::Relation,
                        rel.rd_id,
                        name.as_str(),
                    )? {
                        return Err(utils_error::ereport(ERROR)
                            .errcode(::types_error::ERRCODE_DUPLICATE_OBJECT)
                            .errmsg(format!(
                                "constraint \"{}\" for relation \"{}\" already exists",
                                name.as_str(),
                                rel.name()
                            ))
                            .into_error());
                    }
                }
                None => {
                    let addition =
                        choose_foreign_key_constraint_name_addition(&new_constraint.fk_attrs);
                    let conname = pg_constraint::ChooseConstraintName(
                        mcx,
                        &rel.name(),
                        &addition,
                        "fkey",
                        rel.rd_rel.relnamespace,
                        &[],
                    )?;
                    new_constraint.conname = Some(PgString::from_str_in(&conname, mcx)?);
                }
            }

            // ATAddForeignKeyConstraint(wqueue, tab, rel, newConstraint, recurse,
            // false, lockmode) — validate the FK, create the pg_constraint 'f'
            // row, and install the RI enforcement triggers.
            crate::at_fk::ATAddForeignKeyConstraint(
                mcx,
                wqueue,
                ti,
                rel,
                &new_constraint,
                recurse,
                false,
                lockmode,
            )
        }
        other => Err(utils_error::ereport(ERROR)
            .errmsg_internal(format!("unrecognized constraint type: {}", other as i32))
            .into_error()),
    }
}

/// `ChooseForeignKeyConstraintNameAddition(colnames)` (tablecmds.c) — join the
/// FK column names with `_`, capping the running length at `NAMEDATALEN` (the
/// "middle" component handed to `ChooseConstraintName`).
fn choose_foreign_key_constraint_name_addition(fk_attrs: &PgVec<'_, NodePtr<'_>>) -> String {
    // NAMEDATALEN-1 usable chars; C's strlcpy copies up to NAMEDATALEN-1 bytes
    // of `name` at the current offset, then breaks once buflen >= NAMEDATALEN.
    const NAMEDATALEN: usize = 64;
    let mut buf = String::new();
    for attr in fk_attrs.iter() {
        let name = attr.expect_string().sval.as_str();
        if !buf.is_empty() {
            buf.push('_');
        }
        // strlcpy(buf + buflen, name, NAMEDATALEN): each `name` is already a
        // validated identifier (< NAMEDATALEN), so it copies whole; the running
        // buffer then breaks once it reaches NAMEDATALEN (ChooseConstraintName
        // truncates the final composed name anyway).
        buf.push_str(name);
        if buf.len() >= NAMEDATALEN {
            break;
        }
    }
    buf
}

// ===========================================================================
// ATAddCheckNNConstraint (tablecmds.c:9911)
// ===========================================================================

/// `ATAddCheckNNConstraint(wqueue, tab, rel, constr, recurse, recursing,
/// is_readd, lockmode)` (tablecmds.c:9911) — add a CHECK or NOT NULL constraint
/// to a single table and its children (recursing one level at a time).
pub fn ATAddCheckNNConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    ti: usize,
    rel: &Relation<'mcx>,
    constr: &Constraint<'mcx>,
    recurse: bool,
    recursing: bool,
    is_readd: bool,
    lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    let mut address = ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    };

    // Guard against stack overflow due to overly deep inheritance tree.
    check_stack_depth()?;

    // At top level, permission check was done in ATPrepCmd, else do it.
    if recursing {
        ATSimplePermissions(
            AlterTableType::AT_AddConstraint,
            rel,
            ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
        )?;
    }

    // newcons = AddRelationNewConstraints(rel, NIL, list_make1(copyObject(constr)),
    //     recursing || is_readd, !recursing, is_readd, NULL);
    let constr_copy = ::mcx::alloc_in(mcx, Node::mk_constraint(mcx, constr.clone_in(mcx)?)?)?;
    let new_constraints = [constr_copy];
    let newcons = heap::AddRelationNewConstraints(
        mcx,
        rel,
        &[],
        &new_constraints,
        recursing || is_readd, // allow_merge
        !recursing,            // is_local
        is_readd,              // is_internal
        None,                  // queryString
    )?;

    // Assert(list_length(newcons) <= 1);
    debug_assert!(newcons.len() <= 1);

    // The cooked-constraint carrier (Node::Constraint per backend-catalog-heap):
    // contype/conname/skip_validation are direct; `location` carries attnum and
    // `initially_valid` carries is_local. The C `ccon->conoid` is not carried,
    // so the returned ObjectAddress' objectId is left Invalid for NOT NULL/CHECK
    // (event-trigger surface only; not consumed on the PK path).
    let mut assigned_conname: Option<PgString<'mcx>> = None;
    for ccon_node in newcons.iter() {
        let ccon = ccon_node.expect_constraint();
        let ccon_contype = ccon.contype;
        let ccon_attnum = ccon.location as AttrNumber;

        // Add each to-be-validated constraint to Phase 3's queue.
        if !ccon.skip_validation && ccon_contype != ConstrType::CONSTR_NOTNULL {
            let newcon = NewConstraint {
                name: opt_str_clone(mcx, &ccon.conname)?,
                contype: ccon_contype as i32,
                refrelid: InvalidOid,
                refindid: InvalidOid,
                conid: InvalidOid,
                // The cooked-constraint carrier rides the cooked expr Node on
                // `raw_expr` (see backend-catalog-heap make_cooked_node).
                qual: match &ccon.raw_expr {
                    Some(n) => Some(::mcx::alloc_in(mcx, n.clone_in(mcx)?)?),
                    None => None,
                },
            };
            wqueue[ti].constraints.push(newcon);
        }

        // Save the actually assigned name if it was defaulted.
        if constr.conname.is_none() {
            assigned_conname = opt_str_clone(mcx, &ccon.conname)?;
        }

        // If adding a valid not-null constraint, set the pg_attribute flag and
        // tell phase 3 to verify existing rows, if needed.
        if constr.contype == ConstrType::CONSTR_NOTNULL {
            crate::create::set_attnotnull(
                mcx,
                rel,
                ccon_attnum,
                !constr.skip_validation,
                !constr.skip_validation,
            )?;

            // C: set_attnotnull(wqueue, ...) queues a phase-3 verification scan
            // when queue_validation && !NotNullImpliedByRelConstraints(rel,attr).
            // The owned set_attnotnull cannot take wqueue, so we queue it here.
            // NotNullImpliedByRelConstraints is a pure optimization that lets C
            // *skip* the scan when an existing validated CHECK already implies
            // NOT NULL; conservatively scanning is always correct.
            if !constr.skip_validation {
                wqueue[ti].verify_new_notnull = true;
            }
        }

        address = ObjectAddress {
            classId: ConstraintRelationId,
            objectId: InvalidOid,
            objectSubId: 0,
        };
    }

    // Advance command counter in case same table is visited multiple times.
    CommandCounterIncrement()?;

    // If the constraint got merged with an existing constraint, we're done.
    if newcons.is_empty() {
        return Ok(address);
    }

    // If adding a NO INHERIT constraint, no need to find our children.
    if constr.is_no_inherit {
        return Ok(address);
    }

    // Propagate to children as appropriate, one level of recursion at a time.
    let children = find_inheritance_children(mcx, rel.rd_id, lockmode)?;

    // Check if ONLY was specified with ALTER TABLE.
    if !recurse && !children.is_empty() {
        return Err(utils_error::ereport(ERROR)
            .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
            .errmsg("constraint must be added to child tables too".to_string())
            .into_error());
    }

    // Build the to-be-recursed constraint: if a name was just defaulted, fix it
    // so the child constraints share the parent's name (C scribbles on `constr`).
    let mut child_constr = constr.clone_in(mcx)?;
    if child_constr.conname.is_none() {
        child_constr.conname = assigned_conname;
    }

    // Recurse to create the constraint on each child.
    for &childrelid in children.iter() {
        // find_inheritance_children already got lock.
        let childrel = relation_open(mcx, childrelid, NoLock)?;
        CheckAlterTableIsSafe(&childrel)?;

        // Find or create work queue entry for this table.
        let childtab = ATGetQueueEntry(mcx, wqueue, &childrel)?;

        // Recurse to this child.
        ATAddCheckNNConstraint(
            mcx,
            wqueue,
            childtab,
            &childrel,
            &child_constr,
            recurse,
            true,
            is_readd,
            lockmode,
        )?;

        drop(childrel);
    }

    Ok(address)
}

// ===========================================================================
// Phase-3 NOT NULL verification scan
// (the newrel == NULL, NOT-NULL-only path of ATRewriteTable, tablecmds.c:6126)
// ===========================================================================

/// Verify, by a full table scan, that every column with a (newly added,
/// validated) NOT NULL marking contains no NULL value. This is the
/// `newrel == NULL`, no-CHECK, no-partition-constraint, no-virtual-generated
/// slice of `ATRewriteTable`: when ALTER TABLE adds a NOT NULL (e.g. coerced by
/// ADD PRIMARY KEY) and `tab->verify_new_notnull` is set, phase 3 rescans the
/// existing rows.
pub fn at_verify_not_null<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<()> {
    use ::execTuples::exec_init_slots::ExecDropSingleTupleTableSlot;
    use ::execTuples::slot_deform::slot_getattr;

    // oldrel = table_open(tab->relid, NoLock);
    let oldrel = relation_open(mcx, relid, NoLock)?;
    let new_tup_desc = &oldrel.rd_att; // RelationGetDescr(oldrel) — includes all mods

    // Collect attnums of *valid* (non-virtual) NOT NULL columns.
    // notnull_attrs: attr->attnullability == ATTNULLABLE_VALID && !attisdropped
    //   && attgenerated != ATTRIBUTE_GENERATED_VIRTUAL.
    let mut notnull_attrs: Vec<AttrNumber> = Vec::new();
    let mut has_virtual_notnull = false;
    for i in 0..new_tup_desc.natts {
        let att = new_tup_desc.attr(i as usize);
        if att.attnotnull && !att.attisdropped {
            if att.attgenerated == ::types_tuple::access::ATTRIBUTE_GENERATED_VIRTUAL {
                has_virtual_notnull = true;
            } else {
                notnull_attrs.push(att.attnum);
            }
        }
    }

    if has_virtual_notnull {
        unported("ATRewriteTable NOT NULL verify over virtual generated column");
    }

    if notnull_attrs.is_empty() {
        // needscan would be false; nothing to verify.
        oldrel.close(NoLock)?;
        return Ok(());
    }

    // snapshot = RegisterSnapshot(GetLatestSnapshot());
    // scan = table_beginscan(oldrel, snapshot, 0, NULL);
    //
    // C registers a fresh latest snapshot for the scan; here we reuse the active
    // snapshot already pushed by the utility portal (it is at least as new as the
    // ALTER's own catalog mutations, which is what the verify scan needs to see),
    // mirroring the copyto.c scan idiom. Reusing the managed active snapshot
    // avoids a private RegisterSnapshot/UnregisterSnapshot pair (and the
    // resource-owner leak that an AM-aliased private snapshot would produce).
    let snap_rc = snapmgr_seams::get_active_snapshot::call()?
        .expect("ALTER TABLE NOT NULL verify scan with no active snapshot");

    let rel_alias = oldrel.alias();

    // The scan + slot + per-row null check, wrapped so we always run cleanup
    // (table_endscan + drop slot) before returning, including on the Err path.
    let result: PgResult<()> = (|| {
        let mut scan = table_tableam_seams::table_beginscan::call(
            mcx, &rel_alias, snap_rc,
        )?;
        let mut slot = table_tableam::table_slot_create(mcx, &rel_alias)?;

        // while (table_scan_getnextslot(scan, ForwardScanDirection, oldslot))
        while table_tableam_seams::table_scan_getnextslot::call(
            mcx, &mut scan, &mut slot,
        )? {
            postgres_seams::check_for_interrupts::call()?;

            for &attn in notnull_attrs.iter() {
                let (_value, isnull) = slot_getattr(mcx, &mut slot, attn)?;
                if isnull {
                    let attr = new_tup_desc.attr((attn - 1) as usize);
                    let attname =
                        String::from_utf8_lossy(attr.attname.name_str()).into_owned();
                    let relname = oldrel.name().to_string();
                    table_tableam::table_endscan(scan)?;
                    ExecDropSingleTupleTableSlot(slot)?;
                    return utils_error::ereport(ERROR)
                        .errcode(::types_error::ERRCODE_NOT_NULL_VIOLATION)
                        .errmsg(format!(
                            "column \"{attname}\" of relation \"{relname}\" contains null values"
                        ))
                        .finish(here("at_verify_not_null"));
                }
            }
        }

        // table_endscan(scan); ExecDropSingleTupleTableSlot(oldslot);
        table_tableam::table_endscan(scan)?;
        ExecDropSingleTupleTableSlot(slot)?;
        Ok(())
    })();

    // table_close(oldrel, NoLock) — release the relcache reference.
    oldrel.close(NoLock)?;
    result
}
