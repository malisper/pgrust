//! `commands/tablecmds.c` — ALTER TABLE phase machinery (the "spine").
//!
//! Ported faithfully from PostgreSQL 18.3: the work-queue structs, the pass
//! ordering (`AlterTablePass` / `AT_PASS_*` / `AT_NUM_PASSES`), the relkind
//! targeting bitmask (`ATT_*`), `AlterTableGetLockLevel`, `CheckAlterTableIsSafe`,
//! `ATController`, `ATPrepCmd`, `ATRewriteCatalogs`, `ATExecCmd`,
//! `ATRewriteTables`, `ATGetQueueEntry`, `alter_table_type_to_string`,
//! `ATSimplePermissions`, `ATSimpleRecursion`, `AlterTable` and
//! `AlterTableInternal`.
//!
//! The PORTABLE executed families dispatched from [`ATExecCmd`] live in
//! [`crate::at_column`]:
//!   - SET / DROP DEFAULT (`ATExecColumnDefault`)
//!   - cooked DEFAULT (`ATExecCookedColumnDefault`)
//!   - SET STATISTICS (`ATExecSetStatistics`)
//!   - SET / RESET column OPTIONS (`ATExecSetOptions`)
//!   - SET STORAGE (`ATExecSetStorage`)
//!   - SET / RESET / REPLACE relOPTIONS (`ATExecSetRelOptions`)
//!   - OWNER (wired to the existing `at_exec_change_owner` outward seam)
//!
//! Every NOT-yet-ported subcommand family (ADD/DROP COLUMN, ALTER TYPE +
//! ATRewriteTable, SET/DROP NOT NULL, constraint/index/trigger/rule/inherit/
//! partition/identity/persistence/access-method/tablespace arms) faithfully
//! seam-and-panics via [`unported`]: we mirror the C control flow and stop with
//! a precise rationale rather than silently stubbing or restructuring.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
// The `AT_PASS_*` ordering constants and the phase-3 carrier fields
// (`AlteredTableInfo` / `NewColumnValue` / `NewConstraint`) mirror tablecmds.c
// verbatim; they are consumed by the rewrite/constraint/column families that
// are still seam-and-panicked, so several read as dead code today.
#![allow(dead_code)]

use mcx::{Mcx, PgVec};

use types_catalog::catalog_dependency::ObjectAddress;
use types_core::primitive::{InvalidOid, Oid};
use types_error::{PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR};
use types_acl::ACLCHECK_NOT_OWNER;
use types_nodes::ddlnodes::{AlterTableCmd, AlterTableStmt, AlterTableType};
use types_nodes::ddlnodes::AlterTableType::*;
use types_nodes::nodes::{Node, NodePtr};
use types_nodes::parsenodes::{
    ObjectType, OBJECT_FOREIGN_TABLE, OBJECT_INDEX, OBJECT_MATVIEW, OBJECT_SEQUENCE, OBJECT_TYPE,
    OBJECT_VIEW,
};
use types_rel::Relation;
use types_storage::lock::{
    LOCKMODE, AccessExclusiveLock, NoLock, ShareRowExclusiveLock, ShareUpdateExclusiveLock,
};
use types_tuple::access::{
    RangeVar as AccessRangeVar, RELKIND_COMPOSITE_TYPE, RELKIND_FOREIGN_TABLE, RELKIND_INDEX,
    RELKIND_MATVIEW, RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
    RELKIND_SEQUENCE, RELKIND_VIEW, RELPERSISTENCE_PERMANENT,
};
use types_tuple::heaptuple::TupleDescData;

use backend_access_common_relation::relation_open;
use backend_access_common_tupdesc::CreateTupleDescCopyConstr;
use backend_access_transam_xact::CommandCounterIncrement;
use backend_catalog_aclchk_seams as aclchk_seam;
use backend_catalog_catalog::IsSystemRelation;
use backend_catalog_objectaddress_seams as objaddr_seam;
use backend_catalog_pg_class_seams as pgclass_seam;
use backend_catalog_pg_inherits_seams as inherits_seam;
use backend_utils_init_miscinit::GetUserId;

use backend_commands_tablecmds_seams as seam;

use crate::helpers::{here, RelationRelationId};

/// `errdetail_relkind_not_supported` is exposed through the pg_class seam.
fn errdetail_relkind_not_supported(relkind: u8) -> PgResult<String> {
    pgclass_seam::errdetail_relkind_not_supported::call(relkind)
}

/// `(AlterTableCmd *) lfirst(lcmd)` — project an `AlterTableCmd` out of a
/// `Node::AlterTableCmd` work-list element.
fn as_alter_table_cmd<'a, 'mcx>(node: &'a NodePtr<'mcx>) -> &'a AlterTableCmd<'mcx> {
    match node.as_altertablecmd() {
        Some(c) => c,
        None => unreachable!("Node::AlterTableCmd expected"),
    }
}

/// `(List *) cmd->def` as the reloptions working-view `Vec<DefElem>` — the
/// relOptions subcommands carry a `Node::List` of `Node::DefElem`. (C:
/// `castNode(List, cmd->def)`.) The consumers are
/// `AlterTableGetRelOptionsLockLevel` (reads `defname`) and the unported
/// `ATExecSetRelOptions` body. We populate `defnamespace`/`defname`; `arg` is
/// left `None` here — the lock-level computation never reads it and
/// `ATExecSetRelOptions` (which would) is unported.
fn cmd_def_elem_list<'mcx>(
    cmd: &AlterTableCmd<'mcx>,
) -> Vec<backend_access_common_reloptions::DefElem> {
    let mut out = Vec::new();
    if let Some(def) = &cmd.def {
        if let Some(items) = def.as_list() {
            for it in items.iter() {
                if let Some(de) = it.as_defelem() {
                    out.push(backend_access_common_reloptions::DefElem::new(
                        de.defnamespace.as_ref().map(|s| s.as_str()),
                        de.defname.as_ref().map(|s| s.as_str()).unwrap_or(""),
                        None,
                    ));
                }
            }
        }
    }
    out
}

/// Faithful seam-and-panic for an ALTER TABLE subcommand family whose execution
/// path is not yet ported. We mirror the C structure up to this point and then
/// stop loudly, rather than `todo!()`/`unimplemented!()` or restructuring.
fn unported(what: &str) -> ! {
    panic!(
        "ALTER TABLE: {what} is not yet ported in backend-commands-tablecmds \
         (faithful seam-and-panic: the executed family is unported — see at_phase.rs)"
    );
}

// ===========================================================================
// allowed_targets bitmask (ATT_*) — tablecmds.c
// ===========================================================================

pub const ATT_TABLE: i32 = 1 << 0;
pub const ATT_VIEW: i32 = 1 << 1;
pub const ATT_INDEX: i32 = 1 << 2;
pub const ATT_COMPOSITE_TYPE: i32 = 1 << 3;
pub const ATT_FOREIGN_TABLE: i32 = 1 << 4;
pub const ATT_PARTITIONED_INDEX: i32 = 1 << 5;
pub const ATT_SEQUENCE: i32 = 1 << 6;
pub const ATT_PARTITIONED_TABLE: i32 = 1 << 7;
pub const ATT_MATVIEW: i32 = 1 << 8;

// ===========================================================================
// AlterTablePass — phase-2 ordering (tablecmds.c)
// ===========================================================================

pub type AlterTablePass = i32;

pub const AT_PASS_UNSET: AlterTablePass = -1; /* nothing yet */
pub const AT_PASS_DROP: AlterTablePass = 0; /* DROP (all flavors) */
pub const AT_PASS_ALTER_TYPE: AlterTablePass = 1; /* ALTER COLUMN TYPE */
pub const AT_PASS_ADD_COL: AlterTablePass = 2; /* ADD COLUMN */
pub const AT_PASS_SET_EXPRESSION: AlterTablePass = 3; /* SET EXPRESSION */
pub const AT_PASS_OLD_COL_ATTRS: AlterTablePass = 4; /* re-install attnotnull */
pub const AT_PASS_OLD_INDEX: AlterTablePass = 5; /* re-add existing indexes */
pub const AT_PASS_OLD_CONSTR: AlterTablePass = 6; /* re-add existing constraints */
/* We could support a RENAME COLUMN pass here, but not currently used */
pub const AT_PASS_ADD_COL_NOT_NULL: AlterTablePass = 7; /* set not-null after add */
pub const AT_PASS_ADD_INDEX: AlterTablePass = 8; /* ADD indexes */
pub const AT_PASS_ADD_CONSTR: AlterTablePass = 9; /* ADD constraints, defaults */
pub const AT_PASS_COL_ATTRS: AlterTablePass = 10; /* set column attributes, eg NOT NULL */
pub const AT_PASS_ADD_INDEXCONSTR: AlterTablePass = 11; /* ADD index-based constraints */
pub const AT_PASS_ADD_OTHERCONSTR: AlterTablePass = 12; /* ADD other constraints, defaults */
pub const AT_PASS_MISC: AlterTablePass = 13; /* other stuff */
pub const AT_NUM_PASSES: usize = 14;

// ===========================================================================
// Work-queue carriers (tablecmds.c). The trimmed phase-3 substructures
// (NewColumnValue / NewConstraint) are carried verbatim from the C, but the
// phase-3 rewrite engine that consumes them is itself unported (see
// ATRewriteTables) so they currently stay empty.
// ===========================================================================

/// `AlteredTableInfo` (tablecmds.c) — per-relation ALTER TABLE work-queue entry.
pub struct AlteredTableInfo<'mcx> {
    /* Information saved before any work commences: */
    pub relid: Oid,                          /* Relation to work on */
    pub relkind: u8,                         /* Its relkind */
    pub oldDesc: TupleDescData<'mcx>,        /* Pre-modification tuple descriptor */

    /*
     * Transiently set during Phase 2, normally set to NULL.
     *
     * In the relation: store the OID and re-open with NoLock during phase 2.
     * The C field `Relation rel` becomes an Option<Relation> opened per-pass.
     */
    pub rel: Option<Relation<'mcx>>,

    /* Information saved by Phase 1 for Phase 2: */
    pub subcmds: [PgVec<'mcx, NodePtr<'mcx>>; AT_NUM_PASSES], /* Lists of AlterTableCmd */

    /* Information saved by Phases 1/2 for Phase 3: */
    pub constraints: PgVec<'mcx, NewConstraint<'mcx>>, /* List of NewConstraint */
    pub newvals: PgVec<'mcx, NewColumnValue<'mcx>>,    /* List of NewColumnValue */

    pub verify_new_notnull: bool, /* T if we should recheck NOT NULL */
    pub rewrite: i32,             /* Reason for forced rewrite, if any */
    pub newAccessMethod: Oid,     /* new access method; 0 means no change */
    pub chgAccessMethod: bool,    /* T if SET ACCESS METHOD is used */
    pub newTableSpace: Oid,       /* new tablespace; 0 means no change */
    pub chgPersistence: bool,     /* T if SET LOGGED/UNLOGGED is used */
    pub newrelpersistence: u8,    /* if above is true */
    pub partition_constraint: Option<NodePtr<'mcx>>, /* for attach partition validation */
    pub validate_default: bool,   /* validate default over partition constraint */

    /* Objects to rebuild after completing ALTER TYPE operations */
    pub changedConstraintOids: PgVec<'mcx, Oid>, /* OIDs of constraints to rebuild */
    pub changedConstraintDefs: PgVec<'mcx, NodePtr<'mcx>>, /* string definitions of same */
    pub changedIndexOids: PgVec<'mcx, Oid>,     /* OIDs of indexes to rebuild */
    pub changedIndexDefs: PgVec<'mcx, NodePtr<'mcx>>, /* string definitions of same */
    pub replicaIdentityIndex: Option<mcx::PgString<'mcx>>, /* index to reset as REPLICA IDENTITY */
    pub clusterOnIndex: Option<mcx::PgString<'mcx>>, /* index to use for CLUSTER */
    pub changedStatisticsOids: PgVec<'mcx, Oid>, /* OIDs of statistics to rebuild */
    pub changedStatisticsDefs: PgVec<'mcx, NodePtr<'mcx>>, /* statistics definitions */
}

/// `NewColumnValue` (tablecmds.c) — phase-3 per-column new-value descriptor.
pub struct NewColumnValue<'mcx> {
    pub attnum: i16,
    pub expr: Option<NodePtr<'mcx>>,
    pub is_generated: bool,
}

/// `NewConstraint` (tablecmds.c) — phase-3 added-constraint descriptor.
pub struct NewConstraint<'mcx> {
    pub name: Option<mcx::PgString<'mcx>>,
    pub contype: i32,
    pub refrelid: Oid,
    pub refindid: Oid,
    pub conid: Oid,
    pub qual: Option<NodePtr<'mcx>>,
}

/// `AlterTableUtilityContext` (tablecmds.c). The unported fields (the parse
/// tree `pstmt`, bound `params`, and `queryEnv`) are needed only by the
/// transform-and-recurse families that are themselves unported; we keep the
/// portable subset (`relid` + `queryString`).
pub struct AlterTableUtilityContext<'a> {
    pub relid: Oid,
    pub query_string: Option<&'a str>,
}

// ===========================================================================
// CheckAlterTableIsSafe (tablecmds.c:4449)
// ===========================================================================

/// `CheckAlterTableIsSafe(Relation rel)` (tablecmds.c:4449).
pub(crate) fn CheckAlterTableIsSafe(rel: &Relation<'_>) -> PgResult<()> {
    // if (RELATION_IS_OTHER_TEMP(rel)) ereport(...).
    if seam::relation_is_other_temp::call(rel)? {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("cannot alter temporary tables of other sessions")
            .finish(here("CheckAlterTableIsSafe"));
    }

    // CheckTableNotInUse(rel, "ALTER TABLE");
    crate::smallfns::check_table_not_in_use(rel, "ALTER TABLE")
}

// ===========================================================================
// AlterTable / AlterTableInternal (tablecmds.c:4534 / 4563)
// ===========================================================================

/// `AlterTable(stmt, lockmode, context)` (tablecmds.c:4534) — execute ALTER
/// TABLE, a list of subcommands, in three phases. The caller must already hold
/// an adequate lock (`AlterTableGetLockLevel(stmt->cmds)` or higher).
pub fn AlterTable<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterTableStmt<'mcx>,
    lockmode: LOCKMODE,
    context: &AlterTableUtilityContext<'_>,
) -> PgResult<()> {
    // rel = relation_open(context->relid, NoLock);  (caller holds the lock)
    let rel = relation_open(mcx, context.relid, NoLock)?;

    CheckAlterTableIsSafe(&rel)?;

    // ATController(stmt, rel, stmt->cmds, stmt->relation->inh, lockmode, context);
    let inh = stmt
        .relation
        .as_ref()
        .map(|rv| match rv.as_rangevar() {
            Some(rv) => rv.inh,
            None => false,
        })
        .unwrap_or(false);

    ATController(mcx, Some(stmt), rel, &stmt.cmds, inh, lockmode, context)
}

/// `AlterTableInternal(relid, cmds, recurse)` (tablecmds.c:4563) — ALTER TABLE
/// with target given by OID, no parse-transformation subcommands allowed.
pub fn AlterTableInternal<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    cmds: &PgVec<'mcx, NodePtr<'mcx>>,
    recurse: bool,
) -> PgResult<()> {
    let lockmode = AlterTableGetLockLevel(cmds)?;

    let rel = relation_open(mcx, relid, lockmode)?;

    // EventTriggerAlterTableRelid(relid);
    seam::event_trigger_alter_table_relid::call(relid)?;

    // ATController(NULL, rel, cmds, recurse, lockmode, NULL);
    let context = AlterTableUtilityContext {
        relid,
        query_string: None,
    };
    ATController(mcx, None, rel, cmds, recurse, lockmode, &context)
}

// ===========================================================================
// AlterTableLookupRelation (tablecmds.c:4475) + RangeVarCallbackForAlterRelation
// (tablecmds.c:19586)
// ===========================================================================

/// `AlterTableLookupRelation(stmt, lockmode)` (tablecmds.c:4475) —
/// `RangeVarGetRelidExtended(stmt->relation, lockmode, missing_ok ? RVR_MISSING_OK
/// : 0, RangeVarCallbackForAlterRelation, stmt)`.
pub fn AlterTableLookupRelation<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterTableStmt<'mcx>,
    lockmode: LOCKMODE,
) -> PgResult<Oid> {
    let rv_node = stmt
        .relation
        .as_ref()
        .expect("AlterTableStmt.relation is non-NULL");
    let rv = match rv_node.as_rangevar() {
        Some(rv) => rv,
        None => unreachable!("AlterTableStmt.relation is a Node::RangeVar"),
    };
    let access_rv = crate::helpers::to_access_range_var(rv);
    let flags = if stmt.missing_ok {
        backend_catalog_namespace::RVR_MISSING_OK
    } else {
        0
    };
    let mut cb = |callback_rel: &AccessRangeVar, relid: Oid, oldrelid: Oid| {
        RangeVarCallbackForAlterRelation(mcx, callback_rel, relid, oldrelid, stmt.objtype)
    };
    backend_catalog_namespace::RangeVarGetRelidExtended(
        mcx,
        &access_rv,
        lockmode,
        flags,
        Some(&mut cb),
    )
}

/// `RangeVarCallbackForAlterRelation(rv, relid, oldrelid, arg)` (tablecmds.c:19586)
/// — the `RangeVarGetRelidExtended` callback used by ALTER. The C function is
/// shared by RenameStmt / AlterObjectSchemaStmt / AlterTableStmt; the
/// `alter_table_slow` entry always passes an AlterTableStmt, so `reltype` is the
/// statement's `objtype`. (The RenameStmt namespace-`ACL_CREATE` recheck and the
/// `RenameStmt`-relaxed ALTER INDEX rule live in the rename/schema utility arms,
/// which carry their own callback; here `objtype` is the ALTER TABLE objtype.)
fn RangeVarCallbackForAlterRelation<'mcx>(
    _mcx: Mcx<'mcx>,
    rv: &AccessRangeVar,
    relid: Oid,
    _oldrelid: Oid,
    reltype: ObjectType,
) -> PgResult<()> {
    // tuple = SearchSysCache1(RELOID, relid); if (!valid) return; (concurrently dropped)
    let Some(info) = seam::get_pg_class_drop_info::call(relid)? else {
        return Ok(());
    };
    let relkind = info.relkind;

    // Must own relation.
    if !aclchk_seam::object_ownercheck::call(RelationRelationId, relid, GetUserId())? {
        let actual_kind = backend_utils_cache_lsyscache_seams::get_rel_relkind::call(relid)?;
        aclchk_seam::aclcheck_error::call(
            ACLCHECK_NOT_OWNER,
            objaddr_seam::get_relkind_objtype::call(actual_kind),
            Some(rv.relname.clone()),
        )?;
    }

    // No system table modifications unless explicitly allowed.
    if !backend_commands_tablespace_globals_seams::allowSystemTableMods::call()?
        && seam::is_system_class_relid::call(relid, relkind, info.relnamespace)?
    {
        return backend_utils_error::ereport(ERROR)
            .errcode(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "permission denied: \"{}\" is a system catalog",
                rv.relname
            ))
            .finish(here("RangeVarCallbackForAlterRelation"));
    }

    // For compatibility, ALTER TABLE works on most relation types; the explicit
    // forms must match the relkind.
    if reltype == OBJECT_SEQUENCE && relkind != RELKIND_SEQUENCE {
        return wrong_object_type(&rv.relname, "is not a sequence");
    }
    if reltype == OBJECT_VIEW && relkind != RELKIND_VIEW {
        return wrong_object_type(&rv.relname, "is not a view");
    }
    if reltype == OBJECT_MATVIEW && relkind != RELKIND_MATVIEW {
        return wrong_object_type(&rv.relname, "is not a materialized view");
    }
    if reltype == OBJECT_FOREIGN_TABLE && relkind != RELKIND_FOREIGN_TABLE {
        return wrong_object_type(&rv.relname, "is not a foreign table");
    }
    if reltype == OBJECT_TYPE && relkind != RELKIND_COMPOSITE_TYPE {
        return wrong_object_type(&rv.relname, "is not a composite type");
    }
    if reltype == OBJECT_INDEX
        && relkind != RELKIND_INDEX
        && relkind != RELKIND_PARTITIONED_INDEX
    {
        return wrong_object_type(&rv.relname, "is not an index");
    }

    // Don't allow ALTER TABLE on composite types — use ALTER TYPE instead.
    if reltype != OBJECT_TYPE && relkind == RELKIND_COMPOSITE_TYPE {
        return backend_utils_error::ereport(ERROR)
            .errcode(types_error::ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("\"{}\" is a composite type", rv.relname))
            .errhint("Use ALTER TYPE instead.")
            .finish(here("RangeVarCallbackForAlterRelation"));
    }

    Ok(())
}

/// The `ereport(ERROR, ERRCODE_WRONG_OBJECT_TYPE, "\"%s\" <suffix>")` raised by
/// the relkind/objtype mismatch checks in `RangeVarCallbackForAlterRelation`.
fn wrong_object_type(relname: &str, suffix: &str) -> PgResult<()> {
    backend_utils_error::ereport(ERROR)
        .errcode(types_error::ERRCODE_WRONG_OBJECT_TYPE)
        .errmsg(format!("\"{relname}\" {suffix}"))
        .finish(here("RangeVarCallbackForAlterRelation"))
}

// ===========================================================================
// AlterTableGetLockLevel (tablecmds.c:4607)
// ===========================================================================

/// `AlterTableGetLockLevel(List *cmds)` (tablecmds.c:4607).
pub fn AlterTableGetLockLevel(cmds: &PgVec<'_, NodePtr<'_>>) -> PgResult<LOCKMODE> {
    let mut lockmode: LOCKMODE = ShareUpdateExclusiveLock;

    for lcmd in cmds.iter() {
        let cmd = as_alter_table_cmd(lcmd);
        let mut cmd_lockmode: LOCKMODE = AccessExclusiveLock; /* default for compiler */

        match cmd.subtype {
            // These subcommands rewrite the heap, so require full locks.
            AT_AddColumn | AT_SetAccessMethod | AT_SetTableSpace | AT_AlterColumnType => {
                cmd_lockmode = AccessExclusiveLock;
            }

            // May require addition of toast tables.
            AT_SetStorage => {
                cmd_lockmode = AccessExclusiveLock;
            }

            // Removing constraints can affect optimized SELECTs.
            AT_DropConstraint | AT_DropNotNull => {
                cmd_lockmode = AccessExclusiveLock;
            }

            // Subcommands that may be visible to concurrent SELECTs.
            AT_DropColumn | AT_AddColumnToView | AT_DropOids | AT_EnableAlwaysRule
            | AT_EnableReplicaRule | AT_EnableRule | AT_DisableRule => {
                cmd_lockmode = AccessExclusiveLock;
            }

            // Changing owner may remove implicit SELECT privileges.
            AT_ChangeOwner => {
                cmd_lockmode = AccessExclusiveLock;
            }

            // Changing foreign table options may affect optimization.
            AT_GenericOptions | AT_AlterColumnGenericOptions => {
                cmd_lockmode = AccessExclusiveLock;
            }

            // These subcommands affect write operations only.
            AT_EnableTrig | AT_EnableAlwaysTrig | AT_EnableReplicaTrig | AT_EnableTrigAll
            | AT_EnableTrigUser | AT_DisableTrig | AT_DisableTrigAll | AT_DisableTrigUser => {
                cmd_lockmode = ShareRowExclusiveLock;
            }

            // These subcommands affect write operations only.
            AT_ColumnDefault | AT_CookedColumnDefault | AT_AlterConstraint | AT_AddIndex
            | AT_AddIndexConstraint | AT_ReplicaIdentity | AT_SetNotNull | AT_EnableRowSecurity
            | AT_DisableRowSecurity | AT_ForceRowSecurity | AT_NoForceRowSecurity
            | AT_AddIdentity | AT_DropIdentity | AT_SetIdentity | AT_SetExpression
            | AT_DropExpression | AT_SetCompression => {
                cmd_lockmode = AccessExclusiveLock;
            }

            AT_AddConstraint | AT_ReAddConstraint | AT_ReAddDomainConstraint => {
                // if (IsA(cmd->def, Constraint))
                if let Some(def) = &cmd.def {
                    if let Some(con) = def.as_constraint() {
                        use types_nodes::ddlnodes::ConstrType::*;
                        cmd_lockmode = match con.contype {
                            CONSTR_EXCLUSION | CONSTR_PRIMARY | CONSTR_UNIQUE => AccessExclusiveLock,
                            CONSTR_FOREIGN => ShareRowExclusiveLock,
                            _ => AccessExclusiveLock,
                        };
                    }
                }
            }

            // These subcommands affect inheritance behaviour.
            AT_AddInherit | AT_DropInherit => {
                cmd_lockmode = AccessExclusiveLock;
            }

            // These subcommands affect implicit row type conversion.
            AT_AddOf | AT_DropOf => {
                cmd_lockmode = AccessExclusiveLock;
            }

            // Only used by CREATE OR REPLACE VIEW.
            AT_ReplaceRelOptions => {
                cmd_lockmode = AccessExclusiveLock;
            }

            // Strategy/maintenance changes; lowest restriction.
            AT_SetStatistics | AT_ClusterOn | AT_DropCluster | AT_SetOptions | AT_ResetOptions => {
                cmd_lockmode = ShareUpdateExclusiveLock;
            }

            AT_SetLogged | AT_SetUnLogged => {
                cmd_lockmode = AccessExclusiveLock;
            }

            AT_ValidateConstraint => {
                cmd_lockmode = ShareUpdateExclusiveLock;
            }

            // Rel options: tables/views/indexes share this grammar.
            AT_SetRelOptions | AT_ResetRelOptions => {
                let def_list = cmd_def_elem_list(cmd);
                cmd_lockmode =
                    backend_access_common_reloptions::AlterTableGetRelOptionsLockLevel(&def_list[..]);
            }

            AT_AttachPartition => {
                cmd_lockmode = ShareUpdateExclusiveLock;
            }

            AT_DetachPartition => {
                // if (((PartitionCmd *) cmd->def)->concurrent) ...
                let concurrent = cmd
                    .def
                    .as_ref()
                    .map(|d| match d.as_partitioncmd() {
                        Some(pc) => pc.concurrent,
                        None => false,
                    })
                    .unwrap_or(false);
                cmd_lockmode = if concurrent {
                    ShareUpdateExclusiveLock
                } else {
                    AccessExclusiveLock
                };
            }

            AT_DetachPartitionFinalize => {
                cmd_lockmode = ShareUpdateExclusiveLock;
            }

            // C: `default: elog(ERROR, "unrecognized alter table type")`. These
            // internal-only subtypes (AT_ReAdd*) never appear in a user command
            // list reaching AlterTableGetLockLevel.
            AT_ReAddIndex | AT_ReAddComment | AT_ReAddStatistics => {
                return backend_utils_error::ereport(ERROR)
                    .errmsg_internal(format!(
                        "unrecognized alter table type: {}",
                        cmd.subtype as i32
                    ))
                    .finish(here("AlterTableGetLockLevel"))
                    .map(|()| unreachable!());
            }
        }

        // Take the greatest lockmode from any subcommand.
        if cmd_lockmode > lockmode {
            lockmode = cmd_lockmode;
        }
    }

    Ok(lockmode)
}

// ===========================================================================
// ATController (tablecmds.c:4869)
// ===========================================================================

/// `ATController(...)` (tablecmds.c:4869) — top-level control over the phases.
fn ATController<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: Option<&AlterTableStmt<'mcx>>,
    rel: Relation<'mcx>,
    cmds: &PgVec<'mcx, NodePtr<'mcx>>,
    recurse: bool,
    lockmode: LOCKMODE,
    context: &AlterTableUtilityContext<'_>,
) -> PgResult<()> {
    let mut wqueue: PgVec<'mcx, AlteredTableInfo<'mcx>> = PgVec::new_in(mcx);

    // Phase 1: preliminary examination of commands, create work queue.
    for lcmd in cmds.iter() {
        let cmd = as_alter_table_cmd(lcmd);
        ATPrepCmd(
            mcx, &mut wqueue, &rel, cmd, recurse, false, lockmode, context,
        )?;
    }

    // Close the relation, but keep lock until commit.
    rel.close(NoLock)?;

    // Phase 2: update system catalogs.
    ATRewriteCatalogs(mcx, &mut wqueue, lockmode, context)?;

    // Phase 3: scan/rewrite tables as needed, and run afterStmts.
    ATRewriteTables(mcx, parsetree, &mut wqueue, lockmode, context)
}

// ===========================================================================
// ATPrepCmd (tablecmds.c:4904)
// ===========================================================================

/// `ATPrepCmd(...)` (tablecmds.c:4904) — phase-1 traffic cop: permission/relkind
/// checks, simple recursion, and pass assignment for each subcommand.
fn ATPrepCmd<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
    recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
    context: &AlterTableUtilityContext<'_>,
) -> PgResult<()> {
    // `recursing` is part of the faithful C signature; it is consumed only by
    // the per-arm prep helpers (ATPrepAddColumn / ATPrepDropColumn / ...) that
    // are still seam-and-panicked.
    let _ = recursing;

    // tab = ATGetQueueEntry(wqueue, rel);
    let tab_idx = ATGetQueueEntry(mcx, wqueue, rel)?;

    // Disallow non-FINALIZE ALTER on partitions pending detach.
    if rel.rd_rel.relispartition
        && cmd.subtype != AT_DetachPartitionFinalize
        && backend_catalog_pg_inherits::PartitionHasPendingDetach(rel.rd_id)?
    {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "cannot alter partition \"{}\" with an incomplete detach",
                rel.name()
            ))
            .errhint(
                "Use ALTER TABLE ... DETACH PARTITION ... FINALIZE to complete the pending detach operation.",
            )
            .finish(here("ATPrepCmd"));
    }

    // Copy the original subcommand for each table, so we can scribble on it.
    let cmd = cmd.clone_in(mcx)?;

    let pass: AlterTablePass;

    match cmd.subtype {
        AT_AddColumn => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_COMPOSITE_TYPE | ATT_FOREIGN_TABLE,
            )?;
            unported("ADD COLUMN (ATPrepAddColumn)");
        }
        AT_AddColumnToView => {
            ATSimplePermissions(cmd.subtype, rel, ATT_VIEW)?;
            unported("ADD COLUMN to view (ATPrepAddColumn)");
        }
        AT_ColumnDefault => {
            // ALTER COLUMN DEFAULT — defaults allowed on views too.
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_VIEW | ATT_FOREIGN_TABLE,
            )?;
            ATSimpleRecursion(mcx, wqueue, rel, &cmd, recurse, lockmode, context)?;
            pass = if cmd.def.is_some() {
                AT_PASS_ADD_OTHERCONSTR
            } else {
                AT_PASS_DROP
            };
        }
        AT_CookedColumnDefault => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
            )?;
            // This command never recurses.
            pass = AT_PASS_ADD_OTHERCONSTR;
        }
        AT_AddIdentity => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_VIEW | ATT_FOREIGN_TABLE,
            )?;
            unported("ALTER COLUMN ADD IDENTITY (phase-2 recursion + ATParseTransformCmd)");
        }
        AT_SetIdentity => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_VIEW | ATT_FOREIGN_TABLE,
            )?;
            unported("ALTER COLUMN SET IDENTITY (phase-2 recursion + ATParseTransformCmd)");
        }
        AT_DropIdentity => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_VIEW | ATT_FOREIGN_TABLE,
            )?;
            unported("ALTER COLUMN DROP IDENTITY");
        }
        AT_DropNotNull => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
            )?;
            unported("ALTER COLUMN DROP NOT NULL (pg_constraint-modeled in PG18)");
        }
        AT_SetNotNull => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
            )?;
            unported("ALTER COLUMN SET NOT NULL (pg_constraint-modeled in PG18)");
        }
        AT_SetExpression => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
            )?;
            ATSimpleRecursion(mcx, wqueue, rel, &cmd, recurse, lockmode, context)?;
            unported("ALTER COLUMN SET EXPRESSION (ATRewriteTable)");
        }
        AT_DropExpression => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
            )?;
            ATSimpleRecursion(mcx, wqueue, rel, &cmd, recurse, lockmode, context)?;
            unported("ALTER COLUMN DROP EXPRESSION (ATPrepDropExpression)");
        }
        AT_SetStatistics => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE
                    | ATT_PARTITIONED_TABLE
                    | ATT_MATVIEW
                    | ATT_INDEX
                    | ATT_PARTITIONED_INDEX
                    | ATT_FOREIGN_TABLE,
            )?;
            ATSimpleRecursion(mcx, wqueue, rel, &cmd, recurse, lockmode, context)?;
            pass = AT_PASS_MISC;
        }
        AT_SetOptions | AT_ResetOptions => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_MATVIEW | ATT_FOREIGN_TABLE,
            )?;
            // This command never recurses.
            pass = AT_PASS_MISC;
        }
        AT_SetStorage => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_MATVIEW | ATT_FOREIGN_TABLE,
            )?;
            ATSimpleRecursion(mcx, wqueue, rel, &cmd, recurse, lockmode, context)?;
            pass = AT_PASS_MISC;
        }
        AT_SetCompression => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_MATVIEW,
            )?;
            unported("ALTER COLUMN SET COMPRESSION");
        }
        AT_DropColumn => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_COMPOSITE_TYPE | ATT_FOREIGN_TABLE,
            )?;
            unported("DROP COLUMN (ATPrepDropColumn)");
        }
        AT_AddIndex => {
            ATSimplePermissions(cmd.subtype, rel, ATT_TABLE | ATT_PARTITIONED_TABLE)?;
            unported("ADD INDEX (from ADD CONSTRAINT)");
        }
        AT_AddConstraint => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
            )?;
            unported("ADD CONSTRAINT (ATPrepAddPrimaryKey)");
        }
        AT_AddIndexConstraint => {
            ATSimplePermissions(cmd.subtype, rel, ATT_TABLE | ATT_PARTITIONED_TABLE)?;
            unported("ADD CONSTRAINT USING INDEX");
        }
        AT_DropConstraint => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
            )?;
            unported("DROP CONSTRAINT (ATCheckPartitionsNotInUse)");
        }
        AT_AlterColumnType => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_COMPOSITE_TYPE | ATT_FOREIGN_TABLE,
            )?;
            unported("ALTER COLUMN TYPE (ATParseTransformCmd / ATPrepAlterColumnType)");
        }
        AT_AlterColumnGenericOptions => {
            ATSimplePermissions(cmd.subtype, rel, ATT_FOREIGN_TABLE)?;
            pass = AT_PASS_MISC;
        }
        AT_ChangeOwner => {
            // This command never recurses; no command-specific prep needed.
            pass = AT_PASS_MISC;
        }
        AT_ClusterOn | AT_DropCluster => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_MATVIEW,
            )?;
            // These commands never recurse; no command-specific prep needed.
            pass = AT_PASS_MISC;
        }
        AT_SetLogged | AT_SetUnLogged => {
            ATSimplePermissions(cmd.subtype, rel, ATT_TABLE | ATT_SEQUENCE)?;
            unported("SET LOGGED / SET UNLOGGED (ATPrepChangePersistence)");
        }
        AT_DropOids => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
            )?;
            unported("SET WITHOUT OIDS");
        }
        AT_SetAccessMethod => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_MATVIEW,
            )?;
            unported("SET ACCESS METHOD (ATPrepSetAccessMethod)");
        }
        AT_SetTableSpace => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE
                    | ATT_PARTITIONED_TABLE
                    | ATT_MATVIEW
                    | ATT_INDEX
                    | ATT_PARTITIONED_INDEX,
            )?;
            unported("SET TABLESPACE (ATPrepSetTableSpace)");
        }
        AT_SetRelOptions | AT_ResetRelOptions | AT_ReplaceRelOptions => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_VIEW | ATT_MATVIEW | ATT_INDEX,
            )?;
            // This command never recurses.
            pass = AT_PASS_MISC;
        }
        AT_AddInherit => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
            )?;
            unported("INHERIT (ATPrepAddInherit)");
        }
        AT_DropInherit => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
            )?;
            unported("NO INHERIT");
        }
        AT_AlterConstraint => {
            ATSimplePermissions(cmd.subtype, rel, ATT_TABLE | ATT_PARTITIONED_TABLE)?;
            unported("ALTER CONSTRAINT");
        }
        AT_ValidateConstraint => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
            )?;
            unported("VALIDATE CONSTRAINT");
        }
        AT_ReplicaIdentity => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_MATVIEW,
            )?;
            unported("REPLICA IDENTITY");
        }
        AT_EnableTrig | AT_EnableAlwaysTrig | AT_EnableReplicaTrig | AT_EnableTrigAll
        | AT_EnableTrigUser | AT_DisableTrig | AT_DisableTrigAll | AT_DisableTrigUser => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
            )?;
            unported("ENABLE/DISABLE TRIGGER variants");
        }
        AT_EnableRowSecurity | AT_DisableRowSecurity | AT_ForceRowSecurity
        | AT_NoForceRowSecurity => {
            // utility.c:5251 — ATSimplePermissions, never recurses, no
            // command-specific prep.
            ATSimplePermissions(cmd.subtype, rel, ATT_TABLE | ATT_PARTITIONED_TABLE)?;
            pass = AT_PASS_MISC;
        }
        AT_EnableRule | AT_EnableAlwaysRule | AT_EnableReplicaRule | AT_DisableRule | AT_AddOf
        | AT_DropOf => {
            ATSimplePermissions(cmd.subtype, rel, ATT_TABLE | ATT_PARTITIONED_TABLE)?;
            unported("ENABLE/DISABLE RULE / OF / NOT OF variants");
        }
        AT_GenericOptions => {
            ATSimplePermissions(cmd.subtype, rel, ATT_FOREIGN_TABLE)?;
            unported("OPTIONS (foreign table generic options)");
        }
        AT_AttachPartition => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_PARTITIONED_TABLE | ATT_PARTITIONED_INDEX,
            )?;
            unported("ATTACH PARTITION");
        }
        AT_DetachPartition => {
            ATSimplePermissions(cmd.subtype, rel, ATT_PARTITIONED_TABLE)?;
            unported("DETACH PARTITION");
        }
        AT_DetachPartitionFinalize => {
            ATSimplePermissions(cmd.subtype, rel, ATT_PARTITIONED_TABLE)?;
            unported("DETACH PARTITION FINALIZE");
        }
        _ => {
            return backend_utils_error::ereport(ERROR)
                .errmsg_internal(format!(
                    "unrecognized alter table type: {}",
                    cmd.subtype as i32
                ))
                .finish(here("ATPrepCmd"));
        }
    }

    debug_assert!(pass > AT_PASS_UNSET);

    // Add the subcommand to the appropriate list for phase 2.
    // tab->subcmds[pass] = lappend(tab->subcmds[pass], cmd);
    let node = mcx::alloc_in(mcx, Node::AlterTableCmd(cmd))?;
    wqueue[tab_idx].subcmds[pass as usize].push(node);

    Ok(())
}

// ===========================================================================
// ATRewriteCatalogs (tablecmds.c:5301)
// ===========================================================================

/// `ATRewriteCatalogs(...)` (tablecmds.c:5301) — phase-2 traffic cop: dispatch
/// subcommands in a safe execution order (pass by pass, table by table), then
/// add toast tables where required.
fn ATRewriteCatalogs<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    lockmode: LOCKMODE,
    context: &AlterTableUtilityContext<'_>,
) -> PgResult<()> {
    // Process all the tables "in parallel", one pass at a time.
    for pass in 0..AT_NUM_PASSES as i32 {
        for ti in 0..wqueue.len() {
            if wqueue[ti].subcmds[pass as usize].is_empty() {
                continue;
            }

            // Open the relation and store it in tab (lock already held).
            let relid = wqueue[ti].relid;
            let rel = relation_open(mcx, relid, NoLock)?;
            wqueue[ti].rel = Some(rel);

            // foreach subcmd: ATExecCmd(...). Deep-copy the subcommand out of the
            // work-queue entry before the call so `wqueue` can be passed `&mut`
            // (some unported families propagate work into later passes); the C
            // `ATExecCmd` likewise scribbles only on its own `cmd` copy.
            let n = wqueue[ti].subcmds[pass as usize].len();
            for si in 0..n {
                let cmd = match wqueue[ti].subcmds[pass as usize][si].as_altertablecmd() {
                    Some(c) => c.clone_in(mcx)?,
                    None => unreachable!("subcmds hold Node::AlterTableCmd"),
                };
                ATExecCmd(mcx, wqueue, ti, &cmd, lockmode, pass, context)?;
            }

            // After ALTER TYPE / SET EXPRESSION passes, do cleanup work.
            if pass == AT_PASS_ALTER_TYPE || pass == AT_PASS_SET_EXPRESSION {
                // ATPostAlterTypeCleanup — only reached if those families ran,
                // which they cannot yet (they seam-panic in ATExecCmd).
                unported("ATPostAlterTypeCleanup");
            }

            // Close the per-pass relation.
            if let Some(rel) = wqueue[ti].rel.take() {
                rel.close(NoLock)?;
            }
        }
    }

    // Check to see if a toast table must be added.
    for ti in 0..wqueue.len() {
        let relkind = wqueue[ti].relkind;
        let needs_toast_check = ((relkind == RELKIND_RELATION
            || relkind == RELKIND_PARTITIONED_TABLE)
            && wqueue[ti].partition_constraint.is_none())
            || relkind == RELKIND_MATVIEW;
        if needs_toast_check {
            // AlterTableCreateToastTable(tab->relid, (Datum) 0, lockmode);
            // (Datum) 0 == NULL reloptions == RelOptionsToken { is_null: true }.
            backend_catalog_toasting::AlterTableCreateToastTable(
                mcx,
                wqueue[ti].relid,
                types_cluster::RelOptionsToken {
                    is_null: true,
                    bytes: Vec::new(),
                },
                lockmode,
            )?;
        }
    }

    let _ = context;
    Ok(())
}

// ===========================================================================
// ATExecCmd (tablecmds.c:5375)
// ===========================================================================

/// `ATExecCmd(...)` (tablecmds.c:5375) — dispatch one subcommand to its
/// execution routine. Only the portable families execute; the rest faithfully
/// seam-and-panic with a precise rationale.
fn ATExecCmd<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    ti: usize,
    cmd: &AlterTableCmd<'mcx>,
    lockmode: LOCKMODE,
    cur_pass: AlterTablePass,
    context: &AlterTableUtilityContext<'_>,
) -> PgResult<()> {
    let _address: ObjectAddress;

    // rel = tab->rel;
    let rel = wqueue[ti]
        .rel
        .as_ref()
        .expect("ATExecCmd: tab->rel is open during phase 2");

    match cmd.subtype {
        AT_ColumnDefault => {
            // ATExecColumnDefault(rel, cmd->name, cmd->def, lockmode)
            let colname = cmd
                .name
                .as_ref()
                .map(|s| s.as_str())
                .expect("ALTER COLUMN DEFAULT requires a column name");
            _address =
                crate::at_column::ATExecColumnDefault(mcx, rel, colname, cmd.def.as_deref(), lockmode)?;
        }
        AT_CookedColumnDefault => {
            // ATExecCookedColumnDefault(rel, cmd->num, cmd->def)
            let def = cmd
                .def
                .as_ref()
                .expect("CookedColumnDefault requires a default expr");
            _address = crate::at_column::ATExecCookedColumnDefault(mcx, rel, cmd.num, def)?;
        }
        AT_SetStatistics => {
            let colname = cmd.name.as_ref().map(|s| s.as_str());
            _address = crate::at_column::ATExecSetStatistics(
                mcx,
                rel,
                colname,
                cmd.num,
                cmd.def.as_deref(),
                lockmode,
            )?;
        }
        AT_SetOptions => {
            let colname = cmd
                .name
                .as_ref()
                .map(|s| s.as_str())
                .expect("ALTER COLUMN SET OPTIONS requires a column name");
            _address =
                crate::at_column::ATExecSetOptions(mcx, rel, colname, cmd.def.as_deref(), false, lockmode)?;
        }
        AT_ResetOptions => {
            let colname = cmd
                .name
                .as_ref()
                .map(|s| s.as_str())
                .expect("ALTER COLUMN RESET OPTIONS requires a column name");
            _address =
                crate::at_column::ATExecSetOptions(mcx, rel, colname, cmd.def.as_deref(), true, lockmode)?;
        }
        AT_SetStorage => {
            let colname = cmd
                .name
                .as_ref()
                .map(|s| s.as_str())
                .expect("ALTER COLUMN SET STORAGE requires a column name");
            _address =
                crate::at_column::ATExecSetStorage(mcx, rel, colname, cmd.def.as_deref(), lockmode)?;
        }
        AT_SetRelOptions => {
            _address = crate::at_column::ATExecSetRelOptions(
                mcx,
                rel,
                cmd_def_elem_list(cmd),
                AT_SetRelOptions,
                lockmode,
            )?;
        }
        AT_ResetRelOptions => {
            _address = crate::at_column::ATExecSetRelOptions(
                mcx,
                rel,
                cmd_def_elem_list(cmd),
                AT_ResetRelOptions,
                lockmode,
            )?;
        }
        AT_ReplaceRelOptions => {
            _address = crate::at_column::ATExecSetRelOptions(
                mcx,
                rel,
                cmd_def_elem_list(cmd),
                AT_ReplaceRelOptions,
                lockmode,
            )?;
        }
        AT_ChangeOwner => {
            // ATExecChangeOwner(RelationGetRelid(rel),
            //     get_rolespec_oid(cmd->newowner, false), false, lockmode);
            let newowner = cmd
                .newowner
                .as_ref()
                .expect("ALTER OWNER requires a RoleSpec");
            let new_owner_id = match newowner.as_rolespec() {
                Some(rs) => {
                    // The Node enum carries `ddlnodes::RoleSpec`; the acl seam
                    // consumes the structurally-identical `parsenodes::RoleSpec`.
                    let role = types_nodes::parsenodes::RoleSpec {
                        roletype: rs.roletype,
                        rolename: match &rs.rolename {
                            Some(s) => Some(s.clone_in(mcx)?),
                            None => None,
                        },
                    };
                    backend_utils_adt_acl_seams::get_rolespec_oid::call(&role, false)?
                }
                None => unreachable!("AlterTableCmd.newowner must be a Node::RoleSpec"),
            };
            seam::at_exec_change_owner::call(rel.rd_id, new_owner_id, false, lockmode)?;
            // ATExecChangeOwner returns void; address stays Invalid.
            _address = ObjectAddress {
                classId: InvalidOid,
                objectId: InvalidOid,
                objectSubId: 0,
            };
        }

        // --- Unported executed families (faithful seam-and-panic) ---
        AT_AddColumn | AT_AddColumnToView => unported("ADD COLUMN (ATExecAddColumn)"),
        AT_AddIdentity => unported("ADD IDENTITY (ATExecAddIdentity)"),
        AT_SetIdentity => unported("SET IDENTITY (ATExecSetIdentity)"),
        AT_DropIdentity => unported("DROP IDENTITY (ATExecDropIdentity)"),
        AT_DropNotNull => unported("DROP NOT NULL (ATExecDropNotNull, pg_constraint-modeled)"),
        AT_SetNotNull => unported("SET NOT NULL (ATExecSetNotNull, pg_constraint-modeled)"),
        AT_SetExpression => unported("SET EXPRESSION (ATExecSetExpression + ATRewriteTable)"),
        AT_DropExpression => unported("DROP EXPRESSION (ATExecDropExpression)"),
        AT_SetCompression => unported("SET COMPRESSION (ATExecSetCompression)"),
        AT_DropColumn => unported("DROP COLUMN (ATExecDropColumn)"),
        AT_AddIndex | AT_ReAddIndex => unported("ADD INDEX (ATExecAddIndex)"),
        AT_ReAddStatistics => unported("ReAdd STATISTICS (ATExecAddStatistics)"),
        AT_AddConstraint | AT_ReAddConstraint => unported("ADD CONSTRAINT (ATExecAddConstraint)"),
        AT_ReAddDomainConstraint => unported("ReAdd DOMAIN CONSTRAINT (AlterDomainAddConstraint)"),
        AT_ReAddComment => unported("ReAdd COMMENT (CommentObject)"),
        AT_AddIndexConstraint => unported("ADD CONSTRAINT USING INDEX (ATExecAddIndexConstraint)"),
        AT_AlterConstraint => unported("ALTER CONSTRAINT (ATExecAlterConstraint)"),
        AT_ValidateConstraint => unported("VALIDATE CONSTRAINT (ATExecValidateConstraint)"),
        AT_DropConstraint => unported("DROP CONSTRAINT (ATExecDropConstraint)"),
        AT_AlterColumnType => unported("ALTER COLUMN TYPE (ATExecAlterColumnType + ATRewriteTable)"),
        AT_AlterColumnGenericOptions => {
            unported("ALTER COLUMN OPTIONS (ATExecAlterColumnGenericOptions)")
        }
        AT_ClusterOn => {
            // ATExecClusterOn(rel, cmd->name, lockmode)
            let indexname = cmd
                .name
                .as_ref()
                .map(|s| s.as_str())
                .expect("CLUSTER ON requires an index name");
            _address = crate::at_column::ATExecClusterOn(mcx, rel, indexname, lockmode)?;
        }
        AT_DropCluster => {
            // ATExecDropCluster(rel, lockmode)
            _address = crate::at_column::ATExecDropCluster(mcx, rel, lockmode)?;
        }
        AT_SetLogged | AT_SetUnLogged => unported("SET LOGGED/UNLOGGED (persistence change)"),
        AT_SetAccessMethod => unported("SET ACCESS METHOD"),
        AT_SetTableSpace => unported("SET TABLESPACE (ATExecSetTableSpace)"),
        AT_DropOids => unported("SET WITHOUT OIDS"),
        AT_AddInherit => unported("INHERIT (ATExecAddInherit)"),
        AT_DropInherit => unported("NO INHERIT (ATExecDropInherit)"),
        AT_AddOf => unported("OF (ATExecAddOf)"),
        AT_DropOf => unported("NOT OF (ATExecDropOf)"),
        AT_ReplicaIdentity => unported("REPLICA IDENTITY (ATExecReplicaIdentity)"),
        AT_EnableTrig | AT_EnableAlwaysTrig | AT_EnableReplicaTrig | AT_DisableTrig
        | AT_EnableTrigAll | AT_DisableTrigAll | AT_EnableTrigUser | AT_DisableTrigUser => {
            unported("ENABLE/DISABLE TRIGGER (EnableDisableTrigger)")
        }
        AT_EnableRule | AT_EnableAlwaysRule | AT_EnableReplicaRule | AT_DisableRule => {
            unported("ENABLE/DISABLE RULE (EnableDisableRule)")
        }
        AT_EnableRowSecurity => {
            // ATExecSetRowSecurity(rel, true)
            _address = crate::at_column::ATExecSetRowSecurity(rel, true)?;
        }
        AT_DisableRowSecurity => {
            // ATExecSetRowSecurity(rel, false)
            _address = crate::at_column::ATExecSetRowSecurity(rel, false)?;
        }
        AT_ForceRowSecurity => {
            // ATExecForceNoForceRowSecurity(rel, true)
            _address = crate::at_column::ATExecForceNoForceRowSecurity(rel, true)?;
        }
        AT_NoForceRowSecurity => {
            // ATExecForceNoForceRowSecurity(rel, false)
            _address = crate::at_column::ATExecForceNoForceRowSecurity(rel, false)?;
        }
        AT_GenericOptions => unported("OPTIONS (ATExecGenericOptions)"),
        AT_AttachPartition => unported("ATTACH PARTITION (ATExecAttachPartition)"),
        AT_DetachPartition => unported("DETACH PARTITION (ATExecDetachPartition)"),
        AT_DetachPartitionFinalize => {
            unported("DETACH PARTITION FINALIZE (ATExecDetachPartitionFinalize)")
        }
        // C: `default: elog(ERROR, "unexpected alter table type")`, unreachable
        // here because `AlterTableType` is exhaustively matched above.
    }

    let _ = (cur_pass, context);
    Ok(())
}

// ===========================================================================
// ATRewriteTables (tablecmds.c:5838) — phase 3
// ===========================================================================

/// `ATRewriteTables(...)` (tablecmds.c:5838) — phase 3: scan/rewrite tables as
/// needed, validate new constraints, run after-statements.
///
/// The portable families ported here (DEFAULT / STATISTICS / OPTIONS / STORAGE
/// / relOPTIONS / OWNER) never set `tab->rewrite` or `tab->verify_new_notnull`
/// and queue no `afterStmts`, so for them phase 3 is exactly the no-op fast
/// path: `CommandCounterIncrement()`. If any table did request a rewrite or a
/// NOT NULL recheck (only reachable once the heavy families land), we faithfully
/// seam-and-panic rather than silently skip the scan.
fn ATRewriteTables<'mcx>(
    mcx: Mcx<'mcx>,
    _parsetree: Option<&AlterTableStmt<'mcx>>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    _lockmode: LOCKMODE,
    _context: &AlterTableUtilityContext<'_>,
) -> PgResult<()> {
    // foreach: if (tab->rewrite > 0 || tab->verify_new_notnull) -> phase-3 scan
    for ti in 0..wqueue.len() {
        if wqueue[ti].rewrite > 0 || wqueue[ti].verify_new_notnull {
            unported("ATRewriteTable (phase-3 heap scan / NOT NULL revalidation)");
        }
        // Also: ATTACH PARTITION constraint validation lives here.
        if wqueue[ti].partition_constraint.is_some() {
            unported("ATRewriteTable (ATTACH PARTITION constraint validation)");
        }
    }

    // foreach table that doesn't need a rewrite: CommandCounterIncrement().
    for _ti in 0..wqueue.len() {
        CommandCounterIncrement()?;
    }

    let _ = mcx;
    Ok(())
}

// ===========================================================================
// ATGetQueueEntry (tablecmds.c:6561)
// ===========================================================================

/// `ATGetQueueEntry(...)` (tablecmds.c:6561) — find or create the work-queue
/// entry for `rel`, returning its index into `wqueue`.
fn ATGetQueueEntry<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    rel: &Relation<'mcx>,
) -> PgResult<usize> {
    let relid = rel.rd_id;

    for (i, tab) in wqueue.iter().enumerate() {
        if tab.relid == relid {
            return Ok(i);
        }
    }

    // Not there, so add it. Copy the relation's existing descriptor first.
    // CreateTupleDescCopyConstr(RelationGetDescr(rel)).
    let old_desc = CreateTupleDescCopyConstr(mcx, &rel.rd_att)?;

    let tab = AlteredTableInfo {
        relid,
        relkind: rel.rd_rel.relkind,
        oldDesc: old_desc,
        rel: None,
        subcmds: core::array::from_fn(|_| PgVec::new_in(mcx)),
        constraints: PgVec::new_in(mcx),
        newvals: PgVec::new_in(mcx),
        verify_new_notnull: false,
        rewrite: 0,
        newAccessMethod: InvalidOid,
        chgAccessMethod: false,
        newTableSpace: InvalidOid,
        chgPersistence: false,
        newrelpersistence: RELPERSISTENCE_PERMANENT,
        partition_constraint: None,
        validate_default: false,
        changedConstraintOids: PgVec::new_in(mcx),
        changedConstraintDefs: PgVec::new_in(mcx),
        changedIndexOids: PgVec::new_in(mcx),
        changedIndexDefs: PgVec::new_in(mcx),
        replicaIdentityIndex: None,
        clusterOnIndex: None,
        changedStatisticsOids: PgVec::new_in(mcx),
        changedStatisticsDefs: PgVec::new_in(mcx),
    };

    wqueue.push(tab);
    Ok(wqueue.len() - 1)
}

// ===========================================================================
// alter_table_type_to_string (tablecmds.c:6595)
// ===========================================================================

/// `alter_table_type_to_string(AlterTableType cmdtype)` (tablecmds.c:6595).
fn alter_table_type_to_string(cmdtype: AlterTableType) -> Option<&'static str> {
    match cmdtype {
        AT_AddColumn | AT_AddColumnToView => Some("ADD COLUMN"),
        AT_ColumnDefault | AT_CookedColumnDefault => Some("ALTER COLUMN ... SET DEFAULT"),
        AT_DropNotNull => Some("ALTER COLUMN ... DROP NOT NULL"),
        AT_SetNotNull => Some("ALTER COLUMN ... SET NOT NULL"),
        AT_SetExpression => Some("ALTER COLUMN ... SET EXPRESSION"),
        AT_DropExpression => Some("ALTER COLUMN ... DROP EXPRESSION"),
        AT_SetStatistics => Some("ALTER COLUMN ... SET STATISTICS"),
        AT_SetOptions => Some("ALTER COLUMN ... SET"),
        AT_ResetOptions => Some("ALTER COLUMN ... RESET"),
        AT_SetStorage => Some("ALTER COLUMN ... SET STORAGE"),
        AT_SetCompression => Some("ALTER COLUMN ... SET COMPRESSION"),
        AT_DropColumn => Some("DROP COLUMN"),
        AT_AddIndex | AT_ReAddIndex => None, /* not real grammar */
        AT_AddConstraint | AT_ReAddConstraint | AT_ReAddDomainConstraint | AT_AddIndexConstraint => {
            Some("ADD CONSTRAINT")
        }
        AT_AlterConstraint => Some("ALTER CONSTRAINT"),
        AT_ValidateConstraint => Some("VALIDATE CONSTRAINT"),
        AT_DropConstraint => Some("DROP CONSTRAINT"),
        AT_ReAddComment => None, /* not real grammar */
        AT_AlterColumnType => Some("ALTER COLUMN ... SET DATA TYPE"),
        AT_AlterColumnGenericOptions => Some("ALTER COLUMN ... OPTIONS"),
        AT_ChangeOwner => Some("OWNER TO"),
        AT_ClusterOn => Some("CLUSTER ON"),
        AT_DropCluster => Some("SET WITHOUT CLUSTER"),
        AT_SetAccessMethod => Some("SET ACCESS METHOD"),
        AT_SetLogged => Some("SET LOGGED"),
        AT_SetUnLogged => Some("SET UNLOGGED"),
        AT_DropOids => Some("SET WITHOUT OIDS"),
        AT_SetTableSpace => Some("SET TABLESPACE"),
        AT_SetRelOptions => Some("SET"),
        AT_ResetRelOptions => Some("RESET"),
        AT_ReplaceRelOptions => None, /* not real grammar */
        AT_EnableTrig => Some("ENABLE TRIGGER"),
        AT_EnableAlwaysTrig => Some("ENABLE ALWAYS TRIGGER"),
        AT_EnableReplicaTrig => Some("ENABLE REPLICA TRIGGER"),
        AT_DisableTrig => Some("DISABLE TRIGGER"),
        AT_EnableTrigAll => Some("ENABLE TRIGGER ALL"),
        AT_DisableTrigAll => Some("DISABLE TRIGGER ALL"),
        AT_EnableTrigUser => Some("ENABLE TRIGGER USER"),
        AT_DisableTrigUser => Some("DISABLE TRIGGER USER"),
        AT_EnableRule => Some("ENABLE RULE"),
        AT_EnableAlwaysRule => Some("ENABLE ALWAYS RULE"),
        AT_EnableReplicaRule => Some("ENABLE REPLICA RULE"),
        AT_DisableRule => Some("DISABLE RULE"),
        AT_AddInherit => Some("INHERIT"),
        AT_DropInherit => Some("NO INHERIT"),
        AT_AddOf => Some("OF"),
        AT_DropOf => Some("NOT OF"),
        AT_ReplicaIdentity => Some("REPLICA IDENTITY"),
        AT_EnableRowSecurity => Some("ENABLE ROW SECURITY"),
        AT_DisableRowSecurity => Some("DISABLE ROW SECURITY"),
        AT_ForceRowSecurity => Some("FORCE ROW SECURITY"),
        AT_NoForceRowSecurity => Some("NO FORCE ROW SECURITY"),
        AT_GenericOptions => Some("OPTIONS"),
        AT_AttachPartition => Some("ATTACH PARTITION"),
        AT_DetachPartition => Some("DETACH PARTITION"),
        AT_DetachPartitionFinalize => Some("DETACH PARTITION ... FINALIZE"),
        AT_AddIdentity => Some("ALTER COLUMN ... ADD IDENTITY"),
        AT_SetIdentity => Some("ALTER COLUMN ... SET"),
        AT_DropIdentity => Some("ALTER COLUMN ... DROP IDENTITY"),
        AT_ReAddStatistics => None, /* not real grammar */
    }
}

// ===========================================================================
// ATSimplePermissions (tablecmds.c:6738)
// ===========================================================================

/// `ATSimplePermissions(...)` (tablecmds.c:6738) — relkind targeting + owner +
/// system-catalog checks.
fn ATSimplePermissions(
    cmdtype: AlterTableType,
    rel: &Relation<'_>,
    allowed_targets: i32,
) -> PgResult<()> {
    let actual_target = match rel.rd_rel.relkind {
        RELKIND_RELATION => ATT_TABLE,
        RELKIND_PARTITIONED_TABLE => ATT_PARTITIONED_TABLE,
        RELKIND_VIEW => ATT_VIEW,
        RELKIND_MATVIEW => ATT_MATVIEW,
        RELKIND_INDEX => ATT_INDEX,
        RELKIND_PARTITIONED_INDEX => ATT_PARTITIONED_INDEX,
        RELKIND_COMPOSITE_TYPE => ATT_COMPOSITE_TYPE,
        RELKIND_FOREIGN_TABLE => ATT_FOREIGN_TABLE,
        RELKIND_SEQUENCE => ATT_SEQUENCE,
        _ => 0,
    };

    // Wrong target type?
    if (actual_target & allowed_targets) == 0 {
        match alter_table_type_to_string(cmdtype) {
            Some(action_str) => {
                return backend_utils_error::ereport(ERROR)
                    .errcode(types_error::ERRCODE_WRONG_OBJECT_TYPE)
                    .errmsg(format!(
                        "ALTER action {action_str} cannot be performed on relation \"{}\"",
                        rel.name()
                    ))
                    .errdetail(errdetail_relkind_not_supported(rel.rd_rel.relkind)?)
                    .finish(here("ATSimplePermissions"));
            }
            None => {
                return backend_utils_error::ereport(ERROR)
                    .errmsg_internal(format!(
                        "invalid ALTER action attempted on relation \"{}\"",
                        rel.name()
                    ))
                    .finish(here("ATSimplePermissions"));
            }
        }
    }

    // Permissions checks.
    if !aclchk_seam::object_ownercheck::call(RelationRelationId, rel.rd_id, GetUserId())? {
        let objtype = objaddr_seam::get_relkind_objtype::call(rel.rd_rel.relkind);
        aclchk_seam::aclcheck_error::call(
            types_acl::acl::ACLCHECK_NOT_OWNER,
            objtype,
            Some(rel.name().to_string()),
        )?;
    }

    // if (!allowSystemTableMods && IsSystemRelation(rel)) ...
    if !backend_commands_tablespace_globals_seams::allowSystemTableMods::call()?
        && IsSystemRelation(rel)
    {
        return backend_utils_error::ereport(ERROR)
            .errcode(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "permission denied: \"{}\" is a system catalog",
                rel.name()
            ))
            .finish(here("ATSimplePermissions"));
    }

    Ok(())
}

// ===========================================================================
// ATSimpleRecursion (tablecmds.c:6815)
// ===========================================================================

/// `ATSimpleRecursion(...)` (tablecmds.c:6815) — propagate the subcommand to all
/// direct and indirect children (each visited once).
fn ATSimpleRecursion<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
    recurse: bool,
    lockmode: LOCKMODE,
    context: &AlterTableUtilityContext<'_>,
) -> PgResult<()> {
    if recurse && rel.rd_rel.relhassubclass {
        let relid = rel.rd_id;

        // find_all_inheritors does the recursive search.
        let children = inherits_seam::find_all_inheritors::call(mcx, relid, lockmode)?;

        for &childrelid in children.iter() {
            if childrelid == relid {
                continue;
            }
            // find_all_inheritors already got lock.
            let childrel = relation_open(mcx, childrelid, NoLock)?;
            CheckAlterTableIsSafe(&childrel)?;
            ATPrepCmd(mcx, wqueue, &childrel, cmd, false, true, lockmode, context)?;
            childrel.close(NoLock)?;
        }
    }
    Ok(())
}
