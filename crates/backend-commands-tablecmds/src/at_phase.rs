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
use types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_error::{
    PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_TABLE_DEFINITION,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR,
};
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
    LOCKMODE, AccessExclusiveLock, AccessShareLock, NoLock, RowShareLock, ShareRowExclusiveLock,
    ShareUpdateExclusiveLock,
};
use types_tuple::access::{
    RangeVar as AccessRangeVar, RELKIND_COMPOSITE_TYPE, RELKIND_FOREIGN_TABLE, RELKIND_INDEX,
    RELKIND_MATVIEW, RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
    RELKIND_SEQUENCE, RELKIND_VIEW, RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP,
    RELPERSISTENCE_UNLOGGED,
};
use types_tuple::heaptuple::TupleDescData;

use backend_access_common_relation::relation_open;
use backend_access_common_tupdesc::CreateTupleDescCopyConstr;
use backend_access_transam_xact::CommandCounterIncrement;
use backend_catalog_aclchk_seams as aclchk_seam;
use backend_catalog_catalog::IsSystemRelation;
use backend_catalog_objectaccess_seams as objaccess_seam;
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
) -> PgResult<Vec<backend_access_common_reloptions::DefElem>> {
    let mut out = Vec::new();
    if let Some(def) = &cmd.def {
        if let Some(items) = def.as_list() {
            for it in items.iter() {
                if let Some(de) = it.as_defelem() {
                    // Project the value node (`def->arg`) to the `DefElemArg` the
                    // reloptions `defGetString`/`defGetBoolean` seams read; `None`
                    // mirrors `def->arg == NULL`. The full `defGetString` node
                    // switch (incl. T_TypeName/T_List bare-identifier values such
                    // as `check_option=local`) lives in `crate::create::defel_arg`;
                    // a prior `_ => AStar` catch-all here collapsed those to `"*"`.
                    let arg = crate::create::defel_arg(de)?;
                    out.push(backend_access_common_reloptions::DefElem::new(
                        de.defnamespace.as_ref().map(|s| s.as_str()),
                        de.defname.as_ref().map(|s| s.as_str()).unwrap_or(""),
                        arg,
                    ));
                }
            }
        }
    }
    Ok(out)
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

pub const AT_PASS_UNSET: AlterTablePass = -1; /* UNSET will cause ERROR */
pub const AT_PASS_DROP: AlterTablePass = 0; /* DROP (all flavors) */
pub const AT_PASS_ALTER_TYPE: AlterTablePass = 1; /* ALTER COLUMN TYPE */
pub const AT_PASS_ADD_COL: AlterTablePass = 2; /* ADD COLUMN */
pub const AT_PASS_SET_EXPRESSION: AlterTablePass = 3; /* ALTER SET EXPRESSION */
pub const AT_PASS_OLD_INDEX: AlterTablePass = 4; /* re-add existing indexes */
pub const AT_PASS_OLD_CONSTR: AlterTablePass = 5; /* re-add existing constraints */
/* We could support a RENAME COLUMN pass here, but not currently used */
pub const AT_PASS_ADD_CONSTR: AlterTablePass = 6; /* ADD constraints (initial examination) */
pub const AT_PASS_COL_ATTRS: AlterTablePass = 7; /* set column attributes, eg NOT NULL */
pub const AT_PASS_ADD_INDEXCONSTR: AlterTablePass = 8; /* ADD index-based constraints */
pub const AT_PASS_ADD_INDEX: AlterTablePass = 9; /* ADD indexes */
pub const AT_PASS_ADD_OTHERCONSTR: AlterTablePass = 10; /* ADD other constraints, defaults */
pub const AT_PASS_MISC: AlterTablePass = 11; /* other stuff */
pub const AT_NUM_PASSES: usize = 12;

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
    pub afterStmts: PgVec<'mcx, NodePtr<'mcx>>, /* List of utility command parsetrees */

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

/// `parser_errposition(pstate, location)` with `pstate->p_sourcetext = query_string`
/// (parse_node.c): byte offset → 1-based char position; 0 if no location/source.
fn parser_errposition_src(query_string: Option<&str>, location: i32) -> i32 {
    if location < 0 {
        return 0;
    }
    let sourcetext = match query_string {
        Some(s) => s,
        None => return 0,
    };
    let limit = (location as usize).min(sourcetext.len());
    sourcetext[..limit].chars().count() as i32 + 1
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
// ATCheckPartitionsNotInUse (tablecmds.c:6532)
// ===========================================================================

/// `ATCheckPartitionsNotInUse(rel, lockmode)` (tablecmds.c:6532) — for a
/// partitioned table, ensure none of its partitions are in use (a partition's
/// rows are addressed via the parent's FK triggers, etc.). For a plain table
/// this is a no-op.
pub(crate) fn ATCheckPartitionsNotInUse<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    if rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        // inh = find_all_inheritors(RelationGetRelid(rel), lockmode, NULL);
        let (inh, _) =
            backend_catalog_pg_inherits::find_all_inheritors(mcx, rel.rd_id, lockmode, false)?;
        // First element is the parent rel; must ignore it.
        for &childoid in inh.iter().skip(1) {
            // find_all_inheritors already got lock.
            let childrel = relation_open(mcx, childoid, NoLock)?;
            CheckAlterTableIsSafe(&childrel)?;
            childrel.close(NoLock)?;
        }
    }
    Ok(())
}

// ===========================================================================
// AlterTable / AlterTableInternal (tablecmds.c:4534 / 4563)
// ===========================================================================

/// `AlterTable(stmt, lockmode, context)` (tablecmds.c:4534) — execute ALTER
/// TABLE, a list of subcommands, in three phases. The caller must already hold
/// an adequate lock (`AlterTableGetLockLevel(stmt->cmds)` or higher).
pub fn AlterTable<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree: &Node<'mcx>,
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

    ATController(mcx, Some(parsetree), rel, &stmt.cmds, inh, lockmode, context)
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
pub(crate) fn RangeVarCallbackForAlterRelation<'mcx>(
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
                let def_list = cmd_def_elem_list(cmd)?;
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
    parsetree: Option<&Node<'mcx>>,
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
pub(crate) fn ATPrepCmd<'mcx>(
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
    let mut cmd = cmd.clone_in(mcx)?;

    let pass: AlterTablePass;

    match cmd.subtype {
        AT_AddColumn => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_COMPOSITE_TYPE | ATT_FOREIGN_TABLE,
            )?;
            crate::at_coladd::ATPrepAddColumn(mcx, rel, recurse, recursing, false, &mut cmd)?;
            // Recursion occurs during execution phase.
            pass = AT_PASS_ADD_COL;
        }
        AT_AddColumnToView => {
            ATSimplePermissions(cmd.subtype, rel, ATT_VIEW)?;
            crate::at_coladd::ATPrepAddColumn(mcx, rel, recurse, recursing, true, &mut cmd)?;
            pass = AT_PASS_ADD_COL;
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
            // Set up recursion for phase 2; no other prep needed.
            if recurse {
                cmd.recurse = true;
            }
            pass = AT_PASS_ADD_OTHERCONSTR;
        }
        AT_SetIdentity => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_VIEW | ATT_FOREIGN_TABLE,
            )?;
            // Set up recursion for phase 2; no other prep needed.
            if recurse {
                cmd.recurse = true;
            }
            // This should run after AddIdentity, so do it in MISC pass.
            pass = AT_PASS_MISC;
        }
        AT_DropIdentity => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_VIEW | ATT_FOREIGN_TABLE,
            )?;
            // Set up recursion for phase 2; no other prep needed.
            if recurse {
                cmd.recurse = true;
            }
            pass = AT_PASS_DROP;
        }
        AT_DropNotNull => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
            )?;
            // Set up recursion for phase 2; no other prep needed.
            if recurse {
                cmd.recurse = true;
            }
            pass = AT_PASS_DROP;
        }
        AT_SetNotNull => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
            )?;
            // Set up recursion for phase 2; no other prep needed.
            if recurse {
                cmd.recurse = true;
            }
            pass = AT_PASS_COL_ATTRS;
        }
        AT_SetExpression => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
            )?;
            ATSimpleRecursion(mcx, wqueue, rel, &cmd, recurse, lockmode, context)?;
            pass = AT_PASS_SET_EXPRESSION;
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
            // This command never recurses; no command-specific prep needed.
            pass = AT_PASS_MISC;
        }
        AT_DropColumn => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_COMPOSITE_TYPE | ATT_FOREIGN_TABLE,
            )?;
            crate::at_coldrop::ATPrepDropColumn(
                mcx, wqueue, rel, recurse, recursing, &mut cmd, lockmode, context,
            )?;
            pass = AT_PASS_DROP;
        }
        AT_AddIndex => {
            ATSimplePermissions(cmd.subtype, rel, ATT_TABLE | ATT_PARTITIONED_TABLE)?;
            // This command never recurses; no command-specific prep needed.
            pass = AT_PASS_ADD_INDEX;
        }
        AT_AddConstraint => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
            )?;
            crate::at_constraint::ATPrepAddPrimaryKey(
                mcx, wqueue, rel, &cmd, recurse, lockmode, context,
            )?;
            if recurse {
                // recurses at exec time; lock descendants and set flag.
                inherits_seam::find_all_inheritors::call(mcx, rel.rd_id, lockmode)?;
                cmd.recurse = true;
            }
            pass = AT_PASS_ADD_CONSTR;
        }
        AT_AddIndexConstraint => {
            ATSimplePermissions(cmd.subtype, rel, ATT_TABLE | ATT_PARTITIONED_TABLE)?;
            // This command never recurses; no command-specific prep needed.
            pass = AT_PASS_ADD_INDEXCONSTR;
        }
        AT_DropConstraint => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
            )?;
            ATCheckPartitionsNotInUse(mcx, rel, lockmode)?;
            // Other recursion occurs during execution phase.
            // No command-specific prep needed except saving recurse flag.
            if recurse {
                cmd.recurse = true;
            }
            pass = AT_PASS_DROP;
        }
        AT_AlterColumnType => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_COMPOSITE_TYPE | ATT_FOREIGN_TABLE,
            )?;
            // See comments for ATPrepAlterColumnType. Re-run parse analysis so the
            // USING clause (def->raw_default) is transformed into def->cooked_default
            // before ATPrepAlterColumnType consumes it.
            // cmd = ATParseTransformCmd(wqueue, tab, rel, cmd, recurse, lockmode,
            //   AT_PASS_UNSET, context); Assert(cmd != NULL);
            cmd = crate::at_coladd::ATParseTransformCmd(
                mcx, wqueue, tab_idx, rel, cmd, recurse, lockmode, AT_PASS_UNSET, context,
            )?
            .expect("ATParseTransformCmd returned NULL for AT_AlterColumnType");
            // Performs own recursion.
            // ATPrepAlterColumnType(wqueue, tab, rel, recurse, recursing, cmd,
            //   lockmode, context). Performs own recursion.
            crate::at_altertype::ATPrepAlterColumnType(
                mcx, wqueue, tab_idx, rel, recurse, recursing, &cmd, lockmode, context,
            )?;
            pass = AT_PASS_ALTER_TYPE;
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
            if wqueue[tab_idx].chgPersistence {
                return backend_utils_error::ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("cannot change persistence setting twice".to_string())
                    .finish(here("ATPrepCmd"));
            }
            ATPrepChangePersistence(
                mcx,
                wqueue,
                tab_idx,
                rel,
                cmd.subtype == AT_SetLogged,
            )?;
            pass = AT_PASS_MISC;
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
            // ATPrepAddInherit: if (child_rel->rd_rel->reloftype)
            //   ereport(ERROR, "cannot change inheritance of typed table").
            let reloftype =
                backend_utils_cache_syscache_seams::search_relation_reloftype::call(rel.rd_id)?
                    .unwrap_or(types_core::InvalidOid);
            if reloftype != types_core::InvalidOid {
                return backend_utils_error::ereport(ERROR)
                    .errcode(types_error::ERRCODE_WRONG_OBJECT_TYPE)
                    .errmsg("cannot change inheritance of typed table".to_string())
                    .finish(here("ATPrepCmd"));
            }
            // ATPrepAddInherit: if (child_rel->rd_rel->relispartition)
            //   ereport(ERROR, "cannot change inheritance of a partition").
            if rel.rd_rel.relispartition {
                return backend_utils_error::ereport(ERROR)
                    .errcode(types_error::ERRCODE_WRONG_OBJECT_TYPE)
                    .errmsg("cannot change inheritance of a partition".to_string())
                    .finish(here("ATPrepCmd"));
            }
            // if (child_rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
            //   ereport(ERROR, "cannot change inheritance of partitioned table").
            if rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
                return backend_utils_error::ereport(ERROR)
                    .errcode(types_error::ERRCODE_WRONG_OBJECT_TYPE)
                    .errmsg("cannot change inheritance of partitioned table".to_string())
                    .finish(here("ATPrepCmd"));
            }
            // This command never recurses.
            pass = AT_PASS_MISC;
        }
        AT_DropInherit => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
            )?;
            // This command never recurses; no command-specific prep needed.
            pass = AT_PASS_MISC;
        }
        AT_AlterConstraint => {
            ATSimplePermissions(cmd.subtype, rel, ATT_TABLE | ATT_PARTITIONED_TABLE)?;
            // Recursion occurs during execution phase.
            if recurse {
                cmd.recurse = true;
            }
            pass = AT_PASS_MISC;
        }
        AT_ValidateConstraint => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
            )?;
            // Recursion occurs during execution phase.
            // No command-specific prep needed except saving recurse flag.
            if recurse {
                cmd.recurse = true;
            }
            pass = AT_PASS_MISC;
        }
        AT_ReplicaIdentity => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_MATVIEW,
            )?;
            // This command never recurses; no command-specific prep needed.
            pass = AT_PASS_MISC;
        }
        AT_EnableTrig | AT_EnableAlwaysTrig | AT_EnableReplicaTrig | AT_EnableTrigAll
        | AT_EnableTrigUser | AT_DisableTrig | AT_DisableTrigAll | AT_DisableTrigUser => {
            ATSimplePermissions(
                cmd.subtype,
                rel,
                ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
            )?;
            // Set up recursion for phase 2; no other prep needed.
            if recurse {
                cmd.recurse = true;
            }
            pass = AT_PASS_MISC;
        }
        AT_EnableRowSecurity | AT_DisableRowSecurity | AT_ForceRowSecurity
        | AT_NoForceRowSecurity => {
            // utility.c:5251 — ATSimplePermissions, never recurses, no
            // command-specific prep.
            ATSimplePermissions(cmd.subtype, rel, ATT_TABLE | ATT_PARTITIONED_TABLE)?;
            pass = AT_PASS_MISC;
        }
        AT_EnableRule | AT_EnableAlwaysRule | AT_EnableReplicaRule | AT_DisableRule => {
            // tablecmds.c:5245 — ATSimplePermissions, never recurses, no
            // command-specific prep needed.
            ATSimplePermissions(cmd.subtype, rel, ATT_TABLE | ATT_PARTITIONED_TABLE)?;
            pass = AT_PASS_MISC;
        }
        AT_AddOf | AT_DropOf => {
            ATSimplePermissions(cmd.subtype, rel, ATT_TABLE | ATT_PARTITIONED_TABLE)?;
            unported("OF / NOT OF variants");
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
            // C: `cmd->recurse = false;` (never auto-recurses) and the catch-all
            // tail sets `pass = AT_PASS_MISC`. Execution happens in phase 2.
            pass = AT_PASS_MISC;
        }
        AT_DetachPartition => {
            ATSimplePermissions(cmd.subtype, rel, ATT_PARTITIONED_TABLE)?;
            // No command-specific prep needed; execution happens in phase 2.
            pass = AT_PASS_MISC;
        }
        AT_DetachPartitionFinalize => {
            ATSimplePermissions(cmd.subtype, rel, ATT_PARTITIONED_TABLE)?;
            // No command-specific prep needed; execution happens in phase 2.
            pass = AT_PASS_MISC;
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
    let node = mcx::alloc_in(mcx, Node::mk_alter_table_cmd(mcx, cmd)?)?;
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

            // After the ALTER TYPE / SET EXPRESSION pass, do cleanup work
            // (tablecmds.c:5343 `ATPostAlterTypeCleanup`). It re-parses and
            // rebuilds every dependent index/constraint/extended-statistics
            // object captured into `changed*Oids`/`changed*Defs` by
            // `RememberAllDependentForRebuilding`, drops them, and queues the
            // recreate commands plus a remembered REPLICA IDENTITY / CLUSTER
            // index restore into later passes. Done only once per table even if
            // multiple columns were altered.
            if pass == AT_PASS_ALTER_TYPE || pass == AT_PASS_SET_EXPRESSION {
                crate::at_altertype::ATPostAlterTypeCleanup(mcx, wqueue, ti, lockmode)?;
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
                cmd_def_elem_list(cmd)?,
                AT_SetRelOptions,
                lockmode,
            )?;
        }
        AT_ResetRelOptions => {
            _address = crate::at_column::ATExecSetRelOptions(
                mcx,
                rel,
                cmd_def_elem_list(cmd)?,
                AT_ResetRelOptions,
                lockmode,
            )?;
        }
        AT_ReplaceRelOptions => {
            _address = crate::at_column::ATExecSetRelOptions(
                mcx,
                rel,
                cmd_def_elem_list(cmd)?,
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
            crate::at_owner::ATExecChangeOwner(mcx, rel.rd_id, new_owner_id, false, lockmode)?;
            // ATExecChangeOwner returns void; address stays Invalid.
            _address = ObjectAddress {
                classId: InvalidOid,
                objectId: InvalidOid,
                objectSubId: 0,
            };
        }

        // --- Unported executed families (faithful seam-and-panic) ---
        AT_AddColumn | AT_AddColumnToView => {
            // ATExecAddColumn(wqueue, tab, rel, &cmd, cmd->recurse, false,
            //     lockmode, cur_pass, context). ATExecAddColumn needs &mut wqueue
            // (to append newvals / schedule transformed subcommands / recurse into
            // children), so we re-open `rel` by relid into an owned carrier rather
            // than borrowing it out of wqueue[ti].
            let relid = wqueue[ti].relid;
            let owned_rel = backend_access_common_relation::relation_open(mcx, relid, NoLock)?;
            let cmd_owned = cmd.clone_in(mcx)?;
            let recurse = cmd.recurse;
            _address = crate::at_coladd::ATExecAddColumn(
                mcx,
                wqueue,
                ti,
                &owned_rel,
                cmd_owned,
                recurse,
                false,
                lockmode,
                cur_pass,
                Some(context),
            )?;
            drop(owned_rel);
        }
        AT_AddIdentity => {
            // cmd = ATParseTransformCmd(wqueue, tab, rel, cmd, false, lockmode,
            //     cur_pass, context); Assert(cmd != NULL);
            // address = ATExecAddIdentity(rel, cmd->name, cmd->def, lockmode,
            //     cmd->recurse, false);
            // ATParseTransformCmd needs &mut wqueue, so re-open `rel` by relid
            // into an owned carrier rather than borrowing it out of wqueue[ti].
            let relid = wqueue[ti].relid;
            let owned_rel = backend_access_common_relation::relation_open(mcx, relid, NoLock)?;
            let cmd2 = crate::at_coladd::ATParseTransformCmd(
                mcx,
                wqueue,
                ti,
                &owned_rel,
                cmd.clone_in(mcx)?,
                false,
                lockmode,
                cur_pass,
                context,
            )?
            .expect("ATParseTransformCmd returned None for AT_AddIdentity");
            let colname = cmd2
                .name
                .as_ref()
                .map(|s| s.as_str())
                .expect("ADD IDENTITY requires a column name");
            let def = cmd2
                .def
                .as_deref()
                .expect("ADD IDENTITY requires a transformed ColumnDef");
            _address = crate::at_identity::ATExecAddIdentity(
                mcx,
                &owned_rel,
                colname,
                def,
                lockmode,
                cmd2.recurse,
                false,
            )?;
            drop(owned_rel);
        }
        AT_SetIdentity => {
            // cmd = ATParseTransformCmd(wqueue, tab, rel, cmd, false, lockmode,
            //     cur_pass, context); Assert(cmd != NULL);
            // address = ATExecSetIdentity(rel, cmd->name, cmd->def, lockmode,
            //     cmd->recurse, false);
            let relid = wqueue[ti].relid;
            let owned_rel = backend_access_common_relation::relation_open(mcx, relid, NoLock)?;
            let cmd2 = crate::at_coladd::ATParseTransformCmd(
                mcx,
                wqueue,
                ti,
                &owned_rel,
                cmd.clone_in(mcx)?,
                false,
                lockmode,
                cur_pass,
                context,
            )?
            .expect("ATParseTransformCmd returned None for AT_SetIdentity");
            let colname = cmd2
                .name
                .as_ref()
                .map(|s| s.as_str())
                .expect("SET IDENTITY requires a column name");
            let def = cmd2
                .def
                .as_deref()
                .expect("SET IDENTITY requires an options List");
            _address = crate::at_identity::ATExecSetIdentity(
                mcx,
                &owned_rel,
                colname,
                def,
                lockmode,
                cmd2.recurse,
                false,
            )?;
            drop(owned_rel);
        }
        AT_DropIdentity => {
            // ATExecDropIdentity(rel, cmd->name, cmd->missing_ok, lockmode,
            //     cmd->recurse, false). No parse-transform for DROP.
            let colname = cmd
                .name
                .as_ref()
                .map(|s| s.as_str())
                .expect("DROP IDENTITY requires a column name");
            _address = crate::at_identity::ATExecDropIdentity(
                mcx,
                rel,
                colname,
                cmd.missing_ok,
                lockmode,
                cmd.recurse,
                false,
            )?;
        }
        AT_DropNotNull => {
            // ATExecDropNotNull(rel, cmd->name, cmd->recurse, lockmode)
            let colname = cmd
                .name
                .as_ref()
                .map(|s| s.as_str())
                .expect("DROP NOT NULL requires a column name");
            _address = crate::at_dropvalidate::ATExecDropNotNull(
                mcx,
                rel,
                colname,
                cmd.recurse,
                lockmode,
            )?;
        }
        AT_SetNotNull => {
            // ATExecSetNotNull(wqueue, rel, NULL, cmd->name, cmd->recurse,
            //     false, lockmode). Needs &mut wqueue (queue phase-3 verify,
            // recurse into children) alongside &rel; take the single open rel
            // out of the queue entry (a second relation_open would bump
            // rd_refcnt and trip CheckTableNotInUse), then restore it.
            let colname = cmd
                .name
                .as_ref()
                .map(|s| s.as_str())
                .expect("SET NOT NULL requires a column name")
                .to_string();
            let recurse = cmd.recurse;
            let owned_rel = wqueue[ti].rel.take().expect("ATExecCmd: tab->rel is open");
            let res = crate::at_constraint::ATExecSetNotNull(
                mcx,
                wqueue,
                &owned_rel,
                None,
                &colname,
                recurse,
                false,
                lockmode,
            );
            wqueue[ti].rel = Some(owned_rel);
            _address = res?;
        }
        AT_SetExpression => {
            // ATExecSetExpression(tab, rel, cmd->name, cmd->def, lockmode).
            // ATExecSetExpression appends to tab->newvals and sets tab->rewrite,
            // so it needs &mut wqueue; re-open `rel` by relid into an owned
            // carrier rather than borrowing it out of wqueue[ti].
            let relid = wqueue[ti].relid;
            let owned_rel = backend_access_common_relation::relation_open(mcx, relid, NoLock)?;
            let colname = cmd
                .name
                .as_ref()
                .map(|s| s.as_str())
                .expect("ALTER COLUMN SET EXPRESSION requires a column name");
            let new_expr = cmd
                .def
                .as_deref()
                .expect("ALTER COLUMN SET EXPRESSION requires a new expression");
            let new_expr = new_expr.clone_in(mcx)?;
            _address = crate::at_altertype::ATExecSetExpression(
                mcx,
                wqueue,
                ti,
                &owned_rel,
                colname,
                &new_expr,
                lockmode,
            )?;
            drop(owned_rel);
        }
        AT_DropExpression => unported("DROP EXPRESSION (ATExecDropExpression)"),
        AT_SetCompression => {
            let colname = cmd
                .name
                .as_ref()
                .map(|s| s.as_str())
                .expect("ALTER COLUMN SET COMPRESSION requires a column name");
            _address = crate::at_column::ATExecSetCompression(
                mcx,
                rel,
                colname,
                cmd.def.as_deref(),
                lockmode,
            )?;
        }
        AT_DropColumn => {
            // ATExecDropColumn(wqueue, rel, cmd->name, cmd->behavior,
            //     cmd->recurse, false, cmd->missing_ok, lockmode, NULL)
            let colname = cmd
                .name
                .as_ref()
                .map(|s| s.as_str())
                .expect("DROP COLUMN requires a column name");
            _address = crate::at_coldrop::ATExecDropColumn(
                mcx,
                rel,
                colname,
                cmd.behavior,
                cmd.recurse,
                false,
                cmd.missing_ok,
            )?;
        }
        AT_AddIndex | AT_ReAddIndex => {
            // ATExecAddIndex(tab, rel, (IndexStmt *) cmd->def, is_rebuild, lockmode).
            // C uses the single already-open `tab->rel`; take it out of the queue
            // entry so we can pass `&mut wqueue[ti]` alongside `&rel` without a
            // second relation_open (which would bump rd_refcnt and trip
            // CheckTableNotInUse).
            let is_rebuild = cmd.subtype == AT_ReAddIndex;
            let owned_rel = wqueue[ti].rel.take().expect("ATExecCmd: tab->rel is open");
            let stmt = cmd
                .def
                .as_deref()
                .expect("AT_AddIndex: cmd.def is NULL")
                .as_indexstmt()
                .expect("AT_AddIndex: cmd.def is not an IndexStmt");
            let res = crate::at_constraint::ATExecAddIndex(
                mcx,
                &mut wqueue[ti],
                &owned_rel,
                stmt,
                is_rebuild,
                lockmode,
            );
            wqueue[ti].rel = Some(owned_rel);
            _address = res?;
        }
        AT_ReAddStatistics => unported("ReAdd STATISTICS (ATExecAddStatistics)"),
        AT_AddConstraint | AT_ReAddConstraint => {
            // Transform the command only during initial examination
            // (AT_PASS_ADD_CONSTR). Take the single open `tab->rel` out of the
            // queue entry (see AT_AddIndex above) so we can pass `&mut wqueue`.
            let owned_rel = wqueue[ti].rel.take().expect("ATExecCmd: tab->rel is open");
            let is_readd = cmd.subtype == AT_ReAddConstraint;

            let exec = (|| -> PgResult<ObjectAddress> {
                let cmd_for_exec: Option<AlterTableCmd<'mcx>> =
                    if !is_readd && cur_pass == AT_PASS_ADD_CONSTR {
                        crate::at_coladd::ATParseTransformCmd(
                            mcx,
                            wqueue,
                            ti,
                            &owned_rel,
                            cmd.clone_in(mcx)?,
                            cmd.recurse,
                            lockmode,
                            cur_pass,
                            context,
                        )?
                    } else {
                        Some(cmd.clone_in(mcx)?)
                    };

                // Depending on constraint type, there might be no more work now.
                if let Some(c) = cmd_for_exec {
                    let newcon = c
                        .def
                        .as_deref()
                        .expect("AT_AddConstraint: cmd.def is NULL")
                        .expect_constraint();
                    crate::at_constraint::ATExecAddConstraint(
                        mcx,
                        wqueue,
                        ti,
                        &owned_rel,
                        newcon,
                        c.recurse,
                        is_readd,
                        lockmode,
                    )
                } else {
                    Ok(ObjectAddress {
                        classId: InvalidOid,
                        objectId: InvalidOid,
                        objectSubId: 0,
                    })
                }
            })();
            wqueue[ti].rel = Some(owned_rel);
            _address = exec?;
        }
        AT_ReAddDomainConstraint => unported("ReAdd DOMAIN CONSTRAINT (AlterDomainAddConstraint)"),
        AT_ReAddComment => unported("ReAdd COMMENT (CommentObject)"),
        AT_AddIndexConstraint => {
            // ATExecAddIndexConstraint(tab, rel, (IndexStmt *) cmd->def, lockmode).
            let owned_rel = wqueue[ti].rel.take().expect("ATExecCmd: tab->rel is open");
            let stmt = cmd
                .def
                .as_deref()
                .expect("AT_AddIndexConstraint: cmd.def is NULL")
                .as_indexstmt()
                .expect("AT_AddIndexConstraint: cmd.def is not an IndexStmt");
            let res = crate::at_constraint::ATExecAddIndexConstraint(
                mcx,
                &mut wqueue[ti],
                &owned_rel,
                stmt,
                lockmode,
            );
            wqueue[ti].rel = Some(owned_rel);
            _address = res?;
        }
        AT_AlterConstraint => {
            // ATExecAlterConstraint(wqueue, rel, castNode(ATAlterConstraint, cmd->def),
            //     cmd->recurse, lockmode).
            let owned_rel = wqueue[ti].rel.take().expect("ATExecCmd: tab->rel is open");
            let cmdcon = cmd
                .def
                .as_deref()
                .expect("AT_AlterConstraint: cmd.def is NULL")
                .as_atalterconstraint()
                .expect("AT_AlterConstraint: cmd.def is not an ATAlterConstraint");
            let res = crate::at_alter_constr::ATExecAlterConstraint(
                mcx,
                wqueue,
                &owned_rel,
                cmdcon,
                cmd.recurse,
                lockmode,
            );
            wqueue[ti].rel = Some(owned_rel);
            _address = res?;
        }
        AT_ValidateConstraint => {
            // ATExecValidateConstraint(wqueue, rel, cmd->name, cmd->recurse, false, lockmode).
            let owned_rel = wqueue[ti].rel.take().expect("ATExecCmd: tab->rel is open");
            let constr_name = cmd
                .name
                .as_ref()
                .map(|s| s.as_str())
                .expect("VALIDATE CONSTRAINT requires a constraint name");
            let res = crate::at_dropvalidate::ATExecValidateConstraint(
                mcx,
                wqueue,
                &owned_rel,
                constr_name,
                cmd.recurse,
                false,
                lockmode,
            );
            wqueue[ti].rel = Some(owned_rel);
            _address = res?;
        }
        AT_DropConstraint => {
            // ATExecDropConstraint(rel, cmd->name, cmd->behavior, cmd->recurse,
            //     cmd->missing_ok, lockmode).
            let owned_rel = wqueue[ti].rel.take().expect("ATExecCmd: tab->rel is open");
            let constr_name = cmd
                .name
                .as_ref()
                .map(|s| s.as_str())
                .expect("DROP CONSTRAINT requires a constraint name");
            let res = crate::at_dropvalidate::ATExecDropConstraint(
                mcx,
                &owned_rel,
                constr_name,
                cmd.behavior,
                cmd.recurse,
                cmd.missing_ok,
                lockmode,
            );
            wqueue[ti].rel = Some(owned_rel);
            res?;
            _address = ObjectAddress {
                classId: InvalidOid,
                objectId: InvalidOid,
                objectSubId: 0,
            };
        }
        AT_AlterColumnType => {
            // ATExecAlterColumnType(tab, rel, cmd, lockmode). Take the single
            // open `tab->rel` out of the queue entry (see AT_AddIndex above) so
            // we can pass `&mut wqueue` alongside `&rel`.
            let owned_rel = wqueue[ti].rel.take().expect("ATExecCmd: tab->rel is open");
            let res = crate::at_altertype::ATExecAlterColumnType(
                mcx, wqueue, ti, &owned_rel, &cmd, lockmode,
            );
            wqueue[ti].rel = Some(owned_rel);
            _address = res?;
        }
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
        AT_SetLogged | AT_SetUnLogged => {
            // C: `break` — a no-op. The actual persistence change is driven by
            // tab->chgPersistence / tab->newrelpersistence in ATRewriteTables
            // (the make_new_heap + finish_heap_swap rewrite, set up by
            // ATPrepChangePersistence in phase 1).
        }
        AT_SetAccessMethod => unported("SET ACCESS METHOD"),
        AT_SetTableSpace => unported("SET TABLESPACE (ATExecSetTableSpace)"),
        AT_DropOids => unported("SET WITHOUT OIDS"),
        AT_AddInherit => {
            // ATExecAddInherit(rel, (RangeVar *) cmd->def, lockmode).
            let def = cmd
                .def
                .as_deref()
                .expect("AT_AddInherit: cmd.def is NULL");
            let parent = def
                .as_rangevar()
                .expect("AT_AddInherit: cmd.def is not a RangeVar");
            _address = crate::at_attach::ATExecAddInherit(mcx, rel, parent, lockmode)?;
        }
        AT_DropInherit => {
            // ATExecDropInherit(rel, (RangeVar *) cmd->def, lockmode).
            let def = cmd
                .def
                .as_deref()
                .expect("AT_DropInherit: cmd.def is NULL");
            let parent = def
                .as_rangevar()
                .expect("AT_DropInherit: cmd.def is not a RangeVar");
            _address = crate::at_detach::ATExecDropInherit(mcx, rel, parent, lockmode)?;
        }
        AT_AddOf => unported("OF (ATExecAddOf)"),
        AT_DropOf => unported("NOT OF (ATExecDropOf)"),
        AT_ReplicaIdentity => {
            // ATExecReplicaIdentity(rel, (ReplicaIdentityStmt *) cmd->def, lockmode).
            let stmt = cmd
                .def
                .as_deref()
                .expect("AT_ReplicaIdentity: cmd.def is NULL")
                .as_replicaidentitystmt()
                .expect("AT_ReplicaIdentity: cmd.def is not a ReplicaIdentityStmt");
            _address = crate::at_column::ATExecReplicaIdentity(mcx, rel, stmt, lockmode)?;
        }
        AT_EnableTrig | AT_EnableAlwaysTrig | AT_EnableReplicaTrig | AT_DisableTrig
        | AT_EnableTrigAll | AT_DisableTrigAll | AT_EnableTrigUser | AT_DisableTrigUser => {
            // ATExecEnableDisableTrigger(rel, cmd->name, fires_when,
            //                            skip_system, cmd->recurse, lockmode)
            // (tablecmds.c:5558-5602). The fires_when char + skip_system flag +
            // trigger-name-vs-NULL depend on the subcommand variant.
            use types_catalog::pg_trigger::{
                TRIGGER_DISABLED, TRIGGER_FIRES_ALWAYS, TRIGGER_FIRES_ON_ORIGIN,
                TRIGGER_FIRES_ON_REPLICA,
            };
            // (trigname_is_used, fires_when, skip_system)
            let (use_name, fires_when, skip_system) = match cmd.subtype {
                AT_EnableTrig => (true, TRIGGER_FIRES_ON_ORIGIN, false),
                AT_EnableAlwaysTrig => (true, TRIGGER_FIRES_ALWAYS, false),
                AT_EnableReplicaTrig => (true, TRIGGER_FIRES_ON_REPLICA, false),
                AT_DisableTrig => (true, TRIGGER_DISABLED, false),
                AT_EnableTrigAll => (false, TRIGGER_FIRES_ON_ORIGIN, false),
                AT_DisableTrigAll => (false, TRIGGER_DISABLED, false),
                AT_EnableTrigUser => (false, TRIGGER_FIRES_ON_ORIGIN, true),
                AT_DisableTrigUser => (false, TRIGGER_DISABLED, true),
                _ => unreachable!(),
            };
            let trigname = if use_name {
                Some(
                    cmd.name
                        .as_ref()
                        .map(|s| s.as_str())
                        .expect("ENABLE/DISABLE TRIGGER name requires a trigger name"),
                )
            } else {
                None
            };
            backend_commands_trigger::enable_disable::ATExecEnableDisableTrigger(
                mcx,
                rel,
                trigname,
                fires_when,
                skip_system,
                cmd.recurse,
                lockmode,
            )?;
        }
        AT_EnableRule | AT_EnableAlwaysRule | AT_EnableReplicaRule | AT_DisableRule => {
            // ATExecEnableDisableRule(rel, cmd->name, fires_when, lockmode)
            // (tablecmds.c:5607-5623). The fires_when char depends on the
            // subcommand variant.
            let fires_when = match cmd.subtype {
                AT_EnableRule => types_catalog::pg_rewrite::RULE_FIRES_ON_ORIGIN,
                AT_EnableAlwaysRule => types_catalog::pg_rewrite::RULE_FIRES_ALWAYS,
                AT_EnableReplicaRule => types_catalog::pg_rewrite::RULE_FIRES_ON_REPLICA,
                AT_DisableRule => types_catalog::pg_rewrite::RULE_DISABLED,
                _ => unreachable!(),
            };
            let rulename = cmd
                .name
                .as_ref()
                .map(|s| s.as_str())
                .expect("ENABLE/DISABLE RULE requires a rule name");
            // EnableDisableRule(rel, rulename, fires_when);
            backend_rewrite_rewriteDefine::EnableDisableRule(mcx, rel, rulename, fires_when)?;
            // InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), 0);
            objaccess_seam::invoke_object_post_alter_hook::call(
                RelationRelationId,
                rel.rd_id,
                0,
            )?;
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
        AT_AttachPartition => {
            // cmd = ATParseTransformCmd(wqueue, tab, rel, cmd, false, ...): transform
            // the FOR VALUES bound (raw A_Const → PartitionRangeDatum/Const) before
            // execution, exactly as C's ATExecCmd does for this subcommand.
            let owned_rel = wqueue[ti].rel.take().expect("ATExecCmd: tab->rel is open");
            let res = (|| {
                let transformed = crate::at_coladd::ATParseTransformCmd(
                    mcx,
                    wqueue,
                    ti,
                    &owned_rel,
                    cmd.clone_in(mcx)?,
                    false,
                    lockmode,
                    cur_pass,
                    context,
                )?
                .expect("ATParseTransformCmd returned None for ATTACH PARTITION");
                let pc = transformed
                    .def
                    .as_deref()
                    .and_then(|d| d.as_partitioncmd())
                    .expect("AT_AttachPartition: transformed cmd.def is not a PartitionCmd");
                // C: rd_rel->relkind == RELKIND_PARTITIONED_TABLE ⇒ ATExecAttachPartition;
                // RELKIND_PARTITIONED_INDEX ⇒ ATExecAttachPartitionIdx (unported).
                if owned_rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
                    crate::at_attach::ATExecAttachPartition(mcx, wqueue, &owned_rel, pc)
                } else {
                    crate::at_attach_idx::ATExecAttachPartitionIdx(mcx, &owned_rel, pc)
                }
            })();
            wqueue[ti].rel = Some(owned_rel);
            _address = res?;
        }
        AT_DetachPartition => {
            // cmd = ATParseTransformCmd(...): transform the bound (a no-op for
            // DETACH, which carries no FOR VALUES bound) before execution, exactly
            // as C's ATExecCmd does. ATPrepCmd ensures rel is a partitioned table.
            let owned_rel = wqueue[ti].rel.take().expect("ATExecCmd: tab->rel is open");
            let res = (|| {
                let transformed = crate::at_coladd::ATParseTransformCmd(
                    mcx,
                    wqueue,
                    ti,
                    &owned_rel,
                    cmd.clone_in(mcx)?,
                    false,
                    lockmode,
                    cur_pass,
                    context,
                )?
                .expect("ATParseTransformCmd returned None for DETACH PARTITION");
                let pc = transformed
                    .def
                    .as_deref()
                    .and_then(|d| d.as_partitioncmd())
                    .expect("AT_DetachPartition: transformed cmd.def is not a PartitionCmd");
                crate::at_detach::ATExecDetachPartition(mcx, wqueue, &owned_rel, pc)
            })();
            wqueue[ti].rel = Some(owned_rel);
            _address = res?;
        }
        AT_DetachPartitionFinalize => {
            let owned_rel = wqueue[ti].rel.take().expect("ATExecCmd: tab->rel is open");
            let res = (|| {
                let pc = cmd
                    .def
                    .as_deref()
                    .and_then(|d| d.as_partitioncmd())
                    .expect("AT_DetachPartitionFinalize: cmd.def is not a PartitionCmd");
                crate::at_detach::ATExecDetachPartitionFinalize(mcx, &owned_rel, pc)
            })();
            wqueue[ti].rel = Some(owned_rel);
            _address = res?;
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
    parsetree: Option<&Node<'mcx>>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    _lockmode: LOCKMODE,
    _context: &AlterTableUtilityContext<'_>,
) -> PgResult<()> {
    // foreach: if (tab->rewrite > 0 || tab->verify_new_notnull) -> phase-3 scan
    for ti in 0..wqueue.len() {
        // Relations without storage may be ignored here (tablecmds.c:5849).
        if !backend_catalog_heap::RELKIND_HAS_STORAGE(wqueue[ti].relkind) {
            continue;
        }

        // If we change column data types (or add a column with a default), the
        // operation has to be propagated to tables that use this table's rowtype
        // as a column type (tablecmds.c:5865). `tab->newvals != NIL ||
        // tab->rewrite > 0` → re-open and run `find_composite_type_dependencies`
        // over the relation's composite rowtype. This is the guard that rejects
        // e.g. `ALTER COLUMN ... TYPE` on a table whose rowtype is stored in
        // another table's column.
        if !wqueue[ti].newvals.is_empty() || wqueue[ti].rewrite > 0 {
            let relid = wqueue[ti].relid;
            let rel = backend_access_common_relation::relation_open(mcx, relid, NoLock)?;
            crate::at_altertype::find_composite_type_dependencies(
                mcx,
                rel.rd_rel.reltype,
                Some(&rel),
                None,
            )?;
            rel.close(NoLock)?;
        }

        // We only need to rewrite the table if at least one column needs to be
        // recomputed, or we are changing its persistence or access method
        // (tablecmds.c:5883). `tab->rewrite > 0 && relkind != RELKIND_SEQUENCE`.
        if wqueue[ti].rewrite > 0 && wqueue[ti].relkind != RELKIND_SEQUENCE {
            // Build a temporary relation and copy data.
            let relid = wqueue[ti].relid;
            let old_heap = relation_open(mcx, relid, NoLock)?;

            // We don't support rewriting of system catalogs.
            if IsSystemRelation(&old_heap) {
                let name = old_heap.name().to_string();
                old_heap.close(NoLock)?;
                return backend_utils_error::ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg(format!("cannot rewrite system relation \"{name}\""))
                    .finish(here("ATRewriteTables"));
            }
            // RelationIsUsedAsCatalogTable(rel): rd_options->user_catalog_table
            // (only meaningful for RELATION / MATVIEW).
            let used_as_catalog = matches!(
                old_heap.rd_rel.relkind,
                RELKIND_RELATION | RELKIND_MATVIEW
            ) && old_heap
                .rd_options
                .as_ref()
                .and_then(|o| o.std())
                .is_some_and(|o| o.user_catalog_table);
            if used_as_catalog {
                let name = old_heap.name().to_string();
                old_heap.close(NoLock)?;
                return backend_utils_error::ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg(format!(
                        "cannot rewrite table \"{name}\" used as a catalog table"
                    ))
                    .finish(here("ATRewriteTables"));
            }
            // Don't allow rewrite on temp tables of other backends.
            if crate::smallfns::relation_is_other_temp(&old_heap)? {
                old_heap.close(NoLock)?;
                return backend_utils_error::ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("cannot rewrite temporary tables of other sessions".to_string())
                    .finish(here("ATRewriteTables"));
            }

            // Select destination tablespace / access method / persistence (same
            // as original unless the user requested a change).
            let new_table_space = if OidIsValid(wqueue[ti].newTableSpace) {
                wqueue[ti].newTableSpace
            } else {
                old_heap.rd_rel.reltablespace
            };
            let new_access_method = if wqueue[ti].chgAccessMethod {
                wqueue[ti].newAccessMethod
            } else {
                old_heap.rd_rel.relam
            };
            let persistence = if wqueue[ti].chgPersistence {
                wqueue[ti].newrelpersistence
            } else {
                old_heap.rd_rel.relpersistence
            };

            old_heap.close(NoLock)?;

            // Fire the table_rewrite Event Trigger now, before rewriting (only
            // once, and only when parsetree is non-NULL — not from
            // AlterTableInternal). A no-op without active event-trigger state.
            if parsetree.is_some() {
                backend_commands_event_trigger_seams::event_trigger_table_rewrite::call(
                    parsetree,
                    relid,
                    wqueue[ti].rewrite,
                )?;
            }

            // Create the transient table that will receive the modified data.
            let oid_new_heap = backend_commands_cluster_seams::make_new_heap::call(
                mcx,
                relid,
                new_table_space,
                new_access_method,
                persistence,
                _lockmode,
            )?;

            // Copy the heap data into the new table with the desired
            // modifications, testing the current data against new constraints.
            run_at_rewrite_table_scan(mcx, &wqueue[ti], oid_new_heap)?;

            // Swap the physical files, rebuild indexes, discard the old heap.
            // We use RecentXmin for the new relfrozenxid (all tuples rewritten).
            let frozen_xid = backend_utils_time_snapmgr_pc_seams::recent_xmin::call();
            let cutoff_multi =
                backend_access_transam_multixact_seams::read_next_multixact_id::call()?;
            backend_commands_cluster_seams::finish_heap_swap::call(
                mcx,
                relid,
                oid_new_heap,
                false,
                false,
                true,
                !OidIsValid(wqueue[ti].newTableSpace),
                frozen_xid,
                cutoff_multi,
                persistence,
            )?;

            // InvokeObjectPostAlterHook(RelationRelationId, tab->relid, 0).
            backend_catalog_objectaccess_seams::invoke_object_post_alter_hook::call(
                RelationRelationId,
                relid,
                0,
            )?;
        } else if wqueue[ti].rewrite > 0 && wqueue[ti].relkind == RELKIND_SEQUENCE {
            // SequenceChangePersistence on rewrite of a sequence (persistence
            // change). tablecmds.c:6009-6013.
            if wqueue[ti].chgPersistence {
                backend_commands_sequence_seams::sequence_change_persistence::call(
                    mcx,
                    wqueue[ti].relid,
                    wqueue[ti].newrelpersistence,
                )?;
            }
        } else {
            // If required, test the current data against new constraints, but
            // don't rebuild data. C: if (tab->constraints != NIL ||
            // tab->verify_new_notnull || tab->partition_constraint != NULL)
            // ATRewriteTable(tab, InvalidOid).
            if !wqueue[ti].constraints.is_empty()
                || wqueue[ti].verify_new_notnull
                || wqueue[ti].partition_constraint.is_some()
            {
                run_at_rewrite_table_scan(mcx, &wqueue[ti], InvalidOid)?;
            }

            // SET TABLESPACE with no reason to reconstruct tuples → block copy.
            if OidIsValid(wqueue[ti].newTableSpace) {
                unported("ATRewriteTables: ATExecSetTableSpace (block-by-block copy)");
            }
        }

        // Also change persistence of owned sequences, so that it matches the
        // table persistence (tablecmds.c:6033-6048).
        if wqueue[ti].chgPersistence {
            let relid = wqueue[ti].relid;
            let newpersistence = wqueue[ti].newrelpersistence;
            let seqlist =
                backend_catalog_pg_depend_seams::getOwnedSequences::call(mcx, relid)?;
            for &seq_relid in seqlist.iter() {
                backend_commands_sequence_seams::sequence_change_persistence::call(
                    mcx,
                    seq_relid,
                    newpersistence,
                )?;
            }
        }
    }

    // Foreign-key constraints are checked in a final pass, since (a) it's
    // generally best to examine each one separately, and (b) it's at least
    // theoretically possible that we have changed both relations of the foreign
    // key, and we'd better have finished both rewrites before we try to read the
    // tables.
    for ti in 0..wqueue.len() {
        // Relations without storage may be ignored here too.
        if !backend_catalog_heap::RELKIND_HAS_STORAGE(wqueue[ti].relkind) {
            continue;
        }

        // rel = NULL: opened lazily on the first FK constraint, kept across the
        // inner loop (C `if (rel == NULL) rel = table_open(tab->relid, NoLock)`).
        let mut rel: Option<Relation<'mcx>> = None;

        for ci in 0..wqueue[ti].constraints.len() {
            if wqueue[ti].constraints[ci].contype
                != types_nodes::ddlnodes::ConstrType::CONSTR_FOREIGN as i32
            {
                continue;
            }

            let conname = wqueue[ti].constraints[ci]
                .name
                .as_ref()
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();
            let refrelid = wqueue[ti].constraints[ci].refrelid;
            let refindid = wqueue[ti].constraints[ci].refindid;
            let conid = wqueue[ti].constraints[ci].conid;

            if rel.is_none() {
                // Long since locked, no need for another.
                rel = Some(relation_open(mcx, wqueue[ti].relid, NoLock)?);
            }

            // refrel = table_open(con->refrelid, RowShareLock).
            let refrel = relation_open(mcx, refrelid, RowShareLock)?;

            // con->conwithperiod: the FK uses PERIOD? Read pg_constraint.conperiod
            // (the NewConstraint carries no period flag).
            let hasperiod =
                backend_utils_cache_syscache_seams::search_constraint_form_by_oid::call(conid)?
                    .map(|c| c.form.conperiod)
                    .unwrap_or(false);

            // validateForeignKeyConstraint(conname, rel, refrel, con->refindid,
            //                              con->conid, con->conwithperiod);
            let result = backend_commands_trigger_seams::validate_foreign_key_constraint::call(
                mcx,
                &conname,
                rel.as_ref().expect("rel opened above"),
                &refrel,
                refindid,
                conid,
                hasperiod,
            );

            // No need to mark the constraint row as validated; that was done when
            // the row was inserted earlier. table_close(refrel, NoLock).
            refrel.close(NoLock)?;
            result?;
        }

        if let Some(rel) = rel {
            rel.close(NoLock)?;
        }
    }

    // Finally, run any afterStmts that were queued up.
    for ti in 0..wqueue.len() {
        if !wqueue[ti].afterStmts.is_empty() {
            unported("ATRewriteTables: afterStmts (ProcessUtilityForAlterTable)");
        }
    }

    // foreach table that doesn't need a rewrite: CommandCounterIncrement().
    for _ti in 0..wqueue.len() {
        CommandCounterIncrement()?;
    }

    let _ = mcx;
    Ok(())
}

/// Project a phase-3 work-queue entry into the executor-owned
/// [`backend_executor_execMain_seams::at_rewrite_table_scan`] seam (the scan +
/// expression-eval + constraint-recheck + insert body of C's `ATRewriteTable`).
/// `oid_new_heap` is the transient heap (`InvalidOid` for the scan-only verify
/// path). The newvals carry `(attnum, planned-expr, is_generated)`; the CHECK
/// constraints carry `(name, cooked-qual-expr)`; the partition constraint and
/// `validate_default` flag round out the recheck inputs.
fn run_at_rewrite_table_scan<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &AlteredTableInfo<'mcx>,
    oid_new_heap: Oid,
) -> PgResult<()> {
    // tab->newvals: each NewColumnValue carries an `Expr` (the planned
    // cast/USING/default), its attnum, and is_generated.
    let mut newvals: Vec<(i16, types_nodes::primnodes::Expr, bool)> =
        Vec::with_capacity(tab.newvals.len());
    for nv in tab.newvals.iter() {
        let node = nv
            .expr
            .as_ref()
            .expect("NewColumnValue.expr is NULL in phase-3 rewrite");
        let expr = node
            .as_expr()
            .cloned()
            .expect("NewColumnValue.expr is not an Expr");
        newvals.push((nv.attnum, expr, nv.is_generated));
    }

    // tab->constraints: only CONSTR_CHECK entries are evaluated by the scan (the
    // CONSTR_FOREIGN entries are validated in the separate FK pass). Their qual
    // is the cooked CHECK Expr.
    const CONSTR_CHECK: i32 = 5; // ConstrType::CONSTR_CHECK
    let mut check_names: Vec<String> = Vec::new();
    let mut check_exprs: Vec<types_nodes::primnodes::Expr> = Vec::new();
    for con in tab.constraints.iter() {
        if con.contype != CONSTR_CHECK {
            continue;
        }
        let node = con
            .qual
            .as_ref()
            .expect("CONSTR_CHECK NewConstraint.qual is NULL");
        let expr = node
            .as_expr()
            .cloned()
            .expect("CONSTR_CHECK NewConstraint.qual is not an Expr");
        let name = con
            .name
            .as_ref()
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();
        check_names.push(name);
        check_exprs.push(expr);
    }
    let check_constraints: Vec<(&str, types_nodes::primnodes::Expr)> = check_names
        .iter()
        .zip(check_exprs.iter())
        .map(|(n, e)| (n.as_str(), e.clone()))
        .collect();

    // tab->partition_constraint: the single ANDed Expr, if any.
    let partition_constraint: Vec<types_nodes::primnodes::Expr> =
        match tab.partition_constraint.as_ref() {
            Some(node) => vec![node
                .as_expr()
                .cloned()
                .expect("partition_constraint is not an Expr")],
            None => Vec::new(),
        };

    backend_executor_execMain_seams::at_rewrite_table_scan::call(
        mcx,
        tab.relid,
        oid_new_heap,
        &tab.oldDesc,
        tab.rewrite,
        &newvals,
        &check_constraints,
        tab.verify_new_notnull,
        &partition_constraint,
        tab.validate_default,
    )
}

// ===========================================================================
// ATPrepChangePersistence (tablecmds.c:18820)
// ===========================================================================

/// `AT_REWRITE_ALTER_PERSISTENCE` (event_trigger.h) — the persistence-change
/// rewrite reason bit ORed into `tab->rewrite`.
const AT_REWRITE_ALTER_PERSISTENCE: i32 = 0x01;

/// `ATPrepChangePersistence(tab, rel, toLogged)` (tablecmds.c:18820) — phase-1
/// prep for `ALTER TABLE ... SET LOGGED/UNLOGGED`. Rejects temp tables, the
/// no-op case (already in the target persistence), publication membership when
/// going unlogged, and FK references that would break the
/// permanent-cannot-reference-unlogged invariant; then forces a rewrite and
/// records the new persistence on the work-queue entry.
fn ATPrepChangePersistence<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    tab_idx: usize,
    rel: &Relation<'mcx>,
    to_logged: bool,
) -> PgResult<()> {
    // Disallow changing status for a temp table; also verify whether we can get
    // away with doing nothing.
    match rel.rd_rel.relpersistence {
        RELPERSISTENCE_TEMP => {
            return backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                .errmsg(format!(
                    "cannot change logged status of table \"{}\" because it is temporary",
                    rel.name()
                ))
                .finish(here("ATPrepChangePersistence"));
        }
        RELPERSISTENCE_PERMANENT => {
            if to_logged {
                // nothing to do
                return Ok(());
            }
        }
        RELPERSISTENCE_UNLOGGED => {
            if !to_logged {
                // nothing to do
                return Ok(());
            }
        }
        _ => {}
    }

    // Check that the table is not part of any publication when changing to
    // UNLOGGED, as UNLOGGED tables can't be published.
    if !to_logged
        && !backend_catalog_pg_publication_seams::GetRelationPublications::call(mcx, rel.rd_id)?
            .is_empty()
    {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "cannot change table \"{}\" to unlogged because it is part of a publication",
                rel.name()
            ))
            .errdetail("Unlogged relations cannot be replicated.".to_string())
            .finish(here("ATPrepChangePersistence"));
    }

    // Check existing foreign key constraints to preserve the invariant that
    // permanent tables cannot reference unlogged ones. Self-referencing foreign
    // keys are skipped by the scan helper.
    let fks = backend_catalog_pg_constraint::fk_constraints_for_persistence_check(
        mcx, rel.rd_id, to_logged,
    )?;
    for (foreignrelid, conname) in fks {
        let foreignrel = relation_open(mcx, foreignrelid, AccessShareLock)?;
        let foreign_permanent =
            foreignrel.rd_rel.relpersistence == RELPERSISTENCE_PERMANENT;

        if to_logged {
            if !foreign_permanent {
                let fname = foreignrel.name().to_string();
                foreignrel.close(AccessShareLock)?;
                let _ = conname;
                return backend_utils_error::ereport(ERROR)
                    .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                    .errmsg(format!(
                        "could not change table \"{}\" to logged because it references unlogged table \"{}\"",
                        rel.name(),
                        fname
                    ))
                    .finish(here("ATPrepChangePersistence"));
            }
        } else if foreign_permanent {
            let fname = foreignrel.name().to_string();
            foreignrel.close(AccessShareLock)?;
            let _ = conname;
            return backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                .errmsg(format!(
                    "could not change table \"{}\" to unlogged because it references logged table \"{}\"",
                    rel.name(),
                    fname
                ))
                .finish(here("ATPrepChangePersistence"));
        }

        foreignrel.close(AccessShareLock)?;
    }

    // Force rewrite if necessary; see comment in ATRewriteTables.
    wqueue[tab_idx].rewrite |= AT_REWRITE_ALTER_PERSISTENCE;
    wqueue[tab_idx].newrelpersistence = if to_logged {
        RELPERSISTENCE_PERMANENT
    } else {
        RELPERSISTENCE_UNLOGGED
    };
    wqueue[tab_idx].chgPersistence = true;

    Ok(())
}

// ===========================================================================
// ATGetQueueEntry (tablecmds.c:6561)
// ===========================================================================

/// `ATGetQueueEntry(...)` (tablecmds.c:6561) — find or create the work-queue
/// entry for `rel`, returning its index into `wqueue`.
pub(crate) fn ATGetQueueEntry<'mcx>(
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
        afterStmts: PgVec::new_in(mcx),
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
pub(crate) fn ATSimplePermissions(
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
