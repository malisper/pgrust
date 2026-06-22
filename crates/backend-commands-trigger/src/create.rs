//! Idiomatic port of the **trigger catalog-write DDL leg** of
//! `backend/commands/trigger.c` (PostgreSQL 18.3): `CreateTrigger` /
//! `CreateTriggerFiringOn`.
//!
//! Validates a `CreateTrigStmt`, looks up/validates the trigger function,
//! allocates the `pg_trigger` OID, inserts the row (via the typed
//! `catalog_tuple_insert_pg_trigger` seam), sets `relhastriggers` on the
//! relation's `pg_class` row (so `RelationBuildTriggers` rebuilds
//! `rd_trigdesc`), records the dependencies, and runs the post-create hook.
//!
//! Installs the two outward seams that drive it (see [`init_seams`]):
//!   * `create_trigger` (utility.c `T_CreateTrigStmt`): user `CREATE TRIGGER`.
//!   * `create_unique_key_recheck_trigger` (catalog/index.c): the internal
//!     AFTER INSERT OR UPDATE deferred-uniqueness-recheck trigger for a
//!     deferrable PK/UNIQUE constraint.
//!
//! Partitioned-table FOR EACH ROW triggers fan out: after the parent
//! `pg_trigger` row is written, the trigger is cloned onto every partition
//! (`tgparentid` set, WHEN qual remapped through the partition attribute map),
//! recursing through `CreateTriggerFiringOn`.
//!
//! User `CREATE CONSTRAINT TRIGGER` writes its own `pg_constraint` entry
//! (CONSTRAINT_TRIGGER) via the `create_constraint_entry` seam, then records
//! the trigger→constraint dependency exactly like an internal constraint trigger.

use mcx::Mcx;
use types_acl::acl::{ACL_EXECUTE, ACL_TRIGGER, ACLCHECK_OK};
use types_catalog::catalog_dependency::{
    ObjectAddress, DEPENDENCY_AUTO, DEPENDENCY_INTERNAL, DEPENDENCY_NORMAL,
    DEPENDENCY_PARTITION_PRI, DEPENDENCY_PARTITION_SEC,
};
use types_catalog::pg_trigger as pt;
use types_core::fmgr::{F_NAMEEQ, F_OIDEQ};
use types_core::Oid;
use backend_utils_error::ereport;
use types_error::{
    PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_WRONG_OBJECT_TYPE,
    ERROR,
};
use types_nodes::ddlnodes::CreateTrigStmt;
use types_nodes::nodes::Node;
use types_nodes::parsenodes::OBJECT_FUNCTION;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_tuple::access::{
    RELKIND_FOREIGN_TABLE, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_VIEW,
};
use types_tuple::heaptuple::ItemPointerData;

use backend_access_common_heaptuple::heap_deform_tuple;
use backend_access_common_scankey::ScanKeyInit;
use backend_access_index_genam_seams as genam_seams;
use backend_catalog_aclchk_seams as aclchk;
use backend_catalog_indexing_seams as indexing;
use backend_catalog_pg_depend::{deleteDependencyRecordsFor, recordDependencyOn};
use types_tuple::backend_access_common_heaptuple::Datum;

const InvalidOid: Oid = 0;
/// `TRIGGEROID` (pg_type.h) — the `trigger` pseudo-type OID.
const TRIGGEROID: Oid = 2279;
const PROCEDURE_RELATION_ID: Oid = 1255;
const RELATION_RELATION_ID: Oid = 1259;
const CONSTRAINT_RELATION_ID: Oid = 2606;
/// `CONSTRAINT_TRIGGER` (pg_constraint.h) — contype for a constraint trigger.
const CONSTRAINT_TRIGGER: i8 = b't' as i8;

const ShareRowExclusiveLock: i32 = 6;
const AccessShareLock: i32 = 1;
const RowExclusiveLock: i32 = 3;
const NoLock: i32 = 0;

fn valid(oid: Oid) -> bool {
    oid != InvalidOid
}

/// `CreateTrigger(...)` — the 11-arg public entry (commands/trigger.c:161).
#[allow(clippy::too_many_arguments)]
pub fn CreateTrigger<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateTrigStmt<'mcx>,
    query_string: &str,
    rel_oid: Oid,
    ref_rel_oid: Oid,
    constraint_oid: Oid,
    index_oid: Oid,
    funcoid: Oid,
    parent_trigger_oid: Oid,
    is_internal: bool,
    in_partition: bool,
) -> PgResult<ObjectAddress> {
    CreateTriggerFiringOn(
        mcx,
        stmt,
        query_string,
        rel_oid,
        ref_rel_oid,
        constraint_oid,
        index_oid,
        funcoid,
        parent_trigger_oid,
        None,
        is_internal,
        in_partition,
        pt::TRIGGER_FIRES_ON_ORIGIN,
    )
}

/// `CreateTriggerFiringOn(...)` — commands/trigger.c:178-1209.
#[allow(clippy::too_many_arguments)]
pub fn CreateTriggerFiringOn<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateTrigStmt<'mcx>,
    query_string: &str,
    rel_oid: Oid,
    ref_rel_oid: Oid,
    constraint_oid: Oid,
    index_oid: Oid,
    mut funcoid: Oid,
    parent_trigger_oid: Oid,
    when_clause: Option<Node<'mcx>>,
    is_internal: bool,
    in_partition: bool,
    trigger_fires_when: i8,
) -> PgResult<ObjectAddress> {
    // rel = table_open(relOid, ShareRowExclusiveLock) (or by RangeVar).
    let rel = if valid(rel_oid) {
        backend_access_table_table_seams::table_open::call(mcx, rel_oid, ShareRowExclusiveLock)?
    } else {
        let rv_node = stmt.relation.as_ref().ok_or_else(|| {
            ereport(ERROR)
                .errmsg_internal("CreateTrigger: neither relOid nor stmt->relation given")
                .into_error()
        })?;
        let rv = (**rv_node).as_rangevar().ok_or_else(|| {
            ereport(ERROR)
                .errmsg_internal("CreateTrigger: stmt->relation is not a RangeVar")
                .into_error()
        })?;
        // table_openrv(relation, lock) = RangeVarGetRelid + table_open.
        let arv = to_access_range_var(rv);
        let oid =
            backend_catalog_namespace::RangeVarGetRelid(mcx, &arv, ShareRowExclusiveLock, false)?;
        backend_access_table_table_seams::table_open::call(mcx, oid, ShareRowExclusiveLock)?
    };

    let relkind = rel.rd_rel.relkind;
    let relname = rel.name().to_string();
    let relid = rel.rd_id;

    // Triggers must be on tables or views, with type-specific restrictions.
    if relkind == RELKIND_RELATION {
        if stmt.timing != pt::TRIGGER_TYPE_BEFORE && stmt.timing != pt::TRIGGER_TYPE_AFTER {
            return wrong_type(&relname, "is a table", "Tables cannot have INSTEAD OF triggers.");
        }
    } else if relkind == RELKIND_PARTITIONED_TABLE {
        if stmt.timing != pt::TRIGGER_TYPE_BEFORE && stmt.timing != pt::TRIGGER_TYPE_AFTER {
            return wrong_type(&relname, "is a table", "Tables cannot have INSTEAD OF triggers.");
        }
    } else if relkind == RELKIND_VIEW {
        if stmt.timing != pt::TRIGGER_TYPE_INSTEAD && stmt.row {
            return wrong_type(
                &relname,
                "is a view",
                "Views cannot have row-level BEFORE or AFTER triggers.",
            );
        }
        if (stmt.events & pt::TRIGGER_TYPE_TRUNCATE) != 0 {
            return wrong_type(&relname, "is a view", "Views cannot have TRUNCATE triggers.");
        }
    } else if relkind == RELKIND_FOREIGN_TABLE {
        if stmt.timing != pt::TRIGGER_TYPE_BEFORE && stmt.timing != pt::TRIGGER_TYPE_AFTER {
            return wrong_type(
                &relname,
                "is a foreign table",
                "Foreign tables cannot have INSTEAD OF triggers.",
            );
        }
        if stmt.isconstraint {
            return wrong_type(
                &relname,
                "is a foreign table",
                "Foreign tables cannot have constraint triggers.",
            );
        }
    } else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("relation \"{relname}\" cannot have triggers"))
            .into_error());
    }

    if !backend_utils_misc_guc_tables::vars::allowSystemTableMods.read()
        && backend_catalog_catalog::IsSystemRelation(&rel)
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "permission denied: \"{relname}\" is a system catalog"
            ))
            .into_error());
    }

    // Constraint triggers: resolve/lock the referenced (FK) relation.
    let mut constrrelid = InvalidOid;
    if stmt.isconstraint {
        if valid(ref_rel_oid) {
            backend_storage_lmgr_lmgr::LockRelationOid(ref_rel_oid, AccessShareLock)?;
            constrrelid = ref_rel_oid;
        } else if let Some(cr_node) = stmt.constrrel.as_ref() {
            let cr = (**cr_node).as_rangevar().ok_or_else(|| {
                ereport(ERROR)
                    .errmsg_internal("CreateTrigger: stmt->constrrel is not a RangeVar")
                    .into_error()
            })?;
            let acr = to_access_range_var(cr);
            constrrelid =
                backend_catalog_namespace::RangeVarGetRelid(mcx, &acr, AccessShareLock, false)?;
        }
    }

    // Permission checks (skipped for internal triggers).
    if !is_internal {
        let userid = backend_utils_init_miscinit::GetUserId();
        let r = aclchk::pg_class_aclcheck::call(relid, userid, ACL_TRIGGER)?;
        if r != ACLCHECK_OK {
            aclchk::aclcheck_error::call(
                r,
                backend_catalog_objectaddress::resolve::get_relkind_objtype(relkind as u8),
                Some(relname.clone()),
            )?;
        }
        if valid(constrrelid) {
            let r = aclchk::pg_class_aclcheck::call(constrrelid, userid, ACL_TRIGGER)?;
            if r != ACLCHECK_OK {
                let k = backend_utils_cache_lsyscache::relation::get_rel_relkind(constrrelid)?;
                let nm = backend_utils_cache_lsyscache::relation::get_rel_name(mcx, constrrelid)?
                    .map(|s| s.to_string())
                    .unwrap_or_default();
                aclchk::aclcheck_error::call(
                    r,
                    backend_catalog_objectaddress::resolve::get_relkind_objtype(k),
                    Some(nm),
                )?;
            }
        }
    }

    // When called on a partitioned table to create a FOR EACH ROW trigger
    // that's not internal, we create one trigger for each partition, too.
    // For that, we'd better hold lock on all of them ahead of time.
    let partition_recurse = !is_internal && stmt.row && relkind == RELKIND_PARTITIONED_TABLE;
    if partition_recurse {
        backend_catalog_pg_inherits::find_all_inheritors(mcx, relid, ShareRowExclusiveLock, false)?;
    }

    // Compute tgtype.
    let mut tgtype: i16 = 0;
    if stmt.row {
        tgtype |= pt::TRIGGER_TYPE_ROW;
    }
    tgtype |= stmt.timing;
    tgtype |= stmt.events;

    if pt::TRIGGER_FOR_ROW(tgtype) && (tgtype & pt::TRIGGER_TYPE_TRUNCATE) != 0 {
        return feature_err("TRUNCATE FOR EACH ROW triggers are not supported");
    }
    if (tgtype & pt::TRIGGER_TYPE_INSTEAD) != 0 {
        if !pt::TRIGGER_FOR_ROW(tgtype) {
            return feature_err("INSTEAD OF triggers must be FOR EACH ROW");
        }
        if stmt.whenClause.is_some() {
            return feature_err("INSTEAD OF triggers cannot have WHEN conditions");
        }
        if !stmt.columns.is_empty() {
            return feature_err("INSTEAD OF triggers cannot have column lists");
        }
    }

    // REFERENCING transition tables (CreateTriggerFiringOn, trigger.c:464-557).
    //
    // Validate each `TriggerTransition` clause and collect the OLD/NEW table
    // names that get written into pg_trigger.tgoldtable/tgnewtable. Faithful
    // 1:1 with the C loop, including every ereport.
    let (oldtablename, newtablename) =
        transform_trigger_transitions(&rel, stmt, relkind, tgtype)?;

    // Parse the WHEN clause, if any. As a side effect this fills when_rtable
    // (the OLD/NEW pseudo-relation RTEs), which we'll need below for
    // recordDependencyOnExpr. (We are never passed an already-transformed
    // clause here; the partition-recurse leg that would do so is gated below.)
    let when = transform_trigger_when(mcx, &rel, stmt, tgtype, query_string, when_clause)?;

    // If it's a user-entered CREATE CONSTRAINT TRIGGER command, make a
    // corresponding pg_constraint entry (commands/trigger.c:805-844).
    let mut constraint_oid = constraint_oid;
    if stmt.isconstraint && !valid(constraint_oid) {
        // Internal callers should have made their own constraints.
        debug_assert!(!is_internal);
        let trigname_for_constr = stmt
            .trigname
            .as_ref()
            .map(|s| s.as_str())
            .unwrap_or_default();
        constraint_oid = backend_catalog_pg_constraint_seams::create_constraint_entry::call(
            mcx,
            backend_catalog_pg_constraint_seams::CreateConstraintEntryArgs {
                constraint_name: trigname_for_constr,
                constraint_namespace: rel.rd_rel.relnamespace,
                constraint_type: CONSTRAINT_TRIGGER,
                is_deferrable: stmt.deferrable,
                is_deferred: stmt.initdeferred,
                parent_constr_id: InvalidOid, // no parent
                rel_id: relid,
                constraint_key: &[], // no conkey
                constraint_n_keys: 0,
                constraint_n_total_keys: 0,
                index_rel_id: InvalidOid, // no index
                excl_op: None,            // no exclusion
                con_is_local: true,       // islocal
                con_inh_count: 0,         // inhcount
                con_no_inherit: true,     // noinherit
                con_period: false,        // conperiod
                is_internal,
            },
        )?;
    }

    // Find and validate the trigger function.
    if !valid(funcoid) {
        let names = funcname_strings(mcx, stmt)?;
        funcoid = backend_parser_func::LookupFuncName(mcx, &names, 0, &[], false)?;
    }
    if !is_internal {
        let userid = backend_utils_init_miscinit::GetUserId();
        let r = aclchk::object_aclcheck::call(PROCEDURE_RELATION_ID, funcoid, userid, ACL_EXECUTE)?;
        if r != ACLCHECK_OK {
            aclchk::aclcheck_error::call(r, OBJECT_FUNCTION, Some(funcname_display(stmt)))?;
        }
    }
    if backend_utils_cache_lsyscache::function::get_func_rettype(funcoid)? != TRIGGEROID {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "function {} must return type trigger",
                funcname_display(stmt)
            ))
            .into_error());
    }

    // Open pg_trigger; for a user trigger, scan for an existing same-named row.
    let tgrel = backend_access_table_table_seams::table_open::call(
        mcx,
        pt::TriggerRelationId,
        RowExclusiveLock,
    )?;

    let trigname_in = stmt
        .trigname
        .as_ref()
        .map(|s| s.as_str().to_string())
        .unwrap_or_default();

    let mut existing: Option<(Oid, ItemPointerData)> = None;
    let mut trigoid = InvalidOid;

    if !is_internal {
        if let Some(found) = scan_existing_trigger(mcx, &tgrel, relid, &trigname_in)? {
            trigoid = found.tgoid;
            existing = Some((found.tgoid, found.t_self));
            if !stmt.replace {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_DUPLICATE_OBJECT)
                    .errmsg(format!(
                        "trigger \"{trigname_in}\" for relation \"{relname}\" already exists"
                    ))
                    .into_error());
            }
            if (found.tgisinternal || valid(found.tgparentid)) && !is_internal && !in_partition {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_DUPLICATE_OBJECT)
                    .errmsg(format!(
                        "trigger \"{trigname_in}\" for relation \"{relname}\" is an internal or a child trigger"
                    ))
                    .into_error());
            }
            if valid(found.tgconstraint) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_DUPLICATE_OBJECT)
                    .errmsg(format!(
                        "trigger \"{trigname_in}\" for relation \"{relname}\" is a constraint trigger"
                    ))
                    .into_error());
            }
        }
    }

    if existing.is_none() {
        trigoid = backend_catalog_catalog::GetNewOidWithIndex(
            &tgrel,
            pt::TriggerOidIndexId,
            pt::Anum_pg_trigger_oid,
        )?;
    }

    // Internal triggers get a unique name by appending the OID.
    let trigname = if is_internal {
        format!("{trigname_in}_{trigoid}")
    } else {
        trigname_in.clone()
    };

    // tgargs bytea payload: arg1\0arg2\0...
    let mut tgargs: Vec<u8> = Vec::new();
    let mut nargs: i16 = 0;
    for a in stmt.args.iter() {
        let s = node_strval(a)?;
        tgargs.extend_from_slice(s.as_bytes());
        tgargs.push(0);
        nargs += 1;
    }

    // Column-number array for a column-specific trigger.
    let mut columns: Vec<i16> = Vec::new();
    for c in stmt.columns.iter() {
        let name = node_strval(c)?;
        let attnum = backend_parser_relation::attnameAttNum(&rel, &name, false)?;
        if attnum == 0 {
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_UNDEFINED_COLUMN)
                .errmsg(format!(
                    "column \"{name}\" of relation \"{relname}\" does not exist"
                ))
                .into_error());
        }
        let attnum = attnum as i16;
        if columns.contains(&attnum) {
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_DUPLICATE_COLUMN)
                .errmsg(format!("column \"{name}\" specified more than once"))
                .into_error());
        }
        columns.push(attnum);
    }

    let is_replace = existing.is_some();
    let row = pt::PgTriggerInsertRow {
        existing,
        tgrelid: relid,
        tgparentid: parent_trigger_oid,
        tgname: trigname,
        tgfoid: funcoid,
        tgtype,
        tgenabled: trigger_fires_when,
        tgisinternal: is_internal,
        tgconstrrelid: constrrelid,
        tgconstrindid: index_oid,
        tgconstraint: constraint_oid,
        tgdeferrable: stmt.deferrable,
        tginitdeferred: stmt.initdeferred,
        tgnargs: nargs,
        tgattr: columns.clone(),
        tgargs,
        tgqual: when.qual.clone(),
        tgoldtable: oldtablename.clone(),
        tgnewtable: newtablename.clone(),
    };
    trigoid = indexing::catalog_tuple_insert_pg_trigger::call(mcx, &tgrel, &row)?;

    tgrel.close(RowExclusiveLock)?;

    // pg_class.relhastriggers = true (forces rd_trigdesc rebuild).
    if !indexing::set_pg_class_relhastriggers::call(relid)? {
        return Err(ereport(ERROR)
            .errmsg_internal(format!("cache lookup failed for relation {relid}"))
            .into_error());
    }

    if is_replace {
        deleteDependencyRecordsFor(pt::TriggerRelationId, trigoid, true)?;
    }

    let myself = ObjectAddress {
        classId: pt::TriggerRelationId,
        objectId: trigoid,
        objectSubId: 0,
    };

    // Always a normal dependency on the function.
    recordDependencyOn(
        mcx,
        &myself,
        &addr(PROCEDURE_RELATION_ID, funcoid, 0),
        DEPENDENCY_NORMAL,
    )?;

    if is_internal && valid(constraint_oid) {
        recordDependencyOn(
            mcx,
            &myself,
            &addr(CONSTRAINT_RELATION_ID, constraint_oid, 0),
            DEPENDENCY_INTERNAL,
        )?;
    } else {
        recordDependencyOn(
            mcx,
            &myself,
            &addr(RELATION_RELATION_ID, relid, 0),
            DEPENDENCY_AUTO,
        )?;
        if valid(constrrelid) {
            recordDependencyOn(
                mcx,
                &myself,
                &addr(RELATION_RELATION_ID, constrrelid, 0),
                DEPENDENCY_AUTO,
            )?;
        }
        if valid(constraint_oid) {
            recordDependencyOn(
                mcx,
                &addr(CONSTRAINT_RELATION_ID, constraint_oid, 0),
                &myself,
                DEPENDENCY_INTERNAL,
            )?;
        }

        // If it's a partition trigger, create the partition dependencies.
        if valid(parent_trigger_oid) {
            recordDependencyOn(
                mcx,
                &myself,
                &addr(pt::TriggerRelationId, parent_trigger_oid, 0),
                DEPENDENCY_PARTITION_PRI,
            )?;
            recordDependencyOn(
                mcx,
                &myself,
                &addr(RELATION_RELATION_ID, relid, 0),
                DEPENDENCY_PARTITION_SEC,
            )?;
        }
    }

    for &col in &columns {
        recordDependencyOn(
            mcx,
            &myself,
            &addr(RELATION_RELATION_ID, relid, col as i32),
            DEPENDENCY_NORMAL,
        )?;
    }

    // If it has a WHEN clause, add dependencies on objects mentioned in the
    // expression (eg, functions, as well as any columns used).
    if !when.when_rtable.is_empty() {
        if let Some(when_clause) = when.when_clause.as_ref() {
            backend_catalog_dependency_seams::record_dependency_on_expr::call(
                myself.clone(),
                when_clause,
                &when.when_rtable,
                DEPENDENCY_NORMAL,
            )?;
        }
    }

    // InvokeObjectPostCreateHookArg(TriggerRelationId, trigoid, 0, isInternal)
    // (a no-op without an installed object-access hook).
    backend_catalog_objectaccess_seams::invoke_object_post_create_hook_arg::call(
        pt::TriggerRelationId,
        trigoid,
        0,
        is_internal,
    )?;

    // Lastly, create the trigger on child relations, if needed.
    if partition_recurse {
        let partdesc = backend_partitioning_partdesc::RelationGetPartitionDesc(mcx, &rel, true)?;

        // We don't currently expect to be called with a valid indexOid. If that
        // ever changes then we'll need to find the corresponding child index.
        debug_assert!(!valid(index_oid));

        // Iterate to create the trigger on each existing partition.
        for i in 0..(partdesc.nparts as usize) {
            let child_oid = partdesc.oids[i];
            let child_tbl = backend_access_table_table_seams::table_open::call(
                mcx,
                child_oid,
                ShareRowExclusiveLock,
            )?;

            // Initialize our fabricated parse node by copying the original one,
            // then resetting fields that we pass separately.
            let mut child_stmt = stmt.clone_in(mcx)?;
            child_stmt.funcname = mcx::PgVec::new_in(mcx);
            child_stmt.whenClause = None;

            // If there is a WHEN clause, create a modified copy of it.
            let child_qual = map_when_to_partition(mcx, &when.when_clause, &child_tbl, &rel)?;

            CreateTriggerFiringOn(
                mcx,
                &child_stmt,
                query_string,
                child_oid,
                ref_rel_oid,
                InvalidOid,
                InvalidOid,
                funcoid,
                trigoid,
                child_qual,
                is_internal,
                true,
                trigger_fires_when,
            )?;

            child_tbl.close(NoLock)?;
        }
    }

    // Keep lock on target rel until end of xact.
    rel.close(NoLock)?;

    Ok(myself)
}

/// Build a per-partition copy of a trigger WHEN clause: `copyObject(whenClause)`
/// followed by `map_partition_varattnos(..., PRS2_OLD_VARNO, ...)` and the same
/// for `PRS2_NEW_VARNO` (commands/trigger.c:1181-1188). Returns `None` when
/// there is no WHEN clause.
fn map_when_to_partition<'mcx>(
    mcx: Mcx<'mcx>,
    when_clause: &Option<Node<'mcx>>,
    child_tbl: &types_rel::RelationData<'mcx>,
    parent: &types_rel::RelationData<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    let Some(node) = when_clause.as_ref() else {
        return Ok(None);
    };

    // map_partition_varattnos walks any node tree; wrap the single qual node in a
    // one-element list, map OLD then NEW varnos, and unwrap.
    let mut exprs: mcx::PgVec<'mcx, Node<'mcx>> = mcx::PgVec::new_in(mcx);
    exprs.push(node.clone_in(mcx)?);
    let exprs = backend_catalog_partition_seams::map_partition_varattnos::call(
        mcx,
        exprs,
        PRS2_OLD_VARNO,
        child_tbl,
        parent,
    )?;
    let mut exprs = backend_catalog_partition_seams::map_partition_varattnos::call(
        mcx,
        exprs,
        PRS2_NEW_VARNO,
        child_tbl,
        parent,
    )?;
    Ok(exprs.pop())
}

/// `PRS2_OLD_VARNO` / `PRS2_NEW_VARNO` (primnodes.h): the WHEN clause's OLD/NEW
/// pseudo-relations are always range-table entries 1 and 2.
const PRS2_OLD_VARNO: i32 = 1;
const PRS2_NEW_VARNO: i32 = 2;

/// The result of transforming a trigger WHEN clause (commands/trigger.c:566-688).
struct WhenTransform<'mcx> {
    /// `nodeToString(whenClause)` — the `pg_node_tree` image stored in
    /// `pg_trigger.tgqual` (None when there is no WHEN clause).
    qual: Option<String>,
    /// The already-transformed WHEN expression, for `recordDependencyOnExpr`.
    when_clause: Option<Node<'mcx>>,
    /// `pstate->p_rtable` (the OLD/NEW RTEs) — needed by
    /// `recordDependencyOnExpr`. Empty when there is no WHEN clause.
    when_rtable: mcx::PgVec<'mcx, types_nodes::parsenodes::RangeTblEntry<'mcx>>,
}

/// `make_old_new_alias(name)` — an `Alias` with just `aliasname` set, for the
/// OLD/NEW WHEN-clause range-table entries (mirrors `makeAlias(name, NIL)`).
fn make_old_new_alias<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
) -> PgResult<types_nodes::rawnodes::Alias<'mcx>> {
    Ok(types_nodes::rawnodes::Alias {
        aliasname: Some(mcx::PgString::from_str_in(name, mcx)?),
        colnames: mcx::PgVec::new_in(mcx),
    })
}

/// `TRIGGER_FOR_BEFORE(type)` — `(type) & TRIGGER_TYPE_BEFORE` (pg_trigger.h).
fn trigger_for_before(tgtype: i16) -> bool {
    (tgtype & pt::TRIGGER_TYPE_BEFORE) != 0
}

/// `CreateTriggerFiringOn` REFERENCING handling (trigger.c:464-557).
///
/// Validates each `TriggerTransition` clause of a `REFERENCING [OLD|NEW] TABLE
/// AS name` and returns the `(oldtablename, newtablename)` to write into
/// `pg_trigger.tgoldtable`/`tgnewtable`. The C body is a `foreach` over
/// `stmt->transitionRels`; every `ereport` is reproduced 1:1.
fn transform_trigger_transitions<'mcx>(
    rel: &types_rel::RelationData<'mcx>,
    stmt: &CreateTrigStmt<'mcx>,
    relkind: u8,
    tgtype: i16,
) -> PgResult<(Option<String>, Option<String>)> {
    let mut oldtablename: Option<String> = None;
    let mut newtablename: Option<String> = None;

    if stmt.transitionRels.is_empty() {
        return Ok((None, None));
    }

    // C iterates the List; each element is a TriggerTransition.
    for node in stmt.transitionRels.iter() {
        let tt = (**node).as_triggertransition().ok_or_else(|| {
            ereport(ERROR)
                .errmsg_internal("CreateTrigger: transitionRels element is not a TriggerTransition")
                .into_error()
        })?;

        // if (!(tt->isTable))
        if !tt.isTable {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("ROW variable naming in the REFERENCING clause is not supported")
                .errhint("Use OLD TABLE or NEW TABLE for naming transition tables.")
                .into_error());
        }

        // Because of the above test, we omit further ROW-related testing
        // below. If we later allow naming OLD/NEW row variables, adjust this.

        // if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
        //   ereport: "%s" is a foreign table / Triggers on foreign tables ...
        if relkind == RELKIND_FOREIGN_TABLE {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!("\"{}\" is a foreign table", rel.name()))
                .errdetail("Triggers on foreign tables cannot have transition tables.")
                .into_error());
        }

        // if (rel->rd_rel->relkind == RELKIND_VIEW)
        //   ereport: "%s" is a view / Triggers on views cannot have transition tables.
        if relkind == RELKIND_VIEW {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!("\"{}\" is a view", rel.name()))
                .errdetail("Triggers on views cannot have transition tables.")
                .into_error());
        }

        // We currently don't allow row-level triggers with transition tables on
        // partition or inheritance children.
        //   if (TRIGGER_FOR_ROW(tgtype) && has_superclass(rel->rd_id)) {
        //       if (rel->rd_rel->relispartition)
        //           ereport: ROW triggers ... not supported on partitions
        //       else
        //           ereport: ROW triggers ... not supported on inheritance children
        //   }
        if pt::TRIGGER_FOR_ROW(tgtype)
            && backend_catalog_pg_inherits::has_superclass(rel.rd_id)?
        {
            if rel.rd_rel.relispartition {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("ROW triggers with transition tables are not supported on partitions")
                    .into_error());
            } else {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg(
                        "ROW triggers with transition tables are not supported on inheritance children",
                    )
                    .into_error());
            }
        }

        // if (stmt->timing != TRIGGER_TYPE_AFTER)
        if stmt.timing != pt::TRIGGER_TYPE_AFTER {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("transition table name can only be specified for an AFTER trigger")
                .into_error());
        }

        // if (TRIGGER_FOR_TRUNCATE(tgtype))
        if (tgtype & pt::TRIGGER_TYPE_TRUNCATE) != 0 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("TRUNCATE triggers with transition tables are not supported")
                .into_error());
        }

        // We currently don't allow multi-event triggers ("INSERT OR UPDATE")
        // with transition tables.
        //   if (((TRIGGER_FOR_INSERT(tgtype) ? 1 : 0) +
        //        (TRIGGER_FOR_UPDATE(tgtype) ? 1 : 0) +
        //        (TRIGGER_FOR_DELETE(tgtype) ? 1 : 0)) != 1)
        let nevents = ((tgtype & pt::TRIGGER_TYPE_INSERT != 0) as i32)
            + ((tgtype & pt::TRIGGER_TYPE_UPDATE != 0) as i32)
            + ((tgtype & pt::TRIGGER_TYPE_DELETE != 0) as i32);
        if nevents != 1 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("transition tables cannot be specified for triggers with more than one event")
                .into_error());
        }

        // We currently don't allow column-specific triggers with transition
        // tables for the same reason.
        if !stmt.columns.is_empty() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("transition tables cannot be specified for triggers with column lists")
                .into_error());
        }

        let name = tt.name.as_deref().unwrap_or("").to_string();

        // if (tt->isNew) { ... NEW TABLE ... } else { ... OLD TABLE ... }
        if tt.isNew {
            // if (!(TRIGGER_FOR_INSERT(tgtype) || TRIGGER_FOR_UPDATE(tgtype)))
            let for_insert = (tgtype & pt::TRIGGER_TYPE_INSERT) != 0;
            let for_update = (tgtype & pt::TRIGGER_TYPE_UPDATE) != 0;
            if !(for_insert || for_update) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("NEW TABLE can only be specified for an INSERT or UPDATE trigger")
                    .into_error());
            }
            // if (newtablename != NULL) — duplicate.
            if newtablename.is_some() {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("NEW TABLE cannot be specified multiple times")
                    .into_error());
            }
            newtablename = Some(name);
        } else {
            // if (!(TRIGGER_FOR_DELETE(tgtype) || TRIGGER_FOR_UPDATE(tgtype)))
            let for_delete = (tgtype & pt::TRIGGER_TYPE_DELETE) != 0;
            let for_update = (tgtype & pt::TRIGGER_TYPE_UPDATE) != 0;
            if !(for_delete || for_update) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("OLD TABLE can only be specified for a DELETE or UPDATE trigger")
                    .into_error());
            }
            if oldtablename.is_some() {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("OLD TABLE cannot be specified multiple times")
                    .into_error());
            }
            oldtablename = Some(name);
        }
    }

    // if (newtablename != NULL && oldtablename != NULL && strcmp(...) == 0)
    if let (Some(n), Some(o)) = (newtablename.as_deref(), oldtablename.as_deref()) {
        if n == o {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("OLD TABLE name and NEW TABLE name cannot be the same")
                .into_error());
        }
    }

    Ok((oldtablename, newtablename))
}

/// Parse + validate a trigger WHEN clause and render it as a `tgqual` image
/// (commands/trigger.c:558-688). Sets up a `ParseState` with OLD (varno 1) and
/// NEW (varno 2) pseudo-relation RTEs over `rel`, transforms the clause,
/// fixes its collations, checks the OLD/NEW reference restrictions per tgtype,
/// then `nodeToString`s it. Returns an empty [`WhenTransform`] when there is no
/// WHEN clause.
fn transform_trigger_when<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::RelationData<'mcx>,
    stmt: &CreateTrigStmt<'mcx>,
    tgtype: i16,
    query_string: &str,
    when_clause_in: Option<Node<'mcx>>,
) -> PgResult<WhenTransform<'mcx>> {
    // Mirrors commands/trigger.c:566-688. Three branches:
    //   * stmt->whenClause set: transform the raw clause (parent path).
    //   * neither stmt->whenClause nor a passed-in clause: no WHEN at all.
    //   * a pre-transformed clause passed in (partition-recurse / clone path):
    //     just nodeToString() it; no rtable (deps already recorded on parent).
    let Some(when_in) = stmt.whenClause.as_ref() else {
        return match when_clause_in {
            None => Ok(WhenTransform {
                qual: None,
                when_clause: None,
                when_rtable: mcx::PgVec::new_in(mcx),
            }),
            Some(node) => {
                let qual = backend_nodes_outfuncs::nodeToString(mcx, &node)?
                    .as_str()
                    .to_string();
                Ok(WhenTransform {
                    qual: Some(qual),
                    when_clause: Some(node),
                    when_rtable: mcx::PgVec::new_in(mcx),
                })
            }
        };
    };

    // Set up a pstate to parse with.
    let mut pstate = backend_parser_small1::make_parsestate(mcx, None)?;
    pstate.p_sourcetext = Some(mcx::PgString::from_str_in(query_string, mcx)?);

    // Set up nsitems for OLD and NEW references.
    // 'OLD' must always have varno equal to 1 and 'NEW' equal to 2.
    let old_alias = make_old_new_alias(mcx, "old")?;
    let new_alias = make_old_new_alias(mcx, "new")?;
    let oldnsitem = backend_parser_relation::addRangeTableEntryForRelation(
        mcx,
        &mut pstate,
        rel,
        AccessShareLock,
        Some(old_alias),
        false,
        false,
    )?;
    backend_parser_relation::addNSItemToQuery(mcx, &mut pstate, oldnsitem, false, true, true)?;
    let newnsitem = backend_parser_relation::addRangeTableEntryForRelation(
        mcx,
        &mut pstate,
        rel,
        AccessShareLock,
        Some(new_alias),
        false,
        false,
    )?;
    backend_parser_relation::addNSItemToQuery(mcx, &mut pstate, newnsitem, false, true, true)?;

    // Transform expression.  Copy to be sure we don't modify original.
    let clause_copy = (**when_in).clone_in(mcx)?;
    let mut when_expr = backend_parser_clause_seams::transform_where_clause::call(
        mcx,
        &mut pstate,
        Some(clause_copy),
        types_nodes::parsestmt::ParseExprKind::EXPR_KIND_TRIGGER_WHEN,
        "WHEN",
    )?;

    // We have to fix its collations too.
    if let Some(e) = when_expr.as_mut() {
        backend_parser_parse_collate::assign_expr_collations(Some(&pstate), e)?;
    }
    let when_node = match when_expr {
        Some(e) => Node::mk_expr(mcx, e)?,
        None => {
            return Ok(WhenTransform {
                qual: None,
                when_clause: None,
                when_rtable: core::mem::replace(&mut pstate.p_rtable, mcx::PgVec::new_in(mcx)),
            })
        }
    };

    // Check for disallowed references to OLD/NEW.
    //
    // NB: pull_var_clause is okay here only because we don't allow subselects
    // in WHEN clauses; it would fail to examine the contents of subselects.
    for var_expr in backend_optimizer_util_vars::pull_var_clause(mcx, &when_node, 0)? {
        let var = var_expr.as_var().ok_or_else(|| {
            ereport(ERROR)
                .errmsg_internal("trigger WHEN: pull_var_clause returned a non-Var")
                .into_error()
        })?;
        match var.varno {
            PRS2_OLD_VARNO => {
                if !pt::TRIGGER_FOR_ROW(tgtype) {
                    return when_obj_def_err(
                        "statement trigger's WHEN condition cannot reference column values",
                        &pstate,
                        var.location,
                    );
                }
                if pt::TRIGGER_FOR_INSERT(tgtype) {
                    return when_obj_def_err(
                        "INSERT trigger's WHEN condition cannot reference OLD values",
                        &pstate,
                        var.location,
                    );
                }
                // system columns are okay here
            }
            PRS2_NEW_VARNO => {
                if !pt::TRIGGER_FOR_ROW(tgtype) {
                    return when_obj_def_err(
                        "statement trigger's WHEN condition cannot reference column values",
                        &pstate,
                        var.location,
                    );
                }
                if pt::TRIGGER_FOR_DELETE(tgtype) {
                    return when_obj_def_err(
                        "DELETE trigger's WHEN condition cannot reference NEW values",
                        &pstate,
                        var.location,
                    );
                }
                if var.varattno < 0 && trigger_for_before(tgtype) {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("BEFORE trigger's WHEN condition cannot reference NEW system columns")
                        .errposition(backend_parser_small1::parser_errposition(
                            &pstate,
                            var.location,
                        ))
                        .into_error());
                }
                if trigger_for_before(tgtype) && var.varattno == 0 {
                    if let Some(constr) = rel.rd_att.constr.as_ref() {
                        if constr.has_generated_stored || constr.has_generated_virtual {
                            return Err(ereport(ERROR)
                                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                                .errmsg("BEFORE trigger's WHEN condition cannot reference NEW generated columns")
                                .errdetail("A whole-row reference is used and the table contains generated columns.")
                                .errposition(backend_parser_small1::parser_errposition(
                                    &pstate,
                                    var.location,
                                ))
                                .into_error());
                        }
                    }
                }
                if trigger_for_before(tgtype) && var.varattno > 0 {
                    let att = rel.rd_att.attr((var.varattno - 1) as usize);
                    if att.attgenerated != 0 {
                        let colname = String::from_utf8_lossy(att.attname.name_str());
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                            .errmsg("BEFORE trigger's WHEN condition cannot reference NEW generated columns")
                            .errdetail(format!("Column \"{colname}\" is a generated column."))
                            .errposition(backend_parser_small1::parser_errposition(
                                &pstate,
                                var.location,
                            ))
                            .into_error());
                    }
                }
            }
            _ => {
                // can't happen without add_missing_from, so just elog
                return Err(ereport(ERROR)
                    .errmsg_internal(
                        "trigger WHEN condition cannot contain references to other relations",
                    )
                    .into_error());
            }
        }
    }

    // we'll need the rtable for recordDependencyOnExpr
    let when_rtable = core::mem::replace(&mut pstate.p_rtable, mcx::PgVec::new_in(mcx));
    let qual = backend_nodes_outfuncs::nodeToString(mcx, &when_node)?
        .as_str()
        .to_string();

    Ok(WhenTransform {
        qual: Some(qual),
        when_clause: Some(when_node),
        when_rtable,
    })
}

fn when_obj_def_err<'a>(
    msg: &str,
    pstate: &types_nodes::parsestmt::ParseState<'_>,
    location: i32,
) -> PgResult<WhenTransform<'a>> {
    Err(ereport(ERROR)
        .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
        .errmsg(msg.to_string())
        .errposition(backend_parser_small1::parser_errposition(pstate, location))
        .into_error())
}

fn addr(class_id: Oid, object_id: Oid, sub_id: i32) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: sub_id,
    }
}

fn wrong_type(relname: &str, what: &str, detail: &str) -> PgResult<ObjectAddress> {
    Err(ereport(ERROR)
        .errcode(ERRCODE_WRONG_OBJECT_TYPE)
        .errmsg(format!("\"{relname}\" {what}"))
        .errdetail(detail.to_string())
        .into_error())
}

fn feature_err(msg: &str) -> PgResult<ObjectAddress> {
    Err(ereport(ERROR)
        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
        .errmsg(msg.to_string())
        .into_error())
}

/// Convert an owned-tree `rawnodes::RangeVar` to the resolved
/// `types_tuple::access::RangeVar` that `RangeVarGetRelid` consumes (precedent:
/// policy.c's `to_access_range_var`).
fn to_access_range_var(rv: &types_nodes::rawnodes::RangeVar<'_>) -> types_tuple::access::RangeVar {
    types_tuple::access::RangeVar {
        catalogname: rv.catalogname.as_deref().map(|s| s.into()),
        schemaname: rv.schemaname.as_deref().map(|s| s.into()),
        relname: rv.relname.as_deref().unwrap_or("").into(),
        inh: rv.inh,
        relpersistence: rv.relpersistence as u8,
        location: rv.location,
    }
}

/// The columns of an existing pg_trigger row that `CreateTrigger`'s duplicate
/// scan reads.
struct ExistingTrigger {
    tgoid: Oid,
    t_self: ItemPointerData,
    tgconstraint: Oid,
    tgisinternal: bool,
    tgparentid: Oid,
}

/// `systable_beginscan(tgrel, TriggerRelidNameIndexId, (tgrelid, tgname))` +
/// `systable_getnext`: at most one matching row (commands/trigger.c:717-749).
fn scan_existing_trigger<'mcx>(
    mcx: Mcx<'mcx>,
    tgrel: &types_rel::RelationData<'mcx>,
    relid: Oid,
    trigname: &str,
) -> PgResult<Option<ExistingTrigger>> {
    let mut k0 = ScanKeyData::empty();
    ScanKeyInit(
        &mut k0,
        pt::Anum_pg_trigger_tgrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(relid),
    )?;
    let mut k1 = ScanKeyData::empty();
    ScanKeyInit(
        &mut k1,
        pt::Anum_pg_trigger_tgname,
        BTEqualStrategyNumber,
        F_NAMEEQ,
        Datum::ByRef(mcx::slice_in(mcx, trigname.as_bytes())?),
    )?;
    let keys = [k0, k1];

    let mut scan =
        genam_seams::systable_beginscan::call(tgrel, pt::TriggerRelidNameIndexId, true, None, &keys)?;
    let mut result = None;
    if let Some(tup) = genam_seams::systable_getnext::call(mcx, scan.desc_mut())? {
        let cols = heap_deform_tuple(mcx, &tup.tuple, &tgrel.rd_att, &tup.data)?;
        let col = |attno: i16| cols[attno as usize - 1].0.clone();
        result = Some(ExistingTrigger {
            tgoid: col(pt::Anum_pg_trigger_oid).as_oid(),
            t_self: tup.tuple.t_self,
            tgconstraint: col(pt::Anum_pg_trigger_tgconstraint).as_oid(),
            tgisinternal: col(pt::Anum_pg_trigger_tgisinternal).as_bool(),
            tgparentid: col(pt::Anum_pg_trigger_tgparentid).as_oid(),
        });
    }
    let _ = scan;
    Ok(result)
}

/// `strVal(lfirst(le))` over a `String` node.
fn node_strval(node: &Node<'_>) -> PgResult<String> {
    match node.as_string() {
        Some(s) => Ok(s.sval.as_str().to_string()),
        None => Err(ereport(ERROR)
            .errmsg_internal("CreateTrigger: expected a String node")
            .into_error()),
    }
}

/// The `funcname` list as `PgString`s (allocated in `mcx`) for `LookupFuncName`.
fn funcname_strings<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateTrigStmt<'_>,
) -> PgResult<Vec<mcx::PgString<'mcx>>> {
    let mut out = Vec::new();
    for n in stmt.funcname.iter() {
        let s = node_strval(n)?;
        out.push(mcx::PgString::from_str_in(&s, mcx)?);
    }
    Ok(out)
}

fn funcname_display(stmt: &CreateTrigStmt<'_>) -> String {
    stmt.funcname
        .iter()
        .filter_map(|n| n.as_string().map(|s| s.sval.as_str().to_string()))
        .collect::<Vec<_>>()
        .join(".")
}

/// `unique_key_recheck` (pg_proc.dat OID 1250) — the deferred-uniqueness
/// recheck trigger function.
const F_UNIQUE_KEY_RECHECK: Oid = 1250;

/// Install the catalog-write DDL seams:
///   * `create_trigger` (utility.c `T_CreateTrigStmt`).
///   * `create_unique_key_recheck_trigger` (catalog/index.c).
pub fn init_seams() {
    // ProcessUtilitySlow dispatch: CREATE TRIGGER.
    backend_tcop_utility_out_seams::create_trigger::set(
        |mcx, parsetree, query_string| match parsetree.as_createtrigstmt() {
            Some(stmt) => CreateTrigger(
                mcx,
                stmt,
                query_string,
                InvalidOid,
                InvalidOid,
                InvalidOid,
                InvalidOid,
                InvalidOid,
                InvalidOid,
                false,
                false,
            ),
            None => Err(types_error::PgError::error(
                "create_trigger: parse tree is not a CreateTrigStmt",
            )),
        },
    );

    // catalog/index.c index_constraint_create's deferrable-constraint
    // CreateTrigger call (the FK / deferrable PK-UNIQUE recheck trigger).
    backend_commands_trigger_seams::create_unique_key_recheck_trigger::set(
        |rel_oid, constraint_oid, index_oid, is_primary, initdeferred| {
            let ctx = mcx::MemoryContext::new("create_unique_key_recheck_trigger");
            let mcx = ctx.mcx();

            // Build the fixed CreateTrigStmt index.c fabricates.
            let trigname = if is_primary {
                "PK_ConstraintTrigger"
            } else {
                "Unique_ConstraintTrigger"
            };
            let stmt = CreateTrigStmt {
                replace: false,
                isconstraint: true,
                trigname: Some(mcx::PgString::from_str_in(trigname, mcx)?),
                relation: None,
                funcname: mcx::PgVec::new_in(mcx),
                args: mcx::PgVec::new_in(mcx),
                row: true,
                timing: pt::TRIGGER_TYPE_AFTER,
                events: pt::TRIGGER_TYPE_INSERT | pt::TRIGGER_TYPE_UPDATE,
                columns: mcx::PgVec::new_in(mcx),
                whenClause: None,
                transitionRels: mcx::PgVec::new_in(mcx),
                deferrable: true,
                initdeferred,
                constrrel: None,
            };

            // CreateTrigger(stmt, NULL, relOid, InvalidOid, conOid, indexOid,
            //               unique_key_recheck, InvalidOid, NULL, true, false).
            CreateTrigger(
                mcx,
                &stmt,
                "",
                rel_oid,
                InvalidOid,
                constraint_oid,
                index_oid,
                F_UNIQUE_KEY_RECHECK,
                InvalidOid,
                true,
                false,
            )?;
            Ok(())
        },
    );
}
