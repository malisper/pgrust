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
//! Genuinely-unported STOP boundaries (loud `PgError`): the WHEN-clause
//! transform (needs a Trigger pstate + `transformWhereClause` + a `pg_node_tree`
//! image), REFERENCING transition tables (no `TriggerTransition` node), user
//! `CREATE CONSTRAINT TRIGGER` (the `pg_constraint` entry), and the
//! partitioned-table FOR EACH ROW fan-out (`RelationGetPartitionDesc`).

use mcx::Mcx;
use types_acl::acl::{ACL_EXECUTE, ACL_TRIGGER, ACLCHECK_OK};
use types_catalog::catalog_dependency::{
    ObjectAddress, DEPENDENCY_AUTO, DEPENDENCY_INTERNAL, DEPENDENCY_NORMAL,
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
        rel_oid,
        ref_rel_oid,
        constraint_oid,
        index_oid,
        funcoid,
        parent_trigger_oid,
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
    rel_oid: Oid,
    ref_rel_oid: Oid,
    constraint_oid: Oid,
    index_oid: Oid,
    mut funcoid: Oid,
    parent_trigger_oid: Oid,
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

    // Partitioned FOR EACH ROW fan-out is unported.
    if !is_internal && stmt.row && relkind == RELKIND_PARTITIONED_TABLE {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("FOR EACH ROW triggers on partitioned tables are not yet supported")
            .into_error());
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

    // REFERENCING transition tables: not yet ported (no TriggerTransition node).
    if !stmt.transitionRels.is_empty() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("REFERENCING transition tables are not yet supported")
            .into_error());
    }

    // WHEN clause: not yet ported on the create path.
    if stmt.whenClause.is_some() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("trigger WHEN conditions are not yet supported")
            .into_error());
    }

    // User CREATE CONSTRAINT TRIGGER (its own pg_constraint entry) is unported.
    if stmt.isconstraint && !valid(constraint_oid) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("user-defined constraint triggers are not yet supported")
            .into_error());
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
        tgqual: None,
        tgoldtable: None,
        tgnewtable: None,
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
    }

    for &col in &columns {
        recordDependencyOn(
            mcx,
            &myself,
            &addr(RELATION_RELATION_ID, relid, col as i32),
            DEPENDENCY_NORMAL,
        )?;
    }

    // InvokeObjectPostCreateHookArg(TriggerRelationId, trigoid, 0, isInternal)
    // (a no-op without an installed object-access hook).
    backend_catalog_objectaccess_seams::invoke_object_post_create_hook_arg::call(
        pt::TriggerRelationId,
        trigoid,
        0,
        is_internal,
    )?;

    // Keep lock on target rel until end of xact.
    rel.close(NoLock)?;

    Ok(myself)
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
        |mcx, parsetree, _query_string| match parsetree.as_createtrigstmt() {
            Some(stmt) => CreateTrigger(
                mcx,
                stmt,
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
