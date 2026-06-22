//! `commands/tablecmds.c` — ALTER TABLE ALTER COLUMN TYPE family.
//!
//! PORTED here (faithful, 100% C logic):
//!   - `ATColumnChangeRequiresRewrite` (tablecmds.c:14678) — decide whether the
//!     column type change needs a heap rewrite (pure expr walk).
//!   - `ATPrepAlterColumnType` (tablecmds.c:14373) — phase-1 prep: validate the
//!     target type, build the USING/cast transform, queue the `NewColumnValue`,
//!     and decide the rewrite flag; recurse to children.
//!   - `find_composite_type_dependencies` (tablecmds.c:6936) — reject a column
//!     type change that would break a stored composite/rowtype user.
//!   - `ATExecAlterColumnType` (tablecmds.c:14725) — phase-2 catalog leg: update
//!     `pg_attribute` (atttypid/atttypmod/attcollation/...), swap the datatype +
//!     collation dependencies, drop stale statistics, re-store the default.
//!
//! NOT yet landed (faithful `unported(...)` seam-and-panic at the exact C call
//! site, never `todo!`/`unimplemented!`/fake output):
//!   - The phase-3 heap rewrite (`ATRewriteTable`, consuming `tab->rewrite` /
//!     `tab->newvals`) is unported — see `at_phase::ATRewriteTables`. A
//!     non-binary-coercible type change (one that sets `AT_REWRITE_COLUMN_REWRITE`)
//!     stops there.
//!   - The dependent-object rebuild (`ATPostAlterTypeCleanup` /
//!     `ATPostAlterTypeParse` / `RememberConstraint/Index/StatisticsForRebuilding`)
//!     IS ported: a rewriting type change rebuilds dependent indexes and
//!     constraints (incl. UNIQUE/PK, replica-identity / cluster restore) by
//!     deparsing + re-parsing them and queuing `AT_ReAdd*` work-queue entries.
//!     The remaining loud stops inside it are at exact C sites whose substrate
//!     is unported: `TryReuseIndex`/`TryReuseForeignKey` (the non-rewriting
//!     relfilenumber/FK-revalidation reuse), the domain-constraint leg
//!     (`getBaseType`), and `AT_ReAddDomainConstraint`.
//!   - The `atthasmissing` array repack and the recurse-to-children remap of a
//!     USING expression stop loudly where their substrate (`construct_array` /
//!     `map_variable_attnos`) would be exercised on a path we cannot yet verify.
//!
//! The binary-coercible, no-dependent case (`pg_attribute` update + datatype /
//! collation dependency swap + default re-coerce) runs end-to-end.

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use mcx::{Mcx, PgVec};

use types_catalog::catalog_dependency::{ObjectAddress, DEPENDENCY_NORMAL};
use types_catalog::pg_attribute::{
    Anum_pg_attribute_attalign, Anum_pg_attribute_attbyval, Anum_pg_attribute_attcollation,
    Anum_pg_attribute_attgenerated, Anum_pg_attribute_atthasdef, Anum_pg_attribute_atthasmissing,
    Anum_pg_attribute_attinhcount, Anum_pg_attribute_attlen, Anum_pg_attribute_attmissingval,
    Anum_pg_attribute_attnotnull, Anum_pg_attribute_attnum, Anum_pg_attribute_atttypid,
    Anum_pg_attribute_atttypmod, AttributeRelationId, PgAttributeUpdateRow,
};
use types_catalog::pg_attrdef::AttrDefaultRelationId;
use types_catalog::pg_collation::CollationRelationId;
use types_catalog::pg_policy::PolicyRelationId;
use types_catalog::pg_proc::ProcedureRelationId;
use types_catalog::pg_publication::PublicationRelRelationId;
use types_catalog::pg_rewrite::RewriteRelationId;
use types_catalog::pg_statistic_ext::StatisticExtRelationId;
use types_catalog::pg_trigger::TriggerRelationId;
use types_catalog::pg_type::TypeRelationId;
use types_core::catalog::CONSTRAINT_RELATION_ID as ConstraintRelationId;
use types_core::primitive::{AttrNumber, InvalidOid, Oid, OidIsValid};
use types_error::{
    PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_COLUMN_DEFINITION, ERRCODE_INVALID_TABLE_DEFINITION,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
    ERRCODE_UNDEFINED_COLUMN, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_nodes::ddlnodes::{AlterTableCmd, AlterTableType, CoercionContext};
use types_nodes::nodes::Node;
use types_nodes::primnodes::{CoercionForm, Expr};
use types_rel::Relation;
use types_storage::lock::{AccessShareLock, NoLock, RowExclusiveLock, LOCKMODE};
use types_tuple::access::{
    ATTRIBUTE_GENERATED_STORED, ATTRIBUTE_GENERATED_VIRTUAL, RELKIND_COMPOSITE_TYPE,
    RELKIND_FOREIGN_TABLE, RELKIND_INDEX, RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE,
    RELKIND_RELATION, RELKIND_SEQUENCE,
};
use types_tuple::heaptuple::{FirstLowInvalidHeapAttributeNumber, InvalidCompressionMethod};

use backend_access_common_relation::relation_open;
use backend_access_transam_xact::CommandCounterIncrement;
use backend_catalog_aclchk_seams as aclchk_seam;
use backend_catalog_heap::{
    CheckAttributeType, RelationClearMissing, RemoveStatistics, CHKATYPE_IS_VIRTUAL,
    RELKIND_HAS_PARTITIONS, RELKIND_HAS_STORAGE,
};
use backend_catalog_indexing_seams as indexing_seam;
use backend_catalog_pg_attrdef::{RemoveAttrDefault, StoreAttrDefault};
use backend_catalog_pg_depend_seams as pg_depend_seam;
use backend_catalog_pg_inherits::find_inheritance_children;
use backend_nodes_core::makefuncs::make_var;
use backend_nodes_core::nodefuncs::{expr_type, strip_implicit_coercions};
use backend_parser_parse_collate::assign_expr_collations_in;
use backend_rewrite_rewritehandler_seams as rewrite_seam;
use backend_utils_cache_lsyscache::relation::get_rel_relkind;
use backend_utils_cache_syscache::{
    SearchSysCacheAttName, SearchSysCacheCopyAttName, SysCacheGetAttrNotNull, ATTNAME,
};
use backend_utils_init_miscinit::GetUserId;

use crate::at_coladd::{add_column_collation_dependency, add_column_datatype_dependency};
use crate::at_phase::{
    AlteredTableInfo, AlterTableUtilityContext, CheckAlterTableIsSafe,
};
use crate::helpers::{here, RelationRelationId};

use backend_catalog_dependency_seams as dep_seam;
use backend_commands_tablecmds_seams as seam;

/// `AT_REWRITE_COLUMN_REWRITE` (tablecmds.c) — the column-rewrite reason bit.
const AT_REWRITE_COLUMN_REWRITE: i32 = 0x04;

/// Faithful seam-and-panic for an unported ALTER COLUMN TYPE leg. We mirror the
/// C structure up to this point and stop loudly rather than `todo!()` or fake.
fn unported(what: &str) -> ! {
    panic!(
        "ALTER TABLE ALTER COLUMN TYPE: {what} is not yet ported in \
         backend-commands-tablecmds (faithful seam-and-panic — see at_altertype.rs)"
    );
}

/// `object_address_subset(addr, classId, objectId, sub)` (objectaddress.h).
fn object_address_subset(class_id: Oid, object_id: Oid, sub: i32) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: sub,
    }
}

// ===========================================================================
// ATColumnChangeRequiresRewrite (tablecmds.c:14678)
// ===========================================================================

/// `F_TIMESTAMP_TIMESTAMPTZ` (utils/fmgroids.h) — `timestamp_timestamptz(timestamp)`.
const F_TIMESTAMP_TIMESTAMPTZ: Oid = 2027;
/// `F_TIMESTAMPTZ_TIMESTAMP` (utils/fmgroids.h) — `timestamptz_timestamp(timestamptz)`.
const F_TIMESTAMPTZ_TIMESTAMP: Oid = 2028;

/// `ATColumnChangeRequiresRewrite(Node *expr, AttrNumber varattno)`
/// (tablecmds.c:14678). When the data type of a column is changed, a rewrite
/// might not be required if the new type is sufficiently identical to the old
/// one and the USING clause isn't inserting some other value.
fn ATColumnChangeRequiresRewrite(expr: &Expr, varattno: AttrNumber) -> PgResult<bool> {
    let mut cur = expr;
    loop {
        match cur {
            // only one varno, so no need to check that
            Expr::Var(v) => {
                if v.varattno == varattno {
                    return Ok(false);
                }
                return Ok(true);
            }
            Expr::RelabelType(r) => {
                cur = r.arg.as_deref().expect("RelabelType.arg is NULL");
            }
            Expr::CoerceToDomain(d) => {
                if backend_utils_cache_typcache_seams::domain_has_constraints::call(d.resulttype)? {
                    return Ok(true);
                }
                cur = d.arg.as_deref().expect("CoerceToDomain.arg is NULL");
            }
            Expr::FuncExpr(f) => {
                // The only no-rewrite FuncExpr cases are the
                // timestamp<->timestamptz pair when the session timezone makes
                // the conversion a no-op (TimestampTimestampTzRequiresRewrite);
                // every other function requires a rewrite.
                match f.funcid {
                    F_TIMESTAMPTZ_TIMESTAMP | F_TIMESTAMP_TIMESTAMPTZ => {
                        if backend_utils_adt_timestamp_seams::timestamp_timestamptz_requires_rewrite::call() {
                            return Ok(true);
                        }
                        // expr = linitial(f->args)
                        cur = f
                            .args
                            .first()
                            .expect("timestamp<->timestamptz FuncExpr has no args");
                    }
                    _ => return Ok(true),
                }
            }
            _ => return Ok(true),
        }
    }
}

// ===========================================================================
// find_composite_type_dependencies (tablecmds.c:6936)
// ===========================================================================

/// `find_composite_type_dependencies(Oid typeOid, Relation origRelation,
/// const char *origTypeName)` (tablecmds.c:6936). Scan pg_depend for things
/// that depend on `typeOid`; reject the type change if a relation with storage
/// (or a partitioned relation) has a stored column of the type.
pub(crate) fn find_composite_type_dependencies<'mcx>(
    mcx: Mcx<'mcx>,
    type_oid: Oid,
    orig_relation: Option<&Relation<'mcx>>,
    orig_type_name: Option<&str>,
) -> PgResult<()> {
    // since this function recurses, it could be driven to stack overflow
    backend_utils_misc_stack_depth::check_stack_depth()?;

    // We scan pg_depend to find those things that depend on the given type.
    // (We assume we can ignore refobjsubid for a type.)
    let rows = pg_depend_seam::scan_type_referers::call(mcx, type_oid)?;

    for row in rows.iter() {
        // Check for directly dependent types.
        if row.classid == TypeRelationId {
            // An array, domain, or range containing the given type; recurse.
            find_composite_type_dependencies(mcx, row.objid, orig_relation, orig_type_name)?;
            continue;
        }

        // Else, ignore dependees that aren't relations.
        if row.classid != RelationRelationId {
            continue;
        }

        let rel = relation_open(mcx, row.objid, AccessShareLock)?;
        let tupdesc = &rel.rd_att;

        // If objsubid identifies a specific column, refer to that; otherwise
        // search for a user column of the type.
        let att_idx: Option<usize> = if row.objsubid > 0 && (row.objsubid as i32) <= tupdesc.natts {
            Some((row.objsubid - 1) as usize)
        } else {
            let mut found: Option<usize> = None;
            for attno in 1..=tupdesc.natts {
                let att = tupdesc.attr((attno - 1) as usize);
                if att.atttypid == type_oid && !att.attisdropped {
                    found = Some((attno - 1) as usize);
                    break;
                }
            }
            found
        };

        let Some(att_idx) = att_idx else {
            // No such column, so assume OK.
            rel.close(AccessShareLock)?;
            continue;
        };

        let att = tupdesc.attr(att_idx);
        let att_name = String::from_utf8_lossy(att.attname.name_str()).into_owned();
        let relkind = rel.rd_rel.relkind;

        // We definitely reject if the relation has storage; partitioned rels too.
        if RELKIND_HAS_STORAGE(relkind) || RELKIND_HAS_PARTITIONS(relkind) {
            let dependent_name = rel.name().to_string();
            if let Some(orig_type_name) = orig_type_name {
                return backend_utils_error::ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg(format!(
                        "cannot alter type \"{orig_type_name}\" because column \"{dependent_name}.{att_name}\" uses it"
                    ))
                    .finish(here("find_composite_type_dependencies"))
                    .map(|()| unreachable!());
            }
            // origTypeName is NULL here, so origRelation is non-NULL (the C
            // contract: callers pass exactly one of the two).
            let orig_relation = orig_relation
                .expect("find_composite_type_dependencies: NULL origRelation with NULL origTypeName");
            if orig_relation.rd_rel.relkind == RELKIND_COMPOSITE_TYPE {
                return backend_utils_error::ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg(format!(
                        "cannot alter type \"{}\" because column \"{dependent_name}.{att_name}\" uses it",
                        orig_relation.name()
                    ))
                    .finish(here("find_composite_type_dependencies"))
                    .map(|()| unreachable!());
            } else if orig_relation.rd_rel.relkind == RELKIND_FOREIGN_TABLE {
                return backend_utils_error::ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg(format!(
                        "cannot alter foreign table \"{}\" because column \"{dependent_name}.{att_name}\" uses its row type",
                        orig_relation.name()
                    ))
                    .finish(here("find_composite_type_dependencies"))
                    .map(|()| unreachable!());
            } else {
                return backend_utils_error::ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg(format!(
                        "cannot alter table \"{}\" because column \"{dependent_name}.{att_name}\" uses its row type",
                        orig_relation.name()
                    ))
                    .finish(here("find_composite_type_dependencies"))
                    .map(|()| unreachable!());
            }
        } else if OidIsValid(rel.rd_rel.reltype) {
            // A view or composite type itself isn't a problem, but we must
            // recursively check for indirect dependencies via its rowtype.
            let reltype = rel.rd_rel.reltype;
            find_composite_type_dependencies(mcx, reltype, orig_relation, orig_type_name)?;
        }

        rel.close(AccessShareLock)?;
    }

    Ok(())
}

// ===========================================================================
// ATPrepAlterColumnType (tablecmds.c:14373)
// ===========================================================================

/// `ATPrepAlterColumnType(wqueue, tab, rel, recurse, recursing, cmd, lockmode,
/// context)` (tablecmds.c:14373).
pub fn ATPrepAlterColumnType<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    ti: usize,
    rel: &Relation<'mcx>,
    recurse: bool,
    recursing: bool,
    cmd: &AlterTableCmd<'mcx>,
    lockmode: LOCKMODE,
    context: &AlterTableUtilityContext<'_>,
) -> PgResult<()> {
    let col_name = cmd
        .name
        .as_ref()
        .map(|s| s.as_str())
        .expect("ALTER COLUMN TYPE: cmd.name is NULL");
    let def = cmd
        .def
        .as_deref()
        .expect("ALTER COLUMN TYPE: cmd.def is NULL")
        .expect_columndef();
    let type_name = def
        .typeName
        .as_deref()
        .expect("ALTER COLUMN TYPE: ColumnDef.typeName is NULL");
    // def->cooked_default — the transformed USING expression, if any.
    let transform_node: Option<&Node<'mcx>> = def.cooked_default.as_deref();

    let location = def.location;

    // pstate->p_sourcetext = context->queryString (used by errposition).
    let query_string = context.query_string;

    if OidIsValid(rel.rd_rel.reltype)
        && reloftype_of(rel.rd_id)? != InvalidOid
        && !recursing
    {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg("cannot alter column type of typed table".to_string())
            .errposition(errpos(query_string, location))
            .finish(here("ATPrepAlterColumnType"))
            .map(|()| unreachable!());
    }

    // lookup the attribute so we can check inheritance status
    let tuple = SearchSysCacheAttName(mcx, rel.rd_id, col_name)?;
    let Some(tuple) = tuple else {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_COLUMN)
            .errmsg(format!(
                "column \"{}\" of relation \"{}\" does not exist",
                col_name,
                rel.name()
            ))
            .errposition(errpos(query_string, location))
            .finish(here("ATPrepAlterColumnType"))
            .map(|()| unreachable!());
    };

    let attnum =
        SysCacheGetAttrNotNull(mcx, ATTNAME, &tuple, Anum_pg_attribute_attnum as i32)?.as_i16();
    let atttypid =
        SysCacheGetAttrNotNull(mcx, ATTNAME, &tuple, Anum_pg_attribute_atttypid as i32)?.as_oid();
    let atttypmod =
        SysCacheGetAttrNotNull(mcx, ATTNAME, &tuple, Anum_pg_attribute_atttypmod as i32)?.as_i32();
    let attcollation =
        SysCacheGetAttrNotNull(mcx, ATTNAME, &tuple, Anum_pg_attribute_attcollation as i32)?
            .as_oid();
    let attgenerated =
        SysCacheGetAttrNotNull(mcx, ATTNAME, &tuple, Anum_pg_attribute_attgenerated as i32)?
            .as_char();
    let attinhcount =
        SysCacheGetAttrNotNull(mcx, ATTNAME, &tuple, Anum_pg_attribute_attinhcount as i32)?
            .as_i16();

    // Can't alter a system attribute.
    if attnum <= 0 {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("cannot alter system column \"{col_name}\""))
            .errposition(errpos(query_string, location))
            .finish(here("ATPrepAlterColumnType"))
            .map(|()| unreachable!());
    }

    // Cannot specify USING when altering type of a generated column.
    if attgenerated != 0 && transform_node.is_some() {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_DEFINITION)
            .errmsg("cannot specify USING when altering type of generated column".to_string())
            .errdetail(format!("Column \"{col_name}\" is a generated column."))
            .errposition(errpos(query_string, location))
            .finish(here("ATPrepAlterColumnType"))
            .map(|()| unreachable!());
    }

    // Don't alter inherited columns (at outer level).
    if attinhcount > 0 && !recursing {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
            .errmsg(format!("cannot alter inherited column \"{col_name}\""))
            .errposition(errpos(query_string, location))
            .finish(here("ATPrepAlterColumnType"))
            .map(|()| unreachable!());
    }

    // Don't alter columns used in the partition key.
    {
        let singleton = crate::at_coldrop::bms_make_singleton(
            (attnum as i32) - (FirstLowInvalidHeapAttributeNumber as i32),
        );
        let (is_part_attr, _is_expr) =
            backend_catalog_partition_seams::has_partition_attrs::call(mcx, rel, Some(&singleton))?;
        if is_part_attr {
            return backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                .errmsg(format!(
                    "cannot alter column \"{}\" because it is part of the partition key of relation \"{}\"",
                    col_name,
                    rel.name()
                ))
                .errposition(errpos(query_string, location))
                .finish(here("ATPrepAlterColumnType"))
                .map(|()| unreachable!());
        }
    }

    // Look up the target type. C passes `pstate` (p_sourcetext = queryString)
    // to typenameTypeIdAndMod so type-lookup errors (e.g. "type x does not
    // exist") carry a parse cursor at typeName->location. The seam resolves
    // with a NULL pstate, so re-attach the cursor here, mirroring the internal
    // parser_errposition(pstate, typeName->location).
    let (targettype, targettypmod) =
        seam::typename_type_id_and_mod::call(mcx, type_name)
            .map_err(|e| attach_errpos(e, query_string, type_name.location))?;

    // ACL_USAGE on the target type.
    let aclresult =
        aclchk_seam::object_aclcheck::call(TypeRelationId, targettype, GetUserId(), ACL_USAGE)?;
    if aclresult != types_acl::ACLCHECK_OK {
        aclchk_seam::aclcheck_error_type::call(aclresult, targettype)?;
    }

    // And the collation. C passes pstate to GetColumnDefCollation, so the
    // "collations are not supported by type ..." error carries a cursor at
    // the COLLATE clause (def->collClause->location), falling back to
    // def->location when there is no COLLATE clause. Re-attach the cursor
    // from the same location since the seam runs with a NULL pstate.
    let coll_location = def
        .collClause
        .as_deref()
        .map(|cc| cc.location)
        .unwrap_or(location);
    let targetcollid = seam::get_column_def_collation::call(mcx, def, targettype)
        .map_err(|e| attach_errpos(e, query_string, coll_location))?;

    // Make sure datatype is legal for a column.
    let flags = if attgenerated == ATTRIBUTE_GENERATED_VIRTUAL {
        CHKATYPE_IS_VIRTUAL
    } else {
        0
    };
    let mut containing = vec![rel.rd_rel.reltype];
    CheckAttributeType(mcx, col_name, targettype, targetcollid, &mut containing, flags)?;

    let relkind = wqueue[ti].relkind;

    if attgenerated == ATTRIBUTE_GENERATED_VIRTUAL {
        // do nothing
    } else if relkind == RELKIND_RELATION || relkind == RELKIND_PARTITIONED_TABLE {
        // Set up an expression to transform the old data value to the new type.
        // If a USING option was given, use the transformed expression; else just
        // take the old value and try to coerce it.
        let transform: Expr = match transform_node {
            Some(n) => n.as_expr().expect("USING transform is not an Expr").clone_in(mcx)?,
            None => Expr::Var(make_var(1, attnum, atttypid, atttypmod, attcollation, 0)),
        };

        let src_type = expr_type(Some(&transform))?;
        let coerced = backend_parser_coerce::coerce_to_target_type(
            mcx,
            None,
            transform.erase_lifetime(),
            src_type,
            targettype,
            targettypmod,
            CoercionContext::COERCION_ASSIGNMENT,
            CoercionForm::COERCE_IMPLICIT_CAST,
            -1,
        )?;
        let Some(coerced) = coerced else {
            // error text depends on whether USING was specified or not
            if def.cooked_default.is_some() {
                return backend_utils_error::ereport(ERROR)
                    .errcode(ERRCODE_DATATYPE_MISMATCH)
                    .errmsg(format!(
                        "result of USING clause for column \"{}\" cannot be cast automatically to type {}",
                        col_name,
                        format_type_be(mcx, targettype)?
                    ))
                    .errhint("You might need to add an explicit cast.".to_string())
                    .finish(here("ATPrepAlterColumnType"))
                    .map(|()| unreachable!());
            } else {
                let mut b = backend_utils_error::ereport(ERROR)
                    .errcode(ERRCODE_DATATYPE_MISMATCH)
                    .errmsg(format!(
                        "column \"{}\" cannot be cast automatically to type {}",
                        col_name,
                        format_type_be(mcx, targettype)?
                    ));
                if attgenerated == 0 {
                    b = b.errhint(format!(
                        "You might need to specify \"USING {}::{}\".",
                        quote_identifier(mcx, col_name)?,
                        format_type_with_typemod(mcx, targettype, targettypmod)?
                    ));
                }
                return b
                    .finish(here("ATPrepAlterColumnType"))
                    .map(|()| unreachable!());
            }
        };
        // Bring the parser-arena `'static` coercion result into `mcx` for the
        // in-place collation pass below (`Expr` is invariant over its lifetime).
        let mut transform2: Expr<'mcx> = coerced.clone_in(mcx)?;

        // Fix collations after all else. C: assign_expr_collations(pstate,
        // transform) with a NULL-ish utility pstate; the port's in-place Node
        // walker needs an explicit arena, supplied via the `_in` variant
        // (behaviourally identical to `assign_expr_collations(None, ...)`).
        assign_expr_collations_in(mcx, &mut transform2)?;

        // Expand virtual generated columns in the expr. The seam operates over
        // the parser-arena `'static` form (erase in, clone the result back into
        // `mcx` for the `'mcx` planner call below; `Expr` is invariant).
        let expanded = rewrite_seam::expand_generated_columns_in_expr::call(
            mcx,
            Some(transform2.erase_lifetime()),
            rel.rd_id,
            1,
        )?;
        let transform2 = expanded
            .expect("expand_generated_columns_in_expr returned None")
            .clone_in(mcx)?;

        // Plan the expr now so we can accurately assess the need to rewrite.
        let planned = backend_optimizer_plan_planner::expression_planner(mcx, transform2)?;

        // Add a work queue item to make ATRewriteTable update the column contents.
        let requires_rewrite = ATColumnChangeRequiresRewrite(&planned, attnum)?;
        let node = mcx::alloc_in(mcx, Node::mk_expr(mcx, planned)?)?;
        wqueue[ti].newvals.push(crate::at_phase::NewColumnValue {
            attnum,
            expr: Some(node),
            is_generated: false,
        });
        if requires_rewrite {
            wqueue[ti].rewrite |= AT_REWRITE_COLUMN_REWRITE;
        }
    } else if transform_node.is_some() {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("\"{}\" is not a table", rel.name()))
            .finish(here("ATPrepAlterColumnType"))
            .map(|()| unreachable!());
    }

    if !RELKIND_HAS_STORAGE(relkind) || attgenerated == ATTRIBUTE_GENERATED_VIRTUAL {
        // For relations or columns without storage, do this check now. Regular
        // tables will check it later when the table is being rewritten.
        find_composite_type_dependencies(mcx, rel.rd_rel.reltype, Some(rel), None)?;
    }

    // (ReleaseSysCache(tuple) — the FormedTuple drops at end of scope.)

    // Recurse manually by queueing a new command for each child, if
    // necessary. We cannot apply ATSimpleRecursion here because we need to
    // remap attribute numbers in the USING expression, if any.
    //
    // If we are told not to recurse, there had better not be any child
    // tables; else the alter would put them out of step.
    if recurse {
        let (child_oids, child_numparents) =
            backend_catalog_pg_inherits::find_all_inheritors(mcx, rel.rd_id, lockmode, true)?;
        // want_numparents=true always returns Some.
        let child_numparents =
            child_numparents.expect("find_all_inheritors did not return numparents");

        // forboth(lo, child_oids, li, child_numparents)
        for (&childrelid, &numparents) in child_oids.iter().zip(child_numparents.iter()) {
            if childrelid == rel.rd_id {
                continue;
            }
            // find_all_inheritors already got lock.
            let childrel = relation_open(mcx, childrelid, NoLock)?;
            CheckAlterTableIsSafe(&childrel)?;

            // Verify that the child doesn't have any inherited definitions of
            // this column that came from outside this inheritance hierarchy.
            // (renameatt makes a similar test, though in a different way
            // because of its different recursion mechanism.)
            let childtuple = SearchSysCacheAttName(mcx, childrel.rd_id, col_name)?;
            let Some(childtuple) = childtuple else {
                return backend_utils_error::ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_COLUMN)
                    .errmsg(format!(
                        "column \"{}\" of relation \"{}\" does not exist",
                        col_name,
                        childrel.name()
                    ))
                    .finish(here("ATPrepAlterColumnType"))
                    .map(|()| unreachable!());
            };

            let child_attinhcount = SysCacheGetAttrNotNull(
                mcx,
                ATTNAME,
                &childtuple,
                Anum_pg_attribute_attinhcount as i32,
            )?
            .as_i16();

            if (child_attinhcount as i32) > numparents {
                return backend_utils_error::ereport(ERROR)
                    .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                    .errmsg(format!(
                        "cannot alter inherited column \"{}\" of relation \"{}\"",
                        col_name,
                        childrel.name()
                    ))
                    .finish(here("ATPrepAlterColumnType"))
                    .map(|()| unreachable!());
            }

            // (ReleaseSysCache(childtuple) — childtuple drops at end of loop.)

            // Build the per-child subcommand. C scribbles on a copyObject(cmd);
            // here we clone the original cmd and, when a USING expression was
            // specified, remap its attribute numbers for the child.
            let mut childcmd = cmd.clone_in(mcx)?;

            // Remap the attribute numbers. If no USING expression was
            // specified, there is no need for this step.
            if let Some(cooked) = def.cooked_default.as_deref() {
                let attmap = backend_access_common_next::attmap::build_attrmap_by_name(
                    mcx,
                    &childrel.rd_att,
                    &rel.rd_att,
                    false,
                )?;
                let cooked_clone = cooked.clone_in(mcx)?;
                let cooked_ptr = mcx::alloc_in(mcx, cooked_clone)?;
                let (mapped, found_whole_row) =
                    backend_rewrite_rewritemanip_seams::map_variable_attnos_node::call(
                        mcx,
                        cooked_ptr,
                        1,
                        0,
                        &attmap.attnums,
                        InvalidOid,
                    )?;
                if found_whole_row {
                    return backend_utils_error::ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("cannot convert whole-row table reference".to_string())
                        .errdetail(
                            "USING expression contains a whole-row table reference.".to_string(),
                        )
                        .finish(here("ATPrepAlterColumnType"))
                        .map(|()| unreachable!());
                }
                let child_def = childcmd
                    .def
                    .as_deref_mut()
                    .expect("ALTER COLUMN TYPE child cmd.def is NULL")
                    .expect_columndef_mut();
                child_def.cooked_default = Some(mapped);
            }

            crate::at_phase::ATPrepCmd(
                mcx,
                wqueue,
                &childrel,
                &childcmd,
                false,
                true,
                lockmode,
                context,
            )?;
            childrel.close(NoLock)?;
        }
    } else if !recursing
        && !find_inheritance_children(mcx, rel.rd_id, NoLock)?.is_empty()
    {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
            .errmsg(format!(
                "type of inherited column \"{col_name}\" must be changed in child tables too"
            ))
            .finish(here("ATPrepAlterColumnType"))
            .map(|()| unreachable!());
    }

    if relkind == RELKIND_COMPOSITE_TYPE {
        crate::at_phase::ATTypedTableRecursion(mcx, wqueue, rel, cmd, lockmode, context)?;
    }

    Ok(())
}

// ===========================================================================
// RememberAllDependentForRebuilding (tablecmds.c:15042)
// ===========================================================================

/// `RememberAllDependentForRebuilding(tab, subtype, rel, attnum, colName)`
/// (tablecmds.c:15042). Subroutine for `ATExecAlterColumnType` and
/// `ATExecSetExpression`: find everything that depends on the column
/// (constraints, indexes, etc) and record enough information to recreate it.
///
/// The dependent-object rebuild itself (`ATPostAlterTypeCleanup`) is unported,
/// so any index / constraint / extended-stats dependent stops loudly here. The
/// function/view/rule/trigger/policy/publication/generated-column punts are
/// errors only for `AT_AlterColumnType` (per the C `if (subtype ==
/// AT_AlterColumnType)` guards); for `AT_SetExpression` those dependents are
/// tolerated (no rebuild needed — only the generation expression changed).
fn RememberAllDependentForRebuilding<'mcx>(
    mcx: Mcx<'mcx>,
    tab: &mut AlteredTableInfo<'mcx>,
    subtype: AlterTableType,
    rel: &Relation<'mcx>,
    attnum: AttrNumber,
    col_name: &str,
) -> PgResult<()> {
    debug_assert!(
        subtype == AlterTableType::AT_AlterColumnType
            || subtype == AlterTableType::AT_SetExpression
    );
    let is_alter_type = subtype == AlterTableType::AT_AlterColumnType;

    // C scans pg_depend by (refclassid=pg_class, refobjid=relid,
    // refobjsubid=attnum) — every object depending on this specific column.
    let rows = pg_depend_seam::scan_column_referers::call(mcx, rel.rd_id, attnum)?;
    for row in rows.iter() {
        match row.classid {
            x if x == RelationRelationId => {
                let rel_kind = get_rel_relkind(row.objid)?;
                if rel_kind == RELKIND_INDEX || rel_kind == RELKIND_PARTITIONED_INDEX {
                    RememberIndexForRebuilding(mcx, row.objid, tab)?;
                } else if rel_kind == RELKIND_SEQUENCE {
                    // SERIAL column's sequence — nothing to do.
                } else {
                    // C: elog(ERROR, "unexpected object depending on column").
                    return Err(types_error::PgError::error(
                        "unexpected object depending on column",
                    ));
                }
            }
            x if x == ConstraintRelationId => {
                RememberConstraintForRebuilding(mcx, row.objid, tab)?;
            }
            x if x == ProcedureRelationId => {
                if is_alter_type {
                    return feature_not_supported(
                        mcx,
                        "cannot alter type of a column used by a function or procedure",
                        &found_object_of(row),
                        col_name,
                    );
                }
            }
            x if x == RewriteRelationId => {
                if is_alter_type {
                    return feature_not_supported(
                        mcx,
                        "cannot alter type of a column used by a view or rule",
                        &found_object_of(row),
                        col_name,
                    );
                }
            }
            x if x == TriggerRelationId => {
                if is_alter_type {
                    return feature_not_supported(
                        mcx,
                        "cannot alter type of a column used in a trigger definition",
                        &found_object_of(row),
                        col_name,
                    );
                }
            }
            x if x == PolicyRelationId => {
                if is_alter_type {
                    return feature_not_supported(
                        mcx,
                        "cannot alter type of a column used in a policy definition",
                        &found_object_of(row),
                        col_name,
                    );
                }
            }
            x if x == PublicationRelRelationId => {
                if is_alter_type {
                    return feature_not_supported(
                        mcx,
                        "cannot alter type of a column used by a publication WHERE clause",
                        &found_object_of(row),
                        col_name,
                    );
                }
            }
            x if x == StatisticExtRelationId => {
                RememberStatisticsForRebuilding(mcx, row.objid, tab)?;
            }
            x if x == AttrDefaultRelationId => {
                // Could be the column's own default/generation expression
                // (handled by the caller) or a generated column elsewhere in the
                // same table referencing it.
                let col = backend_catalog_pg_attrdef::GetAttrDefaultColumnAddress(mcx, row.objid)?;
                if col.objectId == rel.rd_id && col.objectSubId == attnum as i32 {
                    // Ignore the column's own expression; the caller deals with it.
                } else if is_alter_type {
                    // A generated column elsewhere uses this column — punt.
                    let gen_name = backend_utils_cache_lsyscache::attribute::get_attname(
                        mcx,
                        col.objectId,
                        col.objectSubId as AttrNumber,
                        false,
                    )?
                    .map(|s| s.to_string())
                    .unwrap_or_default();
                    return backend_utils_error::ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg(
                            "cannot alter type of a column used by a generated column".to_string(),
                        )
                        .errdetail(format!(
                            "Column \"{col_name}\" is used by generated column \"{gen_name}\"."
                        ))
                        .finish(here("RememberAllDependentForRebuilding"))
                        .map(|()| unreachable!());
                }
                // For AT_SetExpression a foreign generated-column reference needs
                // no action.
            }
            _ => {
                // Other classes: not relevant (the column's own datatype /
                // collation dependencies are removed by the caller).
            }
        }
    }
    Ok(())
}

/// Build `foundObject` (`{classid, objid, objsubid}`) from a pg_depend referer
/// row, matching the C `ObjectAddress` populated in the dependency scan loop.
fn found_object_of(
    row: &backend_catalog_pg_depend_seams::TypeRefererRow,
) -> types_catalog::catalog_dependency::ObjectAddress {
    types_catalog::catalog_dependency::ObjectAddress {
        classId: row.classid,
        objectId: row.objid,
        objectSubId: row.objsubid,
    }
}

fn feature_not_supported<'mcx>(
    mcx: Mcx<'mcx>,
    msg: &str,
    found_object: &types_catalog::catalog_dependency::ObjectAddress,
    col_name: &str,
) -> PgResult<()> {
    // errdetail("%s depends on column \"%s\"",
    //           getObjectDescription(&foundObject, false), colName)
    let obj_desc =
        backend_catalog_objectaddress_seams::get_object_description::call(mcx, found_object, false)?
            .map(|s| s.to_string())
            .unwrap_or_default();
    backend_utils_error::ereport(ERROR)
        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
        .errmsg(msg.to_string())
        .errdetail(format!("{obj_desc} depends on column \"{col_name}\""))
        .finish(here("RememberAllDependentForRebuilding"))
        .map(|()| unreachable!())
}

// ===========================================================================
// Remember{ReplicaIdentity,ClusterOn,Constraint,Index,Statistics}ForRebuilding
// (tablecmds.c:15265-15418)
// ===========================================================================

/// `CONSTRAINT_NOTNULL` (`catalog/pg_constraint.h`) — `'n'`.
const CONSTRAINT_NOTNULL: u8 = b'n';

/// `RememberReplicaIdentityForRebuilding(indoid, tab)` (tablecmds.c:15269).
fn RememberReplicaIdentityForRebuilding<'mcx>(
    mcx: Mcx<'mcx>,
    indoid: Oid,
    tab: &mut AlteredTableInfo<'mcx>,
) -> PgResult<()> {
    if !backend_utils_cache_lsyscache::relation::get_index_isreplident(indoid)? {
        return Ok(());
    }

    if tab.replicaIdentityIndex.is_some() {
        return Err(types_error::PgError::error(format!(
            "relation {} has multiple indexes marked as replica identity",
            tab.relid
        )));
    }

    let name = backend_utils_cache_lsyscache_seams::get_rel_name::call(mcx, indoid)?;
    tab.replicaIdentityIndex = name;
    Ok(())
}

/// `RememberClusterOnForRebuilding(indoid, tab)` (tablecmds.c:15283).
fn RememberClusterOnForRebuilding<'mcx>(
    mcx: Mcx<'mcx>,
    indoid: Oid,
    tab: &mut AlteredTableInfo<'mcx>,
) -> PgResult<()> {
    if !backend_utils_cache_lsyscache::relation::get_index_isclustered(indoid)? {
        return Ok(());
    }

    if tab.clusterOnIndex.is_some() {
        return Err(types_error::PgError::error(format!(
            "relation {} has multiple clustered indexes",
            tab.relid
        )));
    }

    let name = backend_utils_cache_lsyscache_seams::get_rel_name::call(mcx, indoid)?;
    tab.clusterOnIndex = name;
    Ok(())
}

/// `RememberConstraintForRebuilding(conoid, tab)` (tablecmds.c:15300).
fn RememberConstraintForRebuilding<'mcx>(
    mcx: Mcx<'mcx>,
    conoid: Oid,
    tab: &mut AlteredTableInfo<'mcx>,
) -> PgResult<()> {
    // De-dup: don't recreate the same constraint twice, and capture the
    // definition string before any column type change (ruleutils.c gets
    // confused if we ask again later).
    if tab.changedConstraintOids.contains(&conoid) {
        return Ok(());
    }

    // OK, capture the constraint's existing definition string.
    let defstring =
        backend_utils_adt_ruleutils::constraintdef::pg_get_constraintdef_command(mcx, conoid)?;
    let defnode = mcx::alloc_in(
        mcx,
        Node::mk_string(mcx, types_nodes::value::StringNode { sval: defstring })?,
    )?;

    // Create not-null constraints ahead of primary key indexes; otherwise the
    // not-null constraint would be created by the primary key with the wrong
    // name.
    if backend_utils_cache_lsyscache::collation_constraint_language_cast::get_constraint_type(
        conoid,
    )? == CONSTRAINT_NOTNULL
    {
        tab.changedConstraintOids.insert(0, conoid);
        tab.changedConstraintDefs.insert(0, defnode);
    } else {
        tab.changedConstraintOids.push(conoid);
        tab.changedConstraintDefs.push(defnode);
    }

    // For the index of a constraint, if any, remember replica-identity /
    // clustered status so ATPostAlterTypeCleanup can restore it.
    let indoid =
        backend_utils_cache_lsyscache::collation_constraint_language_cast::get_constraint_index(
            conoid,
        )?;
    if OidIsValid(indoid) {
        RememberReplicaIdentityForRebuilding(mcx, indoid, tab)?;
        RememberClusterOnForRebuilding(mcx, indoid, tab)?;
    }
    Ok(())
}

/// `RememberIndexForRebuilding(indoid, tab)` (tablecmds.c:15356).
fn RememberIndexForRebuilding<'mcx>(
    mcx: Mcx<'mcx>,
    indoid: Oid,
    tab: &mut AlteredTableInfo<'mcx>,
) -> PgResult<()> {
    if tab.changedIndexOids.contains(&indoid) {
        return Ok(());
    }

    // If the index belongs to a constraint, rebuild the constraint instead.
    let conoid = pg_depend_seam::get_index_constraint::call(indoid)?;
    if OidIsValid(conoid) {
        return RememberConstraintForRebuilding(mcx, conoid, tab);
    }

    // OK, capture the index's existing definition string.
    let defstring =
        backend_utils_adt_ruleutils::indexdef::pg_get_indexdef_string(mcx, indoid)?;
    let defnode = mcx::alloc_in(
        mcx,
        Node::mk_string(mcx, types_nodes::value::StringNode { sval: defstring })?,
    )?;
    tab.changedIndexOids.push(indoid);
    tab.changedIndexDefs.push(defnode);

    RememberReplicaIdentityForRebuilding(mcx, indoid, tab)?;
    RememberClusterOnForRebuilding(mcx, indoid, tab)?;
    Ok(())
}

/// `RememberStatisticsForRebuilding(stxoid, tab)` (tablecmds.c:15403).
fn RememberStatisticsForRebuilding<'mcx>(
    mcx: Mcx<'mcx>,
    stxoid: Oid,
    tab: &mut AlteredTableInfo<'mcx>,
) -> PgResult<()> {
    if tab.changedStatisticsOids.contains(&stxoid) {
        return Ok(());
    }

    let defstring = backend_utils_adt_ruleutils::statisticsdef::pg_get_statisticsobjdef_string(
        mcx, stxoid,
    )?;
    let defnode = mcx::alloc_in(
        mcx,
        Node::mk_string(mcx, types_nodes::value::StringNode { sval: defstring })?,
    )?;
    tab.changedStatisticsOids.push(stxoid);
    tab.changedStatisticsDefs.push(defnode);
    Ok(())
}

// ===========================================================================
// ATPostAlterTypeCleanup (tablecmds.c:15436)
// ===========================================================================

/// `ATPostAlterTypeCleanup(wqueue, tab, lockmode)` (tablecmds.c:15436) —
/// cleanup after the ALTER TYPE / SET EXPRESSION operations for one relation.
/// Drop and recreate every index/constraint/extended-statistics object that
/// depends on the altered columns: actual dropping happens here, recreation is
/// queued as later work-queue entries (`AT_ReAdd*`).
pub(crate) fn ATPostAlterTypeCleanup<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    ti: usize,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    // Collect all the constraints and indexes to drop so we can process them in
    // a single call (no need to worry about dependencies among them).
    let mut objects = dep_seam::new_object_addresses::call()?;

    let relid = wqueue[ti].relid;
    let rewrite = wqueue[ti].rewrite;

    // Re-parse the constraint definitions and attach them to the proper work
    // queue entries, BEFORE dropping. Snapshot the (oid,def) pairs first; the
    // C `forboth` iterates the saved lists while ATPostAlterTypeParse appends to
    // `tab->subcmds` (later passes) and possibly to `wqueue`.
    let con_pairs: alloc::vec::Vec<(Oid, mcx::PgString<'mcx>)> = wqueue[ti]
        .changedConstraintOids
        .iter()
        .zip(wqueue[ti].changedConstraintDefs.iter())
        .map(|(oid, def)| {
            (
                *oid,
                def.expect_string()
                    .sval
                    .clone_in(mcx)
                    .expect("clone constraint def string"),
            )
        })
        .collect();

    for (old_id, defstr) in con_pairs.into_iter() {
        // con = SearchSysCache1(CONSTROID, oldId); read conrelid/confrelid/
        // conislocal.
        let con = backend_utils_cache_syscache_seams::search_constraint_form_by_oid::call(old_id)?;
        let con = con.ok_or_else(|| {
            types_error::PgError::error(format!("cache lookup failed for constraint {old_id}"))
        })?;
        let conform = con.form;
        let con_relid;
        if OidIsValid(conform.conrelid) {
            con_relid = conform.conrelid;
        } else {
            // Must be a domain constraint:
            // relid = get_typ_typrelid(getBaseType(con->contypid)).
            let base = backend_utils_cache_lsyscache_seams::get_base_type::call(conform.contypid)?;
            con_relid = backend_utils_cache_lsyscache_seams::get_typ_typrelid::call(base)?;
        }
        let confrelid = conform.confrelid;
        let conislocal = conform.conislocal;

        // obj = {ConstraintRelationId, oldId}; add_exact_object_address.
        dep_seam::add_exact_object_address::call(
            object_address_subset(ConstraintRelationId, old_id, 0),
            &mut objects,
        )?;

        // If the constraint is inherited (only), don't inject a new definition;
        // it'll get recreated when the parent's constraint recurses. But we had
        // to carry it this far so we can drop it below.
        if !conislocal {
            continue;
        }

        // Lock the constraint's table if it's not the one we're modifying.
        if con_relid != relid {
            backend_storage_lmgr_lmgr::LockRelationOid(
                con_relid,
                types_storage::lock::AccessExclusiveLock,
            )?;
        }

        ATPostAlterTypeParse(
            mcx, wqueue, old_id, con_relid, confrelid, defstr.as_str(), lockmode, rewrite,
        )?;
    }

    // Re-parse the index definitions.
    let idx_pairs: alloc::vec::Vec<(Oid, mcx::PgString<'mcx>)> = wqueue[ti]
        .changedIndexOids
        .iter()
        .zip(wqueue[ti].changedIndexDefs.iter())
        .map(|(oid, def)| {
            (
                *oid,
                def.expect_string()
                    .sval
                    .clone_in(mcx)
                    .expect("clone index def string"),
            )
        })
        .collect();

    for (old_id, defstr) in idx_pairs.into_iter() {
        let idx_relid = backend_catalog_index::IndexGetRelation(old_id, false)?;

        if idx_relid != relid {
            backend_storage_lmgr_lmgr::LockRelationOid(
                idx_relid,
                types_storage::lock::AccessExclusiveLock,
            )?;
        }

        ATPostAlterTypeParse(
            mcx, wqueue, old_id, idx_relid, InvalidOid, defstr.as_str(), lockmode, rewrite,
        )?;

        dep_seam::add_exact_object_address::call(
            object_address_subset(RelationRelationId, old_id, 0),
            &mut objects,
        )?;
    }

    // Re-parse the extended-statistics definitions.
    let stat_pairs: alloc::vec::Vec<(Oid, mcx::PgString<'mcx>)> = wqueue[ti]
        .changedStatisticsOids
        .iter()
        .zip(wqueue[ti].changedStatisticsDefs.iter())
        .map(|(oid, def)| {
            (
                *oid,
                def.expect_string()
                    .sval
                    .clone_in(mcx)
                    .expect("clone statistics def string"),
            )
        })
        .collect();

    for (old_id, defstr) in stat_pairs.into_iter() {
        // StatisticsGetRelation(oldId, false) (statscmds.c) — statscmds cannot be
        // a direct dep (cycle), so go through the shared syscache projection it
        // itself wraps.
        let stat_relid = backend_utils_cache_syscache_seams::statext_get_relid::call(old_id)?
            .ok_or_else(|| {
                types_error::PgError::error(format!(
                    "cache lookup failed for statistics object {old_id}"
                ))
            })?;

        // ShareUpdateExclusiveLock here (matches CreateStatistics /
        // RemoveStatisticsById); done after all AccessExclusiveLock cases to
        // avoid deadlock from a lock-level promotion.
        if stat_relid != relid {
            backend_storage_lmgr_lmgr::LockRelationOid(
                stat_relid,
                types_storage::lock::ShareUpdateExclusiveLock,
            )?;
        }

        ATPostAlterTypeParse(
            mcx, wqueue, old_id, stat_relid, InvalidOid, defstr.as_str(), lockmode, rewrite,
        )?;

        dep_seam::add_exact_object_address::call(
            object_address_subset(StatisticExtRelationId, old_id, 0),
            &mut objects,
        )?;
    }

    // Queue up a command to restore replica identity index marking.
    if let Some(rep_idx) = wqueue[ti].replicaIdentityIndex.as_ref() {
        let rep_idx = rep_idx.clone_in(mcx)?;
        let subcmd = types_nodes::ddlnodes::ReplicaIdentityStmt {
            identity_type: b'i' as i8, // REPLICA_IDENTITY_INDEX
            name: Some(rep_idx),
        };
        let subnode = mcx::alloc_in(mcx, Node::mk_replica_identity_stmt(mcx, subcmd)?)?;
        let cmd = AlterTableCmd {
            subtype: AlterTableType::AT_ReplicaIdentity,
            name: None,
            num: 0,
            newowner: None,
            def: Some(subnode),
            behavior: types_nodes::parsenodes::DropBehavior::Restrict,
            missing_ok: false,
            recurse: false,
        };
        // do it after indexes and constraints
        let cmdnode = mcx::alloc_in(mcx, Node::mk_alter_table_cmd(mcx, cmd)?)?;
        wqueue[ti].subcmds[crate::at_phase::AT_PASS_OLD_CONSTR as usize].push(cmdnode);
    }

    // Queue up a command to restore marking of index used for cluster.
    if let Some(cl_idx) = wqueue[ti].clusterOnIndex.as_ref() {
        let cl_idx = cl_idx.clone_in(mcx)?;
        let cmd = AlterTableCmd {
            subtype: AlterTableType::AT_ClusterOn,
            name: Some(cl_idx),
            num: 0,
            newowner: None,
            def: None,
            behavior: types_nodes::parsenodes::DropBehavior::Restrict,
            missing_ok: false,
            recurse: false,
        };
        let cmdnode = mcx::alloc_in(mcx, Node::mk_alter_table_cmd(mcx, cmd)?)?;
        wqueue[ti].subcmds[crate::at_phase::AT_PASS_OLD_CONSTR as usize].push(cmdnode);
    }

    // DROP_RESTRICT is fine: nothing else should depend on these objects. The
    // objects get recreated during the subsequent work-queue passes.
    dep_seam::perform_multiple_deletions::call(
        &objects.refs,
        types_nodes::parsenodes::DROP_RESTRICT,
        dep_seam::PERFORM_DELETION_INTERNAL,
    )?;
    dep_seam::free_object_addresses::call(objects)?;
    Ok(())
}

// ===========================================================================
// ATPostAlterTypeParse (tablecmds.c:15628)
// ===========================================================================

/// `ATPostAlterTypeParse(oldId, oldRelId, refRelId, cmd, wqueue, lockmode,
/// rewrite)` (tablecmds.c:15628) — parse the previously-saved definition string
/// for a constraint/index/statistics object against the newly-established
/// column types, and queue the resulting commands for execution.
#[allow(clippy::too_many_arguments)]
fn ATPostAlterTypeParse<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    old_id: Oid,
    old_rel_id: Oid,
    ref_rel_id: Oid,
    cmd: &str,
    _lockmode: LOCKMODE,
    rewrite: i32,
) -> PgResult<()> {
    use types_nodes::nodes::ntag;

    // We expect only ALTER TABLE / CREATE INDEX / CREATE STATISTICS statements;
    // pass them through parse_utilcmd.c (no parse_analyze / rewriter needed).
    // raw_parser needs a 'mcx-lived &str: allocate the command text into the
    // arena and leak it into an honest 'mcx borrow.
    let cmd_box = mcx::alloc_in(mcx, mcx::PgString::from_str_in(cmd, mcx)?)?;
    let cmd_str: &'mcx str = mcx::leak_in(cmd_box).as_str();
    let raw_parsetree_list = backend_parser_driver::raw_parser(
        mcx,
        cmd_str,
        types_parsenodes::RawParseMode::RAW_PARSE_DEFAULT,
    )?;

    // querytree_list: the transformed statements, in execution order.
    let mut querytree_list: PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>> = PgVec::new_in(mcx);
    for rs in raw_parsetree_list.iter() {
        let stmt: &Node<'mcx> = &rs.stmt;
        let stmt_tag = stmt.node_tag();
        match stmt_tag {
            t if t == ntag::T_IndexStmt => {
                let stmt_clone = mcx::alloc_in(mcx, stmt.clone_in(mcx)?)?;
                let transformed = backend_tcop_utility_out_seams::transform_index_stmt::call(
                    mcx, old_rel_id, stmt_clone, cmd,
                )?;
                querytree_list.push(transformed);
            }
            t if t == ntag::T_AlterTableStmt => {
                let stmt_box = mcx::alloc_in(mcx, stmt.clone_in(mcx)?)?;
                let (new_stmt, before_stmts, after_stmts) =
                    backend_parser_parse_utilcmd_seams::transformAlterTableStmt::call(
                        mcx, old_rel_id, stmt_box, cmd,
                    )?;
                for b in before_stmts {
                    querytree_list.push(b);
                }
                querytree_list.push(new_stmt);
                for a in after_stmts {
                    querytree_list.push(a);
                }
            }
            t if t == ntag::T_CreateStatsStmt => {
                let stmt_clone = mcx::alloc_in(mcx, stmt.clone_in(mcx)?)?;
                let transformed = backend_tcop_utility_out_seams::transform_stats_stmt::call(
                    mcx, old_rel_id, stmt_clone, cmd,
                )?;
                querytree_list.push(transformed);
            }
            _ => {
                let cloned = mcx::alloc_in(mcx, stmt.clone_in(mcx)?)?;
                querytree_list.push(cloned);
            }
        }
    }

    // Caller already holds whatever lock we need.
    let rel = relation_open(mcx, old_rel_id, NoLock)?;

    // Attach each generated command to the proper work-queue entry. Note this
    // could create entirely new work-queue entries.
    for stm in querytree_list.iter() {
        let stm_tag = stm.node_tag();
        let tab_idx = crate::at_phase::ATGetQueueEntry(mcx, wqueue, &rel)?;

        if stm_tag == ntag::T_IndexStmt {
            // if (!rewrite) TryReuseIndex(oldId, stmt) — for a non-rewriting
            // ALTER (rewrite == 0) we may be able to reuse the existing index's
            // physical storage (and so preserve its relfilenode/tablespace);
            // a rewriting ALTER (rewrite != 0) rebuilds the index fresh.
            let mut istmt = stm
                .clone_in(mcx)?
                .into_indexstmt()
                .expect("ATPostAlterTypeParse: T_IndexStmt node");
            if rewrite == 0 {
                TryReuseIndex(mcx, old_id, &mut istmt)?;
            }
            istmt.reset_default_tblspc = true;
            // keep the index's comment: idxcomment = GetComment(oldId,
            // RelationRelationId, 0). DefineIndex applies stmt->idxcomment via
            // CreateComments after building the rebuilt index, so a comment on
            // the old index survives ALTER COLUMN TYPE.
            istmt.idxcomment = match backend_commands_tablecmds_seams::get_comment::call(
                mcx,
                old_id,
                RelationRelationId,
                0,
            )? {
                Some(c) => Some(mcx::PgString::from_str_in(&c, mcx)?),
                None => None,
            };

            let newcmd = AlterTableCmd {
                subtype: AlterTableType::AT_ReAddIndex,
                name: None,
                num: 0,
                newowner: None,
                def: Some(mcx::alloc_in(mcx, Node::mk_index_stmt(mcx, istmt)?)?),
                behavior: types_nodes::parsenodes::DropBehavior::Restrict,
                missing_ok: false,
                recurse: false,
            };
            let node = mcx::alloc_in(mcx, Node::mk_alter_table_cmd(mcx, newcmd)?)?;
            wqueue[tab_idx].subcmds[crate::at_phase::AT_PASS_OLD_INDEX as usize].push(node);
        } else if stm_tag == ntag::T_AlterTableStmt {
            let atstmt = stm
                .clone_in(mcx)?
                .into_altertablestmt()
                .expect("ATPostAlterTypeParse: T_AlterTableStmt node");
            for subcmd_node in atstmt.cmds.iter() {
                let subcmd = subcmd_node
                    .as_altertablecmd()
                    .expect("ATPostAlterTypeParse: AlterTableStmt.cmds hold AlterTableCmd");
                match subcmd.subtype {
                    AlterTableType::AT_AddIndex => {
                        let mut indstmt = subcmd
                            .def
                            .as_deref()
                            .expect("AT_AddIndex: def is NULL")
                            .clone_in(mcx)?
                            .into_indexstmt()
                            .expect("AT_AddIndex: def is IndexStmt");
                        // indoid = get_constraint_index(oldId): the index OID
                        // backing the constraint being rebuilt.
                        let indoid =
                            backend_utils_cache_lsyscache::collation_constraint_language_cast::get_constraint_index(
                                old_id,
                            )?;
                        if rewrite == 0 {
                            TryReuseIndex(mcx, indoid, &mut indstmt)?;
                        }
                        // keep any comment on the index:
                        // indstmt->idxcomment = GetComment(indoid, RelationRelationId, 0).
                        indstmt.idxcomment =
                            match backend_commands_tablecmds_seams::get_comment::call(
                                mcx,
                                indoid,
                                RelationRelationId,
                                0,
                            )? {
                                Some(c) => Some(mcx::PgString::from_str_in(&c, mcx)?),
                                None => None,
                            };
                        indstmt.reset_default_tblspc = true;
                        let idxname = indstmt.idxname.as_ref().map(|s| s.to_string());

                        let newcmd = AlterTableCmd {
                            subtype: AlterTableType::AT_ReAddIndex,
                            name: None,
                            num: 0,
                            newowner: None,
                            def: Some(mcx::alloc_in(mcx, Node::mk_index_stmt(mcx, indstmt)?)?),
                            behavior: subcmd.behavior,
                            missing_ok: subcmd.missing_ok,
                            recurse: subcmd.recurse,
                        };
                        let node = mcx::alloc_in(mcx, Node::mk_alter_table_cmd(mcx, newcmd)?)?;
                        wqueue[tab_idx].subcmds[crate::at_phase::AT_PASS_OLD_INDEX as usize]
                            .push(node);

                        // recreate any comment on the constraint:
                        // RebuildConstraintComment(tab, AT_PASS_OLD_INDEX, oldId,
                        //                          rel, NIL, indstmt->idxname).
                        if let Some(idxname) = idxname {
                            RebuildConstraintComment(
                                mcx,
                                wqueue,
                                tab_idx,
                                crate::at_phase::AT_PASS_OLD_INDEX,
                                old_id,
                                Some(&rel),
                                None,
                                &idxname,
                            )?;
                        }
                    }
                    AlterTableType::AT_AddConstraint => {
                        let mut con = subcmd
                            .def
                            .as_deref()
                            .expect("AT_AddConstraint: def is NULL")
                            .clone_in(mcx)?
                            .into_constraint()
                            .expect("AT_AddConstraint: def is Constraint");
                        con.old_pktable_oid = ref_rel_id;
                        // rewriting neither side of a FK → TryReuseForeignKey:
                        // stash the old constraint's conpfeqop operator OIDs (as
                        // an Integer-node list) so the FK re-add can skip
                        // revalidation when the equality operators are unchanged.
                        if con.contype == types_nodes::ddlnodes::ConstrType::CONSTR_FOREIGN
                            && rewrite == 0
                            && wqueue[tab_idx].rewrite == 0
                        {
                            let pfeqops =
                                backend_catalog_pg_constraint::TryReuseForeignKey(mcx, old_id)?;
                            for op in pfeqops {
                                let node = mcx::alloc_in(
                                    mcx,
                                    Node::mk_integer(
                                        mcx,
                                        types_nodes::value::Integer { ival: op as i32 },
                                    )?,
                                )?;
                                con.old_conpfeqop.push(node);
                            }
                        }
                        con.reset_default_tblspc = true;
                        let conname = con.conname.as_ref().map(|s| s.to_string());

                        let newcmd = AlterTableCmd {
                            subtype: AlterTableType::AT_ReAddConstraint,
                            name: None,
                            num: 0,
                            newowner: None,
                            def: Some(mcx::alloc_in(mcx, Node::mk_constraint(mcx, con)?)?),
                            behavior: subcmd.behavior,
                            missing_ok: subcmd.missing_ok,
                            recurse: subcmd.recurse,
                        };
                        let node = mcx::alloc_in(mcx, Node::mk_alter_table_cmd(mcx, newcmd)?)?;
                        wqueue[tab_idx].subcmds[crate::at_phase::AT_PASS_OLD_CONSTR as usize]
                            .push(node);

                        // Recreate any comment on the constraint. If we recreated
                        // a primary key, transformTableConstraint added an unnamed
                        // not-null constraint here; skip in that case.
                        if let Some(name) = conname {
                            RebuildConstraintComment(
                                mcx,
                                wqueue,
                                tab_idx,
                                crate::at_phase::AT_PASS_OLD_CONSTR,
                                old_id,
                                Some(&rel),
                                None,
                                &name,
                            )?;
                        }
                    }
                    other => {
                        return Err(types_error::PgError::error(format!(
                            "unexpected statement subtype: {}",
                            other as i32
                        )));
                    }
                }
            }
        } else if stm_tag == ntag::T_AlterDomainStmt {
            // Domain ADD CONSTRAINT rebuild (tablecmds.c:15783). Reached when an
            // ALTER TYPE ... ALTER ATTRIBUTE rewrites a composite type that a
            // domain is based on: RememberConstraintForRebuilding saved an
            // AlterDomainStmt re-adding the domain's pre-existing CHECK; turn it
            // into an AT_ReAddDomainConstraint command on AT_PASS_OLD_CONSTR.
            let ads = stm
                .as_alterdomainstmt()
                .expect("ATPostAlterTypeParse: T_AlterDomainStmt node");
            if ads.subtype == b'C' as i8 {
                // con->conname is used only to recreate the constraint comment.
                let con = ads
                    .def
                    .as_deref()
                    .expect("AlterDomainStmt.def is NULL")
                    .as_constraint()
                    .expect("AlterDomainStmt.def is not a Constraint");
                let conname = con.conname.as_ref().map(|s| s.to_string());
                // stmt->typeName: domain namelist used by RebuildConstraintComment.
                let mut domname: PgVec<'mcx, mcx::PgString<'mcx>> = PgVec::new_in(mcx);
                for n in ads.typeName.iter() {
                    let s = n
                        .as_string()
                        .expect("AlterDomainStmt.typeName element is not a String");
                    domname.push(mcx::PgString::from_str_in(s.sval.as_str(), mcx)?);
                }

                let stmt_clone = mcx::alloc_in(mcx, stm.clone_in(mcx)?)?;
                let newcmd = AlterTableCmd {
                    subtype: AlterTableType::AT_ReAddDomainConstraint,
                    name: None,
                    num: 0,
                    newowner: None,
                    def: Some(stmt_clone),
                    behavior: types_nodes::parsenodes::DropBehavior::Restrict,
                    missing_ok: false,
                    recurse: false,
                };
                let node = mcx::alloc_in(mcx, Node::mk_alter_table_cmd(mcx, newcmd)?)?;
                wqueue[tab_idx].subcmds[crate::at_phase::AT_PASS_OLD_CONSTR as usize].push(node);

                // recreate any comment on the constraint:
                // RebuildConstraintComment(tab, AT_PASS_OLD_CONSTR, oldId, NULL,
                //                          stmt->typeName, con->conname).
                if let Some(name) = conname {
                    RebuildConstraintComment(
                        mcx,
                        wqueue,
                        tab_idx,
                        crate::at_phase::AT_PASS_OLD_CONSTR,
                        old_id,
                        None,
                        Some(&domname),
                        &name,
                    )?;
                }
            } else {
                return Err(types_error::PgError::error(format!(
                    "unexpected statement subtype: {}",
                    ads.subtype
                )));
            }
        } else if stm_tag == ntag::T_CreateStatsStmt {
            let stmt = stm
                .clone_in(mcx)?
                .into_createstatsstmt()
                .expect("ATPostAlterTypeParse: CreateStatsStmt node");
            // keep the statistics object's comment:
            // stmt->stxcomment = GetComment(oldId, StatisticExtRelationId, 0).
            let mut stmt = stmt;
            stmt.stxcomment = match backend_commands_tablecmds_seams::get_comment::call(
                mcx,
                old_id,
                StatisticExtRelationId,
                0,
            )? {
                Some(c) => Some(mcx::PgString::from_str_in(&c, mcx)?),
                None => None,
            };
            let newcmd = AlterTableCmd {
                subtype: AlterTableType::AT_ReAddStatistics,
                name: None,
                num: 0,
                newowner: None,
                def: Some(mcx::alloc_in(mcx, Node::mk_create_stats_stmt(mcx, stmt)?)?),
                behavior: types_nodes::parsenodes::DropBehavior::Restrict,
                missing_ok: false,
                recurse: false,
            };
            let node = mcx::alloc_in(mcx, Node::mk_alter_table_cmd(mcx, newcmd)?)?;
            wqueue[tab_idx].subcmds[crate::at_phase::AT_PASS_MISC as usize].push(node);
        } else {
            return Err(types_error::PgError::error(format!(
                "unexpected statement type: {}",
                stm_tag
            )));
        }
    }

    rel.close(NoLock)?;
    Ok(())
}

/// `TryReuseIndex(oldId, stmt)` (tablecmds.c:15886) — subroutine for
/// `ATPostAlterTypeParse`. If the existing index `old_id` is compatible enough
/// with the rebuilt definition `stmt` (`CheckIndexCompatible`), stash the old
/// index's relfilenumber (and the subtransaction-id tracking fields) into the
/// `IndexStmt` so `ATExecAddIndex`/`DefineIndex` reuse the existing storage
/// instead of building from scratch — preserving the index's relfilenode and
/// tablespace across a no-rewrite `ALTER COLUMN TYPE`.
fn TryReuseIndex<'mcx>(
    mcx: Mcx<'mcx>,
    old_id: Oid,
    stmt: &mut types_nodes::ddlnodes::IndexStmt<'mcx>,
) -> PgResult<()> {
    let compatible =
        backend_commands_indexcmds_seams::check_index_compatible::call(mcx, old_id, stmt)?;
    if compatible {
        // irel = index_open(oldId, NoLock); caller holds a lock already.
        let irel = relation_open(mcx, old_id, NoLock)?;

        // If it's a partitioned index, there is no storage to share.
        if irel.rd_rel.relkind != RELKIND_PARTITIONED_INDEX {
            stmt.oldNumber = irel.rd_locator.relNumber;
            // C reads irel->rd_createSubid / rd_firstRelfilelocatorSubid off the
            // live relcache entry (not carried on the trimmed RelationData). For
            // the reachable ALTER-COLUMN-TYPE path the index was created in a
            // prior (sub)transaction, so both are InvalidSubTransactionId; the
            // downstream restore (ATExecAddIndex) then lets relcache.c rebuild.
            stmt.oldCreateSubid = types_core::xact::InvalidSubTransactionId;
            stmt.oldFirstRelfilelocatorSubid = types_core::xact::InvalidSubTransactionId;
        }
        irel.close(NoLock)?;
    }
    Ok(())
}

/// `RebuildConstraintComment(tab, pass, objid, rel, domname, conname)`
/// (tablecmds.c:15843) — recreate any comment on a rebuilt constraint. Reads the
/// constraint's existing comment (`GetComment`); if there is one, builds a
/// `CommentStmt` (OBJECT_TABCONSTRAINT when a `rel` is given, else
/// OBJECT_DOMCONSTRAINT using the domain `domname` namelist) and queues an
/// `AT_ReAddComment` command on the given pass. `GetComment` and the
/// `AT_ReAddComment`/`CommentObject` executor leg live in
/// `backend-commands-comment` (reached through the tablecmds-seams seams).
fn RebuildConstraintComment<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, crate::at_phase::AlteredTableInfo<'mcx>>,
    tab_idx: usize,
    pass: i32,
    objid: Oid,
    rel: Option<&Relation<'mcx>>,
    domname: Option<&[mcx::PgString<'mcx>]>,
    conname: &str,
) -> PgResult<()> {
    // comment_str = GetComment(objid, ConstraintRelationId, 0); if NULL return.
    let comment_str = backend_commands_tablecmds_seams::get_comment::call(
        mcx,
        objid,
        ConstraintRelationId,
        0,
    )?;
    let Some(comment_str) = comment_str else {
        return Ok(());
    };

    // Helper: build a String value node holding `s` in the arena.
    let mk_str = |s: &str| -> PgResult<types_nodes::nodes::NodePtr<'mcx>> {
        let sn = types_nodes::value::StringNode {
            sval: mcx::PgString::from_str_in(s, mcx)?,
        };
        mcx::alloc_in(mcx, Node::mk_string(mcx, sn)?)
    };

    // Build the CommentStmt, copying input data for safety.
    let object: Node<'mcx> = if let Some(rel) = rel {
        // OBJECT_TABCONSTRAINT: list_make3(schema, relname, conname)
        let nsp = backend_utils_cache_lsyscache::namespace_range_index_pubsub::get_namespace_name(
            mcx,
            rel.rd_rel.relnamespace,
        )?
        .ok_or_else(|| {
            types_error::PgError::error("RebuildConstraintComment: namespace not found")
        })?;
        let mut list: PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>> = PgVec::new_in(mcx);
        list.push(mk_str(nsp.as_str())?);
        list.push(mk_str(rel.name())?);
        list.push(mk_str(conname)?);
        Node::mk_list(mcx, list)?
    } else {
        // OBJECT_DOMCONSTRAINT: list_make2(makeTypeNameFromNameList(domname), conname)
        let domname =
            domname.expect("RebuildConstraintComment: domname required for domain constraint");
        let mut tn_names: PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>> = PgVec::new_in(mcx);
        for s in domname.iter() {
            tn_names.push(mk_str(s.as_str())?);
        }
        // makeTypeNameFromNameList(names): names + location -1, typemod -1.
        let typename = types_nodes::rawnodes::TypeName {
            names: tn_names,
            typeOid: InvalidOid,
            setof: false,
            pct_type: false,
            typmods: PgVec::new_in(mcx),
            typemod: -1,
            arrayBounds: PgVec::new_in(mcx),
            location: -1,
        };
        let mut list: PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>> = PgVec::new_in(mcx);
        list.push(mcx::alloc_in(mcx, Node::mk_type_name(mcx, typename)?)?);
        list.push(mk_str(conname)?);
        Node::mk_list(mcx, list)?
    };

    let objtype = if rel.is_some() {
        types_nodes::parsenodes::ObjectType::Tabconstraint
    } else {
        types_nodes::parsenodes::ObjectType::Domconstraint
    };
    let cmt = types_nodes::ddlnodes::CommentStmt {
        objtype,
        object: Some(mcx::alloc_in(mcx, object)?),
        comment: Some(mcx::PgString::from_str_in(&comment_str, mcx)?),
    };

    let newcmd = AlterTableCmd {
        subtype: AlterTableType::AT_ReAddComment,
        name: None,
        num: 0,
        newowner: None,
        def: Some(mcx::alloc_in(mcx, Node::mk_comment_stmt(mcx, cmt)?)?),
        behavior: types_nodes::parsenodes::DropBehavior::Restrict,
        missing_ok: false,
        recurse: false,
    };
    let node = mcx::alloc_in(mcx, Node::mk_alter_table_cmd(mcx, newcmd)?)?;
    wqueue[tab_idx].subcmds[pass as usize].push(node);
    Ok(())
}

/// `AT_REWRITE_DEFAULT_VAL` (tablecmds.c) — phase-3 must rewrite to recompute a
/// changed default / generation expression. Same bit as in `at_coladd`.
const AT_REWRITE_DEFAULT_VAL: i32 = 0x02;

// ===========================================================================
// ATExecSetExpression (tablecmds.c:8602)
// ===========================================================================

/// `ATExecSetExpression(tab, rel, colName, newExpr, lockmode)`
/// (tablecmds.c:8602) — ALTER COLUMN ... SET EXPRESSION AS (...). Replace the
/// generation expression of a generated column: drop the old `pg_attrdef`
/// expression (and its dependency records), store the new one, and — for STORED
/// generated columns — queue a phase-3 table rewrite that recomputes every
/// existing row's stored value.
pub fn ATExecSetExpression<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    ti: usize,
    rel: &Relation<'mcx>,
    col_name: &str,
    new_expr: &Node<'mcx>,
    _lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    // tuple = SearchSysCacheAttName(RelationGetRelid(rel), colName);
    let tuple = SearchSysCacheAttName(mcx, rel.rd_id, col_name)?;
    let Some(tuple) = tuple else {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_COLUMN)
            .errmsg(format!(
                "column \"{}\" of relation \"{}\" does not exist",
                col_name,
                rel.name()
            ))
            .finish(here("ATExecSetExpression"))
            .map(|()| unreachable!());
    };

    let attnum = SysCacheGetAttrNotNull(mcx, ATTNAME, &tuple, Anum_pg_attribute_attnum as i32)?
        .as_i16();
    if attnum <= 0 {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("cannot alter system column \"{col_name}\""))
            .finish(here("ATExecSetExpression"))
            .map(|()| unreachable!());
    }

    let attgenerated =
        SysCacheGetAttrNotNull(mcx, ATTNAME, &tuple, Anum_pg_attribute_attgenerated as i32)?
            .as_char();
    if attgenerated == 0 {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "column \"{}\" of relation \"{}\" is not a generated column",
                col_name,
                rel.name()
            ))
            .finish(here("ATExecSetExpression"))
            .map(|()| unreachable!());
    }
    let attnotnull =
        SysCacheGetAttrNotNull(mcx, ATTNAME, &tuple, Anum_pg_attribute_attnotnull as i32)?
            .as_bool();

    // TODO (C comment): virtual generated columns with CHECK constraints could be
    // supported, just need to recheck constraints afterwards. For now reject.
    let has_check = rel
        .rd_att
        .constr
        .as_ref()
        .is_some_and(|c| c.num_check > 0);
    if attgenerated == ATTRIBUTE_GENERATED_VIRTUAL && has_check {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(
                "ALTER TABLE / SET EXPRESSION is not supported for virtual generated columns in \
                 tables with check constraints"
                    .to_string(),
            )
            .errdetail(format!(
                "Column \"{}\" of relation \"{}\" is a virtual generated column.",
                col_name,
                rel.name()
            ))
            .finish(here("ATExecSetExpression"))
            .map(|()| unreachable!());
    }

    if attgenerated == ATTRIBUTE_GENERATED_VIRTUAL && attnotnull {
        wqueue[ti].verify_new_notnull = true;
    }

    // A change of expression could affect a row filter and inject expressions
    // that are not permitted in a row filter; prevent that for virtual columns
    // that belong to a published table.
    if attgenerated == ATTRIBUTE_GENERATED_VIRTUAL
        && !backend_catalog_pg_publication_seams::GetRelationPublications::call(mcx, rel.rd_id)?
            .is_empty()
    {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(
                "ALTER TABLE / SET EXPRESSION is not supported for virtual generated columns in \
                 tables that are part of a publication"
                    .to_string(),
            )
            .errdetail(format!(
                "Column \"{}\" of relation \"{}\" is a virtual generated column.",
                col_name,
                rel.name()
            ))
            .finish(here("ATExecSetExpression"))
            .map(|()| unreachable!());
    }

    let rewrite = attgenerated == ATTRIBUTE_GENERATED_STORED;

    // ReleaseSysCache(tuple): the FormedTuple is dropped at end of scope; we have
    // copied every field we need out of it.
    drop(tuple);

    if rewrite {
        // Clear all the missing values if we're rewriting the table, since this
        // renders them pointless.
        RelationClearMissing(mcx, rel)?;

        // Make sure we don't conflict with later attribute modifications.
        CommandCounterIncrement()?;

        // Find everything that depends on the column (constraints, indexes, etc),
        // and record enough information to recreate the objects after rewrite.
        RememberAllDependentForRebuilding(
            mcx,
            &mut wqueue[ti],
            AlterTableType::AT_SetExpression,
            rel,
            attnum,
            col_name,
        )?;
    }

    // Drop the dependency records of the GENERATED expression, in particular its
    // INTERNAL dependency on the column, which would otherwise cause
    // dependency.c to refuse to perform the deletion.
    let attrdefoid = backend_catalog_pg_attrdef::GetAttrDefaultOid(mcx, rel.rd_id, attnum)?;
    if !OidIsValid(attrdefoid) {
        return Err(types_error::PgError::error(&format!(
            "could not find attrdef tuple for relation {} attnum {}",
            rel.rd_id, attnum
        )));
    }
    let _ = pg_depend_seam::deleteDependencyRecordsFor::call(
        AttrDefaultRelationId,
        attrdefoid,
        false,
    )?;

    // Make above changes visible.
    CommandCounterIncrement()?;

    // Get rid of the GENERATED expression itself. RESTRICT for safety; nothing is
    // expected to depend on the expression.
    RemoveAttrDefault(rel.rd_id, attnum, DROP_RESTRICT, false, false)?;

    // Prepare to store the new expression, in the catalogs.
    //   rawEnt->attnum = attnum;
    //   rawEnt->raw_default = newExpr;
    //   rawEnt->generated = attgenerated;
    //   AddRelationNewConstraints(rel, list_make1(rawEnt), NIL, false, true, false, NULL);
    let raw_default_ptr = mcx::alloc_in(mcx, new_expr.clone_in(mcx)?)?;
    let raw_defaults: [(AttrNumber, types_nodes::nodes::NodePtr<'mcx>, i8); 1] =
        [(attnum, raw_default_ptr, attgenerated)];
    seam::add_relation_new_constraints::call(
        mcx,
        rel,
        &raw_defaults,
        &[],
        false,
        true,
        false,
        None,
    )?;

    // Make the new expression visible.
    CommandCounterIncrement()?;

    if rewrite {
        // Prepare for table rewrite: defval = build_column_default(rel, attnum);
        // newval->expr = expression_planner(defval).
        //
        // build_column_default reads the relation's in-memory tuple descriptor
        // (rd_att) for the column's generation expression. C relies on the
        // relcache being rebuilt in place by the invalidation that
        // AddRelationNewConstraints + CommandCounterIncrement triggered, so
        // `rel`'s rd_att already reflects the new expression. Our `rel` carrier
        // is a snapshot taken before the catalog write, so re-open the relation
        // to pick up the freshly-built descriptor carrying the new expression.
        let fresh_rel = relation_open(mcx, rel.rd_id, NoLock)?;
        let defval =
            rewrite_seam::build_column_default::call(mcx, fresh_rel.alias(), attnum as i32)?
                .expect("build_column_default returned NULL for generated column SET EXPRESSION");
        fresh_rel.close(NoLock)?;
        let planned =
            backend_optimizer_plan_planner::expression_planner(mcx, (*defval).clone_in(mcx)?)?;

        let node = mcx::alloc_in(mcx, Node::mk_expr(mcx, planned)?)?;
        wqueue[ti].newvals.push(crate::at_phase::NewColumnValue {
            attnum,
            expr: Some(node),
            is_generated: true,
        });
        wqueue[ti].rewrite |= AT_REWRITE_DEFAULT_VAL;
    }

    // Drop any pg_statistic entry for the column.
    RemoveStatistics(mcx, rel.rd_id, attnum)?;

    // InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attnum).
    backend_catalog_objectaccess_seams::invoke_object_post_alter_hook::call(
        RelationRelationId,
        rel.rd_id,
        attnum as i32,
    )?;

    // ObjectAddressSubSet(address, RelationRelationId, RelationGetRelid(rel), attnum).
    Ok(object_address_subset(RelationRelationId, rel.rd_id, attnum as i32))
}

// ===========================================================================
// ATExecAlterColumnType (tablecmds.c:14725)
// ===========================================================================

/// `ATExecAlterColumnType(tab, rel, cmd, lockmode)` (tablecmds.c:14725) — the
/// catalog-update leg. The actual heap rewrite is queued for phase 3.
pub fn ATExecAlterColumnType<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    ti: usize,
    rel: &Relation<'mcx>,
    cmd: &AlterTableCmd<'mcx>,
    _lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    let col_name = cmd
        .name
        .as_ref()
        .map(|s| s.as_str())
        .expect("ALTER COLUMN TYPE: cmd.name is NULL");
    let def = cmd
        .def
        .as_deref()
        .expect("ALTER COLUMN TYPE: cmd.def is NULL")
        .expect_columndef();
    let type_name = def
        .typeName
        .as_deref()
        .expect("ALTER COLUMN TYPE: ColumnDef.typeName is NULL");

    let relid = rel.rd_id;
    let rewrite = wqueue[ti].rewrite;

    // Clear all the missing values if we're rewriting the table.
    if rewrite != 0 {
        let newrel = relation_open(mcx, relid, NoLock)?;
        RelationClearMissing(mcx, &newrel)?;
        newrel.close(NoLock)?;
        CommandCounterIncrement()?;
    }

    let attrelation = relation_open(mcx, AttributeRelationId, RowExclusiveLock)?;

    // Look up the target column (a modifiable copy of the syscache entry).
    let heap_tup = SearchSysCacheCopyAttName(mcx, relid, col_name)?;
    let Some(heap_tup) = heap_tup else {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_COLUMN)
            .errmsg(format!(
                "column \"{}\" of relation \"{}\" does not exist",
                col_name,
                rel.name()
            ))
            .finish(here("ATExecAlterColumnType"))
            .map(|()| unreachable!());
    };

    let attnum =
        SysCacheGetAttrNotNull(mcx, ATTNAME, &heap_tup, Anum_pg_attribute_attnum as i32)?.as_i16();
    let cur_atttypid =
        SysCacheGetAttrNotNull(mcx, ATTNAME, &heap_tup, Anum_pg_attribute_atttypid as i32)?
            .as_oid();
    let cur_atttypmod =
        SysCacheGetAttrNotNull(mcx, ATTNAME, &heap_tup, Anum_pg_attribute_atttypmod as i32)?
            .as_i32();
    let cur_attcollation =
        SysCacheGetAttrNotNull(mcx, ATTNAME, &heap_tup, Anum_pg_attribute_attcollation as i32)?
            .as_oid();
    let attgenerated =
        SysCacheGetAttrNotNull(mcx, ATTNAME, &heap_tup, Anum_pg_attribute_attgenerated as i32)?
            .as_char();
    let atthasdef =
        SysCacheGetAttrNotNull(mcx, ATTNAME, &heap_tup, Anum_pg_attribute_atthasdef as i32)?
            .as_bool();
    let atthasmissing =
        SysCacheGetAttrNotNull(mcx, ATTNAME, &heap_tup, Anum_pg_attribute_atthasmissing as i32)?
            .as_bool();

    // attOldTup = TupleDescAttr(tab->oldDesc, attnum - 1).
    let att_old = wqueue[ti].oldDesc.attr((attnum - 1) as usize);
    let old_atttypid = att_old.atttypid;
    let old_atttypmod = att_old.atttypmod;

    // Check for multiple ALTER TYPE on the same column.
    if cur_atttypid != old_atttypid || cur_atttypmod != old_atttypmod {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("cannot alter type of column \"{col_name}\" twice"))
            .finish(here("ATExecAlterColumnType"))
            .map(|()| unreachable!());
    }

    // Look up the target type (should not fail; prep found it). Bridge the owned
    // rawnodes TypeName into the resolver-facing parsenodes TypeName.
    let parse_type_name = backend_parser_parse_type::raw_typename_to_parse(type_name)?;
    let (tform, targettypmod) =
        backend_parser_parse_type::typenameType(mcx, None, &parse_type_name)?;
    let targettype = tform.oid;
    let targetcollid = seam::get_column_def_collation::call(mcx, def, targettype)?;

    // If there is a default, coerce it to the new datatype now (before changing
    // the column type), so build_column_default's own coercion will not fire the
    // wrong error.
    let mut defaultexpr: Option<Expr<'mcx>> = None;
    if atthasdef {
        let built = rewrite_seam::build_column_default::call(mcx, rel.alias(), attnum as i32)?;
        let built = built.expect("build_column_default returned NULL for atthasdef column");
        let stripped = strip_implicit_coercions(&built);
        let src_type = expr_type(Some(stripped))?;
        let coerced = backend_parser_coerce::coerce_to_target_type(
            mcx,
            None,
            stripped.clone_in(mcx)?.erase_lifetime(),
            src_type,
            targettype,
            targettypmod,
            CoercionContext::COERCION_ASSIGNMENT,
            CoercionForm::COERCE_IMPLICIT_CAST,
            -1,
        )?;
        let Some(coerced) = coerced else {
            let msg = if attgenerated != 0 {
                format!(
                    "generation expression for column \"{}\" cannot be cast automatically to type {}",
                    col_name,
                    format_type_be(mcx, targettype)?
                )
            } else {
                format!(
                    "default for column \"{}\" cannot be cast automatically to type {}",
                    col_name,
                    format_type_be(mcx, targettype)?
                )
            };
            return backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(msg)
                .finish(here("ATExecAlterColumnType"))
                .map(|()| unreachable!());
        };
        // `coerced` is the parser-arena `'static` coercion result; bring it into
        // `mcx` for the `'mcx` `defaultexpr_node`/StoreAttrDefault path below.
        defaultexpr = Some(coerced.clone_in(mcx)?);
    }

    // Find everything that depends on the column and record enough info to
    // recreate the objects after the rewrite (ATPostAlterTypeCleanup).
    RememberAllDependentForRebuilding(
        mcx,
        &mut wqueue[ti],
        AlterTableType::AT_AlterColumnType,
        rel,
        attnum,
        col_name,
    )?;

    // Now drop the column's own dependency on its (still-current) type +
    // collation. C scans pg_depend by depender and deletes exactly the NORMAL
    // dependency on `attTup->atttypid` and, if any, on `attTup->attcollation`;
    // we delete those two specific records directly.
    pg_depend_seam::deleteDependencyRecordsForSpecific::call(
        RelationRelationId,
        relid,
        DEPENDENCY_NORMAL.as_char(),
        TypeRelationId,
        cur_atttypid,
    )?;
    if OidIsValid(cur_attcollation) {
        pg_depend_seam::deleteDependencyRecordsForSpecific::call(
            RelationRelationId,
            relid,
            DEPENDENCY_NORMAL.as_char(),
            CollationRelationId,
            cur_attcollation,
        )?;
    }

    // First fix up the missing value, if any. If `rewrite` is set the missing
    // value should already have been cleared, so this only fires on the
    // no-rewrite path. We assume that since the table doesn't need rewriting,
    // the actual Datum doesn't need to be changed, only the array metadata: get
    // the element out of the old-type array and repack it in a new array built
    // with the new type data (tablecmds.c:14897).
    let mut new_missingval: Option<Option<alloc::vec::Vec<u8>>> = None;
    if atthasmissing {
        // Assert(tab->rewrite == 0);
        debug_assert_eq!(rewrite, 0);

        // missingval = heap_getattr(heapTup, Anum_pg_attribute_attmissingval,
        //                           attrelation->rd_att, &missingNull);
        let (missingval_bytes, missing_null) = backend_utils_cache_syscache::SysCacheGetAttr(
            mcx,
            ATTNAME,
            &heap_tup,
            Anum_pg_attribute_attmissingval as i32,
        )?;

        // if it's a null array there is nothing to do.
        if !missing_null {
            // The old type's array element metadata (still current on the
            // pg_attribute tuple before this update): attlen/attbyval/attalign.
            let old_attlen =
                SysCacheGetAttrNotNull(mcx, ATTNAME, &heap_tup, Anum_pg_attribute_attlen as i32)?
                    .as_i16();
            let old_attbyval = SysCacheGetAttrNotNull(
                mcx,
                ATTNAME,
                &heap_tup,
                Anum_pg_attribute_attbyval as i32,
            )?
            .as_bool();
            let old_attalign = SysCacheGetAttrNotNull(
                mcx,
                ATTNAME,
                &heap_tup,
                Anum_pg_attribute_attalign as i32,
            )?
            .as_char();

            // missingval = array_get_element(missingval, 1, &one, 0, attlen,
            //                                attbyval, attalign, &isNull);
            // The single-element array is deconstructed; element 0 is the C
            // "element 1".
            let old_bytes = missingval_bytes.as_ref_bytes();
            let elems = backend_utils_adt_arrayfuncs_seams::deconstruct_array_values_bytes::call(
                mcx,
                old_bytes,
                old_atttypid,
                old_attlen,
                old_attbyval,
                old_attalign as core::ffi::c_char,
            )?;
            let (elem_datum, _elem_isnull) = elems
                .first()
                .ok_or_else(|| {
                    backend_utils_error::PgError::error(
                        "attmissingval array has no element".to_string(),
                    )
                })?
                .clone();

            // missingval = PointerGetDatum(construct_array(&missingval, 1,
            //     targettype, tform->typlen, tform->typbyval, tform->typalign));
            let new_arr = backend_utils_adt_arrayfuncs_seams::construct_array_values_bytes::call(
                mcx,
                core::slice::from_ref(&elem_datum),
                targettype,
                tform.typlen,
                tform.typbyval,
                tform.typalign as core::ffi::c_char,
            )?;
            new_missingval = Some(Some(new_arr.as_slice().to_vec()));
        }
    }

    // Here we go — change the recorded column type and collation.
    let attndims_count = type_name.arrayBounds.len();
    if attndims_count > i16::MAX as usize {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg("too many array dimensions".to_string())
            .finish(here("ATExecAlterColumnType"))
            .map(|()| unreachable!());
    }

    let row = PgAttributeUpdateRow {
        atttypid: Some(targettype),
        atttypmod: Some(targettypmod),
        attcollation: Some(targetcollid),
        attndims: Some(attndims_count as i16),
        attlen: Some(tform.typlen),
        attbyval: Some(tform.typbyval),
        attalign: Some(tform.typalign),
        attstorage: Some(tform.typstorage),
        attcompression: Some(InvalidCompressionMethod),
        // Repacked missing value (no-rewrite path); `None` leaves it unchanged.
        attmissingval: new_missingval,
        ..Default::default()
    };
    indexing_seam::catalog_tuple_update_pg_attribute::call(mcx, &attrelation, &heap_tup, &row)?;

    attrelation.close(RowExclusiveLock)?;

    // Install dependencies on new datatype and collation.
    add_column_datatype_dependency(relid, attnum as i32, targettype)?;
    add_column_collation_dependency(relid, attnum as i32, targetcollid)?;

    // Drop any pg_statistic entry for the column, since it's now wrong type.
    RemoveStatistics(mcx, relid, attnum)?;

    // (InvokeObjectPostAlterHook — no-op in this build.)

    // Update the default, if present, by brute force (remove and re-add).
    if let Some(defaultexpr) = defaultexpr {
        // If it's a GENERATED default, drop its dependency records, in
        // particular its INTERNAL dependency on the column, which would
        // otherwise cause dependency.c to refuse to perform the deletion.
        if attgenerated != 0 {
            let attrdefoid = backend_catalog_pg_attrdef::GetAttrDefaultOid(mcx, relid, attnum)?;
            if !OidIsValid(attrdefoid) {
                return Err(types_error::PgError::error(&format!(
                    "could not find attrdef tuple for relation {} attnum {}",
                    relid, attnum
                )));
            }
            let _ = pg_depend_seam::deleteDependencyRecordsFor::call(
                AttrDefaultRelationId,
                attrdefoid,
                false,
            )?;
        }

        // Make updates-so-far visible.
        CommandCounterIncrement()?;

        RemoveAttrDefault(relid, attnum, DROP_RESTRICT, true, true)?;
        let _ = StoreAttrDefault(mcx, relid, attnum, &defaultexpr_node(mcx, defaultexpr)?, true)?;
    }

    Ok(object_address_subset(RelationRelationId, relid, attnum as i32))
}

// ---------------------------------------------------------------------------
// Small local helpers.
// ---------------------------------------------------------------------------

/// `DROP_RESTRICT` (parsenodes.h).
use types_nodes::parsenodes::DROP_RESTRICT;

/// `ACL_USAGE` (parsenodes.h).
const ACL_USAGE: types_acl::AclMode = types_acl::ACL_USAGE;

/// `parser_errposition(pstate, location)` with `pstate->p_sourcetext = query`.
fn errpos(query: Option<&str>, location: i32) -> i32 {
    if location < 0 {
        return 0;
    }
    let Some(s) = query else { return 0 };
    let limit = (location as usize).min(s.len());
    s[..limit].chars().count() as i32 + 1
}

/// Re-attach a parse cursor to an error raised by a callee that ran with a
/// NULL `ParseState` (the seam resolves `typenameTypeIdAndMod`/
/// `GetColumnDefCollation` without a pstate). Mirrors C's
/// `parser_errposition(pstate, location)` where `pstate->p_sourcetext =
/// context->queryString`. Only set the cursor when the error carries none of
/// its own, matching errstart's `edata->cursorpos == 0` guard.
fn attach_errpos(
    e: backend_utils_error::PgError,
    query: Option<&str>,
    location: i32,
) -> backend_utils_error::PgError {
    if e.cursor_position().is_some() {
        return e;
    }
    let pos = errpos(query, location);
    if pos > 0 {
        e.with_cursor_position(pos)
    } else {
        e
    }
}

/// `rel->rd_rel->reloftype` via the syscache projection.
fn reloftype_of(relid: Oid) -> PgResult<Oid> {
    Ok(
        backend_utils_cache_syscache_seams::search_relation_reloftype::call(relid)?
            .unwrap_or(InvalidOid),
    )
}

/// `format_type_be(typid)` (format_type.c).
fn format_type_be<'mcx>(mcx: Mcx<'mcx>, typid: Oid) -> PgResult<String> {
    Ok(backend_utils_adt_format_type::format_type_be(mcx, typid)?
        .as_str()
        .to_string())
}

/// `format_type_with_typemod(typid, typmod)` (format_type.c).
fn format_type_with_typemod<'mcx>(mcx: Mcx<'mcx>, typid: Oid, typmod: i32) -> PgResult<String> {
    Ok(
        backend_utils_adt_format_type::format_type_with_typemod(mcx, typid, typmod)?
            .as_str()
            .to_string(),
    )
}

/// `quote_identifier(ident)` (ruleutils.c).
fn quote_identifier<'mcx>(mcx: Mcx<'mcx>, ident: &str) -> PgResult<String> {
    Ok(backend_utils_adt_ruleutils::quote_identifier(mcx, ident)?
        .as_str()
        .to_string())
}

/// Wrap an `Expr` default back into a `Node` for `StoreAttrDefault`.
fn defaultexpr_node<'mcx>(mcx: Mcx<'mcx>, expr: Expr<'mcx>) -> PgResult<Node<'mcx>> {
    Node::mk_expr(mcx, expr)
}
