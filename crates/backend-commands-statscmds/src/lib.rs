#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

//! `backend/commands/statscmds.c` — CREATE / ALTER / DROP STATISTICS (extended
//! statistics objects).
//!
//! Faithful idiomatic (owned-tree) port. This module creates, alters, and drops
//! the `pg_statistic_ext` catalog rows (and the associated
//! `pg_statistic_ext_data` data rows) that describe extended statistics
//! objects.
//!
//! THIS file's own logic — ported branch-for-branch: the FROM-clause
//! single-relation check + relkind whitelist + system-catalog prohibition; the
//! name-vs-`defnames` namespace decision + name choosing; the duplicate-object
//! check + `STATS_MAX_DIMENSIONS` check; the per-`StatsElem` classification
//! (column reference / parenthesized `Var` / arbitrary expression) with the
//! system-attribute / virtual-generated-column / no-less-than-operator
//! rejections + the `stxexprs` collection; the statistics-kind parsing + the
//! "no kinds on a single expression" / "build all when none requested and >= 2"
//! / "at least 2 columns or a single expression" rules; the attnum sort +
//! adjacent-duplicate detection + the O(N^2) duplicate-expression scan + the
//! `stxkind` assembly + the `pg_statistic_ext` column-value population; the
//! AUTO/NORMAL dependency-recording orchestration; `AlterStatistics`'s
//! stattarget range clamp; `RemoveStatisticsById` / `RemoveStatisticsDataById`'s
//! delete sequence; `StatisticsGetRelation`'s syscache lookup.
//!
//! Genuine externals cross owner seams; `errdetail_relkind_not_supported`,
//! `exprType`/`equal`/`pull_varattnos`/`nodeToString`, the catalog tuple write
//! engine, `format_type_be`, etc. are reached through their owners' seam or
//! direct crates. The variable-length pg_statistic_ext columns (int2vector
//! `stxkeys`, char[] `stxkind`, text `stxexprs`) are packed by the indexing
//! owner behind `catalog_tuple_insert_pg_statistic_ext`.

use mcx::{Mcx, MemoryContext, PgVec};

use backend_utils_error::{ereport, PgResult};
use types_error::pg_error::ErrorLocation;
use types_error::{
    ERRCODE_DUPLICATE_COLUMN, ERRCODE_DUPLICATE_OBJECT, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_OBJECT_DEFINITION,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_SYNTAX_ERROR, ERRCODE_TOO_MANY_COLUMNS,
    ERRCODE_UNDEFINED_COLUMN, ERRCODE_WRONG_OBJECT_TYPE, ERROR, NOTICE, WARNING,
};

use types_acl::acl::{ACLCHECK_OK, ACL_CREATE};
use types_catalog::catalog_dependency::{
    InvalidObjectAddress, ObjectAddress, DEPENDENCY_AUTO, DEPENDENCY_NORMAL,
};
use types_catalog::pg_statistic_ext::{
    PgStatisticExtInsertRow, StatisticExtDataRelationId, StatisticExtRelationId,
    STATS_EXT_DEPENDENCIES, STATS_EXT_EXPRESSIONS, STATS_EXT_MCV, STATS_EXT_NDISTINCT,
};
use types_core::{AttrNumber, Oid};
use types_nodes::nodes::{ntag, Node};
use types_nodes::parsenodes::ObjectType;
use types_nodes::ddlnodes::{AlterStatsStmt, CreateStatsStmt};
use types_storage::lock::{NoLock, RowExclusiveLock, ShareUpdateExclusiveLock};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::FirstLowInvalidHeapAttributeNumber;

use backend_access_table_table::table_open;
use backend_access_common_heaptuple::heap_modify_tuple;
use backend_nodes_outfuncs::nodeToString;
use backend_catalog_catalog::IsSystemRelation;
use backend_catalog_dependency::recordDependencyOnSingleRelExpr;
use backend_catalog_indexing::keystone::{CatalogTupleDelete, CatalogTupleUpdate};
use backend_utils_cache_inval::cache_invalidate::{
    CacheInvalidateRelcache, CacheInvalidateRelcacheByRelid,
};
use backend_nodes_core::nodefuncs::expr_type as node_expr_type;
use backend_commands_comment::CreateComments;

use backend_access_common_relation_seams::relation_openrv;
use backend_catalog_aclchk_seams::{aclcheck_error, object_aclcheck, object_ownercheck};
use backend_catalog_indexing_seams::catalog_tuple_insert_pg_statistic_ext;
use backend_catalog_objectaccess_seams::{invoke_object_post_alter_hook, invoke_object_post_create_hook};
use backend_catalog_objectaddress_seams::get_relkind_objtype;
use backend_catalog_pg_depend_seams::recordDependencyOn;
use backend_catalog_pg_shdepend_seams::recordDependencyOnOwner;
use backend_commands_indexcmds_seams::make_object_name;
use backend_nodes_core_seams::bms_next_member;
use backend_nodes_nodeFuncs_seams::equal;
use backend_optimizer_util_var_seams::pull_varattnos;
use backend_utils_adt_format_type_seams::format_type_be;
use backend_utils_cache_lsyscache_seams::{get_attgenerated, get_attname, get_namespace_name};
use backend_utils_cache_syscache_seams::{
    get_statext_oid, search_syscache_attname, statext_data_search_tuple, statext_exists,
    statext_get_relid, statext_search_tuple,
};
use backend_utils_cache_typcache_seams::lookup_type_cache_lt_opr;
use backend_utils_init_miscinit_seams::get_user_id;
use backend_utils_misc_guc_seams::allow_system_table_mods;

/* ===========================================================================
 * Constants (statistics.h / vacuum.h / c.h).
 * ======================================================================= */

/// `STATS_MAX_DIMENSIONS` (statistics.h) — max attributes per stats object.
const STATS_MAX_DIMENSIONS: usize = 8;
/// `MAX_STATISTICS_TARGET` (vacuum.h).
const MAX_STATISTICS_TARGET: i32 = 10000;
/// `NAMEDATALEN` (c.h) — the fixed `NameData` width.
const NAMEDATALEN: usize = 64;

/// `ATTRIBUTE_GENERATED_VIRTUAL` (`pg_attribute.h`) — `'v'`.
const ATTRIBUTE_GENERATED_VIRTUAL: u8 = b'v';

// relkind codes (pg_class.h).
const RELKIND_RELATION: u8 = b'r';
const RELKIND_MATVIEW: u8 = b'm';
const RELKIND_FOREIGN_TABLE: u8 = b'f';
const RELKIND_PARTITIONED_TABLE: u8 = b'p';

// Relation OIDs (catalog OIDs).
const RelationRelationId: Oid = 1259;
const NamespaceRelationId: Oid = 2615;

/* ===========================================================================
 * Helpers.
 * ======================================================================= */

/// `errstart`/`errfinish` source-location helper for the `NOTICE`/`WARNING`
/// reports (statscmds.c is `src/backend/commands/statscmds.c`).
fn errloc(lineno: i32, funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/commands/statscmds.c", lineno, funcname)
}

/// `strVal(lfirst(cell))` for a `String` value node (nodes/value.h).
fn str_val<'a>(n: &'a Node<'a>) -> &'a str {
    match n.node_tag() {
        ntag::T_String => n.expect_string().sval.as_str(),
        // A non-String here would be a grammar bug; the empty string mirrors a
        // NULL `char *` (which `strcmp`/`pg_char_to_encoding` handle).
        _ => "",
    }
}

/// `intVal(node)` for an `Integer` value node (nodes/value.h). `None` when the
/// node is absent (the C `stmt->stxstattarget == NULL`).
fn int_val_opt(node: Option<&Node>) -> Option<i32> {
    match node.and_then(|n| n.as_integer()) {
        Some(i) => Some(i.ival),
        _ => None,
    }
}

/// `namestrcpy(&stxname, namestr)` (utils/adt/name.c) — copy `namestr` into a
/// fixed `NameData`, truncating to `NAMEDATALEN - 1` bytes plus the NUL.
fn namestrcpy(namestr: &str) -> [u8; NAMEDATALEN] {
    let mut nd = [0u8; NAMEDATALEN];
    let src = namestr.as_bytes();
    let n = src.len().min(NAMEDATALEN - 1);
    nd[..n].copy_from_slice(&src[..n]);
    nd
}

/// `RelationGetRelationName(rel)` (rel.h) — `rd_rel->relname` as a string.
fn rel_name(rel: &types_rel::Relation<'_>) -> alloc::string::String {
    rel.name().to_string()
}

extern crate alloc;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

/* ===========================================================================
 * CREATE STATISTICS (statscmds.c:61-632).
 * ======================================================================= */

pub fn CreateStatistics<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateStatsStmt<'_>,
    check_rights: bool,
) -> PgResult<ObjectAddress> {
    let mut attnums: [i16; STATS_MAX_DIMENSIONS] = [0; STATS_MAX_DIMENSIONS];
    let mut nattnums: usize = 0;
    let numcols: usize;
    let namestr: String;
    let stxname: [u8; NAMEDATALEN];
    let statoid: Oid;
    let namespaceId: Oid;
    let stxowner: Oid = get_user_id::call();
    let mut stxexprs: Vec<Node<'mcx>> = Vec::new(); // NIL
    let mut rel: Option<types_rel::Relation<'mcx>> = None;
    let relid: Oid;
    let mut build_ndistinct: bool;
    let mut build_dependencies: bool;
    let mut build_mcv: bool;
    let build_expressions: bool;
    let mut requested_type = false;

    /* Assert(IsA(stmt, CreateStatsStmt)); — guaranteed by typed reference */

    /*
     * Examine the FROM clause.  Currently, we only allow it to be a single
     * simple table.
     */
    if stmt.relations.len() != 1 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("only a single relation is allowed in CREATE STATISTICS")
            .into_error());
    }

    for rln in stmt.relations.iter() {
        let rangevar = match (&**rln).node_tag() {
            ntag::T_RangeVar => (&**rln).expect_rangevar(),
            _ => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("only a single relation is allowed in CREATE STATISTICS")
                    .into_error());
            }
        };

        /*
         * CREATE STATISTICS will influence future execution plans but does not
         * interfere with currently executing plans, so ShareUpdateExclusiveLock
         * is enough.
         */
        // The seam opens a relation from the resolved (owned-String) RangeVar
        // form; build it from the parse-node RangeVar's fields.
        let rv = types_tuple::access::RangeVar {
            catalogname: rangevar.catalogname.as_ref().map(|s| s.as_str().to_string()),
            schemaname: rangevar.schemaname.as_ref().map(|s| s.as_str().to_string()),
            relname: rangevar.relname.as_ref().map(|s| s.as_str().to_string()).unwrap_or_default(),
            inh: rangevar.inh,
            relpersistence: rangevar.relpersistence as u8,
            location: rangevar.location,
        };
        let r = relation_openrv::call(mcx, &rv, ShareUpdateExclusiveLock)?;

        /* Restrict to allowed relation types */
        let relkind = r.rd_rel.relkind as u8;
        if relkind != RELKIND_RELATION
            && relkind != RELKIND_MATVIEW
            && relkind != RELKIND_FOREIGN_TABLE
            && relkind != RELKIND_PARTITIONED_TABLE
        {
            let relname = rel_name(&r);
            let detail =
                backend_catalog_pg_class::errdetail_relkind_not_supported(relkind)?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!("cannot define statistics for relation \"{relname}\""))
                .errdetail(detail)
                .into_error());
        }

        /*
         * You must own the relation to create stats on it.  NB: this check runs
         * even when check_rights == false (concurrent-change safety).
         */
        if !object_ownercheck::call(RelationRelationId, r.rd_id, stxowner)? {
            let objtype = get_relkind_objtype::call(relkind);
            let relname = rel_name(&r);
            aclcheck_error::call(
                types_acl::acl::ACLCHECK_NOT_OWNER,
                objtype,
                Some(relname),
            )?;
        }

        /* Creating statistics on system catalogs is not allowed */
        if !allow_system_table_mods::call() && IsSystemRelation(&r) {
            let relname = rel_name(&r);
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg(format!("permission denied: \"{relname}\" is a system catalog"))
                .into_error());
        }

        rel = Some(r);
    }

    let rel = rel.expect("CreateStatistics: rel"); /* Assert(rel); */
    relid = rel.rd_id;

    /*
     * If the node has a name, split it up and determine creation namespace.  If
     * not, put the object in the same namespace as the relation and cons up a
     * name for it.
     */
    if !stmt.defnames.is_empty() {
        let names: Vec<Option<String>> =
            stmt.defnames.iter().map(|n| Some(str_val(n).to_string())).collect();
        let (nsp, name) =
            backend_catalog_namespace::QualifiedNameGetCreationNamespace(mcx, &names)?;
        namespaceId = nsp;
        namestr = name.to_string();
    } else {
        namespaceId = rel.rd_rel.relnamespace;
        let name1 = rel_name(&rel);
        let name2 = ChooseExtendedStatisticNameAddition(&stmt.exprs);
        namestr = ChooseExtendedStatisticName(&name1, &name2, "stat", namespaceId)?;
    }
    stxname = namestrcpy(&namestr);

    /*
     * Check we have creation rights in target namespace.  Skip check if caller
     * doesn't want it.
     */
    if check_rights {
        let aclresult =
            object_aclcheck::call(NamespaceRelationId, namespaceId, get_user_id::call(), ACL_CREATE)?;
        if aclresult != ACLCHECK_OK {
            let nspname = get_namespace_name::call(mcx, namespaceId)?
                .map(|s| s.as_str().to_string());
            aclcheck_error::call(aclresult, ObjectType::Schema, nspname)?;
        }
    }

    /*
     * Deal with the possibility that the statistics object already exists.
     */
    if statext_exists::call(&namestr, namespaceId)? {
        if stmt.if_not_exists {
            ereport(NOTICE)
                .errcode(ERRCODE_DUPLICATE_OBJECT)
                .errmsg(format!("statistics object \"{namestr}\" already exists, skipping"))
                .finish(errloc(206, "CreateStatistics"))?;
            rel.close(NoLock)?;
            return Ok(InvalidObjectAddress);
        }

        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!("statistics object \"{namestr}\" already exists"))
            .into_error());
    }

    /*
     * Make sure no more than STATS_MAX_DIMENSIONS columns are used.
     */
    numcols = stmt.exprs.len();
    if numcols > STATS_MAX_DIMENSIONS {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_TOO_MANY_COLUMNS)
            .errmsg(format!(
                "cannot have more than {STATS_MAX_DIMENSIONS} columns in statistics"
            ))
            .into_error());
    }

    /*
     * Convert the expression list to a simple array of attnums, but also keep a
     * list of more complex expressions, enforcing the system-attribute /
     * virtual-generated-column / less-than-operator constraints.
     */
    for selem_node in stmt.exprs.iter() {
        let selem = match (&**selem_node).node_tag() {
            ntag::T_StatsElem => (&**selem_node).expect_statselem(),
            // The grammar guarantees these are StatsElem; mirror lfirst_node.
            _ => unreachable!("CreateStatsStmt.exprs element is not a StatsElem"),
        };

        if let Some(attname) = selem.name.as_ref() {
            /* column reference */
            let attname = attname.as_str();
            let att = match search_syscache_attname::call(relid, attname)? {
                Some(a) => a,
                None => {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_UNDEFINED_COLUMN)
                        .errmsg(format!("column \"{attname}\" does not exist"))
                        .into_error());
                }
            };
            let (attnum, atttypid) = att;

            /* Disallow use of system attributes in extended stats */
            if attnum <= 0 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("statistics creation on system columns is not supported")
                    .into_error());
            }

            /* Disallow use of virtual generated columns in extended stats */
            if get_attgenerated::call(relid, attnum)? == ATTRIBUTE_GENERATED_VIRTUAL {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("statistics creation on virtual generated columns is not supported")
                    .into_error());
            }

            /* Disallow data types without a less-than operator */
            if lookup_type_cache_lt_opr::call(atttypid)? == 0 {
                let typname = format_type_be::call(mcx, atttypid)?;
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg(format!(
                        "column \"{attname}\" cannot be used in statistics because its type {} has no default btree operator class",
                        typname.as_str()
                    ))
                    .into_error());
            }

            attnums[nattnums] = attnum;
            nattnums += 1;
        } else if let Some(var) = selem.expr.as_ref().and_then(|n| n.as_var()) {
            /* column reference in parens */

            /* Disallow use of system attributes in extended stats */
            if var.varattno <= 0 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("statistics creation on system columns is not supported")
                    .into_error());
            }

            /* Disallow use of virtual generated columns in extended stats */
            if get_attgenerated::call(relid, var.varattno)? == ATTRIBUTE_GENERATED_VIRTUAL {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("statistics creation on virtual generated columns is not supported")
                    .into_error());
            }

            /* Disallow data types without a less-than operator */
            if lookup_type_cache_lt_opr::call(var.vartype)? == 0 {
                let attname = get_attname::call(mcx, relid, var.varattno, false)?
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default();
                let typname = format_type_be::call(mcx, var.vartype)?;
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg(format!(
                        "column \"{attname}\" cannot be used in statistics because its type {} has no default btree operator class",
                        typname.as_str()
                    ))
                    .into_error());
            }

            attnums[nattnums] = var.varattno;
            nattnums += 1;
        } else {
            /* expression */
            let expr_node = selem
                .expr
                .as_ref()
                .expect("CreateStatistics: stat expression is NULL"); /* Assert(expr != NULL) */
            let expr = expr_node
                .as_expr()
                .expect("CreateStatistics: stat expression is not an Expr");

            /*
             * Collect the referenced attnos. C `pull_varattnos(expr, 1,
             * &attnums)` accumulates an offset Bitmapset that the
             * `bms_next_member` loop walks.
             */
            let var_attnos = pull_varattnos::call(mcx, expr, 1)?;
            let mut k: i32 = -1;
            loop {
                k = bms_next_member::call(var_attnos.as_deref(), k);
                if k < 0 {
                    break;
                }
                let attnum: AttrNumber =
                    (k + FirstLowInvalidHeapAttributeNumber as i32) as AttrNumber;

                /* Disallow expressions referencing system attributes. */
                if attnum <= 0 {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("statistics creation on system columns is not supported")
                        .into_error());
                }

                /* Disallow use of virtual generated columns in extended stats */
                if get_attgenerated::call(relid, attnum)? == ATTRIBUTE_GENERATED_VIRTUAL {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("statistics creation on virtual generated columns is not supported")
                        .into_error());
                }
            }

            /*
             * Disallow data types without a less-than operator.  Ignored for
             * statistics on a single expression (regular stats only, which can
             * deal with such data types).
             */
            if stmt.exprs.len() > 1 {
                let atttype = node_expr_type(Some(expr))?;
                if lookup_type_cache_lt_opr::call(atttype)? == 0 {
                    let typname = format_type_be::call(mcx, atttype)?;
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg(format!(
                            "expression cannot be used in multivariate statistics because its type {} has no default btree operator class",
                            typname.as_str()
                        ))
                        .into_error());
                }
            }

            stxexprs.push(expr_node.as_ref().clone_in(mcx)?);
        }
    }

    /*
     * Parse the statistics kinds.  First check that for a single expression,
     * no statistics kinds are specified.
     */
    if stmt.exprs.len() == 1 && stxexprs.len() == 1 {
        if !stmt.stat_types.is_empty() {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(
                    "when building statistics on a single expression, statistics kinds may not be specified",
                )
                .into_error());
        }
    }

    /* OK, let's check that we recognize the statistics kinds. */
    build_ndistinct = false;
    build_dependencies = false;
    build_mcv = false;
    for type_node in stmt.stat_types.iter() {
        let type_ = str_val(type_node);

        if type_ == "ndistinct" {
            build_ndistinct = true;
            requested_type = true;
        } else if type_ == "dependencies" {
            build_dependencies = true;
            requested_type = true;
        } else if type_ == "mcv" {
            build_mcv = true;
            requested_type = true;
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("unrecognized statistics kind \"{type_}\""))
                .into_error());
        }
    }

    /*
     * If no statistic type was specified, build them all (but only when defined
     * on more than one column/expression).
     */
    if !requested_type && numcols >= 2 {
        build_ndistinct = true;
        build_dependencies = true;
        build_mcv = true;
    }

    /*
     * When there are non-trivial expressions, build expression stats
     * automatically.
     */
    build_expressions = !stxexprs.is_empty();

    /*
     * Check at least two columns were specified, or we're building statistics
     * on a single expression.
     */
    if numcols < 2 && stxexprs.len() != 1 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg("extended statistics require at least 2 columns")
            .into_error());
    }

    /*
     * Sort the attnums (makes detecting duplicates easier; does not matter for
     * the contents). `compare_int16`: `return (av - bv)` cannot overflow since
     * int is wider than int16.
     */
    attnums[..nattnums].sort_by(|a, b| (*a as i32 - *b as i32).cmp(&0));

    /*
     * Check for duplicates in the (sorted) column list.
     */
    for i in 1..nattnums {
        if attnums[i] == attnums[i - 1] {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DUPLICATE_COLUMN)
                .errmsg("duplicate column name in statistics definition")
                .into_error());
        }
    }

    /*
     * Check for duplicate expressions, counting occurrences of each (O(N^2),
     * but the expression count is small).
     */
    for expr1 in &stxexprs {
        let mut cnt = 0;
        for expr2 in &stxexprs {
            if let (Some(e1), Some(e2)) = (expr1.as_expr(), expr2.as_expr()) {
                if equal::call(e1, e2) {
                    cnt += 1;
                }
            }
        }
        /* every expression should find at least itself: Assert(cnt >= 1) */
        if cnt > 1 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DUPLICATE_COLUMN)
                .errmsg("duplicate expression in statistics definition")
                .into_error());
        }
    }

    /* Form the sorted column list for the int2vector. */
    let stxkeys: Vec<i16> = attnums[..nattnums].to_vec();

    /* construct the char array of enabled statistic types */
    let mut stxkind: Vec<i8> = Vec::new();
    if build_ndistinct {
        stxkind.push(STATS_EXT_NDISTINCT);
    }
    if build_dependencies {
        stxkind.push(STATS_EXT_DEPENDENCIES);
    }
    if build_mcv {
        stxkind.push(STATS_EXT_MCV);
    }
    if build_expressions {
        stxkind.push(STATS_EXT_EXPRESSIONS);
    }
    /* Assert(ntypes > 0 && ntypes <= lengthof(types)); */

    /*
     * The dependency recording + the text serialization both take the
     * expression List as a single `Node` (`(Node *) stxexprs`); build it once.
     * `None` when there are no expressions (the C NIL list).
     */
    let stxexprs_list: Option<Node<'mcx>> = if !stxexprs.is_empty() {
        let mut items: PgVec<'mcx, types_nodes::nodes::NodePtr<'mcx>> =
            mcx::vec_with_capacity_in(mcx, stxexprs.len())?;
        for e in &stxexprs {
            items.push(mcx::alloc_in(mcx, e.clone_in(mcx)?)?);
        }
        Some(Node::mk_list(mcx, items))
    } else {
        None
    };

    /* convert the expressions (if any) to a text datum */
    let stxexprs_text: Option<String> = match &stxexprs_list {
        // exprsString = nodeToString(stxexprs); CStringGetTextDatum(exprsString);
        Some(list) => Some(nodeToString(mcx, list)?.as_str().to_string()),
        None => None,
    };

    /*
     * Everything seems fine; build + insert the pg_statistic_ext tuple.  The
     * OID assignment + Datum packing (int2vector / char[] / text) +
     * heap_form_tuple + CatalogTupleInsert over pg_statistic_ext cross the
     * indexing seam; every column value was decided here. stxstattarget is left
     * NULL on a fresh CREATE.
     */
    let statrel = table_open(mcx, StatisticExtRelationId, RowExclusiveLock)?;

    statoid = catalog_tuple_insert_pg_statistic_ext::call(
        mcx,
        &statrel,
        &PgStatisticExtInsertRow {
            stxrelid: relid,
            stxname,
            stxnamespace: namespaceId,
            stxowner,
            stxkeys,
            stxkind,
            stxexprs: stxexprs_text,
        },
    )?;

    statrel.close(RowExclusiveLock)?;

    /*
     * We used to create the pg_statistic_ext_data tuple too, but it's not clear
     * what value the stxdinherit flag should have.
     */

    invoke_object_post_create_hook::call(StatisticExtRelationId, statoid, 0)?;

    /* Invalidate relcache so that others see the new statistics object. */
    CacheInvalidateRelcache(&rel)?;

    rel.close(NoLock)?;

    /*
     * Add an AUTO dependency on each column used in the stats.
     */
    let myself = ObjectAddress {
        classId: StatisticExtRelationId,
        objectId: statoid,
        objectSubId: 0,
    };

    /* add dependencies for plain column references */
    for i in 0..nattnums {
        let parentobject = ObjectAddress {
            classId: RelationRelationId,
            objectId: relid,
            objectSubId: attnums[i] as i32,
        };
        recordDependencyOn::call(mcx, &myself, &parentobject, DEPENDENCY_AUTO)?;
    }

    /*
     * If there are no column dependencies, give the stats object an auto
     * dependency on the whole table.
     */
    if nattnums == 0 {
        let parentobject = ObjectAddress {
            classId: RelationRelationId,
            objectId: relid,
            objectSubId: 0,
        };
        recordDependencyOn::call(mcx, &myself, &parentobject, DEPENDENCY_AUTO)?;
    }

    /*
     * Store dependencies on anything mentioned in statistics expressions, just
     * like for index expressions.
     */
    if let Some(exprs_list) = &stxexprs_list {
        // recordDependencyOnSingleRelExpr(&myself, (Node *) stxexprs, relid,
        //   DEPENDENCY_NORMAL, DEPENDENCY_AUTO, false);
        recordDependencyOnSingleRelExpr(
            &myself,
            exprs_list,
            relid,
            DEPENDENCY_NORMAL,
            DEPENDENCY_AUTO,
            false,
        )?;
    }

    /*
     * Also add dependencies on namespace and owner.
     */
    let parentobject = ObjectAddress {
        classId: NamespaceRelationId,
        objectId: namespaceId,
        objectSubId: 0,
    };
    recordDependencyOn::call(mcx, &myself, &parentobject, DEPENDENCY_NORMAL)?;

    recordDependencyOnOwner::call(StatisticExtRelationId, statoid, stxowner)?;

    /* Add any requested comment */
    if let Some(comment) = stmt.stxcomment.as_ref() {
        CreateComments(mcx, statoid, StatisticExtRelationId, 0, Some(comment.as_str()))?;
    }

    /* Return stats object's address */
    Ok(myself)
}

/* ===========================================================================
 * ALTER STATISTICS (statscmds.c:637-754).
 * ======================================================================= */

pub fn AlterStatistics<'mcx>(mcx: Mcx<'mcx>, stmt: &AlterStatsStmt<'_>) -> PgResult<ObjectAddress> {
    let stxoid: Oid;
    let address;
    let mut newtarget: i32 = 0;
    let newtarget_default: bool;

    /* -1 was used in previous versions for the default setting */
    if let Some(target) = int_val_opt(stmt.stxstattarget.as_deref()) {
        if target != -1 {
            newtarget = target;
            newtarget_default = false;
        } else {
            newtarget_default = true;
        }
    } else {
        newtarget_default = true;
    }

    if !newtarget_default {
        /* Limit statistics target to a sane range */
        if newtarget < 0 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!("statistics target {newtarget} is too low"))
                .into_error());
        } else if newtarget > MAX_STATISTICS_TARGET {
            newtarget = MAX_STATISTICS_TARGET;
            ereport(WARNING)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!("lowering statistics target to {newtarget}"))
                .finish(errloc(673, "AlterStatistics"))?;
        }
    }

    let defnames: Vec<Option<String>> = stmt
        .defnames
        .iter()
        .map(|n| Some(str_val(n).to_string()))
        .collect();

    /* lookup OID of the statistics object */
    stxoid = backend_catalog_namespace::get_statistics_object_oid(mcx, &defnames, stmt.missing_ok)?;

    /*
     * If the OID is invalid the object does not exist but IF EXISTS was given;
     * report a NOTICE and return.
     */
    if stxoid == 0 {
        /* Assert(stmt->missing_ok); */
        let (schemaname, statname) =
            backend_catalog_namespace::DeconstructQualifiedName(mcx, &defnames)?;

        if let Some(schemaname) = schemaname {
            ereport(NOTICE)
                .errmsg(format!(
                    "statistics object \"{schemaname}.{statname}\" does not exist, skipping"
                ))
                .finish(errloc(698, "AlterStatistics"))?;
        } else {
            ereport(NOTICE)
                .errmsg(format!("statistics object \"{statname}\" does not exist, skipping"))
                .finish(errloc(702, "AlterStatistics"))?;
        }

        return Ok(InvalidObjectAddress);
    }

    /* Search pg_statistic_ext */
    let rel = table_open(mcx, StatisticExtRelationId, RowExclusiveLock)?;

    let oldtup = statext_search_tuple::call(mcx, stxoid)?;
    let oldtup = match oldtup {
        Some(t) => t,
        None => {
            return Err(ereport(ERROR)
                .errmsg(format!("cache lookup failed for extended statistics object {stxoid}"))
                .into_error());
        }
    };

    /* Must be owner of the existing statistics object */
    if !object_ownercheck::call(StatisticExtRelationId, stxoid, get_user_id::call())? {
        let objname = backend_catalog_namespace::NameListToString(mcx, &defnames)?;
        aclcheck_error::call(
            types_acl::acl::ACLCHECK_NOT_OWNER,
            ObjectType::StatisticExt,
            Some(objname.as_str().to_string()),
        )?;
    }

    /*
     * Build new tuple: replace the stxstattarget column.  An explicit target is
     * Int16GetDatum(newtarget); the default leaves the column NULL.
     */
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let natts = types_catalog::pg_statistic_ext::Natts_pg_statistic_ext;
    let mut repl_val: Vec<Datum> = Vec::with_capacity(natts);
    let mut repl_null: Vec<bool> = Vec::with_capacity(natts);
    let mut repl_repl: Vec<bool> = Vec::with_capacity(natts);
    for _ in 0..natts {
        repl_val.push(Datum::ByVal(0));
        repl_null.push(false);
        repl_repl.push(false);
    }
    let target_idx =
        (types_catalog::pg_statistic_ext::Anum_pg_statistic_ext_stxstattarget - 1) as usize;
    repl_repl[target_idx] = true;
    if !newtarget_default {
        repl_val[target_idx] = Datum::from_i16(newtarget as i16);
    } else {
        repl_null[target_idx] = true;
    }

    let mut newtup = heap_modify_tuple(mcx, &oldtup, &tupdesc, &repl_val, &repl_null, &repl_repl)
        .map_err(types_error::PgError::from)?;

    /* Update system catalog. */
    let otid = newtup.tuple.t_self;
    CatalogTupleUpdate(mcx, &rel, otid, &mut newtup)?;

    invoke_object_post_alter_hook::call(StatisticExtRelationId, stxoid, 0)?;

    address = ObjectAddress {
        classId: StatisticExtRelationId,
        objectId: stxoid,
        objectSubId: 0,
    };

    /*
     * NOTE: because we only support altering the statistics target, there is no
     * need to update dependencies.
     */

    rel.close(RowExclusiveLock)?;

    Ok(address)
}

/* ===========================================================================
 * DROP STATISTICS support.
 * ======================================================================= */

/// `RemoveStatisticsDataById` (statscmds.c:760-780): delete the
/// `pg_statistic_ext_data` row for `(statsOid, inh)`, if it exists.  Does not
/// error when the row is absent.
pub fn RemoveStatisticsDataById<'mcx>(mcx: Mcx<'mcx>, statsOid: Oid, inh: bool) -> PgResult<()> {
    let relation = table_open(mcx, StatisticExtDataRelationId, RowExclusiveLock)?;

    let tup = statext_data_search_tuple::call(mcx, statsOid, inh)?;

    /* We don't know if the data row for inh value exists. */
    if let Some(tup) = tup {
        CatalogTupleDelete(mcx, &relation, tup.tuple.t_self)?;
    }

    relation.close(RowExclusiveLock)?;
    Ok(())
}

/// `RemoveStatisticsById` (statscmds.c:785-829): guts of statistics-object
/// deletion — delete the `pg_statistic_ext` tuple plus both
/// `pg_statistic_ext_data` rows, locking the underlying table and sending a
/// relcache inval.
pub fn RemoveStatisticsById<'mcx>(mcx: Mcx<'mcx>, statsOid: Oid) -> PgResult<()> {
    /*
     * Delete the pg_statistic_ext tuple.  Also send a cache inval on the
     * associated table so dependent plans rebuild.
     */
    let relation = table_open(mcx, StatisticExtRelationId, RowExclusiveLock)?;

    let tup = statext_search_tuple::call(mcx, statsOid)?;
    let tup = match tup {
        Some(t) => t,
        None => {
            /* should not happen */
            return Err(ereport(ERROR)
                .errmsg(format!("cache lookup failed for statistics object {statsOid}"))
                .into_error());
        }
    };

    let relid = statext_get_relid::call(statsOid)?.ok_or_else(|| {
        ereport(ERROR)
            .errmsg(format!("cache lookup failed for statistics object {statsOid}"))
            .into_error()
    })?;

    /*
     * Delete the pg_statistic_ext_data tuples holding the actual statistical
     * data (with/without inheritance).  Lock the user table first.
     */
    let rel = table_open(mcx, relid, ShareUpdateExclusiveLock)?;

    RemoveStatisticsDataById(mcx, statsOid, true)?;
    RemoveStatisticsDataById(mcx, statsOid, false)?;

    CacheInvalidateRelcacheByRelid(relid)?;

    CatalogTupleDelete(mcx, &relation, tup.tuple.t_self)?;

    /* Keep lock until the end of the transaction. */
    rel.close(NoLock)?;

    relation.close(RowExclusiveLock)?;
    Ok(())
}

/* ===========================================================================
 * Name choosing (statscmds.c:847-930).
 * ======================================================================= */

/// `ChooseExtendedStatisticName` (statscmds.c:847-876): pick a non-conflicting
/// name for a new statistics object.  Digits are appended to `label` until the
/// name is unique within `namespaceid`.
fn ChooseExtendedStatisticName(
    name1: &str,
    name2: &str,
    label: &str,
    namespaceid: Oid,
) -> PgResult<String> {
    let mut pass = 0;
    let stxname;
    /* try the unmodified label first */
    let mut modlabel = label.to_string();

    loop {
        let candidate = make_object_name::call(name1, name2, &modlabel)?;

        let existingstats = get_statext_oid::call(&candidate, namespaceid)?;
        if existingstats == 0 {
            stxname = candidate;
            break;
        }

        /* found a conflict, so try a new name component */
        pass += 1;
        modlabel = format!("{label}{pass}");
    }

    Ok(stxname)
}

/// `ChooseExtendedStatisticNameAddition` (statscmds.c:889-930): generate the
/// "name2" component for a new statistics object from its column-name list.
fn ChooseExtendedStatisticNameAddition(exprs: &[types_nodes::nodes::NodePtr<'_>]) -> String {
    /* char buf[NAMEDATALEN * 2]; */
    let mut buf: Vec<u8> = Vec::with_capacity(NAMEDATALEN * 2);

    for selem_node in exprs {
        /* It should be one of these, but just skip if it happens not to be */
        let selem = match (&**selem_node).node_tag() {
            ntag::T_StatsElem => (&**selem_node).expect_statselem(),
            _ => continue,
        };

        /*
         * Fixed 'expr' for expressions (empty column names).
         */
        let name: &str = selem.name.as_ref().map(|s| s.as_str()).unwrap_or("expr");

        if !buf.is_empty() {
            buf.push(b'_'); /* insert _ between names */
        }

        // strlcpy(buf + buflen, name, NAMEDATALEN): copy up to NAMEDATALEN-1
        // bytes of `name`.
        let name_bytes = name.as_bytes();
        let copy_len = name_bytes.len().min(NAMEDATALEN - 1);
        buf.extend_from_slice(&name_bytes[..copy_len]);

        if buf.len() >= NAMEDATALEN {
            break;
        }
    }

    String::from_utf8_lossy(&buf).into_owned()
}

/* ===========================================================================
 * StatisticsGetRelation (statscmds.c:936-956).
 * ======================================================================= */

/// `StatisticsGetRelation`: given a statistics object's OID, get the OID of the
/// relation it is defined on.  Returns `InvalidOid` (0) when `missing_ok` and
/// the object is gone.
pub fn StatisticsGetRelation(statId: Oid, missing_ok: bool) -> PgResult<Oid> {
    match statext_get_relid::call(statId)? {
        Some(r) => {
            /* Assert(stx->oid == statId) */
            Ok(r)
        }
        None => {
            if missing_ok {
                return Ok(0);
            }
            Err(ereport(ERROR)
                .errmsg(format!("cache lookup failed for statistics object {statId}"))
                .into_error())
        }
    }
}

/// `statscmds.c` owns the `RemoveStatisticsById` inward seam (dependency.c's
/// `doDeletion` for `OCLASS_STATISTIC_EXT`).  Installed here.
pub fn init_seams() {
    backend_commands_statscmds_seams::RemoveStatisticsById::set(remove_statistics_by_id_seam);
}

/// Seam shim for [`RemoveStatisticsById`]: the inward seam carries no `mcx`
/// (the C `RemoveStatisticsById(Oid)` allocates in `CurrentMemoryContext`).
/// There is no ambient context, so the owner creates its own (the standard
/// sibling pattern, e.g. `RemoveConstraintById`).
fn remove_statistics_by_id_seam(stats_oid: Oid) -> PgResult<()> {
    let ctx = MemoryContext::new("RemoveStatisticsById");
    RemoveStatisticsById(ctx.mcx(), stats_oid)
}
