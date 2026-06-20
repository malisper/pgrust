//! The constraint-cooker / attribute-mutate half of `catalog/heap.c`.
//!
//! Ported faithfully here:
//!   * [`cookDefault`] / [`cookConstraint`] — raw-parsetree → cooked `Expr`
//!     (working in `Expr`, wrapped to `Node::Expr` at the storage boundary);
//!   * the generated-column walkers `check_nested_generated` /
//!     `check_virtual_generated_security` (via `expression_tree_walker`);
//!   * the constraint writers `StoreRelCheck` / `StoreRelNotNull` /
//!     `StoreConstraints` / [`AddRelationNewConstraints`] /
//!     [`AddRelationNotNullConstraints`];
//!   * [`SetRelationNumChecks`] — the relchecks read + the
//!     `relchecks == numchecks` `CacheInvalidateRelcache` branch is real; the
//!     disk-store branch is a mirror-and-panic seam (the trimmed
//!     `PgClassForm` carries no `relchecks` to write back);
//!   * the attribute-mutate family `RemoveAttributeById` /
//!     `RelationClearMissing` / `StoreAttrMissingVal` — these need a writable
//!     full-row `ATTNUM` syscache copy + a `pg_attribute` `CatalogTupleUpdate`
//!     carrier (and `construct_array`-of-missingval / `OidFunctionCall3
//!     F_ARRAY_IN`) the typed catalog-write model does not yet expose; each
//!     drives a mirror-and-panic seam declared in `backend-catalog-heap-seams`.
//!   * `MergeWithExistingConstraint` likewise drives a mirror-and-panic seam
//!     (it needs a `conbin` reader + an extended `pg_constraint` field-update
//!     carrier).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::cell::RefCell;

use backend_utils_error::ereport;
use mcx::{alloc_in, Mcx, PgString, PgVec};
use types_core::primitive::{AttrNumber, InvalidOid, Oid, OidIsValid};
use types_core::catalog::FirstUnpinnedObjectId;
use types_error::{
    PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_DUPLICATE_OBJECT,
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_COLUMN_REFERENCE,
    ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_INVALID_TABLE_DEFINITION, ERRCODE_UNDEFINED_COLUMN,
    ERROR,
};
use types_nodes::ddlnodes::{CoercionContext, ConstrType, Constraint};
use types_nodes::nodes::{ntag, Node, NodePtr};
use types_nodes::parsestmt::ParseExprKind;
use types_nodes::primnodes::{CoercionForm, Expr};
use types_rel::Relation;
use types_storage::lock::AccessShareLock;
use types_tuple::access::{ATTRIBUTE_GENERATED_VIRTUAL, RELKIND_PARTITIONED_TABLE};

/* pg_constraint contype codes (catalog/pg_constraint.h). */
const CONSTRAINT_CHECK: i8 = b'c' as i8;
const CONSTRAINT_NOTNULL: i8 = b'n' as i8;

/* InvalidAttrNumber (access/attnum.h). */
const InvalidAttrNumber: AttrNumber = 0;

/* ----------------------------------------------------------------
 *  Expr ↔ Node bridge helper.
 * ---------------------------------------------------------------- */

/// Wrap a cooked [`Expr`] into a `Node::Expr` `NodePtr` for the storage
/// boundary (`nodeToString` / `CreateConstraintEntry` take `&Node`).
fn expr_to_nodeptr<'mcx>(mcx: Mcx<'mcx>, expr: Expr) -> PgResult<NodePtr<'mcx>> {
    alloc_in(mcx, Node::mk_expr(mcx, expr)?)
}

/* ================================================================
 *  cookDefault / cookConstraint
 * ================================================================ */

/// `cookDefault` (heap.c) — take a raw default and convert it to a cooked
/// `Expr` ready for storage. Returns `Ok(None)` when `transformExpr` yields a
/// NULL expression (the C `expr == NULL` case the caller checks).
pub fn cookDefault<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut types_nodes::parsestmt::ParseState<'mcx>,
    raw_default: NodePtr<'mcx>,
    atttypid: Oid,
    atttypmod: i32,
    attname: &str,
    attgenerated: i8,
) -> PgResult<Option<Expr>> {
    // Assert(raw_default != NULL); — the NodePtr is non-null by construction.

    /*
     * Transform raw parsetree to executable expression.
     */
    let raw_node: Node<'mcx> = (*raw_default).clone_in(mcx)?;
    let expr_kind = if attgenerated != 0 {
        ParseExprKind::EXPR_KIND_GENERATED_COLUMN
    } else {
        ParseExprKind::EXPR_KIND_COLUMN_DEFAULT
    };
    let mut expr = match backend_parser_parse_expr::transformExpr(pstate, Some(raw_node), expr_kind)?
    {
        Some(e) => e,
        None => return Ok(None),
    };

    if attgenerated != 0 {
        /* Disallow refs to other generated columns */
        let expr_node = Node::mk_expr(mcx, expr)?;
        check_nested_generated(mcx, pstate, &expr_node)?;
        let Some(e) = expr_node.into_expr() else {
            unreachable!("wrapped Expr node");
        };
        expr = e;

        /* Disallow mutable functions */
        if backend_optimizer_util_clauses::deferred::contain_mutable_functions_after_planning(
            mcx,
            expr.clone_in(mcx)?,
        )? {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg("generation expression is not immutable")
                .into_error());
        }

        /* Check security of expressions for virtual generated column */
        if attgenerated == ATTRIBUTE_GENERATED_VIRTUAL as i8 {
            let expr_node = Node::mk_expr(mcx, expr)?;
            check_virtual_generated_security(mcx, pstate, &expr_node)?;
            let Some(e) = expr_node.into_expr() else {
                unreachable!("wrapped Expr node");
            };
            expr = e;
        }
    } else {
        /*
         * For a default expression, transformExpr() should have rejected
         * column references.
         */
        debug_assert!(!backend_optimizer_util_vars::var::contain_var_clause(&Node::mk_expr(mcx, 
            expr.clone_in(mcx)?
        )?));
    }

    /*
     * Coerce the expression to the correct type and typmod, if given.
     */
    if OidIsValid(atttypid) {
        let type_id = backend_nodes_core::nodefuncs::expr_type(Some(&expr))?;

        let coerced = backend_parser_coerce::coerce_to_target_type(
            mcx,
            Some(pstate),
            expr,
            type_id,
            atttypid,
            atttypmod,
            CoercionContext::COERCION_ASSIGNMENT,
            CoercionForm::COERCE_IMPLICIT_CAST,
            -1,
        )?;
        expr = match coerced {
            Some(e) => e,
            None => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_DATATYPE_MISMATCH)
                    .errmsg(format!(
                        "column \"{attname}\" is of type {} but default expression is of type {}",
                        crate::format_type_be_pub(atttypid)?,
                        crate::format_type_be_pub(type_id)?,
                    ))
                    .errhint("You will need to rewrite or cast the expression.")
                    .into_error());
            }
        };
    }

    /*
     * Finally, take care of collations in the finished expression.
     */
    backend_parser_parse_collate::assign_expr_collations(Some(pstate), &mut expr)?;

    Ok(Some(expr))
}

/// `cookConstraint` (heap.c) — take a raw CHECK constraint expression and
/// convert it to a cooked boolean `Expr` ready for storage.
fn cookConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut types_nodes::parsestmt::ParseState<'mcx>,
    raw_constraint: NodePtr<'mcx>,
    relname: &str,
) -> PgResult<Expr> {
    /*
     * Transform raw parsetree to executable expression.
     */
    let raw_node: Node<'mcx> = (*raw_constraint).clone_in(mcx)?;
    let expr = backend_parser_parse_expr::transformExpr(
        pstate,
        Some(raw_node),
        ParseExprKind::EXPR_KIND_CHECK_CONSTRAINT,
    )?;
    let expr = expr.unwrap_or_else(|| unreachable!("CHECK constraint cannot be NULL"));

    /*
     * Make sure it yields a boolean result.
     */
    let mut expr = backend_parser_coerce::coerce_to_boolean(mcx, Some(pstate), expr, "CHECK")?;

    /*
     * Take care of collations.
     */
    backend_parser_parse_collate::assign_expr_collations(Some(pstate), &mut expr)?;

    /*
     * Make sure no outside relations are referred to.
     */
    if pstate.p_rtable.len() != 1 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
            .errmsg(format!(
                "only table \"{relname}\" can be referenced in check constraint"
            ))
            .into_error());
    }

    Ok(expr)
}

/* ================================================================
 *  Generated-column expression walkers.
 * ================================================================ */

/// `check_nested_generated_walker` (heap.c) — bool walker carrying any
/// `ereport` out-of-band via `err`. Returns `true` to abort the walk.
fn check_nested_generated_walker(
    mcx: Mcx<'_>,
    node: &Node,
    pstate: &types_nodes::parsestmt::ParseState<'_>,
    err: &RefCell<Option<PgError>>,
) -> bool {
    match node.node_tag() {
        ntag::T_Var => {
            let var = node.expect_var();
            // relid = rt_fetch(var->varno, pstate->p_rtable)->relid;
            let idx = var.varno as usize;
            if idx == 0 || idx > pstate.p_rtable.len() {
                return false;
            }
            let relid = pstate.p_rtable[idx - 1].relid;
            if !OidIsValid(relid) {
                return false; /* XXX shouldn't we raise an error? */
            }

            let attnum = var.varattno;

            if attnum > 0 {
                match backend_utils_cache_lsyscache_seams::get_attgenerated::call(relid, attnum) {
                    Ok(g) if g != 0 => {
                        let colname = match backend_utils_cache_lsyscache_seams::get_attname::call(
                            mcx,
                            relid,
                            attnum,
                            false,
                        ) {
                            Ok(Some(s)) => s.as_str().to_string(),
                            Ok(None) => String::new(),
                            Err(e) => {
                                *err.borrow_mut() = Some(e);
                                return true;
                            }
                        };
                        let e = ereport(ERROR)
                            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                            .errmsg(format!(
                                "cannot use generated column \"{colname}\" in column generation expression"
                            ))
                            .errdetail(
                                "A generated column cannot reference another generated column.",
                            )
                            .into_error();
                        *err.borrow_mut() = Some(e);
                        return true;
                    }
                    Ok(_) => {}
                    Err(e) => {
                        *err.borrow_mut() = Some(e);
                        return true;
                    }
                }
            }
            /* A whole-row Var is necessarily self-referential, so forbid it */
            if attnum == 0 {
                let e = ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("cannot use whole-row variable in column generation expression")
                    .errdetail(
                        "This would cause the generated column to depend on its own value.",
                    )
                    .into_error();
                *err.borrow_mut() = Some(e);
                return true;
            }
            /* System columns were already checked in the parser */
            false
        }
        _ => backend_nodes_core::node_walker::expression_tree_walker(node, &mut |n| {
            check_nested_generated_walker(mcx, n, pstate, err)
        }),
    }
}

/// `check_nested_generated` (heap.c).
fn check_nested_generated<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &types_nodes::parsestmt::ParseState<'mcx>,
    node: &Node,
) -> PgResult<()> {
    let err: RefCell<Option<PgError>> = RefCell::new(None);
    check_nested_generated_walker(mcx, node, pstate, &err);
    if let Some(e) = err.into_inner() {
        return Err(e);
    }
    Ok(())
}

/// `contains_user_functions_checker` (heap.c) — `func_id >= FirstUnpinnedObjectId`.
fn contains_user_functions_checker(func_id: Oid) -> bool {
    func_id >= FirstUnpinnedObjectId
}

/// `check_virtual_generated_security_walker` (heap.c).
fn check_virtual_generated_security_walker(
    mcx: Mcx<'_>,
    node: &Node,
    pstate: &types_nodes::parsestmt::ParseState<'_>,
    err: &RefCell<Option<PgError>>,
) -> bool {
    if !node.is_list() {
        if let Some(expr) = node.as_expr() {
            let mut e_owned = match expr.clone_in(mcx) {
                Ok(v) => v,
                Err(e) => {
                    *err.borrow_mut() = Some(e);
                    return true;
                }
            };
            let mut checker = contains_user_functions_checker;
            match backend_nodes_core::nodefuncs::check_functions_in_node(&mut e_owned, &mut checker) {
                Ok(true) => {
                    let location =
                        backend_nodes_nodeFuncs_seams::exprLocation::call(expr);
                    let e = ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("generation expression uses user-defined function")
                        .errdetail("Virtual generated columns that make use of user-defined functions are not yet supported.")
                        .errposition(backend_parser_small1::parser_errposition(pstate, location))
                        .into_error();
                    *err.borrow_mut() = Some(e);
                    return true;
                }
                Ok(false) => {}
                Err(e) => {
                    *err.borrow_mut() = Some(e);
                    return true;
                }
            }

            /*
             * check_functions_in_node() doesn't check some node types. We
             * handle CoerceToDomain and MinMaxExpr by checking for built-in
             * types.
             */
            match backend_nodes_core::nodefuncs::expr_type(Some(expr)) {
                Ok(t) if t >= FirstUnpinnedObjectId => {
                    let location =
                        backend_nodes_nodeFuncs_seams::exprLocation::call(expr);
                    let e = ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("generation expression uses user-defined type")
                        .errdetail("Virtual generated columns that make use of user-defined types are not yet supported.")
                        .errposition(backend_parser_small1::parser_errposition(pstate, location))
                        .into_error();
                    *err.borrow_mut() = Some(e);
                    return true;
                }
                Ok(_) => {}
                Err(e) => {
                    *err.borrow_mut() = Some(e);
                    return true;
                }
            }
        }
    }

    backend_nodes_core::node_walker::expression_tree_walker(node, &mut |n| {
        check_virtual_generated_security_walker(mcx, n, pstate, err)
    })
}

/// `check_virtual_generated_security` (heap.c).
fn check_virtual_generated_security<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &types_nodes::parsestmt::ParseState<'mcx>,
    node: &Node,
) -> PgResult<()> {
    let err: RefCell<Option<PgError>> = RefCell::new(None);
    check_virtual_generated_security_walker(mcx, node, pstate, &err);
    if let Some(e) = err.into_inner() {
        return Err(e);
    }
    Ok(())
}

/* ================================================================
 *  StoreRelCheck / StoreRelNotNull / StoreConstraints
 * ================================================================ */

/// `StoreRelCheck` (heap.c) — store a check-constraint expression for the
/// given relation. The caller updates the pg_class constraint count.
fn StoreRelCheck<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    ccname: &str,
    expr: &Node<'mcx>,
    is_enforced: bool,
    is_validated: bool,
    is_local: bool,
    inhcount: i16,
    is_no_inherit: bool,
    is_internal: bool,
) -> PgResult<Oid> {
    /*
     * Flatten expression to string form for storage.
     */
    let ccbin = backend_nodes_outfuncs::nodeToString(mcx, expr)?;

    /*
     * Find columns of rel that are used in expr. (pull_var_clause is okay
     * because we don't allow subselects in check constraints.)
     */
    let var_list = backend_optimizer_util_vars::var::pull_var_clause(mcx, expr, 0)?;

    let attnos: Vec<i16> = {
        let mut out: Vec<i16> = Vec::with_capacity(var_list.len());
        for v in var_list.iter() {
            if let Some(var) = v.as_var() {
                let va = var.varattno;
                if !out.iter().any(|&a| a == va) {
                    out.push(va);
                }
            }
        }
        out
    };
    let keycount = attnos.len() as i32;

    /*
     * Partitioned tables do not contain any rows themselves, so a NO INHERIT
     * constraint makes no sense.
     */
    if is_no_inherit && rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
            .errmsg(format!(
                "cannot add NO INHERIT constraint to partitioned table \"{}\"",
                rel.rd_rel.relname.as_str()
            ))
            .into_error());
    }

    /*
     * Create the Check Constraint.
     */
    let constr_oid = backend_catalog_pg_constraint::CreateConstraintEntry(
        mcx,
        ccname,
        rel.rd_rel.relnamespace,
        CONSTRAINT_CHECK,
        false, /* Is Deferrable */
        false, /* Is Deferred */
        is_enforced,
        is_validated,
        InvalidOid, /* no parent constraint */
        rel.rd_id,
        &attnos,
        keycount,
        keycount,
        InvalidOid, /* not a domain constraint */
        InvalidOid, /* no associated index */
        InvalidOid, /* Foreign key fields */
        &[],
        &[],
        &[],
        &[],
        0,
        b' ' as i8,
        b' ' as i8,
        &[],
        0,
        b' ' as i8,
        None,         /* not an exclusion constraint */
        Some(expr),   /* Tree form of check constraint */
        Some(ccbin.as_str()),
        is_local,
        inhcount,
        is_no_inherit,
        false, /* conperiod */
        is_internal,
    )?;

    Ok(constr_oid)
}

/// `StoreRelNotNull` (heap.c) — store a not-null constraint for the given
/// relation.
fn StoreRelNotNull<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    nnname: &str,
    attnum: AttrNumber,
    is_validated: bool,
    is_local: bool,
    inhcount: i16,
    is_no_inherit: bool,
) -> PgResult<Oid> {
    debug_assert!(attnum > InvalidAttrNumber);

    let attnums = [attnum];
    let constr_oid = backend_catalog_pg_constraint::CreateConstraintEntry(
        mcx,
        nnname,
        rel.rd_rel.relnamespace,
        CONSTRAINT_NOTNULL,
        false,
        false,
        true, /* Is Enforced */
        is_validated,
        InvalidOid,
        rel.rd_id,
        &attnums,
        1,
        1,
        InvalidOid,
        InvalidOid,
        InvalidOid,
        &[],
        &[],
        &[],
        &[],
        0,
        b' ' as i8,
        b' ' as i8,
        &[],
        0,
        b' ' as i8,
        None,
        None,
        None,
        is_local,
        inhcount,
        is_no_inherit,
        false,
        false,
    )?;
    Ok(constr_oid)
}

/* ================================================================
 *  SetRelationNumChecks
 * ================================================================ */

/// `SetRelationNumChecks` (heap.c) — update the count of constraints in the
/// relation's pg_class tuple. The `relchecks == numchecks` branch (force a
/// relcache inval but skip the disk update) is real; the disk-store branch is
/// a mirror-and-panic seam (the trimmed `PgClassForm` carries no `relchecks`
/// to write back).
fn SetRelationNumChecks<'mcx>(_mcx: Mcx<'mcx>, rel: &Relation<'mcx>, numchecks: i32) -> PgResult<()> {
    // C: relrel = table_open(RelationRelationId, RowExclusiveLock);
    //    reltup = SearchSysCacheCopy1(RELOID, relid);  GETSTRUCT->relchecks
    let cur = backend_utils_cache_syscache_seams::fetch_relchecks::call(rel.rd_id)?;
    let cur = cur.ok_or_else(|| {
        ereport(ERROR)
            .errmsg_internal(format!("cache lookup failed for relation {}", rel.rd_id))
            .into_error()
    })?;

    if cur as i32 != numchecks {
        // relStruct->relchecks = numchecks; CatalogTupleUpdate(...).
        // The trimmed PgClassForm carries no relchecks field, and no
        // pg_class relchecks-set carrier exists in the typed catalog-write
        // model — mirror-and-panic on the store branch.
        backend_catalog_heap_seams::set_relation_num_checks::call(rel.rd_id, numchecks)?;
    } else {
        /* Skip the disk update, but force relcache inval anyway */
        backend_utils_cache_inval::cache_invalidate::CacheInvalidateRelcache(rel)?;
    }

    Ok(())
}

/* ================================================================
 *  StoreConstraints
 * ================================================================ */

/// `StoreConstraints` (heap.c) — store defaults and CHECK constraints passed
/// as a list of `CookedConstraint` (carried here as `Node`s). Only pre-cooked
/// expressions are passed this way (constraints inherited from an existing
/// relation).
pub fn StoreConstraints<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    cooked_constraints: &[NodePtr<'mcx>],
    is_internal: bool,
) -> PgResult<()> {
    if cooked_constraints.is_empty() {
        return Ok(()); /* nothing to do */
    }

    /*
     * Deparsing of constraint expressions will fail unless the just-created
     * pg_attribute tuples for this relation are made visible. So, bump the
     * command counter.
     */
    backend_access_transam_xact::CommandCounterIncrement()?;

    let mut numchecks = 0;

    for con_node in cooked_constraints.iter() {
        let Some(con) = (&**con_node).as_constraint() else {
            return Err(ereport(ERROR)
                .errmsg_internal("StoreConstraints: expected cooked CookedConstraint node")
                .into_error());
        };
        match con.contype {
            ConstrType::CONSTR_DEFAULT => {
                let expr = con
                    .raw_expr
                    .as_ref()
                    .ok_or_else(|| {
                        ereport(ERROR)
                            .errmsg_internal("cooked DEFAULT missing expr")
                            .into_error()
                    })?;
                // attnum carried in con->location field of the cooked node
                // (see CookedConstraint node mapping). C reads con->attnum.
                let attnum = con.location as AttrNumber;
                backend_catalog_pg_attrdef::StoreAttrDefault(
                    mcx, rel.rd_id, attnum, expr, is_internal,
                )?;
            }
            ConstrType::CONSTR_CHECK => {
                let expr = con.raw_expr.as_ref().ok_or_else(|| {
                    ereport(ERROR)
                        .errmsg_internal("cooked CHECK missing expr")
                        .into_error()
                })?;
                let name = con
                    .conname
                    .as_ref()
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default();
                StoreRelCheck(
                    mcx,
                    rel,
                    &name,
                    expr,
                    con.is_enforced,
                    !con.skip_validation,
                    con.initially_valid, // is_local carried in initially_valid for cooked
                    con.location as i16, // inhcount carried in location for cooked (CookedConstraint.inhcount)
                    con.is_no_inherit,
                    is_internal,
                )?;
                numchecks += 1;
            }
            other => {
                return Err(ereport(ERROR)
                    .errmsg_internal(format!("unrecognized constraint type: {}", other as i32))
                    .into_error());
            }
        }
    }

    if numchecks > 0 {
        SetRelationNumChecks(mcx, rel, numchecks)?;
    }

    Ok(())
}

/* ================================================================
 *  MergeWithExistingConstraint
 * ================================================================ */

/// `MergeWithExistingConstraint` (heap.c) — check for a pre-existing CHECK
/// constraint conflicting with a proposed new one. The full body (pg_constraint
/// scan + `conbin` reader + `equal(expr, stringToNode(conbin))` comparison +
/// `conislocal`/`coninhcount`/`connoinherit`/`conenforced`/`convalidated`
/// field-update) lives in the pg_constraint owner crate; we delegate to it.
fn MergeWithExistingConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    ccname: &str,
    expr: &Node<'mcx>,
    allow_merge: bool,
    is_local: bool,
    is_enforced: bool,
    is_initially_valid: bool,
    is_no_inherit: bool,
) -> PgResult<bool> {
    backend_catalog_pg_constraint::MergeWithExistingConstraint(
        mcx,
        rel.rd_id,
        &rel.rd_rel.relname.as_str().to_string(),
        rel.rd_rel.relispartition,
        ccname,
        expr,
        allow_merge,
        is_local,
        is_enforced,
        is_initially_valid,
        is_no_inherit,
    )
}

/* ================================================================
 *  AddRelationNewConstraints
 * ================================================================ */

/// Build a `Node::Constraint` carrying the cooked result (the C
/// `CookedConstraint`). The seam consumer reads only `conname` (and `contype`),
/// so the remaining fields are the empty / default image.
fn make_cooked_node<'mcx>(
    mcx: Mcx<'mcx>,
    contype: ConstrType,
    conname: Option<&str>,
    attnum: AttrNumber,
    expr: Option<NodePtr<'mcx>>,
    is_enforced: bool,
    skip_validation: bool,
    is_local: bool,
    // The cooked `CookedConstraint.inhcount` is not read back by the seam
    // consumer (which only harvests `conname`/`contype`), so it is dropped in
    // the Constraint-carrier mapping — mirroring the C cooked struct's other
    // fields we likewise do not re-store.
    _inhcount: i16,
    is_no_inherit: bool,
) -> PgResult<NodePtr<'mcx>> {
    let conname = match conname {
        Some(s) => Some(PgString::from_str_in(s, mcx)?),
        None => None,
    };
    let c = Constraint {
        contype,
        conname,
        deferrable: false,
        initdeferred: false,
        // We reuse the Constraint carrier for the cooked result; the
        // cooked-only fields (attnum, is_enforced, skip_validation, is_local,
        // inhcount) ride the spare scalar slots so the seam consumer can read
        // them back without a separate node type. `initially_valid` carries
        // is_local; `location` carries attnum (matching StoreConstraints).
        is_enforced,
        skip_validation,
        initially_valid: is_local,
        is_no_inherit,
        raw_expr: expr,
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
        fk_matchtype: 0,
        fk_upd_action: 0,
        fk_del_action: 0,
        fk_del_set_cols: PgVec::new_in(mcx),
        old_conpfeqop: PgVec::new_in(mcx),
        old_pktable_oid: InvalidOid,
        location: attnum as i32,
    };
    alloc_in(mcx, Node::mk_constraint(mcx, c)?)
}

/// `AddRelationNewConstraints` (heap.c) — add new column default / generation
/// expressions and/or CHECK constraints to an existing relation. Returns the
/// list of cooked constraints (carried as `Node::Constraint`).
pub fn AddRelationNewConstraints<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    new_col_defaults: &[(AttrNumber, NodePtr<'mcx>, i8)],
    new_constraints: &[NodePtr<'mcx>],
    allow_merge: bool,
    is_local: bool,
    is_internal: bool,
    query_string: Option<&str>,
) -> PgResult<PgVec<'mcx, NodePtr<'mcx>>> {
    let mut cooked_constraints: PgVec<'mcx, NodePtr<'mcx>> = PgVec::new_in(mcx);

    /*
     * Get info about existing constraints.
     */
    let numoldchecks = match rel.rd_att.constr.as_ref() {
        Some(c) => c.num_check as i32,
        None => 0,
    };

    /*
     * Create a dummy ParseState and insert the target relation as its sole
     * rangetable entry.
     */
    let mut pstate = backend_parser_small1::make_parsestate(mcx, None)?;
    if let Some(qs) = query_string {
        pstate.p_sourcetext = Some(PgString::from_str_in(qs, mcx)?);
    }
    let nsitem = backend_parser_relation::addRangeTableEntryForRelation(
        mcx,
        &mut pstate,
        rel,
        AccessShareLock,
        None,
        false,
        true,
    )?;
    backend_parser_relation::addNSItemToQuery(mcx, &mut pstate, nsitem, true, true, true)?;

    /*
     * Process column default expressions.
     */
    for (attnum, raw_default, generated) in new_col_defaults.iter() {
        let attnum = *attnum;
        let generated = *generated;
        let atp = rel.rd_att.attr((attnum - 1) as usize);
        let atttypid = atp.atttypid;
        let atttypmod = atp.atttypmod;
        let aname = String::from_utf8_lossy(atp.attname.name_str()).into_owned();

        let expr = cookDefault(
            mcx,
            &mut pstate,
            alloc_in(mcx, (**raw_default).clone_in(mcx)?)?,
            atttypid,
            atttypmod,
            &aname,
            generated,
        )?;

        /*
         * If the expression is just a NULL constant, we do not bother to make
         * an explicit pg_attrdef entry (column defaults only, not generation
         * expressions).
         */
        let is_null_const = expr
            .as_ref()
            .and_then(|e| e.as_const())
            .is_some_and(|c| c.constisnull);
        let expr = match expr {
            None => continue,
            Some(_) if generated == 0 && is_null_const => continue,
            Some(e) => e,
        };

        let expr_node = expr_to_nodeptr(mcx, expr)?;
        let def_oid =
            backend_catalog_pg_attrdef::StoreAttrDefault(mcx, rel.rd_id, attnum, &expr_node, is_internal)?;

        let cooked = make_cooked_node(
            mcx,
            ConstrType::CONSTR_DEFAULT,
            None,
            attnum,
            Some(expr_node),
            true,
            false,
            is_local,
            if is_local { 0 } else { 1 },
            false,
        )?;
        let _ = def_oid;
        cooked_constraints.push(cooked);
    }

    /*
     * Process constraint expressions.
     */
    let mut numchecks = numoldchecks;
    let mut checknames: Vec<String> = Vec::new();
    let mut nnnames: Vec<String> = Vec::new();

    for cdef_node in new_constraints.iter() {
        let Some(cdef) = (&**cdef_node).as_constraint() else {
            continue;
        };

        if cdef.contype == ConstrType::CONSTR_CHECK {
            let expr_node: NodePtr<'mcx>;
            if let Some(raw) = cdef.raw_expr.as_ref() {
                debug_assert!(cdef.cooked_expr.is_none());
                /*
                 * Transform raw parsetree to executable expression, and verify
                 * it's valid as a CHECK constraint.
                 */
                let expr = cookConstraint(
                    mcx,
                    &mut pstate,
                    alloc_in(mcx, (**raw).clone_in(mcx)?)?,
                    &rel.rd_rel.relname.as_str().to_string(),
                )?;
                expr_node = expr_to_nodeptr(mcx, expr)?;
            } else {
                debug_assert!(cdef.cooked_expr.is_some());
                /*
                 * Here, we assume the parser will only pass us valid CHECK
                 * expressions, so we do no particular checking.
                 *   expr = stringToNode(cdef->cooked_expr);
                 */
                let cooked = cdef.cooked_expr.as_ref().unwrap().as_str().to_string();
                expr_node = backend_nodes_read_seams::string_to_node::call(mcx, &cooked)?;
            }

            /*
             * Check name uniqueness, or generate a name if none was given.
             */
            let ccname: String;
            if let Some(cn) = cdef.conname.as_ref() {
                let cn = cn.as_str().to_string();
                /* Check against other new constraints */
                for chkname in checknames.iter() {
                    if *chkname == cn {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_DUPLICATE_OBJECT)
                            .errmsg(format!("check constraint \"{cn}\" already exists"))
                            .into_error());
                    }
                }
                /* save name for future checks */
                checknames.push(cn.clone());

                /*
                 * Check against pre-existing constraints. If we are allowed to
                 * merge with an existing constraint, there's no more to do.
                 */
                if MergeWithExistingConstraint(
                    mcx,
                    rel,
                    &cn,
                    &expr_node,
                    allow_merge,
                    is_local,
                    cdef.is_enforced,
                    cdef.initially_valid,
                    cdef.is_no_inherit,
                )? {
                    continue;
                }
                ccname = cn;
            } else {
                /*
                 * Generate a name. Approximate column- vs table-constraint by
                 * whether the expression references more than one column.
                 */
                let vars = backend_optimizer_util_vars::var::pull_var_clause(mcx, &expr_node, 0)?;
                /* eliminate duplicates */
                let mut uniq_attnos: Vec<AttrNumber> = Vec::new();
                for v in vars.iter() {
                    if let Some(var) = v.as_var() {
                        if !uniq_attnos.iter().any(|&a| a == var.varattno) {
                            uniq_attnos.push(var.varattno);
                        }
                    }
                }
                let colname: Option<String> = if uniq_attnos.len() == 1 {
                    backend_utils_cache_lsyscache_seams::get_attname::call(
                        mcx,
                        rel.rd_id,
                        uniq_attnos[0],
                        true,
                    )?
                    .map(|s| s.as_str().to_string())
                } else {
                    None
                };

                ccname = backend_catalog_pg_constraint::ChooseConstraintName(
                    mcx,
                    &rel.rd_rel.relname.as_str().to_string(),
                    colname.as_deref().unwrap_or(""),
                    "check",
                    rel.rd_rel.relnamespace,
                    &checknames,
                )?;
                /* save name for future checks */
                checknames.push(ccname.clone());
            }

            /*
             * OK, store it.
             */
            let _constr_oid = StoreRelCheck(
                mcx,
                rel,
                &ccname,
                &expr_node,
                cdef.is_enforced,
                cdef.initially_valid,
                is_local,
                if is_local { 0 } else { 1 },
                cdef.is_no_inherit,
                is_internal,
            )?;

            numchecks += 1;

            let cooked = make_cooked_node(
                mcx,
                ConstrType::CONSTR_CHECK,
                Some(&ccname),
                0,
                Some(expr_node),
                cdef.is_enforced,
                cdef.skip_validation,
                is_local,
                if is_local { 0 } else { 1 },
                cdef.is_no_inherit,
            )?;
            cooked_constraints.push(cooked);
        } else if cdef.contype == ConstrType::CONSTR_NOTNULL {
            let inhcount: i16 = if is_local { 0 } else { 1 };

            /* Determine which column to modify */
            let keyname = key_strval(cdef)?;
            let colnum = backend_utils_cache_lsyscache_seams::get_attnum::call(rel.rd_id, &keyname)?;
            if colnum == InvalidAttrNumber {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_COLUMN)
                    .errmsg(format!(
                        "column \"{keyname}\" of relation \"{}\" does not exist",
                        rel.rd_rel.relname.as_str()
                    ))
                    .into_error());
            }
            if colnum < InvalidAttrNumber {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg(format!(
                        "cannot add not-null constraint on system column \"{keyname}\""
                    ))
                    .into_error());
            }

            debug_assert!(cdef.initially_valid != cdef.skip_validation);

            /*
             * If the column already has a not-null constraint, adjust
             * inheritance status as needed (this also checks validity match).
             */
            let cdef_conname = cdef.conname.as_ref().map(|s| s.as_str().to_string());
            if backend_catalog_pg_constraint::AdjustNotNullInheritance(
                mcx,
                rel.rd_id,
                colnum,
                cdef_conname.as_deref(),
                is_local,
                cdef.is_no_inherit,
                cdef.skip_validation,
            )? {
                continue;
            }

            /*
             * Resolve the constraint name.
             */
            let nnname: String;
            if let Some(cn) = cdef_conname.as_ref() {
                if backend_catalog_pg_constraint::ConstraintNameIsUsed(
                    mcx,
                    types_catalog::pg_constraint::ConstraintCategory::Relation,
                    rel.rd_id,
                    cn,
                )? {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_DUPLICATE_OBJECT)
                        .errmsg(format!(
                            "constraint \"{cn}\" for relation \"{}\" already exists",
                            rel.rd_rel.relname.as_str()
                        ))
                        .into_error());
                }
                nnname = cn.clone();
            } else {
                nnname = backend_catalog_pg_constraint::ChooseConstraintName(
                    mcx,
                    &rel.rd_rel.relname.as_str().to_string(),
                    &keyname,
                    "not_null",
                    rel.rd_rel.relnamespace,
                    &nnnames,
                )?;
            }
            nnnames.push(nnname.clone());

            let _constr_oid = StoreRelNotNull(
                mcx,
                rel,
                &nnname,
                colnum,
                cdef.initially_valid,
                is_local,
                inhcount,
                cdef.is_no_inherit,
            )?;

            let nncooked = make_cooked_node(
                mcx,
                ConstrType::CONSTR_NOTNULL,
                Some(&nnname),
                colnum,
                None,
                true,
                cdef.skip_validation,
                is_local,
                inhcount,
                cdef.is_no_inherit,
            )?;
            cooked_constraints.push(nncooked);
        }
    }

    /*
     * Update the count of constraints in the relation's pg_class tuple. We do
     * this even if there was no change, to ensure an SI update is sent out.
     */
    SetRelationNumChecks(mcx, rel, numchecks)?;

    Ok(cooked_constraints)
}

/// `strVal(linitial(cdef->keys))` — the first key column name of a NOT NULL
/// `Constraint`.
fn key_strval(cdef: &Constraint<'_>) -> PgResult<String> {
    let first = cdef.keys.first().ok_or_else(|| {
        ereport(ERROR)
            .errmsg_internal("not-null constraint with empty key list")
            .into_error()
    })?;
    match first.node_tag() {
        ntag::T_String => Ok(first.expect_string().sval.as_str().to_string()),
        _ => Err(ereport(ERROR)
            .errmsg_internal("not-null constraint key is not a String value")
            .into_error()),
    }
}

/* ================================================================
 *  AddRelationNotNullConstraints
 * ================================================================ */

/// `AddRelationNotNullConstraints` (heap.c) — create the not-null constraints
/// when creating a new relation, merging directly-declared constraints with
/// inherited ones. Returns the list of column attnums that gained the
/// constraint.
pub fn AddRelationNotNullConstraints<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    constraints: &[NodePtr<'mcx>],
    old_notnulls: &[NodePtr<'mcx>],
    existing_constraints: &[String],
) -> PgResult<PgVec<'mcx, AttrNumber>> {
    let mut nncols: PgVec<'mcx, AttrNumber> = PgVec::new_in(mcx);

    // nnnames = list_copy(existing_constraints); givennames = NIL;
    let mut nnnames: Vec<String> = existing_constraints.to_vec();
    let mut givennames: Vec<String> = Vec::new();

    // Build a mutable working list of the directly-specified constraints so we
    // can mirror the C's index-based inner-loop deletion + conname carryover.
    struct WorkConstr {
        keyname: String,
        is_no_inherit: bool,
        conname: Option<String>,
    }
    let mut work: Vec<WorkConstr> = Vec::with_capacity(constraints.len());
    for c in constraints.iter() {
        let Some(cdef) = (&**c).as_constraint() else {
            return Err(ereport(ERROR)
                .errmsg_internal("AddRelationNotNullConstraints: expected Constraint node")
                .into_error());
        };
        debug_assert!(cdef.contype == ConstrType::CONSTR_NOTNULL);
        work.push(WorkConstr {
            keyname: key_strval(cdef)?,
            is_no_inherit: cdef.is_no_inherit,
            conname: cdef.conname.as_ref().map(|s| s.as_str().to_string()),
        });
    }

    // old_notnulls working list of (attnum, name).
    let mut old: Vec<(AttrNumber, Option<String>)> = Vec::with_capacity(old_notnulls.len());
    for c in old_notnulls.iter() {
        let Some(cooked) = (&**c).as_constraint() else {
            return Err(ereport(ERROR)
                .errmsg_internal("AddRelationNotNullConstraints: expected cooked node")
                .into_error());
        };
        debug_assert!(cooked.contype == ConstrType::CONSTR_NOTNULL);
        let attnum = cooked.location as AttrNumber;
        let name = cooked.conname.as_ref().map(|s| s.as_str().to_string());
        old.push((attnum, name));
    }

    /*
     * First, create all not-null constraints directly specified by the user.
     */
    let mut outerpos = 0;
    while outerpos < work.len() {
        let attnum = backend_utils_cache_lsyscache_seams::get_attnum::call(
            rel.rd_id,
            &work[outerpos].keyname,
        )?;
        if attnum == InvalidAttrNumber {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_COLUMN)
                .errmsg(format!(
                    "column \"{}\" of relation \"{}\" does not exist",
                    work[outerpos].keyname,
                    rel.rd_rel.relname.as_str()
                ))
                .into_error());
        }
        if attnum < InvalidAttrNumber {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!(
                    "cannot add not-null constraint on system column \"{}\"",
                    work[outerpos].keyname
                ))
                .into_error());
        }

        let mut inhcount = 0;

        /*
         * A column can only have one not-null constraint; discard additional
         * ones for columns we already saw (checking NO INHERIT / name match).
         */
        let outer_key = work[outerpos].keyname.clone();
        let outer_noinh = work[outerpos].is_no_inherit;
        let mut restpos = outerpos + 1;
        while restpos < work.len() {
            if work[restpos].keyname == outer_key {
                if work[restpos].is_no_inherit != outer_noinh {
                    return Err(ereport(ERROR)
                        .errcode(types_error::ERRCODE_SYNTAX_ERROR)
                        .errmsg(format!(
                            "conflicting NO INHERIT declaration for not-null constraint on column \"{outer_key}\""
                        ))
                        .into_error());
                }
                if let Some(other_name) = work[restpos].conname.clone() {
                    match work[outerpos].conname.clone() {
                        None => work[outerpos].conname = Some(other_name),
                        Some(my) if my != other_name => {
                            return Err(ereport(ERROR)
                                .errcode(types_error::ERRCODE_SYNTAX_ERROR)
                                .errmsg(format!(
                                    "conflicting not-null constraint names \"{my}\" and \"{other_name}\""
                                ))
                                .into_error());
                        }
                        Some(_) => {}
                    }
                }
                work.remove(restpos);
            } else {
                restpos += 1;
            }
        }

        /*
         * Search inherited constraints on the same column; determine an
         * inheritance count, deleting processed entries.
         */
        let mut oi = 0;
        while oi < old.len() {
            if old[oi].0 == attnum {
                if outer_noinh {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_DATATYPE_MISMATCH)
                        .errmsg(format!(
                            "cannot define not-null constraint with NO INHERIT on column \"{outer_key}\""
                        ))
                        .errdetail("The column has an inherited not-null constraint.")
                        .into_error());
                }
                inhcount += 1;
                old.remove(oi);
            } else {
                oi += 1;
            }
        }

        /*
         * Determine a constraint name.
         */
        let conname: String;
        if let Some(cn) = work[outerpos].conname.clone() {
            for thisname in givennames.iter() {
                if *thisname == cn {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_DUPLICATE_OBJECT)
                        .errmsg(format!(
                            "constraint \"{cn}\" for relation \"{}\" already exists",
                            rel.rd_rel.relname.as_str()
                        ))
                        .into_error());
                }
            }
            conname = cn;
            givennames.push(conname.clone());
        } else {
            let colname = backend_utils_cache_lsyscache_seams::get_attname::call(
                mcx, rel.rd_id, attnum, false,
            )?
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();
            conname = backend_catalog_pg_constraint::ChooseConstraintName(
                mcx,
                &rel.rd_rel.relname.as_str().to_string(),
                &colname,
                "not_null",
                rel.rd_rel.relnamespace,
                &nnnames,
            )?;
        }
        nnnames.push(conname.clone());

        StoreRelNotNull(mcx, rel, &conname, attnum, true, true, inhcount, outer_noinh)?;
        nncols.push(attnum);

        outerpos += 1;
    }

    /*
     * Any column remaining in the old_notnulls list gets a not-local
     * constraint with an appropriate inhcount.
     */
    let mut outerpos = 0;
    while outerpos < old.len() {
        let target_attnum = old[outerpos].0;
        let mut inhcount = 1;
        let mut conname: Option<String> = old[outerpos].1.clone();

        let mut restpos = outerpos + 1;
        while restpos < old.len() {
            if old[restpos].0 == target_attnum {
                if conname.is_none() {
                    conname = old[restpos].1.clone();
                }
                inhcount += 1;
                old.remove(restpos);
            } else {
                restpos += 1;
            }
        }

        /* If we got a name, make sure it isn't one we've already used */
        if let Some(cn) = conname.clone() {
            if nnnames.iter().any(|n| *n == cn) {
                conname = None;
            }
        }

        let conname = match conname {
            Some(cn) => cn,
            None => {
                let colname = backend_utils_cache_lsyscache_seams::get_attname::call(
                    mcx,
                    rel.rd_id,
                    target_attnum,
                    false,
                )?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();
                backend_catalog_pg_constraint::ChooseConstraintName(
                    mcx,
                    &rel.rd_rel.relname.as_str().to_string(),
                    &colname,
                    "not_null",
                    rel.rd_rel.relnamespace,
                    &nnnames,
                )?
            }
        };
        nnnames.push(conname.clone());

        /* ignore the origin constraint's is_local and inhcount */
        StoreRelNotNull(mcx, rel, &conname, target_attnum, true, false, inhcount, false)?;
        nncols.push(target_attnum);

        outerpos += 1;
    }

    Ok(nncols)
}

/* ================================================================
 *  Attribute-mutate family (mirror-and-panic on the writable
 *  pg_attribute carrier keystone).
 * ================================================================ */

/// `RemoveAttributeById` (heap.c) — mark the attribute dropped, reset its
/// nullable fields, rename it, drop its statistics.
///
/// The C grabs `relation_open(relid, AccessExclusiveLock)` (held until end of
/// transaction), `table_open(AttributeRelationId, RowExclusiveLock)`, finds the
/// `(relid, attnum)` pg_attribute row, then `heap_modify_tuple` setting
/// `attisdropped = true`, `atttypid = 0`, `attnotnull = false`,
/// `attgenerated = '\0'`, renaming `attname` to `........pg.dropped.N........`,
/// `atthasmissing = false`, and nulling `attmissingval` / `attstattarget` /
/// `attacl` / `attoptions` / `attfdwoptions`, then `CatalogTupleUpdate`.
///
/// We do the pg_attribute mutation in-crate (the same `systable_beginscan` on
/// `AttributeRelidNumIndexId` + `heap_modify_tuple` + `CatalogTupleUpdate` idiom
/// as `RelationClearMissing` above; `SearchSysCacheCopy2(ATTNUM)` is the keyed
/// single-row case of that scan). The `RemoveStatistics` half is real in-crate
/// and runs after.
pub fn RemoveAttributeById<'mcx>(mcx: Mcx<'mcx>, relid: Oid, attnum: AttrNumber) -> PgResult<()> {
    use backend_access_common_scankey::ScanKeyInit;
    use types_core::fmgr::F_OIDEQ;
    use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
    use types_tuple::backend_access_common_heaptuple::Datum;

    // pg_attribute catalog OID + its (attrelid, attnum) index, and the columns
    // touched (catalog/pg_attribute.h).
    const AttributeRelationId: Oid = 1249;
    const AttributeRelidNumIndexId: Oid = 2659;
    const Anum_pg_attribute_attrelid: AttrNumber = 1;
    const Anum_pg_attribute_attname: AttrNumber = 2;
    const Anum_pg_attribute_atttypid: AttrNumber = 3;
    const Anum_pg_attribute_attnum: AttrNumber = 5;
    const Anum_pg_attribute_attnotnull: AttrNumber = 12;
    const Anum_pg_attribute_atthasmissing: AttrNumber = 14;
    const Anum_pg_attribute_attgenerated: AttrNumber = 16;
    const Anum_pg_attribute_attisdropped: AttrNumber = 17;
    const Anum_pg_attribute_attstattarget: AttrNumber = 21;
    const Anum_pg_attribute_attacl: AttrNumber = 22;
    const Anum_pg_attribute_attoptions: AttrNumber = 23;
    const Anum_pg_attribute_attfdwoptions: AttrNumber = 24;
    const Anum_pg_attribute_attmissingval: AttrNumber = 25;
    const NAMEDATALEN: usize = 64;

    // C: rel = relation_open(relid, AccessExclusiveLock); held until end of
    //    transaction (closed NoLock at the end so the lock is retained).
    let rel =
        backend_access_table_table::table_open(mcx, relid, types_storage::lock::AccessExclusiveLock)?;

    // attr_rel = table_open(AttributeRelationId, RowExclusiveLock);
    let attr_rel = backend_access_table_table::table_open(
        mcx,
        AttributeRelationId,
        types_storage::lock::RowExclusiveLock,
    )?;

    // SearchSysCacheCopy2(ATTNUM, relid, attnum) — keyed single-row case: scan
    // the (attrelid, attnum) index over attrelid = relid, then match attnum.
    let mut key = [ScanKeyData::empty()];
    ScanKeyInit(
        &mut key[0],
        Anum_pg_attribute_attrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(relid),
    )?;

    let mut scan = backend_access_index_genam_seams::systable_beginscan::call(
        &attr_rel,
        AttributeRelidNumIndexId,
        true,
        None,
        &key[..1],
    )?;

    let natts = attr_rel.rd_att.natts as usize;
    let mut found = false;
    loop {
        let Some(tuple) =
            backend_access_index_genam_seams::systable_getnext::call(mcx, scan.desc_mut())?
        else {
            break;
        };

        let (attnum_val, _) = backend_access_common_heaptuple::heap_getattr(
            mcx,
            &tuple,
            Anum_pg_attribute_attnum as i32,
            &attr_rel.rd_att,
        )?;
        if attnum_val.as_i16() != attnum {
            continue;
        }

        // newtuple = heap_modify_tuple(tuple, RelationGetDescr(attr_rel),
        //     valuesAtt, nullsAtt, replacesAtt).
        let mut repl_val = alloc::vec![Datum::null(); natts];
        let mut repl_null = alloc::vec![false; natts];
        let mut repl_repl = alloc::vec![false; natts];

        let set = |val: &mut Vec<Datum<'mcx>>,
                   repl: &mut Vec<bool>,
                   anum: AttrNumber,
                   d: Datum<'mcx>| {
            val[(anum - 1) as usize] = d;
            repl[(anum - 1) as usize] = true;
        };

        // attStruct->attisdropped = true;
        set(&mut repl_val, &mut repl_repl, Anum_pg_attribute_attisdropped, Datum::from_bool(true));
        // attStruct->atttypid = InvalidOid;
        set(&mut repl_val, &mut repl_repl, Anum_pg_attribute_atttypid, Datum::from_oid(InvalidOid));
        // attStruct->attnotnull = false;
        set(&mut repl_val, &mut repl_repl, Anum_pg_attribute_attnotnull, Datum::from_bool(false));
        // attStruct->attgenerated = '\0';
        set(&mut repl_val, &mut repl_repl, Anum_pg_attribute_attgenerated, Datum::from_char(0));

        // snprintf newattname = "........pg.dropped.%d........", attnum;
        // namestrcpy(&attStruct->attname, newattname).
        let newattname = alloc::format!("........pg.dropped.{}........", attnum);
        let mut image: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, NAMEDATALEN)?;
        let src = newattname.as_bytes();
        let take = core::cmp::min(src.len(), NAMEDATALEN - 1);
        for &b in &src[..take] {
            image.push(b);
        }
        while image.len() < NAMEDATALEN {
            image.push(0);
        }
        set(&mut repl_val, &mut repl_repl, Anum_pg_attribute_attname, Datum::ByRef(image));

        // attStruct->atthasmissing = false;
        set(&mut repl_val, &mut repl_repl, Anum_pg_attribute_atthasmissing, Datum::from_bool(false));

        // nullsAtt/replacesAtt: attmissingval, attstattarget, attacl,
        // attoptions, attfdwoptions := NULL.
        for anum in [
            Anum_pg_attribute_attmissingval,
            Anum_pg_attribute_attstattarget,
            Anum_pg_attribute_attacl,
            Anum_pg_attribute_attoptions,
            Anum_pg_attribute_attfdwoptions,
        ] {
            repl_null[(anum - 1) as usize] = true;
            repl_repl[(anum - 1) as usize] = true;
        }

        let mut newtuple = backend_access_common_heaptuple::heap_modify_tuple(
            mcx,
            &tuple,
            &attr_rel.rd_att,
            &repl_val,
            &repl_null,
            &repl_repl,
        )?;

        // CatalogTupleUpdate(attr_rel, &tuple->t_self, tuple);
        backend_catalog_indexing::keystone::CatalogTupleUpdate(
            mcx,
            &attr_rel,
            tuple.tuple.t_self,
            &mut newtuple,
        )?;
        found = true;
        break;
    }

    scan.end()?;

    if !found {
        // elog(ERROR, "cache lookup failed for attribute %d of relation %u")
        return Err(ereport(ERROR)
            .errmsg_internal(format!(
                "cache lookup failed for attribute {} of relation {}",
                attnum, relid
            ))
            .into_error());
    }

    // table_close(attr_rel, RowExclusiveLock);
    attr_rel.close(types_storage::lock::RowExclusiveLock)?;

    crate::statistics::RemoveStatistics(mcx, relid, attnum)?;

    // relation_close(rel, NoLock) — keep the AccessExclusiveLock until end of
    // transaction.
    rel.close(types_storage::lock::NoLock)?;
    Ok(())
}

/// `RelationClearMissing` (heap.c) — clear `atthasmissing` and null
/// `attmissingval` for every (user) column of `rel` where `atthasmissing` is
/// set. Safe + useful when the table is rewritten (VACUUM FULL / CLUSTER) so
/// no rows can be missing a full attribute complement. The caller holds an
/// `AccessExclusive` lock on the relation.
///
/// The C reads each row through `SearchSysCache2(ATTNUM, relid, attnum)` for
/// `attnum` 1..=natts; the equivalent here is a keyed `systable_beginscan` on
/// the `AttributeRelidNumIndexId` over `attrelid = relid` (the same idiom as
/// `DeleteAttributeTuples`), filtered to user columns (`attnum >= 1`,
/// including dropped). For each row with `atthasmissing` set, `heap_modify_tuple`
/// clears `atthasmissing` and nulls `attmissingval`, then `CatalogTupleUpdate`.
pub fn RelationClearMissing<'mcx>(mcx: Mcx<'mcx>, rel: &Relation<'mcx>) -> PgResult<()> {
    use backend_access_common_scankey::ScanKeyInit;
    use types_core::fmgr::F_OIDEQ;
    use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
    use types_tuple::backend_access_common_heaptuple::Datum;

    // pg_attribute catalog OID + its (attrelid, attnum) index, and the columns
    // touched (catalog/pg_attribute.h).
    const AttributeRelationId: Oid = 1249;
    const AttributeRelidNumIndexId: Oid = 2659;
    const Anum_pg_attribute_attrelid: AttrNumber = 1;
    const Anum_pg_attribute_attnum: AttrNumber = 6;
    const Anum_pg_attribute_atthasmissing: AttrNumber = 14;
    const Anum_pg_attribute_attmissingval: AttrNumber = 25;

    let relid = rel.rd_id;

    // attr_rel = table_open(AttributeRelationId, RowExclusiveLock);
    let attr_rel = backend_access_table_table::table_open(
        mcx,
        AttributeRelationId,
        types_storage::lock::RowExclusiveLock,
    )?;

    // Use the index to scan only attributes of the target relation.
    let mut key = [ScanKeyData::empty()];
    ScanKeyInit(
        &mut key[0],
        Anum_pg_attribute_attrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(relid),
    )?;

    let mut scan = backend_access_index_genam_seams::systable_beginscan::call(
        &attr_rel,
        AttributeRelidNumIndexId,
        true,
        None,
        &key[..1],
    )?;

    let natts = attr_rel.rd_att.natts as usize;
    loop {
        let Some(tuple) =
            backend_access_index_genam_seams::systable_getnext::call(mcx, scan.desc_mut())?
        else {
            break;
        };

        // attrtuple = (Form_pg_attribute) GETSTRUCT(tuple); the C loop is over
        // attnum 1..=natts (user columns, including dropped). System columns
        // (attnum <= 0) are skipped.
        let (attnum_val, _) = backend_access_common_heaptuple::heap_getattr(
            mcx,
            &tuple,
            Anum_pg_attribute_attnum as i32,
            &attr_rel.rd_att,
        )?;
        if attnum_val.as_i16() < 1 {
            continue;
        }

        // if (attrtuple->atthasmissing)
        let (hasmissing, _) = backend_access_common_heaptuple::heap_getattr(
            mcx,
            &tuple,
            Anum_pg_attribute_atthasmissing as i32,
            &attr_rel.rd_att,
        )?;
        if !hasmissing.as_bool() {
            continue;
        }

        // newtuple = heap_modify_tuple(tuple, RelationGetDescr(attr_rel),
        //     repl_val, repl_null, repl_repl): atthasmissing := false,
        //     attmissingval := NULL.
        let mut repl_val = alloc::vec![Datum::null(); natts];
        let mut repl_null = alloc::vec![false; natts];
        let mut repl_repl = alloc::vec![false; natts];

        repl_val[(Anum_pg_attribute_atthasmissing - 1) as usize] = Datum::from_bool(false);
        repl_repl[(Anum_pg_attribute_atthasmissing - 1) as usize] = true;

        repl_null[(Anum_pg_attribute_attmissingval - 1) as usize] = true;
        repl_repl[(Anum_pg_attribute_attmissingval - 1) as usize] = true;

        let mut newtuple = backend_access_common_heaptuple::heap_modify_tuple(
            mcx,
            &tuple,
            &attr_rel.rd_att,
            &repl_val,
            &repl_null,
            &repl_repl,
        )?;

        // CatalogTupleUpdate(attr_rel, &newtuple->t_self, newtuple);
        backend_catalog_indexing::keystone::CatalogTupleUpdate(
            mcx,
            &attr_rel,
            tuple.tuple.t_self,
            &mut newtuple,
        )?;
    }

    // Our update of the pg_attribute rows forces a relcache rebuild; nothing
    // else to do. table_close(attr_rel, RowExclusiveLock).
    scan.end()?;
    attr_rel.close(types_storage::lock::RowExclusiveLock)
}

/// `StoreAttrDefault`'s pg_attribute update (pg_attrdef.c): after inserting the
/// pg_attrdef row, mark the column's `atthasdef = true` and recover its
/// pre-existing `attgenerated` (so the caller picks the dependency type).
///
/// The C does `SearchSysCacheCopy2(ATTNUM, relid, attnum)` for a single writable
/// tuple; the equivalent here is a keyed `systable_beginscan` on
/// `AttributeRelidNumIndexId` over `attrelid = relid AND attnum = N` (the same
/// idiom as `RelationClearMissing`), then `heap_modify_tuple` setting
/// `atthasdef = true` + `CatalogTupleUpdate`. Returns the pre-existing
/// `attgenerated`; `Ok(None)` on the cache miss (the C
/// `cache lookup failed for attribute %d of relation %u`).
pub fn SetAttributeHasDefault<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: AttrNumber,
) -> PgResult<Option<i8>> {
    use backend_access_common_scankey::ScanKeyInit;
    use types_catalog::pg_attribute::{
        Anum_pg_attribute_attgenerated, Anum_pg_attribute_atthasdef, Anum_pg_attribute_attnum,
        Anum_pg_attribute_attrelid,
    };
    use types_core::fmgr::F_OIDEQ;
    use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
    use types_tuple::backend_access_common_heaptuple::Datum;

    const AttributeRelationId: Oid = 1249;
    const AttributeRelidNumIndexId: Oid = 2659;

    // attrrel = table_open(AttributeRelationId, RowExclusiveLock);
    let attr_rel = backend_access_table_table::table_open(
        mcx,
        AttributeRelationId,
        types_storage::lock::RowExclusiveLock,
    )?;

    // SearchSysCacheCopy2(ATTNUM, relid, attnum): scan on attrelid (the index's
    // leading key) and filter to the target attnum in the loop — the same idiom
    // as `RelationClearMissing` (the genam scan keys only on the index's leading
    // column).
    let mut key = [ScanKeyData::empty()];
    ScanKeyInit(
        &mut key[0],
        Anum_pg_attribute_attrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(relid),
    )?;

    let mut scan = backend_access_index_genam_seams::systable_beginscan::call(
        &attr_rel,
        AttributeRelidNumIndexId,
        true,
        None,
        &key[..1],
    )?;

    let mut result = None;
    loop {
        let Some(tuple) =
            backend_access_index_genam_seams::systable_getnext::call(mcx, scan.desc_mut())?
        else {
            break;
        };

        // Filter to the requested attnum.
        let (this_attnum, _) = backend_access_common_heaptuple::heap_getattr(
            mcx,
            &tuple,
            Anum_pg_attribute_attnum as i32,
            &attr_rel.rd_att,
        )?;
        if this_attnum.as_i16() != attnum {
            continue;
        }

        {
            {
                // attgenerated = attStruct->attgenerated;
                let (attgenerated, _) = backend_access_common_heaptuple::heap_getattr(
                    mcx,
                    &tuple,
                    Anum_pg_attribute_attgenerated as i32,
                    &attr_rel.rd_att,
                )?;
                let attgenerated = attgenerated.as_char();

                // valuesAtt[atthasdef-1] = true; replacesAtt[atthasdef-1] = true;
                let natts = attr_rel.rd_att.natts as usize;
                let mut repl_val = alloc::vec![Datum::null(); natts];
                let repl_null = alloc::vec![false; natts];
                let mut repl_repl = alloc::vec![false; natts];

                repl_val[(Anum_pg_attribute_atthasdef - 1) as usize] = Datum::from_bool(true);
                repl_repl[(Anum_pg_attribute_atthasdef - 1) as usize] = true;

                let mut newtuple = backend_access_common_heaptuple::heap_modify_tuple(
                    mcx,
                    &tuple,
                    &attr_rel.rd_att,
                    &repl_val,
                    &repl_null,
                    &repl_repl,
                )?;

                // CatalogTupleUpdate(attrrel, &atttup->t_self, atttup);
                backend_catalog_indexing::keystone::CatalogTupleUpdate(
                    mcx,
                    &attr_rel,
                    tuple.tuple.t_self,
                    &mut newtuple,
                )?;

                result = Some(attgenerated as i8);
                break;
            }
        }
    }

    scan.end()?;
    attr_rel.close(types_storage::lock::RowExclusiveLock)?;

    Ok(result)
}

/// `RemoveAttrDefaultById`'s `pg_attribute` reset (pg_attrdef.c): clear the
/// owning column's `atthasdef = false`.
///
/// ```c
/// attr_rel = table_open(AttributeRelationId, RowExclusiveLock);
/// tuple = SearchSysCacheCopy2(ATTNUM, ObjectIdGetDatum(myrelid),
///                             Int16GetDatum(myattnum));
/// if (!HeapTupleIsValid(tuple))  /* shouldn't happen */
///     elog(ERROR, "cache lookup failed for attribute %d of relation %u", ...);
/// ((Form_pg_attribute) GETSTRUCT(tuple))->atthasdef = false;
/// CatalogTupleUpdate(attr_rel, &tuple->t_self, tuple);
/// table_close(attr_rel, RowExclusiveLock);
/// ```
///
/// Returns `false` on the cache miss (the C "shouldn't happen" `elog(ERROR)`,
/// left to the caller). Mirrors [`SetAttributeHasDefault`]'s genam scan idiom
/// (key on the `attrelid` leading index column, filter to `attnum`).
pub fn ClearAttributeHasDefault<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: AttrNumber,
) -> PgResult<bool> {
    use backend_access_common_scankey::ScanKeyInit;
    use types_catalog::pg_attribute::{
        Anum_pg_attribute_attnum, Anum_pg_attribute_atthasdef, Anum_pg_attribute_attrelid,
    };
    use types_core::fmgr::F_OIDEQ;
    use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
    use types_tuple::backend_access_common_heaptuple::Datum;

    const AttributeRelationId: Oid = 1249;
    const AttributeRelidNumIndexId: Oid = 2659;

    // attr_rel = table_open(AttributeRelationId, RowExclusiveLock);
    let attr_rel = backend_access_table_table::table_open(
        mcx,
        AttributeRelationId,
        types_storage::lock::RowExclusiveLock,
    )?;

    // SearchSysCacheCopy2(ATTNUM, relid, attnum): scan on attrelid (the index's
    // leading key) and filter to the target attnum in the loop.
    let mut key = [ScanKeyData::empty()];
    ScanKeyInit(
        &mut key[0],
        Anum_pg_attribute_attrelid,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(relid),
    )?;

    let mut scan = backend_access_index_genam_seams::systable_beginscan::call(
        &attr_rel,
        AttributeRelidNumIndexId,
        true,
        None,
        &key[..1],
    )?;

    let mut found = false;
    loop {
        let Some(tuple) =
            backend_access_index_genam_seams::systable_getnext::call(mcx, scan.desc_mut())?
        else {
            break;
        };

        // Filter to the requested attnum.
        let (this_attnum, _) = backend_access_common_heaptuple::heap_getattr(
            mcx,
            &tuple,
            Anum_pg_attribute_attnum as i32,
            &attr_rel.rd_att,
        )?;
        if this_attnum.as_i16() != attnum {
            continue;
        }

        // ((Form_pg_attribute) GETSTRUCT(tuple))->atthasdef = false;
        let natts = attr_rel.rd_att.natts as usize;
        let mut repl_val = alloc::vec![Datum::null(); natts];
        let repl_null = alloc::vec![false; natts];
        let mut repl_repl = alloc::vec![false; natts];

        repl_val[(Anum_pg_attribute_atthasdef - 1) as usize] = Datum::from_bool(false);
        repl_repl[(Anum_pg_attribute_atthasdef - 1) as usize] = true;

        let mut newtuple = backend_access_common_heaptuple::heap_modify_tuple(
            mcx,
            &tuple,
            &attr_rel.rd_att,
            &repl_val,
            &repl_null,
            &repl_repl,
        )?;

        // CatalogTupleUpdate(attr_rel, &tuple->t_self, tuple);
        backend_catalog_indexing::keystone::CatalogTupleUpdate(
            mcx,
            &attr_rel,
            tuple.tuple.t_self,
            &mut newtuple,
        )?;

        found = true;
        break;
    }

    scan.end()?;
    attr_rel.close(types_storage::lock::RowExclusiveLock)?;

    Ok(found)
}

/// `StoreAttrMissingVal` (heap.c) — set the missing value of a single
/// attribute. Needs `construct_array`-of-missingval + a writable full-row
/// `ATTNUM` syscache copy + a `pg_attribute` `CatalogTupleUpdate` carrier;
/// driven through a mirror-and-panic seam.
pub fn StoreAttrMissingVal<'mcx>(
    _mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    attnum: AttrNumber,
    missingval: types_tuple::backend_access_common_heaptuple::Datum<'mcx>,
) -> PgResult<()> {
    backend_catalog_heap_seams::store_attr_missing_val::call(rel.rd_id, attnum, &missingval)
}
