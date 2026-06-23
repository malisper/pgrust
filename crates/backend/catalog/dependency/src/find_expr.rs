//! `find_expr_references_walker` / `process_function_rte_ref` (dependency.c
//! 1697-2392) — the expression-dependency engine behind
//! [`crate::recordDependencyOnExpr`] / [`crate::recordDependencyOnSingleRelExpr`].
//!
//! Ported 1:1 against this repo's unified [`Node`] enum. The C node-by-node
//! dependency recording, error codes/messages, `DEFAULT_COLLATION_OID`
//! short-circuits, regclass-family `Const` handling, the `Query` rtable /
//! INSERT-UPDATE-targetlist / `constraintDeps` handling, and the
//! `query_tree_walker` / `expression_tree_walker` recursion are preserved.
//!
//! Where dependency.c threads a `find_expr_references_context` (`ObjectAddresses
//! *addrs` + a `List *rtables` of rangetables) by pointer, this port threads an
//! owned [`FindExprReferencesContext`] by `&mut`. `rtables` is a stack of owned
//! rangetables with index 0 = innermost level, so the C `lcons` /
//! `list_delete_first` map to `insert(0, …)` / `remove(0)`. The node-support
//! walkers take a `&mut dyn FnMut(&Node) -> bool` callback that returns `true`
//! to abort; the C walker raises errors via `ereport(ERROR)`, so we stash the
//! first error in `context.err`/a local and surface it after the walk (see
//! [`run_subwalk`]).

use mcx::Mcx;

use nodes_core::node_walker::{
    self, QTW_EXAMINE_SORTGROUP, QTW_IGNORE_JOINALIASES,
};
use nodes_core::nodefuncs::expr_type;
use utils_error::{ereport, elog};

use types_catalog::catalog_dependency::ObjectAddresses;
use types_core::primitive::{AttrNumber, InvalidAttrNumber, Oid};
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_UNDEFINED_COLUMN, ERROR};
use nodes::copy_query::Query;
use nodes::nodes::{ntag, Node, CMD_INSERT, CMD_UPDATE};
use nodes::parsenodes::{
    RangeTblEntry, RTE_FUNCTION, RTE_JOIN, RTE_NAMEDTUPLESTORE, RTE_RELATION,
};
use nodes::primnodes::Expr;
use types_tuple::heaptuple::{DEFAULT_COLLATION_OID, RECORDOID};

// Catalog class OIDs the walker references, aliased to the C `...RelationId`
// identifier spelling so dispatch reads 1:1 against dependency.c.
use types_catalog::catalog::{
    COLLATION_RELATION_ID as CollationRelationId, CONSTRAINT_RELATION_ID as ConstraintRelationId,
    NAMESPACE_RELATION_ID as NamespaceRelationId,
    OPERATOR_FAMILY_RELATION_ID as OperatorFamilyRelationId,
    OPERATOR_RELATION_ID as OperatorRelationId, PROCEDURE_RELATION_ID as ProcedureRelationId,
    RELATION_RELATION_ID as RelationRelationId, TS_CONFIG_RELATION_ID as TSConfigRelationId,
    TS_DICTIONARY_RELATION_ID as TSDictionaryRelationId, TYPE_RELATION_ID as TypeRelationId,
};

use lsyscache_seams as lsyscache_seams;
use syscache_seams as syscache_seams;
use funcapi_seams as funcapi_seams;

use crate::{add_object_address, OidIsValid};

// The REG* constant-type OIDs the regclass-family `Const` arm switches on
// (pg_type.dat, PostgreSQL 18.3).
const REGPROCOID: Oid = 24;
const REGPROCEDUREOID: Oid = 2202;
const REGOPEROID: Oid = 2203;
const REGOPERATOROID: Oid = 2204;
const REGCLASSOID: Oid = 2205;
const REGTYPEOID: Oid = 2206;
const REGCOLLATIONOID: Oid = 4191;
const REGCONFIGOID: Oid = 3734;
const REGDICTIONARYOID: Oid = 3769;
const REGNAMESPACEOID: Oid = 4089;
const REGROLEOID: Oid = 4096;

/// True for the OID-alias (`reg*`) constant datatypes whose `constvalue` is a
/// pass-by-value object OID (`DatumGetObjectId`). Every other constant type
/// (e.g. `text`/`bytea`/`numeric`) carries a by-reference value that must not be
/// read as a scalar OID.
fn is_regclass_like_type(consttype: Oid) -> bool {
    matches!(
        consttype,
        REGPROCOID
            | REGPROCEDUREOID
            | REGOPEROID
            | REGOPERATOROID
            | REGCLASSOID
            | REGTYPEOID
            | REGCOLLATIONOID
            | REGCONFIGOID
            | REGDICTIONARYOID
            | REGNAMESPACEOID
            | REGROLEOID
    )
}

/// `RELKIND_RELATION` (`pg_class.h`).
const RELKIND_RELATION: i8 = b'r' as i8;
/// `AccessShareLock` (`lockdefs.h`).
const ACCESS_SHARE_LOCK: types_storage::lock::LOCKMODE = types_storage::lock::AccessShareLock;

/* ===========================================================================
 * find_expr_references_context — for find_expr_references_walker.
 * ========================================================================= */

/// `typedef struct { ObjectAddresses *addrs; List *rtables; }
/// find_expr_references_context;` (dependency.c).
///
/// `addrs` is the accumulating [`ObjectAddresses`]; `rtables` is the stack of
/// rangetables used to resolve `Var`s, index 0 the innermost (most recently
/// `lcons`'d) level. `err` carries the first error raised inside a node-support
/// sub-walk back out across the abort-returning callback. `mcx` is the arena
/// used to deep-copy a `Query`'s rtable when it must be pushed onto the stack
/// (the C `lcons(query->rtable, …)` shares a pointer; our owned model clones).
pub struct FindExprReferencesContext<'mcx> {
    /// Addresses being accumulated.
    pub addrs: ObjectAddresses,
    /// Stack of rangetables to resolve Vars (index 0 = innermost level).
    pub rtables: Vec<Vec<RangeTblEntry<'mcx>>>,
    /// First error raised inside a node-support sub-walk, surfaced after it
    /// returns. `None` until an arm fails.
    pub err: Option<PgError>,
    /// Arena for deep-copying rtables pushed onto `rtables`.
    pub mcx: Mcx<'mcx>,
}

impl<'mcx> FindExprReferencesContext<'mcx> {
    /// A fresh context with no rangetables and an empty address set.
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        FindExprReferencesContext {
            addrs: crate::new_object_addresses(),
            rtables: Vec::new(),
            err: None,
            mcx,
        }
    }
}

/// Build the one-element bogus rangetable
/// (`recordDependencyOnSingleRelExpr` gins one up to resolve Vars). The single
/// RTE is `RTE_RELATION` for `rel_id`, with `relkind = RELKIND_RELATION` and
/// `rellockmode = AccessShareLock` ("no need for exactness here", per the C).
pub fn bogus_single_rel_rtable<'mcx>(
    mcx: Mcx<'mcx>,
    rel_id: Oid,
) -> Vec<RangeTblEntry<'mcx>> {
    let mut rte = RangeTblEntry::new_in(mcx);
    rte.rtekind = RTE_RELATION;
    rte.relid = rel_id;
    rte.relkind = RELKIND_RELATION;
    rte.rellockmode = ACCESS_SHARE_LOCK;
    vec![rte]
}

/* ===========================================================================
 * find_expr_references_walker (dependency.c:1697-2329)
 * ========================================================================= */

/// Recursively search an expression tree for object references.
pub fn find_expr_references_walker(
    node: &Node<'_>,
    context: &mut FindExprReferencesContext<'_>,
) -> PgResult<bool> {
    // Expr-leaf nodes (Var/Const/.../NextValueExpr) live under `Node::Expr`.
    if let Some(expr) = node.as_expr() {
        return find_expr_references_expr(node, expr, context);
    }

    match node.node_tag() {
        ntag::T_SortGroupClause => {
            let sgc = node.expect_sortgroupclause();
            add_object_address(OperatorRelationId, sgc.eqop, 0, &mut context.addrs);
            if OidIsValid(sgc.sortop) {
                add_object_address(OperatorRelationId, sgc.sortop, 0, &mut context.addrs);
            }
            Ok(false)
        }
        ntag::T_WindowClause => {
            let wc = node.expect_windowclause();
            if OidIsValid(wc.startInRangeFunc) {
                add_object_address(ProcedureRelationId, wc.startInRangeFunc, 0, &mut context.addrs);
            }
            if OidIsValid(wc.endInRangeFunc) {
                add_object_address(ProcedureRelationId, wc.endInRangeFunc, 0, &mut context.addrs);
            }
            if OidIsValid(wc.inRangeColl) && wc.inRangeColl != DEFAULT_COLLATION_OID {
                add_object_address(CollationRelationId, wc.inRangeColl, 0, &mut context.addrs);
            }
            /* fall through to examine substructure */
            run_subwalk(node, context)
        }
        ntag::T_Query => {
            let query = node.expect_query();
            find_expr_references_query(node, query, context)
        }
        ntag::T_SetOperationStmt => {
            let setop = node.expect_setoperationstmt();
            /*
             * we need to look at the groupClauses for operator references.
             *
             * C: `find_expr_references_walker((Node *) setop->groupClauses,
             * context)` — the argument is the `List *` itself; the `T_List` arm
             * of the walker visits each element. Our `groupClauses` is a
             * list of `Node *`, so call the walker on each element directly.
             */
            for gc in setop.groupClauses.iter() {
                find_expr_references_walker(&**gc, context)?;
            }
            /* fall through to examine child nodes */
            run_subwalk(node, context)
        }
        ntag::T_RangeTblFunction => {
            let rtfunc = node.expect_rangetblfunction();
            /*
             * Add refs for any datatypes and collations used in a column
             * definition list for a RECORD function. (For other cases, it
             * should be enough to depend on the function itself.)
             */
            for ct in rtfunc.funccoltypes.iter() {
                add_object_address(TypeRelationId, *ct, 0, &mut context.addrs);
            }
            for collid in rtfunc.funccolcollations.iter() {
                if OidIsValid(*collid) && *collid != DEFAULT_COLLATION_OID {
                    add_object_address(CollationRelationId, *collid, 0, &mut context.addrs);
                }
            }
            run_subwalk(node, context)
        }
        ntag::T_OnConflictExpr => {
            let onconflict = node.expect_onconflictexpr();
            if OidIsValid(onconflict.constraint) {
                add_object_address(ConstraintRelationId, onconflict.constraint, 0, &mut context.addrs);
            }
            /* fall through to examine arguments */
            run_subwalk(node, context)
        }
        ntag::T_TableSampleClause => {
            let tsc = node.expect_tablesampleclause();
            add_object_address(ProcedureRelationId, tsc.tsmhandler, 0, &mut context.addrs);
            /* fall through to examine arguments */
            run_subwalk(node, context)
        }
        ntag::T_TableFunc => {
            let tf = node.expect_tablefunc();
            /*
             * Add refs for the datatypes and collations used in the TableFunc.
             */
            if let Some(coltypes) = tf.coltypes.as_ref() {
                for ct in coltypes.iter() {
                    add_object_address(TypeRelationId, *ct, 0, &mut context.addrs);
                }
            }
            if let Some(colcollations) = tf.colcollations.as_ref() {
                for collid in colcollations.iter() {
                    if OidIsValid(*collid) && *collid != DEFAULT_COLLATION_OID {
                        add_object_address(CollationRelationId, *collid, 0, &mut context.addrs);
                    }
                }
            }
            /* fall through to examine substructure */
            run_subwalk(node, context)
        }
        ntag::T_CTECycleClause => {
            let cc = node.expect_ctecycleclause();
            if OidIsValid(cc.cycle_mark_type) {
                add_object_address(TypeRelationId, cc.cycle_mark_type, 0, &mut context.addrs);
            }
            if OidIsValid(cc.cycle_mark_collation) {
                add_object_address(CollationRelationId, cc.cycle_mark_collation, 0, &mut context.addrs);
            }
            if OidIsValid(cc.cycle_mark_neop) {
                add_object_address(OperatorRelationId, cc.cycle_mark_neop, 0, &mut context.addrs);
            }
            /* fall through to examine substructure */
            run_subwalk(node, context)
        }
        _ => run_subwalk(node, context),
    }
}

/// The `Node::Expr` arms (dependency.c's `IsA(node, Var)` … `NextValueExpr`).
fn find_expr_references_expr(
    node: &Node<'_>,
    expr: &Expr,
    context: &mut FindExprReferencesContext<'_>,
) -> PgResult<bool> {
    match expr {
        Expr::Var(var) => {
            /* Find matching rtable entry, or complain if not found */
            if (var.varlevelsup as usize) >= context.rtables.len() {
                return Err(elog(ERROR, format!("invalid varlevelsup {}", var.varlevelsup))
                    .unwrap_err());
            }
            {
                let rtable = &context.rtables[var.varlevelsup as usize];
                if var.varno <= 0 || (var.varno as usize) > rtable.len() {
                    return Err(elog(ERROR, format!("invalid varno {}", var.varno)).unwrap_err());
                }
            }
            /* rt_fetch(varno, rtable) == rtable[varno - 1] */

            /*
             * A whole-row Var references no specific columns, so adds no new
             * dependency. (We assume that there is a whole-table dependency
             * arising from each underlying rangetable entry.)
             */
            if var.varattno == InvalidAttrNumber {
                return Ok(false);
            }
            let rtekind = context.rtables[var.varlevelsup as usize][(var.varno - 1) as usize].rtekind;
            if rtekind == RTE_RELATION {
                /* If it's a plain relation, reference this column */
                let relid = context.rtables[var.varlevelsup as usize][(var.varno - 1) as usize].relid;
                add_object_address(RelationRelationId, relid, var.varattno as i32, &mut context.addrs);
            } else if rtekind == RTE_FUNCTION {
                /* Might need to add a dependency on a composite type's column */
                /* (done out of line, because it's a bit bulky) */
                let attnum = var.varattno;
                // Deep-clone the RTE so the borrow on `context` is released for
                // the out-of-line helper (which mutates `context.addrs`).
                let rte_owned = context.rtables[var.varlevelsup as usize][(var.varno - 1) as usize]
                    .clone_in(context.mcx)?;
                process_function_rte_ref(&rte_owned, attnum, context)?;
            }

            /*
             * Vars referencing other RTE types require no additional work. In
             * particular, a join alias Var can be ignored, because it must
             * reference a merged USING column.
             */
            Ok(false)
        }
        Expr::Const(con) => {
            /* A constant must depend on the constant's datatype */
            add_object_address(TypeRelationId, con.consttype, 0, &mut context.addrs);

            /*
             * We must also depend on the constant's collation: it could be
             * different from the datatype's, if a CollateExpr was const-folded
             * to a simple constant. However we can save work in the most common
             * case where the collation is "default", since we know that's
             * pinned.
             */
            if OidIsValid(con.constcollid) && con.constcollid != DEFAULT_COLLATION_OID {
                add_object_address(CollationRelationId, con.constcollid, 0, &mut context.addrs);
            }

            /*
             * If it's a regclass or similar literal referring to an existing
             * object, add a reference to that object. (Currently, only the
             * regclass and regconfig cases have any likely use, but we may as
             * well handle all the OID-alias datatypes consistently.)
             */
            /*
             * Only the OID-alias (reg*) datatypes carry an object reference in
             * the constant value. C reads `DatumGetObjectId(con->constvalue)`
             * unconditionally — harmless there because Datum is a bare machine
             * word — but here the canonical by-reference Datum (e.g. a `text`
             * default like 'text') cannot be read as a scalar OID and would
             * panic. Decode the OID only for the reg* types that actually use
             * it; for every other type the value is never inspected (matching
             * C's behaviour, since none of the switch arms below fire).
             */
            if !con.constisnull && is_regclass_like_type(con.consttype) {
                let objoid = con.constvalue.as_oid(); /* DatumGetObjectId */
                if con.consttype == REGPROCOID || con.consttype == REGPROCEDUREOID {
                    if syscache_seams::procoid_exists::call(objoid)? {
                        add_object_address(ProcedureRelationId, objoid, 0, &mut context.addrs);
                    }
                } else if con.consttype == REGOPEROID || con.consttype == REGOPERATOROID {
                    if syscache_seams::operoid_exists::call(objoid)? {
                        add_object_address(OperatorRelationId, objoid, 0, &mut context.addrs);
                    }
                } else if con.consttype == REGCLASSOID {
                    if syscache_seams::reloid_exists::call(objoid)? {
                        add_object_address(RelationRelationId, objoid, 0, &mut context.addrs);
                    }
                } else if con.consttype == REGTYPEOID {
                    if syscache_seams::typeoid_exists::call(objoid)? {
                        add_object_address(TypeRelationId, objoid, 0, &mut context.addrs);
                    }
                } else if con.consttype == REGCOLLATIONOID {
                    if syscache_seams::colloid_exists::call(objoid)? {
                        add_object_address(CollationRelationId, objoid, 0, &mut context.addrs);
                    }
                } else if con.consttype == REGCONFIGOID {
                    if syscache_seams::tsconfigoid_exists::call(objoid)? {
                        add_object_address(TSConfigRelationId, objoid, 0, &mut context.addrs);
                    }
                } else if con.consttype == REGDICTIONARYOID {
                    if syscache_seams::tsdictoid_exists::call(objoid)? {
                        add_object_address(TSDictionaryRelationId, objoid, 0, &mut context.addrs);
                    }
                } else if con.consttype == REGNAMESPACEOID {
                    if syscache_seams::namespaceoid_exists::call(objoid)? {
                        add_object_address(NamespaceRelationId, objoid, 0, &mut context.addrs);
                    }
                } else if con.consttype == REGROLEOID {
                    /*
                     * Dependencies for regrole should be shared among all
                     * databases, so explicitly inhibit to have dependencies.
                     */
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg(format!(
                            "constant of the type {} cannot be used here",
                            "regrole"
                        ))
                        .into_error());
                }
            }
            Ok(false)
        }
        Expr::Param(param) => {
            /* A parameter must depend on the parameter's datatype */
            add_object_address(TypeRelationId, param.paramtype, 0, &mut context.addrs);
            /* and its collation, just as for Consts */
            if OidIsValid(param.paramcollid) && param.paramcollid != DEFAULT_COLLATION_OID {
                add_object_address(CollationRelationId, param.paramcollid, 0, &mut context.addrs);
            }
            /* fall through to examine arguments */
            run_subwalk(node, context)
        }
        Expr::FuncExpr(funcexpr) => {
            add_object_address(ProcedureRelationId, funcexpr.funcid, 0, &mut context.addrs);
            /* fall through to examine arguments */
            run_subwalk(node, context)
        }
        Expr::OpExpr(opexpr) => {
            add_object_address(OperatorRelationId, opexpr.opno, 0, &mut context.addrs);
            /* fall through to examine arguments */
            run_subwalk(node, context)
        }
        Expr::DistinctExpr(distinctexpr) => {
            add_object_address(OperatorRelationId, distinctexpr.opno, 0, &mut context.addrs);
            /* fall through to examine arguments */
            run_subwalk(node, context)
        }
        Expr::NullIfExpr(nullifexpr) => {
            add_object_address(OperatorRelationId, nullifexpr.opno, 0, &mut context.addrs);
            /* fall through to examine arguments */
            run_subwalk(node, context)
        }
        Expr::ScalarArrayOpExpr(opexpr) => {
            add_object_address(OperatorRelationId, opexpr.opno, 0, &mut context.addrs);
            /* fall through to examine arguments */
            run_subwalk(node, context)
        }
        Expr::Aggref(aggref) => {
            add_object_address(ProcedureRelationId, aggref.aggfnoid, 0, &mut context.addrs);
            /* fall through to examine arguments */
            run_subwalk(node, context)
        }
        Expr::WindowFunc(wfunc) => {
            add_object_address(ProcedureRelationId, wfunc.winfnoid, 0, &mut context.addrs);
            /* fall through to examine arguments */
            run_subwalk(node, context)
        }
        Expr::SubscriptingRef(sbsref) => {
            /*
             * The refexpr should provide adequate dependency on
             * refcontainertype, and that type in turn depends on refelemtype.
             * However, a custom subscripting handler might set refrestype to
             * something different from either of those, in which case we'd
             * better record it.
             */
            if sbsref.refrestype != sbsref.refcontainertype
                && sbsref.refrestype != sbsref.refelemtype
            {
                add_object_address(TypeRelationId, sbsref.refrestype, 0, &mut context.addrs);
            }
            /* fall through to examine arguments */
            run_subwalk(node, context)
        }
        Expr::SubPlan(_) => {
            /* Extra work needed here if we ever need this case */
            Err(elog(ERROR, "already-planned subqueries not supported").unwrap_err())
        }
        Expr::FieldSelect(fselect) => {
            let argtype =
                lsyscache_seams::get_base_type::call(expr_type(fselect.arg.as_deref())?)?;
            let reltype = lsyscache_seams::get_typ_typrelid::call(argtype)?;

            /*
             * We need a dependency on the specific column named in FieldSelect,
             * assuming we can identify the pg_class OID for it. If we can make a
             * column dependency then we shouldn't need a dependency on the
             * column's type; but if we can't, make a dependency on the type, as
             * it might not appear anywhere else in the expression.
             */
            if OidIsValid(reltype) {
                add_object_address(RelationRelationId, reltype, fselect.fieldnum as i32, &mut context.addrs);
            } else {
                add_object_address(TypeRelationId, fselect.resulttype, 0, &mut context.addrs);
            }
            /* the collation might not be referenced anywhere else, either */
            if OidIsValid(fselect.resultcollid) && fselect.resultcollid != DEFAULT_COLLATION_OID {
                add_object_address(CollationRelationId, fselect.resultcollid, 0, &mut context.addrs);
            }
            run_subwalk(node, context)
        }
        Expr::FieldStore(fstore) => {
            let reltype = lsyscache_seams::get_typ_typrelid::call(fstore.resulttype)?;

            /* similar considerations to FieldSelect, but multiple column(s) */
            if OidIsValid(reltype) {
                for fieldnum in fstore.fieldnums.iter() {
                    add_object_address(RelationRelationId, reltype, *fieldnum as i32, &mut context.addrs);
                }
            } else {
                add_object_address(TypeRelationId, fstore.resulttype, 0, &mut context.addrs);
            }
            run_subwalk(node, context)
        }
        Expr::RelabelType(relab) => {
            /* since there is no function dependency, need to depend on type */
            add_object_address(TypeRelationId, relab.resulttype, 0, &mut context.addrs);
            /* the collation might not be referenced anywhere else, either */
            if OidIsValid(relab.resultcollid) && relab.resultcollid != DEFAULT_COLLATION_OID {
                add_object_address(CollationRelationId, relab.resultcollid, 0, &mut context.addrs);
            }
            run_subwalk(node, context)
        }
        Expr::CoerceViaIO(iocoerce) => {
            /* since there is no exposed function, need to depend on type */
            add_object_address(TypeRelationId, iocoerce.resulttype, 0, &mut context.addrs);
            /* the collation might not be referenced anywhere else, either */
            if OidIsValid(iocoerce.resultcollid) && iocoerce.resultcollid != DEFAULT_COLLATION_OID {
                add_object_address(CollationRelationId, iocoerce.resultcollid, 0, &mut context.addrs);
            }
            run_subwalk(node, context)
        }
        Expr::ArrayCoerceExpr(acoerce) => {
            /* as above, depend on type */
            add_object_address(TypeRelationId, acoerce.resulttype, 0, &mut context.addrs);
            /* the collation might not be referenced anywhere else, either */
            if OidIsValid(acoerce.resultcollid) && acoerce.resultcollid != DEFAULT_COLLATION_OID {
                add_object_address(CollationRelationId, acoerce.resultcollid, 0, &mut context.addrs);
            }
            /* fall through to examine arguments */
            run_subwalk(node, context)
        }
        Expr::ConvertRowtypeExpr(cvt) => {
            /* since there is no function dependency, need to depend on type */
            add_object_address(TypeRelationId, cvt.resulttype, 0, &mut context.addrs);
            run_subwalk(node, context)
        }
        Expr::CollateExpr(coll) => {
            add_object_address(CollationRelationId, coll.collOid, 0, &mut context.addrs);
            run_subwalk(node, context)
        }
        Expr::RowExpr(rowexpr) => {
            add_object_address(TypeRelationId, rowexpr.row_typeid, 0, &mut context.addrs);
            run_subwalk(node, context)
        }
        Expr::RowCompareExpr(rcexpr) => {
            for opno in rcexpr.opnos.iter() {
                add_object_address(OperatorRelationId, *opno, 0, &mut context.addrs);
            }
            for opfamily in rcexpr.opfamilies.iter() {
                add_object_address(OperatorFamilyRelationId, *opfamily, 0, &mut context.addrs);
            }
            /* fall through to examine arguments */
            run_subwalk(node, context)
        }
        Expr::CoerceToDomain(cd) => {
            add_object_address(TypeRelationId, cd.resulttype, 0, &mut context.addrs);
            run_subwalk(node, context)
        }
        Expr::NextValueExpr(nve) => {
            add_object_address(RelationRelationId, nve.seqid, 0, &mut context.addrs);
            run_subwalk(node, context)
        }
        /* Other Expr variants require no additional work; examine substructure. */
        _ => run_subwalk(node, context),
    }
}

/// The `IsA(node, Query)` arm (dependency.c:2136-2262).
fn find_expr_references_query(
    _node: &Node<'_>,
    query: &Query<'_>,
    context: &mut FindExprReferencesContext<'_>,
) -> PgResult<bool> {
    /* Recurse into RTE subquery or not-yet-planned sublink subquery */

    /*
     * Add whole-relation refs for each plain relation mentioned in the
     * subquery's rtable, and ensure we add refs for any type-coercion
     * functions used in join alias lists.
     *
     * Note: query_tree_walker takes care of recursing into RTE_FUNCTION RTEs,
     * subqueries, etc, so no need to do that here. But we must tell it not to
     * visit join alias lists, or we'll add refs for join input columns whether
     * or not they are actually used in our query.
     */
    for rte_index in 0..query.rtable.len() {
        let rtekind = query.rtable[rte_index].rtekind;
        if rtekind == RTE_RELATION {
            let relid = query.rtable[rte_index].relid;
            add_object_address(RelationRelationId, relid, 0, &mut context.addrs);
        } else if rtekind == RTE_JOIN {
            /*
             * Examine joinaliasvars entries only for merged JOIN USING columns.
             * Only those entries could contain type-coercion functions. Also,
             * their join input columns must be referenced in the join quals, so
             * this won't accidentally add refs to otherwise-unused join input
             * columns.
             */
            let cloned_rtable = clone_rtable(&query.rtable, context.mcx)?;
            context.rtables.insert(0, cloned_rtable);
            let joinmergedcols = query.rtable[rte_index].joinmergedcols;
            for i in 0..joinmergedcols {
                // list_nth(joinaliasvars, i)
                let aliasvar = &*query.rtable[rte_index].joinaliasvars[i as usize];
                if !aliasvar.is_var() {
                    find_expr_references_walker(aliasvar, context)?;
                }
            }
            context.rtables.remove(0);
        } else if rtekind == RTE_NAMEDTUPLESTORE {
            /*
             * Cataloged objects cannot depend on tuplestores, because those
             * have no cataloged representation. For now we can call the
             * tuplestore a "transition table" because that's the only kind
             * exposed to SQL, but someday we might have to work harder.
             */
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!(
                    "transition table \"{}\" cannot be referenced in a persistent object",
                    rte_aliasname(&query.rtable[rte_index])
                ))
                .into_error());
        }
        /* Other RTE types can be ignored here */
    }

    /*
     * If the query is an INSERT or UPDATE, we should create a dependency on
     * each target column, to prevent the specific target column from being
     * dropped. Although we will visit the TargetEntry nodes again during
     * query_tree_walker, we won't have enough context to do this conveniently,
     * so do it here.
     */
    if query.commandType == CMD_INSERT || query.commandType == CMD_UPDATE {
        if query.resultRelation <= 0 || query.resultRelation > query.rtable.len() as i32 {
            return Err(
                elog(ERROR, format!("invalid resultRelation {}", query.resultRelation)).unwrap_err(),
            );
        }
        /* rt_fetch(resultRelation, rtable) == rtable[resultRelation - 1] */
        let rte = &query.rtable[(query.resultRelation - 1) as usize];
        if rte.rtekind == RTE_RELATION {
            let relid = rte.relid;
            for tle in query.targetList.iter() {
                if tle.resjunk {
                    continue; /* ignore junk tlist items */
                }
                add_object_address(RelationRelationId, relid, tle.resno as i32, &mut context.addrs);
            }
        }
    }

    /*
     * Add dependencies on constraints listed in query's constraintDeps
     */
    for constr in query.constraintDeps.iter() {
        add_object_address(ConstraintRelationId, *constr, 0, &mut context.addrs);
    }

    /* Examine substructure of query */
    let cloned_rtable = clone_rtable(&query.rtable, context.mcx)?;
    context.rtables.insert(0, cloned_rtable);
    let result = run_query_subwalk(query, context);
    context.rtables.remove(0);
    result
}

/// Deep-copy an rtable (`PgVec<RangeTblEntry>`) into `mcx` so it can be pushed
/// onto the owned `context.rtables` stack — the C `lcons(query->rtable, …)`
/// shares the list pointer, but our owned model has no shared pointer.
fn clone_rtable<'mcx>(
    rtable: &mcx::PgVec<'_, RangeTblEntry<'_>>,
    mcx: Mcx<'mcx>,
) -> PgResult<Vec<RangeTblEntry<'mcx>>> {
    let mut out = Vec::with_capacity(rtable.len());
    for rte in rtable.iter() {
        out.push(rte.clone_in(mcx)?);
    }
    Ok(out)
}

/// Invoke `expression_tree_walker(node, find_expr_references_walker, context)`
/// over the unified node tree (the trailing C
/// `return expression_tree_walker(...)`). Threads the first error raised inside
/// any callback back out: the closure stashes it in a local and aborts, and we
/// surface it after the walk completes (mirroring the C contract where the
/// walker raises `ereport(ERROR)` directly).
fn run_subwalk(
    node: &Node<'_>,
    context: &mut FindExprReferencesContext<'_>,
) -> PgResult<bool> {
    let mut callback_err: Option<PgError> = None;
    let aborted = {
        let mut walker = |child: &Node| -> bool {
            match find_expr_references_walker(child, context) {
                Ok(abort) => abort,
                Err(e) => {
                    if callback_err.is_none() {
                        callback_err = Some(e);
                    }
                    true
                }
            }
        };
        node_walker::expression_tree_walker(node, &mut walker)
    };
    if let Some(e) = callback_err {
        return Err(e);
    }
    Ok(aborted)
}

/// `query_tree_walker(query, find_expr_references_walker, context,
/// QTW_IGNORE_JOINALIASES | QTW_EXAMINE_SORTGROUP)` — the `Query` arm's
/// substructure recursion, with the same error-threading as [`run_subwalk`].
fn run_query_subwalk(
    query: &Query<'_>,
    context: &mut FindExprReferencesContext<'_>,
) -> PgResult<bool> {
    let mut callback_err: Option<PgError> = None;
    let aborted = {
        let mut walker = |child: &Node| -> bool {
            match find_expr_references_walker(child, context) {
                Ok(abort) => abort,
                Err(e) => {
                    if callback_err.is_none() {
                        callback_err = Some(e);
                    }
                    true
                }
            }
        };
        node_walker::query_tree_walker(
            query,
            &mut walker,
            QTW_IGNORE_JOINALIASES | QTW_EXAMINE_SORTGROUP,
        )
    };
    if let Some(e) = callback_err {
        return Err(e);
    }
    Ok(aborted)
}

/* ===========================================================================
 * process_function_rte_ref (dependency.c:2335-2392)
 * ========================================================================= */

/// find_expr_references_walker subroutine: handle a Var reference to an
/// RTE_FUNCTION RTE.
fn process_function_rte_ref<'mcx>(
    rte: &RangeTblEntry<'mcx>,
    attnum: AttrNumber,
    context: &mut FindExprReferencesContext<'mcx>,
) -> PgResult<()> {
    let mut atts_done: i32 = 0;

    /*
     * Identify which RangeTblFunction produces this attnum, and see if it
     * returns a composite type. If so, we'd better make a dependency on the
     * referenced column of the composite type (or actually, of its associated
     * relation).
     */
    for rtfunc_node in rte.functions.iter() {
        let Some(rtfunc) = (**rtfunc_node).as_rangetblfunction() else {
            panic!(
                "process_function_rte_ref: RangeTblEntry.functions element is not a \
                 RangeTblFunction (tag {})",
                (**rtfunc_node).node_tag().0
            );
        };

        if (attnum as i32) > atts_done && (attnum as i32) <= atts_done + rtfunc.funccolcount {
            /* If it has a coldeflist, it certainly returns RECORD */
            let tupdesc = if !rtfunc.funccolnames.is_empty() {
                None /* no need to work hard */
            } else {
                let funcexpr_node = rtfunc.funcexpr.as_deref().ok_or_else(|| {
                    elog(
                        ERROR,
                        "process_function_rte_ref: RTE_FUNCTION RangeTblFunction has no funcexpr",
                    )
                    .unwrap_err()
                })?;
                // funcexpr is a Node; get_expr_result_tupdesc takes Option<&Node>
                // (C: get_expr_result_tupdesc((Node *) rtfunc->funcexpr, true)).
                funcapi_seams::get_expr_result_tupdesc::call(context.mcx, Some(funcexpr_node), true)?
            };
            if let Some(td) = tupdesc {
                if td.tdtypeid != RECORDOID {
                    /*
                     * Named composite type, so individual columns could get
                     * dropped. Make a dependency on this specific column.
                     */
                    let reltype = lsyscache_seams::get_typ_typrelid::call(td.tdtypeid)?;

                    debug_assert!((attnum as i32) - atts_done <= td.natts);
                    if OidIsValid(reltype) {
                        /* can this fail? */
                        add_object_address(
                            RelationRelationId,
                            reltype,
                            (attnum as i32) - atts_done,
                            &mut context.addrs,
                        );
                    }
                    return Ok(());
                }
            }
            /* Nothing to do; function's result type is handled elsewhere */
            return Ok(());
        }
        atts_done += rtfunc.funccolcount;
    }

    /* If we get here, must be looking for the ordinality column */
    if rte.funcordinality && (attnum as i32) == atts_done + 1 {
        return Ok(());
    }

    /* this probably can't happen ... */
    Err(ereport(ERROR)
        .errcode(ERRCODE_UNDEFINED_COLUMN)
        .errmsg(format!(
            "column {} of relation \"{}\" does not exist",
            attnum,
            rte_aliasname(rte)
        ))
        .into_error())
}

/// `rte->eref->aliasname` for error messages. An absent eref or aliasname
/// renders as the empty string (matching the C NULL handling in these
/// diagnostic paths).
fn rte_aliasname<'a>(rte: &'a RangeTblEntry<'_>) -> &'a str {
    match rte.eref.as_ref() {
        Some(alias) => alias.aliasname.as_ref().map(|s| s.as_str()).unwrap_or(""),
        None => "",
    }
}
