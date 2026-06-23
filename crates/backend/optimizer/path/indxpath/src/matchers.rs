//! The clause-to-index *matcher family* of indxpath.c, ported 1:1 over the
//! unified node tree + opfamily/opclass catalogs.

use alloc::format;
use alloc::vec;
use alloc::vec::Vec;

use ::mcx::Mcx;
use ::types_core::primitive::{AttrNumber, Oid};
use ::types_error::PgResult;
use ::nodes::primnodes::{CompareType, Expr, RowCompareExpr};
use pathnodes::{IndexClause, IndexOptInfo, PlannerInfo, RinfoId};

use ::nodes_core::makefuncs::{make_bool_const, make_opclause};
use ::nodes_core::nodefuncs::expr_type;
use clauses::{contain_volatile_functions, make_SAOP_expr};
use lsyscache_seams as lsyscache;
use fmgr_support_seams as fmgr_support;

use crate::operand::match_index_to_operand;
use crate::util::{
    get_notclausearg, index_coll_matches_expr_coll, is_builtin_boolean_opfamily, is_notclause,
    restriction_is_or_clause, BOOLEAN_EQUAL_OPERATOR, BOOLOID, BTREE_AM_OID,
    BT_GREATER_EQUAL_STRATEGY_NUMBER, BT_GREATER_STRATEGY_NUMBER, BT_LESS_EQUAL_STRATEGY_NUMBER,
    BT_LESS_STRATEGY_NUMBER, INVALID_OID, RECORDOID,
};

/// Deep-copy a slice of `Expr` into `mcx` via `Expr::clone_in` (C copyObject).
/// The derived `Expr::clone` panics on an owned-subtree child
/// (`Aggref`/`SubLink`/`SubPlan`), so any moved-into-node copy must go here.
fn clone_exprs_in<'mcx>(exprs: &[Expr<'_>], mcx: Mcx<'mcx>) -> PgResult<Vec<Expr<'mcx>>> {
    let mut out = Vec::with_capacity(exprs.len());
    for e in exprs {
        out.push(e.clone_in(mcx)?);
    }
    Ok(out)
}

/* ==========================================================================
 * IndexClauseSet — the C-file-private accumulator (indxpath.c:60).
 * ======================================================================== */

/// `IndexClauseSet` (indxpath.c:60) — the per-index accumulator the matchers add
/// to. C declares one `List *indexclauses[INDEX_MAX_KEYS]` plus a `nonempty`
/// flag; it is C-file-private, so owned here.
#[derive(Clone, Debug, Default)]
pub struct IndexClauseSet {
    /// `List *indexclauses[INDEX_MAX_KEYS]` — one matched-clause list per index
    /// key column.
    pub indexclauses: Vec<Vec<IndexClause>>,
    /// `bool nonempty` — set once any column has a matched clause.
    pub nonempty: bool,
}

impl IndexClauseSet {
    /// A fresh, all-zero `IndexClauseSet` for an `nkeycolumns`-key index.
    pub fn new(nkeycolumns: usize) -> Self {
        IndexClauseSet {
            indexclauses: vec![Vec::new(); nkeycolumns],
            nonempty: false,
        }
    }
}

/// Build a fresh non-lossy `IndexClause` reusing the original `rinfo` directly as
/// its own indexqual (`indexcols = NIL`).
fn make_self_index_clause(rinfo: RinfoId, indexqual: RinfoId, indexcol: usize) -> IndexClause {
    IndexClause {
        rinfo: Some(rinfo),
        indexquals: vec![indexqual],
        lossy: false,
        indexcol: indexcol as AttrNumber,
        indexcols: Vec::new(),
    }
}

/* ==========================================================================
 * Restriction-clause drivers.
 * ======================================================================== */

/// `match_restriction_clauses_to_index(root, index, clauseset)`
/// (indxpath.c:2466) — identify restriction clauses for the rel that match the
/// index.
pub fn match_restriction_clauses_to_index(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    index: &IndexOptInfo,
    clauseset: &mut IndexClauseSet,
) -> PgResult<()> {
    // We can ignore clauses that are implied by the index predicate.
    let clauses = index.indrestrictinfo.clone();
    match_clauses_to_index(mcx, root, &clauses, index, clauseset)
}

/// `match_clauses_to_index(root, clauses, index, clauseset)` (indxpath.c:2554) —
/// perform `match_clause_to_index` for each clause in a list.
pub fn match_clauses_to_index(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    clauses: &[RinfoId],
    index: &IndexOptInfo,
    clauseset: &mut IndexClauseSet,
) -> PgResult<()> {
    for &rinfo in clauses {
        match_clause_to_index(mcx, root, rinfo, index, clauseset)?;
    }
    Ok(())
}

/// `match_clause_to_index(root, rinfo, index, clauseset)` (indxpath.c:2587) —
/// test whether a qual clause can be used with an index; if usable, add an
/// `IndexClause` entry for it to the appropriate per-column list.
pub fn match_clause_to_index(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    rinfo: RinfoId,
    index: &IndexOptInfo,
    clauseset: &mut IndexClauseSet,
) -> PgResult<()> {
    // Never match pseudoconstants to indexes.
    if root.rinfo(rinfo).pseudoconstant {
        return Ok(());
    }

    // If clause can't be used as an indexqual because it must wait till after
    // some lower-security-level restriction clause, reject it.
    // restriction_is_securely_promotable(rinfo, index->rel):
    //   rinfo->security_level <= rel->baserestrict_min_security || rinfo->leakproof
    let index_rel = index.rel.expect("IndexOptInfo without rel");
    {
        let r = root.rinfo(rinfo);
        let promotable = r.security_level <= root.rel(index_rel).baserestrict_min_security as u32
            || r.leakproof;
        if !promotable {
            return Ok(());
        }
    }

    // OK, check each index key column for a match.
    let nkeycolumns = index.nkeycolumns as usize;
    for indexcol in 0..nkeycolumns {
        // Ignore duplicates (pointer equality -> RinfoId equality).
        if clauseset.indexclauses[indexcol]
            .iter()
            .any(|iclause| iclause.rinfo == Some(rinfo))
        {
            return Ok(());
        }

        // Try to match the clause to the index column.
        if let Some(iclause) = match_clause_to_indexcol(mcx, root, rinfo, indexcol, index)? {
            // Success, so record it.
            clauseset.indexclauses[indexcol].push(iclause);
            clauseset.nonempty = true;
            return Ok(());
        }
    }
    Ok(())
}

/* ==========================================================================
 * match_clause_to_indexcol — the per-column dispatcher.
 * ======================================================================== */

/// `match_clause_to_indexcol(root, rinfo, indexcol, index)` (indxpath.c:2711) —
/// determine whether a restriction clause matches a column of an index, and if
/// so, build an `IndexClause` describing the details. `None` if it can't be used
/// with this index key.
pub fn match_clause_to_indexcol(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    rinfo: RinfoId,
    indexcol: usize,
    index: &IndexOptInfo,
) -> PgResult<Option<IndexClause>> {
    debug_assert!(indexcol < index.nkeycolumns as usize);

    // First check for boolean-index cases.
    let opfamily = index.opfamily[indexcol];
    if IsBooleanOpfamily(opfamily) {
        if let Some(iclause) = match_boolean_index_clause(mcx, root, rinfo, indexcol, index)? {
            return Ok(Some(iclause));
        }
    }

    // Dispatch on the clause node kind.
    let clause_id = root.rinfo(rinfo).clause;
    let clause = root.node(clause_id);
    if clause.is_opexpr() {
        match_opclause_to_indexcol(mcx, root, rinfo, indexcol, index)
    } else if clause.is_funcexpr() {
        match_funcclause_to_indexcol(mcx, root, rinfo, indexcol, index)
    } else if clause.is_scalararrayopexpr() {
        Ok(match_saopclause_to_indexcol(mcx, root, rinfo, indexcol, index)?)
    } else if clause.is_rowcompareexpr() {
        match_rowcompare_to_indexcol(mcx, root, rinfo, indexcol, index)
    } else if restriction_is_or_clause(root, rinfo) {
        match_orclause_to_indexcol(mcx, root, rinfo, indexcol, index)
    } else if index.amsearchnulls && clause.is_nulltest() {
        // IS NULL / IS NOT NULL on the index column (no derived qual; the rinfo
        // itself is the indexqual).
        let nt = clause.as_nulltest().unwrap();
        let matched = if nt.argisrow {
            false
        } else {
            let arg: &Expr = nt.arg.as_deref().expect("NullTest without arg");
            match_index_to_operand(root, arg, indexcol, index)
        };
        if matched {
            Ok(Some(make_self_index_clause(rinfo, rinfo, indexcol)))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

/* ==========================================================================
 * IsBooleanOpfamily.
 * ======================================================================== */

/// `IsBooleanOpfamily(opfamily)` (indxpath.c:2793) — does this opfamily support
/// boolean equality? Built-in opfamilies use hard-wired knowledge; extension
/// opfamilies do a catalog lookup.
pub fn IsBooleanOpfamily(opfamily: Oid) -> bool {
    if opfamily < crate::util::FIRST_NORMAL_OBJECT_ID {
        is_builtin_boolean_opfamily(opfamily)
    } else {
        lsyscache::op_in_opfamily::call(BOOLEAN_EQUAL_OPERATOR, opfamily).expect("op_in_opfamily")
    }
}

/* ==========================================================================
 * match_boolean_index_clause.
 * ======================================================================== */

/// `match_boolean_index_clause(root, rinfo, indexcol, index)` (indxpath.c:2817)
/// — recognize a restriction clause matchable to a boolean index and rewrite it
/// to `indexkey = TRUE/FALSE`.
pub fn match_boolean_index_clause(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    rinfo: RinfoId,
    indexcol: usize,
    index: &IndexOptInfo,
) -> PgResult<Option<IndexClause>> {
    // Build the candidate derived OpExpr, if any, by inspecting the clause.
    // The clause / its arg is *moved* into a new derived OpExpr, so any owned
    // copy must go through `Expr::clone_in` (the derived `Expr::clone` panics on
    // an `Aggref`/`SubLink`/`SubPlan` payload); the read-only matching only
    // needs a borrow.
    let op: Option<Expr> = {
        let clause_id = root.rinfo(rinfo).clause;
        let clause: &Expr = root.node(clause_id);

        // Direct match? -> indexkey = TRUE
        if match_index_to_operand(root, clause, indexcol, index) {
            Some(make_bool_eq_opclause(clause.clone_in(mcx)?, true))
        }
        // NOT clause? -> indexkey = FALSE
        else if is_notclause(clause) {
            let arg: &Expr = get_notclausearg(clause);
            if match_index_to_operand(root, arg, indexcol, index) {
                Some(make_bool_eq_opclause(arg.clone_in(mcx)?, false))
            } else {
                None
            }
        }
        // indexkey IS TRUE / IS FALSE
        else if let Some(btest) = clause.as_booleantest() {
            use ::nodes::primnodes::BoolTestType;
            let arg: &Expr = btest.arg.as_deref().expect("BooleanTest without arg");
            if btest.booltesttype == BoolTestType::IS_TRUE
                && match_index_to_operand(root, arg, indexcol, index)
            {
                Some(make_bool_eq_opclause(arg.clone_in(mcx)?, true))
            } else if btest.booltesttype == BoolTestType::IS_FALSE
                && match_index_to_operand(root, arg, indexcol, index)
            {
                Some(make_bool_eq_opclause(arg.clone_in(mcx)?, false))
            } else {
                None
            }
        } else {
            None
        }
    };

    // If we successfully made an operator clause, wrap it in an IndexClause.
    let op = match op {
        Some(op) => op,
        None => return Ok(None),
    };
    let op_id = root.alloc_node(op);
    let derived = restrictinfo_seams::make_simple_restrictinfo::call(
        mcx, root, op_id,
    );
    Ok(Some(IndexClause {
        rinfo: Some(rinfo),
        indexquals: vec![derived],
        lossy: false,
        indexcol: indexcol as AttrNumber,
        indexcols: Vec::new(),
    }))
}

/// Build the derived `indexkey = TRUE/FALSE` `OpExpr`:
///   `make_opclause(BooleanEqualOperator, BOOLOID, false, indexkey,
///                  makeBoolConst(value, false), InvalidOid, InvalidOid)`.
fn make_bool_eq_opclause(indexkey: Expr, value: bool) -> Expr {
    let rhs = Expr::Const(make_bool_const(value, false));
    make_opclause(
        BOOLEAN_EQUAL_OPERATOR,
        BOOLOID,
        false,
        indexkey,
        Some(rhs),
        INVALID_OID,
        INVALID_OID,
    )
}

/* ==========================================================================
 * match_opclause_to_indexcol.
 * ======================================================================== */

/// `match_opclause_to_indexcol(root, rinfo, indexcol, index)` (indxpath.c:2904)
/// — handle the `OpExpr` case.
pub fn match_opclause_to_indexcol(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    rinfo: RinfoId,
    indexcol: usize,
    index: &IndexOptInfo,
) -> PgResult<Option<IndexClause>> {
    let index_relid = root.rel(index.rel.expect("IndexOptInfo without rel")).relid;
    let opfamily = index.opfamily[indexcol];
    let idxcollation = index.indexcollations[indexcol];

    // Only binary operators need apply. Read the operands / operator / collation.
    let (left_matches, right_matches, expr_op, expr_coll, opfuncid) = {
        let clause_id = root.rinfo(rinfo).clause;
        // Borrow the OpExpr to read its operands (a derived `.clone()` would
        // panic on an owned-subtree operand); the operands feed read-only
        // `match_index_to_operand`.
        let clause = root.node(clause_id).as_opexpr().expect("OpExpr");
        if clause.args.len() != 2 {
            return Ok(None);
        }
        let leftop: &Expr = &clause.args[0];
        let rightop: &Expr = &clause.args[1];
        let (opno, inputcollid, opfuncid) = (clause.opno, clause.inputcollid, clause.opfuncid);
        let left_matches = match_index_to_operand(root, leftop, indexcol, index);
        let right_matches = match_index_to_operand(root, rightop, indexcol, index);
        (left_matches, right_matches, opno, inputcollid, opfuncid)
    };

    // Case 1: (indexkey op const).
    let right_relids_has_index =
        crate::util::relids_is_member(index_relid as i32, &root.rinfo(rinfo).right_relids);
    let rightop_volatile = {
        let clause_id = root.rinfo(rinfo).clause;
        let rightop: &Expr = &root.node(clause_id).as_opexpr().unwrap().args[1];
        contain_volatile_functions(Some(rightop))?
    };
    if left_matches && !right_relids_has_index && !rightop_volatile {
        if index_coll_matches_expr_coll(idxcollation, expr_coll)
            && lsyscache::op_in_opfamily::call(expr_op, opfamily)?
        {
            return Ok(Some(make_self_index_clause(rinfo, rinfo, indexcol)));
        }
        // Operator not in the index's opfamily: try the support function.
        let funcid = resolve_opfuncid(opfuncid, expr_op)?;
        return get_index_clause_from_support(mcx, root, rinfo, funcid, 0, indexcol, index);
    }

    // Case 2: (const op indexkey). Use it only if the operator commutes.
    let left_relids_has_index =
        crate::util::relids_is_member(index_relid as i32, &root.rinfo(rinfo).left_relids);
    let leftop_volatile = {
        let clause_id = root.rinfo(rinfo).clause;
        let leftop: &Expr = &root.node(clause_id).as_opexpr().unwrap().args[0];
        contain_volatile_functions(Some(leftop))?
    };
    if right_matches && !left_relids_has_index && !leftop_volatile {
        if index_coll_matches_expr_coll(idxcollation, expr_coll) {
            let comm_op = lsyscache::get_commutator::call(expr_op)?;
            if comm_op != INVALID_OID && lsyscache::op_in_opfamily::call(comm_op, opfamily)? {
                // Build a commuted OpExpr + RestrictInfo (pushed into the arena).
                let commrinfo = commute_restrictinfo(mcx, root, rinfo, comm_op)?;
                return Ok(Some(make_self_index_clause(rinfo, commrinfo, indexcol)));
            }
        }
        // Operator (or its commutator) not in the index's opfamily: support fn.
        let funcid = resolve_opfuncid(opfuncid, expr_op)?;
        return get_index_clause_from_support(mcx, root, rinfo, funcid, 1, indexcol, index);
    }

    Ok(None)
}

/// `set_opfuncid(clause)` — ensure we have the operator's underlying function
/// OID, looking it up via `get_opcode` when the clause didn't carry it.
fn resolve_opfuncid(opfuncid: Oid, opno: Oid) -> PgResult<Oid> {
    if opfuncid != INVALID_OID {
        Ok(opfuncid)
    } else {
        lsyscache::get_opcode::call(opno)
    }
}

/* ==========================================================================
 * match_funcclause_to_indexcol.
 * ======================================================================== */

/// `match_funcclause_to_indexcol(root, rinfo, indexcol, index)`
/// (indxpath.c:3023) — handle the `FuncExpr` case via the planner support
/// function.
pub fn match_funcclause_to_indexcol(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    rinfo: RinfoId,
    indexcol: usize,
    index: &IndexOptInfo,
) -> PgResult<Option<IndexClause>> {
    // Only call the support function if at least one argument matches the column.
    let (nargs, funcid) = {
        let clause_id = root.rinfo(rinfo).clause;
        let f = root.node(clause_id).as_funcexpr().expect("FuncExpr");
        (f.args.len(), f.funcid)
    };
    for indexarg in 0..nargs {
        let matched = {
            let clause_id = root.rinfo(rinfo).clause;
            let arg: &Expr = &root.node(clause_id).as_funcexpr().unwrap().args[indexarg];
            match_index_to_operand(root, arg, indexcol, index)
        };
        if matched {
            return get_index_clause_from_support(
                mcx,
                root,
                rinfo,
                funcid,
                indexarg as i32,
                indexcol,
                index,
            );
        }
    }
    Ok(None)
}

/* ==========================================================================
 * get_index_clause_from_support — the planner-support fmgr fallback.
 * ======================================================================== */

/// `get_index_clause_from_support(root, rinfo, funcid, indexarg, indexcol,
/// index)` (indxpath.c:3069) — if the function has a planner support function,
/// try to construct an `IndexClause` using indexquals it creates. The
/// `OidFunctionCall1` fmgr dispatch over the `SupportRequestIndexCondition` node
/// is the genuine cross-subsystem boundary (seam-and-panic).
pub fn get_index_clause_from_support(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    rinfo: RinfoId,
    funcid: Oid,
    indexarg: i32,
    indexcol: usize,
    index: &IndexOptInfo,
) -> PgResult<Option<IndexClause>> {
    let prosupport = lsyscache::get_func_support::call(funcid)?;
    if prosupport == INVALID_OID {
        return Ok(None);
    }

    let clause_id = root.rinfo(rinfo).clause;
    // Invoke the support function (fmgr boundary): returns the bare derived
    // index-condition clauses + the lossy flag.
    let (sresult, lossy) = fmgr_support::oid_function_call1_index_support::call(
        root,
        prosupport,
        funcid,
        clause_id,
        indexarg,
        index,
        indexcol as i32,
    );

    if sresult.is_empty() {
        return Ok(None);
    }

    // The support function gives back bare clauses; wrap each in a RestrictInfo.
    let mut indexquals: Vec<RinfoId> = Vec::new();
    for clause in sresult {
        let node_id = root.alloc_node(clause);
        let ri = restrictinfo_seams::make_simple_restrictinfo::call(
            mcx, root, node_id,
        );
        indexquals.push(ri);
    }

    Ok(Some(IndexClause {
        rinfo: Some(rinfo),
        indexquals,
        lossy,
        indexcol: indexcol as AttrNumber,
        indexcols: Vec::new(),
    }))
}

/* ==========================================================================
 * match_saopclause_to_indexcol.
 * ======================================================================== */

/// `match_saopclause_to_indexcol(root, rinfo, indexcol, index)`
/// (indxpath.c:3135) — handle the `ScalarArrayOpExpr` case (`indexkey op ANY
/// (arrayconst)`).
pub fn match_saopclause_to_indexcol(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    rinfo: RinfoId,
    indexcol: usize,
    index: &IndexOptInfo,
) -> PgResult<Option<IndexClause>> {
    let index_relid = root.rel(index.rel.expect("IndexOptInfo without rel")).relid;
    let opfamily = index.opfamily[indexcol];
    let idxcollation = index.indexcollations[indexcol];

    let (use_or, leftop, rightop, expr_op, expr_coll) = {
        let clause_id = root.rinfo(rinfo).clause;
        let saop = root.node(clause_id).as_scalararrayopexpr().expect("SAOP");
        (
            saop.useOr,
            saop.args.first().cloned(),
            saop.args.get(1).cloned(),
            saop.opno,
            saop.inputcollid,
        )
    };

    // We only accept ANY clauses, not ALL.
    if !use_or {
        return Ok(None);
    }
    let leftop = leftop.expect("ScalarArrayOpExpr without left operand");
    let rightop = rightop.expect("ScalarArrayOpExpr without right operand");

    // right_relids = pull_varnos(root, rightop).
    let right_relids = var_seams::pull_varnos::call(mcx, &rightop)?;

    // We must have indexkey on the left and a pseudo-constant array argument.
    if match_index_to_operand(root, &leftop, indexcol, index)
        && !::nodes_core::bitmapset::bms_is_member(index_relid as i32, right_relids.as_deref())
        && !contain_volatile_functions(Some(&rightop))?
    {
        if index_coll_matches_expr_coll(idxcollation, expr_coll)
            && lsyscache::op_in_opfamily::call(expr_op, opfamily)?
        {
            return Ok(Some(make_self_index_clause(rinfo, rinfo, indexcol)));
        }
        // We do not ask support functions about ScalarArrayOpExprs.
    }

    Ok(None)
}

/* ==========================================================================
 * commute_restrictinfo — restrictinfo.c:350, ported 1:1 over the arena.
 * ======================================================================== */

/// `commute_restrictinfo(rinfo, comm_op)` (restrictinfo.c:350) — build a
/// commuted version of the (binary `OpExpr`) restriction clause `rinfo` using
/// `comm_op`, push the new `RestrictInfo` into the arena, and return its handle.
pub fn commute_restrictinfo(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    rinfo: RinfoId,
    comm_op: Oid,
) -> PgResult<RinfoId> {
    // Flat-copy all the fields of the source RestrictInfo ...
    let mut result = root.rinfo(rinfo).clone();

    // ... build the commuted OpExpr clause. Deep-copy the OpExpr via
    // `clone_in` (it is moved into a fresh arena node; a derived `.clone()`
    // panics on an owned-subtree operand).
    let orig_op = match root.node(result.clause).clone_in(mcx)? {
        Expr::OpExpr(o) => o,
        _ => panic!("commute_restrictinfo: clause is not an OpExpr"),
    };
    let orig_opno = orig_op.opno;
    let mut newclause = orig_op;
    debug_assert_eq!(newclause.args.len(), 2);
    // newclause->args = list_make2(lsecond(args), linitial(args)) -> reverse.
    newclause.args.swap(0, 1);
    newclause.opno = comm_op;
    newclause.opfuncid = INVALID_OID;

    // result->clause = (Expr *) newclause — a fresh arena node.
    result.clause = root.alloc_node(Expr::OpExpr(newclause));
    core::mem::swap(&mut result.left_relids, &mut result.right_relids);
    debug_assert!(result.orclause.is_none());
    core::mem::swap(&mut result.left_ec, &mut result.right_ec);
    core::mem::swap(&mut result.left_em, &mut result.right_em);
    result.scansel_cache = Vec::new(); // not worth updating this
    result.hashjoinoperator = if result.hashjoinoperator == orig_opno {
        comm_op
    } else {
        INVALID_OID
    };
    core::mem::swap(&mut result.left_bucketsize, &mut result.right_bucketsize);
    core::mem::swap(&mut result.left_mcvfreq, &mut result.right_mcvfreq);
    result.left_hasheqoperator = INVALID_OID;
    result.right_hasheqoperator = INVALID_OID;

    Ok(root.alloc_rinfo(result))
}

/* ==========================================================================
 * match_rowcompare_to_indexcol + expand_indexqual_rowcompare.
 * ======================================================================== */

/// `match_rowcompare_to_indexcol(root, rinfo, indexcol, index)`
/// (indxpath.c:3203) — handle the `RowCompareExpr` case: check the first column
/// matches the index column, then expand via `expand_indexqual_rowcompare`.
pub fn match_rowcompare_to_indexcol(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    rinfo: RinfoId,
    indexcol: usize,
    index: &IndexOptInfo,
) -> PgResult<Option<IndexClause>> {
    // Forget it if we're not dealing with a btree index.
    if index.relam != BTREE_AM_OID {
        return Ok(None);
    }

    let index_relid = root.rel(index.rel.expect("IndexOptInfo without rel")).relid;
    let opfamily = index.opfamily[indexcol];
    let idxcollation = index.indexcollations[indexcol];

    // Look only at the operator (after commutation if indexkey is on the right).
    // `leftop`/`rightop` must outlive the `&mut root` calls below
    // (`operand_uses_index_relid` allocs into the arena), so deep-copy them via
    // `clone_in` (a derived `.clone()` panics on an owned-subtree operand).
    let (leftop, rightop, mut expr_op, expr_coll) = {
        let clause_id = root.rinfo(rinfo).clause;
        let rc = root.node(clause_id).as_rowcompareexpr().expect("RowCompareExpr");
        let expr_op = *rc.opnos.first().expect("RowCompareExpr opnos");
        let expr_coll = *rc.inputcollids.first().expect("RowCompareExpr inputcollids");
        let leftop = rc.largs.first().expect("RowCompareExpr largs").clone_in(mcx)?;
        let rightop = rc.rargs.first().expect("RowCompareExpr rargs").clone_in(mcx)?;
        (leftop, rightop, expr_op, expr_coll)
    };

    // Collations must match, if relevant.
    if !index_coll_matches_expr_coll(idxcollation, expr_coll) {
        return Ok(None);
    }

    // The same syntactic tests as match_opclause_to_indexcol.
    let var_on_left;
    if match_index_to_operand(root, &leftop, indexcol, index)
        && !operand_uses_index_relid(mcx, root, &rightop, index_relid)?
        && !contain_volatile_functions(Some(&rightop))?
    {
        // OK, indexkey is on left.
        var_on_left = true;
    } else if match_index_to_operand(root, &rightop, indexcol, index)
        && !operand_uses_index_relid(mcx, root, &leftop, index_relid)?
        && !contain_volatile_functions(Some(&leftop))?
    {
        // indexkey is on right, so commute the operator.
        expr_op = lsyscache::get_commutator::call(expr_op)?;
        if expr_op == INVALID_OID {
            return Ok(None);
        }
        var_on_left = false;
    } else {
        return Ok(None);
    }

    // We're good if the operator is the right type of opfamily member.
    let strat = lsyscache::get_op_opfamily_strategy::call(expr_op, opfamily)?;
    match strat {
        s if s == BT_LESS_STRATEGY_NUMBER
            || s == BT_LESS_EQUAL_STRATEGY_NUMBER
            || s == BT_GREATER_EQUAL_STRATEGY_NUMBER
            || s == BT_GREATER_STRATEGY_NUMBER =>
        {
            Ok(Some(expand_indexqual_rowcompare(
                mcx,
                root,
                rinfo,
                indexcol,
                index,
                expr_op,
                var_on_left,
            )?))
        }
        _ => Ok(None),
    }
}

/// `!bms_is_member(index->rel->relid, pull_varnos(root, op))` helper over an
/// arena node operand: builds the operand into the arena to reuse the joinpath
/// `pull_varnos(root, NodeId)` seam, then tests relid membership.
fn operand_uses_index_relid(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    op: &Expr,
    index_relid: ::types_core::primitive::Index,
) -> PgResult<bool> {
    let op = op.clone_in(mcx)?;
    let id = root.alloc_node(op);
    let varnos = joinpath_seams::pull_varnos::call(root, id);
    Ok(crate::util::relids_is_member(index_relid as i32, &varnos))
}

/// `expand_indexqual_rowcompare(root, rinfo, indexcol, index, expr_op,
/// var_on_left)` (indxpath.c:3496) — expand a `RowCompareExpr` indexqual,
/// possibly building a shortened `RowCompareExpr` or a single `OpExpr`.
pub fn expand_indexqual_rowcompare(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    rinfo: RinfoId,
    indexcol: usize,
    index: &IndexOptInfo,
    mut expr_op: Oid,
    var_on_left: bool,
) -> PgResult<IndexClause> {
    // Deep-copy the RowCompareExpr via `clone_in` (its arg Exprs are moved into
    // freshly built index quals; the derived `.clone()` panics on an
    // owned-subtree operand).
    let clause: RowCompareExpr = {
        let clause_id = root.rinfo(rinfo).clause;
        match root.node(clause_id).clone_in(mcx)? {
            Expr::RowCompareExpr(rc) => rc,
            _ => panic!("expand_indexqual_rowcompare: clause is not a RowCompareExpr"),
        }
    };

    // var_args / non_var_args (already deep copies, taken from the cloned-in
    // RowCompareExpr by value).
    let (var_args, non_var_args): (Vec<Expr>, Vec<Expr>) = if var_on_left {
        (
            clone_exprs_in(&clause.largs, mcx)?,
            clone_exprs_in(&clause.rargs, mcx)?,
        )
    } else {
        (
            clone_exprs_in(&clause.rargs, mcx)?,
            clone_exprs_in(&clause.largs, mcx)?,
        )
    };

    let (mut op_strategy, op_lefttype, op_righttype) = lsyscache::get_op_opfamily_properties::call(
        expr_op,
        index.opfamily[indexcol],
        false,
        false,
    )?
    .expect("get_op_opfamily_properties");

    // Returned list of which index columns are used.
    let mut indexcols: Vec<AttrNumber> = vec![indexcol as AttrNumber];

    // Build lists of ops, opfamilies and operator datatypes in case needed.
    let mut expr_ops: Vec<Oid> = vec![expr_op];
    let mut opfamilies: Vec<Oid> = vec![index.opfamily[indexcol]];
    let mut lefttypes: Vec<Oid> = vec![op_lefttype];
    let mut righttypes: Vec<Oid> = vec![op_righttype];

    // See how many of the remaining columns match in the same way.
    let mut matching_cols = 1usize;
    while matching_cols < var_args.len() {
        let varop: &Expr = &var_args[matching_cols];
        let constop: &Expr = &non_var_args[matching_cols];

        let mut col_op = clause.opnos[matching_cols];
        if !var_on_left {
            // indexkey is on right, so commute the operator.
            col_op = lsyscache::get_commutator::call(col_op)?;
            if col_op == INVALID_OID {
                break; // operator is not usable
            }
        }
        if operand_uses_index_relid(
            mcx,
            root,
            constop,
            root.rel(index.rel.expect("IndexOptInfo without rel")).relid,
        )? {
            break; // no good, Var on wrong side
        }
        if contain_volatile_functions(Some(constop))? {
            break; // no good, volatile comparison value
        }

        // The Var side can match any key column of the index.
        let mut i = 0usize;
        let nkeycolumns = index.nkeycolumns as usize;
        while i < nkeycolumns {
            if match_index_to_operand(root, varop, i, index)
                && lsyscache::get_op_opfamily_strategy::call(col_op, index.opfamily[i])?
                    == op_strategy
                && index_coll_matches_expr_coll(
                    index.indexcollations[i],
                    clause.inputcollids[matching_cols],
                )
            {
                break;
            }
            i += 1;
        }
        if i >= nkeycolumns {
            break; // no match found
        }

        // Add column number to returned list.
        indexcols.push(i as AttrNumber);

        // Add operator info to lists.
        let (s, lt, rt) = lsyscache::get_op_opfamily_properties::call(
            col_op,
            index.opfamily[i],
            false,
            false,
        )?
        .expect("get_op_opfamily_properties");
        op_strategy = s;
        expr_ops.push(col_op);
        opfamilies.push(index.opfamily[i]);
        lefttypes.push(lt);
        righttypes.push(rt);

        matching_cols += 1;
    }

    // Result is non-lossy iff all columns are usable as index quals.
    let lossy = matching_cols != clause.opnos.len();

    let indexquals: Vec<RinfoId>;
    if var_on_left && !lossy {
        // We can use rinfo->clause as-is.
        indexquals = vec![rinfo];
        let _ = &mut expr_op; // (expr_op only needed in the rebuild branches)
        return Ok(IndexClause {
            rinfo: Some(rinfo),
            indexquals,
            lossy,
            indexcol: indexcol as AttrNumber,
            indexcols,
        });
    }

    // We have to generate a modified rowcompare (possibly just one OpExpr).
    // First deal with changing < to <= or > to >=.
    let new_ops: Vec<Oid>;
    if !lossy {
        // Very easy: just use the commuted operators.
        new_ops = expr_ops.clone();
    } else if op_strategy == BT_LESS_EQUAL_STRATEGY_NUMBER
        || op_strategy == BT_GREATER_EQUAL_STRATEGY_NUMBER
    {
        // Easy: just use the same (possibly commuted) operators, truncated.
        new_ops = expr_ops[..matching_cols].to_vec();
    } else {
        if op_strategy == BT_LESS_STRATEGY_NUMBER {
            op_strategy = BT_LESS_EQUAL_STRATEGY_NUMBER;
        } else if op_strategy == BT_GREATER_STRATEGY_NUMBER {
            op_strategy = BT_GREATER_EQUAL_STRATEGY_NUMBER;
        } else {
            return Err(::types_error::PgError::error(format!(
                "unexpected strategy number {}",
                op_strategy
            )));
        }
        let mut ops: Vec<Oid> = Vec::new();
        for ((&opfam, &lefttype), &righttype) in opfamilies
            .iter()
            .zip(lefttypes.iter())
            .zip(righttypes.iter())
        {
            let new_op = lsyscache::get_opfamily_member::call(
                opfam,
                lefttype,
                righttype,
                op_strategy as i16,
            )?;
            if new_op == INVALID_OID {
                return Err(::types_error::PgError::error(format!(
                    "missing operator {}({},{}) in opfamily {}",
                    op_strategy, lefttype, righttype, opfam
                )));
            }
            ops.push(new_op);
        }
        new_ops = ops;
    }

    if matching_cols > 1 {
        // Create a subset rowcompare.
        let rc = RowCompareExpr {
            cmptype: cmptype_from_strategy(op_strategy),
            opnos: new_ops,
            opfamilies: clause.opfamilies[..matching_cols].to_vec(),
            inputcollids: clause.inputcollids[..matching_cols].to_vec(),
            largs: clone_exprs_in(&var_args[..matching_cols], mcx)?,
            rargs: clone_exprs_in(&non_var_args[..matching_cols], mcx)?,
        };
        let rc_id = root.alloc_node(Expr::RowCompareExpr(rc));
        let ri = restrictinfo_seams::make_simple_restrictinfo::call(
            mcx, root, rc_id,
        );
        indexquals = vec![ri];
    } else {
        // We don't report an index column list in this case.
        indexcols = Vec::new();
        let op = make_opclause(
            new_ops[0],
            BOOLOID,
            false,
            var_args[0].clone_in(mcx)?,
            Some(non_var_args[0].clone_in(mcx)?),
            INVALID_OID,
            clause.inputcollids[0],
        );
        let op_id = root.alloc_node(op);
        let ri = restrictinfo_seams::make_simple_restrictinfo::call(
            mcx, root, op_id,
        );
        indexquals = vec![ri];
    }

    Ok(IndexClause {
        rinfo: Some(rinfo),
        indexquals,
        lossy,
        indexcol: indexcol as AttrNumber,
        indexcols,
    })
}

/// Map a btree strategy number to the `RowCompareExpr.cmptype` (`CompareType`).
fn cmptype_from_strategy(strat: i32) -> CompareType {
    match strat {
        s if s == BT_LESS_STRATEGY_NUMBER => CompareType::COMPARE_LT,
        s if s == BT_LESS_EQUAL_STRATEGY_NUMBER => CompareType::COMPARE_LE,
        s if s == BT_GREATER_EQUAL_STRATEGY_NUMBER => CompareType::COMPARE_GE,
        s if s == BT_GREATER_STRATEGY_NUMBER => CompareType::COMPARE_GT,
        _ => CompareType::COMPARE_INVALID,
    }
}

/* ==========================================================================
 * match_orclause_to_indexcol — OR-arms -> ScalarArrayOpExpr.
 * ======================================================================== */

/// `match_orclause_to_indexcol(root, rinfo, indexcol, index)` (indxpath.c:3297)
/// — attempt to transform a list of OR-clause args into a single SAOP matching
/// the index column.
pub fn match_orclause_to_indexcol<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    rinfo: RinfoId,
    indexcol: usize,
    index: &IndexOptInfo,
) -> PgResult<Option<IndexClause>> {
    // Forget it if index doesn't support SAOP clauses.
    if !index.amsearcharray {
        return Ok(None);
    }

    let index_relid = root.rel(index.rel.expect("IndexOptInfo without rel")).relid;

    // The OR clause's arms (each is a RestrictInfo node, or a sub-AND).
    // Deep-copy each arm via `clone_in` (a derived `.clone()` panics on an
    // owned-subtree operand such as a SubPlan inside an `OpExpr` arm).
    let orclause_id = root.rinfo(rinfo).orclause.expect("RestrictInfo without orclause");
    // Deep-copy each arm into the planner-run `mcx`, then erase to the arena's
    // notional `'static` (the OR-arm clauses are used at the arena level alongside
    // `root.node(..)`-resolved nodes via `orarg_clause_owned`).
    let or_args: Vec<Expr<'static>> = {
        let args = &root
            .node(orclause_id)
            .as_boolexpr()
            .expect("orclause must be a BoolExpr")
            .args;
        let mut out = Vec::with_capacity(args.len());
        for a in args {
            out.push(a.clone_in(mcx)?.erase_lifetime());
        }
        out
    };

    let mut consts: Vec<Expr<'mcx>> = Vec::new();
    let mut index_expr: Option<Expr<'mcx>> = None;
    let mut match_opno: Oid = INVALID_OID;
    let mut consttype: Oid = INVALID_OID;
    let mut arraytype: Oid = INVALID_OID;
    let mut inputcollid: Oid = INVALID_OID;
    let mut first_time = true;
    let mut have_non_const = false;
    let mut broke = false;

    for arg in &or_args {
        // OR arms are RestrictInfo handles (Expr::RestrictInfo). Deref to the
        // wrapped clause; a usable arm carries a binary OpExpr (mirrors C's
        // "IsA(arg, RestrictInfo) && IsA(subRinfo->clause, OpExpr)"). A sub-AND
        // arm (BoolExpr clause) is not usable -> bail.
        let sub = match crate::bitmap::orarg_clause_owned(mcx, root, arg)? {
            Some(Expr::OpExpr(s)) => s,
            _ => {
                broke = true;
                break;
            }
        };
        let mut opno = sub.opno;

        // Only binary operators can match.
        if sub.args.len() != 2 {
            broke = true;
            break;
        }

        // The operands are moved into the new SAOP node (index_expr / consts);
        // deep-copy via `clone_in` (a derived `.clone()` panics on an
        // owned-subtree operand).
        let leftop = sub.args[0].clone_in(mcx)?;
        let rightop = sub.args[1].clone_in(mcx)?;
        let sub_inputcollid = sub.inputcollid;

        // Determine the index key side / const side. These tests should agree
        // with match_opclause_to_indexcol. We compute the per-arm relids via
        // pull_varnos over each operand (the arms carry bare clause nodes here,
        // not RestrictInfos with precomputed left/right_relids).
        let const_expr: Expr<'mcx>;
        let left_uses = operand_uses_index_relid(mcx, root, &leftop, index_relid)?;
        let right_uses = operand_uses_index_relid(mcx, root, &rightop, index_relid)?;
        if match_index_to_operand(root, &leftop, indexcol, index)
            && !right_uses
            && !contain_volatile_functions(Some(&rightop))?
        {
            index_expr = Some(leftop);
            const_expr = rightop;
        } else if match_index_to_operand(root, &rightop, indexcol, index)
            && !left_uses
            && !contain_volatile_functions(Some(&leftop))?
        {
            opno = lsyscache::get_commutator::call(opno)?;
            if opno == INVALID_OID {
                broke = true;
                break;
            }
            index_expr = Some(rightop);
            const_expr = leftop;
        } else {
            broke = true;
            break;
        }

        if first_time {
            match_opno = opno;
            consttype = expr_type(Some(&const_expr))?;
            arraytype = lsyscache::get_array_type::call(consttype)?.unwrap_or(INVALID_OID);
            inputcollid = sub_inputcollid;

            // Check the operator is in the opfamily, the collation matches, and
            // an array type exists.
            if !index_coll_matches_expr_coll(index.indexcollations[indexcol], inputcollid)
                || !lsyscache::op_in_opfamily::call(match_opno, index.opfamily[indexcol])?
                || arraytype == INVALID_OID
            {
                broke = true;
                break;
            }

            // Disallow if either type is RECORD.
            if consttype == RECORDOID
                || expr_type(Some(index_expr.as_ref().unwrap()))? == RECORDOID
            {
                broke = true;
                break;
            }

            first_time = false;
        } else {
            if match_opno != opno
                || inputcollid != sub_inputcollid
                || consttype != expr_type(Some(&const_expr))?
            {
                broke = true;
                break;
            }
        }

        // The righthand inputs don't have to be plain Consts, but make_SAOP_expr
        // needs to know if any are not.
        if !const_expr.is_const() {
            have_non_const = true;
        }

        consts.push(const_expr);
    }

    // Handle failed conversion (broke out of the loop, or empty OR list).
    if broke || index_expr.is_none() {
        return Ok(None);
    }

    let index_expr = index_expr.unwrap();

    // Build the new SAOP node.
    let saopexpr = make_SAOP_expr(
        mcx,
        match_opno,
        index_expr,
        consttype,
        inputcollid,
        inputcollid,
        consts,
        have_non_const,
    )?
    .expect("make_SAOP_expr");
    let _ = arraytype; // (checked above; not needed past make_SAOP_expr)

    // Build an IndexClause based on the SAOP node. It's not lossy.
    let saop_id = root.alloc_node(saopexpr);
    let ri = restrictinfo_seams::make_simple_restrictinfo::call(
        mcx, root, saop_id,
    );
    Ok(Some(IndexClause {
        rinfo: Some(rinfo),
        indexquals: vec![ri],
        lossy: false,
        indexcol: indexcol as AttrNumber,
        indexcols: Vec::new(),
    }))
}

/* ==========================================================================
 * Join + eclass clause drivers.
 * ======================================================================== */

/// `match_join_clauses_to_index(root, rel, index, clauseset, joinorclauses)`
/// (indxpath.c:2482) — identify join clauses for the rel that match the index;
/// also collect join OR clauses for later.
pub fn match_join_clauses_to_index(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    rel: ::pathnodes::RelId,
    index: &IndexOptInfo,
    clauseset: &mut IndexClauseSet,
    joinorclauses: &mut Vec<RinfoId>,
) -> PgResult<()> {
    // Scan the rel's joininfo list.
    let joininfo = root.rel(rel).joininfo.clone();
    for rinfo in joininfo {
        // Check if clause can be moved to this rel (C 2496).
        if !restrictinfo_seams::join_clause_is_movable_to::call(
            root, rinfo, rel,
        ) {
            continue;
        }

        // Potentially usable: collect OR clauses (list_append_unique_ptr), then
        // unconditionally try matching the clause to the index (C 2503-2507).
        if restriction_is_or_clause(root, rinfo) && !joinorclauses.contains(&rinfo) {
            joinorclauses.push(rinfo);
        }

        match_clause_to_index(mcx, root, rinfo, index, clauseset)?;
    }
    Ok(())
}

/// `match_eclass_clauses_to_index(root, index, clauseset)` (indxpath.c:2516) —
/// look for EquivalenceClasses that can generate join clauses matching the
/// index, via `generate_implied_equalities_for_column` (whose per-member callback
/// is `ec_member_matches_indexcol`, ported in [`crate::unique`]).
pub fn match_eclass_clauses_to_index<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &::pathnodes::planner_run::PlannerRun<'mcx>,
    index: &IndexOptInfo,
    clauseset: &mut IndexClauseSet,
) -> PgResult<()> {
    // Fast path: if the index has no EC-derivable joins at all, do nothing.
    let rel = index.rel.expect("IndexOptInfo without rel");
    if !root.rel(rel).has_eclass_joins {
        return Ok(());
    }

    // C passes `index->rel->lateral_referencers` as the prohibited_rels arg.
    let prohibited_rels = root.rel(rel).lateral_referencers.clone();

    let nkeycolumns = index.nkeycolumns as usize;
    for indexcol in 0..nkeycolumns {
        // Generate clauses matching this index column from any EquivalenceClass.
        // The C callback `ec_member_matches_indexcol` (with `arg` carrying
        // `index`+`indexcol`) is supplied here as a closure that resolves the
        // EC/EM arena handles and dispatches to the ported matcher in
        // `crate::unique`.
        let mut callback =
            |cb_root: &PlannerInfo,
             cb_rel: ::pathnodes::RelId,
             ec: ::pathnodes::EcId,
             em: ::pathnodes::EmId|
             -> bool {
                crate::unique::ec_member_matches_indexcol(
                    cb_root,
                    cb_rel,
                    cb_root.ec(ec),
                    cb_root.em(em),
                    index,
                    indexcol,
                )
            };
        let clauses =
            equivclass::join::generate_implied_equalities_for_column(
                root,
                run,
                rel,
                &mut callback,
                &prohibited_rels,
            )?;

        // Recheck via the matcher: as in C, the generated clauses are double-
        // checked against the index (the EC opfamily test in
        // ec_member_matches_indexcol can return true for a useless EC).
        match_clauses_to_index(mcx, root, &clauses, index, clauseset)?;
    }
    Ok(())
}
