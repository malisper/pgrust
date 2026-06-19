//! CHECKS FOR MERGEJOINABLE AND HASHJOINABLE CLAUSES (initsplan.c) —
//! `check_mergejoinable`, `check_hashjoinable`, `check_memoizable`.

extern crate alloc;

use alloc::vec::Vec;

use types_core::primitive::Oid;
use types_error::PgResult;
use types_nodes::primnodes::Expr;
use types_pathnodes::{PlannerInfo, RinfoId};

use backend_utils_cache_lsyscache_seams as lsc;
use backend_optimizer_path_equivclass_ext_seams as eqext;
use backend_optimizer_plan_init_subselect_ext_seams as initext;

/// `InvalidOid`.
const INVALID_OID: Oid = 0;

/// Resolve the rinfo's `clause` to a 2-arg `OpExpr`, returning
/// `(opno, lefttype, righttype)`. Returns `None` for pseudoconstant rinfos or
/// when the clause is not a binary `OpExpr` (`is_opclause` + `list_length == 2`).
///
/// C reads the left/right operands as bare `Node *` pointers (`get_leftop` /
/// `get_rightop`) and immediately passes them to `exprType`. We mirror that by
/// computing the operand types while the clause node borrow is live, rather than
/// deep-cloning the operand `Expr` trees out of `root` — cloning is both
/// needless and unsound for operands carrying context-allocated children (e.g.
/// a `SubPlan`, whose lifetime-free `Expr::clone` deliberately panics; clone
/// must route through `clone_in`).
fn binary_opclause(root: &PlannerInfo, restrictinfo: RinfoId) -> Option<(Oid, Oid, Oid)> {
    let ri = root.rinfo(restrictinfo);
    if ri.pseudoconstant {
        return None;
    }
    let clause_id = ri.clause;
    if let Expr::OpExpr(op) = root.node(clause_id) {
        if op.args.len() != 2 {
            return None;
        }
        let opno = op.opno;
        let lefttype = eqext::expr_type::call(&op.args[0]);
        let righttype = eqext::expr_type::call(&op.args[1]);
        Some((opno, lefttype, righttype))
    } else {
        None
    }
}

/// Read `contain_volatile_functions((Node *) restrictinfo->clause)`.
fn clause_is_volatile(root: &PlannerInfo, restrictinfo: RinfoId) -> bool {
    // C reads the clause as a bare `Node *`; `contain_volatile_functions` only
    // walks it. Pass the borrowed clause node directly — cloning the whole Expr
    // tree is both needless and unsound for clauses carrying context-allocated
    // children (e.g. a `SubPlan`, whose lifetime-free `Expr::clone` panics).
    let clause_id = root.rinfo(restrictinfo).clause;
    eqext::contain_volatile_functions::call(root.node(clause_id))
}

/// `check_mergejoinable` (initsplan.c:3795).
///
/// If the restrictinfo's clause is mergejoinable, set the mergejoin info fields.
/// Supported for binary opclauses where the operator is mergejoinable and there
/// are no volatile functions in the args.
pub fn check_mergejoinable(root: &mut PlannerInfo, restrictinfo: RinfoId) -> PgResult<()> {
    let (opno, lefttype, _righttype) = match binary_opclause(root, restrictinfo) {
        Some(x) => x,
        None => return Ok(()),
    };

    if lsc::op_mergejoinable::call(opno, lefttype)?
        && !clause_is_volatile(root, restrictinfo)
    {
        // get_mergejoin_opfamilies allocates its result list; charge it to a
        // transient MemoryContext and copy out the OIDs (the equivclass /
        // pathkeys idiom for the Mcx-allocating lsyscache seam).
        let cx = mcx::MemoryContext::new("check_mergejoinable get_mergejoin_opfamilies transient");
        let fams: Vec<Oid> = lsc::get_mergejoin_opfamilies::call(cx.mcx(), opno)?
            .iter()
            .copied()
            .collect();
        root.rinfo_mut(restrictinfo).mergeopfamilies = fams;
    }
    Ok(())
}

/// `check_hashjoinable` (initsplan.c:3832).
pub fn check_hashjoinable(root: &mut PlannerInfo, restrictinfo: RinfoId) -> PgResult<()> {
    let (opno, lefttype, _righttype) = match binary_opclause(root, restrictinfo) {
        Some(x) => x,
        None => return Ok(()),
    };

    if lsc::op_hashjoinable::call(opno, lefttype)?
        && !clause_is_volatile(root, restrictinfo)
    {
        root.rinfo_mut(restrictinfo).hashjoinoperator = opno;
    }
    Ok(())
}

/// `check_memoizable` (initsplan.c:3860).
///
/// Set `left_hasheqoperator`/`right_hasheqoperator` if the operand types have a
/// usable hash function + equality operator (`TYPECACHE_HASH_PROC |
/// TYPECACHE_EQ_OPR`).
pub fn check_memoizable(root: &mut PlannerInfo, restrictinfo: RinfoId) {
    let (_opno, lefttype, righttype) = match binary_opclause(root, restrictinfo) {
        Some(x) => x,
        None => return,
    };

    let (hash_proc, eq_opr) = initext::lookup_type_cache_hasheq::call(lefttype);
    if hash_proc != INVALID_OID && eq_opr != INVALID_OID {
        root.rinfo_mut(restrictinfo).left_hasheqoperator = eq_opr;
    }

    // Lookup the right type, unless it's the same as the left type.
    let (hash_proc_r, eq_opr_r) = if lefttype != righttype {
        initext::lookup_type_cache_hasheq::call(righttype)
    } else {
        (hash_proc, eq_opr)
    };
    if hash_proc_r != INVALID_OID && eq_opr_r != INVALID_OID {
        root.rinfo_mut(restrictinfo).right_hasheqoperator = eq_opr_r;
    }
}
