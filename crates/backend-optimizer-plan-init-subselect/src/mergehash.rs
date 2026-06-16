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
/// `(opno, leftarg, rightarg)`. Returns `None` for pseudoconstant rinfos or
/// when the clause is not a binary `OpExpr` (`is_opclause` + `list_length == 2`).
fn binary_opclause(root: &PlannerInfo, restrictinfo: RinfoId) -> Option<(Oid, Expr, Expr)> {
    let ri = root.rinfo(restrictinfo);
    if ri.pseudoconstant {
        return None;
    }
    let clause_id = ri.clause;
    if let Expr::OpExpr(op) = root.node(clause_id) {
        if op.args.len() != 2 {
            return None;
        }
        let left = op.args[0].clone();
        let right = op.args[1].clone();
        Some((op.opno, left, right))
    } else {
        None
    }
}

/// Read `contain_volatile_functions((Node *) restrictinfo->clause)`.
fn clause_is_volatile(root: &PlannerInfo, restrictinfo: RinfoId) -> bool {
    let clause_id = root.rinfo(restrictinfo).clause;
    let clause = root.node(clause_id).clone();
    eqext::contain_volatile_functions::call(&clause)
}

/// `check_mergejoinable` (initsplan.c:3795).
///
/// If the restrictinfo's clause is mergejoinable, set the mergejoin info fields.
/// Supported for binary opclauses where the operator is mergejoinable and there
/// are no volatile functions in the args.
pub fn check_mergejoinable(root: &mut PlannerInfo, restrictinfo: RinfoId) -> PgResult<()> {
    let (opno, leftarg, _rightarg) = match binary_opclause(root, restrictinfo) {
        Some(x) => x,
        None => return Ok(()),
    };
    let lefttype = eqext::expr_type::call(&leftarg);

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
    let (opno, leftarg, _rightarg) = match binary_opclause(root, restrictinfo) {
        Some(x) => x,
        None => return Ok(()),
    };
    let lefttype = eqext::expr_type::call(&leftarg);

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
    let (_opno, leftarg, rightarg) = match binary_opclause(root, restrictinfo) {
        Some(x) => x,
        None => return,
    };

    let lefttype = eqext::expr_type::call(&leftarg);
    let (hash_proc, eq_opr) = initext::lookup_type_cache_hasheq::call(lefttype);
    if hash_proc != INVALID_OID && eq_opr != INVALID_OID {
        root.rinfo_mut(restrictinfo).left_hasheqoperator = eq_opr;
    }

    let righttype = eqext::expr_type::call(&rightarg);

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
