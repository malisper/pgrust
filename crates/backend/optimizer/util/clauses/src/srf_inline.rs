// inline_set_returning_function (clauses.c:5067) — the SRF-inline gate ladder
// called by preprocess_function_rtes. The parser-dependent middle (body
// parse/rewrite, check_sql_fn_retval, parameter substitution) rides the
// inline_set_returning_sql_body seam, installed by sql_functions (a
// clauses->parser dep cycles). DIVERGENCE: FmgrHookIsNeeded is not modeled
// (no fmgr hook mechanism exists here).

use mcx::Mcx;
use types_core::catalog::{PROCEDURE_RELATION_ID, VOIDOID};
use types_error::PgResult;
use types_nodes::parsenodes::{Query, RTEKind};
use types_nodes::Node;

const PROKIND_FUNCTION: i8 = b'f' as i8;
const PROVOLATILE_VOLATILE: i8 = b'v' as i8;
const ACL_EXECUTE: u64 = 1 << 7;
const ACLCHECK_OK: i32 = 0;

pub fn inline_set_returning_function<'mcx>(
    mcx: Mcx<'mcx>,
    rte_node: Node<'mcx>,
) -> PgResult<Option<&'mcx Query<'mcx>>> {
    let rte = rte_node.as_range_tbl_entry().expect("RTE_FUNCTION RangeTblEntry");
    debug_assert_eq!(rte.rtekind, RTEKind::RTE_FUNCTION);

    // A SQL SRF referring to itself recurses here too; C only guards the
    // stack.
    stack_depth::check_stack_depth()?;

    if rte.funcordinality {
        return Ok(None);
    }
    if rte.functions.len() != 1 {
        return Ok(None);
    }
    let rtfunc = rte.functions.nth(0).as_range_tbl_function().expect("functions cell");
    let Some(fexpr_node) = rtfunc.funcexpr else { return Ok(None) };
    let Some(fexpr) = fexpr_node.as_func_expr() else { return Ok(None) };
    let func_oid = fexpr.funcid;

    // Inlining a non-set-returning call would change the results if the
    // contained SELECT didn't return exactly one row.
    if !fexpr.funcretset {
        return Ok(None);
    }
    // Volatile or subplan-bearing arguments could be evaluated more than once
    // after substitution.
    for arg in &fexpr.args {
        if crate::contain_volatile_functions(arg)? || crate::contain_subplans(arg)? {
            return Ok(None);
        }
    }

    let userid = miscinit_seams::get_user_id::call();
    let aclresult = aclchk_seams::object_aclcheck::call(
        PROCEDURE_RELATION_ID,
        func_oid,
        userid,
        ACL_EXECUTE,
    )?;
    if aclresult != ACLCHECK_OK {
        return Ok(None);
    }

    // Showstopper pg_proc properties: STRICT can't be enforced, VOLATILE
    // implies its own snapshot, SETOF VOID would expose the last SELECT's
    // real result. (Rechecking prokind/proretset/pronargs is paranoia, as C.)
    let shape = syscache_seams::lookup_pg_proc_shape::call(func_oid)?
        .ok_or_else(|| crate::fold::func_lookup_failed(func_oid))?;
    if shape.prolang != fmgr_core::SQL_LANGUAGE_ID
        || shape.prokind != PROKIND_FUNCTION
        || shape.proisstrict
        || shape.provolatile == PROVOLATILE_VOLATILE
        || shape.prorettype == VOIDOID
        || shape.prosecdef
        || !shape.proretset
        || fexpr.args.len() != shape.pronargs as usize
        || !shape.proconfig_isnull
    {
        return Ok(None);
    }

    clauses_seams::inline_set_returning_sql_body::call(mcx, rte_node, shape.prokind)
}
