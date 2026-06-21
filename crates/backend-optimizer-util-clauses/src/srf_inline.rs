//! `inline_set_returning_function` (clauses.c:5067) — the SRF-inline gate
//! called by `preprocess_function_rtes` (prepjointree.c:914).
//!
//! The C routine inspects a FUNCTION `RangeTblEntry` and, for a single simple
//! SQL-language set-returning `FuncExpr` that clears the gate ladder, parses +
//! rewrites + validates the function body into a `Query` to substitute as the
//! RTE's subquery. The gate ladder — ORDINALITY, single simple `FuncExpr`,
//! `funcretset`, volatile/sub-select-free args, EXECUTE privilege, no fmgr hook,
//! and the `pg_proc` property ladder (LANGUAGE SQL, plain function, not STRICT /
//! VOLATILE / SECURITY DEFINER, non-VOID rettype, no `proconfig`) — is run here
//! over the real owned-`Expr` `FuncExpr`. A function that fails any gate
//! declines with `Ok(None)` (the C `return NULL`), which is exactly what every
//! C-language SRF (e.g. `generate_series`) does at the LANGUAGE-SQL gate.
//!
//! Only when the ladder confirms an inlinable SQL-language SRF does the routine
//! enter the body parse/rewrite/validate core, which rides the
//! `inline_set_returning_function_sql_body` seam (owner: the clauses.c SRF-
//! inliner SQL leg — the SQL-function parse/rewrite path is not ported, so a
//! reachable SQL SRF panics loudly there; no C-language SRF reaches it).

use mcx::Mcx;
use types_acl::{ACL_EXECUTE, ACLCHECK_OK};
use types_catalog::pg_proc::PROKIND_FUNCTION;
use types_core::catalog::{PROCEDURE_RELATION_ID, VOIDOID};
use types_core::Oid;
use types_error::PgResult;
use types_nodes::parsenodes::{RangeTblEntry, RTEKind};
use types_nodes::primnodes::Expr;
use types_pathnodes::PlannerInfo;

use backend_optimizer_util_clauses_seams as clauses_seam;

/// `PROVOLATILE_VOLATILE` (`pg_proc.h`) — the `provolatile` byte for a VOLATILE
/// function.
const PROVOLATILE_VOLATILE: u8 = b'v';

/// `inline_set_returning_function(root, rte)` (clauses.c:5067). Returns the
/// inlined `Query` (`Ok(Some)`) to substitute as the RTE's subquery, or
/// `Ok(None)` to decline.
pub fn inline_set_returning_function<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    rte: &RangeTblEntry<'mcx>,
) -> PgResult<Option<types_nodes::copy_query::Query<'mcx>>> {
    debug_assert_eq!(rte.rtekind, RTEKind::RTE_FUNCTION);

    // check_stack_depth();  -- a SQL SRF referring to itself would recurse; the
    // single-user backend's stack-depth guard fires elsewhere, so the explicit
    // recursion guard is the runtime's, not modeled here.

    // Fail if the RTE has ORDINALITY — we don't implement that here.
    if rte.funcordinality {
        return Ok(None);
    }

    // Fail if RTE isn't a single, simple FuncExpr.
    if rte.functions.len() != 1 {
        return Ok(None);
    }
    // rtfunc = linitial(rte->functions); if (!IsA(rtfunc->funcexpr, FuncExpr)) NULL.
    let fexpr = match rtfunc_funcexpr(&rte.functions[0]) {
        Some(fe) => fe,
        None => return Ok(None),
    };

    let func_oid = fexpr.funcid;

    // The function must be declared to return a set, else inlining would change
    // the results if the contained SELECT didn't return exactly one row.
    if !fexpr.funcretset {
        return Ok(None);
    }

    // Refuse to inline if the arguments contain any volatile functions or
    // sub-selects (multiple evaluation would change behavior).
    for arg in &fexpr.args {
        if crate::grounded::contain_volatile_functions(Some(arg))?
            || crate::grounded::contain_subplans(Some(arg))?
        {
            return Ok(None);
        }
    }

    // Check permission to call function (fail later, if not).
    let userid = backend_utils_init_miscinit_seams::get_user_id::call();
    let aclresult = backend_catalog_aclchk_seams::object_aclcheck::call(
        PROCEDURE_RELATION_ID,
        func_oid,
        userid,
        ACL_EXECUTE,
    )?;
    if aclresult != ACLCHECK_OK {
        return Ok(None);
    }

    // Check whether a plugin wants to hook function entry/exit.
    if clauses_seam::fmgr_hook_is_needed::call(func_oid) {
        return Ok(None);
    }

    // Look at the function's pg_proc entry. Forget it if the function is not
    // SQL-language or has other showstopper properties: STRICT (can't enforce),
    // VOLATILE (own snapshot), SETOF VOID, SECURITY DEFINER, a different
    // prokind, an argument-count mismatch, or any proconfig. (Rechecking
    // prokind / proretset / pronargs is paranoia, as in C.)
    let form = clauses_seam::get_func_form::call(func_oid)?;
    let prokind = backend_utils_cache_lsyscache_seams::get_func_prokind::call(func_oid)?;
    if !form.prolang_is_sql
        || prokind as i8 != PROKIND_FUNCTION
        || form.proisstrict
        || form.provolatile == PROVOLATILE_VOLATILE
        || form.prorettype == VOIDOID
        || form.prosecdef
        || !form.proretset
        || fexpr.args.len() as i16 != form.pronargs
        || !form.proconfig_isnull
    {
        return Ok(None);
    }

    // The function is an inlinable SQL-language SRF. The body parse / rewrite /
    // single-SELECT querytree validation + parameter substitution core rides the
    // SQL leg seam (owned by backend-parser-analyze).
    let querytree = match clauses_seam::inline_set_returning_function_sql_body::call(
        mcx, root, rte, func_oid,
    )? {
        Some(q) => q,
        None => return Ok(None),
    };

    // Since there is now no trace of the function in the plan tree, explicitly
    // record the plan's dependency on the function (clauses.c:5331).
    backend_optimizer_plan_setrefs_seams::record_plan_function_dependency::call(root, func_oid)?;

    // Notice if the inserted query adds a dependency on the calling role due to
    // RLS quals (clauses.c:5337).
    if querytree.hasRowSecurity {
        if let Some(glob) = root.glob.as_deref_mut() {
            glob.depends_on_role = true;
        }
    }

    Ok(Some(querytree))
}

/// Extract the `FuncExpr` from a `RangeTblFunction` node's `funcexpr`, mirroring
/// C's `IsA(rtfunc->funcexpr, FuncExpr)` gate: `Some(&FuncExpr)` only when the
/// node is exactly a `Node::Expr(Expr::FuncExpr(..))`.
fn rtfunc_funcexpr<'a, 'mcx>(
    node: &'a types_nodes::nodes::NodePtr<'mcx>,
) -> Option<&'a types_nodes::primnodes::FuncExpr> {
    let rtf = node.as_rangetblfunction()?;
    let fe = rtf.funcexpr.as_deref()?;
    fe.as_funcexpr()
}

/// Re-export of the funcid-keyed scalar-inline declination, kept for the
/// `inline_set_returning_function_core` consumers that delegate by OID.
pub fn inline_set_returning_function_by_oid(funcid: Oid) -> PgResult<Option<Expr>> {
    clauses_seam::inline_set_returning_function_core::call(funcid)
}
