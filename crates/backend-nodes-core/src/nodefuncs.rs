//! Family: **nodefuncs** — `nodes/nodeFuncs.c` (4838 lines).
//!
//! `exprType` / `exprTypmod` / `exprCollation` / `exprInputCollation` /
//! `exprSetCollation` / `exprLocation`, `exprIsLengthCoercion`,
//! `applyRelabelType`, `strip_implicit_coercions`, `expression_returns_set`,
//! `fix_opfuncids` / `set_opfuncid` / `set_sa_opfuncid`,
//! `check_functions_in_node`, and the generic tree walk/mutate drivers
//! (`expression_tree_walker` / `_mutator`, `query_tree_walker` / `_mutator`,
//! `range_table_walker` / `_mutator`, `raw_expression_tree_walker`,
//! `planstate_tree_walker`) — each a ~533-arm dispatch over every node tag.
//!
//! Owns the canonical `backend-nodes-nodeFuncs-seams` (`expr_type_info`,
//! `expr_type`, `call_expr_argtype`, `call_expr_arg_stable`, `expr_variadic`,
//! `get_expr_result_type_node`, `get_call_expr_argtype_node`,
//! `expr_input_collation_node`) — installed in `init_seams()` when filled.
//!
//! Builds on the F0-expanded Expr/Node vocabulary (assemble/expr-eval-keystone,
//! already merged) — the split Expr/Node model is preserved (163+ consumers
//! depend on it; do NOT unify).
//!
//! NOTE — SUB-DECOMP CANDIDATE: at 4838 lines with ~533-arm dispatch tables in
//! each walker/mutator, this family is itself too large for one pass. When
//! scheduled it should be split into sub-families (e.g. expr-type-inspect /
//! expr-collation / opfuncid-fixup / expression-tree-walk-mutate /
//! query+range-table-walk-mutate / raw-walk / planstate-walk), each its own
//! pass against the expanded Expr/Node enum. Skeleton here is the family-level
//! placeholder.

#![allow(unused)]

/// Family marker — nodeFuncs lands here (itself a sub-decomp). See module docs.
pub fn nodefuncs_family_unimplemented() -> ! {
    todo!("nodefuncs: nodes/nodeFuncs.c not yet ported (decomp family; itself a sub-decomp)")
}
