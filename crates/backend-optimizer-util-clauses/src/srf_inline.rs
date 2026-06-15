//! `inline_set_returning_function` (clauses.c) — the SRF-inline gate called by
//! `preprocess_function_rtes` (prepjointree.c:931).
//!
//! # Model gap
//!
//! The C body inspects a `RangeTblEntry` (`rtekind`/`funcordinality`/
//! `functions`) and, for a single simple `FuncExpr`, reads its `funcretset` /
//! `args` and the function's `pg_proc` row through the gate ladder, then (for an
//! inlinable SQL-language SRF) runs the prosrc parse + rewrite + querytree
//! validation core. In this repo's model the `RangeTblEntry.functions` list
//! holds `NodePtr` handles into the not-yet-ported parser/planner node universe
//! (a `RangeTblFunction` whose `funcexpr` is itself an opaque node), NOT the
//! lifetime-free `Expr` tree this crate operates on — so neither the gate ladder
//! nor the inline core can read a walkable `FuncExpr` here.
//!
//! The whole routine therefore rides the `inline_set_returning_function_core`
//! seam, installed by the SRF-inliner owner (the planner/parser leg) when it
//! lands. No merged consumer calls this entry today; until the owner installs
//! the seam a call panics loudly (the inline decision changes which plans
//! SQL-language SRFs get — a wrong-answer class, never a silent NULL).

use types_core::Oid;
use types_error::PgResult;
use types_nodes::primnodes::Expr;

/// C: `inline_set_returning_function(root, rte)` (clauses.c). Returns the
/// inlined query expression (`Ok(Some)`) or declines (`Ok(None)`). Keyed by the
/// function OID of the RTE's single `FuncExpr` (all the gate inputs the C reads
/// off the RTE/FuncExpr/pg_proc are resolved by the owner from `funcid`).
///
/// The gate ladder + inline core live behind the seam; see the module docs for
/// why the `RangeTblEntry`/`RangeTblFunction` node universe is not walkable as
/// `Expr` here.
pub fn inline_set_returning_function(funcid: Oid) -> PgResult<Option<Expr>> {
    backend_optimizer_util_clauses_seams::inline_set_returning_function_core::call(funcid)
}
