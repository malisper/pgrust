//! Seam declarations for the `backend-optimizer-util-inherit-predtest` unit
//! (`optimizer/util/predtest.c` + `optimizer/util/inherit.c`).
//!
//! This is an OUTWARD dependency on a subsystem not yet ported into the layered
//! tree: the executor-backed constant-comparison evaluator that
//! `operator_predicate_proof` uses. Its real owner is the executor (execExpr).
//!
//! The `pg_amop` proof-cache invalidation callback registration does NOT need a
//! seam of its own: `predtest.c`'s `CacheRegisterSyscacheCallback(AMOPOPID,
//! InvalidateOprProofCacheCallBack, 0)` goes straight through the real inval.c
//! owner via `inval_seams::cache_register_syscache_callback`.
//!
//! Everything else `predtest.c`/`inherit.c` need crosses through already-landed
//! seam crates that their real owners ship:
//!   * lsyscache (`get_commutator`/`get_negator`/`op_strict`/`op_volatile`/
//!     `func_strict`/`get_opfamily_member_for_cmptype`/
//!     `get_op_index_interpretation`/`get_typlenbyvalalign`),
//!   * arrayfuncs (`array_const_nitems`/`deconstruct_array`/
//!     `array_get_elemtype`),
//!   * pathnode (`check_for_interrupts`).

#![allow(non_snake_case)]

use ::types_core::primitive::Oid;
use ::types_error::PgResult;
use ::nodes::primnodes::Const;

seam_core::seam!(
    /// The executor-backed constant test of `operator_predicate_proof`
    /// (predtest.c:1983-2020): build `make_opclause(test_op, BOOLOID, false,
    /// pred_const, clause_const, InvalidOid, collation)`, `fix_opfuncids`,
    /// `ExecInitExpr`, and `ExecEvalExprSwitchContext` it in a throwaway
    /// `EState`. Returns the boolean result, or `None` for a NULL result
    /// (C's "null predicate test result" DEBUG2 + non-proof). `Err` carries the
    /// evaluation `ereport(ERROR)`. Owner: executor (execExpr) / nodeFuncs
    /// `make_opclause`+`fix_opfuncids`.
    pub fn eval_const_test(
        test_op: Oid,
        pred_const: &Const,
        clause_const: &Const,
        collation: Oid,
    ) -> PgResult<Option<bool>>
);
