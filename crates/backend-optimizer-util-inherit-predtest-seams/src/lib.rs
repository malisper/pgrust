//! Seam declarations for the `backend-optimizer-util-inherit-predtest` unit
//! (`optimizer/util/predtest.c` + `optimizer/util/inherit.c`).
//!
//! These two are OUTWARD dependencies on subsystems not yet ported into the
//! layered tree: the executor-backed constant-comparison evaluator that
//! `operator_predicate_proof` uses, and the `inval.c` syscache-callback
//! registry that the btree-proof lookaside cache hooks for `pg_amop` changes.
//! Their real owners are the executor (execExpr) and inval.c; both are tracked
//! in `seams-init`'s `CONTRACT_RECONCILE_PENDING` until a const-eval entry
//! point / a non-plancache syscache-callback registration land. Until then a
//! call panics loudly.
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

use types_core::primitive::Oid;
use types_error::PgResult;
use types_nodes::primnodes::Const;
use types_syscache::syscache_ids::SysCacheIdentifier;

/// The `pg_amop` invalidation callback signature, mirroring the C
/// `SyscacheCallbackFunction` `(Datum arg, int cacheid, uint32 hashvalue)`.
/// `predtest.c`'s `InvalidateOprProofCacheCallBack` ignores `arg` (always 0),
/// so the slot drops it and carries only the cache id and hash value.
pub type OprProofCacheCallbackFn = fn(cacheid: i32, hashvalue: u32);

seam_core::seam!(
    /// `CacheRegisterSyscacheCallback(cacheid, func, (Datum) 0)` (inval.c) —
    /// arrange for `func` to be called whenever rows of the syscache `cacheid`
    /// (`AMOPOPID` for the proof cache) are invalidated. `predtest.c` registers
    /// this once, when it first builds `OprProofCacheHash`. Owner: inval.c.
    pub fn register_oprproof_syscache_callback(
        cacheid: SysCacheIdentifier,
        func: OprProofCacheCallbackFn,
    ) -> PgResult<()>
);

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
