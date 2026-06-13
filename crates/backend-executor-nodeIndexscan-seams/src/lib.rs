//! Seam declarations for the shared index-scan-key helpers in the
//! `backend-executor-nodeIndexscan` unit (`executor/nodeIndexscan.c`):
//! `ExecIndexBuildScanKeys` / `ExecIndexEvalRuntimeKeys`, used by every
//! index-scan node (plain, index-only, bitmap).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. (The parallel-executor methods on index-scan
//! nodes live in the separate `backend-executor-nodeIndexscan-pq-seams` crate.)

#![allow(non_snake_case)]

seam_core::seam!(
    /// `ExecIndexBuildScanKeys(planstate, index, quals, isorderby, &scanKeys,
    /// &numScanKeys, &runtimeKeys, &numRuntimeKeys, &arrayKeys, &numArrayKeys)`
    /// (nodeIndexscan.c): build the index scan-key arrays from the index
    /// qualification clauses. For index-only scans the owned model fills the
    /// node's `ioss_ScanKeys`/`ioss_OrderByKeys` (per `is_orderby`) and appends
    /// to `ioss_RuntimeKeys` (index-only scans pass `NULL` for ArrayKeys).
    /// Allocates the key arrays in the per-query context; building can also
    /// `ereport(ERROR)` (unsupported operator forms). Fallible.
    pub fn exec_index_build_scan_keys_ios<'mcx>(
        node: &mut types_nodes::IndexOnlyScanState<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        index: types_rel::Relation<'mcx>,
        quals: Option<&[types_nodes::primnodes::Expr]>,
        is_orderby: bool,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `ExecIndexEvalRuntimeKeys(econtext, runtimeKeys, numRuntimeKeys)`
    /// (nodeIndexscan.c): evaluate the index-only scan node's runtime scan
    /// keys, writing the computed datums into the keys' target scankey slots.
    /// Runs in the node's runtime expression context (id into the EState pool).
    /// Fallible on `ereport(ERROR)` from a key expression.
    pub fn exec_index_eval_runtime_keys_ios<'mcx>(
        node: &mut types_nodes::IndexOnlyScanState<'mcx>,
        estate: &mut types_nodes::EStateData<'mcx>,
        econtext: types_nodes::EcxtId,
    ) -> types_error::PgResult<()>
);
