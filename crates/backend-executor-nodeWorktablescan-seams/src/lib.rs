//! Seam declarations for the `backend-executor-nodeWorktablescan` unit
//! (`executor/nodeWorktablescan.c`).
//!
//! A `WorkTableScan` node is the recursive (inner) term's window onto the
//! `RecursiveUnion`'s working table. At init time it recovers the live
//! `RecursiveUnionState` its enclosing `RecursiveUnion` deposited into the
//! reserved `wtParam` `Param` slot
//! (`EState.es_param_exec_vals[plan->wtParam].value`). That cross-node channel
//! is owned by nodeWorktablescan: the deposit side (the seam below, driven by
//! `ExecInitRecursiveUnion`) and the recovery side both live with that unit's
//! state model, so the deposit goes through this seam and panics loudly until
//! nodeWorktablescan lands.

#![allow(non_snake_case)]

seam_core::seam!(
    /// `prmdata = &(estate->es_param_exec_vals[node->wtParam]);
    /// Assert(prmdata->execPlan == NULL); prmdata->value =
    /// PointerGetDatum(rustate); prmdata->isnull = false;`
    /// (nodeRecursiveunion.c `ExecInitRecursiveUnion`).
    ///
    /// Publish the freshly-built `RecursiveUnionState` into the reserved
    /// `wt_param` `Param` slot so descendant `WorkTableScan` nodes can recover
    /// it. The `EState.es_param_exec_vals` slot stores a `Datum` that aliases
    /// the live node-state pointer — a cross-node aliasing channel owned by
    /// nodeWorktablescan's state model (the recovery side). The owning unit
    /// installs this from its `init_seams()` when it lands; until then the call
    /// panics loudly.
    pub fn publish_wtparam_slot(
        rustate: &mut types_nodes::RecursiveUnionStateData<'_>,
        estate: &mut types_nodes::EStateData<'_>,
        wt_param: i32,
    ) -> types_error::PgResult<()>
);
