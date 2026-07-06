//! optimizer/util var.c — Var-node collection/inspection walkers over the
//! opaque `Node` vocabulary (the engine lives in `nodes_core`).
//! Unit rows appendinfo.c/paramassign.c/tlist.c have their own catalog rows;
//! this crate is the var.c half.

pub mod var;

#[cfg(test)]
mod tests;

pub use var::{
    contain_uplevel_vars, contain_var_clause, contain_vars_of_level,
    contain_vars_returning_old_or_new,
    flatten_group_exprs, flatten_group_exprs_list, flatten_join_alias_vars, locate_var_of_level, pull_var_clause,
    FjavRoot,
    pull_varattnos, pull_varnos, pull_varnos_of_level, pull_varnos_with_phv_hook,
    pull_vars_of_level, strip_noop_phvs, PhvVarnosHook,
    PVC_INCLUDE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS, PVC_INCLUDE_WINDOWFUNCS,
    PVC_RECURSE_AGGREGATES, PVC_RECURSE_PLACEHOLDERS, PVC_RECURSE_WINDOWFUNCS,
};

pub fn init_seams() {
    var_seams::contain_var_clause::set(|node| {
        // Infallible in C (pure walk); the PgResult exists only for engine
        // uniformity, so an Err here is a real bug — fail loud.
        var::contain_var_clause(node).expect("contain_var_clause")
    });
}
