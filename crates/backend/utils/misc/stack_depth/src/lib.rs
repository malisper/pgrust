//! stack_depth: the GUC-facing half of C
//! src/backend/utils/misc/stack_depth.c (check/assign hooks, the rlimit-based
//! startup adjustment, seam installation).
//!
//! The GUARD ITSELF lives in `stack_depth_core` and is RE-EXPORTED here, so
//! every existing `stack_depth::check_stack_depth()` call site is unchanged.
//! The split (lane p1-nodes, behavior-identical) exists because the node
//! walkers must call the guard as C does, and this crate's `guc` dependency
//! is cyclic with them.

#![allow(non_camel_case_types)]

pub use stack_depth_core::{self, *};

use types_error::PgResult;
use types_guc::GucSource;

#[cfg(test)]
mod tests;

pub fn adjust_max_stack_depth_from_rlimit() -> PgResult<()> {
    let stack_rlimit = get_stack_depth_rlimit();
    if stack_rlimit > 0 {
        let mut new_limit = stack_rlimit.saturating_sub(STACK_DEPTH_SLOP) / 1024;
        if new_limit > 100 {
            new_limit = new_limit.min(2048);
            guc::SetConfigOption(
                "max_stack_depth",
                Some(&new_limit.to_string()),
                types_guc::GucContext::PGC_POSTMASTER,
                GucSource::PGC_S_ENV_VAR,
            )?;
        }
    }
    Ok(())
}

pub fn check_max_stack_depth(newval: i32, _source: GucSource) -> bool {
    let newval_bytes = newval as isize * 1024;
    let stack_rlimit = get_stack_depth_rlimit();

    if stack_rlimit > 0 && newval_bytes > stack_rlimit - STACK_DEPTH_SLOP {
        guc::GUC_check_errdetail(format!(
            "\"max_stack_depth\" must not exceed {}kB.",
            (stack_rlimit - STACK_DEPTH_SLOP) / 1024
        ));
        guc::GUC_check_errhint(
            "Increase the platform's stack depth limit via \"ulimit -s\" or local equivalent.",
        );
        return false;
    }
    true
}

pub fn init_seams() {
    guc_tables::hooks::check_max_stack_depth
        .install(|newval, _extra, source| Ok(check_max_stack_depth(*newval, source)));
    guc_tables::hooks::assign_max_stack_depth
        .install(|newval, _extra| assign_max_stack_depth(newval));
    guc_tables::vars::max_stack_depth.install(guc_tables::GucVarAccessors {
        get: max_stack_depth,
        set: set_max_stack_depth,
    });
}

