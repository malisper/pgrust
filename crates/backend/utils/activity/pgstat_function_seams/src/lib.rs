//! Seam declarations for the function-usage tracking entry points of
//! `src/backend/utils/activity/pgstat_function.c` (PostgreSQL 18.3).
//!
//! The executor's FUSAGE function-call opcodes (`EEOP_FUNCEXPR_FUSAGE` /
//! `EEOP_FUNCEXPR_STRICT_FUSAGE`, and the set-returning-function path in
//! `nodeFunctionscan`) wrap the function invocation in
//! `pgstat_init_function_usage()` / `pgstat_end_function_usage()`. Those live in
//! the `backend-utils-activity-pgstat-function` owner, which pulls in the whole
//! pgstat shared-entry machinery; the executor reaches them through these seams.
//! `pgstat_function::init_seams()` installs them.

#![allow(non_snake_case)]

use ::types_core::primitive::Oid;
use ::types_error::PgResult;
use ::types_pgstat::activity_pgstat::PgStat_FunctionCallUsage;

seam_core::seam!(
    /// `pgstat_init_function_usage(FunctionCallInfo fcinfo,
    /// PgStat_FunctionCallUsage *fcu)` (pgstat_function.c): initialize the
    /// per-call usage block before invoking a function. `fn_stats` / `fn_oid`
    /// are C's `fcinfo->flinfo->fn_stats` / `fcinfo->flinfo->fn_oid`. Returns the
    /// filled-in `PgStat_FunctionCallUsage` (C fills the caller's stack value).
    pub fn pgstat_init_function_usage(
        fn_stats: u8,
        fn_oid: Oid,
    ) -> PgResult<PgStat_FunctionCallUsage>
);

seam_core::seam!(
    /// `pgstat_end_function_usage(PgStat_FunctionCallUsage *fcu, bool finalize)`
    /// (pgstat_function.c): compute the function call usage and update the
    /// pending per-function stat counters after invoking the function.
    pub fn pgstat_end_function_usage(
        fcu: &mut PgStat_FunctionCallUsage,
        finalize: bool,
    ) -> PgResult<()>
);
