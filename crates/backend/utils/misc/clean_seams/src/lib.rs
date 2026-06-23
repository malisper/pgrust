//! Seam declarations for the `backend-utils-misc-clean` unit
//! (`utils/misc/pg_rusage.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.
//!
//! `PGRUsage` is a fully spelled-out C struct ([`::rusage::PgRUsage`]) the
//! caller stack-allocates; these seams only run the `getrusage`/`gettimeofday`
//! capture (`init`) and the delta formatting (`show`) the owner provides.

#![allow(non_snake_case)]

use ::mcx::Mcx;
use ::types_error::PgResult;
use ::rusage::PgRUsage;

seam_core::seam!(
    /// `pg_rusage_init(&ru0)` (pg_rusage.c): capture the wall-clock + CPU-usage
    /// snapshot. Returns the populated struct the caller owns on its stack.
    pub fn pg_rusage_init() -> PgRUsage
);

seam_core::seam!(
    /// `pg_rusage_show(&ru0)` (pg_rusage.c): capture a fresh snapshot and format
    /// the elapsed user/system/wall deltas since `ru0` into the displayable
    /// `CPU: user: ... system: ... elapsed: ...` text.
    pub fn pg_rusage_show<'mcx>(mcx: Mcx<'mcx>, ru0: PgRUsage) -> PgResult<::mcx::PgString<'mcx>>
);
