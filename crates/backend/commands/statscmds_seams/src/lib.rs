//! Seam declarations for the `backend-commands-statscmds` unit
//! (`commands/statscmds.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use ::types_core::primitive::Oid;
use ::types_error::PgResult;

seam_core::seam!(
    /// `RemoveStatisticsById(statsOid)` (commands/statscmds.c): the per-class
    /// `OCLASS_STATISTIC_EXT` drop handler dependency.c's `doDeletion` invokes
    /// for a `pg_statistic_ext` object. Removes the extended-statistics object's
    /// catalog rows. Can `ereport(ERROR)`, carried on `Err`.
    pub fn RemoveStatisticsById(statsOid: Oid) -> PgResult<()>
);
