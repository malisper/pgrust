//! Seam declarations for the `backend-nodes-queryjumble` unit
//! (`nodes/queryjumble.c`): the query-id machinery portalcmds invokes.

use ::types_error::PgResult;
use ::nodes::portalcmds::{JumbleState, Query};

/// The canonical field-bearing `Query` (the simple-query / parse-analysis tree).
/// Distinct from the opaque [`Query`] token above (the DECLARE-CURSOR
/// pass-through); the canonical path jumbles this one.
use ::nodes::copy_query::Query as CanonicalQuery;

seam_core::seam!(
    /// `IsQueryIdEnabled()` (queryjumble.c) — true if query-id computation is
    /// turned on (the `compute_query_id` GUC plus any extension request).
    pub fn is_query_id_enabled() -> bool
);

seam_core::seam!(
    /// `JumbleQuery(query)` (queryjumble.c) — compute the query's jumble (sets
    /// `query->queryId`) and return the `JumbleState` describing constant
    /// locations. Allocates; can `ereport(ERROR)`.
    pub fn jumble_query(query: &Query) -> PgResult<JumbleState>
);

seam_core::seam!(
    /// `JumbleQuery(query)->queryId` (queryjumble.c) over the canonical
    /// field-bearing `Query` — compute the query's 64-bit jumble id and return
    /// it (the caller stores it into `query.queryId`). This is the
    /// simple-query / parse-analysis entry that drives `pg_stat_activity`'s
    /// `query_id`; the caller has already checked `IsQueryIdEnabled()`.
    pub fn jumble_query_compute<'mcx>(query: &CanonicalQuery<'mcx>) -> i64
);
