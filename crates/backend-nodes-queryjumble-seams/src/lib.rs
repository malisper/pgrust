//! Seam declarations for the `backend-nodes-queryjumble` unit
//! (`nodes/queryjumble.c`): the query-id machinery portalcmds invokes.

use types_error::PgResult;
use types_nodes::portalcmds::{JumbleState, Query};

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
