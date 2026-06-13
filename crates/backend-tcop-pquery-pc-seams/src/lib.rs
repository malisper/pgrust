//! plancache's slice of the portal machinery (`tcop/pquery.c`) and the
//! executor result-tupdesc helper (`executor/execTuples.c`'s
//! `ExecCleanTypeFromTL`). The owning units install these; until then a call
//! panics loudly.

use types_error::PgResult;
use types_plancache::{PortalStrategy, QueryListHandle, TargetListHandle, TupleDescHandle};

seam_core::seam!(
    /// `ChoosePortalStrategy(stmt_list)`.
    pub fn choose_portal_strategy(stmt_list: QueryListHandle) -> PgResult<PortalStrategy>
);

seam_core::seam!(
    /// `ExecCleanTypeFromTL(targetList)` (allocated in the current context).
    pub fn exec_clean_type_from_tl(target_list: TargetListHandle) -> PgResult<TupleDescHandle>
);

seam_core::seam!(
    /// `FetchStatementTargetList(stmt)`.
    pub fn fetch_statement_target_list(stmt: types_plancache::QueryHandle) -> PgResult<TargetListHandle>
);
