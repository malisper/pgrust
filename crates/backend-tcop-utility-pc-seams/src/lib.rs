//! plancache's slice of utility-statement handling (`tcop/utility.c`). The
//! owning unit installs these; until then a call panics loudly.

use types_error::PgResult;
use types_plancache::{QueryHandle, TupleDescHandle, UtilityStmtHandle};

seam_core::seam!(
    /// `UtilityTupleDescriptor(utilityStmt)`.
    pub fn utility_tuple_descriptor(utility_stmt: UtilityStmtHandle) -> PgResult<TupleDescHandle>
);

seam_core::seam!(
    /// `UtilityContainsQuery(utilityStmt)`; NULL if it contains no parsed query.
    pub fn utility_contains_query(utility_stmt: UtilityStmtHandle) -> PgResult<QueryHandle>
);
