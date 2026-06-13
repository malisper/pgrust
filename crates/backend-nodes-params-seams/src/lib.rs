//! Seam declarations for the `backend-nodes-params` unit (`nodes/params.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_error::PgResult;
use types_nodes::parsestmt::ParamListInfoHandle;

seam_core::seam!(
    /// `makeParamList(numParams)` (params.c) — allocate a `ParamListInfo` with
    /// `numParams` `ParamExternData` slots. Allocates.
    pub fn make_param_list(num_params: i32) -> PgResult<ParamListInfoHandle>
);
