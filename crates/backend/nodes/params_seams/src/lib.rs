//! Seam declarations for the `backend-nodes-params` unit (`nodes/params.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_error::PgResult;
use ::nodes::params::{ParamListInfo, ParamListInfoData};

seam_core::seam!(
    /// `makeParamList(numParams)` (params.c) — allocate a `ParamListInfo` with
    /// `numParams` `ParamExternData` slots. Returns the value param list
    /// (`Option<Rc<ParamListInfoData>>`), shared by `Rc`. Allocates.
    pub fn make_param_list(num_params: i32) -> PgResult<ParamListInfo>
);

seam_core::seam!(
    /// Store one external parameter value into a value param list slot — the
    /// `params->params[paramno].{value,isnull,pflags,ptype} = ...` block of
    /// `exec_bind_message` (postgres.c). `datumCopy`s the per-message (`'mcx`)
    /// value into the backend-lifetime param-list context so the stored
    /// `Datum<'static>` outlives the message, and marks the slot
    /// `PARAM_FLAG_CONST`. The owner (`backend-nodes-core`) installs it from its
    /// `init_seams()`.
    pub fn store_param_extern<'mcx>(
        param_li: &mut ParamListInfoData<'static>,
        paramno: i32,
        value: &types_tuple::heaptuple::Datum<'mcx>,
        isnull: bool,
        ptype: types_core::Oid,
    ) -> PgResult<()>
);
