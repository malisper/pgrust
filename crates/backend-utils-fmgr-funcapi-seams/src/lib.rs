//! Seam declarations for the `backend-utils-fmgr-funcapi` unit
//! (`utils/fmgr/funcapi.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_namespace::FuncArgInfo;

seam_core::seam!(
    /// `get_func_arg_info(proctup, &p_argtypes, &p_argnames, &p_argmodes)`
    /// (funcapi.c) for the pg_proc row of `func_oid` (the C caller holds the
    /// tuple; the owned marshal re-fetches it by OID). The arrays are
    /// allocated in `mcx` (C: palloc in the current context). `Err` carries
    /// cache-lookup / deform `elog(ERROR)`s and OOM from the copies.
    pub fn get_func_arg_info<'mcx>(
        mcx: Mcx<'mcx>,
        func_oid: Oid,
    ) -> PgResult<FuncArgInfo<'mcx>>
);
