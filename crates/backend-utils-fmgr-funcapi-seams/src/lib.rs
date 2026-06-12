//! Seam declarations for the `backend-utils-fmgr-funcapi` unit
//! (`utils/fmgr/funcapi.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;
use types_namespace::FuncArgInfo;

seam_core::seam!(
    /// `get_func_arg_info(proctup, &p_argtypes, &p_argnames, &p_argmodes)`
    /// (funcapi.c) for the pg_proc row of `func_oid`, preceded by the
    /// caller's `proargnames` null test (`MatchNamedCall`'s
    /// `SysCacheGetAttr(PROCOID, ..., Anum_pg_proc_proargnames, &isnull)`):
    /// `Ok(None)` when `proargnames` is SQL null. `Err` carries cache-lookup
    /// `elog(ERROR)`s and OOM from the copies.
    pub fn func_arg_info(func_oid: Oid) -> PgResult<Option<FuncArgInfo>>
);
