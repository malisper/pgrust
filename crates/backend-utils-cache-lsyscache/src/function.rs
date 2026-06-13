//! `function` family — `lsyscache.c` lookups keyed on `pg_proc`
//! (`PROCOID` syscache).
//!
//! SCAFFOLD STAGE: signatures mirror the seam declarations; bodies are
//! `todo!()` until the SearchSysCache logic (catcache seam) lands.
//!
//! C entry points covered here: `get_func_rettype`, `get_func_signature`.

use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::PgResult;

/// `get_func_rettype(funcid)` (lsyscache.c).
pub fn get_func_rettype(_funcid: Oid) -> PgResult<Oid> {
    todo!("get_func_rettype: SearchSysCache(PROCOID) -> prorettype")
}

/// `get_func_signature(funcid, &argtypes, &nargs)` (lsyscache.c).
pub fn get_func_signature<'mcx>(_mcx: Mcx<'mcx>, _func_oid: Oid) -> PgResult<PgVec<'mcx, Oid>> {
    todo!("get_func_signature: SearchSysCache(PROCOID) -> copy proargtypes into mcx")
}
