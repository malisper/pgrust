//! `namespace-range-index-pubsub` family — `lsyscache.c` lookups keyed on
//! `pg_namespace` / `pg_am` and the range / index / publication-subscription
//! helpers.
//!
//! SCAFFOLD STAGE: signatures mirror the seam declarations; bodies are
//! `todo!()` until the SearchSysCache logic (catcache seam) lands.
//!
//! C entry points covered here so far: `get_namespace_name`,
//! `get_namespace_name_or_temp`, `get_am_name`. The remaining range / index /
//! pubsub helpers in this C section (`get_range_subtype`,
//! `get_range_collation`, `get_index_column_opclass`,
//! `get_publication_oid` / `get_subscription_oid`, ...) have no seam
//! declaration yet and will land here with their own decls.

use mcx::{Mcx, PgString};
use types_core::Oid;
use types_error::PgResult;

/// `get_namespace_name(nspid)` (lsyscache.c).
pub fn get_namespace_name<'mcx>(
    _mcx: Mcx<'mcx>,
    _nspid: Oid,
) -> PgResult<Option<PgString<'mcx>>> {
    todo!("get_namespace_name: SearchSysCache(NAMESPACEOID) -> pstrdup(nspname)")
}

/// `get_namespace_name_or_temp(nspid)` (lsyscache.c).
pub fn get_namespace_name_or_temp<'mcx>(
    _mcx: Mcx<'mcx>,
    _nspid: Oid,
) -> PgResult<Option<PgString<'mcx>>> {
    todo!("get_namespace_name_or_temp: \"pg_temp\" if temp, else get_namespace_name")
}

/// `get_am_name(amOid)` (lsyscache.c).
pub fn get_am_name<'mcx>(_mcx: Mcx<'mcx>, _am_oid: Oid) -> PgResult<Option<PgString<'mcx>>> {
    todo!("get_am_name: SearchSysCache(AMOID) -> pstrdup(amname)")
}
