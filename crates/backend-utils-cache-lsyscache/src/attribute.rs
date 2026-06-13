//! `attribute` family — `lsyscache.c` lookups keyed on `pg_attribute`
//! (`ATTNAME` / `ATTNUM` syscaches).
//!
//! SCAFFOLD STAGE: signatures mirror the seam declarations; bodies are
//! `todo!()` until the SearchSysCache logic (catcache seam) lands.
//!
//! C entry points covered here: `get_attname`, `get_attnum`.

use mcx::{Mcx, PgString};
use types_core::{AttrNumber, Oid};
use types_error::PgResult;

/// `get_attname(relid, attnum, missing_ok)` (lsyscache.c).
pub fn get_attname<'mcx>(
    _mcx: Mcx<'mcx>,
    _relid: Oid,
    _attnum: AttrNumber,
    _missing_ok: bool,
) -> PgResult<Option<PgString<'mcx>>> {
    todo!("get_attname: SearchSysCache2(ATTNUM) -> pstrdup(attname)")
}

/// `get_attnum(relid, attname)` (lsyscache.c).
pub fn get_attnum(_relid: Oid, _attname: &str) -> PgResult<AttrNumber> {
    todo!("get_attnum: SearchSysCacheAttName -> attnum")
}
