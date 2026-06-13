//! `opclass` family — `lsyscache.c` lookups keyed on `pg_opclass` /
//! `pg_opfamily` / `pg_amproc` (operator-class and operator-family metadata).
//!
//! SCAFFOLD STAGE: signatures mirror the seam declarations; bodies are
//! `todo!()` until the SearchSysCache logic (catcache seam) lands.
//!
//! C entry points covered here: `get_opclass_input_type`, `get_opclass_family`,
//! `get_opfamily_method`, `get_opfamily_proc`, `get_opfamily_name`,
//! `GetDefaultOpClass` (default-opclass surface).

use mcx::{Mcx, PgString};
use types_core::Oid;
use types_error::PgResult;

/// `get_opclass_input_type(opclass)` (lsyscache.c).
pub fn get_opclass_input_type(_opclass: Oid) -> PgResult<Oid> {
    todo!("get_opclass_input_type: SearchSysCache(CLAOID) -> opcintype")
}

/// `get_opclass_family(opclass)` (lsyscache.c).
pub fn get_opclass_family(_opclass: Oid) -> PgResult<Oid> {
    todo!("get_opclass_family: SearchSysCache(CLAOID) -> opcfamily")
}

/// `get_opfamily_method(opfid)` (lsyscache.c).
pub fn get_opfamily_method(_opfid: Oid) -> PgResult<Oid> {
    todo!("get_opfamily_method: SearchSysCache(OPFAMILYOID) -> opfmethod")
}

/// `get_opfamily_proc(opfamily, lefttype, righttype, procnum)` (lsyscache.c).
pub fn get_opfamily_proc(
    _opfamily: Oid,
    _lefttype: Oid,
    _righttype: Oid,
    _procnum: i16,
) -> PgResult<Oid> {
    todo!("get_opfamily_proc: SearchSysCache4(AMPROCNUM) -> amproc")
}

/// `get_opfamily_name(opfid, missing_ok)` (lsyscache.c).
pub fn get_opfamily_name<'mcx>(
    _mcx: Mcx<'mcx>,
    _opfid: Oid,
    _missing_ok: bool,
) -> PgResult<Option<PgString<'mcx>>> {
    todo!("get_opfamily_name: SearchSysCache(OPFAMILYOID) -> pstrdup(opfname)")
}

/// `GetDefaultOpClass(type_id, am_id)` (default operator-class surface).
pub fn get_default_opclass(_type_id: Oid, _am_id: Oid) -> PgResult<Oid> {
    todo!("get_default_opclass: scan pg_opclass for opcdefault matching type/am")
}
