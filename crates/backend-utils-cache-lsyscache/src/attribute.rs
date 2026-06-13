//! `attribute` family — `lsyscache.c` lookups keyed on `pg_attribute`
//! (`ATTNAME` / `ATTNUM` syscaches).
//!
//! C entry points covered here: `get_attname`, `get_attnum`.
//!
//! The `SearchSysCache*` probes themselves live in the (unported) syscache /
//! catcache layer, so they route through the syscache owner's per-owner seam
//! (`backend-utils-cache-syscache-seams`): `search_attnum_attname` for the raw
//! `ATTNUM` read behind `get_attname`, and `search_attname_attnum` for the
//! dropped-aware `SearchSysCacheAttName` behind `get_attnum`. Both panic
//! loudly until the syscache owner installs them.

use backend_utils_cache_syscache_seams as syscache_seam;
use mcx::{Mcx, PgString};
use types_core::{AttrNumber, InvalidAttrNumber, Oid};
use types_error::{PgError, PgResult};

/// `elog(ERROR, ...)` — an internal error with the default
/// `ERRCODE_INTERNAL_ERROR` SQLSTATE.
fn elog_error(msg: String) -> PgError {
    PgError::error(msg)
}

/// `get_attname(relid, attnum, missing_ok)` (lsyscache.c).
///
/// Given the relation id and the attribute number, return the "attname" field
/// from the attribute relation as a palloc'ed string. If no such attribute
/// exists and `missing_ok` is true, `None` is returned; otherwise a
/// not-intended-for-user-consumption error is thrown.
pub fn get_attname<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: AttrNumber,
    missing_ok: bool,
) -> PgResult<Option<PgString<'mcx>>> {
    // tp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum));
    // if (HeapTupleIsValid(tp))
    // {
    //     Form_pg_attribute att_tup = (Form_pg_attribute) GETSTRUCT(tp);
    //     char *result = pstrdup(NameStr(att_tup->attname));
    //     ReleaseSysCache(tp);
    //     return result;
    // }
    //
    // The seam performs the SearchSysCache2(ATTNUM) lookup, copies
    // NameStr(att_tup->attname) into `mcx` (the C pstrdup), and releases the
    // cache entry; it is the *raw* ATTNUM read, so a present-but-dropped column
    // still yields its name, exactly as get_attname does.
    if let Some(result) = syscache_seam::search_attnum_attname::call(mcx, relid, attnum)? {
        return Ok(Some(result));
    }

    // if (!missing_ok)
    //     elog(ERROR, "cache lookup failed for attribute %d of relation %u",
    //          attnum, relid);
    // return NULL;
    if !missing_ok {
        return Err(elog_error(format!(
            "cache lookup failed for attribute {attnum} of relation {relid}"
        )));
    }
    Ok(None)
}

/// `get_attnum(relid, attname)` (lsyscache.c).
///
/// Given the relation id and the attribute name, return the "attnum" field
/// from the attribute relation. Returns `InvalidAttrNumber` if the attr
/// doesn't exist (or is dropped).
pub fn get_attnum(relid: Oid, attname: &str) -> PgResult<AttrNumber> {
    // tp = SearchSysCacheAttName(relid, attname);
    // if (HeapTupleIsValid(tp))
    // {
    //     Form_pg_attribute att_tup = (Form_pg_attribute) GETSTRUCT(tp);
    //     AttrNumber result = att_tup->attnum;
    //     ReleaseSysCache(tp);
    //     return result;
    // }
    // else
    //     return InvalidAttrNumber;
    //
    // SearchSysCacheAttName is the dropped-aware ATTNAME read: it returns NULL
    // (here `Ok(None)`) on a cache miss AND for a present-but-dropped column.
    // The seam returns the raw (attnum, attisdropped) of the ATTNAME tuple, so
    // we reproduce the dropped filter here: a dropped column is treated as
    // absent, yielding InvalidAttrNumber.
    match syscache_seam::search_attname_attnum::call(relid, attname)? {
        Some((attnum, attisdropped)) if !attisdropped => Ok(attnum),
        _ => Ok(InvalidAttrNumber),
    }
}
