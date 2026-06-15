//! Implementations of the caller-shaped projected-row seams declared in
//! `backend-utils-cache-syscache-seams`: catcache lookup + attribute
//! extraction + field projection, copied into the caller's `mcx`.

use mcx::{vec_with_capacity_in, Mcx, MemoryContext, PgString, PgVec};
use types_cache::SysCacheKey;
use types_core::Oid;
use types_error::{PgError, PgResult};
use types_hash::backend_access_hash_hashvalidate::{AmopRow, AmprocRow, OpclassForm};
use types_tuple::backend_access_common_heaptuple::{Datum, FormedTuple};
// `types_datum::Datum` (the bare-word shim) survives only at the unmigrated
// cross-crate contract edge `SysCacheKey::Value`'s search-key word (C:
// `Datum key1..key4`), audited `types-cache` vocabulary not in this batch.
use types_datum::Datum as KeyDatum;

use crate::{
    ReleaseSysCache, SearchSysCache1, SearchSysCache2, SearchSysCacheList1, SysCacheGetAttrNotNull,
    AMOPSTRATEGY, AMPROCNUM, CASTSOURCETARGET, CLAOID, RELOID,
};
use backend_utils_cache_syscache_seams::CastRow;

/// `Anum_pg_class_relam` (`catalog/pg_class.h`).
const Anum_pg_class_relam: i32 = 7;
/// `Anum_pg_class_reloftype` (`catalog/pg_class.h`).
const Anum_pg_class_reloftype: i32 = 5;

// `catalog/pg_cast.h` attribute numbers.
const Anum_pg_cast_oid: i32 = 1;
const Anum_pg_cast_castfunc: i32 = 4;
const Anum_pg_cast_castcontext: i32 = 5;
const Anum_pg_cast_castmethod: i32 = 6;

// `catalog/pg_opclass.h` attribute numbers.
const Anum_pg_opclass_opcname: i32 = 3;
const Anum_pg_opclass_opcfamily: i32 = 6;
const Anum_pg_opclass_opcintype: i32 = 7;
const Anum_pg_opclass_opckeytype: i32 = 9;

// `catalog/pg_amop.h` attribute numbers.
const Anum_pg_amop_amoplefttype: i32 = 3;
const Anum_pg_amop_amoprighttype: i32 = 4;
const Anum_pg_amop_amopstrategy: i32 = 5;
const Anum_pg_amop_amoppurpose: i32 = 6;
const Anum_pg_amop_amopopr: i32 = 7;
const Anum_pg_amop_amopsortfamily: i32 = 9;

// `catalog/pg_amproc.h` attribute numbers.
const Anum_pg_amproc_amproclefttype: i32 = 3;
const Anum_pg_amproc_amprocrighttype: i32 = 4;
const Anum_pg_amproc_amprocnum: i32 = 5;
const Anum_pg_amproc_amproc: i32 = 6;

fn byval<'mcx>(value: Datum<'mcx>) -> PgResult<Datum<'mcx>> {
    match value {
        Datum::ByVal(_) => Ok(value),
        Datum::ByRef(_) => Err(PgError::error(
            "syscache projection: expected a by-value attribute",
        )),
    }
}

fn getattr_oid(mcx: Mcx<'_>, cache_id: i32, tup: &FormedTuple<'_>, attnum: i32) -> PgResult<Oid> {
    Ok(byval(SysCacheGetAttrNotNull(mcx, cache_id, tup, attnum)?)?.as_oid())
}

fn getattr_i16(mcx: Mcx<'_>, cache_id: i32, tup: &FormedTuple<'_>, attnum: i32) -> PgResult<i16> {
    Ok(byval(SysCacheGetAttrNotNull(mcx, cache_id, tup, attnum)?)?.as_i16())
}

fn getattr_char(mcx: Mcx<'_>, cache_id: i32, tup: &FormedTuple<'_>, attnum: i32) -> PgResult<i8> {
    Ok(byval(SysCacheGetAttrNotNull(mcx, cache_id, tup, attnum)?)?.as_char())
}

/// A `name` attribute (`NameData` bytes) as an owned string in `mcx`.
fn getattr_name<'mcx>(
    mcx: Mcx<'mcx>,
    cache_id: i32,
    tup: &FormedTuple<'_>,
    attnum: i32,
) -> PgResult<PgString<'mcx>> {
    let value = SysCacheGetAttrNotNull(mcx, cache_id, tup, attnum)?;
    let bytes = match &value {
        Datum::ByRef(b) => &b[..],
        Datum::ByVal(_) => {
            return Err(PgError::error("syscache projection: name attribute is by-value"))
        }
    };
    // NameStr(): the NUL-padded fixed-size buffer up to the first NUL.
    let len = bytes.iter().position(|&c| c == 0).unwrap_or(bytes.len());
    PgString::from_str_in(&String::from_utf8_lossy(&bytes[..len]), mcx)
}

/// `SearchSysCache1(RELOID, ObjectIdGetDatum(relid))` projected to the
/// `Form_pg_class.relam` field. `Ok(None)` on a cache miss
/// (`!HeapTupleIsValid`). The projection is by-value, so the tuple copy
/// lives in a scratch context dropped before returning.
pub(crate) fn search_relation_relam(relid: Oid) -> PgResult<Option<Oid>> {
    let scratch = MemoryContext::new("syscache relam projection");
    let mcx = scratch.mcx();
    let tuple = SearchSysCache1(mcx, RELOID, SysCacheKey::Value(KeyDatum::from_oid(relid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let relam = getattr_oid(mcx, RELOID, &tup, Anum_pg_class_relam)?;
    ReleaseSysCache(tup);
    Ok(Some(relam))
}

/// `SearchSysCache1(RELOID, ObjectIdGetDatum(relid))` projected to the
/// `Form_pg_class.reloftype` field (the OF-type for a typed table; `InvalidOid`
/// otherwise). `Ok(None)` on a cache miss (`!HeapTupleIsValid`).
pub(crate) fn search_relation_reloftype(relid: Oid) -> PgResult<Option<Oid>> {
    let scratch = MemoryContext::new("syscache reloftype projection");
    let mcx = scratch.mcx();
    let tuple = SearchSysCache1(mcx, RELOID, SysCacheKey::Value(KeyDatum::from_oid(relid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let reloftype = getattr_oid(mcx, RELOID, &tup, Anum_pg_class_reloftype)?;
    ReleaseSysCache(tup);
    Ok(Some(reloftype))
}

/// `SearchSysCache2(CASTSOURCETARGET, srctype, targettype)` projected to the
/// [`CastRow`] fields (`Form_pg_cast`). `Ok(None)` on a cache miss (no cast).
pub(crate) fn cast_by_source_target(
    sourcetypeid: Oid,
    targettypeid: Oid,
) -> PgResult<Option<CastRow>> {
    let scratch = MemoryContext::new("syscache pg_cast projection");
    let mcx = scratch.mcx();
    let tuple = SearchSysCache2(
        mcx,
        CASTSOURCETARGET,
        SysCacheKey::Value(KeyDatum::from_oid(sourcetypeid)),
        SysCacheKey::Value(KeyDatum::from_oid(targettypeid)),
    )?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let row = CastRow {
        oid: getattr_oid(mcx, CASTSOURCETARGET, &tup, Anum_pg_cast_oid)?,
        castfunc: getattr_oid(mcx, CASTSOURCETARGET, &tup, Anum_pg_cast_castfunc)?,
        castcontext: getattr_char(mcx, CASTSOURCETARGET, &tup, Anum_pg_cast_castcontext)?,
        castmethod: getattr_char(mcx, CASTSOURCETARGET, &tup, Anum_pg_cast_castmethod)?,
    };
    ReleaseSysCache(tup);
    Ok(Some(row))
}

/// `SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassoid))` projected to the
/// `Form_pg_opclass` fields the hash validator reads. `Ok(None)` on a cache
/// miss (`!HeapTupleIsValid`).
pub(crate) fn search_opclass<'mcx>(
    mcx: Mcx<'mcx>,
    opclassoid: Oid,
) -> PgResult<Option<OpclassForm<'mcx>>> {
    let tuple = SearchSysCache1(mcx, CLAOID, SysCacheKey::Value(KeyDatum::from_oid(opclassoid)))?;
    let Some(tup) = tuple else {
        return Ok(None);
    };
    let form = OpclassForm {
        opcfamily: getattr_oid(mcx, CLAOID, &tup, Anum_pg_opclass_opcfamily)?,
        opcintype: getattr_oid(mcx, CLAOID, &tup, Anum_pg_opclass_opcintype)?,
        opckeytype: getattr_oid(mcx, CLAOID, &tup, Anum_pg_opclass_opckeytype)?,
        opcname: getattr_name(mcx, CLAOID, &tup, Anum_pg_opclass_opcname)?,
    };
    ReleaseSysCache(tup);
    Ok(Some(form))
}

/// `SearchSysCacheList1(AMOPSTRATEGY, ObjectIdGetDatum(opfamilyoid))` member
/// rows, projected.
pub(crate) fn search_amop_list<'mcx>(
    mcx: Mcx<'mcx>,
    opfamilyoid: Oid,
) -> PgResult<PgVec<'mcx, AmopRow>> {
    let members = SearchSysCacheList1(
        mcx,
        AMOPSTRATEGY,
        SysCacheKey::Value(KeyDatum::from_oid(opfamilyoid)),
    )?;
    let mut rows = vec_with_capacity_in(mcx, members.len())?;
    for tup in &members {
        rows.push(AmopRow {
            amopstrategy: getattr_i16(mcx, AMOPSTRATEGY, tup, Anum_pg_amop_amopstrategy)?,
            amoppurpose: getattr_char(mcx, AMOPSTRATEGY, tup, Anum_pg_amop_amoppurpose)?,
            amopopr: getattr_oid(mcx, AMOPSTRATEGY, tup, Anum_pg_amop_amopopr)?,
            amopsortfamily: getattr_oid(mcx, AMOPSTRATEGY, tup, Anum_pg_amop_amopsortfamily)?,
            amoplefttype: getattr_oid(mcx, AMOPSTRATEGY, tup, Anum_pg_amop_amoplefttype)?,
            amoprighttype: getattr_oid(mcx, AMOPSTRATEGY, tup, Anum_pg_amop_amoprighttype)?,
        });
    }
    Ok(rows)
}

/// `SearchSysCacheList1(AMPROCNUM, ObjectIdGetDatum(opfamilyoid))` member
/// rows, projected.
pub(crate) fn search_amproc_list<'mcx>(
    mcx: Mcx<'mcx>,
    opfamilyoid: Oid,
) -> PgResult<PgVec<'mcx, AmprocRow>> {
    let members = SearchSysCacheList1(
        mcx,
        AMPROCNUM,
        SysCacheKey::Value(KeyDatum::from_oid(opfamilyoid)),
    )?;
    let mut rows = vec_with_capacity_in(mcx, members.len())?;
    for tup in &members {
        rows.push(AmprocRow {
            amproclefttype: getattr_oid(mcx, AMPROCNUM, tup, Anum_pg_amproc_amproclefttype)?,
            amprocrighttype: getattr_oid(mcx, AMPROCNUM, tup, Anum_pg_amproc_amprocrighttype)?,
            amprocnum: getattr_i16(mcx, AMPROCNUM, tup, Anum_pg_amproc_amprocnum)?,
            amproc: getattr_oid(mcx, AMPROCNUM, tup, Anum_pg_amproc_amproc)?,
        });
    }
    Ok(rows)
}
