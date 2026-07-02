use crate::cache_lookup_error;
use datum::Datum;
use mcx::{Mcx, PgString};
use types_core::{AttrNumber, InvalidOid, Oid};
use types_error::PgResult;
use types_tuple::NameData;

pub(crate) fn name_to_pgstring<'mcx>(mcx: Mcx<'mcx>, name: &NameData) -> PgResult<PgString<'mcx>> {
    let s = core::str::from_utf8(name.name_str()).expect("catalog NameData is valid UTF-8");
    PgString::from_str_in(s, mcx)
}

#[cold]
fn attribute_lookup_failed(attnum: AttrNumber, relid: Oid) -> Box<types_error::PgError> {
    cache_lookup_error(format!(
        "cache lookup failed for attribute {attnum} of relation {relid}"
    ))
}

pub fn get_attname<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: AttrNumber,
    missing_ok: bool,
) -> PgResult<Option<PgString<'mcx>>> {
    match syscache_seams::lookup_pg_attribute_shape::call(relid, attnum)? {
        Some(att) => Ok(Some(name_to_pgstring(mcx, &att.attname)?)),
        None if missing_ok => Ok(None),
        None => Err(attribute_lookup_failed(attnum, relid)),
    }
}

pub fn get_attnum(relid: Oid, attname: &str) -> PgResult<AttrNumber> {
    syscache_seams::lookup_pg_attribute_attnum_by_name::call(relid, attname)
}

pub fn get_attgenerated(relid: Oid, attnum: AttrNumber) -> PgResult<i8> {
    match syscache_seams::lookup_pg_attribute_shape::call(relid, attnum)? {
        Some(att) => Ok(att.attgenerated),
        None => Err(attribute_lookup_failed(attnum, relid)),
    }
}

pub fn get_atttype(relid: Oid, attnum: AttrNumber) -> PgResult<Oid> {
    Ok(match syscache_seams::lookup_pg_attribute_shape::call(relid, attnum)? {
        Some(att) => att.atttypid,
        None => InvalidOid,
    })
}

pub fn get_atttypetypmodcoll(relid: Oid, attnum: AttrNumber) -> PgResult<(Oid, i32, Oid)> {
    match syscache_seams::lookup_pg_attribute_shape::call(relid, attnum)? {
        Some(att) => Ok((att.atttypid, att.atttypmod, att.attcollation)),
        None => Err(attribute_lookup_failed(attnum, relid)),
    }
}

pub fn get_attoptions<'mcx>(mcx: Mcx<'mcx>, relid: Oid, attnum: i16) -> PgResult<Datum> {
    match syscache_seams::pg_attribute_attoptions::call(mcx, relid, attnum)? {
        Some(Some(attopts)) => Ok(attopts),
        Some(None) => Ok(Datum::null()),
        None => Err(attribute_lookup_failed(attnum, relid)),
    }
}
