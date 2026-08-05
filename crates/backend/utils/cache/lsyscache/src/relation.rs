use crate::attribute::name_to_pgstring;
use crate::cache_lookup_error;
use mcx::{Mcx, PgString};
use types_core::{InvalidAttrNumber, InvalidOid, Oid};
use types_error::PgResult;

#[cold]
fn relation_lookup_failed(relid: Oid) -> Box<types_error::PgError> {
    cache_lookup_error(format!("cache lookup failed for relation {relid}"))
}

#[cold]
fn index_lookup_failed(index_oid: Oid) -> Box<types_error::PgError> {
    cache_lookup_error(format!("cache lookup failed for index {index_oid}"))
}

pub fn get_relname_relid(relname: &str, relnamespace: Oid) -> PgResult<Oid> {
    syscache_seams::lookup_pg_class_relid_by_name::call(relname, relnamespace)
}

// #ifdef NOT_USED in C.
pub fn get_relnatts(relid: Oid) -> PgResult<i32> {
    Ok(match syscache_seams::lookup_pg_class_ls_shape::call(relid)? {
        Some(reltup) => reltup.relnatts as i32,
        None => InvalidAttrNumber as i32,
    })
}

pub fn get_rel_name<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<Option<PgString<'mcx>>> {
    match syscache_seams::pg_class_relname::call(relid)? {
        Some(name) => Ok(Some(name_to_pgstring(mcx, &name)?)),
        None => Ok(None),
    }
}

pub fn get_rel_namespace(relid: Oid) -> PgResult<Oid> {
    Ok(match syscache_seams::lookup_pg_class_ls_shape::call(relid)? {
        Some(reltup) => reltup.relnamespace,
        None => InvalidOid,
    })
}

pub fn get_rel_type_id(relid: Oid) -> PgResult<Oid> {
    Ok(match syscache_seams::lookup_pg_class_ls_shape::call(relid)? {
        Some(reltup) => reltup.reltype,
        None => InvalidOid,
    })
}

pub fn get_rel_relkind(relid: Oid) -> PgResult<i8> {
    Ok(match syscache_seams::lookup_pg_class_ls_shape::call(relid)? {
        Some(reltup) => reltup.relkind,
        None => 0,
    })
}

pub fn get_rel_relhassubclass(relid: Oid) -> PgResult<bool> {
    Ok(match syscache_seams::lookup_pg_class_ls_shape::call(relid)? {
        Some(reltup) => reltup.relhassubclass,
        None => false,
    })
}

pub fn get_rel_relispartition(relid: Oid) -> PgResult<bool> {
    Ok(match syscache_seams::lookup_pg_class_ls_shape::call(relid)? {
        Some(reltup) => reltup.relispartition,
        None => false,
    })
}

pub fn get_rel_tablespace(relid: Oid) -> PgResult<Oid> {
    Ok(match syscache_seams::lookup_pg_class_ls_shape::call(relid)? {
        Some(reltup) => reltup.reltablespace,
        None => InvalidOid,
    })
}

pub fn get_rel_persistence(relid: Oid) -> PgResult<i8> {
    match syscache_seams::lookup_pg_class_ls_shape::call(relid)? {
        Some(reltup) => Ok(reltup.relpersistence),
        None => Err(relation_lookup_failed(relid)),
    }
}

pub fn get_rel_relam(relid: Oid) -> PgResult<Oid> {
    match syscache_seams::lookup_pg_class_ls_shape::call(relid)? {
        Some(reltup) => Ok(reltup.relam),
        None => Err(relation_lookup_failed(relid)),
    }
}

// C reads shape + indclass off one INDEXRELID probe; the split projections
// cost a second probe on this cold path.
pub fn get_index_column_opclass(index_oid: Oid, attno: i32) -> PgResult<Oid> {
    let Some(rd_index) = syscache_seams::lookup_pg_index_ls_shape::call(index_oid)? else {
        return Ok(InvalidOid);
    };
    debug_assert!(attno > 0 && attno <= rd_index.indnatts as i32);
    if attno > rd_index.indnkeyatts as i32 {
        return Ok(InvalidOid);
    }
    match syscache_seams::pg_index_indclass_element::call(index_oid, attno - 1)? {
        Some(opclass) => Ok(opclass),
        None => Ok(InvalidOid),
    }
}

pub fn get_index_isreplident(index_oid: Oid) -> PgResult<bool> {
    Ok(match syscache_seams::lookup_pg_index_ls_shape::call(index_oid)? {
        Some(rd_index) => rd_index.indisreplident,
        None => false,
    })
}

pub fn get_index_isvalid(index_oid: Oid) -> PgResult<bool> {
    match syscache_seams::lookup_pg_index_ls_shape::call(index_oid)? {
        Some(rd_index) => Ok(rd_index.indisvalid),
        None => Err(index_lookup_failed(index_oid)),
    }
}

pub fn get_index_isclustered(index_oid: Oid) -> PgResult<bool> {
    match syscache_seams::lookup_pg_index_ls_shape::call(index_oid)? {
        Some(rd_index) => Ok(rd_index.indisclustered),
        None => Err(index_lookup_failed(index_oid)),
    }
}
