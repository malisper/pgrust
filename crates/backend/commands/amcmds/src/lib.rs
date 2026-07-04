//! amcmds.c: get_am_type_oid lookup family (DDL arms unported).

use cache_syscache::{ReleaseSysCache, SearchSysCache1, SysCacheGetAttrNotNull, SysCacheKey};
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult, ERROR};

pub const AMTYPE_INDEX: u8 = b'i';
pub const AMTYPE_TABLE: u8 = b't';

const Anum_pg_am_oid: i32 = 1;
const Anum_pg_am_amtype: i32 = 4;

fn get_am_type_string(amtype: u8) -> &'static str {
    match amtype {
        AMTYPE_INDEX => "INDEX",
        AMTYPE_TABLE => "TABLE",
        _ => unreachable!("invalid access method type '{}'", amtype as char),
    }
}

fn get_am_type_oid(amname: &str, amtype: u8, missing_ok: bool) -> PgResult<Oid> {
    let mut oid = InvalidOid;
    if let Some(tup) = SearchSysCache1(cache_syscache::cacheinfo::AMNAME, SysCacheKey::Str(amname))?
    {
        let this_type =
            SysCacheGetAttrNotNull(cache_syscache::cacheinfo::AMNAME, &tup, Anum_pg_am_amtype)?
                .as_i8() as u8;
        if amtype != 0 && this_type != amtype {
            ReleaseSysCache(tup);
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "access method \"{amname}\" is not of type {}",
                        get_am_type_string(amtype)
                    ),
                )
                .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE),
            ));
        }
        oid = SysCacheGetAttrNotNull(cache_syscache::cacheinfo::AMNAME, &tup, Anum_pg_am_oid)?
            .as_oid();
        ReleaseSysCache(tup);
    }
    if oid == InvalidOid && !missing_ok {
        return Err(Box::new(
            PgError::new(ERROR, format!("access method \"{amname}\" does not exist"))
                .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT),
        ));
    }
    Ok(oid)
}

pub fn get_table_am_oid(amname: &str, missing_ok: bool) -> PgResult<Oid> {
    get_am_type_oid(amname, AMTYPE_TABLE, missing_ok)
}

pub fn get_index_am_oid(amname: &str, missing_ok: bool) -> PgResult<Oid> {
    get_am_type_oid(amname, AMTYPE_INDEX, missing_ok)
}
