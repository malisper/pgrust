// amcmds.c, get_am_type_oid family only; CREATE/DROP ACCESS METHOD stay with
// the utility dispatch louds.
#![allow(non_upper_case_globals)]

use cache_syscache::{ReleaseSysCache, SearchSysCache1, SysCacheGetAttr, SysCacheKey, AMNAME};
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult, ERRCODE_UNDEFINED_OBJECT, ERROR};

const Anum_pg_am_oid: i32 = 1;
const Anum_pg_am_amtype: i32 = 4;
pub const AMTYPE_INDEX: u8 = b'i';
pub const AMTYPE_TABLE: u8 = b't';

fn get_am_type_string(amtype: u8) -> &'static str {
    match amtype {
        AMTYPE_INDEX => "INDEX",
        AMTYPE_TABLE => "TABLE",
        other => panic!("invalid access method type '{}'", other as char),
    }
}

pub fn get_am_type_oid(amname: &str, amtype: u8, missing_ok: bool) -> PgResult<Oid> {
    let mut oid = InvalidOid;
    if let Some(tup) = SearchSysCache1(AMNAME, SysCacheKey::Str(amname))? {
        let (t, _) = SysCacheGetAttr(AMNAME, &tup, Anum_pg_am_amtype)?;
        if amtype != 0 && t.as_i8() as u8 != amtype {
            ReleaseSysCache(tup);
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "access method \"{amname}\" is not of type {}",
                        get_am_type_string(amtype)
                    ),
                )
                .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            ));
        }
        let (d, _) = SysCacheGetAttr(AMNAME, &tup, Anum_pg_am_oid)?;
        oid = d.as_oid();
        ReleaseSysCache(tup);
    }
    if oid == InvalidOid && !missing_ok {
        return Err(Box::new(
            PgError::new(ERROR, format!("access method \"{amname}\" does not exist"))
                .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
        ));
    }
    Ok(oid)
}

pub fn get_index_am_oid(amname: &str, missing_ok: bool) -> PgResult<Oid> {
    get_am_type_oid(amname, AMTYPE_INDEX, missing_ok)
}

pub fn get_table_am_oid(amname: &str, missing_ok: bool) -> PgResult<Oid> {
    get_am_type_oid(amname, AMTYPE_TABLE, missing_ok)
}
