//! foreign.c lookup slice hosted here: backend-foreign-foreign is scoped
//! non-core, but the DDL surface needs these accessors.
use datum::Datum;
use mcx::Mcx;
use types_core::{InvalidOid, Oid};
use types_error::{PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_UNDEFINED_OBJECT, ERROR};

use cache_syscache::cacheinfo::{
    FOREIGNDATAWRAPPERNAME, FOREIGNDATAWRAPPEROID, FOREIGNSERVERNAME, FOREIGNSERVEROID,
    FOREIGNTABLEREL, USERMAPPINGUSERSERVER,
};
use cache_syscache::{
    GetSysCacheOid, ReleaseSysCache, SearchSysCache1, SearchSysCache2, SysCacheGetAttr,
    SysCacheGetAttrNotNull, SysCacheKey,
};

pub const Anum_pg_foreign_data_wrapper_oid: i32 = 1;
pub const Anum_pg_foreign_data_wrapper_fdwname: i32 = 2;
pub const Anum_pg_foreign_data_wrapper_fdwowner: i32 = 3;
pub const Anum_pg_foreign_data_wrapper_fdwhandler: i32 = 4;
pub const Anum_pg_foreign_data_wrapper_fdwvalidator: i32 = 5;
pub const Anum_pg_foreign_data_wrapper_fdwacl: i32 = 6;
pub const Anum_pg_foreign_data_wrapper_fdwoptions: i32 = 7;
pub const Natts_pg_foreign_data_wrapper: usize = 7;

pub const Anum_pg_foreign_server_oid: i32 = 1;
pub const Anum_pg_foreign_server_srvname: i32 = 2;
pub const Anum_pg_foreign_server_srvowner: i32 = 3;
pub const Anum_pg_foreign_server_srvfdw: i32 = 4;
pub const Anum_pg_foreign_server_srvtype: i32 = 5;
pub const Anum_pg_foreign_server_srvversion: i32 = 6;
pub const Anum_pg_foreign_server_srvacl: i32 = 7;
pub const Anum_pg_foreign_server_srvoptions: i32 = 8;
pub const Natts_pg_foreign_server: usize = 8;

pub const Anum_pg_user_mapping_oid: i32 = 1;
pub const Anum_pg_user_mapping_umuser: i32 = 2;
pub const Anum_pg_user_mapping_umserver: i32 = 3;
pub const Anum_pg_user_mapping_umoptions: i32 = 4;
pub const Natts_pg_user_mapping: usize = 4;

pub const Anum_pg_foreign_table_ftrelid: i32 = 1;
pub const Anum_pg_foreign_table_ftserver: i32 = 2;
pub const Anum_pg_foreign_table_ftoptions: i32 = 3;
pub const Natts_pg_foreign_table: usize = 3;

pub struct ForeignDataWrapper<'mcx> {
    pub fdwid: Oid,
    pub owner: Oid,
    pub fdwname: &'mcx str,
    pub fdwhandler: Oid,
    pub fdwvalidator: Oid,
}

pub struct ForeignServer<'mcx> {
    pub serverid: Oid,
    pub servername: &'mcx str,
    pub owner: Oid,
    pub fdwid: Oid,
}

fn str_in<'mcx>(mcx: Mcx<'mcx>, s: &[u8]) -> PgResult<&'mcx str> {
    let bytes = mcx::slice_borrow_in(mcx, s)?;
    // SAFETY: catalog names are valid UTF-8 (server encoding invariant).
    Ok(unsafe { core::str::from_utf8_unchecked(bytes) })
}

pub(crate) fn name_attr<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<&'mcx str> {
    // SAFETY: d points at the row's inline NAMEDATALEN name column.
    let name = unsafe { &*(d.as_usize() as *const types_tuple::NameData) };
    str_in(mcx, name.name_str())
}

pub fn get_foreign_data_wrapper_oid(fdwname: &str, missing_ok: bool) -> PgResult<Oid> {
    let oid = GetSysCacheOid(
        FOREIGNDATAWRAPPERNAME,
        Anum_pg_foreign_data_wrapper_oid,
        SysCacheKey::Str(fdwname),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )?;
    if oid == InvalidOid && !missing_ok {
        return Err(::elog::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("foreign-data wrapper \"{fdwname}\" does not exist"))
            .into_error()
            .into());
    }
    Ok(oid)
}

pub fn get_foreign_server_oid(servername: &str, missing_ok: bool) -> PgResult<Oid> {
    let oid = GetSysCacheOid(
        FOREIGNSERVERNAME,
        Anum_pg_foreign_server_oid,
        SysCacheKey::Str(servername),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )?;
    if oid == InvalidOid && !missing_ok {
        return Err(::elog::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("server \"{servername}\" does not exist"))
            .into_error()
            .into());
    }
    Ok(oid)
}

pub fn GetForeignDataWrapper<'mcx>(mcx: Mcx<'mcx>, fdwid: Oid) -> PgResult<ForeignDataWrapper<'mcx>> {
    let Some(tp) = SearchSysCache1(FOREIGNDATAWRAPPEROID, SysCacheKey::Value(Datum::from_oid(fdwid)))?
    else {
        panic!("cache lookup failed for foreign-data wrapper {fdwid}");
    };
    let fdw = ForeignDataWrapper {
        fdwid,
        owner: SysCacheGetAttrNotNull(FOREIGNDATAWRAPPEROID, &tp, Anum_pg_foreign_data_wrapper_fdwowner)?
            .as_oid(),
        fdwname: name_attr(
            mcx,
            SysCacheGetAttrNotNull(FOREIGNDATAWRAPPEROID, &tp, Anum_pg_foreign_data_wrapper_fdwname)?,
        )?,
        fdwhandler: SysCacheGetAttrNotNull(
            FOREIGNDATAWRAPPEROID,
            &tp,
            Anum_pg_foreign_data_wrapper_fdwhandler,
        )?
        .as_oid(),
        fdwvalidator: SysCacheGetAttrNotNull(
            FOREIGNDATAWRAPPEROID,
            &tp,
            Anum_pg_foreign_data_wrapper_fdwvalidator,
        )?
        .as_oid(),
    };
    ReleaseSysCache(tp);
    Ok(fdw)
}

pub fn GetForeignDataWrapperByName<'mcx>(
    mcx: Mcx<'mcx>,
    fdwname: &str,
    missing_ok: bool,
) -> PgResult<Option<ForeignDataWrapper<'mcx>>> {
    let fdw_id = get_foreign_data_wrapper_oid(fdwname, missing_ok)?;
    if fdw_id == InvalidOid {
        return Ok(None);
    }
    Ok(Some(GetForeignDataWrapper(mcx, fdw_id)?))
}

pub fn GetForeignServer<'mcx>(mcx: Mcx<'mcx>, serverid: Oid) -> PgResult<ForeignServer<'mcx>> {
    let Some(tp) = SearchSysCache1(FOREIGNSERVEROID, SysCacheKey::Value(Datum::from_oid(serverid)))?
    else {
        panic!("cache lookup failed for foreign server {serverid}");
    };
    let server = ForeignServer {
        serverid,
        servername: name_attr(
            mcx,
            SysCacheGetAttrNotNull(FOREIGNSERVEROID, &tp, Anum_pg_foreign_server_srvname)?,
        )?,
        owner: SysCacheGetAttrNotNull(FOREIGNSERVEROID, &tp, Anum_pg_foreign_server_srvowner)?.as_oid(),
        fdwid: SysCacheGetAttrNotNull(FOREIGNSERVEROID, &tp, Anum_pg_foreign_server_srvfdw)?.as_oid(),
    };
    ReleaseSysCache(tp);
    Ok(server)
}

pub fn GetForeignServerByName<'mcx>(
    mcx: Mcx<'mcx>,
    srvname: &str,
    missing_ok: bool,
) -> PgResult<Option<ForeignServer<'mcx>>> {
    let serverid = get_foreign_server_oid(srvname, missing_ok)?;
    if serverid == InvalidOid {
        return Ok(None);
    }
    Ok(Some(GetForeignServer(mcx, serverid)?))
}

pub fn get_user_mapping_oid(userid: Oid, serverid: Oid) -> PgResult<Oid> {
    GetSysCacheOid(
        USERMAPPINGUSERSERVER,
        Anum_pg_user_mapping_oid,
        SysCacheKey::Value(Datum::from_oid(userid)),
        SysCacheKey::Value(Datum::from_oid(serverid)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )
}

pub fn GetForeignServerIdByRelId(relid: Oid) -> PgResult<Oid> {
    let Some(tp) = SearchSysCache1(FOREIGNTABLEREL, SysCacheKey::Value(Datum::from_oid(relid)))?
    else {
        panic!("cache lookup failed for foreign table {relid}");
    };
    let serverid =
        SysCacheGetAttrNotNull(FOREIGNTABLEREL, &tp, Anum_pg_foreign_table_ftserver)?.as_oid();
    ReleaseSysCache(tp);
    Ok(serverid)
}

/// GetFdwRoutineByServerId up to the handler call: the no-handler error is the
/// live surface; an installed handler is loud (dfmgr/LANGUAGE C unported).
pub fn GetFdwRoutineByServerId<'mcx>(mcx: Mcx<'mcx>, serverid: Oid) -> PgResult<()> {
    let Some(tp) = SearchSysCache1(FOREIGNSERVEROID, SysCacheKey::Value(Datum::from_oid(serverid)))?
    else {
        panic!("cache lookup failed for foreign server {serverid}");
    };
    let fdwid = SysCacheGetAttrNotNull(FOREIGNSERVEROID, &tp, Anum_pg_foreign_server_srvfdw)?.as_oid();
    ReleaseSysCache(tp);

    let fdw = GetForeignDataWrapper(mcx, fdwid)?;
    if fdw.fdwhandler == InvalidOid {
        return Err(::elog::ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!("foreign-data wrapper \"{}\" has no handler", fdw.fdwname))
            .into_error()
            .into());
    }
    panic!("unported: foreign.c GetFdwRoutine (FDW handler invocation; dfmgr/LANGUAGE C)");
}

pub fn GetFdwRoutineByRelId<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<()> {
    let serverid = GetForeignServerIdByRelId(relid)?;
    GetFdwRoutineByServerId(mcx, serverid)
}

/// MappingUserName (foreign.h).
pub fn MappingUserName<'mcx>(mcx: Mcx<'mcx>, userid: Oid) -> PgResult<&'mcx str> {
    if userid == InvalidOid {
        return Ok("public");
    }
    let name = miscinit::GetUserNameFromId(mcx, userid, false)?.expect("noerr=false");
    str_in(mcx, name.as_bytes())
}

pub(crate) fn attr_option_datum(
    cache_id: i32,
    tup: &catcache::CatCTuple,
    attnum: i32,
) -> PgResult<Option<Datum>> {
    let (d, isnull) = SysCacheGetAttr(cache_id, tup, attnum)?;
    Ok(if isnull { None } else { Some(d) })
}

pub(crate) fn user_mapping_lookup(user: Oid, server: Oid) -> PgResult<Option<catcache::CatCTuple>> {
    SearchSysCache2(
        USERMAPPINGUSERSERVER,
        SysCacheKey::Value(Datum::from_oid(user)),
        SysCacheKey::Value(Datum::from_oid(server)),
    )
}
