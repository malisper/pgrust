//! foreign.c lookup slice hosted here: backend-foreign-foreign is scoped
//! non-core, but the DDL surface needs these accessors.
use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::{InvalidOid, Oid};
use types_error::{PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_UNDEFINED_OBJECT, ERROR};
use types_nodes::{FdwKind, FdwRoutine, NodeTag};

use crate::options::{text_body, untransform_options, varlena_image, OptionPair};

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
    pub options: PgVec<'mcx, OptionPair<'mcx>>,
}

pub struct ForeignServer<'mcx> {
    pub serverid: Oid,
    pub servername: &'mcx str,
    pub owner: Oid,
    pub fdwid: Oid,
    pub servertype: Option<&'mcx str>,
    pub serverversion: Option<&'mcx str>,
    pub options: PgVec<'mcx, OptionPair<'mcx>>,
}

pub struct ForeignTable<'mcx> {
    pub relid: Oid,
    pub serverid: Oid,
    pub options: PgVec<'mcx, OptionPair<'mcx>>,
}

pub struct UserMapping<'mcx> {
    pub umid: Oid,
    pub userid: Oid,
    pub serverid: Oid,
    pub options: PgVec<'mcx, OptionPair<'mcx>>,
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
        options: untransform_options(
            mcx,
            attr_option_datum(FOREIGNDATAWRAPPEROID, &tp, Anum_pg_foreign_data_wrapper_fdwoptions)?,
        )?,
    };
    ReleaseSysCache(tp);
    Ok(fdw)
}

fn text_attr<'mcx>(
    mcx: Mcx<'mcx>,
    cache_id: i32,
    tup: &catcache::CatCTuple,
    attnum: i32,
) -> PgResult<Option<&'mcx str>> {
    let Some(d) = attr_option_datum(cache_id, tup, attnum)? else {
        return Ok(None);
    };
    let image = detoast::detoast_attr(mcx, varlena_image(d))?;
    let body = mcx::slice_borrow_in(mcx, text_body(&image))?;
    // SAFETY: catalog text in server encoding.
    Ok(Some(unsafe { core::str::from_utf8_unchecked(body) }))
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
        servertype: text_attr(mcx, FOREIGNSERVEROID, &tp, Anum_pg_foreign_server_srvtype)?,
        serverversion: text_attr(mcx, FOREIGNSERVEROID, &tp, Anum_pg_foreign_server_srvversion)?,
        options: untransform_options(
            mcx,
            attr_option_datum(FOREIGNSERVEROID, &tp, Anum_pg_foreign_server_srvoptions)?,
        )?,
    };
    ReleaseSysCache(tp);
    Ok(server)
}

pub fn GetForeignTable<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<ForeignTable<'mcx>> {
    let Some(tp) = SearchSysCache1(FOREIGNTABLEREL, SysCacheKey::Value(Datum::from_oid(relid)))?
    else {
        panic!("cache lookup failed for foreign table {relid}");
    };
    let ft = ForeignTable {
        relid,
        serverid: SysCacheGetAttrNotNull(FOREIGNTABLEREL, &tp, Anum_pg_foreign_table_ftserver)?
            .as_oid(),
        options: untransform_options(
            mcx,
            attr_option_datum(FOREIGNTABLEREL, &tp, Anum_pg_foreign_table_ftoptions)?,
        )?,
    };
    ReleaseSysCache(tp);
    Ok(ft)
}

const Anum_pg_attribute_attfdwoptions: i32 = 24;

/// GetForeignColumnOptions (foreign.c) — attfdwoptions of relid/attnum.
pub fn GetForeignColumnOptions<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attnum: i16,
) -> PgResult<PgVec<'mcx, OptionPair<'mcx>>> {
    let Some(tp) = SearchSysCache2(
        cache_syscache::cacheinfo::ATTNUM,
        SysCacheKey::Value(Datum::from_oid(relid)),
        SysCacheKey::Value(Datum::from_i16(attnum)),
    )?
    else {
        panic!("cache lookup failed for attribute {attnum} of relation {relid}");
    };
    let options = untransform_options(
        mcx,
        attr_option_datum(cache_syscache::cacheinfo::ATTNUM, &tp, Anum_pg_attribute_attfdwoptions)?,
    )?;
    ReleaseSysCache(tp);
    Ok(options)
}

/// GetUserMapping (foreign.c) — falls back to the PUBLIC mapping.
pub fn GetUserMapping<'mcx>(mcx: Mcx<'mcx>, userid: Oid, serverid: Oid) -> PgResult<UserMapping<'mcx>> {
    let mut tp = user_mapping_lookup(userid, serverid)?;
    if tp.is_none() {
        tp = user_mapping_lookup(InvalidOid, serverid)?;
    }
    let Some(tp) = tp else {
        let server = GetForeignServer(mcx, serverid)?;
        return Err(::elog::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!(
                "user mapping not found for user \"{}\", server \"{}\"",
                MappingUserName(mcx, userid)?,
                server.servername
            ))
            .into_error()
            .into());
    };
    let um = UserMapping {
        umid: SysCacheGetAttrNotNull(USERMAPPINGUSERSERVER, &tp, Anum_pg_user_mapping_oid)?.as_oid(),
        userid,
        serverid,
        options: untransform_options(
            mcx,
            attr_option_datum(USERMAPPINGUSERSERVER, &tp, Anum_pg_user_mapping_umoptions)?,
        )?,
    };
    ReleaseSysCache(tp);
    Ok(um)
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

/// GetFdwRoutine (foreign.c) — call the handler function to get its routine.
pub fn GetFdwRoutine(fdwhandler: Oid) -> PgResult<FdwKind> {
    if guc_tables::backing::restrict_nonsystem_relation_kind()
        & guc_tables::consts::RESTRICT_RELKIND_FOREIGN_TABLE
        != 0
    {
        return Err(::elog::ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("access to non-system foreign table is restricted")
            .into_error()
            .into());
    }
    let datum = fmgr_core::oid_function_call0_coll(fdwhandler, InvalidOid)?;
    let p = datum.as_usize() as *const FdwRoutine;
    // SAFETY: handler functions are in-tree dfmgr-registered builtins that
    // return a pointer datum to a `static FdwRoutine`; the tag read mirrors
    // C's `IsA(routine, FdwRoutine)` check on the same trust boundary.
    if p.is_null() || unsafe { (*p).tag } != NodeTag::T_FdwRoutine {
        panic!("foreign-data wrapper handler function {fdwhandler} did not return an FdwRoutine struct");
    }
    Ok(unsafe { (*p).kind })
}

/// GetFdwRoutineByServerId (foreign.c): the no-handler error surface plus the
/// handler invocation.
pub fn GetFdwRoutineByServerId<'mcx>(mcx: Mcx<'mcx>, serverid: Oid) -> PgResult<FdwKind> {
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
    GetFdwRoutine(fdw.fdwhandler)
}

/// C caches the routine in the relcache (rd_fdwroutine); the id-sized routine
/// makes the cache two syscache probes' worth per scan init — accepted, FDW
/// scans are cold in the milestone workloads.
pub fn GetFdwRoutineByRelId<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<FdwKind> {
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
