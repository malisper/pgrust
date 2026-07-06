// amcmds.c: CreateAccessMethod + get_am_*_oid lookup family.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use cache_syscache::{ReleaseSysCache, SearchSysCache1, SysCacheGetAttrNotNull, SysCacheKey};
use datum::Datum;
use mcx::Mcx;
use types_core::{InvalidOid, Oid, OidIsValid, INTERNALOID};
use types_error::{PgError, PgResult, ERROR};
use types_tuple::NameData;

pub const AMTYPE_INDEX: u8 = b'i';
pub const AMTYPE_TABLE: u8 = b't';

pub const AccessMethodRelationId: Oid = 2601;
const AmOidIndexId: Oid = 2652;
const Natts_pg_am: usize = 4;
const Anum_pg_am_oid: i32 = 1;

const PROCEDURE_RELATION_ID: Oid = 1255;
const INDEX_AM_HANDLEROID: Oid = 325;
const TABLE_AM_HANDLEROID: Oid = 269;

const Anum_pg_am_amtype: i32 = 4;

pub use pg_depend::ObjectAddress;

fn get_am_type_string(amtype: u8) -> &'static str {
    match amtype {
        AMTYPE_INDEX => "INDEX",
        AMTYPE_TABLE => "TABLE",
        _ => unreachable!("invalid access method type '{}'", amtype as char),
    }
}

pub fn CreateAccessMethod<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &types_nodes::parsenodes::CreateAmStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let amname = stmt.amname.expect("access method name");
    let rel = table::table_open(mcx, AccessMethodRelationId, types_rel::RowExclusiveLock)?;

    if !superuser::superuser()? {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("permission denied to create access method \"{amname}\""),
            )
            .with_sqlstate(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE)
            .with_hint("Must be superuser to create an access method."),
        ));
    }

    if OidIsValid(get_am_type_oid(amname, 0, true)?) {
        return Err(Box::new(
            PgError::new(ERROR, format!("access method \"{amname}\" already exists"))
                .with_sqlstate(types_error::ERRCODE_DUPLICATE_OBJECT),
        ));
    }

    let amhandler = lookup_am_handler_func(mcx, &stmt.handler_name, stmt.amtype)?;

    let amoid = catalog::GetNewOidWithIndex(
        mcx,
        &rel,
        AmOidIndexId,
        Anum_pg_am_oid as types_core::AttrNumber,
    )?;
    let mut name = NameData::default();
    name.namestrcpy(amname);
    let values: [Datum; Natts_pg_am] = [
        Datum::from_oid(amoid),
        Datum::from_usize(name.data.as_ptr() as usize),
        Datum::from_oid(amhandler),
        Datum::from_char(stmt.amtype as i8),
    ];
    let nulls = [false; Natts_pg_am];
    let mut tup = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tup)?;

    let myself = ObjectAddress::set(AccessMethodRelationId, amoid);
    let referenced = ObjectAddress::set(PROCEDURE_RELATION_ID, amhandler);
    pg_depend::recordDependencyOn(mcx, &myself, &referenced, pg_depend::DependencyType::Normal)?;
    pg_depend::recordDependencyOnCurrentExtension(mcx, &myself, false)?;

    // InvokeObjectPostCreateHook: object-access hooks are elided repo-wide.

    rel.close(types_rel::RowExclusiveLock)?;

    Ok(myself)
}

pub fn get_am_type_oid(amname: &str, amtype: u8, missing_ok: bool) -> PgResult<Oid> {
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
                .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
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

pub fn get_am_oid(amname: &str, missing_ok: bool) -> PgResult<Oid> {
    get_am_type_oid(amname, 0, missing_ok)
}

fn lookup_am_handler_func(
    mcx: Mcx<'_>,
    handler_name: &types_nodes::NodeList<'_>,
    amtype: u8,
) -> PgResult<Oid> {
    if handler_name.is_nil() {
        return Err(Box::new(
            PgError::new(ERROR, "handler function is not specified".to_string())
                .with_sqlstate(types_error::ERRCODE_UNDEFINED_FUNCTION),
        ));
    }

    let funcargtypes = [INTERNALOID];
    let handlerOid = parse_func::LookupFuncName(handler_name, 1, &funcargtypes, false)?;

    let expectedType = match amtype {
        AMTYPE_INDEX => INDEX_AM_HANDLEROID,
        AMTYPE_TABLE => TABLE_AM_HANDLEROID,
        _ => panic!("unrecognized access method type \"{}\"", amtype as char),
    };

    if lsyscache::get_func_rettype(handlerOid)? != expectedType {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "function {} must return type {}",
                    lsyscache::get_func_name(mcx, handlerOid)?
                        .map(|s| s.as_str().to_string())
                        .unwrap_or_default(),
                    format_type::format_type_be(expectedType)?
                ),
            )
            .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }

    Ok(handlerOid)
}

pub fn init_seams() {
    tableam_seams::get_table_am_oid::set(get_table_am_oid);
}
