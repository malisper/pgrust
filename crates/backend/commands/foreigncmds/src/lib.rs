//! foreigncmds.c — FOREIGN DATA WRAPPER / SERVER / USER MAPPING / FOREIGN
//! TABLE DDL. Hosts the foreign.c lookup slice and postgresql_fdw_validator
//! (backend-foreign-foreign is scoped non-core).
#![allow(non_snake_case, non_upper_case_globals)]

pub mod foreign;
pub mod options;

use datum::Datum;
use mcx::{Mcx, PgVec};
use pg_depend::{DependencyType, ObjectAddress};
use types_core::{
    InvalidOid, Oid, FOREIGN_DATA_WRAPPER_OID_INDEX_ID, FOREIGN_DATA_WRAPPER_RELATION_ID,
    FOREIGN_SERVER_OID_INDEX_ID, FOREIGN_SERVER_RELATION_ID, FOREIGN_TABLE_RELATION_ID,
    PROCEDURE_RELATION_ID, RELATION_RELATION_ID, USER_MAPPING_OID_INDEX_ID,
    USER_MAPPING_RELATION_ID,
};
use types_error::{
    PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_SYNTAX_ERROR,
    ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, ERROR, NOTICE, WARNING,
};
use types_nodes::list::NodeList;
use types_nodes::parsenodes::{DefElem, DropBehavior, ObjectType, RoleSpec, RoleSpecType};
use types_nodes::rawnodes::{
    AlterFdwStmt, AlterForeignServerStmt, AlterUserMappingStmt, CreateFdwStmt,
    CreateForeignServerStmt, CreateForeignTableStmt, CreateUserMappingStmt, DropUserMappingStmt,
    ImportForeignSchemaStmt,
};
use types_rel::RowExclusiveLock;

use cache_syscache::cacheinfo::{FOREIGNDATAWRAPPERNAME, FOREIGNSERVERNAME, FOREIGNSERVEROID, USERMAPPINGUSERSERVER};
use cache_syscache::{ReleaseSysCache, SearchSysCacheCopy, SysCacheGetAttrNotNull, SysCacheKey};

use crate::foreign::*;

const FDW_HANDLEROID: Oid = 3115;
const TEXTARRAYOID: Oid = 1009;
const OIDOID: Oid = 26;
const ACL_ID_PUBLIC: Oid = 0;

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: foreigncmds {what}")
}

fn err(
    sqlstate: types_error::SqlState,
    msg: String,
    hint: Option<&str>,
) -> Box<types_error::PgError> {
    let mut e = ::elog::ereport(ERROR).errcode(sqlstate).errmsg(msg);
    if let Some(h) = hint {
        e = e.errhint(h.to_string());
    }
    e.into_error().into()
}

/// object_ownercheck (objectaddress.c) over the srvowner/fdwowner column.
fn owner_check(owner: Oid, roleid: Oid) -> PgResult<bool> {
    if superuser::superuser_arg(roleid)? {
        return Ok(true);
    }
    adt_acl::has_privs_of_role(roleid, owner)
}

fn rolespec_oid(role: Option<&RoleSpec<'_>>, missing_ok: bool) -> PgResult<Oid> {
    let role = role.expect("user mapping RoleSpec");
    if role.roletype == RoleSpecType::ROLESPEC_PUBLIC {
        return Ok(ACL_ID_PUBLIC);
    }
    aclchk::get_rolespec_oid(role, missing_ok)
}

fn cstring_text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum> {
    let total = 4 + s.len();
    let mut buf: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, total)?;
    mcx::vec_append_bytes(&mut buf, &datum::varlena::set_varsize_4b(total))?;
    mcx::vec_append_bytes(&mut buf, s.as_bytes())?;
    // Leaked: the datum must stay live through heap_form_tuple.
    Ok(Datum::from_usize(buf.leak().as_ptr() as usize))
}

fn name_datum(name: &str, buf: &mut types_tuple::NameData) -> Datum {
    buf.namestrcpy(name);
    Datum::from_usize(buf.data.as_ptr() as usize)
}

fn image_datum(image: &[u8]) -> Datum {
    Datum::from_usize(image.as_ptr() as usize)
}

fn lookup_fdw_handler_func<'mcx>(mcx: Mcx<'mcx>, handler: &DefElem<'mcx>) -> PgResult<Oid> {
    let Some(arg) = handler.arg else {
        return Ok(InvalidOid);
    };
    let names = arg.as_list().expect("handler_name is a name list");
    let handler_oid = parse_func::LookupFuncName(names, 0, &[], false)?;
    if lsyscache::function::get_func_rettype(handler_oid)? != FDW_HANDLEROID {
        return Err(err(
            ERRCODE_WRONG_OBJECT_TYPE,
            format!(
                "function {} must return type {}",
                commands_define::NameListToString(mcx, names)?.as_str(),
                "fdw_handler"
            ),
            None,
        ));
    }
    Ok(handler_oid)
}

fn lookup_fdw_validator_func(validator: &DefElem<'_>) -> PgResult<Oid> {
    let Some(arg) = validator.arg else {
        return Ok(InvalidOid);
    };
    let names = arg.as_list().expect("handler_name is a name list");
    parse_func::LookupFuncName(names, 2, &[TEXTARRAYOID, OIDOID], false)
}

struct FuncOptions {
    handler_given: bool,
    fdwhandler: Oid,
    validator_given: bool,
    fdwvalidator: Oid,
}

fn parse_func_options<'mcx>(
    mcx: Mcx<'mcx>,
    func_options: &NodeList<'mcx>,
    source_text: &str,
) -> PgResult<FuncOptions> {
    let mut out = FuncOptions {
        handler_given: false,
        fdwhandler: InvalidOid,
        validator_given: false,
        fdwvalidator: InvalidOid,
    };
    for cell in func_options.iter() {
        let def = cell.as_variant::<DefElem>().expect("DefElem");
        match def.defname {
            Some("handler") => {
                if out.handler_given {
                    return Err(conflicting_option(source_text, def.location));
                }
                out.handler_given = true;
                out.fdwhandler = lookup_fdw_handler_func(mcx, def)?;
            }
            Some("validator") => {
                if out.validator_given {
                    return Err(conflicting_option(source_text, def.location));
                }
                out.validator_given = true;
                out.fdwvalidator = lookup_fdw_validator_func(def)?;
            }
            other => panic!("option \"{}\" not recognized", other.unwrap_or("")),
        }
    }
    Ok(out)
}

// errorConflictingDefElem (define.c).
#[cold]
#[inline(never)]
fn conflicting_option(src: &str, location: types_core::ParseLoc) -> Box<types_error::PgError> {
    let pos = parser_small1::parser_errposition_source(
        Some(src.as_bytes()),
        location,
        mbutils::GetDatabaseEncoding(),
    );
    Box::new(
        types_error::PgError::error("conflicting or redundant options".to_string())
            .with_sqlstate(ERRCODE_SYNTAX_ERROR)
            .with_cursor_position(pos),
    )
}

pub fn CreateForeignDataWrapper<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateFdwStmt<'mcx>,
    source_text: &str,
) -> PgResult<Oid> {
    let fdwname = stmt.fdwname.expect("CreateFdwStmt.fdwname");
    let rel = table::table_open(mcx, FOREIGN_DATA_WRAPPER_RELATION_ID, RowExclusiveLock)?;

    if !superuser::superuser()? {
        return Err(err(
            ERRCODE_INSUFFICIENT_PRIVILEGE,
            format!("permission denied to create foreign-data wrapper \"{fdwname}\""),
            Some("Must be superuser to create a foreign-data wrapper."),
        ));
    }
    let owner_id = miscinit::GetUserId();

    if get_foreign_data_wrapper_oid(fdwname, true)? != InvalidOid {
        return Err(err(
            ERRCODE_DUPLICATE_OBJECT,
            format!("foreign-data wrapper \"{fdwname}\" already exists"),
            None,
        ));
    }

    let fdw_id = catalog::GetNewOidWithIndex(
        mcx,
        &rel,
        FOREIGN_DATA_WRAPPER_OID_INDEX_ID,
        Anum_pg_foreign_data_wrapper_oid as i16,
    )?;

    let func = parse_func_options(mcx, &stmt.func_options, source_text)?;

    let fdwoptions = options::transformGenericOptions(
        mcx,
        FOREIGN_DATA_WRAPPER_RELATION_ID,
        None,
        &stmt.options,
        func.fdwvalidator,
    )?;

    let mut namebuf = types_tuple::NameData { data: [0; 64] };
    let mut values = [Datum::null(); Natts_pg_foreign_data_wrapper];
    let mut nulls = [false; Natts_pg_foreign_data_wrapper];
    values[Anum_pg_foreign_data_wrapper_oid as usize - 1] = Datum::from_oid(fdw_id);
    values[Anum_pg_foreign_data_wrapper_fdwname as usize - 1] = name_datum(fdwname, &mut namebuf);
    values[Anum_pg_foreign_data_wrapper_fdwowner as usize - 1] = Datum::from_oid(owner_id);
    values[Anum_pg_foreign_data_wrapper_fdwhandler as usize - 1] = Datum::from_oid(func.fdwhandler);
    values[Anum_pg_foreign_data_wrapper_fdwvalidator as usize - 1] =
        Datum::from_oid(func.fdwvalidator);
    nulls[Anum_pg_foreign_data_wrapper_fdwacl as usize - 1] = true;
    match &fdwoptions {
        Some(image) => {
            values[Anum_pg_foreign_data_wrapper_fdwoptions as usize - 1] = image_datum(image)
        }
        None => nulls[Anum_pg_foreign_data_wrapper_fdwoptions as usize - 1] = true,
    }

    let mut tuple = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tuple)?;

    let myself = ObjectAddress::set(FOREIGN_DATA_WRAPPER_RELATION_ID, fdw_id);
    if func.fdwhandler != InvalidOid {
        let referenced = ObjectAddress::set(PROCEDURE_RELATION_ID, func.fdwhandler);
        pg_depend::recordDependencyOn(mcx, &myself, &referenced, DependencyType::Normal)?;
    }
    if func.fdwvalidator != InvalidOid {
        let referenced = ObjectAddress::set(PROCEDURE_RELATION_ID, func.fdwvalidator);
        pg_depend::recordDependencyOn(mcx, &myself, &referenced, DependencyType::Normal)?;
    }
    pg_depend::recordDependencyOnOwner(mcx, FOREIGN_DATA_WRAPPER_RELATION_ID, fdw_id, owner_id)?;
    pg_depend::recordDependencyOnCurrentExtension(mcx, &myself, false)?;

    rel.close(RowExclusiveLock)?;
    Ok(fdw_id)
}

pub fn AlterForeignDataWrapper<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterFdwStmt<'mcx>,
    source_text: &str,
) -> PgResult<()> {
    let fdwname = stmt.fdwname.expect("AlterFdwStmt.fdwname");
    let rel = table::table_open(mcx, FOREIGN_DATA_WRAPPER_RELATION_ID, RowExclusiveLock)?;

    if !superuser::superuser()? {
        return Err(err(
            ERRCODE_INSUFFICIENT_PRIVILEGE,
            format!("permission denied to alter foreign-data wrapper \"{fdwname}\""),
            Some("Must be superuser to alter a foreign-data wrapper."),
        ));
    }

    let Some(tp) = SearchSysCacheCopy(
        mcx,
        FOREIGNDATAWRAPPERNAME,
        SysCacheKey::Str(fdwname),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )?
    else {
        return Err(err(
            ERRCODE_UNDEFINED_OBJECT,
            format!("foreign-data wrapper \"{fdwname}\" does not exist"),
            None,
        ));
    };
    let getattr = |attnum: i32| -> (Datum, bool) {
        let mut isnull = false;
        // SAFETY: pg_foreign_data_wrapper column of the copied tuple.
        let d = unsafe { types_tuple::heap_getattr(&tp, attnum, rel.descr(), &mut isnull) };
        (d, isnull)
    };
    let fdw_id = getattr(Anum_pg_foreign_data_wrapper_oid).0.as_oid();

    let mut repl_val = [Datum::null(); Natts_pg_foreign_data_wrapper];
    let mut repl_null = [false; Natts_pg_foreign_data_wrapper];
    let mut repl_repl = [false; Natts_pg_foreign_data_wrapper];

    let func = parse_func_options(mcx, &stmt.func_options, source_text)?;
    let mut fdwvalidator = func.fdwvalidator;

    if func.handler_given {
        repl_val[Anum_pg_foreign_data_wrapper_fdwhandler as usize - 1] =
            Datum::from_oid(func.fdwhandler);
        repl_repl[Anum_pg_foreign_data_wrapper_fdwhandler as usize - 1] = true;
        elog_seams::ereport_msg::call(
            WARNING,
            "changing the foreign-data wrapper handler can change behavior of existing foreign tables"
                .to_string(),
            None,
        )?;
    }

    if func.validator_given {
        repl_val[Anum_pg_foreign_data_wrapper_fdwvalidator as usize - 1] =
            Datum::from_oid(func.fdwvalidator);
        repl_repl[Anum_pg_foreign_data_wrapper_fdwvalidator as usize - 1] = true;
        if func.fdwvalidator != InvalidOid {
            elog_seams::ereport_msg::call(
                WARNING,
                "changing the foreign-data wrapper validator can cause the options for dependent objects to become invalid"
                    .to_string(),
                None,
            )?;
        }
    } else {
        fdwvalidator = getattr(Anum_pg_foreign_data_wrapper_fdwvalidator).0.as_oid();
    }

    let new_options;
    if !stmt.options.is_nil() {
        let (datum, isnull) = getattr(Anum_pg_foreign_data_wrapper_fdwoptions);
        let old = if isnull { None } else { Some(datum) };
        new_options = options::transformGenericOptions(
            mcx,
            FOREIGN_DATA_WRAPPER_RELATION_ID,
            old,
            &stmt.options,
            fdwvalidator,
        )?;
        match &new_options {
            Some(image) => {
                repl_val[Anum_pg_foreign_data_wrapper_fdwoptions as usize - 1] = image_datum(image)
            }
            None => repl_null[Anum_pg_foreign_data_wrapper_fdwoptions as usize - 1] = true,
        }
        repl_repl[Anum_pg_foreign_data_wrapper_fdwoptions as usize - 1] = true;
    }

    let mut newtup =
        heaptuple::heap_modify_tuple(mcx, &tp, rel.descr(), &repl_val, &repl_null, &repl_repl)?;
    let otid = tp.t_self;
    catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut newtup)?;

    if func.handler_given || func.validator_given {
        let myself = ObjectAddress::set(FOREIGN_DATA_WRAPPER_RELATION_ID, fdw_id);
        pg_depend::deleteDependencyRecordsForClass(
            mcx,
            FOREIGN_DATA_WRAPPER_RELATION_ID,
            fdw_id,
            PROCEDURE_RELATION_ID,
            DependencyType::Normal,
        )?;
        if func.fdwhandler != InvalidOid {
            let referenced = ObjectAddress::set(PROCEDURE_RELATION_ID, func.fdwhandler);
            pg_depend::recordDependencyOn(mcx, &myself, &referenced, DependencyType::Normal)?;
        }
        if func.fdwvalidator != InvalidOid {
            let referenced = ObjectAddress::set(PROCEDURE_RELATION_ID, func.fdwvalidator);
            pg_depend::recordDependencyOn(mcx, &myself, &referenced, DependencyType::Normal)?;
        }
    }

    rel.close(RowExclusiveLock)
}

// AlterForeignDataWrapperOwner + _oid + _internal (foreigncmds.c).
pub fn AlterForeignDataWrapperOwner<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    new_owner_id: Oid,
) -> PgResult<ObjectAddress> {
    let rel = table::table_open(mcx, FOREIGN_DATA_WRAPPER_RELATION_ID, RowExclusiveLock)?;

    let Some(tp) = SearchSysCacheCopy(
        mcx,
        FOREIGNDATAWRAPPERNAME,
        SysCacheKey::Str(name),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )?
    else {
        return Err(err(
            ERRCODE_UNDEFINED_OBJECT,
            format!("foreign-data wrapper \"{name}\" does not exist"),
            None,
        ));
    };
    let fdw_id = AlterForeignDataWrapperOwner_internal(mcx, &rel, &tp, new_owner_id)?;

    rel.close(RowExclusiveLock)?;
    Ok(ObjectAddress::set(FOREIGN_DATA_WRAPPER_RELATION_ID, fdw_id))
}

pub fn AlterForeignDataWrapperOwner_oid<'mcx>(
    mcx: Mcx<'mcx>,
    fdw_id: Oid,
    new_owner_id: Oid,
) -> PgResult<()> {
    let rel = table::table_open(mcx, FOREIGN_DATA_WRAPPER_RELATION_ID, RowExclusiveLock)?;

    let Some(tp) = SearchSysCacheCopy(
        mcx,
        cache_syscache::cacheinfo::FOREIGNDATAWRAPPEROID,
        SysCacheKey::Value(Datum::from_oid(fdw_id)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )?
    else {
        return Err(err(
            ERRCODE_UNDEFINED_OBJECT,
            format!("foreign-data wrapper with OID {fdw_id} does not exist"),
            None,
        ));
    };
    AlterForeignDataWrapperOwner_internal(mcx, &rel, &tp, new_owner_id)?;

    rel.close(RowExclusiveLock)
}

fn AlterForeignDataWrapperOwner_internal<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'mcx>,
    tp: &heaptuple::HeapTuple<'mcx>,
    new_owner_id: Oid,
) -> PgResult<Oid> {
    let getattr = |attnum: i32| -> (Datum, bool) {
        let mut isnull = false;
        // SAFETY: pg_foreign_data_wrapper column of the copied tuple.
        let d = unsafe { types_tuple::heap_getattr(tp, attnum, rel.descr(), &mut isnull) };
        (d, isnull)
    };
    let fdw_id = getattr(Anum_pg_foreign_data_wrapper_oid).0.as_oid();
    let old_owner = getattr(Anum_pg_foreign_data_wrapper_fdwowner).0.as_oid();
    let name = name_attr(mcx, getattr(Anum_pg_foreign_data_wrapper_fdwname).0)?;

    if !superuser::superuser()? {
        return Err(err(
            ERRCODE_INSUFFICIENT_PRIVILEGE,
            format!("permission denied to change owner of foreign-data wrapper \"{name}\""),
            Some("Must be superuser to change owner of a foreign-data wrapper."),
        ));
    }
    if !superuser::superuser_arg(new_owner_id)? {
        return Err(err(
            ERRCODE_INSUFFICIENT_PRIVILEGE,
            format!("permission denied to change owner of foreign-data wrapper \"{name}\""),
            Some("The owner of a foreign-data wrapper must be a superuser."),
        ));
    }

    if old_owner != new_owner_id {
        let mut repl_val = [Datum::null(); Natts_pg_foreign_data_wrapper];
        let mut repl_null = [false; Natts_pg_foreign_data_wrapper];
        let mut repl_repl = [false; Natts_pg_foreign_data_wrapper];
        repl_val[Anum_pg_foreign_data_wrapper_fdwowner as usize - 1] =
            Datum::from_oid(new_owner_id);
        repl_repl[Anum_pg_foreign_data_wrapper_fdwowner as usize - 1] = true;

        let (acl_datum, isnull) = getattr(Anum_pg_foreign_data_wrapper_fdwacl);
        let acl_img;
        if !isnull {
            let new_acl = aclchk::with_acl_datum(acl_datum, |acl| {
                adt_acl::aclnewowner(mcx, acl, old_owner, new_owner_id)
            })?;
            acl_img = adt_acl::varlena::acl_image(mcx, &new_acl)?;
            repl_val[Anum_pg_foreign_data_wrapper_fdwacl as usize - 1] = image_datum(&acl_img);
            repl_repl[Anum_pg_foreign_data_wrapper_fdwacl as usize - 1] = true;
        }

        let mut newtup =
            heaptuple::heap_modify_tuple(mcx, tp, rel.descr(), &repl_val, &repl_null, &repl_repl)?;
        let otid = tp.t_self;
        catalog_indexing::CatalogTupleUpdate(mcx, rel, &otid, &mut newtup)?;

        pg_shdepend::changeDependencyOnOwner(
            mcx,
            FOREIGN_DATA_WRAPPER_RELATION_ID,
            fdw_id,
            new_owner_id,
        )?;
    }

    Ok(fdw_id)
}

// AlterForeignServerOwner + _oid + _internal (foreigncmds.c).
pub fn AlterForeignServerOwner<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    new_owner_id: Oid,
) -> PgResult<ObjectAddress> {
    let rel = table::table_open(mcx, FOREIGN_SERVER_RELATION_ID, RowExclusiveLock)?;

    let Some(tp) = SearchSysCacheCopy(
        mcx,
        FOREIGNSERVERNAME,
        SysCacheKey::Str(name),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )?
    else {
        return Err(err(
            ERRCODE_UNDEFINED_OBJECT,
            format!("server \"{name}\" does not exist"),
            None,
        ));
    };
    let srv_id = AlterForeignServerOwner_internal(mcx, &rel, &tp, new_owner_id)?;

    rel.close(RowExclusiveLock)?;
    Ok(ObjectAddress::set(FOREIGN_SERVER_RELATION_ID, srv_id))
}

pub fn AlterForeignServerOwner_oid<'mcx>(
    mcx: Mcx<'mcx>,
    srv_id: Oid,
    new_owner_id: Oid,
) -> PgResult<()> {
    let rel = table::table_open(mcx, FOREIGN_SERVER_RELATION_ID, RowExclusiveLock)?;

    let Some(tp) = SearchSysCacheCopy(
        mcx,
        FOREIGNSERVEROID,
        SysCacheKey::Value(Datum::from_oid(srv_id)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )?
    else {
        return Err(err(
            ERRCODE_UNDEFINED_OBJECT,
            format!("foreign server with OID {srv_id} does not exist"),
            None,
        ));
    };
    AlterForeignServerOwner_internal(mcx, &rel, &tp, new_owner_id)?;

    rel.close(RowExclusiveLock)
}

fn AlterForeignServerOwner_internal<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'mcx>,
    tp: &heaptuple::HeapTuple<'mcx>,
    new_owner_id: Oid,
) -> PgResult<Oid> {
    let getattr = |attnum: i32| -> (Datum, bool) {
        let mut isnull = false;
        // SAFETY: pg_foreign_server column of the copied tuple.
        let d = unsafe { types_tuple::heap_getattr(tp, attnum, rel.descr(), &mut isnull) };
        (d, isnull)
    };
    let srv_id = getattr(Anum_pg_foreign_server_oid).0.as_oid();
    let old_owner = getattr(Anum_pg_foreign_server_srvowner).0.as_oid();
    let srv_fdw = getattr(Anum_pg_foreign_server_srvfdw).0.as_oid();
    let name = name_attr(mcx, getattr(Anum_pg_foreign_server_srvname).0)?;

    if old_owner != new_owner_id {
        if !superuser::superuser()? {
            if !aclchk::object_ownercheck(
                FOREIGN_SERVER_RELATION_ID,
                srv_id,
                miscinit::GetUserId(),
            )? {
                aclchk::aclcheck_error(
                    aclchk::ACLCHECK_NOT_OWNER,
                    ObjectType::OBJECT_FOREIGN_SERVER,
                    name,
                )?;
            }
            // check_can_set_role (acl.c).
            if !adt_acl::member_can_set_role(miscinit::GetUserId(), new_owner_id)? {
                let rolename = miscinit::GetUserNameFromId(mcx, new_owner_id, false)?
                    .map(|n| n.to_string())
                    .unwrap_or_default();
                return Err(err(
                    ERRCODE_INSUFFICIENT_PRIVILEGE,
                    format!("must be able to SET ROLE \"{rolename}\""),
                    None,
                ));
            }
            let aclresult = aclchk::object_aclcheck(
                FOREIGN_DATA_WRAPPER_RELATION_ID,
                srv_fdw,
                new_owner_id,
                adt_acl::ACL_USAGE,
            )?;
            if aclresult != aclchk::ACLCHECK_OK {
                let fdw = GetForeignDataWrapper(mcx, srv_fdw)?;
                aclchk::aclcheck_error(aclresult, ObjectType::OBJECT_FDW, fdw.fdwname)?;
            }
        }

        let mut repl_val = [Datum::null(); Natts_pg_foreign_server];
        let mut repl_null = [false; Natts_pg_foreign_server];
        let mut repl_repl = [false; Natts_pg_foreign_server];
        repl_val[Anum_pg_foreign_server_srvowner as usize - 1] = Datum::from_oid(new_owner_id);
        repl_repl[Anum_pg_foreign_server_srvowner as usize - 1] = true;

        let (acl_datum, isnull) = getattr(Anum_pg_foreign_server_srvacl);
        let acl_img;
        if !isnull {
            let new_acl = aclchk::with_acl_datum(acl_datum, |acl| {
                adt_acl::aclnewowner(mcx, acl, old_owner, new_owner_id)
            })?;
            acl_img = adt_acl::varlena::acl_image(mcx, &new_acl)?;
            repl_val[Anum_pg_foreign_server_srvacl as usize - 1] = image_datum(&acl_img);
            repl_repl[Anum_pg_foreign_server_srvacl as usize - 1] = true;
        }

        let mut newtup =
            heaptuple::heap_modify_tuple(mcx, tp, rel.descr(), &repl_val, &repl_null, &repl_repl)?;
        let otid = tp.t_self;
        catalog_indexing::CatalogTupleUpdate(mcx, rel, &otid, &mut newtup)?;

        pg_shdepend::changeDependencyOnOwner(mcx, FOREIGN_SERVER_RELATION_ID, srv_id, new_owner_id)?;
    }

    Ok(srv_id)
}

pub fn CreateForeignServer<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateForeignServerStmt<'mcx>,
) -> PgResult<Oid> {
    let servername = stmt.servername.expect("CreateForeignServerStmt.servername");
    let fdwname = stmt.fdwname.expect("CreateForeignServerStmt.fdwname");
    let rel = table::table_open(mcx, FOREIGN_SERVER_RELATION_ID, RowExclusiveLock)?;

    let owner_id = miscinit::GetUserId();

    if get_foreign_server_oid(servername, true)? != InvalidOid {
        if stmt.if_not_exists {
            ::elog::ereport(NOTICE)
                .errcode(ERRCODE_DUPLICATE_OBJECT)
                .errmsg(format!("server \"{servername}\" already exists, skipping"))
                .finish(types_error::ErrorLocation::new(file!(), line!() as i32, "CreateForeignServer"))?;
            rel.close(RowExclusiveLock)?;
            return Ok(InvalidOid);
        }
        return Err(err(
            ERRCODE_DUPLICATE_OBJECT,
            format!("server \"{servername}\" already exists"),
            None,
        ));
    }

    let fdw = GetForeignDataWrapperByName(mcx, fdwname, false)?.expect("missing_ok=false");
    if aclchk::object_aclcheck(
        FOREIGN_DATA_WRAPPER_RELATION_ID,
        fdw.fdwid,
        owner_id,
        adt_acl::ACL_USAGE,
    )? != aclchk::ACLCHECK_OK
    {
        aclchk::aclcheck_error(aclchk::ACLCHECK_NO_PRIV, ObjectType::OBJECT_FDW, fdw.fdwname)?;
    }

    let srv_id = catalog::GetNewOidWithIndex(
        mcx,
        &rel,
        FOREIGN_SERVER_OID_INDEX_ID,
        Anum_pg_foreign_server_oid as i16,
    )?;

    let srvoptions = options::transformGenericOptions(
        mcx,
        FOREIGN_SERVER_RELATION_ID,
        None,
        &stmt.options,
        fdw.fdwvalidator,
    )?;

    let mut namebuf = types_tuple::NameData { data: [0; 64] };
    let mut values = [Datum::null(); Natts_pg_foreign_server];
    let mut nulls = [false; Natts_pg_foreign_server];
    values[Anum_pg_foreign_server_oid as usize - 1] = Datum::from_oid(srv_id);
    values[Anum_pg_foreign_server_srvname as usize - 1] = name_datum(servername, &mut namebuf);
    values[Anum_pg_foreign_server_srvowner as usize - 1] = Datum::from_oid(owner_id);
    values[Anum_pg_foreign_server_srvfdw as usize - 1] = Datum::from_oid(fdw.fdwid);
    match stmt.servertype {
        Some(t) => {
            values[Anum_pg_foreign_server_srvtype as usize - 1] = cstring_text_datum(mcx, t)?
        }
        None => nulls[Anum_pg_foreign_server_srvtype as usize - 1] = true,
    }
    match stmt.version {
        Some(v) => {
            values[Anum_pg_foreign_server_srvversion as usize - 1] = cstring_text_datum(mcx, v)?
        }
        None => nulls[Anum_pg_foreign_server_srvversion as usize - 1] = true,
    }
    nulls[Anum_pg_foreign_server_srvacl as usize - 1] = true;
    match &srvoptions {
        Some(image) => values[Anum_pg_foreign_server_srvoptions as usize - 1] = image_datum(image),
        None => nulls[Anum_pg_foreign_server_srvoptions as usize - 1] = true,
    }

    let mut tuple = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tuple)?;

    let myself = ObjectAddress::set(FOREIGN_SERVER_RELATION_ID, srv_id);
    let referenced = ObjectAddress::set(FOREIGN_DATA_WRAPPER_RELATION_ID, fdw.fdwid);
    pg_depend::recordDependencyOn(mcx, &myself, &referenced, DependencyType::Normal)?;
    pg_depend::recordDependencyOnOwner(mcx, FOREIGN_SERVER_RELATION_ID, srv_id, owner_id)?;

    rel.close(RowExclusiveLock)?;
    Ok(srv_id)
}

pub fn AlterForeignServer<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterForeignServerStmt<'mcx>,
) -> PgResult<()> {
    let servername = stmt.servername.expect("AlterForeignServerStmt.servername");
    let rel = table::table_open(mcx, FOREIGN_SERVER_RELATION_ID, RowExclusiveLock)?;

    let Some(tp) = SearchSysCacheCopy(
        mcx,
        FOREIGNSERVERNAME,
        SysCacheKey::Str(servername),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )?
    else {
        return Err(err(
            ERRCODE_UNDEFINED_OBJECT,
            format!("server \"{servername}\" does not exist"),
            None,
        ));
    };
    let getattr = |attnum: i32| -> (Datum, bool) {
        let mut isnull = false;
        // SAFETY: pg_foreign_server column of the copied tuple.
        let d = unsafe { types_tuple::heap_getattr(&tp, attnum, rel.descr(), &mut isnull) };
        (d, isnull)
    };
    let srv_owner = getattr(Anum_pg_foreign_server_srvowner).0.as_oid();
    let srv_fdw = getattr(Anum_pg_foreign_server_srvfdw).0.as_oid();

    if !owner_check(srv_owner, miscinit::GetUserId())? {
        aclchk::aclcheck_error(
            aclchk::ACLCHECK_NOT_OWNER,
            ObjectType::OBJECT_FOREIGN_SERVER,
            servername,
        )?;
    }

    let mut repl_val = [Datum::null(); Natts_pg_foreign_server];
    let mut repl_null = [false; Natts_pg_foreign_server];
    let mut repl_repl = [false; Natts_pg_foreign_server];

    if stmt.has_version {
        match stmt.version {
            Some(v) => {
                repl_val[Anum_pg_foreign_server_srvversion as usize - 1] =
                    cstring_text_datum(mcx, v)?
            }
            None => repl_null[Anum_pg_foreign_server_srvversion as usize - 1] = true,
        }
        repl_repl[Anum_pg_foreign_server_srvversion as usize - 1] = true;
    }

    let new_options;
    if !stmt.options.is_nil() {
        let fdw = GetForeignDataWrapper(mcx, srv_fdw)?;
        let (datum, isnull) = getattr(Anum_pg_foreign_server_srvoptions);
        let old = if isnull { None } else { Some(datum) };
        new_options = options::transformGenericOptions(
            mcx,
            FOREIGN_SERVER_RELATION_ID,
            old,
            &stmt.options,
            fdw.fdwvalidator,
        )?;
        match &new_options {
            Some(image) => {
                repl_val[Anum_pg_foreign_server_srvoptions as usize - 1] = image_datum(image)
            }
            None => repl_null[Anum_pg_foreign_server_srvoptions as usize - 1] = true,
        }
        repl_repl[Anum_pg_foreign_server_srvoptions as usize - 1] = true;
    }

    let mut newtup =
        heaptuple::heap_modify_tuple(mcx, &tp, rel.descr(), &repl_val, &repl_null, &repl_repl)?;
    let otid = tp.t_self;
    catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut newtup)?;

    rel.close(RowExclusiveLock)
}

/// user_mapping_ddl_aclcheck (foreigncmds.c).
fn user_mapping_ddl_aclcheck(umuserid: Oid, serverid: Oid, servername: &str) -> PgResult<()> {
    let curuserid = miscinit::GetUserId();
    let owner = {
        let Some(tp) = cache_syscache::SearchSysCache1(
            FOREIGNSERVEROID,
            SysCacheKey::Value(Datum::from_oid(serverid)),
        )?
        else {
            panic!("cache lookup failed for foreign server {serverid}");
        };
        let owner =
            SysCacheGetAttrNotNull(FOREIGNSERVEROID, &tp, Anum_pg_foreign_server_srvowner)?
                .as_oid();
        ReleaseSysCache(tp);
        owner
    };
    if !owner_check(owner, curuserid)? {
        if umuserid == curuserid {
            if aclchk::object_aclcheck(
                FOREIGN_SERVER_RELATION_ID,
                serverid,
                curuserid,
                adt_acl::ACL_USAGE,
            )? != aclchk::ACLCHECK_OK
            {
                aclchk::aclcheck_error(
                    aclchk::ACLCHECK_NO_PRIV,
                    ObjectType::OBJECT_FOREIGN_SERVER,
                    servername,
                )?;
            }
        } else {
            aclchk::aclcheck_error(
                aclchk::ACLCHECK_NOT_OWNER,
                ObjectType::OBJECT_FOREIGN_SERVER,
                servername,
            )?;
        }
    }
    Ok(())
}

pub fn CreateUserMapping<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateUserMappingStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let servername = stmt.servername.expect("CreateUserMappingStmt.servername");
    let rel = table::table_open(mcx, USER_MAPPING_RELATION_ID, RowExclusiveLock)?;

    let use_id = rolespec_oid(stmt.user, false)?;
    let srv = GetForeignServerByName(mcx, servername, false)?.expect("missing_ok=false");
    user_mapping_ddl_aclcheck(use_id, srv.serverid, servername)?;

    if get_user_mapping_oid(use_id, srv.serverid)? != InvalidOid {
        if stmt.if_not_exists {
            ::elog::ereport(NOTICE)
                .errcode(ERRCODE_DUPLICATE_OBJECT)
                .errmsg(format!(
                    "user mapping for \"{}\" already exists for server \"{servername}\", skipping",
                    MappingUserName(mcx, use_id)?
                ))
                .finish(types_error::ErrorLocation::new(file!(), line!() as i32, "CreateUserMapping"))?;
            rel.close(RowExclusiveLock)?;
            // C: return InvalidObjectAddress on the IF NOT EXISTS skip.
            return Ok(ObjectAddress::set(InvalidOid, InvalidOid));
        }
        return Err(err(
            ERRCODE_DUPLICATE_OBJECT,
            format!(
                "user mapping for \"{}\" already exists for server \"{servername}\"",
                MappingUserName(mcx, use_id)?
            ),
            None,
        ));
    }

    let fdw = GetForeignDataWrapper(mcx, srv.fdwid)?;

    let um_id = catalog::GetNewOidWithIndex(
        mcx,
        &rel,
        USER_MAPPING_OID_INDEX_ID,
        Anum_pg_user_mapping_oid as i16,
    )?;

    let useoptions = options::transformGenericOptions(
        mcx,
        USER_MAPPING_RELATION_ID,
        None,
        &stmt.options,
        fdw.fdwvalidator,
    )?;

    let mut values = [Datum::null(); Natts_pg_user_mapping];
    let mut nulls = [false; Natts_pg_user_mapping];
    values[Anum_pg_user_mapping_oid as usize - 1] = Datum::from_oid(um_id);
    values[Anum_pg_user_mapping_umuser as usize - 1] = Datum::from_oid(use_id);
    values[Anum_pg_user_mapping_umserver as usize - 1] = Datum::from_oid(srv.serverid);
    match &useoptions {
        Some(image) => values[Anum_pg_user_mapping_umoptions as usize - 1] = image_datum(image),
        None => nulls[Anum_pg_user_mapping_umoptions as usize - 1] = true,
    }

    let mut tuple = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tuple)?;

    let myself = ObjectAddress::set(USER_MAPPING_RELATION_ID, um_id);
    let referenced = ObjectAddress::set(FOREIGN_SERVER_RELATION_ID, srv.serverid);
    pg_depend::recordDependencyOn(mcx, &myself, &referenced, DependencyType::Normal)?;
    if use_id != InvalidOid {
        pg_depend::recordDependencyOnOwner(mcx, USER_MAPPING_RELATION_ID, um_id, use_id)?;
    }
    // No recordDependencyOnCurrentExtension: user mappings are not extension
    // members (C comment, foreigncmds.c:1217).

    rel.close(RowExclusiveLock)?;
    Ok(myself)
}

pub fn AlterUserMapping<'mcx>(mcx: Mcx<'mcx>, stmt: &AlterUserMappingStmt<'mcx>) -> PgResult<()> {
    let servername = stmt.servername.expect("AlterUserMappingStmt.servername");
    let rel = table::table_open(mcx, USER_MAPPING_RELATION_ID, RowExclusiveLock)?;

    let use_id = rolespec_oid(stmt.user, false)?;
    let srv = GetForeignServerByName(mcx, servername, false)?.expect("missing_ok=false");

    let um_id = get_user_mapping_oid(use_id, srv.serverid)?;
    if um_id == InvalidOid {
        return Err(err(
            ERRCODE_UNDEFINED_OBJECT,
            format!(
                "user mapping for \"{}\" does not exist for server \"{servername}\"",
                MappingUserName(mcx, use_id)?
            ),
            None,
        ));
    }
    user_mapping_ddl_aclcheck(use_id, srv.serverid, servername)?;

    let Some(tp) = user_mapping_lookup(use_id, srv.serverid)? else {
        panic!("cache lookup failed for user mapping {um_id}");
    };

    let mut repl_val = [Datum::null(); Natts_pg_user_mapping];
    let mut repl_null = [false; Natts_pg_user_mapping];
    let mut repl_repl = [false; Natts_pg_user_mapping];

    let new_options;
    if !stmt.options.is_nil() {
        let fdw = GetForeignDataWrapper(mcx, srv.fdwid)?;
        let old = attr_option_datum(USERMAPPINGUSERSERVER, &tp, Anum_pg_user_mapping_umoptions)?;
        new_options = options::transformGenericOptions(
            mcx,
            USER_MAPPING_RELATION_ID,
            old,
            &stmt.options,
            fdw.fdwvalidator,
        )?;
        match &new_options {
            Some(image) => {
                repl_val[Anum_pg_user_mapping_umoptions as usize - 1] = image_datum(image)
            }
            None => repl_null[Anum_pg_user_mapping_umoptions as usize - 1] = true,
        }
        repl_repl[Anum_pg_user_mapping_umoptions as usize - 1] = true;
    }

    let tuple = tp.tuple();
    let mut newtup =
        heaptuple::heap_modify_tuple(mcx, &tuple, rel.descr(), &repl_val, &repl_null, &repl_repl)?;
    let otid = tuple.t_self;
    ReleaseSysCache(tp);
    catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut newtup)?;

    rel.close(RowExclusiveLock)
}

pub fn RemoveUserMapping<'mcx>(mcx: Mcx<'mcx>, stmt: &DropUserMappingStmt<'mcx>) -> PgResult<()> {
    let servername = stmt.servername.expect("DropUserMappingStmt.servername");
    let role = stmt.user.expect("user mapping RoleSpec");

    let use_id = if role.roletype == RoleSpecType::ROLESPEC_PUBLIC {
        ACL_ID_PUBLIC
    } else {
        let oid = aclchk::get_rolespec_oid(role, stmt.missing_ok)?;
        if oid == InvalidOid {
            elog_seams::ereport_msg::call(
                NOTICE,
                format!("role \"{}\" does not exist, skipping", role.rolename.unwrap_or("")),
                None,
            )?;
            return Ok(());
        }
        oid
    };

    let Some(srv) = GetForeignServerByName(mcx, servername, true)? else {
        if !stmt.missing_ok {
            return Err(err(
                ERRCODE_UNDEFINED_OBJECT,
                format!("server \"{servername}\" does not exist"),
                None,
            ));
        }
        elog_seams::ereport_msg::call(
            NOTICE,
            format!("server \"{servername}\" does not exist, skipping"),
            None,
        )?;
        return Ok(());
    };

    let um_id = get_user_mapping_oid(use_id, srv.serverid)?;
    if um_id == InvalidOid {
        if !stmt.missing_ok {
            return Err(err(
                ERRCODE_UNDEFINED_OBJECT,
                format!(
                    "user mapping for \"{}\" does not exist for server \"{servername}\"",
                    MappingUserName(mcx, use_id)?
                ),
                None,
            ));
        }
        elog_seams::ereport_msg::call(
            NOTICE,
            format!(
                "user mapping for \"{}\" does not exist for server \"{servername}\", skipping",
                MappingUserName(mcx, use_id)?
            ),
            None,
        )?;
        return Ok(());
    }

    user_mapping_ddl_aclcheck(use_id, srv.serverid, srv.servername)?;

    let object = ObjectAddress::set(USER_MAPPING_RELATION_ID, um_id);
    catalog_dependency::performDeletion(mcx, &object, DropBehavior::DROP_CASCADE, 0)
}

/// CreateForeignTable (foreigncmds.c); called after DefineRelation.
pub fn CreateForeignTable<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateForeignTableStmt<'mcx>,
    relid: Oid,
) -> PgResult<()> {
    let servername = stmt.servername.expect("CreateForeignTableStmt.servername");

    xact::CommandCounterIncrement()?;

    let ftrel = table::table_open(mcx, FOREIGN_TABLE_RELATION_ID, RowExclusiveLock)?;

    let owner_id = miscinit::GetUserId();

    let server = GetForeignServerByName(mcx, servername, false)?.expect("missing_ok=false");
    if aclchk::object_aclcheck(
        FOREIGN_SERVER_RELATION_ID,
        server.serverid,
        owner_id,
        adt_acl::ACL_USAGE,
    )? != aclchk::ACLCHECK_OK
    {
        aclchk::aclcheck_error(
            aclchk::ACLCHECK_NO_PRIV,
            ObjectType::OBJECT_FOREIGN_SERVER,
            server.servername,
        )?;
    }

    let fdw = GetForeignDataWrapper(mcx, server.fdwid)?;

    let ftoptions = options::transformGenericOptions(
        mcx,
        FOREIGN_TABLE_RELATION_ID,
        None,
        &stmt.options,
        fdw.fdwvalidator,
    )?;

    let mut values = [Datum::null(); Natts_pg_foreign_table];
    let mut nulls = [false; Natts_pg_foreign_table];
    values[Anum_pg_foreign_table_ftrelid as usize - 1] = Datum::from_oid(relid);
    values[Anum_pg_foreign_table_ftserver as usize - 1] = Datum::from_oid(server.serverid);
    match &ftoptions {
        Some(image) => values[Anum_pg_foreign_table_ftoptions as usize - 1] = image_datum(image),
        None => nulls[Anum_pg_foreign_table_ftoptions as usize - 1] = true,
    }

    let mut tuple = heaptuple::heap_form_tuple(mcx, ftrel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &ftrel, &mut tuple)?;

    let myself = ObjectAddress::set(RELATION_RELATION_ID, relid);
    let referenced = ObjectAddress::set(FOREIGN_SERVER_RELATION_ID, server.serverid);
    pg_depend::recordDependencyOn(mcx, &myself, &referenced, DependencyType::Normal)?;

    ftrel.close(RowExclusiveLock)
}

pub fn ImportForeignSchema<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &ImportForeignSchemaStmt<'mcx>,
) -> PgResult<()> {
    let servername = stmt.server_name.expect("ImportForeignSchemaStmt.server_name");
    let server = GetForeignServerByName(mcx, servername, false)?.expect("missing_ok=false");
    if aclchk::object_aclcheck(
        FOREIGN_SERVER_RELATION_ID,
        server.serverid,
        miscinit::GetUserId(),
        adt_acl::ACL_USAGE,
    )? != aclchk::ACLCHECK_OK
    {
        aclchk::aclcheck_error(
            aclchk::ACLCHECK_NO_PRIV,
            ObjectType::OBJECT_FOREIGN_SERVER,
            server.servername,
        )?;
    }
    catalog_namespace::LookupCreationNamespace(
        mcx,
        stmt.local_schema.expect("ImportForeignSchemaStmt.local_schema"),
    )?;
    // The no-handler error is the live surface; a handler-bearing FDW is loud.
    let fdw = GetForeignDataWrapper(mcx, server.fdwid)?;
    if fdw.fdwhandler == InvalidOid {
        return Err(::elog::ereport(ERROR)
            .errcode(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!("foreign-data wrapper \"{}\" has no handler", fdw.fdwname))
            .into_error()
            .into());
    }
    // unported: ImportForeignSchema handler invocation
    // (GetFdwRoutine; dfmgr/LANGUAGE C)
    Err(::elog::ereport(ERROR)
        .errcode(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
        .errmsg("IMPORT FOREIGN SCHEMA is not supported yet".to_string())
        .into_error()
        .into())
}

pub fn init_seams() {
    foreigncmds_seams::postgresql_fdw_validator::set(|mcx, options, catalog| {
        options::postgresql_fdw_validator(mcx, options, catalog)
    });
    foreigncmds_seams::get_fdw_routine_by_rel_id::set(GetFdwRoutineByRelId);
    foreigncmds_seams::get_fdw_routine_by_server_id::set(GetFdwRoutineByServerId);
    foreigncmds_seams::get_foreign_server_id_by_rel_id::set(GetForeignServerIdByRelId);
    foreigncmds_seams::get_foreign_data_wrapper_oid::set(get_foreign_data_wrapper_oid);
    foreigncmds_seams::get_foreign_server_oid::set(get_foreign_server_oid);
    foreigncmds_seams::pg_options_to_table::set(options::pg_options_to_table);
    pg_shdepend::alter_foreign_server_owner_oid::set(AlterForeignServerOwner_oid);
    pg_shdepend::alter_foreign_data_wrapper_owner_oid::set(AlterForeignDataWrapperOwner_oid);
}
