//! schemacmds.c, CreateSchemaCommand lane. The caller supplies exec_elements
//! (C calls ProcessUtility directly; the layering here runs upward through the
//! tcop dispatcher), invoked between the search_path override and its undo.

#![allow(non_snake_case)]

use datum::Datum;
use mcx::Mcx;
use types_core::{
    AttrNumber, InvalidOid, Oid, NAMESPACE_RELATION_ID, SECURITY_LOCAL_USERID_CHANGE,
};
use types_error::{
    PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_RESERVED_NAME, ERROR, NOTICE,
};
use types_guc::{PGC_S_SESSION, PGC_USERSET};
use types_nodes::parsenodes::{CreateSchemaStmt, ObjectType};
use types_nodes::NodeList;
use types_rel::{NoLock, Relation, RowExclusiveLock};
use types_tuple::{HeapTupleData, NameData, TupleDescData};

const Anum_pg_namespace_nspname: i32 = 2;
const Anum_pg_namespace_nspowner: i32 = 3;
const Anum_pg_namespace_nspacl: i32 = 4;

// check_can_set_role (acl.c).
fn check_can_set_role(mcx: Mcx<'_>, member: Oid, role: Oid) -> PgResult<()> {
    if !adt_acl::member_can_set_role(member, role)? {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "must be able to SET ROLE \"{}\"",
                    miscinit::GetUserNameFromId(mcx, role, false)?
                        .expect("noerr=false")
                        .as_str()
                ),
            )
            .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }
    Ok(())
}

pub fn CreateSchemaCommand<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateSchemaStmt<'mcx>,
    exec_elements: &mut dyn FnMut(Oid, &NodeList<'mcx>, &str) -> PgResult<()>,
) -> PgResult<Oid> {
    let (saved_uid, save_sec_context) = miscinit::GetUserIdAndSecContext();

    let owner_uid = match stmt.authrole {
        Some(role) => aclchk::get_rolespec_oid(
            role.as_role_spec().expect("authrole is a RoleSpec"),
            false,
        )?,
        None => saved_uid,
    };

    // Fill schema name with the user name if not specified.
    let owner_name;
    let schema_name = match stmt.schemaname {
        Some(s) => s,
        None => {
            owner_name =
                miscinit::GetUserNameFromId(mcx, owner_uid, false)?.expect("noerr=false");
            owner_name.as_str()
        }
    };

    let aclresult = aclchk::object_aclcheck(
        types_core::catalog::DATABASE_RELATION_ID,
        init_small::globals::MyDatabaseId(),
        saved_uid,
        adt_acl::ACL_CREATE,
    )?;
    if aclresult != aclchk::ACLCHECK_OK {
        let dbname = dbcommands::get_database_name(init_small::globals::MyDatabaseId())?
            .unwrap_or_default();
        aclchk_seams::aclcheck_error::call(
            aclresult,
            ObjectType::OBJECT_DATABASE as i32,
            &dbname,
        )?;
    }

    check_can_set_role(mcx, saved_uid, owner_uid)?;

    if !init_small::globals::allowSystemTableMods() && catalog::IsReservedName(schema_name) {
        return Err(Box::new(
            PgError::new(ERROR, format!("unacceptable schema name \"{schema_name}\""))
                .with_sqlstate(ERRCODE_RESERVED_NAME)
                .with_detail("The prefix \"pg_\" is reserved for system schemas."),
        ));
    }

    if stmt.if_not_exists
        && catalog_namespace::get_namespace_oid(schema_name, true)? != InvalidOid
    {
        // C: checkMembershipInCurrentExtension guards extension scripts
        // reusing pre-existing schemas; extension-script state is loud at the
        // extension lane, so the pre-existing-object hole cannot be reached
        // silently here.
        elog_seams::ereport::call(
            PgError::new(NOTICE, format!("schema \"{schema_name}\" already exists, skipping"))
                .with_sqlstate(types_error::ERRCODE_DUPLICATE_SCHEMA),
        )?;
        return Ok(InvalidOid);
    }

    // Create the objects as the target role; error paths rely on transaction
    // abort to restore the identity, as C does.
    if saved_uid != owner_uid {
        miscinit::SetUserIdAndSecContext(
            owner_uid,
            save_sec_context | SECURITY_LOCAL_USERID_CHANGE,
        );
    }

    let namespace_id = pg_namespace::NamespaceCreate(mcx, schema_name, owner_uid, false)?;
    xact::CommandCounterIncrement()?;

    // Prepend the new schema to the search path for exactly the duration of
    // the element subcommands (function-SET-option style save; guc.c undoes
    // it on error).
    let save_nestlevel = guc::NewGUCNestLevel();
    let mut pathbuf = String::from_utf8(
        adt_quote::quote_identifier(mcx, schema_name.as_bytes())?.as_bytes().to_vec(),
    )
    .expect("identifier is UTF-8");
    let nsp = guc::GetConfigOption("search_path", false, false)?.unwrap_or_default();
    let nsp = nsp.trim_start();
    if !nsp.is_empty() {
        pathbuf.push_str(", ");
        pathbuf.push_str(nsp);
    }
    guc::set_config_option(
        "search_path",
        Some(&pathbuf),
        PGC_USERSET,
        PGC_S_SESSION,
        guc::GUC_ACTION_SAVE,
        true,
        types_error::ErrorLevel(0),
        false,
    )?;

    // The caller collects the schema for event triggers ahead of the element
    // subcommands and hands each element to ProcessUtility (C does both
    // inline here).
    exec_elements(namespace_id, &stmt.schemaElts, schema_name)?;

    guc::AtEOXact_GUC(true, save_nestlevel);

    if saved_uid != owner_uid {
        miscinit::SetUserIdAndSecContext(saved_uid, save_sec_context);
    }
    Ok(namespace_id)
}

fn getattr(td: &TupleDescData<'_>, tup: &HeapTupleData<'_>, attno: i32) -> (Datum, bool) {
    let mut isnull = false;
    // SAFETY: tup is a pg_namespace row read under pg_namespace's descriptor.
    let d = unsafe { types_tuple::heap_getattr(tup, attno, td, &mut isnull) };
    (d, isnull)
}

fn begin_oid_scan<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    nsp_oid: Oid,
) -> PgResult<genam::SysScanDesc<'mcx>> {
    let mut key = types_scan::scankey::ScanKeyData::empty();
    key.sk_attno = pg_namespace::Anum_pg_namespace_oid as AttrNumber;
    key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(nsp_oid);
    genam::systable_beginscan(
        mcx,
        rel,
        pg_namespace::NamespaceOidIndexId,
        true,
        None,
        core::slice::from_ref(&key),
    )
}

fn database_create_aclcheck() -> PgResult<()> {
    let aclresult = aclchk::object_aclcheck(
        types_core::catalog::DATABASE_RELATION_ID,
        init_small::globals::MyDatabaseId(),
        miscinit::GetUserId(),
        adt_acl::ACL_CREATE,
    )?;
    if aclresult != aclchk::ACLCHECK_OK {
        let dbname = dbcommands::get_database_name(init_small::globals::MyDatabaseId())?
            .unwrap_or_default();
        aclchk::aclcheck_error(aclresult, ObjectType::OBJECT_DATABASE, &dbname)?;
    }
    Ok(())
}

fn modify_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    td: &TupleDescData<'mcx>,
    oldtup: &HeapTupleData<'_>,
    replacements: &[(i32, Datum)],
) -> PgResult<heaptuple::HeapTuple<'mcx>> {
    let natts = td.natts as usize;
    let mut repl_values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl_isnull: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    repl_values.resize(natts, Datum::null());
    repl_isnull.resize(natts, false);
    repl.resize(natts, false);
    for &(attnum, d) in replacements {
        repl_values[(attnum - 1) as usize] = d;
        repl[(attnum - 1) as usize] = true;
    }
    heaptuple::heap_modify_tuple(mcx, oldtup, td, &repl_values, &repl_isnull, &repl)
}

// RenameSchema (schemacmds.c). C fetches the tuple via the NAMESPACENAME
// syscache; the by-name oid probe + oid-index scan is this repo's catalog
// idiom, with the same error surface.
pub fn RenameSchema<'mcx>(mcx: Mcx<'mcx>, oldname: &str, newname: &str) -> PgResult<Oid> {
    let rel = table::table_open(mcx, NAMESPACE_RELATION_ID, RowExclusiveLock)?;
    let nsp_oid = catalog_namespace::get_namespace_oid(oldname, false)?;
    let td = rel.descr();
    let mut scan = begin_oid_scan(mcx, &rel, nsp_oid)?;
    let oldtup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for namespace {nsp_oid}"));

    if catalog_namespace::get_namespace_oid(newname, true)? != InvalidOid {
        return Err(Box::new(
            PgError::new(ERROR, format!("schema \"{newname}\" already exists"))
                .with_sqlstate(types_error::ERRCODE_DUPLICATE_SCHEMA),
        ));
    }

    if !aclchk::object_ownercheck(NAMESPACE_RELATION_ID, nsp_oid, miscinit::GetUserId())? {
        aclchk::aclcheck_error(aclchk::ACLCHECK_NOT_OWNER, ObjectType::OBJECT_SCHEMA, oldname)?;
    }
    database_create_aclcheck()?;

    if !init_small::globals::allowSystemTableMods() && catalog::IsReservedName(newname) {
        return Err(Box::new(
            PgError::new(ERROR, format!("unacceptable schema name \"{newname}\""))
                .with_sqlstate(ERRCODE_RESERVED_NAME)
                .with_detail("The prefix \"pg_\" is reserved for system schemas."),
        ));
    }

    let mut name = NameData::default();
    name.namestrcpy(newname);
    let mut newtup = modify_tuple(
        mcx,
        td,
        oldtup,
        &[(Anum_pg_namespace_nspname, Datum::from_usize(name.data.as_ptr() as usize))],
    )?;
    let otid = oldtup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut newtup)?;

    rel.close(NoLock)?;
    Ok(nsp_oid)
}

pub fn init_seams() {
    pg_shdepend::alter_schema_owner_oid::set(AlterSchemaOwner_oid);
}

pub fn AlterSchemaOwner<'mcx>(mcx: Mcx<'mcx>, name: &str, newOwnerId: Oid) -> PgResult<Oid> {
    let rel = table::table_open(mcx, NAMESPACE_RELATION_ID, RowExclusiveLock)?;
    let nsp_oid = catalog_namespace::get_namespace_oid(name, false)?;
    AlterSchemaOwner_internal(mcx, &rel, nsp_oid, newOwnerId)?;
    rel.close(RowExclusiveLock)?;
    Ok(nsp_oid)
}

pub fn AlterSchemaOwner_oid<'mcx>(mcx: Mcx<'mcx>, schemaoid: Oid, newOwnerId: Oid) -> PgResult<()> {
    let rel = table::table_open(mcx, NAMESPACE_RELATION_ID, RowExclusiveLock)?;
    AlterSchemaOwner_internal(mcx, &rel, schemaoid, newOwnerId)?;
    rel.close(RowExclusiveLock)
}

fn AlterSchemaOwner_internal<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    nsp_oid: Oid,
    newOwnerId: Oid,
) -> PgResult<()> {
    let td = rel.descr();
    let mut scan = begin_oid_scan(mcx, rel, nsp_oid)?;
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(Box::new(PgError::error(format!(
            "cache lookup failed for schema {nsp_oid}"
        ))));
    };

    let old_owner = getattr(td, tup, Anum_pg_namespace_nspowner).0.as_oid();
    if old_owner == newOwnerId {
        return genam::systable_endscan(mcx, scan);
    }

    if !aclchk::object_ownercheck(NAMESPACE_RELATION_ID, nsp_oid, miscinit::GetUserId())? {
        let d = getattr(td, tup, Anum_pg_namespace_nspname).0;
        // SAFETY: a name attr datum addresses NAMEDATALEN in-tuple bytes.
        let nspname = unsafe { core::ptr::read_unaligned(d.as_usize() as *const NameData) };
        let nspname =
            core::str::from_utf8(nspname.name_str()).expect("catalog name is UTF-8").to_string();
        aclchk::aclcheck_error(aclchk::ACLCHECK_NOT_OWNER, ObjectType::OBJECT_SCHEMA, &nspname)?;
    }

    // check_can_set_role (acl.c).
    if !adt_acl::member_can_set_role(miscinit::GetUserId(), newOwnerId)? {
        let rolename = miscinit::GetUserNameFromId(mcx, newOwnerId, false)?
            .map(|n| n.to_string())
            .unwrap_or_default();
        return Err(Box::new(
            PgError::error(format!("must be able to SET ROLE \"{rolename}\""))
                .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }

    database_create_aclcheck()?;

    let mut replacements: Vec<(i32, Datum)> =
        vec![(Anum_pg_namespace_nspowner, Datum::from_oid(newOwnerId))];
    let acl_img;
    let (acl_datum, isnull) = getattr(td, tup, Anum_pg_namespace_nspacl);
    if !isnull {
        let new_acl = aclchk::with_acl_datum(acl_datum, |acl| {
            adt_acl::aclnewowner(mcx, acl, old_owner, newOwnerId)
        })?;
        acl_img = adt_acl::varlena::acl_image(mcx, &new_acl)?;
        replacements.push((Anum_pg_namespace_nspacl, Datum::from_usize(acl_img.as_ptr() as usize)));
    }

    let mut newtup = modify_tuple(mcx, td, tup, &replacements)?;
    let otid = tup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, rel, &otid, &mut newtup)?;

    pg_shdepend::changeDependencyOnOwner(mcx, NAMESPACE_RELATION_ID, nsp_oid, newOwnerId)
}
