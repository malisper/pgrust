// alter.c: AlterObjectRename_internal / AlterObjectNamespace_internal /
// AlterObjectOwner_internal driven by objectaddress's ObjectProperty table,
// plus the ExecRenameStmt/ExecAlterObjectSchemaStmt/ExecAlterOwnerStmt
// generic arms. Per-class arms whose target commands are unported stay loud
// in tcop's dispatch.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::fmgr::{F_OIDEQ, NAMEDATALEN};
use types_core::primitive::OidIsValid;
use types_core::{
    AttrNumber, InvalidOid, Oid, DATABASE_RELATION_ID, NAMESPACE_RELATION_ID,
};
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_INSUFFICIENT_PRIVILEGE,
};
use types_nodes::parsenodes::{AlterObjectSchemaStmt, AlterOwnerStmt, ObjectType, RenameStmt};
use types_rel::{AccessExclusiveLock, Relation, RowExclusiveLock};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::InplaceUpdateTupleLock;
use types_tuple::{HeapTupleData, ItemPointerData, NameData, TupleDescData};

use cache_syscache::cacheinfo::SUBSCRIPTIONNAME;
use cache_syscache::{SearchSysCacheExists, SysCacheKey};
use catalog_objectaddress::ObjectAddress;

use pg_publication::{Anum_pg_publication_puballtables, PublicationRelationId};
use pg_subscription::{Anum_pg_subscription_subpasswordrequired, SubscriptionRelationId};

const CollationRelationId: Oid = 3456;
const OperatorRelationId: Oid = 2617;
const ConversionRelationId: Oid = 2607;
const ProcedureRelationId: Oid = 1255;
const OperatorClassRelationId: Oid = 2616;
const OperatorFamilyRelationId: Oid = 2753;
const StatisticExtRelationId: Oid = 3381;
const TSParserRelationId: Oid = 3601;
const TSDictionaryRelationId: Oid = 3600;
const TSTemplateRelationId: Oid = 3764;
const TSConfigRelationId: Oid = 3602;
const EventTriggerRelationId: Oid = 3466;
const ForeignDataWrapperRelationId: Oid = 2328;
const ForeignServerRelationId: Oid = 1417;
const LanguageRelationId: Oid = 2612;
const LargeObjectRelationId: Oid = 2613;
const LargeObjectMetadataRelationId: Oid = 2995;
const Anum_pg_opclass_opcmethod: i32 = 2;
const Anum_pg_opfamily_opfmethod: i32 = 2;

pub fn init_seams() {
    pg_shdepend::alter_object_owner_internal::set(AlterObjectOwner_internal);
    alter_seams::alter_object_namespace_oid::set(AlterObjectNamespace_oid);
}

fn eq_key(attno: AttrNumber, arg: Datum) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info({F_OIDEQ}) failed: {e:?}"));
    key.sk_argument = arg;
    key
}

fn name_arg<'mcx>(mcx: Mcx<'mcx>, name: &str) -> PgResult<PgVec<'mcx, u8>> {
    let n = NAMEDATALEN as usize;
    assert!(name.len() < n, "identifier truncation unported: {name:?}");
    let mut buf: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, n)?;
    mcx::vec_append_bytes(&mut buf, name.as_bytes())?;
    mcx::vec_append_bytes(&mut buf, &[0u8; 64][..n - name.len()])?;
    Ok(buf)
}

fn getattr(td: &TupleDescData<'_>, tup: &HeapTupleData<'_>, attno: i32) -> (Datum, bool) {
    let mut isnull = false;
    // SAFETY: tup is a catalog row read under its relation's descriptor.
    let d = unsafe { types_tuple::heap_getattr(tup, attno, td, &mut isnull) };
    (d, isnull)
}

fn name_attr(td: &TupleDescData<'_>, tup: &HeapTupleData<'_>, attno: i32) -> String {
    let d = getattr(td, tup, attno).0;
    // SAFETY: a name attr datum addresses NAMEDATALEN in-tuple bytes.
    let name = unsafe { core::ptr::read_unaligned(d.as_usize() as *const NameData) };
    core::str::from_utf8(name.name_str()).expect("catalog name is UTF-8").to_string()
}

fn namespace_name(mcx: Mcx<'_>, nsp_oid: Oid) -> PgResult<String> {
    Ok(lsyscache::get_namespace_name(mcx, nsp_oid)?
        .map(|n| n.to_string())
        .unwrap_or_default())
}

fn dup_err(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_DUPLICATE_OBJECT))
}

fn report_name_conflict(class_id: Oid, name: &str) -> Box<PgError> {
    let msg = match class_id {
        EventTriggerRelationId => format!("event trigger \"{name}\" already exists"),
        ForeignDataWrapperRelationId => {
            format!("foreign-data wrapper \"{name}\" already exists")
        }
        ForeignServerRelationId => format!("server \"{name}\" already exists"),
        LanguageRelationId => format!("language \"{name}\" already exists"),
        PublicationRelationId => format!("publication \"{name}\" already exists"),
        SubscriptionRelationId => format!("subscription \"{name}\" already exists"),
        other => panic!("report_name_conflict (alter.c): unsupported object class: {other}"),
    };
    dup_err(msg)
}

fn report_namespace_conflict(
    mcx: Mcx<'_>,
    class_id: Oid,
    name: &str,
    nsp_oid: Oid,
) -> PgResult<Box<PgError>> {
    let noun = match class_id {
        ConversionRelationId => "conversion",
        StatisticExtRelationId => "statistics object",
        TSParserRelationId => "text search parser",
        TSDictionaryRelationId => "text search dictionary",
        TSTemplateRelationId => "text search template",
        TSConfigRelationId => "text search configuration",
        other => panic!(
            "report_namespace_conflict (alter.c): unsupported object class: {other}"
        ),
    };
    Ok(dup_err(format!(
        "{noun} \"{name}\" already exists in schema \"{}\"",
        namespace_name(mcx, nsp_oid)?
    )))
}

// SearchSysCacheExists over one or two keys, alter.c's nameCacheId probes.
fn name_cache_exists(cache_id: i32, name: &str, nsp: Option<Oid>) -> PgResult<bool> {
    match nsp {
        Some(nsp) => SearchSysCacheExists(
            cache_id,
            SysCacheKey::Str(name),
            SysCacheKey::Value(Datum::from_oid(nsp)),
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
        ),
        None => SearchSysCacheExists(
            cache_id,
            SysCacheKey::Str(name),
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
        ),
    }
}

fn must_be_owner_err(
    mcx: Mcx<'_>,
    class_id: Oid,
    object_id: Oid,
    objname: &str,
) -> PgResult<()> {
    aclchk::aclcheck_error(
        aclchk::ACLCHECK_NOT_OWNER,
        catalog_objectaddress::get_object_type(class_id, object_id)?,
        objname,
    )?;
    let _ = mcx;
    Ok(())
}

fn check_namespace_create(mcx: Mcx<'_>, nsp_oid: Oid, roleid: Oid) -> PgResult<()> {
    let aclresult = aclchk::object_aclcheck(
        NAMESPACE_RELATION_ID,
        nsp_oid,
        roleid,
        adt_acl::ACL_CREATE,
    )?;
    if aclresult != aclchk::ACLCHECK_OK {
        aclchk::aclcheck_error(
            aclresult,
            ObjectType::OBJECT_SCHEMA,
            &namespace_name(mcx, nsp_oid)?,
        )?;
    }
    Ok(())
}

// Duplicate-name friendliness checks shared by rename (new name into the old
// namespace) and set-schema (old name into the new namespace).
fn check_duplicate_name<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tup: &HeapTupleData<'_>,
    object_id: Oid,
    name: &str,
    nsp_oid: Oid,
) -> PgResult<()> {
    let class_id = rel.rd_id;
    let td = rel.descr();
    match class_id {
        ProcedureRelationId => {
            let (_, argtypes) = lsyscache::get_func_signature(mcx, object_id)?;
            pg_proc::IsThereFunctionInNamespace(mcx, name, &argtypes, nsp_oid)?;
        }
        CollationRelationId => {
            collationcmds::IsThereCollationInNamespace(mcx, name, nsp_oid)?;
        }
        OperatorClassRelationId => {
            let opcmethod = getattr(td, tup, Anum_pg_opclass_opcmethod).0.as_oid();
            opclasscmds::IsThereOpClassInNamespace(mcx, name, opcmethod, nsp_oid)?;
        }
        OperatorFamilyRelationId => {
            let opfmethod = getattr(td, tup, Anum_pg_opfamily_opfmethod).0.as_oid();
            opclasscmds::IsThereOpFamilyInNamespace(mcx, name, opfmethod, nsp_oid)?;
        }
        SubscriptionRelationId => {
            if SearchSysCacheExists(
                SUBSCRIPTIONNAME,
                SysCacheKey::Value(Datum::from_oid(init_small::globals::MyDatabaseId())),
                SysCacheKey::Str(name),
                SysCacheKey::UNUSED,
                SysCacheKey::UNUSED,
            )? {
                return Err(report_name_conflict(class_id, name));
            }
            // Wake up related replication workers to handle this change
            // quickly (alter.c AlterObjectRename_internal).
            launcher::LogicalRepWorkersWakeupAtCommit(object_id);
        }
        _ => {
            let name_cache = catalog_objectaddress::get_object_catcache_name(class_id);
            if name_cache >= 0 {
                if OidIsValid(nsp_oid) {
                    if name_cache_exists(name_cache, name, Some(nsp_oid))? {
                        return Err(report_namespace_conflict(mcx, class_id, name, nsp_oid)?);
                    }
                } else if name_cache_exists(name_cache, name, None)? {
                    return Err(report_name_conflict(class_id, name));
                }
            }
        }
    }
    Ok(())
}

fn begin_oid_scan<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    object_id: Oid,
) -> PgResult<genam::SysScanDesc<'mcx>> {
    let class_id = rel.rd_id;
    let oid_attnum = catalog_objectaddress::get_object_attnum_oid(class_id);
    let oid_index = catalog_objectaddress::get_object_oid_index(class_id);
    let keys = [eq_key(oid_attnum as AttrNumber, Datum::from_oid(object_id))];
    genam::systable_beginscan(mcx, rel, oid_index, true, None, &keys)
}

#[track_caller]
#[cold]
fn row_missing(rel: &Relation<'_>, object_id: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "cache lookup failed for object {object_id} of catalog \"{}\"",
        rel.name()
    )))
}

fn build_replace_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    td: &TupleDescData<'mcx>,
    natts: usize,
    replacements: &[(i32, Datum)],
    oldtup: &HeapTupleData<'_>,
) -> PgResult<heaptuple::HeapTuple<'mcx>> {
    let mut repl_values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl_isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    repl_values.resize(natts, Datum::null());
    repl_isnull.resize(natts, false);
    repl.resize(natts, false);
    for &(attnum, d) in replacements {
        repl_values[(attnum - 1) as usize] = d;
        repl[(attnum - 1) as usize] = true;
    }
    heaptuple::heap_modify_tuple(mcx, oldtup, td, &repl_values, &repl_isnull, &repl)
}

// AlterObjectRename_internal (alter.c): rename the name column of a single
// catalog entry; won't work for tables or other multi-step renames.
pub fn AlterObjectRename_internal<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    object_id: Oid,
    new_name: &str,
) -> PgResult<()> {
    let class_id = rel.rd_id;
    let anum_name = catalog_objectaddress::get_object_attnum_name(class_id);
    let anum_namespace = catalog_objectaddress::get_object_attnum_namespace(class_id);
    let anum_owner = catalog_objectaddress::get_object_attnum_owner(class_id);
    let td = rel.descr();

    let mut scan = begin_oid_scan(mcx, rel, object_id)?;
    let Some(oldtup) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(row_missing(rel, object_id));
    };

    let old_name = name_attr(td, oldtup, anum_name);
    let namespace_id = if anum_namespace > 0 {
        getattr(td, oldtup, anum_namespace).0.as_oid()
    } else {
        InvalidOid
    };

    if !superuser::superuser()? {
        if anum_owner <= 0 {
            let descr = catalog_objectaddress::getObjectDescription(
                mcx,
                &ObjectAddress::set(class_id, object_id),
                false,
            )?
            .unwrap_or_default();
            return Err(Box::new(
                PgError::error(format!("must be superuser to rename {descr}"))
                    .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
            ));
        }
        let owner_id = getattr(td, oldtup, anum_owner).0.as_oid();
        if !adt_acl::has_privs_of_role(miscinit::GetUserId(), owner_id)? {
            must_be_owner_err(mcx, class_id, object_id, &old_name)?;
        }
        if OidIsValid(namespace_id) {
            check_namespace_create(mcx, namespace_id, miscinit::GetUserId())?;
        }

        if class_id == SubscriptionRelationId {
            let aclresult = aclchk::object_aclcheck(
                DATABASE_RELATION_ID,
                init_small::globals::MyDatabaseId(),
                miscinit::GetUserId(),
                adt_acl::ACL_CREATE,
            )?;
            if aclresult != aclchk::ACLCHECK_OK {
                let dbname = dbcommands::get_database_name(init_small::globals::MyDatabaseId())?
                    .unwrap_or_default();
                aclchk::aclcheck_error(aclresult, ObjectType::OBJECT_DATABASE, &dbname)?;
            }
            let subpasswordrequired =
                getattr(td, oldtup, Anum_pg_subscription_subpasswordrequired).0.as_bool();
            if !subpasswordrequired {
                return Err(Box::new(
                    PgError::error("password_required=false is superuser-only")
                        .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE)
                        .with_hint(
                            "Subscriptions with the password_required option set to false \
                             may only be created or modified by the superuser.",
                        ),
                ));
            }
        }
    }

    check_duplicate_name(mcx, rel, oldtup, object_id, new_name, namespace_id)?;

    let puballtables = if class_id == PublicationRelationId {
        getattr(td, oldtup, Anum_pg_publication_puballtables).0.as_bool()
    } else {
        false
    };

    let nname = name_arg(mcx, new_name)?;
    let mut newtup = build_replace_tuple(
        mcx,
        td,
        td.natts as usize,
        &[(anum_name, Datum::from_usize(nname.as_ptr() as usize))],
        oldtup,
    )?;
    let otid = oldtup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, rel, &otid, &mut newtup)?;

    if class_id == PublicationRelationId {
        commands_publicationcmds::InvalidatePubRelSyncCache(mcx, object_id, puballtables)?;
    }

    Ok(())
}

// ExecRenameStmt (alter.c), generic catalog arm.
pub fn ExecRenameStmt_generic<'mcx>(mcx: Mcx<'mcx>, stmt: &RenameStmt<'mcx>) -> PgResult<ObjectAddress> {
    let (address, _relation) = catalog_objectaddress::get_object_address(
        mcx,
        stmt.renameType,
        stmt.object.expect("RenameStmt.object"),
        AccessExclusiveLock,
        false,
    )?;

    let catalog_rel = table::table_open(mcx, address.classId, RowExclusiveLock)?;
    AlterObjectRename_internal(
        mcx,
        &catalog_rel,
        address.objectId,
        stmt.newname.expect("RenameStmt.newname"),
    )?;
    catalog_rel.close(RowExclusiveLock)?;
    Ok(address)
}

// AlterObjectNamespace_internal (alter.c): move a single catalog entry to a
// new namespace. Returns the previous namespace.
pub fn AlterObjectNamespace_internal<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    objid: Oid,
    nsp_oid: Oid,
) -> PgResult<Oid> {
    let class_id = rel.rd_id;
    let anum_name = catalog_objectaddress::get_object_attnum_name(class_id);
    let anum_namespace = catalog_objectaddress::get_object_attnum_namespace(class_id);
    let anum_owner = catalog_objectaddress::get_object_attnum_owner(class_id);
    let td = rel.descr();

    let mut scan = begin_oid_scan(mcx, rel, objid)?;
    let Some(oldtup) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(row_missing(rel, objid));
    };

    let name = name_attr(td, oldtup, anum_name);
    let old_nsp_oid = getattr(td, oldtup, anum_namespace).0.as_oid();

    if old_nsp_oid == nsp_oid {
        genam::systable_endscan(mcx, scan)?;
        return Ok(old_nsp_oid);
    }

    catalog_namespace::CheckSetNamespace(old_nsp_oid, nsp_oid)?;

    if !superuser::superuser()? {
        if anum_owner <= 0 {
            let descr = catalog_objectaddress::getObjectDescription(
                mcx,
                &ObjectAddress::set(class_id, objid),
                false,
            )?
            .unwrap_or_default();
            return Err(Box::new(
                PgError::error(format!("must be superuser to set schema of {descr}"))
                    .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
            ));
        }
        let owner_id = getattr(td, oldtup, anum_owner).0.as_oid();
        if !adt_acl::has_privs_of_role(miscinit::GetUserId(), owner_id)? {
            must_be_owner_err(mcx, class_id, objid, &name)?;
        }
        check_namespace_create(mcx, nsp_oid, miscinit::GetUserId())?;
    }

    check_duplicate_name(mcx, rel, oldtup, objid, &name, nsp_oid)?;

    let mut newtup = build_replace_tuple(
        mcx,
        td,
        td.natts as usize,
        &[(anum_namespace, Datum::from_oid(nsp_oid))],
        oldtup,
    )?;
    let otid = oldtup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, rel, &otid, &mut newtup)?;

    if pg_depend::changeDependencyFor(
        mcx,
        class_id,
        objid,
        NAMESPACE_RELATION_ID,
        old_nsp_oid,
        nsp_oid,
    )? != 1
    {
        return Err(Box::new(PgError::error(format!(
            "could not change schema dependency for object {objid}"
        ))));
    }

    Ok(old_nsp_oid)
}

// AlterObjectNamespace_oid (alter.c): change the schema of ONE object of
// any class, as part of a bulk move (only ALTER EXTENSION SET SCHEMA reaches
// this today). Returns the object's old namespace OID, or InvalidOid for
// object types that do not have schema-qualified names ("ignore object
// types that don't have schema-qualified names" — C's default arm).
pub fn AlterObjectNamespace_oid<'mcx>(
    mcx: Mcx<'mcx>,
    class_id: Oid,
    objid: Oid,
    nsp_oid: Oid,
    objs_moved: &mut PgVec<'mcx, ObjectAddress>,
) -> PgResult<Oid> {
    const RELATION_RELATION_ID: Oid = 1259;
    const TYPE_RELATION_ID: Oid = 1247;
    match class_id {
        RELATION_RELATION_ID => {
            let rel = relation_seams::relation_open::call(mcx, objid, AccessExclusiveLock)?;
            let old_nsp_oid = rel.rd_rel.relnamespace;
            tablecmds::AlterTableNamespaceInternal(
                mcx,
                &rel,
                old_nsp_oid,
                nsp_oid,
                objs_moved,
            )?;
            rel.close(types_rel::lock::NoLock)?;
            Ok(old_nsp_oid)
        }
        TYPE_RELATION_ID => {
            typecmds::AlterTypeNamespace_oid(mcx, objid, nsp_oid, true, objs_moved)
        }
        ProcedureRelationId
        | CollationRelationId
        | ConversionRelationId
        | OperatorRelationId
        | OperatorClassRelationId
        | OperatorFamilyRelationId
        | StatisticExtRelationId
        | TSParserRelationId
        | TSDictionaryRelationId
        | TSTemplateRelationId
        | TSConfigRelationId => {
            let catalog_rel = table::table_open(mcx, class_id, RowExclusiveLock)?;
            let old_nsp_oid = AlterObjectNamespace_internal(mcx, &catalog_rel, objid, nsp_oid)?;
            catalog_rel.close(RowExclusiveLock)?;
            Ok(old_nsp_oid)
        }
        // C: Assert(get_object_attnum_namespace(classId) == InvalidAttrNumber).
        _ => Ok(InvalidOid),
    }
}

// ExecAlterObjectSchemaStmt (alter.c), generic catalog arm.
pub fn ExecAlterObjectSchemaStmt_generic<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterObjectSchemaStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let (address, _relation) = catalog_objectaddress::get_object_address(
        mcx,
        stmt.objectType,
        stmt.object.expect("AlterObjectSchemaStmt.object"),
        AccessExclusiveLock,
        false,
    )?;

    let catalog_rel = table::table_open(mcx, address.classId, RowExclusiveLock)?;
    let nsp_oid =
        catalog_namespace::LookupCreationNamespace(mcx, stmt.newschema.expect("newschema"))?;
    AlterObjectNamespace_internal(mcx, &catalog_rel, address.objectId, nsp_oid)?;
    catalog_rel.close(RowExclusiveLock)?;
    Ok(address)
}

// AlterObjectOwner_internal (alter.c): change the owner column of a single
// catalog entry; large objects redirect to pg_largeobject_metadata.
pub fn AlterObjectOwner_internal<'mcx>(
    mcx: Mcx<'mcx>,
    class_id: Oid,
    object_id: Oid,
    new_owner_id: Oid,
) -> PgResult<()> {
    let catalog_id = if class_id == LargeObjectRelationId {
        LargeObjectMetadataRelationId
    } else {
        class_id
    };
    let anum_owner = catalog_objectaddress::get_object_attnum_owner(catalog_id);
    let anum_namespace = catalog_objectaddress::get_object_attnum_namespace(catalog_id);
    let anum_acl = catalog_objectaddress::get_object_attnum_acl(catalog_id);
    let anum_name = catalog_objectaddress::get_object_attnum_name(catalog_id);

    let rel = table::table_open(mcx, catalog_id, RowExclusiveLock)?;
    let td = rel.descr();

    // get_catalog_object_by_oid_extended(locktup=true).
    let mut scan = begin_oid_scan(mcx, &rel, object_id)?;
    let Some(oldtup) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(row_missing(&rel, object_id));
    };
    let otid: ItemPointerData = oldtup.t_self;
    lmgr::LockTuple(&rel, &otid, InplaceUpdateTupleLock)?;

    let old_owner_id = getattr(td, oldtup, anum_owner).0.as_oid();
    let namespace_id = if anum_namespace > 0 {
        getattr(td, oldtup, anum_namespace).0.as_oid()
    } else {
        InvalidOid
    };

    if old_owner_id != new_owner_id {
        if !superuser::superuser()? {
            if !adt_acl::has_privs_of_role(miscinit::GetUserId(), old_owner_id)? {
                let objname = if anum_name > 0 {
                    name_attr(td, oldtup, anum_name)
                } else {
                    format!("{object_id}")
                };
                must_be_owner_err(mcx, catalog_id, object_id, &objname)?;
            }
            // check_can_set_role (acl.c).
            if !adt_acl::member_can_set_role(miscinit::GetUserId(), new_owner_id)? {
                let rolename = miscinit::GetUserNameFromId(mcx, new_owner_id, false)?
                    .map(|n| n.to_string())
                    .unwrap_or_default();
                return Err(Box::new(
                    PgError::error(format!("must be able to SET ROLE \"{rolename}\""))
                        .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
                ));
            }
            if OidIsValid(namespace_id) {
                check_namespace_create(mcx, namespace_id, new_owner_id)?;
            }
        }

        let mut replacements: Vec<(i32, Datum)> =
            vec![(anum_owner, Datum::from_oid(new_owner_id))];
        let acl_img;
        if anum_acl > 0 {
            let (acl_datum, isnull) = getattr(td, oldtup, anum_acl);
            if !isnull {
                let new_acl = aclchk::with_acl_datum(acl_datum, |acl| {
                    adt_acl::aclnewowner(mcx, acl, old_owner_id, new_owner_id)
                })?;
                acl_img = adt_acl::varlena::acl_image(mcx, &new_acl)?;
                replacements.push((anum_acl, Datum::from_usize(acl_img.as_ptr() as usize)));
            }
        }

        let mut newtup =
            build_replace_tuple(mcx, td, td.natts as usize, &replacements, oldtup)?;
        genam::systable_endscan(mcx, scan)?;
        catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut newtup)?;
        lmgr::UnlockTuple(&rel, &otid, InplaceUpdateTupleLock)?;

        pg_shdepend::changeDependencyOnOwner(mcx, class_id, object_id, new_owner_id)?;
    } else {
        genam::systable_endscan(mcx, scan)?;
        lmgr::UnlockTuple(&rel, &otid, InplaceUpdateTupleLock)?;
    }

    rel.close(RowExclusiveLock)?;
    Ok(())
}

// ExecAlterOwnerStmt (alter.c). Per-class arms whose target commands are
// unported stay loud.
pub fn ExecAlterOwnerStmt<'mcx>(mcx: Mcx<'mcx>, stmt: &AlterOwnerStmt<'mcx>) -> PgResult<ObjectAddress> {
    let newowner = aclchk::get_rolespec_oid(stmt.newowner.expect("AlterOwnerStmt.newowner"), false)?;

    match stmt.objectType {
        ObjectType::OBJECT_PUBLICATION => {
            let name = stmt
                .object
                .and_then(|o| o.as_string())
                .expect("ALTER PUBLICATION OWNER object is a String")
                .sval;
            commands_publicationcmds::AlterPublicationOwner(mcx, name, newowner)
        }
        ObjectType::OBJECT_SUBSCRIPTION => {
            let name = stmt
                .object
                .and_then(|o| o.as_string())
                .expect("ALTER SUBSCRIPTION OWNER object is a String")
                .sval;
            subscriptioncmds::AlterSubscriptionOwner(mcx, name, newowner)
        }
        ObjectType::OBJECT_SCHEMA => {
            let name = stmt
                .object
                .and_then(|o| o.as_string())
                .expect("ALTER SCHEMA OWNER object is a String")
                .sval;
            let nsp_oid = schemacmds::AlterSchemaOwner(mcx, name, newowner)?;
            Ok(ObjectAddress {
                classId: NAMESPACE_RELATION_ID,
                objectId: nsp_oid,
                objectSubId: 0,
            })
        }
        ObjectType::OBJECT_DATABASE => {
            let name = stmt
                .object
                .and_then(|o| o.as_string())
                .expect("ALTER DATABASE OWNER object is a String")
                .sval;
            let db_id = dbcommands::AlterDatabaseOwner(mcx, name, newowner)?;
            Ok(ObjectAddress {
                classId: DATABASE_RELATION_ID,
                objectId: db_id,
                objectSubId: 0,
            })
        }
        ObjectType::OBJECT_FDW => {
            let name = stmt
                .object
                .and_then(|o| o.as_string())
                .expect("ALTER FOREIGN DATA WRAPPER OWNER object is a String")
                .sval;
            foreigncmds::AlterForeignDataWrapperOwner(mcx, name, newowner)
        }
        ObjectType::OBJECT_FOREIGN_SERVER => {
            let name = stmt
                .object
                .and_then(|o| o.as_string())
                .expect("ALTER SERVER OWNER object is a String")
                .sval;
            foreigncmds::AlterForeignServerOwner(mcx, name, newowner)
        }
        ObjectType::OBJECT_AGGREGATE
        | ObjectType::OBJECT_COLLATION
        | ObjectType::OBJECT_CONVERSION
        | ObjectType::OBJECT_FUNCTION
        | ObjectType::OBJECT_LANGUAGE
        | ObjectType::OBJECT_LARGEOBJECT
        | ObjectType::OBJECT_OPERATOR
        | ObjectType::OBJECT_OPCLASS
        | ObjectType::OBJECT_OPFAMILY
        | ObjectType::OBJECT_PROCEDURE
        | ObjectType::OBJECT_ROUTINE
        | ObjectType::OBJECT_STATISTIC_EXT
        | ObjectType::OBJECT_TABLESPACE
        | ObjectType::OBJECT_TSDICTIONARY
        | ObjectType::OBJECT_TSCONFIGURATION => {
            let (address, _relation) = catalog_objectaddress::get_object_address(
                mcx,
                stmt.objectType,
                stmt.object.expect("AlterOwnerStmt.object"),
                AccessExclusiveLock,
                false,
            )?;
            AlterObjectOwner_internal(mcx, address.classId, address.objectId, newowner)?;
            Ok(address)
        }
        // unported: ExecAlterOwnerStmt (alter.c) remaining object-type arms
        _ => Err(Box::new(
            types_error::PgError::error(
                "changing the owner of this type of object is not supported yet",
            )
            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
        )),
    }
}
