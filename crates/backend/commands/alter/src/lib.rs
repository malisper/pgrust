// alter.c, publication/subscription slice: AlterObjectRename_internal with
// object properties for pg_publication + pg_subscription only, the
// ExecRenameStmt generic arm for those classes, and ExecAlterOwnerStmt.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::fmgr::{F_NAMEEQ, F_OIDEQ, NAMEDATALEN};
use types_core::primitive::RegProcedure;
use types_core::{AttrNumber, Oid, DATABASE_RELATION_ID};
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_INSUFFICIENT_PRIVILEGE,
};
use types_nodes::parsenodes::{AlterOwnerStmt, ObjectType, RenameStmt};
use types_rel::{AccessExclusiveLock, Relation, RowExclusiveLock};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_tuple::{HeapTupleData, NameData, TupleDescData};

use cache_syscache::cacheinfo::{PUBLICATIONNAME, SUBSCRIPTIONNAME};
use cache_syscache::{SearchSysCacheExists, SysCacheKey};
use catalog_objectaddress::ObjectAddress;

use pg_publication::{
    Anum_pg_publication_puballtables, Anum_pg_publication_pubname, Anum_pg_publication_pubowner,
    PublicationObjectIndexId, PublicationRelationId,
};
use pg_subscription::{
    Anum_pg_subscription_subname, Anum_pg_subscription_subowner,
    Anum_pg_subscription_subpasswordrequired, SubscriptionObjectIndexId, SubscriptionRelationId,
};

fn eq_key(attno: AttrNumber, func: RegProcedure, arg: Datum) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(func)
        .unwrap_or_else(|e| panic!("fmgr_info({func}) failed: {e:?}"));
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

struct ObjectProps {
    oid_index: Oid,
    name_attnum: i32,
    owner_attnum: i32,
    objtype: ObjectType,
}

fn object_props(class_id: Oid) -> ObjectProps {
    match class_id {
        PublicationRelationId => ObjectProps {
            oid_index: PublicationObjectIndexId,
            name_attnum: Anum_pg_publication_pubname,
            owner_attnum: Anum_pg_publication_pubowner,
            objtype: ObjectType::OBJECT_PUBLICATION,
        },
        SubscriptionRelationId => ObjectProps {
            oid_index: SubscriptionObjectIndexId,
            name_attnum: Anum_pg_subscription_subname,
            owner_attnum: Anum_pg_subscription_subowner,
            objtype: ObjectType::OBJECT_SUBSCRIPTION,
        },
        other => panic!("AlterObjectRename_internal (alter.c): object class {other} unported"),
    }
}

fn report_name_conflict(class_id: Oid, name: &str) -> Box<PgError> {
    let msg = match class_id {
        PublicationRelationId => format!("publication \"{name}\" already exists"),
        SubscriptionRelationId => format!("subscription \"{name}\" already exists"),
        other => panic!("report_name_conflict (alter.c): object class {other} unported"),
    };
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_DUPLICATE_OBJECT))
}

pub fn AlterObjectRename_internal<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    object_id: Oid,
    new_name: &str,
) -> PgResult<()> {
    let class_id = rel.rd_id;
    let props = object_props(class_id);
    let td = rel.descr();

    let keys = [eq_key(1, F_OIDEQ, Datum::from_oid(object_id))];
    let mut scan = genam::systable_beginscan(mcx, rel, props.oid_index, true, None, &keys)?;
    let Some(oldtup) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(Box::new(PgError::error(format!(
            "cache lookup failed for object {object_id} of catalog \"{}\"",
            rel.name()
        ))));
    };

    let old_name = name_attr(td, oldtup, props.name_attnum);

    if !superuser::superuser()? {
        let owner_id = getattr(td, oldtup, props.owner_attnum).0.as_oid();
        if !adt_acl::has_privs_of_role(miscinit::GetUserId(), owner_id)? {
            aclchk::aclcheck_error(aclchk::ACLCHECK_NOT_OWNER, props.objtype, &old_name)?;
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

    match class_id {
        PublicationRelationId => {
            if SearchSysCacheExists(
                PUBLICATIONNAME,
                SysCacheKey::Str(new_name),
                SysCacheKey::UNUSED,
                SysCacheKey::UNUSED,
                SysCacheKey::UNUSED,
            )? {
                return Err(report_name_conflict(class_id, new_name));
            }
        }
        SubscriptionRelationId => {
            if SearchSysCacheExists(
                SUBSCRIPTIONNAME,
                SysCacheKey::Value(Datum::from_oid(init_small::globals::MyDatabaseId())),
                SysCacheKey::Str(new_name),
                SysCacheKey::UNUSED,
                SysCacheKey::UNUSED,
            )? {
                return Err(report_name_conflict(class_id, new_name));
            }
            // C: LogicalRepWorkersWakeupAtCommit(objectId) — fire-and-forget
            // worker wakeup; no logical replication workers exist here.
        }
        _ => unreachable!(),
    }

    let puballtables = if class_id == PublicationRelationId {
        getattr(td, oldtup, Anum_pg_publication_puballtables).0.as_bool()
    } else {
        false
    };

    let natts = td.natts as usize;
    let mut repl_values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl_isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut repl: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    repl_values.resize(natts, Datum::null());
    repl_isnull.resize(natts, false);
    repl.resize(natts, false);
    let nname = name_arg(mcx, new_name)?;
    repl_values[(props.name_attnum - 1) as usize] = Datum::from_usize(nname.as_ptr() as usize);
    repl[(props.name_attnum - 1) as usize] = true;

    let mut newtup = heaptuple::heap_modify_tuple(mcx, oldtup, td, &repl_values, &repl_isnull, &repl)?;
    let otid = oldtup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, rel, &otid, &mut newtup)?;

    if class_id == PublicationRelationId {
        commands_publicationcmds::InvalidatePubRelSyncCache(mcx, object_id, puballtables)?;
    }

    Ok(())
}

pub fn ExecRenameStmt_generic<'mcx>(mcx: Mcx<'mcx>, stmt: &RenameStmt<'mcx>) -> PgResult<ObjectAddress> {
    debug_assert!(matches!(
        stmt.renameType,
        ObjectType::OBJECT_PUBLICATION | ObjectType::OBJECT_SUBSCRIPTION
    ));
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
        other => panic!("ExecAlterOwnerStmt (alter.c): object type {other:?} unported"),
    }
}
