// ATExecChangeOwner + change_owner_fix_column_acls +
// change_owner_recurse_to_sequences (tablecmds.c).
use datum::Datum;
use mcx::Mcx;
use types_core::{InvalidOid, Oid, OidIsValid, NAMESPACE_RELATION_ID, RELATION_RELATION_ID};
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_WRONG_OBJECT_TYPE, ERROR, WARNING,
};
use types_rel::{NoLock, Relation, RowExclusiveLock, LOCKMODE};

use crate::alter::oid_scankey;

const Anum_pg_class_relowner: usize = 6;
const Anum_pg_class_relacl: usize = 32;
const Anum_pg_attribute_attacl: usize = 22;
const AttributeRelidNumIndexId: Oid = 2659;
const DependRelationId: Oid = 2608;
const DependReferenceIndexId: Oid = 2674;
const Anum_pg_depend_classid: i32 = 1;
const Anum_pg_depend_objid: i32 = 2;
const Anum_pg_depend_objsubid: i32 = 3;
const Anum_pg_depend_refclassid: usize = 4;
const Anum_pg_depend_refobjid: usize = 5;
const Anum_pg_depend_refobjsubid: i32 = 6;
const Anum_pg_depend_deptype: i32 = 7;

pub(crate) fn ATExecChangeOwner<'mcx>(
    mcx: Mcx<'mcx>,
    relation_oid: Oid,
    new_owner_id: Oid,
    recursing: bool,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let mut new_owner_id = new_owner_id;
    let target_rel = relation_seams::relation_open::call(mcx, relation_oid, lockmode)?;
    let class_rel = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;

    let relkind = target_rel.rd_rel.relkind;
    let old_owner_id = target_rel.rd_rel.relowner;
    let relname = target_rel.name().to_string();

    match relkind {
        types_rel::RELKIND_RELATION
        | types_rel::RELKIND_VIEW
        | types_rel::RELKIND_MATVIEW
        | types_rel::RELKIND_FOREIGN_TABLE
        | types_rel::RELKIND_PARTITIONED_TABLE => {}
        types_rel::RELKIND_INDEX => {
            if !recursing {
                if old_owner_id != new_owner_id {
                    elog_seams::ereport::call(
                        PgError::new(
                            WARNING,
                            format!("cannot change owner of index \"{relname}\""),
                        )
                        .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
                        .with_hint("Change the ownership of the index's table instead."),
                    )?;
                }
                new_owner_id = old_owner_id;
            }
        }
        types_rel::RELKIND_PARTITIONED_INDEX if recursing => {}
        types_rel::RELKIND_PARTITIONED_INDEX => {
            return Err(Box::new(
                PgError::new(ERROR, format!("cannot change owner of index \"{relname}\""))
                    .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
                    .with_hint("Change the ownership of the index's table instead."),
            ));
        }
        types_rel::RELKIND_SEQUENCE => {
            if !recursing && old_owner_id != new_owner_id {
                let owned = pg_depend::sequenceIsOwned(
                    mcx,
                    relation_oid,
                    pg_depend::DependencyType::Auto,
                )?
                .or(pg_depend::sequenceIsOwned(
                    mcx,
                    relation_oid,
                    pg_depend::DependencyType::Internal,
                )?);
                if let Some((table_id, _col_id)) = owned {
                    let tabname = lsyscache::get_rel_name(mcx, table_id)?
                        .map(|n| n.to_string())
                        .unwrap_or_default();
                    return Err(Box::new(
                        PgError::new(
                            ERROR,
                            format!("cannot change owner of sequence \"{relname}\""),
                        )
                        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .with_detail(format!(
                            "Sequence \"{relname}\" is linked to table \"{tabname}\"."
                        )),
                    ));
                }
            }
        }
        types_rel::RELKIND_COMPOSITE_TYPE if recursing => {}
        types_rel::RELKIND_COMPOSITE_TYPE => {
            return Err(Box::new(
                PgError::new(ERROR, format!("\"{relname}\" is a composite type"))
                    .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
                    .with_hint("Use ALTER TYPE instead."),
            ));
        }
        types_rel::RELKIND_TOASTVALUE if recursing => {}
        _ => {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!("cannot change owner of relation \"{relname}\""),
                )
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
                .with_detail(pg_class_seams::errdetail_relkind_not_supported::call(
                    relkind,
                )?),
            ));
        }
    }

    if old_owner_id != new_owner_id {
        if !recursing && !superuser::superuser()? {
            if !aclchk::object_ownercheck(
                RELATION_RELATION_ID,
                relation_oid,
                miscinit::GetUserId(),
            )? {
                aclchk::aclcheck_error(
                    aclchk::ACLCHECK_NOT_OWNER,
                    crate::get_relkind_objtype(relkind),
                    &relname,
                )?;
            }
            if !adt_acl::member_can_set_role(miscinit::GetUserId(), new_owner_id)? {
                let rolename = miscinit::GetUserNameFromId(mcx, new_owner_id, false)?
                    .map(|n| n.to_string())
                    .unwrap_or_default();
                return Err(Box::new(
                    PgError::new(ERROR, format!("must be able to SET ROLE \"{rolename}\""))
                        .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
                ));
            }
            let namespace_oid = target_rel.rd_rel.relnamespace;
            let aclresult = aclchk::object_aclcheck(
                NAMESPACE_RELATION_ID,
                namespace_oid,
                new_owner_id,
                adt_acl::ACL_CREATE,
            )?;
            if aclresult != aclchk::ACLCHECK_OK {
                let nsname = lsyscache::misc::get_namespace_name(mcx, namespace_oid)?
                    .map(|n| n.to_string())
                    .unwrap_or_default();
                aclchk::aclcheck_error(
                    aclresult,
                    types_nodes::parsenodes::ObjectType::OBJECT_SCHEMA,
                    &nsname,
                )?;
            }
        }

        {
            let keys = [oid_scankey(1, relation_oid)];
            let mut scan = genam::systable_beginscan(
                mcx,
                &class_rel,
                catalog::ClassOidIndexId,
                true,
                None,
                &keys,
            )?;
            let tup = genam::systable_getnext(mcx, &mut scan)?
                .unwrap_or_else(|| panic!("cache lookup failed for relation {relation_oid}"));
            let desc = class_rel.descr();
            let natts = desc.natts as usize;
            let mut values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
            let mut isnull: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
            let mut replace: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
            values.resize(natts, Datum::null());
            isnull.resize(natts, false);
            replace.resize(natts, false);
            values[Anum_pg_class_relowner - 1] = Datum::from_oid(new_owner_id);
            replace[Anum_pg_class_relowner - 1] = true;

            let mut acl_null = false;
            // SAFETY: relacl read under the open scan's held tuple.
            let acl_datum = unsafe {
                types_tuple::heap_getattr(tup, Anum_pg_class_relacl as i32, desc, &mut acl_null)
            };
            let acl_img;
            if !acl_null {
                let new_acl = aclchk::with_acl_datum(acl_datum, |acl| {
                    adt_acl::aclnewowner(mcx, acl, old_owner_id, new_owner_id)
                })?;
                acl_img = adt_acl::varlena::acl_image(mcx, &new_acl)?;
                values[Anum_pg_class_relacl - 1] =
                    Datum::from_usize(acl_img.as_ptr() as usize);
                replace[Anum_pg_class_relacl - 1] = true;
            }

            let mut newtup =
                heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &isnull, &replace)?;
            let otid = tup.t_self;
            genam::systable_endscan(mcx, scan)?;
            catalog_indexing::CatalogTupleUpdate(mcx, &class_rel, &otid, &mut newtup)?;
        }

        change_owner_fix_column_acls(mcx, relation_oid, old_owner_id, new_owner_id)?;

        if relkind != types_rel::RELKIND_COMPOSITE_TYPE
            && relkind != types_rel::RELKIND_INDEX
            && relkind != types_rel::RELKIND_PARTITIONED_INDEX
            && relkind != types_rel::RELKIND_TOASTVALUE
        {
            pg_shdepend::changeDependencyOnOwner(
                mcx,
                RELATION_RELATION_ID,
                relation_oid,
                new_owner_id,
            )?;
        }

        if OidIsValid(target_rel.rd_rel.reltype) {
            typecmds_seams::alter_type_owner_internal::call(
                mcx,
                target_rel.rd_rel.reltype,
                new_owner_id,
            )?;
        }

        if relkind == types_rel::RELKIND_RELATION
            || relkind == types_rel::RELKIND_PARTITIONED_TABLE
            || relkind == types_rel::RELKIND_MATVIEW
            || relkind == types_rel::RELKIND_TOASTVALUE
        {
            for &index_oid in relcache::RelationGetIndexList(mcx, relation_oid)?.iter() {
                ATExecChangeOwner(mcx, index_oid, new_owner_id, true, lockmode)?;
            }
        }

        if target_rel.rd_rel.reltoastrelid != InvalidOid {
            ATExecChangeOwner(mcx, target_rel.rd_rel.reltoastrelid, new_owner_id, true, lockmode)?;
        }

        change_owner_recurse_to_sequences(mcx, relation_oid, new_owner_id, lockmode)?;
    }

    class_rel.close(RowExclusiveLock)?;
    target_rel.close(NoLock)
}

fn change_owner_fix_column_acls<'mcx>(
    mcx: Mcx<'mcx>,
    relation_oid: Oid,
    old_owner_id: Oid,
    new_owner_id: Oid,
) -> PgResult<()> {
    let att_rel =
        table::table_open(mcx, types_core::ATTRIBUTE_RELATION_ID, RowExclusiveLock)?;
    let keys = [oid_scankey(1, relation_oid)];
    let mut scan =
        genam::systable_beginscan(mcx, &att_rel, AttributeRelidNumIndexId, true, None, &keys)?;
    let desc = att_rel.descr();
    let natts = desc.natts as usize;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY (each): fixed pg_attribute columns under its descriptor.
        let dropped = unsafe { types_tuple::heap_getattr(tup, 17, desc, &mut isnull) }.as_bool();
        if dropped {
            continue;
        }
        let mut acl_null = false;
        // SAFETY: as above; attacl is nullable, guarded by acl_null.
        let acl_datum = unsafe {
            types_tuple::heap_getattr(tup, Anum_pg_attribute_attacl as i32, desc, &mut acl_null)
        };
        if acl_null {
            continue;
        }
        let new_acl = aclchk::with_acl_datum(acl_datum, |acl| {
            adt_acl::aclnewowner(mcx, acl, old_owner_id, new_owner_id)
        })?;
        let acl_img = adt_acl::varlena::acl_image(mcx, &new_acl)?;

        let mut values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut nulls: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        let mut replace: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
        values.resize(natts, Datum::null());
        nulls.resize(natts, false);
        replace.resize(natts, false);
        values[Anum_pg_attribute_attacl - 1] = Datum::from_usize(acl_img.as_ptr() as usize);
        replace[Anum_pg_attribute_attacl - 1] = true;
        let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
        let otid = tup.t_self;
        catalog_indexing::CatalogTupleUpdate(mcx, &att_rel, &otid, &mut newtup)?;
    }
    genam::systable_endscan(mcx, scan)?;
    att_rel.close(RowExclusiveLock)
}

fn change_owner_recurse_to_sequences<'mcx>(
    mcx: Mcx<'mcx>,
    relation_oid: Oid,
    new_owner_id: Oid,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    let dep_rel = table::table_open(mcx, DependRelationId, types_rel::AccessShareLock)?;
    let keys = [
        oid_scankey(Anum_pg_depend_refclassid, RELATION_RELATION_ID),
        oid_scankey(Anum_pg_depend_refobjid, relation_oid),
    ];
    let mut scan =
        genam::systable_beginscan(mcx, &dep_rel, DependReferenceIndexId, true, None, &keys)?;
    let desc = dep_rel.descr();
    let mut seq_oids: mcx::PgVec<'_, Oid> = mcx::PgVec::new_in(mcx);
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY (each): fixed NOT NULL pg_depend columns under its descriptor.
        let refobjsubid =
            unsafe { types_tuple::heap_getattr(tup, Anum_pg_depend_refobjsubid, desc, &mut isnull) }
                .as_i32();
        let classid =
            unsafe { types_tuple::heap_getattr(tup, Anum_pg_depend_classid, desc, &mut isnull) }
                .as_oid();
        let objid =
            unsafe { types_tuple::heap_getattr(tup, Anum_pg_depend_objid, desc, &mut isnull) }
                .as_oid();
        let objsubid =
            unsafe { types_tuple::heap_getattr(tup, Anum_pg_depend_objsubid, desc, &mut isnull) }
                .as_i32();
        let deptype =
            unsafe { types_tuple::heap_getattr(tup, Anum_pg_depend_deptype, desc, &mut isnull) }
                .as_i8() as u8;
        if refobjsubid == 0
            || classid != RELATION_RELATION_ID
            || objsubid != 0
            || !(deptype == b'a' || deptype == b'i')
        {
            continue;
        }
        seq_oids.push(objid);
    }
    genam::systable_endscan(mcx, scan)?;

    for &objid in seq_oids.iter() {
        let seq_rel = relation_seams::relation_open::call(mcx, objid, lockmode)?;
        if seq_rel.rd_rel.relkind != types_rel::RELKIND_SEQUENCE {
            seq_rel.close(lockmode)?;
            continue;
        }
        ATExecChangeOwner(mcx, objid, new_owner_id, true, lockmode)?;
        seq_rel.close(NoLock)?;
    }

    dep_rel.close(types_rel::AccessShareLock)
}
