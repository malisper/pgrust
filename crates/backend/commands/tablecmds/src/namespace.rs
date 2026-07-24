// AlterTableNamespace lane (tablecmds.c): ALTER TABLE/SEQUENCE/VIEW/MATVIEW/
// FOREIGN TABLE ... SET SCHEMA.
use datum::Datum;
use mcx::{Mcx, PgVec};
use pg_depend::ObjectAddress;
use types_core::{InvalidOid, Oid, RELATION_RELATION_ID};
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_TABLE, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR, NOTICE,
};
use types_nodes::parsenodes::{AlterObjectSchemaStmt, ObjectType};
use types_rel::{AccessExclusiveLock, InplaceUpdateTupleLock, NoLock, Relation, RowExclusiveLock, LOCKMODE};

use crate::alter::{oid_scankey, AlterRelationStmtKind, AlterTableLookupRangeVar, NamespaceRelationId};

const Anum_pg_class_relnamespace: usize = 3;
const Anum_pg_depend_classid: usize = 1;
const Anum_pg_depend_objid: usize = 2;
const Anum_pg_depend_objsubid: usize = 3;
const Anum_pg_depend_refclassid: usize = 4;
const Anum_pg_depend_refobjid: usize = 5;
const Anum_pg_depend_refobjsubid: usize = 6;
const Anum_pg_depend_deptype: usize = 7;

fn object_present(objs_moved: &PgVec<'_, ObjectAddress>, obj: &ObjectAddress) -> bool {
    objs_moved.iter().any(|a| {
        a.classId == obj.classId && a.objectId == obj.objectId && a.objectSubId == obj.objectSubId
    })
}

pub fn AlterTableNamespace<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterObjectSchemaStmt<'_>,
) -> PgResult<()> {
    let relid = AlterTableLookupRangeVar(
        mcx,
        stmt.relation.expect("AlterObjectSchemaStmt.relation"),
        AccessExclusiveLock,
        stmt.missing_ok,
        stmt.objectType,
        AlterRelationStmtKind::AlterObjectSchema,
    )?;
    if relid == InvalidOid {
        elog_seams::ereport_msg::call(
            NOTICE,
            format!(
                "relation \"{}\" does not exist, skipping",
                stmt.relation.and_then(|r| r.relname).unwrap_or("")
            ),
            None,
        )?;
        return Ok(());
    }

    let rel = relation_seams::relation_open::call(mcx, relid, NoLock)?;
    let old_nsp_oid = rel.rd_rel.relnamespace;

    if rel.rd_rel.relkind == types_rel::RELKIND_SEQUENCE {
        let owned = pg_depend::sequenceIsOwned(mcx, relid, pg_depend::DependencyType::Auto)?
            .or(pg_depend::sequenceIsOwned(mcx, relid, pg_depend::DependencyType::Internal)?);
        if let Some((table_id, _col_id)) = owned {
            let tabname = lsyscache::get_rel_name(mcx, table_id)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();
            return Err(Box::new(
                PgError::new(ERROR, "cannot move an owned sequence into another schema".to_string())
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .with_detail(format!(
                        "Sequence \"{}\" is linked to table \"{tabname}\".",
                        rel.name()
                    )),
            ));
        }
    }

    // Get and lock schema OID and check its permissions (tablecmds.c:18991).
    let newrv = rel_vocab::RangeVar {
        catalogname: None,
        schemaname: Some(stmt.newschema.expect("AlterObjectSchemaStmt.newschema")),
        relname: rel.name(),
        inh: false,
        relpersistence: types_core::RELPERSISTENCE_PERMANENT,
        location: -1,
    };
    let (nsp_oid, _existing_relid, _relpersistence) =
        catalog_namespace::RangeVarGetAndCheckCreationNamespace(mcx, &newrv, types_rel::NoLock, false)?;

    catalog_namespace::CheckSetNamespace(old_nsp_oid, nsp_oid)?;

    let mut objs_moved: PgVec<'mcx, ObjectAddress> = PgVec::new_in(mcx);
    AlterTableNamespaceInternal(mcx, &rel, old_nsp_oid, nsp_oid, &mut objs_moved)?;

    rel.close(NoLock)
}

pub fn AlterTableNamespaceInternal<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    old_nsp_oid: Oid,
    nsp_oid: Oid,
    objs_moved: &mut PgVec<'mcx, ObjectAddress>,
) -> PgResult<()> {
    let class_rel = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;

    AlterRelationNamespaceInternal(
        mcx, &class_rel, rel.rd_id, old_nsp_oid, nsp_oid, true, objs_moved,
    )?;

    if rel.rd_rel.reltype != InvalidOid {
        typecmds_seams::alter_type_namespace_internal::call(
            mcx,
            rel.rd_rel.reltype,
            nsp_oid,
            false,
            false,
            false,
            objs_moved,
        )?;
    }

    AlterIndexNamespaces(mcx, &class_rel, rel, old_nsp_oid, nsp_oid, objs_moved)?;
    AlterSeqNamespaces(
        mcx, &class_rel, rel, old_nsp_oid, nsp_oid, objs_moved, AccessExclusiveLock,
    )?;
    pg_constraint::AlterConstraintNamespaces(
        mcx, rel.rd_id, old_nsp_oid, nsp_oid, false, objs_moved,
    )?;

    class_rel.close(RowExclusiveLock)
}

pub fn AlterRelationNamespaceInternal<'mcx>(
    mcx: Mcx<'mcx>,
    class_rel: &Relation<'mcx>,
    rel_oid: Oid,
    old_nsp_oid: Oid,
    new_nsp_oid: Oid,
    has_depend_entry: bool,
    objs_moved: &mut PgVec<'mcx, ObjectAddress>,
) -> PgResult<()> {
    let thisobj = ObjectAddress::set(RELATION_RELATION_ID, rel_oid);
    let already_done = object_present(objs_moved, &thisobj);

    if !already_done && old_nsp_oid != new_nsp_oid {
        let relname = lsyscache::get_rel_name(mcx, rel_oid)?
            .unwrap_or_else(|| panic!("cache lookup failed for relation {rel_oid}"));
        if lsyscache::get_relname_relid(&relname, new_nsp_oid)? != InvalidOid {
            let nspname = lsyscache::get_namespace_name(mcx, new_nsp_oid)?
                .map(|s| s.as_str().to_string())
                .unwrap_or_default();
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "relation \"{}\" already exists in schema \"{nspname}\"",
                        relname.as_str()
                    ),
                )
                .with_sqlstate(ERRCODE_DUPLICATE_TABLE),
            ));
        }

        let key = oid_scankey(1, rel_oid);
        let mut scan = genam::systable_beginscan(
            mcx,
            class_rel,
            catalog::ClassOidIndexId,
            true,
            None,
            &[key],
        )?;
        let classtup = genam::systable_getnext(mcx, &mut scan)?
            .unwrap_or_else(|| panic!("cache lookup failed for relation {rel_oid}"));
        // C: SearchSysCacheLockedCopy1 (tablecmds.c:19065) / UnlockTuple
        // (:19099, and :19113 on the already-in-that-schema early exit).
        let otid = classtup.t_self;
        lmgr::LockTuple(class_rel, &otid, InplaceUpdateTupleLock)?;
        let desc = class_rel.descr();
        let n = desc.natts as usize;
        let mut values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, n)?;
        let mut nulls: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, n)?;
        let mut replace: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, n)?;
        values.resize(n, Datum::null());
        nulls.resize(n, false);
        replace.resize(n, false);
        values[Anum_pg_class_relnamespace - 1] = Datum::from_oid(new_nsp_oid);
        replace[Anum_pg_class_relnamespace - 1] = true;
        let mut newtup =
            heaptuple::heap_modify_tuple(mcx, classtup, desc, &values, &nulls, &replace)?;
        genam::systable_endscan(mcx, scan)?;
        catalog_indexing::CatalogTupleUpdate(mcx, class_rel, &otid, &mut newtup)?;
        lmgr::UnlockTuple(class_rel, &otid, InplaceUpdateTupleLock)?;

        if has_depend_entry
            && pg_depend::changeDependencyFor(
                mcx,
                RELATION_RELATION_ID,
                rel_oid,
                NamespaceRelationId,
                old_nsp_oid,
                new_nsp_oid,
            )? != 1
        {
            panic!(
                "could not change schema dependency for relation \"{}\"",
                relname.as_str()
            );
        }
    }
    if !already_done {
        objs_moved.push(thisobj);
    }
    Ok(())
}

fn AlterIndexNamespaces<'mcx>(
    mcx: Mcx<'mcx>,
    class_rel: &Relation<'mcx>,
    rel: &Relation<'mcx>,
    old_nsp_oid: Oid,
    new_nsp_oid: Oid,
    objs_moved: &mut PgVec<'mcx, ObjectAddress>,
) -> PgResult<()> {
    let index_list = relcache::indexlist::RelationGetIndexList(mcx, rel.rd_id)?;
    for &index_oid in index_list.iter() {
        let thisobj = ObjectAddress::set(RELATION_RELATION_ID, index_oid);
        // Indexes carry no namespace dependency and no pg_type row.
        if !object_present(objs_moved, &thisobj) {
            AlterRelationNamespaceInternal(
                mcx, class_rel, index_oid, old_nsp_oid, new_nsp_oid, false, objs_moved,
            )?;
        }
    }
    Ok(())
}

fn AlterSeqNamespaces<'mcx>(
    mcx: Mcx<'mcx>,
    class_rel: &Relation<'mcx>,
    rel: &Relation<'mcx>,
    old_nsp_oid: Oid,
    new_nsp_oid: Oid,
    objs_moved: &mut PgVec<'mcx, ObjectAddress>,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    // SERIAL/identity sequences are AUTO/INTERNAL column dependencies of the
    // relation; refobjsubid keys off the column.
    let dep_rel = table::table_open(mcx, pg_depend::DependRelationId, types_rel::AccessShareLock)?;
    let keys = [
        oid_scankey(Anum_pg_depend_refclassid, RELATION_RELATION_ID),
        oid_scankey(Anum_pg_depend_refobjid, rel.rd_id),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &dep_rel,
        pg_depend::DependReferenceIndexId,
        true,
        None,
        &keys,
    )?;
    let desc = dep_rel.descr();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let get = |attno: usize| {
            let mut isnull = false;
            // SAFETY: fixed NOT NULL pg_depend columns under its descriptor.
            unsafe { types_tuple::heap_getattr(tup, attno as i32, desc, &mut isnull) }
        };
        let refobjsubid = get(Anum_pg_depend_refobjsubid).as_i32();
        let classid = get(Anum_pg_depend_classid).as_oid();
        let objid = get(Anum_pg_depend_objid).as_oid();
        let objsubid = get(Anum_pg_depend_objsubid).as_i32();
        let deptype = get(Anum_pg_depend_deptype).as_i8();
        if refobjsubid == 0
            || classid != RELATION_RELATION_ID
            || objsubid != 0
            || !(deptype == pg_depend::DependencyType::Auto.as_char()
                || deptype == pg_depend::DependencyType::Internal.as_char())
        {
            continue;
        }

        let seq_rel = relation_seams::relation_open::call(mcx, objid, lockmode)?;
        if seq_rel.rd_rel.relkind != types_rel::RELKIND_SEQUENCE {
            seq_rel.close(lockmode)?;
            continue;
        }

        AlterRelationNamespaceInternal(
            mcx, class_rel, objid, old_nsp_oid, new_nsp_oid, true, objs_moved,
        )?;

        debug_assert!(seq_rel.rd_rel.reltype == InvalidOid);
        seq_rel.close(NoLock)?;
    }
    genam::systable_endscan(mcx, scan)?;
    dep_rel.close(types_rel::AccessShareLock)
}
