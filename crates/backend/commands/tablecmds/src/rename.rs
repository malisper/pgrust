// renameatt / RenameRelation lane (tablecmds.c). LOUD: inheritance children,
// non-table relkinds (except toast/index rides from cluster), constraint
// renames.
use datum::Datum;
use mcx::Mcx;
use types_core::{InvalidOid, Oid, RELATION_RELATION_ID};
use types_error::{
    PgError, PgResult, ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST, ERRCODE_DUPLICATE_TABLE,
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_TABLE_DEFINITION, ERRCODE_UNDEFINED_COLUMN,
    ERRCODE_WRONG_OBJECT_TYPE, ERROR, NOTICE,
};
use types_nodes::parsenodes::{DropBehavior, RenameStmt};
use types_rel::{AccessExclusiveLock, InplaceUpdateTupleLock, NoLock, RowExclusiveLock, ShareUpdateExclusiveLock, RELKIND_RELATION};

use crate::alter::{
    check_for_column_name_collision, update_pg_attribute, Anum_pg_attribute_attname,
};

fn renameatt_check<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    relkind: u8,
    relnamespace: Oid,
    relname: &str,
    recursing: bool,
) -> PgResult<()> {
    let reloftype = crate::alter::pg_class_read_attr(
        mcx,
        relid,
        crate::alter::Anum_pg_class_reloftype,
    )?
    .as_oid();
    if reloftype != InvalidOid && !recursing {
        return Err(Box::new(
            PgError::new(ERROR, "cannot rename column of typed table".to_string())
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }
    if !matches!(
        relkind,
        RELKIND_RELATION
            | types_rel::RELKIND_VIEW
            | types_rel::RELKIND_MATVIEW
            | types_rel::RELKIND_COMPOSITE_TYPE
            | types_rel::RELKIND_INDEX
            | types_rel::RELKIND_PARTITIONED_INDEX
            | types_rel::RELKIND_FOREIGN_TABLE
            | types_rel::RELKIND_PARTITIONED_TABLE
    ) {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("cannot rename columns of relation \"{relname}\""),
            )
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
            .with_detail(pg_class_seams::errdetail_relkind_not_supported::call(relkind)?),
        ));
    }
    if !aclchk::object_ownercheck(RELATION_RELATION_ID, relid, miscinit::GetUserId())? {
        aclchk::aclcheck_error(
            aclchk::ACLCHECK_NOT_OWNER,
            crate::get_relkind_objtype(relkind),
            relname,
        )?;
    }
    let is_system =
        catalog::IsCatalogRelationOid(relid) || catalog::IsToastNamespace(relnamespace);
    if is_system && !init_small::globals::allowSystemTableMods() {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("permission denied: \"{relname}\" is a system catalog"),
            )
            .with_sqlstate(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }
    Ok(())
}

fn rename_lookup_rangevar<'mcx>(
    mcx: Mcx<'mcx>,
    prv: &types_nodes::primnodes::RangeVar<'_>,
    missing_ok: bool,
) -> PgResult<Oid> {
    let rv = rel_vocab::RangeVar {
        catalogname: prv.catalogname,
        schemaname: prv.schemaname,
        relname: prv.relname.expect("RangeVar.relname"),
        inh: prv.inh,
        relpersistence: prv.relpersistence,
        location: prv.location,
    };
    let mut callback = |rv: &rel_vocab::RangeVar<'_>, relOid: Oid, _old: Oid| {
        RangeVarCallbackForRenameAttribute(mcx, rv, relOid)
    };
    let flags = if missing_ok { catalog_namespace::RVR_MISSING_OK } else { 0 };
    catalog_namespace::RangeVarGetRelidExtended(
        &rv,
        AccessExclusiveLock,
        flags,
        Some(&mut callback),
    )
}

fn RangeVarCallbackForRenameAttribute<'mcx>(
    mcx: Mcx<'mcx>,
    rv: &rel_vocab::RangeVar<'_>,
    relid: Oid,
) -> PgResult<()> {
    if relid == InvalidOid {
        return Ok(());
    }
    let Some((relkind, relnamespace)) = pg_class_kind_namespace(mcx, relid)? else {
        return Ok(()); // concurrently dropped
    };
    renameatt_check(mcx, relid, relkind, relnamespace, rv.relname, false)
}

fn pg_class_kind_namespace(mcx: Mcx<'_>, relid: Oid) -> PgResult<Option<(u8, Oid)>> {
    let pg_class = table::table_open(mcx, RELATION_RELATION_ID, types_rel::AccessShareLock)?;
    let key = crate::alter::oid_scankey(1, relid);
    let mut scan =
        genam::systable_beginscan(mcx, &pg_class, catalog::ClassOidIndexId, true, None, &[key])?;
    let out = match genam::systable_getnext(mcx, &mut scan)? {
        Some(tup) => {
            let desc = pg_class.descr();
            let mut isnull = false;
            // SAFETY (each): fixed NOT NULL pg_class columns under its
            // descriptor.
            let relnamespace =
                unsafe { types_tuple::heap_getattr(tup, 3, desc, &mut isnull) }.as_oid();
            // SAFETY: as above.
            let relkind =
                unsafe { types_tuple::heap_getattr(tup, 18, desc, &mut isnull) }.as_i8() as u8;
            Some((relkind, relnamespace))
        }
        None => None,
    };
    genam::systable_endscan(mcx, scan)?;
    pg_class.close(types_rel::AccessShareLock)?;
    Ok(out)
}

// find_typed_table_dependencies (tablecmds.c): typed tables of a composite
// type, or error under RESTRICT.
pub(crate) fn find_typed_table_dependencies<'mcx>(
    mcx: Mcx<'mcx>,
    type_oid: Oid,
    type_name: &str,
    behavior: DropBehavior,
) -> PgResult<mcx::PgVec<'mcx, Oid>> {
    let class_rel = table::table_open(mcx, RELATION_RELATION_ID, types_rel::AccessShareLock)?;
    let key = crate::alter::oid_scankey(crate::alter::Anum_pg_class_reloftype, type_oid);
    let mut scan = genam::systable_beginscan(mcx, &class_rel, InvalidOid, false, None, &[key])?;
    let desc = class_rel.descr();
    let mut result: mcx::PgVec<'mcx, Oid> = mcx::PgVec::new_in(mcx);
    let mut found_restrict = false;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        if behavior == DropBehavior::DROP_RESTRICT {
            found_restrict = true;
            break;
        }
        let mut isnull = false;
        // SAFETY: pg_class oid column under its descriptor.
        let oid = unsafe { types_tuple::heap_getattr(tup, 1, desc, &mut isnull) }.as_oid();
        result.push(oid);
    }
    genam::systable_endscan(mcx, scan)?;
    class_rel.close(types_rel::AccessShareLock)?;
    if found_restrict {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "cannot alter type \"{type_name}\" because it is the type of a typed table"
                ),
            )
            .with_sqlstate(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST)
            .with_hint("Use ALTER ... CASCADE to alter the typed tables too."),
        ));
    }
    Ok(result)
}

// renameatt: ALTER TABLE ... RENAME [COLUMN] ... TO ...
pub fn renameatt<'mcx>(mcx: Mcx<'mcx>, stmt: &RenameStmt<'_>) -> PgResult<()> {
    let relid = rename_lookup_rangevar(
        mcx,
        stmt.relation.expect("RenameStmt.relation"),
        stmt.missing_ok,
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
    renameatt_internal(
        mcx,
        relid,
        stmt.subname.expect("RenameStmt.subname"),
        stmt.newname.expect("RenameStmt.newname"),
        stmt.relation.expect("RenameStmt.relation").inh,
        false,
        0,
        stmt.behavior,
    )
}

fn renameatt_internal<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    oldattname: &str,
    newattname: &str,
    recurse: bool,
    recursing: bool,
    expected_parents: i32,
    behavior: DropBehavior,
) -> PgResult<()> {
    let rel = relation_seams::relation_open::call(mcx, relid, AccessExclusiveLock)?;
    renameatt_check(
        mcx,
        relid,
        rel.rd_rel.relkind,
        rel.rd_rel.relnamespace,
        rel.name(),
        recursing,
    )?;
    if recurse {
        let (child_oids, child_numparents) =
            pg_inherits::find_all_inheritors_numparents(mcx, relid, AccessExclusiveLock)?;
        for (i, &childrelid) in child_oids.iter().enumerate() {
            if childrelid == relid {
                continue;
            }
            renameatt_internal(
                mcx,
                childrelid,
                oldattname,
                newattname,
                false,
                true,
                child_numparents[i],
                behavior,
            )?;
        }
    } else if expected_parents == 0
        && !pg_inherits::find_inheritance_children(mcx, relid, types_rel::NoLock)?.is_empty()
    {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "inherited column \"{oldattname}\" must be renamed in child tables too"
                ),
            )
            .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
        ));
    }
    let relname = rel.name().to_string();
    if rel.rd_rel.relkind == types_rel::RELKIND_COMPOSITE_TYPE {
        let child_oids =
            find_typed_table_dependencies(mcx, rel.rd_rel.reltype, &relname, behavior)?;
        for &childrelid in child_oids.iter() {
            renameatt_internal(
                mcx, childrelid, oldattname, newattname, true, true, 0, behavior,
            )?;
        }
    }
    let Some((attnum, attinhcount)) = attname_lookup_local(mcx, relid, oldattname)? else {
        return Err(Box::new(
            PgError::new(ERROR, format!("column \"{oldattname}\" does not exist"))
                .with_sqlstate(ERRCODE_UNDEFINED_COLUMN),
        ));
    };
    if attnum <= 0 {
        return Err(Box::new(
            PgError::new(ERROR, format!("cannot rename system column \"{oldattname}\""))
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    if attinhcount as i32 > expected_parents {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("cannot rename inherited column \"{oldattname}\""),
            )
            .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
        ));
    }
    check_for_column_name_collision(mcx, relid, &relname, newattname, false)?;
    let namebuf = name_datum(mcx, newattname)?;
    update_pg_attribute(
        mcx,
        relid,
        attnum,
        &[(Anum_pg_attribute_attname, Datum::from_usize(namebuf.as_ptr() as usize))],
    )?;
    rel.close(NoLock)
}

fn attname_lookup_local<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    colname: &str,
) -> PgResult<Option<(i16, i16)>> {
    crate::alter::attname_lookup(mcx, relid, colname, false)
}

// RenameConstraint + rename_constraint_internal (tablecmds.c), relation arm;
// domain constraints ride the typecmds lane.
pub fn RenameConstraint<'mcx>(mcx: Mcx<'mcx>, stmt: &RenameStmt<'_>) -> PgResult<()> {
    let relid = rename_lookup_rangevar(
        mcx,
        stmt.relation.expect("RenameStmt.relation"),
        stmt.missing_ok,
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
    rename_constraint_internal(
        mcx,
        relid,
        stmt.subname.expect("RenameStmt.subname"),
        stmt.newname.expect("RenameStmt.newname"),
        stmt.relation.map(|r| r.inh).unwrap_or(false),
        false,
        0,
    )
}

fn rename_constraint_internal<'mcx>(
    mcx: Mcx<'mcx>,
    myrelid: types_core::Oid,
    oldconname: &str,
    newconname: &str,
    recurse: bool,
    recursing: bool,
    expected_parents: i32,
) -> PgResult<()> {
    let rel = relation_seams::relation_open::call(mcx, myrelid, AccessExclusiveLock)?;
    let _ = recursing;
    renameatt_check(
        mcx,
        myrelid,
        rel.rd_rel.relkind,
        rel.rd_rel.relnamespace,
        rel.name(),
        false,
    )?;
    let relname = rel.name().to_string();
    let Some(con) = pg_constraint::findConstraintByName(mcx, myrelid, oldconname)? else {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "constraint \"{oldconname}\" for table \"{relname}\" does not exist"
                ),
            )
            .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT),
        ));
    };
    if matches!(
        con.contype,
        pg_constraint::CONSTRAINT_CHECK | pg_constraint::CONSTRAINT_NOTNULL
    ) && !con.connoinherit
    {
        if recurse {
            let (child_oids, child_numparents) =
                pg_inherits::find_all_inheritors_numparents(mcx, myrelid, AccessExclusiveLock)?;
            for (i, &childrelid) in child_oids.iter().enumerate() {
                if childrelid == myrelid {
                    continue;
                }
                rename_constraint_internal(
                    mcx,
                    childrelid,
                    oldconname,
                    newconname,
                    false,
                    true,
                    child_numparents[i],
                )?;
            }
        } else if expected_parents == 0
            && !pg_inherits::find_inheritance_children(mcx, myrelid, types_rel::NoLock)?
                .is_empty()
        {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!(
                        "inherited constraint \"{oldconname}\" must be renamed in child \
                         tables too"
                    ),
                )
                .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
            ));
        }
        if con.coninhcount as i32 > expected_parents {
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!("cannot rename inherited constraint \"{oldconname}\""),
                )
                .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
            ));
        }
    }
    if con.conindid != InvalidOid
        && matches!(
            con.contype,
            pg_constraint::CONSTRAINT_PRIMARY
                | pg_constraint::CONSTRAINT_UNIQUE
                | pg_constraint::CONSTRAINT_EXCLUSION
        )
    {
        // Renaming the index renames the constraint as well.
        RenameRelationInternal(mcx, con.conindid, newconname, true)?;
    } else {
        pg_constraint::RenameConstraintById(mcx, con.oid, newconname)?;
    }
    inval::invalidate::CacheInvalidateRelcacheByRelid(myrelid)?;
    rel.close(NoLock)
}

// RenameRelation: ALTER TABLE/INDEX/SEQUENCE/VIEW/MATVIEW/FOREIGN TABLE
// RENAME TO ...
pub fn RenameRelation<'mcx>(mcx: Mcx<'mcx>, stmt: &RenameStmt<'_>) -> PgResult<()> {
    let mut is_index_stmt =
        stmt.renameType == types_nodes::parsenodes::ObjectType::OBJECT_INDEX;
    let relid = loop {
        // ALTER INDEX takes only ShareUpdateExclusiveLock; a mismatched
        // statement/object pair retries under the object's lock level.
        let lockmode = if is_index_stmt { ShareUpdateExclusiveLock } else { AccessExclusiveLock };
        let relid = crate::alter::AlterTableLookupRangeVar(
            mcx,
            stmt.relation.expect("RenameStmt.relation"),
            lockmode,
            stmt.missing_ok,
            stmt.renameType,
            crate::alter::AlterRelationStmtKind::Rename,
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
        let relkind = lsyscache::get_rel_relkind(relid)? as u8;
        let obj_is_index = relkind == types_rel::RELKIND_INDEX
            || relkind == types_rel::RELKIND_PARTITIONED_INDEX;
        if obj_is_index || is_index_stmt == obj_is_index {
            break relid;
        }
        lmgr::UnlockRelationOid(relid, lockmode)?;
        is_index_stmt = obj_is_index;
    };
    RenameRelationInternal(mcx, relid, stmt.newname.expect("RenameStmt.newname"), is_index_stmt)
}

pub fn RenameRelationInternal<'mcx>(
    mcx: Mcx<'mcx>,
    myrelid: Oid,
    newrelname: &str,
    is_index: bool,
) -> PgResult<()> {
    let lock = if is_index { ShareUpdateExclusiveLock } else { AccessExclusiveLock };
    let targetrelation = relation_seams::relation_open::call(mcx, myrelid, lock)?;
    let namespace_id = targetrelation.rd_rel.relnamespace;

    if lsyscache::get_relname_relid(newrelname, namespace_id)? != InvalidOid {
        return Err(Box::new(
            PgError::new(ERROR, format!("relation \"{newrelname}\" already exists"))
                .with_sqlstate(ERRCODE_DUPLICATE_TABLE),
        ));
    }

    let relrelation = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;
    let key = crate::alter::oid_scankey(1, myrelid);
    let mut scan = genam::systable_beginscan(
        mcx,
        &relrelation,
        catalog::ClassOidIndexId,
        true,
        None,
        &[key],
    )?;
    let reltup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for relation {myrelid}"));
    // C: SearchSysCacheLockedCopy1 (tablecmds.c:4297) / UnlockTuple (:4326).
    // The lock precedes every content read that feeds the replacement image,
    // so a concurrent inplace writer's relfrozenxid/relminmxid advance is
    // either serialized behind us or visible in what we copy.
    let otid = reltup.t_self;
    lmgr::LockTuple(&relrelation, &otid, InplaceUpdateTupleLock)?;
    let desc = relrelation.descr();
    let n = desc.natts as usize;
    let namebuf = name_datum(mcx, newrelname)?;
    let mut values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, n)?;
    let mut nulls: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, n)?;
    let mut replace: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, n)?;
    values.resize(n, Datum::null());
    nulls.resize(n, false);
    replace.resize(n, false);
    values[2 - 1] = Datum::from_usize(namebuf.as_ptr() as usize); // relname
    replace[2 - 1] = true;
    let mut newtup =
        heaptuple::heap_modify_tuple(mcx, reltup, desc, &values, &nulls, &replace)?;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &relrelation, &otid, &mut newtup)?;
    lmgr::UnlockTuple(&relrelation, &otid, InplaceUpdateTupleLock)?;
    relrelation.close(RowExclusiveLock)?;

    if targetrelation.rd_rel.reltype != InvalidOid {
        pg_type::RenameTypeInternal(mcx, targetrelation.rd_rel.reltype, newrelname, namespace_id)?;
    }
    if targetrelation.rd_rel.relkind == types_rel::RELKIND_INDEX
        || targetrelation.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_INDEX
    {
        let constraint_id = get_index_constraint(mcx, myrelid)?;
        if constraint_id != InvalidOid {
            pg_constraint::RenameConstraintById(mcx, constraint_id, newrelname)?;
        }
    }
    targetrelation.close(NoLock)
}

// get_index_constraint (pg_depend.c): the INTERNAL dependency from an index
// to the constraint it implements, if any.
fn get_index_constraint<'mcx>(mcx: Mcx<'mcx>, index_id: Oid) -> PgResult<Oid> {
    const ConstraintRelationId: Oid = 2606;
    let dep_rel = table::table_open(mcx, pg_depend::DependRelationId, types_rel::AccessShareLock)?;
    let keys = [
        crate::alter::oid_scankey(1, RELATION_RELATION_ID),
        crate::alter::oid_scankey(2, index_id),
        crate::alter::int4_key(3, 0),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &dep_rel,
        pg_depend::DependDependerIndexId,
        true,
        None,
        &keys,
    )?;
    let desc = dep_rel.descr();
    let mut constraint_id = InvalidOid;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY (each): fixed NOT NULL pg_depend columns under its descriptor.
        let refclassid =
            unsafe { types_tuple::heap_getattr(tup, 4, desc, &mut isnull) }.as_oid();
        // SAFETY: as above.
        let refobjid = unsafe { types_tuple::heap_getattr(tup, 5, desc, &mut isnull) }.as_oid();
        // SAFETY: as above.
        let deptype =
            unsafe { types_tuple::heap_getattr(tup, 7, desc, &mut isnull) }.as_i8() as u8;
        if refclassid == ConstraintRelationId && deptype == b'i' {
            constraint_id = refobjid;
            break;
        }
    }
    genam::systable_endscan(mcx, scan)?;
    dep_rel.close(types_rel::AccessShareLock)?;
    Ok(constraint_id)
}

fn name_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<mcx::PgVec<'mcx, u8>> {
    let s = crate::truncate_name(mcx, s)?;
    let mut buf: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, 64)?;
    mcx::vec_append_bytes(&mut buf, s.as_bytes())?;
    mcx::vec_append_bytes(&mut buf, &[0u8; 64][..64 - s.len()])?;
    Ok(buf)
}
