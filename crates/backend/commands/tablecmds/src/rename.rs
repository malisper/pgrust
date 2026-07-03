// renameatt / RenameRelation lane (tablecmds.c). LOUD: inheritance children,
// typed tables, non-table relkinds (except toast/index rides from cluster),
// constraint renames.
use datum::Datum;
use mcx::Mcx;
use types_core::{InvalidOid, Oid, RELATION_RELATION_ID};
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_TABLE, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_TABLE_DEFINITION, ERRCODE_UNDEFINED_COLUMN, ERROR, NOTICE,
};
use types_nodes::parsenodes::RenameStmt;
use types_rel::{AccessExclusiveLock, NoLock, RowExclusiveLock, ShareUpdateExclusiveLock, RELKIND_RELATION};

use crate::alter::{
    check_for_column_name_collision, find_inheritance_children_exist, update_pg_attribute,
    AlterTableLookupRangeVar, Anum_pg_attribute_attname,
};

fn unported(what: &str) -> ! {
    panic!("unported: tablecmds rename {what}")
}

// renameatt: ALTER TABLE ... RENAME [COLUMN] ... TO ...
pub fn renameatt<'mcx>(mcx: Mcx<'mcx>, stmt: &RenameStmt<'_>) -> PgResult<()> {
    let relid = AlterTableLookupRangeVar(
        mcx,
        stmt.relation.expect("RenameStmt.relation"),
        AccessExclusiveLock,
        stmt.missing_ok,
        types_nodes::parsenodes::ObjectType::OBJECT_TABLE,
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
    )
}

fn renameatt_internal<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    oldattname: &str,
    newattname: &str,
    recurse: bool,
) -> PgResult<()> {
    let rel = table::table_open(mcx, relid, AccessExclusiveLock)?;
    // renameatt_check: relkind gate; ownership rode the lookup callback.
    if rel.rd_rel.relkind != RELKIND_RELATION {
        unported("renameatt_check: non-plain-table relkind");
    }
    if recurse && find_inheritance_children_exist(mcx, relid)? {
        unported("renameatt_internal inheritance recursion");
    }
    let relname = rel.name().to_string();
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
    if attinhcount > 0 {
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

// RenameRelation: ALTER TABLE ... RENAME TO ...
pub fn RenameRelation<'mcx>(mcx: Mcx<'mcx>, stmt: &RenameStmt<'_>) -> PgResult<()> {
    let relid = AlterTableLookupRangeVar(
        mcx,
        stmt.relation.expect("RenameStmt.relation"),
        AccessExclusiveLock,
        stmt.missing_ok,
        types_nodes::parsenodes::ObjectType::OBJECT_TABLE,
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
    RenameRelationInternal(mcx, relid, stmt.newname.expect("RenameStmt.newname"), false)
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
    let otid = reltup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &relrelation, &otid, &mut newtup)?;
    relrelation.close(RowExclusiveLock)?;

    if targetrelation.rd_rel.reltype != InvalidOid {
        pg_type::RenameTypeInternal(mcx, targetrelation.rd_rel.reltype, newrelname, namespace_id)?;
    }
    if is_index {
        // get_index_constraint: index-backed constraints are unported lanes.
    }
    targetrelation.close(NoLock)
}

fn name_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<mcx::PgVec<'mcx, u8>> {
    assert!(s.len() < 64, "identifier truncation unported: {s:?}");
    let mut buf: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, 64)?;
    mcx::vec_append_bytes(&mut buf, s.as_bytes())?;
    mcx::vec_append_bytes(&mut buf, &[0u8; 64][..64 - s.len()])?;
    Ok(buf)
}
