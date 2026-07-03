use mcx::{Mcx, PgVec};
use types_core::{InvalidOid, Oid, CONSTRAINT_OID_INDEX_ID, CONSTRAINT_RELATION_ID};
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
use types_rel::{AccessExclusiveLock, AccessShareLock, NoLock, Relation, RELKIND_PARTITIONED_TABLE};

use pg_constraint::{
    Anum_pg_constraint_confrelid, Anum_pg_constraint_conparentid, Anum_pg_constraint_conrelid,
    Anum_pg_constraint_contype, CONSTRAINT_FOREIGN,
};

use crate::drop::oid_scankey;

pub fn heap_truncate<'mcx>(mcx: Mcx<'mcx>, relids: &[Oid]) -> PgResult<()> {
    // std Vec: Relation is a pin/lock guard (drop glue), banned from mcx vecs.
    let mut relations: Vec<Relation<'mcx>> = Vec::with_capacity(relids.len());
    for &rid in relids {
        relations.push(table::table_open(mcx, rid, AccessExclusiveLock)?);
    }

    heap_truncate_check_FKs(mcx, &relations, true)?;

    for rel in relations.iter() {
        heap_truncate_one_rel(mcx, rel)?;
    }
    for rel in relations {
        rel.close(NoLock)?;
    }
    Ok(())
}

pub fn heap_truncate_one_rel<'mcx>(mcx: Mcx<'mcx>, rel: &Relation<'mcx>) -> PgResult<()> {
    if rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        return Ok(());
    }

    tableam::table_relation_nontransactional_truncate(rel)?;
    RelationTruncateIndexes(mcx, rel)?;

    let toastrelid = rel.rd_rel.reltoastrelid;
    if toastrelid != InvalidOid {
        let toastrel = table::table_open(mcx, toastrelid, AccessExclusiveLock)?;
        tableam::table_relation_nontransactional_truncate(&toastrel)?;
        RelationTruncateIndexes(mcx, &toastrel)?;
        toastrel.close(NoLock)?;
    }
    Ok(())
}

// C uses BuildDummyIndexInfo to avoid evaluating expression/predicate code;
// both are loud inside BuildIndexInfo here, so the full form is equivalent.
fn RelationTruncateIndexes<'mcx>(mcx: Mcx<'mcx>, heapRelation: &Relation<'mcx>) -> PgResult<()> {
    let indexIds = relcache::indexlist::RelationGetIndexList(mcx, heapRelation.rd_id)?;
    for &indexId in indexIds.iter() {
        let currentIndex = indexam::index_open(mcx, indexId, AccessExclusiveLock)?;
        catalog_storage::RelationTruncate(&currentIndex, 0)?;
        catalog_index_seams::index_build_dummy::call(mcx, heapRelation, &currentIndex, true)?;
        indexam::index_close(currentIndex, NoLock)?;
    }
    Ok(())
}

pub fn heap_truncate_check_FKs<'mcx>(
    mcx: Mcx<'mcx>,
    relations: &[Relation<'mcx>],
    tempTables: bool,
) -> PgResult<()> {
    let mut oids: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, relations.len())?;
    for rel in relations {
        if rel.rd_hastriggers || rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
            oids.push(rel.rd_id);
        }
    }
    if oids.is_empty() {
        return Ok(());
    }

    let dependents = heap_truncate_find_FKs(mcx, &oids)?;
    if dependents.is_empty() {
        return Ok(());
    }

    for &relid in oids.iter() {
        let dependents = heap_truncate_find_FKs(mcx, &[relid])?;
        for &relid2 in dependents.iter() {
            if !oids.contains(&relid2) {
                let relname = lsyscache::get_rel_name(mcx, relid)?
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default();
                let relname2 = lsyscache::get_rel_name(mcx, relid2)?
                    .map(|s| s.as_str().to_string())
                    .unwrap_or_default();
                let err = if tempTables {
                    PgError::new(
                        ERROR,
                        "unsupported ON COMMIT and foreign key combination".to_string(),
                    )
                    .with_detail(format!(
                        "Table \"{relname2}\" references \"{relname}\", but they do not have the same ON COMMIT setting."
                    ))
                } else {
                    PgError::new(
                        ERROR,
                        "cannot truncate a table referenced in a foreign key constraint"
                            .to_string(),
                    )
                    .with_detail(format!("Table \"{relname2}\" references \"{relname}\"."))
                    .with_hint(format!(
                        "Truncate table \"{relname2}\" at the same time, or use TRUNCATE ... CASCADE."
                    ))
                };
                return Err(Box::new(err.with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)));
            }
        }
    }
    Ok(())
}

pub fn heap_truncate_find_FKs<'mcx>(
    mcx: Mcx<'mcx>,
    relationIds: &[Oid],
) -> PgResult<PgVec<'mcx, Oid>> {
    let mut result: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, 4)?;
    let mut oids: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, relationIds.len())?;
    oids.extend_from_slice(relationIds);

    let fkeyRel = table::table_open(mcx, CONSTRAINT_RELATION_ID, AccessShareLock)?;
    let desc = fkeyRel.descr();
    let get = |tup: &types_tuple::HeapTupleData<'_>, attnum: i32| {
        let mut isnull = false;
        // SAFETY: fixed NOT NULL pg_constraint columns under its descriptor.
        let d = unsafe { types_tuple::heap_getattr(tup, attnum, desc, &mut isnull) };
        debug_assert!(!isnull);
        d
    };

    loop {
        let mut restart = false;
        let mut parent_cons: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, 4)?;

        // Seqscan: no index on confrelid exists, matching C.
        let mut scan = genam::systable_beginscan(mcx, &fkeyRel, InvalidOid, false, None, &[])?;
        while let Some(tuple) = genam::systable_getnext(mcx, &mut scan)? {
            if get(tuple, Anum_pg_constraint_contype as i32).as_i8() as u8 != CONSTRAINT_FOREIGN {
                continue;
            }
            let confrelid = get(tuple, Anum_pg_constraint_confrelid as i32).as_oid();
            if !oids.contains(&confrelid) {
                continue;
            }
            let conparentid = get(tuple, Anum_pg_constraint_conparentid as i32).as_oid();
            if conparentid != InvalidOid && !parent_cons.contains(&conparentid) {
                parent_cons.push(conparentid);
            }
            let conrelid = get(tuple, Anum_pg_constraint_conrelid as i32).as_oid();
            if !relationIds.contains(&conrelid) {
                result.push(conrelid);
            }
        }
        genam::systable_endscan(mcx, scan)?;

        let mut i = 0;
        while i < parent_cons.len() {
            let parent = parent_cons[i];
            i += 1;
            let key = [oid_scankey(1, parent)];
            let mut scan = genam::systable_beginscan(
                mcx,
                &fkeyRel,
                CONSTRAINT_OID_INDEX_ID,
                true,
                None,
                &key,
            )?;
            if let Some(tuple) = genam::systable_getnext(mcx, &mut scan)? {
                let conparentid = get(tuple, Anum_pg_constraint_conparentid as i32).as_oid();
                if conparentid != InvalidOid {
                    if !parent_cons.contains(&conparentid) {
                        parent_cons.push(conparentid);
                    }
                } else {
                    let confrelid = get(tuple, Anum_pg_constraint_confrelid as i32).as_oid();
                    if !oids.contains(&confrelid) {
                        oids.push(confrelid);
                        restart = true;
                    }
                }
            }
            genam::systable_endscan(mcx, scan)?;
        }

        if !restart {
            break;
        }
    }

    fkeyRel.close(AccessShareLock)?;

    result.sort_unstable();
    result.dedup();
    Ok(result)
}
