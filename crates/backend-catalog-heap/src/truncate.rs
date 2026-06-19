//! `heap.c`'s TRUNCATE foreign-key-check tail: `heap_truncate_find_FKs` and
//! `heap_truncate_check_FKs`.
//!
//! These guard `ExecuteTruncateGuts`: a relation may not be truncated while a
//! foreign key from a relation *outside* the truncated group references it
//! (`heap_truncate_check_FKs`), and CASCADE truncate uses the same scan to pull
//! in the referencing relations (`heap_truncate_find_FKs`). The owned seams
//! (`backend-commands-tablecmds-seams`) carry relids rather than open
//! `Relation`s, so the trigger fast-path reads `relhastriggers` via lsyscache
//! instead of `rel->rd_rel`.
//!
//! The actual `pg_constraint` seqscan+deform is genam-owned vocabulary
//! (`scan_pg_constraint_truncate_fks`); we filter and resolve parent
//! constraints over the returned rows. C re-scans `pg_constraint` by
//! `ConstraintOidIndexId` to resolve each parent constraint; since the genam
//! helper hands back every row, we resolve parents by an in-memory OID lookup,
//! which is equivalent (the answer cannot depend on row locations — the result
//! is sorted and de-duplicated either way).

extern crate alloc;

use alloc::vec::Vec;

use backend_access_index_genam_seams::ScannedConstraintFk;
use backend_utils_cache_lsyscache::relation::{get_rel_name, get_rel_relhastriggers, get_rel_relkind};
use backend_utils_error::ereport;
use mcx::{Mcx, PgVec};
use types_core::primitive::{Oid, OidIsValid};
use types_error::{PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
use types_catalog::pg_constraint::CONSTRAINT_FOREIGN;
use types_tuple::access::RELKIND_PARTITIONED_TABLE;

/// `list_member_oid(list, datum)`.
fn list_member_oid(list: &[Oid], datum: Oid) -> bool {
    list.contains(&datum)
}

/// `heap_truncate_find_FKs(relationIds)` (heap.c).
///
/// Find relations having foreign keys referencing any of the given rels. The
/// result contains no duplicates, does *not* include any rels already in the
/// input list, and is sorted in OID order (the sort is enforced so the answer
/// can't depend on chance row locations within `pg_constraint`).
pub fn heap_truncate_find_FKs<'mcx>(
    mcx: Mcx<'mcx>,
    relation_ids: &[Oid],
) -> PgResult<PgVec<'mcx, Oid>> {
    // The full set of pg_constraint rows (the C seqscan), fetched once. C
    // re-runs the seqscan after each parent-constraint pass; the rows are stable
    // under the caller's locks, so a single fetch + in-memory passes is faithful.
    let rows: Vec<ScannedConstraintFk> =
        backend_access_index_genam_seams::scan_pg_constraint_truncate_fks::call()?;

    let mut result: Vec<Oid> = Vec::new();
    // oids = list_copy(relationIds);
    let mut oids: Vec<Oid> = relation_ids.to_vec();

    loop {
        let mut restart = false;
        let mut parent_cons: Vec<Oid> = Vec::new();

        for con in rows.iter() {
            /* Not a foreign key */
            if con.contype != CONSTRAINT_FOREIGN {
                continue;
            }

            /* Not referencing one of our list of tables */
            if !list_member_oid(&oids, con.confrelid) {
                continue;
            }

            /*
             * If this constraint has a parent constraint which we have not seen
             * yet, keep track of it for the second loop, below.
             */
            if OidIsValid(con.conparentid) && !list_member_oid(&parent_cons, con.conparentid) {
                parent_cons.push(con.conparentid);
            }

            /*
             * Add referencer to result, unless present in input list. (Don't
             * worry about dupes: we'll fix that below.)
             */
            if !list_member_oid(relation_ids, con.conrelid) {
                result.push(con.conrelid);
            }
        }

        /*
         * Process each parent constraint we found to add the list of referenced
         * relations by them to the oids list.  If we do add any new such
         * relations, redo the first loop above.  Also, if we see that the parent
         * constraint in turn has a parent, add that so that we process all
         * relations in a single additional pass.
         *
         * `parent_cons` grows inside the loop (C uses `foreach`, which observes
         * appends), so iterate by index.
         */
        let mut idx = 0;
        while idx < parent_cons.len() {
            let parent = parent_cons[idx];
            idx += 1;

            // C: systable_beginscan(fkeyRel, ConstraintOidIndexId, true,
            //                       key on Anum_pg_constraint_oid = parent).
            // Resolve over the row set we already hold.
            if let Some(con) = rows.iter().find(|c| c.oid == parent) {
                if OidIsValid(con.conparentid) {
                    if !list_member_oid(&parent_cons, con.conparentid) {
                        parent_cons.push(con.conparentid);
                    }
                } else if !list_member_oid(&oids, con.confrelid) {
                    oids.push(con.confrelid);
                    restart = true;
                }
            }
        }

        if !restart {
            break;
        }
    }

    /* Now sort and de-duplicate the result list */
    result.sort_unstable();
    result.dedup();

    let mut out: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, result.len())?;
    for relid in result {
        out.push(relid);
    }
    Ok(out)
}

/// `heap_truncate_check_FKs(relations, tempTables)` (heap.c).
///
/// Check for foreign keys referencing a list of relations that are to be
/// truncated, and raise an error if there are any (except self-referential
/// ones). The owned seam passes relids; the trigger fast-path reads
/// `relhastriggers`/`relkind` from the syscache.
pub fn heap_truncate_check_FKs<'mcx>(
    mcx: Mcx<'mcx>,
    relations: &[Oid],
    temp_tables: bool,
) -> PgResult<()> {
    /*
     * Build a list of OIDs of the interesting relations.
     *
     * If a relation has no triggers, then it can neither have FKs nor be
     * referenced by a FK from another table, so we can ignore it. For
     * partitioned tables, FKs have no triggers, so we must include them anyway.
     */
    let mut oids: Vec<Oid> = Vec::new();
    for &relid in relations {
        if get_rel_relhastriggers(relid)? || get_rel_relkind(relid)? == RELKIND_PARTITIONED_TABLE {
            oids.push(relid);
        }
    }

    /* Fast path: if no relation has triggers, none has FKs either. */
    if oids.is_empty() {
        return Ok(());
    }

    /*
     * Otherwise, must scan pg_constraint.  We make one pass with all the
     * relations considered; if this finds nothing, then all is well.
     */
    let dependents = heap_truncate_find_FKs(mcx, &oids)?;
    if dependents.is_empty() {
        return Ok(());
    }

    /*
     * Otherwise we repeat the scan once per relation to identify a particular
     * pair of relations to complain about.  The reason for doing things this
     * way is to ensure that the message produced is not dependent on chance row
     * locations within pg_constraint.
     */
    for &relid in oids.iter() {
        let dependents = heap_truncate_find_FKs(mcx, &[relid])?;

        for &relid2 in dependents.iter() {
            if !list_member_oid(&oids, relid2) {
                let relname = get_rel_name(mcx, relid)?
                    .map(|s| s.to_string())
                    .unwrap_or_default();
                let relname2 = get_rel_name(mcx, relid2)?
                    .map(|s| s.to_string())
                    .unwrap_or_default();

                if temp_tables {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("unsupported ON COMMIT and foreign key combination")
                        .errdetail(alloc::format!(
                            "Table \"{relname2}\" references \"{relname}\", but they do not have the same ON COMMIT setting."
                        ))
                        .into_error());
                } else {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                        .errmsg("cannot truncate a table referenced in a foreign key constraint")
                        .errdetail(alloc::format!(
                            "Table \"{relname2}\" references \"{relname}\"."
                        ))
                        .errhint(alloc::format!(
                            "Truncate table \"{relname2}\" at the same time, or use TRUNCATE ... CASCADE."
                        ))
                        .into_error());
                }
            }
        }
    }

    Ok(())
}
