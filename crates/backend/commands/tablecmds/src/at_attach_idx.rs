//! `ATExecAttachPartitionIdx` (tablecmds.c:21633) and its subroutines —
//! `ALTER INDEX <parent_partitioned_idx> ATTACH PARTITION <child_idx>`.
//!
//! Faithful 1:1 port of PostgreSQL 18.3. Covers:
//!
//!   * `ATExecAttachPartitionIdx` — lock + open the child index, verify it
//!     indexes a partition of the parent index's table, check index/constraint
//!     compatibility (`CompareIndexInfo` + constraint matching + PK NOT NULL),
//!     then `IndexSetParentIndex` / `ConstraintSetParentConstraint` and
//!     `validatePartitionedIndex`;
//!   * `RangeVarCallbackForAttachIndex` — the RangeVarGetRelidExtended callback
//!     that locks the parent table, verifies the named relation is an index,
//!     and records the partition OID it indexes;
//!   * `refuseDupeIndexAttach` — reject attaching a second index for the same
//!     partition;
//!   * `verifyPartitionIndexNotNull` — a PK's key columns on the partition must
//!     be NOT NULL;
//!   * `validatePartitionedIndex` — count valid leaf indexes via pg_inherits and
//!     mark the parent index valid (`pg_index.indisvalid = true`) once every
//!     partition has one, recursing to the grandparent index if applicable.

#![allow(non_snake_case)]

use mcx::Mcx;

use types_catalog::catalog_dependency::ObjectAddress;
use types_core::primitive::{Oid, OidIsValid};
use types_error::{
    PgError, PgResult, ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_INVALID_TABLE_DEFINITION,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_UNDEFINED_OBJECT, ERROR,
};
use rel::Relation;
use types_storage::lock::{AccessExclusiveLock, AccessShareLock, NoLock};
use types_tuple::access::{RELKIND_INDEX, RELKIND_PARTITIONED_INDEX};

use common_relation::relation_open;
use utils_error::ereport;

use partition_seams as partition_seam;
use index_seams as index_seam;
use lmgr_seams as lmgr_seam;
use lsyscache_seams as lsyscache;

use crate::helpers::{object_address_set, RelationRelationId};

type AccessRangeVar = types_tuple::access::RangeVar;

/// `struct AttachIndexCallbackState` (tablecmds.c) — mutable state threaded
/// through `RangeVarCallbackForAttachIndex`.
struct AttachIndexCallbackState {
    /// OID of the partition (heap) that the named index belongs to.
    partition_oid: Oid,
    /// OID of the partitioned table owning the parent index.
    parent_tbl_oid: Oid,
    /// Whether we have already taken the AccessShareLock on `parent_tbl_oid`.
    locked_parent_tbl: bool,
}

/// `RangeVarCallbackForAttachIndex(rv, relOid, oldRelOid, arg)` (tablecmds.c) —
/// lock the parent table once, drop a stale partition lock if the lookup
/// retried onto a different relation, verify the named relation is an index,
/// and record the partition (heap) OID it indexes (locking that partition with
/// AccessShareLock).
fn RangeVarCallbackForAttachIndex(
    rv: &AccessRangeVar,
    rel_oid: Oid,
    old_rel_oid: Oid,
    state: &mut AttachIndexCallbackState,
) -> PgResult<()> {
    if !state.locked_parent_tbl {
        // LockRelationOid held until commit (transaction abort releases it).
        core::mem::forget(lmgr_seam::lock_relation_oid::call(
            state.parent_tbl_oid,
            AccessShareLock,
        )?);
        state.locked_parent_tbl = true;
    }

    // If we previously locked some other heap and the name no longer refers to
    // an index on that relation, release the now-useless lock.
    if rel_oid != old_rel_oid && OidIsValid(state.partition_oid) {
        lmgr_seam::unlock_relation_oid::call(state.partition_oid, AccessShareLock)?;
        state.partition_oid = Oid::default();
    }

    // Didn't find a relation, so no need for locking or permission checks.
    if !OidIsValid(rel_oid) {
        return Ok(());
    }

    // SearchSysCache1(RELOID): read the relkind; concurrently-dropped ⇒ nothing
    // to do.
    let relkind = match syscache_seams::rel_relkind::call(rel_oid)? {
        Some(k) => k,
        None => return Ok(()),
    };
    if relkind != RELKIND_PARTITIONED_INDEX && relkind != RELKIND_INDEX {
        return Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "\"{}\" is not an index",
                &rv.relname
            ))
            .into_error());
    }

    // Since we need only examine the heap's tupledesc, an AccessShareLock on it
    // (preventing any DDL) is sufficient.
    state.partition_oid = index_seam::index_get_relation::call(rel_oid, false)?;
    core::mem::forget(lmgr_seam::lock_relation_oid::call(
        state.partition_oid,
        AccessShareLock,
    )?);
    Ok(())
}

/// `refuseDupeIndexAttach(parentIdx, partIdx, partitionTbl)` (tablecmds.c) —
/// reject attaching a second index for the same partition.
fn refuseDupeIndexAttach(
    parent_idx: &Relation<'_>,
    part_idx: &Relation<'_>,
    partition_tbl: &Relation<'_>,
) -> PgResult<()> {
    let existing_idx =
        catalog_partition::index_get_partition(partition_tbl, parent_idx.rd_id)?;
    if OidIsValid(existing_idx) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "cannot attach index \"{}\" as a partition of index \"{}\"",
                part_idx.name(),
                parent_idx.name()
            ))
            .errdetail(format!(
                "Another index is already attached for partition \"{}\".",
                partition_tbl.name()
            ))
            .into_error());
    }
    Ok(())
}

/// `verifyPartitionIndexNotNull(iinfo, partition)` (tablecmds.c) — a primary
/// key's key columns on the partition must be marked NOT NULL.
fn verifyPartitionIndexNotNull(
    iinfo: &nodes::execnodes::IndexInfo<'_>,
    partition: &Relation<'_>,
) -> PgResult<()> {
    for i in 0..(iinfo.ii_NumIndexKeyAttrs as usize) {
        let attno = iinfo.ii_IndexAttrNumbers[i];
        let att = partition.rd_att.attr((attno - 1) as usize);
        if !att.attnotnull {
            let attname = String::from_utf8_lossy(att.attname.name_str()).into_owned();
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                .errmsg("invalid primary key definition".to_string())
                .errdetail(format!(
                    "Column \"{}\" of relation \"{}\" is not marked NOT NULL.",
                    attname,
                    partition.name()
                ))
                .into_error());
        }
    }
    Ok(())
}

/// Convert a raw `RangeVar` node into the access-layer `RangeVar` shape
/// `RangeVarGetRelidExtended` consumes (mirrors `at_attach::to_access_range_var`).
fn to_access_range_var(rv: &nodes::rawnodes::RangeVar<'_>) -> AccessRangeVar {
    AccessRangeVar {
        catalogname: rv.catalogname.as_deref().map(|s| s.into()),
        schemaname: rv.schemaname.as_deref().map(|s| s.into()),
        relname: rv.relname.as_deref().unwrap_or("").into(),
        inh: rv.inh,
        relpersistence: rv.relpersistence as u8,
        location: rv.location,
    }
}

/// `ATExecAttachPartitionIdx(wqueue, parentIdx, cmd->name)` (tablecmds.c:21633).
pub(crate) fn ATExecAttachPartitionIdx<'mcx>(
    mcx: Mcx<'mcx>,
    parent_idx: &Relation<'mcx>,
    cmd: &nodes::ddlnodes::PartitionCmd<'mcx>,
) -> PgResult<ObjectAddress> {
    let name_node = cmd.name.as_deref().ok_or_else(|| {
        PgError::error("ATTACH PARTITION: PartitionCmd has no index name")
    })?;
    let rv = name_node.as_rangevar().ok_or_else(|| {
        PgError::error("ATTACH PARTITION: PartitionCmd name is not a RangeVar")
    })?;
    let name = to_access_range_var(rv);
    let name = &name;
    // parentIdx->rd_index->indrelid — the partitioned table owning the parent
    // index.
    let parent_indrelid = parent_idx
        .rd_index
        .as_ref()
        .map(|i| i.indrelid)
        .expect("ATExecAttachPartitionIdx: parentIdx is not an index");

    // Lock the index 'name', but lock the parent table first (deadlock-free),
    // recording the partition the index belongs to. The callback locks the
    // parent table and the partition heap.
    let mut state = AttachIndexCallbackState {
        partition_oid: Oid::default(),
        parent_tbl_oid: parent_indrelid,
        locked_parent_tbl: false,
    };

    let part_idx_id = {
        let mut cb = |rv: &AccessRangeVar, relid: Oid, oldrelid: Oid| {
            RangeVarCallbackForAttachIndex(rv, relid, oldrelid, &mut state)
        };
        catalog_namespace::RangeVarGetRelidExtended(
            mcx,
            name,
            AccessExclusiveLock,
            0,
            Some(&mut cb),
        )?
    };

    // Not there?
    if !OidIsValid(part_idx_id) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!(
                "index \"{}\" does not exist",
                &name.relname
            ))
            .into_error());
    }

    // no deadlock risk: RangeVarGetRelidExtended already acquired the lock.
    let part_idx = relation_open(mcx, part_idx_id, AccessExclusiveLock)?;

    // we already hold locks on both tables, so this is safe.
    let parent_tbl = relation_open(mcx, parent_indrelid, AccessShareLock)?;
    let part_indrelid = part_idx
        .rd_index
        .as_ref()
        .map(|i| i.indrelid)
        .expect("ATExecAttachPartitionIdx: partIdx is not an index");
    let part_tbl = relation_open(mcx, part_indrelid, NoLock)?;

    let address = object_address_set(RelationRelationId, part_idx.rd_id);

    // Silently do nothing if already in the right state.
    let curr_parent = if part_idx.rd_rel.relispartition {
        partition_seam::get_partition_parent::call(part_idx_id, false)?
    } else {
        Oid::default()
    };

    if curr_parent != parent_idx.rd_id {
        // If this partition already has an index attached, refuse.
        refuseDupeIndexAttach(parent_idx, &part_idx, &part_tbl)?;

        if OidIsValid(curr_parent) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "cannot attach index \"{}\" as a partition of index \"{}\"",
                    part_idx.name(),
                    parent_idx.name()
                ))
                .errdetail(format!(
                    "Index \"{}\" is already attached to another index.",
                    part_idx.name()
                ))
                .into_error());
        }

        // Make sure it indexes a partition of the other index's table.
        let part_desc =
            partdesc::RelationGetPartitionDesc(mcx, &parent_tbl, true)?;
        let mut found = false;
        for i in 0..(part_desc.nparts as usize) {
            if part_desc.oids[i] == state.partition_oid {
                found = true;
                break;
            }
        }
        if !found {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "cannot attach index \"{}\" as a partition of index \"{}\"",
                    part_idx.name(),
                    parent_idx.name()
                ))
                .errdetail(format!(
                    "Index \"{}\" is not an index on any partition of table \"{}\".",
                    part_idx.name(),
                    parent_tbl.name()
                ))
                .into_error());
        }

        // Ensure the indexes are compatible.
        let child_info = index::BuildIndexInfo(mcx, &part_idx)?;
        let parent_info = index::BuildIndexInfo(mcx, parent_idx)?;
        let attmap = next::attmap::build_attrmap_by_name(
            mcx,
            &part_tbl.rd_att,
            &parent_tbl.rd_att,
            false,
        )?;
        if !index::CompareIndexInfo(
            mcx,
            &child_info,
            &parent_info,
            &part_idx.rd_indcollation,
            &parent_idx.rd_indcollation,
            &part_idx.rd_opfamily,
            &parent_idx.rd_opfamily,
            &attmap.attnums,
        )? {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                .errmsg(format!(
                    "cannot attach index \"{}\" as a partition of index \"{}\"",
                    part_idx.name(),
                    parent_idx.name()
                ))
                .errdetail("The index definitions do not match.".to_string())
                .into_error());
        }

        // If there is a constraint in the parent, make sure there is one in the
        // child too.
        let constraint_oid = pg_constraint::get_relation_idx_constraint_oid(
            parent_tbl.rd_id,
            parent_idx.rd_id,
        )?;

        let mut cld_constr_id = Oid::default();
        if OidIsValid(constraint_oid) {
            cld_constr_id = pg_constraint::get_relation_idx_constraint_oid(
                part_tbl.rd_id,
                part_idx_id,
            )?;
            if !OidIsValid(cld_constr_id) {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg(format!(
                        "cannot attach index \"{}\" as a partition of index \"{}\"",
                        part_idx.name(),
                        parent_idx.name()
                    ))
                    .errdetail(format!(
                        "The index \"{}\" belongs to a constraint in table \"{}\" but no constraint exists for index \"{}\".",
                        parent_idx.name(),
                        parent_tbl.name(),
                        part_idx.name()
                    ))
                    .into_error());
            }
        }

        // If it's a primary key, make sure the columns in the partition are NOT
        // NULL.
        let parent_is_pk = parent_idx
            .rd_index
            .as_ref()
            .map(|i| i.indisprimary)
            .unwrap_or(false);
        if parent_is_pk {
            verifyPartitionIndexNotNull(&child_info, &part_tbl)?;
        }

        // All good -- do it.
        indexcmds::IndexSetParentIndex(mcx, &part_idx, parent_idx.rd_id)?;
        if OidIsValid(constraint_oid) {
            pg_constraint::ConstraintSetParentConstraint(
                mcx,
                cld_constr_id,
                constraint_oid,
                part_tbl.rd_id,
            )?;
        }

        validatePartitionedIndex(mcx, parent_idx, &parent_tbl)?;
    }

    parent_tbl.close(AccessShareLock)?;
    // keep these locks till commit.
    part_tbl.close(NoLock)?;
    part_idx.close(NoLock)?;

    Ok(address)
}

/// `validatePartitionedIndex(partedIdx, partedTbl)` (tablecmds.c:21818) — count
/// the valid leaf indexes inheriting `partedIdx` (via pg_inherits) and, if they
/// cover every partition of `partedTbl`, mark `partedIdx` valid; recurse to the
/// grandparent index if `partedIdx` is itself a partition.
fn validatePartitionedIndex<'mcx>(
    mcx: Mcx<'mcx>,
    parted_idx: &Relation<'mcx>,
    parted_tbl: &Relation<'mcx>,
) -> PgResult<()> {
    debug_assert!(parted_idx.rd_rel.relkind == RELKIND_PARTITIONED_INDEX);

    // Scan pg_inherits for this parent index. Count each valid index we find
    // (verifying the pg_index entry for each).
    let children =
        pg_inherits::find_inheritance_children(mcx, parted_idx.rd_id, NoLock)?;
    let mut tuples = 0i32;
    for &child in children.iter() {
        if lsyscache::get_index_isvalid::call(child)? {
            tuples += 1;
        }
    }

    // If we found as many inherited indexes as the partitioned table has
    // partitions, update pg_index to set indisvalid.
    let part_desc = partdesc::RelationGetPartitionDesc(mcx, parted_tbl, true)?;
    let mut updated = false;
    if tuples == part_desc.nparts {
        index_seam::index_mark_valid::call(parted_idx.rd_id)?;
        updated = true;
    }

    // If this index is in turn a partition of a larger index, validating it
    // might cause the parent to become valid also. Try that.
    if updated && parted_idx.rd_rel.relispartition {
        // make sure we see the validation we just did.
        transam_xact_seams::command_counter_increment::call()?;

        let parent_idx_id = partition_seam::get_partition_parent::call(parted_idx.rd_id, false)?;
        let parent_tbl_id = partition_seam::get_partition_parent::call(parted_tbl.rd_id, false)?;
        let parent_idx = relation_open(mcx, parent_idx_id, AccessExclusiveLock)?;
        let parent_tbl = relation_open(mcx, parent_tbl_id, AccessExclusiveLock)?;

        validatePartitionedIndex(mcx, &parent_idx, &parent_tbl)?;

        parent_idx.close(AccessExclusiveLock)?;
        parent_tbl.close(AccessExclusiveLock)?;
    }

    Ok(())
}
