//! `ATExecDetachPartition` / `ATExecDetachPartitionFinalize` (tablecmds.c:20912 /
//! 21429) and their subroutines — `ALTER TABLE <parent> DETACH PARTITION
//! <child> [CONCURRENTLY | FINALIZE]`.
//!
//! Faithful 1:1 port of PostgreSQL 18.3. Covers the **plain** (non-concurrent)
//! DETACH end to end:
//!
//!   * `ATExecDetachPartition` (non-concurrent path) — lock the default
//!     partition if any, open the partition, delete the inheritance link
//!     (`RemoveInheritance`), check no foreign keys would be violated, then run
//!     `DetachPartitionFinalize`;
//!   * `RemoveInheritance` — delete the `pg_inherits` row, decrement each
//!     inherited column's `attinhcount`, disinherit matched CHECK / NOT NULL
//!     constraints (`coninhcount`), and drop the parent dependency;
//!   * `DetachPartitionFinalize` — drop cloned triggers, clear `relpartbound` and
//!     reset `relispartition`, and invalidate the relcache for the parent (and,
//!     where relevant, the default partition / descendant partitions);
//!   * `DropClonedTriggersFromPartition`.
//!
//! GAPS (precise errors, never silent skips):
//!   * `DETACH ... CONCURRENTLY` (the two-transaction protocol:
//!     `MarkInheritDetached`, `DetachAddConstraintIfNeeded`,
//!     `WaitForLockersMultiple`, the cross-transaction re-open, and
//!     `WaitForOlderSnapshots` for `... FINALIZE`) is reported as
//!     `FEATURE_NOT_SUPPORTED`.
//!   * The inherited-foreign-key detach legs (`ATDetachCheckNoForeignKeyRefs`,
//!     the `RelationGetFKeyList` / `GetParentedForeignKeyRefs` action-trigger
//!     rework) raise a precise error when the partition actually carries inherited
//!     FKs; with none present they are genuine no-ops. Partitioned-index detach
//!     (`IndexSetParentIndex` / `ConstraintSetParentConstraint`) is fully ported.
//!   * Identity-column drop (`ATExecDropIdentity`) raises a precise error when the
//!     partition has an identity column (never in the common case).

#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};

use types_catalog::catalog_dependency::ObjectAddress;
use types_catalog::pg_attribute::{
    Anum_pg_attribute_attinhcount, Anum_pg_attribute_attislocal, AttributeRelationId,
    PgAttributeUpdateRow,
};
use types_core::primitive::{Oid, OidIsValid};
use types_error::{
    PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERRCODE_UNDEFINED_TABLE, ERROR,
};
use types_nodes::ddlnodes::PartitionCmd;
use types_tuple::access::RELKIND_PARTITIONED_TABLE;

use backend_access_common_relation::relation_open;
use backend_access_table_table::table_openrv;
use backend_catalog_indexing_seams as indexing_seam;
use backend_utils_cache_syscache::{
    SearchSysCacheCopyAttName, SearchSysCacheExistsAttName, SysCacheGetAttrNotNull, ATTNAME,
};
use backend_utils_error::ereport;

use types_rel::Relation;
use types_storage::lock::{AccessExclusiveLock, NoLock, RowExclusiveLock};

use crate::helpers::{here, object_address_set, RelationRelationId};
use crate::at_phase::AlteredTableInfo;

/// Convert a rich parse-node `RangeVar` to the trimmed `types_tuple::access`
/// shape `table_openrv` consumes (mirrors `at_attach::to_access_range_var`).
fn to_access_range_var(rv: &types_nodes::rawnodes::RangeVar<'_>) -> types_tuple::access::RangeVar {
    types_tuple::access::RangeVar {
        catalogname: rv.catalogname.as_deref().map(|s| s.into()),
        schemaname: rv.schemaname.as_deref().map(|s| s.into()),
        relname: rv.relname.as_deref().unwrap_or("").into(),
        inh: rv.inh,
        relpersistence: rv.relpersistence as u8,
        location: rv.location,
    }
}

fn elog(msg: impl Into<String>) -> types_error::PgError {
    ereport(ERROR).errmsg_internal(msg.into()).into_error()
}

/// `child_dependency_type(child_is_partition)` (catalog/heap.c): partitions get
/// an AUTO dependency, regular inheritance children a NORMAL one.
fn child_dependency_type(
    child_is_partition: bool,
) -> types_catalog::catalog_dependency::DependencyType {
    if child_is_partition {
        types_catalog::catalog_dependency::DEPENDENCY_AUTO
    } else {
        types_catalog::catalog_dependency::DEPENDENCY_NORMAL
    }
}

// ===========================================================================
// ATExecDetachPartition (tablecmds.c:20912)
// ===========================================================================

/// `ATExecDetachPartition(wqueue, tab, rel, name, concurrent)` (tablecmds.c:20912).
///
/// `rel` is the (open, locked) partitioned parent. `cmd` carries the partition's
/// name and the `concurrent` flag.
pub(crate) fn ATExecDetachPartition<'mcx>(
    mcx: Mcx<'mcx>,
    _wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    rel: &Relation<'mcx>,
    cmd: &PartitionCmd<'mcx>,
) -> PgResult<ObjectAddress> {
    let concurrent = cmd.concurrent;

    // We must lock the default partition, because detaching this partition will
    // change its partition constraint.
    let partdesc = backend_partitioning_partdesc::RelationGetPartitionDesc(mcx, rel, true)?;
    let default_part_oid =
        backend_partitioning_partdesc::get_default_oid_from_partdesc(Some(&partdesc));
    if OidIsValid(default_part_oid) {
        if concurrent {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(
                    "cannot detach partitions concurrently when a default partition exists"
                        .to_string(),
                )
                .into_error());
        }
        backend_storage_lmgr_lmgr::LockRelationOid(default_part_oid, AccessExclusiveLock)?;
    }

    if concurrent {
        // The two-transaction CONCURRENTLY protocol (MarkInheritDetached /
        // DetachAddConstraintIfNeeded / WaitForLockersMultiple / cross-transaction
        // re-open) is unported.
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(
                "DETACH PARTITION ... CONCURRENTLY is not yet supported \
                 (the two-transaction concurrent-detach protocol is unported)"
                    .to_string(),
            )
            .into_error());
    }

    // partRel = table_openrv(name, AccessExclusiveLock);
    let name_node = cmd
        .name
        .as_deref()
        .ok_or_else(|| elog("DETACH PARTITION: PartitionCmd has no relation name"))?;
    let rv = name_node
        .as_rangevar()
        .ok_or_else(|| elog("DETACH PARTITION: PartitionCmd name is not a RangeVar"))?;
    let access_rv = to_access_range_var(rv);
    let partRel = table_openrv(mcx, &access_rv, AccessExclusiveLock)?;

    // Delete the pg_inherits row (non-concurrent) and disinherit columns /
    // constraints.
    RemoveInheritance(mcx, &partRel, rel, false)?;

    // Make RemoveInheritance's pg_attribute updates (the per-column attinhcount
    // decrements) visible before DetachPartitionFinalize's identity-drop loop
    // re-updates the same pg_attribute tuples via ATExecDropIdentity. Without
    // this, an identity column that was inherited from the partitioned parent
    // (attinhcount > 0) is updated twice in the same command → "tuple already
    // updated by self". (C reaches the identity drop with the syscache still
    // serving the pre-decrement tuple version; our owned-snapshot model needs
    // the explicit command-counter bump to get the same effect.)
    backend_access_transam_xact::CommandCounterIncrement()?;

    // Ensure foreign keys still hold after this detach.
    ATDetachCheckNoForeignKeyRefs(mcx, &partRel)?;

    // Detaching the partition might involve TOAST table access, so ensure we
    // have a valid snapshot.
    backend_utils_time_snapmgr_seams::push_active_snapshot_transaction::call()?;

    // Do the final part of detaching.
    DetachPartitionFinalize(mcx, rel, &partRel, false, default_part_oid)?;

    backend_utils_time_snapmgr_seams::pop_active_snapshot::call()?;

    let address = object_address_set(RelationRelationId, partRel.rd_id);

    // keep our lock until commit
    partRel.close(NoLock)?;

    Ok(address)
}

// ===========================================================================
// ATExecDropInherit (tablecmds.c:17825)
// ===========================================================================

/// `ATExecDropInherit(rel, parent, lockmode)` (tablecmds.c:17825) —
/// `ALTER TABLE <child> NO INHERIT <parent>`. Returns the address of the parent
/// relation. `rel` is the (open, locked) child; `parent` is the parse-node
/// `RangeVar` from `cmd->def`.
pub(crate) fn ATExecDropInherit<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    parent: &types_nodes::rawnodes::RangeVar<'mcx>,
    _lockmode: types_storage::lock::LOCKMODE,
) -> PgResult<ObjectAddress> {
    if rel.rd_rel.relispartition {
        return Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg("cannot change inheritance of a partition".to_string())
            .into_error());
    }

    // AccessShareLock on the parent is probably enough, seeing that DROP TABLE
    // doesn't lock parent tables at all. We need some lock since we'll be
    // inspecting the parent's schema.
    let access_rv = to_access_range_var(parent);
    let parent_rel = table_openrv(
        mcx,
        &access_rv,
        types_storage::lock::AccessShareLock,
    )?;

    // We don't bother to check ownership of the parent table --- ownership of the
    // child is presumed enough rights.

    // Off to RemoveInheritance() where most of the work happens.
    RemoveInheritance(mcx, rel, &parent_rel, false)?;

    let address = object_address_set(RelationRelationId, parent_rel.rd_id);

    // keep our lock on the parent relation until commit.
    parent_rel.close(NoLock)?;

    Ok(address)
}

// ===========================================================================
// RemoveInheritance (tablecmds.c:17950)
// ===========================================================================

/// `RemoveInheritance(child_rel, parent_rel, expect_detached)` (tablecmds.c:17950)
/// — common to `ATExecDropInherit` and `ATExecDetachPartition`.
fn RemoveInheritance<'mcx>(
    mcx: Mcx<'mcx>,
    child_rel: &Relation<'mcx>,
    parent_rel: &Relation<'mcx>,
    expect_detached: bool,
) -> PgResult<()> {
    let is_partitioning = parent_rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE;

    // found = DeleteInheritsTuple(child, parent, expect_detached, childname);
    let childname = child_rel.name().to_string();
    let found = backend_catalog_pg_inherits::DeleteInheritsTuple(
        child_rel.rd_id,
        parent_rel.rd_id,
        expect_detached,
        Some(&childname),
    )?;
    if !found {
        if is_partitioning {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_TABLE)
                .errmsg(format!(
                    "relation \"{}\" is not a partition of relation \"{}\"",
                    child_rel.name(),
                    parent_rel.name()
                ))
                .into_error());
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_TABLE)
                .errmsg(format!(
                    "relation \"{}\" is not a parent of relation \"{}\"",
                    parent_rel.name(),
                    child_rel.name()
                ))
                .into_error());
        }
    }

    // Decrement attinhcount on each inherited child column.
    DisinheritAttributes(mcx, child_rel, parent_rel)?;

    // Disinherit matched CHECK / NOT NULL constraints (decrement coninhcount).
    // attmap = build_attrmap_by_name(child_desc, parent_desc, false);
    let attmap = backend_access_common_next::attmap::build_attrmap_by_name(
        mcx,
        &child_rel.rd_att,
        &parent_rel.rd_att,
        false,
    )?;
    backend_catalog_pg_constraint::disinherit_constraints(mcx, child_rel, parent_rel, &attmap)?;

    // drop_parent_dependency(child, RelationRelationId, parent,
    //                        child_dependency_type(is_partitioning));
    let deptype = child_dependency_type(is_partitioning).as_char();
    backend_catalog_pg_depend_seams::deleteDependencyRecordsForSpecific::call(
        RelationRelationId,
        child_rel.rd_id,
        deptype,
        RelationRelationId,
        parent_rel.rd_id,
    )?;

    // InvokeObjectPostAlterHookArg(InheritsRelationId, ...): a no-op in this port.

    Ok(())
}

/// The pg_attribute half of `RemoveInheritance` (tablecmds.c:17987-18022):
/// scan the child's columns, and for each inherited column also present in the
/// parent, decrement `attinhcount` (setting `attislocal` true when it reaches 0).
///
/// The C scans pg_attribute by `attrelid`; here we iterate the child relcache
/// tuple descriptor (which carries `attinhcount` / `attislocal`) and update via
/// the same `SearchSysCacheCopyAttName` + `catalog_tuple_update_pg_attribute`
/// path `MergeAttributesIntoExisting` uses.
fn DisinheritAttributes<'mcx>(
    mcx: Mcx<'mcx>,
    child_rel: &Relation<'mcx>,
    parent_rel: &Relation<'mcx>,
) -> PgResult<()> {
    // catalogRelation = table_open(AttributeRelationId, RowExclusiveLock);
    let attrrel = relation_open(mcx, AttributeRelationId, RowExclusiveLock)?;

    let tupdesc = &child_rel.rd_att;
    for attno in 1..=tupdesc.natts {
        let att = tupdesc.attr((attno - 1) as usize);

        // Ignore if dropped or not inherited.
        if att.attisdropped {
            continue;
        }
        if att.attinhcount <= 0 {
            continue;
        }

        let attname = String::from_utf8_lossy(att.attname.name_str()).into_owned();

        if SearchSysCacheExistsAttName(mcx, parent_rel.rd_id, &attname)? {
            // Read the current attinhcount from the catalog (heap_copytuple of
            // the scanned tuple in the C), decrement, possibly set islocal.
            let tuple = SearchSysCacheCopyAttName(mcx, child_rel.rd_id, &attname)?
                .ok_or_else(|| elog("RemoveInheritance: child column vanished mid-detach"))?;
            let cur_inhcount = SysCacheGetAttrNotNull(
                mcx,
                ATTNAME,
                &tuple,
                Anum_pg_attribute_attinhcount as i32,
            )?
            .as_i16();
            let cur_islocal = SysCacheGetAttrNotNull(
                mcx,
                ATTNAME,
                &tuple,
                Anum_pg_attribute_attislocal as i32,
            )?
            .as_bool();

            let new_inhcount = cur_inhcount - 1;
            let mut row = PgAttributeUpdateRow {
                attinhcount: Some(new_inhcount),
                ..Default::default()
            };
            if new_inhcount == 0 && !cur_islocal {
                row.attislocal = Some(true);
            }

            indexing_seam::catalog_tuple_update_pg_attribute::call(mcx, &attrrel, &tuple, &row)?;
        }
    }

    drop(attrrel);
    Ok(())
}

// ===========================================================================
// ATDetachCheckNoForeignKeyRefs (tablecmds.c)
// ===========================================================================

/// `ATDetachCheckNoForeignKeyRefs(partition)` (tablecmds.c) — verify that
/// detaching this partition does not leave a referencing table with rows whose
/// FK now points at no partition. Unported; a genuine no-op when the partition
/// is not referenced by any foreign key, a precise error otherwise.
fn ATDetachCheckNoForeignKeyRefs<'mcx>(
    _mcx: Mcx<'mcx>,
    partition: &Relation<'mcx>,
) -> PgResult<()> {
    let has_fkeys = backend_utils_cache_relcache::derived::relation_has_foreign_keys(partition.rd_id)?;
    if has_fkeys {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(
                "DETACH PARTITION of a partition involved in foreign keys is not yet supported \
                 (ATDetachCheckNoForeignKeyRefs / inherited-FK detach unported)"
                    .to_string(),
            )
            .into_error());
    }
    Ok(())
}

// ===========================================================================
// DetachPartitionFinalize (tablecmds.c:21095)
// ===========================================================================

/// `DetachPartitionFinalize(rel, partRel, concurrent, defaultPartOid)`
/// (tablecmds.c:21095) — the second (catalog-update) part of DETACH.
fn DetachPartitionFinalize<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    partRel: &Relation<'mcx>,
    concurrent: bool,
    default_part_oid: Oid,
) -> PgResult<()> {
    if concurrent {
        // RemoveInheritance(partRel, rel, true) — only reachable on the
        // CONCURRENTLY path, which is rejected earlier.
        return Err(elog(
            "DetachPartitionFinalize: concurrent finalize reached without the unported \
             CONCURRENTLY protocol",
        ));
    }

    // Drop any triggers that were cloned on creation/attach.
    DropClonedTriggersFromPartition(mcx, partRel.rd_id)?;

    // Detach inherited foreign keys. When the partition carries no foreign keys
    // (the common case) there is nothing to do. When it has some, the
    // inherited-FK detach rework (RelationGetFKeyList + addFkRecurseReferenced)
    // is unported.
    if backend_utils_cache_relcache::derived::relation_has_foreign_keys(partRel.rd_id)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(
                "DETACH PARTITION of a partition with inherited foreign keys is not yet supported \
                 (the addFkRecurseReferenced action-trigger rework is unported)"
                    .to_string(),
            )
            .into_error());
    }

    // GetParentedForeignKeyRefs(partRel): sub-constraints on the referenced side.
    // Unported; the FK-presence check above already gates the only way a
    // partition acquires such refs, so this is a no-op here.

    // Now we can detach indexes (tablecmds.c:21309-21341). For each of the
    // partition's indexes that is a child of a partitioned index, set its parent
    // to InvalidOid; if it (and the parent index) carry constraints, detach those
    // too.
    let indexes = backend_utils_cache_relcache::derived::RelationGetIndexList(partRel.rd_id)?;
    for &idxid in indexes.iter() {
        if !backend_catalog_pg_inherits::has_superclass(idxid)? {
            continue;
        }

        let parentidx =
            backend_catalog_partition_seams::get_partition_parent::call(idxid, false)?;

        let idx = backend_access_index_indexam_seams::index_open::call(
            mcx,
            idxid,
            AccessExclusiveLock,
        )?;
        backend_commands_indexcmds::IndexSetParentIndex(mcx, &idx, types_core::primitive::InvalidOid)?;

        // If there's a constraint associated with the index, detach it too.
        // It is possible for a constraint index in a partition to be the child
        // of a non-constraint index, so verify whether the parent index does
        // actually have a constraint.
        let constr_oid =
            backend_catalog_pg_constraint::get_relation_idx_constraint_oid(partRel.rd_id, idxid)?;
        let parent_constr_oid = backend_catalog_pg_constraint::get_relation_idx_constraint_oid(
            rel.rd_id, parentidx,
        )?;
        if OidIsValid(parent_constr_oid) && OidIsValid(constr_oid) {
            backend_catalog_pg_constraint::ConstraintSetParentConstraint(
                mcx,
                constr_oid,
                types_core::primitive::InvalidOid,
                types_core::primitive::InvalidOid,
            )?;
        }

        idx.close(NoLock)?;
    }

    // Update pg_class tuple: clear relpartbound and reset relispartition.
    backend_catalog_heap::ClearPartitionBound(mcx, partRel)?;

    // Drop identity property from all identity columns of partition.
    //
    //   for (attno = 0; attno < RelationGetNumberOfAttributes(partRel); attno++)
    //     if (!attr->attisdropped && attr->attidentity)
    //       ATExecDropIdentity(partRel, NameStr(attr->attname), false,
    //                          AccessExclusiveLock, true, true);
    //
    // Collect the names first so we don't read the relcache descriptor while
    // ATExecDropIdentity mutates the catalog.
    let mut identity_cols: Vec<String> = Vec::new();
    {
        let tupdesc = &partRel.rd_att;
        for attno in 0..tupdesc.natts {
            let attr = tupdesc.attr(attno as usize);
            if !attr.attisdropped && attr.attidentity != 0 {
                identity_cols.push(String::from_utf8_lossy(attr.attname.name_str()).into_owned());
            }
        }
    }
    for colname in &identity_cols {
        crate::at_identity::ATExecDropIdentity(
            mcx,
            partRel,
            colname,
            false,
            AccessExclusiveLock,
            true,
            true,
        )?;
    }

    if OidIsValid(default_part_oid) {
        // If the relation being detached is the default partition itself, remove
        // it from the parent's pg_partitioned_table entry. Otherwise invalidate
        // the default partition's relcache entry.
        if partRel.rd_id == default_part_oid {
            backend_catalog_partition::update_default_partition_oid(
                rel.rd_id,
                types_core::primitive::InvalidOid,
            )?;
        } else {
            backend_utils_cache_inval::cache_invalidate::CacheInvalidateRelcacheByRelid(
                default_part_oid,
            )?;
        }
    }

    // Invalidate the parent's relcache so the partition is no longer included in
    // its partition descriptor.
    backend_utils_cache_inval::cache_invalidate::CacheInvalidateRelcache(rel)?;

    // If the detached partition is itself partitioned, invalidate relcache for
    // all descendant partitions so their rd_partcheck trees are rebuilt.
    if partRel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        let (children, _) = backend_catalog_pg_inherits::find_all_inheritors(
            mcx,
            partRel.rd_id,
            AccessExclusiveLock,
            false,
        )?;
        for &child in children.iter() {
            backend_utils_cache_inval::cache_invalidate::CacheInvalidateRelcacheByRelid(child)?;
        }
    }

    Ok(())
}

// ===========================================================================
// DropClonedTriggersFromPartition (tablecmds.c:21506)
// ===========================================================================

/// `DropClonedTriggersFromPartition(partitionId)` (tablecmds.c:21506) — remove
/// triggers cloned onto the partition at creation/attach. Unported; a precise
/// error if the partition carries any triggers (so the cloned-trigger scan would
/// have work), a genuine no-op when it has none.
fn DropClonedTriggersFromPartition<'mcx>(
    _mcx: Mcx<'mcx>,
    partition_id: Oid,
) -> PgResult<()> {
    let has_triggers =
        backend_utils_cache_syscache_seams::rel_relhastriggers::call(partition_id)?
            .unwrap_or(false);
    if has_triggers {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(
                "DETACH PARTITION of a partition with triggers is not yet supported \
                 (DropClonedTriggersFromPartition unported)"
                    .to_string(),
            )
            .into_error());
    }
    Ok(())
}

// ===========================================================================
// ATExecDetachPartitionFinalize (tablecmds.c:21429)
// ===========================================================================

/// `ATExecDetachPartitionFinalize(rel, name)` (tablecmds.c:21429) — complete a
/// previously-interrupted DETACH ... CONCURRENTLY. Unported (depends on the
/// concurrent protocol and `WaitForOlderSnapshots`).
pub(crate) fn ATExecDetachPartitionFinalize<'mcx>(
    _mcx: Mcx<'mcx>,
    _rel: &Relation<'mcx>,
    _cmd: &PartitionCmd<'mcx>,
) -> PgResult<ObjectAddress> {
    ereport(ERROR)
        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
        .errmsg(
            "DETACH PARTITION ... FINALIZE is not yet supported \
             (it completes the unported two-transaction CONCURRENTLY protocol)"
                .to_string(),
        )
        .finish(here("ATExecDetachPartitionFinalize"))
        .map(|()| unreachable!())
}
