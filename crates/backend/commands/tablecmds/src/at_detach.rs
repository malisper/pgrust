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

use ::types_catalog::catalog_dependency::ObjectAddress;
use ::types_catalog::pg_attribute::{
    Anum_pg_attribute_attinhcount, Anum_pg_attribute_attislocal, AttributeRelationId,
    PgAttributeUpdateRow,
};
use ::types_core::primitive::{Oid, OidIsValid};
use types_error::{
    PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_UNDEFINED_TABLE, ERROR,
};
use ::nodes::ddlnodes::{Constraint, ConstrType, PartitionCmd};
use ::nodes::nodes::Node;
use ::nodes::partition::PartitionStrategy;
use ::nodes::primnodes::Expr;
use ::types_tuple::access::RELKIND_PARTITIONED_TABLE;

use common_relation::{relation_open, try_relation_open};
use ::table::table_openrv;
use indexing_seams as indexing_seam;
use ::nodes_core::makefuncs::make_ands_explicit;
use cache_syscache::{
    SearchSysCacheCopyAttName, SearchSysCacheExistsAttName, SysCacheGetAttrNotNull, ATTNAME,
};
use ::utils_error::ereport;

use ::rel::Relation;
use ::types_storage::lock::{AccessExclusiveLock, NoLock, RowExclusiveLock, ShareUpdateExclusiveLock};

use crate::helpers::{here, object_address_set, RelationRelationId};
use crate::at_phase::AlteredTableInfo;

/// Convert a rich parse-node `RangeVar` to the trimmed `::types_tuple::access`
/// shape `table_openrv` consumes (mirrors `at_attach::to_access_range_var`).
fn to_access_range_var(rv: &::nodes::rawnodes::RangeVar<'_>) -> ::types_tuple::access::RangeVar {
    ::types_tuple::access::RangeVar {
        catalogname: rv.catalogname.as_deref().map(|s| s.into()),
        schemaname: rv.schemaname.as_deref().map(|s| s.into()),
        relname: rv.relname.as_deref().unwrap_or("").into(),
        inh: rv.inh,
        relpersistence: rv.relpersistence as u8,
        location: rv.location,
    }
}

fn elog(msg: impl Into<String>) -> ::types_error::PgError {
    ereport(ERROR).errmsg_internal(msg.into()).into_error()
}

/// `child_dependency_type(child_is_partition)` (catalog/heap.c): partitions get
/// an AUTO dependency, regular inheritance children a NORMAL one.
fn child_dependency_type(
    child_is_partition: bool,
) -> ::types_catalog::catalog_dependency::DependencyType {
    if child_is_partition {
        ::types_catalog::catalog_dependency::DEPENDENCY_AUTO
    } else {
        ::types_catalog::catalog_dependency::DEPENDENCY_NORMAL
    }
}

// ===========================================================================
// ATExecDetachPartition (tablecmds.c:20912)
// ===========================================================================

/// `ATExecDetachPartition(wqueue, tab, rel, name, concurrent)` (tablecmds.c:20912).
///
/// `rel` is the (open, locked) partitioned parent, taken *by value*: on the
/// CONCURRENTLY path the C closes both relations, commits, restarts a new
/// transaction, and re-opens them, storing the reopened parent back into
/// `tab->rel`. This port mirrors that by consuming `rel` and returning the
/// (possibly reopened) parent alongside the address; the caller stores it back
/// into `wqueue[ti].rel`. `ti` is the parent's work-queue index; `cmd` carries
/// the partition's name and the `concurrent` flag.
pub(crate) fn ATExecDetachPartition<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    ti: usize,
    rel: Relation<'mcx>,
    cmd: &PartitionCmd<'mcx>,
) -> PgResult<(ObjectAddress, Relation<'mcx>)> {
    let concurrent = cmd.concurrent;

    // We must lock the default partition, because detaching this partition will
    // change its partition constraint.
    let partdesc = partdesc::RelationGetPartitionDesc(mcx, &rel, true)?;
    let default_part_oid =
        partdesc::get_default_oid_from_partdesc(Some(&partdesc));
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
        lmgr::LockRelationOid(default_part_oid, AccessExclusiveLock)?;
    }

    // In concurrent mode, the partition is locked with share-update-exclusive in
    // the first transaction. This allows concurrent transactions to be doing DML
    // to the partition.
    let name_node = cmd
        .name
        .as_deref()
        .ok_or_else(|| elog("DETACH PARTITION: PartitionCmd has no relation name"))?;
    let rv = name_node
        .as_rangevar()
        .ok_or_else(|| elog("DETACH PARTITION: PartitionCmd name is not a RangeVar"))?;
    let access_rv = to_access_range_var(rv);
    let part_lockmode = if concurrent {
        ShareUpdateExclusiveLock
    } else {
        AccessExclusiveLock
    };
    let partRel = table_openrv(mcx, &access_rv, part_lockmode)?;

    // Check inheritance conditions and either delete the pg_inherits row (in
    // non-concurrent mode) or just set the inhdetachpending flag.
    if !concurrent {
        RemoveInheritance(mcx, &partRel, &rel, false)?;
    } else {
        pg_inherits::MarkInheritDetached(
            mcx,
            &partRel,
            rel.rd_id,
            rel.rd_rel.relkind,
            rel.rd_rel.relnamespace,
            rel.name(),
        )?;
    }

    // Ensure that foreign keys still hold after this detach.
    //
    // C runs this with NO intervening CommandCounterIncrement after
    // RemoveInheritance, so the pg_inherits row it just deleted is not yet
    // command-visible: `RI_PartitionRemove_Check`'s `RelationGetPartitionQual`
    // (→ get_partition_parent → pg_inherits scan) still finds the partition's
    // parent and can build the partition-constraint WHERE clause. We must
    // preserve that ordering — bumping the command counter here would expose the
    // delete and make get_partition_parent fail with "could not find tuple for
    // parent of relation".
    ATDetachCheckNoForeignKeyRefs(mcx, &partRel)?;

    // Concurrent mode has to work harder; first we add a new constraint to the
    // partition that matches the partition constraint. Then we close our
    // existing transaction, and in a new one wait for all processes to catch up
    // on the catalog updates we've done so far; at that point we can complete the
    // operation.
    if concurrent {
        // For strategies other than hash, add a constraint to the partition
        // being detached which supplants the partition constraint. For hash we
        // cannot do that, because the constraint would reference the partitioned
        // table OID, possibly causing problems later.
        let strategy = partdesc
            .boundinfo
            .as_deref()
            .map(|b| b.strategy)
            .unwrap_or(PartitionStrategy::Hash);
        if strategy != PartitionStrategy::Hash {
            DetachAddConstraintIfNeeded(mcx, wqueue, &partRel)?;
        }

        // We're almost done now; the only traces that remain are the pg_inherits
        // tuple and the partition's relpartbounds. Before we can remove those, we
        // need to wait until all transactions that know that this is a partition
        // are gone.

        // Remember relation OIDs to re-acquire them later.
        let partrelid = partRel.rd_id;
        let parentrelid = rel.rd_id;
        let parentrelname = rel.name().to_string();
        let partrelname = partRel.name().to_string();

        // Build the parent's relation locktag for WaitForLockersMultiple, using
        // the cached LockRelId (dbId == MyDatabaseId for a non-shared relation),
        // before we close the relation. (C: SET_LOCKTAG_RELATION(tag,
        // MyDatabaseId, parentrelid).)
        let lockrelid =
            relcache_seams::rel_lock_relid::call(parentrelid)?;
        let tag = lmgr_seams::set_locktag_relation::call(
            lockrelid.dbId,
            lockrelid.relId,
        );

        // Invalidate relcache entries for the parent -- must be before close.
        inval::cache_invalidate::CacheInvalidateRelcache(&rel)?;

        // table_close(partRel, NoLock); table_close(rel, NoLock); tab->rel = NULL;
        partRel.close(NoLock)?;
        rel.close(NoLock)?;
        debug_assert!(wqueue[ti].rel.is_none());

        // Make updated catalog entry visible.
        // PopActiveSnapshot(); CommitTransactionCommand();
        snapmgr_seams::pop_active_snapshot::call()?;
        transam_xact_seams::commit_transaction_command::call()?;

        // StartTransactionCommand();
        transam_xact_seams::start_transaction_command::call()?;

        // Now wait. This ensures that all queries that were planned including the
        // partition are finished before we remove the rest of catalog entries. We
        // don't need or indeed want to acquire this lock, though -- that would
        // block later queries.
        let locktags = [tag];
        lmgr::WaitForLockersMultiple(
            mcx,
            &locktags,
            AccessExclusiveLock,
            false,
        )?;

        // Now acquire locks in both relations again. Note they may have been
        // removed in the meantime, so care is required.
        let reopened_rel = try_relation_open(mcx, parentrelid, ShareUpdateExclusiveLock)?;
        let reopened_part = try_relation_open(mcx, partrelid, AccessExclusiveLock)?;

        // If the relations aren't there, something bad happened; bail out.
        let Some(reopened_rel) = reopened_rel else {
            if reopened_part.is_some() {
                // shouldn't happen
                ereport(::types_error::WARNING)
                    .errmsg(format!(
                        "dangling partition \"{partrelname}\" remains, can't fix"
                    ))
                    .finish(here("ATExecDetachPartition"))?;
            }
            return Err(ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "partitioned table \"{parentrelname}\" was removed concurrently"
                ))
                .into_error());
        };
        let Some(reopened_part) = reopened_part else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!("partition \"{partrelname}\" was removed concurrently"))
                .into_error());
        };

        // tab->rel = rel;
        wqueue[ti].rel = Some(reopened_rel);

        // Detaching the partition might involve TOAST table access, so ensure we
        // have a valid snapshot.
        snapmgr_seams::push_active_snapshot_transaction::call()?;

        // Do the final part of detaching (concurrent = true).
        let rel_ref = wqueue[ti]
            .rel
            .as_ref()
            .ok_or_else(|| elog("ATExecDetachPartition: tab->rel vanished after reopen"))?;
        DetachPartitionFinalize(mcx, rel_ref, &reopened_part, true, default_part_oid)?;

        snapmgr_seams::pop_active_snapshot::call()?;

        let address = object_address_set(RelationRelationId, reopened_part.rd_id);

        // keep our lock until commit
        reopened_part.close(NoLock)?;

        // The caller (ATExecCmd) put back wqueue[ti].rel itself; hand the reopened
        // parent back so the per-pass loop's close releases the right relation.
        let reopened_parent = wqueue[ti]
            .rel
            .take()
            .ok_or_else(|| elog("ATExecDetachPartition: tab->rel vanished after finalize"))?;
        return Ok((address, reopened_parent));
    }

    // --- non-concurrent path (rel stays open the whole time) ---

    // Make RemoveInheritance's pg_attribute updates (the per-column attinhcount
    // decrements) visible before DetachPartitionFinalize's identity-drop loop
    // re-updates the same pg_attribute tuples via ATExecDropIdentity. Without
    // this, an identity column that was inherited from the partitioned parent
    // (attinhcount > 0) is updated twice in the same command → "tuple already
    // updated by self". (C reaches the identity drop with the syscache still
    // serving the pre-decrement tuple version; our owned-snapshot model needs
    // the explicit command-counter bump to get the same effect.)
    transam_xact::CommandCounterIncrement()?;

    // Detaching the partition might involve TOAST table access, so ensure we
    // have a valid snapshot.
    snapmgr_seams::push_active_snapshot_transaction::call()?;

    // Do the final part of detaching.
    DetachPartitionFinalize(mcx, &rel, &partRel, false, default_part_oid)?;

    snapmgr_seams::pop_active_snapshot::call()?;

    let address = object_address_set(RelationRelationId, partRel.rd_id);

    // keep our lock until commit
    partRel.close(NoLock)?;

    Ok((address, rel))
}

// ===========================================================================
// DetachAddConstraintIfNeeded (tablecmds.c:21464)
// ===========================================================================

/// `DetachAddConstraintIfNeeded(wqueue, partRel)` (tablecmds.c:21464) — create a
/// constraint on the partition being detached that takes the place of the
/// partition constraint, avoiding a duplicate when an existing constraint already
/// implies the needed one.
fn DetachAddConstraintIfNeeded<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    partRel: &Relation<'mcx>,
) -> PgResult<()> {
    // constraintExpr = RelationGetPartitionQual(partRel);
    let constraint_expr = partcache::RelationGetPartitionQual(mcx, partRel)?;

    // constraintExpr = (List *) eval_const_expressions(NULL, (Node *) constraintExpr);
    // (eval_const_expressions on a list recurses element-wise.)
    let mut folded: Vec<Expr<'mcx>> = Vec::with_capacity(constraint_expr.len());
    for n in constraint_expr.into_iter() {
        let e = n
            .into_expr()
            .ok_or_else(|| elog("DetachAddConstraintIfNeeded: partition qual node is not an Expr"))?;
        folded.push(clauses::eval_const_expressions(mcx, e)?);
    }

    // Avoid adding a new constraint if the needed constraint is implied by an
    // existing constraint. PartConstraintImpliedByRelConstraint takes the
    // implicit-AND list as Nodes.
    let mut implied_input: Vec<Node<'mcx>> = Vec::with_capacity(folded.len());
    for e in folded.iter() {
        implied_input.push(Node::mk_expr(mcx, e.clone_in(mcx)?)?);
    }
    if crate::at_attach::PartConstraintImpliedByRelConstraint(mcx, partRel, &implied_input)? {
        return Ok(());
    }

    // tab = ATGetQueueEntry(wqueue, partRel);
    let tab = crate::at_phase::ATGetQueueEntry(mcx, wqueue, partRel)?;

    // n->cooked_expr = nodeToString(make_ands_explicit(constraintExpr));
    let ands = make_ands_explicit(folded);
    let ands_node = Node::mk_expr(mcx, ands)?;
    let cooked = outfuncs::nodeToString(mcx, &ands_node)?;

    // Add constraint on partition, equivalent to the partition constraint.
    let n = Constraint {
        contype: ConstrType::CONSTR_CHECK,
        conname: None,
        location: -1,
        is_no_inherit: false,
        raw_expr: None,
        cooked_expr: Some(cooked),
        is_enforced: true,
        initially_valid: true,
        skip_validation: true,
        deferrable: false,
        initdeferred: false,
        generated_when: 0,
        generated_kind: 0,
        nulls_not_distinct: false,
        keys: PgVec::new_in(mcx),
        without_overlaps: false,
        including: PgVec::new_in(mcx),
        exclusions: PgVec::new_in(mcx),
        options: PgVec::new_in(mcx),
        indexname: None,
        indexspace: None,
        reset_default_tblspc: false,
        access_method: None,
        where_clause: None,
        pktable: None,
        fk_attrs: PgVec::new_in(mcx),
        pk_attrs: PgVec::new_in(mcx),
        fk_with_period: false,
        pk_with_period: false,
        fk_matchtype: 0,
        fk_upd_action: 0,
        fk_del_action: 0,
        fk_del_set_cols: PgVec::new_in(mcx),
        old_conpfeqop: PgVec::new_in(mcx),
        old_pktable_oid: ::types_core::primitive::InvalidOid,
    };

    // It's a re-add, since it nominally already exists.
    // ATAddCheckNNConstraint(wqueue, tab, partRel, n, true, false, true, ShareUpdateExclusiveLock);
    crate::at_constraint::ATAddCheckNNConstraint(
        mcx,
        wqueue,
        tab,
        partRel,
        &n,
        true,                       // recurse
        false,                      // recursing
        true,                       // is_readd
        ShareUpdateExclusiveLock,
    )?;

    Ok(())
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
    parent: &::nodes::rawnodes::RangeVar<'mcx>,
    _lockmode: ::types_storage::lock::LOCKMODE,
) -> PgResult<ObjectAddress> {
    if rel.rd_rel.relispartition {
        return Err(ereport(ERROR)
            .errcode(::types_error::ERRCODE_WRONG_OBJECT_TYPE)
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
        ::types_storage::lock::AccessShareLock,
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
    let found = pg_inherits::DeleteInheritsTuple(
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
    let attmap = next::attmap::build_attrmap_by_name(
        mcx,
        &child_rel.rd_att,
        &parent_rel.rd_att,
        false,
    )?;
    pg_constraint::disinherit_constraints(mcx, child_rel, parent_rel, &attmap)?;

    // drop_parent_dependency(child, RelationRelationId, parent,
    //                        child_dependency_type(is_partitioning));
    let deptype = child_dependency_type(is_partitioning).as_char();
    pg_depend_seams::deleteDependencyRecordsForSpecific::call(
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
// GetParentedForeignKeyRefs (tablecmds.c:21942)
// ===========================================================================

/// `GetParentedForeignKeyRefs(partition)` (tablecmds.c:21942) — collect the OIDs
/// of all FK constraints that reference `partition` (i.e. `confrelid ==
/// partition`) and that are themselves sub-constraints of a larger FK
/// (`conparentid` valid). These are the constraints that must be re-checked /
/// removed when the partition leaves the key space of a partitioned PK.
fn GetParentedForeignKeyRefs<'mcx>(
    mcx: Mcx<'mcx>,
    partition: &Relation<'mcx>,
) -> PgResult<Vec<Oid>> {
    use ::types_catalog::pg_constraint as pc;

    // If no indexes, or no columns are referenceable by FKs, avoid the scan.
    let idxlist = relcache::derived::RelationGetIndexList(partition.rd_id)?;
    if idxlist.is_empty() {
        return Ok(Vec::new());
    }
    let keyattrs = relcache::derived::RelationGetIndexAttrBitmap(
        partition.rd_id,
        relcache::derived::IndexAttrBitmapKind::Keys,
    )?;
    if keyattrs.is_empty() {
        return Ok(Vec::new());
    }

    // Search for constraints referencing this table.
    let pg_constraint = table_seams::table_open::call(
        mcx,
        objectaddress::consts::ConstraintRelationId,
        ::types_storage::lock::AccessShareLock,
    )?;

    let mut key0 = types_scan::scankey::ScanKeyData::empty();
    scankey::ScanKeyInit(
        &mut key0,
        pc::Anum_pg_constraint_confrelid,
        types_scan::scankey::BTEqualStrategyNumber,
        ::types_core::fmgr::F_OIDEQ,
        types_tuple::heaptuple::Datum::from_oid(partition.rd_id),
    )?;
    let mut key1 = types_scan::scankey::ScanKeyData::empty();
    scankey::ScanKeyInit(
        &mut key1,
        pc::Anum_pg_constraint_contype,
        types_scan::scankey::BTEqualStrategyNumber,
        ::types_core::fmgr::F_CHAREQ,
        types_tuple::heaptuple::Datum::from_char(pc::CONSTRAINT_FOREIGN),
    )?;
    let keys = [key0, key1];

    // XXX This is a seqscan, as we don't have a usable index (InvalidOid +
    // index_ok=false ⇒ heap scan, as C's genam does for indexId == InvalidOid).
    let mut scan = genam_seams::systable_beginscan::call(
        &pg_constraint,
        ::types_core::primitive::InvalidOid,
        false,
        None,
        &keys,
    )?;

    let mut constraints: Vec<Oid> = Vec::new();
    while let Some(tup) =
        genam_seams::systable_getnext::call(mcx, scan.desc_mut())?
    {
        let cols = heaptuple::heap_deform_tuple(
            mcx,
            &tup.tuple,
            &pg_constraint.rd_att,
            &tup.data,
        )?;
        let col = |attno: i16| cols[attno as usize - 1].0.clone();
        let conparentid = col(pc::Anum_pg_constraint_conparentid).as_oid();
        let conoid = col(pc::Anum_pg_constraint_oid).as_oid();

        // We only need to process constraints that are part of larger ones.
        if !OidIsValid(conparentid) {
            continue;
        }
        constraints.push(conoid);
    }
    drop(scan);
    pg_constraint.close(::types_storage::lock::AccessShareLock)?;

    Ok(constraints)
}

// ===========================================================================
// ATDetachCheckNoForeignKeyRefs (tablecmds.c:21995)
// ===========================================================================

/// `ATDetachCheckNoForeignKeyRefs(partition)` (tablecmds.c:21995) — during
/// DETACH PARTITION, verify that any foreign keys pointing to the partitioned
/// table (via a parented FK whose child references this partition) would not
/// become invalid; an error is raised if any referenced values exist.
fn ATDetachCheckNoForeignKeyRefs<'mcx>(
    mcx: Mcx<'mcx>,
    partition: &Relation<'mcx>,
) -> PgResult<()> {
    let constraints = GetParentedForeignKeyRefs(mcx, partition)?;

    for &constr_oid in constraints.iter() {
        let row = syscache_seams::search_constraint_form_by_oid::call(
            constr_oid,
        )?
        .ok_or_else(|| elog(format!("cache lookup failed for constraint {constr_oid}")))?;
        let constr_form = row.form;

        debug_assert!(OidIsValid(constr_form.conparentid));
        debug_assert!(constr_form.confrelid == partition.rd_id);

        // Prevent data changes into the referencing table until commit.
        let rel = table_seams::table_open::call(
            mcx,
            constr_form.conrelid,
            ::types_storage::lock::ShareLock,
        )?;

        // Run RI_PartitionRemove_Check through the trigger manager, which
        // installs the synthetic-trigger side-channel (C's stack
        // `Trigger trig = {0}` with the constraint identity) the RI proc reads.
        trigger_seams::detach_partition_remove_check::call(
            mcx,
            constr_form.conname_str(),
            &rel,
            partition,
            constr_form.conindid,
            constr_form.oid,
        )?;

        rel.close(NoLock)?;
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
        // We can remove the pg_inherits row now. (In the non-concurrent case,
        // this was already done by RemoveInheritance in ATExecDetachPartition.)
        RemoveInheritance(mcx, partRel, rel, true)?;
    }

    // Drop any triggers that were cloned on creation/attach.
    DropClonedTriggersFromPartition(mcx, partRel.rd_id)?;

    // Detach any foreign keys that are inherited. This includes creating
    // additional action triggers on the referenced tables (addFkRecurseReferenced).
    crate::at_fk::DetachPartitionForeignKeys(mcx, partRel)?;

    // Any sub-constraints that are in the referenced-side of a larger constraint
    // have to be removed. This partition is no longer part of the key space of
    // the constraint.
    for constr_oid in GetParentedForeignKeyRefs(mcx, partRel)? {
        pg_constraint::ConstraintSetParentConstraint(
            mcx,
            constr_oid,
            ::types_core::primitive::InvalidOid,
            ::types_core::primitive::InvalidOid,
        )?;
        pg_depend_seams::deleteDependencyRecordsForClass::call(
            objectaddress::consts::ConstraintRelationId,
            constr_oid,
            objectaddress::consts::ConstraintRelationId,
            ::types_catalog::catalog_dependency::DEPENDENCY_INTERNAL.as_char(),
        )?;
        transam_xact::CommandCounterIncrement()?;

        let constraint =
            object_address_set(objectaddress::consts::ConstraintRelationId, constr_oid);
        dependency_seams::perform_deletion::call(
            constraint.classId,
            constraint.objectId,
            constraint.objectSubId,
            ::nodes::parsenodes::DROP_RESTRICT,
            0,
        )?;
    }

    // Now we can detach indexes (tablecmds.c:21309-21341). For each of the
    // partition's indexes that is a child of a partitioned index, set its parent
    // to InvalidOid; if it (and the parent index) carry constraints, detach those
    // too.
    let indexes = relcache::derived::RelationGetIndexList(partRel.rd_id)?;
    for &idxid in indexes.iter() {
        if !pg_inherits::has_superclass(idxid)? {
            continue;
        }

        let parentidx =
            partition_seams::get_partition_parent::call(idxid, false)?;

        let idx = indexam_seams::index_open::call(
            mcx,
            idxid,
            AccessExclusiveLock,
        )?;
        indexcmds::IndexSetParentIndex(mcx, &idx, ::types_core::primitive::InvalidOid)?;

        // If there's a constraint associated with the index, detach it too.
        // It is possible for a constraint index in a partition to be the child
        // of a non-constraint index, so verify whether the parent index does
        // actually have a constraint.
        let constr_oid =
            pg_constraint::get_relation_idx_constraint_oid(partRel.rd_id, idxid)?;
        let parent_constr_oid = pg_constraint::get_relation_idx_constraint_oid(
            rel.rd_id, parentidx,
        )?;
        if OidIsValid(parent_constr_oid) && OidIsValid(constr_oid) {
            pg_constraint::ConstraintSetParentConstraint(
                mcx,
                constr_oid,
                ::types_core::primitive::InvalidOid,
                ::types_core::primitive::InvalidOid,
            )?;
        }

        idx.close(NoLock)?;
    }

    // Update pg_class tuple: clear relpartbound and reset relispartition.
    heap::ClearPartitionBound(mcx, partRel)?;

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
            catalog_partition::update_default_partition_oid(
                rel.rd_id,
                ::types_core::primitive::InvalidOid,
            )?;
        } else {
            inval::cache_invalidate::CacheInvalidateRelcacheByRelid(
                default_part_oid,
            )?;
        }
    }

    // Invalidate the parent's relcache so the partition is no longer included in
    // its partition descriptor.
    inval::cache_invalidate::CacheInvalidateRelcache(rel)?;

    // If the detached partition is itself partitioned, invalidate relcache for
    // all descendant partitions so their rd_partcheck trees are rebuilt.
    if partRel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        let (children, _) = pg_inherits::find_all_inheritors(
            mcx,
            partRel.rd_id,
            AccessExclusiveLock,
            false,
        )?;
        for &child in children.iter() {
            inval::cache_invalidate::CacheInvalidateRelcacheByRelid(child)?;
        }
    }

    Ok(())
}

// ===========================================================================
// DropClonedTriggersFromPartition (tablecmds.c:21506)
// ===========================================================================

/// `DropClonedTriggersFromPartition(partitionId)` (tablecmds.c:21506) — remove
/// triggers that were cloned onto the partition when it was created-as-partition
/// or attached (undoes `CloneRowTriggersToPartition`). Scans `pg_trigger` by
/// `tgrelid`, skips non-cloned triggers (`tgparentid` unset) and FK
/// implementation triggers (`tgconstrrelid` set — those detach with their
/// foreign keys), removes the partition dependency markings, and deletes the
/// rest in one `performMultipleDeletions`.
fn DropClonedTriggersFromPartition<'mcx>(
    mcx: Mcx<'mcx>,
    partition_id: Oid,
) -> PgResult<()> {
    use ::types_catalog::catalog_dependency::{
        DEPENDENCY_PARTITION_PRI, DEPENDENCY_PARTITION_SEC,
    };
    use ::types_catalog::pg_trigger as pt;

    let mut objects = dependency_seams::new_object_addresses::call()?;

    // Scan pg_trigger to search for all triggers on this rel.
    let tgrel = table_seams::table_open::call(
        mcx,
        pt::TriggerRelationId,
        RowExclusiveLock,
    )?;

    let mut skey = types_scan::scankey::ScanKeyData::empty();
    scankey::ScanKeyInit(
        &mut skey,
        pt::Anum_pg_trigger_tgrelid,
        types_scan::scankey::BTEqualStrategyNumber,
        ::types_core::fmgr::F_OIDEQ,
        types_tuple::heaptuple::Datum::from_oid(partition_id),
    )?;
    let keys = [skey];

    let mut scan = genam_seams::systable_beginscan::call(
        &tgrel,
        pt::TriggerRelidNameIndexId,
        true,
        None,
        &keys,
    )?;

    let mut trig_oids: Vec<Oid> = Vec::new();
    while let Some(trigtup) =
        genam_seams::systable_getnext::call(mcx, scan.desc_mut())?
    {
        let cols = heaptuple::heap_deform_tuple(
            mcx,
            &trigtup.tuple,
            &tgrel.rd_att,
            &trigtup.data,
        )?;
        let col = |attno: i16| cols[attno as usize - 1].0.clone();
        let tgparentid = col(pt::Anum_pg_trigger_tgparentid).as_oid();
        let tgconstrrelid = col(pt::Anum_pg_trigger_tgconstrrelid).as_oid();
        let tgoid = col(pt::Anum_pg_trigger_oid).as_oid();

        // Ignore triggers that weren't cloned.
        if !OidIsValid(tgparentid) {
            continue;
        }
        // Ignore internal triggers that are implementation objects of foreign
        // keys, because these will be detached when the foreign keys themselves
        // are.
        if OidIsValid(tgconstrrelid) {
            continue;
        }
        trig_oids.push(tgoid);
    }
    drop(scan);

    // Remove the partition dependency markings so the triggers can be removed,
    // then collect their addresses for deletion.
    for tgoid in trig_oids {
        dependency_seams::delete_dependency_records_for_class::call(
            pt::TriggerRelationId,
            tgoid,
            pt::TriggerRelationId,
            DEPENDENCY_PARTITION_PRI,
        )?;
        dependency_seams::delete_dependency_records_for_class::call(
            pt::TriggerRelationId,
            tgoid,
            RelationRelationId,
            DEPENDENCY_PARTITION_SEC,
        )?;
        let trig = object_address_set(pt::TriggerRelationId, tgoid);
        dependency_seams::add_exact_object_address::call(trig, &mut objects)?;
    }

    // Make the dependency removal visible to the deletion below.
    transam_xact::CommandCounterIncrement()?;
    dependency_seams::perform_multiple_deletions::call(
        &objects.refs,
        ::nodes::parsenodes::DROP_RESTRICT,
        dependency_seams::PERFORM_DELETION_INTERNAL,
    )?;

    dependency_seams::free_object_addresses::call(objects)?;
    tgrel.close(RowExclusiveLock)?;

    Ok(())
}

// ===========================================================================
// ATExecDetachPartitionFinalize (tablecmds.c:21429)
// ===========================================================================

/// `ATExecDetachPartitionFinalize(rel, name)` (tablecmds.c:21429) — complete a
/// previously-interrupted DETACH ... CONCURRENTLY.
///
/// C:
/// ```c
/// partRel = table_openrv(name, AccessExclusiveLock);
/// // Wait until existing snapshots are gone.  This is important if the second
/// // transaction of DETACH PARTITION CONCURRENTLY is canceled: the user could
/// // immediately run DETACH FINALIZE without actually waiting for existing
/// // transactions.  We must not complete the detach until all such queries are
/// // complete (otherwise we present them an inconsistent view of catalogs).
/// WaitForOlderSnapshots(snap->xmin, false);
/// DetachPartitionFinalize(rel, partRel, true, InvalidOid);
/// ObjectAddressSet(address, RelationRelationId, RelationGetRelid(partRel));
/// table_close(partRel, NoLock);
/// ```
pub(crate) fn ATExecDetachPartitionFinalize<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    cmd: &PartitionCmd<'mcx>,
) -> PgResult<ObjectAddress> {
    // Snapshot snap = GetActiveSnapshot();
    let snap_xmin = snapmgr_seams::get_active_snapshot::call()?
        .map(|s| s.xmin)
        .unwrap_or(::types_core::xact::InvalidTransactionId);

    // partRel = table_openrv(name, AccessExclusiveLock);
    let name_node = cmd
        .name
        .as_deref()
        .ok_or_else(|| elog("DETACH PARTITION FINALIZE: PartitionCmd has no relation name"))?;
    let name = name_node
        .as_rangevar()
        .ok_or_else(|| elog("DETACH PARTITION FINALIZE: PartitionCmd name is not a RangeVar"))?;
    let access_rv = to_access_range_var(name);
    let partRel = table_openrv(mcx, &access_rv, AccessExclusiveLock)?;

    // Wait until existing snapshots are gone. Cancel-then-FINALIZE must not
    // complete the detach until all queries that could still see the partition
    // as attached have ended.
    indexcmds::WaitForOlderSnapshots(mcx, snap_xmin, false)?;

    // DetachPartitionFinalize(rel, partRel, true, InvalidOid);
    DetachPartitionFinalize(mcx, rel, &partRel, true, ::types_core::primitive::InvalidOid)?;

    // ObjectAddressSet(address, RelationRelationId, RelationGetRelid(partRel));
    let address = object_address_set(RelationRelationId, partRel.rd_id);

    // table_close(partRel, NoLock);  -- keep our AccessExclusiveLock until commit.
    partRel.close(NoLock)?;

    Ok(address)
}
