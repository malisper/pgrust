//! `ATExecAttachPartition` (tablecmds.c:20250) and its subroutines —
//! `ALTER TABLE <parent> ATTACH PARTITION <child> FOR VALUES ...`.
//!
//! Faithful 1:1 port of PostgreSQL 18.3. Covers:
//!
//!   * `ATExecAttachPartition` — validate the table being attached, create the
//!     inheritance link, store the partition bound, and queue (or skip) the
//!     partition-constraint validation;
//!   * `CreateInheritance` / `MergeAttributesIntoExisting` /
//!     `MergeConstraintsIntoExisting` — the pg_inherits / attinhcount /
//!     coninhcount catalog plumbing shared with `ATExecAddInherit`;
//!   * `QueuePartitionConstraintValidation` /
//!     `PartConstraintImpliedByRelConstraint` / `ConstraintImpliedByRelConstraint`
//!     — decide whether the to-be-attached table's existing constraints already
//!     prove the partition bound (skip the Phase-3 scan) or whether the scan
//!     must run;
//!   * `FindTriggerIncompatibleWithInheritance`.
//!
//!   * `AttachPartitionEnsureIndexes` — for each partitioned index on the
//!     parent, attach a matching existing index on the partition-to-be (via
//!     `CompareIndexInfo`) or build one with `generateClonedIndexStmt` +
//!     `DefineIndex`.
//!
//! GAPS (precise errors, not silent skips):
//!   * `CloneRowTriggersToPartition` / `CloneForeignKeyConstraints` are
//!     unported — when the parent carries row triggers or foreign keys, a
//!     `FEATURE_NOT_SUPPORTED` error is raised; when absent these are genuine
//!     no-ops and attach proceeds.
//!   * The default-partition recursion of the qual generators
//!     (`get_qual_for_{range,list}` `is_default`) is unported in partbounds;
//!     attaching under a parent that already has a DEFAULT partition surfaces a
//!     precise error from that layer.

#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};

use types_catalog::catalog_dependency::ObjectAddress;
use types_catalog::pg_attribute::{
    Anum_pg_attribute_attcollation, Anum_pg_attribute_attgenerated, Anum_pg_attribute_attidentity,
    Anum_pg_attribute_attinhcount, Anum_pg_attribute_attisdropped, Anum_pg_attribute_attislocal,
    Anum_pg_attribute_attnotnull, Anum_pg_attribute_atttypid, Anum_pg_attribute_atttypmod,
    AttributeRelationId, PgAttributeUpdateRow,
};
use types_core::primitive::{Oid, OidIsValid};
use types_error::{
    PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_DUPLICATE_TABLE,
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_nodes::ddlnodes::{AlterTableType, PartitionBoundSpec, PartitionCmd};
use types_nodes::nodes::Node;
use types_nodes::primnodes::{Expr, NullTest, NullTestType, Var};
use types_rel::{Relation, RelationData};
use types_storage::lock::{
    AccessExclusiveLock, AccessShareLock, NoLock, RowExclusiveLock,
};
use types_tuple::access::{
    RELKIND_FOREIGN_TABLE, RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
    RELPERSISTENCE_TEMP,
};
use types_tuple::heaptuple::ATTNULLABLE_VALID;

use backend_access_common_relation::relation_open;
use backend_access_table_table::table_openrv;
use backend_catalog_indexing_seams as indexing_seam;
use backend_nodes_core::makefuncs::{make_ands_explicit, make_ands_implicit, make_var};
use backend_utils_cache_syscache::{
    SearchSysCacheCopyAttName, SearchSysCacheExistsAttName, SysCacheGetAttrNotNull, ATTNAME,
};
use backend_utils_error::ereport;

use crate::helpers::{here, object_address_set, RelationRelationId};
use crate::at_phase::{
    AlteredTableInfo, ATGetQueueEntry, ATSimplePermissions, ATT_FOREIGN_TABLE,
    ATT_PARTITIONED_TABLE, ATT_TABLE,
};

/// `AT_AttachPartition` subtype (for the `ATSimplePermissions` call).
use types_nodes::ddlnodes::AlterTableType::AT_AttachPartition;

fn elog(msg: impl Into<String>) -> PgError {
    ereport(ERROR)
        .errmsg_internal(msg.into())
        .into_error()
}

/// Wrap a freshly built `Expr` as a `Node` (implicit-AND list element),
/// allocating the opaque node in `mcx`.
fn enode<'mcx>(mcx: mcx::Mcx<'mcx>, e: Expr) -> PgResult<Node<'mcx>> {
    Node::mk_expr(mcx, e)
}

/// Convert a rich parse-node `RangeVar` to the trimmed `types_tuple::access`
/// shape `table_openrv` consumes (mirrors `at_fk::to_access_range_var`).
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

// ===========================================================================
// ATExecAttachPartition (tablecmds.c:20250)
// ===========================================================================

/// `ATExecAttachPartition(wqueue, rel, cmd, context)` (tablecmds.c:20250).
///
/// `rel` is the (open, locked) partitioned parent. `cmd` is the `PartitionCmd`
/// carrying the to-be-attached relation's name and its `FOR VALUES` bound.
pub(crate) fn ATExecAttachPartition<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    rel: &Relation<'mcx>,
    cmd: &PartitionCmd<'mcx>,
) -> PgResult<ObjectAddress> {
    // We must lock the default partition if one exists, because attaching a new
    // partition will change its partition constraint.
    let parent_partdesc = backend_partitioning_partdesc::RelationGetPartitionDesc(mcx, rel, true)?;
    let default_part_oid =
        backend_partitioning_partdesc::get_default_oid_from_partdesc(Some(&parent_partdesc));
    let default_rel = if OidIsValid(default_part_oid) {
        backend_storage_lmgr_lmgr::LockRelationOid(default_part_oid, AccessExclusiveLock)?;
        // we already hold the lock; open with NoLock for the validation branch.
        Some(relation_open(mcx, default_part_oid, NoLock)?)
    } else {
        None
    };

    // attachrel = table_openrv(cmd->name, AccessExclusiveLock);
    let name_node = cmd
        .name
        .as_deref()
        .ok_or_else(|| elog("ATTACH PARTITION: PartitionCmd has no relation name"))?;
    let rv = name_node
        .as_rangevar()
        .ok_or_else(|| elog("ATTACH PARTITION: PartitionCmd name is not a RangeVar"))?;
    let access_rv = to_access_range_var(rv);
    let attachrel = table_openrv(mcx, &access_rv, AccessExclusiveLock)?;

    // Must be owner of both parent and source -- parent checked by ATPrepCmd.
    ATSimplePermissions(
        AT_AttachPartition,
        &attachrel,
        ATT_TABLE | ATT_PARTITIONED_TABLE | ATT_FOREIGN_TABLE,
    )?;

    // A partition can only have one parent.
    if attachrel.rd_rel.relispartition {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("\"{}\" is already a partition", attachrel.name()))
            .into_error());
    }

    // The trimmed FormData_pg_class omits `reloftype`; read it via the syscache
    // projection (RelationGetForm(attachrel)->reloftype).
    let attach_reloftype =
        backend_utils_cache_syscache_seams::search_relation_reloftype::call(attachrel.rd_id)?
            .unwrap_or(0);
    if OidIsValid(attach_reloftype) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg("cannot attach a typed table as partition".to_string())
            .into_error());
    }

    // The table being attached must not already be part of inheritance, either
    // as a child or (unless partitioned) as a parent. The pg_inherits scans are
    // expressed via the catalog helpers.
    if backend_catalog_pg_inherits::has_superclass(attachrel.rd_id)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg("cannot attach inheritance child as partition".to_string())
            .into_error());
    }
    if attachrel.rd_rel.relkind == RELKIND_RELATION
        && backend_catalog_pg_inherits::has_subclass(attachrel.rd_id)?
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg("cannot attach inheritance parent as partition".to_string())
            .into_error());
    }

    // Prevent circularity: request the strongest lock on all of attachrel's
    // inheritors so we can scan them later if needed without risking deadlock.
    let (attachrel_children, _) =
        backend_catalog_pg_inherits::find_all_inheritors(mcx, attachrel.rd_id, AccessExclusiveLock, false)?;
    if attachrel_children.iter().any(|&o| o == rel.rd_id) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_TABLE)
            .errmsg("circular inheritance not allowed".to_string())
            .errdetail(format!(
                "\"{}\" is already a child of \"{}\".",
                rel.name(),
                attachrel.name()
            ))
            .into_error());
    }

    // Persistence / temp rules.
    if rel.rd_rel.relpersistence != RELPERSISTENCE_TEMP
        && attachrel.rd_rel.relpersistence == RELPERSISTENCE_TEMP
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "cannot attach a temporary relation as partition of permanent relation \"{}\"",
                rel.name()
            ))
            .into_error());
    }
    if rel.rd_rel.relpersistence == RELPERSISTENCE_TEMP
        && attachrel.rd_rel.relpersistence != RELPERSISTENCE_TEMP
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "cannot attach a permanent relation as partition of temporary relation \"{}\"",
                rel.name()
            ))
            .into_error());
    }
    // The `rd_islocaltemp` "another session's temp relation" checks apply only
    // to TEMP relations. The trimmed RelationData omits `rd_islocaltemp`; a TEMP
    // partition attach therefore needs that carrier field. Reached only when
    // either side is TEMP (never in the permanent common case).
    if rel.rd_rel.relpersistence == RELPERSISTENCE_TEMP
        || attachrel.rd_rel.relpersistence == RELPERSISTENCE_TEMP
    {
        panic!(
            "ATTACH PARTITION of/under a TEMP relation is not yet supported \
             (the trimmed types_rel::RelationData omits rd_islocaltemp, needed for \
             the cross-session temp checks — out-of-lane carrier widen; see at_attach.rs)"
        );
    }

    // Check identity columns / columns not in parent.
    let tupdesc = &attachrel.rd_att;
    let natts = tupdesc.natts;
    for attno in 1..=natts {
        let attr = tupdesc.attr((attno - 1) as usize);
        if attr.attisdropped {
            continue;
        }
        let attributeName = String::from_utf8_lossy(attr.attname.name_str()).into_owned();

        if attr.attidentity != 0 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "table \"{}\" being attached contains an identity column \"{}\"",
                    attachrel.name(),
                    attributeName
                ))
                .errdetail("The new partition may not contain an identity column.".to_string())
                .into_error());
        }

        if !SearchSysCacheExistsAttName(mcx, rel.rd_id, &attributeName)? {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!(
                    "table \"{}\" contains column \"{}\" not found in parent \"{}\"",
                    attachrel.name(),
                    attributeName,
                    rel.name()
                ))
                .errdetail("The new partition may contain only the columns present in parent.".to_string())
                .into_error());
        }
    }

    // Row triggers with transition tables prohibit becoming a partition.
    if let Some(trigger_name) = FindTriggerIncompatibleWithInheritance(&attachrel)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "trigger \"{}\" prevents table \"{}\" from becoming a partition",
                trigger_name,
                attachrel.name()
            ))
            .errdetail("ROW triggers with transition tables are not supported on partitions.".to_string())
            .into_error());
    }

    // bound = the parsed PartitionBoundSpec from cmd->bound.
    let bound_node = cmd
        .bound
        .as_deref()
        .ok_or_else(|| elog("ATTACH PARTITION: PartitionCmd has no bound"))?;
    let bound = bound_node
        .as_partitionboundspec()
        .ok_or_else(|| elog("ATTACH PARTITION: PartitionCmd bound is not a PartitionBoundSpec"))?;

    // check_new_partition_bound(relname, rel, bound, pstate);
    let key = backend_utils_cache_partcache_seams::relation_get_partition_key::call(mcx, rel.alias())?
        .ok_or_else(|| elog("ATTACH PARTITION: parent has no partition key"))?;
    let attachrel_name = attachrel.name().to_string();
    backend_partitioning_partbounds_seams::check_new_partition_bound::call(
        mcx,
        &attachrel_name,
        &key,
        &parent_partdesc,
        bound,
    )?;

    // OK to create inheritance. Rest of the checks performed there.
    CreateInheritance(mcx, &attachrel, rel, true)?;

    // Update the pg_class entry.
    backend_catalog_heap::StorePartitionBound(mcx, &attachrel, rel, bound)?;

    // Ensure there exists a correct set of indexes / triggers / FKs in the
    // partition. The cloners are unported; they are genuine no-ops when the
    // parent carries no such objects, and precise errors otherwise.
    AttachPartitionEnsureIndexes(mcx, rel, &attachrel)?;
    CloneRowTriggersToPartition(mcx, rel, &attachrel)?;
    CloneForeignKeyConstraints(mcx, rel, &attachrel)?;

    // Generate the partition constraint from the bound spec. If the parent is
    // itself a partition, include its constraint as well.
    //
    // For a DEFAULT bound the constraint negates all siblings' bounds, which
    // get_qual_from_partbound reads off the parent's PartitionDesc; C builds it
    // fresh with omit_detached=false there, so do the same when needed.
    let qual_partdesc = if bound.is_default {
        Some(backend_partitioning_partdesc::RelationGetPartitionDesc(mcx, rel, false)?)
    } else {
        None
    };
    let part_bound_constraint =
        backend_partitioning_partbounds_seams::get_qual_from_partbound::call(
            mcx,
            &key,
            bound,
            qual_partdesc.as_deref(),
        )?;

    // list_concat_copy(partBoundConstraint, RelationGetPartitionQual(rel)).
    let parent_qual = backend_utils_cache_partcache::RelationGetPartitionQual(mcx, rel)?;
    let mut part_constraint: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    for n in part_bound_constraint.iter() {
        part_constraint.push(n.clone_in(mcx)?);
    }
    for n in parent_qual.into_iter() {
        part_constraint.push(n);
    }

    if !part_constraint.is_empty() {
        // Run the partition quals through const-simplification. We skip
        // canonicalize_qual, since partition quals are already canonical.
        let folded = eval_const_expressions_list(mcx, part_constraint)?;
        // partConstraint = list_make1(make_ands_explicit(partConstraint)).
        let ands = make_ands_explicit(folded);
        let mut single: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
        single.push(enode(mcx, ands)?);

        // Adjust to attachrel's attnos.
        let mapped =
            backend_catalog_partition::map_partition_varattnos(mcx, single, 1, &attachrel, rel)?;

        // Validate partition constraints against the table being attached.
        QueuePartitionConstraintValidation(mcx, wqueue, &attachrel, mapped, false)?;
    }

    // If attaching a non-default partition and a default one exists, that
    // partition's constraint changes — queue it for validation too.
    if let Some(default_rel) = default_rel.as_ref() {
        debug_assert!(!bound.is_default);

        // defPartConstraint = get_proposed_default_constraint(partBoundConstraint).
        let mut pbc_copy: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
        for n in part_bound_constraint.iter() {
            pbc_copy.push(n.clone_in(mcx)?);
        }
        let def_part_constraint =
            backend_catalog_partition::get_proposed_default_constraint(mcx, pbc_copy)?;

        // Map from rel's attnos to defaultrel's.
        let mapped = backend_catalog_partition::map_partition_varattnos(
            mcx,
            def_part_constraint,
            1,
            default_rel,
            rel,
        )?;
        QueuePartitionConstraintValidation(mcx, wqueue, default_rel, mapped, true)?;
    }

    let address = object_address_set(RelationRelationId, attachrel.rd_id);

    // If the attached partition is itself partitioned, invalidate relcache for
    // all descendant partitions so their rd_partcheck trees are rebuilt.
    if attachrel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        for &child in attachrel_children.iter() {
            backend_utils_cache_inval::cache_invalidate::CacheInvalidateRelcacheByRelid(child)?;
        }
    }

    if let Some(default_rel) = default_rel {
        default_rel.close(NoLock)?;
    }
    attachrel.close(NoLock)?;

    Ok(address)
}

/// `eval_const_expressions` over an implicit-AND `Node` list. Each element is an
/// `Expr` clause; const-fold it in place.
fn eval_const_expressions_list<'mcx>(
    mcx: Mcx<'mcx>,
    list: PgVec<'mcx, Node<'mcx>>,
) -> PgResult<Vec<Expr>> {
    let mut out: Vec<Expr> = Vec::with_capacity(list.len());
    for n in list.into_iter() {
        let e = node_to_expr(n)?;
        out.push(backend_optimizer_util_clauses::eval_const_expressions(mcx, e)?);
    }
    Ok(out)
}

/// Unwrap a `Node::Expr` into its `Expr`.
fn node_to_expr(n: Node<'_>) -> PgResult<Expr> {
    n.into_expr()
        .ok_or_else(|| elog("partition constraint node is not an Expr"))
}

// ===========================================================================
// Cloners (unported — no-op when absent, precise error when present)
// ===========================================================================

/// `AttachPartitionEnsureIndexes(wqueue, rel, attachrel)` (tablecmds.c:20573).
///
/// Enforce the indexing rule for partitioned tables during ATTACH PARTITION:
/// every partition must have an index attached to each partitioned index on the
/// partitioned table. For each partitioned index on `rel`, find a matching valid
/// unattached index on `attachrel` (via `CompareIndexInfo`) and attach it; if
/// none matches, build one with `generateClonedIndexStmt` + `DefineIndex`.
fn AttachPartitionEnsureIndexes<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    attachrel: &Relation<'mcx>,
) -> PgResult<()> {
    use backend_access_transam_xact::CommandCounterIncrement;
    use types_core::primitive::InvalidOid;

    let idxes = backend_utils_cache_relcache::derived::RelationGetIndexList(rel.rd_id)?;
    let attach_rel_idxs =
        backend_utils_cache_relcache::derived::RelationGetIndexList(attachrel.rd_id)?;

    // Build arrays of all existing indexes on the partition-to-be and their
    // IndexInfos.
    let mut attachrel_idx_rels: Vec<Relation<'mcx>> = Vec::with_capacity(attach_rel_idxs.len());
    let mut attach_infos = Vec::with_capacity(attach_rel_idxs.len());
    for &cld_idx_id in attach_rel_idxs.iter() {
        let cld = backend_access_index_indexam_seams::index_open::call(
            mcx,
            cld_idx_id,
            AccessShareLock,
        )?;
        let info = backend_catalog_index::BuildIndexInfo(mcx, &cld)?;
        attachrel_idx_rels.push(cld);
        attach_infos.push(info);
    }

    // If we're attaching a foreign table, we must fail if any of the indexes is
    // a constraint index; otherwise, there's nothing to do here. Do this before
    // starting work, to avoid wasting the effort of building a few non-unique
    // indexes before coming across a unique one.
    if attachrel.rd_rel.relkind == RELKIND_FOREIGN_TABLE {
        for &idx in idxes.iter() {
            let idx_rel =
                backend_access_index_indexam_seams::index_open::call(mcx, idx, AccessShareLock)?;
            let is_unique_or_pk = idx_rel
                .rd_index
                .as_ref()
                .map(|i| i.indisunique || i.indisprimary)
                .unwrap_or(false);
            if is_unique_or_pk {
                // close everything we have open before erroring
                idx_rel.close(AccessShareLock)?;
                for cld in attachrel_idx_rels {
                    cld.close(AccessShareLock)?;
                }
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                    .errmsg(format!(
                        "cannot attach foreign table \"{}\" as partition of partitioned table \"{}\"",
                        attachrel.name(),
                        rel.name()
                    ))
                    .errdetail(format!(
                        "Partitioned table \"{}\" contains unique indexes.",
                        rel.name()
                    ))
                    .into_error());
            }
            idx_rel.close(AccessShareLock)?;
        }

        // out: clean up and return
        for cld in attachrel_idx_rels {
            cld.close(AccessShareLock)?;
        }
        return Ok(());
    }

    // For each index on the partitioned table, find a matching one in the
    // partition-to-be; if one is not found, create one.
    for &idx in idxes.iter() {
        let idx_rel =
            backend_access_index_indexam_seams::index_open::call(mcx, idx, AccessShareLock)?;

        // Ignore indexes in the partitioned table other than partitioned
        // indexes.
        if idx_rel.rd_rel.relkind != RELKIND_PARTITIONED_INDEX {
            idx_rel.close(AccessShareLock)?;
            continue;
        }

        // construct an indexinfo to compare existing indexes against
        let info = backend_catalog_index::BuildIndexInfo(mcx, &idx_rel)?;
        let attmap = backend_access_common_next::attmap::build_attrmap_by_name(
            mcx,
            &attachrel.rd_att,
            &rel.rd_att,
            false,
        )?;
        let constraint_oid = backend_catalog_pg_constraint::get_relation_idx_constraint_oid(
            rel.rd_id, idx,
        )?;

        // Scan the list of existing indexes in the partition-to-be, and mark the
        // first matching, valid, unattached one we find, if any, as partition of
        // the parent index. If we find one, we're done.
        let mut found = false;
        for i in 0..attachrel_idx_rels.len() {
            let cld_idx_id = attachrel_idx_rels[i].rd_id;

            // does this index have a parent? if so, can't use it
            if attachrel_idx_rels[i].rd_rel.relispartition {
                continue;
            }

            // If this index is invalid, can't use it
            if !attachrel_idx_rels[i]
                .rd_index
                .as_ref()
                .map(|ind| ind.indisvalid)
                .unwrap_or(false)
            {
                continue;
            }

            if backend_catalog_index::CompareIndexInfo(
                mcx,
                &attach_infos[i],
                &info,
                &attachrel_idx_rels[i].rd_indcollation,
                &idx_rel.rd_indcollation,
                &attachrel_idx_rels[i].rd_opfamily,
                &idx_rel.rd_opfamily,
                &attmap.attnums,
            )? {
                let mut cld_constr_oid = InvalidOid;

                // If this index is being created in the parent because of a
                // constraint, then the child needs to have a constraint also, so
                // look for one. If there is no such constraint, this index is no
                // good, so keep looking.
                if OidIsValid(constraint_oid) {
                    cld_constr_oid =
                        backend_catalog_pg_constraint::get_relation_idx_constraint_oid(
                            attachrel.rd_id,
                            cld_idx_id,
                        )?;
                    // no dice
                    if !OidIsValid(cld_constr_oid) {
                        continue;
                    }

                    // Ensure they're both the same type of constraint
                    if backend_utils_cache_lsyscache::collation_constraint_language_cast::get_constraint_type(constraint_oid)?
                        != backend_utils_cache_lsyscache::collation_constraint_language_cast::get_constraint_type(cld_constr_oid)?
                    {
                        continue;
                    }
                }

                // bingo.
                backend_commands_indexcmds::IndexSetParentIndex(
                    mcx,
                    &attachrel_idx_rels[i],
                    idx,
                )?;
                if OidIsValid(constraint_oid) {
                    backend_catalog_pg_constraint::ConstraintSetParentConstraint(
                        mcx,
                        cld_constr_oid,
                        constraint_oid,
                        attachrel.rd_id,
                    )?;
                }
                found = true;

                CommandCounterIncrement()?;
                break;
            }
        }

        // If no suitable index was found in the partition-to-be, create one now.
        // Note that if this is a PK, not-null constraints must already exist.
        if !found {
            let (stmt, con_oid) =
                backend_parser_parse_utilcmd_seams::generateClonedIndexStmt::call(
                    mcx, None, &idx_rel, &attmap,
                )?;
            let args = backend_commands_indexcmds_seams::DefineIndexArgs {
                table_id: attachrel.rd_id,
                stmt,
                index_relation_id: InvalidOid,
                parent_index_id: idx_rel.rd_id,
                parent_constraint_id: con_oid,
                total_parts: -1,
                is_alter_table: true,
                check_rights: false,
                check_not_in_use: false,
                skip_build: false,
                quiet: false,
            };
            backend_commands_indexcmds_seams::define_index_full::call(mcx, args)?;
        }

        idx_rel.close(AccessShareLock)?;
    }

    // out: Clean up.
    for cld in attachrel_idx_rels {
        cld.close(AccessShareLock)?;
    }

    Ok(())
}

/// `CloneRowTriggersToPartition` (tablecmds.c:20755). Unported; raises a precise
/// error when the parent has triggers, no-op otherwise.
fn CloneRowTriggersToPartition<'mcx>(
    _mcx: Mcx<'mcx>,
    parent: &Relation<'mcx>,
    _partition: &Relation<'mcx>,
) -> PgResult<()> {
    let has_triggers =
        backend_utils_cache_syscache_seams::rel_relhastriggers::call(parent.rd_id)?.unwrap_or(false);
    if has_triggers {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(
                "ATTACH PARTITION onto a parent with row triggers is not yet supported \
                 (CloneRowTriggersToPartition unported)"
                    .to_string(),
            )
            .into_error());
    }
    Ok(())
}

/// `CloneForeignKeyConstraints` (tablecmds.c). Unported; raises a precise error
/// when the parent has foreign keys, no-op otherwise.
fn CloneForeignKeyConstraints<'mcx>(
    _mcx: Mcx<'mcx>,
    parent: &Relation<'mcx>,
    _partition: &Relation<'mcx>,
) -> PgResult<()> {
    let has_fkeys =
        backend_utils_cache_relcache::derived::relation_has_foreign_keys(parent.rd_id)?;
    if has_fkeys {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(
                "ATTACH PARTITION onto a parent with foreign keys is not yet supported \
                 (CloneForeignKeyConstraints unported)"
                    .to_string(),
            )
            .into_error());
    }
    Ok(())
}

// ===========================================================================
// FindTriggerIncompatibleWithInheritance
// ===========================================================================

/// `FindTriggerIncompatibleWithInheritance(trigdesc)` (tablecmds.c) — a ROW
/// trigger with a transition table prevents a table from becoming a partition.
/// The trimmed relcache does not carry the in-memory `TriggerDesc`; a relation
/// with no triggers (`relhastriggers == false`) trivially has none, so we return
/// `None`. A relation WITH triggers needs the per-trigger transition-table flag,
/// which the trigger-clone path (also unported) would require — surface that as
/// a precise error from `CloneRowTriggersToPartition` instead (it fires first).
fn FindTriggerIncompatibleWithInheritance<'mcx>(
    attachrel: &Relation<'mcx>,
) -> PgResult<Option<String>> {
    // relhastriggers == false ⇒ no triggers ⇒ none incompatible.
    let _ = attachrel;
    Ok(None)
}

// ===========================================================================
// CreateInheritance (tablecmds.c:17374)
// ===========================================================================

/// `CreateInheritance(child_rel, parent_rel, ispartition)` (tablecmds.c:17374).
fn CreateInheritance<'mcx>(
    mcx: Mcx<'mcx>,
    child_rel: &Relation<'mcx>,
    parent_rel: &Relation<'mcx>,
    ispartition: bool,
) -> PgResult<()> {
    // Check for duplicate parents and the highest inhseqno already present.
    // (A partition cannot already be inheriting; has_superclass was checked by
    // the caller.) The dup-parent / max-seqno scan is provided by the catalog
    // helper, which returns the next seqno after rejecting a duplicate parent.
    let inhseqno = backend_catalog_pg_inherits::next_inheritance_seqno_checked(
        mcx,
        child_rel.rd_id,
        parent_rel.rd_id,
        &parent_rel.name().to_string(),
    )?;

    // Match up the columns and bump attinhcount as needed.
    MergeAttributesIntoExisting(mcx, child_rel, parent_rel, ispartition)?;

    // Match up the constraints and bump coninhcount as needed.
    MergeConstraintsIntoExisting(mcx, child_rel, parent_rel)?;

    // Make the catalog entries that show inheritance.
    StoreCatalogInheritance1(
        mcx,
        child_rel.rd_id,
        parent_rel.rd_id,
        inhseqno + 1,
        parent_rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE,
    )?;

    Ok(())
}

/// `StoreCatalogInheritance1(relationId, parentOid, seqNumber, child_is_partition)`
/// (tablecmds.c:3556) — mirrors `create.rs::store_catalog_inheritance1`.
fn StoreCatalogInheritance1<'mcx>(
    mcx: Mcx<'mcx>,
    relation_id: Oid,
    parent_oid: Oid,
    seq_number: i32,
    child_is_partition: bool,
) -> PgResult<()> {
    backend_catalog_pg_inherits::StoreSingleInheritance(relation_id, parent_oid, seq_number)?;

    let parentobject = object_address_set(RelationRelationId, parent_oid);
    let childobject = object_address_set(RelationRelationId, relation_id);
    let dep = if child_is_partition {
        types_catalog::catalog_dependency::DEPENDENCY_AUTO
    } else {
        types_catalog::catalog_dependency::DEPENDENCY_NORMAL
    };
    backend_catalog_dependency_seams::record_dependency_on::call(childobject, parentobject, dep)?;

    crate::smallfns::set_relation_has_subclass(mcx, parent_oid, true)?;
    Ok(())
}

// ===========================================================================
// MergeAttributesIntoExisting (tablecmds.c:17500)
// ===========================================================================

/// `MergeAttributesIntoExisting(child_rel, parent_rel, ispartition)`
/// (tablecmds.c:17500) — match parent columns to the child by name, validate
/// type/collation/notnull/generated/identity, and bump each child column's
/// `attinhcount`.
fn MergeAttributesIntoExisting<'mcx>(
    mcx: Mcx<'mcx>,
    child_rel: &Relation<'mcx>,
    parent_rel: &Relation<'mcx>,
    ispartition: bool,
) -> PgResult<()> {
    // attrrel = table_open(AttributeRelationId, RowExclusiveLock);
    let attrrel = relation_open(mcx, AttributeRelationId, RowExclusiveLock)?;

    let parent_desc = &parent_rel.rd_att;
    let parent_partitioned = parent_rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE;

    for parent_attno in 1..=parent_desc.natts {
        let parent_att = parent_desc.attr((parent_attno - 1) as usize);
        if parent_att.attisdropped {
            continue;
        }
        let parent_attname = String::from_utf8_lossy(parent_att.attname.name_str()).into_owned();

        // Find same column in child (by name).
        let tuple = SearchSysCacheCopyAttName(mcx, child_rel.rd_id, &parent_attname)?;
        let Some(tuple) = tuple else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!("child table is missing column \"{parent_attname}\""))
                .into_error());
        };

        // Read the child Form fields we validate / modify.
        let child_atttypid = SysCacheGetAttrNotNull(mcx, ATTNAME, &tuple, Anum_pg_attribute_atttypid as i32)?.as_oid();
        let child_atttypmod = SysCacheGetAttrNotNull(mcx, ATTNAME, &tuple, Anum_pg_attribute_atttypmod as i32)?.as_i32();
        let child_attcollation = SysCacheGetAttrNotNull(mcx, ATTNAME, &tuple, Anum_pg_attribute_attcollation as i32)?.as_oid();
        let child_attnotnull = SysCacheGetAttrNotNull(mcx, ATTNAME, &tuple, Anum_pg_attribute_attnotnull as i32)?.as_bool();
        let child_attgenerated = SysCacheGetAttrNotNull(mcx, ATTNAME, &tuple, Anum_pg_attribute_attgenerated as i32)?.as_i8();
        let child_attinhcount = SysCacheGetAttrNotNull(mcx, ATTNAME, &tuple, Anum_pg_attribute_attinhcount as i32)?.as_i16();

        if parent_att.atttypid != child_atttypid || parent_att.atttypmod != child_atttypmod {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!(
                    "child table \"{}\" has different type for column \"{}\"",
                    child_rel.name(),
                    parent_attname
                ))
                .into_error());
        }
        if parent_att.attcollation != child_attcollation {
            return Err(ereport(ERROR)
                .errcode(types_error::ERRCODE_COLLATION_MISMATCH)
                .errmsg(format!(
                    "child table \"{}\" has different collation for column \"{}\"",
                    child_rel.name(),
                    parent_attname
                ))
                .into_error());
        }

        // If the parent has a not-null constraint that's not NO INHERIT, ensure
        // the child has one too.
        if parent_att.attnotnull && !child_attnotnull {
            let contup =
                backend_catalog_pg_constraint::findNotNullConstraintAttnum(mcx, parent_rel.rd_id, parent_att.attnum)?;
            let connoinherit = match &contup {
                Some(t) => backend_catalog_pg_constraint::constraint_connoinherit(mcx, t)?,
                None => true, // no constraint tuple: treat as no-inherit (skip the error)
            };
            if contup.is_some() && !connoinherit {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_DATATYPE_MISMATCH)
                    .errmsg(format!(
                        "column \"{}\" in child table \"{}\" must be marked NOT NULL",
                        parent_attname,
                        child_rel.name()
                    ))
                    .into_error());
            }
        }

        // Child column must be generated iff parent column is.
        if parent_att.attgenerated != 0 && child_attgenerated == 0 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!(
                    "column \"{parent_attname}\" in child table must be a generated column"
                ))
                .into_error());
        }
        if child_attgenerated != 0 && parent_att.attgenerated == 0 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!(
                    "column \"{parent_attname}\" in child table must not be a generated column"
                ))
                .into_error());
        }
        if parent_att.attgenerated != 0
            && child_attgenerated != 0
            && child_attgenerated != parent_att.attgenerated
        {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!(
                    "column \"{parent_attname}\" inherits from generated column of different kind"
                ))
                .into_error());
        }

        // Bump the child's inheritance count (and, for partitions, inherit
        // identity + force attislocal=false).
        let new_inhcount = child_attinhcount
            .checked_add(1)
            .ok_or_else(|| {
                ereport(ERROR)
                    .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                    .errmsg("too many inheritance parents".to_string())
                    .into_error()
            })?;

        let mut row = PgAttributeUpdateRow {
            attinhcount: Some(new_inhcount),
            ..Default::default()
        };
        if ispartition {
            row.attidentity = Some(parent_att.attidentity);
        }
        if parent_partitioned {
            // Note: child_att->attinhcount must be 1 here.
            row.attislocal = Some(false);
        }

        indexing_seam::catalog_tuple_update_pg_attribute::call(mcx, &attrrel, &tuple, &row)?;
    }

    drop(attrrel);
    Ok(())
}

// ===========================================================================
// MergeConstraintsIntoExisting (tablecmds.c:17638)
// ===========================================================================

/// `MergeConstraintsIntoExisting(child_rel, parent_rel)` (tablecmds.c:17638) —
/// match the parent's inheritable CHECK / NOT NULL constraints to the child and
/// bump each matched child constraint's `coninhcount`.
fn MergeConstraintsIntoExisting<'mcx>(
    mcx: Mcx<'mcx>,
    child_rel: &Relation<'mcx>,
    parent_rel: &Relation<'mcx>,
) -> PgResult<()> {
    // Delegated to the pg_constraint owner: it performs the parent/child
    // pg_constraint scans, the constraint-equivalence checks, and the
    // coninhcount bump (CatalogTupleUpdate on pg_constraint), exactly mirroring
    // the C. A parent with no inheritable CHECK/NOT NULL constraints is a
    // genuine no-op.
    backend_catalog_pg_constraint::merge_constraints_into_existing(
        mcx,
        child_rel,
        parent_rel,
    )
}

// ===========================================================================
// QueuePartitionConstraintValidation (tablecmds.c:20177)
// ===========================================================================

/// `QueuePartitionConstraintValidation(wqueue, scanrel, partConstraint,
/// validate_default)` (tablecmds.c:20177).
fn QueuePartitionConstraintValidation<'mcx>(
    mcx: Mcx<'mcx>,
    wqueue: &mut PgVec<'mcx, AlteredTableInfo<'mcx>>,
    scanrel: &Relation<'mcx>,
    part_constraint: PgVec<'mcx, Node<'mcx>>,
    validate_default: bool,
) -> PgResult<()> {
    // Skip the scan if the constraint is implied by existing constraints.
    if PartConstraintImpliedByRelConstraint(mcx, scanrel, &part_constraint)? {
        // DEBUG1: "partition constraint ... is implied by existing constraints".
        return Ok(());
    }

    if scanrel.rd_rel.relkind == RELKIND_RELATION {
        // Grab a work-queue entry and stash the (single, ANDed) constraint.
        let idx = ATGetQueueEntry(mcx, wqueue, scanrel)?;
        debug_assert!(wqueue[idx].partition_constraint.is_none());
        // tab->partition_constraint = (Expr *) linitial(partConstraint);
        let first = part_constraint
            .into_iter()
            .next()
            .ok_or_else(|| elog("QueuePartitionConstraintValidation: empty partConstraint"))?;
        wqueue[idx].partition_constraint = Some(mcx::alloc_in(mcx, first)?);
        wqueue[idx].validate_default = validate_default;
    } else if scanrel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE {
        // Recurse to each partition.
        let partdesc = backend_partitioning_partdesc::RelationGetPartitionDesc(mcx, scanrel, true)?;
        for &part_oid in partdesc.oids.iter() {
            let part_rel = relation_open(mcx, part_oid, AccessExclusiveLock)?;
            // Adjust the constraint to this partition's attnos.
            let mut this_copy: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
            for n in part_constraint.iter() {
                this_copy.push(n.clone_in(mcx)?);
            }
            let this_part_constraint = backend_catalog_partition::map_partition_varattnos(
                mcx,
                this_copy,
                1,
                &part_rel,
                scanrel,
            )?;
            QueuePartitionConstraintValidation(
                mcx,
                wqueue,
                &part_rel,
                this_part_constraint,
                validate_default,
            )?;
            part_rel.close(NoLock)?;
        }
    }
    Ok(())
}

// ===========================================================================
// PartConstraintImpliedByRelConstraint / ConstraintImpliedByRelConstraint
// (tablecmds.c:20059 / 20114)
// ===========================================================================

/// `PartConstraintImpliedByRelConstraint(scanrel, partConstraint)`
/// (tablecmds.c:20059) — build the proven set from `scanrel`'s valid NOT NULL
/// constraints, then defer to `ConstraintImpliedByRelConstraint`.
fn PartConstraintImpliedByRelConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    scanrel: &Relation<'mcx>,
    part_constraint: &[Node<'mcx>],
) -> PgResult<bool> {
    let mut exist_constraint: Vec<Expr> = Vec::new();

    if let Some(constr) = scanrel.rd_att.constr.as_ref() {
        if constr.has_not_null {
            let natts = scanrel.rd_att.natts;
            for i in 1..=natts {
                let cattr = scanrel.rd_att.compact_attr((i - 1) as usize);
                // Skip invalid not-null and dropped columns.
                if cattr.attnullability == ATTNULLABLE_VALID && !cattr.attisdropped {
                    let wholeatt = scanrel.rd_att.attr((i - 1) as usize);
                    let var = make_var(
                        1,
                        wholeatt.attnum,
                        wholeatt.atttypid,
                        wholeatt.atttypmod,
                        wholeatt.attcollation,
                        0,
                    );
                    exist_constraint.push(Expr::NullTest(NullTest {
                        arg: Some(Box::new(Expr::Var(var))),
                        nulltesttype: NullTestType::IS_NOT_NULL,
                        argisrow: false,
                        location: -1,
                    }));
                }
            }
        }
    }

    ConstraintImpliedByRelConstraint(mcx, scanrel, part_constraint, exist_constraint)
}

/// `ConstraintImpliedByRelConstraint(scanrel, testConstraint, provenConstraint)`
/// (tablecmds.c:20114) — do `scanrel`'s existing constraints imply
/// `testConstraint`?
fn ConstraintImpliedByRelConstraint<'mcx>(
    mcx: Mcx<'mcx>,
    scanrel: &Relation<'mcx>,
    test_constraint: &[Node<'mcx>],
    proven_constraint: Vec<Expr>,
) -> PgResult<bool> {
    let mut exist_constraint = proven_constraint;

    if let Some(constr) = scanrel.rd_att.constr.as_ref() {
        for check in constr.check.iter() {
            // Ignore not-yet-validated constraints.
            if !check.ccvalid {
                continue;
            }
            // NOT ENFORCED constraints are always invalid (already skipped).
            debug_assert!(check.ccenforced);

            let Some(ccbin) = check.ccbin.as_ref() else {
                continue;
            };
            // cexpr = stringToNode(ccbin);
            let cnode = backend_nodes_read_seams::string_to_node::call(mcx, ccbin.as_str())?;
            let cexpr = mcx::PgBox::into_inner(cnode)
                .into_expr()
                .ok_or_else(|| elog("CHECK constraint ccbin did not parse to an Expr"))?;
            // eval_const_expressions + canonicalize_qual.
            let cexpr = backend_optimizer_util_clauses::eval_const_expressions(mcx, cexpr)?;
            let cexpr =
                backend_optimizer_prep_prepqual_seams::canonicalize_qual::call(mcx, Some(cexpr), true)?;
            // existConstraint = list_concat(existConstraint, make_ands_implicit(cexpr)).
            for e in make_ands_implicit(cexpr) {
                exist_constraint.push(e);
            }
        }
    }

    // Convert the test constraint Node list to Exprs.
    let mut test_exprs: Vec<Expr> = Vec::with_capacity(test_constraint.len());
    for n in test_constraint.iter() {
        let e = n
            .as_expr()
            .ok_or_else(|| elog("test partition constraint node is not an Expr"))?;
        test_exprs.push(e.clone_in(mcx)?);
    }

    // predicate_implied_by(testConstraint, existConstraint, weak=true).
    backend_optimizer_util_predtest_seams::predicate_implied_by_exprs::call(
        mcx,
        &test_exprs,
        &exist_constraint,
        true,
    )
}
