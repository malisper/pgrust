// DefineRelation plain-table lane; BuildDescForRelation rides here as in 18.3.
#![allow(non_snake_case, non_upper_case_globals)]

mod alter;
mod partition;
mod constraints;
mod fk;
mod drop;
mod oncommit;
mod rename;
mod truncate;
pub use alter::{AlterTable, AlterTableGetLockLevel, AlterTableLookupRelation};
pub use rename::{renameatt, RenameRelation, RenameRelationInternal};
pub use drop::RemoveRelations;
pub use oncommit::{
    register_on_commit_action, remove_on_commit_action, AtEOSubXact_on_commit_actions,
    AtEOXact_on_commit_actions, PreCommit_on_commit_actions,
};
pub use truncate::ExecuteTruncate;

pub fn init_seams() {
    tablecmds_seams::rename_relation_internal::set(RenameRelationInternal);
    tablecmds_seams::range_var_callback_maintains_table::set(RangeVarCallbackMaintainsTable);
    tablecmds_seams::pre_commit_on_commit_actions::set(PreCommit_on_commit_actions);
    tablecmds_seams::at_eoxact_on_commit_actions::set(AtEOXact_on_commit_actions);
    tablecmds_seams::at_eosubxact_on_commit_actions::set(AtEOSubXact_on_commit_actions);
    tablecmds_seams::remove_on_commit_action::set(remove_on_commit_action);
}

use mcx::Mcx;
use types_core::{AttrNumber, InvalidOid, Oid, NAMEDATALEN};
use types_error::{PgError, PgResult, ERROR};
use types_nodes::rawnodes::{ColumnDef, CreateStmt, OnCommitAction, TypeName};
use types_rel::{RELKIND_RELATION, RELKIND_SEQUENCE};
use types_tuple::TupleDescData;

const HEAP_TABLE_AM_OID: Oid = 2;

// RangeVarCallbackMaintainsTable (tablecmds.c); shared by CLUSTER and
// REINDEX TABLE lookups.
pub fn RangeVarCallbackMaintainsTable(
    relation: &rel_vocab::RangeVar<'_>,
    relId: Oid,
    _oldRelId: Oid,
) -> PgResult<()> {
    if relId == InvalidOid {
        return Ok(());
    }
    let relkind = lsyscache::get_rel_relkind(relId)? as u8;
    if relkind == 0 {
        return Ok(());
    }
    if !matches!(
        relkind,
        RELKIND_RELATION
            | types_rel::RELKIND_TOASTVALUE
            | types_rel::RELKIND_MATVIEW
            | types_rel::RELKIND_PARTITIONED_TABLE
    ) {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "\"{}\" is not a table or materialized view",
                    relation.relname
                ),
            )
            .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }
    let aclresult =
        aclchk::pg_class_aclcheck(relId, miscinit::GetUserId(), adt_acl::ACL_MAINTAIN)?;
    if aclresult != aclchk::ACLCHECK_OK {
        // get_relkind_objtype: every reachable relkind maps to OBJECT_TABLE
        // except matview, where C says "permission denied for materialized
        // view" — error-text divergence until the matview lane lands.
        aclchk_seams::aclcheck_error::call(
            aclresult,
            types_nodes::parsenodes::ObjectType::OBJECT_TABLE as i32,
            relation.relname,
        )?;
    }
    Ok(())
}

#[cold]
#[inline(never)]
pub(crate) fn unported(what: &str) -> ! {
    panic!("unported: tablecmds {what}")
}

// BuildDescForRelation (tablecmds.c in 18.3).
pub fn BuildDescForRelation<'mcx>(
    mcx: Mcx<'mcx>,
    table_elts: &types_nodes::NodeList<'_>,
) -> PgResult<TupleDescData<'mcx>> {
    let natts = table_elts.len();
    let mut desc = tupdesc::CreateTemplateTupleDesc(mcx, natts as i32)?;

    for (i, elt) in table_elts.iter().enumerate() {
        let entry = elt.as_variant::<ColumnDef>().expect("ColumnDef");
        let attnum = (i + 1) as AttrNumber;
        let colname = entry.colname.expect("ColumnDef.colname");
        if colname.len() >= NAMEDATALEN as usize {
            unported("overlength column name truncation");
        }
        let tn = entry
            .typeName
            .expect("ColumnDef.typeName")
            .as_variant::<TypeName>()
            .expect("TypeName");
        let (atttypid, atttypmod) = parse_utilcmd::typenameTypeIdAndMod(mcx, None, tn)?;
        // GetColumnDefCollation: collClause is loud upstream; collOid is the
        // pre-cooked (LIKE) carrier, else the type's default.
        let attcollation = if entry.collOid != InvalidOid {
            entry.collOid
        } else {
            syscache_seams::lookup_pg_type_shape::call(atttypid)?
                .expect("pg_type row vanished")
                .typcollation
        };
        tupdesc::TupleDescInitEntry(&mut desc, attnum, Some(colname), atttypid, atttypmod, 0)?;
        tupdesc::TupleDescInitEntryCollation(&mut desc, attnum, attcollation);

        let att = desc.attr_mut(attnum as usize - 1);
        att.attnotnull = entry.is_not_null;
        att.attislocal = entry.is_local;
        att.attinhcount = entry.inhcount;
        att.attidentity = entry.identity as i8;
        att.attgenerated = entry.generated as i8;
        if entry.compression.is_some() {
            unported("GetAttributeCompression (per-column COMPRESSION)");
        }
        if entry.storage != 0 {
            att.attstorage = entry.storage as i8;
        } else if entry.storage_name.is_some() {
            unported("GetAttributeStorage (STORAGE by name)");
        }
        tupdesc::populate_compact_attribute(&mut desc, attnum as usize - 1);
    }
    Ok(desc)
}

pub fn DefineRelation<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateStmt<'mcx>,
    relkind: u8,
    owner_id: Oid,
    query_string: &str,
) -> PgResult<Oid> {
    debug_assert!(relkind == RELKIND_RELATION || relkind == RELKIND_SEQUENCE);
    let partitioned = stmt.partspec.is_some();
    let relkind = if partitioned { types_rel::RELKIND_PARTITIONED_TABLE } else { relkind };
    let rv = stmt.relation.expect("CreateStmt.relation");
    let relname = rv.relname.expect("RangeVar.relname");
    if relname.len() >= NAMEDATALEN as usize {
        unported("overlength relation name truncation");
    }
    if !stmt.options.is_nil() {
        unported("transformRelOptions/heap_reloptions (WITH options)");
    }
    if stmt.tablespacename.is_some() {
        unported("TABLESPACE clauses");
    }
    if !stmt.inhRelations.is_nil() && stmt.partbound.is_none() {
        unported("MergeAttributes inheritance");
    }
    // PARTITION OF: the parent's partition descriptor changes — take an
    // exclusive lock (C parentLockmode).
    let parent_oid = if stmt.partbound.is_some() {
        assert_eq!(stmt.inhRelations.len(), 1);
        let prv = stmt
            .inhRelations
            .nth(0)
            .as_variant::<types_nodes::RangeVar>()
            .expect("inhRelations RangeVar");
        let creation_rv = rel_vocab::RangeVar {
            catalogname: prv.catalogname,
            schemaname: prv.schemaname,
            relname: prv.relname.expect("RangeVar.relname"),
            inh: prv.inh,
            relpersistence: prv.relpersistence,
            location: prv.location,
        };
        Some(catalog_namespace::RangeVarGetRelid(
            &creation_rv,
            types_rel::AccessExclusiveLock,
            false,
        )?)
    } else {
        None
    };
    // C: accessMethodId is InvalidOid unless RELKIND_HAS_TABLE_AM;
    // partitions inherit the parent's relam and the parent USING is loud,
    // so heap is the only reachable AM.
    let access_method_id = match stmt.accessMethod {
        None if !types_rel::RELKIND_HAS_TABLE_AM(relkind) => InvalidOid,
        None => HEAP_TABLE_AM_OID, // default_table_access_method = "heap"
        Some("heap") => HEAP_TABLE_AM_OID,
        Some(_) => unported("get_table_am_oid (non-heap USING)"),
    };

    // RangeVarGetAndCheckCreationNamespace resolve-only: CREATE ACL check and
    // oid-collision retry ride with the aclchk lane.
    let creation_rv = rel_vocab::RangeVar {
        catalogname: rv.catalogname,
        schemaname: rv.schemaname,
        relname,
        inh: rv.inh,
        relpersistence: rv.relpersistence,
        location: rv.location,
    };
    let namespace_id = catalog_namespace::RangeVarGetCreationNamespace(mcx, &creation_rv)?;
    let relpersistence =
        catalog_namespace::RangeVarAdjustRelationPersistence(rv.relpersistence, namespace_id)?;

    if stmt.oncommit != OnCommitAction::ONCOMMIT_NOOP
        && relpersistence != types_core::RELPERSISTENCE_TEMP
    {
        return Err(Box::new(
            PgError::new(ERROR, "ON COMMIT can only be used on temporary tables".to_string())
                .with_sqlstate(types_error::ERRCODE_INVALID_TABLE_DEFINITION),
        ));
    }

    let owner_id = if owner_id != InvalidOid { owner_id } else { miscinit::GetUserId() };

    let descriptor = match parent_oid {
        // MergeAttributes, empty-column partition arm: the partition's
        // columns are exactly the parent's (attislocal=false, attinhcount=1).
        Some(parent_oid) => {
            assert!(stmt.tableElts.is_nil(), "loud in transformCreateStmt");
            let parent = table::table_open(mcx, parent_oid, types_rel::NoLock)?;
            if parent.rd_rel.relkind != types_rel::RELKIND_PARTITIONED_TABLE {
                let pname = parent.name().to_string();
                return Err(Box::new(
                    PgError::new(ERROR, format!("\"{pname}\" is not partitioned"))
                        .with_sqlstate(types_error::ERRCODE_INVALID_OBJECT_DEFINITION),
                ));
            }
            if let Some(constr) = parent.rd_att.constr.as_deref() {
                if constr.num_check > 0 || constr.has_generated_stored {
                    unported("inherited CHECK/generated constraints on partitions");
                }
            }
            let mut desc = tupdesc::CreateTupleDescCopy(mcx, parent.descr())?;
            for i in 0..desc.natts as usize {
                let parent_att = parent.rd_att.attr(i);
                if parent_att.atthasdef {
                    unported("inherited column defaults on partitions");
                }
                if parent_att.attidentity != 0 || parent_att.attgenerated != 0 {
                    unported("identity/generated columns on partitions");
                }
                let att = desc.attr_mut(i);
                att.attnotnull = parent_att.attnotnull;
                att.attislocal = false;
                att.attinhcount = 1;
                tupdesc::populate_compact_attribute(&mut desc, i);
            }
            parent.close(types_rel::NoLock)?;
            desc
        }
        None => BuildDescForRelation(mcx, &stmt.tableElts)?,
    };

    let relation_id = catalog_heap::heap_create_with_catalog(
        mcx,
        &catalog_heap::HeapCreateParams {
            relname,
            relnamespace: namespace_id,
            reltablespace: InvalidOid,
            ownerid: owner_id,
            accessmtd: access_method_id,
            relkind,
            relpersistence,
            allow_system_table_mods: false,
        },
        &descriptor,
    )?;

    register_on_commit_action(relation_id, stmt.oncommit);

    xact::CommandCounterIncrement()?;

    // Partition bound: transform, validate against siblings, store.
    if let Some(parent_oid) = parent_oid {
        let bound_spec_node = stmt.partbound.expect("checked above");
        let parent = table::table_open(mcx, parent_oid, types_rel::NoLock)?;
        let rel = table::table_open(mcx, relation_id, types_rel::AccessExclusiveLock)?;
        let mut pstate = parser_small1::make_parsestate(mcx, None);
        let bound = partition::transformPartitionBound(
            mcx,
            &mut pstate,
            &parent,
            bound_spec_node,
        )?;
        let _ = query_string;
        {
            let key = partcache::RelationGetPartitionKey(&parent)?;
            let pdesc = partdesc::RelationGetPartitionDesc(&parent)?;
            let spec = bound
                .as_variant::<types_nodes::rawnodes::PartitionBoundSpec>()
                .expect("PartitionBoundSpec");
            partbounds::check_new_partition_bound(
                mcx,
                relname,
                &key,
                pdesc.boundinfo.as_ref(),
                &pdesc.oids,
                spec,
            )?;
        }
        catalog_heap::StorePartitionBound(mcx, &rel, &parent, bound)?;
        partition::store_catalog_inheritance1(mcx, relation_id, parent_oid)?;
        if parent.rd_rel.relhasindex
            && !relcache::RelationGetIndexList(mcx, parent_oid)?.is_empty()
        {
            unported("cloning parent indexes onto new partitions (DefineIndex recursion)");
        }
        rel.close(types_rel::NoLock)?;
        parent.close(types_rel::NoLock)?;
        xact::CommandCounterIncrement()?;
    }

    // Partition key: compute and store pg_partitioned_table.
    if partitioned {
        let spec = stmt
            .partspec
            .expect("checked above")
            .as_variant::<types_nodes::rawnodes::PartitionSpec>()
            .expect("PartitionSpec");
        let rel = table::table_open(mcx, relation_id, types_rel::AccessExclusiveLock)?;
        let info = partition::compute_partition_key(mcx, &rel, spec)?;
        catalog_heap::StorePartitionKey(
            mcx,
            &rel,
            info.strategy,
            info.partattrs.len() as i16,
            &info.partattrs,
            &info.partopclass,
            &info.partcollation,
        )?;
        rel.close(types_rel::NoLock)?;
        xact::CommandCounterIncrement()?;
    }

    let raw_defaults = constraints::collect_raw_defaults(mcx, &stmt.tableElts)?;
    if !raw_defaults.is_empty() || !stmt.constraints.is_nil() || !stmt.nnconstraints.is_nil() {
        let rel = table::table_open(mcx, relation_id, types_rel::AccessExclusiveLock)?;
        if !raw_defaults.is_empty() {
            constraints::add_relation_new_constraints(
                mcx,
                &rel,
                &raw_defaults,
                &types_nodes::NodeList::nil(),
                query_string,
            )?;
            xact::CommandCounterIncrement()?;
        }
        if !stmt.constraints.is_nil() {
            constraints::add_relation_new_constraints(
                mcx,
                &rel,
                &[],
                &stmt.constraints,
                query_string,
            )?;
        }
        if !stmt.nnconstraints.is_nil() {
            constraints::add_relation_not_null_constraints(mcx, &rel, &stmt.nnconstraints)?;
        }
        table::table_close(rel, types_rel::NoLock)?;
        xact::CommandCounterIncrement()?;
    }
    Ok(relation_id)
}
