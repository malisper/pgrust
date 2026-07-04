// DefineRelation plain-table lane; BuildDescForRelation rides here as in 18.3.
#![allow(non_snake_case, non_upper_case_globals)]

mod alter;
mod inheritance;
mod partition;
mod constraints;
mod fk;
mod drop;
mod oncommit;
mod rename;
mod truncate;
pub use alter::{AlterTable, AlterTableGetLockLevel, AlterTableLookupRelation};
pub use rename::{renameatt, RenameConstraint, RenameRelation, RenameRelationInternal};
pub use drop::RemoveRelations;
pub use partition::SetRelationHasSubclass;
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
    tablecmds_seams::set_relation_has_subclass::set(partition::SetRelationHasSubclass);
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

// GetColumnDefCollation (parse_type.c).
fn GetColumnDefCollation(coldef: &ColumnDef<'_>, type_oid: Oid) -> PgResult<Oid> {
    let typcollation = syscache_seams::lookup_pg_type_shape::call(type_oid)?
        .expect("pg_type row vanished")
        .typcollation;
    let result = if let Some(cc) = coldef.collClause {
        let cc = cc.as_collate_clause().expect("CollateClause");
        catalog_namespace::get_collation_oid_list(&cc.collname, false)?
    } else if coldef.collOid != types_core::InvalidOid {
        coldef.collOid
    } else {
        typcollation
    };
    if result != types_core::InvalidOid && typcollation == types_core::InvalidOid {
        return Err(types_error::PgError::error(format!(
            "collations are not supported by type {}",
            format_type::format_type_be(type_oid)?
        ))
        .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH)
        .into());
    }
    Ok(result)
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
        let attcollation = GetColumnDefCollation(entry, atttypid)?;
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
    debug_assert!(
        relkind == RELKIND_RELATION
            || relkind == RELKIND_SEQUENCE
            || relkind == types_rel::RELKIND_VIEW
            || relkind == types_rel::RELKIND_MATVIEW
    );
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
    // PARTITION OF: the parent's partition descriptor changes — take an
    // exclusive lock (C parentLockmode).
    let parent_lockmode = if stmt.partbound.is_some() {
        types_rel::AccessExclusiveLock
    } else {
        types_rel::ShareUpdateExclusiveLock
    };
    let inherit_oids = inheritance::lookup_inherit_oids(mcx, stmt, parent_lockmode)?;
    let parent_oid = if stmt.partbound.is_some() {
        assert_eq!(inherit_oids.len(), 1);
        Some(inherit_oids[0])
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

    if partitioned && stmt.partbound.is_none() && !inherit_oids.is_empty() {
        return Err(Box::new(
            PgError::new(
                ERROR,
                "cannot create partitioned table as inheritance child".to_string(),
            )
            // C raises this in transformCreateStmt (parse_utilcmd.c:261);
            // here parents are already locked -- the error unwinds them.
            .with_sqlstate(types_error::ERRCODE_INVALID_OBJECT_DEFINITION),
        ));
    }
    let merged = if stmt.partbound.is_none() && !inherit_oids.is_empty() {
        Some(inheritance::MergeAttributes(
            mcx,
            &stmt.tableElts,
            &inherit_oids,
            relpersistence as u8,
        )?)
    } else {
        None
    };

    let mut partition_notnulls: mcx::PgVec<'mcx, inheritance::InheritedNotNull<'mcx>> =
        mcx::PgVec::new_in(mcx);
    let mut partition_checks: mcx::PgVec<'mcx, inheritance::InheritedCheck<'mcx>> =
        mcx::PgVec::new_in(mcx);
    let mut partition_gendefs: mcx::PgVec<'mcx, (AttrNumber, types_nodes::Node<'mcx>)> =
        mcx::PgVec::new_in(mcx);
    let descriptor = match parent_oid {
        // MergeAttributes, empty-column partition arm: the partition's
        // columns are exactly the parent's (attislocal=false, attinhcount=1),
        // so parent CHECK ccbin and generation expressions ride unmapped.
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
                for check in constr.check.iter() {
                    if check.ccnoinherit {
                        continue;
                    }
                    let name = {
                        let owned = check.ccname.as_ref().expect("check name").as_str();
                        let bytes = mcx::slice_borrow_in(mcx, owned.as_bytes())?;
                        // SAFETY: byte-for-byte copy of a &str.
                        unsafe { core::str::from_utf8_unchecked(bytes) }
                    };
                    let expr = readfuncs::stringToNode(
                        mcx,
                        check.ccbin.as_ref().expect("check ccbin").as_str(),
                    )?;
                    partition_checks.push(inheritance::InheritedCheck {
                        name,
                        expr,
                        inhcount: 1,
                        is_enforced: check.ccenforced,
                        skip_validation: !check.ccenforced,
                    });
                }
            }
            // The parent's catalogued not-null constraints ride to the
            // partition (identical attnos: the descriptor is a full copy).
            for cnode in pg_constraint::RelationGetNotNullConstraints(mcx, &parent, false)?.iter()
            {
                let c = cnode
                    .as_variant::<types_nodes::rawnodes::Constraint>()
                    .expect("Constraint");
                let colname = c.keys.nth(0).as_string().expect("nn keys").sval;
                let attnum = (0..parent.rd_att.natts as usize)
                    .find(|&i| parent.rd_att.attr(i).attname.name_str() == colname.as_bytes())
                    .map(|i| (i + 1) as AttrNumber)
                    .unwrap_or_else(|| panic!("not-null column {colname:?} not found"));
                partition_notnulls.push(inheritance::InheritedNotNull {
                    name: c.conname.expect("catalogued nn constraint has a name"),
                    attnum,
                });
            }
            let mut desc = tupdesc::CreateTupleDescCopy(mcx, parent.descr())?;
            for i in 0..desc.natts as usize {
                let parent_att = parent.rd_att.attr(i);
                if parent_att.attisdropped {
                    continue;
                }
                if parent_att.attidentity != 0 {
                    unported("identity columns on partitions");
                }
                if parent_att.atthasdef {
                    if parent_att.attgenerated == 0 {
                        unported("inherited column defaults on partitions");
                    }
                    let adbin =
                        pg_attrdef::GetAttrDefaultBin(mcx, parent_oid, (i + 1) as AttrNumber)?
                            .unwrap_or_else(|| {
                                panic!("default expression not found for attribute {}", i + 1)
                            });
                    let expr = readfuncs::stringToNode(mcx, &adbin)?;
                    partition_gendefs.push(((i + 1) as AttrNumber, expr));
                }
                let att = desc.attr_mut(i);
                att.attnotnull = parent_att.attnotnull;
                att.attgenerated = parent_att.attgenerated;
                att.attislocal = false;
                att.attinhcount = 1;
                tupdesc::populate_compact_attribute(&mut desc, i);
            }
            parent.close(types_rel::NoLock)?;
            desc
        }
        None => match &merged {
            Some(m) => BuildDescForRelation(mcx, &m.columns)?,
            None => BuildDescForRelation(mcx, &stmt.tableElts)?,
        },
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

    // C StoreConstraints runs inside heap_create_with_catalog: inherited
    // cooked CHECKs and generation expressions land before pg_inherits rows.
    if let Some(m) = &merged {
        inheritance::store_inherited_checks(mcx, relation_id, &m.checks)?;
    }
    if !partition_checks.is_empty() {
        inheritance::store_inherited_checks(mcx, relation_id, &partition_checks)?;
    }
    if !partition_gendefs.is_empty() {
        xact::CommandCounterIncrement()?;
        let rel = table::table_open(mcx, relation_id, types_rel::NoLock)?;
        for &(attnum, expr) in partition_gendefs.iter() {
            pg_attrdef::StoreAttrDefault(mcx, &rel, attnum, expr)?;
        }
        table::table_close(rel, types_rel::NoLock)?;
    }

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

    if !inherit_oids.is_empty() && stmt.partbound.is_none() {
        inheritance::StoreCatalogInheritance(mcx, relation_id, &inherit_oids, false)?;
        xact::CommandCounterIncrement()?;
    }

    // Create in the new partition every index (and index-backed constraint)
    // the parent carries; triggers and FKs have no cloning lane yet.
    if let Some(parent_oid) = parent_oid {
        let parent = table::table_open(mcx, parent_oid, types_rel::NoLock)?;
        let rel = table::table_open(mcx, relation_id, types_rel::NoLock)?;
        let idxlist = relcache::RelationGetIndexList(mcx, parent_oid)?;
        for &idxoid in idxlist.iter() {
            let idx_rel = indexam::index_open(mcx, idxoid, types_rel::AccessShareLock)?;
            let attmap = tupdesc::build_attrmap_by_name(mcx, rel.descr(), parent.descr())?;
            let (idxstmt, constraint_oid) =
                parse_utilcmd::generateClonedIndexStmt(mcx, None, &idx_rel, &attmap)?;
            indexcmds_seams::define_index::call(
                mcx,
                relation_id,
                &idxstmt,
                InvalidOid,
                idxoid,
                constraint_oid,
                false,
                false,
                false,
                false,
                false,
            )?;
            indexam::index_close(idx_rel, types_rel::AccessShareLock)?;
        }
        if parent.rd_hastriggers {
            unported("CloneRowTriggersToPartition");
        }
        if rel_has_fk_constraints(mcx, parent_oid)? {
            unported("CloneForeignKeyConstraints onto new partitions");
        }
        rel.close(types_rel::NoLock)?;
        parent.close(types_rel::NoLock)?;
    }

    // Merged columns re-number local attributes; raw defaults ride them.
    let raw_defaults = match &merged {
        Some(m) => constraints::collect_raw_defaults(mcx, &m.columns)?,
        None => constraints::collect_raw_defaults(mcx, &stmt.tableElts)?,
    };
    let old_notnulls: &[inheritance::InheritedNotNull<'mcx>] = match &merged {
        Some(m) => &m.notnulls[..],
        None => &partition_notnulls[..],
    };
    if !raw_defaults.is_empty()
        || !stmt.constraints.is_nil()
        || !stmt.nnconstraints.is_nil()
        || !old_notnulls.is_empty()
    {
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
        let mut connames: mcx::PgVec<'_, &str> = mcx::PgVec::new_in(mcx);
        if !stmt.constraints.is_nil() {
            let conlist = constraints::add_relation_new_constraints(
                mcx,
                &rel,
                &[],
                &stmt.constraints,
                query_string,
            )?;
            for con in conlist.iter() {
                connames.push(con.name);
            }
        }
        if !stmt.nnconstraints.is_nil() || !old_notnulls.is_empty() {
            let nncols = constraints::add_relation_not_null_constraints(
                mcx,
                &rel,
                &stmt.nnconstraints,
                old_notnulls,
                &connames,
            )?;
            // set_attnotnull leg (tablecmds.c:1357): a table-level NOT NULL
            // naming an inherited column has no local ColumnDef carrying it.
            let mut updated = false;
            for &attnum in nncols.iter() {
                let att = rel.rd_att.attr(attnum as usize - 1);
                if att.attisdropped || att.attnotnull {
                    continue;
                }
                alter::update_pg_attribute(
                    mcx,
                    rel.rd_id,
                    attnum,
                    &[(alter::Anum_pg_attribute_attnotnull, ::datum::Datum::from_bool(true))],
                )?;
                updated = true;
            }
            if updated {
                xact::CommandCounterIncrement()?;
            }
        }
        table::table_close(rel, types_rel::NoLock)?;
        xact::CommandCounterIncrement()?;
    }
    Ok(relation_id)
}

// CloneForeignKeyConstraints detector: any FK on or referencing the parent
// means the cloning lane is required.
fn rel_has_fk_constraints(mcx: Mcx<'_>, relid: Oid) -> PgResult<bool> {
    let con_rel = table::table_open(
        mcx,
        types_core::CONSTRAINT_RELATION_ID,
        types_rel::AccessShareLock,
    )?;
    let mut key = types_scan::scankey::ScanKeyData::empty();
    key.sk_attno = pg_constraint::Anum_pg_constraint_conrelid;
    key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = ::datum::Datum::from_oid(relid);
    let mut scan = genam::systable_beginscan(
        mcx,
        &con_rel,
        pg_constraint::ConstraintRelidTypidNameIndexId,
        true,
        None,
        &[key],
    )?;
    let mut found = false;
    let desc = con_rel.descr();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY: contype is a fixed NOT NULL pg_constraint column.
        let contype = unsafe {
            types_tuple::heap_getattr(
                tup,
                pg_constraint::Anum_pg_constraint_contype as i32,
                desc,
                &mut isnull,
            )
        }
        .as_i8() as u8;
        if contype == pg_constraint::CONSTRAINT_FOREIGN {
            found = true;
            break;
        }
    }
    genam::systable_endscan(mcx, scan)?;
    if !found {
        // CloneFkReferenced side: FKs pointing AT the parent (confrelid);
        // seqscan, no index on confrelid exists (C shape).
        let mut scan =
            genam::systable_beginscan(mcx, &con_rel, types_core::InvalidOid, false, None, &[])?;
        while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
            let mut isnull = false;
            // SAFETY (each): fixed NOT NULL pg_constraint columns.
            let contype = unsafe {
                types_tuple::heap_getattr(
                    tup,
                    pg_constraint::Anum_pg_constraint_contype as i32,
                    desc,
                    &mut isnull,
                )
            }
            .as_i8() as u8;
            if contype != pg_constraint::CONSTRAINT_FOREIGN {
                continue;
            }
            // SAFETY: as above.
            let confrelid = unsafe {
                types_tuple::heap_getattr(
                    tup,
                    pg_constraint::Anum_pg_constraint_confrelid as i32,
                    desc,
                    &mut isnull,
                )
            }
            .as_oid();
            if confrelid == relid {
                found = true;
                break;
            }
        }
        genam::systable_endscan(mcx, scan)?;
    }
    con_rel.close(types_rel::AccessShareLock)?;
    Ok(found)
}
