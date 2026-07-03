// DefineRelation plain-table lane; BuildDescForRelation rides here as in 18.3.
#![allow(non_snake_case, non_upper_case_globals)]

mod alter;
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
    catalog_index_seams::relation_set_new_relfilenumber::set(truncate::RelationSetNewRelfilenumber);
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
        // except matview; both render the same aclcheck_error message class.
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
fn unported(what: &str) -> ! {
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
    if !stmt.inhRelations.is_nil() {
        unported("MergeAttributes inheritance");
    }
    // C: accessMethodId is InvalidOid unless RELKIND_HAS_TABLE_AM.
    let access_method_id = if !types_rel::RELKIND_HAS_TABLE_AM(relkind) {
        InvalidOid
    } else {
        match stmt.accessMethod {
            None => HEAP_TABLE_AM_OID, // default_table_access_method = "heap"
            Some("heap") => HEAP_TABLE_AM_OID,
            Some(_) => unported("get_table_am_oid (non-heap USING)"),
        }
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

    let descriptor = BuildDescForRelation(mcx, &stmt.tableElts)?;

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
