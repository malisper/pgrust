// DefineRelation plain-table lane; BuildDescForRelation rides here as in 18.3.
#![allow(non_snake_case)]

use mcx::Mcx;
use types_core::{AttrNumber, InvalidOid, Oid, NAMEDATALEN};
use types_error::{PgError, PgResult, ERRCODE_UNDEFINED_SCHEMA, ERROR};
use types_nodes::rawnodes::{ColumnDef, CreateStmt, OnCommitAction, TypeName};
use types_rel::RELKIND_RELATION;
use types_tuple::TupleDescData;

const HEAP_TABLE_AM_OID: Oid = 2;

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
        let (atttypid, atttypmod) = parse_utilcmd::typenameTypeIdAndMod(mcx, tn)?;
        let attcollation = syscache_seams::lookup_pg_type_shape::call(atttypid)?
            .expect("pg_type row vanished")
            .typcollation;
        debug_assert!(tn.arrayBounds.is_nil()); // loud in typenameTypeIdAndMod

        tupdesc::TupleDescInitEntry(&mut desc, attnum, Some(colname), atttypid, atttypmod, 0)?;
        tupdesc::TupleDescInitEntryCollation(&mut desc, attnum, attcollation);

        let att = desc.attr_mut(attnum as usize - 1);
        att.attnotnull = entry.is_not_null;
        att.attislocal = entry.is_local;
        att.attinhcount = entry.inhcount;
        if entry.identity != 0 || entry.generated != 0 {
            unported("identity/generated columns");
        }
        if entry.compression.is_some() {
            unported("GetAttributeCompression (per-column COMPRESSION)");
        }
        if entry.storage != 0 || entry.storage_name.is_some() {
            unported("per-column STORAGE overrides");
        }
        tupdesc::populate_compact_attribute(&mut desc, attnum as usize - 1);
    }
    Ok(desc)
}

pub fn DefineRelation<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateStmt<'_>,
    relkind: u8,
    owner_id: Oid,
    _query_string: &str,
) -> PgResult<Oid> {
    debug_assert!(relkind == RELKIND_RELATION);
    let rv = stmt.relation.expect("CreateStmt.relation");
    let relname = rv.relname.expect("RangeVar.relname");
    if relname.len() >= NAMEDATALEN as usize {
        unported("overlength relation name truncation");
    }
    if stmt.oncommit != OnCommitAction::ONCOMMIT_NOOP {
        unported("ON COMMIT clauses");
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
    let access_method_id = match stmt.accessMethod {
        None => HEAP_TABLE_AM_OID, // default_table_access_method = "heap"
        Some("heap") => HEAP_TABLE_AM_OID,
        Some(_) => unported("get_table_am_oid (non-heap USING)"),
    };

    // RangeVarGetAndCheckCreationNamespace resolve-only: CREATE ACL check and
    // oid-collision retry ride with the aclchk lane.
    let namespace_id = match rv.schemaname {
        Some(schemaname) => catalog_namespace::get_namespace_oid(schemaname, false)?,
        None => {
            let path = catalog_namespace::fetch_search_path(mcx, false)?;
            match path.first() {
                Some(&ns) => ns,
                None => {
                    return Err(Box::new(
                        PgError::new(
                            ERROR,
                            "no schema has been selected to create in".to_string(),
                        )
                        .with_sqlstate(ERRCODE_UNDEFINED_SCHEMA),
                    ))
                }
            }
        }
    };
    if catalog_namespace::isAnyTempNamespace(namespace_id)? {
        unported("temp-namespace relation creation");
    }

    let owner_id = if owner_id != InvalidOid { owner_id } else { miscinit::GetUserId() };

    let descriptor = BuildDescForRelation(mcx, &stmt.tableElts)?;
    for i in 0..descriptor.natts as usize {
        if descriptor.attr(i).attlen < 0 {
            unported("NewRelationCreateToastTable (varlena column => TOAST decision)");
        }
    }

    let relation_id = catalog_heap::heap_create_with_catalog(
        mcx,
        &catalog_heap::HeapCreateParams {
            relname,
            relnamespace: namespace_id,
            reltablespace: InvalidOid,
            ownerid: owner_id,
            accessmtd: access_method_id,
            relkind,
            relpersistence: rv.relpersistence,
            allow_system_table_mods: false,
        },
        &descriptor,
    )?;

    xact::CommandCounterIncrement()?;
    Ok(relation_id)
}
