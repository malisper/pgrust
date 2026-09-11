// DefineRelation plain-table lane; BuildDescForRelation rides here as in 18.3.
#![allow(non_snake_case, non_upper_case_globals)]

mod alter;
mod attach;
mod constraints;
mod drop;
mod fk;
mod inheritance;
mod namespace;
mod oncommit;
mod owner;
mod partition;
mod rename;
mod setrelopts;
mod truncate;
pub use alter::{
    find_composite_type_dependencies, AlterTable, AlterTableGetLockLevel, AlterTableInternal,
    AlterTableLookupRelation, AlterTableMoveAll,
};
pub use constraints::cook_default;
pub use drop::RemoveRelations;
pub use namespace::{
    AlterRelationNamespaceInternal, AlterTableNamespace, AlterTableNamespaceInternal,
};
pub use oncommit::{
    register_on_commit_action, remove_on_commit_action, AtEOSubXact_on_commit_actions,
    AtEOXact_on_commit_actions, PreCommit_on_commit_actions,
};
pub use partition::SetRelationHasSubclass;
pub use rename::{renameatt, RenameConstraint, RenameRelation, RenameRelationInternal};
pub use truncate::{ExecuteTruncate, ExecuteTruncateGuts};

pub fn init_seams() {
    tablecmds_seams::rename_relation_internal::set(RenameRelationInternal);
    tablecmds_seams::range_var_callback_maintains_table::set(RangeVarCallbackMaintainsTable);
    tablecmds_seams::pre_commit_on_commit_actions::set(PreCommit_on_commit_actions);
    tablecmds_seams::at_eoxact_on_commit_actions::set(AtEOXact_on_commit_actions);
    tablecmds_seams::at_eosubxact_on_commit_actions::set(AtEOSubXact_on_commit_actions);
    tablecmds_seams::remove_on_commit_action::set(remove_on_commit_action);
    tablecmds_seams::set_relation_has_subclass::set(partition::SetRelationHasSubclass);
    tablecmds_seams::check_of_type::set(alter::check_of_type);
    pg_shdepend::at_exec_change_owner::set(owner::ATExecChangeOwner);
}

use mcx::Mcx;
use types_core::{AttrNumber, InvalidOid, Oid, NAMEDATALEN};
use types_error::{
    PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_TABLE_DEFINITION, ERRCODE_PROGRAM_LIMIT_EXCEEDED,
    ERRCODE_WARNING_DEPRECATED_FEATURE, ERROR, WARNING,
};

use commands_tablespace::{TableSpaceRelationId, GLOBALTABLESPACE_OID};
use types_nodes::rawnodes::{ColumnDef, CreateStmt, OnCommitAction, TypeName};
use types_rel::{RELKIND_RELATION, RELKIND_SEQUENCE};
use types_tuple::TupleDescData;

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
    let aclresult = aclchk::pg_class_aclcheck(relId, miscinit::GetUserId(), adt_acl::ACL_MAINTAIN)?;
    if aclresult != aclchk::ACLCHECK_OK {
        // get_relkind_objtype (objectaddress.c): matview vs table noun.
        let objtype = if relkind == types_rel::RELKIND_MATVIEW {
            types_nodes::parsenodes::ObjectType::OBJECT_MATVIEW
        } else {
            types_nodes::parsenodes::ObjectType::OBJECT_TABLE
        };
        aclchk_seams::aclcheck_error::call(aclresult, objtype as i32, relation.relname)?;
    }
    Ok(())
}

fn reject_temp_in_security_restricted(relpersistence: u8) -> PgResult<()> {
    if relpersistence == types_core::RELPERSISTENCE_TEMP
        && miscinit::InSecurityRestrictedOperation()
    {
        return Err(Box::new(
            PgError::new(
                ERROR,
                "cannot create temporary table within security-restricted operation".to_string(),
            )
            .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }
    Ok(())
}

// Every one of these is `elog(ERROR, "cache lookup failed for <object> %u", oid)`
// in C (tablecmds.c and friends): a catchable error whose SQLSTATE is elog's
// default XX000 / ERRCODE_INTERNAL_ERROR, never a backend abort.  pgrust used
// to panic!() at these probes, which kills the process instead.
#[track_caller]
#[cold]
#[inline(never)]
pub(crate) fn cache_lookup_failed(what: &str, oid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "cache lookup failed for {what} {oid}"
    )))
}

// tablecmds.c:7875 / :14193 elog(ERROR, "cache lookup failed for attribute %d of relation %u").
#[track_caller]
#[cold]
#[inline(never)]
pub(crate) fn cache_lookup_failed_attribute(attnum: AttrNumber, relid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "cache lookup failed for attribute {attnum} of relation {relid}"
    )))
}

// makeObjectName/namestrcpy truncation: silent, multibyte-aware.
pub(crate) fn truncate_name<'a, 'mcx>(mcx: Mcx<'mcx>, name: &'a str) -> PgResult<&'a str> {
    if name.len() < NAMEDATALEN as usize {
        return Ok(name);
    }
    let mut buf: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, name.len())?;
    mcx::vec_append_bytes(&mut buf, name.as_bytes())?;
    parser_small1::truncate_identifier(&mut buf, false, mbutils::GetDatabaseEncoding())?;
    Ok(&name[..buf.len()])
}

// get_relkind_objtype (objectaddress.c)
pub fn get_relkind_objtype(relkind: u8) -> types_nodes::parsenodes::ObjectType {
    use types_nodes::parsenodes::ObjectType::*;
    match relkind {
        RELKIND_RELATION | types_rel::RELKIND_PARTITIONED_TABLE => OBJECT_TABLE,
        types_rel::RELKIND_INDEX | types_rel::RELKIND_PARTITIONED_INDEX => OBJECT_INDEX,
        RELKIND_SEQUENCE => OBJECT_SEQUENCE,
        types_rel::RELKIND_VIEW => OBJECT_VIEW,
        types_rel::RELKIND_MATVIEW => OBJECT_MATVIEW,
        types_rel::RELKIND_FOREIGN_TABLE => OBJECT_FOREIGN_TABLE,
        _ => OBJECT_TABLE,
    }
}

// aclcheck_error_type (aclchk.c): arrays report their element type.
fn aclcheck_error_type(aclerr: i32, type_oid: Oid) -> PgResult<()> {
    let element_type = lsyscache::get_element_type(type_oid)?;
    let type_oid = if element_type != InvalidOid {
        element_type
    } else {
        type_oid
    };
    aclchk::aclcheck_error(
        aclerr,
        types_nodes::parsenodes::ObjectType::OBJECT_TYPE,
        &format_type::format_type_be(type_oid)?,
    )
}

// GetColumnDefCollation (parse_type.c).
fn GetColumnDefCollation(coldef: &ColumnDef<'_>, type_oid: Oid) -> PgResult<Oid> {
    GetColumnDefCollationPos(None, coldef, type_oid)
}

fn GetColumnDefCollationPos(
    source: Option<&[u8]>,
    coldef: &ColumnDef<'_>,
    type_oid: Oid,
) -> PgResult<Oid> {
    let typcollation = syscache_seams::lookup_pg_type_shape::call(type_oid)?
        .expect("pg_type row vanished")
        .typcollation;
    let mut location = coldef.location;
    let result = if let Some(cc) = coldef.collClause {
        let cc = cc.as_collate_clause().expect("CollateClause");
        location = cc.location;
        catalog_namespace::get_collation_oid_list(&cc.collname, false)?
    } else if coldef.collOid != types_core::InvalidOid {
        coldef.collOid
    } else {
        typcollation
    };
    if result != types_core::InvalidOid && typcollation == types_core::InvalidOid {
        let mut e = types_error::PgError::error(format!(
            "collations are not supported by type {}",
            format_type::format_type_be(type_oid)?
        ))
        .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH);
        let pos = parser_small1::parser_errposition_source(
            source,
            location,
            mbutils::GetDatabaseEncoding(),
        );
        if pos > 0 {
            e = e.with_cursor_position(pos);
        }
        return Err(Box::new(e));
    }
    Ok(result)
}

// GetAttributeCompression (tablecmds.c) -> CompressionNameToMethod
// (toast_compression.c). lz4 is supported (matching C built with USE_LZ4,
// the stock-build default).
pub(crate) fn GetAttributeCompression(atttypid: Oid, compression: Option<&str>) -> PgResult<i8> {
    let Some(compression) = compression else {
        return Ok(types_tuple::InvalidCompressionMethod);
    };
    if compression == "default" {
        return Ok(types_tuple::InvalidCompressionMethod);
    }
    let typstorage = syscache_seams::lookup_pg_type_shape::call(atttypid)?
        .expect("pg_type row vanished")
        .typstorage as u8;
    if typstorage == b'p' {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!(
                    "column data type {} does not support compression",
                    format_type::format_type_be(atttypid)?
                ),
            )
            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    if compression == "pglz" {
        Ok(b'p' as i8)
    } else if compression == "lz4" {
        Ok(b'l' as i8)
    } else {
        Err(Box::new(
            PgError::new(
                ERROR,
                format!("invalid compression method \"{compression}\""),
            )
            .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE),
        ))
    }
}

// C BuildDescForRelation (tablecmds.c): attdim =
// list_length(entry->typeName->arrayBounds) — the count of declared array
// bounds becomes pg_attribute.attndims — with an ERROR above PG_INT16_MAX,
// and SETOF column declarations rejected at the same spot.
fn column_def_attdim(tn: &TypeName<'_>, attname: &str) -> PgResult<i32> {
    let attdim = tn.arrayBounds.len();
    if attdim > i16::MAX as usize {
        return Err(Box::new(
            PgError::new(ERROR, "too many array dimensions")
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
        ));
    }
    if tn.setof {
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("column \"{attname}\" cannot be declared SETOF"),
            )
            .with_sqlstate(ERRCODE_INVALID_TABLE_DEFINITION),
        ));
    }
    Ok(attdim as i32)
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
        let colname = truncate_name(mcx, entry.colname.expect("ColumnDef.colname"))?;
        let tn = entry
            .typeName
            .expect("ColumnDef.typeName")
            .as_variant::<TypeName>()
            .expect("TypeName");
        let (atttypid, atttypmod) = parse_utilcmd::typenameTypeIdAndMod(mcx, None, tn)?;
        // C BuildDescForRelation (tablecmds.c): USAGE on every column type.
        let aclresult = aclchk::object_aclcheck(
            types_core::TYPE_RELATION_ID,
            atttypid,
            miscinit::GetUserId(),
            adt_acl::ACL_USAGE,
        )?;
        if aclresult != aclchk::ACLCHECK_OK {
            aclcheck_error_type(aclresult, atttypid)?;
        }
        let attcollation = GetColumnDefCollation(entry, atttypid)?;
        let attdim = column_def_attdim(tn, colname)?;
        tupdesc::TupleDescInitEntry(
            &mut desc,
            attnum,
            Some(colname),
            atttypid,
            atttypmod,
            attdim,
        )?;
        tupdesc::TupleDescInitEntryCollation(&mut desc, attnum, attcollation);

        let att = desc.attr_mut(attnum as usize - 1);
        att.attnotnull = entry.is_not_null;
        att.attislocal = entry.is_local;
        att.attinhcount = entry.inhcount;
        att.attidentity = entry.identity as i8;
        att.attgenerated = entry.generated as i8;
        att.attcompression = GetAttributeCompression(atttypid, entry.compression)?;
        if entry.storage != 0 {
            att.attstorage = entry.storage as i8;
        } else if let Some(storage_name) = entry.storage_name {
            att.attstorage = alter::get_attribute_storage(atttypid, storage_name)? as i8;
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
            || relkind == types_rel::RELKIND_COMPOSITE_TYPE
            || relkind == types_rel::RELKIND_FOREIGN_TABLE
    );
    let partitioned = stmt.partspec.is_some();
    let relkind = if partitioned {
        types_rel::RELKIND_PARTITIONED_TABLE
    } else {
        relkind
    };
    let rv = stmt.relation.expect("CreateStmt.relation");
    let relname = truncate_name(mcx, rv.relname.expect("RangeVar.relname"))?;
    // C's argument-consistency check runs FIRST (tablecmds.c:799-803), on the
    // pre-namespace-adjustment persistence — before parent lookup, reloptions,
    // everything, and before the unlogged-partitioned check below.
    if stmt.oncommit != OnCommitAction::ONCOMMIT_NOOP
        && rv.relpersistence != types_core::RELPERSISTENCE_TEMP
    {
        return Err(Box::new(
            PgError::new(
                ERROR,
                "ON COMMIT can only be used on temporary tables".to_string(),
            )
            .with_sqlstate(types_error::ERRCODE_INVALID_TABLE_DEFINITION),
        ));
    }
    // Pre-adjustment persistence, like C (tablecmds.c:816-820) — AFTER ON COMMIT.
    if relkind == types_rel::RELKIND_PARTITIONED_TABLE
        && rv.relpersistence == types_core::RELPERSISTENCE_UNLOGGED
    {
        return Err(Box::new(
            PgError::new(ERROR, "partitioned tables cannot be unlogged".to_string())
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    // Look up the namespace in which we are supposed to create the relation,
    // check we have permission to create there, lock it against concurrent
    // drop, and adjust the persistence if a temporary namespace is selected
    // (tablecmds.c:829) — BEFORE parents are resolved.
    let creation_rv = rel_vocab::RangeVar {
        catalogname: rv.catalogname,
        schemaname: rv.schemaname,
        relname,
        inh: rv.inh,
        relpersistence: rv.relpersistence,
        location: rv.location,
    };
    let (namespace_id, _existing_relid, relpersistence) =
        catalog_namespace::RangeVarGetAndCheckCreationNamespace(
            mcx,
            &creation_rv,
            types_rel::NoLock,
            false,
        )?;
    reject_temp_in_security_restricted(relpersistence)?;

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
        // tablecmds.c MergeAttributes: object_ownercheck(RelationRelationId,
        // parent) applies to PARTITION OF exactly as to INHERITS.
        let parent = inherit_oids[0];
        if !aclchk::object_ownercheck(types_core::RELATION_RELATION_ID, parent, miscinit::GetUserId())? {
            let relkind = lsyscache::get_rel_relkind(parent)?;
            let relname = lsyscache::get_rel_name(mcx, parent)?
                .map(|n| n.as_str().to_owned())
                .unwrap_or_default();
            aclchk::aclcheck_error(aclchk::ACLCHECK_NOT_OWNER, get_relkind_objtype(relkind as u8), &relname)?;
        }
        Some(parent)
    } else {
        None
    };

    let mut tablespace_id = match stmt.tablespacename {
        Some(name) => {
            let oid = commands_tablespace::get_tablespace_oid(mcx, name, false)?;
            if partitioned && oid == init_small::globals::MyDatabaseTableSpace() {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        "cannot specify default tablespace for partitioned relations".to_string(),
                    )
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                ));
            }
            oid
        }
        None if stmt.partbound.is_some() => lsyscache::get_rel_tablespace(inherit_oids[0])?,
        None => InvalidOid,
    };
    if tablespace_id == InvalidOid {
        tablespace_id =
            commands_tablespace::GetDefaultTablespace(mcx, relpersistence as u8, partitioned)?;
    }
    if tablespace_id != InvalidOid && tablespace_id != init_small::globals::MyDatabaseTableSpace() {
        let aclresult = aclchk::object_aclcheck(
            TableSpaceRelationId,
            tablespace_id,
            miscinit::GetUserId(),
            adt_acl::ACL_CREATE,
        )?;
        if aclresult != aclchk::ACLCHECK_OK {
            let ctx = mcx::MemoryContext::new("DefineRelation");
            let name = commands_tablespace::get_tablespace_name(ctx.mcx(), tablespace_id)?;
            aclchk::aclcheck_error(
                aclresult,
                types_nodes::parsenodes::ObjectType::OBJECT_TABLESPACE,
                name.as_ref()
                    .map(|n| std::str::from_utf8(n.name_str()).unwrap_or(""))
                    .unwrap_or(""),
            )?;
        }
    }
    if tablespace_id == GLOBALTABLESPACE_OID {
        return Err(Box::new(
            PgError::new(
                ERROR,
                "only shared relations can be placed in pg_global tablespace".to_string(),
            )
            .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }

    let owner_id = if owner_id != InvalidOid {
        owner_id
    } else {
        miscinit::GetUserId()
    };

    // Parse and validate reloptions at C's position (tablecmds.c:932-950):
    // after namespace/parents/tablespace, before ofTypename.
    let reloptions = reloptions::transformRelOptions(
        mcx,
        None,
        &stmt.options,
        None,
        reloptions::HEAP_RELOPT_NAMESPACES,
        true,
        false,
    )?;
    // C: explicit USING, else a partition inherits the parent's relam, else
    // default_table_access_method; InvalidOid for relkinds without table AM.
    // C resolves the AM AFTER MergeAttributes (tablecmds.c:1032), so a bad
    // USING name loses to MergeAttributes' errors; this provisional pass
    // (missing_ok) only feeds the reloptions dispatch and the pgrcolumnar
    // type gate — the erroring lookup runs at C's position, below.
    let access_method_id = if let Some(amname) = stmt.accessMethod {
        debug_assert!(
            types_rel::RELKIND_HAS_TABLE_AM(relkind)
                || relkind == types_rel::RELKIND_PARTITIONED_TABLE
        );
        commands_amcmds::get_am_oid(amname, true)?
    } else if types_rel::RELKIND_HAS_TABLE_AM(relkind)
        || relkind == types_rel::RELKIND_PARTITIONED_TABLE
    {
        let mut amoid = InvalidOid;
        if stmt.partbound.is_some() {
            amoid = lsyscache::get_rel_relam(inherit_oids[0])?;
        }
        if types_rel::RELKIND_HAS_TABLE_AM(relkind) && amoid == InvalidOid {
            amoid = commands_amcmds::get_am_oid(&tableam::default_table_access_method(), true)?;
        }
        amoid
    } else {
        InvalidOid
    };
    // Reloptions validation dispatches on the resolved relam, not just
    // relkind: the pgrcolumnar AM owns its option namespace.
    match relkind {
        types_rel::RELKIND_PARTITIONED_TABLE => {
            reloptions::partitioned_table_reloptions(reloptions.as_deref(), true)?;
        }
        types_rel::RELKIND_RELATION if reloptions::relam_is_pgrcolumnar(access_method_id) => {
            reloptions::pgrcolumnar_reloptions(mcx, reloptions.as_deref(), true)?;
        }
        _ => {
            reloptions::heap_reloptions(mcx, relkind, reloptions.as_deref(), true)?;
        }
    }

    let of_type_id = match stmt.ofTypename {
        Some(tn_node) => {
            let tn = tn_node.as_variant::<TypeName>().expect("TypeName");
            // transformCreateStmt cached the resolved oid (C re-resolves the
            // same names; identical outcome).
            let of_type_id = if tn.typeOid != InvalidOid {
                tn.typeOid
            } else {
                parse_utilcmd::typenameTypeIdAndModAllowComposite(mcx, None, tn)?.0
            };
            let aclresult = aclchk::object_aclcheck(
                types_core::TYPE_RELATION_ID,
                of_type_id,
                miscinit::GetUserId(),
                adt_acl::ACL_USAGE,
            )?;
            if aclresult != aclchk::ACLCHECK_OK {
                aclcheck_error_type(aclresult, of_type_id)?;
            }
            of_type_id
        }
        None => InvalidOid,
    };

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
    // MergeAttributes' duplicate-name scan runs even without parents
    // (tablecmds.c:2589): typed-table column options merge onto the column
    // from the type; option entries without a type column are errors.
    let merged_opts;
    let table_elts: &types_nodes::NodeList<'mcx> = if merged.is_none() && parent_oid.is_none() {
        merged_opts = inheritance::merge_column_options(mcx, &stmt.tableElts)?;
        &merged_opts
    } else {
        &stmt.tableElts
    };

    let mut partition_notnulls: mcx::PgVec<'mcx, inheritance::InheritedNotNull<'mcx>> =
        mcx::PgVec::new_in(mcx);
    let mut partition_checks: mcx::PgVec<'mcx, inheritance::InheritedCheck<'mcx>> =
        mcx::PgVec::new_in(mcx);
    let mut partition_gendefs: mcx::PgVec<'mcx, (AttrNumber, types_nodes::Node<'mcx>)> =
        mcx::PgVec::new_in(mcx);
    let mut partition_raw_defaults: mcx::PgVec<'mcx, (AttrNumber, types_nodes::Node<'mcx>, u8)> =
        mcx::PgVec::new_in(mcx);
    let descriptor = match parent_oid {
        // MergeAttributes, partition arm (tablecmds.c:2652-2967): the
        // partition's schema is the parent's NON-dropped columns, compactly
        // renumbered (attislocal=false, attinhcount=1); CHECK ccbin and
        // default/generation expressions are attno-mapped through newattmap.
        // Any tableElts are column options merged below (tablecmds.c:3031
        // saved_columns loop).
        Some(parent_oid) => {
            // C's duplicate-name scan (tablecmds.c:2589) precedes parent
            // processing, so it outranks the "is not partitioned" error.
            inheritance::partition_column_dup_scan(&stmt.tableElts)?;
            let parent = table::table_open(mcx, parent_oid, types_rel::NoLock)?;
            // C MergeAttributes (tablecmds.c:2675-2676): an enclosing command
            // still scanning the parent must not see its partition set grow.
            catalog_heap::CheckTableNotInUse(&parent, "CREATE TABLE .. PARTITION OF")?;
            // C MergeAttributes' relkind gate (tablecmds.c:2693-2699): a
            // non-partitioned table parent passes here — column merging (and
            // its 42703s) runs first; "is not partitioned" (42P17) is the
            // partition-bound step's error, after heap creation.
            if parent.rd_rel.relkind != types_rel::RELKIND_RELATION
                && parent.rd_rel.relkind != types_rel::RELKIND_FOREIGN_TABLE
                && parent.rd_rel.relkind != types_rel::RELKIND_PARTITIONED_TABLE
            {
                let pname = parent.name().to_string();
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!("inherited relation \"{pname}\" is not a table or foreign table"),
                    )
                    .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE),
                ));
            }
            // MergeAttributes persistence checks (tablecmds.c:2700-2730).
            if parent.rd_rel.relpersistence != types_core::RELPERSISTENCE_TEMP
                && relpersistence == types_core::RELPERSISTENCE_TEMP
            {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!(
                            "cannot create a temporary relation as partition of permanent relation \"{}\"",
                            parent.name()
                        ),
                    )
                    .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE),
                ));
            }
            if relpersistence != types_core::RELPERSISTENCE_TEMP
                && parent.rd_rel.relpersistence == types_core::RELPERSISTENCE_TEMP
            {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!(
                            "cannot create a permanent relation as partition of temporary relation \"{}\"",
                            parent.name()
                        ),
                    )
                    .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE),
                ));
            }
            if parent.rd_rel.relpersistence == types_core::RELPERSISTENCE_TEMP
                && !parent.rd_islocaltemp
            {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        "cannot create as partition of temporary relation of another session"
                            .to_string(),
                    )
                    .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE),
                ));
            }
            // newattmap: parent attno (1-based) -> child attno, 0 for
            // dropped parent columns (C MergeAttributes newattmap).
            let mut newattmap: mcx::PgVec<'mcx, AttrNumber> =
                mcx::vec_from_elem_in(mcx, 0i16, parent.rd_att.natts as usize);
            let mut child_natts: i32 = 0;
            for i in 0..parent.rd_att.natts as usize {
                if !parent.rd_att.attr(i).attisdropped {
                    child_natts += 1;
                    newattmap[i] = child_natts as AttrNumber;
                }
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
                    let raw = readfuncs::stringToNode(
                        mcx,
                        check.ccbin.as_ref().expect("check ccbin").as_str(),
                    )?;
                    let (expr, found_whole_row) = rewrite_manip::map_variable_attnos(
                        mcx,
                        raw,
                        1,
                        0,
                        &newattmap,
                        types_core::InvalidOid,
                    )?;
                    if found_whole_row {
                        return Err(Box::new(
                            PgError::new(
                                ERROR,
                                "cannot convert whole-row table reference".to_string(),
                            )
                            .with_detail(format!(
                                "Constraint \"{name}\" contains a whole-row reference to table \"{}\".",
                                parent.name()
                            ))
                            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
                        ));
                    }
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
            // partition with their attnos mapped through newattmap.
            for cnode in pg_constraint::RelationGetNotNullConstraints(mcx, &parent, false)?.iter() {
                let c = cnode
                    .as_variant::<types_nodes::rawnodes::Constraint>()
                    .expect("Constraint");
                let colname = c.keys.nth(0).as_string().expect("nn keys").sval;
                let attnum = (0..parent.rd_att.natts as usize)
                    .find(|&i| parent.rd_att.attr(i).attname.name_str() == colname.as_bytes())
                    .map(|i| newattmap[i])
                    .unwrap_or_else(|| panic!("not-null column {colname:?} not found"));
                partition_notnulls.push(inheritance::InheritedNotNull {
                    name: c.conname.expect("catalogued nn constraint has a name"),
                    attnum,
                });
            }
            let mut desc = tupdesc::CreateTemplateTupleDesc(mcx, child_natts)?;
            for i in 0..parent.rd_att.natts as usize {
                let child_attno = newattmap[i];
                if child_attno == 0 {
                    continue;
                }
                let parent_att = parent.rd_att.attr(i);
                tupdesc::TupleDescCopyEntry(
                    &mut desc,
                    child_attno,
                    parent.descr(),
                    (i + 1) as AttrNumber,
                );
                if parent_att.atthasdef {
                    let adbin =
                        pg_attrdef::GetAttrDefaultBin(mcx, parent_oid, (i + 1) as AttrNumber)?
                            .unwrap_or_else(|| {
                                panic!("default expression not found for attribute {}", i + 1)
                            });
                    let raw = readfuncs::stringToNode(mcx, &adbin)?;
                    let (expr, found_whole_row) = rewrite_manip::map_variable_attnos(
                        mcx,
                        raw,
                        1,
                        0,
                        &newattmap,
                        types_core::InvalidOid,
                    )?;
                    if found_whole_row {
                        return Err(Box::new(
                            PgError::new(
                                ERROR,
                                "cannot convert whole-row table reference".to_string(),
                            )
                            .with_detail(format!(
                                "Generation expression for column \"{}\" contains a whole-row reference to table \"{}\".",
                                String::from_utf8_lossy(parent_att.attname.name_str()),
                                parent.name()
                            ))
                            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
                        ));
                    }
                    partition_gendefs.push((child_attno, expr));
                }
                let j = child_attno as usize - 1;
                let att = desc.attr_mut(j);
                att.attnotnull = parent_att.attnotnull;
                att.attgenerated = parent_att.attgenerated;
                // Partitions are an integral part of the parent and inherit
                // identity columns (MergeAttributes' is_partition leg).
                att.attidentity = parent_att.attidentity;
                att.attislocal = false;
                att.attinhcount = 1;
                tupdesc::populate_compact_attribute(&mut desc, j);
            }
            parent.close(types_rel::NoLock)?;
            desc
        }
        None => match &merged {
            Some(m) => BuildDescForRelation(mcx, &m.columns)?,
            None => BuildDescForRelation(mcx, table_elts)?,
        },
    };

    // pgrcolumnar accepts only the analytics charter's type surface; refuse at CREATE TABLE
    // (docs/design/pgrcolumnar-impl.md §3).
    if access_method_id != InvalidOid
        && access_method_id != 2
        && syscache_seams::pg_am_amname::call(access_method_id)?.as_deref() == Some("pgrcolumnar")
    {
        // Deprecation posture of record (ADJUDICATION-20260818.md §3, Michael
        // 2026-08-19): the v1 endgame is MIGRATE to pgrcolumnar2; new v1 tables
        // warn per the house deprecation style (01P01, cf. the md5-password
        // worked example in libpq/crypt). A WARNING, not a refusal — existing
        // v1 batteries keep running until the migration drain completes.
        elog_seams::ereport::call(
            PgError::new(WARNING, "creating a table with the deprecated pgrcolumnar access method")
                .with_sqlstate(ERRCODE_WARNING_DEPRECATED_FEATURE)
                .with_detail(
                    "pgrcolumnar (v1) is deprecated; its endgame is migration to pgrcolumnar2.",
                )
                .with_hint(
                    "Use pgrcolumnar2 for new tables; migrate existing tables with \
                     scripts/pgrust-migrate-v1.sh (ALTER TABLE ... SET ACCESS METHOD pgrcolumnar2).",
                ),
        )?;
        for i in 0..descriptor.natts as usize {
            let att = descriptor.attr(i);
            if att.attisdropped {
                continue;
            }
            if pgrcolumnar::ColType::of_type_oid(att.atttypid).is_none() {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!(
                            "pgrcolumnar does not support the type of column \"{}\" (type oid {})",
                            String::from_utf8_lossy(att.attname.name_str()),
                            att.atttypid
                        ),
                    )
                    .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
                ));
            }
        }
    }

    // MergeAttributes' is_partition saved_columns pass (tablecmds.c:3031):
    // each column option must name an inherited column; generated-ness must
    // match the parent; a local raw default/generation expression overrides
    // the parent's inherited one.
    if parent_oid.is_some() {
        for elt in stmt.tableElts.iter() {
            if elt.node_tag() != types_nodes::NodeTag::T_ColumnDef {
                continue;
            }
            let restdef = elt
                .as_variant::<types_nodes::rawnodes::ColumnDef>()
                .expect("ColumnDef");
            let colname = restdef.colname.expect("ColumnDef.colname");
            let attno = (0..descriptor.natts as usize).find(|&i| {
                let a = descriptor.attr(i);
                !a.attisdropped && a.attname.name_str() == colname.as_bytes()
            });
            let Some(i) = attno else {
                return Err(Box::new(
                    PgError::new(ERROR, format!("column \"{colname}\" does not exist"))
                        .with_sqlstate(types_error::ERRCODE_UNDEFINED_COLUMN),
                ));
            };
            let coldef_generated = descriptor.attr(i).attgenerated as u8;
            if coldef_generated != 0 {
                if restdef.raw_default.is_some() && restdef.generated == 0 {
                    return Err(Box::new(
                        PgError::new(
                            ERROR,
                            format!(
                                "column \"{colname}\" inherits from generated column but specifies default"
                            ),
                        )
                        .with_sqlstate(types_error::ERRCODE_INVALID_COLUMN_DEFINITION),
                    ));
                }
                if restdef.identity != 0 {
                    return Err(Box::new(
                        PgError::new(
                            ERROR,
                            format!(
                                "column \"{colname}\" inherits from generated column but specifies identity"
                            ),
                        )
                        .with_sqlstate(types_error::ERRCODE_INVALID_COLUMN_DEFINITION),
                    ));
                }
            } else if restdef.generated != 0 {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!("child column \"{colname}\" specifies generation expression"),
                    )
                    .with_sqlstate(types_error::ERRCODE_INVALID_COLUMN_DEFINITION)
                    .with_hint(
                        "A child table column cannot be generated unless its parent column is."
                            .to_string(),
                    ),
                ));
            }
            if coldef_generated != 0
                && restdef.generated != 0
                && coldef_generated != restdef.generated
            {
                let kind = |g: u8| if g == b's' { "STORED" } else { "VIRTUAL" };
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!(
                            "column \"{colname}\" inherits from generated column of different kind"
                        ),
                    )
                    .with_sqlstate(types_error::ERRCODE_INVALID_COLUMN_DEFINITION)
                    .with_detail(format!(
                        "Parent column is {}, child column is {}.",
                        kind(coldef_generated),
                        kind(restdef.generated)
                    )),
                ));
            }
            if let Some(raw) = restdef.raw_default {
                let attnum = (i + 1) as AttrNumber;
                partition_gendefs.retain(|&(a, _)| a != attnum);
                partition_raw_defaults.push((attnum, raw, restdef.generated));
            }
        }
    }

    // DefineRelation's rawDefaults/cookedDefaults split (tablecmds.c:988-1025).
    // Merged columns re-number local attributes; raw defaults ride them.
    // Partition column options carry name-resolved attnos, not positions.
    let (raw_defaults, cooked_defaults) = match &merged {
        Some(m) => constraints::collect_column_defaults(mcx, &m.columns)?,
        None if parent_oid.is_some() => (partition_raw_defaults, mcx::PgVec::new_in(mcx)),
        None => constraints::collect_column_defaults(mcx, table_elts)?,
    };

    // C's access-method selection point (tablecmds.c:1027-1048): the erroring
    // lookup runs here, after MergeAttributes' checks, so e.g. the partition
    // persistence errors outrank "access method ... does not exist".
    let access_method_id = if let Some(amname) = stmt.accessMethod {
        commands_amcmds::get_table_am_oid(amname, false)?
    } else if types_rel::RELKIND_HAS_TABLE_AM(relkind)
        || relkind == types_rel::RELKIND_PARTITIONED_TABLE
    {
        let mut amoid = InvalidOid;
        if stmt.partbound.is_some() {
            amoid = lsyscache::get_rel_relam(inherit_oids[0])?;
        }
        if types_rel::RELKIND_HAS_TABLE_AM(relkind) && amoid == InvalidOid {
            amoid =
                commands_amcmds::get_table_am_oid(&tableam::default_table_access_method(), false)?;
        }
        amoid
    } else {
        InvalidOid
    };

    let relation_id = catalog_heap::heap_create_with_catalog(
        mcx,
        &catalog_heap::HeapCreateParams {
            relname,
            relnamespace: namespace_id,
            reltablespace: tablespace_id,
            ownerid: owner_id,
            accessmtd: access_method_id,
            relkind,
            relpersistence,
            reloftype: of_type_id,
            // tablecmds.c:1077-1078: shared_relation = false, mapped_relation = false.
            shared: false,
            mapped: false,
            // tablecmds.c:1082 passes the allowSystemTableMods GUC through,
            // lifting heap_create's system-namespace refusal (heap.c:316).
            allow_system_table_mods: init_small::globals::allowSystemTableMods(),
            reloptions: reloptions.as_deref(),
            relrewrite: InvalidOid,
            is_internal: false,
        },
        &descriptor,
    )?;

    // C StoreConstraints runs inside heap_create_with_catalog: inherited
    // cooked CHECKs, generation expressions and cooked column defaults land
    // before pg_inherits rows.
    if let Some(m) = &merged {
        inheritance::store_inherited_checks(mcx, relation_id, &m.checks)?;
        if !m.gendefs.is_empty() {
            xact::CommandCounterIncrement()?;
            let rel = table::table_open(mcx, relation_id, types_rel::NoLock)?;
            for &(attnum, expr) in m.gendefs.iter() {
                pg_attrdef::StoreAttrDefault(mcx, &rel, attnum, expr, false)?;
            }
            table::table_close(rel, types_rel::NoLock)?;
        }
    }
    if !cooked_defaults.is_empty() {
        xact::CommandCounterIncrement()?;
        let rel = table::table_open(mcx, relation_id, types_rel::NoLock)?;
        for &(attnum, expr) in cooked_defaults.iter() {
            pg_attrdef::StoreAttrDefault(mcx, &rel, attnum, expr, false)?;
        }
        table::table_close(rel, types_rel::NoLock)?;
    }
    if !partition_checks.is_empty() {
        inheritance::store_inherited_checks(mcx, relation_id, &partition_checks)?;
    }
    if !partition_gendefs.is_empty() {
        xact::CommandCounterIncrement()?;
        let rel = table::table_open(mcx, relation_id, types_rel::NoLock)?;
        for &(attnum, expr) in partition_gendefs.iter() {
            pg_attrdef::StoreAttrDefault(mcx, &rel, attnum, expr, false)?;
        }
        table::table_close(rel, types_rel::NoLock)?;
    }

    // Cook raw column DEFAULT and GENERATED expressions BEFORE processing the
    // partitioning clauses — C DefineRelation (tablecmds.c:1104-1106): "This
    // needs to be before processing the partitioning clauses because those
    // could refer to generated columns." So a bad generation expression
    // (42804/42883) is reported before partition-key validation (42P17/…).
    if !raw_defaults.is_empty() {
        // Make the newly-created pg_attribute rows visible before cooking reads
        // column types (C bumps the command counter right after
        // heap_create_with_catalog, tablecmds.c:1082, before opening the rel).
        xact::CommandCounterIncrement()?;
        let rel = table::table_open(mcx, relation_id, types_rel::AccessExclusiveLock)?;
        constraints::add_relation_new_constraints(
            mcx,
            &rel,
            &raw_defaults,
            &types_nodes::NodeList::nil(),
            Some(query_string),
        )?;
        rel.close(types_rel::NoLock)?;
        // Make column generation expressions visible for use by partitioning
        // (tablecmds.c:1111).
        xact::CommandCounterIncrement()?;
    }

    register_on_commit_action(relation_id, stmt.oncommit);

    xact::CommandCounterIncrement()?;

    // Partition bound: transform, validate against siblings, store.
    if let Some(parent_oid) = parent_oid {
        let bound_spec_node = stmt.partbound.expect("checked above");
        let parent = table::table_open(mcx, parent_oid, types_rel::NoLock)?;
        // C DefineRelation (tablecmds.c:1130-1136): the bound is validated
        // against the parent's partition key, so it better have one.
        if parent.rd_rel.relkind != types_rel::RELKIND_PARTITIONED_TABLE {
            let pname = parent.name().to_string();
            return Err(Box::new(
                PgError::new(ERROR, format!("\"{pname}\" is not partitioned"))
                    .with_sqlstate(types_error::ERRCODE_INVALID_OBJECT_DEFINITION),
            ));
        }
        // Lock the default partition before validating: its constraint changes
        // with every sibling added (C DefineRelation tablecmds.c:1156).
        let pdesc = partdesc::RelationGetPartitionDesc(&parent, true)?;
        let default_part_oid = pdesc
            .boundinfo
            .as_ref()
            .filter(|b| b.has_default())
            .map(|b| pdesc.oids[b.default_index as usize]);
        let default_rel = match default_part_oid {
            Some(oid) => Some(table::table_open(mcx, oid, types_rel::AccessExclusiveLock)?),
            None => None,
        };
        let rel = table::table_open(mcx, relation_id, types_rel::AccessExclusiveLock)?;
        let mut pstate = parser_small1::make_parsestate(mcx, None);
        pstate.p_sourcetext = Some(query_string.as_bytes());
        let bound = partition::transformPartitionBound(mcx, &mut pstate, &parent, bound_spec_node)?;
        {
            let key = partcache::RelationGetPartitionKey(&parent)?;
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
                Some(query_string.as_bytes()),
            )?;
            if let Some(default_rel) = &default_rel {
                partbounds::check_default_partition_contents(
                    mcx,
                    &parent,
                    default_rel,
                    &key,
                    pdesc.boundinfo.as_ref(),
                    &pdesc.oids,
                    spec,
                )?;
            }
        }
        if let Some(default_rel) = default_rel {
            // Keep the lock until commit.
            default_rel.close(types_rel::NoLock)?;
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
        let info = partition::compute_partition_key(mcx, &rel, spec, query_string)?;
        catalog_heap::StorePartitionKey(
            mcx,
            &rel,
            info.strategy,
            info.partattrs.len() as i16,
            &info.partattrs,
            &info.partexprs,
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
    // and row trigger the parent carries; FKs have no cloning lane yet.
    if let Some(parent_oid) = parent_oid {
        let parent = table::table_open(mcx, parent_oid, types_rel::NoLock)?;
        let rel = table::table_open(mcx, relation_id, types_rel::NoLock)?;
        let idxlist = relcache::RelationGetIndexList(mcx, parent_oid)?;
        for &idxoid in idxlist.iter() {
            let idx_rel = indexam::index_open(mcx, idxoid, types_rel::AccessShareLock)?;
            // A foreign partition gets no index: skip, or fail on a unique
            // parent index (tablecmds.c:1279-1295).
            if rel.rd_rel.relkind == types_rel::RELKIND_FOREIGN_TABLE {
                if idx_rel.rd_index.as_ref().expect("rd_index").indisunique {
                    return Err(Box::new(
                        PgError::new(
                            ERROR,
                            format!(
                                "cannot create foreign partition of partitioned table \"{}\"",
                                parent.name()
                            ),
                        )
                        .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE)
                        .with_detail(format!(
                            "Table \"{}\" contains indexes that are unique.",
                            parent.name()
                        )),
                    ));
                }
                indexam::index_close(idx_rel, types_rel::AccessShareLock)?;
                continue;
            }
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
            partition::CloneRowTriggersToPartition(mcx, &parent, &rel)?;
        }
        fk::CloneForeignKeyConstraints(mcx, None, &parent, &rel)?;
        rel.close(types_rel::NoLock)?;
        parent.close(types_rel::NoLock)?;
    }

    let old_notnulls: &[inheritance::InheritedNotNull<'mcx>] = match &merged {
        Some(m) => &m.notnulls[..],
        None => &partition_notnulls[..],
    };
    // Raw DEFAULT/GENERATED expressions were already cooked above (before the
    // partitioning clauses, per C tablecmds.c:1104). CHECK constraints
    // (tablecmds.c:1339) and NOT NULL constraints (tablecmds.c:1354) are
    // processed here, AFTER the partition key is stored.
    if !stmt.constraints.is_nil() || !stmt.nnconstraints.is_nil() || !old_notnulls.is_empty() {
        let rel = table::table_open(mcx, relation_id, types_rel::AccessExclusiveLock)?;
        let mut connames: mcx::PgVec<'_, &str> = mcx::PgVec::new_in(mcx);
        if !stmt.constraints.is_nil() {
            // C passes allow_merge=true here (tablecmds.c:1339); partitions
            // depend on it — the purely-inherited fallback in
            // MergeWithExistingConstraint excludes relispartition rels.
            let conlist = constraints::add_relation_new_constraints_ext(
                mcx,
                &rel,
                &[],
                &stmt.constraints,
                true,
                true,
                false,
                Some(query_string),
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
                    &[(
                        alter::Anum_pg_attribute_attnotnull,
                        ::datum::Datum::from_bool(true),
                    )],
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

#[cfg(test)]
mod build_desc_tests {
    use super::*;
    use types_nodes::Node;

    fn tn_with_bounds<'mcx>(mcx: Mcx<'mcx>, ndims: usize) -> TypeName<'mcx> {
        // The grammar stores one Integer node per `[]` / `[n]` bound
        // (gram actions: NodeList::make1(Node::mk_integer(mcx, -1))).
        let mut tn = TypeName::default();
        for _ in 0..ndims {
            tn.arrayBounds
                .lappend(mcx, Node::mk_integer(mcx, -1).unwrap())
                .unwrap();
        }
        tn
    }

    // C BuildDescForRelation: attndims = list_length(typeName->arrayBounds).
    // `labels text[]` carries one bound; `int[][]` two; scalars none.
    #[test]
    fn attdim_is_array_bounds_length() {
        let root = mcx::session_root("attdim-test");
        let mcx = root.mcx();
        for ndims in [0usize, 1, 2, 3] {
            let tn = tn_with_bounds(mcx, ndims);
            assert_eq!(column_def_attdim(&tn, "c").unwrap(), ndims as i32);
        }
    }

    // C caps attdim at PG_INT16_MAX with 54000 "too many array dimensions";
    // the cap must hold in release builds (not just a debug_assert further
    // down in TupleDescInitEntry).
    #[test]
    fn attdim_cap_error_identity_matches_c() {
        let root = mcx::session_root("attdim-cap-test");
        let mcx = root.mcx();
        let tn = tn_with_bounds(mcx, i16::MAX as usize);
        assert_eq!(column_def_attdim(&tn, "c").unwrap(), i16::MAX as i32);
        let tn = tn_with_bounds(mcx, i16::MAX as usize + 1);
        let e = column_def_attdim(&tn, "c").unwrap_err();
        assert_eq!(e.message(), "too many array dimensions");
        assert_eq!(e.sqlstate(), ERRCODE_PROGRAM_LIMIT_EXCEEDED);
    }

    // C rejects SETOF column declarations here with 42P16.
    #[test]
    fn setof_error_identity_matches_c() {
        let root = mcx::session_root("attdim-setof-test");
        let mcx = root.mcx();
        let mut tn = tn_with_bounds(mcx, 0);
        tn.setof = true;
        let e = column_def_attdim(&tn, "x").unwrap_err();
        assert_eq!(e.message(), "column \"x\" cannot be declared SETOF");
        assert_eq!(e.sqlstate(), ERRCODE_INVALID_TABLE_DEFINITION);
    }
}

#[cfg(test)]
mod define_relation_error_tests {
    use super::*;

    #[test]
    fn temp_table_in_security_restricted_operation_is_42501() {
        let (uid, ctx) = miscinit::GetUserIdAndSecContext();
        miscinit::SetUserIdAndSecContext(uid, ctx | types_core::SECURITY_RESTRICTED_OPERATION);
        let e = reject_temp_in_security_restricted(types_core::RELPERSISTENCE_TEMP).unwrap_err();
        assert_eq!(
            e.message(),
            "cannot create temporary table within security-restricted operation"
        );
        assert_eq!(e.sqlstate(), ERRCODE_INSUFFICIENT_PRIVILEGE);
        miscinit::SetUserIdAndSecContext(uid, ctx);

        let (uid, ctx) = miscinit::GetUserIdAndSecContext();
        miscinit::SetUserIdAndSecContext(uid, ctx | types_core::SECURITY_RESTRICTED_OPERATION);
        assert!(reject_temp_in_security_restricted(types_core::RELPERSISTENCE_PERMANENT).is_ok());
        miscinit::SetUserIdAndSecContext(uid, ctx);

        assert!(reject_temp_in_security_restricted(types_core::RELPERSISTENCE_TEMP).is_ok());
    }
}

#[cfg(test)]
mod cache_lookup_error_tests {
    use super::*;

    // C: elog(ERROR, "cache lookup failed for relation %u") -- a catchable
    // XX000, not a backend abort.  These probes used to panic!(), killing the
    // process; the whole family now returns PgError.
    #[test]
    fn cache_lookup_failure_is_a_catchable_xx000() {
        let e = cache_lookup_failed("relation", 16384);
        assert_eq!(e.message(), "cache lookup failed for relation 16384");
        assert_eq!(e.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
        assert_eq!(e.level(), ERROR);

        let e = cache_lookup_failed_attribute(3, 16384);
        assert_eq!(
            e.message(),
            "cache lookup failed for attribute 3 of relation 16384"
        );
        assert_eq!(e.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
    }
}
