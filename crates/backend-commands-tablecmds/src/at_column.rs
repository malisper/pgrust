//! `commands/tablecmds.c` — ALTER TABLE per-column executed families dispatched
//! from [`crate::at_phase::ATExecCmd`].
//!
//! PORTED here (faithful, 100% C logic):
//!   - `ATExecColumnDefault` (tablecmds.c:8126) — ALTER COLUMN SET / DROP DEFAULT
//!   - `ATExecCookedColumnDefault` (tablecmds.c:8210) — add a pre-cooked default
//!   - `ATExecSetStatistics` (tablecmds.c:8906) — ALTER COLUMN SET STATISTICS
//!   - `ATExecSetStorage` (tablecmds.c:9192) — ALTER COLUMN SET STORAGE
//!     (`GetAttributeStorage` validates the mode for the type, writes
//!     `attstorage`, then `SetIndexStorageProperties` recurses to each index
//!     whose `indkey` includes the altered column). The `FormData_pg_index`
//!     carrier now carries the full `indkey` int2vector (additive widen), and
//!     the recursion opens each index and reads it via `rd_index_indkey`.
//!   - `ATExecClusterOn` (tablecmds.c) — ALTER TABLE CLUSTER ON `<index>`
//!     (`get_relname_relid` + `check_index_is_clusterable` +
//!     `mark_index_clustered`, all landed cluster.c / lsyscache.c seams)
//!   - `ATExecDropCluster` (tablecmds.c) — ALTER TABLE SET WITHOUT CLUSTER
//!     (`mark_index_clustered(rel, InvalidOid)`)
//!
//! These last two perform the C `SearchSysCacheAttName(ATTNAME, relid, colName)`
//! → modify the `Form_pg_attribute` field → `heap_modify_tuple(repl_val/null/repl)`
//! → `CatalogTupleUpdate(attrelation, ...)` write, expressed over the typed
//! [`PgAttributeUpdateRow`] carrier and the `catalog_tuple_update_pg_attribute`
//! seam (the shared pg_attribute write leaf, owner backend-catalog-indexing).
//!
//! VARIABLE-COLUMN OPTION WRITES (the `text[]` reloptions/attoptions lane) —
//! both legs are ported via the on-disk varlena-image bridge rather than a bare
//! `Datum`:
//!   - `ATExecSetRelOptions` writes the variable `reloptions` (`text[]`) column
//!     of `pg_class`. The merged options ride the
//!     `update_pg_class_reloptions` carrier (`heap_modify_tuple` over the
//!     `text[]` image built by `transformRelOptionsBytes`).
//!   - `ATExecSetOptions` writes the variable `attoptions` (`text[]`) column of
//!     `pg_attribute`. The merged options ride the
//!     `PgAttributeUpdateRow.attoptions` ByRef-bytes carrier (same
//!     `transformRelOptionsBytes` → `construct_text_array_bytes` image path).
//!
//! [`PgAttributeUpdateRow`]: types_catalog::pg_attribute::PgAttributeUpdateRow

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

use mcx::Mcx;

use types_catalog::catalog_dependency::ObjectAddress;
use types_catalog::pg_attribute::{AttributeRelationId, PgAttributeUpdateRow};
use types_core::primitive::{AttrNumber, InvalidAttrNumber};
use types_error::{
    PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_COLUMN_REFERENCE,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_INVALID_TABLE_DEFINITION,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_COLUMN,
    ERRCODE_UNDEFINED_OBJECT, ERRCODE_WRONG_OBJECT_TYPE, ERROR, WARNING,
};
use types_nodes::ddlnodes::AlterTableType;
use types_nodes::nodes::{ntag, Node};
use types_rel::Relation;
use types_storage::lock::{NoLock, RowExclusiveLock, ShareLock, LOCKMODE};
use types_nodes::parsenodes::DROP_RESTRICT;
use types_statistics::MAX_STATISTICS_TARGET;
use types_tuple::access::{
    ATTRIBUTE_GENERATED_STORED, ATTRIBUTE_GENERATED_VIRTUAL, RELKIND_INDEX, RELKIND_MATVIEW,
    RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_TOASTVALUE,
    RELKIND_VIEW,
};
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_access_common_relation::relation_open;
use backend_catalog_indexing_seams as indexing_seam;
use backend_catalog_pg_attrdef::{RemoveAttrDefault, StoreAttrDefault};
use backend_utils_cache_lsyscache::attribute::get_attnum;
use backend_utils_cache_syscache::{SearchSysCacheAttName, ATTNAME, ATTNUM};
use backend_catalog_objectaccess_seams as objaccess_seam;

use backend_commands_tablecmds_seams as seam;

use crate::helpers::{here, RelationRelationId};

/// `ObjectAddressSubSet(addr, class, object, sub)`.
fn object_address_subset(class_id: types_core::Oid, object_id: types_core::Oid, sub: i32) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: sub,
    }
}

// `pg_class.relreplident` values (catalog/pg_class.h).
const REPLICA_IDENTITY_DEFAULT: i8 = b'd' as i8;
const REPLICA_IDENTITY_NOTHING: i8 = b'n' as i8;
const REPLICA_IDENTITY_FULL: i8 = b'f' as i8;
const REPLICA_IDENTITY_INDEX: i8 = b'i' as i8;

/// `relation_mark_replica_identity(rel, ri_type, indexOid, is_internal)`
/// (tablecmds.c:18402) — update `pg_class.relreplident` and the per-index
/// `pg_index.indisreplident` flags. `indexOid` is `InvalidOid` for the
/// non-index identity types. Caller holds an exclusive lock on `rel`.
fn relation_mark_replica_identity<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    ri_type: i8,
    index_oid: types_core::Oid,
) -> PgResult<()> {
    // Check whether relreplident has changed, and update it if so. The pg_class
    // open/SearchSysCacheCopy1/conditional-poke/CatalogTupleUpdate/close lives in
    // the pg_class-write owner (backend-catalog-indexing).
    let valid = indexing_seam::set_pg_class_relreplident::call(rel.rd_id, ri_type)?;
    if !valid {
        return backend_utils_error::ereport(ERROR)
            .errmsg_internal(format!("cache lookup failed for relation \"{}\"", rel.name()))
            .finish(here("relation_mark_replica_identity"))
            .map(|()| unreachable!());
    }

    // Update the per-index indisreplident flags correctly. Iterate
    // RelationGetIndexList(rel): set the bit on `index_oid`, clear it on all the
    // others; each dirty index gets a CacheInvalidateRelcache(rel).
    let index_list = backend_utils_cache_relcache_seams::relation_get_index_list::call(mcx, rel)?;
    for this_index_oid in index_list.iter().copied() {
        let want = this_index_oid == index_oid;
        let (found, dirty) =
            indexing_seam::set_index_isreplident::call(this_index_oid, want)?;
        if !found {
            return backend_utils_error::ereport(ERROR)
                .errmsg_internal(format!("cache lookup failed for index {this_index_oid}"))
                .finish(here("relation_mark_replica_identity"))
                .map(|()| unreachable!());
        }
        if dirty {
            // InvokeObjectPostAlterHookArg(IndexRelationId, ...): no-op without an
            // installed object-access hook.
            //
            // Invalidate the relcache for the table, so that after we commit all
            // sessions will refresh the table's replica identity index before
            // attempting any UPDATE or DELETE on the table.
            backend_utils_cache_inval_seams::cache_invalidate_relcache::call(rel.rd_id)?;
        }
    }
    Ok(())
}

/// `ATExecReplicaIdentity(rel, stmt, lockmode)` (tablecmds.c:18490) — ALTER TABLE
/// `<name>` REPLICA IDENTITY ...
pub fn ATExecReplicaIdentity<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    stmt: &types_nodes::ddlnodes::ReplicaIdentityStmt<'_>,
    _lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    let identity_type = stmt.identity_type;

    if identity_type == REPLICA_IDENTITY_DEFAULT
        || identity_type == REPLICA_IDENTITY_FULL
        || identity_type == REPLICA_IDENTITY_NOTHING
    {
        relation_mark_replica_identity(mcx, rel, identity_type, types_core::InvalidOid)?;
        return Ok(object_address_subset(types_core::InvalidOid, types_core::InvalidOid, 0));
    } else if identity_type == REPLICA_IDENTITY_INDEX {
        // fallthrough
    } else {
        return backend_utils_error::ereport(ERROR)
            .errmsg_internal(format!("unexpected identity type {}", identity_type as u8))
            .finish(here("ATExecReplicaIdentity"))
            .map(|()| unreachable!());
    }

    // Check that the index exists.
    let index_name = stmt
        .name
        .as_ref()
        .map(|s| s.as_str())
        .expect("REPLICA IDENTITY USING INDEX requires an index name");
    let index_oid = backend_utils_cache_lsyscache_seams::get_relname_relid::call(
        index_name,
        rel.rd_rel.relnamespace,
    )?;
    if index_oid == types_core::InvalidOid {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!(
                "index \"{}\" for table \"{}\" does not exist",
                index_name,
                rel.name()
            ))
            .finish(here("ATExecReplicaIdentity"))
            .map(|()| unreachable!());
    }

    // indexRel = index_open(indexOid, ShareLock): take the lock + build/pin the
    // index relcache entry. Held for the duration (the lock kept to txn end).
    let index_rel = relation_open(mcx, index_oid, ShareLock)?;
    let index_relname = index_rel.name().to_string();

    // Read everything ATExecReplicaIdentity inspects off the opened index
    // (rd_index flags, rd_indam->amcanunique, expression/predicate presence, key
    // columns) via the relcache projection.
    let info = backend_utils_cache_relcache_seams::get_replident_index_info::call(index_oid)?;

    // Check that the index is on the relation we're altering.
    if !info.is_index || info.indrelid != rel.rd_id {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "\"{}\" is not an index for table \"{}\"",
                index_relname,
                rel.name()
            ))
            .finish(here("ATExecReplicaIdentity"))
            .map(|()| unreachable!());
    }

    // The AM must support uniqueness, and the index must in fact be unique. If we
    // have a WITHOUT OVERLAPS constraint (uniqueness + exclusion), we can use that
    // too.
    if (!info.amcanunique || !info.indisunique)
        && !(info.indisunique && info.indisexclusion)
    {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "cannot use non-unique index \"{index_relname}\" as replica identity"
            ))
            .finish(here("ATExecReplicaIdentity"))
            .map(|()| unreachable!());
    }
    // Deferred indexes are not guaranteed to be always unique.
    if !info.indimmediate {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "cannot use non-immediate index \"{index_relname}\" as replica identity"
            ))
            .finish(here("ATExecReplicaIdentity"))
            .map(|()| unreachable!());
    }
    // Expression indexes aren't supported.
    if info.has_expressions {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "cannot use expression index \"{index_relname}\" as replica identity"
            ))
            .finish(here("ATExecReplicaIdentity"))
            .map(|()| unreachable!());
    }
    // Predicate indexes aren't supported.
    if info.has_predicate {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "cannot use partial index \"{index_relname}\" as replica identity"
            ))
            .finish(here("ATExecReplicaIdentity"))
            .map(|()| unreachable!());
    }

    // Check index for nullable columns.
    for col in &info.key_columns {
        let attno = col.attno;

        // Reject any system columns (attno <= 0, which also covers the 0
        // expression-column marker, though expression indexes are rejected
        // above).
        if attno <= 0 {
            return backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
                .errmsg(format!(
                    "index \"{index_relname}\" cannot be used as replica identity because column {attno} is a system column"
                ))
                .finish(here("ATExecReplicaIdentity"))
                .map(|()| unreachable!());
        }

        let attr = rel.rd_att.attr((attno - 1) as usize);
        if !attr.attnotnull {
            let attname = String::from_utf8_lossy(attr.attname.name_str()).into_owned();
            return backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!(
                    "index \"{index_relname}\" cannot be used as replica identity because column \"{attname}\" is nullable"
                ))
                .finish(here("ATExecReplicaIdentity"))
                .map(|()| unreachable!());
        }
    }

    // This index is suitable for use as a replica identity. Mark it.
    relation_mark_replica_identity(mcx, rel, identity_type, index_oid)?;

    // index_close(indexRel, NoLock): drop the relcache pin, keep the lock.
    drop(index_rel);

    Ok(object_address_subset(types_core::InvalidOid, types_core::InvalidOid, 0))
}


// ===========================================================================
// ATExecColumnDefault (tablecmds.c:8126) — ALTER COLUMN SET / DROP DEFAULT
// ===========================================================================

/// `ATExecColumnDefault(rel, colName, newDefault, lockmode)` (tablecmds.c:8126).
/// `newDefault == NULL` is DROP DEFAULT; otherwise SET DEFAULT.
pub fn ATExecColumnDefault<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    colName: &str,
    newDefault: Option<&Node<'mcx>>,
    _lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    // attnum = get_attnum(RelationGetRelid(rel), colName);
    let attnum = get_attnum(rel.rd_id, colName)?;
    if attnum == InvalidAttrNumber {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_COLUMN)
            .errmsg(format!(
                "column \"{}\" of relation \"{}\" does not exist",
                colName,
                rel.name()
            ))
            .finish(here("ATExecColumnDefault")).map(|()| unreachable!());
    }

    // Prevent them from altering a system attribute.
    if attnum <= 0 {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("cannot alter system column \"{colName}\""))
            .finish(here("ATExecColumnDefault")).map(|()| unreachable!());
    }

    let att = rel.rd_att.attr((attnum - 1) as usize);

    if att.attidentity != 0 {
        // column is an identity column
        let mut b = backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!(
                "column \"{}\" of relation \"{}\" is an identity column",
                colName,
                rel.name()
            ));
        if newDefault.is_none() {
            b = b.errhint(format!(
                "Use {} instead.",
                "ALTER TABLE ... ALTER COLUMN ... DROP IDENTITY"
            ));
        }
        return b.finish(here("ATExecColumnDefault")).map(|()| unreachable!());
    }

    if att.attgenerated != 0 {
        // column is a generated column
        let mut b = backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!(
                "column \"{}\" of relation \"{}\" is a generated column",
                colName,
                rel.name()
            ));
        if newDefault.is_some() {
            b = b.errhint(format!(
                "Use {} instead.",
                "ALTER TABLE ... ALTER COLUMN ... SET EXPRESSION"
            ));
        } else if att.attgenerated == ATTRIBUTE_GENERATED_STORED {
            b = b.errhint(format!(
                "Use {} instead.",
                "ALTER TABLE ... ALTER COLUMN ... DROP EXPRESSION"
            ));
        }
        return b.finish(here("ATExecColumnDefault")).map(|()| unreachable!());
    }

    // Remove any old default for the column. RESTRICT for safety. Treated as an
    // internal op when preparatory to adding a new default, else user-initiated.
    // RemoveAttrDefault(relid, attnum, DROP_RESTRICT, false, newDefault != NULL);
    RemoveAttrDefault(
        rel.rd_id,
        attnum,
        DROP_RESTRICT,
        false,
        newDefault.is_some(),
    )?;

    if let Some(new_default) = newDefault {
        // SET DEFAULT: build one RawColumnDefault and run AddRelationNewConstraints.
        //   rawEnt->attnum = attnum; rawEnt->raw_default = newDefault;
        //   rawEnt->generated = '\0';
        //   AddRelationNewConstraints(rel, list_make1(rawEnt), NIL,
        //                             false, true, false, NULL);
        let raw_default_ptr = mcx::alloc_in(mcx, new_default.clone_in(mcx)?)?;
        let raw_defaults: [(AttrNumber, types_nodes::nodes::NodePtr<'mcx>, i8); 1] =
            [(attnum, raw_default_ptr, 0)];
        seam::add_relation_new_constraints::call(
            mcx,
            rel,
            &raw_defaults,
            &[],
            false,
            true,
            false,
            None,
        )?;
    }

    // ObjectAddressSubSet(address, RelationRelationId, relid, attnum);
    Ok(object_address_subset(RelationRelationId, rel.rd_id, attnum as i32))
}

// ===========================================================================
// ATExecCookedColumnDefault (tablecmds.c:8210) — add a pre-cooked default
// ===========================================================================

/// `ATExecCookedColumnDefault(rel, attnum, newDefault)` (tablecmds.c:8210).
pub fn ATExecCookedColumnDefault<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    attnum: i16,
    newDefault: &Node<'mcx>,
) -> PgResult<ObjectAddress> {
    // We assume no checking is required.

    // Remove any old default for the column. RESTRICT for safety; internal op.
    // RemoveAttrDefault(relid, attnum, DROP_RESTRICT, false, true);
    RemoveAttrDefault(rel.rd_id, attnum, DROP_RESTRICT, false, true)?;

    // (void) StoreAttrDefault(rel, attnum, newDefault, true);
    let _ = StoreAttrDefault(mcx, rel.rd_id, attnum, newDefault, true)?;

    // ObjectAddressSubSet(address, RelationRelationId, relid, attnum);
    Ok(object_address_subset(RelationRelationId, rel.rd_id, attnum as i32))
}

// ===========================================================================
// ATExecSetStatistics (tablecmds.c:8906) — ALTER COLUMN SET STATISTICS
// ===========================================================================

/// `ATExecSetStatistics(rel, colName, colNum, newValue, lockmode)`
/// (tablecmds.c:8906). Writes `pg_attribute.attstattarget`.
pub fn ATExecSetStatistics<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    colName: Option<&str>,
    colNum: i16,
    newValue: Option<&Node<'mcx>>,
    _lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    // We allow referencing columns by numbers only for indexes, since table
    // column numbers could contain gaps if columns are later dropped.
    if rel.rd_rel.relkind != RELKIND_INDEX
        && rel.rd_rel.relkind != RELKIND_PARTITIONED_INDEX
        && colName.is_none()
    {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("cannot refer to non-index column by number".to_string())
            .finish(here("ATExecSetStatistics"))
            .map(|()| unreachable!());
    }

    // -1 was used in previous versions for the default setting.
    let mut newtarget: i32 = 0;
    let newtarget_default;
    match newValue {
        Some(node) => {
            let ival = match node.node_tag() {
                ntag::T_Integer => node.expect_integer().ival,
                _ => {
                    return Err(types_error::PgError::error(
                        "ATExecSetStatistics: SET STATISTICS value is not an Integer node",
                    ))
                }
            };
            if ival != -1 {
                newtarget = ival;
                newtarget_default = false;
            } else {
                newtarget_default = true;
            }
        }
        None => newtarget_default = true,
    }

    if !newtarget_default {
        // Limit target to a sane range.
        if newtarget < 0 {
            return backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!("statistics target {newtarget} is too low"))
                .finish(here("ATExecSetStatistics"))
                .map(|()| unreachable!());
        } else if newtarget > MAX_STATISTICS_TARGET {
            newtarget = MAX_STATISTICS_TARGET;
            backend_utils_error::ereport(WARNING)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!("lowering statistics target to {newtarget}"))
                .finish(here("ATExecSetStatistics"))?;
        }
    }

    // attrelation = table_open(AttributeRelationId, RowExclusiveLock);
    let attrelation = relation_open(mcx, AttributeRelationId, RowExclusiveLock)?;

    let tuple = match colName {
        Some(colname) => match SearchSysCacheAttName(mcx, rel.rd_id, colname)? {
            Some(t) => t,
            None => {
                return backend_utils_error::ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_COLUMN)
                    .errmsg(format!(
                        "column \"{}\" of relation \"{}\" does not exist",
                        colname,
                        rel.name()
                    ))
                    .finish(here("ATExecSetStatistics"))
                    .map(|()| unreachable!());
            }
        },
        None => match backend_utils_cache_syscache::SearchSysCacheAttNum(mcx, rel.rd_id, colNum)? {
            Some(t) => t,
            None => {
                return backend_utils_error::ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_COLUMN)
                    .errmsg(format!(
                        "column number {} of relation \"{}\" does not exist",
                        colNum,
                        rel.name()
                    ))
                    .finish(here("ATExecSetStatistics"))
                    .map(|()| unreachable!());
            }
        },
    };

    // attrtuple = (Form_pg_attribute) GETSTRUCT(tuple); attnum = attrtuple->attnum;
    let cache_id = if colName.is_some() { ATTNAME } else { backend_utils_cache_syscache::ATTNUM };
    let attnum = att_field_i16(mcx, cache_id, &tuple, Anum_pg_attribute_attnum)?;
    let attgenerated = att_field_char(mcx, cache_id, &tuple, Anum_pg_attribute_attgenerated)?;
    let colname_for_msg = colName.unwrap_or("");

    if attnum <= 0 {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("cannot alter system column \"{colname_for_msg}\""))
            .finish(here("ATExecSetStatistics"))
            .map(|()| unreachable!());
    }

    // Prevent this as long as the ANALYZE code skips virtual generated columns.
    if attgenerated == ATTRIBUTE_GENERATED_VIRTUAL {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "cannot alter statistics on virtual generated column \"{colname_for_msg}\""
            ))
            .finish(here("ATExecSetStatistics"))
            .map(|()| unreachable!());
    }

    if rel.rd_rel.relkind == RELKIND_INDEX || rel.rd_rel.relkind == RELKIND_PARTITIONED_INDEX {
        let rd_index = rel
            .rd_index
            .as_ref()
            .expect("an index relation must carry rd_index");
        if attnum > rd_index.indnkeyatts {
            return backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!(
                    "cannot alter statistics on included column \"{}\" of index \"{}\"",
                    String::from_utf8_lossy(rel.rd_att.attr((attnum - 1) as usize).attname.name_str()),
                    rel.name()
                ))
                .finish(here("ATExecSetStatistics"))
                .map(|()| unreachable!());
        }
        // C: `rel->rd_index->indkey.values[attnum - 1] != 0`. The widened
        // `FormData_pg_index` carrier now holds the full `indkey` int2vector.
        let indkey_val = rd_index
            .indkey
            .get((attnum - 1) as usize)
            .copied()
            .unwrap_or(0);
        if indkey_val != 0 {
            return backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!(
                    "cannot alter statistics on non-expression column \"{}\" of index \"{}\"",
                    String::from_utf8_lossy(rel.rd_att.attr((attnum - 1) as usize).attname.name_str()),
                    rel.name()
                ))
                .errhint("Alter statistics on table column instead.".to_string())
                .finish(here("ATExecSetStatistics"))
                .map(|()| unreachable!());
        }
    }

    // Build new tuple: replace attstattarget only (Some(value) or SQL NULL).
    let row = PgAttributeUpdateRow {
        attstattarget: Some(if newtarget_default {
            None
        } else {
            Some(newtarget as i16)
        }),
        ..Default::default()
    };
    indexing_seam::catalog_tuple_update_pg_attribute::call(mcx, &attrelation, &tuple, &row)?;

    // InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attnum);
    // ObjectAddressSubSet(address, RelationRelationId, RelationGetRelid(rel), attnum);
    let address = object_address_subset(RelationRelationId, rel.rd_id, attnum as i32);

    // table_close(attrelation, RowExclusiveLock) — RAII drop of the relation
    // handle (lmgr lock is transaction-scoped).
    drop(attrelation);

    Ok(address)
}

/// `ATPrepDropExpression(rel, cmd, recurse, recursing, lockmode)`
/// (tablecmds.c:8756) — ALTER COLUMN DROP EXPRESSION, prep leg.
pub fn ATPrepDropExpression<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    colName: Option<&str>,
    recurse: bool,
    recursing: bool,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    // Reject ONLY if there are child tables.
    if !recurse
        && !backend_catalog_pg_inherits::find_inheritance_children(mcx, rel.rd_id, lockmode)?
            .is_empty()
    {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(
                "ALTER TABLE / DROP EXPRESSION must be applied to child tables too".to_string(),
            )
            .finish(here("ATPrepDropExpression"));
    }

    // Cannot drop generation expression from inherited columns.
    if !recursing {
        let colname = colName.unwrap_or("");
        let tuple = match SearchSysCacheAttName(mcx, rel.rd_id, colname)? {
            Some(t) => t,
            None => {
                return backend_utils_error::ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_COLUMN)
                    .errmsg(format!(
                        "column \"{}\" of relation \"{}\" does not exist",
                        colname,
                        rel.name()
                    ))
                    .finish(here("ATPrepDropExpression"));
            }
        };
        let attinhcount = att_field_i16(
            mcx,
            ATTNAME,
            &tuple,
            types_catalog::pg_attribute::Anum_pg_attribute_attinhcount,
        )?;
        if attinhcount > 0 {
            return backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
                .errmsg("cannot drop generation expression from inherited column".to_string())
                .finish(here("ATPrepDropExpression"));
        }
    }
    Ok(())
}

/// `ATExecDropExpression(rel, colName, missing_ok, lockmode)`
/// (tablecmds.c:8800) — ALTER COLUMN DROP EXPRESSION, exec leg. Return value is
/// the address of the affected column.
pub fn ATExecDropExpression<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    colName: Option<&str>,
    missing_ok: bool,
    _lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    let colname = colName.unwrap_or("");

    // attrelation = table_open(AttributeRelationId, RowExclusiveLock);
    let attrelation = relation_open(mcx, AttributeRelationId, RowExclusiveLock)?;

    let tuple = match SearchSysCacheAttName(mcx, rel.rd_id, colname)? {
        Some(t) => t,
        None => {
            return backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_COLUMN)
                .errmsg(format!(
                    "column \"{}\" of relation \"{}\" does not exist",
                    colname,
                    rel.name()
                ))
                .finish(here("ATExecDropExpression"))
                .map(|()| unreachable!());
        }
    };

    let attnum = att_field_i16(mcx, ATTNAME, &tuple, Anum_pg_attribute_attnum)?;
    let attgenerated = att_field_char(mcx, ATTNAME, &tuple, Anum_pg_attribute_attgenerated)?;

    if attnum <= 0 {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("cannot alter system column \"{colname}\""))
            .finish(here("ATExecDropExpression"))
            .map(|()| unreachable!());
    }

    // A virtual generated column would need a table rewrite to materialize.
    if attgenerated == ATTRIBUTE_GENERATED_VIRTUAL {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(
                "ALTER TABLE / DROP EXPRESSION is not supported for virtual generated columns"
                    .to_string(),
            )
            .errdetail(format!(
                "Column \"{}\" of relation \"{}\" is a virtual generated column.",
                colname,
                rel.name()
            ))
            .finish(here("ATExecDropExpression"))
            .map(|()| unreachable!());
    }

    if attgenerated == 0 {
        if !missing_ok {
            return backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "column \"{}\" of relation \"{}\" is not a generated column",
                    colname,
                    rel.name()
                ))
                .finish(here("ATExecDropExpression"))
                .map(|()| unreachable!());
        } else {
            backend_utils_error::ereport(types_error::error::NOTICE)
                .errmsg(format!(
                    "column \"{}\" of relation \"{}\" is not a generated column, skipping",
                    colname,
                    rel.name()
                ))
                .finish(here("ATExecDropExpression"))?;
            // heap_freetuple(tuple); table_close(attrelation, ...); — RAII.
            drop(attrelation);
            return Ok(object_address_subset(
                types_core::InvalidOid,
                types_core::InvalidOid,
                0,
            ));
        }
    }

    // Mark the column as no longer generated. (The atthasdef flag is cleared
    // by RemoveAttrDefault.)
    let row = PgAttributeUpdateRow {
        attgenerated: Some(0),
        ..Default::default()
    };
    indexing_seam::catalog_tuple_update_pg_attribute::call(mcx, &attrelation, &tuple, &row)?;

    // InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attnum);
    objaccess_seam::invoke_object_post_alter_hook::call(RelationRelationId, rel.rd_id, attnum as i32)?;

    // heap_freetuple(tuple); table_close(attrelation, RowExclusiveLock); — RAII.
    drop(attrelation);

    // Drop the dependency records of the GENERATED expression, in particular
    // its INTERNAL dependency on the column.
    let attrdefoid = backend_catalog_pg_attrdef::GetAttrDefaultOid(mcx, rel.rd_id, attnum)?;
    if !types_core::primitive::OidIsValid(attrdefoid) {
        return backend_utils_error::ereport(ERROR)
            .errmsg(format!(
                "could not find attrdef tuple for relation {} attnum {}",
                rel.rd_id, attnum
            ))
            .finish(here("ATExecDropExpression"))
            .map(|()| unreachable!());
    }
    backend_catalog_pg_depend_seams::deleteDependencyRecordsFor::call(
        types_catalog::pg_attrdef::AttrDefaultRelationId,
        attrdefoid,
        false,
    )?;

    // Make above changes visible.
    backend_access_transam_xact_seams::command_counter_increment::call()?;

    // Get rid of the GENERATED expression itself. RESTRICT for safety.
    RemoveAttrDefault(rel.rd_id, attnum, DROP_RESTRICT, false, false)?;

    Ok(object_address_subset(RelationRelationId, rel.rd_id, attnum as i32))
}

/// `ATExecDropOf(rel, lockmode)` (tablecmds.c:18358) — ALTER TABLE ... NOT OF.
/// Detaches a typed table from its composite type: drops the parent dependency
/// and clears `pg_class.reloftype`.
pub fn ATExecDropOf<'mcx>(
    _mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    _lockmode: LOCKMODE,
) -> PgResult<()> {
    let relid = rel.rd_id;
    // rel->rd_rel->reloftype (read via the syscache RELOID projection).
    let reloftype = backend_utils_cache_syscache_seams::search_relation_reloftype::call(relid)?
        .unwrap_or(types_core::InvalidOid);

    if !types_core::primitive::OidIsValid(reloftype) {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!("\"{}\" is not a typed table", rel.name()))
            .finish(here("ATExecDropOf"))
            .map(|()| unreachable!());
    }

    // We don't bother to check ownership of the type — ownership of the table
    // is presumed enough rights. No lock required on the type, either.
    //
    // drop_parent_dependency(relid, TypeRelationId, reloftype,
    //                        DEPENDENCY_NORMAL): remove the relation's NORMAL
    // dependency on its OF-type (depender = (pg_class, relid, 0)). The
    // documented general equivalent is deleteDependencyRecordsForSpecific.
    backend_catalog_pg_depend_seams::deleteDependencyRecordsForSpecific::call(
        RelationRelationId,
        relid,
        types_catalog::catalog_dependency::DEPENDENCY_NORMAL.as_char(),
        types_catalog::pg_type::TypeRelationId,
        reloftype,
    )?;

    // Clear pg_class.reloftype.
    let valid = indexing_seam::set_pg_class_reloftype::call(relid, types_core::InvalidOid)?;
    if !valid {
        return backend_utils_error::ereport(ERROR)
            .errmsg(format!("cache lookup failed for relation {relid}"))
            .finish(here("ATExecDropOf"))
            .map(|()| unreachable!());
    }

    // InvokeObjectPostAlterHook(RelationRelationId, relid, 0);
    objaccess_seam::invoke_object_post_alter_hook::call(RelationRelationId, relid, 0)?;

    Ok(())
}

/// `ATExecAddOf(rel, ofTypename, lockmode)` (tablecmds.c:18216) — ALTER TABLE
/// ... OF type. Validate the target composite type, require the table's column
/// layout to match the type's rowtype (name + type + typmod + collation, order
/// included; only attnotnull may differ), record a NORMAL dependency on the
/// type, and set `pg_class.reloftype`.
pub fn ATExecAddOf<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    of_typename: &types_nodes::rawnodes::TypeName<'mcx>,
    _lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    use types_catalog::catalog_dependency::DEPENDENCY_NORMAL;
    use types_catalog::pg_type::{TypeRelationId, TYPTYPE_COMPOSITE};
    use types_storage::lock::AccessShareLock;

    let relid = rel.rd_id;

    // typetuple = typenameType(NULL, ofTypename, NULL); typeid = GETSTRUCT->oid.
    let parse_type_name = backend_parser_parse_type::raw_typename_to_parse(of_typename)?;
    let (typeform, _typmod) =
        backend_parser_parse_type::typenameType(mcx, None, &parse_type_name)?;
    let typeid = typeform.oid;

    // check_of_type(typetuple): the target must be a stand-alone composite type
    // (typtype == 'c' and its typrelid relkind == 'c'). (tablecmds.c check_of_type)
    if typeform.typtype == TYPTYPE_COMPOSITE {
        debug_assert!(types_core::primitive::OidIsValid(typeform.typrelid));
        let type_relation = relation_open(mcx, typeform.typrelid, AccessShareLock)?;
        let type_ok = type_relation.rd_rel.relkind == b'c';
        // Close the parent rel, but keep the AccessShareLock until xact commit.
        backend_access_table_table::table_close(type_relation, NoLock)?;
        if !type_ok {
            return backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!(
                    "type {} is the row type of another table",
                    backend_utils_adt_format_type_seams::format_type_be::call(mcx, typeid)?
                        .as_str()
                ))
                .errdetail(
                    "A typed table must use a stand-alone composite type created with CREATE TYPE.",
                )
                .finish(here("ATExecAddOf"))
                .map(|_| unreachable!());
        }
    } else {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg(format!(
                "type {} is not a composite type",
                backend_utils_adt_format_type_seams::format_type_be::call(mcx, typeid)?.as_str()
            ))
            .finish(here("ATExecAddOf"))
            .map(|_| unreachable!());
    }

    // Fail if the table has any inheritance parents.
    if backend_catalog_pg_inherits_seams::has_superclass::call(relid)? {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg("typed tables cannot inherit")
            .finish(here("ATExecAddOf"))
            .map(|_| unreachable!());
    }

    // Check the tuple descriptors for compatibility. Unlike inheritance, we
    // require that the order also match. However, attnotnull need not match.
    let type_tupdesc =
        backend_utils_cache_typcache_seams::lookup_rowtype_tupdesc::call(mcx, typeid, -1)?;
    let table_tupdesc = &rel.rd_att;
    let table_natts = table_tupdesc.natts as usize;
    let type_natts = type_tupdesc.natts as usize;

    // table_attno is a 0-based cursor into the table descriptor.
    let mut table_attno: usize = 0;
    for type_attno in 0..type_natts {
        let type_attr = type_tupdesc.attr(type_attno);
        // Get the next non-dropped type attribute.
        if type_attr.attisdropped {
            continue;
        }
        let type_attname = core::str::from_utf8(type_attr.attname.name_str()).unwrap_or("");

        // Get the next non-dropped table attribute.
        let table_attr = loop {
            if table_attno >= table_natts {
                return backend_utils_error::ereport(ERROR)
                    .errcode(ERRCODE_DATATYPE_MISMATCH)
                    .errmsg(format!("table is missing column \"{type_attname}\""))
                    .finish(here("ATExecAddOf"))
                    .map(|_| unreachable!());
            }
            let attr = table_tupdesc.attr(table_attno);
            table_attno += 1;
            if !attr.attisdropped {
                break attr;
            }
        };
        let table_attname = core::str::from_utf8(table_attr.attname.name_str()).unwrap_or("");

        // Compare name.
        if table_attname != type_attname {
            return backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!(
                    "table has column \"{table_attname}\" where type requires \"{type_attname}\""
                ))
                .finish(here("ATExecAddOf"))
                .map(|_| unreachable!());
        }

        // Compare type.
        if table_attr.atttypid != type_attr.atttypid
            || table_attr.atttypmod != type_attr.atttypmod
            || table_attr.attcollation != type_attr.attcollation
        {
            return backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!(
                    "table \"{}\" has different type for column \"{type_attname}\"",
                    rel.name()
                ))
                .finish(here("ATExecAddOf"))
                .map(|_| unreachable!());
        }
    }

    // Any remaining columns at the end of the table had better be dropped.
    while table_attno < table_natts {
        let table_attr = table_tupdesc.attr(table_attno);
        table_attno += 1;
        if !table_attr.attisdropped {
            let attname = core::str::from_utf8(table_attr.attname.name_str()).unwrap_or("");
            return backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg(format!("table has extra column \"{attname}\""))
                .finish(here("ATExecAddOf"))
                .map(|_| unreachable!());
        }
    }
    // ReleaseTupleDesc(typeTupleDesc) — the owned copy drops here.
    drop(type_tupdesc);

    // If the table was already typed, drop the existing dependency.
    let cur_reloftype =
        backend_utils_cache_syscache_seams::search_relation_reloftype::call(relid)?
            .unwrap_or(types_core::InvalidOid);
    if types_core::primitive::OidIsValid(cur_reloftype) {
        // drop_parent_dependency(relid, TypeRelationId, reloftype, DEPENDENCY_NORMAL):
        // remove the relation's NORMAL dependency on its previous OF-type.
        backend_catalog_pg_depend_seams::deleteDependencyRecordsForSpecific::call(
            RelationRelationId,
            relid,
            DEPENDENCY_NORMAL.as_char(),
            TypeRelationId,
            cur_reloftype,
        )?;
    }

    // Record a dependency on the new type.
    let tableobj = ObjectAddress {
        classId: RelationRelationId,
        objectId: relid,
        objectSubId: 0,
    };
    let typeobj = ObjectAddress {
        classId: TypeRelationId,
        objectId: typeid,
        objectSubId: 0,
    };
    backend_catalog_pg_depend_seams::recordDependencyOn::call(
        mcx, &tableobj, &typeobj, DEPENDENCY_NORMAL,
    )?;

    // Update pg_class.reloftype.
    let valid = indexing_seam::set_pg_class_reloftype::call(relid, typeid)?;
    if !valid {
        return backend_utils_error::ereport(ERROR)
            .errmsg(format!("cache lookup failed for relation {relid}"))
            .finish(here("ATExecAddOf"))
            .map(|_| unreachable!());
    }

    // InvokeObjectPostAlterHook(RelationRelationId, relid, 0);
    objaccess_seam::invoke_object_post_alter_hook::call(RelationRelationId, relid, 0)?;

    Ok(typeobj)
}

// ---------------------------------------------------------------------------
// Helpers for reading Form_pg_attribute fields off a syscache tuple (GETSTRUCT
// reads).
// ---------------------------------------------------------------------------

use backend_access_common_heaptuple::FormedTuple;
use types_catalog::pg_attribute::{
    Anum_pg_attribute_attgenerated, Anum_pg_attribute_attnum, Anum_pg_attribute_atttypid,
};

/// `GETSTRUCT(tuple)->field` for a non-null `int2` `pg_attribute` column.
fn att_field_i16(mcx: Mcx<'_>, cache_id: i32, tup: &FormedTuple<'_>, anum: i16) -> PgResult<i16> {
    Ok(backend_utils_cache_syscache::SysCacheGetAttrNotNull(mcx, cache_id, tup, anum as i32)?.as_i16())
}

/// `GETSTRUCT(tuple)->field` for a non-null `oid` `pg_attribute` column.
fn att_field_oid(mcx: Mcx<'_>, cache_id: i32, tup: &FormedTuple<'_>, anum: i16) -> PgResult<types_core::Oid> {
    Ok(backend_utils_cache_syscache::SysCacheGetAttrNotNull(mcx, cache_id, tup, anum as i32)?.as_oid())
}

/// `GETSTRUCT(tuple)->field` for a non-null `char` `pg_attribute` column.
fn att_field_char(mcx: Mcx<'_>, cache_id: i32, tup: &FormedTuple<'_>, anum: i16) -> PgResult<i8> {
    Ok(backend_utils_cache_syscache::SysCacheGetAttrNotNull(mcx, cache_id, tup, anum as i32)?.as_char())
}

// ===========================================================================
// Unported column-option / relation-option / storage families
// (faithful keystone stop)
// ===========================================================================

/// `ATExecSetOptions(rel, colName, options, isReset, lockmode)`
/// (tablecmds.c:9050) — ALTER COLUMN SET / RESET (...) per-column attoptions.
///
/// Reads the current `pg_attribute.attoptions` `text[]`, merges the SET/RESET
/// `DefElem` list via `transformRelOptions`, validates with
/// `attribute_reloptions`, and writes the resulting `text[]` varlena image back
/// to `pg_attribute.attoptions` (or SQL NULL when the merge empties the array).
///
/// The merged `text[]` is built on the on-disk varlena-image lane
/// (`transformRelOptionsBytes` → `construct_text_array_bytes`) and ridden into
/// the catalog write through the `PgAttributeUpdateRow.attoptions` ByRef-bytes
/// carrier — the same path `ATExecSetRelOptions` uses for `pg_class.reloptions`
/// and `index_create` uses for index attoptions.
pub fn ATExecSetOptions<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    colName: &str,
    options: Option<&Node<'mcx>>,
    isReset: bool,
    _lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    // attrelation = table_open(AttributeRelationId, RowExclusiveLock);
    let attrelation = relation_open(mcx, AttributeRelationId, RowExclusiveLock)?;

    // tuple = SearchSysCacheAttName(RelationGetRelid(rel), colName);
    let tuple = match SearchSysCacheAttName(mcx, rel.rd_id, colName)? {
        Some(t) => t,
        None => {
            return backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_COLUMN)
                .errmsg(format!(
                    "column \"{}\" of relation \"{}\" does not exist",
                    colName,
                    rel.name()
                ))
                .finish(here("ATExecSetOptions"))
                .map(|()| unreachable!());
        }
    };

    // attnum = attrtuple->attnum;
    let attnum = att_field_i16(mcx, ATTNAME, &tuple, Anum_pg_attribute_attnum)?;
    if attnum <= 0 {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("cannot alter system column \"{colName}\""))
            .finish(here("ATExecSetOptions"))
            .map(|()| unreachable!());
    }

    // Project the `(List *) options` (a Node::List of Node::DefElem) into the
    // reloptions working-view DefElem list. C: `castNode(List, options)`.
    let mut def_list: Vec<backend_access_common_reloptions::DefElem> = Vec::new();
    if let Some(opts) = options {
        if let Some(items) = opts.as_list() {
            for it in items.iter() {
                if let Some(de) = it.as_defelem() {
                    let arg = crate::create::defel_arg(de)?;
                    def_list.push(backend_access_common_reloptions::DefElem::new(
                        de.defnamespace.as_ref().map(|s| s.as_str()),
                        de.defname.as_ref().map(|s| s.as_str()).unwrap_or(""),
                        arg,
                    ));
                }
            }
        }
    }

    // Generate new proposed attoptions (text array).
    //   datum = SysCacheGetAttr(ATTNAME, tuple, Anum_pg_attribute_attoptions, &isnull);
    //   newOptions = transformRelOptions(isnull ? (Datum) 0 : datum, options,
    //                                    NULL, NULL, false, isReset);
    let (cur_datum, cur_isnull) = backend_utils_cache_syscache::SysCacheGetAttr(
        mcx,
        ATTNAME,
        &tuple,
        types_catalog::pg_attribute::Anum_pg_attribute_attoptions as i32,
    )?;
    let old_bytes: Option<&[u8]> = if cur_isnull {
        None
    } else {
        Some(cur_datum.as_ref_bytes())
    };
    let new_options = backend_access_common_reloptions::transformRelOptionsBytes(
        mcx,
        old_bytes,
        &def_list,
        None,
        None,
        false,
        isReset,
    )?;
    let new_options: Option<Vec<u8>> = new_options.map(|v| v.iter().copied().collect());

    // Validate new options. (void) attribute_reloptions(newOptions, true);
    backend_access_common_reloptions::attribute_reloptions(mcx, new_options.as_deref(), true)?;

    // Build new tuple: replace attoptions only (the built varlena image, or SQL
    // NULL when the merge produced no options).
    let row = PgAttributeUpdateRow {
        attoptions: Some(new_options),
        ..Default::default()
    };
    indexing_seam::catalog_tuple_update_pg_attribute::call(mcx, &attrelation, &tuple, &row)?;

    // InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attnum);
    objaccess_seam::invoke_object_post_alter_hook::call(RelationRelationId, rel.rd_id, attnum as i32)?;

    // ObjectAddressSubSet(address, RelationRelationId, RelationGetRelid(rel), attnum);
    let address = object_address_subset(RelationRelationId, rel.rd_id, attnum as i32);

    // heap_freetuple(newtuple); table_close(attrelation, RowExclusiveLock) — RAII.
    drop(attrelation);

    Ok(address)
}

/// `ATExecSetStorage(rel, colName, newValue, lockmode)` (tablecmds.c:9192) —
/// ALTER COLUMN SET STORAGE. Sets `pg_attribute.attstorage` for the column,
/// then recurses to the column's index columns via
/// `SetIndexStorageProperties`.
pub fn ATExecSetStorage<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    colName: &str,
    newValue: Option<&Node<'mcx>>,
    lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    // strVal(newValue) — the storage-mode keyword carried by the String node.
    let storagemode = newValue
        .expect("ALTER COLUMN SET STORAGE requires a storage-mode value")
        .expect_string()
        .sval
        .as_str()
        .to_string();

    // attrelation = table_open(AttributeRelationId, RowExclusiveLock);
    let attrelation = relation_open(mcx, AttributeRelationId, RowExclusiveLock)?;

    // tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), colName);
    let tuple = match SearchSysCacheAttName(mcx, rel.rd_id, colName)? {
        Some(t) => t,
        None => {
            return backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_COLUMN)
                .errmsg(format!(
                    "column \"{}\" of relation \"{}\" does not exist",
                    colName,
                    rel.name()
                ))
                .finish(here("ATExecSetStorage"))
                .map(|()| unreachable!());
        }
    };

    // attrtuple = GETSTRUCT(tuple); attnum = attrtuple->attnum;
    let attnum = att_field_i16(mcx, ATTNAME, &tuple, Anum_pg_attribute_attnum)?;
    if attnum <= 0 {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("cannot alter system column \"{colName}\""))
            .finish(here("ATExecSetStorage"))
            .map(|()| unreachable!());
    }

    // attrtuple->attstorage = GetAttributeStorage(attrtuple->atttypid, strVal(newValue));
    let atttypid = att_field_oid(mcx, ATTNAME, &tuple, Anum_pg_attribute_atttypid)?;
    let newstorage = seam::get_attribute_storage::call(atttypid, &storagemode)?;

    // CatalogTupleUpdate(attrelation, &tuple->t_self, tuple);
    let row = PgAttributeUpdateRow {
        attstorage: Some(newstorage),
        ..Default::default()
    };
    indexing_seam::catalog_tuple_update_pg_attribute::call(mcx, &attrelation, &tuple, &row)?;

    // InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attrtuple->attnum);
    objaccess_seam::invoke_object_post_alter_hook::call(RelationRelationId, rel.rd_id, attnum as i32)?;

    // Apply the change to indexes as well (only for simple index columns,
    // matching behavior of index.c ConstructTupleDescriptor()).
    SetIndexStorageProperties(
        mcx,
        rel,
        &attrelation,
        attnum,
        true,
        newstorage,
        false,
        0,
        lockmode,
    )?;

    // heap_freetuple(tuple) / table_close(attrelation, RowExclusiveLock) — RAII.
    drop(attrelation);

    // ObjectAddressSubSet(address, RelationRelationId, RelationGetRelid(rel), attnum);
    Ok(object_address_subset(RelationRelationId, rel.rd_id, attnum as i32))
}

/// `ATExecAlterColumnGenericOptions(rel, colName, options, lockmode)`
/// (tablecmds.c:15955) — ALTER COLUMN OPTIONS (...) on a foreign-table column.
/// Determines the FDW validator for the foreign table's server, merges the
/// SET/ADD/DROP option actions into the column's current `attfdwoptions` (via
/// `transformGenericOptions`), and writes the resulting `text[]` back to
/// `pg_attribute.attfdwoptions`.
///
/// `options` is the `(List *) cmd->def` — a `Node::List` of `Node::DefElem`.
/// The foreign-domain pieces (the validator lookup and the option merge /
/// validator call) are delegated to the `backend-commands-foreigncmds` seams;
/// the `pg_attribute` read/write stays here.
pub fn ATExecAlterColumnGenericOptions<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    colName: &str,
    options: Option<&Node<'mcx>>,
    _lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    use backend_commands_foreigncmds_seams as fc_seam;

    // Collect the new options: (name, value, defaction) from the DefElem list.
    // if (options == NIL) return InvalidObjectAddress;
    let mut new_options: Vec<(String, Option<String>, i32)> = Vec::new();
    if let Some(def) = options {
        if let Some(items) = def.as_list() {
            for it in items.iter() {
                if let Some(de) = it.as_defelem() {
                    let name = de.defname.as_ref().map(|s| s.as_str().to_string()).ok_or_else(
                        || backend_utils_error::PgError::error("DefElem has no defname"),
                    )?;
                    // The option value: a quoted string literal (T_String). A
                    // value-less DROP carries `arg == NULL`.
                    let value = match de.arg.as_deref() {
                        None => None,
                        Some(node) => Some(node.expect_string().sval.as_str().to_string()),
                    };
                    let action = de.defaction as i32;
                    new_options.push((name, value, action));
                }
            }
        }
    }
    if new_options.is_empty() {
        // C: `if (options == NIL) return InvalidObjectAddress;`
        return Ok(ObjectAddress {
            classId: types_core::InvalidOid,
            objectId: types_core::InvalidOid,
            objectSubId: 0,
        });
    }

    // First, determine FDW validator associated to the foreign table.
    let fdwvalidator = fc_seam::foreign_table_fdwvalidator::call(rel.rd_id, &rel.name())?;

    // attrel = table_open(AttributeRelationId, RowExclusiveLock);
    let attrelation = relation_open(mcx, AttributeRelationId, RowExclusiveLock)?;

    // tuple = SearchSysCacheAttName(RelationGetRelid(rel), colName);
    let tuple = match SearchSysCacheAttName(mcx, rel.rd_id, colName)? {
        Some(t) => t,
        None => {
            return backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_COLUMN)
                .errmsg(format!(
                    "column \"{}\" of relation \"{}\" does not exist",
                    colName,
                    rel.name()
                ))
                .finish(here("ATExecAlterColumnGenericOptions"))
                .map(|()| unreachable!());
        }
    };

    // Prevent them from altering a system attribute.
    let attnum = att_field_i16(mcx, ATTNAME, &tuple, Anum_pg_attribute_attnum)?;
    if attnum <= 0 {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("cannot alter system column \"{colName}\""))
            .finish(here("ATExecAlterColumnGenericOptions"))
            .map(|()| unreachable!());
    }

    // Extract the current options: datum = SysCacheGetAttr(ATTNAME, tuple,
    // attfdwoptions, &isnull); options = isnull ? NIL : untransformRelOptions(datum);
    let (cur_datum, cur_isnull) = backend_utils_cache_syscache::SysCacheGetAttr(
        mcx,
        ATTNAME,
        &tuple,
        types_catalog::pg_attribute::Anum_pg_attribute_attfdwoptions as i32,
    )?;
    let old_options: Vec<(String, Option<String>)> = if cur_isnull {
        Vec::new()
    } else {
        backend_access_common_reloptions::untransformRelOptions(mcx, Some(cur_datum.as_ref_bytes()))?
    };

    // datum = transformGenericOptions(AttributeRelationId, datum, options,
    //                                 fdw->fdwvalidator);
    let merged = fc_seam::transform_generic_options::call(
        AttributeRelationId,
        &old_options,
        &new_options,
        fdwvalidator,
    )?;

    // if (PointerIsValid(DatumGetPointer(datum))) repl_val[..] = datum;
    // else repl_null[..] = true;  — an empty merged list is the SQL-NULL case.
    let pairs: Option<Vec<(String, String)>> = if merged.is_empty() {
        None
    } else {
        Some(
            merged
                .into_iter()
                .map(|(n, v)| (n, v.unwrap_or_default()))
                .collect(),
        )
    };
    let row = PgAttributeUpdateRow {
        attfdwoptions: Some(pairs),
        ..Default::default()
    };
    indexing_seam::catalog_tuple_update_pg_attribute::call(mcx, &attrelation, &tuple, &row)?;

    // InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attnum);
    objaccess_seam::invoke_object_post_alter_hook::call(RelationRelationId, rel.rd_id, attnum as i32)?;

    // table_close(attrel, RowExclusiveLock) — RAII.
    drop(attrelation);

    // ObjectAddressSubSet(address, RelationRelationId, RelationGetRelid(rel), attnum);
    Ok(object_address_subset(RelationRelationId, rel.rd_id, attnum as i32))
}

/// `ATExecGenericOptions(rel, options)` (tablecmds.c:18663) — ALTER FOREIGN
/// TABLE ... OPTIONS (...) on a foreign table. Determines the FDW validator for
/// the foreign table's server, merges the SET/ADD/DROP option actions into the
/// table's current `ftoptions` (via `transformGenericOptions`), writes the
/// resulting `text[]` back to `pg_foreign_table.ftoptions`, invalidates the
/// relcache, and fires the post-alter hook.
///
/// `options` is the `(List *) cmd->def` — a `Node::List` of `Node::DefElem`.
/// Returns void in C (`AT_GenericOptions` keeps the InvalidObjectAddress).
pub fn ATExecGenericOptions<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    options: Option<&Node<'mcx>>,
) -> PgResult<()> {
    use backend_commands_foreigncmds_seams as fc_seam;
    use backend_foreign_foreign_seams as ff_seam;

    // Collect the new options: (name, value, defaction). if (options == NIL) return;
    let mut new_options: Vec<(String, Option<String>, i32)> = Vec::new();
    if let Some(def) = options {
        if let Some(items) = def.as_list() {
            for it in items.iter() {
                if let Some(de) = it.as_defelem() {
                    let name = de
                        .defname
                        .as_ref()
                        .map(|s| s.as_str().to_string())
                        .ok_or_else(|| {
                            backend_utils_error::PgError::error("DefElem has no defname")
                        })?;
                    let value = match de.arg.as_deref() {
                        None => None,
                        Some(node) => Some(node.expect_string().sval.as_str().to_string()),
                    };
                    new_options.push((name, value, de.defaction as i32));
                }
            }
        }
    }
    if new_options.is_empty() {
        // C: `if (options == NIL) return;`
        return Ok(());
    }

    // ftrel = table_open(ForeignTableRelationId, RowExclusiveLock);
    // tuple = SearchSysCacheCopy1(FOREIGNTABLEREL, rel->rd_id);
    // server = GetForeignServer(tableform->ftserver);
    // fdw = GetForeignDataWrapper(server->fdwid);  → fdw->fdwvalidator
    // (a missing pg_foreign_table row → "foreign table does not exist").
    let fdwvalidator = fc_seam::foreign_table_fdwvalidator::call(rel.rd_id, &rel.name())?;

    // Extract the current options (datum = SysCacheGetAttr(ftoptions); isnull → NIL).
    let old_options_pairs = match ff_seam::foreign_table_options::call(rel.rd_id)? {
        Some(p) => p,
        None => {
            return backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("foreign table \"{}\" does not exist", rel.name()))
                .finish(here("ATExecGenericOptions"))
                .map(|()| ());
        }
    };
    let old_options: Vec<(String, Option<String>)> = old_options_pairs
        .into_iter()
        .map(|(n, v)| (n, Some(v)))
        .collect();

    // datum = transformGenericOptions(ForeignTableRelationId, datum, options,
    //                                 fdw->fdwvalidator);
    let merged = fc_seam::transform_generic_options::call(
        types_foreigncmds::ForeignTableRelationId,
        &old_options,
        &new_options,
        fdwvalidator,
    )?;

    // if (PointerIsValid(DatumGetPointer(datum))) repl_val[ftoptions] = datum;
    // else repl_null[ftoptions] = true;  — empty merged ⇒ SQL NULL.
    let pairs: Option<Vec<(String, String)>> = if merged.is_empty() {
        None
    } else {
        Some(
            merged
                .into_iter()
                .map(|(n, v)| (n, v.unwrap_or_default()))
                .collect(),
        )
    };

    // ftrel = table_open(ForeignTableRelationId, RowExclusiveLock);
    let ftrel = relation_open(
        mcx,
        types_foreigncmds::ForeignTableRelationId,
        RowExclusiveLock,
    )?;
    // tuple = heap_modify_tuple(...); CatalogTupleUpdate(ftrel, &tuple->t_self, tuple);
    let row = types_foreigncmds::PgForeignTableUpdateRow {
        options: Some(pairs),
    };
    indexing_seam::catalog_tuple_update_pg_foreign_table::call(&ftrel, rel.rd_id, &row)?;

    // CacheInvalidateRelcache(rel);
    backend_utils_cache_inval_seams::cache_invalidate_relcache::call(rel.rd_id)?;

    // InvokeObjectPostAlterHook(ForeignTableRelationId, RelationGetRelid(rel), 0);
    objaccess_seam::invoke_object_post_alter_hook::call(
        types_foreigncmds::ForeignTableRelationId,
        rel.rd_id,
        0,
    )?;

    // table_close(ftrel, RowExclusiveLock) — RAII.
    drop(ftrel);
    Ok(())
}

/// `ATExecSetCompression(rel, column, newValue, lockmode)` (tablecmds.c) —
/// ALTER COLUMN SET COMPRESSION. Resolves the named compression method via
/// `GetAttributeCompression`, writes `pg_attribute.attcompression`, then
/// recurses to the column's simple index columns via
/// `SetIndexStorageProperties`. No table rewrite — the change applies to newly
/// stored values only.
pub fn ATExecSetCompression<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    column: &str,
    newValue: Option<&Node<'mcx>>,
    lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    // compression = strVal(newValue);
    let compression = newValue
        .expect("ALTER COLUMN SET COMPRESSION requires a compression value")
        .expect_string()
        .sval
        .as_str()
        .to_string();

    // attrel = table_open(AttributeRelationId, RowExclusiveLock);
    let attrel = relation_open(mcx, AttributeRelationId, RowExclusiveLock)?;

    // tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), column);
    let tuple = match SearchSysCacheAttName(mcx, rel.rd_id, column)? {
        Some(t) => t,
        None => {
            return backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_COLUMN)
                .errmsg(format!(
                    "column \"{}\" of relation \"{}\" does not exist",
                    column,
                    rel.name()
                ))
                .finish(here("ATExecSetCompression"))
                .map(|()| unreachable!());
        }
    };

    // prevent them from altering a system attribute
    let attnum = att_field_i16(mcx, ATTNAME, &tuple, Anum_pg_attribute_attnum)?;
    if attnum <= 0 {
        return backend_utils_error::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("cannot alter system column \"{column}\""))
            .finish(here("ATExecSetCompression"))
            .map(|()| unreachable!());
    }

    // Check that column type is compressible, then get the attribute
    // compression method code.
    let atttypid = att_field_oid(mcx, ATTNAME, &tuple, Anum_pg_attribute_atttypid)?;
    let cmethod = seam::get_attribute_compression::call(atttypid, Some(compression.as_str()))?;

    // update pg_attribute entry: atttableform->attcompression = cmethod;
    // CatalogTupleUpdate(attrel, &tuple->t_self, tuple);
    let row = PgAttributeUpdateRow {
        attcompression: Some(cmethod),
        ..Default::default()
    };
    indexing_seam::catalog_tuple_update_pg_attribute::call(mcx, &attrel, &tuple, &row)?;

    // InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attnum);
    objaccess_seam::invoke_object_post_alter_hook::call(RelationRelationId, rel.rd_id, attnum as i32)?;

    // Apply the change to indexes as well (only for simple index columns,
    // matching behavior of index.c ConstructTupleDescriptor()).
    SetIndexStorageProperties(
        mcx,
        rel,
        &attrel,
        attnum,
        false,
        0,
        true,
        cmethod,
        lockmode,
    )?;

    // heap_freetuple(tuple) / table_close(attrel, RowExclusiveLock) — RAII.
    drop(attrel);

    // CommandCounterIncrement(); — make changes visible.
    backend_access_transam_xact_seams::command_counter_increment::call()?;

    // ObjectAddressSubSet(address, RelationRelationId, RelationGetRelid(rel), attnum);
    Ok(object_address_subset(RelationRelationId, rel.rd_id, attnum as i32))
}

/// `SetIndexStorageProperties(rel, attrelation, attnum, setstorage, newstorage,
/// setcompression, newcompression, lockmode)` (tablecmds.c:9098) — push a
/// storage/compression change from a table column down to every index column
/// whose `indkey` maps to it (only simple, non-expression index columns).
#[allow(clippy::too_many_arguments)]
fn SetIndexStorageProperties<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    attrelation: &Relation<'mcx>,
    attnum: AttrNumber,
    setstorage: bool,
    newstorage: i8,
    setcompression: bool,
    newcompression: i8,
    lockmode: LOCKMODE,
) -> PgResult<()> {
    // foreach(lc, RelationGetIndexList(rel))
    let index_list = backend_utils_cache_relcache_seams::relation_get_index_list::call(mcx, rel)?;
    for indexoid in index_list.iter().copied() {
        // indrel = index_open(indexoid, lockmode);
        let indrel = relation_open(mcx, indexoid, lockmode)?;

        // for (i = 0; i < indrel->rd_index->indnatts; i++)
        //     if (indrel->rd_index->indkey.values[i] == attnum) { indattnum = i+1; break; }
        let indkey = backend_utils_cache_relcache_seams::rd_index_indkey::call(&indrel)?
            .unwrap_or_default();
        let indnatts =
            backend_utils_cache_relcache_seams::rd_index_indnatts::call(&indrel)?.unwrap_or(0)
                as usize;
        let mut indattnum: AttrNumber = 0;
        for i in 0..indnatts {
            if indkey.get(i).copied().unwrap_or(0) == attnum {
                indattnum = (i + 1) as AttrNumber;
                break;
            }
        }

        if indattnum == 0 {
            // index_close(indrel, lockmode);
            drop(indrel);
            continue;
        }

        // tuple = SearchSysCacheCopyAttNum(RelationGetRelid(indrel), indattnum);
        let tuple =
            backend_utils_cache_syscache::SearchSysCacheAttNum(mcx, indrel.rd_id, indattnum)?;
        if let Some(tuple) = tuple {
            let attnum_idx = att_field_i16(mcx, ATTNUM, &tuple, Anum_pg_attribute_attnum)?;
            // if (setstorage) attrtuple->attstorage = newstorage;
            // if (setcompression) attrtuple->attcompression = newcompression;
            let row = PgAttributeUpdateRow {
                attstorage: if setstorage { Some(newstorage) } else { None },
                attcompression: if setcompression { Some(newcompression) } else { None },
                ..Default::default()
            };
            // CatalogTupleUpdate(attrelation, &tuple->t_self, tuple);
            indexing_seam::catalog_tuple_update_pg_attribute::call(mcx, attrelation, &tuple, &row)?;

            // InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attrtuple->attnum);
            objaccess_seam::invoke_object_post_alter_hook::call(
                RelationRelationId,
                rel.rd_id,
                attnum_idx as i32,
            )?;
            // heap_freetuple(tuple) — RAII drop.
        }

        // index_close(indrel, lockmode) — RAII drop.
        drop(indrel);
    }
    Ok(())
}

// ===========================================================================
// ATExecClusterOn (tablecmds.c) — ALTER TABLE CLUSTER ON <index>
// ATExecDropCluster (tablecmds.c) — ALTER TABLE SET WITHOUT CLUSTER
// ===========================================================================

/// `ATExecClusterOn(rel, indexName, lockmode)` (tablecmds.c). Marks the named
/// index as the clustered index of `rel` via `mark_index_clustered`.
pub fn ATExecClusterOn<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    indexName: &str,
    lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    // indexOid = get_relname_relid(indexName, rel->rd_rel->relnamespace);
    let index_oid = backend_utils_cache_lsyscache_seams::get_relname_relid::call(
        indexName,
        rel.rd_rel.relnamespace,
    )?;

    if index_oid == types_core::InvalidOid {
        return backend_utils_error::ereport(ERROR)
            .errcode(types_error::ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!(
                "index \"{}\" for table \"{}\" does not exist",
                indexName,
                rel.name()
            ))
            .finish(here("ATExecClusterOn"))
            .map(|()| unreachable!());
    }

    // Check index is valid to cluster on.
    backend_commands_cluster_seams::check_index_is_clusterable::call(mcx, rel, index_oid, lockmode)?;

    // And do the work.
    backend_commands_cluster_seams::mark_index_clustered::call(mcx, rel, index_oid, false)?;

    // ObjectAddressSet(address, RelationRelationId, indexOid);
    Ok(crate::helpers::object_address_set(RelationRelationId, index_oid))
}

/// `ATExecDropCluster(rel, lockmode)` (tablecmds.c). Clears the clustered-index
/// flag on all of `rel`'s indexes (`mark_index_clustered(rel, InvalidOid)`).
pub fn ATExecDropCluster<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    _lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    // mark_index_clustered(rel, InvalidOid, false);
    backend_commands_cluster_seams::mark_index_clustered::call(mcx, rel, types_core::InvalidOid, false)?;

    // C returns void / InvalidObjectAddress; the dispatch records no address.
    Ok(object_address_subset(types_core::InvalidOid, types_core::InvalidOid, 0))
}

/// `ATExecSetRowSecurity(rel, rls)` (tablecmds.c:18604) — ALTER TABLE
/// ENABLE/DISABLE ROW LEVEL SECURITY. Sets `pg_class.relrowsecurity`.
pub fn ATExecSetRowSecurity<'mcx>(rel: &Relation<'mcx>, rls: bool) -> PgResult<ObjectAddress> {
    let relid = rel.rd_id;
    // pg_class = table_open(RelationRelationId, RowExclusiveLock);
    // tuple = SearchSysCacheCopy1(RELOID, relid);
    // ((Form_pg_class) GETSTRUCT(tuple))->relrowsecurity = rls;
    // CatalogTupleUpdate(pg_class, &tuple->t_self, tuple);
    let valid = indexing_seam::set_pg_class_row_security::call(relid, Some(rls), None)?;
    if !valid {
        return backend_utils_error::ereport(ERROR)
            .errmsg(format!("cache lookup failed for relation {relid}"))
            .finish(here("ATExecSetRowSecurity"))
            .map(|()| unreachable!());
    }
    // InvokeObjectPostAlterHook(RelationRelationId, relid, 0): no-op.
    Ok(object_address_subset(types_core::InvalidOid, types_core::InvalidOid, 0))
}

/// `ATExecForceNoForceRowSecurity(rel, force_rls)` (tablecmds.c:18634) — ALTER
/// TABLE FORCE/NO FORCE ROW LEVEL SECURITY. Sets `pg_class.relforcerowsecurity`.
pub fn ATExecForceNoForceRowSecurity<'mcx>(
    rel: &Relation<'mcx>,
    force_rls: bool,
) -> PgResult<ObjectAddress> {
    let relid = rel.rd_id;
    let valid = indexing_seam::set_pg_class_row_security::call(relid, None, Some(force_rls))?;
    if !valid {
        return backend_utils_error::ereport(ERROR)
            .errmsg(format!("cache lookup failed for relation {relid}"))
            .finish(here("ATExecForceNoForceRowSecurity"))
            .map(|()| unreachable!());
    }
    Ok(object_address_subset(types_core::InvalidOid, types_core::InvalidOid, 0))
}

/// `ResetRelRewrite(myrelid)` (tablecmds.c:4363) — clear `pg_class.relrewrite`
/// (set to `InvalidOid`) on `myrelid` after a heap rewrite/swap. Installed as the
/// `reset_rel_rewrite` seam (consumed by cluster's `finish_heap_swap` for the
/// swapped relation and its toast table).
pub fn ResetRelRewrite(myrelid: types_core::Oid) -> PgResult<()> {
    // pg_class = table_open(RelationRelationId, RowExclusiveLock);
    // reltup = SearchSysCacheCopy1(RELOID, myrelid);
    // ((Form_pg_class) GETSTRUCT(reltup))->relrewrite = InvalidOid;
    // CatalogTupleUpdate(pg_class, &reltup->t_self, reltup);
    let valid =
        indexing_seam::set_pg_class_relrewrite::call(myrelid, types_core::InvalidOid)?;
    if !valid {
        return backend_utils_error::ereport(ERROR)
            .errmsg(format!("cache lookup failed for relation {myrelid}"))
            .finish(here("ResetRelRewrite"))
            .map(|()| unreachable!());
    }
    Ok(())
}

/// `HEAP_RELOPT_NAMESPACES` (access/reloptions.h) — `{ "toast", NULL }`.
const HEAP_RELOPT_NAMESPACES: &[&str] = &["toast"];

/// Validate `new_options` per the relation's `relkind` — the C `switch
/// (rel->rd_rel->relkind)` block of `ATExecSetRelOptions` (tablecmds.c:16694).
/// All run with `validate = true`; the parsed struct is discarded (C `(void)
/// ...`), only the `ereport(ERROR)` matters. `amhandler` is the index AM's
/// handler OID (the port's `index_reloptions` dispatch key), needed only for the
/// index relkinds.
fn validate_setrel_options(
    mcx: Mcx<'_>,
    relkind: u8,
    amhandler: types_core::Oid,
    new_options: Option<&[u8]>,
) -> PgResult<()> {
    if relkind == RELKIND_RELATION || relkind == RELKIND_MATVIEW {
        backend_access_common_reloptions::heap_reloptions(mcx, relkind, new_options, true)?;
    } else if relkind == RELKIND_PARTITIONED_TABLE {
        backend_access_common_reloptions::partitioned_table_reloptions(new_options, true)?;
    } else if relkind == RELKIND_VIEW {
        backend_access_common_reloptions::view_reloptions(mcx, new_options, true)?;
    } else if relkind == RELKIND_INDEX || relkind == RELKIND_PARTITIONED_INDEX {
        backend_access_common_reloptions::index_reloptions(mcx, amhandler, new_options, true)?;
    } else {
        // RELKIND_TOASTVALUE / default — shouldn't ever get here.
        return backend_utils_error::ereport(ERROR)
            .errcode(types_error::ERRCODE_WRONG_OBJECT_TYPE)
            .errmsg("cannot set options for this relation")
            .finish(here("ATExecSetRelOptions"))
            .map(|()| unreachable!());
    }
    Ok(())
}

/// `ATExecSetRelOptions` (tablecmds.c:16645). Generate the new proposed
/// `pg_class.reloptions` (`transformRelOptions` over the existing reloptions +
/// `defList`), validate per relkind, and write the variable reloptions column
/// via the `update_pg_class_reloptions` carrier (`heap_modify_tuple` +
/// `CatalogTupleUpdate`). Repeat the whole exercise for the TOAST table, if any.
pub fn ATExecSetRelOptions<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    def_list: Vec<backend_access_common_reloptions::DefElem>,
    operation: AlterTableType,
    lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    // if (defList == NIL && operation != AT_ReplaceRelOptions) return;
    if def_list.is_empty() && operation != AlterTableType::AT_ReplaceRelOptions {
        return Ok(object_address_subset(types_core::InvalidOid, types_core::InvalidOid, 0));
    }

    let relid = rel.rd_id;
    let relkind = rel.rd_rel.relkind;
    let is_reset = operation == AlterTableType::AT_ResetRelOptions;

    // amhandler dispatch key for the index relkind validation (C reads
    // rel->rd_indam->amoptions; the port keys on the handler OID).
    let amhandler = if relkind == RELKIND_INDEX || relkind == RELKIND_PARTITIONED_INDEX {
        backend_utils_cache_syscache_seams::search_am_handler::call(rel.rd_rel.relam)?
            .unwrap_or(types_core::InvalidOid)
    } else {
        types_core::InvalidOid
    };

    // Get the old reloptions (AT_ReplaceRelOptions pretends there were none).
    let old_bytes: Option<Vec<u8>> = if operation == AlterTableType::AT_ReplaceRelOptions {
        None
    } else {
        let tok = backend_utils_cache_syscache_seams::fetch_class_reloptions::call(mcx, relid)?;
        if tok.is_null {
            None
        } else {
            Some(tok.bytes)
        }
    };

    // Generate new proposed reloptions (text array). namspace = NULL,
    // validnsps = HEAP_RELOPT_NAMESPACES, acceptOidsOff = false.
    let new_options = backend_access_common_reloptions::transformRelOptionsBytes(
        mcx,
        old_bytes.as_deref(),
        &def_list,
        None,
        Some(HEAP_RELOPT_NAMESPACES),
        false,
        is_reset,
    )?;
    let new_options: Option<Vec<u8>> = new_options.map(|v| v.iter().copied().collect());

    // Validate per relkind.
    validate_setrel_options(mcx, relkind, amhandler, new_options.as_deref())?;

    // Update the pg_class row (the new options propagate via cache inval).
    indexing_seam::update_pg_class_reloptions::call(mcx, relid, new_options.as_deref())?;

    // InvokeObjectPostAlterHook(RelationRelationId, relid, 0): no-op.

    // Repeat the whole exercise for the toast table, if there's one.
    let toastid = rel.rd_rel.reltoastrelid;
    if types_core::OidIsValid(toastid) {
        // toastrel = table_open(toastid, lockmode); — take and hold `lockmode`
        // on the toast table until commit (C closes with NoLock). This is what
        // makes ALTER TABLE ... SET (toast.*) show the toast relation's lock in
        // pg_locks.
        let toastrel = relation_open(mcx, toastid, lockmode)?;

        let toast_old: Option<Vec<u8>> = if operation == AlterTableType::AT_ReplaceRelOptions {
            None
        } else {
            let tok = backend_utils_cache_syscache_seams::fetch_class_reloptions::call(mcx, toastid)?;
            if tok.is_null {
                None
            } else {
                Some(tok.bytes)
            }
        };
        // transformRelOptions(datum, defList, "toast", validnsps, false, isReset).
        let toast_new = backend_access_common_reloptions::transformRelOptionsBytes(
            mcx,
            toast_old.as_deref(),
            &def_list,
            Some("toast"),
            Some(HEAP_RELOPT_NAMESPACES),
            false,
            is_reset,
        )?;
        let toast_new: Option<Vec<u8>> = toast_new.map(|v| v.iter().copied().collect());

        // (void) heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);
        backend_access_common_reloptions::heap_reloptions(
            mcx,
            RELKIND_TOASTVALUE,
            toast_new.as_deref(),
            true,
        )?;

        indexing_seam::update_pg_class_reloptions::call(mcx, toastid, toast_new.as_deref())?;
        // InvokeObjectPostAlterHook(RelationRelationId, toastid, 0): no-op.

        // table_close(toastrel, NoLock) — keep the lock until commit.
        toastrel.close(NoLock)?;
    }

    Ok(object_address_subset(types_core::InvalidOid, types_core::InvalidOid, 0))
}
