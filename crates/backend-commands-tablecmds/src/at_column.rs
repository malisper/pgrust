//! `commands/tablecmds.c` — ALTER TABLE per-column executed families dispatched
//! from [`crate::at_phase::ATExecCmd`].
//!
//! PORTED here (faithful, 100% C logic):
//!   - `ATExecColumnDefault` (tablecmds.c:8126) — ALTER COLUMN SET / DROP DEFAULT
//!   - `ATExecCookedColumnDefault` (tablecmds.c:8210) — add a pre-cooked default
//!   - `ATExecSetStatistics` (tablecmds.c:8906) — ALTER COLUMN SET STATISTICS
//!   - `ATExecSetOptions` (tablecmds.c:9050) — ALTER COLUMN SET / RESET OPTIONS
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
//! SEAM-AND-PANIC (faithful, carrier-keystone blocked) — `ATExecSetStorage` and
//! the relation-level `ATExecSetRelOptions`:
//!   - `ATExecSetStorage` writes `attstorage` (expressible) *and then* recurses
//!     into index columns via `SetIndexStorageProperties`, which scans the full
//!     `indrel->rd_index->indkey.values[0..indnatts]`. The trimmed
//!     `types_rel::FormData_pg_index` carries only `indkey0` (the first key
//!     column), so the index recursion cannot be written faithfully; doing the
//!     main-table write and stopping would leave a partial mutation. Stays a
//!     loud stop until the `indkey` array is carried (out-of-lane carrier widen).
//!   - `ATExecSetRelOptions` writes the variable `reloptions` (`text[]`) column
//!     of `pg_class` via `heap_modify_tuple`. The only pg_class write carrier
//!     (`catalog_tuple_update_pg_class`) takes the fixed-length `PgClassForm`
//!     struct, which has no `reloptions` field — there is no pg_class
//!     variable-column write carrier. Stays a loud stop (out-of-lane carrier
//!     keystone).
//!
//! [`PgAttributeUpdateRow`]: types_catalog::pg_attribute::PgAttributeUpdateRow

#![allow(non_snake_case)]
#![allow(clippy::too_many_arguments)]

use mcx::Mcx;

use types_catalog::catalog_dependency::ObjectAddress;
use types_catalog::pg_attribute::{AttributeRelationId, PgAttributeUpdateRow};
use types_core::primitive::{AttrNumber, InvalidAttrNumber};
use types_error::{
    PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_COLUMN, ERROR, WARNING,
};
use types_nodes::ddlnodes::AlterTableType;
use types_nodes::nodes::Node;
use types_rel::Relation;
use types_storage::lock::{RowExclusiveLock, LOCKMODE};
use types_nodes::parsenodes::DROP_RESTRICT;
use types_statistics::MAX_STATISTICS_TARGET;
use types_tuple::access::{
    ATTRIBUTE_GENERATED_STORED, ATTRIBUTE_GENERATED_VIRTUAL, RELKIND_INDEX,
    RELKIND_PARTITIONED_INDEX,
};
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_access_common_relation::relation_open;
use backend_catalog_indexing_seams as indexing_seam;
use backend_catalog_pg_attrdef::{RemoveAttrDefault, StoreAttrDefault};
use backend_utils_cache_lsyscache::attribute::get_attnum;
use backend_utils_cache_syscache::{SearchSysCacheAttName, ATTNAME};

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

/// Faithful seam-and-panic for an unported column-attribute family. See module
/// docs for why these are not yet landed.
fn unported(what: &str) -> ! {
    panic!(
        "ALTER TABLE: {what} is not yet ported in backend-commands-tablecmds \
         (faithful seam-and-panic — needs the pg_attribute/pg_class \
         heap_deform_tuple + per-Anum Datum + heap_modify_tuple write path; \
         see at_column.rs)"
    );
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
            let ival = match node {
                Node::Integer(i) => i.ival,
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
                    att_name(mcx, cache_id, &tuple)?,
                    rel.name()
                ))
                .finish(here("ATExecSetStatistics"))
                .map(|()| unreachable!());
        }
        // C: `rel->rd_index->indkey.values[attnum - 1] != 0`. The trimmed
        // `FormData_pg_index` carries only `indkey0` (the first key column), so
        // for `attnum > 1` this read is not expressible. (The check fires
        // before any write, so the loud stop is partial-write-safe.)
        let indkey_val = if attnum == 1 {
            rd_index.indkey0
        } else {
            panic!(
                "ALTER INDEX ... ALTER COLUMN {attnum} SET STATISTICS: the trimmed \
                 types_rel::FormData_pg_index carries only indkey0 (first key column); \
                 indkey.values[attnum-1] for attnum>1 is not expressible \
                 (out-of-lane carrier widen — see at_column.rs)"
            );
        };
        if indkey_val != 0 {
            return backend_utils_error::ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!(
                    "cannot alter statistics on non-expression column \"{}\" of index \"{}\"",
                    att_name(mcx, cache_id, &tuple)?,
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

// ---------------------------------------------------------------------------
// Helpers for reading Form_pg_attribute fields off a syscache tuple (GETSTRUCT
// reads).
// ---------------------------------------------------------------------------

use backend_access_common_heaptuple::FormedTuple;
use types_catalog::pg_attribute::{Anum_pg_attribute_attgenerated, Anum_pg_attribute_attnum};

/// `GETSTRUCT(tuple)->field` for a non-null `int2` `pg_attribute` column.
fn att_field_i16(mcx: Mcx<'_>, cache_id: i32, tup: &FormedTuple<'_>, anum: i16) -> PgResult<i16> {
    Ok(backend_utils_cache_syscache::SysCacheGetAttrNotNull(mcx, cache_id, tup, anum as i32)?.as_i16())
}

/// `GETSTRUCT(tuple)->field` for a non-null `char` `pg_attribute` column.
fn att_field_char(mcx: Mcx<'_>, cache_id: i32, tup: &FormedTuple<'_>, anum: i16) -> PgResult<i8> {
    Ok(backend_utils_cache_syscache::SysCacheGetAttrNotNull(mcx, cache_id, tup, anum as i32)?.as_char())
}

/// `NameStr(attrtuple->attname)` from a syscache tuple (`attname`, Anum 1).
fn att_name(mcx: Mcx<'_>, cache_id: i32, tup: &FormedTuple<'_>) -> PgResult<String> {
    let datum = backend_utils_cache_syscache::SysCacheGetAttrNotNull(mcx, cache_id, tup, 1)?;
    match &datum {
        Datum::ByRef(b) => {
            // A `Name` is a fixed 64-byte NUL-padded image; read up to the NUL.
            let end = b.iter().position(|&c| c == 0).unwrap_or(b.len());
            Ok(String::from_utf8_lossy(&b[..end]).into_owned())
        }
        _ => Err(types_error::PgError::error(
            "att_name: attname attribute is by-value",
        )),
    }
}

// ===========================================================================
// Unported column-option / relation-option / storage families
// (faithful keystone stop)
// ===========================================================================

/// `ATExecSetOptions` (tablecmds.c:9050). See module docs.
pub fn ATExecSetOptions<'mcx>(
    _mcx: Mcx<'mcx>,
    _rel: &Relation<'mcx>,
    _colName: &str,
    _options: Option<&Node<'mcx>>,
    _isReset: bool,
    _lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    unported(
        "ALTER COLUMN SET/RESET OPTIONS — the attoptions write would store the \
         text[] image transformRelOptions builds, but transformRelOptions (and \
         construct_text_array) return a bare-word types_datum::Datum varlena \
         pointer with NO safe bridge to the types_tuple::Datum ByRef-bytes lane \
         the PgAttributeUpdateRow.attoptions carrier needs (same Datum-redesign \
         keystone backend-commands-indexcmds documents for opclass options)",
    );
}

/// `ATExecSetStorage` (tablecmds.c:9192). See module docs.
pub fn ATExecSetStorage<'mcx>(
    _mcx: Mcx<'mcx>,
    _rel: &Relation<'mcx>,
    _colName: &str,
    _newValue: Option<&Node<'mcx>>,
    _lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    unported(
        "ALTER COLUMN SET STORAGE — the attstorage write is expressible, but the \
         mandatory SetIndexStorageProperties index recursion scans \
         indrel->rd_index->indkey.values[0..indnatts] and the trimmed \
         types_rel::FormData_pg_index carries only indkey0 (out-of-lane carrier widen)",
    );
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

/// `ATExecSetRelOptions` (tablecmds.c:16645). See module docs.
pub fn ATExecSetRelOptions<'mcx>(
    _mcx: Mcx<'mcx>,
    _rel: &Relation<'mcx>,
    _def_list: Vec<backend_access_common_reloptions::DefElem>,
    _operation: AlterTableType,
    _lockmode: LOCKMODE,
) -> PgResult<ObjectAddress> {
    unported(
        "SET/RESET/REPLACE relOPTIONS — writes the variable pg_class.reloptions \
         (text[]) column via heap_modify_tuple, but the only pg_class write carrier \
         (catalog_tuple_update_pg_class) takes the fixed-length PgClassForm struct, \
         which has no reloptions field; no pg_class variable-column write carrier exists \
         (out-of-lane carrier keystone)",
    );
}
