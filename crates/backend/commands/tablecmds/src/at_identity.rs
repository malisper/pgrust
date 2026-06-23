//! `ALTER TABLE ... ALTER COLUMN { ADD | SET | DROP } IDENTITY` execution
//! handlers (tablecmds.c:8240/8371/8488).
//!
//! The phase-2 prep (`ATPrepCmd`) and the parse-transform that gins up the
//! implicit sequence (`generateSerialExtraStmts` via `transformAlterTableStmt`,
//! reached through `ATParseTransformCmd`) live elsewhere; this module ports the
//! three `ATExec*Identity` functions that flip `pg_attribute.attidentity` and,
//! for DROP, tear down the owned sequence.

use types_catalog::catalog_dependency::ObjectAddress;
use types_catalog::pg_attribute::{
    AttributeRelationId, Anum_pg_attribute_atthasdef, Anum_pg_attribute_attidentity,
    Anum_pg_attribute_attnotnull, Anum_pg_attribute_attnum, PgAttributeUpdateRow,
};
use types_core::primitive::{AttrNumber, InvalidOid, Oid};
use types_error::{
    PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_TABLE_DEFINITION,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_COLUMN,
    ERROR, NOTICE,
};
use mcx::Mcx;
use ::nodes::nodes::Node;
use ::nodes::parsenodes::DROP_RESTRICT;
use rel::Relation;
use types_storage::lock::{NoLock, RowExclusiveLock, LOCKMODE};

use common_relation::relation_open;
use transam_xact::CommandCounterIncrement;
use indexing_seams as indexing_seam;
use pg_inherits::find_inheritance_children;
use cache_syscache::{SearchSysCacheCopyAttName, SysCacheGetAttrNotNull, ATTNAME};

use crate::helpers::{here, RelationRelationId};

const RELKIND_PARTITIONED_TABLE: u8 = b'p';

/// `PERFORM_DELETION_INTERNAL` (dependency.h).
const PERFORM_DELETION_INTERNAL: i32 = 0x0001;

/// `DEPENDENCY_INTERNAL` (`dependency.h`) deptype byte 'i'.
const DEPENDENCY_INTERNAL: i8 = b'i' as i8;

/// `InvalidObjectAddress` (objectaddress.c).
fn invalid_object_address() -> ObjectAddress {
    ObjectAddress {
        classId: InvalidOid,
        objectId: InvalidOid,
        objectSubId: 0,
    }
}

/// `ObjectAddressSubSet(address, RelationRelationId, relid, attnum)`.
fn rel_sub_address(relid: Oid, attnum: AttrNumber) -> ObjectAddress {
    ObjectAddress {
        classId: RelationRelationId,
        objectId: relid,
        objectSubId: attnum as i32,
    }
}

/// `errmsg("column \"%s\" of relation \"%s\" does not exist", ...)`.
fn undefined_column_error(rel: &Relation<'_>, colname: &str) -> types_error::PgError {
    utils_error::ereport(ERROR)
        .errcode(ERRCODE_UNDEFINED_COLUMN)
        .errmsg(format!(
            "column \"{}\" of relation \"{}\" does not exist",
            colname,
            rel.name()
        ))
        .into_error()
}

/// `ALTER TABLE ALTER COLUMN ADD IDENTITY` (tablecmds.c:8240).
///
/// Returns the address of the affected column.
pub fn ATExecAddIdentity<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    colname: &str,
    def: &Node<'mcx>,
    lockmode: LOCKMODE,
    recurse: bool,
    recursing: bool,
) -> PgResult<ObjectAddress> {
    // ColumnDef *cdef = castNode(ColumnDef, def);
    let cdef = def.expect_columndef();

    let ispartitioned = rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE;
    if ispartitioned && !recurse {
        return Err(utils_error::ereport(ERROR)
            .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
            .errmsg("cannot add identity to a column of only the partitioned table".to_string())
            .errhint("Do not specify the ONLY keyword.".to_string())
            .into_error());
    }

    if rel.rd_rel.relispartition && !recursing {
        return Err(utils_error::ereport(ERROR)
            .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
            .errmsg("cannot add identity to a column of a partition".to_string())
            .into_error());
    }

    // attrelation = table_open(AttributeRelationId, RowExclusiveLock);
    let attrelation = relation_open(mcx, AttributeRelationId, RowExclusiveLock)?;

    // tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), colName);
    let tuple = SearchSysCacheCopyAttName(mcx, rel.rd_id, colname)?
        .ok_or_else(|| undefined_column_error(rel, colname))?;

    let attnum =
        SysCacheGetAttrNotNull(mcx, ATTNAME, &tuple, Anum_pg_attribute_attnum as i32)?.as_i16();
    let attnotnull =
        SysCacheGetAttrNotNull(mcx, ATTNAME, &tuple, Anum_pg_attribute_attnotnull as i32)?
            .as_bool();
    let attidentity =
        SysCacheGetAttrNotNull(mcx, ATTNAME, &tuple, Anum_pg_attribute_attidentity as i32)?
            .as_char();
    let atthasdef =
        SysCacheGetAttrNotNull(mcx, ATTNAME, &tuple, Anum_pg_attribute_atthasdef as i32)?.as_bool();

    // Can't alter a system attribute.
    if attnum <= 0 {
        return Err(utils_error::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("cannot alter system column \"{colname}\""))
            .into_error());
    }

    // Creating a column as identity implies NOT NULL, so adding the identity to
    // an existing column that is not NOT NULL would create a state that cannot
    // be reproduced without contortions.
    if !attnotnull {
        return Err(utils_error::ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "column \"{}\" of relation \"{}\" must be declared NOT NULL before identity can be added",
                colname,
                rel.name()
            ))
            .into_error());
    }

    // If a not-null constraint exists, verify that it's compatible.
    if attnotnull {
        let contup = pg_constraint::findNotNullConstraintAttnum(
            mcx, rel.rd_id, attnum,
        )?
        .ok_or_else(|| {
            utils_error::ereport(ERROR)
                .errmsg_internal(format!(
                    "cache lookup failed for not-null constraint on column \"{}\" of relation \"{}\"",
                    colname,
                    rel.name()
                ))
                .into_error()
        })?;

        let con_form =
            syscache_seams::read_constraint_form::call(&contup)?;
        if !con_form.convalidated {
            return Err(utils_error::ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "incompatible NOT VALID constraint \"{}\" on relation \"{}\"",
                    con_form.conname_str(),
                    rel.name()
                ))
                .errhint(
                    "You might need to validate it using ALTER TABLE ... VALIDATE CONSTRAINT."
                        .to_string(),
                )
                .into_error());
        }
    }

    if attidentity != 0 {
        return Err(utils_error::ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "column \"{}\" of relation \"{}\" is already an identity column",
                colname,
                rel.name()
            ))
            .into_error());
    }

    if atthasdef {
        return Err(utils_error::ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "column \"{}\" of relation \"{}\" already has a default value",
                colname,
                rel.name()
            ))
            .into_error());
    }

    // attTup->attidentity = cdef->identity;
    // CatalogTupleUpdate(attrelation, &tuple->t_self, tuple);
    let row = PgAttributeUpdateRow {
        attidentity: Some(cdef.identity),
        ..Default::default()
    };
    indexing_seam::catalog_tuple_update_pg_attribute::call(mcx, &attrelation, &tuple, &row)?;

    // InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attnum);
    objectaccess_seams::invoke_object_post_alter_hook::call(
        RelationRelationId,
        rel.rd_id,
        attnum as i32,
    )?;
    let address = rel_sub_address(rel.rd_id, attnum);

    // table_close(attrelation, RowExclusiveLock): RAII drop.
    drop(attrelation);

    // Recurse to propagate the identity column to partitions.  Identity is not
    // inherited in regular inheritance children.
    if recurse && ispartitioned {
        let children = find_inheritance_children(mcx, rel.rd_id, lockmode)?;
        for childoid in children.iter() {
            let childrel = relation_open(mcx, *childoid, NoLock)?;
            ATExecAddIdentity(mcx, &childrel, colname, def, lockmode, recurse, true)?;
            childrel.close(NoLock)?;
        }
    }

    Ok(address)
}

/// `ALTER TABLE ALTER COLUMN SET { GENERATED or sequence options }`
/// (tablecmds.c:8371).
///
/// Returns the address of the affected column.
pub fn ATExecSetIdentity<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    colname: &str,
    def: &Node<'mcx>,
    lockmode: LOCKMODE,
    recurse: bool,
    recursing: bool,
) -> PgResult<ObjectAddress> {
    let ispartitioned = rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE;
    if ispartitioned && !recurse {
        return Err(utils_error::ereport(ERROR)
            .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
            .errmsg("cannot change identity column of only the partitioned table".to_string())
            .errhint("Do not specify the ONLY keyword.".to_string())
            .into_error());
    }

    if rel.rd_rel.relispartition && !recursing {
        return Err(utils_error::ereport(ERROR)
            .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
            .errmsg("cannot change identity column of a partition".to_string())
            .into_error());
    }

    // foreach(option, castNode(List, def)) { ... } — find the lone "generated"
    // DefElem (parse_utilcmd already split the sequence options out).
    let options = def
        .as_list()
        .ok_or_else(|| {
            utils_error::ereport(ERROR)
                .errmsg_internal("ATExecSetIdentity: def is not a List".to_string())
                .into_error()
        })?;
    let mut generated_value: Option<i32> = None;
    for opt in options.iter() {
        let defel = match opt.as_defelem() {
            Some(d) => d,
            None => {
                return Err(utils_error::ereport(ERROR)
                    .errmsg_internal("ATExecSetIdentity: option is not a DefElem".to_string())
                    .into_error())
            }
        };
        let defname = defel.defname.as_deref().unwrap_or("");
        if defname == "generated" {
            if generated_value.is_some() {
                return Err(utils_error::ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg("conflicting or redundant options".to_string())
                    .into_error());
            }
            // defGetInt32(generatedEl): the value node is an Integer holding the
            // ATTRIBUTE_IDENTITY_* char (define.c:148).
            let arg = defel.arg.as_deref().ok_or_else(|| {
                utils_error::ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!("{defname} requires an integer value"))
                    .into_error()
            })?;
            let ival = arg.as_integer().map(|i| i.ival).ok_or_else(|| {
                utils_error::ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!("{defname} requires an integer value"))
                    .into_error()
            })?;
            generated_value = Some(ival);
        } else {
            return Err(utils_error::ereport(ERROR)
                .errmsg_internal(format!("option \"{defname}\" not recognized"))
                .into_error());
        }
    }

    // Even if there is nothing to change here, we run all the checks.  There
    // will be a subsequent ALTER SEQUENCE that relies on everything being there.
    let attrelation = relation_open(mcx, AttributeRelationId, RowExclusiveLock)?;
    let tuple = SearchSysCacheCopyAttName(mcx, rel.rd_id, colname)?
        .ok_or_else(|| undefined_column_error(rel, colname))?;

    let attnum =
        SysCacheGetAttrNotNull(mcx, ATTNAME, &tuple, Anum_pg_attribute_attnum as i32)?.as_i16();
    let attidentity =
        SysCacheGetAttrNotNull(mcx, ATTNAME, &tuple, Anum_pg_attribute_attidentity as i32)?
            .as_char();

    if attnum <= 0 {
        return Err(utils_error::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("cannot alter system column \"{colname}\""))
            .into_error());
    }

    if attidentity == 0 {
        return Err(utils_error::ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!(
                "column \"{}\" of relation \"{}\" is not an identity column",
                colname,
                rel.name()
            ))
            .into_error());
    }

    let address = if let Some(generated) = generated_value {
        // attTup->attidentity = defGetInt32(generatedEl);
        // CatalogTupleUpdate(attrelation, &tuple->t_self, tuple);
        let row = PgAttributeUpdateRow {
            attidentity: Some(generated as i8),
            ..Default::default()
        };
        indexing_seam::catalog_tuple_update_pg_attribute::call(mcx, &attrelation, &tuple, &row)?;

        objectaccess_seams::invoke_object_post_alter_hook::call(
            RelationRelationId,
            rel.rd_id,
            attnum as i32,
        )?;
        rel_sub_address(rel.rd_id, attnum)
    } else {
        invalid_object_address()
    };

    drop(attrelation);

    // Recurse to propagate the identity change to partitions. Identity is not
    // inherited in regular inheritance children.
    if generated_value.is_some() && recurse && ispartitioned {
        let children = find_inheritance_children(mcx, rel.rd_id, lockmode)?;
        for childoid in children.iter() {
            let childrel = relation_open(mcx, *childoid, NoLock)?;
            ATExecSetIdentity(mcx, &childrel, colname, def, lockmode, recurse, true)?;
            childrel.close(NoLock)?;
        }
    }

    Ok(address)
}

/// `ALTER TABLE ALTER COLUMN DROP IDENTITY` (tablecmds.c:8488).
///
/// Returns the address of the affected column.
pub fn ATExecDropIdentity<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    colname: &str,
    missing_ok: bool,
    lockmode: LOCKMODE,
    recurse: bool,
    recursing: bool,
) -> PgResult<ObjectAddress> {
    let ispartitioned = rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE;
    if ispartitioned && !recurse {
        return Err(utils_error::ereport(ERROR)
            .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
            .errmsg("cannot drop identity from a column of only the partitioned table".to_string())
            .errhint("Do not specify the ONLY keyword.".to_string())
            .into_error());
    }

    if rel.rd_rel.relispartition && !recursing {
        return Err(utils_error::ereport(ERROR)
            .errcode(ERRCODE_INVALID_TABLE_DEFINITION)
            .errmsg("cannot drop identity from a column of a partition".to_string())
            .into_error());
    }

    let attrelation = relation_open(mcx, AttributeRelationId, RowExclusiveLock)?;
    let tuple = SearchSysCacheCopyAttName(mcx, rel.rd_id, colname)?
        .ok_or_else(|| undefined_column_error(rel, colname))?;

    let attnum =
        SysCacheGetAttrNotNull(mcx, ATTNAME, &tuple, Anum_pg_attribute_attnum as i32)?.as_i16();
    let attidentity =
        SysCacheGetAttrNotNull(mcx, ATTNAME, &tuple, Anum_pg_attribute_attidentity as i32)?
            .as_char();

    if attnum <= 0 {
        return Err(utils_error::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!("cannot alter system column \"{colname}\""))
            .into_error());
    }

    if attidentity == 0 {
        if !missing_ok {
            return Err(utils_error::ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "column \"{}\" of relation \"{}\" is not an identity column",
                    colname,
                    rel.name()
                ))
                .into_error());
        } else {
            utils_error::ereport(NOTICE)
                .errmsg(format!(
                    "column \"{}\" of relation \"{}\" is not an identity column, skipping",
                    colname,
                    rel.name()
                ))
                .finish(here("ATExecDropIdentity"))?;
            // heap_freetuple + table_close: RAII drop.
            drop(attrelation);
            return Ok(invalid_object_address());
        }
    }

    // attTup->attidentity = '\0';
    // CatalogTupleUpdate(attrelation, &tuple->t_self, tuple);
    let row = PgAttributeUpdateRow {
        attidentity: Some(0),
        ..Default::default()
    };
    indexing_seam::catalog_tuple_update_pg_attribute::call(mcx, &attrelation, &tuple, &row)?;

    objectaccess_seams::invoke_object_post_alter_hook::call(
        RelationRelationId,
        rel.rd_id,
        attnum as i32,
    )?;
    let address = rel_sub_address(rel.rd_id, attnum);

    drop(attrelation);

    // Recurse to drop the identity from column in partitions.  Identity is not
    // inherited in regular inheritance children so ignore them.
    if recurse && ispartitioned {
        let children = find_inheritance_children(mcx, rel.rd_id, lockmode)?;
        for childoid in children.iter() {
            let childrel = relation_open(mcx, *childoid, NoLock)?;
            ATExecDropIdentity(mcx, &childrel, colname, false, lockmode, recurse, true)?;
            childrel.close(NoLock)?;
        }
    }

    if !recursing {
        // Drop the internal sequence.
        let seqid = pg_depend_seams::getIdentitySequence::call(
            mcx, rel, attnum, false,
        )?;
        pg_depend_seams::deleteDependencyRecordsForClass::call(
            RelationRelationId,
            seqid,
            RelationRelationId,
            DEPENDENCY_INTERNAL,
        )?;
        CommandCounterIncrement()?;
        dependency_seams::perform_deletion::call(
            RelationRelationId,
            seqid,
            0,
            DROP_RESTRICT,
            PERFORM_DELETION_INTERNAL,
        )?;
    }

    Ok(address)
}
