//! Shared constants, type re-exports, and small helpers for the
//! `commands/tablecmds.c` F0 port.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use utils_error::ereport;
use types_catalog::catalog_dependency::ObjectAddress;
use types_core::primitive::Oid;
use types_error::{ErrorLocation, PgResult, ERRCODE_UNDEFINED_SCHEMA, ERROR, NOTICE};
use nodes::nodes::Node;
use nodes::rawnodes::RangeVar;
use types_tuple::access::RangeVar as AccessRangeVar;

use catalog_namespace::LookupNamespaceNoError;

/// `RelationRelationId` — `pg_class` OID.
pub const RelationRelationId: Oid = types_core::catalog::RELATION_RELATION_ID;
/// `NamespaceRelationId` — `pg_namespace` OID (catalog OID 2615).
pub const NamespaceRelationId: Oid = 2615;
/// `TableSpaceRelationId` — `pg_tablespace` OID (catalog OID 1213).
pub const TableSpaceRelationId: Oid = 1213;
/// `TypeRelationId` — `pg_type` OID (catalog OID 1247).
pub const TypeRelationId: Oid = 1247;
/// `LargeObjectRelationId` — `pg_largeobject` OID (catalog OID 2613).
pub const LargeObjectRelationId: Oid = 2613;
/// `GLOBALTABLESPACE_OID` — `pg_global` (OID 1664).
pub const GLOBALTABLESPACE_OID: Oid = 1664;

/// `PG_INT16_MAX`.
pub const PG_INT16_MAX: i32 = 32767;

/// `NAMEDATALEN` — the catalog name length limit.
pub const NAMEDATALEN: usize = 64;

/// `ErrorLocation` for `ereport(...).finish(...)` in this unit.
pub fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("src/backend/commands/tablecmds.c", 0, funcname)
}

/// `ObjectAddressSet(addr, class, object)` — sets `objectSubId = 0`.
pub fn object_address_set(class_id: Oid, object_id: Oid) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: 0,
    }
}

/// Bridge the owned grammar `RangeVar` node into the lifetime-free
/// `access::RangeVar` the namespace machinery consumes. (Copied from view.c.)
pub fn to_access_range_var(rv: &RangeVar<'_>) -> AccessRangeVar {
    AccessRangeVar {
        catalogname: rv.catalogname.as_ref().map(|s| s.as_str().to_string()),
        schemaname: rv.schemaname.as_ref().map(|s| s.as_str().to_string()),
        relname: rv
            .relname
            .as_ref()
            .map(|s| s.as_str().to_string())
            .unwrap_or_default(),
        inh: rv.inh,
        relpersistence: rv.relpersistence as u8,
        location: rv.location,
    }
}

/// `strlcpy(relname, name, NAMEDATALEN)` — truncate to `NAMEDATALEN - 1`
/// bytes. Defensive (the parser already did this).
pub fn strlcpy_namedatalen(name: &str) -> String {
    if name.len() < NAMEDATALEN {
        name.to_string()
    } else {
        name[..NAMEDATALEN - 1].to_string()
    }
}

/// Project a `List` value-node's `String` cells to a `NameList`
/// (`&[Option<String>]`) for `makeRangeVarFromNameList`. (Mirrors dropcmds.)
pub fn namelist_of_nodes(cells: &[Node]) -> Vec<Option<String>> {
    cells
        .iter()
        .map(|cell| match cell.as_string() {
            Some(s) => Some(s.sval.as_str().to_string()),
            None => unreachable!("Node::String expected in name list"),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// DropErrorMsgNonExistent / DropErrorMsgWrongType + the dropmsgstringarray.
// ---------------------------------------------------------------------------

/// One entry of the C `dropmsgstringarray` (tablecmds.c:255). The `%s`
/// placeholders are substituted via `.replace("%s", relname)` for exact text
/// fidelity.
struct DropMsgStrings {
    kind: u8,
    nonexistent_code: types_error::SqlState,
    nonexistent_msg: &'static str,
    skipping_msg: &'static str,
    nota_msg: &'static str,
    drophint_msg: &'static str,
}

use types_tuple::access::{
    RELKIND_COMPOSITE_TYPE, RELKIND_FOREIGN_TABLE, RELKIND_INDEX, RELKIND_MATVIEW,
    RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_SEQUENCE,
    RELKIND_VIEW,
};

use types_error::{ERRCODE_UNDEFINED_OBJECT, ERRCODE_UNDEFINED_TABLE};
const ERRCODE_UNDEFINED_TABLE_STR: types_error::SqlState = ERRCODE_UNDEFINED_TABLE;
const ERRCODE_UNDEFINED_OBJECT_STR: types_error::SqlState = ERRCODE_UNDEFINED_OBJECT;

/// The C `dropmsgstringarray` (tablecmds.c:255-310), 9 real entries.
const DROPMSGSTRINGARRAY: &[DropMsgStrings] = &[
    DropMsgStrings {
        kind: RELKIND_RELATION,
        nonexistent_code: ERRCODE_UNDEFINED_TABLE_STR,
        nonexistent_msg: "table \"%s\" does not exist",
        skipping_msg: "table \"%s\" does not exist, skipping",
        nota_msg: "\"%s\" is not a table",
        drophint_msg: "Use DROP TABLE to remove a table.",
    },
    DropMsgStrings {
        kind: RELKIND_SEQUENCE,
        nonexistent_code: ERRCODE_UNDEFINED_TABLE_STR,
        nonexistent_msg: "sequence \"%s\" does not exist",
        skipping_msg: "sequence \"%s\" does not exist, skipping",
        nota_msg: "\"%s\" is not a sequence",
        drophint_msg: "Use DROP SEQUENCE to remove a sequence.",
    },
    DropMsgStrings {
        kind: RELKIND_VIEW,
        nonexistent_code: ERRCODE_UNDEFINED_TABLE_STR,
        nonexistent_msg: "view \"%s\" does not exist",
        skipping_msg: "view \"%s\" does not exist, skipping",
        nota_msg: "\"%s\" is not a view",
        drophint_msg: "Use DROP VIEW to remove a view.",
    },
    DropMsgStrings {
        kind: RELKIND_MATVIEW,
        nonexistent_code: ERRCODE_UNDEFINED_TABLE_STR,
        nonexistent_msg: "materialized view \"%s\" does not exist",
        skipping_msg: "materialized view \"%s\" does not exist, skipping",
        nota_msg: "\"%s\" is not a materialized view",
        drophint_msg: "Use DROP MATERIALIZED VIEW to remove a materialized view.",
    },
    DropMsgStrings {
        kind: RELKIND_INDEX,
        nonexistent_code: ERRCODE_UNDEFINED_OBJECT_STR,
        nonexistent_msg: "index \"%s\" does not exist",
        skipping_msg: "index \"%s\" does not exist, skipping",
        nota_msg: "\"%s\" is not an index",
        drophint_msg: "Use DROP INDEX to remove an index.",
    },
    DropMsgStrings {
        kind: RELKIND_COMPOSITE_TYPE,
        nonexistent_code: ERRCODE_UNDEFINED_OBJECT_STR,
        nonexistent_msg: "type \"%s\" does not exist",
        skipping_msg: "type \"%s\" does not exist, skipping",
        nota_msg: "\"%s\" is not a type",
        drophint_msg: "Use DROP TYPE to remove a type.",
    },
    DropMsgStrings {
        kind: RELKIND_FOREIGN_TABLE,
        nonexistent_code: ERRCODE_UNDEFINED_OBJECT_STR,
        nonexistent_msg: "foreign table \"%s\" does not exist",
        skipping_msg: "foreign table \"%s\" does not exist, skipping",
        nota_msg: "\"%s\" is not a foreign table",
        drophint_msg: "Use DROP FOREIGN TABLE to remove a foreign table.",
    },
    DropMsgStrings {
        kind: RELKIND_PARTITIONED_TABLE,
        nonexistent_code: ERRCODE_UNDEFINED_TABLE_STR,
        nonexistent_msg: "table \"%s\" does not exist",
        skipping_msg: "table \"%s\" does not exist, skipping",
        nota_msg: "\"%s\" is not a table",
        drophint_msg: "Use DROP TABLE to remove a table.",
    },
    DropMsgStrings {
        kind: RELKIND_PARTITIONED_INDEX,
        nonexistent_code: ERRCODE_UNDEFINED_OBJECT_STR,
        nonexistent_msg: "index \"%s\" does not exist",
        skipping_msg: "index \"%s\" does not exist, skipping",
        nota_msg: "\"%s\" is not an index",
        drophint_msg: "Use DROP INDEX to remove an index.",
    },
];

/// `DropErrorMsgNonExistent(rel, rightkind, missing_ok)` (tablecmds.c:1462).
pub fn DropErrorMsgNonExistent(
    rel: &AccessRangeVar,
    rightkind: u8,
    missing_ok: bool,
) -> PgResult<()> {
    if let Some(schemaname) = rel.schemaname.as_deref() {
        if !types_core::primitive::OidIsValid(LookupNamespaceNoError(schemaname)?) {
            if !missing_ok {
                return ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_SCHEMA)
                    .errmsg(format!("schema \"{schemaname}\" does not exist"))
                    .finish(here("DropErrorMsgNonExistent"));
            } else {
                ereport(NOTICE)
                    .errmsg(format!("schema \"{schemaname}\" does not exist, skipping"))
                    .finish(here("DropErrorMsgNonExistent"))?;
            }
            return Ok(());
        }
    }

    let relname = rel.relname.as_str();
    for rentry in DROPMSGSTRINGARRAY {
        if rentry.kind == rightkind {
            if !missing_ok {
                return ereport(ERROR)
                    .errcode(rentry.nonexistent_code)
                    .errmsg(rentry.nonexistent_msg.replace("%s", relname))
                    .finish(here("DropErrorMsgNonExistent"));
            } else {
                ereport(NOTICE)
                    .errmsg(rentry.skipping_msg.replace("%s", relname))
                    .finish(here("DropErrorMsgNonExistent"))?;
                break;
            }
        }
    }
    Ok(())
}

/// `DropErrorMsgWrongType(relname, wrongkind, rightkind)` (tablecmds.c:1511).
pub fn DropErrorMsgWrongType(relname: &str, wrongkind: u8, rightkind: u8) -> PgResult<()> {
    let rentry = DROPMSGSTRINGARRAY
        .iter()
        .find(|e| e.kind == rightkind)
        .expect("rightkind must be in dropmsgstringarray");
    let wentry = DROPMSGSTRINGARRAY.iter().find(|e| e.kind == wrongkind);

    let mut builder = ereport(ERROR)
        .errcode(types_error::ERRCODE_WRONG_OBJECT_TYPE)
        .errmsg(rentry.nota_msg.replace("%s", relname));
    if let Some(wentry) = wentry {
        builder = builder.errhint(wentry.drophint_msg);
    }
    builder.finish(here("DropErrorMsgWrongType"))
}
