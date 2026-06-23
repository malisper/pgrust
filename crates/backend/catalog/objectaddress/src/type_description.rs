//! F2 — `getObjectTypeDescription` and the relation/constraint/procedure
//! type-disambiguation helpers (objectaddress.c 4497-4823).
//!
//! Builds on the F0 `ObjectProperty[]` tables and
//! [`crate::resolve::get_catalog_object_by_oid`]. The per-class "type" strings
//! mirror the C `appendStringInfoString` arms verbatim; the catalog reads go
//! through the owning units' `-seams` crates (`rel_relkind` / `pg_proc_form`
//! project `Form_pg_class.relkind` / `Form_pg_proc.prokind` via the
//! `RELOID` / `PROCOID` syscaches, `constraint_type_oids` projects
//! `Form_pg_constraint.{conrelid,contypid,oid}` via the F0
//! `get_catalog_object_by_oid` scan).

// The catalog-relation OID constants keep their C `*RelationId` names; used as
// `match` patterns here (cf. `consts.rs`'s `#![allow(non_upper_case_globals)]`).
#![allow(non_upper_case_globals)]

use mcx::{Mcx, PgString};
use types_core::{InvalidOid, Oid};
use types_catalog::catalog_dependency::ObjectAddress;
use types_error::{PgError, PgResult};

use crate::consts::{
    AccessMethodOperatorRelationId, AccessMethodProcedureRelationId, AccessMethodRelationId,
    AttrDefaultRelationId, AuthIdRelationId, AuthMemRelationId, CastRelationId,
    CollationRelationId, ConstraintRelationId, ConversionRelationId, DatabaseRelationId,
    DefaultAclRelationId, EventTriggerRelationId, ExtensionRelationId,
    ForeignDataWrapperRelationId, ForeignServerRelationId, LanguageRelationId,
    LargeObjectRelationId, NamespaceRelationId, OperatorClassRelationId,
    OperatorFamilyRelationId, OperatorRelationId, ParameterAclRelationId, PolicyRelationId,
    ProcedureRelationId, PublicationNamespaceRelationId, PublicationRelRelationId,
    PublicationRelationId, RelationRelationId, RewriteRelationId, StatisticExtRelationId,
    SubscriptionRelationId, TSConfigRelationId, TSDictionaryRelationId, TSParserRelationId,
    TSTemplateRelationId, TableSpaceRelationId, TransformRelationId, TriggerRelationId,
    TypeRelationId, UserMappingRelationId, PROKIND_AGGREGATE, PROKIND_PROCEDURE,
};

/// pg_class relkind chars (`pg_class.h`).
const RELKIND_RELATION: u8 = b'r';
const RELKIND_INDEX: u8 = b'i';
const RELKIND_SEQUENCE: u8 = b'S';
const RELKIND_TOASTVALUE: u8 = b't';
const RELKIND_VIEW: u8 = b'v';
const RELKIND_MATVIEW: u8 = b'm';
const RELKIND_COMPOSITE_TYPE: u8 = b'c';
const RELKIND_FOREIGN_TABLE: u8 = b'f';
const RELKIND_PARTITIONED_TABLE: u8 = b'p';
const RELKIND_PARTITIONED_INDEX: u8 = b'I';

/// `getObjectTypeDescription(const ObjectAddress *object, bool missing_ok)`
/// (objectaddress.c 4497): the catalog-class "type" string (e.g. "table",
/// "view column", "operator of access method"). `Ok(None)` mirrors the C NULL
/// for a vanished object under `missing_ok`.
///
/// Keep `ObjectTypeMap` ([`crate::tables`]) in sync with this.
pub fn get_object_type_description<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
    missing_ok: bool,
) -> PgResult<Option<PgString<'mcx>>> {
    let mut buffer = String::new();

    match object.classId {
        RelationRelationId => {
            get_relation_type_description(
                mcx,
                &mut buffer,
                object.objectId,
                object.objectSubId,
                missing_ok,
            )?;
        }
        ProcedureRelationId => {
            get_procedure_type_description(mcx, &mut buffer, object.objectId, missing_ok)?;
        }
        TypeRelationId => buffer.push_str("type"),
        CastRelationId => buffer.push_str("cast"),
        CollationRelationId => buffer.push_str("collation"),
        ConstraintRelationId => {
            get_constraint_type_description(mcx, &mut buffer, object.objectId, missing_ok)?;
        }
        ConversionRelationId => buffer.push_str("conversion"),
        AttrDefaultRelationId => buffer.push_str("default value"),
        LanguageRelationId => buffer.push_str("language"),
        LargeObjectRelationId => buffer.push_str("large object"),
        OperatorRelationId => buffer.push_str("operator"),
        OperatorClassRelationId => buffer.push_str("operator class"),
        OperatorFamilyRelationId => buffer.push_str("operator family"),
        AccessMethodRelationId => buffer.push_str("access method"),
        AccessMethodOperatorRelationId => buffer.push_str("operator of access method"),
        AccessMethodProcedureRelationId => buffer.push_str("function of access method"),
        RewriteRelationId => buffer.push_str("rule"),
        TriggerRelationId => buffer.push_str("trigger"),
        NamespaceRelationId => buffer.push_str("schema"),
        StatisticExtRelationId => buffer.push_str("statistics object"),
        TSParserRelationId => buffer.push_str("text search parser"),
        TSDictionaryRelationId => buffer.push_str("text search dictionary"),
        TSTemplateRelationId => buffer.push_str("text search template"),
        TSConfigRelationId => buffer.push_str("text search configuration"),
        AuthIdRelationId => buffer.push_str("role"),
        AuthMemRelationId => buffer.push_str("role membership"),
        DatabaseRelationId => buffer.push_str("database"),
        TableSpaceRelationId => buffer.push_str("tablespace"),
        ForeignDataWrapperRelationId => buffer.push_str("foreign-data wrapper"),
        ForeignServerRelationId => buffer.push_str("server"),
        UserMappingRelationId => buffer.push_str("user mapping"),
        DefaultAclRelationId => buffer.push_str("default acl"),
        ExtensionRelationId => buffer.push_str("extension"),
        EventTriggerRelationId => buffer.push_str("event trigger"),
        ParameterAclRelationId => buffer.push_str("parameter ACL"),
        PolicyRelationId => buffer.push_str("policy"),
        PublicationRelationId => buffer.push_str("publication"),
        PublicationNamespaceRelationId => buffer.push_str("publication namespace"),
        PublicationRelRelationId => buffer.push_str("publication relation"),
        SubscriptionRelationId => buffer.push_str("subscription"),
        TransformRelationId => buffer.push_str("transform"),
        _ => {
            return Err(PgError::error(format!(
                "unsupported object class: {}",
                object.classId
            )));
        }
    }

    /* the result can never be empty */
    debug_assert!(!buffer.is_empty());

    Ok(Some(PgString::from_str_in(&buffer, mcx)?))
}

/// `getRelationTypeDescription(StringInfo buffer, Oid relid, int32
/// objectSubId, bool missing_ok)` (objectaddress.c 4687): decode the relkind
/// (and column subid) into the relation-type string.
pub fn get_relation_type_description<'mcx>(
    _mcx: Mcx<'mcx>,
    buffer: &mut String,
    relid: Oid,
    object_sub_id: i32,
    missing_ok: bool,
) -> PgResult<()> {
    // SearchSysCache1(RELOID, ObjectIdGetDatum(relid)) -> Form_pg_class.relkind;
    // Ok(None) is the C `!HeapTupleIsValid(relTup)`.
    let relkind = match syscache_seams::rel_relkind::call(relid)? {
        Some(rk) => rk,
        None => {
            if !missing_ok {
                return Err(PgError::error(format!(
                    "cache lookup failed for relation {relid}"
                )));
            }
            /* fallback to "relation" for an undefined object */
            buffer.push_str("relation");
            return Ok(());
        }
    };

    match relkind {
        RELKIND_RELATION | RELKIND_PARTITIONED_TABLE => buffer.push_str("table"),
        RELKIND_INDEX | RELKIND_PARTITIONED_INDEX => buffer.push_str("index"),
        RELKIND_SEQUENCE => buffer.push_str("sequence"),
        RELKIND_TOASTVALUE => buffer.push_str("toast table"),
        RELKIND_VIEW => buffer.push_str("view"),
        RELKIND_MATVIEW => buffer.push_str("materialized view"),
        RELKIND_COMPOSITE_TYPE => buffer.push_str("composite type"),
        RELKIND_FOREIGN_TABLE => buffer.push_str("foreign table"),
        /* shouldn't get here */
        _ => buffer.push_str("relation"),
    }

    if object_sub_id != 0 {
        buffer.push_str(" column");
    }

    Ok(())
}

/// `getConstraintTypeDescription(StringInfo buffer, Oid constroid, bool
/// missing_ok)` (objectaddress.c 4750): table- vs domain-constraint
/// disambiguation.
pub fn get_constraint_type_description<'mcx>(
    _mcx: Mcx<'mcx>,
    buffer: &mut String,
    constroid: Oid,
    missing_ok: bool,
) -> PgResult<()> {
    // table_open(ConstraintRelationId) + get_catalog_object_by_oid(...,
    // Anum_pg_constraint_oid, constroid) + GETSTRUCT(Form_pg_constraint) ->
    // (conrelid, contypid, oid); the installer owns the table open/close.
    let (conrelid, contypid, oid) =
        match pg_constraint_seams::constraint_type_oids::call(constroid)? {
            Some(t) => t,
            None => {
                if !missing_ok {
                    return Err(PgError::error(format!(
                        "cache lookup failed for constraint {constroid}"
                    )));
                }
                /* fallback to "constraint" for an undefined object */
                buffer.push_str("constraint");
                return Ok(());
            }
        };

    if conrelid != InvalidOid {
        buffer.push_str("table constraint");
    } else if contypid != InvalidOid {
        buffer.push_str("domain constraint");
    } else {
        return Err(PgError::error(format!("invalid constraint {oid}")));
    }

    Ok(())
}

/// `getProcedureTypeDescription(StringInfo buffer, Oid procid, bool
/// missing_ok)` (objectaddress.c 4787): prokind → function/procedure/aggregate
/// type string.
pub fn get_procedure_type_description<'mcx>(
    mcx: Mcx<'mcx>,
    buffer: &mut String,
    procid: Oid,
    missing_ok: bool,
) -> PgResult<()> {
    // SearchSysCache1(PROCOID, ObjectIdGetDatum(procid)) -> Form_pg_proc.prokind;
    // Ok(None) is the C `!HeapTupleIsValid(procTup)`.
    let prokind = match syscache_seams::pg_proc_form::call(mcx, procid)? {
        Some(form) => form.prokind,
        None => {
            if !missing_ok {
                return Err(PgError::error(format!(
                    "cache lookup failed for procedure {procid}"
                )));
            }
            /* fallback to "procedure" for an undefined object */
            buffer.push_str("routine");
            return Ok(());
        }
    };

    if prokind == PROKIND_AGGREGATE {
        buffer.push_str("aggregate");
    } else if prokind == PROKIND_PROCEDURE {
        buffer.push_str("procedure");
    } else {
        /* function or window function */
        buffer.push_str("function");
    }

    Ok(())
}
