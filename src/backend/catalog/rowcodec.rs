use std::collections::HashMap;
use std::sync::OnceLock;

use crate::backend::catalog::catalog::CatalogError;
use crate::backend::catalog::rows::PhysicalCatalogRows;
use crate::backend::executor::RelationDesc;
use crate::backend::executor::value_io::{decode_value, missing_column_value};
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::access::htup::{AttributeAlign, AttributeCompression, AttributeStorage};
use crate::include::catalog::{
    BootstrapCatalogKind, PG_STATISTIC_RELATION_OID, PG_STATISTIC_ROWTYPE_OID, PgAggregateRow,
    PgAmRow, PgAmopRow, PgAmprocRow, PgAttrdefRow, PgAttributeRow, PgAuthIdRow, PgAuthMembersRow,
    PgCastRow, PgClassRow, PgCollationRow, PgConstraintRow, PgConversionRow, PgDatabaseRow,
    PgDependRow, PgDescriptionRow, PgEventTriggerRow, PgForeignDataWrapperRow, PgForeignServerRow,
    PgForeignTableRow, PgIndexRow, PgInheritsRow, PgLanguageRow, PgNamespaceRow, PgOpclassRow,
    PgOperatorRow, PgOpfamilyRow, PgPartitionedTableRow, PgPolicyRow, PgProcRow,
    PgPublicationNamespaceRow, PgPublicationRelRow, PgPublicationRow, PgRewriteRow, PgSequenceRow,
    PgShdependRow, PgStatisticExtDataRow, PgStatisticExtRow, PgStatisticRow, PgTablespaceRow,
    PgTriggerRow, PgTsConfigMapRow, PgTsConfigRow, PgTsDictRow, PgTsParserRow, PgTsTemplateRow,
    PgTypeRow, PgUserMappingRow, bootstrap_composite_type_rows, builtin_type_rows, pg_type_desc,
};
use crate::include::nodes::datetime::TimestampTzADT;
use crate::include::nodes::datum::{ArrayDimension, ArrayValue, RecordValue, Value};

pub(crate) fn catalog_row_values_for_kind(
    rows: &PhysicalCatalogRows,
    kind: BootstrapCatalogKind,
) -> Vec<Vec<Value>> {
    match kind {
        BootstrapCatalogKind::PgNamespace => rows
            .namespaces
            .iter()
            .cloned()
            .map(namespace_row_values)
            .collect(),
        BootstrapCatalogKind::PgClass => rows
            .classes
            .iter()
            .cloned()
            .map(pg_class_row_values)
            .collect(),
        BootstrapCatalogKind::PgAttribute => rows
            .attributes
            .iter()
            .cloned()
            .map(pg_attribute_row_values)
            .collect(),
        BootstrapCatalogKind::PgType => {
            rows.types.iter().cloned().map(pg_type_row_values).collect()
        }
        BootstrapCatalogKind::PgProc => {
            rows.procs.iter().cloned().map(pg_proc_row_values).collect()
        }
        BootstrapCatalogKind::PgAggregate => rows
            .aggregates
            .iter()
            .cloned()
            .map(pg_aggregate_row_values)
            .collect(),
        BootstrapCatalogKind::PgLanguage => rows
            .languages
            .iter()
            .cloned()
            .map(pg_language_row_values)
            .collect(),
        BootstrapCatalogKind::PgTsParser => rows
            .ts_parsers
            .iter()
            .cloned()
            .map(pg_ts_parser_row_values)
            .collect(),
        BootstrapCatalogKind::PgTsTemplate => rows
            .ts_templates
            .iter()
            .cloned()
            .map(pg_ts_template_row_values)
            .collect(),
        BootstrapCatalogKind::PgTsDict => rows
            .ts_dicts
            .iter()
            .cloned()
            .map(pg_ts_dict_row_values)
            .collect(),
        BootstrapCatalogKind::PgTsConfig => rows
            .ts_configs
            .iter()
            .cloned()
            .map(pg_ts_config_row_values)
            .collect(),
        BootstrapCatalogKind::PgTsConfigMap => rows
            .ts_config_maps
            .iter()
            .cloned()
            .map(pg_ts_config_map_row_values)
            .collect(),
        BootstrapCatalogKind::PgOperator => rows
            .operators
            .iter()
            .cloned()
            .map(pg_operator_row_values)
            .collect(),
        BootstrapCatalogKind::PgDatabase => rows
            .databases
            .iter()
            .cloned()
            .map(pg_database_row_values)
            .collect(),
        BootstrapCatalogKind::PgAuthId => rows
            .authids
            .iter()
            .cloned()
            .map(pg_authid_row_values)
            .collect(),
        BootstrapCatalogKind::PgAuthMembers => rows
            .auth_members
            .iter()
            .cloned()
            .map(pg_auth_members_row_values)
            .collect(),
        BootstrapCatalogKind::PgCollation => rows
            .collations
            .iter()
            .cloned()
            .map(pg_collation_row_values)
            .collect(),
        BootstrapCatalogKind::PgLargeobject | BootstrapCatalogKind::PgLargeobjectMetadata => {
            Vec::new()
        }
        BootstrapCatalogKind::PgTablespace => rows
            .tablespaces
            .iter()
            .cloned()
            .map(pg_tablespace_row_values)
            .collect(),
        BootstrapCatalogKind::PgAm => rows.ams.iter().cloned().map(pg_am_row_values).collect(),
        BootstrapCatalogKind::PgAmop => {
            rows.amops.iter().cloned().map(pg_amop_row_values).collect()
        }
        BootstrapCatalogKind::PgAmproc => rows
            .amprocs
            .iter()
            .cloned()
            .map(pg_amproc_row_values)
            .collect(),
        BootstrapCatalogKind::PgAttrdef => rows
            .attrdefs
            .iter()
            .cloned()
            .map(pg_attrdef_row_values)
            .collect(),
        BootstrapCatalogKind::PgCast => {
            rows.casts.iter().cloned().map(pg_cast_row_values).collect()
        }
        BootstrapCatalogKind::PgConstraint => rows
            .constraints
            .iter()
            .cloned()
            .map(pg_constraint_row_values)
            .collect(),
        BootstrapCatalogKind::PgConversion => rows
            .conversions
            .iter()
            .cloned()
            .map(pg_conversion_row_values)
            .collect(),
        BootstrapCatalogKind::PgDepend => rows
            .depends
            .iter()
            .cloned()
            .map(pg_depend_row_values)
            .collect(),
        BootstrapCatalogKind::PgDefaultAcl
        | BootstrapCatalogKind::PgExtension
        | BootstrapCatalogKind::PgTransform
        | BootstrapCatalogKind::PgSubscription
        | BootstrapCatalogKind::PgParameterAcl
        | BootstrapCatalogKind::PgShdescription
        | BootstrapCatalogKind::PgReplicationOrigin => Vec::new(),
        BootstrapCatalogKind::PgShdepend => rows
            .shdepends
            .iter()
            .cloned()
            .map(pg_shdepend_row_values)
            .collect(),
        BootstrapCatalogKind::PgInherits => rows
            .inherits
            .iter()
            .cloned()
            .map(pg_inherits_row_values)
            .collect(),
        BootstrapCatalogKind::PgDescription => rows
            .descriptions
            .iter()
            .cloned()
            .map(pg_description_row_values)
            .collect(),
        BootstrapCatalogKind::PgForeignDataWrapper => rows
            .foreign_data_wrappers
            .iter()
            .cloned()
            .map(pg_foreign_data_wrapper_row_values)
            .collect(),
        BootstrapCatalogKind::PgForeignServer => rows
            .foreign_servers
            .iter()
            .cloned()
            .map(pg_foreign_server_row_values)
            .collect(),
        BootstrapCatalogKind::PgUserMapping => rows
            .user_mappings
            .iter()
            .cloned()
            .map(pg_user_mapping_row_values)
            .collect(),
        BootstrapCatalogKind::PgForeignTable => rows
            .foreign_tables
            .iter()
            .cloned()
            .map(pg_foreign_table_row_values)
            .collect(),
        BootstrapCatalogKind::PgIndex => rows
            .indexes
            .iter()
            .cloned()
            .map(pg_index_row_values)
            .collect(),
        BootstrapCatalogKind::PgPartitionedTable => rows
            .partitioned_tables
            .iter()
            .cloned()
            .map(pg_partitioned_table_row_values)
            .collect(),
        BootstrapCatalogKind::PgRewrite => rows
            .rewrites
            .iter()
            .cloned()
            .map(pg_rewrite_row_values)
            .collect(),
        BootstrapCatalogKind::PgSequence => rows
            .sequences
            .iter()
            .cloned()
            .map(pg_sequence_row_values)
            .collect(),
        BootstrapCatalogKind::PgTrigger => rows
            .triggers
            .iter()
            .cloned()
            .map(pg_trigger_row_values)
            .collect(),
        BootstrapCatalogKind::PgEventTrigger => rows
            .event_triggers
            .iter()
            .cloned()
            .map(pg_event_trigger_row_values)
            .collect(),
        BootstrapCatalogKind::PgPolicy => rows
            .policies
            .iter()
            .cloned()
            .map(pg_policy_row_values)
            .collect(),
        BootstrapCatalogKind::PgPublication => rows
            .publications
            .iter()
            .cloned()
            .map(pg_publication_row_values)
            .collect(),
        BootstrapCatalogKind::PgPublicationRel => rows
            .publication_rels
            .iter()
            .cloned()
            .map(pg_publication_rel_row_values)
            .collect(),
        BootstrapCatalogKind::PgPublicationNamespace => rows
            .publication_namespaces
            .iter()
            .cloned()
            .map(pg_publication_namespace_row_values)
            .collect(),
        BootstrapCatalogKind::PgStatistic => rows
            .statistics
            .iter()
            .cloned()
            .map(pg_statistic_row_values)
            .collect(),
        BootstrapCatalogKind::PgStatisticExt => rows
            .statistics_ext
            .iter()
            .cloned()
            .map(pg_statistic_ext_row_values)
            .collect(),
        BootstrapCatalogKind::PgStatisticExtData => rows
            .statistics_ext_data
            .iter()
            .cloned()
            .map(pg_statistic_ext_data_row_values)
            .collect(),
        BootstrapCatalogKind::PgOpclass => rows
            .opclasses
            .iter()
            .cloned()
            .map(pg_opclass_row_values)
            .collect(),
        BootstrapCatalogKind::PgOpfamily => rows
            .opfamilies
            .iter()
            .cloned()
            .map(pg_opfamily_row_values)
            .collect(),
    }
}

pub(crate) fn decode_catalog_tuple_values(
    desc: &RelationDesc,
    tuple: &crate::include::access::htup::HeapTuple,
) -> Result<Vec<Value>, CatalogError> {
    let raw = tuple
        .deform(&desc.attribute_descs())
        .map_err(|e| CatalogError::Io(format!("{e:?}")))?;
    desc.columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            if let Some(datum) = raw.get(index) {
                decode_value(column, *datum).map_err(|e| CatalogError::Io(format!("{e:?}")))
            } else {
                Ok(missing_column_value(column))
            }
        })
        .collect()
}

pub(crate) fn parse_indkey(indkey: &str) -> Vec<i16> {
    vector_text_items(indkey)
        .into_iter()
        .filter_map(|value| value.parse::<i16>().ok())
        .collect()
}

pub(crate) fn parse_oidvector(values: &str) -> Vec<u32> {
    vector_text_items(values)
        .into_iter()
        .filter_map(|value| value.parse::<u32>().ok())
        .collect()
}

fn vector_text_items(text: &str) -> Vec<&str> {
    let trimmed = text.trim();
    let body = if let Some(equals) = trimmed.find('=')
        && trimmed.starts_with('[')
    {
        trimmed[equals + 1..].trim()
    } else {
        trimmed
    };
    if let Some(inner) = body
        .strip_prefix('{')
        .and_then(|value| value.strip_suffix('}'))
    {
        inner
            .split(',')
            .map(|value| value.trim().trim_matches('"'))
            .filter(|value| !value.is_empty())
            .collect()
    } else {
        body.split_ascii_whitespace().collect()
    }
}

fn format_oidvector(values: &[u32]) -> String {
    values
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(" ")
}

fn parse_indkey_value(value: &Value) -> Result<Vec<i16>, CatalogError> {
    match value {
        Value::Text(text) => Ok(parse_indkey(text)),
        Value::PgArray(array) => {
            array
                .elements
                .iter()
                .map(|value| match value {
                    Value::Int16(v) => Ok(*v),
                    Value::Int32(v) => i16::try_from(*v)
                        .map_err(|_| CatalogError::Corrupt("invalid int2vector value")),
                    Value::Int64(v) => i16::try_from(*v)
                        .map_err(|_| CatalogError::Corrupt("invalid int2vector value")),
                    _ => Err(CatalogError::Corrupt("expected int2vector value")),
                })
                .collect()
        }
        _ => Err(CatalogError::Corrupt("expected int2vector value")),
    }
}

fn parse_oidvector_value(value: &Value) -> Result<Vec<u32>, CatalogError> {
    match value {
        Value::Text(text) => Ok(parse_oidvector(text)),
        Value::PgArray(array) => array
            .elements
            .iter()
            .map(|value| match value {
                Value::Int32(v) if *v >= 0 => Ok(*v as u32),
                Value::Int64(v) => {
                    u32::try_from(*v).map_err(|_| CatalogError::Corrupt("invalid oidvector value"))
                }
                _ => Err(CatalogError::Corrupt("expected oidvector value")),
            })
            .collect(),
        _ => Err(CatalogError::Corrupt("expected oidvector value")),
    }
}

pub(crate) fn namespace_row_from_values(
    values: Vec<Value>,
) -> Result<PgNamespaceRow, CatalogError> {
    Ok(PgNamespaceRow {
        oid: expect_oid(&values[0])?,
        nspname: expect_text(&values[1])?,
        nspowner: expect_oid(&values[2])?,
        nspacl: nullable_text_array(&values[3])?,
    })
}

pub(crate) fn pg_class_row_from_values(values: Vec<Value>) -> Result<PgClassRow, CatalogError> {
    let has_relhasindex = matches!(values.get(13), Some(Value::Bool(_)));
    let offset = usize::from(has_relhasindex);
    let relpersistence = expect_char(&values[13 + offset], "relpersistence")?;
    let relkind = expect_char(&values[14 + offset], "relkind")?;
    Ok(PgClassRow {
        oid: expect_oid(&values[0])?,
        relname: expect_text(&values[1])?,
        relnamespace: expect_oid(&values[2])?,
        reltype: expect_oid(&values[3])?,
        relowner: expect_oid(&values[4])?,
        relam: expect_oid(&values[5])?,
        relfilenode: expect_oid(&values[6])?,
        reltablespace: expect_oid(&values[7])?,
        relpages: expect_int32(&values[8])?,
        reltuples: expect_float64(&values[9])?,
        relallvisible: expect_int32(&values[10])?,
        relallfrozen: expect_int32(&values[11])?,
        reltoastrelid: expect_oid(&values[12])?,
        relhasindex: if has_relhasindex {
            expect_bool(&values[13])?
        } else {
            false
        },
        relpersistence,
        relkind,
        relnatts: expect_int16(&values[15 + offset])?,
        relhassubclass: expect_bool(&values[16 + offset])?,
        relhastriggers: expect_bool(&values[17 + offset])?,
        relrowsecurity: expect_bool(&values[18 + offset])?,
        relforcerowsecurity: expect_bool(&values[19 + offset])?,
        relispopulated: expect_bool(&values[20 + offset])?,
        relispartition: expect_bool(&values[21 + offset])?,
        relfrozenxid: expect_oid(&values[22 + offset])?,
        relpartbound: nullable_text(&values[23 + offset])?,
        reloptions: nullable_text_array(&values[24 + offset])?,
        relacl: nullable_text_array(&values[25 + offset])?,
        relreplident: match values.get(26 + offset) {
            Some(Value::InternalChar(_) | Value::Text(_)) => {
                expect_char(&values[26 + offset], "relreplident")?
            }
            _ => 'd',
        },
        reloftype: values
            .get(27 + offset)
            .map(expect_oid)
            .transpose()?
            .or_else(|| {
                values
                    .get(26 + offset)
                    .and_then(|value| expect_oid(value).ok())
            })
            .unwrap_or(0),
    })
}

pub(crate) fn pg_partitioned_table_row_from_values(
    values: Vec<Value>,
) -> Result<PgPartitionedTableRow, CatalogError> {
    Ok(PgPartitionedTableRow {
        partrelid: expect_oid(&values[0])?,
        partstrat: expect_char(&values[1], "partstrat")?,
        partnatts: expect_int16(&values[2])?,
        partdefid: expect_oid(&values[3])?,
        partattrs: parse_indkey_value(&values[4])?,
        partclass: parse_oidvector_value(&values[5])?,
        partcollation: parse_oidvector_value(&values[6])?,
        partexprs: nullable_text(&values[7])?,
    })
}

pub(crate) fn pg_am_row_from_values(values: Vec<Value>) -> Result<PgAmRow, CatalogError> {
    Ok(PgAmRow {
        oid: expect_oid(&values[0])?,
        amname: expect_text(&values[1])?,
        amhandler: expect_oid(&values[2])?,
        amtype: expect_char(&values[3], "amtype")?,
    })
}

pub(crate) fn pg_trigger_row_from_values(values: Vec<Value>) -> Result<PgTriggerRow, CatalogError> {
    Ok(PgTriggerRow {
        oid: expect_oid(&values[0])?,
        tgrelid: expect_oid(&values[1])?,
        tgparentid: expect_oid(&values[2])?,
        tgname: expect_text(&values[3])?,
        tgfoid: expect_oid(&values[4])?,
        tgtype: expect_int16(&values[5])?,
        tgenabled: expect_char(&values[6], "tgenabled")?,
        tgisinternal: expect_bool(&values[7])?,
        tgconstrrelid: expect_oid(&values[8])?,
        tgconstrindid: expect_oid(&values[9])?,
        tgconstraint: expect_oid(&values[10])?,
        tgdeferrable: expect_bool(&values[11])?,
        tginitdeferred: expect_bool(&values[12])?,
        tgnargs: expect_int16(&values[13])?,
        tgattr: nullable_int16_array(&values[14])?
            .ok_or(CatalogError::Corrupt("expected tgattr array"))?,
        tgargs: nullable_text_array(&values[15])?
            .ok_or(CatalogError::Corrupt("expected tgargs array"))?,
        tgqual: nullable_text(&values[16])?,
        tgoldtable: nullable_text(&values[17])?,
        tgnewtable: nullable_text(&values[18])?,
    })
}

pub(crate) fn pg_event_trigger_row_from_values(
    values: Vec<Value>,
) -> Result<PgEventTriggerRow, CatalogError> {
    Ok(PgEventTriggerRow {
        oid: expect_oid(&values[0])?,
        evtname: expect_text(&values[1])?,
        evtevent: expect_text(&values[2])?,
        evtowner: expect_oid(&values[3])?,
        evtfoid: expect_oid(&values[4])?,
        evtenabled: expect_char(&values[5], "evtenabled")?,
        evttags: nullable_text_array(&values[6])?,
    })
}

pub(crate) fn pg_publication_row_from_values(
    values: Vec<Value>,
) -> Result<PgPublicationRow, CatalogError> {
    let has_puballsequences = values.len() >= 11;
    let action_offset = if has_puballsequences { 1 } else { 0 };
    Ok(PgPublicationRow {
        oid: expect_oid(&values[0])?,
        pubname: expect_text(&values[1])?,
        pubowner: expect_oid(&values[2])?,
        puballtables: expect_bool(&values[3])?,
        puballsequences: if has_puballsequences {
            expect_bool(&values[4])?
        } else {
            false
        },
        pubinsert: expect_bool(&values[4 + action_offset])?,
        pubupdate: expect_bool(&values[5 + action_offset])?,
        pubdelete: expect_bool(&values[6 + action_offset])?,
        pubtruncate: expect_bool(&values[7 + action_offset])?,
        pubviaroot: expect_bool(&values[8 + action_offset])?,
        pubgencols: expect_char(&values[9 + action_offset], "pubgencols")?,
    })
}

pub(crate) fn pg_publication_rel_row_from_values(
    values: Vec<Value>,
) -> Result<PgPublicationRelRow, CatalogError> {
    let has_prexcept = values.len() >= 6;
    let varlena_offset = if has_prexcept { 1 } else { 0 };
    Ok(PgPublicationRelRow {
        oid: expect_oid(&values[0])?,
        prpubid: expect_oid(&values[1])?,
        prrelid: expect_oid(&values[2])?,
        prexcept: if has_prexcept {
            expect_bool(&values[3])?
        } else {
            false
        },
        prqual: expect_nullable_text(&values[3 + varlena_offset])?,
        prattrs: match values.get(4 + varlena_offset) {
            Some(Value::Null) | None => None,
            Some(value) => Some(parse_indkey_value(value)?),
        },
    })
}

pub(crate) fn pg_publication_namespace_row_from_values(
    values: Vec<Value>,
) -> Result<PgPublicationNamespaceRow, CatalogError> {
    Ok(PgPublicationNamespaceRow {
        oid: expect_oid(&values[0])?,
        pnpubid: expect_oid(&values[1])?,
        pnnspid: expect_oid(&values[2])?,
    })
}

pub(crate) fn pg_policy_row_from_values(values: Vec<Value>) -> Result<PgPolicyRow, CatalogError> {
    Ok(PgPolicyRow {
        oid: expect_oid(&values[0])?,
        polname: expect_text(&values[1])?,
        polrelid: expect_oid(&values[2])?,
        polcmd: crate::include::catalog::PolicyCommand::from_char(expect_char(
            &values[3], "polcmd",
        )?)
        .ok_or(CatalogError::Corrupt("expected recognized policy command"))?,
        polpermissive: expect_bool(&values[4])?,
        polroles: nullable_oid_array(&values[5])?
            .ok_or(CatalogError::Corrupt("expected polroles array"))?,
        polqual: nullable_text(&values[6])?,
        polwithcheck: nullable_text(&values[7])?,
    })
}

pub(crate) fn pg_amop_row_from_values(values: Vec<Value>) -> Result<PgAmopRow, CatalogError> {
    Ok(PgAmopRow {
        oid: expect_oid(&values[0])?,
        amopfamily: expect_oid(&values[1])?,
        amoplefttype: expect_oid(&values[2])?,
        amoprighttype: expect_oid(&values[3])?,
        amopstrategy: expect_int16(&values[4])?,
        amoppurpose: expect_char(&values[5], "amoppurpose")?,
        amopopr: expect_oid(&values[6])?,
        amopmethod: expect_oid(&values[7])?,
        amopsortfamily: expect_oid(&values[8])?,
    })
}

pub(crate) fn pg_amproc_row_from_values(values: Vec<Value>) -> Result<PgAmprocRow, CatalogError> {
    Ok(PgAmprocRow {
        oid: expect_oid(&values[0])?,
        amprocfamily: expect_oid(&values[1])?,
        amproclefttype: expect_oid(&values[2])?,
        amprocrighttype: expect_oid(&values[3])?,
        amprocnum: expect_int16(&values[4])?,
        amproc: expect_oid(&values[5])?,
    })
}

pub(crate) fn pg_authid_row_from_values(values: Vec<Value>) -> Result<PgAuthIdRow, CatalogError> {
    Ok(PgAuthIdRow {
        oid: expect_oid(&values[0])?,
        rolname: expect_text(&values[1])?,
        rolsuper: expect_bool(&values[2])?,
        rolinherit: expect_bool(&values[3])?,
        rolcreaterole: expect_bool(&values[4])?,
        rolcreatedb: expect_bool(&values[5])?,
        rolcanlogin: expect_bool(&values[6])?,
        rolreplication: expect_bool(&values[7])?,
        rolbypassrls: expect_bool(&values[8])?,
        rolconnlimit: expect_int32(&values[9])?,
        rolpassword: values.get(10).map(nullable_text).transpose()?.flatten(),
        rolvaliduntil: values
            .get(11)
            .map(nullable_timestamptz)
            .transpose()?
            .flatten(),
    })
}

pub(crate) fn pg_auth_members_row_from_values(
    values: Vec<Value>,
) -> Result<PgAuthMembersRow, CatalogError> {
    Ok(PgAuthMembersRow {
        oid: expect_oid(&values[0])?,
        roleid: expect_oid(&values[1])?,
        member: expect_oid(&values[2])?,
        grantor: expect_oid(&values[3])?,
        admin_option: expect_bool(&values[4])?,
        inherit_option: expect_bool(&values[5])?,
        set_option: expect_bool(&values[6])?,
    })
}

pub(crate) fn pg_language_row_from_values(
    values: Vec<Value>,
) -> Result<PgLanguageRow, CatalogError> {
    Ok(PgLanguageRow {
        oid: expect_oid(&values[0])?,
        lanname: expect_text(&values[1])?,
        lanowner: expect_oid(&values[2])?,
        lanispl: expect_bool(&values[3])?,
        lanpltrusted: expect_bool(&values[4])?,
        lanplcallfoid: expect_oid(&values[5])?,
        laninline: expect_oid(&values[6])?,
        lanvalidator: expect_oid(&values[7])?,
    })
}

pub(crate) fn pg_ts_parser_row_from_values(
    values: Vec<Value>,
) -> Result<PgTsParserRow, CatalogError> {
    Ok(PgTsParserRow {
        oid: expect_oid(&values[0])?,
        prsname: expect_text(&values[1])?,
        prsnamespace: expect_oid(&values[2])?,
        prsstart: expect_oid(&values[3])?,
        prstoken: expect_oid(&values[4])?,
        prsend: expect_oid(&values[5])?,
        prsheadline: expect_nullable_oid(&values[6])?,
        prslextype: expect_oid(&values[7])?,
    })
}

pub(crate) fn pg_ts_template_row_from_values(
    values: Vec<Value>,
) -> Result<PgTsTemplateRow, CatalogError> {
    Ok(PgTsTemplateRow {
        oid: expect_oid(&values[0])?,
        tmplname: expect_text(&values[1])?,
        tmplnamespace: expect_oid(&values[2])?,
        tmplinit: expect_nullable_oid(&values[3])?,
        tmpllexize: expect_oid(&values[4])?,
    })
}

pub(crate) fn pg_ts_dict_row_from_values(values: Vec<Value>) -> Result<PgTsDictRow, CatalogError> {
    Ok(PgTsDictRow {
        oid: expect_oid(&values[0])?,
        dictname: expect_text(&values[1])?,
        dictnamespace: expect_oid(&values[2])?,
        dictowner: expect_oid(&values[3])?,
        dicttemplate: expect_oid(&values[4])?,
        dictinitoption: expect_nullable_text(&values[5])?,
    })
}

pub(crate) fn pg_ts_config_row_from_values(
    values: Vec<Value>,
) -> Result<PgTsConfigRow, CatalogError> {
    Ok(PgTsConfigRow {
        oid: expect_oid(&values[0])?,
        cfgname: expect_text(&values[1])?,
        cfgnamespace: expect_oid(&values[2])?,
        cfgowner: expect_oid(&values[3])?,
        cfgparser: expect_oid(&values[4])?,
    })
}

pub(crate) fn pg_ts_config_map_row_from_values(
    values: Vec<Value>,
) -> Result<PgTsConfigMapRow, CatalogError> {
    Ok(PgTsConfigMapRow {
        mapcfg: expect_oid(&values[0])?,
        maptokentype: expect_int32(&values[1])?,
        mapseqno: expect_int32(&values[2])?,
        mapdict: expect_oid(&values[3])?,
    })
}

pub(crate) fn pg_sequence_row_from_values(
    values: Vec<Value>,
) -> Result<PgSequenceRow, CatalogError> {
    Ok(PgSequenceRow {
        seqrelid: expect_oid(&values[0])?,
        seqtypid: expect_oid(&values[1])?,
        seqstart: expect_int64(&values[2])?,
        seqincrement: expect_int64(&values[3])?,
        seqmax: expect_int64(&values[4])?,
        seqmin: expect_int64(&values[5])?,
        seqcache: expect_int64(&values[6])?,
        seqcycle: expect_bool(&values[7])?,
    })
}

pub(crate) fn pg_operator_row_from_values(
    values: Vec<Value>,
) -> Result<PgOperatorRow, CatalogError> {
    Ok(PgOperatorRow {
        oid: expect_oid(&values[0])?,
        oprname: expect_text(&values[1])?,
        oprnamespace: expect_oid(&values[2])?,
        oprowner: expect_oid(&values[3])?,
        oprkind: expect_char(&values[4], "oprkind")?,
        oprcanmerge: expect_bool(&values[5])?,
        oprcanhash: expect_bool(&values[6])?,
        oprleft: expect_oid(&values[7])?,
        oprright: expect_oid(&values[8])?,
        oprresult: expect_oid(&values[9])?,
        oprcom: expect_oid(&values[10])?,
        oprnegate: expect_oid(&values[11])?,
        oprcode: expect_oid(&values[12])?,
        oprrest: expect_oid(&values[13])?,
        oprjoin: expect_oid(&values[14])?,
    })
}

pub(crate) fn pg_proc_row_from_values(values: Vec<Value>) -> Result<PgProcRow, CatalogError> {
    let has_added_proc_columns = values.len() > 24;
    let has_proconfig_column = values.len() > 28;
    let prosrc_index = if has_added_proc_columns { 24 } else { 23 };
    let proacl_index = if has_proconfig_column {
        28
    } else if has_added_proc_columns {
        27
    } else {
        24
    };
    Ok(PgProcRow {
        oid: expect_oid(&values[0])?,
        proname: expect_text(&values[1])?,
        pronamespace: expect_oid(&values[2])?,
        proowner: expect_oid(&values[3])?,
        prolang: expect_oid(&values[4])?,
        procost: expect_float64(&values[5])?,
        prorows: expect_float64(&values[6])?,
        provariadic: expect_oid(&values[7])?,
        prosupport: expect_oid(&values[8])?,
        prokind: expect_char(&values[9], "prokind")?,
        prosecdef: expect_bool(&values[10])?,
        proleakproof: expect_bool(&values[11])?,
        proisstrict: expect_bool(&values[12])?,
        proretset: expect_bool(&values[13])?,
        provolatile: expect_char(&values[14], "provolatile")?,
        proparallel: expect_char(&values[15], "proparallel")?,
        pronargs: expect_int16(&values[16])?,
        pronargdefaults: expect_int16(&values[17])?,
        prorettype: expect_oid(&values[18])?,
        proargtypes: format_oidvector(&parse_oidvector_value(&values[19])?),
        proallargtypes: nullable_oid_array(&values[20])?,
        proargmodes: nullable_char_array(&values[21])?,
        proargnames: nullable_text_array(&values[22])?,
        proargdefaults: if has_added_proc_columns {
            nullable_text(&values[23])?
        } else {
            None
        },
        prosrc: expect_text(&values[prosrc_index])?,
        probin: values
            .get(prosrc_index + 1)
            .map(nullable_text)
            .transpose()?
            .flatten(),
        prosqlbody: values
            .get(prosrc_index + 2)
            .map(nullable_text)
            .transpose()?
            .flatten(),
        proconfig: if has_proconfig_column {
            nullable_text_array(&values[27])?
        } else {
            None
        },
        proacl: values
            .get(proacl_index)
            .map(nullable_text_array)
            .transpose()?
            .flatten(),
    })
}

pub(crate) fn pg_aggregate_row_from_values(
    values: Vec<Value>,
) -> Result<PgAggregateRow, CatalogError> {
    Ok(PgAggregateRow {
        aggfnoid: expect_oid(&values[0])?,
        aggkind: expect_char(&values[1], "aggkind")?,
        aggnumdirectargs: expect_int16(&values[2])?,
        aggtransfn: expect_oid(&values[3])?,
        aggfinalfn: expect_oid(&values[4])?,
        aggcombinefn: expect_oid(&values[5])?,
        aggserialfn: expect_oid(&values[6])?,
        aggdeserialfn: expect_oid(&values[7])?,
        aggmtransfn: expect_oid(&values[8])?,
        aggminvtransfn: expect_oid(&values[9])?,
        aggmfinalfn: expect_oid(&values[10])?,
        aggfinalextra: expect_bool(&values[11])?,
        aggmfinalextra: expect_bool(&values[12])?,
        aggfinalmodify: expect_char(&values[13], "aggfinalmodify")?,
        aggmfinalmodify: expect_char(&values[14], "aggmfinalmodify")?,
        aggsortop: expect_oid(&values[15])?,
        aggtranstype: expect_oid(&values[16])?,
        aggtransspace: expect_int32(&values[17])?,
        aggmtranstype: expect_oid(&values[18])?,
        aggmtransspace: expect_int32(&values[19])?,
        agginitval: nullable_text(&values[20])?,
        aggminitval: nullable_text(&values[21])?,
    })
}

pub(crate) fn pg_collation_row_from_values(
    values: Vec<Value>,
) -> Result<PgCollationRow, CatalogError> {
    Ok(PgCollationRow {
        oid: expect_oid(&values[0])?,
        collname: expect_text(&values[1])?,
        collnamespace: expect_oid(&values[2])?,
        collowner: expect_oid(&values[3])?,
        collprovider: expect_char(&values[4], "collprovider")?,
        collisdeterministic: expect_bool(&values[5])?,
        collencoding: expect_int32(&values[6])?,
    })
}

pub(crate) fn pg_foreign_data_wrapper_row_from_values(
    values: Vec<Value>,
) -> Result<PgForeignDataWrapperRow, CatalogError> {
    Ok(PgForeignDataWrapperRow {
        oid: expect_oid(&values[0])?,
        fdwname: expect_text(&values[1])?,
        fdwowner: expect_oid(&values[2])?,
        fdwhandler: expect_oid(&values[3])?,
        fdwvalidator: expect_oid(&values[4])?,
        fdwacl: nullable_text_array(&values[5])?,
        fdwoptions: nullable_text_array(&values[6])?,
    })
}

pub(crate) fn pg_foreign_server_row_from_values(
    values: Vec<Value>,
) -> Result<PgForeignServerRow, CatalogError> {
    Ok(PgForeignServerRow {
        oid: expect_oid(&values[0])?,
        srvname: expect_text(&values[1])?,
        srvowner: expect_oid(&values[2])?,
        srvfdw: expect_oid(&values[3])?,
        srvtype: nullable_text(&values[4])?,
        srvversion: nullable_text(&values[5])?,
        srvacl: nullable_text_array(&values[6])?,
        srvoptions: nullable_text_array(&values[7])?,
    })
}

pub(crate) fn pg_user_mapping_row_from_values(
    values: Vec<Value>,
) -> Result<PgUserMappingRow, CatalogError> {
    Ok(PgUserMappingRow {
        oid: expect_oid(&values[0])?,
        umuser: expect_oid(&values[1])?,
        umserver: expect_oid(&values[2])?,
        umoptions: nullable_text_array(&values[3])?,
    })
}

pub(crate) fn pg_foreign_table_row_from_values(
    values: Vec<Value>,
) -> Result<PgForeignTableRow, CatalogError> {
    Ok(PgForeignTableRow {
        ftrelid: expect_oid(&values[0])?,
        ftserver: expect_oid(&values[1])?,
        ftoptions: nullable_text_array(&values[2])?,
    })
}

pub(crate) fn pg_cast_row_from_values(values: Vec<Value>) -> Result<PgCastRow, CatalogError> {
    Ok(PgCastRow {
        oid: expect_oid(&values[0])?,
        castsource: expect_oid(&values[1])?,
        casttarget: expect_oid(&values[2])?,
        castfunc: expect_oid(&values[3])?,
        castcontext: expect_char(&values[4], "castcontext")?,
        castmethod: expect_char(&values[5], "castmethod")?,
    })
}

pub(crate) fn pg_conversion_row_from_values(
    values: Vec<Value>,
) -> Result<PgConversionRow, CatalogError> {
    Ok(PgConversionRow {
        oid: expect_oid(&values[0])?,
        conname: expect_text(&values[1])?,
        connamespace: expect_oid(&values[2])?,
        conowner: expect_oid(&values[3])?,
        conforencoding: expect_int32(&values[4])?,
        contoencoding: expect_int32(&values[5])?,
        conproc: expect_oid(&values[6])?,
        condefault: expect_bool(&values[7])?,
    })
}

pub(crate) fn pg_constraint_row_from_values(
    values: Vec<Value>,
) -> Result<PgConstraintRow, CatalogError> {
    Ok(PgConstraintRow {
        oid: expect_oid(&values[0])?,
        conname: expect_text(&values[1])?,
        connamespace: expect_oid(&values[2])?,
        contype: expect_char(&values[3], "contype")?,
        condeferrable: expect_bool(&values[4])?,
        condeferred: expect_bool(&values[5])?,
        conenforced: expect_bool(&values[6])?,
        convalidated: expect_bool(&values[7])?,
        conrelid: expect_oid(&values[8])?,
        contypid: expect_oid(&values[9])?,
        conindid: expect_oid(&values[10])?,
        conparentid: expect_oid(&values[11])?,
        confrelid: expect_oid(&values[12])?,
        confupdtype: expect_char(&values[13], "confupdtype")?,
        confdeltype: expect_char(&values[14], "confdeltype")?,
        confmatchtype: expect_char(&values[15], "confmatchtype")?,
        conkey: nullable_int16_array(&values[16])?,
        confkey: nullable_int16_array(&values[17])?,
        conpfeqop: nullable_oid_array(&values[18])?,
        conppeqop: nullable_oid_array(&values[19])?,
        conffeqop: nullable_oid_array(&values[20])?,
        confdelsetcols: nullable_int16_array(&values[21])?,
        conexclop: nullable_oid_array(&values[22])?,
        conbin: nullable_text(&values[23])?,
        conislocal: expect_bool(&values[24])?,
        coninhcount: expect_int16(&values[25])?,
        connoinherit: expect_bool(&values[26])?,
        conperiod: expect_bool(&values[27])?,
    })
}

pub(crate) fn pg_database_row_from_values(
    values: Vec<Value>,
) -> Result<PgDatabaseRow, CatalogError> {
    let dathasloginevt = match values.get(15) {
        Some(Value::Null) | None => false,
        Some(value) => expect_bool(value)?,
    };
    Ok(PgDatabaseRow {
        oid: expect_oid(&values[0])?,
        datname: expect_text(&values[1])?,
        datdba: expect_oid(&values[2])?,
        encoding: expect_int32(&values[3])?,
        datlocprovider: expect_char(&values[4], "datlocprovider")?,
        dattablespace: expect_oid(&values[5])?,
        datistemplate: expect_bool(&values[6])?,
        datallowconn: expect_bool(&values[7])?,
        datconnlimit: expect_int32(&values[8])?,
        datcollate: expect_text(&values[9])?,
        datctype: expect_text(&values[10])?,
        datlocale: expect_nullable_text(&values[11])?,
        daticurules: expect_nullable_text(&values[12])?,
        datcollversion: expect_nullable_text(&values[13])?,
        datacl: nullable_text_array(&values[14])?,
        dathasloginevt,
    })
}

pub(crate) fn pg_tablespace_row_from_values(
    values: Vec<Value>,
) -> Result<PgTablespaceRow, CatalogError> {
    Ok(PgTablespaceRow {
        oid: expect_oid(&values[0])?,
        spcname: expect_text(&values[1])?,
        spcowner: expect_oid(&values[2])?,
        spcacl: values
            .get(3)
            .map(nullable_text_array)
            .transpose()?
            .flatten(),
        spcoptions: values
            .get(4)
            .map(nullable_text_array)
            .transpose()?
            .flatten(),
    })
}

pub(crate) fn pg_attribute_row_from_values(
    values: Vec<Value>,
) -> Result<PgAttributeRow, CatalogError> {
    let atttypid = expect_oid(&values[2])?;
    let attalign = expect_char(&values[8], "attalign")?;
    let attstorage = expect_char(&values[9], "attstorage")?;
    let attcompression = match &values[10] {
        Value::Text(text) if text.is_empty() => '\0',
        other => expect_char(other, "attcompression")?,
    };
    let attidentity = values
        .get(14)
        .map(|value| match value {
            Value::Text(text) if text.is_empty() => Ok('\0'),
            other => expect_char(other, "attidentity"),
        })
        .transpose()?
        .unwrap_or('\0');
    let attgenerated = values
        .get(15)
        .map(|value| match value {
            Value::Text(text) if text.is_empty() => Ok('\0'),
            other => expect_char(other, "attgenerated"),
        })
        .transpose()?
        .unwrap_or('\0');
    let attcollation = match values.get(16) {
        Some(Value::Null) | None => default_attcollation_for_type_oid(atttypid),
        Some(value) => expect_oid(value)?,
    };
    let attacl = values
        .get(17)
        .map(nullable_text_array)
        .transpose()?
        .flatten();
    let attoptions = values
        .get(18)
        .map(nullable_text_array)
        .transpose()?
        .flatten();
    let attfdwoptions = values
        .get(19)
        .map(nullable_text_array)
        .transpose()?
        .flatten();
    let attmissingval = values
        .get(20)
        .map(nullable_any_array)
        .transpose()?
        .flatten();
    let attbyval = values
        .get(21)
        .map(expect_bool)
        .transpose()?
        .unwrap_or(false);
    Ok(PgAttributeRow {
        attrelid: expect_oid(&values[0])?,
        attname: expect_text(&values[1])?,
        atttypid,
        attlen: expect_int16(&values[3])?,
        attnum: expect_int16(&values[4])?,
        attnotnull: expect_bool(&values[5])?,
        attisdropped: expect_bool(&values[6])?,
        atttypmod: expect_int32(&values[7])?,
        attalign: AttributeAlign::from_char(attalign)
            .ok_or(CatalogError::Corrupt("unknown attalign"))?,
        attstorage: AttributeStorage::from_char(attstorage)
            .ok_or(CatalogError::Corrupt("unknown attstorage"))?,
        attcompression: AttributeCompression::from_char(attcompression)
            .ok_or(CatalogError::Corrupt("unknown attcompression"))?,
        attstattarget: expect_int16(&values[11])?,
        attinhcount: expect_int16(&values[12])?,
        attislocal: expect_bool(&values[13])?,
        attidentity,
        attgenerated,
        attcollation,
        attacl,
        attoptions,
        attfdwoptions,
        attmissingval,
        attbyval,
        sql_type: SqlType::new(SqlTypeKind::Text),
    })
}

fn default_attcollation_for_type_oid(type_oid: u32) -> u32 {
    builtin_type_rows()
        .into_iter()
        .chain(bootstrap_composite_type_rows())
        .find(|row| row.oid == type_oid)
        .map(|row| crate::backend::catalog::catalog::default_column_collation_oid(row.sql_type))
        .unwrap_or(0)
}

pub(crate) fn pg_inherits_row_from_values(
    values: Vec<Value>,
) -> Result<PgInheritsRow, CatalogError> {
    Ok(PgInheritsRow {
        inhrelid: expect_oid(&values[0])?,
        inhparent: expect_oid(&values[1])?,
        inhseqno: expect_int32(&values[2])?,
        inhdetachpending: expect_bool(&values[3])?,
    })
}

pub(crate) fn pg_attrdef_row_from_values(values: Vec<Value>) -> Result<PgAttrdefRow, CatalogError> {
    Ok(PgAttrdefRow {
        oid: expect_oid(&values[0])?,
        adrelid: expect_oid(&values[1])?,
        adnum: expect_int16(&values[2])?,
        adbin: expect_text(&values[3])?,
    })
}

pub(crate) fn pg_depend_row_from_values(values: Vec<Value>) -> Result<PgDependRow, CatalogError> {
    Ok(PgDependRow {
        classid: expect_oid(&values[0])?,
        objid: expect_oid(&values[1])?,
        objsubid: expect_int32(&values[2])?,
        refclassid: expect_oid(&values[3])?,
        refobjid: expect_oid(&values[4])?,
        refobjsubid: expect_int32(&values[5])?,
        deptype: expect_char(&values[6], "deptype")?,
    })
}

pub(crate) fn pg_shdepend_row_from_values(
    values: Vec<Value>,
) -> Result<PgShdependRow, CatalogError> {
    Ok(PgShdependRow {
        dbid: expect_oid(&values[0])?,
        classid: expect_oid(&values[1])?,
        objid: expect_oid(&values[2])?,
        objsubid: expect_int32(&values[3])?,
        refclassid: expect_oid(&values[4])?,
        refobjid: expect_oid(&values[5])?,
        deptype: expect_char(&values[6], "deptype")?,
    })
}

pub(crate) fn pg_description_row_from_values(
    values: Vec<Value>,
) -> Result<PgDescriptionRow, CatalogError> {
    Ok(PgDescriptionRow {
        objoid: expect_oid(&values[0])?,
        classoid: expect_oid(&values[1])?,
        objsubid: expect_int32(&values[2])?,
        description: expect_text(&values[3])?,
    })
}

pub(crate) fn pg_opclass_row_from_values(values: Vec<Value>) -> Result<PgOpclassRow, CatalogError> {
    Ok(PgOpclassRow {
        oid: expect_oid(&values[0])?,
        opcmethod: expect_oid(&values[1])?,
        opcname: expect_text(&values[2])?,
        opcnamespace: expect_oid(&values[3])?,
        opcowner: expect_oid(&values[4])?,
        opcfamily: expect_oid(&values[5])?,
        opcintype: expect_oid(&values[6])?,
        opcdefault: expect_bool(&values[7])?,
        opckeytype: expect_oid(&values[8])?,
    })
}

pub(crate) fn pg_opfamily_row_from_values(
    values: Vec<Value>,
) -> Result<PgOpfamilyRow, CatalogError> {
    Ok(PgOpfamilyRow {
        oid: expect_oid(&values[0])?,
        opfmethod: expect_oid(&values[1])?,
        opfname: expect_text(&values[2])?,
        opfnamespace: expect_oid(&values[3])?,
        opfowner: expect_oid(&values[4])?,
    })
}

pub(crate) fn pg_index_row_from_values(values: Vec<Value>) -> Result<PgIndexRow, CatalogError> {
    Ok(PgIndexRow {
        indexrelid: expect_oid(&values[0])?,
        indrelid: expect_oid(&values[1])?,
        indnatts: expect_int16(&values[2])?,
        indnkeyatts: expect_int16(&values[3])?,
        indisunique: expect_bool(&values[4])?,
        indnullsnotdistinct: expect_bool(&values[5])?,
        indisprimary: expect_bool(&values[6])?,
        indisexclusion: expect_bool(&values[7])?,
        indimmediate: expect_bool(&values[8])?,
        indisclustered: expect_bool(&values[9])?,
        indisvalid: expect_bool(&values[10])?,
        indcheckxmin: expect_bool(&values[11])?,
        indisready: expect_bool(&values[12])?,
        indislive: expect_bool(&values[13])?,
        indisreplident: expect_bool(&values[14])?,
        indkey: parse_indkey_value(&values[15])?,
        indcollation: parse_oidvector_value(&values[16])?,
        indclass: parse_oidvector_value(&values[17])?,
        indoption: parse_indkey_value(&values[18])?,
        indexprs: expect_nullable_text(&values[19])?,
        indpred: expect_nullable_text(&values[20])?,
    })
}

pub(crate) fn pg_type_row_from_values(values: Vec<Value>) -> Result<PgTypeRow, CatalogError> {
    let has_current_type_columns = values.len() >= pg_type_desc().columns.len();
    let has_new_type_columns = has_current_type_columns || values.len() >= 25;
    let has_typtype = has_new_type_columns || values.len() >= 16;
    let typbyval_idx = has_new_type_columns.then_some(5);
    let typtype_idx = if has_new_type_columns { 6 } else { 5 };
    let typisdefined_idx = if has_new_type_columns { 7 } else { 6 };
    let typalign_idx = if has_new_type_columns {
        8
    } else if has_typtype {
        7
    } else {
        5
    };
    let typstorage_idx = if has_new_type_columns {
        9
    } else if has_typtype {
        8
    } else {
        6
    };
    let typrelid_idx = if has_new_type_columns {
        10
    } else if has_typtype {
        9
    } else {
        7
    };
    let typsubscript_idx = has_new_type_columns.then_some(11);
    let typelem_idx = if has_new_type_columns {
        12
    } else if has_typtype {
        10
    } else {
        8
    };
    let typarray_idx = if has_new_type_columns {
        13
    } else if has_typtype {
        11
    } else {
        9
    };
    let typinput_idx = if has_new_type_columns {
        14
    } else if has_typtype {
        12
    } else {
        10
    };
    let typoutput_idx = if has_new_type_columns {
        15
    } else if has_typtype {
        13
    } else {
        11
    };
    let typreceive_idx = has_new_type_columns.then_some(16);
    let typsend_idx = has_new_type_columns.then_some(17);
    let typmodin_idx = has_new_type_columns.then_some(18);
    let typmodout_idx = if has_new_type_columns {
        19
    } else if has_typtype {
        14
    } else {
        12
    };
    let typdelim_idx = has_new_type_columns.then_some(20);
    let typanalyze_idx = has_new_type_columns.then_some(21);
    let typbasetype_idx = has_new_type_columns.then_some(22);
    let typcollation_idx = if has_current_type_columns {
        Some(24)
    } else {
        has_new_type_columns.then_some(23)
    };
    let typacl_idx = if has_new_type_columns {
        if has_current_type_columns { 27 } else { 24 }
    } else if has_typtype {
        15
    } else {
        13
    };
    let oid = expect_oid(&values[0])?;
    let typrelid = expect_oid(&values[typrelid_idx])?;
    let typelem = expect_oid(&values[typelem_idx])?;
    let typarray = expect_oid(&values[typarray_idx])?;
    let typalign = AttributeAlign::from_char(expect_char(&values[typalign_idx], "typalign")?)
        .ok_or(CatalogError::Corrupt("invalid typalign"))?;
    let typstorage =
        AttributeStorage::from_char(expect_char(&values[typstorage_idx], "typstorage")?)
            .ok_or(CatalogError::Corrupt("invalid typstorage"))?;
    let typinput = values
        .get(typinput_idx)
        .map(expect_nullable_oid)
        .transpose()?
        .flatten()
        .unwrap_or(0);
    let typoutput = values
        .get(typoutput_idx)
        .map(expect_nullable_oid)
        .transpose()?
        .flatten()
        .unwrap_or(0);
    let typmodout = values
        .get(typmodout_idx)
        .map(expect_nullable_oid)
        .transpose()?
        .flatten()
        .unwrap_or(0);
    Ok(PgTypeRow {
        oid,
        typname: expect_text(&values[1])?,
        typnamespace: expect_oid(&values[2])?,
        typowner: expect_oid(&values[3])?,
        typlen: expect_int16(&values[4])?,
        typbyval: typbyval_idx
            .and_then(|idx| values.get(idx))
            .map(expect_bool)
            .transpose()?
            .unwrap_or(false),
        typtype: if has_typtype {
            expect_char(&values[typtype_idx], "typtype")?
        } else {
            'b'
        },
        typisdefined: if has_typtype {
            expect_bool(&values[typisdefined_idx])?
        } else {
            true
        },
        typalign,
        typstorage,
        typrelid,
        typsubscript: typsubscript_idx
            .and_then(|idx| values.get(idx))
            .map(expect_oid)
            .transpose()?
            .unwrap_or(0),
        typelem,
        typarray,
        typinput,
        typoutput,
        typreceive: typreceive_idx
            .and_then(|idx| values.get(idx))
            .map(expect_oid)
            .transpose()?
            .unwrap_or(0),
        typsend: typsend_idx
            .and_then(|idx| values.get(idx))
            .map(expect_oid)
            .transpose()?
            .unwrap_or(0),
        typmodin: typmodin_idx
            .and_then(|idx| values.get(idx))
            .map(expect_oid)
            .transpose()?
            .unwrap_or(0),
        typmodout,
        typdelim: typdelim_idx
            .and_then(|idx| values.get(idx))
            .map(|value| expect_char(value, "typdelim"))
            .transpose()?
            .unwrap_or(','),
        typanalyze: typanalyze_idx
            .and_then(|idx| values.get(idx))
            .map(expect_oid)
            .transpose()?
            .unwrap_or(0),
        typbasetype: typbasetype_idx
            .and_then(|idx| values.get(idx))
            .map(expect_oid)
            .transpose()?
            .unwrap_or(0),
        typcollation: typcollation_idx
            .and_then(|idx| values.get(idx))
            .map(expect_oid)
            .transpose()?
            .unwrap_or(0),
        typacl: nullable_text_array(&values[typacl_idx])?,
        sql_type: decode_builtin_sql_type(oid).unwrap_or_else(|| {
            if typrelid != 0 {
                SqlType::named_composite(oid, typrelid)
            } else if typelem != 0 && typarray == 0 {
                // :HACK: Dynamic user base arrays and composite arrays both
                // persist only typelem today. Use the array row alignment to
                // keep existing composite arrays as records until pg_type rows
                // carry enough metadata to decode this directly.
                let element = decode_builtin_sql_type(typelem).unwrap_or_else(|| {
                    if matches!(typalign, AttributeAlign::Double) {
                        SqlType::record(typelem)
                    } else {
                        SqlType::new(SqlTypeKind::Text).with_identity(typelem, 0)
                    }
                });
                SqlType::array_of(element)
            } else if typelem != 0 {
                // A user-defined base type can set typelem to advertise that
                // its external syntax is array-like without being an array
                // type itself. Real array rows have typelem set and typarray 0.
                SqlType::new(SqlTypeKind::Text).with_identity(oid, 0)
            } else if typarray != 0 {
                SqlType::new(SqlTypeKind::Text).with_identity(oid, 0)
            } else {
                SqlType::new(SqlTypeKind::Shell).with_identity(oid, 0)
            }
        }),
    })
}

pub(crate) fn pg_statistic_row_from_values(
    values: Vec<Value>,
) -> Result<PgStatisticRow, CatalogError> {
    Ok(PgStatisticRow {
        starelid: expect_oid(&values[0])?,
        staattnum: expect_int16(&values[1])?,
        stainherit: expect_bool(&values[2])?,
        stanullfrac: expect_float64(&values[3])?,
        stawidth: expect_int32(&values[4])?,
        stadistinct: expect_float64(&values[5])?,
        stakind: [
            expect_int16(&values[6])?,
            expect_int16(&values[7])?,
            expect_int16(&values[8])?,
            expect_int16(&values[9])?,
            expect_int16(&values[10])?,
        ],
        staop: [
            expect_oid(&values[11])?,
            expect_oid(&values[12])?,
            expect_oid(&values[13])?,
            expect_oid(&values[14])?,
            expect_oid(&values[15])?,
        ],
        stacoll: [
            expect_oid(&values[16])?,
            expect_oid(&values[17])?,
            expect_oid(&values[18])?,
            expect_oid(&values[19])?,
            expect_oid(&values[20])?,
        ],
        stanumbers: [
            expect_nullable_array(&values[21])?,
            expect_nullable_array(&values[22])?,
            expect_nullable_array(&values[23])?,
            expect_nullable_array(&values[24])?,
            expect_nullable_array(&values[25])?,
        ],
        stavalues: [
            expect_nullable_array(&values[26])?,
            expect_nullable_array(&values[27])?,
            expect_nullable_array(&values[28])?,
            expect_nullable_array(&values[29])?,
            expect_nullable_array(&values[30])?,
        ],
    })
}

pub(crate) fn pg_statistic_ext_row_from_values(
    values: Vec<Value>,
) -> Result<PgStatisticExtRow, CatalogError> {
    Ok(PgStatisticExtRow {
        oid: expect_oid(&values[0])?,
        stxrelid: expect_oid(&values[1])?,
        stxname: expect_text(&values[2])?,
        stxnamespace: expect_oid(&values[3])?,
        stxowner: expect_oid(&values[4])?,
        stxkeys: parse_indkey_value(&values[5])?,
        stxstattarget: expect_nullable_int16(&values[6])?,
        stxkind: nullable_char_array(&values[7])?
            .ok_or(CatalogError::Corrupt("expected stxkind array"))?,
        stxexprs: expect_nullable_text(&values[8])?,
    })
}

pub(crate) fn pg_statistic_ext_data_row_from_values(
    values: Vec<Value>,
) -> Result<PgStatisticExtDataRow, CatalogError> {
    Ok(PgStatisticExtDataRow {
        stxoid: expect_oid(&values[0])?,
        stxdinherit: expect_bool(&values[1])?,
        stxdndistinct: expect_nullable_bytea(&values[2])?,
        stxddependencies: expect_nullable_bytea(&values[3])?,
        stxdmcv: expect_nullable_bytea(&values[4])?,
        stxdexpr: nullable_pg_statistic_array(&values[5])?,
    })
}

pub(crate) fn pg_rewrite_row_from_values(values: Vec<Value>) -> Result<PgRewriteRow, CatalogError> {
    Ok(PgRewriteRow {
        oid: expect_oid(&values[0])?,
        rulename: expect_text(&values[1])?,
        ev_class: expect_oid(&values[2])?,
        ev_type: expect_char(&values[3], "ev_type")?,
        ev_enabled: expect_char(&values[4], "ev_enabled")?,
        is_instead: expect_bool(&values[5])?,
        ev_qual: expect_text(&values[6])?,
        ev_action: expect_text(&values[7])?,
    })
}

fn namespace_row_values(row: PgNamespaceRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.nspname.into()),
        Value::Int32(row.nspowner as i32),
        nullable_array_value(row.nspacl.map(text_array_value)),
    ]
}

fn pg_class_row_values(row: PgClassRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.relname.into()),
        Value::Int32(row.relnamespace as i32),
        Value::Int32(row.reltype as i32),
        Value::Int32(row.relowner as i32),
        Value::Int32(row.relam as i32),
        Value::Int32(row.relfilenode as i32),
        Value::Int32(row.reltablespace as i32),
        Value::Int32(row.relpages),
        Value::Float64(row.reltuples),
        Value::Int32(row.relallvisible),
        Value::Int32(row.relallfrozen),
        Value::Int32(row.reltoastrelid as i32),
        Value::Bool(row.relhasindex),
        Value::Text(row.relpersistence.to_string().into()),
        Value::Text(row.relkind.to_string().into()),
        Value::Int16(row.relnatts),
        Value::Bool(row.relhassubclass),
        Value::Bool(row.relhastriggers),
        Value::Bool(row.relrowsecurity),
        Value::Bool(row.relforcerowsecurity),
        Value::Bool(row.relispopulated),
        Value::Bool(row.relispartition),
        Value::Int32(row.relfrozenxid as i32),
        row.relpartbound
            .map_or(Value::Null, |value| Value::Text(value.into())),
        row.reloptions
            .map(|values| Value::PgArray(text_array_value(values)))
            .unwrap_or(Value::Null),
        row.relacl
            .map(|values| Value::PgArray(text_array_value(values)))
            .unwrap_or(Value::Null),
        Value::InternalChar(row.relreplident as u8),
        Value::Int32(row.reloftype as i32),
    ]
}

fn pg_partitioned_table_row_values(row: PgPartitionedTableRow) -> Vec<Value> {
    vec![
        Value::Int32(row.partrelid as i32),
        Value::Text(row.partstrat.to_string().into()),
        Value::Int16(row.partnatts),
        Value::Int32(row.partdefid as i32),
        Value::PgArray(int16_vector_value(row.partattrs)),
        Value::PgArray(oid_vector_value(row.partclass)),
        Value::PgArray(oid_vector_value(row.partcollation)),
        nullable_text_value(row.partexprs),
    ]
}

fn pg_amop_row_values(row: PgAmopRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Int32(row.amopfamily as i32),
        Value::Int32(row.amoplefttype as i32),
        Value::Int32(row.amoprighttype as i32),
        Value::Int16(row.amopstrategy),
        Value::Text(row.amoppurpose.to_string().into()),
        Value::Int32(row.amopopr as i32),
        Value::Int32(row.amopmethod as i32),
        Value::Int32(row.amopsortfamily as i32),
    ]
}

fn pg_amproc_row_values(row: PgAmprocRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Int32(row.amprocfamily as i32),
        Value::Int32(row.amproclefttype as i32),
        Value::Int32(row.amprocrighttype as i32),
        Value::Int16(row.amprocnum),
        Value::Int32(row.amproc as i32),
    ]
}

fn pg_am_row_values(row: PgAmRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.amname.into()),
        Value::Int32(row.amhandler as i32),
        Value::Text(row.amtype.to_string().into()),
    ]
}

fn pg_authid_row_values(row: PgAuthIdRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.rolname.into()),
        Value::Bool(row.rolsuper),
        Value::Bool(row.rolinherit),
        Value::Bool(row.rolcreaterole),
        Value::Bool(row.rolcreatedb),
        Value::Bool(row.rolcanlogin),
        Value::Bool(row.rolreplication),
        Value::Bool(row.rolbypassrls),
        Value::Int32(row.rolconnlimit),
        row.rolpassword
            .map_or(Value::Null, |value| Value::Text(value.into())),
        nullable_timestamptz_value(row.rolvaliduntil),
    ]
}

fn pg_auth_members_row_values(row: PgAuthMembersRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Int32(row.roleid as i32),
        Value::Int32(row.member as i32),
        Value::Int32(row.grantor as i32),
        Value::Bool(row.admin_option),
        Value::Bool(row.inherit_option),
        Value::Bool(row.set_option),
    ]
}

fn pg_collation_row_values(row: PgCollationRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.collname.into()),
        Value::Int32(row.collnamespace as i32),
        Value::Int32(row.collowner as i32),
        Value::Text(row.collprovider.to_string().into()),
        Value::Bool(row.collisdeterministic),
        Value::Int32(row.collencoding),
    ]
}

fn pg_foreign_data_wrapper_row_values(row: PgForeignDataWrapperRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.fdwname.into()),
        Value::Int32(row.fdwowner as i32),
        Value::Int32(row.fdwhandler as i32),
        Value::Int32(row.fdwvalidator as i32),
        row.fdwacl
            .map(|values| Value::PgArray(text_array_value(values)))
            .unwrap_or(Value::Null),
        row.fdwoptions
            .map(|values| Value::PgArray(text_array_value(values)))
            .unwrap_or(Value::Null),
    ]
}

fn pg_foreign_server_row_values(row: PgForeignServerRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.srvname.into()),
        Value::Int32(row.srvowner as i32),
        Value::Int32(row.srvfdw as i32),
        nullable_text_value(row.srvtype),
        nullable_text_value(row.srvversion),
        row.srvacl
            .map(|values| Value::PgArray(text_array_value(values)))
            .unwrap_or(Value::Null),
        row.srvoptions
            .map(|values| Value::PgArray(text_array_value(values)))
            .unwrap_or(Value::Null),
    ]
}

fn pg_user_mapping_row_values(row: PgUserMappingRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Int32(row.umuser as i32),
        Value::Int32(row.umserver as i32),
        row.umoptions
            .map(|values| Value::PgArray(text_array_value(values)))
            .unwrap_or(Value::Null),
    ]
}

fn pg_foreign_table_row_values(row: PgForeignTableRow) -> Vec<Value> {
    vec![
        Value::Int32(row.ftrelid as i32),
        Value::Int32(row.ftserver as i32),
        row.ftoptions
            .map(|values| Value::PgArray(text_array_value(values)))
            .unwrap_or(Value::Null),
    ]
}

fn pg_language_row_values(row: PgLanguageRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.lanname.into()),
        Value::Int32(row.lanowner as i32),
        Value::Bool(row.lanispl),
        Value::Bool(row.lanpltrusted),
        Value::Int32(row.lanplcallfoid as i32),
        Value::Int32(row.laninline as i32),
        Value::Int32(row.lanvalidator as i32),
    ]
}

fn pg_ts_parser_row_values(row: PgTsParserRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.prsname.into()),
        Value::Int32(row.prsnamespace as i32),
        Value::Int32(row.prsstart as i32),
        Value::Int32(row.prstoken as i32),
        Value::Int32(row.prsend as i32),
        row.prsheadline
            .map(|oid| Value::Int32(oid as i32))
            .unwrap_or(Value::Null),
        Value::Int32(row.prslextype as i32),
    ]
}

fn pg_ts_template_row_values(row: PgTsTemplateRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.tmplname.into()),
        Value::Int32(row.tmplnamespace as i32),
        row.tmplinit
            .map(|oid| Value::Int32(oid as i32))
            .unwrap_or(Value::Null),
        Value::Int32(row.tmpllexize as i32),
    ]
}

fn pg_ts_dict_row_values(row: PgTsDictRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.dictname.into()),
        Value::Int32(row.dictnamespace as i32),
        Value::Int32(row.dictowner as i32),
        Value::Int32(row.dicttemplate as i32),
        row.dictinitoption
            .map(|text| Value::Text(text.into()))
            .unwrap_or(Value::Null),
    ]
}

fn pg_ts_config_row_values(row: PgTsConfigRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.cfgname.into()),
        Value::Int32(row.cfgnamespace as i32),
        Value::Int32(row.cfgowner as i32),
        Value::Int32(row.cfgparser as i32),
    ]
}

fn pg_ts_config_map_row_values(row: PgTsConfigMapRow) -> Vec<Value> {
    vec![
        Value::Int32(row.mapcfg as i32),
        Value::Int32(row.maptokentype),
        Value::Int32(row.mapseqno),
        Value::Int32(row.mapdict as i32),
    ]
}

fn pg_sequence_row_values(row: PgSequenceRow) -> Vec<Value> {
    vec![
        Value::Int32(row.seqrelid as i32),
        Value::Int32(row.seqtypid as i32),
        Value::Int64(row.seqstart),
        Value::Int64(row.seqincrement),
        Value::Int64(row.seqmax),
        Value::Int64(row.seqmin),
        Value::Int64(row.seqcache),
        Value::Bool(row.seqcycle),
    ]
}

fn pg_proc_row_values(row: PgProcRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.proname.into()),
        Value::Int32(row.pronamespace as i32),
        Value::Int32(row.proowner as i32),
        Value::Int32(row.prolang as i32),
        Value::Float64(row.procost),
        Value::Float64(row.prorows),
        Value::Int32(row.provariadic as i32),
        Value::Int32(row.prosupport as i32),
        Value::Text(row.prokind.to_string().into()),
        Value::Bool(row.prosecdef),
        Value::Bool(row.proleakproof),
        Value::Bool(row.proisstrict),
        Value::Bool(row.proretset),
        Value::Text(row.provolatile.to_string().into()),
        Value::Text(row.proparallel.to_string().into()),
        Value::Int16(row.pronargs),
        Value::Int16(row.pronargdefaults),
        Value::Int32(row.prorettype as i32),
        Value::PgArray(oid_vector_value(parse_oidvector(&row.proargtypes))),
        nullable_array_value(row.proallargtypes.map(|oids| {
            ArrayValue::from_1d(
                oids.into_iter()
                    .map(|oid| Value::Int32(oid as i32))
                    .collect(),
            )
            .with_element_type_oid(crate::include::catalog::OID_TYPE_OID)
        })),
        nullable_array_value(row.proargmodes.map(|modes| {
            ArrayValue::from_1d(
                modes
                    .into_iter()
                    .map(Value::InternalChar)
                    .collect::<Vec<_>>(),
            )
            .with_element_type_oid(crate::include::catalog::INTERNAL_CHAR_TYPE_OID)
        })),
        nullable_array_value(row.proargnames.map(|names| {
            ArrayValue::from_1d(
                names
                    .into_iter()
                    .map(|name| Value::Text(name.into()))
                    .collect::<Vec<_>>(),
            )
            .with_element_type_oid(crate::include::catalog::TEXT_TYPE_OID)
        })),
        nullable_text_value(row.proargdefaults),
        Value::Text(row.prosrc.into()),
        nullable_text_value(row.probin),
        nullable_text_value(row.prosqlbody),
        nullable_array_value(row.proconfig.map(text_array_value)),
        nullable_array_value(row.proacl.map(text_array_value)),
    ]
}

fn pg_conversion_row_values(row: PgConversionRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.conname.into()),
        Value::Int32(row.connamespace as i32),
        Value::Int32(row.conowner as i32),
        Value::Int32(row.conforencoding),
        Value::Int32(row.contoencoding),
        Value::Int32(row.conproc as i32),
        Value::Bool(row.condefault),
    ]
}

fn pg_aggregate_row_values(row: PgAggregateRow) -> Vec<Value> {
    vec![
        Value::Int32(row.aggfnoid as i32),
        Value::InternalChar(row.aggkind as u8),
        Value::Int16(row.aggnumdirectargs),
        Value::Int32(row.aggtransfn as i32),
        Value::Int32(row.aggfinalfn as i32),
        Value::Int32(row.aggcombinefn as i32),
        Value::Int32(row.aggserialfn as i32),
        Value::Int32(row.aggdeserialfn as i32),
        Value::Int32(row.aggmtransfn as i32),
        Value::Int32(row.aggminvtransfn as i32),
        Value::Int32(row.aggmfinalfn as i32),
        Value::Bool(row.aggfinalextra),
        Value::Bool(row.aggmfinalextra),
        Value::InternalChar(row.aggfinalmodify as u8),
        Value::InternalChar(row.aggmfinalmodify as u8),
        Value::Int32(row.aggsortop as i32),
        Value::Int32(row.aggtranstype as i32),
        Value::Int32(row.aggtransspace),
        Value::Int32(row.aggmtranstype as i32),
        Value::Int32(row.aggmtransspace),
        nullable_text_value(row.agginitval),
        nullable_text_value(row.aggminitval),
    ]
}

fn pg_operator_row_values(row: PgOperatorRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.oprname.into()),
        Value::Int32(row.oprnamespace as i32),
        Value::Int32(row.oprowner as i32),
        Value::Text(row.oprkind.to_string().into()),
        Value::Bool(row.oprcanmerge),
        Value::Bool(row.oprcanhash),
        Value::Int32(row.oprleft as i32),
        Value::Int32(row.oprright as i32),
        Value::Int32(row.oprresult as i32),
        Value::Int32(row.oprcom as i32),
        Value::Int32(row.oprnegate as i32),
        Value::Int32(row.oprcode as i32),
        Value::Int32(row.oprrest as i32),
        Value::Int32(row.oprjoin as i32),
    ]
}

fn pg_cast_row_values(row: PgCastRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Int32(row.castsource as i32),
        Value::Int32(row.casttarget as i32),
        Value::Int32(row.castfunc as i32),
        Value::Text(row.castcontext.to_string().into()),
        Value::Text(row.castmethod.to_string().into()),
    ]
}

fn pg_constraint_row_values(row: PgConstraintRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.conname.into()),
        Value::Int32(row.connamespace as i32),
        Value::Text(row.contype.to_string().into()),
        Value::Bool(row.condeferrable),
        Value::Bool(row.condeferred),
        Value::Bool(row.conenforced),
        Value::Bool(row.convalidated),
        Value::Int32(row.conrelid as i32),
        Value::Int32(row.contypid as i32),
        Value::Int32(row.conindid as i32),
        Value::Int32(row.conparentid as i32),
        Value::Int32(row.confrelid as i32),
        Value::Text(row.confupdtype.to_string().into()),
        Value::Text(row.confdeltype.to_string().into()),
        Value::Text(row.confmatchtype.to_string().into()),
        nullable_array_value(row.conkey.map(int16_array_value)),
        nullable_array_value(row.confkey.map(int16_array_value)),
        nullable_array_value(row.conpfeqop.map(oid_array_value)),
        nullable_array_value(row.conppeqop.map(oid_array_value)),
        nullable_array_value(row.conffeqop.map(oid_array_value)),
        nullable_array_value(row.confdelsetcols.map(int16_array_value)),
        nullable_array_value(row.conexclop.map(oid_array_value)),
        nullable_text_value(row.conbin),
        Value::Bool(row.conislocal),
        Value::Int16(row.coninhcount),
        Value::Bool(row.connoinherit),
        Value::Bool(row.conperiod),
    ]
}

fn pg_database_row_values(row: PgDatabaseRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.datname.into()),
        Value::Int32(row.datdba as i32),
        Value::Int32(row.encoding),
        Value::InternalChar(row.datlocprovider as u8),
        Value::Int32(row.dattablespace as i32),
        Value::Bool(row.datistemplate),
        Value::Bool(row.datallowconn),
        Value::Int32(row.datconnlimit),
        Value::Text(row.datcollate.into()),
        Value::Text(row.datctype.into()),
        row.datlocale
            .map_or(Value::Null, |value| Value::Text(value.into())),
        row.daticurules
            .map_or(Value::Null, |value| Value::Text(value.into())),
        row.datcollversion
            .map_or(Value::Null, |value| Value::Text(value.into())),
        row.datacl.map_or(Value::Null, |values| {
            Value::PgArray(ArrayValue::from_1d(
                values
                    .into_iter()
                    .map(|value| Value::Text(value.into()))
                    .collect(),
            ))
        }),
        Value::Bool(row.dathasloginevt),
    ]
}

fn pg_tablespace_row_values(row: PgTablespaceRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.spcname.into()),
        Value::Int32(row.spcowner as i32),
        nullable_array_value(row.spcacl.map(text_array_value)),
        nullable_array_value(row.spcoptions.map(text_array_value)),
    ]
}

fn pg_attribute_row_values(row: PgAttributeRow) -> Vec<Value> {
    vec![
        Value::Int32(row.attrelid as i32),
        Value::Text(row.attname.into()),
        Value::Int32(row.atttypid as i32),
        Value::Int16(row.attlen),
        Value::Int16(row.attnum),
        Value::Bool(row.attnotnull),
        Value::Bool(row.attisdropped),
        Value::Int32(row.atttypmod),
        Value::InternalChar(row.attalign.as_char() as u8),
        Value::InternalChar(row.attstorage.as_char() as u8),
        Value::InternalChar(row.attcompression.as_char() as u8),
        Value::Int16(row.attstattarget),
        Value::Int16(row.attinhcount),
        Value::Bool(row.attislocal),
        Value::InternalChar(row.attidentity as u8),
        Value::InternalChar(row.attgenerated as u8),
        Value::Int32(row.attcollation as i32),
        row.attacl.map_or(Value::Null, |values| {
            Value::PgArray(ArrayValue::from_1d(
                values
                    .into_iter()
                    .map(|value| Value::Text(value.into()))
                    .collect(),
            ))
        }),
        row.attoptions.map_or(Value::Null, |values| {
            Value::PgArray(ArrayValue::from_1d(
                values
                    .into_iter()
                    .map(|value| Value::Text(value.into()))
                    .collect(),
            ))
        }),
        row.attfdwoptions.map_or(Value::Null, |values| {
            Value::PgArray(ArrayValue::from_1d(
                values
                    .into_iter()
                    .map(|value| Value::Text(value.into()))
                    .collect(),
            ))
        }),
        row.attmissingval.map_or(Value::Null, |values| {
            Value::PgArray(ArrayValue::from_1d(values))
        }),
        Value::Bool(row.attbyval),
    ]
}

fn pg_inherits_row_values(row: PgInheritsRow) -> Vec<Value> {
    vec![
        Value::Int32(row.inhrelid as i32),
        Value::Int32(row.inhparent as i32),
        Value::Int32(row.inhseqno),
        Value::Bool(row.inhdetachpending),
    ]
}

fn pg_type_row_values(row: PgTypeRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.typname.into()),
        Value::Int32(row.typnamespace as i32),
        Value::Int32(row.typowner as i32),
        Value::Int16(row.typlen),
        Value::Bool(row.typbyval),
        Value::InternalChar(row.typtype as u8),
        Value::Bool(row.typisdefined),
        Value::InternalChar(row.typalign.as_char() as u8),
        Value::InternalChar(row.typstorage.as_char() as u8),
        Value::Int32(row.typrelid as i32),
        Value::Int32(row.typsubscript as i32),
        Value::Int32(row.typelem as i32),
        Value::Int32(row.typarray as i32),
        Value::Int32(row.typinput as i32),
        Value::Int32(row.typoutput as i32),
        Value::Int32(row.typreceive as i32),
        Value::Int32(row.typsend as i32),
        Value::Int32(row.typmodin as i32),
        Value::Int32(row.typmodout as i32),
        Value::InternalChar(row.typdelim as u8),
        Value::Int32(row.typanalyze as i32),
        Value::Int32(row.typbasetype as i32),
        Value::Int32(row.sql_type.typmod),
        Value::Int32(row.typcollation as i32),
        Value::Bool(false),
        Value::Null,
        nullable_array_value(row.typacl.map(text_array_value)),
    ]
}

fn pg_rewrite_row_values(row: PgRewriteRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.rulename.into()),
        Value::Int32(row.ev_class as i32),
        Value::Text(row.ev_type.to_string().into()),
        Value::Text(row.ev_enabled.to_string().into()),
        Value::Bool(row.is_instead),
        Value::Text(row.ev_qual.into()),
        Value::Text(row.ev_action.into()),
    ]
}

fn pg_trigger_row_values(row: PgTriggerRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Int32(row.tgrelid as i32),
        Value::Int32(row.tgparentid as i32),
        Value::Text(row.tgname.into()),
        Value::Int32(row.tgfoid as i32),
        Value::Int16(row.tgtype),
        Value::InternalChar(row.tgenabled as u8),
        Value::Bool(row.tgisinternal),
        Value::Int32(row.tgconstrrelid as i32),
        Value::Int32(row.tgconstrindid as i32),
        Value::Int32(row.tgconstraint as i32),
        Value::Bool(row.tgdeferrable),
        Value::Bool(row.tginitdeferred),
        Value::Int16(row.tgnargs),
        Value::PgArray(int16_array_value(row.tgattr)),
        Value::PgArray(ArrayValue::from_1d(
            row.tgargs
                .into_iter()
                .map(|arg| Value::Text(arg.into()))
                .collect::<Vec<_>>(),
        )),
        nullable_text_value(row.tgqual),
        nullable_text_value(row.tgoldtable),
        nullable_text_value(row.tgnewtable),
    ]
}

fn pg_event_trigger_row_values(row: PgEventTriggerRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.evtname.into()),
        Value::Text(row.evtevent.into()),
        Value::Int32(row.evtowner as i32),
        Value::Int32(row.evtfoid as i32),
        Value::InternalChar(row.evtenabled as u8),
        nullable_array_value(row.evttags.map(text_array_value)),
    ]
}

fn pg_publication_row_values(row: PgPublicationRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.pubname.into()),
        Value::Int32(row.pubowner as i32),
        Value::Bool(row.puballtables),
        Value::Bool(row.puballsequences),
        Value::Bool(row.pubinsert),
        Value::Bool(row.pubupdate),
        Value::Bool(row.pubdelete),
        Value::Bool(row.pubtruncate),
        Value::Bool(row.pubviaroot),
        Value::InternalChar(row.pubgencols as u8),
    ]
}

fn pg_publication_rel_row_values(row: PgPublicationRelRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Int32(row.prpubid as i32),
        Value::Int32(row.prrelid as i32),
        Value::Bool(row.prexcept),
        nullable_text_value(row.prqual),
        nullable_array_value(row.prattrs.map(int16_vector_value)),
    ]
}

fn pg_publication_namespace_row_values(row: PgPublicationNamespaceRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Int32(row.pnpubid as i32),
        Value::Int32(row.pnnspid as i32),
    ]
}

fn pg_policy_row_values(row: PgPolicyRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Text(row.polname.into()),
        Value::Int32(row.polrelid as i32),
        Value::InternalChar(row.polcmd.as_char() as u8),
        Value::Bool(row.polpermissive),
        Value::PgArray(oid_array_value(row.polroles)),
        nullable_text_value(row.polqual),
        nullable_text_value(row.polwithcheck),
    ]
}

fn pg_statistic_row_values(row: PgStatisticRow) -> Vec<Value> {
    vec![
        Value::Int32(row.starelid as i32),
        Value::Int16(row.staattnum),
        Value::Bool(row.stainherit),
        Value::Float64(row.stanullfrac),
        Value::Int32(row.stawidth),
        Value::Float64(row.stadistinct),
        Value::Int16(row.stakind[0]),
        Value::Int16(row.stakind[1]),
        Value::Int16(row.stakind[2]),
        Value::Int16(row.stakind[3]),
        Value::Int16(row.stakind[4]),
        Value::Int32(row.staop[0] as i32),
        Value::Int32(row.staop[1] as i32),
        Value::Int32(row.staop[2] as i32),
        Value::Int32(row.staop[3] as i32),
        Value::Int32(row.staop[4] as i32),
        Value::Int32(row.stacoll[0] as i32),
        Value::Int32(row.stacoll[1] as i32),
        Value::Int32(row.stacoll[2] as i32),
        Value::Int32(row.stacoll[3] as i32),
        Value::Int32(row.stacoll[4] as i32),
        nullable_array_value(row.stanumbers[0].clone()),
        nullable_array_value(row.stanumbers[1].clone()),
        nullable_array_value(row.stanumbers[2].clone()),
        nullable_array_value(row.stanumbers[3].clone()),
        nullable_array_value(row.stanumbers[4].clone()),
        nullable_array_value(row.stavalues[0].clone()),
        nullable_array_value(row.stavalues[1].clone()),
        nullable_array_value(row.stavalues[2].clone()),
        nullable_array_value(row.stavalues[3].clone()),
        nullable_array_value(row.stavalues[4].clone()),
    ]
}

fn pg_statistic_ext_row_values(row: PgStatisticExtRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Int32(row.stxrelid as i32),
        Value::Text(row.stxname.into()),
        Value::Int32(row.stxnamespace as i32),
        Value::Int32(row.stxowner as i32),
        Value::PgArray(int16_vector_value(row.stxkeys)),
        nullable_int16_value(row.stxstattarget),
        Value::PgArray(
            ArrayValue::from_1d(row.stxkind.into_iter().map(Value::InternalChar).collect())
                .with_element_type_oid(crate::include::catalog::INTERNAL_CHAR_TYPE_OID),
        ),
        nullable_text_value(row.stxexprs),
    ]
}

fn pg_statistic_ext_data_row_values(row: PgStatisticExtDataRow) -> Vec<Value> {
    vec![
        Value::Int32(row.stxoid as i32),
        Value::Bool(row.stxdinherit),
        nullable_bytea_value(row.stxdndistinct),
        nullable_bytea_value(row.stxddependencies),
        nullable_bytea_value(row.stxdmcv),
        nullable_pg_statistic_array_value(row.stxdexpr),
    ]
}

fn pg_attrdef_row_values(row: PgAttrdefRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Int32(row.adrelid as i32),
        Value::Int16(row.adnum),
        Value::Text(row.adbin.into()),
    ]
}

fn pg_depend_row_values(row: PgDependRow) -> Vec<Value> {
    vec![
        Value::Int32(row.classid as i32),
        Value::Int32(row.objid as i32),
        Value::Int32(row.objsubid),
        Value::Int32(row.refclassid as i32),
        Value::Int32(row.refobjid as i32),
        Value::Int32(row.refobjsubid),
        Value::Text(row.deptype.to_string().into()),
    ]
}

fn pg_shdepend_row_values(row: PgShdependRow) -> Vec<Value> {
    vec![
        Value::Int32(row.dbid as i32),
        Value::Int32(row.classid as i32),
        Value::Int32(row.objid as i32),
        Value::Int32(row.objsubid),
        Value::Int32(row.refclassid as i32),
        Value::Int32(row.refobjid as i32),
        Value::Text(row.deptype.to_string().into()),
    ]
}

fn pg_description_row_values(row: PgDescriptionRow) -> Vec<Value> {
    vec![
        Value::Int32(row.objoid as i32),
        Value::Int32(row.classoid as i32),
        Value::Int32(row.objsubid),
        Value::Text(row.description.into()),
    ]
}

fn pg_index_row_values(row: PgIndexRow) -> Vec<Value> {
    vec![
        Value::Int32(row.indexrelid as i32),
        Value::Int32(row.indrelid as i32),
        Value::Int16(row.indnatts),
        Value::Int16(row.indnkeyatts),
        Value::Bool(row.indisunique),
        Value::Bool(row.indnullsnotdistinct),
        Value::Bool(row.indisprimary),
        Value::Bool(row.indisexclusion),
        Value::Bool(row.indimmediate),
        Value::Bool(row.indisclustered),
        Value::Bool(row.indisvalid),
        Value::Bool(row.indcheckxmin),
        Value::Bool(row.indisready),
        Value::Bool(row.indislive),
        Value::Bool(row.indisreplident),
        Value::PgArray(int16_vector_value(row.indkey)),
        Value::PgArray(oid_vector_value(row.indcollation)),
        Value::PgArray(oid_vector_value(row.indclass)),
        Value::PgArray(int16_vector_value(row.indoption)),
        row.indexprs.map_or(Value::Null, |v| Value::Text(v.into())),
        row.indpred.map_or(Value::Null, |v| Value::Text(v.into())),
    ]
}

fn pg_opclass_row_values(row: PgOpclassRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Int32(row.opcmethod as i32),
        Value::Text(row.opcname.into()),
        Value::Int32(row.opcnamespace as i32),
        Value::Int32(row.opcowner as i32),
        Value::Int32(row.opcfamily as i32),
        Value::Int32(row.opcintype as i32),
        Value::Bool(row.opcdefault),
        Value::Int32(row.opckeytype as i32),
    ]
}

fn pg_opfamily_row_values(row: PgOpfamilyRow) -> Vec<Value> {
    vec![
        Value::Int32(row.oid as i32),
        Value::Int32(row.opfmethod as i32),
        Value::Text(row.opfname.into()),
        Value::Int32(row.opfnamespace as i32),
        Value::Int32(row.opfowner as i32),
    ]
}

fn decode_builtin_sql_type(oid: u32) -> Option<SqlType> {
    builtin_sql_type_by_oid().get(&oid).copied()
}

fn builtin_sql_type_by_oid() -> &'static HashMap<u32, SqlType> {
    static BUILTIN_SQL_TYPE_BY_OID: OnceLock<HashMap<u32, SqlType>> = OnceLock::new();

    BUILTIN_SQL_TYPE_BY_OID.get_or_init(|| {
        builtin_type_rows()
            .into_iter()
            .chain(bootstrap_composite_type_rows())
            .map(|row| (row.oid, row.sql_type))
            .collect()
    })
}

fn expect_oid(value: &Value) -> Result<u32, CatalogError> {
    match value {
        Value::Int64(v) => {
            u32::try_from(*v).map_err(|_| CatalogError::Corrupt("invalid oid value"))
        }
        Value::Int32(v) => {
            u32::try_from(*v).map_err(|_| CatalogError::Corrupt("invalid oid value"))
        }
        _ => Err(CatalogError::Corrupt("expected oid value")),
    }
}

fn expect_nullable_oid(value: &Value) -> Result<Option<u32>, CatalogError> {
    match value {
        Value::Null => Ok(None),
        other => expect_oid(other).map(Some),
    }
}

fn expect_text(value: &Value) -> Result<String, CatalogError> {
    match value {
        Value::Text(text) => Ok(text.to_string()),
        _ => Err(CatalogError::Corrupt("expected text value")),
    }
}

fn expect_nullable_text(value: &Value) -> Result<Option<String>, CatalogError> {
    match value {
        Value::Null => Ok(None),
        Value::Text(text) => Ok(Some(text.to_string())),
        _ => Err(CatalogError::Corrupt("expected nullable text value")),
    }
}

fn expect_bool(value: &Value) -> Result<bool, CatalogError> {
    match value {
        Value::Bool(v) => Ok(*v),
        _ => Err(CatalogError::Corrupt("expected bool value")),
    }
}

fn expect_int16(value: &Value) -> Result<i16, CatalogError> {
    match value {
        Value::Int16(v) => Ok(*v),
        _ => Err(CatalogError::Corrupt("expected int2 value")),
    }
}

fn expect_nullable_int16(value: &Value) -> Result<Option<i16>, CatalogError> {
    match value {
        Value::Null => Ok(None),
        other => expect_int16(other).map(Some),
    }
}

fn expect_int32(value: &Value) -> Result<i32, CatalogError> {
    match value {
        Value::Int32(v) => Ok(*v),
        _ => Err(CatalogError::Corrupt("expected int4 value")),
    }
}

fn expect_int64(value: &Value) -> Result<i64, CatalogError> {
    match value {
        Value::Int64(v) => Ok(*v),
        Value::Int32(v) => Ok(i64::from(*v)),
        _ => Err(CatalogError::Corrupt("expected int8 value")),
    }
}

fn expect_float64(value: &Value) -> Result<f64, CatalogError> {
    match value {
        Value::Float64(v) => Ok(*v),
        _ => Err(CatalogError::Corrupt("expected float value")),
    }
}

fn expect_nullable_array(value: &Value) -> Result<Option<ArrayValue>, CatalogError> {
    match value {
        Value::Null => Ok(None),
        Value::PgArray(array) => Ok(Some(array.clone())),
        _ => Err(CatalogError::Corrupt("expected nullable array value")),
    }
}

fn expect_nullable_bytea(value: &Value) -> Result<Option<Vec<u8>>, CatalogError> {
    match value {
        Value::Null => Ok(None),
        Value::Bytea(bytes) => Ok(Some(bytes.clone())),
        _ => Err(CatalogError::Corrupt("expected nullable bytea value")),
    }
}

fn nullable_array_value(value: Option<ArrayValue>) -> Value {
    value.map(Value::PgArray).unwrap_or(Value::Null)
}

fn nullable_bytea_value(value: Option<Vec<u8>>) -> Value {
    value.map(Value::Bytea).unwrap_or(Value::Null)
}

fn nullable_oid_array(value: &Value) -> Result<Option<Vec<u32>>, CatalogError> {
    let Some(array) = expect_nullable_array(value)? else {
        return Ok(None);
    };
    array
        .elements
        .into_iter()
        .map(|value| match value {
            Value::Int32(v) if v >= 0 => Ok(v as u32),
            _ => Err(CatalogError::Corrupt("expected oid array value")),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn nullable_int16_array(value: &Value) -> Result<Option<Vec<i16>>, CatalogError> {
    let Some(array) = expect_nullable_array(value)? else {
        return Ok(None);
    };
    array
        .elements
        .into_iter()
        .map(|value| match value {
            Value::Int16(v) => Ok(v),
            _ => Err(CatalogError::Corrupt("expected int2 array value")),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn nullable_text_array(value: &Value) -> Result<Option<Vec<String>>, CatalogError> {
    let Some(array) = expect_nullable_array(value)? else {
        return Ok(None);
    };
    array
        .elements
        .into_iter()
        .map(|value| match value {
            Value::Text(text) => Ok(text.to_string()),
            _ => Err(CatalogError::Corrupt("expected text array value")),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn nullable_any_array(value: &Value) -> Result<Option<Vec<Value>>, CatalogError> {
    let Some(array) = expect_nullable_array(value)? else {
        return Ok(None);
    };
    Ok(Some(array.elements))
}

fn nullable_char_array(value: &Value) -> Result<Option<Vec<u8>>, CatalogError> {
    let Some(array) = expect_nullable_array(value)? else {
        return Ok(None);
    };
    array
        .elements
        .into_iter()
        .map(|value| match value {
            Value::InternalChar(v) => Ok(v),
            Value::Text(text) if text.len() == 1 => Ok(text.as_bytes()[0]),
            _ => Err(CatalogError::Corrupt("expected char array value")),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn nullable_text(value: &Value) -> Result<Option<String>, CatalogError> {
    match value {
        Value::Null => Ok(None),
        Value::Text(text) => Ok(Some(text.to_string())),
        _ => Err(CatalogError::Corrupt("expected nullable text value")),
    }
}

fn nullable_timestamptz(value: &Value) -> Result<Option<TimestampTzADT>, CatalogError> {
    match value {
        Value::Null => Ok(None),
        Value::TimestampTz(timestamp) => Ok(Some(*timestamp)),
        _ => Err(CatalogError::Corrupt("expected nullable timestamptz value")),
    }
}

fn nullable_pg_statistic_array(value: &Value) -> Result<Option<Vec<PgStatisticRow>>, CatalogError> {
    let Some(array) = expect_nullable_array(value)? else {
        return Ok(None);
    };
    array
        .elements
        .into_iter()
        .map(|element| match element {
            Value::Record(record) => {
                let values = record.fields;
                pg_statistic_row_from_values(values)
            }
            _ => Err(CatalogError::Corrupt("expected pg_statistic[] element")),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

fn oid_array_value(values: Vec<u32>) -> ArrayValue {
    ArrayValue::from_1d(
        values
            .into_iter()
            .map(|oid| Value::Int32(oid as i32))
            .collect(),
    )
    .with_element_type_oid(crate::include::catalog::OID_TYPE_OID)
}

fn int16_array_value(values: Vec<i16>) -> ArrayValue {
    ArrayValue::from_1d(values.into_iter().map(Value::Int16).collect())
        .with_element_type_oid(crate::include::catalog::INT2_TYPE_OID)
}

fn oid_vector_value(values: Vec<u32>) -> ArrayValue {
    let length = values.len();
    ArrayValue::from_dimensions(
        vector_dimensions(length),
        values
            .into_iter()
            .map(|oid| Value::Int32(oid as i32))
            .collect(),
    )
    .with_element_type_oid(crate::include::catalog::OID_TYPE_OID)
}

fn int16_vector_value(values: Vec<i16>) -> ArrayValue {
    let length = values.len();
    ArrayValue::from_dimensions(
        vector_dimensions(length),
        values.into_iter().map(Value::Int16).collect(),
    )
    .with_element_type_oid(crate::include::catalog::INT2_TYPE_OID)
}

fn vector_dimensions(length: usize) -> Vec<ArrayDimension> {
    vec![ArrayDimension {
        lower_bound: 0,
        length,
    }]
}

fn text_array_value(values: Vec<String>) -> ArrayValue {
    ArrayValue::from_1d(
        values
            .into_iter()
            .map(|value| Value::Text(value.into()))
            .collect(),
    )
    .with_element_type_oid(crate::include::catalog::TEXT_TYPE_OID)
}

fn nullable_int16_value(value: Option<i16>) -> Value {
    value.map(Value::Int16).unwrap_or(Value::Null)
}

fn nullable_pg_statistic_array_value(value: Option<Vec<PgStatisticRow>>) -> Value {
    let Some(rows) = value else {
        return Value::Null;
    };
    Value::PgArray(
        ArrayValue::from_1d(
            rows.into_iter()
                .map(|row| Value::Record(pg_statistic_record_value(row)))
                .collect(),
        )
        .with_element_type_oid(PG_STATISTIC_ROWTYPE_OID),
    )
}

fn pg_statistic_record_value(row: PgStatisticRow) -> RecordValue {
    let values = pg_statistic_row_values(row);
    let desc = crate::include::catalog::pg_statistic_desc();
    RecordValue::named(
        PG_STATISTIC_ROWTYPE_OID,
        PG_STATISTIC_RELATION_OID,
        -1,
        desc.columns
            .into_iter()
            .zip(values)
            .map(|(column, value)| (column.name, value))
            .collect(),
    )
}

fn nullable_text_value(value: Option<String>) -> Value {
    value
        .map(|text| Value::Text(text.into()))
        .unwrap_or(Value::Null)
}

fn nullable_timestamptz_value(value: Option<TimestampTzADT>) -> Value {
    value.map(Value::TimestampTz).unwrap_or(Value::Null)
}

fn expect_char(value: &Value, label: &'static str) -> Result<char, CatalogError> {
    match value {
        Value::Text(text) => text
            .chars()
            .next()
            .ok_or(CatalogError::Corrupt(match label {
                "relpersistence" => "empty relpersistence",
                "relkind" => "empty relkind",
                "amtype" => "empty amtype",
                "oprkind" => "empty oprkind",
                "prokind" => "empty prokind",
                "provolatile" => "empty provolatile",
                "proparallel" => "empty proparallel",
                "collprovider" => "empty collprovider",
                "castcontext" => "empty castcontext",
                "castmethod" => "empty castmethod",
                "contype" => "empty contype",
                "confupdtype" => "empty confupdtype",
                "confdeltype" => "empty confdeltype",
                "confmatchtype" => "empty confmatchtype",
                "deptype" => "empty deptype",
                "attalign" => "empty attalign",
                "attstorage" => "empty attstorage",
                "attcompression" => "empty attcompression",
                "attidentity" => "empty attidentity",
                "attgenerated" => "empty attgenerated",
                _ => "empty char value",
            })),
        Value::InternalChar(byte) => Ok(char::from(*byte)),
        _ => Err(CatalogError::Corrupt(match label {
            "relpersistence" => "expected relpersistence text",
            "relkind" => "expected relkind text",
            "amtype" => "expected amtype text",
            "oprkind" => "expected oprkind text",
            "prokind" => "expected prokind text",
            "provolatile" => "expected provolatile text",
            "proparallel" => "expected proparallel text",
            "collprovider" => "expected collprovider text",
            "castcontext" => "expected castcontext text",
            "castmethod" => "expected castmethod text",
            "contype" => "expected contype text",
            "confupdtype" => "expected confupdtype text",
            "confdeltype" => "expected confdeltype text",
            "confmatchtype" => "expected confmatchtype text",
            "deptype" => "expected deptype text",
            "attalign" => "expected attalign text",
            "attstorage" => "expected attstorage text",
            "attcompression" => "expected attcompression text",
            "attidentity" => "expected attidentity text",
            "attgenerated" => "expected attgenerated text",
            _ => "expected text char value",
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::executor::value_io::tuple_from_values;
    use crate::include::catalog::{BootstrapCatalogKind, bootstrap_relation_desc};

    #[test]
    fn pg_statistic_anyarray_catalog_tuple_roundtrips() {
        let row = PgStatisticRow {
            starelid: 42,
            staattnum: 1,
            stainherit: false,
            stanullfrac: 0.2,
            stawidth: 4,
            stadistinct: 3.0,
            stakind: [1, 2, 3, 0, 0],
            staop: [96, 97, 98, 0, 0],
            stacoll: [0; 5],
            stanumbers: [
                Some(
                    ArrayValue::from_1d(vec![Value::Float64(0.5)])
                        .with_element_type_oid(crate::include::catalog::FLOAT4_TYPE_OID),
                ),
                None,
                Some(
                    ArrayValue::from_1d(vec![Value::Float64(1.0)])
                        .with_element_type_oid(crate::include::catalog::FLOAT4_TYPE_OID),
                ),
                None,
                None,
            ],
            stavalues: [
                Some(
                    ArrayValue::from_1d(vec![Value::Int32(1), Value::Int32(2)])
                        .with_element_type_oid(crate::include::catalog::INT4_TYPE_OID),
                ),
                Some(
                    ArrayValue::from_1d(vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)])
                        .with_element_type_oid(crate::include::catalog::INT4_TYPE_OID),
                ),
                None,
                None,
                None,
            ],
        };

        let values = pg_statistic_row_values(row.clone());
        let desc = bootstrap_relation_desc(BootstrapCatalogKind::PgStatistic);
        let tuple = tuple_from_values(&desc, &values).unwrap();
        let decoded = decode_catalog_tuple_values(&desc, &tuple).unwrap();
        let roundtrip = pg_statistic_row_from_values(decoded).unwrap();

        assert_eq!(roundtrip.starelid, row.starelid);
        assert_eq!(roundtrip.staattnum, row.staattnum);
        assert_eq!(roundtrip.stainherit, row.stainherit);
        assert!((roundtrip.stanullfrac - row.stanullfrac).abs() < 1e-6);
        assert_eq!(roundtrip.stawidth, row.stawidth);
        assert_eq!(roundtrip.stadistinct, row.stadistinct);
        assert_eq!(roundtrip.stakind, row.stakind);
        assert_eq!(roundtrip.staop, row.staop);
        assert_eq!(roundtrip.stacoll, row.stacoll);
        assert_eq!(roundtrip.stanumbers, row.stanumbers);
        assert_eq!(roundtrip.stavalues, row.stavalues);
    }

    #[test]
    fn pg_class_long_relacl_catalog_tuple_roundtrips() {
        let row = PgClassRow {
            oid: 16_384,
            relname: "target".into(),
            relnamespace: crate::include::catalog::PG_CATALOG_NAMESPACE_OID,
            reltype: 16_385,
            relowner: 16_386,
            relam: crate::include::catalog::HEAP_TABLE_AM_OID,
            relfilenode: 16_384,
            reltablespace: 0,
            relpages: 0,
            reltuples: 0.0,
            relallvisible: 0,
            relallfrozen: 0,
            reltoastrelid: 0,
            relhasindex: false,
            relpersistence: 'p',
            relkind: 'r',
            relnatts: 2,
            relhassubclass: false,
            relhastriggers: false,
            relrowsecurity: false,
            relforcerowsecurity: false,
            relispopulated: true,
            relispartition: false,
            relfrozenxid: 3,
            relpartbound: None,
            reloptions: None,
            relacl: Some(vec![
                "regress_merge_privs=arwdDxtm/regress_merge_privs".into(),
                "regress_merge_no_privs=a/malisper".into(),
                "regress_merge_no_privs=a/regress_merge_privs".into(),
            ]),
            relreplident: 'd',
            reloftype: 0,
        };

        let values = pg_class_row_values(row.clone());
        let desc = bootstrap_relation_desc(BootstrapCatalogKind::PgClass);
        let tuple = tuple_from_values(&desc, &values).unwrap();
        let decoded = decode_catalog_tuple_values(&desc, &tuple).unwrap();
        let roundtrip = pg_class_row_from_values(decoded).unwrap();

        assert_eq!(roundtrip, row);
    }
}
