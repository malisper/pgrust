use pgrust_analyze::CatalogLookup;
use pgrust_catalog_data::{
    OID_TYPE_OID, PG_CATALOG_NAMESPACE_OID, PgCollationRow, PgConversionRow, PgOpclassRow,
    PgOperatorRow, PgOpfamilyRow, PgTsConfigRow, PgTsDictRow, PgTsParserRow, PgTsTemplateRow,
    PgTypeRow,
};
use pgrust_nodes::datum::{ArrayValue, Value};
use pgrust_nodes::parsenodes::{ParseError, SqlTypeKind};
use pgrust_nodes::primnodes::BuiltinScalarFunction;

#[derive(Debug, Clone)]
pub enum CatalogBuiltinError {
    Detailed {
        message: String,
        detail: Option<String>,
        hint: Option<String>,
        sqlstate: &'static str,
    },
    UnexpectedToken {
        expected: &'static str,
        actual: String,
    },
    TypeMismatch {
        op: &'static str,
        left: Value,
        right: Value,
    },
    Parse(ParseError),
}

pub fn eval_enum_function(
    func: BuiltinScalarFunction,
    values: &[Value],
    result_type: Option<pgrust_nodes::SqlType>,
    catalog: &dyn CatalogLookup,
) -> Option<Result<Value, CatalogBuiltinError>> {
    if !matches!(
        func,
        BuiltinScalarFunction::EnumFirst
            | BuiltinScalarFunction::EnumLast
            | BuiltinScalarFunction::EnumRange
    ) {
        return None;
    }
    Some(eval_enum_function_inner(func, values, result_type, catalog))
}

fn eval_enum_function_inner(
    func: BuiltinScalarFunction,
    values: &[Value],
    result_type: Option<pgrust_nodes::SqlType>,
    catalog: &dyn CatalogLookup,
) -> Result<Value, CatalogBuiltinError> {
    let enum_type = result_type
        .map(|ty| if ty.is_array { ty.element_type() } else { ty })
        .filter(|ty| matches!(ty.kind, SqlTypeKind::Enum) && ty.type_oid != 0)
        .ok_or_else(|| CatalogBuiltinError::Detailed {
            message: "enum support function requires a concrete enum type".into(),
            detail: None,
            hint: None,
            sqlstate: "42804",
        })?;
    let enum_type_oid = if enum_type.typrelid != 0 {
        enum_type.typrelid
    } else {
        enum_type.type_oid
    };
    let mut labels = catalog
        .enum_rows()
        .into_iter()
        .filter(|row| row.enumtypid == enum_type_oid)
        .collect::<Vec<_>>();
    labels.sort_by(|left, right| {
        left.enumsortorder
            .partial_cmp(&right.enumsortorder)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    match func {
        BuiltinScalarFunction::EnumFirst => labels
            .first()
            .map(|row| {
                ensure_enum_function_label_safe(catalog, enum_type_oid, row.oid)?;
                Ok(Value::EnumOid(row.oid))
            })
            .unwrap_or(Ok(Value::Null)),
        BuiltinScalarFunction::EnumLast => labels
            .last()
            .map(|row| {
                ensure_enum_function_label_safe(catalog, enum_type_oid, row.oid)?;
                Ok(Value::EnumOid(row.oid))
            })
            .unwrap_or(Ok(Value::Null)),
        BuiltinScalarFunction::EnumRange => {
            let lower = values.first().and_then(|value| match value {
                Value::EnumOid(oid) => labels.iter().position(|row| row.oid == *oid),
                Value::Null => Some(0),
                _ => None,
            });
            let upper = values.get(1).and_then(|value| match value {
                Value::EnumOid(oid) => labels.iter().position(|row| row.oid == *oid),
                Value::Null => labels.len().checked_sub(1),
                _ => None,
            });
            let (start, end) = match values.len() {
                1 => (0, labels.len().saturating_sub(1)),
                2 => (lower.unwrap_or(labels.len()), upper.unwrap_or(0)),
                _ => {
                    return Err(CatalogBuiltinError::UnexpectedToken {
                        expected: "enum_range(anyenum [, anyenum])",
                        actual: format!("enum_range({} args)", values.len()),
                    });
                }
            };
            let items = if labels.is_empty() || start > end {
                Vec::new()
            } else {
                let mut items = Vec::new();
                for row in &labels[start..=end] {
                    ensure_enum_function_label_safe(catalog, enum_type_oid, row.oid)?;
                    items.push(Value::EnumOid(row.oid));
                }
                items
            };
            Ok(Value::PgArray(
                ArrayValue::from_1d(items).with_element_type_oid(enum_type_oid),
            ))
        }
        _ => unreachable!(),
    }
}

fn ensure_enum_function_label_safe(
    catalog: &dyn CatalogLookup,
    enum_type_oid: u32,
    label_oid: u32,
) -> Result<(), CatalogBuiltinError> {
    if catalog.enum_label_is_committed(enum_type_oid, label_oid) {
        return Ok(());
    }
    let label = catalog
        .enum_label(enum_type_oid, label_oid)
        .or_else(|| catalog.enum_label_by_oid(label_oid))
        .unwrap_or_else(|| label_oid.to_string());
    let type_name = catalog
        .type_by_oid(enum_type_oid)
        .map(|row| row.typname)
        .unwrap_or_else(|| enum_type_oid.to_string());
    Err(CatalogBuiltinError::Detailed {
        message: format!("unsafe use of new value \"{label}\" of enum type {type_name}"),
        detail: None,
        hint: Some("New enum values must be committed before they can be used.".into()),
        sqlstate: "55P04",
    })
}

fn catalog_is_temp_schema_name(schema_name: &str) -> bool {
    schema_name.eq_ignore_ascii_case("pg_temp")
        || schema_name.to_ascii_lowercase().starts_with("pg_temp_")
}

fn catalog_visibility_search_path(catalog: &dyn CatalogLookup) -> Vec<String> {
    let configured = catalog.search_path();
    let mut search_path = Vec::new();
    if !configured
        .iter()
        .any(|schema| schema.eq_ignore_ascii_case("pg_catalog"))
    {
        search_path.push("pg_catalog".into());
    }
    search_path.extend(configured);
    search_path
}

fn catalog_object_visible_in_search_path(
    catalog: &dyn CatalogLookup,
    target_oid: u32,
    target_namespace_oid: u32,
    target_name: &str,
    mut same_name_oid_in_namespace: impl FnMut(u32, &str) -> Option<u32>,
) -> bool {
    if catalog
        .namespace_row_by_oid(target_namespace_oid)
        .is_some_and(|namespace| catalog_is_temp_schema_name(&namespace.nspname))
    {
        return false;
    }
    for schema_name in catalog_visibility_search_path(catalog) {
        if catalog_is_temp_schema_name(&schema_name) {
            continue;
        }
        let Some(namespace) = catalog
            .namespace_rows()
            .into_iter()
            .find(|row| row.nspname.eq_ignore_ascii_case(&schema_name))
        else {
            continue;
        };
        if let Some(candidate_oid) = same_name_oid_in_namespace(namespace.oid, target_name) {
            return candidate_oid == target_oid;
        }
    }
    false
}

fn oid_arg_to_u32(value: &Value, op: &'static str) -> Result<u32, CatalogBuiltinError> {
    match value {
        Value::EnumOid(oid) => Ok(*oid),
        Value::Int32(oid) => u32::try_from(*oid).map_err(|_| CatalogBuiltinError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::Int64(i64::from(OID_TYPE_OID)),
        }),
        Value::Int64(oid) => u32::try_from(*oid).map_err(|_| CatalogBuiltinError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::Int64(i64::from(OID_TYPE_OID)),
        }),
        _ if value.as_text().is_some() => value
            .as_text()
            .expect("guarded above")
            .trim()
            .parse::<u32>()
            .map_err(|_| CatalogBuiltinError::TypeMismatch {
                op,
                left: value.clone(),
                right: Value::Int64(i64::from(OID_TYPE_OID)),
            }),
        _ => Err(CatalogBuiltinError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::Int64(i64::from(OID_TYPE_OID)),
        }),
    }
}

fn eval_catalog_visibility_result(
    values: &[Value],
    function_name: &'static str,
    mut is_visible: impl FnMut(u32) -> Result<Option<bool>, CatalogBuiltinError>,
) -> Result<Value, CatalogBuiltinError> {
    match values {
        [Value::Null] => Ok(Value::Null),
        [value] => {
            let oid = oid_arg_to_u32(value, function_name)?;
            Ok(is_visible(oid)?.map(Value::Bool).unwrap_or(Value::Null))
        }
        _ => Err(CatalogBuiltinError::UnexpectedToken {
            expected: function_name,
            actual: format!("{function_name}({} args)", values.len()),
        }),
    }
}

pub fn eval_pg_type_is_visible(
    values: &[Value],
    catalog: &dyn CatalogLookup,
) -> Result<Value, CatalogBuiltinError> {
    eval_catalog_visibility_result(values, "pg_type_is_visible", |oid| {
        let Some(row) = catalog.type_by_oid(oid) else {
            return Ok(None);
        };
        Ok(Some(catalog_object_visible_in_search_path(
            catalog,
            row.oid,
            row.typnamespace,
            &row.typname,
            |namespace_oid, typname| {
                catalog
                    .type_rows()
                    .into_iter()
                    .find(|candidate: &PgTypeRow| {
                        candidate.typnamespace == namespace_oid
                            && candidate.typname.eq_ignore_ascii_case(typname)
                    })
                    .map(|candidate| candidate.oid)
            },
        )))
    })
}

pub fn eval_pg_operator_is_visible(
    values: &[Value],
    catalog: &dyn CatalogLookup,
) -> Result<Value, CatalogBuiltinError> {
    eval_catalog_visibility_result(values, "pg_operator_is_visible", |oid| {
        let Some(row) = catalog.operator_by_oid(oid) else {
            return Ok(None);
        };
        Ok(Some(catalog_object_visible_in_search_path(
            catalog,
            row.oid,
            row.oprnamespace,
            &row.oprname,
            |namespace_oid, oprname| {
                catalog
                    .operator_rows()
                    .into_iter()
                    .find(|candidate: &PgOperatorRow| {
                        candidate.oprnamespace == namespace_oid
                            && candidate.oprname.eq_ignore_ascii_case(oprname)
                            && candidate.oprleft == row.oprleft
                            && candidate.oprright == row.oprright
                    })
                    .map(|candidate| candidate.oid)
            },
        )))
    })
}

pub fn eval_pg_opclass_is_visible(
    values: &[Value],
    catalog: &dyn CatalogLookup,
) -> Result<Value, CatalogBuiltinError> {
    eval_catalog_visibility_result(values, "pg_opclass_is_visible", |oid| {
        let Some(row) = catalog
            .opclass_rows()
            .into_iter()
            .find(|row| row.oid == oid)
        else {
            return Ok(None);
        };
        Ok(Some(catalog_object_visible_in_search_path(
            catalog,
            row.oid,
            row.opcnamespace,
            &row.opcname,
            |namespace_oid, opcname| {
                catalog
                    .opclass_rows()
                    .into_iter()
                    .find(|candidate: &PgOpclassRow| {
                        candidate.opcnamespace == namespace_oid
                            && candidate.opcmethod == row.opcmethod
                            && candidate.opcname.eq_ignore_ascii_case(opcname)
                    })
                    .map(|candidate| candidate.oid)
            },
        )))
    })
}

pub fn eval_pg_opfamily_is_visible(
    values: &[Value],
    catalog: &dyn CatalogLookup,
) -> Result<Value, CatalogBuiltinError> {
    eval_catalog_visibility_result(values, "pg_opfamily_is_visible", |oid| {
        let Some(row) = catalog
            .opfamily_rows()
            .into_iter()
            .find(|row| row.oid == oid)
        else {
            return Ok(None);
        };
        Ok(Some(catalog_object_visible_in_search_path(
            catalog,
            row.oid,
            row.opfnamespace,
            &row.opfname,
            |namespace_oid, opfname| {
                catalog
                    .opfamily_rows()
                    .into_iter()
                    .find(|candidate: &PgOpfamilyRow| {
                        candidate.opfnamespace == namespace_oid
                            && candidate.opfmethod == row.opfmethod
                            && candidate.opfname.eq_ignore_ascii_case(opfname)
                    })
                    .map(|candidate| candidate.oid)
            },
        )))
    })
}

pub fn eval_pg_conversion_is_visible(
    values: &[Value],
    catalog: &dyn CatalogLookup,
) -> Result<Value, CatalogBuiltinError> {
    eval_catalog_visibility_result(values, "pg_conversion_is_visible", |oid| {
        let Some(row) = catalog
            .conversion_rows()
            .into_iter()
            .find(|row| row.oid == oid)
        else {
            return Ok(None);
        };
        Ok(Some(catalog_object_visible_in_search_path(
            catalog,
            row.oid,
            row.connamespace,
            &row.conname,
            |namespace_oid, conname| {
                catalog
                    .conversion_rows()
                    .into_iter()
                    .find(|candidate: &PgConversionRow| {
                        candidate.connamespace == namespace_oid
                            && candidate.conname.eq_ignore_ascii_case(conname)
                    })
                    .map(|candidate| candidate.oid)
            },
        )))
    })
}

pub fn eval_pg_collation_is_visible(
    values: &[Value],
    catalog: &dyn CatalogLookup,
) -> Result<Value, CatalogBuiltinError> {
    eval_catalog_visibility_result(values, "pg_collation_is_visible", |oid| {
        let Some(row) = catalog
            .collation_rows()
            .into_iter()
            .find(|row| row.oid == oid)
        else {
            return Ok(None);
        };
        Ok(Some(catalog_object_visible_in_search_path(
            catalog,
            row.oid,
            row.collnamespace,
            &row.collname,
            |namespace_oid, collname| {
                catalog
                    .collation_rows()
                    .into_iter()
                    .find(|candidate: &PgCollationRow| {
                        candidate.collnamespace == namespace_oid
                            && candidate.collname.eq_ignore_ascii_case(collname)
                    })
                    .map(|candidate| candidate.oid)
            },
        )))
    })
}

pub fn eval_pg_ts_parser_is_visible(
    values: &[Value],
    catalog: &dyn CatalogLookup,
) -> Result<Value, CatalogBuiltinError> {
    eval_catalog_visibility_result(values, "pg_ts_parser_is_visible", |oid| {
        let Some(row) = catalog
            .ts_parser_rows()
            .into_iter()
            .find(|row| row.oid == oid)
        else {
            return Ok(None);
        };
        Ok(Some(catalog_object_visible_in_search_path(
            catalog,
            row.oid,
            row.prsnamespace,
            &row.prsname,
            |namespace_oid, prsname| {
                catalog
                    .ts_parser_rows()
                    .into_iter()
                    .find(|candidate: &PgTsParserRow| {
                        candidate.prsnamespace == namespace_oid
                            && candidate.prsname.eq_ignore_ascii_case(prsname)
                    })
                    .map(|candidate| candidate.oid)
            },
        )))
    })
}

pub fn eval_pg_ts_dict_is_visible(
    values: &[Value],
    catalog: &dyn CatalogLookup,
) -> Result<Value, CatalogBuiltinError> {
    eval_catalog_visibility_result(values, "pg_ts_dict_is_visible", |oid| {
        let Some(row) = catalog
            .ts_dict_rows()
            .into_iter()
            .find(|row| row.oid == oid)
        else {
            return Ok(None);
        };
        Ok(Some(catalog_object_visible_in_search_path(
            catalog,
            row.oid,
            row.dictnamespace,
            &row.dictname,
            |namespace_oid, dictname| {
                catalog
                    .ts_dict_rows()
                    .into_iter()
                    .find(|candidate: &PgTsDictRow| {
                        candidate.dictnamespace == namespace_oid
                            && candidate.dictname.eq_ignore_ascii_case(dictname)
                    })
                    .map(|candidate| candidate.oid)
            },
        )))
    })
}

pub fn eval_pg_ts_template_is_visible(
    values: &[Value],
    catalog: &dyn CatalogLookup,
) -> Result<Value, CatalogBuiltinError> {
    eval_catalog_visibility_result(values, "pg_ts_template_is_visible", |oid| {
        let Some(row) = catalog
            .ts_template_rows()
            .into_iter()
            .find(|row| row.oid == oid)
        else {
            return Ok(None);
        };
        Ok(Some(catalog_object_visible_in_search_path(
            catalog,
            row.oid,
            row.tmplnamespace,
            &row.tmplname,
            |namespace_oid, tmplname| {
                catalog
                    .ts_template_rows()
                    .into_iter()
                    .find(|candidate: &PgTsTemplateRow| {
                        candidate.tmplnamespace == namespace_oid
                            && candidate.tmplname.eq_ignore_ascii_case(tmplname)
                    })
                    .map(|candidate| candidate.oid)
            },
        )))
    })
}

pub fn eval_pg_ts_config_is_visible(
    values: &[Value],
    catalog: &dyn CatalogLookup,
) -> Result<Value, CatalogBuiltinError> {
    eval_catalog_visibility_result(values, "pg_ts_config_is_visible", |oid| {
        let Some(row) = catalog
            .ts_config_rows()
            .into_iter()
            .find(|row| row.oid == oid)
        else {
            return Ok(None);
        };
        Ok(Some(catalog_object_visible_in_search_path(
            catalog,
            row.oid,
            row.cfgnamespace,
            &row.cfgname,
            |namespace_oid, cfgname| {
                catalog
                    .ts_config_rows()
                    .into_iter()
                    .find(|candidate: &PgTsConfigRow| {
                        candidate.cfgnamespace == namespace_oid
                            && candidate.cfgname.eq_ignore_ascii_case(cfgname)
                    })
                    .map(|candidate| candidate.oid)
            },
        )))
    })
}

pub fn pg_catalog_namespace_oid() -> u32 {
    PG_CATALOG_NAMESPACE_OID
}
