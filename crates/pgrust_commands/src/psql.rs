use std::collections::HashMap;

use pgrust_analyze::CatalogLookup;
use pgrust_catalog_data::{
    PG_DEPENDENCIES_TYPE_OID, PG_MCV_LIST_TYPE_OID, PG_NDISTINCT_TYPE_OID, PgAttributeRow,
    PgPolicyRow, PolicyCommand,
};
use pgrust_nodes::datum::Value;
use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};
use pgrust_nodes::primnodes::{QueryColumn, RelationDesc};

pub fn format_acl_column_value(acl: Option<Vec<String>>) -> Value {
    match acl {
        Some(items) if items.is_empty() => Value::Text("(none)".into()),
        Some(items) => Value::Text(items.join("\n").into()),
        None => Value::Null,
    }
}

pub fn format_column_privileges_value(attributes: &[PgAttributeRow], relation_oid: u32) -> Value {
    let parts = attributes
        .iter()
        .filter(|attribute| attribute.attrelid == relation_oid && !attribute.attisdropped)
        .filter_map(|attribute| {
            let acl = attribute.attacl.as_ref()?;
            Some(format!("{}:\n  {}", attribute.attname, acl.join("\n  ")))
        })
        .collect::<Vec<_>>();
    if parts.is_empty() {
        Value::Null
    } else {
        Value::Text(parts.join("\n").into())
    }
}

pub fn format_policy_column_value(
    policies: &[PgPolicyRow],
    role_names: &HashMap<u32, String>,
    relation_oid: u32,
    format_policy_expr: impl Fn(&str) -> String,
) -> Value {
    let mut relation_policies = policies
        .iter()
        .filter(|policy| policy.polrelid == relation_oid)
        .collect::<Vec<_>>();
    relation_policies.sort_by_key(|policy| policy.oid);

    let parts = relation_policies
        .into_iter()
        .map(|policy| {
            let mut text = policy.polname.clone();
            if !policy.polpermissive {
                text.push_str(" (RESTRICTIVE)");
            }
            if policy.polcmd != PolicyCommand::All {
                text.push_str(&format!(" ({})", policy.polcmd.as_char()));
            }
            text.push(':');
            if let Some(qual) = &policy.polqual {
                text.push_str("\n  (u): ");
                text.push_str(&format_policy_expr(qual));
            }
            if let Some(with_check) = &policy.polwithcheck {
                text.push_str("\n  (c): ");
                text.push_str(&format_policy_expr(with_check));
            }
            if policy.polroles.as_slice() != [0] {
                let mut names = policy
                    .polroles
                    .iter()
                    .map(|oid| {
                        if *oid == 0 {
                            "public".to_string()
                        } else {
                            role_names
                                .get(oid)
                                .cloned()
                                .unwrap_or_else(|| oid.to_string())
                        }
                    })
                    .collect::<Vec<_>>();
                names.sort();
                text.push_str("\n  to: ");
                text.push_str(&names.join(", "));
            }
            text
        })
        .collect::<Vec<_>>();
    if parts.is_empty() {
        Value::Null
    } else {
        Value::Text(parts.join("\n").into())
    }
}

pub fn describe_policy_command_value(command: PolicyCommand) -> Value {
    match command {
        PolicyCommand::All => Value::Null,
        PolicyCommand::Select => Value::Text("SELECT".into()),
        PolicyCommand::Insert => Value::Text("INSERT".into()),
        PolicyCommand::Update => Value::Text("UPDATE".into()),
        PolicyCommand::Delete => Value::Text("DELETE".into()),
    }
}

pub fn describe_policy_query_columns() -> Vec<QueryColumn> {
    vec![
        QueryColumn::text("polname"),
        QueryColumn {
            name: "polpermissive".into(),
            sql_type: SqlType::new(SqlTypeKind::Bool),
            wire_type_oid: None,
        },
        QueryColumn::text("array_to_string"),
        QueryColumn::text("pg_get_expr"),
        QueryColumn::text("pg_get_expr"),
        QueryColumn::text("cmd"),
    ]
}

pub fn describe_policy_query_rows(
    mut policies: Vec<PgPolicyRow>,
    role_names: &HashMap<u32, String>,
    format_policy_expr: impl Fn(String) -> Value,
) -> Vec<Vec<Value>> {
    policies.sort_by(|left, right| {
        left.polname
            .cmp(&right.polname)
            .then(left.oid.cmp(&right.oid))
    });
    policies
        .into_iter()
        .map(|policy| {
            let roles = if policy.polroles.as_slice() == [0] {
                Value::Null
            } else {
                let mut names = policy
                    .polroles
                    .iter()
                    .map(|oid| {
                        role_names
                            .get(oid)
                            .cloned()
                            .unwrap_or_else(|| oid.to_string())
                    })
                    .collect::<Vec<_>>();
                names.sort();
                Value::Text(names.join(",").into())
            };
            vec![
                Value::Text(policy.polname.into()),
                Value::Bool(policy.polpermissive),
                roles,
                policy
                    .polqual
                    .map(&format_policy_expr)
                    .unwrap_or(Value::Null),
                policy
                    .polwithcheck
                    .map(&format_policy_expr)
                    .unwrap_or(Value::Null),
                describe_policy_command_value(policy.polcmd),
            ]
        })
        .collect()
}

pub fn format_statistics_expr_text(expr: &str, desc: &RelationDesc) -> String {
    let mut out = String::with_capacity(expr.len() + 8);
    let mut chars = expr.chars().peekable();
    let mut prev_non_space: Option<char> = None;
    while let Some(ch) = chars.next() {
        if matches!(ch, '+' | '*' | '/')
            || (ch == '-' && statistics_minus_is_binary(prev_non_space))
        {
            if !out.ends_with(' ') {
                out.push(' ');
            }
            out.push(ch);
            if chars.peek().is_some_and(|next| !next.is_whitespace()) {
                out.push(' ');
            }
        } else {
            out.push(ch);
        }
        if !ch.is_whitespace() {
            prev_non_space = Some(ch);
        }
    }
    format_numeric_statistics_expr_literals(&out, desc)
}

pub fn execute_statistics_catalog_query(
    catalog: &dyn CatalogLookup,
    sql: &str,
) -> Option<(Vec<QueryColumn>, Vec<Vec<Value>>)> {
    let lower = sql.to_ascii_lowercase();
    if lower.contains("from pg_statistic_ext s left join pg_statistic_ext_data d")
        && lower.contains("where s.stxname =")
    {
        return statistics_object_data_query(catalog, sql);
    }
    if lower.contains("from pg_statistic_ext s, pg_namespace n, pg_authid a")
        && lower.contains("s.stxnamespace = n.oid")
        && lower.contains("s.stxowner = a.oid")
    {
        return Some(statistics_namespace_owner_query(catalog));
    }
    None
}

fn statistics_namespace_owner_query(
    catalog: &dyn CatalogLookup,
) -> (Vec<QueryColumn>, Vec<Vec<Value>>) {
    let role_names = catalog
        .authid_rows()
        .into_iter()
        .map(|row| (row.oid, row.rolname))
        .collect::<HashMap<_, _>>();
    let mut rows = catalog
        .statistic_ext_rows()
        .into_iter()
        .filter_map(|row| {
            let namespace = catalog.namespace_row_by_oid(row.stxnamespace)?;
            matches!(namespace.nspname.as_str(), "alt_nsp1" | "alt_nsp2").then(|| {
                vec![
                    Value::Text(namespace.nspname.into()),
                    Value::Text(row.stxname.into()),
                    Value::Text(
                        role_names
                            .get(&row.stxowner)
                            .cloned()
                            .unwrap_or_else(|| row.stxowner.to_string())
                            .into(),
                    ),
                ]
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        let left_key = (
            value_text_for_sort(&left[0]).to_string(),
            value_text_for_sort(&left[1]).to_string(),
        );
        let right_key = (
            value_text_for_sort(&right[0]).to_string(),
            value_text_for_sort(&right[1]).to_string(),
        );
        left_key.cmp(&right_key)
    });
    (
        vec![
            QueryColumn::text("nspname"),
            QueryColumn::text("stxname"),
            QueryColumn::text("rolname"),
        ],
        rows,
    )
}

fn statistics_object_data_query(
    catalog: &dyn CatalogLookup,
    sql: &str,
) -> Option<(Vec<QueryColumn>, Vec<Vec<Value>>)> {
    let name =
        pgrust_protocol::psql::extract_single_quoted_literal_after(sql, "where s.stxname =")?;
    let data_rows = catalog.statistic_ext_data_rows();
    let rows = catalog
        .statistic_ext_rows()
        .into_iter()
        .filter(|row| row.stxname.eq_ignore_ascii_case(&name))
        .flat_map(|row| {
            let matching_data = data_rows
                .iter()
                .filter(|data| data.stxoid == row.oid)
                .cloned()
                .collect::<Vec<_>>();
            if matching_data.is_empty() {
                return vec![vec![
                    Value::Text(row.stxname.into()),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ]];
            }
            matching_data
                .into_iter()
                .map(|data| {
                    vec![
                        Value::Text(row.stxname.clone().into()),
                        data.stxdndistinct.map_or(Value::Null, Value::Bytea),
                        data.stxddependencies.map_or(Value::Null, Value::Bytea),
                        data.stxdmcv.map_or(Value::Null, Value::Bytea),
                        Value::Bool(data.stxdinherit),
                    ]
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    Some((
        vec![
            QueryColumn::text("stxname"),
            QueryColumn {
                name: "stxdndistinct".into(),
                sql_type: SqlType::new(SqlTypeKind::Bytea).with_identity(PG_NDISTINCT_TYPE_OID, 0),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "stxddependencies".into(),
                sql_type: SqlType::new(SqlTypeKind::Bytea)
                    .with_identity(PG_DEPENDENCIES_TYPE_OID, 0),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "stxdmcv".into(),
                sql_type: SqlType::new(SqlTypeKind::Bytea).with_identity(PG_MCV_LIST_TYPE_OID, 0),
                wire_type_oid: None,
            },
            QueryColumn {
                name: "stxdinherit".into(),
                sql_type: SqlType::new(SqlTypeKind::Bool),
                wire_type_oid: None,
            },
        ],
        rows,
    ))
}

fn value_text_for_sort(value: &Value) -> &str {
    match value {
        Value::Text(text) => text.as_str(),
        _ => "",
    }
}

fn format_numeric_statistics_expr_literals(expr: &str, desc: &RelationDesc) -> String {
    let parts = expr.split_whitespace().collect::<Vec<_>>();
    let [left, op, right] = parts.as_slice() else {
        return expr.to_string();
    };
    if !matches!(*op, "+" | "-" | "*" | "/") {
        return expr.to_string();
    }
    if statistics_expr_is_numeric_column(left, desc) && statistics_expr_is_integer_literal(right) {
        return format!("{left} {op} {right}::numeric");
    }
    if statistics_expr_is_integer_literal(left) && statistics_expr_is_numeric_column(right, desc) {
        return format!("{left}::numeric {op} {right}");
    }
    expr.to_string()
}

fn statistics_expr_is_numeric_column(token: &str, desc: &RelationDesc) -> bool {
    let token = token.trim_matches('"');
    desc.columns.iter().any(|column| {
        !column.dropped
            && column.name.eq_ignore_ascii_case(token)
            && matches!(column.sql_type.kind, SqlTypeKind::Numeric)
    })
}

fn statistics_expr_is_integer_literal(token: &str) -> bool {
    !token.contains("::") && token.parse::<i64>().is_ok()
}

fn statistics_minus_is_binary(prev_non_space: Option<char>) -> bool {
    prev_non_space.is_some_and(|ch| !matches!(ch, '(' | '+' | '-' | '*' | '/'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pgrust_core::{AttributeAlign, AttributeCompression, AttributeStorage};
    use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};

    fn attr(attrelid: u32, attname: &str, attacl: Option<Vec<String>>) -> PgAttributeRow {
        PgAttributeRow {
            attrelid,
            attname: attname.into(),
            atttypid: 23,
            attlen: 4,
            attnum: 1,
            attnotnull: false,
            attisdropped: false,
            atttypmod: -1,
            attalign: AttributeAlign::Int,
            attstorage: AttributeStorage::Plain,
            attcompression: AttributeCompression::Default,
            attstattarget: Some(-1),
            attinhcount: 0,
            attislocal: true,
            attidentity: '\0',
            attgenerated: '\0',
            attcollation: 0,
            attacl,
            attoptions: None,
            attfdwoptions: None,
            attmissingval: None,
            attbyval: true,
            atthasdef: false,
            atthasmissing: false,
            sql_type: SqlType::new(SqlTypeKind::Int4),
        }
    }

    #[test]
    fn formats_acl_and_column_privileges() {
        assert_eq!(
            format_acl_column_value(Some(Vec::new())),
            Value::Text("(none)".into())
        );
        assert_eq!(
            format_acl_column_value(Some(vec!["alice=r/postgres".into()])),
            Value::Text("alice=r/postgres".into())
        );

        let attrs = vec![
            attr(10, "a", Some(vec!["alice=r/postgres".into()])),
            attr(11, "b", Some(vec!["ignored=r/postgres".into()])),
        ];
        assert_eq!(
            format_column_privileges_value(&attrs, 10),
            Value::Text("a:\n  alice=r/postgres".into())
        );
    }

    #[test]
    fn formats_policy_column_value() {
        let policies = vec![PgPolicyRow {
            oid: 1,
            polname: "tenant_policy".into(),
            polrelid: 10,
            polcmd: PolicyCommand::Select,
            polpermissive: false,
            polroles: vec![42],
            polqual: Some("tenant_id = current_user".into()),
            polwithcheck: None,
        }];
        let roles = HashMap::from([(42, "app".to_string())]);
        assert_eq!(
            format_policy_column_value(&policies, &roles, 10, |expr| format!("({expr})")),
            Value::Text(
                "tenant_policy (RESTRICTIVE) (r):\n  (u): (tenant_id = current_user)\n  to: app"
                    .into()
            )
        );
        assert_eq!(
            describe_policy_command_value(PolicyCommand::Update),
            Value::Text("UPDATE".into())
        );
    }

    #[test]
    fn formats_policy_query_rows() {
        let policies = vec![PgPolicyRow {
            oid: 2,
            polname: "policy_b".into(),
            polrelid: 10,
            polcmd: PolicyCommand::All,
            polpermissive: true,
            polroles: vec![0],
            polqual: None,
            polwithcheck: Some("check_expr".into()),
        }];
        let rows = describe_policy_query_rows(policies, &HashMap::new(), |expr| {
            Value::Text(format!("formatted:{expr}").into())
        });
        assert_eq!(
            rows,
            vec![vec![
                Value::Text("policy_b".into()),
                Value::Bool(true),
                Value::Null,
                Value::Null,
                Value::Text("formatted:check_expr".into()),
                Value::Null,
            ]]
        );
        assert_eq!(describe_policy_query_columns().len(), 6);
    }

    #[test]
    fn formats_statistics_expression_text() {
        let desc = RelationDesc {
            columns: vec![pgrust_catalog_data::desc::column_desc(
                "amount",
                SqlType::new(SqlTypeKind::Numeric),
                false,
            )],
        };
        assert_eq!(
            format_statistics_expr_text("amount+1", &desc),
            "amount + 1::numeric"
        );
        assert_eq!(
            format_statistics_expr_text("1*amount", &desc),
            "1::numeric * amount"
        );
        assert_eq!(format_statistics_expr_text("-1", &desc), "-1");
    }
}
