use std::cmp::Ordering;

use pgrust_catalog_data::{
    RI_FKEY_CASCADE_DEL_PROC_OID, RI_FKEY_CASCADE_UPD_PROC_OID, RI_FKEY_NOACTION_DEL_PROC_OID,
    RI_FKEY_NOACTION_UPD_PROC_OID, RI_FKEY_RESTRICT_DEL_PROC_OID, RI_FKEY_RESTRICT_UPD_PROC_OID,
    RI_FKEY_SETDEFAULT_DEL_PROC_OID, RI_FKEY_SETDEFAULT_UPD_PROC_OID, RI_FKEY_SETNULL_DEL_PROC_OID,
    RI_FKEY_SETNULL_UPD_PROC_OID,
};
use pgrust_expr::DateTimeConfig;
use pgrust_expr::executor::expr_multirange::{
    multirange_contains_multirange, multirange_contains_range, multirange_from_range,
    multirange_overlaps_multirange, multirange_overlaps_range, normalize_multirange,
};
use pgrust_expr::executor::expr_range::{range_contains_range, range_overlap};
use pgrust_nodes::datum::IndirectVarlenaValue;
use pgrust_nodes::parsenodes::ForeignKeyAction;
use pgrust_nodes::primnodes::RelationDesc;
use pgrust_nodes::{ScanKeyData, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertForeignKeyCheckPhase {
    BeforeHeapInsert,
    AfterIndexInsert,
}

#[derive(Debug, Clone)]
pub enum ForeignKeyHelperError {
    Internal(&'static str),
    Expr(pgrust_expr::ExprError),
    TypeMismatch {
        op: &'static str,
        left: Value,
        right: Value,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyViolationMessage {
    pub constraint: String,
    pub message: String,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundForeignKeyViolationInfo<'a> {
    pub relation_name: &'a str,
    pub constraint_name: &'a str,
    pub child_relation_name: &'a str,
    pub referenced_column_names: &'a [String],
}

pub trait ForeignKeyValueRenderContext {
    fn enum_label_by_oid(&self, oid: u32) -> Option<String>;
    fn decode_indirect_varlena(&self, indirect: &IndirectVarlenaValue) -> Option<Value>;
    fn datetime_config(&self) -> &DateTimeConfig;
}

pub fn inbound_foreign_key_violation_message(
    info: InboundForeignKeyViolationInfo<'_>,
    rendered_key_values: Option<&str>,
) -> ForeignKeyViolationMessage {
    let detail = if let Some(rendered_key_values) = rendered_key_values {
        format!(
            "Key ({})=({}) is still referenced from table \"{}\".",
            info.referenced_column_names.join(", "),
            rendered_key_values,
            info.child_relation_name
        )
    } else {
        format!(
            "Key is still referenced from table \"{}\".",
            info.child_relation_name
        )
    };
    ForeignKeyViolationMessage {
        constraint: info.constraint_name.to_string(),
        message: format!(
            "update or delete on table \"{}\" violates foreign key constraint \"{}\" on table \"{}\"",
            info.relation_name, info.constraint_name, info.child_relation_name
        ),
        detail: Some(detail),
    }
}

pub fn inbound_restrict_foreign_key_violation_message(
    info: InboundForeignKeyViolationInfo<'_>,
    rendered_key_values: Option<&str>,
) -> ForeignKeyViolationMessage {
    let detail = if let Some(rendered_key_values) = rendered_key_values {
        format!(
            "Key ({})=({}) is referenced from table \"{}\".",
            info.referenced_column_names.join(", "),
            rendered_key_values,
            info.child_relation_name
        )
    } else {
        format!(
            "Key is referenced from table \"{}\".",
            info.child_relation_name
        )
    };
    ForeignKeyViolationMessage {
        constraint: info.constraint_name.to_string(),
        message: format!(
            "update or delete on table \"{}\" violates RESTRICT setting of foreign key constraint \"{}\" on table \"{}\"",
            info.relation_name, info.constraint_name, info.child_relation_name
        ),
        detail: Some(detail),
    }
}

pub fn render_key_values(values: &[Value], ctx: &dyn ForeignKeyValueRenderContext) -> String {
    values
        .iter()
        .map(|value| render_key_value(value, ctx))
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn render_key_value(value: &Value, ctx: &dyn ForeignKeyValueRenderContext) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Int16(v) => v.to_string(),
        Value::Int32(v) => v.to_string(),
        Value::Int64(v) => v.to_string(),
        Value::Xid8(v) => v.to_string(),
        Value::PgLsn(v) => pgrust_expr::render_pg_lsn_text(*v),
        Value::Tid(v) => pgrust_expr::render_tid_text(v),
        Value::Money(v) => v.to_string(),
        Value::Float64(v) => v.to_string(),
        Value::Numeric(v) => format!("{v:?}"),
        Value::Interval(v) => format!("{v:?}"),
        Value::Uuid(v) => pgrust_expr::render_uuid_text(v),
        Value::Bool(v) => v.to_string(),
        Value::InternalChar(v) => v.to_string(),
        Value::EnumOid(v) => ctx.enum_label_by_oid(*v).unwrap_or_else(|| v.to_string()),
        Value::TextRef(_, _) | Value::Text(_) | Value::JsonPath(_) => {
            value.as_text().unwrap_or_default().to_string()
        }
        Value::Xml(v) => v.to_string(),
        Value::Json(v) => v.to_string(),
        Value::Bytea(v) => format!("{v:?}"),
        Value::Inet(v) => v.render_inet(),
        Value::Cidr(v) => v.render_cidr(),
        Value::MacAddr(v) => pgrust_expr::render_macaddr_text(v),
        Value::MacAddr8(v) => pgrust_expr::render_macaddr8_text(v),
        Value::Date(v) => format!("{v:?}"),
        Value::Time(v) => format!("{v:?}"),
        Value::TimeTz(v) => format!("{v:?}"),
        Value::Timestamp(v) => format!("{v:?}"),
        Value::TimestampTz(v) => format!("{v:?}"),
        Value::Bit(v) => format!("{v:?}"),
        Value::Point(v) => format!("{v:?}"),
        Value::Lseg(v) => format!("{v:?}"),
        Value::Path(v) => format!("{v:?}"),
        Value::Line(v) => format!("{v:?}"),
        Value::Box(v) => format!("{v:?}"),
        Value::Polygon(v) => format!("{v:?}"),
        Value::Circle(v) => format!("{v:?}"),
        Value::Jsonb(v) => format!("{v:?}"),
        Value::TsVector(v) => format!("{v:?}"),
        Value::TsQuery(v) => format!("{v:?}"),
        Value::Array(v) => format!("{v:?}"),
        Value::PgArray(v) => format!("{v:?}"),
        Value::Record(v) => format!("{v:?}"),
        Value::Range(v) => pgrust_expr::executor::expr_range::render_range_value_with_config(
            v,
            ctx.datetime_config(),
        ),
        Value::Multirange(v) => {
            pgrust_expr::executor::expr_multirange::render_multirange_with_config(
                v,
                ctx.datetime_config(),
            )
        }
        Value::IndirectVarlena(indirect) => ctx
            .decode_indirect_varlena(indirect)
            .map(|decoded| render_key_value(&decoded, ctx))
            .unwrap_or_else(|| "null".into()),
        Value::DroppedColumn(_) | Value::WrongTypeColumn { .. } => "null".into(),
    }
}

impl From<pgrust_expr::ExprError> for ForeignKeyHelperError {
    fn from(err: pgrust_expr::ExprError) -> Self {
        Self::Expr(err)
    }
}

pub fn foreign_key_delete_proc_oid(action: ForeignKeyAction) -> u32 {
    match action {
        ForeignKeyAction::Cascade => RI_FKEY_CASCADE_DEL_PROC_OID,
        ForeignKeyAction::Restrict => RI_FKEY_RESTRICT_DEL_PROC_OID,
        ForeignKeyAction::SetNull => RI_FKEY_SETNULL_DEL_PROC_OID,
        ForeignKeyAction::SetDefault => RI_FKEY_SETDEFAULT_DEL_PROC_OID,
        ForeignKeyAction::NoAction => RI_FKEY_NOACTION_DEL_PROC_OID,
    }
}

pub fn foreign_key_update_proc_oid(action: ForeignKeyAction) -> u32 {
    match action {
        ForeignKeyAction::Cascade => RI_FKEY_CASCADE_UPD_PROC_OID,
        ForeignKeyAction::Restrict => RI_FKEY_RESTRICT_UPD_PROC_OID,
        ForeignKeyAction::SetNull => RI_FKEY_SETNULL_UPD_PROC_OID,
        ForeignKeyAction::SetDefault => RI_FKEY_SETDEFAULT_UPD_PROC_OID,
        ForeignKeyAction::NoAction => RI_FKEY_NOACTION_UPD_PROC_OID,
    }
}

pub fn map_column_indexes_by_name(
    parent_desc: &RelationDesc,
    child_desc: &RelationDesc,
    parent_indexes: &[usize],
) -> Result<Vec<usize>, ForeignKeyHelperError> {
    parent_indexes
        .iter()
        .map(|parent_index| {
            let parent_column =
                parent_desc
                    .columns
                    .get(*parent_index)
                    .ok_or(ForeignKeyHelperError::Internal(
                        "invalid parent column index",
                    ))?;
            child_desc
                .columns
                .iter()
                .enumerate()
                .find(|(_, column)| {
                    !column.dropped && column.name.eq_ignore_ascii_case(&parent_column.name)
                })
                .map(|(index, _)| index)
                .ok_or(ForeignKeyHelperError::Internal(
                    "missing partition foreign key column",
                ))
        })
        .collect()
}

pub fn row_matches_key(values: &[Value], key_indexes: &[usize], key_values: &[Value]) -> bool {
    key_indexes.iter().zip(key_values).all(|(index, expected)| {
        values.get(*index).is_some_and(|actual| {
            pgrust_expr::compare_order_values(actual, expected, None, None, false)
                .expect("foreign-key key comparisons use implicit default collation")
                == Ordering::Equal
        })
    })
}

pub fn values_match_cross_indexes(
    left_values: &[Value],
    left_indexes: &[usize],
    left_period_index: Option<usize>,
    right_values: &[Value],
    right_indexes: &[usize],
    right_period_index: Option<usize>,
) -> bool {
    left_indexes
        .iter()
        .zip(right_indexes)
        .filter(|(left, right)| {
            Some(**left) != left_period_index && Some(**right) != right_period_index
        })
        .all(|(left, right)| {
            left_values
                .get(*left)
                .zip(right_values.get(*right))
                .is_some_and(|(left, right)| {
                    pgrust_expr::compare_order_values(left, right, None, None, false)
                        .expect("foreign-key key comparisons use implicit default collation")
                        == Ordering::Equal
                })
        })
}

pub fn key_columns_changed(previous_values: &[Value], values: &[Value], indexes: &[usize]) -> bool {
    indexes.iter().any(|index| {
        let previous = previous_values.get(*index).unwrap_or(&Value::Null);
        let current = values.get(*index).unwrap_or(&Value::Null);
        previous != current
    })
}

pub fn extract_key_values(values: &[Value], indexes: &[usize]) -> Vec<Value> {
    indexes
        .iter()
        .map(|index| {
            values
                .get(*index)
                .cloned()
                .unwrap_or(Value::Null)
                .to_owned_value()
        })
        .collect()
}

pub fn build_equality_scan_keys(key_values: &[Value]) -> Vec<ScanKeyData> {
    key_values
        .iter()
        .enumerate()
        .map(|(index, value)| ScanKeyData {
            attribute_number: index.saturating_add(1) as i16,
            strategy: 3,
            argument: value.to_owned_value(),
        })
        .collect()
}

pub fn periods_overlap(left: &Value, right: &Value) -> Result<bool, ForeignKeyHelperError> {
    match (left, right) {
        (Value::Range(left), Value::Range(right)) => Ok(range_overlap(left, right)),
        (Value::Multirange(left), Value::Range(right)) => {
            Ok(multirange_overlaps_range(left, right))
        }
        (Value::Range(left), Value::Multirange(right)) => {
            Ok(multirange_overlaps_range(right, left))
        }
        (Value::Multirange(left), Value::Multirange(right)) => {
            Ok(multirange_overlaps_multirange(left, right))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(false),
        _ => Err(ForeignKeyHelperError::TypeMismatch {
            op: "PERIOD foreign key",
            left: left.to_owned_value(),
            right: right.to_owned_value(),
        }),
    }
}

pub fn temporal_periods_cover(
    parent_periods: &[Value],
    child_period: &Value,
) -> Result<bool, ForeignKeyHelperError> {
    match child_period {
        Value::Range(child) => {
            let mut ranges = Vec::new();
            for period in parent_periods {
                match period {
                    Value::Range(range) => ranges.push(range.clone()),
                    Value::Multirange(multirange) => ranges.extend(multirange.ranges.clone()),
                    Value::Null => {}
                    other => {
                        return Err(ForeignKeyHelperError::TypeMismatch {
                            op: "PERIOD foreign key",
                            left: other.to_owned_value(),
                            right: child_period.to_owned_value(),
                        });
                    }
                }
            }
            if ranges.is_empty() {
                return Ok(false);
            }
            match multirange_from_range(child) {
                Ok(multirange) => {
                    let parent = normalize_multirange(multirange.multirange_type, ranges)?;
                    Ok(multirange_contains_range(&parent, child))
                }
                Err(_) => Ok(ranges
                    .iter()
                    .any(|parent| range_contains_range(parent, child))),
            }
        }
        Value::Multirange(child) => {
            let mut ranges = Vec::new();
            for period in parent_periods {
                match period {
                    Value::Range(range) => ranges.push(range.clone()),
                    Value::Multirange(multirange) => ranges.extend(multirange.ranges.clone()),
                    Value::Null => {}
                    other => {
                        return Err(ForeignKeyHelperError::TypeMismatch {
                            op: "PERIOD foreign key",
                            left: other.to_owned_value(),
                            right: child_period.to_owned_value(),
                        });
                    }
                }
            }
            if ranges.is_empty() {
                return Ok(false);
            }
            let parent = normalize_multirange(child.multirange_type, ranges)?;
            Ok(multirange_contains_multirange(&parent, child))
        }
        Value::Null => Ok(true),
        other => Err(ForeignKeyHelperError::TypeMismatch {
            op: "PERIOD foreign key",
            left: other.to_owned_value(),
            right: Value::Null,
        }),
    }
}

#[cfg(test)]
mod tests {
    use pgrust_catalog_data::desc::column_desc;
    use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};
    use pgrust_nodes::primnodes::ColumnDesc;

    use super::*;

    fn column(name: &str, dropped: bool) -> ColumnDesc {
        let mut column = column_desc(name, SqlType::new(SqlTypeKind::Int4), true);
        column.dropped = dropped;
        column
    }

    #[test]
    fn foreign_key_action_proc_oids_match_catalog_constants() {
        assert_eq!(
            foreign_key_delete_proc_oid(ForeignKeyAction::Cascade),
            RI_FKEY_CASCADE_DEL_PROC_OID
        );
        assert_eq!(
            foreign_key_update_proc_oid(ForeignKeyAction::SetDefault),
            RI_FKEY_SETDEFAULT_UPD_PROC_OID
        );
        assert_eq!(
            foreign_key_delete_proc_oid(ForeignKeyAction::NoAction),
            RI_FKEY_NOACTION_DEL_PROC_OID
        );
    }

    #[test]
    fn key_helpers_preserve_value_and_index_order() {
        let values = vec![Value::Int32(10), Value::Text("skip".into()), Value::Null];
        assert_eq!(
            extract_key_values(&values, &[2, 0]),
            vec![Value::Null, Value::Int32(10)]
        );
        assert!(row_matches_key(
            &values,
            &[0, 2],
            &[Value::Int32(10), Value::Null]
        ));
        assert!(key_columns_changed(
            &[Value::Int32(10), Value::Text("old".into())],
            &[Value::Int32(10), Value::Text("new".into())],
            &[1]
        ));
        assert_eq!(
            build_equality_scan_keys(&[Value::Int32(10), Value::Null]),
            vec![
                ScanKeyData {
                    attribute_number: 1,
                    strategy: 3,
                    argument: Value::Int32(10),
                },
                ScanKeyData {
                    attribute_number: 2,
                    strategy: 3,
                    argument: Value::Null,
                },
            ]
        );
    }

    #[test]
    fn cross_index_matching_ignores_period_columns() {
        let left = vec![Value::Int32(1), Value::Text("left-period".into())];
        let right = vec![Value::Text("right-period".into()), Value::Int32(1)];
        assert!(values_match_cross_indexes(
            &left,
            &[0, 1],
            Some(1),
            &right,
            &[1, 0],
            Some(0),
        ));
    }

    #[test]
    fn map_partition_columns_by_name_skips_dropped_columns() {
        let parent = RelationDesc {
            columns: vec![column("id", false), column("tenant", false)],
        };
        let child = RelationDesc {
            columns: vec![
                column("tenant", false),
                column("id", true),
                column("ID", false),
            ],
        };

        assert_eq!(
            map_column_indexes_by_name(&parent, &child, &[0, 1]).unwrap(),
            vec![2, 0]
        );
    }

    #[test]
    fn temporal_period_helpers_match_null_and_type_policy() {
        assert!(!periods_overlap(&Value::Null, &Value::Int32(1)).unwrap());
        assert!(temporal_periods_cover(&[], &Value::Null).unwrap());
        assert!(matches!(
            temporal_periods_cover(&[Value::Int32(1)], &Value::Null),
            Ok(true)
        ));
        assert!(matches!(
            periods_overlap(&Value::Int32(1), &Value::Int32(2)),
            Err(ForeignKeyHelperError::TypeMismatch { .. })
        ));
    }

    #[test]
    fn inbound_violation_messages_match_restrict_and_no_action_shapes() {
        let columns = vec!["id".to_string(), "tenant_id".to_string()];
        let info = InboundForeignKeyViolationInfo {
            relation_name: "parent",
            constraint_name: "child_parent_fkey",
            child_relation_name: "child",
            referenced_column_names: &columns,
        };

        let no_action = inbound_foreign_key_violation_message(info.clone(), Some("1, 2"));
        assert_eq!(no_action.constraint, "child_parent_fkey");
        assert_eq!(
            no_action.message,
            "update or delete on table \"parent\" violates foreign key constraint \"child_parent_fkey\" on table \"child\""
        );
        assert_eq!(
            no_action.detail.as_deref(),
            Some("Key (id, tenant_id)=(1, 2) is still referenced from table \"child\".")
        );

        let restrict = inbound_restrict_foreign_key_violation_message(info, None);
        assert_eq!(
            restrict.message,
            "update or delete on table \"parent\" violates RESTRICT setting of foreign key constraint \"child_parent_fkey\" on table \"child\""
        );
        assert_eq!(
            restrict.detail.as_deref(),
            Some("Key is referenced from table \"child\".")
        );
    }

    struct TestRenderContext(DateTimeConfig);

    impl ForeignKeyValueRenderContext for TestRenderContext {
        fn enum_label_by_oid(&self, oid: u32) -> Option<String> {
            (oid == 7).then(|| "seven".into())
        }

        fn decode_indirect_varlena(&self, _indirect: &IndirectVarlenaValue) -> Option<Value> {
            None
        }

        fn datetime_config(&self) -> &DateTimeConfig {
            &self.0
        }
    }

    #[test]
    fn key_value_rendering_uses_enum_labels_and_text_policy() {
        let ctx = TestRenderContext(DateTimeConfig::default());
        assert_eq!(
            render_key_values(
                &[
                    Value::Int32(1),
                    Value::EnumOid(7),
                    Value::EnumOid(8),
                    Value::Text("abc".into()),
                    Value::Null,
                ],
                &ctx,
            ),
            "1, seven, 8, abc, null"
        );
    }
}
