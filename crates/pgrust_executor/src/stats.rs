use std::time::Duration;

use pgrust_nodes::Value;
use pgrust_nodes::datetime::TimestampTzADT;
use pgrust_nodes::datum::RecordValue;
use pgrust_nodes::primnodes::BuiltinScalarFunction;
use pgrust_storage::BufferUsageStats;

#[derive(Debug, Clone, Default)]
pub struct NodeExecStats {
    pub loops: u64,
    pub rows: u64,
    pub total_time: Duration,
    pub first_tuple_time: Option<Duration>,
    pub rows_removed_by_filter: u64,
    pub rows_removed_by_index_recheck: u64,
    pub index_searches: u64,
    pub heap_fetches: u64,
    pub stack_depth_checked: bool,
    pub buffer_usage: BufferUsageStats,
    pub buffer_usage_start: Option<BufferUsageStats>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatsArgError {
    MalformedCall {
        op: &'static str,
    },
    TypeMismatch {
        op: &'static str,
        left: Value,
        right: Value,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RelationStatsSnapshot {
    pub numscans: i64,
    pub tuples_returned: i64,
    pub tuples_fetched: i64,
    pub tuples_inserted: i64,
    pub tuples_updated: i64,
    pub tuples_hot_updated: i64,
    pub tuples_deleted: i64,
    pub live_tuples: i64,
    pub dead_tuples: i64,
    pub blocks_fetched: i64,
    pub blocks_hit: i64,
    pub lastscan: Option<TimestampTzADT>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FunctionStatsSnapshot {
    pub calls: i64,
    pub total_time_micros: u64,
    pub self_time_micros: u64,
}

pub fn stats_oid_arg(values: &[Value], op: &'static str) -> Result<u32, StatsArgError> {
    match values.first() {
        Some(Value::Int32(v)) if *v >= 0 => Ok(*v as u32),
        Some(Value::Int64(v)) if *v >= 0 && *v <= i64::from(u32::MAX) => Ok(*v as u32),
        Some(other) => Err(StatsArgError::TypeMismatch {
            op,
            left: other.clone(),
            right: Value::Int64(i64::from(pgrust_catalog_data::OID_TYPE_OID)),
        }),
        None => Err(StatsArgError::MalformedCall { op }),
    }
}

pub fn relation_stats_value(func: BuiltinScalarFunction, entry: &RelationStatsSnapshot) -> Value {
    match func {
        BuiltinScalarFunction::PgStatGetNumscans => Value::Int64(entry.numscans),
        BuiltinScalarFunction::PgStatGetLastscan => entry
            .lastscan
            .map(Value::TimestampTz)
            .unwrap_or(Value::Null),
        BuiltinScalarFunction::PgStatGetTuplesReturned => Value::Int64(entry.tuples_returned),
        BuiltinScalarFunction::PgStatGetTuplesFetched => Value::Int64(entry.tuples_fetched),
        BuiltinScalarFunction::PgStatGetTuplesInserted => Value::Int64(entry.tuples_inserted),
        BuiltinScalarFunction::PgStatGetTuplesUpdated => Value::Int64(entry.tuples_updated),
        BuiltinScalarFunction::PgStatGetTuplesHotUpdated => Value::Int64(entry.tuples_hot_updated),
        BuiltinScalarFunction::PgStatGetTuplesDeleted => Value::Int64(entry.tuples_deleted),
        BuiltinScalarFunction::PgStatGetLiveTuples => Value::Int64(entry.live_tuples),
        BuiltinScalarFunction::PgStatGetDeadTuples => Value::Int64(entry.dead_tuples),
        BuiltinScalarFunction::PgStatGetBlocksFetched => Value::Int64(entry.blocks_fetched),
        BuiltinScalarFunction::PgStatGetBlocksHit => Value::Int64(entry.blocks_hit),
        _ => unreachable!("non-relation stats builtin in relation_stats_value"),
    }
}

pub fn relation_xact_stats_value(
    func: BuiltinScalarFunction,
    current: &RelationStatsSnapshot,
) -> Value {
    match func {
        BuiltinScalarFunction::PgStatGetXactNumscans => Value::Int64(current.numscans),
        BuiltinScalarFunction::PgStatGetXactTuplesReturned => Value::Int64(current.tuples_returned),
        BuiltinScalarFunction::PgStatGetXactTuplesFetched => Value::Int64(current.tuples_fetched),
        BuiltinScalarFunction::PgStatGetXactTuplesInserted => Value::Int64(current.tuples_inserted),
        BuiltinScalarFunction::PgStatGetXactTuplesUpdated => Value::Int64(current.tuples_updated),
        BuiltinScalarFunction::PgStatGetXactTuplesDeleted => Value::Int64(current.tuples_deleted),
        _ => unreachable!("non-xact relation stats builtin in relation_xact_stats_value"),
    }
}

pub fn function_stats_value(func: BuiltinScalarFunction, entry: &FunctionStatsSnapshot) -> Value {
    match func {
        BuiltinScalarFunction::PgStatGetFunctionCalls => Value::Int64(entry.calls),
        BuiltinScalarFunction::PgStatGetFunctionTotalTime => {
            Value::Float64(entry.total_time_micros as f64 / 1000.0)
        }
        BuiltinScalarFunction::PgStatGetFunctionSelfTime => {
            Value::Float64(entry.self_time_micros as f64 / 1000.0)
        }
        _ => unreachable!("non-function stats builtin in function_stats_value"),
    }
}

pub fn function_xact_stats_value(
    func: BuiltinScalarFunction,
    entry: &FunctionStatsSnapshot,
) -> Value {
    match func {
        BuiltinScalarFunction::PgStatGetXactFunctionCalls => Value::Int64(entry.calls),
        BuiltinScalarFunction::PgStatGetXactFunctionTotalTime => {
            Value::Float64(entry.total_time_micros as f64 / 1000.0)
        }
        BuiltinScalarFunction::PgStatGetXactFunctionSelfTime => {
            Value::Float64(entry.self_time_micros as f64 / 1000.0)
        }
        _ => unreachable!("non-xact function stats builtin in function_xact_stats_value"),
    }
}

pub fn pg_stat_get_backend_pid_value(
    values: &[Value],
    current_backend_id: i32,
    client_id: u32,
) -> Value {
    let backend_id = values.first().and_then(|value| match value {
        Value::Int32(value) => Some(*value),
        Value::Int64(value) => i32::try_from(*value).ok(),
        _ => None,
    });
    Value::Int32(
        (backend_id == Some(current_backend_id))
            .then_some(client_id as i32)
            .unwrap_or(0),
    )
}

pub fn pg_stat_get_backend_wal_value(
    values: &[Value],
    client_id: u32,
    wal_bytes: i64,
    stats_reset: TimestampTzADT,
) -> Value {
    let pid = values.first().and_then(|value| match value {
        Value::Int32(value) => Some(*value),
        Value::Int64(value) => i32::try_from(*value).ok(),
        _ => None,
    });
    if pid != Some(client_id as i32) {
        return Value::Null;
    }
    Value::Record(RecordValue::anonymous(vec![
        ("wal_records".into(), Value::Int64(0)),
        ("wal_fpi".into(), Value::Int64(0)),
        ("wal_bytes".into(), Value::Int64(wal_bytes)),
        ("wal_buffers_full".into(), Value::Int64(0)),
        ("stats_reset".into(), Value::TimestampTz(stats_reset)),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_oid_arg_accepts_positive_ints_and_reports_shape_errors() {
        assert_eq!(stats_oid_arg(&[Value::Int32(42)], "pg_stat_get_*"), Ok(42));
        assert_eq!(stats_oid_arg(&[Value::Int64(42)], "pg_stat_get_*"), Ok(42));
        assert!(matches!(
            stats_oid_arg(&[], "pg_stat_get_*"),
            Err(StatsArgError::MalformedCall {
                op: "pg_stat_get_*"
            })
        ));
        assert!(matches!(
            stats_oid_arg(&[Value::Int32(-1)], "pg_stat_get_*"),
            Err(StatsArgError::TypeMismatch { .. })
        ));
    }

    #[test]
    fn relation_and_function_stats_map_to_sql_values() {
        let relation = RelationStatsSnapshot {
            numscans: 3,
            tuples_returned: 4,
            blocks_hit: 5,
            ..Default::default()
        };
        assert_eq!(
            relation_stats_value(BuiltinScalarFunction::PgStatGetNumscans, &relation),
            Value::Int64(3)
        );
        assert_eq!(
            relation_stats_value(BuiltinScalarFunction::PgStatGetBlocksHit, &relation),
            Value::Int64(5)
        );

        let function = FunctionStatsSnapshot {
            calls: 2,
            total_time_micros: 1500,
            self_time_micros: 500,
        };
        assert_eq!(
            function_stats_value(BuiltinScalarFunction::PgStatGetFunctionCalls, &function),
            Value::Int64(2)
        );
        assert_eq!(
            function_stats_value(BuiltinScalarFunction::PgStatGetFunctionTotalTime, &function),
            Value::Float64(1.5)
        );
    }

    #[test]
    fn backend_stats_helpers_shape_pid_and_wal_values() {
        assert_eq!(
            pg_stat_get_backend_pid_value(&[Value::Int32(7)], 7, 42),
            Value::Int32(42)
        );
        assert_eq!(
            pg_stat_get_backend_pid_value(&[Value::Int32(8)], 7, 42),
            Value::Int32(0)
        );

        let wal = pg_stat_get_backend_wal_value(&[Value::Int32(42)], 42, 99, TimestampTzADT(123));
        let Value::Record(record) = wal else {
            panic!("backend wal stats should return a record");
        };
        assert_eq!(record.fields[2], Value::Int64(99));
        assert_eq!(
            pg_stat_get_backend_wal_value(&[Value::Int32(41)], 42, 99, TimestampTzADT(123)),
            Value::Null
        );
    }
}
