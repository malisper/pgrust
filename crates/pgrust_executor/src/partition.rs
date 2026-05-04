use pgrust_catalog_data::OID_TYPE_OID;
use pgrust_nodes::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum HashPartitionArgError {
    OidOutOfRange,
    TypeMismatch {
        op: &'static str,
        left: Value,
        right: Value,
    },
    InvalidModulus {
        message: &'static str,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionErrorMessage {
    pub message: String,
    pub sqlstate: &'static str,
}

pub fn hash_partition_key_count_error(expected: usize, actual: usize) -> PartitionErrorMessage {
    PartitionErrorMessage {
        message: format!(
            "number of partitioning columns ({expected}) does not match number of partition keys provided ({actual})"
        ),
        sqlstate: "22023",
    }
}

pub fn hash_partition_key_type_error(
    key_index: usize,
    expected: String,
    actual: String,
    quoted: bool,
) -> PartitionErrorMessage {
    let (expected, actual) = if quoted {
        (format!("\"{expected}\""), format!("\"{actual}\""))
    } else {
        (expected, actual)
    };
    PartitionErrorMessage {
        message: format!(
            "column {} of the partition key has type {expected}, but supplied value is of type {actual}",
            key_index + 1
        ),
        sqlstate: "22023",
    }
}

pub fn hash_partition_relation_open_error(relation_oid: u32) -> PartitionErrorMessage {
    PartitionErrorMessage {
        message: format!("could not open relation with OID {relation_oid}"),
        sqlstate: "42P01",
    }
}

pub fn not_hash_partitioned_error(relation_name: String) -> PartitionErrorMessage {
    PartitionErrorMessage {
        message: format!("\"{relation_name}\" is not a hash partitioned table"),
        sqlstate: "22023",
    }
}

pub fn unsupported_hash_partition_key_error(message: String) -> PartitionErrorMessage {
    PartitionErrorMessage {
        message: format!("unsupported hash partition key value {message}"),
        sqlstate: "0A000",
    }
}

pub fn hash_partition_support_proc_return_error(returned: String) -> PartitionErrorMessage {
    PartitionErrorMessage {
        message: "hash partition support function returned non-integer value".into(),
        sqlstate: "XX000",
    }
    .with_detail(returned)
}

impl PartitionErrorMessage {
    fn with_detail(mut self, detail: String) -> Self {
        self.message = format!("{}\n{detail}", self.message);
        self
    }

    pub fn split_detail(self) -> (String, Option<String>, &'static str) {
        if let Some((message, detail)) = self.message.split_once('\n') {
            (message.to_string(), Some(detail.to_string()), self.sqlstate)
        } else {
            (self.message, None, self.sqlstate)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionTreeViewRow {
    pub relid: u32,
    pub parentrelid: Option<u32>,
    pub isleaf: bool,
    pub level: i32,
}

pub fn oid_arg_to_u32(value: &Value, op: &'static str) -> Result<u32, HashPartitionArgError> {
    match value {
        Value::Int32(oid) => u32::try_from(*oid).map_err(|_| HashPartitionArgError::OidOutOfRange),
        Value::Int64(oid) => u32::try_from(*oid).map_err(|_| HashPartitionArgError::OidOutOfRange),
        _ if value.as_text().is_some() => value
            .as_text()
            .expect("guarded above")
            .trim()
            .parse::<u32>()
            .map_err(|_| oid_type_mismatch(op, value)),
        _ => Err(oid_type_mismatch(op, value)),
    }
}

pub fn int32_arg(value: &Value, op: &'static str) -> Result<i32, HashPartitionArgError> {
    match value {
        Value::Int16(value) => Ok(i32::from(*value)),
        Value::Int32(value) => Ok(*value),
        Value::Int64(value) => {
            i32::try_from(*value).map_err(|_| HashPartitionArgError::TypeMismatch {
                op,
                left: Value::Int64(*value),
                right: Value::Int32(0),
            })
        }
        _ => Err(HashPartitionArgError::TypeMismatch {
            op,
            left: value.clone(),
            right: Value::Int32(0),
        }),
    }
}

pub fn validate_hash_partition_modulus_remainder(
    modulus: i32,
    remainder: i32,
) -> Result<(), HashPartitionArgError> {
    if modulus <= 0 {
        return Err(HashPartitionArgError::InvalidModulus {
            message: "modulus for hash partition must be an integer value greater than zero",
        });
    }
    if remainder < 0 {
        return Err(HashPartitionArgError::InvalidModulus {
            message: "remainder for hash partition must be an integer value greater than or equal to zero",
        });
    }
    if remainder >= modulus {
        return Err(HashPartitionArgError::InvalidModulus {
            message: "remainder for hash partition must be less than modulus",
        });
    }
    Ok(())
}

fn oid_type_mismatch(op: &'static str, value: &Value) -> HashPartitionArgError {
    HashPartitionArgError::TypeMismatch {
        op,
        left: value.clone(),
        right: Value::Int64(i64::from(OID_TYPE_OID)),
    }
}

pub fn pg_partition_tree_rows(
    rows: impl IntoIterator<Item = PartitionTreeViewRow>,
) -> Vec<Vec<Value>> {
    rows.into_iter()
        .map(|entry| {
            vec![
                Value::Int32(entry.relid as i32),
                entry
                    .parentrelid
                    .map(|oid| Value::Int32(oid as i32))
                    .unwrap_or(Value::Null),
                Value::Bool(entry.isleaf),
                Value::Int32(entry.level),
            ]
        })
        .collect()
}

pub fn pg_partition_ancestor_rows(oids: impl IntoIterator<Item = u32>) -> Vec<Vec<Value>> {
    oids.into_iter()
        .map(|oid| vec![Value::Int32(oid as i32)])
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_tree_rows_shape_pg_partition_tree_output() {
        assert_eq!(
            pg_partition_tree_rows([PartitionTreeViewRow {
                relid: 11,
                parentrelid: Some(7),
                isleaf: true,
                level: 1,
            }]),
            vec![vec![
                Value::Int32(11),
                Value::Int32(7),
                Value::Bool(true),
                Value::Int32(1),
            ]]
        );
    }

    #[test]
    fn partition_ancestor_rows_shape_single_oid_column() {
        assert_eq!(
            pg_partition_ancestor_rows([11, 7]),
            vec![vec![Value::Int32(11)], vec![Value::Int32(7)]]
        );
    }

    #[test]
    fn hash_partition_error_messages_match_postgres_shapes() {
        assert_eq!(
            hash_partition_key_count_error(2, 1).message,
            "number of partitioning columns (2) does not match number of partition keys provided (1)"
        );
        assert_eq!(
            hash_partition_key_type_error(0, "integer".into(), "text".into(), false).message,
            "column 1 of the partition key has type integer, but supplied value is of type text"
        );
        assert_eq!(
            hash_partition_key_type_error(1, "integer".into(), "text".into(), true).message,
            "column 2 of the partition key has type \"integer\", but supplied value is of type \"text\""
        );
        assert_eq!(
            not_hash_partitioned_error("parent".into()).message,
            "\"parent\" is not a hash partitioned table"
        );
    }
}
