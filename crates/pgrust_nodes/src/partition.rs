use serde::{Deserialize, Serialize};

use crate::parsenodes::{PartitionStrategy, SqlType};
use crate::primnodes::Expr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoweredPartitionSpec {
    pub strategy: PartitionStrategy,
    pub key_columns: Vec<String>,
    pub key_exprs: Vec<Expr>,
    pub key_types: Vec<SqlType>,
    pub key_sqls: Vec<String>,
    pub partattrs: Vec<i16>,
    pub partclass: Vec<u32>,
    pub partcollation: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoweredPartitionClause {
    pub parent_oid: Option<u32>,
    pub spec: Option<LoweredPartitionSpec>,
    pub bound: Option<PartitionBoundSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SerializedPartitionValue {
    Null,
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Money(i64),
    Float64(String),
    Numeric(String),
    Text(String),
    Bytea(Vec<u8>),
    Json(String),
    Jsonb(Vec<u8>),
    JsonPath(String),
    Xml(String),
    InternalChar(u8),
    Bool(bool),
    Date(i32),
    Time(i64),
    TimeTz { time: i64, offset_seconds: i32 },
    Timestamp(i64),
    TimestampTz(i64),
    EnumOid(u32),
    Array(Box<SerializedPartitionArrayValue>),
    Record(Box<SerializedPartitionRecordValue>),
    Range(Box<SerializedPartitionRangeValue>),
    Multirange(Box<SerializedPartitionMultirangeValue>),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SerializedPartitionRangeBound {
    pub value: SerializedPartitionValue,
    pub inclusive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SerializedPartitionRangeValue {
    pub range_type_oid: u32,
    pub empty: bool,
    pub lower: Option<SerializedPartitionRangeBound>,
    pub upper: Option<SerializedPartitionRangeBound>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SerializedPartitionMultirangeValue {
    pub multirange_type_oid: u32,
    pub ranges: Vec<SerializedPartitionRangeValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SerializedPartitionArrayValue {
    pub element_type_oid: Option<u32>,
    pub type_name: String,
    pub dimensions: Vec<(i32, usize)>,
    pub elements: Vec<SerializedPartitionValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SerializedPartitionRecordField {
    pub name: String,
    pub sql_type: SqlType,
    pub value: SerializedPartitionValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SerializedPartitionRecordValue {
    pub type_oid: u32,
    pub typrelid: u32,
    pub typmod: i32,
    pub fields: Vec<SerializedPartitionRecordField>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionRangeDatumValue {
    MinValue,
    MaxValue,
    Value(SerializedPartitionValue),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionBoundSpec {
    List {
        values: Vec<SerializedPartitionValue>,
        is_default: bool,
    },
    Range {
        from: Vec<PartitionRangeDatumValue>,
        to: Vec<PartitionRangeDatumValue>,
        is_default: bool,
    },
    Hash {
        modulus: i32,
        remainder: i32,
    },
}

impl PartitionBoundSpec {
    pub fn is_default(&self) -> bool {
        match self {
            Self::List { is_default, .. } | Self::Range { is_default, .. } => *is_default,
            Self::Hash { .. } => false,
        }
    }
}
