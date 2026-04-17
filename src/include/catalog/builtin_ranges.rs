use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::PgRangeRow;
use crate::include::catalog::{
    DATERANGE_TYPE_OID, INT4RANGE_TYPE_OID, INT8RANGE_TYPE_OID, NUMRANGE_TYPE_OID,
    TSRANGE_TYPE_OID, TSTZRANGE_TYPE_OID,
};
use crate::include::nodes::datum::RangeTypeId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeCanonicalization {
    Discrete,
    Continuous,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuiltinRangeSpec {
    pub kind: RangeTypeId,
    pub sql_type: SqlType,
    pub oid: u32,
    pub name: &'static str,
    pub subtype: SqlType,
    pub canonicalization: RangeCanonicalization,
}

const BUILTIN_RANGE_SPECS: [BuiltinRangeSpec; 6] = [
    BuiltinRangeSpec {
        kind: RangeTypeId::Int4Range,
        sql_type: SqlType::new(SqlTypeKind::Int4Range),
        oid: INT4RANGE_TYPE_OID,
        name: "int4range",
        subtype: SqlType::new(SqlTypeKind::Int4),
        canonicalization: RangeCanonicalization::Discrete,
    },
    BuiltinRangeSpec {
        kind: RangeTypeId::Int8Range,
        sql_type: SqlType::new(SqlTypeKind::Int8Range),
        oid: INT8RANGE_TYPE_OID,
        name: "int8range",
        subtype: SqlType::new(SqlTypeKind::Int8),
        canonicalization: RangeCanonicalization::Discrete,
    },
    BuiltinRangeSpec {
        kind: RangeTypeId::NumericRange,
        sql_type: SqlType::new(SqlTypeKind::NumericRange),
        oid: NUMRANGE_TYPE_OID,
        name: "numrange",
        subtype: SqlType::new(SqlTypeKind::Numeric),
        canonicalization: RangeCanonicalization::Continuous,
    },
    BuiltinRangeSpec {
        kind: RangeTypeId::DateRange,
        sql_type: SqlType::new(SqlTypeKind::DateRange),
        oid: DATERANGE_TYPE_OID,
        name: "daterange",
        subtype: SqlType::new(SqlTypeKind::Date),
        canonicalization: RangeCanonicalization::Discrete,
    },
    BuiltinRangeSpec {
        kind: RangeTypeId::TimestampRange,
        sql_type: SqlType::new(SqlTypeKind::TimestampRange),
        oid: TSRANGE_TYPE_OID,
        name: "tsrange",
        subtype: SqlType::new(SqlTypeKind::Timestamp),
        canonicalization: RangeCanonicalization::Continuous,
    },
    BuiltinRangeSpec {
        kind: RangeTypeId::TimestampTzRange,
        sql_type: SqlType::new(SqlTypeKind::TimestampTzRange),
        oid: TSTZRANGE_TYPE_OID,
        name: "tstzrange",
        subtype: SqlType::new(SqlTypeKind::TimestampTz),
        canonicalization: RangeCanonicalization::Continuous,
    },
];

pub fn builtin_range_specs() -> &'static [BuiltinRangeSpec] {
    &BUILTIN_RANGE_SPECS
}

pub fn builtin_range_rows() -> Vec<PgRangeRow> {
    builtin_range_specs()
        .iter()
        .map(|spec| PgRangeRow {
            rngtypid: spec.oid,
            rngsubtype: crate::include::catalog::builtin_type_rows()
                .into_iter()
                .find(|row| row.sql_type == spec.subtype)
                .map(|row| row.oid)
                .unwrap_or(0),
            rngcollation: 0,
            rngcanonical: None,
            rngsubdiff: None,
            canonicalization: spec.canonicalization,
        })
        .collect()
}

pub fn builtin_range_spec(kind: RangeTypeId) -> &'static BuiltinRangeSpec {
    builtin_range_specs()
        .iter()
        .find(|spec| spec.kind == kind)
        .unwrap_or_else(|| panic!("missing builtin range spec for {:?}", kind))
}

pub fn builtin_range_spec_by_oid(oid: u32) -> Option<&'static BuiltinRangeSpec> {
    builtin_range_specs().iter().find(|spec| spec.oid == oid)
}

pub fn builtin_range_spec_for_sql_type(sql_type: SqlType) -> Option<&'static BuiltinRangeSpec> {
    builtin_range_specs()
        .iter()
        .find(|spec| spec.sql_type == sql_type.element_type())
}

pub fn range_kind_for_sql_type(sql_type: SqlType) -> Option<RangeTypeId> {
    builtin_range_spec_for_sql_type(sql_type).map(|spec| spec.kind)
}

pub fn sql_type_for_range_kind(kind: RangeTypeId) -> SqlType {
    builtin_range_spec(kind).sql_type
}
