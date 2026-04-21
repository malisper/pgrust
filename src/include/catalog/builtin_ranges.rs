use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{
    BOOL_TYPE_OID, BOOTSTRAP_SUPERUSER_OID, DATEMULTIRANGE_TYPE_OID, DATE_TYPE_OID,
    DATERANGE_TYPE_OID, INT4MULTIRANGE_TYPE_OID, INT4_TYPE_OID, INT4RANGE_TYPE_OID,
    INT8MULTIRANGE_TYPE_OID, INT8_TYPE_OID, INT8RANGE_TYPE_OID, NUMERIC_TYPE_OID,
    NUMMULTIRANGE_TYPE_OID, NUMRANGE_TYPE_OID, PG_CATALOG_NAMESPACE_OID,
    PG_LANGUAGE_INTERNAL_OID, TEXT_TYPE_OID, TIMESTAMP_TYPE_OID, TIMESTAMPTZ_TYPE_OID,
    TSMULTIRANGE_TYPE_OID, TSRANGE_TYPE_OID, TSTZMULTIRANGE_TYPE_OID, TSTZRANGE_TYPE_OID,
};
use crate::include::catalog::{PgProcRow, PgRangeRow, PgTypeRow};
use crate::include::nodes::datum::{MultirangeTypeRef, RangeTypeRef};
use crate::include::nodes::primnodes::AggFunc;
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RangeCanonicalization {
    Discrete,
    Continuous,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BuiltinRangeSpec {
    pub range_type: RangeTypeRef,
    pub oid: u32,
    pub name: &'static str,
    pub multirange_oid: u32,
    pub multirange_name: &'static str,
}

const BUILTIN_RANGE_SPECS: [BuiltinRangeSpec; 6] =
    [
        BuiltinRangeSpec {
            range_type: RangeTypeRef {
                sql_type: SqlType::range(INT4RANGE_TYPE_OID, INT4_TYPE_OID).with_range_metadata(
                    INT4_TYPE_OID,
                    INT4MULTIRANGE_TYPE_OID,
                    true,
                ),
                subtype: SqlType::new(SqlTypeKind::Int4).with_identity(INT4_TYPE_OID, 0),
                multirange_type_oid: INT4MULTIRANGE_TYPE_OID,
                canonicalization: RangeCanonicalization::Discrete,
            },
            oid: INT4RANGE_TYPE_OID,
            name: "int4range",
            multirange_oid: INT4MULTIRANGE_TYPE_OID,
            multirange_name: "int4multirange",
        },
        BuiltinRangeSpec {
            range_type: RangeTypeRef {
                sql_type: SqlType::range(INT8RANGE_TYPE_OID, INT8_TYPE_OID).with_range_metadata(
                    INT8_TYPE_OID,
                    INT8MULTIRANGE_TYPE_OID,
                    true,
                ),
                subtype: SqlType::new(SqlTypeKind::Int8).with_identity(INT8_TYPE_OID, 0),
                multirange_type_oid: INT8MULTIRANGE_TYPE_OID,
                canonicalization: RangeCanonicalization::Discrete,
            },
            oid: INT8RANGE_TYPE_OID,
            name: "int8range",
            multirange_oid: INT8MULTIRANGE_TYPE_OID,
            multirange_name: "int8multirange",
        },
        BuiltinRangeSpec {
            range_type: RangeTypeRef {
                sql_type: SqlType::range(NUMRANGE_TYPE_OID, NUMERIC_TYPE_OID).with_range_metadata(
                    NUMERIC_TYPE_OID,
                    NUMMULTIRANGE_TYPE_OID,
                    false,
                ),
                subtype: SqlType::new(SqlTypeKind::Numeric).with_identity(NUMERIC_TYPE_OID, 0),
                multirange_type_oid: NUMMULTIRANGE_TYPE_OID,
                canonicalization: RangeCanonicalization::Continuous,
            },
            oid: NUMRANGE_TYPE_OID,
            name: "numrange",
            multirange_oid: NUMMULTIRANGE_TYPE_OID,
            multirange_name: "nummultirange",
        },
        BuiltinRangeSpec {
            range_type: RangeTypeRef {
                sql_type: SqlType::range(DATERANGE_TYPE_OID, DATE_TYPE_OID).with_range_metadata(
                    DATE_TYPE_OID,
                    DATEMULTIRANGE_TYPE_OID,
                    true,
                ),
                subtype: SqlType::new(SqlTypeKind::Date).with_identity(DATE_TYPE_OID, 0),
                multirange_type_oid: DATEMULTIRANGE_TYPE_OID,
                canonicalization: RangeCanonicalization::Discrete,
            },
            oid: DATERANGE_TYPE_OID,
            name: "daterange",
            multirange_oid: DATEMULTIRANGE_TYPE_OID,
            multirange_name: "datemultirange",
        },
        BuiltinRangeSpec {
            range_type: RangeTypeRef {
                sql_type: SqlType::range(TSRANGE_TYPE_OID, TIMESTAMP_TYPE_OID).with_range_metadata(
                    TIMESTAMP_TYPE_OID,
                    TSMULTIRANGE_TYPE_OID,
                    false,
                ),
                subtype: SqlType::new(SqlTypeKind::Timestamp).with_identity(TIMESTAMP_TYPE_OID, 0),
                multirange_type_oid: TSMULTIRANGE_TYPE_OID,
                canonicalization: RangeCanonicalization::Continuous,
            },
            oid: TSRANGE_TYPE_OID,
            name: "tsrange",
            multirange_oid: TSMULTIRANGE_TYPE_OID,
            multirange_name: "tsmultirange",
        },
        BuiltinRangeSpec {
            range_type: RangeTypeRef {
                sql_type: SqlType::range(TSTZRANGE_TYPE_OID, TIMESTAMPTZ_TYPE_OID)
                    .with_range_metadata(TIMESTAMPTZ_TYPE_OID, TSTZMULTIRANGE_TYPE_OID, false),
                subtype: SqlType::new(SqlTypeKind::TimestampTz)
                    .with_identity(TIMESTAMPTZ_TYPE_OID, 0),
                multirange_type_oid: TSTZMULTIRANGE_TYPE_OID,
                canonicalization: RangeCanonicalization::Continuous,
            },
            oid: TSTZRANGE_TYPE_OID,
            name: "tstzrange",
            multirange_oid: TSTZMULTIRANGE_TYPE_OID,
            multirange_name: "tstzmultirange",
        },
    ];

const DYNAMIC_RANGE_PROC_OID_BASE: u32 = 0x6000_0000;
const DYNAMIC_RANGE_PROC_OID_MASK: u32 = 0x007f_ffff;

const RANGE_PROC_CONSTRUCTOR_2: u32 = 0;
const RANGE_PROC_CONSTRUCTOR_3: u32 = 1;
const RANGE_PROC_ISEMPTY: u32 = 2;
const RANGE_PROC_LOWER: u32 = 3;
const RANGE_PROC_UPPER: u32 = 4;
const RANGE_PROC_LOWER_INC: u32 = 5;
const RANGE_PROC_UPPER_INC: u32 = 6;
const RANGE_PROC_LOWER_INF: u32 = 7;
const RANGE_PROC_UPPER_INF: u32 = 8;
const RANGE_PROC_MERGE: u32 = 9;
const RANGE_PROC_ADJACENT: u32 = 10;
const RANGE_PROC_MINUS: u32 = 11;
const RANGE_PROC_CONTAINS_RANGE: u32 = 12;
const RANGE_PROC_CONTAINED_RANGE: u32 = 13;
const RANGE_PROC_CONTAINS_ELEM: u32 = 14;
const RANGE_PROC_CONTAINED_ELEM: u32 = 15;
const RANGE_PROC_INTERSECT_AGG: u32 = 16;
const RANGE_PROC_MULTIRANGE_CONSTRUCTOR: u32 = 17;
const RANGE_PROC_MULTIRANGE_CAST: u32 = 18;
const RANGE_PROC_MULTIRANGE_ISEMPTY: u32 = 19;
const RANGE_PROC_MULTIRANGE_LOWER: u32 = 20;
const RANGE_PROC_MULTIRANGE_UPPER: u32 = 21;
const RANGE_PROC_MULTIRANGE_LOWER_INC: u32 = 22;
const RANGE_PROC_MULTIRANGE_UPPER_INC: u32 = 23;
const RANGE_PROC_MULTIRANGE_LOWER_INF: u32 = 24;
const RANGE_PROC_MULTIRANGE_UPPER_INF: u32 = 25;
const RANGE_PROC_MULTIRANGE_RANGE_AGG_RANGE: u32 = 26;
const RANGE_PROC_MULTIRANGE_RANGE_AGG_MULTIRANGE: u32 = 27;
const RANGE_PROC_MULTIRANGE_INTERSECT_AGG: u32 = 28;
const RANGE_PROC_MULTIRANGE_UNNEST: u32 = 29;
const RANGE_PROC_MULTIRANGE_MERGE: u32 = 30;
const RANGE_PROC_MULTIRANGE_DIRECT_FUNC_BASE: u32 = 31;

pub fn builtin_range_specs() -> &'static [BuiltinRangeSpec] {
    &BUILTIN_RANGE_SPECS
}

pub fn builtin_range_rows() -> Vec<PgRangeRow> {
    builtin_range_specs()
        .iter()
        .map(|spec| PgRangeRow {
            rngtypid: spec.oid,
            rngsubtype: spec.range_type.subtype_oid(),
            rngmultitypid: spec.multirange_oid,
            rngcollation: 0,
            rngcanonical: None,
            rngsubdiff: None,
            canonicalization: spec.range_type.canonicalization,
        })
        .collect()
}

pub fn builtin_range_spec_by_oid(oid: u32) -> Option<&'static BuiltinRangeSpec> {
    builtin_range_specs().iter().find(|spec| spec.oid == oid)
}

pub fn builtin_range_spec_by_multirange_oid(oid: u32) -> Option<&'static BuiltinRangeSpec> {
    builtin_range_specs()
        .iter()
        .find(|spec| spec.multirange_oid == oid)
}

fn legacy_builtin_range_oid(kind: SqlTypeKind) -> Option<u32> {
    match kind {
        SqlTypeKind::Int4Range => Some(INT4RANGE_TYPE_OID),
        SqlTypeKind::Int8Range => Some(INT8RANGE_TYPE_OID),
        SqlTypeKind::NumericRange => Some(NUMRANGE_TYPE_OID),
        SqlTypeKind::DateRange => Some(DATERANGE_TYPE_OID),
        SqlTypeKind::TimestampRange => Some(TSRANGE_TYPE_OID),
        SqlTypeKind::TimestampTzRange => Some(TSTZRANGE_TYPE_OID),
        _ => None,
    }
}

pub fn builtin_range_spec_for_sql_type(sql_type: SqlType) -> Option<&'static BuiltinRangeSpec> {
    let sql_type = sql_type.element_type();
    if sql_type.type_oid != 0 {
        if let Some(spec) = builtin_range_spec_by_oid(sql_type.type_oid) {
            return Some(spec);
        }
    }
    if let Some(oid) = legacy_builtin_range_oid(sql_type.kind) {
        return builtin_range_spec_by_oid(oid);
    }
    builtin_range_specs().iter().find(|spec| {
        spec.range_type.sql_type == sql_type
            || (sql_type.type_oid != 0 && spec.range_type.type_oid() == sql_type.type_oid)
    })
}

pub fn builtin_range_name_for_sql_type(sql_type: SqlType) -> Option<&'static str> {
    builtin_range_spec_for_sql_type(sql_type).map(|spec| spec.name)
}

pub fn builtin_multirange_name_for_sql_type(sql_type: SqlType) -> Option<&'static str> {
    multirange_type_ref_for_sql_type(sql_type).and_then(|multirange_type| {
        builtin_range_spec_by_multirange_oid(multirange_type.type_oid()).map(|spec| spec.multirange_name)
    })
}

pub fn range_type_ref_for_sql_type(sql_type: SqlType) -> Option<RangeTypeRef> {
    if let Some(spec) = builtin_range_spec_for_sql_type(sql_type) {
        return Some(spec.range_type);
    }
    let sql_type = sql_type.element_type();
    if !matches!(sql_type.kind, SqlTypeKind::Range) || sql_type.range_subtype_oid == 0 {
        return None;
    }
    let subtype = crate::include::catalog::builtin_type_rows()
        .into_iter()
        .find(|row| row.oid == sql_type.range_subtype_oid)
        .map(|row| row.sql_type.with_identity(row.oid, row.typrelid))?;
    Some(RangeTypeRef {
        sql_type,
        subtype,
        multirange_type_oid: sql_type.range_multitype_oid,
        canonicalization: if sql_type.range_discrete {
            RangeCanonicalization::Discrete
        } else {
            RangeCanonicalization::Continuous
        },
    })
}

pub fn range_type_ref_for_multirange_sql_type(sql_type: SqlType) -> Option<RangeTypeRef> {
    let sql_type = sql_type.element_type();
    if let Some(spec) = builtin_range_spec_by_multirange_oid(sql_type.type_oid) {
        return Some(spec.range_type);
    }
    if !matches!(sql_type.kind, SqlTypeKind::Multirange) || sql_type.multirange_range_oid == 0 {
        return None;
    }
    let subtype = crate::include::catalog::builtin_type_rows()
        .into_iter()
        .find(|row| row.oid == sql_type.range_subtype_oid)
        .map(|row| row.sql_type.with_identity(row.oid, row.typrelid))?;
    Some(RangeTypeRef {
        sql_type: SqlType::range(sql_type.multirange_range_oid, sql_type.range_subtype_oid)
            .with_identity(sql_type.multirange_range_oid, 0)
            .with_range_metadata(
                sql_type.range_subtype_oid,
                sql_type.type_oid,
                sql_type.range_discrete,
            ),
        subtype,
        multirange_type_oid: sql_type.type_oid,
        canonicalization: if sql_type.range_discrete {
            RangeCanonicalization::Discrete
        } else {
            RangeCanonicalization::Continuous
        },
    })
}

pub fn multirange_type_ref_for_sql_type(sql_type: SqlType) -> Option<MultirangeTypeRef> {
    let sql_type = sql_type.element_type();
    let range_type = range_type_ref_for_multirange_sql_type(sql_type)?;
    let multirange_sql_type = if let Some(spec) = builtin_range_spec_by_multirange_oid(sql_type.type_oid) {
        SqlType::multirange(spec.multirange_oid, spec.oid)
            .with_identity(spec.multirange_oid, 0)
    } else {
        sql_type
    };
    Some(MultirangeTypeRef {
        sql_type: multirange_sql_type,
        range_type,
    })
}

pub fn synthetic_range_proc_rows(
    type_rows: &[PgTypeRow],
    range_rows: &[PgRangeRow],
) -> Vec<PgProcRow> {
    let mut rows = Vec::new();
    let mut seen_constructor_names = BTreeSet::new();
    let mut seen_multirange_constructor_names = BTreeSet::new();
    for type_row in type_rows.iter().filter(|row| row.typelem == 0) {
        let Some(range_row) = range_rows.iter().find(|row| row.rngtypid == type_row.oid) else {
            continue;
        };
        let range_oid = type_row.oid;
        let subtype_oid = range_row.rngsubtype;
        let Some(multirange_type_row) = type_rows
            .iter()
            .find(|row| row.oid == range_row.rngmultitypid && row.typelem == 0)
        else {
            continue;
        };
        let Some(range_array_oid) = array_type_oid_for_element(type_rows, range_oid) else {
            continue;
        };
        let multirange_oid = multirange_type_row.oid;
        if builtin_range_spec_by_oid(type_row.oid).is_none()
            && seen_constructor_names.insert(type_row.typname.to_ascii_lowercase())
        {
            rows.push(range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_CONSTRUCTOR_2),
                &type_row.typname,
                type_row.typnamespace,
                range_oid,
                &[subtype_oid, subtype_oid],
                "range_constructor",
                'f',
            ));
            rows.push(range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_CONSTRUCTOR_3),
                &type_row.typname,
                type_row.typnamespace,
                range_oid,
                &[subtype_oid, subtype_oid, TEXT_TYPE_OID],
                "range_constructor",
                'f',
            ));
        }
        if seen_multirange_constructor_names
            .insert(multirange_type_row.typname.to_ascii_lowercase())
        {
            rows.push(range_variadic_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_MULTIRANGE_CONSTRUCTOR),
                &multirange_type_row.typname,
                multirange_type_row.typnamespace,
                multirange_oid,
                &[range_array_oid],
                range_oid,
                "range_constructor",
                'f',
            ));
        }
        rows.extend([
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_MULTIRANGE_CAST),
                "multirange",
                PG_CATALOG_NAMESPACE_OID,
                multirange_oid,
                &[range_oid],
                "range_constructor",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_ISEMPTY),
                "isempty",
                PG_CATALOG_NAMESPACE_OID,
                BOOL_TYPE_OID,
                &[range_oid],
                "range_isempty",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_LOWER),
                "lower",
                PG_CATALOG_NAMESPACE_OID,
                subtype_oid,
                &[range_oid],
                "range_lower",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_UPPER),
                "upper",
                PG_CATALOG_NAMESPACE_OID,
                subtype_oid,
                &[range_oid],
                "range_upper",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_LOWER_INC),
                "lower_inc",
                PG_CATALOG_NAMESPACE_OID,
                BOOL_TYPE_OID,
                &[range_oid],
                "range_lower_inc",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_UPPER_INC),
                "upper_inc",
                PG_CATALOG_NAMESPACE_OID,
                BOOL_TYPE_OID,
                &[range_oid],
                "range_upper_inc",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_LOWER_INF),
                "lower_inf",
                PG_CATALOG_NAMESPACE_OID,
                BOOL_TYPE_OID,
                &[range_oid],
                "range_lower_inf",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_UPPER_INF),
                "upper_inf",
                PG_CATALOG_NAMESPACE_OID,
                BOOL_TYPE_OID,
                &[range_oid],
                "range_upper_inf",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_MERGE),
                "range_merge",
                PG_CATALOG_NAMESPACE_OID,
                range_oid,
                &[range_oid, range_oid],
                "range_merge",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_ADJACENT),
                "range_adjacent",
                PG_CATALOG_NAMESPACE_OID,
                BOOL_TYPE_OID,
                &[range_oid, range_oid],
                "range_adjacent",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_MINUS),
                "range_minus",
                PG_CATALOG_NAMESPACE_OID,
                range_oid,
                &[range_oid, range_oid],
                "range_difference",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_CONTAINS_RANGE),
                "range_contains",
                PG_CATALOG_NAMESPACE_OID,
                BOOL_TYPE_OID,
                &[range_oid, range_oid],
                "range_contains",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_CONTAINED_RANGE),
                "range_contained_by",
                PG_CATALOG_NAMESPACE_OID,
                BOOL_TYPE_OID,
                &[range_oid, range_oid],
                "range_contained_by",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_CONTAINS_ELEM),
                "range_contains",
                PG_CATALOG_NAMESPACE_OID,
                BOOL_TYPE_OID,
                &[range_oid, subtype_oid],
                "range_contains",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_CONTAINED_ELEM),
                "range_contained_by",
                PG_CATALOG_NAMESPACE_OID,
                BOOL_TYPE_OID,
                &[subtype_oid, range_oid],
                "range_contained_by",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_INTERSECT_AGG),
                "range_intersect_agg",
                PG_CATALOG_NAMESPACE_OID,
                range_oid,
                &[range_oid],
                "range_intersect_agg",
                'a',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_MULTIRANGE_ISEMPTY),
                "isempty",
                PG_CATALOG_NAMESPACE_OID,
                BOOL_TYPE_OID,
                &[multirange_oid],
                "range_isempty",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_MULTIRANGE_LOWER),
                "lower",
                PG_CATALOG_NAMESPACE_OID,
                subtype_oid,
                &[multirange_oid],
                "range_lower",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_MULTIRANGE_UPPER),
                "upper",
                PG_CATALOG_NAMESPACE_OID,
                subtype_oid,
                &[multirange_oid],
                "range_upper",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_MULTIRANGE_LOWER_INC),
                "lower_inc",
                PG_CATALOG_NAMESPACE_OID,
                BOOL_TYPE_OID,
                &[multirange_oid],
                "range_lower_inc",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_MULTIRANGE_UPPER_INC),
                "upper_inc",
                PG_CATALOG_NAMESPACE_OID,
                BOOL_TYPE_OID,
                &[multirange_oid],
                "range_upper_inc",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_MULTIRANGE_LOWER_INF),
                "lower_inf",
                PG_CATALOG_NAMESPACE_OID,
                BOOL_TYPE_OID,
                &[multirange_oid],
                "range_lower_inf",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_MULTIRANGE_UPPER_INF),
                "upper_inf",
                PG_CATALOG_NAMESPACE_OID,
                BOOL_TYPE_OID,
                &[multirange_oid],
                "range_upper_inf",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_MULTIRANGE_MERGE),
                "range_merge",
                PG_CATALOG_NAMESPACE_OID,
                range_oid,
                &[multirange_oid],
                "range_merge",
                'f',
            ),
            range_set_returning_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_MULTIRANGE_UNNEST),
                "unnest",
                PG_CATALOG_NAMESPACE_OID,
                range_oid,
                &[multirange_oid],
                "unnest",
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_MULTIRANGE_RANGE_AGG_RANGE),
                "range_agg",
                PG_CATALOG_NAMESPACE_OID,
                multirange_oid,
                &[range_oid],
                "range_agg",
                'a',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_MULTIRANGE_RANGE_AGG_MULTIRANGE),
                "range_agg",
                PG_CATALOG_NAMESPACE_OID,
                multirange_oid,
                &[multirange_oid],
                "range_agg",
                'a',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_MULTIRANGE_INTERSECT_AGG),
                "range_intersect_agg",
                PG_CATALOG_NAMESPACE_OID,
                multirange_oid,
                &[multirange_oid],
                "range_intersect_agg",
                'a',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_MULTIRANGE_DIRECT_FUNC_BASE),
                "range_overlaps_multirange",
                PG_CATALOG_NAMESPACE_OID,
                BOOL_TYPE_OID,
                &[range_oid, multirange_oid],
                "range_overlap",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_MULTIRANGE_DIRECT_FUNC_BASE + 1),
                "multirange_overlaps_range",
                PG_CATALOG_NAMESPACE_OID,
                BOOL_TYPE_OID,
                &[multirange_oid, range_oid],
                "range_overlap",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_MULTIRANGE_DIRECT_FUNC_BASE + 2),
                "multirange_overlaps_multirange",
                PG_CATALOG_NAMESPACE_OID,
                BOOL_TYPE_OID,
                &[multirange_oid, multirange_oid],
                "range_overlap",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_MULTIRANGE_DIRECT_FUNC_BASE + 3),
                "multirange_contains_elem",
                PG_CATALOG_NAMESPACE_OID,
                BOOL_TYPE_OID,
                &[multirange_oid, subtype_oid],
                "range_contains",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_MULTIRANGE_DIRECT_FUNC_BASE + 4),
                "multirange_contains_range",
                PG_CATALOG_NAMESPACE_OID,
                BOOL_TYPE_OID,
                &[multirange_oid, range_oid],
                "range_contains",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_MULTIRANGE_DIRECT_FUNC_BASE + 5),
                "multirange_contains_multirange",
                PG_CATALOG_NAMESPACE_OID,
                BOOL_TYPE_OID,
                &[multirange_oid, multirange_oid],
                "range_contains",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_MULTIRANGE_DIRECT_FUNC_BASE + 6),
                "elem_contained_by_multirange",
                PG_CATALOG_NAMESPACE_OID,
                BOOL_TYPE_OID,
                &[subtype_oid, multirange_oid],
                "range_contained_by",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_MULTIRANGE_DIRECT_FUNC_BASE + 7),
                "range_contained_by_multirange",
                PG_CATALOG_NAMESPACE_OID,
                BOOL_TYPE_OID,
                &[range_oid, multirange_oid],
                "range_contained_by",
                'f',
            ),
            range_proc_row(
                dynamic_range_proc_oid(range_oid, RANGE_PROC_MULTIRANGE_DIRECT_FUNC_BASE + 8),
                "multirange_contained_by_multirange",
                PG_CATALOG_NAMESPACE_OID,
                BOOL_TYPE_OID,
                &[multirange_oid, multirange_oid],
                "range_contained_by",
                'f',
            ),
        ]);
    }
    rows
}

pub fn synthetic_range_proc_rows_by_name(
    name: &str,
    type_rows: &[PgTypeRow],
    range_rows: &[PgRangeRow],
) -> Vec<PgProcRow> {
    let normalized = normalize_lookup_name(name);
    synthetic_range_proc_rows(type_rows, range_rows)
        .into_iter()
        .filter(|row| row.proname.eq_ignore_ascii_case(&normalized))
        .collect()
}

pub fn synthetic_range_proc_row_by_oid(
    oid: u32,
    type_rows: &[PgTypeRow],
    range_rows: &[PgRangeRow],
) -> Option<PgProcRow> {
    synthetic_range_proc_rows(type_rows, range_rows)
        .into_iter()
        .find(|row| row.oid == oid)
}

pub fn aggregate_func_for_dynamic_range_proc_oid(oid: u32) -> Option<AggFunc> {
    let slot = dynamic_range_proc_slot(oid)?;
    match slot {
        RANGE_PROC_INTERSECT_AGG | RANGE_PROC_MULTIRANGE_INTERSECT_AGG => {
            Some(AggFunc::RangeIntersectAgg)
        }
        RANGE_PROC_MULTIRANGE_RANGE_AGG_RANGE | RANGE_PROC_MULTIRANGE_RANGE_AGG_MULTIRANGE => {
            Some(AggFunc::RangeAgg)
        }
        _ => None,
    }
}

fn dynamic_range_proc_oid(range_oid: u32, slot: u32) -> u32 {
    DYNAMIC_RANGE_PROC_OID_BASE | ((range_oid & DYNAMIC_RANGE_PROC_OID_MASK) << 6) | slot
}

fn dynamic_range_proc_slot(oid: u32) -> Option<u32> {
    ((oid & 0xe000_0000) == DYNAMIC_RANGE_PROC_OID_BASE).then_some(oid & 0x3f)
}

fn array_type_oid_for_element(type_rows: &[PgTypeRow], elem_oid: u32) -> Option<u32> {
    type_rows
        .iter()
        .find(|row| row.typelem == elem_oid)
        .map(|row| row.oid)
}

fn normalize_lookup_name(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name)
}

fn range_proc_row(
    oid: u32,
    proname: &str,
    pronamespace: u32,
    prorettype: u32,
    arg_oids: &[u32],
    prosrc: &str,
    prokind: char,
) -> PgProcRow {
    PgProcRow {
        oid,
        proname: proname.into(),
        pronamespace,
        proowner: BOOTSTRAP_SUPERUSER_OID,
        prolang: PG_LANGUAGE_INTERNAL_OID,
        procost: 1.0,
        prorows: 0.0,
        provariadic: 0,
        prosupport: 0,
        prokind,
        prosecdef: false,
        proleakproof: false,
        proisstrict: prokind == 'f',
        proretset: false,
        provolatile: 'i',
        proparallel: 's',
        pronargs: arg_oids.len() as i16,
        pronargdefaults: 0,
        prorettype,
        proargtypes: oid_argtypes(arg_oids),
        proallargtypes: None,
        proargmodes: None,
        proargnames: None,
        prosrc: prosrc.into(),
    }
}

fn range_variadic_proc_row(
    oid: u32,
    proname: &str,
    pronamespace: u32,
    prorettype: u32,
    arg_oids: &[u32],
    variadic_oid: u32,
    prosrc: &str,
    prokind: char,
) -> PgProcRow {
    let mut row = range_proc_row(
        oid,
        proname,
        pronamespace,
        prorettype,
        arg_oids,
        prosrc,
        prokind,
    );
    row.provariadic = variadic_oid;
    row
}

fn range_set_returning_proc_row(
    oid: u32,
    proname: &str,
    pronamespace: u32,
    prorettype: u32,
    arg_oids: &[u32],
    prosrc: &str,
) -> PgProcRow {
    let mut row = range_proc_row(oid, proname, pronamespace, prorettype, arg_oids, prosrc, 'f');
    row.proretset = true;
    row
}

fn oid_argtypes(arg_oids: &[u32]) -> String {
    arg_oids
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(" ")
}
