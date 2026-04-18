use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{
    BOOL_TYPE_OID, BOOTSTRAP_SUPERUSER_OID, DATE_TYPE_OID, DATERANGE_TYPE_OID, INT4_TYPE_OID,
    INT4RANGE_TYPE_OID, INT8_TYPE_OID, INT8RANGE_TYPE_OID, NUMERIC_TYPE_OID, NUMRANGE_TYPE_OID,
    PG_CATALOG_NAMESPACE_OID, PG_LANGUAGE_INTERNAL_OID, TEXT_TYPE_OID, TIMESTAMP_TYPE_OID,
    TIMESTAMPTZ_TYPE_OID, TSRANGE_TYPE_OID, TSTZRANGE_TYPE_OID,
};
use crate::include::catalog::{PgProcRow, PgRangeRow, PgTypeRow};
use crate::include::nodes::datum::RangeTypeRef;
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
}

const BUILTIN_RANGE_SPECS: [BuiltinRangeSpec; 6] =
    [
        BuiltinRangeSpec {
            range_type: RangeTypeRef {
                sql_type: SqlType::range(INT4RANGE_TYPE_OID, INT4_TYPE_OID).with_range_metadata(
                    INT4_TYPE_OID,
                    0,
                    true,
                ),
                subtype: SqlType::new(SqlTypeKind::Int4).with_identity(INT4_TYPE_OID, 0),
                multirange_type_oid: 0,
                canonicalization: RangeCanonicalization::Discrete,
            },
            oid: INT4RANGE_TYPE_OID,
            name: "int4range",
        },
        BuiltinRangeSpec {
            range_type: RangeTypeRef {
                sql_type: SqlType::range(INT8RANGE_TYPE_OID, INT8_TYPE_OID).with_range_metadata(
                    INT8_TYPE_OID,
                    0,
                    true,
                ),
                subtype: SqlType::new(SqlTypeKind::Int8).with_identity(INT8_TYPE_OID, 0),
                multirange_type_oid: 0,
                canonicalization: RangeCanonicalization::Discrete,
            },
            oid: INT8RANGE_TYPE_OID,
            name: "int8range",
        },
        BuiltinRangeSpec {
            range_type: RangeTypeRef {
                sql_type: SqlType::range(NUMRANGE_TYPE_OID, NUMERIC_TYPE_OID).with_range_metadata(
                    NUMERIC_TYPE_OID,
                    0,
                    false,
                ),
                subtype: SqlType::new(SqlTypeKind::Numeric).with_identity(NUMERIC_TYPE_OID, 0),
                multirange_type_oid: 0,
                canonicalization: RangeCanonicalization::Continuous,
            },
            oid: NUMRANGE_TYPE_OID,
            name: "numrange",
        },
        BuiltinRangeSpec {
            range_type: RangeTypeRef {
                sql_type: SqlType::range(DATERANGE_TYPE_OID, DATE_TYPE_OID).with_range_metadata(
                    DATE_TYPE_OID,
                    0,
                    true,
                ),
                subtype: SqlType::new(SqlTypeKind::Date).with_identity(DATE_TYPE_OID, 0),
                multirange_type_oid: 0,
                canonicalization: RangeCanonicalization::Discrete,
            },
            oid: DATERANGE_TYPE_OID,
            name: "daterange",
        },
        BuiltinRangeSpec {
            range_type: RangeTypeRef {
                sql_type: SqlType::range(TSRANGE_TYPE_OID, TIMESTAMP_TYPE_OID).with_range_metadata(
                    TIMESTAMP_TYPE_OID,
                    0,
                    false,
                ),
                subtype: SqlType::new(SqlTypeKind::Timestamp).with_identity(TIMESTAMP_TYPE_OID, 0),
                multirange_type_oid: 0,
                canonicalization: RangeCanonicalization::Continuous,
            },
            oid: TSRANGE_TYPE_OID,
            name: "tsrange",
        },
        BuiltinRangeSpec {
            range_type: RangeTypeRef {
                sql_type: SqlType::range(TSTZRANGE_TYPE_OID, TIMESTAMPTZ_TYPE_OID)
                    .with_range_metadata(TIMESTAMPTZ_TYPE_OID, 0, false),
                subtype: SqlType::new(SqlTypeKind::TimestampTz)
                    .with_identity(TIMESTAMPTZ_TYPE_OID, 0),
                multirange_type_oid: 0,
                canonicalization: RangeCanonicalization::Continuous,
            },
            oid: TSTZRANGE_TYPE_OID,
            name: "tstzrange",
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

pub fn builtin_range_specs() -> &'static [BuiltinRangeSpec] {
    &BUILTIN_RANGE_SPECS
}

pub fn builtin_range_rows() -> Vec<PgRangeRow> {
    builtin_range_specs()
        .iter()
        .map(|spec| PgRangeRow {
            rngtypid: spec.oid,
            rngsubtype: spec.range_type.subtype_oid(),
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

pub fn synthetic_range_proc_rows(
    type_rows: &[PgTypeRow],
    range_rows: &[PgRangeRow],
) -> Vec<PgProcRow> {
    let mut rows = Vec::new();
    let mut seen_constructor_names = BTreeSet::new();
    for type_row in type_rows.iter().filter(|row| row.typelem == 0) {
        if builtin_range_spec_by_oid(type_row.oid).is_some() {
            continue;
        }
        let Some(range_row) = range_rows.iter().find(|row| row.rngtypid == type_row.oid) else {
            continue;
        };
        let range_oid = type_row.oid;
        let subtype_oid = range_row.rngsubtype;
        if seen_constructor_names.insert(type_row.typname.to_ascii_lowercase()) {
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
        rows.extend([
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

fn dynamic_range_proc_oid(range_oid: u32, slot: u32) -> u32 {
    DYNAMIC_RANGE_PROC_OID_BASE | ((range_oid & DYNAMIC_RANGE_PROC_OID_MASK) << 5) | slot
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

fn oid_argtypes(arg_oids: &[u32]) -> String {
    arg_oids
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(" ")
}
