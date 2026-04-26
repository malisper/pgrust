use std::sync::OnceLock;

use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgAmopRow {
    pub oid: u32,
    pub amopfamily: u32,
    pub amoplefttype: u32,
    pub amoprighttype: u32,
    pub amopstrategy: i16,
    pub amoppurpose: char,
    pub amopopr: u32,
    pub amopmethod: u32,
    pub amopsortfamily: u32,
}

pub fn pg_amop_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("amopfamily", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("amoplefttype", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("amoprighttype", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("amopstrategy", SqlType::new(SqlTypeKind::Int2), false),
            column_desc(
                "amoppurpose",
                SqlType::new(SqlTypeKind::InternalChar),
                false,
            ),
            column_desc("amopopr", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("amopmethod", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("amopsortfamily", SqlType::new(SqlTypeKind::Oid), false),
        ],
    }
}

pub fn bootstrap_pg_amop_rows() -> Vec<PgAmopRow> {
    static ROWS: OnceLock<Vec<PgAmopRow>> = OnceLock::new();
    ROWS.get_or_init(build_bootstrap_pg_amop_rows).clone()
}

fn build_bootstrap_pg_amop_rows() -> Vec<PgAmopRow> {
    let mut oid = 8000u32;
    let mut rows = Vec::new();
    for (family, type_oid, operators) in [
        (
            BTREE_BOOL_FAMILY_OID,
            BOOL_TYPE_OID,
            [58, 1694, 91, 1695, 59],
        ),
        (
            BTREE_INTEGER_FAMILY_OID,
            INT4_TYPE_OID,
            [97, 523, 96, 525, 521],
        ),
        (
            BTREE_TEXT_FAMILY_OID,
            TEXT_TYPE_OID,
            [664, 665, 98, 667, 666],
        ),
        (
            BTREE_BIT_FAMILY_OID,
            BIT_TYPE_OID,
            [1786, 1788, 1784, 1789, 1787],
        ),
        (
            BTREE_VARBIT_FAMILY_OID,
            VARBIT_TYPE_OID,
            [1806, 1808, 1804, 1809, 1807],
        ),
        (
            BTREE_BYTEA_FAMILY_OID,
            BYTEA_TYPE_OID,
            [1957, 1958, 1955, 1960, 1959],
        ),
        (
            BTREE_UUID_FAMILY_OID,
            UUID_TYPE_OID,
            [2974, 2976, 2972, 2977, 2975],
        ),
        (
            BTREE_INTERVAL_FAMILY_OID,
            INTERVAL_TYPE_OID,
            [1332, 1333, 1330, 1335, 1334],
        ),
        (
            BTREE_MACADDR_FAMILY_OID,
            MACADDR_TYPE_OID,
            [1222, 1223, 1220, 1225, 1224],
        ),
        (
            BTREE_MACADDR8_FAMILY_OID,
            MACADDR8_TYPE_OID,
            [3364, 3365, 3362, 3367, 3366],
        ),
    ] {
        for (strategy, operator_oid) in (1_i16..=5).zip(operators) {
            rows.push(PgAmopRow {
                oid,
                amopfamily: family,
                amoplefttype: type_oid,
                amoprighttype: type_oid,
                amopstrategy: strategy,
                amoppurpose: 's',
                amopopr: operator_oid,
                amopmethod: BTREE_AM_OID,
                amopsortfamily: family,
            });
            oid = oid.saturating_add(1);
        }
    }
    let operators = bootstrap_pg_operator_rows();
    for (family, type_oid) in [
        (BTREE_ARRAY_FAMILY_OID, ANYARRAYOID),
        (BTREE_CHAR_FAMILY_OID, INTERNAL_CHAR_TYPE_OID),
        (BTREE_DATETIME_FAMILY_OID, TIMESTAMP_TYPE_OID),
        (BTREE_DATETIME_FAMILY_OID, TIMESTAMPTZ_TYPE_OID),
        (BTREE_FLOAT_FAMILY_OID, FLOAT4_TYPE_OID),
        (BTREE_FLOAT_FAMILY_OID, FLOAT8_TYPE_OID),
        (BTREE_INTEGER_FAMILY_OID, INT2_TYPE_OID),
        (BTREE_INTEGER_FAMILY_OID, INT8_TYPE_OID),
        (BTREE_NUMERIC_FAMILY_OID, NUMERIC_TYPE_OID),
        (BTREE_OID_FAMILY_OID, OID_TYPE_OID),
        (BTREE_OIDVECTOR_FAMILY_OID, OIDVECTOR_TYPE_OID),
        (BTREE_BPCHAR_FAMILY_OID, BPCHAR_TYPE_OID),
        (BTREE_TEXT_FAMILY_OID, NAME_TYPE_OID),
        (BTREE_MULTIRANGE_FAMILY_OID, ANYMULTIRANGEOID),
        (BTREE_RECORD_FAMILY_OID, RECORD_TYPE_OID),
        (BTREE_TSVECTOR_FAMILY_OID, TSVECTOR_TYPE_OID),
        (BTREE_TSQUERY_FAMILY_OID, TSQUERY_TYPE_OID),
        (BTREE_RANGE_FAMILY_OID, ANYRANGEOID),
        (BTREE_JSONB_FAMILY_OID, JSONB_TYPE_OID),
        (BTREE_NETWORK_FAMILY_OID, INET_TYPE_OID),
    ] {
        for (strategy, name) in [(1_i16, "<"), (2, "<="), (3, "="), (4, ">="), (5, ">")] {
            rows.push(PgAmopRow {
                oid,
                amopfamily: family,
                amoplefttype: type_oid,
                amoprighttype: type_oid,
                amopstrategy: strategy,
                amoppurpose: 's',
                amopopr: operator_oid(&operators, name, type_oid, type_oid),
                amopmethod: BTREE_AM_OID,
                amopsortfamily: 0,
            });
            oid = oid.saturating_add(1);
        }
    }
    for (family, type_oid, operators) in [
        (
            BTREE_RECORD_IMAGE_FAMILY_OID,
            RECORD_TYPE_OID,
            [3190_u32, 3192, 3188, 3193, 3191],
        ),
        (
            BTREE_TEXT_PATTERN_FAMILY_OID,
            TEXT_TYPE_OID,
            [2314, 2315, 0, 2317, 2318],
        ),
        (
            BTREE_BPCHAR_PATTERN_FAMILY_OID,
            BPCHAR_TYPE_OID,
            [2326, 2327, 0, 2329, 2330],
        ),
    ] {
        for (strategy, operator_oid) in (1_i16..=5).zip(operators) {
            if operator_oid == 0 {
                continue;
            }
            rows.push(PgAmopRow {
                oid,
                amopfamily: family,
                amoplefttype: type_oid,
                amoprighttype: type_oid,
                amopstrategy: strategy,
                amoppurpose: 's',
                amopopr: operator_oid,
                amopmethod: BTREE_AM_OID,
                amopsortfamily: 0,
            });
            oid = oid.saturating_add(1);
        }
    }
    for (strategy, name, righttype) in [
        (1_i16, "<<", BOX_TYPE_OID),
        (2, "&<", BOX_TYPE_OID),
        (3, "&&", BOX_TYPE_OID),
        (4, "&>", BOX_TYPE_OID),
        (5, ">>", BOX_TYPE_OID),
        (6, "~=", BOX_TYPE_OID),
        (7, "@>", BOX_TYPE_OID),
        (8, "<@", BOX_TYPE_OID),
        (9, "&<|", BOX_TYPE_OID),
        (10, "<<|", BOX_TYPE_OID),
        (11, "|>>", BOX_TYPE_OID),
        (12, "|&>", BOX_TYPE_OID),
    ] {
        rows.push(PgAmopRow {
            oid,
            amopfamily: GIST_BOX_FAMILY_OID,
            amoplefttype: BOX_TYPE_OID,
            amoprighttype: righttype,
            amopstrategy: strategy,
            amoppurpose: 's',
            amopopr: operator_oid(&operators, name, BOX_TYPE_OID, righttype),
            amopmethod: GIST_AM_OID,
            amopsortfamily: 0,
        });
        oid = oid.saturating_add(1);
    }
    rows.push(PgAmopRow {
        oid,
        amopfamily: GIST_BOX_FAMILY_OID,
        amoplefttype: BOX_TYPE_OID,
        amoprighttype: BOX_TYPE_OID,
        amopstrategy: 15,
        amoppurpose: 'o',
        amopopr: operator_oid(&operators, "<->", BOX_TYPE_OID, BOX_TYPE_OID),
        amopmethod: GIST_AM_OID,
        amopsortfamily: BTREE_FLOAT_FAMILY_OID,
    });
    oid = oid.saturating_add(1);
    for (strategy, name, righttype) in [
        (1_i16, "<<", POINT_TYPE_OID),
        (5, ">>", POINT_TYPE_OID),
        (6, "~=", POINT_TYPE_OID),
        (10, "<<|", POINT_TYPE_OID),
        (11, "|>>", POINT_TYPE_OID),
        (28, "<@", BOX_TYPE_OID),
        (29, "<^", POINT_TYPE_OID),
        (30, ">^", POINT_TYPE_OID),
    ] {
        rows.push(PgAmopRow {
            oid,
            amopfamily: GIST_POINT_FAMILY_OID,
            amoplefttype: POINT_TYPE_OID,
            amoprighttype: righttype,
            amopstrategy: strategy,
            amoppurpose: 's',
            amopopr: operator_oid(&operators, name, POINT_TYPE_OID, righttype),
            amopmethod: GIST_AM_OID,
            amopsortfamily: 0,
        });
        oid = oid.saturating_add(1);
    }
    for (strategy, righttype) in [
        (48_i16, crate::include::catalog::POLYGON_TYPE_OID),
        (68, crate::include::catalog::CIRCLE_TYPE_OID),
    ] {
        rows.push(PgAmopRow {
            oid,
            amopfamily: GIST_POINT_FAMILY_OID,
            amoplefttype: POINT_TYPE_OID,
            amoprighttype: righttype,
            amopstrategy: strategy,
            amoppurpose: 's',
            amopopr: operator_oid(&operators, "<@", POINT_TYPE_OID, righttype),
            amopmethod: GIST_AM_OID,
            amopsortfamily: 0,
        });
        oid = oid.saturating_add(1);
    }
    for family in [SPGIST_QUAD_POINT_FAMILY_OID, SPGIST_KD_POINT_FAMILY_OID] {
        for (strategy, name, righttype) in [
            (11_i16, "|>>", POINT_TYPE_OID),
            (30, ">^", POINT_TYPE_OID),
            (1, "<<", POINT_TYPE_OID),
            (5, ">>", POINT_TYPE_OID),
            (10, "<<|", POINT_TYPE_OID),
            (29, "<^", POINT_TYPE_OID),
            (6, "~=", POINT_TYPE_OID),
            (8, "<@", BOX_TYPE_OID),
        ] {
            rows.push(PgAmopRow {
                oid,
                amopfamily: family,
                amoplefttype: POINT_TYPE_OID,
                amoprighttype: righttype,
                amopstrategy: strategy,
                amoppurpose: 's',
                amopopr: operator_oid(&operators, name, POINT_TYPE_OID, righttype),
                amopmethod: SPGIST_AM_OID,
                amopsortfamily: 0,
            });
            oid = oid.saturating_add(1);
        }
        rows.push(PgAmopRow {
            oid,
            amopfamily: family,
            amoplefttype: POINT_TYPE_OID,
            amoprighttype: POINT_TYPE_OID,
            amopstrategy: 15,
            amoppurpose: 'o',
            amopopr: operator_oid(&operators, "<->", POINT_TYPE_OID, POINT_TYPE_OID),
            amopmethod: SPGIST_AM_OID,
            amopsortfamily: BTREE_FLOAT_FAMILY_OID,
        });
        oid = oid.saturating_add(1);
    }
    for (strategy, name, righttype) in [
        (1_i16, "<<", BOX_TYPE_OID),
        (2, "&<", BOX_TYPE_OID),
        (3, "&&", BOX_TYPE_OID),
        (4, "&>", BOX_TYPE_OID),
        (5, ">>", BOX_TYPE_OID),
        (6, "~=", BOX_TYPE_OID),
        (7, "@>", BOX_TYPE_OID),
        (8, "<@", BOX_TYPE_OID),
        (9, "&<|", BOX_TYPE_OID),
        (10, "<<|", BOX_TYPE_OID),
        (11, "|>>", BOX_TYPE_OID),
        (12, "|&>", BOX_TYPE_OID),
    ] {
        rows.push(PgAmopRow {
            oid,
            amopfamily: SPGIST_BOX_FAMILY_OID,
            amoplefttype: BOX_TYPE_OID,
            amoprighttype: righttype,
            amopstrategy: strategy,
            amoppurpose: 's',
            amopopr: operator_oid(&operators, name, BOX_TYPE_OID, righttype),
            amopmethod: SPGIST_AM_OID,
            amopsortfamily: 0,
        });
        oid = oid.saturating_add(1);
    }
    rows.push(PgAmopRow {
        oid,
        amopfamily: SPGIST_BOX_FAMILY_OID,
        amoplefttype: BOX_TYPE_OID,
        amoprighttype: POINT_TYPE_OID,
        amopstrategy: 15,
        amoppurpose: 'o',
        amopopr: operator_oid(&operators, "<->", BOX_TYPE_OID, POINT_TYPE_OID),
        amopmethod: SPGIST_AM_OID,
        amopsortfamily: BTREE_FLOAT_FAMILY_OID,
    });
    oid = oid.saturating_add(1);
    for (strategy, name, righttype) in [
        (1_i16, "<<", POLYGON_TYPE_OID),
        (2, "&<", POLYGON_TYPE_OID),
        (3, "&&", POLYGON_TYPE_OID),
        (4, "&>", POLYGON_TYPE_OID),
        (5, ">>", POLYGON_TYPE_OID),
        (6, "~=", POLYGON_TYPE_OID),
        (7, "@>", POLYGON_TYPE_OID),
        (8, "<@", POLYGON_TYPE_OID),
        (9, "&<|", POLYGON_TYPE_OID),
        (10, "<<|", POLYGON_TYPE_OID),
        (11, "|>>", POLYGON_TYPE_OID),
        (12, "|&>", POLYGON_TYPE_OID),
    ] {
        rows.push(PgAmopRow {
            oid,
            amopfamily: SPGIST_POLY_FAMILY_OID,
            amoplefttype: POLYGON_TYPE_OID,
            amoprighttype: righttype,
            amopstrategy: strategy,
            amoppurpose: 's',
            amopopr: operator_oid(&operators, name, POLYGON_TYPE_OID, righttype),
            amopmethod: SPGIST_AM_OID,
            amopsortfamily: 0,
        });
        oid = oid.saturating_add(1);
    }
    rows.push(PgAmopRow {
        oid,
        amopfamily: SPGIST_POLY_FAMILY_OID,
        amoplefttype: POLYGON_TYPE_OID,
        amoprighttype: POINT_TYPE_OID,
        amopstrategy: 15,
        amoppurpose: 'o',
        amopopr: operator_oid(&operators, "<->", POLYGON_TYPE_OID, POINT_TYPE_OID),
        amopmethod: SPGIST_AM_OID,
        amopsortfamily: BTREE_FLOAT_FAMILY_OID,
    });
    oid = oid.saturating_add(1);
    for (strategy, name, righttype) in [
        (1_i16, "<<", ANYMULTIRANGEOID),
        (1_i16, "<<", ANYRANGEOID),
        (2, "&<", ANYMULTIRANGEOID),
        (2, "&<", ANYRANGEOID),
        (3, "&&", ANYMULTIRANGEOID),
        (3, "&&", ANYRANGEOID),
        (4, "&>", ANYMULTIRANGEOID),
        (4, "&>", ANYRANGEOID),
        (5, ">>", ANYMULTIRANGEOID),
        (5, ">>", ANYRANGEOID),
        (6, "-|-", ANYMULTIRANGEOID),
        (6, "-|-", ANYRANGEOID),
        (7, "@>", ANYMULTIRANGEOID),
        (7, "@>", ANYRANGEOID),
        (8, "<@", ANYMULTIRANGEOID),
        (8, "<@", ANYRANGEOID),
        (16, "@>", ANYELEMENTOID),
        (18, "=", ANYRANGEOID),
    ] {
        rows.push(PgAmopRow {
            oid,
            amopfamily: GIST_RANGE_FAMILY_OID,
            amoplefttype: ANYRANGEOID,
            amoprighttype: righttype,
            amopstrategy: strategy,
            amoppurpose: 's',
            amopopr: operator_oid(&operators, name, ANYRANGEOID, righttype),
            amopmethod: GIST_AM_OID,
            amopsortfamily: 0,
        });
        oid = oid.saturating_add(1);
    }
    rows.push(PgAmopRow {
        oid,
        amopfamily: GIST_TSVECTOR_FAMILY_OID,
        amoplefttype: TSVECTOR_TYPE_OID,
        amoprighttype: TSQUERY_TYPE_OID,
        amopstrategy: 1,
        amoppurpose: 's',
        amopopr: operator_oid(&operators, "@@", TSVECTOR_TYPE_OID, TSQUERY_TYPE_OID),
        amopmethod: GIST_AM_OID,
        amopsortfamily: 0,
    });
    oid = oid.saturating_add(1);
    for (family, method) in [
        (GIST_NETWORK_FAMILY_OID, GIST_AM_OID),
        (SPGIST_NETWORK_FAMILY_OID, SPGIST_AM_OID),
    ] {
        for (strategy, name) in [
            (3_i16, "&&"),
            (18, "="),
            (19, "<>"),
            (20, "<"),
            (21, "<="),
            (22, ">"),
            (23, ">="),
            (24, "<<"),
            (25, "<<="),
            (26, ">>"),
            (27, ">>="),
        ] {
            rows.push(PgAmopRow {
                oid,
                amopfamily: family,
                amoplefttype: INET_TYPE_OID,
                amoprighttype: INET_TYPE_OID,
                amopstrategy: strategy,
                amoppurpose: 's',
                amopopr: operator_oid(&operators, name, INET_TYPE_OID, INET_TYPE_OID),
                amopmethod: method,
                amopsortfamily: 0,
            });
            oid = oid.saturating_add(1);
        }
    }
    for (strategy, name, righttype) in [
        (1_i16, "<<", ANYMULTIRANGEOID),
        (1, "<<", ANYRANGEOID),
        (2, "&<", ANYMULTIRANGEOID),
        (2, "&<", ANYRANGEOID),
        (3, "&&", ANYMULTIRANGEOID),
        (3, "&&", ANYRANGEOID),
        (4, "&>", ANYMULTIRANGEOID),
        (4, "&>", ANYRANGEOID),
        (5, ">>", ANYMULTIRANGEOID),
        (5, ">>", ANYRANGEOID),
        (6, "-|-", ANYMULTIRANGEOID),
        (6, "-|-", ANYRANGEOID),
        (7, "@>", ANYMULTIRANGEOID),
        (7, "@>", ANYRANGEOID),
        (8, "<@", ANYMULTIRANGEOID),
        (8, "<@", ANYRANGEOID),
        (16, "@>", ANYELEMENTOID),
        (18, "=", ANYMULTIRANGEOID),
    ] {
        rows.push(PgAmopRow {
            oid,
            amopfamily: GIST_MULTIRANGE_FAMILY_OID,
            amoplefttype: ANYMULTIRANGEOID,
            amoprighttype: righttype,
            amopstrategy: strategy,
            amoppurpose: 's',
            amopopr: operator_oid(&operators, name, ANYMULTIRANGEOID, righttype),
            amopmethod: GIST_AM_OID,
            amopsortfamily: 0,
        });
        oid = oid.saturating_add(1);
    }
    for (strategy, name) in [
        (1_i16, "~<~"),
        (2, "~<=~"),
        (3, "="),
        (4, "~>=~"),
        (5, "~>~"),
        (11, "<"),
        (12, "<="),
        (14, ">="),
        (15, ">"),
        (28, "^@"),
    ] {
        rows.push(PgAmopRow {
            oid,
            amopfamily: SPGIST_TEXT_FAMILY_OID,
            amoplefttype: TEXT_TYPE_OID,
            amoprighttype: TEXT_TYPE_OID,
            amopstrategy: strategy,
            amoppurpose: 's',
            amopopr: operator_oid(&operators, name, TEXT_TYPE_OID, TEXT_TYPE_OID),
            amopmethod: SPGIST_AM_OID,
            amopsortfamily: 0,
        });
        oid = oid.saturating_add(1);
    }
    for (strategy, name, righttype) in [
        (1_i16, "<<", ANYRANGEOID),
        (2, "&<", ANYRANGEOID),
        (3, "&&", ANYRANGEOID),
        (4, "&>", ANYRANGEOID),
        (5, ">>", ANYRANGEOID),
        (6, "-|-", ANYRANGEOID),
        (7, "@>", ANYRANGEOID),
        (8, "<@", ANYRANGEOID),
        (16, "@>", ANYELEMENTOID),
        (18, "=", ANYRANGEOID),
    ] {
        rows.push(PgAmopRow {
            oid,
            amopfamily: SPGIST_RANGE_FAMILY_OID,
            amoplefttype: ANYRANGEOID,
            amoprighttype: righttype,
            amopstrategy: strategy,
            amoppurpose: 's',
            amopopr: operator_oid(&operators, name, ANYRANGEOID, righttype),
            amopmethod: SPGIST_AM_OID,
            amopsortfamily: 0,
        });
        oid = oid.saturating_add(1);
    }
    for (family, type_oid) in [
        (BRIN_BYTEA_MINMAX_FAMILY_OID, BYTEA_TYPE_OID),
        (BRIN_CHAR_MINMAX_FAMILY_OID, INTERNAL_CHAR_TYPE_OID),
        (BRIN_INTEGER_MINMAX_FAMILY_OID, INT2_TYPE_OID),
        (BRIN_INTEGER_MINMAX_FAMILY_OID, INT4_TYPE_OID),
        (BRIN_INTEGER_MINMAX_FAMILY_OID, INT8_TYPE_OID),
        (BRIN_OID_MINMAX_FAMILY_OID, OID_TYPE_OID),
        (BRIN_FLOAT_MINMAX_FAMILY_OID, FLOAT4_TYPE_OID),
        (BRIN_FLOAT_MINMAX_FAMILY_OID, FLOAT8_TYPE_OID),
        (BRIN_TEXT_MINMAX_FAMILY_OID, TEXT_TYPE_OID),
        (BRIN_BPCHAR_MINMAX_FAMILY_OID, BPCHAR_TYPE_OID),
        (BRIN_DATETIME_MINMAX_FAMILY_OID, DATE_TYPE_OID),
        (BRIN_TIME_MINMAX_FAMILY_OID, TIME_TYPE_OID),
        (BRIN_TIMETZ_MINMAX_FAMILY_OID, TIMETZ_TYPE_OID),
        (BRIN_DATETIME_MINMAX_FAMILY_OID, TIMESTAMP_TYPE_OID),
        (BRIN_DATETIME_MINMAX_FAMILY_OID, TIMESTAMPTZ_TYPE_OID),
        (BRIN_BIT_MINMAX_FAMILY_OID, BIT_TYPE_OID),
        (BRIN_VARBIT_MINMAX_FAMILY_OID, VARBIT_TYPE_OID),
        (BRIN_MACADDR_MINMAX_FAMILY_OID, MACADDR_TYPE_OID),
        (BRIN_MACADDR8_MINMAX_FAMILY_OID, MACADDR8_TYPE_OID),
        // :HACK: pgrust only executes generic BRIN minmax today. Keep these
        // PostgreSQL-compatible catalog rows visible until generic
        // minmax-multi runtime support lands.
        (BRIN_MACADDR_MINMAX_MULTI_FAMILY_OID, MACADDR_TYPE_OID),
        (BRIN_MACADDR8_MINMAX_MULTI_FAMILY_OID, MACADDR8_TYPE_OID),
    ] {
        for (strategy, name) in [(1_i16, "<"), (2, "<="), (3, "="), (4, ">="), (5, ">")] {
            rows.push(PgAmopRow {
                oid,
                amopfamily: family,
                amoplefttype: type_oid,
                amoprighttype: type_oid,
                amopstrategy: strategy,
                amoppurpose: 's',
                amopopr: operator_oid(&operators, name, type_oid, type_oid),
                amopmethod: BRIN_AM_OID,
                amopsortfamily: 0,
            });
            oid = oid.saturating_add(1);
        }
    }
    // :HACK: Generic BRIN bloom runtime support is not implemented yet; these
    // rows expose PostgreSQL-compatible catalogs for MAC address types.
    for (family, type_oid) in [
        (BRIN_MACADDR_BLOOM_FAMILY_OID, MACADDR_TYPE_OID),
        (BRIN_MACADDR8_BLOOM_FAMILY_OID, MACADDR8_TYPE_OID),
    ] {
        rows.push(PgAmopRow {
            oid,
            amopfamily: family,
            amoplefttype: type_oid,
            amoprighttype: type_oid,
            amopstrategy: 1,
            amoppurpose: 's',
            amopopr: operator_oid(&operators, "=", type_oid, type_oid),
            amopmethod: BRIN_AM_OID,
            amopsortfamily: 0,
        });
        oid = oid.saturating_add(1);
    }
    for (strategy, name, righttype) in [
        (1_i16, "<<", BOX_TYPE_OID),
        (2, "&<", BOX_TYPE_OID),
        (3, "&&", BOX_TYPE_OID),
        (4, "&>", BOX_TYPE_OID),
        (5, ">>", BOX_TYPE_OID),
        (6, "~=", BOX_TYPE_OID),
        (7, "@>", BOX_TYPE_OID),
        (8, "<@", BOX_TYPE_OID),
        (9, "&<|", BOX_TYPE_OID),
        (10, "<<|", BOX_TYPE_OID),
        (11, "|>>", BOX_TYPE_OID),
        (12, "|&>", BOX_TYPE_OID),
        (7, "@>", POINT_TYPE_OID),
    ] {
        rows.push(PgAmopRow {
            oid,
            amopfamily: BRIN_BOX_INCLUSION_FAMILY_OID,
            amoplefttype: BOX_TYPE_OID,
            amoprighttype: righttype,
            amopstrategy: strategy,
            amoppurpose: 's',
            amopopr: operator_oid(&operators, name, BOX_TYPE_OID, righttype),
            amopmethod: BRIN_AM_OID,
            amopsortfamily: 0,
        });
        oid = oid.saturating_add(1);
    }
    for (strategy, name, righttype) in [
        (1_i16, "<<", ANYRANGEOID),
        (2, "&<", ANYRANGEOID),
        (3, "&&", ANYRANGEOID),
        (4, "&>", ANYRANGEOID),
        (5, ">>", ANYRANGEOID),
        (7, "@>", ANYRANGEOID),
        (8, "<@", ANYRANGEOID),
        (16, "@>", ANYELEMENTOID),
        (17, "-|-", ANYRANGEOID),
        (18, "=", ANYRANGEOID),
        (20, "<", ANYRANGEOID),
        (21, "<=", ANYRANGEOID),
        (22, ">", ANYRANGEOID),
        (23, ">=", ANYRANGEOID),
    ] {
        rows.push(PgAmopRow {
            oid,
            amopfamily: BRIN_RANGE_INCLUSION_FAMILY_OID,
            amoplefttype: ANYRANGEOID,
            amoprighttype: righttype,
            amopstrategy: strategy,
            amoppurpose: 's',
            amopopr: operator_oid(&operators, name, ANYRANGEOID, righttype),
            amopmethod: BRIN_AM_OID,
            amopsortfamily: 0,
        });
        oid = oid.saturating_add(1);
    }
    for (strategy, name) in [
        (3_i16, "&&"),
        (7, ">>="),
        (8, "<<="),
        (18, "="),
        (24, ">>"),
        (26, "<<"),
    ] {
        rows.push(PgAmopRow {
            oid,
            amopfamily: BRIN_NETWORK_INCLUSION_FAMILY_OID,
            amoplefttype: INET_TYPE_OID,
            amoprighttype: INET_TYPE_OID,
            amopstrategy: strategy,
            amoppurpose: 's',
            amopopr: operator_oid(&operators, name, INET_TYPE_OID, INET_TYPE_OID),
            amopmethod: BRIN_AM_OID,
            amopsortfamily: 0,
        });
        oid = oid.saturating_add(1);
    }
    rows.push(PgAmopRow {
        oid,
        amopfamily: BRIN_TEXT_BLOOM_FAMILY_OID,
        amoplefttype: TEXT_TYPE_OID,
        amoprighttype: TEXT_TYPE_OID,
        amopstrategy: 1,
        amoppurpose: 's',
        amopopr: operator_oid(&operators, "=", TEXT_TYPE_OID, TEXT_TYPE_OID),
        amopmethod: BRIN_AM_OID,
        amopsortfamily: 0,
    });
    oid = oid.saturating_add(1);
    for (strategy, name) in [(1_i16, "&&"), (2, "@>"), (3, "<@"), (4, "=")] {
        rows.push(PgAmopRow {
            oid,
            amopfamily: GIN_ARRAY_FAMILY_OID,
            amoplefttype: ANYARRAYOID,
            amoprighttype: ANYARRAYOID,
            amopstrategy: strategy,
            amoppurpose: 's',
            amopopr: operator_oid(&operators, name, ANYARRAYOID, ANYARRAYOID),
            amopmethod: GIN_AM_OID,
            amopsortfamily: 0,
        });
        oid = oid.saturating_add(1);
    }
    for (strategy, name) in [(1_i16, "@@"), (2, "@@@")] {
        rows.push(PgAmopRow {
            oid,
            amopfamily: GIN_TSVECTOR_FAMILY_OID,
            amoplefttype: TSVECTOR_TYPE_OID,
            amoprighttype: TSQUERY_TYPE_OID,
            amopstrategy: strategy,
            amoppurpose: 's',
            amopopr: operator_oid(&operators, name, TSVECTOR_TYPE_OID, TSQUERY_TYPE_OID),
            amopmethod: GIN_AM_OID,
            amopsortfamily: 0,
        });
        oid = oid.saturating_add(1);
    }
    for (strategy, righttype, operator_oid) in [
        (7_i16, JSONB_TYPE_OID, JSONB_CONTAINS_OPERATOR_OID),
        (9, TEXT_TYPE_OID, JSONB_EXISTS_OPERATOR_OID),
        (10, TEXT_ARRAY_TYPE_OID, JSONB_EXISTS_ANY_OPERATOR_OID),
        (11, TEXT_ARRAY_TYPE_OID, JSONB_EXISTS_ALL_OPERATOR_OID),
        (
            15,
            JSONPATH_TYPE_OID,
            operator_oid(&operators, "@?", JSONB_TYPE_OID, JSONPATH_TYPE_OID),
        ),
        (
            16,
            JSONPATH_TYPE_OID,
            operator_oid(&operators, "@@", JSONB_TYPE_OID, JSONPATH_TYPE_OID),
        ),
    ] {
        rows.push(PgAmopRow {
            oid,
            amopfamily: GIN_JSONB_FAMILY_OID,
            amoplefttype: JSONB_TYPE_OID,
            amoprighttype: righttype,
            amopstrategy: strategy,
            amoppurpose: 's',
            amopopr: operator_oid,
            amopmethod: GIN_AM_OID,
            amopsortfamily: 0,
        });
        oid = oid.saturating_add(1);
    }
    for (family, type_oid) in [
        (HASH_BOOL_FAMILY_OID, BOOL_TYPE_OID),
        (HASH_INTEGER_FAMILY_OID, INT2_TYPE_OID),
        (HASH_INTEGER_FAMILY_OID, INT4_TYPE_OID),
        (HASH_INTEGER_FAMILY_OID, INT8_TYPE_OID),
        (HASH_OID_FAMILY_OID, OID_TYPE_OID),
        (HASH_CHAR_FAMILY_OID, INTERNAL_CHAR_TYPE_OID),
        (HASH_TEXT_FAMILY_OID, NAME_TYPE_OID),
        (HASH_TEXT_FAMILY_OID, TEXT_TYPE_OID),
        (HASH_TEXT_FAMILY_OID, VARCHAR_TYPE_OID),
        (HASH_BPCHAR_FAMILY_OID, BPCHAR_TYPE_OID),
        (HASH_FLOAT_FAMILY_OID, FLOAT4_TYPE_OID),
        (HASH_FLOAT_FAMILY_OID, FLOAT8_TYPE_OID),
        (HASH_NUMERIC_FAMILY_OID, NUMERIC_TYPE_OID),
        (HASH_TIMESTAMP_FAMILY_OID, TIMESTAMP_TYPE_OID),
        (HASH_TIMESTAMPTZ_FAMILY_OID, TIMESTAMPTZ_TYPE_OID),
        (HASH_DATE_FAMILY_OID, DATE_TYPE_OID),
        (HASH_TIME_FAMILY_OID, TIME_TYPE_OID),
        (HASH_TIMETZ_FAMILY_OID, TIMETZ_TYPE_OID),
        (HASH_BYTEA_FAMILY_OID, BYTEA_TYPE_OID),
        (HASH_UUID_FAMILY_OID, UUID_TYPE_OID),
        (HASH_MULTIRANGE_FAMILY_OID, ANYMULTIRANGEOID),
        (HASH_MACADDR_FAMILY_OID, MACADDR_TYPE_OID),
        (HASH_MACADDR8_FAMILY_OID, MACADDR8_TYPE_OID),
        (HASH_INTERVAL_FAMILY_OID, INTERVAL_TYPE_OID),
    ] {
        rows.push(PgAmopRow {
            oid,
            amopfamily: family,
            amoplefttype: type_oid,
            amoprighttype: type_oid,
            amopstrategy: 1,
            amoppurpose: 's',
            amopopr: operator_oid(&operators, "=", type_oid, type_oid),
            amopmethod: HASH_AM_OID,
            amopsortfamily: 0,
        });
        oid = oid.saturating_add(1);
    }
    rows.push(PgAmopRow {
        oid,
        amopfamily: HASH_RANGE_FAMILY_OID,
        amoplefttype: ANYRANGEOID,
        amoprighttype: ANYRANGEOID,
        amopstrategy: 1,
        amoppurpose: 's',
        amopopr: 0,
        amopmethod: HASH_AM_OID,
        amopsortfamily: 0,
    });
    rows
}

fn operator_oid(
    rows: &[crate::include::catalog::PgOperatorRow],
    name: &str,
    left: u32,
    right: u32,
) -> u32 {
    rows.iter()
        .find(|row| row.oprname == name && row.oprleft == left && row.oprright == right)
        .map(|row| row.oid)
        .unwrap_or_else(|| panic!("missing bootstrap operator {name}({left},{right})"))
}

#[cfg(test)]
mod tests {
    use crate::include::catalog::{
        BOX_TYPE_OID, BRIN_AM_OID, BRIN_MACADDR_MINMAX_FAMILY_OID, BRIN_MACADDR8_BLOOM_FAMILY_OID,
        BRIN_MACADDR8_MINMAX_FAMILY_OID, BTREE_AM_OID, BTREE_FLOAT_FAMILY_OID,
        BTREE_MACADDR_FAMILY_OID, BTREE_MACADDR8_FAMILY_OID, HASH_AM_OID, HASH_MACADDR_FAMILY_OID,
        HASH_MACADDR8_FAMILY_OID, MACADDR_TYPE_OID, MACADDR8_TYPE_OID, POINT_TYPE_OID,
        SPGIST_AM_OID, SPGIST_BOX_FAMILY_OID,
    };

    use super::bootstrap_pg_amop_rows;

    #[test]
    fn spgist_box_ordering_row_matches_postgres_shape() {
        let row = bootstrap_pg_amop_rows()
            .into_iter()
            .find(|row| row.amopfamily == SPGIST_BOX_FAMILY_OID && row.amoppurpose == 'o')
            .expect("spgist box ordering row");

        assert_eq!(row.amopmethod, SPGIST_AM_OID);
        assert_eq!(row.amopstrategy, 15);
        assert_eq!(row.amoplefttype, BOX_TYPE_OID);
        assert_eq!(row.amoprighttype, POINT_TYPE_OID);
        assert_eq!(row.amopsortfamily, BTREE_FLOAT_FAMILY_OID);
    }

    #[test]
    fn macaddr_amop_rows_cover_btree_hash_and_brin_catalogs() {
        let rows = bootstrap_pg_amop_rows();
        assert!(rows.iter().any(|row| {
            row.amopfamily == BTREE_MACADDR_FAMILY_OID
                && row.amoplefttype == MACADDR_TYPE_OID
                && row.amoprighttype == MACADDR_TYPE_OID
                && row.amopstrategy == 3
                && row.amopopr == 1220
                && row.amopmethod == BTREE_AM_OID
                && row.amopsortfamily == BTREE_MACADDR_FAMILY_OID
        }));
        assert!(rows.iter().any(|row| {
            row.amopfamily == BTREE_MACADDR8_FAMILY_OID
                && row.amoplefttype == MACADDR8_TYPE_OID
                && row.amoprighttype == MACADDR8_TYPE_OID
                && row.amopstrategy == 1
                && row.amopopr == 3364
                && row.amopmethod == BTREE_AM_OID
        }));
        assert!(rows.iter().any(|row| {
            row.amopfamily == HASH_MACADDR_FAMILY_OID
                && row.amoplefttype == MACADDR_TYPE_OID
                && row.amopstrategy == 1
                && row.amopopr == 1220
                && row.amopmethod == HASH_AM_OID
        }));
        assert!(rows.iter().any(|row| {
            row.amopfamily == HASH_MACADDR8_FAMILY_OID
                && row.amoplefttype == MACADDR8_TYPE_OID
                && row.amopstrategy == 1
                && row.amopopr == 3362
                && row.amopmethod == HASH_AM_OID
        }));
        assert!(rows.iter().any(|row| {
            row.amopfamily == BRIN_MACADDR_MINMAX_FAMILY_OID
                && row.amoplefttype == MACADDR_TYPE_OID
                && row.amopstrategy == 5
                && row.amopopr == 1224
                && row.amopmethod == BRIN_AM_OID
        }));
        assert!(rows.iter().any(|row| {
            row.amopfamily == BRIN_MACADDR8_MINMAX_FAMILY_OID
                && row.amoplefttype == MACADDR8_TYPE_OID
                && row.amopstrategy == 3
                && row.amopopr == 3362
                && row.amopmethod == BRIN_AM_OID
        }));
        assert!(rows.iter().any(|row| {
            row.amopfamily == BRIN_MACADDR8_BLOOM_FAMILY_OID
                && row.amoplefttype == MACADDR8_TYPE_OID
                && row.amopstrategy == 1
                && row.amopopr == 3362
                && row.amopmethod == BRIN_AM_OID
        }));
    }
}
