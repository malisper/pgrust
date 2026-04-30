use std::sync::OnceLock;

use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::*;

const BTEQUALIMAGE_PROC_OID: u32 = 5051;
const BTVARSTREQUALIMAGE_PROC_OID: u32 = 5050;
const BTINT4_SORTSUPPORT_PROC_OID: u32 = 3130;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgAmprocRow {
    pub oid: u32,
    pub amprocfamily: u32,
    pub amproclefttype: u32,
    pub amprocrighttype: u32,
    pub amprocnum: i16,
    pub amproc: u32,
}

pub fn pg_amproc_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("amprocfamily", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("amproclefttype", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("amprocrighttype", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("amprocnum", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("amproc", SqlType::new(SqlTypeKind::Oid), false),
        ],
    }
}

pub fn bootstrap_pg_amproc_rows() -> Vec<PgAmprocRow> {
    static ROWS: OnceLock<Vec<PgAmprocRow>> = OnceLock::new();
    ROWS.get_or_init(build_bootstrap_pg_amproc_rows).clone()
}

fn build_bootstrap_pg_amproc_rows() -> Vec<PgAmprocRow> {
    let mut oid = 8100u32;
    let mut rows = Vec::new();
    for (family, type_oid, proc_oid) in [
        (BTREE_BOOL_FAMILY_OID, BOOL_TYPE_OID, BOOL_CMP_EQ_PROC_OID),
        (
            BTREE_INTEGER_FAMILY_OID,
            INT4_TYPE_OID,
            INT4_CMP_EQ_PROC_OID,
        ),
        (BTREE_TEXT_FAMILY_OID, TEXT_TYPE_OID, TEXT_CMP_EQ_PROC_OID),
        (BTREE_BIT_FAMILY_OID, BIT_TYPE_OID, BIT_CMP_EQ_PROC_OID),
        (
            BTREE_VARBIT_FAMILY_OID,
            VARBIT_TYPE_OID,
            VARBIT_CMP_EQ_PROC_OID,
        ),
        (
            BTREE_BYTEA_FAMILY_OID,
            BYTEA_TYPE_OID,
            BYTEA_CMP_EQ_PROC_OID,
        ),
        (BTREE_UUID_FAMILY_OID, UUID_TYPE_OID, UUID_CMP_PROC_OID),
        (
            BTREE_MACADDR_FAMILY_OID,
            MACADDR_TYPE_OID,
            MACADDR_CMP_PROC_OID,
        ),
        (
            BTREE_MACADDR8_FAMILY_OID,
            MACADDR8_TYPE_OID,
            MACADDR8_CMP_PROC_OID,
        ),
        (
            BTREE_NETWORK_FAMILY_OID,
            CIDR_TYPE_OID,
            TEXT_CMP_EQ_PROC_OID,
        ),
        (
            BTREE_NETWORK_FAMILY_OID,
            INET_TYPE_OID,
            TEXT_CMP_EQ_PROC_OID,
        ),
        (BTREE_ENUM_FAMILY_OID, ANYENUMOID, ENUM_CMP_PROC_OID),
    ] {
        rows.push(PgAmprocRow {
            oid,
            amprocfamily: family,
            amproclefttype: type_oid,
            amprocrighttype: type_oid,
            amprocnum: 1,
            amproc: proc_oid,
        });
        oid = oid.saturating_add(1);
    }
    rows.push(PgAmprocRow {
        oid,
        amprocfamily: BTREE_INTEGER_FAMILY_OID,
        amproclefttype: INT4_TYPE_OID,
        amprocrighttype: INT4_TYPE_OID,
        amprocnum: 2,
        amproc: BTINT4_SORTSUPPORT_PROC_OID,
    });
    oid = oid.saturating_add(1);
    for (family, type_oid, proc_oid) in [
        (BTREE_BOOL_FAMILY_OID, BOOL_TYPE_OID, BTEQUALIMAGE_PROC_OID),
        (
            BTREE_INTEGER_FAMILY_OID,
            INT2_TYPE_OID,
            BTEQUALIMAGE_PROC_OID,
        ),
        (
            BTREE_INTEGER_FAMILY_OID,
            INT4_TYPE_OID,
            BTEQUALIMAGE_PROC_OID,
        ),
        (
            BTREE_INTEGER_FAMILY_OID,
            INT8_TYPE_OID,
            BTEQUALIMAGE_PROC_OID,
        ),
        (BTREE_BIT_FAMILY_OID, BIT_TYPE_OID, BTEQUALIMAGE_PROC_OID),
        (
            BTREE_VARBIT_FAMILY_OID,
            VARBIT_TYPE_OID,
            BTEQUALIMAGE_PROC_OID,
        ),
        (BTREE_ENUM_FAMILY_OID, ANYENUMOID, BTEQUALIMAGE_PROC_OID),
        (
            BTREE_MACADDR_FAMILY_OID,
            MACADDR_TYPE_OID,
            BTEQUALIMAGE_PROC_OID,
        ),
        (
            BTREE_MACADDR8_FAMILY_OID,
            MACADDR8_TYPE_OID,
            BTEQUALIMAGE_PROC_OID,
        ),
        (
            BTREE_NETWORK_FAMILY_OID,
            CIDR_TYPE_OID,
            BTEQUALIMAGE_PROC_OID,
        ),
        (
            BTREE_BYTEA_FAMILY_OID,
            BYTEA_TYPE_OID,
            BTEQUALIMAGE_PROC_OID,
        ),
        (BTREE_UUID_FAMILY_OID, UUID_TYPE_OID, BTEQUALIMAGE_PROC_OID),
        (
            BTREE_NETWORK_FAMILY_OID,
            crate::include::catalog::INET_TYPE_OID,
            BTEQUALIMAGE_PROC_OID,
        ),
        (
            crate::include::catalog::BTREE_CHAR_FAMILY_OID,
            INTERNAL_CHAR_TYPE_OID,
            BTEQUALIMAGE_PROC_OID,
        ),
        (
            crate::include::catalog::BTREE_DATETIME_FAMILY_OID,
            TIMESTAMP_TYPE_OID,
            BTEQUALIMAGE_PROC_OID,
        ),
        (
            crate::include::catalog::BTREE_DATETIME_FAMILY_OID,
            TIMESTAMPTZ_TYPE_OID,
            BTEQUALIMAGE_PROC_OID,
        ),
        (
            crate::include::catalog::BTREE_OID_FAMILY_OID,
            OID_TYPE_OID,
            BTEQUALIMAGE_PROC_OID,
        ),
        (
            crate::include::catalog::BTREE_OIDVECTOR_FAMILY_OID,
            crate::include::catalog::OIDVECTOR_TYPE_OID,
            BTEQUALIMAGE_PROC_OID,
        ),
        (
            BTREE_TEXT_FAMILY_OID,
            TEXT_TYPE_OID,
            BTVARSTREQUALIMAGE_PROC_OID,
        ),
        (
            BTREE_TEXT_FAMILY_OID,
            VARCHAR_TYPE_OID,
            BTVARSTREQUALIMAGE_PROC_OID,
        ),
        (
            BTREE_TEXT_FAMILY_OID,
            NAME_TYPE_OID,
            BTVARSTREQUALIMAGE_PROC_OID,
        ),
        (
            BTREE_BPCHAR_FAMILY_OID,
            BPCHAR_TYPE_OID,
            BTVARSTREQUALIMAGE_PROC_OID,
        ),
        (
            BTREE_TEXT_PATTERN_FAMILY_OID,
            TEXT_TYPE_OID,
            BTEQUALIMAGE_PROC_OID,
        ),
        (
            BTREE_BPCHAR_PATTERN_FAMILY_OID,
            BPCHAR_TYPE_OID,
            BTEQUALIMAGE_PROC_OID,
        ),
    ] {
        rows.push(PgAmprocRow {
            oid,
            amprocfamily: family,
            amproclefttype: type_oid,
            amprocrighttype: type_oid,
            amprocnum: 4,
            amproc: proc_oid,
        });
        oid = oid.saturating_add(1);
    }
    for (procnum, proc_oid) in [
        (1_i16, GIST_BOX_CONSISTENT_PROC_OID),
        (2, GIST_BOX_UNION_PROC_OID),
        (5, GIST_BOX_PENALTY_PROC_OID),
        (6, GIST_BOX_PICKSPLIT_PROC_OID),
        (7, GIST_BOX_SAME_PROC_OID),
        (8, GIST_BOX_DISTANCE_PROC_OID),
    ] {
        rows.push(PgAmprocRow {
            oid,
            amprocfamily: GIST_BOX_FAMILY_OID,
            amproclefttype: BOX_TYPE_OID,
            amprocrighttype: BOX_TYPE_OID,
            amprocnum: procnum,
            amproc: proc_oid,
        });
        oid = oid.saturating_add(1);
    }
    rows.push(PgAmprocRow {
        oid,
        amprocfamily: GIST_BOX_FAMILY_OID,
        amproclefttype: ANYOID,
        amprocrighttype: ANYOID,
        amprocnum: 12,
        amproc: GIST_TRANSLATE_CMPTYPE_COMMON_PROC_OID,
    });
    oid = oid.saturating_add(1);
    for (procnum, proc_oid) in [
        (1_i16, GIST_POINT_CONSISTENT_PROC_OID),
        (2, GIST_POINT_UNION_PROC_OID),
        (5, GIST_POINT_PENALTY_PROC_OID),
        (6, GIST_POINT_PICKSPLIT_PROC_OID),
        (7, GIST_POINT_SAME_PROC_OID),
        (11, GIST_POINT_SORTSUPPORT_PROC_OID),
    ] {
        rows.push(PgAmprocRow {
            oid,
            amprocfamily: GIST_POINT_FAMILY_OID,
            amproclefttype: crate::include::catalog::POINT_TYPE_OID,
            amprocrighttype: crate::include::catalog::POINT_TYPE_OID,
            amprocnum: procnum,
            amproc: proc_oid,
        });
        oid = oid.saturating_add(1);
    }
    for (family, type_oid, procs) in [
        (
            GIST_POLY_FAMILY_OID,
            POLYGON_TYPE_OID,
            [
                (1_i16, GIST_POLY_CONSISTENT_PROC_OID),
                (2, GIST_POLY_UNION_PROC_OID),
                (5, GIST_POLY_PENALTY_PROC_OID),
                (6, GIST_POLY_PICKSPLIT_PROC_OID),
                (7, GIST_POLY_SAME_PROC_OID),
                (8, GIST_POLY_DISTANCE_PROC_OID),
            ],
        ),
        (
            GIST_CIRCLE_FAMILY_OID,
            CIRCLE_TYPE_OID,
            [
                (1_i16, GIST_CIRCLE_CONSISTENT_PROC_OID),
                (2, GIST_CIRCLE_UNION_PROC_OID),
                (5, GIST_CIRCLE_PENALTY_PROC_OID),
                (6, GIST_CIRCLE_PICKSPLIT_PROC_OID),
                (7, GIST_CIRCLE_SAME_PROC_OID),
                (8, GIST_CIRCLE_DISTANCE_PROC_OID),
            ],
        ),
    ] {
        for (procnum, proc_oid) in procs {
            rows.push(PgAmprocRow {
                oid,
                amprocfamily: family,
                amproclefttype: type_oid,
                amprocrighttype: type_oid,
                amprocnum: procnum,
                amproc: proc_oid,
            });
            oid = oid.saturating_add(1);
        }
        rows.push(PgAmprocRow {
            oid,
            amprocfamily: family,
            amproclefttype: ANYOID,
            amprocrighttype: ANYOID,
            amprocnum: 12,
            amproc: GIST_TRANSLATE_CMPTYPE_COMMON_PROC_OID,
        });
        oid = oid.saturating_add(1);
    }
    for (family, procs) in [
        (
            SPGIST_QUAD_POINT_FAMILY_OID,
            [
                (1_i16, SPG_QUAD_CONFIG_PROC_OID),
                (2, SPG_QUAD_CHOOSE_PROC_OID),
                (3, SPG_QUAD_PICKSPLIT_PROC_OID),
                (4, SPG_QUAD_INNER_CONSISTENT_PROC_OID),
                (5, SPG_QUAD_LEAF_CONSISTENT_PROC_OID),
            ],
        ),
        (
            SPGIST_KD_POINT_FAMILY_OID,
            [
                (1_i16, SPG_KD_CONFIG_PROC_OID),
                (2, SPG_KD_CHOOSE_PROC_OID),
                (3, SPG_KD_PICKSPLIT_PROC_OID),
                (4, SPG_KD_INNER_CONSISTENT_PROC_OID),
                (5, SPG_QUAD_LEAF_CONSISTENT_PROC_OID),
            ],
        ),
    ] {
        for (procnum, proc_oid) in procs {
            rows.push(PgAmprocRow {
                oid,
                amprocfamily: family,
                amproclefttype: POINT_TYPE_OID,
                amprocrighttype: POINT_TYPE_OID,
                amprocnum: procnum,
                amproc: proc_oid,
            });
            oid = oid.saturating_add(1);
        }
    }
    for (procnum, proc_oid) in [
        (1_i16, SPG_TEXT_CONFIG_PROC_OID),
        (2, SPG_TEXT_CHOOSE_PROC_OID),
        (3, SPG_TEXT_PICKSPLIT_PROC_OID),
        (4, SPG_TEXT_INNER_CONSISTENT_PROC_OID),
        (5, SPG_TEXT_LEAF_CONSISTENT_PROC_OID),
    ] {
        rows.push(PgAmprocRow {
            oid,
            amprocfamily: SPGIST_TEXT_FAMILY_OID,
            amproclefttype: TEXT_TYPE_OID,
            amprocrighttype: TEXT_TYPE_OID,
            amprocnum: procnum,
            amproc: proc_oid,
        });
        oid = oid.saturating_add(1);
    }
    for (procnum, proc_oid) in [
        (1_i16, SPG_BOX_QUAD_CONFIG_PROC_OID),
        (2, SPG_BOX_QUAD_CHOOSE_PROC_OID),
        (3, SPG_BOX_QUAD_PICKSPLIT_PROC_OID),
        (4, SPG_BOX_QUAD_INNER_CONSISTENT_PROC_OID),
        (5, SPG_BOX_QUAD_LEAF_CONSISTENT_PROC_OID),
    ] {
        rows.push(PgAmprocRow {
            oid,
            amprocfamily: SPGIST_BOX_FAMILY_OID,
            amproclefttype: BOX_TYPE_OID,
            amprocrighttype: BOX_TYPE_OID,
            amprocnum: procnum,
            amproc: proc_oid,
        });
        oid = oid.saturating_add(1);
    }
    for (procnum, proc_oid) in [
        (1_i16, SPG_BOX_QUAD_CONFIG_PROC_OID),
        (2, SPG_BOX_QUAD_CHOOSE_PROC_OID),
        (3, SPG_BOX_QUAD_PICKSPLIT_PROC_OID),
        (4, SPG_BOX_QUAD_INNER_CONSISTENT_PROC_OID),
        (5, SPG_BOX_QUAD_LEAF_CONSISTENT_PROC_OID),
    ] {
        rows.push(PgAmprocRow {
            oid,
            amprocfamily: SPGIST_POLY_FAMILY_OID,
            amproclefttype: POLYGON_TYPE_OID,
            amprocrighttype: POLYGON_TYPE_OID,
            amprocnum: procnum,
            amproc: proc_oid,
        });
        oid = oid.saturating_add(1);
    }
    for (procnum, proc_oid) in [
        (1_i16, SPG_RANGE_CONFIG_PROC_OID),
        (2, SPG_RANGE_CHOOSE_PROC_OID),
        (3, SPG_RANGE_PICKSPLIT_PROC_OID),
        (4, SPG_RANGE_INNER_CONSISTENT_PROC_OID),
        (5, SPG_RANGE_LEAF_CONSISTENT_PROC_OID),
    ] {
        rows.push(PgAmprocRow {
            oid,
            amprocfamily: SPGIST_RANGE_FAMILY_OID,
            amproclefttype: ANYRANGEOID,
            amprocrighttype: ANYRANGEOID,
            amprocnum: procnum,
            amproc: proc_oid,
        });
        oid = oid.saturating_add(1);
    }
    for range_type_oid in [
        INT4RANGE_TYPE_OID,
        INT8RANGE_TYPE_OID,
        NUMRANGE_TYPE_OID,
        DATERANGE_TYPE_OID,
        TSRANGE_TYPE_OID,
        TSTZRANGE_TYPE_OID,
    ] {
        for (procnum, proc_oid) in [
            (1_i16, RANGE_GIST_CONSISTENT_PROC_OID),
            (2, RANGE_GIST_UNION_PROC_OID),
            (5, RANGE_GIST_PENALTY_PROC_OID),
            (6, RANGE_GIST_PICKSPLIT_PROC_OID),
            (7, RANGE_GIST_SAME_PROC_OID),
            (11, RANGE_SORTSUPPORT_PROC_OID),
        ] {
            rows.push(PgAmprocRow {
                oid,
                amprocfamily: GIST_RANGE_FAMILY_OID,
                amproclefttype: range_type_oid,
                amprocrighttype: range_type_oid,
                amprocnum: procnum,
                amproc: proc_oid,
            });
            oid = oid.saturating_add(1);
        }
    }
    rows.push(PgAmprocRow {
        oid,
        amprocfamily: GIST_RANGE_FAMILY_OID,
        amproclefttype: ANYOID,
        amprocrighttype: ANYOID,
        amprocnum: 12,
        amproc: GIST_TRANSLATE_CMPTYPE_COMMON_PROC_OID,
    });
    oid = oid.saturating_add(1);
    for (procnum, proc_oid) in [
        (1_i16, GIST_NETWORK_CONSISTENT_PROC_OID),
        (2, GIST_NETWORK_UNION_PROC_OID),
        (5, GIST_NETWORK_PENALTY_PROC_OID),
        (6, GIST_NETWORK_PICKSPLIT_PROC_OID),
        (7, GIST_NETWORK_SAME_PROC_OID),
    ] {
        rows.push(PgAmprocRow {
            oid,
            amprocfamily: GIST_NETWORK_FAMILY_OID,
            amproclefttype: INET_TYPE_OID,
            amprocrighttype: INET_TYPE_OID,
            amprocnum: procnum,
            amproc: proc_oid,
        });
        oid = oid.saturating_add(1);
    }
    for (procnum, proc_oid) in [
        (1_i16, GIST_TSVECTOR_CONSISTENT_PROC_OID),
        (2, GIST_TSVECTOR_UNION_PROC_OID),
        (5, GIST_TSVECTOR_PENALTY_PROC_OID),
        (6, GIST_TSVECTOR_PICKSPLIT_PROC_OID),
        (7, GIST_TSVECTOR_SAME_PROC_OID),
    ] {
        rows.push(PgAmprocRow {
            oid,
            amprocfamily: GIST_TSVECTOR_FAMILY_OID,
            amproclefttype: TSVECTOR_TYPE_OID,
            amprocrighttype: TSVECTOR_TYPE_OID,
            amprocnum: procnum,
            amproc: proc_oid,
        });
        oid = oid.saturating_add(1);
    }
    for (procnum, proc_oid) in [
        (1_i16, GIST_TSQUERY_CONSISTENT_PROC_OID),
        (2, GIST_TSQUERY_UNION_PROC_OID),
        (5, GIST_TSQUERY_PENALTY_PROC_OID),
        (6, GIST_TSQUERY_PICKSPLIT_PROC_OID),
        (7, GIST_TSQUERY_SAME_PROC_OID),
    ] {
        rows.push(PgAmprocRow {
            oid,
            amprocfamily: GIST_TSQUERY_FAMILY_OID,
            amproclefttype: TSQUERY_TYPE_OID,
            amprocrighttype: TSQUERY_TYPE_OID,
            amprocnum: procnum,
            amproc: proc_oid,
        });
        oid = oid.saturating_add(1);
    }
    for (procnum, proc_oid) in [
        (1_i16, SPG_NETWORK_CONFIG_PROC_OID),
        (2, SPG_NETWORK_CHOOSE_PROC_OID),
        (3, SPG_NETWORK_PICKSPLIT_PROC_OID),
        (4, SPG_NETWORK_INNER_CONSISTENT_PROC_OID),
        (5, SPG_NETWORK_LEAF_CONSISTENT_PROC_OID),
    ] {
        rows.push(PgAmprocRow {
            oid,
            amprocfamily: SPGIST_NETWORK_FAMILY_OID,
            amproclefttype: INET_TYPE_OID,
            amprocrighttype: INET_TYPE_OID,
            amprocnum: procnum,
            amproc: proc_oid,
        });
        oid = oid.saturating_add(1);
    }
    for (procnum, proc_oid) in [
        (1_i16, MULTIRANGE_GIST_CONSISTENT_PROC_OID),
        (2, RANGE_GIST_UNION_PROC_OID),
        (5, RANGE_GIST_PENALTY_PROC_OID),
        (6, RANGE_GIST_PICKSPLIT_PROC_OID),
        (7, RANGE_GIST_SAME_PROC_OID),
        (11, MULTIRANGE_SORTSUPPORT_PROC_OID),
    ] {
        rows.push(PgAmprocRow {
            oid,
            amprocfamily: GIST_MULTIRANGE_FAMILY_OID,
            amproclefttype: ANYMULTIRANGEOID,
            amprocrighttype: ANYMULTIRANGEOID,
            amprocnum: procnum,
            amproc: proc_oid,
        });
        oid = oid.saturating_add(1);
    }
    rows.push(PgAmprocRow {
        oid,
        amprocfamily: GIST_MULTIRANGE_FAMILY_OID,
        amproclefttype: ANYOID,
        amprocrighttype: ANYOID,
        amprocnum: 12,
        amproc: GIST_TRANSLATE_CMPTYPE_COMMON_PROC_OID,
    });
    oid = oid.saturating_add(1);
    let operators = bootstrap_pg_operator_rows();
    for (family, type_oid) in [
        (BRIN_BYTEA_MINMAX_FAMILY_OID, BYTEA_TYPE_OID),
        (BRIN_CHAR_MINMAX_FAMILY_OID, INTERNAL_CHAR_TYPE_OID),
        (BRIN_INTEGER_MINMAX_FAMILY_OID, INT2_TYPE_OID),
        (BRIN_INTEGER_MINMAX_FAMILY_OID, INT4_TYPE_OID),
        (BRIN_INTEGER_MINMAX_FAMILY_OID, INT8_TYPE_OID),
        (BRIN_NUMERIC_MINMAX_FAMILY_OID, NUMERIC_TYPE_OID),
        (BRIN_OID_MINMAX_FAMILY_OID, OID_TYPE_OID),
        (BRIN_TID_MINMAX_FAMILY_OID, TID_TYPE_OID),
        (BRIN_FLOAT_MINMAX_FAMILY_OID, FLOAT4_TYPE_OID),
        (BRIN_FLOAT_MINMAX_FAMILY_OID, FLOAT8_TYPE_OID),
        (BRIN_TEXT_MINMAX_FAMILY_OID, TEXT_TYPE_OID),
        (BRIN_BPCHAR_MINMAX_FAMILY_OID, BPCHAR_TYPE_OID),
        (BRIN_DATETIME_MINMAX_FAMILY_OID, DATE_TYPE_OID),
        (BRIN_TIME_MINMAX_FAMILY_OID, TIME_TYPE_OID),
        (BRIN_TIMETZ_MINMAX_FAMILY_OID, TIMETZ_TYPE_OID),
        (BRIN_INTERVAL_MINMAX_FAMILY_OID, INTERVAL_TYPE_OID),
        (BRIN_DATETIME_MINMAX_FAMILY_OID, TIMESTAMP_TYPE_OID),
        (BRIN_DATETIME_MINMAX_FAMILY_OID, TIMESTAMPTZ_TYPE_OID),
        (BRIN_BIT_MINMAX_FAMILY_OID, BIT_TYPE_OID),
        (BRIN_VARBIT_MINMAX_FAMILY_OID, VARBIT_TYPE_OID),
        (BRIN_UUID_MINMAX_FAMILY_OID, UUID_TYPE_OID),
        (BRIN_PG_LSN_MINMAX_FAMILY_OID, PG_LSN_TYPE_OID),
        (BRIN_MACADDR_MINMAX_FAMILY_OID, MACADDR_TYPE_OID),
        (BRIN_MACADDR8_MINMAX_FAMILY_OID, MACADDR8_TYPE_OID),
        (BRIN_NAME_MINMAX_FAMILY_OID, NAME_TYPE_OID),
        // :HACK: Generic BRIN minmax-multi and bloom runtime support is not
        // implemented yet; these rows expose PostgreSQL-compatible catalogs.
        (BRIN_MACADDR_MINMAX_MULTI_FAMILY_OID, MACADDR_TYPE_OID),
        (BRIN_MACADDR8_MINMAX_MULTI_FAMILY_OID, MACADDR8_TYPE_OID),
        (BRIN_MACADDR_BLOOM_FAMILY_OID, MACADDR_TYPE_OID),
        (BRIN_MACADDR8_BLOOM_FAMILY_OID, MACADDR8_TYPE_OID),
        (BRIN_NETWORK_MINMAX_FAMILY_OID, INET_TYPE_OID),
    ] {
        for (procnum, proc_oid) in [
            (1_i16, BRIN_MINMAX_OPCINFO_PROC_OID),
            (2, BRIN_MINMAX_ADD_VALUE_PROC_OID),
            (3, BRIN_MINMAX_CONSISTENT_PROC_OID),
            (4, BRIN_MINMAX_UNION_PROC_OID),
            (11, operator_proc_oid(&operators, "<", type_oid, type_oid)),
            (12, operator_proc_oid(&operators, "<=", type_oid, type_oid)),
            (13, operator_proc_oid(&operators, ">=", type_oid, type_oid)),
            (14, operator_proc_oid(&operators, ">", type_oid, type_oid)),
        ] {
            rows.push(PgAmprocRow {
                oid,
                amprocfamily: family,
                amproclefttype: type_oid,
                amprocrighttype: type_oid,
                amprocnum: procnum,
                amproc: proc_oid,
            });
            oid = oid.saturating_add(1);
        }
    }
    // :HACK: Native BRIN inclusion summaries are not implemented yet. These
    // rows let explicit inclusion opclasses build, while the BRIN scan path
    // treats them as lossy and relies on heap recheck for correctness.
    for (family, type_oid) in [
        (BRIN_NETWORK_INCLUSION_FAMILY_OID, INET_TYPE_OID),
        (BRIN_RANGE_INCLUSION_FAMILY_OID, ANYRANGEOID),
        (BRIN_BOX_INCLUSION_FAMILY_OID, BOX_TYPE_OID),
    ] {
        for (procnum, proc_oid) in [
            (1_i16, BRIN_MINMAX_OPCINFO_PROC_OID),
            (2, BRIN_MINMAX_ADD_VALUE_PROC_OID),
            (3, BRIN_MINMAX_CONSISTENT_PROC_OID),
            (4, BRIN_MINMAX_UNION_PROC_OID),
        ] {
            rows.push(PgAmprocRow {
                oid,
                amprocfamily: family,
                amproclefttype: type_oid,
                amprocrighttype: type_oid,
                amprocnum: procnum,
                amproc: proc_oid,
            });
            oid = oid.saturating_add(1);
        }
    }
    for (procnum, proc_oid) in [
        (1_i16, GIN_COMPARE_JSONB_PROC_OID),
        (2, GIN_EXTRACT_JSONB_PROC_OID),
        (3, GIN_EXTRACT_JSONB_QUERY_PROC_OID),
        (4, GIN_CONSISTENT_JSONB_PROC_OID),
        (6, GIN_TRICONSISTENT_JSONB_PROC_OID),
    ] {
        rows.push(PgAmprocRow {
            oid,
            amprocfamily: GIN_JSONB_FAMILY_OID,
            amproclefttype: JSONB_TYPE_OID,
            amprocrighttype: JSONB_TYPE_OID,
            amprocnum: procnum,
            amproc: proc_oid,
        });
        oid = oid.saturating_add(1);
    }
    for (family, type_oid, proc_oid) in [
        (HASH_ARRAY_FAMILY_OID, ANYARRAYOID, HASH_ARRAY_PROC_OID),
        (HASH_BOOL_FAMILY_OID, BOOL_TYPE_OID, HASH_BOOL_PROC_OID),
        (HASH_INTEGER_FAMILY_OID, INT2_TYPE_OID, HASH_INT2_PROC_OID),
        (HASH_INTEGER_FAMILY_OID, INT4_TYPE_OID, HASH_INT4_PROC_OID),
        (HASH_INTEGER_FAMILY_OID, INT8_TYPE_OID, HASH_INT8_PROC_OID),
        (HASH_OID_FAMILY_OID, OID_TYPE_OID, HASH_OID_PROC_OID),
        (HASH_ENUM_FAMILY_OID, ANYENUMOID, HASH_ENUM_PROC_OID),
        (
            HASH_RECORD_FAMILY_OID,
            RECORD_TYPE_OID,
            HASH_RECORD_PROC_OID,
        ),
        (
            HASH_CHAR_FAMILY_OID,
            INTERNAL_CHAR_TYPE_OID,
            HASH_CHAR_PROC_OID,
        ),
        (HASH_TEXT_FAMILY_OID, NAME_TYPE_OID, HASH_NAME_PROC_OID),
        (HASH_TEXT_FAMILY_OID, TEXT_TYPE_OID, HASH_TEXT_PROC_OID),
        (
            HASH_TEXT_FAMILY_OID,
            VARCHAR_TYPE_OID,
            HASH_VARCHAR_PROC_OID,
        ),
        (
            HASH_BPCHAR_FAMILY_OID,
            BPCHAR_TYPE_OID,
            HASH_BPCHAR_PROC_OID,
        ),
        (HASH_FLOAT_FAMILY_OID, FLOAT4_TYPE_OID, HASH_FLOAT4_PROC_OID),
        (HASH_FLOAT_FAMILY_OID, FLOAT8_TYPE_OID, HASH_FLOAT8_PROC_OID),
        (
            HASH_NUMERIC_FAMILY_OID,
            NUMERIC_TYPE_OID,
            HASH_NUMERIC_PROC_OID,
        ),
        (
            HASH_TIMESTAMP_FAMILY_OID,
            TIMESTAMP_TYPE_OID,
            HASH_TIMESTAMP_PROC_OID,
        ),
        (
            HASH_TIMESTAMPTZ_FAMILY_OID,
            TIMESTAMPTZ_TYPE_OID,
            HASH_TIMESTAMPTZ_PROC_OID,
        ),
        (HASH_DATE_FAMILY_OID, DATE_TYPE_OID, HASH_DATE_PROC_OID),
        (HASH_TIME_FAMILY_OID, TIME_TYPE_OID, HASH_TIME_PROC_OID),
        (
            HASH_TIMETZ_FAMILY_OID,
            TIMETZ_TYPE_OID,
            HASH_TIMETZ_PROC_OID,
        ),
        (HASH_BYTEA_FAMILY_OID, BYTEA_TYPE_OID, HASH_BYTEA_PROC_OID),
        (HASH_UUID_FAMILY_OID, UUID_TYPE_OID, HASH_UUID_PROC_OID),
        (HASH_RANGE_FAMILY_OID, ANYRANGEOID, HASH_RANGE_PROC_OID),
        (
            HASH_MULTIRANGE_FAMILY_OID,
            ANYMULTIRANGEOID,
            HASH_MULTIRANGE_PROC_OID,
        ),
        (HASH_JSONB_FAMILY_OID, JSONB_TYPE_OID, HASH_JSONB_PROC_OID),
        (
            HASH_MACADDR_FAMILY_OID,
            MACADDR_TYPE_OID,
            HASH_MACADDR_PROC_OID,
        ),
        (
            HASH_MACADDR8_FAMILY_OID,
            MACADDR8_TYPE_OID,
            HASH_MACADDR8_PROC_OID,
        ),
        (
            HASH_INTERVAL_FAMILY_OID,
            INTERVAL_TYPE_OID,
            HASH_INTERVAL_PROC_OID,
        ),
    ] {
        rows.push(PgAmprocRow {
            oid,
            amprocfamily: family,
            amproclefttype: type_oid,
            amprocrighttype: type_oid,
            amprocnum: 1,
            amproc: proc_oid,
        });
        oid = oid.saturating_add(1);
        if family == HASH_ARRAY_FAMILY_OID {
            rows.push(PgAmprocRow {
                oid,
                amprocfamily: family,
                amproclefttype: type_oid,
                amprocrighttype: type_oid,
                amprocnum: 2,
                amproc: HASH_ARRAY_EXTENDED_PROC_OID,
            });
            oid = oid.saturating_add(1);
        } else if family == HASH_ENUM_FAMILY_OID {
            rows.push(PgAmprocRow {
                oid,
                amprocfamily: family,
                amproclefttype: type_oid,
                amprocrighttype: type_oid,
                amprocnum: 2,
                amproc: HASH_ENUM_EXTENDED_PROC_OID,
            });
            oid = oid.saturating_add(1);
        }
        if family == HASH_RECORD_FAMILY_OID {
            rows.push(PgAmprocRow {
                oid,
                amprocfamily: family,
                amproclefttype: type_oid,
                amprocrighttype: type_oid,
                amprocnum: 2,
                amproc: HASH_RECORD_EXTENDED_PROC_OID,
            });
            oid = oid.saturating_add(1);
        }
        if family == HASH_JSONB_FAMILY_OID {
            rows.push(PgAmprocRow {
                oid,
                amprocfamily: family,
                amproclefttype: type_oid,
                amprocrighttype: type_oid,
                amprocnum: 2,
                amproc: HASH_JSONB_EXTENDED_PROC_OID,
            });
            oid = oid.saturating_add(1);
        }
    }
    rows
}

fn operator_proc_oid(
    rows: &[crate::include::catalog::PgOperatorRow],
    name: &str,
    left: u32,
    right: u32,
) -> u32 {
    rows.iter()
        .find(|row| row.oprname == name && row.oprleft == left && row.oprright == right)
        .map(|row| row.oprcode)
        .unwrap_or_else(|| panic!("missing bootstrap operator proc {name}({left},{right})"))
}

#[cfg(test)]
mod tests {
    use crate::include::catalog::{
        BOX_TYPE_OID, BRIN_MACADDR_MINMAX_FAMILY_OID, BRIN_MACADDR8_BLOOM_FAMILY_OID,
        BRIN_MACADDR8_MINMAX_FAMILY_OID, BRIN_MINMAX_CONSISTENT_PROC_OID, BTREE_MACADDR_FAMILY_OID,
        BTREE_MACADDR8_FAMILY_OID, HASH_MACADDR_FAMILY_OID, HASH_MACADDR_PROC_OID,
        HASH_MACADDR8_FAMILY_OID, HASH_MACADDR8_PROC_OID, MACADDR_CMP_PROC_OID, MACADDR_TYPE_OID,
        MACADDR8_CMP_PROC_OID, MACADDR8_GT_PROC_OID, MACADDR8_TYPE_OID,
        SPG_BOX_QUAD_CHOOSE_PROC_OID, SPG_BOX_QUAD_CONFIG_PROC_OID,
        SPG_BOX_QUAD_INNER_CONSISTENT_PROC_OID, SPG_BOX_QUAD_LEAF_CONSISTENT_PROC_OID,
        SPG_BOX_QUAD_PICKSPLIT_PROC_OID, SPGIST_BOX_FAMILY_OID,
    };

    use super::bootstrap_pg_amproc_rows;

    #[test]
    fn spgist_box_family_uses_native_support_proc_numbers() {
        let rows = bootstrap_pg_amproc_rows()
            .into_iter()
            .filter(|row| row.amprocfamily == SPGIST_BOX_FAMILY_OID)
            .collect::<Vec<_>>();

        assert_eq!(rows.len(), 5);
        assert!(rows.iter().all(|row| row.amproclefttype == BOX_TYPE_OID));
        assert!(rows.iter().all(|row| row.amprocrighttype == BOX_TYPE_OID));
        assert_eq!(
            rows.iter()
                .map(|row| (row.amprocnum, row.amproc))
                .collect::<Vec<_>>(),
            vec![
                (1, SPG_BOX_QUAD_CONFIG_PROC_OID),
                (2, SPG_BOX_QUAD_CHOOSE_PROC_OID),
                (3, SPG_BOX_QUAD_PICKSPLIT_PROC_OID),
                (4, SPG_BOX_QUAD_INNER_CONSISTENT_PROC_OID),
                (5, SPG_BOX_QUAD_LEAF_CONSISTENT_PROC_OID),
            ]
        );
    }

    #[test]
    fn macaddr_amproc_rows_cover_btree_hash_and_brin_catalogs() {
        let rows = bootstrap_pg_amproc_rows();
        assert!(rows.iter().any(|row| {
            row.amprocfamily == BTREE_MACADDR_FAMILY_OID
                && row.amproclefttype == MACADDR_TYPE_OID
                && row.amprocrighttype == MACADDR_TYPE_OID
                && row.amprocnum == 1
                && row.amproc == MACADDR_CMP_PROC_OID
        }));
        assert!(rows.iter().any(|row| {
            row.amprocfamily == BTREE_MACADDR8_FAMILY_OID
                && row.amproclefttype == MACADDR8_TYPE_OID
                && row.amprocrighttype == MACADDR8_TYPE_OID
                && row.amprocnum == 1
                && row.amproc == MACADDR8_CMP_PROC_OID
        }));
        assert!(rows.iter().any(|row| {
            row.amprocfamily == HASH_MACADDR_FAMILY_OID
                && row.amproclefttype == MACADDR_TYPE_OID
                && row.amprocnum == 1
                && row.amproc == HASH_MACADDR_PROC_OID
        }));
        assert!(rows.iter().any(|row| {
            row.amprocfamily == HASH_MACADDR8_FAMILY_OID
                && row.amproclefttype == MACADDR8_TYPE_OID
                && row.amprocnum == 1
                && row.amproc == HASH_MACADDR8_PROC_OID
        }));
        assert!(rows.iter().any(|row| {
            row.amprocfamily == BRIN_MACADDR_MINMAX_FAMILY_OID
                && row.amproclefttype == MACADDR_TYPE_OID
                && row.amprocnum == 3
                && row.amproc == BRIN_MINMAX_CONSISTENT_PROC_OID
        }));
        assert!(rows.iter().any(|row| {
            row.amprocfamily == BRIN_MACADDR8_MINMAX_FAMILY_OID
                && row.amproclefttype == MACADDR8_TYPE_OID
                && row.amprocnum == 14
                && row.amproc == MACADDR8_GT_PROC_OID
        }));
        assert!(rows.iter().any(|row| {
            row.amprocfamily == BRIN_MACADDR8_BLOOM_FAMILY_OID
                && row.amproclefttype == MACADDR8_TYPE_OID
                && row.amprocnum == 3
                && row.amproc == BRIN_MINMAX_CONSISTENT_PROC_OID
        }));
    }
}
