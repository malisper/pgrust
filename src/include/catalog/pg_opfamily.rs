use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, BRIN_AM_OID, BTREE_AM_OID, GIN_AM_OID, GIST_AM_OID, HASH_AM_OID,
    PG_CATALOG_NAMESPACE_OID, SPGIST_AM_OID,
};

pub const BTREE_INTEGER_FAMILY_OID: u32 = 1976;
pub const BTREE_ARRAY_FAMILY_OID: u32 = 397;
pub const BTREE_CHAR_FAMILY_OID: u32 = 429;
pub const BTREE_OIDVECTOR_FAMILY_OID: u32 = 1991;
pub const BTREE_TEXT_FAMILY_OID: u32 = 1994;
pub const BTREE_BPCHAR_FAMILY_OID: u32 = 426;
pub const BTREE_TEXT_PATTERN_FAMILY_OID: u32 = 2095;
pub const BTREE_BPCHAR_PATTERN_FAMILY_OID: u32 = 2097;
pub const BTREE_OID_FAMILY_OID: u32 = 1989;
pub const BTREE_BOOL_FAMILY_OID: u32 = 424;
pub const BTREE_NUMERIC_FAMILY_OID: u32 = 1988;
pub const BTREE_INTERVAL_FAMILY_OID: u32 = 1982;
pub const BTREE_NETWORK_FAMILY_OID: u32 = 1974;
pub const BTREE_BIT_FAMILY_OID: u32 = 423;
pub const BTREE_BYTEA_FAMILY_OID: u32 = 428;
pub const BTREE_UUID_FAMILY_OID: u32 = 2968;
pub const BTREE_MACADDR_FAMILY_OID: u32 = 76040;
pub const BTREE_MACADDR8_FAMILY_OID: u32 = 76041;
pub const BTREE_DATETIME_FAMILY_OID: u32 = 434;
pub const BTREE_FLOAT_FAMILY_OID: u32 = 1970;
pub const BTREE_VARBIT_FAMILY_OID: u32 = 2002;
pub const BTREE_RECORD_FAMILY_OID: u32 = 2994;
pub const BTREE_RECORD_IMAGE_FAMILY_OID: u32 = 3194;
pub const BTREE_TSVECTOR_FAMILY_OID: u32 = 3626;
pub const BTREE_TSQUERY_FAMILY_OID: u32 = 3683;
pub const BTREE_RANGE_FAMILY_OID: u32 = 3901;
pub const BTREE_JSONB_FAMILY_OID: u32 = 4033;
pub const BTREE_MULTIRANGE_FAMILY_OID: u32 = 4199;
pub const GIST_POINT_FAMILY_OID: u32 = 1029;
pub const GIST_BOX_FAMILY_OID: u32 = 2593;
pub const GIST_POLY_FAMILY_OID: u32 = 2594;
pub const GIST_CIRCLE_FAMILY_OID: u32 = 2595;
pub const GIST_NETWORK_FAMILY_OID: u32 = 3550;
pub const GIST_RANGE_FAMILY_OID: u32 = 3919;
pub const GIST_MULTIRANGE_FAMILY_OID: u32 = 6158;
pub const GIST_TSVECTOR_FAMILY_OID: u32 = 3655;
pub const GIN_ARRAY_FAMILY_OID: u32 = 2745;
pub const GIN_TSVECTOR_FAMILY_OID: u32 = 3659;
pub const GIN_JSONB_FAMILY_OID: u32 = 4036;
pub const SPGIST_NETWORK_FAMILY_OID: u32 = 3794;
pub const SPGIST_RANGE_FAMILY_OID: u32 = 3474;
pub const SPGIST_QUAD_POINT_FAMILY_OID: u32 = 4015;
pub const SPGIST_KD_POINT_FAMILY_OID: u32 = 4016;
pub const SPGIST_BOX_FAMILY_OID: u32 = 4001;
pub const SPGIST_POLY_FAMILY_OID: u32 = 4002;
pub const SPGIST_TEXT_FAMILY_OID: u32 = 4017;
pub const BRIN_BYTEA_MINMAX_FAMILY_OID: u32 = 76100;
pub const BRIN_CHAR_MINMAX_FAMILY_OID: u32 = 76101;
pub const BRIN_INTEGER_MINMAX_FAMILY_OID: u32 = 76102;
pub const BRIN_TEXT_MINMAX_FAMILY_OID: u32 = 76103;
pub const BRIN_OID_MINMAX_FAMILY_OID: u32 = 76104;
pub const BRIN_FLOAT_MINMAX_FAMILY_OID: u32 = 76105;
pub const BRIN_BPCHAR_MINMAX_FAMILY_OID: u32 = 76106;
pub const BRIN_TIME_MINMAX_FAMILY_OID: u32 = 76107;
pub const BRIN_DATETIME_MINMAX_FAMILY_OID: u32 = 76108;
pub const BRIN_TIMETZ_MINMAX_FAMILY_OID: u32 = 76109;
pub const BRIN_BIT_MINMAX_FAMILY_OID: u32 = 76110;
pub const BRIN_VARBIT_MINMAX_FAMILY_OID: u32 = 76111;
pub const BRIN_MACADDR_MINMAX_FAMILY_OID: u32 = 76114;
pub const BRIN_MACADDR8_MINMAX_FAMILY_OID: u32 = 76115;
pub const BRIN_MACADDR_MINMAX_MULTI_FAMILY_OID: u32 = 76116;
pub const BRIN_MACADDR8_MINMAX_MULTI_FAMILY_OID: u32 = 76117;
pub const BRIN_MACADDR_BLOOM_FAMILY_OID: u32 = 76118;
pub const BRIN_MACADDR8_BLOOM_FAMILY_OID: u32 = 76119;
pub const BRIN_TEXT_BLOOM_FAMILY_OID: u32 = 4573;
pub const BRIN_NETWORK_INCLUSION_FAMILY_OID: u32 = 4102;
pub const BRIN_RANGE_INCLUSION_FAMILY_OID: u32 = 4103;
pub const BRIN_BOX_INCLUSION_FAMILY_OID: u32 = 4104;
pub const HASH_BPCHAR_FAMILY_OID: u32 = 427;
pub const HASH_CHAR_FAMILY_OID: u32 = 431;
pub const HASH_DATE_FAMILY_OID: u32 = 435;
pub const HASH_FLOAT_FAMILY_OID: u32 = 1971;
pub const HASH_INTEGER_FAMILY_OID: u32 = 1977;
pub const HASH_INTERVAL_FAMILY_OID: u32 = 1983;
pub const HASH_NUMERIC_FAMILY_OID: u32 = 1998;
pub const HASH_OID_FAMILY_OID: u32 = 1990;
pub const HASH_TEXT_FAMILY_OID: u32 = 1995;
pub const HASH_TIME_FAMILY_OID: u32 = 1997;
pub const HASH_TIMESTAMPTZ_FAMILY_OID: u32 = 1999;
pub const HASH_TIMETZ_FAMILY_OID: u32 = 2001;
pub const HASH_TIMESTAMP_FAMILY_OID: u32 = 2040;
pub const HASH_BOOL_FAMILY_OID: u32 = 2222;
pub const HASH_BYTEA_FAMILY_OID: u32 = 2223;
pub const HASH_UUID_FAMILY_OID: u32 = 2969;
pub const HASH_MULTIRANGE_FAMILY_OID: u32 = 4225;
pub const HASH_MACADDR_FAMILY_OID: u32 = 76230;
pub const HASH_MACADDR8_FAMILY_OID: u32 = 76231;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgOpfamilyRow {
    pub oid: u32,
    pub opfmethod: u32,
    pub opfname: String,
    pub opfnamespace: u32,
    pub opfowner: u32,
}

pub fn pg_opfamily_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("opfmethod", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("opfname", SqlType::new(SqlTypeKind::Name), false),
            column_desc("opfnamespace", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("opfowner", SqlType::new(SqlTypeKind::Oid), false),
        ],
    }
}

pub fn bootstrap_pg_opfamily_rows() -> Vec<PgOpfamilyRow> {
    vec![
        PgOpfamilyRow {
            oid: BTREE_ARRAY_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "array_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: BTREE_BOOL_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "bool_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: BTREE_BIT_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "bit_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: BTREE_BYTEA_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "bytea_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: BTREE_BPCHAR_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "bpchar_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: BTREE_UUID_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "uuid_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: BTREE_MACADDR_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "macaddr_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: BTREE_MACADDR8_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "macaddr8_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: BTREE_CHAR_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "char_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: BTREE_DATETIME_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "datetime_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: BTREE_FLOAT_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "float_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: BTREE_INTEGER_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "integer_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: BTREE_INTERVAL_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "interval_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: BTREE_OIDVECTOR_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "oidvector_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: BTREE_NUMERIC_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "numeric_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: BTREE_OID_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "oid_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: BTREE_TEXT_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "text_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        btree_row(BTREE_TEXT_PATTERN_FAMILY_OID, "text_pattern_ops"),
        btree_row(BTREE_BPCHAR_PATTERN_FAMILY_OID, "bpchar_pattern_ops"),
        PgOpfamilyRow {
            oid: BTREE_VARBIT_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "varbit_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        btree_row(BTREE_RECORD_FAMILY_OID, "record_ops"),
        btree_row(BTREE_RECORD_IMAGE_FAMILY_OID, "record_image_ops"),
        btree_row(BTREE_TSVECTOR_FAMILY_OID, "tsvector_ops"),
        btree_row(BTREE_TSQUERY_FAMILY_OID, "tsquery_ops"),
        btree_row(BTREE_RANGE_FAMILY_OID, "range_ops"),
        btree_row(BTREE_JSONB_FAMILY_OID, "jsonb_ops"),
        btree_row(BTREE_NETWORK_FAMILY_OID, "network_ops"),
        PgOpfamilyRow {
            oid: BTREE_MULTIRANGE_FAMILY_OID,
            opfmethod: BTREE_AM_OID,
            opfname: "multirange_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: GIST_POINT_FAMILY_OID,
            opfmethod: GIST_AM_OID,
            opfname: "point_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: GIST_BOX_FAMILY_OID,
            opfmethod: GIST_AM_OID,
            opfname: "box_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: GIST_RANGE_FAMILY_OID,
            opfmethod: GIST_AM_OID,
            opfname: "range_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: GIST_NETWORK_FAMILY_OID,
            opfmethod: GIST_AM_OID,
            opfname: "network_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: GIST_TSVECTOR_FAMILY_OID,
            opfmethod: GIST_AM_OID,
            opfname: "tsvector_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: SPGIST_NETWORK_FAMILY_OID,
            opfmethod: SPGIST_AM_OID,
            opfname: "network_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: SPGIST_RANGE_FAMILY_OID,
            opfmethod: SPGIST_AM_OID,
            opfname: "range_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: GIST_MULTIRANGE_FAMILY_OID,
            opfmethod: GIST_AM_OID,
            opfname: "multirange_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: SPGIST_QUAD_POINT_FAMILY_OID,
            opfmethod: SPGIST_AM_OID,
            opfname: "quad_point_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: SPGIST_KD_POINT_FAMILY_OID,
            opfmethod: SPGIST_AM_OID,
            opfname: "kd_point_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: SPGIST_TEXT_FAMILY_OID,
            opfmethod: SPGIST_AM_OID,
            opfname: "text_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: SPGIST_BOX_FAMILY_OID,
            opfmethod: SPGIST_AM_OID,
            opfname: "box_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: SPGIST_POLY_FAMILY_OID,
            opfmethod: SPGIST_AM_OID,
            opfname: "poly_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: GIN_TSVECTOR_FAMILY_OID,
            opfmethod: GIN_AM_OID,
            opfname: "tsvector_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: GIN_ARRAY_FAMILY_OID,
            opfmethod: GIN_AM_OID,
            opfname: "array_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        PgOpfamilyRow {
            oid: GIN_JSONB_FAMILY_OID,
            opfmethod: GIN_AM_OID,
            opfname: "jsonb_ops".into(),
            opfnamespace: PG_CATALOG_NAMESPACE_OID,
            opfowner: BOOTSTRAP_SUPERUSER_OID,
        },
        brin_row(BRIN_BYTEA_MINMAX_FAMILY_OID, "bytea_minmax_ops"),
        brin_row(BRIN_CHAR_MINMAX_FAMILY_OID, "char_minmax_ops"),
        brin_row(BRIN_INTEGER_MINMAX_FAMILY_OID, "integer_minmax_ops"),
        brin_row(BRIN_TEXT_MINMAX_FAMILY_OID, "text_minmax_ops"),
        brin_row(BRIN_OID_MINMAX_FAMILY_OID, "oid_minmax_ops"),
        brin_row(BRIN_FLOAT_MINMAX_FAMILY_OID, "float_minmax_ops"),
        brin_row(BRIN_BPCHAR_MINMAX_FAMILY_OID, "bpchar_minmax_ops"),
        brin_row(BRIN_TIME_MINMAX_FAMILY_OID, "time_minmax_ops"),
        brin_row(BRIN_DATETIME_MINMAX_FAMILY_OID, "datetime_minmax_ops"),
        brin_row(BRIN_TIMETZ_MINMAX_FAMILY_OID, "timetz_minmax_ops"),
        brin_row(BRIN_BIT_MINMAX_FAMILY_OID, "bit_minmax_ops"),
        brin_row(BRIN_VARBIT_MINMAX_FAMILY_OID, "varbit_minmax_ops"),
        brin_row(BRIN_MACADDR_MINMAX_FAMILY_OID, "macaddr_minmax_ops"),
        brin_row(BRIN_MACADDR8_MINMAX_FAMILY_OID, "macaddr8_minmax_ops"),
        // :HACK: pgrust's BRIN runtime is still generic minmax-only; expose
        // PostgreSQL-compatible minmax-multi and bloom catalog families now.
        brin_row(
            BRIN_MACADDR_MINMAX_MULTI_FAMILY_OID,
            "macaddr_minmax_multi_ops",
        ),
        brin_row(
            BRIN_MACADDR8_MINMAX_MULTI_FAMILY_OID,
            "macaddr8_minmax_multi_ops",
        ),
        brin_row(BRIN_MACADDR_BLOOM_FAMILY_OID, "macaddr_bloom_ops"),
        brin_row(BRIN_MACADDR8_BLOOM_FAMILY_OID, "macaddr8_bloom_ops"),
        brin_row(BRIN_TEXT_BLOOM_FAMILY_OID, "text_bloom_ops"),
        brin_row(BRIN_NETWORK_INCLUSION_FAMILY_OID, "network_inclusion_ops"),
        brin_row(BRIN_RANGE_INCLUSION_FAMILY_OID, "range_inclusion_ops"),
        brin_row(BRIN_BOX_INCLUSION_FAMILY_OID, "box_inclusion_ops"),
        hash_row(HASH_BPCHAR_FAMILY_OID, "bpchar_ops"),
        hash_row(HASH_CHAR_FAMILY_OID, "char_ops"),
        hash_row(HASH_DATE_FAMILY_OID, "date_ops"),
        hash_row(HASH_FLOAT_FAMILY_OID, "float_ops"),
        hash_row(HASH_INTEGER_FAMILY_OID, "integer_ops"),
        hash_row(HASH_INTERVAL_FAMILY_OID, "interval_ops"),
        hash_row(HASH_NUMERIC_FAMILY_OID, "numeric_ops"),
        hash_row(HASH_OID_FAMILY_OID, "oid_ops"),
        hash_row(HASH_TEXT_FAMILY_OID, "text_ops"),
        hash_row(HASH_TIME_FAMILY_OID, "time_ops"),
        hash_row(HASH_TIMESTAMPTZ_FAMILY_OID, "timestamptz_ops"),
        hash_row(HASH_TIMETZ_FAMILY_OID, "timetz_ops"),
        hash_row(HASH_TIMESTAMP_FAMILY_OID, "timestamp_ops"),
        hash_row(HASH_BOOL_FAMILY_OID, "bool_ops"),
        hash_row(HASH_BYTEA_FAMILY_OID, "bytea_ops"),
        hash_row(HASH_UUID_FAMILY_OID, "uuid_ops"),
        hash_row(HASH_MULTIRANGE_FAMILY_OID, "multirange_ops"),
        hash_row(HASH_MACADDR_FAMILY_OID, "macaddr_ops"),
        hash_row(HASH_MACADDR8_FAMILY_OID, "macaddr8_ops"),
    ]
}

fn btree_row(oid: u32, name: &str) -> PgOpfamilyRow {
    PgOpfamilyRow {
        oid,
        opfmethod: BTREE_AM_OID,
        opfname: name.into(),
        opfnamespace: PG_CATALOG_NAMESPACE_OID,
        opfowner: BOOTSTRAP_SUPERUSER_OID,
    }
}

fn brin_row(oid: u32, name: &str) -> PgOpfamilyRow {
    PgOpfamilyRow {
        oid,
        opfmethod: BRIN_AM_OID,
        opfname: name.into(),
        opfnamespace: PG_CATALOG_NAMESPACE_OID,
        opfowner: BOOTSTRAP_SUPERUSER_OID,
    }
}

fn hash_row(oid: u32, name: &str) -> PgOpfamilyRow {
    PgOpfamilyRow {
        oid,
        opfmethod: HASH_AM_OID,
        opfname: name.into(),
        opfnamespace: PG_CATALOG_NAMESPACE_OID,
        opfowner: BOOTSTRAP_SUPERUSER_OID,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_rows_include_macaddr_opfamilies() {
        let rows = bootstrap_pg_opfamily_rows();
        for (oid, method, name) in [
            (BTREE_MACADDR_FAMILY_OID, BTREE_AM_OID, "macaddr_ops"),
            (BTREE_MACADDR8_FAMILY_OID, BTREE_AM_OID, "macaddr8_ops"),
            (
                BRIN_MACADDR_MINMAX_FAMILY_OID,
                BRIN_AM_OID,
                "macaddr_minmax_ops",
            ),
            (
                BRIN_MACADDR8_MINMAX_FAMILY_OID,
                BRIN_AM_OID,
                "macaddr8_minmax_ops",
            ),
            (
                BRIN_MACADDR_MINMAX_MULTI_FAMILY_OID,
                BRIN_AM_OID,
                "macaddr_minmax_multi_ops",
            ),
            (
                BRIN_MACADDR8_BLOOM_FAMILY_OID,
                BRIN_AM_OID,
                "macaddr8_bloom_ops",
            ),
            (HASH_MACADDR_FAMILY_OID, HASH_AM_OID, "macaddr_ops"),
            (HASH_MACADDR8_FAMILY_OID, HASH_AM_OID, "macaddr8_ops"),
        ] {
            assert!(
                rows.iter().any(|row| {
                    row.oid == oid && row.opfmethod == method && row.opfname == name
                }),
                "missing opfamily {name} ({oid})"
            );
        }
    }
}
