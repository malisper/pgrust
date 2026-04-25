use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{
    ANYOID, BIT_CMP_EQ_PROC_OID, BIT_TYPE_OID, BOOL_CMP_EQ_PROC_OID, BOOL_TYPE_OID, BOX_TYPE_OID,
    BPCHAR_TYPE_OID, BRIN_BIT_MINMAX_FAMILY_OID, BRIN_BPCHAR_MINMAX_FAMILY_OID,
    BRIN_BYTEA_MINMAX_FAMILY_OID, BRIN_CHAR_MINMAX_FAMILY_OID, BRIN_DATETIME_MINMAX_FAMILY_OID,
    BRIN_FLOAT_MINMAX_FAMILY_OID, BRIN_INTEGER_MINMAX_FAMILY_OID, BRIN_MINMAX_ADD_VALUE_PROC_OID,
    BRIN_MINMAX_CONSISTENT_PROC_OID, BRIN_MINMAX_OPCINFO_PROC_OID, BRIN_MINMAX_UNION_PROC_OID,
    BRIN_OID_MINMAX_FAMILY_OID, BRIN_TEXT_MINMAX_FAMILY_OID, BRIN_TIME_MINMAX_FAMILY_OID,
    BRIN_TIMETZ_MINMAX_FAMILY_OID, BRIN_VARBIT_MINMAX_FAMILY_OID, BTREE_BIT_FAMILY_OID,
    BTREE_BOOL_FAMILY_OID, BTREE_BYTEA_FAMILY_OID, BTREE_INTEGER_FAMILY_OID, BTREE_TEXT_FAMILY_OID,
    BTREE_UUID_FAMILY_OID, BTREE_VARBIT_FAMILY_OID, BYTEA_CMP_EQ_PROC_OID, BYTEA_TYPE_OID,
    DATE_TYPE_OID, DATERANGE_TYPE_OID, FLOAT4_TYPE_OID, FLOAT8_TYPE_OID,
    GIN_COMPARE_JSONB_PROC_OID, GIN_CONSISTENT_JSONB_PROC_OID, GIN_EXTRACT_JSONB_PROC_OID,
    GIN_EXTRACT_JSONB_QUERY_PROC_OID, GIN_JSONB_FAMILY_OID, GIN_TRICONSISTENT_JSONB_PROC_OID,
    GIST_BOX_CONSISTENT_PROC_OID, GIST_BOX_DISTANCE_PROC_OID, GIST_BOX_FAMILY_OID,
    GIST_BOX_PENALTY_PROC_OID, GIST_BOX_PICKSPLIT_PROC_OID, GIST_BOX_SAME_PROC_OID,
    GIST_BOX_UNION_PROC_OID, GIST_POINT_CONSISTENT_PROC_OID, GIST_POINT_FAMILY_OID,
    GIST_POINT_PENALTY_PROC_OID, GIST_POINT_PICKSPLIT_PROC_OID, GIST_POINT_SAME_PROC_OID,
    GIST_POINT_UNION_PROC_OID, GIST_RANGE_FAMILY_OID, GIST_TRANSLATE_CMPTYPE_COMMON_PROC_OID,
    HASH_BOOL_FAMILY_OID, HASH_BOOL_PROC_OID, HASH_BPCHAR_FAMILY_OID, HASH_BPCHAR_PROC_OID,
    HASH_BYTEA_FAMILY_OID, HASH_BYTEA_PROC_OID, HASH_CHAR_FAMILY_OID, HASH_CHAR_PROC_OID,
    HASH_DATE_FAMILY_OID, HASH_DATE_PROC_OID, HASH_FLOAT_FAMILY_OID, HASH_FLOAT4_PROC_OID,
    HASH_FLOAT8_PROC_OID, HASH_INT2_PROC_OID, HASH_INT4_PROC_OID, HASH_INT8_PROC_OID,
    HASH_INTEGER_FAMILY_OID, HASH_NAME_PROC_OID, HASH_NUMERIC_FAMILY_OID, HASH_NUMERIC_PROC_OID,
    HASH_OID_FAMILY_OID, HASH_OID_PROC_OID, HASH_TEXT_FAMILY_OID, HASH_TEXT_PROC_OID,
    HASH_TIME_FAMILY_OID, HASH_TIME_PROC_OID, HASH_TIMESTAMP_FAMILY_OID, HASH_TIMESTAMP_PROC_OID,
    HASH_TIMESTAMPTZ_FAMILY_OID, HASH_TIMESTAMPTZ_PROC_OID, HASH_TIMETZ_FAMILY_OID,
    HASH_TIMETZ_PROC_OID, HASH_UUID_FAMILY_OID, HASH_UUID_PROC_OID, HASH_VARCHAR_PROC_OID,
    INT2_TYPE_OID, INT4_CMP_EQ_PROC_OID, INT4_TYPE_OID, INT4RANGE_TYPE_OID, INT8_TYPE_OID,
    INT8RANGE_TYPE_OID, INTERNAL_CHAR_TYPE_OID, JSONB_TYPE_OID, NAME_TYPE_OID, NUMERIC_TYPE_OID,
    NUMRANGE_TYPE_OID, OID_TYPE_OID, POLYGON_TYPE_OID, RANGE_GIST_CONSISTENT_PROC_OID,
    RANGE_GIST_PENALTY_PROC_OID, RANGE_GIST_PICKSPLIT_PROC_OID, RANGE_GIST_SAME_PROC_OID,
    RANGE_GIST_UNION_PROC_OID, RANGE_SORTSUPPORT_PROC_OID, SPG_BOX_QUAD_CHOOSE_PROC_OID,
    SPG_BOX_QUAD_CONFIG_PROC_OID, SPG_BOX_QUAD_INNER_CONSISTENT_PROC_OID,
    SPG_BOX_QUAD_LEAF_CONSISTENT_PROC_OID, SPG_BOX_QUAD_PICKSPLIT_PROC_OID, SPGIST_BOX_FAMILY_OID,
    SPGIST_POLY_FAMILY_OID, TEXT_CMP_EQ_PROC_OID, TEXT_TYPE_OID, TIME_TYPE_OID, TIMESTAMP_TYPE_OID,
    TIMESTAMPTZ_TYPE_OID, TIMETZ_TYPE_OID, TSRANGE_TYPE_OID, TSTZRANGE_TYPE_OID, UUID_CMP_PROC_OID,
    UUID_TYPE_OID, VARBIT_CMP_EQ_PROC_OID, VARBIT_TYPE_OID, VARCHAR_TYPE_OID,
    bootstrap_pg_operator_rows,
};

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
    let operators = bootstrap_pg_operator_rows();
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
        (HASH_BOOL_FAMILY_OID, BOOL_TYPE_OID, HASH_BOOL_PROC_OID),
        (HASH_INTEGER_FAMILY_OID, INT2_TYPE_OID, HASH_INT2_PROC_OID),
        (HASH_INTEGER_FAMILY_OID, INT4_TYPE_OID, HASH_INT4_PROC_OID),
        (HASH_INTEGER_FAMILY_OID, INT8_TYPE_OID, HASH_INT8_PROC_OID),
        (HASH_OID_FAMILY_OID, OID_TYPE_OID, HASH_OID_PROC_OID),
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
        BOX_TYPE_OID, SPG_BOX_QUAD_CHOOSE_PROC_OID, SPG_BOX_QUAD_CONFIG_PROC_OID,
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
}
