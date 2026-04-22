use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{
    ANYOID, BIT_CMP_EQ_PROC_OID, BIT_TYPE_OID, BOOL_CMP_EQ_PROC_OID, BOOL_TYPE_OID, BOX_TYPE_OID,
    BTREE_BIT_FAMILY_OID, BTREE_BOOL_FAMILY_OID, BTREE_BYTEA_FAMILY_OID, BTREE_INTEGER_FAMILY_OID,
    BTREE_TEXT_FAMILY_OID, BTREE_VARBIT_FAMILY_OID, BYTEA_CMP_EQ_PROC_OID, BYTEA_TYPE_OID,
    DATERANGE_TYPE_OID, GIST_BOX_CONSISTENT_PROC_OID, GIST_BOX_DISTANCE_PROC_OID,
    GIST_BOX_FAMILY_OID, GIST_BOX_PENALTY_PROC_OID, GIST_BOX_PICKSPLIT_PROC_OID,
    GIST_BOX_SAME_PROC_OID, GIST_BOX_UNION_PROC_OID, GIST_RANGE_FAMILY_OID,
    GIST_TRANSLATE_CMPTYPE_COMMON_PROC_OID, INT4_CMP_EQ_PROC_OID, INT4_TYPE_OID,
    INT4RANGE_TYPE_OID, INT8RANGE_TYPE_OID, NUMRANGE_TYPE_OID, RANGE_GIST_CONSISTENT_PROC_OID,
    RANGE_GIST_PENALTY_PROC_OID, RANGE_GIST_PICKSPLIT_PROC_OID, RANGE_GIST_SAME_PROC_OID,
    RANGE_GIST_UNION_PROC_OID, RANGE_SORTSUPPORT_PROC_OID, SPG_BOX_QUAD_CHOOSE_PROC_OID,
    SPG_BOX_QUAD_CONFIG_PROC_OID, SPG_BOX_QUAD_INNER_CONSISTENT_PROC_OID,
    SPG_BOX_QUAD_LEAF_CONSISTENT_PROC_OID, SPG_BOX_QUAD_PICKSPLIT_PROC_OID, SPGIST_BOX_FAMILY_OID,
    TEXT_CMP_EQ_PROC_OID, TEXT_TYPE_OID, TSRANGE_TYPE_OID, TSTZRANGE_TYPE_OID,
    VARBIT_CMP_EQ_PROC_OID, VARBIT_TYPE_OID,
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
    rows
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
