use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{
    BIT_ARRAY_TYPE_OID, BIT_TYPE_OID, BOOL_ARRAY_TYPE_OID, BOOL_TYPE_OID, BOX_TYPE_OID,
    BPCHAR_ARRAY_TYPE_OID, BPCHAR_TYPE_OID, BYTEA_ARRAY_TYPE_OID, BYTEA_TYPE_OID,
    CAST_PROC_INT2_INT4_OID, CAST_PROC_INT2_INT8_OID, CAST_PROC_INT4_INT2_OID,
    CAST_PROC_INT4_INT8_OID, CAST_PROC_INT8_INT2_OID, CAST_PROC_INT8_INT4_OID,
    CAST_PROC_NUMERIC_INT2_OID, CAST_PROC_NUMERIC_INT4_OID, CAST_PROC_NUMERIC_INT8_OID,
    CAST_PROC_TEXT_BPCHAR_OID, CIDR_ARRAY_TYPE_OID, CIDR_TYPE_OID, CIRCLE_TYPE_OID, DATE_TYPE_OID,
    DATEMULTIRANGE_TYPE_OID, DATERANGE_TYPE_OID, FLOAT4_ARRAY_TYPE_OID, FLOAT4_TYPE_OID,
    FLOAT8_ARRAY_TYPE_OID, FLOAT8_TYPE_OID, INET_ARRAY_TYPE_OID, INET_TYPE_OID,
    INT2_ARRAY_TYPE_OID, INT2_TYPE_OID, INT2VECTOR_TYPE_OID, INT4_ARRAY_TYPE_OID, INT4_TYPE_OID,
    INT4MULTIRANGE_TYPE_OID, INT4RANGE_TYPE_OID, INT8_ARRAY_TYPE_OID, INT8_TYPE_OID,
    INT8MULTIRANGE_TYPE_OID, INT8RANGE_TYPE_OID, INTERNAL_CHAR_ARRAY_TYPE_OID,
    INTERNAL_CHAR_TYPE_OID, INTERVAL_TYPE_OID, JSON_ARRAY_TYPE_OID, JSON_TYPE_OID,
    JSONB_ARRAY_TYPE_OID, JSONB_TYPE_OID, JSONPATH_ARRAY_TYPE_OID, JSONPATH_TYPE_OID,
    LINE_TYPE_OID, LSEG_TYPE_OID, MACADDR_ARRAY_TYPE_OID, MACADDR_TO_MACADDR8_PROC_OID,
    MACADDR_TYPE_OID, MACADDR8_ARRAY_TYPE_OID, MACADDR8_TO_MACADDR_PROC_OID, MACADDR8_TYPE_OID,
    MONEY_ARRAY_TYPE_OID, MONEY_TYPE_OID, NAME_ARRAY_TYPE_OID, NAME_TYPE_OID,
    NUMERIC_ARRAY_TYPE_OID, NUMERIC_TYPE_OID, NUMMULTIRANGE_TYPE_OID, NUMRANGE_TYPE_OID,
    OID_ARRAY_TYPE_OID, OID_TYPE_OID, PATH_TYPE_OID, PG_DEPENDENCIES_TYPE_OID, PG_LSN_TYPE_OID,
    PG_MCV_LIST_TYPE_OID, PG_NDISTINCT_TYPE_OID, PG_NODE_TREE_TYPE_OID, PG_SNAPSHOT_TYPE_OID,
    POINT_TYPE_OID, POLYGON_TYPE_OID, REGCLASS_TYPE_OID, REGCOLLATION_ARRAY_TYPE_OID,
    REGCOLLATION_TYPE_OID, REGCONFIG_ARRAY_TYPE_OID, REGCONFIG_TYPE_OID,
    REGDICTIONARY_ARRAY_TYPE_OID, REGDICTIONARY_TYPE_OID, REGNAMESPACE_TYPE_OID,
    REGOPER_ARRAY_TYPE_OID, REGOPER_TYPE_OID, REGOPERATOR_ARRAY_TYPE_OID, REGOPERATOR_TYPE_OID,
    REGPROC_ARRAY_TYPE_OID, REGPROC_TYPE_OID, REGPROCEDURE_ARRAY_TYPE_OID, REGPROCEDURE_TYPE_OID,
    REGROLE_TYPE_OID, REGTYPE_TYPE_OID, TEXT_ARRAY_TYPE_OID, TEXT_TYPE_OID, TID_ARRAY_TYPE_OID,
    TID_TYPE_OID, TIME_TYPE_OID, TIMESTAMP_ARRAY_TYPE_OID, TIMESTAMP_TYPE_OID,
    TIMESTAMPTZ_TYPE_OID, TIMETZ_TYPE_OID, TSMULTIRANGE_TYPE_OID, TSQUERY_ARRAY_TYPE_OID,
    TSQUERY_TYPE_OID, TSRANGE_TYPE_OID, TSTZMULTIRANGE_TYPE_OID, TSTZRANGE_TYPE_OID,
    TSVECTOR_ARRAY_TYPE_OID, TSVECTOR_TYPE_OID, TXID_SNAPSHOT_ARRAY_TYPE_OID,
    TXID_SNAPSHOT_TYPE_OID, UUID_ARRAY_TYPE_OID, UUID_TYPE_OID, VARBIT_ARRAY_TYPE_OID,
    VARBIT_TYPE_OID, VARCHAR_ARRAY_TYPE_OID, VARCHAR_TYPE_OID, XID_ARRAY_TYPE_OID, XID_TYPE_OID,
    XID8_TYPE_OID, XML_ARRAY_TYPE_OID, XML_TYPE_OID,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgCastRow {
    pub oid: u32,
    pub castsource: u32,
    pub casttarget: u32,
    pub castfunc: u32,
    pub castcontext: char,
    pub castmethod: char,
}

pub fn pg_cast_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("castsource", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("casttarget", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("castfunc", SqlType::new(SqlTypeKind::Oid), false),
            column_desc(
                "castcontext",
                SqlType::new(SqlTypeKind::InternalChar),
                false,
            ),
            column_desc("castmethod", SqlType::new(SqlTypeKind::InternalChar), false),
        ],
    }
}

pub fn bootstrap_pg_cast_rows() -> Vec<PgCastRow> {
    let mut rows = vec![
        cast_row(
            4100,
            INT2_TYPE_OID,
            INT4_TYPE_OID,
            CAST_PROC_INT4_INT2_OID,
            'i',
            'f',
        ),
        cast_row(
            4101,
            INT2_TYPE_OID,
            INT8_TYPE_OID,
            CAST_PROC_INT8_INT2_OID,
            'i',
            'f',
        ),
        cast_row(
            4102,
            INT2_TYPE_OID,
            NUMERIC_TYPE_OID,
            CAST_PROC_NUMERIC_INT2_OID,
            'i',
            'f',
        ),
        cast_row(
            4103,
            INT4_TYPE_OID,
            INT2_TYPE_OID,
            CAST_PROC_INT2_INT4_OID,
            'a',
            'f',
        ),
        cast_row(
            4104,
            INT4_TYPE_OID,
            INT8_TYPE_OID,
            CAST_PROC_INT8_INT4_OID,
            'i',
            'f',
        ),
        cast_row(
            4105,
            INT4_TYPE_OID,
            NUMERIC_TYPE_OID,
            CAST_PROC_NUMERIC_INT4_OID,
            'i',
            'f',
        ),
        cast_row(4106, INT4_TYPE_OID, OID_TYPE_OID, 0, 'i', 'b'),
        cast_row(4106_0_00, OID_TYPE_OID, REGCLASS_TYPE_OID, 0, 'i', 'b'),
        cast_row(4106_0_00_1, REGCLASS_TYPE_OID, OID_TYPE_OID, 0, 'a', 'b'),
        cast_row(4106_0_0, OID_TYPE_OID, REGTYPE_TYPE_OID, 0, 'i', 'b'),
        cast_row(4106_0_0_1, REGTYPE_TYPE_OID, OID_TYPE_OID, 0, 'a', 'b'),
        cast_row(4106_0, OID_TYPE_OID, REGROLE_TYPE_OID, 0, 'i', 'b'),
        cast_row(4106_0_1, REGROLE_TYPE_OID, OID_TYPE_OID, 0, 'a', 'b'),
        cast_row(4106_0_3, OID_TYPE_OID, REGNAMESPACE_TYPE_OID, 0, 'i', 'b'),
        cast_row(4106_0_4, REGNAMESPACE_TYPE_OID, OID_TYPE_OID, 0, 'a', 'b'),
        cast_row(4106_0_5, OID_TYPE_OID, REGPROC_TYPE_OID, 0, 'i', 'b'),
        cast_row(4106_0_6, REGPROC_TYPE_OID, OID_TYPE_OID, 0, 'a', 'b'),
        cast_row(4106_1, OID_TYPE_OID, REGPROCEDURE_TYPE_OID, 0, 'i', 'b'),
        cast_row(4106_2, REGPROCEDURE_TYPE_OID, OID_TYPE_OID, 0, 'a', 'b'),
        cast_row(4106_2_2, OID_TYPE_OID, REGOPER_TYPE_OID, 0, 'i', 'b'),
        cast_row(4106_2_3, REGOPER_TYPE_OID, OID_TYPE_OID, 0, 'a', 'b'),
        cast_row(4106_2_0, OID_TYPE_OID, REGOPERATOR_TYPE_OID, 0, 'i', 'b'),
        cast_row(4106_2_1, REGOPERATOR_TYPE_OID, OID_TYPE_OID, 0, 'a', 'b'),
        cast_row(4106_2_4, OID_TYPE_OID, REGCOLLATION_TYPE_OID, 0, 'i', 'b'),
        cast_row(4106_2_5, REGCOLLATION_TYPE_OID, OID_TYPE_OID, 0, 'a', 'b'),
        cast_row(
            4107,
            INT8_TYPE_OID,
            INT2_TYPE_OID,
            CAST_PROC_INT2_INT8_OID,
            'a',
            'f',
        ),
        cast_row(
            4108,
            INT8_TYPE_OID,
            INT4_TYPE_OID,
            CAST_PROC_INT4_INT8_OID,
            'a',
            'f',
        ),
        cast_row(
            4109,
            INT8_TYPE_OID,
            NUMERIC_TYPE_OID,
            CAST_PROC_NUMERIC_INT8_OID,
            'i',
            'f',
        ),
        cast_row(4110, OID_TYPE_OID, INT4_TYPE_OID, 0, 'a', 'b'),
        cast_row(
            4112,
            BPCHAR_TYPE_OID,
            TEXT_TYPE_OID,
            CAST_PROC_TEXT_BPCHAR_OID,
            'i',
            'f',
        ),
        cast_row(4111_0, VARCHAR_TYPE_OID, BPCHAR_TYPE_OID, 0, 'i', 'b'),
        cast_row(4111, VARCHAR_TYPE_OID, TEXT_TYPE_OID, 0, 'i', 'b'),
        cast_row(
            4112_1,
            INT2VECTOR_TYPE_OID,
            INT2_ARRAY_TYPE_OID,
            0,
            'e',
            'i',
        ),
        cast_row(
            4800,
            MACADDR_TYPE_OID,
            MACADDR8_TYPE_OID,
            MACADDR_TO_MACADDR8_PROC_OID,
            'i',
            'f',
        ),
        cast_row(
            4801,
            MACADDR8_TYPE_OID,
            MACADDR_TYPE_OID,
            MACADDR8_TO_MACADDR_PROC_OID,
            'a',
            'f',
        ),
        cast_row(69000, PG_NODE_TREE_TYPE_OID, TEXT_TYPE_OID, 0, 'i', 'b'),
        cast_row(69001, PG_NDISTINCT_TYPE_OID, BYTEA_TYPE_OID, 0, 'i', 'b'),
        cast_row(69002, PG_DEPENDENCIES_TYPE_OID, BYTEA_TYPE_OID, 0, 'i', 'b'),
        cast_row(69003, PG_MCV_LIST_TYPE_OID, BYTEA_TYPE_OID, 0, 'i', 'b'),
        cast_row(69004, CIDR_TYPE_OID, INET_TYPE_OID, 0, 'i', 'b'),
        cast_row(69005, XML_TYPE_OID, TEXT_TYPE_OID, 0, 'a', 'b'),
        cast_row(69006, XML_TYPE_OID, VARCHAR_TYPE_OID, 0, 'a', 'b'),
        cast_row(69007, XML_TYPE_OID, BPCHAR_TYPE_OID, 0, 'a', 'b'),
    ];
    let text_input_rows = text_input_cast_rows(4113);
    let geometry_rows = geometry_cast_rows(4113 + text_input_rows.len() as u32);
    let array_rows = text_input_array_cast_rows(
        4113 + text_input_rows.len() as u32 + geometry_rows.len() as u32,
    );
    rows.extend(text_input_rows);
    rows.extend(geometry_rows);
    rows.extend(array_rows);
    rows
}

const fn cast_row(
    oid: u32,
    castsource: u32,
    casttarget: u32,
    castfunc: u32,
    castcontext: char,
    castmethod: char,
) -> PgCastRow {
    PgCastRow {
        oid,
        castsource,
        casttarget,
        castfunc,
        castcontext,
        castmethod,
    }
}

fn text_input_cast_rows(first_oid: u32) -> Vec<PgCastRow> {
    let targets = [
        INT2_TYPE_OID,
        INT4_TYPE_OID,
        INT8_TYPE_OID,
        OID_TYPE_OID,
        FLOAT4_TYPE_OID,
        FLOAT8_TYPE_OID,
        NUMERIC_TYPE_OID,
        MONEY_TYPE_OID,
        BOOL_TYPE_OID,
        BYTEA_TYPE_OID,
        UUID_TYPE_OID,
        INET_TYPE_OID,
        CIDR_TYPE_OID,
        MACADDR_TYPE_OID,
        MACADDR8_TYPE_OID,
        JSON_TYPE_OID,
        JSONB_TYPE_OID,
        JSONPATH_TYPE_OID,
        XML_TYPE_OID,
        BIT_TYPE_OID,
        VARBIT_TYPE_OID,
        TSVECTOR_TYPE_OID,
        TSQUERY_TYPE_OID,
        REGPROC_TYPE_OID,
        REGCLASS_TYPE_OID,
        REGCONFIG_TYPE_OID,
        REGDICTIONARY_TYPE_OID,
        PG_LSN_TYPE_OID,
        REGTYPE_TYPE_OID,
        REGROLE_TYPE_OID,
        REGNAMESPACE_TYPE_OID,
        REGOPER_TYPE_OID,
        REGOPERATOR_TYPE_OID,
        REGPROCEDURE_TYPE_OID,
        REGCOLLATION_TYPE_OID,
        NAME_TYPE_OID,
        INTERNAL_CHAR_TYPE_OID,
        DATE_TYPE_OID,
        TID_TYPE_OID,
        XID_TYPE_OID,
        XID8_TYPE_OID,
        TXID_SNAPSHOT_TYPE_OID,
        PG_SNAPSHOT_TYPE_OID,
        INTERVAL_TYPE_OID,
        INT4RANGE_TYPE_OID,
        INT8RANGE_TYPE_OID,
        NUMRANGE_TYPE_OID,
        DATERANGE_TYPE_OID,
        TSRANGE_TYPE_OID,
        TSTZRANGE_TYPE_OID,
        INT4MULTIRANGE_TYPE_OID,
        NUMMULTIRANGE_TYPE_OID,
        TSMULTIRANGE_TYPE_OID,
        TSTZMULTIRANGE_TYPE_OID,
        DATEMULTIRANGE_TYPE_OID,
        INT8MULTIRANGE_TYPE_OID,
        BPCHAR_TYPE_OID,
        VARCHAR_TYPE_OID,
        TIME_TYPE_OID,
        TIMETZ_TYPE_OID,
        TIMESTAMP_TYPE_OID,
        TIMESTAMPTZ_TYPE_OID,
        POINT_TYPE_OID,
        LSEG_TYPE_OID,
        PATH_TYPE_OID,
        BOX_TYPE_OID,
        POLYGON_TYPE_OID,
        LINE_TYPE_OID,
        CIRCLE_TYPE_OID,
    ];
    targets
        .into_iter()
        .enumerate()
        .map(|(idx, target)| {
            let (context, method) = if matches!(target, BPCHAR_TYPE_OID | VARCHAR_TYPE_OID) {
                ('i', 'b')
            } else {
                ('e', 'i')
            };
            cast_row(
                first_oid + idx as u32,
                TEXT_TYPE_OID,
                target,
                0,
                context,
                method,
            )
        })
        .collect()
}

fn geometry_cast_rows(first_oid: u32) -> Vec<PgCastRow> {
    let casts = [
        (POINT_TYPE_OID, BOX_TYPE_OID, 'a'),
        (LSEG_TYPE_OID, POINT_TYPE_OID, 'e'),
        (PATH_TYPE_OID, POLYGON_TYPE_OID, 'a'),
        (BOX_TYPE_OID, POINT_TYPE_OID, 'e'),
        (BOX_TYPE_OID, POLYGON_TYPE_OID, 'a'),
        (POLYGON_TYPE_OID, POINT_TYPE_OID, 'e'),
        (POLYGON_TYPE_OID, PATH_TYPE_OID, 'a'),
        (POLYGON_TYPE_OID, BOX_TYPE_OID, 'e'),
        (CIRCLE_TYPE_OID, POINT_TYPE_OID, 'e'),
        (CIRCLE_TYPE_OID, BOX_TYPE_OID, 'e'),
        (CIRCLE_TYPE_OID, POLYGON_TYPE_OID, 'e'),
    ];
    casts
        .into_iter()
        .enumerate()
        .map(|(idx, (source, target, context))| {
            cast_row(first_oid + idx as u32, source, target, 0, context, 'i')
        })
        .collect()
}

fn text_input_array_cast_rows(first_oid: u32) -> Vec<PgCastRow> {
    let targets = [
        INT2_ARRAY_TYPE_OID,
        INT4_ARRAY_TYPE_OID,
        INT8_ARRAY_TYPE_OID,
        OID_ARRAY_TYPE_OID,
        FLOAT4_ARRAY_TYPE_OID,
        FLOAT8_ARRAY_TYPE_OID,
        NUMERIC_ARRAY_TYPE_OID,
        MONEY_ARRAY_TYPE_OID,
        BOOL_ARRAY_TYPE_OID,
        BYTEA_ARRAY_TYPE_OID,
        UUID_ARRAY_TYPE_OID,
        INET_ARRAY_TYPE_OID,
        CIDR_ARRAY_TYPE_OID,
        MACADDR_ARRAY_TYPE_OID,
        MACADDR8_ARRAY_TYPE_OID,
        JSON_ARRAY_TYPE_OID,
        JSONB_ARRAY_TYPE_OID,
        JSONPATH_ARRAY_TYPE_OID,
        XML_ARRAY_TYPE_OID,
        BIT_ARRAY_TYPE_OID,
        VARBIT_ARRAY_TYPE_OID,
        TSVECTOR_ARRAY_TYPE_OID,
        TSQUERY_ARRAY_TYPE_OID,
        REGCONFIG_ARRAY_TYPE_OID,
        REGDICTIONARY_ARRAY_TYPE_OID,
        REGPROC_ARRAY_TYPE_OID,
        REGPROCEDURE_ARRAY_TYPE_OID,
        REGOPER_ARRAY_TYPE_OID,
        REGOPERATOR_ARRAY_TYPE_OID,
        REGCOLLATION_ARRAY_TYPE_OID,
        NAME_ARRAY_TYPE_OID,
        INTERNAL_CHAR_ARRAY_TYPE_OID,
        TID_ARRAY_TYPE_OID,
        XID_ARRAY_TYPE_OID,
        TXID_SNAPSHOT_ARRAY_TYPE_OID,
        BPCHAR_ARRAY_TYPE_OID,
        VARCHAR_ARRAY_TYPE_OID,
        TEXT_ARRAY_TYPE_OID,
        TIMESTAMP_ARRAY_TYPE_OID,
    ];
    targets
        .into_iter()
        .enumerate()
        .map(|(idx, target)| cast_row(first_oid + idx as u32, TEXT_TYPE_OID, target, 0, 'e', 'i'))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_cast_desc_matches_expected_columns() {
        let desc = pg_cast_desc();
        let names: Vec<_> = desc
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(
            names,
            vec![
                "oid",
                "castsource",
                "casttarget",
                "castfunc",
                "castcontext",
                "castmethod",
            ]
        );
    }

    #[test]
    fn bootstrap_pg_cast_rows_include_text_input_casts() {
        let rows = bootstrap_pg_cast_rows();
        assert!(rows.iter().any(|row| {
            row.castsource == TEXT_TYPE_OID
                && row.casttarget == JSONB_TYPE_OID
                && row.castcontext == 'e'
                && row.castmethod == 'i'
        }));
        assert!(rows.iter().any(|row| {
            row.castsource == TEXT_TYPE_OID
                && row.casttarget == JSONPATH_TYPE_OID
                && row.castcontext == 'e'
                && row.castmethod == 'i'
        }));
        assert!(rows.iter().any(|row| {
            row.castsource == TEXT_TYPE_OID
                && row.casttarget == VARBIT_TYPE_OID
                && row.castcontext == 'e'
                && row.castmethod == 'i'
        }));
        assert!(rows.iter().any(|row| {
            row.castsource == TEXT_TYPE_OID
                && row.casttarget == TIME_TYPE_OID
                && row.castcontext == 'e'
                && row.castmethod == 'i'
        }));
        assert!(rows.iter().any(|row| {
            row.castsource == TEXT_TYPE_OID
                && row.casttarget == NAME_TYPE_OID
                && row.castcontext == 'e'
                && row.castmethod == 'i'
        }));
        assert!(rows.iter().any(|row| {
            row.castsource == TEXT_TYPE_OID
                && row.casttarget == REGCLASS_TYPE_OID
                && row.castcontext == 'e'
                && row.castmethod == 'i'
        }));
        assert!(rows.iter().any(|row| {
            row.castsource == TEXT_TYPE_OID
                && row.casttarget == INT4_ARRAY_TYPE_OID
                && row.castcontext == 'e'
                && row.castmethod == 'i'
        }));
        assert!(rows.iter().any(|row| {
            row.castsource == TEXT_TYPE_OID
                && row.casttarget == NAME_ARRAY_TYPE_OID
                && row.castcontext == 'e'
                && row.castmethod == 'i'
        }));
        assert!(rows.iter().any(|row| {
            row.castsource == TEXT_TYPE_OID
                && row.casttarget == JSONB_ARRAY_TYPE_OID
                && row.castcontext == 'e'
                && row.castmethod == 'i'
        }));
        assert!(rows.iter().any(|row| {
            row.castsource == TEXT_TYPE_OID
                && row.casttarget == VARCHAR_ARRAY_TYPE_OID
                && row.castcontext == 'e'
                && row.castmethod == 'i'
        }));
        assert!(rows.iter().any(|row| {
            row.castsource == TEXT_TYPE_OID
                && row.casttarget == MACADDR_TYPE_OID
                && row.castcontext == 'e'
                && row.castmethod == 'i'
        }));
        assert!(rows.iter().any(|row| {
            row.castsource == TEXT_TYPE_OID
                && row.casttarget == MACADDR8_ARRAY_TYPE_OID
                && row.castcontext == 'e'
                && row.castmethod == 'i'
        }));
    }

    #[test]
    fn bootstrap_pg_cast_rows_preserve_core_oid_and_reg_casts() {
        let rows = bootstrap_pg_cast_rows();
        assert!(rows.iter().any(|row| {
            row.castsource == INT4_TYPE_OID
                && row.casttarget == OID_TYPE_OID
                && row.castfunc == 0
                && row.castcontext == 'i'
                && row.castmethod == 'b'
        }));
        assert!(rows.iter().any(|row| {
            row.castsource == OID_TYPE_OID
                && row.casttarget == REGROLE_TYPE_OID
                && row.castfunc == 0
                && row.castcontext == 'i'
                && row.castmethod == 'b'
        }));
        assert!(rows.iter().any(|row| {
            row.castsource == OID_TYPE_OID
                && row.casttarget == REGPROCEDURE_TYPE_OID
                && row.castfunc == 0
                && row.castcontext == 'i'
                && row.castmethod == 'b'
        }));
        assert!(rows.iter().any(|row| {
            row.castsource == OID_TYPE_OID
                && row.casttarget == REGOPERATOR_TYPE_OID
                && row.castfunc == 0
                && row.castcontext == 'i'
                && row.castmethod == 'b'
        }));
    }

    #[test]
    fn bootstrap_pg_cast_rows_include_macaddr_conversions() {
        let rows = bootstrap_pg_cast_rows();
        assert!(rows.iter().any(|row| {
            row.castsource == MACADDR_TYPE_OID
                && row.casttarget == MACADDR8_TYPE_OID
                && row.castfunc == MACADDR_TO_MACADDR8_PROC_OID
                && row.castcontext == 'i'
                && row.castmethod == 'f'
        }));
        assert!(rows.iter().any(|row| {
            row.castsource == MACADDR8_TYPE_OID
                && row.casttarget == MACADDR_TYPE_OID
                && row.castfunc == MACADDR8_TO_MACADDR_PROC_OID
                && row.castcontext == 'a'
                && row.castmethod == 'f'
        }));
    }

    #[test]
    fn geometry_casts_without_proc_use_inout_method() {
        let rows = bootstrap_pg_cast_rows();
        assert!(rows.iter().any(|row| {
            row.castsource == POINT_TYPE_OID
                && row.casttarget == BOX_TYPE_OID
                && row.castfunc == 0
                && row.castmethod == 'i'
        }));
        assert!(rows.iter().all(|row| {
            !(row.castsource == POINT_TYPE_OID
                && row.casttarget == BOX_TYPE_OID
                && row.castmethod == 'f'
                && row.castfunc == 0)
        }));
    }
}
