use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{
    BIT_ARRAY_TYPE_OID, BIT_TYPE_OID, BOOL_ARRAY_TYPE_OID, BOOL_TYPE_OID, BOX_TYPE_OID,
    BPCHAR_ARRAY_TYPE_OID, BPCHAR_TYPE_OID, BYTEA_ARRAY_TYPE_OID, BYTEA_TYPE_OID,
    CAST_PROC_INT2_INT4_OID, CAST_PROC_INT2_INT8_OID, CAST_PROC_INT4_INT2_OID,
    CAST_PROC_INT4_INT8_OID, CAST_PROC_INT8_INT2_OID, CAST_PROC_INT8_INT4_OID,
    CAST_PROC_NUMERIC_INT2_OID, CAST_PROC_NUMERIC_INT4_OID, CAST_PROC_NUMERIC_INT8_OID,
    CAST_PROC_TEXT_BPCHAR_OID, CIRCLE_TYPE_OID, DATE_TYPE_OID, DATERANGE_TYPE_OID,
    FLOAT4_ARRAY_TYPE_OID, FLOAT4_TYPE_OID, FLOAT8_ARRAY_TYPE_OID, FLOAT8_TYPE_OID,
    INT2_ARRAY_TYPE_OID, INT2_TYPE_OID, INT4_ARRAY_TYPE_OID, INT4_TYPE_OID, INT4RANGE_TYPE_OID,
    INT8_ARRAY_TYPE_OID, INT8_TYPE_OID, INT8RANGE_TYPE_OID, INTERNAL_CHAR_ARRAY_TYPE_OID,
    INTERNAL_CHAR_TYPE_OID, JSON_ARRAY_TYPE_OID, JSON_TYPE_OID, JSONB_ARRAY_TYPE_OID,
    JSONB_TYPE_OID, JSONPATH_ARRAY_TYPE_OID, JSONPATH_TYPE_OID, LINE_TYPE_OID, LSEG_TYPE_OID,
    MONEY_ARRAY_TYPE_OID, MONEY_TYPE_OID, NAME_ARRAY_TYPE_OID, NAME_TYPE_OID,
    NUMERIC_ARRAY_TYPE_OID, NUMERIC_TYPE_OID, NUMRANGE_TYPE_OID, OID_ARRAY_TYPE_OID, OID_TYPE_OID,
    PATH_TYPE_OID, POINT_TYPE_OID, POLYGON_TYPE_OID, REGCONFIG_ARRAY_TYPE_OID, REGCONFIG_TYPE_OID,
    REGDICTIONARY_ARRAY_TYPE_OID, REGDICTIONARY_TYPE_OID, REGPROCEDURE_ARRAY_TYPE_OID,
    REGPROCEDURE_TYPE_OID, REGROLE_TYPE_OID, TEXT_ARRAY_TYPE_OID, TEXT_TYPE_OID,
    TIMESTAMP_ARRAY_TYPE_OID,
    TIMESTAMP_TYPE_OID, TSQUERY_ARRAY_TYPE_OID, TSQUERY_TYPE_OID, TSRANGE_TYPE_OID,
    TSTZRANGE_TYPE_OID, TSVECTOR_ARRAY_TYPE_OID, TSVECTOR_TYPE_OID, VARBIT_ARRAY_TYPE_OID,
    VARBIT_TYPE_OID, VARCHAR_ARRAY_TYPE_OID, VARCHAR_TYPE_OID,
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
        cast_row(4106_0, OID_TYPE_OID, REGROLE_TYPE_OID, 0, 'i', 'b'),
        cast_row(4106_0_1, REGROLE_TYPE_OID, OID_TYPE_OID, 0, 'a', 'b'),
        cast_row(4106_1, OID_TYPE_OID, REGPROCEDURE_TYPE_OID, 0, 'i', 'b'),
        cast_row(4106_2, REGPROCEDURE_TYPE_OID, OID_TYPE_OID, 0, 'a', 'b'),
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
        cast_row(4111, VARCHAR_TYPE_OID, TEXT_TYPE_OID, 0, 'i', 'b'),
        cast_row(
            4112,
            BPCHAR_TYPE_OID,
            TEXT_TYPE_OID,
            CAST_PROC_TEXT_BPCHAR_OID,
            'i',
            'f',
        ),
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
        JSON_TYPE_OID,
        JSONB_TYPE_OID,
        JSONPATH_TYPE_OID,
        BIT_TYPE_OID,
        VARBIT_TYPE_OID,
        TSVECTOR_TYPE_OID,
        TSQUERY_TYPE_OID,
        REGCONFIG_TYPE_OID,
        REGDICTIONARY_TYPE_OID,
        REGPROCEDURE_TYPE_OID,
        NAME_TYPE_OID,
        INTERNAL_CHAR_TYPE_OID,
        DATE_TYPE_OID,
        INT4RANGE_TYPE_OID,
        INT8RANGE_TYPE_OID,
        NUMRANGE_TYPE_OID,
        DATERANGE_TYPE_OID,
        TSRANGE_TYPE_OID,
        TSTZRANGE_TYPE_OID,
        BPCHAR_TYPE_OID,
        VARCHAR_TYPE_OID,
        TIMESTAMP_TYPE_OID,
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
        .map(|(idx, target)| cast_row(first_oid + idx as u32, TEXT_TYPE_OID, target, 0, 'e', 'i'))
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
            cast_row(first_oid + idx as u32, source, target, 0, context, 'f')
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
        JSON_ARRAY_TYPE_OID,
        JSONB_ARRAY_TYPE_OID,
        JSONPATH_ARRAY_TYPE_OID,
        BIT_ARRAY_TYPE_OID,
        VARBIT_ARRAY_TYPE_OID,
        TSVECTOR_ARRAY_TYPE_OID,
        TSQUERY_ARRAY_TYPE_OID,
        REGCONFIG_ARRAY_TYPE_OID,
        REGDICTIONARY_ARRAY_TYPE_OID,
        REGPROCEDURE_ARRAY_TYPE_OID,
        NAME_ARRAY_TYPE_OID,
        INTERNAL_CHAR_ARRAY_TYPE_OID,
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
                && row.casttarget == NAME_TYPE_OID
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
    }
}
