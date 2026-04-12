use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{
    BPCHAR_TYPE_OID, INT2_TYPE_OID, INT4_TYPE_OID, INT8_TYPE_OID, NUMERIC_TYPE_OID,
    OID_TYPE_OID, TEXT_TYPE_OID, VARCHAR_TYPE_OID,
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
            column_desc("castcontext", SqlType::new(SqlTypeKind::InternalChar), false),
            column_desc("castmethod", SqlType::new(SqlTypeKind::InternalChar), false),
        ],
    }
}

pub fn bootstrap_pg_cast_rows() -> [PgCastRow; 13] {
    // :HACK: Until `pg_proc` exists, function-backed casts carry `castfunc = 0`
    // even when PostgreSQL would point at a conversion function. The catalog is
    // still useful for bootstrap visibility and later parity work.
    [
        cast_row(4100, INT2_TYPE_OID, INT4_TYPE_OID, 0, 'i', 'f'),
        cast_row(4101, INT2_TYPE_OID, INT8_TYPE_OID, 0, 'i', 'f'),
        cast_row(4102, INT2_TYPE_OID, NUMERIC_TYPE_OID, 0, 'i', 'f'),
        cast_row(4103, INT4_TYPE_OID, INT2_TYPE_OID, 0, 'a', 'f'),
        cast_row(4104, INT4_TYPE_OID, INT8_TYPE_OID, 0, 'i', 'f'),
        cast_row(4105, INT4_TYPE_OID, NUMERIC_TYPE_OID, 0, 'i', 'f'),
        cast_row(4106, INT4_TYPE_OID, OID_TYPE_OID, 0, 'i', 'b'),
        cast_row(4107, INT8_TYPE_OID, INT2_TYPE_OID, 0, 'a', 'f'),
        cast_row(4108, INT8_TYPE_OID, INT4_TYPE_OID, 0, 'a', 'f'),
        cast_row(4109, INT8_TYPE_OID, NUMERIC_TYPE_OID, 0, 'i', 'f'),
        cast_row(4110, OID_TYPE_OID, INT4_TYPE_OID, 0, 'a', 'b'),
        cast_row(4111, VARCHAR_TYPE_OID, TEXT_TYPE_OID, 0, 'i', 'b'),
        cast_row(4112, BPCHAR_TYPE_OID, TEXT_TYPE_OID, 0, 'i', 'f'),
    ]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_cast_desc_matches_expected_columns() {
        let desc = pg_cast_desc();
        let names: Vec<_> = desc.columns.iter().map(|column| column.name.as_str()).collect();
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
}
