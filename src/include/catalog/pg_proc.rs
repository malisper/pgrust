use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{
    BOOTSTRAP_SUPERUSER_OID, FLOAT8_TYPE_OID, INT4_TYPE_OID, INT8_TYPE_OID, JSONB_TYPE_OID,
    JSON_TYPE_OID, NUMERIC_TYPE_OID, PG_CATALOG_NAMESPACE_OID, TEXT_TYPE_OID,
};

pub const INTERNAL_LANGUAGE_OID: u32 = 12;

#[derive(Debug, Clone, PartialEq)]
pub struct PgProcRow {
    pub oid: u32,
    pub proname: String,
    pub pronamespace: u32,
    pub proowner: u32,
    pub prolang: u32,
    pub procost: f64,
    pub prorows: f64,
    pub provariadic: u32,
    pub prosupport: u32,
    pub prokind: char,
    pub prosecdef: bool,
    pub proleakproof: bool,
    pub proisstrict: bool,
    pub proretset: bool,
    pub provolatile: char,
    pub proparallel: char,
    pub pronargs: i16,
    pub pronargdefaults: i16,
    pub prorettype: u32,
    pub proargtypes: String,
    pub prosrc: String,
}

impl Eq for PgProcRow {}

pub fn pg_proc_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("oid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("proname", SqlType::new(SqlTypeKind::Text), false),
            column_desc("pronamespace", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("proowner", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("prolang", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("procost", SqlType::new(SqlTypeKind::Float4), false),
            column_desc("prorows", SqlType::new(SqlTypeKind::Float4), false),
            column_desc("provariadic", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("prosupport", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("prokind", SqlType::new(SqlTypeKind::InternalChar), false),
            column_desc("prosecdef", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("proleakproof", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("proisstrict", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("proretset", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("provolatile", SqlType::new(SqlTypeKind::InternalChar), false),
            column_desc("proparallel", SqlType::new(SqlTypeKind::InternalChar), false),
            column_desc("pronargs", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("pronargdefaults", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("prorettype", SqlType::new(SqlTypeKind::Oid), false),
            // :HACK: PostgreSQL stores `proargtypes` as oidvector. Until pgrust has
            // oidvector and pseudo-type catalog support, keep a textified signature.
            column_desc("proargtypes", SqlType::new(SqlTypeKind::Text), false),
            column_desc("prosrc", SqlType::new(SqlTypeKind::Text), false),
        ],
    }
}

pub fn bootstrap_pg_proc_rows() -> Vec<PgProcRow> {
    // :HACK: Seed a representative builtin subset before pg_proc becomes the
    // authoritative function lookup source. The current rows cover common
    // scalar, aggregate, and set-returning builtins that pgrust already
    // exposes through hardcoded binder and executor paths.
    vec![
        proc_row(6200, "random", FLOAT8_TYPE_OID, "", "random", 0, false, false, 'f', 'v'),
        proc_row(
            6201,
            "getdatabaseencoding",
            TEXT_TYPE_OID,
            "",
            "getdatabaseencoding",
            0,
            false,
            false,
            'f',
            's',
        ),
        proc_row(
            6202,
            "lower",
            TEXT_TYPE_OID,
            &oid_argtypes(&[TEXT_TYPE_OID]),
            "lower",
            1,
            false,
            true,
            'f',
            'i',
        ),
        proc_row(
            6203,
            "length",
            INT4_TYPE_OID,
            &oid_argtypes(&[TEXT_TYPE_OID]),
            "length",
            1,
            false,
            true,
            'f',
            'i',
        ),
        proc_row(
            6204,
            "md5",
            TEXT_TYPE_OID,
            &oid_argtypes(&[TEXT_TYPE_OID]),
            "md5",
            1,
            false,
            true,
            'f',
            'i',
        ),
        proc_row(
            6205,
            "abs",
            NUMERIC_TYPE_OID,
            &oid_argtypes(&[NUMERIC_TYPE_OID]),
            "abs",
            1,
            false,
            true,
            'f',
            'i',
        ),
        proc_row(
            6206,
            "log",
            FLOAT8_TYPE_OID,
            &oid_argtypes(&[FLOAT8_TYPE_OID]),
            "log",
            1,
            false,
            true,
            'f',
            'i',
        ),
        proc_row(
            6207,
            "log10",
            FLOAT8_TYPE_OID,
            &oid_argtypes(&[FLOAT8_TYPE_OID]),
            "log10",
            1,
            false,
            true,
            'f',
            'i',
        ),
        proc_row(
            6208,
            "round",
            NUMERIC_TYPE_OID,
            &oid_argtypes(&[NUMERIC_TYPE_OID]),
            "round",
            1,
            false,
            true,
            'f',
            'i',
        ),
        proc_row(
            6209,
            "sqrt",
            FLOAT8_TYPE_OID,
            &oid_argtypes(&[FLOAT8_TYPE_OID]),
            "sqrt",
            1,
            false,
            true,
            'f',
            'i',
        ),
        proc_row(
            6210,
            "power",
            FLOAT8_TYPE_OID,
            &oid_argtypes(&[FLOAT8_TYPE_OID, FLOAT8_TYPE_OID]),
            "power",
            2,
            false,
            true,
            'f',
            'i',
        ),
        proc_row(6211, "to_json", JSON_TYPE_OID, "any", "to_json", 1, false, false, 'f', 's'),
        proc_row(
            6212,
            "to_jsonb",
            JSONB_TYPE_OID,
            "any",
            "to_jsonb",
            1,
            false,
            false,
            'f',
            's',
        ),
        proc_row(
            6213,
            "json_build_array",
            JSON_TYPE_OID,
            "variadic any",
            "json_build_array",
            1,
            false,
            false,
            'f',
            's',
        ),
        proc_row(
            6214,
            "json_build_object",
            JSON_TYPE_OID,
            "variadic any",
            "json_build_object",
            1,
            false,
            false,
            'f',
            's',
        ),
        proc_row(
            6215,
            "jsonb_build_array",
            JSONB_TYPE_OID,
            "variadic any",
            "jsonb_build_array",
            1,
            false,
            false,
            'f',
            's',
        ),
        proc_row(
            6216,
            "jsonb_build_object",
            JSONB_TYPE_OID,
            "variadic any",
            "jsonb_build_object",
            1,
            false,
            false,
            'f',
            's',
        ),
        set_returning_proc_row(
            6217,
            "json_array_elements",
            JSON_TYPE_OID,
            &oid_argtypes(&[JSON_TYPE_OID]),
            "json_array_elements",
            1,
        ),
        set_returning_proc_row(
            6218,
            "jsonb_array_elements",
            JSONB_TYPE_OID,
            &oid_argtypes(&[JSONB_TYPE_OID]),
            "jsonb_array_elements",
            1,
        ),
        aggregate_row(6219, "count", INT8_TYPE_OID, "any", 1),
        aggregate_row(6220, "sum", NUMERIC_TYPE_OID, &oid_argtypes(&[NUMERIC_TYPE_OID]), 1),
        aggregate_row(6221, "avg", NUMERIC_TYPE_OID, &oid_argtypes(&[NUMERIC_TYPE_OID]), 1),
        aggregate_row(6222, "min", TEXT_TYPE_OID, &oid_argtypes(&[TEXT_TYPE_OID]), 1),
        aggregate_row(6223, "max", TEXT_TYPE_OID, &oid_argtypes(&[TEXT_TYPE_OID]), 1),
        aggregate_row(6224, "json_agg", JSON_TYPE_OID, "any", 1),
        aggregate_row(6225, "jsonb_agg", JSONB_TYPE_OID, "any", 1),
        aggregate_row(
            6226,
            "json_object_agg",
            JSON_TYPE_OID,
            &oid_argtypes(&[TEXT_TYPE_OID, TEXT_TYPE_OID]),
            2,
        ),
        aggregate_row(
            6227,
            "jsonb_object_agg",
            JSONB_TYPE_OID,
            &oid_argtypes(&[TEXT_TYPE_OID, TEXT_TYPE_OID]),
            2,
        ),
    ]
}

fn proc_row(
    oid: u32,
    proname: &str,
    prorettype: u32,
    proargtypes: &str,
    prosrc: &str,
    pronargs: i16,
    proretset: bool,
    proisstrict: bool,
    prokind: char,
    provolatile: char,
) -> PgProcRow {
    PgProcRow {
        oid,
        proname: proname.into(),
        pronamespace: PG_CATALOG_NAMESPACE_OID,
        proowner: BOOTSTRAP_SUPERUSER_OID,
        prolang: INTERNAL_LANGUAGE_OID,
        procost: 1.0,
        prorows: if proretset { 1000.0 } else { 0.0 },
        provariadic: 0,
        prosupport: 0,
        prokind,
        prosecdef: false,
        proleakproof: false,
        proisstrict,
        proretset,
        provolatile,
        proparallel: 's',
        pronargs,
        pronargdefaults: 0,
        prorettype,
        proargtypes: proargtypes.into(),
        prosrc: prosrc.into(),
    }
}

fn set_returning_proc_row(
    oid: u32,
    proname: &str,
    prorettype: u32,
    proargtypes: &str,
    prosrc: &str,
    pronargs: i16,
) -> PgProcRow {
    proc_row(
        oid,
        proname,
        prorettype,
        proargtypes,
        prosrc,
        pronargs,
        true,
        true,
        'f',
        's',
    )
}

fn aggregate_row(
    oid: u32,
    proname: &str,
    prorettype: u32,
    proargtypes: &str,
    pronargs: i16,
) -> PgProcRow {
    proc_row(
        oid,
        proname,
        prorettype,
        proargtypes,
        proname,
        pronargs,
        false,
        false,
        'a',
        'i',
    )
}

fn oid_argtypes(arg_oids: &[u32]) -> String {
    arg_oids
        .iter()
        .map(|oid| oid.to_string())
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_proc_desc_matches_expected_columns() {
        let desc = pg_proc_desc();
        let names: Vec<_> = desc.columns.iter().map(|column| column.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "oid",
                "proname",
                "pronamespace",
                "proowner",
                "prolang",
                "procost",
                "prorows",
                "provariadic",
                "prosupport",
                "prokind",
                "prosecdef",
                "proleakproof",
                "proisstrict",
                "proretset",
                "provolatile",
                "proparallel",
                "pronargs",
                "pronargdefaults",
                "prorettype",
                "proargtypes",
                "prosrc",
            ]
        );
    }
}
