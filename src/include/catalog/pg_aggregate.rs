use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::{
    BOOL_TYPE_OID, TEXT_TYPE_OID, aggregate_transition_proc_oid, bootstrap_pg_proc_rows,
    builtin_hypothetical_aggregate_function_for_proc_oid,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgAggregateRow {
    pub aggfnoid: u32,
    pub aggkind: char,
    pub aggnumdirectargs: i16,
    pub aggtransfn: u32,
    pub aggfinalfn: u32,
    pub aggcombinefn: u32,
    pub aggserialfn: u32,
    pub aggdeserialfn: u32,
    pub aggmtransfn: u32,
    pub aggminvtransfn: u32,
    pub aggmfinalfn: u32,
    pub aggfinalextra: bool,
    pub aggmfinalextra: bool,
    pub aggfinalmodify: char,
    pub aggmfinalmodify: char,
    pub aggsortop: u32,
    pub aggtranstype: u32,
    pub aggtransspace: i32,
    pub aggmtranstype: u32,
    pub aggmtransspace: i32,
    pub agginitval: Option<String>,
    pub aggminitval: Option<String>,
}

pub fn pg_aggregate_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("aggfnoid", SqlType::new(SqlTypeKind::RegProc), false),
            column_desc("aggkind", SqlType::new(SqlTypeKind::InternalChar), false),
            column_desc("aggnumdirectargs", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("aggtransfn", SqlType::new(SqlTypeKind::RegProc), false),
            column_desc("aggfinalfn", SqlType::new(SqlTypeKind::RegProc), false),
            column_desc("aggcombinefn", SqlType::new(SqlTypeKind::RegProc), false),
            column_desc("aggserialfn", SqlType::new(SqlTypeKind::RegProc), false),
            column_desc("aggdeserialfn", SqlType::new(SqlTypeKind::RegProc), false),
            column_desc("aggmtransfn", SqlType::new(SqlTypeKind::RegProc), false),
            column_desc("aggminvtransfn", SqlType::new(SqlTypeKind::RegProc), false),
            column_desc("aggmfinalfn", SqlType::new(SqlTypeKind::RegProc), false),
            column_desc("aggfinalextra", SqlType::new(SqlTypeKind::Bool), false),
            column_desc("aggmfinalextra", SqlType::new(SqlTypeKind::Bool), false),
            column_desc(
                "aggfinalmodify",
                SqlType::new(SqlTypeKind::InternalChar),
                false,
            ),
            column_desc(
                "aggmfinalmodify",
                SqlType::new(SqlTypeKind::InternalChar),
                false,
            ),
            column_desc("aggsortop", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("aggtranstype", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("aggtransspace", SqlType::new(SqlTypeKind::Int4), false),
            column_desc("aggmtranstype", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("aggmtransspace", SqlType::new(SqlTypeKind::Int4), false),
            column_desc("agginitval", SqlType::new(SqlTypeKind::Text), true),
            column_desc("aggminitval", SqlType::new(SqlTypeKind::Text), true),
        ],
    }
}

pub fn bootstrap_pg_aggregate_rows() -> Vec<PgAggregateRow> {
    bootstrap_pg_proc_rows()
        .into_iter()
        .filter(|row| row.prokind == 'a')
        .map(|row| {
            let aggsortop = aggregate_sort_operator_oid(&row.proname, &row.proargtypes);
            PgAggregateRow {
                aggfnoid: row.oid,
                aggkind: if builtin_hypothetical_aggregate_function_for_proc_oid(row.oid).is_some()
                {
                    'h'
                } else {
                    'n'
                },
                aggnumdirectargs: if builtin_hypothetical_aggregate_function_for_proc_oid(row.oid)
                    .is_some()
                {
                    1
                } else {
                    0
                },
                // Builtin aggregates still execute through the existing fast path.
                // Use catalog-only transition functions so opr_sanity can validate
                // PostgreSQL-shaped aggregate metadata without changing execution.
                aggtransfn: aggregate_transition_proc_oid(row.oid),
                aggfinalfn: 0,
                aggcombinefn: 0,
                aggserialfn: 0,
                aggdeserialfn: 0,
                aggmtransfn: 0,
                aggminvtransfn: 0,
                aggmfinalfn: 0,
                aggfinalextra: false,
                aggmfinalextra: false,
                aggfinalmodify: 'r',
                aggmfinalmodify: 'r',
                aggsortop,
                aggtranstype: row.prorettype,
                aggtransspace: 0,
                aggmtranstype: 0,
                aggmtransspace: 0,
                agginitval: None,
                aggminitval: None,
            }
        })
        .collect()
}

fn aggregate_sort_operator_oid(proname: &str, proargtypes: &str) -> u32 {
    match (proname, proargtypes) {
        ("bool_and" | "every", args) if proargtypes_eq(args, &[BOOL_TYPE_OID]) => 58,
        ("bool_or", args) if proargtypes_eq(args, &[BOOL_TYPE_OID]) => 59,
        ("min", args) if proargtypes_eq(args, &[TEXT_TYPE_OID]) => 664,
        ("max", args) if proargtypes_eq(args, &[TEXT_TYPE_OID]) => 666,
        _ => 0,
    }
}

fn proargtypes_eq(proargtypes: &str, arg_oids: &[u32]) -> bool {
    let parsed = proargtypes
        .split_whitespace()
        .map(str::parse::<u32>)
        .collect::<Result<Vec<_>, _>>();
    matches!(parsed, Ok(parsed) if parsed == arg_oids)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pg_aggregate_fnoid_is_regproc() {
        let desc = pg_aggregate_desc();
        assert_eq!(desc.columns[0].name, "aggfnoid");
        assert_eq!(desc.columns[0].sql_type.kind, SqlTypeKind::RegProc);
    }

    #[test]
    fn bootstrap_rows_exist_for_builtin_aggregates() {
        let rows = bootstrap_pg_aggregate_rows();
        assert!(rows.iter().any(|row| row.aggfnoid == 6219));
        assert!(rows.iter().any(|row| row.aggfnoid == 6220));
        assert!(rows.iter().any(|row| {
            row.aggfnoid == 3986 && row.aggkind == 'h' && row.aggnumdirectargs == 1
        }));
        assert!(rows.iter().all(|row| matches!(row.aggkind, 'n' | 'h')));
    }
}
