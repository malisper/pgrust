use crate::desc::column_desc;
use crate::{
    BOOL_TYPE_OID, INTERNAL_TYPE_OID, MODE_FINAL_PROC_OID, ORDERED_SET_TRANSITION_PROC_OID,
    PERCENTILE_CONT_FLOAT8_FINAL_PROC_OID, PERCENTILE_CONT_FLOAT8_MULTI_FINAL_PROC_OID,
    PERCENTILE_CONT_INTERVAL_FINAL_PROC_OID, PERCENTILE_CONT_INTERVAL_MULTI_FINAL_PROC_OID,
    PERCENTILE_DISC_FINAL_PROC_OID, PERCENTILE_DISC_MULTI_FINAL_PROC_OID, PgProcRow, TEXT_TYPE_OID,
    aggregate_transition_proc_oid, bootstrap_pg_proc_rows,
    builtin_hypothetical_aggregate_function_for_proc_oid,
    builtin_ordered_set_aggregate_function_for_proc_oid,
};
use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};
use pgrust_nodes::primnodes::RelationDesc;

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
        .map(|row| pg_aggregate_row_for_proc_row(&row))
        .collect()
}

pub(crate) fn pg_aggregate_row_for_proc_row(row: &PgProcRow) -> PgAggregateRow {
    let aggsortop = aggregate_sort_operator_oid(&row.proname, &row.proargtypes);
    let hypothetical = builtin_hypothetical_aggregate_function_for_proc_oid(row.oid).is_some();
    let ordered_set = builtin_ordered_set_aggregate_function_for_proc_oid(row.oid).is_some();
    PgAggregateRow {
        aggfnoid: row.oid,
        aggkind: if ordered_set {
            'o'
        } else if hypothetical {
            'h'
        } else {
            'n'
        },
        aggnumdirectargs: if ordered_set {
            ordered_set_direct_arg_count(row.oid)
        } else if hypothetical {
            1
        } else {
            0
        },
        // Builtin aggregates still execute through the existing fast path.
        // Use catalog-only transition functions so opr_sanity can validate
        // PostgreSQL-shaped aggregate metadata without changing execution.
        aggtransfn: if ordered_set {
            ORDERED_SET_TRANSITION_PROC_OID
        } else {
            aggregate_transition_proc_oid(row.oid)
        },
        aggfinalfn: if ordered_set {
            ordered_set_final_proc_oid(row.oid)
        } else {
            0
        },
        aggcombinefn: 0,
        aggserialfn: 0,
        aggdeserialfn: 0,
        aggmtransfn: 0,
        aggminvtransfn: 0,
        aggmfinalfn: 0,
        aggfinalextra: ordered_set,
        aggmfinalextra: false,
        aggfinalmodify: if ordered_set { 's' } else { 'r' },
        aggmfinalmodify: 'r',
        aggsortop,
        aggtranstype: if ordered_set {
            INTERNAL_TYPE_OID
        } else {
            row.prorettype
        },
        aggtransspace: 0,
        aggmtranstype: 0,
        aggmtransspace: 0,
        agginitval: None,
        aggminitval: None,
    }
}

fn ordered_set_direct_arg_count(proc_oid: u32) -> i16 {
    if proc_oid == crate::MODE_AGG_PROC_OID {
        0
    } else {
        1
    }
}

fn ordered_set_final_proc_oid(proc_oid: u32) -> u32 {
    match proc_oid {
        crate::PERCENTILE_DISC_AGG_PROC_OID => PERCENTILE_DISC_FINAL_PROC_OID,
        crate::PERCENTILE_DISC_MULTI_AGG_PROC_OID => PERCENTILE_DISC_MULTI_FINAL_PROC_OID,
        crate::PERCENTILE_CONT_FLOAT8_AGG_PROC_OID => PERCENTILE_CONT_FLOAT8_FINAL_PROC_OID,
        crate::PERCENTILE_CONT_INTERVAL_AGG_PROC_OID => PERCENTILE_CONT_INTERVAL_FINAL_PROC_OID,
        crate::PERCENTILE_CONT_FLOAT8_MULTI_AGG_PROC_OID => {
            PERCENTILE_CONT_FLOAT8_MULTI_FINAL_PROC_OID
        }
        crate::PERCENTILE_CONT_INTERVAL_MULTI_AGG_PROC_OID => {
            PERCENTILE_CONT_INTERVAL_MULTI_FINAL_PROC_OID
        }
        crate::MODE_AGG_PROC_OID => MODE_FINAL_PROC_OID,
        _ => 0,
    }
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
        assert!(
            rows.iter()
                .all(|row| matches!(row.aggkind, 'n' | 'h' | 'o'))
        );
    }
}
