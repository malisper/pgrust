use crate::backend::catalog::catalog::column_desc;
use crate::backend::executor::RelationDesc;
use crate::backend::parser::{SqlType, SqlTypeKind};
use crate::include::catalog::bootstrap_pg_proc_rows;

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
            column_desc("aggfnoid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc(
                "aggkind",
                SqlType::new(SqlTypeKind::InternalChar),
                false,
            ),
            column_desc("aggnumdirectargs", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("aggtransfn", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("aggfinalfn", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("aggcombinefn", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("aggserialfn", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("aggdeserialfn", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("aggmtransfn", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("aggminvtransfn", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("aggmfinalfn", SqlType::new(SqlTypeKind::Oid), false),
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
        .map(|row| PgAggregateRow {
            aggfnoid: row.oid,
            aggkind: 'n',
            aggnumdirectargs: 0,
            // Builtin aggregates still execute through the existing fast path.
            // Use PostgreSQL-shaped metadata rows now so catalog lookup is shared.
            aggtransfn: row.oid,
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
            aggsortop: 0,
            aggtranstype: row.prorettype,
            aggtransspace: 0,
            aggmtranstype: 0,
            aggmtransspace: 0,
            agginitval: None,
            aggminitval: None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_rows_exist_for_builtin_aggregates() {
        let rows = bootstrap_pg_aggregate_rows();
        assert!(rows.iter().any(|row| row.aggfnoid == 6219));
        assert!(rows.iter().any(|row| row.aggfnoid == 6220));
        assert!(rows.iter().all(|row| row.aggkind == 'n'));
    }
}
