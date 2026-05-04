use crate::desc::column_desc;
pub use pgrust_core::PgPartitionedTableRow;
use pgrust_nodes::parsenodes::{SqlType, SqlTypeKind};
use pgrust_nodes::primnodes::RelationDesc;

pub fn pg_partitioned_table_desc() -> RelationDesc {
    RelationDesc {
        columns: vec![
            column_desc("partrelid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("partstrat", SqlType::new(SqlTypeKind::InternalChar), false),
            column_desc("partnatts", SqlType::new(SqlTypeKind::Int2), false),
            column_desc("partdefid", SqlType::new(SqlTypeKind::Oid), false),
            column_desc("partattrs", SqlType::new(SqlTypeKind::Int2Vector), false),
            column_desc("partclass", SqlType::new(SqlTypeKind::OidVector), false),
            column_desc("partcollation", SqlType::new(SqlTypeKind::OidVector), false),
            column_desc("partexprs", SqlType::new(SqlTypeKind::PgNodeTree), true),
        ],
    }
}

pub fn bootstrap_pg_partitioned_table_rows() -> [PgPartitionedTableRow; 0] {
    []
}

pub fn sort_pg_partitioned_table_rows(rows: &mut [PgPartitionedTableRow]) {
    rows.sort_by_key(|row| row.partrelid);
}
