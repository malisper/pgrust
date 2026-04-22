use crate::include::catalog::PgPartitionedTableRow;

pub fn sort_pg_partitioned_table_rows(rows: &mut [PgPartitionedTableRow]) {
    crate::include::catalog::sort_pg_partitioned_table_rows(rows);
}
