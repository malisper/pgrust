use pgrust_catalog_data::PgPartitionedTableRow;

pub fn sort_pg_partitioned_table_rows(rows: &mut [PgPartitionedTableRow]) {
    pgrust_catalog_data::sort_pg_partitioned_table_rows(rows);
}
