use crate::include::catalog::PgIndexRow;

pub fn sort_pg_index_rows(rows: &mut [PgIndexRow]) {
    rows.sort_by_key(|row| (row.indrelid, row.indexrelid));
}
