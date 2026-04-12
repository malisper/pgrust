use crate::include::catalog::PgDatabaseRow;

pub fn sort_pg_database_rows(rows: &mut [PgDatabaseRow]) {
    rows.sort_by_key(|row| (row.oid, row.datname.clone()));
}
