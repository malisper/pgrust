use crate::include::catalog::PgForeignTableRow;

pub fn sort_pg_foreign_table_rows(rows: &mut [PgForeignTableRow]) {
    rows.sort_by(|left, right| left.ftrelid.cmp(&right.ftrelid));
}
