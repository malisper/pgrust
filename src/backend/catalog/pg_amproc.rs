use crate::include::catalog::PgAmprocRow;

pub fn sort_pg_amproc_rows(rows: &mut [PgAmprocRow]) {
    rows.sort_by_key(|row| (row.amprocfamily, row.amprocnum, row.amproc));
}
