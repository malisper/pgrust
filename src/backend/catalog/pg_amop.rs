use crate::include::catalog::PgAmopRow;

pub fn sort_pg_amop_rows(rows: &mut [PgAmopRow]) {
    rows.sort_by_key(|row| (row.amopfamily, row.amopstrategy, row.amopopr));
}
