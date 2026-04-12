use crate::include::catalog::PgCastRow;

pub fn sort_pg_cast_rows(rows: &mut [PgCastRow]) {
    rows.sort_by_key(|row| (row.castsource, row.casttarget, row.oid));
}
