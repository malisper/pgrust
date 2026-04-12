use crate::include::catalog::PgAuthIdRow;

pub fn sort_pg_authid_rows(rows: &mut [PgAuthIdRow]) {
    rows.sort_by_key(|row| (row.oid, row.rolname.clone()));
}
