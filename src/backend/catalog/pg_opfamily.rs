use crate::include::catalog::PgOpfamilyRow;

pub fn sort_pg_opfamily_rows(rows: &mut [PgOpfamilyRow]) {
    rows.sort_by_key(|row| (row.opfmethod, row.opfname.clone()));
}
