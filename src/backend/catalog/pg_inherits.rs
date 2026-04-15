use crate::include::catalog::PgInheritsRow;

pub fn sort_pg_inherits_rows(rows: &mut [PgInheritsRow]) {
    crate::include::catalog::sort_pg_inherits_rows(rows);
}
