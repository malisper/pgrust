use crate::include::catalog::PgForeignServerRow;

pub fn sort_pg_foreign_server_rows(rows: &mut [PgForeignServerRow]) {
    rows.sort_by(|left, right| {
        left.srvname
            .cmp(&right.srvname)
            .then_with(|| left.oid.cmp(&right.oid))
    });
}
