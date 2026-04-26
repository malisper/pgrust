use crate::include::catalog::PgUserMappingRow;

pub fn sort_pg_user_mapping_rows(rows: &mut [PgUserMappingRow]) {
    rows.sort_by(|left, right| {
        left.umserver
            .cmp(&right.umserver)
            .then_with(|| left.umuser.cmp(&right.umuser))
            .then_with(|| left.oid.cmp(&right.oid))
    });
}
