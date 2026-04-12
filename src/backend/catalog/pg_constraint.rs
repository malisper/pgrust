use crate::include::catalog::PgConstraintRow;

pub fn sort_pg_constraint_rows(rows: &mut [PgConstraintRow]) {
    rows.sort_by(|left, right| {
        left.connamespace
            .cmp(&right.connamespace)
            .then_with(|| left.conrelid.cmp(&right.conrelid))
            .then_with(|| left.conname.cmp(&right.conname))
            .then_with(|| left.oid.cmp(&right.oid))
    });
}
