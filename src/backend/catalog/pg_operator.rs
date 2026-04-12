use crate::include::catalog::PgOperatorRow;

pub fn sort_pg_operator_rows(rows: &mut [PgOperatorRow]) {
    rows.sort_by(|left, right| {
        left.oprnamespace
            .cmp(&right.oprnamespace)
            .then_with(|| left.oprname.cmp(&right.oprname))
            .then_with(|| left.oprleft.cmp(&right.oprleft))
            .then_with(|| left.oprright.cmp(&right.oprright))
            .then_with(|| left.oid.cmp(&right.oid))
    });
}
