use crate::include::catalog::PgPolicyRow;

pub fn sort_pg_policy_rows(rows: &mut [PgPolicyRow]) {
    rows.sort_by_key(|row| (row.polrelid, row.polname.clone(), row.oid))
}
