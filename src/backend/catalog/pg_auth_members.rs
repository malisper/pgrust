use crate::include::catalog::PgAuthMembersRow;

pub fn sort_pg_auth_members_rows(rows: &mut [PgAuthMembersRow]) {
    rows.sort_by_key(|row| (row.oid, row.roleid, row.member, row.grantor));
}
