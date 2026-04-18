use crate::include::catalog::PgTriggerRow;

pub fn sort_pg_trigger_rows(rows: &mut [PgTriggerRow]) {
    rows.sort_by_key(|row| (row.tgrelid, row.tgname.clone(), row.oid))
}
