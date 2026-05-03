use pgrust_catalog_data::PgAmRow;

pub fn sort_pg_am_rows(rows: &mut [PgAmRow]) {
    rows.sort_by_key(|row| (row.oid, row.amname.clone()));
}
