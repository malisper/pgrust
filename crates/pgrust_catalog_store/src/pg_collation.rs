use pgrust_catalog_data::PgCollationRow;

pub fn sort_pg_collation_rows(rows: &mut [PgCollationRow]) {
    rows.sort_by_key(|row| (row.oid, row.collname.clone()));
}
