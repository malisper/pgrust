use pgrust_catalog_data::PgTablespaceRow;

pub fn sort_pg_tablespace_rows(rows: &mut [PgTablespaceRow]) {
    rows.sort_by_key(|row| (row.oid, row.spcname.clone()));
}
