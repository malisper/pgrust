use pgrust_catalog_data::PgInheritsRow;

pub fn sort_pg_inherits_rows(rows: &mut [PgInheritsRow]) {
    pgrust_catalog_data::sort_pg_inherits_rows(rows);
}
