use pgrust_catalog_data::PgTypeRow;

pub fn is_composite_type(row: &PgTypeRow) -> bool {
    row.typrelid != 0
}
