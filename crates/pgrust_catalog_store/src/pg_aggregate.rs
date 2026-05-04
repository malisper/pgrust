use pgrust_catalog_data::PgAggregateRow;

pub fn sort_pg_aggregate_rows(rows: &mut [PgAggregateRow]) {
    rows.sort_by_key(|row| row.aggfnoid);
}
