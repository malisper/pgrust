use pgrust_catalog_data::PgForeignDataWrapperRow;

pub fn sort_pg_foreign_data_wrapper_rows(rows: &mut [PgForeignDataWrapperRow]) {
    rows.sort_by(|left, right| {
        left.fdwname
            .cmp(&right.fdwname)
            .then_with(|| left.oid.cmp(&right.oid))
    });
}
