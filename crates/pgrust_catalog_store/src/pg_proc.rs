use pgrust_catalog_data::PgProcRow;

pub fn sort_pg_proc_rows(rows: &mut [PgProcRow]) {
    rows.sort_by(|left, right| {
        left.pronamespace
            .cmp(&right.pronamespace)
            .then_with(|| left.proname.cmp(&right.proname))
            .then_with(|| left.proargtypes.cmp(&right.proargtypes))
            .then_with(|| left.oid.cmp(&right.oid))
    });
}
