use pgrust_catalog_data::PgTsConfigRow;

pub fn sort_pg_ts_config_rows(rows: &mut [PgTsConfigRow]) {
    rows.sort_by(|left, right| {
        left.cfgname
            .cmp(&right.cfgname)
            .then_with(|| left.cfgnamespace.cmp(&right.cfgnamespace))
            .then_with(|| left.oid.cmp(&right.oid))
    });
}
