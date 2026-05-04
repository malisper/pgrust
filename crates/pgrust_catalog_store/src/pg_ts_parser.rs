use pgrust_catalog_data::PgTsParserRow;

pub fn sort_pg_ts_parser_rows(rows: &mut [PgTsParserRow]) {
    rows.sort_by(|left, right| {
        left.prsname
            .cmp(&right.prsname)
            .then_with(|| left.prsnamespace.cmp(&right.prsnamespace))
            .then_with(|| left.oid.cmp(&right.oid))
    });
}
