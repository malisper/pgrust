use pgrust_catalog_data::PgTsTemplateRow;

pub fn sort_pg_ts_template_rows(rows: &mut [PgTsTemplateRow]) {
    rows.sort_by(|left, right| {
        left.tmplname
            .cmp(&right.tmplname)
            .then_with(|| left.tmplnamespace.cmp(&right.tmplnamespace))
            .then_with(|| left.oid.cmp(&right.oid))
    });
}
