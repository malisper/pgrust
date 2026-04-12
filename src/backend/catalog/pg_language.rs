use crate::include::catalog::PgLanguageRow;

pub fn sort_pg_language_rows(rows: &mut [PgLanguageRow]) {
    rows.sort_by(|left, right| {
        left.lanname
            .cmp(&right.lanname)
            .then_with(|| left.oid.cmp(&right.oid))
    });
}
