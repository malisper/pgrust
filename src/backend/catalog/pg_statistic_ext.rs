use crate::include::catalog::{PgStatisticExtDataRow, PgStatisticExtRow};

pub fn sort_pg_statistic_ext_rows(rows: &mut [PgStatisticExtRow]) {
    rows.sort_by(|left, right| {
        left.stxrelid
            .cmp(&right.stxrelid)
            .then_with(|| left.oid.cmp(&right.oid))
    });
}

pub fn sort_pg_statistic_ext_data_rows(rows: &mut [PgStatisticExtDataRow]) {
    rows.sort_by(|left, right| {
        left.stxoid
            .cmp(&right.stxoid)
            .then_with(|| left.stxdinherit.cmp(&right.stxdinherit))
    });
}
