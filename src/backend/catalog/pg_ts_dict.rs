use crate::include::catalog::PgTsDictRow;

pub fn sort_pg_ts_dict_rows(rows: &mut [PgTsDictRow]) {
    rows.sort_by(|left, right| {
        left.dictname
            .cmp(&right.dictname)
            .then_with(|| left.dictnamespace.cmp(&right.dictnamespace))
            .then_with(|| left.oid.cmp(&right.oid))
    });
}
