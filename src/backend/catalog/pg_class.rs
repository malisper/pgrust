use crate::include::catalog::PgClassRow;

pub fn relkind_for_plain_table() -> PgClassRow {
    PgClassRow {
        oid: 0,
        relname: String::new(),
        relnamespace: 0,
        reltype: 0,
        relfilenode: 0,
        relkind: 'r',
    }
}
