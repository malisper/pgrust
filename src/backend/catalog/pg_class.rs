use crate::include::catalog::{PgClassRow, relam_for_relkind};

pub fn relkind_for_plain_table() -> PgClassRow {
    PgClassRow {
        oid: 0,
        relname: String::new(),
        relnamespace: 0,
        reltype: 0,
        relam: relam_for_relkind('r'),
        relfilenode: 0,
        relpersistence: 'p',
        relkind: 'r',
    }
}
