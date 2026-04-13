use crate::include::catalog::{BOOTSTRAP_SUPERUSER_OID, PgClassRow, relam_for_relkind};

pub fn relkind_for_plain_table() -> PgClassRow {
    PgClassRow {
        oid: 0,
        relname: String::new(),
        relnamespace: 0,
        reltype: 0,
        relowner: BOOTSTRAP_SUPERUSER_OID,
        relam: relam_for_relkind('r'),
        reltablespace: 0,
        relfilenode: 0,
        reltoastrelid: 0,
        relpersistence: 'p',
        relkind: 'r',
        relnatts: 0,
    }
}
