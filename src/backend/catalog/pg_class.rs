use crate::include::catalog::{BOOTSTRAP_SUPERUSER_OID, PgClassRow, relam_for_relkind};

pub fn relkind_for_plain_table() -> PgClassRow {
    PgClassRow {
        oid: 0,
        relname: String::new(),
        relnamespace: 0,
        reltype: 0,
        relowner: BOOTSTRAP_SUPERUSER_OID,
        relam: relam_for_relkind('r'),
        relfilenode: 0,
        reltablespace: 0,
        relpages: 0,
        reltuples: 0.0,
        relallvisible: 0,
        relallfrozen: 0,
        reltoastrelid: 0,
        relpersistence: 'p',
        relkind: 'r',
        relnatts: 0,
        relhassubclass: false,
        relhastriggers: false,
        relrowsecurity: false,
        relforcerowsecurity: false,
        relispartition: false,
        relfrozenxid: crate::backend::access::transam::xact::FROZEN_TRANSACTION_ID,
    }
}
