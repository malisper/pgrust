use ::types_core::{MultiXactId, Oid, TransactionId};
use ::types_tuple::NameData;

pub const RELKIND_RELATION: u8 = b'r';
pub const RELKIND_INDEX: u8 = b'i';
pub const RELKIND_SEQUENCE: u8 = b'S';
pub const RELKIND_TOASTVALUE: u8 = b't';
pub const RELKIND_VIEW: u8 = b'v';
pub const RELKIND_MATVIEW: u8 = b'm';
pub const RELKIND_COMPOSITE_TYPE: u8 = b'c';
pub const RELKIND_FOREIGN_TABLE: u8 = b'f';
pub const RELKIND_PARTITIONED_TABLE: u8 = b'p';
pub const RELKIND_PARTITIONED_INDEX: u8 = b'I';

pub const REPLICA_IDENTITY_DEFAULT: u8 = b'd';
pub const REPLICA_IDENTITY_NOTHING: u8 = b'n';
pub const REPLICA_IDENTITY_FULL: u8 = b'f';
pub const REPLICA_IDENTITY_INDEX: u8 = b'i';

#[inline]
pub const fn RELKIND_HAS_STORAGE(relkind: u8) -> bool {
    matches!(
        relkind,
        RELKIND_RELATION | RELKIND_INDEX | RELKIND_SEQUENCE | RELKIND_TOASTVALUE | RELKIND_MATVIEW
    )
}

#[inline]
pub const fn RELKIND_HAS_PARTITIONS(relkind: u8) -> bool {
    matches!(relkind, RELKIND_PARTITIONED_TABLE | RELKIND_PARTITIONED_INDEX)
}

#[inline]
pub const fn RELKIND_HAS_TABLESPACE(relkind: u8) -> bool {
    (RELKIND_HAS_STORAGE(relkind) || RELKIND_HAS_PARTITIONS(relkind))
        && relkind != RELKIND_SEQUENCE
}

#[inline]
pub const fn RELKIND_HAS_TABLE_AM(relkind: u8) -> bool {
    matches!(relkind, RELKIND_RELATION | RELKIND_TOASTVALUE | RELKIND_MATVIEW)
}

// FormData_pg_class trimmed to the fields ports consume (the decode-once
// rd_rel projection of a relcache entry).
#[derive(Clone, Debug)]
pub struct FormData_pg_class {
    pub relname: NameData,
    pub relnamespace: Oid,
    pub reltype: Oid,
    pub relowner: Oid,
    pub relam: Oid,
    pub relfilenode: Oid,
    pub reltablespace: Oid,
    pub relpages: i32,
    pub reltuples: f32,
    pub relallvisible: i32,
    pub reltoastrelid: Oid,
    pub relhasindex: bool,
    pub relisshared: bool,
    pub relpersistence: u8,
    pub relkind: u8,
    pub relhassubclass: bool,
    pub relrowsecurity: bool,
    pub relispopulated: bool,
    pub relreplident: u8,
    pub relispartition: bool,
    pub relfrozenxid: TransactionId,
    pub relminmxid: MultiXactId,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relkind_predicates_match_pg_class_h() {
        for (kind, storage, table_am) in [
            (RELKIND_RELATION, true, true),
            (RELKIND_INDEX, true, false),
            (RELKIND_SEQUENCE, true, false),
            (RELKIND_TOASTVALUE, true, true),
            (RELKIND_VIEW, false, false),
            (RELKIND_MATVIEW, true, true),
            (RELKIND_COMPOSITE_TYPE, false, false),
            (RELKIND_FOREIGN_TABLE, false, false),
            (RELKIND_PARTITIONED_TABLE, false, false),
            (RELKIND_PARTITIONED_INDEX, false, false),
        ] {
            assert_eq!(RELKIND_HAS_STORAGE(kind), storage, "kind {}", kind as char);
            assert_eq!(RELKIND_HAS_TABLE_AM(kind), table_am, "kind {}", kind as char);
        }
        assert!(!RELKIND_HAS_TABLESPACE(RELKIND_SEQUENCE));
        assert!(RELKIND_HAS_TABLESPACE(RELKIND_PARTITIONED_TABLE));
        assert!(RELKIND_HAS_TABLESPACE(RELKIND_RELATION));
    }
}
