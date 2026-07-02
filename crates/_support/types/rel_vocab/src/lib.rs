//! Interim vocabulary; absorbed by types_storage / types_rel when those land.

#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]

use types_core::Oid;

pub type LOCKMODE = i32;

pub const NoLock: LOCKMODE = 0;
pub const AccessShareLock: LOCKMODE = 1;
pub const RowShareLock: LOCKMODE = 2;
pub const RowExclusiveLock: LOCKMODE = 3;
pub const ShareUpdateExclusiveLock: LOCKMODE = 4;
pub const ShareLock: LOCKMODE = 5;
pub const ShareRowExclusiveLock: LOCKMODE = 6;
pub const ExclusiveLock: LOCKMODE = 7;
pub const AccessExclusiveLock: LOCKMODE = 8;
pub const MaxLockmode: LOCKMODE = AccessExclusiveLock;

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

pub const RELPERSISTENCE_PERMANENT: u8 = b'p';
pub const RELPERSISTENCE_UNLOGGED: u8 = b'u';
pub const RELPERSISTENCE_TEMP: u8 = b't';

#[derive(Debug)]
pub struct FormData_pg_class<'mcx> {
    pub relname: &'mcx str,
    pub relkind: u8,
}

#[derive(Debug)]
pub struct RelationData<'mcx> {
    pub rd_id: Oid,
    pub rd_rel: FormData_pg_class<'mcx>,
}

impl<'mcx> RelationData<'mcx> {
    pub fn name(&self) -> &str {
        self.rd_rel.relname
    }
}

/// Non-Copy open handle; release goes through `relation_close`, never drop.
#[derive(Debug)]
pub struct Relation<'mcx> {
    pub rd: &'mcx RelationData<'mcx>,
}

impl<'mcx> core::ops::Deref for Relation<'mcx> {
    type Target = RelationData<'mcx>;

    fn deref(&self) -> &Self::Target {
        self.rd
    }
}

#[derive(Debug)]
pub struct RangeVar<'a> {
    pub catalogname: Option<&'a str>,
    pub schemaname: Option<&'a str>,
    pub relname: &'a str,
    pub inh: bool,
    pub relpersistence: u8,
    pub location: i32,
}

const _: () = assert!(!core::mem::needs_drop::<RelationData<'_>>());
