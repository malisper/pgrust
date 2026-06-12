//! `RelFileLocator` (`storage/relfilelocator.h`) — the physical identity of a
//! relation file: tablespace, database, and relfilenumber.

use types_core::primitive::{Oid, RelFileNumber};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct RelFileLocator {
    pub spc_oid: Oid,
    pub db_oid: Oid,
    pub rel_number: RelFileNumber,
}

impl RelFileLocator {
    pub const fn new(spc_oid: Oid, db_oid: Oid, rel_number: RelFileNumber) -> Self {
        Self {
            spc_oid,
            db_oid,
            rel_number,
        }
    }
}
