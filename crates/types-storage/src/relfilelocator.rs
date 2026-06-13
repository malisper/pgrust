//! `RelFileLocator` (`storage/relfilelocator.h`) — the physical identity of a
//! relation: tablespace, database, and relation file number.

use types_core::{Oid, RelFileNumber};

/// `RelFileLocator` (`storage/relfilelocator.h`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
#[allow(non_snake_case)]
pub struct RelFileLocator {
    /// tablespace
    pub spcOid: Oid,
    /// database
    pub dbOid: Oid,
    /// relation
    pub relNumber: RelFileNumber,
}
