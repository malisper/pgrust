//! `RelationData` (`utils/rel.h`) — the open-relation / relcache-entry type.

use types_core::primitive::Oid;

/// `struct RelationData` (`utils/rel.h`) — what C's typed `Relation` pointer
/// points at. Populated incrementally with the fields ports actually consume;
/// today that is the relation OID (`RelationGetRelid`). Reads of relcache-owned
/// state the struct does not yet carry (e.g. `rd_rel->relispartition`,
/// `rd_att`) go through the relcache owner's seams, which resolve the entry
/// from `rd_id`. The `table_open`..`table_close` ownership span is the
/// [`OpenRelation` guard in `backend-access-table-table-seams`]; this struct is
/// only the relation state it lends out.
#[derive(Clone, Debug)]
pub struct RelationData {
    /// `rd_id` — the relation's OID (`RelationGetRelid(relation)`).
    pub rd_id: Oid,
}
