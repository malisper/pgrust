//! Seam declarations for the `backend-utils-cache-relcache` unit
//! (`utils/cache/relcache.c`), which owns relcache entries (`RelationData`)
//! and therefore all field reads on an open `Relation`.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Open relations cross as their `Oid`; the
//! relcache resolves the OID back to the live entry.

seam_core::seam!(
    /// `relation->rd_att` (`RelationGetDescr(relation)`, utils/rel.h): the
    /// tuple descriptor of an open relation, cloned out of the relcache entry.
    pub fn relation_rd_att(
        relation: types_core::primitive::Oid,
    ) -> types_tuple::heaptuple::TupleDesc
);
