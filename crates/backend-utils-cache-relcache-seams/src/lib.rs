//! Seam declarations for the `backend-utils-cache-relcache` unit
//! (`utils/cache/relcache.c`), which owns relcache entries (`RelationData`)
//! and therefore all field reads on an open `Relation`.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Open relations cross as their `Oid`; the
//! relcache resolves the OID back to the live entry.

seam_core::seam!(
    /// `relation->rd_att` (`RelationGetDescr(relation)`, utils/rel.h): the
    /// tuple descriptor of an open relation, cloned out of the relcache entry
    /// into `mcx` (the relcache entry itself lives in `CacheMemoryContext`;
    /// the safe port copies rather than lending a borrow across the seam).
    /// `Err` carries OOM from the copy.
    pub fn relation_rd_att<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relation: types_core::primitive::Oid,
    ) -> types_error::PgResult<types_tuple::heaptuple::TupleDesc<'mcx>>
);

seam_core::seam!(
    /// `relation->rd_rel->relispopulated` (`RelationIsScannable(relation)`,
    /// utils/rel.h): is the relation populated (matviews can be not)? `Err`
    /// carries OID-resolution failure.
    pub fn relation_rd_rel_relispopulated(
        relation: types_core::primitive::Oid,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `relation->rd_rel->relispartition` (utils/rel.h): is the relation a
    /// partition? `Err` carries OID-resolution failure.
    pub fn relation_rd_rel_relispartition(
        relation: types_core::primitive::Oid,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `RelationGetRelationName(relation)` (utils/rel.h): the relation's
    /// name, copied out of the relcache entry. `Err` carries OID-resolution
    /// failure.
    pub fn relation_get_relation_name(
        relation: types_core::primitive::Oid,
    ) -> types_error::PgResult<std::string::String>
);
