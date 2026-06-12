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
    /// `AtEOXact_RelationCache(isCommit)` — relcache cleanup at top-level
    /// transaction end.
    pub fn at_eoxact_relation_cache(is_commit: bool) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `AtEOSubXact_RelationCache(isCommit, mySubid, parentSubid)`.
    pub fn at_eosubxact_relation_cache(
        is_commit: bool,
        my_subid: types_core::SubTransactionId,
        parent_subid: types_core::SubTransactionId,
    ) -> types_error::PgResult<()>
);
