//! Seam declarations for the `backend-utils-cache-relcache` unit
//! (`utils/cache/relcache.c`), which owns relcache entries and therefore all
//! field reads on an open `Relation` that the trimmed
//! [`types_rel::rel::RelationData`] does not yet carry.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. Open relations cross as `&RelationData`; the
//! relcache resolves `rd_id` back to the live entry for state the struct does
//! not yet hold.

use types_rel::rel::RelationData;

seam_core::seam!(
    /// `relation->rd_att` (`RelationGetDescr(relation)`, utils/rel.h): the
    /// tuple descriptor of an open relation, cloned out of the relcache entry
    /// into `mcx` (the relcache entry itself lives in `CacheMemoryContext`;
    /// the safe port copies rather than lending a borrow across the seam).
    /// `Err` carries OOM from the copy.
    pub fn relation_rd_att<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        relation: &RelationData,
    ) -> types_error::PgResult<types_tuple::heaptuple::TupleDesc<'mcx>>
);

seam_core::seam!(
    /// `RelationGetForm(relation)->relispartition` (utils/rel.h): is the open
    /// relation a partition? (`Err` if the relcache entry cannot be
    /// resolved.)
    pub fn relation_is_partition(relation: &RelationData) -> types_error::PgResult<bool>
);
