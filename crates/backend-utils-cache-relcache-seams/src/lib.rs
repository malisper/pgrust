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
    /// `relation->rd_rel->reltoastrelid` (utils/rel.h field read): the OID of
    /// the relation's TOAST table, or `InvalidOid` if none. `Err` carries the
    /// by-OID relcache resolution failure surface.
    pub fn relation_reltoastrelid(
        relation: types_core::primitive::Oid,
    ) -> types_error::PgResult<types_core::primitive::Oid>
);

seam_core::seam!(
    /// `RelationGetRelationName(relation)` (utils/rel.h):
    /// `relation->rd_rel->relname` as an owned string. `Err` carries the
    /// by-OID relcache resolution failure surface.
    pub fn relation_get_relation_name(
        relation: types_core::primitive::Oid,
    ) -> types_error::PgResult<std::string::String>
);

seam_core::seam!(
    /// `RelationGetToastTupleTarget(relation, defaulttarg)` (utils/rel.h):
    /// `((StdRdOptions *) relation->rd_options)->toast_tuple_target`, or
    /// `default_target` when the relation has no reloptions. `Err` carries
    /// the by-OID relcache resolution failure surface.
    pub fn relation_get_toast_tuple_target(
        relation: types_core::primitive::Oid,
        default_target: i32,
    ) -> types_error::PgResult<i32>
);
