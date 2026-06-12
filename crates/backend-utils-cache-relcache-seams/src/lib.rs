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

seam_core::seam!(
    /// `relation->rd_rel->relkind` — the relation kind (`RELKIND_*` char) of
    /// an open relation. Pure field read; cannot `ereport`.
    pub fn relation_relkind(relation: types_core::primitive::Oid) -> u8
);

seam_core::seam!(
    /// `RelationGetRelationName(relation)` (utils/rel.h) —
    /// `NameStr(relation->rd_rel->relname)` as an owned string. `Err`
    /// carries OOM from the copy.
    pub fn relation_name(
        relation: types_core::primitive::Oid,
    ) -> types_error::PgResult<std::string::String>
);

seam_core::seam!(
    /// `relation->rd_tableam` — the relation's table-access-method vtable
    /// (`None` for relations without one: views, foreign tables,
    /// partitioned tables/indexes). Pure field read; cannot `ereport`.
    pub fn relation_rd_tableam(
        relation: types_core::primitive::Oid,
    ) -> Option<types_tableam::TableAmRoutine>
);

seam_core::seam!(
    /// `relation->rd_locator` — the relation's physical identity. Pure
    /// field read; cannot `ereport`.
    pub fn relation_rd_locator(
        relation: types_core::primitive::Oid,
    ) -> types_storage::RelFileLocator
);

seam_core::seam!(
    /// `relation->rd_backend` — the `ProcNumber` of the owning backend for
    /// temp relations (`INVALID_PROC_NUMBER` otherwise). Pure field read;
    /// cannot `ereport`.
    pub fn relation_rd_backend(
        relation: types_core::primitive::Oid,
    ) -> types_core::primitive::ProcNumber
);

seam_core::seam!(
    /// `relation->rd_rel->relpersistence` — the `RELPERSISTENCE_*` char.
    /// Pure field read; cannot `ereport`.
    pub fn relation_relpersistence(relation: types_core::primitive::Oid) -> u8
);

seam_core::seam!(
    /// `relation->rd_rel->relpages` — pg_class page-count estimate. Pure
    /// field read; cannot `ereport`.
    pub fn relation_relpages(relation: types_core::primitive::Oid) -> i32
);

seam_core::seam!(
    /// `relation->rd_rel->reltuples` — pg_class row-count estimate (a C
    /// `float4`; negative means never vacuumed). Pure field read; cannot
    /// `ereport`.
    pub fn relation_reltuples(relation: types_core::primitive::Oid) -> f32
);

seam_core::seam!(
    /// `relation->rd_rel->relallvisible` — pg_class all-visible page count.
    /// Pure field read; cannot `ereport`.
    pub fn relation_relallvisible(relation: types_core::primitive::Oid) -> i32
);

seam_core::seam!(
    /// `relation->rd_rel->relhassubclass` — does the relation have (or once
    /// have had) inheritance children? Pure field read; cannot `ereport`.
    pub fn relation_relhassubclass(relation: types_core::primitive::Oid) -> bool
);

seam_core::seam!(
    /// `RelationGetFillFactor(relation, defaultff)` (utils/rel.h) — the
    /// `fillfactor` reloption from `rd_options`, or `defaultff` when unset.
    /// Pure field read; cannot `ereport`.
    pub fn relation_get_fillfactor(
        relation: types_core::primitive::Oid,
        defaultff: i32,
    ) -> i32
);
