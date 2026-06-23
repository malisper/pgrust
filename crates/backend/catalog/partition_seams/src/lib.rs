//! Seam declarations for the `backend-catalog-partition` unit
//! (`catalog/partition.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgVec};
use ::types_core::primitive::Oid;
use ::types_error::PgResult;
use ::nodes::nodes::Node;
use ::pathnodes::Bitmapset;
use rel::{Relation, RelationData};

seam_core::seam!(
    /// `has_partition_attrs(rel, attnums, &used_in_expr)` (catalog/partition.c):
    /// returns whether any of the columns in `attnums` (offset by
    /// `FirstLowInvalidHeapAttributeNumber`) is used in the relation's partition
    /// key, either directly or within a partition-key expression (in which case
    /// `used_in_expr` is set). Returns `false` for non-partitioned relations.
    pub fn has_partition_attrs<'mcx>(
        mcx: Mcx<'mcx>,
        rel: &Relation<'mcx>,
        attnums: Option<&Bitmapset>,
    ) -> PgResult<(bool, bool)>
);

seam_core::seam!(
    /// `get_partition_parent(relid, even_if_detached)` (catalog/partition.c):
    /// the OID of the given partition's parent table. With `even_if_detached`
    /// true (partcache passes true) a partition mid-detach still reports its
    /// parent. `Err` carries the `elog(ERROR, "could not find tuple for
    /// parent of relation %u")` and the pg_inherits scan errors.
    pub fn get_partition_parent(relid: Oid, even_if_detached: bool) -> PgResult<Oid>
);

seam_core::seam!(
    /// `map_partition_varattnos(expr, fromrel_varno, to_rel, from_rel)`
    /// (catalog/partition.c): rewrite every `Var` in the qual list `exprs` to
    /// bear `to_rel`'s attnos instead of `from_rel`'s (matching by column
    /// name), with the source varno `fromrel_varno` (partcache passes 1). The
    /// rewritten list is allocated in `mcx`. `Err` carries the
    /// `build_attrmap_by_name` mismatch errors and OOM.
    pub fn map_partition_varattnos<'mcx, 'r>(
        mcx: Mcx<'mcx>,
        exprs: PgVec<'mcx, Node<'mcx>>,
        fromrel_varno: i32,
        to_rel: &RelationData<'r>,
        from_rel: &RelationData<'r>,
    ) -> PgResult<PgVec<'mcx, Node<'mcx>>>
);

seam_core::seam!(
    /// `get_partition_ancestors(relid)` (catalog/partition.c): the list of
    /// ancestor relations of the given partition, bottom-up (immediate parent
    /// first, topmost ancestor last — `llast_oid` is the root). The list is
    /// palloc'd in the caller's current context (here: `mcx`). `Err` carries
    /// the pg_inherits scan's `ereport(ERROR)`s and OOM.
    pub fn get_partition_ancestors<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
    ) -> PgResult<PgVec<'mcx, Oid>>
);
