//! Seam declarations for the `backend-utils-cache-partcache` unit
//! (`utils/cache/partcache.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! The `get_partition_natts` / `get_partition_col_attnum` /
//! `get_partition_col_typid` accessors are inline reads of the returned
//! `PartitionKeyData`, not seams — the consumer computes them in-crate once it
//! holds the key.

use mcx::{Mcx, PgBox};
use ::types_core::Oid;
use ::types_error::PgResult;
use ::nodes::nodes::Node;
use ::nodes::partition::PartitionKeyData;
use ::rel::Relation;

seam_core::seam!(
    /// `RelationGetPartitionKey(rel)` (partcache.c): the relation's partition
    /// key, built and cached in the relcache entry on first access and held
    /// live for the executor run. The owned model returns it allocated in
    /// `mcx` (C: the relcache's `rd_partkeycxt`). Fallible on the catalog
    /// read's `ereport(ERROR)`s and OOM. A non-partitioned relation is the C
    /// `NULL` return.
    pub fn relation_get_partition_key<'mcx>(
        mcx: Mcx<'mcx>,
        rel: Relation<'mcx>,
    ) -> PgResult<Option<PgBox<'mcx, PartitionKeyData<'mcx>>>>
);

seam_core::seam!(
    /// `get_partition_qual_relid(relid)` (partcache.c): an expression tree
    /// describing the relation's partition constraint, allocated in `mcx`.
    /// Returns the C `NULL` (`Ok(None)`) when the relation is not found, is not
    /// a partition, or has no partition constraint (a default partition that is
    /// the only partition). The multi-element implicit-AND list is folded into a
    /// `BoolExpr(AND_EXPR, ...)`; a single element is returned bare. Reads the
    /// catalog and opens the relation under `AccessShareLock` (kept for the
    /// caller to deparse safely), so it can `ereport(ERROR)`.
    pub fn get_partition_qual_relid<'mcx>(
        mcx: Mcx<'mcx>,
        relid: Oid,
    ) -> PgResult<Option<PgBox<'mcx, Node<'mcx>>>>
);
