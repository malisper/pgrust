//! Seam declarations for the `backend-catalog-partition` unit
//! (`catalog/partition.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use mcx::{Mcx, PgVec};
use types_core::primitive::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `find_all_inheritors(parentrelId, lockmode, numparents)`
    /// (catalog/pg_inherits.c): every member of the inheritance set rooted at
    /// `parentrel`, including the root itself, in breadth-first order, taking
    /// `lockmode` on each. The list is palloc'd in the caller's current
    /// context (`mcx`). `Err` carries the pg_inherits scan / lock
    /// `ereport(ERROR)`s and OOM.
    pub fn find_all_inheritors<'mcx>(
        mcx: Mcx<'mcx>,
        parentrel: Oid,
        lockmode: types_storage::lock::LOCKMODE,
    ) -> PgResult<PgVec<'mcx, Oid>>
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
