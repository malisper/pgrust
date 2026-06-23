//! Seam declarations for the `backend-catalog-toasting` unit
//! (`catalog/toasting.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

#![allow(non_snake_case)]

use ::mcx::Mcx;
use ::types_cluster::RelOptionsToken;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_storage::lock::LOCKMODE;

seam_core::seam!(
    /// `NewHeapCreateToastTable(relOid, reloptions, lockmode, toastOid)`
    /// (toasting.c) — ends with CommandCounterIncrement.
    pub fn new_heap_create_toast_table<'mcx>(
        mcx: Mcx<'mcx>,
        rel_oid: Oid,
        reloptions: RelOptionsToken,
        lockmode: LOCKMODE,
        toast_oid: Oid,
    ) -> PgResult<()>
);
