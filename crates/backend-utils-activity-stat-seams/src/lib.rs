//! Seam declarations for the `backend-utils-activity-stat` unit (its
//! `utils/activity/pgstat_relation.c` 2PC surface). The owning unit installs
//! these from its `init_seams()` when it lands; until then a call panics
//! loudly.

seam_core::seam!(
    /// `pgstat_twophase_postcommit(xid, info, recdata, len)` — apply the
    /// prepared transaction's per-table stats deltas on COMMIT PREPARED (slot
    /// `TWOPHASE_RM_PGSTAT_ID` of `twophase_postcommit_callbacks`).
    pub fn pgstat_twophase_postcommit(
        xid: types_core::primitive::TransactionId,
        info: u16,
        recdata: &[u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `pgstat_twophase_postabort(xid, info, recdata, len)` — apply the
    /// prepared transaction's per-table stats deltas on ROLLBACK PREPARED
    /// (slot `TWOPHASE_RM_PGSTAT_ID` of `twophase_postabort_callbacks`).
    pub fn pgstat_twophase_postabort(
        xid: types_core::primitive::TransactionId,
        info: u16,
        recdata: &[u8],
    ) -> types_error::PgResult<()>
);
