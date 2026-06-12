//! Seam declarations for the `backend-storage-lmgr-lock` unit
//! (`storage/lmgr/lock.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `lock_twophase_recover(xid, info, recdata, len)` — re-acquire a prepared
    /// transaction's locks at recovery startup (slot `TWOPHASE_RM_LOCK_ID` of
    /// `twophase_recover_callbacks`).
    pub fn lock_twophase_recover(
        xid: types_core::primitive::TransactionId,
        info: u16,
        recdata: &[u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `lock_twophase_postcommit(xid, info, recdata, len)` — release a prepared
    /// transaction's locks on COMMIT PREPARED (slot `TWOPHASE_RM_LOCK_ID` of
    /// `twophase_postcommit_callbacks`).
    pub fn lock_twophase_postcommit(
        xid: types_core::primitive::TransactionId,
        info: u16,
        recdata: &[u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `lock_twophase_postabort(xid, info, recdata, len)` — release a prepared
    /// transaction's locks on ROLLBACK PREPARED (slot `TWOPHASE_RM_LOCK_ID` of
    /// `twophase_postabort_callbacks`).
    pub fn lock_twophase_postabort(
        xid: types_core::primitive::TransactionId,
        info: u16,
        recdata: &[u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `lock_twophase_standby_recover(xid, info, recdata, len)` — acquire a
    /// prepared transaction's AccessExclusiveLocks at hot-standby startup
    /// (slot `TWOPHASE_RM_LOCK_ID` of `twophase_standby_recover_callbacks`).
    pub fn lock_twophase_standby_recover(
        xid: types_core::primitive::TransactionId,
        info: u16,
        recdata: &[u8],
    ) -> types_error::PgResult<()>
);
