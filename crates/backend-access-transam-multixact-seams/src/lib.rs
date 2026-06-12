//! Seam declarations for the `backend-access-transam-multixact` unit
//! (`access/transam/multixact.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `multixact_twophase_recover(xid, info, recdata, len)` — restore the
    /// OldestMemberMXactId entry for a prepared transaction at recovery (slot
    /// `TWOPHASE_RM_MULTIXACT_ID` of `twophase_recover_callbacks`).
    pub fn multixact_twophase_recover(
        xid: types_core::primitive::TransactionId,
        info: u16,
        recdata: &[u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `multixact_twophase_postcommit(xid, info, recdata, len)` — clear the
    /// OldestMemberMXactId entry after 2PC commit (slot
    /// `TWOPHASE_RM_MULTIXACT_ID` of `twophase_postcommit_callbacks`).
    pub fn multixact_twophase_postcommit(
        xid: types_core::primitive::TransactionId,
        info: u16,
        recdata: &[u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `multixact_twophase_postabort(xid, info, recdata, len)` — abort-side
    /// twin of `multixact_twophase_postcommit` (in C its body just calls the
    /// postcommit function); slot `TWOPHASE_RM_MULTIXACT_ID` of
    /// `twophase_postabort_callbacks`.
    pub fn multixact_twophase_postabort(
        xid: types_core::primitive::TransactionId,
        info: u16,
        recdata: &[u8],
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `AtEOXact_MultiXact()` — reset multixact backend state at transaction
    /// end.
    pub fn at_eoxact_multixact()
);

seam_core::seam!(
    /// `AtPrepare_MultiXact()` — record OldestMemberMXactId in the 2PC state.
    pub fn at_prepare_multixact() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `PostPrepare_MultiXact(xid)` — transfer the entry to the dummy proc.
    pub fn post_prepare_multixact(xid: types_core::primitive::TransactionId)
);
