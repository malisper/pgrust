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
    /// `multixact_redo(record)` (multixact.c) — WAL redo for RM_MULTIXACT_ID
    /// records (`rm_redo` slot of `RmgrTable`). Can `ereport(ERROR)`, carried
    /// on `Err`.
    pub fn multixact_redo(
        record: &mut types_wal::rmgr::XLogReaderState<'_>,
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

seam_core::seam!(
    /// `multixactoffsetssyncfiletag(const FileTag *ftag, char *path)`
    /// (multixact.c, the `syncsw[SYNC_HANDLER_MULTIXACT_OFFSET]` sync callback)
    /// — fsync the `pg_multixact/offsets` SLRU segment the tag names, returning
    /// the `0`/`<0` code, resolved path, and saved `errno`.
    pub fn multixactoffsetssyncfiletag(ftag: types_storage::sync::FileTag) -> types_error::PgResult<types_storage::sync::FileTagOpResult>
);

seam_core::seam!(
    /// `multixactmemberssyncfiletag(const FileTag *ftag, char *path)`
    /// (multixact.c, the `syncsw[SYNC_HANDLER_MULTIXACT_MEMBER]` sync callback)
    /// — fsync the `pg_multixact/members` SLRU segment the tag names, returning
    /// the `0`/`<0` code, resolved path, and saved `errno`.
    pub fn multixactmemberssyncfiletag(ftag: types_storage::sync::FileTag) -> types_error::PgResult<types_storage::sync::FileTagOpResult>
);

seam_core::seam!(
    /// `MultiXactShmemSize()` (ipci.c `CalculateShmemSize` accumulator) — shared-memory
    /// bytes this subsystem needs. `Err` carries the `add_size`/`mul_size`
    /// overflow `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn multi_xact_shmem_size() -> types_error::PgResult<types_core::Size>
);

seam_core::seam!(
    /// `MultiXactShmemInit()` (ipci.c `CreateOrAttachShmemStructs`) — allocate-or-attach
    /// this subsystem's shared-memory structures. `Err` carries the C
    /// out-of-shared-memory `ereport(ERROR)`. Owner unported; scaffolded slot.
    pub fn multi_xact_shmem_init() -> types_error::PgResult<()>
);
