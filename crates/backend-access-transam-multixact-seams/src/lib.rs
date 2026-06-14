//! Seam declarations for the `backend-access-transam-multixact` unit
//! (`access/transam/multixact.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `MultiXactIdIsRunning(multi, isLockOnly)` (multixact.c) — is any member
    /// of the multixact still running? `isLockOnly` restricts the test to
    /// lock-only members. `Err` carries the multixact-member-read `ereport`
    /// surface.
    pub fn multi_xact_id_is_running(
        multi: types_core::primitive::TransactionId,
        is_lock_only: bool,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `GetMultiXactIdMembers(multi, &members, allow_old, only_lockers)`
    /// (multixact.c) — the live members of a multixact, returned as an owned
    /// vector (C returns a `palloc`'d `MultiXactMember[]` plus `nmembers`; an
    /// empty vector is the `nmembers <= 0` case). Used by
    /// `GetMultiXactIdHintBits` (always `allow_old=false`, `only_lockers=false`
    /// for the just-created multis it inspects). `Err` carries the SLRU-read
    /// `ereport(ERROR)` surface.
    pub fn get_multi_xact_id_members<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        multi: types_core::primitive::MultiXactId,
        allow_old: bool,
        only_lockers: bool,
    ) -> types_error::PgResult<
        mcx::PgVec<'mcx, types_xlog_records::multixact::MultiXactMember>,
    >
);

seam_core::seam!(
    /// `MultiXactIdCreateFromMembers(nmembers, members)` (multixact.c) — create
    /// a new multixact carrying `members`, returning its id. Used by VACUUM's
    /// `FreezeMultiXactId` when the surviving members of an old multi must be
    /// carried forward into a fresh one. `Err` carries the SLRU/extend
    /// `ereport(ERROR)` surface.
    pub fn multi_xact_id_create_from_members(
        members: &[types_xlog_records::multixact::MultiXactMember],
    ) -> types_error::PgResult<types_core::primitive::MultiXactId>
);

seam_core::seam!(
    /// `MultiXactIdCreate(xid1, status1, xid2, status2)` (multixact.c) — create
    /// a brand-new MultiXactId with exactly the two given (xid, status) members,
    /// returning its id. Used by `compute_new_xmax_infomask` when an existing
    /// regular (non-multi) xmax must be combined with the new locker/updater.
    /// `Err` carries the SLRU/extend `ereport(ERROR)` surface.
    pub fn multi_xact_id_create(
        xid1: types_core::primitive::TransactionId,
        status1: types_xlog_records::multixact::MultiXactStatus,
        xid2: types_core::primitive::TransactionId,
        status2: types_xlog_records::multixact::MultiXactStatus,
    ) -> types_error::PgResult<types_core::primitive::MultiXactId>
);

seam_core::seam!(
    /// `MultiXactIdExpand(multi, xid, status)` (multixact.c) — add `(xid,
    /// status)` to the membership of an existing multixact, creating a fresh
    /// multixact that carries the surviving members plus the new one. Used by
    /// `compute_new_xmax_infomask` when the existing xmax is already a multi.
    /// `Err` carries the SLRU/extend `ereport(ERROR)` surface.
    pub fn multi_xact_id_expand(
        multi: types_core::primitive::MultiXactId,
        xid: types_core::primitive::TransactionId,
        status: types_xlog_records::multixact::MultiXactStatus,
    ) -> types_error::PgResult<types_core::primitive::MultiXactId>
);

seam_core::seam!(
    /// `MultiXactIdGetUpdateXid(xmax, t_infomask)` (multixact.c) — the update
    /// XID carried by a multixact xmax (the single member with an update
    /// status), or `InvalidTransactionId` if there is none. Used by
    /// `compute_new_xmax_infomask`. `Err` carries the SLRU-read `ereport`
    /// surface.
    pub fn multi_xact_id_get_update_xid(
        xmax: types_core::primitive::TransactionId,
        t_infomask: u16,
    ) -> types_error::PgResult<types_core::primitive::TransactionId>
);

seam_core::seam!(
    /// `MultiXactIdSetOldestMember()` (multixact.c) — record this backend's
    /// `OldestMemberMXactId` the first time it does a possibly-multixact-able
    /// operation in the current transaction. Idempotent within a transaction.
    /// `Err` carries the `ereport(ERROR)` surface.
    pub fn multi_xact_id_set_oldest_member() -> types_error::PgResult<()>
);

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
