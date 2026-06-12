//! Seam declarations for the `backend-storage-smgr` unit
//! (`storage/smgr/smgr.c`): storage-manager calls.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly. The C `SMgrRelation` handle is the
//! `(RelFileLocator, ProcNumber)` pair `smgropen` keys on (the C
//! `RelationGetSmgr(rel)` caching is the smgr/relcache owners' concern).
seam_core::seam!(
    /// `smgrnblocks(smgropen(rlocator, backend), forknum)` (smgr.c): the
    /// number of blocks in the fork. `Err` carries the C `ereport(ERROR)`s
    /// from the underlying `mdnblocks` (e.g. `could not seek to end of file`).
    pub fn smgrnblocks(
        rlocator: types_storage::RelFileLocator,
        backend: types_core::primitive::ProcNumber,
        forknum: types_core::primitive::ForkNumber,
    ) -> types_error::PgResult<types_core::primitive::BlockNumber>
);
