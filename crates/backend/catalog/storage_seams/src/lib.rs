use mcx::{Mcx, PgVec};
use types_core::primitive::ForkNumber;
use types_error::PgResult;
use types_storage::RelFileLocator;

seam_core::seam!(
    // smgrGetPendingDeletes(forCommit, &ptr) (catalog/storage.c); allocates in mcx.
    pub fn smgr_get_pending_deletes<'mcx>(
        mcx: Mcx<'mcx>,
        for_commit: bool,
    ) -> PgResult<PgVec<'mcx, RelFileLocator>>
);

seam_core::seam!(
    pub fn smgr_do_pending_syncs(is_commit: bool, is_parallel_worker: bool) -> PgResult<()>
);

seam_core::seam!(
    pub fn rel_file_locator_skipping_wal(rlocator: RelFileLocator) -> bool
);

seam_core::seam!(
    pub fn smgr_do_pending_deletes(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    pub fn at_subcommit_smgr()
);

seam_core::seam!(
    pub fn at_subabort_smgr() -> PgResult<()>
);

seam_core::seam!(
    pub fn post_prepare_smgr()
);

seam_core::seam!(
    // DropRelationFiles(delrels, isRedo) (storage.c); redo-only here.
    pub fn drop_relation_files<'a>(delrels: &'a [RelFileLocator], is_redo: bool) -> PgResult<()>
);

seam_core::seam!(
    // RelationPreserveStorage(rlocator, atCommit) (storage.c): unlinks pending
    // deletes for rlocator; list surgery only, no ereport.
    pub fn relation_preserve_storage(rlocator: RelFileLocator, at_commit: bool)
);

seam_core::seam!(
    pub fn relation_create_storage(
        rlocator: RelFileLocator,
        relpersistence: u8,
        register_delete: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    pub fn log_smgrcreate(rlocator: RelFileLocator, fork_num: ForkNumber) -> PgResult<()>
);
