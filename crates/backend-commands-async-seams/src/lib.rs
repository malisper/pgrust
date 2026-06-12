//! Seam declarations for the `backend-commands-async` unit
//! (`commands/async.c`, LISTEN/NOTIFY). The owning unit installs these from
//! its `init_seams()` when it lands; until then a call panics loudly.

use types_error::PgResult;

seam_core::seam!(
    /// `PreCommit_Notify()` — insert pending notifications into the queue
    /// (may create a snapshot; can `ereport(ERROR)`).
    pub fn pre_commit_notify() -> PgResult<()>
);

seam_core::seam!(
    /// `AtCommit_Notify()` — signal listening backends after commit.
    pub fn at_commit_notify() -> PgResult<()>
);

seam_core::seam!(
    /// `AtAbort_Notify()`.
    pub fn at_abort_notify() -> PgResult<()>
);

seam_core::seam!(
    /// `AtSubCommit_Notify()` — reparent the subxact's pending notifies.
    pub fn at_subcommit_notify() -> PgResult<()>
);

seam_core::seam!(
    /// `AtSubAbort_Notify()`.
    pub fn at_subabort_notify()
);

seam_core::seam!(
    /// `AtPrepare_Notify()` — errors out for transactions that sent NOTIFY /
    /// LISTEN / UNLISTEN (2PC restriction).
    pub fn at_prepare_notify() -> PgResult<()>
);
