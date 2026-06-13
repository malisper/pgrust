//! Seam declarations for the `backend-utils-mmgr-portalmem` unit
//! (`utils/mmgr/portalmem.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.
//!
//! C's `AtSubCommit_Portals` / `AtSubAbort_Portals` also receive the parent's
//! ResourceOwner; resource owners dissolve into RAII owner values here
//! (docs/query-lifecycle-raii.md), so those parameters drop out.

use types_core::SubTransactionId;
use types_error::PgResult;

seam_core::seam!(
    /// `GetPortalByName(name)` (portalmem.c) lending the named cursor's live
    /// state to a callback. The portal owner looks up the `PortalData`, and —
    /// when it has a live `queryDesc`/`estate` — lends the scalar portal fields
    /// plus borrows of the running `EState` and `PlanState` tree as a
    /// [`RunningCursorState`]; `None` is the C `!PortalIsValid` (no such
    /// portal). The borrow is lent for the callback's duration only (no
    /// `&'static mut` escapes), per the seam rules. `execCurrentOf` runs all of
    /// its decision logic inside `f` and returns the resolved
    /// [`CurrentOfTid`]. Owner: `backend-utils-mmgr-portalmem`.
    pub fn with_running_cursor(
        name: &str,
        f: &mut dyn FnMut(
            Option<types_nodes::RunningCursorState>,
        ) -> PgResult<types_nodes::CurrentOfTid>,
    ) -> PgResult<types_nodes::CurrentOfTid>
);

seam_core::seam!(
    /// `PreCommit_Portals(isPrepare)` — close open portals before commit;
    /// returns true if it did anything (the caller loops). Runs user code:
    /// can `ereport(ERROR)`.
    pub fn pre_commit_portals(is_prepare: bool) -> PgResult<bool>
);

seam_core::seam!(
    /// `AtAbort_Portals()`.
    pub fn at_abort_portals() -> PgResult<()>
);

seam_core::seam!(
    /// `AtCleanup_Portals()` — now safe to release portal memory.
    pub fn at_cleanup_portals() -> PgResult<()>
);

seam_core::seam!(
    /// `AtSubCommit_Portals(mySubid, parentSubid, parentLevel, parentXactOwner)`
    /// (owner parameter dissolved).
    pub fn at_subcommit_portals(
        my_subid: SubTransactionId,
        parent_subid: SubTransactionId,
        parent_level: i32,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AtSubAbort_Portals(mySubid, parentSubid, myXactOwner, parentXactOwner)`
    /// (owner parameters dissolved).
    pub fn at_subabort_portals(
        my_subid: SubTransactionId,
        parent_subid: SubTransactionId,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `AtSubCleanup_Portals(mySubid)`.
    pub fn at_subcleanup_portals(my_subid: SubTransactionId) -> PgResult<()>
);
