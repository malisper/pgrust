//! Seam declarations for the `backend-commands-lockcmds` unit
//! (`commands/lockcmds.c`): the `LOCK TABLE` command driver.
//!
//! `commands/lockcmds.c` is a callee of `standard_ProcessUtility`
//! (`tcop/utility.c`): the utility dispatcher invokes `LockTableCommand` across
//! the command boundary. Declaring it here lets the dispatcher reach the LOCK
//! TABLE driver without a cargo cycle. `backend-commands-lockcmds` installs it
//! from its `init_seams()`; until then a call panics loudly.

use types_error::PgResult;
use nodes::ddlnodes::LockStmt;

seam_core::seam!(
    /// `LockTableCommand(lockstmt)` (lockcmds.c): resolve, permission-check, and
    /// lock every relation named in a `LOCK TABLE` statement (recursing over a
    /// view's defining query or an inheritance tree as appropriate). Can
    /// `ereport(ERROR)` (wrong object type, permission denied, lock not
    /// available with `NOWAIT`), carried on `Err`.
    pub fn lock_table_command(lockstmt: &LockStmt) -> PgResult<()>
);
