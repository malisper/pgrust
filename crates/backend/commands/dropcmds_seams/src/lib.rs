//! Seam declarations for the `backend-commands-dropcmds` unit
//! (`commands/dropcmds.c`): the generic DROP command driver.
//!
//! `commands/dropcmds.c` is the callee of `standard_ProcessUtility`
//! (`tcop/utility.c`): the utility dispatcher invokes `RemoveObjects` across
//! the command boundary. Declaring it here lets the dispatcher reach the DROP
//! driver without a cargo cycle. `backend-commands-dropcmds` installs it from
//! its `init_seams()`; until then a call panics loudly.

use ::types_error::PgResult;
use ::parsenodes::DropStmt;

seam_core::seam!(
    /// `RemoveObjects(stmt)` (dropcmds.c): resolve, ownership-check, and
    /// `performMultipleDeletions` every object named in a generic `DROP`
    /// statement. Can `ereport(ERROR)` (bad object type, aggregate-vs-function
    /// mismatch, permission failure, dependency RESTRICT), carried on `Err`.
    pub fn remove_objects(stmt: &DropStmt) -> PgResult<()>
);
