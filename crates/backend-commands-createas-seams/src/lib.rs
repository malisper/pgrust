//! Seam declarations for the `backend-commands-createas` unit
//! (`commands/createas.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_error::PgResult;
use types_nodes::parsestmt::IntoClause;

seam_core::seam!(
    /// `GetIntoRelEFlags(intoClause)` (createas.c) — the executor eflags for a
    /// CREATE TABLE AS target (e.g. `EXEC_FLAG_SKIP_TRIGGERS`,
    /// `EXEC_FLAG_WITH_NO_DATA`). Reads the clause; cannot `ereport`.
    pub fn get_into_rel_eflags<'mcx>(into: &IntoClause<'mcx>) -> PgResult<i32>
);
