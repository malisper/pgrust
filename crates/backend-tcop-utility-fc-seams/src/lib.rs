//! foreigncmds' slice of utility-statement handling (`tcop/utility.c`).
//!
//! `commands/foreigncmds.c`'s `ImportForeignSchema` builds a wrapper
//! `PlannedStmt` for each FDW-returned `CREATE FOREIGN TABLE` and runs it via
//! `ProcessUtility`. The `PlannedStmt` is constructed in the command driver
//! (the C `makeNode(PlannedStmt)`); this seam carries those field values into
//! `ProcessUtility`. The owning unit installs this from its `init_seams()`
//! when it lands; until then a call panics loudly.

use types_error::PgResult;
use types_foreigncmds::ImportPlannedStmt;

seam_core::seam!(
    /// `ProcessUtility(pstmt, queryString, false, PROCESS_UTILITY_SUBCOMMAND,
    /// NULL, NULL, None_Receiver, NULL)` (tcop/utility.c) — execute one
    /// `CREATE FOREIGN TABLE` wrapper statement the IMPORT FOREIGN SCHEMA loop
    /// built. `pstmt` is the driver-constructed wrapper [`ImportPlannedStmt`];
    /// `query_string` is the FDW-generated command text. The fixed call-site
    /// arguments (subcommand context, no params/queryenv, `None_Receiver`
    /// destination, no completion tag) are supplied by the owner. Can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn process_utility_import_subcommand(
        pstmt: ImportPlannedStmt,
        query_string: &str,
    ) -> PgResult<()>
);
