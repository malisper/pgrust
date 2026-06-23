//! Seam declarations for the `backend-bootstrap-bootparse` /
//! `backend-bootstrap-bootscanner` units (`bootstrap/bootparse.y`,
//! `bootstrap/bootscanner.l`), the BKI bootstrap-language front end.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use mcx::Mcx;
use types_error::PgResult;

seam_core::seam!(
    /// `boot_yylex_init(&scanner)` (bootscanner.l): initialize a reentrant
    /// scanner; returns the C nonzero error code on failure (bootstrap.c
    /// `elog(ERROR)`s when it is nonzero). The port reads the bootstrap (BKI)
    /// input stream (`yyin`, default `stdin`) into the owned scanner buffer.
    pub fn boot_yylex_init() -> i32
);

seam_core::seam!(
    /// `boot_yyparse(scanner)` (bootparse.y): parse the bootstrap (BKI) input
    /// stream, driving the catalog-loader callbacks. `Err` carries the
    /// parse/loader `ereport(ERROR)` surface. The owned model threads the
    /// process/transaction memory context (the C `CurTransactionContext`, used
    /// by the grammar's per-line working allocations) through explicitly.
    pub fn boot_yyparse(mcx: Mcx<'static>) -> PgResult<()>
);
