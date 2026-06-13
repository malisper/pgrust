//! Seam declarations for the `backend-bootstrap-bootparse` /
//! `backend-bootstrap-bootscanner` units (`bootstrap/bootparse.y`,
//! `bootstrap/bootscanner.l`), the BKI bootstrap-language front end.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use types_error::PgResult;

seam_core::seam!(
    /// `boot_yylex_init(&scanner)` (bootscanner.l): initialize a reentrant
    /// scanner; returns the C nonzero error code on failure (bootstrap.c
    /// `elog(ERROR)`s when it is nonzero).
    pub fn boot_yylex_init() -> i32
);

seam_core::seam!(
    /// `boot_yyparse(scanner)` (bootparse.y): parse the bootstrap (BKI) input
    /// stream, driving the catalog-loader callbacks. `Err` carries the
    /// parse/loader `ereport(ERROR)` surface.
    pub fn boot_yyparse() -> PgResult<()>
);
