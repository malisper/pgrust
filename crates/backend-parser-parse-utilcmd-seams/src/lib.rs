//! Seam declarations for the `backend-parser-parse-utilcmd` unit
//! (`parser/parse_utilcmd.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
use types_error::PgResult;
use types_nodes::nodes::Node;

seam_core::seam!(
    /// `transformCreateSchemaStmtElements(schemaElts, schemaName)`
    /// (parse_utilcmd.c): reorganize the list of commands embedded in a
    /// CREATE SCHEMA into a sequentially executable order with no forward
    /// references. The result is still a list of raw parsetrees. Can
    /// `ereport(ERROR)` / allocate, carried on `Err`.
    pub fn transformCreateSchemaStmtElements<'mcx>(
        mcx: Mcx<'mcx>,
        schema_elts: &[Node<'_>],
        schema_name: &str,
    ) -> PgResult<PgVec<'mcx, Node<'mcx>>>
);
