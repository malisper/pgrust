//! Seam declarations for the `backend-parser-parse-utilcmd` unit
//! (`parser/parse_utilcmd.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
use types_error::PgResult;
use types_nodes::copy_query::Query;
use types_nodes::ddlnodes::RuleStmt;
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

seam_core::seam!(
    /// `transformRuleStmt(stmt, queryString, &actions, &whereClause)`
    /// (parse_utilcmd.c): parse-analyse a `CREATE RULE` statement. Builds the
    /// fake OLD/NEW range-table entries, runs the rule's action queries and
    /// WHERE qualification through analysis, and returns the analysed action
    /// list (a list of `Query`) plus the transformed WHERE clause (`Node`,
    /// `None` for no qual). Can `ereport(ERROR)` / allocate, carried on `Err`.
    pub fn transformRuleStmt<'mcx>(
        mcx: Mcx<'mcx>,
        stmt: &RuleStmt<'_>,
        query_string: &str,
    ) -> PgResult<(PgVec<'mcx, Query<'mcx>>, Option<Node<'mcx>>)>
);
