//! Seam declarations for the `backend-tcop-utility` unit (`tcop/utility.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_error::PgResult;
use types_nodes::nodes::Node;
use types_nodes::parsestmt::CommandTag;

seam_core::seam!(
    /// `CreateCommandTag(parsetree)` (utility.c) — the `CommandTag` for a raw
    /// parse-tree node (the PREPARE'd query). Pure classification, but reads
    /// the node tree; cannot `ereport` for well-formed nodes.
    pub fn create_command_tag<'mcx>(query: &Node<'mcx>) -> PgResult<CommandTag>
);
