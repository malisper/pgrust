//! Seam declarations for the `backend-nodes-read` unit (`nodes/read.c`): the
//! node-tree de-serializer.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. The reconstructed node tree is allocated in the
//! caller's context (C: `stringToNode` palloc's in `CurrentMemoryContext`).

use mcx::{Mcx, PgBox};
use types_error::PgResult;
use types_nodes::nodes::Node;

seam_core::seam!(
    /// `stringToNode(str)` (`nodes/read.c`): parse a `nodeToString` rendering
    /// back into a node tree, allocated in `mcx`. `Err` carries OOM / a parse
    /// error from the reader.
    pub fn string_to_node<'mcx>(
        mcx: Mcx<'mcx>,
        s: &str,
    ) -> PgResult<PgBox<'mcx, Node<'mcx>>>
);
