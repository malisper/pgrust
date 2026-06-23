//! Seam declarations for the `backend-nodes-read` unit (`nodes/read.c`): the
//! node-tree de-serializer.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly. The reconstructed node tree is allocated in the
//! caller's context (C: `stringToNode` palloc's in `CurrentMemoryContext`).

use ::mcx::{Mcx, PgBox};
use ::types_error::PgResult;
use ::nodes::nodes::Node;

seam_core::seam!(
    /// `stringToNode(str)` (`nodes/read.c`): parse a `nodeToString` rendering
    /// back into a node tree, allocated in `mcx`. `Err` carries OOM / a parse
    /// error from the reader.
    pub fn string_to_node<'mcx>(
        mcx: Mcx<'mcx>,
        s: &str,
    ) -> PgResult<PgBox<'mcx, Node<'mcx>>>
);

seam_core::seam!(
    /// `stringToNode(str)` faithful to C's nullable `void *` return: a top-level
    /// `<>` / empty rendering yields `Ok(None)` (C's NULL), not an error. For
    /// catalog read paths where a stored `pg_node_tree` may legitimately be a
    /// null pointer (e.g. an unconditional rule's `pg_rewrite.ev_qual`).
    pub fn string_to_node_opt<'mcx>(
        mcx: Mcx<'mcx>,
        s: &str,
    ) -> PgResult<Option<PgBox<'mcx, Node<'mcx>>>>
);
