//! Seam declarations for the `backend-rewrite-rewriteManip` unit
//! (`rewrite/rewriteManip.c`, part of the unported `backend-rewrite-core` unit).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly (mirror-PG-and-panic).

#![allow(non_snake_case)]

use types_nodes::nodes::Node;

seam_core::seam!(
    /// `contain_windowfuncs(node)` (rewriteManip.c): does the node contain any
    /// window function? Infallible (a pure expression-tree walk).
    pub fn contain_windowfuncs(node: &Node<'_>) -> bool
);

seam_core::seam!(
    /// `locate_windowfunc(node)` (rewriteManip.c): the parse location of any
    /// window function in the node, or `-1`. Infallible.
    pub fn locate_windowfunc(node: &Node<'_>) -> i32
);
