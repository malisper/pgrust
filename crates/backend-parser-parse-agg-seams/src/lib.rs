//! Seam declarations for the `backend-parser-parse-agg` unit
//! (`parser/parse_agg.c`, part of the unported `backend-parser-medium2` unit).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly (mirror-PG-and-panic).

#![allow(non_snake_case)]

use types_nodes::nodes::Node;

seam_core::seam!(
    /// `contain_aggs_of_level(node, levelsup)` (parse_agg.c): does the node
    /// contain any aggregate of the specified query level? Infallible (a pure
    /// expression-tree walk).
    pub fn contain_aggs_of_level(node: &Node<'_>, levelsup: i32) -> bool
);

seam_core::seam!(
    /// `locate_agg_of_level(node, levelsup)` (parse_agg.c): the parse location
    /// of any aggregate of the specified query level, or `-1`. Infallible.
    pub fn locate_agg_of_level(node: &Node<'_>, levelsup: i32) -> i32
);
