//! Compatibility path: the nodeFuncs.c engine moved to `nodes_core`
//! (backend-nodes-core); existing `clauses::walker::*` users resolve there.

pub use nodes_core::*;
