//! `_out<Type>` writers for the remaining `primnodes.h` expression family arms
//! carried directly as `Node` arms (the raw-grammar `Expr`-deriving nodes:
//! `BoolExpr`/`CaseExpr`/`NullTest`/… in [`types_nodes::rawexprnodes`]) and any
//! expression-shaped helper nodes. Each writer mirrors its `outfuncs.funcs.c`
//! body field-for-field. The post-analysis `Expr` enum arms are handled by
//! `crate::out_expr` (and its leaf writers in `lib.rs`).
//!
//! `try_out` returns `true` iff it claimed and wrote `node`.

use alloc::string::String;

use types_nodes::nodes::Node;

/// Dispatch the expression-family `Node` arms this module owns.
pub(crate) fn try_out(_buf: &mut String, _node: &Node<'_>, _write_loc: bool) -> bool {
    false
}
