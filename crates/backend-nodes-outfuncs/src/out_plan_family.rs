//! `_out<Type>` writers for the out_plan_family node arms. Each writer mirrors its
//! `outfuncs.funcs.c` body field-for-field. `try_out` returns `true` iff it
//! claimed and wrote `node`.

use alloc::string::String;

use types_nodes::nodes::Node;

/// Dispatch the out_plan_family `Node` arms this module owns.
pub(crate) fn try_out(_buf: &mut String, _node: &Node<'_>, _write_loc: bool) -> bool {
    false
}
