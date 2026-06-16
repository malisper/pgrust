//! `backend-nodes-readfuncs` — idiomatic owned-tree port of
//! `src/backend/nodes/readfuncs.c` (`parseNodeString` + the per-tag
//! `READ_*`-macro field readers).
//!
//! `readfuncs.c` is the half of the node de-serializer that, having seen a `{`,
//! reads the node-type keyword (LABEL) and that node's fields back into one
//! concrete node. `read.c` owns the tokenizer (`pg_strtok`) and the polymorphic
//! driver (`nodeRead`); `readfuncs.c` owns `parseNodeString()` — the giant tag
//! dispatch (`readfuncs.switch.c`). The two recurse into each other through the
//! shared `pg_strtok` cursor: `read.c`'s `nodeRead` calls `parseNodeString()`
//! for the `LEFT_BRACE` case, and `parseNodeString`'s `READ_NODE_FIELD` macros
//! call back into `nodeRead`. That edge is broken by the
//! `backend-nodes-readfuncs-seams::parse_node_string` seam, which `read.c`'s
//! `node_read` already calls and which this unit installs.
//!
//! ## `parseNodeString` (readfuncs.c:802-...)
//!
//! ```c
//! parseNodeString(void) {
//!     READ_TEMP_LOCALS();
//!     check_stack_depth();
//!     token = pg_strtok(&length);     // the node-type LABEL
//! #define MATCH(tokname, namelen) (length == namelen && memcmp(...) == 0)
//! #include "readfuncs.switch.c"       // per-tag MATCH -> _read<Type>()
//!     elog(ERROR, "badly formatted node string \"%.32s\"...", token);
//! }
//! ```
//!
//! The shared `pg_strtok` cursor is positioned just past the opening `{`; this
//! reads the LABEL keyword and matches it against the per-tag readers.
//!
//! ## What this port covers
//!
//! `parseNodeString` reconstructs a `{LABEL ...}`-framed node. In this repo the
//! framed per-node `_read<Type>` routines (`readfuncs.funcs.c` + the hand-written
//! custom readers) are emitted by `gen_node_support.pl` over node families that
//! are NOT yet converted into the de-serialization stage of
//! [`types_nodes::nodes::Node`]: every LABEL therefore falls through the (empty)
//! MATCH chain to the faithful `elog(ERROR, "badly formatted node string ...")`
//! tail. The bare value-node / `(...)`-list forms are read by `read.c`'s
//! `nodeRead` directly (not by `parseNodeString`), so the converted value/list
//! leaf families round-trip through `string_to_node` -> `node_read` without ever
//! reaching here. A framed node label reaching this point is therefore a not-yet
//! -ported per-node reader (`mirror-pg-and-panic`, surfaced as the exact C
//! `elog(ERROR)` rather than a fabricated node).

#![no_std]
#![forbid(unsafe_code)]
#![allow(non_snake_case)]

extern crate alloc;

use alloc::string::String;

use mcx::{Mcx, PgBox};
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_nodes::nodes::Node;

use backend_nodes_core::read;

/// `elog(ERROR, msg)` — an internal-error `PgError` (`ERRCODE_INTERNAL_ERROR`),
/// the shape `readfuncs.c`'s `elog(ERROR, ...)` raises for a malformed node
/// string, matching the `read.c` family's error helper.
fn elog_error(message: impl Into<String>) -> PgError {
    PgError::error(message).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

/// `parseNodeString(void)` (readfuncs.c) — with the shared `pg_strtok` cursor
/// positioned just past a node-opening `{`, read the node-type LABEL keyword and
/// that node's fields back into a freshly allocated `Node` (in `mcx`).
///
/// Reads the LABEL off the shared cursor and runs the per-tag MATCH chain. No
/// framed per-node `_read<Type>` reader is ported into this enum's
/// de-serialization stage yet, so every label falls through to the C
/// `elog(ERROR, "badly formatted node string \"%.32s\"...")` tail
/// (`mirror-pg-and-panic`). The `Mcx<'mcx>` is where a reconstructed node tree
/// would be allocated (threaded into the per-node readers when they land).
pub fn parse_node_string<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<PgBox<'mcx, Node<'mcx>>> {
    // C: token = pg_strtok(&length);  — the node-type LABEL.
    // (check_stack_depth() is the C stack guard; the Rust port relies on the
    // runtime's own stack-overflow handling, as elsewhere in the reader.)
    let token = read::pg_strtok();

    // The MATCH chain (`readfuncs.switch.c`) is empty in this repo: no framed
    // node family is converted into the de-serialization stage yet, so we fall
    // straight through to the C error tail.
    //
    // C: elog(ERROR, "badly formatted node string \"%.32s\"...", token);
    let label_preview: String = match token {
        Some(tok) => {
            let bytes = tok.bytes;
            // C uses "%.32s": at most 32 bytes of the token.
            let n = core::cmp::min(bytes.len(), 32);
            String::from_utf8_lossy(&bytes[..n]).into_owned()
        }
        None => String::new(),
    };
    Err(elog_error(alloc::format!(
        "badly formatted node string \"{label_preview}\"..."
    )))
}

/// Install this unit's inward seam: `parse_node_string`, declared on
/// `backend-nodes-readfuncs-seams` and already consumed by `read.c`'s
/// `node_read` (the `LEFT_BRACE` case). Installing it here retires the live
/// panic `string_to_node` of a `{...}`-framed node would otherwise hit.
pub fn init_seams() {
    backend_nodes_readfuncs_seams::parse_node_string::set(parse_node_string);
}

#[cfg(test)]
extern crate std;

#[cfg(test)]
mod tests {
    use super::*;
    use backend_nodes_core::read::string_to_node;
    use backend_nodes_outfuncs::nodeToString;
    use mcx::MemoryContext;
    use types_nodes::value::{BitString, Boolean, Float, Integer, StringNode};

    /// OUT a node, READ it back, and assert the reparse re-serializes to
    /// byte-identical text (a strong idempotence check across the value/list
    /// round-trip through `nodeToString` -> `string_to_node` -> `nodeToString`).
    fn assert_round_trip(node: &Node<'_>, expected_text: &str) {
        let ctx = MemoryContext::new("readfuncs-roundtrip");
        let mcx = ctx.mcx();
        let text = nodeToString(mcx, node).expect("nodeToString");
        assert_eq!(text.as_str(), expected_text, "OUT text mismatch");
        let parsed = string_to_node(mcx, text.as_str()).expect("string_to_node");
        let text2 = nodeToString(mcx, &parsed).expect("nodeToString re-serialize");
        assert_eq!(
            text.as_str(),
            text2.as_str(),
            "re-serialization not byte-stable"
        );
    }

    #[test]
    fn integer_round_trips() {
        let ctx = MemoryContext::new("int");
        let _mcx = ctx.mcx();
        assert_round_trip(&Node::Integer(Integer { ival: 42 }), "42");
        assert_round_trip(&Node::Integer(Integer { ival: -7 }), "-7");
        assert_round_trip(&Node::Integer(Integer { ival: 0 }), "0");
    }

    #[test]
    fn boolean_round_trips() {
        assert_round_trip(&Node::Boolean(Boolean { boolval: true }), "true");
        assert_round_trip(&Node::Boolean(Boolean { boolval: false }), "false");
    }

    #[test]
    fn float_round_trips() {
        let ctx = MemoryContext::new("flt");
        let mcx = ctx.mcx();
        let fval = mcx::PgString::from_str_in("3.14", mcx).unwrap();
        assert_round_trip(&Node::Float(Float { fval }), "3.14");
        // A value too large for i32 lexes as Float and is kept verbatim.
        let big = mcx::PgString::from_str_in("99999999999999999999", mcx).unwrap();
        assert_round_trip(&Node::Float(Float { fval: big }), "99999999999999999999");
    }

    #[test]
    fn string_round_trips() {
        let ctx = MemoryContext::new("str");
        let mcx = ctx.mcx();
        // _outString wraps in quotes; the inner content is outToken-escaped.
        let sval = mcx::PgString::from_str_in("hello", mcx).unwrap();
        assert_round_trip(&Node::String(StringNode { sval }), "\"hello\"");
        // A string with a space gets the space backslash-escaped inside quotes.
        let spaced = mcx::PgString::from_str_in("a b", mcx).unwrap();
        assert_round_trip(&Node::String(StringNode { sval: spaced }), "\"a\\ b\"");
        // The empty string is just `""` (no outToken `""` doubling).
        let empty = mcx::PgString::from_str_in("", mcx).unwrap();
        assert_round_trip(&Node::String(StringNode { sval: empty }), "\"\"");
    }

    #[test]
    fn bitstring_round_trips() {
        let ctx = MemoryContext::new("bits");
        let mcx = ctx.mcx();
        let bsval = mcx::PgString::from_str_in("b101", mcx).unwrap();
        assert_round_trip(&Node::BitString(BitString { bsval }), "b101");
        let hex = mcx::PgString::from_str_in("xFF", mcx).unwrap();
        assert_round_trip(&Node::BitString(BitString { bsval: hex }), "xFF");
    }

    #[test]
    fn node_list_round_trips() {
        let ctx = MemoryContext::new("list");
        let mcx = ctx.mcx();
        // A `(node node ...)` list of value nodes: `_outList` for T_List emits
        // `(` + space-separated children + `)`.
        let mut elements: mcx::PgVec<'_, PgBox<'_, Node<'_>>> =
            mcx::vec_with_capacity_in(mcx, 2).unwrap();
        elements.push(mcx::alloc_in(mcx, Node::Integer(Integer { ival: 10 })).unwrap());
        elements.push(mcx::alloc_in(mcx, Node::Boolean(Boolean { boolval: true })).unwrap());
        assert_round_trip(&Node::List(elements), "(10 true)");
    }

    #[test]
    fn empty_node_list_round_trips() {
        let ctx = MemoryContext::new("emptylist");
        let mcx = ctx.mcx();
        let elements: mcx::PgVec<'_, PgBox<'_, Node<'_>>> =
            mcx::vec_with_capacity_in(mcx, 0).unwrap();
        // An empty list serializes as `()`.
        let node = Node::List(elements);
        let text = nodeToString(mcx, &node).unwrap();
        assert_eq!(text.as_str(), "()");
    }
}
