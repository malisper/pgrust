//! `backend-nodes-outfuncs` — idiomatic owned-tree port of
//! `src/backend/nodes/outfuncs.c`.
//!
//! `outfuncs.c` provides the public entry points `nodeToString(obj)` and
//! `nodeToStringWithLocations(obj)`: they serialize an arbitrary `Node` tree to
//! its parenthesised text form. The core is `outNode` (outfuncs.c:730-772),
//! which dispatches on the node tag:
//!
//!   * a NULL pointer renders as `<>`;
//!   * a `List`/`IntList`/`OidList`/`XidList` renders as a bare `(...)` token
//!     via `_outList` (outfuncs.c:281-318) — `nodeRead` does NOT want `{}` here;
//!   * the five value nodes `Integer`/`Float`/`Boolean`/`String`/`BitString`
//!     render as a bare value token via `_outInteger`..`_outBitString`
//!     (outfuncs.c:662-704) — again no `{}` framing;
//!   * every other node tag opens `{`, runs the generated `_out<Type>` switch
//!     (`outfuncs.switch.c`), and closes `}`.
//!
//! ## What this port covers
//!
//! The live serialization `Node` here is [`types_nodes::nodes::Node`] — the
//! hand-written executor/parse-node enum the `node_to_string_with_locations`
//! seam carries. Its CONVERTED value/list leaf families (`Integer`/`Float`/
//! `Boolean`/`String`/`BitString`/`List`) get their faithful bare-token writers
//! here (the `outNode` cases that route BEFORE the `{`-switch). The framed
//! `{LABEL ...}` per-node `_out<Type>` routines are emitted by
//! `gen_node_support.pl` over node families that are NOT yet converted into this
//! enum's serialization stage; until a family's hand/generated writer is ported,
//! its arm panics loudly (`mirror-pg-and-panic`). C's `outNode` `default:` arm
//! `elog(WARNING)`s and emits an empty `{}`; we never silently produce a partial
//! dump — the loud panic is the sanctioned "not yet ported" signal.
//!
//! This crate also installs the `node_to_string_with_locations` inward seam
//! (declared on the `backend-nodes-core` owner's seam crate, where it was
//! re-homed so the install guard can track it) that the `print` family
//! (`nodes/print.c`) drives `print`/`pprint`/`elog_node_display` through.

#![no_std]
#![forbid(unsafe_code)]
// The public API spells the C entry points `nodeToString` /
// `nodeToStringWithLocations` verbatim.
#![allow(non_snake_case)]

extern crate alloc;

use alloc::string::String;

use mcx::{Mcx, PgString};
use types_error::PgResult;
use types_nodes::nodes::Node;

/// `outToken(str, s)` (outfuncs.c:154-189). Append a non-NULL string token to
/// `buf`, inserting the protective backslashes `read.c`'s `pg_strtok` needs.
///
/// * an empty string becomes `""`;
/// * a leading `<`, `"`, digit, or sign-before-digit/dot gets one protective
///   leading `\` (so the token does not look like a number / `<>` / a quoted
///   string to `nodeTokenType`);
/// * a ` `, `\n`, `\t`, `(`, `)`, `{`, `}`, or `\` anywhere is backslashed.
///
/// NULL is handled by the caller (it emits `<>`); this never receives NULL.
fn out_token(buf: &mut String, s: &str) {
    // C: if (s == NULL) { appendStringInfoString(str, "<>"); return; }
    // The NULL case is handled by the caller's Option dispatch; here s is a
    // real (possibly empty) string.
    if s.is_empty() {
        buf.push_str("\"\"");
        return;
    }
    let bytes = s.as_bytes();
    // C (outfuncs.c:174-179): protect a leading char that would otherwise be
    // misread by nodeTokenType.
    let first = bytes[0];
    let needs_lead = first == b'<'
        || first == b'"'
        || first.is_ascii_digit()
        || ((first == b'+' || first == b'-')
            && bytes.len() > 1
            && (bytes[1].is_ascii_digit() || bytes[1] == b'.'));
    if needs_lead {
        buf.push('\\');
    }
    // C (outfuncs.c:181-188): backslash every special char.
    for ch in s.chars() {
        if ch == ' '
            || ch == '\n'
            || ch == '\t'
            || ch == '('
            || ch == ')'
            || ch == '{'
            || ch == '}'
            || ch == '\\'
        {
            buf.push('\\');
        }
        buf.push(ch);
    }
}

/// `_outInteger` (outfuncs.c:660-664): `appendStringInfo(str, "%d", node->ival)`.
fn out_integer(buf: &mut String, n: &types_nodes::value::Integer) {
    use core::fmt::Write;
    let _ = write!(buf, "{}", n.ival);
}

/// `_outFloat` (outfuncs.c:666-674): the numeric literal is emitted verbatim
/// (`appendStringInfoString(str, node->fval)`) — assumed a valid numeric literal
/// needing no quoting.
fn out_float(buf: &mut String, n: &types_nodes::value::Float<'_>) {
    buf.push_str(n.fval.as_str());
}

/// `_outBoolean` (outfuncs.c:676-680): `"true"` / `"false"`.
fn out_boolean(buf: &mut String, n: &types_nodes::value::Boolean) {
    buf.push_str(if n.boolval { "true" } else { "false" });
}

/// `_outString` (outfuncs.c:682-696): wrap the contents in `"` and, for a
/// non-empty value, escape the inner contents through `outToken` (the outer
/// quotes are added by hand, so an empty value is just `""`, NOT `outToken`'s
/// `""`).
fn out_string(buf: &mut String, n: &types_nodes::value::StringNode<'_>) {
    buf.push('"');
    let s = n.sval.as_str();
    if !s.is_empty() {
        out_token(buf, s);
    }
    buf.push('"');
}

/// `_outBitString` (outfuncs.c:698-707): the lexer always produces a string
/// starting `b`/`x`; `outToken` will not escape that prefix (relied on by
/// `nodeTokenType`), so the whole value goes through `outToken`.
fn out_bit_string(buf: &mut String, n: &types_nodes::value::BitString<'_>) {
    debug_assert!(matches!(n.bsval.as_str().as_bytes().first(), Some(b'b') | Some(b'x')));
    out_token(buf, n.bsval.as_str());
}

/// `_outList` (outfuncs.c:281-318) for a `T_List` (a list of node pointers).
///
/// `outNode` only routes a `List`/`IntList`/`OidList`/`XidList` here; the live
/// [`types_nodes::nodes::Node::List`] arm is always a `T_List` (a
/// `PgVec<NodePtr>`), so this writes the `(` opener (no type char for `T_List`),
/// each child through `out_node` separated by a single space, then `)`.
fn out_list(buf: &mut String, elements: &[mcx::PgBox<'_, Node<'_>>]) {
    buf.push('(');
    // C: foreach(lc, node) { outNode(str, lfirst(lc)); if (lnext(node, lc))
    // appendStringInfoChar(str, ' '); }  — for the IsA(node, List) flavour.
    let mut first = true;
    for cell in elements {
        if !first {
            buf.push(' ');
        }
        first = false;
        out_node(buf, cell);
    }
    buf.push(')');
}

/// `outNode(str, obj)` (outfuncs.c:730-772) — convert a `Node` to its text form
/// and append it to `buf`.
///
/// `obj` is a real (non-NULL) node here; a NULL pointer (`<>`) is emitted by the
/// caller's Option/NodePtr dispatch where one is reachable. The value/list
/// families are written as bare tokens (the cases C handles BEFORE the
/// `{`-switch); every other node tag would, in C, open `{`, run the generated
/// `_out<Type>`, and close `}` — none of those per-node writers are ported into
/// this enum's serialization stage yet, so those arms panic loudly.
pub fn out_node(buf: &mut String, obj: &Node<'_>) {
    match obj {
        // _outList — bare `(...)` token (nodeRead does not want `{}`).
        Node::List(elements) => out_list(buf, elements.as_slice()),
        // Value nodes — bare value tokens (nodeRead does not want `{}`).
        Node::Integer(n) => out_integer(buf, n),
        Node::Float(n) => out_float(buf, n),
        Node::Boolean(n) => out_boolean(buf, n),
        Node::String(n) => out_string(buf, n),
        Node::BitString(n) => out_bit_string(buf, n),
        // The `{`-switch (outfuncs.c:753-770): a per-node `_out<Type>` chosen by
        // nodeTag. None of these per-node writers are ported into this enum's
        // serialization stage yet — `mirror-pg-and-panic` rather than emit a
        // partial / empty `{}` dump (C's `default:` WARNING + empty `{}`).
        other => panic!(
            "outNode: no _out<Type> writer ported for node tag {:?} \
             (only the value/list leaf families serialize so far)",
            other.node_tag()
        ),
    }
}

/// `nodeToStringInternal(obj, write_loc_fields)` (outfuncs.c:783-799).
///
/// `write_loc_fields` selects whether location fields render their actual value
/// or `-1`. No location field is reachable from the converted value/list leaf
/// families (they have none), so this flag is carried for API fidelity but does
/// not change the output yet; it will take effect once a framed node family with
/// a `:location` field is ported into the serialization stage.
fn node_to_string_internal<'mcx>(
    mcx: Mcx<'mcx>,
    obj: &Node<'_>,
    _write_loc_fields: bool,
) -> PgResult<PgString<'mcx>> {
    let mut buf = String::new();
    out_node(&mut buf, obj);
    PgString::from_str_in(&buf, mcx)
}

/// `nodeToString(obj)` (outfuncs.c:804-808) — the ascii representation of the
/// node, allocated in `mcx` (C: a palloc'd `char *`). Location fields render as
/// `-1`.
pub fn nodeToString<'mcx>(mcx: Mcx<'mcx>, obj: &Node<'_>) -> PgResult<PgString<'mcx>> {
    node_to_string_internal(mcx, obj, false)
}

/// `nodeToStringWithLocations(obj)` (outfuncs.c:810-814) — like
/// [`nodeToString`] but location fields render their actual value. This is the
/// entry the `print` family drives through.
pub fn nodeToStringWithLocations<'mcx>(
    mcx: Mcx<'mcx>,
    obj: &Node<'_>,
) -> PgResult<PgString<'mcx>> {
    node_to_string_internal(mcx, obj, true)
}

/// Install this unit's inward seam: the `node_to_string_with_locations` slot the
/// `print` family (and any other whole-tree serializer consumer) calls. Declared
/// on `backend-nodes-core-seams` (re-homed there for the install guard), owned
/// and installed here.
pub fn init_seams() {
    backend_nodes_core_seams::node_to_string_with_locations::set(nodeToStringWithLocations);
}
