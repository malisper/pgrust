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
//! The live serialization `Node` here is [`::nodes::nodes::Node`] — the
//! hand-written executor/parse-node enum the `node_to_string_with_locations`
//! seam carries. Its CONVERTED value/list leaf families (`Integer`/`Float`/
//! `Boolean`/`String`/`BitString`/`List`) get their faithful bare-token writers
//! here (the `outNode` cases that route BEFORE the `{`-switch).
//!
//! The framed `{LABEL ...}` per-node `_out<Type>` routines (the generated
//! `outfuncs.funcs.c` + the hand-written custom writers) are ported
//! field-for-field for the common primitive-expression family carried as
//! [`::nodes::primnodes::Expr`] — `Var`, `Param`, `OpExpr`/`DistinctExpr`/
//! `NullIfExpr`, `FuncExpr`, `BoolExpr` — plus `TargetEntry`, with full
//! `WRITE_*_FIELD` macros (`out_var`/`out_param`/…). `args: Vec<Expr>` is
//! emitted as the bare `(child …)` node-list form via [`out_expr`], and
//! `Var.varnullingrels` as the `outBitmapset` `(b …)` form.
//!
//! Families still pending a faithful per-node writer panic loudly
//! (`mirror-pg-and-panic`) rather than emit a partial / empty `{}` dump (C's
//! `outNode` `default:` `elog(WARNING)` + empty `{}`). `_outConst` serializes
//! `consttype`/`consttypmod`/`constcollid`/`constlen`/`constbyval`/
//! `constisnull`/`location` then `outDatum(constvalue, constlen, constbyval)`
//! over the now-carried `constlen`/`constbyval` fields (the by-value word's
//! native bytes, or a by-reference value's flat [`Datum::ByRef`] image).
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

// Per-family `_out<Type>` writer modules. Each exposes a `try_out(buf, node,
// write_loc) -> bool` dispatcher that handles its own `Node` arms (returning
// `true`) or declines (`false`). `out_node_inner` walks the chain. This keeps
// the families independently editable (no shared central `match` to collide on).
pub(crate) mod out_expr_family;
pub(crate) mod out_parse_family;
pub(crate) mod out_plan_family;
pub(crate) mod out_ddl_family;
pub mod serialize_plan;

use alloc::string::String;

use core::fmt::Write as _;

use ::mcx::{Mcx, PgString};
use ::types_error::PgResult;
use ::nodes::nodes::{ntag, Node};
use ::nodes::primnodes::{
    BoolExpr, BoolExprType, Const, Expr, FuncExpr, OpExpr, Param, TargetEntry, Var,
};
use types_tuple::heaptuple::Datum;

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
pub(crate) fn out_token(buf: &mut String, s: &str) {
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
fn out_integer(buf: &mut String, n: &::nodes::value::Integer) {
    use core::fmt::Write;
    let _ = write!(buf, "{}", n.ival);
}

/// `_outFloat` (outfuncs.c:666-674): the numeric literal is emitted verbatim
/// (`appendStringInfoString(str, node->fval)`) — assumed a valid numeric literal
/// needing no quoting.
fn out_float(buf: &mut String, n: &::nodes::value::Float<'_>) {
    buf.push_str(n.fval.as_str());
}

/// `_outBoolean` (outfuncs.c:676-680): `"true"` / `"false"`.
fn out_boolean(buf: &mut String, n: &::nodes::value::Boolean) {
    buf.push_str(if n.boolval { "true" } else { "false" });
}

/// `_outString` (outfuncs.c:682-696): wrap the contents in `"` and, for a
/// non-empty value, escape the inner contents through `outToken` (the outer
/// quotes are added by hand, so an empty value is just `""`, NOT `outToken`'s
/// `""`).
fn out_string(buf: &mut String, n: &::nodes::value::StringNode<'_>) {
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
fn out_bit_string(buf: &mut String, n: &::nodes::value::BitString<'_>) {
    debug_assert!(matches!(n.bsval.as_str().as_bytes().first(), Some(b'b') | Some(b'x')));
    out_token(buf, n.bsval.as_str());
}

// ---------------------------------------------------------------------------
// WRITE_*_FIELD helpers (outfuncs.c:44-138). Each appends ` :fldname value`.
// ---------------------------------------------------------------------------

/// `WRITE_INT_FIELD` — ` :fld %d`.
pub(crate) fn write_int_field(buf: &mut String, name: &str, val: i32) {
    let _ = write!(buf, " :{} {}", name, val);
}

/// `WRITE_UINT_FIELD` — ` :fld %u`.
pub(crate) fn write_uint_field(buf: &mut String, name: &str, val: u32) {
    let _ = write!(buf, " :{} {}", name, val);
}

/// `WRITE_OID_FIELD` — ` :fld %u` (OID printed as unsigned).
pub(crate) fn write_oid_field(buf: &mut String, name: &str, val: u32) {
    let _ = write!(buf, " :{} {}", name, val);
}

/// `WRITE_BOOL_FIELD` — ` :fld true|false` (`booltostr`).
pub(crate) fn write_bool_field(buf: &mut String, name: &str, val: bool) {
    let _ = write!(buf, " :{} {}", name, if val { "true" } else { "false" });
}

/// `WRITE_ENUM_FIELD` — ` :fld %d` (the enum's integer code).
pub(crate) fn write_enum_field(buf: &mut String, name: &str, code: i32) {
    let _ = write!(buf, " :{} {}", name, code);
}

/// `WRITE_LOCATION_FIELD` — ` :fld %d`, rendering `-1` unless location fields are
/// being written (the `write_location_fields` static).
pub(crate) fn write_location_field(buf: &mut String, name: &str, val: i32, write_loc: bool) {
    let _ = write!(buf, " :{} {}", name, if write_loc { val } else { -1 });
}

/// `WRITE_STRING_FIELD` — ` :fld ` + `outToken` (a NULL string renders `<>`).
pub(crate) fn write_string_field(buf: &mut String, name: &str, val: Option<&str>) {
    let _ = write!(buf, " :{} ", name);
    match val {
        None => buf.push_str("<>"),
        Some(s) => out_token(buf, s),
    }
}

/// `outBitmapset(str, bms)` (outfuncs.c) — `(b m1 m2 ...)`, the members in
/// ascending order. The empty/NULL set is `(b)`. Operates on the
/// [`ExprRelids`]-style word storage carried by `Var.varnullingrels`.
pub(crate) fn out_bitmapset_words(buf: &mut String, words: &[u64]) {
    buf.push('(');
    buf.push('b');
    for (wi, &w) in words.iter().enumerate() {
        let mut bit = 0;
        let mut rem = w;
        while rem != 0 {
            if rem & 1 != 0 {
                let member = wi * 64 + bit;
                let _ = write!(buf, " {}", member);
            }
            rem >>= 1;
            bit += 1;
        }
    }
    buf.push(')');
}

/// `WRITE_BITMAPSET_FIELD` — ` :fld ` + `outBitmapset`.
pub(crate) fn write_bitmapset_field(buf: &mut String, name: &str, words: &[u64]) {
    let _ = write!(buf, " :{} ", name);
    out_bitmapset_words(buf, words);
}

/// `WRITE_BITMAPSET_FIELD` over an optional `Bitmapset *` (the C `NULL` set —
/// `None` — renders as `(b)`, the empty set, exactly like `outBitmapset(NULL)`).
pub(crate) fn write_bitmapset_opt_field(
    buf: &mut String,
    name: &str,
    bms: Option<&::nodes::bitmapset::Bitmapset<'_>>,
) {
    let _ = write!(buf, " :{} ", name);
    out_bitmapset(buf, bms);
}

/// `outBitmapset(str, bms)` (bitmapset.c) over an optional `Bitmapset *`:
/// `(b m1 m2 ...)`, members ascending; the NULL set is `(b)`.
pub(crate) fn out_bitmapset(buf: &mut String, bms: Option<&::nodes::bitmapset::Bitmapset<'_>>) {
    match bms {
        None => buf.push_str("(b)"),
        Some(b) => out_bitmapset_words(buf, &b.words),
    }
}

/// `WRITE_INT64_FIELD` — ` :fld %ld`.
pub(crate) fn write_int64_field(buf: &mut String, name: &str, val: i64) {
    let _ = write!(buf, " :{} {}", name, val);
}

/// `WRITE_UINT64_FIELD` — ` :fld %llu`.
pub(crate) fn write_uint64_field(buf: &mut String, name: &str, val: u64) {
    let _ = write!(buf, " :{} {}", name, val);
}

/// `WRITE_LONG_FIELD` — ` :fld %ld`.
pub(crate) fn write_long_field(buf: &mut String, name: &str, val: i64) {
    let _ = write!(buf, " :{} {}", name, val);
}

/// `outDouble(str, d)` (outfuncs.c:217) — the shortest round-trippable decimal
/// of `d` (C uses Ryū `double_to_shortest_decimal_buf`). Rust's `{}` `f64`
/// formatter likewise emits the shortest decimal that round-trips, giving a
/// byte-stable serialize→parse→serialize cycle.
pub(crate) fn out_double(buf: &mut String, d: f64) {
    let _ = write!(buf, "{}", d);
}

/// `WRITE_FLOAT_FIELD` — ` :fld ` + `outDouble`.
pub(crate) fn write_float_field(buf: &mut String, name: &str, val: f64) {
    let _ = write!(buf, " :{} ", name);
    out_double(buf, val);
}

/// `outChar(str, c)` (outfuncs.c) — `\0` becomes `<>`, otherwise the single
/// character through `outToken`.
pub(crate) fn out_char(buf: &mut String, c: u8) {
    if c == 0 {
        buf.push_str("<>");
        return;
    }
    let s = [c];
    out_token(buf, core::str::from_utf8(&s).unwrap_or("?"));
}

/// `WRITE_CHAR_FIELD` — ` :fld ` + `outChar`.
pub(crate) fn write_char_field(buf: &mut String, name: &str, c: u8) {
    let _ = write!(buf, " :{} ", name);
    out_char(buf, c);
}

/// `WRITE_NODE_FIELD` over a single optional child `Node *` (C: `outNode`; a
/// NULL pointer renders `<>` via `outNode`'s leading NULL check).
pub(crate) fn write_node_field(buf: &mut String, name: &str, child: Option<&Node<'_>>, write_loc: bool) {
    let _ = write!(buf, " :{} ", name);
    match child {
        None => buf.push_str("<>"),
        Some(n) => out_node_inner(buf, n, write_loc),
    }
}

/// `WRITE_NODE_FIELD` over a single optional child `Expr *` carried as the
/// `Node::Expr` cast (C: `outNode`; NULL renders `<>`).
pub(crate) fn write_expr_field(buf: &mut String, name: &str, child: Option<&Expr>, write_loc: bool) {
    let _ = write!(buf, " :{} ", name);
    match child {
        None => buf.push_str("<>"),
        Some(e) => out_expr(buf, e, write_loc),
    }
}

/// `WRITE_NODE_FIELD` over a `List *` of node pointers (C: `outNode` of the
/// `List`, which renders the bare `(child child ...)` form). `None`/empty list
/// → `<>` (C `NIL` is a NULL `List *`, and `outNode(NULL)` is `<>`).
pub(crate) fn write_node_list_field<T>(
    buf: &mut String,
    name: &str,
    list: Option<&[T]>,
    write_loc: bool,
    mut each: impl FnMut(&mut String, &T, bool),
) {
    let _ = write!(buf, " :{} ", name);
    match list {
        None => buf.push_str("<>"),
        Some(elems) => {
            buf.push('(');
            let mut first = true;
            for e in elems {
                if !first {
                    buf.push(' ');
                }
                first = false;
                each(buf, e, write_loc);
            }
            buf.push(')');
        }
    }
}

/// A `List *` of `int` (`_outList` for `T_IntList`): `(i v1 v2 ...)`. `None`/NIL
/// → `<>`.
pub(crate) fn write_int_list_field(buf: &mut String, name: &str, list: Option<&[i32]>) {
    let _ = write!(buf, " :{} ", name);
    match list {
        None => buf.push_str("<>"),
        Some(vals) => {
            buf.push('(');
            buf.push('i');
            for v in vals {
                let _ = write!(buf, " {}", v);
            }
            buf.push(')');
        }
    }
}

/// A `List *` of `Oid` (`_outList` for `T_OidList`): `(o v1 v2 ...)`. `None`/NIL
/// → `<>`.
pub(crate) fn write_oid_list_field(buf: &mut String, name: &str, list: Option<&[u32]>) {
    let _ = write!(buf, " :{} ", name);
    match list {
        None => buf.push_str("<>"),
        Some(vals) => {
            buf.push('(');
            buf.push('o');
            for v in vals {
                let _ = write!(buf, " {}", v);
            }
            buf.push(')');
        }
    }
}

/// `WRITE_NODE_FIELD` over a `List *args` of `Expr` (C: `outNode` of the list).
/// Renders the bare `(child child ...)` list form (`_outList` for `T_List`),
/// each child an `Expr` written through [`out_expr`]. An empty list is `()`,
/// matching `_outList`.
pub(crate) fn write_expr_list_field(buf: &mut String, name: &str, args: &[Expr], write_loc: bool) {
    let _ = write!(buf, " :{} ", name);
    buf.push('(');
    let mut first = true;
    for a in args {
        if !first {
            buf.push(' ');
        }
        first = false;
        out_expr(buf, a, write_loc);
    }
    buf.push(')');
}

// ---------------------------------------------------------------------------
// Per-node `_out<Type>` writers (the generated outfuncs.funcs.c bodies, ported
// field-for-field for the common primitive-expression / target-entry families).
// ---------------------------------------------------------------------------

/// `_outVar` (outfuncs.funcs.c).
pub(crate) fn out_var(buf: &mut String, node: &Var, write_loc: bool) {
    buf.push_str("VAR");
    write_int_field(buf, "varno", node.varno);
    write_int_field(buf, "varattno", node.varattno as i32);
    write_oid_field(buf, "vartype", node.vartype);
    write_int_field(buf, "vartypmod", node.vartypmod);
    write_oid_field(buf, "varcollid", node.varcollid);
    write_bitmapset_field(buf, "varnullingrels", &node.varnullingrels.words);
    write_uint_field(buf, "varlevelsup", node.varlevelsup);
    write_enum_field(buf, "varreturningtype", node.varreturningtype as i32);
    write_uint_field(buf, "varnosyn", node.varnosyn);
    write_int_field(buf, "varattnosyn", node.varattnosyn as i32);
    write_location_field(buf, "location", node.location, write_loc);
}

/// `_outParam` (outfuncs.funcs.c).
fn out_param(buf: &mut String, node: &Param, write_loc: bool) {
    buf.push_str("PARAM");
    write_enum_field(buf, "paramkind", node.paramkind as i32);
    write_int_field(buf, "paramid", node.paramid);
    write_oid_field(buf, "paramtype", node.paramtype);
    write_int_field(buf, "paramtypmod", node.paramtypmod);
    write_oid_field(buf, "paramcollid", node.paramcollid);
    write_location_field(buf, "location", node.location, write_loc);
}

/// `outDatum(str, value, typlen, typbyval)` (outfuncs.c) — serialize a `Datum`
/// as `LENGTH [ b0 b1 ... ]`, the signed-decimal bytes of the value.
///
/// For a by-value datum the C reads `sizeof(Datum)` (8) bytes out of the word
/// itself in native byte order and prints the type's `typlen` as the length;
/// `readDatum` symmetrically re-reads all 8 word bytes. We mirror that exactly
/// over the [`Datum::ByVal`] word via `to_ne_bytes`. For a by-reference datum
/// the C prints `datumGetSize` bytes from the pointed-to image; we print the
/// [`Datum::ByRef`] byte image verbatim (its `len()` is that size). The
/// remaining `Datum` arms (`Cstring`/`Composite`/`Expanded`/`Internal`) cannot
/// reach a `Const.constvalue` — `make_const` rejects them — so they are an
/// unreachable carrier shape here.
pub(crate) fn out_datum(buf: &mut String, value: &Datum<'_>, typlen: i32, typbyval: bool) {
    if typbyval {
        // s = (char *) (&value); print typlen as the length, then the 8 native
        // bytes of the Datum word.
        let word = match value {
            Datum::ByVal(w) => *w,
            other => panic!(
                "outDatum: by-value Const but constvalue is {:?} (only Datum::ByVal \
                 can carry a by-value Const; make_const rejects the rest)",
                core::mem::discriminant(other)
            ),
        };
        let _ = write!(buf, "{} [ ", typlen as u32);
        for b in word.to_ne_bytes() {
            // appendStringInfo(str, "%d ", (int) (s[i])) — signed char.
            let _ = write!(buf, "{} ", b as i8 as i32);
        }
        buf.push(']');
    } else {
        // s = (char *) DatumGetPointer(value);
        // length = datumGetSize(value, false, typlen). For a varlena
        // (typlen == -1) that is VARSIZE_ANY(s); for a cstring (typlen == -2)
        // that is strlen(s) + 1 (the string plus its trailing NUL). We obtain
        // the flat image of the value and emit it byte-for-byte, which carries
        // exactly datumGetSize bytes for each by-reference arm.
        match value {
            Datum::ByRef(bytes) => {
                let _ = write!(buf, "{} [ ", bytes.len() as u32);
                for &b in bytes.iter() {
                    let _ = write!(buf, "{} ", b as i8 as i32);
                }
                buf.push(']');
            }
            // A cstring datum (typlen == -2): datumGetSize is strlen(s) + 1, the
            // raw bytes plus the terminating NUL. readDatum reads these back as a
            // by-reference image (the cstring's flat form), so the round-trip is
            // faithful.
            Datum::Cstring(s) => {
                let raw = s.as_bytes();
                let _ = write!(buf, "{} [ ", (raw.len() + 1) as u32);
                for &b in raw.iter() {
                    let _ = write!(buf, "{} ", b as i8 as i32);
                }
                // trailing NUL terminator
                buf.push_str("0 ");
                buf.push(']');
            }
            // A composite/record value is itself a self-describing varlena
            // (HeapTupleHeader Datum image); emit that flat image.
            Datum::Composite(t) => {
                let image = t.to_datum_image();
                let _ = write!(buf, "{} [ ", image.len() as u32);
                for &b in image.iter() {
                    let _ = write!(buf, "{} ", b as i8 as i32);
                }
                buf.push(']');
            }
            // make_const rejects Expanded/Internal; a Const never carries them.
            other => panic!(
                "outDatum: by-reference Const but constvalue is {:?} (a by-reference \
                 Const carries its flat image in Datum::ByRef/Cstring/Composite; \
                 make_const rejects Expanded/Internal)",
                core::mem::discriminant(other)
            ),
        }
    }
}

/// `_outConst` (outfuncs.c:388).
fn out_const(buf: &mut String, node: &Const, write_loc: bool) {
    buf.push_str("CONST");
    write_oid_field(buf, "consttype", node.consttype);
    write_int_field(buf, "consttypmod", node.consttypmod);
    write_oid_field(buf, "constcollid", node.constcollid);
    write_int_field(buf, "constlen", node.constlen);
    write_bool_field(buf, "constbyval", node.constbyval);
    write_bool_field(buf, "constisnull", node.constisnull);
    write_location_field(buf, "location", node.location, write_loc);

    buf.push_str(" :constvalue ");
    if node.constisnull {
        buf.push_str("<>");
    } else {
        out_datum(buf, &node.constvalue, node.constlen, node.constbyval);
    }
}

/// `_outOpExpr` (outfuncs.funcs.c). `label` distinguishes the
/// `OPEXPR`/`DISTINCTEXPR`/`NULLIFEXPR` aliases (C: `typedef OpExpr` for the
/// latter two — same fields, different node type label).
fn out_opexpr(buf: &mut String, label: &str, node: &OpExpr, write_loc: bool) {
    buf.push_str(label);
    write_oid_field(buf, "opno", node.opno);
    write_oid_field(buf, "opfuncid", node.opfuncid);
    write_oid_field(buf, "opresulttype", node.opresulttype);
    write_bool_field(buf, "opretset", node.opretset);
    write_oid_field(buf, "opcollid", node.opcollid);
    write_oid_field(buf, "inputcollid", node.inputcollid);
    write_expr_list_field(buf, "args", &node.args, write_loc);
    write_location_field(buf, "location", node.location, write_loc);
}

/// `_outFuncExpr` (outfuncs.funcs.c).
fn out_funcexpr(buf: &mut String, node: &FuncExpr, write_loc: bool) {
    buf.push_str("FUNCEXPR");
    write_oid_field(buf, "funcid", node.funcid);
    write_oid_field(buf, "funcresulttype", node.funcresulttype);
    write_bool_field(buf, "funcretset", node.funcretset);
    write_bool_field(buf, "funcvariadic", node.funcvariadic);
    write_enum_field(buf, "funcformat", node.funcformat as i32);
    write_oid_field(buf, "funccollid", node.funccollid);
    write_oid_field(buf, "inputcollid", node.inputcollid);
    write_expr_list_field(buf, "args", &node.args, write_loc);
    write_location_field(buf, "location", node.location, write_loc);
}

/// `_outBoolExpr` (outfuncs.c) — the hand-written custom writer: `boolop` is
/// emitted as the do-it-yourself `:boolop "and"|"or"|"not"` string, then `args`
/// and `location`.
fn out_boolexpr(buf: &mut String, node: &BoolExpr, write_loc: bool) {
    buf.push_str("BOOLEXPR");
    let opstr = match node.boolop {
        BoolExprType::AND_EXPR => "and",
        BoolExprType::OR_EXPR => "or",
        BoolExprType::NOT_EXPR => "not",
    };
    // C: appendStringInfoString(str, " :boolop "); outToken(str, opstr);
    buf.push_str(" :boolop ");
    out_token(buf, opstr);
    write_expr_list_field(buf, "args", &node.args, write_loc);
    write_location_field(buf, "location", node.location, write_loc);
}

/// `_outTargetEntry` (outfuncs.funcs.c).
pub(crate) fn out_targetentry(buf: &mut String, node: &TargetEntry<'_>, write_loc: bool) {
    buf.push_str("TARGETENTRY");
    // WRITE_NODE_FIELD(expr): the child Expr (or `<>` for NULL).
    buf.push_str(" :expr ");
    match node.expr.as_deref() {
        None => buf.push_str("<>"),
        Some(e) => out_expr(buf, e, write_loc),
    }
    write_int_field(buf, "resno", node.resno as i32);
    write_string_field(buf, "resname", node.resname.as_ref().map(|s| s.as_str()));
    write_uint_field(buf, "ressortgroupref", node.ressortgroupref);
    write_oid_field(buf, "resorigtbl", node.resorigtbl);
    write_int_field(buf, "resorigcol", node.resorigcol as i32);
    write_bool_field(buf, "resjunk", node.resjunk);
}

/// `outNode` over an `Expr` subtree (C: every `Expr` is a `Node *`, so `outNode`
/// dispatches it through the `{`-switch). Opens `{`, runs the per-tag writer,
/// closes `}`. Only the common primitive-expression families are ported; any
/// other `Expr` variant `mirror-pg-and-panic`s with its tag (so a partial dump
/// is never produced — the unported per-node writer is the explicit signal).
pub(crate) fn out_expr(buf: &mut String, e: &Expr, write_loc: bool) {
    buf.push('{');
    match e {
        Expr::Var(v) => out_var(buf, v, write_loc),
        Expr::Const(c) => out_const(buf, c, write_loc),
        Expr::Param(p) => out_param(buf, p, write_loc),
        Expr::OpExpr(o) => out_opexpr(buf, "OPEXPR", o, write_loc),
        Expr::DistinctExpr(o) => out_opexpr(buf, "DISTINCTEXPR", o, write_loc),
        Expr::NullIfExpr(o) => out_opexpr(buf, "NULLIFEXPR", o, write_loc),
        Expr::FuncExpr(f) => out_funcexpr(buf, f, write_loc),
        Expr::BoolExpr(b) => out_boolexpr(buf, b, write_loc),
        other => {
            // The remaining post-analysis `Expr` arms are written by the expr
            // family's body writer (label + fields, no framing — the `{`/`}` is
            // ours). A variant no family claims `mirror-pg-and-panic`s.
            if !out_expr_family::out_expr_body(buf, other, write_loc) {
                panic!(
                    "outNode: no _out<Type> writer ported for Expr variant {:?}",
                    core::mem::discriminant(other)
                );
            }
        }
    }
    buf.push('}');
}

/// `_outList` (outfuncs.c:281-318) for a `T_List` (a list of node pointers).
///
/// `outNode` only routes a `List`/`IntList`/`OidList`/`XidList` here; the live
/// [`::nodes::nodes::Node::List`] arm is always a `T_List` (a
/// `PgVec<NodePtr>`), so this writes the `(` opener (no type char for `T_List`),
/// each child through `out_node` separated by a single space, then `)`.
fn out_list(buf: &mut String, elements: &[::mcx::PgBox<'_, Node<'_>>]) {
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
    out_node_inner(buf, obj, false)
}

/// `outNode` with the `write_location_fields` flag threaded (outfuncs.c:730-772).
pub(crate) fn out_node_inner(buf: &mut String, obj: &Node<'_>, write_loc: bool) {
    // The `{`-switch (outfuncs.c:753-770): the common primitive-expression family
    // is carried as `Node::Expr(e)`, a wrapper that spans every `Expr` tag
    // (Var/Const/…/Aggref). That is many tags, not one, so it cannot be a single
    // `ntag::T_*` arm — peel it first via the `as_expr` accessor and dispatch the
    // inner `Expr` enum in `out_expr`.
    if let Some(e) = obj.as_expr() {
        out_expr(buf, e, write_loc);
        return;
    }
    match obj.node_tag() {
        // _outList — bare `(...)` token (nodeRead does not want `{}`).
        ntag::T_List => out_list(buf, obj.expect_list().as_slice()),
        // Value nodes — bare value tokens (nodeRead does not want `{}`).
        ntag::T_Integer => out_integer(buf, obj.expect_integer()),
        ntag::T_Float => out_float(buf, obj.expect_float()),
        ntag::T_Boolean => out_boolean(buf, obj.expect_boolean()),
        ntag::T_String => out_string(buf, obj.expect_string()),
        ntag::T_BitString => out_bit_string(buf, obj.expect_bitstring()),
        // `_outTargetEntry` (lib-owned writer; the parse family does not claim it).
        ntag::T_TargetEntry => framed(buf, |b| out_targetentry(b, obj.expect_targetentry(), write_loc)),
        // The remaining framed `{LABEL ...}` node tags are dispatched through
        // the per-family `try_out` chain (each opens `{`, runs the per-node
        // `_out<Type>`, closes `}`). A tag no family claims `mirror-pg-and-panic`s
        // rather than emit a partial / empty `{}` dump (C's `default:` WARNING +
        // empty `{}`).
        _ => {
            if out_expr_family::try_out(buf, obj, write_loc)
                || out_parse_family::try_out(buf, obj, write_loc)
                || out_plan_family::try_out(buf, obj, write_loc)
                || out_ddl_family::try_out(buf, obj, write_loc)
            {
                return;
            }
            panic!(
                "outNode: no _out<Type> writer ported for node tag {:?}",
                obj.node_tag()
            )
        }
    }
}

/// Open `{`, run a per-node writer body, close `}`. The framing every framed
/// `_out<Type>` shares (`outNode`'s `{`-switch); the body writes the LABEL then
/// its `WRITE_*_FIELD`s.
pub(crate) fn framed(buf: &mut String, body: impl FnOnce(&mut String)) {
    buf.push('{');
    body(buf);
    buf.push('}');
}

/// `nodeToStringInternal(obj, write_loc_fields)` (outfuncs.c:783-799).
///
/// `write_loc_fields` selects whether location fields render their actual value
/// or `-1` (the C `write_location_fields` static). It is threaded through
/// [`out_node_inner`] into every framed `_out<Type>` writer's
/// `WRITE_LOCATION_FIELD` (e.g. `Var`/`Param`/`OpExpr`/`FuncExpr`/`BoolExpr`
/// `:location`).
fn node_to_string_internal<'mcx>(
    mcx: Mcx<'mcx>,
    obj: &Node<'_>,
    write_loc_fields: bool,
) -> PgResult<PgString<'mcx>> {
    let mut buf = String::new();
    out_node_inner(&mut buf, obj, write_loc_fields);
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
    nodes_core_seams::node_to_string_with_locations::set(nodeToStringWithLocations);
    // typecmds.c serializes a cooked default/check expression with
    // `nodeToString(expr)` for `typdefaultbin` / `conbin`.
    typecmds_seams::node_to_string::set(|mcx, node| nodeToString(mcx, &node));
    // `ExecSerializePlan` (execParallel.c) — the worker plan-shipping
    // serializer; reached over the execParallel-support seam to avoid the
    // executor->outfuncs cycle.
    execParallel_support_seams::serialize_plan_for_workers::set(
        serialize_plan::serialize_plan_for_workers,
    );
}
