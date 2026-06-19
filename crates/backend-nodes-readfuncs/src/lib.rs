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
//! `parseNodeString` reconstructs a `{LABEL ...}`-framed node. The per-tag
//! MATCH chain (`readfuncs.switch.c`) and the per-node `_read<Type>` readers
//! (`readfuncs.funcs.c` + the hand-written custom readers) are ported
//! field-for-field for the common primitive-expression family carried as
//! [`types_nodes::primnodes::Expr`] — `VAR`/`PARAM`/`OPEXPR`/`DISTINCTEXPR`/
//! `NULLIFEXPR`/`FUNCEXPR`/`BOOLEXPR` — plus `TARGETENTRY`. Each reads its
//! fields in the exact order the OUT side wrote them (`READ_*_FIELD`), keeping
//! the byte-stable round-trip property: `args` lists recurse through
//! `read.c`'s `nodeRead` (the `Expr`<->`Node` bridge), `varnullingrels` through
//! `_readBitmapset`.
//!
//! The bare value-node / `(...)`-list forms are read by `read.c`'s `nodeRead`
//! directly (not by `parseNodeString`), so the value/list leaf families
//! round-trip through `string_to_node` -> `node_read` without ever reaching
//! here. A LABEL this reader does not yet handle (e.g. `CONST`, deliberately
//! unported because the repo's `Const` trims the `constlen`/`constbyval`
//! `outDatum` needs) falls through to the faithful C
//! `elog(ERROR, "badly formatted node string \"%.32s\"...")` tail
//! (`mirror-pg-and-panic`, surfaced as the exact error).

#![no_std]
#![forbid(unsafe_code)]
#![allow(non_snake_case)]

extern crate alloc;

// Per-family `_read<Type>` reader modules. Each exposes a `try_read(mcx, label)
// -> Option<PgResult<Node>>` that, if it owns `label`, reads the node's fields
// (in the exact order the OUT side wrote them) and returns `Some(Ok(node))`.
// `parse_node_string` walks the chain. Independently editable (no shared `match`).
pub(crate) mod read_expr_family;
pub(crate) mod read_parse_family;
pub(crate) mod read_plan_family;
pub(crate) mod read_ddl_family;

use alloc::boxed::Box;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use mcx::{Mcx, PgBox, PgString};
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_nodes::nodes::Node;
use types_nodes::primnodes::{
    BoolExpr, BoolExprType, CoercionForm, Const, Expr, ExprRelids, FuncExpr, OpExpr, Param,
    ParamKind, TargetEntry, Var, VarReturningType,
};
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_nodes_core::read::{self, Token};

/// `elog(ERROR, msg)` — an internal-error `PgError` (`ERRCODE_INTERNAL_ERROR`),
/// the shape `readfuncs.c`'s `elog(ERROR, ...)` raises for a malformed node
/// string, matching the `read.c` family's error helper.
pub(crate) fn elog_error(message: impl Into<String>) -> PgError {
    PgError::error(message).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

// ---------------------------------------------------------------------------
// READ_*_FIELD helpers (readfuncs.c:73-170). Each consumes the `:fldname`
// token then the value token off the shared `pg_strtok` cursor.
// ---------------------------------------------------------------------------

/// Pull the next token off the shared cursor, erroring on premature EOF (C's
/// readers assume a well-formed string; a missing token is a malformed node).
fn next_token<'a>() -> PgResult<Token<'a>> {
    read::pg_strtok().ok_or_else(|| elog_error("unexpected end of node string"))
}

/// The UTF-8 text of a token's bytes (the reader only sees ASCII-superset
/// source produced by `nodeToString`).
pub(crate) fn tok_str(tok: &Token<'_>) -> String {
    String::from_utf8_lossy(tok.bytes).into_owned()
}

/// Skip the `:fldname` label token, then return the value token (the common
/// `token = pg_strtok(); token = pg_strtok();` prologue of the READ macros).
fn read_field_value<'a>() -> PgResult<Token<'a>> {
    let _label = next_token()?; // skip :fldname
    next_token() // value
}

/// `READ_INT_FIELD` — `atoi`.
pub(crate) fn read_int_field() -> PgResult<i32> {
    let v = read_field_value()?;
    Ok(atoi_i64(&tok_str(&v)) as i32)
}

/// `READ_UINT_FIELD` — `atoui` (`strtoul`, base 10).
pub(crate) fn read_uint_field() -> PgResult<u32> {
    let v = read_field_value()?;
    Ok(atoui_u64(&tok_str(&v)) as u32)
}

/// `READ_OID_FIELD` — `atooid` (an unsigned read).
pub(crate) fn read_oid_field() -> PgResult<u32> {
    read_uint_field()
}

/// `READ_BOOL_FIELD` — `strtobool` (`*token == 't'`).
pub(crate) fn read_bool_field() -> PgResult<bool> {
    let v = read_field_value()?;
    Ok(v.bytes.first() == Some(&b't'))
}

/// `READ_ENUM_FIELD` — `(enumtype) atoi(token)`; returns the raw integer code.
pub(crate) fn read_enum_field() -> PgResult<i32> {
    let v = read_field_value()?;
    Ok(atoi_i64(&tok_str(&v)) as i32)
}

/// `READ_LOCATION_FIELD` — in a non-debug build, the value is read but the field
/// is set to `-1` (the C `#else` branch). We consume the value token and return
/// `-1` to mirror that exactly.
pub(crate) fn read_location_field() -> PgResult<i32> {
    let _v = read_field_value()?;
    Ok(-1)
}

/// `READ_STRING_FIELD` via `nullable_string` (readfuncs.c:194): `<>` (length 0)
/// is C `NULL` (`None`); `""` is the empty string; otherwise `debackslash`.
fn read_string_field<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Option<PgString<'mcx>>> {
    let v = read_field_value()?;
    if v.bytes.is_empty() {
        // outToken emits <> for NULL; pg_strtok makes that a zero-length token.
        return Ok(None);
    }
    if v.bytes == b"\"\"" {
        return Ok(Some(PgString::from_str_in("", mcx)?));
    }
    let s = read::debackslash(v.bytes);
    Ok(Some(PgString::from_str_in(&s, mcx)?))
}

/// `READ_BITMAPSET_FIELD` (readfuncs.c `_readBitmapset`): skip the `:fldname`
/// label, then read a `(b m1 m2 ...)` member list back into the word storage
/// carried by [`ExprRelids`].
pub(crate) fn read_bitmapset_field() -> PgResult<ExprRelids> {
    let _label = next_token()?; // skip :fldname
    // C _readBitmapset: expect '(' then 'b', then members until ')'.
    let open = next_token()?;
    if open.bytes != b"(" {
        return Err(elog_error("unrecognized token: expected '(' for Bitmapset"));
    }
    let b = next_token()?;
    if b.bytes != b"b" {
        return Err(elog_error("unrecognized token: expected 'b' for Bitmapset"));
    }
    let mut words: Vec<u64> = Vec::new();
    loop {
        let t = next_token()?;
        if t.bytes == b")" {
            break;
        }
        let s = tok_str(&t);
        let val: i64 = s
            .parse()
            .map_err(|_| elog_error("unrecognized integer in Bitmapset"))?;
        if val < 0 {
            return Err(elog_error("negative Bitmapset member"));
        }
        let val = val as usize;
        let wi = val / 64;
        let bit = val % 64;
        if wi >= words.len() {
            words.resize(wi + 1, 0);
        }
        words[wi] |= 1u64 << bit;
    }
    Ok(ExprRelids { words })
}

/// `READ_NODE_FIELD` over a `List *args` of `Expr` (C: `nodeRead` of the list).
/// Skip the `:fldname` label, then `node_read` the value: a `(...)` list of
/// `{...}`-framed Exprs comes back as a `Node::List` of `Node::Expr`, which is
/// unwrapped to a `Vec<Expr>`; `<>` (C NULL) is the empty list.
fn read_expr_list_field<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Vec<Expr>> {
    let _label = next_token()?; // skip :fldname
    let read = read::node_read(mcx, None)?;
    let node = match read {
        None => return Ok(Vec::new()), // `<>` — empty arg list
        Some(n) => n,
    };
    {
            let __n = PgBox::into_inner(node);
            let __tag = __n.node_tag();
            match __n.into_list() {
                Some(elements) => {
            let mut out: Vec<Expr> = Vec::with_capacity(elements.len());
            for cell in elements {
                {
            let __n = PgBox::into_inner(cell);
            let __tag = __n.node_tag();
            match __n.into_expr() {
                Some(e) => out.push(e),
                None => {
                        return Err(elog_error(alloc::format!(
                            "expected Expr element in arg list, got {:?}",
                            __tag
                        )))
                    },
            }
        }
            }
            Ok(out)
        },
                None => Err(elog_error(alloc::format!(
            "expected List for arg field, got {:?}",
            __tag
        ))),
            }
        }
}

/// Read a single optional child `Expr` (`READ_NODE_FIELD` of an `Expr *`): skip
/// the label, `node_read` the value; `<>` is C `NULL` (`None`).
fn read_opt_expr_field<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Option<Box<Expr>>> {
    let _label = next_token()?; // skip :fldname
    let read = read::node_read(mcx, None)?;
    match read {
        None => Ok(None),
        Some(n) => {
            let node = PgBox::into_inner(n);
            let tag = node.node_tag();
            match node.into_expr() {
                Some(e) => Ok(Some(Box::new(e))),
                None => Err(elog_error(alloc::format!("expected Expr child, got {:?}", tag))),
            }
        }
    }
}

/// `READ_INT64_FIELD` — `strtoi64` over the value token.
pub(crate) fn read_int64_field() -> PgResult<i64> {
    let v = read_field_value()?;
    Ok(atoi_i64(&tok_str(&v)))
}

/// `READ_UINT64_FIELD` — `strtou64`.
pub(crate) fn read_uint64_field() -> PgResult<u64> {
    let v = read_field_value()?;
    Ok(atoui_u64(&tok_str(&v)))
}

/// `READ_LONG_FIELD` — `atol`.
pub(crate) fn read_long_field() -> PgResult<i64> {
    read_int64_field()
}

/// `READ_FLOAT_FIELD` — `atof`/`strtod` of the value token (`outDouble`'s
/// shortest-decimal text parses back exactly via Rust's `f64::from_str`).
pub(crate) fn read_float_field() -> PgResult<f64> {
    let v = read_field_value()?;
    Ok(tok_str(&v).parse::<f64>().unwrap_or(0.0))
}

/// `READ_CHAR_FIELD` — a `<>` token is `\0`, else the first debackslashed byte.
pub(crate) fn read_char_field() -> PgResult<u8> {
    let v = read_field_value()?;
    if v.bytes.is_empty() {
        return Ok(0);
    }
    let s = read::debackslash(v.bytes);
    Ok(s.as_bytes().first().copied().unwrap_or(0))
}

/// `READ_BITMAPSET_FIELD` returning an owned optional `Bitmapset` (C: a `(b)`
/// empty set is the NULL `Bitmapset *`, i.e. `None`).
pub(crate) fn read_bitmapset_opt_field<'mcx>(
    mcx: Mcx<'mcx>,
) -> PgResult<Option<PgBox<'mcx, types_nodes::bitmapset::Bitmapset<'mcx>>>> {
    let er = read_bitmapset_field()?;
    if er.words.iter().all(|w| *w == 0) {
        return Ok(None);
    }
    let mut words = mcx::PgVec::new_in(mcx);
    words
        .try_reserve(er.words.len())
        .map_err(|_| elog_error("out of memory reading bitmapset"))?;
    for w in &er.words {
        words.push(*w);
    }
    Ok(Some(mcx::alloc_in(
        mcx,
        types_nodes::bitmapset::Bitmapset { words },
    )?))
}

/// `READ_NODE_FIELD` over a single optional child `Node *`: skip the label,
/// `node_read` the value; `<>` is C `NULL` (`None`).
pub(crate) fn read_node_field<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Option<PgBox<'mcx, Node<'mcx>>>> {
    let _label = next_token()?;
    read::node_read(mcx, None)
}

/// `READ_NODE_FIELD` over a `List *` of `Node`: skip the label, `node_read`; a
/// `(...)` list comes back as a `Node::List`. `<>` (C `NIL`) is the empty list.
pub(crate) fn read_node_list_field<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Vec<PgBox<'mcx, Node<'mcx>>>> {
    let _label = next_token()?;
    match read::node_read(mcx, None)? {
        None => Ok(Vec::new()),
        Some(n) => {
            let node = PgBox::into_inner(n);
            let tag = node.node_tag();
            match node.into_list() {
                Some(elements) => Ok(elements.into_iter().collect()),
                None => Err(elog_error(alloc::format!(
                    "expected List for node-list field, got {:?}",
                    tag
                ))),
            }
        }
    }
}

/// A `List *` of `int` (`READ_NODE_FIELD` of a `T_IntList`): `<>` is C `NIL`.
pub(crate) fn read_int_list_field<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Vec<i32>> {
    let _label = next_token()?;
    match read::node_read(mcx, None)? {
        None => Ok(Vec::new()),
        Some(n) => {
            let node = PgBox::into_inner(n);
            let tag = node.node_tag();
            match node.into_list() {
            Some(elements) => {
                let mut out = Vec::with_capacity(elements.len());
                for c in elements {
                    let cell = PgBox::into_inner(c);
                    let ctag = cell.node_tag();
                    match cell.into_integer() {
                        Some(i) => out.push(i.ival),
                        None => {
                            return Err(elog_error(alloc::format!(
                                "expected Integer in IntList, got {:?}",
                                ctag
                            )))
                        }
                    }
                }
                Ok(out)
            }
            None => Err(elog_error(alloc::format!(
                "expected IntList, got {:?}",
                tag
            ))),
            }
        }
    }
}

/// `atoi`-style i64 parse: leading optional sign + digit run, stop at first
/// non-digit (C `atoi`/`atol`). Returns 0 when no leading integer.
pub(crate) fn atoi_i64(s: &str) -> i64 {
    let b = s.as_bytes();
    let mut i = 0;
    let neg = if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
        let neg = b[i] == b'-';
        i += 1;
        neg
    } else {
        false
    };
    let start = i;
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
    }
    if i == start {
        return 0;
    }
    match s[start..i].parse::<i64>() {
        Ok(v) => {
            if neg {
                -v
            } else {
                v
            }
        }
        Err(_) => {
            if neg {
                i64::MIN
            } else {
                i64::MAX
            }
        }
    }
}

/// `strtoul`-style u64 parse over the leading digit run (C `atoui`/`atooid`).
pub(crate) fn atoui_u64(s: &str) -> u64 {
    let b = s.as_bytes();
    let mut i = 0;
    // strtoul accepts a leading '+'; OID/uint outputs never carry a sign.
    if i < b.len() && b[i] == b'+' {
        i += 1;
    }
    let start = i;
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
    }
    if i == start {
        return 0;
    }
    s[start..i].parse::<u64>().unwrap_or(u64::MAX)
}

/// Decode a `ParamKind` from its integer code (`_readParam`'s `READ_ENUM_FIELD`).
fn param_kind_from(code: i32) -> ParamKind {
    match code {
        0 => ParamKind::PARAM_EXTERN,
        1 => ParamKind::PARAM_EXEC,
        2 => ParamKind::PARAM_SUBLINK,
        _ => ParamKind::PARAM_MULTIEXPR,
    }
}

/// Decode a `CoercionForm` from its integer code.
fn coercion_form_from(code: i32) -> CoercionForm {
    match code {
        0 => CoercionForm::COERCE_EXPLICIT_CALL,
        1 => CoercionForm::COERCE_EXPLICIT_CAST,
        2 => CoercionForm::COERCE_IMPLICIT_CAST,
        _ => CoercionForm::COERCE_SQL_SYNTAX,
    }
}

/// Decode a `VarReturningType` from its integer code.
fn var_returning_from(code: i32) -> VarReturningType {
    match code {
        1 => VarReturningType::VAR_RETURNING_OLD,
        2 => VarReturningType::VAR_RETURNING_NEW,
        _ => VarReturningType::VAR_RETURNING_DEFAULT,
    }
}

// ---------------------------------------------------------------------------
// Per-tag `_read<Type>` readers (the readfuncs.funcs.c bodies / hand-written
// custom readers), ported field-for-field for the common families.
// ---------------------------------------------------------------------------

/// `_readVar` (readfuncs.funcs.c). Reads fields in the exact order `_outVar`
/// wrote them.
pub(crate) fn read_var() -> PgResult<Var> {
    let varno = read_int_field()?;
    let varattno = read_int_field()? as i16;
    let vartype = read_oid_field()?;
    let vartypmod = read_int_field()?;
    let varcollid = read_oid_field()?;
    let varnullingrels = read_bitmapset_field()?;
    let varlevelsup = read_uint_field()?;
    let varreturningtype = var_returning_from(read_enum_field()?);
    let varnosyn = read_uint_field()?;
    let varattnosyn = read_int_field()? as i16;
    let location = read_location_field()?;
    Ok(Var {
        varno,
        varattno,
        vartype,
        vartypmod,
        varcollid,
        varnullingrels,
        varlevelsup,
        varnosyn,
        varattnosyn,
        varreturningtype,
        location,
    })
}

/// `_readParam` (readfuncs.funcs.c).
fn read_param() -> PgResult<Param> {
    let paramkind = param_kind_from(read_enum_field()?);
    let paramid = read_int_field()?;
    let paramtype = read_oid_field()?;
    let paramtypmod = read_int_field()?;
    let paramcollid = read_oid_field()?;
    let location = read_location_field()?;
    Ok(Param {
        paramkind,
        paramid,
        paramtype,
        paramtypmod,
        paramcollid,
        location,
    })
}

/// `readDatum(typbyval)` (readfuncs.c:600) — reconstruct a `Datum` from its
/// `LENGTH [ b0 b1 ... ]` text form. The string embeds the length but not
/// by-value-ness, so the caller passes `constbyval`.
///
/// For a by-value datum the C reads exactly `sizeof(Datum)` (8) signed-char
/// byte tokens into the word's native bytes (the embedded `length` — `typlen` —
/// is only range-checked). We rebuild the word via `from_ne_bytes` into a
/// [`Datum::ByVal`]. For a by-reference datum the C palloc's `length` bytes and
/// reads them in; we collect them into a [`Datum::ByRef`] image (length 0 → a
/// NULL pointer, mirrored as an empty image).
fn read_datum<'mcx>(mcx: Mcx<'mcx>, typbyval: bool) -> PgResult<Datum<'mcx>> {
    // length = atoui(pg_strtok());
    let len_tok = next_token()?;
    let length = atoui_u64(&tok_str(&len_tok)) as usize;

    // expect '['
    let open = next_token()?;
    if open.bytes.first() != Some(&b'[') {
        return Err(elog_error(alloc::format!(
            "expected \"[\" to start datum, but got \"{}\"; length = {}",
            tok_str(&open),
            length
        )));
    }

    let res = if typbyval {
        if length > core::mem::size_of::<usize>() {
            return Err(elog_error(alloc::format!(
                "byval datum but length = {length}"
            )));
        }
        // res = 0; s = &res; for i in 0..sizeof(Datum) { s[i] = atoi(token); }
        let mut bytes = [0u8; core::mem::size_of::<usize>()];
        for b in bytes.iter_mut() {
            let t = next_token()?;
            *b = (atoi_i64(&tok_str(&t)) as i8) as u8;
        }
        Datum::ByVal(usize::from_ne_bytes(bytes))
    } else if length == 0 {
        // res = (Datum) NULL — an empty by-reference image.
        Datum::ByRef(mcx::PgVec::new_in(mcx))
    } else {
        // s = palloc(length); for i in 0..length { s[i] = atoi(token); }
        let mut bytes = mcx::PgVec::new_in(mcx);
        bytes
            .try_reserve(length)
            .map_err(|_| elog_error("out of memory reading datum"))?;
        for _ in 0..length {
            let t = next_token()?;
            bytes.push((atoi_i64(&tok_str(&t)) as i8) as u8);
        }
        Datum::ByRef(bytes)
    };

    // expect ']'
    let close = next_token()?;
    if close.bytes.first() != Some(&b']') {
        return Err(elog_error(alloc::format!(
            "expected \"]\" to end datum, but got \"{}\"; length = {}",
            tok_str(&close),
            length
        )));
    }
    Ok(res)
}

/// `_readConst` (readfuncs.c:268). Reads the fields in the order `_outConst`
/// wrote them, then the `:constvalue` payload (`<>` when null, else `readDatum`
/// keyed on `constbyval`).
fn read_const<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Const> {
    let consttype = read_oid_field()?;
    let consttypmod = read_int_field()?;
    let constcollid = read_oid_field()?;
    let constlen = read_int_field()?;
    let constbyval = read_bool_field()?;
    let constisnull = read_bool_field()?;
    let location = read_location_field()?;

    // token = pg_strtok(&length); /* skip :constvalue */
    let _label = next_token()?;
    let constvalue: Datum<'static> = if constisnull {
        // token = pg_strtok(&length); /* skip "<>" */
        let _ = next_token()?;
        Datum::null()
    } else {
        // The repo's `Const.constvalue` is `Datum<'static>`; `make_const` only
        // ever stores the by-value word arm, and `_outConst` only emits a
        // by-value word for a non-null Const, so a reparsed Const is the
        // by-value word. A by-reference image would require a lifetime-carrying
        // Const carrier (the execTuples canonical-carrier follow-on, #113),
        // matching `make_const`'s own restriction.
        match read_datum(mcx, constbyval)? {
            Datum::ByVal(w) => Datum::ByVal(w),
            other => {
                return Err(elog_error(alloc::format!(
                    "readConst: by-reference Const value requires a lifetime-carrying \
                     Const carrier (execTuples canonical-carrier follow-on, #113); got {:?}",
                    core::mem::discriminant(&other)
                )))
            }
        }
    };

    Ok(Const {
        consttype,
        consttypmod,
        constcollid,
        constlen,
        constvalue,
        constisnull,
        constbyval,
        location,
    })
}

/// `_readOpExpr`/`_readDistinctExpr`/`_readNullIfExpr` (same fields).
fn read_opexpr<'mcx>(mcx: Mcx<'mcx>) -> PgResult<OpExpr> {
    let opno = read_oid_field()?;
    let opfuncid = read_oid_field()?;
    let opresulttype = read_oid_field()?;
    let opretset = read_bool_field()?;
    let opcollid = read_oid_field()?;
    let inputcollid = read_oid_field()?;
    let args = read_expr_list_field(mcx)?;
    let location = read_location_field()?;
    Ok(OpExpr {
        opno,
        opfuncid,
        opresulttype,
        opretset,
        opcollid,
        inputcollid,
        args,
        location,
    })
}

/// `_readFuncExpr` (readfuncs.funcs.c).
fn read_funcexpr<'mcx>(mcx: Mcx<'mcx>) -> PgResult<FuncExpr> {
    let funcid = read_oid_field()?;
    let funcresulttype = read_oid_field()?;
    let funcretset = read_bool_field()?;
    let funcvariadic = read_bool_field()?;
    let funcformat = coercion_form_from(read_enum_field()?);
    let funccollid = read_oid_field()?;
    let inputcollid = read_oid_field()?;
    let args = read_expr_list_field(mcx)?;
    let location = read_location_field()?;
    Ok(FuncExpr {
        funcid,
        funcresulttype,
        funcretset,
        funcvariadic,
        funcformat,
        funccollid,
        inputcollid,
        args,
        location,
    })
}

/// `_readBoolExpr` (readfuncs.c, custom): `boolop` is the do-it-yourself string
/// `"and"|"or"|"not"`, then `args` and `location`.
fn read_boolexpr<'mcx>(mcx: Mcx<'mcx>) -> PgResult<BoolExpr> {
    // C: token = pg_strtok (skip ":boolop"); token = pg_strtok (the string).
    let v = read_field_value()?;
    // The opstr went through outToken; for these fixed words no escaping occurs.
    let boolop = match v.bytes {
        b"and" => BoolExprType::AND_EXPR,
        b"or" => BoolExprType::OR_EXPR,
        b"not" => BoolExprType::NOT_EXPR,
        _ => return Err(elog_error("unrecognized boolop type")),
    };
    let args = read_expr_list_field(mcx)?;
    let location = read_location_field()?;
    Ok(BoolExpr {
        boolop,
        args,
        location,
    })
}

/// `_readTargetEntry` (readfuncs.funcs.c).
fn read_targetentry<'mcx>(mcx: Mcx<'mcx>) -> PgResult<TargetEntry<'mcx>> {
    let expr = read_opt_expr_field(mcx)?;
    // TargetEntry.expr is a `PgBox<'mcx, Expr>`; box the read child into mcx.
    let expr = match expr {
        None => None,
        Some(e) => Some(mcx::alloc_in(mcx, *e)?),
    };
    let resno = read_int_field()? as i16;
    let resname = read_string_field(mcx)?;
    let ressortgroupref = read_uint_field()?;
    let resorigtbl = read_oid_field()?;
    let resorigcol = read_int_field()? as i16;
    let resjunk = read_bool_field()?;
    Ok(TargetEntry {
        expr,
        resno,
        resname,
        ressortgroupref,
        resorigtbl,
        resorigcol,
        resjunk,
    })
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
pub fn parse_node_string<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgBox<'mcx, Node<'mcx>>> {
    // C: token = pg_strtok(&length);  — the node-type LABEL.
    // (check_stack_depth() is the C stack guard; the Rust port relies on the
    // runtime's own stack-overflow handling, as elsewhere in the reader.)
    let token = read::pg_strtok();

    // C: #define MATCH(tokname, namelen) (length == namelen && memcmp == 0)
    // The per-tag MATCH chain (`readfuncs.switch.c`) for the common
    // primitive-expression / target-entry families. Each builds its concrete
    // node and wraps it as the central `Node` arm the OUT side emitted.
    let label = match token {
        Some(tok) => tok.bytes,
        // EOF before a LABEL — fall through to the C error tail with empty token.
        None => {
            return Err(elog_error(
                "badly formatted node string \"\"...".to_string(),
            ))
        }
    };

    let node: Node<'mcx> = match label {
        b"VAR" => Node::Expr(Expr::Var(read_var()?)),
        b"CONST" => Node::Expr(Expr::Const(read_const(mcx)?)),
        b"PARAM" => Node::Expr(Expr::Param(read_param()?)),
        b"OPEXPR" => Node::Expr(Expr::OpExpr(read_opexpr(mcx)?)),
        b"DISTINCTEXPR" => Node::Expr(Expr::DistinctExpr(read_opexpr(mcx)?)),
        b"NULLIFEXPR" => Node::Expr(Expr::NullIfExpr(read_opexpr(mcx)?)),
        b"FUNCEXPR" => Node::Expr(Expr::FuncExpr(read_funcexpr(mcx)?)),
        b"BOOLEXPR" => Node::Expr(Expr::BoolExpr(read_boolexpr(mcx)?)),
        b"TARGETENTRY" => Node::TargetEntry(read_targetentry(mcx)?),
        // The remaining per-tag `_read<Type>` readers are dispatched through the
        // per-family `try_read` chain (each reads its fields in the exact order
        // the OUT side wrote them). A LABEL no family claims falls through to C's
        // `elog(ERROR, "badly formatted node string \"%.32s\"...")` tail.
        other => {
            if let Some(res) = read_expr_family::try_read(mcx, other) {
                res?
            } else if let Some(res) = read_parse_family::try_read(mcx, other) {
                res?
            } else if let Some(res) = read_plan_family::try_read(mcx, other) {
                res?
            } else if let Some(res) = read_ddl_family::try_read(mcx, other) {
                res?
            } else {
                let n = core::cmp::min(other.len(), 32);
                let preview = String::from_utf8_lossy(&other[..n]).into_owned();
                return Err(elog_error(alloc::format!(
                    "badly formatted node string \"{preview}\"..."
                )));
            }
        }
    };

    mcx::alloc_in(mcx, node).map_err(Into::into)
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

/// Install `parse_node_string` exactly ONCE across the whole crate's test
/// binary (the seam's `OnceLock` panics on a second `set`). Every test module
/// — the lib `tests` and each `read_*_family::tests` — routes through this one
/// global `Once`, so the seam is set at most once regardless of which module's
/// test runs first.
#[cfg(test)]
pub(crate) fn ensure_seams_for_tests() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(init_seams);
}

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
        let mcx = ctx.mcx();
        assert_round_trip(&Node::mk_integer(mcx, Integer { ival: 42 }), "42");
        assert_round_trip(&Node::mk_integer(mcx, Integer { ival: -7 }), "-7");
        assert_round_trip(&Node::mk_integer(mcx, Integer { ival: 0 }), "0");
    }

    #[test]
    fn boolean_round_trips() {
        let ctx = MemoryContext::new("bool");
        let mcx = ctx.mcx();
        assert_round_trip(&Node::mk_boolean(mcx, Boolean { boolval: true }), "true");
        assert_round_trip(&Node::mk_boolean(mcx, Boolean { boolval: false }), "false");
    }

    #[test]
    fn float_round_trips() {
        let ctx = MemoryContext::new("flt");
        let mcx = ctx.mcx();
        let fval = mcx::PgString::from_str_in("3.14", mcx).unwrap();
        assert_round_trip(&Node::mk_float(mcx, Float { fval }), "3.14");
        // A value too large for i32 lexes as Float and is kept verbatim.
        let big = mcx::PgString::from_str_in("99999999999999999999", mcx).unwrap();
        assert_round_trip(&Node::mk_float(mcx, Float { fval: big }), "99999999999999999999");
    }

    #[test]
    fn string_round_trips() {
        let ctx = MemoryContext::new("str");
        let mcx = ctx.mcx();
        // _outString wraps in quotes; the inner content is outToken-escaped.
        let sval = mcx::PgString::from_str_in("hello", mcx).unwrap();
        assert_round_trip(&Node::mk_string(mcx, StringNode { sval }), "\"hello\"");
        // A string with a space gets the space backslash-escaped inside quotes.
        let spaced = mcx::PgString::from_str_in("a b", mcx).unwrap();
        assert_round_trip(&Node::mk_string(mcx, StringNode { sval: spaced }), "\"a\\ b\"");
        // The empty string is just `""` (no outToken `""` doubling).
        let empty = mcx::PgString::from_str_in("", mcx).unwrap();
        assert_round_trip(&Node::mk_string(mcx, StringNode { sval: empty }), "\"\"");
    }

    #[test]
    fn bitstring_round_trips() {
        let ctx = MemoryContext::new("bits");
        let mcx = ctx.mcx();
        let bsval = mcx::PgString::from_str_in("b101", mcx).unwrap();
        assert_round_trip(&Node::mk_bit_string(mcx, BitString { bsval }), "b101");
        let hex = mcx::PgString::from_str_in("xFF", mcx).unwrap();
        assert_round_trip(&Node::mk_bit_string(mcx, BitString { bsval: hex }), "xFF");
    }

    #[test]
    fn node_list_round_trips() {
        let ctx = MemoryContext::new("list");
        let mcx = ctx.mcx();
        // A `(node node ...)` list of value nodes: `_outList` for T_List emits
        // `(` + space-separated children + `)`.
        let mut elements: mcx::PgVec<'_, PgBox<'_, Node<'_>>> =
            mcx::vec_with_capacity_in(mcx, 2).unwrap();
        elements.push(mcx::alloc_in(mcx, Node::mk_integer(mcx, Integer { ival: 10 })).unwrap());
        elements.push(mcx::alloc_in(mcx, Node::mk_boolean(mcx, Boolean { boolval: true })).unwrap());
        assert_round_trip(&Node::mk_list(mcx, elements), "(10 true)");
    }

    #[test]
    fn empty_node_list_round_trips() {
        let ctx = MemoryContext::new("emptylist");
        let mcx = ctx.mcx();
        let elements: mcx::PgVec<'_, PgBox<'_, Node<'_>>> =
            mcx::vec_with_capacity_in(mcx, 0).unwrap();
        // An empty list serializes as `()`.
        let node = Node::mk_list(mcx, elements);
        let text = nodeToString(mcx, &node).unwrap();
        assert_eq!(text.as_str(), "()");
    }

    // -----------------------------------------------------------------------
    // Framed `{LABEL ...}` per-node round-trips for the common
    // primitive-expression / target-entry families.
    // -----------------------------------------------------------------------

    use types_nodes::primnodes::{
        BoolExprType, CoercionForm, Const, Expr, FuncExpr, OpExpr, Param, ParamKind, TargetEntry,
        Var, VarReturningType,
    };

    use super::ensure_seams_for_tests as ensure_seams;

    /// OUT a framed node, READ it back, and assert byte-stable re-serialization.
    /// `parse_node_string` is the installed seam `string_to_node` recurses
    /// through for the `{`-framed body, so install it first.
    fn assert_framed_round_trip(node: &Node<'_>) -> String {
        ensure_seams();
        let ctx = MemoryContext::new("framed-roundtrip");
        let mcx = ctx.mcx();
        let text = nodeToString(mcx, node).expect("nodeToString");
        let parsed = string_to_node(mcx, text.as_str()).expect("string_to_node");
        let text2 = nodeToString(mcx, &parsed).expect("re-serialize");
        assert_eq!(text.as_str(), text2.as_str(), "framed re-serialize stable");
        text.as_str().to_string()
    }

    fn mk_var() -> Var {
        Var {
            varno: 1,
            varattno: 2,
            vartype: 23,
            vartypmod: -1,
            varcollid: 0,
            varnullingrels: Default::default(),
            varlevelsup: 0,
            varnosyn: 1,
            varattnosyn: 2,
            varreturningtype: VarReturningType::VAR_RETURNING_DEFAULT,
            location: 7,
        }
    }

    #[test]
    fn var_round_trips() {
        let text = assert_framed_round_trip(&Node::Expr(Expr::Var(mk_var())));
        // location renders -1 (non-debug WRITE_LOCATION_FIELD); bitmapset is (b).
        assert!(text.starts_with("{VAR :varno 1 :varattno 2 :vartype 23"), "{text}");
        assert!(text.contains(":varnullingrels (b)"), "{text}");
        assert!(text.ends_with(":location -1}"), "{text}");
    }

    #[test]
    fn var_with_nullingrels_round_trips() {
        let mut v = mk_var();
        v.varnullingrels.words = std::vec![0b1010]; // members 1 and 3
        let text = assert_framed_round_trip(&Node::Expr(Expr::Var(v)));
        assert!(text.contains(":varnullingrels (b 1 3)"), "{text}");
    }

    #[test]
    fn param_round_trips() {
        let p = Param {
            paramkind: ParamKind::PARAM_EXEC,
            paramid: 5,
            paramtype: 23,
            paramtypmod: -1,
            paramcollid: 0,
            location: -1,
        };
        let text = assert_framed_round_trip(&Node::Expr(Expr::Param(p)));
        assert!(text.starts_with("{PARAM :paramkind 1 :paramid 5"), "{text}");
    }

    #[test]
    fn opexpr_with_args_round_trips() {
        // An OpExpr whose two args are Vars (exercises the WRITE_NODE_FIELD
        // arg-list path and the Expr<->Node bridge in both directions).
        let op = OpExpr {
            opno: 96,
            opfuncid: 65,
            opresulttype: 16,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args: std::vec![Expr::Var(mk_var()), Expr::Var(mk_var())],
            location: -1,
        };
        let text = assert_framed_round_trip(&Node::Expr(Expr::OpExpr(op)));
        assert!(text.starts_with("{OPEXPR :opno 96 :opfuncid 65"), "{text}");
        assert!(text.contains(":args ({VAR"), "{text}");
    }

    #[test]
    fn funcexpr_empty_args_round_trips() {
        let f = FuncExpr {
            funcid: 100,
            funcresulttype: 23,
            funcretset: false,
            funcvariadic: false,
            funcformat: CoercionForm::COERCE_EXPLICIT_CALL,
            funccollid: 0,
            inputcollid: 0,
            args: std::vec![],
            location: -1,
        };
        let text = assert_framed_round_trip(&Node::Expr(Expr::FuncExpr(f)));
        assert!(text.contains(":args ()"), "{text}");
    }

    #[test]
    fn boolexpr_round_trips() {
        use types_nodes::primnodes::BoolExpr;
        let b = BoolExpr {
            boolop: BoolExprType::OR_EXPR,
            args: std::vec![Expr::Var(mk_var())],
            location: -1,
        };
        let text = assert_framed_round_trip(&Node::Expr(Expr::BoolExpr(b)));
        assert!(text.starts_with("{BOOLEXPR :boolop or :args"), "{text}");
    }

    #[test]
    fn targetentry_round_trips() {
        ensure_seams();
        let ctx = MemoryContext::new("te");
        let mcx = ctx.mcx();
        let expr = mcx::alloc_in(mcx, Expr::Var(mk_var())).unwrap();
        let resname = mcx::PgString::from_str_in("col", mcx).unwrap();
        let te = TargetEntry {
            expr: Some(expr),
            resno: 1,
            resname: Some(resname),
            ressortgroupref: 0,
            resorigtbl: 0,
            resorigcol: 0,
            resjunk: false,
        };
        let node = Node::mk_target_entry(mcx, te);
        let text = nodeToString(mcx, &node).unwrap();
        assert!(text.starts_with("{TARGETENTRY :expr {VAR"), "{text}");
        assert!(text.contains(":resname col"), "{text}");
        let parsed = string_to_node(mcx, text.as_str()).unwrap();
        let text2 = nodeToString(mcx, &parsed).unwrap();
        assert_eq!(text.as_str(), text2.as_str());
    }

    #[test]
    fn const_byval_round_trips() {
        // A by-value Const (the only carrier shape make_const produces):
        // serialize it, reparse it, and assert byte-stable re-serialization.
        ensure_seams();
        let ctx = MemoryContext::new("const");
        let mcx = ctx.mcx();
        let konst = Const {
            consttype: 23, // INT4OID
            consttypmod: -1,
            constcollid: 0,
            constlen: 4,
            constvalue: Datum::ByVal(42),
            constisnull: false,
            constbyval: true,
            location: -1,
        };
        let node = Node::Expr(Expr::Const(konst));
        let text = nodeToString(mcx, &node).unwrap();
        assert!(text.starts_with("{CONST :consttype 23"), "{text}");
        assert!(text.contains(":constlen 4"), "{text}");
        assert!(text.contains(":constbyval true"), "{text}");
        assert!(text.contains(":constisnull false"), "{text}");
        let parsed = string_to_node(mcx, text.as_str()).unwrap();
        let text2 = nodeToString(mcx, &parsed).unwrap();
        assert_eq!(text.as_str(), text2.as_str());
        // Confirm the reconstructed word survives the byte round-trip.
        if let Some(c) = parsed.as_const() {
            assert_eq!(c.constvalue, Datum::ByVal(42));
            assert!(!c.constisnull);
            assert!(c.constbyval);
            assert_eq!(c.constlen, 4);
        } else {
            panic!("expected Const, got {:?}", parsed.node_tag());
        }
    }

    #[test]
    fn const_null_round_trips() {
        // A NULL Const serializes `:constvalue <>` and reparses to constisnull.
        ensure_seams();
        let ctx = MemoryContext::new("const");
        let mcx = ctx.mcx();
        let konst = Const {
            consttype: 23,
            consttypmod: -1,
            constcollid: 0,
            constlen: 4,
            constvalue: Datum::null(),
            constisnull: true,
            constbyval: true,
            location: -1,
        };
        let node = Node::Expr(Expr::Const(konst));
        let text = nodeToString(mcx, &node).unwrap();
        assert!(text.contains(":constvalue <>"), "{text}");
        let parsed = string_to_node(mcx, text.as_str()).unwrap();
        let text2 = nodeToString(mcx, &parsed).unwrap();
        assert_eq!(text.as_str(), text2.as_str());
    }
}
