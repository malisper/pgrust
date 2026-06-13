#![allow(non_snake_case)]
// `PgError` is the large shared error type used across the whole tree; boxing it
// here would diverge from every sibling crate's `PgResult` shape.
#![allow(clippy::result_large_err)]

//! `backend/commands/define.c` — support routines for dealing with `DefElem`
//! nodes.
//!
//! Every C function is ported here against the raw-parser node tree
//! ([`types_parsenodes::DefElem`], whose value is `Option<Box<Node>>`): the C
//! `nodeTag(def->arg)` `switch` becomes a `match` on the owned node.
//!
//! Cross-subsystem boundaries cross the owning unit's seam: `TypeNameToString`
//! and `makeTypeNameFromNameList` (parse_type.c / makefuncs.c, unported) go
//! through their owner seams; `NameListToString` (namespace.c) and the
//! `int8in` / `oidin` numeric parsers (numutils) are called directly;
//! `parser_errposition` for `errorConflictingDefElem` crosses the parse_node.c
//! owner seam.
//!
//! The `def_get_string` / `def_get_boolean` seams ([`backend_commands_define_seams`])
//! are the cycle-breaking entry points DDL callers use; they receive the value
//! node as the projected [`DefElemArg`] (the variants `defGetString` /
//! `defGetBoolean` read), and this crate runs the same nodeTag logic on it.

use backend_commands_define_seams::DefElemArg;
use backend_parser_parse_type_seams::typename_to_string_node;
use backend_parser_small1_seams::parser_errposition;
use backend_utils_error::ereport;
use mcx::{Mcx, PgString};
use types_cluster::ParseState;
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_SYNTAX_ERROR, ERROR};
use types_parsenodes::{Node, StringNode, TypeName};

// ---------------------------------------------------------------------------
// The raw-node value extractors (operating on `types_parsenodes::DefElem`).
// ---------------------------------------------------------------------------

/// `defGetString` (define.c:34-62) — extract a string value (otherwise
/// uninterpreted) from a `DefElem`. Allocates the result in `mcx`
/// (C: `psprintf` / `pstrdup` / the rendered string).
pub fn defGetString<'mcx>(mcx: Mcx<'mcx>, def: &types_parsenodes::DefElem) -> PgResult<PgString<'mcx>> {
    let arg = require_arg(def, "requires a parameter")?;
    match arg {
        // case T_Integer: return psprintf("%ld", (long) intVal(def->arg));
        Node::Integer(i) => PgString::from_str_in(&i.ival.to_string(), mcx),
        // case T_Float: return castNode(Float, def->arg)->fval;
        Node::Float(f) => PgString::from_str_in(float_fval(f), mcx),
        // case T_Boolean: return boolVal(def->arg) ? "true" : "false";
        Node::Boolean(b) => PgString::from_str_in(if b.boolval { "true" } else { "false" }, mcx),
        // case T_String: return strVal(def->arg);
        Node::String(s) => PgString::from_str_in(string_sval(s), mcx),
        // case T_TypeName: return TypeNameToString((TypeName *) def->arg);
        Node::TypeName(t) => typename_to_string_node::call(mcx, t),
        // case T_List: return NameListToString((List *) def->arg);
        Node::List(cells) => {
            let names = name_list(cells)?;
            backend_catalog_namespace::NameListToString(mcx, &names)
        }
        // case T_A_Star: return pstrdup("*");
        Node::A_Star => PgString::from_str_in("*", mcx),
        // default: elog(ERROR, "unrecognized node type: %d", ...);
        _ => Err(unrecognized_node_type(arg)),
    }
}

/// `defGetNumeric` (define.c:67-88) — extract a numeric value (actually
/// `double`) from a `DefElem`.
pub fn defGetNumeric(def: &types_parsenodes::DefElem) -> PgResult<f64> {
    let arg = require_arg_msg(def, |name| format!("{name} requires a numeric value"))?;
    match arg {
        // case T_Integer: return (double) intVal(def->arg);
        Node::Integer(i) => Ok(i.ival as f64),
        // case T_Float: return floatVal(def->arg);
        Node::Float(f) => Ok(floatVal(f)),
        // default: ereport(... "%s requires a numeric value" ...);
        _ => Err(syntax_error(format!("{} requires a numeric value", defname(def)))),
    }
}

/// `defGetBoolean` (define.c:93-143) — extract a Boolean value from a `DefElem`.
pub fn defGetBoolean(def: &types_parsenodes::DefElem) -> PgResult<bool> {
    // If no parameter value given, assume "true" is meant.
    let Some(arg) = def.arg.as_deref() else {
        return Ok(true);
    };

    // Allow 0, 1, "true", "false", "on", "off"
    match arg {
        Node::Integer(i) => match i.ival {
            0 => return Ok(false),
            1 => return Ok(true),
            // default: /* otherwise, error out below */ break;
            _ => {}
        },
        _ => {
            // The set of strings accepted here should match up with the
            // grammar's opt_boolean_or_string production.
            let sval = def_get_string_text(def)?;
            if pg_strcasecmp(&sval, "true") {
                return Ok(true);
            }
            if pg_strcasecmp(&sval, "false") {
                return Ok(false);
            }
            if pg_strcasecmp(&sval, "on") {
                return Ok(true);
            }
            if pg_strcasecmp(&sval, "off") {
                return Ok(false);
            }
        }
    }
    Err(syntax_error(format!("{} requires a Boolean value", defname(def))))
}

/// `defGetInt32` (define.c:148-167) — extract an int32 value from a `DefElem`.
pub fn defGetInt32(def: &types_parsenodes::DefElem) -> PgResult<i32> {
    let arg = require_arg_msg(def, |name| format!("{name} requires an integer value"))?;
    match arg {
        // case T_Integer: return (int32) intVal(def->arg);
        Node::Integer(i) => Ok(i.ival),
        // default: ereport(... "%s requires an integer value" ...);
        _ => Err(syntax_error(format!("{} requires an integer value", defname(def)))),
    }
}

/// `defGetInt64` (define.c:172-200) — extract an int64 value from a `DefElem`.
pub fn defGetInt64(def: &types_parsenodes::DefElem) -> PgResult<i64> {
    let arg = require_arg_msg(def, |name| format!("{name} requires a numeric value"))?;
    match arg {
        // case T_Integer: return (int64) intVal(def->arg);
        Node::Integer(i) => Ok(i.ival as i64),
        // case T_Float:
        //   Values too large for int4 are Float constants; accept valid int8
        //   strings: DatumGetInt64(DirectFunctionCall1(int8in, ...->fval)).
        Node::Float(f) => int8in(float_fval(f)),
        // default: ereport(... "%s requires a numeric value" ...);
        _ => Err(syntax_error(format!("{} requires a numeric value", defname(def)))),
    }
}

/// `defGetObjectId` (define.c:205-233) — extract an OID value from a `DefElem`.
pub fn defGetObjectId(def: &types_parsenodes::DefElem) -> PgResult<Oid> {
    let arg = require_arg_msg(def, |name| format!("{name} requires a numeric value"))?;
    match arg {
        // case T_Integer: return (Oid) intVal(def->arg);
        Node::Integer(i) => Ok(i.ival as Oid),
        // case T_Float:
        //   Values too large for int4 are Float constants; accept valid OID
        //   strings: DatumGetObjectId(DirectFunctionCall1(oidin, ...->fval)).
        Node::Float(f) => oidin(float_fval(f)),
        // default: ereport(... "%s requires a numeric value" ...);
        _ => Err(syntax_error(format!("{} requires a numeric value", defname(def)))),
    }
}

/// `defGetQualifiedName` (define.c:238-262) — extract a possibly-qualified name
/// (as a list of `String`s) from a `DefElem`.
pub fn defGetQualifiedName(def: &types_parsenodes::DefElem) -> PgResult<Vec<Node>> {
    let arg = require_arg(def, "requires a parameter")?;
    match arg {
        // case T_TypeName: return ((TypeName *) def->arg)->names;
        Node::TypeName(t) => Ok(t.names.clone()),
        // case T_List: return (List *) def->arg;
        Node::List(cells) => Ok(cells.to_vec()),
        // case T_String: /* quoted name */ return list_make1(def->arg);
        Node::String(s) => Ok(list_make1_string(s)),
        // default: ereport(... "argument of %s must be a name" ...);
        _ => Err(syntax_error(format!("argument of {} must be a name", defname(def)))),
    }
}

/// `defGetTypeName` (define.c:270-292) — extract a `TypeName` from a `DefElem`.
///
/// Note: a `List` arg is not accepted, because the parser only returns a bare
/// `List` when the name looks like an operator name.
pub fn defGetTypeName(def: &types_parsenodes::DefElem) -> PgResult<TypeName> {
    let arg = require_arg(def, "requires a parameter")?;
    match arg {
        // case T_TypeName: return (TypeName *) def->arg;
        Node::TypeName(t) => Ok(t.clone()),
        // case T_String: /* quoted typename */
        //   return makeTypeNameFromNameList(list_make1(def->arg));
        Node::String(s) => {
            backend_nodes_makefuncs_seams::make_type_name_from_name_list::call(list_make1_string(s))
        }
        // default: ereport(... "argument of %s must be a type name" ...);
        _ => Err(syntax_error(format!("argument of {} must be a type name", defname(def)))),
    }
}

/// `defGetTypeLength` (define.c:298-337) — extract a type-length indicator
/// (either absolute bytes, or `-1` for "variable") from a `DefElem`. Allocates
/// in `mcx` only on the error path (which renders `defGetString`).
pub fn defGetTypeLength<'mcx>(mcx: Mcx<'mcx>, def: &types_parsenodes::DefElem) -> PgResult<i32> {
    let arg = require_arg(def, "requires a parameter")?;
    match arg {
        // case T_Integer: return intVal(def->arg);
        Node::Integer(i) => return Ok(i.ival),
        // case T_Float: ereport(... "%s requires an integer value" ...); break;
        Node::Float(_) => {
            return Err(syntax_error(format!("{} requires an integer value", defname(def))));
        }
        // case T_String: if (pg_strcasecmp(strVal, "variable") == 0) return -1; break;
        Node::String(s) => {
            if pg_strcasecmp(string_sval(s), "variable") {
                return Ok(-1); // variable length
            }
            // fall through to the invalid-argument error below
        }
        // case T_TypeName: /* cope if grammar believes "variable" is a typename */
        Node::TypeName(t) => {
            if pg_strcasecmp(typename_to_string_node::call(mcx, t)?.as_str(), "variable") {
                return Ok(-1); // variable length
            }
            // fall through to the invalid-argument error below
        }
        // case T_List: /* must be an operator name */ break;
        Node::List(_) => {
            // fall through to the invalid-argument error below
        }
        // default: elog(ERROR, "unrecognized node type: %d", ...);
        _ => return Err(unrecognized_node_type(arg)),
    }
    // ereport(... "invalid argument for %s: \"%s\"", def->defname, defGetString(def) ...)
    let rendered = defGetString(mcx, def)?;
    Err(syntax_error(format!(
        "invalid argument for {}: \"{}\"",
        defname(def),
        rendered.as_str()
    )))
}

/// `defGetStringList` (define.c:342-365) — extract a list of `String` values
/// (otherwise uninterpreted) from a `DefElem`. Returns the cells (borrowing the
/// arg); each is validated to be a `String` node, as the C `IsA(str, String)`
/// loop.
pub fn defGetStringList(def: &types_parsenodes::DefElem) -> PgResult<&[Node]> {
    let arg = require_arg(def, "requires a parameter")?;
    // if (nodeTag(def->arg) != T_List) elog(ERROR, "unrecognized node type: %d", ...);
    let Some(cells) = arg.as_list() else {
        return Err(unrecognized_node_type(arg));
    };

    // foreach(cell, ...) if (!IsA(str, String)) elog(ERROR, "unexpected node type in name list: %d", ...);
    for cell in cells {
        if cell.as_string().is_none() {
            return Err(unexpected_node_type_in_name_list(cell));
        }
    }

    // return (List *) def->arg;
    Ok(cells)
}

/// `errorConflictingDefElem` (define.c:370-377) — raise an error about a
/// conflicting `DefElem`.
pub fn errorConflictingDefElem(defel: &types_parsenodes::DefElem, pstate: &ParseState) -> PgResult<()> {
    let position = parser_errposition::call(pstate, defel.location)?;
    Err(ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg("conflicting or redundant options")
        .errposition(position)
        .into_error())
}

// ---------------------------------------------------------------------------
// Seam entry points: the DefElemArg projection DDL callers marshal across the
// cycle. Runs the same nodeTag logic over the projected value.
// ---------------------------------------------------------------------------

/// Install seam: `defGetString` over the [`DefElemArg`] projection.
fn seam_def_get_string<'mcx>(
    mcx: Mcx<'mcx>,
    defname: String,
    arg: Option<DefElemArg>,
) -> PgResult<PgString<'mcx>> {
    let s = arg_get_string(&defname, arg.as_ref())?;
    PgString::from_str_in(&s, mcx)
}

/// Install seam: `defGetBoolean` over the [`DefElemArg`] projection.
fn seam_def_get_boolean(defname: String, arg: Option<DefElemArg>) -> PgResult<bool> {
    // If no parameter value given, assume "true" is meant.
    let Some(arg) = arg else {
        return Ok(true);
    };
    // Allow 0, 1, "true", "false", "on", "off"
    if let DefElemArg::Integer(value) = arg {
        match value {
            0 => return Ok(false),
            1 => return Ok(true),
            _ => {}
        }
    } else {
        let sval = arg_get_string(&defname, Some(&arg))?;
        if pg_strcasecmp(&sval, "true") {
            return Ok(true);
        }
        if pg_strcasecmp(&sval, "false") {
            return Ok(false);
        }
        if pg_strcasecmp(&sval, "on") {
            return Ok(true);
        }
        if pg_strcasecmp(&sval, "off") {
            return Ok(false);
        }
    }
    Err(syntax_error(format!("{defname} requires a Boolean value")))
}

/// `defGetString` over the projected value (the TypeName/List forms arrive
/// already rendered to text from the caller's real `def->arg`).
fn arg_get_string(defname: &str, arg: Option<&DefElemArg>) -> PgResult<String> {
    let Some(arg) = arg else {
        return Err(syntax_error(format!("{defname} requires a parameter")));
    };
    Ok(match arg {
        DefElemArg::Integer(i) => i.to_string(),
        DefElemArg::Float(f) => f.clone(),
        DefElemArg::Boolean(b) => if *b { "true" } else { "false" }.to_string(),
        DefElemArg::String(s) => s.clone(),
        DefElemArg::TypeName(s) => s.clone(),
        DefElemArg::List(s) => s.clone(),
        DefElemArg::AStar => "*".to_string(),
    })
}

// ---------------------------------------------------------------------------
// Install this crate's seams.
// ---------------------------------------------------------------------------

pub fn init_seams() {
    backend_commands_define_seams::def_get_string::set(seam_def_get_string);
    backend_commands_define_seams::def_get_boolean::set(seam_def_get_boolean);
}

// ---------------------------------------------------------------------------
// In-crate helpers (the C value-node macros and small utilities).
// ---------------------------------------------------------------------------

/// `def->defname` — the option name used in every error message. The owned tree
/// keeps `defname` as `Option<String>`; an absent name renders empty (the
/// parser always sets it).
fn defname(def: &types_parsenodes::DefElem) -> &str {
    def.defname.as_deref().unwrap_or("")
}

/// `if (def->arg == NULL) ereport(ERRCODE_SYNTAX_ERROR, "%s <detail>")` — the
/// shared parameter-presence guard, returning the borrowed argument node.
fn require_arg<'a>(def: &'a types_parsenodes::DefElem, detail: &str) -> PgResult<&'a Node> {
    def.arg
        .as_deref()
        .ok_or_else(|| syntax_error(format!("{} {}", defname(def), detail)))
}

/// Like [`require_arg`] but for the call sites whose missing-parameter message
/// is the same text as the per-tag default (e.g. "<name> requires a numeric
/// value").
fn require_arg_msg<'a>(
    def: &'a types_parsenodes::DefElem,
    message: impl FnOnce(&str) -> String,
) -> PgResult<&'a Node> {
    def.arg
        .as_deref()
        .ok_or_else(|| syntax_error(message(defname(def))))
}

/// `defGetString(def)` as `defGetBoolean`'s `default:` branch sees it (its
/// result only feeds `pg_strcasecmp`, never a `palloc`'d return value).
///
/// `defGetBoolean` is only ever reached for a boolean DefElem, whose value node
/// the grammar restricts to `Integer`/`String`/`TRUE_P`/`FALSE_P` — the
/// scalar forms below. The structural `TypeName`/`List`/`A_Star` forms cannot
/// occur here; were one to, `defGetString` would render it through the
/// namespace/parse-type renderers (needing a context this allocation-free path
/// has none of), and it could not equal a boolean keyword, so it correctly
/// falls through to the "requires a Boolean value" error. We surface that here
/// by leaving the rendered text empty for those forms.
fn def_get_string_text(def: &types_parsenodes::DefElem) -> PgResult<String> {
    let arg = require_arg(def, "requires a parameter")?;
    Ok(match arg {
        Node::Integer(i) => i.ival.to_string(),
        Node::Float(f) => float_fval(f).to_string(),
        Node::Boolean(b) => if b.boolval { "true" } else { "false" }.to_string(),
        Node::String(s) => string_sval(s).to_string(),
        Node::TypeName(_) | Node::List(_) | Node::A_Star => String::new(),
        _ => return Err(unrecognized_node_type(arg)),
    })
}

/// `castNode(Float, def->arg)->fval` — the `Float` value node's textual value.
/// An absent `fval` renders empty (a Float node always carries it in practice).
fn float_fval(value: &types_parsenodes::Float) -> &str {
    value.fval.as_deref().unwrap_or("")
}

/// `floatVal(def->arg)` — `atof(castNode(Float, def->arg)->fval)`
/// (`nodes/value.h`): parse the Float node's text as a `double`, matching C
/// `atof` (which yields `0.0` on an unparsable prefix rather than erroring).
fn floatVal(value: &types_parsenodes::Float) -> f64 {
    atof(float_fval(value))
}

/// `strVal(def->arg)` — the `String` value node's text. An absent `sval`
/// renders empty.
fn string_sval(value: &StringNode) -> &str {
    value.sval.as_deref().unwrap_or("")
}

/// `list_make1(def->arg)` for a quoted `String` value node — a one-element name
/// list (what `makeTypeNameFromNameList` / a qualified name consume).
fn list_make1_string(value: &StringNode) -> Vec<Node> {
    vec![Node::String(value.clone())]
}

/// Project a `List`'s `String`/`A_Star` cells to a `NameList`
/// (`&[Option<String>]`) for `NameListToString`. `None` is the `*` wildcard.
fn name_list(cells: &[Node]) -> PgResult<Vec<Option<String>>> {
    cells
        .iter()
        .map(|cell| match cell {
            Node::String(s) => Ok(Some(string_sval(s).to_string())),
            Node::A_Star => Ok(None),
            _ => Err(unrecognized_node_type(cell)),
        })
        .collect()
}

/// `pg_strcasecmp(a, b) == 0` — ASCII case-insensitive equality (the only use
/// of `pg_strcasecmp` in define.c).
fn pg_strcasecmp(left: &str, right: &str) -> bool {
    left.len() == right.len()
        && left
            .bytes()
            .zip(right.bytes())
            .all(|(l, r)| l.eq_ignore_ascii_case(&r))
}

/// `atof(s)` — C `atof` semantics: parse a leading floating-point prefix,
/// yielding `0.0` when there is no valid prefix (never erroring).
fn atof(s: &str) -> f64 {
    let trimmed = s.trim_start();
    let mut end = trimmed.len();
    while end > 0 {
        if let Ok(v) = trimmed[..end].parse::<f64>() {
            return v;
        }
        end -= 1;
    }
    0.0
}

/// `DirectFunctionCall1(int8in, CStringGetDatum(s))` — parse a C string to
/// `int64` (numutils `pg_strtoint64`, the body of `int8in`).
fn int8in(s: &str) -> PgResult<i64> {
    backend_utils_adt_numutils::pg_strtoint64(s)
}

/// `DirectFunctionCall1(oidin, CStringGetDatum(s))` — parse a C string to an
/// `Oid` (oid.c `oidin`: `uint32in_subr(s, NULL, "oid", ...)`).
fn oidin(s: &str) -> PgResult<Oid> {
    let (value, _) = backend_utils_adt_numutils::uint32in_subr(s, false, "oid", None)?;
    Ok(value as Oid)
}

/// `ereport(ERROR, errcode(ERRCODE_SYNTAX_ERROR), errmsg(msg))`.
fn syntax_error(message: impl Into<String>) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg(message)
        .into_error()
}

/// `elog(ERROR, "unrecognized node type: %d", (int) nodeTag(def->arg))`.
fn unrecognized_node_type(node: &Node) -> PgError {
    PgError::error(format!("unrecognized node type: {}", node.node_tag_name()))
}

/// `elog(ERROR, "unexpected node type in name list: %d", (int) nodeTag(str))`.
fn unexpected_node_type_in_name_list(node: &Node) -> PgError {
    PgError::error(format!("unexpected node type in name list: {}", node.node_tag_name()))
}

#[cfg(test)]
mod tests;
