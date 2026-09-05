//! define.c — DefElem option decoding for DDL commands.
#![allow(non_snake_case)]

use mcx::{Mcx, PgString};
use parser_small1::ParseState;
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR, ERRCODE_SYNTAX_ERROR};
use types_nodes::list::NodeList;
use types_nodes::rawnodes::TypeName;
use types_nodes::NodeTag;
use types_nodes::{parsenodes::DefElem, Node};

#[track_caller]
#[cold]
fn syntax_err(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_SYNTAX_ERROR))
}

// C's `default:` arms: elog(ERROR, "unrecognized node type: %d") — a
// catchable XX000, never a panic (define.c:59, :344, :361).
#[cold]
#[inline(never)]
fn unrecognized_node_type(tag: NodeTag) -> Box<PgError> {
    Box::new(
        PgError::error(format!("unrecognized node type: {}", tag as u16))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

// elog(ERROR, "unexpected node type in name list: %d") (define.c:350,
// namespace.c:3616).
#[cold]
#[inline(never)]
fn unexpected_name_list_node(tag: NodeTag) -> Box<PgError> {
    Box::new(
        PgError::error(format!("unexpected node type in name list: {}", tag as u16))
            .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

fn defname<'a>(def: &DefElem<'a>) -> &'a str {
    def.defname.unwrap_or("")
}

fn str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let bytes = mcx::slice_borrow_in(mcx, s.as_bytes())?;
    // SAFETY: bytes copied verbatim from a &str.
    Ok(unsafe { core::str::from_utf8_unchecked(bytes) })
}

pub fn defGetString<'mcx>(mcx: Mcx<'mcx>, def: &DefElem<'mcx>) -> PgResult<&'mcx str> {
    let Some(arg) = def.arg else {
        return Err(syntax_err(format!("{} requires a parameter", defname(def))));
    };
    Ok(match arg.node_tag() {
        NodeTag::T_Integer => str_in(mcx, &arg.as_integer().unwrap().ival.to_string())?,
        NodeTag::T_Float => arg.as_float().unwrap().fval,
        NodeTag::T_Boolean => {
            if arg.as_boolean().unwrap().boolval {
                "true"
            } else {
                "false"
            }
        }
        NodeTag::T_String => arg.as_string().unwrap().sval,
        NodeTag::T_TypeName => {
            let s = TypeNameToString(mcx, arg.as_variant::<TypeName>().unwrap())?;
            str_in(mcx, s.as_str())?
        }
        NodeTag::T_List => {
            let s = NameListToString(mcx, arg.as_list().unwrap())?;
            str_in(mcx, s.as_str())?
        }
        NodeTag::T_A_Star => "*",
        t => return Err(unrecognized_node_type(t)),
    })
}

pub fn defGetNumeric(def: &DefElem<'_>) -> PgResult<f64> {
    let err = || syntax_err(format!("{} requires a numeric value", defname(def)));
    let Some(arg) = def.arg else { return Err(err()) };
    match arg.node_tag() {
        NodeTag::T_Integer => Ok(arg.as_integer().unwrap().ival as f64),
        // floatVal: strtod semantics; grammar-produced Floats always parse.
        NodeTag::T_Float => arg.as_float().unwrap().fval.parse::<f64>().map_err(|_| err()),
        _ => Err(err()),
    }
}

// defGetObjectId (define.c:206-233): Integer, or a Float (values too large
// for int4 lex as Float) through oidin (oid.c:37 -> uint32in_subr).
pub fn defGetObjectId(def: &DefElem<'_>) -> PgResult<Oid> {
    let err = || syntax_err(format!("{} requires a numeric value", defname(def)));
    let Some(arg) = def.arg else { return Err(err()) };
    match arg.node_tag() {
        NodeTag::T_Integer => Ok(arg.as_integer().unwrap().ival as Oid),
        NodeTag::T_Float => {
            Ok(numutils::uint32in_subr(arg.as_float().unwrap().fval, false, "oid", None)?.0)
        }
        _ => Err(err()),
    }
}

pub fn defGetBoolean(def: &DefElem<'_>) -> PgResult<bool> {
    let Some(arg) = def.arg else {
        return Ok(true);
    };
    if arg.node_tag() == NodeTag::T_Integer {
        match arg.as_integer().unwrap().ival {
            0 => return Ok(false),
            1 => return Ok(true),
            _ => {}
        }
    } else {
        let ctx = mcx::MemoryContext::new("defGetBoolean");
        let sval = defGetString(ctx.mcx(), def)?;
        if sval.eq_ignore_ascii_case("true") || sval.eq_ignore_ascii_case("on") {
            return Ok(true);
        }
        if sval.eq_ignore_ascii_case("false") || sval.eq_ignore_ascii_case("off") {
            return Ok(false);
        }
    }
    Err(syntax_err(format!("{} requires a Boolean value", defname(def))))
}

pub fn defGetInt32(def: &DefElem<'_>) -> PgResult<i32> {
    if let Some(arg) = def.arg {
        if let Some(i) = arg.as_integer() {
            return Ok(i.ival);
        }
    }
    Err(syntax_err(format!("{} requires an integer value", defname(def))))
}

pub fn defGetInt64(def: &DefElem<'_>) -> PgResult<i64> {
    let err = || syntax_err(format!("{} requires a numeric value", defname(def)));
    let Some(arg) = def.arg else { return Err(err()) };
    match arg.node_tag() {
        NodeTag::T_Integer => Ok(arg.as_integer().unwrap().ival as i64),
        NodeTag::T_Float => adt_int8::int8in(arg.as_float().unwrap().fval, None),
        _ => Err(err()),
    }
}

pub fn defGetQualifiedName<'mcx>(
    mcx: Mcx<'mcx>,
    def: &DefElem<'mcx>,
) -> PgResult<&'mcx NodeList<'mcx>> {
    let Some(arg) = def.arg else {
        return Err(syntax_err(format!("{} requires a parameter", defname(def))));
    };
    match arg.node_tag() {
        NodeTag::T_TypeName => Ok(&arg.as_variant::<TypeName>().unwrap().names),
        NodeTag::T_List => Ok(arg.as_list().unwrap()),
        NodeTag::T_String => {
            // Allow quoted name for backwards compatibility.
            let list = NodeList::make1(mcx, arg)?;
            Ok(Node::mk_list(mcx, list)?.as_list().unwrap())
        }
        _ => Err(syntax_err(format!("argument of {} must be a name", defname(def)))),
    }
}

pub fn defGetTypeName<'mcx>(mcx: Mcx<'mcx>, def: &DefElem<'mcx>) -> PgResult<&'mcx TypeName<'mcx>> {
    let Some(arg) = def.arg else {
        return Err(syntax_err(format!("{} requires a parameter", defname(def))));
    };
    match arg.node_tag() {
        NodeTag::T_TypeName => Ok(arg.as_variant::<TypeName>().unwrap()),
        NodeTag::T_String => {
            // makeTypeNameFromNameList(list_make1(def->arg))
            let mut tn = Node::build::<TypeName>(mcx)?;
            tn.names = NodeList::make1(mcx, arg)?;
            tn.typemod = -1;
            tn.location = -1;
            Ok(tn.seal_ref())
        }
        _ => Err(syntax_err(format!("argument of {} must be a type name", defname(def)))),
    }
}

pub fn defGetTypeLength(def: &DefElem<'_>) -> PgResult<i32> {
    let ctx = mcx::MemoryContext::new("defGetTypeLength");
    let mcx = ctx.mcx();
    let Some(arg) = def.arg else {
        return Err(syntax_err(format!("{} requires a parameter", defname(def))));
    };
    match arg.node_tag() {
        NodeTag::T_Integer => return Ok(arg.as_integer().unwrap().ival),
        NodeTag::T_Float => {
            return Err(syntax_err(format!("{} requires an integer value", defname(def))))
        }
        NodeTag::T_String => {
            if arg.as_string().unwrap().sval.eq_ignore_ascii_case("variable") {
                return Ok(-1);
            }
        }
        NodeTag::T_TypeName => {
            let s = TypeNameToString(mcx, arg.as_variant::<TypeName>().unwrap())?;
            if s.as_str().eq_ignore_ascii_case("variable") {
                return Ok(-1);
            }
        }
        NodeTag::T_List => {}
        t => return Err(unrecognized_node_type(t)),
    }
    Err(syntax_err(format!(
        "invalid argument for {}: \"{}\"",
        defname(def),
        defGetString(mcx, def)?
    )))
}

pub fn defGetStringList<'mcx>(def: &DefElem<'mcx>) -> PgResult<&'mcx NodeList<'mcx>> {
    let Some(arg) = def.arg else {
        return Err(syntax_err(format!("{} requires a parameter", defname(def))));
    };
    let Some(list) = arg.as_list() else {
        return Err(unrecognized_node_type(arg.node_tag()));
    };
    for n in list.iter() {
        if n.as_string().is_none() {
            return Err(unexpected_name_list_node(n.node_tag()));
        }
    }
    Ok(list)
}

// errorConflictingDefElem (define.c:371-377): ERRCODE_SYNTAX_ERROR with
// parser_errposition(pstate, defel->location) — no cursor on a NULL pstate,
// a pstate without source text, or a negative location.
#[cold]
#[inline(never)]
pub fn errorConflictingDefElem(
    defel: &DefElem<'_>,
    pstate: Option<&ParseState<'_, '_>>,
) -> Box<PgError> {
    let mut e =
        PgError::error("conflicting or redundant options").with_sqlstate(ERRCODE_SYNTAX_ERROR);
    if let Some(ps) = pstate {
        let pos =
            parser_small1::parser_errposition(ps, defel.location, mbutils::GetDatabaseEncoding());
        if pos > 0 {
            e.cursor_position = Some(pos);
        }
    }
    Box::new(e)
}

// NameListToString (namespace.c:3597-3621): '.'-joined, no quoting; A_Star
// joins as '*'.
pub fn NameListToString<'a>(mcx: Mcx<'a>, names: &NodeList<'_>) -> PgResult<PgString<'a>> {
    let mut out = PgString::new_in(mcx);
    for (i, n) in names.iter().enumerate() {
        if i > 0 {
            out.try_push('.')?;
        }
        if let Some(s) = n.as_string() {
            out.try_push_str(s.sval)?;
        } else if n.node_tag() == NodeTag::T_A_Star {
            out.try_push('*')?;
        } else {
            return Err(unexpected_name_list_node(n.node_tag()));
        }
    }
    Ok(out)
}

// TypeNameToString (parse_type.c): possibly-qualified name as-is, or the
// internally-specified type via format_type_be, plus the decoration
// LookupTypeName considers.
pub fn TypeNameToString<'a>(mcx: Mcx<'a>, tn: &TypeName<'_>) -> PgResult<PgString<'a>> {
    let mut out = if tn.names.is_nil() {
        let mut s = PgString::new_in(mcx);
        s.try_push_str(&format_type::format_type_be(tn.typeOid)?)?;
        s
    } else {
        NameListToString(mcx, &tn.names)?
    };
    if tn.pct_type {
        out.try_push_str("%TYPE")?;
    }
    // C appendTypeNameToBuffer: "[]" appended ONCE when arrayBounds != NIL,
    // regardless of dimension count.
    if !tn.arrayBounds.is_nil() {
        out.try_push_str("[]")?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use types_nodes::Node;

    // TypeNameToString decoration parity (appendTypeNameToBuffer,
    // parse_type.c): %TYPE suffix and "[]" appended once regardless of the
    // number of array bounds. The empty-names arm routes to format_type_be
    // (previously a release-effective assert).
    #[test]
    fn type_name_to_string_decorations_match_c() {
        let ctx = mcx::MemoryContext::new("commands_define-test");
        let mcx = ctx.mcx();
        let string_node =
            |s: &'static str| Node::mk(mcx, types_nodes::String { sval: s }).unwrap();
        let bound = |n: i32| Node::mk(mcx, types_nodes::Integer { ival: n }).unwrap();

        let tn = TypeName {
            names: NodeList::from_slice(mcx, &[string_node("s"), string_node("t")]).unwrap(),
            arrayBounds: NodeList::from_slice(mcx, &[bound(-1), bound(-1)]).unwrap(),
            ..TypeName::default()
        };
        assert_eq!(TypeNameToString(mcx, &tn).unwrap().as_str(), "s.t[]");

        let tn = TypeName {
            names: NodeList::from_slice(mcx, &[string_node("c")]).unwrap(),
            pct_type: true,
            ..TypeName::default()
        };
        assert_eq!(TypeNameToString(mcx, &tn).unwrap().as_str(), "c%TYPE");
    }

    fn defel_with<'m>(mcx: Mcx<'m>, name: &'m str, arg: Option<Node<'m>>) -> DefElem<'m> {
        DefElem {
            defnamespace: None,
            defname: Some(name),
            arg,
            defaction: types_nodes::parsenodes::DefElemAction::DEFELEM_UNSPEC,
            location: -1,
        }
    }

    fn assert_xx000(e: &PgError, msg: &str) {
        assert_eq!(e.message(), msg);
        assert_eq!(e.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
    }

    // define.c:59 / :361 / :350 / namespace.c:3616 `default:` arms are
    // elog(ERROR, "unrecognized node type: %d") / "unexpected node type in
    // name list: %d" — catchable XX000 with the numeric tag, never a panic.
    #[test]
    fn unexpected_node_tags_are_catchable_xx000() {
        let ctx = mcx::MemoryContext::new("commands_define-test");
        let mcx = ctx.mcx();
        let bits = Node::mk(mcx, types_nodes::BitString { bsval: "b101" }).unwrap();
        let bits_tag = format!("unrecognized node type: {}", NodeTag::T_BitString as u16);

        // defGetString (define.c:59)
        let def = defel_with(mcx, "opt", Some(bits));
        assert_xx000(&defGetString(mcx, &def).err().unwrap(), &bits_tag);

        // defGetTypeLength (define.c:361)
        assert_xx000(&defGetTypeLength(&def).err().unwrap(), &bits_tag);

        // defGetStringList (define.c:344): non-List arg
        assert_xx000(&defGetStringList(&def).err().unwrap(), &bits_tag);

        // defGetStringList (define.c:350): List with a non-String member
        let int_node = Node::mk(mcx, types_nodes::Integer { ival: 7 }).unwrap();
        let list = Node::mk_list(mcx, NodeList::make1(mcx, int_node).unwrap()).unwrap();
        let def = defel_with(mcx, "opt", Some(list));
        assert_xx000(
            &defGetStringList(&def).err().unwrap(),
            &format!("unexpected node type in name list: {}", NodeTag::T_Integer as u16),
        );

        // NameListToString (namespace.c:3616): same message; A_Star joins as
        // '*' (namespace.c:3613) instead of erroring.
        let names = NodeList::make1(mcx, int_node).unwrap();
        assert_xx000(
            &NameListToString(mcx, &names).err().unwrap(),
            &format!("unexpected node type in name list: {}", NodeTag::T_Integer as u16),
        );
        let star = Node::mk(mcx, types_nodes::A_Star).unwrap();
        let s = Node::mk(mcx, types_nodes::String { sval: "s" }).unwrap();
        let names = NodeList::from_slice(mcx, &[s, star]).unwrap();
        assert_eq!(NameListToString(mcx, &names).unwrap().as_str(), "s.*");
    }

    // defGetObjectId (define.c:206-233): Integer verbatim, Float through
    // oidin (22003 out of range above uint32), anything else 42601.
    #[test]
    fn def_get_object_id_matches_c() {
        let ctx = mcx::MemoryContext::new("commands_define-test");
        let mcx = ctx.mcx();
        let float = |v: &'static str| Node::mk(mcx, types_nodes::Float { fval: v }).unwrap();
        let sixteen = Node::mk(mcx, types_nodes::Integer { ival: 16 }).unwrap();
        let def = defel_with(mcx, "oid", Some(sixteen));
        assert_eq!(defGetObjectId(&def).unwrap(), 16);
        let def = defel_with(mcx, "oid", Some(float("3000000000")));
        assert_eq!(defGetObjectId(&def).unwrap(), 3_000_000_000);
        let def = defel_with(mcx, "oid", Some(float("4294967296")));
        let e = defGetObjectId(&def).err().unwrap();
        assert_eq!(e.message(), "value \"4294967296\" is out of range for type oid");
        assert_eq!(e.sqlstate(), types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        let text = Node::mk(mcx, types_nodes::String { sval: "abc" }).unwrap();
        let def = defel_with(mcx, "oid", Some(text));
        let e = defGetObjectId(&def).err().unwrap();
        assert_eq!(e.message(), "oid requires a numeric value");
        assert_eq!(e.sqlstate(), ERRCODE_SYNTAX_ERROR);
        let def = defel_with(mcx, "oid", None);
        assert_eq!(defGetObjectId(&def).err().unwrap().message(), "oid requires a numeric value");
    }

    // errorConflictingDefElem (define.c:371-377): the cursor comes from
    // parser_errposition(pstate, defel->location) — character-based, absent
    // without a pstate/source text or with a negative location.
    #[test]
    fn error_conflicting_def_elem_carries_cursor() {
        let ctx = mcx::MemoryContext::new("commands_define-test");
        let mcx = ctx.mcx();
        let src = "CREATE DATABASE \"d\u{e9}\" WITH ENCODING 'UTF8' ENCODING 'UTF8'";
        let second = src.rfind("ENCODING").unwrap() as i32;
        let mut def = defel_with(mcx, "encoding", None);
        def.location = second;
        let mut pstate = parser_small1::make_parsestate(mcx, None);
        pstate.p_sourcetext = Some(src.as_bytes());

        let e = errorConflictingDefElem(&def, Some(&pstate));
        assert_eq!(e.message(), "conflicting or redundant options");
        assert_eq!(e.sqlstate(), ERRCODE_SYNTAX_ERROR);
        let expect = if mbutils::GetDatabaseEncoding() == wchar::PG_UTF8 {
            src[..second as usize].chars().count() as i32 + 1
        } else {
            second + 1
        };
        assert_eq!(e.cursor_position, Some(expect));

        assert_eq!(errorConflictingDefElem(&def, None).cursor_position, None);
        def.location = -1;
        assert_eq!(errorConflictingDefElem(&def, Some(&pstate)).cursor_position, None);
        pstate.p_sourcetext = None;
        def.location = second;
        assert_eq!(errorConflictingDefElem(&def, Some(&pstate)).cursor_position, None);
    }
}
