//! define.c — DefElem option decoding for DDL commands.
#![allow(non_snake_case)]

use mcx::{Mcx, PgString};
use types_error::{PgError, PgResult, ERRCODE_SYNTAX_ERROR};
use types_nodes::list::NodeList;
use types_nodes::rawnodes::TypeName;
use types_nodes::NodeTag;
use types_nodes::{parsenodes::DefElem, Node};

#[track_caller]
#[cold]
fn syntax_err(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_SYNTAX_ERROR))
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
        t => panic!("unrecognized node type: {t:?}"),
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
        t => panic!("unrecognized node type: {t:?}"),
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
        panic!("unrecognized node type: {:?}", arg.node_tag());
    };
    for n in list.iter() {
        if n.as_string().is_none() {
            panic!("unexpected node type in name list: {:?}", n.node_tag());
        }
    }
    Ok(list)
}

// NameListToString (namespace.c): '.'-joined, no quoting.
pub fn NameListToString<'a>(mcx: Mcx<'a>, names: &NodeList<'_>) -> PgResult<PgString<'a>> {
    let mut out = PgString::new_in(mcx);
    for (i, n) in names.iter().enumerate() {
        if i > 0 {
            out.try_push('.')?;
        }
        let Some(s) = n.as_string() else {
            panic!("unexpected node type in name list: {:?}", n.node_tag());
        };
        out.try_push_str(s.sval)?;
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
}
