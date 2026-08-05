// define.c slice (defGetString/defGetBoolean).
#![allow(non_snake_case)]

use core::fmt::Write;

use elog::ereport;
use mcx::{Mcx, PgString};
use types_error::{PgError, PgResult, ERRCODE_SYNTAX_ERROR, ERROR};
use types_nodes::parsenodes::DefElem;
use types_nodes::rawnodes::TypeName;
use types_nodes::{NodeList, NodeTag};

// C: elog(ERROR, "unrecognized node type: %d") — a catchable internal error.
#[cold]
#[inline(never)]
fn unrecognized_node_type(t: NodeTag) -> Box<PgError> {
    Box::new(PgError::error(format!("unrecognized node type: {t:?}")))
}

/// TypeNameToString (parse_type.c) over grammar-produced TypeNames (gram's
/// def_arg func_type wraps any simple identifier as a TypeName). The
/// pre-resolved empty-names form needs format_type_be and stays loud.
pub fn type_name_string(tn: &TypeName<'_>) -> String {
    if tn.names.is_nil() {
        panic!("defGetString (define.c): precooked TypeName needs format_type_be");
    }
    let mut s = String::new();
    for (i, n) in tn.names.iter().enumerate() {
        if i > 0 {
            s.push('.');
        }
        s.push_str(n.as_string().expect("TypeName names").sval);
    }
    if tn.pct_type {
        s.push_str("%TYPE");
    }
    // C appendTypeNameToBuffer (parse_type.c): "[]" appended ONCE
    // when arrayBounds != NIL, regardless of dimension count.
    if !tn.arrayBounds.is_nil() {
        s.push_str("[]");
    }
    s
}

/// NameListToString (namespace.c).
pub fn name_list_string(names: &NodeList<'_>) -> PgResult<String> {
    let mut s = String::new();
    for (i, n) in names.iter().enumerate() {
        if i > 0 {
            s.push('.');
        }
        match n.node_tag() {
            NodeTag::T_String => s.push_str(n.as_string().unwrap().sval),
            NodeTag::T_A_Star => s.push('*'),
            t => {
                return Err(Box::new(PgError::error(format!(
                    "unexpected node type in name list: {t:?}"
                ))))
            }
        }
    }
    Ok(s)
}

pub fn defGetString<'r, 'mcx: 'r, 'a: 'r>(
    mcx: Mcx<'mcx>,
    def: &DefElem<'a>,
) -> PgResult<&'r str> {
    let defname = def.defname.unwrap_or("");
    let Some(arg) = def.arg else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(format!("{defname} requires a parameter"))
            .into_error()
            .into());
    };
    match arg.node_tag() {
        NodeTag::T_Integer => {
            let mut s = PgString::new_in(mcx);
            write!(s, "{}", arg.as_integer().unwrap().ival).expect("PgString write");
            Ok(str_in(mcx, s.as_str())?)
        }
        NodeTag::T_Float => Ok(arg.as_float().unwrap().fval),
        NodeTag::T_Boolean => Ok(if arg.as_boolean().unwrap().boolval { "true" } else { "false" }),
        NodeTag::T_String => Ok(arg.as_string().unwrap().sval),
        NodeTag::T_TypeName => {
            Ok(str_in(mcx, &type_name_string(arg.as_type_name().expect("TypeName")))?)
        }
        NodeTag::T_List => {
            Ok(str_in(mcx, &name_list_string(arg.as_list().expect("List"))?)?)
        }
        NodeTag::T_A_Star => Ok("*"),
        t => Err(unrecognized_node_type(t)),
    }
}

// No-alloc split from C (defGetString's only mcx-allocating arm is Integer,
// which this function handles directly), so log-level probes can call it
// mcx-free.
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
        // C's default arm: sval = defGetString(def).
        let owned;
        let sval = match arg.node_tag() {
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
                owned = type_name_string(arg.as_type_name().expect("TypeName"));
                &owned
            }
            NodeTag::T_List => {
                owned = name_list_string(arg.as_list().expect("List"))?;
                &owned
            }
            NodeTag::T_A_Star => "*",
            t => return Err(unrecognized_node_type(t)),
        };
        if sval.eq_ignore_ascii_case("true") || sval.eq_ignore_ascii_case("on") {
            return Ok(true);
        }
        if sval.eq_ignore_ascii_case("false") || sval.eq_ignore_ascii_case("off") {
            return Ok(false);
        }
    }
    Err(ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg(format!("{} requires a Boolean value", def.defname.unwrap_or("")))
        .into_error()
        .into())
}

pub fn str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let bytes = mcx::slice_borrow_in(mcx, s.as_bytes())?;
    // SAFETY: byte-for-byte copy of a &str.
    Ok(unsafe { core::str::from_utf8_unchecked(bytes) })
}

#[cfg(test)]
mod tests {
    use super::*;
    use types_nodes::Node;

    fn string_node<'mcx>(mcx: Mcx<'mcx>, s: &'mcx str) -> Node<'mcx> {
        Node::mk(mcx, types_nodes::String { sval: s }).unwrap()
    }

    fn type_name_node<'mcx>(mcx: Mcx<'mcx>, parts: &[&'mcx str]) -> Node<'mcx> {
        let nodes: Vec<Node<'mcx>> = parts.iter().map(|p| string_node(mcx, p)).collect();
        let tn = TypeName {
            names: NodeList::from_slice(mcx, &nodes).unwrap(),
            ..TypeName::default()
        };
        Node::mk(mcx, tn).unwrap()
    }

    // defGetString/defGetBoolean T_TypeName and T_List arms (previously
    // panicked): C converts via TypeNameToString / NameListToString, then
    // defGetBoolean matches the boolean words or raises a catchable error.
    #[test]
    fn def_arg_type_name_and_list_arms_match_c() {
        let ctx = mcx::MemoryContext::new("define-test");
        let mcx = ctx.mcx();

        // A double-quoted "true" parses as func_type -> TypeName; C accepts
        // it as a boolean.
        let def = DefElem {
            defname: Some("opt"),
            arg: Some(type_name_node(mcx, &["true"])),
            ..DefElem::default()
        };
        assert_eq!(defGetString(mcx, &def).unwrap(), "true");
        assert!(defGetBoolean(&def).unwrap());

        // A qualified name renders dotted and is not a boolean: C's
        // "%s requires a Boolean value" (42601).
        let names: Vec<Node<'_>> = vec![string_node(mcx, "a"), string_node(mcx, "b")];
        let def = DefElem {
            defname: Some("opt"),
            arg: Some(Node::mk(mcx, NodeList::from_slice(mcx, &names).unwrap()).unwrap()),
            ..DefElem::default()
        };
        assert_eq!(defGetString(mcx, &def).unwrap(), "a.b");
        let e = defGetBoolean(&def).unwrap_err();
        assert_eq!(e.message(), "opt requires a Boolean value");
        assert_eq!(e.sqlstate(), ERRCODE_SYNTAX_ERROR);

        let def = DefElem {
            defname: Some("opt"),
            arg: Some(type_name_node(mcx, &["off"])),
            ..DefElem::default()
        };
        assert!(!defGetBoolean(&def).unwrap());
    }
}
