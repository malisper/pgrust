// define.c slice (defGetString/defGetBoolean).
#![allow(non_snake_case)]

use core::fmt::Write;

use elog::ereport;
use mcx::{Mcx, PgString};
use types_error::{PgResult, ERRCODE_SYNTAX_ERROR, ERROR};
use types_nodes::parsenodes::DefElem;
use types_nodes::NodeTag;

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
            // TypeNameToString: gram's def_arg func_type wraps any simple
            // identifier as a TypeName.
            let tn = arg.as_type_name().expect("TypeName");
            if tn.names.is_nil() {
                panic!("defGetString (define.c): precooked TypeName needs format_type_be");
            }
            let mut s = PgString::new_in(mcx);
            for (i, n) in tn.names.iter().enumerate() {
                if i > 0 {
                    s.try_push_str(".")?;
                }
                s.try_push_str(n.as_string().expect("TypeName names").sval)?;
            }
            if tn.pct_type {
                s.try_push_str("%TYPE")?;
            }
            for _ in tn.arrayBounds.iter() {
                s.try_push_str("[]")?;
            }
            Ok(str_in(mcx, s.as_str())?)
        }
        t @ (NodeTag::T_List | NodeTag::T_A_Star) => panic!(
            "defGetString (define.c): {t:?} arg needs NameListToString (define lane)"
        ),
        t => panic!("unrecognized node type: {t:?}"),
    }
}

// No-alloc split from C (defGetString's only allocating arm is Integer, which
// this function handles directly), so log-level probes can call it mcx-free.
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
            t => panic!("defGetBoolean (define.c): {t:?} arg unported (define lane)"),
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
