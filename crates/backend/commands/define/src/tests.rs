//! Unit tests for the pure value-extraction logic of `define.c`. The
//! cross-subsystem renderers (`TypeNameToString`, `makeTypeNameFromNameList`,
//! `parser_errposition`) and the bare-`List` `NameListToString` path are not
//! exercised here (their owners are unported and the seams panic until then);
//! these cover the in-crate node-tag dispatch, the integer/Boolean/keyword
//! recognition, and the missing-parameter / wrong-type error paths.

use super::*;
use ::mcx::MemoryContext;
use ::parsenodes::{Boolean, DefElem, Float, Integer};

fn defelem(arg: Option<Node>) -> DefElem {
    DefElem {
        defnamespace: None,
        defname: Some("opt".to_string()),
        arg: arg.map(Box::new),
        defaction: ::parsenodes::DEFELEM_UNSPEC,
        location: -1,
    }
}

fn int_node(v: i32) -> Node {
    Node::Integer(Integer { ival: v })
}

fn str_node(v: &str) -> Node {
    Node::String(StringNode { sval: Some(v.to_string()) })
}

fn float_node(v: &str) -> Node {
    Node::Float(Float { fval: Some(v.to_string()) })
}

#[test]
fn get_string_renders_each_scalar() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    assert_eq!(defGetString(mcx, &defelem(Some(int_node(42)))).unwrap().as_str(), "42");
    assert_eq!(defGetString(mcx, &defelem(Some(float_node("1.5")))).unwrap().as_str(), "1.5");
    assert_eq!(
        defGetString(mcx, &defelem(Some(Node::Boolean(Boolean { boolval: true })))).unwrap().as_str(),
        "true"
    );
    assert_eq!(defGetString(mcx, &defelem(Some(str_node("hi")))).unwrap().as_str(), "hi");
    assert_eq!(defGetString(mcx, &defelem(Some(Node::A_Star))).unwrap().as_str(), "*");
}

#[test]
fn get_string_missing_param_errors() {
    let cx = MemoryContext::new("t");
    assert!(defGetString(cx.mcx(), &defelem(None)).is_err());
}

#[test]
fn get_numeric_int_and_float() {
    assert_eq!(defGetNumeric(&defelem(Some(int_node(7)))).unwrap(), 7.0);
    assert_eq!(defGetNumeric(&defelem(Some(float_node("2.25")))).unwrap(), 2.25);
    assert!(defGetNumeric(&defelem(Some(str_node("x")))).is_err());
    assert!(defGetNumeric(&defelem(None)).is_err());
}

#[test]
fn get_boolean_recognizes_all_forms() {
    assert!(defGetBoolean(&defelem(None)).unwrap()); // absent => true
    assert!(!defGetBoolean(&defelem(Some(int_node(0)))).unwrap());
    assert!(defGetBoolean(&defelem(Some(int_node(1)))).unwrap());
    assert!(defGetBoolean(&defelem(Some(str_node("TRUE")))).unwrap());
    assert!(!defGetBoolean(&defelem(Some(str_node("False")))).unwrap());
    assert!(defGetBoolean(&defelem(Some(str_node("on")))).unwrap());
    assert!(!defGetBoolean(&defelem(Some(str_node("OFF")))).unwrap());
    assert!(defGetBoolean(&defelem(Some(int_node(2)))).is_err());
    assert!(defGetBoolean(&defelem(Some(str_node("maybe")))).is_err());
}

#[test]
fn get_int32_only_integer() {
    assert_eq!(defGetInt32(&defelem(Some(int_node(-5)))).unwrap(), -5);
    assert!(defGetInt32(&defelem(Some(float_node("1.0")))).is_err());
    assert!(defGetInt32(&defelem(None)).is_err());
}

#[test]
fn get_int64_integer_and_float_string() {
    assert_eq!(defGetInt64(&defelem(Some(int_node(9)))).unwrap(), 9);
    // A Float-lexed literal too big for int4, but a valid int8.
    assert_eq!(
        defGetInt64(&defelem(Some(float_node("5000000000")))).unwrap(),
        5_000_000_000
    );
    assert!(defGetInt64(&defelem(Some(str_node("x")))).is_err());
}

#[test]
fn get_object_id_integer_and_float_string() {
    assert_eq!(defGetObjectId(&defelem(Some(int_node(1259)))).unwrap(), 1259);
    assert_eq!(
        defGetObjectId(&defelem(Some(float_node("4000000000")))).unwrap(),
        4_000_000_000
    );
    assert!(defGetObjectId(&defelem(Some(str_node("x")))).is_err());
}

#[test]
fn get_type_length_integer_and_variable() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    assert_eq!(defGetTypeLength(mcx, &defelem(Some(int_node(4)))).unwrap(), 4);
    assert_eq!(defGetTypeLength(mcx, &defelem(Some(str_node("variable")))).unwrap(), -1);
    assert_eq!(defGetTypeLength(mcx, &defelem(Some(str_node("VARIABLE")))).unwrap(), -1);
    // Float => integer-value error.
    assert!(defGetTypeLength(mcx, &defelem(Some(float_node("1.0")))).is_err());
    // A non-"variable" string => invalid-argument error.
    assert!(defGetTypeLength(mcx, &defelem(Some(str_node("nope")))).is_err());
}

#[test]
fn get_qualified_name_forms() {
    // String => one-element name list.
    let q = defGetQualifiedName(&defelem(Some(str_node("foo")))).unwrap();
    assert_eq!(q.len(), 1);
    assert_eq!(q[0].as_string().unwrap().sval.as_deref(), Some("foo"));
    // List => the cells as-is.
    let list = Node::List(vec![str_node("a"), str_node("b")]);
    let q = defGetQualifiedName(&defelem(Some(list))).unwrap();
    assert_eq!(q.len(), 2);
    // wrong type => error.
    assert!(defGetQualifiedName(&defelem(Some(int_node(1)))).is_err());
}

#[test]
fn get_string_list_validates_cells() {
    let de = defelem(Some(Node::List(vec![str_node("a"), str_node("b")])));
    let cells = defGetStringList(&de).unwrap();
    assert_eq!(cells.len(), 2);
    // non-String cell => error.
    let bad = Node::List(vec![str_node("a"), int_node(3)]);
    assert!(defGetStringList(&defelem(Some(bad))).is_err());
    // non-List arg => error.
    assert!(defGetStringList(&defelem(Some(int_node(1)))).is_err());
}

#[test]
fn seam_string_and_boolean_projection() {
    let cx = MemoryContext::new("t");
    let s = seam_def_get_string(cx.mcx(), "opt".to_string(), Some(DefElemArg::Integer(12)))
        .unwrap();
    assert_eq!(s.as_str(), "12");
    assert!(seam_def_get_boolean("opt".to_string(), None).unwrap());
    assert!(!seam_def_get_boolean("opt".to_string(), Some(DefElemArg::Integer(0))).unwrap());
    assert!(seam_def_get_boolean("opt".to_string(), Some(DefElemArg::String("on".to_string()))).unwrap());
    assert!(seam_def_get_boolean("opt".to_string(), Some(DefElemArg::String("x".to_string()))).is_err());
}
