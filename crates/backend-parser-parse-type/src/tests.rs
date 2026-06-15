//! Unit tests for the pure (seam-free) `parse_type.c` logic: `TypeName`
//! rendering, the name-list helpers, and the typmod-decoration control flow
//! that does not reach a catalog/fmgr seam.

use super::*;

fn str_node(s: &str) -> Node {
    Node::String(types_parsenodes::StringNode {
        sval: Some(s.to_string()),
    })
}

fn tn_named(parts: &[&str]) -> TypeName {
    TypeName {
        names: parts.iter().map(|p| str_node(p)).collect(),
        ..TypeName::default()
    }
}

#[test]
fn typename_to_string_qualified() {
    let tn = tn_named(&["pg_catalog", "int4"]);
    assert_eq!(TypeNameToString(&tn).unwrap(), "pg_catalog.int4");
}

#[test]
fn typename_to_string_pct_type_and_array() {
    let mut tn = tn_named(&["mytab", "col"]);
    tn.pct_type = true;
    tn.arrayBounds = alloc::vec![Node::Integer(types_parsenodes::Integer { ival: -1 })];
    // names-as-is, then %TYPE decoration, then [] for the array bound.
    assert_eq!(TypeNameToString(&tn).unwrap(), "mytab.col%TYPE[]");
}

#[test]
fn typename_list_to_string_comma_joined() {
    let list = alloc::vec![tn_named(&["int4"]), tn_named(&["a", "text"])];
    assert_eq!(TypeNameListToString(&list).unwrap(), "int4,a.text");
}

#[test]
fn name_list_to_string_joins_with_dots() {
    let names = alloc::vec![str_node("a"), str_node("b"), str_node("c")];
    assert_eq!(NameListToString(&names).unwrap(), "a.b.c");
}

#[test]
fn name_list_to_string_renders_a_star() {
    let names = alloc::vec![str_node("a"), Node::A_Star];
    assert_eq!(NameListToString(&names).unwrap(), "a.*");
}

#[test]
fn typename_type_mod_returns_prespecified_when_no_typmods() {
    // No typmod expressions: returns typeName.typemod verbatim, no seam call.
    let mut tn = tn_named(&["int4"]);
    tn.typemod = 42;
    let typ = FormData_pg_type {
        oid: 23,
        typname: Default::default(),
        typnamespace: 0,
        typowner: 0,
        typlen: 4,
        typbyval: true,
        typtype: b'b' as i8,
        typcategory: 0,
        typispreferred: false,
        typisdefined: true,
        typdelim: 0,
        typrelid: 0,
        typsubscript: 0,
        typelem: 0,
        typarray: 0,
        typinput: 0,
        typoutput: 0,
        typreceive: 0,
        typsend: 0,
        typmodin: 0,
        typmodout: 0,
        typanalyze: 0,
        typalign: 0,
        typstorage: 0,
        typnotnull: false,
        typbasetype: 0,
        typtypmod: 0,
        typndims: 0,
        typcollation: 0,
    };
    assert_eq!(typenameTypeMod(None, &tn, typ).unwrap(), 42);
}

#[test]
fn type_accessors_read_the_form() {
    let typ = FormData_pg_type {
        oid: 99,
        typname: types_tuple::heaptuple::NameData::default(),
        typnamespace: 0,
        typowner: 0,
        typlen: 8,
        typbyval: true,
        typtype: b'b' as i8,
        typcategory: 0,
        typispreferred: false,
        typisdefined: true,
        typdelim: 0,
        typrelid: 1234,
        typsubscript: 0,
        typelem: 0,
        typarray: 0,
        typinput: 0,
        typoutput: 0,
        typreceive: 0,
        typsend: 0,
        typmodin: 0,
        typmodout: 0,
        typanalyze: 0,
        typalign: 0,
        typstorage: 0,
        typnotnull: false,
        typbasetype: 0,
        typtypmod: 0,
        typndims: 0,
        typcollation: 100,
    };
    assert_eq!(typeTypeId(Some(typ)).unwrap(), 99);
    assert_eq!(typeLen(typ), 8);
    assert!(typeByVal(typ));
    assert_eq!(typeTypeRelid(typ), 1234);
    assert_eq!(typeTypeCollation(typ), 100);
}

#[test]
fn type_type_id_null_errors() {
    assert!(typeTypeId(None).is_err());
}

#[test]
fn get_type_io_param_array_vs_scalar() {
    let mut typ = FormData_pg_type {
        oid: 23,
        typname: Default::default(),
        typnamespace: 0,
        typowner: 0,
        typlen: 4,
        typbyval: true,
        typtype: b'b' as i8,
        typcategory: 0,
        typispreferred: false,
        typisdefined: true,
        typdelim: 0,
        typrelid: 0,
        typsubscript: 0,
        typelem: 0,
        typarray: 0,
        typinput: 0,
        typoutput: 0,
        typreceive: 0,
        typsend: 0,
        typmodin: 0,
        typmodout: 0,
        typanalyze: 0,
        typalign: 0,
        typstorage: 0,
        typnotnull: false,
        typbasetype: 0,
        typtypmod: 0,
        typndims: 0,
        typcollation: 0,
    };
    // Scalar: typelem == 0 → own oid.
    assert_eq!(getTypeIOParam(&typ), 23);
    // Array: typelem set → element type.
    typ.typelem = 25;
    assert_eq!(getTypeIOParam(&typ), 25);
}
