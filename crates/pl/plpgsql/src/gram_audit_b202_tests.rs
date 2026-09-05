// Audit remediation batch b202-pl-plpgsql-3: grammar error-position
// witnesses (docs/conformance/audit-18.6/remediation/lanes/b202-pl-plpgsql-3.md).
// Each test asserts what C 18.6 does at the cited pl_gram.y site: the
// error carries parser_errposition(@1), which plpgsql_scanner_errposition
// (pl_scanner.c:504-515) turns into an internal position over the body.
use super::*;

fn int_type() -> PlType {
    PlType {
        typoid: 23, // INT4OID
        ttype: TypeKind::Scalar,
        typlen: 4,
        typbyval: true,
        typtype: b'b' as i8,
        collation: types_core::InvalidOid,
        typisarray: false,
        atttypmod: -1,
        typinput: types_core::InvalidOid,
        typioparam: types_core::InvalidOid,
        rec_ident: None,
    }
}


macro_rules! parser_for {
    ($cx:ident, $comp:ident, $src:expr) => {{
        let buf = mcx::slice_borrow_in($cx.mcx(), $src).unwrap();
        Parser {
            sc: PlScanner::new($cx.mcx(), buf),
            comp: &mut $comp,
            check_syntax: false,
            fn_rettype: 2278, // VOIDOID
            fn_retset: false,
            fn_prokind: b'f' as i8,
            fn_input_collation: types_core::InvalidOid,
            fn_is_trigger: false,
            out_param_varno: -1,
            scratch: $cx.mcx(),
            last_endtoken_loc: -1,
        }
    }};
}

// pl_gram.y decl_stmt (499-503): a block label after DECLARE is a syntax
// error positioned at the "<<" token — first in the section or after an
// earlier declaration (an ALIAS, which needs no catalog in a unit test).
#[test]
fn block_label_after_declare_is_positioned_at_the_label() {
    for (src, pos) in [
        (&b"declare <<lbl>> x int; begin end"[..], 9),
        (&b"declare a alias for v; <<lbl>> y int; begin end"[..], 24),
    ] {
        let cx = mcx::MemoryContext::new("plpgsql b202 declare label");
        let mut comp = crate::comp::CompState::new();
        comp.ns_push_label(Some("f"), LABEL_BLOCK);
        comp.build_variable("v", 1, int_type(), true).unwrap();
        let mut parser = parser_for!(cx, comp, src);
        let err = parser.parse_function_body().unwrap_err();
        assert_eq!(err.sqlstate, types_error::ERRCODE_SYNTAX_ERROR);
        assert_eq!(err.message, "block label must be placed before DECLARE, not after");
        assert_eq!(err.internal_position, Some(pos), "{:?}", core::str::from_utf8(src));
        assert_eq!(err.internal_query.as_deref(), Some(core::str::from_utf8(src).unwrap()));
        assert_eq!(err.cursor_position, None);
    }
}

// pl_gram.y decl_aliasitem (663, 678, 704): an ALIAS FOR target that does
// not exist is 42704 positioned at the target token (word, unreserved
// keyword or compound word).
#[test]
fn alias_for_an_unknown_variable_is_positioned_at_the_target() {
    for (src, name, pos) in [
        (&b"declare a alias for nosuch; begin end"[..], "nosuch", 21),
        (&b"declare a alias for nosuch.f; begin end"[..], "nosuch.f", 21),
        (&b"declare a alias for x.y.z; begin end"[..], "x.y.z", 21),
        (&b"declare a alias for row; begin end"[..], "row", 21),
        (&b"declare\n  a alias for nosuch; begin end"[..], "nosuch", 23),
    ] {
        let cx = mcx::MemoryContext::new("plpgsql b202 alias");
        let mut comp = crate::comp::CompState::new();
        let mut parser = parser_for!(cx, comp, src);
        let err = parser.parse_function_body().unwrap_err();
        assert_eq!(err.sqlstate, types_error::ERRCODE_UNDEFINED_OBJECT);
        assert_eq!(err.message, format!("variable \"{name}\" does not exist"));
        assert_eq!(err.internal_position, Some(pos), "{:?}", core::str::from_utf8(src));
        assert_eq!(err.internal_query.as_deref(), Some(core::str::from_utf8(src).unwrap()));
        assert_eq!(err.cursor_position, None);
    }
}
