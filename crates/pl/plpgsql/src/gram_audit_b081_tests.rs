// Audit remediation batch b081-pl-plpgsql-2: parser / scanner / record-shape
// witnesses (docs/conformance/audit-18.6/remediation/lanes/b081-pl-plpgsql-2.md).
// Each test asserts what C 18.6 does at the cited pl_gram.y / pl_scanner.c /
// expandedrecord.c site.
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

fn refcursor_type() -> PlType {
    let mut t = int_type();
    t.typoid = REFCURSOROID;
    t
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

// plpgsql_scanner_errposition (pl_scanner.c:504-515): every parser error is
// positioned as an INTERNAL error over the function body — internal
// position + internal query (scanorig) — never as an outer-statement
// cursor. (Validation later transposes it onto CREATE FUNCTION; an
// execution-time compile keeps it, which is what psql prints as
// LINE/QUERY over the body.)
#[test]
fn parser_errors_are_internal_positions_over_the_function_body() {
    let src = b"begin for x[1], y in select 1, 2 loop end loop; end";
    let cx = mcx::MemoryContext::new("plpgsql b081 errposition");
    let mut comp = crate::comp::CompState::new();
    let mut parser = parser_for!(cx, comp, src);
    let err = parser.parse_function_body().unwrap_err();
    assert_eq!(err.sqlstate, types_error::ERRCODE_SYNTAX_ERROR);
    assert_eq!(err.message, "syntax error at or near \"[\"");
    assert_eq!(err.internal_position, Some(12));
    assert_eq!(err.internal_query.as_deref(), Some(core::str::from_utf8(src).unwrap()));
    assert_eq!(err.cursor_position, None);

    // A grammar-raised error (parser_errposition(@1)) goes the same way.
    let src = b"begin open nosuch; end";
    let cx = mcx::MemoryContext::new("plpgsql b081 errposition 2");
    let mut comp = crate::comp::CompState::new();
    let mut parser = parser_for!(cx, comp, src);
    let err = parser.parse_function_body().unwrap_err();
    assert_eq!(err.message, "\"nosuch\" is not a known variable");
    assert_eq!(err.internal_position, Some(12));
    assert_eq!(err.internal_query.as_deref(), Some(core::str::from_utf8(src).unwrap()));
    assert_eq!(err.cursor_position, None);
}

// pl_gram.y proc_sect has no EOF rule: bison's plain "syntax error" goes
// through plpgsql_yyerror (pl_scanner.c:538-546) as "syntax error at end
// of input" — not "unexpected end of function definition", which C only
// uses where the grammar reads tokens by hand.
#[test]
fn end_of_input_inside_a_block_is_a_plain_syntax_error() {
    for src in [&b"begin "[..], &b"begin if true then "[..], &b"begin begin "[..]] {
        let cx = mcx::MemoryContext::new("plpgsql b081 eof");
        let mut comp = crate::comp::CompState::new();
        let mut parser = parser_for!(cx, comp, src);
        let err = parser.parse_function_body().unwrap_err();
        assert_eq!(err.sqlstate, types_error::ERRCODE_SYNTAX_ERROR);
        assert_eq!(err.message, "syntax error at end of input", "{:?}", core::str::from_utf8(src));
        assert_eq!(err.internal_position, Some(src.len() as i32 + 1));
    }
    // read_sql_construct-style hand reads keep C's own message.
    let cx = mcx::MemoryContext::new("plpgsql b081 eof raise");
    let mut comp = crate::comp::CompState::new();
    let mut parser = parser_for!(cx, comp, b"begin raise notice 'x' using ");
    let err = parser.parse_function_body().unwrap_err();
    assert_eq!(err.message, "unexpected end of function definition at end of input");
}

// pl_gram.y getdiag_target (1163-1174): an array element is "not a scalar
// variable" (42601, positioned at the variable), detected by peeking '['.
#[test]
fn get_diagnostics_into_an_array_element_is_not_a_scalar_variable() {
    let cx = mcx::MemoryContext::new("plpgsql b081 getdiag");
    let mut comp = crate::comp::CompState::new();
    comp.ns_push_label(Some("f"), LABEL_BLOCK);
    comp.build_variable("arr", 1, int_type(), true).unwrap();
    let mut parser = parser_for!(cx, comp, b"begin get diagnostics arr[1] = row_count; end");
    let err = parser.parse_function_body().unwrap_err();
    assert_eq!(err.sqlstate, types_error::ERRCODE_SYNTAX_ERROR);
    assert_eq!(err.message, "\"arr\" is not a scalar variable");
    assert_eq!(err.internal_position, Some(23));

    // Control: the scalar itself is fine.
    let cx = mcx::MemoryContext::new("plpgsql b081 getdiag control");
    let mut comp = crate::comp::CompState::new();
    comp.ns_push_label(Some("f"), LABEL_BLOCK);
    comp.build_variable("n", 1, int_type(), true).unwrap();
    let mut parser = parser_for!(cx, comp, b"begin get diagnostics n = row_count; end");
    parser.parse_function_body().unwrap();
}

// pl_gram.y cursor_variable (2282-2303): anything but a plain Var — a
// record, a record field, an array element (next token '[') — is "cursor
// variable must be a simple variable" (42804), checked before the type.
#[test]
fn cursor_variable_must_be_a_simple_variable() {
    let cases: [(&[u8], i32); 4] = [
        (b"begin open c[1]; end", 12),
        (b"begin close c[1]; end", 13),
        (b"begin open r; end", 12),
        (b"begin open r.f; end", 12),
    ];
    let msg = "cursor variable must be a simple variable";
    for (src, pos) in cases {
        let cx = mcx::MemoryContext::new("plpgsql b081 cursor variable");
        let mut comp = crate::comp::CompState::new();
        comp.ns_push_label(Some("f"), LABEL_BLOCK);
        comp.build_variable("c", 1, refcursor_type(), true).unwrap();
        comp.build_rec("r", 1, true);
        let mut parser = parser_for!(cx, comp, src);
        let err = parser.parse_function_body().unwrap_err();
        assert_eq!(err.sqlstate, types_error::ERRCODE_DATATYPE_MISMATCH, "{:?}", core::str::from_utf8(src));
        assert_eq!(err.message, msg, "{:?}", core::str::from_utf8(src));
        assert_eq!(err.internal_position, Some(pos), "{:?}", core::str::from_utf8(src));
    }
    // A non-cursor Var keeps its own message.
    let cx = mcx::MemoryContext::new("plpgsql b081 cursor variable type");
    let mut comp = crate::comp::CompState::new();
    comp.ns_push_label(Some("f"), LABEL_BLOCK);
    comp.build_variable("n", 1, int_type(), true).unwrap();
    let mut parser = parser_for!(cx, comp, b"begin open n; end");
    let err = parser.parse_function_body().unwrap_err();
    assert_eq!(err.message, "variable \"n\" must be of type cursor or refcursor");
}

// pl_gram.y for_control: a bad loop target is 42601 for a query loop
// (1592-1596) and 42804 for an EXECUTE loop (1399-1403) — same message.
#[test]
fn query_for_loop_target_errors_carry_the_c_sqlstates() {
    let msg = "loop variable of loop over rows must be a record variable or list of scalar variables";
    let cx = mcx::MemoryContext::new("plpgsql b081 for query");
    let mut comp = crate::comp::CompState::new();
    let mut parser = parser_for!(cx, comp, b"begin for unk in select 1 loop end loop; end");
    let err = parser.parse_function_body().unwrap_err();
    assert_eq!(err.sqlstate, types_error::ERRCODE_SYNTAX_ERROR);
    assert_eq!(err.message, msg);
    assert_eq!(err.internal_position, Some(11));

    let cx = mcx::MemoryContext::new("plpgsql b081 for execute");
    let mut comp = crate::comp::CompState::new();
    let mut parser = parser_for!(cx, comp, b"begin for unk in execute 'select 1' loop end loop; end");
    let err = parser.parse_function_body().unwrap_err();
    assert_eq!(err.sqlstate, types_error::ERRCODE_DATATYPE_MISMATCH);
    assert_eq!(err.message, msg);
    assert_eq!(err.internal_position, Some(11));
}

// expandedrecord.c:1032 (expanded_record_lookup_field): field names are
// compared with namestrcmp, case-sensitively, against the attname as
// stored — a record's shape keeps a quoted column's case.
#[test]
fn record_shape_keeps_attribute_name_case() {
    let cx = mcx::MemoryContext::new("plpgsql b081 record shape");
    let mut a = types_tuple::FormData_pg_attribute::default();
    a.attname.namestrcpy("Foo");
    a.atttypid = 23;
    a.attnum = 1;
    a.attlen = 4;
    a.attbyval = true;
    a.attalign = b'i' as i8;
    a.atttypmod = -1;
    let mut b = types_tuple::FormData_pg_attribute::default();
    b.attname.namestrcpy("foo");
    b.atttypid = 23;
    b.attnum = 2;
    b.attlen = 4;
    b.attbyval = true;
    b.attalign = b'i' as i8;
    b.atttypmod = -1;
    let td = ::tupdesc::CreateTupleDesc(cx.mcx(), &[a, b]).unwrap();
    assert_eq!(td.attrs.len(), 2);
    let desc = crate::exec::RecDesc::from_tupdesc(&td);
    assert_eq!(desc.names, vec!["Foo".to_string(), "foo".to_string()]);
}
