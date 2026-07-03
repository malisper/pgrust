// Differential tree parity: render parse trees in outfuncs.c's
// nodeToStringWithLocations format and compare against vectors emitted by the
// REAL compiled gram.c+outfuncs.c (vendored cgram_expected.txt; the harness
// recipe is in docs/optimizations/gram_core-parity.md).
use crate::raw_parser;
use mcx::MemoryContext;
use parser_seams::RawParseMode;
use types_nodes::rawnodes::{A_Expr_Kind, ValUnion};
use types_nodes::{Node, NodeList};

fn test_ctx() -> &'static MemoryContext {
    thread_local! {
        static CTX: &'static MemoryContext =
            Box::leak(Box::new(MemoryContext::new("gram-dump-test")));
    }
    CTX.with(|c| *c)
}

fn out_token(out: &mut String, s: Option<&str>) {
    let Some(s) = s else {
        out.push_str("<>");
        return;
    };
    if s.is_empty() {
        out.push_str("\"\"");
        return;
    }
    let b = s.as_bytes();
    if b[0] == b'<'
        || b[0] == b'"'
        || b[0].is_ascii_digit()
        || ((b[0] == b'+' || b[0] == b'-')
            && b.len() > 1
            && (b[1].is_ascii_digit() || b[1] == b'.'))
    {
        out.push('\\');
    }
    for c in s.chars() {
        if matches!(c, ' ' | '\n' | '\t' | '(' | ')' | '{' | '}' | '\\') {
            out.push('\\');
        }
        out.push(c);
    }
}

fn string_field(out: &mut String, name: &str, v: Option<&str>) {
    out.push_str(" :");
    out.push_str(name);
    out.push(' ');
    out_token(out, v);
}

fn node_field(out: &mut String, name: &str, v: Option<Node<'_>>) {
    out.push_str(" :");
    out.push_str(name);
    out.push(' ');
    match v {
        Some(n) => node(out, n),
        None => out.push_str("<>"),
    }
}

fn list_field(out: &mut String, name: &str, v: &NodeList<'_>) {
    out.push_str(" :");
    out.push_str(name);
    out.push(' ');
    list(out, v);
}

fn int_field(out: &mut String, name: &str, v: i32) {
    out.push_str(&format!(" :{name} {v}"));
}

fn bool_field(out: &mut String, name: &str, v: bool) {
    out.push_str(&format!(" :{name} {}", if v { "true" } else { "false" }));
}

fn list(out: &mut String, l: &NodeList<'_>) {
    if l.is_nil() {
        out.push_str("<>");
        return;
    }
    out.push('(');
    for (i, n) in l.iter().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        node(out, n);
    }
    out.push(')');
}

fn node(out: &mut String, n: Node<'_>) {
    if let Some(rs) = n.as_raw_stmt() {
        out.push_str("{RAWSTMT");
        node_field(out, "stmt", rs.stmt);
        int_field(out, "stmt_location", rs.stmt_location);
        int_field(out, "stmt_len", rs.stmt_len);
        out.push('}');
    } else if let Some(s) = n.as_select_stmt() {
        out.push_str("{SELECTSTMT");
        // C: plain DISTINCT is a one-NULL-cell list -> "(<>)".
        out.push_str(" :distinctClause ");
        match &s.distinctClause {
            types_nodes::DistinctClause::None => out.push_str("<>"),
            types_nodes::DistinctClause::All => out.push_str("(<>)"),
            types_nodes::DistinctClause::On(l) => list(out, l),
        }
        node_field(out, "intoClause", s.intoClause);
        list_field(out, "targetList", &s.targetList);
        list_field(out, "fromClause", &s.fromClause);
        node_field(out, "whereClause", s.whereClause);
        list_field(out, "groupClause", &s.groupClause);
        bool_field(out, "groupDistinct", s.groupDistinct);
        node_field(out, "havingClause", s.havingClause);
        list_field(out, "windowClause", &s.windowClause);
        list_field(out, "valuesLists", &s.valuesLists);
        list_field(out, "sortClause", &s.sortClause);
        node_field(out, "limitOffset", s.limitOffset);
        node_field(out, "limitCount", s.limitCount);
        int_field(out, "limitOption", s.limitOption as i32);
        list_field(out, "lockingClause", &s.lockingClause);
        node_field(out, "withClause", s.withClause);
        int_field(out, "op", s.op as i32);
        bool_field(out, "all", s.all);
        out.push_str(" :larg <>");
        assert!(s.larg.is_none() && s.rarg.is_none(), "set ops unrendered");
        out.push_str(" :rarg <>");
        out.push('}');
    } else if let Some(rt) = n.as_res_target() {
        out.push_str("{RESTARGET");
        string_field(out, "name", rt.name);
        list_field(out, "indirection", &rt.indirection);
        node_field(out, "val", rt.val);
        int_field(out, "location", rt.location);
        out.push('}');
    } else if let Some(c) = n.as_a_const() {
        out.push_str("{A_CONST");
        match c.val {
            None => out.push_str(" NULL"),
            Some(v) => {
                out.push_str(" :val ");
                match v {
                    ValUnion::Integer(i) => out.push_str(&i.ival.to_string()),
                    ValUnion::Float(f) => out.push_str(f.fval),
                    ValUnion::Boolean(b) => {
                        out.push_str(if b.boolval { "true" } else { "false" })
                    }
                    ValUnion::String(s) => {
                        out.push('"');
                        if !s.sval.is_empty() {
                            out_token(out, Some(s.sval));
                        }
                        out.push('"');
                    }
                    ValUnion::BitString(bs) => out_token(out, Some(bs.bsval)),
                }
            }
        }
        int_field(out, "location", c.location);
        out.push('}');
    } else if let Some(e) = n.as_a_expr() {
        out.push_str("{A_EXPR");
        assert!(matches!(e.kind, A_Expr_Kind::AEXPR_OP), "only AEXPR_OP rendered");
        list_field(out, "name", &e.name);
        node_field(out, "lexpr", e.lexpr);
        node_field(out, "rexpr", e.rexpr);
        int_field(out, "rexpr_list_start", e.rexpr_list_start);
        int_field(out, "rexpr_list_end", e.rexpr_list_end);
        int_field(out, "location", e.location);
        out.push('}');
    } else if let Some(cr) = n.as_column_ref() {
        out.push_str("{COLUMNREF");
        list_field(out, "fields", &cr.fields);
        int_field(out, "location", cr.location);
        out.push('}');
    } else if let Some(p) = n.as_param_ref() {
        out.push_str("{PARAMREF");
        int_field(out, "number", p.number);
        int_field(out, "location", p.location);
        out.push('}');
    } else if n.as_a_star().is_some() {
        out.push_str("{A_STAR}");
    } else if let Some(rv) = n.as_range_var() {
        out.push_str("{RANGEVAR");
        string_field(out, "catalogname", rv.catalogname);
        string_field(out, "schemaname", rv.schemaname);
        string_field(out, "relname", rv.relname);
        bool_field(out, "inh", rv.inh);
        out.push_str(" :relpersistence ");
        out_token(out, Some(std::str::from_utf8(&[rv.relpersistence]).unwrap()));
        out.push_str(" :alias ");
        match rv.alias {
            Some(a) => alias(out, a),
            None => out.push_str("<>"),
        }
        int_field(out, "location", rv.location);
        out.push('}');
    } else if let Some(sb) = n.as_sort_by() {
        out.push_str("{SORTBY");
        node_field(out, "node", sb.node);
        int_field(out, "sortby_dir", sb.sortby_dir as i32);
        int_field(out, "sortby_nulls", sb.sortby_nulls as i32);
        list_field(out, "useOp", &sb.useOp);
        int_field(out, "location", sb.location);
        out.push('}');
    } else if let Some(f) = n.as_func_call() {
        out.push_str("{FUNCCALL");
        list_field(out, "funcname", &f.funcname);
        list_field(out, "args", &f.args);
        list_field(out, "agg_order", &f.agg_order);
        node_field(out, "agg_filter", f.agg_filter);
        node_field(out, "over", f.over);
        bool_field(out, "agg_within_group", f.agg_within_group);
        bool_field(out, "agg_star", f.agg_star);
        bool_field(out, "agg_distinct", f.agg_distinct);
        bool_field(out, "func_variadic", f.func_variadic);
        int_field(out, "funcformat", f.funcformat as i32);
        int_field(out, "location", f.location);
        out.push('}');
    } else if let Some(t) = n.as_type_name() {
        out.push_str("{TYPENAME");
        list_field(out, "names", &t.names);
        int_field(out, "typeOid", t.typeOid as i32);
        bool_field(out, "setof", t.setof);
        bool_field(out, "pct_type", t.pct_type);
        list_field(out, "typmods", &t.typmods);
        int_field(out, "typemod", t.typemod);
        list_field(out, "arrayBounds", &t.arrayBounds);
        int_field(out, "location", t.location);
        out.push('}');
    } else if let Some(tc) = n.as_type_cast() {
        out.push_str("{TYPECAST");
        node_field(out, "arg", tc.arg);
        node_field(out, "typeName", tc.typeName);
        int_field(out, "location", tc.location);
        out.push('}');
    } else if let Some(b) = n.as_bool_expr() {
        out.push_str("{BOOLEXPR :boolop ");
        out.push_str(match b.boolop {
            types_nodes::BoolExprType::AND_EXPR => "and",
            types_nodes::BoolExprType::OR_EXPR => "or",
            types_nodes::BoolExprType::NOT_EXPR => "not",
        });
        list_field(out, "args", &b.args);
        int_field(out, "location", b.location);
        out.push('}');
    } else if let Some(nt) = n.as_null_test() {
        out.push_str("{NULLTEST");
        node_field(out, "arg", nt.arg);
        int_field(out, "nulltesttype", nt.nulltesttype as i32);
        bool_field(out, "argisrow", nt.argisrow);
        int_field(out, "location", nt.location);
        out.push('}');
    } else if let Some(p) = n.as_prepare_stmt() {
        out.push_str("{PREPARESTMT");
        string_field(out, "name", p.name);
        list_field(out, "argtypes", &p.argtypes);
        node_field(out, "query", p.query);
        out.push('}');
    } else if let Some(e) = n.as_execute_stmt() {
        out.push_str("{EXECUTESTMT");
        string_field(out, "name", e.name);
        list_field(out, "params", &e.params);
        out.push('}');
    } else if let Some(d) = n.as_deallocate_stmt() {
        out.push_str("{DEALLOCATESTMT");
        string_field(out, "name", d.name);
        bool_field(out, "isall", d.isall);
        int_field(out, "location", d.location);
        out.push('}');
    } else if let Some(s) = n.as_string() {
        out.push('"');
        if !s.sval.is_empty() {
            out_token(out, Some(s.sval));
        }
        out.push('"');
    } else if let Some(i) = n.as_integer() {
        out.push_str(&i.ival.to_string());
    } else if let Some(f) = n.as_float() {
        out.push_str(f.fval);
    } else if let Some(b) = n.as_boolean() {
        out.push_str(if b.boolval { "true" } else { "false" });
    } else if let Some(bs) = n.as_bitstring() {
        out_token(out, Some(bs.bsval));
    } else if let Some(l) = n.as_list() {
        list(out, l);
    } else {
        panic!("tests_dump: unrendered node tag {:?}", n.node_tag());
    }
}

fn alias(out: &mut String, a: &types_nodes::Alias<'_>) {
    out.push_str("{ALIAS");
    string_field(out, "aliasname", a.aliasname);
    list_field(out, "colnames", &a.colnames);
    out.push('}');
}

fn run_one(stmt: &str) -> String {
    match raw_parser(test_ctx().mcx(), stmt, RawParseMode::RAW_PARSE_DEFAULT) {
        Ok(tree) => {
            let mut out = String::from("OK ");
            list(&mut out, &tree);
            out
        }
        Err(e) => format!("ERR {} {}", e.cursor_position().unwrap_or(0), e.message()),
    }
}

#[test]
fn c_reference_vectors() {
    let corpus: Vec<&str> = include_str!("../corpus.txt").split('\0').collect();
    let expected: Vec<&str> = include_str!("../cgram_expected.txt").split('\0').collect();
    assert_eq!(corpus.len(), expected.len(), "corpus/vector count");
    let mut failures = Vec::new();
    for (stmt, want) in corpus.iter().zip(expected.iter()) {
        if stmt.is_empty() && want.is_empty() {
            continue;
        }
        let got = run_one(stmt);
        if got != *want {
            failures.push(format!("stmt {stmt:?}\n  C:    {want}\n  rust: {got}"));
        }
    }
    assert!(failures.is_empty(), "{} mismatches:\n{}", failures.len(), failures.join("\n"));
}
