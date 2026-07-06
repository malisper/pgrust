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

fn int_list_field(out: &mut String, name: &str, v: &types_nodes::list::IntList<'_>) {
    out.push_str(" :");
    out.push_str(name);
    out.push(' ');
    if v.is_nil() {
        out.push_str("<>");
        return;
    }
    // outfuncs.c _outList int-list form: "(i 1 2 3)".
    out.push_str("(i");
    for x in v.as_slice() {
        out.push_str(&format!(" {x}"));
    }
    out.push(')');
}

fn list_field(out: &mut String, name: &str, v: &NodeList<'_>) {
    out.push_str(" :");
    out.push_str(name);
    out.push(' ');
    list(out, v);
}

// outfuncs _outList over a list with NULL cells: outNode(NULL) prints "<>".
fn opt_list_field(out: &mut String, name: &str, v: &types_nodes::OptNodeList<'_>) {
    out.push_str(" :");
    out.push_str(name);
    out.push(' ');
    if v.is_nil() {
        out.push_str("<>");
        return;
    }
    out.push('(');
    for (i, n) in v.iter().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        match n {
            Some(n) => node(out, n),
            None => out.push_str("<>"),
        }
    }
    out.push(')');
}

fn oid_list_field(out: &mut String, name: &str, l: &types_nodes::list::OidList<'_>) {
    out.push_str(" :");
    out.push_str(name);
    out.push(' ');
    if l.is_nil() {
        out.push_str("<>");
        return;
    }
    out.push('(');
    for (i, o) in l.iter().enumerate() {
        if i > 0 {
            out.push(' ');
        }
        out.push_str(&format!("o {o}"));
    }
    out.push(')');
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
        select_stmt(out, s);
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
        out.push_str(match e.kind {
            A_Expr_Kind::AEXPR_OP => "",
            A_Expr_Kind::AEXPR_OP_ANY => " ANY",
            A_Expr_Kind::AEXPR_OP_ALL => " ALL",
            A_Expr_Kind::AEXPR_DISTINCT => " DISTINCT",
            A_Expr_Kind::AEXPR_NOT_DISTINCT => " NOT_DISTINCT",
            A_Expr_Kind::AEXPR_NULLIF => " NULLIF",
            A_Expr_Kind::AEXPR_IN => " IN",
            A_Expr_Kind::AEXPR_LIKE => " LIKE",
            A_Expr_Kind::AEXPR_ILIKE => " ILIKE",
            A_Expr_Kind::AEXPR_SIMILAR => " SIMILAR",
            A_Expr_Kind::AEXPR_BETWEEN => " BETWEEN",
            A_Expr_Kind::AEXPR_NOT_BETWEEN => " NOT_BETWEEN",
            A_Expr_Kind::AEXPR_BETWEEN_SYM => " BETWEEN_SYM",
            A_Expr_Kind::AEXPR_NOT_BETWEEN_SYM => " NOT_BETWEEN_SYM",
        });
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
    } else if let Some(ai) = n.as_a_indirection() {
        out.push_str("{A_INDIRECTION");
        node_field(out, "arg", ai.arg);
        list_field(out, "indirection", &ai.indirection);
        out.push('}');
    } else if let Some(ix) = n.as_variant::<types_nodes::rawnodes::A_Indices>() {
        out.push_str("{A_INDICES");
        bool_field(out, "is_slice", ix.is_slice);
        node_field(out, "lidx", ix.lidx);
        node_field(out, "uidx", ix.uidx);
        out.push('}');
    } else if let Some(ma) = n.as_multi_assign_ref() {
        out.push_str("{MULTIASSIGNREF");
        node_field(out, "source", ma.source);
        int_field(out, "colno", ma.colno);
        int_field(out, "ncolumns", ma.ncolumns);
        out.push('}');
    } else if let Some(rv) = n.as_range_var() {
        range_var(out, rv);
    } else if let Some(v) = n.as_variant::<types_nodes::rawnodes::ViewStmt>() {
        out.push_str("{VIEWSTMT :view ");
        match v.view {
            Some(rv) => range_var(out, rv),
            None => out.push_str("<>"),
        }
        list_field(out, "aliases", &v.aliases);
        node_field(out, "query", v.query);
        bool_field(out, "replace", v.replace);
        list_field(out, "options", &v.options);
        int_field(out, "withCheckOption", v.withCheckOption as i32);
        out.push('}');
    } else if let Some(c) = n.as_variant::<types_nodes::rawnodes::CreateTableAsStmt>() {
        out.push_str("{CREATETABLEASSTMT");
        node_field(out, "query", c.query);
        node_field(out, "into", c.into);
        int_field(out, "objtype", c.objtype as i32);
        bool_field(out, "is_select_into", c.is_select_into);
        bool_field(out, "if_not_exists", c.if_not_exists);
        out.push('}');
    } else if let Some(ic) = n.as_variant::<types_nodes::rawnodes::IntoClause>() {
        out.push_str("{INTOCLAUSE");
        node_field(out, "rel", ic.rel);
        list_field(out, "colNames", &ic.colNames);
        string_field(out, "accessMethod", ic.accessMethod);
        list_field(out, "options", &ic.options);
        int_field(out, "onCommit", ic.onCommit as i32);
        string_field(out, "tableSpaceName", ic.tableSpaceName);
        node_field(out, "viewQuery", ic.viewQuery);
        bool_field(out, "skipData", ic.skipData);
        out.push('}');
    } else if let Some(r) = n.as_variant::<types_nodes::rawnodes::RefreshMatViewStmt>() {
        out.push_str("{REFRESHMATVIEWSTMT");
        bool_field(out, "concurrent", r.concurrent);
        bool_field(out, "skipData", r.skipData);
        out.push_str(" :relation ");
        match r.relation {
            Some(rv) => range_var(out, rv),
            None => out.push_str("<>"),
        }
        out.push('}');
    } else if let Some(sb) = n.as_sort_by() {
        out.push_str("{SORTBY");
        node_field(out, "node", sb.node);
        int_field(out, "sortby_dir", sb.sortby_dir as i32);
        int_field(out, "sortby_nulls", sb.sortby_nulls as i32);
        list_field(out, "useOp", &sb.useOp);
        int_field(out, "location", sb.location);
        out.push('}');
    } else if let Some(r) = n.as_row_expr() {
        out.push_str("{ROWEXPR");
        list_field(out, "args", &r.args);
        int_field(out, "row_typeid", r.row_typeid as i32);
        int_field(out, "row_format", r.row_format as i32);
        list_field(out, "colnames", &r.colnames);
        int_field(out, "location", r.location);
        out.push('}');
    } else if let Some(gs) = n.as_grouping_set() {
        out.push_str("{GROUPINGSET");
        int_field(out, "kind", gs.kind as i32);
        list_field(out, "content", &gs.content);
        int_field(out, "location", gs.location);
        out.push('}');
    } else if let Some(gf) = n.as_grouping_func() {
        out.push_str("{GROUPINGFUNC");
        list_field(out, "args", &gf.args);
        int_list_field(out, "refs", &gf.refs);
        int_list_field(out, "cols", &gf.cols);
        int_field(out, "agglevelsup", gf.agglevelsup as i32);
        int_field(out, "location", gf.location);
        out.push('}');
    } else if let Some(wd) = n.as_window_def() {
        out.push_str("{WINDOWDEF");
        string_field(out, "name", wd.name);
        string_field(out, "refname", wd.refname);
        list_field(out, "partitionClause", &wd.partitionClause);
        list_field(out, "orderClause", &wd.orderClause);
        int_field(out, "frameOptions", wd.frameOptions);
        node_field(out, "startOffset", wd.startOffset);
        node_field(out, "endOffset", wd.endOffset);
        int_field(out, "location", wd.location);
        out.push('}');
    } else if let Some(f) = n.as_func_call() {
        func_call(out, f);
    } else if let Some(na) = n.as_named_arg_expr() {
        out.push_str("{NAMEDARGEXPR");
        node_field(out, "arg", na.arg);
        string_field(out, "name", na.name);
        int_field(out, "argnumber", na.argnumber);
        int_field(out, "location", na.location);
        out.push('}');
    } else if let Some(cs) = n.as_call_stmt() {
        out.push_str("{CALLSTMT :funccall ");
        match cs.funccall {
            Some(f) => func_call(out, f),
            None => out.push_str("<>"),
        }
        assert!(cs.funcexpr.is_none(), "raw CallStmt carries no funcexpr");
        out.push_str(" :funcexpr <>");
        list_field(out, "outargs", &cs.outargs);
        out.push('}');
    } else if let Some(na) = n.as_named_arg_expr() {
        out.push_str("{NAMEDARGEXPR");
        node_field(out, "arg", na.arg);
        string_field(out, "name", na.name);
        int_field(out, "argnumber", na.argnumber);
        int_field(out, "location", na.location);
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
    } else if let Some(sl) = n.as_sub_link() {
        out.push_str("{SUBLINK");
        int_field(out, "subLinkType", sl.subLinkType as i32);
        int_field(out, "subLinkId", sl.subLinkId);
        node_field(out, "testexpr", sl.testexpr);
        list_field(out, "operName", &sl.operName);
        node_field(out, "subselect", Some(sl.subselect));
        int_field(out, "location", sl.location);
        out.push('}');
    } else if let Some(bt) = n.as_boolean_test() {
        out.push_str("{BOOLEANTEST");
        node_field(out, "arg", bt.arg);
        int_field(out, "booltesttype", bt.booltesttype as i32);
        int_field(out, "location", bt.location);
        out.push('}');
    } else if let Some(cc) = n.as_collate_clause() {
        out.push_str("{COLLATECLAUSE");
        node_field(out, "arg", cc.arg);
        list_field(out, "collname", &cc.collname);
        int_field(out, "location", cc.location);
        out.push('}');
    } else if let Some(t) = n.as_variant::<types_nodes::rawnodes::RangeTableFunc>() {
        out.push_str("{RANGETABLEFUNC");
        bool_field(out, "lateral", t.lateral);
        node_field(out, "docexpr", t.docexpr);
        node_field(out, "rowexpr", t.rowexpr);
        list_field(out, "namespaces", &t.namespaces);
        list_field(out, "columns", &t.columns);
        out.push_str(" :alias ");
        match t.alias {
            Some(a) => alias(out, a),
            None => out.push_str("<>"),
        }
        int_field(out, "location", t.location);
        out.push('}');
    } else if let Some(c) = n.as_variant::<types_nodes::rawnodes::RangeTableFuncCol>() {
        out.push_str("{RANGETABLEFUNCCOL");
        string_field(out, "colname", c.colname);
        node_field(out, "typeName", c.typeName);
        bool_field(out, "for_ordinality", c.for_ordinality);
        bool_field(out, "is_not_null", c.is_not_null);
        node_field(out, "colexpr", c.colexpr);
        node_field(out, "coldefexpr", c.coldefexpr);
        int_field(out, "location", c.location);
        out.push('}');
    } else if let Some(s) = n.as_variant::<types_nodes::rawnodes::XmlSerialize>() {
        out.push_str("{XMLSERIALIZE");
        int_field(out, "xmloption", s.xmloption as i32);
        node_field(out, "expr", s.expr);
        node_field(out, "typeName", s.typeName);
        bool_field(out, "indent", s.indent);
        int_field(out, "location", s.location);
        out.push('}');
    } else if let Some(x) = n.as_xml_expr() {
        out.push_str("{XMLEXPR");
        int_field(out, "op", x.op as i32);
        string_field(out, "name", x.name);
        list_field(out, "named_args", &x.named_args);
        list_field(out, "arg_names", &x.arg_names);
        list_field(out, "args", &x.args);
        int_field(out, "xmloption", x.xmloption as i32);
        bool_field(out, "indent", x.indent);
        int_field(out, "type", x.r#type as i32);
        int_field(out, "typmod", x.typmod);
        int_field(out, "location", x.location);
        out.push('}');
    } else if let Some(c) = n.as_variant::<types_nodes::rawnodes::CompositeTypeStmt>() {
        out.push_str("{COMPOSITETYPESTMT :typevar ");
        match c.typevar {
            Some(rv) => range_var(out, rv),
            None => out.push_str("<>"),
        }
        list_field(out, "coldeflist", &c.coldeflist);
        out.push('}');
    } else if let Some(g) = n.as_grant_stmt() {
        grant_stmt(out, g);
    } else if let Some(a) = n.as_alter_default_privileges_stmt() {
        out.push_str("{ALTERDEFAULTPRIVILEGESSTMT");
        list_field(out, "options", &a.options);
        out.push_str(" :action ");
        match a.action {
            Some(g) => grant_stmt(out, g),
            None => out.push_str("<>"),
        }
        out.push('}');
    } else if let Some(ap) = n.as_access_priv() {
        out.push_str("{ACCESSPRIV");
        string_field(out, "priv_name", ap.priv_name);
        list_field(out, "cols", &ap.cols);
        out.push('}');
    } else if let Some(r) = n.as_role_spec() {
        role_spec(out, r);
    } else if let Some(g) = n.as_grant_role_stmt() {
        out.push_str("{GRANTROLESTMT");
        list_field(out, "granted_roles", &g.granted_roles);
        list_field(out, "grantee_roles", &g.grantee_roles);
        bool_field(out, "is_grant", g.is_grant);
        list_field(out, "opt", &g.opt);
        out.push_str(" :grantor ");
        match g.grantor {
            Some(r) => role_spec(out, r),
            None => out.push_str("<>"),
        }
        int_field(out, "behavior", g.behavior as i32);
        out.push('}');
    } else if let Some(c) = n.as_create_role_stmt() {
        out.push_str("{CREATEROLESTMT");
        int_field(out, "stmt_type", c.stmt_type as i32);
        string_field(out, "role", c.role);
        list_field(out, "options", &c.options);
        out.push('}');
    } else if let Some(a) = n.as_alter_role_stmt() {
        out.push_str("{ALTERROLESTMT :role ");
        role_spec(out, a.role);
        list_field(out, "options", &a.options);
        int_field(out, "action", a.action);
        out.push('}');
    } else if let Some(a) = n.as_alter_role_set_stmt() {
        out.push_str("{ALTERROLESETSTMT :role ");
        match a.role {
            Some(r) => role_spec(out, r),
            None => out.push_str("<>"),
        }
        string_field(out, "database", a.database);
        out.push_str(" :setstmt ");
        variable_set_stmt(out, a.setstmt);
        out.push('}');
    } else if let Some(d) = n.as_drop_role_stmt() {
        out.push_str("{DROPROLESTMT");
        list_field(out, "roles", &d.roles);
        bool_field(out, "missing_ok", d.missing_ok);
        out.push('}');
    } else if let Some(d) = n.as_drop_owned_stmt() {
        out.push_str("{DROPOWNEDSTMT");
        list_field(out, "roles", &d.roles);
        int_field(out, "behavior", d.behavior as i32);
        out.push('}');
    } else if let Some(r) = n.as_reassign_owned_stmt() {
        out.push_str("{REASSIGNOWNEDSTMT");
        list_field(out, "roles", &r.roles);
        out.push_str(" :newrole ");
        role_spec(out, r.newrole);
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
    } else if let Some(d) = n.as_declare_cursor_stmt() {
        out.push_str("{DECLARECURSORSTMT");
        string_field(out, "portalname", d.portalname);
        int_field(out, "options", d.options);
        node_field(out, "query", d.query);
        out.push('}');
    } else if let Some(c) = n.as_close_portal_stmt() {
        out.push_str("{CLOSEPORTALSTMT");
        string_field(out, "portalname", c.portalname);
        out.push('}');
    } else if let Some(f) = n.as_fetch_stmt() {
        out.push_str("{FETCHSTMT");
        int_field(out, "direction", f.direction as i32);
        out.push_str(&format!(" :howMany {}", f.howMany));
        string_field(out, "portalname", f.portalname);
        bool_field(out, "ismove", f.ismove);
        out.push('}');
    } else if let Some(c) = n.as_variant::<types_nodes::parsenodes::CreateConversionStmt>() {
        out.push_str("{CREATECONVERSIONSTMT");
        list_field(out, "conversion_name", &c.conversion_name);
        string_field(out, "for_encoding_name", c.for_encoding_name);
        string_field(out, "to_encoding_name", c.to_encoding_name);
        list_field(out, "func_name", &c.func_name);
        bool_field(out, "def", c.def);
        out.push('}');
    } else if let Some(p) = n.as_variant::<types_nodes::parsenodes::CreatePLangStmt>() {
        out.push_str("{CREATEPLANGSTMT");
        bool_field(out, "replace", p.replace);
        string_field(out, "plname", p.plname);
        list_field(out, "plhandler", &p.plhandler);
        list_field(out, "plinline", &p.plinline);
        list_field(out, "plvalidator", &p.plvalidator);
        bool_field(out, "pltrusted", p.pltrusted);
        out.push('}');
    } else if let Some(d) = n.as_variant::<types_nodes::parsenodes::DefineStmt>() {
        out.push_str("{DEFINESTMT");
        int_field(out, "kind", d.kind as i32);
        bool_field(out, "oldstyle", d.oldstyle);
        list_field(out, "defnames", &d.defnames);
        list_field(out, "args", &d.args);
        list_field(out, "definition", &d.definition);
        bool_field(out, "if_not_exists", d.if_not_exists);
        bool_field(out, "replace", d.replace);
        out.push('}');
    } else if let Some(o) = n.as_variant::<types_nodes::parsenodes::ObjectWithArgs>() {
        out.push_str("{OBJECTWITHARGS");
        list_field(out, "objname", &o.objname);
        opt_list_field(out, "objargs", &o.objargs);
        list_field(out, "objfuncargs", &o.objfuncargs);
        bool_field(out, "args_unspecified", o.args_unspecified);
        out.push('}');
    } else if let Some(c) = n.as_variant::<types_nodes::parsenodes::CreateOpClassStmt>() {
        out.push_str("{CREATEOPCLASSSTMT");
        list_field(out, "opclassname", &c.opclassname);
        list_field(out, "opfamilyname", &c.opfamilyname);
        string_field(out, "amname", c.amname);
        node_field(out, "datatype", c.datatype);
        list_field(out, "items", &c.items);
        bool_field(out, "isDefault", c.isDefault);
        out.push('}');
    } else if let Some(i) = n.as_variant::<types_nodes::parsenodes::CreateOpClassItem>() {
        out.push_str("{CREATEOPCLASSITEM");
        int_field(out, "itemtype", i.itemtype);
        node_field(out, "name", i.name);
        int_field(out, "number", i.number);
        list_field(out, "order_family", &i.order_family);
        list_field(out, "class_args", &i.class_args);
        node_field(out, "storedtype", i.storedtype);
        out.push('}');
    } else if let Some(c) = n.as_variant::<types_nodes::parsenodes::CreateOpFamilyStmt>() {
        out.push_str("{CREATEOPFAMILYSTMT");
        list_field(out, "opfamilyname", &c.opfamilyname);
        string_field(out, "amname", c.amname);
        out.push('}');
    } else if let Some(a) = n.as_variant::<types_nodes::parsenodes::AlterOpFamilyStmt>() {
        out.push_str("{ALTEROPFAMILYSTMT");
        list_field(out, "opfamilyname", &a.opfamilyname);
        string_field(out, "amname", a.amname);
        bool_field(out, "isDrop", a.isDrop);
        list_field(out, "items", &a.items);
        out.push('}');
    } else if let Some(a) = n.as_variant::<types_nodes::parsenodes::AlterOperatorStmt>() {
        out.push_str("{ALTEROPERATORSTMT");
        node_field(out, "opername", a.opername);
        list_field(out, "options", &a.options);
        out.push('}');
    } else if let Some(a) = n.as_variant::<types_nodes::parsenodes::CreateAmStmt>() {
        out.push_str("{CREATEAMSTMT");
        string_field(out, "amname", a.amname);
        list_field(out, "handler_name", &a.handler_name);
        out.push_str(" :amtype ");
        if a.amtype == 0 {
            out.push_str("<>");
        } else {
            out.push(a.amtype as char);
        }
        out.push('}');
    } else if let Some(c) = n.as_variant::<types_nodes::parsenodes::CreateCastStmt>() {
        out.push_str("{CREATECASTSTMT");
        node_field(out, "sourcetype", c.sourcetype);
        node_field(out, "targettype", c.targettype);
        node_field(out, "func", c.func);
        int_field(out, "context", c.context as i32);
        bool_field(out, "inout", c.inout);
        out.push('}');
    } else if let Some(t) = n.as_variant::<types_nodes::parsenodes::CreateTransformStmt>() {
        out.push_str("{CREATETRANSFORMSTMT");
        bool_field(out, "replace", t.replace);
        node_field(out, "type_name", t.type_name);
        string_field(out, "lang", t.lang);
        node_field(out, "fromsql", t.fromsql);
        node_field(out, "tosql", t.tosql);
        out.push('}');
    } else if let Some(p) = n.as_variant::<types_nodes::parsenodes::FunctionParameter>() {
        out.push_str("{FUNCTIONPARAMETER");
        string_field(out, "name", p.name);
        node_field(out, "argType", p.argType);
        int_field(out, "mode", p.mode as i32);
        node_field(out, "defexpr", p.defexpr);
        int_field(out, "location", p.location);
        out.push('}');
    } else if let Some(c) = n.as_comment_stmt() {
        out.push_str("{COMMENTSTMT");
        int_field(out, "objtype", c.objtype as i32);
        node_field(out, "object", c.object);
        string_field(out, "comment", c.comment);
        out.push('}');
    } else if let Some(d) = n.as_drop_stmt() {
        out.push_str("{DROPSTMT");
        list_field(out, "objects", &d.objects);
        int_field(out, "removeType", d.removeType as i32);
        int_field(out, "behavior", d.behavior as i32);
        bool_field(out, "missing_ok", d.missing_ok);
        bool_field(out, "concurrent", d.concurrent);
        out.push('}');
    } else if let Some(t) = n.as_create_trig_stmt() {
        out.push_str("{CREATETRIGSTMT");
        bool_field(out, "replace", t.replace);
        bool_field(out, "isconstraint", t.isconstraint);
        string_field(out, "trigname", t.trigname);
        out.push_str(" :relation ");
        match t.relation {
            Some(rv) => range_var(out, rv),
            None => out.push_str("<>"),
        }
        list_field(out, "funcname", &t.funcname);
        list_field(out, "args", &t.args);
        bool_field(out, "row", t.row);
        int_field(out, "timing", t.timing as i32);
        int_field(out, "events", t.events as i32);
        list_field(out, "columns", &t.columns);
        node_field(out, "whenClause", t.whenClause);
        list_field(out, "transitionRels", &t.transitionRels);
        bool_field(out, "deferrable", t.deferrable);
        bool_field(out, "initdeferred", t.initdeferred);
        out.push_str(" :constrrel ");
        match t.constrrel {
            Some(rv) => range_var(out, rv),
            None => out.push_str("<>"),
        }
        out.push('}');
    } else if let Some(c) = n.as_create_event_trig_stmt() {
        out.push_str("{CREATEEVENTTRIGSTMT");
        string_field(out, "trigname", c.trigname);
        string_field(out, "eventname", c.eventname);
        list_field(out, "whenclause", &c.whenclause);
        list_field(out, "funcname", &c.funcname);
        out.push('}');
    } else if let Some(a) = n.as_alter_event_trig_stmt() {
        out.push_str("{ALTEREVENTTRIGSTMT");
        string_field(out, "trigname", a.trigname);
        char_field(out, "tgenabled", a.tgenabled as u8);
        out.push('}');
    } else if let Some(t) = n.as_trigger_transition() {
        out.push_str("{TRIGGERTRANSITION");
        string_field(out, "name", t.name);
        bool_field(out, "isNew", t.isNew);
        bool_field(out, "isTable", t.isTable);
        out.push('}');
    } else if let Some(c) = n.as_constraints_set_stmt() {
        out.push_str("{CONSTRAINTSSETSTMT");
        list_field(out, "constraints", &c.constraints);
        bool_field(out, "deferred", c.deferred);
        out.push('}');
    } else if let Some(r) = n.as_variant::<types_nodes::parsenodes::RenameStmt>() {
        out.push_str("{RENAMESTMT");
        int_field(out, "renameType", r.renameType as i32);
        int_field(out, "relationType", r.relationType as i32);
        out.push_str(" :relation ");
        match r.relation {
            Some(rv) => range_var(out, rv),
            None => out.push_str("<>"),
        }
        node_field(out, "object", r.object);
        string_field(out, "subname", r.subname);
        string_field(out, "newname", r.newname);
        int_field(out, "behavior", r.behavior as i32);
        bool_field(out, "missing_ok", r.missing_ok);
        out.push('}');
    } else if let Some(v) = n.as_variable_set_stmt() {
        variable_set_stmt(out, v);
    } else if let Some(v) = n.as_variable_show_stmt() {
        out.push_str("{VARIABLESHOWSTMT");
        string_field(out, "name", v.name);
        out.push('}');
    } else if let Some(d) = n.as_do_stmt() {
        out.push_str("{DOSTMT");
        list_field(out, "args", &d.args);
        out.push('}');
    } else if let Some(t) = n.as_transaction_stmt() {
        out.push_str("{TRANSACTIONSTMT");
        int_field(out, "kind", t.kind as i32);
        list_field(out, "options", &t.options);
        string_field(out, "savepoint_name", t.savepoint_name);
        string_field(out, "gid", t.gid);
        bool_field(out, "chain", t.chain);
        int_field(out, "location", t.location);
        out.push('}');
    } else if let Some(e) = n.as_explain_stmt() {
        out.push_str("{EXPLAINSTMT");
        node_field(out, "query", e.query);
        list_field(out, "options", &e.options);
        out.push('}');
    } else if let Some(d) = n.as_def_elem() {
        out.push_str("{DEFELEM");
        string_field(out, "defnamespace", d.defnamespace);
        string_field(out, "defname", d.defname);
        node_field(out, "arg", d.arg);
        int_field(out, "defaction", d.defaction as i32);
        int_field(out, "location", d.location);
        out.push('}');
    } else if let Some(cs) = n.as_create_seq_stmt() {
        out.push_str("{CREATESEQSTMT :sequence ");
        match cs.sequence {
            Some(rv) => range_var(out, rv),
            None => out.push_str("<>"),
        }
        list_field(out, "options", &cs.options);
        out.push_str(&format!(" :ownerId {}", cs.ownerId));
        bool_field(out, "for_identity", cs.for_identity);
        bool_field(out, "if_not_exists", cs.if_not_exists);
        out.push('}');
    } else if let Some(ce) = n.as_variant::<types_nodes::rawnodes::CreateExtensionStmt>() {
        out.push_str("{CREATEEXTENSIONSTMT");
        string_field(out, "extname", ce.extname);
        bool_field(out, "if_not_exists", ce.if_not_exists);
        list_field(out, "options", &ce.options);
        out.push('}');
    } else if let Some(ae) = n.as_variant::<types_nodes::rawnodes::AlterExtensionStmt>() {
        out.push_str("{ALTEREXTENSIONSTMT");
        string_field(out, "extname", ae.extname);
        list_field(out, "options", &ae.options);
        out.push('}');
    } else if let Some(j) = n.as_join_expr() {
        out.push_str("{JOINEXPR");
        int_field(out, "jointype", j.jointype as i32);
        bool_field(out, "isNatural", j.isNatural);
        node_field(out, "larg", Some(j.larg));
        node_field(out, "rarg", Some(j.rarg));
        list_field(out, "usingClause", &j.usingClause);
        out.push_str(" :join_using_alias ");
        match j.join_using_alias {
            Some(a) => alias(out, a),
            None => out.push_str("<>"),
        }
        node_field(out, "quals", j.quals);
        out.push_str(" :alias ");
        match j.alias {
            Some(a) => alias(out, a),
            None => out.push_str("<>"),
        }
        int_field(out, "rtindex", j.rtindex);
        out.push('}');
    } else if let Some(r) = n.as_range_subselect() {
        out.push_str("{RANGESUBSELECT");
        bool_field(out, "lateral", r.lateral);
        node_field(out, "subquery", r.subquery);
        out.push_str(" :alias ");
        match r.alias {
            Some(a) => alias(out, a),
            None => out.push_str("<>"),
        }
        out.push('}');
    } else if let Some(c) = n.as_case_expr() {
        out.push_str("{CASEEXPR");
        int_field(out, "casetype", c.casetype as i32);
        int_field(out, "casecollid", c.casecollid as i32);
        node_field(out, "arg", c.arg);
        list_field(out, "args", &c.args);
        node_field(out, "defresult", c.defresult);
        int_field(out, "location", c.location);
        out.push('}');
    } else if let Some(w) = n.as_case_when() {
        out.push_str("{CASEWHEN");
        node_field(out, "expr", w.expr);
        node_field(out, "result", w.result);
        int_field(out, "location", w.location);
        out.push('}');
    } else if let Some(c) = n.as_coalesce_expr() {
        out.push_str("{COALESCEEXPR");
        int_field(out, "coalescetype", c.coalescetype as i32);
        int_field(out, "coalescecollid", c.coalescecollid as i32);
        list_field(out, "args", &c.args);
        int_field(out, "location", c.location);
        out.push('}');
    } else if let Some(m) = n.as_min_max_expr() {
        out.push_str("{MINMAXEXPR");
        int_field(out, "minmaxtype", m.minmaxtype as i32);
        int_field(out, "minmaxcollid", m.minmaxcollid as i32);
        int_field(out, "inputcollid", m.inputcollid as i32);
        int_field(out, "op", m.op as i32);
        list_field(out, "args", &m.args);
        int_field(out, "location", m.location);
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
    } else if let Some(w) = n.as_with_clause() {
        out.push_str("{WITHCLAUSE");
        list_field(out, "ctes", &w.ctes);
        bool_field(out, "recursive", w.recursive);
        int_field(out, "location", w.location);
        out.push('}');
    } else if let Some(c) = n.as_common_table_expr() {
        out.push_str("{COMMONTABLEEXPR");
        string_field(out, "ctename", c.ctename);
        list_field(out, "aliascolnames", &c.aliascolnames);
        int_field(out, "ctematerialized", c.ctematerialized as i32);
        node_field(out, "ctequery", c.ctequery);
        node_field(out, "search_clause", c.search_clause);
        node_field(out, "cycle_clause", c.cycle_clause);
        int_field(out, "location", c.location);
        bool_field(out, "cterecursive", c.cterecursive);
        int_field(out, "cterefcount", c.cterefcount);
        list_field(out, "ctecolnames", &c.ctecolnames);
        // Raw parse never fills the analysis lists; C prints <>.
        assert!(c.ctecoltypes.is_nil() && c.ctecoltypmods.is_nil() && c.ctecolcollations.is_nil());
        out.push_str(" :ctecoltypes <> :ctecoltypmods <> :ctecolcollations <>");
        out.push('}');
    } else if let Some(s) = n.as_cte_search_clause() {
        out.push_str("{CTESEARCHCLAUSE");
        list_field(out, "search_col_list", &s.search_col_list);
        bool_field(out, "search_breadth_first", s.search_breadth_first);
        string_field(out, "search_seq_column", s.search_seq_column);
        int_field(out, "location", s.location);
        out.push('}');
    } else if let Some(c) = n.as_cte_cycle_clause() {
        out.push_str("{CTECYCLECLAUSE");
        list_field(out, "cycle_col_list", &c.cycle_col_list);
        string_field(out, "cycle_mark_column", c.cycle_mark_column);
        node_field(out, "cycle_mark_value", c.cycle_mark_value);
        node_field(out, "cycle_mark_default", c.cycle_mark_default);
        string_field(out, "cycle_path_column", c.cycle_path_column);
        int_field(out, "location", c.location);
        int_field(out, "cycle_mark_type", c.cycle_mark_type as i32);
        int_field(out, "cycle_mark_typmod", c.cycle_mark_typmod);
        int_field(out, "cycle_mark_collation", c.cycle_mark_collation as i32);
        int_field(out, "cycle_mark_neop", c.cycle_mark_neop as i32);
        out.push('}');
    } else if let Some(e) = n.as_variant::<types_nodes::rawnodes::IndexElem>() {
        out.push_str("{INDEXELEM");
        string_field(out, "name", e.name);
        node_field(out, "expr", e.expr);
        string_field(out, "indexcolname", e.indexcolname);
        list_field(out, "collation", &e.collation);
        list_field(out, "opclass", &e.opclass);
        list_field(out, "opclassopts", &e.opclassopts);
        int_field(out, "ordering", e.ordering as i32);
        int_field(out, "nulls_ordering", e.nulls_ordering as i32);
        out.push('}');
    } else if let Some(s) = n.as_variant::<types_nodes::rawnodes::IndexStmt>() {
        out.push_str("{INDEXSTMT");
        string_field(out, "idxname", s.idxname);
        out.push_str(" :relation ");
        match s.relation {
            Some(rv) => range_var(out, rv),
            None => out.push_str("<>"),
        }
        string_field(out, "accessMethod", s.accessMethod);
        string_field(out, "tableSpace", s.tableSpace);
        list_field(out, "indexParams", &s.indexParams);
        list_field(out, "indexIncludingParams", &s.indexIncludingParams);
        list_field(out, "options", &s.options);
        node_field(out, "whereClause", s.whereClause);
        list_field(out, "excludeOpNames", &s.excludeOpNames);
        string_field(out, "idxcomment", s.idxcomment);
        out.push_str(&format!(" :indexOid {}", s.indexOid));
        out.push_str(&format!(" :oldNumber {}", s.oldNumber));
        out.push_str(&format!(" :oldCreateSubid {}", s.oldCreateSubid));
        out.push_str(&format!(" :oldFirstRelfilelocatorSubid {}", s.oldFirstRelfilelocatorSubid));
        bool_field(out, "unique", s.unique);
        bool_field(out, "nulls_not_distinct", s.nulls_not_distinct);
        bool_field(out, "primary", s.primary);
        bool_field(out, "isconstraint", s.isconstraint);
        bool_field(out, "iswithoutoverlaps", s.iswithoutoverlaps);
        bool_field(out, "deferrable", s.deferrable);
        bool_field(out, "initdeferred", s.initdeferred);
        bool_field(out, "transformed", s.transformed);
        bool_field(out, "concurrent", s.concurrent);
        bool_field(out, "if_not_exists", s.if_not_exists);
        bool_field(out, "reset_default_tblspc", s.reset_default_tblspc);
        out.push('}');
    } else if let Some(a) = n.as_variant::<types_nodes::parsenodes::AlterTableStmt>() {
        out.push_str("{ALTERTABLESTMT");
        out.push_str(" :relation ");
        match a.relation {
            Some(rv) => range_var(out, rv),
            None => out.push_str("<>"),
        }
        list_field(out, "cmds", &a.cmds);
        int_field(out, "objtype", a.objtype as i32);
        bool_field(out, "missing_ok", a.missing_ok);
        out.push('}');
    } else if let Some(a) = n.as_variant::<types_nodes::parsenodes::AlterTableMoveAllStmt>() {
        out.push_str("{ALTERTABLEMOVEALLSTMT");
        string_field(out, "orig_tablespacename", a.orig_tablespacename);
        int_field(out, "objtype", a.objtype as i32);
        list_field(out, "roles", &a.roles);
        string_field(out, "new_tablespacename", a.new_tablespacename);
        bool_field(out, "nowait", a.nowait);
        out.push('}');
    } else if let Some(c) = n.as_variant::<types_nodes::parsenodes::ATAlterConstraint>() {
        out.push_str("{ATALTERCONSTRAINT");
        string_field(out, "conname", c.conname);
        bool_field(out, "alterEnforceability", c.alterEnforceability);
        bool_field(out, "is_enforced", c.is_enforced);
        bool_field(out, "alterDeferrability", c.alterDeferrability);
        bool_field(out, "deferrable", c.deferrable);
        bool_field(out, "initdeferred", c.initdeferred);
        bool_field(out, "alterInheritability", c.alterInheritability);
        bool_field(out, "noinherit", c.noinherit);
        out.push('}');
    } else if let Some(c) = n.as_variant::<types_nodes::parsenodes::AlterTableCmd>() {
        out.push_str("{ALTERTABLECMD");
        int_field(out, "subtype", c.subtype as i32);
        string_field(out, "name", c.name);
        int_field(out, "num", c.num as i32);
        node_field(out, "newowner", c.newowner);
        node_field(out, "def", c.def);
        int_field(out, "behavior", c.behavior as i32);
        bool_field(out, "missing_ok", c.missing_ok);
        bool_field(out, "recurse", c.recurse);
        out.push('}');
    } else if let Some(c) = n.as_variant::<types_nodes::rawnodes::ColumnDef>() {
        out.push_str("{COLUMNDEF");
        string_field(out, "colname", c.colname);
        node_field(out, "typeName", c.typeName);
        string_field(out, "compression", c.compression);
        int_field(out, "inhcount", c.inhcount as i32);
        bool_field(out, "is_local", c.is_local);
        bool_field(out, "is_not_null", c.is_not_null);
        bool_field(out, "is_from_type", c.is_from_type);
        char_field(out, "storage", c.storage);
        string_field(out, "storage_name", c.storage_name);
        node_field(out, "raw_default", c.raw_default);
        node_field(out, "cooked_default", c.cooked_default);
        char_field(out, "identity", c.identity);
        out.push_str(" :identitySequence ");
        match c.identitySequence {
            Some(rv) => range_var(out, rv),
            None => out.push_str("<>"),
        }
        char_field(out, "generated", c.generated);
        node_field(out, "collClause", c.collClause);
        int_field(out, "collOid", c.collOid as i32);
        list_field(out, "constraints", &c.constraints);
        list_field(out, "fdwoptions", &c.fdwoptions);
        int_field(out, "location", c.location);
        out.push('}');
    } else if let Some(c) = n.as_create_domain_stmt() {
        out.push_str("{CREATEDOMAINSTMT");
        list_field(out, "domainname", &c.domainname);
        node_field(out, "typeName", c.typeName);
        node_field(out, "collClause", c.collClause);
        list_field(out, "constraints", &c.constraints);
        out.push('}');
    } else if let Some(c) = n.as_composite_type_stmt() {
        out.push_str("{COMPOSITETYPESTMT :typevar ");
        match c.typevar {
            Some(rv) => range_var(out, rv),
            None => out.push_str("<>"),
        }
        list_field(out, "coldeflist", &c.coldeflist);
        out.push('}');
    } else if let Some(a) = n.as_alter_ts_dictionary_stmt() {
        out.push_str("{ALTERTSDICTIONARYSTMT");
        list_field(out, "dictname", &a.dictname);
        list_field(out, "options", &a.options);
        out.push('}');
    } else if let Some(a) = n.as_alter_ts_configuration_stmt() {
        out.push_str("{ALTERTSCONFIGURATIONSTMT");
        int_field(out, "kind", a.kind as i32);
        list_field(out, "cfgname", &a.cfgname);
        list_field(out, "tokentype", &a.tokentype);
        list_field(out, "dicts", &a.dicts);
        bool_field(out, "override", a.r#override);
        bool_field(out, "replace", a.replace);
        bool_field(out, "missing_ok", a.missing_ok);
        out.push('}');
    } else if let Some(a) = n.as_a_array_expr() {
        out.push_str("{A_ARRAYEXPR");
        list_field(out, "elements", &a.elements);
        int_field(out, "list_start", a.list_start);
        int_field(out, "list_end", a.list_end);
        int_field(out, "location", a.location);
        out.push('}');
    } else if let Some(c) = n.as_create_enum_stmt() {
        out.push_str("{CREATEENUMSTMT");
        list_field(out, "typeName", &c.typeName);
        list_field(out, "vals", &c.vals);
        out.push('}');
    } else if let Some(c) = n.as_create_range_stmt() {
        out.push_str("{CREATERANGESTMT");
        list_field(out, "typeName", &c.typeName);
        list_field(out, "params", &c.params);
        out.push('}');
    } else if let Some(c) = n.as_alter_type_stmt() {
        out.push_str("{ALTERTYPESTMT");
        list_field(out, "typeName", &c.typeName);
        list_field(out, "options", &c.options);
        out.push('}');
    } else if let Some(c) = n.as_alter_enum_stmt() {
        out.push_str("{ALTERENUMSTMT");
        list_field(out, "typeName", &c.typeName);
        string_field(out, "oldVal", c.oldVal);
        string_field(out, "newVal", c.newVal);
        string_field(out, "newValNeighbor", c.newValNeighbor);
        bool_field(out, "newValIsAfter", c.newValIsAfter);
        bool_field(out, "skipIfNewValExists", c.skipIfNewValExists);
        out.push('}');
    } else if let Some(c) = n.as_variant::<types_nodes::rawnodes::Constraint>() {
        // Fields absent from the ported Constraint render as palloc0 defaults.
        out.push_str("{CONSTRAINT");
        int_field(out, "contype", c.contype as i32);
        string_field(out, "conname", c.conname);
        bool_field(out, "deferrable", c.deferrable);
        bool_field(out, "initdeferred", c.initdeferred);
        bool_field(out, "is_enforced", c.is_enforced);
        bool_field(out, "skip_validation", c.skip_validation);
        bool_field(out, "initially_valid", c.initially_valid);
        bool_field(out, "is_no_inherit", c.is_no_inherit);
        node_field(out, "raw_expr", c.raw_expr);
        string_field(out, "cooked_expr", c.cooked_expr);
        char_field(out, "generated_when", c.generated_when);
        char_field(out, "generated_kind", c.generated_kind);
        bool_field(out, "nulls_not_distinct", c.nulls_not_distinct);
        list_field(out, "keys", &c.keys);
        bool_field(out, "without_overlaps", c.without_overlaps);
        list_field(out, "including", &c.including);
        list_field(out, "exclusions", &c.exclusions);
        list_field(out, "options", &c.options);
        string_field(out, "indexname", c.indexname);
        string_field(out, "indexspace", c.indexspace);
        bool_field(out, "reset_default_tblspc", false);
        string_field(out, "access_method", c.access_method);
        node_field(out, "where_clause", c.where_clause);
        out.push_str(" :pktable ");
        match c.pktable {
            Some(rv) => range_var(out, rv),
            None => out.push_str("<>"),
        }
        list_field(out, "fk_attrs", &c.fk_attrs);
        list_field(out, "pk_attrs", &c.pk_attrs);
        bool_field(out, "fk_with_period", c.fk_with_period);
        bool_field(out, "pk_with_period", c.pk_with_period);
        char_field(out, "fk_matchtype", c.fk_matchtype);
        char_field(out, "fk_upd_action", c.fk_upd_action);
        char_field(out, "fk_del_action", c.fk_del_action);
        list_field(out, "fk_del_set_cols", &c.fk_del_set_cols);
        oid_list_field(out, "old_conpfeqop", &c.old_conpfeqop);
        int_field(out, "old_pktable_oid", c.old_pktable_oid as i32);
        int_field(out, "location", c.location);
        out.push('}');
    } else if let Some(st) = n.as_string() {
        out.push('"');
        if !st.sval.is_empty() {
            out_token(out, Some(st.sval));
        }
        out.push('"');
    } else if let Some(cs) = n.as_variant::<types_nodes::rawnodes::CreateStmt>() {
        out.push_str("{CREATESTMT :relation ");
        match cs.relation {
            Some(rv) => range_var(out, rv),
            None => out.push_str("<>"),
        }
        list_field(out, "tableElts", &cs.tableElts);
        list_field(out, "inhRelations", &cs.inhRelations);
        node_field(out, "partbound", cs.partbound);
        node_field(out, "partspec", cs.partspec);
        node_field(out, "ofTypename", cs.ofTypename);
        list_field(out, "constraints", &cs.constraints);
        list_field(out, "nnconstraints", &cs.nnconstraints);
        list_field(out, "options", &cs.options);
        int_field(out, "oncommit", cs.oncommit as i32);
        string_field(out, "tablespacename", cs.tablespacename);
        string_field(out, "accessMethod", cs.accessMethod);
        bool_field(out, "if_not_exists", cs.if_not_exists);
        out.push('}');
    } else if let Some(cd) = n.as_variant::<types_nodes::rawnodes::ColumnDef>() {
        out.push_str("{COLUMNDEF");
        string_field(out, "colname", cd.colname);
        node_field(out, "typeName", cd.typeName);
        string_field(out, "compression", cd.compression);
        int_field(out, "inhcount", cd.inhcount as i32);
        bool_field(out, "is_local", cd.is_local);
        bool_field(out, "is_not_null", cd.is_not_null);
        bool_field(out, "is_from_type", cd.is_from_type);
        char_field(out, "storage", cd.storage);
        string_field(out, "storage_name", cd.storage_name);
        node_field(out, "raw_default", cd.raw_default);
        node_field(out, "cooked_default", cd.cooked_default);
        char_field(out, "identity", cd.identity);
        out.push_str(" :identitySequence ");
        match cd.identitySequence {
            Some(rv) => range_var(out, rv),
            None => out.push_str("<>"),
        }
        char_field(out, "generated", cd.generated);
        node_field(out, "collClause", cd.collClause);
        int_field(out, "collOid", cd.collOid as i32);
        list_field(out, "constraints", &cd.constraints);
        list_field(out, "fdwoptions", &cd.fdwoptions);
        int_field(out, "location", cd.location);
        out.push('}');
    } else if let Some(tn) = n.as_variant::<types_nodes::rawnodes::TypeName>() {
        out.push_str("{TYPENAME");
        list_field(out, "names", &tn.names);
        int_field(out, "typeOid", tn.typeOid as i32);
        bool_field(out, "setof", tn.setof);
        bool_field(out, "pct_type", tn.pct_type);
        list_field(out, "typmods", &tn.typmods);
        int_field(out, "typemod", tn.typemod);
        list_field(out, "arrayBounds", &tn.arrayBounds);
        int_field(out, "location", tn.location);
        out.push('}');
    } else if let Some(p) = n.as_variant::<types_nodes::rawnodes::PartitionSpec>() {
        out.push_str("{PARTITIONSPEC");
        int_field(out, "strategy", p.strategy as i32);
        list_field(out, "partParams", &p.partParams);
        int_field(out, "location", p.location);
        out.push('}');
    } else if let Some(e) = n.as_variant::<types_nodes::rawnodes::PartitionElem>() {
        out.push_str("{PARTITIONELEM");
        string_field(out, "name", e.name);
        node_field(out, "expr", e.expr);
        list_field(out, "collation", &e.collation);
        list_field(out, "opclass", &e.opclass);
        int_field(out, "location", e.location);
        out.push('}');
    } else if let Some(b) = n.as_variant::<types_nodes::rawnodes::PartitionBoundSpec>() {
        out.push_str("{PARTITIONBOUNDSPEC");
        char_field(out, "strategy", b.strategy);
        bool_field(out, "is_default", b.is_default);
        int_field(out, "modulus", b.modulus);
        int_field(out, "remainder", b.remainder);
        list_field(out, "listdatums", &b.listdatums);
        list_field(out, "lowerdatums", &b.lowerdatums);
        list_field(out, "upperdatums", &b.upperdatums);
        int_field(out, "location", b.location);
        out.push('}');
    } else if let Some(d) = n.as_variant::<types_nodes::rawnodes::PartitionRangeDatum>() {
        out.push_str("{PARTITIONRANGEDATUM");
        int_field(out, "kind", d.kind as i32);
        node_field(out, "value", d.value);
        int_field(out, "location", d.location);
        out.push('}');
    } else if let Some(c) = n.as_variant::<types_nodes::rawnodes::PartitionCmd>() {
        out.push_str("{PARTITIONCMD");
        out.push_str(" :name ");
        match c.name {
            Some(rv) => range_var(out, rv),
            None => out.push_str("<>"),
        }
        node_field(out, "bound", c.bound);
        bool_field(out, "concurrent", c.concurrent);
        out.push('}');
    } else if let Some(rts) = n.as_variant::<types_nodes::RangeTableSample>() {
        out.push_str("{RANGETABLESAMPLE");
        node_field(out, "relation", rts.relation);
        list_field(out, "method", &rts.method);
        list_field(out, "args", &rts.args);
        node_field(out, "repeatable", rts.repeatable);
        int_field(out, "location", rts.location);
        out.push('}');
    } else if let Some(rf) = n.as_variant::<types_nodes::RangeFunction>() {
        out.push_str("{RANGEFUNCTION");
        bool_field(out, "lateral", rf.lateral);
        bool_field(out, "ordinality", rf.ordinality);
        bool_field(out, "is_rowsfrom", rf.is_rowsfrom);
        list_field(out, "functions", &rf.functions);
        out.push_str(" :alias ");
        match rf.alias {
            Some(a) => alias(out, a),
            None => out.push_str("<>"),
        }
        list_field(out, "coldeflist", &rf.coldeflist);
        out.push('}');
    } else if let Some(r) = n.as_variant::<types_nodes::parsenodes::ReplicaIdentityStmt>() {
        out.push_str("{REPLICAIDENTITYSTMT");
        char_field(out, "identity_type", r.identity_type);
        string_field(out, "name", r.name);
        out.push('}');
    } else if let Some(lc) = n.as_locking_clause() {
        out.push_str("{LOCKINGCLAUSE");
        list_field(out, "lockedRels", &lc.lockedRels);
        int_field(out, "strength", lc.strength as i32);
        int_field(out, "waitPolicy", lc.waitPolicy as i32);
        out.push('}');
    } else if let Some(c) = n.as_variant::<types_nodes::parsenodes::CreatePolicyStmt>() {
        out.push_str("{CREATEPOLICYSTMT");
        string_field(out, "policy_name", c.policy_name);
        out.push_str(" :table ");
        match c.table {
            Some(rv) => range_var(out, rv),
            None => out.push_str("<>"),
        }
        string_field(out, "cmd_name", c.cmd_name);
        bool_field(out, "permissive", c.permissive);
        list_field(out, "roles", &c.roles);
        node_field(out, "qual", c.qual);
        node_field(out, "with_check", c.with_check);
        out.push('}');
    } else if let Some(a) = n.as_variant::<types_nodes::parsenodes::AlterPolicyStmt>() {
        out.push_str("{ALTERPOLICYSTMT");
        string_field(out, "policy_name", a.policy_name);
        out.push_str(" :table ");
        match a.table {
            Some(rv) => range_var(out, rv),
            None => out.push_str("<>"),
        }
        list_field(out, "roles", &a.roles);
        node_field(out, "qual", a.qual);
        node_field(out, "with_check", a.with_check);
        out.push('}');
    } else if let Some(pt) = n.as_variant::<types_nodes::parsenodes::PublicationTable>() {
        publication_table(out, pt);
    } else if let Some(p) = n.as_variant::<types_nodes::parsenodes::PublicationObjSpec>() {
        out.push_str("{PUBLICATIONOBJSPEC");
        int_field(out, "pubobjtype", p.pubobjtype as i32);
        string_field(out, "name", p.name);
        out.push_str(" :pubtable ");
        match p.pubtable {
            Some(pt) => publication_table(out, pt),
            None => out.push_str("<>"),
        }
        int_field(out, "location", p.location);
        out.push('}');
    } else if let Some(c) = n.as_variant::<types_nodes::parsenodes::CreatePublicationStmt>() {
        out.push_str("{CREATEPUBLICATIONSTMT");
        string_field(out, "pubname", c.pubname);
        list_field(out, "options", &c.options);
        list_field(out, "pubobjects", &c.pubobjects);
        bool_field(out, "for_all_tables", c.for_all_tables);
        out.push('}');
    } else if let Some(a) = n.as_variant::<types_nodes::parsenodes::AlterPublicationStmt>() {
        out.push_str("{ALTERPUBLICATIONSTMT");
        string_field(out, "pubname", a.pubname);
        list_field(out, "options", &a.options);
        list_field(out, "pubobjects", &a.pubobjects);
        bool_field(out, "for_all_tables", a.for_all_tables);
        int_field(out, "action", a.action as i32);
        out.push('}');
    } else if let Some(c) = n.as_variant::<types_nodes::parsenodes::CreateSubscriptionStmt>() {
        out.push_str("{CREATESUBSCRIPTIONSTMT");
        string_field(out, "subname", c.subname);
        string_field(out, "conninfo", c.conninfo);
        list_field(out, "publication", &c.publication);
        list_field(out, "options", &c.options);
        out.push('}');
    } else if let Some(a) = n.as_variant::<types_nodes::parsenodes::AlterSubscriptionStmt>() {
        out.push_str("{ALTERSUBSCRIPTIONSTMT");
        int_field(out, "kind", a.kind as i32);
        string_field(out, "subname", a.subname);
        string_field(out, "conninfo", a.conninfo);
        list_field(out, "publication", &a.publication);
        list_field(out, "options", &a.options);
        out.push('}');
    } else if let Some(d) = n.as_variant::<types_nodes::parsenodes::DropSubscriptionStmt>() {
        out.push_str("{DROPSUBSCRIPTIONSTMT");
        string_field(out, "subname", d.subname);
        bool_field(out, "missing_ok", d.missing_ok);
        int_field(out, "behavior", d.behavior as i32);
        out.push('}');
    } else if let Some(o) = n.as_object_with_args() {
        object_with_args(out, o);
    } else if let Some(p) = n.as_function_parameter() {
        out.push_str("{FUNCTIONPARAMETER");
        string_field(out, "name", p.name);
        node_field(out, "argType", p.argType);
        int_field(out, "mode", p.mode as i32);
        node_field(out, "defexpr", p.defexpr);
        int_field(out, "location", p.location);
        out.push('}');
    } else if let Some(c) = n.as_variant::<types_nodes::parsenodes::CreateTableSpaceStmt>() {
        out.push_str("{CREATETABLESPACESTMT");
        string_field(out, "tablespacename", c.tablespacename);
        out.push_str(" :owner ");
        match c.owner {
            Some(r) => role_spec(out, r),
            None => out.push_str("<>"),
        }
        string_field(out, "location", c.location);
        list_field(out, "options", &c.options);
        out.push('}');
    } else if let Some(d) = n.as_variant::<types_nodes::parsenodes::DropTableSpaceStmt>() {
        out.push_str("{DROPTABLESPACESTMT");
        string_field(out, "tablespacename", d.tablespacename);
        bool_field(out, "missing_ok", d.missing_ok);
        out.push('}');
    } else if let Some(a) = n.as_variant::<types_nodes::parsenodes::AlterTableSpaceOptionsStmt>() {
        out.push_str("{ALTERTABLESPACEOPTIONSSTMT");
        string_field(out, "tablespacename", a.tablespacename);
        list_field(out, "options", &a.options);
        bool_field(out, "isReset", a.isReset);
        out.push('}');
    } else if let Some(r) = n.as_variant::<types_nodes::parsenodes::RenameStmt>() {
        out.push_str("{RENAMESTMT");
        int_field(out, "renameType", r.renameType as i32);
        int_field(out, "relationType", r.relationType as i32);
        out.push_str(" :relation ");
        match r.relation {
            Some(rv) => range_var(out, rv),
            None => out.push_str("<>"),
        }
        node_field(out, "object", r.object);
        string_field(out, "subname", r.subname);
        string_field(out, "newname", r.newname);
        int_field(out, "behavior", r.behavior as i32);
        bool_field(out, "missing_ok", r.missing_ok);
        out.push('}');
    } else if let Some(a) = n.as_alter_owner_stmt() {
        out.push_str("{ALTEROWNERSTMT");
        int_field(out, "objectType", a.objectType as i32);
        out.push_str(" :relation ");
        match a.relation {
            Some(rv) => range_var(out, rv),
            None => out.push_str("<>"),
        }
        node_field(out, "object", a.object);
        out.push_str(" :newowner ");
        match a.newowner {
            Some(r) => role_spec(out, r),
            None => out.push_str("<>"),
        }
        out.push('}');
    } else if let Some(a) = n.as_variant::<types_nodes::parsenodes::AlterCollationStmt>() {
        out.push_str("{ALTERCOLLATIONSTMT");
        list_field(out, "collname", &a.collname);
        out.push('}');
    } else if let Some(a) = n.as_variant::<types_nodes::parsenodes::AlterDomainStmt>() {
        out.push_str("{ALTERDOMAINSTMT :subtype ");
        out.push(a.subtype as char);
        list_field(out, "typeName", &a.typeName);
        string_field(out, "name", a.name);
        node_field(out, "def", a.def);
        int_field(out, "behavior", a.behavior as i32);
        bool_field(out, "missing_ok", a.missing_ok);
        out.push('}');
    } else if let Some(a) = n.as_variant::<types_nodes::parsenodes::AlterObjectSchemaStmt>() {
        out.push_str("{ALTEROBJECTSCHEMASTMT");
        int_field(out, "objectType", a.objectType as i32);
        out.push_str(" :relation ");
        match a.relation {
            Some(rv) => range_var(out, rv),
            None => out.push_str("<>"),
        }
        node_field(out, "object", a.object);
        string_field(out, "newschema", a.newschema);
        bool_field(out, "missing_ok", a.missing_ok);
        out.push('}');
    } else if let Some(a) = n.as_alter_function_stmt() {
        out.push_str("{ALTERFUNCTIONSTMT");
        int_field(out, "objtype", a.objtype as i32);
        out.push_str(" :func ");
        match a.func {
            Some(f) => object_with_args(out, f),
            None => out.push_str("<>"),
        }
        list_field(out, "actions", &a.actions);
        out.push('}');
    } else if let Some(m) = n.as_merge_support_func() {
        out.push_str("{MERGESUPPORTFUNC");
        int_field(out, "msftype", m.msftype as i32);
        int_field(out, "msfcollid", m.msfcollid as i32);
        int_field(out, "location", m.location);
        out.push('}');
    } else if let Some(m) = n.as_merge_stmt() {
        out.push_str("{MERGESTMT");
        node_field(out, "relation", m.relation);
        node_field(out, "sourceRelation", m.sourceRelation);
        node_field(out, "joinCondition", m.joinCondition);
        list_field(out, "mergeWhenClauses", &m.mergeWhenClauses);
        node_field(out, "returningClause", m.returningClause);
        node_field(out, "withClause", m.withClause);
        out.push('}');
    } else if let Some(w) = n.as_merge_when_clause() {
        out.push_str("{MERGEWHENCLAUSE");
        int_field(out, "matchKind", w.matchKind as i32);
        int_field(out, "commandType", w.commandType as i32);
        int_field(out, "override", w.r#override as i32);
        node_field(out, "condition", w.condition);
        list_field(out, "targetList", &w.targetList);
        list_field(out, "values", &w.values);
        out.push('}');
    } else if let Some(rc) = n.as_returning_clause() {
        out.push_str("{RETURNINGCLAUSE");
        list_field(out, "options", &rc.options);
        list_field(out, "exprs", &rc.exprs);
        out.push('}');
    } else if let Some(f) = n.as_json_format() {
        json_format(out, f);
    } else if let Some(r) = n.as_json_returning() {
        json_returning(out, r);
    } else if let Some(v) = n.as_json_value_expr() {
        out.push_str("{JSONVALUEEXPR");
        node_field(out, "raw_expr", v.raw_expr);
        node_field(out, "formatted_expr", v.formatted_expr);
        json_format_field(out, "format", v.format);
        out.push('}');
    } else if let Some(b) = n.as_json_behavior() {
        out.push_str("{JSONBEHAVIOR");
        int_field(out, "btype", b.btype as i32);
        node_field(out, "expr", b.expr);
        bool_field(out, "coerce", b.coerce);
        int_field(out, "location", b.location);
        out.push('}');
    } else if let Some(p) = n.as_json_is_predicate() {
        out.push_str("{JSONISPREDICATE");
        node_field(out, "expr", p.expr);
        json_format_field(out, "format", p.format);
        int_field(out, "item_type", p.item_type as i32);
        bool_field(out, "unique_keys", p.unique_keys);
        int_field(out, "location", p.location);
        out.push('}');
    } else if let Some(o) = n.as_json_output() {
        out.push_str("{JSONOUTPUT");
        node_field(out, "typeName", o.typeName);
        out.push_str(" :returning ");
        match o.returning {
            Some(r) => json_returning(out, r),
            None => out.push_str("<>"),
        }
        out.push('}');
    } else if let Some(a) = n.as_json_argument() {
        out.push_str("{JSONARGUMENT");
        node_field(out, "val", a.val);
        string_field(out, "name", a.name);
        out.push('}');
    } else if let Some(f) = n.as_json_func_expr() {
        out.push_str("{JSONFUNCEXPR");
        int_field(out, "op", f.op as i32);
        string_field(out, "column_name", f.column_name);
        node_field(out, "context_item", f.context_item);
        node_field(out, "pathspec", f.pathspec);
        list_field(out, "passing", &f.passing);
        node_field(out, "output", f.output);
        node_field(out, "on_empty", f.on_empty);
        node_field(out, "on_error", f.on_error);
        int_field(out, "wrapper", f.wrapper as i32);
        int_field(out, "quotes", f.quotes as i32);
        int_field(out, "location", f.location);
        out.push('}');
    } else if let Some(ps) = n.as_json_table_path_spec() {
        out.push_str("{JSONTABLEPATHSPEC");
        node_field(out, "string", ps.string);
        string_field(out, "name", ps.name);
        int_field(out, "name_location", ps.name_location);
        int_field(out, "location", ps.location);
        out.push('}');
    } else if let Some(jt) = n.as_json_table() {
        out.push_str("{JSONTABLE");
        node_field(out, "context_item", jt.context_item);
        node_field(out, "pathspec", jt.pathspec);
        list_field(out, "passing", &jt.passing);
        list_field(out, "columns", &jt.columns);
        node_field(out, "on_error", jt.on_error);
        out.push_str(" :alias ");
        match jt.alias {
            Some(a) => alias(out, a),
            None => out.push_str("<>"),
        }
        bool_field(out, "lateral", jt.lateral);
        int_field(out, "location", jt.location);
        out.push('}');
    } else if let Some(jtc) = n.as_json_table_column() {
        out.push_str("{JSONTABLECOLUMN");
        int_field(out, "coltype", jtc.coltype as i32);
        string_field(out, "name", jtc.name);
        node_field(out, "typeName", jtc.typeName);
        node_field(out, "pathspec", jtc.pathspec);
        json_format_field(out, "format", jtc.format);
        int_field(out, "wrapper", jtc.wrapper as i32);
        int_field(out, "quotes", jtc.quotes as i32);
        list_field(out, "columns", &jtc.columns);
        node_field(out, "on_empty", jtc.on_empty);
        node_field(out, "on_error", jtc.on_error);
        int_field(out, "location", jtc.location);
        out.push('}');
    } else if let Some(kv) = n.as_json_key_value() {
        out.push_str("{JSONKEYVALUE");
        node_field(out, "key", kv.key);
        node_field(out, "value", kv.value);
        out.push('}');
    } else if let Some(p) = n.as_json_parse_expr() {
        out.push_str("{JSONPARSEEXPR");
        node_field(out, "expr", p.expr);
        node_field(out, "output", p.output);
        bool_field(out, "unique_keys", p.unique_keys);
        int_field(out, "location", p.location);
        out.push('}');
    } else if let Some(s) = n.as_json_scalar_expr() {
        out.push_str("{JSONSCALAREXPR");
        node_field(out, "expr", s.expr);
        node_field(out, "output", s.output);
        int_field(out, "location", s.location);
        out.push('}');
    } else if let Some(s) = n.as_json_serialize_expr() {
        out.push_str("{JSONSERIALIZEEXPR");
        node_field(out, "expr", s.expr);
        node_field(out, "output", s.output);
        int_field(out, "location", s.location);
        out.push('}');
    } else if let Some(c) = n.as_json_object_constructor() {
        out.push_str("{JSONOBJECTCONSTRUCTOR");
        list_field(out, "exprs", &c.exprs);
        node_field(out, "output", c.output);
        bool_field(out, "absent_on_null", c.absent_on_null);
        bool_field(out, "unique", c.unique);
        int_field(out, "location", c.location);
        out.push('}');
    } else if let Some(c) = n.as_json_array_constructor() {
        out.push_str("{JSONARRAYCONSTRUCTOR");
        list_field(out, "exprs", &c.exprs);
        node_field(out, "output", c.output);
        bool_field(out, "absent_on_null", c.absent_on_null);
        int_field(out, "location", c.location);
        out.push('}');
    } else if let Some(c) = n.as_json_array_query_constructor() {
        out.push_str("{JSONARRAYQUERYCONSTRUCTOR");
        node_field(out, "query", c.query);
        node_field(out, "output", c.output);
        json_format_field(out, "format", c.format);
        bool_field(out, "absent_on_null", c.absent_on_null);
        int_field(out, "location", c.location);
        out.push('}');
    } else if let Some(c) = n.as_json_agg_constructor() {
        out.push_str("{JSONAGGCONSTRUCTOR");
        node_field(out, "output", c.output);
        node_field(out, "agg_filter", c.agg_filter);
        list_field(out, "agg_order", &c.agg_order);
        node_field(out, "over", c.over);
        int_field(out, "location", c.location);
        out.push('}');
    } else if let Some(a) = n.as_json_object_agg() {
        out.push_str("{JSONOBJECTAGG");
        node_field(out, "constructor", a.constructor);
        node_field(out, "arg", a.arg);
        bool_field(out, "absent_on_null", a.absent_on_null);
        bool_field(out, "unique", a.unique);
        out.push('}');
    } else if let Some(a) = n.as_json_array_agg() {
        out.push_str("{JSONARRAYAGG");
        node_field(out, "constructor", a.constructor);
        node_field(out, "arg", a.arg);
        bool_field(out, "absent_on_null", a.absent_on_null);
        out.push('}');
    } else if let Some(i) = n.as_insert_stmt() {
        out.push_str("{INSERTSTMT");
        node_field(out, "relation", i.relation);
        list_field(out, "cols", &i.cols);
        node_field(out, "selectStmt", i.selectStmt);
        node_field(out, "onConflictClause", i.onConflictClause);
        node_field(out, "returningClause", i.returningClause);
        node_field(out, "withClause", i.withClause);
        int_field(out, "override", i.r#override as i32);
        out.push('}');
    } else if let Some(d) = n.as_delete_stmt() {
        out.push_str("{DELETESTMT");
        node_field(out, "relation", d.relation);
        list_field(out, "usingClause", &d.usingClause);
        node_field(out, "whereClause", d.whereClause);
        node_field(out, "returningClause", d.returningClause);
        node_field(out, "withClause", d.withClause);
        out.push('}');
    } else if let Some(u) = n.as_update_stmt() {
        out.push_str("{UPDATESTMT");
        node_field(out, "relation", u.relation);
        list_field(out, "targetList", &u.targetList);
        node_field(out, "whereClause", u.whereClause);
        list_field(out, "fromClause", &u.fromClause);
        node_field(out, "returningClause", u.returningClause);
        node_field(out, "withClause", u.withClause);
        out.push('}');
    } else if let Some(st) = n.as_set_to_default() {
        out.push_str("{SETTODEFAULT");
        int_field(out, "typeId", st.typeId as i32);
        int_field(out, "typeMod", st.typeMod);
        int_field(out, "collation", st.collation as i32);
        int_field(out, "location", st.location);
        out.push('}');
    } else if let Some(v) = n.as_vacuum_stmt() {
        out.push_str("{VACUUMSTMT");
        list_field(out, "options", &v.options);
        list_field(out, "rels", &v.rels);
        bool_field(out, "is_vacuumcmd", v.is_vacuumcmd);
        out.push('}');
    } else if let Some(v) = n.as_vacuum_relation() {
        out.push_str("{VACUUMRELATION");
        node_field(out, "relation", v.relation);
        int_field(out, "oid", v.oid as i32);
        list_field(out, "va_cols", &v.va_cols);
        out.push('}');
    } else if let Some(c) = n.as_current_of_expr() {
        out.push_str("{CURRENTOFEXPR");
        int_field(out, "cvarno", c.cvarno as i32);
        string_field(out, "cursor_name", c.cursor_name);
        int_field(out, "cursor_param", c.cursor_param);
        out.push('}');
    } else if let Some(s) = n.as_variant::<types_nodes::rawnodes::CreateStatsStmt>() {
        out.push_str("{CREATESTATSSTMT");
        list_field(out, "defnames", &s.defnames);
        list_field(out, "stat_types", &s.stat_types);
        list_field(out, "exprs", &s.exprs);
        list_field(out, "relations", &s.relations);
        string_field(out, "stxcomment", s.stxcomment);
        bool_field(out, "transformed", s.transformed);
        bool_field(out, "if_not_exists", s.if_not_exists);
        out.push('}');
    } else if let Some(s) = n.as_variant::<types_nodes::rawnodes::StatsElem>() {
        out.push_str("{STATSELEM");
        string_field(out, "name", s.name);
        node_field(out, "expr", s.expr);
        out.push('}');
    } else if let Some(s) = n.as_variant::<types_nodes::rawnodes::AlterStatsStmt>() {
        out.push_str("{ALTERSTATSSTMT");
        list_field(out, "defnames", &s.defnames);
        node_field(out, "stxstattarget", s.stxstattarget);
        bool_field(out, "missing_ok", s.missing_ok);
        out.push('}');
    } else if let Some(s) = n.as_variant::<types_nodes::parsenodes::CreateSchemaStmt>() {
        out.push_str("{CREATESCHEMASTMT");
        string_field(out, "schemaname", s.schemaname);
        node_field(out, "authrole", s.authrole);
        list_field(out, "schemaElts", &s.schemaElts);
        bool_field(out, "if_not_exists", s.if_not_exists);
        out.push('}');
    } else if let Some(s) = n.as_variant::<types_nodes::parsenodes::CreateFunctionStmt>() {
        out.push_str("{CREATEFUNCTIONSTMT");
        bool_field(out, "is_procedure", s.is_procedure);
        bool_field(out, "replace", s.replace);
        list_field(out, "funcname", &s.funcname);
        list_field(out, "parameters", &s.parameters);
        node_field(out, "returnType", s.returnType);
        list_field(out, "options", &s.options);
        node_field(out, "sql_body", s.sql_body);
        out.push('}');
    } else if let Some(s) = n.as_variant::<types_nodes::parsenodes::ReturnStmt>() {
        out.push_str("{RETURNSTMT");
        node_field(out, "returnval", s.returnval);
        out.push('}');
    } else if let Some(o) = n.as_variant::<types_nodes::rawnodes::ReturningOption>() {
        out.push_str("{RETURNINGOPTION");
        int_field(out, "option", o.option as i32);
        string_field(out, "value", o.value);
        int_field(out, "location", o.location);
        out.push('}');
    } else {
        panic!("tests_dump: unrendered node tag {:?}", n.node_tag());
    }
}

fn object_with_args(out: &mut String, o: &types_nodes::parsenodes::ObjectWithArgs<'_>) {
    out.push_str("{OBJECTWITHARGS");
    list_field(out, "objname", &o.objname);
    opt_list_field(out, "objargs", &o.objargs);
    list_field(out, "objfuncargs", &o.objfuncargs);
    bool_field(out, "args_unspecified", o.args_unspecified);
    out.push('}');
}

fn variable_set_stmt(out: &mut String, v: &types_nodes::parsenodes::VariableSetStmt<'_>) {
    out.push_str("{VARIABLESETSTMT");
    int_field(out, "kind", v.kind as i32);
    string_field(out, "name", v.name);
    list_field(out, "args", &v.args);
    bool_field(out, "jumble_args", v.jumble_args);
    bool_field(out, "is_local", v.is_local);
    int_field(out, "location", v.location);
    out.push('}');
}

fn json_format(out: &mut String, f: &types_nodes::JsonFormat) {
    out.push_str("{JSONFORMAT");
    int_field(out, "format_type", f.format_type as i32);
    int_field(out, "encoding", f.encoding as i32);
    int_field(out, "location", f.location);
    out.push('}');
}

fn json_format_field(out: &mut String, name: &str, f: Option<&types_nodes::JsonFormat>) {
    out.push_str(" :");
    out.push_str(name);
    out.push(' ');
    match f {
        Some(f) => json_format(out, f),
        None => out.push_str("<>"),
    }
}

fn json_returning(out: &mut String, r: &types_nodes::JsonReturning<'_>) {
    out.push_str("{JSONRETURNING");
    json_format_field(out, "format", r.format);
    int_field(out, "typid", r.typid as i32);
    int_field(out, "typmod", r.typmod);
    out.push('}');
}

fn grant_stmt(out: &mut String, g: &types_nodes::parsenodes::GrantStmt<'_>) {
    out.push_str("{GRANTSTMT");
    bool_field(out, "is_grant", g.is_grant);
    int_field(out, "targtype", g.targtype as i32);
    int_field(out, "objtype", g.objtype as i32);
    list_field(out, "objects", &g.objects);
    list_field(out, "privileges", &g.privileges);
    list_field(out, "grantees", &g.grantees);
    bool_field(out, "grant_option", g.grant_option);
    out.push_str(" :grantor ");
    match g.grantor {
        Some(r) => role_spec(out, r),
        None => out.push_str("<>"),
    }
    int_field(out, "behavior", g.behavior as i32);
    out.push('}');
}

fn role_spec(out: &mut String, r: &types_nodes::parsenodes::RoleSpec<'_>) {
    out.push_str("{ROLESPEC");
    out.push_str(&format!(" :roletype {}", r.roletype as i32));
    string_field(out, "rolename", r.rolename);
    int_field(out, "location", r.location);
    out.push('}');
}

fn range_var(out: &mut String, rv: &types_nodes::RangeVar<'_>) {
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
}

fn char_field(out: &mut String, name: &str, c: u8) {
    out.push_str(" :");
    out.push_str(name);
    out.push(' ');
    if c == 0 {
        out.push_str("<>");
    } else {
        out_token(out, Some(std::str::from_utf8(std::slice::from_ref(&c)).unwrap()));
    }
}

fn select_stmt(out: &mut String, s: &types_nodes::SelectStmt<'_>) {
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
    out.push_str(" :larg ");
    match s.larg {
        Some(l) => select_stmt(out, l),
        None => out.push_str("<>"),
    }
    out.push_str(" :rarg ");
    match s.rarg {
        Some(r) => select_stmt(out, r),
        None => out.push_str("<>"),
    }
    out.push('}');
}

fn publication_table(out: &mut String, pt: &types_nodes::parsenodes::PublicationTable<'_>) {
    out.push_str("{PUBLICATIONTABLE");
    out.push_str(" :relation ");
    match pt.relation {
        Some(rv) => range_var(out, rv),
        None => out.push_str("<>"),
    }
    node_field(out, "whereClause", pt.whereClause);
    list_field(out, "columns", &pt.columns);
    out.push('}');
}

fn alias(out: &mut String, a: &types_nodes::Alias<'_>) {
    out.push_str("{ALIAS");
    string_field(out, "aliasname", a.aliasname);
    list_field(out, "colnames", &a.colnames);
    out.push('}');
}

fn func_call(out: &mut String, f: &types_nodes::rawnodes::FuncCall<'_>) {
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
        let got = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_one(stmt)))
        {
            Ok(g) => g,
            Err(e) => {
                let msg = e
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| e.downcast_ref::<String>().cloned())
                    .unwrap_or_default();
                failures.push(format!("stmt {stmt:?} PANICKED: {msg}"));
                continue;
            }
        };
        if got != *want {
            failures.push(format!("stmt {stmt:?}\n  C:    {want}\n  rust: {got}"));
        }
    }
    assert!(failures.is_empty(), "{} mismatches:\n{}", failures.len(), failures.join("\n"));
}
