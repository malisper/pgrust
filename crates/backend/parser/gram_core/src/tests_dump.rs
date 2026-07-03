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
    } else if let Some(rv) = n.as_range_var() {
        range_var(out, rv);
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
    } else if let Some(g) = n.as_grant_stmt() {
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
    } else if let Some(ap) = n.as_access_priv() {
        out.push_str("{ACCESSPRIV");
        string_field(out, "priv_name", ap.priv_name);
        list_field(out, "cols", &ap.cols);
        out.push('}');
    } else if let Some(r) = n.as_role_spec() {
        role_spec(out, r);
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
    } else if let Some(d) = n.as_drop_stmt() {
        out.push_str("{DROPSTMT");
        list_field(out, "objects", &d.objects);
        int_field(out, "removeType", d.removeType as i32);
        int_field(out, "behavior", d.behavior as i32);
        bool_field(out, "missing_ok", d.missing_ok);
        bool_field(out, "concurrent", d.concurrent);
        out.push('}');
    } else if let Some(v) = n.as_variable_set_stmt() {
        out.push_str("{VARIABLESETSTMT");
        int_field(out, "kind", v.kind as i32);
        string_field(out, "name", v.name);
        list_field(out, "args", &v.args);
        bool_field(out, "jumble_args", v.jumble_args);
        bool_field(out, "is_local", v.is_local);
        int_field(out, "location", v.location);
        out.push('}');
    } else if let Some(v) = n.as_variable_show_stmt() {
        out.push_str("{VARIABLESHOWSTMT");
        string_field(out, "name", v.name);
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
        bool_field(out, "without_overlaps", false);
        list_field(out, "including", &c.including);
        out.push_str(" :exclusions <>");
        list_field(out, "options", &c.options);
        string_field(out, "indexname", c.indexname);
        string_field(out, "indexspace", c.indexspace);
        bool_field(out, "reset_default_tblspc", false);
        out.push_str(" :access_method <> :where_clause <> :pktable ");
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
        list_field(out, "old_conpfeqop", &c.old_conpfeqop);
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
    } else if let Some(lc) = n.as_locking_clause() {
        out.push_str("{LOCKINGCLAUSE");
        list_field(out, "lockedRels", &lc.lockedRels);
        int_field(out, "strength", lc.strength as i32);
        int_field(out, "waitPolicy", lc.waitPolicy as i32);
        out.push('}');
    } else {
        panic!("tests_dump: unrendered node tag {:?}", n.node_tag());
    }
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
