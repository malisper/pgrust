//! read.c + readfuncs.c, minimal arm: exactly the node set a stored view
//! SELECT rule (pg_rewrite ev_action) can contain; every other node label or
//! token shape is a loud panic naming the C reader.

#![allow(non_snake_case)]

use datum::Datum;
use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::bitmapset::Bitmapset;
use types_nodes::list::{IntList, NodeList, OidList, OptNodeList};
use types_nodes::jointype::JoinType;
use types_nodes::nodes_enums::{CmdType, LimitOption};
use types_nodes::parsenodes::{
    CTECycleClause, CTEMaterialize, CTESearchClause, CommonTableExpr, NotifyStmt, Query,
    QuerySource, RTEKind, RTEPermissionInfo, RangeTblEntry, RangeTblFunction, SetOperation,
    SetOperationStmt, SortGroupClause, WindowClause,
};
use types_nodes::primnodes::{
    Aggref, Alias, ArrayExpr, BoolExpr, BoolExprType, CaseExpr, CaseTestExpr, CaseWhen,
    CoalesceExpr, CoerceViaIO, CoercionForm, Const, FieldSelect, FieldStore, FromExpr, FuncExpr,
    JoinExpr, MergeAction,
    MergeMatchKind, MinMaxExpr, MinMaxOp, NamedArgExpr, NextValueExpr, NullTest, NullTestType,
    OpExpr, OverridingKind, Param, ParamKind, RangeTblRef, CollateExpr, RelabelType,
    ScalarArrayOpExpr, SubLink, SubLinkType, TableFunc, TableFuncType, TargetEntry, Var,
    VarReturningType, WindowFunc, XmlExpr, XmlExprOp, XmlOptionType,
};
use types_nodes::Node;

#[cfg(test)]
mod tests;

// stringToNode (read.c) for node strings that may be the two-character
// null-node marker "<>" (outfuncs writes it for a NULL node; pg_rewrite's
// ev_qual column holds it on every unconditional rule — 146/147 rows in a
// fresh catalog). C's stringToNode returns NULL there; this returns Ok(None).
// SQL-reachable readers of such columns (pg_get_expr) MUST use this entry.
pub fn stringToNodeNullable<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Option<Node<'mcx>>> {
    let mut r = Reader { mcx, buf: s.as_bytes(), pos: 0 };
    r.node_read().expect("stringToNode: empty input")
}

// stringToNode (read.c) for the call sites whose C counterpart dereferences
// the result unconditionally — the catalog columns they read never hold "<>"
// (a NULL there would crash C). Loud panic per this crate's charter; columns
// that CAN hold "<>" go through stringToNodeNullable, or pre-filter the
// marker like relcache rules.rs / ruledef.rs do for ev_qual.
pub fn stringToNode<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Node<'mcx>> {
    Ok(stringToNodeNullable(mcx, s)?
        .expect("stringToNode: <> (null node) in a never-null node column"))
}

struct Reader<'a, 'mcx> {
    mcx: Mcx<'mcx>,
    buf: &'a [u8],
    pos: usize,
}

const SPECIALS: &[u8] = b"(){}";

fn is_space(c: u8) -> bool {
    c == b' ' || c == b'\n' || c == b'\t'
}

impl<'a, 'mcx> Reader<'a, 'mcx> {
    // pg_strtok (read.c): "<>" comes back as an empty token (NULL marker).
    fn next_token(&mut self) -> Option<&'a [u8]> {
        while self.pos < self.buf.len() && is_space(self.buf[self.pos]) {
            self.pos += 1;
        }
        if self.pos >= self.buf.len() {
            return None;
        }
        let start = self.pos;
        if SPECIALS.contains(&self.buf[self.pos]) {
            self.pos += 1;
            return Some(&self.buf[start..self.pos]);
        }
        while self.pos < self.buf.len() {
            let c = self.buf[self.pos];
            if is_space(c) || SPECIALS.contains(&c) {
                break;
            }
            if c == b'\\' && self.pos + 1 < self.buf.len() {
                self.pos += 2;
            } else {
                self.pos += 1;
            }
        }
        let tok = &self.buf[start..self.pos];
        if tok == b"<>" {
            return Some(b"");
        }
        Some(tok)
    }

    fn token(&mut self, what: &str) -> &'a [u8] {
        match self.next_token() {
            Some(t) => t,
            None => panic!("nodeRead (read.c): unterminated input reading {what}"),
        }
    }

    fn expect(&mut self, lit: &str) {
        let t = self.token(lit);
        assert!(
            t == lit.as_bytes(),
            "pg_strtok (read.c): expected {lit:?}, got {:?}",
            String::from_utf8_lossy(t)
        );
    }

    fn label(&mut self, name: &str) {
        let t = self.token(name);
        assert!(
            t.len() == name.len() + 1 && t[0] == b':' && &t[1..] == name.as_bytes(),
            "readfuncs.c: expected field :{name}, got {:?}",
            String::from_utf8_lossy(t)
        );
    }

    // debackslash (read.c) into the arena.
    fn arena_str(&self, tok: &[u8]) -> PgResult<&'mcx str> {
        let mut v: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(self.mcx, tok.len())?;
        let mut i = 0;
        while i < tok.len() {
            if tok[i] == b'\\' && i + 1 < tok.len() {
                i += 1;
            }
            v.push(tok[i]);
            i += 1;
        }
        let bytes = v.leak();
        Ok(core::str::from_utf8(bytes).expect("non-UTF-8 node token"))
    }

    fn read_bool(&mut self, name: &str) -> bool {
        self.label(name);
        match self.token(name) {
            b"true" => true,
            b"false" => false,
            t => panic!("READ_BOOL_FIELD: bad bool {:?}", String::from_utf8_lossy(t)),
        }
    }

    fn parse_int(tok: &[u8]) -> i64 {
        core::str::from_utf8(tok)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or_else(|| {
                panic!("readfuncs.c: bad integer token {:?}", String::from_utf8_lossy(tok))
            })
    }

    fn read_i32(&mut self, name: &str) -> i32 {
        self.label(name);
        Self::parse_int(self.token(name)) as i32
    }

    fn read_u32(&mut self, name: &str) -> u32 {
        self.label(name);
        Self::parse_int(self.token(name)) as u32
    }

    fn read_u64(&mut self, name: &str) -> u64 {
        self.label(name);
        Self::parse_int(self.token(name)) as u64
    }

    // READ_LOCATION_FIELD: consumed but restored to -1.
    fn read_location(&mut self, name: &str) -> i32 {
        self.label(name);
        let _ = self.token(name);
        -1
    }

    // READ_FLOAT_FIELD: C atof.
    fn read_f64(&mut self, name: &str) -> f64 {
        self.label(name);
        let t = self.token(name);
        core::str::from_utf8(t)
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0)
    }

    fn read_char(&mut self, name: &str) -> u8 {
        self.label(name);
        let t = self.token(name);
        if t.is_empty() {
            0
        } else if t[0] == b'\\' && t.len() > 1 {
            t[1]
        } else {
            t[0]
        }
    }

    fn read_str(&mut self, name: &str) -> PgResult<Option<&'mcx str>> {
        self.label(name);
        let t = self.token(name);
        if t.is_empty() {
            return Ok(None);
        }
        if t == b"\"\"" {
            return Ok(Some(""));
        }
        Ok(Some(self.arena_str(t)?))
    }

    fn read_node(&mut self, name: &str) -> PgResult<Option<Node<'mcx>>> {
        self.label(name);
        match self.node_read() {
            None => panic!("nodeRead (read.c): unterminated input at :{name}"),
            Some(n) => n,
        }
    }

    fn read_opt_node_list(
        &mut self,
        name: &str,
    ) -> PgResult<types_nodes::list::OptNodeList<'mcx>> {
        self.label(name);
        let t = self.token(name);
        if t.is_empty() {
            return Ok(types_nodes::list::OptNodeList::nil());
        }
        assert!(t == b"(", "readfuncs.c: field :{name} is not a node list");
        let mut l = types_nodes::list::OptNodeList::nil();
        loop {
            let tok = self.token("list");
            if tok == b")" {
                return Ok(l);
            }
            let elem = self.node_read_token(tok)?;
            l.lappend(self.mcx, elem)?;
        }
    }

    fn read_node_list(&mut self, name: &str) -> PgResult<NodeList<'mcx>> {
        self.label(name);
        let t = self.token(name);
        if t.is_empty() {
            return Ok(NodeList::nil());
        }
        assert!(t == b"(", "readfuncs.c: field :{name} is not a node list");
        let mut l = NodeList::nil();
        loop {
            let tok = self.token("list");
            if tok == b")" {
                return Ok(l);
            }
            // C nodeRead maps <> to a NIL list member; expanded grouping
            // sets carry one for GROUP BY () (read_list_body's convention).
            let elem = match self.node_read_token(tok)? {
                Some(n) => n,
                None => Node::mk_list(self.mcx, NodeList::nil())?,
            };
            l.lappend(self.mcx, elem)?;
        }
    }

    fn read_int_list(&mut self, name: &str) -> PgResult<IntList<'mcx>> {
        self.label(name);
        let t = self.token(name);
        if t.is_empty() {
            return Ok(IntList::nil());
        }
        assert!(t == b"(", "readfuncs.c: field :{name} is not an int list");
        self.expect("i");
        let mut l = IntList::nil();
        loop {
            let tok = self.token("int list");
            if tok == b")" {
                return Ok(l);
            }
            l.lappend(self.mcx, Self::parse_int(tok) as i32)?;
        }
    }

    fn read_oid_list(&mut self, name: &str) -> PgResult<OidList<'mcx>> {
        self.label(name);
        let t = self.token(name);
        if t.is_empty() {
            return Ok(OidList::nil());
        }
        assert!(t == b"(", "readfuncs.c: field :{name} is not an oid list");
        self.expect("o");
        let mut l = OidList::nil();
        loop {
            let tok = self.token("oid list");
            if tok == b")" {
                return Ok(l);
            }
            l.lappend(self.mcx, Self::parse_int(tok) as Oid)?;
        }
    }

    fn read_bitmapset(&mut self, name: &str) -> PgResult<Bitmapset<'mcx>> {
        self.label(name);
        self.expect("(");
        self.expect("b");
        let mut bms = Bitmapset::empty();
        loop {
            let t = self.token("bitmapset");
            if t == b")" {
                return Ok(bms);
            }
            bms.add_member(self.mcx, Self::parse_int(t) as i32)?;
        }
    }

    // nodeRead (read.c). None = end of input; Ok(None) = the "<>" token.
    fn node_read(&mut self) -> Option<PgResult<Option<Node<'mcx>>>> {
        let t = self.next_token()?;
        Some(self.node_read_token(t))
    }

    fn node_read_token(&mut self, t: &'a [u8]) -> PgResult<Option<Node<'mcx>>> {
        if t.is_empty() {
            return Ok(None);
        }
        if t == b"{" {
            let n = self.parse_node_string()?;
            self.expect("}");
            return Ok(Some(n));
        }
        if t == b"(" {
            return Ok(Some(self.read_list_body()?));
        }
        // Value tokens (list elements): the SELECT-rule set only carries
        // quoted strings (Alias colnames) and integers.
        if t.len() >= 2 && t[0] == b'"' && t[t.len() - 1] == b'"' {
            let s = self.arena_str(&t[1..t.len() - 1])?;
            return Ok(Some(Node::mk_string(self.mcx, s)?));
        }
        if t[0].is_ascii_digit() || (t[0] == b'-' && t.len() > 1 && t[1].is_ascii_digit()) {
            return Ok(Some(Node::mk_integer(self.mcx, Self::parse_int(t) as i32)?));
        }
        panic!(
            "nodeRead (read.c): unhandled token {:?} (view SELECT-rule read set)",
            String::from_utf8_lossy(t)
        );
    }

    fn read_list_body(&mut self) -> PgResult<Node<'mcx>> {
        let first = self.token("list");
        match first {
            b"i" => {
                let mut l = IntList::nil();
                loop {
                    let t = self.token("int list");
                    if t == b")" {
                        return Node::mk_int_list(self.mcx, l);
                    }
                    l.lappend(self.mcx, Self::parse_int(t) as i32)?;
                }
            }
            b"o" => {
                let mut l = OidList::nil();
                loop {
                    let t = self.token("oid list");
                    if t == b")" {
                        return Node::mk_oid_list(self.mcx, l);
                    }
                    l.lappend(self.mcx, Self::parse_int(t) as Oid)?;
                }
            }
            b"x" => panic!("nodeRead (read.c): xid list unported"),
            _ => {}
        }
        let mut l = NodeList::nil();
        let mut tok = first;
        loop {
            if tok == b")" {
                return Node::mk_list(self.mcx, l);
            }
            // C nodeRead maps <> to NIL; a NIL list element is C's empty
            // list (empty BEGIN ATOMIC body: list_make1(NIL)).
            let elem = match self.node_read_token(tok)? {
                Some(e) => e,
                None => Node::mk_list(self.mcx, NodeList::nil())?,
            };
            l.lappend(self.mcx, elem)?;
            tok = self.token("list");
        }
    }

    fn parse_node_string(&mut self) -> PgResult<Node<'mcx>> {
        let name = self.token("node label");
        match name {
            b"QUERY" => self.read_query(),
            b"RANGETBLENTRY" => self.read_range_tbl_entry(),
            b"RTEPERMISSIONINFO" => self.read_rte_permission_info(),
            b"ALIAS" => self.read_alias(),
            b"FROMEXPR" => self.read_from_expr(),
            b"JOINEXPR" => self.read_join_expr(),
            b"RANGETBLFUNCTION" => self.read_range_tbl_function(),
            b"RANGETBLREF" => self.read_range_tbl_ref(),
            b"TARGETENTRY" => self.read_target_entry(),
            b"VAR" => self.read_var(),
            b"PLACEHOLDERVAR" => self.read_place_holder_var(),
            b"CONST" => self.read_const(),
            b"OPEXPR" => self.read_op_expr(),
            b"FUNCEXPR" => self.read_func_expr(),
            b"BOOLEXPR" => self.read_bool_expr(),
            b"SQLVALUEFUNCTION" => self.read_sql_value_function(),
            b"RELABELTYPE" => self.read_relabel_type(),
            b"COERCEVIAIO" => self.read_coerce_via_io(),
            b"ARRAYCOERCEEXPR" => self.read_array_coerce_expr(),
            b"CONVERTROWTYPEEXPR" => self.read_convert_rowtype_expr(),
            b"COERCETODOMAIN" => self.read_coerce_to_domain(),
            b"COERCETODOMAINVALUE" => self.read_coerce_to_domain_value(),
            b"PARTITIONBOUNDSPEC" => self.read_partition_bound_spec(),
            b"PARTITIONRANGEDATUM" => self.read_partition_range_datum(),
            b"NULLTEST" => self.read_null_test(),
            b"SORTGROUPCLAUSE" => self.read_sort_group_clause(),
            b"GROUPINGSET" => self.read_grouping_set(),
            b"TABLESAMPLECLAUSE" => self.read_table_sample_clause(),
            b"ROWMARKCLAUSE" => self.read_row_mark_clause(),
            b"WITHCHECKOPTION" => self.read_with_check_option(),
            b"SETOPERATIONSTMT" => self.read_set_operation_stmt(),
            b"AGGREF" => self.read_aggref(),
            b"GROUPINGFUNC" => self.read_grouping_func(),
            b"CASEEXPR" => self.read_case_expr(),
            b"CASEWHEN" => self.read_case_when(),
            b"CASETESTEXPR" => self.read_case_test_expr(),
            b"COALESCEEXPR" => self.read_coalesce_expr(),
            b"CURRENTOFEXPR" => self.read_current_of_expr(),
            b"MINMAXEXPR" => self.read_min_max_expr(),
            b"SCALARARRAYOPEXPR" => self.read_scalar_array_op_expr(),
            b"SUBLINK" => self.read_sub_link(),
            b"SUBPLAN" => self.read_sub_plan(),
            b"ALTERNATIVESUBPLAN" => self.read_alternative_sub_plan(),
            b"PARAM" => self.read_param(),
            b"ARRAYEXPR" => self.read_array_expr(),
            b"SETTODEFAULT" => self.read_set_to_default(),
            b"BOOLEANTEST" => self.read_boolean_test(),
            b"DISTINCTEXPR" => self.read_distinct_expr(),
            b"NULLIFEXPR" => self.read_null_if_expr(),
            b"ONCONFLICTEXPR" => self.read_on_conflict_expr(),
            b"INFERENCEELEM" => self.read_inference_elem(),
            b"SUBSCRIPTINGREF" => self.read_subscripting_ref(),
            b"WINDOWFUNC" => self.read_window_func(),
            b"MERGESUPPORTFUNC" => self.read_merge_support_func(),
            b"WINDOWCLAUSE" => self.read_window_clause(),
            b"COMMONTABLEEXPR" => self.read_common_table_expr(),
            b"CTESEARCHCLAUSE" => self.read_cte_search_clause(),
            b"CTECYCLECLAUSE" => self.read_cte_cycle_clause(),
            b"COLLATEEXPR" => self.read_collate_expr(),
            b"JSONFORMAT" => self.read_json_format(),
            b"JSONRETURNING" => self.read_json_returning(),
            b"JSONVALUEEXPR" => self.read_json_value_expr(),
            b"JSONCONSTRUCTOREXPR" => self.read_json_constructor_expr(),
            b"JSONISPREDICATE" => self.read_json_is_predicate(),
            b"JSONBEHAVIOR" => self.read_json_behavior(),
            b"JSONEXPR" => self.read_json_expr(),
            b"NAMEDARGEXPR" => self.read_named_arg_expr(),
            b"RETURNINGEXPR" => self.read_returning_expr(),
            b"FIELDSELECT" => self.read_field_select(),
            b"FIELDSTORE" => self.read_field_store(),
            b"ROWEXPR" => self.read_row_expr(),
            b"ROWCOMPAREEXPR" => self.read_row_compare_expr(),
            b"MERGEACTION" => self.read_merge_action(),
            b"XMLEXPR" => self.read_xml_expr(),
            b"TABLEFUNC" => self.read_table_func(),
            b"JSONTABLEPATH" => self.read_json_table_path(),
            b"JSONTABLEPATHSCAN" => self.read_json_table_path_scan(),
            b"JSONTABLESIBLINGJOIN" => self.read_json_table_sibling_join(),
            b"NOTIFYSTMT" => self.read_notify_stmt(),
            b"NEXTVALUEEXPR" => self.read_next_value_expr(),
            other => panic!(
                "parseNodeString (readfuncs.c): {} read arm unported (view SELECT-rule + \
                 DEFAULT/CHECK expr sets only)",
                String::from_utf8_lossy(other)
            ),
        }
    }

    fn read_json_format(&mut self) -> PgResult<Node<'mcx>> {
        let f = types_nodes::JsonFormat {
            format_type: json_format_type(self.read_u32("format_type")),
            encoding: json_encoding(self.read_u32("encoding")),
            location: self.read_location("location"),
        };
        Node::mk(self.mcx, f)
    }

    fn json_format_ref(&mut self, name: &str) -> PgResult<Option<&'mcx types_nodes::JsonFormat>> {
        Ok(self.read_node(name)?.map(|n| n.as_json_format().expect("JsonFormat")))
    }

    fn read_json_returning(&mut self) -> PgResult<Node<'mcx>> {
        let format = self.json_format_ref("format")?;
        let r = types_nodes::JsonReturning {
            format,
            typid: self.read_u32("typid"),
            typmod: self.read_i32("typmod"),
        };
        Node::mk(self.mcx, r)
    }

    fn json_returning_ref(
        &mut self,
        name: &str,
    ) -> PgResult<Option<&'mcx types_nodes::JsonReturning<'mcx>>> {
        Ok(self.read_node(name)?.map(|n| n.as_json_returning().expect("JsonReturning")))
    }

    fn read_json_value_expr(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut j = Node::build::<types_nodes::JsonValueExpr>(mcx)?;
        j.raw_expr = self.read_node("raw_expr")?;
        j.formatted_expr = self.read_node("formatted_expr")?;
        j.format = self.json_format_ref("format")?;
        Ok(j.seal())
    }

    fn read_json_constructor_expr(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut c = Node::build::<types_nodes::JsonConstructorExpr>(mcx)?;
        c.r#type = json_ctor_type(self.read_u32("type"));
        c.args = self.read_node_list("args")?;
        c.func = self.read_node("func")?;
        c.coercion = self.read_node("coercion")?;
        c.returning = self.json_returning_ref("returning")?;
        c.absent_on_null = self.read_bool("absent_on_null");
        c.unique = self.read_bool("unique");
        c.location = self.read_location("location");
        Ok(c.seal())
    }

    fn read_json_is_predicate(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut p = Node::build::<types_nodes::JsonIsPredicate>(mcx)?;
        p.expr = self.read_node("expr")?;
        p.format = self.json_format_ref("format")?;
        p.item_type = json_value_type(self.read_u32("item_type"));
        p.unique_keys = self.read_bool("unique_keys");
        p.location = self.read_location("location");
        Ok(p.seal())
    }

    fn read_json_behavior(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut b = Node::build::<types_nodes::JsonBehavior>(mcx)?;
        b.btype = json_behavior_type(self.read_u32("btype"));
        b.expr = self.read_node("expr")?;
        b.coerce = self.read_bool("coerce");
        b.location = self.read_location("location");
        Ok(b.seal())
    }

    fn read_json_expr(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut j = Node::build::<types_nodes::JsonExpr>(mcx)?;
        j.op = json_expr_op(self.read_u32("op"));
        j.column_name = self.read_str("column_name")?;
        j.formatted_expr = self.read_node("formatted_expr")?;
        j.format = self.json_format_ref("format")?;
        j.path_spec = self.read_node("path_spec")?;
        j.returning = self.json_returning_ref("returning")?;
        j.passing_names = self.read_node_list("passing_names")?;
        j.passing_values = self.read_node_list("passing_values")?;
        j.on_empty = self.read_node("on_empty")?;
        j.on_error = self.read_node("on_error")?;
        j.use_io_coercion = self.read_bool("use_io_coercion");
        j.use_json_coercion = self.read_bool("use_json_coercion");
        j.wrapper = json_wrapper(self.read_u32("wrapper"));
        j.omit_quotes = self.read_bool("omit_quotes");
        j.collation = self.read_u32("collation");
        j.location = self.read_location("location");
        Ok(j.seal())
    }

    fn read_distinct_expr(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut d = Node::build::<types_nodes::primnodes::DistinctExpr>(mcx)?;
        d.opno = self.read_u32("opno");
        d.opfuncid = self.read_u32("opfuncid");
        d.opresulttype = self.read_u32("opresulttype");
        d.opretset = self.read_bool("opretset");
        d.opcollid = self.read_u32("opcollid");
        d.inputcollid = self.read_u32("inputcollid");
        d.args = self.read_node_list("args")?;
        d.location = self.read_location("location");
        Ok(d.seal())
    }

    fn read_null_if_expr(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut d = Node::build::<types_nodes::primnodes::NullIfExpr>(mcx)?;
        d.opno = self.read_u32("opno");
        d.opfuncid = self.read_u32("opfuncid");
        d.opresulttype = self.read_u32("opresulttype");
        d.opretset = self.read_bool("opretset");
        d.opcollid = self.read_u32("opcollid");
        d.inputcollid = self.read_u32("inputcollid");
        d.args = self.read_node_list("args")?;
        d.location = self.read_location("location");
        Ok(d.seal())
    }

    fn read_on_conflict_expr(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut c = Node::build::<types_nodes::primnodes::OnConflictExpr>(mcx)?;
        c.action = on_conflict_action(self.read_u32("action"));
        c.arbiterElems = self.read_node_list("arbiterElems")?;
        c.arbiterWhere = self.read_node("arbiterWhere")?;
        c.constraint = self.read_u32("constraint");
        c.onConflictSet = self.read_node_list("onConflictSet")?;
        c.onConflictWhere = self.read_node("onConflictWhere")?;
        c.exclRelIndex = self.read_i32("exclRelIndex");
        c.exclRelTlist = self.read_node_list("exclRelTlist")?;
        Ok(c.seal())
    }

    fn read_inference_elem(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut ie = Node::build::<types_nodes::primnodes::InferenceElem>(mcx)?;
        ie.expr = self.read_node("expr")?;
        ie.infercollid = self.read_u32("infercollid");
        ie.inferopclass = self.read_u32("inferopclass");
        Ok(ie.seal())
    }

    fn read_subscripting_ref(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut sr = Node::build::<types_nodes::primnodes::SubscriptingRef>(mcx)?;
        sr.refcontainertype = self.read_u32("refcontainertype");
        sr.refelemtype = self.read_u32("refelemtype");
        sr.refrestype = self.read_u32("refrestype");
        sr.reftypmod = self.read_i32("reftypmod");
        sr.refcollid = self.read_u32("refcollid");
        sr.refupperindexpr = self.read_opt_node_list("refupperindexpr")?;
        sr.reflowerindexpr = self.read_opt_node_list("reflowerindexpr")?;
        sr.refexpr = self.read_node("refexpr")?;
        sr.refassgnexpr = self.read_node("refassgnexpr")?;
        Ok(sr.seal())
    }

    fn read_boolean_test(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut bt = Node::build::<types_nodes::primnodes::BooleanTest>(mcx)?;
        bt.arg = self.read_node("arg")?;
        bt.booltesttype = bool_test_type(self.read_u32("booltesttype"));
        bt.location = self.read_location("location");
        Ok(bt.seal())
    }

    fn read_merge_support_func(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut m = Node::build::<types_nodes::primnodes::MergeSupportFunc>(mcx)?;
        m.msftype = self.read_u32("msftype");
        m.msfcollid = self.read_u32("msfcollid");
        m.location = self.read_location("location");
        Ok(m.seal())
    }

    fn read_window_func(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut w = Node::build::<WindowFunc>(mcx)?;
        w.winfnoid = self.read_u32("winfnoid");
        w.wintype = self.read_u32("wintype");
        w.wincollid = self.read_u32("wincollid");
        w.inputcollid = self.read_u32("inputcollid");
        w.args = self.read_node_list("args")?;
        w.aggfilter = self.read_node("aggfilter")?;
        w.runCondition = self.read_node_list("runCondition")?;
        w.winref = self.read_u32("winref");
        w.winstar = self.read_bool("winstar");
        w.winagg = self.read_bool("winagg");
        w.location = self.read_location("location");
        Ok(w.seal())
    }

    fn read_window_clause(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut w = Node::build::<WindowClause>(mcx)?;
        w.name = self.read_str("name")?;
        w.refname = self.read_str("refname")?;
        w.partitionClause = self.read_node_list("partitionClause")?;
        w.orderClause = self.read_node_list("orderClause")?;
        w.frameOptions = self.read_i32("frameOptions");
        w.startOffset = self.read_node("startOffset")?;
        w.endOffset = self.read_node("endOffset")?;
        w.startInRangeFunc = self.read_u32("startInRangeFunc");
        w.endInRangeFunc = self.read_u32("endInRangeFunc");
        w.inRangeColl = self.read_u32("inRangeColl");
        w.inRangeAsc = self.read_bool("inRangeAsc");
        w.inRangeNullsFirst = self.read_bool("inRangeNullsFirst");
        w.winref = self.read_u32("winref");
        w.copiedOrder = self.read_bool("copiedOrder");
        Ok(w.seal())
    }

    fn read_common_table_expr(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut c = Node::build::<CommonTableExpr>(mcx)?;
        c.ctename = self.read_str("ctename")?;
        c.aliascolnames = self.read_node_list("aliascolnames")?;
        c.ctematerialized = cte_materialize(self.read_u32("ctematerialized"));
        c.ctequery = self.read_node("ctequery")?;
        c.search_clause = self.read_node("search_clause")?;
        c.cycle_clause = self.read_node("cycle_clause")?;
        c.location = self.read_location("location");
        c.cterecursive = self.read_bool("cterecursive");
        c.cterefcount = self.read_i32("cterefcount");
        c.ctecolnames = self.read_node_list("ctecolnames")?;
        c.ctecoltypes = self.read_oid_list("ctecoltypes")?;
        c.ctecoltypmods = self.read_int_list("ctecoltypmods")?;
        c.ctecolcollations = self.read_oid_list("ctecolcollations")?;
        Ok(c.seal())
    }

    fn read_cte_search_clause(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut s = Node::build::<CTESearchClause>(mcx)?;
        s.search_col_list = self.read_node_list("search_col_list")?;
        s.search_breadth_first = self.read_bool("search_breadth_first");
        s.search_seq_column = self.read_str("search_seq_column")?;
        s.location = self.read_location("location");
        Ok(s.seal())
    }

    fn read_cte_cycle_clause(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut c = Node::build::<CTECycleClause>(mcx)?;
        c.cycle_col_list = self.read_node_list("cycle_col_list")?;
        c.cycle_mark_column = self.read_str("cycle_mark_column")?;
        c.cycle_mark_value = self.read_node("cycle_mark_value")?;
        c.cycle_mark_default = self.read_node("cycle_mark_default")?;
        c.cycle_path_column = self.read_str("cycle_path_column")?;
        c.location = self.read_location("location");
        c.cycle_mark_type = self.read_u32("cycle_mark_type");
        c.cycle_mark_typmod = self.read_i32("cycle_mark_typmod");
        c.cycle_mark_collation = self.read_u32("cycle_mark_collation");
        c.cycle_mark_neop = self.read_u32("cycle_mark_neop");
        Ok(c.seal())
    }

    fn read_notify_stmt(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut n = Node::build::<NotifyStmt>(mcx)?;
        n.conditionname = self.read_str("conditionname")?;
        n.payload = self.read_str("payload")?;
        Ok(n.seal())
    }

    fn read_next_value_expr(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut n = Node::build::<NextValueExpr>(mcx)?;
        n.seqid = self.read_u32("seqid");
        n.typeId = self.read_u32("typeId");
        Ok(n.seal())
    }

    fn read_named_arg_expr(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut n = Node::build::<NamedArgExpr>(mcx)?;
        n.arg = self.read_node("arg")?;
        n.name = self.read_str("name")?;
        n.argnumber = self.read_i32("argnumber");
        n.location = self.read_location("location");
        Ok(n.seal())
    }

    fn read_row_expr(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut r = Node::build::<types_nodes::primnodes::RowExpr>(mcx)?;
        r.args = self.read_node_list("args")?;
        r.row_typeid = self.read_u32("row_typeid");
        r.row_format = coercion_form(self.read_u32("row_format"));
        r.colnames = self.read_node_list("colnames")?;
        r.location = self.read_location("location");
        Ok(r.seal())
    }

    fn read_returning_expr(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let retlevelsup = self.read_i32("retlevelsup");
        let retold = self.read_bool("retold");
        let retexpr = self.read_node("retexpr")?.expect("ReturningExpr has a retexpr");
        Node::mk(mcx, types_nodes::primnodes::ReturningExpr { retlevelsup, retold, retexpr })
    }

    fn read_row_compare_expr(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut r = Node::build::<types_nodes::primnodes::RowCompareExpr>(mcx)?;
        r.cmptype = self.read_i32("cmptype");
        r.opnos = self.read_oid_list("opnos")?;
        r.opfamilies = self.read_oid_list("opfamilies")?;
        r.inputcollids = self.read_oid_list("inputcollids")?;
        r.largs = self.read_node_list("largs")?;
        r.rargs = self.read_node_list("rargs")?;
        Ok(r.seal())
    }

    fn read_field_select(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let arg = self.read_node("arg")?.expect("FieldSelect has an arg");
        let fieldnum = self.read_i32("fieldnum") as i16;
        let resulttype = self.read_u32("resulttype");
        let resulttypmod = self.read_i32("resulttypmod");
        let resultcollid = self.read_u32("resultcollid");
        Node::mk(mcx, FieldSelect { arg, fieldnum, resulttype, resulttypmod, resultcollid })
    }

    fn read_field_store(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let arg = self.read_node("arg")?.expect("FieldStore has an arg");
        let newvals = self.read_node_list("newvals")?;
        let fieldnums = self.read_int_list("fieldnums")?;
        let resulttype = self.read_u32("resulttype");
        Node::mk(mcx, FieldStore { arg, newvals, fieldnums, resulttype })
    }

    fn read_merge_action(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut m = Node::build::<MergeAction>(mcx)?;
        m.matchKind = merge_match_kind(self.read_u32("matchKind"));
        m.commandType = cmd_type(self.read_u32("commandType"));
        m.r#override = overriding_kind(self.read_u32("override"));
        m.qual = self.read_node("qual")?;
        m.targetList = self.read_node_list("targetList")?;
        m.updateColnos = self.read_int_list("updateColnos")?;
        Ok(m.seal())
    }

    fn read_collate_expr(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let arg = self.read_node("arg")?.expect("CollateExpr has an arg");
        let collOid = self.read_u32("collOid");
        let location = self.read_location("location");
        Node::mk(mcx, CollateExpr { arg, collOid, location })
    }

    fn read_set_to_default(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut d = Node::build::<types_nodes::primnodes::SetToDefault>(mcx)?;
        d.typeId = self.read_u32("typeId");
        d.typeMod = self.read_i32("typeMod");
        d.collation = self.read_u32("collation");
        d.location = self.read_location("location");
        Ok(d.seal())
    }

    // _readQuery (readfuncs.funcs.c); queryId is read_write_ignore/read_as(0).
    fn read_query(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut q = Node::build::<Query>(mcx)?;
        q.commandType = cmd_type(self.read_u32("commandType"));
        q.querySource = query_source(self.read_u32("querySource"));
        q.queryId = 0;
        q.canSetTag = self.read_bool("canSetTag");
        q.utilityStmt = self.read_node("utilityStmt")?;
        q.resultRelation = self.read_i32("resultRelation");
        q.hasAggs = self.read_bool("hasAggs");
        q.hasWindowFuncs = self.read_bool("hasWindowFuncs");
        q.hasTargetSRFs = self.read_bool("hasTargetSRFs");
        q.hasSubLinks = self.read_bool("hasSubLinks");
        q.hasDistinctOn = self.read_bool("hasDistinctOn");
        q.hasRecursive = self.read_bool("hasRecursive");
        q.hasModifyingCTE = self.read_bool("hasModifyingCTE");
        q.hasForUpdate = self.read_bool("hasForUpdate");
        q.hasRowSecurity = self.read_bool("hasRowSecurity");
        q.hasGroupRTE = self.read_bool("hasGroupRTE");
        q.isReturn = self.read_bool("isReturn");
        q.cteList = self.read_node_list("cteList")?;
        q.rtable = self.read_node_list("rtable")?;
        q.rteperminfos = self.read_node_list("rteperminfos")?;
        q.jointree = match self.read_node("jointree")? {
            None => None,
            Some(n) => Some(n.as_from_expr().expect("jointree is a FromExpr")),
        };
        q.mergeActionList = self.read_node_list("mergeActionList")?;
        q.mergeTargetRelation = self.read_i32("mergeTargetRelation");
        q.mergeJoinCondition = self.read_node("mergeJoinCondition")?;
        q.targetList = self.read_node_list("targetList")?;
        q.r#override = overriding_kind(self.read_u32("override"));
        q.onConflict = self.read_node("onConflict")?;
        q.returningOldAlias = self.read_str("returningOldAlias")?;
        q.returningNewAlias = self.read_str("returningNewAlias")?;
        q.returningList = self.read_node_list("returningList")?;
        q.groupClause = self.read_node_list("groupClause")?;
        q.groupDistinct = self.read_bool("groupDistinct");
        q.groupingSets = self.read_node_list("groupingSets")?;
        q.havingQual = self.read_node("havingQual")?;
        q.windowClause = self.read_node_list("windowClause")?;
        q.distinctClause = self.read_node_list("distinctClause")?;
        q.sortClause = self.read_node_list("sortClause")?;
        q.limitOffset = self.read_node("limitOffset")?;
        q.limitCount = self.read_node("limitCount")?;
        q.limitOption = limit_option(self.read_u32("limitOption"));
        q.rowMarks = self.read_node_list("rowMarks")?;
        q.setOperations = self.read_node("setOperations")?;
        q.constraintDeps = self.read_oid_list("constraintDeps")?;
        q.withCheckOptions = self.read_node_list("withCheckOptions")?;
        q.stmt_location = self.read_location("stmt_location");
        q.stmt_len = self.read_location("stmt_len");
        Ok(q.seal())
    }

    // _readRangeTblEntry (readfuncs.c, custom_read_write): common head,
    // per-rtekind middle, common tail. Only the arms a stored view SELECT
    // rule can contain are live.
    fn read_range_tbl_entry(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut rte = Node::build::<RangeTblEntry>(mcx)?;
        rte.alias = self.read_alias_ref("alias")?;
        rte.eref = self.read_alias_ref("eref")?;
        rte.rtekind = rte_kind(self.read_u32("rtekind"));
        match rte.rtekind {
            RTEKind::RTE_RELATION => {
                rte.relid = self.read_u32("relid");
                rte.inh = self.read_bool("inh");
                rte.relkind = self.read_char("relkind");
                rte.rellockmode = self.read_i32("rellockmode");
                rte.perminfoindex = self.read_u32("perminfoindex");
                rte.tablesample = self.read_node("tablesample")?;
            }
            RTEKind::RTE_SUBQUERY => {
                rte.subquery = match self.read_node("subquery")? {
                    None => None,
                    Some(n) => Some(n.as_query().expect("subquery is a Query")),
                };
                rte.security_barrier = self.read_bool("security_barrier");
                rte.relid = self.read_u32("relid");
                rte.inh = self.read_bool("inh");
                rte.relkind = self.read_char("relkind");
                rte.rellockmode = self.read_i32("rellockmode");
                rte.perminfoindex = self.read_u32("perminfoindex");
            }
            RTEKind::RTE_GROUP => {
                rte.groupexprs = self.read_node_list("groupexprs")?;
            }
            RTEKind::RTE_JOIN => {
                rte.jointype = join_type(self.read_u32("jointype"));
                rte.joinmergedcols = self.read_i32("joinmergedcols");
                rte.joinaliasvars = self.read_node_list("joinaliasvars")?;
                rte.joinleftcols = self.read_int_list("joinleftcols")?;
                rte.joinrightcols = self.read_int_list("joinrightcols")?;
                rte.join_using_alias = self.read_alias_ref("join_using_alias")?;
            }
            RTEKind::RTE_FUNCTION => {
                rte.functions = self.read_node_list("functions")?;
                rte.funcordinality = self.read_bool("funcordinality");
            }
            RTEKind::RTE_TABLEFUNC => {
                let tfnode = self.read_node("tablefunc")?;
                rte.tablefunc = tfnode;
                // C: the RTE must carry a copy of the column type info.
                if let Some(tf) = tfnode.and_then(|n| n.as_table_func()) {
                    rte.coltypes = tf.coltypes.clone_in(mcx)?;
                    rte.coltypmods = tf.coltypmods.clone_in(mcx)?;
                    rte.colcollations = tf.colcollations.clone_in(mcx)?;
                }
            }
            RTEKind::RTE_VALUES => {
                rte.values_lists = self.read_node_list("values_lists")?;
                rte.coltypes = self.read_oid_list("coltypes")?;
                rte.coltypmods = self.read_int_list("coltypmods")?;
                rte.colcollations = self.read_oid_list("colcollations")?;
            }
            RTEKind::RTE_CTE => {
                rte.ctename = self.read_str("ctename")?;
                rte.ctelevelsup = self.read_u32("ctelevelsup");
                rte.self_reference = self.read_bool("self_reference");
                rte.coltypes = self.read_oid_list("coltypes")?;
                rte.coltypmods = self.read_int_list("coltypmods")?;
                rte.colcollations = self.read_oid_list("colcollations")?;
            }
            RTEKind::RTE_NAMEDTUPLESTORE => {
                rte.enrname = self.read_str("enrname")?;
                rte.enrtuples = self.read_f64("enrtuples");
                rte.coltypes = self.read_oid_list("coltypes")?;
                rte.coltypmods = self.read_int_list("coltypmods")?;
                rte.colcollations = self.read_oid_list("colcollations")?;
                rte.relid = self.read_u32("relid");
            }
            other => panic!(
                "_readRangeTblEntry (readfuncs.c): {other:?} arm unported (view SELECT-rule set)"
            ),
        }
        rte.lateral = self.read_bool("lateral");
        rte.inFromCl = self.read_bool("inFromCl");
        rte.securityQuals = self.read_node_list("securityQuals")?;
        Ok(rte.seal())
    }

    fn read_alias_ref(&mut self, name: &str) -> PgResult<Option<&'mcx Alias<'mcx>>> {
        match self.read_node(name)? {
            None => Ok(None),
            Some(n) => Ok(Some(n.as_alias().expect("Alias field"))),
        }
    }

    fn read_rte_permission_info(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut p = Node::build::<RTEPermissionInfo>(mcx)?;
        p.relid = self.read_u32("relid");
        p.inh = self.read_bool("inh");
        p.requiredPerms = self.read_u64("requiredPerms");
        p.checkAsUser = self.read_u32("checkAsUser");
        p.selectedCols = self.read_bitmapset("selectedCols")?;
        p.insertedCols = self.read_bitmapset("insertedCols")?;
        p.updatedCols = self.read_bitmapset("updatedCols")?;
        Ok(p.seal())
    }

    fn read_alias(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut a = Node::build::<Alias>(mcx)?;
        a.aliasname = self.read_str("aliasname")?;
        a.colnames = self.read_node_list("colnames")?;
        Ok(a.seal())
    }

    fn read_from_expr(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut f = Node::build::<FromExpr>(mcx)?;
        f.fromlist = self.read_node_list("fromlist")?;
        f.quals = self.read_node("quals")?;
        Ok(f.seal())
    }

    fn read_join_expr(&mut self) -> PgResult<Node<'mcx>> {
        let jointype = join_type(self.read_u32("jointype"));
        let isNatural = self.read_bool("isNatural");
        let larg = self.read_node("larg")?.expect("JoinExpr has a larg");
        let rarg = self.read_node("rarg")?.expect("JoinExpr has a rarg");
        let usingClause = self.read_node_list("usingClause")?;
        let join_using_alias = self.read_alias_ref("join_using_alias")?;
        let quals = self.read_node("quals")?;
        let alias = self.read_alias_ref("alias")?;
        let rtindex = self.read_i32("rtindex");
        Node::mk(
            self.mcx,
            JoinExpr {
                jointype,
                isNatural,
                larg,
                rarg,
                usingClause,
                join_using_alias,
                quals,
                alias,
                rtindex,
            },
        )
    }

    fn read_range_tbl_function(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut f = Node::build::<RangeTblFunction>(mcx)?;
        f.funcexpr = self.read_node("funcexpr")?;
        f.funccolcount = self.read_i32("funccolcount");
        f.funccolnames = self.read_node_list("funccolnames")?;
        f.funccoltypes = self.read_oid_list("funccoltypes")?;
        f.funccoltypmods = self.read_int_list("funccoltypmods")?;
        f.funccolcollations = self.read_oid_list("funccolcollations")?;
        f.funcparams = self.read_bitmapset("funcparams")?;
        Ok(f.seal())
    }

    fn read_range_tbl_ref(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut r = Node::build::<RangeTblRef>(mcx)?;
        r.rtindex = self.read_i32("rtindex");
        Ok(r.seal())
    }

    fn read_target_entry(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let expr = self.read_node("expr")?.expect("TargetEntry has an expr");
        let te = TargetEntry {
            expr,
            resno: self.read_i32("resno") as i16,
            resname: self.read_str("resname")?,
            ressortgroupref: self.read_u32("ressortgroupref"),
            resorigtbl: self.read_u32("resorigtbl"),
            resorigcol: self.read_i32("resorigcol") as i16,
            resjunk: self.read_bool("resjunk"),
        };
        Node::mk(mcx, te)
    }

    fn read_var(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut v = Node::build::<Var>(mcx)?;
        v.varno = self.read_i32("varno");
        v.varattno = self.read_i32("varattno") as i16;
        v.vartype = self.read_u32("vartype");
        v.vartypmod = self.read_i32("vartypmod");
        v.varcollid = self.read_u32("varcollid");
        v.varnullingrels = self.read_bitmapset("varnullingrels")?;
        v.varlevelsup = self.read_u32("varlevelsup");
        v.varreturningtype = var_returning_type(self.read_u32("varreturningtype"));
        v.varnosyn = self.read_u32("varnosyn");
        v.varattnosyn = self.read_i32("varattnosyn") as i16;
        v.location = self.read_location("location");
        Ok(v.seal())
    }

    // _readConst (readfuncs.c, handwritten): trailing constvalue via readDatum.
    fn read_const(&mut self) -> PgResult<Node<'mcx>> {
        let consttype = self.read_u32("consttype");
        let consttypmod = self.read_i32("consttypmod");
        let constcollid = self.read_u32("constcollid");
        let constlen = self.read_i32("constlen");
        let constbyval = self.read_bool("constbyval");
        let constisnull = self.read_bool("constisnull");
        let location = self.read_location("location");
        self.label("constvalue");
        let constvalue = if constisnull {
            let t = self.token("constvalue");
            assert!(t.is_empty(), "_readConst: null Const with a value");
            Datum::from_usize(0)
        } else {
            self.read_datum(constbyval)?
        };
        Node::mk(
            self.mcx,
            Const {
                consttype,
                consttypmod,
                constcollid,
                constlen,
                constvalue,
                constisnull,
                constbyval,
                location,
            },
        )
    }

    fn read_partition_bound_spec(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut b = Node::build::<types_nodes::rawnodes::PartitionBoundSpec>(mcx)?;
        b.strategy = self.read_char("strategy");
        b.is_default = self.read_bool("is_default");
        b.modulus = self.read_i32("modulus");
        b.remainder = self.read_i32("remainder");
        b.listdatums = self.read_node_list("listdatums")?;
        b.lowerdatums = self.read_node_list("lowerdatums")?;
        b.upperdatums = self.read_node_list("upperdatums")?;
        b.location = self.read_location("location");
        Ok(b.seal())
    }

    fn read_partition_range_datum(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut d = Node::build::<types_nodes::rawnodes::PartitionRangeDatum>(mcx)?;
        d.kind = match self.read_i32("kind") {
            -1 => types_nodes::rawnodes::PartitionRangeDatumKind::Minvalue,
            0 => types_nodes::rawnodes::PartitionRangeDatumKind::Value,
            1 => types_nodes::rawnodes::PartitionRangeDatumKind::Maxvalue,
            k => panic!("_readPartitionRangeDatum: bad kind {k}"),
        };
        d.value = self.read_node("value")?;
        d.location = self.read_location("location");
        Ok(d.seal())
    }

    fn read_op_expr(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut o = Node::build::<OpExpr>(mcx)?;
        o.opno = self.read_u32("opno");
        o.opfuncid = self.read_u32("opfuncid");
        o.opresulttype = self.read_u32("opresulttype");
        o.opretset = self.read_bool("opretset");
        o.opcollid = self.read_u32("opcollid");
        o.inputcollid = self.read_u32("inputcollid");
        o.args = self.read_node_list("args")?;
        o.location = self.read_location("location");
        Ok(o.seal())
    }

    fn read_func_expr(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut f = Node::build::<FuncExpr>(mcx)?;
        f.funcid = self.read_u32("funcid");
        f.funcresulttype = self.read_u32("funcresulttype");
        f.funcretset = self.read_bool("funcretset");
        f.funcvariadic = self.read_bool("funcvariadic");
        f.funcformat = coercion_form(self.read_u32("funcformat"));
        f.funccollid = self.read_u32("funccollid");
        f.inputcollid = self.read_u32("inputcollid");
        f.args = self.read_node_list("args")?;
        f.location = self.read_location("location");
        Ok(f.seal())
    }

    // _readBoolExpr (readfuncs.c, handwritten): boolop stored as a word.
    fn read_bool_expr(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut b = Node::build::<BoolExpr>(mcx)?;
        self.label("boolop");
        b.boolop = match self.token("boolop") {
            b"and" => BoolExprType::AND_EXPR,
            b"or" => BoolExprType::OR_EXPR,
            b"not" => BoolExprType::NOT_EXPR,
            other => panic!(
                "_readBoolExpr (readfuncs.c): unrecognized boolop \"{}\"",
                String::from_utf8_lossy(other)
            ),
        };
        b.args = self.read_node_list("args")?;
        b.location = self.read_location("location");
        Ok(b.seal())
    }

    fn read_sql_value_function(&mut self) -> PgResult<Node<'mcx>> {
        use types_nodes::primnodes::{SQLValueFunction, SQLValueFunctionOp as Op};
        let op = match self.read_u32("op") {
            0 => Op::SVFOP_CURRENT_DATE,
            1 => Op::SVFOP_CURRENT_TIME,
            2 => Op::SVFOP_CURRENT_TIME_N,
            3 => Op::SVFOP_CURRENT_TIMESTAMP,
            4 => Op::SVFOP_CURRENT_TIMESTAMP_N,
            5 => Op::SVFOP_LOCALTIME,
            6 => Op::SVFOP_LOCALTIME_N,
            7 => Op::SVFOP_LOCALTIMESTAMP,
            8 => Op::SVFOP_LOCALTIMESTAMP_N,
            9 => Op::SVFOP_CURRENT_ROLE,
            10 => Op::SVFOP_CURRENT_USER,
            11 => Op::SVFOP_USER,
            12 => Op::SVFOP_SESSION_USER,
            13 => Op::SVFOP_CURRENT_CATALOG,
            14 => Op::SVFOP_CURRENT_SCHEMA,
            other => panic!("_readSQLValueFunction (readfuncs.c): bad op {other}"),
        };
        let svf = SQLValueFunction {
            op,
            r#type: self.read_u32("type"),
            typmod: self.read_i32("typmod"),
            location: self.read_location("location"),
        };
        Node::mk(self.mcx, svf)
    }

    fn read_coerce_via_io(&mut self) -> PgResult<Node<'mcx>> {
        let arg = self.read_node("arg")?.expect("CoerceViaIO has an arg");
        let c = types_nodes::primnodes::CoerceViaIO {
            arg,
            resulttype: self.read_u32("resulttype"),
            resultcollid: self.read_u32("resultcollid"),
            coerceformat: coercion_form(self.read_u32("coerceformat")),
            location: self.read_location("location"),
        };
        Node::mk(self.mcx, c)
    }

    fn read_array_coerce_expr(&mut self) -> PgResult<Node<'mcx>> {
        let arg = self.read_node("arg")?.expect("ArrayCoerceExpr has an arg");
        let elemexpr = self.read_node("elemexpr")?;
        let a = types_nodes::ArrayCoerceExpr {
            arg,
            elemexpr,
            resulttype: self.read_u32("resulttype"),
            resulttypmod: self.read_i32("resulttypmod"),
            resultcollid: self.read_u32("resultcollid"),
            coerceformat: coercion_form(self.read_u32("coerceformat")),
            location: self.read_location("location"),
        };
        Node::mk(self.mcx, a)
    }

    fn read_convert_rowtype_expr(&mut self) -> PgResult<Node<'mcx>> {
        let arg = self.read_node("arg")?.expect("ConvertRowtypeExpr has an arg");
        let c = types_nodes::ConvertRowtypeExpr {
            arg,
            resulttype: self.read_u32("resulttype"),
            convertformat: coercion_form(self.read_u32("convertformat")),
            location: self.read_location("location"),
        };
        Node::mk(self.mcx, c)
    }

    fn read_place_holder_var(&mut self) -> PgResult<Node<'mcx>> {
        let phexpr = self.read_node("phexpr")?.expect("PlaceHolderVar has a phexpr");
        let phv = types_nodes::primnodes::PlaceHolderVar {
            phexpr,
            phrels: self.read_bitmapset("phrels")?,
            phnullingrels: self.read_bitmapset("phnullingrels")?,
            phid: self.read_u32("phid"),
            phlevelsup: self.read_u32("phlevelsup"),
        };
        Node::mk(self.mcx, phv)
    }

    fn read_relabel_type(&mut self) -> PgResult<Node<'mcx>> {
        let arg = self.read_node("arg")?.expect("RelabelType has an arg");
        let r = RelabelType {
            arg,
            resulttype: self.read_u32("resulttype"),
            resulttypmod: self.read_i32("resulttypmod"),
            resultcollid: self.read_u32("resultcollid"),
            relabelformat: coercion_form(self.read_u32("relabelformat")),
            location: self.read_location("location"),
        };
        Node::mk(self.mcx, r)
    }

    fn read_coerce_to_domain(&mut self) -> PgResult<Node<'mcx>> {
        let arg = self.read_node("arg")?.expect("CoerceToDomain has an arg");
        let c = types_nodes::CoerceToDomain {
            arg,
            resulttype: self.read_u32("resulttype"),
            resulttypmod: self.read_i32("resulttypmod"),
            resultcollid: self.read_u32("resultcollid"),
            coercionformat: coercion_form(self.read_u32("coercionformat")),
            location: self.read_location("location"),
        };
        Node::mk(self.mcx, c)
    }

    fn read_coerce_to_domain_value(&mut self) -> PgResult<Node<'mcx>> {
        let c = types_nodes::CoerceToDomainValue {
            typeId: self.read_u32("typeId"),
            typeMod: self.read_i32("typeMod"),
            collation: self.read_u32("collation"),
            location: self.read_location("location"),
        };
        Node::mk(self.mcx, c)
    }

    fn read_null_test(&mut self) -> PgResult<Node<'mcx>> {
        let arg = self.read_node("arg")?;
        let n = NullTest {
            arg,
            nulltesttype: match self.read_u32("nulltesttype") {
                0 => NullTestType::IS_NULL,
                1 => NullTestType::IS_NOT_NULL,
                other => panic!("readfuncs.c: bad NullTestType {other}"),
            },
            argisrow: self.read_bool("argisrow"),
            location: self.read_location("location"),
        };
        Node::mk(self.mcx, n)
    }

    fn read_table_sample_clause(&mut self) -> PgResult<Node<'mcx>> {
        let t = types_nodes::parsenodes::TableSampleClause {
            tsmhandler: self.read_u32("tsmhandler"),
            args: self.read_node_list("args")?,
            repeatable: self.read_node("repeatable")?,
        };
        Node::mk(self.mcx, t)
    }

    fn read_grouping_set(&mut self) -> PgResult<Node<'mcx>> {
        let kind = match self.read_u32("kind") {
            0 => types_nodes::parsenodes::GroupingSetKind::GROUPING_SET_EMPTY,
            1 => types_nodes::parsenodes::GroupingSetKind::GROUPING_SET_SIMPLE,
            2 => types_nodes::parsenodes::GroupingSetKind::GROUPING_SET_ROLLUP,
            3 => types_nodes::parsenodes::GroupingSetKind::GROUPING_SET_CUBE,
            4 => types_nodes::parsenodes::GroupingSetKind::GROUPING_SET_SETS,
            other => panic!("unrecognized GroupingSetKind: {other}"),
        };
        // SIMPLE content is stored as C's int list; keep Integer nodes in
        // memory (parse-side shape).
        self.label("content");
        let t = self.token("content");
        let mut content = NodeList::nil();
        if !t.is_empty() {
            assert!(t == b"(", "readfuncs.c: GroupingSet content is not a list");
            let mut first = true;
            loop {
                let tok = self.token("list");
                if tok == b")" {
                    break;
                }
                if first && tok == b"i" {
                    first = false;
                    continue;
                }
                first = false;
                let elem = self
                    .node_read_token(tok)?
                    .expect("nodeRead: <> is not a valid list element here");
                content.lappend(self.mcx, elem)?;
            }
        }
        let location = self.read_location("location");
        let g = types_nodes::parsenodes::GroupingSet { kind, content, location };
        Node::mk(self.mcx, g)
    }

    fn read_sort_group_clause(&mut self) -> PgResult<Node<'mcx>> {
        let s = SortGroupClause {
            tleSortGroupRef: self.read_u32("tleSortGroupRef"),
            eqop: self.read_u32("eqop"),
            sortop: self.read_u32("sortop"),
            reverse_sort: self.read_bool("reverse_sort"),
            nulls_first: self.read_bool("nulls_first"),
            hashable: self.read_bool("hashable"),
        };
        Node::mk(self.mcx, s)
    }

    fn read_with_check_option(&mut self) -> PgResult<Node<'mcx>> {
        let w = types_nodes::parsenodes::WithCheckOption {
            kind: match self.read_u32("kind") {
                0 => types_nodes::parsenodes::WCOKind::WCO_VIEW_CHECK,
                1 => types_nodes::parsenodes::WCOKind::WCO_RLS_INSERT_CHECK,
                2 => types_nodes::parsenodes::WCOKind::WCO_RLS_UPDATE_CHECK,
                3 => types_nodes::parsenodes::WCOKind::WCO_RLS_CONFLICT_CHECK,
                4 => types_nodes::parsenodes::WCOKind::WCO_RLS_MERGE_UPDATE_CHECK,
                5 => types_nodes::parsenodes::WCOKind::WCO_RLS_MERGE_DELETE_CHECK,
                other => panic!("readfuncs.c: bad WCOKind {other}"),
            },
            relname: self.read_str("relname")?,
            polname: self.read_str("polname")?,
            qual: self.read_node("qual")?,
            cascaded: self.read_bool("cascaded"),
        };
        Node::mk(self.mcx, w)
    }

    fn read_row_mark_clause(&mut self) -> PgResult<Node<'mcx>> {
        let r = types_nodes::parsenodes::RowMarkClause {
            rti: self.read_u32("rti"),
            strength: match self.read_u32("strength") {
                0 => types_nodes::nodes_enums::LockClauseStrength::LCS_NONE,
                1 => types_nodes::nodes_enums::LockClauseStrength::LCS_FORKEYSHARE,
                2 => types_nodes::nodes_enums::LockClauseStrength::LCS_FORSHARE,
                3 => types_nodes::nodes_enums::LockClauseStrength::LCS_FORNOKEYUPDATE,
                4 => types_nodes::nodes_enums::LockClauseStrength::LCS_FORUPDATE,
                other => panic!("readfuncs.c: bad LockClauseStrength {other}"),
            },
            waitPolicy: match self.read_u32("waitPolicy") {
                0 => types_nodes::nodes_enums::LockWaitPolicy::LockWaitBlock,
                1 => types_nodes::nodes_enums::LockWaitPolicy::LockWaitSkip,
                2 => types_nodes::nodes_enums::LockWaitPolicy::LockWaitError,
                other => panic!("readfuncs.c: bad LockWaitPolicy {other}"),
            },
            pushedDown: self.read_bool("pushedDown"),
        };
        Node::mk(self.mcx, r)
    }

    fn read_set_operation_stmt(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut s = Node::build::<SetOperationStmt>(mcx)?;
        s.op = set_operation(self.read_u32("op"));
        s.all = self.read_bool("all");
        s.larg = self.read_node("larg")?;
        s.rarg = self.read_node("rarg")?;
        s.colTypes = self.read_oid_list("colTypes")?;
        s.colTypmods = self.read_int_list("colTypmods")?;
        s.colCollations = self.read_oid_list("colCollations")?;
        s.groupClauses = self.read_node_list("groupClauses")?;
        Ok(s.seal())
    }

    fn read_aggref(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut a = Node::build::<Aggref>(mcx)?;
        a.aggfnoid = self.read_u32("aggfnoid");
        a.aggtype = self.read_u32("aggtype");
        a.aggcollid = self.read_u32("aggcollid");
        a.inputcollid = self.read_u32("inputcollid");
        a.aggtranstype = self.read_u32("aggtranstype");
        a.aggargtypes = self.read_oid_list("aggargtypes")?;
        a.aggdirectargs = self.read_node_list("aggdirectargs")?;
        a.args = self.read_node_list("args")?;
        a.aggorder = self.read_node_list("aggorder")?;
        a.aggdistinct = self.read_node_list("aggdistinct")?;
        a.aggfilter = self.read_node("aggfilter")?;
        a.aggstar = self.read_bool("aggstar");
        a.aggvariadic = self.read_bool("aggvariadic");
        a.aggkind = self.read_char("aggkind") as i8;
        a.aggpresorted = self.read_bool("aggpresorted");
        a.agglevelsup = self.read_u32("agglevelsup");
        a.aggsplit = self.read_u32("aggsplit");
        a.aggno = self.read_i32("aggno");
        a.aggtransno = self.read_i32("aggtransno");
        a.location = self.read_location("location");
        Ok(a.seal())
    }

    fn read_grouping_func(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut g = Node::build::<types_nodes::primnodes::GroupingFunc>(mcx)?;
        g.args = self.read_node_list("args")?;
        g.refs = self.read_int_list("refs")?;
        g.cols = self.read_int_list("cols")?;
        g.agglevelsup = self.read_u32("agglevelsup");
        g.location = self.read_location("location");
        Ok(g.seal())
    }

    fn read_case_expr(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut c = Node::build::<CaseExpr>(mcx)?;
        c.casetype = self.read_u32("casetype");
        c.casecollid = self.read_u32("casecollid");
        c.arg = self.read_node("arg")?;
        c.args = self.read_node_list("args")?;
        c.defresult = self.read_node("defresult")?;
        c.location = self.read_location("location");
        Ok(c.seal())
    }

    fn read_case_when(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut w = Node::build::<CaseWhen>(mcx)?;
        w.expr = self.read_node("expr")?;
        w.result = self.read_node("result")?;
        w.location = self.read_location("location");
        Ok(w.seal())
    }

    fn read_case_test_expr(&mut self) -> PgResult<Node<'mcx>> {
        let c = CaseTestExpr {
            typeId: self.read_u32("typeId"),
            typeMod: self.read_i32("typeMod"),
            collation: self.read_u32("collation"),
        };
        Node::mk(self.mcx, c)
    }

    fn read_coalesce_expr(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut c = Node::build::<CoalesceExpr>(mcx)?;
        c.coalescetype = self.read_u32("coalescetype");
        c.coalescecollid = self.read_u32("coalescecollid");
        c.args = self.read_node_list("args")?;
        c.location = self.read_location("location");
        Ok(c.seal())
    }

    fn read_current_of_expr(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut c = Node::build::<types_nodes::primnodes::CurrentOfExpr>(mcx)?;
        c.cvarno = self.read_u32("cvarno");
        c.cursor_name = self.read_str("cursor_name")?;
        c.cursor_param = self.read_i32("cursor_param");
        Ok(c.seal())
    }

    fn read_min_max_expr(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut m = Node::build::<MinMaxExpr>(mcx)?;
        m.minmaxtype = self.read_u32("minmaxtype");
        m.minmaxcollid = self.read_u32("minmaxcollid");
        m.inputcollid = self.read_u32("inputcollid");
        m.op = match self.read_u32("op") {
            0 => MinMaxOp::IS_GREATEST,
            1 => MinMaxOp::IS_LEAST,
            other => panic!("readfuncs.c: bad MinMaxOp {other}"),
        };
        m.args = self.read_node_list("args")?;
        m.location = self.read_location("location");
        Ok(m.seal())
    }

    fn read_xml_expr(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut x = Node::build::<XmlExpr>(mcx)?;
        x.op = match self.read_u32("op") {
            0 => XmlExprOp::IS_XMLCONCAT,
            1 => XmlExprOp::IS_XMLELEMENT,
            2 => XmlExprOp::IS_XMLFOREST,
            3 => XmlExprOp::IS_XMLPARSE,
            4 => XmlExprOp::IS_XMLPI,
            5 => XmlExprOp::IS_XMLROOT,
            6 => XmlExprOp::IS_XMLSERIALIZE,
            7 => XmlExprOp::IS_DOCUMENT,
            other => panic!("readfuncs.c: bad XmlExprOp {other}"),
        };
        x.name = self.read_str("name")?;
        x.named_args = self.read_node_list("named_args")?;
        x.arg_names = self.read_node_list("arg_names")?;
        x.args = self.read_node_list("args")?;
        x.xmloption = xml_option_type(self.read_u32("xmloption"));
        x.indent = self.read_bool("indent");
        x.r#type = self.read_u32("type");
        x.typmod = self.read_i32("typmod");
        x.location = self.read_location("location");
        Ok(x.seal())
    }

    fn read_table_func(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut tf = Node::build::<TableFunc>(mcx)?;
        tf.functype = match self.read_u32("functype") {
            0 => TableFuncType::TFT_XMLTABLE,
            1 => TableFuncType::TFT_JSON_TABLE,
            other => panic!("readfuncs.c: bad TableFuncType {other}"),
        };
        tf.ns_uris = self.read_node_list("ns_uris")?;
        tf.ns_names = self.read_opt_node_list("ns_names")?;
        tf.docexpr = self.read_node("docexpr")?;
        tf.rowexpr = self.read_node("rowexpr")?;
        tf.colnames = self.read_node_list("colnames")?;
        tf.coltypes = self.read_oid_list("coltypes")?;
        tf.coltypmods = self.read_int_list("coltypmods")?;
        tf.colcollations = self.read_oid_list("colcollations")?;
        tf.colexprs = self.read_opt_node_list("colexprs")?;
        tf.coldefexprs = self.read_opt_node_list("coldefexprs")?;
        tf.colvalexprs = self.read_opt_node_list("colvalexprs")?;
        tf.passingvalexprs = self.read_node_list("passingvalexprs")?;
        tf.notnulls = self.read_bitmapset("notnulls")?;
        tf.plan = self.read_node("plan")?;
        tf.ordinalitycol = self.read_i32("ordinalitycol");
        tf.location = self.read_location("location");
        Ok(tf.seal())
    }

    fn read_json_table_path(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut p = Node::build::<types_nodes::primnodes::JsonTablePath>(mcx)?;
        p.value = self.read_node("value")?;
        p.name = self.read_str("name")?;
        Ok(p.seal())
    }

    fn read_json_table_path_scan(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut s = Node::build::<types_nodes::primnodes::JsonTablePathScan>(mcx)?;
        s.path = self.read_node("path")?;
        s.errorOnError = self.read_bool("errorOnError");
        s.child = self.read_node("child")?;
        s.colMin = self.read_i32("colMin");
        s.colMax = self.read_i32("colMax");
        Ok(s.seal())
    }

    fn read_json_table_sibling_join(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut j = Node::build::<types_nodes::primnodes::JsonTableSiblingJoin>(mcx)?;
        j.lplan = self.read_node("lplan")?;
        j.rplan = self.read_node("rplan")?;
        Ok(j.seal())
    }

    fn read_scalar_array_op_expr(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut s = Node::build::<ScalarArrayOpExpr>(mcx)?;
        s.opno = self.read_u32("opno");
        s.opfuncid = self.read_u32("opfuncid");
        s.hashfuncid = self.read_u32("hashfuncid");
        s.negfuncid = self.read_u32("negfuncid");
        s.useOr = self.read_bool("useOr");
        s.inputcollid = self.read_u32("inputcollid");
        s.args = self.read_node_list("args")?;
        s.location = self.read_location("location");
        Ok(s.seal())
    }

    fn read_sub_link(&mut self) -> PgResult<Node<'mcx>> {
        let subLinkType = sub_link_type(self.read_u32("subLinkType"));
        let subLinkId = self.read_i32("subLinkId");
        let testexpr = self.read_node("testexpr")?;
        let operName = self.read_node_list("operName")?;
        let subselect = self.read_node("subselect")?.expect("SubLink has a subselect");
        let location = self.read_location("location");
        Node::mk(
            self.mcx,
            SubLink { subLinkType, subLinkId, testexpr, operName, subselect, location },
        )
    }

    fn read_sub_plan(&mut self) -> PgResult<Node<'mcx>> {
        let subLinkType = sub_link_type(self.read_u32("subLinkType"));
        let testexpr = self.read_node("testexpr")?;
        let paramIds = self.read_int_list("paramIds")?;
        let plan_id = self.read_i32("plan_id");
        let plan_name = self.read_str("plan_name")?;
        let firstColType = self.read_u32("firstColType");
        let firstColTypmod = self.read_i32("firstColTypmod");
        let firstColCollation = self.read_u32("firstColCollation");
        let useHashTable = self.read_bool("useHashTable");
        let unknownEqFalse = self.read_bool("unknownEqFalse");
        let parallel_safe = self.read_bool("parallel_safe");
        let setParam = self.read_int_list("setParam")?;
        let parParam = self.read_int_list("parParam")?;
        let args = self.read_node_list("args")?;
        let startup_cost = self.read_f64("startup_cost");
        let per_call_cost = self.read_f64("per_call_cost");
        Node::mk(
            self.mcx,
            types_nodes::primnodes::SubPlan {
                subLinkType,
                testexpr,
                paramIds,
                plan_id,
                plan_name,
                firstColType,
                firstColTypmod,
                firstColCollation,
                useHashTable,
                unknownEqFalse,
                parallel_safe,
                setParam,
                parParam,
                args,
                startup_cost,
                per_call_cost,
            },
        )
    }

    fn read_alternative_sub_plan(&mut self) -> PgResult<Node<'mcx>> {
        let subplans = self.read_node_list("subplans")?;
        Node::mk(self.mcx, types_nodes::primnodes::AlternativeSubPlan { subplans })
    }

    fn read_param(&mut self) -> PgResult<Node<'mcx>> {
        let p = Param {
            paramkind: match self.read_u32("paramkind") {
                0 => ParamKind::PARAM_EXTERN,
                1 => ParamKind::PARAM_EXEC,
                2 => ParamKind::PARAM_SUBLINK,
                3 => ParamKind::PARAM_MULTIEXPR,
                other => panic!("readfuncs.c: bad ParamKind {other}"),
            },
            paramid: self.read_i32("paramid"),
            paramtype: self.read_u32("paramtype"),
            paramtypmod: self.read_i32("paramtypmod"),
            paramcollid: self.read_u32("paramcollid"),
            location: self.read_location("location"),
        };
        Node::mk(self.mcx, p)
    }

    fn read_array_expr(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut a = Node::build::<ArrayExpr>(mcx)?;
        a.array_typeid = self.read_u32("array_typeid");
        a.array_collid = self.read_u32("array_collid");
        a.element_typeid = self.read_u32("element_typeid");
        a.elements = self.read_node_list("elements")?;
        a.multidims = self.read_bool("multidims");
        a.list_start = self.read_location("list_start");
        a.list_end = self.read_location("list_end");
        a.location = self.read_location("location");
        Ok(a.seal())
    }

    // readDatum (readfuncs.c): "<len> [ <byte> ... ]"; byval always carries
    // sizeof(Datum) byte tokens regardless of the leading length.
    fn read_datum(&mut self, typbyval: bool) -> PgResult<Datum> {
        let length = Self::parse_int(self.token("datum length")) as usize;
        self.expect("[");
        if typbyval {
            assert!(length <= 8, "readDatum: byval length {length} too large");
            let mut word = [0u8; 8];
            for b in word.iter_mut() {
                *b = Self::parse_int(self.token("datum byte")) as u8;
            }
            self.expect("]");
            return Ok(Datum::from_u64(u64::from_le_bytes(word)));
        }
        if length == 0 {
            self.expect("]");
            return Ok(Datum::from_usize(0));
        }
        let mut v: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(self.mcx, length)?;
        for _ in 0..length {
            v.push(Self::parse_int(self.token("datum byte")) as u8);
        }
        self.expect("]");
        Ok(Datum::from_usize(v.leak().as_ptr() as usize))
    }
}

fn json_format_type(v: u32) -> types_nodes::primnodes::JsonFormatType {
    use types_nodes::primnodes::JsonFormatType::*;
    match v {
        1 => JS_FORMAT_JSON,
        2 => JS_FORMAT_JSONB,
        _ => JS_FORMAT_DEFAULT,
    }
}

fn json_encoding(v: u32) -> types_nodes::primnodes::JsonEncoding {
    use types_nodes::primnodes::JsonEncoding::*;
    match v {
        1 => JS_ENC_UTF8,
        2 => JS_ENC_UTF16,
        3 => JS_ENC_UTF32,
        _ => JS_ENC_DEFAULT,
    }
}

fn json_ctor_type(v: u32) -> types_nodes::JsonConstructorType {
    use types_nodes::JsonConstructorType::*;
    match v {
        1 => JSCTOR_JSON_OBJECT,
        2 => JSCTOR_JSON_ARRAY,
        3 => JSCTOR_JSON_OBJECTAGG,
        4 => JSCTOR_JSON_ARRAYAGG,
        5 => JSCTOR_JSON_PARSE,
        6 => JSCTOR_JSON_SCALAR,
        7 => JSCTOR_JSON_SERIALIZE,
        other => panic!("readfuncs.c: bad JsonConstructorType {other}"),
    }
}

fn json_value_type(v: u32) -> types_nodes::JsonValueType {
    use types_nodes::JsonValueType::*;
    match v {
        1 => JS_TYPE_OBJECT,
        2 => JS_TYPE_ARRAY,
        3 => JS_TYPE_SCALAR,
        _ => JS_TYPE_ANY,
    }
}

fn json_behavior_type(v: u32) -> types_nodes::JsonBehaviorType {
    use types_nodes::JsonBehaviorType::*;
    match v {
        1 => JSON_BEHAVIOR_ERROR,
        2 => JSON_BEHAVIOR_EMPTY,
        3 => JSON_BEHAVIOR_TRUE,
        4 => JSON_BEHAVIOR_FALSE,
        5 => JSON_BEHAVIOR_UNKNOWN,
        6 => JSON_BEHAVIOR_EMPTY_ARRAY,
        7 => JSON_BEHAVIOR_EMPTY_OBJECT,
        8 => JSON_BEHAVIOR_DEFAULT,
        _ => JSON_BEHAVIOR_NULL,
    }
}

fn json_expr_op(v: u32) -> types_nodes::JsonExprOp {
    use types_nodes::JsonExprOp::*;
    match v {
        1 => JSON_QUERY_OP,
        2 => JSON_VALUE_OP,
        3 => JSON_TABLE_OP,
        _ => JSON_EXISTS_OP,
    }
}

fn json_wrapper(v: u32) -> types_nodes::JsonWrapper {
    use types_nodes::JsonWrapper::*;
    match v {
        1 => JSW_NONE,
        2 => JSW_CONDITIONAL,
        3 => JSW_UNCONDITIONAL,
        _ => JSW_UNSPEC,
    }
}

fn bool_test_type(v: u32) -> types_nodes::primnodes::BoolTestType {
    use types_nodes::primnodes::BoolTestType::*;
    match v {
        0 => IS_TRUE,
        1 => IS_NOT_TRUE,
        2 => IS_FALSE,
        3 => IS_NOT_FALSE,
        4 => IS_UNKNOWN,
        5 => IS_NOT_UNKNOWN,
        other => panic!("readfuncs.c: bad BoolTestType {other}"),
    }
}

fn cmd_type(v: u32) -> CmdType {
    match v {
        0 => CmdType::CMD_UNKNOWN,
        1 => CmdType::CMD_SELECT,
        2 => CmdType::CMD_UPDATE,
        3 => CmdType::CMD_INSERT,
        4 => CmdType::CMD_DELETE,
        5 => CmdType::CMD_MERGE,
        6 => CmdType::CMD_UTILITY,
        7 => CmdType::CMD_NOTHING,
        other => panic!("readfuncs.c: bad CmdType {other}"),
    }
}

fn query_source(v: u32) -> QuerySource {
    match v {
        0 => QuerySource::QSRC_ORIGINAL,
        1 => QuerySource::QSRC_PARSER,
        2 => QuerySource::QSRC_INSTEAD_RULE,
        3 => QuerySource::QSRC_QUAL_INSTEAD_RULE,
        4 => QuerySource::QSRC_NON_INSTEAD_RULE,
        other => panic!("readfuncs.c: bad QuerySource {other}"),
    }
}

fn xml_option_type(v: u32) -> XmlOptionType {
    match v {
        0 => XmlOptionType::XMLOPTION_DOCUMENT,
        1 => XmlOptionType::XMLOPTION_CONTENT,
        other => panic!("readfuncs.c: bad XmlOptionType {other}"),
    }
}

fn rte_kind(v: u32) -> RTEKind {
    match v {
        0 => RTEKind::RTE_RELATION,
        1 => RTEKind::RTE_SUBQUERY,
        2 => RTEKind::RTE_JOIN,
        3 => RTEKind::RTE_FUNCTION,
        4 => RTEKind::RTE_TABLEFUNC,
        5 => RTEKind::RTE_VALUES,
        6 => RTEKind::RTE_CTE,
        7 => RTEKind::RTE_NAMEDTUPLESTORE,
        8 => RTEKind::RTE_RESULT,
        9 => RTEKind::RTE_GROUP,
        other => panic!("readfuncs.c: bad RTEKind {other}"),
    }
}

fn join_type(v: u32) -> JoinType {
    match v {
        0 => JoinType::JOIN_INNER,
        1 => JoinType::JOIN_LEFT,
        2 => JoinType::JOIN_FULL,
        3 => JoinType::JOIN_RIGHT,
        4 => JoinType::JOIN_SEMI,
        5 => JoinType::JOIN_ANTI,
        6 => JoinType::JOIN_RIGHT_SEMI,
        7 => JoinType::JOIN_RIGHT_ANTI,
        8 => JoinType::JOIN_UNIQUE_OUTER,
        9 => JoinType::JOIN_UNIQUE_INNER,
        other => panic!("readfuncs.c: bad JoinType {other}"),
    }
}

fn overriding_kind(v: u32) -> OverridingKind {
    match v {
        0 => OverridingKind::OVERRIDING_NOT_SET,
        1 => OverridingKind::OVERRIDING_USER_VALUE,
        2 => OverridingKind::OVERRIDING_SYSTEM_VALUE,
        other => panic!("readfuncs.c: bad OverridingKind {other}"),
    }
}

fn merge_match_kind(v: u32) -> MergeMatchKind {
    match v {
        0 => MergeMatchKind::MERGE_WHEN_MATCHED,
        1 => MergeMatchKind::MERGE_WHEN_NOT_MATCHED_BY_SOURCE,
        2 => MergeMatchKind::MERGE_WHEN_NOT_MATCHED_BY_TARGET,
        other => panic!("readfuncs.c: bad MergeMatchKind {other}"),
    }
}

fn cte_materialize(v: u32) -> CTEMaterialize {
    match v {
        0 => CTEMaterialize::CTEMaterializeDefault,
        1 => CTEMaterialize::CTEMaterializeAlways,
        2 => CTEMaterialize::CTEMaterializeNever,
        other => panic!("readfuncs.c: bad CTEMaterialize {other}"),
    }
}

fn limit_option(v: u32) -> LimitOption {
    match v {
        0 => LimitOption::LIMIT_OPTION_COUNT,
        1 => LimitOption::LIMIT_OPTION_WITH_TIES,
        other => panic!("readfuncs.c: bad LimitOption {other}"),
    }
}

fn set_operation(v: u32) -> SetOperation {
    match v {
        0 => SetOperation::SETOP_NONE,
        1 => SetOperation::SETOP_UNION,
        2 => SetOperation::SETOP_INTERSECT,
        3 => SetOperation::SETOP_EXCEPT,
        other => panic!("readfuncs.c: bad SetOperation {other}"),
    }
}

fn sub_link_type(v: u32) -> SubLinkType {
    match v {
        0 => SubLinkType::EXISTS_SUBLINK,
        1 => SubLinkType::ALL_SUBLINK,
        2 => SubLinkType::ANY_SUBLINK,
        3 => SubLinkType::ROWCOMPARE_SUBLINK,
        4 => SubLinkType::EXPR_SUBLINK,
        5 => SubLinkType::MULTIEXPR_SUBLINK,
        6 => SubLinkType::ARRAY_SUBLINK,
        7 => SubLinkType::CTE_SUBLINK,
        other => panic!("readfuncs.c: bad SubLinkType {other}"),
    }
}

fn on_conflict_action(v: u32) -> types_nodes::primnodes::OnConflictAction {
    use types_nodes::primnodes::OnConflictAction as A;
    match v {
        0 => A::ONCONFLICT_NONE,
        1 => A::ONCONFLICT_NOTHING,
        2 => A::ONCONFLICT_UPDATE,
        other => panic!("readfuncs.c: bad OnConflictAction {other}"),
    }
}

fn var_returning_type(v: u32) -> VarReturningType {
    match v {
        0 => VarReturningType::VAR_RETURNING_DEFAULT,
        1 => VarReturningType::VAR_RETURNING_OLD,
        2 => VarReturningType::VAR_RETURNING_NEW,
        other => panic!("readfuncs.c: bad VarReturningType {other}"),
    }
}

fn coercion_form(v: u32) -> CoercionForm {
    match v {
        0 => CoercionForm::COERCE_EXPLICIT_CALL,
        1 => CoercionForm::COERCE_EXPLICIT_CAST,
        2 => CoercionForm::COERCE_IMPLICIT_CAST,
        3 => CoercionForm::COERCE_SQL_SYNTAX,
        other => panic!("readfuncs.c: bad CoercionForm {other}"),
    }
}
