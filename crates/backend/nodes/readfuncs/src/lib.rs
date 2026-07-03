//! read.c + readfuncs.c, minimal arm: exactly the node set a stored view
//! SELECT rule (pg_rewrite ev_action) can contain; every other node label or
//! token shape is a loud panic naming the C reader.

#![allow(non_snake_case)]

use datum::Datum;
use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_nodes::bitmapset::Bitmapset;
use types_nodes::list::{IntList, NodeList, OidList};
use types_nodes::jointype::JoinType;
use types_nodes::nodes_enums::{CmdType, LimitOption};
use types_nodes::parsenodes::{
    Query, QuerySource, RTEKind, RTEPermissionInfo, RangeTblEntry, RangeTblFunction, SetOperation,
    SetOperationStmt, SortGroupClause,
};
use types_nodes::primnodes::{
    Aggref, Alias, ArrayExpr, BoolExpr, BoolExprType, CaseExpr, CaseTestExpr, CaseWhen,
    CoalesceExpr, CoerceViaIO, CoercionForm, Const, FromExpr, FuncExpr, JoinExpr, MinMaxExpr,
    MinMaxOp, NullTest, NullTestType, OpExpr, OverridingKind, Param, ParamKind, RangeTblRef,
    RelabelType, ScalarArrayOpExpr, SubLink, SubLinkType, TargetEntry, Var, VarReturningType,
};
use types_nodes::Node;

#[cfg(test)]
mod tests;

pub fn stringToNode<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Node<'mcx>> {
    let mut r = Reader { mcx, buf: s.as_bytes(), pos: 0 };
    let node = r.node_read().expect("stringToNode: empty input")?;
    Ok(node.expect("stringToNode: <> input"))
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
            let elem = self
                .node_read_token(tok)?
                .expect("nodeRead: <> is not a valid list element here");
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
            let elem = self
                .node_read_token(tok)?
                .expect("nodeRead: <> is not a valid list element here");
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
            b"CONST" => self.read_const(),
            b"OPEXPR" => self.read_op_expr(),
            b"FUNCEXPR" => self.read_func_expr(),
            b"BOOLEXPR" => self.read_bool_expr(),
            b"RELABELTYPE" => self.read_relabel_type(),
            b"COERCEVIAIO" => self.read_coerce_via_io(),
            b"COERCETODOMAIN" => self.read_coerce_to_domain(),
            b"COERCETODOMAINVALUE" => self.read_coerce_to_domain_value(),
            b"PARTITIONBOUNDSPEC" => self.read_partition_bound_spec(),
            b"PARTITIONRANGEDATUM" => self.read_partition_range_datum(),
            b"NULLTEST" => self.read_null_test(),
            b"SORTGROUPCLAUSE" => self.read_sort_group_clause(),
            b"SETOPERATIONSTMT" => self.read_set_operation_stmt(),
            b"AGGREF" => self.read_aggref(),
            b"CASEEXPR" => self.read_case_expr(),
            b"CASEWHEN" => self.read_case_when(),
            b"CASETESTEXPR" => self.read_case_test_expr(),
            b"COALESCEEXPR" => self.read_coalesce_expr(),
            b"MINMAXEXPR" => self.read_min_max_expr(),
            b"SCALARARRAYOPEXPR" => self.read_scalar_array_op_expr(),
            b"SUBLINK" => self.read_sub_link(),
            b"PARAM" => self.read_param(),
            b"ARRAYEXPR" => self.read_array_expr(),
            b"SETTODEFAULT" => self.read_set_to_default(),
            b"BOOLEANTEST" => self.read_boolean_test(),
            other => panic!(
                "parseNodeString (readfuncs.c): {} read arm unported (view SELECT-rule + \
                 DEFAULT/CHECK expr sets only)",
                String::from_utf8_lossy(other)
            ),
        }
    }

    fn read_boolean_test(&mut self) -> PgResult<Node<'mcx>> {
        let mcx = self.mcx;
        let mut bt = Node::build::<types_nodes::primnodes::BooleanTest>(mcx)?;
        bt.arg = self.read_node("arg")?;
        bt.booltesttype = bool_test_type(self.read_u32("booltesttype"));
        bt.location = self.read_location("location");
        Ok(bt.seal())
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
            RTEKind::RTE_VALUES => {
                rte.values_lists = self.read_node_list("values_lists")?;
                rte.coltypes = self.read_oid_list("coltypes")?;
                rte.coltypmods = self.read_int_list("coltypmods")?;
                rte.colcollations = self.read_oid_list("colcollations")?;
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
