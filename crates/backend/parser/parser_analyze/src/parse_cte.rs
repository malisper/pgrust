// parse_cte.c: WITH and WITH RECURSIVE analysis; SEARCH/CYCLE clauses and
// data-modifying CTEs are loud panics naming their lanes.
#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
use types_core::catalog::{DEFAULT_COLLATION_OID, TEXTOID, UNKNOWNOID};
use types_core::{InvalidOid, Oid, ParseLoc};
use types_error::{
    ERRCODE_COLLATION_MISMATCH, ERRCODE_DATATYPE_MISMATCH, ERRCODE_DUPLICATE_ALIAS,
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_COLUMN_REFERENCE, ERRCODE_INVALID_RECURSION,
    ErrorLocation, PgError, PgResult, ERROR,
};
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{CommonTableExpr, SetOperation, WithClause};
use types_nodes::primnodes::TargetEntry;
use types_nodes::rawnodes::SelectStmt;
use types_nodes::{Bitmapset, JoinType, Node, NodeList, NodeTag};

use nodes_core::NodeWalker;
use parser_small1::{parser_errposition, ParseState};

pub fn transformWithClause<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    with: Node<'mcx>,
) -> PgResult<NodeList<'mcx>> {
    let wc = with.as_with_clause().expect("withClause is a WithClause");
    debug_assert!(pstate.p_ctenamespace.is_nil());
    debug_assert!(pstate.p_future_ctes.is_nil());

    for (i, cte_node) in wc.ctes.iter().enumerate() {
        {
            let cte = cte_node.as_common_table_expr().expect("WITH list cell");
            for later in wc.ctes.iter().skip(i + 1) {
                let cte2 = later.as_common_table_expr().expect("WITH list cell");
                if cte2.ctename == cte.ctename {
                    return Err(duplicate_cte_name(
                        pstate,
                        cte2.ctename.unwrap_or(""),
                        cte2.location,
                    ));
                }
            }
            if cte.ctequery.expect("CTE has no query").node_tag() != NodeTag::T_SelectStmt {
                panic!(
                    "transformWithClause (parse_cte.c): data-modifying CTE \"{}\"; \
                     DML WITH lane",
                    cte.ctename.unwrap_or("")
                );
            }
        }
        // SAFETY: parser-owned tree under analysis; no live derived refs.
        unsafe {
            cte_node.with_mut::<CommonTableExpr, _>(|c| {
                c.cterecursive = false;
                c.cterefcount = 0;
            })
        };
    }

    if wc.recursive {
        let mut items: PgVec<'mcx, CteItem<'mcx>> =
            mcx::vec_with_capacity_in(mcx, wc.ctes.len())?;
        for (i, cte_node) in wc.ctes.iter().enumerate() {
            items.push(CteItem { cte: cte_node, id: i as i32, depends_on: Bitmapset::empty() });
        }

        makeDependencyGraph(mcx, pstate, &mut items)?;
        checkWellFormedRecursion(mcx, pstate, &items)?;

        for item in items.iter() {
            pstate.p_ctenamespace.lappend(mcx, item.cte)?;
        }
        for item in items.iter() {
            analyzeCTE(mcx, pstate, item.cte)?;
        }
    } else {
        // C list_copy: fresh cells, shared CommonTableExpr nodes.
        pstate.p_future_ctes = wc.ctes.clone_in(mcx)?;
        for (i, cte_node) in wc.ctes.iter().enumerate() {
            analyzeCTE(mcx, pstate, cte_node)?;
            pstate.p_ctenamespace.lappend(mcx, cte_node)?;
            let mut rest = NodeList::nil();
            for later in wc.ctes.iter().skip(i + 1) {
                rest.lappend(mcx, later)?;
            }
            pstate.p_future_ctes = rest;
        }
    }

    pstate.p_ctenamespace.clone_in(mcx)
}

fn analyzeCTE<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    cte_node: Node<'mcx>,
) -> PgResult<()> {
    let (ctequery, location, cterecursive, ctename) = {
        let cte = cte_node.as_common_table_expr().expect("WITH list cell");
        if cte.search_clause.is_some() || cte.cycle_clause.is_some() {
            panic!("analyzeCTE (parse_cte.c): SEARCH/CYCLE clause; recursive-cte lane");
        }
        (
            cte.ctequery.expect("CTE has no query"),
            cte.location,
            cte.cterecursive,
            cte.ctename.unwrap_or(""),
        )
    };
    debug_assert!(ctequery.node_tag() != NodeTag::T_Query);

    let mut query = crate::parse_sub_analyze(mcx, ctequery, pstate, Some(cte_node), false, true)?;

    if query.utilityStmt.is_some() {
        return Err(elog_error("unexpected utility statement in WITH"));
    }
    if query.commandType != CmdType::CMD_SELECT && pstate.parentParseState.is_some() {
        return Err(Box::new(
            elog::ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(
                    "WITH clause containing a data-modifying statement must be at the top level"
                        .to_string(),
                )
                .errposition(parser_errposition(
                    pstate,
                    location,
                    mbutils::GetDatabaseEncoding(),
                ))
                .into_error()
                .with_error_location(ErrorLocation::new("parse_cte.c", 0, "analyzeCTE")),
        ));
    }

    query.canSetTag = false;
    let query_node = Node::mk(mcx, query)?;
    // SAFETY: parser-owned tree under analysis; no live derived refs.
    unsafe { cte_node.with_mut::<CommonTableExpr, _>(|c| c.ctequery = Some(query_node)) };

    let q = query_node.as_query().expect("just built");
    // GetCTETargetList's returningList arm is dead (DML CTEs are loud).
    debug_assert!(q.commandType == CmdType::CMD_SELECT);

    if !cterecursive {
        analyzeCTETargetList(mcx, pstate, cte_node, &q.targetList)
    } else {
        // The output columns were set from the non-recursive term
        // (determineRecursiveColTypes); the whole query must agree.
        let cte = cte_node.as_common_table_expr().expect("WITH list cell");
        let ncols = cte.ctecoltypes.len();
        let mut varattno: i32 = 0;
        let mut i = 0usize;
        for te_node in &q.targetList {
            let te = te_node.as_variant::<TargetEntry>().expect("tlist cell");
            if te.resjunk {
                continue;
            }
            varattno += 1;
            debug_assert_eq!(varattno, te.resno as i32);
            if i >= ncols {
                return Err(elog_error("wrong number of output columns in WITH"));
            }
            let texpr = te.expr;
            let coltype = cte.ctecoltypes.nth(i);
            let coltypmod = cte.ctecoltypmods.nth(i);
            let colcoll = cte.ctecolcollations.nth(i);
            if parse_expr::expr_type(texpr) != coltype
                || parse_expr::expr_typmod(texpr) != coltypmod
            {
                let expected = format_type::format_type_with_typemod(coltype, coltypmod)?;
                let actual = format_type::format_type_with_typemod(
                    parse_expr::expr_type(texpr),
                    parse_expr::expr_typmod(texpr),
                )?;
                return Err(recursive_type_mismatch(
                    pstate, ctename, varattno, &expected, &actual, texpr,
                ));
            }
            if parse_expr::expr_collation(texpr) != colcoll {
                let expected = collation_name_or_null(mcx, colcoll)?;
                let actual =
                    collation_name_or_null(mcx, parse_expr::expr_collation(texpr))?;
                return Err(recursive_collation_mismatch(
                    pstate, ctename, varattno, &expected, &actual, texpr,
                ));
            }
            i += 1;
        }
        if i != ncols {
            return Err(elog_error("wrong number of output columns in WITH"));
        }
        Ok(())
    }
}

pub(crate) fn analyzeCTETargetList<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    cte_node: Node<'mcx>,
    tlist: &NodeList<'mcx>,
) -> PgResult<()> {
    let (aliascolnames, cterecursive, ctename, location) = {
        let cte = cte_node.as_common_table_expr().expect("WITH list cell");
        debug_assert!(cte.ctecolnames.is_nil());
        (cte.aliascolnames.clone_in(mcx)?, cte.cterecursive, cte.ctename.unwrap_or(""), cte.location)
    };

    let numaliases = aliascolnames.len() as i32;
    let mut colnames = aliascolnames;
    let mut ctypes = types_nodes::list::OidList::nil();
    let mut ctypmods = types_nodes::list::IntList::nil();
    let mut ccolls = types_nodes::list::OidList::nil();
    let mut varattno: i32 = 0;
    for te_node in tlist {
        let te = te_node.as_variant::<TargetEntry>().expect("tlist cell");
        if te.resjunk {
            continue;
        }
        varattno += 1;
        debug_assert_eq!(varattno, te.resno as i32);
        if varattno > numaliases {
            let name = te.resname.expect("non-junk tlist entry has resname");
            colnames.lappend(mcx, Node::mk_string(mcx, name)?)?;
        }
        let mut coltype = parse_expr::expr_type(te.expr);
        let mut coltypmod = parse_expr::expr_typmod(te.expr);
        let mut colcoll = parse_expr::expr_collation(te.expr);
        // C: a recursive CTE resolves unknown outputs to text before the
        // recursive term is examined; a set collation is kept.
        if cterecursive && coltype == UNKNOWNOID {
            coltype = TEXTOID;
            coltypmod = -1;
            if colcoll == InvalidOid {
                colcoll = DEFAULT_COLLATION_OID;
            }
        }
        ctypes.lappend(mcx, coltype)?;
        ctypmods.lappend(mcx, coltypmod)?;
        ccolls.lappend(mcx, colcoll)?;
    }
    if varattno < numaliases {
        return Err(Box::new(
            elog::ereport(ERROR)
                .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
                .errmsg(format!(
                    "WITH query \"{ctename}\" has {varattno} columns available but \
                     {numaliases} columns specified"
                ))
                .errposition(parser_errposition(
                    pstate,
                    location,
                    mbutils::GetDatabaseEncoding(),
                ))
                .into_error()
                .with_error_location(ErrorLocation::new("parse_cte.c", 0, "analyzeCTETargetList")),
        ));
    }

    // SAFETY: parser-owned tree under analysis; no live derived refs.
    unsafe {
        cte_node.with_mut::<CommonTableExpr, _>(|c| {
            c.ctecolnames = colnames;
            c.ctecoltypes = ctypes;
            c.ctecoltypmods = ctypmods;
            c.ctecolcollations = ccolls;
        })
    };
    Ok(())
}

struct CteItem<'mcx> {
    cte: Node<'mcx>,
    id: i32,
    depends_on: Bitmapset<'mcx>,
}

fn makeDependencyGraph<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    items: &mut PgVec<'mcx, CteItem<'mcx>>,
) -> PgResult<()> {
    for i in 0..items.len() {
        let ctequery = items[i]
            .cte
            .as_common_table_expr()
            .expect("WITH list cell")
            .ctequery
            .expect("CTE has no query");
        let mut w = DependencyGraphWalker {
            mcx,
            items: &mut *items,
            curitem: i,
            innerwiths: PgVec::new_in(mcx),
        };
        w.visit(ctequery)?;
        debug_assert!(w.innerwiths.is_empty());
    }

    TopologicalSort(pstate, items)
}

struct DependencyGraphWalker<'a, 'mcx> {
    mcx: Mcx<'mcx>,
    items: &'a mut PgVec<'mcx, CteItem<'mcx>>,
    curitem: usize,
    innerwiths: PgVec<'mcx, NodeList<'mcx>>,
}

impl<'a, 'mcx> DependencyGraphWalker<'a, 'mcx> {
    fn walk_inner_with(
        &mut self,
        stmt: &'mcx SelectStmt<'mcx>,
        wc: &'mcx WithClause<'mcx>,
    ) -> PgResult<()> {
        if wc.recursive {
            self.innerwiths.push(wc.ctes.clone_in(self.mcx)?);
            for cte_node in &wc.ctes {
                let q = cte_node
                    .as_common_table_expr()
                    .expect("WITH list cell")
                    .ctequery
                    .expect("CTE has no query");
                self.visit(q)?;
            }
            nodes_core::walk_select_stmt(stmt, self)?;
            self.innerwiths.pop();
        } else {
            self.innerwiths.push(NodeList::nil());
            for cte_node in &wc.ctes {
                let q = cte_node
                    .as_common_table_expr()
                    .expect("WITH list cell")
                    .ctequery
                    .expect("CTE has no query");
                self.visit(q)?;
                let mcx = self.mcx;
                let top = self.innerwiths.len() - 1;
                self.innerwiths[top].lappend(mcx, cte_node)?;
            }
            nodes_core::walk_select_stmt(stmt, self)?;
            self.innerwiths.pop();
        }
        Ok(())
    }
}

impl<'a, 'mcx> NodeWalker<'mcx> for DependencyGraphWalker<'a, 'mcx> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        match node.node_tag() {
            NodeTag::T_RangeVar => {
                let rv = node.as_range_var().expect("tag checked");
                if rv.schemaname.is_none() {
                    let relname = rv.relname.expect("grammar always sets relname");
                    if name_captured(&self.innerwiths, relname) {
                        return Ok(false);
                    }
                    for i in 0..self.items.len() {
                        let matches = self.items[i]
                            .cte
                            .as_common_table_expr()
                            .expect("WITH list cell")
                            .ctename
                            == Some(relname);
                        if matches {
                            if i != self.curitem {
                                let id = self.items[i].id;
                                let mcx = self.mcx;
                                self.items[self.curitem].depends_on.add_member(mcx, id)?;
                            } else {
                                // SAFETY: parser-owned tree under analysis; no
                                // live derived refs.
                                unsafe {
                                    self.items[i]
                                        .cte
                                        .with_mut::<CommonTableExpr, _>(|c| c.cterecursive = true)
                                };
                            }
                            break;
                        }
                    }
                }
                Ok(false)
            }
            NodeTag::T_SelectStmt => {
                self.visit_select_stmt_ref(node.as_select_stmt().expect("tag checked"))
            }
            NodeTag::T_WithClause => Ok(false),
            _ => walk_raw(node, self),
        }
    }

    fn visit_select_stmt_ref(&mut self, s: &'mcx SelectStmt<'mcx>) -> PgResult<bool> {
        match s.withClause.and_then(|n| n.as_with_clause()) {
            Some(wc) => {
                self.walk_inner_with(s, wc)?;
                Ok(false)
            }
            None => nodes_core::walk_select_stmt(s, self),
        }
    }
}

fn name_captured(innerwiths: &PgVec<'_, NodeList<'_>>, relname: &str) -> bool {
    for frame in innerwiths.iter() {
        for cte_node in frame {
            if cte_node.as_common_table_expr().expect("WITH list cell").ctename == Some(relname) {
                return true;
            }
        }
    }
    false
}

// Raw-tree arms C's raw_expression_tree_walker carries that nodes_core's port
// has not grown yet; unlisted tags stay loud through the nodes_core fallback.
fn walk_raw<'mcx, W: NodeWalker<'mcx> + ?Sized>(node: Node<'mcx>, w: &mut W) -> PgResult<bool> {
    match node.node_tag() {
        NodeTag::T_BoolExpr => {
            nodes_core::walk_list(&node.as_bool_expr().expect("tag checked").args, w)
        }
        NodeTag::T_NullTest => {
            nodes_core::walk_opt(node.as_null_test().expect("tag checked").arg, w)
        }
        NodeTag::T_SubLink => {
            let sl = node.as_sub_link().expect("tag checked");
            Ok(nodes_core::walk_opt(sl.testexpr, w)? || w.visit(sl.subselect)?)
        }
        NodeTag::T_JoinExpr => {
            let j = node.as_join_expr().expect("tag checked");
            Ok(w.visit(j.larg)? || w.visit(j.rarg)? || nodes_core::walk_opt(j.quals, w)?)
        }
        NodeTag::T_RangeSubselect => {
            nodes_core::walk_opt(node.as_range_subselect().expect("tag checked").subquery, w)
        }
        NodeTag::T_CommonTableExpr => {
            nodes_core::walk_opt(node.as_common_table_expr().expect("tag checked").ctequery, w)
        }
        NodeTag::T_LockingClause => {
            nodes_core::walk_list(&node.as_locking_clause().expect("tag checked").lockedRels, w)
        }
        _ => nodes_core::raw_expression_tree_walker(node, w),
    }
}

fn TopologicalSort<'mcx>(
    pstate: &ParseState<'_, 'mcx>,
    items: &mut PgVec<'mcx, CteItem<'mcx>>,
) -> PgResult<()> {
    let numitems = items.len();
    for i in 0..numitems {
        let mut j = i;
        while j < numitems && !items[j].depends_on.is_empty() {
            j += 1;
        }

        if j >= numitems {
            let location =
                items[i].cte.as_common_table_expr().expect("WITH list cell").location;
            return Err(mutual_recursion(pstate, location));
        }

        if i != j {
            items.swap(i, j);
        }

        let id = items[i].id;
        for item in items.iter_mut().skip(i + 1) {
            item.depends_on.del_member(id);
        }
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RecursionContext {
    Allowed,
    NonRecursiveTerm,
    Sublink,
    OuterJoin,
    Intersect,
    Except,
}

fn recursion_errormsg(ctx: RecursionContext, ctename: &str) -> String {
    match ctx {
        RecursionContext::Allowed => unreachable!("RECURSION_OK has no message"),
        RecursionContext::NonRecursiveTerm => format!(
            "recursive reference to query \"{ctename}\" must not appear within its non-recursive term"
        ),
        RecursionContext::Sublink => {
            format!("recursive reference to query \"{ctename}\" must not appear within a subquery")
        }
        RecursionContext::OuterJoin => format!(
            "recursive reference to query \"{ctename}\" must not appear within an outer join"
        ),
        RecursionContext::Intersect => format!(
            "recursive reference to query \"{ctename}\" must not appear within INTERSECT"
        ),
        RecursionContext::Except => {
            format!("recursive reference to query \"{ctename}\" must not appear within EXCEPT")
        }
    }
}

fn checkWellFormedRecursion<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &ParseState<'_, 'mcx>,
    items: &PgVec<'mcx, CteItem<'mcx>>,
) -> PgResult<()> {
    for item in items.iter() {
        let (ctename, location, recursive, ctequery) = {
            let cte = item.cte.as_common_table_expr().expect("WITH list cell");
            (
                cte.ctename.unwrap_or(""),
                cte.location,
                cte.cterecursive,
                cte.ctequery.expect("CTE has no query"),
            )
        };
        debug_assert!(ctequery.node_tag() != NodeTag::T_Query);

        if !recursive {
            continue;
        }

        let stmt = match ctequery.as_select_stmt() {
            Some(s) => s,
            None => {
                return Err(invalid_recursion(
                    pstate,
                    format!(
                        "recursive query \"{ctename}\" must not contain data-modifying statements"
                    ),
                    location,
                ))
            }
        };

        if stmt.op != SetOperation::SETOP_UNION {
            return Err(invalid_recursion(
                pstate,
                format!(
                    "recursive query \"{ctename}\" does not have the form \
                     non-recursive-term UNION [ALL] recursive-term"
                ),
                location,
            ));
        }

        // C: a top-level WITH is tolerated, but must not self-reference; test
        // it before the UNION arms to avoid confusing errors.
        if let Some(wc_node) = stmt.withClause {
            let wc = wc_node.as_with_clause().expect("withClause is a WithClause");
            let mut w = WellFormedWalker {
                mcx,
                pstate,
                myname: ctename,
                innerwiths: PgVec::new_in(mcx),
                selfrefcount: 0,
                context: RecursionContext::Sublink,
            };
            nodes_core::walk_list(&wc.ctes, &mut w)?;
            debug_assert!(w.innerwiths.is_empty());
        }

        if !stmt.sortClause.is_nil() {
            return Err(recursive_decoration(
                pstate,
                "ORDER BY in a recursive query is not implemented",
                raw_list_location(&stmt.sortClause),
            ));
        }
        if let Some(off) = stmt.limitOffset {
            return Err(recursive_decoration(
                pstate,
                "OFFSET in a recursive query is not implemented",
                raw_expr_location(off),
            ));
        }
        if let Some(cnt) = stmt.limitCount {
            return Err(recursive_decoration(
                pstate,
                "LIMIT in a recursive query is not implemented",
                raw_expr_location(cnt),
            ));
        }
        if !stmt.lockingClause.is_nil() {
            return Err(recursive_decoration(
                pstate,
                "FOR UPDATE/SHARE in a recursive query is not implemented",
                raw_list_location(&stmt.lockingClause),
            ));
        }

        let mut w = WellFormedWalker {
            mcx,
            pstate,
            myname: ctename,
            innerwiths: PgVec::new_in(mcx),
            selfrefcount: 0,
            context: RecursionContext::NonRecursiveTerm,
        };
        w.visit_select_stmt_ref(stmt.larg.expect("set-op node has larg"))?;
        debug_assert!(w.innerwiths.is_empty());

        let mut w = WellFormedWalker {
            mcx,
            pstate,
            myname: ctename,
            innerwiths: PgVec::new_in(mcx),
            selfrefcount: 0,
            context: RecursionContext::Allowed,
        };
        w.visit_select_stmt_ref(stmt.rarg.expect("set-op node has rarg"))?;
        debug_assert!(w.innerwiths.is_empty());
        if w.selfrefcount != 1 {
            return Err(elog_error("missing recursive reference"));
        }
    }
    Ok(())
}

struct WellFormedWalker<'a, 'p, 'mcx> {
    mcx: Mcx<'mcx>,
    pstate: &'a ParseState<'p, 'mcx>,
    myname: &'mcx str,
    innerwiths: PgVec<'mcx, NodeList<'mcx>>,
    selfrefcount: i32,
    context: RecursionContext,
}

impl<'a, 'p, 'mcx> WellFormedWalker<'a, 'p, 'mcx> {
    fn check_select_stmt(&mut self, s: &'mcx SelectStmt<'mcx>) -> PgResult<()> {
        let save_context = self.context;
        if save_context != RecursionContext::Allowed {
            nodes_core::walk_select_stmt(s, self)?;
            return Ok(());
        }
        match s.op {
            SetOperation::SETOP_NONE | SetOperation::SETOP_UNION => {
                nodes_core::walk_select_stmt(s, self)?;
            }
            SetOperation::SETOP_INTERSECT => {
                if s.all {
                    self.context = RecursionContext::Intersect;
                }
                self.visit_select_stmt_ref(s.larg.expect("set-op node has larg"))?;
                self.visit_select_stmt_ref(s.rarg.expect("set-op node has rarg"))?;
                self.context = save_context;
                nodes_core::walk_list(&s.sortClause, self)?;
                nodes_core::walk_opt(s.limitOffset, self)?;
                nodes_core::walk_opt(s.limitCount, self)?;
                nodes_core::walk_list(&s.lockingClause, self)?;
                // withClause is intentionally ignored here.
            }
            SetOperation::SETOP_EXCEPT => {
                if s.all {
                    self.context = RecursionContext::Except;
                }
                self.visit_select_stmt_ref(s.larg.expect("set-op node has larg"))?;
                self.context = RecursionContext::Except;
                self.visit_select_stmt_ref(s.rarg.expect("set-op node has rarg"))?;
                self.context = save_context;
                nodes_core::walk_list(&s.sortClause, self)?;
                nodes_core::walk_opt(s.limitOffset, self)?;
                nodes_core::walk_opt(s.limitCount, self)?;
                nodes_core::walk_list(&s.lockingClause, self)?;
                // withClause is intentionally ignored here.
            }
        }
        Ok(())
    }
}

impl<'a, 'p, 'mcx> NodeWalker<'mcx> for WellFormedWalker<'a, 'p, 'mcx> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        let save_context = self.context;
        match node.node_tag() {
            NodeTag::T_RangeVar => {
                let rv = node.as_range_var().expect("tag checked");
                if rv.schemaname.is_none() {
                    let relname = rv.relname.expect("grammar always sets relname");
                    if name_captured(&self.innerwiths, relname) {
                        return Ok(false);
                    }
                    if relname == self.myname {
                        if self.context != RecursionContext::Allowed {
                            return Err(invalid_recursion(
                                self.pstate,
                                recursion_errormsg(self.context, self.myname),
                                rv.location,
                            ));
                        }
                        self.selfrefcount += 1;
                        if self.selfrefcount > 1 {
                            return Err(invalid_recursion(
                                self.pstate,
                                format!(
                                    "recursive reference to query \"{}\" must not appear \
                                     more than once",
                                    self.myname
                                ),
                                rv.location,
                            ));
                        }
                    }
                }
                Ok(false)
            }
            NodeTag::T_SelectStmt => {
                self.visit_select_stmt_ref(node.as_select_stmt().expect("tag checked"))
            }
            NodeTag::T_WithClause => Ok(false),
            NodeTag::T_JoinExpr => {
                let j = node.as_join_expr().expect("tag checked");
                match j.jointype {
                    JoinType::JOIN_INNER => {
                        self.visit(j.larg)?;
                        self.visit(j.rarg)?;
                        nodes_core::walk_opt(j.quals, self)?;
                    }
                    JoinType::JOIN_LEFT => {
                        self.visit(j.larg)?;
                        if save_context == RecursionContext::Allowed {
                            self.context = RecursionContext::OuterJoin;
                        }
                        self.visit(j.rarg)?;
                        self.context = save_context;
                        nodes_core::walk_opt(j.quals, self)?;
                    }
                    JoinType::JOIN_FULL => {
                        if save_context == RecursionContext::Allowed {
                            self.context = RecursionContext::OuterJoin;
                        }
                        self.visit(j.larg)?;
                        self.visit(j.rarg)?;
                        self.context = save_context;
                        nodes_core::walk_opt(j.quals, self)?;
                    }
                    JoinType::JOIN_RIGHT => {
                        if save_context == RecursionContext::Allowed {
                            self.context = RecursionContext::OuterJoin;
                        }
                        self.visit(j.larg)?;
                        self.context = save_context;
                        self.visit(j.rarg)?;
                        nodes_core::walk_opt(j.quals, self)?;
                    }
                    other => {
                        return Err(elog_error(&format!(
                            "unrecognized join type: {}",
                            other as i32
                        )))
                    }
                }
                Ok(false)
            }
            NodeTag::T_SubLink => {
                let sl = node.as_sub_link().expect("tag checked");
                // C: the outer context is overridden — a subquery is
                // independent.
                self.context = RecursionContext::Sublink;
                self.visit(sl.subselect)?;
                self.context = save_context;
                nodes_core::walk_opt(sl.testexpr, self)?;
                Ok(false)
            }
            _ => walk_raw(node, self),
        }
    }

    fn visit_select_stmt_ref(&mut self, s: &'mcx SelectStmt<'mcx>) -> PgResult<bool> {
        match s.withClause.and_then(|n| n.as_with_clause()) {
            Some(wc) => {
                if wc.recursive {
                    self.innerwiths.push(wc.ctes.clone_in(self.mcx)?);
                    for cte_node in &wc.ctes {
                        let q = cte_node
                            .as_common_table_expr()
                            .expect("WITH list cell")
                            .ctequery
                            .expect("CTE has no query");
                        self.visit(q)?;
                    }
                    self.check_select_stmt(s)?;
                    self.innerwiths.pop();
                } else {
                    self.innerwiths.push(NodeList::nil());
                    for cte_node in &wc.ctes {
                        let q = cte_node
                            .as_common_table_expr()
                            .expect("WITH list cell")
                            .ctequery
                            .expect("CTE has no query");
                        self.visit(q)?;
                        let mcx = self.mcx;
                        let top = self.innerwiths.len() - 1;
                        self.innerwiths[top].lappend(mcx, cte_node)?;
                    }
                    self.check_select_stmt(s)?;
                    self.innerwiths.pop();
                }
            }
            None => self.check_select_stmt(s)?,
        }
        Ok(false)
    }
}

// C exprLocation's raw SortBy arm and its -1 default for LockingClause;
// analyzed-expr tags go through the shared port.
fn raw_expr_location(node: Node<'_>) -> ParseLoc {
    match node.node_tag() {
        NodeTag::T_SortBy => {
            node.as_sort_by().expect("tag checked").node.map_or(-1, raw_expr_location)
        }
        NodeTag::T_LockingClause => -1,
        _ => parse_expr::expr_location(node),
    }
}

fn raw_list_location(list: &NodeList<'_>) -> ParseLoc {
    for n in list {
        let loc = raw_expr_location(n);
        if loc >= 0 {
            return loc;
        }
    }
    -1
}

#[cold]
#[inline(never)]
fn duplicate_cte_name(pstate: &ParseState<'_, '_>, name: &str, location: i32) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_ALIAS)
            .errmsg(format!("WITH query name \"{name}\" specified more than once"))
            .errposition(parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_cte.c", 0, "transformWithClause")),
    )
}

#[cold]
#[inline(never)]
fn invalid_recursion(
    pstate: &ParseState<'_, '_>,
    message: String,
    location: ParseLoc,
) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_INVALID_RECURSION)
            .errmsg(message)
            .errposition(parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_cte.c", 0, "checkWellFormedRecursion")),
    )
}

#[cold]
#[inline(never)]
fn mutual_recursion(pstate: &ParseState<'_, '_>, location: ParseLoc) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("mutual recursion between WITH items is not implemented".to_string())
            .errposition(parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_cte.c", 0, "TopologicalSort")),
    )
}

#[cold]
#[inline(never)]
fn recursive_decoration(
    pstate: &ParseState<'_, '_>,
    message: &str,
    location: ParseLoc,
) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(message.to_string())
            .errposition(parser_errposition(
                pstate,
                location,
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_cte.c", 0, "checkWellFormedRecursion")),
    )
}

#[cold]
#[inline(never)]
fn recursive_type_mismatch(
    pstate: &ParseState<'_, '_>,
    ctename: &str,
    varattno: i32,
    expected: &str,
    actual: &str,
    texpr: Node<'_>,
) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg(format!(
                "recursive query \"{ctename}\" column {varattno} has type {expected} in \
                 non-recursive term but type {actual} overall"
            ))
            .errhint("Cast the output of the non-recursive term to the correct type.")
            .errposition(parser_errposition(
                pstate,
                parse_expr::expr_location(texpr),
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_cte.c", 0, "analyzeCTE")),
    )
}

#[cold]
#[inline(never)]
fn recursive_collation_mismatch(
    pstate: &ParseState<'_, '_>,
    ctename: &str,
    varattno: i32,
    expected: &str,
    actual: &str,
    texpr: Node<'_>,
) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_COLLATION_MISMATCH)
            .errmsg(format!(
                "recursive query \"{ctename}\" column {varattno} has collation \"{expected}\" \
                 in non-recursive term but collation \"{actual}\" overall"
            ))
            .errhint("Use the COLLATE clause to set the collation of the non-recursive term.")
            .errposition(parser_errposition(
                pstate,
                parse_expr::expr_location(texpr),
                mbutils::GetDatabaseEncoding(),
            ))
            .into_error()
            .with_error_location(ErrorLocation::new("parse_cte.c", 0, "analyzeCTE")),
    )
}

// C errmsg("%s", get_collation_name(oid)) prints glibc's "(null)" for NULL.
fn collation_name_or_null(mcx: Mcx<'_>, colloid: Oid) -> PgResult<String> {
    Ok(lsyscache::misc::get_collation_name(mcx, colloid)?
        .map(|s| s.as_str().to_string())
        .unwrap_or_else(|| "(null)".to_string()))
}

#[cold]
#[inline(never)]
fn elog_error(msg: &str) -> Box<PgError> {
    Box::new(PgError::error(msg.to_string()))
}
