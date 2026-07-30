// parse_cte.c: WITH and WITH RECURSIVE analysis, SEARCH/CYCLE included;
// data-modifying CTEs are loud panics naming their lane.
#![allow(non_snake_case)]

use mcx::{Mcx, PgVec};
use types_core::catalog::{DEFAULT_COLLATION_OID, TEXTOID, UNKNOWNOID};
use types_core::{InvalidOid, Oid, ParseLoc};
use types_error::{
    ERRCODE_COLLATION_MISMATCH, ERRCODE_DATATYPE_MISMATCH, ERRCODE_DUPLICATE_ALIAS,
    ERRCODE_DUPLICATE_COLUMN, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_COLUMN_REFERENCE,
    ERRCODE_INVALID_RECURSION, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_FUNCTION, ErrorLocation,
    PgError, PgResult, ERROR,
};
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{CommonTableExpr, SetOperation, WithClause};
use types_nodes::primnodes::TargetEntry;
use types_nodes::rawnodes::SelectStmt;
use types_nodes::{Bitmapset, JoinType, Node, NodeList, NodeTag};

use nodes_core::NodeWalker;
use parser_small1::{parser_errposition, ParseExprKind, ParseState};

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
            let qtag = cte.ctequery.expect("CTE has no query").node_tag();
            if qtag != NodeTag::T_SelectStmt {
                debug_assert!(matches!(
                    qtag,
                    NodeTag::T_InsertStmt
                        | NodeTag::T_UpdateStmt
                        | NodeTag::T_DeleteStmt
                        | NodeTag::T_MergeStmt
                ));
                pstate.p_hasModifyingCTE = true;
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
    let (ctequery, location, cterecursive, ctename, search_clause, cycle_clause) = {
        let cte = cte_node.as_common_table_expr().expect("WITH list cell");
        (
            cte.ctequery.expect("CTE has no query"),
            cte.location,
            cte.cterecursive,
            cte.ctename.unwrap_or(""),
            cte.search_clause,
            cte.cycle_clause,
        )
    };
    debug_assert!(ctequery.node_tag() != NodeTag::T_Query);

    // The cycle mark type is resolved before analyzing the query, which can
    // refer to the mark column.
    if let Some(cc_node) = cycle_clause {
        transformCycleMark(mcx, pstate, cc_node)?;
    }

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
                .with_error_location(ErrorLocation::new(file!(), line!() as i32, "analyzeCTE")),
        ));
    }

    query.canSetTag = false;
    let query_node = Node::mk(mcx, query)?;
    // SAFETY: parser-owned tree under analysis; no live derived refs.
    unsafe { cte_node.with_mut::<CommonTableExpr, _>(|c| c.ctequery = Some(query_node)) };

    let q = query_node.as_query().expect("just built");

    if !cterecursive {
        // GetCTETargetList: a DML CTE's output columns come from RETURNING.
        let tlist = if q.commandType == CmdType::CMD_SELECT {
            &q.targetList
        } else {
            &q.returningList
        };
        analyzeCTETargetList(mcx, pstate, cte_node, tlist)?;
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
    }

    if search_clause.is_some() || cycle_clause.is_some() {
        if !cterecursive {
            return Err(syntax_err(pstate, "WITH query is not recursive".to_string(), location));
        }
        let sos = query_node
            .as_query()
            .expect("analyzed CTE query")
            .setOperations
            .expect("recursive CTE query has setOperations")
            .as_set_operation_stmt()
            .expect("recursive CTE top is a set operation");
        if sos.larg.expect("setop larg").node_tag() != NodeTag::T_RangeTblRef {
            return Err(Box::new(
                elog::ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg(
                        "with a SEARCH or CYCLE clause, the left side of the UNION must be a SELECT"
                            .to_string(),
                    )
                    .into_error()
                    .with_error_location(ErrorLocation::new(file!(), line!() as i32, "analyzeCTE")),
            ));
        }
        if sos.rarg.expect("setop rarg").node_tag() != NodeTag::T_RangeTblRef {
            return Err(Box::new(
                elog::ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(
                        "with a SEARCH or CYCLE clause, the right side of the UNION must be a SELECT"
                            .to_string(),
                    )
                    .into_error()
                    .with_error_location(ErrorLocation::new(file!(), line!() as i32, "analyzeCTE")),
            ));
        }
    }

    let ctecolnames = &cte_node.as_common_table_expr().expect("WITH list cell").ctecolnames;

    if let Some(sc) = search_clause.map(|n| n.as_cte_search_clause().expect("search clause")) {
        checkColumnList(
            pstate,
            ctecolnames,
            &sc.search_col_list,
            "search column \"{}\" not in WITH query column list",
            "search column \"{}\" specified more than once",
            sc.location,
        )?;
        let seq_col = sc.search_seq_column.expect("SEARCH SET column");
        if colname_member(ctecolnames, seq_col) {
            return Err(syntax_err(
                pstate,
                format!(
                    "search sequence column name \"{seq_col}\" already used in \
                     WITH query column list"
                ),
                sc.location,
            ));
        }
    }

    if let Some(cc) = cycle_clause.map(|n| n.as_cte_cycle_clause().expect("cycle clause")) {
        checkColumnList(
            pstate,
            ctecolnames,
            &cc.cycle_col_list,
            "cycle column \"{}\" not in WITH query column list",
            "cycle column \"{}\" specified more than once",
            cc.location,
        )?;
        let mark_col = cc.cycle_mark_column.expect("CYCLE SET column");
        let path_col = cc.cycle_path_column.expect("CYCLE USING column");
        if colname_member(ctecolnames, mark_col) {
            return Err(syntax_err(
                pstate,
                format!(
                    "cycle mark column name \"{mark_col}\" already used in \
                     WITH query column list"
                ),
                cc.location,
            ));
        }
        if colname_member(ctecolnames, path_col) {
            return Err(syntax_err(
                pstate,
                format!(
                    "cycle path column name \"{path_col}\" already used in \
                     WITH query column list"
                ),
                cc.location,
            ));
        }
        if mark_col == path_col {
            return Err(syntax_err(
                pstate,
                "cycle mark column name and cycle path column name are the same".to_string(),
                cc.location,
            ));
        }
    }

    if let (Some(sc), Some(cc)) = (
        search_clause.map(|n| n.as_cte_search_clause().expect("search clause")),
        cycle_clause.map(|n| n.as_cte_cycle_clause().expect("cycle clause")),
    ) {
        let seq_col = sc.search_seq_column.expect("SEARCH SET column");
        if seq_col == cc.cycle_mark_column.expect("CYCLE SET column") {
            return Err(syntax_err(
                pstate,
                "search sequence column name and cycle mark column name are the same".to_string(),
                sc.location,
            ));
        }
        if seq_col == cc.cycle_path_column.expect("CYCLE USING column") {
            return Err(syntax_err(
                pstate,
                "search sequence column name and cycle path column name are the same".to_string(),
                sc.location,
            ));
        }
    }

    Ok(())
}

fn transformCycleMark<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: &mut ParseState<'_, 'mcx>,
    cc_node: Node<'mcx>,
) -> PgResult<()> {
    let (raw_value, raw_default) = {
        let cc = cc_node.as_cte_cycle_clause().expect("cycle clause");
        (
            cc.cycle_mark_value.expect("cycle_mark_value"),
            cc.cycle_mark_default.expect("cycle_mark_default"),
        )
    };
    let value =
        parse_expr::transformExpr(mcx, pstate, raw_value, ParseExprKind::EXPR_KIND_CYCLE_MARK)?;
    let default =
        parse_expr::transformExpr(mcx, pstate, raw_default, ParseExprKind::EXPR_KIND_CYCLE_MARK)?;

    let (vt, vloc) = (parse_expr::expr_type(value), nodes_core::expr_location(value));
    let (dt, dloc) = (parse_expr::expr_type(default), nodes_core::expr_location(default));
    let mark_type =
        coerce::select_common_type(pstate, &[(vt, vloc), (dt, dloc)], Some("CYCLE"))?;
    let value =
        coerce::coerce_to_common_type(mcx, pstate, value, vt, vloc, mark_type, "CYCLE/SET/TO")?;
    let default = coerce::coerce_to_common_type(
        mcx,
        pstate,
        default,
        dt,
        dloc,
        mark_type,
        "CYCLE/SET/DEFAULT",
    )?;

    let mark_typmod = coerce::select_common_typmod(
        &[
            (parse_expr::expr_type(value), parse_expr::expr_typmod(value)),
            (parse_expr::expr_type(default), parse_expr::expr_typmod(default)),
        ],
        mark_type,
    );
    let both = NodeList::make2(mcx, value, default)?;
    let mark_collation = parse_collate::select_common_collation(mcx, pstate, &both, true)?;

    let typentry = typcache::lookup_type_cache(mark_type, typcache::TYPECACHE_EQ_OPR)?;
    if !types_core::OidIsValid(typentry.eq_opr()) {
        return Err(operator_lookup_err(mark_type, "equality")?);
    }
    let neop = lsyscache::get_negator(typentry.eq_opr())?;
    if !types_core::OidIsValid(neop) {
        return Err(operator_lookup_err(mark_type, "inequality")?);
    }

    // SAFETY: parser-owned tree under analysis; no live derived refs.
    unsafe {
        cc_node.with_mut::<types_nodes::parsenodes::CTECycleClause, _>(|c| {
            c.cycle_mark_value = Some(value);
            c.cycle_mark_default = Some(default);
            c.cycle_mark_type = mark_type;
            c.cycle_mark_typmod = mark_typmod;
            c.cycle_mark_collation = mark_collation;
            c.cycle_mark_neop = neop;
        })
    };
    Ok(())
}

fn checkColumnList<'mcx>(
    pstate: &ParseState<'_, 'mcx>,
    ctecolnames: &NodeList<'mcx>,
    col_list: &NodeList<'mcx>,
    missing_fmt: &str,
    dup_fmt: &str,
    location: ParseLoc,
) -> PgResult<()> {
    for (i, col) in col_list.iter().enumerate() {
        let colname = col.as_string().expect("column list cell").sval;
        if !colname_member(ctecolnames, colname) {
            return Err(syntax_err(
                pstate,
                missing_fmt.replacen("{}", colname, 1),
                location,
            ));
        }
        for earlier in col_list.iter().take(i) {
            if earlier.as_string().expect("column list cell").sval == colname {
                return Err(Box::new(
                    elog::ereport(ERROR)
                        .errcode(ERRCODE_DUPLICATE_COLUMN)
                        .errmsg(dup_fmt.replacen("{}", colname, 1))
                        .errposition(parser_errposition(
                            pstate,
                            location,
                            mbutils::GetDatabaseEncoding(),
                        ))
                        .into_error()
                        .with_error_location(ErrorLocation::new(file!(), line!() as i32, "analyzeCTE")),
                ));
            }
        }
    }
    Ok(())
}

fn colname_member(colnames: &NodeList<'_>, name: &str) -> bool {
    colnames.iter().any(|n| n.as_string().is_some_and(|s| s.sval == name))
}

#[track_caller]
#[cold]
#[inline(never)]
fn syntax_err(pstate: &ParseState<'_, '_>, msg: String, location: ParseLoc) -> Box<PgError> {
    Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg(msg)
            .errposition(parser_errposition(pstate, location, mbutils::GetDatabaseEncoding()))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "analyzeCTE")),
    )
}

#[cold]
#[inline(never)]
fn operator_lookup_err(mark_type: Oid, which: &str) -> PgResult<Box<PgError>> {
    Ok(Box::new(
        elog::ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_FUNCTION)
            .errmsg(format!(
                "could not identify an {which} operator for type {}",
                format_type::format_type_be(mark_type)?
            ))
            .into_error()
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "analyzeCTE")),
    ))
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
                .with_error_location(ErrorLocation::new(file!(), line!() as i32, "analyzeCTETargetList")),
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
        wc: &'mcx WithClause<'mcx>,
        walk_body: impl FnOnce(&mut Self) -> PgResult<()>,
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
            walk_body(self)?;
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
            walk_body(self)?;
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
            NodeTag::T_InsertStmt
            | NodeTag::T_UpdateStmt
            | NodeTag::T_DeleteStmt
            | NodeTag::T_MergeStmt => match raw_stmt_with_clause(node) {
                Some(wc) => {
                    self.walk_inner_with(wc, |w| {
                        nodes_core::raw_expression_tree_walker(node, w).map(|_| ())
                    })?;
                    Ok(false)
                }
                None => nodes_core::raw_expression_tree_walker(node, self),
            },
            NodeTag::T_WithClause => Ok(false),
            _ => nodes_core::raw_expression_tree_walker(node, self),
        }
    }

    fn visit_select_stmt_ref(&mut self, s: &'mcx SelectStmt<'mcx>) -> PgResult<bool> {
        match s.withClause.and_then(|n| n.as_with_clause()) {
            Some(wc) => {
                self.walk_inner_with(wc, |w| nodes_core::walk_select_stmt(s, w).map(|_| ()))?;
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

fn raw_stmt_with_clause<'mcx>(node: Node<'mcx>) -> Option<&'mcx WithClause<'mcx>> {
    let wc = match node.node_tag() {
        NodeTag::T_InsertStmt => node.as_insert_stmt().unwrap().withClause,
        NodeTag::T_UpdateStmt => node.as_update_stmt().unwrap().withClause,
        NodeTag::T_DeleteStmt => node.as_delete_stmt().unwrap().withClause,
        NodeTag::T_MergeStmt => node.as_merge_stmt().unwrap().withClause,
        _ => None,
    };
    wc.and_then(|n| n.as_with_clause())
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
            _ => nodes_core::raw_expression_tree_walker(node, self),
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

#[track_caller]
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
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "transformWithClause")),
    )
}

#[track_caller]
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
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "checkWellFormedRecursion")),
    )
}

#[track_caller]
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
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "TopologicalSort")),
    )
}

#[track_caller]
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
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "checkWellFormedRecursion")),
    )
}

#[track_caller]
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
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "analyzeCTE")),
    )
}

#[track_caller]
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
            .with_error_location(ErrorLocation::new(file!(), line!() as i32, "analyzeCTE")),
    )
}

// C errmsg("%s", get_collation_name(oid)) prints glibc's "(null)" for NULL.
fn collation_name_or_null(mcx: Mcx<'_>, colloid: Oid) -> PgResult<String> {
    Ok(lsyscache::misc::get_collation_name(mcx, colloid)?
        .map(|s| s.as_str().to_string())
        .unwrap_or_else(|| "(null)".to_string()))
}

#[track_caller]
#[cold]
#[inline(never)]
fn elog_error(msg: &str) -> Box<PgError> {
    Box::new(PgError::error(msg.to_string()))
}
