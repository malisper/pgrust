// inline_set_returning_function's parser-dependent middle (clauses.c:5178+)
// plus substitute_actual_srf_parameters. The gate ladder runs in
// clauses::srf_inline; this seam body fetches and parses/rewrites the body,
// validates the result shape, and substitutes the actual parameters.

use mcx::{Mcx, PgVec};
use types_error::{PgError, PgResult};
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{Query, RTEKind, RangeTblEntry, RangeTblFunction, WindowClause};
use types_nodes::primnodes::{FromExpr, FuncExpr, ParamKind, SubLink};
use types_nodes::{Node, NodeList, NodeTag};
use types_portal::QueryEnvHandle;

pub fn init_seams() {
    clauses_seams::inline_set_returning_sql_body::set(inline_set_returning_sql_body);
}

fn inline_set_returning_sql_body<'mcx>(
    mcx: Mcx<'mcx>,
    rte_node: Node<'mcx>,
    prokind: i8,
) -> PgResult<Option<&'mcx Query<'mcx>>> {
    let rte = rte_node.as_range_tbl_entry().expect("RTE_FUNCTION RangeTblEntry");
    let rtfunc = rte.functions.nth(0).as_range_tbl_function().expect("functions cell");
    let fexpr_node = rtfunc.funcexpr.expect("gate-checked FuncExpr");
    let fexpr = fexpr_node.as_func_expr().expect("gate-checked FuncExpr");

    let row = crate::inline_fn::read_inline_proc_row(mcx, fexpr.funcid)?;
    // C installs sql_inline_error_callback across the parse/validate region.
    inline_srf_body(mcx, &row, prokind, fexpr, fexpr_node, rtfunc).map_err(|e| {
        crate::inline_fn::sql_inline_error_callback(e, row.proname.as_str(), row.prosrc.as_str())
    })
}

fn inline_srf_body<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    row: &crate::inline_fn::InlineProcRow<'mcx>,
    prokind: i8,
    fexpr: &'a FuncExpr<'mcx>,
    fexpr_node: Node<'mcx>,
    rtfunc: &'a RangeTblFunction<'mcx>,
) -> PgResult<Option<&'mcx Query<'mcx>>> {
    let mut query_list: PgVec<'mcx, Query<'mcx>>;
    if let Some(body) = row.prosqlbody.as_ref() {
        let qs = crate::cache::sqlbody_queries(mcx, body.as_str())?;
        if qs.len() != 1 {
            return Ok(None);
        }
        let q = qs.into_iter().next().expect("length checked");
        if q.commandType == CmdType::CMD_UTILITY {
            query_list = mcx::vec_with_capacity_in(mcx, 1)?;
            query_list.push(q);
        } else {
            rewrite_handler_seams::acquire_rewrite_locks::call(mcx, &q, true, false)?;
            query_list = rewrite_handler_seams::query_rewrite::call(mcx, q)?;
        }
        if query_list.len() != 1 {
            return Ok(None);
        }
    } else {
        let raw_list = parser_seams::raw_parser::call(
            mcx,
            row.prosrc.as_str(),
            parser_seams::RawParseMode::RAW_PARSE_DEFAULT,
        )?;
        if raw_list.len() != 1 {
            return Ok(None);
        }
        let argtypes =
            crate::inline_fn::resolve_polymorphic_argtypes(mcx, &row.argtypes, &fexpr.args)?;
        let mut name_refs: PgVec<'mcx, &str> =
            mcx::vec_with_capacity_in(mcx, row.argnames.len())?;
        for n in row.argnames.iter() {
            name_refs.push(n.as_str());
        }
        let query = analyze_seams::parse_analyze_sql_fn::call(
            mcx,
            &raw_list[0],
            row.prosrc.as_str(),
            row.proname.as_str(),
            &argtypes,
            &name_refs,
            fexpr.inputcollid,
            QueryEnvHandle::NULL,
        )?;
        // C pg_analyze_and_rewrite_withcb: unlike inline_function, rewriting
        // cannot be skipped here.
        if query.commandType == CmdType::CMD_UTILITY {
            query_list = mcx::vec_with_capacity_in(mcx, 1)?;
            query_list.push(query);
        } else {
            query_list = rewrite_handler_seams::query_rewrite::call(mcx, query)?;
        }
        if query_list.len() != 1 {
            return Ok(None);
        }
    }

    // Resolve the actual function result tupdesc, if composite: a coldeflist
    // wins; otherwise get_expr_result_type (matches ExecInitFunctionScan).
    let (functypclass, rettupdesc) = if !rtfunc.funccolnames.is_nil() {
        let n = rtfunc.funccolnames.len();
        let mut d = tupdesc::CreateTemplateTupleDesc(mcx, n as i32)?;
        for i in 0..n {
            let attno = (i + 1) as i16;
            let name = rtfunc
                .funccolnames
                .nth(i)
                .as_string()
                .expect("funccolnames cell is String")
                .sval;
            tupdesc::TupleDescInitEntry(
                &mut d,
                attno,
                Some(name),
                rtfunc.funccoltypes.nth(i),
                rtfunc.funccoltypmods.nth(i),
                0,
            )?;
            tupdesc::TupleDescInitEntryCollation(&mut d, attno, rtfunc.funccolcollations.nth(i));
        }
        (funcapi::TypeFuncClass::Record, Some(d))
    } else {
        let resolved = funcapi::get_expr_result_type(mcx, Some(fexpr_node))?;
        (resolved.class, resolved.result_tuple_desc)
    };

    if query_list[0].commandType != CmdType::CMD_SELECT {
        return Ok(None);
    }

    // check_sql_fn_retval coerces the tlist to the declared type (erroring on
    // mismatch, as C: the function would fail at runtime anyway) and inserts
    // dummy NULLs for dropped columns; a composite declared type must come
    // back as a whole-tuple result or inlining declines.
    let is_tuple_result = crate::retval::check_sql_stmt_retval(
        mcx,
        &mut query_list,
        fexpr.funcresulttype,
        rettupdesc.as_ref(),
        prokind,
        true,
    )?;
    if !is_tuple_result
        && matches!(
            functypclass,
            funcapi::TypeFuncClass::Composite
                | funcapi::TypeFuncClass::CompositeDomain
                | funcapi::TypeFuncClass::Record
        )
    {
        return Ok(None);
    }

    // check_sql_fn_retval might have injected a projection; use the upper
    // Query either way.
    let mut querytree = query_list.pop().expect("one query in, one out");
    substitute_actual_srf_parameters(mcx, &mut querytree, fexpr.args.len(), &fexpr.args)?;

    Ok(Some(mcx::leak_in(mcx::alloc_in(mcx, querytree)?)))
}

// substitute_actual_srf_parameters (clauses.c:5360): PARAM_EXTERN Params
// become copies of the actual arguments, var levels bumped by the query
// nesting depth (the body starts one level down, as the new subquery RTE).
fn substitute_actual_srf_parameters<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    q: &mut Query<'mcx>,
    nargs: usize,
    args: &'a NodeList<'mcx>,
) -> PgResult<()> {
    let mut ctx = SrfSubst { mcx, args, nargs, sublevels_up: 1 };
    ctx.query_fields(q)
}

struct SrfSubst<'a, 'mcx> {
    mcx: Mcx<'mcx>,
    args: &'a NodeList<'mcx>,
    nargs: usize,
    sublevels_up: i32,
}

impl<'a, 'mcx> SrfSubst<'a, 'mcx> {
    // substitute_actual_srf_parameters_mutator. None = unchanged.
    fn mutate(&mut self, node: Node<'mcx>) -> PgResult<Option<Node<'mcx>>> {
        match node.node_tag() {
            NodeTag::T_Query => {
                let q = node.as_query().expect("tag-checked");
                let mut qc = crate::clone_query(q);
                self.sublevels_up += 1;
                self.query_fields(&mut qc)?;
                self.sublevels_up -= 1;
                Ok(Some(Node::mk(self.mcx, qc)?))
            }
            NodeTag::T_Param => {
                let p = node.as_param().expect("tag-checked");
                if p.paramkind == ParamKind::PARAM_EXTERN {
                    if p.paramid <= 0 || p.paramid as usize > self.nargs {
                        return Err(
                            PgError::error(format!("invalid paramid: {}", p.paramid)).into()
                        );
                    }
                    let copied =
                        rewrite_manip::copy_node(self.mcx, self.args.nth((p.paramid - 1) as usize))?;
                    rewrite_manip::IncrementVarSublevelsUp(copied, self.sublevels_up, 0)?;
                    return Ok(Some(copied));
                }
                Ok(None)
            }
            // expression_tree_mutator's SubLink arm does not visit the
            // subselect; C's does, and the Query hop carries the level bump.
            NodeTag::T_SubLink => {
                let sl = node.as_sub_link().expect("tag-checked");
                let new_test = match sl.testexpr {
                    Some(t) => self.mutate(t)?,
                    None => None,
                };
                let new_sub = self.mutate(sl.subselect)?;
                if new_test.is_none() && new_sub.is_none() {
                    return Ok(None);
                }
                Ok(Some(Node::mk(
                    self.mcx,
                    SubLink {
                        subLinkType: sl.subLinkType,
                        subLinkId: sl.subLinkId,
                        testexpr: new_test.or(sl.testexpr),
                        operName: sl.operName.clone_in(self.mcx)?,
                        subselect: new_sub.unwrap_or(sl.subselect),
                        location: sl.location,
                    },
                )?))
            }
            _ => nodes_core::expression_tree_mutator(self.mcx, node, &mut |n| self.mutate(n)),
        }
    }

    fn mutate_opt(&mut self, n: Option<Node<'mcx>>) -> PgResult<Option<Node<'mcx>>> {
        match n {
            Some(x) => self.mutate(x),
            None => Ok(None),
        }
    }

    fn mutate_list(&mut self, list: &NodeList<'mcx>) -> PgResult<Option<NodeList<'mcx>>> {
        let mut changed = false;
        let mut out = NodeList::nil();
        for n in list {
            let m = self.mutate(n)?;
            changed |= m.is_some();
            out.lappend(self.mcx, m.unwrap_or(n))?;
        }
        Ok(if changed { Some(out) } else { None })
    }

    // query_tree_mutator's field set (nodeFuncs.c), applied in place; this
    // tree is exclusively owned (fresh body parse), so RTE cells mutate via
    // with_mut.
    fn query_fields(&mut self, q: &mut Query<'mcx>) -> PgResult<()> {
        let mcx = self.mcx;
        if let Some(l) = self.mutate_list(&q.targetList)? {
            q.targetList = l;
        }
        if let Some(l) = self.mutate_list(&q.withCheckOptions)? {
            q.withCheckOptions = l;
        }
        if let Some(n) = self.mutate_opt(q.onConflict)? {
            q.onConflict = Some(n);
        }
        if let Some(l) = self.mutate_list(&q.mergeActionList)? {
            q.mergeActionList = l;
        }
        if let Some(n) = self.mutate_opt(q.mergeJoinCondition)? {
            q.mergeJoinCondition = Some(n);
        }
        if let Some(l) = self.mutate_list(&q.returningList)? {
            q.returningList = l;
        }
        if let Some(jt) = q.jointree {
            let fl = self.mutate_list(&jt.fromlist)?;
            let quals = self.mutate_opt(jt.quals)?;
            if fl.is_some() || quals.is_some() {
                q.jointree = Some(mcx::alloc_leak_in(
                    mcx,
                    FromExpr {
                        fromlist: match fl {
                            Some(l) => l,
                            None => jt.fromlist.clone_in(mcx)?,
                        },
                        quals: quals.or(jt.quals),
                    },
                )?);
            }
        }
        if let Some(n) = self.mutate_opt(q.setOperations)? {
            q.setOperations = Some(n);
        }
        if let Some(n) = self.mutate_opt(q.havingQual)? {
            q.havingQual = Some(n);
        }
        if let Some(n) = self.mutate_opt(q.limitOffset)? {
            q.limitOffset = Some(n);
        }
        if let Some(n) = self.mutate_opt(q.limitCount)? {
            q.limitCount = Some(n);
        }
        for wc_node in &q.windowClause {
            let wc = wc_node.as_window_clause().expect("windowClause cell");
            let start = self.mutate_opt(wc.startOffset)?;
            let end = self.mutate_opt(wc.endOffset)?;
            if start.is_some() || end.is_some() {
                // SAFETY: exclusive tree (module contract above).
                unsafe {
                    wc_node.with_mut::<WindowClause, _>(|w| {
                        if start.is_some() {
                            w.startOffset = start;
                        }
                        if end.is_some() {
                            w.endOffset = end;
                        }
                    })
                };
            }
        }
        for cte_node in &q.cteList {
            let cte = cte_node.as_common_table_expr().expect("cteList cell");
            let ctequery = cte.ctequery.expect("analyzed CTE");
            if let Some(new_q) = self.mutate(ctequery)? {
                // SAFETY: exclusive tree (module contract above).
                unsafe {
                    cte_node.with_mut::<types_nodes::parsenodes::CommonTableExpr, _>(|c| {
                        c.ctequery = Some(new_q)
                    })
                };
            }
        }
        for rte_node in &q.rtable {
            self.range_table_entry(rte_node)?;
        }
        Ok(())
    }

    // range_table_mutator (nodeFuncs.c), per-kind fields. All reads off the
    // shared RTE borrow happen before the single with_mut write.
    fn range_table_entry(&mut self, rte_node: Node<'mcx>) -> PgResult<()> {
        let mcx = self.mcx;
        let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
        let mut new_tablesample = None;
        let mut new_subquery: Option<&'mcx Query<'mcx>> = None;
        let mut new_joinaliasvars = None;
        let mut new_functions = None;
        let mut new_tablefunc = None;
        let mut new_values_lists = None;
        let mut new_groupexprs = None;
        match rte.rtekind {
            RTEKind::RTE_RELATION => new_tablesample = self.mutate_opt(rte.tablesample)?,
            RTEKind::RTE_SUBQUERY => {
                let sub = rte.subquery.expect("subquery RTE has a subquery");
                let mut qc = crate::clone_query(sub);
                self.sublevels_up += 1;
                self.query_fields(&mut qc)?;
                self.sublevels_up -= 1;
                new_subquery = Some(mcx::leak_in(mcx::alloc_in(mcx, qc)?));
            }
            RTEKind::RTE_JOIN => new_joinaliasvars = self.mutate_list(&rte.joinaliasvars)?,
            RTEKind::RTE_FUNCTION => new_functions = self.mutate_list(&rte.functions)?,
            RTEKind::RTE_TABLEFUNC => new_tablefunc = self.mutate_opt(rte.tablefunc)?,
            RTEKind::RTE_VALUES => new_values_lists = self.mutate_list(&rte.values_lists)?,
            RTEKind::RTE_GROUP => new_groupexprs = self.mutate_list(&rte.groupexprs)?,
            RTEKind::RTE_CTE | RTEKind::RTE_NAMEDTUPLESTORE | RTEKind::RTE_RESULT => {}
        }
        let new_secquals = self.mutate_list(&rte.securityQuals)?;
        if new_tablesample.is_some()
            || new_subquery.is_some()
            || new_joinaliasvars.is_some()
            || new_functions.is_some()
            || new_tablefunc.is_some()
            || new_values_lists.is_some()
            || new_groupexprs.is_some()
            || new_secquals.is_some()
        {
            // SAFETY: exclusive tree (module contract above); the shared rte
            // borrow is not read past this write.
            unsafe {
                rte_node.with_mut::<RangeTblEntry, _>(|r| {
                    if new_tablesample.is_some() {
                        r.tablesample = new_tablesample;
                    }
                    if new_subquery.is_some() {
                        r.subquery = new_subquery;
                    }
                    if let Some(l) = new_joinaliasvars {
                        r.joinaliasvars = l;
                    }
                    if let Some(l) = new_functions {
                        r.functions = l;
                    }
                    if new_tablefunc.is_some() {
                        r.tablefunc = new_tablefunc;
                    }
                    if let Some(l) = new_values_lists {
                        r.values_lists = l;
                    }
                    if let Some(l) = new_groupexprs {
                        r.groupexprs = l;
                    }
                    if let Some(l) = new_secquals {
                        r.securityQuals = l;
                    }
                })
            };
        }
        Ok(())
    }
}
