use nodes_core::{
    deferred, expression_tree_walker, query_or_expression_tree_walker, query_tree_walker,
    NodeWalker,
};
use mcx::Mcx;
use types_error::{PgError, PgResult};
use types_nodes::parsenodes::Query;
use types_nodes::primnodes::VarReturningType;
use types_nodes::{Bitmapset, Node, NodeList, NodeTag};
use types_tuple::htup::FirstLowInvalidHeapAttributeNumber;

pub const PVC_INCLUDE_AGGREGATES: u32 = 0x0001;
pub const PVC_RECURSE_AGGREGATES: u32 = 0x0002;
pub const PVC_INCLUDE_WINDOWFUNCS: u32 = 0x0004;
pub const PVC_RECURSE_WINDOWFUNCS: u32 = 0x0008;
pub const PVC_INCLUDE_PLACEHOLDERS: u32 = 0x0010;
pub const PVC_RECURSE_PLACEHOLDERS: u32 = 0x0020;

struct PullVarnos<'mcx> {
    mcx: Mcx<'mcx>,
    varnos: Bitmapset<'mcx>,
    sublevels_up: i64,
}

impl<'mcx> NodeWalker<'mcx> for PullVarnos<'mcx> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        match node.node_tag() {
            NodeTag::T_Var => {
                let v = node.as_var().unwrap();
                if v.varlevelsup as i64 == self.sublevels_up {
                    self.varnos.add_member(self.mcx, v.varno)?;
                    self.varnos.add_members(self.mcx, &v.varnullingrels)?;
                }
                Ok(false)
            }
            t @ (NodeTag::T_CurrentOfExpr | NodeTag::T_PlaceHolderVar) => {
                deferred("pull_varnos_walker", t)
            }
            NodeTag::T_Query => {
                let q = node.as_query().unwrap();
                self.sublevels_up += 1;
                let r = query_tree_walker(q, self, 0);
                self.sublevels_up -= 1;
                r
            }
            _ => expression_tree_walker(node, self),
        }
    }

    fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
        self.sublevels_up += 1;
        let r = query_tree_walker(q, self, 0);
        self.sublevels_up -= 1;
        r
    }
}

/// C's `root` feeds only the PlaceHolderVar arm (deferred loud); the
/// parameter returns with that vocabulary.
pub fn pull_varnos<'mcx>(mcx: Mcx<'mcx>, node: Node<'mcx>) -> PgResult<Bitmapset<'mcx>> {
    pull_varnos_of_level(mcx, node, 0)
}

pub fn pull_varnos_of_level<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    levelsup: i32,
) -> PgResult<Bitmapset<'mcx>> {
    let mut cx = PullVarnos { mcx, varnos: Bitmapset::empty(), sublevels_up: levelsup as i64 };
    // A top-level Query does not bump sublevels_up.
    match node.as_query() {
        Some(q) => {
            query_tree_walker(q, &mut cx, 0)?;
        }
        None => {
            cx.visit(node)?;
        }
    }
    Ok(cx.varnos)
}

struct PullVarattnos<'a, 'mcx> {
    mcx: Mcx<'mcx>,
    varattnos: &'a mut Bitmapset<'mcx>,
    varno: i32,
}

impl<'a, 'mcx> NodeWalker<'mcx> for PullVarattnos<'a, 'mcx> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        if let Some(v) = node.as_var() {
            if v.varno == self.varno && v.varlevelsup == 0 {
                self.varattnos.add_member(
                    self.mcx,
                    v.varattno as i32 - FirstLowInvalidHeapAttributeNumber,
                )?;
            }
            return Ok(false);
        }
        if node.node_tag() == NodeTag::T_Query {
            panic!("pull_varattnos: unexpected unplanned Query subtree");
        }
        expression_tree_walker(node, self)
    }
}

/// Walks the shared tree directly — no per-node copies (fabled #417: the
/// clone-wrapped walk cost −5.8% on pointplan).
pub fn pull_varattnos<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    varno: i32,
    varattnos: &mut Bitmapset<'mcx>,
) -> PgResult<()> {
    let mut cx = PullVarattnos { mcx, varattnos, varno };
    cx.visit(node)?;
    Ok(())
}

struct PullVars<'mcx> {
    mcx: Mcx<'mcx>,
    vars: NodeList<'mcx>,
    sublevels_up: i64,
}

impl<'mcx> NodeWalker<'mcx> for PullVars<'mcx> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        match node.node_tag() {
            NodeTag::T_Var => {
                let v = node.as_var().unwrap();
                if v.varlevelsup as i64 == self.sublevels_up {
                    self.vars.lappend(self.mcx, node)?;
                }
                Ok(false)
            }
            t @ NodeTag::T_PlaceHolderVar => deferred("pull_vars_walker", t),
            NodeTag::T_Query => {
                let q = node.as_query().unwrap();
                self.sublevels_up += 1;
                let r = query_tree_walker(q, self, 0);
                self.sublevels_up -= 1;
                r
            }
            _ => expression_tree_walker(node, self),
        }
    }

    fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
        self.sublevels_up += 1;
        let r = query_tree_walker(q, self, 0);
        self.sublevels_up -= 1;
        r
    }
}

/// The result list links the found nodes, not copies (C lappends pointers).
pub fn pull_vars_of_level<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    levelsup: i32,
) -> PgResult<NodeList<'mcx>> {
    let mut cx = PullVars { mcx, vars: NodeList::nil(), sublevels_up: levelsup as i64 };
    match node.as_query() {
        Some(q) => {
            query_tree_walker(q, &mut cx, 0)?;
        }
        None => {
            cx.visit(node)?;
        }
    }
    Ok(cx.vars)
}

struct ContainVar;

impl<'mcx> NodeWalker<'mcx> for ContainVar {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        match node.node_tag() {
            NodeTag::T_Var => Ok(node.as_var().unwrap().varlevelsup == 0),
            NodeTag::T_CurrentOfExpr => Ok(true),
            t @ NodeTag::T_PlaceHolderVar => deferred("contain_var_clause_walker", t),
            _ => expression_tree_walker(node, self),
        }
    }
}

/// Does not examine subqueries — use only after sublink reduction.
pub fn contain_var_clause(node: Node<'_>) -> PgResult<bool> {
    ContainVar.visit(node)
}

struct ContainVarsOfLevel {
    sublevels_up: i64,
}

impl<'mcx> NodeWalker<'mcx> for ContainVarsOfLevel {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        match node.node_tag() {
            NodeTag::T_Var => Ok(node.as_var().unwrap().varlevelsup as i64 == self.sublevels_up),
            NodeTag::T_CurrentOfExpr => Ok(self.sublevels_up == 0),
            t @ NodeTag::T_PlaceHolderVar => {
                deferred("contain_vars_of_level_walker", t)
            }
            NodeTag::T_Query => {
                let q = node.as_query().unwrap();
                self.sublevels_up += 1;
                let r = query_tree_walker(q, self, 0);
                self.sublevels_up -= 1;
                r
            }
            _ => expression_tree_walker(node, self),
        }
    }

    fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
        self.sublevels_up += 1;
        let r = query_tree_walker(q, self, 0);
        self.sublevels_up -= 1;
        r
    }
}

pub fn contain_vars_of_level(node: Node<'_>, levelsup: i32) -> PgResult<bool> {
    let mut cx = ContainVarsOfLevel { sublevels_up: levelsup as i64 };
    query_or_expression_tree_walker(node, &mut cx, 0)
}

struct ContainVarsReturningOldOrNew;

impl<'mcx> NodeWalker<'mcx> for ContainVarsReturningOldOrNew {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        match node.node_tag() {
            NodeTag::T_Var => {
                let v = node.as_var().unwrap();
                Ok(v.varlevelsup == 0
                    && v.varreturningtype != VarReturningType::VAR_RETURNING_DEFAULT)
            }
            t @ NodeTag::T_ReturningExpr => {
                deferred("contain_vars_returning_old_or_new_walker", t)
            }
            _ => expression_tree_walker(node, self),
        }
    }
}

pub fn contain_vars_returning_old_or_new(node: Node<'_>) -> PgResult<bool> {
    ContainVarsReturningOldOrNew.visit(node)
}

struct LocateVarOfLevel {
    var_location: i32,
    sublevels_up: i64,
}

impl<'mcx> NodeWalker<'mcx> for LocateVarOfLevel {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        match node.node_tag() {
            NodeTag::T_Var => {
                let v = node.as_var().unwrap();
                if v.varlevelsup as i64 == self.sublevels_up && v.location >= 0 {
                    self.var_location = v.location;
                    return Ok(true);
                }
                Ok(false)
            }
            NodeTag::T_CurrentOfExpr => Ok(false),
            NodeTag::T_Query => {
                let q = node.as_query().unwrap();
                self.sublevels_up += 1;
                let r = query_tree_walker(q, self, 0);
                self.sublevels_up -= 1;
                r
            }
            _ => expression_tree_walker(node, self),
        }
    }

    fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
        self.sublevels_up += 1;
        let r = query_tree_walker(q, self, 0);
        self.sublevels_up -= 1;
        r
    }
}

pub fn locate_var_of_level(node: Node<'_>, levelsup: i32) -> PgResult<i32> {
    let mut cx = LocateVarOfLevel { var_location: -1, sublevels_up: levelsup as i64 };
    query_or_expression_tree_walker(node, &mut cx, 0)?;
    Ok(cx.var_location)
}

#[cold]
fn upper_level_error(what: &str) -> Box<PgError> {
    Box::new(PgError::error(format!("Upper-level {what} found where not expected")))
}

struct PullVarClause<'mcx> {
    mcx: Mcx<'mcx>,
    flags: u32,
    varlist: NodeList<'mcx>,
}

impl<'mcx> NodeWalker<'mcx> for PullVarClause<'mcx> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        match node.node_tag() {
            NodeTag::T_Var => {
                if node.as_var().unwrap().varlevelsup != 0 {
                    return Err(upper_level_error("Var"));
                }
                self.varlist.lappend(self.mcx, node)?;
                return Ok(false);
            }
            NodeTag::T_Aggref => {
                if node.as_aggref().unwrap().agglevelsup != 0 {
                    return Err(upper_level_error("Aggref"));
                }
                if self.flags & PVC_INCLUDE_AGGREGATES != 0 {
                    self.varlist.lappend(self.mcx, node)?;
                    return Ok(false);
                }
                if self.flags & PVC_RECURSE_AGGREGATES == 0 {
                    return Err(Box::new(PgError::error(
                        "Aggref found where not expected".to_string(),
                    )));
                }
            }
            NodeTag::T_GroupingFunc => {
                if node.as_grouping_func().unwrap().agglevelsup != 0 {
                    return Err(upper_level_error("GROUPING"));
                }
                if self.flags & PVC_INCLUDE_AGGREGATES != 0 {
                    self.varlist.lappend(self.mcx, node)?;
                    return Ok(false);
                }
                if self.flags & PVC_RECURSE_AGGREGATES == 0 {
                    return Err(Box::new(PgError::error(
                        "GROUPING found where not expected".to_string(),
                    )));
                }
            }
            NodeTag::T_WindowFunc => {
                if self.flags & PVC_INCLUDE_WINDOWFUNCS != 0 {
                    self.varlist.lappend(self.mcx, node)?;
                    return Ok(false);
                }
                if self.flags & PVC_RECURSE_WINDOWFUNCS == 0 {
                    return Err(Box::new(PgError::error(
                        "WindowFunc found where not expected".to_string(),
                    )));
                }
            }
            t @ NodeTag::T_PlaceHolderVar => deferred("pull_var_clause_walker", t),
            _ => {}
        }
        expression_tree_walker(node, self)
    }
}

/// Returns the found nodes by shared handle.
pub fn pull_var_clause<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    flags: u32,
) -> PgResult<NodeList<'mcx>> {
    debug_assert!(
        flags & (PVC_INCLUDE_AGGREGATES | PVC_RECURSE_AGGREGATES)
            != (PVC_INCLUDE_AGGREGATES | PVC_RECURSE_AGGREGATES)
    );
    debug_assert!(
        flags & (PVC_INCLUDE_WINDOWFUNCS | PVC_RECURSE_WINDOWFUNCS)
            != (PVC_INCLUDE_WINDOWFUNCS | PVC_RECURSE_WINDOWFUNCS)
    );
    debug_assert!(
        flags & (PVC_INCLUDE_PLACEHOLDERS | PVC_RECURSE_PLACEHOLDERS)
            != (PVC_INCLUDE_PLACEHOLDERS | PVC_RECURSE_PLACEHOLDERS)
    );
    let mut cx = PullVarClause { mcx, flags, varlist: NodeList::nil() };
    cx.visit(node)?;
    Ok(cx.varlist)
}

/// C mutator, detection form: join-alias Vars only arise from merged
/// USING/NATURAL columns or join whole-row refs, both unported (loud in the
/// parser), so C's rewrite is the identity on every tree that parses today.
pub fn flatten_join_alias_vars<'mcx>(
    query: &Query<'mcx>,
    node: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    struct W<'a, 'mcx> {
        query: &'a Query<'mcx>,
        sublevels_up: i64,
    }
    impl<'mcx> NodeWalker<'mcx> for W<'_, 'mcx> {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            match node.node_tag() {
                NodeTag::T_Var => {
                    let v = node.as_var().unwrap();
                    if v.varlevelsup as i64 == self.sublevels_up {
                        let rte = self
                            .query
                            .rtable
                            .nth(v.varno as usize - 1)
                            .as_range_tbl_entry()
                            .expect("rtable cell");
                        if rte.rtekind == types_nodes::parsenodes::RTEKind::RTE_JOIN {
                            panic!(
                                "flatten_join_alias_vars (var.c): join alias Var \
                                 (varno {}); join-using lane",
                                v.varno
                            );
                        }
                    }
                    Ok(false)
                }
                t @ NodeTag::T_PlaceHolderVar => deferred("flatten_join_alias_vars", t),
                NodeTag::T_Query => {
                    let q = node.as_query().unwrap();
                    self.sublevels_up += 1;
                    let r = query_tree_walker(q, self, nodes_core::QTW_IGNORE_JOINALIASES);
                    self.sublevels_up -= 1;
                    r
                }
                _ => expression_tree_walker(node, self),
            }
        }

        fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
            self.sublevels_up += 1;
            let r = query_tree_walker(q, self, nodes_core::QTW_IGNORE_JOINALIASES);
            self.sublevels_up -= 1;
            r
        }
    }
    let mut w = W { query, sublevels_up: 0 };
    w.visit(node)?;
    Ok(node)
}

// flatten_group_exprs (var.c), root == NULL arm: GROUP-RTE Vars replaced by
// the referenced grouping expressions (shared, not copied — deparse reads
// only). sublevels_up stays 0 because Query/SubLink descent is a loud panic,
// so IncrementVarSublevelsUp and the agglevelsup < sublevels_up arm are
// unreachable here.
pub fn flatten_group_exprs<'mcx>(
    mcx: Mcx<'mcx>,
    query: &'mcx Query<'mcx>,
    node: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    Ok(fge_mutate(mcx, query, node)?.unwrap_or(node))
}

/// None = unchanged (caller keeps the original list).
pub fn flatten_group_exprs_list<'mcx>(
    mcx: Mcx<'mcx>,
    query: &'mcx Query<'mcx>,
    list: &'mcx NodeList<'mcx>,
) -> PgResult<Option<&'mcx NodeList<'mcx>>> {
    match fge_list(mcx, query, list)? {
        None => Ok(None),
        Some(new) => Ok(Some(
            Node::mk_list(mcx, new)?.as_list().expect("mk_list yields a List"),
        )),
    }
}

fn fge_list<'mcx>(
    mcx: Mcx<'mcx>,
    query: &'mcx Query<'mcx>,
    list: &NodeList<'mcx>,
) -> PgResult<Option<NodeList<'mcx>>> {
    let mut changed = false;
    let mut out: Vec<Node<'mcx>> = Vec::with_capacity(list.len());
    for item in list.iter() {
        match fge_mutate(mcx, query, item)? {
            Some(new) => {
                changed = true;
                out.push(new);
            }
            None => out.push(item),
        }
    }
    if !changed {
        return Ok(None);
    }
    let mut l = NodeList::nil();
    for n in out {
        l.lappend(mcx, n)?;
    }
    Ok(Some(l))
}

#[cold]
#[inline(never)]
fn fge_unported(what: &str) -> ! {
    panic!("flatten_group_exprs (var.c): {what} unported")
}

fn fge_mutate<'mcx>(
    mcx: Mcx<'mcx>,
    query: &'mcx Query<'mcx>,
    node: Node<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    use types_nodes::primnodes as pn;
    use types_nodes::RTEKind;
    match node.node_tag() {
        NodeTag::T_Var => {
            let var = node.as_var().unwrap();
            if var.varlevelsup != 0 {
                return Ok(None);
            }
            let rte = query
                .rtable
                .nth(var.varno as usize - 1)
                .as_range_tbl_entry()
                .expect("rtable entry");
            if rte.rtekind != RTEKind::RTE_GROUP {
                return Ok(None);
            }
            debug_assert!(var.varattno > 0);
            Ok(Some(rte.groupexprs.nth(var.varattno as usize - 1)))
        }
        NodeTag::T_Const | NodeTag::T_Param | NodeTag::T_CaseTestExpr => Ok(None),
        NodeTag::T_Aggref => {
            let a = node.as_aggref().unwrap();
            if !a.aggdirectargs.is_nil() {
                fge_unported("ordered-set aggregate direct args");
            }
            Ok(None)
        }
        NodeTag::T_TargetEntry => {
            let tle = node.as_target_entry().unwrap();
            match fge_mutate(mcx, query, tle.expr)? {
                None => Ok(None),
                Some(expr) => Ok(Some(Node::mk(
                    mcx,
                    pn::TargetEntry {
                        expr,
                        resno: tle.resno,
                        resname: tle.resname,
                        ressortgroupref: tle.ressortgroupref,
                        resorigtbl: tle.resorigtbl,
                        resorigcol: tle.resorigcol,
                        resjunk: tle.resjunk,
                    },
                )?)),
            }
        }
        NodeTag::T_List => match fge_list(mcx, query, node.as_list().unwrap())? {
            None => Ok(None),
            Some(l) => Ok(Some(Node::mk_list(mcx, l)?)),
        },
        NodeTag::T_OpExpr => {
            let o = node.as_op_expr().unwrap();
            match fge_list(mcx, query, &o.args)? {
                None => Ok(None),
                Some(args) => Ok(Some(Node::mk(
                    mcx,
                    pn::OpExpr {
                        opno: o.opno,
                        opfuncid: o.opfuncid,
                        opresulttype: o.opresulttype,
                        opretset: o.opretset,
                        opcollid: o.opcollid,
                        inputcollid: o.inputcollid,
                        args,
                        location: o.location,
                    },
                )?)),
            }
        }
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().unwrap();
            match fge_list(mcx, query, &f.args)? {
                None => Ok(None),
                Some(args) => Ok(Some(Node::mk(
                    mcx,
                    pn::FuncExpr {
                        funcid: f.funcid,
                        funcresulttype: f.funcresulttype,
                        funcretset: f.funcretset,
                        funcvariadic: f.funcvariadic,
                        funcformat: f.funcformat,
                        funccollid: f.funccollid,
                        inputcollid: f.inputcollid,
                        args,
                        location: f.location,
                    },
                )?)),
            }
        }
        NodeTag::T_BoolExpr => {
            let b = node.as_bool_expr().unwrap();
            match fge_list(mcx, query, &b.args)? {
                None => Ok(None),
                Some(args) => Ok(Some(Node::mk(
                    mcx,
                    pn::BoolExpr { boolop: b.boolop, args, location: b.location },
                )?)),
            }
        }
        NodeTag::T_ScalarArrayOpExpr => {
            let s = node.as_scalar_array_op_expr().unwrap();
            match fge_list(mcx, query, &s.args)? {
                None => Ok(None),
                Some(args) => Ok(Some(Node::mk(
                    mcx,
                    pn::ScalarArrayOpExpr {
                        opno: s.opno,
                        opfuncid: s.opfuncid,
                        hashfuncid: s.hashfuncid,
                        negfuncid: s.negfuncid,
                        useOr: s.useOr,
                        inputcollid: s.inputcollid,
                        args,
                        location: s.location,
                    },
                )?)),
            }
        }
        NodeTag::T_CoalesceExpr => {
            let c = node.as_coalesce_expr().unwrap();
            match fge_list(mcx, query, &c.args)? {
                None => Ok(None),
                Some(args) => Ok(Some(Node::mk(
                    mcx,
                    pn::CoalesceExpr {
                        coalescetype: c.coalescetype,
                        coalescecollid: c.coalescecollid,
                        args,
                        location: c.location,
                    },
                )?)),
            }
        }
        NodeTag::T_MinMaxExpr => {
            let m = node.as_min_max_expr().unwrap();
            match fge_list(mcx, query, &m.args)? {
                None => Ok(None),
                Some(args) => Ok(Some(Node::mk(
                    mcx,
                    pn::MinMaxExpr {
                        minmaxtype: m.minmaxtype,
                        minmaxcollid: m.minmaxcollid,
                        inputcollid: m.inputcollid,
                        op: m.op,
                        args,
                        location: m.location,
                    },
                )?)),
            }
        }
        NodeTag::T_ArrayExpr => {
            let a = node.as_array_expr().unwrap();
            match fge_list(mcx, query, &a.elements)? {
                None => Ok(None),
                Some(elements) => Ok(Some(Node::mk(
                    mcx,
                    pn::ArrayExpr {
                        array_typeid: a.array_typeid,
                        array_collid: a.array_collid,
                        element_typeid: a.element_typeid,
                        elements,
                        multidims: a.multidims,
                        list_start: a.list_start,
                        list_end: a.list_end,
                        location: a.location,
                    },
                )?)),
            }
        }
        NodeTag::T_CaseExpr => {
            let c = node.as_case_expr().unwrap();
            let arg = fge_opt(mcx, query, c.arg)?;
            let args = fge_list(mcx, query, &c.args)?;
            let defresult = fge_opt(mcx, query, c.defresult)?;
            if arg.is_none() && args.is_none() && defresult.is_none() {
                return Ok(None);
            }
            let mut new_args = NodeList::nil();
            match args {
                Some(l) => new_args = l,
                None => {
                    for x in c.args.iter() {
                        new_args.lappend(mcx, x)?;
                    }
                }
            }
            Ok(Some(Node::mk(
                mcx,
                pn::CaseExpr {
                    casetype: c.casetype,
                    casecollid: c.casecollid,
                    arg: arg.or(c.arg),
                    args: new_args,
                    defresult: defresult.or(c.defresult),
                    location: c.location,
                },
            )?))
        }
        NodeTag::T_CaseWhen => {
            let w = node.as_case_when().unwrap();
            let expr = fge_opt(mcx, query, w.expr)?;
            let result = fge_opt(mcx, query, w.result)?;
            if expr.is_none() && result.is_none() {
                return Ok(None);
            }
            Ok(Some(Node::mk(
                mcx,
                pn::CaseWhen {
                    expr: expr.or(w.expr),
                    result: result.or(w.result),
                    location: w.location,
                },
            )?))
        }
        NodeTag::T_NullTest => {
            let n = node.as_null_test().unwrap();
            match fge_opt(mcx, query, n.arg)? {
                None => Ok(None),
                Some(arg) => Ok(Some(Node::mk(
                    mcx,
                    pn::NullTest {
                        arg: Some(arg),
                        nulltesttype: n.nulltesttype,
                        argisrow: n.argisrow,
                        location: n.location,
                    },
                )?)),
            }
        }
        NodeTag::T_RelabelType => {
            let r = node.as_relabel_type().unwrap();
            match fge_mutate(mcx, query, r.arg)? {
                None => Ok(None),
                Some(arg) => Ok(Some(Node::mk(
                    mcx,
                    pn::RelabelType {
                        arg,
                        resulttype: r.resulttype,
                        resulttypmod: r.resulttypmod,
                        resultcollid: r.resultcollid,
                        relabelformat: r.relabelformat,
                        location: r.location,
                    },
                )?)),
            }
        }
        NodeTag::T_CoerceViaIO => {
            let c = node.as_coerce_via_io().unwrap();
            match fge_mutate(mcx, query, c.arg)? {
                None => Ok(None),
                Some(arg) => Ok(Some(Node::mk(
                    mcx,
                    pn::CoerceViaIO {
                        arg,
                        resulttype: c.resulttype,
                        resultcollid: c.resultcollid,
                        coerceformat: c.coerceformat,
                        location: c.location,
                    },
                )?)),
            }
        }
        NodeTag::T_SubLink | NodeTag::T_Query => {
            fge_unported("GROUP-var flattening below a subquery")
        }
        other => fge_unported(&format!("{other:?} mutator arm")),
    }
}

fn fge_opt<'mcx>(
    mcx: Mcx<'mcx>,
    query: &'mcx Query<'mcx>,
    node: Option<Node<'mcx>>,
) -> PgResult<Option<Node<'mcx>>> {
    match node {
        None => Ok(None),
        Some(n) => fge_mutate(mcx, query, n),
    }
}
