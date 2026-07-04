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
            NodeTag::T_CurrentOfExpr => {
                if self.sublevels_up == 0 {
                    let cvarno = node.as_current_of_expr().unwrap().cvarno;
                    self.varnos.add_member(self.mcx, cvarno as i32)?;
                }
                Ok(false)
            }
            t @ NodeTag::T_PlaceHolderVar => deferred("pull_varnos_walker", t),
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

struct ContainUplevelVars {
    sublevels_up: i64,
}

impl<'mcx> NodeWalker<'mcx> for ContainUplevelVars {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        match node.node_tag() {
            NodeTag::T_Var => Ok(node.as_var().unwrap().varlevelsup as i64 >= self.sublevels_up),
            NodeTag::T_CurrentOfExpr => Ok(false),
            t @ NodeTag::T_PlaceHolderVar => deferred("contain_uplevel_vars_walker", t),
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

/// Any Var escaping `node` (varlevelsup >= 1 relative to it); the gate for
/// C's IncrementVarSublevelsUp being a no-op.
pub fn contain_uplevel_vars(node: Node<'_>) -> PgResult<bool> {
    let mut cx = ContainUplevelVars { sublevels_up: 1 };
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

// flatten_join_alias_vars (var.c), root == NULL shape: join alias Vars are
// replaced by copies of the joinaliasvars expression (whole-row by a RowExpr
// over them); nullingrels transfer via the standard-expression adjustment.
// The PlaceHolderVar fallback (root != NULL) and IncrementVarSublevelsUp for
// aliases carried into subqueries stay loud.
pub fn flatten_join_alias_vars<'mcx>(
    mcx: Mcx<'mcx>,
    rtable: &NodeList<'mcx>,
    node: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    Ok(fjav_mutate(mcx, rtable, node)?.unwrap_or(node))
}

fn fjav_mutate<'mcx>(
    mcx: Mcx<'mcx>,
    rtable: &NodeList<'mcx>,
    node: Node<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    match node.node_tag() {
        NodeTag::T_Var => {
            let v = node.as_var().unwrap();
            if v.varlevelsup != 0 {
                return Ok(None);
            }
            let rte = rtable
                .nth(v.varno as usize - 1)
                .as_range_tbl_entry()
                .expect("rtable cell");
            if rte.rtekind != types_nodes::parsenodes::RTEKind::RTE_JOIN {
                return Ok(None);
            }
            if v.varattno == 0 {
                let eref = rte.eref.expect("join RTE has eref");
                assert_eq!(rte.joinaliasvars.len(), eref.colnames.len());
                let mut fields = NodeList::nil();
                let mut colnames = NodeList::nil();
                for (av, cn) in rte.joinaliasvars.iter().zip(eref.colnames.iter()) {
                    let newvar = fjav_copy(mcx, av)?;
                    if newvar.as_var().is_some() {
                        // SAFETY: fjav_copy returned a fresh node.
                        unsafe {
                            newvar
                                .with_mut::<types_nodes::Var, _>(|x| x.location = v.location)
                                .unwrap();
                        }
                    }
                    let newvar = fjav_mutate(mcx, rtable, newvar)?.unwrap_or(newvar);
                    fields.lappend(mcx, newvar)?;
                    colnames.lappend(mcx, cn)?;
                }
                let rowexpr = Node::mk(
                    mcx,
                    types_nodes::RowExpr {
                        args: fields,
                        row_typeid: v.vartype,
                        row_format: types_nodes::CoercionForm::COERCE_IMPLICIT_CAST,
                        colnames,
                        location: v.location,
                    },
                )?;
                return Ok(Some(add_nullingrels_if_needed(mcx, rowexpr, v)?));
            }
            debug_assert!(v.varattno > 0);
            let aliasvar = rte.joinaliasvars.nth(v.varattno as usize - 1);
            let newvar = fjav_copy(mcx, aliasvar)?;
            if newvar.as_var().is_some() {
                // SAFETY: fjav_copy returned a fresh node.
                unsafe {
                    newvar
                        .with_mut::<types_nodes::Var, _>(|x| x.location = v.location)
                        .unwrap();
                }
            }
            let newvar = fjav_mutate(mcx, rtable, newvar)?.unwrap_or(newvar);
            Ok(Some(add_nullingrels_if_needed(mcx, newvar, v)?))
        }
        t @ NodeTag::T_PlaceHolderVar => deferred("flatten_join_alias_vars", t),
        NodeTag::T_Query => {
            // A subquery matters here only if it references a join RTE of an
            // upper level; the sublevels bookkeeping is unported, so scan and
            // stay loud only when the rewrite would change anything.
            let q = node.as_query().unwrap();
            assert_subquery_free_of_upper_join_vars(rtable, q)?;
            Ok(None)
        }
        _ => nodes_core::expression_tree_mutator(mcx, node, &mut |n| fjav_mutate(mcx, rtable, n)),
    }
}

fn assert_subquery_free_of_upper_join_vars<'mcx>(
    outer_rtable: &NodeList<'mcx>,
    q: &'mcx Query<'mcx>,
) -> PgResult<()> {
    struct W<'a, 'mcx> {
        outer_rtable: &'a NodeList<'mcx>,
        levels: i64,
    }
    impl<'mcx> NodeWalker<'mcx> for W<'_, 'mcx> {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            match node.node_tag() {
                NodeTag::T_Var => {
                    let v = node.as_var().unwrap();
                    if v.varlevelsup as i64 == self.levels {
                        let rte = self
                            .outer_rtable
                            .nth(v.varno as usize - 1)
                            .as_range_tbl_entry()
                            .expect("rtable cell");
                        if rte.rtekind == types_nodes::parsenodes::RTEKind::RTE_JOIN {
                            panic!(
                                "flatten_join_alias_vars (var.c): join alias Var under a \
                                 subquery (IncrementVarSublevelsUp leg) — join-using residue"
                            );
                        }
                    }
                    Ok(false)
                }
                NodeTag::T_Query => {
                    let sub = node.as_query().unwrap();
                    self.levels += 1;
                    let r = query_tree_walker(sub, self, nodes_core::QTW_IGNORE_JOINALIASES);
                    self.levels -= 1;
                    r
                }
                _ => expression_tree_walker(node, self),
            }
        }
        fn visit_query_ref(&mut self, sub: &'mcx Query<'mcx>) -> PgResult<bool> {
            self.levels += 1;
            let r = query_tree_walker(sub, self, nodes_core::QTW_IGNORE_JOINALIASES);
            self.levels -= 1;
            r
        }
    }
    let mut w = W { outer_rtable, levels: 1 };
    query_tree_walker(q, &mut w, nodes_core::QTW_IGNORE_JOINALIASES)?;
    Ok(())
}

// Deep copy of a joinaliasvars entry along its standard-expression spine
// (parse_clause only builds Vars, implicit coercions, and COALESCE); the
// nullingrels adjustment mutates the copy in place, so sharing is not safe.
fn fjav_copy<'mcx>(mcx: Mcx<'mcx>, node: Node<'mcx>) -> PgResult<Node<'mcx>> {
    match node.node_tag() {
        NodeTag::T_Var => {
            let v = node.as_var().unwrap();
            Node::mk(
                mcx,
                types_nodes::Var {
                    varno: v.varno,
                    varattno: v.varattno,
                    vartype: v.vartype,
                    vartypmod: v.vartypmod,
                    varcollid: v.varcollid,
                    varnullingrels: v.varnullingrels.clone_in(mcx)?,
                    varlevelsup: v.varlevelsup,
                    varreturningtype: v.varreturningtype,
                    varnosyn: v.varnosyn,
                    varattnosyn: v.varattnosyn,
                    location: v.location,
                },
            )
        }
        NodeTag::T_Const => Ok(node),
        NodeTag::T_RelabelType => {
            let r = node.as_relabel_type().unwrap();
            Node::mk(
                mcx,
                types_nodes::RelabelType {
                    arg: fjav_copy(mcx, r.arg)?,
                    resulttype: r.resulttype,
                    resulttypmod: r.resulttypmod,
                    resultcollid: r.resultcollid,
                    relabelformat: r.relabelformat,
                    location: r.location,
                },
            )
        }
        NodeTag::T_CoerceViaIO => {
            let c = node.as_coerce_via_io().unwrap();
            Node::mk(
                mcx,
                types_nodes::CoerceViaIO {
                    arg: fjav_copy(mcx, c.arg)?,
                    resulttype: c.resulttype,
                    resultcollid: c.resultcollid,
                    coerceformat: c.coerceformat,
                    location: c.location,
                },
            )
        }
        NodeTag::T_ArrayCoerceExpr => {
            let a = node.as_array_coerce_expr().unwrap();
            // elemexpr is off the Var spine (the nullingrels adjustment never
            // descends into it), so sharing it is safe.
            Node::mk(
                mcx,
                types_nodes::ArrayCoerceExpr {
                    arg: fjav_copy(mcx, a.arg)?,
                    ..*a
                },
            )
        }
        NodeTag::T_ConvertRowtypeExpr => {
            let c = node.as_convert_rowtype_expr().unwrap();
            Node::mk(
                mcx,
                types_nodes::ConvertRowtypeExpr {
                    arg: fjav_copy(mcx, c.arg)?,
                    ..*c
                },
            )
        }
        NodeTag::T_FuncExpr => {
            let f = node.as_func_expr().unwrap();
            let mut args = NodeList::nil();
            for a in &f.args {
                args.lappend(mcx, fjav_copy(mcx, a)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::FuncExpr {
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
            )
        }
        NodeTag::T_CoalesceExpr => {
            let c = node.as_coalesce_expr().unwrap();
            let mut args = NodeList::nil();
            for a in &c.args {
                args.lappend(mcx, fjav_copy(mcx, a)?)?;
            }
            Node::mk(
                mcx,
                types_nodes::primnodes::CoalesceExpr {
                    coalescetype: c.coalescetype,
                    coalescecollid: c.coalescecollid,
                    args,
                    location: c.location,
                },
            )
        }
        t => deferred("fjav_copy: joinaliasvars entry", t),
    }
}

// add_nullingrels_if_needed (var.c); the make_placeholder_expr fallback for
// non-standard expressions is unported and loud.
fn add_nullingrels_if_needed<'mcx>(
    mcx: Mcx<'mcx>,
    newnode: Node<'mcx>,
    oldvar: &types_nodes::Var<'mcx>,
) -> PgResult<Node<'mcx>> {
    if oldvar.varnullingrels.is_empty() {
        return Ok(newnode);
    }
    if !is_standard_join_alias_expression(newnode, oldvar) {
        panic!(
            "add_nullingrels_if_needed (var.c): non-standard join alias expression \
             (make_placeholder_expr leg) — join-using residue"
        );
    }
    adjust_standard_join_alias_expression(mcx, newnode, oldvar)?;
    Ok(newnode)
}

fn is_standard_join_alias_expression(newnode: Node<'_>, oldvar: &types_nodes::Var<'_>) -> bool {
    match newnode.node_tag() {
        NodeTag::T_Var => newnode.as_var().unwrap().varlevelsup == oldvar.varlevelsup,
        NodeTag::T_FuncExpr => {
            let f = newnode.as_func_expr().unwrap();
            // Implicit coercions never make non-NULL from NULL; examine only
            // the first argument (the rest are coercion constants).
            if f.funcformat != types_nodes::CoercionForm::COERCE_IMPLICIT_CAST
                || f.args.is_nil()
            {
                return false;
            }
            is_standard_join_alias_expression(f.args.nth(0), oldvar)
        }
        NodeTag::T_RelabelType => {
            is_standard_join_alias_expression(newnode.as_relabel_type().unwrap().arg, oldvar)
        }
        NodeTag::T_CoerceViaIO => {
            is_standard_join_alias_expression(newnode.as_coerce_via_io().unwrap().arg, oldvar)
        }
        // C accepts ArrayCoerceExpr here but not ConvertRowtypeExpr.
        NodeTag::T_ArrayCoerceExpr => {
            is_standard_join_alias_expression(newnode.as_array_coerce_expr().unwrap().arg, oldvar)
        }
        NodeTag::T_CoalesceExpr => {
            let c = newnode.as_coalesce_expr().unwrap();
            debug_assert!(!c.args.is_nil());
            c.args.iter().all(|a| is_standard_join_alias_expression(a, oldvar))
        }
        _ => false,
    }
}

fn adjust_standard_join_alias_expression<'mcx>(
    mcx: Mcx<'mcx>,
    newnode: Node<'mcx>,
    oldvar: &types_nodes::Var<'mcx>,
) -> PgResult<()> {
    match newnode.node_tag() {
        NodeTag::T_Var if newnode.as_var().unwrap().varlevelsup == oldvar.varlevelsup => {
            // SAFETY: fjav_copy made this node fresh; no live derived refs.
            unsafe {
                newnode
                    .with_mut::<types_nodes::Var, _>(|v| {
                        v.varnullingrels.add_members(mcx, &oldvar.varnullingrels)
                    })
                    .unwrap()
            }
        }
        NodeTag::T_FuncExpr => adjust_standard_join_alias_expression(
            mcx,
            newnode.as_func_expr().unwrap().args.nth(0),
            oldvar,
        ),
        NodeTag::T_RelabelType => adjust_standard_join_alias_expression(
            mcx,
            newnode.as_relabel_type().unwrap().arg,
            oldvar,
        ),
        NodeTag::T_CoerceViaIO => adjust_standard_join_alias_expression(
            mcx,
            newnode.as_coerce_via_io().unwrap().arg,
            oldvar,
        ),
        NodeTag::T_ArrayCoerceExpr => adjust_standard_join_alias_expression(
            mcx,
            newnode.as_array_coerce_expr().unwrap().arg,
            oldvar,
        ),
        NodeTag::T_CoalesceExpr => {
            for a in &newnode.as_coalesce_expr().unwrap().args {
                adjust_standard_join_alias_expression(mcx, a, oldvar)?;
            }
            Ok(())
        }
        t => panic!("adjust_standard_join_alias_expression: unexpected {t:?}"),
    }
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
            // C: at the agg's own level only aggdirectargs can hold grouped
            // Vars; args/order/filter are not recursed into.
            let a = node.as_aggref().unwrap();
            if a.agglevelsup != 0 {
                fge_unported("outer-level aggregate");
            }
            match fge_list(mcx, query, &a.aggdirectargs)? {
                None => Ok(None),
                Some(aggdirectargs) => Ok(Some(Node::mk(
                    mcx,
                    pn::Aggref {
                        aggfnoid: a.aggfnoid,
                        aggtype: a.aggtype,
                        aggcollid: a.aggcollid,
                        inputcollid: a.inputcollid,
                        aggtranstype: a.aggtranstype,
                        aggargtypes: a.aggargtypes.clone_in(mcx)?,
                        aggdirectargs,
                        args: a.args.clone_in(mcx)?,
                        aggorder: a.aggorder.clone_in(mcx)?,
                        aggdistinct: a.aggdistinct.clone_in(mcx)?,
                        aggfilter: a.aggfilter,
                        aggstar: a.aggstar,
                        aggvariadic: a.aggvariadic,
                        aggkind: a.aggkind,
                        aggpresorted: a.aggpresorted,
                        agglevelsup: a.agglevelsup,
                        aggsplit: a.aggsplit,
                        aggno: a.aggno,
                        aggtransno: a.aggtransno,
                        location: a.location,
                    },
                )?)),
            }
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
        NodeTag::T_FieldSelect => {
            let f = node.as_field_select().unwrap();
            match fge_mutate(mcx, query, f.arg)? {
                None => Ok(None),
                Some(arg) => Ok(Some(Node::mk(
                    mcx,
                    pn::FieldSelect { arg, ..*f },
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
        NodeTag::T_ArrayCoerceExpr => {
            let a = node.as_array_coerce_expr().unwrap();
            let arg = fge_mutate(mcx, query, a.arg)?;
            let elemexpr = fge_opt(mcx, query, a.elemexpr)?;
            if arg.is_none() && elemexpr.is_none() {
                Ok(None)
            } else {
                Ok(Some(Node::mk(
                    mcx,
                    pn::ArrayCoerceExpr {
                        arg: arg.unwrap_or(a.arg),
                        elemexpr: elemexpr.or(a.elemexpr),
                        ..*a
                    },
                )?))
            }
        }
        NodeTag::T_ConvertRowtypeExpr => {
            let c = node.as_convert_rowtype_expr().unwrap();
            match fge_mutate(mcx, query, c.arg)? {
                None => Ok(None),
                Some(arg) => Ok(Some(Node::mk(mcx, pn::ConvertRowtypeExpr { arg, ..*c })?)),
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
