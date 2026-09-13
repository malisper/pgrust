use nodes_core::{
    expression_tree_walker, query_or_expression_tree_walker, query_tree_walker,
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

/// Level-zero PlaceHolderVar override for pull_varnos: returns the varnos the
/// PHV contributes (exclusive of phnullingrels, which the walker always
/// adds), or None for the syntactic phrels fallback. C's pull_varnos_walker
/// consults root->placeholder_array here (var.c pull_varnos_walker PHV arm);
/// this hook carries that consultation without a root in this crate.
pub type PhvVarnosHook<'a, 'mcx> = &'a mut dyn FnMut(
    &types_nodes::primnodes::PlaceHolderVar<'mcx>,
) -> PgResult<Option<Bitmapset<'mcx>>>;

struct PullVarnos<'a, 'mcx> {
    mcx: Mcx<'mcx>,
    varnos: Bitmapset<'mcx>,
    sublevels_up: i64,
    phv_hook: Option<PhvVarnosHook<'a, 'mcx>>,
}

impl<'a, 'mcx> NodeWalker<'mcx> for PullVarnos<'a, 'mcx> {
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
            NodeTag::T_PlaceHolderVar => {
                let phv = node.as_place_holder_var().unwrap();
                if phv.phlevelsup as i64 == self.sublevels_up {
                    // C consults the phinfo only for phlevelsup == 0 PHVs.
                    let hooked = if phv.phlevelsup == 0 {
                        match self.phv_hook.as_mut() {
                            Some(h) => h(phv)?,
                            None => None,
                        }
                    } else {
                        None
                    };
                    match hooked {
                        Some(v) => self.varnos.add_members(self.mcx, &v)?,
                        None => self.varnos.add_members(self.mcx, &phv.phrels)?,
                    }
                    self.varnos.add_members(self.mcx, &phv.phnullingrels)?;
                    return Ok(false);
                }
                expression_tree_walker(node, self)
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

/// C's `root` feeds only the PlaceHolderVar arm; rootless callers get the
/// phrels fallback, planner callers pass [`pull_varnos_with_phv_hook`].
pub fn pull_varnos<'mcx>(mcx: Mcx<'mcx>, node: Node<'mcx>) -> PgResult<Bitmapset<'mcx>> {
    pull_varnos_of_level(mcx, node, 0)
}

pub fn pull_varnos_of_level<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    levelsup: i32,
) -> PgResult<Bitmapset<'mcx>> {
    let mut cx = PullVarnos {
        mcx,
        varnos: Bitmapset::empty(),
        sublevels_up: levelsup as i64,
        phv_hook: None,
    };
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

/// pull_varnos with a PlaceHolderVar override (C's root-carrying form).
pub fn pull_varnos_with_phv_hook<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    phv_hook: PhvVarnosHook<'a, 'mcx>,
) -> PgResult<Bitmapset<'mcx>> {
    let mut cx = PullVarnos {
        mcx,
        varnos: Bitmapset::empty(),
        sublevels_up: 0,
        phv_hook: Some(phv_hook),
    };
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

/// Walks the shared tree directly — no per-node copies (an internal issue: the
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
            NodeTag::T_PlaceHolderVar => {
                let phv = node.as_place_holder_var().unwrap();
                if phv.phlevelsup as i64 == self.sublevels_up {
                    self.vars.lappend(self.mcx, node)?;
                }
                Ok(false)
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
            NodeTag::T_PlaceHolderVar => {
                if node.as_place_holder_var().unwrap().phlevelsup == 0 {
                    return Ok(true);
                }
                expression_tree_walker(node, self)
            }
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
            NodeTag::T_PlaceHolderVar => {
                let phv = node.as_place_holder_var().unwrap();
                if phv.phlevelsup as i64 == self.sublevels_up {
                    return Ok(true);
                }
                expression_tree_walker(node, self)
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
            NodeTag::T_PlaceHolderVar => {
                let phv = node.as_place_holder_var().unwrap();
                if phv.phlevelsup as i64 >= self.sublevels_up {
                    return Ok(true);
                }
                expression_tree_walker(node, self)
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
            NodeTag::T_ReturningExpr => {
                Ok(node.as_returning_expr().expect("ReturningExpr").retlevelsup == 0)
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

#[track_caller]
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
            NodeTag::T_PlaceHolderVar => {
                if node.as_place_holder_var().unwrap().phlevelsup != 0 {
                    return Err(upper_level_error("PlaceHolderVar"));
                }
                if self.flags & PVC_INCLUDE_PLACEHOLDERS != 0 {
                    self.varlist.lappend(self.mcx, node)?;
                    return Ok(false);
                }
                if self.flags & PVC_RECURSE_PLACEHOLDERS == 0 {
                    return Err(Box::new(PgError::error(
                        "PlaceHolderVar found where not expected".to_string(),
                    )));
                }
            }
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

/// C's PlannerInfo as flatten_join_alias_vars needs it: only
/// make_placeholder_expr's phid allocation (root->glob->lastPHId).
pub struct FjavRoot<'a> {
    pub last_ph_id: &'a core::cell::Cell<u32>,
}

struct FjavCtx<'a, 'mcx> {
    mcx: Mcx<'mcx>,
    rtable: &'a NodeList<'mcx>,
    jointree: Option<&'mcx types_nodes::primnodes::FromExpr<'mcx>>,
    root: Option<&'a FjavRoot<'a>>,
    sublevels_up: i32,
    possible_sublink: bool,
    inserted_sublink: bool,
}

pub fn flatten_join_alias_vars<'a, 'mcx>(
    mcx: Mcx<'mcx>,
    rtable: &'a NodeList<'mcx>,
    jointree: Option<&'mcx types_nodes::primnodes::FromExpr<'mcx>>,
    root: Option<&'a FjavRoot<'a>>,
    node: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let mut ctx = FjavCtx {
        mcx,
        rtable,
        jointree,
        root,
        sublevels_up: 0,
        possible_sublink: true,
        inserted_sublink: true,
    };
    Ok(fjav_mutate(&mut ctx, node)?.unwrap_or(node))
}

fn fjav_shift_copy<'mcx>(
    ctx: &mut FjavCtx<'_, 'mcx>,
    src: Node<'mcx>,
    location: i32,
) -> PgResult<Node<'mcx>> {
    let newvar = fjav_copy(ctx.mcx, src)?;
    if ctx.sublevels_up != 0 {
        rewrite_manip::IncrementVarSublevelsUp(newvar, ctx.sublevels_up, 0)?;
    }
    if newvar.as_var().is_some() {
        // SAFETY: fjav_copy returned a fresh node.
        unsafe {
            newvar
                .with_mut::<types_nodes::Var, _>(|x| x.location = location)
                .unwrap();
        }
    }
    Ok(fjav_mutate(ctx, newvar)?.unwrap_or(newvar))
}

fn fjav_mutate<'mcx>(
    ctx: &mut FjavCtx<'_, 'mcx>,
    node: Node<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    match node.node_tag() {
        NodeTag::T_Var => {
            let v = node.as_var().unwrap();
            if v.varlevelsup as i32 != ctx.sublevels_up {
                return Ok(None);
            }
            let rte = ctx
                .rtable
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
                    // C var.c: a dropped join column is a NULL joinaliasvars
                    // cell (skipped here). This port marks it with an
                    // InvalidOid-typed null Const sentinel (AcquireRewriteLocks);
                    // a real NULL Const (subquery pull-up) stays a row field.
                    if matches!(av.as_const(), Some(c) if c.constisnull && c.consttype == types_core::InvalidOid) {
                        continue;
                    }
                    let newvar = fjav_shift_copy(ctx, av, v.location)?;
                    fields.lappend(ctx.mcx, newvar)?;
                    colnames.lappend(ctx.mcx, cn)?;
                }
                let rowexpr = Node::mk(
                    ctx.mcx,
                    types_nodes::RowExpr {
                        args: fields,
                        row_typeid: v.vartype,
                        row_format: types_nodes::CoercionForm::COERCE_IMPLICIT_CAST,
                        colnames,
                        location: v.location,
                    },
                )?;
                return Ok(Some(add_nullingrels_if_needed(ctx, rowexpr, v)?));
            }
            debug_assert!(v.varattno > 0);
            let aliasvar = rte.joinaliasvars.nth(v.varattno as usize - 1);
            debug_assert!(
                !matches!(aliasvar.as_const(), Some(c) if c.constisnull && c.consttype == types_core::InvalidOid),
                "flatten_join_alias_vars: reference to a dropped join column"
            );
            let newvar = fjav_shift_copy(ctx, aliasvar, v.location)?;
            if ctx.possible_sublink && !ctx.inserted_sublink {
                ctx.inserted_sublink = rewrite_manip::checkExprHasSubLink(newvar)?;
            }
            Ok(Some(add_nullingrels_if_needed(ctx, newvar, v)?))
        }
        NodeTag::T_PlaceHolderVar => {
            let phv = node.as_place_holder_var().unwrap();
            let new_expr = fjav_mutate(ctx, phv.phexpr)?.unwrap_or(phv.phexpr);
            let phrels = if phv.phlevelsup as i32 == ctx.sublevels_up {
                alias_relid_set(ctx.rtable, ctx.jointree, &phv.phrels, ctx.mcx)?
            } else {
                phv.phrels.clone_in(ctx.mcx)?
            };
            Ok(Some(Node::mk(
                ctx.mcx,
                types_nodes::primnodes::PlaceHolderVar {
                    phexpr: new_expr,
                    phrels,
                    phnullingrels: phv.phnullingrels.clone_in(ctx.mcx)?,
                    phid: phv.phid,
                    phlevelsup: phv.phlevelsup,
                },
            )?))
        }
        NodeTag::T_SubLink => {
            let sl = node.as_sub_link().unwrap();
            let new_test = match sl.testexpr {
                None => None,
                Some(te) => fjav_mutate(ctx, te)?,
            };
            let new_sub = fjav_query_descend(ctx, sl.subselect.as_query().expect("SubLink holds a Query"))?;
            if new_test.is_none() && new_sub.is_none() {
                return Ok(None);
            }
            Ok(Some(Node::mk(
                ctx.mcx,
                types_nodes::primnodes::SubLink {
                    subLinkType: sl.subLinkType,
                    subLinkId: sl.subLinkId,
                    testexpr: new_test.or(sl.testexpr),
                    operName: sl.operName.clone_in(ctx.mcx)?,
                    subselect: new_sub.unwrap_or(sl.subselect),
                    location: sl.location,
                },
            )?))
        }
        NodeTag::T_Query => fjav_query_descend(ctx, node.as_query().expect("Query")),
        _ => {
            let mcx = ctx.mcx;
            nodes_core::expression_tree_mutator(mcx, node, &mut |n| fjav_mutate(ctx, n))
        }
    }
}

fn fjav_query_has_join_alias_vars<'mcx>(
    ctx: &FjavCtx<'_, 'mcx>,
    q: &'mcx Query<'mcx>,
) -> PgResult<bool> {
    struct W<'a, 'x> {
        rtable: &'a NodeList<'x>,
        sublevels_up: i32,
        found: bool,
    }
    impl<'mcx> NodeWalker<'mcx> for W<'_, 'mcx> {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            if let Some(v) = node.as_var() {
                if v.varlevelsup as i32 == self.sublevels_up {
                    let rte = self
                        .rtable
                        .nth(v.varno as usize - 1)
                        .as_range_tbl_entry()
                        .expect("rtable cell");
                    if rte.rtekind == types_nodes::parsenodes::RTEKind::RTE_JOIN {
                        self.found = true;
                        return Ok(true);
                    }
                }
                return Ok(false);
            }
            if let Some(q) = node.as_query() {
                self.sublevels_up += 1;
                let r = query_tree_walker(q, self, nodes_core::QTW_IGNORE_JOINALIASES);
                self.sublevels_up -= 1;
                return r;
            }
            expression_tree_walker(node, self)
        }
        fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
            self.sublevels_up += 1;
            let r = query_tree_walker(q, self, nodes_core::QTW_IGNORE_JOINALIASES);
            self.sublevels_up -= 1;
            r
        }
    }
    let mut w = W {
        rtable: ctx.rtable,
        sublevels_up: ctx.sublevels_up + 1,
        found: false,
    };
    query_tree_walker(q, &mut w, nodes_core::QTW_IGNORE_JOINALIASES)?;
    Ok(w.found)
}

fn fjav_query_descend<'mcx>(
    ctx: &mut FjavCtx<'_, 'mcx>,
    q: &'mcx Query<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    if !fjav_query_has_join_alias_vars(ctx, q)? {
        return Ok(None);
    }
    let qnode = rewrite_manip::copy_query_node(ctx.mcx, q)?;
    fjav_query_inplace(ctx, qnode)?;
    Ok(Some(qnode))
}

fn fjav_query_inplace<'mcx>(ctx: &mut FjavCtx<'_, 'mcx>, qnode: Node<'mcx>) -> PgResult<()> {
    let mcx = ctx.mcx;
    let q = qnode.as_query().expect("Query");
    ctx.sublevels_up += 1;
    let save_inserted = ctx.inserted_sublink;
    ctx.inserted_sublink = q.hasSubLinks;

    let new_target = fjav_list(ctx, &q.targetList)?;
    let new_returning = fjav_list(ctx, &q.returningList)?;
    let new_having = fjav_opt(ctx, q.havingQual)?;
    let new_limit_off = fjav_opt(ctx, q.limitOffset)?;
    let new_limit_cnt = fjav_opt(ctx, q.limitCount)?;
    let new_setops = fjav_opt(ctx, q.setOperations)?;
    let new_merge_join = fjav_opt(ctx, q.mergeJoinCondition)?;
    if let Some(oc) = q.onConflict {
        fjav_onconflict_inplace(ctx, oc)?;
    }
    for wco_node in &q.withCheckOptions {
        let wco = wco_node.as_with_check_option().expect("withCheckOptions cell");
        if let Some(new_qual) = fjav_opt(ctx, wco.qual)? {
            // SAFETY: exclusive copy from fjav_query_descend.
            unsafe {
                wco_node.with_mut::<types_nodes::parsenodes::WithCheckOption, _>(|w| {
                    w.qual = Some(new_qual)
                })
            }
            .expect("WithCheckOption");
        }
    }
    for action_node in &q.mergeActionList {
        let action = action_node.as_merge_action().expect("mergeActionList cell");
        let new_qual = fjav_opt(ctx, action.qual)?;
        let new_tlist = fjav_list(ctx, &action.targetList)?;
        if new_qual.is_some() || new_tlist.is_some() {
            // SAFETY: exclusive copy from fjav_query_descend.
            unsafe {
                action_node.with_mut::<types_nodes::MergeAction, _>(|a| {
                    if new_qual.is_some() {
                        a.qual = new_qual;
                    }
                    if let Some(t) = new_tlist {
                        a.targetList = t;
                    }
                })
            }
            .expect("MergeAction");
        }
    }
    for wc_node in &q.windowClause {
        let wc = wc_node.as_window_clause().expect("windowClause cell");
        let new_start = fjav_opt(ctx, wc.startOffset)?;
        let new_end = fjav_opt(ctx, wc.endOffset)?;
        if new_start.is_some() || new_end.is_some() {
            // SAFETY: exclusive copy from fjav_query_descend.
            unsafe {
                wc_node.with_mut::<types_nodes::parsenodes::WindowClause, _>(|w| {
                    if new_start.is_some() {
                        w.startOffset = new_start;
                    }
                    if new_end.is_some() {
                        w.endOffset = new_end;
                    }
                })
            }
            .expect("WindowClause");
        }
    }
    let new_jointree = match q.jointree {
        None => None,
        Some(jt) => {
            let fl = fjav_list(ctx, &jt.fromlist)?;
            let quals = fjav_opt(ctx, jt.quals)?;
            if fl.is_some() || quals.is_some() {
                Some(mcx::alloc_leak_in(
                    mcx,
                    types_nodes::primnodes::FromExpr {
                        fromlist: match fl {
                            Some(l) => l,
                            None => jt.fromlist.clone_in(mcx)?,
                        },
                        quals: quals.or(jt.quals),
                    },
                )?)
            } else {
                None
            }
        }
    };
    for cte_node in &q.cteList {
        let cte = cte_node.as_common_table_expr().expect("cteList cell");
        if let Some(cq) = cte.ctequery {
            if fjav_query_has_join_alias_vars(ctx, cq.as_query().expect("Query"))? {
                fjav_query_inplace(ctx, cq)?;
            }
        }
    }
    for rte_node in &q.rtable {
        let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
        match rte.rtekind {
            types_nodes::parsenodes::RTEKind::RTE_RELATION => {
                if let Some(ts) = rte.tablesample {
                    if let Some(new) = fjav_mutate(ctx, ts)? {
                        // SAFETY: exclusive copy from fjav_query_descend.
                        unsafe {
                            rte_node.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                                r.tablesample = Some(new)
                            })
                        }
                        .expect("RangeTblEntry");
                    }
                }
            }
            types_nodes::parsenodes::RTEKind::RTE_SUBQUERY => {
                if let Some(sub) = rte.subquery {
                    if let Some(newsub) = fjav_query_descend(ctx, sub)? {
                        let newsub = newsub.as_query().expect("Query");
                        // SAFETY: exclusive copy from fjav_query_descend.
                        unsafe {
                            rte_node.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                                r.subquery = Some(newsub)
                            })
                        }
                        .expect("RangeTblEntry");
                    }
                }
            }
            types_nodes::parsenodes::RTEKind::RTE_FUNCTION => {
                if let Some(l) = fjav_list(ctx, &rte.functions)? {
                    // SAFETY: exclusive copy from fjav_query_descend.
                    unsafe {
                        rte_node.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                            r.functions = l
                        })
                    }
                    .expect("RangeTblEntry");
                }
            }
            types_nodes::parsenodes::RTEKind::RTE_TABLEFUNC => {
                if let Some(tf) = rte.tablefunc {
                    if let Some(new) = fjav_mutate(ctx, tf)? {
                        // SAFETY: exclusive copy from fjav_query_descend.
                        unsafe {
                            rte_node.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                                r.tablefunc = Some(new)
                            })
                        }
                        .expect("RangeTblEntry");
                    }
                }
            }
            types_nodes::parsenodes::RTEKind::RTE_VALUES => {
                if let Some(l) = fjav_list(ctx, &rte.values_lists)? {
                    // SAFETY: exclusive copy from fjav_query_descend.
                    unsafe {
                        rte_node.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                            r.values_lists = l
                        })
                    }
                    .expect("RangeTblEntry");
                }
            }
            types_nodes::parsenodes::RTEKind::RTE_GROUP => {
                // C range_table_mutator mutates groupexprs unless
                // QTW_IGNORE_GROUPEXPRS is set; flatten_join_alias_vars does
                // not set it (only QTW_IGNORE_JOINALIASES), so descend here.
                if let Some(l) = fjav_list(ctx, &rte.groupexprs)? {
                    // SAFETY: exclusive copy from fjav_query_descend.
                    unsafe {
                        rte_node.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                            r.groupexprs = l
                        })
                    }
                    .expect("RangeTblEntry");
                }
            }
            _ => {}
        }
        if let Some(l) = fjav_list(ctx, &rte.securityQuals)? {
            // SAFETY: exclusive copy from fjav_query_descend.
            unsafe {
                rte_node.with_mut::<types_nodes::parsenodes::RangeTblEntry, _>(|r| {
                    r.securityQuals = l
                })
            }
            .expect("RangeTblEntry");
        }
    }

    let inserted = ctx.inserted_sublink;
    ctx.inserted_sublink = save_inserted;
    ctx.sublevels_up -= 1;

    // SAFETY: qnode is the exclusive copy from fjav_query_descend.
    unsafe {
        qnode.with_mut::<Query, _>(|qm| {
            if let Some(t) = new_target {
                qm.targetList = t;
            }
            if let Some(r) = new_returning {
                qm.returningList = r;
            }
            if new_having.is_some() {
                qm.havingQual = new_having;
            }
            if new_limit_off.is_some() {
                qm.limitOffset = new_limit_off;
            }
            if new_limit_cnt.is_some() {
                qm.limitCount = new_limit_cnt;
            }
            if new_setops.is_some() {
                qm.setOperations = new_setops;
            }
            if new_merge_join.is_some() {
                qm.mergeJoinCondition = new_merge_join;
            }
            if let Some(jt) = new_jointree {
                qm.jointree = Some(jt);
            }
            qm.hasSubLinks |= inserted;
        })
    }
    .expect("Query");
    Ok(())
}

fn fjav_list<'mcx>(
    ctx: &mut FjavCtx<'_, 'mcx>,
    list: &NodeList<'mcx>,
) -> PgResult<Option<NodeList<'mcx>>> {
    let mut changed = false;
    let mut out = NodeList::nil();
    for item in list.iter() {
        match fjav_mutate(ctx, item)? {
            Some(new) => {
                changed = true;
                out.lappend(ctx.mcx, new)?;
            }
            None => out.lappend(ctx.mcx, item)?,
        }
    }
    Ok(if changed { Some(out) } else { None })
}

fn fjav_opt<'mcx>(
    ctx: &mut FjavCtx<'_, 'mcx>,
    node: Option<Node<'mcx>>,
) -> PgResult<Option<Node<'mcx>>> {
    match node {
        None => Ok(None),
        Some(n) => fjav_mutate(ctx, n),
    }
}

fn fjav_onconflict_inplace<'mcx>(
    ctx: &mut FjavCtx<'_, 'mcx>,
    oc_node: Node<'mcx>,
) -> PgResult<()> {
    let oc = oc_node.as_on_conflict_expr().expect("OnConflictExpr");
    let arbiter_elems = fjav_list(ctx, &oc.arbiterElems)?;
    let arbiter_where = fjav_opt(ctx, oc.arbiterWhere)?;
    let set = fjav_list(ctx, &oc.onConflictSet)?;
    let oc_where = fjav_opt(ctx, oc.onConflictWhere)?;
    let excl_tlist = fjav_list(ctx, &oc.exclRelTlist)?;
    // SAFETY: exclusive copy from fjav_query_descend.
    unsafe {
        oc_node.with_mut::<types_nodes::primnodes::OnConflictExpr, _>(|o| {
            if let Some(v) = arbiter_elems {
                o.arbiterElems = v;
            }
            if arbiter_where.is_some() {
                o.arbiterWhere = arbiter_where;
            }
            if let Some(v) = set {
                o.onConflictSet = v;
            }
            if oc_where.is_some() {
                o.onConflictWhere = oc_where;
            }
            if let Some(v) = excl_tlist {
                o.exclRelTlist = v;
            }
        })
    }
    .expect("OnConflictExpr");
    Ok(())
}

// alias_relid_set (var.c): JOIN members expand to their base + outer-join
// relids via get_relids_for_join (prepjointree.c:4300).
fn alias_relid_set<'mcx>(
    rtable: &NodeList<'mcx>,
    jointree: Option<&'mcx types_nodes::primnodes::FromExpr<'mcx>>,
    relids: &Bitmapset<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<Bitmapset<'mcx>> {
    let mut out = Bitmapset::empty();
    for rti in relids.iter() {
        let rte = rtable.nth(rti as usize - 1).as_range_tbl_entry().expect("rtable cell");
        if rte.rtekind == types_nodes::parsenodes::RTEKind::RTE_JOIN {
            let Some(jt) = jointree else {
                panic!(
                    "alias_relid_set (var.c): RTE_JOIN member on a caller without \
                     the query jointree (get_relids_for_join)"
                );
            };
            let mut jtnode = None;
            for child in &jt.fromlist {
                jtnode = find_jointree_node_for_rel(child, rti);
                if jtnode.is_some() {
                    break;
                }
            }
            let jtnode =
                jtnode.unwrap_or_else(|| panic!("could not find join node {rti}"));
            join_relids_no_inner(mcx, jtnode, &mut out)?;
        } else {
            out.add_member(mcx, rti)?;
        }
    }
    Ok(out)
}

// find_jointree_node_for_rel (prepjointree.c:4319).
fn find_jointree_node_for_rel<'mcx>(node: Node<'mcx>, relid: i32) -> Option<Node<'mcx>> {
    match node.node_tag() {
        NodeTag::T_RangeTblRef => {
            (node.as_range_tbl_ref().unwrap().rtindex == relid).then_some(node)
        }
        NodeTag::T_FromExpr => node
            .as_from_expr()
            .unwrap()
            .fromlist
            .iter()
            .find_map(|child| find_jointree_node_for_rel(child, relid)),
        NodeTag::T_JoinExpr => {
            let j = node.as_join_expr().unwrap();
            if j.rtindex == relid {
                return Some(node);
            }
            find_jointree_node_for_rel(j.larg, relid)
                .or_else(|| find_jointree_node_for_rel(j.rarg, relid))
        }
        other => panic!("find_jointree_node_for_rel (prepjointree.c): {other:?}"),
    }
}

// get_relids_in_jointree (prepjointree.c), include_outer_joins=true,
// include_inner_joins=false.
fn join_relids_no_inner<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    out: &mut Bitmapset<'mcx>,
) -> PgResult<()> {
    match node.node_tag() {
        NodeTag::T_RangeTblRef => {
            out.add_member(mcx, node.as_range_tbl_ref().unwrap().rtindex)?;
        }
        NodeTag::T_FromExpr => {
            for child in &node.as_from_expr().unwrap().fromlist {
                join_relids_no_inner(mcx, child, out)?;
            }
        }
        NodeTag::T_JoinExpr => {
            let j = node.as_join_expr().unwrap();
            join_relids_no_inner(mcx, j.larg, out)?;
            join_relids_no_inner(mcx, j.rarg, out)?;
            if j.rtindex != 0 && j.jointype != types_nodes::JoinType::JOIN_INNER {
                out.add_member(mcx, j.rtindex)?;
            }
        }
        other => panic!("get_relids_in_jointree (prepjointree.c): {other:?}"),
    }
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
        // Subquery pull-up rewrites joinaliasvars into arbitrary expressions
        // (var.c header comment); C copyObject covers them all.
        _ => copyfuncs::copy_object(mcx, node),
    }
}

// add_nullingrels_if_needed (var.c). Parser path (root=None) ereports on a
// non-standard alias; planner wraps it in a PlaceHolderVar.
fn unsupported_join_alias() -> Box<PgError> {
    Box::new(PgError::error("unsupported join alias expression".to_string()))
}

fn add_nullingrels_if_needed<'mcx>(
    ctx: &mut FjavCtx<'_, 'mcx>,
    newnode: Node<'mcx>,
    oldvar: &types_nodes::Var<'mcx>,
) -> PgResult<Node<'mcx>> {
    if oldvar.varnullingrels.is_empty() {
        return Ok(newnode);
    }
    if is_standard_join_alias_expression(newnode, oldvar) {
        adjust_standard_join_alias_expression(ctx.mcx, newnode, oldvar)?;
        return Ok(newnode);
    }
    let Some(root) = ctx.root else {
        return Err(unsupported_join_alias());
    };
    let mut phrels = pull_varnos_of_level(ctx.mcx, newnode, oldvar.varlevelsup as i32)?;
    if phrels.is_empty() {
        if oldvar.varlevelsup != 0 {
            return Err(unsupported_join_alias());
        }
        let Some(jt) = ctx.jointree else {
            return Err(unsupported_join_alias());
        };
        let mut jtnode = None;
        for child in &jt.fromlist {
            jtnode = find_jointree_node_for_rel(child, oldvar.varno);
            if jtnode.is_some() {
                break;
            }
        }
        let Some(jtnode) = jtnode else {
            return Err(unsupported_join_alias());
        };
        join_relids_no_inner(ctx.mcx, jtnode, &mut phrels)?;
        phrels.del_member(oldvar.varno);
        assert!(!phrels.is_empty());
    }
    root.last_ph_id.set(root.last_ph_id.get() + 1);
    Node::mk(
        ctx.mcx,
        types_nodes::primnodes::PlaceHolderVar {
            phexpr: newnode,
            phrels,
            phnullingrels: oldvar.varnullingrels.clone_in(ctx.mcx)?,
            phid: root.last_ph_id.get(),
            phlevelsup: oldvar.varlevelsup,
        },
    )
}

fn is_standard_join_alias_expression(newnode: Node<'_>, oldvar: &types_nodes::Var<'_>) -> bool {
    match newnode.node_tag() {
        NodeTag::T_Var => newnode.as_var().unwrap().varlevelsup == oldvar.varlevelsup,
        NodeTag::T_PlaceHolderVar => {
            newnode.as_place_holder_var().unwrap().phlevelsup == oldvar.varlevelsup
        }
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
        NodeTag::T_PlaceHolderVar
            if newnode.as_place_holder_var().unwrap().phlevelsup == oldvar.varlevelsup =>
        {
            // SAFETY: the PHV was freshly built by fjav_mutate's PHV arm.
            unsafe {
                newnode
                    .with_mut::<types_nodes::primnodes::PlaceHolderVar, _>(|phv| {
                        phv.phnullingrels.add_members(mcx, &oldvar.varnullingrels)
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

// flatten_group_exprs (var.c), root == NULL arm. Planner mark_nullable
// lives in planner/flatten_group.rs. Level-0 replacements are shared.
pub fn flatten_group_exprs<'mcx>(
    mcx: Mcx<'mcx>,
    query: &Query<'mcx>,
    node: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let mut ctx = FgeCtx { mcx, query, sublevels_up: 0 };
    Ok(fge_mutate(&mut ctx, node)?.unwrap_or(node))
}

/// None = unchanged (caller keeps the original list).
pub fn flatten_group_exprs_list<'mcx>(
    mcx: Mcx<'mcx>,
    query: &Query<'mcx>,
    list: &NodeList<'mcx>,
) -> PgResult<Option<&'mcx NodeList<'mcx>>> {
    let mut ctx = FgeCtx { mcx, query, sublevels_up: 0 };
    match fge_list(&mut ctx, list)? {
        None => Ok(None),
        Some(new) => Ok(Some(
            Node::mk_list(mcx, new)?.as_list().expect("mk_list yields a List"),
        )),
    }
}

struct FgeCtx<'a, 'mcx> {
    mcx: Mcx<'mcx>,
    /// The query whose GROUP RTE is being flattened; Var lookups always
    /// resolve against this level's rtable (C keeps context->query fixed).
    query: &'a Query<'mcx>,
    sublevels_up: i32,
}

fn fge_list<'mcx>(
    ctx: &mut FgeCtx<'_, 'mcx>,
    list: &NodeList<'mcx>,
) -> PgResult<Option<NodeList<'mcx>>> {
    let mut changed = false;
    let mut out: Vec<Node<'mcx>> = Vec::with_capacity(list.len());
    for item in list.iter() {
        match fge_mutate(ctx, item)? {
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
        l.lappend(ctx.mcx, n)?;
    }
    Ok(Some(l))
}

fn fge_opt<'mcx>(
    ctx: &mut FgeCtx<'_, 'mcx>,
    node: Option<Node<'mcx>>,
) -> PgResult<Option<Node<'mcx>>> {
    match node {
        None => Ok(None),
        Some(n) => fge_mutate(ctx, n),
    }
}

// flatten_group_exprs_mutator (var.c:993-1101); None = unchanged.
fn fge_mutate<'mcx>(
    ctx: &mut FgeCtx<'_, 'mcx>,
    node: Node<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    use types_nodes::primnodes as pn;
    use types_nodes::RTEKind;
    let mcx = ctx.mcx;
    match node.node_tag() {
        NodeTag::T_Var => {
            let var = node.as_var().unwrap();
            if var.varlevelsup as i32 != ctx.sublevels_up {
                return Ok(None);
            }
            let rte = ctx
                .query
                .rtable
                .nth(var.varno as usize - 1)
                .as_range_tbl_entry()
                .expect("rtable entry");
            if rte.rtekind != RTEKind::RTE_GROUP {
                return Ok(None);
            }
            debug_assert!(var.varattno > 0);
            let newvar = rte.groupexprs.nth(var.varattno as usize - 1);
            if ctx.sublevels_up != 0 {
                // The replacement lands inside a subquery, so its variables
                // move down with it; the copy keeps the level shift off the
                // shared groupexprs entry.
                let copy = rewrite_manip::copy_node(mcx, newvar)?;
                rewrite_manip::IncrementVarSublevelsUp(copy, ctx.sublevels_up, 0)?;
                if copy.node_tag() == NodeTag::T_Var {
                    let location = var.location;
                    // SAFETY: copy is the fresh exclusive tree made above.
                    unsafe { copy.with_mut::<pn::Var, _>(|v| v.location = location) }
                        .expect("Var");
                }
                return Ok(Some(copy));
            }
            // C preserves the original Var's location on a Var replacement.
            if let Some(v) = newvar.as_var() {
                if v.location != var.location {
                    return Ok(Some(Node::mk(
                        mcx,
                        pn::Var {
                            varnullingrels: v.varnullingrels.clone_in(mcx)?,
                            location: var.location,
                            ..*v
                        },
                    )?));
                }
            }
            Ok(Some(newvar))
        }
        // C: a GroupingFunc of the flatten level or higher holds no grouped
        // Vars of this level; a lower one may hold them in its refs' exprs.
        NodeTag::T_GroupingFunc => {
            if node.as_grouping_func().unwrap().agglevelsup as i32 >= ctx.sublevels_up {
                Ok(None)
            } else {
                nodes_core::expression_tree_mutator(mcx, node, &mut |n| fge_mutate(ctx, n))
            }
        }
        NodeTag::T_Aggref => {
            let a = node.as_aggref().unwrap();
            let agglevelsup = a.agglevelsup as i32;
            if agglevelsup > ctx.sublevels_up {
                // Higher-level agg: no grouped Vars of this level inside (C
                // skips recursing into aggregates of higher levels).
                return Ok(None);
            }
            if agglevelsup < ctx.sublevels_up {
                return nodes_core::expression_tree_mutator(mcx, node, &mut |n| {
                    fge_mutate(ctx, n)
                });
            }
            // C: at the agg's own level only aggdirectargs can hold grouped
            // Vars; args/order/filter are not recursed into.
            match fge_list(ctx, &a.aggdirectargs)? {
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
        // nodes_core mutator skips SubLink.subselect; C mutates both.
        NodeTag::T_SubLink => {
            let sl = node.as_sub_link().unwrap();
            let new_test = match sl.testexpr {
                None => None,
                Some(te) => fge_mutate(ctx, te)?,
            };
            let new_sub =
                fge_query_descend(ctx, sl.subselect.as_query().expect("SubLink holds a Query"))?;
            if new_test.is_none() && new_sub.is_none() {
                return Ok(None);
            }
            Ok(Some(Node::mk(
                mcx,
                pn::SubLink {
                    subLinkType: sl.subLinkType,
                    subLinkId: sl.subLinkId,
                    testexpr: new_test.or(sl.testexpr),
                    operName: sl.operName.clone_in(mcx)?,
                    subselect: new_sub.unwrap_or(sl.subselect),
                    location: sl.location,
                },
            )?))
        }
        NodeTag::T_Query => {
            fge_query_descend(ctx, node.as_query().expect("Query"))
        }
        _ => nodes_core::expression_tree_mutator(mcx, node, &mut |n| fge_mutate(ctx, n)),
    }
}

// Query descent: copy once, walk in place. None = unchanged.
fn fge_query_descend<'mcx>(
    ctx: &mut FgeCtx<'_, 'mcx>,
    q: &'mcx Query<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    if !fge_query_has_grouped_vars(ctx, q)? {
        return Ok(None);
    }
    let qnode = rewrite_manip::copy_query_node(ctx.mcx, q)?;
    fge_query_inplace(ctx, qnode)?;
    Ok(Some(qnode))
}

fn fge_query_has_grouped_vars<'mcx>(
    ctx: &FgeCtx<'_, 'mcx>,
    q: &'mcx Query<'mcx>,
) -> PgResult<bool> {
    use types_nodes::parsenodes::RTEKind;
    struct W<'a, 'x> {
        parse: &'a Query<'x>,
        sublevels_up: i32,
        found: bool,
    }
    impl<'mcx> NodeWalker<'mcx> for W<'_, 'mcx> {
        fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
            if let Some(v) = node.as_var() {
                if v.varlevelsup as i32 == self.sublevels_up {
                    let rte = self
                        .parse
                        .rtable
                        .nth(v.varno as usize - 1)
                        .as_range_tbl_entry()
                        .expect("rtable entry");
                    if rte.rtekind == RTEKind::RTE_GROUP {
                        self.found = true;
                        return Ok(true);
                    }
                }
                return Ok(false);
            }
            if let Some(q) = node.as_query() {
                self.sublevels_up += 1;
                let r = query_tree_walker(q, self, 0);
                self.sublevels_up -= 1;
                return r;
            }
            expression_tree_walker(node, self)
        }
        fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
            self.sublevels_up += 1;
            let r = query_tree_walker(q, self, 0);
            self.sublevels_up -= 1;
            r
        }
    }
    let mut w = W { parse: ctx.query, sublevels_up: ctx.sublevels_up + 1, found: false };
    query_tree_walker(q, &mut w, 0)?;
    Ok(w.found)
}

fn fge_query_inplace<'mcx>(ctx: &mut FgeCtx<'_, 'mcx>, qnode: Node<'mcx>) -> PgResult<()> {
    use types_nodes::parsenodes::{RTEKind, RangeTblEntry};
    let mcx = ctx.mcx;
    let q = qnode.as_query().expect("Query");
    ctx.sublevels_up += 1;

    let new_target = fge_list(ctx, &q.targetList)?;
    let new_returning = fge_list(ctx, &q.returningList)?;
    let new_having = fge_opt(ctx, q.havingQual)?;
    let new_limit_off = fge_opt(ctx, q.limitOffset)?;
    let new_limit_cnt = fge_opt(ctx, q.limitCount)?;
    let new_setops = fge_opt(ctx, q.setOperations)?;
    let new_merge_join = fge_opt(ctx, q.mergeJoinCondition)?;
    if let Some(oc_node) = q.onConflict {
        let oc = oc_node.as_on_conflict_expr().expect("OnConflictExpr");
        let arbiter_elems = fge_list(ctx, &oc.arbiterElems)?;
        let arbiter_where = fge_opt(ctx, oc.arbiterWhere)?;
        let set = fge_list(ctx, &oc.onConflictSet)?;
        let oc_where = fge_opt(ctx, oc.onConflictWhere)?;
        let excl_tlist = fge_list(ctx, &oc.exclRelTlist)?;
        // SAFETY: exclusive copy from fge_query_descend.
        unsafe {
            oc_node.with_mut::<types_nodes::primnodes::OnConflictExpr, _>(|o| {
                if let Some(v) = arbiter_elems {
                    o.arbiterElems = v;
                }
                if arbiter_where.is_some() {
                    o.arbiterWhere = arbiter_where;
                }
                if let Some(v) = set {
                    o.onConflictSet = v;
                }
                if oc_where.is_some() {
                    o.onConflictWhere = oc_where;
                }
                if let Some(v) = excl_tlist {
                    o.exclRelTlist = v;
                }
            })
        }
        .expect("OnConflictExpr");
    }
    for wco_node in &q.withCheckOptions {
        let wco = wco_node.as_with_check_option().expect("withCheckOptions cell");
        if let Some(new_qual) = fge_opt(ctx, wco.qual)? {
            // SAFETY: exclusive copy from fge_query_descend.
            unsafe {
                wco_node.with_mut::<types_nodes::parsenodes::WithCheckOption, _>(|w| {
                    w.qual = Some(new_qual)
                })
            }
            .expect("WithCheckOption");
        }
    }
    for action_node in &q.mergeActionList {
        let action = action_node.as_merge_action().expect("mergeActionList cell");
        let new_qual = fge_opt(ctx, action.qual)?;
        let new_tlist = fge_list(ctx, &action.targetList)?;
        if new_qual.is_some() || new_tlist.is_some() {
            // SAFETY: exclusive copy from fge_query_descend.
            unsafe {
                action_node.with_mut::<types_nodes::MergeAction, _>(|a| {
                    if new_qual.is_some() {
                        a.qual = new_qual;
                    }
                    if let Some(t) = new_tlist {
                        a.targetList = t;
                    }
                })
            }
            .expect("MergeAction");
        }
    }
    for wc_node in &q.windowClause {
        let wc = wc_node.as_window_clause().expect("windowClause cell");
        let new_start = fge_opt(ctx, wc.startOffset)?;
        let new_end = fge_opt(ctx, wc.endOffset)?;
        if new_start.is_some() || new_end.is_some() {
            // SAFETY: exclusive copy from fge_query_descend.
            unsafe {
                wc_node.with_mut::<types_nodes::parsenodes::WindowClause, _>(|w| {
                    if new_start.is_some() {
                        w.startOffset = new_start;
                    }
                    if new_end.is_some() {
                        w.endOffset = new_end;
                    }
                })
            }
            .expect("WindowClause");
        }
    }
    let new_jointree = match q.jointree {
        None => None,
        Some(jt) => {
            let fl = fge_list(ctx, &jt.fromlist)?;
            let quals = fge_opt(ctx, jt.quals)?;
            if fl.is_some() || quals.is_some() {
                Some(mcx::alloc_leak_in(
                    mcx,
                    types_nodes::primnodes::FromExpr {
                        fromlist: match fl {
                            Some(l) => l,
                            None => jt.fromlist.clone_in(mcx)?,
                        },
                        quals: quals.or(jt.quals),
                    },
                )?)
            } else {
                None
            }
        }
    };
    for cte_node in &q.cteList {
        let cte = cte_node.as_common_table_expr().expect("cteList cell");
        if let Some(cq) = cte.ctequery {
            debug_assert!(cq.node_tag() == NodeTag::T_Query);
            // Part of the exclusive copy already: walk it in place.
            if fge_query_has_grouped_vars(ctx, cq.as_query().expect("Query"))? {
                fge_query_inplace(ctx, cq)?;
            }
        }
    }
    for rte_node in &q.rtable {
        let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
        match rte.rtekind {
            RTEKind::RTE_SUBQUERY => {
                if let Some(sub) = rte.subquery {
                    if let Some(newsub) = fge_query_descend(ctx, sub)? {
                        let newsub = newsub.as_query().expect("Query");
                        // SAFETY: rte_node is part of the exclusive copy.
                        unsafe {
                            rte_node
                                .with_mut::<RangeTblEntry, _>(|r| r.subquery = Some(newsub))
                        }
                        .expect("RangeTblEntry");
                    }
                }
            }
            RTEKind::RTE_FUNCTION => {
                if let Some(l) = fge_list(ctx, &rte.functions)? {
                    // SAFETY: as above.
                    unsafe { rte_node.with_mut::<RangeTblEntry, _>(|r| r.functions = l) }
                        .expect("RangeTblEntry");
                }
            }
            RTEKind::RTE_TABLEFUNC => {
                if let Some(tf) = rte.tablefunc {
                    if let Some(new) = fge_mutate(ctx, tf)? {
                        // SAFETY: as above.
                        unsafe {
                            rte_node.with_mut::<RangeTblEntry, _>(|r| r.tablefunc = Some(new))
                        }
                        .expect("RangeTblEntry");
                    }
                }
            }
            RTEKind::RTE_VALUES => {
                if let Some(l) = fge_list(ctx, &rte.values_lists)? {
                    // SAFETY: as above.
                    unsafe { rte_node.with_mut::<RangeTblEntry, _>(|r| r.values_lists = l) }
                        .expect("RangeTblEntry");
                }
            }
            // QTW_IGNORE_GROUPEXPRS: nested GROUP RTEs keep their exprs.
            _ => {}
        }
        if let Some(l) = fge_list(ctx, &rte.securityQuals)? {
            // SAFETY: as above.
            unsafe { rte_node.with_mut::<RangeTblEntry, _>(|r| r.securityQuals = l) }
                .expect("RangeTblEntry");
        }
    }

    ctx.sublevels_up -= 1;

    if new_target.is_some()
        || new_returning.is_some()
        || new_having.is_some()
        || new_limit_off.is_some()
        || new_limit_cnt.is_some()
        || new_setops.is_some()
        || new_merge_join.is_some()
        || new_jointree.is_some()
    {
        // SAFETY: qnode is the exclusive copy made by fge_query_descend.
        unsafe {
            qnode.with_mut::<Query, _>(|qm| {
                if let Some(t) = new_target {
                    qm.targetList = t;
                }
                if let Some(r) = new_returning {
                    qm.returningList = r;
                }
                if new_having.is_some() {
                    qm.havingQual = new_having;
                }
                if new_limit_off.is_some() {
                    qm.limitOffset = new_limit_off;
                }
                if new_limit_cnt.is_some() {
                    qm.limitCount = new_limit_cnt;
                }
                if new_setops.is_some() {
                    qm.setOperations = new_setops;
                }
                if new_merge_join.is_some() {
                    qm.mergeJoinCondition = new_merge_join;
                }
                if let Some(jt) = new_jointree {
                    qm.jointree = Some(jt);
                }
            })
        }
        .expect("Query");
    }
    Ok(())
}

struct ContainNoopPhv {
    found: bool,
}

impl<'mcx> NodeWalker<'mcx> for ContainNoopPhv {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        if let Some(phv) = node.as_place_holder_var() {
            if phv.phnullingrels.is_empty() {
                self.found = true;
                return Ok(true);
            }
        }
        expression_tree_walker(node, self)
    }
}

fn strip_noop_phvs_mutator<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    if let Some(phv) = node.as_place_holder_var() {
        if phv.phnullingrels.is_empty() {
            return Ok(Some(
                strip_noop_phvs_mutator(mcx, phv.phexpr)?.unwrap_or(phv.phexpr),
            ));
        }
    }
    nodes_core::expression_tree_mutator(mcx, node, &mut |n| strip_noop_phvs_mutator(mcx, n))
}

/// strip_noop_phvs (placeholder.c): remove PlaceHolderVars whose
/// phnullingrels is empty — no-ops in a scan-level expression (the caller
/// guarantees scan level). Walker-gated so the common no-PHV case does not
/// copy.
pub fn strip_noop_phvs<'mcx>(mcx: Mcx<'mcx>, node: Node<'mcx>) -> PgResult<Node<'mcx>> {
    let mut cx = ContainNoopPhv { found: false };
    cx.visit(node)?;
    if !cx.found {
        return Ok(node);
    }
    Ok(strip_noop_phvs_mutator(mcx, node)?.unwrap_or(node))
}
