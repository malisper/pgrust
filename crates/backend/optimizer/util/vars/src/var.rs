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

// PVC flags are consulted only by the Aggref/WindowFunc/PHV arms, all
// deferred with their payloads; the struct carries no flags until then.
struct PullVarClause<'mcx> {
    mcx: Mcx<'mcx>,
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
            // INCLUDE/RECURSE/error all need these payloads (levelsup
            // checks, argument lists) to stay faithful — deferred together.
            t @ (NodeTag::T_Aggref
            | NodeTag::T_GroupingFunc
            | NodeTag::T_WindowFunc
            | NodeTag::T_PlaceHolderVar) => deferred("pull_var_clause_walker", t),
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
    let _ = flags;
    let mut cx = PullVarClause { mcx, varlist: NodeList::nil() };
    cx.visit(node)?;
    Ok(cx.varlist)
}

pub fn flatten_join_alias_vars<'mcx>(_query: &Query<'mcx>, _node: Node<'mcx>) -> ! {
    panic!("flatten_join_alias_vars deferred: RowExpr/PlaceHolderVar vocabulary unported");
}

pub fn flatten_group_exprs<'mcx>(_query: &Query<'mcx>, _node: Node<'mcx>) -> ! {
    panic!("flatten_group_exprs deferred: Aggref/GroupingFunc vocabulary unported");
}
