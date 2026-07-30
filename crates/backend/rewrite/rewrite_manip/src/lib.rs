// In-place walker contract: every mutating entry point here requires the
// exclusive-ownership C guarantees by copyObject-before-scribble — callers
// pass freshly read (stringToNode) or statement-owned trees only.
#![allow(non_snake_case, non_upper_case_globals)]

use mcx::Mcx;
use types_core::AttrNumber;
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use types_nodes::parsenodes::{Query, RTEKind, RangeTblEntry, RowMarkClause};
use types_nodes::primnodes::{
    Aggref, BoolExpr, BoolExprType, BoolTestType, BooleanTest, FromExpr, GroupingFunc,
    PlaceHolderVar, TargetEntry, Var, VarReturningType,
};
use types_nodes::{Node, NodeList, NodeTag};

#[cfg(test)]
mod tests;

pub const PRS2_OLD_VARNO: i32 = 1;
pub const PRS2_NEW_VARNO: i32 = 2;

pub fn map_variable_attnos<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    target_varno: i32,
    sublevels_up: u32,
    attnums: &[AttrNumber],
    to_rowtype: types_core::Oid,
) -> PgResult<(Node<'mcx>, bool)> {
    let mut found_whole_row = false;
    let mapped = mva_mutate(
        mcx,
        node,
        target_varno,
        sublevels_up,
        attnums,
        to_rowtype,
        &mut found_whole_row,
    )?
    .unwrap_or(node);
    Ok((mapped, found_whole_row))
}

fn mva_mutate<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    target_varno: i32,
    sublevels_up: u32,
    attnums: &[AttrNumber],
    to_rowtype: types_core::Oid,
    found_whole_row: &mut bool,
) -> PgResult<Option<Node<'mcx>>> {
    if node.node_tag() == NodeTag::T_Var {
        let var = node.as_variant::<Var>().expect("Var");
        if var.varno == target_varno && var.varlevelsup == sublevels_up {
            let attno = var.varattno;
            if attno > 0 {
                if attno as usize > attnums.len() || attnums[attno as usize - 1] == 0 {
                    panic!("unexpected varattno {attno} in expression to be mapped");
                }
                let mut newvar = Var {
                    varnullingrels: var.varnullingrels.clone_in(mcx)?,
                    ..*var
                };
                newvar.varattno = attnums[attno as usize - 1];
                if newvar.varnosyn == target_varno as u32 {
                    newvar.varattnosyn = newvar.varattno;
                }
                return Ok(Some(Node::mk(mcx, newvar)?));
            }
            if attno == 0 {
                *found_whole_row = true;
                if to_rowtype != types_core::InvalidOid && to_rowtype != var.vartype {
                    let mut newvar = Var {
                        varnullingrels: var.varnullingrels.clone_in(mcx)?,
                        ..*var
                    };
                    newvar.vartype = to_rowtype;
                    let cre = types_nodes::primnodes::ConvertRowtypeExpr {
                        arg: Node::mk(mcx, newvar)?,
                        resulttype: var.vartype,
                        convertformat: types_nodes::primnodes::CoercionForm::COERCE_IMPLICIT_CAST,
                        location: -1,
                    };
                    return Ok(Some(Node::mk(mcx, cre)?));
                }
            }
            // attno < 0 (system column): C copies the Var unchanged.
            return Ok(None);
        }
        return Ok(None);
    }
    if node.node_tag() == NodeTag::T_ConvertRowtypeExpr {
        // Collapse var::parenttype::grandparenttype to var::grandparenttype
        // instead of stacking ConvertRowtypeExprs.
        let r = node
            .as_variant::<types_nodes::primnodes::ConvertRowtypeExpr>()
            .expect("ConvertRowtypeExpr");
        if let Some(var) = r.arg.as_variant::<Var>() {
            if var.varno == target_varno
                && var.varlevelsup == sublevels_up
                && var.varattno == 0
                && to_rowtype != types_core::InvalidOid
                && to_rowtype != var.vartype
            {
                *found_whole_row = true;
                let mut newvar = Var {
                    varnullingrels: var.varnullingrels.clone_in(mcx)?,
                    ..*var
                };
                newvar.vartype = to_rowtype;
                let newnode = types_nodes::primnodes::ConvertRowtypeExpr {
                    arg: Node::mk(mcx, newvar)?,
                    resulttype: r.resulttype,
                    convertformat: r.convertformat,
                    location: r.location,
                };
                return Ok(Some(Node::mk(mcx, newnode)?));
            }
        }
    }
    if node.node_tag() == NodeTag::T_SubLink {
        // nodes_core's SubLink arm skips the subselect C recurses into.
        panic!("unported: map_variable_attnos over SubLink (Query walk)");
    }
    let mut m = |n: Node<'mcx>| {
        mva_mutate(mcx, n, target_varno, sublevels_up, attnums, to_rowtype, found_whole_row)
    };
    nodes_core::expression_tree_mutator(mcx, node, &mut m)
}

// copyObject: serialize/deserialize round trip — the only generic deep copy
// this vocabulary has; rule firing is per-statement on rule-bearing tables
// only, and both halves panic loudly on unported arms.
pub fn copy_node<'mcx>(mcx: Mcx<'mcx>, node: Node<'mcx>) -> PgResult<Node<'mcx>> {
    let s = outfuncs::nodeToString(mcx, node)?;
    readfuncs::stringToNode(mcx, s.as_str())
}

pub fn copy_node_list<'mcx>(mcx: Mcx<'mcx>, list: &NodeList<'mcx>) -> PgResult<NodeList<'mcx>> {
    if list.is_nil() {
        return Ok(NodeList::nil());
    }
    let copied = copy_node(mcx, Node::mk_list(mcx, list.clone_in(mcx)?)?)?;
    Ok(copied.as_list().expect("List round trip").clone_in(mcx)?)
}

pub fn copy_query_node<'mcx>(mcx: Mcx<'mcx>, q: &Query<'_>) -> PgResult<Node<'mcx>> {
    let s = outfuncs::queryToString(mcx, q)?;
    readfuncs::stringToNode(mcx, s.as_str())
}

// CombineRangeTables (rewriteManip.c). C scribbles src RTEs' perminfoindex;
// same here (src comes from the caller's fresh rule-action tree).
pub fn CombineRangeTables<'mcx>(
    mcx: Mcx<'mcx>,
    dst_rtable: &mut NodeList<'mcx>,
    dst_perminfos: &mut NodeList<'mcx>,
    src_rtable: &NodeList<'mcx>,
    src_perminfos: &NodeList<'mcx>,
) -> PgResult<()> {
    let offset = dst_perminfos.len() as u32;
    if offset > 0 {
        for rte_node in src_rtable {
            // SAFETY: src tree is exclusively owned (module contract).
            unsafe {
                rte_node.with_mut::<RangeTblEntry, _>(|r| {
                    if r.perminfoindex > 0 {
                        r.perminfoindex += offset;
                    }
                })
            }
            .expect("rtable holds RangeTblEntry nodes");
        }
    }
    dst_perminfos.concat(mcx, src_perminfos)?;
    dst_rtable.concat(mcx, src_rtable)?;
    Ok(())
}

struct OffsetVars<'mcx> {
    mcx: Mcx<'mcx>,
    offset: i32,
    sublevels_up: u32,
}

impl<'mcx> nodes_core::NodeWalker<'mcx> for OffsetVars<'mcx> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        match node.node_tag() {
            NodeTag::T_Var => {
                let (sup, offset, mcx) = (self.sublevels_up, self.offset, self.mcx);
                // SAFETY: exclusive tree (module contract); no derived ref live.
                unsafe {
                    node.with_mut::<Var, _>(|v| -> PgResult<()> {
                        if v.varlevelsup == sup {
                            v.varno += offset;
                            v.varnullingrels = offset_relid_set(mcx, &v.varnullingrels, offset)?;
                            if v.varnosyn > 0 {
                                v.varnosyn += offset as u32;
                            }
                        }
                        Ok(())
                    })
                }
                .expect("Var")?;
                Ok(false)
            }
            NodeTag::T_PlaceHolderVar => {
                let (sup, offset, mcx) = (self.sublevels_up, self.offset, self.mcx);
                // SAFETY: as above.
                unsafe {
                    node.with_mut::<PlaceHolderVar, _>(|p| -> PgResult<()> {
                        if p.phlevelsup == sup {
                            p.phrels = offset_relid_set(mcx, &p.phrels, offset)?;
                            p.phnullingrels = offset_relid_set(mcx, &p.phnullingrels, offset)?;
                        }
                        Ok(())
                    })
                }
                .expect("PlaceHolderVar")?;
                nodes_core::expression_tree_walker(node, self)
            }
            NodeTag::T_CurrentOfExpr => {
                if self.sublevels_up == 0 {
                    let offset = self.offset;
                    // SAFETY: as above.
                    unsafe {
                        node.with_mut::<types_nodes::CurrentOfExpr, _>(|c| {
                            c.cvarno = (c.cvarno as i32 + offset) as u32;
                        })
                    }
                    .expect("CurrentOfExpr");
                }
                Ok(false)
            }
            NodeTag::T_RangeTblRef => {
                if self.sublevels_up == 0 {
                    let offset = self.offset;
                    // SAFETY: as above.
                    unsafe {
                        node.with_mut::<types_nodes::primnodes::RangeTblRef, _>(|r| {
                            r.rtindex += offset;
                        })
                    }
                    .expect("RangeTblRef");
                }
                Ok(false)
            }
            NodeTag::T_JoinExpr => {
                if self.sublevels_up == 0 {
                    let offset = self.offset;
                    // SAFETY: as above.
                    unsafe {
                        node.with_mut::<types_nodes::JoinExpr, _>(|j| {
                            if j.rtindex != 0 {
                                j.rtindex += offset;
                            }
                        })
                    }
                    .expect("JoinExpr");
                }
                nodes_core::expression_tree_walker(node, self)
            }
            NodeTag::T_Query => {
                self.sublevels_up += 1;
                let r =
                    nodes_core::query_tree_walker(node.as_query().expect("Query"), self, 0)?;
                self.sublevels_up -= 1;
                Ok(r)
            }
            _ => nodes_core::expression_tree_walker(node, self),
        }
    }

    fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
        self.sublevels_up += 1;
        let r = nodes_core::query_tree_walker(q, self, 0)?;
        self.sublevels_up -= 1;
        Ok(r)
    }
}

fn offset_relid_set<'mcx>(
    mcx: Mcx<'mcx>,
    relids: &types_nodes::Bitmapset<'mcx>,
    offset: i32,
) -> PgResult<types_nodes::Bitmapset<'mcx>> {
    if relids.is_empty() {
        return Ok(types_nodes::Bitmapset::empty());
    }
    let mut s = types_nodes::Bitmapset::empty();
    for m in relids.iter() {
        s.add_member(mcx, m + offset)?;
    }
    Ok(s)
}

pub fn OffsetVarNodes<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    offset: i32,
    sublevels_up: u32,
) -> PgResult<()> {
    let mut w = OffsetVars { mcx, offset, sublevels_up };
    if node.node_tag() == NodeTag::T_Query {
        if sublevels_up == 0 {
            // SAFETY: exclusive tree (module contract).
            unsafe {
                node.with_mut::<Query, _>(|q| {
                    if q.resultRelation != 0 {
                        q.resultRelation += offset;
                    }
                    if q.mergeTargetRelation != 0 {
                        q.mergeTargetRelation += offset;
                    }
                })
            }
            .expect("Query");
            let q = node.as_query().expect("Query");
            if let Some(oc) = q.onConflict {
                // SAFETY: as above.
                unsafe {
                    oc.with_mut::<types_nodes::primnodes::OnConflictExpr, _>(|o| {
                        if o.exclRelIndex != 0 {
                            o.exclRelIndex += offset;
                        }
                    })
                }
                .expect("OnConflictExpr");
            }
            for rc in &q.rowMarks {
                // SAFETY: as above.
                unsafe { rc.with_mut::<RowMarkClause, _>(|r| r.rti += offset as u32) }
                    .expect("rowMarks holds RowMarkClause");
            }
        }
        use nodes_core::NodeWalker as _;
        nodes_core::query_tree_walker(node.as_query().expect("Query"), &mut w, 0)?;
    } else {
        use nodes_core::NodeWalker as _;
        w.visit(node)?;
    }
    Ok(())
}

struct ChangeVars<'mcx> {
    mcx: Mcx<'mcx>,
    rt_index: i32,
    new_index: i32,
    sublevels_up: u32,
    /// replace_relid_callback (analyzejoins.c) RangeTblRef arm: SJE leaves
    /// RangeTblRefs untouched so remove_rel_from_joinlist still finds them.
    /// (Its RestrictInfo arm lives planner-side; RestrictInfos are not
    /// expression-tree nodes here.)
    skip_rangetblref: bool,
}

impl<'mcx> nodes_core::NodeWalker<'mcx> for ChangeVars<'mcx> {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        match node.node_tag() {
            NodeTag::T_Var => {
                let (sup, rt_index, new_index, mcx) =
                    (self.sublevels_up, self.rt_index, self.new_index, self.mcx);
                // SAFETY: exclusive tree (module contract); no derived ref live.
                unsafe {
                    node.with_mut::<Var, _>(|v| -> PgResult<()> {
                        if v.varlevelsup == sup {
                            if v.varno == rt_index {
                                v.varno = new_index;
                            }
                            v.varnullingrels =
                                adjust_relid_set(mcx, &v.varnullingrels, rt_index, new_index)?;
                            if v.varnosyn == rt_index as u32 {
                                v.varnosyn = new_index as u32;
                            }
                        }
                        Ok(())
                    })
                }
                .expect("Var")?;
                Ok(false)
            }
            NodeTag::T_PlaceHolderVar => {
                let (sup, rt_index, new_index, mcx) =
                    (self.sublevels_up, self.rt_index, self.new_index, self.mcx);
                // SAFETY: as above.
                unsafe {
                    node.with_mut::<PlaceHolderVar, _>(|p| -> PgResult<()> {
                        if p.phlevelsup == sup {
                            p.phrels = adjust_relid_set(mcx, &p.phrels, rt_index, new_index)?;
                            p.phnullingrels =
                                adjust_relid_set(mcx, &p.phnullingrels, rt_index, new_index)?;
                        }
                        Ok(())
                    })
                }
                .expect("PlaceHolderVar")?;
                return nodes_core::expression_tree_walker(node, self);
            }
            NodeTag::T_CurrentOfExpr => {
                if self.sublevels_up == 0 {
                    let (rt_index, new_index) = (self.rt_index, self.new_index);
                    // SAFETY: as above.
                    unsafe {
                        node.with_mut::<types_nodes::CurrentOfExpr, _>(|c| {
                            if c.cvarno == rt_index as u32 {
                                c.cvarno = new_index as u32;
                            }
                        })
                    }
                    .expect("CurrentOfExpr");
                }
                Ok(false)
            }
            NodeTag::T_RangeTblRef => {
                if self.skip_rangetblref {
                    return Ok(false);
                }
                if self.sublevels_up == 0 {
                    let (rt_index, new_index) = (self.rt_index, self.new_index);
                    // SAFETY: as above.
                    unsafe {
                        node.with_mut::<types_nodes::primnodes::RangeTblRef, _>(|r| {
                            if r.rtindex == rt_index {
                                r.rtindex = new_index;
                            }
                        })
                    }
                    .expect("RangeTblRef");
                }
                Ok(false)
            }
            NodeTag::T_JoinExpr => {
                if self.sublevels_up == 0 {
                    let (rt_index, new_index) = (self.rt_index, self.new_index);
                    // SAFETY: as above.
                    unsafe {
                        node.with_mut::<types_nodes::JoinExpr, _>(|j| {
                            if j.rtindex == rt_index {
                                j.rtindex = new_index;
                            }
                        })
                    }
                    .expect("JoinExpr");
                }
                nodes_core::expression_tree_walker(node, self)
            }
            NodeTag::T_Query => {
                self.sublevels_up += 1;
                let r =
                    nodes_core::query_tree_walker(node.as_query().expect("Query"), self, 0)?;
                self.sublevels_up -= 1;
                Ok(r)
            }
            _ => nodes_core::expression_tree_walker(node, self),
        }
    }

    fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
        self.sublevels_up += 1;
        let r = nodes_core::query_tree_walker(q, self, 0)?;
        self.sublevels_up -= 1;
        Ok(r)
    }
}

const IS_SPECIAL_VARNO_MAX: i32 = 0;

fn adjust_relid_set<'mcx>(
    mcx: Mcx<'mcx>,
    relids: &types_nodes::Bitmapset<'mcx>,
    oldrelid: i32,
    newrelid: i32,
) -> PgResult<types_nodes::Bitmapset<'mcx>> {
    // IS_SPECIAL_VARNO(v) == v <= 0 for the varno arms reachable here
    // (INNER/OUTER/ROWID special varnos are negative in C 18).
    if oldrelid > IS_SPECIAL_VARNO_MAX && relids.is_member(oldrelid) {
        let mut s = types_nodes::Bitmapset::empty();
        for m in relids.iter() {
            if m != oldrelid {
                s.add_member(mcx, m)?;
            }
        }
        if newrelid > IS_SPECIAL_VARNO_MAX {
            s.add_member(mcx, newrelid)?;
        }
        return Ok(s);
    }
    relids.clone_in(mcx)
}

pub fn ChangeVarNodes<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    rt_index: i32,
    new_index: i32,
    sublevels_up: u32,
) -> PgResult<()> {
    let mut w = ChangeVars { mcx, rt_index, new_index, sublevels_up, skip_rangetblref: false };
    if node.node_tag() == NodeTag::T_Query {
        if sublevels_up == 0 {
            // SAFETY: exclusive tree (module contract).
            unsafe {
                node.with_mut::<Query, _>(|q| {
                    if q.resultRelation == rt_index {
                        q.resultRelation = new_index;
                    }
                    if q.mergeTargetRelation == rt_index {
                        q.mergeTargetRelation = new_index;
                    }
                })
            }
            .expect("Query");
            let q = node.as_query().expect("Query");
            if let Some(oc) = q.onConflict {
                // SAFETY: as above.
                unsafe {
                    oc.with_mut::<types_nodes::primnodes::OnConflictExpr, _>(|o| {
                        if o.exclRelIndex == rt_index {
                            o.exclRelIndex = new_index;
                        }
                    })
                }
                .expect("OnConflictExpr");
            }
            for rc in &q.rowMarks {
                // SAFETY: as above.
                unsafe {
                    rc.with_mut::<RowMarkClause, _>(|r| {
                        if r.rti == rt_index as u32 {
                            r.rti = new_index as u32;
                        }
                    })
                }
                .expect("rowMarks holds RowMarkClause");
            }
        }
        use nodes_core::NodeWalker as _;
        nodes_core::query_tree_walker(node.as_query().expect("Query"), &mut w, 0)?;
    } else {
        use nodes_core::NodeWalker as _;
        w.visit(node)?;
    }
    Ok(())
}

/// ChangeVarNodesExtended with analyzejoins.c's replace_relid_callback,
/// expression-tree form (RangeTblRefs left untouched). The callback's
/// RestrictInfo arm has no equivalent here: planner RestrictInfos are arena
/// structs, adjusted by the caller.
pub fn ChangeVarNodesExtendedSJE<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    rt_index: i32,
    new_index: i32,
    sublevels_up: u32,
) -> PgResult<()> {
    debug_assert!(node.node_tag() != NodeTag::T_Query);
    let mut w = ChangeVars { mcx, rt_index, new_index, sublevels_up, skip_rangetblref: true };
    use nodes_core::NodeWalker as _;
    w.visit(node)?;
    Ok(())
}

/// ChangeVarNodesExtended over a whole Query reached by reference (SJE's
/// root->parse rewrite). resultRelation/mergeTargetRelation cannot name an
/// SJE-removed rel (remove_self_joins_recurse excludes them), so the two
/// scalar fields need no mutation here.
pub fn ChangeVarNodesExtendedSJEQueryRef<'mcx>(
    mcx: Mcx<'mcx>,
    q: &'mcx Query<'mcx>,
    rt_index: i32,
    new_index: i32,
) -> PgResult<()> {
    debug_assert!(q.resultRelation != rt_index && q.mergeTargetRelation != rt_index);
    if let Some(oc) = q.onConflict {
        // SAFETY: in-place fixup, no derived ref held across the call.
        unsafe {
            oc.with_mut::<types_nodes::primnodes::OnConflictExpr, _>(|o| {
                if o.exclRelIndex == rt_index {
                    o.exclRelIndex = new_index;
                }
            })
        }
        .expect("OnConflictExpr");
    }
    for rc in &q.rowMarks {
        // SAFETY: as above.
        unsafe {
            rc.with_mut::<RowMarkClause, _>(|r| {
                if r.rti == rt_index as u32 {
                    r.rti = new_index as u32;
                }
            })
        }
        .expect("rowMarks holds RowMarkClause");
    }
    let mut w = ChangeVars { mcx, rt_index, new_index, sublevels_up: 0, skip_rangetblref: true };
    use nodes_core::NodeWalker as _;
    nodes_core::query_tree_walker(q, &mut w, 0)?;
    Ok(())
}

struct IncrVarSublevels {
    delta: i32,
    min_sublevels_up: u32,
}

impl<'mcx> nodes_core::NodeWalker<'mcx> for IncrVarSublevels {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        match node.node_tag() {
            NodeTag::T_Var => {
                let (min, delta) = (self.min_sublevels_up, self.delta);
                // SAFETY: exclusive tree (module contract); no derived ref live.
                unsafe {
                    node.with_mut::<Var, _>(|v| {
                        if v.varlevelsup >= min {
                            v.varlevelsup = v.varlevelsup.wrapping_add_signed(delta);
                        }
                    })
                }
                .expect("Var");
                Ok(false)
            }
            NodeTag::T_Aggref => {
                let (min, delta) = (self.min_sublevels_up, self.delta);
                // SAFETY: as above.
                unsafe {
                    node.with_mut::<Aggref, _>(|a| {
                        if a.agglevelsup >= min {
                            a.agglevelsup = a.agglevelsup.wrapping_add_signed(delta);
                        }
                    })
                }
                .expect("Aggref");
                nodes_core::expression_tree_walker(node, self)
            }
            NodeTag::T_PlaceHolderVar => {
                let (min, delta) = (self.min_sublevels_up, self.delta);
                // SAFETY: as above.
                unsafe {
                    node.with_mut::<PlaceHolderVar, _>(|p| {
                        if p.phlevelsup >= min {
                            p.phlevelsup = p.phlevelsup.wrapping_add_signed(delta);
                        }
                    })
                }
                .expect("PlaceHolderVar");
                nodes_core::expression_tree_walker(node, self)
            }
            NodeTag::T_GroupingFunc => {
                let (min, delta) = (self.min_sublevels_up, self.delta);
                // SAFETY: as above.
                unsafe {
                    node.with_mut::<GroupingFunc, _>(|g| {
                        if g.agglevelsup >= min {
                            g.agglevelsup = g.agglevelsup.wrapping_add_signed(delta);
                        }
                    })
                }
                .expect("GroupingFunc");
                nodes_core::expression_tree_walker(node, self)
            }
            NodeTag::T_ReturningExpr => {
                let (min, delta) = (self.min_sublevels_up, self.delta);
                // SAFETY: as above.
                unsafe {
                    node.with_mut::<types_nodes::primnodes::ReturningExpr, _>(|r| {
                        if r.retlevelsup as u32 >= min {
                            r.retlevelsup = r.retlevelsup.wrapping_add(delta);
                        }
                    })
                }
                .expect("ReturningExpr");
                nodes_core::expression_tree_walker(node, self)
            }
            NodeTag::T_CurrentOfExpr => {
                if self.min_sublevels_up == 0 {
                    return Err(internal("cannot push down CurrentOfExpr").into());
                }
                Ok(false)
            }
            NodeTag::T_RangeTblEntry => {
                let (min, delta) = (self.min_sublevels_up, self.delta);
                // SAFETY: as above.
                unsafe {
                    node.with_mut::<RangeTblEntry, _>(|rte| {
                        if rte.rtekind == RTEKind::RTE_CTE && rte.ctelevelsup >= min {
                            rte.ctelevelsup = rte.ctelevelsup.wrapping_add_signed(delta);
                        }
                    })
                }
                .expect("RangeTblEntry");
                Ok(false)
            }
            NodeTag::T_Query => {
                self.min_sublevels_up += 1;
                let r = nodes_core::query_tree_walker(
                    node.as_query().expect("Query"),
                    self,
                    nodes_core::QTW_EXAMINE_RTES_BEFORE,
                )?;
                self.min_sublevels_up -= 1;
                Ok(r)
            }
            _ => nodes_core::expression_tree_walker(node, self),
        }
    }

    fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
        self.min_sublevels_up += 1;
        let r = nodes_core::query_tree_walker(q, self, nodes_core::QTW_EXAMINE_RTES_BEFORE)?;
        self.min_sublevels_up -= 1;
        Ok(r)
    }
}

pub fn IncrementVarSublevelsUp_query<'mcx>(
    q: &'mcx Query<'mcx>,
    delta_sublevels_up: i32,
    min_sublevels_up: u32,
) -> PgResult<()> {
    let mut w = IncrVarSublevels { delta: delta_sublevels_up, min_sublevels_up };
    nodes_core::query_tree_walker(q, &mut w, nodes_core::QTW_EXAMINE_RTES_BEFORE)?;
    Ok(())
}

pub fn IncrementVarSublevelsUp<'mcx>(
    node: Node<'mcx>,
    delta_sublevels_up: i32,
    min_sublevels_up: u32,
) -> PgResult<()> {
    let mut w = IncrVarSublevels { delta: delta_sublevels_up, min_sublevels_up };
    use nodes_core::NodeWalker as _;
    if node.node_tag() == NodeTag::T_Query {
        nodes_core::query_tree_walker(
            node.as_query().expect("Query"),
            &mut w,
            nodes_core::QTW_EXAMINE_RTES_BEFORE,
        )?;
    } else {
        w.visit(node)?;
    }
    Ok(())
}

struct RtiUsed {
    rt_index: i32,
    sublevels_up: u32,
}

impl<'mcx> nodes_core::NodeWalker<'mcx> for RtiUsed {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        match node.node_tag() {
            NodeTag::T_Var => {
                let v = node.as_var().expect("Var");
                Ok(v.varlevelsup == self.sublevels_up
                    && (v.varno == self.rt_index || v.varnullingrels.is_member(self.rt_index)))
            }
            NodeTag::T_CurrentOfExpr => Ok(self.sublevels_up == 0
                && node.as_current_of_expr().expect("CurrentOfExpr").cvarno
                    == self.rt_index as u32),
            NodeTag::T_RangeTblRef => Ok(self.sublevels_up == 0
                && node.as_range_tbl_ref().expect("RangeTblRef").rtindex == self.rt_index),
            NodeTag::T_JoinExpr => {
                let j = node.as_join_expr().expect("JoinExpr");
                if self.sublevels_up == 0 && j.rtindex == self.rt_index {
                    return Ok(true);
                }
                nodes_core::expression_tree_walker(node, self)
            }
            NodeTag::T_Query => {
                self.sublevels_up += 1;
                let r =
                    nodes_core::query_tree_walker(node.as_query().expect("Query"), self, 0)?;
                self.sublevels_up -= 1;
                Ok(r)
            }
            _ => nodes_core::expression_tree_walker(node, self),
        }
    }

    fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
        self.sublevels_up += 1;
        let r = nodes_core::query_tree_walker(q, self, 0)?;
        self.sublevels_up -= 1;
        Ok(r)
    }
}

pub fn rangeTableEntry_used<'mcx>(
    node: Node<'mcx>,
    rt_index: i32,
    sublevels_up: u32,
) -> PgResult<bool> {
    let mut w = RtiUsed { rt_index, sublevels_up };
    use nodes_core::NodeWalker as _;
    if node.node_tag() == NodeTag::T_Query {
        nodes_core::query_tree_walker(node.as_query().expect("Query"), &mut w, 0)
    } else {
        w.visit(node)
    }
}

pub fn rangeTableEntry_used_query<'mcx>(
    q: &'mcx Query<'mcx>,
    rt_index: i32,
    sublevels_up: u32,
) -> PgResult<bool> {
    let mut w = RtiUsed { rt_index, sublevels_up };
    nodes_core::query_tree_walker(q, &mut w, 0)
}

pub fn rangeTableEntry_used_list<'mcx>(
    list: &NodeList<'mcx>,
    rt_index: i32,
    sublevels_up: u32,
) -> PgResult<bool> {
    let mut w = RtiUsed { rt_index, sublevels_up };
    nodes_core::walk_list(list, &mut w)
}

pub fn rangeTableEntry_used_opt<'mcx>(
    node: Option<Node<'mcx>>,
    rt_index: i32,
    sublevels_up: u32,
) -> PgResult<bool> {
    match node {
        Some(n) => rangeTableEntry_used(n, rt_index, sublevels_up),
        None => Ok(false),
    }
}

fn eref_alias<'mcx>(rte: &RangeTblEntry<'mcx>) -> &'mcx str {
    rte.eref.and_then(|e| e.aliasname).unwrap_or("")
}

pub fn getInsertSelectQuery_parts<'mcx>(
    parsetree: &Query<'mcx>,
) -> PgResult<Option<(Node<'mcx>, &'mcx Query<'mcx>)>> {
    use types_nodes::nodes_enums::CmdType;
    if parsetree.commandType != CmdType::CMD_INSERT {
        return Ok(None);
    }
    if parsetree.rtable.len() >= 2
        && eref_alias(rte_at(&parsetree.rtable, PRS2_OLD_VARNO)) == "old"
        && eref_alias(rte_at(&parsetree.rtable, PRS2_NEW_VARNO)) == "new"
    {
        return Ok(None);
    }
    let jt = parsetree.jointree.expect("INSERT jointree is a FromExpr");
    if jt.fromlist.len() != 1 {
        return Err(internal("expected to find SELECT subquery"));
    }
    let rtr = match jt.fromlist.nth(0).as_range_tbl_ref() {
        Some(r) => r,
        None => return Err(internal("expected to find SELECT subquery")),
    };
    let select_rte_node = parsetree.rtable.nth(rtr.rtindex as usize - 1);
    let select_rte = rte_at(&parsetree.rtable, rtr.rtindex);
    let selectquery = match (select_rte.rtekind, select_rte.subquery) {
        (RTEKind::RTE_SUBQUERY, Some(q)) if q.commandType == CmdType::CMD_SELECT => q,
        _ => return Err(internal("expected to find SELECT subquery")),
    };
    if selectquery.rtable.len() >= 2
        && eref_alias(rte_at(&selectquery.rtable, PRS2_OLD_VARNO)) == "old"
        && eref_alias(rte_at(&selectquery.rtable, PRS2_NEW_VARNO)) == "new"
    {
        return Ok(Some((select_rte_node, selectquery)));
    }
    Err(internal("could not find rule placeholders"))
}

// getInsertSelectQuery (rewriteManip.c), read-only shape (C's NULL
// subquery_ptr callers).
pub fn getInsertSelectQuery_ref<'a, 'mcx>(
    parsetree: &'a Query<'mcx>,
) -> PgResult<&'a Query<'mcx>>
where
    'mcx: 'a,
{
    match getInsertSelectQuery_parts(parsetree)? {
        Some((_, sub)) => Ok(sub),
        None => Ok(parsetree),
    }
}

// getInsertSelectQuery (rewriteManip.c), mutating shape. RTE.subquery is a
// &Query with no recoverable node handle, so the INSERT ... SELECT arm
// re-reads the sub-query into a fresh node and relinks it — the returned
// node is then the live tree (C's *subquery_ptr writeback happens here).
pub fn getInsertSelectQuery_node<'mcx>(
    mcx: Mcx<'mcx>,
    parsetree_node: Node<'mcx>,
) -> PgResult<(Node<'mcx>, bool)> {
    let parsetree = parsetree_node.as_query().expect("Query");
    match getInsertSelectQuery_parts(parsetree)? {
        None => Ok((parsetree_node, false)),
        Some((select_rte_node, selectquery)) => {
            let sub_node = copy_query_node(mcx, selectquery)?;
            let sub_ref = sub_node.as_query().expect("Query round trip");
            // SAFETY: exclusive tree (module contract).
            unsafe {
                select_rte_node.with_mut::<RangeTblEntry, _>(|r| r.subquery = Some(sub_ref))
            }
            .expect("RangeTblEntry");
            Ok((sub_node, true))
        }
    }
}

fn rte_at<'a, 'mcx>(rtable: &'a NodeList<'mcx>, varno: i32) -> &'mcx RangeTblEntry<'mcx> {
    rtable.nth(varno as usize - 1).as_range_tbl_entry().expect("rtable holds RangeTblEntry")
}

// contain_aggs_of_level / contain_aggs_of_level_walker (rewriteManip.c).
struct ContainAggsOfLevel {
    sublevels_up: i32,
}

impl<'mcx> nodes_core::NodeWalker<'mcx> for ContainAggsOfLevel {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        if let Some(a) = node.as_aggref() {
            if a.agglevelsup as i32 == self.sublevels_up {
                return Ok(true);
            }
            // C falls through to examine the arguments.
        }
        if let Some(g) = node.as_grouping_func() {
            if g.agglevelsup as i32 == self.sublevels_up {
                return Ok(true);
            }
        }
        if let Some(q) = node.as_query() {
            return self.visit_query_ref(q);
        }
        nodes_core::expression_tree_walker(node, self)
    }

    fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
        self.sublevels_up += 1;
        let result = nodes_core::query_tree_walker(q, self, 0);
        self.sublevels_up -= 1;
        result
    }
}

pub fn contain_aggs_of_level(node: Node<'_>, levelsup: i32) -> PgResult<bool> {
    let mut w = ContainAggsOfLevel { sublevels_up: levelsup };
    nodes_core::query_or_expression_tree_walker(node, &mut w, 0)
}

// locate_agg_of_level / locate_agg_of_level_walker (rewriteManip.c).
struct LocateAggOfLevel {
    agg_location: types_core::ParseLoc,
    sublevels_up: i32,
}

impl<'mcx> nodes_core::NodeWalker<'mcx> for LocateAggOfLevel {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        if let Some(a) = node.as_aggref() {
            if a.agglevelsup as i32 == self.sublevels_up && a.location >= 0 {
                self.agg_location = a.location;
                return Ok(true);
            }
            // C falls through to examine the arguments.
        }
        if let Some(g) = node.as_grouping_func() {
            if g.agglevelsup as i32 == self.sublevels_up && g.location >= 0 {
                self.agg_location = g.location;
                return Ok(true);
            }
        }
        if let Some(q) = node.as_query() {
            return self.visit_query_ref(q);
        }
        nodes_core::expression_tree_walker(node, self)
    }

    fn visit_query_ref(&mut self, q: &'mcx Query<'mcx>) -> PgResult<bool> {
        self.sublevels_up += 1;
        let result = nodes_core::query_tree_walker(q, self, 0);
        self.sublevels_up -= 1;
        result
    }
}

pub fn locate_agg_of_level(node: Node<'_>, levelsup: i32) -> PgResult<types_core::ParseLoc> {
    let mut w = LocateAggOfLevel { agg_location: -1, sublevels_up: levelsup };
    nodes_core::query_or_expression_tree_walker(node, &mut w, 0)?;
    Ok(w.agg_location)
}

struct HasSubLink;

impl<'mcx> nodes_core::NodeWalker<'mcx> for HasSubLink {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        if node.node_tag() == NodeTag::T_SubLink {
            return Ok(true);
        }
        nodes_core::expression_tree_walker(node, self)
    }
}

pub fn checkExprHasSubLink<'mcx>(node: Node<'mcx>) -> PgResult<bool> {
    let mut w = HasSubLink;
    use nodes_core::NodeWalker as _;
    w.visit(node)
}

pub fn checkExprHasSubLink_opt<'mcx>(node: Option<Node<'mcx>>) -> PgResult<bool> {
    match node {
        Some(n) => checkExprHasSubLink(n),
        None => Ok(false),
    }
}

pub fn checkExprHasSubLink_list<'mcx>(list: &NodeList<'mcx>) -> PgResult<bool> {
    let mut w = HasSubLink;
    nodes_core::walk_list(list, &mut w)
}

// AddQual (rewriteManip.c). `query_node` must hold a Query; the qual is
// deep-copied before attachment as C does (the caller's qual may be shared
// with the still-live original parsetree).
pub fn AddQual<'mcx>(
    mcx: Mcx<'mcx>,
    query_node: Node<'mcx>,
    qual: Option<Node<'mcx>>,
) -> PgResult<()> {
    let Some(qual) = qual else { return Ok(()) };
    let q = query_node.as_query().expect("Query");
    if q.commandType == types_nodes::nodes_enums::CmdType::CMD_UTILITY {
        if q.utilityStmt.is_some_and(|u| u.node_tag() == NodeTag::T_NotifyStmt) {
            return Ok(());
        }
        return Err(feature_not_supported("conditional utility statements are not implemented"));
    }
    if q.setOperations.is_some() {
        return Err(feature_not_supported(
            "conditional UNION/INTERSECT/EXCEPT statements are not implemented",
        ));
    }
    let copy = copy_node(mcx, qual)?;
    let jt = q.jointree.expect("non-utility Query has a jointree");
    let new_quals = make_and_qual(mcx, jt.quals, copy)?;
    let new_jt = mcx::alloc_leak_in(
        mcx,
        FromExpr { fromlist: jt.fromlist.clone_in(mcx)?, quals: Some(new_quals) },
    )?;
    let has_sublinks = q.hasSubLinks || checkExprHasSubLink(copy)?;
    // SAFETY: exclusive tree (module contract); `q`/`jt` reads completed.
    unsafe {
        query_node.with_mut::<Query, _>(|qm| {
            qm.jointree = Some(new_jt);
            qm.hasSubLinks = has_sublinks;
        })
    }
    .expect("Query");
    Ok(())
}

// make_and_qual (makefuncs.c).
pub fn make_and_qual<'mcx>(
    mcx: Mcx<'mcx>,
    qual1: Option<Node<'mcx>>,
    qual2: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    match qual1 {
        None => Ok(qual2),
        Some(q1) => {
            let mut args = NodeList::nil();
            args.lappend(mcx, q1)?;
            args.lappend(mcx, qual2)?;
            Node::mk(
                mcx,
                BoolExpr { boolop: BoolExprType::AND_EXPR, args, location: -1 },
            )
        }
    }
}

pub fn AddInvertedQual<'mcx>(
    mcx: Mcx<'mcx>,
    query_node: Node<'mcx>,
    qual: Option<Node<'mcx>>,
) -> PgResult<()> {
    let Some(qual) = qual else { return Ok(()) };
    let invqual = Node::mk(
        mcx,
        BooleanTest { arg: Some(qual), booltesttype: BoolTestType::IS_NOT_TRUE, location: -1 },
    )?;
    AddQual(mcx, query_node, Some(invqual))
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ReplaceVarsNoMatchOption {
    ReportError,
    ChangeVarno(i32),
    SubstituteNull,
}

struct ReplaceVarsCtx<'a, 'mcx> {
    mcx: Mcx<'mcx>,
    target_varno: i32,
    sublevels_up: u32,
    target_rte: &'a RangeTblEntry<'mcx>,
    targetlist: &'a NodeList<'mcx>,
    result_relation: i32,
    nomatch_option: ReplaceVarsNoMatchOption,
    inserted_sublink: bool,
}

// ReplaceVarsFromTargetList (rewriteManip.c) over replace_rte_variables.
// The Query arm mutates the exclusively-owned tree in place (C's mutator
// copy is dead weight here); expression positions rebuild functionally.
#[allow(clippy::too_many_arguments)]
pub fn ReplaceVarsFromTargetList<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    target_varno: i32,
    sublevels_up: u32,
    target_rte: &RangeTblEntry<'mcx>,
    targetlist: &NodeList<'mcx>,
    result_relation: i32,
    nomatch_option: ReplaceVarsNoMatchOption,
    outer_has_sublinks: Option<&mut bool>,
) -> PgResult<Node<'mcx>> {
    let mut ctx = ReplaceVarsCtx {
        mcx,
        target_varno,
        sublevels_up,
        target_rte,
        targetlist,
        result_relation,
        nomatch_option,
        inserted_sublink: if node.node_tag() == NodeTag::T_Query {
            node.as_query().expect("Query").hasSubLinks
        } else {
            outer_has_sublinks.as_deref().copied().unwrap_or(false)
        },
    };
    let result = if node.node_tag() == NodeTag::T_Query {
        rv_query_inplace(node, &mut ctx)?;
        node
    } else {
        rv_mutate(node, &mut ctx)?.unwrap_or(node)
    };
    if ctx.inserted_sublink {
        if result.node_tag() == NodeTag::T_Query {
            // SAFETY: exclusive tree (module contract).
            unsafe { result.with_mut::<Query, _>(|q| q.hasSubLinks = true) }.expect("Query");
        } else if let Some(flag) = outer_has_sublinks {
            *flag = true;
        } else {
            return Err(internal(
                "replace_rte_variables inserted a SubLink, but has noplace to record it",
            ));
        }
    }
    Ok(result)
}

#[allow(clippy::too_many_arguments)]
pub fn ReplaceVarsFromTargetList_list<'mcx>(
    mcx: Mcx<'mcx>,
    list: &NodeList<'mcx>,
    target_varno: i32,
    sublevels_up: u32,
    target_rte: &RangeTblEntry<'mcx>,
    targetlist: &NodeList<'mcx>,
    result_relation: i32,
    nomatch_option: ReplaceVarsNoMatchOption,
    outer_has_sublinks: Option<&mut bool>,
) -> PgResult<NodeList<'mcx>> {
    let node = Node::mk_list(mcx, list.clone_in(mcx)?)?;
    let out = ReplaceVarsFromTargetList(
        mcx,
        node,
        target_varno,
        sublevels_up,
        target_rte,
        targetlist,
        result_relation,
        nomatch_option,
        outer_has_sublinks,
    )?;
    Ok(out.as_list().expect("List").clone_in(mcx)?)
}

fn rv_mutate<'mcx>(
    node: Node<'mcx>,
    ctx: &mut ReplaceVarsCtx<'_, 'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    let mcx = ctx.mcx;
    match node.node_tag() {
        NodeTag::T_Var => {
            let var = node.as_var().expect("Var");
            if var.varno == ctx.target_varno && var.varlevelsup == ctx.sublevels_up {
                let newnode = ReplaceVarFromTargetList(
                    mcx,
                    var,
                    ctx.target_rte,
                    ctx.targetlist,
                    ctx.result_relation,
                    ctx.nomatch_option,
                )?;
                if var.varlevelsup > 0 {
                    IncrementVarSublevelsUp(newnode, var.varlevelsup as i32, 0)?;
                }
                if !ctx.inserted_sublink {
                    ctx.inserted_sublink = checkExprHasSubLink(newnode)?;
                }
                return Ok(Some(newnode));
            }
            Ok(None)
        }
        NodeTag::T_CurrentOfExpr => {
            let cexpr = node.as_current_of_expr().expect("CurrentOfExpr");
            if cexpr.cvarno == ctx.target_varno as u32 && ctx.sublevels_up == 0 {
                return Err(feature_not_supported(
                    "WHERE CURRENT OF on a view is not implemented",
                ).into());
            }
            Ok(None)
        }
        // nodes_core's SubLink mutator arm skips the subselect C mutates.
        NodeTag::T_SubLink => {
            let sl = node.as_sub_link().expect("SubLink");
            if rv_mutate(sl.subselect, ctx)?.is_some() {
                panic!("Query subselect mutates in place");
            }
            match sl.testexpr {
                None => Ok(None),
                Some(te) => match rv_mutate(te, ctx)? {
                    None => Ok(None),
                    Some(new_te) => Ok(Some(Node::mk(
                        mcx,
                        types_nodes::SubLink {
                            subLinkType: sl.subLinkType,
                            subLinkId: sl.subLinkId,
                            testexpr: Some(new_te),
                            operName: sl.operName.clone_in(mcx)?,
                            subselect: sl.subselect,
                            location: sl.location,
                        },
                    )?)),
                },
            }
        }
        NodeTag::T_Query => {
            ctx.sublevels_up += 1;
            let saved = ctx.inserted_sublink;
            ctx.inserted_sublink = node.as_query().expect("Query").hasSubLinks;
            rv_query_inplace(node, ctx)?;
            let inserted = ctx.inserted_sublink;
            // SAFETY: exclusive tree (module contract).
            unsafe { node.with_mut::<Query, _>(|q| q.hasSubLinks |= inserted) }
                .expect("Query");
            ctx.inserted_sublink = saved;
            ctx.sublevels_up -= 1;
            Ok(None)
        }
        // In-place, like the Query arm (exclusively-owned tree).
        NodeTag::T_OnConflictExpr => {
            rv_mutate_onconflict(node, ctx)?;
            Ok(None)
        }
        _ => {
            let mut m = |n: Node<'mcx>| rv_mutate(n, ctx);
            nodes_core::expression_tree_mutator(mcx, node, &mut m)
        }
    }
}

fn rv_mutate_onconflict<'mcx>(
    oc_node: Node<'mcx>,
    ctx: &mut ReplaceVarsCtx<'_, 'mcx>,
) -> PgResult<()> {
    let oc = oc_node
        .as_on_conflict_expr()
        .expect("OnConflictExpr");
    let arbiter_elems = rv_mutate_list(&oc.arbiterElems, ctx)?;
    let arbiter_where = rv_mutate_opt(oc.arbiterWhere, ctx)?;
    let set = rv_mutate_list(&oc.onConflictSet, ctx)?;
    let oc_where = rv_mutate_opt(oc.onConflictWhere, ctx)?;
    let excl_tlist = rv_mutate_list(&oc.exclRelTlist, ctx)?;
    // SAFETY: exclusive tree (module contract).
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

// None = unchanged (mutator convention).
fn rv_mutate_opt<'mcx>(
    node: Option<Node<'mcx>>,
    ctx: &mut ReplaceVarsCtx<'_, 'mcx>,
) -> PgResult<Option<Node<'mcx>>> {
    match node {
        None => Ok(None),
        Some(n) => rv_mutate(n, ctx),
    }
}

fn rv_mutate_list<'mcx>(
    list: &NodeList<'mcx>,
    ctx: &mut ReplaceVarsCtx<'_, 'mcx>,
) -> PgResult<Option<NodeList<'mcx>>> {
    let mcx = ctx.mcx;
    let mut changed = false;
    let mut out = NodeList::nil();
    for n in list {
        let m = rv_mutate(n, ctx)?;
        changed |= m.is_some();
        out.lappend(mcx, m.unwrap_or(n))?;
    }
    Ok(if changed { Some(out) } else { None })
}

// query_tree_mutator's field set, applied in place on the Query node.
fn rv_query_inplace<'mcx>(
    qnode: Node<'mcx>,
    ctx: &mut ReplaceVarsCtx<'_, 'mcx>,
) -> PgResult<()> {
    let mcx = ctx.mcx;
    let q = qnode.as_query().expect("Query");

    let new_target = rv_mutate_list(&q.targetList, ctx)?;
    let new_returning = rv_mutate_list(&q.returningList, ctx)?;
    let new_having = rv_mutate_opt(q.havingQual, ctx)?;
    let new_limit_off = rv_mutate_opt(q.limitOffset, ctx)?;
    let new_limit_cnt = rv_mutate_opt(q.limitCount, ctx)?;
    let new_setops = rv_mutate_opt(q.setOperations, ctx)?;
    if let Some(oc_node) = q.onConflict {
        rv_mutate_onconflict(oc_node, ctx)?;
    }
    for wco_node in &q.withCheckOptions {
        let wco = wco_node
            .as_with_check_option()
            .expect("withCheckOptions cell");
        if let Some(new_qual) = rv_mutate_opt(wco.qual, ctx)? {
            // SAFETY: exclusive tree (module contract).
            unsafe {
                wco_node.with_mut::<types_nodes::parsenodes::WithCheckOption, _>(|w| {
                    w.qual = Some(new_qual)
                })
            }
            .expect("WithCheckOption");
        }
    }
    for action_node in &q.mergeActionList {
        let action = action_node
            .as_merge_action()
            .expect("mergeActionList cell is a MergeAction");
        let new_qual = rv_mutate_opt(action.qual, ctx)?;
        let new_tlist = rv_mutate_list(&action.targetList, ctx)?;
        if new_qual.is_some() || new_tlist.is_some() {
            // SAFETY: exclusive tree (module contract).
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
    let new_merge_join_cond = rv_mutate_opt(q.mergeJoinCondition, ctx)?;
    for wc_node in &q.windowClause {
        let wc = wc_node.as_window_clause().expect("windowClause cell");
        if rv_mutate_opt(wc.startOffset, ctx)?.is_some()
            || rv_mutate_opt(wc.endOffset, ctx)?.is_some()
        {
            panic!(
                "ReplaceVarsFromTargetList (rewriteManip.c): NEW/OLD reference \
                 inside a window frame offset (WindowClause rebuild unported)"
            );
        }
    }
    let new_jointree = match q.jointree {
        None => None,
        Some(jt) => {
            let fl = rv_mutate_list(&jt.fromlist, ctx)?;
            let quals = rv_mutate_opt(jt.quals, ctx)?;
            if fl.is_some() || quals.is_some() {
                Some(mcx::alloc_leak_in(
                    mcx,
                    FromExpr {
                        fromlist: fl.unwrap_or(jt.fromlist.clone_in(mcx)?),
                        quals: quals.or(jt.quals),
                    },
                )?)
            } else {
                None
            }
        }
    };
    for cte in &q.cteList {
        let ctequery = cte
            .as_common_table_expr()
            .expect("cteList cell")
            .ctequery
            .expect("analyzed CTE");
        rv_mutate(ctequery, ctx)?;
    }
    for rte_node in q.rtable.iter() {
        let rte = rte_node.as_range_tbl_entry().expect("rtable cell");
        match rte.rtekind {
            RTEKind::RTE_SUBQUERY => {
                // &Query has no recoverable node handle; only re-read it when
                // it actually holds outer references to the target varno.
                let sub = rte.subquery.expect("subquery RTE has a subquery");
                let mut w = RtiUsed {
                    rt_index: ctx.target_varno,
                    sublevels_up: ctx.sublevels_up + 1,
                };
                if nodes_core::query_tree_walker(sub, &mut w, 0)? {
                    let sub_node = copy_query_node(ctx.mcx, sub)?;
                    if rv_mutate(sub_node, ctx)?.is_some() {
                        panic!("Query subselect mutates in place");
                    }
                    let sub_ref = sub_node.as_query().expect("Query round trip");
                    // SAFETY: exclusive tree (module contract).
                    unsafe {
                        rte_node.with_mut::<RangeTblEntry, _>(|r| r.subquery = Some(sub_ref))
                    }
                    .expect("RangeTblEntry");
                }
            }
            RTEKind::RTE_VALUES => {
                if let Some(new_lists) = rv_mutate_list(&rte.values_lists, ctx)? {
                    // SAFETY: exclusive tree (module contract).
                    unsafe {
                        rte_node
                            .with_mut::<RangeTblEntry, _>(|r| r.values_lists = new_lists)
                    }
                    .expect("RangeTblEntry");
                }
            }
            RTEKind::RTE_JOIN => {
                if let Some(new_javs) = rv_mutate_list(&rte.joinaliasvars, ctx)? {
                    // SAFETY: as above.
                    unsafe {
                        rte_node
                            .with_mut::<RangeTblEntry, _>(|r| r.joinaliasvars = new_javs)
                    }
                    .expect("RangeTblEntry");
                }
            }
            RTEKind::RTE_FUNCTION | RTEKind::RTE_TABLEFUNC | RTEKind::RTE_GROUP => panic!(
                "ReplaceVarsFromTargetList (rewriteManip.c): {:?} RTE mutation arm unported",
                rte.rtekind
            ),
            _ => {}
        }
        if let Some(new_sq) = rv_mutate_list(&rte.securityQuals, ctx)? {
            // SAFETY: exclusive tree (module contract).
            unsafe {
                rte_node.with_mut::<RangeTblEntry, _>(|r| r.securityQuals = new_sq)
            }
            .expect("RangeTblEntry");
        }
    }
    // SAFETY: exclusive tree (module contract); field reads completed.
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
            if let Some(jt) = new_jointree {
                qm.jointree = Some(jt);
            }
            if new_setops.is_some() {
                qm.setOperations = new_setops;
            }
            if new_merge_join_cond.is_some() {
                qm.mergeJoinCondition = new_merge_join_cond;
            }
        })
    }
    .expect("Query");
    Ok(())
}

// ReplaceVarFromTargetList (rewriteManip.c).
fn ReplaceVarFromTargetList<'mcx>(
    mcx: Mcx<'mcx>,
    var: &Var<'mcx>,
    target_rte: &RangeTblEntry<'mcx>,
    targetlist: &NodeList<'mcx>,
    result_relation: i32,
    nomatch_option: ReplaceVarsNoMatchOption,
) -> PgResult<Node<'mcx>> {
    if var.varattno == 0 {
        // Whole-tuple reference: expand to RowExpr. Named rowtype (plain
        // relation RTE) includes dummy items for dropped columns; RECORD
        // (JOIN) omits them and carries colnames instead. Expansion is
        // generated with varlevelsup = 0; the caller re-adjusts.
        let (colnames, fields) = parse_relation::expandRTE(
            mcx,
            target_rte,
            var.varno,
            0,
            var.varreturningtype,
            var.location,
            var.vartype != types_core::catalog::RECORDOID,
        )?;
        let mut args = NodeList::nil();
        for field in fields.iter() {
            let field = if field.node_tag() == NodeTag::T_Var {
                ReplaceVarFromTargetList(
                    mcx,
                    field.as_var().expect("Var"),
                    target_rte,
                    targetlist,
                    result_relation,
                    nomatch_option,
                )?
            } else {
                field
            };
            args.lappend(mcx, field)?;
        }
        let rowexpr = Node::mk(
            mcx,
            types_nodes::RowExpr {
                args,
                row_typeid: var.vartype,
                row_format: types_nodes::CoercionForm::COERCE_IMPLICIT_CAST,
                colnames: if var.vartype == types_core::catalog::RECORDOID {
                    colnames
                } else {
                    NodeList::nil()
                },
                location: var.location,
            },
        )?;
        if var.varreturningtype != VarReturningType::VAR_RETURNING_DEFAULT {
            return Node::mk(
                mcx,
                types_nodes::primnodes::ReturningExpr {
                    retlevelsup: 0,
                    retold: var.varreturningtype == VarReturningType::VAR_RETURNING_OLD,
                    retexpr: rowexpr,
                },
            );
        }
        return Ok(rowexpr);
    }
    let tle = targetlist
        .iter()
        .map(|n| n.as_target_entry().expect("tlist cell"))
        .find(|te| te.resno == var.varattno);
    match tle.filter(|t| !t.resjunk) {
        None => match nomatch_option {
            ReplaceVarsNoMatchOption::ReportError => Err(internal(&format!(
                "could not find replacement targetlist entry for attno {}",
                var.varattno
            ))),
            ReplaceVarsNoMatchOption::ChangeVarno(nomatch_varno) => {
                let mut newvar = Var { varnullingrels: var.varnullingrels.clone_in(mcx)?, ..*var };
                newvar.varno = nomatch_varno;
                newvar.varlevelsup = 0;
                Node::mk(mcx, newvar)
            }
            ReplaceVarsNoMatchOption::SubstituteNull => {
                // C wraps coerce_null_to_domain; CREATE DOMAIN is unreachable
                // on this base, so a plain NULL Const is C-identical.
                let (typlen, typbyval) = lsyscache::get_typlenbyval(var.vartype)?;
                Node::mk_const(
                    mcx,
                    var.vartype,
                    var.vartypmod,
                    var.varcollid,
                    typlen as i32,
                    datum::Datum::null(),
                    true,
                    typbyval,
                )
            }
        },
        Some(tle) => {
            let newnode = copy_node(mcx, tle_expr_node(tle))?;
            if contains_multiexpr_param(newnode)? {
                return Err(feature_not_supported(
                    "NEW variables in ON UPDATE rules cannot reference columns that are part of a multiple assignment in the subject UPDATE command",
                ));
            }
            if var.varreturningtype != VarReturningType::VAR_RETURNING_DEFAULT {
                if result_relation == 0 {
                    return Err(internal(
                        "variable returning old/new found outside RETURNING list",
                    ));
                }
                SetVarReturningType(newnode, result_relation, 0, var.varreturningtype)?;
                let wrap = match newnode.as_var() {
                    Some(v) => v.varno != result_relation || v.varlevelsup != 0,
                    None => true,
                };
                if wrap {
                    return Node::mk(
                        mcx,
                        types_nodes::primnodes::ReturningExpr {
                            retlevelsup: 0,
                            retold: var.varreturningtype
                                == VarReturningType::VAR_RETURNING_OLD,
                            retexpr: newnode,
                        },
                    );
                }
            }
            Ok(newnode)
        }
    }
}

struct SetVarReturningTypeWalker {
    result_relation: i32,
    sublevels_up: u32,
    returning_type: VarReturningType,
}

impl<'mcx> nodes_core::NodeWalker<'mcx> for SetVarReturningTypeWalker {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        match node.node_tag() {
            NodeTag::T_Var => {
                let v = node.as_var().expect("Var");
                if v.varno == self.result_relation && v.varlevelsup == self.sublevels_up {
                    // SAFETY: exclusive freshly-copied tree (caller copies
                    // before calling, per C's copyObject-then-modify shape).
                    unsafe {
                        node.with_mut::<Var, _>(|vm| {
                            vm.varreturningtype = self.returning_type
                        })
                    }
                    .expect("Var");
                }
                Ok(false)
            }
            NodeTag::T_Query => {
                self.sublevels_up += 1;
                let r =
                    nodes_core::query_tree_walker(node.as_query().expect("Query"), self, 0)?;
                self.sublevels_up -= 1;
                Ok(r)
            }
            _ => nodes_core::expression_tree_walker(node, self),
        }
    }
}

// SetVarReturningType (rewriteManip.c); mutates Vars in place — the given
// tree must be a fresh copy.
#[allow(non_snake_case)]
pub fn SetVarReturningType(
    node: Node<'_>,
    result_relation: i32,
    sublevels_up: u32,
    returning_type: VarReturningType,
) -> PgResult<()> {
    let mut w = SetVarReturningTypeWalker { result_relation, sublevels_up, returning_type };
    use nodes_core::NodeWalker as _;
    w.visit(node)?;
    Ok(())
}

fn tle_expr_node<'mcx>(tle: &TargetEntry<'mcx>) -> Node<'mcx> {
    tle.expr
}

struct MultiExprParam;

impl<'mcx> nodes_core::NodeWalker<'mcx> for MultiExprParam {
    fn visit(&mut self, node: Node<'mcx>) -> PgResult<bool> {
        if node.node_tag() == NodeTag::T_Param {
            let p = node.as_variant::<types_nodes::primnodes::Param>().expect("Param");
            return Ok(
                p.paramkind == types_nodes::primnodes::ParamKind::PARAM_MULTIEXPR
            );
        }
        nodes_core::expression_tree_walker(node, self)
    }
}

pub fn contains_multiexpr_param<'mcx>(node: Node<'mcx>) -> PgResult<bool> {
    let mut w = MultiExprParam;
    use nodes_core::NodeWalker as _;
    w.visit(node)
}

#[track_caller]
#[cold]
#[inline(never)]
fn internal(msg: &str) -> Box<PgError> {
    Box::new(PgError::error(msg.to_string()))
}

#[track_caller]
#[cold]
#[inline(never)]
fn feature_not_supported(msg: &str) -> Box<PgError> {
    Box::new(PgError::error(msg.to_string()).with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED))
}

// makeNode(Query)-with-memcpy flat copy: list headers re-cloned so the value
// owns its cells, substructure shared (post-mutation conversion only).
pub fn flat_copy_query<'mcx>(mcx: Mcx<'mcx>, q: &Query<'mcx>) -> PgResult<Query<'mcx>> {
    Ok(Query {
        commandType: q.commandType,
        querySource: q.querySource,
        queryId: q.queryId,
        canSetTag: q.canSetTag,
        utilityStmt: q.utilityStmt,
        resultRelation: q.resultRelation,
        hasAggs: q.hasAggs,
        hasWindowFuncs: q.hasWindowFuncs,
        hasTargetSRFs: q.hasTargetSRFs,
        hasSubLinks: q.hasSubLinks,
        hasDistinctOn: q.hasDistinctOn,
        hasRecursive: q.hasRecursive,
        hasModifyingCTE: q.hasModifyingCTE,
        hasForUpdate: q.hasForUpdate,
        hasRowSecurity: q.hasRowSecurity,
        hasGroupRTE: q.hasGroupRTE,
        isReturn: q.isReturn,
        cteList: q.cteList.clone_in(mcx)?,
        rtable: q.rtable.clone_in(mcx)?,
        rteperminfos: q.rteperminfos.clone_in(mcx)?,
        jointree: q.jointree,
        mergeActionList: q.mergeActionList.clone_in(mcx)?,
        mergeTargetRelation: q.mergeTargetRelation,
        mergeJoinCondition: q.mergeJoinCondition,
        targetList: q.targetList.clone_in(mcx)?,
        r#override: q.r#override,
        onConflict: q.onConflict,
        returningOldAlias: q.returningOldAlias,
        returningNewAlias: q.returningNewAlias,
        returningList: q.returningList.clone_in(mcx)?,
        groupClause: q.groupClause.clone_in(mcx)?,
        groupDistinct: q.groupDistinct,
        groupingSets: q.groupingSets.clone_in(mcx)?,
        havingQual: q.havingQual,
        windowClause: q.windowClause.clone_in(mcx)?,
        distinctClause: q.distinctClause.clone_in(mcx)?,
        sortClause: q.sortClause.clone_in(mcx)?,
        limitOffset: q.limitOffset,
        limitCount: q.limitCount,
        limitOption: q.limitOption,
        rowMarks: q.rowMarks.clone_in(mcx)?,
        setOperations: q.setOperations,
        constraintDeps: q.constraintDeps.clone_in(mcx)?,
        withCheckOptions: q.withCheckOptions.clone_in(mcx)?,
        stmt_location: q.stmt_location,
        stmt_len: q.stmt_len,
    })
}
