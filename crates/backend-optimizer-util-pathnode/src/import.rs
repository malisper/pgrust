//! Cross-root path-tree import (`import_path_from_subroot`).
//!
//! # Why this exists
//!
//! The C planner shares `Path *`/`RelOptInfo *`/`RestrictInfo *` pointers freely
//! across `PlannerInfo` boundaries: a subquery's `subroot` builds paths that the
//! outer root's `create_subqueryscan_path` consumes directly, by pointer. This
//! port models the planner graph as per-[`PlannerInfo`] arenas
//! (`path_arena`/`rel_arena`/`rinfo_arena`/`node_arena`) addressed by bare
//! `u32` handles ([`PathId`]/[`RelId`]/[`RinfoId`]/[`NodeId`]) with **no arena
//! identity**. A [`PathId`] minted in a subroot's arena is meaningless in the
//! outer root's arena: `root.path(sub_id)` indexes the wrong vector.
//!
//! Set-op planning (`prepunion.c`'s `build_setop_child_paths`) is the first
//! consumer that must feed a subroot's final-rel paths into
//! `create_subqueryscan_path(ROOT, …)`. To resolve correctly, the subroot path
//! and its whole subtree must first be **deep-copied** into the outer root's
//! arenas, remapping every handle. That is what [`import_path_from_subroot`]
//! does — it is the enabler the K8 set-op port consumes; C needs no analogue
//! because it shares pointers.
//!
//! # What is copied
//!
//! Starting from a subroot [`PathId`], the importer recursively deep-copies:
//!
//! - the [`PathNode`] itself and every subpath it references
//!   (`subpath`/`leftpath`/`rightpath`/`subpaths`/`bitmapquals`/the join
//!   inner/outer paths/…), allocating fresh [`PathId`]s in the root arena;
//! - each path's parent [`RelOptInfo`] (`path.parent`, and the rel's
//!   `parent`/`top_parent` chain), with its `reltarget`
//!   [`PathTarget`], `baserestrictinfo`/`joininfo` [`RestrictInfo`]s, and
//!   `lateral_vars`, allocating fresh [`RelId`]s;
//! - each referenced [`RestrictInfo`]'s `clause`/`orclause` expression nodes,
//!   allocating fresh [`RinfoId`]s and [`NodeId`]s;
//! - the parent rel's `reltarget` [`PathTarget`] expression nodes, **and the
//!   path's own `pathtarget` exprs** ([`NodeId`] → fresh [`NodeId`]). A Path can
//!   carry a `pathtarget` distinct from its rel's `reltarget` (the `target` arg of
//!   `create_*_path`), so both must be remapped; missing the per-Path one left a
//!   subroot [`NodeId`] aliasing into `root`'s arena at the wrong index (it
//!   resolved a `TargetEntry` where `build_path_tlist` wanted an `Expr`);
//! - any `param_info.ppi_clauses` [`RestrictInfo`] handles.
//!
//! Every handle is memoized, so a node shared by several parents (the C aliased
//! `Path *`) is copied exactly once and the shared identity is preserved within
//! the destination arena.
//!
//! # Expression remapping
//!
//! The expression nodes ([`NodeId`] payloads — `Var`/`Const`/`OpExpr`/…)
//! reference range-table offsets (`varno`/`varnosyn`) **by value**. Set-op
//! planning runs over a single flat range table (the outer query's), and the
//! subroot's `Var`s already carry the rtable offsets valid in that flat space
//! (subquery_planner does not renumber them). So expression payloads are copied
//! **verbatim** — only the arena handle changes, never the `varno`. This is
//! faithful to C, where the same `Var *` is shared by pointer across the roots.

extern crate alloc;

use alloc::collections::BTreeMap;
use alloc::vec::Vec;

use types_pathnodes::{
    ArenaNode, EcId, EmId, EquivalenceClass, EquivalenceMember, NodeId, PathId, PathNode,
    PlannerInfo, RelId, RelOptInfo, RestrictInfo, RinfoId,
};

/// Recursive deep-copy + handle-remap state for one import operation.
///
/// Each `BTreeMap` memoizes "subroot handle → freshly-allocated root handle" so
/// a node reachable by more than one path (the C shared `Path *`/`RelOptInfo *`)
/// is imported once and its destination identity is preserved.
struct PathImporter<'a, 'mcx> {
    mcx: mcx::Mcx<'mcx>,
    sub: &'a PlannerInfo,
    path_map: BTreeMap<u32, PathId>,
    rel_map: BTreeMap<u32, RelId>,
    rinfo_map: BTreeMap<u32, RinfoId>,
    node_map: BTreeMap<u32, NodeId>,
    ec_map: BTreeMap<u32, EcId>,
    em_map: BTreeMap<u32, EmId>,
}

impl<'a, 'mcx> PathImporter<'a, 'mcx> {
    fn new(mcx: mcx::Mcx<'mcx>, sub: &'a PlannerInfo) -> Self {
        PathImporter {
            mcx,
            sub,
            path_map: BTreeMap::new(),
            rel_map: BTreeMap::new(),
            rinfo_map: BTreeMap::new(),
            node_map: BTreeMap::new(),
            ec_map: BTreeMap::new(),
            em_map: BTreeMap::new(),
        }
    }

    /// Import an [`EquivalenceClass`] (and its [`EquivalenceMember`]s + source
    /// expression / RestrictInfo handles) from the subroot into `root`,
    /// returning the fresh `root` [`EcId`]. Memoized. Pathkeys on imported paths
    /// reference subroot ECs; this makes them resolve in `root`.
    fn import_ec(&mut self, root: &mut PlannerInfo, sub_id: EcId) -> EcId {
        if let Some(&new_id) = self.ec_map.get(&sub_id.0) {
            return new_id;
        }
        // Allocate a placeholder first so a self/cyclic reference (ec_merged,
        // or an EM whose parent loops back) resolves to a stable id.
        let placeholder = root.alloc_ec(EquivalenceClass::default());
        self.ec_map.insert(sub_id.0, placeholder);

        let src: EquivalenceClass = self.sub.eq_classes[sub_id.index()].clone();
        let members: Vec<EmId> = src
            .ec_members
            .iter()
            .map(|&m| self.import_em(root, m))
            .collect();
        let childmembers: Vec<Vec<EmId>> = src
            .ec_childmembers
            .iter()
            .map(|v| v.iter().map(|&m| self.import_em(root, m)).collect())
            .collect();
        let sources: Vec<RinfoId> =
            src.ec_sources.iter().map(|&r| self.import_rinfo(root, r)).collect();
        let derives: Vec<RinfoId> =
            src.ec_derives_list.iter().map(|&r| self.import_rinfo(root, r)).collect();
        let merged = src.ec_merged.map(|m| self.import_ec(root, m));

        let dst = EquivalenceClass {
            ec_opfamilies: src.ec_opfamilies,
            ec_collation: src.ec_collation,
            ec_childmembers_size: src.ec_childmembers_size,
            ec_members: members,
            ec_childmembers: childmembers,
            ec_sources: sources,
            ec_derives_list: derives,
            // Opaque rebuildable cache; drop it (do not carry a subroot handle).
            ec_derives_hash: None,
            ec_relids: src.ec_relids,
            ec_has_const: src.ec_has_const,
            ec_has_volatile: src.ec_has_volatile,
            ec_broken: src.ec_broken,
            ec_sortref: src.ec_sortref,
            ec_min_security: src.ec_min_security,
            ec_max_security: src.ec_max_security,
            ec_merged: merged,
        };
        *root.ec_mut(placeholder) = dst;
        placeholder
    }

    /// Import an [`EquivalenceMember`] from the subroot into `root`. Memoized.
    fn import_em(&mut self, root: &mut PlannerInfo, sub_id: EmId) -> EmId {
        if let Some(&new_id) = self.em_map.get(&sub_id.0) {
            return new_id;
        }
        let placeholder = root.alloc_em(EquivalenceMember::default());
        self.em_map.insert(sub_id.0, placeholder);

        let src: EquivalenceMember = self.sub.em_arena[sub_id.index()].clone();
        let expr = self.import_node(root, src.em_expr);
        let parent = src.em_parent.map(|p| self.import_em(root, p));
        let dst = EquivalenceMember {
            em_expr: expr,
            em_relids: src.em_relids,
            em_is_const: src.em_is_const,
            em_is_child: src.em_is_child,
            em_datatype: src.em_datatype,
            em_jdomain: src.em_jdomain,
            em_parent: parent,
        };
        *root.em_mut(placeholder) = dst;
        placeholder
    }

    /// Import one expression-arena node ([`NodeId`]) from the subroot into
    /// `root`, returning the fresh root handle. Memoized.
    ///
    /// The payload is cloned verbatim (range-table offsets preserved; see the
    /// module note). Only the node kinds a set-op path subtree actually feeds —
    /// `Expr` (PathTarget exprs, RestrictInfo clauses, lateral vars),
    /// `TargetEntry`, and `SortGroupClause` — are imported; any other arena
    /// payload reachable here would be a model bug and panics loudly rather than
    /// silently dropping data.
    fn import_node(&mut self, root: &mut PlannerInfo, sub_id: NodeId) -> NodeId {
        if let Some(&new_id) = self.node_map.get(&sub_id.0) {
            return new_id;
        }
        let new_id = match &self.sub.node_arena[sub_id.index()] {
            // Deep-clone via `clone_in` (NOT the derived `.clone()`): an
            // `Expr::Aggref`/`SubPlan` carries context-allocated child lists whose
            // derived `Clone` is an intentional guard-panic — `clone_in`
            // dispatches them correctly (the #280 Aggref-clone convention).
            ArenaNode::Expr(e) => {
                let cloned = e
                    .clone_in(self.mcx)
                    .expect("import_path_from_subroot: Expr::clone_in failed");
                root.alloc_node(cloned)
            }
            ArenaNode::TargetEntry(te) => {
                // A TargetEntry's child `expr` is itself a NodeId into the same
                // arena; import it so the copy's handle resolves in `root`.
                let mut te = te.clone();
                te.expr = self.import_node(root, te.expr);
                root.alloc_targetentry(te)
            }
            ArenaNode::SortGroupClause(sgc) => root.alloc_sortgroupclause(*sgc),
            other => panic!(
                "import_path_from_subroot: NodeId {} resolves to an unsupported \
                 arena node ({other:?}) for cross-root import",
                sub_id.0
            ),
        };
        self.node_map.insert(sub_id.0, new_id);
        new_id
    }

    /// Import a [`RestrictInfo`] (and its clause expression nodes). Memoized.
    fn import_rinfo(&mut self, root: &mut PlannerInfo, sub_id: RinfoId) -> RinfoId {
        if let Some(&new_id) = self.rinfo_map.get(&sub_id.0) {
            return new_id;
        }
        let mut ri: RestrictInfo = self.sub.rinfo_arena[sub_id.index()].clone();
        ri.clause = self.import_node(root, ri.clause);
        if let Some(orc) = ri.orclause {
            ri.orclause = Some(self.import_node(root, orc));
        }
        // `scansel_cache` holds opaque non-Node handles that C replaces with NIL
        // on copy; drop them rather than dangle subroot ids.
        ri.scansel_cache = Vec::new();
        let new_id = root.alloc_rinfo(ri);
        self.rinfo_map.insert(sub_id.0, new_id);
        new_id
    }

    /// Import a [`RelOptInfo`] (reltarget exprs, base restrict/join clauses,
    /// lateral vars, and the parent/top_parent chain). Memoized.
    fn import_rel(&mut self, root: &mut PlannerInfo, sub_id: RelId) -> RelId {
        if let Some(&new_id) = self.rel_map.get(&sub_id.0) {
            return new_id;
        }
        // Allocate a placeholder slot first and record the mapping, so a
        // self/parent cycle in the rel chain terminates (the C graph never
        // cycles, but memoize-before-recurse is the safe discipline).
        let new_id = root.alloc_rel(RelOptInfo::default());
        self.rel_map.insert(sub_id.0, new_id);

        let src: RelOptInfo = self.sub.rel_arena[sub_id.index()].clone();
        let mut dst = src;

        if let Some(ref mut tgt) = dst.reltarget {
            let exprs: Vec<NodeId> = tgt
                .exprs
                .iter()
                .map(|&n| self.import_node(root, n))
                .collect();
            tgt.exprs = exprs;
        }
        dst.baserestrictinfo = dst
            .baserestrictinfo
            .iter()
            .map(|&r| self.import_rinfo(root, r))
            .collect();
        dst.joininfo = dst
            .joininfo
            .iter()
            .map(|&r| self.import_rinfo(root, r))
            .collect();
        dst.lateral_vars = dst
            .lateral_vars
            .iter()
            .map(|&n| self.import_node(root, n))
            .collect();
        if let Some(p) = dst.parent {
            dst.parent = Some(self.import_rel(root, p));
        }
        if let Some(p) = dst.top_parent {
            dst.top_parent = Some(self.import_rel(root, p));
        }
        // Cross-root references the set-op path subtree does not read are left
        // as-is would dangle into the subroot arena; null the ones that hold
        // foreign handles (subroot, partition trees) — set-op child paths are
        // flat scan/join paths whose RelOptInfo carries none of these live.
        dst.subroot = types_pathnodes::Subroot(None);
        dst.part_rels = Vec::new();

        *root.rel_mut(new_id) = dst;
        new_id
    }

    /// Import a [`PathId`] subtree. Memoized; recurses through every embedded
    /// subpath handle and the path's parent rel.
    fn import_path(&mut self, root: &mut PlannerInfo, sub_id: PathId) -> PathId {
        if let Some(&new_id) = self.path_map.get(&sub_id.0) {
            return new_id;
        }
        let mut node: PathNode = self.sub.path_arena[sub_id.index()].clone();

        // Remap the embedded base Path's parent rel.
        let parent = node.base().parent;
        let new_parent = self.import_rel(root, parent);
        node.base_mut().parent = new_parent;

        // Remap the path's own `pathtarget` exprs. These are `NodeId`s into the
        // subroot expression arena; without remapping they would alias into
        // `root`'s arena at the wrong indices (the bug that made a `SubqueryScan`
        // child's seqscan pathtarget resolve a `TargetEntry` where `build_path_tlist`
        // expects an `Expr`). C's path pathtarget shares the same `Node *` exprs
        // that `copyObject` would deep-copy on import; mirror that here. The parent
        // `RelOptInfo.reltarget` is imported separately in `import_rel`, but a Path
        // can carry its own pathtarget (create_*_path's `target` arg) distinct from
        // the rel's, so it must be remapped on the Path too.
        if let Some(sub_exprs) =
            node.base().pathtarget.as_deref().map(|t| t.exprs.clone())
        {
            let exprs: Vec<NodeId> =
                sub_exprs.iter().map(|&n| self.import_node(root, n)).collect();
            node.base_mut().pathtarget.as_deref_mut().unwrap().exprs = exprs;
        }

        // Remap `param_info.ppi_clauses` (RestrictInfo handles into the subroot
        // rinfo arena). NULL for the flat scan paths a set-op child carries, but
        // handle it for faithfulness with the cross-root copy contract.
        if let Some(sub_clauses) =
            node.base().param_info.as_deref().map(|p| p.ppi_clauses.clone())
        {
            let clauses: Vec<RinfoId> =
                sub_clauses.iter().map(|&r| self.import_rinfo(root, r)).collect();
            node.base_mut().param_info.as_deref_mut().unwrap().ppi_clauses = clauses;
        }

        // Remap the path's `pathkeys` — each carries a `pk_eclass` EcId into the
        // SUBROOT's equivalence-class arena, meaningless in `root`. Import the
        // referenced EC (and its members / source exprs) into `root` and rewrite
        // the EcId so a sorted subroot path (e.g. a subquery with an inner ORDER
        // BY) presents valid pathkeys to `convert_subquery_pathkeys`. Without
        // this, `root.ec(sub_eclass)` indexes out of bounds.
        {
            let mut new_pathkeys = node.base().pathkeys.clone();
            for pk in new_pathkeys.iter_mut() {
                if let Some(sub_ec) = pk.pk_eclass {
                    pk.pk_eclass = Some(self.import_ec(root, sub_ec));
                }
            }
            node.base_mut().pathkeys = new_pathkeys;
        }

        // Remap every subpath handle the variant carries.
        self.remap_subpaths(root, &mut node);

        let new_id = root.alloc_path(node);
        self.path_map.insert(sub_id.0, new_id);
        new_id
    }

    /// Recurse into and remap every [`PathId`] a path subtype embeds.
    fn remap_subpaths(&mut self, root: &mut PlannerInfo, node: &mut PathNode) {
        // Helper closures can't borrow `self` mutably twice, so import each
        // child id into a temporary then write it back.
        macro_rules! one {
            ($opt:expr) => {
                if let Some(id) = *$opt {
                    *$opt = Some(self.import_path(root, id));
                }
            };
        }
        macro_rules! many {
            ($vec:expr) => {{
                let imported: Vec<PathId> =
                    $vec.iter().map(|&id| self.import_path(root, id)).collect();
                $vec = imported;
            }};
        }

        match node {
            PathNode::Path(_)
            | PathNode::IndexPath(_)
            | PathNode::TidPath(_)
            | PathNode::TidRangePath(_)
            | PathNode::ForeignPath(_)
            | PathNode::CustomPath(_)
            | PathNode::GroupResultPath(_)
            | PathNode::MinMaxAggPath(_) => {
                // No PathId subpaths (leaf scans / degenerate result).
            }
            PathNode::BitmapHeapPath(p) => one!(&mut p.bitmapqual),
            PathNode::BitmapAndPath(p) => many!(p.bitmapquals),
            PathNode::BitmapOrPath(p) => many!(p.bitmapquals),
            PathNode::SubqueryScanPath(p) => {
                one!(&mut p.subpath);
                // The imported copy is a cost-only clone in the destination
                // root; its `subpath` was remapped above. The `subroot_subpath`
                // (an id into the *source* subroot's arena) is meaningless here
                // and must never be plan-built through this copy — drop it so a
                // stray dereference fails loudly rather than indexing the wrong
                // arena.
                p.subroot_subpath = None;
            }
            PathNode::NestPath(p) => {
                one!(&mut p.jpath.outerjoinpath);
                one!(&mut p.jpath.innerjoinpath);
            }
            PathNode::MergePath(p) => {
                one!(&mut p.jpath.outerjoinpath);
                one!(&mut p.jpath.innerjoinpath);
            }
            PathNode::HashPath(p) => {
                one!(&mut p.jpath.outerjoinpath);
                one!(&mut p.jpath.innerjoinpath);
            }
            PathNode::AppendPath(p) => many!(p.subpaths),
            PathNode::MergeAppendPath(p) => many!(p.subpaths),
            PathNode::MaterialPath(p) => one!(&mut p.subpath),
            PathNode::MemoizePath(p) => one!(&mut p.subpath),
            PathNode::UniquePath(p) => one!(&mut p.subpath),
            PathNode::GatherPath(p) => one!(&mut p.subpath),
            PathNode::GatherMergePath(p) => one!(&mut p.subpath),
            PathNode::ProjectionPath(p) => one!(&mut p.subpath),
            PathNode::ProjectSetPath(p) => one!(&mut p.subpath),
            PathNode::SortPath(p) => one!(&mut p.subpath),
            PathNode::IncrementalSortPath(p) => one!(&mut p.spath.subpath),
            PathNode::GroupPath(p) => one!(&mut p.subpath),
            PathNode::UpperUniquePath(p) => one!(&mut p.subpath),
            PathNode::AggPath(p) => one!(&mut p.subpath),
            PathNode::GroupingSetsPath(p) => one!(&mut p.subpath),
            PathNode::WindowAggPath(p) => one!(&mut p.subpath),
            PathNode::SetOpPath(p) => {
                one!(&mut p.leftpath);
                one!(&mut p.rightpath);
            }
            PathNode::RecursiveUnionPath(p) => {
                one!(&mut p.leftpath);
                one!(&mut p.rightpath);
            }
            PathNode::LockRowsPath(p) => one!(&mut p.subpath),
            PathNode::ModifyTablePath(p) => one!(&mut p.subpath),
            PathNode::LimitPath(p) => one!(&mut p.subpath),
            // `#[non_exhaustive]`: a path variant added later that carries a
            // subpath would silently leak a subroot handle, so fail loudly.
            other => panic!(
                "import_path_from_subroot: unhandled PathNode variant {other:?}; \
                 add its subpath remap"
            ),
        }
    }
}

/// Deep-copy the subroot path `sub_path_id` and its whole subtree — subpaths,
/// the parent [`RelOptInfo`]s (reltarget/restrictinfos/lateral vars), and the
/// clause expression nodes — from `subroot`'s arenas into `root`'s arenas,
/// remapping every [`PathId`]/[`RelId`]/[`RinfoId`]/[`NodeId`] to a fresh
/// `root` handle, and return the root [`PathId`].
///
/// The returned id resolves via `root.path(...)` and survives into
/// `create_plan` (its whole subtree lives in `root`'s arenas). Expression
/// payloads keep their range-table offsets verbatim — set-op planning runs over
/// the outer query's flat range table and the subroot `Var`s are already valid
/// in it (see the module-level note).
///
/// This is the enabler for `build_setop_child_paths` →
/// `create_subqueryscan_path(root, imported_id, …)`: the imported id resolves in
/// the outer root's arena, which a raw subroot id never could.
pub fn import_path_from_subroot(
    mcx: mcx::Mcx<'_>,
    root: &mut PlannerInfo,
    subroot: &PlannerInfo,
    sub_path_id: PathId,
) -> PathId {
    let mut importer = PathImporter::new(mcx, subroot);
    importer.import_path(root, sub_path_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use types_core::primitive::Cost;
    use types_nodes::nodes::NodeTag;
    use types_nodes::primnodes::{Const, Var};
    use types_pathnodes::{
        Path, PathTarget, Relids, SubqueryScanPath, VolatileFunctionStatus,
    };
    use types_tuple::backend_access_common_heaptuple::Datum;

    fn base_path(parent: RelId) -> Path {
        Path {
            type_: NodeTag(279),  // T_Path
            pathtype: NodeTag(339), // T_SeqScan
            parent,
            pathtarget: None,
            param_info: None,
            parallel_aware: false,
            parallel_safe: true,
            parallel_workers: 0,
            rows: 10.0,
            disabled_nodes: 0,
            startup_cost: 0.0 as Cost,
            total_cost: 100.0 as Cost,
            pathkeys: Vec::new(),
        }
    }

    /// Round-trip: build a small SubqueryScanPath subtree (subquery-scan over a
    /// seqscan leaf, each with a parent rel carrying a reltarget Var and a
    /// baserestrictinfo clause) in one PlannerInfo arena, import it into a
    /// second, and assert every handle resolves in the destination and nothing
    /// indexes the source arena.
    #[test]
    fn import_round_trips_subtree() {
        // ---- Build the subroot subtree. ----
        let mut sub = PlannerInfo::default();

        // Leaf rel with a reltarget Var(varno=3) and a baserestrictinfo whose
        // clause is a Const.
        let var_node = sub.alloc_node(types_nodes::primnodes::Expr::Var(Var {
            varno: 3,
            varattno: 1,
            vartype: 23,
            vartypmod: -1,
            varnosyn: 3,
            varattnosyn: 1,
            location: -1,
            ..Default::default()
        }));
        let clause_node = sub.alloc_node(types_nodes::primnodes::Expr::Const(Const {
            consttype: 16,
            consttypmod: -1,
            constcollid: 0,
            constlen: 1,
            constvalue: Datum::null(),
            constisnull: false,
            constbyval: true,
            location: -1,
        }));
        let ri = RestrictInfo {
            clause: clause_node,
            is_pushed_down: false,
            can_join: false,
            pseudoconstant: false,
            has_clone: false,
            is_clone: false,
            leakproof: false,
            has_volatile: VolatileFunctionStatus::default(),
            security_level: 0,
            num_base_rels: 0,
            clause_relids: Relids::default(),
            required_relids: Relids::default(),
            incompatible_relids: Relids::default(),
            outer_relids: Relids::default(),
            left_relids: Relids::default(),
            right_relids: Relids::default(),
            orclause: None,
            rinfo_serial: 0,
            parent_ec: None,
            eval_cost: Default::default(),
            norm_selec: -1.0,
            outer_selec: -1.0,
            mergeopfamilies: Vec::new(),
            left_ec: None,
            right_ec: None,
            left_em: None,
            right_em: None,
            scansel_cache: Vec::new(),
            outer_is_left: false,
            hashjoinoperator: 0,
            left_bucketsize: -1.0,
            right_bucketsize: -1.0,
            left_mcvfreq: -1.0,
            right_mcvfreq: -1.0,
            left_hasheqoperator: 0,
            right_hasheqoperator: 0,
        };
        let ri_id = sub.alloc_rinfo(ri);

        let mut leaf_rel = RelOptInfo::default();
        leaf_rel.reltarget = Some(Box::new(PathTarget {
            exprs: alloc::vec![var_node],
            sortgrouprefs: Vec::new(),
            cost: Default::default(),
            width: 4,
            has_volatile_expr: VolatileFunctionStatus::default(),
        }));
        leaf_rel.baserestrictinfo = alloc::vec![ri_id];
        let leaf_rel_id = sub.alloc_rel(leaf_rel);

        let leaf_path = sub.alloc_path(PathNode::Path(base_path(leaf_rel_id)));

        // SubqueryScan rel + path wrapping the leaf path.
        let sqs_rel = RelOptInfo::default();
        let sqs_rel_id = sub.alloc_rel(sqs_rel);
        let mut sqs_base = base_path(sqs_rel_id);
        sqs_base.pathtype = NodeTag(347); // T_SubqueryScan
        let sqs_path = sub.alloc_path(PathNode::SubqueryScanPath(SubqueryScanPath {
            path: sqs_base,
            subpath: Some(leaf_path),
            subroot_subpath: None,
        }));

        let sub_n_paths = sub.path_arena.len();
        let sub_n_rels = sub.rel_arena.len();

        // ---- Import into a fresh root. ----
        let mut root = PlannerInfo::default();
        // Pre-populate the root arena so source/dest indices differ (catches an
        // accidental "resolve in source arena" bug).
        let filler_rel = root.alloc_rel(RelOptInfo::default());
        root.alloc_path(PathNode::Path(base_path(filler_rel)));

        let ctx = mcx::MemoryContext::new("import-test");
        let imported = import_path_from_subroot(ctx.mcx(), &mut root, &sub, sqs_path);

        // The returned id resolves in ROOT.
        let pn = root.path(imported);
        assert!(matches!(pn, PathNode::SubqueryScanPath(_)));

        // Its subpath resolves in ROOT (not the source arena) and is the leaf.
        let sub_id = match pn {
            PathNode::SubqueryScanPath(p) => p.subpath.unwrap(),
            _ => unreachable!(),
        };
        assert!((sub_id.index()) < root.path_arena.len());
        let leaf = root.path(sub_id);
        assert!(matches!(leaf, PathNode::Path(_)));

        // The leaf's parent rel resolves in ROOT and carries the remapped
        // reltarget Var + baserestrictinfo clause, value-preserved.
        let leaf_parent = leaf.base().parent;
        assert!(leaf_parent.index() < root.rel_arena.len());
        let rel = root.rel(leaf_parent);
        let tgt = rel.reltarget.as_ref().unwrap();
        assert_eq!(tgt.exprs.len(), 1);
        match root.node(tgt.exprs[0]) {
            types_nodes::primnodes::Expr::Var(v) => assert_eq!(v.varno, 3),
            _ => panic!("reltarget expr not a Var"),
        }
        assert_eq!(rel.baserestrictinfo.len(), 1);
        let imported_ri = rel.baserestrictinfo[0];
        assert!(imported_ri.index() < root.rinfo_arena.len());
        match root.node(root.rinfo(imported_ri).clause) {
            types_nodes::primnodes::Expr::Const(_) => {}
            _ => panic!("clause not a Const"),
        }

        // The source arenas were untouched (pure copy, no move).
        assert_eq!(sub.path_arena.len(), sub_n_paths);
        assert_eq!(sub.rel_arena.len(), sub_n_rels);
    }
}
