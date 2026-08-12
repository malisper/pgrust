//! Statement-level dispatch: a statement group is produced by a weighted
//! choice among registered statement modules (crate::toggles owns the
//! on/off toggles and module-selection weights; this registry owns the
//! generator functions). Most modules emit one statement per call; the txn
//! module emits a whole BEGIN..COMMIT/ROLLBACK bracket. Adding a module is
//! three steps:
//!
//!   1. write `fn gen_<name>_module(&mut Gen) -> Vec<StmtKind>` in its own
//!      file and register it in `STMT_MODULES` below;
//!   2. add a `ModuleSpec` with the same name to `toggles::REGISTRY`;
//!   3. add the module's weighted productions to `weights::PROD_WEIGHTS`.
//!
//! (`registries_are_in_sync` fails the build-out if 1 and 2 drift.)
//!
//! `Gen` is the per-group generation context: the session PRNG, the
//! catalog snapshot, the weight table, the fired-production sink, the
//! statement-unique alias allocator, and the session-persistent DML state
//! (crate::dml — the session loop swaps it in and out so pk allocation
//! survives across groups). All randomness flows through `Gen::rng`, so
//! same seed + same toggles/weights = byte-identical stream.

use crate::admin::gen_admin_module;
use crate::adtmisc::gen_adtmisc_module;
use crate::agg::gen_agg_stmt;
use crate::catalog::{Catalog, Table};
use crate::coll::{gen_coll_module, CollState};
use crate::cursor::{gen_cursor_module, CursorState};
use crate::ddl::{gen_ddl_module, DdlState};
use crate::dml::{gen_dml_module, DmlState};
use crate::dtm::gen_dtm_module;
use crate::einterp::gen_einterp_module;
use crate::exd::gen_exd_module;
use crate::earm::gen_earm_module;
use crate::earm2::gen_earm2_module;
use crate::explain::gen_explain_module;
use crate::exr::{gen_exr_module, ExrState};
use crate::geo::{gen_geo_module, GeoState};
use crate::idx::{gen_idx_module, IdxState};
use crate::join::gen_join_stmt;
use crate::mbconv::gen_mbconv_module;
use crate::merge::gen_merge_module;
use crate::nodes::gen_nodes_module;
use crate::objddl::{gen_objddl_module, ObjState};
use crate::numx::gen_numx_module;
use crate::objid::gen_objid_module;
use crate::obs::gen_obs_module;
use crate::par::{gen_par_module, ParState};
use crate::part::{gen_part_module, PartState};
use crate::plansel::{gen_plansel_module, PlanState};
use crate::plpg::{gen_plpg_module, PlpgState};
use crate::pubsub::gen_pubsub_module;
use crate::render::{FromItem, OrderKey, SelectItem, SelectStmt};
use crate::rng::Rng;
use crate::scope::{Scope, ScopeRel};
use crate::spill::{gen_spill_module, SpillState};
use crate::sqljson::gen_sqljson_module;
use crate::subq::gen_subq_stmt;
use crate::tsdl::{gen_tsdl_module, TsState};
use crate::txn::gen_txn_module;
use crate::types_stmt::gen_types_module;
use crate::util::gen_util_module;
use crate::views::{gen_views_module, ViewsState};
use crate::weights::WeightTable;
use crate::win::gen_win_stmt;
use crate::xnum::gen_xnum_module;

pub struct Gen<'a> {
    pub rng: &'a mut Rng,
    pub catalog: &'a Catalog,
    pub weights: &'a WeightTable,
    pub productions: &'a mut Vec<String>,
    pub max_depth: u32,
    /// Remaining subquery nesting budget: expression-level subquery
    /// productions (scalar/IN/EXISTS) are offered only while > 0. The
    /// subq module raises it; other modules leave it at 0.
    pub subq_depth: u32,
    /// Session-persistent DML metadata (pk allocation, plausible-row
    /// tracking). `Gen::new` seeds a fresh one from the catalog; the
    /// session loop swaps the long-lived one in for each group.
    pub dml: DmlState,
    /// Session-persistent DDL catalog model (created tables/indexes/
    /// sequences); swapped in and out by the session loop like `dml`.
    pub ddl: DdlState,
    /// Session-persistent partitioned-parent model (crate::part); swapped
    /// in and out by the session loop like `ddl`.
    pub part: PartState,
    /// Session-persistent object/utility DDL model (crate::objddl — roles,
    /// created types, statistics objects); swapped like `ddl`.
    pub obj: ObjState,
    /// Session-persistent index-AM table model (crate::idx); swapped in and
    /// out by the session loop like `ddl`.
    pub idx: IdxState,
    /// Session-persistent parallel-query table model (crate::par); swapped
    /// in and out by the session loop like `idx`.
    pub par: ParState,
    /// Session-persistent text-search object model (crate::tsdl); swapped
    /// in and out by the session loop like `ddl`.
    pub ts: TsState,
    /// Session-persistent cursor/prepared-statement model (crate::cursor:
    /// live WITH HOLD cursors and the prepared-statement pool); swapped in
    /// and out by the session loop like `ddl`.
    pub cursor: CursorState,
    /// Session-persistent views/rules model (crate::views); swapped in and
    pub views: ViewsState,
    /// Session-persistent geo-table model (crate::geo); swapped in and out
    /// by the session loop like `idx`.
    pub geo: GeoState,
    /// Session-persistent plpgsql-module name counters (crate::plpg —
    /// objects are group-local; only the counters persist); swapped in and
    /// out by the session loop like `ddl`.
    pub plpg: PlpgState,
    /// Session-persistent collation-module name counters (crate::coll —
    /// objects are group-local; only the counters persist); swapped in and
    /// out by the session loop like `plpg`.
    pub coll: CollState,
    /// Session-persistent spill-table model (crate::spill, LD5); swapped
    /// in and out by the session loop like `par`.
    pub spill: SpillState,
    /// Session-persistent plan-selection fixture-set model
    /// (crate::plansel, LD7); swapped in and out by the session loop like
    /// `spill`.
    pub plan: PlanState,
    /// Session-persistent executor-residue fixture model (crate::exr,
    /// LD9); swapped in and out by the session loop like `spill`.
    pub exr: ExrState,
    alias_n: u32,
    cte_n: u32,
}

impl<'a> Gen<'a> {
    pub fn new(
        rng: &'a mut Rng,
        catalog: &'a Catalog,
        weights: &'a WeightTable,
        productions: &'a mut Vec<String>,
        max_depth: u32,
    ) -> Gen<'a> {
        Gen {
            rng,
            catalog,
            weights,
            productions,
            max_depth,
            subq_depth: 0,
            dml: DmlState::new(catalog),
            ddl: DdlState::new(),
            part: PartState::new(),
            obj: ObjState::new(),
            idx: IdxState::new(),
            par: ParState::new(),
            ts: TsState::new(),
            cursor: CursorState::new(),
            views: ViewsState::new(),
            geo: GeoState::new(),
            plpg: PlpgState::new(),
            coll: CollState::new(),
            spill: SpillState::new(),
            plan: PlanState::new(),
            exr: ExrState::new(),
            alias_n: 0,
            cte_n: 0,
        }
    }

    pub fn fire(&mut self, prod: &str) {
        self.productions.push(prod.to_string());
    }

    pub fn fire2(&mut self, prefix: &str, suffix: &str) {
        self.productions.push(format!("{}{}", prefix, suffix));
    }

    /// Statement-unique relation alias (unique across nesting levels too,
    /// so correlated references can never be shadowed).
    pub fn next_alias(&mut self) -> String {
        let n = self.alias_n;
        self.alias_n += 1;
        format!("t{}", n)
    }

    /// Statement-unique CTE name.
    pub fn next_cte_name(&mut self) -> String {
        let n = self.cte_n;
        self.cte_n += 1;
        format!("w{}", n)
    }

    pub fn pick_table(&mut self) -> &'a Table {
        &self.catalog.tables[self.rng.below_usize(self.catalog.tables.len())]
    }
}

/// One generated statement: SELECTs keep their AST (the scoping checker
/// runs against it); DML and transaction-control statements are rendered
/// text (their validity gates are textual invariants + the differential
/// runs themselves).
#[derive(Clone, Debug)]
pub enum StmtKind {
    Select(Box<SelectStmt>),
    Raw(String),
}

impl StmtKind {
    pub fn to_sql(&self) -> String {
        match self {
            StmtKind::Select(s) => s.to_sql(),
            StmtKind::Raw(s) => s.clone(),
        }
    }
}

pub struct StmtModuleDef {
    pub name: &'static str,
    pub generate: fn(&mut Gen) -> Vec<StmtKind>,
}

/// All statement modules, parallel in names to `toggles::REGISTRY`.
pub const STMT_MODULES: &[StmtModuleDef] = &[
    StmtModuleDef { name: "expr", generate: gen_expr_module },
    StmtModuleDef { name: "joins", generate: gen_join_module },
    StmtModuleDef { name: "subq", generate: gen_subq_module },
    StmtModuleDef { name: "agg", generate: gen_agg_module },
    StmtModuleDef { name: "win", generate: gen_win_module },
    StmtModuleDef { name: "dml", generate: gen_dml_module },
    StmtModuleDef { name: "merge", generate: gen_merge_module },
    StmtModuleDef { name: "txn", generate: gen_txn_module },
    StmtModuleDef { name: "ddl", generate: gen_ddl_module },
    StmtModuleDef { name: "types", generate: gen_types_module },
    StmtModuleDef { name: "explain", generate: gen_explain_module },
    StmtModuleDef { name: "util", generate: gen_util_module },
    StmtModuleDef { name: "part", generate: gen_part_module },
    StmtModuleDef { name: "objddl", generate: gen_objddl_module },
    StmtModuleDef { name: "idx", generate: gen_idx_module },
    StmtModuleDef { name: "par", generate: gen_par_module },
    StmtModuleDef { name: "tsdl", generate: gen_tsdl_module },
    StmtModuleDef { name: "cursor", generate: gen_cursor_module },
    StmtModuleDef { name: "views", generate: gen_views_module },
    StmtModuleDef { name: "geo", generate: gen_geo_module },
    StmtModuleDef { name: "dtm", generate: gen_dtm_module },
    StmtModuleDef { name: "adtmisc", generate: gen_adtmisc_module },
    StmtModuleDef { name: "sqljson", generate: gen_sqljson_module },
    StmtModuleDef { name: "plpg", generate: gen_plpg_module },
    StmtModuleDef { name: "coll", generate: gen_coll_module },
    StmtModuleDef { name: "mbconv", generate: gen_mbconv_module },
    StmtModuleDef { name: "xnum", generate: gen_xnum_module },
    StmtModuleDef { name: "nodes", generate: gen_nodes_module },
    StmtModuleDef { name: "obs", generate: gen_obs_module },
    StmtModuleDef { name: "admin", generate: gen_admin_module },
    StmtModuleDef { name: "objid", generate: gen_objid_module },
    StmtModuleDef { name: "einterp", generate: gen_einterp_module },
    StmtModuleDef { name: "exd", generate: gen_exd_module },
    StmtModuleDef { name: "spill", generate: gen_spill_module },
    StmtModuleDef { name: "earm", generate: gen_earm_module },
    StmtModuleDef { name: "plansel", generate: gen_plansel_module },
    StmtModuleDef { name: "earm2", generate: gen_earm2_module },
    StmtModuleDef { name: "exr", generate: gen_exr_module },
    StmtModuleDef { name: "numx", generate: gen_numx_module },
    StmtModuleDef { name: "pubsub", generate: gen_pubsub_module },
];

/// Produce one statement group from the named module (the toggle vector
/// picked it, so it must be registered). The session layer renders each
/// statement and derives per-statement compare metadata
/// (render::soft_float_cols on the SELECT ASTs).
pub fn gen_statements(module: &str, g: &mut Gen) -> Vec<StmtKind> {
    let def = STMT_MODULES
        .iter()
        .find(|m| m.name == module)
        .unwrap_or_else(|| unreachable!("module {} toggled but not registered", module));
    (def.generate)(g)
}

fn gen_join_module(g: &mut Gen) -> Vec<StmtKind> {
    vec![StmtKind::Select(Box::new(gen_join_stmt(g)))]
}

fn gen_subq_module(g: &mut Gen) -> Vec<StmtKind> {
    // Recursive CTEs (SEARCH/CYCLE) are self-contained raw statements; the
    // AST shapes carry the scoped-subquery surface.
    if g.weights.pick(g.rng, &["subq:ast", "subq:rec"]) == "subq:rec" {
        vec![crate::subq::gen_recursive_cte_stmt(g)]
    } else {
        vec![StmtKind::Select(Box::new(gen_subq_stmt(g)))]
    }
}

fn gen_expr_module(g: &mut Gen) -> Vec<StmtKind> {
    vec![StmtKind::Select(Box::new(gen_expr_stmt(g)))]
}

fn gen_agg_module(g: &mut Gen) -> Vec<StmtKind> {
    // Q7 agg-tweaks: a weighted split between the standing AST aggregate
    // statement and the raw aggregate-breadth families (crate::agg::
    // gen_aggx_stmts — ordered-set/hypothetical aggs, regr matrix, hash/
    // minmax breadth, Group-node and grouping-set tails).
    if g.weights.pick(g.rng, &["agg:ast", "agg:aggx"]) == "agg:aggx" {
        return crate::agg::gen_aggx_stmts(g);
    }
    vec![StmtKind::Select(Box::new(gen_agg_stmt(g)))]
}

fn gen_win_module(g: &mut Gen) -> Vec<StmtKind> {
    vec![StmtKind::Select(Box::new(gen_win_stmt(g)))]
}

/// The expr module: a single-table SELECT (the F0 shape, now alias-scoped).
/// Also the txn module's read-statement generator and the dml module's
/// fallback when a catalog exposes no DML-eligible table.
pub(crate) fn gen_expr_stmt(g: &mut Gen) -> SelectStmt {
    let table = g.pick_table();
    let alias = g.next_alias();
    let rels = vec![ScopeRel::from_table(table, alias.clone())];
    let from = FromItem::Table { name: table.name.clone(), alias };
    finish_select(g, &rels, from, Vec::new())
}

/// Shared statement core: SELECT list + optional WHERE over an established
/// scope, then the ORDER BY/LIMIT/OFFSET suffix. Every module funnels
/// through here so the suffix rules (and their compare-safety invariants)
/// hold uniformly.
pub fn finish_select(
    g: &mut Gen,
    rels: &[ScopeRel],
    from: FromItem,
    ctes: Vec<(String, SelectStmt)>,
) -> SelectStmt {
    g.fire("select");
    let scope = Scope { rels, outer: None };
    let ncols = 1 + g.rng.below(4) as usize;
    let mut items = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        let ty = g.any_type(&scope);
        let expr = g.gen_typed(&scope, ty, g.max_depth);
        items.push(SelectItem { expr, alias: None, ty });
    }
    let where_clause = if g.rng.chance(1, 2) {
        g.fire("where");
        Some(g.gen_bool(&scope, g.max_depth))
    } else {
        None
    };
    let mut stmt = SelectStmt { ctes, items, from: Some(from), where_clause, ..Default::default() };
    order_limit_suffix(g, &mut stmt);
    stmt
}

/// ORDER BY/LIMIT/OFFSET production. Compare-safety invariants (the F1
/// differ treats ORDER BY statements as ordered-compare):
///   - a *total* ORDER BY covers every output column, so any residual tie
///     is between identical rows — safe under LIMIT;
///   - a *partial* ORDER BY underdetermines order (ruled tie-order surface,
///     not a finding) and therefore never gets LIMIT/OFFSET, which would
///     underdetermine the row *set*;
///   - LIMIT/OFFSET also require float-free output columns: equal-comparing
///     but textually distinct sort keys (-0 vs 0) could otherwise cut the
///     row set differently across implementations.
pub(crate) fn order_limit_suffix(g: &mut Gen, stmt: &mut SelectStmt) {
    let shape = g
        .weights
        .pick(g.rng, &["orderby:none", "orderby:total", "orderby:partial"]);
    if shape == "orderby:none" {
        return;
    }
    g.fire("orderby");
    g.fire(shape);
    let n = stmt.items.len();
    let positions: Vec<usize> = if shape == "orderby:total" {
        (1..=n).collect()
    } else {
        // Non-empty contiguous run of output positions (may coincide with
        // the whole list on 1-column selects — still recorded as partial;
        // it never carries LIMIT either way).
        let start = g.rng.below_usize(n);
        let take = 1 + g.rng.below_usize(n - start);
        (start + 1..=start + take).collect()
    };
    for position in positions {
        let desc = match g.weights.pick(g.rng, &["orderby:asc", "orderby:desc"]) {
            "orderby:desc" => {
                g.fire("orderby:desc");
                Some(true)
            }
            _ => {
                if g.rng.chance(1, 4) {
                    Some(false) // explicit ASC
                } else {
                    None
                }
            }
        };
        let nulls_first = match g.weights.pick(
            g.rng,
            &["orderby:nulls_default", "orderby:nulls_first", "orderby:nulls_last"],
        ) {
            "orderby:nulls_first" => {
                g.fire("orderby:nulls_first");
                Some(true)
            }
            "orderby:nulls_last" => {
                g.fire("orderby:nulls_last");
                Some(false)
            }
            _ => None,
        };
        stmt.order_by.push(OrderKey { position, desc, nulls_first });
    }
    // LIMIT/OFFSET: total ORDER BY + float-free output only (see above).
    let has_float = stmt.items.iter().any(|i| i.ty.is_float());
    if shape != "orderby:total" || has_float {
        return;
    }
    if g.weights.pick(g.rng, &["limit", "limit:none"]) == "limit" {
        g.fire("limit");
        stmt.limit = Some(boundary_count(g, 9000000000));
    }
    if g.weights.pick(g.rng, &["offset", "offset:none"]) == "offset" {
        g.fire("offset");
        stmt.offset = Some(boundary_count(g, 10000000));
    }
}

/// Boundary-heavy row count for LIMIT/OFFSET: 0, 1, huge, or small random.
fn boundary_count(g: &mut Gen, huge: u64) -> String {
    match g.rng.below(4) {
        0 => "0".to_string(),
        1 => "1".to_string(),
        2 => huge.to_string(),
        _ => (2 + g.rng.below(9)).to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::render::scope_errors;
    use crate::toggles::REGISTRY;

    #[test]
    fn registries_are_in_sync() {
        let module_names: Vec<&str> = STMT_MODULES.iter().map(|m| m.name).collect();
        let toggle_names: Vec<&str> = REGISTRY.iter().map(|m| m.name).collect();
        assert_eq!(
            module_names, toggle_names,
            "stmt::STMT_MODULES and toggles::REGISTRY must list the same modules"
        );
    }

    /// The load-bearing scoping gate: every module's SELECT statements pass
    /// the AST checker (in-scope aliases only, uncorrelated FROM subqueries,
    /// in-range ORDER BY, no nested ORDER BY/LIMIT/CTEs). Raw statements
    /// (DML, transaction control) get textual sanity checks; their semantic
    /// gates are the dml/txn module tests and the null differential.
    #[test]
    fn all_modules_generate_scoped_statements() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        for module in STMT_MODULES {
            let mut rng = Rng::new(0xF4A);
            for i in 0..300 {
                let mut prods = Vec::new();
                let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
                let stmts = (module.generate)(&mut g);
                assert!(!stmts.is_empty(), "module {} produced no statements", module.name);
                for stmt in &stmts {
                    match stmt {
                        StmtKind::Select(s) => {
                            let errs = scope_errors(s, &cat);
                            assert!(
                                errs.is_empty(),
                                "module {} stmt {}: {:?}\nsql: {}",
                                module.name,
                                i,
                                errs,
                                s.to_sql()
                            );
                        }
                        StmtKind::Raw(_) => {}
                    }
                    let sql = stmt.to_sql();
                    assert!(!sql.contains('\n'), "multi-line statement: {sql}");
                    assert!(sql.ends_with(';'), "unterminated statement: {sql}");
                    assert_eq!(
                        sql.matches('(').count(),
                        sql.matches(')').count(),
                        "unbalanced parens: {sql}"
                    );
                }
            }
        }
    }

    /// Suffix invariants: LIMIT/OFFSET imply a total ORDER BY over
    /// float-free output; partial ORDER BY never carries LIMIT.
    #[test]
    fn limit_requires_total_float_free_order() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        // Bias hard toward ORDER BY + LIMIT to exercise the suffix.
        let w = WeightTable::parse(
            "orderby:none=1,orderby:total=4,orderby:partial=2,limit=5,limit:none=1,\
             offset=3,offset:none=1",
        )
        .unwrap();
        let mut rng = Rng::new(99);
        let mut saw_limit = false;
        let mut saw_partial = false;
        for module in STMT_MODULES {
            for _ in 0..300 {
                let mut prods = Vec::new();
                let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 3);
                let stmts = (module.generate)(&mut g);
                for kind in &stmts {
                    let StmtKind::Select(stmt) = kind else { continue };
                    if stmt.limit.is_some() || stmt.offset.is_some() {
                        saw_limit = true;
                        let positions: Vec<usize> =
                            stmt.order_by.iter().map(|k| k.position).collect();
                        for p in 1..=stmt.items.len() {
                            assert!(
                                positions.contains(&p),
                                "LIMIT with non-total ORDER BY: {}",
                                stmt.to_sql()
                            );
                        }
                        assert!(
                            stmt.items.iter().all(|i| !i.ty.is_float()),
                            "LIMIT over float output: {}",
                            stmt.to_sql()
                        );
                    }
                }
                if prods.iter().any(|p| p == "orderby:partial") {
                    for kind in &stmts {
                        if let StmtKind::Select(stmt) = kind {
                            if stmt
                                .order_by
                                .iter()
                                .map(|k| k.position)
                                .collect::<Vec<_>>()
                                .len()
                                < stmt.items.len()
                            {
                                saw_partial = true;
                                assert!(stmt.limit.is_none() && stmt.offset.is_none());
                            }
                        }
                    }
                }
            }
        }
        assert!(saw_limit, "LIMIT never fired under heavy bias");
        assert!(saw_partial, "partial ORDER BY never fired under heavy bias");
    }

    #[test]
    fn aliases_are_statement_unique() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(7);
        let mut prods = Vec::new();
        let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
        let a = g.next_alias();
        let b = g.next_alias();
        let c = g.next_cte_name();
        let d = g.next_cte_name();
        assert_ne!(a, b);
        assert_ne!(c, d);
        assert!(a.starts_with('t') && c.starts_with('w'));
    }
}
