//! Session plumbing: (seed, toggle vector, statement budget) → deterministic
//! statement stream with per-statement production metadata. Modules emit
//! statement *groups* (usually one statement; the txn module emits a whole
//! BEGIN..COMMIT bracket) — a group started inside the budget always
//! completes, so brackets are never truncated and the stream may run a few
//! statements past the budget. DML metadata (crate::dml::DmlState) persists
//! across groups so pk allocation stays collision-controlled stream-wide.

use crate::catalog::Catalog;
use crate::coll::CollState;
use crate::ddl::{DdlEventKind, DdlState};
use crate::dml::DmlState;
use crate::objddl::ObjState;
use crate::geo::GeoState;
use crate::idx::IdxState;
use crate::par::ParState;
use crate::plansel::PlanState;
use crate::exr::ExrState;
use crate::spill::SpillState;
use crate::cursor::CursorState;
use crate::part::PartState;
use crate::plpg::PlpgState;
use crate::tsdl::TsState;
use crate::render::soft_float_cols;
use crate::rng::Rng;
use crate::stmt::{gen_statements, Gen};
use crate::toggles::ToggleVector;
use crate::views::ViewsState;
use crate::weights::WeightTable;

#[derive(Clone, Debug)]
pub struct SessionConfig {
    pub seed: u64,
    pub toggles: ToggleVector,
    pub weights: WeightTable,
    pub budget: u32,
    pub max_depth: u32,
}

#[derive(Clone, Debug)]
pub struct Statement {
    pub stmt_index: u32,
    pub sql: String,
    /// Sorted, deduplicated production names that fired for this
    /// statement's group (statements of one group share the list).
    pub productions: Vec<String>,
    /// Output columns holding order-sensitive float aggregates: the differ
    /// compares these ruled-soft (render::soft_float_cols; B1 ruling).
    pub soft_float_cols: Vec<usize>,
}

/// Existence window of one ddl-created table: it exists for probe rounds
/// whose last-applied statement index `i` satisfies `i >= from` and
/// `until.is_none_or(|u| i < u)` (`from` = the CREATE statement's index,
/// `until` = the DROP statement's index). Fed into the runner's ProbeSpec
/// so state probes cover exactly the ddl tables alive at probe time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DdlWindow {
    pub table: String,
    pub pk: String,
    pub from: u32,
    pub until: Option<u32>,
}

/// A generated session: the statement stream plus the ddl-table probe
/// windows the runner needs.
#[derive(Clone, Debug)]
pub struct SessionOutput {
    pub stmts: Vec<Statement>,
    pub ddl_windows: Vec<DdlWindow>,
}

/// Generate the full statement stream for a session. Same config (seed,
/// toggles, budget, depth) + same catalog = byte-identical output.
pub fn run_session(cfg: &SessionConfig, catalog: &Catalog) -> Vec<Statement> {
    run_session_probed(cfg, catalog).stmts
}

/// `run_session` plus ddl probe windows. DDL and partitioning groups mutate
/// the generation catalog: after each one the effective catalog is rebuilt
/// (fixture tables plus live ddl tables plus live partitioned parents) so
/// every later group — any module — can target the created tables, and the
/// DML state is re-synced to it by table name. Table create/drop events
/// resolve to the group's base statement index (a table-lifecycle group's
/// creating/dropping statement is its first).
pub fn run_session_probed(cfg: &SessionConfig, catalog: &Catalog) -> SessionOutput {
    let mut rng = Rng::new(cfg.seed);
    let mut dml_state = DmlState::new(catalog);
    let mut ddl_state = DdlState::new();
    let mut part_state = PartState::new();
    let mut obj_state = ObjState::new();
    let mut idx_state = IdxState::new();
    let mut par_state = ParState::new();
    let mut spill_state = SpillState::new();
    let mut plan_state = PlanState::new();
    let mut exr_state = ExrState::new();
    let mut geo_state = GeoState::new();
    let mut ts_state = TsState::new();
    let mut cursor_state = CursorState::new();
    let mut views_state = ViewsState::new();
    let mut plpg_state = PlpgState::new();
    let mut coll_state = CollState::new();
    let mut eff_catalog = catalog.clone();
    let mut windows: Vec<DdlWindow> = Vec::new();
    let mut out: Vec<Statement> = Vec::with_capacity(cfg.budget as usize);
    while (out.len() as u32) < cfg.budget {
        let module = cfg.toggles.pick_module(&mut rng);
        let mut productions = vec![format!("module:{}", module)];
        let group_base = out.len() as u32;
        let kinds = {
            let mut g = Gen::new(
                &mut rng,
                &eff_catalog,
                &cfg.weights,
                &mut productions,
                cfg.max_depth,
            );
            std::mem::swap(&mut g.dml, &mut dml_state);
            std::mem::swap(&mut g.ddl, &mut ddl_state);
            std::mem::swap(&mut g.part, &mut part_state);
            std::mem::swap(&mut g.obj, &mut obj_state);
            std::mem::swap(&mut g.idx, &mut idx_state);
            std::mem::swap(&mut g.par, &mut par_state);
            std::mem::swap(&mut g.spill, &mut spill_state);
            std::mem::swap(&mut g.plan, &mut plan_state);
            std::mem::swap(&mut g.exr, &mut exr_state);
            std::mem::swap(&mut g.geo, &mut geo_state);
            std::mem::swap(&mut g.ts, &mut ts_state);
            std::mem::swap(&mut g.cursor, &mut cursor_state);
            std::mem::swap(&mut g.views, &mut views_state);
            std::mem::swap(&mut g.plpg, &mut plpg_state);
            std::mem::swap(&mut g.coll, &mut coll_state);
            let kinds = gen_statements(module, &mut g);
            std::mem::swap(&mut g.coll, &mut coll_state);
            std::mem::swap(&mut g.plpg, &mut plpg_state);
            std::mem::swap(&mut g.cursor, &mut cursor_state);
            std::mem::swap(&mut g.dml, &mut dml_state);
            std::mem::swap(&mut g.ddl, &mut ddl_state);
            std::mem::swap(&mut g.part, &mut part_state);
            std::mem::swap(&mut g.obj, &mut obj_state);
            std::mem::swap(&mut g.idx, &mut idx_state);
            std::mem::swap(&mut g.par, &mut par_state);
            std::mem::swap(&mut g.spill, &mut spill_state);
            std::mem::swap(&mut g.plan, &mut plan_state);
            std::mem::swap(&mut g.exr, &mut exr_state);
            std::mem::swap(&mut g.geo, &mut geo_state);
            std::mem::swap(&mut g.ts, &mut ts_state);
            std::mem::swap(&mut g.views, &mut views_state);
            kinds
        };
        for ev in ddl_state
            .take_events()
            .into_iter()
            .chain(part_state.take_events())
            .chain(idx_state.take_events())
            .chain(par_state.take_events())
            .chain(spill_state.take_events())
            .chain(plan_state.take_events())
            .chain(exr_state.take_events())
            .chain(geo_state.take_events())
            .chain(views_state.take_events())
        {
            match ev.kind {
                DdlEventKind::Created => windows.push(DdlWindow {
                    table: ev.table,
                    pk: ev.pk,
                    from: group_base,
                    until: None,
                }),
                DdlEventKind::Dropped => {
                    if let Some(w) = windows
                        .iter_mut()
                        .rev()
                        .find(|w| w.table == ev.table && w.until.is_none())
                    {
                        w.until = Some(group_base);
                    }
                }
            }
        }
        productions.sort();
        productions.dedup();
        for kind in kinds {
            let stmt_index = out.len() as u32;
            let soft = match &kind {
                crate::stmt::StmtKind::Select(sel) => soft_float_cols(sel),
                crate::stmt::StmtKind::Raw(_) => Vec::new(),
            };
            out.push(Statement {
                stmt_index,
                sql: kind.to_sql(),
                productions: productions.clone(),
                soft_float_cols: soft,
            });
        }
        if module == "ddl" || module == "part" || module == "views" {
            eff_catalog = views_state
                .extend_catalog(&part_state.extend_catalog(&ddl_state.extend_catalog(catalog)));
            dml_state.sync(&eff_catalog);
        }
    }
    SessionOutput { stmts: out, ddl_windows: windows }
}

/// Minimal JSON string escaping for JSONL output.
pub fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// One JSONL record: {"seed":..,"stmt_index":..,"sql":"..","productions":[..]}
pub fn jsonl_record(seed: u64, stmt: &Statement) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "{{\"seed\":{},\"stmt_index\":{},\"sql\":\"{}\",\"productions\":[",
        seed,
        stmt.stmt_index,
        json_escape(&stmt.sql)
    ));
    for (i, p) in stmt.productions.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('"');
        out.push_str(&json_escape(p));
        out.push('"');
    }
    out.push_str("]}");
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};

    fn cfg(seed: u64) -> SessionConfig {
        SessionConfig {
            seed,
            toggles: ToggleVector::all_on(),
            weights: WeightTable::defaults(),
            budget: 80,
            max_depth: 4,
        }
    }

    #[test]
    fn same_seed_byte_identical() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let a = run_session(&cfg(1234), &cat);
        let b = run_session(&cfg(1234), &cat);
        let render = |stmts: &[Statement]| {
            stmts
                .iter()
                .map(|s| jsonl_record(1234, s))
                .collect::<Vec<_>>()
                .join("\n")
        };
        assert_eq!(render(&a), render(&b));
    }

    #[test]
    fn different_seeds_differ() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let a = run_session(&cfg(1), &cat);
        let b = run_session(&cfg(2), &cat);
        let sqls = |stmts: &[Statement]| stmts.iter().map(|s| s.sql.clone()).collect::<Vec<_>>();
        assert_ne!(sqls(&a), sqls(&b));
    }

    #[test]
    fn budget_and_metadata() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let mut c = cfg(12);
        // 14 modules with sub-1.0 weights on the bulky ones: 240 statements
        // is where every module reliably fires for this seed.
        c.budget = 240;
        let stmts = run_session(&c, &cat);
        // Groups complete past the budget: at least the budget, and no more
        // than one whole group beyond it (the LD8 earm2 verbatim sections
        // are the largest groups in the registry at up to ~60 statements).
        assert!(stmts.len() >= 240 && stmts.len() < 240 + 61, "len {}", stmts.len());
        for (i, s) in stmts.iter().enumerate() {
            assert_eq!(s.stmt_index as usize, i);
            assert!(s.productions.iter().any(|p| p.starts_with("module:")));
            // Sorted + deduped.
            let mut sorted = s.productions.clone();
            sorted.sort();
            sorted.dedup();
            assert_eq!(sorted, s.productions);
        }
        // Core productions all fire and every statement module is
        // exercised. Measured over a longer session than the budget check
        // above: with 14 registered modules an 80-statement stream no
        // longer reliably picks every one of them (and the idx module's B2
        // families lengthened its draw sequence, so 400 is where every
        // module reliably fires for this seed now).
        // (600 since the 28th module, admin/Q5, joined the all-on mix.)
        // 600 -> 800 with the 32-module registry (exd, LD4): the sparse
        // types module needs the larger budget to fire at this seed.
        // 800 -> 1400 with the 40-module registry (pgram/opt2, SQLcov-B):
        // the two new battery modules emit chunky groups that dilute the
        // per-module draw further; explain needs the longer stream to
        // reliably fire at this seed.
        let long = run_session(&SessionConfig { budget: 1400, ..cfg(12) }, &cat);
        let all: Vec<String> = long.iter().flat_map(|s| s.productions.clone()).collect();
        for prefix in [
            "colref",
            "lit:",
            "case",
            "cmp:",
            "cast:",
            "select",
            "module:expr",
            "module:joins",
            "module:subq",
            "module:agg",
            "module:win",
            "module:dml",
            "module:merge",
            "module:txn",
            "module:ddl",
            "module:types",
            "module:explain",
            "module:util",
            "module:part",
            "module:objddl",
            "module:idx",
            "module:tsdl",
            "module:cursor",
            "module:views",
        ] {
            assert!(
                all.iter().any(|p| p.starts_with(prefix)),
                "production {} never fired in the session",
                prefix
            );
        }
        // Transaction brackets in the stream are balanced.
        let mut open = 0i32;
        for s in &stmts {
            if s.sql == "BEGIN;" {
                assert_eq!(open, 0, "nested BEGIN");
                open += 1;
            } else if s.sql == "COMMIT;" || s.sql == "ROLLBACK;" {
                assert_eq!(open, 1, "terminator outside bracket");
                open -= 1;
            }
        }
        assert_eq!(open, 0, "unclosed bracket at stream end");
    }

    #[test]
    fn dml_state_persists_across_groups() {
        // Insert-heavy stream: fresh pks must never repeat stream-wide
        // (the monotonic allocator survives the per-group Gen swap).
        let cat = FixtureCatalog.load_catalog().unwrap();
        let mut c = cfg(9);
        c.toggles = ToggleVector::parse(
            "expr=off,joins=off,subq=off,agg=off,win=off,dml=on,txn=off,\
             ddl=off,types=off,explain=off,util=off,part=off,\
             objddl=off,idx=off,par=off,tsdl=off,cursor=off,views=off,geo=off,\
             nodes=off,obs=off,objid=off,einterp=off,exd=off,spill=off,\
             earm=off,plansel=off,earm2=off,exr=off,numx=off,pubsub=off,pgram=off,opt2=off,cfgm=off",
        )
        .unwrap();
        c.weights = WeightTable::parse(
            "dml:insert=1,dml:update=0,dml:delete=0,dml:insert:single=1,\
             dml:insert:multirow=0,dml:insert:select=0,dml:pk:collide=0,\
             dml:onconflict:nothing=0,dml:onconflict:update=0",
        )
        .unwrap();
        let stmts = run_session(&c, &cat);
        let mut pks_by_table: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for s in &stmts {
            let Some(rest) = s.sql.strip_prefix("INSERT INTO ") else { continue };
            let table = rest.split(' ').next().unwrap().to_string();
            let vals = s.sql.split_once(" VALUES (").unwrap().1;
            let pk = vals.split(',').next().unwrap().to_string();
            pks_by_table.entry(table).or_default().push(pk);
        }
        assert!(!pks_by_table.is_empty());
        for (table, pks) in pks_by_table {
            let mut sorted = pks.clone();
            sorted.sort();
            sorted.dedup();
            assert_eq!(sorted.len(), pks.len(), "{table}: repeated fresh pk {pks:?}");
        }
    }

    /// Probe windows line up with the stream (window.from indexes the
    /// CREATE TABLE statement, window.until the DROP), and ddl-created
    /// tables really are targeted by OTHER modules later in the stream —
    /// the cross-module reuse the module exists for.
    #[test]
    fn ddl_windows_and_cross_module_reuse() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let mut c = cfg(41);
        // 3000 (was 1800, 900, 600): the round-3 drain-module merges
        // (plansel/earm2/exr/numx/pubsub — the 38-module registry) dilute
        // per-module traffic further; the cross-module-reuse property
        // needs the longer window under the new module mix.
        c.budget = 3000;
        let outp = run_session_probed(&c, &cat);
        assert!(!outp.ddl_windows.is_empty(), "no ddl table created in 600 stmts");
        for w in &outp.ddl_windows {
            assert_eq!(w.pk, "pk");
            let create = &outp.stmts[w.from as usize].sql;
            assert!(
                create.starts_with(&format!("CREATE TABLE {} ", w.table))
                    || create.starts_with(&format!("CREATE TEMPORARY TABLE {} ", w.table))
                    // The views module probe-registers its materialized
                    // views, whose CREATE leads their group.
                    || create.starts_with(&format!("CREATE MATERIALIZED VIEW {} ", w.table)),
                "window.from does not index the CREATE: {create}"
            );
            if let Some(u) = w.until {
                assert!(u > w.from);
                let drop = &outp.stmts[u as usize].sql;
                assert!(
                    (drop.starts_with("DROP TABLE ")
                        || drop.starts_with("DROP MATERIALIZED VIEW ")
                        // A views-module drop group leads with DROP VIEW;
                        // the base table it registers goes one statement
                        // later, so the window closes conservatively early.
                        || drop.starts_with("DROP VIEW "))
                        && (drop.contains(&w.table) || w.table.starts_with("fz_vb_")),
                    "window.until does not index the DROP: {drop}"
                );
            }
        }
        // At most one open window per table name; names never reused.
        let names: Vec<&String> = outp.ddl_windows.iter().map(|w| &w.table).collect();
        let mut dedup = names.clone();
        dedup.sort();
        dedup.dedup();
        assert_eq!(dedup.len(), names.len(), "table name reused across windows");
        // Cross-module reuse: a non-ddl group references a ddl table.
        let reused = outp.stmts.iter().any(|s| {
            !s.productions.iter().any(|p| p == "module:ddl")
                && s.sql.contains("fz_ddl_")
        });
        assert!(reused, "no non-ddl statement ever targeted a ddl-created table");
        // Specifically DML: fresh-pk discipline must extend to ddl tables.
        let dml_on_ddl = outp.stmts.iter().any(|s| {
            s.productions.iter().any(|p| p == "module:dml" || p == "module:txn")
                && (s.sql.starts_with("INSERT INTO fz_ddl_")
                    || s.sql.starts_with("UPDATE fz_ddl_")
                    || s.sql.starts_with("DELETE FROM fz_ddl_"))
        });
        assert!(dml_on_ddl, "DML never targeted a ddl-created table");
        // And no statement references a table after its DROP (ordering
        // hazard gate): for every closed window, later statements must not
        // mention the table name.
        for w in &outp.ddl_windows {
            if let Some(u) = w.until {
                for s in &outp.stmts[(u as usize + 1)..] {
                    let hay = &s.sql;
                    // A views-module drop group leads with DROP VIEW (which
                    // closes the window) and drops its registered base table
                    // one statement later; that trailing statement IS the
                    // drop, not a use-after-drop.
                    if *hay == format!("DROP TABLE {};", w.table) {
                        continue;
                    }
                    let gone = !hay.match_indices(w.table.as_str()).any(|(i, _)| {
                        hay[i + w.table.len()..]
                            .chars()
                            .next()
                            .is_none_or(|ch| !ch.is_alphanumeric() && ch != '_')
                    });
                    assert!(gone, "statement after DROP references {}: {hay}", w.table);
                }
            }
        }
    }

    /// Index-AM tables get probe windows like ddl tables, so the runner's
    /// pk-ordered state probes cover them (the only way an index-vs-heap
    /// state divergence from the churn paths would be caught): window.from
    /// indexes the CREATE, window.until the DROP, and nothing references a
    /// table after its DROP. The idx module owns its own statements (its
    /// column types are outside the generic type system, so it deliberately
    /// does NOT register into the shared catalog).
    #[test]
    fn idx_windows_and_bracket_discipline() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let mut c = cfg(37);
        // Budget retuned 1200 -> 2000 -> 2200 -> 6500 as the registry grew
        // (par/coll/mbconv/xnum, then the einterp/exd/earm drain groups
        // dilute per-module traffic); the >=50 idx-statement floor below
        // stays meaningful at this budget.
        // Budget retuned 1200 -> 2000 -> 2200 -> 3600 -> 5000 as the
        // registry grew (par/coll/mbconv/xnum, then the einterp/exd/earm
        // drain groups, then the LD10 pubsub module dilute per-module
        // traffic); the >=50 idx-statement floor below stays meaningful at
        // this budget. Re-bumped 5000 -> 5800 when the CFG-lane cfgm
        // module joined the registry.
        c.budget = 5800;
        let outp = run_session_probed(&c, &cat);
        let idx_windows: Vec<_> = outp
            .ddl_windows
            .iter()
            .filter(|w| {
                ["fz_iam_", "fz_btd_", "fz_adv_"].iter().any(|p| w.table.starts_with(p))
            })
            .collect();
        assert!(!idx_windows.is_empty(), "no index-AM table in the leg");
        for w in &idx_windows {
            assert_eq!(w.pk, "pk");
            let create = &outp.stmts[w.from as usize].sql;
            assert!(
                create.starts_with(&format!("CREATE TABLE {} (pk int4 PRIMARY KEY", w.table)),
                "window.from does not index the CREATE: {create}"
            );
            if let Some(u) = w.until {
                assert!(u > w.from);
                let drop = &outp.stmts[u as usize].sql;
                assert_eq!(drop, &format!("DROP TABLE {};", w.table));
                for s in &outp.stmts[(u as usize + 1)..] {
                    let hay = &s.sql;
                    let gone = !hay.match_indices(w.table.as_str()).any(|(i, _)| {
                        hay[i + w.table.len()..]
                            .chars()
                            .next()
                            .is_none_or(|ch| !ch.is_alphanumeric() && ch != '_')
                    });
                    assert!(gone, "statement after DROP references {}: {hay}", w.table);
                }
            }
        }
        // GUC-bracket balance across idx-produced statements: every
        // seqscan/bitmapscan SET off has its RESET later, and no bracket
        // stays open at stream end (both sides always see identical GUC
        // state). Scoped to idx groups because the util module twiddles the
        // same GUCs independently — a util SET that outlives an idx bracket
        // is harmless (it lands on both sides identically), it just makes a
        // stream-wide count meaningless.
        let idx_stmts: Vec<&Statement> = outp
            .stmts
            .iter()
            .filter(|s| s.productions.iter().any(|p| p == "module:idx"))
            .collect();
        for guc in ["enable_seqscan", "enable_bitmapscan"] {
            let mut open = 0i32;
            for s in &idx_stmts {
                if s.sql == format!("SET {guc} TO off;") {
                    assert_eq!(open, 0, "nested SET {guc}");
                    open += 1;
                } else if s.sql == format!("RESET {guc};") {
                    assert_eq!(open, 1, "RESET {guc} outside a bracket");
                    open -= 1;
                }
            }
            assert_eq!(open, 0, "unclosed {guc} bracket at stream end");
        }
        // Index-AM statements really do reach the stream in volume (the
        // module's whole point: data-driven AM internals need traffic).
        let n_idx = outp
            .stmts
            .iter()
            .filter(|s| {
                ["fz_iam_", "fz_btd_", "fz_adv_", "fz_gst_", "fz_gbuf_", "fz_gex_"]
                    .iter()
                    .any(|p| s.sql.contains(p))
            })
            .count();
        assert!(n_idx >= 50, "only {n_idx} index-AM statements in the leg");
    }

    /// Partitioned parents get probe windows like ddl tables (window.from
    /// indexes the CREATE, window.until the DROP), and the cross-module
    /// payoff is real: OTHER modules SELECT from parents (pruning paths)
    /// and DML them (tuple routing; UPDATEs on the partition key make
    /// cross-partition moves).
    #[test]
    fn part_windows_and_cross_module_reuse() {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let mut c = cfg(23);
        // Budget retuned 900 -> 1500 -> 2200 -> 6500 as the registry grew
        // (round-6 par/coll/mbconv/xnum, round-7 nodes/obs/ssi/admin,
        // round-3 line-drain plansel/earm2/exr/numx/pubsub dilute
        // per-module traffic).
        c.budget = 6500;
        let outp = run_session_probed(&c, &cat);
        let part_windows: Vec<_> = outp
            .ddl_windows
            .iter()
            .filter(|w| w.table.starts_with("fz_part_"))
            .collect();
        assert!(!part_windows.is_empty(), "no partitioned parent in 2200 stmts");
        for w in &part_windows {
            assert_eq!(w.pk, "pk");
            let create = &outp.stmts[w.from as usize].sql;
            assert!(
                create.starts_with(&format!("CREATE TABLE {} ", w.table))
                    && create.contains(" PARTITION BY "),
                "window.from does not index the parent CREATE: {create}"
            );
            if let Some(u) = w.until {
                let drop = &outp.stmts[u as usize].sql;
                assert!(
                    drop.starts_with("DROP TABLE ") && drop.contains(&w.table),
                    "window.until does not index the DROP: {drop}"
                );
            }
        }
        // Cross-module reuse: non-part groups read from and write to
        // partitioned parents.
        let is_part_group =
            |s: &Statement| s.productions.iter().any(|p| p == "module:part");
        let read = outp
            .stmts
            .iter()
            .any(|s| !is_part_group(s) && s.sql.contains("fz_part_"));
        assert!(read, "no non-part statement ever targeted a partitioned parent");
        let routed = outp.stmts.iter().any(|s| {
            !is_part_group(s)
                && (s.sql.starts_with("INSERT INTO fz_part_")
                    || s.sql.starts_with("UPDATE fz_part_")
                    || s.sql.starts_with("DELETE FROM fz_part_"))
        });
        assert!(routed, "DML never targeted a partitioned parent");
        // Key-moving UPDATEs exist in the stream (SET touches k_int or
        // k_text on a parent — the cross-partition move surface).
        let moved = outp.stmts.iter().any(|s| {
            s.sql.starts_with("UPDATE fz_part_")
                && (s.sql.contains("SET k_int = ")
                    || s.sql.contains("SET k_text = ")
                    || s.sql.contains(", k_int = ")
                    || s.sql.contains(", k_text = "))
        });
        assert!(moved, "no partition-key UPDATE in 6500 stmts");
        // No ON CONFLICT and no pk-collide on parents without a unique pk
        // constraint is enforced by dml tests + the part module's model
        // test; here just assert the stream stays in-window after drops.
        for w in &part_windows {
            if let Some(u) = w.until {
                for s in &outp.stmts[(u as usize + 1)..] {
                    let hay = &s.sql;
                    let gone = !hay.match_indices(w.table.as_str()).any(|(i, _)| {
                        hay[i + w.table.len()..]
                            .chars()
                            .next()
                            .is_none_or(|ch| !ch.is_alphanumeric() && ch != '_')
                    });
                    assert!(gone, "statement after DROP references {}: {hay}", w.table);
                }
            }
        }
    }

    #[test]
    fn jsonl_escapes_quotes() {
        let s = Statement {
            stmt_index: 0,
            sql: "SELECT '\"';".to_string(),
            productions: vec!["lit:text".to_string()],
            soft_float_cols: Vec::new(),
        };
        let rec = jsonl_record(9, &s);
        assert!(rec.contains("\\\""));
        assert!(rec.starts_with("{\"seed\":9,"));
    }
}

