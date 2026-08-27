//! Partitioning statement module (P1): CREATE TABLE ... PARTITION BY
//! {RANGE|LIST|HASH}, CREATE TABLE ... PARTITION OF, ATTACH/DETACH
//! PARTITION (plain and CONCURRENTLY), targeted partition-pruning SELECTs,
//! and direct partition scans. The structural target is the uncovered
//! partitioning machinery from gap-report-003: tuple routing
//! (ExecInitPartitionInfo), partition pruning (match_clause_to_partition_key,
//! get_matching_range_bounds, gen_prune_steps_from_opexps), bound handling
//! (get_qual_for_range, merge_list_bounds), and DETACH
//! (DetachPartitionFinalize).
//!
//! `PartState` (session-persistent, swapped in and out of `Gen` like
//! `DmlState`/`DdlState`) tracks every partitioned parent and its children,
//! so statements are valid by construction (zero-42xxx discipline). Parents
//! are registered into the effective catalog after every part group, so the
//! OTHER modules supply the deep coverage: SELECT/join/agg/win statements
//! over parents engage partition pruning and Append plans, INSERTs engage
//! tuple routing, UPDATEs on the partition key make cross-partition moves
//! (the classic bug surface), DELETEs prune, and probes run pk-ordered
//! SELECT * over the whole partition tree. Children are deliberately NOT
//! registered (their lifecycle churns under DETACH/DROP); direct partition
//! scans come from this module's own child_select action.
//!
//! Parent flavors (all verified to behave identically on pgrust and C
//! Postgres 18 before this module was written — 2026-08-11 hand probe):
//!   - `range_pk`   PARTITION BY RANGE (pk), real PRIMARY KEY (pk), bounds
//!     cover MINVALUE..MAXVALUE completely (no DEFAULT needed: routing can
//!     never miss); full DML including ON CONFLICT (pk).
//!   - `range_kint` PARTITION BY RANGE (k_int), finite bounds + a DEFAULT
//!     partition ALWAYS (k_int is nullable and generated values are
//!     arbitrary ints: NULLs and out-of-range rows route to DEFAULT).
//!   - `range_multi` PARTITION BY RANGE (k_int, pk), multi-column key,
//!     DEFAULT always. Upper bounds use `TO (b, MINVALUE)` — `TO (b,
//!     MAXVALUE)` would overlap the next partition's `FROM (b, MINVALUE)`
//!     (both engines reject it identically; found by the hand probe).
//!   - `list_text`  PARTITION BY LIST (k_text) over a small vocabulary
//!     (NULL sometimes listed), DEFAULT always.
//!   - `list_expr`  PARTITION BY LIST ((k_int % 4)) — expression partition
//!     key; values -3..3 split across children, DEFAULT always (catches
//!     NULL and unlisted residues).
//!   - `hash_pk`    PARTITION BY HASH (pk), real PRIMARY KEY (pk), all
//!     remainders of one modulus (routing can never miss; DEFAULT is not
//!     allowed for hash anyway).
//!
//! Validity disciplines:
//!   - parents without a unique constraint on pk (every kind whose
//!     partition key is not exactly pk) set `Table::pk_unique = false`: the
//!     DML module then never emits ON CONFLICT (pk) (42P10 without a
//!     matching unique index) and never picks colliding pks (a duplicate
//!     would silently succeed and break the probes' pk total order);
//!   - DETACH/ATTACH is a same-group cycle with identical bounds: rows in
//!     the detached child were routed there, so no DEFAULT-partition row
//!     can conflict on re-ATTACH, and nothing runs in between;
//!   - DETACH CONCURRENTLY only on parents without a DEFAULT partition
//!     (Postgres forbids the combination);
//!   - detach_drop (DETACH + DROP the child) only on parents WITH a
//!     DEFAULT: later rows for the dropped range/list route to DEFAULT
//!     instead of erroring "no partition found";
//!   - a dropped parent takes its children with it; any ddl-module index
//!     on the parent is marked dead in DdlState (the ddl module would
//!     otherwise DROP/REINDEX a vanished index);
//!   - fixture tables and ddl tables are never touched by this module.

use crate::catalog::{Catalog, Column, Pk, SqlType, Table, ALL_TYPES};
use crate::ddl::{DdlEvent, DdlEventKind};
use crate::stmt::{Gen, StmtKind};

/// Live-parent population cap (each parent is 3-6 relations).
const MAX_LIVE_PARENTS: usize = 4;

/// Split-point ladder for range bounds and pruning literals.
const SPLITS: &[i64] = &[-1000, -100, -10, 0, 5, 10, 25, 50, 100, 250, 500, 1000, 100000];

/// LIST (k_text) vocabulary (matches the fixture seed flavor: short,
/// quote-free, includes the empty string).
const TEXT_VOCAB: &[&str] = &["a", "b", "c", "d", "e", "x", "y", "z", ""];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartKind {
    RangePk,
    RangeKint,
    RangeMulti,
    ListText,
    ListExpr,
    HashPk,
}

impl PartKind {
    fn prod(self) -> &'static str {
        match self {
            PartKind::RangePk => "part:kind:range_pk",
            PartKind::RangeKint => "part:kind:range_kint",
            PartKind::RangeMulti => "part:kind:range_multi",
            PartKind::ListText => "part:kind:list_text",
            PartKind::ListExpr => "part:kind:list_expr",
            PartKind::HashPk => "part:kind:hash_pk",
        }
    }

    /// Partition key includes exactly the pk column (a PRIMARY KEY (pk)
    /// exists, so ON CONFLICT (pk) and deliberate pk collisions are legal).
    fn pk_keyed(self) -> bool {
        matches!(self, PartKind::RangePk | PartKind::HashPk)
    }

    /// Parents of this kind always carry a DEFAULT partition.
    fn has_default(self) -> bool {
        matches!(
            self,
            PartKind::RangeKint | PartKind::RangeMulti | PartKind::ListText | PartKind::ListExpr
        )
    }
}

#[derive(Clone, Debug)]
pub struct PartChild {
    pub name: String,
    /// Bound clause as attached: `FOR VALUES ...`, or `DEFAULT`.
    pub bound: String,
    pub is_default: bool,
    pub live: bool,
}

#[derive(Clone, Debug)]
pub struct PartParent {
    pub table: Table,
    pub kind: PartKind,
    pub children: Vec<PartChild>,
    pub live: bool,
    /// Int split points used in the bounds (pruning-literal pool).
    splits: Vec<i64>,
    /// Text list values used in the bounds (pruning-literal pool).
    texts: Vec<String>,
}

/// Session-persistent partitioning catalog model.
#[derive(Clone, Debug, Default)]
pub struct PartState {
    pub parents: Vec<PartParent>,
    next_parent: u32,
    next_col: u32,
    events: Vec<DdlEvent>,
}

impl PartState {
    pub fn new() -> PartState {
        PartState::default()
    }

    /// Base catalog + live partitioned parents, in creation order. The
    /// session loop feeds this to every later group — the cross-module
    /// reuse (pruned SELECTs, routed INSERTs, key-moving UPDATEs) rides on
    /// it.
    pub fn extend_catalog(&self, base: &Catalog) -> Catalog {
        let mut cat = base.clone();
        for p in &self.parents {
            if p.live {
                cat.tables.push(p.table.clone());
            }
        }
        cat
    }

    /// Drain pending create/drop events (session loop resolves indices into
    /// probe windows, exactly like DdlState's).
    pub fn take_events(&mut self) -> Vec<DdlEvent> {
        std::mem::take(&mut self.events)
    }

    fn live_parents(&self) -> Vec<usize> {
        self.parents
            .iter()
            .enumerate()
            .filter(|(_, p)| p.live)
            .map(|(i, _)| i)
            .collect()
    }
}

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_part_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("part");
    let action = g.weights.pick(
        g.rng,
        &[
            "part:create",
            "part:drop",
            "part:detach_attach",
            "part:detach_drop",
            "part:prune",
            "part:child_select",
        ],
    );
    match action {
        "part:create" => gen_create_parent(g),
        "part:drop" => gen_drop_parent(g),
        "part:detach_attach" => gen_detach_attach(g),
        "part:detach_drop" => gen_detach_drop(g),
        "part:prune" => gen_prune_select(g),
        _ => gen_child_select(g),
    }
}

// ------------------------------------------------------------- create ----

/// Ascending distinct split points: n draws from the ladder, sorted,
/// deduplicated (fewer splits than asked just means wider partitions).
fn draw_splits(g: &mut Gen, n: usize) -> Vec<i64> {
    let mut s: Vec<i64> = (0..n).map(|_| SPLITS[g.rng.below_usize(SPLITS.len())]).collect();
    s.sort_unstable();
    s.dedup();
    s
}

fn gen_create_parent(g: &mut Gen) -> Vec<StmtKind> {
    if g.part.live_parents().len() >= MAX_LIVE_PARENTS {
        g.fire("part:cap:parents");
        return gen_drop_parent(g);
    }
    g.fire("part:create");
    let kind = match g.weights.pick(
        g.rng,
        &[
            "part:kind:range_pk",
            "part:kind:range_kint",
            "part:kind:range_multi",
            "part:kind:list_text",
            "part:kind:list_expr",
            "part:kind:hash_pk",
        ],
    ) {
        "part:kind:range_pk" => PartKind::RangePk,
        "part:kind:range_kint" => PartKind::RangeKint,
        "part:kind:range_multi" => PartKind::RangeMulti,
        "part:kind:list_text" => PartKind::ListText,
        "part:kind:list_expr" => PartKind::ListExpr,
        _ => PartKind::HashPk,
    };
    g.fire(kind.prod());
    let nparts = match g.weights.pick(g.rng, &["part:parts:2", "part:parts:3", "part:parts:4"]) {
        "part:parts:2" => 2,
        "part:parts:3" => 3,
        _ => 4,
    };

    let name = format!("fz_part_{}", g.part.next_parent);
    g.part.next_parent += 1;

    // Column set: the protected trio plus 1-3 nullable palette columns.
    // pk is a real PRIMARY KEY only when it is exactly the partition key
    // (a partitioned unique constraint must include every key column);
    // it is NOT NULL and unique-by-construction otherwise (fresh-only
    // allocation — see pk_unique below).
    let mut columns = vec![
        Column { name: "pk".to_string(), ty: SqlType::Int4, nullable: false, ddl_type: None },
        Column { name: "k_int".to_string(), ty: SqlType::Int4, nullable: true, ddl_type: None },
        Column { name: "k_text".to_string(), ty: SqlType::Text, nullable: true, ddl_type: None },
    ];
    let mut defs = vec![
        if kind.pk_keyed() { "pk int4 PRIMARY KEY" } else { "pk int4 NOT NULL" }.to_string(),
        "k_int int4".to_string(),
        "k_text text".to_string(),
    ];
    let ncols = 1 + g.rng.below_usize(3);
    for _ in 0..ncols {
        let cname = format!("pc{}", g.part.next_col);
        g.part.next_col += 1;
        let ty = ALL_TYPES[g.rng.below_usize(ALL_TYPES.len())];
        defs.push(format!("{} {}", cname, ty.name()));
        columns.push(Column { name: cname, ty, nullable: true, ddl_type: None });
    }

    let key_sql = match kind {
        PartKind::RangePk => "RANGE (pk)",
        PartKind::RangeKint => "RANGE (k_int)",
        PartKind::RangeMulti => "RANGE (k_int, pk)",
        PartKind::ListText => "LIST (k_text)",
        PartKind::ListExpr => "LIST ((k_int % 4))",
        PartKind::HashPk => "HASH (pk)",
    };
    let mut stmts = vec![StmtKind::Raw(format!(
        "CREATE TABLE {} ({}) PARTITION BY {};",
        name,
        defs.join(", "),
        key_sql
    ))];

    // Children bounds per kind.
    let mut children: Vec<PartChild> = Vec::new();
    let mut splits: Vec<i64> = Vec::new();
    let mut texts: Vec<String> = Vec::new();
    let bound_of = |lo: &str, hi: &str| format!("FOR VALUES FROM ({}) TO ({})", lo, hi);
    match kind {
        PartKind::RangePk => {
            // Full coverage: MINVALUE .. s1 .. s2 .. MAXVALUE.
            splits = draw_splits(g, nparts - 1);
            let mut edges = vec!["MINVALUE".to_string()];
            edges.extend(splits.iter().map(|s| s.to_string()));
            edges.push("MAXVALUE".to_string());
            for w in edges.windows(2) {
                children.push(PartChild {
                    name: String::new(),
                    bound: bound_of(&w[0], &w[1]),
                    is_default: false,
                    live: true,
                });
            }
        }
        PartKind::RangeKint => {
            // Finite bounds (occasionally an unbounded low end); DEFAULT
            // catches NULLs and both tails.
            splits = draw_splits(g, nparts);
            let mut edges: Vec<String> = splits.iter().map(|s| s.to_string()).collect();
            if g.rng.chance(1, 2) && !edges.is_empty() {
                edges.insert(0, "MINVALUE".to_string());
            }
            for w in edges.windows(2) {
                children.push(PartChild {
                    name: String::new(),
                    bound: bound_of(&w[0], &w[1]),
                    is_default: false,
                    live: true,
                });
            }
        }
        PartKind::RangeMulti => {
            // (k_int, pk) key: upper bounds end at (b, MINVALUE) so
            // adjacent partitions cannot overlap (hand-probe finding).
            splits = draw_splits(g, nparts);
            let mut edges: Vec<String> =
                splits.iter().map(|s| format!("{}, MINVALUE", s)).collect();
            if g.rng.chance(1, 2) && !edges.is_empty() {
                edges.insert(0, "MINVALUE, MINVALUE".to_string());
            }
            for w in edges.windows(2) {
                children.push(PartChild {
                    name: String::new(),
                    bound: bound_of(&w[0], &w[1]),
                    is_default: false,
                    live: true,
                });
            }
        }
        PartKind::ListText => {
            // Disjoint vocabulary chunks; NULL listed in one chunk 1/3 of
            // the time (else NULL routes to DEFAULT).
            let mut vocab: Vec<String> = TEXT_VOCAB.iter().map(|s| s.to_string()).collect();
            // Seeded shuffle (Fisher-Yates over the session PRNG).
            for i in (1..vocab.len()).rev() {
                let j = g.rng.below_usize(i + 1);
                vocab.swap(i, j);
            }
            let with_null = g.rng.chance(1, 3);
            for ci in 0..nparts {
                let take = 1 + g.rng.below_usize(3);
                let mut vals: Vec<String> = Vec::new();
                for _ in 0..take {
                    if let Some(v) = vocab.pop() {
                        texts.push(v.clone());
                        vals.push(format!("'{}'", v));
                    }
                }
                if vals.is_empty() {
                    continue; // vocabulary exhausted; DEFAULT covers the rest
                }
                if with_null && ci == 0 {
                    vals.push("NULL".to_string());
                }
                children.push(PartChild {
                    name: String::new(),
                    bound: format!("FOR VALUES IN ({})", vals.join(", ")),
                    is_default: false,
                    live: true,
                });
            }
        }
        PartKind::ListExpr => {
            // (k_int % 4) residues: -3..3 split disjointly; DEFAULT catches
            // NULL and any unlisted residue.
            let mut residues: Vec<i64> = vec![-3, -2, -1, 0, 1, 2, 3];
            for i in (1..residues.len()).rev() {
                let j = g.rng.below_usize(i + 1);
                residues.swap(i, j);
            }
            for _ in 0..nparts {
                let take = 1 + g.rng.below_usize(3);
                let mut vals: Vec<String> = Vec::new();
                for _ in 0..take {
                    if let Some(v) = residues.pop() {
                        splits.push(v);
                        vals.push(v.to_string());
                    }
                }
                if vals.is_empty() {
                    continue;
                }
                children.push(PartChild {
                    name: String::new(),
                    bound: format!("FOR VALUES IN ({})", vals.join(", ")),
                    is_default: false,
                    live: true,
                });
            }
        }
        PartKind::HashPk => {
            for r in 0..nparts {
                children.push(PartChild {
                    name: String::new(),
                    bound: format!("FOR VALUES WITH (MODULUS {}, REMAINDER {})", nparts, r),
                    is_default: false,
                    live: true,
                });
            }
        }
    }
    if kind.has_default() {
        g.fire("part:default");
        children.push(PartChild {
            name: String::new(),
            bound: "DEFAULT".to_string(),
            is_default: true,
            live: true,
        });
    }
    for (i, c) in children.iter_mut().enumerate() {
        c.name = if c.is_default {
            format!("{}_def", name)
        } else {
            format!("{}_c{}", name, i)
        };
        // autovacuum_enabled = off (round-18a blanket rule, exd RB-15
        // lineage): the parent is a compared EXPLAIN (COSTS OFF) prune
        // target (part:prune:explain) and long-lived children accumulate
        // dml.rs churn with no module-owned ANALYZE, so an autoanalyze
        // landing on exactly ONE engine can flip the pruned plan. The
        // partitioned parent has no storage (reloption rejected there);
        // the children carry the pin.
        stmts.push(StmtKind::Raw(format!(
            "CREATE TABLE {} PARTITION OF {} {} WITH (autovacuum_enabled = off);",
            c.name, name, c.bound
        )));
    }

    let pk_keyed = kind.pk_keyed();
    let table = Table {
        name: name.clone(),
        columns,
        at_most_one_row: false,
        pk: Some(Pk { column: "pk".to_string(), seeded_max: 0 }),
        // No unique-key hint without a real constraint: order-sensitive
        // window functions then take the fallback path (fz_scalar-style).
        unique_key: pk_keyed.then(|| "pk".to_string()),
        pk_unique: pk_keyed,
    };
    g.part.parents.push(PartParent {
        table,
        kind,
        children,
        live: true,
        splits,
        texts,
    });
    g.part.events.push(DdlEvent {
        table: name,
        pk: "pk".to_string(),
        kind: DdlEventKind::Created,
    });
    stmts
}

// --------------------------------------------------------------- drop ----

fn gen_drop_parent(g: &mut Gen) -> Vec<StmtKind> {
    let live = g.part.live_parents();
    if live.is_empty() {
        // Nothing to drop: create instead (never recurses back — an empty
        // population is below the cap by definition).
        g.fire("part:fallback:create");
        return gen_create_parent(g);
    }
    g.fire("part:drop");
    let pi = live[g.rng.below_usize(live.len())];
    let name = g.part.parents[pi].table.name.clone();
    g.part.parents[pi].live = false;
    for c in &mut g.part.parents[pi].children {
        c.live = false;
    }
    // Any ddl-module index on the parent dies with it.
    for x in &mut g.ddl.indexes {
        if x.table == name {
            x.live = false;
        }
    }
    g.part.events.push(DdlEvent {
        table: name.clone(),
        pk: "pk".to_string(),
        kind: DdlEventKind::Dropped,
    });
    vec![StmtKind::Raw(format!("DROP TABLE {};", name))]
}

// ----------------------------------------------------- detach / attach ----

/// Live parents having at least `min_nondefault` live non-default children.
fn parents_with_children(g: &Gen, min_nondefault: usize) -> Vec<usize> {
    g.part
        .parents
        .iter()
        .enumerate()
        .filter(|(_, p)| {
            p.live
                && p.children.iter().filter(|c| c.live && !c.is_default).count()
                    >= min_nondefault
        })
        .map(|(i, _)| i)
        .collect()
}

/// DETACH + re-ATTACH with identical bounds, one group: rows in the child
/// were routed there while attached, so nothing can conflict on re-ATTACH
/// and no statement runs in between. Covers the DETACH machinery
/// (DetachPartitionFinalize) and ATTACH validation both.
fn gen_detach_attach(g: &mut Gen) -> Vec<StmtKind> {
    let cands = parents_with_children(g, 1);
    if cands.is_empty() {
        g.fire("part:fallback:create");
        return gen_create_parent(g);
    }
    g.fire("part:detach_attach");
    let pi = cands[g.rng.below_usize(cands.len())];
    let parent = g.part.parents[pi].table.name.clone();
    let has_default = g.part.parents[pi].kind.has_default();
    let kids: Vec<usize> = g.part.parents[pi]
        .children
        .iter()
        .enumerate()
        .filter(|(_, c)| c.live && !c.is_default)
        .map(|(i, _)| i)
        .collect();
    let ci = kids[g.rng.below_usize(kids.len())];
    let child = g.part.parents[pi].children[ci].name.clone();
    let bound = g.part.parents[pi].children[ci].bound.clone();
    // CONCURRENTLY is forbidden while a DEFAULT partition exists.
    let concurrent = !has_default
        && g.weights.pick(g.rng, &["part:detach:concurrent", "part:detach:plain"])
            == "part:detach:concurrent";
    if concurrent {
        g.fire("part:detach:concurrent");
    } else {
        g.fire("part:detach:plain");
    }
    vec![
        StmtKind::Raw(format!(
            "ALTER TABLE {} DETACH PARTITION {}{};",
            parent,
            child,
            if concurrent { " CONCURRENTLY" } else { "" }
        )),
        StmtKind::Raw(format!(
            "ALTER TABLE {} ATTACH PARTITION {} {};",
            parent, child, bound
        )),
    ]
}

/// DETACH + DROP the child. Only on parents WITH a DEFAULT partition (later
/// rows for the dropped range/list route to DEFAULT instead of erroring)
/// and only while >= 2 non-default children remain.
fn gen_detach_drop(g: &mut Gen) -> Vec<StmtKind> {
    let cands: Vec<usize> = parents_with_children(g, 2)
        .into_iter()
        .filter(|&pi| g.part.parents[pi].kind.has_default())
        .collect();
    if cands.is_empty() {
        return gen_detach_attach(g);
    }
    g.fire("part:detach_drop");
    let pi = cands[g.rng.below_usize(cands.len())];
    let parent = g.part.parents[pi].table.name.clone();
    let kids: Vec<usize> = g.part.parents[pi]
        .children
        .iter()
        .enumerate()
        .filter(|(_, c)| c.live && !c.is_default)
        .map(|(i, _)| i)
        .collect();
    let ci = kids[g.rng.below_usize(kids.len())];
    g.part.parents[pi].children[ci].live = false;
    let child = g.part.parents[pi].children[ci].name.clone();
    vec![
        StmtKind::Raw(format!("ALTER TABLE {} DETACH PARTITION {};", parent, child)),
        StmtKind::Raw(format!("DROP TABLE {};", child)),
    ]
}

// -------------------------------------------------------------- prune ----

/// Split-point-adjacent int literal: partition boundaries are where the
/// pruning logic has its edge cases.
fn near_split(g: &mut Gen, splits: &[i64]) -> i64 {
    let base = if splits.is_empty() {
        SPLITS[g.rng.below_usize(SPLITS.len())]
    } else {
        splits[g.rng.below_usize(splits.len())]
    };
    base + [-1, 0, 0, 1][g.rng.below_usize(4)]
}

/// Targeted pruning predicate over the parent's partition key.
fn prune_pred(g: &mut Gen, pi: usize) -> String {
    let kind = g.part.parents[pi].kind;
    let splits = g.part.parents[pi].splits.clone();
    let col = match kind {
        PartKind::RangePk | PartKind::HashPk => "pk",
        PartKind::ListText => "k_text",
        _ => "k_int",
    };
    if kind == PartKind::ListText {
        let texts = g.part.parents[pi].texts.clone();
        let lit = |g: &mut Gen| {
            if !texts.is_empty() && g.rng.chance(3, 4) {
                format!("'{}'", texts[g.rng.below_usize(texts.len())])
            } else {
                format!("'{}'", TEXT_VOCAB[g.rng.below_usize(TEXT_VOCAB.len())])
            }
        };
        return match g.weights.pick(g.rng, &["part:prune:eq", "part:prune:in", "part:prune:null"])
        {
            "part:prune:in" => {
                g.fire("part:prune:in");
                let a = lit(g);
                let b = lit(g);
                format!("{} IN ({}, {})", col, a, b)
            }
            "part:prune:null" => {
                g.fire("part:prune:null");
                format!("{} IS NULL", col)
            }
            _ => {
                g.fire("part:prune:eq");
                format!("{} = {}", col, lit(g))
            }
        };
    }
    if kind == PartKind::ListExpr && g.rng.chance(1, 2) {
        // Predicate over the partition expression itself: the strongest
        // pruning engagement for expression keys.
        g.fire("part:prune:expr");
        return format!("(k_int % 4) = {}", (g.rng.below(7) as i64) - 3);
    }
    match g.weights.pick(
        g.rng,
        &["part:prune:eq", "part:prune:range", "part:prune:in"],
    ) {
        "part:prune:range" => {
            g.fire("part:prune:range");
            let a = near_split(g, &splits);
            let b = near_split(g, &splits);
            let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
            match g.rng.below(3) {
                0 => format!("{} BETWEEN {} AND {}", col, lo, hi),
                1 => format!("{} >= {} AND {} < {}", col, lo, col, hi),
                _ => format!("{} < {}", col, hi),
            }
        }
        "part:prune:in" => {
            g.fire("part:prune:in");
            let a = near_split(g, &splits);
            let b = near_split(g, &splits);
            let c = near_split(g, &splits);
            format!("{} IN ({}, {}, {})", col, a, b, c)
        }
        _ => {
            g.fire("part:prune:eq");
            format!("{} = {}", col, near_split(g, &splits))
        }
    }
}

/// Pruning-shaped statement over a random parent: plain rowset, count, or
/// EXPLAIN (COSTS OFF) — the pruning decision itself is only visible in the
/// plan, and plans must match under COSTS OFF (E1 comparison policy).
fn gen_prune_select(g: &mut Gen) -> Vec<StmtKind> {
    let live = g.part.live_parents();
    if live.is_empty() {
        g.fire("part:fallback:create");
        return gen_create_parent(g);
    }
    g.fire("part:prune");
    let pi = live[g.rng.below_usize(live.len())];
    let parent = g.part.parents[pi].table.name.clone();
    let pred = prune_pred(g, pi);
    let shape = g.weights.pick(
        g.rng,
        &["part:prune:select", "part:prune:count", "part:prune:explain"],
    );
    g.fire(shape);
    vec![StmtKind::Raw(match shape {
        "part:prune:count" => format!("SELECT count(*) FROM {} WHERE {};", parent, pred),
        "part:prune:explain" => format!(
            "EXPLAIN (COSTS OFF) SELECT count(*) FROM {} WHERE {};",
            parent, pred
        ),
        // pk is unique on every parent (constraint or fresh-only
        // allocation), so ORDER BY pk is a total order: strict compare.
        _ => format!("SELECT * FROM {} WHERE {} ORDER BY pk;", parent, pred),
    })]
}

/// Direct partition scan (children are not in the shared catalog; this is
/// the only statement surface that reads a partition as a plain table).
fn gen_child_select(g: &mut Gen) -> Vec<StmtKind> {
    let cands = parents_with_children(g, 1);
    if cands.is_empty() {
        g.fire("part:fallback:create");
        return gen_create_parent(g);
    }
    g.fire("part:child_select");
    let pi = cands[g.rng.below_usize(cands.len())];
    let kids: Vec<usize> = g.part.parents[pi]
        .children
        .iter()
        .enumerate()
        .filter(|(_, c)| c.live)
        .map(|(i, _)| i)
        .collect();
    let ci = kids[g.rng.below_usize(kids.len())];
    let child = g.part.parents[pi].children[ci].name.clone();
    vec![StmtKind::Raw(if g.rng.chance(1, 2) {
        format!("SELECT count(*) FROM {};", child)
    } else {
        format!("SELECT * FROM {} ORDER BY pk;", child)
    })]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::ddl::DdlState;
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    /// Session-shaped harness: persistent PartState (and DdlState) across
    /// groups, effective catalog rebuilt after every group.
    fn gen_many(seed: u64, n: usize, spec: &str) -> (Vec<String>, Vec<String>, PartState) {
        let base = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::parse(spec).unwrap();
        let mut rng = Rng::new(seed);
        let mut part = PartState::new();
        let mut ddl = DdlState::new();
        let mut sqls = Vec::new();
        let mut prods = Vec::new();
        for _ in 0..n {
            let eff = part.extend_catalog(&ddl.extend_catalog(&base));
            let mut p = Vec::new();
            let mut g = Gen::new(&mut rng, &eff, &w, &mut p, 3);
            std::mem::swap(&mut g.part, &mut part);
            std::mem::swap(&mut g.ddl, &mut ddl);
            let kinds = gen_part_module(&mut g);
            std::mem::swap(&mut g.part, &mut part);
            std::mem::swap(&mut g.ddl, &mut ddl);
            assert!(!kinds.is_empty());
            for k in kinds {
                sqls.push(k.to_sql());
            }
            prods.extend(p);
        }
        (sqls, prods, part)
    }

    #[test]
    fn part_is_deterministic() {
        let (a, _, _) = gen_many(31, 300, "");
        let (b, _, _) = gen_many(31, 300, "");
        assert_eq!(a, b);
        let (c, _, _) = gen_many(32, 300, "");
        assert_ne!(a, c);
    }

    /// Round-18a blanket rule (exd RB-15 lineage): every storage-bearing
    /// part CREATE TABLE (i.e. every partition child) pins
    /// autovacuum_enabled = off. The parent is a compared EXPLAIN
    /// (COSTS OFF) prune target and long-lived children take dml.rs churn
    /// with no module-owned ANALYZE. Partitioned parents are exempt (no
    /// storage; the reloption is rejected there).
    #[test]
    fn creates_pin_autovacuum_off() {
        let (sqls, _, _) = gen_many(0x18A, 600, "");
        let mut seen = 0;
        for sql in sqls {
            if !sql.starts_with("CREATE TABLE ") || sql.contains(" PARTITION BY ") {
                continue;
            }
            assert!(
                sql.contains("autovacuum_enabled = off"),
                "part fixture does not pin autovacuum off: `{sql}`"
            );
            seen += 1;
        }
        assert!(seen > 0, "no partition child generated in 600 groups");
    }

    #[test]
    fn part_variety() {
        let (sqls, prods, _) = gen_many(0xB417, 1200, "");
        let all = sqls.join("\n");
        for frag in [
            "PARTITION BY RANGE (pk)",
            "PARTITION BY RANGE (k_int)",
            "PARTITION BY RANGE (k_int, pk)",
            "PARTITION BY LIST (k_text)",
            "PARTITION BY LIST ((k_int % 4))",
            "PARTITION BY HASH (pk)",
            "PARTITION OF ",
            " DEFAULT WITH (autovacuum_enabled = off);",
            "FOR VALUES FROM (MINVALUE)",
            "TO (MAXVALUE)",
            "FOR VALUES IN (",
            "MODULUS ",
            "REMAINDER ",
            "DETACH PARTITION ",
            " CONCURRENTLY;",
            "ATTACH PARTITION ",
            "DROP TABLE fz_part_",
            "EXPLAIN (COSTS OFF) SELECT count(*) FROM fz_part_",
            " BETWEEN ",
            " IN (",
            " IS NULL",
            "(k_int % 4) = ",
            "ORDER BY pk;",
        ] {
            assert!(all.contains(frag), "part flavor {frag:?} never generated");
        }
        for p in [
            "part:create",
            "part:kind:range_pk",
            "part:kind:range_kint",
            "part:kind:range_multi",
            "part:kind:list_text",
            "part:kind:list_expr",
            "part:kind:hash_pk",
            "part:default",
            "part:drop",
            "part:detach_attach",
            "part:detach:plain",
            "part:detach:concurrent",
            "part:detach_drop",
            "part:prune",
            "part:prune:eq",
            "part:prune:range",
            "part:prune:in",
            "part:prune:explain",
            "part:child_select",
        ] {
            assert!(prods.iter().any(|q| q == p), "production {p} never fired");
        }
        for sql in &sqls {
            assert!(!sql.contains('\n') && sql.ends_with(';'), "{sql}");
            assert_eq!(sql.matches('(').count(), sql.matches(')').count(), "{sql}");
        }
    }

    /// Zero-42xxx discipline, statically: replay the stream against a
    /// text-level model — no statement references a dead or never-created
    /// relation, re-ATTACH bounds match the original bounds exactly,
    /// CONCURRENTLY never happens on a parent with a DEFAULT partition,
    /// range_pk/hash parents never lose a partition (no DEFAULT to absorb
    /// the hole), and every DETACH has its partner in the same group.
    #[test]
    fn references_are_live_and_bounds_are_stable() {
        let (sqls, _, _) = gen_many(0xF17E, 2500, "");
        use std::collections::HashMap;
        // relation -> live; child -> (parent, bound); parent -> has_default
        let mut live: HashMap<String, bool> = HashMap::new();
        let mut child_bound: HashMap<String, (String, String)> = HashMap::new();
        let mut has_default: HashMap<String, bool> = HashMap::new();
        let mut attached: HashMap<String, bool> = HashMap::new();
        for sql in &sqls {
            if let Some(rest) = sql.strip_prefix("CREATE TABLE ") {
                let name = rest.split(' ').next().unwrap().to_string();
                assert!(!live.contains_key(&name), "relation name reused: {sql}");
                live.insert(name.clone(), true);
                if let Some((child_part, tail)) = rest.split_once(" PARTITION OF ") {
                    let child = child_part.trim().to_string();
                    let parent = tail.split(' ').next().unwrap().trim_end_matches(';');
                    assert_eq!(live.get(parent), Some(&true), "partition of dead parent: {sql}");
                    let bound = tail
                        .split_once(' ')
                        .map(|(_, b)| {
                            // CREATE-time children carry the round-18a
                            // autovacuum pin; ATTACH has no WITH clause,
                            // so strip it for bound comparison.
                            b.trim_end_matches(';')
                                .trim_end_matches(" WITH (autovacuum_enabled = off)")
                                .to_string()
                        })
                        .unwrap();
                    if bound == "DEFAULT" {
                        has_default.insert(parent.to_string(), true);
                    }
                    child_bound.insert(child.clone(), (parent.to_string(), bound));
                    attached.insert(child, true);
                }
            } else if let Some(rest) = sql.strip_prefix("DROP TABLE ") {
                let name = rest.trim_end_matches(';').to_string();
                assert_eq!(live.get(&name), Some(&true), "drop of dead relation: {sql}");
                live.insert(name.clone(), false);
                // Dropping a parent takes its attached children with it.
                let victims: Vec<String> = child_bound
                    .iter()
                    .filter(|(c, (p, _))| p == &name && attached.get(*c) == Some(&true))
                    .map(|(c, _)| c.clone())
                    .collect();
                for c in victims {
                    live.insert(c, false);
                }
                // A dropped child must have been detached first.
                if let Some((parent, _)) = child_bound.get(&name) {
                    assert_eq!(
                        attached.get(&name),
                        Some(&false),
                        "child dropped while attached: {sql}"
                    );
                    assert_eq!(
                        has_default.get(parent),
                        Some(&true),
                        "detach_drop on a parent without DEFAULT: {sql}"
                    );
                }
            } else if sql.contains(" DETACH PARTITION ") {
                let parent = sql
                    .strip_prefix("ALTER TABLE ")
                    .unwrap()
                    .split(' ')
                    .next()
                    .unwrap()
                    .to_string();
                let child = sql
                    .split(" DETACH PARTITION ")
                    .nth(1)
                    .unwrap()
                    .trim_end_matches(';')
                    .trim_end_matches(" CONCURRENTLY")
                    .to_string();
                assert_eq!(live.get(&parent), Some(&true), "{sql}");
                assert_eq!(attached.get(&child), Some(&true), "double detach: {sql}");
                attached.insert(child, false);
                if sql.contains(" CONCURRENTLY") {
                    assert_ne!(
                        has_default.get(&parent),
                        Some(&true),
                        "concurrent detach with DEFAULT present: {sql}"
                    );
                }
            } else if sql.contains(" ATTACH PARTITION ") {
                let tail = sql.split(" ATTACH PARTITION ").nth(1).unwrap();
                let child = tail.split(' ').next().unwrap().to_string();
                let bound = tail
                    .split_once(' ')
                    .map(|(_, b)| b.trim_end_matches(';').to_string())
                    .unwrap();
                let (_, orig) = child_bound.get(&child).expect("attach of unknown child");
                assert_eq!(&bound, orig, "re-attach with different bounds: {sql}");
                assert_eq!(attached.get(&child), Some(&false), "double attach: {sql}");
                attached.insert(child, true);
            } else if let Some(rest) = sql.strip_prefix("SELECT count(*) FROM ") {
                let name = rest.split([' ', ';']).next().unwrap();
                assert_eq!(live.get(name), Some(&true), "select from dead relation: {sql}");
            } else if let Some(rest) = sql
                .strip_prefix("SELECT * FROM ")
                .or_else(|| sql.strip_prefix("EXPLAIN (COSTS OFF) SELECT count(*) FROM "))
            {
                let name = rest.split([' ', ';']).next().unwrap();
                assert_eq!(live.get(name), Some(&true), "read of dead relation: {sql}");
            }
        }
    }

    #[test]
    fn extend_catalog_registers_live_parents_with_sound_dml_metadata() {
        let base = FixtureCatalog.load_catalog().unwrap();
        let (_, _, state) = gen_many(0xCA7, 400, "");
        let eff = state.extend_catalog(&base);
        let live_n = state.parents.iter().filter(|p| p.live).count();
        assert!(live_n > 0, "no live parent after 400 groups");
        assert!(live_n <= MAX_LIVE_PARENTS);
        assert_eq!(eff.tables.len(), base.tables.len() + live_n);
        for p in state.parents.iter().filter(|p| p.live) {
            let t = &p.table;
            // DML-eligible (routed INSERTs, key-moving UPDATEs, probes).
            assert!(t.pk.as_ref().is_some_and(|k| k.column == "pk" && k.seeded_max == 0));
            assert!(!t.at_most_one_row);
            // ON CONFLICT / pk collisions only where a real unique
            // constraint exists (partition key == pk).
            assert_eq!(t.pk_unique, p.kind.pk_keyed(), "{}", t.name);
            assert_eq!(t.unique_key.is_some(), p.kind.pk_keyed(), "{}", t.name);
            for key in ["pk", "k_int", "k_text"] {
                assert!(t.columns.iter().any(|c| c.name == key), "{} missing {key}", t.name);
            }
        }
    }

    /// Range bounds are internally consistent: range_pk children tile
    /// MINVALUE..MAXVALUE with no DEFAULT; defaulted kinds always emit a
    /// DEFAULT child; multi-column upper bounds always end (b, MINVALUE).
    #[test]
    fn bounds_shapes_hold() {
        let (sqls, _, state) = gen_many(0x60_0D, 800, "part:create=10,part:drop=0");
        for p in &state.parents {
            let n_default = p.children.iter().filter(|c| c.is_default).count();
            match p.kind {
                PartKind::RangePk | PartKind::HashPk => assert_eq!(n_default, 0),
                _ => assert_eq!(n_default, 1, "{:?}", p.table.name),
            }
            if p.kind == PartKind::RangePk {
                let bounds: Vec<&str> =
                    p.children.iter().map(|c| c.bound.as_str()).collect();
                assert!(bounds.first().unwrap().contains("FROM (MINVALUE)"));
                assert!(bounds.last().unwrap().ends_with("TO (MAXVALUE)"));
                // Adjacent partitions share their edge.
                for w in p.children.windows(2) {
                    let hi = w[0].bound.split(" TO (").nth(1).unwrap().trim_end_matches(')');
                    let lo = w[1].bound.split("FROM (").nth(1).unwrap().split(')').next().unwrap();
                    assert_eq!(hi, lo, "{:?}", p.table.name);
                }
            }
            if p.kind == PartKind::RangeMulti {
                for c in p.children.iter().filter(|c| !c.is_default) {
                    assert!(
                        c.bound.ends_with(", MINVALUE)"),
                        "multi-col upper bound must end (b, MINVALUE): {}",
                        c.bound
                    );
                }
            }
        }
        // Hash parents cover every remainder of their modulus.
        let all = sqls.join("\n");
        if all.contains("MODULUS 3") {
            for r in 0..3 {
                assert!(all.contains(&format!("(MODULUS 3, REMAINDER {})", r)));
            }
        }
    }
}
