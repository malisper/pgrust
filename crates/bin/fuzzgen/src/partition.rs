//! Partition-surface drain module (PARTITION lane): the partitioning
//! machinery that the standing `part`/`partalt`/`plansel` modules leave
//! dark. gap-report-005/006 rank the partition-wise-join BOUND MERGING
//! family in backend/partitioning/partbounds.c as the single largest
//! uncovered partition surface (merge_list_bounds 127, merge_range_bounds
//! 112, generate_matching_part_pairs, merge_matching_partitions,
//! merge_default_partitions, merge_null_partitions, build_merged_partition_
//! bounds, fix_merged_indexes, process_outer/inner_partition,
//! partition_bounds_merge, add_merged_range_bounds, get_range_partition[_
//! internal], compare_range_partitions, get_merged_range_bounds,
//! init/free_partition_map, is_dummy_partition, merge_partition_with_dummy)
//! plus check_default_partition_contents (partbounds.c, ATTACH validation),
//! ExecBuildSlotPartitionKeyDescription + adjust_partition_colnos[_using_
//! map] (execPartition.c, no-partition-found error detail and attribute-
//! mapped cross-partition routing), get_steps_using_prefix_recurse
//! (partprune.c, prefix-equality runtime pruning) and get_partition_qual_
//! relid (partcache.c, partition-constraint deparse).
//!
//! WHY the standing modules miss the merge family: partition_bounds_merge
//! is only called from try_partitionwise_join (joinrels.c) when the two
//! joined parents have DIFFERENT PartitionBoundInfo — when the bounds are
//! equal, partition_bounds_equal short-circuits to the fast path and every
//! merge_* function stays dark. `plansel` forces partitionwise joins only
//! over IDENTICAL-bound twins (see plansel::gen_create), so it exercises the
//! equal-bounds arm and never the merge arm. This module builds
//! compatibly-partitioned parents with DELIBERATELY DIFFERENT (but
//! internally valid, fully covering) bound lists, so the planner MUST merge.
//!
//! Mechanism: fully self-contained statement groups (no session-persistent
//! state in `Gen`). Each group drops its fixtures with `DROP TABLE IF EXISTS
//! ... CASCADE` first (re-entrant against a ddmin-reduced prior group),
//! CREATEs a family's fixtures, runs the family's probes, and DROPs them at
//! the end. Fixed per-family names are safe because every group cleans up
//! after itself and no other module touches the `fz_pt_*` namespace.
//!
//! Determinism laws (same discipline as crate::plansel / crate::spill):
//!   - every value is a pure integer formula of the row number (identical
//!     A/B by construction); no float anywhere (B1);
//!   - every row-returning statement carries a TOTAL order (ORDER BY ending
//!     in a key that is unique within the projection); aggregate-only shapes
//!     (count/sum/min/max) are accumulation-order-independent and skip it;
//!   - GUC brackets SET each pair then RESET in reverse, all in ONE group,
//!     so both differential sides always leave the group with identical GUC
//!     state (the runner's GucPinned wrapper re-applies the C-parity pin);
//!   - the result set of a deterministic query is a pure function of table
//!     state: it must be identical across the merged and non-merged plans
//!     and across both engines. Any A/B divergence is a real finding.
//!   - statements that intentionally ERROR (no-partition-found routing,
//!     default-contents ATTACH conflict) run in AUTOCOMMIT (never inside a
//!     BEGIN bracket), so the error is isolated to that one statement and
//!     never aborts the rest of the group's sweep; the differ compares them
//!     by error identity.
//!
//! Fixtures are small (<= ~720 rows) and dropped in-group.

use crate::stmt::{Gen, StmtKind};

/// Range split layouts over the domain [0, DOM): each entry is the ordered
/// list of interior upper bounds; the first partition starts at 0 and the
/// last ends at DOM, so [0, DOM) is fully covered (no routing miss on load).
const DOM: i64 = 300;
const RANGE_LAYOUTS: &[&[i64]] = &[
    &[100, 200],          // 3 parts: [0,100) [100,200) [200,300)
    &[150],               // 2 parts: [0,150) [150,300)
    &[60, 120, 180, 240], // 5 parts
    &[120, 240],          // 3 parts, offset splits
    &[75, 150, 225],      // 4 parts
];

/// List value groupings over the domain 0..=5 (plus NULL handling).
const LIST_LAYOUTS: &[&[&[i64]]] = &[
    &[&[0, 1], &[2, 3], &[4, 5]],
    &[&[0], &[1, 2], &[3, 4]],
    &[&[0, 2, 4], &[1, 3, 5]],
    &[&[5], &[0, 1, 2], &[3, 4]],
];

// --------------------------------------------------------------- brackets ---

/// Wrap `body` in SET/RESET pairs (RESETs in reverse), all in one group.
fn bracket(gucs: &[(&str, &str)], body: Vec<StmtKind>) -> Vec<StmtKind> {
    let mut stmts: Vec<StmtKind> = gucs
        .iter()
        .map(|(n, v)| StmtKind::Raw(format!("SET {n} = {v};")))
        .collect();
    stmts.extend(body);
    for (n, _) in gucs.iter().rev() {
        stmts.push(StmtKind::Raw(format!("RESET {n};")));
    }
    stmts
}

const PWISE: &[(&str, &str)] = &[
    ("enable_partitionwise_join", "on"),
    ("enable_partitionwise_aggregate", "on"),
];

/// Partitionwise join forced away from hashjoin, so the merge machinery is
/// exercised under both merge- and nestloop-child join strategies.
const PWISE_NEST: &[(&str, &str)] = &[
    ("enable_partitionwise_join", "on"),
    ("enable_partitionwise_aggregate", "on"),
    ("enable_hashjoin", "off"),
    ("enable_mergejoin", "off"),
];

fn raw(s: String) -> StmtKind {
    StmtKind::Raw(s)
}

// --------------------------------------------------------------- dispatch ---

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_partition_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("partition");
    let action = g.weights.pick(
        g.rng,
        &[
            "partition:pwrange",
            "partition:pwlist",
            "partition:route_err",
            "partition:attach_default",
            "partition:colmap",
            "partition:prune_prefix",
            "partition:constraintdef",
        ],
    );
    g.fire(action);
    match action {
        "partition:pwrange" => gen_pwrange(g),
        "partition:pwlist" => gen_pwlist(g),
        "partition:route_err" => gen_route_err(g),
        "partition:attach_default" => gen_attach_default(g),
        "partition:colmap" => gen_colmap(g),
        "partition:prune_prefix" => gen_prune_prefix(g),
        _ => gen_constraintdef(g),
    }
}

// ------------------------------------------------------------- pwrange ---

/// Build a RANGE-partitioned table `name` over key column `k` using the
/// interior split points `splits` (fully covering [0, DOM)), then bulk-load
/// `rows` deterministic rows and ANALYZE.
fn build_range(
    name: &str,
    splits: &[i64],
    with_default: bool,
    mult: i64,
    rows: i64,
) -> Vec<StmtKind> {
    let mut stmts = vec![
        raw(format!("DROP TABLE IF EXISTS {name} CASCADE;")),
        raw(format!(
            "CREATE TABLE {name} (k int4 NOT NULL, v int4, t text) PARTITION BY RANGE (k);"
        )),
    ];
    let mut lo = 0i64;
    let mut bounds: Vec<(i64, i64)> = Vec::new();
    for &s in splits {
        bounds.push((lo, s));
        lo = s;
    }
    bounds.push((lo, DOM));
    for (i, (a, b)) in bounds.iter().enumerate() {
        stmts.push(raw(format!(
            "CREATE TABLE {name}_p{i} PARTITION OF {name} FOR VALUES FROM ({a}) TO ({b}) WITH (autovacuum_enabled = off);"
        )));
    }
    if with_default {
        stmts.push(raw(format!(
            "CREATE TABLE {name}_pd PARTITION OF {name} DEFAULT WITH (autovacuum_enabled = off);"
        )));
    }
    // k in [0, DOM): always routes into a covering partition.
    stmts.push(raw(format!(
        "INSERT INTO {name} SELECT (i * {mult}) % {DOM}, i, 'r' || (i % 17) \
         FROM generate_series(1, {rows}) i;"
    )));
    stmts.push(raw(format!("ANALYZE {name};")));
    stmts
}

/// RANGE partition-wise join/aggregate with DIFFERENT bounds on the two
/// parents -> partition_bounds_merge / merge_range_bounds and the whole
/// range-merge family (compare_range_partitions, get_[merged_]range_bounds,
/// add_merged_range_bounds, process_outer/inner_partition, dummy handling).
fn gen_pwrange(g: &mut Gen) -> Vec<StmtKind> {
    let a = "fz_pt_ra";
    let b = "fz_pt_rb";
    // Two DIFFERENT layouts: the merge is required (bounds not equal).
    let ia = g.rng.below_usize(RANGE_LAYOUTS.len());
    let mut ib = g.rng.below_usize(RANGE_LAYOUTS.len());
    if ib == ia {
        ib = (ib + 1) % RANGE_LAYOUTS.len();
    }
    let a_default = g.rng.chance(1, 2);
    let b_default = g.rng.chance(1, 3);
    let mut stmts = Vec::new();
    stmts.extend(build_range(a, RANGE_LAYOUTS[ia], a_default, 7, 600));
    stmts.extend(build_range(b, RANGE_LAYOUTS[ib], b_default, 11, 480));

    let guc = if g.rng.chance(1, 2) {
        PWISE
    } else {
        PWISE_NEST
    };
    let mut body = vec![
        // INNER join on the partition key: the core merge match path.
        raw(format!(
            "SELECT x.k, count(*), sum(x.v::int8), sum(y.v::int8) \
             FROM {a} x JOIN {b} y ON x.k = y.k GROUP BY x.k ORDER BY x.k;"
        )),
        // LEFT join: process_outer_partition with a possibly-missing inner
        // (merge_partition_with_dummy on the inner side).
        raw(format!(
            "SELECT count(*), count(y.k), sum(x.v::int8) \
             FROM {a} x LEFT JOIN {b} y ON x.k = y.k;"
        )),
        // FULL join: both-sided dummy handling (is_dummy_partition,
        // merge_partition_with_dummy on either side).
        raw(format!(
            "SELECT count(*), sum(COALESCE(x.v, 0)::int8), sum(COALESCE(y.v, 0)::int8) \
             FROM {a} x FULL JOIN {b} y ON x.k = y.k;"
        )),
        // Partitionwise aggregate over one parent (grouped on the key).
        raw(format!(
            "SELECT k, count(*), sum(v::int8) FROM {a} GROUP BY k ORDER BY k;"
        )),
    ];
    // A low-rate EXPLAIN (COSTS OFF) plan-shape witness of the merged join
    // (same GUCs both sides: pgrust must pick a C-equivalent plan).
    if g.rng.chance(1, 4) {
        body.push(raw(format!(
            "EXPLAIN (COSTS OFF) SELECT x.k FROM {a} x JOIN {b} y ON x.k = y.k;"
        )));
    }
    stmts.extend(bracket(guc, body));
    stmts.push(raw(format!("DROP TABLE {a} CASCADE;")));
    stmts.push(raw(format!("DROP TABLE {b} CASCADE;")));
    stmts
}

// -------------------------------------------------------------- pwlist ---

/// Build a LIST-partitioned table over key `k` using value `groups`; the
/// residue (values not listed) plus optional NULL go to a partition chosen
/// by `null_own` (a dedicated FOR VALUES IN (NULL) partition) and always a
/// DEFAULT for the remaining residue.
fn build_list(
    name: &str,
    groups: &[&[i64]],
    null_own: bool,
    mult: i64,
    rows: i64,
) -> Vec<StmtKind> {
    let mut stmts = vec![
        raw(format!("DROP TABLE IF EXISTS {name} CASCADE;")),
        raw(format!(
            "CREATE TABLE {name} (k int4, v int4, t text) PARTITION BY LIST (k);"
        )),
    ];
    for (i, grp) in groups.iter().enumerate() {
        let vals = grp
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        stmts.push(raw(format!(
            "CREATE TABLE {name}_p{i} PARTITION OF {name} FOR VALUES IN ({vals}) WITH (autovacuum_enabled = off);"
        )));
    }
    if null_own {
        stmts.push(raw(format!(
            "CREATE TABLE {name}_pn PARTITION OF {name} FOR VALUES IN (NULL) WITH (autovacuum_enabled = off);"
        )));
    }
    // DEFAULT catches unlisted residue (and NULL when null_own is false).
    stmts.push(raw(format!(
        "CREATE TABLE {name}_pd PARTITION OF {name} DEFAULT WITH (autovacuum_enabled = off);"
    )));
    // k in 0..=5, with a scattering of NULLs (i % 11 == 0).
    stmts.push(raw(format!(
        "INSERT INTO {name} SELECT CASE WHEN i % 11 = 0 THEN NULL ELSE (i * {mult}) % 6 END, \
         i, 'l' || (i % 13) FROM generate_series(1, {rows}) i;"
    )));
    stmts.push(raw(format!("ANALYZE {name};")));
    stmts
}

/// LIST partition-wise join with DIFFERENT value groupings, NULL and DEFAULT
/// partitions -> merge_list_bounds, merge_matching_partitions,
/// merge_null_partitions, merge_default_partitions.
fn gen_pwlist(g: &mut Gen) -> Vec<StmtKind> {
    let a = "fz_pt_la";
    let b = "fz_pt_lb";
    let ia = g.rng.below_usize(LIST_LAYOUTS.len());
    let mut ib = g.rng.below_usize(LIST_LAYOUTS.len());
    if ib == ia {
        ib = (ib + 1) % LIST_LAYOUTS.len();
    }
    // NULL handled by a dedicated partition on one side, by DEFAULT on the
    // other: exercises the merge_null_partitions asymmetric arm.
    let mut stmts = Vec::new();
    stmts.extend(build_list(a, LIST_LAYOUTS[ia], true, 1, 540));
    stmts.extend(build_list(b, LIST_LAYOUTS[ib], false, 5, 420));

    let guc = if g.rng.chance(1, 2) {
        PWISE
    } else {
        PWISE_NEST
    };
    let body = vec![
        raw(format!(
            "SELECT x.k, count(*), sum(x.v::int8), sum(y.v::int8) \
             FROM {a} x JOIN {b} y ON x.k = y.k GROUP BY x.k ORDER BY x.k;"
        )),
        raw(format!(
            "SELECT count(*), count(y.k), sum(x.v::int8) \
             FROM {a} x LEFT JOIN {b} y ON x.k = y.k;"
        )),
        raw(format!(
            "SELECT count(*), sum(COALESCE(x.v, 0)::int8), sum(COALESCE(y.v, 0)::int8) \
             FROM {a} x FULL JOIN {b} y ON x.k = y.k;"
        )),
        // Partitionwise aggregate; NULL group ordered last for total order.
        raw(format!(
            "SELECT k, count(*), sum(v::int8) FROM {a} GROUP BY k ORDER BY k NULLS LAST;"
        )),
    ];
    stmts.extend(bracket(guc, body));
    stmts.push(raw(format!("DROP TABLE {a} CASCADE;")));
    stmts.push(raw(format!("DROP TABLE {b} CASCADE;")));
    stmts
}

// ------------------------------------------------------------ route_err ---

/// No-partition-found routing errors -> ExecBuildSlotPartitionKeyDescription
/// (the "Partition key of the failing row contains ..." detail builder).
/// Each failing INSERT is a standalone autocommit statement so the ERROR is
/// isolated and compared by error identity; the surrounding CREATE/DROP and
/// the successful routing INSERTs are unaffected.
fn gen_route_err(g: &mut Gen) -> Vec<StmtKind> {
    let r = "fz_pt_re";
    let r2 = "fz_pt_re2";
    let l = "fz_pt_rl";
    let mut stmts = vec![
        raw(format!("DROP TABLE IF EXISTS {r} CASCADE;")),
        raw(format!("DROP TABLE IF EXISTS {r2} CASCADE;")),
        raw(format!("DROP TABLE IF EXISTS {l} CASCADE;")),
        // Single-column RANGE, no DEFAULT, covers only [0, 200).
        raw(format!(
            "CREATE TABLE {r} (k int4, v int4) PARTITION BY RANGE (k);"
        )),
        raw(format!(
            "CREATE TABLE {r}_p0 PARTITION OF {r} FOR VALUES FROM (0) TO (100) WITH (autovacuum_enabled = off);"
        )),
        raw(format!(
            "CREATE TABLE {r}_p1 PARTITION OF {r} FOR VALUES FROM (100) TO (200) WITH (autovacuum_enabled = off);"
        )),
        raw(format!(
            "INSERT INTO {r} SELECT i % 200, i FROM generate_series(1, 200) i;"
        )),
    ];
    // A value outside every partition and with no DEFAULT -> 23514.
    let bad = 200 + g.rng.below(500) as i64;
    stmts.push(raw(format!("INSERT INTO {r} VALUES ({bad}, 1);")));
    // Multi-column RANGE key: richer failing-key description.
    stmts.push(raw(format!(
        "CREATE TABLE {r2} (a int4, b int4, v int4) PARTITION BY RANGE (a, b);"
    )));
    stmts.push(raw(format!(
        "CREATE TABLE {r2}_p0 PARTITION OF {r2} FOR VALUES FROM (0, 0) TO (10, 0) WITH (autovacuum_enabled = off);"
    )));
    stmts.push(raw(format!(
        "CREATE TABLE {r2}_p1 PARTITION OF {r2} FOR VALUES FROM (10, 0) TO (20, 0) WITH (autovacuum_enabled = off);"
    )));
    let ba = 20 + g.rng.below(50) as i64;
    stmts.push(raw(format!("INSERT INTO {r2} VALUES ({ba}, 5, 1);")));
    // LIST, no DEFAULT: unlisted value fails to route.
    stmts.push(raw(format!(
        "CREATE TABLE {l} (k int4, v int4) PARTITION BY LIST (k);"
    )));
    stmts.push(raw(format!(
        "CREATE TABLE {l}_p0 PARTITION OF {l} FOR VALUES IN (1, 2, 3) WITH (autovacuum_enabled = off);"
    )));
    let bl = 4 + g.rng.below(90) as i64;
    stmts.push(raw(format!("INSERT INTO {l} VALUES ({bl}, 1);")));
    stmts.push(raw(format!("DROP TABLE {r} CASCADE;")));
    stmts.push(raw(format!("DROP TABLE {r2} CASCADE;")));
    stmts.push(raw(format!("DROP TABLE {l} CASCADE;")));
    stmts
}

// -------------------------------------------------------- attach_default ---

/// ATTACH validation against a DEFAULT partition that already holds rows ->
/// check_default_partition_contents (partbounds.c). The success arm scans a
/// DEFAULT with no rows in the new bound; the failure arm hits a DEFAULT row
/// that would belong to the newly attached partition (ERROR 23514, isolated
/// autocommit statement).
fn gen_attach_default(g: &mut Gen) -> Vec<StmtKind> {
    let p = "fz_pt_ad";
    let ok = "fz_pt_ad_ok";
    let bad = "fz_pt_ad_bad";
    let mut stmts = vec![
        raw(format!("DROP TABLE IF EXISTS {p} CASCADE;")),
        raw(format!("DROP TABLE IF EXISTS {ok} CASCADE;")),
        raw(format!("DROP TABLE IF EXISTS {bad} CASCADE;")),
        raw(format!(
            "CREATE TABLE {p} (k int4, v int4) PARTITION BY RANGE (k);"
        )),
        raw(format!(
            "CREATE TABLE {p}_p0 PARTITION OF {p} FOR VALUES FROM (0) TO (50) WITH (autovacuum_enabled = off);"
        )),
        raw(format!("CREATE TABLE {p}_pd PARTITION OF {p} DEFAULT WITH (autovacuum_enabled = off);")),
        // Rows 0..49 -> p0, rows 50..149 -> DEFAULT.
        raw(format!(
            "INSERT INTO {p} SELECT i, i FROM generate_series(0, 149) i;"
        )),
        // Success: attach a range the DEFAULT has NO rows for (>=200);
        // check_default_partition_contents scans the DEFAULT and passes.
        raw(format!("CREATE TABLE {ok} (k int4, v int4) WITH (autovacuum_enabled = off);")),
        raw(format!(
            "INSERT INTO {ok} SELECT i, i FROM generate_series(200, 249) i;"
        )),
        raw(format!(
            "ALTER TABLE {p} ATTACH PARTITION {ok} FOR VALUES FROM (200) TO (250);"
        )),
        // Failure: attach [50,100) — the DEFAULT holds rows 50..99 that
        // would belong to the new partition -> ERROR (isolated autocommit).
        raw(format!("CREATE TABLE {bad} (k int4, v int4) WITH (autovacuum_enabled = off);")),
        raw(format!(
            "ALTER TABLE {p} ATTACH PARTITION {bad} FOR VALUES FROM (50) TO (100);"
        )),
        // Ordered read-back witness of the (only successfully-mutated) tree.
        raw(format!("SELECT k, v FROM {p} ORDER BY k, v;")),
    ];
    // Drop: `bad` never attached (its ATTACH errored), so drop it separately.
    stmts.push(raw(format!("DROP TABLE {p} CASCADE;")));
    stmts.push(raw(format!("DROP TABLE IF EXISTS {bad} CASCADE;")));
    let _ = g;
    stmts
}

// ----------------------------------------------------------------- colmap ---

/// Attribute-mapped tuple routing across partitions whose physical column
/// order differs from the parent -> adjust_partition_colnos[_using_map]
/// (execPartition.c). A partition ATTACHed from a standalone table built
/// with a different column order needs a parent->child attno map on routing,
/// and a cross-partition UPDATE row-move re-routes through that map.
fn gen_colmap(g: &mut Gen) -> Vec<StmtKind> {
    let p = "fz_pt_cm";
    let c1 = "fz_pt_cm_hi";
    let mut stmts = vec![
        raw(format!("DROP TABLE IF EXISTS {p} CASCADE;")),
        raw(format!("DROP TABLE IF EXISTS {c1} CASCADE;")),
        raw(format!(
            "CREATE TABLE {p} (id int4, k int4, v int4) PARTITION BY RANGE (k);"
        )),
        // Same-order partition for the low range.
        raw(format!(
            "CREATE TABLE {p}_lo PARTITION OF {p} FOR VALUES FROM (0) TO (100) WITH (autovacuum_enabled = off);"
        )),
        // Different physical column order, ATTACHed -> attribute map needed.
        raw(format!("CREATE TABLE {c1} (k int4, v int4, id int4) WITH (autovacuum_enabled = off);")),
        raw(format!(
            "ALTER TABLE {p} ATTACH PARTITION {c1} FOR VALUES FROM (100) TO (200);"
        )),
        // Route into both partitions (the reordered one via the map).
        raw(format!(
            "INSERT INTO {p} SELECT i, i % 200, i * 2 FROM generate_series(1, 199) i;"
        )),
        raw(format!("SELECT id, k, v FROM {p} ORDER BY id;")),
    ];
    // Cross-partition UPDATE row-move re-routing through the attno map (both
    // directions). State is dropped in-group, so the mutation is fine; the
    // ordered read-back is the A/B witness.
    if g.rng.chance(1, 2) {
        stmts.push(raw(format!(
            "UPDATE {p} SET k = k + 100 WHERE k < 100 AND id % 3 = 0;"
        )));
    } else {
        stmts.push(raw(format!(
            "UPDATE {p} SET k = k - 100 WHERE k >= 100 AND id % 4 = 0;"
        )));
    }
    stmts.push(raw(format!("SELECT id, k, v FROM {p} ORDER BY id;")));
    stmts.push(raw(format!("DROP TABLE {p} CASCADE;")));
    stmts
}

// ------------------------------------------------------------ prune_prefix ---

/// Prefix-equality runtime pruning over a multi-column RANGE key ->
/// get_steps_using_prefix_recurse (partprune.c) and the init/exec pruning
/// arms. A generic (parameterized) plan under force_generic_plan drives
/// execution-time pruning; a const-qual variant drives plan-time pruning.
/// Both compare by result identity (a pure function of table state).
fn gen_prune_prefix(g: &mut Gen) -> Vec<StmtKind> {
    let t = "fz_pt_pp";
    let mut stmts = vec![
        raw(format!("DROP TABLE IF EXISTS {t} CASCADE;")),
        // Multi-column RANGE key (a, b): partitions split on `a` with `b`
        // MINVALUE, so a prefix equality on `a` plus a range on `b` builds
        // prefix pruning steps.
        raw(format!(
            "CREATE TABLE {t} (a int4 NOT NULL, b int4 NOT NULL, v int4) PARTITION BY RANGE (a, b);"
        )),
        raw(format!(
            "CREATE TABLE {t}_p0 PARTITION OF {t} FOR VALUES FROM (MINVALUE, MINVALUE) TO (10, MINVALUE) WITH (autovacuum_enabled = off);"
        )),
        raw(format!(
            "CREATE TABLE {t}_p1 PARTITION OF {t} FOR VALUES FROM (10, MINVALUE) TO (20, MINVALUE) WITH (autovacuum_enabled = off);"
        )),
        raw(format!(
            "CREATE TABLE {t}_p2 PARTITION OF {t} FOR VALUES FROM (20, MINVALUE) TO (MAXVALUE, MAXVALUE) WITH (autovacuum_enabled = off);"
        )),
        raw(format!(
            "INSERT INTO {t} SELECT (i * 7) % 30, (i * 13) % 50, i FROM generate_series(1, 600) i;"
        )),
        raw(format!("ANALYZE {t};")),
    ];
    let av = g.rng.below(30) as i64;
    let bv = g.rng.below(50) as i64;
    // Plan-time pruning with const quals (init pruning path).
    stmts.push(raw(format!(
        "SELECT a, b, count(*) FROM {t} WHERE a = {av} AND b >= {bv} GROUP BY a, b ORDER BY a, b;"
    )));
    // Execution-time pruning under a forced generic plan (prefix steps built
    // from a parameterized prefix equality + trailing range).
    let body = vec![
        raw(format!(
            "PREPARE fz_pt_pp_q (int4, int4) AS \
             SELECT a, b, count(*) FROM {t} WHERE a = $1 AND b >= $2 GROUP BY a, b ORDER BY a, b;"
        )),
        raw(format!("EXECUTE fz_pt_pp_q({av}, {bv});")),
        raw(format!(
            "EXECUTE fz_pt_pp_q({}, {});",
            g.rng.below(30),
            g.rng.below(50)
        )),
        raw(format!(
            "EXECUTE fz_pt_pp_q({}, {});",
            g.rng.below(30),
            g.rng.below(50)
        )),
        raw("DEALLOCATE fz_pt_pp_q;".to_string()),
    ];
    stmts.extend(bracket(
        &[
            ("plan_cache_mode", "force_generic_plan"),
            ("enable_partition_pruning", "on"),
        ],
        body,
    ));
    stmts.push(raw(format!("DROP TABLE {t} CASCADE;")));
    stmts
}

// ---------------------------------------------------------- constraintdef ---

/// Partition-constraint deparse + equal-bounds partitionwise ->
/// get_partition_qual_relid (partcache.c) via pg_get_partition_constraintdef,
/// and partition_bounds_equal via an IDENTICAL-bound partitionwise self-join
/// (the fast-path arm the merge families deliberately avoid). Also a
/// validated ATTACH/DETACH cycle over the constraint surface.
fn gen_constraintdef(g: &mut Gen) -> Vec<StmtKind> {
    let t = "fz_pt_cd";
    let tw = "fz_pt_cd2";
    let ext = "fz_pt_cd_ext";
    let mut stmts = vec![
        raw(format!("DROP TABLE IF EXISTS {t} CASCADE;")),
        raw(format!("DROP TABLE IF EXISTS {tw} CASCADE;")),
        raw(format!("DROP TABLE IF EXISTS {ext} CASCADE;")),
    ];
    // Two IDENTICAL-bound RANGE tables (equal-bounds partitionwise fast path).
    for name in [t, tw] {
        stmts.push(raw(format!(
            "CREATE TABLE {name} (k int4 NOT NULL, v int4) PARTITION BY RANGE (k);"
        )));
        stmts.push(raw(format!(
            "CREATE TABLE {name}_p0 PARTITION OF {name} FOR VALUES FROM (0) TO (100) WITH (autovacuum_enabled = off);"
        )));
        stmts.push(raw(format!(
            "CREATE TABLE {name}_p1 PARTITION OF {name} FOR VALUES FROM (100) TO (200) WITH (autovacuum_enabled = off);"
        )));
        stmts.push(raw(format!(
            "CREATE TABLE {name}_pd PARTITION OF {name} DEFAULT WITH (autovacuum_enabled = off);"
        )));
        stmts.push(raw(format!(
            "INSERT INTO {name} SELECT (i * 3) % 250, i FROM generate_series(1, 300) i;"
        )));
        stmts.push(raw(format!("ANALYZE {name};")));
    }
    // Partition-constraint deparse for every partition (ordered) ->
    // get_partition_qual_relid.
    stmts.push(raw(format!(
        "SELECT c.relname, pg_get_partition_constraintdef(c.oid) \
         FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid \
         WHERE i.inhparent = '{t}'::regclass ORDER BY c.relname;"
    )));
    // Equal-bounds partitionwise join (partition_bounds_equal true arm).
    stmts.extend(bracket(
        PWISE,
        vec![raw(format!(
            "SELECT x.k, count(*), sum(y.v::int8) \
             FROM {t} x JOIN {tw} y ON x.k = y.k GROUP BY x.k ORDER BY x.k;"
        ))],
    ));
    // Validated ATTACH (no CHECK -> full validation scan) then DETACH.
    stmts.push(raw(format!(
        "CREATE TABLE {ext} (k int4 NOT NULL, v int4) WITH (autovacuum_enabled = off);"
    )));
    stmts.push(raw(format!(
        "INSERT INTO {ext} SELECT 200 + (i % 100), i FROM generate_series(1, 80) i;"
    )));
    stmts.push(raw(format!(
        "ALTER TABLE {t} ATTACH PARTITION {ext} FOR VALUES FROM (200) TO (300);"
    )));
    stmts.push(raw(format!(
        "SELECT pg_get_partition_constraintdef('{ext}'::regclass);"
    )));
    stmts.push(raw(format!("ALTER TABLE {t} DETACH PARTITION {ext};")));
    stmts.push(raw(format!("DROP TABLE {t} CASCADE;")));
    stmts.push(raw(format!("DROP TABLE {tw} CASCADE;")));
    stmts.push(raw(format!("DROP TABLE {ext} CASCADE;")));
    let _ = g;
    stmts
}

// ------------------------------------------------------------------ tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn gen_groups(seed: u64, n: usize) -> Vec<Vec<String>> {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let weights = WeightTable::defaults();
        let mut rng = Rng::new(seed);
        let mut productions = Vec::new();
        let mut out = Vec::new();
        let mut g = Gen::new(&mut rng, &cat, &weights, &mut productions, 4);
        for _ in 0..n {
            let stmts = gen_partition_module(&mut g);
            out.push(stmts.iter().map(|s| s.to_sql()).collect());
        }
        out
    }

    #[test]
    fn deterministic_per_seed() {
        assert_eq!(gen_groups(5, 80), gen_groups(5, 80));
    }

    #[test]
    fn brackets_are_set_reset_balanced() {
        // Every SET in a group has a matching RESET in the SAME group
        // (RESETs in reverse order); this module opens no BEGIN brackets.
        for group in gen_groups(11, 400) {
            let mut sets: Vec<String> = Vec::new();
            for sql in &group {
                assert_ne!(sql, "BEGIN;", "partition module must not open transactions");
                if let Some(rest) = sql.strip_prefix("SET ") {
                    sets.push(rest.split(' ').next().unwrap().to_string());
                } else if let Some(rest) = sql.strip_prefix("RESET ") {
                    let name = rest.trim_end_matches(';').to_string();
                    let pos = sets
                        .iter()
                        .rposition(|s| *s == name)
                        .unwrap_or_else(|| panic!("RESET {name} without SET in {group:?}"));
                    sets.remove(pos);
                }
            }
            assert!(sets.is_empty(), "unRESET SETs {sets:?} in {group:?}");
        }
    }

    #[test]
    fn row_returning_selects_are_totally_ordered() {
        // Every plain SELECT that projects non-aggregate columns carries an
        // ORDER BY. Aggregate-only shapes (count/sum/min/max) and scalar
        // catalog probes (pg_get_partition_constraintdef) are exempt, as is
        // EXPLAIN (plan text).
        for group in gen_groups(23, 400) {
            for sql in &group {
                if !sql.starts_with("SELECT ") {
                    continue;
                }
                let exempt = sql.starts_with("SELECT count(")
                    || sql.starts_with("SELECT pg_get_partition_constraintdef(")
                    || sql.contains("pg_get_partition_constraintdef(c.oid)")
                    || sql.starts_with("EXPLAIN");
                if !exempt {
                    assert!(
                        sql.contains("ORDER BY"),
                        "row-returning statement without total order: {sql}"
                    );
                }
            }
        }
    }

    /// Round-18a blanket rule (exd RB-15 lineage): every storage-bearing
    /// partition CREATE TABLE (children and plain side tables) pins
    /// autovacuum_enabled = off — the module emits compared EXPLAIN
    /// (COSTS OFF) prune probes. Partitioned parents are exempt (no
    /// storage; the reloption is rejected there).
    #[test]
    fn creates_pin_autovacuum_off() {
        let mut seen = 0;
        for grp in gen_groups(0x18A, 600) {
            for sql in grp {
                if !sql.starts_with("CREATE TABLE ") || sql.contains(" PARTITION BY ") {
                    continue;
                }
                assert!(
                    sql.contains("autovacuum_enabled = off"),
                    "partition fixture does not pin autovacuum off: `{sql}`"
                );
                seen += 1;
            }
        }
        assert!(seen > 0, "no CREATE TABLE generated in 600 groups");
    }

    #[test]
    fn every_created_table_is_dropped() {
        // Self-contained discipline: a group creates and drops within
        // itself. Every CREATE TABLE name has a later DROP of that name in
        // the same group (partitions vanish via CASCADE / partition-of).
        for group in gen_groups(31, 300) {
            let mut created_roots: Vec<String> = Vec::new();
            for sql in &group {
                if let Some(rest) = sql.strip_prefix("CREATE TABLE ") {
                    let name = rest.split_whitespace().next().unwrap().to_string();
                    // Only track root/standalone tables (not PARTITION OF).
                    if !sql.contains("PARTITION OF") {
                        created_roots.push(name);
                    }
                }
            }
            for root in &created_roots {
                let dropped = group.iter().any(|s| {
                    s.starts_with(&format!("DROP TABLE {root} "))
                        || s.starts_with(&format!("DROP TABLE {root};"))
                        || s.contains(&format!("DROP TABLE IF EXISTS {root} "))
                });
                assert!(
                    dropped,
                    "table {root} created but never dropped in group {group:?}"
                );
            }
        }
    }

    #[test]
    fn differ_bounds_in_pwise_families() {
        // The merge families must use two DIFFERENT bound layouts (equal
        // bounds would take the fast path and never merge). Witness that at
        // least some pwrange/pwlist groups create two partitioned parents.
        let mut saw_two_parents = false;
        for group in gen_groups(7, 200) {
            let parents = group
                .iter()
                .filter(|s| s.contains("PARTITION BY RANGE") || s.contains("PARTITION BY LIST"))
                .count();
            if group
                .iter()
                .any(|s| s.contains("enable_partitionwise_join"))
                && parents >= 2
            {
                saw_two_parents = true;
            }
        }
        assert!(
            saw_two_parents,
            "no partitionwise group built two partitioned parents"
        );
    }
}
