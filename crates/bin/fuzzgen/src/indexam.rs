//! Index-AM operator-class deep drain (INDEXAM; line-drain-queue chunk
//! `index-gin-gist` residue after the W5-GIN/GIST waves, plus the SP-GiST
//! half of the same chunk). The idx module (I1/B2) proved all five AMs
//! exist and reached first-order behavior over small fixtures, and btbrin
//! (W5-BTBRIN) drained btree + BRIN; this module drains the GIN / GiST /
//! SP-GiST internals that only fire under real data volume, multi-key
//! scan keys, fastupdate/pending-list churn and page-split / vacuum
//! cascades. Target functions (docs/fuzzing/line-gap-w5gin.md +
//! line-drain-queue.tsv, `index-gin-gist`):
//!
//!   GIN: `ginNewScanKey` / `startScanEntry` / `entryGetItem` /
//!   `keyGetItem` / `collectMatchesForHeapRow` (multi-key AND/OR scan
//!   keys, lossy vs exact posting lists, the GIN_SEARCH_MODE_ALL /
//!   EVERYTHING arms from empty-array and pure-negation queries, partial
//!   match via tsquery prefix `:*` — comparePartial), pending-list merge
//!   (`ginInsertCleanup` via fastupdate=on + gin_clean_pending_list) and
//!   `ginbulkdelete`/`ginvacuumcleanup` via delete + VACUUM.
//!
//!   GiST: `gistSplitByKey` / `gistUserPicksplit` / `gistdoinsert`
//!   (multi-way and secondary picksplit under wide polygon/circle keys at
//!   volume), the buffering build (buffering=on), KNN ordered scans
//!   (`gistrescan` distance arms), `gistvacuum_delete_empty_pages` /
//!   `gistbulkdelete` via bulk delete + VACUUM, and the exclusion-
//!   constraint check path (`check_exclusion_constraint`).
//!
//!   SP-GiST: `doPickSplit` / `spgdoinsert` (radix node split under long
//!   shared text prefixes; quadtree/kd point picksplit at volume),
//!   `spg_kd_inner_consistent` (kd_point_ops index forced by being the
//!   only point index of its column), `spg_quad_inner_consistent` /
//!   box quad, inet, prefix (`^@`) and range inner-consistent arms, plus
//!   `spgvacuumscan` / `spgbulkdelete`.
//!
//!   Cross-cutting: partial indexes (predicate match and nonmatch),
//!   expression indexes, INCLUDE (covering) index-only scans,
//!   multicolumn indexes and a non-default operator class
//!   (text_pattern_ops LIKE-prefix).
//!
//! Correctness bar (LD7/opt2/btbrin law): with deterministic data the
//! forced-index result must be byte-identical across both engines; a
//! divergence between the index path and the sequential path on the same
//! query is a HIGH-severity finding (surfaced at triage — both engines
//! replay the identical forced-index stream, so a pgrust index bug shows
//! as a pgrust-vs-C divergence on the forced-index statement). Plans are
//! not compared here; coverage comes from planning + executing real
//! statements, and the exclusion-violation / oversized-key errors are
//! themselves compared arms.
//!
//! Discipline (spill/opt2/btbrin rules): self-contained groups over fixed
//! `fz_ix_*` fixtures created and dropped in-group; every SET has its
//! RESET in reverse order in the same group; row-returning statements
//! carry a total ORDER BY or are single-row aggregates; exact-typed
//! aggregates only (no float surfaces, B1 — KNN distance is only ever an
//! ORDER BY key with a pk tiebreak, never projected); every data value is
//! a pure integer / lpad / md5-free formula of generate_series, so both
//! engines load byte-identical data. VACUUM / CREATE INDEX run outside
//! explicit transactions by construction (module groups are top-level).

use crate::stmt::{Gen, StmtKind};

fn raw(s: impl Into<String>) -> StmtKind {
    StmtKind::Raw(s.into())
}

fn raws(list: &[&str]) -> Vec<StmtKind> {
    list.iter().map(|s| raw(*s)).collect()
}

/// Bracket `body` with SET .. / RESET .. pairs (reverse order on the way
/// out) so GUC state is restored no matter which statements error.
fn bracket(gucs: &[(&str, &str)], body: Vec<StmtKind>) -> Vec<StmtKind> {
    let mut v = Vec::with_capacity(body.len() + 2 * gucs.len());
    for (g, val) in gucs {
        v.push(raw(format!("SET {g} = {val};")));
    }
    v.extend(body);
    for (g, _) in gucs.iter().rev() {
        v.push(raw(format!("RESET {g};")));
    }
    v
}

/// Sample `k` distinct entries from `pool`, order chosen by the session
/// PRNG (Fisher-Yates prefix), so seeds spread over the pool while each
/// group stays bounded.
fn sample(g: &mut Gen, pool: &[&str], k: usize) -> Vec<StmtKind> {
    let k = k.min(pool.len());
    let mut idx: Vec<usize> = (0..pool.len()).collect();
    for i in 0..k {
        let j = i + g.rng.below_usize(idx.len() - i);
        idx.swap(i, j);
    }
    idx.iter().take(k).map(|&i| raw(pool[i])).collect()
}

// GIN and BRIN only offer bitmap scans; disabling seqscan is enough to
// force the AM. GiST / SP-GiST support plain index scans (needed for KNN
// ordering) and bitmap; seqscan off lets the planner choose the index.
const SEQOFF: &[(&str, &str)] = &[("enable_seqscan", "off")];
// Index-only scan forcing (covering INCLUDE probes): also bar the bitmap
// path so an index-only scan is chosen over a bitmap heap scan.
const IDXONLY: &[(&str, &str)] = &[("enable_seqscan", "off"), ("enable_bitmapscan", "off")];

const SHAPES: &[&str] = &[
    "indexam:gin",
    "indexam:gist",
    "indexam:spgist",
    "indexam:cover",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_indexam_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("indexam");
    let shape = g.weights.pick(g.rng, SHAPES);
    g.fire2("indexam:shape:", &shape["indexam:".len()..]);
    match shape {
        "indexam:gin" => gen_gin(g),
        "indexam:gist" => gen_gist(g),
        "indexam:spgist" => gen_spgist(g),
        _ => gen_cover(g),
    }
}

// -------------------------------------------------------------- GIN -------

/// GIN scan-key + posting-list + pending-list drain over array / jsonb /
/// tsvector opclasses, forced onto bitmap scans.
fn gen_gin(g: &mut Gen) -> Vec<StmtKind> {
    let rows = 5000 + 2000 * g.rng.below_usize(3) as i64; // 5k..9k
    let mut v = vec![
        raw("CREATE TABLE fz_ix_gin (pk int4 PRIMARY KEY, arr int4[], doc jsonb, tsv tsvector) \
             WITH (autovacuum_enabled = off);"),
        // Dup-rich arrays -> deep posting lists; jsonb keys with a small
        // domain -> many entries share an entry-tree leaf; tsvector words
        // recur so posting lists span multiple pages.
        raw(format!(
            "INSERT INTO fz_ix_gin SELECT i, \
             ARRAY[i % 50, (i * 7) % 50, (i * 13) % 50], \
             jsonb_build_object('a', i % 20, 'b', (i * 3) % 13, 'tag', 'v' || (i % 30)), \
             to_tsvector('english', 'w' || (i % 40) || ' w' || ((i * 13) % 40) || ' common' || (i % 7)) \
             FROM generate_series(1, {rows}) i;"
        )),
        // fastupdate=on accumulates a pending list on INSERT (ginfast.c);
        // fastupdate=off forces every insert straight into the trees.
        raw("CREATE INDEX fz_ix_gin_arr ON fz_ix_gin USING gin (arr) WITH (fastupdate = on);"),
        raw("CREATE INDEX fz_ix_gin_docp ON fz_ix_gin USING gin (doc jsonb_path_ops);"),
        raw("CREATE INDEX fz_ix_gin_doc ON fz_ix_gin USING gin (doc);"),
        raw("CREATE INDEX fz_ix_gin_tsv ON fz_ix_gin USING gin (tsv) WITH (fastupdate = off);"),
        // Multicolumn GIN: ginNewScanKey walks >1 attribute; a query
        // spanning both columns exercises the multi-key AND merge.
        raw("CREATE INDEX fz_ix_gin_mc ON fz_ix_gin USING gin (arr, tsv);"),
        raw("ANALYZE fz_ix_gin;"),
        // Insert churn into the fastupdate index: fills the pending list.
        raw(format!(
            "INSERT INTO fz_ix_gin SELECT {rows} + i, \
             ARRAY[i % 50, (i * 3) % 50], \
             jsonb_build_object('a', i % 20, 'tag', 'v' || (i % 30)), \
             to_tsvector('english', 'w' || (i % 40) || ' common' || (i % 7)) \
             FROM generate_series(1, 1200) i;"
        )),
        // Force the pending-list merge (ginInsertCleanup) explicitly.
        raw("SELECT gin_clean_pending_list('fz_ix_gin_arr')::int8 >= 0;"),
    ];
    let pool: &[&str] = &[
        // Array containment / overlap incl. the empty-array all-scan.
        "SELECT count(*)::int8 FROM fz_ix_gin WHERE arr @> ARRAY[7];",
        "SELECT count(*)::int8 FROM fz_ix_gin WHERE arr @> ARRAY[7, 13];",
        "SELECT count(*)::int8 FROM fz_ix_gin WHERE arr && ARRAY[3, 40, 49];",
        "SELECT count(*)::int8 FROM fz_ix_gin WHERE arr @> ARRAY[]::int4[];",
        "SELECT count(*)::int8 FROM fz_ix_gin WHERE arr @> ARRAY[7] AND arr @> ARRAY[21];",
        // jsonb containment + key-existence (jsonb_ops) and path_ops.
        "SELECT count(*)::int8 FROM fz_ix_gin WHERE doc @> '{\"a\": 5}';",
        "SELECT count(*)::int8 FROM fz_ix_gin WHERE doc @> '{\"tag\": \"v3\"}';",
        "SELECT count(*)::int8 FROM fz_ix_gin WHERE doc @> '{\"a\": 5, \"b\": 6}';",
        "SELECT count(*)::int8 FROM fz_ix_gin WHERE doc ? 'a';",
        "SELECT count(*)::int8 FROM fz_ix_gin WHERE doc ? 'zz';",
        "SELECT count(*)::int8 FROM fz_ix_gin WHERE doc ?| ARRAY['a', 'zz'];",
        "SELECT count(*)::int8 FROM fz_ix_gin WHERE doc ?& ARRAY['a', 'tag'];",
        // tsvector: AND / OR / NOT (all-scan) / prefix (partial match).
        "SELECT count(*)::int8 FROM fz_ix_gin WHERE tsv @@ to_tsquery('english', 'w1 & w2');",
        "SELECT count(*)::int8 FROM fz_ix_gin WHERE tsv @@ to_tsquery('english', 'w1 | w20');",
        "SELECT count(*)::int8 FROM fz_ix_gin WHERE tsv @@ to_tsquery('english', 'w1 & !w2');",
        "SELECT count(*)::int8 FROM fz_ix_gin WHERE tsv @@ to_tsquery('english', '!w5');",
        "SELECT count(*)::int8 FROM fz_ix_gin WHERE tsv @@ to_tsquery('english', 'common1:*');",
        "SELECT count(*)::int8 FROM fz_ix_gin WHERE tsv @@ plainto_tsquery('english', 'common3');",
        // Cross-opclass multi-key AND (collectMatchesForHeapRow).
        "SELECT count(*)::int8 FROM fz_ix_gin WHERE arr @> ARRAY[7] AND tsv @@ to_tsquery('english', 'w1');",
        // Row-returning: total order via pk.
        "SELECT pk FROM fz_ix_gin WHERE arr @> ARRAY[13] ORDER BY pk LIMIT 10;",
        "SELECT pk FROM fz_ix_gin WHERE doc @> '{\"a\": 9}' ORDER BY pk LIMIT 10;",
    ];
    v.extend(bracket(SEQOFF, sample(g, pool, 12)));
    // Delete + VACUUM: ginbulkdelete / ginvacuumcleanup, then a probe over
    // the pruned trees.
    v.extend(raws(&[
        "DELETE FROM fz_ix_gin WHERE pk % 3 = 0;",
        "UPDATE fz_ix_gin SET arr = ARRAY[pk % 50, (pk * 5) % 50] WHERE pk % 7 = 1;",
        "VACUUM fz_ix_gin;",
        "SELECT gin_clean_pending_list('fz_ix_gin_arr')::int8 >= 0;",
    ]));
    v.extend(bracket(
        SEQOFF,
        vec![raw("SELECT count(*)::int8 FROM fz_ix_gin WHERE arr @> ARRAY[7];")],
    ));
    v.push(raw("DROP TABLE fz_ix_gin;"));
    v
}

// ------------------------------------------------------------- GiST --------

/// GiST picksplit / KNN / vacuum / exclusion drain over point / box /
/// polygon / circle / range / inet opclasses at page-splitting volume.
fn gen_gist(g: &mut Gen) -> Vec<StmtKind> {
    let rows = 6000 + 3000 * g.rng.below_usize(3) as i64; // 6k..12k
    let buffering = if g.rng.below_usize(2) == 1 { "on" } else { "off" };
    let mut v = vec![
        raw("CREATE TABLE fz_ix_gist (pk int4 PRIMARY KEY, pt point, bx box, pl polygon, \
             cr circle, rng int4range, addr inet) WITH (autovacuum_enabled = off);"),
        raw(format!(
            "INSERT INTO fz_ix_gist SELECT i, \
             point((i * 7) % 1000, (i * 13) % 1000), \
             box(point((i * 3) % 800, (i * 5) % 800), point((i * 3) % 800 + 20, (i * 5) % 800 + 15)), \
             polygon(box(point((i * 11) % 900, (i * 17) % 900), point((i * 11) % 900 + 30, (i * 17) % 900 + 25))), \
             circle(point((i * 19) % 900, (i * 23) % 900), 5 + i % 20), \
             int4range((i * 5) % 1000, (i * 5) % 1000 + 1 + i % 40), \
             ('10.' || (i % 200) || '.' || ((i * 3) % 250) || '.7/32')::inet \
             FROM generate_series(1, {rows}) i;"
        )),
        raw("CREATE INDEX fz_ix_gist_pt ON fz_ix_gist USING gist (pt);"),
        raw("CREATE INDEX fz_ix_gist_bx ON fz_ix_gist USING gist (bx);"),
        // Wide polygon keys drive gistSplitByKey / gistUserPicksplit;
        // buffering flips gistBuildCallback between the buffered and
        // unbuffered builds.
        raw(format!(
            "CREATE INDEX fz_ix_gist_pl ON fz_ix_gist USING gist (pl) WITH (buffering = {buffering});"
        )),
        raw("CREATE INDEX fz_ix_gist_cr ON fz_ix_gist USING gist (cr);"),
        raw("CREATE INDEX fz_ix_gist_rng ON fz_ix_gist USING gist (rng);"),
        raw("CREATE INDEX fz_ix_gist_addr ON fz_ix_gist USING gist (addr inet_ops);"),
        // Multicolumn GiST.
        raw("CREATE INDEX fz_ix_gist_mc ON fz_ix_gist USING gist (pt, bx);"),
        raw("ANALYZE fz_ix_gist;"),
    ];
    let pool: &[&str] = &[
        "SELECT count(*)::int8 FROM fz_ix_gist WHERE pt <@ box(point(100, 100), point(300, 300));",
        "SELECT count(*)::int8 FROM fz_ix_gist WHERE bx && box(point(200, 200), point(260, 250));",
        "SELECT count(*)::int8 FROM fz_ix_gist WHERE bx @> point(210, 205);",
        "SELECT count(*)::int8 FROM fz_ix_gist WHERE bx <@ box(point(0, 0), point(900, 900));",
        "SELECT count(*)::int8 FROM fz_ix_gist WHERE pl && polygon '((100,100),(180,100),(180,170),(100,170))';",
        "SELECT count(*)::int8 FROM fz_ix_gist WHERE pl @> point(120, 130);",
        "SELECT count(*)::int8 FROM fz_ix_gist WHERE cr @> point(300, 300);",
        "SELECT count(*)::int8 FROM fz_ix_gist WHERE cr && circle(point(400, 400), 30);",
        "SELECT count(*)::int8 FROM fz_ix_gist WHERE rng @> 555;",
        "SELECT count(*)::int8 FROM fz_ix_gist WHERE rng && int4range(400, 460);",
        "SELECT count(*)::int8 FROM fz_ix_gist WHERE rng <@ int4range(0, 2000);",
        "SELECT count(*)::int8 FROM fz_ix_gist WHERE rng -|- int4range(120, 200);",
        "SELECT count(*)::int8 FROM fz_ix_gist WHERE addr <<= inet '10.55.0.0/16';",
        "SELECT count(*)::int8 FROM fz_ix_gist WHERE addr && inet '10.100.0.0/16';",
        // KNN ordered scans: distance is an ORDER BY key only, pk breaks
        // ties -> total order, never a projected float surface.
        "SELECT pk FROM fz_ix_gist ORDER BY pt <-> point(500, 500), pk LIMIT 10;",
        "SELECT pk FROM fz_ix_gist WHERE pt <@ box(point(0, 0), point(500, 500)) ORDER BY pt <-> point(250, 250), pk LIMIT 8;",
        "SELECT pk FROM fz_ix_gist ORDER BY cr <-> point(450, 450), pk LIMIT 10;",
        "SELECT pk FROM fz_ix_gist ORDER BY bx <-> point(300, 300), pk LIMIT 10;",
    ];
    v.extend(bracket(SEQOFF, sample(g, pool, 12)));
    // Bulk delete + VACUUM: gistbulkdelete + gistvacuum_delete_empty_pages
    // (empty inner/leaf pages become deletable), then reinsert to reuse.
    v.extend(vec![
        raw("DELETE FROM fz_ix_gist WHERE pk % 10 <> 0;"),
        raw("VACUUM fz_ix_gist;"),
        raw(format!(
            "INSERT INTO fz_ix_gist SELECT {rows} + i, \
             point((i * 7) % 1000, (i * 13) % 1000), \
             box(point((i * 3) % 800, (i * 5) % 800), point((i * 3) % 800 + 20, (i * 5) % 800 + 15)), \
             polygon(box(point((i * 11) % 900, (i * 17) % 900), point((i * 11) % 900 + 30, (i * 17) % 900 + 25))), \
             circle(point((i * 19) % 900, (i * 23) % 900), 5 + i % 20), \
             int4range((i * 5) % 1000, (i * 5) % 1000 + 1 + i % 40), \
             ('10.' || (i % 200) || '.' || ((i * 3) % 250) || '.7/32')::inet \
             FROM generate_series(1, 2000) i;"
        )),
        raw("VACUUM fz_ix_gist;"),
    ]);
    v.extend(bracket(
        SEQOFF,
        vec![
            raw("SELECT count(*)::int8 FROM fz_ix_gist WHERE bx && box(point(200, 200), point(260, 250));"),
            raw("SELECT pk FROM fz_ix_gist ORDER BY pt <-> point(500, 500), pk LIMIT 5;"),
        ],
    ));
    v.push(raw("DROP TABLE fz_ix_gist;"));
    // Exclusion constraint (check_exclusion_constraint via a GiST index):
    // disjoint boxes insert cleanly, an overlapping box raises the
    // conflict error identically on both engines (a compared arm).
    v.extend(raws(&[
        "CREATE TABLE fz_ix_gx (pk int4 PRIMARY KEY, bx box, EXCLUDE USING gist (bx WITH &&));",
        "INSERT INTO fz_ix_gx SELECT i, box(point(i * 100, 0), point(i * 100 + 40, 40)) FROM generate_series(1, 30) i;",
        "INSERT INTO fz_ix_gx VALUES (9001, box(point(120, 10), point(160, 30)));",
        "SELECT count(*)::int8 FROM fz_ix_gx;",
        "DROP TABLE fz_ix_gx;",
    ]));
    v
}

// ------------------------------------------------------------ SP-GiST ------

/// SP-GiST doPickSplit / spgdoinsert / inner-consistent drain over
/// quadtree + kd point, box quad, inet, and radix text (long shared
/// prefixes) opclasses at page-splitting volume.
fn gen_spgist(g: &mut Gen) -> Vec<StmtKind> {
    let rows = 6000 + 3000 * g.rng.below_usize(3) as i64; // 6k..12k
    let mut v = vec![
        // ptk mirrors pt so the kd index is the only point index on its
        // own column -> the planner must use spg_kd_inner_consistent.
        raw("CREATE TABLE fz_ix_spg (pk int4 PRIMARY KEY, pt point, ptk point, bx box, \
             addr inet, txt text) WITH (autovacuum_enabled = off);"),
        raw(format!(
            "INSERT INTO fz_ix_spg SELECT i, \
             point((i * 7) % 500, (i * 13) % 500), \
             point((i * 7) % 500, (i * 13) % 500), \
             box(point((i * 3) % 400, (i * 5) % 400), point((i * 3) % 400 + 15, (i * 5) % 400 + 10)), \
             ('10.' || (i % 200) || '.' || ((i * 3) % 250) || '.7/32')::inet, \
             'shared_common_prefix_' || lpad((i % 3000)::text, 6, '0') \
             FROM generate_series(1, {rows}) i;"
        )),
        raw("CREATE INDEX fz_ix_spg_pt ON fz_ix_spg USING spgist (pt);"),
        raw("CREATE INDEX fz_ix_spg_ptk ON fz_ix_spg USING spgist (ptk kd_point_ops);"),
        raw("CREATE INDEX fz_ix_spg_bx ON fz_ix_spg USING spgist (bx);"),
        raw("CREATE INDEX fz_ix_spg_addr ON fz_ix_spg USING spgist (addr);"),
        raw("CREATE INDEX fz_ix_spg_txt ON fz_ix_spg USING spgist (txt);"),
        raw("ANALYZE fz_ix_spg;"),
        // Churn: non-HOT churn on indexed columns feeds spgdoinsert with
        // fresh splits and later a redirect/dead scan under VACUUM.
        raw(format!(
            "INSERT INTO fz_ix_spg SELECT {rows} + i, \
             point((i * 11) % 500, (i * 17) % 500), \
             point((i * 11) % 500, (i * 17) % 500), \
             box(point((i * 7) % 400, (i * 9) % 400), point((i * 7) % 400 + 15, (i * 9) % 400 + 10)), \
             ('10.' || (i % 200) || '.' || ((i * 5) % 250) || '.9/32')::inet, \
             'shared_common_prefix_' || lpad((i % 3000)::text, 6, '0') \
             FROM generate_series(1, 2000) i;"
        )),
    ];
    let pool: &[&str] = &[
        // quadtree point (default opclass).
        "SELECT count(*)::int8 FROM fz_ix_spg WHERE pt <@ box(point(100, 100), point(300, 300));",
        "SELECT pk FROM fz_ix_spg WHERE pt <@ box(point(0, 0), point(250, 250)) ORDER BY pk LIMIT 10;",
        "SELECT pk FROM fz_ix_spg ORDER BY pt <-> point(250, 250), pk LIMIT 8;",
        // kd point (forced by ptk being the only index on that column).
        "SELECT count(*)::int8 FROM fz_ix_spg WHERE ptk <@ box(point(100, 100), point(300, 300));",
        "SELECT pk FROM fz_ix_spg ORDER BY ptk <-> point(300, 300), pk LIMIT 8;",
        // box quad.
        "SELECT count(*)::int8 FROM fz_ix_spg WHERE bx <@ box(point(0, 0), point(200, 200));",
        "SELECT count(*)::int8 FROM fz_ix_spg WHERE bx && box(point(100, 100), point(160, 150));",
        "SELECT count(*)::int8 FROM fz_ix_spg WHERE bx @> point(120, 110);",
        // inet.
        "SELECT count(*)::int8 FROM fz_ix_spg WHERE addr <<= inet '10.55.0.0/16';",
        "SELECT count(*)::int8 FROM fz_ix_spg WHERE addr && inet '10.100.0.0/16';",
        // radix text: exact, prefix (^@), and range inner-consistent.
        "SELECT count(*)::int8 FROM fz_ix_spg WHERE txt = 'shared_common_prefix_000123';",
        "SELECT count(*)::int8 FROM fz_ix_spg WHERE txt ^@ 'shared_common_prefix_0001';",
        "SELECT count(*)::int8 FROM fz_ix_spg WHERE txt < 'shared_common_prefix_000500';",
        "SELECT count(*)::int8 FROM fz_ix_spg WHERE txt >= 'shared_common_prefix_002900';",
        "SELECT pk FROM fz_ix_spg WHERE txt ^@ 'shared_common_prefix_0025' ORDER BY pk LIMIT 10;",
    ];
    v.extend(bracket(SEQOFF, sample(g, pool, 11)));
    // Delete + VACUUM: spgbulkdelete / spgvacuumscan (redirect + dead
    // tuple reclaim), then probe the pruned trees.
    v.extend(raws(&[
        "DELETE FROM fz_ix_spg WHERE pk % 4 = 0;",
        "VACUUM fz_ix_spg;",
    ]));
    v.extend(bracket(
        SEQOFF,
        vec![
            raw("SELECT count(*)::int8 FROM fz_ix_spg WHERE txt ^@ 'shared_common_prefix_0001';"),
            raw("SELECT count(*)::int8 FROM fz_ix_spg WHERE pt <@ box(point(100, 100), point(300, 300));"),
        ],
    ));
    v.push(raw("DROP TABLE fz_ix_spg;"));
    v
}

// ------------------------------------------------ partial / expr / cover ---

/// Partial (predicate match + nonmatch), expression, INCLUDE (covering
/// index-only), multicolumn and non-default operator-class btree indexes.
fn gen_cover(g: &mut Gen) -> Vec<StmtKind> {
    let rows = 4000 + 2000 * g.rng.below_usize(3) as i64; // 4k..8k
    let mut v = vec![
        raw("CREATE TABLE fz_ix_cov (pk int4 PRIMARY KEY, a int4, b int4, c text, d int4) \
             WITH (autovacuum_enabled = off);"),
        raw(format!(
            "INSERT INTO fz_ix_cov SELECT i, (i * 7) % 1000, (i * 13) % 1000, \
             'c' || lpad(((i * 3) % 900)::text, 4, '0'), i % 5 \
             FROM generate_series(1, {rows}) i;"
        )),
        // Partial indexes: predicate on d, predicate on b.
        raw("CREATE INDEX fz_ix_cov_pa ON fz_ix_cov (a) WHERE d = 0;"),
        raw("CREATE INDEX fz_ix_cov_pb ON fz_ix_cov (b) WHERE b > 500;"),
        // Expression indexes.
        raw("CREATE INDEX fz_ix_cov_ex ON fz_ix_cov ((a % 97), (lower(c)));"),
        // INCLUDE covering index -> index-only scan when all-visible.
        raw("CREATE INDEX fz_ix_cov_inc ON fz_ix_cov (a) INCLUDE (b, c);"),
        // Multicolumn.
        raw("CREATE INDEX fz_ix_cov_mc ON fz_ix_cov (d, a, b);"),
        // Non-default operator class: LIKE-prefix under C locale.
        raw("CREATE INDEX fz_ix_cov_tp ON fz_ix_cov (c text_pattern_ops);"),
        // All-visible pages so INCLUDE index-only scans stay index-only.
        raw("VACUUM (ANALYZE) fz_ix_cov;"),
    ];
    // Partial predicate match / nonmatch + expression + non-default opclass
    // probes under seqscan-off (planner picks the partial/expr index when
    // usable, another index otherwise).
    let seq_pool: &[&str] = &[
        // Partial-match: predicate satisfied -> partial index usable.
        "SELECT count(*)::int8 FROM fz_ix_cov WHERE d = 0 AND a > 100;",
        "SELECT count(*)::int8 FROM fz_ix_cov WHERE d = 0 AND a BETWEEN 200 AND 400;",
        // Partial-nonmatch: predicate not implied -> partial index unusable.
        "SELECT count(*)::int8 FROM fz_ix_cov WHERE d = 1 AND a > 100;",
        "SELECT count(*)::int8 FROM fz_ix_cov WHERE b > 700;",
        "SELECT count(*)::int8 FROM fz_ix_cov WHERE b > 100 AND b < 400;",
        // Expression index matches.
        "SELECT count(*)::int8 FROM fz_ix_cov WHERE a % 97 = 3;",
        "SELECT count(*)::int8 FROM fz_ix_cov WHERE lower(c) = 'c0123';",
        // Multicolumn leading-then-range.
        "SELECT count(*)::int8 FROM fz_ix_cov WHERE d = 2 AND a > 500;",
        // text_pattern_ops LIKE-prefix.
        "SELECT count(*)::int8 FROM fz_ix_cov WHERE c LIKE 'c012%';",
        "SELECT count(*)::int8 FROM fz_ix_cov WHERE c LIKE 'c0500%';",
        "SELECT pk FROM fz_ix_cov WHERE c LIKE 'c001%' ORDER BY pk LIMIT 10;",
        "SELECT pk FROM fz_ix_cov WHERE d = 0 AND a > 800 ORDER BY pk LIMIT 10;",
    ];
    v.extend(bracket(SEQOFF, sample(g, seq_pool, 9)));
    // Covering INCLUDE index-only scans (seqscan + bitmap off).
    v.extend(bracket(
        IDXONLY,
        vec![
            raw("SELECT count(*)::int8, sum(b)::int8 FROM fz_ix_cov WHERE a > 800;"),
            raw("SELECT a, b FROM fz_ix_cov WHERE a BETWEEN 100 AND 130 ORDER BY a, b, pk LIMIT 12;"),
            raw("SELECT count(*)::int8 FROM fz_ix_cov WHERE a < 50;"),
        ],
    ));
    // Churn + REINDEX over the expression / partial indexes, then reprobe.
    v.extend(raws(&[
        "UPDATE fz_ix_cov SET d = (d + 1) % 5 WHERE pk % 6 = 0;",
        "DELETE FROM fz_ix_cov WHERE pk % 11 = 0;",
        "REINDEX INDEX fz_ix_cov_ex;",
        "VACUUM (ANALYZE) fz_ix_cov;",
    ]));
    v.extend(bracket(
        SEQOFF,
        vec![raw("SELECT count(*)::int8 FROM fz_ix_cov WHERE a % 97 = 3;")],
    ));
    v.push(raw("DROP TABLE fz_ix_cov;"));
    v
}

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
            let stmts = gen_indexam_module(&mut g);
            out.push(stmts.iter().map(|s| s.to_sql()).collect());
        }
        out
    }

    #[test]
    fn groups_are_set_reset_balanced_and_self_contained() {
        // Every SET has its RESET in the same group; no explicit
        // transactions here; every CREATE TABLE has an in-group DROP.
        for group in gen_groups(41, 300) {
            let mut sets: Vec<String> = Vec::new();
            let joined = group.join("\n");
            for sql in &group {
                assert_ne!(sql, "BEGIN;", "unexpected explicit transaction");
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
                if let Some(rest) = sql.strip_prefix("CREATE TABLE ") {
                    let t = rest.split_whitespace().next().unwrap();
                    assert!(
                        joined.contains(&format!("DROP TABLE {t};")),
                        "no in-group DROP for {t} in {group:?}"
                    );
                }
            }
            assert!(sets.is_empty(), "unRESET SETs {sets:?} in {group:?}");
        }
    }

    #[test]
    fn row_returning_statements_are_totally_ordered() {
        // Row-returning SELECTs carry a total ORDER BY; single-row
        // aggregate projections and scalar function probes are exempt.
        for group in gen_groups(17, 300) {
            for sql in &group {
                if !sql.starts_with("SELECT ") {
                    continue;
                }
                let single_row = sql.starts_with("SELECT count(")
                    || sql.starts_with("SELECT gin_clean_pending_list(")
                    || !sql.contains(" FROM ");
                if !single_row {
                    assert!(
                        sql.contains("ORDER BY"),
                        "row-returning statement without total order: {sql}"
                    );
                }
            }
        }
    }

    #[test]
    fn no_float_surfaces() {
        // B1: exact-typed data and aggregates only. KNN distance (<->)
        // appears solely as an ORDER BY key with a pk tiebreak, never a
        // projected column.
        for group in gen_groups(23, 200) {
            for sql in &group {
                assert!(
                    !sql.contains("float8") && !sql.contains("random("),
                    "float surface in {sql}"
                );
                if sql.contains("<->") {
                    assert!(
                        sql.contains("ORDER BY") && sql.contains(", pk "),
                        "KNN distance must be an ordered, pk-tiebroken key: {sql}"
                    );
                }
            }
        }
    }

    #[test]
    fn stream_is_seed_deterministic() {
        assert_eq!(gen_groups(99, 80), gen_groups(99, 80));
    }

    #[test]
    fn all_shapes_reachable() {
        let mut seen = std::collections::BTreeSet::new();
        let cat = FixtureCatalog.load_catalog().unwrap();
        let weights = WeightTable::defaults();
        let mut rng = Rng::new(3);
        let mut productions = Vec::new();
        let mut g = Gen::new(&mut rng, &cat, &weights, &mut productions, 4);
        for _ in 0..400 {
            gen_indexam_module(&mut g);
        }
        for p in productions.iter() {
            if let Some(s) = p.strip_prefix("indexam:shape:") {
                seen.insert(s.to_string());
            }
        }
        assert_eq!(seen.len(), SHAPES.len(), "unreached shapes: {seen:?}");
    }
}
