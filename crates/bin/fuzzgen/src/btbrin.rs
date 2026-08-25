//! Btree + BRIN ALT-PATH drain (W5-BTBRIN; line-drain queue chunks
//! `index-btree` — 113 fns / 1,084 hollow lines — and the BRIN half of
//! `index-brin-hash` — 35 fns / 410 hollow lines — after report-003; see
//! docs/fuzzing/line-drain-queue.md). The idx module (I1/B2) proved the
//! index AMs exist and reached first-order behavior; the residue is the
//! ALT-PATH mass that only fires under data-size/GUC forcing:
//!
//!   btree: nbtpreprocesskeys.c redundancy/contradiction/cross-type
//!   scankey merging, row-compare and skip-array (PG18) arms, SAOP
//!   binary search + array advancement, backward/parallel/mark-restore
//!   scans, unique-check arms (dead duplicates, speculative insertion,
//!   deferred-constraint UNIQUE_CHECK_EXISTING recheck), dedup passes +
//!   split strategies (rightmost fastpath / leftmost / interior, suffix
//!   truncation), multi-level page deletion (`_bt_pagedel` /
//!   `_bt_unlink_halfdead_page` internal-page unlink) via low-fillfactor
//!   indexes + range DELETE + double VACUUM, parallel + spool2-merge
//!   index builds (`_bt_load` merge arm needs a unique build over dead
//!   tuples), and `btadjustmembers` via ALTER OPERATOR FAMILY.
//!
//!   BRIN: `brin_doupdate` samepage vs move update (summaries that GROW
//!   — text minmax under pages_per_range=1 with md5-fueled widening),
//!   `brin_getinsertbuffer` / revmap extension, inclusion-opclass
//!   consistent strategies (box directional ops, inet incl. the
//!   IPv4+IPv6 unmergeable arm, int4range incl. empty ranges),
//!   minmax_multi build/add_value/consistent (never-entered in 003),
//!   summarize/desummarize maintenance incl. error arms, BRIN vacuum
//!   (brin_page_cleanup), and the oversized-index-row ereport arm.
//!
//! Correctness bar (LD7/opt2 law): deterministic query results must be
//! identical across every forced plan and across both engines; any
//! divergence is a HIGH-severity index/executor finding. Plans are not
//! compared here — coverage comes from planning + executing real
//! statements, and errors (unique violations, summarize misuse,
//! oversized rows) are themselves compared arms.
//!
//! Discipline (spill/opt2 rules): self-contained groups over fixed
//! `fz_bb_*` fixtures created and dropped in-group; every SET has its
//! RESET in reverse order in the same group; explicit transactions are
//! closed in-group (ROLLBACK, or COMMIT for the deferred-unique arm
//! whose COMMIT is itself the probe); row-returning statements carry a
//! total ORDER BY or are single-row aggregates; exact-typed aggregates
//! only (no float surfaces, B1); every data formula is a pure integer
//! or md5-of-integer function of generate_series, so both engines load
//! byte-identical data; ANALYZE only on tables small enough for an
//! exhaustive statistics sample. VACUUM/CREATE INDEX run outside
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

const IDXONLY: &[(&str, &str)] = &[("enable_seqscan", "off"), ("enable_bitmapscan", "off")];
const BITMAP: &[(&str, &str)] = &[
    ("enable_seqscan", "off"),
    ("enable_indexscan", "off"),
    ("enable_indexonlyscan", "off"),
];

const SHAPES: &[&str] = &[
    "btbrin:keys",
    "btbrin:uniq",
    "btbrin:scan",
    "btbrin:split",
    "btbrin:pagedel",
    "btbrin:build",
    "btbrin:brinup",
    "btbrin:brininc",
    "btbrin:brinmm",
    "btbrin:brinmaint",
    "btbrin:opfam",
];

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_btbrin_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("btbrin");
    let shape = g.weights.pick(g.rng, SHAPES);
    g.fire2("btbrin:shape:", &shape["btbrin:".len()..]);
    match shape {
        "btbrin:keys" => gen_keys(g),
        "btbrin:uniq" => gen_uniq(g),
        "btbrin:scan" => gen_scan(g),
        "btbrin:split" => gen_split(g),
        "btbrin:pagedel" => gen_pagedel(g),
        "btbrin:build" => gen_build(g),
        "btbrin:brinup" => gen_brinup(g),
        "btbrin:brininc" => gen_brininc(g),
        "btbrin:brinmm" => gen_brinmm(g),
        "btbrin:brinmaint" => gen_brinmaint(g),
        _ => gen_opfam(g),
    }
}

// ------------------------------------------------ scankey preprocessing ---

/// nbtpreprocesskeys.c drain: redundant / contradictory / cross-type
/// scankeys, IS NULL interplay, row compares, SAOPs, skip arrays.
fn gen_keys(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        // b DESC NULLS LAST flips both direction flags off the default;
        // a plain ASC, c DESC (NULLS FIRST default for DESC) covers the
        // SK_BT_DESC / SK_BT_NULLS_FIRST matrix across columns.
        "CREATE TABLE fz_bb_k (pk int4 PRIMARY KEY, a int4, b int4, c text, d int4);",
        "INSERT INTO fz_bb_k SELECT i, (i * 7) % 200, \
         CASE WHEN i % 11 = 0 THEN NULL ELSE (i * 13) % 350 END, \
         'k' || lpad(((i * 3) % 500)::text, 4, '0'), i % 6 \
         FROM generate_series(1, 3000) i;",
        "CREATE INDEX fz_bb_k_ab ON fz_bb_k (a, b DESC NULLS LAST);",
        "CREATE INDEX fz_bb_k_c ON fz_bb_k (c DESC);",
        "CREATE INDEX fz_bb_k_da ON fz_bb_k (d, a);",
        "ANALYZE fz_bb_k;",
    ]);
    // Pool of forced-index probes; sample 10 per group so seeds spread
    // over the pool while each group stays bounded.
    let pool: &[&str] = &[
        // Redundant same-direction inequalities collapse to the tighter.
        "SELECT count(*)::int8 FROM fz_bb_k WHERE a > 50 AND a > 80 AND a >= 81;",
        "SELECT count(*)::int8 FROM fz_bb_k WHERE a < 120 AND a <= 119 AND a < 180;",
        // Equality vs inequality redundancy and contradiction.
        "SELECT count(*)::int8 FROM fz_bb_k WHERE a = 60 AND a > 10 AND a < 199;",
        "SELECT count(*)::int8 FROM fz_bb_k WHERE a = 60 AND a > 99;",
        "SELECT count(*)::int8 FROM fz_bb_k WHERE a > 100 AND a < 50;",
        "SELECT count(*)::int8 FROM fz_bb_k WHERE a BETWEEN 90 AND 40;",
        // Cross-type comparisons (int2/int8 vs the int4 opclass): the
        // redundancy proof goes through get_opfamily_member lookups.
        "SELECT count(*)::int8 FROM fz_bb_k WHERE a > 5::int2 AND a > 9::int8;",
        "SELECT count(*)::int8 FROM fz_bb_k WHERE a = 66::int8 AND a >= 2::int2;",
        "SELECT count(*)::int8 FROM fz_bb_k WHERE a <= 150::int8 AND a <= 90::int2 AND a >= 8::int8;",
        // IS NULL / IS NOT NULL interplay on the DESC NULLS LAST column.
        "SELECT count(*)::int8 FROM fz_bb_k WHERE a = 7 AND b IS NULL;",
        "SELECT count(*)::int8 FROM fz_bb_k WHERE a = 7 AND b IS NULL AND b > 10;",
        "SELECT count(*)::int8 FROM fz_bb_k WHERE a = 7 AND b IS NOT NULL AND b > 10;",
        "SELECT count(*)::int8 FROM fz_bb_k WHERE b IS NULL AND b IS NOT NULL;",
        // Row comparisons, forward and backward, with NULL members.
        "SELECT count(*)::int8 FROM fz_bb_k WHERE (a, b) > (100, 200);",
        "SELECT count(*)::int8 FROM fz_bb_k WHERE (a, b) >= (100, 200) AND a < 150;",
        "SELECT count(*)::int8 FROM fz_bb_k WHERE (a, b) < (50, 40);",
        "SELECT count(*)::int8 FROM fz_bb_k WHERE (a, b) <= (50, NULL);",
        "SELECT count(*)::int8 FROM fz_bb_k WHERE (a, b) > (NULL, 3);",
        "SELECT pk FROM fz_bb_k WHERE (a, b) > (190, 100) ORDER BY a DESC, b, pk LIMIT 10;",
        "SELECT count(*)::int8 FROM fz_bb_k WHERE (d, a) > (2, 100) AND (d, a) < (5, 20);",
        // SAOPs: NULL elements, duplicates, unsorted input, empties,
        // non-equality strategies (_bt_find_extreme_element).
        "SELECT count(*)::int8 FROM fz_bb_k WHERE a = ANY ('{7,3,199,3,NULL,42}'::int4[]);",
        "SELECT count(*)::int8 FROM fz_bb_k WHERE a = ANY ('{}'::int4[]);",
        "SELECT count(*)::int8 FROM fz_bb_k WHERE a = ANY ('{NULL,NULL}'::int4[]);",
        "SELECT count(*)::int8 FROM fz_bb_k WHERE a > ANY ('{180,20,150}'::int4[]);",
        "SELECT count(*)::int8 FROM fz_bb_k WHERE a < ALL ('{40,90,60}'::int4[]);",
        "SELECT count(*)::int8 FROM fz_bb_k WHERE a = ANY ('{10,20,30}') AND a > 15;",
        "SELECT count(*)::int8 FROM fz_bb_k WHERE a = ANY ('{10,20,30}') AND a = ANY ('{20,30,40}');",
        "SELECT count(*)::int8 FROM fz_bb_k WHERE b = ANY ('{5,105,205,305}') AND a = 7;",
        // SAOP driving a backward-ordered scan (required-direction arms).
        "SELECT pk FROM fz_bb_k WHERE a = ANY ('{30,60,90}') ORDER BY a DESC, b, pk LIMIT 12;",
        // Skip scan (PG18): qual on the low-order column only.
        "SELECT count(*)::int8 FROM fz_bb_k WHERE b = 105;",
        "SELECT count(*)::int8 FROM fz_bb_k WHERE b > 340;",
        "SELECT count(*)::int8 FROM fz_bb_k WHERE b = ANY ('{5,205}'::int4[]);",
        // Skip array + IS NOT NULL (redundancy arm in
        // _bt_compare_scankey_args' SK_BT_SKIP branch).
        "SELECT count(*)::int8 FROM fz_bb_k WHERE b = 44 AND a IS NOT NULL;",
        "SELECT count(*)::int8 FROM fz_bb_k WHERE a IS NOT NULL AND b IS NULL;",
        // DESC text column: commuted strategies + pattern bounds.
        "SELECT count(*)::int8 FROM fz_bb_k WHERE c > 'k0100' AND c > 'k0200' AND c < 'k0400';",
        "SELECT count(*)::int8 FROM fz_bb_k WHERE c = 'k0123' AND c >= 'k0100';",
        "SELECT pk FROM fz_bb_k WHERE c < 'k0050' ORDER BY c, pk LIMIT 9;",
    ];
    let mut idx: Vec<usize> = (0..pool.len()).collect();
    for i in 0..10 {
        let j = i + g.rng.below_usize(idx.len() - i);
        idx.swap(i, j);
    }
    let picked: Vec<StmtKind> = idx.iter().take(10).map(|&i| raw(pool[i])).collect();
    v.extend(bracket(IDXONLY, picked));
    v.push(raw("DROP TABLE fz_bb_k;"));
    v
}

// ---------------------------------------------------------- unique-check ---

/// _bt_check_unique arms: live conflict ereport, dead duplicates
/// (ItemIdMarkDead), speculative insertion (ON CONFLICT), and the
/// deferred-constraint UNIQUE_CHECK_EXISTING commit-time recheck.
fn gen_uniq(g: &mut Gen) -> Vec<StmtKind> {
    let cycles = 120 + 40 * g.rng.below_usize(4) as i64; // 120..240
    let mut v = raws(&[
        "CREATE TABLE fz_bb_u (pk int4 PRIMARY KEY, v int4, w int4);",
        "CREATE UNIQUE INDEX fz_bb_u_v ON fz_bb_u (v);",
        "INSERT INTO fz_bb_u SELECT i, i * 2, i % 17 FROM generate_series(1, 2000) i;",
        "ANALYZE fz_bb_u;",
        // Live conflict: definite-duplicate ereport arm (key detail text).
        "INSERT INTO fz_bb_u VALUES (900001, 400, 0);",
        // Speculative insertion: UNIQUE_CHECK_PARTIAL both outcomes.
        "INSERT INTO fz_bb_u VALUES (900002, 500, 0) ON CONFLICT (v) DO NOTHING;",
        "INSERT INTO fz_bb_u VALUES (900003, 600, 0) ON CONFLICT (v) DO UPDATE SET w = 99;",
        "INSERT INTO fz_bb_u VALUES (900004, 4000001, 0) ON CONFLICT (v) DO NOTHING;",
        // Dead duplicate: delete-commit then reinsert the same key; the
        // fetch sees an all-dead HOT chain and marks the entry killed.
        "DELETE FROM fz_bb_u WHERE v = 700;",
        "INSERT INTO fz_bb_u VALUES (900005, 700, 1);",
    ]);
    // Pile many dead versions of a small key band onto the same leaf
    // pages, then reinsert: _bt_check_unique has to walk dead entries
    // (and possibly cross a page boundary) before concluding uniqueness;
    // _bt_findinsertloc's duplicate handling sees the garbage flags.
    v.push(raw(format!(
        "DO $$ BEGIN FOR i IN 1..{cycles} LOOP \
         DELETE FROM fz_bb_u WHERE v = 802; \
         INSERT INTO fz_bb_u VALUES (910000 + i, 802, i); \
         END LOOP; END $$;"
    )));
    v.push(raw(
        "DO $$ BEGIN FOR i IN 1..80 LOOP \
         UPDATE fz_bb_u SET v = -v WHERE v = 804; \
         UPDATE fz_bb_u SET v = -v WHERE v = -804; \
         END LOOP; END $$;",
    ));
    v.extend(raws(&[
        // Rollback-superseded insert then reinsert: dead never-committed
        // duplicate in the index.
        "BEGIN;",
        "INSERT INTO fz_bb_u VALUES (920001, 5001, 0);",
        "ROLLBACK;",
        "INSERT INTO fz_bb_u VALUES (920002, 5001, 0);",
        // Deferred unique: COMMIT itself is the probe — the recheck path
        // is UNIQUE_CHECK_EXISTING (and the COMMIT-time error is a
        // compared arm).
        "CREATE TABLE fz_bb_ud (pk int4 PRIMARY KEY, v int4, \
         CONSTRAINT fz_bb_ud_v UNIQUE (v) DEFERRABLE INITIALLY IMMEDIATE);",
        "INSERT INTO fz_bb_ud SELECT i, i FROM generate_series(1, 200) i;",
        "BEGIN;",
        "SET CONSTRAINTS fz_bb_ud_v DEFERRED;",
        "INSERT INTO fz_bb_ud VALUES (9001, 50);",
        "COMMIT;",
        // Deferred but resolved before commit: recheck finds no dup.
        "BEGIN;",
        "SET CONSTRAINTS fz_bb_ud_v DEFERRED;",
        "INSERT INTO fz_bb_ud VALUES (9002, 60);",
        "DELETE FROM fz_bb_ud WHERE pk = 60;",
        "COMMIT;",
        "SELECT count(*)::int8, sum(v)::int8 FROM fz_bb_ud;",
        "SELECT count(*)::int8, sum(w)::int8 FROM fz_bb_u;",
        "DROP TABLE fz_bb_u, fz_bb_ud;",
    ]));
    v
}

// ----------------------------------------------------------------- scans ---

/// _bt_first / _bt_readpage / backward + parallel scans, mark/restore
/// (btrestrpos), endpoint scans, index-only scans.
fn gen_scan(g: &mut Gen) -> Vec<StmtKind> {
    let rows = 12000 + 4000 * g.rng.below_usize(3) as i64; // 12k..20k
    let mut v = vec![
        raw("CREATE TABLE fz_bb_s (pk int4 PRIMARY KEY, a int4, b int4, t text);"),
        raw(format!(
            "INSERT INTO fz_bb_s SELECT i, (i * 17) % 1000, (i * 5) % 97, \
             't' || ((i * 11) % 500) FROM generate_series(1, {rows}) i;"
        )),
        raw("CREATE INDEX fz_bb_s_ab ON fz_bb_s (a, b);"),
        raw("CREATE INDEX fz_bb_s_t ON fz_bb_s (t);"),
        // All-visible pages so index-only scans stay index-only.
        raw("VACUUM (ANALYZE) fz_bb_s;"),
    ];
    v.extend(bracket(
        IDXONLY,
        vec![
            // Endpoint scans (no keys): both directions.
            raw("SELECT a, b FROM fz_bb_s ORDER BY a, b LIMIT 5;"),
            raw("SELECT a, b FROM fz_bb_s ORDER BY a DESC, b DESC LIMIT 5;"),
            // Backward scan with a start key (>= under DESC ordering).
            raw("SELECT a, b FROM fz_bb_s WHERE a >= 990 ORDER BY a DESC, b DESC LIMIT 20;"),
            raw("SELECT a, b FROM fz_bb_s WHERE a < 10 ORDER BY a DESC, b DESC LIMIT 20;"),
            raw("SELECT count(*)::int8 FROM fz_bb_s WHERE a > 400 AND a <= 460 AND b > 50;"),
            // Scroll cursor: forward/backward/absolute over a real scan.
            raw("BEGIN;"),
            raw(
                "DECLARE fz_bb_cur SCROLL CURSOR FOR \
                 SELECT a, b, pk FROM fz_bb_s WHERE a BETWEEN 100 AND 300 ORDER BY a, b, pk;",
            ),
            raw("FETCH FORWARD 120 FROM fz_bb_cur;"),
            raw("FETCH BACKWARD 60 FROM fz_bb_cur;"),
            raw("FETCH ABSOLUTE 200 FROM fz_bb_cur;"),
            raw("FETCH PRIOR FROM fz_bb_cur;"),
            raw("FETCH FIRST FROM fz_bb_cur;"),
            raw("FETCH LAST FROM fz_bb_cur;"),
            raw("FETCH BACKWARD 500 FROM fz_bb_cur;"),
            raw("CLOSE fz_bb_cur;"),
            raw("ROLLBACK;"),
        ],
    ));
    // Merge join with mark/restore on the inner index scan (btrestrpos):
    // duplicate-rich join keys force repeated restores.
    v.extend(bracket(
        &[
            ("enable_hashjoin", "off"),
            ("enable_nestloop", "off"),
            ("enable_material", "off"),
            ("enable_seqscan", "off"),
        ],
        vec![raw(
            "SELECT count(*)::int8, sum(x.pk)::int8 FROM fz_bb_s x \
             JOIN fz_bb_s y ON x.b = y.b WHERE x.pk <= 300 AND y.pk <= 3000;",
        )],
    ));
    // Parallel btree scan: _bt_parallel_seize/release + estimate arms.
    v.extend(bracket(
        &[
            ("enable_seqscan", "off"),
            ("enable_bitmapscan", "off"),
            ("parallel_setup_cost", "0"),
            ("parallel_tuple_cost", "0"),
            ("min_parallel_index_scan_size", "0"),
            ("min_parallel_table_scan_size", "0"),
            ("max_parallel_workers_per_gather", "2"),
        ],
        vec![
            raw("SELECT count(*)::int8, sum(b)::int8 FROM fz_bb_s WHERE a > 5;"),
            raw("SELECT count(*)::int8 FROM fz_bb_s WHERE a = ANY ('{10,200,400,600,800}');"),
        ],
    ));
    v.push(raw("DROP TABLE fz_bb_s;"));
    v
}

// ------------------------------------------------------- dedup and split ---

/// nbtdedup.c / nbtsplitloc.c drain: posting lists, dedup passes under
/// churn, split strategies under ascending / descending / interior
/// insertion patterns, suffix truncation over long shared prefixes.
fn gen_split(g: &mut Gen) -> Vec<StmtKind> {
    let dup_rows = 9000 + 3000 * g.rng.below_usize(3) as i64; // 9k..15k
    let mut v = vec![
        // Duplicate-heavy: 40 distinct keys -> deep posting lists; the
        // 'many' fillfactor keeps leaves splitting during dedup churn.
        raw("CREATE TABLE fz_bb_d (pk int4 PRIMARY KEY, k int4, t text) \
             WITH (autovacuum_enabled = off);"),
        raw(format!(
            "INSERT INTO fz_bb_d SELECT i, i % 40, \
             'pfx_common_prefix_shared_' || lpad((i % 900)::text, 6, '0') \
             FROM generate_series(1, {dup_rows}) i;"
        )),
        raw("CREATE INDEX fz_bb_d_k ON fz_bb_d (k) WITH (deduplicate_items = on);"),
        raw("CREATE INDEX fz_bb_d_koff ON fz_bb_d (k, pk) WITH (deduplicate_items = off);"),
        // Long-shared-prefix text keys: suffix truncation decides split
        // points; DESC build variant flips the comparisons.
        raw("CREATE INDEX fz_bb_d_t ON fz_bb_d (t);"),
        raw("ANALYZE fz_bb_d;"),
        // Churn: deletes + updates leave dead posting-list TIDs, the
        // reinsert wave forces dedup passes and interior splits.
        raw("DELETE FROM fz_bb_d WHERE pk % 7 = 0;"),
        raw("UPDATE fz_bb_d SET k = k + 40 WHERE pk % 5 = 0;"),
        raw(format!(
            "INSERT INTO fz_bb_d SELECT {dup_rows} + i, i % 40, \
             'pfx_common_prefix_shared_' || lpad((i % 900)::text, 6, '0') \
             FROM generate_series(1, 4000) i;"
        )),
        // Ascending appends: rightmost-split fastpath.
        raw(format!(
            "INSERT INTO fz_bb_d SELECT {dup_rows} + 4000 + i, 100000 + i, 'zz' || i \
             FROM generate_series(1, 3000) i;"
        )),
        // Descending inserts: leftmost splits.
        raw(format!(
            "INSERT INTO fz_bb_d SELECT {dup_rows} + 7000 + i, -i, 'aa' || i \
             FROM generate_series(1, 3000) i;"
        )),
    ];
    v.extend(bracket(
        IDXONLY,
        vec![
            raw("SELECT count(*)::int8, sum(pk)::int8 FROM fz_bb_d WHERE k = 13;"),
            raw("SELECT count(*)::int8 FROM fz_bb_d WHERE k > 70;"),
            raw(
                "SELECT count(*)::int8 FROM fz_bb_d \
                 WHERE t > 'pfx_common_prefix_shared_000500';",
            ),
            raw("SELECT count(*)::int8 FROM fz_bb_d WHERE k < -2500;"),
        ],
    ));
    v.extend(raws(&[
        // Vacuum prunes the dead posting TIDs (btreevacuumposting).
        "VACUUM fz_bb_d;",
        "SELECT count(*)::int8 FROM fz_bb_d;",
        "DROP TABLE fz_bb_d;",
    ]));
    v
}

// -------------------------------------------------------- page deletion ---

/// _bt_pagedel / _bt_mark_page_halfdead / _bt_unlink_halfdead_page
/// (including internal-page unlink) + btvacuumpage recycling arms:
/// fillfactor-10 index over 20k rows -> ~550 leaves / 3 levels; a 95%
/// range DELETE plus double VACUUM cascades deletions up a level and
/// then revisits deleted pages as recyclable.
fn gen_pagedel(g: &mut Gen) -> Vec<StmtKind> {
    // Which band survives decides whether rightmost/leftmost pages die.
    let variant = g.rng.below_usize(3);
    let (lo, hi) = match variant {
        0 => (1000, 20000), // delete the right edge and middle
        1 => (1, 19000),    // delete the left edge and middle
        _ => (4000, 16000), // delete the middle band only
    };
    let mut v = vec![
        raw("CREATE TABLE fz_bb_p (pk int4, filler text) WITH (autovacuum_enabled = off);"),
        raw(
            "INSERT INTO fz_bb_p SELECT i, 'f' || (i % 10) \
             FROM generate_series(1, 20000) i;",
        ),
        raw("CREATE INDEX fz_bb_p_pk ON fz_bb_p (pk) WITH (fillfactor = 10);"),
        raw("ANALYZE fz_bb_p;"),
        raw(format!(
            "DELETE FROM fz_bb_p WHERE pk >= {lo} AND pk < {hi};"
        )),
        // First VACUUM: kills tuples, half-dead marking + unlink walk.
        raw("VACUUM (INDEX_CLEANUP ON) fz_bb_p;"),
        // Second VACUUM: sees fully deleted pages, recycles
        // (btvacuumpage P_ISDELETED / BTPageIsRecyclable arms,
        // _bt_pendingfsm machinery).
        raw("VACUUM fz_bb_p;"),
        // Reuse: inserts into the emptied key space allocate from the
        // freed pages (_bt_allocbuf recycle arm).
        raw(format!(
            "INSERT INTO fz_bb_p SELECT i, 'r' FROM generate_series({lo}, {} ) i;",
            lo + 3000
        )),
    ];
    v.extend(bracket(
        IDXONLY,
        vec![
            raw("SELECT count(*)::int8, sum(pk)::int8 FROM fz_bb_p WHERE pk > 0;"),
            raw("SELECT pk FROM fz_bb_p ORDER BY pk DESC LIMIT 3;"),
            raw("SELECT pk FROM fz_bb_p ORDER BY pk LIMIT 3;"),
        ],
    ));
    v.extend(raws(&["VACUUM fz_bb_p;", "DROP TABLE fz_bb_p;"]));
    v
}

// ------------------------------------------------------------ builds ------

/// nbtsort.c drain: parallel builds (leader on/off), the spool2 merge
/// arm (unique build over dead tuples), DESC / NULLS FIRST builds,
/// INCLUDE columns, expression keys, REINDEX.
fn gen_build(g: &mut Gen) -> Vec<StmtKind> {
    let rows = 8000 + 4000 * g.rng.below_usize(2) as i64; // 8k/12k
    let mut v = vec![
        raw("CREATE TABLE fz_bb_b (pk int4 PRIMARY KEY, a int4, b int4, t text) \
             WITH (autovacuum_enabled = off);"),
        raw(format!(
            "INSERT INTO fz_bb_b SELECT i, (i * 31) % 5000, \
             CASE WHEN i % 13 = 0 THEN NULL ELSE (i * 7) % 400 END, \
             't' || ((i * 3) % 1000) FROM generate_series(1, {rows}) i;"
        )),
        // Dead tuples BEFORE the unique build: HOT-broken updates give
        // CREATE UNIQUE INDEX a nonempty second spool -> _bt_load merge.
        raw("UPDATE fz_bb_b SET a = a + 5000 WHERE pk % 3 = 0;"),
        raw("UPDATE fz_bb_b SET a = a - 5000 WHERE pk % 3 = 0 AND a > 5000;"),
    ];
    // Parallel build bracket: workers + tiny sort memory (multi-run
    // leader merge). parallel_leader_participation off is its own arm.
    let leader_off = g.rng.below_usize(2) == 1;
    let mut gucs: Vec<(&str, &str)> = vec![
        ("min_parallel_table_scan_size", "0"),
        ("max_parallel_maintenance_workers", "2"),
        ("maintenance_work_mem", "'1MB'"),
    ];
    if leader_off {
        gucs.push(("parallel_leader_participation", "off"));
    }
    v.extend(bracket(
        &gucs,
        vec![
            raw("CREATE INDEX fz_bb_b_par ON fz_bb_b (a, t);"),
            // Unique build over the dead-tuple table: spool2 merge; may
            // legitimately error on a residual duplicate — the error is
            // itself a compared arm (deterministic data).
            raw("CREATE UNIQUE INDEX fz_bb_b_upar ON fz_bb_b (pk) WITH (fillfactor = 70);"),
            raw("CREATE INDEX fz_bb_b_desc ON fz_bb_b (a DESC NULLS LAST, b NULLS FIRST);"),
            raw("CREATE INDEX fz_bb_b_inc ON fz_bb_b (b) INCLUDE (t, a);"),
            raw("CREATE INDEX fz_bb_b_expr ON fz_bb_b ((a % 97), (length(t)));"),
            raw("REINDEX INDEX fz_bb_b_par;"),
        ],
    ));
    v.push(raw("ANALYZE fz_bb_b;"));
    v.extend(bracket(
        IDXONLY,
        vec![
            raw("SELECT count(*)::int8, sum(b)::int8 FROM fz_bb_b WHERE a > 2500;"),
            raw("SELECT count(*)::int8 FROM fz_bb_b WHERE a % 97 = 3;"),
            raw("SELECT count(*)::int8 FROM fz_bb_b WHERE b IS NULL;"),
            raw("SELECT a, b FROM fz_bb_b WHERE a > 4990 ORDER BY a DESC NULLS LAST, pk LIMIT 8;"),
        ],
    ));
    v.push(raw("DROP TABLE fz_bb_b;"));
    v
}

// ------------------------------------------------------- BRIN doupdate ----

/// brin_doupdate samepage vs move-update: pages_per_range=1 over a
/// pad-widened heap (~9 rows/page), md5-fueled text minmax summaries
/// that GROW under update/insert churn until regular pages overflow
/// (brin_getinsertbuffer / revmap extension / evacuation feeders), plus
/// the oversized-index-row ereport arm.
fn gen_brinup(g: &mut Gen) -> Vec<StmtKind> {
    let pages = 200 + 50 * g.rng.below_usize(3) as i64; // ~200..300 heap pages
    let rows = pages * 9;
    let mut v = vec![
        raw("CREATE TABLE fz_bb_bu (pk int4 PRIMARY KEY, t text, pad text) \
             WITH (autovacuum_enabled = off, fillfactor = 100);"),
        raw(format!(
            "INSERT INTO fz_bb_bu SELECT i, 'm' || (i % 50), repeat('x', 800) \
             FROM generate_series(1, {rows}) i;"
        )),
        raw("CREATE INDEX fz_bb_bu_brin ON fz_bb_bu USING brin (t) \
             WITH (pages_per_range = 1);"),
        raw("SELECT brin_summarize_new_values('fz_bb_bu_brin')::int8 >= 0;"),
        // Widen many summaries: non-HOT updates (t is indexed) relocate
        // rows and re-summarize target ranges with much longer strings;
        // each grown summary re-lands via brin_doupdate.
        raw("UPDATE fz_bb_bu SET t = md5(pk::text) || md5((pk * 3)::text) \
             WHERE pk % 4 = 0;"),
        raw("UPDATE fz_bb_bu SET t = 'zzz_' || md5(pk::text) || md5((pk * 7)::text) \
             || md5((pk * 11)::text) WHERE pk % 10 = 0;"),
        raw("SELECT brin_summarize_new_values('fz_bb_bu_brin')::int8 >= 0;"),
        // Second churn wave over now-summarized ranges: doupdate under
        // fuller regular pages (move updates + page extension).
        raw("UPDATE fz_bb_bu SET t = 'aaa_' || md5((pk * 13)::text) || md5((pk * 17)::text) \
             WHERE pk % 6 = 1;"),
        raw(format!(
            "INSERT INTO fz_bb_bu SELECT {rows} + i, 'AA_' || md5(i::text), repeat('y', 800) \
             FROM generate_series(1, 200) i;"
        )),
        raw("VACUUM fz_bb_bu;"),
        raw("ANALYZE fz_bb_bu;"),
    ];
    v.extend(bracket(
        BITMAP,
        vec![
            raw("SELECT count(*)::int8 FROM fz_bb_bu WHERE t > 'zzz';"),
            raw("SELECT count(*)::int8 FROM fz_bb_bu WHERE t < 'b';"),
            raw("SELECT count(*)::int8 FROM fz_bb_bu WHERE t = 'm7';"),
        ],
    ));
    v.extend(raws(&[
        "SELECT count(*)::int8 FROM fz_bb_bu;",
        "DROP TABLE fz_bb_bu;",
        // Oversized BRIN index row (program-limit 54000 arm), on an
        // isolated single-page table instead of the churned fz_bb_bu
        // heap. On the churned heap the outcome was placement-dependent:
        // the 1200-chained-md5 value (~38kB hex, beyond compression
        // rescue) only errors when the FSM steers the row into a range
        // whose summary must widen, and post-VACUUM free space is
        // visibility-horizon-sensitive — ANY concurrent session whose
        // snapshot spans the churn->VACUUM window blocks reaping on
        // exactly one engine's timeline and flips its placement, so the
        // arm A/B-diverged under concurrent load with both engines
        // C-parity on identical state (a fuzzing round/9/10 RB-3;
        // engine mechanism fixed by #1546, residual nondeterminism is
        // this fixture's). Here range 0 is the only range, it is
        // summarized ('seed'), and the value must widen its minmax
        // ('c4ca...' < 'seed'), so the identical ereport fires on both
        // engines regardless of any concurrent activity.
        "CREATE TABLE fz_bb_ov (pk int4 PRIMARY KEY, t text) \
         WITH (autovacuum_enabled = off);",
        "INSERT INTO fz_bb_ov VALUES (1, 'seed');",
        "CREATE INDEX fz_bb_ov_brin ON fz_bb_ov USING brin (t) \
         WITH (pages_per_range = 1);",
        "SELECT brin_summarize_new_values('fz_bb_ov_brin')::int8 >= 0;",
        "INSERT INTO fz_bb_ov SELECT 2, string_agg(md5(i::text), '') \
         FROM generate_series(1, 1200) i;",
        "SELECT count(*)::int8 FROM fz_bb_ov;",
        "DROP TABLE fz_bb_ov;",
    ]));
    v
}

// ---------------------------------------------------- BRIN inclusion ------

/// brin_inclusion_* strategies: box directional operators, inet
/// (including the IPv4+IPv6 unmergeable-union arm), int4range with
/// empty ranges, each probed under a forced bitmap scan.
fn gen_brininc(g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_bb_i (pk int4 PRIMARY KEY, bx box, ip inet, r int4range, pad text) \
         WITH (autovacuum_enabled = off);",
        // ~9 rows/page keeps ranges plural; every value a pure integer
        // formula. Rows 1500..1560 mix IPv6 into the same ranges as
        // IPv4 (unmergeable unions); r is empty for every 13th row
        // (contains-empty arms).
        "INSERT INTO fz_bb_i SELECT i, \
         box(point((i * 7) % 500, (i * 13) % 400), point((i * 7) % 500 + 40, (i * 13) % 400 + 30)), \
         CASE WHEN i BETWEEN 1500 AND 1560 THEN ('2001:db8::' || (i % 200))::inet \
              ELSE ('10.' || (i % 200) || '.' || ((i * 3) % 250) || '.7/32')::inet END, \
         CASE WHEN i % 13 = 0 THEN 'empty'::int4range \
              ELSE int4range((i * 5) % 1000, (i * 5) % 1000 + 20 + i % 30) END, \
         repeat('q', 700) FROM generate_series(1, 2700) i;",
        "CREATE INDEX fz_bb_i_bx ON fz_bb_i USING brin (bx box_inclusion_ops) \
         WITH (pages_per_range = 2);",
        "CREATE INDEX fz_bb_i_ip ON fz_bb_i USING brin (ip inet_inclusion_ops) \
         WITH (pages_per_range = 2);",
        "CREATE INDEX fz_bb_i_r ON fz_bb_i USING brin (r range_inclusion_ops) \
         WITH (pages_per_range = 2);",
        "ANALYZE fz_bb_i;",
    ]);
    let pool: &[&str] = &[
        // Box placement strategies (each maps to a converse operator in
        // brin_inclusion_consistent).
        "SELECT count(*)::int8 FROM fz_bb_i WHERE bx << box '((600,0),(650,10))';",
        "SELECT count(*)::int8 FROM fz_bb_i WHERE bx &< box '((300,0),(340,10))';",
        "SELECT count(*)::int8 FROM fz_bb_i WHERE bx >> box '((-50,0),(-10,10))';",
        "SELECT count(*)::int8 FROM fz_bb_i WHERE bx &> box '((200,0),(260,10))';",
        "SELECT count(*)::int8 FROM fz_bb_i WHERE bx <<| box '((0,600),(10,650))';",
        "SELECT count(*)::int8 FROM fz_bb_i WHERE bx &<| box '((0,300),(10,360))';",
        "SELECT count(*)::int8 FROM fz_bb_i WHERE bx |>> box '((0,-60),(10,-10))';",
        "SELECT count(*)::int8 FROM fz_bb_i WHERE bx |&> box '((0,200),(10,240))';",
        "SELECT count(*)::int8 FROM fz_bb_i WHERE bx && box '((100,100),(180,170))';",
        "SELECT count(*)::int8 FROM fz_bb_i WHERE bx @> box '((120,120),(125,125))';",
        "SELECT count(*)::int8 FROM fz_bb_i WHERE bx <@ box '((0,0),(600,500))';",
        "SELECT count(*)::int8 FROM fz_bb_i WHERE bx ~= box '((7,13),(47,43))';",
        "SELECT count(*)::int8 FROM fz_bb_i WHERE bx @> point(130, 130);",
        // Inet: overlap/containment incl. the unmergeable v4/v6 ranges.
        "SELECT count(*)::int8 FROM fz_bb_i WHERE ip << '10.55.0.0/16'::inet;",
        "SELECT count(*)::int8 FROM fz_bb_i WHERE ip <<= '10.100.0.0/16'::inet;",
        "SELECT count(*)::int8 FROM fz_bb_i WHERE ip >>= '10.9.9.7/32'::inet;",
        "SELECT count(*)::int8 FROM fz_bb_i WHERE ip && '2001:db8::/64'::inet;",
        "SELECT count(*)::int8 FROM fz_bb_i WHERE ip = '10.44.132.7'::inet;",
        // Ranges: overlap/containment/adjacency + empties.
        "SELECT count(*)::int8 FROM fz_bb_i WHERE r && int4range(400, 460);",
        "SELECT count(*)::int8 FROM fz_bb_i WHERE r @> 555;",
        "SELECT count(*)::int8 FROM fz_bb_i WHERE r @> 'empty'::int4range;",
        "SELECT count(*)::int8 FROM fz_bb_i WHERE r <@ int4range(0, 2000);",
        "SELECT count(*)::int8 FROM fz_bb_i WHERE r << int4range(1500, 1600);",
        "SELECT count(*)::int8 FROM fz_bb_i WHERE r >> int4range(-100, -50);",
        "SELECT count(*)::int8 FROM fz_bb_i WHERE r &< int4range(0, 700);",
        "SELECT count(*)::int8 FROM fz_bb_i WHERE r &> int4range(300, 400);",
        "SELECT count(*)::int8 FROM fz_bb_i WHERE r -|- int4range(120, 200);",
        "SELECT count(*)::int8 FROM fz_bb_i WHERE r = 'empty'::int4range;",
    ];
    let mut idx: Vec<usize> = (0..pool.len()).collect();
    for i in 0..12 {
        let j = i + g.rng.below_usize(idx.len() - i);
        idx.swap(i, j);
    }
    let picked: Vec<StmtKind> = idx.iter().take(12).map(|&i| raw(pool[i])).collect();
    v.extend(bracket(BITMAP, picked));
    v.extend(raws(&[
        // Churn + resummarize: inclusion add_value / union widening.
        "UPDATE fz_bb_i SET bx = box(point(pk % 900, pk % 700), point(pk % 900 + 80, pk % 700 + 60)) \
         WHERE pk % 9 = 2;",
        "SELECT brin_summarize_new_values('fz_bb_i_bx')::int8 >= 0;",
        "VACUUM fz_bb_i;",
        "DROP TABLE fz_bb_i;",
    ]));
    v
}

// --------------------------------------------------- BRIN minmax_multi ----

/// brin_minmax_multi_*: clustered-with-outliers int4/numeric/date data
/// under small values_per_range (compaction + distance merging),
/// add_value expansion via churn, consistent probes for every strategy.
fn gen_brinmm(g: &mut Gen) -> Vec<StmtKind> {
    let vpr = [8usize, 16, 32][g.rng.below_usize(3)];
    let mut v = vec![
        raw("CREATE TABLE fz_bb_m (pk int4 PRIMARY KEY, n int4, q numeric, d date, pad text) \
             WITH (autovacuum_enabled = off);"),
        // Three value clusters plus scattered outliers per page-run: the
        // summaries must merge intervals under the values_per_range cap.
        raw(
            "INSERT INTO fz_bb_m SELECT i, \
             CASE WHEN i % 17 = 0 THEN 1000000 + i \
                  WHEN i % 3 = 0 THEN 500000 + (i * 7) % 1000 \
                  ELSE (i * 11) % 5000 END, \
             (((i * 13) % 90000)::numeric) / 100, \
             date '2001-01-01' + ((i * 37) % 9000), \
             repeat('w', 700) FROM generate_series(1, 2700) i;",
        ),
        raw(format!(
            "CREATE INDEX fz_bb_m_n ON fz_bb_m USING brin (n int4_minmax_multi_ops) \
             WITH (pages_per_range = 2, values_per_range = {vpr});"
        )),
        raw(format!(
            "CREATE INDEX fz_bb_m_q ON fz_bb_m USING brin (q numeric_minmax_multi_ops) \
             WITH (pages_per_range = 2, values_per_range = {vpr});"
        )),
        raw("CREATE INDEX fz_bb_m_d ON fz_bb_m USING brin (d date_minmax_multi_ops) \
             WITH (pages_per_range = 4);"),
        raw("ANALYZE fz_bb_m;"),
    ];
    v.extend(bracket(
        BITMAP,
        vec![
            raw("SELECT count(*)::int8 FROM fz_bb_m WHERE n < 100;"),
            raw("SELECT count(*)::int8 FROM fz_bb_m WHERE n <= 4999;"),
            raw("SELECT count(*)::int8 FROM fz_bb_m WHERE n = 500250;"),
            raw("SELECT count(*)::int8 FROM fz_bb_m WHERE n >= 1000000;"),
            raw("SELECT count(*)::int8 FROM fz_bb_m WHERE n > 1002000;"),
            raw("SELECT count(*)::int8 FROM fz_bb_m WHERE q < 10.00;"),
            raw("SELECT count(*)::int8 FROM fz_bb_m WHERE q = 123.45;"),
            raw("SELECT count(*)::int8 FROM fz_bb_m WHERE q >= 899.99;"),
            raw("SELECT count(*)::int8 FROM fz_bb_m WHERE d < date '2002-06-01';"),
            raw("SELECT count(*)::int8 FROM fz_bb_m WHERE d = date '2013-03-07';"),
            raw("SELECT count(*)::int8 FROM fz_bb_m WHERE d > date '2024-01-01';"),
        ],
    ));
    v.extend(raws(&[
        // Churn: inserts landing inside summarized ranges force
        // add_value (interval expansion, re-compaction) via doupdate.
        "UPDATE fz_bb_m SET n = n + 2000000 WHERE pk % 23 = 5;",
        "INSERT INTO fz_bb_m SELECT 100000 + i, -i * 97, (i::numeric) / 7, \
         date '1954-06-01' + i % 300, repeat('v', 700) FROM generate_series(1, 300) i;",
        "SELECT brin_summarize_new_values('fz_bb_m_n')::int8 >= 0;",
        "SELECT brin_summarize_new_values('fz_bb_m_q')::int8 >= 0;",
        "VACUUM fz_bb_m;",
        "SELECT count(*)::int8, sum(n)::int8 FROM fz_bb_m;",
        "DROP TABLE fz_bb_m;",
    ]));
    v
}

// -------------------------------------------------- BRIN maintenance ------

/// brinsummarize / desummarize / revmap arms + their SQL-callable error
/// fuel, BRIN vacuum cleanup, REINDEX; pages_per_range=1 over a ~300
/// page heap grows the revmap past one page (revmap_physical_extend).
fn gen_brinmaint(_g: &mut Gen) -> Vec<StmtKind> {
    let mut v = raws(&[
        "CREATE TABLE fz_bb_mt (pk int4 PRIMARY KEY, v int4, pad text) \
         WITH (autovacuum_enabled = off);",
        "INSERT INTO fz_bb_mt SELECT i, (i * 19) % 10000, repeat('e', 800) \
         FROM generate_series(1, 2700) i;",
        "CREATE INDEX fz_bb_mt_b ON fz_bb_mt USING brin (v) \
         WITH (pages_per_range = 1, autosummarize = on);",
        "ANALYZE fz_bb_mt;",
        // Range-at-a-time summarize incl. already-summarized re-calls.
        "SELECT brin_summarize_range('fz_bb_mt_b', 0)::int8;",
        "SELECT brin_summarize_range('fz_bb_mt_b', 1)::int8;",
        "SELECT brin_summarize_range('fz_bb_mt_b', 0)::int8;",
        "SELECT brin_summarize_range('fz_bb_mt_b', 4294967295)::int8;",
        "SELECT brin_summarize_new_values('fz_bb_mt_b')::int8;",
        // Desummarize + requery (unsummarized ranges scan everything),
        // then resummarize.
        "SELECT brin_desummarize_range('fz_bb_mt_b', 0);",
        "SELECT brin_desummarize_range('fz_bb_mt_b', 5);",
        "SELECT brin_desummarize_range('fz_bb_mt_b', 5);",
    ]);
    v.extend(bracket(
        BITMAP,
        vec![
            raw("SELECT count(*)::int8 FROM fz_bb_mt WHERE v < 50;"),
            raw("SELECT count(*)::int8 FROM fz_bb_mt WHERE v BETWEEN 4000 AND 4400;"),
        ],
    ));
    v.extend(raws(&[
        "SELECT brin_summarize_range('fz_bb_mt_b', 5)::int8;",
        // Error fuel — every arm deterministic on both engines.
        "SELECT brin_summarize_range('fz_bb_mt_b', -1)::int8;",
        "SELECT brin_desummarize_range('fz_bb_mt_b', -7);",
        "SELECT brin_summarize_range('fz_bb_mt_pkey', 0)::int8;",
        "SELECT brin_summarize_new_values('fz_bb_mt')::int8;",
        // Delete + BRIN VACUUM: brin_page_cleanup / brin_vacuum_scan.
        "DELETE FROM fz_bb_mt WHERE pk % 2 = 0;",
        "VACUUM fz_bb_mt;",
        "REINDEX INDEX fz_bb_mt_b;",
        "SELECT count(*)::int8, sum(v)::int8 FROM fz_bb_mt;",
        "DROP TABLE fz_bb_mt;",
    ]));
    v
}

// ---------------------------------------------------- operator family -----

/// btadjustmembers + btvalidate arms: ALTER OPERATOR FAMILY .. USING
/// btree ADD/DROP over search operators, cross-type members and every
/// support-function slot, plus the rejection arms (bad strategy / bad
/// support number / FOR ORDER BY on btree).
fn gen_opfam(_g: &mut Gen) -> Vec<StmtKind> {
    raws(&[
        "CREATE OPERATOR FAMILY fz_bb_fam USING btree;",
        // Same-type members: full strategy set + cmp / sortsupport /
        // in_range / equalimage support functions.
        "ALTER OPERATOR FAMILY fz_bb_fam USING btree ADD \
         OPERATOR 1 < (int4, int4), OPERATOR 2 <= (int4, int4), \
         OPERATOR 3 = (int4, int4), OPERATOR 4 >= (int4, int4), \
         OPERATOR 5 > (int4, int4), \
         FUNCTION 1 btint4cmp(int4, int4), \
         FUNCTION 2 btint4sortsupport(internal), \
         FUNCTION 3 (int4, int4) in_range(int4, int4, int4, boolean, boolean), \
         FUNCTION 4 (int4, int4) btequalimage(oid);",
        // Cross-type members.
        "ALTER OPERATOR FAMILY fz_bb_fam USING btree ADD \
         OPERATOR 1 < (int4, int8), OPERATOR 3 = (int4, int8), \
         OPERATOR 5 > (int4, int8), FUNCTION 1 btint48cmp(int4, int8);",
        // Rejection arms (each a compared error).
        "ALTER OPERATOR FAMILY fz_bb_fam USING btree ADD OPERATOR 9 < (int2, int2);",
        "ALTER OPERATOR FAMILY fz_bb_fam USING btree ADD FUNCTION 9 btint2cmp(int2, int2);",
        "ALTER OPERATOR FAMILY fz_bb_fam USING btree ADD \
         OPERATOR 1 < (box, box) FOR ORDER BY float_ops;",
        // DROP arms: cross-type first, then same-type members.
        "ALTER OPERATOR FAMILY fz_bb_fam USING btree DROP \
         OPERATOR 1 (int4, int8), FUNCTION 1 (int4, int8);",
        "ALTER OPERATOR FAMILY fz_bb_fam USING btree DROP \
         OPERATOR 2 (int4, int4), FUNCTION 3 (int4, int4);",
        // A loose operator class inside the family exercises
        // btvalidate over a partial opclass.
        "CREATE OPERATOR CLASS fz_bb_opc FOR TYPE int2 USING btree FAMILY fz_bb_fam AS \
         OPERATOR 1 <, OPERATOR 2 <=, OPERATOR 3 =, OPERATOR 4 >=, OPERATOR 5 >, \
         FUNCTION 1 btint2cmp(int2, int2);",
        // An index that actually uses the custom opclass.
        "CREATE TABLE fz_bb_oc (pk int4 PRIMARY KEY, s int2);",
        "INSERT INTO fz_bb_oc SELECT i, (i % 300)::int2 FROM generate_series(1, 900) i;",
        "CREATE INDEX fz_bb_oc_s ON fz_bb_oc (s fz_bb_opc);",
        "SET enable_seqscan = off;",
        "SELECT count(*)::int8 FROM fz_bb_oc WHERE s < 40::int2;",
        "SELECT count(*)::int8 FROM fz_bb_oc WHERE s = 123::int2;",
        "RESET enable_seqscan;",
        "DROP TABLE fz_bb_oc;",
        "DROP OPERATOR CLASS fz_bb_opc USING btree;",
        "DROP OPERATOR FAMILY fz_bb_fam USING btree;",
    ])
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
            let stmts = gen_btbrin_module(&mut g);
            out.push(stmts.iter().map(|s| s.to_sql()).collect());
        }
        out
    }

    #[test]
    fn groups_are_set_reset_balanced_and_self_contained() {
        // Every SET has its RESET in the same group; every explicit
        // transaction is closed in-group (ROLLBACK or COMMIT — the
        // deferred-unique arm commits deliberately); every CREATE
        // TABLE / OPERATOR FAMILY / OPERATOR CLASS has an in-group DROP.
        for group in gen_groups(41, 300) {
            let mut sets: Vec<String> = Vec::new();
            let mut open_txn = 0i32;
            let joined = group.join("\n");
            for sql in &group {
                if sql == "BEGIN;" {
                    open_txn += 1;
                } else if sql == "ROLLBACK;" || sql == "COMMIT;" {
                    open_txn -= 1;
                } else if let Some(rest) = sql.strip_prefix("SET ") {
                    if !rest.starts_with("CONSTRAINTS") {
                        sets.push(rest.split(' ').next().unwrap().to_string());
                    }
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
                        joined.contains(&format!("DROP TABLE {t};"))
                            || joined.contains(&format!("DROP TABLE {t},"))
                            || joined.contains(&format!(", {t};")),
                        "no in-group DROP for {t} in {group:?}"
                    );
                }
            }
            assert!(sets.is_empty(), "unRESET SETs {sets:?} in {group:?}");
            assert_eq!(open_txn, 0, "unclosed BEGIN in {group:?}");
            if joined.contains("CREATE OPERATOR FAMILY") {
                assert!(joined.contains("DROP OPERATOR FAMILY"));
            }
            if joined.contains("CREATE OPERATOR CLASS") {
                assert!(joined.contains("DROP OPERATOR CLASS"));
            }
        }
    }

    #[test]
    fn row_returning_statements_are_totally_ordered() {
        // Row-returning SELECTs carry a total ORDER BY or LIMIT-with-
        // ORDER; single-row aggregate projections and scalar summarize
        // probes are exempt. FETCH output order is the cursor's ORDER BY.
        for group in gen_groups(17, 300) {
            for sql in &group {
                if !sql.starts_with("SELECT ") {
                    continue;
                }
                let single_row = sql.starts_with("SELECT count(")
                    || sql.starts_with("SELECT sum(")
                    || sql.starts_with("SELECT brin_")
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
        // B1: exact-typed data and aggregates only. (The box literals /
        // point constructors are int-valued and only feed count(*)
        // probes, never projected raw; float_ops appears solely in the
        // deliberately-rejected FOR ORDER BY error arm.)
        for group in gen_groups(23, 200) {
            for sql in &group {
                assert!(
                    !sql.contains("float8") && !sql.contains("random("),
                    "float surface in {sql}"
                );
            }
        }
    }

    #[test]
    fn oversized_brin_arm_is_isolated_and_placement_independent() {
        // RB-3-recur regression (Antithesis rounds 7/9/10): the 54000
        // program-limit arm must be deterministic under concurrent load.
        // (a) The oversized chained-md5 insert only ever targets the
        //     dedicated single-page fz_bb_ov table — never the churned
        //     fz_bb_bu heap, where the outcome depended on FSM placement
        //     and thus on the visibility horizon at VACUUM time.
        // (b) Whenever the arm appears, range 0 is summarized before the
        //     oversized insert (summarize precedes it in the group), so
        //     the widen — and the identical ereport on both engines — is
        //     unconditional.
        let mut saw_arm = false;
        for group in gen_groups(7, 300) {
            for (i, sql) in group.iter().enumerate() {
                if sql.contains("string_agg(md5(i::text), '')") {
                    assert!(
                        sql.contains("INSERT INTO fz_bb_ov"),
                        "oversized BRIN insert targets a churned heap: {sql}"
                    );
                    saw_arm = true;
                    let before = &group[..i];
                    assert!(
                        before.iter().any(|s| s
                            .contains("brin_summarize_new_values('fz_bb_ov_brin')")),
                        "oversized insert not preceded by fz_bb_ov summarize"
                    );
                    assert!(
                        before
                            .iter()
                            .any(|s| s.contains("INSERT INTO fz_bb_ov VALUES (1, 'seed');")),
                        "fz_bb_ov seed row missing before oversized insert"
                    );
                }
            }
        }
        assert!(saw_arm, "oversized BRIN program-limit arm never generated");
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
            gen_btbrin_module(&mut g);
        }
        for p in productions.iter() {
            if let Some(s) = p.strip_prefix("btbrin:shape:") {
                seen.insert(s.to_string());
            }
        }
        assert_eq!(seen.len(), SHAPES.len(), "unreached shapes: {seen:?}");
    }
}
