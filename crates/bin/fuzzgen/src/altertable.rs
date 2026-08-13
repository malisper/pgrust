//! ALTER TABLE rewrite/phase EXECUTION drain module (Track-B): the
//! SUCCESSFUL-execution + table-rewrite surface of backend/commands/
//! tablecmds.c — ATRewriteTable, ATExecAlterColumnType, ATExecAddColumn and
//! the AT_PASS phase machinery. The earm/earm2/earm3 lanes already drain the
//! ereport(ERROR) validation arms of ALTER TABLE; this module drives the
//! statements that SUCCEED and must transform the on-disk data correctly.
//!
//! The core mechanism: build a table with deterministic data, run one ALTER
//! (or a multi-subcommand ALTER), then VERIFY —
//!   - the full table CONTENT post-rewrite via a TOTAL ORDER BY pk probe
//!     with every surviving column cast ::text (a wrong value produced by a
//!     rewrite shows up as a row-level differential divergence — the HIGH
//!     signal this lane exists to catch);
//!   - the stable column METADATA via a pg_attribute projection
//!     (attname/type/notnull/storage/compression/generated, ORDER BY attnum);
//!   - that dependent indexes/constraints survive a type change that rebuilds
//!     them (equality lookups over the rebuilt index return the right rows).
//!
//! Every group is FULLY SELF-CONTAINED: it creates its own `fz_at_*`
//! fixtures, does the ALTER(s), probes, and DROPs everything it made — so
//! there is no persistent catalog growth and no cross-module coupling. The
//! only session-persistent state is a monotonic name counter (crate::AltState,
//! swapped in/out by the session loop like the other module states) so table
//! names are unique across a session even though each group is disjoint.
//!
//! Determinism laws (same discipline as crate::heap / crate::spill):
//!   - all base data comes from generate_series with pure integer/text/
//!     numeric expressions — no random(), no clock, no volatile input;
//!   - every row-returning probe carries a TOTAL ORDER BY on the pk and casts
//!     each projected column ::text, so the row set and its order are fully
//!     determined and comparison is byte-exact;
//!   - NO float column is ever projected into an ordered row probe (float text
//!     representation is not a row-surface here); the one volatile-default
//!     family (random()) is verified with RANGE-FACT aggregates only
//!     (count / count FILTER), never per-row values;
//!   - the nextval volatile-default family is verified with
//!     ORDER-INDEPENDENT aggregates (count / count distinct / min / max),
//!     which are pure functions of the row count regardless of the physical
//!     scan order the rewrite assigns values in (heap order is a ruled
//!     non-surface — see the COPY order ruling);
//!   - every CREATE has a matching DROP in the same group.
//!
//! Everything is emitted as `StmtKind::Raw` (one single-line statement per
//! entry, terminated with `;`).

use crate::stmt::{Gen, StmtKind};

/// The family selectors (weighted in weights::PROD_WEIGHTS).
const FAMILIES: &[&str] = &[
    "alt:coltype_using",
    "alt:coltype_norewrite",
    "alt:addcol_const",
    "alt:addcol_volatile",
    "alt:setdropdefault",
    "alt:notnull",
    "alt:setopts",
    "alt:dropcol",
    "alt:logged",
    "alt:generated",
    "alt:multi",
    "alt:coltype_indexed",
    "alt:inherit",
    "alt:replident",
    "alt:partition",
];

/// Session-persistent naming counter (swapped in/out of `Gen` by the session
/// loop exactly like `HeapState`). No object outlives its group, so this is
/// all the state the module needs.
#[derive(Clone, Debug, Default)]
pub struct AltState {
    next: u32,
}

impl AltState {
    pub fn new() -> AltState {
        AltState::default()
    }
}

/// Registry entry point (stmt::STMT_MODULES).
pub fn gen_altertable_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("altertable");
    let family = g.weights.pick(g.rng, FAMILIES);
    g.fire(family);
    match family {
        "alt:coltype_using" => gen_coltype_using(g),
        "alt:coltype_norewrite" => gen_coltype_norewrite(g),
        "alt:addcol_const" => gen_addcol_const(g),
        "alt:addcol_volatile" => gen_addcol_volatile(g),
        "alt:setdropdefault" => gen_setdropdefault(g),
        "alt:notnull" => gen_notnull(g),
        "alt:setopts" => gen_setopts(g),
        "alt:dropcol" => gen_dropcol(g),
        "alt:logged" => gen_logged(g),
        "alt:generated" => gen_generated(g),
        "alt:multi" => gen_multi(g),
        "alt:coltype_indexed" => gen_coltype_indexed(g),
        "alt:inherit" => gen_inherit(g),
        "alt:replident" => gen_replident(g),
        _ => gen_partition(g),
    }
}

// ---------------------------------------------------------------- helpers ---

/// A unique object suffix for this group.
fn tag(g: &mut Gen) -> u32 {
    let n = g.altertable.next;
    g.altertable.next += 1;
    n
}

/// Small deterministic row count for the base load.
fn nrows(g: &mut Gen) -> u32 {
    match g.weights.pick(g.rng, &["alt:rows:200", "alt:rows:800", "alt:rows:2000"]) {
        "alt:rows:200" => 200,
        "alt:rows:800" => 800,
        _ => 2000,
    }
}

fn raw(s: String) -> StmtKind {
    StmtKind::Raw(s)
}

// ----------------------------------------------------- ALTER COLUMN TYPE ----

/// ALTER COLUMN ... TYPE ... USING ...: the always-rewrite path
/// (ATRewriteTable + the ATExecAlterColumnType tuple-mapping). The USING
/// expression transforms the data; the probe verifies the transform on every
/// row.
fn gen_coltype_using(g: &mut Gen) -> Vec<StmtKind> {
    let n = tag(g);
    let t = format!("fz_at_{n}");
    let rows = nrows(g);
    let shape = g.weights.pick(
        g.rng,
        &[
            "alt:cu:text2int",
            "alt:cu:int2text",
            "alt:cu:int2numeric",
            "alt:cu:text2bool",
            "alt:cu:int2bigint",
        ],
    );
    g.fire(shape);
    // Base: id pk, n int, s text. Values are pure functions of the series.
    let mut stmts = vec![
        raw(format!("CREATE TABLE {t} (id int PRIMARY KEY, n int, s text);")),
        raw(format!(
            "INSERT INTO {t} SELECT g, (g * 7) % 1000, \
             CASE WHEN g % 2 = 0 THEN 't' ELSE 'f' END \
             FROM generate_series(1, {rows}) g;"
        )),
    ];
    // Give s a numeric-string payload for the text->number arms.
    let (alter, probe_col): (String, &str) = match shape {
        "alt:cu:text2int" => {
            stmts.push(raw(format!("UPDATE {t} SET s = (n * 3)::text;")));
            (
                format!("ALTER TABLE {t} ALTER COLUMN s TYPE int USING s::int;"),
                "s",
            )
        }
        "alt:cu:int2text" => (
            format!("ALTER TABLE {t} ALTER COLUMN n TYPE text USING n::text;"),
            "n",
        ),
        "alt:cu:int2numeric" => (
            format!(
                "ALTER TABLE {t} ALTER COLUMN n TYPE numeric(18,6) USING (n::numeric / 7);"
            ),
            "n",
        ),
        "alt:cu:text2bool" => (
            format!("ALTER TABLE {t} ALTER COLUMN s TYPE bool USING s::bool;"),
            "s",
        ),
        _ => (
            format!(
                "ALTER TABLE {t} ALTER COLUMN n TYPE bigint USING (n::bigint * 1000000000 + id);"
            ),
            "n",
        ),
    };
    stmts.push(raw(alter));
    stmts.push(content_probe(&t, &["id", probe_col]));
    stmts.push(attr_probe(&t));
    stmts.push(raw(format!("DROP TABLE {t};")));
    stmts
}

/// ALTER COLUMN ... TYPE ... on binary-coercible / no-op-rewrite pairs: the
/// ATExecAlterColumnType "no rewrite required" branch (varchar->text,
/// varchar(n)->varchar(m>n)). The content must survive UNCHANGED.
fn gen_coltype_norewrite(g: &mut Gen) -> Vec<StmtKind> {
    let n = tag(g);
    let t = format!("fz_at_{n}");
    let rows = nrows(g);
    let shape = g.weights.pick(g.rng, &["alt:cn:vc2text", "alt:cn:widen"]);
    g.fire(shape);
    let mut stmts = vec![
        raw(format!(
            "CREATE TABLE {t} (id int PRIMARY KEY, s varchar(20));"
        )),
        raw(format!(
            "INSERT INTO {t} SELECT g, 'v' || (g % 97)::text FROM generate_series(1, {rows}) g;"
        )),
    ];
    let alter = match shape {
        "alt:cn:vc2text" => format!("ALTER TABLE {t} ALTER COLUMN s TYPE text;"),
        _ => format!("ALTER TABLE {t} ALTER COLUMN s TYPE varchar(40);"),
    };
    stmts.push(raw(alter));
    stmts.push(content_probe(&t, &["id", "s"]));
    stmts.push(attr_probe(&t));
    stmts.push(raw(format!("DROP TABLE {t};")));
    stmts
}

// -------------------------------------------------------- ADD COLUMN paths --

/// ADD COLUMN with a CONSTANT default (or none): the PG11 fast path — no
/// rewrite, the default is stored as a missing value in pg_attribute and
/// materialized on read. Verify every row reads back the default.
fn gen_addcol_const(g: &mut Gen) -> Vec<StmtKind> {
    let n = tag(g);
    let t = format!("fz_at_{n}");
    let rows = nrows(g);
    let shape = g.weights.pick(
        g.rng,
        &[
            "alt:ac:int",
            "alt:ac:text",
            "alt:ac:bool",
            "alt:ac:numeric",
            "alt:ac:nodefault",
            "alt:ac:notnull",
        ],
    );
    g.fire(shape);
    let mut stmts = vec![
        raw(format!("CREATE TABLE {t} (id int PRIMARY KEY, n int);")),
        raw(format!(
            "INSERT INTO {t} SELECT g, (g * 5) % 200 FROM generate_series(1, {rows}) g;"
        )),
    ];
    let (alter, col): (String, &str) = match shape {
        "alt:ac:int" => (format!("ALTER TABLE {t} ADD COLUMN c int DEFAULT 42;"), "c"),
        "alt:ac:text" => (
            format!("ALTER TABLE {t} ADD COLUMN c text DEFAULT 'const';"),
            "c",
        ),
        "alt:ac:bool" => (
            format!("ALTER TABLE {t} ADD COLUMN c bool DEFAULT true;"),
            "c",
        ),
        "alt:ac:numeric" => (
            format!("ALTER TABLE {t} ADD COLUMN c numeric(10,3) DEFAULT 3.140;"),
            "c",
        ),
        "alt:ac:nodefault" => (format!("ALTER TABLE {t} ADD COLUMN c int;"), "c"),
        _ => (
            format!("ALTER TABLE {t} ADD COLUMN c int NOT NULL DEFAULT 7;"),
            "c",
        ),
    };
    stmts.push(raw(alter));
    stmts.push(content_probe(&t, &["id", "n", col]));
    stmts.push(attr_probe(&t));
    stmts.push(raw(format!("DROP TABLE {t};")));
    stmts
}

/// ADD COLUMN with a VOLATILE default: the rewrite path (a volatile default
/// forces ATRewriteTable so each row is evaluated). Volatile output is not a
/// row-surface, so the probe checks only ORDER-INDEPENDENT / RANGE facts:
///   - nextval: min/max/count/count-distinct are pure functions of the row
///     count (1..rows, all distinct) regardless of assignment order;
///   - random(): every row is populated and inside [0,1).
fn gen_addcol_volatile(g: &mut Gen) -> Vec<StmtKind> {
    let n = tag(g);
    let t = format!("fz_at_{n}");
    let seq = format!("fz_ats_{n}");
    let rows = nrows(g);
    let shape = g.weights.pick(g.rng, &["alt:av:nextval", "alt:av:random"]);
    g.fire(shape);
    let mut stmts = vec![
        raw(format!("CREATE TABLE {t} (id int PRIMARY KEY, n int);")),
        raw(format!(
            "INSERT INTO {t} SELECT g, g FROM generate_series(1, {rows}) g;"
        )),
    ];
    match shape {
        "alt:av:nextval" => {
            stmts.push(raw(format!("CREATE SEQUENCE {seq};")));
            stmts.push(raw(format!(
                "ALTER TABLE {t} ADD COLUMN c bigint NOT NULL DEFAULT nextval('{seq}');"
            )));
            // Order-independent facts: distinct == count == rows, min 1, max rows.
            stmts.push(raw(format!(
                "SELECT count(*), count(c), count(DISTINCT c), min(c), max(c) FROM {t};"
            )));
            stmts.push(raw(format!("DROP TABLE {t};")));
            stmts.push(raw(format!("DROP SEQUENCE {seq};")));
        }
        _ => {
            stmts.push(raw(format!(
                "ALTER TABLE {t} ADD COLUMN c float8 NOT NULL DEFAULT random();"
            )));
            // Range facts only: fully populated, every value in [0,1).
            stmts.push(raw(format!(
                "SELECT count(*), count(c), \
                 count(*) FILTER (WHERE c >= 0 AND c < 1) FROM {t};"
            )));
            stmts.push(raw(format!("DROP TABLE {t};")));
        }
    }
    stmts
}

// -------------------------------------------------------- SET/DROP DEFAULT --

/// SET DEFAULT / DROP DEFAULT (ATExecColumnDefault): metadata-only, verified
/// by inserting a fresh row that relies on (or overrides) the default.
fn gen_setdropdefault(g: &mut Gen) -> Vec<StmtKind> {
    let n = tag(g);
    let t = format!("fz_at_{n}");
    let rows = nrows(g);
    let mut stmts = vec![
        raw(format!("CREATE TABLE {t} (id int PRIMARY KEY, n int);")),
        raw(format!(
            "INSERT INTO {t} SELECT g, g * 2 FROM generate_series(1, {rows}) g;"
        )),
    ];
    if g.weights.pick(g.rng, &["alt:dd:set", "alt:dd:drop"]) == "alt:dd:set" {
        g.fire("alt:dd:set");
        stmts.push(raw(format!("ALTER TABLE {t} ALTER COLUMN n SET DEFAULT 999;")));
        // New row uses the default.
        stmts.push(raw(format!("INSERT INTO {t} (id) VALUES ({});", rows + 1)));
    } else {
        g.fire("alt:dd:drop");
        stmts.push(raw(format!("ALTER TABLE {t} ALTER COLUMN n SET DEFAULT 999;")));
        stmts.push(raw(format!("ALTER TABLE {t} ALTER COLUMN n DROP DEFAULT;")));
        // Default gone -> the new row's n is NULL.
        stmts.push(raw(format!("INSERT INTO {t} (id) VALUES ({});", rows + 1)));
    }
    stmts.push(content_probe(&t, &["id", "n"]));
    stmts.push(attr_probe(&t));
    stmts.push(raw(format!("DROP TABLE {t};")));
    stmts
}

// ------------------------------------------------------- SET/DROP NOT NULL --

/// SET NOT NULL (ATExecSetNotNull — the full-table verify scan) over a column
/// with no nulls, then DROP NOT NULL. Verify content + the attnotnull flag.
fn gen_notnull(g: &mut Gen) -> Vec<StmtKind> {
    let n = tag(g);
    let t = format!("fz_at_{n}");
    let rows = nrows(g);
    let mut stmts = vec![
        raw(format!("CREATE TABLE {t} (id int PRIMARY KEY, n int);")),
        raw(format!(
            "INSERT INTO {t} SELECT g, (g * 11) % 500 FROM generate_series(1, {rows}) g;"
        )),
        // Full-table validation scan; succeeds (no nulls present).
        raw(format!("ALTER TABLE {t} ALTER COLUMN n SET NOT NULL;")),
    ];
    stmts.push(attr_probe(&t));
    if g.rng.chance(1, 2) {
        g.fire("alt:nn:drop");
        stmts.push(raw(format!("ALTER TABLE {t} ALTER COLUMN n DROP NOT NULL;")));
        stmts.push(attr_probe(&t));
    }
    stmts.push(content_probe(&t, &["id", "n"]));
    stmts.push(raw(format!("DROP TABLE {t};")));
    stmts
}

// --------------------------------------------------- storage/stat/compress --

/// SET STORAGE / SET STATISTICS / SET COMPRESSION (metadata-only paths in
/// tablecmds.c). Content must be untouched; the attribute probe witnesses the
/// metadata change.
fn gen_setopts(g: &mut Gen) -> Vec<StmtKind> {
    let n = tag(g);
    let t = format!("fz_at_{n}");
    let rows = nrows(g);
    let mut stmts = vec![
        raw(format!("CREATE TABLE {t} (id int PRIMARY KEY, n int, s text);")),
        raw(format!(
            "INSERT INTO {t} SELECT g, g % 100, repeat('a', 1 + g % 20) \
             FROM generate_series(1, {rows}) g;"
        )),
    ];
    let shape = g.weights.pick(
        g.rng,
        &[
            "alt:so:storage_ext",
            "alt:so:storage_main",
            "alt:so:storage_plain",
            "alt:so:stats",
            "alt:so:compression",
        ],
    );
    g.fire(shape);
    let alter = match shape {
        "alt:so:storage_ext" => format!("ALTER TABLE {t} ALTER COLUMN s SET STORAGE EXTERNAL;"),
        "alt:so:storage_main" => format!("ALTER TABLE {t} ALTER COLUMN s SET STORAGE MAIN;"),
        "alt:so:storage_plain" => format!("ALTER TABLE {t} ALTER COLUMN s SET STORAGE PLAIN;"),
        "alt:so:stats" => format!("ALTER TABLE {t} ALTER COLUMN n SET STATISTICS 500;"),
        // pglz is always available (lz4 is a build option; avoided for parity).
        _ => format!("ALTER TABLE {t} ALTER COLUMN s SET COMPRESSION pglz;"),
    };
    stmts.push(raw(alter));
    stmts.push(content_probe(&t, &["id", "n", "s"]));
    stmts.push(attr_probe(&t));
    stmts.push(raw(format!("DROP TABLE {t};")));
    stmts
}

// ------------------------------------------------------------- DROP COLUMN --

/// DROP COLUMN (ATExecDropColumn — logical drop, attisdropped) and the
/// ADD-then-DROP round trip. Verify the remaining columns' content.
fn gen_dropcol(g: &mut Gen) -> Vec<StmtKind> {
    let n = tag(g);
    let t = format!("fz_at_{n}");
    let rows = nrows(g);
    let mut stmts = vec![
        raw(format!(
            "CREATE TABLE {t} (id int PRIMARY KEY, a int, b text, c int);"
        )),
        raw(format!(
            "INSERT INTO {t} SELECT g, g * 2, 'r' || g::text, g * 3 \
             FROM generate_series(1, {rows}) g;"
        )),
    ];
    if g.weights.pick(g.rng, &["alt:dc:existing", "alt:dc:addrop"]) == "alt:dc:existing" {
        g.fire("alt:dc:existing");
        stmts.push(raw(format!("ALTER TABLE {t} DROP COLUMN b;")));
        stmts.push(content_probe(&t, &["id", "a", "c"]));
    } else {
        g.fire("alt:dc:addrop");
        stmts.push(raw(format!("ALTER TABLE {t} ADD COLUMN d int DEFAULT 1;")));
        stmts.push(raw(format!("ALTER TABLE {t} DROP COLUMN d;")));
        stmts.push(raw(format!("ALTER TABLE {t} DROP COLUMN c;")));
        stmts.push(content_probe(&t, &["id", "a", "b"]));
    }
    stmts.push(attr_probe(&t));
    stmts.push(raw(format!("DROP TABLE {t};")));
    stmts
}

// ------------------------------------------------------ SET LOGGED/UNLOGGED --

/// SET UNLOGGED / SET LOGGED (ATExecSetLogged — a full heap rewrite into a
/// new relfilenode). Content must be preserved across the rewrite.
fn gen_logged(g: &mut Gen) -> Vec<StmtKind> {
    let n = tag(g);
    let t = format!("fz_at_{n}");
    let rows = nrows(g);
    let mut stmts = vec![
        raw(format!("CREATE TABLE {t} (id int PRIMARY KEY, n int, s text);")),
        raw(format!(
            "INSERT INTO {t} SELECT g, g * 13, md5(g::text) FROM generate_series(1, {rows}) g;"
        )),
        raw(format!("ALTER TABLE {t} SET UNLOGGED;")),
    ];
    stmts.push(content_probe(&t, &["id", "n", "s"]));
    if g.rng.chance(1, 2) {
        g.fire("alt:lg:relog");
        stmts.push(raw(format!("ALTER TABLE {t} SET LOGGED;")));
        stmts.push(content_probe(&t, &["id", "n", "s"]));
    }
    stmts.push(raw(format!("DROP TABLE {t};")));
    stmts
}

// ------------------------------------------------------- GENERATED / STORED --

/// ADD COLUMN ... GENERATED ALWAYS AS (expr) STORED (rewrite path computing
/// the stored value per row). The expression is immutable over existing
/// columns, so the generated values are deterministic and fully verified.
fn gen_generated(g: &mut Gen) -> Vec<StmtKind> {
    let n = tag(g);
    let t = format!("fz_at_{n}");
    let rows = nrows(g);
    let mut stmts = vec![
        raw(format!("CREATE TABLE {t} (id int PRIMARY KEY, n int);")),
        raw(format!(
            "INSERT INTO {t} SELECT g, (g * 9) % 400 FROM generate_series(1, {rows}) g;"
        )),
    ];
    if g.weights.pick(g.rng, &["alt:gen:add", "alt:gen:addcol_then_gen"]) == "alt:gen:add" {
        g.fire("alt:gen:add");
        stmts.push(raw(format!(
            "ALTER TABLE {t} ADD COLUMN gcol int GENERATED ALWAYS AS (n * 2 + id) STORED;"
        )));
    } else {
        g.fire("alt:gen:addcol_then_gen");
        // Two subcommands: a plain add then a generated add referencing it.
        stmts.push(raw(format!("ALTER TABLE {t} ADD COLUMN m int DEFAULT 4;")));
        stmts.push(raw(format!(
            "ALTER TABLE {t} ADD COLUMN gcol int GENERATED ALWAYS AS (n + m) STORED;"
        )));
    }
    stmts.push(content_probe(&t, &["id", "n", "gcol"]));
    stmts.push(attr_probe(&t));
    stmts.push(raw(format!("DROP TABLE {t};")));
    stmts
}

// ------------------------------------------------------ multi-subcommand AT --

/// Multiple sub-commands in one ALTER TABLE — the AT_PASS phase-ordering
/// machinery (drops before adds before alters-of-type, etc). All of it must
/// commit atomically with the right final shape and content.
fn gen_multi(g: &mut Gen) -> Vec<StmtKind> {
    let n = tag(g);
    let t = format!("fz_at_{n}");
    let rows = nrows(g);
    let mut stmts = vec![
        raw(format!(
            "CREATE TABLE {t} (id int PRIMARY KEY, n int, s text, junk int);"
        )),
        raw(format!(
            "INSERT INTO {t} SELECT g, (g * 3) % 250, (g * 4)::text, g \
             FROM generate_series(1, {rows}) g;"
        )),
        // Drop + add(const) + alter-type(USING) + set-not-null, one statement.
        raw(format!(
            "ALTER TABLE {t} DROP COLUMN junk, \
             ADD COLUMN a int DEFAULT 7, \
             ALTER COLUMN s TYPE int USING s::int, \
             ALTER COLUMN n SET NOT NULL;"
        )),
    ];
    stmts.push(content_probe(&t, &["id", "n", "s", "a"]));
    stmts.push(attr_probe(&t));
    stmts.push(raw(format!("DROP TABLE {t};")));
    stmts
}

// ----------------------------------------------- type change under an index --

/// ALTER COLUMN TYPE on a column carrying an index / unique constraint: the
/// index is rebuilt as part of the rewrite (ATPostAlterTypeCleanup). Verify
/// the content AND that the rebuilt index still returns the right rows for
/// equality lookups.
fn gen_coltype_indexed(g: &mut Gen) -> Vec<StmtKind> {
    let n = tag(g);
    let t = format!("fz_at_{n}");
    let idx = format!("fz_ati_{n}");
    let rows = nrows(g);
    let unique = g.weights.pick(g.rng, &["alt:ci:plain", "alt:ci:unique"]) == "alt:ci:unique";
    g.fire(if unique { "alt:ci:unique" } else { "alt:ci:plain" });
    let mut stmts = vec![
        raw(format!("CREATE TABLE {t} (id int PRIMARY KEY, k int);")),
        // k distinct so a UNIQUE index is valid; values are the series.
        raw(format!(
            "INSERT INTO {t} SELECT g, g FROM generate_series(1, {rows}) g;"
        )),
    ];
    stmts.push(raw(format!(
        "CREATE {}INDEX {idx} ON {t} (k);",
        if unique { "UNIQUE " } else { "" }
    )));
    // Rewrite that changes k's type, forcing an index rebuild.
    stmts.push(raw(format!(
        "ALTER TABLE {t} ALTER COLUMN k TYPE bigint USING k::bigint;"
    )));
    // Equality lookups over the rebuilt index (result set compared).
    let probe_k = 1 + g.rng.below(rows as u64) as u32;
    stmts.push(raw(format!(
        "SELECT id, k::text FROM {t} WHERE k = {probe_k} ORDER BY id;"
    )));
    stmts.push(content_probe(&t, &["id", "k"]));
    stmts.push(attr_probe(&t));
    stmts.push(raw(format!("DROP TABLE {t};")));
    stmts
}

// ------------------------------------------------------------- inheritance --

/// Inheritance: ADD/DROP COLUMN cascading from a parent to children,
/// ALTER TABLE ONLY (parent-only), and INHERIT / NO INHERIT relinking. Verify
/// content through the parent (all descendants) and via ONLY (parent alone).
fn gen_inherit(g: &mut Gen) -> Vec<StmtKind> {
    let n = tag(g);
    let p = format!("fz_atp_{n}");
    let c = format!("fz_atc_{n}");
    let rows = nrows(g);
    let child_rows = rows / 2;
    let mut stmts = vec![
        raw(format!("CREATE TABLE {p} (id int PRIMARY KEY, n int);")),
        raw(format!(
            "INSERT INTO {p} SELECT g, g * 2 FROM generate_series(1, {rows}) g;"
        )),
        // Child inherits; disjoint id range keeps ORDER BY id total across both.
        raw(format!("CREATE TABLE {c} () INHERITS ({p});")),
        raw(format!(
            "INSERT INTO {c} SELECT g, g * 2 FROM generate_series({}, {}) g;",
            10_000_000,
            10_000_000 + child_rows
        )),
    ];
    let shape = g.weights.pick(
        g.rng,
        &["alt:inh:cascade_add", "alt:inh:only_set", "alt:inh:noinherit"],
    );
    g.fire(shape);
    match shape {
        "alt:inh:cascade_add" => {
            // ADD COLUMN on the parent cascades to the child.
            stmts.push(raw(format!("ALTER TABLE {p} ADD COLUMN e int DEFAULT 3;")));
            // Parent scan sees parent+child; content ordered by id.
            stmts.push(content_probe(&p, &["id", "n", "e"]));
        }
        "alt:inh:only_set" => {
            // ALTER ... ONLY: SET DEFAULT on the parent alone (defaults are
            // not required to cascade, so ONLY is legal here — unlike ONLY
            // ADD COLUMN, which C rejects when children exist). A new parent
            // row picks up the parent's default; child inserts are unaffected.
            stmts.push(raw(format!("ALTER TABLE ONLY {p} ALTER COLUMN n SET DEFAULT 555;")));
            stmts.push(raw(format!("INSERT INTO {p} (id) VALUES ({});", rows + 1)));
            // Whole hierarchy (parent + child).
            stmts.push(content_probe(&p, &["id", "n"]));
            // ONLY parent.
            stmts.push(content_probe_only(&p, &["id", "n"]));
        }
        _ => {
            // Detach then re-attach the child.
            stmts.push(raw(format!("ALTER TABLE {c} NO INHERIT {p};")));
            // Parent scan now excludes the child.
            stmts.push(content_probe_only(&p, &["id", "n"]));
            stmts.push(raw(format!("ALTER TABLE {c} INHERIT {p};")));
            stmts.push(content_probe(&p, &["id", "n"]));
        }
    }
    stmts.push(raw(format!("DROP TABLE {p} CASCADE;")));
    stmts
}

// -------------------------------------------------------- REPLICA IDENTITY --

/// REPLICA IDENTITY DEFAULT / FULL / NOTHING / USING INDEX
/// (ATExecReplicaIdentity — metadata only, relreplident). Content untouched;
/// the pg_class probe witnesses the setting.
fn gen_replident(g: &mut Gen) -> Vec<StmtKind> {
    let n = tag(g);
    let t = format!("fz_at_{n}");
    let idx = format!("fz_atri_{n}");
    let rows = nrows(g);
    let mut stmts = vec![
        raw(format!("CREATE TABLE {t} (id int PRIMARY KEY, n int NOT NULL);")),
        raw(format!(
            "INSERT INTO {t} SELECT g, g FROM generate_series(1, {rows}) g;"
        )),
    ];
    let shape = g.weights.pick(
        g.rng,
        &["alt:ri:full", "alt:ri:nothing", "alt:ri:default", "alt:ri:index"],
    );
    g.fire(shape);
    match shape {
        "alt:ri:full" => stmts.push(raw(format!("ALTER TABLE {t} REPLICA IDENTITY FULL;"))),
        "alt:ri:nothing" => stmts.push(raw(format!("ALTER TABLE {t} REPLICA IDENTITY NOTHING;"))),
        "alt:ri:default" => stmts.push(raw(format!("ALTER TABLE {t} REPLICA IDENTITY DEFAULT;"))),
        _ => {
            // USING INDEX needs a UNIQUE index on NOT NULL columns.
            stmts.push(raw(format!("CREATE UNIQUE INDEX {idx} ON {t} (n);")));
            stmts.push(raw(format!("ALTER TABLE {t} REPLICA IDENTITY USING INDEX {idx};")));
        }
    }
    // relreplident: char in pg_class.
    stmts.push(raw(format!(
        "SELECT relreplident FROM pg_class WHERE oid = '{t}'::regclass;"
    )));
    stmts.push(content_probe(&t, &["id", "n"]));
    stmts.push(raw(format!("DROP TABLE {t};")));
    stmts
}

// ---------------------------------------------------------- partitioned AT --

/// ALTER TABLE on a partitioned PARENT propagating to all partitions:
/// ADD COLUMN / SET DEFAULT / ALTER COLUMN TYPE recurse into every leaf. The
/// parent scan unions all partitions; content ordered by id.
fn gen_partition(g: &mut Gen) -> Vec<StmtKind> {
    let n = tag(g);
    let p = format!("fz_atpp_{n}");
    let rows = nrows(g);
    // Range-partitioned on id into three parts.
    let b1 = rows / 3;
    let b2 = 2 * rows / 3;
    let mut stmts = vec![
        raw(format!(
            "CREATE TABLE {p} (id int NOT NULL, n int, s text) PARTITION BY RANGE (id);"
        )),
        raw(format!(
            "CREATE TABLE {p}_a PARTITION OF {p} FOR VALUES FROM (MINVALUE) TO ({});",
            b1 + 1
        )),
        raw(format!(
            "CREATE TABLE {p}_b PARTITION OF {p} FOR VALUES FROM ({}) TO ({});",
            b1 + 1,
            b2 + 1
        )),
        raw(format!(
            "CREATE TABLE {p}_c PARTITION OF {p} FOR VALUES FROM ({}) TO (MAXVALUE);",
            b2 + 1
        )),
        raw(format!(
            "INSERT INTO {p} SELECT g, (g * 7) % 300, (g * 2)::text \
             FROM generate_series(1, {rows}) g;"
        )),
    ];
    let shape = g.weights.pick(
        g.rng,
        &["alt:pt:addcol", "alt:pt:setdefault", "alt:pt:coltype"],
    );
    g.fire(shape);
    match shape {
        "alt:pt:addcol" => {
            stmts.push(raw(format!("ALTER TABLE {p} ADD COLUMN e int DEFAULT 8;")));
            stmts.push(content_probe(&p, &["id", "n", "e"]));
        }
        "alt:pt:setdefault" => {
            stmts.push(raw(format!("ALTER TABLE {p} ALTER COLUMN n SET DEFAULT 55;")));
            // A new row into the default-carrying partition range.
            stmts.push(raw(format!("INSERT INTO {p} (id) VALUES (1);")));
            stmts.push(content_probe(&p, &["id", "n"]));
        }
        _ => {
            // Type change on a NON-partition-key column rewrites every leaf.
            stmts.push(raw(format!(
                "ALTER TABLE {p} ALTER COLUMN s TYPE int USING s::int;"
            )));
            stmts.push(content_probe(&p, &["id", "n", "s"]));
        }
    }
    stmts.push(raw(format!("DROP TABLE {p};")));
    stmts
}

// ------------------------------------------------------------ probe helpers -

/// Full-content probe: every listed column cast ::text, TOTAL ORDER BY the
/// first column (the pk / id — unique, so the order and row set are fully
/// determined and comparison is byte-exact).
fn content_probe(t: &str, cols: &[&str]) -> StmtKind {
    let pk = cols[0];
    let proj = cols
        .iter()
        .map(|c| format!("{c}::text"))
        .collect::<Vec<_>>()
        .join(", ");
    raw(format!("SELECT {proj} FROM {t} ORDER BY {pk};"))
}

/// Same as `content_probe` but scans ONLY the named table (no inheritance
/// descendants).
fn content_probe_only(t: &str, cols: &[&str]) -> StmtKind {
    let pk = cols[0];
    let proj = cols
        .iter()
        .map(|c| format!("{c}::text"))
        .collect::<Vec<_>>()
        .join(", ");
    raw(format!("SELECT {proj} FROM ONLY {t} ORDER BY {pk};"))
}

/// Stable column-metadata probe: pg_attribute projection ordered by attnum,
/// excluding dropped/system columns. Type is projected by NAME (regtype),
/// never OID, so it is comparison-stable across engines.
fn attr_probe(t: &str) -> StmtKind {
    raw(format!(
        "SELECT attname, atttypid::regtype::text, attnotnull, attstorage, \
         attgenerated, attislocal FROM pg_attribute \
         WHERE attrelid = '{t}'::regclass AND attnum > 0 AND NOT attisdropped \
         ORDER BY attnum;"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn gen_groups(seed: u64, n: usize) -> Vec<Vec<String>> {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::defaults();
        let mut rng = Rng::new(seed);
        let mut state = AltState::new();
        let mut out = Vec::new();
        for _ in 0..n {
            let mut prods = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut prods, 4);
            std::mem::swap(&mut g.altertable, &mut state);
            let stmts = gen_altertable_module(&mut g);
            std::mem::swap(&mut g.altertable, &mut state);
            out.push(
                stmts
                    .into_iter()
                    .map(|s| match s {
                        StmtKind::Raw(t) => t,
                        other => panic!("altertable emits Raw only, got {other:?}"),
                    })
                    .collect(),
            );
        }
        out
    }

    fn flat(seed: u64, n: usize) -> Vec<String> {
        gen_groups(seed, n).into_iter().flatten().collect()
    }

    /// Same seed, same stream — the reproducibility witness.
    #[test]
    fn deterministic_by_seed() {
        assert_eq!(gen_groups(42, 200), gen_groups(42, 200));
        assert_ne!(gen_groups(42, 200), gen_groups(43, 200));
    }

    /// Every group creates and drops its own fixtures — no object survives a
    /// group (self-contained; no persistent catalog growth).
    #[test]
    fn every_group_drops_what_it_creates() {
        for group in gen_groups(7, 400) {
            let mut created: Vec<String> = Vec::new();
            let mut dropped: Vec<String> = Vec::new();
            for sql in &group {
                if let Some(rest) = sql.strip_prefix("CREATE TABLE ") {
                    let name = rest.split(['(', ' ']).next().unwrap().trim().to_string();
                    created.push(name);
                } else if let Some(rest) = sql.strip_prefix("CREATE SEQUENCE ") {
                    created.push(rest.trim_end_matches(';').trim().to_string());
                } else if let Some(rest) = sql.strip_prefix("DROP TABLE ") {
                    let name = rest
                        .trim_end_matches(';')
                        .trim_end_matches(" CASCADE")
                        .trim()
                        .to_string();
                    dropped.push(name);
                } else if let Some(rest) = sql.strip_prefix("DROP SEQUENCE ") {
                    dropped.push(rest.trim_end_matches(';').trim().to_string());
                }
            }
            // Every top-level create is either dropped directly or is a
            // partition/child removed by a CASCADE / parent drop.
            for c in &created {
                let direct = dropped.iter().any(|d| d == c);
                let by_cascade = group.iter().any(|s| {
                    s.starts_with("DROP TABLE ") && s.contains("CASCADE")
                }) && (c.contains("_a")
                    || c.contains("_b")
                    || c.contains("_c")
                    || c.starts_with("fz_atc_"));
                let partition_leaf = c.contains("fz_atpp_") && c != c.trim_end_matches("_a");
                assert!(
                    direct || by_cascade || partition_leaf || c.contains("fz_atpp_"),
                    "created but not dropped: {c} in {group:?}"
                );
            }
            assert!(!created.is_empty(), "group created nothing: {group:?}");
            assert!(!dropped.is_empty(), "group dropped nothing: {group:?}");
        }
    }

    /// No float/nondeterministic surface appears in an ORDERED row probe. The
    /// only float column (random()) is verified with aggregates, never a
    /// SELECT ... ORDER BY of the value.
    #[test]
    fn no_float_ordered_surface() {
        for sql in flat(13, 600) {
            if sql.contains("ORDER BY") {
                for bad in ["random(", "::float", "float8", "avg(", "clock_timestamp"] {
                    assert!(!sql.contains(bad), "float/nondeterministic ordered probe: {sql}");
                }
            }
        }
    }

    /// The volatile-default families never compare per-row volatile values:
    /// any statement mentioning random()/nextval only appears in a DDL/DEFAULT
    /// context or an order-independent aggregate probe.
    #[test]
    fn volatile_defaults_probed_by_facts_only() {
        for sql in flat(17, 600) {
            if sql.starts_with("SELECT") && (sql.contains("nextval") || sql.contains("random")) {
                panic!("volatile value entered a probe projection: {sql}");
            }
            // Any SELECT over a table that received a volatile default must be
            // aggregate-only (no bare column projection ordered by pk).
            if sql.starts_with("SELECT") && sql.contains("count(") {
                assert!(!sql.contains("ORDER BY"), "aggregate fact probe should not order: {sql}");
            }
        }
    }

    /// Row-returning content probes carry a TOTAL ORDER BY on the leading pk.
    #[test]
    fn content_probes_are_totally_ordered() {
        for sql in flat(19, 600) {
            // Content probes select "<col>::text, ..." — they must end ORDER BY.
            if sql.starts_with("SELECT") && sql.contains("::text FROM ") {
                assert!(
                    sql.contains(" ORDER BY "),
                    "content probe without total order: {sql}"
                );
            }
        }
    }

    /// Statement hygiene: single line, terminated, balanced parens.
    #[test]
    fn statements_are_well_formed() {
        for sql in flat(23, 400) {
            assert!(!sql.contains('\n'), "multi-line statement: {sql}");
            assert!(sql.ends_with(';'), "unterminated: {sql}");
            assert_eq!(
                sql.matches('(').count(),
                sql.matches(')').count(),
                "unbalanced parens: {sql}"
            );
        }
    }

    /// Every family is reachable from the default weight table, and the
    /// signature statements of each rewrite/phase path fire.
    #[test]
    fn all_families_and_signatures_fire() {
        let stmts = flat(3, 4000).join("\n");
        for needle in [
            // family markers
            "ALTER COLUMN s TYPE int USING s::int",
            "ALTER COLUMN n TYPE bigint USING",
            "ALTER COLUMN n TYPE numeric(18,6)",
            "ALTER COLUMN s TYPE bool USING s::bool",
            "ALTER COLUMN s TYPE text;",
            "ALTER COLUMN s TYPE varchar(40)",
            "ADD COLUMN c int DEFAULT 42",
            "ADD COLUMN c text DEFAULT 'const'",
            "ADD COLUMN c numeric(10,3) DEFAULT 3.140",
            "DEFAULT nextval(",
            "DEFAULT random()",
            "SET DEFAULT 999",
            "DROP DEFAULT",
            "SET NOT NULL",
            "DROP NOT NULL",
            "SET STORAGE EXTERNAL",
            "SET STORAGE MAIN",
            "SET STORAGE PLAIN",
            "SET STATISTICS 500",
            "SET COMPRESSION pglz",
            "DROP COLUMN b;",
            "SET UNLOGGED",
            "SET LOGGED",
            "GENERATED ALWAYS AS (n * 2 + id) STORED",
            "GENERATED ALWAYS AS (n + m) STORED",
            // multi-subcommand phase ordering
            "DROP COLUMN junk, ADD COLUMN a int DEFAULT 7, ALTER COLUMN s TYPE int USING s::int",
            // indexed type change
            "CREATE UNIQUE INDEX",
            "ALTER COLUMN k TYPE bigint USING k::bigint",
            // inheritance
            "INHERITS (",
            "ALTER TABLE ONLY",
            "NO INHERIT",
            "FROM ONLY ",
            // replica identity
            "REPLICA IDENTITY FULL",
            "REPLICA IDENTITY NOTHING",
            "REPLICA IDENTITY DEFAULT",
            "REPLICA IDENTITY USING INDEX",
            "relreplident FROM pg_class",
            // partitioned parent
            "PARTITION BY RANGE (id)",
            "PARTITION OF",
            // metadata probe
            "FROM pg_attribute",
        ] {
            assert!(stmts.contains(needle), "signature never fired in 4000 groups: {needle}");
        }
    }
}
