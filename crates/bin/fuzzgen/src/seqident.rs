//! Sequences + GENERATED IDENTITY + serial drain module.
//!
//! Track-B SQL-drainable surface untouched by the earlier waves: the
//! sequence generator (backend/commands/sequence.c) and the identity/serial
//! machinery in parse_utilcmd.c + tablecmds.c. Each group is self-contained
//! (create, exercise, capture, drop — the plpg/coll discipline): no
//! cross-group edges, name counters live in `SeqState` so names stay
//! session-unique even when a drop was pre-empted by an earlier error.
//!
//! Determinism argument (the load-bearing invariant for a differential
//! sequence workload):
//!   - Sequence values are DETERMINISTIC within a single backend session:
//!     `nextval` yields start, start+increment, ... regardless of CACHE
//!     (the cache only changes how many values one backend pre-allocates;
//!     the same backend then consumes them in order, so a single session
//!     sees an unbroken run). Both differential sides run the identical
//!     statement stream in one session each, so the value streams line up.
//!   - Multi-row `nextval` captures are taken as
//!     `SELECT nextval('s') AS v FROM generate_series(1, N) ORDER BY v` so
//!     the OUTPUT is a total-ordered multiset — never dependent on the
//!     per-row evaluation order of the target-list function. Scalar probes
//!     (currval/lastval/setval, single nextval) are one-row and compare
//!     positionally.
//!   - `currval` on a freshly-created sequence that has had no `nextval`
//!     this group reliably raises 55000 (object_not_in_prerequisite_state)
//!     — a per-sequence property independent of session history, so it is
//!     the reliable currval-not-yet arm. `lastval` is session-global
//!     (defined by ANY prior nextval in the session), so it is exercised
//!     best-effort: both sides share the session state in lockstep, so
//!     whichever way it resolves it resolves identically.
//!   - Boundary/overflow arms drive `nextval` to MAXVALUE (asc) / MINVALUE
//!     (desc) with explicit single `nextval` calls: the exhausting call
//!     raises 2200H (sequence_generator_limit_exceeded) under NO CYCLE, or
//!     wraps under CYCLE. Error statements are standalone (module groups
//!     are never inside txn brackets, so autocommit isolates each error;
//!     the group's later DROPs still run).
//!   - IDENTITY: a GENERATED ALWAYS column rejects a non-DEFAULT INSERT
//!     without OVERRIDING SYSTEM VALUE (428C9, ERRCODE_GENERATED_ALWAYS);
//!     the reject probe is standalone. Captured rows are read back
//!     `ORDER BY` the identity column.
//!
//! No hazards are hardcoded: the differ captures both engines' SQLSTATE +
//! message at runtime and compares. Error arms are targets, never dropped.

use crate::stmt::{Gen, StmtKind};

/// Session-persistent name counters (objects themselves are group-local).
#[derive(Clone, Debug, Default)]
pub struct SeqState {
    next_seq: u32,
    next_tab: u32,
}

impl SeqState {
    pub fn new() -> SeqState {
        SeqState::default()
    }
    fn seq(&mut self) -> String {
        let n = self.next_seq;
        self.next_seq += 1;
        format!("fz_si_s_{}", n)
    }
    fn tab(&mut self) -> String {
        let n = self.next_tab;
        self.next_tab += 1;
        format!("fz_si_t_{}", n)
    }
}

/// Per-type ascending upper bound (the sequence default MAXVALUE for a
/// positive increment) and lower bound, keyed by the SQL type name used in
/// `AS` / `serial`. Values are the exact SMALLINT/INT/BIGINT extents.
struct TypeInfo {
    /// SQL type name for `CREATE SEQUENCE ... AS <sql>`.
    sql: &'static str,
    /// serial spelling that desugars to this type.
    serial: &'static str,
    max: &'static str,
}

const SEQ_TYPES: &[TypeInfo] = &[
    TypeInfo { sql: "smallint", serial: "smallserial", max: "32767" },
    TypeInfo { sql: "integer", serial: "serial", max: "2147483647" },
    TypeInfo { sql: "bigint", serial: "bigserial", max: "9223372036854775807" },
];

fn pick_type(g: &mut Gen) -> &'static TypeInfo {
    &SEQ_TYPES[g.rng.below_usize(SEQ_TYPES.len())]
}

/// Small increment magnitude for capture groups (keeps the nextval stream
/// human-readable and int-safe; every value stays well inside int4).
fn small_inc(g: &mut Gen) -> i64 {
    *g.rng.pick(&[1i64, 1, 2, 3, 5, 10])
}

/// Registry entry point (stmt::STMT_MODULES). One self-contained sequence /
/// identity / serial group per pick.
pub fn gen_seqident_module(g: &mut Gen) -> Vec<StmtKind> {
    g.fire("seqident");
    let form = g.weights.pick(
        g.rng,
        &[
            "seqident:create",
            "seqident:astype",
            "seqident:cycle",
            "seqident:setval",
            "seqident:currval",
            "seqident:alter",
            "seqident:identity",
            "seqident:serial",
            "seqident:altercol",
            "seqident:owned",
            "seqident:dropdep",
        ],
    );
    g.fire(form);
    match form {
        "seqident:create" => gen_create_group(g),
        "seqident:astype" => gen_astype_group(g),
        "seqident:cycle" => gen_cycle_group(g),
        "seqident:setval" => gen_setval_group(g),
        "seqident:currval" => gen_currval_group(g),
        "seqident:alter" => gen_alter_group(g),
        "seqident:identity" => gen_identity_group(g),
        "seqident:serial" => gen_serial_group(g),
        "seqident:altercol" => gen_altercol_group(g),
        "seqident:owned" => gen_owned_group(g),
        _ => gen_dropdep_group(g),
    }
}

fn raw(s: String) -> StmtKind {
    StmtKind::Raw(s)
}

/// CREATE SEQUENCE with a coherent, randomly-selected option set (INCREMENT,
/// MIN/MAXVALUE, START, RESTART, CACHE, CYCLE/NO CYCLE), then a total-ordered
/// nextval stream capture plus currval and a direct catalog read of the
/// sequence relation's state row.
fn gen_create_group(g: &mut Gen) -> Vec<StmtKind> {
    let s = g.seq.seq();
    let ascending = g.weights.pick(g.rng, &["seqident:asc", "seqident:desc"]) == "seqident:asc";
    g.fire(if ascending { "seqident:asc" } else { "seqident:desc" });
    let inc = small_inc(g);
    let cache = if g.weights.pick(g.rng, &["seqident:cache1", "seqident:cachehi"])
        == "seqident:cachehi"
    {
        g.fire("seqident:cachehi");
        *g.rng.pick(&[2i64, 4, 8, 20])
    } else {
        g.fire("seqident:cache1");
        1
    };
    let cycle = g.rng.chance(1, 2);
    // Coherent bounds: a small window so a modest generate_series can be
    // read against it. For ascending, [lo, hi] with start=lo; descending,
    // [lo, hi] with start=hi and a negated increment.
    let lo = *g.rng.pick(&[1i64, 0, -5, 100]);
    let span = *g.rng.pick(&[10i64, 20, 50, 100]);
    let hi = lo + span;
    let mut opts = String::new();
    if ascending {
        opts.push_str(&format!(" INCREMENT BY {}", inc));
        opts.push_str(&format!(" MINVALUE {} MAXVALUE {}", lo, hi));
        opts.push_str(&format!(" START WITH {}", lo));
    } else {
        opts.push_str(&format!(" INCREMENT BY {}", -inc));
        opts.push_str(&format!(" MINVALUE {} MAXVALUE {}", lo, hi));
        opts.push_str(&format!(" START WITH {}", hi));
    }
    opts.push_str(&format!(" CACHE {}", cache));
    opts.push_str(if cycle { " CYCLE" } else { " NO CYCLE" });
    let n = 4 + g.rng.below_usize(8);
    let mut out = vec![raw(format!("CREATE SEQUENCE {}{};", s, opts))];
    // Optional RESTART before the stream (RESTART with no value re-seeds to
    // the original START; RESTART WITH re-seeds explicitly).
    if g.rng.chance(1, 3) {
        g.fire("seqident:restart");
        if g.rng.chance(1, 2) {
            let r = if ascending { lo } else { hi };
            out.push(raw(format!("ALTER SEQUENCE {} RESTART WITH {};", s, r)));
        } else {
            out.push(raw(format!("ALTER SEQUENCE {} RESTART;", s)));
        }
    }
    out.push(raw(format!(
        "SELECT nextval('{}') AS v FROM generate_series(1, {}) ORDER BY v;",
        s, n
    )));
    out.push(raw(format!("SELECT currval('{}');", s)));
    out.push(raw(format!(
        "SELECT last_value, is_called FROM {};",
        s
    )));
    out.push(raw(format!("DROP SEQUENCE {};", s)));
    out
}

/// CREATE SEQUENCE AS <int type> with DEFAULT bounds (so PG derives the
/// exact per-type MAXVALUE), then drive the upper boundary: setval to the
/// type max with is_called=false, nextval returns the max, the next nextval
/// exhausts the range (2200H under the implied NO CYCLE default).
fn gen_astype_group(g: &mut Gen) -> Vec<StmtKind> {
    let s = g.seq.seq();
    let t = pick_type(g);
    g.fire(match t.sql {
        "smallint" => "seqident:astype:small",
        "integer" => "seqident:astype:int",
        _ => "seqident:astype:big",
    });
    let mut out = vec![raw(format!("CREATE SEQUENCE {} AS {};", s, t.sql))];
    // First value is the derived START (=1 for a default ascending seq).
    out.push(raw(format!("SELECT nextval('{}');", s)));
    // Seed to max, not-yet-called: next value == max, then overflow.
    out.push(raw(format!("SELECT setval('{}', {}, false);", s, t.max)));
    out.push(raw(format!("SELECT nextval('{}');", s)));
    g.fire("seqident:overflow");
    out.push(raw(format!("SELECT nextval('{}');", s)));
    out.push(raw(format!("DROP SEQUENCE {};", s)));
    out
}

/// Small-range MAXVALUE with CYCLE and, in a second sibling sequence, NO
/// CYCLE: the CYCLE stream wraps to MINVALUE (deterministic multiset), the
/// NO CYCLE stream raises 2200H on the exhausting call. Both ascending and
/// descending wrap directions are covered.
fn gen_cycle_group(g: &mut Gen) -> Vec<StmtKind> {
    let sc = g.seq.seq();
    let sn = g.seq.seq();
    let ascending = g.weights.pick(g.rng, &["seqident:asc", "seqident:desc"]) == "seqident:asc";
    g.fire(if ascending { "seqident:asc" } else { "seqident:desc" });
    let hi = *g.rng.pick(&[3i64, 4, 5]);
    // CYCLE sequence: read past exhaustion; the wrap makes the multiset
    // repeat but stay deterministic under ORDER BY.
    let (create_c, create_n) = if ascending {
        (
            format!("CREATE SEQUENCE {} INCREMENT 1 MINVALUE 1 MAXVALUE {} START 1 CYCLE;", sc, hi),
            format!("CREATE SEQUENCE {} INCREMENT 1 MINVALUE 1 MAXVALUE {} START 1 NO CYCLE;", sn, hi),
        )
    } else {
        (
            format!(
                "CREATE SEQUENCE {} INCREMENT -1 MINVALUE 1 MAXVALUE {} START {} CYCLE;",
                sc, hi, hi
            ),
            format!(
                "CREATE SEQUENCE {} INCREMENT -1 MINVALUE 1 MAXVALUE {} START {} NO CYCLE;",
                sn, hi, hi
            ),
        )
    };
    let n = (hi as usize) + 3;
    g.fire("seqident:cycle:wrap");
    g.fire("seqident:cycle:err");
    vec![
        raw(create_c),
        raw(format!(
            "SELECT nextval('{}') AS v FROM generate_series(1, {}) ORDER BY v;",
            sc, n
        )),
        raw(format!("DROP SEQUENCE {};", sc)),
        raw(create_n),
        // Exhaust the NO CYCLE range: the (hi+1)-th call raises 2200H.
        raw(format!(
            "SELECT nextval('{}') AS v FROM generate_series(1, {}) ORDER BY v;",
            sn,
            hi + 1
        )),
        raw(format!("DROP SEQUENCE {};", sn)),
    ]
}

/// setval surface: 2-arg (implicit is_called=true) and 3-arg with both
/// is_called values, each followed by a nextval/currval capture that pins
/// the resulting state.
fn gen_setval_group(g: &mut Gen) -> Vec<StmtKind> {
    let s = g.seq.seq();
    let v1 = *g.rng.pick(&[10i64, 42, 100, 1000]);
    let v2 = *g.rng.pick(&[7i64, 50, 500]);
    let mut out = vec![raw(format!("CREATE SEQUENCE {} INCREMENT 1 MINVALUE 1;", s))];
    // 2-arg setval: is_called=true, so the NEXT nextval is v1+1.
    g.fire("seqident:setval:2arg");
    out.push(raw(format!("SELECT setval('{}', {});", s, v1)));
    out.push(raw(format!("SELECT currval('{}');", s)));
    out.push(raw(format!("SELECT nextval('{}');", s)));
    // 3-arg, is_called=false: the next nextval RETURNS v2 unchanged.
    g.fire("seqident:setval:3false");
    out.push(raw(format!("SELECT setval('{}', {}, false);", s, v2)));
    out.push(raw(format!("SELECT nextval('{}');", s)));
    // 3-arg, is_called=true: next nextval is v1+... advance.
    g.fire("seqident:setval:3true");
    out.push(raw(format!("SELECT setval('{}', {}, true);", s, v1)));
    out.push(raw(format!("SELECT nextval('{}');", s)));
    out.push(raw(format!("SELECT last_value, is_called FROM {};", s)));
    out.push(raw(format!("DROP SEQUENCE {};", s)));
    out
}

/// currval / lastval discipline: currval on a freshly-created (never
/// nextval'd) sequence reliably raises 55000; after a nextval, currval and
/// lastval both resolve. lastval is session-global so its pre-nextval
/// resolution is best-effort (identical on both sides regardless).
fn gen_currval_group(g: &mut Gen) -> Vec<StmtKind> {
    let s = g.seq.seq();
    g.fire("seqident:currval:notyet");
    vec![
        raw(format!("CREATE SEQUENCE {} INCREMENT 2 MINVALUE 1 START 3;", s)),
        // 55000: currval before any nextval this session for THIS sequence.
        raw(format!("SELECT currval('{}');", s)),
        raw(format!("SELECT nextval('{}');", s)),
        raw(format!("SELECT currval('{}');", s)),
        raw("SELECT lastval();".to_string()),
        raw(format!("SELECT nextval('{}');", s)),
        raw(format!("SELECT currval('{}'), lastval();", s)),
        raw(format!("DROP SEQUENCE {};", s)),
    ]
}

/// ALTER SEQUENCE across the option surface (INCREMENT, MIN/MAXVALUE,
/// CYCLE, CACHE, AS type, RESTART), reading a fresh total-ordered stream
/// after each shape change.
fn gen_alter_group(g: &mut Gen) -> Vec<StmtKind> {
    let s = g.seq.seq();
    let mut out = vec![raw(format!(
        "CREATE SEQUENCE {} INCREMENT 1 MINVALUE 1 MAXVALUE 100 START 1 CACHE 1;",
        s
    ))];
    out.push(raw(format!(
        "SELECT nextval('{}') AS v FROM generate_series(1, 3) ORDER BY v;",
        s
    )));
    g.fire("seqident:alter:incr");
    out.push(raw(format!(
        "ALTER SEQUENCE {} INCREMENT BY 5 MAXVALUE 1000 CACHE 4;",
        s
    )));
    out.push(raw(format!(
        "SELECT nextval('{}') AS v FROM generate_series(1, 3) ORDER BY v;",
        s
    )));
    g.fire("seqident:alter:restart");
    let r = *g.rng.pick(&[1i64, 50, 200]);
    out.push(raw(format!("ALTER SEQUENCE {} RESTART WITH {};", s, r)));
    out.push(raw(format!(
        "SELECT nextval('{}') AS v FROM generate_series(1, 3) ORDER BY v;",
        s
    )));
    g.fire("seqident:alter:cycle");
    out.push(raw(format!("ALTER SEQUENCE {} MAXVALUE 60 CYCLE;", s)));
    out.push(raw(format!("SELECT setval('{}', 59, true);", s)));
    out.push(raw(format!(
        "SELECT nextval('{}') AS v FROM generate_series(1, 4) ORDER BY v;",
        s
    )));
    g.fire("seqident:alter:astype");
    out.push(raw(format!("ALTER SEQUENCE {} AS smallint MAXVALUE 32767;", s)));
    out.push(raw(format!("SELECT last_value, is_called FROM {};", s)));
    out.push(raw(format!("DROP SEQUENCE {};", s)));
    out
}

/// GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY: DEFAULT inserts, the
/// OVERRIDING SYSTEM/USER VALUE arms, the ALWAYS non-DEFAULT reject
/// (428C9), and a total-ordered readback; plus ALTER COLUMN DROP IDENTITY.
fn gen_identity_group(g: &mut Gen) -> Vec<StmtKind> {
    let t = g.seq.tab();
    let always =
        g.weights.pick(g.rng, &["seqident:id:always", "seqident:id:bydefault"]) == "seqident:id:always";
    g.fire(if always { "seqident:id:always" } else { "seqident:id:bydefault" });
    let clause = if always { "GENERATED ALWAYS AS IDENTITY" } else { "GENERATED BY DEFAULT AS IDENTITY" };
    // Optional sequence-option payload on the identity column.
    let seqopts = if g.rng.chance(1, 2) {
        g.fire("seqident:id:seqopts");
        " (START WITH 10 INCREMENT BY 10)"
    } else {
        ""
    };
    let mut out = vec![raw(format!(
        "CREATE TABLE {} (id int {}{}, val text);",
        t, clause, seqopts
    ))];
    // DEFAULT-driven inserts.
    out.push(raw(format!("INSERT INTO {} (val) VALUES ('a'), ('b'), ('c');", t)));
    out.push(raw(format!("INSERT INTO {} (id, val) VALUES (DEFAULT, 'd');", t)));
    if always {
        // 428C9: a non-DEFAULT value into a GENERATED ALWAYS column with no
        // OVERRIDING SYSTEM VALUE.
        g.fire("seqident:id:reject");
        out.push(raw(format!("INSERT INTO {} (id, val) VALUES (500, 'x');", t)));
        // OVERRIDING SYSTEM VALUE forces the user value through.
        g.fire("seqident:id:overriding_system");
        out.push(raw(format!(
            "INSERT INTO {} (id, val) OVERRIDING SYSTEM VALUE VALUES (500, 'sys');",
            t
        )));
    } else {
        // BY DEFAULT: user value accepted directly; OVERRIDING USER VALUE
        // forces the sequence value instead.
        g.fire("seqident:id:userval");
        out.push(raw(format!("INSERT INTO {} (id, val) VALUES (500, 'usr');", t)));
        g.fire("seqident:id:overriding_user");
        out.push(raw(format!(
            "INSERT INTO {} (id, val) OVERRIDING USER VALUE VALUES (700, 'ovr');",
            t
        )));
    }
    out.push(raw(format!("SELECT id, val FROM {} ORDER BY id, val;", t)));
    // The identity-owned sequence is addressable via pg_get_serial_sequence.
    out.push(raw(format!(
        "SELECT pg_get_serial_sequence('{}', 'id') IS NOT NULL;",
        t
    )));
    // Drop the identity property, then the table.
    g.fire("seqident:id:dropidentity");
    out.push(raw(format!("ALTER TABLE {} ALTER COLUMN id DROP IDENTITY;", t)));
    out.push(raw(format!("SELECT count(*) FROM {};", t)));
    out.push(raw(format!("DROP TABLE {};", t)));
    out
}

/// serial / bigserial / smallserial desugaring: the implicit owned
/// sequence, DEFAULT-nextval inserts, a total-ordered readback, and a
/// direct probe of the auto-named `<tab>_<col>_seq` relation.
fn gen_serial_group(g: &mut Gen) -> Vec<StmtKind> {
    let t = g.seq.tab();
    let ti = pick_type(g);
    g.fire(match ti.serial {
        "smallserial" => "seqident:serial:small",
        "serial" => "seqident:serial:int",
        _ => "seqident:serial:big",
    });
    let seqname = format!("{}_id_seq", t);
    let mut out = vec![raw(format!(
        "CREATE TABLE {} (id {} PRIMARY KEY, val text);",
        t, ti.serial
    ))];
    out.push(raw(format!("INSERT INTO {} (val) VALUES ('a'), ('b'), ('c'), ('d');", t)));
    // Explicit id insert past the sequence, then reset the sequence over
    // the max — the classic serial catch-up pattern.
    out.push(raw(format!("INSERT INTO {} (id, val) VALUES (100, 'e');", t)));
    g.fire("seqident:serial:setvalmax");
    out.push(raw(format!(
        "SELECT setval('{}', (SELECT max(id) FROM {}));",
        seqname, t
    )));
    out.push(raw(format!("INSERT INTO {} (val) VALUES ('f');", t)));
    out.push(raw(format!("SELECT id, val FROM {} ORDER BY id, val;", t)));
    // The owned sequence's own state row.
    out.push(raw(format!("SELECT is_called FROM {};", seqname)));
    out.push(raw(format!(
        "SELECT pg_get_serial_sequence('{}', 'id');",
        t
    )));
    out.push(raw(format!("DROP TABLE {};", t)));
    out
}

/// ALTER TABLE ... ALTER COLUMN ADD/SET/DROP IDENTITY on a pre-existing
/// plain column (the tablecmds identity arms distinct from CREATE-time
/// identity): add identity, insert, flip ALWAYS<->BY DEFAULT, drop with IF
/// EXISTS.
fn gen_altercol_group(g: &mut Gen) -> Vec<StmtKind> {
    let t = g.seq.tab();
    let mut out = vec![raw(format!("CREATE TABLE {} (id int NOT NULL, val text);", t))];
    g.fire("seqident:altercol:add");
    out.push(raw(format!(
        "ALTER TABLE {} ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (START WITH 5);",
        t
    )));
    out.push(raw(format!("INSERT INTO {} (val) VALUES ('a'), ('b');", t)));
    g.fire("seqident:altercol:setgen");
    out.push(raw(format!(
        "ALTER TABLE {} ALTER COLUMN id SET GENERATED ALWAYS;",
        t
    )));
    // Now ALWAYS: a non-DEFAULT insert rejects (428C9).
    g.fire("seqident:altercol:reject");
    out.push(raw(format!("INSERT INTO {} (id, val) VALUES (99, 'x');", t)));
    out.push(raw(format!(
        "INSERT INTO {} (id, val) OVERRIDING SYSTEM VALUE VALUES (99, 'y');",
        t
    )));
    out.push(raw(format!("SELECT id, val FROM {} ORDER BY id, val;", t)));
    // SET the sequence-option payload of the identity, then drop identity.
    g.fire("seqident:altercol:restart");
    out.push(raw(format!(
        "ALTER TABLE {} ALTER COLUMN id RESTART WITH 50;",
        t
    )));
    out.push(raw(format!("INSERT INTO {} (val) VALUES ('z');", t)));
    out.push(raw(format!("SELECT id, val FROM {} ORDER BY id, val;", t)));
    g.fire("seqident:altercol:dropif");
    out.push(raw(format!(
        "ALTER TABLE {} ALTER COLUMN id DROP IDENTITY IF EXISTS;",
        t
    )));
    // Second DROP IDENTITY IF EXISTS is now a no-op (not an error).
    out.push(raw(format!(
        "ALTER TABLE {} ALTER COLUMN id DROP IDENTITY IF EXISTS;",
        t
    )));
    out.push(raw(format!("DROP TABLE {};", t)));
    out
}

/// CREATE SEQUENCE ... OWNED BY and the ownership-cascade dependency: the
/// owned sequence is removed when its owning table (column) is dropped.
/// Probed by pg_class relkind='S' counts before and after DROP TABLE.
fn gen_owned_group(g: &mut Gen) -> Vec<StmtKind> {
    let s = g.seq.seq();
    let t = g.seq.tab();
    g.fire("seqident:owned:cascade");
    vec![
        raw(format!("CREATE TABLE {} (id int, val text);", t)),
        raw(format!("CREATE SEQUENCE {} OWNED BY {}.id;", s, t)),
        raw(format!(
            "ALTER TABLE {} ALTER COLUMN id SET DEFAULT nextval('{}');",
            t, s
        )),
        raw(format!("INSERT INTO {} (val) VALUES ('a'), ('b');", t)),
        raw(format!("SELECT id, val FROM {} ORDER BY id, val;", t)),
        raw(format!(
            "SELECT count(*) FROM pg_class WHERE relname = '{}' AND relkind = 'S';",
            s
        )),
        // Dropping the owning table cascades the owned sequence away.
        raw(format!("DROP TABLE {};", t)),
        raw(format!(
            "SELECT count(*) FROM pg_class WHERE relname = '{}' AND relkind = 'S';",
            s
        )),
        // Idempotent cleanup (the sequence is already gone).
        raw(format!("DROP SEQUENCE IF EXISTS {};", s)),
    ]
}

/// DROP SEQUENCE dependency behaviour: a sequence referenced by a column
/// DEFAULT cannot be dropped without CASCADE (2BP01,
/// dependent_objects_still_exist); CASCADE removes the default too.
fn gen_dropdep_group(g: &mut Gen) -> Vec<StmtKind> {
    let s = g.seq.seq();
    let t = g.seq.tab();
    g.fire("seqident:dropdep:restrict");
    let mut out = vec![
        raw(format!("CREATE SEQUENCE {};", s)),
        raw(format!(
            "CREATE TABLE {} (id int DEFAULT nextval('{}'), val text);",
            t, s
        )),
        raw(format!("INSERT INTO {} (val) VALUES ('a');", t)),
        // Plain DROP raises 2BP01: the column default still depends on it.
        raw(format!("DROP SEQUENCE {};", s)),
    ];
    g.fire("seqident:dropdep:cascade");
    out.push(raw(format!("DROP SEQUENCE {} CASCADE;", s)));
    // Default is gone; a further insert leaves id NULL.
    out.push(raw(format!("INSERT INTO {} (val) VALUES ('b');", t)));
    out.push(raw(format!("SELECT id, val FROM {} ORDER BY val;", t)));
    out.push(raw(format!("DROP TABLE {};", t)));
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogSource, FixtureCatalog};
    use crate::rng::Rng;
    use crate::weights::WeightTable;

    fn gen_groups(seed: u64, n: usize, spec: &str) -> (Vec<Vec<String>>, Vec<String>) {
        let cat = FixtureCatalog.load_catalog().unwrap();
        let w = WeightTable::parse(spec).unwrap();
        let mut rng = Rng::new(seed);
        let mut state = SeqState::new();
        let mut groups = Vec::new();
        let mut prods = Vec::new();
        for _ in 0..n {
            let mut p = Vec::new();
            let mut g = Gen::new(&mut rng, &cat, &w, &mut p, 3);
            std::mem::swap(&mut g.seq, &mut state);
            let kinds = gen_seqident_module(&mut g);
            std::mem::swap(&mut g.seq, &mut state);
            assert!(!kinds.is_empty());
            groups.push(kinds.iter().map(|k| k.to_sql()).collect());
            prods.extend(p);
        }
        (groups, prods)
    }

    #[test]
    fn seqident_is_deterministic() {
        let (a, _) = gen_groups(31, 400, "");
        let (b, _) = gen_groups(31, 400, "");
        assert_eq!(a, b);
        let (c, _) = gen_groups(32, 400, "");
        assert_ne!(a, c);
    }

    /// Statement shape invariants (mirrors the registry-wide gate) plus
    /// group self-containment: every created object is dropped within its
    /// group (owned/dropdep sequences use IF EXISTS / CASCADE, so they are
    /// checked by create/drop-name balance, not literal DROP SEQUENCE).
    #[test]
    fn seqident_groups_well_formed() {
        let (groups, _) = gen_groups(0x51, 600, "");
        for group in &groups {
            for sql in group {
                assert!(!sql.contains('\n'), "multi-line statement: {sql}");
                assert!(sql.ends_with(';'), "unterminated: {sql}");
                assert_eq!(
                    sql.matches('(').count(),
                    sql.matches(')').count(),
                    "unbalanced parens: {sql}"
                );
            }
            let hay = group.join("\n");
            // Every CREATE TABLE has a matching DROP TABLE in-group.
            assert_eq!(
                hay.matches("CREATE TABLE ").count(),
                hay.matches("DROP TABLE ").count(),
                "unbalanced CREATE/DROP TABLE in group:\n{hay}"
            );
            // Every CREATE SEQUENCE is cleaned up by at least one DROP
            // SEQUENCE in-group (dropdep deliberately drops twice: a
            // RESTRICT attempt then CASCADE).
            assert!(
                hay.matches("DROP SEQUENCE ").count()
                    >= hay.matches("CREATE SEQUENCE ").count(),
                "CREATE SEQUENCE without in-group DROP:\n{hay}"
            );
        }
    }

    #[test]
    fn seqident_variety() {
        let (groups, prods) = gen_groups(0x5EED, 1600, "");
        let all: Vec<String> = groups.iter().flatten().cloned().collect();
        let hay = all.join("\n");
        for frag in [
            "CREATE SEQUENCE fz_si_s_",
            "INCREMENT BY",
            "MINVALUE",
            "MAXVALUE",
            "START WITH",
            "CACHE",
            " CYCLE",
            " NO CYCLE",
            "ALTER SEQUENCE fz_si_s_",
            "RESTART WITH",
            "RESTART;",
            "AS smallint",
            "SELECT nextval('fz_si_s_",
            "generate_series(1,",
            "ORDER BY v;",
            "SELECT currval('fz_si_s_",
            "SELECT lastval();",
            "SELECT setval('fz_si_s_",
            ", false);",
            ", true);",
            "last_value, is_called",
            "GENERATED ALWAYS AS IDENTITY",
            "GENERATED BY DEFAULT AS IDENTITY",
            "OVERRIDING SYSTEM VALUE",
            "OVERRIDING USER VALUE",
            "ALTER COLUMN id DROP IDENTITY",
            "ALTER COLUMN id ADD GENERATED",
            "SET GENERATED ALWAYS",
            "DROP IDENTITY IF EXISTS",
            "ALTER COLUMN id RESTART WITH",
            "id smallserial",
            "id serial",
            "id bigserial",
            "_id_seq",
            "pg_get_serial_sequence(",
            "OWNED BY",
            "relkind = 'S'",
            "DROP SEQUENCE IF EXISTS",
            " CASCADE;",
        ] {
            assert!(hay.contains(frag), "seqident flavor {frag:?} never generated");
        }
        for p in [
            "seqident:create",
            "seqident:astype",
            "seqident:cycle",
            "seqident:setval",
            "seqident:currval",
            "seqident:alter",
            "seqident:identity",
            "seqident:serial",
            "seqident:altercol",
            "seqident:owned",
            "seqident:dropdep",
            "seqident:asc",
            "seqident:desc",
            "seqident:overflow",
            "seqident:cycle:wrap",
            "seqident:cycle:err",
            "seqident:setval:2arg",
            "seqident:setval:3false",
            "seqident:currval:notyet",
            "seqident:id:always",
            "seqident:id:bydefault",
            "seqident:id:reject",
            "seqident:serial:setvalmax",
            "seqident:owned:cascade",
            "seqident:dropdep:restrict",
            "seqident:dropdep:cascade",
        ] {
            assert!(prods.iter().any(|q| q == p), "production {p} never fired");
        }
    }

    /// Sequence names never escape the fixed prefix (session-unique via the
    /// SeqState counters).
    #[test]
    fn seqident_names_are_prefixed() {
        let (groups, _) = gen_groups(0x99, 400, "");
        for group in &groups {
            for sql in group {
                if sql.starts_with("CREATE SEQUENCE ") {
                    assert!(
                        sql.contains("fz_si_s_"),
                        "sequence name outside prefix: {sql}"
                    );
                }
            }
        }
    }
}
