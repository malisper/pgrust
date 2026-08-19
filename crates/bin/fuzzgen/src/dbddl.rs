//! Database + tablespace DDL differential suite (lane Q8, database-ddl
//! chunk of docs/fuzzing/sql-reachable-queue.tsv).
//!
//! CREATE/ALTER/DROP DATABASE option breadth (dbcommands.c), tablespace
//! DDL (tablespace.c) + ALTER ... SET TABLESPACE (tablecmds.c block-copy
//! paths), busy-db / WITH (FORCE) / connection-limit arms, plus the
//! observation brackets (debug_print_parse=on → outfuncs,
//! compute_query_id=on → queryjumblefuncs) and a forced-extended-protocol
//! replay (plancache raw-tree copyObject → copyfuncs).
//!
//! Unlike the stream modules this suite owns its CONNECTIONS: database DDL
//! has to run outside transaction blocks, needs second sessions (busy-db
//! error, WITH (FORCE) termination, connection-limit rejects) and fresh
//! connections INTO the created databases to observe per-database state
//! (ALTER DATABASE SET, encoding). It also owns per-side tablespace
//! DIRECTORIES: two live clusters cannot share one tablespace dir, so the
//! suite creates a scratch dir per side and the CREATE TABLESPACE
//! statement TEXT differs between the sides. Every per-side path is
//! rewritten to the `<TSDIR>` placeholder in both the recorded SQL and
//! both outcomes before classification, so the path difference can never
//! itself be an asymmetry.
//!
//! DETERMINISM/ISOLATION disciplines (charter): databases/tablespaces/
//! roles are uniquely named from the suite tag and always dropped in the
//! same run (best-effort sweep at the end even after failures);
//! template0/template1/postgres and the connected database are never
//! touched; CREATE DATABASE never runs inside an explicit transaction
//! (the in-txn-block reject is itself one matched-error case). Catalog
//! probes project to stable pg_database/pg_tablespace columns — never
//! datfrozenxid/oid counters; pg_tablespace_location projects to a
//! non-emptiness predicate (per-side paths). xid-counter-, oid-counter-
//! and path-valued outputs are all engine-local environment, not surface.

use crate::diff::{classify, DiffClass, DiffInput, StmtOutcome};
use crate::ruled::{apply_ruled, RuledEntry};
use crate::runner::{ClientExecutor, Executor, Record};

/// One side's connection coordinates (the suite opens its own sessions).
#[derive(Clone, Debug)]
pub struct ConnSpec {
    pub host: String,
    pub port: u16,
    pub db: String,
    pub user: String,
    /// Scratch directory for this side's tablespaces (created by the
    /// suite; unique per side and per tag).
    pub tsdir: String,
}

#[derive(Default, Debug)]
pub struct DbDdlStats {
    pub cases: u32,
    pub matches: u32,
    pub ruled: u32,
    pub findings: u32,
}

/// Placeholder substituted for per-side scratch paths in recorded SQL and
/// outcomes.
const TSDIR: &str = "<TSDIR>";

/// xproto seed with bit 63 set: every statement rides the extended
/// protocol (Parse/Bind/Execute) — the plancache copyObject vehicle.
const XPROTO_FORCED: u64 = 1u64 << 63;

/// CREATE TABLESPACE requires its LOCATION directory to pre-exist: create
/// the scratch root plus every subdir the deck's LOCATION clauses name
/// (t1; t9 is only ever used inside a rolled-back txn; tobs for the
/// observation deck).
fn make_ts_dirs(root: &str) -> Result<(), String> {
    for sub in ["t1", "t9", "tobs"] {
        let p = std::path::Path::new(root).join(sub);
        std::fs::create_dir_all(&p).map_err(|e| format!("mkdir {}: {e}", p.display()))?;
    }
    Ok(())
}

fn norm_str(s: &str, needles: &[&str]) -> String {
    let mut out = s.to_string();
    for n in needles {
        out = out.replace(n, TSDIR);
    }
    out
}

fn norm_outcome(o: &StmtOutcome, needles: &[&str]) -> StmtOutcome {
    match o {
        StmtOutcome::Error { sqlstate, message } => StmtOutcome::Error {
            sqlstate: sqlstate.clone(),
            message: norm_str(message, needles),
        },
        StmtOutcome::Rows { col_oids, rows } => StmtOutcome::Rows {
            col_oids: col_oids.clone(),
            rows: rows
                .iter()
                .map(|r| r.iter().map(|c| c.as_ref().map(|v| norm_str(v, needles))).collect())
                .collect(),
        },
        other => other.clone(),
    }
}

/// A connect failure surfaced as an outcome, so "connection refused by
/// policy" (ALLOW_CONNECTIONS false, CONNECTION LIMIT) is comparable
/// between the sides like any other error. The wire error string carries
/// the server's FATAL message; per-side noise (pids, paths) is normalized
/// by the caller's needle list.
fn connect_outcome(spec: &ConnSpec, db: &str) -> (Option<ClientExecutor>, StmtOutcome) {
    match ClientExecutor::connect(&spec.host, spec.port, db, &spec.user) {
        Ok(ex) => (Some(ex), StmtOutcome::Command { tag: "CONNECT".to_string(), affected: None }),
        Err(e) => (None, StmtOutcome::Error { sqlstate: "XXCON".to_string(), message: e }),
    }
}

/// Suite context for one side.
struct Side {
    spec: ConnSpec,
    main: ClientExecutor,
}

pub struct DbDdlSuite {
    a: Side,
    b: Side,
    /// Unique name tag (db/role/tablespace names embed it).
    tag: String,
    needles_a: Vec<String>,
    needles_b: Vec<String>,
}

impl DbDdlSuite {
    pub fn connect(a: &ConnSpec, b: &ConnSpec, tag: u32) -> Result<DbDdlSuite, String> {
        make_ts_dirs(&a.tsdir)?;
        make_ts_dirs(&b.tsdir)?;
        let ma = ClientExecutor::connect(&a.host, a.port, &a.db, &a.user)
            .map_err(|e| format!("A: {e}"))?;
        let mb = ClientExecutor::connect(&b.host, b.port, &b.db, &b.user)
            .map_err(|e| format!("B: {e}"))?;
        Ok(DbDdlSuite {
            a: Side { spec: a.clone(), main: ma },
            b: Side { spec: b.clone(), main: mb },
            tag: format!("q8t{tag}"),
            needles_a: vec![a.tsdir.clone()],
            needles_b: vec![b.tsdir.clone()],
        })
    }
}

/// Names used by the deck for one tag.
struct Names {
    db1: String,
    db2: String,
    dbren: String,
    dbmov: String,
    dbfc: String,
    role: String,
    ts1: String,
    ts2: String,
}

fn names(tag: &str) -> Names {
    Names {
        db1: format!("fz_db_{tag}_a"),
        db2: format!("fz_db_{tag}_b"),
        dbren: format!("fz_db_{tag}_ren"),
        dbmov: format!("fz_db_{tag}_mov"),
        dbfc: format!("fz_db_{tag}_fc"),
        role: format!("fz_dbown_{tag}"),
        ts1: format!("fz_ts_{tag}_1"),
        ts2: format!("fz_ts_{tag}_2"),
    }
}

/// The deck: mostly side-symmetric statements; `TSDIR` in a statement is
/// replaced per side with that side's scratch dir. `probe_db` entries run
/// their SQL on a FRESH connection to the named database.
enum Step {
    /// Symmetric statement on the main connections.
    Sql(&'static str),
    /// Owned (formatted) symmetric statement.
    SqlOwned(String),
    /// Fresh-connection probe: (database, statement). A failed connect is
    /// itself the compared outcome.
    ProbeDb(String, String),
    /// Busy-db bracket: hold a session into the db, run the statement on
    /// the main connections, drop the held session.
    WithHeldConn(String, String),
}

fn deck(n: &Names) -> Vec<Step> {
    use Step::*;
    let mut v: Vec<Step> = Vec::new();
    let stable_dbrow = |db: &str| {
        format!(
            "SELECT pg_encoding_to_char(encoding), datcollate, datctype, datlocprovider, \
             datistemplate, datallowconn, dathasloginevt, datconnlimit, \
             datcollversion IS NOT NULL, shobj_description(oid, 'pg_database') \
             FROM pg_database WHERE datname = '{db}';"
        )
    };

    // -- 1. CREATE DATABASE basics + catalog probe + comment + drop. -----
    v.push(SqlOwned(format!("CREATE DATABASE {};", n.db1)));
    v.push(SqlOwned(stable_dbrow(&n.db1)));
    v.push(SqlOwned(format!("COMMENT ON DATABASE {} IS 'q8 dbddl deck';", n.db1)));
    v.push(SqlOwned(stable_dbrow(&n.db1)));
    v.push(SqlOwned(format!("CREATE DATABASE {};", n.db1))); // duplicate -> error
    v.push(SqlOwned(format!("DROP DATABASE {};", n.db1)));
    v.push(SqlOwned(format!("DROP DATABASE {};", n.db1))); // missing -> error
    v.push(SqlOwned(format!("DROP DATABASE IF EXISTS {};", n.db1)));

    // -- 2. Option matrix (each created under a fresh name, then dropped).
    v.push(SqlOwned(format!(
        "CREATE DATABASE {} TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'C' LC_CTYPE 'C' \
         CONNECTION LIMIT 5 IS_TEMPLATE false ALLOW_CONNECTIONS true;",
        n.db1
    )));
    v.push(SqlOwned(stable_dbrow(&n.db1)));
    v.push(SqlOwned(format!("DROP DATABASE {};", n.db1)));
    v.push(SqlOwned(format!(
        "CREATE DATABASE {} WITH TEMPLATE = template0 ENCODING = 'LATIN1' \
         LOCALE = 'C' STRATEGY = wal_log;",
        n.db1
    )));
    v.push(SqlOwned(stable_dbrow(&n.db1)));
    // Encoding observation from inside the database itself.
    v.push(ProbeDb(n.db1.clone(), "SHOW server_encoding;".to_string()));
    v.push(SqlOwned(format!("DROP DATABASE {};", n.db1)));
    v.push(SqlOwned(format!(
        "CREATE DATABASE {} STRATEGY = file_copy TEMPLATE template0;",
        n.dbfc
    )));
    v.push(SqlOwned(stable_dbrow(&n.dbfc)));
    v.push(SqlOwned(format!("DROP DATABASE {};", n.dbfc)));
    // Option-error fuel (matched rejects; nothing created).
    v.push(SqlOwned(format!("CREATE DATABASE {} ENCODING 'no_such_enc';", n.db2)));
    v.push(SqlOwned(format!("CREATE DATABASE {} STRATEGY = teleport;", n.db2)));
    v.push(SqlOwned(format!("CREATE DATABASE {} TEMPLATE no_such_template_fz;", n.db2)));
    v.push(SqlOwned(format!("CREATE DATABASE {} CONNECTION LIMIT -2;", n.db2)));
    v.push(SqlOwned(format!("CREATE DATABASE {} OID 799999;", n.db2)));
    v.push(SqlOwned(format!(
        "CREATE DATABASE {} ENCODING 'LATIN1' LOCALE 'en_US.UTF-8';",
        n.db2
    )));
    v.push(Sql("BEGIN;"));
    v.push(SqlOwned(format!("CREATE DATABASE {};", n.db2))); // in txn -> error
    v.push(Sql("ROLLBACK;"));

    // -- 3. ALTER DATABASE family. --------------------------------------
    v.push(SqlOwned(format!("CREATE DATABASE {};", n.db2)));
    v.push(SqlOwned(format!("ALTER DATABASE {} CONNECTION LIMIT 1;", n.db2)));
    v.push(SqlOwned(format!("ALTER DATABASE {} IS_TEMPLATE true;", n.db2)));
    v.push(SqlOwned(stable_dbrow(&n.db2)));
    v.push(SqlOwned(format!("DROP DATABASE {};", n.db2))); // template -> error
    v.push(SqlOwned(format!("ALTER DATABASE {} IS_TEMPLATE false;", n.db2)));
    v.push(SqlOwned(format!("ALTER DATABASE {} ALLOW_CONNECTIONS false;", n.db2)));
    // Connect while ALLOW_CONNECTIONS false -> matched FATAL.
    v.push(ProbeDb(n.db2.clone(), "SELECT 1;".to_string()));
    v.push(SqlOwned(format!("ALTER DATABASE {} ALLOW_CONNECTIONS true;", n.db2)));
    v.push(SqlOwned(format!("ALTER DATABASE {} WITH CONNECTION LIMIT 0;", n.db2)));
    // Connect while CONNECTION LIMIT 0 (superuser: allowed, warningless
    // in 18; the probe compares whatever both sides do).
    v.push(ProbeDb(n.db2.clone(), "SELECT current_database() <> '';".to_string()));
    v.push(SqlOwned(format!("ALTER DATABASE {} CONNECTION LIMIT -1;", n.db2)));
    // Per-database GUC: set, observe on a fresh connection, reset.
    v.push(SqlOwned(format!("ALTER DATABASE {} SET work_mem = '7701kB';", n.db2)));
    v.push(SqlOwned(format!(
        "ALTER DATABASE {} SET search_path = fz_a, public;",
        n.db2
    )));
    v.push(ProbeDb(n.db2.clone(), "SHOW work_mem;".to_string()));
    v.push(ProbeDb(n.db2.clone(), "SHOW search_path;".to_string()));
    v.push(SqlOwned(format!(
        "SELECT unnest(setconfig) FROM pg_db_role_setting s JOIN pg_database d \
         ON d.oid = s.setdatabase WHERE d.datname = '{}' ORDER BY 1;",
        n.db2
    )));
    v.push(SqlOwned(format!("ALTER DATABASE {} RESET work_mem;", n.db2)));
    v.push(ProbeDb(n.db2.clone(), "SHOW work_mem;".to_string()));
    v.push(SqlOwned(format!("ALTER DATABASE {} RESET ALL;", n.db2)));
    v.push(SqlOwned(format!("ALTER DATABASE {} SET no_such_guc_fz = 1;", n.db2)));
    // Owner + rename + refresh collation version.
    v.push(SqlOwned(format!("CREATE ROLE {} LOGIN;", n.role)));
    v.push(SqlOwned(format!("ALTER DATABASE {} OWNER TO {};", n.db2, n.role)));
    v.push(SqlOwned(format!(
        "SELECT r.rolname FROM pg_database d JOIN pg_roles r ON r.oid = d.datdba \
         WHERE d.datname = '{}';",
        n.db2
    )));
    v.push(SqlOwned(format!("ALTER DATABASE {} REFRESH COLLATION VERSION;", n.db2)));
    v.push(SqlOwned(format!("ALTER DATABASE {} RENAME TO {};", n.db2, n.dbren)));
    v.push(SqlOwned(stable_dbrow(&n.dbren)));
    v.push(SqlOwned(format!("ALTER DATABASE {} RENAME TO {};", n.dbren, n.db2)));
    // Renaming/altering missing databases -> matched errors.
    v.push(SqlOwned(format!("ALTER DATABASE no_such_db_fz RENAME TO {};", n.dbren)));
    v.push(Sql("ALTER DATABASE no_such_db_fz CONNECTION LIMIT 3;"));
    // NOTE: no template0/template1/postgres statements of ANY kind here.
    // ALTER DATABASE template1 RENAME actually SUCCEEDS in PostgreSQL
    // (only DROP checks datistemplate), so even a "matched error" probe
    // against a template database is cluster vandalism, not error fuel
    // (learned the hard way: the first deck revision renamed template1 on
    // both rig clusters).

    // -- 4. Busy-db + WITH (FORCE). -------------------------------------
    v.push(WithHeldConn(n.db2.clone(), format!("DROP DATABASE {};", n.db2)));
    v.push(WithHeldConn(n.db2.clone(), format!("DROP DATABASE {} WITH (FORCE);", n.db2)));
    v.push(SqlOwned(format!("DROP DATABASE IF EXISTS {};", n.db2)));
    v.push(SqlOwned(format!("DROP ROLE {};", n.role)));

    // -- 5. Tablespaces. -------------------------------------------------
    v.push(SqlOwned(format!("CREATE TABLESPACE {} LOCATION '{TSDIR}/t1';", n.ts1)));
    v.push(SqlOwned(format!("CREATE TABLESPACE {} LOCATION '{TSDIR}/t1';", n.ts1))); // dup -> error
    v.push(SqlOwned(format!(
        "SELECT spcname, spcoptions FROM pg_tablespace WHERE spcname = '{}';",
        n.ts1
    )));
    v.push(SqlOwned(format!(
        "SELECT pg_tablespace_location(oid) <> '' FROM pg_tablespace WHERE spcname = '{}';",
        n.ts1
    )));
    v.push(SqlOwned(format!(
        "SELECT count(*) FROM pg_tablespace_databases((SELECT oid FROM pg_tablespace \
         WHERE spcname = '{}'));",
        n.ts1
    )));
    v.push(Sql("SELECT count(*) >= 0 FROM pg_tablespace_databases((SELECT oid FROM pg_tablespace WHERE spcname = 'pg_default'));"));
    v.push(SqlOwned(format!(
        "ALTER TABLESPACE {} SET (random_page_cost = 3.5, seq_page_cost = 2.0);",
        n.ts1
    )));
    v.push(SqlOwned(format!(
        "SELECT spcoptions FROM pg_tablespace WHERE spcname = '{}';",
        n.ts1
    )));
    v.push(SqlOwned(format!(
        "ALTER TABLESPACE {} SET (effective_io_concurrency = 4);",
        n.ts1
    )));
    v.push(SqlOwned(format!("ALTER TABLESPACE {} RESET (random_page_cost);", n.ts1)));
    v.push(SqlOwned(format!("ALTER TABLESPACE {} SET (no_such_option = 1);", n.ts1)));
    v.push(SqlOwned(format!("ALTER TABLESPACE {} RENAME TO {};", n.ts1, n.ts2)));
    v.push(SqlOwned(format!("COMMENT ON TABLESPACE {} IS 'q8 ts';", n.ts2)));
    // Error fuel: bad locations (per-side path normalized), in-txn reject.
    v.push(Sql("CREATE TABLESPACE fz_ts_bad LOCATION 'relative/path';"));
    v.push(SqlOwned(format!(
        "CREATE TABLESPACE fz_ts_bad LOCATION '{TSDIR}/no_such_subdir_fz/deeper';"
    )));
    v.push(Sql("BEGIN;"));
    v.push(SqlOwned(format!("CREATE TABLESPACE fz_ts_txn LOCATION '{TSDIR}/t9';")));
    v.push(Sql("ROLLBACK;"));
    v.push(Sql("DROP TABLESPACE no_such_ts_fz;"));

    // Objects in the tablespace + SET TABLESPACE moves.
    v.push(SqlOwned(format!(
        "CREATE TABLE fz_ts_tab (pk int4 PRIMARY KEY, v text) TABLESPACE {};",
        n.ts2
    )));
    v.push(Sql("INSERT INTO fz_ts_tab SELECT g, 'r' || g FROM generate_series(1, 500) g;"));
    v.push(SqlOwned(format!("CREATE INDEX fz_ts_idx ON fz_ts_tab (v) TABLESPACE {};", n.ts2)));
    v.push(SqlOwned(format!(
        "SELECT count(*) FROM pg_class c JOIN pg_tablespace t ON t.oid = c.reltablespace \
         WHERE t.spcname = '{}';",
        n.ts2
    )));
    v.push(SqlOwned(format!("DROP TABLESPACE {};", n.ts2))); // not empty -> error
    v.push(Sql("ALTER TABLE fz_ts_tab SET TABLESPACE pg_default;"));
    v.push(SqlOwned(format!("ALTER INDEX fz_ts_idx SET TABLESPACE {};", n.ts2)));
    v.push(Sql("ALTER INDEX fz_ts_idx SET TABLESPACE pg_default;"));
    v.push(SqlOwned(format!("ALTER TABLE fz_ts_tab SET TABLESPACE {};", n.ts2)));
    v.push(Sql("SELECT count(*), min(pk), max(pk) FROM fz_ts_tab;"));
    v.push(SqlOwned(format!(
        "ALTER TABLE ALL IN TABLESPACE {} SET TABLESPACE pg_default;",
        n.ts2
    )));
    v.push(SqlOwned(format!(
        "ALTER TABLE ALL IN TABLESPACE {} SET TABLESPACE pg_default;",
        n.ts2
    ))); // now-empty -> notice/no-op
    // Partitioned parent: SET TABLESPACE takes the no-storage path.
    v.push(Sql("CREATE TABLE fz_ts_part (pk int4) PARTITION BY RANGE (pk);"));
    v.push(SqlOwned(format!("ALTER TABLE fz_ts_part SET TABLESPACE {};", n.ts2)));
    v.push(Sql("DROP TABLE fz_ts_part;"));
    // temp_tablespaces + a spilling sort through it.
    v.push(SqlOwned(format!("SET temp_tablespaces = {};", n.ts2)));
    v.push(Sql("SET work_mem = '64kB';"));
    v.push(Sql("CREATE TEMP TABLE fz_ts_tmp AS SELECT g AS a, (g * 7919) % 10007 AS b FROM generate_series(1, 20000) g;"));
    v.push(Sql("SELECT count(*), min(b), max(b) FROM (SELECT b FROM fz_ts_tmp ORDER BY b) s;"));
    v.push(Sql("DROP TABLE fz_ts_tmp;"));
    v.push(Sql("RESET temp_tablespaces;"));
    v.push(Sql("RESET work_mem;"));

    // -- 6. Database in a tablespace + movedb (SET TABLESPACE). ----------
    v.push(SqlOwned(format!("CREATE DATABASE {} TABLESPACE {};", n.dbmov, n.ts2)));
    v.push(SqlOwned(format!(
        "SELECT t.spcname FROM pg_database d JOIN pg_tablespace t ON t.oid = d.dattablespace \
         WHERE d.datname = '{}';",
        n.dbmov
    )));
    v.push(ProbeDb(
        n.dbmov.clone(),
        "CREATE TABLE fz_mv_t AS SELECT g FROM generate_series(1, 100) g;".to_string(),
    ));
    v.push(SqlOwned(format!("ALTER DATABASE {} SET TABLESPACE pg_default;", n.dbmov)));
    v.push(SqlOwned(format!("ALTER DATABASE {} SET TABLESPACE {};", n.dbmov, n.ts2)));
    v.push(SqlOwned(format!("ALTER DATABASE {} SET TABLESPACE {};", n.dbmov, n.ts2))); // same -> notice
    v.push(ProbeDb(n.dbmov.clone(), "SELECT count(*) FROM fz_mv_t;".to_string()));
    // movedb with a live connection into the db -> matched busy error.
    v.push(WithHeldConn(
        n.dbmov.clone(),
        format!("ALTER DATABASE {} SET TABLESPACE pg_default;", n.dbmov),
    ));
    v.push(SqlOwned(format!("DROP DATABASE {};", n.dbmov)));
    // Tablespace now empty again -> droppable.
    v.push(Sql("DROP TABLE fz_ts_tab;"));
    v.push(SqlOwned(format!("DROP TABLESPACE {};", n.ts2)));
    v.push(SqlOwned(format!("DROP TABLESPACE IF EXISTS {};", n.ts2)));
    v
}

/// Utility statements re-run under the observation bracket
/// (debug_print_parse / compute_query_id) and the forced-extended replay.
/// All error-free and self-cleaning.
fn observation_deck(n: &Names) -> Vec<String> {
    vec![
        format!("CREATE DATABASE {};", n.db1),
        format!("ALTER DATABASE {} CONNECTION LIMIT 4;", n.db1),
        format!("ALTER DATABASE {} SET work_mem = '5MB';", n.db1),
        format!("ALTER DATABASE {} RESET work_mem;", n.db1),
        format!("ALTER DATABASE {} REFRESH COLLATION VERSION;", n.db1),
        format!("DROP DATABASE {};", n.db1),
        format!("CREATE TABLESPACE {} LOCATION '{TSDIR}/tobs';", n.ts1),
        format!("ALTER TABLESPACE {} SET (random_page_cost = 2.5);", n.ts1),
        format!("ALTER TABLE ALL IN TABLESPACE {} SET TABLESPACE pg_default;", n.ts1),
        format!("DROP TABLESPACE {};", n.ts1),
    ]
}

impl DbDdlSuite {
    /// Run the differential deck. Consumes the suite (connections close).
    pub fn run_suite(
        mut self,
        table: &[RuledEntry],
        ulp_tol: u64,
    ) -> (Vec<Record>, DbDdlStats) {
        let mut records = Vec::new();
        let mut stats = DbDdlStats::default();
        let mut idx = 0u32;
        let n = names(&self.tag);
        let needles_a: Vec<&str> = self.needles_a.iter().map(|s| s.as_str()).collect();
        let needles_b: Vec<&str> = self.needles_b.iter().map(|s| s.as_str()).collect();

        let mut record = |canon: &str,
                          oa: StmtOutcome,
                          ob: StmtOutcome,
                          records: &mut Vec<Record>,
                          stats: &mut DbDdlStats| {
            let na = norm_outcome(&oa, &needles_a);
            let nb = norm_outcome(&ob, &needles_b);
            let raw = classify(&DiffInput {
                sql: canon,
                a: &na,
                b: &nb,
                ulp_tol,
                soft_cols: &[],
                mask_explain_timing: false,
            });
            let c = apply_ruled(table, canon, raw);
            stats.cases += 1;
            idx += 1;
            match &c.class {
                DiffClass::Match => stats.matches += 1,
                DiffClass::Ruled(_) => {
                    stats.ruled += 1;
                    records.push(Record {
                        stmt_index: idx - 1,
                        sql: canon.to_string(),
                        class: c.class,
                        detail: c.detail,
                        probe: false,
                    });
                }
                _ => {
                    stats.findings += 1;
                    records.push(Record {
                        stmt_index: idx - 1,
                        sql: canon.to_string(),
                        class: c.class,
                        detail: c.detail,
                        probe: false,
                    });
                }
            }
        };

        let tsdir_a = self.a.spec.tsdir.clone();
        let tsdir_b = self.b.spec.tsdir.clone();
        for step in deck(&n) {
            match step {
                Step::Sql(sql) => {
                    let (sa, sb) = (sql.replace(TSDIR, &tsdir_a), sql.replace(TSDIR, &tsdir_b));
                    let oa = self.a.main.apply(&sa);
                    let ob = self.b.main.apply(&sb);
                    record(sql, oa, ob, &mut records, &mut stats);
                }
                Step::SqlOwned(sql) => {
                    let (sa, sb) = (sql.replace(TSDIR, &tsdir_a), sql.replace(TSDIR, &tsdir_b));
                    let oa = self.a.main.apply(&sa);
                    let ob = self.b.main.apply(&sb);
                    record(&sql, oa, ob, &mut records, &mut stats);
                }
                Step::ProbeDb(db, sql) => {
                    let canon = format!("\\connect {db} :: {sql}");
                    let (ca, oa) = connect_outcome(&self.a.spec, &db);
                    let (cb, ob) = connect_outcome(&self.b.spec, &db);
                    match (ca, cb) {
                        (Some(mut ea), Some(mut eb)) => {
                            let ra = ea.apply(&sql);
                            let rb = eb.apply(&sql);
                            record(&canon, ra, rb, &mut records, &mut stats);
                        }
                        _ => {
                            // At least one side refused the connection:
                            // the connect outcomes ARE the compared pair.
                            record(&canon, oa, ob, &mut records, &mut stats);
                        }
                    }
                }
                Step::WithHeldConn(db, sql) => {
                    let held_a = ClientExecutor::connect(
                        &self.a.spec.host,
                        self.a.spec.port,
                        &db,
                        &self.a.spec.user,
                    );
                    let held_b = ClientExecutor::connect(
                        &self.b.spec.host,
                        self.b.spec.port,
                        &db,
                        &self.b.spec.user,
                    );
                    let canon = format!("-- with held connection into {db}\n{sql}");
                    match (held_a, held_b) {
                        (Ok(ha), Ok(hb)) => {
                            let oa = self.a.main.apply(&sql);
                            let ob = self.b.main.apply(&sql);
                            record(&canon, oa, ob, &mut records, &mut stats);
                            drop(ha);
                            drop(hb);
                        }
                        (ra, rb) => {
                            let mk = |r: Result<ClientExecutor, String>| match r {
                                Ok(_) => StmtOutcome::Command {
                                    tag: "CONNECT".to_string(),
                                    affected: None,
                                },
                                Err(e) => StmtOutcome::Error {
                                    sqlstate: "XXCON".to_string(),
                                    message: e,
                                },
                            };
                            record(&canon, mk(ra), mk(rb), &mut records, &mut stats);
                        }
                    }
                }
            }
        }

        // Observation bracket: raw parse trees + query jumbling on, replay
        // the utility deck, reset. Output goes to the server log — the
        // compared surface is just that both sides accept the bracket.
        for set in ["SET debug_print_parse = on;", "SET compute_query_id = on;"] {
            let oa = self.a.main.apply(set);
            let ob = self.b.main.apply(set);
            record(set, oa, ob, &mut records, &mut stats);
        }
        for sql in observation_deck(&n) {
            let (sa, sb) = (sql.replace(TSDIR, &tsdir_a), sql.replace(TSDIR, &tsdir_b));
            let oa = self.a.main.apply(&sa);
            let ob = self.b.main.apply(&sb);
            record(&sql, oa, ob, &mut records, &mut stats);
        }
        for reset in ["RESET debug_print_parse;", "RESET compute_query_id;"] {
            let oa = self.a.main.apply(reset);
            let ob = self.b.main.apply(reset);
            record(reset, oa, ob, &mut records, &mut stats);
        }

        // Forced-extended replay: fresh connections whose every statement
        // rides Parse/Bind/Execute (plancache copyObject over the raw
        // utility trees — the copyfuncs arm).
        let xa = ClientExecutor::connect_opts(
            &self.a.spec.host,
            self.a.spec.port,
            &self.a.spec.db,
            &self.a.spec.user,
            Some(XPROTO_FORCED),
        );
        let xb = ClientExecutor::connect_opts(
            &self.b.spec.host,
            self.b.spec.port,
            &self.b.spec.db,
            &self.b.spec.user,
            Some(XPROTO_FORCED),
        );
        if let (Ok(mut xa), Ok(mut xb)) = (xa, xb) {
            for sql in observation_deck(&n) {
                let (sa, sb) = (sql.replace(TSDIR, &tsdir_a), sql.replace(TSDIR, &tsdir_b));
                let canon = format!("-- extended protocol\n{sql}");
                let oa = xa.apply(&sa);
                let ob = xb.apply(&sb);
                record(&canon, oa, ob, &mut records, &mut stats);
            }
        }

        // Best-effort cleanup sweep (never recorded): drop anything the
        // deck may have left behind after a mid-deck error.
        for sql in cleanup_sql(&n) {
            let (sa, sb) = (sql.replace(TSDIR, &tsdir_a), sql.replace(TSDIR, &tsdir_b));
            let _ = self.a.main.apply(&sa);
            let _ = self.b.main.apply(&sb);
        }

        (records, stats)
    }
}

fn cleanup_sql(n: &Names) -> Vec<String> {
    let mut v = Vec::new();
    for db in [&n.db1, &n.db2, &n.dbren, &n.dbmov, &n.dbfc] {
        v.push(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE);"));
    }
    v.push("DROP TABLE IF EXISTS fz_ts_tab;".to_string());
    v.push("DROP TABLE IF EXISTS fz_ts_part;".to_string());
    for ts in [&n.ts1, &n.ts2] {
        v.push(format!("DROP TABLESPACE IF EXISTS {ts};"));
    }
    v.push(format!("DROP ROLE IF EXISTS {};", n.role));
    v
}

/// Single-engine deck for coverage runs (covapply --dbddl): same steps,
/// one server; fresh-connection probes and held-session brackets included.
pub fn run_single(spec: &ConnSpec, tag: u32) -> Result<(u32, u32), String> {
    make_ts_dirs(&spec.tsdir)?;
    let mut main =
        ClientExecutor::connect(&spec.host, spec.port, &spec.db, &spec.user)?;
    let tag = format!("q8t{tag}");
    let n = names(&tag);
    let (mut applied, mut errors) = (0u32, 0u32);
    let run = |ex: &mut ClientExecutor, sql: &str, applied: &mut u32, errors: &mut u32| {
        *applied += 1;
        if matches!(
            ex.apply(sql),
            StmtOutcome::Error { .. } | StmtOutcome::ConnLost { .. }
        ) {
            *errors += 1;
        }
    };
    for step in deck(&n) {
        match step {
            Step::Sql(sql) => {
                let s = sql.replace(TSDIR, &spec.tsdir);
                run(&mut main, &s, &mut applied, &mut errors);
            }
            Step::SqlOwned(sql) => {
                let s = sql.replace(TSDIR, &spec.tsdir);
                run(&mut main, &s, &mut applied, &mut errors);
            }
            Step::ProbeDb(db, sql) => {
                applied += 1;
                match ClientExecutor::connect(&spec.host, spec.port, &db, &spec.user) {
                    Ok(mut ex) => run(&mut ex, &sql, &mut applied, &mut errors),
                    Err(_) => errors += 1,
                }
            }
            Step::WithHeldConn(db, sql) => {
                let held = ClientExecutor::connect(&spec.host, spec.port, &db, &spec.user);
                run(&mut main, &sql, &mut applied, &mut errors);
                drop(held);
            }
        }
    }
    for set in ["SET debug_print_parse = on;", "SET compute_query_id = on;"] {
        run(&mut main, set, &mut applied, &mut errors);
    }
    for sql in observation_deck(&n) {
        let s = sql.replace(TSDIR, &spec.tsdir);
        run(&mut main, &s, &mut applied, &mut errors);
    }
    for reset in ["RESET debug_print_parse;", "RESET compute_query_id;"] {
        run(&mut main, reset, &mut applied, &mut errors);
    }
    if let Ok(mut xe) = ClientExecutor::connect_opts(
        &spec.host,
        spec.port,
        &spec.db,
        &spec.user,
        Some(XPROTO_FORCED),
    ) {
        for sql in observation_deck(&n) {
            let s = sql.replace(TSDIR, &spec.tsdir);
            run(&mut xe, &s, &mut applied, &mut errors);
        }
    }
    for sql in cleanup_sql(&n) {
        let s = sql.replace(TSDIR, &spec.tsdir);
        let _ = main.apply(&s);
    }
    Ok((applied, errors))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deck_is_deterministic_and_self_cleaning() {
        let n = names("q8t7");
        let render = |steps: Vec<Step>| -> Vec<String> {
            steps
                .into_iter()
                .map(|s| match s {
                    Step::Sql(x) => x.to_string(),
                    Step::SqlOwned(x) => x,
                    Step::ProbeDb(db, sql) => format!("probe:{db}:{sql}"),
                    Step::WithHeldConn(db, sql) => format!("held:{db}:{sql}"),
                })
                .collect()
        };
        assert_eq!(render(deck(&n)), render(deck(&n)));
        // Every CREATE DATABASE name is later dropped in the deck or
        // covered by the cleanup sweep.
        let all: String = render(deck(&n)).join("\n");
        let sweep: String = cleanup_sql(&n).join("\n");
        for db in [&n.db1, &n.db2, &n.dbren, &n.dbmov, &n.dbfc] {
            assert!(
                all.contains(&format!("DROP DATABASE {db}"))
                    || all.contains(&format!("DROP DATABASE IF EXISTS {db}"))
                    || sweep.contains(&format!("DROP DATABASE IF EXISTS {db}")),
                "database {db} never dropped"
            );
        }
        for ts in [&n.ts1, &n.ts2] {
            assert!(sweep.contains(&format!("DROP TABLESPACE IF EXISTS {ts}")));
        }
    }

    #[test]
    fn deck_never_touches_protected_databases() {
        let n = names("q8t9");
        for step in deck(&n) {
            let sql = match &step {
                Step::Sql(x) => x.to_string(),
                Step::SqlOwned(x) => x.clone(),
                Step::ProbeDb(db, sql) => {
                    assert!(db.starts_with("fz_db_"), "probe into non-fuzz db {db}");
                    sql.clone()
                }
                Step::WithHeldConn(db, sql) => {
                    assert!(db.starts_with("fz_db_"), "held conn into non-fuzz db {db}");
                    sql.clone()
                }
            };
            for protected in ["template1", "postgres"] {
                assert!(
                    !sql.contains(protected),
                    "deck touches {protected}: {sql}"
                );
            }
            // template0 may appear ONLY as a read-only TEMPLATE source.
            if sql.contains("template0") {
                assert!(
                    sql.contains("TEMPLATE template0") || sql.contains("TEMPLATE = template0"),
                    "unexpected template0 use: {sql}"
                );
            }
        }
    }

    #[test]
    fn tsdir_placeholder_is_substituted_per_side() {
        assert_eq!(
            norm_str("/tmp/side_a/t1 failed", &["/tmp/side_a"]),
            "<TSDIR>/t1 failed"
        );
    }
}
