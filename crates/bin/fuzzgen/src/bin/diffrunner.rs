//! Differential runner CLI: apply one fuzzgen statement stream to two live
//! servers in lockstep, classify per-statement divergences against the
//! ruled table, inject state-sync probes (pk-ordered SELECT * on every
//! probeable table, every --probe-every statements and at stream end),
//! emit findings as JSONL, and optionally ddmin the first finding down to
//! a minimal .sql repro (fresh server pairs per probe; transaction
//! brackets drop or stay whole).

use std::io::Write;
use std::process::ExitCode;

use fuzzgen::catalog::{fixture_ddl, fixture_seed_sql, CatalogSource, FixtureCatalog};
use fuzzgen::diff::DiffClass;
use fuzzgen::livecat::LiveCatalog;
use fuzzgen::reduce::{reduce_stream, ExecPair, ExecPairFactory, ReduceTarget};
use fuzzgen::runner::{
    run_stream, ClientExecutor, Executor, GucPinned, ProbeSpec, ProbeTable, Record, StreamStmt,
};
use fuzzgen::session::{run_session_probed, SessionConfig};
use fuzzgen::toggles::ToggleVector;
use fuzzgen::weights::WeightTable;

const USAGE: &str = "\
usage: diffrunner --a <host:port> --b <host:port> [options]
  --a <host:port>     reference server (A: C Postgres)
  --b <host:port>     candidate server (B)
  --db <name>         database on both sides (default: fuzz)
  --user <name>       role on both sides (default: $USER)
  --seed <u64>        session seed (default 0); the reproducibility witness
  --count <n>         statement budget (default 100; statement groups such
                      as transaction brackets complete past it)
  --max-depth <n>     expression nesting bound (default 4)
  --modules <spec>    pin the toggle vector, e.g. expr=on
  --weight <spec>     per-production bias weights, e.g. case=5 (repeatable)
  --setup             create fixture schema + seed rows on both sides first
  --perturb-b <sql>   extra statement run on B after every setup (repeatable;
                      the seeded-divergence self-test hook)
  --findings <path>   findings JSONL output (default: stdout)
  --probe-every <n>   state-probe cadence in statements (default 50;
                      0 disables probes entirely)
  --reduce            ddmin the first finding to a minimal repro
  --repro-dir <dir>   where minimal .sql repros go (default: .)
  --live-catalog      introspect the generator schema from A (default:
                      fixture; live catalogs have no pk metadata, so no
                      DML targets and no state probes)
  --ulp <n>           float ulp tolerance (default 4)
  --no-guc-pin        drop the default C-parity GUC pin. By default both
                      sessions get the six parallel + three jit_*_cost
                      GUCs SET to the C defaults at setup (a no-op on A),
                      so ratified pgrust default divergences (P1-A ruling)
                      do not consume findings budget; --no-guc-pin runs
                      stock defaults to exercise that surface on purpose.
  --profile <name>    config-profile provenance tag recorded in the findings
                      manifest meta line (the R1 config-variant pairs rig:
                      both servers were booted with this named GUC profile —
                      scripts/lib/configprofiles.sh; recording it is the
                      rig's job, applying it is the driver's). Default:
                      \"default\".
  --replay <path>     replay a .sql statement stream (one statement per
                      line; '--' comment lines and blanks skipped) instead
                      of generating from --seed — the ddmin-repro
                      re-verification path. stmt_index = 0-based position
                      among the kept statements.
  --mask-explain-timing
                      (with --replay) opt the replayed statements into the
                      H1 EXPLAIN ANALYZE wall-clock mask, like a generated
                      gramwalk stream (which opts in automatically).
  --xproto <seed>     per-statement seeded protocol-mode mix: each statement
                      deterministically rides simple query or extended
                      Parse/Bind/Execute (with occasional $n parameters —
                      text- or binary-format — Execute row limits + portal
                      resume, and occasional all-binary result format),
                      identically on both sides. The mode is a pure
                      function of (seed, statement text), so --replay under
                      the same --xproto seed reproduces modes exactly;
                      findings carry a \"mode\" key. Seed bit 63
                      (--xproto 9223372036854775808 + n) forces EVERY
                      statement extended with all-binary results (the X2
                      hand-verification vehicle). Default: simple only.
  --copybin           run the deterministic COPY BINARY suite instead of a
                      statement stream: COPY TO STDOUT (FORMAT binary)
                      byte-compared A-vs-B over every fixture + breadth
                      table, plus COPY FROM STDIN (FORMAT binary)
                      round-trips of A's (reference) bytes into BOTH
                      engines with state probes and re-emits. Ignores
                      --seed/--count/--xproto/--replay.
  --copytext          run the COPY text/csv option-matrix suite (Q8
                      copy-variants chunk) instead of a statement stream:
                      deterministic TO/FROM option decks + matched error
                      fuel + reg*/xid/cid tail, then a seeded payload-fuzz
                      arm sized by --count with --seed. Ignores
                      --xproto/--replay.
  --copyopts          run the COPY options-matrix drain (lane COPYOPTS)
                      instead of a statement stream: an option-error deck
                      (conflicting/duplicate options, illegal defGetCopy*
                      values, BINARY/text-only rejects, unrecognized
                      options) whose rejects must MATCH by SQLSTATE, plus
                      differential round-trip identity over a control-char
                      source (TO emits, both engines reload A's bytes,
                      clone state + re-emit compared), with a seeded
                      valid-option round-trip arm sized by --count/--seed.
                      Ignores --xproto/--replay.
  --dbddl             run the database/tablespace DDL suite (Q8
                      database-ddl chunk): the suite opens its OWN
                      connections (fresh-db probes, held-session busy-db
                      brackets, forced-extended replay) and creates
                      per-side tablespace scratch dirs (paths normalized
                      to <TSDIR> before compare). Names embed --seed.
                      Ignores --count/--xproto/--replay.
";

#[derive(Clone)]
struct Endpoint {
    host: String,
    port: u16,
}

fn parse_endpoint(s: &str, name: &str) -> Result<Endpoint, String> {
    let (host, port) = s
        .rsplit_once(':')
        .ok_or_else(|| format!("{name} must be host:port, got {s:?}"))?;
    Ok(Endpoint {
        host: host.to_string(),
        port: port.parse().map_err(|e| format!("bad {name} port {port:?}: {e}"))?,
    })
}

struct Args {
    a: Endpoint,
    b: Endpoint,
    db: String,
    user: String,
    seed: u64,
    count: u32,
    max_depth: u32,
    modules: Option<String>,
    weights: WeightTable,
    setup: bool,
    perturb_b: Vec<String>,
    findings: Option<String>,
    probe_every: u32,
    reduce: bool,
    repro_dir: String,
    live_catalog: bool,
    ulp: u64,
    guc_pin: bool,
    replay: Option<String>,
    /// H1 opt-in for --replay decks: mask EXPLAIN ANALYZE wall-clock text
    /// like a gramwalk-generated stream would (generated gramwalk
    /// statements opt in automatically via their module tag).
    mask_explain_timing: bool,
    xproto: Option<u64>,
    profile: String,
    copybin: bool,
    copytext: bool,
    copyopts: bool,
    dbddl: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut a = None;
    let mut b = None;
    let mut args = Args {
        a: Endpoint { host: String::new(), port: 0 },
        b: Endpoint { host: String::new(), port: 0 },
        db: "fuzz".to_string(),
        user: std::env::var("USER").unwrap_or_else(|_| "postgres".to_string()),
        seed: 0,
        count: 100,
        max_depth: 4,
        modules: None,
        weights: WeightTable::defaults(),
        setup: false,
        perturb_b: Vec::new(),
        findings: None,
        probe_every: 50,
        reduce: false,
        repro_dir: ".".to_string(),
        live_catalog: false,
        ulp: 4,
        guc_pin: true,
        replay: None,
        mask_explain_timing: false,
        xproto: None,
        profile: "default".to_string(),
        copybin: false,
        copytext: false,
        copyopts: false,
        dbddl: false,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut value = |name: &str| {
            it.next().ok_or_else(|| format!("{name} requires a value"))
        };
        match arg.as_str() {
            "--a" => a = Some(parse_endpoint(&value("--a")?, "--a")?),
            "--b" => b = Some(parse_endpoint(&value("--b")?, "--b")?),
            "--db" => args.db = value("--db")?,
            "--user" => args.user = value("--user")?,
            "--seed" => {
                args.seed = value("--seed")?.parse().map_err(|e| format!("bad --seed: {e}"))?
            }
            "--count" => {
                args.count = value("--count")?.parse().map_err(|e| format!("bad --count: {e}"))?
            }
            "--max-depth" => {
                args.max_depth =
                    value("--max-depth")?.parse().map_err(|e| format!("bad --max-depth: {e}"))?
            }
            "--modules" => args.modules = Some(value("--modules")?),
            "--weight" => args.weights.apply_spec(&value("--weight")?)?,
            "--setup" => args.setup = true,
            "--perturb-b" => args.perturb_b.push(value("--perturb-b")?),
            "--findings" => args.findings = Some(value("--findings")?),
            "--probe-every" => {
                args.probe_every = value("--probe-every")?
                    .parse()
                    .map_err(|e| format!("bad --probe-every: {e}"))?
            }
            "--reduce" => args.reduce = true,
            "--repro-dir" => args.repro_dir = value("--repro-dir")?,
            "--live-catalog" => args.live_catalog = true,
            "--ulp" => args.ulp = value("--ulp")?.parse().map_err(|e| format!("bad --ulp: {e}"))?,
            "--no-guc-pin" => args.guc_pin = false,
            "--profile" => args.profile = value("--profile")?,
            "--replay" => args.replay = Some(value("--replay")?),
            "--mask-explain-timing" => args.mask_explain_timing = true,
            "--xproto" => {
                args.xproto = Some(
                    value("--xproto")?.parse().map_err(|e| format!("bad --xproto: {e}"))?,
                )
            }
            "--copybin" => args.copybin = true,
            "--copytext" => args.copytext = true,
            "--copyopts" => args.copyopts = true,
            "--dbddl" => args.dbddl = true,
            "--help" | "-h" => {
                print!("{USAGE}");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument {other:?}")),
        }
    }
    args.a = a.ok_or("--a is required")?;
    args.b = b.ok_or("--b is required")?;
    if args.copybin || args.copytext || args.copyopts || args.dbddl {
        // The suites always ride the simple protocol (their COPY FROM
        // feeds are unsupported on the extended path; dbddl owns its own
        // extended-replay pass) and own their decks.
        args.xproto = None;
        args.replay = None;
    }
    Ok(args)
}

/// Connect both sides and (when configured) run schema setup + B-side
/// perturbations. Used for the main run and for every reduction probe, so
/// probe pairs start from the identical initial state.
struct ServerPairFactory<'a> {
    args: &'a Args,
    setup_sql: Vec<String>,
}

impl ServerPairFactory<'_> {
    fn connect_pair(&self) -> Result<(ClientExecutor, ClientExecutor), String> {
        // The xproto seed is applied to BOTH sides: the protocol path is
        // part of the statement, never an asymmetry.
        let a = ClientExecutor::connect_opts(
            &self.args.a.host,
            self.args.a.port,
            &self.args.db,
            &self.args.user,
            self.args.xproto,
        )
        .map_err(|e| format!("A: {e}"))?;
        let b = ClientExecutor::connect_opts(
            &self.args.b.host,
            self.args.b.port,
            &self.args.db,
            &self.args.user,
            self.args.xproto,
        )
        .map_err(|e| format!("B: {e}"))?;
        Ok((a, b))
    }

    fn setup(&self, a: &mut ClientExecutor, b: &mut ClientExecutor) -> Result<(), String> {
        // C-parity GUC pin first (default on; --no-guc-pin drops it): both
        // sessions run C-default parallel/JIT costing so ratified pgrust
        // default divergences (P1-A ruling) never reach classification.
        // Applied per connection, so reducer probe pairs are pinned too;
        // the GucPinned wrapper keeps it invariant across the stream's
        // RESET ALL / DISCARD statements.
        if self.args.guc_pin {
            for sql in fuzzgen::runner::c_parity_pin_sql() {
                for (side, ex) in [("A", &mut *a), ("B", &mut *b)] {
                    if let fuzzgen::diff::StmtOutcome::Error { sqlstate, message } = ex.apply(&sql)
                    {
                        return Err(format!(
                            "guc pin on {side} failed ({sqlstate} {message}): {sql}"
                        ));
                    }
                }
            }
        }
        for sql in &self.setup_sql {
            for (side, ex) in [("A", &mut *a), ("B", &mut *b)] {
                if let fuzzgen::diff::StmtOutcome::Error { sqlstate, message } = ex.apply(sql) {
                    return Err(format!("setup on {side} failed ({sqlstate} {message}): {sql}"));
                }
            }
        }
        for sql in &self.args.perturb_b {
            if let fuzzgen::diff::StmtOutcome::Error { sqlstate, message } = b.apply(sql) {
                return Err(format!("--perturb-b failed ({sqlstate} {message}): {sql}"));
            }
        }
        Ok(())
    }
}

impl ExecPairFactory for ServerPairFactory<'_> {
    fn fresh(&mut self) -> Result<ExecPair, String> {
        let (mut a, mut b) = self.connect_pair()?;
        self.setup(&mut a, &mut b)?;
        Ok(if self.args.guc_pin {
            (Box::new(GucPinned(a)), Box::new(GucPinned(b)))
        } else {
            (Box::new(a), Box::new(b))
        })
    }
}

fn run() -> Result<ExitCode, String> {
    let args = parse_args()?;

    let setup_sql: Vec<String> = if args.setup {
        let catalog = FixtureCatalog.load_catalog()?;
        // Full schema reset first: the ddl module leaves persistent objects
        // (fz_ddl_* tables, fz_idx_* indexes, sequences, trigger functions)
        // that a fixture-only re-setup would not clear — a later run's
        // streams would collide with them (42P07/42703 floods) and reducer
        // probe pairs would not start from the run's true initial state.
        ["DROP SCHEMA public CASCADE;".to_string(), "CREATE SCHEMA public;".to_string()]
            .into_iter()
            .chain(fixture_ddl(&catalog).lines().map(|l| l.to_string()))
            .chain(fixture_seed_sql())
            .collect()
    } else {
        Vec::new()
    };
    let mut factory = ServerPairFactory { args: &args, setup_sql };

    // Setup must land before a live-catalog introspection can see the schema.
    // The main pair gets the same RESET-proof pin wrapper as reducer pairs.
    let (mut a, mut b): (Box<dyn Executor>, Box<dyn Executor>) = {
        let (mut ca, mut cb) = factory.connect_pair()?;
        factory.setup(&mut ca, &mut cb)?;
        if args.guc_pin {
            (Box::new(GucPinned(ca)), Box::new(GucPinned(cb)))
        } else {
            (Box::new(ca), Box::new(cb))
        }
    };

    if args.copybin {
        // Deterministic COPY BINARY suite instead of a statement stream.
        let table = fuzzgen::ruled::default_table();
        let (records, stats) = fuzzgen::copybin::run_suite(&mut *a, &mut *b, &table, args.ulp);
        let mut findings_out: Box<dyn Write> = match &args.findings {
            Some(path) => Box::new(
                std::fs::File::create(path).map_err(|e| format!("open {path}: {e}"))?,
            ),
            None => Box::new(std::io::stdout()),
        };
        writeln!(
            findings_out,
            "{{\"meta\":\"diffrunner\",\"suite\":\"copybin\",\"guc_pin\":{}}}",
            args.guc_pin
        )
        .map_err(|e| e.to_string())?;
        for r in &records {
            writeln!(findings_out, "{}", r.to_jsonl(0)).map_err(|e| e.to_string())?;
        }
        drop(findings_out);
        eprintln!(
            "diffrunner: copybin guc_pin={} cases={} matches={} ruled={} findings={}",
            if args.guc_pin { "on" } else { "off" },
            stats.cases,
            stats.matches,
            stats.ruled,
            stats.findings
        );
        return Ok(if stats.findings > 0 { ExitCode::from(2) } else { ExitCode::SUCCESS });
    }

    if args.copytext {
        // COPY text/csv option-matrix suite (Q8). Seeded arm sized by
        // --count (deterministic in --seed).
        let table = fuzzgen::ruled::default_table();
        let (records, stats) = fuzzgen::copytext::run_suite(
            &mut *a,
            &mut *b,
            &table,
            args.ulp,
            args.seed,
            args.count,
        );
        let mut findings_out: Box<dyn Write> = match &args.findings {
            Some(path) => Box::new(
                std::fs::File::create(path).map_err(|e| format!("open {path}: {e}"))?,
            ),
            None => Box::new(std::io::stdout()),
        };
        writeln!(
            findings_out,
            "{{\"meta\":\"diffrunner\",\"suite\":\"copytext\",\"seed\":{},\"count\":{},\"guc_pin\":{}}}",
            args.seed, args.count, args.guc_pin
        )
        .map_err(|e| e.to_string())?;
        for r in &records {
            writeln!(findings_out, "{}", r.to_jsonl(args.seed)).map_err(|e| e.to_string())?;
        }
        drop(findings_out);
        eprintln!(
            "diffrunner: copytext seed={} count={} guc_pin={} cases={} matches={} ruled={} findings={}",
            args.seed,
            args.count,
            if args.guc_pin { "on" } else { "off" },
            stats.cases,
            stats.matches,
            stats.ruled,
            stats.findings
        );
        return Ok(if stats.findings > 0 { ExitCode::from(2) } else { ExitCode::SUCCESS });
    }

    if args.copyopts {
        // COPY options-matrix drain (lane COPYOPTS). Seeded round-trip arm
        // sized by --count (deterministic in --seed).
        let table = fuzzgen::ruled::default_table();
        let (records, stats) = fuzzgen::copyopts::run_suite(
            &mut *a,
            &mut *b,
            &table,
            args.ulp,
            args.seed,
            args.count,
        );
        let mut findings_out: Box<dyn Write> = match &args.findings {
            Some(path) => Box::new(
                std::fs::File::create(path).map_err(|e| format!("open {path}: {e}"))?,
            ),
            None => Box::new(std::io::stdout()),
        };
        writeln!(
            findings_out,
            "{{\"meta\":\"diffrunner\",\"suite\":\"copyopts\",\"seed\":{},\"count\":{},\"guc_pin\":{}}}",
            args.seed, args.count, args.guc_pin
        )
        .map_err(|e| e.to_string())?;
        for r in &records {
            writeln!(findings_out, "{}", r.to_jsonl(args.seed)).map_err(|e| e.to_string())?;
        }
        drop(findings_out);
        eprintln!(
            "diffrunner: copyopts seed={} count={} guc_pin={} cases={} matches={} ruled={} findings={}",
            args.seed,
            args.count,
            if args.guc_pin { "on" } else { "off" },
            stats.cases,
            stats.matches,
            stats.ruled,
            stats.findings
        );
        return Ok(if stats.findings > 0 { ExitCode::from(2) } else { ExitCode::SUCCESS });
    }

    if args.dbddl {
        // Database/tablespace DDL suite (Q8): owns its own connections and
        // per-side tablespace scratch dirs; the setup pair above is only
        // used for the fixture/GUC-pin setup.
        drop(a);
        drop(b);
        let table = fuzzgen::ruled::default_table();
        let mk_spec = |ep: &Endpoint, side: &str| fuzzgen::dbddl::ConnSpec {
            host: ep.host.clone(),
            port: ep.port,
            db: args.db.clone(),
            user: args.user.clone(),
            tsdir: std::env::temp_dir()
                .join(format!("fz_q8ts_{side}_{}_{}", ep.port, args.seed))
                .to_string_lossy()
                .into_owned(),
        };
        let suite = fuzzgen::dbddl::DbDdlSuite::connect(
            &mk_spec(&args.a, "a"),
            &mk_spec(&args.b, "b"),
            args.seed as u32,
        )?;
        let (records, stats) = suite.run_suite(&table, args.ulp);
        let mut findings_out: Box<dyn Write> = match &args.findings {
            Some(path) => Box::new(
                std::fs::File::create(path).map_err(|e| format!("open {path}: {e}"))?,
            ),
            None => Box::new(std::io::stdout()),
        };
        writeln!(
            findings_out,
            "{{\"meta\":\"diffrunner\",\"suite\":\"dbddl\",\"seed\":{},\"guc_pin\":{}}}",
            args.seed, args.guc_pin
        )
        .map_err(|e| e.to_string())?;
        for r in &records {
            writeln!(findings_out, "{}", r.to_jsonl(args.seed)).map_err(|e| e.to_string())?;
        }
        drop(findings_out);
        eprintln!(
            "diffrunner: dbddl seed={} guc_pin={} cases={} matches={} ruled={} findings={}",
            args.seed,
            if args.guc_pin { "on" } else { "off" },
            stats.cases,
            stats.matches,
            stats.ruled,
            stats.findings
        );
        return Ok(if stats.findings > 0 { ExitCode::from(2) } else { ExitCode::SUCCESS });
    }

    let catalog = if args.live_catalog {
        LiveCatalog {
            host: args.a.host.clone(),
            port: args.a.port,
            db: args.db.clone(),
            user: args.user.clone(),
        }
        .load_catalog()?
    } else {
        FixtureCatalog.load_catalog()?
    };

    // Stream source: generated session (default) or a replayed .sql file
    // (ddmin-repro re-verification; no ddl windows, no soft-float masks).
    let (stream, ddl_windows): (Vec<StreamStmt>, Vec<fuzzgen::session::DdlWindow>) =
        match &args.replay {
            Some(path) => {
                let text =
                    std::fs::read_to_string(path).map_err(|e| format!("read {path}: {e}"))?;
                // stmt_index is the 0-based position among KEPT statements
                // (not the file line number): the reducer indexes the
                // stream by stmt_index, exactly like generated sessions.
                let stream: Vec<StreamStmt> = text
                    .lines()
                    .filter(|l| {
                        let t = l.trim();
                        !t.is_empty() && !t.starts_with("--")
                    })
                    .enumerate()
                    .map(|(i, l)| StreamStmt {
                        stmt_index: i as u32,
                        sql: l.trim().to_string(),
                        soft_float_cols: Vec::new(),
                        mask_explain_timing: args.mask_explain_timing,
                    })
                    .collect();
                if stream.is_empty() {
                    return Err(format!("--replay {path}: no statements"));
                }
                (stream, Vec::new())
            }
            None => {
                let toggles = match &args.modules {
                    Some(spec) => ToggleVector::parse(spec)?,
                    None => ToggleVector::all_on(),
                };
                let cfg = SessionConfig {
                    seed: args.seed,
                    toggles,
                    weights: args.weights.clone(),
                    budget: args.count,
                    max_depth: args.max_depth,
                };
                let session = run_session_probed(&cfg, &catalog);
                let stream = session
                    .stmts
                    .iter()
                    .map(|s| StreamStmt {
                        stmt_index: s.stmt_index,
                        sql: s.sql.clone(),
                        soft_float_cols: s.soft_float_cols.clone(),
                        // Opt-in H1 mask: gramwalk derives raw EXPLAIN
                        // ANALYZE straight from the grammar and cannot
                        // carry the explain module's TIMING OFF hygiene.
                        mask_explain_timing: s
                            .productions
                            .iter()
                            .any(|p| p == "module:gramwalk"),
                    })
                    .collect();
                (stream, session.ddl_windows)
            }
        };

    let table = fuzzgen::ruled::default_table();
    // Probeable tables: every pk-carrying catalog table (whole-stream
    // window) plus ddl-created tables gated by their existence windows.
    let probe_spec = if args.probe_every > 0 {
        let mut tables: Vec<ProbeTable> = ProbeSpec::from_catalog(&catalog, args.probe_every)
            .map(|s| s.tables)
            .unwrap_or_default();
        tables.extend(ddl_windows.iter().map(|w| ProbeTable {
            name: w.table.clone(),
            pk: w.pk.clone(),
            from: w.from,
            until: w.until,
        }));
        if tables.is_empty() {
            None
        } else {
            Some(ProbeSpec { every: args.probe_every, tables })
        }
    } else {
        None
    };
    let (records, stats) =
        run_stream(&mut *a, &mut *b, &stream, &table, args.ulp, probe_spec.as_ref());

    let mut findings_out: Box<dyn Write> = match &args.findings {
        Some(path) => Box::new(
            std::fs::File::create(path).map_err(|e| format!("open {path}: {e}"))?,
        ),
        None => Box::new(std::io::stdout()),
    };
    // Run-manifest meta line (first line, "meta" key distinguishes it from
    // finding records): triage must always know whether a finding came from
    // a C-parity-pinned session or a stock-defaults one.
    writeln!(
        findings_out,
        "{{\"meta\":\"diffrunner\",\"seed\":{},\"guc_pin\":{},\"modules\":\"{}\",\"count\":{},\"profile\":\"{}\"{}{}}}",
        args.seed,
        args.guc_pin,
        args.modules.as_deref().unwrap_or("all-on"),
        args.count,
        fuzzgen::session::json_escape(&args.profile),
        match &args.replay {
            Some(p) => format!(",\"replay\":\"{}\"", fuzzgen::session::json_escape(p)),
            None => String::new(),
        },
        match args.xproto {
            Some(xs) => format!(",\"xproto\":{xs}"),
            None => String::new(),
        }
    )
    .map_err(|e| e.to_string())?;
    for r in &records {
        // Under --xproto each record carries the statement's protocol mode
        // (re-derivable: pure in (xproto seed, sql), but recorded so repros
        // replay exactly without re-deriving by hand).
        let mut line = r.to_jsonl(args.seed);
        if let Some(xs) = args.xproto {
            line.pop();
            line.push_str(&format!(
                ",\"mode\":\"{}\"}}",
                fuzzgen::xproto::mode_string(xs, &r.sql)
            ));
        }
        writeln!(findings_out, "{line}").map_err(|e| e.to_string())?;
    }
    drop(findings_out);

    let error_hist: Vec<String> = stats
        .error_states
        .iter()
        .map(|(state, n)| format!("{state}:{n}"))
        .collect();
    eprintln!(
        "diffrunner: seed={} profile={} guc_pin={} xproto={} applied={} matches={} ruled={} findings={} probes={} errors=[{}]",
        args.seed,
        args.profile,
        if args.guc_pin { "on" } else { "off" },
        match args.xproto {
            Some(xs) => xs.to_string(),
            None => "off".to_string(),
        },
        stats.applied,
        stats.matches,
        stats.ruled,
        stats.findings,
        stats.probes,
        error_hist.join(" ")
    );

    let first_finding: Option<&Record> = records.iter().find(|r| r.is_finding());
    if args.reduce {
        if let Some(f) = first_finding {
            let sqls: Vec<String> = stream.iter().map(|s| s.sql.clone()).collect();
            // Probe findings reduce over the whole stream against the
            // probe's table; statement findings over their prefix.
            // Fixture tables carry their pk in the catalog; ddl-created
            // tables carry it in their probe window.
            let pk_of = |t: &str| {
                catalog
                    .tables
                    .iter()
                    .find(|c| c.name == t)
                    .and_then(|c| c.pk.as_ref())
                    .map(|pk| pk.column.clone())
                    .or_else(|| {
                        ddl_windows
                            .iter()
                            .find(|w| w.table == t)
                            .map(|w| w.pk.clone())
                    })
                    .ok_or_else(|| format!("probe table {t} has no pk in catalog or windows"))
            };
            let pk_col;
            let target = match &f.class {
                DiffClass::StateDiff(t) => {
                    pk_col = pk_of(t)?;
                    // Candidates end at the probe that fired: later
                    // statements may re-converge the state.
                    ReduceTarget::Probe {
                        table: t,
                        pk: &pk_col,
                        upto: f.stmt_index as usize + 1,
                    }
                }
                _ => ReduceTarget::Stmt {
                    fail_idx: f.stmt_index as usize,
                    target_key: f.class.key(),
                },
            };
            let reduction = reduce_stream(&mut factory, &sqls, target, &table, args.ulp)?;
            let path = format!(
                "{}/repro-seed{}-stmt{}-{}.sql",
                args.repro_dir,
                args.seed,
                f.stmt_index,
                f.class.key().to_ascii_lowercase()
            );
            let mut out = String::new();
            out.push_str(&format!(
                "-- diffrunner minimal repro: seed={} guc_pin={} stmt_index={} class={}{}\n-- detail: {}\n",
                args.seed,
                if args.guc_pin { "on" } else { "off" },
                f.stmt_index,
                f.class.key(),
                if f.probe { " (state probe)" } else { "" },
                f.detail.replace('\n', " ")
            ));
            for s in &reduction.stmts {
                out.push_str(s);
                out.push('\n');
            }
            std::fs::write(&path, out).map_err(|e| format!("write {path}: {e}"))?;
            eprintln!(
                "diffrunner: reduced {} -> {} statements in {} probes: {}",
                stream.len(),
                reduction.stmts.len(),
                reduction.probes,
                path
            );
        } else {
            eprintln!("diffrunner: --reduce requested but no findings to reduce");
        }
    }

    Ok(if stats.findings > 0 { ExitCode::from(2) } else { ExitCode::SUCCESS })
}

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(e) => {
            eprintln!("diffrunner: {e}");
            ExitCode::FAILURE
        }
    }
}
