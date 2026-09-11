//! sitediff CLI (plan v2 §3.1, M0 integration): the site runner over a
//! live cell, the rulings audit, and a version banner.
//!
//! `smoke` generates N statements from the existing fuzzgen session
//! (seed + toggles + weights, the same stream `fuzzgen --format jsonl`
//! prints), turns them into StepRecords (`ordered` from the generator's
//! `orderby:*` production tag), and runs them through
//! `runner::SiteRunner` against the two servers a cell describes
//! (`scripts/sitediff-cell.sh up` writes `$WORK/cell.env` +
//! `$WORK/cell.json`; `--a/--b host:port` override). Output is the
//! CONTRACTS.md JSONL set under `--out`: `steps.jsonl`, `obs-a.jsonl`,
//! `obs-b.jsonl`, `findings.jsonl`, plus `summary.json` and
//! `rulings-hits.json` (the `rulings audit` input).
//!
//! `--dry-run` needs no servers: the whole pipeline (runner, tailer,
//! probes, canonicalization, comparator, ledger, finding builder, sinks)
//! runs over a fixture `Observer` that replays the contracts fixtures'
//! WireMsg sequences (`fixtures/contracts/observation-*.json`) — the
//! analyze-1 notice-plane pair is the first step, so a dry run always
//! yields at least that finding.

use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use fuzzgen::catalog::{fixture_ddl, fixture_seed_sql, CatalogSource, FixtureCatalog};
use fuzzgen::client::ConnectOpts;
use fuzzgen::compose::{self, Template};
use fuzzgen::recipes::Bank;
use fuzzgen::contracts::{self, Cell, ObservationRecord, Ordered, Side, StepKind, StepRecord, WireMsg};
use fuzzgen::rng::Rng;
use fuzzgen::rulings::{self, Ledger};
use fuzzgen::runner::{
    load_ledger, ClientObserver, Connector, Exchange, Fault, FileSink, Observer, SideRig, Sink, SiteConfig, SiteRunner,
    SiteSummary,
};
use fuzzgen::session::{run_session, SessionConfig, Statement};
use fuzzgen::toggles::ToggleVector;
use fuzzgen::weights::WeightTable;

const USAGE: &str = "\
usage: sitediff <command> [options]

  smoke        run generated statements against a cell's two servers
    --cell-env <path>    cell.env written by scripts/sitediff-cell.sh up
                         (CELL_ID, WORK, DB, PORT_A, PORT_B, SRV_B, PGBIN)
    --cell-json <path>   cell.json (default: $WORK/cell.json from cell.env;
                         the base cell when neither is given)
    --a <host:port>      A (C 18.6) — overrides cell.env
    --b <host:port>      B (pgrust) — overrides cell.env
    --db <name>          database on both sides (default: cell.env DB, else fuzz)
    --user <name>        role on both sides (default: postgres with a
                         cell.env — the factory initdbs -U postgres — else $USER)
    --password <pw>      password for the hba password/md5/scram rungs
                         (default: $PGPASSWORD)
    --seed <u64>         session seed (default 0); the reproducibility witness
    --count <n>          statement budget (default 20)
    --modules <spec>     pin the toggle vector (fuzzgen --modules)
    --swarm              swarm-random toggle vector from the seed
    --weight <spec>      per-production bias weights (repeatable)
    --max-depth <n>      expression nesting bound (default 4)
    --setup              apply the fixture schema + seed rows on both sides first
    --self-test          run the boot-time tailer self test on B
    --rulings <path>     rulings ledger (default: docs/fuzzing/rulings.toml,
                         else the embedded copy)
    --out <dir>          JSONL output dir (default: out/sitediff-<seed>)
    --dry-run            no servers: replay the contracts fixtures' wire
                         through a fixture Observer
    --compose <p>        wrap each SELECT-shaped generated statement with
                         probability p (0..1) in a composition shell
                         (fuzzgen::compose: secdef-srf, plpgsql-record,
                         cte, prepared, hold-cursor, agg-sublink,
                         multi-statement); the step keeps its production
                         tags and gains compose:<template>
    --compose-templates <a,b,..>  restrict the shells (default: all)

  recipes      run a recipe bank through the cell (same observer,
               comparator, ledger and findings.jsonl as smoke)
    --cell-env/--cell-json/--a/--b/--db/--user/--password/--rulings/--out
                         as for smoke (default --out out/sitediff-recipes)
    --bank <dir>         bank root (default: recipes/; RECIPES.md layout
                         recipes/<subsystem>/<cfile-stem>/<id>.sql)
    --filter <substr>    only recipes whose id contains <substr>
                         (repeatable: any match)
    --env <cell|all>     only recipes whose header env is <cell>
                         (default: CELL_NAME from cell.env, else base)
    --no-reset           keep objects between recipes (default: every
                         recipe's sessions are dropped and every user
                         schema / role / publication / event trigger is
                         removed on both sides before the next recipe)
    --dry-run            list the selected recipes and their steps only
    --canonicalize       rewrite the selected files in canonical form
                         (Recipe::render + recomputed ordered:), no run

  rulings audit  stale / expired verdict over the ledger
    --rulings <path>     ledger (default as above)
    --hits <path>        a rulings-hits.json from one run (repeatable; the
                         last 5 are the audit window)
    --today <date>       YYYY-MM-DD (default: the system clock)

  version
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let r = match args.first().map(String::as_str) {
        Some("smoke") => smoke(&args[1..]),
        Some("recipes") => recipes(&args[1..]),
        Some("rulings") if args.get(1).map(String::as_str) == Some("audit") => rulings_audit(&args[2..]),
        Some("version") => {
            println!("{}", version_banner());
            Ok(())
        }
        Some("--help") | Some("-h") | None => {
            print!("{USAGE}");
            Ok(())
        }
        Some(other) => Err(format!("unknown command {other:?}\n{USAGE}")),
    };
    match r {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("sitediff: {e}");
            ExitCode::FAILURE
        }
    }
}

fn version_banner() -> String {
    let ledger = load_ledger();
    format!(
        "sitediff (fuzzgen {}) contracts cell-base {} · ledger {} rulings · oracle REL_18_6",
        env!("CARGO_PKG_VERSION"),
        Cell::base().cell_id(),
        ledger.len()
    )
}

// ---------------------------------------------------------------------
// smoke
// ---------------------------------------------------------------------

struct SmokeArgs {
    cell_env: Option<PathBuf>,
    cell_json: Option<PathBuf>,
    a: Option<String>,
    b: Option<String>,
    db: Option<String>,
    user: Option<String>,
    password: Option<String>,
    seed: u64,
    count: u32,
    modules: Option<String>,
    swarm: bool,
    weights: WeightTable,
    max_depth: u32,
    setup: bool,
    self_test: bool,
    rulings: Option<PathBuf>,
    out: Option<PathBuf>,
    dry_run: bool,
    /// Composition probability per mille (0 = off).
    compose: u64,
    compose_templates: Vec<Template>,
}

fn parse_smoke(args: &[String]) -> Result<SmokeArgs, String> {
    let mut a = SmokeArgs {
        cell_env: None,
        cell_json: None,
        a: None,
        b: None,
        db: None,
        user: None,
        password: std::env::var("PGPASSWORD").ok(),
        seed: 0,
        count: 20,
        modules: None,
        swarm: false,
        weights: WeightTable::defaults(),
        max_depth: 4,
        setup: false,
        self_test: false,
        rulings: None,
        out: None,
        dry_run: false,
        compose: 0,
        compose_templates: Template::ALL.to_vec(),
    };
    let mut it = args.iter();
    while let Some(k) = it.next() {
        let mut value = |name: &str| it.next().cloned().ok_or_else(|| format!("{name} requires a value"));
        match k.as_str() {
            "--cell-env" => a.cell_env = Some(PathBuf::from(value("--cell-env")?)),
            "--cell-json" => a.cell_json = Some(PathBuf::from(value("--cell-json")?)),
            "--a" => a.a = Some(value("--a")?),
            "--b" => a.b = Some(value("--b")?),
            "--db" => a.db = Some(value("--db")?),
            "--user" => a.user = Some(value("--user")?),
            "--password" => a.password = Some(value("--password")?),
            "--seed" => a.seed = value("--seed")?.parse().map_err(|e| format!("bad --seed: {e}"))?,
            "--count" => a.count = value("--count")?.parse().map_err(|e| format!("bad --count: {e}"))?,
            "--modules" => a.modules = Some(value("--modules")?),
            "--swarm" => a.swarm = true,
            "--weight" => a.weights.apply_spec(&value("--weight")?)?,
            "--max-depth" => a.max_depth = value("--max-depth")?.parse().map_err(|e| format!("bad --max-depth: {e}"))?,
            "--setup" => a.setup = true,
            "--self-test" => a.self_test = true,
            "--rulings" => a.rulings = Some(PathBuf::from(value("--rulings")?)),
            "--out" => a.out = Some(PathBuf::from(value("--out")?)),
            "--dry-run" => a.dry_run = true,
            "--compose" => {
                let p: f64 = value("--compose")?.parse().map_err(|e| format!("bad --compose: {e}"))?;
                if !(0.0..=1.0).contains(&p) {
                    return Err("--compose expects a probability in 0..1".into());
                }
                a.compose = (p * 1000.0).round() as u64;
            }
            "--compose-templates" => {
                a.compose_templates = value("--compose-templates")?
                    .split(',')
                    .filter(|t| !t.trim().is_empty())
                    .map(|t| Template::parse(t.trim()).ok_or_else(|| format!("unknown compose template {t:?}")))
                    .collect::<Result<Vec<_>, _>>()?;
            }
            other => return Err(format!("unknown smoke argument {other:?}\n{USAGE}")),
        }
    }
    if a.modules.is_some() && a.swarm {
        return Err("--modules and --swarm are mutually exclusive".into());
    }
    Ok(a)
}

/// `KEY=VALUE` lines (values optionally single-quoted), as the factory writes them.
fn parse_env_file(path: &Path) -> Result<BTreeMap<String, String>, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let mut m = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((k, v)) = line.split_once('=') {
            let v = v.trim();
            let v = v.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')).unwrap_or(v);
            m.insert(k.trim().to_string(), v.to_string());
        }
    }
    Ok(m)
}

fn split_hostport(s: &str) -> Result<(String, u16), String> {
    let (h, p) = s.rsplit_once(':').ok_or_else(|| format!("expected host:port, got {s:?}"))?;
    Ok((h.to_string(), p.parse().map_err(|e| format!("bad port in {s:?}: {e}"))?))
}

fn ledger_for(path: Option<&Path>) -> Result<Ledger, String> {
    match path {
        Some(p) => Ledger::load(p),
        None => Ok(load_ledger()),
    }
}

/// `ordered` from the generator's production tags (plan §4.1: the
/// `orderby:total` tag is the total-order witness).
fn ordered_of(productions: &[String]) -> Ordered {
    if productions.iter().any(|p| p == "orderby:total") {
        Ordered::Total
    } else if productions.iter().any(|p| p == "orderby:partial") {
        Ordered::Partial
    } else {
        Ordered::None
    }
}

fn step_of(scenario: &str, seq: u64, sql: &str, productions: Vec<String>, ordered: Ordered) -> StepRecord {
    StepRecord {
        scenario: scenario.to_string(),
        seq,
        session: "s1".into(),
        role: "superuser".into(),
        kind: StepKind::Sql,
        sql: Some(sql.to_string()),
        xproto: None,
        productions,
        targets: Vec::new(),
        ordered,
        expect_c: None,
        bracket: None,
        recipe: None,
        mutant: None,
        slots: BTreeMap::new(),
    }
}

fn generate(a: &SmokeArgs) -> Result<Vec<Statement>, String> {
    let catalog = FixtureCatalog.load_catalog()?;
    let toggles = match &a.modules {
        Some(spec) => ToggleVector::parse(spec)?,
        None if a.swarm => ToggleVector::swarm(&mut Rng::new(a.seed ^ 0x7377_6172_6d5f_7476)),
        None => ToggleVector::all_on(),
    };
    let cfg = SessionConfig { seed: a.seed, toggles, weights: a.weights.clone(), budget: a.count, max_depth: a.max_depth };
    Ok(run_session(&cfg, &catalog))
}

fn write_summary(out: &Path, summary: &SiteSummary, hits: &BTreeMap<String, u64>) -> Result<(), String> {
    use contracts::json::Value;
    let v = Value::obj()
        .with("steps", Value::from(summary.steps))
        .with("findings", Value::from(summary.findings))
        .with("divergences", Value::from(summary.divergences))
        .with("ruled", Value::from(summary.ruled))
        .with("crashes_a", Value::from(summary.crashes_a))
        .with("crashes_b", Value::from(summary.crashes_b))
        .with("hangs", Value::from(summary.hangs))
        .with("panics", Value::from(summary.panics))
        .with("probe_rounds", Value::from(summary.probe_rounds))
        .with("prefix_violations", Value::from(summary.prefix_violations.len() as u64))
        .with("invariant_rows", Value::from(summary.invariant_rows.len() as u64))
        .with("invariant_rig_errors", Value::from(summary.invariant_rig_errors.len() as u64))
        .with("steps_ms", Value::from(summary.steps_ms))
        .with("probes_ms", Value::from(summary.probes_ms))
        .with(
            "deck_ms",
            Value::Obj(
                summary
                    .deck_ms
                    .iter()
                    .map(|(k, (ms, runs))| (k.clone(), Value::obj().with("ms", Value::from(*ms)).with("runs", Value::from(*runs))))
                    .collect(),
            ),
        );
    std::fs::write(out.join("summary.json"), contracts::json::to_pretty(&v)?).map_err(|e| e.to_string())?;
    let hits_v = Value::Obj(hits.iter().map(|(k, n)| (k.clone(), Value::from(*n))).collect());
    std::fs::write(out.join("rulings-hits.json"), contracts::json::to_pretty(&hits_v)?).map_err(|e| e.to_string())?;
    Ok(())
}

fn print_summary(out: &Path, s: &SiteSummary) {
    println!(
        "sitediff: {} steps, {} findings ({} divergences, {} ruled), crashes a={} b={}, hangs {}, panics {}, probe rounds {}, prefix violations {}, invariant rows {}, invariant rig errors {}, steps {} ms, probes {} ms ({})",
        s.steps,
        s.findings,
        s.divergences,
        s.ruled,
        s.crashes_a,
        s.crashes_b,
        s.hangs,
        s.panics,
        s.probe_rounds,
        s.prefix_violations.len(),
        s.invariant_rows.len(),
        s.invariant_rig_errors.len(),
        s.steps_ms,
        s.probes_ms,
        s.deck_ms.iter().map(|(k, (ms, n))| format!("{k} {ms} ms/{n}")).collect::<Vec<_>>().join(", ")
    );
    println!("sitediff: wrote {}/{{steps,obs-a,obs-b,findings}}.jsonl + summary.json + rulings-hits.json", out.display());
}

fn smoke(args: &[String]) -> Result<(), String> {
    let a = parse_smoke(args)?;
    let out = a.out.clone().unwrap_or_else(|| PathBuf::from(format!("out/sitediff-{}", a.seed)));
    std::fs::create_dir_all(&out).map_err(|e| format!("create {}: {e}", out.display()))?;
    let ledger = ledger_for(a.rulings.as_deref())?;
    if a.dry_run {
        return dry_run(&a, &out, ledger);
    }
    live(&a, &out, ledger)
}

// ---- live ------------------------------------------------------------

/// The cell half of a live command: cell.env / cell.json / --a / --b /
/// --db / --user / --password, shared by `smoke` and `recipes`.
struct CellArgs {
    cell_env: Option<PathBuf>,
    cell_json: Option<PathBuf>,
    a: Option<String>,
    b: Option<String>,
    db: Option<String>,
    user: Option<String>,
    password: Option<String>,
}

impl CellArgs {
    /// `true` when `k` (with its value, if any) was consumed.
    fn parse_flag(&mut self, k: &str, it: &mut std::slice::Iter<'_, String>) -> Result<bool, String> {
        let mut value = |name: &str| it.next().cloned().ok_or_else(|| format!("{name} requires a value"));
        match k {
            "--cell-env" => self.cell_env = Some(PathBuf::from(value("--cell-env")?)),
            "--cell-json" => self.cell_json = Some(PathBuf::from(value("--cell-json")?)),
            "--a" => self.a = Some(value("--a")?),
            "--b" => self.b = Some(value("--b")?),
            "--db" => self.db = Some(value("--db")?),
            "--user" => self.user = Some(value("--user")?),
            "--password" => self.password = Some(value("--password")?),
            _ => return Ok(false),
        }
        Ok(true)
    }
}

/// A resolved cell: config, the two sides' connect options, B's server
/// pid, and the factory's cell.env map.
struct OpenCell {
    cfg: SiteConfig,
    opts_a: ConnectOpts,
    opts_b: ConnectOpts,
    srv_b: Option<i32>,
}

fn open_cell(a: &CellArgs, out: &Path) -> Result<OpenCell, String> {
    let env = match &a.cell_env {
        Some(p) => parse_env_file(p)?,
        None => BTreeMap::new(),
    };
    let work = env.get("WORK").map(PathBuf::from).or_else(|| a.cell_env.as_ref().and_then(|p| p.parent().map(Path::to_path_buf)));
    let cell_json = a.cell_json.clone().or_else(|| work.as_ref().map(|w| w.join("cell.json")));
    let work = work.unwrap_or_else(|| out.to_path_buf());
    let mut cfg = match cell_json {
        Some(p) if p.exists() => {
            let text = std::fs::read_to_string(&p).map_err(|e| format!("read {}: {e}", p.display()))?;
            match env.get("CELL_ID") {
                Some(id) => SiteConfig::from_cell_json_expecting(&text, &work, id)?,
                None => SiteConfig::from_cell_json(&text, &work)?,
            }
        }
        Some(p) => return Err(format!("cell.json not found: {}", p.display())),
        None => SiteConfig::from_cell(Cell::base(), &work),
    };
    if let Some(bin) = env.get("PGBIN") {
        // A's $libdir from the reference install's pg_config.
        if let Ok(o) = std::process::Command::new(Path::new(bin).join("pg_config")).arg("--pkglibdir").output() {
            if o.status.success() {
                cfg.a_libdir = Some(String::from_utf8_lossy(&o.stdout).trim().to_string());
            }
        }
    }
    let db = a.db.clone().or_else(|| env.get("DB").cloned()).unwrap_or_else(|| "fuzz".into());
    // The factory initdbs both sides with `-U postgres` (sitediff-cell.sh).
    let user = a.user.clone().unwrap_or_else(|| {
        if a.cell_env.is_some() {
            "postgres".to_string()
        } else {
            std::env::var("USER").unwrap_or_else(|_| "postgres".into())
        }
    });
    let side = |flag: &Option<String>, key: &str| -> Result<(String, u16), String> {
        match flag {
            Some(hp) => split_hostport(hp),
            None => {
                let port = env.get(key).ok_or_else(|| format!("no --{} and no {key} in cell.env", key.chars().last().unwrap_or('?').to_ascii_lowercase()))?;
                Ok(("127.0.0.1".into(), port.parse().map_err(|e| format!("bad {key} in cell.env: {e}"))?))
            }
        }
    };
    let (ha, pa) = side(&a.a, "PORT_A")?;
    let (hb, pb) = side(&a.b, "PORT_B")?;
    let mk_opts = |h: &str, p: u16| {
        let mut o = ConnectOpts::new(h, p, &db, &user).timeouts(Some(Duration::from_secs(10)), None, None);
        if let Some(pw) = &a.password {
            o = o.password(pw);
        }
        o
    };
    let opts_a = mk_opts(&ha, pa);
    let opts_b = mk_opts(&hb, pb);
    let srv_b: Option<i32> = env.get("SRV_B").and_then(|s| s.parse().ok());
    Ok(OpenCell { cfg, opts_a, opts_b, srv_b })
}

fn live(a: &SmokeArgs, out: &Path, ledger: Ledger) -> Result<(), String> {
    let cell_args = CellArgs {
        cell_env: a.cell_env.clone(),
        cell_json: a.cell_json.clone(),
        a: a.a.clone(),
        b: a.b.clone(),
        db: a.db.clone(),
        user: a.user.clone(),
        password: a.password.clone(),
    };
    let OpenCell { cfg, opts_a, opts_b, srv_b } = open_cell(&cell_args, out)?;

    if a.setup {
        let catalog = FixtureCatalog.load_catalog()?;
        let ddl = fixture_ddl(&catalog);
        for (name, o) in [("A", &opts_a), ("B", &opts_b)] {
            let mut obs = ClientObserver::connect(o).map_err(|e| format!("setup connect {name}: {e}"))?;
            obs.probe(&ddl, cfg.deadline_ms).map_err(|f| format!("setup DDL on {name}: {f:?}"))?;
            for s in fixture_seed_sql() {
                obs.probe(&s, cfg.deadline_ms).map_err(|f| format!("setup seed on {name}: {f:?}"))?;
            }
        }
    }

    let scenario = format!("seed-{}/cell-{}", a.seed, cfg.cell.conf_profile.clone().unwrap_or_else(|| "base".into()));
    let stmts = generate(a)?;
    let stream: Vec<(String, Vec<String>, Ordered)> =
        stmts.iter().map(|s| (s.sql.clone(), s.productions.clone(), ordered_of(&s.productions))).collect();
    let composed = if a.compose > 0 {
        // A seed-derived stream distinct from the generator's so the
        // wrap decisions never perturb the generated statements.
        let mut rng = Rng::new(a.seed ^ 0x636f_6d70_6f73_6521);
        compose::compose(&stream, a.compose, &a.compose_templates, &mut rng)
    } else {
        compose::compose(&stream, 0, &[], &mut Rng::new(0))
    };
    let wrapped = composed.iter().filter(|c| c.productions.iter().any(|p| p.starts_with("compose:"))).count();
    if a.compose > 0 {
        println!("sitediff smoke: --compose {}/1000: {} generated statements -> {} steps ({} composed)", a.compose, stmts.len(), composed.len(), wrapped);
    }
    let steps: Vec<StepRecord> = composed
        .iter()
        .enumerate()
        .map(|(i, c)| step_of(&scenario, i as u64 + 1, &c.sql, c.productions.clone(), c.ordered))
        .collect();

    let rig_a = SideRig::new(Side::A, &cfg, ClientObserver::connector(opts_a, None), None);
    let rig_b = SideRig::new(Side::B, &cfg, ClientObserver::connector(opts_b, srv_b), None);
    let mut runner = SiteRunner::new(cfg, &scenario, rig_a, rig_b).with_ledger(ledger);
    if a.self_test {
        let v = runner.self_test()?;
        println!("sitediff smoke: tailer self test: {v:?}");
    }
    let mut sink = FileSink::create(out).map_err(|e| format!("open sinks under {}: {e}", out.display()))?;
    let summary = runner.run_stream(&steps, &mut sink);
    write_summary(out, &summary, &runner.ledger.hits())?;
    print_summary(out, &summary);
    Ok(())
}

// ---------------------------------------------------------------------
// recipes
// ---------------------------------------------------------------------

struct RecipesArgs {
    cell: CellArgs,
    bank: PathBuf,
    filters: Vec<String>,
    env: Option<String>,
    reset: bool,
    rulings: Option<PathBuf>,
    out: Option<PathBuf>,
    dry_run: bool,
    canonicalize: bool,
}

fn parse_recipes(args: &[String]) -> Result<RecipesArgs, String> {
    let mut a = RecipesArgs {
        cell: CellArgs { cell_env: None, cell_json: None, a: None, b: None, db: None, user: None, password: std::env::var("PGPASSWORD").ok() },
        bank: PathBuf::from("recipes"),
        filters: Vec::new(),
        env: None,
        reset: true,
        rulings: None,
        out: None,
        dry_run: false,
        canonicalize: false,
    };
    let mut it = args.iter();
    while let Some(k) = it.next() {
        if a.cell.parse_flag(k, &mut it)? {
            continue;
        }
        let mut value = |name: &str| it.next().cloned().ok_or_else(|| format!("{name} requires a value"));
        match k.as_str() {
            "--bank" => a.bank = PathBuf::from(value("--bank")?),
            "--filter" => a.filters.push(value("--filter")?),
            "--env" => a.env = Some(value("--env")?),
            "--no-reset" => a.reset = false,
            "--rulings" => a.rulings = Some(PathBuf::from(value("--rulings")?)),
            "--out" => a.out = Some(PathBuf::from(value("--out")?)),
            "--dry-run" => a.dry_run = true,
            "--canonicalize" => a.canonicalize = true,
            other => return Err(format!("unknown recipes argument {other:?}\n{USAGE}")),
        }
    }
    Ok(a)
}

/// The between-recipes reset, one simple-query message on `s1` of both
/// sides: drop every user schema (public is recreated with initdb's
/// ownership and grants), event trigger, publication and role other
/// than the connecting one, with notices muted so the reset itself
/// carries no wire to compare beyond the tags.
const RESET_SQL: &str = "SET client_min_messages = warning; \
DO $fzr$ DECLARE r record; BEGIN \
FOR r IN SELECT evtname FROM pg_event_trigger LOOP EXECUTE format('DROP EVENT TRIGGER %I', r.evtname); END LOOP; \
FOR r IN SELECT pubname FROM pg_publication LOOP EXECUTE format('DROP PUBLICATION %I', r.pubname); END LOOP; \
FOR r IN SELECT nspname FROM pg_namespace WHERE nspname NOT LIKE 'pg\\_%' AND nspname <> 'information_schema' LOOP EXECUTE format('DROP SCHEMA %I CASCADE', r.nspname); END LOOP; \
FOR r IN SELECT rolname FROM pg_roles WHERE rolname NOT LIKE 'pg\\_%' AND rolname <> current_user LOOP EXECUTE format('DROP OWNED BY %I', r.rolname); EXECUTE format('DROP ROLE %I', r.rolname); END LOOP; \
CREATE SCHEMA public AUTHORIZATION pg_database_owner; GRANT USAGE ON SCHEMA public TO PUBLIC; \
END $fzr$; \
RESET client_min_messages;";

/// A `Sink` wrapper that checks each step's `expect_c` against A's wire
/// (`E:<sqlstate>:<message>` / `N:<severity>:<message>` per RECIPES.md;
/// the message part is a prefix match after `%s`-style placeholders are
/// cut) and tallies findings per recipe.
struct RecipeSink<S: Sink> {
    inner: S,
    expects: BTreeMap<u64, (String, String)>,
    /// recipe id → (steps, findings)
    per_recipe: BTreeMap<String, (u64, u64)>,
    current_recipe: Option<String>,
    misses: Vec<(String, u64, String, String)>,
    hits: u64,
}

impl<S: Sink> RecipeSink<S> {
    fn new(inner: S) -> RecipeSink<S> {
        RecipeSink { inner, expects: BTreeMap::new(), per_recipe: BTreeMap::new(), current_recipe: None, misses: Vec::new(), hits: 0 }
    }
}

/// `true` when A's wire carries the expectation.
fn expect_matches(expect: &str, wire: &[WireMsg]) -> bool {
    let mut parts = expect.splitn(3, ':');
    let kind = parts.next().unwrap_or("");
    let code = parts.next().unwrap_or("");
    let msg = parts.next().unwrap_or("");
    // Cut the template at the first placeholder.
    let prefix = msg.split("%s").next().unwrap_or("").split("%d").next().unwrap_or("").trim_end();
    let field = |f: &contracts::ErrFields, k: char| f.get(&k).and_then(|b| std::str::from_utf8(&b.0).ok()).unwrap_or("").to_string();
    wire.iter().any(|m| match (kind, m) {
        ("E", WireMsg::ErrorResponse(f)) => (code == "-" || field(f, 'C') == code) && field(f, 'M').starts_with(prefix),
        ("N", WireMsg::NoticeResponse(f)) => (code.is_empty() || field(f, 'S') == code) && field(f, 'M').starts_with(prefix),
        _ => false,
    })
}

impl<S: Sink> Sink for RecipeSink<S> {
    fn step(&mut self, rec: &StepRecord) {
        self.current_recipe = rec.recipe.clone();
        if let Some(r) = &rec.recipe {
            self.per_recipe.entry(r.clone()).or_default().0 += 1;
        }
        if let (Some(e), Some(r)) = (&rec.expect_c, &rec.recipe) {
            self.expects.insert(rec.seq, (r.clone(), e.clone()));
        }
        self.inner.step(rec);
    }
    fn observation(&mut self, rec: &ObservationRecord) {
        if rec.side == Side::A {
            if let Some((r, e)) = self.expects.remove(&rec.seq) {
                if expect_matches(&e, &rec.wire) {
                    self.hits += 1;
                } else {
                    let got = rec
                        .wire
                        .iter()
                        .filter_map(|m| match m {
                            WireMsg::ErrorResponse(f) | WireMsg::NoticeResponse(f) => {
                                let g = |k: char| f.get(&k).and_then(|b| std::str::from_utf8(&b.0).ok()).unwrap_or("").to_string();
                                Some(format!("{}:{}:{}", g('S'), g('C'), g('M')))
                            }
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join(" | ");
                    self.misses.push((r, rec.seq, e, got));
                }
            }
        }
        self.inner.observation(rec);
    }
    fn finding(&mut self, f: &contracts::Finding) {
        if let Some(r) = f.recipe.clone().or_else(|| self.current_recipe.clone()) {
            self.per_recipe.entry(r).or_default().1 += 1;
        }
        self.inner.finding(f);
    }
    fn note(&mut self, v: &contracts::json::Value) {
        self.inner.note(v);
    }
}

fn recipes(args: &[String]) -> Result<(), String> {
    let a = parse_recipes(args)?;
    let out = a.out.clone().unwrap_or_else(|| PathBuf::from("out/sitediff-recipes"));
    std::fs::create_dir_all(&out).map_err(|e| format!("create {}: {e}", out.display()))?;
    let ledger = ledger_for(a.rulings.as_deref())?;
    let bank = Bank::load(&a.bank).map_err(|e| format!("bank {}: {e}", a.bank.display()))?;
    for e in &bank.errors {
        eprintln!("sitediff recipes: skipping {}: {}", e.rel_path, e.message);
    }
    let env_from_cell = a
        .cell
        .cell_env
        .as_ref()
        .and_then(|p| parse_env_file(p).ok())
        .and_then(|m| m.get("CELL_NAME").cloned())
        .unwrap_or_else(|| "base".into());
    let env_filter = a.env.clone().unwrap_or(env_from_cell);
    let selected: Vec<&fuzzgen::recipes::BankEntry> = bank
        .recipes
        .values()
        .filter(|e| env_filter == "all" || e.recipe.header.env == env_filter)
        .filter(|e| a.filters.is_empty() || a.filters.iter().any(|f| e.id().contains(f.as_str())))
        .collect();
    println!(
        "sitediff recipes: bank {} has {} recipes ({} errors); {} selected (env {}{})",
        a.bank.display(),
        bank.len(),
        bank.errors.len(),
        selected.len(),
        env_filter,
        if a.filters.is_empty() { String::new() } else { format!(", filter {:?}", a.filters) }
    );
    if selected.is_empty() {
        return Err("no recipes selected".into());
    }
    if a.canonicalize {
        let mut changed = 0usize;
        for e in &selected {
            let mut r = e.recipe.clone();
            r.header.ordered = r.ordered_summary();
            let text = r.render();
            let path = a.bank.join(&e.rel_path);
            let old = std::fs::read_to_string(&path).map_err(|e| format!("read {}: {e}", path.display()))?;
            if old != text {
                std::fs::write(&path, text).map_err(|e| format!("write {}: {e}", path.display()))?;
                changed += 1;
            }
        }
        println!("sitediff recipes: canonicalized {} of {} files", changed, selected.len());
        return Ok(());
    }
    let scenario = format!("recipes-{}/cell-{}", a.bank.file_name().map(|s| s.to_string_lossy().into_owned()).unwrap_or_else(|| "bank".into()), env_filter);

    // The stream: per recipe its steps (from RecipeHeader/body), then the
    // teardown: a Disconnect per session the recipe used and the reset.
    let mut steps: Vec<StepRecord> = Vec::new();
    let mut skipped_non_wire = 0usize;
    let mut seq = 1u64;
    let mut order: Vec<String> = Vec::new();
    for e in &selected {
        order.push(e.id().to_string());
        let mut sessions: Vec<String> = Vec::new();
        for mut st in e.recipe.to_steps(&scenario, seq) {
            if !sessions.contains(&st.session) {
                sessions.push(st.session.clone());
            }
            match &st.kind {
                StepKind::CopyIn => {
                    // `COPY ... FROM STDIN;` + data lines + `\.` → the
                    // statement on the wire and the data in the slot the
                    // client observer reads.
                    let text = st.sql.clone().unwrap_or_default();
                    let mut lines = text.lines();
                    let stmt = lines.next().unwrap_or("").to_string();
                    let data: Vec<&str> = lines.take_while(|l| l.trim() != "\\.").collect();
                    let mut d = data.join("\n");
                    if !d.is_empty() {
                        d.push('\n');
                    }
                    st.sql = Some(stmt);
                    st.slots.insert("copy_data".into(), d);
                }
                StepKind::Env(op) if op == "psql" || op == "manual" => {
                    // Not wire (RECIPES.md: run by stock psql / a harness);
                    // recorded in steps.jsonl, exchanged as a no-op.
                    skipped_non_wire += 1;
                }
                _ => {}
            }
            st.seq = seq;
            seq += 1;
            steps.push(st);
        }
        if a.reset {
            for s in &sessions {
                let mut d = step_of(&scenario, seq, "", vec!["recipe:teardown".into()], Ordered::None);
                d.kind = StepKind::Disconnect;
                d.sql = None;
                d.session = s.clone();
                d.recipe = Some(e.id().to_string());
                seq += 1;
                steps.push(d);
            }
            let mut r = step_of(&scenario, seq, RESET_SQL, vec!["recipe:reset".into()], Ordered::None);
            r.recipe = Some(e.id().to_string());
            seq += 1;
            steps.push(r);
        }
    }
    println!("sitediff recipes: {} steps ({} non-wire psql/manual steps exchanged as no-ops)", steps.len(), skipped_non_wire);
    if a.dry_run {
        for s in &steps {
            println!("  {:>4} {:<4} {:<10} {} {}", s.seq, s.session, s.kind.to_string_key(), s.recipe.as_deref().unwrap_or("-"), s.sql.as_deref().unwrap_or("").lines().next().unwrap_or(""));
        }
        return Ok(());
    }

    let OpenCell { cfg, opts_a, opts_b, srv_b } = open_cell(&a.cell, &out)?;
    let rig_a = SideRig::new(Side::A, &cfg, ClientObserver::connector(opts_a, None), None);
    let rig_b = SideRig::new(Side::B, &cfg, ClientObserver::connector(opts_b, srv_b), None);
    let mut runner = SiteRunner::new(cfg, &scenario, rig_a, rig_b).with_ledger(ledger);
    let file_sink = FileSink::create(&out).map_err(|e| format!("open sinks under {}: {e}", out.display()))?;
    let mut sink = RecipeSink::new(file_sink);
    let summary = runner.run_stream(&steps, &mut sink);
    write_summary(&out, &summary, &runner.ledger.hits())?;
    print_summary(&out, &summary);

    // Per-recipe report.
    use contracts::json::Value;
    let mut rows = Vec::new();
    let mut with_findings = 0usize;
    for id in &order {
        let (n, f) = sink.per_recipe.get(id).copied().unwrap_or((0, 0));
        if f > 0 {
            with_findings += 1;
        }
        let misses: Vec<Value> = sink
            .misses
            .iter()
            .filter(|m| &m.0 == id)
            .map(|m| Value::obj().with("seq", Value::from(m.1)).with("expect", Value::Str(m.2.clone())).with("a_wire", Value::Str(m.3.clone())))
            .collect();
        rows.push(
            Value::obj()
                .with("id", Value::Str(id.clone()))
                .with("steps", Value::from(n))
                .with("findings", Value::from(f))
                .with("expect_misses", Value::Arr(misses)),
        );
    }
    let report = Value::obj()
        .with("bank", Value::Str(a.bank.display().to_string()))
        .with("env", Value::Str(env_filter.clone()))
        .with("recipes", Value::from(order.len() as u64))
        .with("recipes_with_findings", Value::from(with_findings as u64))
        .with("expect_hits", Value::from(sink.hits))
        .with("expect_misses", Value::from(sink.misses.len() as u64))
        .with("per_recipe", Value::Arr(rows));
    std::fs::write(out.join("recipes-report.json"), contracts::json::to_pretty(&report)?).map_err(|e| e.to_string())?;
    println!(
        "sitediff recipes: {} recipes, {} with findings; expect_c hits {} misses {}; wrote {}/recipes-report.json",
        order.len(),
        with_findings,
        sink.hits,
        sink.misses.len(),
        out.display()
    );
    for m in &sink.misses {
        println!("  expect miss {} seq {}: wanted {:?}, A sent {:?}", m.0, m.1, m.2, m.3);
    }
    Ok(())
}

// ---- dry run ---------------------------------------------------------

/// Replays scripted wire per exchange; probes answer an empty result.
struct FixtureObserver {
    script: std::rc::Rc<std::cell::RefCell<VecDeque<Vec<WireMsg>>>>,
    pid: u32,
}

impl Observer for FixtureObserver {
    fn exchange(&mut self, _step: &StepRecord, _deadline_ms: u64) -> Result<Exchange, Fault> {
        let wire = self.script.borrow_mut().pop_front().unwrap_or_else(|| vec![WireMsg::ReadyForQuery { status: 'I' }]);
        Ok(Exchange { wire, ms: 1 })
    }
    fn resume(&mut self, _deadline_ms: u64) -> Result<Exchange, Fault> {
        Err(Fault::Lost("fixture observer never hangs".into()))
    }
    fn probe(&mut self, _sql: &str, _deadline_ms: u64) -> Result<Exchange, Fault> {
        Ok(Exchange { wire: vec![WireMsg::CommandComplete(contracts::Bytes::text("SELECT 0")), WireMsg::ReadyForQuery { status: 'I' }], ms: 0 })
    }
    fn backend_pid(&self) -> Option<u32> {
        Some(self.pid)
    }
    fn os_pid(&self) -> Option<i32> {
        None
    }
    fn reconnect(&mut self) -> Result<(), String> {
        Ok(())
    }
    fn version(&self) -> String {
        "18.6 (fixture)".into()
    }
}

fn fixture_wire(text: &str) -> Result<Vec<WireMsg>, String> {
    Ok(ObservationRecord::from_json(&contracts::json::parse(text)?)?.wire)
}

fn dry_run(a: &SmokeArgs, out: &Path, ledger: Ledger) -> Result<(), String> {
    let cell = match &a.cell_json {
        Some(p) => {
            let text = std::fs::read_to_string(p).map_err(|e| format!("read {}: {e}", p.display()))?;
            SiteConfig::from_cell_json(&text, out)?
        }
        None => SiteConfig::from_cell(Cell::base(), out),
    };
    // Empty logs so the tailer path runs (no server writes them here).
    for p in [&cell.a_log, &cell.b_log] {
        std::fs::write(p, b"").map_err(|e| format!("create {}: {e}", p.display()))?;
    }
    let notice = fixture_wire(include_str!("../../fixtures/contracts/observation-notice.json"))?;
    let error = fixture_wire(include_str!("../../fixtures/contracts/observation-error.json"))?;
    let xstep = StepRecord::from_json(&contracts::json::parse(include_str!("../../fixtures/contracts/step-record-xproto.json"))?)?;
    // The row group of the notice fixture (T, D, D, C, Z) on its own.
    let rows: Vec<WireMsg> = notice
        .iter()
        .skip_while(|m| !matches!(m, WireMsg::RowDescription(_)))
        .filter(|m| !matches!(m, WireMsg::NotificationResponse { .. }))
        .cloned()
        .collect();
    let no_notices: Vec<WireMsg> = notice.iter().filter(|m| !matches!(m, WireMsg::NoticeResponse(_))).cloned().collect();

    let scenario = format!("seed-{}/cell-{}", a.seed, cell.cell.conf_profile.clone().unwrap_or_else(|| "base".into()));
    let mut steps = vec![
        // analyze-1: INFO stream on A, absent on B.
        step_of(&scenario, 1, "ANALYZE VERBOSE t;", vec!["util.analyze".into()], Ordered::None),
        // The error fixture on both sides: a match.
        step_of(&scenario, 2, "INSERT INTO t VALUES (NULL, 1);", vec!["dml.insert".into()], Ordered::None),
    ];
    let mut x = xstep.clone();
    x.scenario = scenario.clone();
    x.seq = 3;
    steps.push(x);
    let script_a: VecDeque<Vec<WireMsg>> = vec![notice.clone(), error.clone(), rows.clone()].into();
    let script_b: VecDeque<Vec<WireMsg>> = vec![no_notices, error, rows].into();
    let mk = |script: VecDeque<Vec<WireMsg>>, pid: u32| -> Connector {
        let s = std::rc::Rc::new(std::cell::RefCell::new(script));
        Box::new(move |_name| Ok(Box::new(FixtureObserver { script: s.clone(), pid }) as Box<dyn Observer>))
    };
    let rig_a = SideRig::new(Side::A, &cell, mk(script_a, 41233), None);
    let rig_b = SideRig::new(Side::B, &cell, mk(script_b, 19), None);
    let mut runner = SiteRunner::new(cell, &scenario, rig_a, rig_b).with_ledger(ledger);
    let mut sink = FileSink::create(out).map_err(|e| format!("open sinks under {}: {e}", out.display()))?;
    let summary = runner.run_stream(&steps, &mut sink);
    write_summary(out, &summary, &runner.ledger.hits())?;
    print_summary(out, &summary);
    let findings = std::fs::read_to_string(out.join("findings.jsonl")).unwrap_or_default();
    for line in findings.lines() {
        let v = contracts::json::parse(line)?;
        let get = |k: &str| v.get(k).and_then(|x| x.as_str()).unwrap_or("-").to_string();
        if v.get("signature").is_some() && v.get("plane").is_some() {
            println!("  finding {} {} {} sig={:?} rule={}", get("status"), get("plane"), get("class"), get("signature"), get("rule"));
        } else {
            println!("  note {line}");
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------
// rulings audit
// ---------------------------------------------------------------------

fn today_utc() -> String {
    let secs = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    let (y, m, d) = rulings::civil_from_days((secs / 86_400) as i64);
    format!("{y:04}-{m:02}-{d:02}")
}

fn rulings_audit(args: &[String]) -> Result<(), String> {
    let mut path: Option<PathBuf> = None;
    let mut hits: Vec<PathBuf> = Vec::new();
    let mut today: Option<String> = None;
    let mut it = args.iter();
    while let Some(k) = it.next() {
        let mut value = |name: &str| it.next().cloned().ok_or_else(|| format!("{name} requires a value"));
        match k.as_str() {
            "--rulings" => path = Some(PathBuf::from(value("--rulings")?)),
            "--hits" => hits.push(PathBuf::from(value("--hits")?)),
            "--today" => today = Some(value("--today")?),
            other => return Err(format!("unknown rulings audit argument {other:?}\n{USAGE}")),
        }
    }
    let ledger = ledger_for(path.as_deref())?;
    let mut runs: Vec<BTreeMap<String, u64>> = Vec::new();
    for h in &hits {
        let text = std::fs::read_to_string(h).map_err(|e| format!("read {}: {e}", h.display()))?;
        let v = contracts::json::parse(&text)?;
        let obj = v.as_obj().ok_or_else(|| format!("{}: expected an object of id -> hits", h.display()))?;
        let mut m = BTreeMap::new();
        for (id, n) in obj {
            m.insert(id.clone(), n.as_i64().unwrap_or(0).max(0) as u64);
        }
        runs.push(m);
    }
    let today = today.unwrap_or_else(today_utc);
    let report = ledger.audit(&runs, &today)?;
    print!("{}", report.render());
    if report.hard_fail() {
        return Err("rulings audit: STALE rulings (hard fail)".into());
    }
    Ok(())
}
