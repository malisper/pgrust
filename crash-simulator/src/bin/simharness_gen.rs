//! simharness-gen — WS-GEN's gen-only CLI (bootstrap binary).
//!
//! WS-RUNNER's `simharness` binary owns run/replay/bugbase; this tool covers
//! the generation-only gates and stays useful afterwards as the plan
//! inspection/smoke tool:
//!
//!   simharness-gen gen       --seed N --profile FILE [--out FILE]
//!   simharness-gen gen-batch --seed-base N --count K --profile FILE --out-dir DIR
//!   simharness-gen smoke     --profile-dir DIR --count N [--seed-base B] [--census FILE]
//!
//! Generator version pin: SIMHARNESS_GENERATOR_SHA env if set, else
//! `git rev-parse --short=12 HEAD`, else "unknown". Timestamps never enter
//! plan bytes (determinism law A3).

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::exit;

use simharness::gen::profile::GenProfile;
use simharness::gen::screens;
use simharness::gen::generate_plan;
use simharness::plan::{self, Plan, PlanItem, Step};

fn generator_sha() -> String {
    if let Ok(s) = std::env::var("SIMHARNESS_GENERATOR_SHA") {
        if !s.is_empty() && !s.chars().any(|c| c.is_whitespace()) {
            return s;
        }
    }
    if let Ok(out) = std::process::Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
    {
        if out.status.success() {
            if let Ok(s) = String::from_utf8(out.stdout) {
                let s = s.trim().to_string();
                if !s.is_empty() {
                    return s;
                }
            }
        }
    }
    "unknown".to_string()
}

fn die(msg: &str) -> ! {
    eprintln!("simharness-gen: {msg}");
    exit(2);
}

fn parse_flags(args: &[String]) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let mut i = 0;
    while i < args.len() {
        let k = &args[i];
        if !k.starts_with("--") {
            die(&format!("unexpected argument '{k}'"));
        }
        let Some(v) = args.get(i + 1) else {
            die(&format!("flag {k} needs a value"));
        };
        out.insert(k[2..].to_string(), v.clone());
        i += 2;
    }
    out
}

fn load_profile(path: &Path) -> (GenProfile, String) {
    let bytes = fs::read(path)
        .unwrap_or_else(|e| die(&format!("cannot read profile {}: {e}", path.display())));
    GenProfile::from_bytes(&bytes)
        .unwrap_or_else(|e| die(&format!("profile {}: {e}", path.display())))
}

#[derive(Default, Clone)]
struct Census {
    ddl: u64,
    dml: u64,
    query: u64,
    tx: u64,
    arm: u64,
    fault: u64,
    assume: u64,
    assert_: u64,
    property_blocks: u64,
    order_underdetermined: u64,
    float_lenient: u64,
    session: u64,
}

impl Census {
    fn add_step(&mut self, s: &Step) {
        match s {
            Step::Ddl(_) => self.ddl += 1,
            Step::Dml(_) => self.dml += 1,
            Step::Query(q) => {
                self.query += 1;
                if q.flags.order_underdetermined {
                    self.order_underdetermined += 1;
                }
                if q.flags.float_lenient {
                    self.float_lenient += 1;
                }
            }
            Step::Tx(_) => self.tx += 1,
            Step::Arm(_) => self.arm += 1,
            Step::Assumption(_) => self.assume += 1,
            Step::Assertion(_) => self.assert_ += 1,
            Step::Fault(_) => self.fault += 1,
            // H8 session-family steps (multi-session properties).
            Step::Session(_) | Step::AsyncDml(_) | Step::Join(_) | Step::WaitUntil(_) => {
                self.session += 1
            }
        }
    }

    fn add_plan(&mut self, p: &Plan) {
        for item in &p.items {
            match item {
                PlanItem::Step(s) => self.add_step(s),
                PlanItem::Property { steps, .. } => {
                    self.property_blocks += 1;
                    for s in steps {
                        self.add_step(s);
                    }
                }
            }
        }
    }

    fn merge(&mut self, o: &Census) {
        self.ddl += o.ddl;
        self.dml += o.dml;
        self.query += o.query;
        self.tx += o.tx;
        self.arm += o.arm;
        self.fault += o.fault;
        self.assume += o.assume;
        self.assert_ += o.assert_;
        self.property_blocks += o.property_blocks;
        self.order_underdetermined += o.order_underdetermined;
        self.float_lenient += o.float_lenient;
    }

    fn tsv(&self) -> String {
        format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            self.ddl,
            self.dml,
            self.query,
            self.tx,
            self.arm,
            self.fault,
            self.assume,
            self.assert_,
            self.property_blocks,
            self.order_underdetermined,
            self.float_lenient
        )
    }

    fn kv(&self) -> String {
        format!(
            "ddl={} dml={} query={} tx={} arm={} fault={} assume={} assert={} props={} orderund={} floatlen={}",
            self.ddl,
            self.dml,
            self.query,
            self.tx,
            self.arm,
            self.fault,
            self.assume,
            self.assert_,
            self.property_blocks,
            self.order_underdetermined,
            self.float_lenient
        )
    }
}

const CENSUS_TSV_HEADER: &str =
    "profile\tseed\tddl\tdml\tquery\ttx\tarm\tfault\tassume\tassert\tprops\torderund\tfloatlen";

fn cmd_gen(flags: BTreeMap<String, String>) {
    let seed: u64 = flags
        .get("seed")
        .unwrap_or_else(|| die("gen needs --seed"))
        .parse()
        .unwrap_or_else(|_| die("bad --seed"));
    let ppath = PathBuf::from(flags.get("profile").unwrap_or_else(|| die("gen needs --profile")));
    let (profile, sha) = load_profile(&ppath);
    let plan = generate_plan(seed, &profile, &sha, &generator_sha(), &[]);
    let text = plan::render(&plan);
    match flags.get("out") {
        Some(o) => fs::write(o, &text).unwrap_or_else(|e| die(&format!("write {o}: {e}"))),
        None => print!("{text}"),
    }
}

fn cmd_gen_batch(flags: BTreeMap<String, String>) {
    let base: u64 = flags
        .get("seed-base")
        .unwrap_or_else(|| die("gen-batch needs --seed-base"))
        .parse()
        .unwrap_or_else(|_| die("bad --seed-base"));
    let count: u64 = flags
        .get("count")
        .unwrap_or_else(|| die("gen-batch needs --count"))
        .parse()
        .unwrap_or_else(|_| die("bad --count"));
    let ppath =
        PathBuf::from(flags.get("profile").unwrap_or_else(|| die("gen-batch needs --profile")));
    let out_dir =
        PathBuf::from(flags.get("out-dir").unwrap_or_else(|| die("gen-batch needs --out-dir")));
    fs::create_dir_all(&out_dir).unwrap_or_else(|e| die(&format!("mkdir out-dir: {e}")));
    let (profile, sha) = load_profile(&ppath);
    let gsha = generator_sha();
    for i in 0..count {
        let seed = base + i;
        let plan = generate_plan(seed, &profile, &sha, &gsha, &[]);
        let text = plan::render(&plan);
        let path = out_dir.join(format!("{}-{seed}.plan", profile.name));
        fs::write(&path, &text)
            .unwrap_or_else(|e| die(&format!("write {}: {e}", path.display())));
    }
    println!("SIMHARNESS|gen-batch|{}|{count}", profile.name);
}

fn cmd_smoke(flags: BTreeMap<String, String>) {
    let dir = PathBuf::from(
        flags.get("profile-dir").unwrap_or_else(|| die("smoke needs --profile-dir")),
    );
    let count: u64 = flags
        .get("count")
        .unwrap_or_else(|| die("smoke needs --count"))
        .parse()
        .unwrap_or_else(|_| die("bad --count"));
    let base: u64 =
        flags.get("seed-base").map(|s| s.parse().unwrap_or_else(|_| die("bad --seed-base"))).unwrap_or(1);
    let mut profiles: Vec<PathBuf> = fs::read_dir(&dir)
        .unwrap_or_else(|e| die(&format!("read profile-dir: {e}")))
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    profiles.sort();
    if profiles.is_empty() {
        die("no *.json profiles in --profile-dir");
    }
    let gsha = generator_sha();
    let per = count / profiles.len() as u64;
    let extra = count % profiles.len() as u64;
    let mut census_lines = vec![CENSUS_TSV_HEADER.to_string()];
    let mut violations = 0u64;
    let mut roundtrip_failures = 0u64;
    let mut plans_done = 0u64;
    for (pi, ppath) in profiles.iter().enumerate() {
        let (profile, sha) = load_profile(ppath);
        let n = per + if (pi as u64) < extra { 1 } else { 0 };
        let mut agg = Census::default();
        for i in 0..n {
            let seed = base + plans_done + i;
            let plan = generate_plan(seed, &profile, &sha, &gsha, &[]);
            let text = plan::render(&plan);
            // Re-parse and compare IR (round-trip law).
            match plan::parse(&text) {
                Ok(parsed) if parsed == plan => {}
                Ok(_) => {
                    eprintln!("ROUNDTRIP-MISMATCH profile={} seed={seed}", profile.name);
                    roundtrip_failures += 1;
                }
                Err(e) => {
                    eprintln!("ROUNDTRIP-PARSE-FAIL profile={} seed={seed}: {e}", profile.name);
                    roundtrip_failures += 1;
                }
            }
            // Screen lint (R2 / R3R6 / R7 backstop).
            for v in screens::lint_plan(&plan) {
                eprintln!("SCREEN-VIOLATION profile={} seed={seed} {}: {}", profile.name, v.rule, v.detail);
                violations += 1;
            }
            let mut c = Census::default();
            c.add_plan(&plan);
            census_lines.push(format!("{}\t{seed}\t{}", profile.name, c.tsv()));
            agg.merge(&c);
        }
        plans_done += n;
        println!("SIMHARNESS|gen-census|{}|plans={n} {}", profile.name, agg.kv());
    }
    if let Some(cpath) = flags.get("census") {
        fs::write(cpath, census_lines.join("\n") + "\n")
            .unwrap_or_else(|e| die(&format!("write census: {e}")));
    }
    println!("SIMHARNESS|gen-smoke-plans|{plans_done}");
    println!("SIMHARNESS|screen-violations|{violations}");
    println!("SIMHARNESS|roundtrip-failures|{roundtrip_failures}");
    if violations == 0 && roundtrip_failures == 0 && plans_done == count {
        println!("SIMHARNESS-VERDICT|PASS");
    } else {
        println!("SIMHARNESS-VERDICT|FAIL:gen-smoke");
        exit(1);
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(cmd) = args.first() else {
        die("usage: simharness-gen <gen|gen-batch|smoke> [flags]");
    };
    let flags = parse_flags(&args[1..]);
    match cmd.as_str() {
        "gen" => cmd_gen(flags),
        "gen-batch" => cmd_gen_batch(flags),
        "smoke" => cmd_smoke(flags),
        other => die(&format!("unknown command '{other}'")),
    }
}
