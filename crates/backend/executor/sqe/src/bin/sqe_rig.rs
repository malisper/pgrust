//! sqe_rig <bankdir> [--q 0,7,19] [--suite] [--sql] [--oracle]
//!         [--unseen] [--reps N] [--phase] [--elected-only]
//! --elected-only = elected arm alone, first; oracle fails exit 1 at end.

// The PRODUCTION allocator (the server binary's global allocator). The §9
// comparator is production-engine-bare: with the rig on the system
// allocator, the two arms differed in allocator policy, not engine work —
// the system allocator RETAINS the shrink-law-dropped pass-1 scatter
// arenas across reps while mimalloc purges them, so the native arm
// under-priced every fresh execution by the arena recommit/refault cost
// (the q32-residual conviction, 2026-08-18: ~35ms/execute at 100m width
// 64 — phase timers place it inside pass1/pass2, and it reproduces by
// allocator swap alone).
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("usage: sqe_rig <bankdir> [--q list] [--suite] [--sql] [--oracle] [--unseen] [--reps N] [--phase]");
        std::process::exit(2);
    }
    let dir = &args[0];
    let mut qs: Vec<u32> = vec![0, 7, 19, 32, 37];
    let mut sql = false;
    let mut oracle = false;
    let mut unseen = false;
    let mut elected_only = false;
    let mut reps = 3usize;
    let mut text: Option<String> = None;
    let mut schema: Option<String> = None;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--q" => {
                i += 1;
                qs = args[i].split(',').map(|s| s.parse().unwrap()).collect();
            }
            "--suite" | "--suite-full" => qs = (0..43).collect(),
            "--sql" => sql = true,
            "--oracle" => oracle = true,
            "--unseen" => unseen = true,
            "--reps" => {
                i += 1;
                reps = args[i].parse().unwrap();
            }
            "--phase" => {
                sqe::engine::PHASE_ON.store(true, std::sync::atomic::Ordering::Relaxed)
            }
            "--fp" | "--fpcache" => {}
            // Ad-hoc SQL text over the bank's own schema (diagnosis lanes:
            // synthetic banks whose attnos differ from the canonical hits
            // schema). Same lower + same arms as --sql; q id 999.
            "--text" => {
                i += 1;
                text = Some(args[i].clone());
            }
            // Bank schema override for --text: "name:int8,name:int4,..."
            // (attnos 1..n in list order — the diagnosis-bank contract).
            "--schema" => {
                i += 1;
                schema = Some(args[i].clone());
            }
            "--elected-only" => elected_only = true,
            other => {
                eprintln!("sqe_rig: unknown flag {other}");
                std::process::exit(2);
            }
        }
        i += 1;
    }
    sqe::coldledger::set_main_thread();
    let t0 = std::time::Instant::now();
    let cfg = sqe::engine::SqeConfig {
        threads: sqe::rig::thread_count(),
        ..sqe::engine::SqeConfig::default()
    };
    let engine = match &schema {
        None => sqe::rig::open_engine_cfg(dir, cfg),
        Some(s) => {
            use sqe::typmeta::{oids, TypMeta, COLLATION_C};
            let cols: Vec<sqe::bank::ColMeta> = s
                .split(',')
                .enumerate()
                .map(|(i, spec)| {
                    let (name, ty) = spec.split_once(':').expect("--schema name:type");
                    let typ = match ty {
                        "int8" => TypMeta::INT8,
                        "int4" => TypMeta::INT4,
                        "int2" => TypMeta::INT2,
                        "text" => TypMeta::varlena(oids::TEXT, COLLATION_C),
                        other => panic!("--schema: unhandled type {other}"),
                    };
                    sqe::bank::ColMeta::new(i as u32 + 1, name, typ)
                })
                .collect();
            let bank =
                sqe::bank::Bank::open(dir, cols, &sqe::bank::OpenOpts { bankstats: true, threads: 0 });
            sqe::engine::Engine::new(bank, cfg)
        }
    };
    println!(
        "BANK|dir={dir}|parts={}|rows={}|gen={}|open_ms={:.1}",
        engine.bank.parts.len(),
        engine.bank.rows_total(),
        engine.bank.manifest.header.gen,
        t0.elapsed().as_secs_f64() * 1e3
    );
    if sqe::engine::PHASE_ON.load(std::sync::atomic::Ordering::Relaxed) {
        for l in sqe::coldledger::drain_lines(999, "open") {
            println!("{l}");
        }
        let mut by_tag: std::collections::BTreeMap<String, (u64, u64)> =
            std::collections::BTreeMap::new();
        for p in &engine.bank.parts {
            for f in p.faults() {
                let e = by_tag.entry(format!("{:?}", f.tag)).or_insert((0, 0));
                e.0 += 1;
                e.1 += f.len;
            }
        }
        for (tag, (n, b)) in by_tag {
            println!("OPENIO|tag={tag}|n={n}|bytes={b}");
        }
    }
    println!("POOL|threads={}", engine.pool.threads());
    let mut all_ok = true;
    if let Some(t) = &text {
        match sqe::rig::lower::lower(&engine.ctx(), 999, t, "text") {
            Ok(node) => {
                let node = sqe::rig::maybe_server_bound(node);
                let (answer, best_h, best_e) = if elected_only {
                    // Elected arm alone (diagnosis lanes: honest arms can be
                    // orders slower in extreme-group regimes). GAP twin per
                    // the q39/q10 lane's parked-arrival diagnosis (their
                    // PGRCBENCH_REP_GAP_MS lever, 35fa447c4e6): an inter-rep
                    // sleep parks the pool workers the way a served
                    // statement's arrival always finds them.
                    let gap_ms: u64 = std::env::var("PGRCBENCH_REP_GAP_MS")
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0);
                    let mut best = f64::MAX;
                    let mut a: Option<sqe::answer::AnswerSet> = None;
                    for _ in 0..reps.max(1) {
                        if gap_ms > 0 {
                            std::thread::sleep(std::time::Duration::from_millis(gap_ms));
                        }
                        let t0 = std::time::Instant::now();
                        let x = engine.run(&node);
                        best = best.min(t0.elapsed().as_secs_f64() * 1e3);
                        a = Some(x);
                    }
                    (a.unwrap(), f64::NAN, best)
                } else {
                    sqe::rig::run_arms_grouped(&engine, &node, reps)
                };
                let lines = sqe::render::to_lines(&answer);
                println!(
                    "SQE|q=999|src=text|sqe_ms={best_e:.3}|sqe_honest_ms={best_h:.3}|lines={}",
                    lines.len()
                );
                for l in &lines {
                    println!("ROW|{l}");
                }
            }
            Err(e) => {
                println!("SQLLOWER|q=999|FAIL|{e}");
                std::process::exit(1);
            }
        }
        return;
    }
    for &q in &qs {
        if elected_only {
            sqe::rig::run_query_elected(&engine, q, reps);
        } else {
            all_ok &= sqe::rig::run_query(&engine, q, sql, oracle, reps);
        }
    }
    if sqe::engine::PHASE_ON.load(std::sync::atomic::Ordering::Relaxed) {
        let mut n = 0u64;
        let mut b = 0u64;
        for p in &engine.bank.parts {
            for f in p.faults() {
                n += 1;
                b += f.len;
            }
        }
        let (pc, pb) = pgrc2_read::io::pread_census();
        println!("ENDIO|faults={n}|bytes={b}|preads={pc}|pread_bytes={pb}");
    }
    if unseen {
        for uq in 0..5u32 {
            all_ok &= sqe::rig::run_unseen(&engine, uq, reps);
        }
    }
    if !all_ok {
        std::process::exit(1);
    }
}
