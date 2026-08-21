//! The ingest scaling probe (M3-I slice leg 5: "parallel COPY ≥ 1.5× serial
//! at dop 8 on the bank-shaped corpus — regime-matched, engine-pinned,
//! witnessed"). Env-gated (`PGRC2_SCALING=1`) and RELEASE-effective: the
//! CI cluster leg runs it through `scripts/pgrc2-ingest-par-scaling-e2e.sh` with
//! `--release`; a dev-profile invocation refuses the bar (a debug number is
//! not a number).
//!
//! Regime match: serial and parallel run in the SAME process over the SAME
//! in-memory vfs universe class (hermetic — the probe measures the
//! ingest+seal pipeline's CPU scaling; production file writes ride the page
//! cache behind the identical seal face). Shape class: bank-shaped — int
//! lanes (id, small-domain, hash-spread, monotone) + two text lanes (a
//! small-domain short lane and a varied long lane). No benchmark names.
//!
//! Protocol (CI cluster statistical discipline): interleaved serial/parallel
//! pairs, first pair discarded, medians of the remaining three; the pool
//! witness (worker ordinals seen) is printed with the verdict.

use pgrc2_write::testkit::*;
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::writer::{PartCutPolicy, SealEnv};
use pgrc2_write::par::ParIngestOpts;
use runtime::{Runtime, RuntimeConfig, WorkerPool};
use std::sync::Arc;
use std::time::Instant;

const FXID: u64 = 42;

fn bank_schema() -> Vec<pgrc2_format::class::ColSchema> {
    vec![
        int8_col(1),
        int8_col(2),
        int8_col(3),
        int8_col(4),
        text_col(5),
        text_col(6),
    ]
}

/// One bank-shaped row; text images are prebuilt per call.
fn bank_texts(i: u64) -> (Vec<u8>, Vec<u8>) {
    let short = img_4b_u(format!("cat-{:02}", i % 53).as_bytes());
    let long = img_4b_u(
        format!(
            "ref-{:07}/seg-{:03}/leaf-{:05}?k={}&v={}",
            i % 9_999_983,
            i % 719,
            i % 88_811,
            i % 13,
            (i.wrapping_mul(2654435761)) % 100_000
        )
        .as_bytes(),
    );
    (short, long)
}

fn bank_ints(i: u64) -> [u64; 4] {
    [
        i,
        i % 97,
        i.wrapping_mul(2654435761),
        i / 64,
    ]
}

fn policy() -> PartCutPolicy {
    PartCutPolicy::default() // 1 Mi rows / 256 MiB — the format-default geometry
}

/// Prebuilt corpus: the feed side of a real COPY is the M3-H parser, not
/// this probe's cost — generation happens ONCE outside every timed region,
/// identically for both arms (regime match).
struct Corpus {
    ints: Vec<[u64; 4]>,
    short: Vec<Vec<u8>>,
    long: Vec<Vec<u8>>,
}

fn build_corpus(n: u64) -> Corpus {
    let mut ints = Vec::with_capacity(n as usize);
    let mut short = Vec::with_capacity(n as usize);
    let mut long = Vec::with_capacity(n as usize);
    for i in 0..n {
        ints.push(bank_ints(i));
        let (s, l) = bank_texts(i);
        short.push(s);
        long.push(l);
    }
    Corpus { ints, short, long }
}

fn row_at(c: &Corpus, i: usize) -> [RawDatum<'_>; 6] {
    [
        RawDatum::Word(c.ints[i][0]),
        RawDatum::Word(c.ints[i][1]),
        RawDatum::Word(c.ints[i][2]),
        RawDatum::Word(c.ints[i][3]),
        RawDatum::Bytes(&c.short[i]),
        RawDatum::Bytes(&c.long[i]),
    ]
}

fn serial_run(c: &Corpus) -> std::time::Duration {
    let mut vfs = mem_with_dir();
    let mut kit = Kit::new();
    let mut w = open_writer_policy(bank_schema(), stamp(FXID, 1), policy());
    let t0 = Instant::now();
    for i in 0..c.ints.len() {
        let row = row_at(c, i);
        let sources: [&dyn pgrc2_write::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.append_row(&row, &mut kit.ext, &mut env).expect("append");
    }
    let sources: [&dyn pgrc2_write::elect::CandidateSource; 1] = [&kit.cands];
    let mut env = SealEnv {
        vfs: &mut vfs,
        sources: &sources,
        resolver: &kit.resolver,
        shred: &mut kit.shred,
        shred_opts: &kit.opts,
    };
    w.finish(&mut env).expect("finish");
    let dt = t0.elapsed();
    assert!(!w.sealed_parts().is_empty());
    dt
}

fn par_run(rt: &Arc<Runtime>, c: &Corpus) -> (std::time::Duration, usize, usize) {
    let shared = shared_mem_with_dir();
    let schema = bank_schema();
    let spec = pgrc2_write::seal::PartSpec {
        spc: SPC,
        db: DB,
        relfilenumber: RELFILENUMBER,
        schema_fingerprint: pgrc2_format::ident::schema_fingerprint(&schema),
    };
    let engine = Arc::new(
        pgrc2_write::par::ParEngine::new(
            par_providers(&shared),
            schema,
            spec,
            DIR.to_string(),
            FXID,
            policy(),
            ParIngestOpts::default(),
            0,
        )
        .expect("engine"),
    );
    let t0 = Instant::now();
    let (parts, _reports, ()) = pgrc2_ingest_par::run_parallel(rt, Arc::clone(&engine), |sess| {
        for i in 0..c.ints.len() {
            let row = row_at(c, i);
            sess.append_row(&row)?;
        }
        Ok(())
    })
    .expect("parallel");
    let dt = t0.elapsed();
    assert!(!parts.is_empty());
    let seen = engine.workers_seen();
    (dt, seen.len(), rt.nthreads())
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
    v[v.len() / 2]
}

/// The probe. `PGRC2_SCALING=1` arms it; `PGRC2_SCALING_ROWS` overrides the
/// corpus size (default 2,000,000 ≈ two format-default parts).
#[test]
fn ingest_scaling_probe_dop8() {
    if std::env::var("PGRC2_SCALING").is_err() {
        eprintln!("ingest_scaling_probe_dop8: unarmed (set PGRC2_SCALING=1) — skipping");
        return;
    }
    // A debug number is not a number: the BAR runs release-only. The
    // plumbing may be smoke-tested in dev via PGRC2_SCALING_ALLOW_DEBUG=1
    // (the ratio assert is skipped there — never bank a dev figure).
    let debug_smoke = cfg!(debug_assertions);
    if debug_smoke && std::env::var("PGRC2_SCALING_ALLOW_DEBUG").is_err() {
        panic!("the scaling bar is RELEASE-only — a debug number is not a number");
    }
    let n: u64 = std::env::var("PGRC2_SCALING_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2_000_000);
    let dop = 8usize;
    let rt = Runtime::new(RuntimeConfig::new(dop));
    let pool = WorkerPool::spawn_std(Arc::clone(&rt)).expect("pool");

    let corpus = build_corpus(n);

    // dop-1 sanity lap (not the bar): the parallel path must not be
    // pathological at width 1.
    let rt1 = Runtime::new(RuntimeConfig::new(1));
    let pool1 = WorkerPool::spawn_std(Arc::clone(&rt1)).expect("pool1");
    let (d1, _, _) = par_run(&rt1, &corpus);
    eprintln!("par dop1 sanity ({n} rows): {:.0} ms", d1.as_secs_f64() * 1e3);
    pool1.shutdown();

    let mut serial_ms = Vec::new();
    let mut par_ms = Vec::new();
    let mut witness = (0usize, 0usize);
    for lap in 0..4 {
        let s = serial_run(&corpus);
        let (p, seen, nthreads) = par_run(&rt, &corpus);
        witness = (seen, nthreads);
        eprintln!(
            "lap {lap}: serial {:.0} ms | par(dop {dop}) {:.0} ms | workers_seen {seen}/{nthreads}",
            s.as_secs_f64() * 1e3,
            p.as_secs_f64() * 1e3,
        );
        if lap == 0 {
            continue; // discard the warm-up pair
        }
        serial_ms.push(s.as_secs_f64() * 1e3);
        par_ms.push(p.as_secs_f64() * 1e3);
    }
    pool.shutdown();

    let sm = median(serial_ms);
    let pm = median(par_ms);
    let ratio = sm / pm;
    eprintln!(
        "SCALING VERDICT: rows={n} shape=bank(4xint8+2xtext) serial_median={sm:.0}ms \
         par_dop{dop}_median={pm:.0}ms speedup={ratio:.2}x workers_seen={}/{} bar=1.50x",
        witness.0, witness.1
    );
    assert!(
        witness.0 >= 2,
        "pool engagement witness: parallel work must have run on ≥2 workers"
    );
    if debug_smoke {
        eprintln!("dev smoke: plumbing verified; the 1.5x bar adjudicates in release only");
        return;
    }
    assert!(
        ratio >= 1.5,
        "M3-I scaling bar: parallel COPY must be ≥ 1.5x serial at dop {dop} (got {ratio:.2}x)"
    );
}
