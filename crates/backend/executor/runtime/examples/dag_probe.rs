//! M5+1 pipeline-DAG overlap probe (m5-planner §3.6 measurement plan).
//!
//! Measures the independent-subtree overlap win at the runtime altitude:
//! the §3.6 win shapes — (a) multi-build-side join (2–3 independent
//! dimension builds + a gated probe), (b) independent subqueries (two
//! chains joining a final), (c) UNION ALL (independent branches + concat) —
//! each run A/B with DAG dispatch ON vs OFF (today's sequential walk) on
//! one warm pool, plus the single-pipeline flatness control (the wide-events
//! shape class, expected ≈0).
//!
//! The predicted win is the SERIALIZED BUILD TAIL: dimension builds are
//! width-limited (granule count ≈ DOP or below — a small build cannot keep
//! 16 workers busy), so the sequential walk idles workers during each
//! build's ramp/tail while DAG dispatch backfills them with the sibling
//! build's morsels. Work is conserved; the win is idle-tail reclamation —
//! honest expectation per §3.6: large on width-limited shapes, ≈0 when
//! every pipeline saturates the pool.
//!
//! Run (fleet, DOP16):
//!   cargo run -p runtime --release --example dag_probe [dop] [iters]
//! Output: one line per (shape, mode) with median wall µs, then a verdict
//! line per shape: DAGPROBE|shape=…|dop=…|seq_us=…|dag_us=…|win_pct=…

use std::sync::Arc;
use std::time::{Duration, Instant};

use runtime::{
    MorselRange, QuerySpec, Runtime, RuntimeConfig, SizingParams, SyntheticMorselSource,
    TaskSetSpec, TaskSetWork, WorkerPool,
};

/// CPU-bound work: busy-spin `ns_per_granule` per granule (the hash-build /
/// probe compute stand-in; no allocation, no I/O).
struct Spin {
    ns_per_granule: u64,
}

impl TaskSetWork for Spin {
    fn run_morsel(&self, _w: usize, range: MorselRange) {
        let n = range.end - range.start;
        let t0 = Instant::now();
        let budget = Duration::from_nanos(self.ns_per_granule * n);
        while t0.elapsed() < budget {
            std::hint::spin_loop();
        }
    }
    fn finalize(&self) {}
}

/// (granules, ns_per_granule, deps) per pipeline.
type Shape = Vec<(u64, u64, Vec<usize>)>;

fn spec(qid: u64, shape: &Shape) -> QuerySpec {
    QuerySpec {
        query_id: qid,
        tasksets: shape
            .iter()
            .map(|(granules, ns, deps)| TaskSetSpec {
                source: Arc::new(SyntheticMorselSource::new(*granules)),
                work: Arc::new(Spin { ns_per_granule: *ns }),
                deps: deps.clone(),
            })
            .collect(),
    }
}

fn run_once(rt: &Arc<Runtime>, qid: u64, shape: &Shape) -> u64 {
    let t0 = Instant::now();
    let (_h, waiter) = rt.submit(spec(qid, shape));
    waiter.wait();
    t0.elapsed().as_micros() as u64
}

fn median(mut v: Vec<u64>) -> u64 {
    v.sort_unstable();
    v[v.len() / 2]
}

fn main() {
    let mut args = std::env::args().skip(1);
    let dop: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(16);
    let iters: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(5);

    // Width-limited dimension build: DOP granules of 2 ms — one wave, all
    // ramp/tail (the m3 dimension-build class). The probe/sink pipelines
    // are wide (saturate the pool) so the barrier semantics stay honest.
    let g = dop as u64;
    let build = (g, 2_000_000u64, Vec::<usize>::new());
    let wide = |granules: u64, deps: Vec<usize>| (granules, 250_000u64, deps);

    let shapes: Vec<(&str, Shape)> = vec![
        (
            "multibuild3",
            vec![
                build.clone(),
                build.clone(),
                build.clone(),
                wide(g * 16, vec![0, 1, 2]), // probe over the three builds
            ],
        ),
        (
            "subq2",
            vec![
                build.clone(),
                (g, 2_000_000, vec![0]), // agg over subquery A
                build.clone(),
                (g, 2_000_000, vec![2]), // agg over subquery B
                wide(g * 8, vec![1, 3]), // final join/agg
            ],
        ),
        (
            "unionall4",
            vec![
                build.clone(),
                build.clone(),
                build.clone(),
                build.clone(),
                (g, 250_000, vec![0, 1, 2, 3]), // concat sink
            ],
        ),
        (
            // Honesty control: builds WIDE enough to saturate the pool —
            // work is conserved, so overlap should buy ≈0 here (§3.6's
            // "expected ≈0 when every pipeline saturates the pool").
            "multibuild3_wide",
            vec![
                (g * 16, 250_000, vec![]),
                (g * 16, 250_000, vec![]),
                (g * 16, 250_000, vec![]),
                wide(g * 16, vec![0, 1, 2]),
            ],
        ),
        (
            // Flatness control: ONE wide pipeline (the wide-events shape class).
            "single_pipeline",
            vec![wide(g * 64, vec![])],
        ),
    ];

    let mut cfg = RuntimeConfig::new(dop);
    cfg.sizing = SizingParams::default();
    let rt = Runtime::new(cfg);
    let pool = WorkerPool::spawn_std(Arc::clone(&rt)).expect("pool");

    let mut qid = 1u64;
    println!("DAGPROBE|start|dop={dop}|iters={iters}");
    for (name, shape) in &shapes {
        // Warm one run per mode, then measure.
        let mut med = [0u64; 2];
        for (mi, dag) in [(0usize, false), (1usize, true)] {
            rt.set_dag(dag);
            qid += 1;
            let _warm = run_once(&rt, qid, shape);
            let mut walls = Vec::new();
            for _ in 0..iters {
                qid += 1;
                walls.push(run_once(&rt, qid, shape));
            }
            println!(
                "DAGPROBE|shape={name}|mode={}|walls_us={walls:?}",
                if dag { "dag" } else { "seq" }
            );
            med[mi] = median(walls);
        }
        let win = 100.0 * (med[0] as f64 - med[1] as f64) / med[0] as f64;
        println!(
            "DAGPROBE|verdict|shape={name}|dop={dop}|seq_us={}|dag_us={}|win_pct={win:.1}",
            med[0], med[1]
        );
    }
    let s = rt.stats();
    println!(
        "DAGPROBE|stats|fanout={}|deferred={}|depth_picks={}",
        s.dag_fanout_publishes, s.dag_ready_deferred, s.dag_depth_picks
    );
    rt.request_stop();
    pool.shutdown();
}
