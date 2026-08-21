//! The rtpool binding (M3-I slice legs: rtpool-only assertion + dop-ladder
//! byte stability on the REAL runtime). Real `runtime::Runtime` +
//! `WorkerPool` — the same pool population the server runs; this crate
//! spawns no thread of its own (structurally pinned below).

use pgrc2_write::testkit::*;
use pgrc2_write::wvfs::WriteVfs;
use pgrc2_write::writer::{PartCutPolicy, SealEnv};
use pgrc2_write::par::ParIngestOpts;
use runtime::{Runtime, RuntimeConfig, WorkerPool};
use std::collections::BTreeMap;
use std::sync::Arc;

const FXID: u64 = 42;

fn rt_pool(workers: usize) -> (Arc<Runtime>, WorkerPool) {
    let rt = Runtime::new(RuntimeConfig::new(workers));
    let pool = WorkerPool::spawn_std(Arc::clone(&rt)).expect("pool");
    (rt, pool)
}

/// Run one full parallel ingest of `n` mixed rows at the given pool width;
/// return (tmp bytes per seq, logical totals).
fn run_at_dop(
    workers: usize,
    n: u64,
    policy: PartCutPolicy,
    chunk_rows: u32,
) -> (Vec<Vec<u8>>, BTreeMap<(u32, u32), (u64, u64, u64)>) {
    let (rt, pool) = rt_pool(workers);
    let shared = shared_mem_with_dir();
    let mut w = open_writer_policy(vec![int8_col(1), text_col(2)], stamp(FXID, 1), policy);
    let opts = ParIngestOpts {
        chunk_rows,
        max_chunks_in_flight: 16,
        max_parts_in_flight: 8,
    };
    pgrc2_ingest_par::parallel_ingest(&mut w, &rt, par_providers(&shared), opts, |sess| {
        for i in 0..n {
            with_mixed_row(i, |row| sess.append_row(row))?;
        }
        Ok(())
    })
    .expect("parallel ingest");
    let bytes = w
        .sealed_parts()
        .iter()
        .map(|p| shared.with(|v| v.read_full(&format!("{DIR}/{}", p.tmp_name)).expect("tmp")))
        .collect();
    let totals = logical_totals(w.seal_reports());
    pool.shutdown();
    (bytes, totals)
}

/// Leg 1: the dop ladder {1, 2, 4, 8} is byte-stable AND byte-identical to
/// the serial writer on a row-budget partition (chunk-aligned max_rows) —
/// the §1.9 law on the real pool, claim schedules genuinely concurrent.
#[test]
fn dop_ladder_byte_stable_and_serial_identical() {
    let n: u64 = 20_000;
    let chunk_rows: u32 = 1024;
    let policy = PartCutPolicy {
        max_rows: 4096, // 4 chunks per part — divisible, so serial cuts match
        max_bytes: u64::MAX,
        cut_granule_rows: 1024, // == chunk_rows (M3-I)
    };
    let serial = serial_mixed_tmp_bytes(n, FXID, policy);
    assert_eq!(serial.len(), 5, "20000 rows / 4096-row parts + tail");
    for dop in [1usize, 2, 4, 8] {
        let (bytes, _) = run_at_dop(dop, n, policy, chunk_rows);
        assert_eq!(bytes, serial, "dop {dop} diverged from serial");
    }
}

/// Leg 2: full pipeline e2e at dop 4 — parallel ingest deposits into the
/// UNCHANGED lifecycle: publish (spec §13.3, serial, above the frozen
/// boundary) renames every temp, the manifest lists parts in seq order, and
/// the logical totals equal the serial oracle's.
#[test]
fn full_pipeline_publish_e2e_at_dop4() {
    let (rt, pool) = rt_pool(4);
    let shared = shared_mem_with_dir();
    let policy = PartCutPolicy {
        max_rows: 4096,
        max_bytes: u64::MAX,
        cut_granule_rows: 1024, // == chunk_rows (M3-I)
    };
    let mut w = open_writer_policy(vec![int8_col(1), text_col(2)], stamp(FXID, 1), policy);
    let n: u64 = 10_000;
    pgrc2_ingest_par::parallel_ingest(
        &mut w,
        &rt,
        par_providers(&shared),
        ParIngestOpts {
            chunk_rows: 1024,
            max_chunks_in_flight: 16,
            max_parts_in_flight: 8,
        },
        |sess| {
            for i in 0..n {
                with_mixed_row(i, |row| sess.append_row(row))?;
            }
            Ok(())
        },
    )
    .expect("ingest");
    let par_totals = logical_totals(w.seal_reports());

    let probe = Probe::new(pgrc2_write::publish::TxnVerdict::Committed);
    let mut vfs_handle = pgrc2_write::par::SharedMemVfs::clone(&shared);
    let out = w.publish(&mut vfs_handle, &probe).expect("publish");
    assert_eq!(out.part_nos, (0..3).collect::<Vec<u32>>(), "3 parts, seq order");

    // No temp residue; every part file readable.
    let names = shared.with(|v| v.list_dir(DIR).expect("list"));
    assert!(names.iter().all(|s| !s.starts_with("tmp-")), "temps all renamed");
    for pn in &out.part_nos {
        let name = pgrc2_format::dirlayout::part_file_name(*pn);
        assert!(names.contains(&name), "part file {name} published");
    }

    // Logical identity vs the serial oracle.
    let serial_totals = {
        let mut vfs = mem_with_dir();
        let mut kit = Kit::new();
        let mut sw =
            open_writer_policy(vec![int8_col(1), text_col(2)], stamp(FXID, 1), policy);
        for i in 0..n {
            let sources: [&dyn pgrc2_write::elect::CandidateSource; 1] = [&kit.cands];
            let mut env = SealEnv {
                vfs: &mut vfs,
                sources: &sources,
                resolver: &kit.resolver,
                shred: &mut kit.shred,
                shred_opts: &kit.opts,
            };
            with_mixed_row(i, |row| sw.append_row(row, &mut kit.ext, &mut env).expect("append"));
        }
        let sources: [&dyn pgrc2_write::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        sw.finish(&mut env).expect("finish");
        logical_totals(sw.seal_reports())
    };
    assert_eq!(par_totals, serial_totals, "O-10 logical identity");
    pool.shutdown();
}

/// Leg 3: empty COPY is a clean no-op (no parts, no files, writer reusable).
#[test]
fn empty_copy_is_a_clean_noop() {
    let (rt, pool) = rt_pool(2);
    let shared = shared_mem_with_dir();
    let mut w = open_writer(vec![int8_col(1), text_col(2)], stamp(FXID, 1));
    pgrc2_ingest_par::parallel_ingest(
        &mut w,
        &rt,
        par_providers(&shared),
        ParIngestOpts::default(),
        |_sess| Ok(()),
    )
    .expect("empty session");
    assert!(w.sealed_parts().is_empty());
    assert!(shared.with(|v| v.list_dir(DIR).expect("list")).is_empty());
    pool.shutdown();
}

/// Leg 4: one statement is serial OR parallel, never spliced — the typed
/// guard.
#[test]
fn parallel_over_serial_buffered_rows_refused() {
    let (rt, pool) = rt_pool(2);
    let shared = shared_mem_with_dir();
    let mut kit = Kit::new();
    let mut vfs = mem_with_dir();
    let mut w = open_writer(vec![int8_col(1)], stamp(FXID, 1));
    append_int8_rows(&mut w, &mut vfs, &mut kit, 10, |i| Some(i as i64));
    let err = pgrc2_ingest_par::parallel_ingest(
        &mut w,
        &rt,
        par_providers(&shared),
        ParIngestOpts::default(),
        |_s| Ok(()),
    )
    .expect_err("must refuse");
    assert!(matches!(err, pgrc2_write::WriteError::Contract { .. }));
    pool.shutdown();
}

/// Leg 5 (the rtpool-only assertion, §7.1 law 6): the parallel-ingest module
/// contains NO thread population of its own — no `std::thread`, no ad-hoc
/// spawn; the ONLY execution vehicle is the runtime (rtpool) task set, and
/// the coordinator's sync comes from pgsync (loom-modelable). Source-pinned
/// so a future edit that sneaks a thread in goes RED here.
#[test]
fn rtpool_only_no_new_thread_population() {
    // Both halves of the split: the engine (pgrc2_write/src/par.rs) and the
    // binding (this crate's src/lib.rs).
    let engine = include_str!("../../pgrc2_write/src/par.rs");
    let binding = include_str!("../src/lib.rs");
    for (name, src) in [("par.rs", engine), ("lib.rs", binding)] {
        // v4 delta: the engine's NotYetPublished claim arm yields
        // (`std::thread::yield_now`) — a scheduling hint on the CURRENT
        // thread, never a population. The pin targets creation surfaces.
        let src = src.replace("std::thread::yield_now", "");
        let src = src.as_str();
        assert!(
            !src.contains("std::thread"),
            "{name} must not touch std::thread (rtpool is the only population)"
        );
        assert!(
            !src.contains("thread::spawn"),
            "{name} must not spawn threads"
        );
        assert!(
            !src.contains("thread_local"),
            "{name}: no thread-resident state (TLS census pinned)"
        );
    }
    assert!(
        binding.contains("runtime::"),
        "the runtime (rtpool) binding is the sanctioned execution vehicle"
    );
}

/// Leg 6: the pool-engagement witness — at dop 4 with many chunks the work
/// provably ran on rtpool worker ordinals (within the pool's range). This is
/// the engagement side of the rtpool-only law; the structural side is leg 5.
#[test]
fn pool_engagement_witness() {
    let (rt, pool) = rt_pool(4);
    let shared = shared_mem_with_dir();
    let schema = vec![int8_col(1), text_col(2)];
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
            PartCutPolicy {
                max_rows: 4096,
                max_bytes: u64::MAX,
                cut_granule_rows: 512, // == chunk_rows below (M3-I)
            },
            ParIngestOpts {
                chunk_rows: 512,
                max_chunks_in_flight: 32,
                max_parts_in_flight: 8,
            },
            0,
        )
        .expect("engine"),
    );
    let (_parts, _reports, ()) =
        pgrc2_ingest_par::run_parallel(&rt, Arc::clone(&engine), |sess| {
            for i in 0..20_000u64 {
                with_mixed_row(i, |row| sess.append_row(row))?;
            }
            Ok(())
        })
        .expect("run");
    let seen = engine.workers_seen();
    assert!(!seen.is_empty(), "ingest work ran");
    let nthreads = rt.nthreads();
    assert!(
        seen.iter().all(|&wkr| wkr < nthreads),
        "every executing ordinal is a pool worker (seen {seen:?}, pool {nthreads})"
    );
    pool.shutdown();
}
