//! Loom models for the parallel-ingest coordinator (§5 M3-I: "loom or
//! real-thread stress for new shared state per the claim-channel laws").
//! Production faces, production fences — never mirror models (the lx_pipe
//! law). The chunk CLAIM channel itself is the runtime scheduler's
//! (loom-verified in `runtime/tests/loom.rs`); these models cover the state
//! THIS crate adds: the coordinator mutex (cut cursor, part closing,
//! first-error-wins cancellation) driven through the real [`ParEngine`]
//! faces over a real shared-MemVfs universe and the real frozen seal.
//!
//! Build/run:
//!   RUSTFLAGS="--cfg loom" cargo test -p pgrc2_write --test loom --release
//!
//! **BLOCKED TREE-WIDE (witnessed 2026-08-14, CI cluster job
//! pgrust-fast-tests-43faca58e3-1786681622-4d77): the invocation above no
//! longer compiles** — `--cfg loom` applies to the whole dep graph, and
//! pgrc2_write's cone now reaches `pgstat` (via adt_jsonb_shred → …
//! adt_date → xact → … → bufmgr → pgstat), whose STATIC `pgsync::Mutex`es
//! are illegal in the loom world ("no statics hold loom types"; 14 E0015/
//! E0599 errors). These models stay in-tree as the ready gate for when
//! that cone is fixed (pgstat → `pgsync::global` shim is the sanctioned
//! shape — another lane's cone); the per-train teeth for the same faces
//! meanwhile ride the REAL-THREAD stress gate in
//! `src/tests/par_determinism.rs` (`parked_workers_*`), which runs in the
//! standard units battery.
//!
//! Models:
//!   1. `racing_workers_close_each_part_exactly_once` — two workers race
//!      chunk completions in every interleaving: the cut cursor closes the
//!      part exactly once, the sealed part bytes are IDENTICAL across all
//!      interleavings AND equal to the single-threaded oracle (the
//!      byte-identical-parts law under the founding race).
//!   2. `first_error_wins_and_cleanup_is_total` — a poisoned chunk races a
//!      good one: exactly one typed error surfaces, the session cancels, and
//!      cleanup leaves zero temp residue in every interleaving.
//!   3. `parked_claimer_always_wakes_on_publish_and_drain` — FIX-B (the
//!      ingest-contention charter): a parked `run_worker` races the
//!      leader's publish/close pokes in every interleaving — a lost wake
//!      is a loom-detected deadlock (publish-wake + drain-wake legs).
//!   4. `parked_claimer_always_wakes_on_cancel` — the cancel-wake leg:
//!      `fail()` (no close_input ever) must always un-park the worker.
//!   5. `seeded_lost_wake_is_caught_by_the_model` — the gate's
//!      calibration twin (born-RED evidence): a deliberately broken
//!      eventcount (sequence checked OUTSIDE the wake mutex — the classic
//!      TOCTOU lost-wake) MUST be reported by loom. Not a production
//!      face; it proves the checker catches the defect class models 3-4
//!      guard against.
//!   6. `gated_backpressure_never_strands_the_leader` — FIX-B notify
//!      hygiene: with both bounds at 1, the captured-from-full and
//!      parts-crossing transition notifies must wake the blocked leader
//!      in every interleaving (a wrong gate is a loom-detected deadlock).

#![cfg(loom)]

use std::sync::Arc;

use loom::thread;

use pgrc2_write::elect::ReferenceCandidates;
use pgrc2_write::ingest::RawDatum;
use pgrc2_write::par::{
    NoDetoastProvider, NoShredProvider, ParEngine, ParIngestOpts, ParProviders, RowChunk,
    SharedMemVfs, SharedMemVfsProvider,
};
use pgrc2_write::seal::{PartSpec, ReferenceResolver};
use pgrc2_write::writer::PartCutPolicy;
use pgrc2_write::wvfs::{MemVfs, WriteVfs};
use pgrc2_format::class::{ColSchema, CollationClass, StorageClass, TypeSemantics};
use pgrc2_format::relopt::ShredOptions;

const DIR: &str = "/tbl/pgrc2_777";
const FXID: u64 = 42;

fn int8_col(attno: u32) -> ColSchema {
    ColSchema {
        attno,
        class: StorageClass::ByvalWord {
            width: 8,
            signed: true,
        },
        typlen: 8,
        typbyval: true,
        typalign: b'd',
        collation_class: CollationClass::C,
        semantics: TypeSemantics::SignedInt,
    }
}

fn shared_dir() -> SharedMemVfs {
    let mut v = MemVfs::new();
    v.mkdir_path(DIR).expect("mkdir");
    SharedMemVfs::new(v)
}

fn engine_with(
    shared: &SharedMemVfs,
    max_rows: u64,
    max_chunks_in_flight: usize,
    max_parts_in_flight: usize,
) -> Arc<ParEngine> {
    let schema = vec![int8_col(1)];
    let spec = PartSpec {
        spc: 1663,
        db: 5,
        relfilenumber: 777,
        schema_fingerprint: pgrc2_format::ident::schema_fingerprint(&schema),
    };
    Arc::new(
        ParEngine::new(
            ParProviders {
                vfs: Arc::new(SharedMemVfsProvider(shared.clone())),
                detoast: Arc::new(NoDetoastProvider),
                shred: Arc::new(NoShredProvider),
                sources: vec![Arc::new(ReferenceCandidates)],
                resolver: Arc::new(ReferenceResolver),
                shred_opts: ShredOptions::default(),
                structural: pgrc2_write::structural::StructuralPolicy::default(),
            },
            schema,
            spec,
            DIR.to_string(),
            FXID,
            PartCutPolicy {
                max_rows,
                max_bytes: u64::MAX,
                // Must equal ParIngestOpts::chunk_rows below (M3-I).
                cut_granule_rows: 64,
            },
            ParIngestOpts {
                chunk_rows: 64,
                max_chunks_in_flight,
                max_parts_in_flight,
            },
            0,
        )
        .expect("engine"),
    )
}

fn engine_over(shared: &SharedMemVfs) -> Arc<ParEngine> {
    engine_with(shared, 128, 64, 64)
}

/// A 64-row int8 chunk whose values start at `base` (value variety breaks
/// constancy so the seal exercises the verbatim path).
fn int_chunk(base: i64) -> RowChunk {
    let mut c = RowChunk::new(1, 64);
    for i in 0..64i64 {
        c.push_row(&[RawDatum::Word((base + i) as u64)]).expect("push");
    }
    c
}

fn tmp_residue(shared: &SharedMemVfs) -> Vec<String> {
    shared.with(|v| {
        v.list_dir(DIR)
            .expect("list")
            .into_iter()
            .filter(|n| n.starts_with("tmp-"))
            .collect()
    })
}

/// Model 1: the cut cursor under a racing pair of chunk completions.
#[test]
fn racing_workers_close_each_part_exactly_once() {
    loom::model(move || {
        // Single-threaded oracle bytes, fresh per iteration (loom sync
        // types only live inside the model).
        let oracle: Vec<u8> = {
            let shared = shared_dir();
            let engine = engine_over(&shared);
            engine.publish_chunk(int_chunk(0), 0).expect("pub");
            engine.publish_chunk(int_chunk(64), 1).expect("pub");
            engine.run_chunk(0, 0);
            engine.run_chunk(0, 1);
            engine.close_input(2);
            let sealed = engine.collect().expect("collect");
            assert_eq!(sealed.len(), 1);
            shared.with(|v| v.read_full(&format!("{DIR}/{}", sealed[0].0.tmp_name)).expect("tmp"))
        };
        let shared = shared_dir();
        let engine = engine_over(&shared);
        engine.publish_chunk(int_chunk(0), 0).expect("pub");
        engine.publish_chunk(int_chunk(64), 1).expect("pub");
        let e1 = Arc::clone(&engine);
        let t1 = thread::spawn(move || e1.run_chunk(0, 0));
        let e2 = Arc::clone(&engine);
        let t2 = thread::spawn(move || e2.run_chunk(1, 1));
        t1.join().expect("t1");
        t2.join().expect("t2");
        engine.close_input(2);
        let sealed = engine.collect().expect("collect");
        assert_eq!(sealed.len(), 1, "the part closes exactly once");
        let bytes = shared
            .with(|v| v.read_full(&format!("{DIR}/{}", sealed[0].0.tmp_name)).expect("tmp"));
        assert_eq!(bytes, oracle, "byte-identical under every interleaving");
    });
}

/// Model 2: first-error-wins cancellation + total cleanup under the race.
#[test]
fn first_error_wins_and_cleanup_is_total() {
    loom::model(|| {
        let shared = shared_dir();
        let engine = engine_over(&shared);
        // Chunk 0 good; chunk 1 poisoned (a Word datum on a byref-free
        // schema is fine — poison instead with a wrong-width row).
        engine.publish_chunk(int_chunk(0), 0).expect("pub");
        let mut bad = RowChunk::new(1, 64);
        for i in 0..64i64 {
            // Bytes cell on a byval class: normalize refuses typed.
            bad.push_row(&[RawDatum::Bytes(&i.to_le_bytes())]).expect("push");
        }
        engine.publish_chunk(bad, 1).expect("pub");
        let e1 = Arc::clone(&engine);
        let t1 = thread::spawn(move || e1.run_chunk(0, 0));
        let e2 = Arc::clone(&engine);
        let t2 = thread::spawn(move || e2.run_chunk(1, 1));
        t1.join().expect("t1");
        t2.join().expect("t2");
        engine.close_input(2);
        let err = engine.collect().expect_err("the poison must surface");
        assert!(
            matches!(err, pgrc2_write::WriteError::Contract { .. }),
            "typed: {err:?}"
        );
        engine.cleanup_temps().expect("cleanup");
        assert!(tmp_residue(&shared).is_empty(), "no residue in any interleaving");
    });
}

/// Model 3 (FIX-B, the parked-claimer protocol): a `run_worker` that may
/// park races the leader's publish/close pokes in EVERY interleaving —
/// including the founding race, the poke landing between the worker's
/// wake-sequence snapshot and its park. A lost wake is a worker parked
/// forever = a loom-detected deadlock. Covers the publish-wake AND
/// drain-wake legs; the sealed bytes equal the single-threaded oracle
/// (parking is schedule, never content).
#[test]
fn parked_claimer_always_wakes_on_publish_and_drain() {
    loom::model(move || {
        let oracle: Vec<u8> = {
            let shared = shared_dir();
            let engine = engine_over(&shared);
            engine.publish_chunk(int_chunk(0), 0).expect("pub");
            engine.publish_chunk(int_chunk(64), 1).expect("pub");
            engine.run_chunk(0, 0);
            engine.run_chunk(0, 1);
            engine.close_input(2);
            let sealed = engine.collect().expect("collect");
            assert_eq!(sealed.len(), 1);
            shared.with(|v| v.read_full(&format!("{DIR}/{}", sealed[0].0.tmp_name)).expect("tmp"))
        };
        let shared = shared_dir();
        let engine = engine_over(&shared);
        let e1 = Arc::clone(&engine);
        // The worker starts BEFORE anything is published: its first probe
        // may observe an empty space and park; every subsequent leader
        // step (two publishes, then close) must un-park it.
        let t = thread::spawn(move || e1.run_worker(0));
        engine.publish_chunk(int_chunk(0), 0).expect("pub");
        engine.publish_chunk(int_chunk(64), 1).expect("pub");
        engine.close_input(2);
        t.join().expect("worker returns in every interleaving");
        let sealed = engine.collect().expect("collect");
        assert_eq!(sealed.len(), 1, "one part");
        let bytes = shared
            .with(|v| v.read_full(&format!("{DIR}/{}", sealed[0].0.tmp_name)).expect("tmp"));
        assert_eq!(bytes, oracle, "parking is schedule, never content");
    });
}

/// Model 4 (FIX-B, cancel-wake leg): the session fails WITHOUT ever
/// closing input — a parked worker must still un-park (record_error's
/// poke) and drain, in every interleaving.
#[test]
fn parked_claimer_always_wakes_on_cancel() {
    loom::model(|| {
        let shared = shared_dir();
        let engine = engine_over(&shared);
        let e1 = Arc::clone(&engine);
        let t = thread::spawn(move || e1.run_worker(0));
        engine.publish_chunk(int_chunk(0), 0).expect("pub");
        engine.fail(pgrc2_write::WriteError::Contract { detail: "seeded session failure" });
        t.join().expect("worker returns in every interleaving");
        let err = engine.collect().expect_err("the failure must surface");
        assert!(matches!(err, pgrc2_write::WriteError::Contract { .. }), "typed: {err:?}");
        engine.cleanup_temps().expect("cleanup");
        assert!(tmp_residue(&shared).is_empty(), "no residue in any interleaving");
    });
}

/// Model 5 — the gate's calibration twin (born-RED evidence for the
/// parked-claimer cells): a deliberately BROKEN eventcount whose parker
/// checks the wake sequence OUTSIDE the wake mutex (the classic TOCTOU
/// lost-wake: a poke landing between the check and the wait registration
/// is lost). Loom MUST report it (the lost interleaving parks forever =
/// deadlock). This is not a production face — it exists to prove the
/// checker catches the exact defect class models 3-4 stand guard over;
/// the REAL protocol re-checks the sequence under the mutex
/// (`ParEngine::park_wake`) and bumps before notifying
/// (`ParEngine::poke_wake`).
#[test]
fn seeded_lost_wake_is_caught_by_the_model() {
    let found = std::panic::catch_unwind(|| {
        loom::model(|| {
            use loom::sync::{Condvar, Mutex};
            use std::sync::atomic::{AtomicU64, Ordering};
            let gate = Arc::new((AtomicU64::new(0), Mutex::new(()), Condvar::new()));
            let g2 = Arc::clone(&gate);
            let parker = thread::spawn(move || {
                let (seq, mu, cv) = &*g2;
                // SEEDED DEFECT: check outside the mutex, then register.
                if seq.load(Ordering::Acquire) == 0 {
                    let guard = mu.lock().unwrap();
                    let _g = cv.wait(guard).unwrap();
                }
            });
            let (seq, mu, cv) = &*gate;
            seq.fetch_add(1, Ordering::AcqRel);
            let guard = mu.lock().unwrap();
            cv.notify_all();
            drop(guard);
            parker.join().unwrap();
        });
    });
    assert!(
        found.is_err(),
        "loom must report the seeded lost-wake as a failed model (deadlock)"
    );
}

/// Model 6 (FIX-B notify hygiene): with BOTH leader bounds at 1 and a
/// one-chunk part cut, the leader's second publish blocks on the
/// captured bound and then (possibly) the parts bound — the
/// captured-from-full transition notify and the parts-crossing notify
/// must wake it in every interleaving (a wrong gate = the leader blocked
/// forever = a loom-detected deadlock), while the worker parks/wakes on
/// the same schedule.
#[test]
fn gated_backpressure_never_strands_the_leader() {
    loom::model(|| {
        let shared = shared_dir();
        // max_rows = 64: every 64-row chunk closes a part; bounds 1/1.
        let engine = engine_with(&shared, 64, 1, 1);
        let e1 = Arc::clone(&engine);
        let t = thread::spawn(move || e1.run_worker(0));
        engine.publish_chunk(int_chunk(0), 0).expect("pub");
        // Blocks until the worker drains chunk 0 AND seals part 0.
        engine.publish_chunk(int_chunk(64), 1).expect("pub");
        engine.close_input(2);
        t.join().expect("worker returns in every interleaving");
        let sealed = engine.collect().expect("collect");
        assert_eq!(sealed.len(), 2, "two single-chunk parts");
    });
}
