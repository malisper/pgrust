//! dop>1 seal determinism (M3-I slice leg 1): same partition ⇒
//! byte-identical parts under PERMUTED CLAIM SCHEDULES — driven through the
//! REAL engine faces with scripted schedules (every chunk-completion order
//! is a legal claim schedule; the runtime scheduler only ever picks one of
//! these). Plus the serial-identity law (§1.9): a parallel-loaded table is
//! byte-identical to a serial-loaded one through the one frozen seal face,
//! under EITHER budget.
//!
//! The byte-budget leg is the M3-I born-RED tooth. It was previously
//! asserted only up to O-10 logical (multiset) identity, on the ruling that
//! byte-budget partitions "legally diverge": serial evaluates its budget
//! after every row and can cut mid-chunk, while the parallel cursor can
//! close a part only at a capture-chunk boundary. That divergence is the
//! LIVE case, not a corner — production runs `PartCutPolicy::default()`'s
//! 256 MiB byte budget, which governs at ClickBench row widths (the 10M
//! ingest-attribution run wrote 55 files where a row-governed cut gives
//! ~10). Since a bank's MANIFEST carries part geometry and elections, a
//! divergent partition breaks bank identity outright.

use super::*;
use crate::par::{ParEngine, ParIngestOpts};
use crate::seal::PartSpec;
use std::sync::Arc;

const FXID: u64 = 42;
const CHUNK_ROWS: u32 = 128;

fn engine_over(
    shared: &SharedMemVfs,
    policy: PartCutPolicy,
) -> Arc<ParEngine> {
    let schema = vec![int8_col(1), text_col(2)];
    let spec = PartSpec {
        spc: SPC,
        db: DB,
        relfilenumber: RELFILENUMBER,
        schema_fingerprint: pgrc2_format::ident::schema_fingerprint(&schema),
    };
    Arc::new(
        ParEngine::new(
            par_providers(shared),
            schema,
            spec,
            DIR.to_string(),
            FXID,
            policy,
            ParIngestOpts {
                chunk_rows: CHUNK_ROWS,
                max_chunks_in_flight: 1024,
                max_parts_in_flight: 1024,
            },
            0,
        )
        .expect("engine"),
    )
}

/// Run one scripted schedule: capture all chunks, run `run_chunk` in the
/// given completion order (each call is one claim), close, collect. Returns
/// (tmp bytes per seq, reports).
fn run_schedule(
    n_rows: u64,
    policy: PartCutPolicy,
    order: &[usize],
) -> (Vec<Vec<u8>>, Vec<crate::seal::SealReport>) {
    let shared = shared_mem_with_dir();
    let engine = engine_over(&shared, policy);
    let chunks = capture_mixed_chunks(n_rows, CHUNK_ROWS);
    assert_eq!(order.len(), chunks.len(), "schedule covers every chunk");
    let total = chunks.len() as u64;
    for (id, ch) in chunks.into_iter().enumerate() {
        engine.publish_chunk(ch, id as u64).expect("publish");
    }
    for &id in order {
        engine.run_chunk(id % 3, id as u64); // worker ordinal is arbitrary
    }
    engine.close_input(total);
    let sealed = engine.collect().expect("collect");
    let mut bytes = Vec::new();
    let mut reports = Vec::new();
    for (i, (p, rep)) in sealed.into_iter().enumerate() {
        assert_eq!(
            p.tmp_name,
            pgrc2_format::dirlayout::temp_file_name(FXID, i as u32),
            "ordered commit: seq order == input order"
        );
        bytes.push(shared.with(|v| v.read_full(&format!("{DIR}/{}", p.tmp_name)).expect("tmp")));
        reports.push(rep);
    }
    (bytes, reports)
}

/// Deterministic LCG shuffles (the lx_spill pattern — no rand dep).
fn lcg_orders(n: usize, count: usize) -> Vec<Vec<usize>> {
    let mut orders = Vec::new();
    orders.push((0..n).collect()); // identity
    orders.push((0..n).rev().collect()); // reverse
    let mut state = 0x1234_5678_9abc_def0u64;
    for _ in 0..count {
        let mut v: Vec<usize> = (0..n).collect();
        for i in (1..n).rev() {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let j = (state >> 33) as usize % (i + 1);
            v.swap(i, j);
        }
        orders.push(v);
    }
    orders
}

/// Leg 1: EVERY permuted claim schedule yields byte-identical parts, and on
/// a row-budget partition those bytes equal the SERIAL writer's (the one
/// seal implementation, witnessed at the file level).
#[test]
fn permuted_claim_schedules_byte_identical_and_serial_equal() {
    // 6 chunks of 128 rows + partial tail; max_rows = 256 = 2 chunks ⇒ the
    // parallel cut coincides with serial's exactly.
    let n_rows = 6 * CHUNK_ROWS as u64 + 37;
    let policy = PartCutPolicy {
        max_rows: 256,
        max_bytes: u64::MAX,
        cut_granule_rows: CHUNK_ROWS,
    };
    let serial = serial_mixed_tmp_bytes(n_rows, FXID, policy);
    assert_eq!(serial.len(), 4, "3 full parts + tail part");
    let n_chunks = 7;
    let mut first: Option<Vec<Vec<u8>>> = None;
    for order in lcg_orders(n_chunks, 12) {
        let (bytes, _) = run_schedule(n_rows, policy, &order);
        assert_eq!(bytes, serial, "schedule {order:?} diverged from serial");
        if let Some(f) = &first {
            assert_eq!(&bytes, f, "schedule {order:?} diverged from schedule 0");
        } else {
            first = Some(bytes);
        }
    }
}

/// Leg 2 (the M3-I born-RED tooth): a BYTE-budget partition must also be
/// byte-identical to serial. The parallel cursor closes parts only at
/// capture-chunk boundaries, so this holds exactly when the serial writer
/// evaluates the same budget at the same granularity — the `PartCutPolicy`
/// granule gate. Without that gate serial cuts mid-chunk, the partitions
/// differ, and the part bytes differ with them.
///
/// The O-10 logical multiset identity is asserted alongside: it is the
/// weaker invariant that survived the divergence, and keeping it pins that
/// the granule gate did not buy byte-identity by losing rows.
#[test]
fn byte_budget_partition_byte_identical_to_serial() {
    let n_rows = 5 * CHUNK_ROWS as u64;
    let policy = PartCutPolicy {
        max_rows: u64::MAX,
        max_bytes: 6_000, // trips on bytes mid-stream
        cut_granule_rows: CHUNK_ROWS,
    };
    // Serial oracle: the sealed temp BYTES, plus logical totals.
    let serial_bytes = serial_mixed_tmp_bytes(n_rows, FXID, policy);
    let mut vfs = mem_with_dir();
    let mut kit = Kit::new();
    let mut w = open_writer_policy(vec![int8_col(1), text_col(2)], stamp(FXID, 1), policy);
    append_mixed(&mut w, &mut vfs, &mut kit, n_rows);
    let serial_tot = logical_totals(w.seal_reports());

    let orders = lcg_orders(5, 6);
    let mut first: Option<Vec<Vec<u8>>> = None;
    for order in orders {
        let (bytes, reports) = run_schedule(n_rows, policy, &order);
        assert!(bytes.len() > 1, "the byte budget must actually trip");
        assert_eq!(
            logical_totals(&reports),
            serial_tot,
            "O-10 logical identity vs serial (schedule {order:?})"
        );
        assert_eq!(
            bytes, serial_bytes,
            "byte-budget partition must be BYTE-identical to serial (schedule {order:?})"
        );
        if let Some(f) = &first {
            assert_eq!(&bytes, f, "parallel runs must agree byte-for-byte");
        } else {
            first = Some(bytes);
        }
    }
}

/// Tooth 2: the invariant leg 2 rests on is GUARDED. An engine whose
/// capture-chunk grain is not the policy's cut granule must refuse typed —
/// a chunk coarser than the granule silently reproduces the byte-budget
/// divergence (the cursor could only close on the coarser boundary while
/// serial closed on the finer one), so it must not be constructible.
#[test]
fn engine_refuses_chunk_grain_that_is_not_the_cut_granule() {
    let shared = shared_mem_with_dir();
    let schema = vec![int8_col(1), text_col(2)];
    let spec = PartSpec {
        spc: SPC,
        db: DB,
        relfilenumber: RELFILENUMBER,
        schema_fingerprint: pgrc2_format::ident::schema_fingerprint(&schema),
    };
    // `expect_err` is unavailable: the Ok type (`ParEngine`) is not Debug.
    let made = ParEngine::new(
        par_providers(&shared),
        schema,
        spec,
        DIR.to_string(),
        FXID,
        PartCutPolicy {
            max_rows: 1024,
            max_bytes: u64::MAX,
            cut_granule_rows: 64, // finer than CHUNK_ROWS below
        },
        ParIngestOpts {
            chunk_rows: CHUNK_ROWS,
            max_chunks_in_flight: 8,
            max_parts_in_flight: 8,
        },
        0,
    );
    let err = match made {
        Ok(_) => panic!("a chunk grain that is not the cut granule must refuse"),
        Err(e) => e,
    };
    match err {
        crate::WriteError::Contract { detail } => {
            assert!(
                detail.contains("cut_granule_rows"),
                "unexpected refusal: {detail}"
            );
        }
        other => panic!("wrong error shape: {other:?}"),
    }
}

/// The shared predicate itself: the budget is consulted ONLY at a granule
/// boundary, and the resulting overshoot is bounded by one granule.
#[test]
fn cut_predicate_gates_on_granule_and_bounds_overshoot() {
    let p = PartCutPolicy {
        max_rows: u64::MAX,
        max_bytes: 1_000,
        cut_granule_rows: 128,
    };
    assert!(!p.should_cut(0, 0), "never cuts an empty buffer");
    assert!(!p.should_cut(100, 5_000), "over budget mid-granule: no cut");
    assert!(!p.should_cut(128, 999), "at a boundary, under budget: no cut");
    assert!(p.should_cut(128, 5_000), "at a boundary, over budget: cut");
    // The budget trips somewhere inside (0, 128]; the cut lands at 128, so
    // a part exceeds max_bytes by at most one granule's worth of rows.
    assert!(!p.should_cut(127, 1_000));
    assert!(p.should_cut(128, 1_000));

    // The row budget behaves the same way: the cut lands at the first
    // granule boundary at or after max_rows.
    let r = PartCutPolicy {
        max_rows: 200,
        max_bytes: u64::MAX,
        cut_granule_rows: 128,
    };
    assert!(!r.should_cut(128, 0), "128 < max_rows 200");
    assert!(r.should_cut(256, 0), "first granule boundary at/after 200");
}

// ---------------------------------------------------------------------------
// Issue #597: the shared predicate must be fed the SAME byte metric.
//
// `PartCutPolicy::should_cut` is one implementation, but a shared predicate
// with two different byte OPERANDS is the same drift the predicate exists to
// prevent. Serial feeds the part-accumulated `ColBuffer::approx_bytes` sum;
// the pre-fix parallel cursor summed chunk-LOCAL `approx_bytes`, which omits
// the inter-chunk heap `pad8` the splice inserts — up to 7 bytes short per
// byref column per interior chunk seam. The tests below pin (a) cross-path
// part identity where that skew straddles `max_bytes` at a granule boundary,
// and (b) metric equality at EVERY granule boundary, cut or no cut.
// ---------------------------------------------------------------------------

/// The #597-tuned corpus: one text column, every payload 3 bytes, so every
/// heap entry is 7 bytes and every chunk heap ends 1 byte short of the pad8
/// the splice inserts at the seam. After k 64-row chunks the serial metric
/// is 1032k - 1 and the chunk-local sum is 1031k — a skew of exactly k - 1
/// bytes, made to straddle `max_bytes` at a granule boundary below.
fn tuned_row<R>(i: u64, f: impl FnOnce(&[RawDatum<'_>]) -> R) -> R {
    let img = img_4b_u(format!("{:03}", i % 1000).as_bytes());
    f(&[RawDatum::Bytes(&img)])
}

const TUNED_CHUNK_ROWS: u32 = 64;

/// Serial oracle over the tuned corpus (the real serial cut site:
/// `TableWriter::append_row` → `cut_part`). Returns sealed temp bytes in
/// seq order.
fn serial_tuned_tmp_bytes(n: u64, policy: PartCutPolicy) -> Vec<Vec<u8>> {
    let mut vfs = mem_with_dir();
    let mut kit = Kit::new();
    let mut w = open_writer_policy(vec![text_col(1)], stamp(FXID, 1), policy);
    for i in 0..n {
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        tuned_row(i, |row| w.append_row(row, &mut kit.ext, &mut env).expect("append"));
    }
    {
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.finish(&mut env).expect("finish");
    }
    (0..w.sealed_parts().len() as u32)
        .map(|seq| {
            vfs.read_full(&format!(
                "{DIR}/{}",
                pgrc2_format::dirlayout::temp_file_name(FXID, seq)
            ))
            .expect("tmp bytes")
        })
        .collect()
}

/// Parallel run over the tuned corpus (the real parallel cut site:
/// `ParEngine::run_chunk` → `advance_cut` → `seal_one`), one scripted claim
/// schedule. Returns sealed temp bytes in seq order.
fn parallel_tuned_tmp_bytes(n: u64, policy: PartCutPolicy, order: &[usize]) -> Vec<Vec<u8>> {
    let shared = shared_mem_with_dir();
    let schema = vec![text_col(1)];
    let spec = PartSpec {
        spc: SPC,
        db: DB,
        relfilenumber: RELFILENUMBER,
        schema_fingerprint: pgrc2_format::ident::schema_fingerprint(&schema),
    };
    let engine = Arc::new(
        ParEngine::new(
            par_providers(&shared),
            schema,
            spec,
            DIR.to_string(),
            FXID,
            policy,
            ParIngestOpts {
                chunk_rows: TUNED_CHUNK_ROWS,
                max_chunks_in_flight: 1024,
                max_parts_in_flight: 1024,
            },
            0,
        )
        .expect("engine"),
    );
    let mut chunks = Vec::new();
    let mut cur = crate::par::RowChunk::new(1, TUNED_CHUNK_ROWS);
    for i in 0..n {
        tuned_row(i, |row| cur.push_row(row).expect("capture"));
        if cur.rows() >= TUNED_CHUNK_ROWS {
            chunks.push(std::mem::replace(&mut cur, crate::par::RowChunk::new(1, TUNED_CHUNK_ROWS)));
        }
    }
    if cur.rows() > 0 {
        chunks.push(cur);
    }
    assert_eq!(order.len(), chunks.len(), "schedule covers every chunk");
    let total = chunks.len() as u64;
    for (id, ch) in chunks.into_iter().enumerate() {
        engine.publish_chunk(ch, id as u64).expect("publish");
    }
    for &id in order {
        engine.run_chunk(id % 3, id as u64);
    }
    engine.close_input(total);
    let sealed = engine.collect().expect("collect");
    sealed
        .into_iter()
        .enumerate()
        .map(|(i, (p, _))| {
            assert_eq!(
                p.tmp_name,
                pgrc2_format::dirlayout::temp_file_name(FXID, i as u32),
                "ordered commit"
            );
            shared.with(|v| v.read_full(&format!("{DIR}/{}", p.tmp_name)).expect("tmp"))
        })
        .collect()
}

/// The #597 cross-path tooth (born RED): serial and parallel ingest of the
/// SAME input under the SAME policy must cut the SAME parts — including when
/// the byte budget trips at a granule boundary inside the window where the
/// two pre-fix byte metrics disagreed.
///
/// Geometry: serial metric after k chunks = 1032k - 1, pre-fix parallel
/// chunk-sum = 1031k. `max_bytes` = 10,315 sits inside (10,310, 10,319], so
/// the serial writer's metric crosses the budget at granule boundary k = 10
/// (640 rows) while the chunk-local sum does not cross until k = 11 (704
/// rows): pre-fix the two paths cut different partitions ([640, 384] vs
/// [704, 320] over 1,024 rows) and every part file diverges with them.
#[test]
fn byte_budget_cut_boundary_identical_where_prefix_metrics_disagreed() {
    let n_rows = 16 * TUNED_CHUNK_ROWS as u64; // 1,024 rows = 16 chunks
    let policy = PartCutPolicy {
        max_rows: u64::MAX,
        max_bytes: 10_315,
        cut_granule_rows: TUNED_CHUNK_ROWS,
    };
    let serial = serial_tuned_tmp_bytes(n_rows, policy);
    assert_eq!(serial.len(), 2, "the byte budget must trip exactly once");
    for order in lcg_orders(16, 4) {
        let parallel = parallel_tuned_tmp_bytes(n_rows, policy, &order);
        assert_eq!(
            parallel.len(),
            serial.len(),
            "part-boundary divergence: serial and parallel cut different partitions \
             (schedule {order:?})"
        );
        assert_eq!(
            parallel, serial,
            "byte-divergent parts from the same input (schedule {order:?})"
        );
    }
}

/// Append tuned row `i` into a bare [`ColBuffer`](crate::ingest::ColBuffer)
/// through the same normalize-then-append motion both ingest paths run.
fn tuned_append(
    col: &mut crate::ingest::ColBuffer,
    i: u64,
    ext: &mut dyn crate::ingest::ExternalDetoast,
    scratch: &mut Vec<u8>,
) {
    tuned_row(i, |row| {
        let RawDatum::Bytes(img) = &row[0] else {
            panic!("tuned corpus is varlena");
        };
        let (payload, _) =
            crate::ingest::normalize_varlena(img, ext, scratch).expect("normalize");
        col.append_varlena_payload(payload).expect("append");
    });
}

/// The #597 metric tooth (b): the byte operand the parallel cursor feeds
/// `should_cut` equals the serial writer's `buffered_bytes` at EVERY
/// granule boundary — cut or no cut — not just at the one boundary test
/// (a) tunes `max_bytes` to straddle.
///
/// Leg 1 pins the physical identity the cursor must reproduce, at the
/// `ColBuffer` level: the part-accumulated metric is the SPLICED buffer's
/// `approx_bytes`, and on this corpus it exceeds the chunk-local sum by
/// exactly the accumulated seam pads (b - 1 bytes after b chunks) — which
/// also proves the corpus exercises the skew at every interior seam.
///
/// Leg 2 pins the cursor itself through the REAL ingest faces. The
/// operand's only observable is the cut decision, so every boundary is
/// probed where its decision flips: for each boundary b with serial metric
/// S(b), `max_bytes` = S(b) makes serial first-cut exactly at b, and
/// `max_bytes` = S(b) + 1 exactly at b + 1 (S is strictly increasing). If
/// the parallel operand P(j) diverged from S(j) at ANY boundary j, the
/// first divergent boundary flips a probe: P(j) < S(j) leaves parallel
/// uncut at j under S(j); P(j) > S(j) cuts parallel at j under S(j) + 1
/// while serial holds to j + 1. Either way the partitions — and the part
/// files asserted below — differ.
#[test]
fn cut_metric_equals_serial_at_every_granule_boundary() {
    const N_CHUNKS: usize = 16;
    let n_rows = N_CHUNKS as u64 * TUNED_CHUNK_ROWS as u64;
    let mut kit = Kit::new();
    let mut scratch = Vec::new();

    // S(b): the serial metric at each granule boundary, from the same
    // appends serial ingest runs (one column, so `buffered_bytes` IS this
    // buffer's `approx_bytes`).
    let mut serial_col = crate::ingest::ColBuffer::new(text_col(1));
    let mut s_at = Vec::with_capacity(N_CHUNKS);
    for i in 0..n_rows {
        tuned_append(&mut serial_col, i, &mut kit.ext, &mut scratch);
        if (i + 1) % TUNED_CHUNK_ROWS as u64 == 0 {
            s_at.push(serial_col.approx_bytes());
        }
    }
    assert!(
        s_at.windows(2).all(|w| w[1] > w[0]),
        "leg 2's probe scheme needs a strictly increasing serial metric"
    );

    // Leg 1: splice-accumulated == serial at every boundary; the
    // chunk-local sum lags by exactly the seam pads (one byte per interior
    // seam on this corpus).
    let mut spliced = crate::ingest::ColBuffer::new(text_col(1));
    let mut chunk_sum = 0u64;
    for b in 0..N_CHUNKS {
        let mut chunk = crate::ingest::ColBuffer::new(text_col(1));
        for i in 0..TUNED_CHUNK_ROWS as u64 {
            let row = b as u64 * TUNED_CHUNK_ROWS as u64 + i;
            tuned_append(&mut chunk, row, &mut kit.ext, &mut scratch);
        }
        chunk_sum += chunk.approx_bytes();
        spliced.splice_chunk(&chunk).expect("splice");
        assert_eq!(
            spliced.approx_bytes(),
            s_at[b],
            "spliced metric diverged from serial at boundary {}",
            b + 1
        );
        assert_eq!(
            s_at[b] - chunk_sum,
            b as u64,
            "seam-pad skew must be one byte per interior seam (boundary {})",
            b + 1
        );
    }

    // Leg 2: probe every boundary through the real cut sites.
    let order: Vec<usize> = (0..N_CHUNKS).collect();
    for (b0, &s_b) in s_at.iter().enumerate() {
        let b = b0 + 1; // 1-based boundary = chunks per first part under S(b)
        for (probe, chunks_per_part) in [(s_b, b), (s_b + 1, b + 1)] {
            let policy = PartCutPolicy {
                max_rows: u64::MAX,
                max_bytes: probe,
                cut_granule_rows: TUNED_CHUNK_ROWS,
            };
            let serial = serial_tuned_tmp_bytes(n_rows, policy);
            // The corpus is uniform (every chunk adds the same metric), so
            // the whole partition is parts of `chunks_per_part` chunks plus
            // a remainder tail — pinning that S(b) really is the operand
            // serial consulted at boundary b.
            assert_eq!(
                serial.len(),
                N_CHUNKS.div_ceil(chunks_per_part),
                "serial did not cut where its own metric says (boundary {b}, probe {probe})"
            );
            let parallel = parallel_tuned_tmp_bytes(n_rows, policy, &order);
            assert_eq!(
                parallel, serial,
                "parallel operand diverged from serial at some boundary <= {b} (probe {probe})"
            );
        }
    }
}

fn append_mixed(
    w: &mut crate::writer::TableWriter,
    vfs: &mut MemVfs,
    kit: &mut Kit,
    n: u64,
) {
    for i in 0..n {
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        with_mixed_row(i, |row| w.append_row(row, &mut kit.ext, &mut env).expect("append"));
    }
    let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
    let mut env = SealEnv {
        vfs,
        sources: &sources,
        resolver: &kit.resolver,
        shred: &mut kit.shred,
        shred_opts: &kit.opts,
    };
    w.finish(&mut env).expect("finish");
}

// ---------------------------------------------------------------------------
// The claim-plane legs (M3-L2; the ruled COPY-on-morsels amendment)
// ---------------------------------------------------------------------------

/// DOP-N workers pulling chunk-morsels through the SHARED claim cursor —
/// with an incrementally growing publish watermark and one deliberately
/// SLOW worker — produce parts byte-identical to serial, and two runs with
/// different skew (different claim→worker assignments) agree byte-for-byte.
/// Claim-order independence is WITNESSED, not assumed: ordering lives
/// wholly in the sink (the input-order cut cursor), never in the claim
/// schedule (PC-3.4).
#[test]
fn claim_plane_dop_n_with_worker_skew_byte_identical_to_serial() {
    let n_rows = 24 * CHUNK_ROWS as u64 + 37;
    let policy = PartCutPolicy {
        max_rows: 256,
        max_bytes: u64::MAX,
        cut_granule_rows: CHUNK_ROWS,
    };
    let serial = serial_mixed_tmp_bytes(n_rows, FXID, policy);
    let mut runs: Vec<Vec<Vec<u8>>> = Vec::new();
    for slow_worker in [0usize, 2usize] {
        // Byte-identity is asserted on EVERY attempt; attempts repeat only
        // until the multi-worker engagement witness lands (a single worker
        // claiming all 25 morsels is a legal but vanishingly rare
        // schedule — the witness must not flake the determinism gate).
        let mut multi_seen = false;
        for _attempt in 0..5 {
            let shared = shared_mem_with_dir();
            let engine = engine_over(&shared, policy);
            let chunks = capture_mixed_chunks(n_rows, CHUNK_ROWS);
            let total = chunks.len() as u64;
            std::thread::scope(|s| {
                for w in 0..4usize {
                    let engine = &engine;
                    s.spawn(move || {
                        while engine.run_one_claim(w) {
                            if w == slow_worker {
                                // Worker-speed skew: the slow claimant
                                // paces between claims, shifting which
                                // morsels each worker wins across runs.
                                std::thread::sleep(std::time::Duration::from_micros(
                                    400,
                                ));
                            }
                        }
                    });
                }
                // Leader: publish incrementally — the growing-watermark
                // shape.
                for (id, ch) in chunks.into_iter().enumerate() {
                    engine.publish_chunk(ch, id as u64).expect("publish");
                }
                engine.close_input(total);
            });
            let sealed = engine.collect().expect("collect");
            let mut bytes = Vec::new();
            for (i, (p, _rep)) in sealed.into_iter().enumerate() {
                assert_eq!(
                    p.tmp_name,
                    pgrc2_format::dirlayout::temp_file_name(FXID, i as u32),
                    "ordered commit under dynamic claiming"
                );
                bytes.push(shared.with(|v| {
                    v.read_full(&format!("{DIR}/{}", p.tmp_name)).expect("tmp")
                }));
            }
            assert_eq!(bytes, serial, "skewed DOP-4 run diverged from serial");
            if let Some(prev) = runs.last() {
                assert_eq!(&bytes, prev, "skewed runs must agree byte-for-byte");
            }
            runs.push(bytes);
            if engine.workers_seen().len() > 1 {
                multi_seen = true;
                break;
            }
        }
        assert!(
            multi_seen,
            "no attempt engaged more than one worker — the probe is vacuous"
        );
    }
}

/// Serial-replay error parity, the single-defect leg (IN-2): a poisoned
/// row surfaces the SAME typed error from the parallel session as from
/// serial ingest — the engine's lowest-input-ordinal error selection.
#[test]
fn error_parity_single_defect_matches_serial() {
    let n_rows = 3 * CHUNK_ROWS as u64;
    let poison_row = CHUNK_ROWS as u64 + 5; // interior of chunk 1
    let policy = PartCutPolicy {
        max_rows: u64::MAX,
        max_bytes: u64::MAX,
        cut_granule_rows: CHUNK_ROWS,
    };
    // The poison: a Bytes datum on the byval int8 column.
    let poisoned = |i: u64| i == poison_row;

    // Serial oracle: the first typed error in input order.
    let serial_err = {
        let mut vfs = mem_with_dir();
        let mut kit = Kit::new();
        let mut w =
            open_writer_policy(vec![int8_col(1), text_col(2)], stamp(FXID, 1), policy);
        let mut err: Option<crate::WriteError> = None;
        for i in 0..n_rows {
            let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
            let mut env = SealEnv {
                vfs: &mut vfs,
                sources: &sources,
                resolver: &kit.resolver,
                shred: &mut kit.shred,
                shred_opts: &kit.opts,
            };
            let res = if poisoned(i) {
                w.append_row(
                    &[RawDatum::Bytes(b"xx"), RawDatum::Null],
                    &mut kit.ext,
                    &mut env,
                )
            } else {
                with_mixed_row(i, |row| w.append_row(row, &mut kit.ext, &mut env))
            };
            if let Err(e) = res {
                err = Some(e);
                break;
            }
        }
        err.expect("the poisoned row must error serially")
    };

    // Parallel: same input through the claim plane at DOP 4.
    let shared = shared_mem_with_dir();
    let engine = engine_over(&shared, policy);
    let mut chunks: Vec<crate::par::RowChunk> = Vec::new();
    let mut cur = crate::par::RowChunk::new(2, CHUNK_ROWS);
    for i in 0..n_rows {
        if poisoned(i) {
            cur.push_row(&[RawDatum::Bytes(b"xx"), RawDatum::Null])
                .expect("capture");
        } else {
            with_mixed_row(i, |row| cur.push_row(row).expect("capture"));
        }
        if cur.rows() >= CHUNK_ROWS {
            chunks.push(std::mem::replace(&mut cur, crate::par::RowChunk::new(2, CHUNK_ROWS)));
        }
    }
    if cur.rows() > 0 {
        chunks.push(cur);
    }
    let total = chunks.len() as u64;
    std::thread::scope(|s| {
        for w in 0..4usize {
            let engine = &engine;
            s.spawn(move || engine.run_worker(w));
        }
        for (id, ch) in chunks.into_iter().enumerate() {
            // The session may cancel mid-feed once the poison lands —
            // a publish error IS the recorded session error, fine here.
            if engine.publish_chunk(ch, id as u64).is_err() {
                break;
            }
        }
        engine.close_input(total);
    });
    let par_err = engine.collect().expect_err("the poisoned session must error");
    assert_eq!(
        par_err, serial_err,
        "parallel session must surface serial's exact typed error"
    );
    engine.cleanup_temps().expect("cleanup");
}

// ---------------------------------------------------------------------------
// FIX-B wake-face stress (the ingest-fix charter's parked-claimer cell)
// ---------------------------------------------------------------------------

/// Engine with explicit in-flight bounds (the wake-face stress rigs drive
/// backpressure deliberately; `engine_over` pins 1024/1024).
fn engine_bounded(
    shared: &SharedMemVfs,
    policy: PartCutPolicy,
    max_chunks_in_flight: usize,
    max_parts_in_flight: usize,
) -> Arc<ParEngine> {
    let schema = vec![int8_col(1), text_col(2)];
    let spec = PartSpec {
        spc: SPC,
        db: DB,
        relfilenumber: RELFILENUMBER,
        schema_fingerprint: pgrc2_format::ident::schema_fingerprint(&schema),
    };
    Arc::new(
        ParEngine::new(
            par_providers(shared),
            schema,
            spec,
            DIR.to_string(),
            FXID,
            policy,
            ParIngestOpts {
                chunk_rows: CHUNK_ROWS,
                max_chunks_in_flight,
                max_parts_in_flight,
            },
            0,
        )
        .expect("engine"),
    )
}

/// One free-running round of the wake-face stress: workers spawn BEFORE
/// anything is published (each one's first probe parks), the leader
/// trickles publishes with yields between them (maximizing snapshot/poke
/// races on the eventcount), closes, and the sealed bytes must equal the
/// scripted oracle. A lost wake is a hang; a moved byte fails the compare.
fn park_stress_round(
    policy: PartCutPolicy,
    bounds: (usize, usize),
    n_rows: u64,
    oracle: &[Vec<u8>],
) {
    let shared = shared_mem_with_dir();
    let engine = engine_bounded(&shared, policy, bounds.0, bounds.1);
    let chunks = capture_mixed_chunks(n_rows, CHUNK_ROWS);
    let total = chunks.len() as u64;
    std::thread::scope(|s| {
        for w in 0..4usize {
            let engine = &engine;
            s.spawn(move || engine.run_worker(w));
        }
        for (id, ch) in chunks.into_iter().enumerate() {
            engine.publish_chunk(ch, id as u64).expect("publish");
            std::thread::yield_now();
        }
        engine.close_input(total);
    });
    let sealed = engine.collect().expect("collect");
    let bytes: Vec<Vec<u8>> = sealed
        .iter()
        .map(|(p, _)| shared.with(|v| v.read_full(&format!("{DIR}/{}", p.tmp_name)).expect("tmp")))
        .collect();
    assert_eq!(bytes, oracle, "parking is schedule, never content");
    assert!(engine.peak_parts_open() >= 1, "the peak witness moved");
}

/// FIX-B wake-face stress (real threads, real faces). The exhaustive loom
/// twin (tests/loom.rs models 3-6) is BLOCKED tree-wide by the pgstat
/// loom-cone defect (static pgsync::Mutex under --cfg loom — see that
/// file's header), so this real-thread gate carries the per-train teeth
/// for the parked-claimer protocol: publish-wake, drain-wake, and the
/// park/poke snapshot races, across repeated free-running rounds.
#[test]
fn parked_workers_always_drain_and_match_oracle() {
    let n_rows = 6 * CHUNK_ROWS as u64 + 37;
    let policy = PartCutPolicy {
        max_rows: 256,
        max_bytes: u64::MAX,
        cut_granule_rows: CHUNK_ROWS,
    };
    let n_chunks = capture_mixed_chunks(n_rows, CHUNK_ROWS).len();
    let order: Vec<usize> = (0..n_chunks).collect();
    let (oracle, _) = run_schedule(n_rows, policy, &order);
    for _round in 0..16 {
        park_stress_round(policy, (1024, 1024), n_rows, &oracle);
    }
}

/// Same protocol under bounds 1/1 — the loom model-6 analog: every publish
/// rides the captured bound and every part close rides the parts bound, so
/// the FIX-B gated notifies (captured-from-full + parts-crossing) sit on
/// the hot path of every round. A wrong gate strands the leader (hang);
/// the bounds move schedule only, never bytes (same oracle).
#[test]
fn tight_bounds_backpressure_drains_with_parked_workers() {
    let n_rows = 6 * CHUNK_ROWS as u64 + 37;
    let policy = PartCutPolicy {
        max_rows: 256,
        max_bytes: u64::MAX,
        cut_granule_rows: CHUNK_ROWS,
    };
    let n_chunks = capture_mixed_chunks(n_rows, CHUNK_ROWS).len();
    let order: Vec<usize> = (0..n_chunks).collect();
    let (oracle, _) = run_schedule(n_rows, policy, &order);
    for _round in 0..8 {
        park_stress_round(policy, (1, 1), n_rows, &oracle);
    }
}
