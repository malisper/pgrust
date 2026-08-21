//! # pgrc2_ingest_par — the rtpool binding for pgrcolumnar2 parallel ingest
//! (chunk M3-I)
//!
//! The coordination + assembly ENGINE lives in [`pgrc2_write::par`] (the
//! single-owner ingest handoff); this crate binds it to the rtpool: a
//! [`runtime::StreamSource`]-fed task set (the runtime's stream-fed morsel
//! source — its doc names parallel COPY's segmentator feed), the leader's
//! feeding session, and the ordered-commit driver. Split-crate shape is
//! FORCED by the dependency graph: since M3-H (#473), `pgrc2_am` (table-AM
//! cone, below `runtime`) depends on `pgrc2_write`, so the writer crate can
//! never depend on `runtime`; the binding sits above both, and the COPY
//! surface reaches it from executor altitude (declared M3-H follow-on).
//!
//! No new thread population (§7.1 law 6): everything runs on rtpool workers;
//! the leader only feeds and parks. The rtpool-only source pin lives in
//! `tests/runtime_binding.rs`.

use std::sync::Arc;

use pgrc2_write::par::{ParEngine, ParIngestOpts, ParProviders, RowChunk};
use pgrc2_write::seal::{SealReport, SealedPart};
use pgrc2_write::writer::TableWriter;
use pgrc2_write::{WriteError, WriteResult};
use pgrc2_write::ingest::RawDatum;
use runtime::{
    CompletionWaiter, MorselRange, QuerySpec, RgOutcome, Runtime, StreamSource, TaskSetSpec,
    TaskSetWork,
};

struct IngestWork {
    engine: Arc<ParEngine>,
}

impl TaskSetWork for IngestWork {
    fn run_morsel(&self, worker: usize, range: MorselRange) {
        for id in range {
            self.engine.run_chunk(worker, id);
        }
    }

    fn finalize(&self) {}
}

/// The leader's feeding face for one parallel COPY statement.
pub struct ParSession<'a> {
    engine: &'a Arc<ParEngine>,
    stream: &'a Arc<StreamSource>,
    rt: &'a Arc<Runtime>,
    cur: RowChunk,
    published: u64,
    ncols: usize,
}

impl ParSession<'_> {
    /// The engine's chunk grain (== the policy's cut granule, #560): the
    /// aligned-fast-path caller verifies its prebuilt chunks against THIS
    /// before handing any through `append_chunk`.
    pub fn chunk_rows(&self) -> u32 {
        self.engine.chunk_rows()
    }

    /// Append one row (COPY-shaped, same currency as the serial face). Rows
    /// are captured raw; chunk grain full ⇒ publish to the claim stream
    /// (may block on backpressure).
    pub fn append_row(&mut self, row: &[RawDatum<'_>]) -> WriteResult<()> {
        self.cur.push_row(row)?;
        if self.cur.rows() >= self.engine.chunk_rows() {
            self.flush()?;
        }
        Ok(())
    }

    /// Append one PREBUILT full-grain chunk (the parquet-parallel aligned
    /// fast path: decode workers build the chunk OFF the leader; the
    /// leader hands it through in input order — measured leader-bound at
    /// ~100% of one core pumping per-row without this face). Legal ONLY on
    /// a chunk boundary (no partial rows buffered) with exactly
    /// `chunk_rows` rows of the session's width — anything else is a
    /// Contract error: the caller's alignment invariant is structural
    /// (fragments always end on grain boundaries), never best-effort.
    /// The published-id order is identical to the row path's, so the #560
    /// partition law (chunk grain == cut granule) is preserved by
    /// construction.
    pub fn append_chunk(&mut self, chunk: RowChunk) -> WriteResult<()> {
        if self.cur.rows() != 0 {
            return Err(WriteError::Contract {
                detail: "append_chunk off a chunk boundary (partial rows buffered)",
            });
        }
        if chunk.rows() != self.engine.chunk_rows() || chunk.ncols() != self.ncols {
            return Err(WriteError::Contract {
                detail: "append_chunk shape mismatch (grain or width)",
            });
        }
        let id = self.published;
        self.engine.publish_chunk(chunk, id)?;
        self.published += 1;
        self.stream.publish(self.published);
        self.rt.notify_source_progress();
        Ok(())
    }

    fn flush(&mut self) -> WriteResult<()> {
        if self.cur.rows() == 0 {
            return Ok(());
        }
        let full = std::mem::replace(
            &mut self.cur,
            RowChunk::new(self.ncols, self.engine.chunk_rows()),
        );
        let id = self.published;
        self.engine.publish_chunk(full, id)?;
        self.published += 1;
        // Publish-then-wake: the watermark advance makes the chunk claimable
        // (StreamSource is the runtime's Dekker-checked stream face).
        self.stream.publish(self.published);
        self.rt.notify_source_progress();
        Ok(())
    }
}

/// Run one parallel ingest session on the rtpool over a prebuilt engine:
/// submit the stream-fed task set, feed rows through `feed`, drain, and
/// return the ordered seal results. On error the drain still completes and
/// every assigned temp is unlinked (exactly-old-or-new: a failed session
/// leaves nothing readable).
pub fn run_parallel<R>(
    rt: &Arc<Runtime>,
    engine: Arc<ParEngine>,
    feed: impl FnOnce(&mut ParSession<'_>) -> WriteResult<R>,
) -> WriteResult<(Vec<SealedPart>, Vec<SealReport>, R)> {
    let stream = Arc::new(StreamSource::new());
    let (_rg, waiter): (runtime::RgHandle, CompletionWaiter) = rt.submit(QuerySpec {
        query_id: engine.fxid(),
        tasksets: vec![TaskSetSpec {
            source: Arc::clone(&stream) as Arc<dyn runtime::MorselSource>,
            work: Arc::new(IngestWork {
                engine: Arc::clone(&engine),
            }),
            deps: Vec::new(),
        }],
    });
    let ncols = engine.ncols();
    let mut sess = ParSession {
        engine: &engine,
        stream: &stream,
        rt,
        cur: RowChunk::new(ncols, engine.chunk_rows()),
        published: 0,
        ncols,
    };
    // Feed; flush the tail; on ANY failure record it (first-wins) and fall
    // through to the drain — the stream must close regardless so the RG
    // completes and no task is left holding a claim.
    let fed: WriteResult<R> = feed(&mut sess).and_then(|r| {
        sess.flush()?;
        Ok(r)
    });
    let published = sess.published;
    match &fed {
        Ok(_) => {
            engine.close_input(published);
        }
        Err(e) => engine.fail(e.clone()),
    }
    stream.close();
    rt.notify_source_progress();
    let outcome = waiter.wait();
    let e: WriteError = match fed {
        Ok(r) => match engine.collect() {
            Ok(sealed) if outcome == RgOutcome::Completed => {
                let mut parts = Vec::with_capacity(sealed.len());
                let mut reports = Vec::with_capacity(sealed.len());
                for (p, rep) in sealed {
                    parts.push(p);
                    reports.push(rep);
                }
                return Ok((parts, reports, r));
            }
            Ok(_) => WriteError::Contract {
                detail: "parallel ingest resource group aborted",
            },
            Err(e) => e,
        },
        Err(e) => e,
    };
    // Error path: the drain is complete (waiter returned), so no task is
    // mid-file — unlink the assigned temps.
    engine.cleanup_temps()?;
    Err(e)
}

/// The whole-statement convenience over a [`TableWriter`]: build the engine
/// from the writer's facts, run the session, deposit the ordered results
/// (the self-checking `deposit_sealed_parts` face). On `Ok` the writer is
/// exactly where an equivalent serial statement would have left it;
/// `publish`/`abort`/registry lifecycle proceed UNCHANGED above the frozen
/// seal boundary.
pub fn parallel_ingest<R>(
    w: &mut TableWriter,
    rt: &Arc<Runtime>,
    providers: ParProviders,
    opts: ParIngestOpts,
    feed: impl FnOnce(&mut ParSession<'_>) -> WriteResult<R>,
) -> WriteResult<R> {
    let facts = w.par_facts()?;
    // v4 delta (TY-1): the writer's structural-election facts override the
    // provider default — the parallel seal must elect exactly as a serial
    // statement on the SAME writer would (byte-identical-parts law). The
    // facts side carries them since the v4 rebuild; a driver-supplied
    // posture that disagreed with the writer would silently split election
    // behavior across the serial/parallel arms.
    let mut providers = providers;
    providers.structural = facts.structural.clone();
    let engine = Arc::new(ParEngine::new(
        providers,
        facts.schema,
        facts.spec,
        facts.table_dir,
        facts.fxid,
        facts.policy,
        opts,
        facts.base_seq,
    )?);
    let (parts, reports, r) = run_parallel(rt, engine, feed)?;
    w.deposit_sealed_parts(parts, reports)?;
    Ok(r)
}
