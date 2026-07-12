//! Push control-model skeleton for lane-executor-v2 (design §Architecture 1).
//!
//! The lane pipeline is a **push island with a pull adapter at its root**:
//!
//! ```text
//!   Source ──batch──▶ Operator (…chain…) ──tuple──▶ Sink (RootAdapter)
//!      ▲                                                    │ buffers ≤ 1 tuple
//!      └──────────── pipeline driver (pull_step) ◀──────────┘
//!                              ▲
//!                PG's Volcano executor pulls one tuple
//!                per `exec_proc_node` call
//! ```
//!
//! Control flows *forward* (push): the driver pulls a batch from the source
//! and pushes it through the operator chain into the sink; operators never
//! pull from a child. PostgreSQL's executor stays Volcano/pull, so the
//! pipeline ROOT presents a pull face to PG: a capacity-one buffer the
//! pipeline fills and `exec_proc_node` drains, one tuple per call.
//!
//! Why capacity one: byte-identity. The per-tuple path resets the node's
//! per-tuple expression context before evaluating each row's qual/projection,
//! so at most one produced tuple is ever live; buffering more would (a) reset
//! the context under the parent's view of the current tuple and (b) evaluate
//! quals/projections on rows the per-tuple path would never reach (LIMIT /
//! error-in-order / volatile-qual invocation counts). The `SinkFeed::Full`
//! backpressure signal makes the push pipeline exactly as lazy as the pull
//! drive it replaces: same primitive calls, same order, same per-row
//! semantics — ONLY the control model (who calls whom) changes.
//!
//! Cross-call state (the staged batch + consume position) stays node-resident
//! (`lane_cursor`), surviving the Volcano call boundary; the pipeline stage
//! objects are stateless and reassembled per call (free — they are unit
//! structs). One `&mut` executor node exists, so the driver owns it and
//! threads it into each stage call (`Source::Node`/`Operator::Node`).

use ::executils::{EStateData, ExecSlotId};
use ::types_error::PgResult;

/// A batch flowing source → operator(s) → sink. The staged rows themselves
/// live in node-owned staging (heap page batch / index TID run); `n` is the
/// staged row count, rows addressed `0..n` through the owning node's batch
/// primitives.
#[derive(Clone, Copy, Debug)]
pub(super) struct Batch {
    pub(super) n: u32,
}

/// Backpressure signal returned by `Sink::accept`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SinkFeed {
    /// The sink can take more tuples.
    ///
    /// Never produced by the capacity-one `RootAdapter`; pipeline breakers
    /// (hash-agg build, sort feed) accept whole inputs and return it.
    NeedMore,
    /// The sink is full: the pushing operator must save its position and
    /// return `OpStatus::Paused` so the driver hands control back to PG.
    Full,
}

/// What an `Operator::consume` step did with the batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum OpStatus {
    /// Batch fully consumed; the driver should produce the next one.
    NeedInput,
    /// The sink went `Full` mid-batch; position saved (see
    /// `Operator::pending`), resumed on a later driver round.
    Paused,
    /// The operator will never produce again (LIMIT reached, semi/anti
    /// satisfied, merge side exhausted — Phase-2 breadth operators). Only
    /// returned when the root buffer is empty: if the last `accept` came back
    /// `Full`, the operator must return `Paused` first so the boundary tuple
    /// is delivered, and report `Finished` on the next driver round
    /// (byte-identity: the source is pulled exactly to the boundary tuple's
    /// batch and no further — push-executor study, Pattern 2).
    Finished,
}

/// Produces batches — a scan is a source. `Node` is the executor node owning
/// the staged storage + scan position; the driver threads it into every stage
/// call, so the stage objects themselves hold no node borrow.
pub(super) trait Source<'mcx> {
    type Node;
    /// Stage the next batch into node-owned storage; `None` = exhausted.
    fn produce(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<Batch>>;
}

/// A push operator: consumes a staged batch — doing its work row-by-row (the
/// scalar-within-lane filter/project segment) — and pushes produced tuples
/// into `out`. It never pulls from a child. Must honor `SinkFeed::Full` by
/// pausing with its position saved node-side (it must survive the PG pull
/// boundary).
///
/// Scan-only pipelines have exactly one operator; Phase-2 chains splice
/// operators by handing an upstream operator a `Sink` adapter that feeds the
/// downstream one.
pub(super) trait Operator<'mcx> {
    type Node;
    /// The not-yet-consumed remainder of a previously accepted batch; `None`
    /// = the driver must `produce` a fresh batch.
    fn pending(&self, node: &Self::Node) -> Option<Batch>;
    /// Push (the rest of) `batch` into `out`.
    fn consume(
        &mut self,
        node: &mut Self::Node,
        batch: Batch,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus>;
}

/// A pipeline endpoint. For scan-only pipelines this is the `RootAdapter`;
/// Phase-2 pipeline breakers (hash-agg build, hash-join build, sort feed)
/// implement this to collect an entire input before their output pipeline
/// runs.
pub(super) trait Sink<'mcx> {
    /// Accept one produced tuple (by slot id).
    fn accept(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<SinkFeed>;
    /// Upstream exhausted: final flush/cleanup.
    fn finish(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()>;
}

/// The pull adapter at the pipeline root — the PG boundary. PG pulls one
/// tuple per `exec_proc_node` call; the pipeline pushes into this
/// capacity-one buffer, the `Full` backpressure pauses the pipeline, and the
/// driver drains the buffer to PG (see module docs for why exactly one).
pub(super) struct RootAdapter {
    buffered: Option<ExecSlotId>,
    /// End-of-stream projected-slot clear, mirroring `ExecScanExtended`'s
    /// end-of-scan behavior (`None` for non-projecting pipelines, which
    /// return end-of-scan without clearing).
    clear_on_finish: Option<ExecSlotId>,
}

impl RootAdapter {
    pub(super) fn new(clear_on_finish: Option<ExecSlotId>) -> Self {
        RootAdapter { buffered: None, clear_on_finish }
    }

    /// The PG-side pull face: drain the buffered tuple.
    fn take(&mut self) -> Option<ExecSlotId> {
        self.buffered.take()
    }
}

impl<'mcx> Sink<'mcx> for RootAdapter {
    fn accept(
        &mut self,
        tuple: ExecSlotId,
        _estate: &mut EStateData<'mcx>,
    ) -> PgResult<SinkFeed> {
        // Overfill = an operator ignored `SinkFeed::Full`; silently replacing
        // the buffered tuple would be silent row loss, so this is a hard
        // error in release too, not just a debug assert.
        if self.buffered.is_some() {
            return Err(Box::new(::types_error::PgError::error(
                "lane-v2 root pull-adapter overfilled (operator ignored SinkFeed::Full)"
                    .to_string(),
            )));
        }
        self.buffered = Some(tuple);
        Ok(SinkFeed::Full)
    }

    fn finish(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        if let Some(slot) = self.clear_on_finish {
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(slot), mcx);
        }
        Ok(())
    }
}

/// The pipeline driver, one PG pull's worth: **pull a batch from the source
/// and push it through the operator chain into the sink**, repeating until
/// the root adapter buffers a tuple (backpressure pause) or the source is
/// exhausted. Returns the drained tuple — the `exec_proc_node` contract.
pub(super) fn pull_step<'mcx, S, O>(
    node: &mut S::Node,
    src: &mut S,
    op: &mut O,
    root: &mut RootAdapter,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>>
where
    S: Source<'mcx>,
    O: Operator<'mcx, Node = S::Node>,
{
    debug_assert!(root.buffered.is_none());
    loop {
        let batch = match op.pending(node) {
            Some(b) => b,
            None => match src.produce(node, estate)? {
                Some(b) => b,
                None => {
                    root.finish(estate)?;
                    return Ok(None);
                }
            },
        };
        match op.consume(node, batch, root, estate)? {
            OpStatus::Paused => {
                let t = root.take();
                debug_assert!(t.is_some(), "operator paused on a non-full root");
                return Ok(t);
            }
            OpStatus::NeedInput => {}
            // Operator-driven early stop: treated exactly like source
            // exhaustion (the source is never pulled again). Legal only with
            // an empty root buffer — the Paused-then-Finished rule above.
            OpStatus::Finished => {
                debug_assert!(root.buffered.is_none(), "Finished with a buffered tuple");
                root.finish(estate)?;
                return Ok(None);
            }
        }
    }
}

/// A mid-pipeline expanding operator — the minimal operator-CHAIN seam
/// (design §Architecture 1: "expanding operators (join probe, unnest) keep
/// intra-row expansion state node-resident so a mid-expansion pause resumes
/// exactly"). Where `Operator` consumes node-staged batches, a `TupleOp` sits
/// BETWEEN an upstream operator and the pipeline sink: it accepts one input
/// tuple at a time (pushed by the upstream operator through a `TupleOpSink`
/// adapter) and pushes 0..K produced tuples into the downstream sink.
///
/// Pause protocol: if the downstream sink goes `Full` mid-expansion, the op
/// returns `Paused` with its position saved node-resident (e.g. the hash
/// join's own `hj_CurTuple` bucket cursor); the chain driver must `resume` it
/// before feeding the next upstream tuple — otherwise the remainder of the
/// expansion would be lost.
pub(super) trait TupleOp<'mcx> {
    /// An accepted tuple's expansion is not yet fully emitted.
    fn pending(&self) -> bool;
    /// Accept one upstream tuple and push its expansion into `out`.
    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus>;
    /// Continue a paused expansion into `out`.
    fn resume(
        &mut self,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus>;
}

/// Splices a `TupleOp` between an upstream `Operator` and the pipeline sink
/// (the module-doc chaining shape: "Phase-2 chains splice operators by
/// handing an upstream operator a `Sink` adapter that feeds the downstream
/// one"). `Paused` (downstream full mid-expansion) maps to `SinkFeed::Full`,
/// pausing the upstream operator too — both positions are node-resident, so
/// the chain driver resumes the downstream op first, then the upstream batch.
struct TupleOpSink<'a, 'b, 'mcx> {
    op: &'a mut dyn TupleOp<'mcx>,
    out: &'b mut dyn Sink<'mcx>,
}

impl<'mcx> Sink<'mcx> for TupleOpSink<'_, '_, 'mcx> {
    fn accept(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<SinkFeed> {
        Ok(match self.op.accept(tuple, self.out, estate)? {
            OpStatus::NeedInput => SinkFeed::NeedMore,
            OpStatus::Paused => SinkFeed::Full,
            OpStatus::Finished => {
                // Early-stop TupleOps (LimitOp) obey the Paused-then-Finished
                // rule: accept() delivers the boundary tuple via `Paused` and
                // only resume() — called by the driver directly, never
                // through this splice — reports `Finished`.
                unreachable!("mid-chain TupleOp returned Finished from accept")
            }
        })
    }

    fn finish(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        self.out.finish(estate)
    }
}

/// `pull_step` over a two-operator chain (upstream batch operator, then a
/// `TupleOp`): one PG pull's worth. The downstream op's pending expansion is
/// always resumed BEFORE the upstream feed advances — the upstream operator
/// consumed the expanding tuple already, so its remainder exists only in the
/// downstream op's node-resident cursor.
pub(super) fn pull_step_chain<'mcx, S, O>(
    node: &mut S::Node,
    src: &mut S,
    op: &mut O,
    top: &mut dyn TupleOp<'mcx>,
    root: &mut RootAdapter,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>>
where
    S: Source<'mcx>,
    O: Operator<'mcx, Node = S::Node>,
{
    debug_assert!(root.buffered.is_none());
    loop {
        if top.pending() {
            match top.resume(root, estate)? {
                OpStatus::Paused => {
                    let t = root.take();
                    debug_assert!(t.is_some(), "TupleOp paused on a non-full root");
                    return Ok(t);
                }
                OpStatus::NeedInput => {}
                OpStatus::Finished => {
                    debug_assert!(root.buffered.is_none(), "Finished with a buffered tuple");
                    root.finish(estate)?;
                    return Ok(None);
                }
            }
        }
        let batch = match op.pending(node) {
            Some(b) => b,
            None => match src.produce(node, estate)? {
                Some(b) => b,
                None => {
                    root.finish(estate)?;
                    return Ok(None);
                }
            },
        };
        let mut mid = TupleOpSink { op: top, out: root };
        match op.consume(node, batch, &mut mid, estate)? {
            OpStatus::Paused => {
                let t = root.take();
                debug_assert!(t.is_some(), "operator paused on a non-full root");
                return Ok(t);
            }
            OpStatus::NeedInput => {}
            OpStatus::Finished => {
                debug_assert!(root.buffered.is_none(), "Finished with a buffered tuple");
                root.finish(estate)?;
                return Ok(None);
            }
        }
    }
}

/// `drain_pipeline` over a two-operator chain: run the whole feed (scan →
/// upstream operator → `TupleOp` → breaker sink) to exhaustion, then
/// `finish()` the sink. Breaker sinks never fill, so neither op ever pauses.
pub(super) fn drain_pipeline_chain<'mcx, S, O>(
    node: &mut S::Node,
    src: &mut S,
    op: &mut O,
    top: &mut dyn TupleOp<'mcx>,
    sink: &mut dyn Sink<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()>
where
    S: Source<'mcx>,
    O: Operator<'mcx, Node = S::Node>,
{
    loop {
        debug_assert!(!top.pending(), "chain build pipeline paused: breaker sink returned Full");
        let batch = match op.pending(node) {
            Some(b) => b,
            None => match src.produce(node, estate)? {
                Some(b) => b,
                None => break,
            },
        };
        let mut mid = TupleOpSink { op: top, out: sink };
        match op.consume(node, batch, &mut mid, estate)? {
            OpStatus::NeedInput => {}
            OpStatus::Finished => break,
            OpStatus::Paused => {
                unreachable!("chain build pipeline paused: breaker sink returned Full")
            }
        }
    }
    sink.finish(estate)
}

/// The build-pipeline driver — pipeline N in full: drain the source through
/// the operator chain into a pipeline-breaker sink to completion, then
/// `finish()` the sink (= Finalize; the breaker delegates it to the row-path
/// build — hashagg spill finish, `tuplesort_performsort`, hash build, …).
/// Breaker sinks accept whole inputs (`SinkFeed::NeedMore`, never `Full`), so
/// the pipeline never pauses: the whole feed runs inside one `exec_proc_node`
/// call, mirroring C's build-before-first-probe order (nodeAgg's
/// agg_fill_hash_table, exec_sort's feed loop, nodeHashjoin's
/// HJ_BUILD_HASHTABLE) for free; the node-side phase flag then flips the
/// breaker to its `Source` face for pipeline N+1.
pub(super) fn drain_pipeline<'mcx, S, O>(
    node: &mut S::Node,
    src: &mut S,
    op: &mut O,
    sink: &mut dyn Sink<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()>
where
    S: Source<'mcx>,
    O: Operator<'mcx, Node = S::Node>,
{
    loop {
        let batch = match op.pending(node) {
            Some(b) => b,
            None => match src.produce(node, estate)? {
                Some(b) => b,
                None => break,
            },
        };
        match op.consume(node, batch, sink, estate)? {
            OpStatus::NeedInput => {}
            OpStatus::Finished => break,
            // Breaker sinks never return `Full`; a pause here means a
            // non-breaker sink was wired into a build pipeline. A silent
            // continue would spin forever on the paused operator, so this is
            // a hard bug-panic in release too.
            OpStatus::Paused => unreachable!("build pipeline paused: breaker sink returned Full"),
        }
    }
    sink.finish(estate)
}
