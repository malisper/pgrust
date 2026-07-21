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
    /// Batch-granular variant for BREAKER-fed pipelines (`drain_pipeline`):
    /// hand the sink the whole staged range once (`BatchSink::accept_batch`)
    /// instead of one dyn `accept` per produced tuple. Operators that
    /// override this skip the per-row consume-cursor saves too — sound only
    /// because a breaker sink never pauses (an error mid-batch aborts the
    /// query; a rescan restages). Default: the per-row `consume`, unchanged.
    fn consume_batch<K: BatchSink<'mcx>>(
        &mut self,
        node: &mut Self::Node,
        batch: Batch,
        out: &mut K,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        self.consume(node, batch, out, estate)
    }
    /// Arm the direct sort-key feed on the operator's leaf (the lane mirror
    /// of `SortFeedSource::key_direct`): probed ONCE by the sort breaker's
    /// feed driver, BEFORE the first `produce` (arming decides what the
    /// staging pass stages), and only for datum sorts. True arms
    /// `BatchEmit::emit_key` — output column 0 served straight from the
    /// leaf's staged column (value/null identical to `emit` +
    /// `slot_getsomeattrs(1)`, no qual, same row order). Default: never arms.
    fn arm_sort_key(
        &mut self,
        _node: &mut Self::Node,
        _estate: &mut EStateData<'mcx>,
    ) -> bool {
        false
    }
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
    /// Combine-before-finish (the Stage-4 seam, reserved since Phase 2):
    /// a parallel worker's breaker publishes its partial state for the
    /// cross-worker combine here — the hash-agg breaker hands its whole
    /// table to the leader by pointer (nodeagg::merge handoff; the leader
    /// merges partition-parallel with the ported combinefn machinery) —
    /// before `finish` flips the breaker to its Source face. Serial
    /// pipelines and non-partial sinks keep the default no-op; drivers call
    /// it exactly once, immediately before `finish`.
    fn combine(&mut self, _estate: &mut EStateData<'mcx>) -> PgResult<()> {
        Ok(())
    }
    /// Upstream exhausted: final flush/cleanup.
    fn finish(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()>;
}

/// Per-row emit face over a staged batch: the operator's filter/project
/// segment bound to its node, handed to a batch-granular sink
/// (`BatchSink::accept_batch`) so the sink runs the per-row delegation loop
/// internally. `emit` must reproduce the owning operator's `consume` body for
/// staged row `i` EXACTLY — same primitive, same interrupt cadence, same
/// order — so a batch-fed sink sees the identical row stream the per-row
/// `accept` feed would deliver.
pub(super) trait BatchEmit<'mcx> {
    /// Produce staged row `i`'s output slot; `None` = qual-filtered.
    fn emit(
        &mut self,
        i: u32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>>;
    /// Direct sort-key read for staged row `i` (`SortFeedSource::emit_key`'s
    /// lane mirror): only meaningful after the owning operator's
    /// `arm_sort_key` returned true for the feed. `None` = staged row not
    /// covered (narrow-tuple fallback); the caller takes the full `emit`
    /// path for that row. Default: never serves.
    fn emit_key(&mut self, _i: u32) -> Option<(::datum::Datum, bool)> {
        None
    }

    /// Staged leading-sort-key lane of the current batch for the sort
    /// breaker's streaming top-k cutoff: `(values, isnull, fallback_words)`
    /// over the first `n` staged rows, or `None` when no key lane is staged
    /// (the default — only the seqscan emit face arms one). The sink may
    /// consult this only when its own top-k pre-filter was armed against the
    /// SAME node (the arm and the emit face are wired together per feed).
    fn topk_key_lane(&self, _n: u32) -> Option<(&[::datum::Datum], &[bool], &[u64])> {
        None
    }

    /// Consumer bound feedback for a zone-adaptive top-N scan: the bounded
    /// sort's current k-th boundary LEADING-key datum (by-value; the arm
    /// admits int-family keys only). Default no-op; only the seqscan emit
    /// face forwards it to the AM, where an unarmed scan ignores it.
    fn push_topk_bound(&mut self, _key: ::datum::Datum) {}

    /// Dict-code answer for the staged window's direct key column (the
    /// distinct-set text key feed; armed via `seq_scan_key_dict_arm`).
    /// `Some` = the window is dict-coded and the key's datum cells are STALE
    /// — the sink must consume codes+dict for the whole window and skip
    /// `emit_key`. Default: never serves (only the seqscan emit face can).
    fn key_dict_lane(&self) -> Option<::exectuples::SoaDictLane> {
        None
    }

    /// Staged-window base for ref-carrying sinks (the refsort feed): (row
    /// group, rg-global row index of staged row 0); the ref of staged row
    /// `i` is `base + i`. Default `None` = no ref mode (heap batches, non-
    /// scan emits) — a ref-carrying sink must demote to the legacy feed.
    fn window_ref(&self) -> Option<(u32, u32)> {
        None
    }

    /// The refsort fast leg's batch view for scan column `col`:
    /// `(key_values, key_isnull, fallback_words, sel_words)` — see
    /// `nodeseqscan::seq_scan_refsort_key_batch` for the soundness contract.
    /// Default `None` = every row takes the per-row `emit` path.
    fn refsort_key_batch(
        &self,
        _col: u16,
        _n: u32,
    ) -> Option<(&[::datum::Datum], &[bool], &[u64], Option<&[u64]>)> {
        None
    }

    /// Physical rowref base of the CURRENT staged batch (tie-ordering rule
    /// 2, the zone-adaptive rowref-selection sort feed): staged row `i`'s
    /// rowref is `base + i`. Default: never serves (only the pgrcolumnar-backed
    /// seqscan emit face carries physical rowrefs).
    fn rowref_base(&self) -> Option<u64> {
        None
    }

    /// Stitched dict-code view of scan column `col` for the CURRENT staged
    /// window (the DictCode sort-key class, docs/design/dict-code-flow.md
    /// inc-1): codes + per-RG dict identity, with the v7 part-global stitch
    /// published when the scan carries one. `Some` certifies only the
    /// window's codes/dict identity; a consumer using codes for ORDER
    /// semantics must additionally gate on `table.has_stitch()` and fail
    /// closed otherwise. Default: never serves (only the pgrcolumnar-backed
    /// seqscan emit face can).
    fn refsort_dictcode_batch(&mut self, _col: u16) -> Option<::exectuples::SoaDictLane> {
        None
    }

    /// Column-independent staged-batch masks for the refsort fast leg:
    /// `(fallback_words, sel_words)` — see
    /// `nodeseqscan::seq_scan_refsort_batch_masks` for the soundness
    /// contract. Default `None` = no certified masks (the caller fails
    /// closed or takes the per-row emit path).
    fn refsort_batch_masks(&self, _n: u32) -> Option<(&[u64], Option<&[u64]>)> {
        None
    }

    /// Survivor-bit snapshot of the CURRENT staged batch's qual selection:
    /// a CLEARED bit means `emit(i)` returns `None` with no observable
    /// side effect (the staged qual verdict already rejected row i without
    /// running the original qual — the PREWHERE selection contract; requal
    /// and fallback rows carry SET bits), so a batch-feeding sink may skip
    /// cleared rows without the `emit` call. Weaker than a qual-verdict
    /// lane: SET bits may still be filtered by `emit` itself. Default
    /// `None` = no live bitmap; every position must go through `emit`.
    fn live_sel(&self) -> Option<[u64; ::exectuples::SOA_BM_WORDS]> {
        None
    }
}

/// Batch-granular accept face for pipeline-BREAKER sinks (the Phase-3
/// "batch-granular sink calls" item). Instead of one dyn `accept` per
/// produced tuple, the operator hands the sink its per-row emit face plus the
/// staged range once per batch, and the sink runs the per-row delegation loop
/// internally. `accept_batch` is generic over the emit type, so the whole
/// loop monomorphizes: no per-tuple dyn dispatch, no per-row `SinkFeed`
/// status matching, no per-row consume-cursor saves — and a sink may hoist
/// per-put invariants (the sort breaker hoists its tuplesort handle and holds
/// the by-val datum batch putter open across the batch, exactly as
/// `exec_sort`/`exec_sort_batched` do).
///
/// BREAKERS ONLY: a batch-fed sink must consume the whole range —
/// `SinkFeed::Full` mid-batch is the same protocol violation it is in
/// `drain_pipeline`, and the default loop hard-errors on it (never reached by
/// the real breakers, which are structurally `NeedMore`). The capacity-one
/// `RootAdapter` (the PG pull face) stays per-row by design.
///
/// Byte-identity: the default impl is the per-row feed loop the operator ran
/// before (same emit, same accept, same order), word-skipping positions the
/// feed's `live_sel` snapshot proves emit-dead (a cleared bit = `emit`
/// returns None with no observable effect, so the surviving feed stream is
/// identical); overrides must keep the same per-row delegation in the same
/// order — dispatch granularity and emit-dead skips are the ONLY changes.
pub(super) trait BatchSink<'mcx>: Sink<'mcx> {
    /// Feed staged rows `pos..n` through `emit` into the sink.
    fn accept_batch<E: BatchEmit<'mcx>>(
        &mut self,
        emit: &mut E,
        pos: u32,
        n: u32,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()> {
        // Word-skip the feed's qual-survivor snapshot (`live_sel`): a
        // cleared bit answers `emit` with None and no observable effect, so
        // skipping it is feed-stream-identical — the per-row emit ceremony
        // collapses to one word test per 64 rows on selective quals (the
        // qualed-top-n sort-feed lever, generalized; CFI cadence for skipped rows
        // follows the page-level staging check, the topk-cut precedent).
        let live = emit.live_sel();
        ::exectuples::for_each_live(live.as_ref().map(|w| &w[..]), pos, n, |i| -> PgResult<()> {
            if let Some(slot) = emit.emit(i, estate)? {
                match self.accept(slot, estate)? {
                    SinkFeed::NeedMore => {}
                    // A breaker never fills; see `drain_pipeline`'s Paused arm.
                    SinkFeed::Full => {
                        return Err(Box::new(::types_error::PgError::error(
                            "lane-v2 batch feed: breaker sink returned Full".to_string(),
                        )))
                    }
                }
            }
            Ok(())
        })
    }
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
    /// The upstream source is exhausted — the `Finished`-vs-more-phases
    /// seam. An op with a post-exhaustion phase flips into source mode here
    /// and pushes into the SAME sink: the right-fill hash join's
    /// unmatched-BUILD fill scan (HJ_FILL_INNER_TUPLES), or the sorted-agg
    /// operator's final open-group flush. `Paused` = downstream full
    /// (position node-resident; a multi-row phase must report `pending()`
    /// true so the driver `resume`s it on the next round — a single-tuple
    /// tail may instead rely on the driver re-calling this method), anything
    /// else = nothing further will ever be produced (the driver then
    /// finishes the sink). Called possibly repeatedly — implementations must
    /// be idempotent once drained (the sorted-agg op's `agg_done` is; a
    /// drained fill scan reports `Finished`). Default: no post-exhaustion
    /// phase.
    fn source_exhausted(
        &mut self,
        _out: &mut dyn Sink<'mcx>,
        _estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        Ok(OpStatus::Finished)
    }
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
                    // The Finished-vs-more-phases seam: a TupleOp with a
                    // post-exhaustion phase (right-fill hash join) keeps
                    // producing into the root here.
                    match top.source_exhausted(root, estate)? {
                        OpStatus::Paused => {
                            let t = root.take();
                            debug_assert!(t.is_some(), "TupleOp paused on a non-full root");
                            return Ok(t);
                        }
                        _ => {
                            debug_assert!(
                                root.buffered.is_none(),
                                "post-exhaustion phase done with a buffered tuple"
                            );
                            root.finish(estate)?;
                            return Ok(None);
                        }
                    }
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

/// A row-mode leaf: produces at most one tuple per step (a singleton batch)
/// — the row-mode mirror of `Source`, and the missing LEAF half of the
/// row-mode operator contract `TupleOp` ratifies (see
/// docs/design/rowmode-operators.md). Per-row cross-call state (done flags,
/// probe cursors) is node-resident, and every implementation reuses its
/// node's ported per-row body (code moves, not rewrites), so error unwind
/// and interrupt cadence are the Volcano body's own.
pub(super) trait RowSource<'mcx> {
    type Node;
    /// Produce the next tuple; `None` = exhausted. Replays the wrapped
    /// Volcano body's own entry-CFI cadence (one per produced row).
    fn next_row(
        &mut self,
        node: &mut Self::Node,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>>;
}

/// One PG pull over a row-mode pipeline: `RowSource` → `TupleOp` →
/// `RootAdapter`. `pull_step_chain` minus the batch-staging layer — the
/// source row IS the batch: resume a pending expansion BEFORE producing (the
/// expansion's remainder exists only in the op's node-resident cursor), then
/// produce → accept rounds until the capacity-one root pauses the pipeline
/// or the source is exhausted (then `top.source_exhausted` → `root.finish`).
/// Same `OpStatus` arms, same Paused-then-Finished rule, same debug_asserts
/// as `pull_step_chain`.
pub(super) fn pull_step_rows<'mcx, S>(
    node: &mut S::Node,
    src: &mut S,
    top: &mut dyn TupleOp<'mcx>,
    root: &mut RootAdapter,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>>
where
    S: RowSource<'mcx>,
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
        let Some(row) = src.next_row(node, estate)? else {
            // The Finished-vs-more-phases seam, exactly as in
            // `pull_step_chain`: a TupleOp with a post-exhaustion phase keeps
            // producing into the root here.
            match top.source_exhausted(root, estate)? {
                OpStatus::Paused => {
                    let t = root.take();
                    debug_assert!(t.is_some(), "TupleOp paused on a non-full root");
                    return Ok(t);
                }
                _ => {
                    debug_assert!(
                        root.buffered.is_none(),
                        "post-exhaustion phase done with a buffered tuple"
                    );
                    root.finish(estate)?;
                    return Ok(None);
                }
            }
        };
        match top.accept(row, root, estate)? {
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
                None => {
                    // Post-exhaustion phase (right-fill hash join): the
                    // TupleOp keeps producing into the breaker sink, which
                    // never fills, so the fill runs to completion here.
                    if top.source_exhausted(sink, estate)? == OpStatus::Paused {
                        unreachable!(
                            "chain build pipeline paused: breaker sink returned Full"
                        )
                    }
                    break;
                }
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
    // Upstream exhausted (second, idempotent seam call for ops whose
    // post-exhaustion phase ran inside the loop): flush the TupleOp's tail
    // into the breaker sink (breaker sinks never fill, so a flush cannot
    // pause) before finishing.
    if let OpStatus::Paused = top.source_exhausted(sink, estate)? {
        unreachable!("chain build pipeline paused in flush: breaker sink returned Full")
    }
    sink.combine(estate)?;
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
/// Generic (not dyn) over the sink so `Operator::consume_batch` +
/// `BatchSink::accept_batch` monomorphize the whole feed loop — the
/// batch-granular dispatch that displaces the per-row dyn `accept` calls.
pub(super) fn drain_pipeline<'mcx, S, O, K>(
    node: &mut S::Node,
    src: &mut S,
    op: &mut O,
    sink: &mut K,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()>
where
    S: Source<'mcx>,
    O: Operator<'mcx, Node = S::Node>,
    K: BatchSink<'mcx>,
{
    loop {
        let batch = match op.pending(node) {
            Some(b) => b,
            None => match src.produce(node, estate)? {
                Some(b) => b,
                None => break,
            },
        };
        match op.consume_batch(node, batch, sink, estate)? {
            OpStatus::NeedInput => {}
            OpStatus::Finished => break,
            // Breaker sinks never return `Full`; a pause here means a
            // non-breaker sink was wired into a build pipeline. A silent
            // continue would spin forever on the paused operator, so this is
            // a hard bug-panic in release too.
            OpStatus::Paused => unreachable!("build pipeline paused: breaker sink returned Full"),
        }
    }
    sink.combine(estate)?;
    sink.finish(estate)
}

/// Canonical never-pending pass-through `TupleOp` for hosting bare leaves
/// (Phase-1 integration contract §2b: ONE definition, this spelling;
/// consumed by WS-G's merge-join hosting and WS-J's express mode 2).
/// `accept` forwards the tuple and maps the sink's backpressure verbatim
/// (`Full` → `Paused`, `NeedMore` → `NeedInput` — the Paused-then-Finished
/// rule per the `OpStatus` docs); `pending()` is always false, so `resume`
/// is unreachable by the driver contract — it fails LOUDLY as a `PgError`
/// (panicfix discipline: never `unreachable!()` on a plausible-path arm)
/// plus a debug assert. `source_exhausted`: the default (`Finished`).
pub(super) struct PassthroughOp;

impl<'mcx> TupleOp<'mcx> for PassthroughOp {
    fn pending(&self) -> bool {
        false
    }

    fn accept(
        &mut self,
        tuple: ExecSlotId,
        out: &mut dyn Sink<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        Ok(match out.accept(tuple, estate)? {
            SinkFeed::Full => OpStatus::Paused,
            SinkFeed::NeedMore => OpStatus::NeedInput,
        })
    }

    fn resume(
        &mut self,
        _out: &mut dyn Sink<'mcx>,
        _estate: &mut EStateData<'mcx>,
    ) -> PgResult<OpStatus> {
        debug_assert!(false, "PassthroughOp::resume: pending() is always false");
        Err(Box::new(::types_error::PgError::error(
            "lane-v2 PassthroughOp resumed with no pending expansion (driver contract violation)"
                .to_string(),
        )))
    }
}

/// §5's express driver (rowmode-operators.md): a SOURCE-ONLY row pipeline.
/// NO `TupleOp`, NO `RootAdapter`, no capacity-one buffer — `src.next_row`
/// is returned directly (the buffer exists only to backpressure multi-row
/// operators; a bare row source needs none). This degenerate driver is HOW
/// instruction parity with the fused per-tuple path is reachable: the pull
/// is the same per-row call chain as Volcano with only the admission verdict
/// on top.
///
/// SCOPE RATIFICATION (se-delegtax, 2026-07-17; supersedes the Phase-1
/// integration-contract §2b lock, which held "until the fleet G1–G4
/// verdict" — that verdict exists: se-express-adm §3). This is THE shared
/// driver for every PURE DELEGATION LEAF: any pipeline of the exact shape
/// `RowSource → PassthroughOp → RootAdapter::new(None)` is
/// statement-identical to a bare `src.next_row` call BY CONSTRUCTION —
/// `PassthroughOp::pending()` is constantly false (resume unreachable);
/// `Some(row)` maps accept→buffer→Full→Paused→take back to `Some(row)`;
/// `None` maps source_exhausted→Finished→finish(no clear)→`None`; errors
/// propagate untouched on both drivers. The full `pull_step_rows` stays the
/// driver for real `TupleOp` chains (ProjectSet). SE4-GATES measured the
/// pipeline round trip (2 dyn calls + the capacity-one buffer protocol per
/// pull) as the dominant share of the FLIP-1/FLIP-2 lane tax; this driver
/// is the deletion.
#[inline(always)]
pub(super) fn pull_step_point<'mcx, S: RowSource<'mcx>>(
    node: &mut S::Node,
    src: &mut S,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    src.next_row(node, estate)
}

// --- WS-AI wave-9 (forward-pull cursors inc-1; contract §3, band 92001+) ------
//
// §1's budget-N emit sink, substrate half. `ExecutorRun(Forward, N)` installs
// a per-run emission budget on the ESTATE (`es_cursor_run_budget`): the run
// seam (`execmain.rs::execute_plan`) computes it here and writes the field
// UNCONDITIONALLY at run entry, so the budget is per-run by construction,
// nested-ExecutorRun-safe (SPI inside a FETCH runs on its own estate) and
// unwind-safe with no guard. Estate-resident rather than thread_local by
// the TLS-census-zero law (contract §8 law 8; the session TLS census pin
// stays 479) — and it is the shape C itself uses for per-run state
// (es_direction/es_processed).
//
// ENFORCEMENT HONESTY (recorded, not hidden): the capacity-one `RootAdapter`
// already pauses the pipeline after EVERY emitted tuple (`SinkFeed::Full` →
// `OpStatus::Paused`, position node-resident), and the run loop above
// (`execute_plan`'s `number_tuples` check — C's own ExecutePlan enforcement)
// stops the drive at exactly N pulls. Budget-zero ⇒ Paused is therefore
// STRUCTURAL today: no per-accept budget decrement is wired in inc-1
// (a per-tuple field test on the knob-OFF hot path would break the
// instruction-invisibility law for zero behavior). The installed budget is
// the cross-module signal the park/settle glue (§2, inc-1b) and the
// single-executor push-drive endgame read; when the emit face is driven as
// a push sink (capacity > 1), the decrement moves INTO `RootAdapter::accept`
// and this field is its source of truth.
//
// §3 serial law (the ported execmain.rs:978 gate — `use_parallel_mode` only
// when `!already_executed && count == 0`): every count-limited run is DOP-1
// caller-as-worker. `cursor_run_budget_install` is FAIL-CLOSED on a parallel
// run (returns None, arming nothing) — a suspended portal can never park a
// gang because a budgeted run never has one. FETCH_ALL first runs
// (count == 0) install no budget and keep C's parallel eligibility; a
// count-0 run never suspends mid-gang (it runs to exhaustion inside one
// ExecutorRun). The unit pins live in `crate::tests` (WS-AI wave-9 region).

use std::sync::atomic::{AtomicU8, Ordering::Relaxed};

/// `PGRUST_LANE_V2_CURSORS` (default ON since the SE12-GATES flip; R-KNOBS
/// registry spelling): the forward-pull cursor gate. ON = count-limited
/// forward SELECT runs carry a per-run emission budget for the cursor
/// machinery to read, and scrollable portals arm the wave-10 cursor store.
/// Explicit `=0`/`off` is the permanent kill switch and restores the legacy
/// run path byte-identically (rowmode FLIP-1/FLIP-2 idiom verbatim; flips
/// never delete knobs). AtomicU8 + `_set_for_tests` idiom (heapfeed
/// precedent, batch_source.rs).
static CURSORS: AtomicU8 = AtomicU8::new(0);

pub(crate) fn cursors_v2_enabled() -> bool {
    match CURSORS.load(Relaxed) {
        1 => false,
        2 => true,
        _ => {
            // SE12-GATES CURSORS FLIP (flip-ladder board; notes/se12-gates.md):
            // default ON — the SE11 B1 blocker (+18.15% instr forloop cadence)
            // was cleared to FLAT by the se/b1fix NO_SCROLL C-parity fix
            // (notes/se-b1fix.md §5: B1 −0.01% instr, analytics-bank store leg −6.66%,
            // point-pair invisibility ±0.01%), and the flip letter battery
            // re-proved every bar at the flipped tip. Only this default read
            // changes — the explicit `=0`/`off` spelling is the permanent
            // kill switch (restores legacy bytes AND ticks).
            let on = !matches!(
                std::env::var("PGRUST_LANE_V2_CURSORS").as_deref(),
                Ok("0") | Ok("off")
            );
            CURSORS.store(if on { 2 } else { 1 }, Relaxed);
            on
        }
    }
}

/// Same-process A/B lever for the unit corpus (`crate::tests`).
#[cfg(test)]
pub(crate) fn cursors_set_for_tests(on: bool) {
    CURSORS.store(if on { 2 } else { 1 }, Relaxed);
}

/// inc-1b ADMISSION CLASSIFIER (contract §3 "Admits/Refuses", the NAMED
/// refusal taxonomy): given the run-seam-visible portal shape, `None` =
/// this budgeted run may carry the cursor machinery (Forward direction,
/// no SCROLL demand — declared or free-upgraded, both arrive as the
/// REWIND|BACKWARD top eflags from PortalStart, pquery.rs:390-395; serial);
/// `Some(reason)` = the WHOLE run refuses to Volcano exactly as today,
/// under the named `ShapeClass::Cursor` class. Seam-visibility honesty
/// (registry doc, stats.rs): `cursor-with-hold` / `cursor-persist-holdable`
/// are RESERVED classes — holdability is portal-level state invisible below
/// `executor_run`, and today's structural posture already satisfies design
/// §5 (persist = count-0 run ⇒ never budgeted; pre-COMMIT FETCHes resume
/// forward). `cursor-plan-refused` ticks at the settle seam (inc-1b park
/// walker), not here — the plan's engagement is knowable only after pulls.
pub(crate) fn cursor_admission_refusal(
    forward: bool,
    // Kept in the signature (SUNSET): the eflags arm is gone but the seam
    // passes the value and the taxonomy pins name it; a future arm reading
    // it again must go through R-VOCAB.
    _top_eflags: i32,
    use_parallel_mode: bool,
) -> Option<super::stats::RefuseReason> {
    if !forward {
        // The direction demand is the more specific class (backward runs
        // reach the seam only through scroll-capable portals).
        return Some(super::stats::RefuseReason::CursorBackward);
    }
    // SUNSET (SE10-GATES item 1, the audited shrink; wave-10 contract §3.4):
    // the inc-1b `cursor-scroll` eflags arm is REMOVED — knob-ON, every
    // SCROLL/HOLD portal is store-served with a lane-admitted fill, so the
    // REWIND|BACKWARD|MARK top eflags a run still carries here belong to a
    // CURRENT-OF-eligible portal's ROW-CHAIN fill (D-CA-2's one fence).
    // Those runs now take the budget: the batch dispatch and the per-pull
    // hooks both refuse on the scan's `batch_allowed=false` (init-time
    // eflags), nothing lane-stages, and the settle walker's roll-up ticks
    // `cursor-plan-refused` — the RETAINED-redefined class ("the plan's own
    // refusal ticks AND the cursor is still served"). Removing this arm is
    // what makes the allowlist-row removal legal (the reason no longer
    // fires — proven by the three-arm matrix at the wiring tip).
    if use_parallel_mode {
        // FAIL-CLOSED serial-law arm: `use_parallel_mode` is false for
        // every count-limited run by the ported execmain.rs:978 gate; if
        // that gate ever regressed this refuses (no cursor machinery over
        // a gang, ever) rather than asserting — the corpus batteries would
        // read the missing engagement loudly instead of a debug-only
        // crash. Deliberately NOT a named cursor class: it is the §3
        // serial-law pin, not an admission taxonomy row.
        return Some(super::stats::RefuseReason::ParallelGate);
    }
    None
}

/// Test/corpus face of the classifier: the NAMED refusal-class string
/// (`RefuseReason::name()`, the registry vocabulary) or None on admit —
/// `RefuseReason` itself is lanev2-private (pub(super)).
#[cfg(test)]
pub(crate) fn cursor_admission_refusal_name(
    forward: bool,
    top_eflags: i32,
    use_parallel_mode: bool,
) -> Option<&'static str> {
    cursor_admission_refusal(forward, top_eflags, use_parallel_mode).map(|r| r.name())
}

/// The run seam's install half (`execute_plan`, once per ExecutorRun):
/// computes the value of `es_cursor_run_budget` for this run —
/// `Some(count)` iff this run is a knob-ON, count-limited, cursor-ADMITTED
/// (forward, non-scroll, serial) SELECT: the §3.1 count-exact suspension
/// shape. The caller writes the result to the estate UNCONDITIONALLY (a
/// None overwrites any stale value, so an estate re-run after an error can
/// never inherit a budget). Gate order is the cost order: `count == 0`
/// (every simple-protocol run) answers with one register test before the
/// knob cell is ever loaded; the named-refusal classifier and its ticks run
/// only knob-ON (knob-OFF accounting byte-identical by construction).
pub(crate) fn cursor_run_budget_install(
    is_select: bool,
    forward: bool,
    count: u64,
    use_parallel_mode: bool,
    _top_eflags: i32,
) -> Option<u64> {
    if count == 0 || !is_select {
        return None;
    }
    if !cursors_v2_enabled() {
        return None;
    }
    if let Some(reason) = cursor_admission_refusal(forward, _top_eflags, use_parallel_mode) {
        // Once per budgeted run (never per tuple); `tick_refused` is a
        // no-op unless accounting is armed.
        super::stats::tick_refused(super::stats::ShapeClass::Cursor, reason);
        return None;
    }
    Some(count)
}

/// The read half for lane-side consumers (the §2 park/settle glue, inc-1b):
/// the emission budget of the current run, None outside a budgeted one.
/// STALENESS LAW (inc-1a fixer ledger item 8): meaningful only between
/// `execute_plan` entry and return — post-run readers see the LAST run's
/// budget (NoMovement runs never overwrite). EPQ LAW (inc-1a §5 note): a
/// consumer must gate on `es_epq_active` — an EPQ recheck drive shares the
/// estate and the budget belongs to the OUTER run.
#[allow(dead_code)] // run-seam gates read the estate field directly; unit-pin face
pub(crate) fn cursor_run_budget(estate: &::executils::EStateData<'_>) -> Option<u64> {
    estate.es_cursor_run_budget
}

// --- WS-AI wave-9.5 (cursors inc-1b): the §2 park shape -------------------------
//
// Design (lane-cursors.md §2, DECIDED): at suspension, SETTLE everything;
// repossess on resume. The budget-N emit sink (inc-1a) is the settle POINT:
// when a budgeted run returns to the protocol layer, `cursor_run_park`
// (below the drive loop in `execute_plan`) walks the plan tree and settles
// every lane-staged scan claim through the ledgered claim-release chain —
// `seq_scan_cursor_settle` → `table_scan_end_claim_release` →
// `heap_end_claim_release` — with the reposition point recorded
// node-resident (`SeqScanState::lane_park`). R3 ZERO-PINS-AT-SETTLE is
// debug-asserted at every settled claim. The next run's entry
// (`cursor_park_resume`) restages the suspended page batch and restores the
// consume cursor BEFORE the first pull touches staged state — re-entry costs
// the emit-face hop + one buffer re-lookup, never translate/verdict/compile
// (the §8 explicit-FETCH guard's bar).
//
// WHAT CAN ACTUALLY BE STAGED AT SUSPENSION (audited at inc-1b, recorded in
// notes/se-wave9-ai.md): `HeapBatchSource` claims are DRAIN-SCOPED (both
// construction sites settle via `end_claim` inside one exec_proc_node call
// — they can never span a suspension); breaker pipelines (agg/sort/join
// builds) consume their input whole at first pull, so post-build suspension
// holds zero scan claims; the per-pull staged shapes are the standalone
// scan pipeline's page batch (heap arm: ONE pinned page, rs_cbuf — the
// claim this walker settles; production standalone admission is
// pgrcolumnar-only today, whose staged windows are Arc/mmap-backed decode
// scratch under R4 — no bufmgr pins, nothing to release, node-resident by
// design §1). Ledger: a budgeted run is DOP-1 caller-as-worker (§3) —
// pool-invisible, zero ledger width to retire.
//
// STATED DIVERGENCE carried (design §2): C keeps rs_cbuf pinned across
// FETCHes; the settled lane claim does not. Pin-visible/pgstat only, never
// output bytes; priced by the §8 cadence pairs. The VOLCANO scan's own
// cross-FETCH pin is C parity and is deliberately NOT touched (the walker
// settles only lane-STAGED batches, `lane_n > 0`).

/// The run seam's settle half: walk + settle + the `cursor-plan-refused`
/// roll-up tick. Returns true iff anything parked (the caller then sets
/// `es_lane_cursor_parked`). Runs only on budgeted runs (caller gates on
/// `es_cursor_run_budget.is_some()`); refuses under EPQ (the budget belongs
/// to the outer run — inc-1a §5 EPQ law, pinned in units).
pub(crate) fn cursor_run_park<'mcx>(
    node: &mut crate::procnode::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if estate.es_epq_active {
        return Ok(false);
    }
    let mut w = ParkWalk { engaged: false, parked: false };
    w.settle(node, estate)?;
    if !w.engaged {
        // The budgeted run's top plan carried no (scan-class) lane
        // engagement: the whole portal rides Volcano exactly as today.
        // Once per budgeted run; no-op unless accounting armed. Detection
        // breadth = scan classes this increment (see the registry doc).
        super::stats::tick_refused(
            super::stats::ShapeClass::Cursor,
            super::stats::RefuseReason::CursorPlanRefused,
        );
    }
    Ok(w.parked)
}

/// Repossession (design §2 "on resume, the source repositions"): restage
/// every parked scan's suspended page batch and restore its consume cursor.
/// Called at `execute_plan` entry when the previous budgeted run parked
/// (`es_lane_cursor_parked`); count-0 follow-ups (FETCH ALL) resume through
/// the same walk — the flag, not the budget, carries the obligation.
pub(crate) fn cursor_park_resume<'mcx>(
    node: &mut crate::procnode::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    debug_assert!(!estate.es_epq_active, "cursor resume inside an EPQ drive");
    resume_walk(node, estate)
}

struct ParkWalk {
    engaged: bool,
    parked: bool,
}

impl ParkWalk {
    /// Settle-walk over the plan tree (the `exec_shutdown_node` recursion
    /// arms). Leaf behavior: a SeqScan with a lane-STAGED page batch
    /// settles through the claim-release chain; everything else is a
    /// no-op (their staged state is either drain-scoped or pin-free — the
    /// module doc's audit). Fallible ONLY through the slot-materialize
    /// hygiene (allocation); the release half never fails.
    fn settle<'mcx>(
        &mut self,
        node: &mut crate::procnode::PlanStateNode<'mcx>,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<()> {
        use crate::procnode::PlanStateNode as P;
        match node {
            P::SeqScan(ss) => {
                if ss.lane_verdict() == Some(true) || ss.cb_standalone_verdict() == Some(true) {
                    self.engaged = true;
                }
                if ::nodeseqscan::seq_scan_cursor_park_pending(ss) {
                    // Slot hygiene (BRIN-BUDGET P1 fix, was the
                    // `end_claim_clear_slot` clear): a node ABOVE the seam
                    // can still hold the suspended scan's last emitted slot
                    // across the park — a lane join probe keeps
                    // `ecxt_outertuple` pointing at the scan/result slot
                    // for the whole inner iteration (the receiver-copied-out
                    // argument covered only the standalone pipeline). So the
                    // emitted slots MATERIALIZE (C's ExecMaterializeSlot
                    // contract: values survive the buffer going away) rather
                    // than clear. Order is load-bearing: the RESULT slot's
                    // virtual byref datums may alias the staged page with no
                    // pin of their own, so it copies out FIRST, while the
                    // page is still pinned; the scan slot's materialize then
                    // drops the slot's own buffer pin (R3 zero-pins across
                    // the suspension preserved), and the settle call below
                    // releases the claim pin. Park cadence only — never
                    // per-tuple, unreachable budgets-off.
                    let mcx = estate.es_query_cxt;
                    if let Some(p) = ss.ss.ps_ProjInfo.as_ref() {
                        let result_slot = p.pi_result_slot;
                        let slot = estate.slot_mut(result_slot);
                        if !slot.base().is_empty() {
                            ::exectuples::exec_materialize_slot(slot, mcx)?;
                        }
                    }
                    let slot = estate.slot_mut(ss.ss.ss_ScanTupleSlot);
                    if !slot.base().is_empty() {
                        ::exectuples::exec_materialize_slot(slot, mcx)?;
                    }
                    let parked = ::nodeseqscan::seq_scan_cursor_settle(ss);
                    debug_assert!(parked, "park-pending probe and settle disagree");
                    self.parked = true;
                    self.engaged = true;
                }
            }
            P::Instrumented(w) => self.settle(&mut w.inner, estate)?,
            P::Result(rs) => {
                if let Some(outer) = rs.outer.as_deref_mut() {
                    self.settle(outer, estate)?;
                }
            }
            P::ProjectSet(ps) => self.settle(&mut ps.outer, estate)?,
            P::RecursiveUnion(ru) => {
                let ru = &mut **ru;
                self.settle(&mut ru.outer, estate)?;
                self.settle(&mut ru.inner, estate)?;
            }
            P::Agg(aps) => self.settle(&mut aps.outer, estate)?,
            P::WindowAgg(w) => self.settle(&mut w.outer, estate)?,
            P::Sort(s) => self.settle(&mut s.outer, estate)?,
            P::IncrementalSort(s) => self.settle(&mut s.outer, estate)?,
            P::Material(m) => self.settle(&mut m.outer, estate)?,
            P::Memoize(m) => self.settle(&mut m.outer, estate)?,
            P::Unique(u) => self.settle(&mut u.outer, estate)?,
            P::Group(g) => self.settle(&mut g.outer, estate)?,
            P::Limit(l) => self.settle(&mut l.outer, estate)?,
            P::LockRows(l) => self.settle(&mut l.outer, estate)?,
            P::ModifyTable(mps) => self.settle(&mut mps.subplan, estate)?,
            P::Append(a) => {
                for sub in a.substates.iter_mut() {
                    self.settle(sub, estate)?;
                }
            }
            P::MergeAppend(m) => {
                for sub in m.substates.iter_mut() {
                    self.settle(sub, estate)?;
                }
            }
            P::SubqueryScan(s) => self.settle(&mut s.subplan, estate)?,
            P::SetOp(s) => {
                let s = &mut **s;
                self.settle(&mut s.outer, estate)?;
                self.settle(&mut s.inner, estate)?;
            }
            P::NestLoop(nl) => {
                self.settle(&mut nl.outer, estate)?;
                self.settle(&mut nl.inner, estate)?;
            }
            P::HashJoin(hj) => {
                let hj = &mut **hj;
                self.settle(&mut hj.outer, estate)?;
                self.settle(&mut hj.hash.child, estate)?;
            }
            P::MergeJoin(mj) => {
                let mj = &mut **mj;
                self.settle(&mut mj.outer, estate)?;
                self.settle(&mut mj.inner, estate)?;
            }
            P::Gather(g) => self.settle(&mut g.outer, estate)?,
            P::GatherMerge(gm) => self.settle(&mut gm.outer, estate)?,
            P::BitmapHeapScan(b) => self.settle(&mut b.bitmapqual, estate)?,
            P::BitmapAnd(bc) | P::BitmapOr(bc) => {
                for sub in bc.substates.iter_mut() {
                    self.settle(sub, estate)?;
                }
            }
            // Leaves with no lane-staged claim state (drain-scoped or
            // pin-free; a shape this walk misses settles nothing — the
            // C-parity pin-held posture, never a correctness change).
            P::SampleScan(_)
            | P::FunctionScan(_)
            | P::TableFuncScan(_)
            | P::ValuesScan(_)
            | P::ForeignScan(_)
            | P::CteScan(_)
            | P::WorkTableScan(_)
            | P::NamedTuplestoreScan(_)
            | P::IndexScan(_)
            | P::TidScan(_)
            | P::TidRangeScan(_)
            | P::IndexOnlyScan(_)
            | P::BitmapIndexScan(_) => {}
        }
        Ok(())
    }
}

/// Resume-walk twin (same arms; fallible — restaging reads pages).
fn resume_walk<'mcx>(
    node: &mut crate::procnode::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    use crate::procnode::PlanStateNode as P;
    match node {
        P::SeqScan(ss) => {
            ::nodeseqscan::seq_scan_cursor_resume(ss, estate)?;
            Ok(())
        }
        P::Instrumented(w) => resume_walk(&mut w.inner, estate),
        P::Result(rs) => match rs.outer.as_deref_mut() {
            Some(outer) => resume_walk(outer, estate),
            None => Ok(()),
        },
        P::ProjectSet(ps) => resume_walk(&mut ps.outer, estate),
        P::RecursiveUnion(ru) => {
            let ru = &mut **ru;
            resume_walk(&mut ru.outer, estate)?;
            resume_walk(&mut ru.inner, estate)
        }
        P::Agg(aps) => resume_walk(&mut aps.outer, estate),
        P::WindowAgg(w) => resume_walk(&mut w.outer, estate),
        P::Sort(s) => resume_walk(&mut s.outer, estate),
        P::IncrementalSort(s) => resume_walk(&mut s.outer, estate),
        P::Material(m) => resume_walk(&mut m.outer, estate),
        P::Memoize(m) => resume_walk(&mut m.outer, estate),
        P::Unique(u) => resume_walk(&mut u.outer, estate),
        P::Group(g) => resume_walk(&mut g.outer, estate),
        P::Limit(l) => resume_walk(&mut l.outer, estate),
        P::LockRows(l) => resume_walk(&mut l.outer, estate),
        P::ModifyTable(mps) => resume_walk(&mut mps.subplan, estate),
        P::Append(a) => {
            for sub in a.substates.iter_mut() {
                resume_walk(sub, estate)?;
            }
            Ok(())
        }
        P::MergeAppend(m) => {
            for sub in m.substates.iter_mut() {
                resume_walk(sub, estate)?;
            }
            Ok(())
        }
        P::SubqueryScan(s) => resume_walk(&mut s.subplan, estate),
        P::SetOp(s) => {
            let s = &mut **s;
            resume_walk(&mut s.outer, estate)?;
            resume_walk(&mut s.inner, estate)
        }
        P::NestLoop(nl) => {
            resume_walk(&mut nl.outer, estate)?;
            resume_walk(&mut nl.inner, estate)
        }
        P::HashJoin(hj) => {
            let hj = &mut **hj;
            resume_walk(&mut hj.outer, estate)?;
            resume_walk(&mut hj.hash.child, estate)
        }
        P::MergeJoin(mj) => {
            let mj = &mut **mj;
            resume_walk(&mut mj.outer, estate)?;
            resume_walk(&mut mj.inner, estate)
        }
        P::Gather(g) => resume_walk(&mut g.outer, estate),
        P::GatherMerge(gm) => resume_walk(&mut gm.outer, estate),
        P::BitmapHeapScan(b) => resume_walk(&mut b.bitmapqual, estate),
        P::BitmapAnd(bc) | P::BitmapOr(bc) => {
            for sub in bc.substates.iter_mut() {
                resume_walk(sub, estate)?;
            }
            Ok(())
        }
        P::SampleScan(_)
        | P::FunctionScan(_)
        | P::TableFuncScan(_)
        | P::ValuesScan(_)
        | P::ForeignScan(_)
        | P::CteScan(_)
        | P::WorkTableScan(_)
        | P::NamedTuplestoreScan(_)
        | P::IndexScan(_)
        | P::TidScan(_)
        | P::TidRangeScan(_)
        | P::IndexOnlyScan(_)
        | P::BitmapIndexScan(_) => Ok(()),
    }
}

// --- end WS-AI wave-9 ----------------------------------------------------------

// --- WS-AJ wave-9.5 (SPI Stage-A seam, `se/spi-stage-a`; lane-spi.md §1/§3) -----
//
// Stage A of docs/design/lane-spi.md: `_SPI_pquery` runs a statement as
// executor_start → ONE `executor_run(Forward, tcount, dest)`
// (spi/src/execute.rs:562) → executor_finish/end — the tcount limit is the
// SAME count-exact stop the cursor budget carries. TWO producers reach this
// seam with a count-limited `CommandDest::Spi` run (review re-baseline,
// notes/se-spi-stage-a.md §8 — the original STOP-ONLY premise named only
// the first and was falsified by live evidence):
//   1. `_SPI_pquery` itself — STOP-then-END cadence (executor_finish/end
//      follow immediately; the spi_inc1_aj_w9 unit pins freeze it).
//   2. `SPI_cursor_fetch` / `SPI_scroll_cursor_fetch`
//      (spi/src/cursor.rs:203) → `PortalRunFetch` → `PortalRunSelect`,
//      which threads the per-fetch SPI receiver into `executor_run`
//      (pquery/src/lib.rs:594-630) on the SAME QueryDesc/estate — every
//      plpgsql FOR loop (exec_for_query: fetch 10 then 50 per call) is a
//      stream of count-limited Spi-dest runs that RESUME.
// The seam therefore rides WS-AI's budget-sink SHAPE — an estate-resident
// per-run budget (`es_spi_run_budget`, its own field so the two taxonomies
// never cross) installed at the `execute_plan` run seam — settles
// lane-staged claims at the count-limited stop with the SAME ParkWalk
// release chain, AND arms the SAME resume signal the cursor walker owns
// (`es_lane_cursor_parked` → the entry-side `cursor_park_resume` walk):
// producer 2's next fetch repossesses exactly like a cursor FETCH; for
// producer 1 the armed flag is estate-resident dead state torn down by the
// immediately-following ExecutorEnd (the same parked-then-close path a
// partially-fetched cursor already rides under WS-AI).
//
// spi.c PROVENANCE, BINDING (the wave-9.5 review's attack list):
//   * `SPI_processed` = `es_processed` read after the single run
//     (execute.rs:563) — the budget changes NOTHING about the drive loop's
//     count enforcement (C's own ExecutePlan `number_tuples` check), so the
//     count is byte-preserved BY CONSTRUCTION (pins: tcount-exact stop,
//     tcount=0 completeness, tcount>rows saturation).
//   * tuptable lifetime / `SPI_freetuptable` timing: the tuptable is built
//     by the SpiPrintTup receiver ABOVE this seam and owned by the SPI
//     connection stack; nothing here touches it.
//   * connect/finish nesting: each nested SPI statement runs on its OWN
//     QueryDesc/estate (fresh `es_spi_run_budget` written at ITS run entry;
//     the unconditional-overwrite idiom makes the field per-run by
//     construction), so nesting cannot leak a budget across levels.
//   * rewind / EXCEPTION-block unwind mid-SPI: an error unwind abandons the
//     estate without re-entry; the budget is estate-resident dead state
//     (no guard needed — the WS-AI unwind argument verbatim), and staged
//     lane claims release through the normal executor/resowner teardown.
//   * INVARIANT 5 (teardown ordering, post-t26 map per notes/se-wave9-aj.md
//     §11.3): the settle retires lane-staged claims at the stop point,
//     BEFORE executor_finish/end return control toward the three
//     plancache release points (per-eval put-back on invalidation-replan,
//     `free_function_plans`, the `on_proc_exit` release path).
//
// INVARIANT 1 (never route detected-simple): the plpgsql simple path
// evaluates via ExprState off the function-lifetime plan cache with no
// ExecutorStart/Run — it can never reach this seam; nothing here can demote
// it (the rate + Ir/call guard pair is fleet evidence — named obligation
// `aj-ir-pair` in the worklog, the ai-ir-pair cadence).
//
// INVARIANT 3 (thread-affinity LAW), seam-side half: a budgeted SPI run is
// DOP-1 caller-as-worker BY CONSTRUCTION — count-limited runs never carry a
// gang (the ported execmain.rs use_parallel_mode gate), and the classifier
// FAIL-CLOSED refuses a parallel run (ParallelGate) rather than asserting.
// The plpgsql-side owner of the law is the function-lifetime `EXPR_PLANS`
// SimpleState machinery (the RV3 re-baseline, se-wave9-aj.md §11.3) — a
// frozen surface this increment; the owner-side debug assertion lands with
// t26 absorption (named obligation in the worklog).

use std::sync::atomic::AtomicU8 as SpiAtomicU8;

/// `PGRUST_LANE_V2_SPI` (default OFF; R-KNOBS registry spelling): the SPI
/// Stage-A gate. OFF = the run seam installs no SPI budget and every byte
/// of the run path behaves as today (the install call short-circuits on
/// `count == 0` / non-SELECT / non-SPI dest before this cell is ever
/// loaded). ON = tcount-limited SPI-statement runs carry a per-run emission
/// budget and settle lane-staged claims at the count-limited stop.
/// AtomicU8 + `_set_for_tests` idiom (heapfeed precedent; the CURSORS cell
/// above).
static SPI_LANE: SpiAtomicU8 = SpiAtomicU8::new(0);

pub(crate) fn spi_v2_enabled() -> bool {
    match SPI_LANE.load(Relaxed) {
        1 => false,
        2 => true,
        _ => {
            let on = matches!(std::env::var("PGRUST_LANE_V2_SPI").as_deref(), Ok("1") | Ok("on"));
            SPI_LANE.store(if on { 2 } else { 1 }, Relaxed);
            on
        }
    }
}

/// Same-process A/B lever for the unit corpus (`crate::tests`).
#[cfg(test)]
pub(crate) fn spi_set_for_tests(on: bool) {
    SPI_LANE.store(if on { 2 } else { 1 }, Relaxed);
}

/// Stage-A ADMISSION CLASSIFIER (the NAMED refusal taxonomy,
/// `ShapeClass::Spi`): given the run-seam-visible statement shape, `None` =
/// this budgeted run may carry the SPI count-seam machinery (forward, no
/// random-access eflags demand, serial); `Some(reason)` = the WHOLE
/// statement refuses to Volcano exactly as today (refusal-not-error).
/// REACHABILITY (re-baselined by the backward-execution wave; the wave-9.5
/// record — notes/se-spi-stage-a.md §8 — described the pre-B1/B2 world):
///   * `Backward` arm DELETED (wave B11): a backward demand cannot reach a
///     budgeted run anymore — at defaults the portal store serves every
///     backward fetch above this seam (SE13 flip), and any kill-switch
///     backward run dies 0A000 at the forward-only run seam (deletion-prep
///     B1) immediately after budget install; the seam error is the single
///     authority. Allowlist row `spi backward` retired with the arm.
///   * `ScrollMark` arm KEPT but its wave-9.5 producer is GONE: PortalStart
///     no longer passes REWIND|BACKWARD for auto-SCROLL portals (B2 deleted
///     the eflags arm), so the once-per-fetch plpgsql FOR-loop tick cadence
///     (the aj-allowlist-honesty record) STOPS. The arm stays as the
///     defensive random-access-eflags fence (MARK-narrowed vocabulary);
///     its allowlist row stays legal-but-quiet.
///   * `ParallelGate` remains the FAIL-CLOSED serial-law pin: count-limited
///     runs are serial by the ported use_parallel_mode gate; the arm
///     refuses loudly (corpus-visible) if that gate ever regressed, and
///     keeps NO allowlist row.
pub(crate) fn spi_admission_refusal(
    // Kept in the signature (SUNSET, the cursor classifier's _top_eflags
    // precedent): the backward arm is gone but the seam passes the value
    // and the taxonomy pins name it; a future arm reading it again must go
    // through R-VOCAB.
    _forward: bool,
    top_eflags: i32,
    use_parallel_mode: bool,
) -> Option<super::stats::RefuseReason> {
    use ::types_slot::{EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, EXEC_FLAG_REWIND};
    if top_eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK) != 0 {
        return Some(super::stats::RefuseReason::ScrollMark);
    }
    if use_parallel_mode {
        return Some(super::stats::RefuseReason::ParallelGate);
    }
    None
}

/// Test/corpus face of the classifier: the refusal-reason string
/// (`RefuseReason::name()`, the registry vocabulary) or None on admit.
#[cfg(test)]
pub(crate) fn spi_admission_refusal_name(
    forward: bool,
    top_eflags: i32,
    use_parallel_mode: bool,
) -> Option<&'static str> {
    spi_admission_refusal(forward, top_eflags, use_parallel_mode).map(|r| r.name())
}

/// The run seam's install half (`execute_plan`, once per ExecutorRun):
/// computes the value of `es_spi_run_budget` for this run — `Some(tcount)`
/// iff this run is a knob-ON, tcount-limited, SPI-ADMITTED (forward,
/// non-random-access, serial) SELECT driven into the SPI tuptable receiver
/// (`CommandDest::Spi`): `_SPI_pquery`'s count-exact STOP shape or a
/// NO_SCROLL SPI portal fetch (the RESUMABLE producer — module doc; the
/// settle half arms the resume signal so both cadences are sound). The caller
/// writes the result to the estate UNCONDITIONALLY (a None overwrites any
/// stale value; an estate re-run after an error can never inherit a
/// budget). Gate order is the cost order: `count == 0` (every SPI_execute
/// default-tcount statement and every non-SPI simple-protocol run) answers
/// with one register test before the dest compare or the knob cell load;
/// the classifier and its ticks run only knob-ON (knob-OFF accounting
/// byte-identical by construction).
pub(crate) fn spi_run_budget_install(
    is_select: bool,
    spi_dest: bool,
    forward: bool,
    count: u64,
    use_parallel_mode: bool,
    top_eflags: i32,
) -> Option<u64> {
    if count == 0 || !is_select || !spi_dest {
        return None;
    }
    if !spi_v2_enabled() {
        return None;
    }
    if let Some(reason) = spi_admission_refusal(forward, top_eflags, use_parallel_mode) {
        // Once per budgeted run (never per tuple); no-op unless accounting
        // is armed.
        super::stats::tick_refused(super::stats::ShapeClass::Spi, reason);
        return None;
    }
    // INV3 seam-side half (lane-spi.md invariant 3): an admitted budgeted
    // SPI run is DOP-1 caller-as-worker — the classifier just refused the
    // parallel arm, so this documents the admitted-world law.
    debug_assert!(!use_parallel_mode, "budgeted SPI run over a gang");
    Some(count)
}

/// The run seam's settle half (below the `execute_plan` drive loop, gated
/// on `es_spi_run_budget.is_some()`): retire every lane-staged scan claim
/// through the SAME ledgered claim-release chain the cursor park walker
/// owns (`seq_scan_cursor_settle` → `table_scan_end_claim_release` →
/// `heap_end_claim_release`; R3 zero-pins-at-settle debug-asserted at every
/// settled claim), then tick the `spi-plan-refused` roll-up when the plan
/// carried no lane engagement. Returns true iff anything parked — the
/// caller then arms `es_lane_cursor_parked`, the SAME resume signal the
/// WS-AI walker owns (review re-baseline, notes/se-spi-stage-a.md §8:
/// portal-fetch Spi-dest runs RESUME on the same QueryDesc/estate, so
/// dropping the parked bit would resume an un-inited scan the moment a
/// budgeted SPI run carries a lane-staged batch). For a true `_SPI_pquery`
/// run the armed flag is dead state torn down by the immediately-following
/// ExecutorEnd (the parked-then-close path cursors already ride). Settled
/// claims retire BEFORE control returns toward the plancache release
/// points (INVARIANT 5). EPQ law shared with cursors: an EPQ recheck drive
/// never enters `execute_plan`, and the walk refuses under
/// `es_epq_active`.
pub(crate) fn spi_run_settle<'mcx>(
    node: &mut crate::procnode::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    if estate.es_epq_active {
        return Ok(false);
    }
    let mut w = ParkWalk { engaged: false, parked: false };
    w.settle(node, estate)?;
    if !w.engaged {
        // The budgeted SPI statement's plan carried no (scan-class) lane
        // engagement: the whole statement rides Volcano exactly as today.
        // Once per budgeted run; no-op unless accounting armed. Detection
        // breadth = the WS-AI inc-1b scan-class detector.
        super::stats::tick_refused(
            super::stats::ShapeClass::Spi,
            super::stats::RefuseReason::SpiPlanRefused,
        );
    }
    Ok(w.parked)
}

// --- end WS-AJ wave-9.5 ----------------------------------------------------------

// --- WS-CB wave-10 (cursors inc-2: the batch store fill; contract §2.1, band 95001+) ---
//
// The ratified fill shape: SCROLL/WITH-HOLD cursors are served from a
// portal-boundary tuplestore, and the store FILL is a lane-engine batch
// producer from day one — batches flow into the store WITHOUT the
// capacity-one per-row pull ceremony. `TuplestoreBatchSink` is the
// push-mode generalization the WS-AI enforcement-honesty note (above)
// names: when the emit face is driven as a push sink (capacity > 1), the
// per-accept budget decrement moves INTO the sink's `accept`, and
// `es_cursor_run_budget` is its source of truth.
//
// BYTE-IDENTITY BY IDENTITY, not reimplementation: `accept` hands each
// produced slot to the SAME `DestReceiver` the row-chain fill uses (the
// tuplestore receiver — `tuplestore::hold::puttupleslot` per row,
// detoast-on-append when armed; tstoreReceiver.c semantics), and carries
// the drive loop's own SELECT accounting (`es_processed += 1`). The store
// therefore receives the identical row stream in the identical order under
// either fill engine — the §2.3 fetch-invisibility gate's substrate.
//
// Park shape (§2.1, load-bearing): budget exhaustion mid-batch returns
// `SinkFeed::Full` ⇒ the operator saves its position NODE-RESIDENT and
// returns `OpStatus::Paused`; the run returns through `execute_plan`'s
// EXISTING wave-9.5 settle point (`cursor_run_park` — claims retire through
// the ledgered claim-release chain, R3 zero-pins-at-settle), and the next
// run's entry repossesses (`cursor_park_resume`). No new park machinery:
// the sink's pause is indistinguishable from the row-chain pause at the
// settle seam (batch_source.rs re-audit finding, worklog §1).
//
// EPQ + staleness pins (§2.1; WS-AI worklog §5 + §6 item 8, due here):
// the dispatch refuses under `es_epq_active` (an EPQ recheck drive inside
// a budgeted run must not read the outer run's budget) and the sink
// debug-asserts it per accept; the budget field is meaningful ONLY between
// `execute_plan` entry and return (the sink runs strictly inside that
// window; NoMovement runs never overwrite).

/// The §7.3 knob face for the PORTAL layer (WS-CA gates store arming on
/// it): pquery must not link lanev2 internals — this is the pub face,
/// re-exported at the execmain crate root (worklog EX-CB-1). Same cell as
/// `cursors_v2_enabled` (inc-2 rides `PGRUST_LANE_V2_CURSORS`, no new
/// knob — contract §7.3).
pub fn cursor_store_fill_enabled() -> bool {
    cursors_v2_enabled()
}

/// SEAM-WIRING (SE10-GATES item 1): the SAME-PROCESS A/B lever for the
/// portal-layer unit batteries (pquery/portalcmds band-94001 pins run in
/// dependent crates, so this cannot be `cfg(test)` — the retired portalmem
/// `cursor_store_set_for_tests` precedent). Writes THE single knob cell
/// (`CURSORS`), so the portal face and the run-seam budget classifier can
/// never skew — the CB review F1(a) hazard closed by construction.
#[doc(hidden)]
pub fn cursor_store_fill_set_for_tests(on: bool) {
    CURSORS.store(if on { 2 } else { 1 }, Relaxed);
}

/// §6 deletion-clock staging: set once by WS-CA when a cursor store is
/// armed in this process (SEAM-WIRING: now LIVE — pquery's PortalStart
/// calls the `cursor_store_armed_note` seam on every arming decision).
/// Arms the run seam's forward-only debug assert (a store-armed KNOB-ON
/// world never legally drives the executor backward); before any store
/// exists (knob-OFF worlds; processes that never arm) the assert is inert
/// and only the evidence counter ticks.
static STORE_ARMED: AtomicU8 = AtomicU8::new(0);

pub fn cursor_store_armed_note() {
    STORE_ARMED.store(1, Relaxed);
}

pub(crate) fn cursor_store_ever_armed() -> bool {
    STORE_ARMED.load(Relaxed) != 0
}

/// §6 staging (a): release-mode evidence counter — every
/// `BackwardScanDirection` drive reaching `execute_plan`. The post-flip
/// physical-deletion bake reads this at zero across all corpora
/// (`counter\trun-seam-backward` dump line, stats.rs wave-10 block).
static BACKWARD_RUNS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub(crate) fn run_seam_backward_evidence() {
    BACKWARD_RUNS.fetch_add(1, Relaxed);
    // SEAM-WIRING F3 rework: the assert is scoped to the KNOB-ON world.
    // Production processes fix the env at start, so armed ⇒ knob-ON there
    // and the conjunct is free; test processes flip the knob per-test with
    // a never-cleared armed static — a knob-OFF backward drive after some
    // earlier test armed a store is legal (that test's knob-ON world ended
    // with its `_set_for_tests(false)` restore), and asserting on it was
    // the F3 order hazard.
    debug_assert!(
        !(cursor_store_ever_armed() && cursors_v2_enabled()),
        "forward-only run seam (§6): backward ExecutorRun after a cursor store was armed \
         — every SCROLL/HOLD portal is store-served knob-ON, so no backward drive may \
         reach the executor core"
    );
}

pub(crate) fn run_seam_backward_evidence_count() -> u64 {
    BACKWARD_RUNS.load(Relaxed)
}

// R1a (night/r1a-impl, §2a reason-41 completion): the §3.3
// `cursor_fill_tid_capture_refused` tick face is RETIRED. It accounted for
// fill_portal_store_to routing a CURRENT-OF-eligible fill onto the row
// chain so a POST-run `ss_ScanTupleSlot` read could capture identity (the
// deleted arm B). Every eligible fill now captures IN-RUN (batch sink /
// capture row loop), so that routing — and its accounting — no longer
// exists. `RefuseReason::CursorCurrentOfTidCapture` (41) stays as an
// append-only TOMBSTONE in stats.rs (NEVER-TICKING; the `cursor-scroll`
// SUNSET precedent), and its `scripts/lane-gates.allowlist` row is removed.

/// The batch store sink (§2.1, THE WS-CB core deliverable). Capacity-N push
/// face over the fill run's `DestReceiver`: `accept` appends the produced
/// slot to the portal store through the receiver (the row-chain receive
/// path verbatim), bumps `es_processed`, and decrements the per-run
/// emission budget; `Full` exactly when the budget hits zero. Budget `None`
/// (a count-0 drain — the §2.4 persist arms) never fills: breaker posture,
/// the fill runs to exhaustion inside one ExecutorRun (and never suspends
/// mid-gang, §2.6).
pub(super) struct TuplestoreBatchSink<'d, 'm> {
    dest: &'d mut ::tcop_dest::DestReceiver<'m>,
    /// End-of-stream projected-slot clear, mirroring `RootAdapter`'s
    /// `clear_on_finish` (byte/state parity with the per-pull drive at
    /// exhaustion).
    clear_on_finish: Option<ExecSlotId>,
    /// The receiver returned false (receiver-initiated stop — the row
    /// loop's `break` arm). Never produced by the tuplestore receiver
    /// today; carried for protocol completeness.
    stopped: bool,
    /// SE-R41 (notes/se-r41-retire.md §3.6): the §4.2 in-run identity
    /// capture of a capture-batchable eligible fill — one sidecar append
    /// per ACCEPTED row, on the same condition as the store append
    /// (sidecar/store row alignment by construction). Capture at the emit
    /// surface is what makes the batch fill settle-safe: every row's
    /// identity is read from the scan slot BEFORE `cursor_run_park`'s slot
    /// hygiene can clear it.
    capture: Option<SinkCapture>,
}

/// SE-R41: the capture identity source, pinned at dispatch (the plan top is
/// the one SeqScan the §3.1 probe admitted): the scan's own tuple slot,
/// which the heap batch emit path stores per emitted row
/// (`heap_batch_store_slot` — a buffer heap tuple carrying its (block,
/// lineoff) tid), and the scan relation's oid.
pub(super) struct SinkCapture {
    pub(super) sidecar: ::types_portal::TuplestoreHandle,
    pub(super) rel_oid: ::types_core::Oid,
    pub(super) scan_slot: ExecSlotId,
}

impl SinkCapture {
    /// The per-row capture body: read the scan slot's identity (empty ⇒
    /// the invalid-identity row, C's lisnull arm at resolution) and append
    /// it sidecar-aligned. (The run seam's capture row loop fallback runs
    /// the tree-walk twin, `execcurrent::capture_current_into_sidecar`.)
    fn capture_row(&self, estate: &EStateData<'_>) -> PgResult<()> {
        let slot = estate.slot(self.scan_slot);
        let (oid, packed) = if slot.base().is_empty() {
            (0, 0)
        } else {
            (self.rel_oid, crate::execcurrent::pack_tid(&slot.base().tts_tid))
        };
        ::tuplestore::hold::tidstore_put(self.sidecar, oid, packed)
    }
}

impl<'d, 'm> TuplestoreBatchSink<'d, 'm> {
    pub(super) fn new(
        dest: &'d mut ::tcop_dest::DestReceiver<'m>,
        clear_on_finish: Option<ExecSlotId>,
        capture: Option<SinkCapture>,
    ) -> Self {
        TuplestoreBatchSink { dest, clear_on_finish, stopped: false, capture }
    }
}

impl<'mcx> Sink<'mcx> for TuplestoreBatchSink<'_, '_> {
    fn accept(
        &mut self,
        tuple: ExecSlotId,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<SinkFeed> {
        // EPQ pin (§2.1): the budget belongs to the outer run; an EPQ
        // recheck drive must never reach a store sink.
        debug_assert!(!estate.es_epq_active, "TuplestoreBatchSink inside an EPQ drive");
        // Overfill = the operator ignored `SinkFeed::Full` (the
        // RootAdapter-overfill law: silent row loss is a hard error in
        // release too).
        if estate.es_cursor_run_budget == Some(0) || self.stopped {
            return Err(Box::new(::types_error::PgError::error(
                "lane-v2 cursor store sink overfilled (operator ignored SinkFeed::Full)"
                    .to_string(),
            )));
        }
        {
            let slot = estate.slot_mut(tuple);
            // SAFETY: lifetime bridge at the seam boundary — identical to
            // `execute_plan`'s receive_slot arm (the receiver only copies
            // datums out during the call and retains no borrow).
            let slot: &mut ::types_slot::SlotData<'_> = unsafe {
                &mut *(slot as *mut ::types_slot::SlotData<'mcx>)
                    .cast::<::types_slot::SlotData<'_>>()
            };
            if !self.dest.receive_slot(slot)? {
                self.stopped = true;
                return Ok(SinkFeed::Full);
            }
        }
        // SE-R41: sidecar capture rides every store append (and only store
        // appends — a receiver stop above appends nothing and captures
        // nothing).
        if let Some(c) = &self.capture {
            c.capture_row(estate)?;
        }
        // The drive loop's SELECT accounting, moved with the drive (budget
        // installs only on CMD_SELECT runs).
        estate.es_processed += 1;
        // The per-accept budget decrement (the WS-AI enforcement-honesty
        // note executed): this field is the push drive's source of truth.
        match estate.es_cursor_run_budget.as_mut() {
            Some(b) => {
                *b -= 1;
                Ok(if *b == 0 { SinkFeed::Full } else { SinkFeed::NeedMore })
            }
            // Count-0 drain (§2.4): no budget, never Full.
            None => Ok(SinkFeed::NeedMore),
        }
    }

    fn finish(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()> {
        if let Some(slot) = self.clear_on_finish {
            let mcx = estate.es_query_cxt;
            ::exectuples::exec_clear_tuple(estate.slot_mut(slot), mcx);
        }
        Ok(())
    }
}

/// The fill driver (§2.2's executor half, one budgeted ExecutorRun's
/// worth): batches flow `Source → Operator → store sink` with NO
/// capacity-one per-row pull ceremony — the ratification's batching
/// clause. `drain_pipeline`'s pause-tolerant sibling: `Paused` (budget
/// exhausted mid-batch, position node-resident) returns control to the
/// run seam — the wave-9.5 settle point below the caller then parks the
/// staged claim (R3). Returns true iff the source exhausted
/// (`sink.finish` runs only then, mirroring `pull_step`'s end-of-stream
/// clear; a paused fill keeps its staged state for the resume walk).
pub(super) fn fill_step<'mcx, S, O>(
    node: &mut S::Node,
    src: &mut S,
    op: &mut O,
    sink: &mut TuplestoreBatchSink<'_, '_>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool>
where
    S: Source<'mcx>,
    O: Operator<'mcx, Node = S::Node>,
{
    loop {
        let batch = match op.pending(node) {
            Some(b) => b,
            None => match src.produce(node, estate)? {
                Some(b) => b,
                None => {
                    sink.finish(estate)?;
                    return Ok(true);
                }
            },
        };
        match op.consume(node, batch, sink, estate)? {
            OpStatus::Paused => return Ok(false),
            OpStatus::NeedInput => {}
            // Operator-driven early stop: treated exactly like source
            // exhaustion (`pull_step`'s Finished arm).
            OpStatus::Finished => {
                sink.finish(estate)?;
                return Ok(true);
            }
        }
    }
}

/// `execute_plan`'s batch-fill arm (§2.1/§2.3, called from the wave-10 CB
/// sub-region at the run seam): the fill-engine decision for a budgeted
/// store-fill run. Returns true iff the batch fill DROVE this run (the
/// caller then skips the per-tuple loop — the budget was consumed by the
/// sink); false = not a batch-fill shape, the row loop serves the same
/// store byte-identically (§2.3 fallback; the plan's own refusal reasons
/// ticked by the admission hooks).
///
/// Gates, in cost order: EPQ (§2.1 pin), the store face
/// (`CommandDest::Tuplestore` — any other receiver keeps the row loop;
/// this is also the compose-safe default for CURRENT-OF capturing
/// receivers, worklog §3), then the top-node shape. Batch-fill breadth
/// this increment = the standalone SeqScan pipeline; admission is
/// `try_own_seq_scan` ITSELF (first row through the standard hook — zero
/// duplication of the §3.2 admission set; heap standalone refuses on
/// today's admission economics and rides the row loop), remainder through
/// `fill_step`.
pub(crate) fn cursor_store_batch_fill<'m, 'mcx>(
    node: &mut crate::procnode::PlanStateNode<'mcx>,
    estate: &mut EStateData<'mcx>,
    dest: &mut ::tcop_dest::DestReceiver<'m>,
    capture: Option<::types_portal::TuplestoreHandle>,
) -> PgResult<bool> {
    debug_assert!(
        estate.es_cursor_run_budget.is_some(),
        "cursor_store_batch_fill on an unbudgeted run"
    );
    if estate.es_epq_active {
        return Ok(false);
    }
    // The lane-family master switch, EXACTLY as the per-pull hook path
    // gates it (procnode::seq_scan_arm: `if crate::lanev2::enabled()`):
    // the §7.2 Arm-R definition (CURSORS=1 + lane family OFF) must be the
    // ROW-CHAIN fill — calling the admission hook here without the master
    // gate would lane-own a fill the per-pull world would never own.
    if !super::enabled() {
        return Ok(false);
    }
    if dest.mydest() != ::types_dest::CommandDest::Tuplestore {
        return Ok(false);
    }
    let crate::procnode::PlanStateNode::SeqScan(ss) = node else {
        // A capture-armed run whose planstate top is not the bare SeqScan
        // (the Instrumented wrapper is the reachable case — the §3.1 probe
        // is plan-shape, instrumentation wraps at build): the caller's
        // capture row loop serves the fill, sidecar-aligned.
        return Ok(false);
    };
    // --- SE-R41 (notes/se-r41-retire.md §3.5): the heap capture arm -------
    // A capture-armed fill is the retirement's target cell: a
    // CURRENT-OF-ELIGIBLE bare heap SeqScan whose §4.2 capture rides the
    // sink (per accepted row, settle-safe). Admission = the standard
    // fusibility cascade (batch_allowed / instrumented / variant /
    // page-batch AM; its own class accounting + memoized verdict, which
    // also makes the settle walker's `engaged` detection see this fill) —
    // deliberately NOT `try_own_seq_scan`: its heap standalone refuse
    // prices the PER-PULL capacity-one adapter, which this push-sink drive
    // never pays (the analytics-bank store leg measured the store-fill economics at
    // −6.68% — the SE11 4a-store controlled experiment).
    if let Some(sidecar) = capture {
        debug_assert!(
            !::nodeseqscan::seq_scan_is_pgrcolumnar(ss),
            "capture-batchable probe admitted a pgrcolumnar scan (§3.1 AM narrowing)"
        );
        // SE-R41 v2: the ROW drive's own page-batch mode owns this scan's
        // staging (its position is an SoA selection cursor the lane can
        // neither continue nor adopt) — the row loop serves, byte-correctly.
        // Structurally unreachable today (both verdicts memoized-sticky
        // from scan start); one load per FILL, keeps the exclusion local.
        if ::nodeseqscan::seq_scan_row_batch_mode_on(ss) {
            return Ok(false);
        }
        if !super::seq_scan_fusible(ss, estate)? {
            // Run-time refusal (instrumented / no page batch / …): the
            // caller's capture row loop serves this run — correctness
            // never rides on the lane admitting.
            return Ok(false);
        }
        debug_assert!(::types_scan::sdir::ScanDirectionIsForward(estate.es_direction));
        // SE-R41 v2 (notes/se-r41-v2.md §3): the cursor-fill pin posture —
        // the staged page and its pin survive suspension (C-parity Volcano
        // posture; the settle walker refuses to park this scan), so the
        // per-fill park→release→restage ceremony never runs. Idempotent.
        ss.set_lane_hold_pin();
        super::stats::tick_owned(super::stats::ShapeClass::Cursor);
        let cap = SinkCapture {
            sidecar,
            rel_oid: ss
                .ss
                .ss_currentRelation
                .as_ref()
                .expect("seqscan has a relation")
                .rd_id,
            scan_slot: ss.ss.ss_ScanTupleSlot,
        };
        let clear = ss.ss.ps_ProjInfo.as_ref().map(|p| p.pi_result_slot);
        let mut sink = TuplestoreBatchSink::new(dest, clear, Some(cap));
        fill_step(
            ss,
            &mut super::SeqScanSource,
            &mut super::SeqScanFilterProject,
            &mut sink,
            estate,
        )?;
        return Ok(true);
    }
    // --- end SE-R41 -------------------------------------------------------
    // First row through the standard admission hook: the identical verdict
    // cascade, stats ticks and engine capture as a per-pull drive; a
    // refusal falls to the row-chain fill of the same store (§2.3).
    let Some(first) = super::try_own_seq_scan(ss, estate)? else {
        return Ok(false);
    };
    // SEAM-WIRING (SE10-GATES item 1): the `owned cursor` census goes LIVE —
    // one OWNED tick per ENGAGED batch store fill (per budgeted run the sink
    // drives, never per tuple; the scan's own class ticked its ownership in
    // the hook above). This is the §7.2 Arm-L attribution counter the
    // three-arm matrix's MATRIX_REQUIRE_LANE_FILL bar reads
    // (`owned\tcursor\tN>0`).
    super::stats::tick_owned(super::stats::ShapeClass::Cursor);
    let mut sink = TuplestoreBatchSink::new(
        dest,
        ss.ss.ps_ProjInfo.as_ref().map(|p| p.pi_result_slot),
        None,
    );
    if let Some(slot) = first {
        if let SinkFeed::Full = sink.accept(slot, estate)? {
            // Budget 1 (or receiver stop): the fill ran and paused with the
            // first row; position is node-resident from the pull.
            return Ok(true);
        }
    } else {
        // Admitted and exhausted at the first pull (empty result): the
        // pull's RootAdapter already ran the end-of-stream clear.
        return Ok(true);
    }
    fill_step(ss, &mut super::SeqScanSource, &mut super::SeqScanFilterProject, &mut sink, estate)?;
    Ok(true)
}

/// Test face: drive the standalone-scan fill pipeline into a
/// `TuplestoreBatchSink` over `dest` WITHOUT the admission cascade (the
/// scanfix heap fixture refuses standalone admission by design, so the
/// band-95001 protocol pins enter below the verdict — the same
/// Source/Operator/sink composition `cursor_store_batch_fill` drives after
/// admission). Returns `fill_step`'s exhausted-vs-paused.
#[cfg(test)]
pub(crate) fn cursor_fill_step_seqscan_for_tests<'m, 'mcx>(
    ss: &mut ::nodeseqscan::SeqScanState<'mcx>,
    dest: &mut ::tcop_dest::DestReceiver<'m>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    let clear = ss.ss.ps_ProjInfo.as_ref().map(|p| p.pi_result_slot);
    let mut sink = TuplestoreBatchSink::new(dest, clear, None);
    fill_step(ss, &mut super::SeqScanSource, &mut super::SeqScanFilterProject, &mut sink, estate)
}

// --- end WS-CB wave-10 -----------------------------------------------------------

// =============================================================================
// Row-mode driver mechanics (pull_step_rows over stub source/op): the driver
// contract itself — resume-before-produce ordering, Paused-then-Finished,
// the source_exhausted seam, error propagation, and no-pull-past-exhaustion.
// Byte-identity of the REAL faces (ResultRowSource / ProjectSetOp) is proven
// by the A/B corpus in `crate::tests` and scripts/lane-rowmode-e2e.sh.
// =============================================================================
#[cfg(test)]
mod rows_tests {
    use super::*;

    fn with_estate<R>(f: impl for<'m> FnOnce(&mut EStateData<'m>) -> R) -> R {
        let mut exec = ::mcx::McxOwned::<crate::querydesc::ExecTy>::try_new(
            ::mcx::MemoryContext::new_bump("push-rows-test"),
            |mcx| {
                Ok(crate::querydesc::ExecData {
                    estate: EStateData::new_in(mcx),
                    planstate: None,
                })
            },
        )
        .unwrap();
        let r = exec.with_mut(|d| f(&mut d.estate));
        exec.with_mut(|d| d.estate.teardown());
        r
    }

    /// Node-resident stub state: the scripted rows + a produce-call counter.
    struct StubNode {
        rows: Vec<u32>,
        next: usize,
        produce_calls: usize,
        error_at: Option<usize>,
    }

    struct StubSource;

    impl<'mcx> RowSource<'mcx> for StubSource {
        type Node = StubNode;
        fn next_row(
            &mut self,
            node: &mut StubNode,
            _estate: &mut EStateData<'mcx>,
        ) -> PgResult<Option<ExecSlotId>> {
            node.produce_calls += 1;
            if node.error_at == Some(node.next) {
                return Err(Box::new(::types_error::PgError::error(
                    "stub row source error".to_string(),
                )));
            }
            let Some(&id) = node.rows.get(node.next) else {
                return Ok(None);
            };
            node.next += 1;
            Ok(Some(ExecSlotId(id)))
        }
    }

    /// Expanding stub: each accepted tuple emits `expand` copies; cross-call
    /// remainder lives in `left` (the node-resident cursor stand-in), plus an
    /// optional single-tuple post-exhaustion tail.
    struct StubOp {
        expand: usize,
        left: usize,
        cur: Option<ExecSlotId>,
        tail: Option<ExecSlotId>,
        tail_done: bool,
    }

    impl StubOp {
        fn passthrough() -> StubOp {
            StubOp { expand: 1, left: 0, cur: None, tail: None, tail_done: false }
        }

        fn emit_one<'mcx>(
            &mut self,
            out: &mut dyn Sink<'mcx>,
            estate: &mut EStateData<'mcx>,
        ) -> PgResult<OpStatus> {
            self.left -= 1;
            Ok(match out.accept(self.cur.expect("expansion tuple"), estate)? {
                SinkFeed::Full => OpStatus::Paused,
                SinkFeed::NeedMore => OpStatus::NeedInput,
            })
        }
    }

    impl<'mcx> TupleOp<'mcx> for StubOp {
        fn pending(&self) -> bool {
            self.left > 0
        }

        fn accept(
            &mut self,
            tuple: ExecSlotId,
            out: &mut dyn Sink<'mcx>,
            estate: &mut EStateData<'mcx>,
        ) -> PgResult<OpStatus> {
            assert_eq!(self.left, 0, "accept while an expansion pends");
            self.cur = Some(tuple);
            self.left = self.expand;
            if self.left == 0 {
                return Ok(OpStatus::NeedInput);
            }
            self.emit_one(out, estate)
        }

        fn resume(
            &mut self,
            out: &mut dyn Sink<'mcx>,
            estate: &mut EStateData<'mcx>,
        ) -> PgResult<OpStatus> {
            assert!(self.left > 0, "resume without a pending expansion");
            self.emit_one(out, estate)
        }

        fn source_exhausted(
            &mut self,
            out: &mut dyn Sink<'mcx>,
            estate: &mut EStateData<'mcx>,
        ) -> PgResult<OpStatus> {
            match self.tail {
                Some(t) if !self.tail_done => {
                    self.tail_done = true;
                    Ok(match out.accept(t, estate)? {
                        SinkFeed::Full => OpStatus::Paused,
                        SinkFeed::NeedMore => OpStatus::NeedInput,
                    })
                }
                _ => Ok(OpStatus::Finished),
            }
        }
    }

    fn pull<'mcx>(
        node: &mut StubNode,
        op: &mut StubOp,
        estate: &mut EStateData<'mcx>,
    ) -> PgResult<Option<ExecSlotId>> {
        let mut root = RootAdapter::new(None);
        pull_step_rows(node, &mut StubSource, op, &mut root, estate)
    }

    #[test]
    fn rows_driver_delivers_each_row_then_exhausts() {
        with_estate(|estate| {
            let mut node =
                StubNode { rows: vec![7, 8], next: 0, produce_calls: 0, error_at: None };
            let mut op = StubOp::passthrough();
            assert_eq!(pull(&mut node, &mut op, estate).unwrap(), Some(ExecSlotId(7)));
            assert_eq!(node.produce_calls, 1, "capacity-one root: one produce per pull");
            assert_eq!(pull(&mut node, &mut op, estate).unwrap(), Some(ExecSlotId(8)));
            assert_eq!(node.produce_calls, 2);
            assert_eq!(pull(&mut node, &mut op, estate).unwrap(), None);
            assert_eq!(node.produce_calls, 3, "EOF pull sees exhaustion exactly once");
        });
    }

    #[test]
    fn rows_driver_resumes_pending_expansion_before_producing() {
        with_estate(|estate| {
            let mut node =
                StubNode { rows: vec![1, 2], next: 0, produce_calls: 0, error_at: None };
            let mut op = StubOp { expand: 2, left: 0, cur: None, tail: None, tail_done: false };
            // Pull 1: produce row 1, eat expansion tuple 1 of 2 (Paused).
            assert_eq!(pull(&mut node, &mut op, estate).unwrap(), Some(ExecSlotId(1)));
            assert_eq!(node.produce_calls, 1);
            assert!(op.pending());
            // Pull 2: the pending expansion resumes WITHOUT touching the
            // source (its remainder exists only in the op's cursor).
            assert_eq!(pull(&mut node, &mut op, estate).unwrap(), Some(ExecSlotId(1)));
            assert_eq!(node.produce_calls, 1, "resume must not produce");
            assert!(!op.pending());
            // Pulls 3-4: row 2's expansion; pull 5: EOF.
            assert_eq!(pull(&mut node, &mut op, estate).unwrap(), Some(ExecSlotId(2)));
            assert_eq!(pull(&mut node, &mut op, estate).unwrap(), Some(ExecSlotId(2)));
            assert_eq!(node.produce_calls, 2);
            assert_eq!(pull(&mut node, &mut op, estate).unwrap(), None);
        });
    }

    #[test]
    fn rows_driver_skips_empty_expansions() {
        with_estate(|estate| {
            // expand=0: every accepted tuple is filtered (NeedInput), so one
            // pull walks the whole source to EOF.
            let mut node =
                StubNode { rows: vec![1, 2, 3], next: 0, produce_calls: 0, error_at: None };
            let mut op = StubOp { expand: 0, left: 0, cur: None, tail: None, tail_done: false };
            assert_eq!(pull(&mut node, &mut op, estate).unwrap(), None);
            assert_eq!(node.produce_calls, 4);
        });
    }

    #[test]
    fn rows_driver_source_exhausted_tail_obeys_paused_then_finished() {
        with_estate(|estate| {
            let mut node = StubNode { rows: vec![], next: 0, produce_calls: 0, error_at: None };
            let mut op = StubOp {
                expand: 1,
                left: 0,
                cur: None,
                tail: Some(ExecSlotId(99)),
                tail_done: false,
            };
            // The tail tuple is delivered via Paused; only the NEXT pull's
            // (idempotent) seam call reports Finished.
            assert_eq!(pull(&mut node, &mut op, estate).unwrap(), Some(ExecSlotId(99)));
            assert_eq!(pull(&mut node, &mut op, estate).unwrap(), None);
        });
    }

    #[test]
    fn rows_driver_propagates_source_errors() {
        with_estate(|estate| {
            let mut node =
                StubNode { rows: vec![5, 6], next: 0, produce_calls: 0, error_at: Some(1) };
            let mut op = StubOp::passthrough();
            assert_eq!(pull(&mut node, &mut op, estate).unwrap(), Some(ExecSlotId(5)));
            let err = pull(&mut node, &mut op, estate).unwrap_err();
            assert!(err.to_string().contains("stub row source error"));
        });
    }
}
