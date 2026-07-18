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
        // q22 sort-feed lever, generalized; CFI cadence for skipped rows
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

/// `PGRUST_LANE_V2_CURSORS` (default OFF; R-KNOBS registry spelling): the
/// forward-pull cursor gate. OFF = the run seam installs no budget and every
/// byte of the run path behaves as today (the install call short-circuits on
/// `count == 0` before reaching this cell, so simple-query runs never even
/// load it). ON = count-limited forward SELECT runs carry a per-run emission
/// budget for the cursor machinery to read. AtomicU8 + `_set_for_tests`
/// idiom (heapfeed precedent, batch_source.rs).
static CURSORS: AtomicU8 = AtomicU8::new(0);

pub(crate) fn cursors_v2_enabled() -> bool {
    match CURSORS.load(Relaxed) {
        1 => false,
        2 => true,
        _ => {
            let on = matches!(
                std::env::var("PGRUST_LANE_V2_CURSORS").as_deref(),
                Ok("1") | Ok("on")
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

/// The run seam's install half (`execute_plan`, once per ExecutorRun):
/// computes the value of `es_cursor_run_budget` for this run —
/// `Some(count)` iff this run is a knob-ON, count-limited, forward SELECT:
/// the §3.1 count-exact suspension shape. The caller writes the result to
/// the estate UNCONDITIONALLY (a None overwrites any stale value, so an
/// estate re-run after an error can never inherit a budget). Gate order is
/// the cost order: `count == 0` (every simple-protocol run) answers with
/// one register test before the knob cell is ever loaded.
///
/// FAIL-CLOSED serial-law arm: `use_parallel_mode` is false for every
/// count-limited run by the ported :978 gate; if that gate ever regressed,
/// this returns None (no cursor machinery over a gang, ever) rather than
/// asserting — the corpus batteries would then read the missing engagement
/// loudly instead of a debug-only crash.
pub(crate) fn cursor_run_budget_install(
    is_select: bool,
    forward: bool,
    count: u64,
    use_parallel_mode: bool,
) -> Option<u64> {
    if count == 0 || !is_select || !forward {
        return None;
    }
    if !cursors_v2_enabled() {
        return None;
    }
    if use_parallel_mode {
        // Unreachable by the :978 gate (count != 0 forces serial); the
        // fail-closed refusal IS the §3 pin at this seam.
        return None;
    }
    Some(count)
}

/// The read half for lane-side consumers (the §2 park/settle glue, inc-1b):
/// the emission budget of the current run, None outside a budgeted one.
#[allow(dead_code)] // first non-test consumer = the inc-1b park walker
pub(crate) fn cursor_run_budget(estate: &::executils::EStateData<'_>) -> Option<u64> {
    estate.es_cursor_run_budget
}

// --- end WS-AI wave-9 ----------------------------------------------------------

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
