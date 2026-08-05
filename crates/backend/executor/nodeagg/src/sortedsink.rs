//! sorted-arm lane — the ordered-grouped runtime aggregation sink's nodeagg
//! seams (leader emit state + boundary-group stitch installs).
//!
//! The runtime ordered arm (execmain lanev2/runtime_agg_sorted.rs) drives the
//! serial sorted-fold kernels per morsel claim in the workers; each claim's
//! COMPLETE interior groups are finalized+HAVING'd+projected worker-side and
//! captured as self-contained rows ([`SortedEmitAcc`]/[`SortedEmitSeg`]);
//! the claim's two edge groups cross as `RuntimePartial` boundary partials
//! (runtime_partial.rs) and the LEADER stitches adjacent claims here:
//! [`agg_sorted_stitch_begin`] (install the group representative +
//! initialize), `runtime_partial::agg_sorted_absorb_partial` (write the
//! combined states), then the node's own `agg_sorted_emit` (finalize +
//! HAVING + project — the serial path's exact code).
//!
//! Emission: the adopted ordered segments ([`SortedSinkEmitState`]) serve one
//! row per pull through [`agg_sorted_sink_emit_next`] — segment order is
//! claim range order, which on a group-key-clustered store IS group-key
//! order, so the AGG_SORTED pathkey contract is preserved.

use ::datum::Datum;
use ::types_error::{PgError, PgResult};

use ::executils::{EStateData, ExecSlotId};

use crate::AggStateData;

/// One self-contained ordered segment of projected result rows (row-major
/// values/nulls; byref datums point into the segment's own arena). Plain
/// Rust memory — Send, no memory-context residue.
pub struct SortedEmitSeg {
    pub values: Vec<Datum>,
    pub nulls: Vec<bool>,
    pub nrows: usize,
    pub natts: usize,
    pub arena: Vec<u8>,
}

impl SortedEmitSeg {
    pub fn bytes(&self) -> usize {
        self.values.capacity() * core::mem::size_of::<Datum>()
            + self.nulls.capacity()
            + self.arena.capacity()
    }
}

/// Per-output-column byref spec: 0 = byval (datum word verbatim), -1 =
/// varlena (copy `varsize_any` bytes), n > 0 = fixed-length byref (copy n
/// bytes). Derived once per engagement by [`agg_sorted_result_byref_spec`];
/// any other attlen class (cstring/expanded) refuses the arm.
pub type SortedByrefSpec = Vec<i16>;

/// The result tupledesc's byref spec, `None` = a column class the capture
/// cannot deep-copy (fail-closed admission input).
pub fn agg_sorted_result_byref_spec(node: &AggStateData<'_>) -> Option<SortedByrefSpec> {
    let desc = node.ps_ResultTupleDesc.as_ref()?;
    let mut spec = Vec::with_capacity(desc.compact_attrs.len());
    for att in desc.compact_attrs.iter() {
        if att.attbyval {
            spec.push(0);
        } else if att.attlen == -1 {
            spec.push(-1);
        } else if att.attlen > 0 {
            spec.push(att.attlen);
        } else {
            return None;
        }
    }
    Some(spec)
}

/// Row accumulator for one segment: push projected rows (deep-copying byref
/// datums into the arena, 8-aligned, with end-resolved fixups — Vec growth
/// may move the buffer), then [`SortedEmitAcc::finish`].
pub struct SortedEmitAcc {
    values: Vec<Datum>,
    nulls: Vec<bool>,
    arena: Vec<u8>,
    fixups: Vec<(usize, usize)>,
    nrows: usize,
    natts: usize,
}

impl SortedEmitAcc {
    pub fn new(natts: usize) -> Self {
        SortedEmitAcc {
            values: Vec::new(),
            nulls: Vec::new(),
            arena: Vec::new(),
            fixups: Vec::new(),
            nrows: 0,
            natts,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.nrows == 0
    }

    /// Retained bytes (budget metering — the R3 envelope input).
    pub fn bytes(&self) -> usize {
        self.values.capacity() * core::mem::size_of::<Datum>()
            + self.nulls.capacity()
            + self.arena.capacity()
    }

    /// Append one projected row. `values`/`nulls` are the result slot's
    /// populated arrays (natts leading entries).
    ///
    /// # Safety
    /// Non-null byref datums (per `spec`) point at live, readable images of
    /// their declared class (varlena header readable for -1).
    pub unsafe fn push_row(
        &mut self,
        values: &[Datum],
        nulls: &[bool],
        spec: &SortedByrefSpec,
    ) -> PgResult<()> {
        debug_assert_eq!(spec.len(), self.natts);
        for c in 0..self.natts {
            let isnull = nulls[c];
            let d = values[c];
            if isnull || spec[c] == 0 {
                self.values.push(d);
                self.nulls.push(isnull);
                continue;
            }
            let p = d.as_usize() as *const u8;
            if p.is_null() {
                return Err(Box::new(PgError::error(
                    "sorted sink capture: NULL pointer in a non-null byref datum".to_string(),
                )));
            }
            // SAFETY: caller contract (live image of the declared class).
            let len = unsafe {
                if spec[c] == -1 {
                    ::types_tuple::varatt::varsize_any(p)
                } else {
                    spec[c] as usize
                }
            };
            // 8-align the OFFSET (varlena consumers may read 4-byte headers
            // + aligned payloads; fixed byref types may be int64-aligned).
            // The arena BASE relies on the global allocator returning
            // >=8-aligned blocks for byte allocations — the same contract
            // sink.rs's SinkEmitBuf arena ships on (mimalloc guarantees it);
            // a strict-alignment port would give both arenas word-typed
            // backing in one move.
            let pad = (8 - self.arena.len() % 8) % 8;
            self.arena.resize(self.arena.len() + pad, 0);
            let off = self.arena.len();
            // SAFETY: caller contract — len readable bytes at p.
            self.arena
                .extend_from_slice(unsafe { core::slice::from_raw_parts(p, len) });
            self.fixups.push((self.values.len(), off));
            self.values.push(Datum::null()); // resolved at finish
            self.nulls.push(false);
        }
        self.nrows += 1;
        Ok(())
    }

    /// Resolve fixups (arena final) and freeze into a segment.
    pub fn finish(mut self) -> SortedEmitSeg {
        for (i, off) in self.fixups.drain(..) {
            self.values[i] = Datum::from_usize(self.arena[off..].as_ptr() as usize);
        }
        SortedEmitSeg {
            values: self.values,
            nulls: self.nulls,
            nrows: self.nrows,
            natts: self.natts,
            arena: self.arena,
        }
    }
}

/// The leader's adopted ordered emit state (mirrors the hash sink's
/// `SinkEmitState`, ordered-segments flavor).
pub struct SortedSinkEmitState {
    segs: Vec<SortedEmitSeg>,
    seg: usize,
    pos: usize,
    natts: usize,
}

/// Adopt the stitched ordered segments; subsequent
/// [`agg_sorted_sink_emit_next`] calls drain them in order.
pub fn agg_sorted_sink_adopt(node: &mut AggStateData<'_>, segs: Vec<SortedEmitSeg>, natts: usize) {
    node.sorted_sink_emit = Some(Box::new(SortedSinkEmitState { segs, seg: 0, pos: 0, natts }));
}

/// Mid-emit marker for the lane dispatch.
pub fn agg_sorted_sink_emitting(node: &AggStateData<'_>) -> bool {
    node.sorted_sink_emit.is_some()
}

/// One emitted row per call (datum memcpy into the result slot — rows were
/// finalized/projected at capture/stitch time). `None` = drained (agg_done
/// set; the state is KEPT — its arenas back byref datums already handed out
/// this scan — and drops at rescan/teardown through
/// [`agg_sorted_sink_reset`]).
pub fn agg_sorted_sink_emit_next<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    let mcx = estate.es_query_cxt;
    let next = {
        let st = node.sorted_sink_emit.as_mut().expect("sorted sink emit state adopted");
        loop {
            if st.seg >= st.segs.len() {
                break None;
            }
            if st.pos >= st.segs[st.seg].nrows {
                st.seg += 1;
                st.pos = 0;
                continue;
            }
            let row = st.pos;
            st.pos += 1;
            break Some((st.seg, row));
        }
    };
    let Some((seg, row)) = next else {
        node.agg_done = true;
        return Ok(None);
    };
    let st = node.sorted_sink_emit.as_ref().expect("sorted sink emit state adopted");
    let natts = st.natts;
    let s = &st.segs[seg];
    debug_assert_eq!(natts, s.natts);
    let base_off = row * natts;
    let slot = estate.slot_mut(node.ps_ResultTupleSlot);
    ::exectuples::exec_clear_tuple(slot, mcx);
    {
        let sb = slot.base_mut();
        sb.tts_values[..natts].copy_from_slice(&s.values[base_off..base_off + natts]);
        sb.tts_isnull[..natts].copy_from_slice(&s.nulls[base_off..base_off + natts]);
    }
    ::exectuples::exec_store_virtual_tuple(slot);
    Ok(Some(node.ps_ResultTupleSlot))
}

/// Drop any adopted ordered emit state (rescan / teardown safety).
pub fn agg_sorted_sink_reset(node: &mut AggStateData<'_>) {
    node.sorted_sink_emit = None;
}

/// Leader-side stitch prologue: begin the boundary group with a
/// representative tuple RECONSTRUCTED FROM ITS KEY DATUMS (`keys[k]` at
/// 0-based outer column `cols[k]`, every other column NULL — legal because
/// the arm's admission proved proj/qual reference only grouping columns) —
/// `agg_sorted_group_begin`'s prologue WITHOUT the first-row transition
/// program (the absorbed partial already includes every row of the group).
/// The caller follows with `runtime_partial::agg_sorted_absorb_partial` +
/// `agg_sorted_emit`. The minimal tuple forms directly into
/// `persort.first_slot` (slot-owned; the store frees the previous image) —
/// no per-engagement scratch slot exists.
pub fn agg_sorted_stitch_begin_keys<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    keys: &[(Datum, bool)],
    cols: &[u16],
) -> PgResult<()> {
    debug_assert_eq!(node.plan.aggstrategy, crate::AGG_SORTED);
    debug_assert_eq!(keys.len(), cols.len());
    let mcx = estate.es_query_cxt;
    estate.reset_expr_context(node.ps_ExprContext);
    // SAFETY: sole access path to the node during the reset (the sorted
    // group prologue's own discipline; see agg_sorted_group_begin). The
    // hash-grouped degrade residue never coexists with the runtime arm
    // (engagement admission), but keep the same guard.
    if !crate::hashgrouped::agg_hashgroup_state_active(node) {
        unsafe { node.agg_node.as_mut() }.reset();
    }
    {
        let AggStateData { persort, .. } = node;
        let ps = persort.as_mut().expect("sorted Agg has persort");
        let desc = ps
            .first_slot
            .base()
            .tts_tupleDescriptor
            .clone()
            .expect("persort slot has a descriptor");
        let natts = desc.natts as usize;
        let mut values = vec![Datum::null(); natts];
        let mut isnull = vec![true; natts];
        for (k, &(d, n)) in keys.iter().enumerate() {
            let c = cols[k] as usize;
            debug_assert!(c < natts);
            values[c] = d;
            isnull[c] = n;
        }
        let mtup = ::heaptuple::heap_form_minimal_tuple(mcx, &desc, &values, &isnull, 0)?;
        ::exectuples::exec_store_minimal_tuple_owned(&mut ps.first_slot, mcx, mtup);
    }
    crate::initialize_aggregates(node, estate)?;
    estate.reset_expr_context(node.tmpcontext);
    Ok(())
}
