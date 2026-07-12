//! Lane-v2 compact-row aggregation table hosting (cbstore-v2 plan Stage 2.2).
//!
//! The `lanetable::LaneAggTable` replaces the C-ported tuplehash as the
//! GROUP BY table for a narrow, explicitly-admitted shape; everything else
//! keeps the C table exactly as today (refuse/fallback discipline). The
//! table owns LAYOUT AND PROBE ONLY — transition and finalize still run the
//! real C-ported aggregate code over `AggPerGroup` states stored in the
//! compact payload rows (zero-initialized at group birth, seeded by the same
//! `trans_init` datumCopy loop as `initialize_hash_entry`), so transvalues
//! are byte-identical to the C path's. Group OUTPUT ORDER diverges (row /
//! insertion order vs simplehash bucket order) — legal under the 2026-07-13
//! order-relaxation policy; ORDER BY-wrapped outputs are unaffected.
//!
//! Admission (v1, decided per build by the lane's scan-K2 feed — see
//! `execmain::lanev2`):
//!   * the scan-K2 shape already holds (AGG_HASHED, single grouping key with
//!     a kernel probe, fully lanefold-admitted transitions, no residuals,
//!     unguarded plan, key + needed columns staged in SoA lanes);
//!   * the key kernel is an INTEGER width (int2/int4/int8 — text keys keep
//!     the C table until the str8/arena hosting lands; the lanetable crate
//!     already implements it, microbench-proven);
//!   * `aggsplit == AGGSPLIT_SIMPLE` (partial-agg handoffs read the C table);
//!   * NOT spill-eligible by estimate: planner `numGroups` must fit within
//!     HALF the hash-mem/ngroups limits (v1 policy: the compact table
//!     REFUSES spill-eligible plans; distinct-spill is v2, like the
//!     uniqExact lane). A RUNTIME BACKSTOP re-checks actual memory before
//!     every batch and MIGRATES to the C table when the half-limit is
//!     crossed (planner estimates lie — the rows=1 defect), after which the
//!     build continues on the C path, spill machinery intact. Peak memory
//!     during a migration is bounded by ~2× the half-limit = the limit.
//!
//! Memory accounting: `LaneAggTable::mem_used()` (entry arrays + row chunks
//! + arena, capacities not lengths) + the aggcontext's `subtree_used` stand
//! in for the C path's meta/entry/transvalue triple, compared against the
//! SAME `hash_mem_limit` at half margin — conservative by construction.

use core::ptr::NonNull;

use ::datum::Datum;
use ::execexpr::AggPerGroup;
use ::executils::EStateData;
use ::types_error::PgResult;

use crate::{AggStateData, PerHashData};

/// Per-node compact-table state, hosted in [`PerHashData`].
pub(crate) struct CompactHash {
    pub(crate) table: ::lanetable::LaneAggTable,
    /// Grouping-key integer width in bytes (2/4/8).
    pub(crate) width: u8,
    // Batch scratch (canonical keys + probe outputs), reused across batches.
    keys: Vec<i64>,
    states: Vec<*mut u8>,
    hashes: Vec<u64>,
    new_rows: Vec<u32>,
}

/// The compact-table arming verdict — lanev2 ticks its refuse-reason
/// accounting off the non-`Armed` variants.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompactArm {
    Armed,
    /// Key kernel is not an admitted kind (text/expr — C table hosts it).
    KeyKind,
    /// Spill-eligible by planner estimate (v1 refuses; C table spills).
    SpillRisk,
    /// Kill switch (`PGRUST_LANE_V2_COMPACT=0`) or non-simple aggsplit.
    Off,
}

/// `PGRUST_LANE_V2_COMPACT` kill switch (default ON inside the lane; the
/// lane itself is behind `PGRUST_LANE_V2`). Resolved once per process.
fn compact_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_LANE_V2_COMPACT").map_or(true, |v| v != "0"))
}

/// Decide + arm the compact table for this build. Caller (the lane's scan-K2
/// feed) has already admitted the K2 shape; this adds the compact-specific
/// gates (module doc). Idempotent per build: re-arming an armed node keeps
/// its table.
pub fn agg_hash_compact_try_arm(node: &mut AggStateData<'_>) -> CompactArm {
    if !compact_enabled() || node.plan.aggsplit != ::types_pathnodes::AGGSPLIT_SIMPLE {
        return CompactArm::Off;
    }
    let numgroups = node.plan.numGroups.max(1) as u64;
    let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
    if ph.compact.is_some() {
        return CompactArm::Armed;
    }
    let Some(width) = ph.hashtable.staged_probe_int_width() else {
        return CompactArm::KeyKind;
    };
    let additionalsize = ph.hashtable.additionalsize();
    debug_assert!(additionalsize > 0, "K2 shapes carry a fold plan (numtrans > 0)");
    // Spill-eligibility estimate at half margin: entry (8 B at ≤0.5 fill →
    // 16), key word, states, and a transvalue-slack allowance per group.
    let est_bytes = numgroups.saturating_mul(16 + 8 + additionalsize as u64 + 16);
    if numgroups > ph.hash_ngroups_limit / 2 || est_bytes > ph.hash_mem_limit as u64 / 2 {
        return CompactArm::SpillRisk;
    }
    ph.compact = Some(CompactHash {
        table: ::lanetable::LaneAggTable::new(
            ::lanetable::KeyRepr::Int,
            additionalsize,
            (numgroups as usize).min(1 << 20),
        ),
        width,
        keys: Vec::new(),
        states: Vec::new(),
        hashes: Vec::new(),
        new_rows: Vec::new(),
    });
    CompactArm::Armed
}

/// Whether this build currently runs on the compact table.
pub fn agg_hash_compact_armed(node: &AggStateData<'_>) -> bool {
    node.perhash.as_ref().is_some_and(|ph| ph.compact.is_some())
}

/// One staged batch through the compact table: canonicalize the key lane to
/// i64 per the kernel width, batch-probe (hash inside the table — the PG
/// hash functions are bypassed entirely; internal tables carry no semantic
/// hash constraint), seed NEW groups with the same `trans_init` datumCopy
/// loop as `initialize_hash_entry`, and hand back one live `AggPerGroup`
/// pointer per input row for the caller's whole-batch fold.
///
/// Returns `false` when the runtime backstop fired BEFORE the batch: the
/// table migrated into the C tuplehash and disarmed — the caller re-probes
/// this batch (and all later ones) through the normal staged path.
pub fn agg_hash_compact_batch<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    keys: &[Datum],
    isnull: &[bool],
    groups: &mut Vec<NonNull<AggPerGroup>>,
) -> PgResult<bool> {
    debug_assert_eq!(keys.len(), isnull.len());
    let mcx = estate.es_query_cxt;
    // SAFETY: read of the once-allocated node; no &mut to it is live.
    let aggctx = unsafe { node.agg_node.as_ref() }.aggcontext();
    // Runtime backstop (module doc): actual footprint against the half
    // limits BEFORE the batch, so migration never invalidates pointers the
    // caller's fold still holds.
    {
        let ph = node.perhash.as_ref().expect("hashed Agg has perhash");
        let ch = ph.compact.as_ref().expect("compact batch requires an armed table");
        let mem = ch.table.mem_used() + aggctx.context().subtree_used();
        if ch.table.len() as u64 >= ph.hash_ngroups_limit / 2
            || mem >= ph.hash_mem_limit / 2
        {
            compact_migrate(node, estate)?;
            return Ok(false);
        }
    }
    let AggStateData { perhash, trans_init, trans_typ, .. } = node;
    let ph = perhash.as_mut().expect("hashed Agg has perhash");
    let ch = ph.compact.as_mut().expect("compact batch requires an armed table");
    let CompactHash { table, width, keys: ckeys, states, hashes, new_rows } = ch;
    ckeys.clear();
    states.clear();
    new_rows.clear();
    groups.clear();
    // Canonicalize the key lane (the kernels compare exactly these widths).
    match *width {
        2 => ckeys.extend(keys.iter().map(|d| d.as_i16() as i64)),
        4 => ckeys.extend(keys.iter().map(|d| d.as_i32() as i64)),
        _ => ckeys.extend(keys.iter().map(|d| d.as_i64())),
    }
    if isnull.iter().any(|&n| n) {
        // NULL keys are rare in GROUP BY streams: per-row probe with the
        // out-of-band NULL group (the batched kernel stays null-free).
        for (i, &n) in isnull.iter().enumerate() {
            let pr = if n {
                table.probe_null()
            } else {
                let k = ckeys[i];
                table.probe_int(k, ::lanetable::hash_int(k as u64))
            };
            states.push(pr.states);
            if pr.is_new {
                new_rows.push(i as u32);
            }
        }
    } else {
        // Prefetch idiom: CH-style ADAPTIVE, per the pod A/B verdict
        // (2026-07-14, ch-bench-pod, 8.4M-row staged hits keys): at u64
        // card 1e6/1e8 adaptive beat DuckDB pre-touch by 6–10% (191/170 vs
        // 204/189 Mns/pass) and no-prefetch by 17–24%; below the L2 gate all
        // three are equal by construction (both idioms disable there).
        table.probe_int_batch(
            ckeys,
            ::lanetable::PrefetchMode::Adaptive,
            hashes,
            states,
            new_rows,
        );
    }
    // Seed the new groups' states — initialize_hash_entry's datumCopy loop
    // verbatim, writing into the compact row's zeroed state bytes.
    for &i in new_rows.iter() {
        let pergroup = states[i as usize].cast::<AggPerGroup>();
        for (transno, init) in trans_init.iter().enumerate() {
            let typ = trans_typ[transno];
            let value = if !init.isnull && !typ.byval {
                // SAFETY: node-lifetime initval datum copied into the
                // aggcontext (C initialize_aggregate's datumCopy).
                unsafe { ::execexpr::agg_datum_copy(aggctx, init.value, typ.len)? }
            } else {
                init.value
            };
            // SAFETY: the row's state block holds numtrans AggPerGroup
            // slots, zeroed at creation (lanetable contract).
            unsafe {
                pergroup.add(transno).write(AggPerGroup {
                    trans_value: value,
                    trans_value_is_null: init.isnull,
                    no_trans_value: init.isnull,
                });
            }
        }
    }
    groups.extend(states.iter().map(|&s| {
        // SAFETY: probe never returns null state pointers.
        unsafe { NonNull::new_unchecked(s.cast::<AggPerGroup>()) }
    }));
    let _ = mcx;
    Ok(true)
}

/// Force-disarm the compact table (migrating its groups into the C
/// tuplehash) — the lane calls this the moment a build batch must route
/// through the arrival probe (SoA fallback rows), so ALL groups always live
/// in exactly one table. No-op when not armed.
pub fn agg_hash_compact_disarm<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if agg_hash_compact_armed(node) {
        compact_migrate(node, estate)?;
    }
    Ok(())
}

/// Reconstruct row `row`'s key datum per the kernel width. `None` = the NULL
/// group.
#[inline]
fn compact_key_datum(ch: &CompactHash, row: usize) -> Option<Datum> {
    ch.table.row_key_int(row).map(|k| match ch.width {
        2 => Datum::from_i16(k as i16),
        4 => Datum::from_i32(k as i32),
        _ => Datum::from_i64(k),
    })
}

/// Runtime backstop: move every compact group into the C tuplehash and
/// disarm. Entries land in first-arrival (row) order through the SAME
/// C-ported `lookup` insert leg the per-row path uses; the `AggPerGroup`
/// states are plain bytes whose by-ref transvalues live in the aggcontext —
/// pointer-stable across the copy. Group count and memory checks resume on
/// the C path right after (one `hash_agg_check_limits` here flips spill mode
/// if the merged footprint already crossed the real limit).
#[cold]
#[inline(never)]
fn compact_migrate<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    // SAFETY: read of the once-allocated node; no &mut to it is live.
    let aggctx = unsafe { node.agg_node.as_ref() }.aggcontext();
    let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
    let ch = ph.compact.take().expect("migration requires an armed table");
    {
        // Same switch as lanev2's trace helpers (observability only).
        static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        if *ON.get_or_init(|| {
            matches!(std::env::var("PGRUST_LANE_V2_TRACE").as_deref(), Ok("1") | Ok("on"))
        }) {
            eprintln!(
                "[lanev2] compact table migrating to C tuplehash ({} groups, {} bytes)",
                ch.table.len(),
                ch.table.mem_used()
            );
        }
    }
    let additionalsize = ph.hashtable.additionalsize();
    debug_assert!(!ph.spill.mode, "compact builds never enter spill mode");
    for row in 0..ch.table.nrows() {
        let (key, key_isnull) = match compact_key_datum(&ch, row) {
            Some(d) => (d, false),
            None => (Datum::null(), true),
        };
        ::exectuples::exec_clear_tuple(&mut ph.hashslot, mcx);
        {
            let base = ph.hashslot.base_mut();
            base.tts_values[0] = key;
            base.tts_isnull[0] = key_isnull;
        }
        ::exectuples::exec_store_virtual_tuple(&mut ph.hashslot);
        let hash = ph.hashtable.hash_slot(&mut ph.hashslot)?;
        let table_mcx = ph.table_ctx.mcx();
        let (ix, isnew) = ph.hashtable.lookup(&mut ph.hashslot, hash, Some(table_mcx), mcx)?;
        let ix = ix.expect("non-spill-mode lookup always yields an entry");
        debug_assert!(isnew, "compact rows are distinct groups");
        ph.hash_ngroups_current += 1;
        let dst = ph
            .hashtable
            .entry_additional(ix)
            .expect("numtrans > 0 tables carry additional space");
        // SAFETY: both blocks are `additionalsize` bytes — the C entry's
        // zeroed additional area and the compact row's live states.
        unsafe {
            core::ptr::copy_nonoverlapping(
                ch.table.row_states(row),
                dst.as_ptr(),
                additionalsize,
            );
        }
    }
    // One post-hoc limits check (spill mode may engage for the C path's
    // subsequent inserts — exactly the safety property v1 promises).
    crate::hash_agg_check_limits(ph, aggctx, mcx)?;
    Ok(())
}

/// Read-back: the next compact group as (populated `first_slot`, pergroup).
/// Row (insertion) order; no spill refill (compact builds never spill).
/// `None` = drained. Cursor rides `ph.hashiter` (reset by the same sites the
/// C iterator's reset rides).
pub(crate) fn compact_retrieve_next<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<NonNull<AggPerGroup>>> {
    let mcx = estate.es_query_cxt;
    let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
    let row = ph.hashiter;
    let nrows = ph.compact.as_ref().expect("compact retrieve requires the table").table.nrows();
    if row >= nrows {
        return Ok(None);
    }
    ph.hashiter += 1;
    ::exectuples::exec_store_all_null_tuple(&mut ph.first_slot, mcx);
    let ch = ph.compact.as_ref().expect("compact retrieve requires the table");
    let (key, isnull) = match compact_key_datum(ch, row) {
        Some(d) => (d, false),
        None => (Datum::null(), true),
    };
    {
        let v = (ph.hash_grp_col_idx_input[0] - 1) as usize;
        let base = ph.first_slot.base_mut();
        base.tts_values[v] = key;
        base.tts_isnull[v] = isnull;
    }
    // SAFETY: the row's state block is the group's live AggPerGroup array.
    Ok(Some(unsafe { NonNull::new_unchecked(ch.table.row_states(row).cast::<AggPerGroup>()) }))
}

/// Rescan/reset hook: drop the compact table (the next build re-decides).
pub(crate) fn compact_reset(ph: &mut PerHashData<'_>) {
    ph.compact = None;
}
