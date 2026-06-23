//! Executor-frame registration of the materialize-mode
//! `pg_get_backend_memory_contexts()` set-returning function
//! (`utils/adt/mcxtfuncs.c`).
//!
//! The breadth-first walk over the live `MemoryContext` tree, the transient
//! `context_id` assignment, the ancestor `path` construction, the dynahash
//! relabeling, the identifier clipping, and the per-column row layout live in
//! [`mcxtfuncs::pg_get_backend_memory_contexts_core`]. That
//! core drives four `mcxtfuncs` seams: [`top_memory_context`] (the root of the
//! walk), [`context_node`] (the per-node `parent`/`firstchild`/`nextchild` tree
//! links + `name`/`ident`/`type`), [`context_stats`] (the per-context
//! `methods->stats` counters), and [`tuplestore_putvalues`] (the SRF row sink).
//!
//! This module is the executor-frame adapter:
//!
//!  * It snapshots the live process `MemoryContext` tree — reachable from the
//!    `TopMemoryContext` the `mmgr` substrate owns
//!    ([`mcxt_seams::top_memory_context`]) via
//!    [`::mcx::MemoryContext::stats_tree`], which already prunes dropped children
//!    and yields the live `parent`/`firstchild`/`nextchild` shape — into an
//!    index-addressed arena parked in a thread-local. The `mcxtfuncs` opaque
//!    [`MemoryContextRef`] handle is an index into that arena (`idx + 1`, so a
//!    handle is never the C `NULL`-equivalent 0).
//!  * It installs the three read seams against that arena snapshot and the
//!    `tuplestore_putvalues` consumer seam against the live `ReturnSetInfo` +
//!    per-query `Mcx`, then runs `InitMaterializedSRF` and dispatches the core
//!    (the executor-frame counterpart of a `fmgr_builtins[]` row, registered
//!    under the `pg_proc` OID so `srf_invoke_by_oid` resolves it).
//!
//! pgrust's accounting model is an exact-byte `Rc`-arena (`mcx`), not C's
//! malloc-block model, so the per-context counters carry what the model
//! observes: `totalspace` = the context's own live bytes (`self_used`),
//! `freespace`/`nblocks`/`freechunks` = 0 (no free-list / block bookkeeping in
//! this model), and `used_bytes` = `totalspace - freespace` = `self_used`. The
//! tree shape, names, idents, levels, and `path` arrays are exact.

extern crate alloc;

use alloc::vec::Vec;
use core::cell::RefCell;

use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::nodes::fmgr::FunctionCallInfoBaseData;
use ::nodes::funcapi::ReturnSetInfo;
use types_tuple::heaptuple::Datum;

use mcxtfuncs_seams::{
    self as mcxt_seam, McxtRow, MemoryContextCounters, MemoryContextNode, MemoryContextRef,
    MemoryContextType,
};
use funcapi_seams::{materialized_srf_putvalues, InitMaterializedSRF};

use crate::register_srf;

/// `pg_get_backend_memory_contexts()` (OID 2282).
const PG_GET_BACKEND_MEMORY_CONTEXTS: Oid = 2282;

/// `PG_GET_BACKEND_MEMORY_CONTEXTS_COLS` (mcxtfuncs.c) — 10 output columns.
const PG_GET_BACKEND_MEMORY_CONTEXTS_COLS: usize =
    mcxtfuncs::PG_GET_BACKEND_MEMORY_CONTEXTS_COLS;

/// One node of the snapshotted context tree, in breadth-first order. The tree
/// links are arena indices; `None` is the C `NULL`-equivalent.
struct NodeSnap {
    /// `context->parent` (arena index).
    parent: Option<usize>,
    /// `context->firstchild` (arena index).
    firstchild: Option<usize>,
    /// `context->nextchild` (arena index).
    nextchild: Option<usize>,
    /// `context->name` (raw bytes; `mcx` names are static UTF-8 `&str`).
    name: Option<Vec<u8>>,
    /// `context->ident` (raw bytes).
    ident: Option<Vec<u8>>,
    /// `context->type` (`T_AllocSetContext` vs `T_BumpContext`): bump-backed
    /// `mcx` contexts (e.g. tuplesort's "Caller tuples") report `"Bump"`.
    context_type: MemoryContextType,
    /// per-context counters (`methods->stats`).
    stats: MemoryContextCounters,
}

thread_local! {
    /// The snapshotted context-tree arena for the in-flight
    /// `pg_get_backend_memory_contexts` dispatch; empty between dispatches. The
    /// `mcxtfuncs` opaque handle is `idx + 1` into this vector.
    static CTX_ARENA: RefCell<Vec<NodeSnap>> = const { RefCell::new(Vec::new()) };

    /// Pointers to the live `ReturnSetInfo` + the per-query `Mcx`, parked for the
    /// duration of one core call so the installed `McxtRow` consumer seam can
    /// append into the live materialized result. Valid only while
    /// [`pg_get_backend_memory_contexts`] holds them across the synchronous core
    /// call; cleared on return.
    static MCXT_SINK: RefCell<Option<(*mut ReturnSetInfo<'static>, Mcx<'static>)>> =
        const { RefCell::new(None) };
}

/// `idx -> handle` (`idx + 1`).
#[inline]
fn handle_of(idx: usize) -> MemoryContextRef {
    idx + 1
}

/// `handle -> idx` (`handle - 1`). Handles are minted by this module, so a 0
/// handle (the C `NULL` equivalent) never reaches here.
#[inline]
fn idx_of(handle: MemoryContextRef) -> usize {
    debug_assert!(handle >= 1, "mcxtfuncs handle is never NULL/0");
    handle - 1
}

/// Register the backend-memory-contexts SRF in the executor-frame SRF table and
/// install the four `mcxtfuncs` seams the core drives.
pub(crate) fn register_pg_get_backend_memory_contexts() {
    mcxt_seam::top_memory_context::set(seam_top_memory_context);
    mcxt_seam::context_node::set(seam_context_node);
    mcxt_seam::context_stats::set(seam_context_stats);
    mcxt_seam::tuplestore_putvalues::set(put_mcxt_row);
    register_srf(
        PG_GET_BACKEND_MEMORY_CONTEXTS,
        pg_get_backend_memory_contexts,
    );
}

/// `top_memory_context` seam: the root of the snapshot (arena index 0).
fn seam_top_memory_context() -> PgResult<MemoryContextRef> {
    Ok(handle_of(0))
}

/// `context_node` seam: the tree-cursor view of one snapshotted node.
fn seam_context_node(context: MemoryContextRef) -> PgResult<MemoryContextNode> {
    CTX_ARENA.with(|arena| {
        let arena = arena.borrow();
        let n = &arena[idx_of(context)];
        Ok(MemoryContextNode {
            parent: n.parent.map(handle_of),
            firstchild: n.firstchild.map(handle_of),
            nextchild: n.nextchild.map(handle_of),
            name: n.name.clone(),
            ident: n.ident.clone(),
            // `mcx` distinguishes the bump backend (`bump.c`) from the
            // malloc-backed `AllocSet`-equivalent; surface that as the C
            // `NodeTag`-derived `type` string (e.g. tuplesort's "Caller tuples"
            // reports "Bump").
            context_type: n.context_type,
        })
    })
}

/// `context_stats` seam: the per-context counters of one snapshotted node.
fn seam_context_stats(context: MemoryContextRef) -> PgResult<MemoryContextCounters> {
    CTX_ARENA.with(|arena| {
        let arena = arena.borrow();
        Ok(arena[idx_of(context)].stats)
    })
}

/// `tuplestore_putvalues` consumer seam: turn one [`McxtRow`] into the 10-column
/// `(values, nulls)` pair (the `CStringGetTextDatum` / `Int32GetDatum` /
/// `construct_array_builtin` / `Int64GetDatum` Datum assembly mcxtfuncs.c does
/// inline) and append it to the live materialized result.
fn put_mcxt_row(row: McxtRow) -> PgResult<()> {
    MCXT_SINK.with(|cell| {
        let (rsinfo_ptr, mcx) = cell
            .borrow()
            .expect("pg_get_backend_memory_contexts: McxtRow sink outside a dispatch");
        // SAFETY: `pg_get_backend_memory_contexts` parks the live `&mut rsinfo`
        // pointer + the per-query `Mcx` across the synchronous core call and
        // clears the cell on return, so the pointer is live and uniquely
        // borrowed here.
        let rsinfo: &mut ReturnSetInfo<'static> = unsafe { &mut *rsinfo_ptr };

        let mut values: [Datum<'static>; PG_GET_BACKEND_MEMORY_CONTEXTS_COLS] =
            core::array::from_fn(|_| Datum::null());
        let mut nulls = [false; PG_GET_BACKEND_MEMORY_CONTEXTS_COLS];

        // [0] name — text (CStringGetTextDatum(name)) / NULL
        match &row.name {
            Some(n) => values[0] = text_datum(mcx, n)?,
            None => nulls[0] = true,
        }
        // [1] ident — text (clipped CStringGetTextDatum) / NULL
        match &row.ident {
            Some(id) => values[1] = text_datum(mcx, id)?,
            None => nulls[1] = true,
        }
        // [2] type — text (always present)
        values[2] = text_datum(mcx, &row.context_type)?;
        // [3] level — int4
        values[3] = Datum::from_i32(row.level);
        // [4] path — int4[] (int_list_to_array(path))
        let path_img = array_more_seams::construct_int4_array::call(&row.path)?;
        values[4] = byref_image(mcx, &path_img)?;
        // [5] total_bytes — int8
        values[5] = Datum::from_i64(row.total_bytes);
        // [6] total_nblocks — int8
        values[6] = Datum::from_i64(row.n_blocks);
        // [7] free_bytes — int8
        values[7] = Datum::from_i64(row.free_bytes);
        // [8] free_chunks — int8
        values[8] = Datum::from_i64(row.free_chunks);
        // [9] used_bytes — int8
        values[9] = Datum::from_i64(row.used_bytes);

        materialized_srf_putvalues::call(rsinfo, &values, &nulls)
    })
}

/// `CStringGetTextDatum(bytes)` → a `text` varlena `Datum` carrying the raw
/// server-encoding bytes (no lossy UTF-8 conversion).
fn text_datum<'mcx>(mcx: Mcx<'mcx>, bytes: &[u8]) -> PgResult<Datum<'mcx>> {
    // `cstring_to_text_with_len(bytes, len)` — build a `text` varlena (4-byte
    // natural header + raw payload) from server-encoding bytes; identical to
    // `bytes_to_varlena` for the `text` case.
    varlena_seams::bytes_to_varlena_v::call(mcx, bytes)
}

/// Wrap an owned array varlena image on the by-reference Datum lane — exactly
/// the pointer C's `construct_array_builtin` returned.
fn byref_image<'mcx>(mcx: Mcx<'mcx>, image: &[u8]) -> PgResult<Datum<'mcx>> {
    let mut buf = ::mcx::PgVec::new_in(mcx);
    buf.try_reserve(image.len()).map_err(|_| mcx.oom(image.len()))?;
    buf.extend_from_slice(image);
    Ok(Datum::ByRef(buf))
}

/// Snapshot the live process `MemoryContext` tree (reachable from
/// `TopMemoryContext`) into the breadth-first arena, returning the populated
/// `NodeSnap` vector. Mirrors C's intrusive `parent`/`firstchild`/`nextchild`
/// linkage, sourced from `mcx`'s `stats_tree()`.
fn snapshot_context_tree() -> Vec<NodeSnap> {
    use ::mcx::TreeStats;

    let top = mcxt_seams::top_memory_context::call();
    let tree: TreeStats = top.context().stats_tree();

    let mut arena: Vec<NodeSnap> = Vec::new();

    // Recursively flatten in pre-order, wiring parent/firstchild/nextchild as we
    // go. `to_counters` maps the `mcx` exact-byte accounting onto the C
    // `MemoryContextCounters` the SRF emits.
    fn counters(t: &TreeStats) -> MemoryContextCounters {
        if t.is_bump {
            // `bump.c` `BumpStats`: totalspace = sum of block sizes
            // (`mem_allocated`), freespace = the unused tail of the current
            // block, nblocks = block count, freechunks = 0 (bump never tracks
            // free chunks). `freespace` is derived as footprint minus the live
            // requested bytes, which is always positive while a block has a
            // non-full tail (so a populated "Caller tuples" reports free_bytes>0).
            let freespace = t.arena_footprint.saturating_sub(t.used);
            MemoryContextCounters {
                totalspace: t.arena_footprint,
                nblocks: t.nblocks,
                freespace,
                freechunks: 0,
            }
        } else {
            // Malloc-backed: the exact-byte model has no block/free bookkeeping;
            // totalspace == used, freespace/nblocks/freechunks == 0.
            MemoryContextCounters {
                totalspace: t.used,
                nblocks: 0,
                freespace: 0,
                freechunks: 0,
            }
        }
    }

    fn flatten(arena: &mut Vec<NodeSnap>, t: &::mcx::TreeStats, parent: Option<usize>) -> usize {
        let idx = arena.len();
        arena.push(NodeSnap {
            parent,
            firstchild: None,
            nextchild: None,
            name: Some(t.name.as_bytes().to_vec()),
            ident: t.ident.as_ref().map(|s| s.as_bytes().to_vec()),
            context_type: if t.is_bump {
                MemoryContextType::Bump
            } else {
                MemoryContextType::AllocSet
            },
            stats: counters(t),
        });

        // Children are linked firstchild -> nextchild -> ... in list order.
        let mut prev_child: Option<usize> = None;
        for child in &t.children {
            let child_idx = flatten(arena, child, Some(idx));
            match prev_child {
                None => arena[idx].firstchild = Some(child_idx),
                Some(p) => arena[p].nextchild = Some(child_idx),
            }
            prev_child = Some(child_idx);
        }
        idx
    }

    flatten(&mut arena, &tree, None);
    arena
}

/// `pg_get_backend_memory_contexts(PG_FUNCTION_ARGS)` (mcxtfuncs.c) over the
/// executor frame.
fn pg_get_backend_memory_contexts<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("pg_get_backend_memory_contexts: fn_mcxt set by ExecMakeTableFunctionResult");

    // C: InitMaterializedSRF(fcinfo, 0);
    InitMaterializedSRF::call(fcinfo, 0)?;
    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("InitMaterializedSRF establishes fcinfo->resultinfo");

    // Snapshot the live context tree into the per-dispatch arena the read seams
    // resolve handles against.
    let arena = snapshot_context_tree();
    CTX_ARENA.with(|a| *a.borrow_mut() = arena);

    // Park the live rsinfo + per-query Mcx for the row-sink seam.
    let rsinfo_ptr: *mut ReturnSetInfo<'static> = (rsinfo as *mut ReturnSetInfo<'mcx>).cast();
    // SAFETY: re-tag the per-query `Mcx` to `'static` for the thread-local park;
    // it is only read back inside the synchronous core call, where the per-query
    // context is live, and the cell is cleared before this frame returns.
    let mcx_static: Mcx<'static> = unsafe { core::mem::transmute::<Mcx<'mcx>, Mcx<'static>>(mcx) };
    MCXT_SINK.with(|c| *c.borrow_mut() = Some((rsinfo_ptr, mcx_static)));

    let res = mcxtfuncs::pg_get_backend_memory_contexts_core();

    // Tear down the per-dispatch state unconditionally.
    MCXT_SINK.with(|c| *c.borrow_mut() = None);
    CTX_ARENA.with(|a| a.borrow_mut().clear());
    res?;

    // C: return (Datum) 0;
    Ok(Datum::null())
}
