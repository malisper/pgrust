//! Port of `src/backend/lib/bipartite_match.c` (PostgreSQL 18.3).
//!
//! Hopcroft-Karp maximum cardinality algorithm for bipartite graphs.
//!
//! This implementation is based on pseudocode found at:
//!
//! <https://en.wikipedia.org/w/index.php?title=Hopcroft%E2%80%93Karp_algorithm&oldid=593898016>
//!
//! The sole in-tree caller is the planner's `extract_rollup_sets`
//! (`optimizer/plan/planner.c`), which feeds the "set k is a strict-cardinality
//! subset of set i" adjacency graph and turns the resulting maximum matching
//! into the minimum number of rollup chains.
//!
//! # Safe-slice adaptation (documented divergence from C's trusted `short **`)
//!
//! The C signature is `BipartiteMatch(int u_size, int v_size, short **adjacency)`:
//! `adjacency` is a raw, *trusted* `short **` and the bodies do unchecked
//! pointer arithmetic — `state->adjacency[u]` is `u_adj`, `u_adj[0]` is the edge
//! count, and `pair_vu[u_adj[i]]` dereferences each listed V-vertex with no
//! bounds or range check (`bipartite_match.c:121-126, 151-152, 165-167`). The
//! header also documents that the adjacency list is *owned by the caller*
//! (`BipartiteMatchFree` deliberately does not free it).
//!
//! To stay in safe Rust this port takes the parameter as `&[&[i16]]`, where each
//! row is `[count, v1, v2, ..., v_count]` (an empty row models C's `NULL` row,
//! i.e. zero edges). A safe slice physically cannot reproduce C's unchecked
//! indexing without risking a panic, so the unchecked accesses become explicit,
//! fallible checks routed through `types_error`. These checks are the
//! safe-Rust analogue of C trusting the caller; they are unreachable for the
//! sole in-tree caller (`planner.c` builds a `(num_sets + 1)`-length array with
//! every `vk` in `1..=num_sets` and `NULL` for empty rows), so observable
//! behavior is identical for all valid inputs:
//!
//! - `adjacency.len() <= u_size` (and re-checking `get(u)`): replaces C reading
//!   `state->adjacency[u]` for `u` in `0..=u_size`.
//! - `v == 0 || v > v_size`: replaces C dereferencing `pair_vu[u_adj[i]]` with
//!   an out-of-range V index.
//! - `count < 0 || row.len() < count + 1`: replaces C reading `u_adj[1..=count]`
//!   past the row's allocation.
//!
//! # Memory model
//!
//! The C original `palloc`s the whole `BipartiteMatchState` plus the four scratch
//! arrays (`pair_uv`, `pair_vu`, `distance`, `queue`) in `CurrentMemoryContext`;
//! `BipartiteMatchFree` later `pfree`s them (the adjacency list is left alone, as
//! it is the caller's). There is no private context in C.
//!
//! Here we follow the project's pilot memory model:
//!
//! * The two purely-internal working arrays — `distance` and `queue` — live in
//!   [`mcx::PgVec`]s charged to a private [`mcx::MemoryContext`] (`local_ctx`),
//!   auto-uncharged when the context drops on every return path. C has no private
//!   context (the scratch arrays sit in `CurrentMemoryContext`), but routing them
//!   through a tracked context is the pilot-model analogue of the eventual
//!   `BipartiteMatchFree` reclaiming them.
//! * The caller-facing results — `pair_uv` and `pair_vu` (read by the planner) —
//!   plus `matching`/`u_size`/`v_size` are plain owned `Vec`s handed back in
//!   [`BipartiteMatchState`], the analogue of C's state living in the caller's
//!   `CurrentMemoryContext`. They outlive `local_ctx` and are released by an
//!   ordinary `drop` ([`BipartiteMatchFree`] is therefore a no-op consuming the
//!   value).
//!
//! # Seams (genuinely external)
//!
//! `CHECK_FOR_INTERRUPTS` (`miscadmin.h` → `ProcessInterrupts`, `tcop/postgres.c`)
//! and `check_stack_depth` (`tcop/postgres.c`) are real cross-crate calls,
//! consumed from `backend-tcop-postgres-seams` (the canonical install target,
//! shared with the executor scan nodes / parallel runtime) at the exact C call
//! sites and propagated with `?`.
//!
//! ZERO `extern "C"`; soft errors via `types_error`.

#![no_std]
// `non_snake_case`: keep the exact C public names `BipartiteMatch` /
// `BipartiteMatchFree`.
#![allow(non_snake_case)]
#![forbid(unsafe_code)]

extern crate alloc;

use alloc::vec::Vec;

use mcx::{MemoryContext, PgVec};
use types_error::{PgError, PgResult};

/// The distances computed in `hk_breadth_search` can easily be seen to never
/// exceed `u_size`. Since we restrict `u_size` to be less than `SHRT_MAX`, we
/// can therefore use `SHRT_MAX` as the "infinity" distance needed as a marker.
const HK_INFINITY: i16 = i16::MAX;

/// The result of [`BipartiteMatch`].
///
/// `pair_uv` / `pair_vu` are the matching (indexed `0..=u_size` / `0..=v_size`,
/// index 0 unused, `0` meaning "unmatched"); `matching` is the cardinality.
/// These are the caller-facing fields the planner reads. The scratch arrays
/// (`distance`, `queue`) do not survive `BipartiteMatch` (they are charged to
/// and freed with the private context), so they are not part of this struct —
/// mirroring that the C consumer only ever reads `pair_uv` / `pair_vu` /
/// `matching` before calling `BipartiteMatchFree`.
///
/// This is internal idiomatic working state, never crossing a C boundary, so it
/// is intentionally not `#[repr(C)]`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BipartiteMatchState {
    pub u_size: i32,
    pub v_size: i32,
    pub matching: i32,
    pub pair_uv: Vec<i16>,
    pub pair_vu: Vec<i16>,
}

/// Given the size of U and V, where each is indexed `1..=size`, and an adjacency
/// list, perform the matching and return the resulting state.
pub fn BipartiteMatch(
    u_size: i32,
    v_size: i32,
    adjacency: &[&[i16]],
) -> PgResult<BipartiteMatchState> {
    // if (u_size < 0 || u_size >= SHRT_MAX || v_size < 0 || v_size >= SHRT_MAX)
    //     elog(ERROR, "invalid set size for BipartiteMatch");
    if u_size < 0 || u_size >= i16::MAX as i32 || v_size < 0 || v_size >= i16::MAX as i32 {
        return Err(PgError::error("invalid set size for BipartiteMatch"));
    }
    // Safe-slice analogue of C reading `state->adjacency[u]` for u in 0..=u_size.
    if adjacency.len() <= u_size as usize {
        return Err(PgError::error("adjacency list is smaller than U set"));
    }

    // local_ctx: the pilot context tracking the scratch allocations
    // (`distance`, `queue`). C has no private context here — the scratch arrays
    // sit in CurrentMemoryContext — but routing them through a tracked context
    // lets them be freed deterministically on every path when the context drops,
    // the pilot-model analogue of the eventual `BipartiteMatchFree`.
    let local_ctx = MemoryContext::new("BipartiteMatch");

    // state->pair_uv = palloc0((u_size + 1) * sizeof(short));
    // state->pair_vu = palloc0((v_size + 1) * sizeof(short));
    //
    // These are the caller-facing results, so they are plain owned Vecs (the
    // analogue of C's state in CurrentMemoryContext), not charged to local_ctx.
    let pair_uv = zeroed_vec(u_size as usize + 1)?;
    let pair_vu = zeroed_vec(v_size as usize + 1)?;

    let mut state = BipartiteMatchState {
        u_size,
        v_size,
        matching: 0,
        pair_uv,
        pair_vu,
    };

    // state->distance = palloc((u_size + 1) * sizeof(short));
    // state->queue    = palloc((u_size + 2) * sizeof(short));
    let mut scratch = Scratch {
        distance: charged_zeroed(&local_ctx, u_size as usize + 1)?,
        queue: charged_zeroed(&local_ctx, u_size as usize + 2)?,
        v_size,
    };

    // Drive the H-K loop; whatever it returns (Ok or Err) the scratch arrays are
    // released when `scratch` / `local_ctx` drop at the end of this function.
    drive(&mut state, &mut scratch, adjacency)?;

    Ok(state)
    // `scratch` (and thus `distance` / `queue`) drops here, uncharging from
    // `local_ctx`; `local_ctx` then drops with nothing outstanding — the
    // pilot-model analogue of `BipartiteMatchFree` reclaiming the scratch arrays.
}

/// Free a state returned by [`BipartiteMatch`].
///
/// C's `BipartiteMatchFree` `pfree`s the scratch arrays (the adjacency list is
/// left alone, as it is owned by the caller). Here the scratch arrays are already
/// freed inside [`BipartiteMatch`]; the remaining caller-facing state is owned,
/// so this only needs to drop it — the body is a no-op consuming the value.
pub fn BipartiteMatchFree(_state: BipartiteMatchState) {}

/// The Hopcroft-Karp working memory shared between the BFS and DFS phases. The
/// two scratch arrays mirror the C state struct's internal fields; `v_size` is
/// carried for the V-range check.
struct Scratch<'mcx> {
    distance: PgVec<'mcx, i16>,
    queue: PgVec<'mcx, i16>,
    v_size: i32,
}

/// The outer Hopcroft-Karp loop.
fn drive(
    state: &mut BipartiteMatchState,
    scratch: &mut Scratch,
    adjacency: &[&[i16]],
) -> PgResult<()> {
    // while (hk_breadth_search(state))
    while hk_breadth_search(state, scratch, adjacency)? {
        // for (u = 1; u <= u_size; u++)
        for u in 1..=state.u_size as usize {
            //     if (state->pair_uv[u] == 0)
            //         if (hk_depth_search(state, u))
            //             state->matching++;
            if state.pair_uv[u] == 0 && hk_depth_search(state, scratch, adjacency, u)? {
                state.matching += 1;
            }
        }

        // CHECK_FOR_INTERRUPTS(); /* just in case */
        backend_tcop_postgres_seams::check_for_interrupts::call()?;
    }

    Ok(())
}

/// Perform the breadth-first search step of H-K matching.
/// Returns true if successful.
fn hk_breadth_search(
    state: &mut BipartiteMatchState,
    scratch: &mut Scratch,
    adjacency: &[&[i16]],
) -> PgResult<bool> {
    let usize = state.u_size as usize;
    let mut qhead = 0usize; // we never enqueue any node more than once
    let mut qtail = 0usize; // so don't have to worry about wrapping

    scratch.distance[0] = HK_INFINITY;

    // for (u = 1; u <= usize; u++)
    for u in 1..=usize {
        if state.pair_uv[u] == 0 {
            scratch.distance[u] = 0;
            scratch.queue[qhead] = u as i16;
            qhead += 1;
        } else {
            scratch.distance[u] = HK_INFINITY;
        }
    }

    // while (qtail < qhead)
    while qtail < qhead {
        let u = scratch.queue[qtail] as usize;
        qtail += 1;

        if scratch.distance[u] < scratch.distance[0] {
            // short *u_adj = state->adjacency[u]; int i = u_adj ? u_adj[0] : 0;
            // for (; i > 0; i--)  -- descending i over u_adj[1..=count].
            let edges = adjacency_values(adjacency, u)?;
            for &v in edges.iter().rev() {
                let v = v as usize;
                // Safe-slice analogue of C dereferencing pair_vu[u_adj[i]].
                if v == 0 || v > scratch.v_size as usize {
                    return Err(PgError::error("adjacency entry is outside V set"));
                }
                let u_next = state.pair_vu[v] as usize;
                if scratch.distance[u_next] == HK_INFINITY {
                    scratch.distance[u_next] = 1 + scratch.distance[u];
                    debug_assert!(qhead < usize + 2); // Assert(qhead < usize + 2);
                    scratch.queue[qhead] = u_next as i16;
                    qhead += 1;
                }
            }
        }
    }

    // return (distance[0] != HK_INFINITY);
    Ok(scratch.distance[0] != HK_INFINITY)
}

/// Perform the depth-first search step of H-K matching.
/// Returns true if successful.
fn hk_depth_search(
    state: &mut BipartiteMatchState,
    scratch: &mut Scratch,
    adjacency: &[&[i16]],
    u: usize,
) -> PgResult<bool> {
    // if (u == 0) return true;
    if u == 0 {
        return Ok(true);
    }
    // if (distance[u] == HK_INFINITY) return false;
    if scratch.distance[u] == HK_INFINITY {
        return Ok(false);
    }
    // nextdist = distance[u] + 1;
    let nextdist = scratch.distance[u] + 1;

    // check_stack_depth();
    backend_tcop_postgres_seams::check_stack_depth::call()?;

    // Snapshot the row so the recursive call (which borrows `scratch`/`state`
    // mutably) does not alias the live edge iterator. C re-reads
    // `state->adjacency[u]` at the top of each frame and never mutates a row, so
    // the values are identical.
    let row = adjacency_values(adjacency, u)?;
    let mut edges: Vec<i16> = Vec::new();
    if edges.try_reserve(row.len()).is_err() {
        return Err(oom());
    }
    edges.extend_from_slice(row);

    // for (; i > 0; i--)  -- descending i over u_adj[1..=count].
    for v in edges.into_iter().rev() {
        let v = v as usize;
        // Safe-slice analogue of C dereferencing pair_vu[v] (= pair_vu[u_adj[i]]).
        if v == 0 || v > scratch.v_size as usize {
            return Err(PgError::error("adjacency entry is outside V set"));
        }
        let pair_vu = state.pair_vu[v] as usize;
        // if (distance[pair_vu[v]] == nextdist)
        //     if (hk_depth_search(state, pair_vu[v]))
        if scratch.distance[pair_vu] == nextdist
            && hk_depth_search(state, scratch, adjacency, pair_vu)?
        {
            // pair_vu[v] = u; pair_uv[u] = v; return true;
            state.pair_vu[v] = u as i16;
            state.pair_uv[u] = v as i16;
            return Ok(true);
        }
    }

    // distance[u] = HK_INFINITY; return false;
    scratch.distance[u] = HK_INFINITY;
    Ok(false)
}

/// The safe-slice analogue of C's `short *u_adj = state->adjacency[u]; int i =
/// u_adj ? u_adj[0] : 0;` followed by reading `u_adj[1..=i]`. Returns the `count`
/// edge values of row `u` (an empty slice for C's `NULL` / zero-edge row).
///
/// Private helper with no C counterpart: it factors out the one row access and
/// turns C's unchecked arithmetic into explicit fallible checks (see crate docs).
fn adjacency_values<'a>(adjacency: &'a [&'a [i16]], u: usize) -> PgResult<&'a [i16]> {
    let row: &[i16] = adjacency
        .get(u)
        .copied()
        .ok_or_else(|| PgError::error("adjacency list is smaller than U set"))?;
    if row.is_empty() {
        return Ok(&[]);
    }
    let count = row[0];
    if count < 0 || row.len() < count as usize + 1 {
        return Err(PgError::error("adjacency entry count is invalid"));
    }
    Ok(&row[1..=count as usize])
}

/// An owned, exactly-sized zeroed `Vec<i16>` for the caller-facing arrays
/// (`pair_uv` / `pair_vu`), OOM-safe via `try_reserve`.
fn zeroed_vec(len: usize) -> PgResult<Vec<i16>> {
    let mut v: Vec<i16> = Vec::new();
    v.try_reserve(len).map_err(|_| oom())?;
    v.resize(len, 0);
    Ok(v)
}

/// A `PgVec<i16>` of `len` zeros charged to `ctx` (the scratch `distance` /
/// `queue` arrays). C uses `palloc` (uninitialized) but writes every slot before
/// reading it; zero-init is observably identical and avoids any uninit read.
fn charged_zeroed<'mcx>(ctx: &'mcx MemoryContext, len: usize) -> PgResult<PgVec<'mcx, i16>> {
    let mut v: PgVec<'mcx, i16> = PgVec::new_in(ctx.mcx());
    v.try_reserve(len).map_err(|_| oom())?;
    v.resize(len, 0);
    Ok(v)
}

/// `palloc` out-of-memory failure as a `PgError`, mirroring C's `elog(ERROR,
/// "out of memory")` non-local exit on a failed scratch allocation.
fn oom() -> PgError {
    PgError::error("out of memory")
}

#[cfg(test)]
mod tests;
