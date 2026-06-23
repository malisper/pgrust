//! `backend-nodes-queryjumble` — query normalization and fingerprinting, the
//! port of `src/backend/nodes/queryjumblefuncs.c`.
//!
//! Normalization recognizes queries that differ only in their constants as
//! equivalent, tracking them under a single 64-bit `queryId`. It is implemented
//! by *fingerprinting* a query tree — selectively serializing the fields judged
//! essential (a "query jumble") and hashing the result. Constants' VALUES are
//! ignored (their locations are recorded for later parameter substitution),
//! collations are ignored, etc.
//!
//! `queryjumblefuncs.c` is two halves: (1) the per-node `_jumble#Tag` functions
//! that `gen_node_support.pl` generates plus the central `_jumbleNode`
//! tag-dispatch; and (2) the hand-written driver (`JumbleQuery`/`DoJumble`/
//! `InitJumble`/`AppendJumble`/`RecordConstLocation`/`EnableQueryId`).
//!
//! This crate ports BOTH halves onto the canonical owned
//! [`::nodes::copy_query::Query`] flat node tree. Half (1) is the
//! hand-written tag-dispatch walker (`jumble_node` / the per-`Expr`-arm jumble),
//! faithful to `_jumbleNode`'s "emit the node tag, then the significant fields,
//! then recurse" shape; half (2) is the [`JumbleState`] working state + the
//! [`jumble_query`] driver.
//!
//! Field-selection fidelity: `gen_node_support.pl` jumbles every node field
//! except those annotated `query_jumble_ignore` (and records, but does not
//! jumble, a `query_jumble_location` `ParseLoc`). This port reproduces that
//! behavior for the field families the canonical tree carries: a node's tag is
//! always emitted first; type/collation/typmod OIDs that C marks
//! `query_jumble_ignore` (e.g. `Var.vartype`, `Const.consttype`, `OpExpr`
//! result/collation OIDs) are skipped; operator/function/structural OIDs and
//! discriminants that C jumbles are appended; `Const`/`Param` locations are
//! recorded (not jumbled). The result is a real hash over the query's structure
//! that (a) is non-zero for any real query, (b) is stable across identical
//! queries, (c) differs across structurally-different queries, and (d)
//! normalizes queries that differ only in constant values — the four contract
//! properties of `JumbleQuery`.

#![allow(non_snake_case)]

extern crate alloc;

use core::sync::atomic::{AtomicBool, Ordering};

use ::nodes::copy_query::Query;
use ::nodes::nodes::NodeTag;
use ::nodes::primnodes::ParamKind;

mod state;
mod walker;

pub use state::{JumbleState, LocationLen, JUMBLE_SIZE};

use ::guc_tables::consts::{COMPUTE_QUERY_ID_OFF, COMPUTE_QUERY_ID_ON};
use ::guc_tables::vars;

/// `bool query_id_enabled` (queryjumblefuncs.c:60). True when `compute_query_id`
/// is ON or AUTO and a module requests query identifiers (via `EnableQueryId`).
/// Process-global; no extension calls `EnableQueryId` in the default build, so
/// it stays `false` and `is_query_id_enabled()` falls through to the GUC.
static QUERY_ID_ENABLED: AtomicBool = AtomicBool::new(false);

/// Read the `compute_query_id` GUC through its installed accessors.
#[inline]
fn compute_query_id() -> i32 {
    vars::compute_query_id.read()
}

/// `IsQueryIdEnabled()` (queryjumble.h:99-106). Whether query identifier
/// computation has been enabled, either directly in the GUC or by a module when
/// the setting is `auto`.
#[inline]
pub fn is_query_id_enabled() -> bool {
    let c = compute_query_id();
    if c == COMPUTE_QUERY_ID_OFF {
        return false;
    }
    if c == COMPUTE_QUERY_ID_ON {
        return true;
    }
    QUERY_ID_ENABLED.load(Ordering::Relaxed)
}

/// `EnableQueryId()` (queryjumblefuncs.c:163-169). Third-party plugins call this
/// to inform core that they require a query identifier to be computed.
pub fn enable_query_id() {
    if compute_query_id() != COMPUTE_QUERY_ID_OFF {
        QUERY_ID_ENABLED.store(true, Ordering::Relaxed);
    }
}

/// `InitJumble` (queryjumblefuncs.c:175-196). Allocate a [`JumbleState`] ready
/// to jumble.
#[inline]
pub fn init_jumble() -> JumbleState {
    JumbleState::new()
}

/// `DoJumble` (queryjumblefuncs.c:202-221). Jumble the given query tree, flush
/// any pending NULLs, reset `highest_extern_param_id` if a squashed list was
/// seen, then hash the jumble buffer to a 64-bit value.
fn do_jumble(jstate: &mut JumbleState, query: &Query) -> i64 {
    // Jumble the query tree (the `_jumbleNode(jstate, (Node *) query)` call).
    walker::jumble_query_node(jstate, query);

    // Flush any pending NULLs before doing the final hash.
    if jstate.pending_nulls > 0 {
        jstate.flush_pending_nulls();
    }

    // Squashed list found, reset highest_extern_param_id.
    if jstate.has_squashed_lists {
        jstate.highest_extern_param_id = 0;
    }

    // Process the jumble buffer and produce the hash value.
    //   DatumGetInt64(hash_any_extended(jstate->jumble, jstate->jumble_len, 0))
    hashfn::hash_bytes_extended(&jstate.jumble, 0) as i64
}

/// `JumbleQuery` (queryjumblefuncs.c:135-160). Recursively process the given
/// query tree, producing a 64-bit hash, set it into `query.queryId`, and return
/// the [`JumbleState`] used (the caller keeps it for the normalized-query-string
/// pass / the post-parse-analyze hook).
///
/// A hash of zero is replaced by `2` for a utility statement, else `1`
/// (queryjumblefuncs.c:151-157).
pub fn jumble_query(query: &mut Query) -> JumbleState {
    debug_assert!(is_query_id_enabled());

    let mut jstate = init_jumble();
    query.queryId = do_jumble(&mut jstate, query);

    // If we are unlucky enough to get a hash of zero, use 1 instead for normal
    // statements and 2 for utility queries.
    if query.queryId == 0 {
        query.queryId = if query.utilityStmt.is_some() { 2 } else { 1 };
    }

    jstate
}

/// `JumbleQuery(query)->queryId` over an immutable canonical `Query` — compute
/// the 64-bit jumble id without mutating the query (the caller stores it). This
/// is the parse-analysis entry; it applies the same zero-hash fixup as
/// [`jumble_query`].
pub fn jumble_query_compute(query: &Query) -> i64 {
    let mut jstate = init_jumble();
    let mut query_id = do_jumble(&mut jstate, query);
    if query_id == 0 {
        query_id = if query.utilityStmt.is_some() { 2 } else { 1 };
    }
    query_id
}

// ---------------------------------------------------------------------------
// Internal jumble helpers shared by the walker (the `JUMBLE_FIELD` /
// `JUMBLE_NODE` / `RecordConstLocation` primitives over the owned tree).
// ---------------------------------------------------------------------------

/// `JUMBLE_FIELD(type)` for a node tag: emit the [`NodeTag`]'s 4-byte value,
/// exactly as `_jumbleNode` does at its top (`JUMBLE_FIELD(type)`).
#[inline]
fn jumble_tag(jstate: &mut JumbleState, tag: NodeTag) {
    jstate.append_jumble(&tag.0.to_ne_bytes());
}

/// `RecordConstLocation(jstate, false, location, -1)` for a single constant —
/// the `JUMBLE_LOCATION` emit for a `Const.location` (the value is NOT jumbled).
#[inline]
fn record_const_location(jstate: &mut JumbleState, location: i32) {
    jstate.record_const_location(false, location, -1);
}

/// Param tracking: a `PARAM_EXTERN` raises `highest_extern_param_id` and its
/// location is recorded as an extern-param location, matching `_jumbleParam`'s
/// `RecordConstLocation(..., true, ...)` + highest-id bookkeeping.
#[inline]
fn jumble_param(jstate: &mut JumbleState, kind: ParamKind, paramid: i32, location: i32) {
    if kind == ParamKind::PARAM_EXTERN {
        if paramid > jstate.highest_extern_param_id {
            jstate.highest_extern_param_id = paramid;
        }
        jstate.record_const_location(true, location, -1);
    }
}

#[doc(hidden)]
pub(crate) use jumble_param as _jumble_param;
#[doc(hidden)]
pub(crate) use jumble_tag as _jumble_tag;
#[doc(hidden)]
pub(crate) use record_const_location as _record_const_location;

/// `init_seams()` — install the queryjumble seams.
pub fn init_seams() {
    use queryjumble_seams as seams;

    // `IsQueryIdEnabled()` — read the GUC + extension-enable flag.
    seams::is_query_id_enabled::set(is_query_id_enabled);

    // `JumbleQuery(query)->queryId` over the canonical field-bearing Query
    // (the simple-query / parse-analysis tree that feeds pg_stat_activity).
    seams::jumble_query_compute::set(jumble_query_compute);

    // `JumbleQuery(query)` over the portalcmds opaque `Query` token. That token
    // (DECLARE CURSOR's by-value pass-through) carries no walkable node tree —
    // its only inspected field is `commandType`. The canonical simple-query path
    // jumbles the field-bearing `copy_query::Query` directly (see
    // `jumble_query`), which is where the guc-test queryId comes from; this seam
    // exists only for the portalcmds DECLARE-CURSOR caller, which does not feed
    // pg_stat_activity. Return an empty JumbleState (no const locations) so that
    // caller's `post_parse_analyze_hook` plumbing runs unchanged.
    seams::jumble_query::set(|_query| Ok(seams_jumble_state()));
}

/// Build the seams-facing opaque `JumbleState` token (the portalcmds view).
fn seams_jumble_state() -> ::nodes::portalcmds::JumbleState {
    // The portalcmds `JumbleState` is an opaque `{ _private: () }` placeholder
    // (it is only threaded as the third arg of `post_parse_analyze_hook`, which
    // is NULL by default). Construct it via its public surface.
    ::nodes::portalcmds::JumbleState::opaque()
}

#[cfg(test)]
mod tests;
