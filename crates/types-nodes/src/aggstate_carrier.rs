//! `AggState *` in the central `PlanStateNode` enum — the `'mcx`-safe, owned
//! carrier for `struct AggStateData` (`backend-executor-nodeAgg`).
//!
//! Every other `PlanStateNode` variant holds its concrete `<Node>State` struct
//! by value (`PgBox<SortStateData<'mcx>>`, ...), because those structs live in
//! `types-nodes`. `AggStateData<'mcx>` is the lone exception: it was relocated
//! *out* of `types-nodes` into its real owner `backend-executor-nodeAgg` (it
//! carries `LogicalTapeSet` / `HashAggSpill` / `AggStatePerAgg`, see #200), so
//! this low crate cannot name it. A direct `Agg(PgBox<AggStateData>)` variant
//! would be a `types-nodes -> nodeAgg -> types-nodes` cycle.
//!
//! [`AggStateLive`] is the faithful rendering of the `AggState *` the executor
//! tree owns: a `PlanStateNode::Agg` holds the live `AggStateData<'mcx>` behind
//! a **lifetime-preserving, tag-checked, owned** trait object. Unlike the
//! borrow-style [`crate::IndexInfoCarrier`] (which wraps a `&mut`), this carrier
//! OWNS the box, because the plan-state tree owns its nodes (the C `makeNode`
//! allocation). Only the concrete *type* is hidden across the
//! `nodeAgg -> types-nodes` edge; no lifetime is erased to `'static`
//! (`AggStateData<'mcx>` is not `'static`, so `dyn Any` is unusable). The
//! downcast back to the concrete struct is checked against a per-type tag, so
//! the unsafe reinterpretation is sound — the same discipline the
//! `IndexInfoLive` / `AmOpaque` carriers use.
//!
//! The trait also exposes the *structural* accessors the central
//! [`crate::PlanStateNode`] dispatch needs synchronously through a borrow
//! (`ps()` / `ps_mut()` / `ss()` / `tag()`), because — unlike the leaf
//! `Tuplesortstate` / `IndexInfo` carriers — `AggState` is a tree node whose
//! embedded `PlanState` head and `ScanState` must be reachable for `ps_head()` /
//! `as_scan_state()` / the tree walkers.

use core::any::type_name;

use crate::execnodes::{PlanStateData, ScanStateData};
use crate::nodes::NodeTag;

/// The process-stable, repo-unique tag identifying the canonical
/// `backend_executor_nodeAgg::AggStateData<'mcx>` payload. `"aggstate"` in
/// ASCII. Plays the role `core::any::TypeId` plays for `dyn Any`, but works for
/// the non-`'static` (`'mcx`-bearing) `AggStateData<'mcx>`.
pub const AGG_STATE_TAG: u64 = 0x6167_6773_7461_7465;

/// The erased live `AggStateData<'mcx>` payload — C's `AggState *` node, owned
/// by the plan-state tree.
///
/// Object-safe: every method takes `&self` / `&mut self` and returns plain
/// references or values, so `dyn AggStateLive<'mcx>` is a valid trait object.
/// Implementors get the tag-returning impl for free through the
/// [`AggStateTagged`] blanket impl; the soundness of the downcast relies on tag
/// uniqueness, so that discipline must be honored.
pub trait AggStateLive<'mcx>: core::fmt::Debug + 'mcx {
    /// The tag of the concrete `Self` behind this trait object.
    fn agg_state_tag(&self) -> u64;

    /// A human-readable name for the concrete type, for panic/debug messages
    /// only (never used for the downcast check).
    fn live_type_name(&self) -> &'static str;

    /// `nodeTag(node)` — the concrete state node's tag (always `T_AggState`).
    fn tag(&self) -> NodeTag;

    /// `&((PlanState *) aggstate)` — the embedded `PlanState` head
    /// (`aggstate->ss.ps`).
    fn ps(&self) -> &PlanStateData<'mcx>;

    /// `&mut ((PlanState *) aggstate)`.
    fn ps_mut(&mut self) -> &mut PlanStateData<'mcx>;

    /// `(ScanState *) aggstate` — the embedded `ScanState` (`aggstate->ss`).
    fn ss(&self) -> &ScanStateData<'mcx>;
}

/// Marker implemented by the concrete type carried in a `PlanStateNode::Agg`.
/// Pairs the concrete type with the tag its [`AggStateLive::agg_state_tag`]
/// returns, so the [`downcast_agg_state_ref`] / [`downcast_agg_state_mut`]
/// helpers can recover it.
///
/// SOUNDNESS CONTRACT: [`TAG`](Self::TAG) must be unique across all types ever
/// carried as a `dyn AggStateLive`, and the type's `AggStateLive::agg_state_tag`
/// must return exactly this `TAG`. The tag-checked downcast assumes `tag ==
/// T::TAG` implies the payload really is a `T` (modulo lifetimes); a collision
/// would let a downcast hand back a `&T` aliasing bytes of a different type.
/// Only the canonical `AggStateData<'mcx>` rides here today, using
/// [`AGG_STATE_TAG`].
pub trait AggStateTagged<'mcx>: AggStateLive<'mcx> {
    /// This type's process-stable, repo-unique tag (must equal what
    /// [`AggStateLive::agg_state_tag`] returns for `Self`).
    const TAG: u64;
}

/// Tag-checked downcast of a `&dyn AggStateLive<'mcx>` to the concrete
/// `&T` — the analogue of `<dyn Any>::downcast_ref` for the `'mcx`-bearing
/// `AggStateData<'mcx>`.
///
/// SAFETY ARGUMENT: identical to [`crate::IndexInfoCarrier::downcast_mut`]. The
/// cast `*const dyn AggStateLive -> *const T` is performed only after confirming
/// the payload's tag equals `T::TAG`. By the [`AggStateTagged`] uniqueness
/// contract, equal tags imply the value was constructed as a `T` (its lifetime
/// parameters may differ, but lifetimes are erased at runtime and do not affect
/// layout). The data pointer of the `dyn AggStateLive` object points at the `T`
/// it was unsized from, so reinterpreting it as `*const T` yields a valid `&T`.
pub fn downcast_agg_state_ref<'a, 'mcx, T: AggStateTagged<'mcx>>(
    live: &'a (dyn AggStateLive<'mcx> + 'mcx),
) -> Option<&'a T> {
    if live.agg_state_tag() == T::TAG {
        // SAFETY: tag check above proved the payload is a `T`; the shared borrow
        // is preserved.
        Some(unsafe { &*(live as *const (dyn AggStateLive<'mcx> + 'mcx) as *const T) })
    } else {
        None
    }
}

/// Tag-checked `&mut` downcast — see [`downcast_agg_state_ref`].
pub fn downcast_agg_state_mut<'a, 'mcx, T: AggStateTagged<'mcx>>(
    live: &'a mut (dyn AggStateLive<'mcx> + 'mcx),
) -> Option<&'a mut T> {
    if live.agg_state_tag() == T::TAG {
        // SAFETY: tag check above proved the payload is a `T`; `&mut` borrow is
        // exclusive and preserved.
        Some(unsafe { &mut *(live as *mut (dyn AggStateLive<'mcx> + 'mcx) as *mut T) })
    } else {
        None
    }
}

/// Default name helper used by the blanket impl.
pub fn live_type_name_of<T>() -> &'static str {
    type_name::<T>()
}
