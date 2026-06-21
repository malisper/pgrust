//! `SampleScanState *` in the central `PlanStateNode` enum — the `'mcx`-safe,
//! owned carrier for `struct SampleScanState` (`backend-executor-nodeSamplescan`).
//!
//! Like [`crate::aggstate_carrier`], this is the carrier for a tree node whose
//! concrete `<Node>State` struct was relocated *out* of `types-nodes` into a
//! higher crate: `SampleScanState<'mcx>` lives in `types-samplescan` (it carries
//! a `TableScanDesc` / `Relation` / `TsmRoutine`, see the `types-samplescan`
//! module note), and `types-samplescan` depends on `types-nodes`, so a direct
//! `SampleScan(PgBox<SampleScanState>)` variant would be a
//! `types-nodes -> types-samplescan -> types-nodes` cycle.
//!
//! [`SampleScanStateLive`] is the faithful rendering of the `SampleScanState *`
//! the executor tree owns: a `PlanStateNode::SampleScan` holds the live
//! `SampleScanState<'mcx>` behind a **lifetime-preserving, tag-checked, owned**
//! trait object (the box, because the plan-state tree owns its nodes — the C
//! `makeNode` allocation). Only the concrete *type* is hidden across the
//! `nodeSamplescan -> types-nodes` edge; no lifetime is erased to `'static`. The
//! downcast back to the concrete struct is checked against a per-type tag, so the
//! unsafe reinterpretation is sound — the same discipline the `AggStateLive`
//! carrier uses.
//!
//! The trait exposes the *structural* accessors the central
//! [`crate::PlanStateNode`] dispatch needs synchronously through a borrow
//! (`tag()` / `ps()` / `ps_mut()` / `ss()`), because `SampleScanState` is a tree
//! node whose embedded `PlanState` head and `ScanState` must be reachable for
//! `ps_head()` / `as_scan_state()` / the tree walkers.

use core::any::type_name;

use crate::execnodes::{PlanStateData, ScanStateData};
use crate::nodes::NodeTag;

/// The process-stable, repo-unique tag identifying the canonical
/// `backend_executor_nodeSamplescan` `SampleScanState<'mcx>` payload
/// (`types_samplescan::SampleScanState`). `"samplscn"` in ASCII. Plays the role
/// `core::any::TypeId` plays for `dyn Any`, but works for the non-`'static`
/// (`'mcx`-bearing) `SampleScanState<'mcx>`.
pub const SAMPLE_SCAN_STATE_TAG: u64 = 0x73_61_6d_70_6c_73_63_6e;

/// The erased live `SampleScanState<'mcx>` payload — C's `SampleScanState *`
/// node, owned by the plan-state tree.
///
/// Object-safe: every method takes `&self` / `&mut self` and returns plain
/// references or values, so `dyn SampleScanStateLive<'mcx>` is a valid trait
/// object. The soundness of the downcast relies on tag uniqueness, so that
/// discipline must be honored.
pub trait SampleScanStateLive<'mcx>: core::fmt::Debug + 'mcx {
    /// The tag of the concrete `Self` behind this trait object.
    fn sample_scan_state_tag(&self) -> u64;

    /// A human-readable name for the concrete type, for panic/debug messages
    /// only (never used for the downcast check).
    fn live_type_name(&self) -> &'static str;

    /// `nodeTag(node)` — the concrete state node's tag (always
    /// `T_SampleScanState`).
    fn tag(&self) -> NodeTag;

    /// `&((PlanState *) node)` — the embedded `PlanState` head (`node->ss.ps`).
    fn ps(&self) -> &PlanStateData<'mcx>;

    /// `&mut ((PlanState *) node)`.
    fn ps_mut(&mut self) -> &mut PlanStateData<'mcx>;

    /// `(ScanState *) node` — the embedded `ScanState` (`node->ss`).
    fn ss(&self) -> &ScanStateData<'mcx>;
}

/// Marker implemented by the concrete type carried in a
/// `PlanStateNode::SampleScan`. Pairs the concrete type with the tag its
/// [`SampleScanStateLive::sample_scan_state_tag`] returns, so the
/// [`downcast_sample_scan_state_ref`] / [`downcast_sample_scan_state_mut`]
/// helpers can recover it.
///
/// SOUNDNESS CONTRACT: [`TAG`](Self::TAG) must be unique across all types ever
/// carried as a `dyn SampleScanStateLive`, and the type's
/// [`SampleScanStateLive::sample_scan_state_tag`] must return exactly this
/// `TAG`. The tag-checked downcast assumes `tag == T::TAG` implies the payload
/// really is a `T` (modulo lifetimes); a collision would let a downcast hand
/// back a `&T` aliasing bytes of a different type. Only the canonical
/// `SampleScanState<'mcx>` rides here today, using [`SAMPLE_SCAN_STATE_TAG`].
pub trait SampleScanStateTagged<'mcx>: SampleScanStateLive<'mcx> {
    /// This type's process-stable, repo-unique tag (must equal what
    /// [`SampleScanStateLive::sample_scan_state_tag`] returns for `Self`).
    const TAG: u64;
}

/// Tag-checked downcast of a `&dyn SampleScanStateLive<'mcx>` to the concrete
/// `&T` — the analogue of `<dyn Any>::downcast_ref` for the `'mcx`-bearing
/// `SampleScanState<'mcx>`.
///
/// SAFETY ARGUMENT: identical to
/// [`crate::aggstate_carrier::downcast_agg_state_ref`]. The cast
/// `*const dyn SampleScanStateLive -> *const T` is performed only after
/// confirming the payload's tag equals `T::TAG`. By the
/// [`SampleScanStateTagged`] uniqueness contract, equal tags imply the value was
/// constructed as a `T` (its lifetime parameters may differ, but lifetimes are
/// erased at runtime and do not affect layout). The data pointer of the
/// `dyn SampleScanStateLive` object points at the `T` it was unsized from, so
/// reinterpreting it as `*const T` yields a valid `&T`.
pub fn downcast_sample_scan_state_ref<'a, 'mcx, T: SampleScanStateTagged<'mcx>>(
    live: &'a (dyn SampleScanStateLive<'mcx> + 'mcx),
) -> Option<&'a T> {
    if live.sample_scan_state_tag() == T::TAG {
        // SAFETY: tag check above proved the payload is a `T`; the shared borrow
        // is preserved.
        Some(unsafe { &*(live as *const (dyn SampleScanStateLive<'mcx> + 'mcx) as *const T) })
    } else {
        None
    }
}

/// Tag-checked `&mut` downcast — see [`downcast_sample_scan_state_ref`].
pub fn downcast_sample_scan_state_mut<'a, 'mcx, T: SampleScanStateTagged<'mcx>>(
    live: &'a mut (dyn SampleScanStateLive<'mcx> + 'mcx),
) -> Option<&'a mut T> {
    if live.sample_scan_state_tag() == T::TAG {
        // SAFETY: tag check above proved the payload is a `T`; `&mut` borrow is
        // exclusive and preserved.
        Some(unsafe { &mut *(live as *mut (dyn SampleScanStateLive<'mcx> + 'mcx) as *mut T) })
    } else {
        None
    }
}

/// Default name helper used by implementors.
pub fn live_type_name_of<T>() -> &'static str {
    type_name::<T>()
}
