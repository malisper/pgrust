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

    /// The hash-aggregate stats `show_hashagg_info` (explain.c) reads: the
    /// node-level `aggstate->hash_*` counters plus, after
    /// `ExecAggRetrieveInstrumentation`, the per-worker
    /// `shared_info->sinstrument[..]` slots. Exposed through the carrier so the
    /// EXPLAIN crate (which is below `nodeAgg` and cannot name `AggStateData` /
    /// `SharedAggInfo` / `AggregateInstrumentation`) can render them without a
    /// crate cycle. `None` when the strategy is not hashed/mixed (C's early
    /// `return`).
    fn hashagg_explain_info(&self) -> Option<HashAggExplainInfo>;
}

/// One worker's (or the leader's) hash-aggregate spill statistics — the fields
/// `show_hashagg_info` (explain.c) renders as `Batches`/`Memory Usage`/`Disk
/// Usage`. Mirrors `AggregateInstrumentation` (executor/execnodes.h), but lives
/// in `types-nodes` so the EXPLAIN crate can read it through the carrier without
/// depending on `nodeAgg`.
#[derive(Clone, Copy, Debug, Default)]
pub struct HashAggInstrument {
    /// `Size hash_mem_peak`.
    pub hash_mem_peak: usize,
    /// `uint64 hash_disk_used`.
    pub hash_disk_used: u64,
    /// `int hash_batches_used`.
    pub hash_batches_used: i32,
}

/// The snapshot `show_hashagg_info` needs from a hashed/mixed `AggState`: the
/// node-level (leader) counters plus the per-worker slots copied out of DSM by
/// `ExecAggRetrieveInstrumentation`. `worker_instrument` is empty in the
/// non-parallel case (C `shared_info == NULL`).
#[derive(Clone, Debug, Default)]
pub struct HashAggExplainInfo {
    /// `aggstate->hash_planned_partitions`.
    pub hash_planned_partitions: i32,
    /// The node-level (leader) counters.
    pub node: HashAggInstrument,
    /// `shared_info->sinstrument[0..num_workers]` (empty when `shared_info ==
    /// NULL`, i.e. non-parallel or no instrumentation).
    pub worker_instrument: alloc::vec::Vec<HashAggInstrument>,
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

/// A lifetime-free raw back-pointer to a live `AggState` — the owned-model
/// rendering of C's `fcinfo->context = (Node *) aggstate` (the `Node *` an
/// aggregate's transition/final function call frame carries so the support
/// functions `AggCheckCallContext` / `AggGetAggref` / `AggStateIsShared` /
/// `AggRegisterCallback` can recover the calling `AggState` via
/// `(AggState *) fcinfo->context`).
///
/// Modelled identically to the established [`crate::planstate::PlanStateLink`]
/// uplink (`ExprState.parent`) and the `EStateLink` / `mcx` child->parent raw
/// back-pointer idioms: a `Copy`, lifetime-free `NonNull` to the erased
/// [`AggStateLive`] trait object, with no lifetime to infect the call frame, and
/// the `&` re-derived per access. Tag-checked downcast (via
/// [`downcast_agg_state_ref`] / [`downcast_agg_state_mut`]) recovers the concrete
/// `AggStateData<'mcx>` exactly as C's `IsA(fcinfo->context, AggState)` +
/// `(AggState *) fcinfo->context` cast does -- never a `dyn Any` (the payload is
/// `'mcx`-bearing, not `'static`), never an owning box (the plan-state tree owns
/// the node).
///
/// SAFETY / liveness: the link is set into a transfn/finalfn call frame that is
/// itself owned by the `AggState` (lives in `pertrans->transfn_fcinfo`), so the
/// pointed-at `AggState` OUTLIVES -- and, because it transitively owns the frame
/// carrying this link, never moves while linked -- the frame. This is the same
/// parent-outlives-child invariant `PlanStateLink` discharges, pointing from the
/// shorter-lived call frame back to the longer-lived owning node.
#[derive(Clone, Copy, Debug)]
pub struct AggStateContextLink(core::ptr::NonNull<dyn AggStateLive<'static>>);

impl AggStateContextLink {
    /// Wrap the stable address of the live `AggState` (its erased
    /// [`AggStateLive`] view) as a `fcinfo->context` back-link. The caller must
    /// guarantee the `AggState` outlives every call frame carrying the link (it
    /// does: the `AggState` owns those frames); see the type docs. The `'mcx` is
    /// erased into the raw address.
    #[allow(unsafe_code)]
    #[inline]
    pub fn from_ref<'mcx>(aggstate: &(dyn AggStateLive<'mcx> + 'mcx)) -> Self {
        // Erase the payload lifetime into the raw address (as PlanStateLink does
        // for its `'mcx` node). A `dyn` trait object's runtime layout (data ptr +
        // vtable ptr) is lifetime-invariant, so re-tagging the lifetime parameter
        // preserves both halves of the wide pointer -- but a normal `as` cast
        // won't shorten/extend a trait object's lifetime, so transmute the wide
        // pointer (the sanctioned lifetime-erasure for a `dyn`).
        let p: *mut (dyn AggStateLive<'mcx> + 'mcx) =
            aggstate as *const (dyn AggStateLive<'mcx> + 'mcx) as *mut _;
        // SAFETY: only the (compile-time-only) lifetime parameter of the trait
        // object differs between source and target; the data/vtable wide-pointer
        // representation is identical. `aggstate` is a live reference, hence the
        // resulting pointer is non-null. Mirrors `PlanStateLink::from_ref`'s
        // `'mcx`->`'static` erasure of the owning node address.
        let p: *mut dyn AggStateLive<'static> = unsafe { core::mem::transmute(p) };
        AggStateContextLink(unsafe { core::ptr::NonNull::new_unchecked(p) })
    }

    /// Decompose the link into its raw wide-pointer image (data pointer + vtable
    /// pointer) so it can ride the lifetime-free `types_fmgr` aggregate-context
    /// thread-local channel (which sits below this crate and cannot name
    /// `AggStateContextLink`). Reconstructed by [`Self::from_raw`]. The two words
    /// are the same image `from_ref` already erases into the `NonNull`.
    #[allow(unsafe_code)]
    #[inline]
    pub fn to_raw(self) -> (*const (), *const ()) {
        let p: *mut dyn AggStateLive<'static> = self.0.as_ptr();
        // SAFETY: a `*mut dyn Trait` is a wide pointer laid out as exactly two
        // machine words (data ptr, vtable ptr); reinterpret it as a `[*const ();
        // 2]` to read those words. This is the inverse of `from_raw` and never
        // dereferences.
        let words: [*const (); 2] = unsafe { core::mem::transmute(p) };
        (words[0], words[1])
    }

    /// Rebuild the link from the raw wide-pointer image produced by
    /// [`Self::to_raw`]. The `data`/`vtable` words must come from a `to_raw` of a
    /// link whose `AggState` is still live (the executor guarantees this: the
    /// call frame carrying the image lives strictly within the AggState's
    /// transition/final dispatch).
    #[allow(unsafe_code)]
    #[inline]
    pub fn from_raw(data: *const (), vtable: *const ()) -> Self {
        let words: [*const (); 2] = [data, vtable];
        // SAFETY: `words` is the exact two-word image of a `*mut dyn
        // AggStateLive<'static>` produced by `to_raw`; reconstituting the wide
        // pointer from its two halves restores the original fat pointer. The data
        // half is non-null (it came from a live `&` in `from_ref`).
        let p: *mut dyn AggStateLive<'static> = unsafe { core::mem::transmute(words) };
        AggStateContextLink(unsafe { core::ptr::NonNull::new_unchecked(p) })
    }

    /// Momentary shared read of the live `AggState` through the back-link -- the
    /// single audited deref (mirrors [`crate::planstate::PlanStateLink::get`]).
    /// Re-derives the `&` per access at the caller-chosen lifetime; never stores
    /// a stale reference. This is the owned-model rendering of C's
    /// `(AggState *) fcinfo->context` cast.
    #[allow(unsafe_code)]
    #[inline]
    pub fn get<'a, 'mcx>(&self) -> &'a (dyn AggStateLive<'mcx> + 'mcx) {
        // Re-derive a fresh pointer from the stored raw address so the deref's
        // provenance is current (never deref a once-captured tag); mirrors
        // `PlanStateLink::get`.
        // SAFETY: `self.0` is non-null (newtype invariant) and points at the
        // owning `AggState` that outlives + never moves while linked the call
        // frame carrying this link (see the type docs' liveness invariant). The
        // lifetime is re-attached at the caller-chosen `'a`/`'mcx`; the runtime
        // representation is lifetime-invariant.
        let p: *const dyn AggStateLive<'static> = self.0.as_ptr();
        let p: *const (dyn AggStateLive<'mcx> + 'mcx) = unsafe { core::mem::transmute(p) };
        unsafe { &*p }
    }

    /// Momentary exclusive read of the live `AggState` through the back-link.
    /// Same liveness obligation as [`Self::get`]; used by the call frame's
    /// `AggRegisterCallback` (which registers against the live `curaggcontext`).
    #[allow(unsafe_code)]
    #[inline]
    pub fn get_mut<'a, 'mcx>(&mut self) -> &'a mut (dyn AggStateLive<'mcx> + 'mcx) {
        // SAFETY: as `get`, but the exclusive borrow is justified by the call
        // frame holding the only `&mut` path to the link at the call site (the
        // support function runs with the frame borrowed); the `AggState`
        // outlives and never moves while linked.
        let p: *mut dyn AggStateLive<'static> = self.0.as_ptr();
        let p: *mut (dyn AggStateLive<'mcx> + 'mcx) = unsafe { core::mem::transmute(p) };
        unsafe { &mut *p }
    }
}
