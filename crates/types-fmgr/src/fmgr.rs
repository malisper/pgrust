//! Function-manager ABI vocabulary (`fmgr.h`), owned-value port.
//!
//! `FmgrInfo`, `FunctionCallInfoBaseData`, `PGFunction`, and the builtin-table
//! row. Node links become owned values, the flexible trailing `args[]` array
//! becomes `Vec<NullableDatum>`, C strings become `Option<String>`.

use std::any::Any;

use types_core::Oid;
// Datum-unification migration note: the two shim uses that remain here are the
// *irreducible fmgr-ABI edge*, deliberately left on the bare-word newtype per the
// Datum-redesign plan (Phase 2 "chief lifetime-ripple gate", deferred to the
// fmgr-core lane):
//   * `PGFunction`'s by-value **return** mints a bare machine word — the C
//     `Datum function(FunctionCallInfo)` ABI return slot. Threading `'mcx`
//     through a function-pointer type would force every builtin's signature to
//     change and is the explicitly-deferred edge; the by-value return "still
//     mints a bare word at PGFunction (irreducible)".
//   * `FunctionCallInfoBaseData.args: Vec<NullableDatum>` is the uniform call
//     frame; folding it into `{Datum<'mcx>, isnull}` ripples `'mcx` into the
//     fmgr frame ABI and ~36 consumer crates (a later coordinated wave), so it
//     stays on the shim `NullableDatum` here.
// Every other (genuine-value) Datum use in this crate — the `FmgrArg`/`FmgrOut`
// boundary value arms — has moved onto canonical `types_tuple::Datum<'mcx>`.
use types_datum::{Datum, NullableDatum};
use types_error::SoftErrorContext;

use crate::boundary::RefPayload;

pub const PG_VERSION_NUM: i32 = 180_003;
pub const FUNC_MAX_ARGS: i32 = 100;
pub const INDEX_MAX_KEYS: i32 = 32;
pub const NAMEDATALEN: i32 = 64;
pub const FLOAT8PASSBYVAL: i32 = 1;

pub const TRACK_FUNC_OFF: u8 = 0;
pub const TRACK_FUNC_PL: u8 = 1;
pub const TRACK_FUNC_ALL: u8 = 2;

/// An fmgr-1 PostgreSQL function: `Datum function(FunctionCallInfo fcinfo)`.
///
/// The C callee reads its args from `fcinfo`, *writes* `fcinfo->isnull` (and may
/// set `resultinfo`/`ref_result`), and the caller reads `isnull` back after the
/// call. The owned model borrows the frame mutably so the writeback is
/// observable — `fn(&mut FunctionCallInfoBaseData) -> Datum`. Only the fmgr
/// dispatch invokes a `PGFunction`; other holders just store it.
pub type PGFunction = Option<fn(&mut FunctionCallInfoBaseData) -> Datum>;

/// A **Result-native** fmgr-1 builtin body (the panic→Result migration target).
///
/// Identical calling convention to [`PGFunction`] — reads args / writes
/// `fcinfo->isnull` and the by-ref result through the borrowed frame — except the
/// error channel is the function's *return value* (`Err(PgError)`) rather than an
/// `ereport`-longjmp / `panic_any(PgError)`. A migrated builtin crate exposes its
/// bodies in this shape so the fmgr dispatch can call them **directly** and thread
/// the error with `?`, with no `catch_unwind` boundary.
///
/// This coexists with [`PGFunction`]: legacy (panicking) builtins keep the
/// `PGFunction` shape and are still dispatched through the `catch_unwind` bridge,
/// so bodies migrate one crate at a time without a flag day. See
/// `docs/proposals/panic-to-result-migration.md`.
pub type PgFnNative =
    fn(&mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum>;

/// Handler-private user-data for `FmgrInfo.fn_extra` (an untyped "extra space
/// for use by handler" pointer in `fmgr.h`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FmgrInfoExtra {
    /// Cached per-argument by-ref marshal plan for the interp fmgr bridge
    /// (`true` = the arg's declared type is by-ref and its registry payload must
    /// be resolved into `ref_args`). Computed once on the first call.
    pub byref_arg_plan: Option<Vec<bool>>,
}

/// The expression node `FmgrInfo.fn_expr` carries (`fmNodePtr fn_expr`).
///
/// C points this at a planner expression node (`FuncExpr`/`OpExpr`/`Const`/…)
/// from which the `get_fn_expr_*` accessors read argument/return types. The
/// unified expression-node tree is not ported; this carrier holds the one
/// fn_expr the fmgr crate itself constructs (the bytea `Const` of
/// `set_fn_opclass_options`) plus an opaque tag for an externally-supplied node
/// whose payload-reading accessors route through the `nodeFuncs` seams.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FnExpr {
    /// `makeConst(BYTEAOID, -1, InvalidOid, -1, PointerGetDatum(options),
    /// options == NULL, false)` — the opclass-options `Const`
    /// (`set_fn_opclass_options`). `None` is C's `constisnull` (options == NULL).
    ByteaConst(Option<Vec<u8>>),
    /// An externally-supplied fn_expr node (set by a caller). The tag identifies
    /// the C node kind; the payload (arg list, etc.) lives in the not-yet-ported
    /// expression tree, so the `get_call_expr_*` accessors stay loud.
    External(ExternalFnExpr),
}

/// An opaque externally-supplied fn_expr node — the planner expression node
/// `FmgrInfo.fn_expr` points at when a caller installs one.
///
/// C's `fn_expr` is a bare `Node *` pointing at the call's `FuncExpr`/`OpExpr`/…
/// in the plan tree; the `get_fn_expr_*` accessors read its result/argument
/// types out of the node's struct fields (`FuncExpr.funcresulttype`,
/// `exprType((Node*) list_nth(args, n))`, …). For a faithful polymorphic-type
/// resolution the carrier therefore holds the real expression node, carried
/// *erased* ([`types_core::fmgr::FnExprErased`]) so `types-fmgr` (a leaf on
/// `types-core`) need not name the `types-nodes` `Expr`. The fmgr owner (which
/// depends on `types-nodes`) downcasts it back and routes the field reads
/// through the `nodeFuncs` seams. `node == None` is the legacy tag-only carrier
/// (no field-bearing node available): the accessors then fall through to
/// `InvalidOid`, exactly as before.
#[derive(Clone)]
pub struct ExternalFnExpr {
    /// `nodeTag(expr)` — the C node tag (e.g. `T_FuncExpr`).
    pub tag: u32,
    /// The erased field-bearing call-expression node (`fmgr_info_set_expr`'s
    /// `Node *`), `None` when only the tag is known.
    pub node: Option<types_core::fmgr::FnExprErased>,
}

impl core::fmt::Debug for ExternalFnExpr {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ExternalFnExpr")
            .field("tag", &self.tag)
            .field("has_node", &self.node.is_some())
            .finish()
    }
}

// `FnExprErased` is an `Rc<dyn Any>` (no `PartialEq`/`Eq`); the carrier compares
// by tag only (its prior contract). `node` is identity-shared, never compared.
impl PartialEq for ExternalFnExpr {
    fn eq(&self, other: &Self) -> bool {
        self.tag == other.tag
    }
}
impl Eq for ExternalFnExpr {}

/// `FunctionCallInfoBaseData` (fmgr.h) — the call frame every fmgr-called
/// function receives. The flexible trailing `args[]` array is `Vec`; the
/// by-reference argument/result payloads are the Option-4 side channels.
///
/// Deliberately distinct from `types_nodes::fmgr::FunctionCallInfoBaseData<'mcx>`
/// (the executor frame). WONTFIX dual-home — see DESIGN_DEBT.md "two
/// `FunctionCallInfoBaseData` homes": this is the low-level, `std`,
/// lifetime-free ABI carrier (by-ref side channels, typed `PGFunction`); the
/// nodes copy is `no_std`+`'mcx`+arena/`Node` links. Neither crate deps the
/// other (both on leaf `types-core`); unifying needs a cycle and/or breaks
/// `no_std`, and they never meet (the `function_call_invoke` seam is value-based).
pub struct FunctionCallInfoBaseData {
    /// `FmgrInfo *flinfo` — the caller lookup-info frame (`None` is C's NULL).
    pub flinfo: Option<Box<FmgrInfo>>,
    /// `fmNodePtr context` — extra info about context (the C node-tag a
    /// context-demuxing callee switches on). `None` is C's NULL.
    pub context: Option<ContextNode>,
    /// The aggregate `fcinfo->context = (Node *) aggstate` back-pointer, when
    /// this frame is an aggregate transition/final function call. C carries the
    /// live `AggState` through the SAME `fcinfo->context` `fmNodePtr` as the
    /// trigger node above; in the owned model the tag-only [`ContextNode`] can't
    /// carry the dereferenceable `AggState`, so the aggregate back-pointer image
    /// rides this dedicated channel. `None` is "not an aggregate support call".
    /// Populated by `init_fcinfo` from the [`take_agg_context_link`] thread-local
    /// the executor deposits via [`AggCallContextGuard`]; the
    /// `nodeAgg-aggapi-seams` bodies reconstruct the `AggStateContextLink` from
    /// it. See [`RawAggContextLink`].
    pub agg_context: Option<RawAggContextLink>,
    /// Soft-error channel. In C the soft-error sink reaches the called function
    /// as `fcinfo->context` when it `IsA(ErrorSaveContext)` (an input function
    /// does `escontext = (Node *) fcinfo->context`). The tag-only `context`
    /// above cannot carry the real `ErrorSaveContext`, so it lives here as an
    /// owned, caller-supplied sink: the caller installs `Some(..)` to request
    /// soft handling, the called function (via its fmgr adapter) routes a
    /// recoverable error into it through `ereturn`, and the caller reads it back
    /// after the call. `None` is C's NULL escontext — the called function's
    /// `ereturn` degrades to a hard error (panic), exactly as C `ereport`s.
    pub escontext: Option<SoftErrorContext>,
    /// `fmNodePtr resultinfo` — extra info about the result (set-returning
    /// `ReturnSetInfo`). The tag-only carrier; `None` is C's NULL.
    pub resultinfo: Option<ContextNode>,
    /// `Oid fncollation`.
    pub fncollation: Oid,
    /// `bool isnull` — set by the callee, read back by the caller.
    pub isnull: bool,
    /// `short nargs`.
    pub nargs: i16,
    /// `NullableDatum args[]` — the by-value argument words + null flags.
    pub args: Vec<NullableDatum>,
    /// Option-4 by-reference argument side-channel (parallel to `args`).
    /// `ref_args[i] == Some(payload)` is C's "`args[i].value` is a pointer to
    /// `payload`"; `None` is "pass-by-value, read `args[i].value`".
    pub ref_args: Vec<Option<RefPayload>>,
    /// Option-4 by-reference *result* slot (C: a pointer-`Datum` return).
    pub ref_result: Option<RefPayload>,
    /// INTERNAL pseudo-type lane (C: `internal` = `void *` to live, caller-owned
    /// mutable state, flowing through `args[0]`). Owned `Box<dyn Any>` so the
    /// struct needs no lifetime; the caller moves it in and takes it back.
    pub internal_args: Vec<Option<Box<dyn Any>>>,
}

/// A tag-only context/result node carrier — the C `fcinfo->context` /
/// `fcinfo->resultinfo` `fmNodePtr`, of which only the `nodeTag` is consulted
/// by the in-fmgr-crate code (the payload lives in the unported node tree).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContextNode {
    /// `nodeTag(node)`.
    pub tag: u32,
    /// For a `T_CallContext` node, `CallContext.atomic`. The procedure-CALL
    /// dispatcher (`ExecuteCallStmt`) deposits the calling context's atomicity
    /// here so the call handler's `nonatomic = !castNode(CallContext,
    /// fcinfo->context)->atomic` demux (pl_handler.c) is faithful. Always
    /// `true` (the safe, atomic default) for any non-`CallContext` tag.
    pub atomic: bool,
}

thread_local! {
    /// The `nodeTag` C would have set on `fcinfo->context` for the fmgr call
    /// currently being *issued* on this backend thread, or `None`.
    ///
    /// In C, a trigger / event-trigger / procedure-CALL dispatcher sets
    /// `fcinfo->context = (Node *) &LocTriggerData` (etc.) on the call frame it
    /// builds, so the callee's `CALLED_AS_TRIGGER(fcinfo)` macro (a `nodeTag`
    /// test on `fcinfo->context`) fires. The idiomatic `function_call_invoke`
    /// seam re-resolves the function by OID and builds the call frame *inside*
    /// fmgr-core, so the issuing dispatcher cannot reach into the callee frame to
    /// stamp `context`. It instead deposits the tag here (RAII-scoped to the
    /// call), and [`init_fcinfo`](crate)-style frame construction reads it back
    /// onto `FunctionCallInfoBaseData::context`. The rich payload (the
    /// `TriggerData` relation / NEW-OLD tuples) rides the dispatcher's own
    /// per-call side-channel, which the callee reads through the trigger
    /// accessors; only the tag — the demux discriminant — needs to cross here.
    /// `(nodeTag, atomic)`; `atomic` is only meaningful for `T_CallContext` and
    /// is `true` for every other context tag (the safe default).
    static CURRENT_CALL_CONTEXT_TAG: core::cell::Cell<Option<(u32, bool)>> =
        const { core::cell::Cell::new(None) };
}

/// An opaque, lifetime-free image of the aggregate-call back-pointer C carries
/// as `fcinfo->context = (Node *) aggstate` for an aggregate transition/final
/// function (and which its support functions — `AggCheckCallContext` /
/// `AggGetAggref` / `AggStateIsShared` / `AggRegisterCallback` — recover via
/// `(AggState *) fcinfo->context`).
///
/// In the owned model the live `AggState` back-pointer is the `types_nodes`
/// `AggStateContextLink` (a `NonNull<dyn AggStateLive<'static>>` wide pointer).
/// `types-fmgr` sits BELOW `types-nodes` and cannot name that type, so this
/// crate carries it as its raw wide-pointer image: the data pointer + the
/// vtable pointer (two `usize`s). The executor (`backend-executor-nodeAgg`)
/// deposits it via [`AggCallContextGuard`] before dispatching the
/// transfn/finalfn through the by-OID `function_call_invoke` seam, and the
/// `nodeAgg-aggapi-seams` bodies — installed from `backend-executor-nodeAgg`,
/// where `AggStateContextLink` IS nameable — reconstruct the link from these
/// raw words (the same lifetime-erasure-into-raw-address discipline
/// `AggStateContextLink::from_ref`/`get` already use internally).
///
/// This is the aggregate analogue of the trigger [`ContextNode`] tag channel:
/// the dispatcher cannot reach into the callee frame fmgr-core builds, so it
/// deposits the back-pointer on a thread-local that `init_fcinfo`-style frame
/// construction reads back onto the callee frame's `agg_context`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawAggContextLink {
    /// The data pointer half of the erased `dyn AggStateLive` wide pointer.
    pub data: *const (),
    /// The vtable pointer half of the erased `dyn AggStateLive` wide pointer.
    pub vtable: *const (),
}

// SAFETY: `RawAggContextLink` is a raw, lifetime-erased back-pointer to a live
// `AggState` that is single-backend-thread-confined (it only ever crosses the
// thread-local channel within one backend's fmgr dispatch, exactly as the
// trigger tag does); it is never shared across threads. The `Send`/`Sync` marks
// mirror the established raw-back-pointer carriers (`PlanStateLink` /
// `AggStateContextLink`), which are likewise lifetime-/thread-confined.
#[allow(unsafe_code)]
unsafe impl Send for RawAggContextLink {}
#[allow(unsafe_code)]
unsafe impl Sync for RawAggContextLink {}

thread_local! {
    /// The aggregate back-pointer C would have set on `fcinfo->context =
    /// (Node *) aggstate` for the aggregate transfn/finalfn currently being
    /// *issued* on this backend thread, or `None` (a non-aggregate call).
    ///
    /// Mirrors [`CURRENT_CALL_CONTEXT_TAG`] but carries the rich aggregate
    /// back-pointer image rather than a bare node tag, because the aggregate
    /// support functions need to dereference the live `AggState` (not just
    /// switch on its tag). Deposited by [`AggCallContextGuard`] (RAII-scoped to
    /// the call) and read back by `init_fcinfo` onto
    /// [`FunctionCallInfoBaseData::agg_context`].
    static CURRENT_AGG_CONTEXT_LINK: core::cell::Cell<Option<RawAggContextLink>> =
        const { core::cell::Cell::new(None) };
}

/// RAII guard depositing the aggregate back-pointer for the transfn/finalfn
/// call about to be issued, restoring the prior value on drop (so a nested fmgr
/// call the support function itself issues observes `None`, exactly as C's
/// per-frame `fcinfo->context`). The aggregate analogue of
/// [`CallContextTagGuard`].
#[must_use]
pub struct AggCallContextGuard {
    prev: Option<RawAggContextLink>,
}

impl AggCallContextGuard {
    /// Install `link` as the current aggregate-call back-pointer (C:
    /// `fcinfo->context = (Node *) aggstate`).
    pub fn install(link: RawAggContextLink) -> Self {
        let prev = CURRENT_AGG_CONTEXT_LINK.with(|c| c.replace(Some(link)));
        AggCallContextGuard { prev }
    }
}

impl Drop for AggCallContextGuard {
    fn drop(&mut self) {
        CURRENT_AGG_CONTEXT_LINK.with(|c| c.set(self.prev));
    }
}

/// Take (consuming, leaving `None`) the aggregate back-pointer deposited for the
/// fmgr call currently being issued. fmgr-core calls this when it builds a
/// callee's call frame so the back-pointer rides onto exactly that one frame and
/// is cleared for any nested calls (same per-frame discipline as
/// [`take_call_context_tag`]).
pub fn take_agg_context_link() -> Option<RawAggContextLink> {
    CURRENT_AGG_CONTEXT_LINK.with(|c| c.replace(None))
}

/// Read (without consuming) the aggregate back-pointer deposited for the fmgr
/// call currently being issued.
pub fn current_agg_context_link() -> Option<RawAggContextLink> {
    CURRENT_AGG_CONTEXT_LINK.with(|c| c.get())
}

// ---------------------------------------------------------------------------
// EState back-channel for aggregate support functions (substrate #2)
//
// C's aggregate support functions reach the executor's `EState` through
// `aggstate->ss.ps.state` to register an ExprContext shutdown callback
// (`AggRegisterCallback` -> `RegisterExprContextCallback(aggstate->curaggcontext,
// ...)`). In the owned model the seam body that runs `AggRegisterCallback` holds
// only the call frame, no `&mut EState`, and the executor's `&mut EState` is not
// reachable from the support-fn frame. The transfn/finalfn dispatch (which DOES
// hold the `&mut EState`, and has released its borrow for the duration of the
// fmgr call) deposits a raw image of it on this thread-local — exactly the
// `RawAggContextLink` discipline, but for the EState pointer — so the
// `agg_register_callback` seam body (installed from the executor crate, which
// names `EStateData` and can re-derive the `&mut`) can register the callback into
// the live `ExprContext` pool. RAII-scoped to the one dispatch.
// ---------------------------------------------------------------------------

/// A raw, lifetime-erased back-pointer to the live `EState` for the aggregate
/// transfn/finalfn dispatch currently being issued (C: `aggstate->ss.ps.state`).
///
/// `types-fmgr` cannot name `types_nodes::EStateData`, so it carries only the
/// thin raw address (a single `*mut ()`); the executor crate, which deposits and
/// reconstructs it, casts back to `*mut EStateData<'mcx>` (the same
/// lifetime-erasure-into-raw-address discipline `EStateLink` already uses
/// internally).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RawEStateLink {
    /// The erased `*mut EStateData<'mcx>` address.
    pub data: *mut (),
}

// SAFETY: identical justification to `RawAggContextLink` — a raw,
// lifetime-erased back-pointer to the single owned, single-backend-thread-confined
// `EState`; it only ever crosses this thread-local channel within one backend's
// fmgr dispatch and is never shared across threads.
#[allow(unsafe_code)]
unsafe impl Send for RawEStateLink {}
#[allow(unsafe_code)]
unsafe impl Sync for RawEStateLink {}

thread_local! {
    /// The live `EState` back-pointer C would reach via `aggstate->ss.ps.state`
    /// for the aggregate transfn/finalfn currently being *issued* on this backend
    /// thread, or `None` (a non-aggregate call, or a call whose dispatch did not
    /// deposit it). Deposited by [`EStateCallContextGuard`] (RAII-scoped to the
    /// call) and read by the `agg_register_callback` seam body.
    static CURRENT_ESTATE_LINK: core::cell::Cell<Option<RawEStateLink>> =
        const { core::cell::Cell::new(None) };
}

/// RAII guard depositing the live-`EState` back-pointer for the aggregate
/// transfn/finalfn call about to be issued, restoring the prior value on drop
/// (so a nested fmgr call the support function itself issues observes the outer
/// value again). The `EState` analogue of [`AggCallContextGuard`].
#[must_use]
pub struct EStateCallContextGuard {
    prev: Option<RawEStateLink>,
}

impl EStateCallContextGuard {
    /// Install `link` as the current aggregate-call `EState` back-pointer. The
    /// caller MUST have released its `&mut EState` borrow for the duration of the
    /// call this guard scopes (the dispatch sites do: they pull `mcx =
    /// estate.es_query_cxt` — a `Copy` handle — before installing, so NLL has
    /// ended the `&mut estate` borrow), so the seam body's momentary re-derived
    /// `&mut` does not alias.
    pub fn install(link: RawEStateLink) -> Self {
        let prev = CURRENT_ESTATE_LINK.with(|c| c.replace(Some(link)));
        EStateCallContextGuard { prev }
    }
}

impl Drop for EStateCallContextGuard {
    fn drop(&mut self) {
        CURRENT_ESTATE_LINK.with(|c| c.set(self.prev));
    }
}

/// Read (without consuming) the live-`EState` back-pointer deposited for the
/// aggregate support call currently being issued. The `agg_register_callback`
/// seam body calls this to reach the executor's ExprContext pool.
pub fn current_estate_link() -> Option<RawEStateLink> {
    CURRENT_ESTATE_LINK.with(|c| c.get())
}

/// RAII guard depositing `tag` as the context node-tag for the fmgr call about
/// to be issued, restoring the prior value on drop (so nested fmgr calls — a
/// trigger function that itself issues calls — see the correct context: the
/// inner plain call observes `None`, exactly as a freshly-zeroed C `fcinfo`).
#[must_use]
pub struct CallContextTagGuard {
    prev: Option<(u32, bool)>,
}

impl CallContextTagGuard {
    /// Install `tag` as the current fmgr-call context node-tag (C:
    /// `fcinfo->context = (Node *) node` where `nodeTag(node) == tag`). The
    /// context is treated as atomic (the trigger / non-CALL default); for a
    /// procedure-CALL context use [`install_call`](Self::install_call).
    pub fn install(tag: u32) -> Self {
        let prev = CURRENT_CALL_CONTEXT_TAG.with(|c| c.replace(Some((tag, true))));
        CallContextTagGuard { prev }
    }

    /// Install a `T_CallContext` with `nodeTag == tag` and the calling context's
    /// `atomic` flag (C: `fcinfo->context = (Node *) callcontext` where
    /// `callcontext->atomic == atomic`). `ExecuteCallStmt` uses this so the
    /// procedure language handler's nonatomic demux is faithful.
    pub fn install_call(tag: u32, atomic: bool) -> Self {
        let prev = CURRENT_CALL_CONTEXT_TAG.with(|c| c.replace(Some((tag, atomic))));
        CallContextTagGuard { prev }
    }
}

impl Drop for CallContextTagGuard {
    fn drop(&mut self) {
        CURRENT_CALL_CONTEXT_TAG.with(|c| c.set(self.prev));
    }
}

/// Read (without consuming) the context node-tag deposited for the fmgr call
/// currently being issued — the value fmgr-core stamps onto the new call
/// frame's [`FunctionCallInfoBaseData::context`]. `None` is a plain call (C's
/// freshly-zeroed `fcinfo->context == NULL`).
pub fn current_call_context_tag() -> Option<(u32, bool)> {
    CURRENT_CALL_CONTEXT_TAG.with(|c| c.get())
}

/// Take (consuming, leaving `None`) the context node-tag deposited for the fmgr
/// call currently being issued. fmgr-core calls this when it builds a callee's
/// call frame: C's `fcinfo->context` is a per-frame field, so the tag must ride
/// onto exactly that one frame and be cleared, so any *nested* fmgr call the
/// callee itself issues — e.g. a trigger function whose body runs SPI queries
/// invoking ordinary functions — observes `None`, exactly as a freshly-zeroed C
/// `fcinfo->context`. The dispatcher's RAII [`CallContextTagGuard`] still
/// restores the prior value on drop, so a sibling trigger fired afterwards sees
/// the tag again.
pub fn take_call_context_tag() -> Option<(u32, bool)> {
    CURRENT_CALL_CONTEXT_TAG.with(|c| c.replace(None))
}

impl FunctionCallInfoBaseData {
    pub fn new(
        flinfo: Option<Box<FmgrInfo>>,
        nargs: i16,
        fncollation: Oid,
        context: Option<ContextNode>,
        resultinfo: Option<ContextNode>,
    ) -> Self {
        Self {
            flinfo,
            context,
            agg_context: None,
            escontext: None,
            resultinfo,
            fncollation,
            isnull: false,
            nargs,
            args: Vec::new(),
            ref_args: Vec::new(),
            ref_result: None,
            internal_args: Vec::new(),
        }
    }

    /// The aggregate back-pointer image on this call frame (C's
    /// `(AggState *) fcinfo->context`), `None` when not an aggregate support
    /// call. The `nodeAgg-aggapi-seams` bodies reconstruct the live
    /// `AggStateContextLink` from this.
    pub fn agg_context_link(&self) -> Option<RawAggContextLink> {
        self.agg_context
    }

    /// Set the aggregate back-pointer image on this call frame (used by
    /// `init_fcinfo` when it reads the executor-deposited thread-local back).
    pub fn set_agg_context_link(&mut self, link: RawAggContextLink) {
        self.agg_context = Some(link);
    }

    /// Install the soft-error sink (C: pointing `fcinfo->context` at an
    /// `ErrorSaveContext` node). The called function's fmgr adapter routes a
    /// recoverable error here instead of throwing.
    pub fn set_escontext(&mut self, escontext: SoftErrorContext) {
        self.escontext = Some(escontext);
    }

    /// Borrow the soft-error sink for an fmgr adapter to thread into the value
    /// core (`Some` is C's non-NULL `escontext`; `None` means throw hard).
    pub fn escontext_mut(&mut self) -> Option<&mut SoftErrorContext> {
        self.escontext.as_mut()
    }

    /// C `SOFT_ERROR_OCCURRED(fcinfo->context)`: did the called function record
    /// a recoverable error into the installed sink?
    pub fn soft_error_occurred(&self) -> bool {
        self.escontext
            .as_ref()
            .is_some_and(|c| c.error_occurred())
    }

    pub fn nargs(&self) -> usize {
        self.nargs.max(0) as usize
    }

    /// Invariant (1) check (debug-only): a by-ref payload must not be present
    /// while the argument is marked NULL.
    pub fn debug_assert_ref_null_consistency(&self) {
        for i in 0..self.nargs() {
            let ref_present = self.ref_args.get(i).map(|s| s.is_some()).unwrap_or(false);
            let is_null = self.args.get(i).map(|d| d.isnull).unwrap_or(false);
            debug_assert!(
                !(ref_present && is_null),
                "fmgr invariant (1): by-ref arg {i} has a payload but is marked NULL"
            );
        }
    }

    /// Borrow the by-reference payload for argument `index`.
    pub fn ref_arg(&self, index: usize) -> Option<&RefPayload> {
        self.ref_args.get(index).and_then(|slot| slot.as_ref())
    }

    /// Mutably borrow the by-reference payload for argument `index` (C: the
    /// `internal` aggregate transfn scribbles on `*(StateType *) args[0]` in
    /// place).
    pub fn ref_arg_mut(&mut self, index: usize) -> Option<&mut RefPayload> {
        self.ref_args.get_mut(index).and_then(|slot| slot.as_mut())
    }

    /// Take the by-reference payload for argument `index` out, leaving `None`
    /// (move an `internal` state box out of the call frame).
    pub fn take_ref_arg(&mut self, index: usize) -> Option<RefPayload> {
        self.ref_args.get_mut(index).and_then(|slot| slot.take())
    }

    /// Install a by-reference payload for argument `index`, growing `ref_args`
    /// with empty slots as needed. An aggregate **final** function restores the
    /// `internal` transition state it read (C `PG_GETARG_POINTER(0)` does not
    /// consume it) so the executor can hand the live state to the next aggregate
    /// sharing the same transition state.
    pub fn set_ref_arg(&mut self, index: usize, payload: RefPayload) {
        if self.ref_args.len() <= index {
            self.ref_args.resize_with(index + 1, || None);
        }
        self.ref_args[index] = Some(payload);
    }

    /// Store the by-reference result (C: a pointer-`Datum` return).
    pub fn set_ref_result(&mut self, payload: RefPayload) {
        self.ref_result = Some(payload);
    }

    /// Take the by-reference result back out (the boundary wrapper reads it).
    pub fn take_ref_result(&mut self) -> Option<RefPayload> {
        self.ref_result.take()
    }

    pub fn set_result_null(&mut self, isnull: bool) {
        self.isnull = isnull;
    }

    pub fn result_is_null(&self) -> bool {
        self.isnull
    }

    /// Return an argument from the owned argument vector.
    pub fn arg(&self, index: usize) -> Option<NullableDatum> {
        if index >= self.nargs() {
            return None;
        }
        self.args.get(index).copied()
    }

    /// Move an internal-state `Box` into slot `index` (C: stuffing the
    /// `internal` `void *` into `args[index]`). Grows with empty slots.
    pub fn set_internal_arg(&mut self, index: usize, state: Box<dyn Any>) {
        if self.internal_args.len() <= index {
            self.internal_args.resize_with(index + 1, || None);
        }
        self.internal_args[index] = Some(state);
    }

    /// Take the internal-state `Box` back out, leaving `None`.
    pub fn take_internal_arg(&mut self, index: usize) -> Option<Box<dyn Any>> {
        self.internal_args.get_mut(index).and_then(|slot| slot.take())
    }

    /// CHECKED `&mut T` downcast of the internal state in slot `index` (C: the
    /// unchecked cast). Panics loudly on type mismatch (a wiring bug PG would
    /// silently corrupt memory on).
    pub fn internal_arg_mut<T: Any>(&mut self, index: usize) -> Option<&mut T> {
        let slot = self.internal_args.get_mut(index)?;
        let any = slot.as_mut()?;
        match any.downcast_mut::<T>() {
            Some(t) => Some(t),
            None => panic!(
                "fmgr internal lane: downcast_mut to {} failed for internal_args[{index}]",
                core::any::type_name::<T>()
            ),
        }
    }

    /// CHECKED `&T` downcast; same loud-panic-on-mismatch contract.
    pub fn internal_arg_ref<T: Any>(&self, index: usize) -> Option<&T> {
        let slot = self.internal_args.get(index)?;
        let any = slot.as_ref()?;
        match any.downcast_ref::<T>() {
            Some(t) => Some(t),
            None => panic!(
                "fmgr internal lane: downcast_ref to {} failed for internal_args[{index}]",
                core::any::type_name::<T>()
            ),
        }
    }
}

impl Clone for FunctionCallInfoBaseData {
    /// Clones every real field; `internal_args` resets to EMPTY (the internal
    /// lane is a transient call-time borrow of caller-owned state; `Box<dyn Any>`
    /// is not `Clone` and a cloned template carries no live internal state).
    fn clone(&self) -> Self {
        Self {
            flinfo: self.flinfo.clone(),
            context: self.context,
            agg_context: self.agg_context,
            escontext: self.escontext.clone(),
            resultinfo: self.resultinfo,
            fncollation: self.fncollation,
            isnull: self.isnull,
            nargs: self.nargs,
            args: self.args.clone(),
            ref_args: self.ref_args.clone(),
            ref_result: self.ref_result.clone(),
            internal_args: Vec::new(),
        }
    }
}

impl core::fmt::Debug for FunctionCallInfoBaseData {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("FunctionCallInfoBaseData")
            .field("flinfo", &self.flinfo)
            .field("context", &self.context)
            .field("resultinfo", &self.resultinfo)
            .field("fncollation", &self.fncollation)
            .field("isnull", &self.isnull)
            .field("nargs", &self.nargs)
            .field("args", &self.args)
            .field("ref_args", &self.ref_args)
            .field("ref_result", &self.ref_result)
            .field("internal_args.len", &self.internal_args.len())
            .finish()
    }
}

/// `FmgrInfo` (`fmgr.h`) — the resolved lookup info for a function.
///
/// `Clone`/`Debug` are hand-written (not derived) because [`fn_extra_user`] is a
/// type-erased `Box<dyn Any>` that is neither `Clone` nor `Debug`. Cloning an
/// `FmgrInfo` resets that handler-private cache to `None`, which is faithful to
/// C: C never deep-copies `fn_extra` (callees share the SAME `FmgrInfo *` for
/// the cache to survive across calls; a fresh `FmgrInfo` always has
/// `fn_extra == NULL`, and the handler rebuilds the cache on its first call into
/// the new frame). This mirrors the `internal_args` reset-on-clone discipline in
/// `FunctionCallInfoBaseData::clone`.
pub struct FmgrInfo {
    /// `PGFunction fn_addr` — the resolved callable.
    pub fn_addr: PGFunction,
    /// `Oid fn_oid`.
    pub fn_oid: Oid,
    /// `short fn_nargs`.
    pub fn_nargs: i16,
    /// `bool fn_strict`.
    pub fn_strict: bool,
    /// `bool fn_retset`.
    pub fn_retset: bool,
    /// `unsigned char fn_stats`.
    pub fn_stats: u8,
    /// `void *fn_extra` — the fmgr-internal half of the handler-private cache
    /// (the per-argument by-ref marshal plan the interp fmgr bridge owns). The
    /// *generic* `void *fn_extra` a builtin owns is [`fn_extra_user`].
    pub fn_extra: Option<Box<FmgrInfoExtra>>,
    /// `void *fn_extra` — the GENERIC handler-private cache slot. C builtins
    /// stash an arbitrary owned struct here (`palloc`'d into `fn_mcxt`) and read
    /// it back, downcast, on subsequent calls within the same query: ordered-set
    /// aggregates cache `OSAPerQueryState`, regexp caches the compiled pattern,
    /// typmodin/out cache parsed modifiers, range/record typcache, etc. The
    /// owned model carries it type-erased; set via [`set_fn_extra`], read via
    /// [`fn_extra_user_ref`]/[`fn_extra_user_mut`] with a checked downcast.
    ///
    /// `Send` so the carrier matches the rest of the fmgr ABI surface; the cache
    /// is single-backend-thread-confined in practice (it never crosses threads,
    /// exactly like C's `fn_extra`).
    pub fn_extra_user: Option<Box<dyn Any + Send>>,
    /// `fmNodePtr fn_expr` — the expression node representing the call.
    pub fn_expr: Option<Box<FnExpr>>,
}

// C's `FmgrInfo.fn_mcxt` (the `MemoryContext` callees charge longer-lived
// `fn_extra` caches to) is NOT a stored field here: this repo has no
// ambient/stored allocation context — the allocation target is the `Mcx<'mcx>`
// threaded as a parameter to the `fmgr_info`/`fmgr_security_definer` family, and
// the generic [`fn_extra_user`] cache is an owned `Box` whose Rust lifetime is
// the `FmgrInfo`'s own (which the executor pins to `fn_mcxt`'s context for the
// duration the cache must live — the per-query aggregate context for ordered-set
// aggregates). A builtin that, in C, would `MemoryContextAlloc(fn_mcxt, ...)`
// instead boxes its state and hands it to [`set_fn_extra`]; the box is dropped
// when the `FmgrInfo` is (i.e. when `fn_mcxt` would be reset/deleted in C).

impl FmgrInfo {
    pub fn empty() -> Self {
        Self {
            fn_addr: None,
            fn_oid: 0,
            fn_nargs: 0,
            fn_strict: false,
            fn_retset: false,
            fn_stats: TRACK_FUNC_OFF,
            fn_extra: None,
            fn_extra_user: None,
            fn_expr: None,
        }
    }

    pub fn set_expr(&mut self, expr: Option<Box<FnExpr>>) {
        self.fn_expr = expr;
    }

    /// Store a handler-private cache value in the generic `fn_extra` slot
    /// (C: `fcinfo->flinfo->fn_extra = MemoryContextAlloc(fn_mcxt, sizeof(T));
    /// ... = state`). Replaces any prior value (the old box is dropped, as C's
    /// `fn_mcxt` reset would have freed the prior allocation). Take a `T: Any +
    /// Send`.
    pub fn set_fn_extra<T: Any + Send>(&mut self, state: T) {
        self.fn_extra_user = Some(Box::new(state));
    }

    /// `true` when the generic `fn_extra` slot is populated (C: `fn_extra !=
    /// NULL`, the "is this the first call?" test ordered-set aggregates run).
    pub fn has_fn_extra(&self) -> bool {
        self.fn_extra_user.is_some()
    }

    /// CHECKED `&T` downcast of the generic `fn_extra` cache (C: the unchecked
    /// `(T *) fcinfo->flinfo->fn_extra` cast). `None` when the slot is empty
    /// (C: `fn_extra == NULL`). Panics loudly on a type mismatch — a wiring bug
    /// C would silently corrupt memory on.
    pub fn fn_extra_user_ref<T: Any>(&self) -> Option<&T> {
        let any = self.fn_extra_user.as_ref()?;
        match any.downcast_ref::<T>() {
            Some(t) => Some(t),
            None => panic!(
                "fmgr fn_extra: downcast_ref to {} failed",
                core::any::type_name::<T>()
            ),
        }
    }

    /// CHECKED `&mut T` downcast of the generic `fn_extra` cache; same
    /// loud-panic-on-mismatch contract as [`fn_extra_user_ref`].
    pub fn fn_extra_user_mut<T: Any>(&mut self) -> Option<&mut T> {
        let any = self.fn_extra_user.as_mut()?;
        match any.downcast_mut::<T>() {
            Some(t) => Some(t),
            None => panic!(
                "fmgr fn_extra: downcast_mut to {} failed",
                core::any::type_name::<T>()
            ),
        }
    }
}

impl Clone for FmgrInfo {
    /// Clones every real field; the generic [`fn_extra_user`] cache resets to
    /// `None` (it is neither `Clone` nor meaningfully copyable — a cloned frame
    /// is a fresh `FmgrInfo` with `fn_extra == NULL`, and the handler rebuilds
    /// the cache on first call). Same discipline as
    /// `FunctionCallInfoBaseData::clone`'s `internal_args` reset.
    fn clone(&self) -> Self {
        Self {
            fn_addr: self.fn_addr,
            fn_oid: self.fn_oid,
            fn_nargs: self.fn_nargs,
            fn_strict: self.fn_strict,
            fn_retset: self.fn_retset,
            fn_stats: self.fn_stats,
            fn_extra: self.fn_extra.clone(),
            fn_extra_user: None,
            fn_expr: self.fn_expr.clone(),
        }
    }
}

impl core::fmt::Debug for FmgrInfo {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("FmgrInfo")
            .field("fn_addr", &self.fn_addr.map(|_| "<fn>"))
            .field("fn_oid", &self.fn_oid)
            .field("fn_nargs", &self.fn_nargs)
            .field("fn_strict", &self.fn_strict)
            .field("fn_retset", &self.fn_retset)
            .field("fn_stats", &self.fn_stats)
            .field("fn_extra", &self.fn_extra)
            .field("has_fn_extra_user", &self.fn_extra_user.is_some())
            .field("fn_expr", &self.fn_expr)
            .finish()
    }
}

/// `Pg_finfo_record` (`fmgr.h`) — the info record an extension's
/// `pg_finfo_<name>` returns.
#[derive(Clone, Copy, Debug)]
pub struct Pg_finfo_record {
    /// `int api_version`.
    pub api_version: i32,
}

/// `FmgrBuiltin` (`fmgr.h`) — one row of the generated built-in table.
#[derive(Clone, Debug)]
pub struct FmgrBuiltin {
    /// `Oid foid`.
    pub foid: Oid,
    /// `const char *funcName`.
    pub funcName: Option<String>,
    /// `short nargs`.
    pub nargs: i16,
    /// `bool strict`.
    pub strict: bool,
    /// `bool retset`.
    pub retset: bool,
    /// `PGFunction func`.
    pub func: PGFunction,
}

#[cfg(test)]
mod fn_extra_tests {
    use super::*;

    #[derive(Debug, PartialEq)]
    struct OsaCache {
        rescan_needed: bool,
        sort_col_type: Oid,
    }

    #[test]
    fn fn_extra_set_get_downcast_roundtrip() {
        let mut fi = FmgrInfo::empty();
        assert!(!fi.has_fn_extra());
        assert!(fi.fn_extra_user_ref::<OsaCache>().is_none());

        fi.set_fn_extra(OsaCache {
            rescan_needed: true,
            sort_col_type: 701,
        });
        assert!(fi.has_fn_extra());
        let got = fi.fn_extra_user_ref::<OsaCache>().expect("cache present");
        assert_eq!(
            got,
            &OsaCache {
                rescan_needed: true,
                sort_col_type: 701
            }
        );

        // mutate in place (handler updates its cache on a later call)
        fi.fn_extra_user_mut::<OsaCache>().unwrap().rescan_needed = false;
        assert!(!fi.fn_extra_user_ref::<OsaCache>().unwrap().rescan_needed);
    }

    #[test]
    fn fn_extra_resets_on_clone() {
        // C never deep-copies fn_extra; a cloned FmgrInfo is a fresh frame with
        // fn_extra == NULL.
        let mut fi = FmgrInfo::empty();
        fi.set_fn_extra(OsaCache {
            rescan_needed: true,
            sort_col_type: 701,
        });
        let clone = fi.clone();
        assert!(fi.has_fn_extra(), "original keeps its cache");
        assert!(!clone.has_fn_extra(), "clone is a fresh frame, cache cleared");
    }

    #[test]
    #[should_panic(expected = "downcast_ref")]
    fn fn_extra_wrong_type_panics_loudly() {
        let mut fi = FmgrInfo::empty();
        fi.set_fn_extra(OsaCache {
            rescan_needed: true,
            sort_col_type: 701,
        });
        // Wrong target type — a wiring bug C would silently corrupt memory on.
        let _ = fi.fn_extra_user_ref::<u64>();
    }
}
