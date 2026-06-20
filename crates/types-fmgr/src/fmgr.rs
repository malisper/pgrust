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
    static CURRENT_CALL_CONTEXT_TAG: core::cell::Cell<Option<u32>> =
        const { core::cell::Cell::new(None) };
}

/// RAII guard depositing `tag` as the context node-tag for the fmgr call about
/// to be issued, restoring the prior value on drop (so nested fmgr calls — a
/// trigger function that itself issues calls — see the correct context: the
/// inner plain call observes `None`, exactly as a freshly-zeroed C `fcinfo`).
#[must_use]
pub struct CallContextTagGuard {
    prev: Option<u32>,
}

impl CallContextTagGuard {
    /// Install `tag` as the current fmgr-call context node-tag (C:
    /// `fcinfo->context = (Node *) node` where `nodeTag(node) == tag`).
    pub fn install(tag: u32) -> Self {
        let prev = CURRENT_CALL_CONTEXT_TAG.with(|c| c.replace(Some(tag)));
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
pub fn current_call_context_tag() -> Option<u32> {
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
pub fn take_call_context_tag() -> Option<u32> {
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
#[derive(Clone, Debug)]
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
    /// `void *fn_extra` — handler-private cache.
    pub fn_extra: Option<Box<FmgrInfoExtra>>,
    /// `fmNodePtr fn_expr` — the expression node representing the call.
    pub fn_expr: Option<Box<FnExpr>>,
}

// C's `FmgrInfo.fn_mcxt` (the `MemoryContext` callees charge longer-lived
// `fn_extra` caches to) is NOT a field here: this repo has no ambient/stored
// allocation context — the allocation target is the `Mcx<'mcx>` threaded as a
// parameter to the `fmgr_info`/`fmgr_security_definer` family. The
// `fmgr_security_definer` cache is allocated into a per-call context the handler
// builds locally, so no stored context is needed.

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
            fn_expr: None,
        }
    }

    pub fn set_expr(&mut self, expr: Option<Box<FnExpr>>) {
        self.fn_expr = expr;
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
