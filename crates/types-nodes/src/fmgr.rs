//! Function-manager call-interface vocabulary (`fmgr.h`).

use alloc::vec::Vec;

use types_core::fmgr::FmgrInfo;
use types_core::Oid;
use types_datum::NullableDatum;

use crate::funcapi::ReturnSetInfo;
use crate::nodes::Node;

/// `FunctionCallInfoBaseData` (fmgr.h) — the call frame every fmgr-called
/// function receives (`FunctionCallInfo` is `FunctionCallInfoBaseData *`).
///
/// Widened (#296) field-for-field against `src/include/fmgr.h`:
///
/// ```c
/// typedef struct FunctionCallInfoBaseData
/// {
///     FmgrInfo   *flinfo;      /* ptr to lookup info used for this call */
///     fmNodePtr   context;     /* pass info about context of call */
///     fmNodePtr   resultinfo;  /* pass or return extra info about result */
///     Oid         fncollation; /* collation for function to use */
///     bool        isnull;      /* function must set true if result is NULL */
///     short       nargs;       /* # arguments actually passed */
///     NullableDatum args[FLEXIBLE_ARRAY_MEMBER];
/// } FunctionCallInfoBaseData;
/// ```
///
/// The flexible trailing `args[]` array becomes an owned `Vec<NullableDatum>`
/// (the executor gathers the per-argument result cells into it just before
/// dispatch). `flinfo`/`context` are by-value/by-reference node links: C's
/// `FmgrInfo *flinfo` is the resolved (lifetime-free, OID-keyed)
/// `types_core::fmgr::FmgrInfo` carried by value (`None` is the C NULL frame),
/// and C's `fmNodePtr context` borrows the call's context node (`None` is C's
/// NULL — set only by trigger/SRF/aggregate dispatch).
///
/// Deliberately distinct from `types_fmgr::fmgr::FunctionCallInfoBaseData` (the
/// low-level fmgr-ABI carrier). WONTFIX dual-home — see DESIGN_DEBT.md "two
/// `FunctionCallInfoBaseData` homes": this is the `no_std`+`'mcx` executor frame
/// (arena/`Node` links); the fmgr copy is `std`, lifetime-free, with by-ref side
/// channels. Neither crate deps the other (both on leaf `types-core`); unifying
/// needs a cycle and/or breaks `no_std`, and they never meet (the
/// `function_call_invoke` seam is value-based).
#[derive(Debug, Default)]
pub struct FunctionCallInfoBaseData<'mcx> {
    /// `FmgrInfo *flinfo` — the resolved lookup info this call dispatches
    /// through. The C frame points at the caller's `FmgrInfo`; the owned model
    /// carries the (OID-keyed, lifetime-free) resolution inline. `None` is the
    /// C NULL.
    pub flinfo: Option<FmgrInfo>,
    /// `fmNodePtr context` — info about the context of the call (a trigger /
    /// set-returning / aggregate dispatch node). `None` is the C NULL (an
    /// ordinary FuncExpr/OpExpr call). The borrow lives in the call's arena.
    pub context: Option<&'mcx Node<'mcx>>,
    /// `fmNodePtr resultinfo` — extra info about the result. For a
    /// set-returning call C points this at a `ReturnSetInfo` node; the owned
    /// model stores that node inline (`None` is the C `NULL`).
    pub resultinfo: Option<ReturnSetInfo<'mcx>>,
    /// `Oid fncollation` — the collation the function runs under (the
    /// `inputcollid` `InitFunctionCallInfoData` stamps onto the frame).
    pub fncollation: Oid,
    /// `bool isnull` — the callee sets this true for a NULL result; the caller
    /// reads it back after dispatch.
    pub isnull: bool,
    /// `short nargs` — the number of arguments actually passed.
    pub nargs: i16,
    /// `NullableDatum args[]` — the by-value argument words + null flags. The
    /// executor gathers its per-argument result cells into this vector
    /// immediately before dispatching the call.
    pub args: Vec<NullableDatum>,
    /// CHANNEL for C's `flinfo->fn_extra` (the value-per-call SRF keystone,
    /// #349). C holds the cross-call [`FuncCallContext`] on the *caller's*
    /// `FmgrInfo` (`flinfo->fn_extra`, a `void *`); the owned `flinfo`
    /// ([`types_core::fmgr::FmgrInfo`]) is `std`/lifetime-free and cannot name a
    /// `'mcx`-bound `FuncCallContext` (the std/`no_std` + lifetime divergence,
    /// #327 — WONTFIX-to-unify). So the `'mcx` `FuncCallContext` is threaded
    /// through *this* call frame, the place where both the callee and the
    /// `'mcx` lifetime are in scope. For a `fn_retset` function the executor
    /// keeps this frame alive across the whole row series (it lives in
    /// `SetExprState.fcinfo`), so the channel provides exactly the cross-call
    /// persistence C's `fn_extra` does. `SRF_IS_FIRSTCALL()` (C
    /// `flinfo->fn_extra == NULL`) is `fn_extra.is_none()` here. `None` is the C
    /// `NULL` (no in-flight multi-call state).
    pub fn_extra: Option<crate::funcapi::FuncCallContext<'mcx>>,
    /// CHANNEL for C's `flinfo->fn_mcxt` (the value-per-call SRF keystone,
    /// #349). C's `FmgrInfo.fn_mcxt` is the long-lived memory context callees
    /// charge cross-call state to; `init_MultiFuncCall` makes the multi-call
    /// context a child of it. The owned `flinfo` carries no stored context (this
    /// repo threads `Mcx` explicitly, not ambiently), so the per-query context
    /// the SRF's cross-call data must outlive its individual calls in is carried
    /// on the frame as the `fn_mcxt` channel. `None` is "no per-query context
    /// supplied" (the caller sets it before a `fn_retset` call, exactly as
    /// `fmgr_info` stamps `fn_mcxt`).
    pub fn_mcxt: Option<mcx::Mcx<'mcx>>,
}
