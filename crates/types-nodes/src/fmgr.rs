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
}
