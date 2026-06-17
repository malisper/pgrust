//! The `Tuplestorestate *` carrier.
//!
//! `Tuplestorestate` is private to `utils/sort/tuplestore.c`; everything else
//! in PostgreSQL holds it as an opaque pointer and threads it through the
//! `tuplestore_*` API. The owned model keeps that contract: this carrier's
//! payload is type-erased and reachable only through the [`begin`] /
//! [`payload`] surface, and only the owning tuplestore unit (when it lands)
//! names the concrete engine type, downcasting with a loud panic on mismatch.
//! Consumers never inspect the payload.
//!
//! [`begin`]: Tuplestorestate::begin
//! [`payload`]: Tuplestorestate::payload

use core::any::Any;

use mcx::{Mcx, PgBox};
use types_error::PgResult;

pub struct Tuplestorestate<'mcx> {
    /// The real owned store, type-erased and context-allocated (C:
    /// `tuplestore_begin_common` pallocs the state in the caller's current
    /// context); `None` for a default-constructed (not-yet-begun) carrier —
    /// the C `NULL` `Tuplestorestate *`.
    store: Option<PgBox<'mcx, dyn Any>>,
}

impl<'mcx> Tuplestorestate<'mcx> {
    /// `tuplestore_begin_*`-shaped construction: allocate the concrete engine
    /// state in `mcx` (C: palloc in `CurrentMemoryContext`) and type-erase
    /// it. Fallible: allocating. Only the owning tuplestore unit (or a test
    /// mock standing in for it) calls this.
    pub fn begin<T: Any>(mcx: Mcx<'mcx>, store: T) -> PgResult<Self> {
        let boxed = mcx::alloc_in(mcx, store)?;
        let (ptr, alloc) = PgBox::into_raw_with_allocator(boxed);
        // Unsizing through the raw pointer: `PgBox` has no `CoerceUnsized` on
        // stable. SAFETY: `ptr` came from `into_raw_with_allocator` with the
        // same allocator; the cast only attaches the `dyn Any` vtable.
        let erased: PgBox<'mcx, dyn Any> = unsafe { PgBox::from_raw_in(ptr as *mut dyn Any, alloc) };
        Ok(Tuplestorestate {
            store: Some(erased),
        })
    }

    /// The type-erased engine state (the tuplestore owner downcasts; loud
    /// panic on mismatch is its job). `None` is the C `NULL` store.
    pub fn payload(&self) -> Option<&dyn Any> {
        self.store.as_deref()
    }

    /// Mutable [`Self::payload`].
    pub fn payload_mut(&mut self) -> Option<&mut (dyn Any + 'static)> {
        self.store.as_deref_mut()
    }
}

impl Default for Tuplestorestate<'_> {
    /// The C `Tuplestorestate *tuplestorestate = NULL` initial state.
    fn default() -> Self {
        Tuplestorestate { store: None }
    }
}

impl core::fmt::Debug for Tuplestorestate<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self.store {
            Some(_) => f.write_str("Tuplestorestate(<owned store>)"),
            None => f.write_str("Tuplestorestate(<empty>)"),
        }
    }
}

impl Clone for Tuplestorestate<'_> {
    /// An empty carrier clones freely. A live store has no C clone counterpart
    /// — `tuplestore.c` never copies a store, and C struct assignment of the
    /// holder would alias the pointer, which owned values cannot express — so
    /// cloning a live store stops loud.
    fn clone(&self) -> Self {
        match self.store {
            None => Tuplestorestate { store: None },
            Some(_) => panic!(
                "Tuplestorestate: cannot clone a live tuplestore \
                 (tuplestore.c has no copy operation; C would alias the pointer)"
            ),
        }
    }
}

/// `ReturnSetInfo` (nodes/execnodes.h) — the node passed as
/// `fcinfo->resultinfo` when calling a function that might return a set.
///
/// Field-for-field with the C struct (the value-per-call SRF keystone, #349,
/// added `econtext` + `isDone`):
///
/// ```c
/// typedef struct ReturnSetInfo
/// {
///     NodeTag       type;
///     /* values set by caller: */
///     ExprContext  *econtext;     /* context function is being called in */
///     TupleDesc     expectedDesc; /* tuple descriptor expected by caller */
///     int           allowedModes; /* bitmask: return modes caller can handle */
///     /* result status from function (but pre-initialized by caller): */
///     SetFunctionReturnMode returnMode;   /* actual return mode */
///     ExprDoneCond  isDone;       /* status for ValuePerCall mode */
///     /* fields filled by function in Materialize return mode: */
///     Tuplestorestate *setResult; /* holds the complete returned tuple set */
///     TupleDesc     setDesc;      /* actual descriptor for returned tuples */
/// } ReturnSetInfo;
/// ```
#[derive(Debug, Default)]
pub struct ReturnSetInfo<'mcx> {
    /// `ExprContext *econtext` — the per-node expression-evaluation context the
    /// function is being called in. The owned model addresses the EState's
    /// per-node `ExprContext`s by [`EcxtId`](crate::execnodes::EcxtId) (the same
    /// id `execSRF` / `SetExprState` carry), so this is the context's id rather
    /// than a borrowed pointer. `None` is the C `NULL`. Read by
    /// `init_MultiFuncCall` / `end_MultiFuncCall` to (un)register the
    /// `shutdown_MultiFuncCall` callback against the context, and by
    /// `InitMaterializedSRF` (C reaches `econtext->ecxt_per_query_memory`
    /// through it).
    pub econtext: Option<crate::execnodes::EcxtId>,
    /// `TupleDesc expectedDesc` — descriptor expected by the caller (`None` is
    /// the C `NULL`). Read by `InitMaterializedSRF` under
    /// `MAT_SRF_USE_EXPECTED_DESC` and by `internal_get_result_type`.
    pub expectedDesc: types_tuple::heaptuple::TupleDesc<'mcx>,
    /// `int allowedModes` — bitmask of return modes the caller can handle
    /// (`SFRM_*`). Set by the caller before the SRF runs; `InitMaterializedSRF`
    /// / `init_MultiFuncCall` read it.
    pub allowedModes: i32,
    /// `SetFunctionReturnMode returnMode` — actual return mode the function
    /// chose; `InitMaterializedSRF` sets this to `SFRM_Materialize`.
    pub returnMode: SetFunctionReturnMode,
    /// `ExprDoneCond isDone` — status for ValuePerCall mode. The value-per-call
    /// SRF macros set this every call: `SRF_RETURN_NEXT` →
    /// [`ExprMultipleResult`](crate::execexpr::ExprDoneCond::ExprMultipleResult),
    /// `SRF_RETURN_DONE` →
    /// [`ExprEndResult`](crate::execexpr::ExprDoneCond::ExprEndResult). The
    /// caller (`ExecMakeFunctionResultSet`) reads it back after each call to
    /// drive the row series. Pre-initialized by the caller to
    /// [`ExprSingleResult`](crate::execexpr::ExprDoneCond::ExprSingleResult).
    pub isDone: crate::execexpr::ExprDoneCond,
    /// `Tuplestorestate *setResult` — holds the complete returned tuple set.
    /// The carrier's empty state is the C `NULL` pointer.
    pub setResult: Tuplestorestate<'mcx>,
    /// `TupleDesc setDesc` — actual descriptor for returned tuples (`None`
    /// is the C `NULL`).
    pub setDesc: types_tuple::heaptuple::TupleDesc<'mcx>,
}

/// `SetFunctionReturnMode` (nodes/execnodes.h) — the set-returning-function
/// result-delivery mode bitmask values. The owned `ReturnSetInfo.returnMode`
/// holds one of these. Field-checked against execnodes.h.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(i32)]
pub enum SetFunctionReturnMode {
    /// `SFRM_ValuePerCall = 0x01` — one value returned per call.
    #[default]
    ValuePerCall = 0x01,
    /// `SFRM_Materialize = 0x02` — result set instantiated in a Tuplestore.
    Materialize = 0x02,
}

/// `SFRM_ValuePerCall` (execnodes.h) — one value returned per call.
pub const SFRM_ValuePerCall: i32 = 0x01;
/// `SFRM_Materialize` (execnodes.h) — result set instantiated in a Tuplestore.
pub const SFRM_Materialize: i32 = 0x02;
/// `SFRM_Materialize_Random` (execnodes.h) — Tuplestore needs randomAccess.
pub const SFRM_Materialize_Random: i32 = 0x04;
/// `SFRM_Materialize_Preferred` (execnodes.h) — caller prefers Tuplestore.
pub const SFRM_Materialize_Preferred: i32 = 0x08;

/// `MAT_SRF_USE_EXPECTED_DESC` (funcapi.h) — use `expectedDesc` as the SRF
/// tuple descriptor instead of resolving the result type.
pub const MAT_SRF_USE_EXPECTED_DESC: u32 = 0x01;
/// `MAT_SRF_BLESS` (funcapi.h) — "bless" the tuple descriptor (assign it a
/// typmod for a transient RECORD type).
pub const MAT_SRF_BLESS: u32 = 0x02;

/// `TypeFuncClass` (funcapi.h) — the classification of a function's result
/// type returned by `get_type_func_class` / `get_call_result_type` and
/// friends. Field-checked against funcapi.h (declaration order = the C enum's
/// implicit values 0..4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum TypeFuncClass {
    /// `TYPEFUNC_SCALAR` — scalar result type.
    Scalar = 0,
    /// `TYPEFUNC_COMPOSITE` — determinable rowtype result.
    Composite = 1,
    /// `TYPEFUNC_COMPOSITE_DOMAIN` — domain over a determinable rowtype result.
    CompositeDomain = 2,
    /// `TYPEFUNC_RECORD` — indeterminate rowtype result.
    Record = 3,
    /// `TYPEFUNC_OTHER` — bogus type, e.g. a pseudotype.
    Other = 4,
}

/// `ResolvedResultType` — the `(resultTypeId, resultTupleDesc)` pair the C
/// result-type functions return through out-parameters, packaged with the
/// `TypeFuncClass`. `internal_get_result_type` / `get_call_result_type` /
/// `get_expr_result_type` / `get_func_result_type` build this.
#[derive(Debug, Default)]
pub struct ResolvedResultType<'mcx> {
    /// The classification of the result (`TYPEFUNC_*`).
    pub class: Option<TypeFuncClass>,
    /// `*resultTypeId` — the actual datatype OID (mainly useful for scalar
    /// result types); `None` where the C caller passed a NULL out-pointer.
    pub result_type_id: Option<types_core::Oid>,
    /// `*resultTupleDesc` — the result descriptor when the result is a
    /// composite type (`None` is the C `NULL`).
    pub result_tuple_desc: types_tuple::heaptuple::TupleDesc<'mcx>,
}

/// `polymorphic_actuals` (funcapi.c, file-static struct) — the resolved actual
/// types of the polymorphic pseudo-types, threaded `&mut` through the
/// `resolve_any*_from_others` helpers. `InvalidOid` (0) marks an entry not yet
/// known. Field-for-field with the C struct.
#[derive(Debug, Default, Clone, Copy)]
pub struct PolymorphicActuals {
    /// `Oid anyelement_type` — anyelement mapping, if known.
    pub anyelement_type: types_core::Oid,
    /// `Oid anyarray_type` — anyarray mapping, if known.
    pub anyarray_type: types_core::Oid,
    /// `Oid anyrange_type` — anyrange mapping, if known.
    pub anyrange_type: types_core::Oid,
    /// `Oid anymultirange_type` — anymultirange mapping, if known.
    pub anymultirange_type: types_core::Oid,
}

/// `AttInMetadata` (funcapi.h) — per-attribute input-function metadata derived
/// from a `TupleDesc`, cached across SRF calls so `BuildTupleFromCStrings`
/// avoids redundant lookups. C embeds resolved `FmgrInfo`s; the owned model
/// (opacity-inherited rule) keeps the attribute type-input function OIDs and
/// re-resolves at call time. Field-checked against funcapi.h.
#[derive(Debug)]
pub struct AttInMetadata<'mcx> {
    /// `TupleDesc tupdesc` — the full descriptor (copy).
    pub tupdesc: types_tuple::heaptuple::TupleDesc<'mcx>,
    /// `FmgrInfo *attinfuncs` — per-attribute type-input function. C caches a
    /// resolved `FmgrInfo`; we keep the function OID and re-resolve (no
    /// invented handle).
    pub attinfuncs: mcx::PgVec<'mcx, types_core::Oid>,
    /// `Oid *attioparams` — per-attribute type I/O parameter OIDs.
    pub attioparams: mcx::PgVec<'mcx, types_core::Oid>,
    /// `int32 *atttypmods` — per-attribute typmods.
    pub atttypmods: mcx::PgVec<'mcx, i32>,
}

/// The unpacked result of `extract_variadic_args` (funcapi.c) — the
/// per-element `(value, type, isnull)` triples of a variadic argument run,
/// allocated in the caller's `Mcx`. `convert_unknown` (the C `bool`) having
/// converted `unknown`-typed literals to `text` is reflected in `types`.
#[derive(Debug)]
pub struct ExtractedVariadicArgs<'mcx> {
    /// `*values` — per-element datums (the C `Datum *args`).
    pub values: mcx::PgVec<'mcx, types_tuple::backend_access_common_heaptuple::Datum<'mcx>>,
    /// `*types` — per-element type OIDs (the C `Oid *types`).
    pub types: mcx::PgVec<'mcx, types_core::Oid>,
    /// `*nulls` — per-element null flags (the C `bool *nulls`).
    pub nulls: mcx::PgVec<'mcx, bool>,
}

/// `FuncCallContext` (funcapi.h) — cross-call state for a Set Returning
/// Function, held across fmgr calls via `flinfo->fn_extra`. Field-checked
/// against funcapi.h (the value-per-call SRF keystone, #349, modeled
/// `multi_call_memory_ctx`).
#[derive(Debug, Default)]
pub struct FuncCallContext<'mcx> {
    /// `uint64 call_cntr` — number of times called before.
    pub call_cntr: u64,
    /// `uint64 max_calls` — optional maximum number of calls.
    pub max_calls: u64,
    /// `void *user_fctx` — optional caller-private cross-call state. Genuinely
    /// heterogeneous per-SRF (the C `void *`); kept type-erased. C allocates it
    /// in `multi_call_memory_ctx`; the owned model allocates it in the
    /// per-query `'mcx` (which outlives the whole call series, so the cross-call
    /// persistence the C arena provides is preserved) and frees it with the rest
    /// of the per-query memory at end of query, equivalently to the multi-call
    /// context being deleted by `shutdown_MultiFuncCall`.
    pub user_fctx: Option<mcx::PgBox<'mcx, dyn core::any::Any>>,
    /// `AttInMetadata *attinmeta` — input metadata for `BuildTupleFromCStrings`
    /// (`None` is the C `NULL`).
    pub attinmeta: Option<AttInMetadata<'mcx>>,
    /// `MemoryContext multi_call_memory_ctx` — the context that holds all
    /// cross-call data (`SRF_FIRSTCALL_INIT` sets it; `SRF_RETURN_DONE` /
    /// `shutdown_MultiFuncCall` delete it). C makes it a child of
    /// `flinfo->fn_mcxt` and frees everything (including the `FuncCallContext`
    /// itself) by `MemoryContextDelete`. The owned model stores the *owned*
    /// child arena here; deleting it is dropping this `Option`
    /// ([`Self::shutdown`]). `None` once shut down (the C "context already
    /// deleted"). The `'mcx`-bound fields above are NOT allocated in this arena
    /// (that would be a self-borrow); they live in the per-query `'mcx`, which
    /// outlives this context.
    pub multi_call_memory_ctx: Option<mcx::MemoryContext>,
    /// `TupleDesc tuple_desc` — descriptor for `heap_form_tuple`-built tuples
    /// (`None` is the C `NULL`).
    pub tuple_desc: types_tuple::heaptuple::TupleDesc<'mcx>,
}

impl<'mcx> FuncCallContext<'mcx> {
    /// Delete the multi-call memory context (C:
    /// `MemoryContextDelete(funcctx->multi_call_memory_ctx)`) — drop the owned
    /// arena. Idempotent: a second call is the C "already NULL" no-op.
    pub fn shutdown(&mut self) {
        // Dropping the owned MemoryContext fires its reset callbacks and frees
        // its accounting subtree — the owned-model MemoryContextDelete.
        let _ = self.multi_call_memory_ctx.take();
    }
}
