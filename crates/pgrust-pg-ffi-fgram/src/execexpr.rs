//! `ExprEvalStep` ABI — the flattened-instruction representation shared by the
//! expression *compiler* (`backend-executor-execExpr`) and the expression
//! *interpreter* (`backend-executor-execExprInterp`).
//!
//! These are exact-layout `#[repr(C)]` mirrors of `src/include/executor/execExpr.h`
//! (PostgreSQL 18.3). They cross the compiler/interpreter boundary and (when
//! JIT is in use) the C boundary, so the layout must match the C struct
//! byte-for-byte. The `ExprEvalStep` union is modelled as a real `#[repr(C)]`
//! `union` exactly as the upstream C does; field access is therefore `unsafe`
//! (genuine ABI/union interop, the one place `unsafe` is warranted here).
//!
//! The size invariant from execExpr.h is enforced by a compile-time assert:
//! `sizeof(ExprEvalStep) <= 64`.

use core::ffi::{c_char, c_int, c_void};

use crate::{
    int32, uint8, AttrNumber, CompareType, Datum, FmgrInfo, FunctionCallInfo, List, NullableDatum,
    Oid, PGFunction, TupleConversionMap, TupleDesc, TupleTableSlot, TupleTableSlotOps,
};

// Re-export the node-tree enums needed inline in the union from the nodes ABI.
// They are plain C-uint typedefs and live in backend-nodes-types::primnodes, but
// to avoid a circular dependency we mirror the typedef here (identical repr).
pub use crate::parse::VarReturningType;
pub type MinMaxOp = core::ffi::c_uint;

/// `ExecEvalSubroutine` — typical out-of-line evaluation subroutine signature.
pub type ExecEvalSubroutine =
    Option<unsafe extern "C-unwind" fn(*mut ExprState, *mut ExprEvalStep, *mut ExprContext)>;

/// `ExecEvalBoolSubroutine` — out-of-line evaluation subroutine returning bool.
pub type ExecEvalBoolSubroutine =
    Option<unsafe extern "C-unwind" fn(*mut ExprState, *mut ExprEvalStep, *mut ExprContext) -> bool>;

/// `ExprStateEvalFunc` — the function that actually evaluates an `ExprState`.
pub type ExprStateEvalFunc =
    Option<unsafe extern "C-unwind" fn(*mut ExprState, *mut ExprContext, *mut bool) -> Datum>;

// Forward-declared opaque structs (pointers only).
/// `ExprContext` — opaque here; the concrete layout lives in `funcapi`.
pub use crate::funcapi::ExprContext;

/// `ErrorSaveContext` — re-exported from the error module.
pub use crate::error::ErrorSaveContext;

/// `ExprEvalRowtypeCache` — composite-type tupdesc cache embedded in some steps.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ExprEvalRowtypeCache {
    /// TypeCacheEntry* (if `tupdesc_id != 0`) or cached TupleDesc (anon RECORD).
    pub cacheptr: *mut c_void,
    /// Last-seen tupdesc identifier, or 0.
    pub tupdesc_id: u64,
}

/// `ArrayMetaState` (utils/array.h) — cached type metadata for array
/// manipulation. Layout must match C exactly: it is embedded in
/// [`ArrayMapState`], which `ExecInitExprRec`'s `T_ArrayCoerceExpr` arm
/// allocates and the `array_map` runtime (sibling interpreter/arrayfuncs)
/// reads/writes.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ArrayMetaState {
    pub element_type: Oid,
    pub typlen: i16,
    pub typbyval: bool,
    pub typalign: c_char,
    pub typdelim: c_char,
    pub typioparam: Oid,
    pub typiofunc: Oid,
    pub proc: FmgrInfo,
}

/// `ArrayMapState` (utils/array.h) — private state needed by `array_map`
/// (the caller must provide it). `EEOP_ARRAYCOERCE` allocates this with
/// `palloc0(sizeof(ArrayMapState))`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ArrayMapState {
    pub inp_extra: ArrayMetaState,
    pub ret_extra: ArrayMetaState,
}

// ===========================================================================
// ExprState (execnodes.h) — full layout, including the compile-time fields.
// ===========================================================================

/// `ExprState` — a compiled expression: a flat program of `ExprEvalStep`s plus
/// the scratch and compile-time state used while building it.
///
/// Exact-layout mirror of `struct ExprState` (execnodes.h).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ExprState {
    pub type_: crate::NodeTag,
    pub flags: uint8,
    pub resnull: bool,
    pub resvalue: Datum,
    pub resultslot: *mut TupleTableSlot,
    pub steps: *mut ExprEvalStep,
    pub evalfunc: ExprStateEvalFunc,
    pub expr: *mut c_void, // Expr*
    pub evalfunc_private: *mut c_void,
    pub steps_len: c_int,
    pub steps_alloc: c_int,
    pub parent: *mut crate::PlanState,
    pub ext_params: *mut c_void, // ParamListInfo
    pub innermost_caseval: *mut Datum,
    pub innermost_casenull: *mut bool,
    pub innermost_domainval: *mut Datum,
    pub innermost_domainnull: *mut bool,
    pub escontext: *mut ErrorSaveContext,
}

impl ExprState {
    /// A minimal zero-initialized `ExprState` for JIT-offset/testing use, with
    /// only the parent pointer set. (Kept for `backend-jit-jit`.)
    pub fn new_for_jit(parent: *mut crate::PlanState) -> Self {
        // SAFETY: ExprState is integers/pointers; all-zero is a valid value.
        let mut s: Self = unsafe { core::mem::zeroed() };
        s.parent = parent;
        s
    }

    /// Safe read of the parent PlanState pointer.
    pub fn parent(&self) -> Option<&crate::PlanState> {
        unsafe { self.parent.as_ref() }
    }

    /// Set the parent PlanState pointer.
    pub fn set_parent(&mut self, parent: *mut crate::PlanState) {
        self.parent = parent;
    }
}

/// `ProjectionInfo` — embeds an `ExprState` performing a whole projection.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ProjectionInfo {
    pub type_: crate::NodeTag,
    pub pi_state: ExprState,
    pub pi_exprContext: *mut ExprContext,
}

// ===========================================================================
// Per-op union member structs (execExpr.h: `union { ... } d`)
// ===========================================================================

/// EEOP_INNER/OUTER/SCAN/OLD/NEW_FETCHSOME
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepFetch {
    pub last_var: c_int,
    pub fixed: bool,
    pub known_desc: TupleDesc,
    pub kind: *const TupleTableSlotOps,
}

/// EEOP_INNER/OUTER/SCAN/OLD/NEW_[SYS]VAR
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepVar {
    pub attnum: c_int,
    pub vartype: Oid,
    pub varreturningtype: VarReturningType,
}

/// EEOP_WHOLEROW
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepWholerow {
    pub var: *mut c_void, // Var*
    pub first: bool,
    pub slow: bool,
    pub tupdesc: TupleDesc,
    pub junkFilter: *mut c_void, // JunkFilter*
}

/// EEOP_ASSIGN_*_VAR
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepAssignVar {
    pub resultnum: c_int,
    pub attnum: c_int,
}

/// EEOP_ASSIGN_TMP[_MAKE_RO]
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepAssignTmp {
    pub resultnum: c_int,
}

/// EEOP_RETURNINGEXPR
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepReturningExpr {
    pub nullflag: uint8,
    pub jumpdone: c_int,
}

/// EEOP_CONST
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepConstval {
    pub value: Datum,
    pub isnull: bool,
}

/// EEOP_FUNCEXPR_* / NULLIF / DISTINCT
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepFunc {
    pub finfo: *mut FmgrInfo,
    pub fcinfo_data: FunctionCallInfo,
    pub fn_addr: PGFunction,
    pub nargs: c_int,
    pub make_ro: bool,
}

/// EEOP_BOOL_*_STEP
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepBoolexpr {
    pub anynull: *mut bool,
    pub jumpdone: c_int,
}

/// EEOP_QUAL
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepQualexpr {
    pub jumpdone: c_int,
}

/// EEOP_JUMP[_CONDITION]
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepJump {
    pub jumpdone: c_int,
}

/// EEOP_NULLTEST_ROWIS[NOT]NULL
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepNulltestRow {
    pub rowcache: ExprEvalRowtypeCache,
}

/// EEOP_PARAM_EXEC/EXTERN and EEOP_PARAM_SET
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepParam {
    pub paramid: c_int,
    pub paramtype: Oid,
}

/// EEOP_PARAM_CALLBACK
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepCparam {
    pub paramfunc: ExecEvalSubroutine,
    pub paramarg: *mut c_void,
    pub paramarg2: *mut c_void,
    pub paramid: c_int,
    pub paramtype: Oid,
}

/// EEOP_CASE_TESTVAL/DOMAIN_TESTVAL
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepCasetest {
    pub value: *mut Datum,
    pub isnull: *mut bool,
}

/// EEOP_MAKE_READONLY
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepMakeReadonly {
    pub value: *mut Datum,
    pub isnull: *mut bool,
}

/// EEOP_IOCOERCE
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepIocoerce {
    pub finfo_out: *mut FmgrInfo,
    pub fcinfo_data_out: FunctionCallInfo,
    pub finfo_in: *mut FmgrInfo,
    pub fcinfo_data_in: FunctionCallInfo,
}

/// EEOP_SQLVALUEFUNCTION
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepSqlvaluefunction {
    pub svf: *mut c_void, // SQLValueFunction*
}

/// EEOP_NEXTVALUEEXPR
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepNextvalueexpr {
    pub seqid: Oid,
    pub seqtypid: Oid,
}

/// EEOP_ARRAYEXPR
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepArrayexpr {
    pub elemvalues: *mut Datum,
    pub elemnulls: *mut bool,
    pub nelems: c_int,
    pub elemtype: Oid,
    pub elemlength: i16,
    pub elembyval: bool,
    pub elemalign: c_char,
    pub multidims: bool,
}

/// EEOP_ARRAYCOERCE
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepArraycoerce {
    pub elemexprstate: *mut ExprState,
    pub resultelemtype: Oid,
    pub amstate: *mut c_void, // ArrayMapState*
}

/// EEOP_ROW
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepRow {
    pub tupdesc: TupleDesc,
    pub elemvalues: *mut Datum,
    pub elemnulls: *mut bool,
}

/// EEOP_ROWCOMPARE_STEP
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepRowcompareStep {
    pub finfo: *mut FmgrInfo,
    pub fcinfo_data: FunctionCallInfo,
    pub fn_addr: PGFunction,
    pub jumpnull: c_int,
    pub jumpdone: c_int,
}

/// EEOP_ROWCOMPARE_FINAL
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepRowcompareFinal {
    pub cmptype: CompareType,
}

/// EEOP_MINMAX
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepMinmax {
    pub values: *mut Datum,
    pub nulls: *mut bool,
    pub nelems: c_int,
    pub op: MinMaxOp,
    pub finfo: *mut FmgrInfo,
    pub fcinfo_data: FunctionCallInfo,
}

/// EEOP_FIELDSELECT
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepFieldselect {
    pub fieldnum: AttrNumber,
    pub resulttype: Oid,
    pub rowcache: ExprEvalRowtypeCache,
}

/// EEOP_FIELDSTORE_DEFORM / FIELDSTORE_FORM
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepFieldstore {
    pub fstore: *mut c_void, // FieldStore*
    pub rowcache: *mut ExprEvalRowtypeCache,
    pub values: *mut Datum,
    pub nulls: *mut bool,
    pub ncolumns: c_int,
}

/// EEOP_SBSREF_SUBSCRIPTS
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepSbsrefSubscript {
    pub subscriptfunc: ExecEvalBoolSubroutine,
    pub state: *mut SubscriptingRefState,
    pub jumpdone: c_int,
}

/// EEOP_SBSREF_OLD / ASSIGN / FETCH
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepSbsref {
    pub subscriptfunc: ExecEvalSubroutine,
    pub state: *mut SubscriptingRefState,
}

/// EEOP_DOMAIN_NOTNULL / DOMAIN_CHECK
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepDomaincheck {
    pub constraintname: *mut c_char,
    pub checkvalue: *mut Datum,
    pub checknull: *mut bool,
    pub resulttype: Oid,
    pub escontext: *mut ErrorSaveContext,
}

/// EEOP_HASHDATUM_SET_INITVAL
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepHashdatumInitvalue {
    pub init_value: Datum,
}

/// EEOP_HASHDATUM_(FIRST|NEXT32)[_STRICT]
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepHashdatum {
    pub finfo: *mut FmgrInfo,
    pub fcinfo_data: FunctionCallInfo,
    pub fn_addr: PGFunction,
    pub jumpdone: c_int,
    pub iresult: *mut NullableDatum,
}

/// EEOP_CONVERT_ROWTYPE
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepConvertRowtype {
    pub inputtype: Oid,
    pub outputtype: Oid,
    pub incache: *mut ExprEvalRowtypeCache,
    pub outcache: *mut ExprEvalRowtypeCache,
    pub map: *mut TupleConversionMap,
}

/// EEOP_SCALARARRAYOP
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepScalararrayop {
    pub element_type: Oid,
    pub useOr: bool,
    pub typlen: i16,
    pub typbyval: bool,
    pub typalign: c_char,
    pub finfo: *mut FmgrInfo,
    pub fcinfo_data: FunctionCallInfo,
    pub fn_addr: PGFunction,
}

/// EEOP_HASHED_SCALARARRAYOP
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepHashedscalararrayop {
    pub has_nulls: bool,
    pub inclause: bool,
    pub elements_tab: *mut c_void, // ScalarArrayOpExprHashTable*
    pub finfo: *mut FmgrInfo,
    pub fcinfo_data: FunctionCallInfo,
    pub saop: *mut c_void, // ScalarArrayOpExpr*
}

/// EEOP_XMLEXPR
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepXmlexpr {
    pub xexpr: *mut c_void, // XmlExpr*
    pub named_argvalue: *mut Datum,
    pub named_argnull: *mut bool,
    pub argvalue: *mut Datum,
    pub argnull: *mut bool,
}

/// EEOP_JSON_CONSTRUCTOR
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepJsonConstructor {
    pub jcstate: *mut JsonConstructorExprState,
}

/// EEOP_AGGREF
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepAggref {
    pub aggno: c_int,
}

/// EEOP_GROUPING_FUNC
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepGroupingFunc {
    pub clauses: *mut List,
}

/// EEOP_WINDOW_FUNC
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepWindowFunc {
    pub wfstate: *mut c_void, // WindowFuncExprState*
}

/// EEOP_SUBPLAN
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepSubplan {
    pub sstate: *mut c_void, // SubPlanState*
}

/// EEOP_AGG_*DESERIALIZE
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepAggDeserialize {
    pub fcinfo_data: FunctionCallInfo,
    pub jumpnull: c_int,
}

/// EEOP_AGG_STRICT_INPUT_CHECK_NULLS / STRICT_INPUT_CHECK_ARGS
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepAggStrictInputCheck {
    pub args: *mut NullableDatum,
    pub nulls: *mut bool,
    pub nargs: c_int,
    pub jumpnull: c_int,
}

/// EEOP_AGG_PLAIN_PERGROUP_NULLCHECK
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepAggPlainPergroupNullcheck {
    pub setoff: c_int,
    pub jumpnull: c_int,
}

/// EEOP_AGG_PRESORTED_DISTINCT_{SINGLE,MULTI}
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepAggPresortedDistinctcheck {
    pub pertrans: *mut c_void, // AggStatePerTrans
    pub aggcontext: *mut ExprContext,
    pub jumpdistinct: c_int,
}

/// EEOP_AGG_PLAIN_TRANS_* / EEOP_AGG_ORDERED_TRANS_*
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepAggTrans {
    pub pertrans: *mut c_void, // AggStatePerTrans
    pub aggcontext: *mut ExprContext,
    pub setno: c_int,
    pub transno: c_int,
    pub setoff: c_int,
}

/// EEOP_IS_JSON
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepIsJson {
    pub pred: *mut c_void, // JsonIsPredicate*
}

/// EEOP_JSONEXPR_PATH
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepJsonexpr {
    pub jsestate: *mut c_void, // JsonExprState*
}

/// EEOP_JSONEXPR_COERCION
#[repr(C)]
#[derive(Clone, Copy)]
pub struct EEStepJsonexprCoercion {
    pub targettype: Oid,
    pub targettypmod: int32,
    pub omit_quotes: bool,
    pub exists_coerce: bool,
    pub exists_cast_to_int: bool,
    pub exists_check_domain: bool,
    pub json_coercion_cache: *mut c_void,
    pub escontext: *mut ErrorSaveContext,
}

/// The `d` union of `ExprEvalStep`: the per-opcode inline data.
///
/// Modelled as a real `#[repr(C)] union` exactly as upstream. Reading/writing
/// members is `unsafe` (the active member is determined by `opcode`).
#[repr(C)]
#[derive(Clone, Copy)]
pub union ExprEvalStepData {
    pub fetch: EEStepFetch,
    pub var: EEStepVar,
    pub wholerow: EEStepWholerow,
    pub assign_var: EEStepAssignVar,
    pub assign_tmp: EEStepAssignTmp,
    pub returningexpr: EEStepReturningExpr,
    pub constval: EEStepConstval,
    pub func: EEStepFunc,
    pub boolexpr: EEStepBoolexpr,
    pub qualexpr: EEStepQualexpr,
    pub jump: EEStepJump,
    pub nulltest_row: EEStepNulltestRow,
    pub param: EEStepParam,
    pub cparam: EEStepCparam,
    pub casetest: EEStepCasetest,
    pub make_readonly: EEStepMakeReadonly,
    pub iocoerce: EEStepIocoerce,
    pub sqlvaluefunction: EEStepSqlvaluefunction,
    pub nextvalueexpr: EEStepNextvalueexpr,
    pub arrayexpr: EEStepArrayexpr,
    pub arraycoerce: EEStepArraycoerce,
    pub row: EEStepRow,
    pub rowcompare_step: EEStepRowcompareStep,
    pub rowcompare_final: EEStepRowcompareFinal,
    pub minmax: EEStepMinmax,
    pub fieldselect: EEStepFieldselect,
    pub fieldstore: EEStepFieldstore,
    pub sbsref_subscript: EEStepSbsrefSubscript,
    pub sbsref: EEStepSbsref,
    pub domaincheck: EEStepDomaincheck,
    pub hashdatum_initvalue: EEStepHashdatumInitvalue,
    pub hashdatum: EEStepHashdatum,
    pub convert_rowtype: EEStepConvertRowtype,
    pub scalararrayop: EEStepScalararrayop,
    pub hashedscalararrayop: EEStepHashedscalararrayop,
    pub xmlexpr: EEStepXmlexpr,
    pub json_constructor: EEStepJsonConstructor,
    pub aggref: EEStepAggref,
    pub grouping_func: EEStepGroupingFunc,
    pub window_func: EEStepWindowFunc,
    pub subplan: EEStepSubplan,
    pub agg_deserialize: EEStepAggDeserialize,
    pub agg_strict_input_check: EEStepAggStrictInputCheck,
    pub agg_plain_pergroup_nullcheck: EEStepAggPlainPergroupNullcheck,
    pub agg_presorted_distinctcheck: EEStepAggPresortedDistinctcheck,
    pub agg_trans: EEStepAggTrans,
    pub is_json: EEStepIsJson,
    pub jsonexpr: EEStepJsonexpr,
    pub jsonexpr_coercion: EEStepJsonexprCoercion,
}

/// `ExprEvalStep` — one flattened instruction. `opcode` is an `intptr_t` (an
/// `ExprEvalOp` during compilation, possibly a goto pointer at runtime).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ExprEvalStep {
    pub opcode: isize,
    pub resvalue: *mut Datum,
    pub resnull: *mut bool,
    pub d: ExprEvalStepData,
}

impl ExprEvalStep {
    /// A zero-initialized step, equivalent to `ExprEvalStep scratch = {0};`.
    #[inline]
    pub const fn zeroed() -> Self {
        // SAFETY: ExprEvalStep is composed entirely of integers, pointers, and
        // a #[repr(C)] union; an all-zero bit pattern is a valid value (NULL
        // pointers, opcode 0 == EEOP_DONE_RETURN). This mirrors the C `{0}`
        // initializer used throughout execExpr.c.
        unsafe { core::mem::zeroed() }
    }
}

// ===========================================================================
// Non-inline data structs referenced by union members (execExpr.h)
// ===========================================================================

/// `SubscriptingRefState` — non-inline state for container subscripting.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SubscriptingRefState {
    pub isassignment: bool,
    pub workspace: *mut c_void,
    pub numupper: c_int,
    pub upperprovided: *mut bool,
    pub upperindex: *mut Datum,
    pub upperindexnull: *mut bool,
    pub numlower: c_int,
    pub lowerprovided: *mut bool,
    pub lowerindex: *mut Datum,
    pub lowerindexnull: *mut bool,
    pub replacevalue: Datum,
    pub replacenull: bool,
    pub prevvalue: Datum,
    pub prevnull: bool,
}

/// `SubscriptExecSteps` — execution-step methods filled by container-type code.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SubscriptExecSteps {
    pub sbs_check_subscripts: ExecEvalBoolSubroutine,
    pub sbs_fetch: ExecEvalSubroutine,
    pub sbs_assign: ExecEvalSubroutine,
    pub sbs_fetch_old: ExecEvalSubroutine,
}

/// `JsonConstructorExprState` — out-of-line EEOP_JSON_CONSTRUCTOR state.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonConstructorExprState {
    pub constructor: *mut c_void, // JsonConstructorExpr*
    pub arg_values: *mut Datum,
    pub arg_nulls: *mut bool,
    pub arg_types: *mut Oid,
    pub arg_type_cache: *mut JsonConstructorArgTypeCache,
    pub nargs: c_int,
}

/// Inner anonymous struct of `JsonConstructorExprState.arg_type_cache`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonConstructorArgTypeCache {
    pub category: c_int,
    pub outfuncid: Oid,
}

/// `JsonExprState` (execnodes.h) — out-of-line `EEOP_JSONEXPR_PATH` state, built
/// by `ExecInitJsonExpr`. Exact-layout mirror so the compiler and the
/// interpreter agree on the byte layout (the `jsexpr.jsestate` step field points
/// at one of these).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonExprState {
    /// original expression node (`JsonExpr *`)
    pub jsexpr: *mut c_void,
    /// value/isnull for `formatted_expr`
    pub formatted_expr: NullableDatum,
    /// value/isnull for `pathspec`
    pub pathspec: NullableDatum,
    /// `JsonPathVariable` entries for `passing_values` (`List *`)
    pub args: *mut List,
    /// set to true if jsonpath evaluation caused an error
    pub error: NullableDatum,
    /// set to true if the jsonpath evaluation returned 0 items
    pub empty: NullableDatum,
    /// address of the non-ERROR ON EMPTY step
    pub jump_empty: c_int,
    /// address of the non-ERROR ON ERROR step
    pub jump_error: c_int,
    /// address of the coercion step, or -1
    pub jump_eval_coercion: c_int,
    /// address to jump to to return the JsonPath* result as-is
    pub jump_end: c_int,
    /// RETURNING-type input function info when `use_io_coercion`
    pub input_fcinfo: FunctionCallInfo,
    /// error-safe evaluation context for coercions
    pub escontext: ErrorSaveContext,
}

/// `JsonPathVariable` (utils/jsonpath.h) — one PASSING argument descriptor built
/// by `ExecInitJsonExpr` and appended to `JsonExprState.args`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct JsonPathVariable {
    /// variable name (NUL-terminated C string)
    pub name: *mut c_char,
    /// `strlen(name)` cache for `GetJsonPathVar()`
    pub namelen: c_int,
    pub typid: Oid,
    pub typmod: int32,
    pub value: Datum,
    pub isnull: bool,
}

/// `WindowFuncExprState` — minimal head needed by the compiler (it sets
/// `wfunc`, `args`, `aggfilter`). The interpreter/nodeWindowAgg own the rest;
/// we mirror the full layout for ABI safety.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct WindowFuncExprState {
    pub xprstate: ExprState,
    pub wfunc: *mut crate::WindowFunc,
    pub args: *mut List,
    pub aggfilter: *mut ExprState,
    pub wfuncno: c_int,
}

// ===========================================================================
// Compile-time layout assertions
// ===========================================================================

const _: () = {
    use core::mem::{align_of, size_of};
    // The size rule from execExpr.h: an ExprEvalStep must fit one cacheline.
    assert!(
        size_of::<ExprEvalStep>() <= 64,
        "ExprEvalStep exceeds 64 bytes"
    );
    // The union must be no more than 40 bytes (so opcode+resvalue+resnull+d <= 64).
    assert!(
        size_of::<ExprEvalStepData>() <= 40,
        "ExprEvalStep union d exceeds 40 bytes"
    );
    assert!(size_of::<ExprEvalRowtypeCache>() == 16);
    // ArrayMapState (utils/array.h) is two ArrayMetaState records back-to-back;
    // EEOP_ARRAYCOERCE allocates it with palloc0(sizeof(ArrayMapState)).
    assert!(size_of::<ArrayMapState>() == 2 * size_of::<ArrayMetaState>());
    assert!(align_of::<ArrayMetaState>() == 8);
};

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn expr_eval_step_layout_matches_pg_abi_on_64_bit() {
        assert_eq!(size_of::<ExprEvalStep>(), 64);
        assert_eq!(align_of::<ExprEvalStep>(), 8);
        assert_eq!(offset_of!(ExprEvalStep, opcode), 0);
        assert_eq!(offset_of!(ExprEvalStep, resvalue), 8);
        assert_eq!(offset_of!(ExprEvalStep, resnull), 16);
        assert_eq!(offset_of!(ExprEvalStep, d), 24);
        // d union must fit in 40 bytes (one cacheline total).
        assert!(size_of::<ExprEvalStepData>() <= 40);

        // A few representative member-struct sizes (64-bit).
        assert_eq!(size_of::<EEStepFunc>(), 32);
        assert_eq!(size_of::<EEStepConstval>(), 16);
        assert_eq!(size_of::<EEStepIocoerce>(), 32);
        assert_eq!(size_of::<EEStepScalararrayop>(), 40);
        assert_eq!(size_of::<EEStepMinmax>(), 40);
    }

    #[test]
    fn json_expr_state_and_path_variable_layout() {
        // JsonExprState (execnodes.h) byte layout on 64-bit; the EEOP_JSONEXPR_*
        // step's jsestate pointer must agree with the interpreter's view.
        assert_eq!(offset_of!(JsonExprState, jsexpr), 0);
        assert_eq!(offset_of!(JsonExprState, formatted_expr), 8);
        assert_eq!(offset_of!(JsonExprState, pathspec), 24);
        assert_eq!(offset_of!(JsonExprState, args), 40);
        assert_eq!(offset_of!(JsonExprState, error), 48);
        assert_eq!(offset_of!(JsonExprState, empty), 64);
        assert_eq!(offset_of!(JsonExprState, jump_empty), 80);
        assert_eq!(offset_of!(JsonExprState, jump_error), 84);
        assert_eq!(offset_of!(JsonExprState, jump_eval_coercion), 88);
        assert_eq!(offset_of!(JsonExprState, jump_end), 92);
        assert_eq!(offset_of!(JsonExprState, input_fcinfo), 96);
        assert_eq!(offset_of!(JsonExprState, escontext), 104);
        assert_eq!(align_of::<JsonExprState>(), 8);

        // JsonPathVariable (utils/jsonpath.h).
        assert_eq!(offset_of!(JsonPathVariable, name), 0);
        assert_eq!(offset_of!(JsonPathVariable, namelen), 8);
        assert_eq!(offset_of!(JsonPathVariable, typid), 12);
        assert_eq!(offset_of!(JsonPathVariable, typmod), 16);
        assert_eq!(offset_of!(JsonPathVariable, value), 24);
        assert_eq!(offset_of!(JsonPathVariable, isnull), 32);
        assert_eq!(align_of::<JsonPathVariable>(), 8);
    }

    #[test]
    fn expr_state_and_projection_layout() {
        // ExprState parent offset must match the FIELDNO used by JIT (11th
        // field, 0-based index 10 of named members; computed by offset_of).
        assert_eq!(offset_of!(ExprState, type_), 0);
        assert_eq!(offset_of!(ExprState, flags), 4);
        assert_eq!(offset_of!(ExprState, resnull), 5);
        assert_eq!(offset_of!(ExprState, resvalue), 8);
        // ProjectionInfo embeds an ExprState after the NodeTag.
        assert_eq!(offset_of!(ProjectionInfo, pi_state), 8);
    }
}
