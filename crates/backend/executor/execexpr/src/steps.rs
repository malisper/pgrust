use core::alloc::Layout;
use core::marker::PhantomData;
use core::ptr::NonNull;

use ::datum::{Datum, NullableDatum};
use ::mcx::{Allocator, Mcx, PgVec};
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{AggStateNode, FmgrInfo, FunctionCallInfoBaseData, LocalFcinfo, PGFunction};

pub const EEO_FLAG_IS_QUAL: u8 = 1 << 0;
pub const EEO_FLAG_HAS_SUBPLAN: u8 = 1 << 1;
// C execnodes.h EEO_FLAG_HAS_OLD/HAS_NEW/OLD_IS_NULL/NEW_IS_NULL, repacked
// into the free bits of this u8.
pub const EEO_FLAG_HAS_OLD: u8 = 1 << 2;
pub const EEO_FLAG_HAS_NEW: u8 = 1 << 3;
pub const EEO_FLAG_OLD_IS_NULL: u8 = 1 << 4;
pub const EEO_FLAG_INTERPRETER_INITIALIZED: u8 = 1 << 5;
pub const EEO_FLAG_NEW_IS_NULL: u8 = 1 << 6;
pub const EEO_FLAG_STILL_VALID_CHECKED: u8 = 1 << 7;

// C's `Datum *resv, bool *resnull` pair, always resolved (a frame's fcinfo
// arg slot or ExprState.resnd) — branch-free writes, phi-free loop head.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OutRef(pub(crate) NonNull<NullableDatum>);

// EEOP program step: C ExprEvalStep's (opcode, union d) collapsed into one
// dense #[repr(u8)] enum; only the SELECT-1/point-select families are ported
// (deferred families in notes at lib.rs). Discriminants are internal — C's
// EEOP_* numbering is not a compat surface.
#[derive(Clone, Copy, Debug)]
#[repr(u8)]
pub enum Step {
    DoneReturn,
    DoneNoReturn,
    ScanFetchSome { last_var: u16 },
    InnerFetchSome { last_var: u16 },
    OuterFetchSome { last_var: u16 },
    ScanVar { attnum: u16, vartype: Oid, out: OutRef },
    InnerVar { attnum: u16, vartype: Oid, out: OutRef },
    OuterVar { attnum: u16, vartype: Oid, out: OutRef },
    ScanSysVar { attnum: i16, out: OutRef },
    InnerSysVar { attnum: i16, out: OutRef },
    OuterSysVar { attnum: i16, out: OutRef },
    AssignScanVar { attnum: u16, resultnum: u16 },
    AssignInnerVar { attnum: u16, resultnum: u16 },
    AssignOuterVar { attnum: u16, resultnum: u16 },
    AssignTmp { resultnum: u16 },
    AssignTmpMakeRo { resultnum: u16 },
    Const { value: Datum, isnull: bool, out: OutRef },
    // Param pointers resolve at compile into address-stable params arrays.
    ParamExtern { prm: NonNull<::types_portal::params::ParamExternData>, out: OutRef },
    // Unbound PARAM_EXTERN: C errors at evaluation (ExecEvalParamExtern),
    // not at init — EXPLAIN (GENERIC_PLAN) inits but never evaluates.
    ParamExternMissing { paramid: i32 },
    ParamExec { prm: NonNull<::types_portal::params::ParamExecData>, out: OutRef },
    FuncExpr { call: FuncCall, out: OutRef },
    FuncExprStrict1 { call: FuncCall, out: OutRef },
    FuncExprStrict2 { call: FuncCall, out: OutRef },
    FuncExprStrict { call: FuncCall, out: OutRef },
    // EEOP_FUNCEXPR_FUSAGE / EEOP_FUNCEXPR_STRICT_FUSAGE: compiled only when
    // track_functions covers fn_stats, so counting never touches the
    // default-off path.
    FuncExprFusage { call: FuncCall, out: OutRef },
    FuncExprStrictFusage { call: FuncCall, out: OutRef },
    // EEOP_IOCOERCE: out fn of the arg type then in fn of the result type;
    // incall args 1/2 (typioparam, typmod -1) are compile-time consts. The
    // pair lives in the state's mcx (fcinfo-image precedent) to keep Step
    // <= 64B; one deref per eval on a cast step.
    IoCoerce { calls: NonNull<IoCoerceCalls>, out: OutRef },

    Qual { jumpdone: u32 },
    Jump { jumpdone: u32 },
    JumpIfNotTrue { jumpdone: u32, out: OutRef },
    JumpIfNotNull { jumpdone: u32, out: OutRef },
    // slot: the owning CASE's compile-allocated testval workspace
    // (C d.casetest.value/isnull; the EXT econtext form is unported).
    CaseTestVal { slot: NonNull<NullableDatum>, out: OutRef },
    // C EEOP_MAKE_READONLY, in place on the CASE testval workspace
    // (source and target alias there in C too).
    MakeReadonly { slot: NonNull<NullableDatum> },
    // anynull: per-BoolExpr compile-allocated scratch (C d.boolexpr.anynull);
    // FIRST/STEP short-circuit to jumpdone, LAST resolves the NULL outcome.
    BoolAndStepFirst { anynull: NonNull<bool>, jumpdone: u32, out: OutRef },
    BoolAndStep { anynull: NonNull<bool>, jumpdone: u32, out: OutRef },
    BoolAndStepLast { anynull: NonNull<bool>, out: OutRef },
    BoolOrStepFirst { anynull: NonNull<bool>, jumpdone: u32, out: OutRef },
    BoolOrStep { anynull: NonNull<bool>, jumpdone: u32, out: OutRef },
    BoolOrStepLast { anynull: NonNull<bool>, out: OutRef },
    BoolNotStep { out: OutRef },
    NullTestIsNull { out: OutRef },
    NullTestIsNotNull { out: OutRef },
    // C EEOP_BOOLTEST_IS_*; IS [NOT] UNKNOWN reuses the NullTest steps.
    BoolTestIsTrue { out: OutRef },
    BoolTestIsNotTrue { out: OutRef },
    BoolTestIsFalse { out: OutRef },
    BoolTestIsNotFalse { out: OutRef },
    // C EEOP_DISTINCT: the resolved "=" call with DISTINCT null semantics.
    Distinct { call: FuncCall, out: OutRef },
    // C EEOP_NULLIF: the resolved "=" call; equal non-null args -> NULL,
    // else the first arg unchanged.
    NullIf { call: FuncCall, out: OutRef },
    // Agg pointers resolve at build into once-allocated never-moved AggState arrays.
    AggrefEval { value: NonNull<Datum>, null: NonNull<bool>, out: OutRef },
    // C EEOP_GROUPING_FUNC: bit per clause col, 1 = ungrouped in the
    // current set (None cell: no grouping sets, result 0).
    GroupingFuncEval {
        cols: NonNull<i32>,
        ncols: u16,
        current: Option<NonNull<GroupedColsCell>>,
        out: OutRef,
    },
    // EEOP_SCALARARRAYOP: the array operand evaluates into `out` first;
    // element typ* resolved at compile (C caches them on first eval).
    ScalarArrayOp {
        call: FuncCall,
        use_or: bool,
        strict: bool,
        typlen: i16,
        typbyval: bool,
        typalign: u8,
        out: OutRef,
    },
    // C EEOP_WHOLEROW, named-composite leg over a scan/inner/outer slot
    // (RECORD/subquery whole-row and OLD/NEW are compile louds). The var's
    // typcache tupdesc resolves at compile; the slot-compat check runs once
    // at first eval, per C.
    WholeRow { src: SlotSrc, wr: NonNull<WholeRowState>, frame: u32, out: OutRef },
    // EEOP_NULLTEST_ROWISNULL/ROWISNOTNULL; `frame` is an argless FuncFrame
    // carried only for its armed per-eval mcx (detoast scratch).
    NullTestRowIsNull { rn: NonNull<RowNullState>, frame: u32, out: OutRef },
    NullTestRowIsNotNull { rn: NonNull<RowNullState>, frame: u32, out: OutRef },
    // EEOP_HASHED_SCALARARRAYOP: array operand is a non-null Const; the
    // element table (and its hash FuncCall) lives in state.saop_tables.
    HashedScalarArrayOp {
        call: FuncCall,
        inclause: bool,
        typlen: i16,
        typbyval: bool,
        typalign: u8,
        table: u32,
        out: OutRef,
    },
    // EEOP_ARRAYEXPR, 1-D: elements evaluate into the `elems` scratch;
    // `frame` is an argless FuncFrame carried only for its armed result mcx.
    ArrayExprStep {
        elems: NonNull<NullableDatum>,
        // C execExpr.h arrayexpr.nelems is `int`: ARRAY[] element count is
        // NOT bounded by any 16-bit limit. This was u16, so a literal with
        // 65536 elements truncated to 0 and silently produced an EMPTY array
        // (array_length -> NULL) instead of the right answer.
        nelems: u32,
        frame: u32,
        elmtype: Oid,
        elmlen: i16,
        elmbyval: bool,
        elmalign: u8,
        out: OutRef,
    },
    // C EEOP_ROWEXPR: elements evaluate into `elems`; `desc` is the blessed
    // anonymous-RECORD tupdesc, arena-lived for the plan.
    RowExprStep {
        elems: NonNull<NullableDatum>,
        // C rowexpr.nelems is `int` too (see ArrayExprStep above). In practice
        // the blessed tupdesc bounds this well below 65535, but the field must
        // not be the thing that silently wraps.
        nelems: u32,
        frame: u32,
        desc: NonNull<::types_tuple::TupleDescData<'static>>,
        out: OutRef,
    },
    // C EEOP_AGG_STRICT_INPUT_CHECK_ARGS(_1): args = fcinfo args[1..].
    AggStrictInputCheck { args: NonNull<NullableDatum>, nargs: u16, jumpnull: u32 },
    // Ordered/DISTINCT agg row survived filter+strict checks: flag it for
    // nodeagg's tuplesort feed (scratch already holds the evaluated args).
    AggOrderedMark { flag: NonNull<bool> },
    // C "set up aggstate->curpertrans for AggGetAggref()" (execExprInterp.c);
    // pushed only for ordered-set aggs.
    AggSetCurrent {
        agg: NonNull<::types_fmgr::AggStateNode>,
        aggref: NonNull<()>,
        shared: bool,
    },
    AggStrictInputCheck1 { arg: NonNull<NullableDatum>, jumpnull: u32 },
    // C EEOP_AGG_[STRICT_]DESERIALIZE: args[0] holds the serialized input;
    // the result lands in the combine fcinfo's args[1] slot (`out`).
    AggDeserialize { call: FuncCall, out: OutRef },
    AggStrictDeserialize { call: FuncCall, out: OutRef, jumpnull: u32 },
    AggPlainTransByVal { call: FuncCall, pergroup: NonNull<AggPerGroup> },
    AggPlainTransStrictByVal { call: FuncCall, pergroup: NonNull<AggPerGroup> },
    // C EEOP_AGG_PLAIN_TRANS_[INIT_][STRICT_]BYREF.
    AggPlainTransInitStrictByRef { call: FuncCall, pergroup: NonNull<AggPerGroup>, byref: AggByRef },
    AggPlainTransStrictByRef { call: FuncCall, pergroup: NonNull<AggPerGroup>, byref: AggByRef },
    AggPlainTransByRef { call: FuncCall, pergroup: NonNull<AggPerGroup>, byref: AggByRef },
    AggPlainTransInitStrictByVal { call: FuncCall, pergroup: NonNull<AggPerGroup> },
    // Hashed-agg trans: pergroup resolves per tuple through a cell nodeAgg
    // repoints after each hash lookup (C's setoff into all_pergroups).
    AggTransByValIndirect { call: FuncCall, base: NonNull<NonNull<AggPerGroup>>, transno: u16 },
    AggTransStrictByValIndirect {
        call: FuncCall,
        base: NonNull<NonNull<AggPerGroup>>,
        transno: u16,
    },
    AggTransInitStrictByRefIndirect {
        call: FuncCall,
        base: NonNull<NonNull<AggPerGroup>>,
        transno: u16,
        byref: AggByRef,
    },
    AggTransStrictByRefIndirect {
        call: FuncCall,
        base: NonNull<NonNull<AggPerGroup>>,
        transno: u16,
        byref: AggByRef,
    },
    AggTransByRefIndirect {
        call: FuncCall,
        base: NonNull<NonNull<AggPerGroup>>,
        transno: u16,
        byref: AggByRef,
    },
    AggTransInitStrictByValIndirect {
        call: FuncCall,
        base: NonNull<NonNull<AggPerGroup>>,
        transno: u16,
    },
    HashDatumSetInitVal { init_value: Datum, out: OutRef },
    HashDatumFirst { call: FuncCall, out: OutRef },
    // iresult: build-owned intermediate hash slot the rotate-xor chain reads.
    HashDatumNext32 { call: FuncCall, iresult: NonNull<NullableDatum>, out: OutRef },
    NotDistinct { call: FuncCall, out: OutRef },
    ParamSet { prm: NonNull<::types_portal::params::ParamExecData>, out: OutRef },
    // EEOP_SUBPLAN: the interpreter suspends; the caller's driver runs
    // ExecSubPlan (nodeSubplan.c in execmain) with the full estate and
    // resumes with the result (see interp::EvalOutcome).
    SubPlan { sstate: NonNull<()>, out: OutRef },
    // EEOP_MAKE_READONLY: emitted only for typlen -1 domain-check inputs.
    MakeReadonlyOut { src: OutRef, out: OutRef },
    DomainTestval { src: OutRef, out: OutRef },
    // escontext: C d.domaincheck.escontext — behavior-expr domain checks
    // under a JsonExprState errsave instead of throwing.
    DomainNotNull {
        resulttype: Oid,
        escontext: Option<NonNull<::types_fmgr::ErrorSaveNode>>,
        out: OutRef,
    },
    // name/check: compile-allocated in 'mcx (BoolAndStep anynull precedent).
    DomainCheck {
        resulttype: Oid,
        name: NonNull<str>,
        check: NonNull<NullableDatum>,
        escontext: Option<NonNull<::types_fmgr::ErrorSaveNode>>,
    },
    JumpIfNull { jumpdone: u32, out: OutRef },
    ArrayExprEval { state: NonNull<crate::arrayops::ArrayExprState>, out: OutRef },
    XmlExprEval { state: NonNull<crate::xmlops::XmlExprState>, out: OutRef },
    SbsrefSubscripts { state: NonNull<crate::arrayops::SbsRefState>, jumpdone: u32, out: OutRef },
    SbsrefFetch { state: NonNull<crate::arrayops::SbsRefState>, slice: bool, out: OutRef },
    SbsrefOld { state: NonNull<crate::arrayops::SbsRefState>, out: OutRef },
    SbsrefAssign { state: NonNull<crate::arrayops::SbsRefState>, slice: bool, out: OutRef },
    JsonbSbsrefSubscripts {
        state: NonNull<crate::jsonbsubs::JsonbSbsState>,
        jumpdone: u32,
        out: OutRef,
    },
    JsonbSbsrefFetch { state: NonNull<crate::jsonbsubs::JsonbSbsState>, out: OutRef },
    JsonbSbsrefAssign { state: NonNull<crate::jsonbsubs::JsonbSbsState>, out: OutRef },
    HstoreSbsrefFetch { state: NonNull<crate::hstoresubs::HstoreSbsState>, out: OutRef },
    HstoreSbsrefAssign { state: NonNull<crate::hstoresubs::HstoreSbsState>, out: OutRef },
    // slots: nelems compile-allocated NullableDatum arg targets (C's
    // d.minmax.values/nulls); call is the type's btree cmp proc.
    MinMax { call: FuncCall, slots: NonNull<NullableDatum>, nelems: u32, least: bool, out: OutRef },
    NextValueExpr { seqid: Oid, seqtypid: Oid, out: OutRef },
    // C EEOP_JSON_CONSTRUCTOR: arg subexprs write jcstate's slots; constant
    // metadata + split scratch behind one plan-mcx pointer.
    JsonConstructor { jcstate: NonNull<JsonConstructorState>, frame: u32, out: OutRef },
    // C EEOP_IS_JSON: reads the arg value already in `out`, rewrites it.
    IsJson {
        exprtype: Oid,
        item_type: ::types_nodes::primnodes::JsonValueType,
        unique_keys: bool,
        frame: u32,
        out: OutRef,
    },
    // scratch: compile-allocated by-ref result image (12-byte TimeTz or
    // 64-byte NameData), rewritten per eval — valid until the next eval, the
    // window C's per-tuple context reset gives.
    SqlValueFunction {
        op: ::types_nodes::primnodes::SQLValueFunctionOp,
        typmod: i32,
        scratch: NonNull<u8>,
        out: OutRef,
    },
    // C EEOP_MERGE_SUPPORT_FUNC (ExecEvalMergeSupportFunc): `action` is the
    // state's merge-action cell, armed by the owning ModifyTable node via
    // set_merge_action before each RETURNING projection. scratch: compile-
    // allocated 10-byte text image, rewritten per eval as SqlValueFunction's.
    MergeSupportFunc {
        action: NonNull<Option<::types_nodes::nodes_enums::CmdType>>,
        scratch: NonNull<u8>,
        out: OutRef,
    },
    // Ready-time fused pairs (fuse_program): the two source steps back-to-back.
    ScanVarFuncStrict2 { attnum: u16, argno: u8, vartype: Oid, call: Call2, out: OutRef },
    FuncFuncStrict2 { call1: Call2, argno: u8, call2: Call2, out: OutRef },
    FuncStrict2Qual { call: Call2, jumpdone: u32, out: OutRef },
    OuterVarNotDistinct { attnum: u16, argno: u8, vartype: Oid, call: Call2, out: OutRef },
    NotDistinctQual { call: Call2, jumpdone: u32, out: OutRef },
    OuterVarAggTransByValIndirect {
        attnum: u16,
        argno: u8,
        vartype: Oid,
        call: Call2,
        base: NonNull<NonNull<AggPerGroup>>,
        transno: u16,
    },
    AssignScanVar2 { attnum1: u16, resultnum1: u16, attnum2: u16, resultnum2: u16 },
    // Thin-ABI twins (fmgr_thin_builtin rows), selected at ready time.
    FuncExprStrict1Thin { call: CallThin, out: OutRef },
    FuncExprStrict2Thin { call: CallThin, out: OutRef },
    ScanVarFuncStrict2Thin { attnum: u16, argno: u8, vartype: Oid, call: CallThin, out: OutRef },
    FuncFuncStrict2Thin { call1: CallThin, argno: u8, call2: CallThin, out: OutRef },
    FuncStrict2QualThin { call: CallThin, jumpdone: u32, out: OutRef },
    OuterVarNotDistinctThin { attnum: u16, argno: u8, vartype: Oid, call: CallThin, out: OutRef },
    NotDistinctQualThin { call: CallThin, jumpdone: u32, out: OutRef },
    AggTransStrictByValIndirectThin {
        call: CallThin,
        base: NonNull<NonNull<AggPerGroup>>,
        transno: u16,
    },
    // C EEOP_FIELDSELECT; reads the record datum from `out`, writes the
    // field back to `out`. Per-eval registry tupdesc copy stands in for C's
    // rowcache (cold path; see interp). Kept last: appending preserves the
    // hot variants' discriminants.
    FieldSelect { fieldnum: i16, resulttype: Oid, frame: u32, out: OutRef },
    // C EEOP_ROWCOMPARE_STEP: per-column btree ORDER proc; strict-NULL input
    // or NULL result jumps past the expression, nonzero result to FINAL.
    // Appended last: preserves the hot variants' discriminants.
    RowCompareStep { call: Call2, strict: bool, jumpnull: u32, jumpdone: u32, out: OutRef },
    // C EEOP_ROWCOMPARE_FINAL; cmptype is CompareType (EQ/NE never appear).
    RowCompareFinal { cmptype: i32, out: OutRef },
    // C EEOP_ARRAYCOERCE: reads the array datum from `out`, rewrites it.
    // Appended last: preserves the hot variants' discriminants.
    ArrayCoerce { state: NonNull<crate::arrayops::ArrayCoerceState>, out: OutRef },
    // C EEOP_CONVERT_ROWTYPE; the argless frame supplies the per-eval mcx.
    ConvertRowtype { state: NonNull<ConvertRowtypeState>, frame: u32, out: OutRef },
    // C EEOP_FIELDSTORE_DEFORM/FORM; DEFORM reads the composite datum from
    // `out` into the column workspace, FORM writes the re-formed composite
    // back to `out`. The argless frame supplies the per-eval mcx. Appended
    // last: preserves the hot variants' discriminants.
    FieldStoreDeForm { fs: NonNull<FieldStoreState>, frame: u32, out: OutRef },
    FieldStoreForm { fs: NonNull<FieldStoreState>, frame: u32, out: OutRef },
    // C EEOP_JSONEXPR_PATH: a jumping step — evaluation returns the next step
    // address (one of the state's jump_* fields).
    JsonExprPath { jsestate: NonNull<JsonExprState>, frame: u32, out: OutRef },
    JsonCoercion { jc: NonNull<JsonCoercionState>, frame: u32, out: OutRef },
    JsonCoercionFinish { jsestate: NonNull<JsonExprState>, out: OutRef },
    // C EEOP_IOCOERCE_SAFE: input-fn errors save into the fcinfo-armed
    // ErrorSaveNode instead of throwing.
    IoCoerceSafe { calls: NonNull<IoCoerceCalls>, out: OutRef },
    // C EEOP_OLD_/NEW_FETCHSOME/VAR/SYSVAR + EEOP_RETURNINGEXPR (RETURNING
    // OLD/NEW). Appended last: preserves the hot variants' discriminants.
    OldFetchSome { last_var: u16 },
    NewFetchSome { last_var: u16 },
    OldVar { attnum: u16, vartype: Oid, out: OutRef },
    NewVar { attnum: u16, vartype: Oid, out: OutRef },
    OldSysVar { attnum: i16, out: OutRef },
    NewSysVar { attnum: i16, out: OutRef },
    AssignOldVar { attnum: u16, resultnum: u16 },
    AssignNewVar { attnum: u16, resultnum: u16 },
    ReturningExprStep { nullflag: u8, jumpdone: u32, out: OutRef },
}

// C JsonExprState (execnodes.h): resolve-once carrier for EEOP_JSONEXPR_*.
// jump_* are absolute step addresses in the owning program (-1 = unset);
// formatted_expr/pathspec/var_cells are written in place by sub-expr steps.
pub struct JsonExprState {
    pub op: ::types_nodes::primnodes::JsonExprOp,
    pub column_name: Option<NonNull<str>>,
    pub wrapper: ::adt_jsonpath_exec::JsonWrapper,
    pub returning_typid: Oid,
    pub use_io_coercion: bool,
    pub use_json_coercion: bool,
    pub throw_error: bool,
    pub on_error_btype: ::types_nodes::primnodes::JsonBehaviorType,
    pub on_empty_btype: Option<::types_nodes::primnodes::JsonBehaviorType>,
    pub formatted_expr: NullableDatum,
    pub pathspec: NullableDatum,
    pub error: NullableDatum,
    pub empty: NullableDatum,
    pub nvars: u16,
    // Parallel arrays: name/typid/typmod fixed at compile, value/isnull
    // refreshed per eval from var_cells.
    pub vars: NonNull<::adt_jsonpath_exec::JsonPathVariable<'static>>,
    pub var_cells: NonNull<NullableDatum>,
    pub jump_error: i32,
    pub jump_empty: i32,
    pub jump_eval_coercion: i32,
    pub jump_end: i32,
    pub input_fcinfo: Option<FuncCall>,
    pub escontext: ::types_fmgr::ErrorSaveNode,
}

// C ExprEvalStep d.jsonexpr_coercion. `escontext` points at the owning
// JsonExprState's ErrorSaveNode (None = errors are hard); `cache` is C's
// json_coercion_cache, filled on first eval; `mcx` is the compile mcx
// restamped 'static — it outlives every eval of this step.
pub struct JsonCoercionState {
    pub targettype: Oid,
    pub targettypmod: i32,
    pub omit_quotes: bool,
    pub exists_coerce: bool,
    pub exists_cast_to_int: bool,
    pub exists_check_domain: bool,
    pub escontext: Option<NonNull<::types_fmgr::ErrorSaveNode>>,
    pub cache: Option<::adt_jsonb::populate::ColumnIoData<'static>>,
    pub mcx: Mcx<'static>,
}

// Blessed tupdesc compile-resolved (C: rowcache on first eval); `columns` is
// the values/nulls workspace shared by DEFORM, the per-field subexpressions,
// and FORM.
pub struct FieldStoreState {
    pub ncolumns: u16,
    pub desc: NonNull<::types_tuple::TupleDescData<'static>>,
    pub columns: NonNull<NullableDatum>,
}

// Tupdescs + by-name map compile-resolved (C: first eval; plan invalidation
// covers DDL between). map[out_i] = 1-based in attno, 0 = NULL; None = relabel.
pub struct ConvertRowtypeState {
    pub indesc: NonNull<::types_tuple::TupleDescData<'static>>,
    pub outdesc: NonNull<::types_tuple::TupleDescData<'static>>,
    pub map: Option<NonNull<[i16]>>,
}

// C JsonConstructorExprState: resolved-once metadata for the
// EEOP_JSON_CONSTRUCTOR step; scalar categorize carriers are compile-resolved
// (C caches them in arg_type_cache).
pub struct JsonConstructorState {
    pub ctor_type: ::types_nodes::JsonConstructorType,
    pub is_jsonb: bool,
    pub absent_on_null: bool,
    pub unique: bool,
    pub nargs: u16,
    pub slots: NonNull<NullableDatum>,
    pub values: NonNull<Datum>,
    pub nulls: NonNull<bool>,
    pub types: NonNull<Oid>,
    pub scalar_json: Option<NonNull<::adt_json::tojson::TypeCat>>,
    pub scalar_jsonb: Option<NonNull<::adt_jsonb::tojsonb::ValCategory>>,
}

// C ExprEvalStep d.nulltest_row.rowcache: last-seen rowtype's tupdesc,
// refreshed from typcache when the header's (type, typmod) changes. `mcx` is
// the compile mcx restamped 'static; it outlives every eval of this step.
pub struct RowNullState {
    pub tup_type: Oid,
    pub tup_typmod: i32,
    pub desc: Option<NonNull<::types_tuple::TupleDescData<'static>>>,
    pub mcx: Mcx<'static>,
}

// C ExprEvalStep d.wholerow minus var: first-eval compat state. The
// named-composite leg resolves `tupdesc` at compile; the RECORD leg resolves
// it at first eval from the (junk-filtered) slot's descriptor. `colnames` is
// the Var's RTE eref alias list, captured at compile (C reads it through
// econtext->ecxt_estate at first eval; the range table is init-stable).
// `mcx` is the compile mcx restamped 'static; it outlives every eval.
pub struct WholeRowState {
    pub tupdesc: Option<NonNull<::types_tuple::TupleDescData<'static>>>,
    pub first: bool,
    pub slow: bool,
    pub record: bool,
    pub colnames: Option<NonNull<::types_nodes::list::NodeList<'static>>>,
    pub junk: Option<NonNull<WholeRowJunk>>,
    pub mcx: Mcx<'static>,
}

// C ExprEvalStep d.wholerow.junkFilter (jf_cleanMap + jf_resultSlot). C
// parks the result slot in the estate tuple table; the interpreter has no
// estate, so the state owns it.
pub struct WholeRowJunk {
    pub clean_map: NonNull<[i16]>,
    pub slot: NonNull<::types_slot::SlotData<'static>>,
}

// By-ref copy target: C d.agg_trans.aggcontext + the transtype's typlen.
#[derive(Clone, Copy, Debug)]
pub struct AggByRef {
    pub agg: NonNull<AggStateNode>,
    pub translen: i16,
}

// The current set's grouped child attnos; nodeAgg repoints per set.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct GroupedColsCell {
    pub ptr: *const i16,
    pub len: usize,
}

// C nodeAgg.h AggStatePerGroupData; the trans steps read/write it in place.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct AggPerGroup {
    pub trans_value: Datum,
    pub trans_value_is_null: bool,
    pub no_trans_value: bool,
}

::mcx::forget_safe_nodrop!(AggPerGroup, CmpOp);

/// Clause cap for [`ScanCmpClauses`] (the lane-v2 batched-qual census).
pub const SCAN_CMP_MAX_CLAUSES: usize = 4;

/// AND-of-(scan Var CMP non-null Const) clause census for the lane-v2
/// batched qual tiers (AOT bitmap passes / the stitched-JIT body): one
/// `(attnum, cmp, konst)` per clause, in clause order. Selected at ready
/// time from the PRISTINE step program — before the interpreter peephole
/// (`fuse_program`) rewrites the shapes — so it stays valid whether the
/// program later interprets fused, interprets unfused, or JITs. Every
/// admitted clause is an in-core int comparator (strict, non-erroring,
/// non-volatile) over one scan Var and one compile-time non-null Const, so
/// AND-of-bitmaps equals `exec_qual` exactly (NULL clause result = row
/// fails; short-circuit is unobservable).
#[derive(Clone, Copy, Debug)]
pub struct ScanCmpClauses {
    pub clauses: [(u16, CmpOp, Datum); SCAN_CMP_MAX_CLAUSES],
    pub n: u8,
}

/// Contains-class LIKE qual census (the lane-v2 strsearch qual kernel,
/// `notes/strsearch-parity-2026-07-12.md`): the qual is exactly one
/// `scan_var LIKE '%literal%'` clause — `textlike` (strict, 2-arg) over one
/// scan Var and one compile-time non-null Const pattern whose shape is a
/// leading `%` run + a metachar-free literal (no `%`/`_`/`\` anywhere in the
/// literal, no `\` anywhere in the pattern) + a trailing `%` run. For that
/// class, LIKE match == byte-contains of the literal (`MatchText`'s `%`
/// recursion reduces to substring search; the UTF-8 matcher's char stepping
/// can't skip a byte-aligned occurrence because a valid-UTF-8 needle's first
/// byte is never a continuation byte and stored text is validated UTF-8).
/// Admission also requires the database encoding to be single-byte or UTF-8
/// (`generic_match_text`'s ported arms — other encodings refuse so the
/// per-row path keeps its exact error surface).
///
/// `needle` points into the pattern Const's varlena payload (compile-owned,
/// address-stable for the plan's lifetime, like every frame const).
#[derive(Clone, Copy, Debug)]
pub struct ScanContainsClause {
    pub attnum: u16,
    pub collation: Oid,
    needle: NonNull<u8>,
    needle_len: u32,
}

impl ScanContainsClause {
    pub(crate) fn new(attnum: u16, collation: Oid, needle: NonNull<u8>, needle_len: u32) -> Self {
        ScanContainsClause { attnum, collation, needle, needle_len }
    }

    /// The contains literal. Valid while the owning plan (the pattern
    /// Const's compile mcx) lives — the same lifetime rail every staged
    /// clause Datum in [`ScanCmpClauses`] rides.
    #[inline]
    pub fn needle(&self) -> &[u8] {
        // SAFETY: compile-time pointer into the frame const's varlena
        // payload, address-stable and live for the plan (struct contract).
        unsafe { core::slice::from_raw_parts(self.needle.as_ptr(), self.needle_len as usize) }
    }
}

::mcx::forget_safe_nodrop!(ScanContainsClause);

/// Batched contains-LIKE qual over a staged varlena pointer lane (the varkey
/// lane: each non-null cell is a live in-page varlena datum pointer). For
/// every row `i`: selection bit = `!isnull && contains(text, needle)` when
/// the datum is a plain inline varlena (1B short, not a toast pointer, or 4B
/// uncompressed); a compressed/external datum is UNDECIDABLE here — its bit
/// in `undecided` is set instead and the caller must route the row through
/// the per-row program (which detoasts exactly as C does; the lanefold
/// vguard discipline, per-row instead of per-batch). Non-erroring by
/// construction; NULL rows fail the strict clause, matching `exec_qual`.
///
/// The needle search is `memchr::memmem` with a per-call prebuilt finder —
/// the measured-fastest kernel of the strsearch parity matrix (blob-wide
/// application lands with the pgrcolumnar text arena; a pointer lane has no
/// contiguous blob).
///
/// # Safety
/// Rows with a false isnull bit carry lane values that are live varlena
/// datum pointers readable through their header (the `soa_stage_varkey`
/// contract); `sel`/`undecided` hold at least `values.len().div_ceil(64)`
/// words.
pub unsafe fn qual_bitmap_contains(
    needle: &[u8],
    values: &[Datum],
    isnull: &[bool],
    sel: &mut [u64],
    undecided: &mut [u64],
) {
    use ::types_tuple::varatt::{
        varatt_is_1b, varatt_is_1b_e, varatt_is_4b_u, varsize_1b, varsize_4b, VARHDRSZ,
        VARHDRSZ_SHORT,
    };
    debug_assert!(values.len() == isnull.len());
    debug_assert!(sel.len() >= values.len().div_ceil(64));
    debug_assert!(undecided.len() >= values.len().div_ceil(64));
    let finder = ::memchr::memmem::Finder::new(needle);
    let n = values.len();
    for (w, chunk) in values.chunks(64).enumerate() {
        let mut bits = 0u64;
        let mut und = 0u64;
        for (j, v) in chunk.iter().enumerate() {
            let i = w * 64 + j;
            if isnull[i] {
                continue;
            }
            let p = v.as_usize() as *const u8;
            // SAFETY: non-null staged varkey cell — live varlena pointer
            // readable through its header byte (fn contract).
            let text: &[u8] = unsafe {
                if varatt_is_1b(p) && !varatt_is_1b_e(p) {
                    core::slice::from_raw_parts(
                        p.add(VARHDRSZ_SHORT),
                        varsize_1b(p) - VARHDRSZ_SHORT,
                    )
                } else if varatt_is_4b_u(p) {
                    core::slice::from_raw_parts(p.add(VARHDRSZ), varsize_4b(p) - VARHDRSZ)
                } else {
                    und |= 1u64 << j;
                    continue;
                }
            };
            if finder.find(text).is_some() {
                bits |= 1u64 << j;
            }
        }
        sel[w] = bits;
        undecided[w] = und;
    }
    let nwords = n.div_ceil(64);
    for w in sel.iter_mut().skip(nwords) {
        *w = 0;
    }
    for w in undecided.iter_mut().skip(nwords) {
        *w = 0;
    }
}

/// Column cap for [`ScanProjCols`] (the lane-v2 stitched-projection census;
/// mirrors the lanestitch output-lane cap).
pub const SCAN_PROJ_MAX_COLS: usize = 8;

/// Same-width int2/int4/int8 arithmetic of the scan-projection census
/// (C int.c / int8.c pl/mi/mul/div: erroring — overflow / division by zero).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjArithOp {
    Add2,
    Sub2,
    Mul2,
    Div2,
    Add4,
    Sub4,
    Mul4,
    Div4,
    Add8,
    Sub8,
    Mul8,
    Div8,
}

impl ProjArithOp {
    pub fn for_fn_oid(oid: Oid) -> Option<ProjArithOp> {
        Some(match oid {
            176 => ProjArithOp::Add2,
            180 => ProjArithOp::Sub2,
            152 => ProjArithOp::Mul2,
            153 => ProjArithOp::Div2,
            177 => ProjArithOp::Add4,
            181 => ProjArithOp::Sub4,
            141 => ProjArithOp::Mul4,
            154 => ProjArithOp::Div4,
            463 => ProjArithOp::Add8,
            464 => ProjArithOp::Sub8,
            465 => ProjArithOp::Mul8,
            466 => ProjArithOp::Div8,
            _ => return None,
        })
    }
}

/// One tlist column of the scan-projection census.
#[derive(Clone, Copy, Debug)]
pub enum ScanProjCol {
    /// The column is one scan Var (0-based attnum): a Datum-image copy, any
    /// type (byval or by-ref — same currency as `AssignScanVar`).
    Var { attnum: u16 },
    /// int arith over two scan Vars (strict: NULL in -> NULL out).
    ArithVV { op: ProjArithOp, a: u16, b: u16 },
    /// int arith over one scan Var and one compile-time non-null Const;
    /// `var_is_arg0` = the Var is the left operand.
    ArithVK { op: ProjArithOp, attnum: u16, konst: Datum, var_is_arg0: bool },
}

/// Whole-projection census for the lane-v2 stitched-projection tier: the
/// target list as `n` recognized columns in resultnum order (0..n-1, dense).
/// Selected at ready time from the PRISTINE step program (before
/// `fuse_program` rewrites Assign shapes), like [`ScanCmpClauses`]. Every
/// admitted column is subplan- and param-free by construction (scan Vars,
/// compile-time Consts, in-core strict int arith whose only errors are C's
/// overflow / division-by-zero).
#[derive(Clone, Copy, Debug)]
pub struct ScanProjCols {
    pub cols: [ScanProjCol; SCAN_PROJ_MAX_COLS],
    pub n: u8,
}

impl ScanProjCols {
    /// Highest 0-based scan attnum any column reads.
    pub fn max_attnum(&self) -> u16 {
        self.cols[..self.n as usize]
            .iter()
            .map(|c| match *c {
                ScanProjCol::Var { attnum } => attnum,
                ScanProjCol::ArithVV { a, b, .. } => a.max(b),
                ScanProjCol::ArithVK { attnum, .. } => attnum,
            })
            .max()
            .unwrap_or(0)
    }

    /// True when any column computes (non-Var-passthrough) — the stitched
    /// projection's admission-economics gate keys on this.
    pub fn any_arith(&self) -> bool {
        self.cols[..self.n as usize]
            .iter()
            .any(|c| !matches!(c, ScanProjCol::Var { .. }))
    }
}

/// Call-chain caps for [`ScanProjExprKey`] (the lane-v2 expression-group-key
/// census; sized for the per-row regexp_replace class — one such call with
/// two const siblings — with slack for a short composition).
pub const PROJ_KEY_MAX_CALLS: usize = 2;
pub const PROJ_KEY_MAX_ARGS: usize = 4;

/// One strict fmgr call of an admitted single-Var projection chain
/// (node-free: the consumer owns catalog authority — volatility, language,
/// rettype — exactly the `laneexec::DictCallSpec` split).
#[derive(Clone, Copy, Debug)]
pub struct ProjKeyCall {
    pub fn_oid: Oid,
    /// fcinfo fncollation (collation-sensitive kernels re-evaluate with it).
    pub collation: Oid,
    /// Which arg receives the inner value (the scan Var for calls\[0\], the
    /// previous call's result above); `args[var_argno]` is ignored.
    pub var_argno: u8,
    pub nargs: u8,
    /// Const siblings, prefilled at compile (compile-time non-null gated by
    /// the walk); slots past `nargs` are unused.
    pub args: [NullableDatum; PROJ_KEY_MAX_ARGS],
}

/// Expression-group-key census (lane-v2 expr-key grouping): a projection
/// whose target list is bare scan Vars plus EXACTLY ONE computed column — a
/// chain of strict fmgr calls over exactly one scan Var with compile-time
/// non-null Const siblings. Selected at ready time from the PRISTINE step
/// program, like [`ScanProjCols`]. Structural only: the fn-oid legality gate
/// (IMMUTABLE, internal-language, strictness re-check against pg_proc) is the
/// consumer's (`laneexec::dicteval` — fail-closed there too).
#[derive(Clone, Copy, Debug)]
pub struct ScanProjExprKey {
    /// Per result column: `Some(attnum)` = bare Var passthrough (0-based scan
    /// attnum); `None` = THE computed column.
    pub cols: [Option<u16>; SCAN_PROJ_MAX_COLS],
    pub n: u8,
    /// resultnum of the computed column.
    pub key_out: u16,
    /// The scan Var feeding the chain (0-based attnum) and its vartype.
    pub input_col: u16,
    pub input_type: Oid,
    pub ncalls: u8,
    pub calls: [ProjKeyCall; PROJ_KEY_MAX_CALLS],
}

const _: () = assert!(core::mem::size_of::<Step>() <= 64);

// C ExprEvalStep.d.func minus the FmgrInfo pointer: fn_addr/fcinfo are the
// resolve-once extra copies C keeps "to save an indirection at runtime";
// `frame` reaches the owning FuncFrame (flinfo) in ExprState.
pub struct IoCoerceCalls {
    pub outcall: FuncCall,
    pub incall: FuncCall,
    pub in_strict: bool,
}

// FuncCall minus frame/nargs (a constant 2): keeps fused steps inside 64B.
#[derive(Clone, Copy, Debug)]
pub struct Call2 {
    pub(crate) fcinfo: NonNull<u8>,
    pub(crate) flinfo: NonNull<FmgrInfo>,
}

// Call2 with the thin fn resolved in place of the FmgrInfo indirection.
#[derive(Clone, Copy, Debug)]
pub struct CallThin {
    pub(crate) fcinfo: NonNull<u8>,
    pub(crate) f: ::types_fmgr::PGFunctionThin,
}

impl From<FuncCall> for Call2 {
    fn from(c: FuncCall) -> Call2 {
        debug_assert!(c.nargs == 2);
        Call2 { fcinfo: c.fcinfo, flinfo: c.flinfo }
    }
}

// Resolved once at compile; fn_addr rides in the FmgrInfo header line —
// a copy here would push ScalarArrayOp/MinMax steps past the 64B budget.
#[derive(Clone, Copy, Debug)]
pub struct FuncCall {
    pub(crate) fcinfo: NonNull<u8>,
    pub(crate) flinfo: NonNull<FmgrInfo>,
    pub frame: u32,
    pub nargs: u16,
}

impl FuncCall {
    #[inline(always)]
    pub(crate) fn fn_addr(&self) -> PGFunction {
        // SAFETY: frame-owned mcx-boxed FmgrInfo, live for 'mcx.
        unsafe { self.flinfo.as_ref() }.fn_addr
    }
}

// C ScalarArrayOpExprHashTable: lazily built on first eval, per-query
// lifetime; buckets keyed by the element type's hash-fn result, dedup and
// probe through the step's equality FuncCall.
pub(crate) struct SaopTable<'mcx> {
    pub(crate) hashcall: FuncCall,
    pub(crate) built: bool,
    pub(crate) has_nulls: bool,
    // Cached result of probing a NULL scalar with a non-strict equality
    // function (C hashedscalararrayop null_lhs_result/null_lhs_isnull).
    pub(crate) null_lhs_result: bool,
    pub(crate) null_lhs_isnull: bool,
    pub(crate) map: ::mcx::PgFxHashMap<'mcx, u32, PgVec<'mcx, Datum>>,
}

// Step-owned call state: the FmgrInfo carrier plus its heap fcinfo image
// (header + nargs NullableDatum tail) bump-allocated in 'mcx.
pub struct FuncFrame<'mcx> {
    // mcx-boxed so FuncCall's copy stays valid across frames-vec growth.
    pub flinfo: NonNull<FmgrInfo>,
    pub(crate) fcinfo: NonNull<u8>,
    pub nargs: u16,
    pub(crate) const_args: u16,
    pub(crate) const_null_args: u16,
    _mcx: PhantomData<&'mcx ()>,
}

const FCINFO_ARGS_OFFSET: usize = core::mem::offset_of!(LocalFcinfo<0>, args);

fn fcinfo_layout(nargs: usize) -> Layout {
    let (l, off) = Layout::new::<LocalFcinfo<0>>()
        .extend(Layout::array::<NullableDatum>(nargs).expect("fcinfo layout"))
        .expect("fcinfo layout");
    debug_assert!(nargs == 0 || off == FCINFO_ARGS_OFFSET);
    l.pad_to_align()
}

impl<'mcx> FuncFrame<'mcx> {
    pub(crate) fn new_in(mcx: Mcx<'mcx>, flinfo: FmgrInfo, nargs: u16, collation: Oid) -> PgResult<Self> {
        let fl_layout = Layout::new::<FmgrInfo>();
        let fl: NonNull<FmgrInfo> =
            mcx.allocate(fl_layout).map_err(|_| mcx.oom(fl_layout.size()))?.cast();
        // SAFETY: fresh exclusive allocation; fn_extra released via
        // release_frames, never by arena drop (C fn_mcxt shape).
        unsafe { fl.write(flinfo) };
        let flinfo = fl;
        let layout = fcinfo_layout(nargs as usize);
        let raw = mcx.allocate(layout).map_err(|_| mcx.oom(layout.size()))?;
        let base: NonNull<u8> = raw.cast();
        // SAFETY: fresh allocation of fcinfo_layout(nargs) bytes; header is a
        // POD LocalFcinfo<0> prefix and the args tail is zeroed NullableDatum.
        unsafe {
            base.cast::<LocalFcinfo<0>>().write(LocalFcinfo::<0>::new(collation));
            (*base.as_ptr().cast::<LocalFcinfo<0>>()).nargs = nargs as i16;
            core::ptr::write_bytes(
                base.as_ptr().add(FCINFO_ARGS_OFFSET),
                0,
                nargs as usize * core::mem::size_of::<NullableDatum>(),
            );
        }
        Ok(FuncFrame {
            flinfo,
            fcinfo: base,
            nargs,
            const_args: 0,
            const_null_args: 0,
            _mcx: PhantomData,
        })
    }

    #[inline(always)]
    pub(crate) fn arg_slot(&self, argno: usize) -> NonNull<NullableDatum> {
        debug_assert!(argno < self.nargs as usize);
        // SAFETY: argno < nargs, inside the frame's live fcinfo image.
        unsafe { arg_slot_of(self.fcinfo, argno) }
    }

    /// The call's input collation (fcinfo fncollation), for the lane qual
    /// walker: collation-sensitive predicates (text eq/LIKE over dict lanes)
    /// re-evaluate with it.
    #[inline]
    pub(crate) fn collation(&self) -> Oid {
        // SAFETY: the frame's fcinfo image is a live LocalFcinfo header
        // (written at frame build) followed by the args tail.
        unsafe { self.fcinfo.cast::<LocalFcinfo<0>>().as_ref() }.fncollation
    }
}

/// Emit-time address of a call's arg cell (kernel stencils bake it).
pub(crate) fn call_arg_addr(call: &FuncCall, argno: usize) -> *mut NullableDatum {
    debug_assert!(argno < call.nargs as usize);
    // SAFETY: live fcinfo image with nargs args.
    unsafe { arg_slot_of(call.fcinfo, argno) }.as_ptr()
}

/// # Safety
/// `base` is a live fcinfo image with more than `argno` args.
#[inline(always)]
pub(crate) unsafe fn arg_slot_of(base: NonNull<u8>, argno: usize) -> NonNull<NullableDatum> {
    unsafe {
        NonNull::new_unchecked(
            base.as_ptr()
                .add(FCINFO_ARGS_OFFSET + argno * core::mem::size_of::<NullableDatum>())
                .cast(),
        )
    }
}

/// # Safety
/// `base` is a live fcinfo image of at least `nargs` args allocated by
/// [`FuncFrame::new_in`], with no other live reference for the returned
/// borrow's duration.
#[inline(always)]
pub(crate) unsafe fn fcinfo_mut<'a>(
    base: NonNull<u8>,
    nargs: u16,
) -> &'a mut FunctionCallInfoBaseData {
    let fat = core::ptr::slice_from_raw_parts_mut(
        base.as_ptr().cast::<NullableDatum>(),
        nargs as usize,
    ) as *mut FunctionCallInfoBaseData;
    unsafe { &mut *fat }
}

// Monomorphized comparison kernels (perf-doctrine rule 11): the in-core int
// comparator bodies (int.c/int8.c, all strict, error-free) inlined behind a
// closed enum, selected by fn_oid at ready time. This is lever 4's beat-C
// move: C reaches these bodies only through the fmgr pointer (or an LLVM JIT).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum CmpOp {
    Int4Eq,
    Int4Ne,
    Int4Lt,
    Int4Le,
    Int4Gt,
    Int4Ge,
    Int8Eq,
    Int8Ne,
    Int8Lt,
    Int8Le,
    Int8Gt,
    Int8Ge,
    Int2Eq,
    Int2Ne,
    Int2Lt,
    Int2Le,
    Int2Gt,
    Int2Ge,
    Int84Eq,
    Int84Ne,
    Int84Lt,
    Int84Le,
    Int84Gt,
    Int84Ge,
    Int48Eq,
    Int48Ne,
    Int48Lt,
    Int48Le,
    Int48Gt,
    Int48Ge,
    // censusgaps additions: the stitch-vocabulary comparators the AOT census
    // was missing. int24/int42 are C's int2-vs-int4 promotion compares (int.c:
    // the int16 widens to int32, no traps); Oid is the unsigned 32-bit compare
    // (oid.c); the float bodies are float.h's NaN-aware total order (all NaNs
    // equal, NaN > every non-NaN, -0 == +0) — Float48/84 promote the float4
    // side to float8 exactly, per C's `(float8) arg` casts.
    Int24Eq,
    Int24Ne,
    Int24Lt,
    Int24Le,
    Int24Gt,
    Int24Ge,
    Int42Eq,
    Int42Ne,
    Int42Lt,
    Int42Le,
    Int42Gt,
    Int42Ge,
    OidEq,
    OidNe,
    OidLt,
    OidLe,
    OidGt,
    OidGe,
    Float4Eq,
    Float4Ne,
    Float4Lt,
    Float4Le,
    Float4Gt,
    Float4Ge,
    Float8Eq,
    Float8Ne,
    Float8Lt,
    Float8Le,
    Float8Gt,
    Float8Ge,
    Float48Eq,
    Float48Ne,
    Float48Lt,
    Float48Le,
    Float48Gt,
    Float48Ge,
    Float84Eq,
    Float84Ne,
    Float84Lt,
    Float84Le,
    Float84Gt,
    Float84Ge,
}

impl CmpOp {
    // Admission now consulted from the central batch-function registry
    // (`lanereg`, design §3a): the OID→comparator table lives there as the
    // AotQualCmp tier entries; here we only decode the registry's neutral
    // `CmpShape` into this crate's `CmpOp` selector. Every width family the
    // registry's in-tree AOT tier carries decodes (the legacy 5 int families
    // plus the censusgaps int24/int42/oid/float families, plus the
    // ne-admission census-close date/timestamp/timestamptz aliases — plain
    // int compares at I4/I8, sentinels included, per date.c/timestamp.c).
    // The conformance test in tests.rs pins this to the exact 90-OID golden
    // mapping.
    pub fn for_fn_oid(oid: Oid) -> Option<CmpOp> {
        Some(CmpOp::from_lanereg_shape(::lanereg::aot_qual_cmp(oid)?))
    }

    fn from_lanereg_shape(s: ::lanereg::CmpShape) -> CmpOp {
        use ::lanereg::{CmpPred as P, CmpWidth as W};
        match (s.width, s.pred) {
            (W::I4, P::Eq) => CmpOp::Int4Eq,
            (W::I4, P::Ne) => CmpOp::Int4Ne,
            (W::I4, P::Lt) => CmpOp::Int4Lt,
            (W::I4, P::Le) => CmpOp::Int4Le,
            (W::I4, P::Gt) => CmpOp::Int4Gt,
            (W::I4, P::Ge) => CmpOp::Int4Ge,
            (W::I8, P::Eq) => CmpOp::Int8Eq,
            (W::I8, P::Ne) => CmpOp::Int8Ne,
            (W::I8, P::Lt) => CmpOp::Int8Lt,
            (W::I8, P::Le) => CmpOp::Int8Le,
            (W::I8, P::Gt) => CmpOp::Int8Gt,
            (W::I8, P::Ge) => CmpOp::Int8Ge,
            (W::I2, P::Eq) => CmpOp::Int2Eq,
            (W::I2, P::Ne) => CmpOp::Int2Ne,
            (W::I2, P::Lt) => CmpOp::Int2Lt,
            (W::I2, P::Le) => CmpOp::Int2Le,
            (W::I2, P::Gt) => CmpOp::Int2Gt,
            (W::I2, P::Ge) => CmpOp::Int2Ge,
            (W::I84, P::Eq) => CmpOp::Int84Eq,
            (W::I84, P::Ne) => CmpOp::Int84Ne,
            (W::I84, P::Lt) => CmpOp::Int84Lt,
            (W::I84, P::Le) => CmpOp::Int84Le,
            (W::I84, P::Gt) => CmpOp::Int84Gt,
            (W::I84, P::Ge) => CmpOp::Int84Ge,
            (W::I48, P::Eq) => CmpOp::Int48Eq,
            (W::I48, P::Ne) => CmpOp::Int48Ne,
            (W::I48, P::Lt) => CmpOp::Int48Lt,
            (W::I48, P::Le) => CmpOp::Int48Le,
            (W::I48, P::Gt) => CmpOp::Int48Gt,
            (W::I48, P::Ge) => CmpOp::Int48Ge,
            (W::I24, P::Eq) => CmpOp::Int24Eq,
            (W::I24, P::Ne) => CmpOp::Int24Ne,
            (W::I24, P::Lt) => CmpOp::Int24Lt,
            (W::I24, P::Le) => CmpOp::Int24Le,
            (W::I24, P::Gt) => CmpOp::Int24Gt,
            (W::I24, P::Ge) => CmpOp::Int24Ge,
            (W::I42, P::Eq) => CmpOp::Int42Eq,
            (W::I42, P::Ne) => CmpOp::Int42Ne,
            (W::I42, P::Lt) => CmpOp::Int42Lt,
            (W::I42, P::Le) => CmpOp::Int42Le,
            (W::I42, P::Gt) => CmpOp::Int42Gt,
            (W::I42, P::Ge) => CmpOp::Int42Ge,
            (W::Oid, P::Eq) => CmpOp::OidEq,
            (W::Oid, P::Ne) => CmpOp::OidNe,
            (W::Oid, P::Lt) => CmpOp::OidLt,
            (W::Oid, P::Le) => CmpOp::OidLe,
            (W::Oid, P::Gt) => CmpOp::OidGt,
            (W::Oid, P::Ge) => CmpOp::OidGe,
            (W::F4, P::Eq) => CmpOp::Float4Eq,
            (W::F4, P::Ne) => CmpOp::Float4Ne,
            (W::F4, P::Lt) => CmpOp::Float4Lt,
            (W::F4, P::Le) => CmpOp::Float4Le,
            (W::F4, P::Gt) => CmpOp::Float4Gt,
            (W::F4, P::Ge) => CmpOp::Float4Ge,
            (W::F8, P::Eq) => CmpOp::Float8Eq,
            (W::F8, P::Ne) => CmpOp::Float8Ne,
            (W::F8, P::Lt) => CmpOp::Float8Lt,
            (W::F8, P::Le) => CmpOp::Float8Le,
            (W::F8, P::Gt) => CmpOp::Float8Gt,
            (W::F8, P::Ge) => CmpOp::Float8Ge,
            (W::F48, P::Eq) => CmpOp::Float48Eq,
            (W::F48, P::Ne) => CmpOp::Float48Ne,
            (W::F48, P::Lt) => CmpOp::Float48Lt,
            (W::F48, P::Le) => CmpOp::Float48Le,
            (W::F48, P::Gt) => CmpOp::Float48Gt,
            (W::F48, P::Ge) => CmpOp::Float48Ge,
            (W::F84, P::Eq) => CmpOp::Float84Eq,
            (W::F84, P::Ne) => CmpOp::Float84Ne,
            (W::F84, P::Lt) => CmpOp::Float84Lt,
            (W::F84, P::Le) => CmpOp::Float84Le,
            (W::F84, P::Gt) => CmpOp::Float84Gt,
            (W::F84, P::Ge) => CmpOp::Float84Ge,
        }
    }

    // arg-order flip for a fused (const, var) call evaluated as cmp(var, const).
    pub fn commuted(self) -> CmpOp {
        match self {
            CmpOp::Int4Lt => CmpOp::Int4Gt,
            CmpOp::Int4Le => CmpOp::Int4Ge,
            CmpOp::Int4Gt => CmpOp::Int4Lt,
            CmpOp::Int4Ge => CmpOp::Int4Le,
            CmpOp::Int8Lt => CmpOp::Int8Gt,
            CmpOp::Int8Le => CmpOp::Int8Ge,
            CmpOp::Int8Gt => CmpOp::Int8Lt,
            CmpOp::Int8Ge => CmpOp::Int8Le,
            CmpOp::Int2Lt => CmpOp::Int2Gt,
            CmpOp::Int2Le => CmpOp::Int2Ge,
            CmpOp::Int2Gt => CmpOp::Int2Lt,
            CmpOp::Int2Ge => CmpOp::Int2Le,
            CmpOp::Int84Lt => CmpOp::Int48Gt,
            CmpOp::Int84Le => CmpOp::Int48Ge,
            CmpOp::Int84Gt => CmpOp::Int48Lt,
            CmpOp::Int84Ge => CmpOp::Int48Le,
            CmpOp::Int84Eq => CmpOp::Int48Eq,
            CmpOp::Int84Ne => CmpOp::Int48Ne,
            CmpOp::Int48Lt => CmpOp::Int84Gt,
            CmpOp::Int48Le => CmpOp::Int84Ge,
            CmpOp::Int48Gt => CmpOp::Int84Lt,
            CmpOp::Int48Ge => CmpOp::Int84Le,
            CmpOp::Int48Eq => CmpOp::Int84Eq,
            CmpOp::Int48Ne => CmpOp::Int84Ne,
            CmpOp::Int24Lt => CmpOp::Int42Gt,
            CmpOp::Int24Le => CmpOp::Int42Ge,
            CmpOp::Int24Gt => CmpOp::Int42Lt,
            CmpOp::Int24Ge => CmpOp::Int42Le,
            CmpOp::Int24Eq => CmpOp::Int42Eq,
            CmpOp::Int24Ne => CmpOp::Int42Ne,
            CmpOp::Int42Lt => CmpOp::Int24Gt,
            CmpOp::Int42Le => CmpOp::Int24Ge,
            CmpOp::Int42Gt => CmpOp::Int24Lt,
            CmpOp::Int42Ge => CmpOp::Int24Le,
            CmpOp::Int42Eq => CmpOp::Int24Eq,
            CmpOp::Int42Ne => CmpOp::Int24Ne,
            CmpOp::OidLt => CmpOp::OidGt,
            CmpOp::OidLe => CmpOp::OidGe,
            CmpOp::OidGt => CmpOp::OidLt,
            CmpOp::OidGe => CmpOp::OidLe,
            // The float order is total (NaN sorts greatest, -0 == +0), so
            // argument commutation is exactly predicate reflection.
            CmpOp::Float4Lt => CmpOp::Float4Gt,
            CmpOp::Float4Le => CmpOp::Float4Ge,
            CmpOp::Float4Gt => CmpOp::Float4Lt,
            CmpOp::Float4Ge => CmpOp::Float4Le,
            CmpOp::Float8Lt => CmpOp::Float8Gt,
            CmpOp::Float8Le => CmpOp::Float8Ge,
            CmpOp::Float8Gt => CmpOp::Float8Lt,
            CmpOp::Float8Ge => CmpOp::Float8Le,
            CmpOp::Float48Lt => CmpOp::Float84Gt,
            CmpOp::Float48Le => CmpOp::Float84Ge,
            CmpOp::Float48Gt => CmpOp::Float84Lt,
            CmpOp::Float48Ge => CmpOp::Float84Le,
            CmpOp::Float48Eq => CmpOp::Float84Eq,
            CmpOp::Float48Ne => CmpOp::Float84Ne,
            CmpOp::Float84Lt => CmpOp::Float48Gt,
            CmpOp::Float84Le => CmpOp::Float48Ge,
            CmpOp::Float84Gt => CmpOp::Float48Lt,
            CmpOp::Float84Ge => CmpOp::Float48Le,
            CmpOp::Float84Eq => CmpOp::Float48Eq,
            CmpOp::Float84Ne => CmpOp::Float48Ne,
            other => other,
        }
    }

    #[inline(always)]
    pub fn eval(self, a: Datum, b: Datum) -> bool {
        match self {
            CmpOp::Int4Eq => a.as_i32() == b.as_i32(),
            CmpOp::Int4Ne => a.as_i32() != b.as_i32(),
            CmpOp::Int4Lt => a.as_i32() < b.as_i32(),
            CmpOp::Int4Le => a.as_i32() <= b.as_i32(),
            CmpOp::Int4Gt => a.as_i32() > b.as_i32(),
            CmpOp::Int4Ge => a.as_i32() >= b.as_i32(),
            CmpOp::Int8Eq => a.as_i64() == b.as_i64(),
            CmpOp::Int8Ne => a.as_i64() != b.as_i64(),
            CmpOp::Int8Lt => a.as_i64() < b.as_i64(),
            CmpOp::Int8Le => a.as_i64() <= b.as_i64(),
            CmpOp::Int8Gt => a.as_i64() > b.as_i64(),
            CmpOp::Int8Ge => a.as_i64() >= b.as_i64(),
            CmpOp::Int2Eq => a.as_i16() == b.as_i16(),
            CmpOp::Int2Ne => a.as_i16() != b.as_i16(),
            CmpOp::Int2Lt => a.as_i16() < b.as_i16(),
            CmpOp::Int2Le => a.as_i16() <= b.as_i16(),
            CmpOp::Int2Gt => a.as_i16() > b.as_i16(),
            CmpOp::Int2Ge => a.as_i16() >= b.as_i16(),
            CmpOp::Int84Eq => a.as_i64() == b.as_i32() as i64,
            CmpOp::Int84Ne => a.as_i64() != b.as_i32() as i64,
            CmpOp::Int84Lt => a.as_i64() < b.as_i32() as i64,
            CmpOp::Int84Le => a.as_i64() <= b.as_i32() as i64,
            CmpOp::Int84Gt => a.as_i64() > b.as_i32() as i64,
            CmpOp::Int84Ge => a.as_i64() >= b.as_i32() as i64,
            CmpOp::Int48Eq => (a.as_i32() as i64) == b.as_i64(),
            CmpOp::Int48Ne => (a.as_i32() as i64) != b.as_i64(),
            CmpOp::Int48Lt => (a.as_i32() as i64) < b.as_i64(),
            CmpOp::Int48Le => (a.as_i32() as i64) <= b.as_i64(),
            CmpOp::Int48Gt => (a.as_i32() as i64) > b.as_i64(),
            CmpOp::Int48Ge => (a.as_i32() as i64) >= b.as_i64(),
            CmpOp::Int24Eq => (a.as_i16() as i32) == b.as_i32(),
            CmpOp::Int24Ne => (a.as_i16() as i32) != b.as_i32(),
            CmpOp::Int24Lt => (a.as_i16() as i32) < b.as_i32(),
            CmpOp::Int24Le => (a.as_i16() as i32) <= b.as_i32(),
            CmpOp::Int24Gt => (a.as_i16() as i32) > b.as_i32(),
            CmpOp::Int24Ge => (a.as_i16() as i32) >= b.as_i32(),
            CmpOp::Int42Eq => a.as_i32() == (b.as_i16() as i32),
            CmpOp::Int42Ne => a.as_i32() != (b.as_i16() as i32),
            CmpOp::Int42Lt => a.as_i32() < (b.as_i16() as i32),
            CmpOp::Int42Le => a.as_i32() <= (b.as_i16() as i32),
            CmpOp::Int42Gt => a.as_i32() > (b.as_i16() as i32),
            CmpOp::Int42Ge => a.as_i32() >= (b.as_i16() as i32),
            CmpOp::OidEq => a.as_u32() == b.as_u32(),
            CmpOp::OidNe => a.as_u32() != b.as_u32(),
            CmpOp::OidLt => a.as_u32() < b.as_u32(),
            CmpOp::OidLe => a.as_u32() <= b.as_u32(),
            CmpOp::OidGt => a.as_u32() > b.as_u32(),
            CmpOp::OidGe => a.as_u32() >= b.as_u32(),
            CmpOp::Float4Eq => f4_eq(a.as_f32(), b.as_f32()),
            CmpOp::Float4Ne => f4_ne(a.as_f32(), b.as_f32()),
            CmpOp::Float4Lt => f4_lt(a.as_f32(), b.as_f32()),
            CmpOp::Float4Le => f4_le(a.as_f32(), b.as_f32()),
            CmpOp::Float4Gt => f4_gt(a.as_f32(), b.as_f32()),
            CmpOp::Float4Ge => f4_ge(a.as_f32(), b.as_f32()),
            CmpOp::Float8Eq => f8_eq(a.as_f64(), b.as_f64()),
            CmpOp::Float8Ne => f8_ne(a.as_f64(), b.as_f64()),
            CmpOp::Float8Lt => f8_lt(a.as_f64(), b.as_f64()),
            CmpOp::Float8Le => f8_le(a.as_f64(), b.as_f64()),
            CmpOp::Float8Gt => f8_gt(a.as_f64(), b.as_f64()),
            CmpOp::Float8Ge => f8_ge(a.as_f64(), b.as_f64()),
            CmpOp::Float48Eq => f8_eq(a.as_f32() as f64, b.as_f64()),
            CmpOp::Float48Ne => f8_ne(a.as_f32() as f64, b.as_f64()),
            CmpOp::Float48Lt => f8_lt(a.as_f32() as f64, b.as_f64()),
            CmpOp::Float48Le => f8_le(a.as_f32() as f64, b.as_f64()),
            CmpOp::Float48Gt => f8_gt(a.as_f32() as f64, b.as_f64()),
            CmpOp::Float48Ge => f8_ge(a.as_f32() as f64, b.as_f64()),
            CmpOp::Float84Eq => f8_eq(a.as_f64(), b.as_f32() as f64),
            CmpOp::Float84Ne => f8_ne(a.as_f64(), b.as_f32() as f64),
            CmpOp::Float84Lt => f8_lt(a.as_f64(), b.as_f32() as f64),
            CmpOp::Float84Le => f8_le(a.as_f64(), b.as_f32() as f64),
            CmpOp::Float84Gt => f8_gt(a.as_f64(), b.as_f32() as f64),
            CmpOp::Float84Ge => f8_ge(a.as_f64(), b.as_f32() as f64),
        }
    }
}

// float.h's NaN-aware comparison bodies (all NaNs equal, NaN > every non-NaN,
// -0 == +0), byte-for-byte the ported `adt_float::float{4,8}_{eq,..,ge}`
// forms (the parity test in tests.rs binds them to the adt_float originals).
// Duplicated here (12 one-liners) instead of a crate dependency so the qual
// kernel's hot loop bodies stay local to this crate.
#[inline(always)]
fn f4_eq(a: f32, b: f32) -> bool {
    a == b || (a.is_nan() && b.is_nan())
}
#[inline(always)]
fn f4_ne(a: f32, b: f32) -> bool {
    if a.is_nan() { !b.is_nan() } else { b.is_nan() || a != b }
}
#[inline(always)]
fn f4_lt(a: f32, b: f32) -> bool {
    !a.is_nan() && (b.is_nan() || a < b)
}
#[inline(always)]
fn f4_le(a: f32, b: f32) -> bool {
    b.is_nan() || (!a.is_nan() && a <= b)
}
#[inline(always)]
fn f4_gt(a: f32, b: f32) -> bool {
    !b.is_nan() && (a.is_nan() || a > b)
}
#[inline(always)]
fn f4_ge(a: f32, b: f32) -> bool {
    a.is_nan() || (!b.is_nan() && a >= b)
}
#[inline(always)]
fn f8_eq(a: f64, b: f64) -> bool {
    a == b || (a.is_nan() && b.is_nan())
}
#[inline(always)]
fn f8_ne(a: f64, b: f64) -> bool {
    if a.is_nan() { !b.is_nan() } else { b.is_nan() || a != b }
}
#[inline(always)]
fn f8_lt(a: f64, b: f64) -> bool {
    !a.is_nan() && (b.is_nan() || a < b)
}
#[inline(always)]
fn f8_le(a: f64, b: f64) -> bool {
    b.is_nan() || (!a.is_nan() && a <= b)
}
#[inline(always)]
fn f8_gt(a: f64, b: f64) -> bool {
    !b.is_nan() && (a.is_nan() || a > b)
}
#[inline(always)]
fn f8_ge(a: f64, b: f64) -> bool {
    a.is_nan() || (!b.is_nan() && a >= b)
}

/// `n` int8inc transitions collapsed into one add. false = the caller must
/// re-run this batch through the per-row kernel: an in-batch overflow (the
/// per-row walk ereports "bigint out of range" at exactly C's row — trans+n
/// overflows iff some row's increment does) or a null transvalue under a
/// non-strict call (per-row resolves it; count(*)'s initcond 0 never is).
#[inline]
pub fn agg_count_star_advance(pergroup: NonNull<AggPerGroup>, strict: bool, n: u32) -> bool {
    // SAFETY: once-allocated stable pergroup, sole access here (the kernel
    // AggTransByVal arm's contract).
    unsafe {
        let pg = pergroup.as_ptr();
        if (*pg).trans_value_is_null {
            // Strict: every one of the n calls is skipped.
            return strict;
        }
        match (*pg).trans_value.as_i64().checked_add(n as i64) {
            Some(v) => {
                (*pg).trans_value = Datum::from_i64(v);
                (*pg).trans_value_is_null = false;
                true
            }
            None => false,
        }
    }
}

// Batched ExecQual over an SoA column (comparisons only — non-erroring, so
// evaluation order is unobservable): selection bit = !isnull && cmp(v, k).
// Chunked so LLVM can vectorize the compare and reduce the bit-pack per word.
pub fn qual_bitmap_cmp_const(
    cmp: CmpOp,
    konst: Datum,
    values: &[Datum],
    isnull: &[bool],
    sel: &mut [u64],
) {
    debug_assert!(values.len() == isnull.len() && sel.len() >= values.len().div_ceil(64));
    macro_rules! lanes {
        ($pred:expr) => {
            bitmap_loop(values, isnull, sel, $pred)
        };
    }
    match cmp {
        CmpOp::Int4Eq => lanes!(|v: Datum| v.as_i32() == konst.as_i32()),
        CmpOp::Int4Ne => lanes!(|v: Datum| v.as_i32() != konst.as_i32()),
        CmpOp::Int4Lt => lanes!(|v: Datum| v.as_i32() < konst.as_i32()),
        CmpOp::Int4Le => lanes!(|v: Datum| v.as_i32() <= konst.as_i32()),
        CmpOp::Int4Gt => lanes!(|v: Datum| v.as_i32() > konst.as_i32()),
        CmpOp::Int4Ge => lanes!(|v: Datum| v.as_i32() >= konst.as_i32()),
        CmpOp::Int8Eq => lanes!(|v: Datum| v.as_i64() == konst.as_i64()),
        CmpOp::Int8Ne => lanes!(|v: Datum| v.as_i64() != konst.as_i64()),
        CmpOp::Int8Lt => lanes!(|v: Datum| v.as_i64() < konst.as_i64()),
        CmpOp::Int8Le => lanes!(|v: Datum| v.as_i64() <= konst.as_i64()),
        CmpOp::Int8Gt => lanes!(|v: Datum| v.as_i64() > konst.as_i64()),
        CmpOp::Int8Ge => lanes!(|v: Datum| v.as_i64() >= konst.as_i64()),
        CmpOp::Int2Eq => lanes!(|v: Datum| v.as_i16() == konst.as_i16()),
        CmpOp::Int2Ne => lanes!(|v: Datum| v.as_i16() != konst.as_i16()),
        CmpOp::Int2Lt => lanes!(|v: Datum| v.as_i16() < konst.as_i16()),
        CmpOp::Int2Le => lanes!(|v: Datum| v.as_i16() <= konst.as_i16()),
        CmpOp::Int2Gt => lanes!(|v: Datum| v.as_i16() > konst.as_i16()),
        CmpOp::Int2Ge => lanes!(|v: Datum| v.as_i16() >= konst.as_i16()),
        CmpOp::Int84Eq => lanes!(|v: Datum| v.as_i64() == konst.as_i32() as i64),
        CmpOp::Int84Ne => lanes!(|v: Datum| v.as_i64() != konst.as_i32() as i64),
        CmpOp::Int84Lt => lanes!(|v: Datum| v.as_i64() < konst.as_i32() as i64),
        CmpOp::Int84Le => lanes!(|v: Datum| v.as_i64() <= konst.as_i32() as i64),
        CmpOp::Int84Gt => lanes!(|v: Datum| v.as_i64() > konst.as_i32() as i64),
        CmpOp::Int84Ge => lanes!(|v: Datum| v.as_i64() >= konst.as_i32() as i64),
        CmpOp::Int48Eq => lanes!(|v: Datum| (v.as_i32() as i64) == konst.as_i64()),
        CmpOp::Int48Ne => lanes!(|v: Datum| (v.as_i32() as i64) != konst.as_i64()),
        CmpOp::Int48Lt => lanes!(|v: Datum| (v.as_i32() as i64) < konst.as_i64()),
        CmpOp::Int48Le => lanes!(|v: Datum| (v.as_i32() as i64) <= konst.as_i64()),
        CmpOp::Int48Gt => lanes!(|v: Datum| (v.as_i32() as i64) > konst.as_i64()),
        CmpOp::Int48Ge => lanes!(|v: Datum| (v.as_i32() as i64) >= konst.as_i64()),
        CmpOp::Int24Eq => lanes!(|v: Datum| (v.as_i16() as i32) == konst.as_i32()),
        CmpOp::Int24Ne => lanes!(|v: Datum| (v.as_i16() as i32) != konst.as_i32()),
        CmpOp::Int24Lt => lanes!(|v: Datum| (v.as_i16() as i32) < konst.as_i32()),
        CmpOp::Int24Le => lanes!(|v: Datum| (v.as_i16() as i32) <= konst.as_i32()),
        CmpOp::Int24Gt => lanes!(|v: Datum| (v.as_i16() as i32) > konst.as_i32()),
        CmpOp::Int24Ge => lanes!(|v: Datum| (v.as_i16() as i32) >= konst.as_i32()),
        CmpOp::Int42Eq => lanes!(|v: Datum| v.as_i32() == (konst.as_i16() as i32)),
        CmpOp::Int42Ne => lanes!(|v: Datum| v.as_i32() != (konst.as_i16() as i32)),
        CmpOp::Int42Lt => lanes!(|v: Datum| v.as_i32() < (konst.as_i16() as i32)),
        CmpOp::Int42Le => lanes!(|v: Datum| v.as_i32() <= (konst.as_i16() as i32)),
        CmpOp::Int42Gt => lanes!(|v: Datum| v.as_i32() > (konst.as_i16() as i32)),
        CmpOp::Int42Ge => lanes!(|v: Datum| v.as_i32() >= (konst.as_i16() as i32)),
        CmpOp::OidEq => lanes!(|v: Datum| v.as_u32() == konst.as_u32()),
        CmpOp::OidNe => lanes!(|v: Datum| v.as_u32() != konst.as_u32()),
        CmpOp::OidLt => lanes!(|v: Datum| v.as_u32() < konst.as_u32()),
        CmpOp::OidLe => lanes!(|v: Datum| v.as_u32() <= konst.as_u32()),
        CmpOp::OidGt => lanes!(|v: Datum| v.as_u32() > konst.as_u32()),
        CmpOp::OidGe => lanes!(|v: Datum| v.as_u32() >= konst.as_u32()),
        CmpOp::Float4Eq => lanes!(|v: Datum| f4_eq(v.as_f32(), konst.as_f32())),
        CmpOp::Float4Ne => lanes!(|v: Datum| f4_ne(v.as_f32(), konst.as_f32())),
        CmpOp::Float4Lt => lanes!(|v: Datum| f4_lt(v.as_f32(), konst.as_f32())),
        CmpOp::Float4Le => lanes!(|v: Datum| f4_le(v.as_f32(), konst.as_f32())),
        CmpOp::Float4Gt => lanes!(|v: Datum| f4_gt(v.as_f32(), konst.as_f32())),
        CmpOp::Float4Ge => lanes!(|v: Datum| f4_ge(v.as_f32(), konst.as_f32())),
        CmpOp::Float8Eq => lanes!(|v: Datum| f8_eq(v.as_f64(), konst.as_f64())),
        CmpOp::Float8Ne => lanes!(|v: Datum| f8_ne(v.as_f64(), konst.as_f64())),
        CmpOp::Float8Lt => lanes!(|v: Datum| f8_lt(v.as_f64(), konst.as_f64())),
        CmpOp::Float8Le => lanes!(|v: Datum| f8_le(v.as_f64(), konst.as_f64())),
        CmpOp::Float8Gt => lanes!(|v: Datum| f8_gt(v.as_f64(), konst.as_f64())),
        CmpOp::Float8Ge => lanes!(|v: Datum| f8_ge(v.as_f64(), konst.as_f64())),
        CmpOp::Float48Eq => lanes!(|v: Datum| f8_eq(v.as_f32() as f64, konst.as_f64())),
        CmpOp::Float48Ne => lanes!(|v: Datum| f8_ne(v.as_f32() as f64, konst.as_f64())),
        CmpOp::Float48Lt => lanes!(|v: Datum| f8_lt(v.as_f32() as f64, konst.as_f64())),
        CmpOp::Float48Le => lanes!(|v: Datum| f8_le(v.as_f32() as f64, konst.as_f64())),
        CmpOp::Float48Gt => lanes!(|v: Datum| f8_gt(v.as_f32() as f64, konst.as_f64())),
        CmpOp::Float48Ge => lanes!(|v: Datum| f8_ge(v.as_f32() as f64, konst.as_f64())),
        CmpOp::Float84Eq => lanes!(|v: Datum| f8_eq(v.as_f64(), konst.as_f32() as f64)),
        CmpOp::Float84Ne => lanes!(|v: Datum| f8_ne(v.as_f64(), konst.as_f32() as f64)),
        CmpOp::Float84Lt => lanes!(|v: Datum| f8_lt(v.as_f64(), konst.as_f32() as f64)),
        CmpOp::Float84Le => lanes!(|v: Datum| f8_le(v.as_f64(), konst.as_f32() as f64)),
        CmpOp::Float84Gt => lanes!(|v: Datum| f8_gt(v.as_f64(), konst.as_f32() as f64)),
        CmpOp::Float84Ge => lanes!(|v: Datum| f8_ge(v.as_f64(), konst.as_f32() as f64)),
    }
}

#[inline(always)]
fn bitmap_loop(
    values: &[Datum],
    isnull: &[bool],
    sel: &mut [u64],
    pred: impl Fn(Datum) -> bool,
) {
    for (w, (vch, nch)) in values.chunks(64).zip(isnull.chunks(64)).enumerate() {
        let mut word = 0u64;
        for i in 0..vch.len() {
            word |= ((!nch[i] && pred(vch[i])) as u64) << i;
        }
        sel[w] = word;
    }
}

// Fast-path evaluators selected once at ready time from the compiled program
// shape (C ExecReadyInterpretedExpr's ExecJust* selection, plus the fused
// monomorphized shapes C has no non-JIT equivalent for).
#[derive(Clone, Copy, Debug)]
pub enum Kernel {
    Program,
    JustConst { value: Datum, isnull: bool },
    JustConstAssign { value: Datum, isnull: bool, resultnum: u16 },
    JustVar { src: SlotSrc, attnum: u16 },
    JustVarVirt { src: SlotSrc, attnum: u16 },
    JustAssignVar { src: SlotSrc, attnum: u16, resultnum: u16 },
    JustAssignVarVirt { src: SlotSrc, attnum: u16, resultnum: u16 },
    QualScanVarCmpConst { attnum: u16, konst: Datum, cmp: CmpOp },
    QualVarCmpVar { a_src: SlotSrc, a_attnum: u16, b_src: SlotSrc, b_attnum: u16, cmp: CmpOp },
    Hash32Var { src: SlotSrc, attnum: u16, frame: u32 },
    JustFunc { fn_addr: PGFunction, frame: u32, nargs: u16, strict: bool },
    // Argless byval transition (count(*)-class 2-step programs): the whole
    // per-row program without the interpreter loop (ExecJust* precedent).
    AggTransByVal { call: FuncCall, pergroup: NonNull<AggPerGroup>, strict: bool },
    AggTransByValThin { call: CallThin, pergroup: NonNull<AggPerGroup>, strict: bool },
}

const _: () = assert!(core::mem::size_of::<Kernel>() <= 48);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum SlotSrc {
    Scan,
    Inner,
    Outer,
    // RETURNING OLD/NEW rows (C econtext ecxt_oldtuple/ecxt_newtuple).
    Old,
    New,
}

pub struct ExprState<'mcx> {
    pub(crate) steps: PgVec<'mcx, Step>,
    pub(crate) frames: PgVec<'mcx, FuncFrame<'mcx>>,
    pub(crate) saop_tables: PgVec<'mcx, SaopTable<'mcx>>,
    pub(crate) kernel: Kernel,
    // Multi-clause scan-Var-cmp-Const census (lane-v2 batched qual tiers);
    // the 1-clause case lives in `kernel` as QualScanVarCmpConst.
    pub(crate) scan_cmp_clauses: Option<ScanCmpClauses>,
    // Contains-LIKE qual census (lane-v2 strsearch qual kernel).
    pub(crate) scan_contains_clause: Option<ScanContainsClause>,
    // Scan-projection census (lane-v2 stitched-projection tier).
    pub(crate) scan_proj_cols: Option<ScanProjCols>,
    // Expression-group-key census (lane-v2 expr-key grouping tier).
    pub(crate) scan_proj_expr_key: Option<ScanProjExprKey>,
    pub(crate) flags: u8,
    // C ExprState.resvalue/resnull: mcx-allocated result cell — OutRef raw
    // access carries no Rust borrow provenance.
    pub(crate) resnd: NonNull<NullableDatum>,
    // C ExprState.innermost_caseval/casenull: compile-time only.
    pub(crate) innermost_case: Option<NonNull<NullableDatum>>,
    // PARAM_EXEC ids this expression reads; the owning node resolves pending
    // initplans against these before evaluation (nodeSubplan.c lane).
    pub(crate) param_exec_deps: PgVec<'mcx, u32>,
    // C ExprState.innermost_domainval/innermost_domainnull: compile-time only.
    pub(crate) innermost_domain: Option<OutRef>,
    // resmcx fields of allocating array-op step states, armed with frames.
    pub(crate) alloc_mcx_slots: PgVec<'mcx, NonNull<crate::arrayops::ResMcx>>,
    // C ExprState.escontext: compile-time only; behavior-expr coercions under
    // a JsonExpr compile against the owning JsonExprState's ErrorSaveNode.
    pub(crate) escontext: Option<NonNull<::types_fmgr::ErrorSaveNode>>,
    // C EEOP_CASE_TESTVAL_EXT stand-in: econtext caseValue collapses to one
    // compile-allocated cell the caller writes via set_case_test (JSON_TABLE).
    pub(crate) ext_case_test: Option<NonNull<NullableDatum>>,
    pub(crate) allow_ext_case_test: bool,
    // Copy-and-patch kernel entry (jit.rs); the code block itself is owned by
    // the executor session collector, which outlives this state.
    pub(crate) jit: Option<crate::jit::JitHandle>,
    // C mtstate->mt_merge_action stand-in: MERGE_SUPPORT_FUNC steps read this
    // compile-allocated cell; only MERGE RETURNING projections may compile it.
    pub(crate) merge_action_cell: Option<NonNull<Option<::types_nodes::nodes_enums::CmdType>>>,
    pub(crate) allow_merge_support: bool,
}

impl<'mcx> ExprState<'mcx> {
    // C makeNode(ExprState) + ExprEvalPushStep's 16-step first allocation: box written in place.
    #[inline]
    pub(crate) fn new_boxed_in(mcx: Mcx<'mcx>) -> PgResult<::mcx::PgBox<'mcx, ExprState<'mcx>>> {
        let layout = Layout::new::<ExprState<'mcx>>();
        let raw = mcx.allocate(layout).map_err(|_| mcx.oom(layout.size()))?;
        let p = raw.cast::<ExprState<'mcx>>();
        let rl = Layout::new::<NullableDatum>();
        let resnd: NonNull<NullableDatum> =
            mcx.allocate(rl).map_err(|_| mcx.oom(rl.size()))?.cast();
        // SAFETY: fresh exclusive allocation.
        unsafe { resnd.write(NullableDatum::null()) };
        // On steps-alloc failure the header chunk stays until reset (C's palloc-then-throw shape).
        let steps = ::mcx::vec_with_capacity_in(mcx, 16)?;
        // SAFETY: fresh exclusive layout-sized allocation from `mcx`; written once, then box-owned.
        unsafe {
            p.write(ExprState {
                steps,
                frames: PgVec::new_in(mcx),
                saop_tables: PgVec::new_in(mcx),
                kernel: Kernel::Program,
                scan_cmp_clauses: None,
                scan_contains_clause: None,
                scan_proj_cols: None,
                scan_proj_expr_key: None,
                flags: 0,
                resnd,
                innermost_case: None,
                param_exec_deps: PgVec::new_in(mcx),
                innermost_domain: None,
                alloc_mcx_slots: PgVec::new_in(mcx),
                escontext: None,
                ext_case_test: None,
                allow_ext_case_test: false,
                jit: None,
                merge_action_cell: None,
                allow_merge_support: false,
            });
            Ok(::mcx::PgBox::from_raw_in(p.as_ptr(), mcx))
        }
    }

    pub fn steps(&self) -> &[Step] {
        &self.steps
    }

    pub fn param_exec_deps(&self) -> &[u32] {
        &self.param_exec_deps
    }

    pub fn kernel(&self) -> Kernel {
        self.kernel
    }

    /// Hash32Var over `src` whose resolved fn is a total low-32 hash
    /// (hashint4/hashoid: hash_bytes_uint32 of the datum's low 32 bits,
    /// never errors) — the columnar precompute cover; 0-based key attnum.
    pub fn hash32var_low32(&self, src: SlotSrc) -> Option<u16> {
        let Kernel::Hash32Var { src: s, attnum, frame } = self.kernel else {
            return None;
        };
        if s != src {
            return None;
        }
        // SAFETY: mcx-boxed FmgrInfo owned by this state; read-only field.
        let oid = unsafe { (*self.frames[frame as usize].flinfo.as_ptr()).fn_oid };
        matches!(oid, 450 | 453).then_some(attnum)
    }

    /// count(*)-class transition — int8inc (oid 1219) over the transvalue
    /// alone — where the batched storeless drain may advance the group once
    /// per page batch. Returns (pergroup, strict).
    pub fn agg_count_star(&self) -> Option<(NonNull<AggPerGroup>, bool)> {
        let strict = match self.kernel {
            Kernel::AggTransByVal { strict, .. } | Kernel::AggTransByValThin { strict, .. } => {
                strict
            }
            _ => return None,
        };
        let (call, pergroup) = match self.steps.as_slice().first()? {
            Step::AggPlainTransByVal { call, pergroup }
            | Step::AggPlainTransStrictByVal { call, pergroup } => (call, pergroup),
            _ => return None,
        };
        // SAFETY: frame-owned mcx-boxed FmgrInfo, live for 'mcx.
        let oid = unsafe { call.flinfo.as_ref() }.fn_oid;
        (oid == 1219 && call.nargs == 1).then_some((*pergroup, strict))
    }

    /// Max attnum this expression demands of `src`'s slot (its FETCHSOME
    /// bound; 0 = none); None = shape unknown to the batch-deform planner.
    pub fn max_fetch(&self, src: SlotSrc) -> Option<i32> {
        match self.kernel {
            Kernel::Program => {
                let mut m = 0i32;
                for s in self.steps() {
                    match (s, src) {
                        (Step::ScanFetchSome { last_var }, SlotSrc::Scan)
                        | (Step::InnerFetchSome { last_var }, SlotSrc::Inner)
                        | (Step::OuterFetchSome { last_var }, SlotSrc::Outer) => {
                            m = m.max(*last_var as i32)
                        }
                        _ => {}
                    }
                }
                Some(m)
            }
            Kernel::AggTransByVal { .. }
            | Kernel::AggTransByValThin { .. }
            | Kernel::JustConst { .. }
            | Kernel::JustConstAssign { .. } => Some(0),
            Kernel::QualScanVarCmpConst { attnum, .. } => {
                Some(if src == SlotSrc::Scan { attnum as i32 + 1 } else { 0 })
            }
            Kernel::QualVarCmpVar { a_src, a_attnum, b_src, b_attnum, .. } => {
                let mut m = 0i32;
                if a_src == src {
                    m = a_attnum as i32 + 1;
                }
                if b_src == src {
                    m = m.max(b_attnum as i32 + 1);
                }
                Some(m)
            }
            _ => None,
        }
    }

    #[inline(always)]
    pub(crate) fn result_out(&self) -> OutRef {
        OutRef(self.resnd)
    }

    #[inline(always)]
    pub(crate) fn result_addr(&self) -> *mut NullableDatum {
        self.resnd.as_ptr()
    }

    #[inline(always)]
    pub(crate) fn is_result(&self, out: OutRef) -> bool {
        out.0 == self.resnd
    }

    pub fn is_qual(&self) -> bool {
        self.flags & EEO_FLAG_IS_QUAL != 0
    }

    /// The qual as an AND of scan-Var-CMP-Const clauses
    /// (1..=SCAN_CMP_MAX_CLAUSES), or None. 1 clause = the fused kernel;
    /// 2+ = the ready-time census. Non-erroring, non-volatile, subplan- and
    /// param-free by construction (in-core int comparators, strict 2-arg
    /// calls, compile-time non-null Consts).
    pub fn scan_cmp_const_clauses(&self) -> Option<ScanCmpClauses> {
        if let Kernel::QualScanVarCmpConst { attnum, konst, cmp } = self.kernel {
            let mut c = ScanCmpClauses {
                clauses: [(0, CmpOp::Int4Eq, Datum::null()); SCAN_CMP_MAX_CLAUSES],
                n: 1,
            };
            c.clauses[0] = (attnum, cmp, konst);
            return Some(c);
        }
        self.scan_cmp_clauses
    }

    /// The qual as one contains-class LIKE clause (`scan_var LIKE
    /// '%literal%'`), or None. Non-erroring, non-volatile, subplan- and
    /// param-free by construction (strict in-core `textlike` over one scan
    /// Var and one compile-time non-null Const, admission-gated pattern
    /// class / encoding). See [`ScanContainsClause`].
    pub fn scan_contains_clause(&self) -> Option<ScanContainsClause> {
        self.scan_contains_clause
    }

    /// The projection as `n` scan-Var / int-arith columns in resultnum order
    /// (the ready-time census), or None (shape outside the census
    /// vocabulary). Subplan- and param-free by construction.
    pub fn scan_proj_cols(&self) -> Option<ScanProjCols> {
        self.scan_proj_cols
    }

    /// The projection as bare scan Vars plus ONE strict-fmgr-chain computed
    /// column (the ready-time expr-key census), or None (shape outside the
    /// vocabulary). Subplan- and param-free by construction.
    pub fn scan_proj_expr_key(&self) -> Option<ScanProjExprKey> {
        self.scan_proj_expr_key
    }

    #[inline]
    pub fn has_old(&self) -> bool {
        self.flags & EEO_FLAG_HAS_OLD != 0
    }

    #[inline]
    pub fn has_new(&self) -> bool {
        self.flags & EEO_FLAG_HAS_NEW != 0
    }

    /// C ExecProcessReturning's per-row EEO_FLAG_OLD_IS_NULL/NEW_IS_NULL
    /// toggling: tells RETURNINGEXPR/OLD_*/NEW_* steps whether the rows exist.
    #[inline]
    pub fn set_old_new_null(&mut self, old_is_null: bool, new_is_null: bool) {
        self.flags &= !(EEO_FLAG_OLD_IS_NULL | EEO_FLAG_NEW_IS_NULL);
        if old_is_null {
            self.flags |= EEO_FLAG_OLD_IS_NULL;
        }
        if new_is_null {
            self.flags |= EEO_FLAG_NEW_IS_NULL;
        }
    }

    /// C ExecMergeMatched/NotMatched's mtstate->mt_merge_action write: arms
    /// the active merge action read by MERGE_SUPPORT_FUNC steps.
    #[inline]
    pub fn set_merge_action(&mut self, action: Option<::types_nodes::nodes_enums::CmdType>) {
        if let Some(cell) = self.merge_action_cell {
            // SAFETY: compile-allocated cell owned by this state, live for
            // the state's mcx; exclusive access via &mut self.
            unsafe { cell.write(action) };
        }
    }

    #[inline]
    pub fn has_subplan(&self) -> bool {
        self.flags & EEO_FLAG_HAS_SUBPLAN != 0
    }

    // Result-mcx convention: every frame's fcinfo is armed with the context
    // that owns by-ref call results (C's CurrentMemoryContext at eval).
    pub fn arm_result_mcx(&mut self, mcx: Mcx<'mcx>) {
        for f in self.frames.iter() {
            // SAFETY: the frame's fcinfo image is live for 'mcx and this is
            // the sole reference; 'mcx also bounds the armed context, so it
            // outlives every call through the frame.
            unsafe { fcinfo_mut(f.fcinfo, f.nargs).set_result_mcx(mcx) };
        }
        for slot in self.alloc_mcx_slots.iter() {
            // SAFETY: slot points at a compile-allocated state's resmcx field,
            // live for 'mcx; the armed context outlives evaluation.
            unsafe { slot.write(Some(NonNull::from(mcx.context()))) };
        }
    }

    /// Lifetime-erased [`Self::arm_result_mcx`] (nodeAgg tmpcontext).
    /// # Safety: `mcx`'s context outlives every evaluation of this program,
    /// AND its MemoryContext struct is address-stable for that whole span:
    /// the armed pointer is raw. A per-tuple context satisfied this only
    /// because ExprContextData arena-boxes it (P1 panic-arena-corruption:
    /// es_exprcontexts growth relocated the struct and the armed program kept
    /// allocating through the abandoned copy).
    pub unsafe fn arm_result_mcx_raw(&mut self, mcx: Mcx<'_>) {
        for f in self.frames.iter() {
            // SAFETY: frame image live for 'mcx, sole reference; the caller
            // guarantees the armed context outlives every call.
            unsafe { fcinfo_mut(f.fcinfo, f.nargs).set_result_mcx(mcx) };
        }
        for slot in self.alloc_mcx_slots.iter() {
            // SAFETY: slot points at a compile-allocated state's resmcx field;
            // the caller guarantees the armed context outlives evaluation.
            unsafe { slot.write(Some(NonNull::from(mcx.context()))) };
        }
    }

    /// Writes the externally-supplied CaseTestExpr value (C econtext
    /// caseValue_datum/caseValue_isNull) read by EEOP_CASE_TESTVAL steps
    /// compiled through [`crate::exec_init_expr_with_case_test`].
    pub fn set_case_test(&mut self, nd: NullableDatum) {
        debug_assert!(
            self.ext_case_test.is_some(),
            "set_case_test on a program with no external CaseTestExpr"
        );
        if let Some(cell) = self.ext_case_test {
            // SAFETY: compile-allocated 'mcx cell, sole writer here.
            unsafe { cell.write(nd) };
        }
    }

    /// Drops each frame's fn_extra; the program is then safe to forget.
    pub fn release_frames(&mut self) {
        for f in self.frames.iter_mut() {
            // SAFETY: frame-owned mcx-boxed FmgrInfo, sole reference here.
            unsafe { f.flinfo.as_mut() }.fn_extra = None;
        }
    }

    #[cfg(any(test, feature = "bench-internals"))]
    pub fn force_program_kernel(&mut self) {
        if !matches!(self.kernel, Kernel::Program) {
            self.kernel = Kernel::Program;
            crate::compile::fuse_program(self);
        }
    }
}
