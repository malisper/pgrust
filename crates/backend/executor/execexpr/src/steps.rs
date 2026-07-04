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
pub const EEO_FLAG_INTERPRETER_INITIALIZED: u8 = 1 << 5;
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
    ParamExec { prm: NonNull<::types_portal::params::ParamExecData>, out: OutRef },
    FuncExpr { call: FuncCall, out: OutRef },
    FuncExprStrict1 { call: FuncCall, out: OutRef },
    FuncExprStrict2 { call: FuncCall, out: OutRef },
    FuncExprStrict { call: FuncCall, out: OutRef },
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
        nelems: u16,
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
        nelems: u16,
        frame: u32,
        desc: NonNull<::types_tuple::TupleDescData<'static>>,
        out: OutRef,
    },
    // C EEOP_AGG_STRICT_INPUT_CHECK_ARGS(_1): args = fcinfo args[1..].
    AggStrictInputCheck { args: NonNull<NullableDatum>, nargs: u16, jumpnull: u32 },
    // Ordered/DISTINCT agg row survived filter+strict checks: flag it for
    // nodeagg's tuplesort feed (scratch already holds the evaluated args).
    AggOrderedMark { flag: NonNull<bool> },
    AggStrictInputCheck1 { arg: NonNull<NullableDatum>, jumpnull: u32 },
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
    DomainNotNull { resulttype: Oid, out: OutRef },
    // name/check: compile-allocated in 'mcx (BoolAndStep anynull precedent).
    DomainCheck { resulttype: Oid, name: NonNull<str>, check: NonNull<NullableDatum> },
    JumpIfNull { jumpdone: u32, out: OutRef },
    ArrayExprEval { state: NonNull<crate::arrayops::ArrayExprState>, out: OutRef },
    XmlExprEval { state: NonNull<crate::xmlops::XmlExprState>, out: OutRef },
    SbsrefSubscripts { state: NonNull<crate::arrayops::SbsRefState>, jumpdone: u32, out: OutRef },
    SbsrefFetch { state: NonNull<crate::arrayops::SbsRefState>, slice: bool, out: OutRef },
    SbsrefOld { state: NonNull<crate::arrayops::SbsRefState>, out: OutRef },
    SbsrefAssign { state: NonNull<crate::arrayops::SbsRefState>, out: OutRef },
    // slots: nelems compile-allocated NullableDatum arg targets (C's
    // d.minmax.values/nulls); call is the type's btree cmp proc.
    MinMax { call: FuncCall, slots: NonNull<NullableDatum>, nelems: u16, least: bool, out: OutRef },
    NextValueExpr { seqid: Oid, seqtypid: Oid, out: OutRef },
    // scratch: compile-allocated by-ref result image (12-byte TimeTz or
    // 64-byte NameData), rewritten per eval — valid until the next eval, the
    // window C's per-tuple context reset gives.
    SqlValueFunction {
        op: ::types_nodes::primnodes::SQLValueFunctionOp,
        typmod: i32,
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

// C ExprEvalStep d.wholerow minus var/junkFilter: first-eval compat state.
pub struct WholeRowState {
    pub tupdesc: NonNull<::types_tuple::TupleDescData<'static>>,
    pub first: bool,
    pub slow: bool,
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
}

impl CmpOp {
    pub fn for_fn_oid(oid: Oid) -> Option<CmpOp> {
        Some(match oid {
            65 => CmpOp::Int4Eq,
            144 => CmpOp::Int4Ne,
            66 => CmpOp::Int4Lt,
            149 => CmpOp::Int4Le,
            147 => CmpOp::Int4Gt,
            150 => CmpOp::Int4Ge,
            467 => CmpOp::Int8Eq,
            468 => CmpOp::Int8Ne,
            469 => CmpOp::Int8Lt,
            471 => CmpOp::Int8Le,
            470 => CmpOp::Int8Gt,
            472 => CmpOp::Int8Ge,
            63 => CmpOp::Int2Eq,
            145 => CmpOp::Int2Ne,
            64 => CmpOp::Int2Lt,
            148 => CmpOp::Int2Le,
            146 => CmpOp::Int2Gt,
            151 => CmpOp::Int2Ge,
            474 => CmpOp::Int84Eq,
            475 => CmpOp::Int84Ne,
            476 => CmpOp::Int84Lt,
            478 => CmpOp::Int84Le,
            477 => CmpOp::Int84Gt,
            479 => CmpOp::Int84Ge,
            852 => CmpOp::Int48Eq,
            853 => CmpOp::Int48Ne,
            854 => CmpOp::Int48Lt,
            856 => CmpOp::Int48Le,
            855 => CmpOp::Int48Gt,
            857 => CmpOp::Int48Ge,
            _ => return None,
        })
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
}

pub struct ExprState<'mcx> {
    pub(crate) steps: PgVec<'mcx, Step>,
    pub(crate) frames: PgVec<'mcx, FuncFrame<'mcx>>,
    pub(crate) saop_tables: PgVec<'mcx, SaopTable<'mcx>>,
    pub(crate) kernel: Kernel,
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
                flags: 0,
                resnd,
                innermost_case: None,
                param_exec_deps: PgVec::new_in(mcx),
                innermost_domain: None,
                alloc_mcx_slots: PgVec::new_in(mcx),
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
    pub(crate) fn is_result(&self, out: OutRef) -> bool {
        out.0 == self.resnd
    }

    pub fn is_qual(&self) -> bool {
        self.flags & EEO_FLAG_IS_QUAL != 0
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
    /// # Safety: `mcx`'s context outlives every evaluation of this program.
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
