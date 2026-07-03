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

// C's `Datum *resv, bool *resnull` pair: None = the interpreter's result
// registers, Some = one NullableDatum arg slot inside a frame's fcinfo image.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OutRef(pub(crate) Option<NonNull<NullableDatum>>);

impl OutRef {
    pub const RESULT: OutRef = OutRef(None);

    #[inline(always)]
    pub(crate) fn is_result(self) -> bool {
        self.0.is_none()
    }
}

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
    Qual { jumpdone: u32 },
    Jump { jumpdone: u32 },
    JumpIfNotTrue { jumpdone: u32, out: OutRef },
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
    // Agg pointers resolve at build into once-allocated never-moved AggState arrays.
    AggrefEval { value: NonNull<Datum>, null: NonNull<bool>, out: OutRef },
    // C EEOP_AGG_STRICT_INPUT_CHECK_ARGS(_1): args = fcinfo args[1..].
    AggStrictInputCheck { args: NonNull<NullableDatum>, nargs: u16, jumpnull: u32 },
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
    // slots: nelems compile-allocated NullableDatum arg targets (C's
    // d.minmax.values/nulls); call is the type's btree cmp proc.
    MinMax { call: FuncCall, slots: NonNull<NullableDatum>, nelems: u16, least: bool, out: OutRef },
    // timetz: compile-allocated 12-byte TimeTz image, rewritten per eval —
    // valid until the next eval, the window C's per-tuple context reset gives.
    SqlValueFunction {
        op: ::types_nodes::primnodes::SQLValueFunctionOp,
        typmod: i32,
        timetz: NonNull<u8>,
        out: OutRef,
    },
}

// By-ref copy target: C d.agg_trans.aggcontext + the transtype's typlen.
#[derive(Clone, Copy, Debug)]
pub struct AggByRef {
    pub agg: NonNull<AggStateNode>,
    pub translen: i16,
}

// C nodeAgg.h AggStatePerGroupData; the trans steps read/write it in place.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct AggPerGroup {
    pub trans_value: Datum,
    pub trans_value_is_null: bool,
    pub no_trans_value: bool,
}

const _: () = assert!(core::mem::size_of::<Step>() <= 64);

// C ExprEvalStep.d.func minus the FmgrInfo pointer: fn_addr/fcinfo are the
// resolve-once extra copies C keeps "to save an indirection at runtime";
// `frame` reaches the owning FuncFrame (flinfo) in ExprState.
#[derive(Clone, Copy, Debug)]
pub struct FuncCall {
    pub fn_addr: PGFunction,
    pub(crate) fcinfo: NonNull<u8>,
    pub frame: u32,
    pub nargs: u16,
}

// Step-owned call state: the FmgrInfo carrier plus its heap fcinfo image
// (header + nargs NullableDatum tail) bump-allocated in 'mcx.
pub struct FuncFrame<'mcx> {
    pub flinfo: FmgrInfo,
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
    JustFunc { fn_addr: PGFunction, frame: u32, nargs: u16, strict: bool },
}

const _: () = assert!(core::mem::size_of::<Kernel>() <= 24);

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
    pub(crate) kernel: Kernel,
    pub(crate) flags: u8,
    // C ExprState.innermost_caseval/casenull: compile-time only.
    pub(crate) innermost_case: Option<NonNull<NullableDatum>>,
    // PARAM_EXEC ids this expression reads; the owning node resolves pending
    // initplans against these before evaluation (nodeSubplan.c lane).
    pub(crate) param_exec_deps: PgVec<'mcx, u32>,
}

impl<'mcx> ExprState<'mcx> {
    // C makeNode(ExprState) + ExprEvalPushStep's 16-step first allocation: box written in place.
    #[inline]
    pub(crate) fn new_boxed_in(mcx: Mcx<'mcx>) -> PgResult<::mcx::PgBox<'mcx, ExprState<'mcx>>> {
        let layout = Layout::new::<ExprState<'mcx>>();
        let raw = mcx.allocate(layout).map_err(|_| mcx.oom(layout.size()))?;
        let p = raw.cast::<ExprState<'mcx>>();
        // On steps-alloc failure the header chunk stays until reset (C's palloc-then-throw shape).
        let steps = ::mcx::vec_with_capacity_in(mcx, 16)?;
        // SAFETY: fresh exclusive layout-sized allocation from `mcx`; written once, then box-owned.
        unsafe {
            p.write(ExprState {
                steps,
                frames: PgVec::new_in(mcx),
                kernel: Kernel::Program,
                flags: 0,
                innermost_case: None,
                param_exec_deps: PgVec::new_in(mcx),
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
    }

    /// Lifetime-erased [`Self::arm_result_mcx`] (nodeAgg tmpcontext).
    /// # Safety: `mcx`'s context outlives every evaluation of this program.
    pub unsafe fn arm_result_mcx_raw(&mut self, mcx: Mcx<'_>) {
        for f in self.frames.iter() {
            // SAFETY: frame image live for 'mcx, sole reference; the caller
            // guarantees the armed context outlives every call.
            unsafe { fcinfo_mut(f.fcinfo, f.nargs).set_result_mcx(mcx) };
        }
    }

    #[cfg(any(test, feature = "bench-internals"))]
    pub fn force_program_kernel(&mut self) {
        self.kernel = Kernel::Program;
    }
}
