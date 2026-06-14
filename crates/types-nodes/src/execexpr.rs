//! Expression-evaluation vocabulary (`executor/execExpr.h` and the
//! `ExprState` portion of `nodes/execnodes.h`).
//!
//! This is the keystone type layer for the `backend-executor-execExpr` /
//! `backend-executor-execExprInterp` cycle: the compiled, executable form of an
//! expression tree (`ExecInitExpr`/`ExecInitQual` output) and the linear
//! `ExprEvalStep` program the interpreter walks.
//!
//! Layout is mirrored field-for-field against PostgreSQL 18
//! (`src/include/executor/execExpr.h`). The C `ExprEvalStep` is a discriminant
//! (`opcode`) plus a `union d`; in the owned model the discriminant + payload
//! become a single Rust tagged enum [`ExprEvalStepData`], with one variant per
//! C union member, while the `ExprEvalStep` struct keeps the C result-pointer
//! members. Every `EEOP_*` opcode in C has a corresponding [`ExprEvalOp`]
//! enumerator, in the same order (kept in sync with the interpreter dispatch
//! table in `execExprInterp.c`).

use mcx::{MemoryContext, PgBox, PgString, PgVec};
use types_core::fmgr::FmgrInfo;
use types_core::primitive::{AttrNumber, Oid};
use types_datum::datum::NullableDatum;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::HeapTuple;
use types_tuple::heaptuple::TupleDescData;

use crate::execnodes::{EcxtId, Opaque};
use types_slot::{TupleSlotKind, TupleTableSlot};
use crate::fmgr::FunctionCallInfoBaseData;
use crate::nodes::NodeTag;
use crate::planstate::PlanStateNode;
use crate::primnodes::{Expr, ScalarArrayOpExpr, SubPlan, Var};

/// `EEO_FLAG_IS_QUAL` (execnodes.h) — this expression is a qualification.
pub const EEO_FLAG_IS_QUAL: u8 = 1 << 0;
/// `EEO_FLAG_HAS_OLD` (execnodes.h) — the expression references OLD columns.
pub const EEO_FLAG_HAS_OLD: u8 = 1 << 1;
/// `EEO_FLAG_HAS_NEW` (execnodes.h) — the expression references NEW columns.
pub const EEO_FLAG_HAS_NEW: u8 = 1 << 2;
/// `EEO_FLAG_OLD_IS_NULL` (execnodes.h) — the OLD row is not present (NULL).
pub const EEO_FLAG_OLD_IS_NULL: u8 = 1 << 3;
/// `EEO_FLAG_NEW_IS_NULL` (execnodes.h) — the NEW row is not present (NULL).
pub const EEO_FLAG_NEW_IS_NULL: u8 = 1 << 4;
/// `EEO_FLAG_INTERPRETER_INITIALIZED` (execExpr.h) — expression's interpreter
/// has been initialized.
pub const EEO_FLAG_INTERPRETER_INITIALIZED: u8 = 1 << 5;
/// `EEO_FLAG_DIRECT_THREADED` (execExpr.h) — jump-threading is in use.
pub const EEO_FLAG_DIRECT_THREADED: u8 = 1 << 6;

/// `ExprEvalOp` (execExpr.h) — discriminator for `ExprEvalStep`s. Identifies
/// the operation to execute and which [`ExprEvalStepData`] variant is valid.
///
/// The order of entries must be kept in sync with the `dispatch_table[]` array
/// in `execExprInterp.c:ExecInterpExpr()`; the variants below are declared in
/// exactly the C enumerator order, so `#[repr(u32)]` discriminants match.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum ExprEvalOp {
    /// entire expression has been evaluated, return value
    EEOP_DONE_RETURN,
    /// entire expression has been evaluated, no return value
    EEOP_DONE_NO_RETURN,

    /// apply slot_getsomeattrs on corresponding tuple slot
    EEOP_INNER_FETCHSOME,
    EEOP_OUTER_FETCHSOME,
    EEOP_SCAN_FETCHSOME,
    EEOP_OLD_FETCHSOME,
    EEOP_NEW_FETCHSOME,

    /// compute non-system Var value
    EEOP_INNER_VAR,
    EEOP_OUTER_VAR,
    EEOP_SCAN_VAR,
    EEOP_OLD_VAR,
    EEOP_NEW_VAR,

    /// compute system Var value
    EEOP_INNER_SYSVAR,
    EEOP_OUTER_SYSVAR,
    EEOP_SCAN_SYSVAR,
    EEOP_OLD_SYSVAR,
    EEOP_NEW_SYSVAR,

    /// compute wholerow Var
    EEOP_WHOLEROW,

    /// compute non-system Var value, assign it into ExprState's resultslot
    EEOP_ASSIGN_INNER_VAR,
    EEOP_ASSIGN_OUTER_VAR,
    EEOP_ASSIGN_SCAN_VAR,
    EEOP_ASSIGN_OLD_VAR,
    EEOP_ASSIGN_NEW_VAR,

    /// assign ExprState's resvalue/resnull to a column of its resultslot
    EEOP_ASSIGN_TMP,
    /// ditto, applying MakeExpandedObjectReadOnly()
    EEOP_ASSIGN_TMP_MAKE_RO,

    /// evaluate Const value
    EEOP_CONST,

    /// evaluate function call (including OpExprs etc)
    EEOP_FUNCEXPR,
    EEOP_FUNCEXPR_STRICT,
    EEOP_FUNCEXPR_STRICT_1,
    EEOP_FUNCEXPR_STRICT_2,
    EEOP_FUNCEXPR_FUSAGE,
    EEOP_FUNCEXPR_STRICT_FUSAGE,

    /// evaluate boolean AND expression, one step per subexpression
    EEOP_BOOL_AND_STEP_FIRST,
    EEOP_BOOL_AND_STEP,
    EEOP_BOOL_AND_STEP_LAST,

    /// similarly for boolean OR expression
    EEOP_BOOL_OR_STEP_FIRST,
    EEOP_BOOL_OR_STEP,
    EEOP_BOOL_OR_STEP_LAST,

    /// evaluate boolean NOT expression
    EEOP_BOOL_NOT_STEP,

    /// simplified version of BOOL_AND_STEP for use by ExecQual()
    EEOP_QUAL,

    /// unconditional jump to another step
    EEOP_JUMP,

    /// conditional jumps based on current result value
    EEOP_JUMP_IF_NULL,
    EEOP_JUMP_IF_NOT_NULL,
    EEOP_JUMP_IF_NOT_TRUE,

    /// perform NULL tests for scalar values
    EEOP_NULLTEST_ISNULL,
    EEOP_NULLTEST_ISNOTNULL,

    /// perform NULL tests for row values
    EEOP_NULLTEST_ROWISNULL,
    EEOP_NULLTEST_ROWISNOTNULL,

    /// evaluate a BooleanTest expression
    EEOP_BOOLTEST_IS_TRUE,
    EEOP_BOOLTEST_IS_NOT_TRUE,
    EEOP_BOOLTEST_IS_FALSE,
    EEOP_BOOLTEST_IS_NOT_FALSE,

    /// evaluate PARAM_EXEC/EXTERN parameters
    EEOP_PARAM_EXEC,
    EEOP_PARAM_EXTERN,
    EEOP_PARAM_CALLBACK,
    /// set PARAM_EXEC value
    EEOP_PARAM_SET,

    /// return CaseTestExpr value
    EEOP_CASE_TESTVAL,
    EEOP_CASE_TESTVAL_EXT,

    /// apply MakeExpandedObjectReadOnly() to target value
    EEOP_MAKE_READONLY,

    /// evaluate assorted special-purpose expression types
    EEOP_IOCOERCE,
    EEOP_IOCOERCE_SAFE,
    EEOP_DISTINCT,
    EEOP_NOT_DISTINCT,
    EEOP_NULLIF,
    EEOP_SQLVALUEFUNCTION,
    EEOP_CURRENTOFEXPR,
    EEOP_NEXTVALUEEXPR,
    EEOP_RETURNINGEXPR,
    EEOP_ARRAYEXPR,
    EEOP_ARRAYCOERCE,
    EEOP_ROW,

    /// compare two individual elements of each of two compared ROW()s
    EEOP_ROWCOMPARE_STEP,

    /// evaluate boolean value based on previous ROWCOMPARE_STEP operations
    EEOP_ROWCOMPARE_FINAL,

    /// evaluate GREATEST() or LEAST()
    EEOP_MINMAX,

    /// evaluate FieldSelect expression
    EEOP_FIELDSELECT,

    /// deform tuple before evaluating a FieldStore expression
    EEOP_FIELDSTORE_DEFORM,

    /// form the new tuple for a FieldStore expression
    EEOP_FIELDSTORE_FORM,

    /// process container subscripts; possibly short-circuit result to NULL
    EEOP_SBSREF_SUBSCRIPTS,

    /// compute old container element/slice for a SubscriptingRef assignment
    EEOP_SBSREF_OLD,

    /// compute new value for SubscriptingRef assignment expression
    EEOP_SBSREF_ASSIGN,

    /// compute element/slice for SubscriptingRef fetch expression
    EEOP_SBSREF_FETCH,

    /// evaluate value for CoerceToDomainValue
    EEOP_DOMAIN_TESTVAL,
    EEOP_DOMAIN_TESTVAL_EXT,

    /// evaluate a domain's NOT NULL constraint
    EEOP_DOMAIN_NOTNULL,

    /// evaluate a single domain CHECK constraint
    EEOP_DOMAIN_CHECK,

    /// evaluation steps for hashing
    EEOP_HASHDATUM_SET_INITVAL,
    EEOP_HASHDATUM_FIRST,
    EEOP_HASHDATUM_FIRST_STRICT,
    EEOP_HASHDATUM_NEXT32,
    EEOP_HASHDATUM_NEXT32_STRICT,

    /// evaluate assorted special-purpose expression types
    EEOP_CONVERT_ROWTYPE,
    EEOP_SCALARARRAYOP,
    EEOP_HASHED_SCALARARRAYOP,
    EEOP_XMLEXPR,
    EEOP_JSON_CONSTRUCTOR,
    EEOP_IS_JSON,
    EEOP_JSONEXPR_PATH,
    EEOP_JSONEXPR_COERCION,
    EEOP_JSONEXPR_COERCION_FINISH,
    EEOP_AGGREF,
    EEOP_GROUPING_FUNC,
    EEOP_WINDOW_FUNC,
    EEOP_MERGE_SUPPORT_FUNC,
    EEOP_SUBPLAN,

    /// aggregation related nodes
    EEOP_AGG_STRICT_DESERIALIZE,
    EEOP_AGG_DESERIALIZE,
    EEOP_AGG_STRICT_INPUT_CHECK_ARGS,
    EEOP_AGG_STRICT_INPUT_CHECK_ARGS_1,
    EEOP_AGG_STRICT_INPUT_CHECK_NULLS,
    EEOP_AGG_PLAIN_PERGROUP_NULLCHECK,
    EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL,
    EEOP_AGG_PLAIN_TRANS_STRICT_BYVAL,
    EEOP_AGG_PLAIN_TRANS_BYVAL,
    EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYREF,
    EEOP_AGG_PLAIN_TRANS_STRICT_BYREF,
    EEOP_AGG_PLAIN_TRANS_BYREF,
    EEOP_AGG_PRESORTED_DISTINCT_SINGLE,
    EEOP_AGG_PRESORTED_DISTINCT_MULTI,
    EEOP_AGG_ORDERED_TRANS_DATUM,
    EEOP_AGG_ORDERED_TRANS_TUPLE,

    /// non-existent operation, used e.g. to check array lengths
    EEOP_LAST,
}

/// Total number of `ExprEvalOp` discriminants — equals C `EEOP_LAST + 1`
/// (the count of dispatch-table entries). Asserted in tests against the header.
pub const NUM_EXPR_EVAL_OPS: usize = ExprEvalOp::EEOP_LAST as usize + 1;

/// `ExprEvalRowtypeCache` (execExpr.h) — ExprEvalSteps that cache a composite
/// type's tupdesc need one of these.
///
/// `cacheptr` points to the composite type's `TypeCacheEntry` if `tupdesc_id`
/// is not 0; or for an anonymous RECORD type, it points directly at the cached
/// tupdesc and `tupdesc_id` is 0. Initial state is `cacheptr == NULL`.
/// (`cacheptr` is a `void *` in C; the typecache layer is unported, so it is
/// carried as an opaque address until that owner lands.)
#[derive(Clone, Copy, Debug, Default)]
pub struct ExprEvalRowtypeCache {
    /// `void *cacheptr`.
    pub cacheptr: usize,
    /// `uint64 tupdesc_id` — last-seen tupdesc identifier, or 0.
    pub tupdesc_id: u64,
}

/// `VarReturningType` (nodes/primnodes.h) — return old/new/default value of a
/// `Var` in RETURNING/MERGE. Mirrored here (it is forward-referenced by the
/// `var` step payload) until primnodes carries it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum VarReturningType {
    VAR_RETURNING_DEFAULT,
    VAR_RETURNING_OLD,
    VAR_RETURNING_NEW,
}

/// `CompareType` (nodes/primnodes.h) — abstract comparison result requested of
/// a `RowCompare`. Mirrored locally pending primnodes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum CompareType {
    COMPARE_INVALID,
    COMPARE_LT,
    COMPARE_LE,
    COMPARE_EQ,
    COMPARE_GE,
    COMPARE_GT,
    COMPARE_NE,
}

/// `MinMaxOp` (nodes/primnodes.h) — GREATEST vs LEAST. Mirrored locally.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum MinMaxOp {
    IS_GREATEST,
    IS_LEAST,
}

/// `PGFunction` (fmgr.h) — the C-level fmgr function pointer
/// `Datum (*)(FunctionCallInfo)`. Mirrored here as the stored shape; the fmgr
/// owner installs concrete addresses.
pub type PGFunction = for<'mcx> fn(&mut FunctionCallInfoBaseData<'mcx>) -> Datum<'mcx>;

/// `ExprStateEvalFunc` (execnodes.h) — `Datum (*)(ExprState *, ExprContext *,
/// bool *isNull)`: the function that actually evaluates a compiled expression
/// (set to different bodies depending on expression complexity). The
/// `ExprContext` is identified by its [`EcxtId`] pool index in the owned model.
pub type ExprStateEvalFunc = for<'mcx> fn(&mut ExprState<'mcx>, EcxtId, &mut bool) -> Datum<'mcx>;

/// `ExecEvalSubroutine` (execExpr.h) — typical out-of-line evaluation
/// subroutine: `void (*)(ExprState *, struct ExprEvalStep *, ExprContext *)`.
/// The interpreter owner supplies concrete implementations; here it is the
/// stored function-pointer shape.
pub type ExecEvalSubroutine = for<'mcx> fn(&mut ExprState<'mcx>, &mut ExprEvalStep<'mcx>, EcxtId);

/// `ExecEvalBoolSubroutine` (execExpr.h) — like [`ExecEvalSubroutine`] but
/// returning `bool`.
pub type ExecEvalBoolSubroutine =
    for<'mcx> fn(&mut ExprState<'mcx>, &mut ExprEvalStep<'mcx>, EcxtId) -> bool;

/// `SubscriptingRefState` (execExpr.h) — non-inline data for container
/// (`SubscriptingRef`) operations. Pointed at by the `sbsref*` steps.
#[derive(Debug)]
pub struct SubscriptingRefState<'mcx> {
    /// `bool isassignment` — is it assignment, or just fetch?
    pub isassignment: bool,
    /// `void *workspace` — type-specific subscripting workspace (opaque to this
    /// layer; carried as an address until the subscript handler owner lands).
    pub workspace: usize,
    /// `int numupper`.
    pub numupper: i32,
    /// `bool *upperprovided` — indicates if this position is supplied.
    pub upperprovided: Option<PgVec<'mcx, bool>>,
    /// `Datum *upperindex`.
    pub upperindex: Option<PgVec<'mcx, Datum<'mcx>>>,
    /// `bool *upperindexnull`.
    pub upperindexnull: Option<PgVec<'mcx, bool>>,
    /// `int numlower`.
    pub numlower: i32,
    /// `bool *lowerprovided`.
    pub lowerprovided: Option<PgVec<'mcx, bool>>,
    /// `Datum *lowerindex`.
    pub lowerindex: Option<PgVec<'mcx, Datum<'mcx>>>,
    /// `bool *lowerindexnull`.
    pub lowerindexnull: Option<PgVec<'mcx, bool>>,
    /// `Datum replacevalue` — for assignment, new value to assign.
    pub replacevalue: Datum<'mcx>,
    /// `bool replacenull`.
    pub replacenull: bool,
    /// `Datum prevvalue` — nested-assignment old value sink.
    pub prevvalue: Datum<'mcx>,
    /// `bool prevnull`.
    pub prevnull: bool,
}

impl Default for SubscriptingRefState<'_> {
    fn default() -> Self {
        // C `palloc0` zero-init of the sbsref workspace.
        SubscriptingRefState {
            isassignment: false,
            workspace: 0,
            numupper: 0,
            upperprovided: None,
            upperindex: None,
            upperindexnull: None,
            numlower: 0,
            lowerprovided: None,
            lowerindex: None,
            lowerindexnull: None,
            replacevalue: Datum::null(),
            replacenull: false,
            prevvalue: Datum::null(),
            prevnull: false,
        }
    }
}

/// `JsonConstructorExprState` (execExpr.h) — EEOP_JSON_CONSTRUCTOR state, too
/// big to inline. The `constructor` back-pointer (`JsonConstructorExpr *`) and
/// the JSON support live in unported units, so this carries the inline
/// workspace arrays plus the per-arg type cache; the node back-pointer arrives
/// with the JSON family.
#[derive(Debug, Default)]
pub struct JsonConstructorExprState<'mcx> {
    /// `Datum *arg_values`.
    pub arg_values: Option<PgVec<'mcx, Datum<'mcx>>>,
    /// `bool *arg_nulls`.
    pub arg_nulls: Option<PgVec<'mcx, bool>>,
    /// `Oid *arg_types`.
    pub arg_types: Option<PgVec<'mcx, Oid>>,
    /// `struct { int category; Oid outfuncid; } *arg_type_cache`.
    pub arg_type_cache: Option<PgVec<'mcx, JsonArgTypeCache>>,
    /// `int nargs`.
    pub nargs: i32,
}

/// Anonymous per-arg cache struct inside `JsonConstructorExprState`
/// (`cache for datum_to_json[b]()`).
#[derive(Clone, Copy, Debug, Default)]
pub struct JsonArgTypeCache {
    /// `int category`.
    pub category: i32,
    /// `Oid outfuncid`.
    pub outfuncid: Oid,
}

/// Inline payload of an [`ExprEvalStep`] — the C `union d`, modeled as a tagged
/// enum. Each variant mirrors one C union member (the member name is the
/// variant name); the active variant is selected by the step's [`ExprEvalOp`]
/// opcode.
///
/// Where a C member is a typed pointer to a node/state struct owned by another,
/// not-yet-ported unit (e.g. `FieldStore *`, `XmlExpr *`, `JsonExprState *`,
/// `AggStatePerTrans`, `WindowFuncExprState *`), the field is modeled with the
/// field set this layer can express today and the unported node back-pointer is
/// parked (as an opaque address) until its owner lands; the variant exists for
/// every union member so the discriminant space is complete.
#[derive(Debug)]
pub enum ExprEvalStepData<'mcx> {
    /// No union payload — for opcodes whose `union d` is unused (e.g.
    /// `EEOP_DONE_RETURN` / `EEOP_DONE_NO_RETURN` / `EEOP_CURRENTOFEXPR` /
    /// `EEOP_NULLTEST_*` scalar / `EEOP_BOOL_AND_STEP_FIRST` companions that
    /// carry their data in another field). The C union is simply left
    /// zero-initialized in those steps.
    NoPayload,
    /// `fetch` — for EEOP_INNER/OUTER/SCAN/OLD/NEW_FETCHSOME.
    Fetch {
        /// attribute number up to which to fetch (inclusive)
        last_var: i32,
        /// will the type of slot be the same for every invocation
        fixed: bool,
        /// tuple descriptor, if known
        known_desc: Option<PgBox<'mcx, TupleDescData<'mcx>>>,
        /// type of slot, only reliable if `fixed`
        kind: Option<TupleSlotKind>,
    },
    /// `var` — for EEOP_INNER/OUTER/SCAN/OLD/NEW_[SYS]VAR.
    Var {
        /// attnum is attr number - 1 for regular VAR, or the (negative) attr
        /// number for SYSVAR
        attnum: i32,
        /// type OID of variable
        vartype: Oid,
        varreturningtype: VarReturningType,
    },
    /// `wholerow` — for EEOP_WHOLEROW.
    WholeRow {
        /// original `Var` node in plan tree
        var: Option<PgBox<'mcx, Var>>,
        /// first time through, need to initialize?
        first: bool,
        /// need runtime check for nulls?
        slow: bool,
        /// descriptor for resulting tuples
        tupdesc: Option<PgBox<'mcx, TupleDescData<'mcx>>>,
        /// `JunkFilter *junkFilter` — parked (unported owner) until execJunk
        /// lands; carried as an address.
        junk_filter: usize,
    },
    /// `assign_var` — for EEOP_ASSIGN_*_VAR.
    AssignVar {
        /// target index in resultslot->tts_values/nulls
        resultnum: i32,
        /// source attribute number - 1
        attnum: i32,
    },
    /// `assign_tmp` — for EEOP_ASSIGN_TMP[_MAKE_RO].
    AssignTmp {
        /// target index in resultslot->tts_values/nulls
        resultnum: i32,
    },
    /// `returningexpr` — for EEOP_RETURNINGEXPR.
    ReturningExpr {
        /// flag to test if OLD/NEW row is NULL
        nullflag: u8,
        /// jump here if OLD/NEW row is NULL
        jumpdone: i32,
    },
    /// `constval` — for EEOP_CONST.
    ConstVal { value: Datum<'mcx>, isnull: bool },
    /// `func` — for EEOP_FUNCEXPR_* / NULLIF / DISTINCT.
    Func {
        /// `FmgrInfo *finfo` — function's lookup data.
        finfo: Option<PgBox<'mcx, FmgrInfo>>,
        /// `FunctionCallInfo fcinfo_data` — arguments etc.
        fcinfo_data: Option<PgBox<'mcx, FunctionCallInfoBaseData<'mcx>>>,
        /// Per-argument result cells: the `&fcinfo->args[i].value` /
        /// `&fcinfo->args[i].isnull` aliasing targets the argument
        /// sub-expressions evaluate into (one [`ResultCellId`] per argument).
        /// In C the recursion writes directly through `&fcinfo->args[i]`; in the
        /// owned model the interpreter gathers these arena cells into the fcinfo
        /// args immediately before the call. Empty for a 0-arg function.
        arg_cells: Option<PgVec<'mcx, ResultCellId>>,
        /// `PGFunction fn_addr` — actual call address.
        fn_addr: Option<PGFunction>,
        /// number of arguments
        nargs: i32,
        /// make arg0 R/O (used only for NULLIF)
        make_ro: bool,
    },
    /// `boolexpr` — for EEOP_BOOL_*_STEP.
    BoolExpr {
        /// `bool *anynull` — track if any input was NULL. In the owned model an
        /// is-null cell in the [`ResultCellArena`] (the BoolExpr arm allocates a
        /// dedicated cell shared by all the AND/OR steps of one expression).
        anynull: ResultCellId,
        /// jump here if result determined
        jumpdone: i32,
    },
    /// `qualexpr` — for EEOP_QUAL.
    QualExpr {
        /// jump here on false or null
        jumpdone: i32,
    },
    /// `jump` — for EEOP_JUMP[_CONDITION].
    Jump {
        /// target instruction's index
        jumpdone: i32,
    },
    /// `nulltest_row` — for EEOP_NULLTEST_ROWIS[NOT]NULL.
    NullTestRow { rowcache: ExprEvalRowtypeCache },
    /// `param` — for EEOP_PARAM_EXEC/EXTERN and EEOP_PARAM_SET.
    Param {
        /// numeric ID for parameter
        paramid: i32,
        /// OID of parameter's datatype
        paramtype: Oid,
    },
    /// `cparam` — for EEOP_PARAM_CALLBACK.
    CParam {
        /// `ExecEvalSubroutine paramfunc` — add-on evaluation subroutine.
        paramfunc: Option<ExecEvalSubroutine>,
        /// `void *paramarg` — private data (opaque address).
        paramarg: usize,
        /// `void *paramarg2`.
        paramarg2: usize,
        /// numeric ID for parameter
        paramid: i32,
        /// OID of parameter's datatype
        paramtype: Oid,
    },
    /// `casetest` — for EEOP_CASE_TESTVAL/DOMAIN_TESTVAL. `value`/`isnull` are a
    /// [`ResultCellId`] naming the innermost CASE/domain test cell to read from
    /// (the C `Datum *value`/`bool *isnull` aliasing the caller's
    /// `caseValue_datum`/`domainValue_datum` workspace).
    CaseTest {
        /// `Datum *value` / `bool *isnull` — the test value cell.
        value: ResultCellId,
    },
    /// `make_readonly` — for EEOP_MAKE_READONLY. `value`/`isnull` are a
    /// [`ResultCellId`] naming the source cell to read.
    MakeReadOnly {
        /// `Datum *value` / `bool *isnull` — the source cell.
        value: ResultCellId,
    },
    /// `iocoerce` — for EEOP_IOCOERCE.
    IoCoerce {
        /// source type's output function lookup/call data
        finfo_out: Option<PgBox<'mcx, FmgrInfo>>,
        fcinfo_data_out: Option<PgBox<'mcx, FunctionCallInfoBaseData<'mcx>>>,
        /// result type's input function lookup/call data
        finfo_in: Option<PgBox<'mcx, FmgrInfo>>,
        fcinfo_data_in: Option<PgBox<'mcx, FunctionCallInfoBaseData<'mcx>>>,
    },
    /// `sqlvaluefunction` — for EEOP_SQLVALUEFUNCTION.
    SqlValueFunction {
        /// `SQLValueFunction *svf` — original node; parked until primnodes
        /// carries `SQLValueFunction` (opaque address for now).
        svf: usize,
    },
    /// `nextvalueexpr` — for EEOP_NEXTVALUEEXPR.
    NextValueExpr { seqid: Oid, seqtypid: Oid },
    /// `arrayexpr` — for EEOP_ARRAYEXPR.
    ArrayExpr {
        /// `Datum *elemvalues` — element values get stored here.
        elemvalues: Option<PgVec<'mcx, Datum<'mcx>>>,
        /// `bool *elemnulls`.
        elemnulls: Option<PgVec<'mcx, bool>>,
        /// length of the above arrays
        nelems: i32,
        /// array element type
        elemtype: Oid,
        /// typlen of the array element type
        elemlength: i16,
        /// is the element type pass-by-value?
        elembyval: bool,
        /// typalign of the element type
        elemalign: u8,
        /// is array expression multi-D?
        multidims: bool,
    },
    /// `arraycoerce` — for EEOP_ARRAYCOERCE.
    ArrayCoerce {
        /// `ExprState *elemexprstate` — null if no per-element work.
        elemexprstate: Option<PgBox<'mcx, ExprState<'mcx>>>,
        /// element type of result array
        resultelemtype: Oid,
        /// `struct ArrayMapState *amstate` — array_map workspace; opaque to
        /// this layer until the array unit lends it (carried as an address).
        amstate: usize,
    },
    /// `row` — for EEOP_ROW.
    Row {
        /// descriptor for result tuples
        tupdesc: Option<PgBox<'mcx, TupleDescData<'mcx>>>,
        /// `Datum *elemvalues`.
        elemvalues: Option<PgVec<'mcx, Datum<'mcx>>>,
        /// `bool *elemnulls`.
        elemnulls: Option<PgVec<'mcx, bool>>,
    },
    /// `rowcompare_step` — for EEOP_ROWCOMPARE_STEP.
    RowCompareStep {
        finfo: Option<PgBox<'mcx, FmgrInfo>>,
        fcinfo_data: Option<PgBox<'mcx, FunctionCallInfoBaseData<'mcx>>>,
        fn_addr: Option<PGFunction>,
        /// target for comparison resulting in NULL
        jumpnull: i32,
        /// target for comparison yielding inequality
        jumpdone: i32,
    },
    /// `rowcompare_final` — for EEOP_ROWCOMPARE_FINAL.
    RowCompareFinal { cmptype: CompareType },
    /// `minmax` — for EEOP_MINMAX.
    MinMax {
        /// `Datum *values` — argument workspace.
        values: Option<PgVec<'mcx, Datum<'mcx>>>,
        /// `bool *nulls`.
        nulls: Option<PgVec<'mcx, bool>>,
        nelems: i32,
        /// is it GREATEST or LEAST?
        op: MinMaxOp,
        finfo: Option<PgBox<'mcx, FmgrInfo>>,
        fcinfo_data: Option<PgBox<'mcx, FunctionCallInfoBaseData<'mcx>>>,
    },
    /// `fieldselect` — for EEOP_FIELDSELECT.
    FieldSelect {
        /// field number to extract
        fieldnum: AttrNumber,
        /// field's type
        resulttype: Oid,
        rowcache: ExprEvalRowtypeCache,
    },
    /// `fieldstore` — for EEOP_FIELDSTORE_DEFORM / FIELDSTORE_FORM.
    FieldStore {
        /// `FieldStore *fstore` — original node; parked until primnodes carries
        /// `FieldStore` (opaque address for now).
        fstore: usize,
        /// `ExprEvalRowtypeCache *rowcache` — shared by the DEFORM/FORM pair.
        rowcache: Option<PgBox<'mcx, ExprEvalRowtypeCache>>,
        /// `Datum *values` — column-value workspace.
        values: Option<PgVec<'mcx, Datum<'mcx>>>,
        /// `bool *nulls`.
        nulls: Option<PgVec<'mcx, bool>>,
        ncolumns: i32,
    },
    /// `sbsref_subscript` — for EEOP_SBSREF_SUBSCRIPTS.
    SbsRefSubscript {
        /// `ExecEvalBoolSubroutine subscriptfunc`.
        subscriptfunc: Option<ExecEvalBoolSubroutine>,
        state: Option<PgBox<'mcx, SubscriptingRefState<'mcx>>>,
        /// jump here on null
        jumpdone: i32,
    },
    /// `sbsref` — for EEOP_SBSREF_OLD / ASSIGN / FETCH.
    SbsRef {
        /// `ExecEvalSubroutine subscriptfunc`.
        subscriptfunc: Option<ExecEvalSubroutine>,
        state: Option<PgBox<'mcx, SubscriptingRefState<'mcx>>>,
    },
    /// `domaincheck` — for EEOP_DOMAIN_NOTNULL / DOMAIN_CHECK.
    DomainCheck {
        /// name of constraint
        constraintname: Option<PgString<'mcx>>,
        /// `Datum *checkvalue` / `bool *checknull` — the cell holding the CHECK
        /// expression result ([`ResultCellId`] into the arena).
        checkvalue: ResultCellId,
        /// OID of domain type
        resulttype: Oid,
        /// `ErrorSaveContext *escontext` — parked until the soft-error sink is
        /// threaded here (opaque address).
        escontext: usize,
    },
    /// `hashdatum_initvalue` — for EEOP_HASHDATUM_SET_INITVAL.
    HashDatumInitValue { init_value: Datum<'mcx> },
    /// `hashdatum` — for EEOP_HASHDATUM_(FIRST|NEXT32)[_STRICT].
    HashDatum {
        finfo: Option<PgBox<'mcx, FmgrInfo>>,
        fcinfo_data: Option<PgBox<'mcx, FunctionCallInfoBaseData<'mcx>>>,
        fn_addr: Option<PGFunction>,
        /// The hash-key result cell: the `&fcinfo->args[0].value` /
        /// `&fcinfo->args[0].isnull` aliasing target the hash-key sub-expression
        /// evaluates into (execExpr.c sets `scratch.resvalue =
        /// &fcinfo->args[0].value` on the FIRST step, and the NEXT32 builders
        /// recurse into `&fcinfo->args[0]`). In the owned model the interpreter
        /// gathers this arena cell into `fcinfo->args[0]` immediately before the
        /// hash function call. Always present (a hash step has exactly one arg).
        arg_cell: ResultCellId,
        /// jump here on null
        jumpdone: i32,
        /// `NullableDatum *iresult` — intermediate hash result.
        iresult: Option<PgBox<'mcx, NullableDatum>>,
    },
    /// `convert_rowtype` — for EEOP_CONVERT_ROWTYPE.
    ConvertRowtype {
        /// input composite type
        inputtype: Oid,
        /// output composite type
        outputtype: Oid,
        /// `ExprEvalRowtypeCache *incache`.
        incache: Option<PgBox<'mcx, ExprEvalRowtypeCache>>,
        /// `ExprEvalRowtypeCache *outcache`.
        outcache: Option<PgBox<'mcx, ExprEvalRowtypeCache>>,
        /// `TupleConversionMap *map` — column mapping; parked until tupconvert
        /// is threaded here (opaque address).
        map: usize,
    },
    /// `scalararrayop` — for EEOP_SCALARARRAYOP.
    ScalarArrayOp {
        /// InvalidOid if not yet filled
        element_type: Oid,
        /// use OR or AND semantics?
        use_or: bool,
        typlen: i16,
        typbyval: bool,
        typalign: u8,
        finfo: Option<PgBox<'mcx, FmgrInfo>>,
        fcinfo_data: Option<PgBox<'mcx, FunctionCallInfoBaseData<'mcx>>>,
        fn_addr: Option<PGFunction>,
        /// The scalar-arg cell: `&fcinfo->args[0].value` /
        /// `&fcinfo->args[0].isnull` — the aliasing target the scalar
        /// sub-expression evaluates into (execExpr.c: scalar recurses into
        /// `&fcinfo->args[0]`). Gathered into `fcinfo->args[0]` per array
        /// element before each comparison.
        scalar_cell: ResultCellId,
        /// The array-arg cell: `&fcinfo->args[1].value` /
        /// `&fcinfo->args[1].isnull` — the aliasing target the array
        /// sub-expression evaluates into; the step deconstructs that array and
        /// loads each element into `fcinfo->args[1]`.
        array_cell: ResultCellId,
    },
    /// `hashedscalararrayop` — for EEOP_HASHED_SCALARARRAYOP.
    HashedScalarArrayOp {
        has_nulls: bool,
        /// true for IN and false for NOT IN
        inclause: bool,
        /// `struct ScalarArrayOpExprHashTable *elements_tab` — the hash table
        /// the interpreter builds on first evaluation and reuses across rows.
        /// `None` is the C `NULL` (not yet built); per the "opacity inherited"
        /// rule this is the real typed table, not an address word.
        elements_tab: Option<alloc::boxed::Box<crate::saophash::ScalarArrayOpExprHashTable<'mcx>>>,
        finfo: Option<PgBox<'mcx, FmgrInfo>>,
        fcinfo_data: Option<PgBox<'mcx, FunctionCallInfoBaseData<'mcx>>>,
        /// `ScalarArrayOpExpr *saop` — original node.
        saop: Option<PgBox<'mcx, ScalarArrayOpExpr>>,
        /// The scalar-arg cell: `&fcinfo->args[0].value` /
        /// `&fcinfo->args[0].isnull` — execExpr.c (hashed path) recurses the
        /// scalar directly into `&fcinfo->args[0]`; the array side is
        /// precomputed into the `elements_tab` hash table at first execution,
        /// so only the scalar arg is gathered per row.
        scalar_cell: ResultCellId,
    },
    /// `xmlexpr` — for EEOP_XMLEXPR.
    XmlExpr {
        /// `XmlExpr *xexpr` — original node; parked until primnodes carries
        /// `XmlExpr` (opaque address).
        xexpr: usize,
        /// `Datum *named_argvalue`.
        named_argvalue: Option<PgVec<'mcx, Datum<'mcx>>>,
        /// `bool *named_argnull`.
        named_argnull: Option<PgVec<'mcx, bool>>,
        /// `Datum *argvalue`.
        argvalue: Option<PgVec<'mcx, Datum<'mcx>>>,
        /// `bool *argnull`.
        argnull: Option<PgVec<'mcx, bool>>,
    },
    /// `json_constructor` — for EEOP_JSON_CONSTRUCTOR.
    JsonConstructor {
        jcstate: Option<PgBox<'mcx, JsonConstructorExprState<'mcx>>>,
    },
    /// `aggref` — for EEOP_AGGREF.
    Aggref { aggno: i32 },
    /// `grouping_func` — for EEOP_GROUPING_FUNC.
    GroupingFunc {
        /// `List *clauses` — integer list of column numbers.
        clauses: Option<PgVec<'mcx, i32>>,
    },
    /// `window_func` — for EEOP_WINDOW_FUNC.
    WindowFunc {
        /// `WindowFuncExprState *wfstate` — out-of-line state owned by
        /// nodeWindowAgg; parked (opaque address) until threaded here.
        wfstate: usize,
    },
    /// `subplan` — for EEOP_SUBPLAN.
    SubPlan {
        /// `SubPlanState *sstate` — out-of-line state created by nodeSubplan.
        sstate: Option<PgBox<'mcx, SubPlanState<'mcx>>>,
    },
    /// `agg_deserialize` — for EEOP_AGG_*DESERIALIZE.
    AggDeserialize {
        fcinfo_data: Option<PgBox<'mcx, FunctionCallInfoBaseData<'mcx>>>,
        /// The deserialize input cell: the `&ds_fcinfo->args[0].value` /
        /// `&ds_fcinfo->args[0].isnull` aliasing target the serialized-state
        /// sub-expression evaluates into (execExpr.c:3785-3787). The
        /// interpreter gathers this arena cell into `fcinfo->args[0]` before
        /// calling the deserialization function. Always present (the
        /// deserialfn takes one real argument; args[1] is the dummy).
        arg_cell: ResultCellId,
        jumpnull: i32,
    },
    /// `agg_strict_input_check` — for
    /// EEOP_AGG_STRICT_INPUT_CHECK_NULLS / STRICT_INPUT_CHECK_ARGS.
    AggStrictInputCheck {
        /// `NullableDatum *args` — for the ARGS variant. C points this at
        /// `trans_fcinfo->args + 1` (the transfn's real argument cells); the
        /// step scans `args[i].isnull`. In the owned model the per-arg cells
        /// are named by [`Self::AggStrictInputCheck::arg_cells`]; this is kept
        /// as the owned copy/workspace.
        args: Option<PgVec<'mcx, NullableDatum>>,
        /// `bool *nulls` — for the NULLS variant (points at
        /// `pertrans->sortslot->tts_isnull`).
        nulls: Option<PgVec<'mcx, bool>>,
        /// Per-argument result cells the transfn-argument sub-expressions
        /// evaluate into — the `&trans_fcinfo->args[i]` aliasing targets the
        /// ARGS variant scans for NULLs (execExpr.c:3763/3901). The interpreter
        /// reads `is_null` of each cell to decide the strict-NULL bailout.
        /// Empty for the NULLS variant (which reads `nulls` directly).
        arg_cells: Option<PgVec<'mcx, ResultCellId>>,
        nargs: i32,
        jumpnull: i32,
    },
    /// `agg_plain_pergroup_nullcheck` — for EEOP_AGG_PLAIN_PERGROUP_NULLCHECK.
    AggPlainPergroupNullcheck { setoff: i32, jumpnull: i32 },
    /// `agg_presorted_distinctcheck` — for
    /// EEOP_AGG_PRESORTED_DISTINCT_{SINGLE,MULTI}.
    AggPresortedDistinctCheck {
        /// `AggStatePerTrans pertrans` — parked (nodeAgg-owned) opaque address.
        pertrans: usize,
        /// `ExprContext *aggcontext` — EState ExprContext pool id is threaded by
        /// the owner; opaque address for now.
        aggcontext: usize,
        jumpdistinct: i32,
    },
    /// `agg_trans` — for EEOP_AGG_PLAIN_TRANS_[INIT_][STRICT_]{BYVAL,BYREF}
    /// and EEOP_AGG_ORDERED_TRANS_{DATUM,TUPLE}.
    AggTrans {
        /// `AggStatePerTrans pertrans` — parked opaque address.
        pertrans: usize,
        /// `ExprContext *aggcontext` — opaque address.
        aggcontext: usize,
        setno: i32,
        transno: i32,
        setoff: i32,
    },
    /// `is_json` — for EEOP_IS_JSON.
    IsJson {
        /// `JsonIsPredicate *pred` — original node; parked (opaque address).
        pred: usize,
    },
    /// `jsonexpr` — for EEOP_JSONEXPR_PATH.
    JsonExpr {
        /// `struct JsonExprState *jsestate` — parked (opaque address).
        jsestate: usize,
    },
    /// `jsonexpr_coercion` — for EEOP_JSONEXPR_COERCION.
    JsonExprCoercion {
        targettype: Oid,
        targettypmod: i32,
        omit_quotes: bool,
        /// only relevant for JSON_EXISTS_OP
        exists_coerce: bool,
        exists_cast_to_int: bool,
        exists_check_domain: bool,
        /// `void *json_coercion_cache` — opaque address.
        json_coercion_cache: usize,
        /// `ErrorSaveContext *escontext` — opaque address.
        escontext: usize,
    },
}

/// `ExprEvalStep` (execExpr.h) — one instruction of a compiled expression
/// program.
///
/// In C this is a `{ intptr_t opcode; Datum *resvalue; bool *resnull; union d; }`
/// constrained to <= 64 bytes. Here `opcode` is the [`ExprEvalOp`] discriminant
/// (the C value is initially the enum, later rewritten to a computed-goto
/// pointer; the owned interpreter dispatches on the enum, so it is kept as the
/// enum) and the union payload is the tagged [`ExprEvalStepData`]. The result
/// pointers `resvalue`/`resnull` denote where this step stores its output.
#[derive(Debug)]
pub struct ExprEvalStep<'mcx> {
    /// `intptr_t opcode` — the instruction discriminant.
    pub opcode: ExprEvalOp,
    /// `Datum *resvalue` — where to store the result of this step. In the owned
    /// model this is a [`ResultCellId`] index into the owning [`ExprState`]'s
    /// [`ResultCellArena`] (mirroring `SlotId`/`EcxtId`); the well-known cell
    /// [`STATE_RESULT_CELL`] is the `ExprState`'s own `resvalue`/`resnull`, i.e.
    /// the C `&state->resvalue`/`&state->resnull` default target.
    pub resvalue: ResultCellId,
    /// `bool *resnull` — paired is-null cell. Shares its [`ResultCellId`] with
    /// `resvalue` (the C `resvalue`/`resnull` pointer pair always point at the
    /// matching `Datum`/`bool` of one logical cell).
    pub resnull: ResultCellId,
    /// `union d` — inline payload selected by `opcode`.
    pub d: ExprEvalStepData<'mcx>,
}

/// `SubscriptExecSteps` (execExpr.h) — execution-step methods used for
/// `SubscriptingRef` (see nodes/subscripting.h).
#[derive(Clone, Copy, Debug, Default)]
pub struct SubscriptExecSteps {
    /// `sbs_check_subscripts` — process subscripts.
    pub sbs_check_subscripts: Option<ExecEvalBoolSubroutine>,
    /// `sbs_fetch` — fetch an element.
    pub sbs_fetch: Option<ExecEvalSubroutine>,
    /// `sbs_assign` — assign to an element.
    pub sbs_assign: Option<ExecEvalSubroutine>,
    /// `sbs_fetch_old` — fetch old value for assignment.
    pub sbs_fetch_old: Option<ExecEvalSubroutine>,
}

/// Index of a per-step result cell in an [`ExprState`]'s [`ResultCellArena`].
///
/// In C an `ExprEvalStep`'s `resvalue`/`resnull` are raw `Datum *`/`bool *`
/// pointers, several steps commonly aliasing the same cell (a step computes
/// into a cell that a later step reads). Raw aliasing pointers do not survive
/// the move to owned storage, so — mirroring the [`SlotId`]/[`EcxtId`]
/// precedent (`TupleTableSlot *`/`ExprContext *` become indices into an
/// EState-owned `Vec`) — each result cell is identified by a `ResultCellId`
/// indexing into the owning `ExprState`'s arena. `ExprEvalPushStep` allocates
/// cells in the arena and records the id on the step; the interpreter reads
/// and writes through the id.
///
/// [`SlotId`]: crate::execnodes::SlotId
/// [`EcxtId`]: crate::execnodes::EcxtId
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ResultCellId(pub u32);

/// The well-known result cell that aliases the owning [`ExprState`]'s own
/// `resvalue`/`resnull` fields — the C `&state->resvalue` / `&state->resnull`
/// default output target threaded through `ExecInitExprRec` as `resv`/`resnull`.
/// Always present (allocated first) in every [`ResultCellArena`].
pub const STATE_RESULT_CELL: ResultCellId = ResultCellId(0);

/// One per-step result cell: the `(Datum, bool)` pair a `Datum *`/`bool *`
/// pointer pair points at in C. Stored in the [`ResultCellArena`] and addressed
/// by [`ResultCellId`].
#[derive(Clone, Debug)]
pub struct ResultCell<'mcx> {
    /// The cell's `Datum` value (the `*resvalue` target).
    pub value: Datum<'mcx>,
    /// The cell's is-null flag (the `*resnull` target).
    pub isnull: bool,
}

impl Default for ResultCell<'_> {
    fn default() -> Self {
        // A freshly-allocated cell holds a NULL value, not-null cleared.
        ResultCell {
            value: Datum::null(),
            isnull: false,
        }
    }
}

/// Arena of per-step result cells owned by an [`ExprState`]. Replaces the web
/// of `Datum *`/`bool *` aliasing pointers in the C `ExprEvalStep`s with
/// [`ResultCellId`] indices into a single owned `Vec`, exactly as
/// [`SlotId`]/[`EcxtId`] replace `TupleTableSlot *`/`ExprContext *`.
///
/// [`SlotId`]: crate::execnodes::SlotId
/// [`EcxtId`]: crate::execnodes::EcxtId
#[derive(Debug, Default)]
pub struct ResultCellArena<'mcx> {
    /// The cells, indexed by [`ResultCellId`].
    pub cells: Option<PgVec<'mcx, ResultCell<'mcx>>>,
}

impl<'mcx> ResultCellArena<'mcx> {
    /// Read a cell by id.
    pub fn get(&self, id: ResultCellId) -> ResultCell<'mcx> {
        self.cells
            .as_ref()
            .and_then(|c| c.get(id.0 as usize).cloned())
            .unwrap_or_default()
    }

    /// Write a cell by id (extends the arena with default cells if needed).
    pub fn set(&mut self, id: ResultCellId, cell: ResultCell<'mcx>) {
        if let Some(cells) = self.cells.as_mut() {
            let i = id.0 as usize;
            if i < cells.len() {
                cells[i] = cell;
            }
        }
    }

    /// Number of cells currently allocated.
    pub fn len(&self) -> usize {
        self.cells.as_ref().map(|c| c.len()).unwrap_or(0)
    }

    /// Whether the arena has no cells.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// `ExprState` (execnodes.h) — the compiled, executable form of an expression
/// tree (`ExecInitExpr`/`ExecInitQual` output).
///
/// Full layout: the `flags` bitmask (`EEO_FLAG_*`), result storage
/// (`resvalue`/`resnull`/`resultslot`), the linear `steps` program plus its
/// `evalfunc`, the original `expr` (debug back-link), and the
/// compile-time-only bookkeeping (`steps_len`/`steps_alloc`, `parent`,
/// `ext_params`, the innermost case/domain value pointers, and the soft-error
/// `escontext`). The C `evalfunc_private` is an opaque interpreter scratch
/// pointer, carried as an address.
#[derive(Debug)]
pub struct ExprState<'mcx> {
    /// `uint8 flags` — bitmask of `EEO_FLAG_*` bits.
    pub flags: u8,
    /// `bool resnull` — is-null companion to `resvalue`.
    pub resnull: bool,
    /// `Datum resvalue` — scalar result, or per-column result during
    /// projection.
    pub resvalue: Datum<'mcx>,
    /// `TupleTableSlot *resultslot` — holds the result if projecting a tuple,
    /// else NULL.
    pub resultslot: Option<PgBox<'mcx, TupleTableSlot>>,
    /// `struct ExprEvalStep *steps` — instructions computing the return value.
    pub steps: Option<PgVec<'mcx, ExprEvalStep<'mcx>>>,
    /// Per-step result-cell arena (the owned-model replacement for the C
    /// `Datum *resvalue`/`bool *resnull` aliasing pointers; see
    /// [`ResultCellArena`]). `ExprEvalPushStep` allocates cells here.
    pub result_cells: ResultCellArena<'mcx>,
    /// `ExprStateEvalFunc evalfunc` — function that evaluates the expression.
    pub evalfunc: Option<ExprStateEvalFunc>,
    /// `Expr *expr` — original expression tree (debugging only).
    pub expr: Option<PgBox<'mcx, Expr>>,
    /// `void *evalfunc_private` — private interpreter scratch (opaque address).
    pub evalfunc_private: usize,
    /// `int steps_len` — number of steps currently (compile-time only).
    pub steps_len: i32,
    /// `int steps_alloc` — allocated length of the steps array.
    pub steps_alloc: i32,
    /// `struct PlanState *parent` — parent PlanState node, if any.
    pub parent: Option<PgBox<'mcx, PlanStateNode<'mcx>>>,
    /// `ParamListInfo ext_params` — for compiling PARAM_EXTERN nodes (opaque
    /// address; the param-list owner threads the real list).
    pub ext_params: usize,
    /// `Datum *innermost_caseval` / `bool *innermost_casenull` — the arena cell
    /// holding the innermost CASE test value while compiling a `CaseExpr` arm
    /// (`None` outside any enclosing CASE). [`ResultCellId`] into `result_cells`.
    pub innermost_caseval: Option<ResultCellId>,
    /// `Datum *innermost_domainval` / `bool *innermost_domainnull` — the arena
    /// cell holding the innermost domain value while compiling a `CoerceToDomain`
    /// (`None` outside any enclosing domain coercion).
    pub innermost_domainval: Option<ResultCellId>,
    /// `ErrorSaveContext *escontext` — soft-error sink; NULL means throw
    /// (opaque address until the sink is threaded here).
    pub escontext: usize,
}

impl<'mcx> Clone for ExprState<'mcx> {
    /// Clone the lightweight handle fields (`flags` / `resvalue` / `resnull`
    /// and the compile-time scalar bookkeeping); the compiled program
    /// (`steps` / `result_cells` / `resultslot` / `parent` / `expr`) and the
    /// owned `PgBox`/`PgVec` cells are NOT deep-copied — they reset to `None` /
    /// empty / `Default`. A compiled `ExprState` is owned by its EState's
    /// per-query context and is never deep-cloned during execution; consumers
    /// that `.clone()` an `ExprState` only carry the handle/flags (mirroring
    /// the trivial pre-union `ExprState { flags }`). Recompile via
    /// `ExecInitExpr` to obtain a fresh program.
    fn clone(&self) -> Self {
        ExprState {
            flags: self.flags,
            resnull: self.resnull,
            resvalue: self.resvalue.clone(),
            resultslot: None,
            steps: None,
            result_cells: ResultCellArena::default(),
            evalfunc: self.evalfunc,
            expr: None,
            evalfunc_private: self.evalfunc_private,
            steps_len: self.steps_len,
            steps_alloc: self.steps_alloc,
            parent: None,
            ext_params: self.ext_params,
            innermost_caseval: None,
            innermost_domainval: None,
            escontext: self.escontext,
        }
    }
}

impl Default for ExprState<'_> {
    fn default() -> Self {
        // The C `ExprState` is `palloc0`'d / `makeNode`'d: all-zero, NULL
        // result value. The canonical `Datum` is not itself `Default`, so the
        // NULL `resvalue` is spelled out.
        ExprState {
            flags: 0,
            resnull: false,
            resvalue: Datum::null(),
            resultslot: None,
            steps: None,
            result_cells: ResultCellArena::default(),
            evalfunc: None,
            expr: None,
            evalfunc_private: 0,
            steps_len: 0,
            steps_alloc: 0,
            parent: None,
            ext_params: 0,
            innermost_caseval: None,
            innermost_domainval: None,
            escontext: 0,
        }
    }
}

/// `T_SetExprState` (nodes/nodetags.h) — the executor-state node tag for a
/// [`SetExprState`]. Verified against PostgreSQL 18.3's generated `nodetags.h`
/// (value 391).
pub const T_SetExprState: NodeTag = NodeTag(391);

/// `ExprDoneCond` (executor/executor.h / nodes/execnodes.h) — whether an
/// expression's evaluation is complete, mid-set, or exhausted.
///
/// ```c
/// typedef enum
/// {
///     ExprSingleResult,       /* expression does not return a set */
///     ExprMultipleResult,     /* this result is an element of a set */
///     ExprEndResult,          /* there are no more elements in the set */
/// } ExprDoneCond;
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u32)]
pub enum ExprDoneCond {
    /// `ExprSingleResult` — expression does not return a set.
    #[default]
    ExprSingleResult,
    /// `ExprMultipleResult` — this result is an element of a set.
    ExprMultipleResult,
    /// `ExprEndResult` — there are no more elements in the set.
    ExprEndResult,
}

/// `SetExprState` (execnodes.h) — state for evaluating a potentially
/// set-returning expression (a `FuncExpr` or `OpExpr`).
///
/// ```c
/// typedef struct SetExprState
/// {
///     NodeTag     type;
///     Expr       *expr;
///     List       *args;
///     ExprState  *elidedFuncState;
///     FmgrInfo    func;
///     Tuplestorestate *funcResultStore;
///     TupleTableSlot *funcResultSlot;
///     TupleDesc   funcResultDesc;
///     bool        funcReturnsTuple;
///     bool        funcReturnsSet;
///     bool        setArgsValid;
///     bool        shutdown_reg;
///     FunctionCallInfo fcinfo;
/// } SetExprState;
/// ```
///
/// All of these fields are produced and consumed by the still-unported
/// `execSRF.c` owner (via `ExecInitFunctionResultSet` /
/// `ExecMakeFunctionResultSet`); `nodeProjectSet` only holds the boxed value in
/// its `elems[]` array and hands a `&mut` to the owner's seam. The `expr` /
/// `args` plan-tree links and the heterogeneous SRF execution state therefore
/// live here as owned/boxed fields exactly as the C struct lays them out.
#[derive(Debug, Default)]
pub struct SetExprState<'mcx> {
    /// `Expr *expr` — the expression plan node (`FuncExpr`/`OpExpr`).
    pub expr: Option<PgBox<'mcx, Expr>>,
    /// `List *args` — `ExprState`s for the argument expressions.
    pub args: Option<PgVec<'mcx, ExprState<'mcx>>>,
    /// `ExprState *elidedFuncState` — for an inlined ROWS FROM function, the
    /// compiled non-set-returning expression evaluated with regular
    /// `ExecEvalExpr` (`None` = the C `NULL`).
    pub elidedFuncState: Option<PgBox<'mcx, ExprState<'mcx>>>,
    /// `FmgrInfo func` — function-manager lookup info for the target function
    /// (`func.fn_oid == InvalidOid` until initialized).
    pub func: FmgrInfo,
    /// `Tuplestorestate *funcResultStore` — materialized SRF result rows
    /// (`None` = the C `NULL`).
    pub funcResultStore: Option<PgBox<'mcx, crate::funcapi::Tuplestorestate<'mcx>>>,
    /// `TupleTableSlot *funcResultSlot` — the row currently being returned
    /// (`None` = the C `NULL`).
    pub funcResultSlot: Option<PgBox<'mcx, TupleTableSlot>>,
    /// `TupleDesc funcResultDesc` — tuple descriptor for the function's output
    /// (`None` = the C `NULL`).
    pub funcResultDesc: Option<PgBox<'mcx, TupleDescData<'mcx>>>,
    /// `bool funcReturnsTuple` — valid when `funcResultDesc` isn't NULL.
    pub funcReturnsTuple: bool,
    /// `bool funcReturnsSet` — whether the function is declared to return a set
    /// (set by `ExecInitExpr`, valid even before the `FmgrInfo` is set up).
    pub funcReturnsSet: bool,
    /// `bool setArgsValid` — true when mid value-per-call series, so
    /// `fcinfo` already holds valid argument data.
    pub setArgsValid: bool,
    /// `bool shutdown_reg` — whether a shutdown callback is registered.
    pub shutdown_reg: bool,
    /// `FunctionCallInfo fcinfo` — call-parameter structure for the function
    /// (`None` = not yet initialized).
    pub fcinfo: Option<PgBox<'mcx, FunctionCallInfoBaseData<'mcx>>>,
}

impl SetExprState<'_> {
    /// `nodeTag(node)` — always [`T_SetExprState`].
    #[inline]
    pub fn tag(&self) -> NodeTag {
        T_SetExprState
    }
}

/// `ProjectionInfo` (execnodes.h) — node for caching needed info for
/// projection.
///
/// `pi_state` is the compiled [`ExprState`] program that assigns each target
/// column; `pi_exprContext` is the [`EcxtId`] of the projection's expression
/// context (`None` until the projection is built by execExpr).
#[derive(Clone, Debug, Default)]
pub struct ProjectionInfo<'mcx> {
    /// `ExprContext *pi_exprContext` — context holding the evaluation slots
    /// (`ecxt_scantuple` / `ecxt_outertuple` / `ecxt_oldtuple` /
    /// `ecxt_newtuple`). `None` until the projection is built by execExpr.
    pub pi_exprContext: Option<EcxtId>,
    /// `ExprState pi_state` — the compiled projection state (embedded in C).
    pub pi_state: ExprState<'mcx>,
}

/// `SubPlanState` (execnodes.h) — executor state for a subplan.
///
/// The `planstate` field is consumed by the `ExecReScan` walk; the remaining
/// fields are consumed by `nodeSubplan.c` (the owning unit). The compiled
/// expression states (`testexpr`, `lhs_hash_expr`, `cur_eq_comp`), the two
/// projection nodes (`projLeft`/`projRight`), and the two `TupleHashTable`s
/// (`hashtable`/`hashnulls`) belong to the still-unported execExpr /
/// execGrouping units; here they are heterogeneous owned slots ([`Opaque`])
/// that nodeSubplan only builds and probes through those units' seams. The C
/// `parent` back-pointer is not carried: callers thread the parent state
/// explicitly.
#[derive(Debug)]
pub struct SubPlanState<'mcx> {
    /// `SubPlan *subplan` — the expression plan node.
    pub subplan: Option<PgBox<'mcx, SubPlan<'mcx>>>,
    /// `PlanState *planstate` — the subselect plan's state tree.
    pub planstate: Option<PgBox<'mcx, PlanStateNode<'mcx>>>,
    /// `ExprState *testexpr` — state of combining expression (execExpr-owned).
    pub testexpr: Opaque,
    /// `HeapTuple curTuple` — copy of most recent tuple from subplan.
    pub curTuple: HeapTuple<'mcx>,
    /// `Datum curArray` — most recent array from `ARRAY()` subplan.
    pub curArray: Datum<'mcx>,
    /// `TupleDesc descRight` — subselect desc after projection.
    pub descRight: Option<PgBox<'mcx, TupleDescData<'mcx>>>,
    /// `ProjectionInfo *projLeft` — for projecting lefthand exprs
    /// (execExpr-owned).
    pub projLeft: Opaque,
    /// `ProjectionInfo *projRight` — for projecting subselect output
    /// (execExpr-owned).
    pub projRight: Opaque,
    /// `TupleHashTable hashtable` — hash table for no-nulls subselect rows.
    /// The real owned execGrouping table (`TupleHashTable` in C is
    /// `TupleHashTableData *`; carried by box here).
    pub hashtable: Option<alloc::boxed::Box<crate::nodeagg::TupleHashTable<'mcx>>>,
    /// `TupleHashTable hashnulls` — hash table for rows with null(s).
    pub hashnulls: Option<alloc::boxed::Box<crate::nodeagg::TupleHashTable<'mcx>>>,
    /// `bool havehashrows` — true if `hashtable` is not empty.
    pub havehashrows: bool,
    /// `bool havenullrows` — true if `hashnulls` is not empty.
    pub havenullrows: bool,
    /// `MemoryContext hashtablecxt` — memory context containing hash tables.
    pub hashtablecxt: Option<MemoryContext>,
    /// `MemoryContext hashtempcxt` — temp memory context for hash tables.
    pub hashtempcxt: Option<MemoryContext>,
    /// `TupleHashIterator` cursor used by `findPartialMatch`'s full-table scan
    /// (the C `findPartialMatch` keeps a stack-local `hashiter`; the owned
    /// model carries it on the node so the canonical iterator seams can
    /// advance over the real table). One scan is active at a time.
    pub hashiter: crate::nodeagg::TupleHashIterator,
    /// `ExprContext *innerecontext` — econtext for computing inner tuples (id
    /// into the EState's `es_exprcontexts`).
    pub innerecontext: Option<EcxtId>,
    /// `int numCols` — number of columns being hashed.
    pub numCols: i32,
    /// `AttrNumber *keyColIdx` — control data for hash tables (length
    /// `numCols`).
    pub keyColIdx: Option<PgVec<'mcx, AttrNumber>>,
    /// `Oid *tab_eq_funcoids` — equality func oids for table datatype(s).
    pub tab_eq_funcoids: Option<PgVec<'mcx, Oid>>,
    /// `Oid *tab_collations` — collations for hash and comparison.
    pub tab_collations: Option<PgVec<'mcx, Oid>>,
    /// `FmgrInfo *tab_hash_funcs` — hash functions for table datatype(s).
    pub tab_hash_funcs: Option<PgVec<'mcx, FmgrInfo>>,
    /// `ExprState *lhs_hash_expr` — hash expr for lefthand datatype(s)
    /// (execExpr-owned).
    pub lhs_hash_expr: Opaque,
    /// `FmgrInfo *cur_eq_funcs` — equality functions for LHS vs. table.
    pub cur_eq_funcs: Option<PgVec<'mcx, FmgrInfo>>,
    /// `ExprState *cur_eq_comp` — equality comparator for LHS vs. table
    /// (execExpr-owned).
    pub cur_eq_comp: Opaque,
}

impl Default for SubPlanState<'_> {
    fn default() -> Self {
        // `makeNode(SubPlanState)` zero-init; the canonical `Datum` is not
        // `Default`, so the NULL `curArray` is spelled out.
        SubPlanState {
            subplan: None,
            planstate: None,
            testexpr: Default::default(),
            curTuple: Default::default(),
            curArray: Datum::null(),
            descRight: None,
            projLeft: Default::default(),
            projRight: Default::default(),
            hashtable: None,
            hashnulls: None,
            havehashrows: false,
            havenullrows: false,
            hashtablecxt: None,
            hashtempcxt: None,
            hashiter: Default::default(),
            innerecontext: None,
            numCols: 0,
            keyColIdx: None,
            tab_eq_funcoids: None,
            tab_collations: None,
            tab_hash_funcs: None,
            lhs_hash_expr: Default::default(),
            cur_eq_funcs: None,
            cur_eq_comp: Default::default(),
        }
    }
}

/// `LastAttnumInfo` (execExpr.c) — bookkeeping used by
/// `ExecComputeSlotInfo`/`ExecCreateExprSetupSteps`: the highest attribute
/// number referenced from each input slot, so a single FETCHSOME step can
/// deform up to that attnum.
#[derive(Clone, Copy, Debug, Default)]
pub struct LastAttnumInfo {
    /// highest attnum referenced from the inner slot
    pub last_inner: AttrNumber,
    /// highest attnum referenced from the outer slot
    pub last_outer: AttrNumber,
    /// highest attnum referenced from the scan slot
    pub last_scan: AttrNumber,
}

/// `ExprSetupInfo` (execExpr.c) — collected info that
/// `ExecCreateExprSetupSteps`/`expr_setup_walker` accumulates before emitting
/// the leading FETCHSOME/whole-row setup steps.
#[derive(Debug, Default)]
pub struct ExprSetupInfo {
    /// last referenced attnum per input slot (see [`LastAttnumInfo`]).
    pub last_attnums: LastAttnumInfo,
    /// the MULTIEXPR subplan ids the setup walker must wire so the spine can
    /// emit one FETCHSOME each (`List *multiexpr_subplans` in C).
    pub multiexpr_subplans: i32,
}

/// `T_SubPlanState` (nodes/nodetags.h) — PostgreSQL 18.3 generated value.
pub const T_SubPlanState: NodeTag = NodeTag(392);

/// `T_SubPlan` (nodes/nodetags.h) — PostgreSQL 18.3 generated value.
pub const T_SubPlan: NodeTag = NodeTag(23);

#[cfg(test)]
mod tests {
    use super::*;

    /// `enum ExprEvalOp` has exactly 121 enumerators in PostgreSQL 18
    /// (`EEOP_DONE_RETURN` .. `EEOP_LAST`), so `NUM_EXPR_EVAL_OPS == 121`. The
    /// dispatch table in `execExprInterp.c` is sized from `EEOP_LAST`, so this
    /// count is load-bearing.
    #[test]
    fn expr_eval_op_count_matches_header() {
        assert_eq!(NUM_EXPR_EVAL_OPS, 121);
        assert_eq!(ExprEvalOp::EEOP_LAST as u32, 120);
    }

    /// Spot-check a few discriminants against the C enumerator order (the order
    /// must stay in sync with the interpreter dispatch table).
    #[test]
    fn expr_eval_op_discriminants_match_order() {
        assert_eq!(ExprEvalOp::EEOP_DONE_RETURN as u32, 0);
        assert_eq!(ExprEvalOp::EEOP_DONE_NO_RETURN as u32, 1);
        assert_eq!(ExprEvalOp::EEOP_INNER_FETCHSOME as u32, 2);
        assert_eq!(ExprEvalOp::EEOP_CONST as u32, 25);
        assert_eq!(ExprEvalOp::EEOP_FUNCEXPR as u32, 26);
        assert_eq!(ExprEvalOp::EEOP_QUAL as u32, 39);
    }

    /// The internal `EEO_FLAG_*` interpreter bits sit above the public ones
    /// (execnodes.h reserves bits 0..=4; execExpr.h adds bits 5 and 6).
    #[test]
    fn eeo_flag_bits_match_header() {
        assert_eq!(EEO_FLAG_INTERPRETER_INITIALIZED, 1 << 5);
        assert_eq!(EEO_FLAG_DIRECT_THREADED, 1 << 6);
    }
}
