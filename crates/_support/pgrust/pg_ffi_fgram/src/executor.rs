use core::ffi::{c_char, c_int, c_uint, c_void};

use crate::{
    uint16, uint32, uint8, AttrNumber, Buffer, Datum, HeapTuple, HeapTupleData, ItemPointerData,
    List, MemoryContext, MinimalTuple, NodeTag, Oid, Size, TupleDesc,
};

pub use crate::funcapi::AttInMetadata;

pub const T_TupleTableSlot: NodeTag = 443;
pub const T_JunkFilter: NodeTag = 385;
pub const T_TargetEntry: NodeTag = 62;
pub const T_ReturnSetInfo: NodeTag = 383;
pub const T_ExprContext: NodeTag = 382;
pub const InvalidAttrNumber: AttrNumber = 0;

// Special varno values for Var nodes referencing executor plan-node inputs
// (primnodes.h). These are negative `varno` sentinels.
/// `INNER_VAR` — reference to inner subplan.
pub const INNER_VAR: c_int = -1;
/// `OUTER_VAR` — reference to outer subplan.
pub const OUTER_VAR: c_int = -2;
/// `INDEX_VAR` — reference to index column.
pub const INDEX_VAR: c_int = -3;

pub const TTS_FLAG_EMPTY: uint16 = 1 << 1;
pub const TTS_FLAG_SHOULDFREE: uint16 = 1 << 2;
pub const TTS_FLAG_SLOW: uint16 = 1 << 3;
pub const TTS_FLAG_FIXED: uint16 = 1 << 4;
pub const InvalidBuffer: Buffer = 0;

pub type CommandDest = c_uint;
pub type Index = c_uint;

pub const DestNone: CommandDest = 0;
pub const DestDebug: CommandDest = 1;
pub const DestRemote: CommandDest = 2;
pub const DestRemoteExecute: CommandDest = 3;
pub const DestRemoteSimple: CommandDest = 4;
pub const DestSPI: CommandDest = 5;
pub const DestTuplestore: CommandDest = 6;
pub const DestIntoRel: CommandDest = 7;
pub const DestCopyOut: CommandDest = 8;
pub const DestSQLFunction: CommandDest = 9;
pub const DestTransientRel: CommandDest = 10;
pub const DestTupleQueue: CommandDest = 11;
pub const DestExplainSerialize: CommandDest = 12;

// Executor eflags bits (executor/executor.h).
/// `EXEC_FLAG_EXPLAIN_ONLY` — EXPLAIN, no ANALYZE.
pub const EXEC_FLAG_EXPLAIN_ONLY: c_int = 0x0001;
/// `EXEC_FLAG_EXPLAIN_GENERIC` — EXPLAIN (GENERIC_PLAN).
pub const EXEC_FLAG_EXPLAIN_GENERIC: c_int = 0x0002;
/// `EXEC_FLAG_REWIND` — need efficient rescan.
pub const EXEC_FLAG_REWIND: c_int = 0x0004;
/// `EXEC_FLAG_BACKWARD` — need backward scan.
pub const EXEC_FLAG_BACKWARD: c_int = 0x0008;
/// `EXEC_FLAG_MARK` — need mark/restore.
pub const EXEC_FLAG_MARK: c_int = 0x0010;
/// `EXEC_FLAG_SKIP_TRIGGERS` — skip AfterTrigger setup.
pub const EXEC_FLAG_SKIP_TRIGGERS: c_int = 0x0020;
/// `EXEC_FLAG_WITH_NO_DATA` — REFRESH ... WITH NO DATA.
pub const EXEC_FLAG_WITH_NO_DATA: c_int = 0x0040;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TupleTableSlotOps {
    pub base_slot_size: Size,
    pub init: Option<unsafe extern "C" fn(*mut TupleTableSlot)>,
    pub release: Option<unsafe extern "C" fn(*mut TupleTableSlot)>,
    pub clear: Option<unsafe extern "C" fn(*mut TupleTableSlot)>,
    pub getsomeattrs: Option<unsafe extern "C" fn(*mut TupleTableSlot, c_int)>,
    pub getsysattr: Option<unsafe extern "C" fn(*mut TupleTableSlot, c_int, *mut bool) -> Datum>,
    pub is_current_xact_tuple: Option<unsafe extern "C" fn(*mut TupleTableSlot) -> bool>,
    pub materialize: Option<unsafe extern "C" fn(*mut TupleTableSlot)>,
    pub copyslot: Option<unsafe extern "C" fn(*mut TupleTableSlot, *mut TupleTableSlot)>,
    pub get_heap_tuple: Option<unsafe extern "C" fn(*mut TupleTableSlot) -> HeapTuple>,
    pub get_minimal_tuple: Option<unsafe extern "C" fn(*mut TupleTableSlot) -> MinimalTuple>,
    pub copy_heap_tuple: Option<unsafe extern "C" fn(*mut TupleTableSlot) -> HeapTuple>,
    pub copy_minimal_tuple: Option<unsafe extern "C" fn(*mut TupleTableSlot, Size) -> MinimalTuple>,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TupleTableSlot {
    pub type_: NodeTag,
    pub tts_flags: uint16,
    pub tts_nvalid: AttrNumber,
    pub tts_ops: *const TupleTableSlotOps,
    pub tts_tupleDescriptor: TupleDesc,
    pub tts_values: *mut Datum,
    pub tts_isnull: *mut bool,
    pub tts_mcxt: MemoryContext,
    pub tts_tid: ItemPointerData,
    pub tts_tableOid: Oid,
}

impl TupleTableSlot {
    pub const fn is_empty(&self) -> bool {
        self.tts_flags & TTS_FLAG_EMPTY != 0
    }

    pub const fn should_free(&self) -> bool {
        self.tts_flags & TTS_FLAG_SHOULDFREE != 0
    }

    pub const fn is_fixed(&self) -> bool {
        self.tts_flags & TTS_FLAG_FIXED != 0
    }

    pub fn mark_empty(&mut self) {
        self.tts_flags |= TTS_FLAG_EMPTY;
        self.tts_flags &= !TTS_FLAG_SHOULDFREE;
        self.tts_nvalid = 0;
    }

    pub fn mark_not_empty(&mut self) {
        self.tts_flags &= !TTS_FLAG_EMPTY;
    }

    pub fn set_should_free(&mut self, should_free: bool) {
        if should_free {
            self.tts_flags |= TTS_FLAG_SHOULDFREE;
        } else {
            self.tts_flags &= !TTS_FLAG_SHOULDFREE;
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct VirtualTupleTableSlot {
    pub base: TupleTableSlot,
    pub data: *mut c_char,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct HeapTupleTableSlot {
    pub base: TupleTableSlot,
    pub tuple: HeapTuple,
    pub off: uint32,
    pub tupdata: HeapTupleData,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct MinimalTupleTableSlot {
    pub base: TupleTableSlot,
    pub tuple: HeapTuple,
    pub mintuple: MinimalTuple,
    pub minhdr: HeapTupleData,
    pub off: uint32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct BufferHeapTupleTableSlot {
    pub base: HeapTupleTableSlot,
    pub buffer: Buffer,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct AttrMap {
    pub attnums: *mut AttrNumber,
    pub maplen: c_int,
}

/// `SH_STATUS_EMPTY` for the `tuplehash` simplehash table: bucket is unused.
pub const TUPLEHASH_STATUS_EMPTY: uint32 = 0x00;
/// `SH_STATUS_IN_USE` for the `tuplehash` simplehash table: bucket is occupied.
pub const TUPLEHASH_STATUS_IN_USE: uint32 = 0x01;

/// `TupleHashEntryData` from `src/include/nodes/execnodes.h`.
///
/// One element of the `tuplehash` simplehash array used by `execGrouping.c`.
/// The `firstTuple` field is the simplehash key (`SH_KEY firstTuple`), and
/// `hash` is the stored hash value (`SH_STORE_HASH` / `SH_GET_HASH`). The
/// pointed-to `MinimalTuple` may have `additionalsize` extra bytes allocated
/// immediately before it (see `TupleHashEntryGetAdditional`).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TupleHashEntryData {
    /// Copy of the first tuple in this group.
    pub firstTuple: MinimalTuple,
    /// Hash status (`TUPLEHASH_STATUS_EMPTY` / `TUPLEHASH_STATUS_IN_USE`).
    pub status: uint32,
    /// Hash value (cached).
    pub hash: uint32,
}

impl Default for TupleHashEntryData {
    fn default() -> Self {
        Self {
            firstTuple: core::ptr::null_mut(),
            status: TUPLEHASH_STATUS_EMPTY,
            hash: 0,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TupleConversionMap {
    pub indesc: TupleDesc,
    pub outdesc: TupleDesc,
    pub attrMap: *mut AttrMap,
    pub invalues: *mut Datum,
    pub inisnull: *mut bool,
    pub outvalues: *mut Datum,
    pub outisnull: *mut bool,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct DestReceiver {
    pub receiveSlot: Option<unsafe extern "C" fn(*mut TupleTableSlot, *mut DestReceiver) -> bool>,
    pub rStartup: Option<unsafe extern "C" fn(*mut DestReceiver, c_int, TupleDesc)>,
    pub rShutdown: Option<unsafe extern "C" fn(*mut DestReceiver)>,
    pub rDestroy: Option<unsafe extern "C" fn(*mut DestReceiver)>,
    pub mydest: CommandDest,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TupOutputState {
    pub slot: *mut TupleTableSlot,
    pub dest: *mut DestReceiver,
}

#[repr(C)]
pub struct ScanState {
    _private: [u8; 0],
}

pub type OpaqueRelation = *mut c_void;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Expr {
    pub type_: NodeTag,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct TargetEntry {
    pub xpr: Expr,
    pub expr: *mut Expr,
    pub resno: AttrNumber,
    pub resname: *mut c_char,
    pub ressortgroupref: Index,
    pub resorigtbl: Oid,
    pub resorigcol: AttrNumber,
    pub resjunk: bool,
}

impl TargetEntry {
    pub const fn new(resno: AttrNumber, resname: *mut c_char, resjunk: bool) -> Self {
        Self {
            xpr: Expr {
                type_: T_TargetEntry,
            },
            expr: core::ptr::null_mut(),
            resno,
            resname,
            ressortgroupref: 0,
            resorigtbl: 0,
            resorigcol: 0,
            resjunk,
        }
    }

    pub fn is_junk(&self) -> bool {
        self.resjunk
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct JunkFilter {
    pub type_: NodeTag,
    pub jf_targetList: *mut List,
    pub jf_cleanTupType: TupleDesc,
    pub jf_cleanMap: *mut AttrNumber,
    pub jf_resultSlot: *mut TupleTableSlot,
}

// EEO_FLAG_* flags on ExprState.flags, used by the interpreter setup path
// (see src/include/executor/execExpr.h).
pub const EEO_FLAG_INTERPRETER_INITIALIZED: uint8 = 1 << 5;
pub const EEO_FLAG_DIRECT_THREADED: uint8 = 1 << 6;

/// ExprEvalOp — the discriminated opcodes of a flattened ExprState program.
///
/// This enum is part of the executor ABI: execExpr.c emits these opcodes and
/// execExprInterp.c dispatches on them. Order matters and must match
/// src/include/executor/execExpr.h exactly; the C code relies on the ordinal
/// values (e.g. EEOP_LAST as an array-length sentinel).
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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

    /// compute non-system Var value, assign into ExprState's resultslot
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

    /// boolean AND expression, one step per subexpression
    EEOP_BOOL_AND_STEP_FIRST,
    EEOP_BOOL_AND_STEP,
    EEOP_BOOL_AND_STEP_LAST,

    /// boolean OR expression
    EEOP_BOOL_OR_STEP_FIRST,
    EEOP_BOOL_OR_STEP,
    EEOP_BOOL_OR_STEP_LAST,

    /// boolean NOT expression
    EEOP_BOOL_NOT_STEP,

    /// simplified version of BOOL_AND_STEP for use by ExecQual()
    EEOP_QUAL,

    /// unconditional jump to another step
    EEOP_JUMP,

    /// conditional jumps based on current result value
    EEOP_JUMP_IF_NULL,
    EEOP_JUMP_IF_NOT_NULL,
    EEOP_JUMP_IF_NOT_TRUE,

    /// NULL tests for scalar values
    EEOP_NULLTEST_ISNULL,
    EEOP_NULLTEST_ISNOTNULL,

    /// NULL tests for row values
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

    /// assorted special-purpose expression types
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

    /// compare two individual elements of two compared ROW() expressions
    EEOP_ROWCOMPARE_STEP,

    /// evaluate boolean value based on previous ROWCOMPARE_STEP operations
    EEOP_ROWCOMPARE_FINAL,

    /// evaluate GREATEST() or LEAST()
    EEOP_MINMAX,

    /// evaluate FieldSelect expression
    EEOP_FIELDSELECT,

    /// deform tuple before evaluating new values in a FieldStore expression
    EEOP_FIELDSTORE_DEFORM,

    /// form the new tuple for a FieldStore expression
    EEOP_FIELDSTORE_FORM,

    /// process container subscripts; possibly short-circuit result to NULL
    EEOP_SBSREF_SUBSCRIPTS,

    /// compute old container element/slice for SubscriptingRef assignment
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

    /// assorted special-purpose expression types
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

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn tuple_slot_layout_matches_pg_abi_on_64_bit() {
        assert_eq!(size_of::<TupleTableSlotOps>(), 104);
        assert_eq!(align_of::<TupleTableSlotOps>(), 8);

        assert_eq!(offset_of!(TupleTableSlot, type_), 0);
        assert_eq!(offset_of!(TupleTableSlot, tts_flags), 4);
        assert_eq!(offset_of!(TupleTableSlot, tts_nvalid), 6);
        assert_eq!(offset_of!(TupleTableSlot, tts_ops), 8);
        assert_eq!(offset_of!(TupleTableSlot, tts_tupleDescriptor), 16);
        assert_eq!(offset_of!(TupleTableSlot, tts_values), 24);
        assert_eq!(offset_of!(TupleTableSlot, tts_isnull), 32);
        assert_eq!(offset_of!(TupleTableSlot, tts_mcxt), 40);
        assert_eq!(offset_of!(TupleTableSlot, tts_tid), 48);
        assert_eq!(offset_of!(TupleTableSlot, tts_tableOid), 56);
        assert_eq!(size_of::<TupleTableSlot>(), 64);
        assert_eq!(align_of::<TupleTableSlot>(), 8);

        assert_eq!(offset_of!(VirtualTupleTableSlot, data), 64);
        assert_eq!(size_of::<VirtualTupleTableSlot>(), 72);
        assert_eq!(offset_of!(HeapTupleTableSlot, tuple), 64);
        assert_eq!(offset_of!(HeapTupleTableSlot, off), 72);
        assert_eq!(offset_of!(HeapTupleTableSlot, tupdata), 80);
        assert_eq!(size_of::<HeapTupleTableSlot>(), 104);
        assert_eq!(offset_of!(MinimalTupleTableSlot, tuple), 64);
        assert_eq!(offset_of!(MinimalTupleTableSlot, mintuple), 72);
        assert_eq!(offset_of!(MinimalTupleTableSlot, minhdr), 80);
        assert_eq!(offset_of!(MinimalTupleTableSlot, off), 104);
        assert_eq!(size_of::<MinimalTupleTableSlot>(), 112);
        assert_eq!(offset_of!(BufferHeapTupleTableSlot, buffer), 104);
        assert_eq!(size_of::<BufferHeapTupleTableSlot>(), 112);

        assert_eq!(size_of::<TupleConversionMap>(), 56);
        assert_eq!(size_of::<DestReceiver>(), 40);
        assert_eq!(size_of::<TupOutputState>(), 16);

        assert_eq!(offset_of!(TupleHashEntryData, firstTuple), 0);
        assert_eq!(offset_of!(TupleHashEntryData, status), 8);
        assert_eq!(offset_of!(TupleHashEntryData, hash), 12);
        assert_eq!(size_of::<TupleHashEntryData>(), 16);
        assert_eq!(align_of::<TupleHashEntryData>(), 8);
        assert_eq!(size_of::<AttInMetadata>(), 32);
        assert_eq!(size_of::<Expr>(), 4);
        assert_eq!(offset_of!(TargetEntry, xpr), 0);
        assert_eq!(offset_of!(TargetEntry, expr), 8);
        assert_eq!(offset_of!(TargetEntry, resno), 16);
        assert_eq!(offset_of!(TargetEntry, resname), 24);
        assert_eq!(offset_of!(TargetEntry, ressortgroupref), 32);
        assert_eq!(offset_of!(TargetEntry, resorigtbl), 36);
        assert_eq!(offset_of!(TargetEntry, resorigcol), 40);
        assert_eq!(offset_of!(TargetEntry, resjunk), 42);
        assert_eq!(size_of::<TargetEntry>(), 48);
        assert_eq!(offset_of!(JunkFilter, type_), 0);
        assert_eq!(offset_of!(JunkFilter, jf_targetList), 8);
        assert_eq!(offset_of!(JunkFilter, jf_cleanTupType), 16);
        assert_eq!(offset_of!(JunkFilter, jf_cleanMap), 24);
        assert_eq!(offset_of!(JunkFilter, jf_resultSlot), 32);
        assert_eq!(size_of::<JunkFilter>(), 40);
    }
}
