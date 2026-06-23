use core::ffi::{c_int, c_void};

use crate::{
    uint64, Datum, FmgrInfo, FunctionCallInfo, MemoryContext, NodeTag, Oid, T_ExprContext,
    T_ReturnSetInfo, TupleDesc, TupleTableSlot,
};

pub type bits32 = u32;
pub type ExprDoneCond = c_int;
pub type SetFunctionReturnMode = u32;
pub type TypeFuncClass = u32;
pub type ExprContextCallbackFunction = Option<unsafe extern "C" fn(Datum)>;
pub type Tuplestorestate = c_void;
pub type ParamExecData = c_void;
pub use crate::params::ParamListInfo;
pub type ExprContextEState = c_void;

pub const ExprSingleResult: ExprDoneCond = 0;
pub const ExprMultipleResult: ExprDoneCond = 1;
pub const ExprEndResult: ExprDoneCond = 2;

pub const SFRM_ValuePerCall: SetFunctionReturnMode = 0x01;
pub const SFRM_Materialize: SetFunctionReturnMode = 0x02;
pub const SFRM_Materialize_Random: SetFunctionReturnMode = 0x04;
pub const SFRM_Materialize_Preferred: SetFunctionReturnMode = 0x08;

pub const TYPEFUNC_SCALAR: TypeFuncClass = 0;
pub const TYPEFUNC_COMPOSITE: TypeFuncClass = 1;
pub const TYPEFUNC_COMPOSITE_DOMAIN: TypeFuncClass = 2;
pub const TYPEFUNC_RECORD: TypeFuncClass = 3;
pub const TYPEFUNC_OTHER: TypeFuncClass = 4;

pub const MAT_SRF_USE_EXPECTED_DESC: bits32 = 0x01;
pub const MAT_SRF_BLESS: bits32 = 0x02;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct AttInMetadata {
    pub tupdesc: TupleDesc,
    pub attinfuncs: *mut FmgrInfo,
    pub attioparams: *mut Oid,
    pub atttypmods: *mut i32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FuncCallContext {
    pub call_cntr: uint64,
    pub max_calls: uint64,
    pub user_fctx: *mut c_void,
    pub attinmeta: *mut AttInMetadata,
    pub multi_call_memory_ctx: MemoryContext,
    pub tuple_desc: TupleDesc,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ExprContext_CB {
    pub next: *mut ExprContext_CB,
    pub function: ExprContextCallbackFunction,
    pub arg: Datum,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ExprContext {
    pub type_: NodeTag,
    pub ecxt_scantuple: *mut TupleTableSlot,
    pub ecxt_innertuple: *mut TupleTableSlot,
    pub ecxt_outertuple: *mut TupleTableSlot,
    pub ecxt_per_query_memory: MemoryContext,
    pub ecxt_per_tuple_memory: MemoryContext,
    pub ecxt_param_exec_vals: *mut ParamExecData,
    pub ecxt_param_list_info: ParamListInfo,
    pub ecxt_aggvalues: *mut Datum,
    pub ecxt_aggnulls: *mut bool,
    pub caseValue_datum: Datum,
    pub caseValue_isNull: bool,
    pub domainValue_datum: Datum,
    pub domainValue_isNull: bool,
    pub ecxt_oldtuple: *mut TupleTableSlot,
    pub ecxt_newtuple: *mut TupleTableSlot,
    pub ecxt_estate: *mut ExprContextEState,
    pub ecxt_callbacks: *mut ExprContext_CB,
}

impl ExprContext {
    pub const fn is_expr_context(&self) -> bool {
        self.type_ == T_ExprContext
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ReturnSetInfo {
    pub type_: NodeTag,
    pub econtext: *mut ExprContext,
    pub expectedDesc: TupleDesc,
    pub allowedModes: c_int,
    pub returnMode: SetFunctionReturnMode,
    pub isDone: ExprDoneCond,
    pub setResult: *mut Tuplestorestate,
    pub setDesc: TupleDesc,
}

impl ReturnSetInfo {
    pub const fn is_return_set_info(&self) -> bool {
        self.type_ == T_ReturnSetInfo
    }
}

/// `SetExprState` (`nodes/execnodes.h`) — run-time state of a set-returning (or
/// table-) function expression, the central struct of `execSRF.c`. Its first
/// member is a `NodeTag`, so a `*mut SetExprState` is a valid `Node *`.
///
/// `expr`, `args`, `elidedFuncState`, and `fcinfo` are opaque pointers at this
/// ABI boundary (`Expr *` / `List *` / `ExprState *` / `FunctionCallInfo`); the
/// embedded [`FmgrInfo`] `func` is a full value.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct SetExprState {
    /// `NodeTag type`.
    pub type_: NodeTag,
    /// `Expr *expr` — expression plan node.
    pub expr: *mut c_void,
    /// `List *args` — `ExprState`s for argument expressions.
    pub args: *mut c_void,
    /// `ExprState *elidedFuncState` — compiled expr when the FuncExpr was
    /// inlined away (evaluated with the generic `ExecEvalExpr`).
    pub elidedFuncState: *mut c_void,
    /// `FmgrInfo func` — fmgr lookup info for the target function.
    pub func: FmgrInfo,
    /// `Tuplestorestate *funcResultStore` — materialized SRF result rows.
    pub funcResultStore: *mut Tuplestorestate,
    /// `TupleTableSlot *funcResultSlot` — slot for the row currently returned.
    pub funcResultSlot: *mut TupleTableSlot,
    /// `TupleDesc funcResultDesc` — computed output descriptor, if any.
    pub funcResultDesc: TupleDesc,
    /// `bool funcReturnsTuple` — valid when `funcResultDesc` isn't NULL.
    pub funcReturnsTuple: bool,
    /// `bool funcReturnsSet` — function declared to return a set.
    pub funcReturnsSet: bool,
    /// `bool setArgsValid` — in a value-per-call series, args already in fcinfo.
    pub setArgsValid: bool,
    /// `bool shutdown_reg` — a shutdown callback is registered.
    pub shutdown_reg: bool,
    /// `FunctionCallInfo fcinfo` — call parameter structure for the function.
    pub fcinfo: FunctionCallInfo,
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn funcapi_layout_matches_postgres_abi_on_64_bit() {
        assert_eq!(size_of::<AttInMetadata>(), 32);
        assert_eq!(align_of::<AttInMetadata>(), 8);

        assert_eq!(offset_of!(FuncCallContext, call_cntr), 0);
        assert_eq!(offset_of!(FuncCallContext, max_calls), 8);
        assert_eq!(offset_of!(FuncCallContext, user_fctx), 16);
        assert_eq!(offset_of!(FuncCallContext, attinmeta), 24);
        assert_eq!(offset_of!(FuncCallContext, multi_call_memory_ctx), 32);
        assert_eq!(offset_of!(FuncCallContext, tuple_desc), 40);
        assert_eq!(size_of::<FuncCallContext>(), 48);
        assert_eq!(align_of::<FuncCallContext>(), 8);

        assert_eq!(offset_of!(ExprContext, type_), 0);
        assert_eq!(offset_of!(ExprContext, ecxt_scantuple), 8);
        assert_eq!(offset_of!(ExprContext, ecxt_per_query_memory), 32);
        assert_eq!(offset_of!(ExprContext, ecxt_param_exec_vals), 48);
        assert_eq!(offset_of!(ExprContext, ecxt_aggvalues), 64);
        assert_eq!(offset_of!(ExprContext, caseValue_datum), 80);
        assert_eq!(offset_of!(ExprContext, domainValue_datum), 96);
        assert_eq!(offset_of!(ExprContext, ecxt_oldtuple), 112);
        assert_eq!(offset_of!(ExprContext, ecxt_callbacks), 136);
        assert_eq!(size_of::<ExprContext>(), 144);
        assert_eq!(align_of::<ExprContext>(), 8);

        assert_eq!(offset_of!(ReturnSetInfo, type_), 0);
        assert_eq!(offset_of!(ReturnSetInfo, econtext), 8);
        assert_eq!(offset_of!(ReturnSetInfo, expectedDesc), 16);
        assert_eq!(offset_of!(ReturnSetInfo, allowedModes), 24);
        assert_eq!(offset_of!(ReturnSetInfo, returnMode), 28);
        assert_eq!(offset_of!(ReturnSetInfo, isDone), 32);
        assert_eq!(offset_of!(ReturnSetInfo, setResult), 40);
        assert_eq!(offset_of!(ReturnSetInfo, setDesc), 48);
        assert_eq!(size_of::<ReturnSetInfo>(), 56);
        assert_eq!(align_of::<ReturnSetInfo>(), 8);

        // SetExprState: type(4)+pad, expr/args/elidedFuncState at 8/16/24, the
        // 48-byte FmgrInfo `func` at 32 (ends 80), the three result pointers at
        // 80/88/96, four bools at 104..107, pad, fcinfo at 112; size 120.
        assert_eq!(offset_of!(SetExprState, type_), 0);
        assert_eq!(offset_of!(SetExprState, expr), 8);
        assert_eq!(offset_of!(SetExprState, args), 16);
        assert_eq!(offset_of!(SetExprState, elidedFuncState), 24);
        assert_eq!(offset_of!(SetExprState, func), 32);
        assert_eq!(offset_of!(SetExprState, funcResultStore), 80);
        assert_eq!(offset_of!(SetExprState, funcResultSlot), 88);
        assert_eq!(offset_of!(SetExprState, funcResultDesc), 96);
        assert_eq!(offset_of!(SetExprState, funcReturnsTuple), 104);
        assert_eq!(offset_of!(SetExprState, funcReturnsSet), 105);
        assert_eq!(offset_of!(SetExprState, setArgsValid), 106);
        assert_eq!(offset_of!(SetExprState, shutdown_reg), 107);
        assert_eq!(offset_of!(SetExprState, fcinfo), 112);
        assert_eq!(size_of::<SetExprState>(), 120);
        assert_eq!(align_of::<SetExprState>(), 8);
    }
}
