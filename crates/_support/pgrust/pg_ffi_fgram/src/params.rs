//! ABI definitions for `ParamListInfo` and friends (`nodes/params.h`).
//!
//! These structs cross the C boundary (they are serialized to/from shared
//! memory and passed through the executor/parser), so they are `#[repr(C)]`
//! with exact field layout matching PostgreSQL 18.3.

use core::ffi::{c_char, c_int, c_void};

use crate::types::{uint16, Datum, Oid};

macro_rules! const_assert {
    ($cond:expr) => {
        const _: () = assert!($cond);
    };
}

/// `ParamExternData` (`nodes/params.h`): one external parameter value.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ParamExternData {
    /// parameter value
    pub value: Datum,
    /// is it NULL?
    pub isnull: bool,
    /// flag bits, see PARAM_FLAG_*
    pub pflags: uint16,
    /// parameter's datatype, or 0
    pub ptype: Oid,
}

const_assert!(core::mem::offset_of!(ParamExternData, value) == 0);
const_assert!(core::mem::size_of::<ParamExternData>() == 2 * core::mem::size_of::<Datum>());

/// Flag bits for `ParamExternData.pflags`.
pub const PARAM_FLAG_CONST: uint16 = 0x0001;

/// `ParamFetchHook` (`nodes/params.h`).
///
/// The C declaration is a C-ABI function pointer; in the all-Rust port the hook
/// is invoked across crates as a plain Rust-ABI `unsafe fn` (no `extern "C"`),
/// keeping the function-pointer field the same machine width.
pub type ParamFetchHook = Option<
    unsafe fn(
        params: ParamListInfo,
        paramid: c_int,
        speculative: bool,
        workspace: *mut ParamExternData,
    ) -> *mut ParamExternData,
>;

/// `ParamCompileHook` (`nodes/params.h`). The `Param`/`ExprState` types are
/// opaque here; this hook is never called from the params crate itself.
pub type ParamCompileHook = Option<
    unsafe fn(
        params: ParamListInfo,
        param: *mut c_void,
        state: *mut c_void,
        resv: *mut Datum,
        resnull: *mut bool,
    ),
>;

/// `ParserSetupHook` (`nodes/params.h`). The `ParseState` type is opaque here.
pub type ParserSetupHook = Option<unsafe fn(pstate: *mut c_void, arg: *mut c_void)>;

/// `ParamListInfoData` (`nodes/params.h`).
///
/// This is the header of a variable-length structure: `params[]` is a
/// flexible array member of length `numParams` (or length zero if `paramFetch`
/// is supplied). Allocations always come from a memory context, so the Rust
/// side never instantiates this struct by value; it works through the raw
/// pointer typedef [`ParamListInfo`].
#[repr(C)]
#[derive(Debug)]
pub struct ParamListInfoData {
    /// parameter fetch hook
    pub paramFetch: ParamFetchHook,
    pub paramFetchArg: *mut c_void,
    /// parameter compile hook
    pub paramCompile: ParamCompileHook,
    pub paramCompileArg: *mut c_void,
    /// parser setup hook
    pub parserSetup: ParserSetupHook,
    pub parserSetupArg: *mut c_void,
    /// params as a single string for errors
    pub paramValuesStr: *mut c_char,
    /// nominal/maximum # of Params represented
    pub numParams: c_int,
    /// flexible array member; see comment above.
    pub params: [ParamExternData; 0],
}

const_assert!(core::mem::offset_of!(ParamListInfoData, paramFetch) == 0);

/// `ParamListInfo` (`nodes/params.h`): pointer to a `ParamListInfoData`.
pub type ParamListInfo = *mut ParamListInfoData;

/// `ParamsErrorCbData` (`nodes/params.h`): state for the
/// `ParamsErrorCallback` error-context callback.
#[repr(C)]
#[derive(Debug)]
pub struct ParamsErrorCbData {
    pub portalName: *const c_char,
    pub params: ParamListInfo,
}
