use alloc::boxed::Box;
use alloc::format;
use core::ffi::CStr;
use core::ptr::NonNull;

use ::datum::Datum;
use ::mcx::Mcx;
use ::types_core::{primitive::InvalidOid, Oid};
use ::types_error::{PgError, PgResult, SoftErrorContext};

use crate::fcinfo::{
    FmNode, FmNodePtr, FmgrInfo, FunctionCallInfoBaseData, LocalFcinfo, PGFunction,
};

// nodetags.h value, parity-asserted in fmgr_core tests (types_nodes sits above).
pub const T_ERROR_SAVE_CONTEXT: u32 = 447;

/// C miscnodes.h `ErrorSaveContext`, NodeTag-led for the `IsA` demux.
#[repr(C)]
pub struct ErrorSaveNode {
    node: FmNode,
    pub ctx: SoftErrorContext,
}

impl ErrorSaveNode {
    pub fn new(details_wanted: bool) -> Self {
        Self {
            node: FmNode {
                tag: T_ERROR_SAVE_CONTEXT,
            },
            ctx: SoftErrorContext::new(details_wanted),
        }
    }

    // Derived from the whole node, not the field: the callee's upcast back to
    // ErrorSaveNode needs whole-struct provenance (Miri SB).
    pub fn fm_node_ptr(&mut self) -> FmNodePtr {
        Some(NonNull::from(&mut *self).cast::<FmNode>())
    }
}

impl FunctionCallInfoBaseData {
    /// # Safety
    /// `context`, if set, points at a live FmNode-led node armed for this call
    /// ([`ErrorSaveNode::fm_node_ptr`]), no other reference live to it.
    #[inline]
    pub unsafe fn soft_error_context<'a>(&self) -> Option<&'a mut SoftErrorContext> {
        let p = self.context?;
        // SAFETY: caller contract; the tag check proves the concrete type
        // before the upcast.
        unsafe {
            if p.as_ref().tag != T_ERROR_SAVE_CONTEXT {
                return None;
            }
            Some(&mut p.cast::<ErrorSaveNode>().as_mut().ctx)
        }
    }

    /// The whole `ErrorSaveNode`, for functions that both record their own
    /// soft errors and forward the node to a sub-function (e.g. array_in →
    /// element InputFunctionCallSafe).
    ///
    /// # Safety
    /// Same contract as [`Self::soft_error_context`].
    #[inline]
    pub unsafe fn error_save_node<'a>(&self) -> Option<&'a mut ErrorSaveNode> {
        let p = self.context?;
        // SAFETY: caller contract; the tag check proves the concrete type.
        unsafe {
            if p.as_ref().tag != T_ERROR_SAVE_CONTEXT {
                return None;
            }
            Some(p.cast::<ErrorSaveNode>().as_mut())
        }
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn input_returned_non_null(fn_oid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "input function {fn_oid} returned non-NULL"
    )))
}

#[track_caller]
#[cold]
#[inline(never)]
fn input_returned_null(fn_oid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "input function {fn_oid} returned NULL"
    )))
}

#[track_caller]
#[cold]
#[inline(never)]
fn direct_input_returned_null(func: PGFunction) -> Box<PgError> {
    Box::new(PgError::error(format!(
        "input function {:#x} returned NULL",
        func as usize
    )))
}

/// C `InputFunctionCallSafe`: `Ok(false)` = soft error saved, `*result` garbage.
pub fn input_function_call_safe(
    flinfo: &mut FmgrInfo,
    str: Option<&CStr>,
    typioparam: Oid,
    typmod: i32,
    mcx: Mcx<'_>,
    mut escontext: Option<&mut ErrorSaveNode>,
    result: &mut Datum,
) -> PgResult<bool> {
    if str.is_none() && flinfo.fn_strict {
        *result = Datum::null();
        return Ok(true);
    }

    let mut fcinfo = LocalFcinfo::<3>::fresh(InvalidOid);
    // SAFETY: `mcx` outlives this stack frame's single call.
    unsafe { fcinfo.set_result_mcx(mcx) };
    if let Some(node) = escontext.as_deref_mut() {
        fcinfo.context = node.fm_node_ptr();
    }
    // C keeps isnull=false on all three args even for a NULL str (the
    // non-strict callee sees a NULL cstring pointer).
    fcinfo.set_arg(
        0,
        match str {
            Some(s) => Datum::from_usize(s.as_ptr() as usize),
            None => Datum::null(),
        },
    );
    fcinfo.set_arg(1, Datum::from_oid(typioparam));
    fcinfo.set_arg(2, Datum::from_i32(typmod));

    *result = flinfo.invoke(&mut fcinfo)?;

    if let Some(node) = escontext.as_deref() {
        if node.ctx.error_occurred() {
            return Ok(false);
        }
    }

    if str.is_none() {
        if !fcinfo.isnull {
            return Err(input_returned_non_null(flinfo.fn_oid));
        }
    } else if fcinfo.isnull {
        return Err(input_returned_null(flinfo.fn_oid));
    }
    Ok(true)
}

/// C `InputFunctionCall`; `str: None` reads a NULL.
pub fn input_function_call(
    flinfo: &mut FmgrInfo,
    str: Option<&CStr>,
    typioparam: Oid,
    typmod: i32,
    mcx: Mcx<'_>,
) -> PgResult<Datum> {
    let mut result = Datum::null();
    let ok = input_function_call_safe(flinfo, str, typioparam, typmod, mcx, None, &mut result)?;
    debug_assert!(ok);
    Ok(result)
}

/// C `DirectInputFunctionCallSafe`; the callee is assumed strict, no FmgrInfo.
pub fn direct_input_function_call_safe(
    func: PGFunction,
    str: Option<&CStr>,
    typioparam: Oid,
    typmod: i32,
    mcx: Mcx<'_>,
    mut escontext: Option<&mut ErrorSaveNode>,
    result: &mut Datum,
) -> PgResult<bool> {
    let Some(s) = str else {
        *result = Datum::null();
        return Ok(true);
    };

    let mut fcinfo = LocalFcinfo::<3>::fresh(InvalidOid);
    // SAFETY: `mcx` outlives this stack frame's single call.
    unsafe { fcinfo.set_result_mcx(mcx) };
    if let Some(node) = escontext.as_deref_mut() {
        fcinfo.context = node.fm_node_ptr();
    }
    fcinfo.set_arg(0, Datum::from_usize(s.as_ptr() as usize));
    fcinfo.set_arg(1, Datum::from_oid(typioparam));
    fcinfo.set_arg(2, Datum::from_i32(typmod));

    *result = func(None, &mut fcinfo)?;

    if let Some(node) = escontext.as_deref() {
        if node.ctx.error_occurred() {
            return Ok(false);
        }
    }

    if fcinfo.isnull {
        return Err(direct_input_returned_null(func));
    }
    Ok(true)
}
