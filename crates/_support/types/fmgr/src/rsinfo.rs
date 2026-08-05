use core::ptr::NonNull;

use crate::fcinfo::{FmNode, FmNodePtr, FmgrInfo, FunctionCallInfoBaseData};
use types_error::PgResult;

// C RegisterExprContextCallback(ShutdownSQLFunction): the callee plants the
// hook on suspension; the SRF-driving node fires it at its ExecEnd/ReScan
// (the port's ShutdownExprContext(isCommit=true) moments), before resowner
// checks. Abort paths never fire it (C isCommit=false).
pub type SrfShutdownHook = fn(&mut FmgrInfo) -> PgResult<()>;

// NodeTag::T_ReturnSetInfo; parity with types_nodes asserted in funcapi tests
// (this crate sits below the nodes crate and cannot name the enum).
pub const RETURN_SET_INFO_TAG: u32 = 383;

pub const SFRM_ValuePerCall: u32 = 0x01;
pub const SFRM_Materialize: u32 = 0x02;
pub const SFRM_Materialize_Random: u32 = 0x04;
pub const SFRM_Materialize_Preferred: u32 = 0x08;

#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExprDoneCond {
    ExprSingleResult = 0,
    ExprMultipleResult = 1,
    ExprEndResult = 2,
}

#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SetFunctionReturnMode {
    ValuePerCall = SFRM_ValuePerCall,
    Materialize = SFRM_Materialize,
}

// C ReturnSetInfo minus econtext (contexts thread explicitly).
#[repr(C)]
pub struct ReturnSetInfo {
    tag: u32,
    pub allowedModes: u32,
    pub returnMode: SetFunctionReturnMode,
    pub isDone: ExprDoneCond,
    // SAFETY: type-erased `*const TupleDescData` (this crate sits below the
    // tuple crates); the executor arms it with the scan tupdesc, live for
    // the duration of the call.
    pub expectedDesc: Option<NonNull<core::ffi::c_void>>,
    // C's `Tuplestorestate *setResult`, taken by the executor post-call.
    pub setResult: Option<alloc::boxed::Box<dyn core::any::Any>>,
    // C's `TupleDesc setDesc`, type-erased like expectedDesc: a materialize
    // SRF may arm it with the rowtype its stored tuples actually carry; the
    // executor then runs C's tupledesc_match against the scan tupdesc.
    // SAFETY: when Some, points at a TupleDescData live until the call ends.
    pub setDesc: Option<NonNull<core::ffi::c_void>>,
    pub srf_shutdown: Option<SrfShutdownHook>,
}

impl core::fmt::Debug for ReturnSetInfo {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ReturnSetInfo")
            .field("allowedModes", &self.allowedModes)
            .field("returnMode", &self.returnMode)
            .field("isDone", &self.isDone)
            .field("expectedDesc", &self.expectedDesc)
            .field("has_setResult", &self.setResult.is_some())
            .finish()
    }
}

impl ReturnSetInfo {
    pub fn new(allowed_modes: u32) -> Self {
        ReturnSetInfo {
            tag: RETURN_SET_INFO_TAG,
            allowedModes: allowed_modes,
            returnMode: SetFunctionReturnMode::ValuePerCall,
            isDone: ExprDoneCond::ExprSingleResult,
            expectedDesc: None,
            setResult: None,
            setDesc: None,
            srf_shutdown: None,
        }
    }

    // The invariant that makes fcinfo.rsinfo_mut's cast sound: tag is private,
    // always RETURN_SET_INFO_TAG, and sits at offset 0 (repr(C)).
    //
    // Provenance contract (miri F6, notes/miri-pilot-lane.md): the returned
    // pointer derives from this `&mut` borrow, so ANY later direct access to
    // the ReturnSetInfo (even a safe field write like `rsinfo.isDone = ..`)
    // invalidates it. Drivers must re-arm `fcinfo.resultinfo` with a fresh
    // call immediately before EVERY invoke, after their last direct rsinfo
    // access — the per-call shape C's execSRF.c has anyway.
    pub fn as_fmnode_ptr(&mut self) -> FmNodePtr {
        Some(NonNull::from(self).cast::<FmNode>())
    }
}

const _: () = assert!(core::mem::offset_of!(ReturnSetInfo, tag) == 0);

impl FunctionCallInfoBaseData {
    #[inline]
    pub fn rsinfo_mut(&mut self) -> Option<&mut ReturnSetInfo> {
        let p = self.resultinfo?;
        // SAFETY: fmNodePtr contract — resultinfo leads with its NodeTag; the
        // tag match proves the pointee is the ReturnSetInfo the caller armed
        // via as_fmnode_ptr, live for the duration of this call.
        unsafe {
            if p.as_ref().tag != RETURN_SET_INFO_TAG {
                return None;
            }
            Some(p.cast::<ReturnSetInfo>().as_mut())
        }
    }
}
