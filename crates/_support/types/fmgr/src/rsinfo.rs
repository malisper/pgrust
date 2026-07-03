use core::ptr::NonNull;

use crate::fcinfo::{FmNode, FmNodePtr, FunctionCallInfoBaseData};

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

// C ReturnSetInfo minus econtext/expectedDesc/setResult/setDesc: those legs
// arrive with Materialize-mode SRFs, which are loud everywhere today.
#[repr(C)]
#[derive(Debug)]
pub struct ReturnSetInfo {
    tag: u32,
    pub allowedModes: u32,
    pub returnMode: SetFunctionReturnMode,
    pub isDone: ExprDoneCond,
}

impl ReturnSetInfo {
    pub fn new(allowed_modes: u32) -> Self {
        ReturnSetInfo {
            tag: RETURN_SET_INFO_TAG,
            allowedModes: allowed_modes,
            returnMode: SetFunctionReturnMode::ValuePerCall,
            isDone: ExprDoneCond::ExprSingleResult,
        }
    }

    // The invariant that makes fcinfo.rsinfo_mut's cast sound: tag is private,
    // always RETURN_SET_INFO_TAG, and sits at offset 0 (repr(C)).
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
