use crate::access::htup::AttributeCompression;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ToastAttrInfo {
    pub old_external: Option<Vec<u8>>,
    pub size: i32,
    pub colflags: u8,
    pub compression: AttributeCompression,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ToastTupleContext {
    pub flags: u8,
    pub attr: Vec<ToastAttrInfo>,
}

pub const TOAST_NEEDS_DELETE_OLD: u8 = 0x01;
pub const TOAST_NEEDS_FREE: u8 = 0x02;
pub const TOAST_HAS_NULLS: u8 = 0x04;
pub const TOAST_NEEDS_CHANGE: u8 = 0x08;

pub const TOASTCOL_NEEDS_DELETE_OLD: u8 = TOAST_NEEDS_DELETE_OLD;
pub const TOASTCOL_NEEDS_FREE: u8 = TOAST_NEEDS_FREE;
pub const TOASTCOL_IGNORE: u8 = 0x10;
pub const TOASTCOL_INCOMPRESSIBLE: u8 = 0x20;
