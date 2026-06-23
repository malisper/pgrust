//! ABI definitions for extensible node types (`nodes/extensible.h`).
//!
//! `extensible.c` only ever reads the leading `extnodename` / `CustomName`
//! field and stores the method pointer opaquely, so the callback fields are
//! modeled as pointer-sized opaque slots. The leading-field offset and overall
//! pointer-count layout match PostgreSQL 18.3; the structs are `#[repr(C)]`
//! because loadable modules construct and register them across the boundary.

use core::ffi::{c_char, c_void};

use crate::types::{NodeTag, Size};

/// Maximum length of an extensible node identifier (`EXTNODENAME_MAX_LEN`).
pub const EXTNODENAME_MAX_LEN: usize = 64;

/// Flags for custom paths (`nodes/extensible.h`).
pub const CUSTOMPATH_SUPPORT_BACKWARD_SCAN: u32 = 0x0001;
pub const CUSTOMPATH_SUPPORT_MARK_RESTORE: u32 = 0x0002;
pub const CUSTOMPATH_SUPPORT_PROJECTION: u32 = 0x0004;

/// `ExtensibleNode` (`nodes/extensible.h`): the common header of any
/// extension-defined node. `type` is always `T_ExtensibleNode`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ExtensibleNode {
    pub type_: NodeTag,
    /// identifier of `ExtensibleNodeMethods`
    pub extnodename: *const c_char,
}

/// `ExtensibleNodeMethods` (`nodes/extensible.h`): the callback table for an
/// extensible node type. `extensible.c` only reads `extnodename`; the
/// callbacks are pointer-sized opaque slots here (`nodeCopy`, `nodeEqual`,
/// `nodeOut`, `nodeRead`).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ExtensibleNodeMethods {
    pub extnodename: *const c_char,
    pub node_size: Size,
    /// `void (*nodeCopy)(ExtensibleNode *, const ExtensibleNode *)`
    pub nodeCopy: *const c_void,
    /// `bool (*nodeEqual)(const ExtensibleNode *, const ExtensibleNode *)`
    pub nodeEqual: *const c_void,
    /// `void (*nodeOut)(StringInfoData *, const ExtensibleNode *)`
    pub nodeOut: *const c_void,
    /// `void (*nodeRead)(ExtensibleNode *)`
    pub nodeRead: *const c_void,
}

/// `CustomScanMethods` (`nodes/extensible.h`): the registration table for a
/// custom-scan provider. `extensible.c` only reads `CustomName`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct CustomScanMethods {
    pub CustomName: *const c_char,
    /// `Node *(*CreateCustomScanState)(CustomScan *)`
    pub CreateCustomScanState: *const c_void,
}
