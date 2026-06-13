//! Extensible-node / custom-scan registration vocabulary
//! (`nodes/extensible.h`).
//!
//! These are the method tables loadable modules register with `extensible.c`
//! and that the core copy/equal/out/read and custom-scan executor paths look
//! up by name. `extensible.c` itself only ever reads the leading
//! `extnodename` / `CustomName` field and stores the table opaquely (C keeps a
//! `const void *` in its hash table); the callbacks are invoked by the
//! dispatch consumers, not by the registry.
//!
//! The callback fields are C-ABI function pointers over the node/buffer/scan
//! structs they operate on. Those structs (`ExtensibleNode`,
//! `StringInfoData`, `CustomScan`, `CustomScanState`, `Node`) are forward
//! declared here as opaque types — exactly C's incomplete `struct X *`
//! parameters in `extensible.h` — and collapse onto the owners' real layouts
//! when those subsystems land.

#![allow(non_snake_case)]

use core::ffi::c_void;

use types_core::primitive::Size;

/// Maximum length of an extensible node identifier (`EXTNODENAME_MAX_LEN`).
pub const EXTNODENAME_MAX_LEN: usize = 64;

/// Flags for custom paths (`nodes/extensible.h`): capabilities of the
/// resulting scan, used as bitmasks in `CustomPath.flags` / `CustomScan.flags`.
pub const CUSTOMPATH_SUPPORT_BACKWARD_SCAN: u32 = 0x0001;
pub const CUSTOMPATH_SUPPORT_MARK_RESTORE: u32 = 0x0002;
pub const CUSTOMPATH_SUPPORT_PROJECTION: u32 = 0x0004;

/// `struct ExtensibleNode` — forward declared in `extensible.h`'s callback
/// signatures (`extensible.c` never constructs or reads one). The real layout
/// (`NodeTag type; const char *extnodename;`) lands with the node subsystem.
#[repr(C)]
pub struct ExtensibleNode {
    _opaque: [u8; 0],
}

/// `struct StringInfoData` — forward declared in the `nodeOut` signature.
#[repr(C)]
pub struct StringInfoData {
    _opaque: [u8; 0],
}

/// `CustomScan` — the plan node a `CreateCustomScanState` callback turns into
/// execution state. Forward declared in `extensible.h`.
#[repr(C)]
pub struct CustomScan {
    _opaque: [u8; 0],
}

/// `Node` — the generic node a `CreateCustomScanState` callback returns
/// (really a `CustomScanState *`). Forward declared in `extensible.h`.
#[repr(C)]
pub struct Node {
    _opaque: [u8; 0],
}

/// `void (*nodeCopy)(ExtensibleNode *newnode, const ExtensibleNode *oldnode)`.
pub type NodeCopy = unsafe extern "C" fn(newnode: *mut ExtensibleNode, oldnode: *const ExtensibleNode);

/// `bool (*nodeEqual)(const ExtensibleNode *a, const ExtensibleNode *b)`.
pub type NodeEqual = unsafe extern "C" fn(a: *const ExtensibleNode, b: *const ExtensibleNode) -> bool;

/// `void (*nodeOut)(struct StringInfoData *str, const ExtensibleNode *node)`.
pub type NodeOut = unsafe extern "C" fn(str: *mut StringInfoData, node: *const ExtensibleNode);

/// `void (*nodeRead)(ExtensibleNode *node)`.
pub type NodeRead = unsafe extern "C" fn(node: *mut ExtensibleNode);

/// `Node *(*CreateCustomScanState)(CustomScan *cscan)`.
pub type CreateCustomScanState = unsafe extern "C" fn(cscan: *mut CustomScan) -> *mut Node;

/// `ExtensibleNodeMethods` (`nodes/extensible.h`): the callback table for an
/// extensible node type. `extensible.c` reads `extnodename` (the registry key)
/// and otherwise stores the table opaquely; all callbacks are mandatory and
/// are invoked by the copy/equal/out/read dispatch consumers.
#[repr(C)]
pub struct ExtensibleNodeMethods {
    /// `const char *extnodename` — the identifier this table is registered
    /// under. Owned by the registrant (process-lifetime), never copied.
    pub extnodename: *const core::ffi::c_char,
    /// `Size node_size` — size in bytes of an extensible node of this type.
    pub node_size: Size,
    pub nodeCopy: NodeCopy,
    pub nodeEqual: NodeEqual,
    pub nodeOut: NodeOut,
    pub nodeRead: NodeRead,
}

/// `CustomScanMethods` (`nodes/extensible.h`): the registration table for a
/// custom-scan provider. `extensible.c` reads `CustomName` (the registry key).
#[repr(C)]
pub struct CustomScanMethods {
    /// `const char *CustomName` — the identifier this table is registered
    /// under. Owned by the registrant.
    pub CustomName: *const core::ffi::c_char,
    /// `Node *(*CreateCustomScanState)(CustomScan *cscan)`.
    pub CreateCustomScanState: CreateCustomScanState,
}

/// `ExtensibleNodeEntry` (`extensible.c`, file-local): a hash-table row — a
/// fixed-width name key plus the registered method-table pointer (`const void
/// *`, stored opaquely). Both registries (`extensible_node_methods`,
/// `custom_scan_methods`) use this entry shape.
#[repr(C)]
pub struct ExtensibleNodeEntry {
    /// `char extnodename[EXTNODENAME_MAX_LEN]` — the NUL-padded key.
    pub extnodename: [core::ffi::c_char; EXTNODENAME_MAX_LEN],
    /// `const void *extnodemethods` — the registered method table.
    pub extnodemethods: *const c_void,
}
