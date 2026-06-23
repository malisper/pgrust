//! Extensible-node / custom-scan registration vocabulary
//! (`nodes/extensible.h`).
//!
//! These are the method tables loadable modules register with `extensible.c`
//! and that the core copy/equal/out/read and custom-scan executor paths look
//! up by name. `extensible.c` itself only ever reads the leading
//! `extnodename` / `CustomName` field and stores the table; the callbacks are
//! invoked by the dispatch consumers, not by the registry.
//!
//! # Representation
//!
//! C keeps a `const void *` to the registrant's process-lifetime method table
//! in its hash table and the register/get functions traffic in `const char *`
//! / `const ... *`. In the owned-tree rewrite there are no raw pointers: the
//! method tables are owned, `Clone` values stored directly under their
//! `String` name keys, and a lookup returns a clone of the registered table
//! (the callbacks inside are `Copy` `Option<fn(..)>`s) — the faithful analogue
//! of C returning the stored `const ... *`.
//!
//! The callback fields are function pointers over the node/buffer/scan structs
//! they operate on. Those structs (`ExtensibleNode`, `StringInfoData`,
//! `CustomScan`, `Node`) are forward declared here as opaque types — exactly
//! C's incomplete `struct X *` parameters in `extensible.h` — and collapse onto
//! the owners' real layouts when those subsystems land.

#![allow(non_snake_case)]
#![forbid(unsafe_code)]

use ::types_core::primitive::Size;

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
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ExtensibleNode {
    _opaque: (),
}

/// `struct StringInfoData` — forward declared in the `nodeOut` signature.
#[derive(Clone, Debug, Default)]
pub struct StringInfoData {
    _opaque: (),
}

/// `CustomScan` — the plan node a `CreateCustomScanState` callback turns into
/// execution state. Forward declared in `extensible.h`.
#[derive(Clone, Debug, Default)]
pub struct CustomScan {
    _opaque: (),
}

/// `Node` — the generic node a `CreateCustomScanState` callback returns
/// (really a `CustomScanState *`). Forward declared in `extensible.h`.
#[derive(Clone, Debug, Default)]
pub struct Node {
    _opaque: (),
}

/// `void (*nodeCopy)(ExtensibleNode *newnode, const ExtensibleNode *oldnode)`.
pub type NodeCopy = Option<fn(newnode: &mut ExtensibleNode, oldnode: &ExtensibleNode)>;

/// `bool (*nodeEqual)(const ExtensibleNode *a, const ExtensibleNode *b)`.
pub type NodeEqual = Option<fn(a: &ExtensibleNode, b: &ExtensibleNode) -> bool>;

/// `void (*nodeOut)(struct StringInfoData *str, const ExtensibleNode *node)`.
pub type NodeOut = Option<fn(str: &mut StringInfoData, node: &ExtensibleNode)>;

/// `void (*nodeRead)(ExtensibleNode *node)`.
pub type NodeRead = Option<fn(node: &mut ExtensibleNode)>;

/// `Node *(*CreateCustomScanState)(CustomScan *cscan)`.
pub type CreateCustomScanState = Option<fn(cscan: &CustomScan) -> Node>;

/// `ExtensibleNodeMethods` (`nodes/extensible.h`): the callback table for an
/// extensible node type. `extensible.c` reads `extnodename` (the registry key)
/// and otherwise stores the table; all callbacks are mandatory and are invoked
/// by the copy/equal/out/read dispatch consumers.
#[derive(Clone, Debug, Default)]
pub struct ExtensibleNodeMethods {
    /// `const char *extnodename` — the identifier this table is registered
    /// under.
    pub extnodename: Option<String>,
    /// `Size node_size` — size in bytes of an extensible node of this type.
    pub node_size: Size,
    pub nodeCopy: NodeCopy,
    pub nodeEqual: NodeEqual,
    pub nodeOut: NodeOut,
    pub nodeRead: NodeRead,
}

/// `CustomScanMethods` (`nodes/extensible.h`): the registration table for a
/// custom-scan provider. `extensible.c` reads `CustomName` (the registry key).
#[derive(Clone, Debug, Default)]
pub struct CustomScanMethods {
    /// `const char *CustomName` — the identifier this table is registered
    /// under.
    pub CustomName: Option<String>,
    /// `Node *(*CreateCustomScanState)(CustomScan *cscan)`.
    pub CreateCustomScanState: CreateCustomScanState,
}
